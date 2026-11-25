# Medios High Availability Tiers

A Medios cluster is complete with one controller. High availability adds standby controllers that contend for leadership through an etcd v3 election (`--etcd`, see [architecture.md](architecture.md#leader-election-etcd)), so that losing the leading controller costs a short leaderless window instead of an outage that lasts until someone restarts it. Running tasks don't depend on the controller while they run: nodes keep executing them through a failover, report them to the new leader when they reregister, and frameworks resubscribe under the framework id they already hold.

This document defines four HA tiers by the network the controllers, etcd and clients share, gives the configuration for each, and maps HA requirements to the controls that meet them. The tiers parallel the deployment modes in [medios-security.md](medios-security.md): tier 1 runs inside Mode 1, tier 2 inside Mode 2, and tiers 3 and 4 inside Mode 3.

---

## 1. How failover works in every tier

Each controller started with `--etcd=etcd://<host>:<port>/<election>` takes a 10 s etcd lease, renews it every TTL/3 (about 3.3 s), and campaigns for the named election. One wins; the rest stand by. A standby answers scheduler calls with `307 Temporary Redirect` to the leader, and answers `503 No leader elected` while there is none.

| Event | What happens | Leaderless window |
|---|---|---|
| Leader process or host dies | Its lease lapses; a standby's campaign wins; nodes, which poll the election every 1 s, reregister; frameworks resubscribe | About one lease TTL (10 s) plus reregistration |
| etcd unreachable | The leader's next keepalive fails and the leader exits with status 1. Standbys contend again every 1 s (`CONTEND_RETRY_INTERVAL`) and stay up. Nodes and running tasks stay up | The outage, plus up to one lease TTL after etcd returns |
| Leader cut off from etcd, still reachable by nodes | Same as etcd unreachable, seen from the leader: it exits, and a standby that can reach etcd takes over | About one lease TTL |
| Leader cut off from nodes, still holding its lease | The election does not see this. The leader marks nodes unreachable after `--node_ping_timeout` × `--max_node_ping_timeouts` (15 s × 5 by default) | No failover |

Four rules hold in every tier:

1. A deposed leader exits instead of stepping down in place, so every controller started with `--etcd` needs a supervisor that restarts it (systemd `Restart=always`, a cloud instance restart policy, or a Medios task). The restarted process rejoins as a standby.
2. Election coordinates leadership only. The registry (node admissions, quota, weights, maintenance schedules) lives on each controller's own host, in memory or in its local `leveldb`, and is not replicated to a standby. Keep those settings declarative and reapply them after a failover; replicated registry storage is not planned ([architecture.md](architecture.md#controller-restart-and-persistence)).
3. Frameworks must survive the leaderless window. Set `failover_timeout` well above the longest window you expect, because a framework that doesn't resubscribe within it has its tasks killed. Give the SDK client every controller URL, call subscribe again whenever the stream ends, and reconcile after `SUBSCRIBED`. The Go and Python SDK clients both take a comma-separated URL list, follow a standby's redirect (at most 3 hops per URL), skip a controller that is down or answers 503, and resubscribe under the framework id they were assigned.
4. Nodes find the leader through the election, `--controller=etcd://<host>:<port>/<election>`. A node that has not reregistered within `--node_reregister_timeout` (10 min, the minimum) is marked unreachable.

The redirect names the leader by hostname. A controller advertises `--hostname` when set; otherwise it reverse-resolves its `--ip`, and with `--no-hostname_lookup` it advertises the IP itself. The `Location` header is scheme-relative (`//<host>:<port>/...`), so an HTTPS client stays on HTTPS. Every framework must be able to resolve and reach whatever name the leader advertises; picking that name is the main per-tier decision.

---

## 2. HA Tiers

### Tier 1: Single host on loopback

- Topology: two controllers, one etcd and the nodes on one host, all bound to `127.0.0.1`.
- Failure covered: a controller process crash. A host failure takes everything down, so this tier adds nothing over one controller with `--registry=leveldb` and a supervisor. Use it to exercise the failover path in development and CI, not for availability.
- Redirect name: reverse lookup of `127.0.0.1` gives a local name such as `localhost.localdomain`. Set `--no-hostname_lookup` to advertise `127.0.0.1` instead.
- Configuration:
  ```bash
  etcd --name ha --data-dir "$HOME/.medios/etcd" \
    --listen-client-urls http://127.0.0.1:2379 \
    --advertise-client-urls http://127.0.0.1:2379

  for port in 5050 5052; do
    medios-controller \
      --ip=127.0.0.1 \
      --port=$port \
      --no-hostname_lookup \
      --work_dir="$HOME/.medios/controller-$port" \
      --registry=leveldb \
      --etcd=etcd://127.0.0.1:2379/medios &
  done

  medios-node \
    --ip=127.0.0.1 \
    --port=5051 \
    --controller=etcd://127.0.0.1:2379/medios \
    --work_dir="$HOME/.medios/node" \
    ...                                   # remaining flags as in medios-security.md Mode 1
  ```
  Frameworks: `http://127.0.0.1:5050,http://127.0.0.1:5052`.
- Status: exercised with real etcd: leader kill, a second leader kill after the first leader restarts as a standby, and a 20 s etcd outage with no controller restarted. Tasks survived all three.

---

### Tier 2: Private overlay (tailnet or WireGuard)

- Topology: controllers on two or more hosts of one encrypted overlay, each bound to its overlay address; etcd on an overlay host; nodes and frameworks on the same overlay.
- Failure covered: the loss of one controller host. With a single etcd member, losing the etcd host leaves the cluster leaderless until etcd returns (tasks keep running). To survive that too, run three etcd members on three hosts and point each controller at the member on its own host. The node side is weaker because each node names one member (#14), so a node whose member is lost cannot find a new leader.
- Redirect name: with an overlay DNS that resolves reverse lookups (Tailscale MagicDNS does), leave hostname lookup on; each controller advertises its overlay name (`<host>.<tailnet>.ts.net`), and every overlay peer resolves it. Clients outside the overlay can't, which Mode 2 already excludes.
- Configuration, per controller host:
  ```bash
  medios-controller \
    --ip=<overlay-ip> \
    --port=5050 \
    --work_dir=/var/lib/medios/controller \
    --registry=leveldb \
    --etcd=etcd://<etcd-overlay-ip>:2379/medios
  ```
  Bind etcd's client URL to its overlay address and firewall port 2379 to the overlay interface, as Mode 2 does for 5050 and 5051. The election path is plain HTTP and relies on the overlay for encryption.
  Frameworks: `http://<controller-1-overlay-ip>:5050,http://<controller-2-overlay-ip>:5050`.
- Status: exercised across two hosts on a tailnet, with each framework given only a standby's URL so that a task could run only through a cross-host redirect. The redirect named the other host's MagicDNS name in both directions, both SDKs followed it, and after the leader was killed the node reregistered with the leader on the other host and every task survived.

---

### Tier 3: Heterogeneous network

- Topology: hosts on different networks with no single DNS every client shares: wired LAN, Wi-Fi, a cloud VPC, hosts behind NAT, possibly more than one overlay. Latency and loss differ by link.
- Failure covered: the same as tier 2, provided the election links are reliable.
- Redirect name: reverse lookup gives a different answer, or none, depending on which network does the lookup. Set `--hostname=<name every framework resolves>` or `--no-hostname_lookup` with an `--ip` every framework can route to, and `--advertise_ip` where a host sits behind NAT.
- Link quality: a single failed lease keepalive deposes the leader today, and the gateway calls have no request timeout (#13). On a link that drops requests, every drop costs one failover. On a link that stalls, the lease can lapse while the old leader has not yet seen it. Put etcd and the controllers on wired or same-provider links and keep hosts on Wi-Fi or long-haul paths as nodes. Size the node ping budget (`--node_ping_timeout` × `--max_node_ping_timeouts`) to the worst node link.
- etcd placement: a three-member cluster spread across networks keeps quorum through the loss of one network, but each controller and node still talks to one member (#14). A local `etcd gateway` (a TCP proxy over the member list) on each host gives that host one stable endpoint over all members; this has not been exercised with Medios.
- Status: not exercised as a tier. The redirect and SDK behaviour from tier 2 carry over once the advertised name resolves everywhere.

---

### Tier 4: Enterprise

- Topology: controllers in two or more failure domains (zones, racks or sites), an etcd cluster of three or five members spread across the same domains, nodes and frameworks across the organisation's network, and security per Mode 3.
- Required configuration:
  1. Transport and API security per [medios-security.md](medios-security.md) Mode 3: mTLS between daemons, authenticated HTTP and framework APIs, and non-permissive ACLs. Certificates must carry the name each controller advertises in redirects (`--hostname`) as a SAN, because that is the name a redirected client connects to ([TLS.md](TLS.md)).
  2. Controllers supervised and placed one per failure domain, with `--registry=leveldb`, an explicit `--hostname` that resolves in the organisation's DNS, and the registry settings applied from declarative configuration after every failover.
  3. Frameworks configured with every controller's stable name. A load balancer in front of the controllers works only if clients can also reach the host named in the redirect it returns; otherwise give clients the list.
  4. Alerts on scheduler API 503 responses (no leader), on controller exits, and on etcd health and quorum.
- Gaps:
  - The election client speaks plain HTTP to one etcd endpoint, with no TLS, no etcd authentication and no member list (#14), so the election path does not meet the Mode 3 posture.
  - One dropped keepalive deposes the leader, and a hung gateway call goes unnoticed (#13).
  - Registry state is not replicated across controllers by design; it has to be managed as configuration.
- Status: not supported end to end until #14 lands; the controller, node and SDK behaviour from tiers 1 and 2 apply unchanged.

---

## 3. HA Requirement Mapping Across Tiers

| Requirement | Tier 1 (Loopback) | Tier 2 (Overlay) | Tier 3 (Heterogeneous) | Tier 4 (Enterprise) | Product status |
|---|---|---|---|---|---|
| Automatic leader failover | `--etcd` on every controller | `--etcd` on every controller | `--etcd` on every controller | `--etcd` on every controller | Supported |
| Standbys survive an etcd outage | Automatic: standbys contend again every 1 s | Automatic | Automatic | Automatic | Supported |
| Restart of a deposed leader | Supervisor | Supervisor | Supervisor | Supervisor, one controller per failure domain | Required of the deployment |
| Clients find the leader | URL list, `--no-hostname_lookup` | URL list, overlay DNS names | URL list, `--hostname` or `--no-hostname_lookup`, `--advertise_ip` behind NAT | Stable DNS names, `--hostname` in certificate SANs | Supported (Go and Python SDKs) |
| Running tasks survive failover | `failover_timeout` above the leaderless window | Same | Same | Same | Supported |
| etcd survives a member loss | N/A (one host) | Three members, one per controller host | Three members across networks, local `etcd gateway` (unexercised) | Three or five members across failure domains | Partial: one endpoint per client (#14) |
| Election path encrypted and authenticated | N/A (loopback) | Overlay encryption, port firewalled to the overlay | Overlay or private network only | TLS and client authentication to etcd | Not supported (#14) |
| Leader tolerates a dropped keepalive | Not needed | Rarely needed | Needed | Needed | Not supported (#13) |
| Registry state across failover | Local `leveldb` | Local `leveldb`, reapplied | Local `leveldb`, reapplied | Declarative configuration, reapplied | By design |

## See Also

- [architecture.md](architecture.md#leader-election-etcd): leader election and the controller restart contract
- [medios-security.md](medios-security.md): security deployment modes that each HA tier runs inside
- [scheduler-http-api.md](scheduler-http-api.md): `SUBSCRIBE`, `RECONCILE` and `failover_timeout`
- [TLS.md](TLS.md): certificate authority setup and SAN rules
