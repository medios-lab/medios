"""medios-try: install and run a one-machine Medios preview in user space.

Every subcommand prints one JSON object on stdout and exits 0 on success,
non-zero on failure. Run it through bin/medios-try, which sets the paths below.

  install [--archive PATH]   download (or take) the pinned release and verify it
  up [--resources SPEC]      start a controller and one node, wait for registration
  status                     report processes, controller and registered nodes
  down                       stop the controller and node
  diagnose                   write logs and machine facts to one archive for an issue
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
from pathlib import Path

HOME = Path(os.environ["MEDIOS_TRY_HOME"])
BUNDLE = Path(os.environ["MEDIOS_TRY_BUNDLE"])
INSTALL = HOME / "medios"
LOGS = HOME / "logs"
RUN = HOME / "run.json"

# Heuristic: a loopback node registered in about 4 s on an M-series laptop;
# 60 s leaves room for slow disks and first-run dylib loading.
REGISTRATION_TIMEOUT_S = 60
# Heuristic: time for SIGTERM before SIGKILL on `down`.
STOP_GRACE_S = 10
# Default: first port tried for the controller; the node takes the next free one.
FIRST_PORT = 5050
# Heuristic: a socket read that stalls this long fails the download with JSON
# instead of hanging; a slow but moving download is not affected.
DOWNLOAD_STALL_TIMEOUT_S = 60


def emit(ok: bool, **fields) -> None:
    print(json.dumps({"ok": ok, **fields}))
    sys.exit(0 if ok else 1)


def platform_key() -> str:
    system, machine = platform.system(), platform.machine()
    key = {("Darwin", "arm64"): "darwin-arm64",
           ("Linux", "x86_64"): "linux-amd64",
           ("Linux", "aarch64"): "linux-arm64"}.get((system, machine))
    if key is None:
        emit(False, step="platform", error=f"unsupported platform {system}-{machine}")
    return key


def bundle_revision() -> str:
    p = BUNDLE / "REVISION"
    return p.read_text().strip() if p.exists() else "unpackaged"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def bin_dir() -> Path:
    marker = INSTALL / "current"
    if not marker.exists():
        emit(False, step="install", error="Medios is not installed; run `bin/medios-try install` first")
    return Path(marker.read_text().strip()) / "bin"


def tail(path: Path, lines: int = 30) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(errors="replace").splitlines()[-lines:]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def free_port(start: int, taken: set[int]) -> int:
    port = start
    while port < start + 200:
        if port not in taken:
            with socket.socket() as s:
                try:
                    s.bind(("127.0.0.1", port))
                    return port
                except OSError:
                    pass
        port += 1
    emit(False, step="ports", error=f"no free port in {start}-{start + 199}")


def get_json(url: str, timeout: float = 3.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def read_run() -> dict | None:
    return json.loads(RUN.read_text()) if RUN.exists() else None


# ---- subcommands -----------------------------------------------------------

def cmd_install(args) -> None:
    release = json.loads((BUNDLE / "release.json").read_text())
    key = platform_key()
    entry = release["archives"].get(key)
    if entry is None:
        emit(False, step="install", error=f"release {release['medios_version']} has no archive for {key}")
    INSTALL.mkdir(parents=True, exist_ok=True)
    archive = INSTALL / entry["name"]
    if args.archive:
        shutil.copyfile(args.archive, archive)
        source = str(Path(args.archive).resolve())
    else:
        source = f"{release['download_base']}/{entry['name']}"
        try:
            with urllib.request.urlopen(source, timeout=DOWNLOAD_STALL_TIMEOUT_S) as r, \
                    archive.open("wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)
        except (urllib.error.URLError, OSError) as e:
            emit(False, step="download", url=source, error=str(e))
    got = sha256(archive)
    if got != entry["sha256"]:
        archive.unlink()
        emit(False, step="verify", error=f"sha256 {got} does not match pinned {entry['sha256']}")
    with tarfile.open(archive) as t:
        t.extractall(INSTALL, filter="data")
    root = INSTALL / entry["name"].removesuffix(".tar.gz")
    archive.unlink()
    (INSTALL / "current").write_text(str(root))
    version = subprocess.run([str(root / "bin" / "medios-controller"), "--version"],
                             capture_output=True, text=True)
    emit(True, step="install", medios_version=release["medios_version"], platform=key,
         source=source, sha256=got, bin=str(root / "bin"),
         controller_version=(version.stdout or version.stderr).strip().splitlines()[:1],
         bundle_revision=bundle_revision())


def registered_nodes(url: str) -> list:
    try:
        return get_json(f"{url}/state").get("nodes", [])
    except (urllib.error.URLError, OSError, ValueError):
        return []


def stop(pid: int) -> bool:
    """SIGTERM the process group, SIGKILL after STOP_GRACE_S. True if it was alive."""
    if not alive(pid):
        return False
    os.killpg(pid, signal.SIGTERM)
    deadline = time.monotonic() + STOP_GRACE_S
    while alive(pid) and time.monotonic() < deadline:
        time.sleep(0.2)
    if alive(pid):
        os.killpg(pid, signal.SIGKILL)
    return True


def cmd_up(args) -> None:
    run = read_run()
    if run:
        both = alive(run["controller_pid"]) and alive(run["node_pid"])
        if both and registered_nodes(run["controller_url"]):
            emit(True, step="up", already_running=True, **run)
        # A previous up failed or half-exited: stop what survives before starting again.
        for name in ("node", "controller"):
            stop(run[f"{name}_pid"])
        RUN.unlink()
    bins = bin_dir()
    LOGS.mkdir(parents=True, exist_ok=True)
    cport = free_port(FIRST_PORT, set())
    nport = free_port(cport + 1, {cport})
    env = {**os.environ, "LIBPROCESS_IP": "127.0.0.1"}

    def start(name: str, argv: list[str]) -> subprocess.Popen:
        log = (LOGS / f"{name}.log").open("ab")
        return subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env,
                                start_new_session=True)

    controller = start("controller", [
        str(bins / "medios-controller"), "--ip=127.0.0.1", f"--port={cport}",
        "--registry=in_memory", f"--work_dir={HOME / 'controller'}"])
    node_argv = [
        str(bins / "medios-node"), f"--controller=127.0.0.1:{cport}", "--ip=127.0.0.1",
        f"--port={nport}", f"--work_dir={HOME / 'node'}", f"--runtime_dir={HOME / 'node-runtime'}",
        "--containerizers=medios", "--launcher=posix", "--isolation=",
        f"--launcher_dir={bins}"]
    if args.resources:
        node_argv.append(f"--resources={args.resources}")
    node = start("node", node_argv)

    url = f"http://127.0.0.1:{cport}"
    run = {"controller_url": url, "controller_port": cport, "node_port": nport,
           "controller_pid": controller.pid, "node_pid": node.pid,
           "work_dir": str(HOME), "bundle_revision": bundle_revision()}
    RUN.write_text(json.dumps(run, indent=2))

    def fail(**fields) -> None:
        # Leave nothing running behind a failed up.
        for p in (node, controller):
            stop(p.pid)
        RUN.unlink()
        emit(False, step="up", stopped=True, **fields, **run)

    deadline = time.monotonic() + REGISTRATION_TIMEOUT_S
    while time.monotonic() < deadline:
        for name, proc in (("controller", controller), ("node", node)):
            if proc.poll() is not None:
                fail(error=f"{name} exited with code {proc.returncode}",
                     log=str(LOGS / f"{name}.log"), log_tail=tail(LOGS / f"{name}.log"))
        nodes = registered_nodes(url)
        if nodes:
            n = nodes[0]
            emit(True, step="up", node_id=n.get("id"), node_resources=n.get("resources"),
                 seconds=round(REGISTRATION_TIMEOUT_S - (deadline - time.monotonic()), 1), **run)
        time.sleep(1)
    fail(error=f"node did not register within {REGISTRATION_TIMEOUT_S}s",
         controller_log_tail=tail(LOGS / "controller.log"), node_log_tail=tail(LOGS / "node.log"))


def cmd_status(args) -> None:
    run = read_run()
    if run is None:
        emit(False, step="status", running=False, error="no preview has been started; run `bin/medios-try up`")
    procs = {"controller": alive(run["controller_pid"]), "node": alive(run["node_pid"])}
    try:
        state = get_json(f"{run['controller_url']}/state")
        nodes = [{"id": n.get("id"), "active": n.get("active"), "resources": n.get("resources")}
                 for n in state.get("nodes", [])]
        frameworks = [f.get("name") for f in state.get("frameworks", [])]
        reachable = True
    except (urllib.error.URLError, OSError, ValueError) as e:
        nodes, frameworks, reachable = [], [], False
        run["controller_error"] = str(e)
    ok = all(procs.values()) and reachable and bool(nodes)
    emit(ok, step="status", processes=procs, controller_reachable=reachable,
         nodes=nodes, frameworks=frameworks, **run)


def cmd_down(args) -> None:
    run = read_run()
    if run is None:
        emit(True, step="down", stopped=[], note="nothing was running")
    stopped = [name for name in ("node", "controller") if stop(run[f"{name}_pid"])]
    RUN.unlink()
    emit(True, step="down", stopped=stopped)


def machine_facts() -> dict:
    facts = {"system": platform.system(), "release": platform.release(),
             "machine": platform.machine(), "cpus": os.cpu_count(),
             "python": platform.python_version(),
             "disk_free_gb": round(shutil.disk_usage(HOME).free / 1e9, 1)}
    if platform.system() == "Darwin":
        facts["macos"] = platform.mac_ver()[0]
    return facts


def cmd_diagnose(args) -> None:
    HOME.mkdir(parents=True, exist_ok=True)
    out = HOME / f"medios-try-diagnose-{time.strftime('%Y%m%dT%H%M%S')}.tar.gz"
    facts = {"machine": machine_facts(), "bundle_revision": bundle_revision(),
             "run": read_run()}
    facts_file = HOME / "diagnose-facts.json"
    facts_file.write_text(json.dumps(facts, indent=2))
    with tarfile.open(out, "w:gz") as t:
        t.add(facts_file, arcname="facts.json")
        t.add(BUNDLE / "release.json", arcname="release.json")
        if LOGS.exists():
            t.add(LOGS, arcname="logs")
        marker = INSTALL / "current"
        if marker.exists():
            build = Path(marker.read_text().strip()) / "medios-build.json"
            if build.exists():
                t.add(build, arcname="medios-build.json")
    facts_file.unlink()
    emit(True, step="diagnose", archive=str(out), **facts)


def main() -> None:
    p = argparse.ArgumentParser(prog="medios-try", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("install")
    i.add_argument("--archive", help="install from this local archive instead of downloading it")
    u = sub.add_parser("up")
    u.add_argument("--resources", help="node resources, e.g. 'cpus:2;mem:1024'; default: detected")
    for name in ("status", "down", "diagnose"):
        sub.add_parser(name)
    args = p.parse_args()
    {"install": cmd_install, "up": cmd_up, "status": cmd_status,
     "down": cmd_down, "diagnose": cmd_diagnose}[args.cmd](args)


if __name__ == "__main__":
    main()
