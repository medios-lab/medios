// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Multi-framework contention simulation (#363): the real hierarchical
// allocator driven by scripted framework behavior models under a
// paused clock. Offer *sharing* across frameworks is the designed
// behavior; these scenarios assert its dynamics — no starvation,
// suppress/revive and decline-filter interplay, and gang grants
// staying atomic under concurrent traffic — over many allocation
// cycles, deterministically.
//
// Model shapes: a services framework that places long-running tasks
// and suppresses when satisfied, a bursty batch framework whose tasks
// finish and resubmit, and a kernel-per-task framework launching a
// stream of small short-lived tasks.

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/resources.hpp>

#include <medios/allocator/allocator.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gtest.hpp>

#include <stout/duration.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>

#include "controller/allocator/medios/hierarchical.hpp"

#include "tests/allocator.hpp"
#include "tests/medios.hpp"

using medios::allocator::Allocator;
using medios::allocator::Options;

using medios::internal::controller::allocator::HierarchicalDRFAllocator;

using process::Clock;

using std::mutex;
using std::shared_ptr;
using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace tests {

class ContentionSim;


// One node's worth of offered resources, as delivered to a framework.
struct SimOffer
{
  FrameworkID frameworkId;
  string role;
  NodeID nodeId;
  Resources resources; // Allocated to `role`.
};


// A framework behavior model. `onOffer` must consume the offer: place
// tasks and/or return the remainder (the sim recovers anything the
// model does not explicitly hold). `onCycle` runs once per allocation
// cycle before offers are dispatched.
class Model
{
public:
  virtual ~Model() = default;

  virtual void onCycle(ContentionSim&, size_t) {}
  virtual void onOffer(ContentionSim&, const SimOffer&) = 0;

  FrameworkID frameworkId;
  string role;
};


// The simulation: real HierarchicalDRFAllocator, paused clock, one
// `pump()` per allocation cycle. Deterministic: offers are drained
// only after Clock::settle().
class ContentionSim
{
public:
  ContentionSim()
    : allocator(createAllocator<HierarchicalDRFAllocator>()),
      interval(Seconds(1)),
      cycle_(0),
      nextNodeId(0) {}

  ~ContentionSim()
  {
    delete allocator;
  }

  void initialize()
  {
    Options options;
    options.allocationInterval = interval;

    allocator->initialize(
        options,
        [this](
            const FrameworkID& frameworkId,
            const hashmap<string, hashmap<NodeID, Resources>>& resources) {
          std::lock_guard<mutex> lock(offersMutex);

          size_t nodes = 0;
          foreachkey (const string& role, resources) {
            foreachpair (
                const NodeID& nodeId,
                const Resources& offered,
                resources.at(role)) {
              pendingOffers.push_back(
                  SimOffer{frameworkId, role, nodeId, offered});
              nodes++;
            }
          }

          // Callback granularity: one entry per offerCallback
          // invocation — the atomicity boundary a gang grant must
          // respect (one event, all members).
          callbackSizes.push_back(std::make_pair(frameworkId, nodes));
        },
        {});
  }

  void addNodes(size_t count, const string& resources)
  {
    for (size_t i = 0; i < count; i++) {
      NodeInfo node;
      *(node.mutable_resources()) =
        CHECK_NOTERROR(Resources::parse(resources));
      node.mutable_id()->set_value("node-" + stringify(nextNodeId++));
      node.set_hostname(node.id().value());

      allocator->addNode(
          node.id(), node, node::NODE_CAPABILITIES(), None(), node.resources(), {});
    }

    Clock::settle();
  }

  void addFramework(const string& name, const string& role, Model* model)
  {
    FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
    frameworkInfo.set_name(name);
    frameworkInfo.mutable_id()->set_value(name);
    frameworkInfo.clear_roles();
    frameworkInfo.add_roles(role);

    model->frameworkId = frameworkInfo.id();
    model->role = role;
    models[frameworkInfo.id()] = model;

    allocator->addFramework(frameworkInfo.id(), frameworkInfo, {}, true, {});

    Clock::settle();
  }

  // Runs `cycles` allocation cycles.
  void run(size_t cycles)
  {
    for (size_t i = 0; i < cycles; i++) {
      pump();
    }
  }

  size_t cycle() const { return cycle_; }

  // --- Actions available to models. ---

  // Holds `shape` out of the offer for `holdCycles` cycles (SIZE_MAX =
  // forever); the shape's resources are recovered when the hold ends.
  // Returns false if the offer cannot cover the shape.
  bool launch(
      const SimOffer& offer,
      const Resources& shape,
      size_t holdCycles)
  {
    Resources remaining = offer.resources - consumed[key(offer)];
    remaining.unallocate();

    if (!remaining.contains(shape)) {
      return false;
    }

    Resources allocated = shape;
    allocated.allocate(offer.role);

    consumed[key(offer)] += allocated;

    // Mirror the controller's launch bookkeeping: the held shape
    // moves from offered to allocated, so its eventual recovery (task
    // finish) is an allocated-resource recovery.
    allocator->transitionOfferedToAllocated(offer.nodeId, allocated);

    running.push_back(RunningTask{
        offer.frameworkId,
        offer.nodeId,
        allocated,
        holdCycles == SIZE_MAX ? SIZE_MAX : cycle_ + holdCycles});

    return true;
  }

  // Declines the (remainder of the) offer with an optional refuse
  // filter. Called automatically by the pump for anything a model
  // does not consume; call it explicitly to attach a filter.
  void decline(const SimOffer& offer, const Option<Duration>& refuse = None())
  {
    Option<Filters> filters = None();
    if (refuse.isSome()) {
      Filters f;
      f.set_refuse_seconds(refuse->secs());
      filters = f;
    }

    Resources remainder = offer.resources - consumed[key(offer)];

    if (!remainder.empty()) {
      allocator->recoverResources(
          offer.frameworkId, offer.nodeId, remainder, filters, false);
    }

    consumed[key(offer)] = offer.resources; // Fully handled.
  }

  void suppress(const FrameworkID& frameworkId, const string& role)
  {
    allocator->suppressOffers(frameworkId, {role});
  }

  void revive(const FrameworkID& frameworkId, const string& role)
  {
    allocator->reviveOffers(frameworkId, {role});
  }

  // Terminates one running task of the framework (if any); its
  // resources return to the pool this cycle.
  bool killOne(const FrameworkID& frameworkId)
  {
    for (RunningTask& task : running) {
      if (task.frameworkId == frameworkId && task.endCycle != 0) {
        task.endCycle = 0; // Reap on next pump.
        return true;
      }
    }
    return false;
  }

  size_t runningCount(const FrameworkID& frameworkId) const
  {
    size_t count = 0;
    for (const RunningTask& task : running) {
      if (task.frameworkId == frameworkId) {
        count++;
      }
    }
    return count;
  }

  Allocator* allocator;

  // Callback-granularity log: (framework, node count) per
  // offerCallback invocation, in arrival order.
  vector<std::pair<FrameworkID, size_t>> callbackSizes;

private:
  struct RunningTask
  {
    FrameworkID frameworkId;
    NodeID nodeId;
    Resources resources; // Allocated.
    size_t endCycle;     // SIZE_MAX = holds forever.
  };

  static string key(const SimOffer& offer)
  {
    return offer.frameworkId.value() + "|" + offer.nodeId.value();
  }

  void pump()
  {
    cycle_++;

    // Reap finished tasks first so their resources are in this
    // cycle's allocatable pool.
    for (auto it = running.begin(); it != running.end();) {
      if (it->endCycle <= cycle_ - 1) {
        allocator->recoverResources(
            it->frameworkId, it->nodeId, it->resources, None(), true);
        it = running.erase(it);
      } else {
        ++it;
      }
    }

    foreachpair (const FrameworkID& id, Model* model, models) {
      (void) id;
      model->onCycle(*this, cycle_);
    }

    Clock::advance(interval);
    Clock::settle();

    vector<SimOffer> offers;
    {
      std::lock_guard<mutex> lock(offersMutex);
      offers.swap(pendingOffers);
    }

    for (const SimOffer& offer : offers) {
      consumed[key(offer)] = Resources();

      Option<Model*> model = models.get(offer.frameworkId);
      if (model.isSome()) {
        (*model)->onOffer(*this, offer);
      }

      // Recover whatever the model did not hold or decline.
      decline(offer);
      consumed.erase(key(offer));
    }
  }

  Duration interval;
  size_t cycle_;
  size_t nextNodeId;

  mutex offersMutex;
  vector<SimOffer> pendingOffers;

  hashmap<FrameworkID, Model*> models;
  hashmap<string, Resources> consumed;
  vector<RunningTask> running;
};


// Services-shaped: places `target` long-running tasks, then suppresses.
// Revives (via the scenario calling killOne + markDegraded) when a
// task dies.
class ServiceModel : public Model
{
public:
  ServiceModel(const Resources& _shape, size_t _target)
    : shape(_shape), target(_target), suppressed(false) {}

  void onCycle(ContentionSim& sim, size_t) override
  {
    const size_t running = sim.runningCount(frameworkId);

    if (running < target && suppressed) {
      sim.revive(frameworkId, role);
      suppressed = false;
    }

    if (running == target && !suppressed) {
      sim.suppress(frameworkId, role);
      suppressed = true;
    }
  }

  void onOffer(ContentionSim& sim, const SimOffer& offer) override
  {
    while (sim.runningCount(frameworkId) < target &&
           sim.launch(offer, shape, SIZE_MAX)) {}
  }

  Resources shape;
  size_t target;
  bool suppressed;
};


// Batch-shaped: keeps `backlog` jobs queued; each placed job holds
// its resources for `holdCycles` and then finishes. Tracks the worst
// queue wait in cycles.
class BatchModel : public Model
{
public:
  BatchModel(const Resources& _shape, size_t _backlog, size_t _holdCycles)
    : shape(_shape),
      backlog(_backlog),
      holdCycles(_holdCycles),
      queuedSince(),
      completed(0),
      maxWaitCycles(0) {}

  void onCycle(ContentionSim& sim, size_t cycle) override
  {
    while (queuedSince.size() + sim.runningCount(frameworkId) < backlog) {
      queuedSince.push_back(cycle);
    }
  }

  void onOffer(ContentionSim& sim, const SimOffer& offer) override
  {
    while (!queuedSince.empty() && sim.launch(offer, shape, holdCycles)) {
      const size_t wait = sim.cycle() - queuedSince.front();
      maxWaitCycles = std::max(maxWaitCycles, wait);
      queuedSince.erase(queuedSince.begin());
      completed++; // Counted at placement; the hold finishes by itself.
    }
  }

  Resources shape;
  size_t backlog;
  size_t holdCycles;
  vector<size_t> queuedSince;
  size_t completed;
  size_t maxWaitCycles;
};


// A framework that declines everything with a refuse filter until
// `acceptAfter`, then places a single task. Exercises decline-filter
// expiry: filtering must not turn into permanent starvation.
class PickyModel : public Model
{
public:
  PickyModel(
      const Resources& _shape,
      size_t _acceptAfter,
      const Duration& _refuse)
    : shape(_shape),
      acceptAfter(_acceptAfter),
      refuse(_refuse),
      placedAtCycle(0) {}

  void onOffer(ContentionSim& sim, const SimOffer& offer) override
  {
    if (sim.cycle() < acceptAfter || placedAtCycle != 0) {
      sim.decline(offer, refuse);
      return;
    }

    if (sim.launch(offer, shape, SIZE_MAX)) {
      placedAtCycle = sim.cycle();
    }
  }

  Resources shape;
  size_t acceptAfter;
  Duration refuse;
  size_t placedAtCycle;
};


class FrameworkContentionSim : public MediosTest {};


// A services framework occupying most of the cluster does not starve
// a bursty batch framework: batch jobs keep placing and finishing,
// and their worst queue wait stays bounded.
TEST_F(FrameworkContentionSim, ServicesDoNotStarveBatch)
{
  Clock::pause();

  ContentionSim sim;
  sim.initialize();
  sim.addNodes(10, "cpus:8;mem:16384;disk:10000");

  // Services take ~60% of the cluster (48 of 80 cpus), then suppress.
  ServiceModel services(
      CHECK_NOTERROR(Resources::parse("cpus:4;mem:8192")), 12);
  BatchModel batch(
      CHECK_NOTERROR(Resources::parse("cpus:2;mem:2048")), 8, 3);

  sim.addFramework("services", "services", &services);
  sim.addFramework("batch", "batch", &batch);

  sim.run(100);

  EXPECT_EQ(12u, sim.runningCount(services.frameworkId));
  EXPECT_TRUE(services.suppressed);

  // Batch churned continuously: ~8 concurrent jobs finishing every 3
  // cycles over ~100 cycles must complete well over 100 jobs.
  EXPECT_GT(batch.completed, 100u)
    << "batch starved: only " << batch.completed << " jobs placed";

  // And no job waited pathologically long for placement.
  EXPECT_LE(batch.maxWaitCycles, 10u)
    << "batch max wait " << batch.maxWaitCycles << " cycles";
}


// Suppression is load-bearing for sharing: while the services
// framework is suppressed, batch receives the entire offer stream;
// when a service task dies, revive re-places it promptly.
TEST_F(FrameworkContentionSim, SuppressReviveInterplay)
{
  Clock::pause();

  ContentionSim sim;
  sim.initialize();
  sim.addNodes(4, "cpus:8;mem:16384;disk:10000");

  ServiceModel services(
      CHECK_NOTERROR(Resources::parse("cpus:2;mem:4096")), 4);
  BatchModel batch(
      CHECK_NOTERROR(Resources::parse("cpus:1;mem:1024")), 4, 2);

  sim.addFramework("services", "services", &services);
  sim.addFramework("batch", "batch", &batch);

  sim.run(20);

  ASSERT_EQ(4u, sim.runningCount(services.frameworkId));
  ASSERT_TRUE(services.suppressed);

  // While suppressed, services receive no offers at all.
  const size_t callbacksBefore = sim.callbackSizes.size();
  sim.run(20);

  for (size_t i = callbacksBefore; i < sim.callbackSizes.size(); i++) {
    EXPECT_NE(services.frameworkId, sim.callbackSizes[i].first)
      << "suppressed framework received an offer";
  }

  // Kill one service task: the model revives and re-places within a
  // few cycles.
  ASSERT_TRUE(sim.killOne(services.frameworkId));

  const size_t killedAt = sim.cycle();
  size_t replacedAt = 0;

  for (size_t i = 0; i < 10; i++) {
    sim.run(1);
    if (sim.runningCount(services.frameworkId) == 4) {
      replacedAt = sim.cycle();
      break;
    }
  }

  ASSERT_NE(0u, replacedAt) << "service task not re-placed within 10 cycles";
  EXPECT_LE(replacedAt - killedAt, 5u);
}


// Decline filters time out rather than starve: a framework that
// refused everything for a while still gets offers again once its
// filters expire, and places its task.
TEST_F(FrameworkContentionSim, DeclineFiltersExpireNotStarve)
{
  Clock::pause();

  ContentionSim sim;
  sim.initialize();
  sim.addNodes(2, "cpus:4;mem:8192;disk:1000");

  // Picky declines with a 5s refuse filter (5 cycles at 1s/cycle)
  // until cycle 20, then wants to place one task.
  PickyModel picky(
      CHECK_NOTERROR(Resources::parse("cpus:1;mem:1024")), 20, Seconds(5));
  BatchModel batch(
      CHECK_NOTERROR(Resources::parse("cpus:1;mem:1024")), 2, 2);

  sim.addFramework("picky", "picky", &picky);
  sim.addFramework("batch", "batch", &batch);

  sim.run(40);

  ASSERT_NE(0u, picky.placedAtCycle)
    << "picky framework never placed after its filters should have expired";

  // Placement happens within filter-expiry + a few cycles of becoming
  // willing (cycle 20), not at the end of the run.
  EXPECT_LE(picky.placedAtCycle, 30u)
    << "placed only at cycle " << picky.placedAtCycle;
}


// Kernel-per-task churn under load: with services holding ~70% of the
// cluster, a stream of small one-cycle kernels all place promptly.
TEST_F(FrameworkContentionSim, KernelChurnUnderServiceLoad)
{
  Clock::pause();

  ContentionSim sim;
  sim.initialize();
  sim.addNodes(5, "cpus:8;mem:16384;disk:10000");

  // 28 of 40 cpus held by services.
  ServiceModel services(
      CHECK_NOTERROR(Resources::parse("cpus:4;mem:8192")), 7);

  // Kernels: 1-cycle lifetime, small shape, always one queued.
  BatchModel kernels(
      CHECK_NOTERROR(Resources::parse("cpus:0.5;mem:512")), 1, 1);

  sim.addFramework("services", "services", &services);
  sim.addFramework("kernels", "kernels", &kernels);

  sim.run(100);

  EXPECT_GT(kernels.completed, 40u)
    << "kernel churn stalled: " << kernels.completed << " placed";
  EXPECT_LE(kernels.maxWaitCycles, 5u)
    << "kernel max wait " << kernels.maxWaitCycles;
}


// A gang REQUEST granted amid concurrent batch traffic arrives as one
// atomic callback carrying every member on distinct nodes, and the
// batch framework's flow is undisturbed.
TEST_F(FrameworkContentionSim, GangGrantAtomicUnderContention)
{
  Clock::pause();

  ContentionSim sim;
  sim.initialize();
  sim.addNodes(6, "cpus:8;mem:16384;disk:10000");

  BatchModel batch(
      CHECK_NOTERROR(Resources::parse("cpus:2;mem:2048")), 6, 2);
  sim.addFramework("batch", "batch", &batch);

  // The gang framework declines all cycle offers with a long refuse
  // filter — after a few cycles it stops receiving cycle traffic
  // entirely. The REQUEST grant still arrives: an explicit request
  // deliberately overrides decline filters (#354 semantics).
  class DecliningModel : public Model
  {
  public:
    void onOffer(ContentionSim& sim, const SimOffer& offer) override
    {
      sim.decline(offer, Seconds(1000));
    }
  };

  DecliningModel gang;
  sim.addFramework("gang", "train", &gang);

  // Let the gang framework's decline filters cover every node, then
  // confirm its cycle traffic has gone quiet.
  sim.run(10);

  const size_t quietMark = sim.callbackSizes.size();
  sim.run(5);

  for (size_t i = quietMark; i < sim.callbackSizes.size(); i++) {
    ASSERT_NE(gang.frameworkId, sim.callbackSizes[i].first)
      << "gang framework still receiving cycle offers; test premise broken";
  }

  const size_t grantMark = sim.callbackSizes.size();
  const size_t completedBefore = batch.completed;

  // Request a 3-member gang.
  Request member;
  *(member.mutable_resources()) =
    static_cast<const google::protobuf::RepeatedPtrField<Resource>&>(
        CHECK_NOTERROR(Resources::parse("cpus:4;mem:4096")));

  sim.allocator->requestResources(
      gang.frameworkId, {member, member, member});

  sim.run(5);

  // Find the gang framework's callbacks after the request: exactly
  // one, carrying all three members (the atomic grant).
  size_t gangCallbacks = 0;
  size_t gangNodes = 0;

  for (size_t i = grantMark; i < sim.callbackSizes.size(); i++) {
    if (sim.callbackSizes[i].first == gang.frameworkId) {
      gangCallbacks++;
      gangNodes = sim.callbackSizes[i].second;
    }
  }

  EXPECT_EQ(1u, gangCallbacks);
  EXPECT_EQ(3u, gangNodes) << "gang grant was not atomic";

  // Batch kept churning through the grant.
  sim.run(10);
  EXPECT_GT(batch.completed, completedBefore + 10);
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
