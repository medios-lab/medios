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

#ifndef __MEDIOS_ALLOCATOR_ALLOCATOR_HPP__
#define __MEDIOS_ALLOCATOR_ALLOCATOR_HPP__

#include <string>
#include <vector>

// ONLY USEFUL AFTER RUNNING PROTOC.
#include <medios/allocator/allocator.pb.h>
#include <medios/scheduler/scheduler.pb.h>

#include <medios/authorizer/authorizer.hpp>
#include <medios/maintenance/maintenance.hpp>
#include <medios/quota/quota.hpp>
#include <medios/resource_quantities.hpp>
#include <medios/resources.hpp>

#include <process/future.hpp>

#include <stout/duration.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>


namespace medios {
namespace allocator {

constexpr Duration DEFAULT_ALLOCATOR_RECOVERY_TIMEOUT = Minutes(10);
constexpr double DEFAULT_ALLOCATOR_NODE_RECOVERY_FACTOR = 0.80;


/**
 *  Pass in configuration to the allocator.
 */
struct Options
{
  Duration allocationInterval = Seconds(1);

  // Resources (by name) that will be excluded from a role's fair share.
  Option<std::set<std::string>> fairnessExcludeResourceNames = None();

  // Cluster policy: offer nodes with a `gpus` resource only to frameworks
  // that declare the `GPU_RESOURCES` capability. Off unless the operator
  // turns it on (FAC3-0028).
  bool filterGpuResources = false;

  // Cluster policy: offer shared resources only to frameworks that declare
  // the `SHARED_RESOURCES` capability. Off unless the operator turns it
  // on (FAC3-0028); a framework that does not list a shared resource on a
  // task does not hold it (DR-036).
  bool filterSharedResources = false;

  // The controller's domain, if any.
  Option<DomainInfo> domain = None();

  // The minimum allocatable resource quantities, if any.
  Option<std::vector<ResourceQuantities>> minAllocatableResources = None();

  size_t maxCompletedFrameworks = 0;

  bool publishPerFrameworkMetrics = true;

  // Authentication realm for HTTP debug endpoints exposed by the allocator.
  Option<std::string> readonlyHttpAuthenticationRealm;

  // Medios controller's authorizer.
  Option<::medios::Authorizer*> authorizer;

  // Recovery options
  Duration recoveryTimeout = DEFAULT_ALLOCATOR_RECOVERY_TIMEOUT;
  double nodeRecoveryFactor = DEFAULT_ALLOCATOR_NODE_RECOVERY_FACTOR;
};


namespace internal {
class OfferConstraintsFilterImpl;

} // namespace internal {


class OfferConstraintsFilter
{
public:
  // TODO(asekretenko): Given that most dependants of the public allocator
  // interface do not care about filter creation, we should consider decoupling
  // the filter construction interface (which at this point consists of the
  // `struct Options` and the `create()` method) from the allocator interface.
  struct Options
  {
    struct RE2Limits
    {
      Bytes maxMem;
      int maxProgramSize;
    };

    RE2Limits re2Limits;
  };

  static Try<OfferConstraintsFilter> create(
      const Options& options,
      scheduler::OfferConstraints&& constraints);

  /*
   * Constructs a no-op filter that does not exclude any nodes/resources from
   * being offered. This is equivalent to passing default-constructed
   * `OfferConstraints` to the factory method `create()`.
   */
  OfferConstraintsFilter();

  // Definitions of these need `OfferConstraintsFilterImpl` to be a complete
  // type.
  OfferConstraintsFilter(OfferConstraintsFilter&&);
  OfferConstraintsFilter& operator=(OfferConstraintsFilter&&);
  ~OfferConstraintsFilter();

  /**
   * Returns `true` if the allocator is allowed to offer resoureces
   * on the node to the framework's role, and `false` otherwise.
   */
  bool isNodeExcluded(
      const std::string& role,
      const NodeInfo& nodeInfo) const;

  // TODO(asekretenko): Add a method for filtering `Resources` on a node.

private:
  std::unique_ptr<internal::OfferConstraintsFilterImpl> impl;

  OfferConstraintsFilter(internal::OfferConstraintsFilterImpl&& impl_);
};


/**
 * Per-framework allocator-specific options that are not part of
 * `FrameworkInfo`.
 */
struct FrameworkOptions
{
  /**
   * The set of roles for which the allocator should not generate offers.
   */
  std::set<std::string> suppressedRoles;

  /**
   * The internal representation of framework's offer constraints.
   */
  OfferConstraintsFilter offerConstraintsFilter;
};


/**
 * Basic model of an allocator: resources are allocated to a framework
 * in the form of offers. A framework can refuse some resources in
 * offers and run tasks in others. Allocated resources can have offer
 * operations applied to them in order for frameworks to alter the
 * resource metadata (e.g. creating persistent volumes). Resources can
 * be recovered from a framework when tasks finish/fail (or are lost
 * due to a node failure) or when an offer is rescinded.
 *
 * This is the public API for resource allocators.
 */
class Allocator
{
public:
  /**
   * Attempts either to create a built-in DRF allocator or to load an
   * allocator instance from a module using the given name. If `Try`
   * does not report an error, the wrapped `Allocator*` is not null.
   *
   * TODO(bmahler): Figure out how to pass parameters without
   * burning in the built-in module arguments.
   *
   * @param name Name of the allocator.
   */
  static Try<Allocator*> create(
      const std::string& name,
      const std::string& roleSorter,
      const std::string& frameworkSorter);

  Allocator() {}

  virtual ~Allocator() {}

  /**
   * Initializes the allocator when the controller starts up. Any errors in
   * initialization should fail fast and result in an ABORT. The controller expects
   * the allocator to be successfully initialized if this call returns.
   *
   * @param allocationInterval The allocate interval for the allocator, it
   *     determines how often the allocator should perform the batch
   *     allocation. An allocator may also perform allocation based on events
   *     (a framework is added and so on), this depends on the implementation.
   * @param offerCallback A callback the allocator uses to send allocations
   *     to the frameworks.
   * @param inverseOfferCallback A callback the allocator uses to send reclaim
   *     allocations from the frameworks.
   */
  virtual void initialize(
      const Options& options,
      const lambda::function<
          void(const FrameworkID&,
               const hashmap<std::string, hashmap<NodeID, Resources>>&)>&
                   offerCallback,
      const lambda::function<
          void(const FrameworkID&,
               const hashmap<NodeID, UnavailableResources>&)>&
        inverseOfferCallback) = 0;

  /**
   * Informs the allocator of the recovered state from the controller.
   *
   * Because it is hard to define recovery for a running allocator, this
   * method should be called after `initialize()`, but before actual
   * allocation starts (i.e. `addNode()` is called).
   *
   * TODO(alexr): Consider extending the signature with expected
   * frameworks count once it is available upon the controller failover.
   *
   * @param quotas A (possibly empty) collection of quotas, keyed by
   *     their role, known to the controller.
   */
  virtual void recover(
      const int expectedNodeCount,
      const hashmap<std::string, Quota>& quotas) = 0;

  /**
   * Adds a framework to the Medios cluster. The allocator is invoked when
   * a new framework joins the Medios cluster and is entitled to participate
   * in resource sharing.
   *
   * @param used Resources used by this framework. The allocator should
   *     account for these resources when updating the allocation of this
   *     framework. The allocator should avoid double accounting when yet
   *     unknown nodes are added later in `addNode()`.
   *
   * @param active Whether the framework is initially activated.
   *
   * @param options Initial FrameworkOptions of this framework.
   */
  virtual void addFramework(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      const hashmap<NodeID, Resources>& used,
      bool active,
      FrameworkOptions&& options) = 0;

  /**
   * Removes a framework from the Medios cluster. It is up to an allocator to
   * decide what to do with framework's resources. For example, they may be
   * released and added back to the shared pool of resources.
   */
  virtual void removeFramework(
      const FrameworkID& frameworkId) = 0;

  /**
   * Activates a framework in the Medios cluster.
   * Offers are only sent to active frameworks.
   */
  virtual void activateFramework(
      const FrameworkID& frameworkId) = 0;

   /**
   * Deactivates a framework in the Medios cluster.
   * Resource offers are not sent to deactivated frameworks.
   */
  virtual void deactivateFramework(
      const FrameworkID& frameworkId) = 0;

  /**
   * Updates FrameworkInfo and FrameworkOptions.
   *
   * NOTE: Effective framework options can also be changed via other methods.
   * For example, `suppress()` and `revive()` change the suppression state
   * of framework's roles.
   */
  virtual void updateFramework(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      FrameworkOptions&& options) = 0;

  /**
   * Adds or re-adds a node to the Medios cluster. It is invoked when a
   * new node joins the cluster or in case of node recovery.
   *
   * @param nodeId ID of the node to be added or re-added.
   * @param nodeInfo Detailed info of the node. The nodeInfo resources
   *     correspond directly to the static --resources flag value on the node.
   * @param capabilities Capabilities of the node.
   * @param total The `total` resources are passed explicitly because it
   *     includes resources that are dynamically "checkpointed" on the node
   *     (e.g. persistent volumes, dynamic reservations, etc).
   * @param used Resources that are allocated on the current node. The
   *     allocator should avoid double accounting when yet unknown frameworks
   *     are added later in `addFramework()`.
   *
   * TODO(asekretenko): Ideally, to get rig of an intermediate allocator state
   * when some resources are used by nonexistent frameworks, we should change
   * the interface so that per-node per-framework used resources and the
   * not yet known frameworks that are using them are added atomically.
   */
  virtual void addNode(
      const NodeID& nodeId,
      const NodeInfo& nodeInfo,
      const std::vector<NodeInfo::Capability>& capabilities,
      const Option<Unavailability>& unavailability,
      const Resources& total,
      const hashmap<FrameworkID, Resources>& used) = 0;

  /**
   * Removes a node from the Medios cluster. All resources belonging to this
   * node should be released by the allocator.
   */
  virtual void removeNode(
      const NodeID& nodeId) = 0;

  /**
   * Updates a node.
   *
   * TODO(bevers): Make `total` and `capabilities` non-optional.
   *
   * @param nodeInfo The current node info of the node.
   * @param total The new total resources on the node.
   * @param capabilities The new capabilities of the node.
   */
  virtual void updateNode(
      const NodeID& node,
      const NodeInfo& nodeInfo,
      const Option<Resources>& total = None(),
      const Option<std::vector<NodeInfo::Capability>>&
          capabilities = None()) = 0;

  /**
   * Add resources from a local resource provider to a node.
   *
   * @param node Id of the node to modify.
   * @param total The resources to add to the node's total resources.
   * @param used The resources to add to the resources tracked as used
   *     for this node.
   */
  virtual void addResourceProvider(
      const NodeID& node,
      const Resources& total,
      const hashmap<FrameworkID, Resources>& used) = 0;

  /**
   * Activates a node. This is invoked when a node reregisters. Offers
   * are only sent for activated nodes.
   */
  virtual void activateNode(
      const NodeID& nodeId) = 0;

  /**
   * Deactivates a node.
   *
   * This is triggered if a node disconnects from the controller. The allocator
   * should treat all offers from the deactivated node as rescinded. (There
   * is no separate call to the allocator to handle this). Resources aren't
   * "recovered" when a node deactivates because the resources are lost.
   */
  virtual void deactivateNode(
      const NodeID& nodeId) = 0;

  /**
   * Updates the list of trusted nodes.
   *
   * This is invoked when the controller starts up with the --allowlist flag.
   *
   * @param allowlist A set of nodes that are allowed to contribute
   *     their resources to the resource pool.
   */
  virtual void updateAllowlist(
      const Option<hashset<std::string>>& allowlist) = 0;

  /**
   * Requests resources for a framework.
   *
   * A framework may request resources via this call. It is up to the allocator
   * how to react to this request. For example, a request may be ignored, or
   * may influence internal priorities the allocator may keep for frameworks.
   */
  virtual void requestResources(
      const FrameworkID& frameworkId,
      const std::vector<Request>& requests) = 0;

  /**
   * Updates allocation by applying offer operations.
   *
   * This call is mainly intended to support persistence-related features
   * (dynamic reservation and persistent volumes). The allocator may react
   * differently for certain offer operations. The allocator should use this
   * call to update bookkeeping information related to the framework. The
   * `offeredResources` are the resources that the operations are applied to
   * and must be allocated to a single role.
   */
  virtual void updateAllocation(
      const FrameworkID& frameworkId,
      const NodeID& nodeId,
      const Resources& offeredResources,
      const std::vector<ResourceConversion>& conversions) = 0;

  /**
   * Updates available resources on a node based on a sequence of offer
   * operations. Operations may include reserve, unreserve, create or destroy.
   *
   * @param nodeId ID of the node.
   * @param operations The offer operations to apply to this node's resources.
   */
  virtual process::Future<Nothing> updateAvailable(
      const NodeID& nodeId,
      const std::vector<Offer::Operation>& operations) = 0;

  /**
   * Updates unavailability for a node.
   *
   * We currently support storing the next unavailability, if there is one,
   * per node. If `unavailability` is not set then there is no known upcoming
   * unavailability. This might require the implementation of the function to
   * remove any inverse offers that are outstanding.
   */
  virtual void updateUnavailability(
      const NodeID& nodeId,
      const Option<Unavailability>& unavailability) = 0;

  /**
   * Updates inverse offer.
   *
   * Informs the allocator that the inverse offer has been responded to or
   * revoked.
   *
   * @param unavailableResources The `unavailableResources` can be used by the
   *     allocator to distinguish between different inverse offers sent to the
   *     same framework for the same node.
   * @param status If `status` is not set then the inverse offer was not
   *     responded to, possibly because the offer timed out or was rescinded.
   *     This might require the implementation of the function to remove any
   *     inverse offers that are outstanding.
   * @param filters A filter attached to the inverse offer can be used by the
   *     framework to control when it wants to be contacted again with the
   *     inverse offer. The "filters" for InverseOffers are identical to the
   *     existing mechanism for re-offering Offers to frameworks.
   */
  virtual void updateInverseOffer(
      const NodeID& nodeId,
      const FrameworkID& frameworkId,
      const Option<UnavailableResources>& unavailableResources,
      const Option<InverseOfferStatus>& status,
      const Option<Filters>& filters = None()) = 0;

  /**
   * Retrieves the status of all inverse offers maintained by the allocator.
   */
  virtual process::Future<
      hashmap<NodeID,
              hashmap<FrameworkID, medios::allocator::InverseOfferStatus>>>
    getInverseOfferStatuses() = 0;

  /**
   * This method should be invoked when the offered resources has become
   * actually allocated.
   */
  virtual void transitionOfferedToAllocated(
      const NodeID& nodeId, const Resources& resources) = 0;

  /**
   * Recovers resources.
   *
   * Used to update the set of available resources for a specific node. This
   * method is invoked to inform the allocator about offered resources that
   * have been refused or allocated (i.e. used for launching tasks) resources
   * that are no longer in use. The resources will have an
   * `allocation_info.role` assigned and callers are expected to only call this
   * with resources allocated to a single role.
   *
   *
   * TODO(bmahler): We could allow resources allocated to multiple roles
   * within a single call here, but filtering them in the same way does
   * not seem desirable.
   */
  virtual void recoverResources(
      const FrameworkID& frameworkId,
      const NodeID& nodeId,
      const Resources& resources,
      const Option<Filters>& filters,
      bool isAllocated) = 0;

  /**
   * Suppresses offers.
   *
   * Informs the allocator to stop sending offers to this framework for the
   * specified roles. If `roles` is an empty set, we will stop sending offers
   * to this framework for all of the framework's subscribed roles.
   */
  virtual void suppressOffers(
      const FrameworkID& frameworkId,
      const std::set<std::string>& roles) = 0;

  /**
   * Revives offers to this framework for the specified roles. This is
   * invoked by a framework when it wishes to receive filtered resources
   * immediately or get itself out of a suppressed state. If `roles` is
   * an empty set, it is treated as being set to all of the framework's
   * subscribed roles.
   */
  virtual void reviveOffers(
      const FrameworkID& frameworkId,
      const std::set<std::string>& roles) = 0;

  /**
   * Informs the allocator to update quota for the given role.
   *
   * It is up to the allocator implementation how to satisfy quota. An
   * implementation may employ different strategies for roles with or
   * without quota. All roles have a default quota defined as `DEFAULT_QUOTA`.
   * Currently, it is no guarantees and no limits. Thus to "remove" a quota,
   * one should simply update the quota to be `DEFAULT_QUOTA`.
   */
  virtual void updateQuota(
      const std::string& role,
      const Quota& quota) = 0;

  /**
   * Updates the weight associated with one or more roles. If a role
   * was previously configured to have a weight and that role is
   * omitted from this list, it keeps its old weight.
   */
  virtual void updateWeights(
      const std::vector<WeightInfo>& weightInfos) = 0;

  /**
   * Idempotent helper to pause allocations.
   */
  virtual void pause() = 0;

  /**
   * Idempotent helper to resume allocations.
   */
  virtual void resume() = 0;
};

} // namespace allocator {
} // namespace medios {

#endif // __MEDIOS_CONTROLLER_ALLOCATOR_HPP__
