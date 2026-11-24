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

#ifndef __CONTROLLER_ALLOCATOR_MEDIOS_ALLOCATOR_HPP__
#define __CONTROLLER_ALLOCATOR_MEDIOS_ALLOCATOR_HPP__

#include <medios/allocator/allocator.hpp>

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/process.hpp>

#include <stout/hashmap.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace controller {
namespace allocator {

class MediosAllocatorProcess;

// A wrapper for Process-based allocators. It redirects all function
// invocations to the underlying AllocatorProcess and manages its
// lifetime. We ensure the template parameter AllocatorProcess
// implements MediosAllocatorProcess by storing a pointer to it.
template <typename AllocatorProcess>
class MediosAllocator : public medios::allocator::Allocator
{
public:
  // Factory to allow for typed tests.
  static Try<medios::allocator::Allocator*> create();

  ~MediosAllocator() override;

  void initialize(
      const medios::allocator::Options& options,
      const lambda::function<
          void(const FrameworkID&,
               const hashmap<std::string, hashmap<NodeID, Resources>>&)>&
                   offerCallback,
      const lambda::function<
          void(const FrameworkID&,
               const hashmap<NodeID, UnavailableResources>&)>&
        inverseOfferCallback) override;

  void recover(
      const int expectedNodeCount,
      const hashmap<std::string, Quota>& quotas) override;

  void addFramework(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      const hashmap<NodeID, Resources>& used,
      bool active,
      ::medios::allocator::FrameworkOptions&& options) override;

  void removeFramework(
      const FrameworkID& frameworkId) override;

  void activateFramework(
      const FrameworkID& frameworkId) override;

  void deactivateFramework(
      const FrameworkID& frameworkId) override;

  void updateFramework(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      ::medios::allocator::FrameworkOptions&& options) override;

  void addNode(
      const NodeID& nodeId,
      const NodeInfo& nodeInfo,
      const std::vector<NodeInfo::Capability>& capabilities,
      const Option<Unavailability>& unavailability,
      const Resources& total,
      const hashmap<FrameworkID, Resources>& used) override;

  void removeNode(
      const NodeID& nodeId) override;

  void updateNode(
      const NodeID& node,
      const NodeInfo& nodeInfo,
      const Option<Resources>& total = None(),
      const Option<std::vector<NodeInfo::Capability>>& capabilities = None())
    override;

  void addResourceProvider(
      const NodeID& node,
      const Resources& total,
      const hashmap<FrameworkID, Resources>& used) override;

  void activateNode(
      const NodeID& nodeId) override;

  void deactivateNode(
      const NodeID& nodeId) override;

  void updateAllowlist(
      const Option<hashset<std::string>>& allowlist) override;

  void requestResources(
      const FrameworkID& frameworkId,
      const std::vector<Request>& requests) override;

  void updateAllocation(
      const FrameworkID& frameworkId,
      const NodeID& nodeId,
      const Resources& offeredResources,
      const std::vector<ResourceConversion>& conversions) override;

  process::Future<Nothing> updateAvailable(
      const NodeID& nodeId,
      const std::vector<Offer::Operation>& operations) override;

  void updateUnavailability(
      const NodeID& nodeId,
      const Option<Unavailability>& unavailability) override;

  void updateInverseOffer(
      const NodeID& nodeId,
      const FrameworkID& frameworkId,
      const Option<UnavailableResources>& unavailableResources,
      const Option<medios::allocator::InverseOfferStatus>& status,
      const Option<Filters>& filters) override;

  process::Future<
      hashmap<NodeID,
              hashmap<FrameworkID, medios::allocator::InverseOfferStatus>>>
    getInverseOfferStatuses() override;

  void transitionOfferedToAllocated(
     const NodeID& nodeId, const Resources& resources) override;

  void recoverResources(
      const FrameworkID& frameworkId,
      const NodeID& nodeId,
      const Resources& resources,
      const Option<Filters>& filters,
      bool isAllocated) override;

  void suppressOffers(
      const FrameworkID& frameworkId,
      const std::set<std::string>& roles) override;

  void reviveOffers(
      const FrameworkID& frameworkId,
      const std::set<std::string>& roles) override;

  void updateQuota(
      const std::string& role,
      const Quota& quota) override;

  void updateWeights(
      const std::vector<WeightInfo>& weightInfos) override;

  void pause() override;

  void resume() override;

private:
  MediosAllocator();
  MediosAllocator(const MediosAllocator&); // Not copyable.
  MediosAllocator& operator=(const MediosAllocator&); // Not assignable.

  MediosAllocatorProcess* process;
};


// The basic interface for all Process-based allocators.
class MediosAllocatorProcess : public process::Process<MediosAllocatorProcess>
{
public:
  MediosAllocatorProcess() {}

  ~MediosAllocatorProcess() override {}

  // Explicitly unhide 'initialize' to silence a compiler warning
  // from clang, since we overload below.
  using process::ProcessBase::initialize;

  virtual void initialize(
      const medios::allocator::Options& options,
      const lambda::function<
          void(const FrameworkID&,
               const hashmap<std::string, hashmap<NodeID, Resources>>&)>&
                   offerCallback,
      const lambda::function<
          void(const FrameworkID&,
               const hashmap<NodeID, UnavailableResources>&)>&
        inverseOfferCallback) = 0;

  virtual void recover(
      const int expectedNodeCount,
      const hashmap<std::string, Quota>& quotas) = 0;

  virtual void addFramework(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      const hashmap<NodeID, Resources>& used,
      bool active,
      ::medios::allocator::FrameworkOptions&& options) = 0;

  virtual void removeFramework(
      const FrameworkID& frameworkId) = 0;

  virtual void activateFramework(
      const FrameworkID& frameworkId) = 0;

  virtual void deactivateFramework(
      const FrameworkID& frameworkId) = 0;

  virtual void updateFramework(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      ::medios::allocator::FrameworkOptions&& options) = 0;

  virtual void addNode(
      const NodeID& nodeId,
      const NodeInfo& nodeInfo,
      const std::vector<NodeInfo::Capability>& capabilities,
      const Option<Unavailability>& unavailability,
      const Resources& total,
      const hashmap<FrameworkID, Resources>& used) = 0;

  virtual void removeNode(
      const NodeID& nodeId) = 0;

  virtual void updateNode(
      const NodeID& node,
      const NodeInfo& nodeInfo,
      const Option<Resources>& total = None(),
      const Option<std::vector<NodeInfo::Capability>>&
          capabilities = None()) = 0;

  virtual void addResourceProvider(
      const NodeID& node,
      const Resources& total,
      const hashmap<FrameworkID, Resources>& used) = 0;

  virtual void activateNode(
      const NodeID& nodeId) = 0;

  virtual void deactivateNode(
      const NodeID& nodeId) = 0;

  virtual void updateAllowlist(
      const Option<hashset<std::string>>& allowlist) = 0;

  virtual void requestResources(
      const FrameworkID& frameworkId,
      const std::vector<Request>& requests) = 0;

  virtual void updateAllocation(
      const FrameworkID& frameworkId,
      const NodeID& nodeId,
      const Resources& offeredResources,
      const std::vector<ResourceConversion>& conversions) = 0;

  virtual process::Future<Nothing> updateAvailable(
      const NodeID& nodeId,
      const std::vector<Offer::Operation>& operations) = 0;

  virtual void updateUnavailability(
      const NodeID& nodeId,
      const Option<Unavailability>& unavailability) = 0;

  virtual void updateInverseOffer(
      const NodeID& nodeId,
      const FrameworkID& frameworkId,
      const Option<UnavailableResources>& unavailableResources,
      const Option<medios::allocator::InverseOfferStatus>& status,
      const Option<Filters>& filters = None()) = 0;

  virtual process::Future<
      hashmap<NodeID,
              hashmap<FrameworkID, medios::allocator::InverseOfferStatus>>>
    getInverseOfferStatuses() = 0;

  virtual void transitionOfferedToAllocated(
      const NodeID& nodeId, const Resources& resources) = 0;

  virtual void recoverResources(
      const FrameworkID& frameworkId,
      const NodeID& nodeId,
      const Resources& resources,
      const Option<Filters>& filters,
      bool isAllocated) = 0;

  virtual void suppressOffers(
      const FrameworkID& frameworkId,
      const std::set<std::string>& roles) = 0;

  virtual void reviveOffers(
      const FrameworkID& frameworkId,
      const std::set<std::string>& roles) = 0;

  virtual void updateQuota(
      const std::string& role,
      const Quota& quota) = 0;

  virtual void updateWeights(
      const std::vector<WeightInfo>& weightInfos) = 0;

  virtual void pause() = 0;

  virtual void resume() = 0;
};


template <typename AllocatorProcess>
Try<medios::allocator::Allocator*>
MediosAllocator<AllocatorProcess>::create()
{
  medios::allocator::Allocator* allocator =
    new MediosAllocator<AllocatorProcess>();
  return CHECK_NOTNULL(allocator);
}


template <typename AllocatorProcess>
MediosAllocator<AllocatorProcess>::MediosAllocator()
{
  process = new AllocatorProcess();
  process::spawn(process);
}


template <typename AllocatorProcess>
MediosAllocator<AllocatorProcess>::~MediosAllocator()
{
  process::terminate(process);
  process::wait(process);
  delete process;
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::initialize(
    const medios::allocator::Options& options,
    const lambda::function<
        void(const FrameworkID&,
             const hashmap<std::string, hashmap<NodeID, Resources>>&)>&
                 offerCallback,
    const lambda::function<
        void(const FrameworkID&,
              const hashmap<NodeID, UnavailableResources>&)>&
      inverseOfferCallback)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::initialize,
      options,
      offerCallback,
      inverseOfferCallback);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::recover(
    const int expectedNodeCount,
    const hashmap<std::string, Quota>& quotas)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::recover,
      expectedNodeCount,
      quotas);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::addFramework(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const hashmap<NodeID, Resources>& used,
    bool active,
    ::medios::allocator::FrameworkOptions&& options)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::addFramework,
      frameworkId,
      frameworkInfo,
      used,
      active,
      std::move(options));
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::removeFramework(
    const FrameworkID& frameworkId)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::removeFramework,
      frameworkId);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::activateFramework(
    const FrameworkID& frameworkId)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::activateFramework,
      frameworkId);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::deactivateFramework(
    const FrameworkID& frameworkId)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::deactivateFramework,
      frameworkId);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateFramework(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    ::medios::allocator::FrameworkOptions&& options)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::updateFramework,
      frameworkId,
      frameworkInfo,
      std::move(options));
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::addNode(
    const NodeID& nodeId,
    const NodeInfo& nodeInfo,
    const std::vector<NodeInfo::Capability>& capabilities,
    const Option<Unavailability>& unavailability,
    const Resources& total,
    const hashmap<FrameworkID, Resources>& used)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::addNode,
      nodeId,
      nodeInfo,
      capabilities,
      unavailability,
      total,
      used);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::removeNode(
    const NodeID& nodeId)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::removeNode,
      nodeId);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateNode(
    const NodeID& nodeId,
    const NodeInfo& nodeInfo,
    const Option<Resources>& total,
    const Option<std::vector<NodeInfo::Capability>>& capabilities)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::updateNode,
      nodeId,
      nodeInfo,
      total,
      capabilities);
}

template <typename AllocatorProcess>
void MediosAllocator<AllocatorProcess>::addResourceProvider(
    const NodeID& node,
    const Resources& total,
    const hashmap<FrameworkID, Resources>& used)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::addResourceProvider,
      node,
      total,
      used);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::activateNode(
    const NodeID& nodeId)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::activateNode,
      nodeId);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::deactivateNode(
    const NodeID& nodeId)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::deactivateNode,
      nodeId);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateAllowlist(
    const Option<hashset<std::string>>& allowlist)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::updateAllowlist,
      allowlist);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::requestResources(
    const FrameworkID& frameworkId,
    const std::vector<Request>& requests)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::requestResources,
      frameworkId,
      requests);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateAllocation(
    const FrameworkID& frameworkId,
    const NodeID& nodeId,
    const Resources& offeredResources,
    const std::vector<ResourceConversion>& conversions)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::updateAllocation,
      frameworkId,
      nodeId,
      offeredResources,
      conversions);
}


template <typename AllocatorProcess>
inline process::Future<Nothing>
MediosAllocator<AllocatorProcess>::updateAvailable(
    const NodeID& nodeId,
    const std::vector<Offer::Operation>& operations)
{
  return process::dispatch(
      process,
      &MediosAllocatorProcess::updateAvailable,
      nodeId,
      operations);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateUnavailability(
    const NodeID& nodeId,
    const Option<Unavailability>& unavailability)
{
  return process::dispatch(
      process,
      &MediosAllocatorProcess::updateUnavailability,
      nodeId,
      unavailability);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateInverseOffer(
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const Option<UnavailableResources>& unavailableResources,
    const Option<medios::allocator::InverseOfferStatus>& status,
    const Option<Filters>& filters)
{
  return process::dispatch(
      process,
      &MediosAllocatorProcess::updateInverseOffer,
      nodeId,
      frameworkId,
      unavailableResources,
      status,
      filters);
}


template <typename AllocatorProcess>
inline process::Future<
    hashmap<NodeID,
            hashmap<FrameworkID, medios::allocator::InverseOfferStatus>>>
  MediosAllocator<AllocatorProcess>::getInverseOfferStatuses()
{
  return process::dispatch(
      process,
      &MediosAllocatorProcess::getInverseOfferStatuses);
}

template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::transitionOfferedToAllocated(
    const NodeID& nodeId,
    const Resources& resources)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::transitionOfferedToAllocated,
      nodeId,
      resources);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::recoverResources(
    const FrameworkID& frameworkId,
    const NodeID& nodeId,
    const Resources& resources,
    const Option<Filters>& filters,
    bool isAllocated)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::recoverResources,
      frameworkId,
      nodeId,
      resources,
      filters,
      isAllocated);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::suppressOffers(
    const FrameworkID& frameworkId,
    const std::set<std::string>& roles)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::suppressOffers,
      frameworkId,
      roles);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::reviveOffers(
    const FrameworkID& frameworkId,
    const std::set<std::string>& roles)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::reviveOffers,
      frameworkId,
      roles);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateQuota(
    const std::string& role,
    const Quota& quota)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::updateQuota,
      role,
      quota);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::updateWeights(
    const std::vector<WeightInfo>& weightInfos)
{
  process::dispatch(
      process,
      &MediosAllocatorProcess::updateWeights,
      weightInfos);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::pause()
{
  process::dispatch(process, &MediosAllocatorProcess::pause);
}


template <typename AllocatorProcess>
inline void MediosAllocator<AllocatorProcess>::resume()
{
  process::dispatch(process, &MediosAllocatorProcess::resume);
}


} // namespace allocator {
} // namespace controller {
} // namespace internal {
} // namespace medios {

#endif // __CONTROLLER_ALLOCATOR_MEDIOS_ALLOCATOR_HPP__
