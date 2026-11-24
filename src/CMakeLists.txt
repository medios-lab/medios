# Reuse existing protobuf codegen helper.
include(${CMAKE_SOURCE_DIR}/src/cmake/MediosProtobuf.cmake)

set(MEDIOS_PUBLIC_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include)
set(MEDIOS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(MEDIOS_BIN ${CMAKE_BINARY_DIR})
set(MEDIOS_BIN_INCLUDE_DIR ${CMAKE_BINARY_DIR}/include)
set(MEDIOS_BIN_SRC_DIR ${CMAKE_BINARY_DIR}/src)

add_custom_target(make_bin_include_dir
  COMMAND ${CMAKE_COMMAND} -E make_directory ${MEDIOS_BIN_INCLUDE_DIR}
)

add_custom_target(make_bin_src_dir
  COMMAND ${CMAKE_COMMAND} -E make_directory ${MEDIOS_BIN_SRC_DIR}
)

# Generated proto sources accumulate via PROTOC_GENERATE calls. BUILD-004 will
# expand this list while curating the full compile target graph.
set(PUBLIC_PROTOBUF_SRC "")
set(INTERNAL_PROTOBUF_SRC "")

file(GLOB_RECURSE MEDIOS_PUBLIC_PROTOS CONFIGURE_DEPENDS
  ${MEDIOS_PUBLIC_INCLUDE_DIR}/medios/*.proto
)

foreach(PROTO_FILE IN LISTS MEDIOS_PUBLIC_PROTOS)
  file(RELATIVE_PATH PROTO_TARGET ${MEDIOS_PUBLIC_INCLUDE_DIR} ${PROTO_FILE})
  string(REGEX REPLACE "\\.proto$" "" PROTO_TARGET ${PROTO_TARGET})
  PROTOC_GENERATE(TARGET ${PROTO_TARGET})
endforeach()

file(GLOB_RECURSE MEDIOS_INTERNAL_PROTOS CONFIGURE_DEPENDS
  ${MEDIOS_SRC_DIR}/*.proto
)

foreach(PROTO_FILE IN LISTS MEDIOS_INTERNAL_PROTOS)
  if(PROTO_FILE MATCHES "/tests/")
    continue()
  endif()

  file(RELATIVE_PATH PROTO_TARGET ${MEDIOS_SRC_DIR} ${PROTO_FILE})
  string(REGEX REPLACE "\\.proto$" "" PROTO_TARGET ${PROTO_TARGET})
  PROTOC_GENERATE(INTERNAL TARGET ${PROTO_TARGET})
endforeach()

set(MEDIOS_RUNTIME_SOURCES
  authentication/identity_assertion/identity_assertion_authenticator.cpp
  authentication/http/basic_authenticatee.cpp
  authentication/http/basic_authenticator_factory.cpp
  authentication/http/combined_authenticator.cpp
  authorizer/acls.cpp
  authorizer/authorizer.cpp
  authorizer/local/authorizer.cpp
  checks/checker.cpp
  checks/checker_process.cpp
  checks/health_checker.cpp
  common/attributes.cpp
  common/authorization.cpp
  common/build.cpp
  common/command_utils.cpp
  common/http.cpp
  common/kernel_version.cpp
  common/protobuf_utils.cpp
  common/resource_quantities.cpp
  common/resources.cpp
  common/resources_utils.cpp
  common/roles.cpp
  common/type_utils.cpp
  common/validation.cpp
  common/values.cpp
  controller/allocator/allocator.cpp
  controller/allocator/medios/hierarchical.cpp
  controller/allocator/medios/metrics.cpp
  controller/allocator/medios/offer_constraints_filter.cpp
  controller/allocator/medios/sorter/drf/metrics.cpp
  controller/allocator/medios/sorter/drf/sorter.cpp
  controller/allocator/medios/sorter/random/sorter.cpp
  controller/authorization.cpp
  controller/constants.cpp
  controller/contender/contender.cpp
  controller/contender/etcd.cpp
  controller/contender/standalone.cpp
  controller/controller.cpp
  controller/detector/detector.cpp
  controller/detector/etcd.cpp
  controller/detector/standalone.cpp
  controller/etcd/client.cpp
  controller/flags.cpp
  controller/framework.cpp
  controller/http.cpp
  controller/maintenance.cpp
  controller/metrics.cpp
  controller/node.cpp
  controller/quota.cpp
  controller/quota_handler.cpp
  controller/readonly_handler.cpp
  controller/stores.cpp
  controller/registrar.cpp
  controller/registry_operations.cpp
  controller/validation.cpp
  controller/weights.cpp
  controller/weights_handler.cpp
  docker/spec.cpp
  exec/exec.cpp
  executor/executor.cpp
  executor/v0_v1executor.cpp
  files/files.cpp
  hook/manager.cpp
  internal/devolve.cpp
  internal/evolve.cpp
  # The observers and the progress reader both built-in executors drive,
  # compiled here so the two executor binaries and the tests resolve
  # against one copy.
  launcher/finish.cpp
  launcher/observer.cpp
  launcher/progress.cpp
  local/local.cpp
  logging/flags.cpp
  logging/logging.cpp
  messages/messages.cpp
  module/manager.cpp
  node/compatibility.cpp
  node/constants.cpp
  node/containerizer/containerizer.cpp
  # The NVML wrapper is portable: it resolves libnvidia-ml through dlopen at
  # runtime and reports unavailable everywhere the library is absent, which
  # includes every non-Linux host. It is built on all platforms so the GPU
  # module, which is also built everywhere, links against one copy of it.
  modules/gpu/nvml.cpp
  # Compiled here, not into the module, for the reason nvml.cpp is: one copy
  # in libmedios that the module and the tests both resolve against.
  modules/gpu/checkpoint.cpp
  modules/gpu/delivery.cpp
  modules/gpu/units.cpp
  node/containerizer/fetcher.cpp
  node/containerizer/docker_shell/docker_shell_containerizer.cpp
  node/containerizer/medios/containerizer.cpp
  node/containerizer/medios/io/switchboard.cpp
  node/containerizer/medios/isolator.cpp
  node/containerizer/medios/isolator_tracker.cpp
  node/containerizer/medios/isolators/filesystem/posix.cpp
  node/containerizer/medios/isolators/posix/disk.cpp
  node/containerizer/medios/isolators/posix/rlimits.cpp
  node/containerizer/medios/launch.cpp
  node/containerizer/medios/launcher.cpp
  node/containerizer/medios/launcher_tracker.cpp
  node/containerizer/medios/mount.cpp
  node/containerizer/medios/paths.cpp
  node/containerizer/medios/provisioner/backend.cpp
  node/containerizer/medios/provisioner/backends/copy.cpp
  node/containerizer/medios/provisioner/docker/image_tar_puller.cpp
  node/containerizer/medios/provisioner/docker/metadata_manager.cpp
  node/containerizer/medios/provisioner/docker/paths.cpp
  node/containerizer/medios/provisioner/docker/puller.cpp
  node/containerizer/medios/provisioner/docker/registry_puller.cpp
  node/containerizer/medios/provisioner/docker/store.cpp
  node/containerizer/medios/provisioner/paths.cpp
  node/containerizer/medios/provisioner/provisioner.cpp
  node/containerizer/medios/provisioner/store.cpp
  node/containerizer/medios/provisioner/utils.cpp
  node/containerizer/medios/utils.cpp
  node/container_logger.cpp
  node/container_loggers/sandbox.cpp
  node/executor.cpp
  node/flags.cpp
  node/framework.cpp
  node/gc.cpp
  node/http.cpp
  node/metrics.cpp
  node/node.cpp
  node/paths.cpp
  node/qos_controller.cpp
  node/qos_controllers/noop.cpp
  node/resource_provider.cpp
  node/resource_estimator.cpp
  node/resource_estimators/noop.cpp
  node/state.cpp
  node/stores.cpp
  node/task_status_update_manager.cpp
  node/validation.cpp
  node/volume_gid_manager/volume_gid_manager.cpp
  posix/rlimits.cpp
  resource_provider/daemon.cpp
  resource_provider/detector.cpp
  resource_provider/driver.cpp
  resource_provider/local.cpp
  resource_provider/manager.cpp
  resource_provider/registrar.cpp
  resource_provider/validation.cpp
  sched/sched.cpp
  scheduler/scheduler.cpp
  usage/usage.cpp
  secret/resolver.cpp
  state/in_memory.cpp
  state/leveldb.cpp
  status_update_manager/operation.cpp
  uri/fetcher.cpp
  uri/fetchers/copy.cpp
  uri/fetchers/curl.cpp
  uri/fetchers/docker.cpp
  uri/utils.cpp
  v1/attributes.cpp
  v1/medios.cpp
  v1/resources.cpp
  v1/values.cpp
  version/version.cpp
  watcher/allowlist_watcher.cpp
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND MEDIOS_RUNTIME_SOURCES
    linux/capabilities.cpp
    linux/cgroups.cpp
    linux/cgroups2.cpp
    linux/ebpf.cpp
    linux/fs.cpp
    linux/ldcache.cpp
    linux/ldd.cpp
    linux/ns.cpp
    linux/perf.cpp
    linux/systemd.cpp
    node/containerizer/device_manager/device_manager.cpp
    node/containerizer/process_limits.cpp
    node/containerizer/medios/isolators/cgroups/cgroups.cpp
    node/containerizer/medios/isolators/cgroups/subsystem.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/blkio.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/cpu.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/cpuacct.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/cpuset.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/devices.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/hugetlb.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/memory.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/net_cls.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/net_prio.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/perf_event.cpp
    node/containerizer/medios/isolators/cgroups/subsystems/pids.cpp
    node/containerizer/medios/isolators/cgroups2/cgroups2.cpp
    node/containerizer/medios/isolators/cgroups2/controller.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/core.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/cpu.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/cpuset.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/devices.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/hugetlb.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/io.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/memory.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/perf_event.cpp
    node/containerizer/medios/isolators/cgroups2/controllers/pids.cpp
    node/containerizer/medios/isolators/docker/runtime.cpp
    node/containerizer/medios/isolators/filesystem/linux.cpp
    node/containerizer/medios/isolators/linux/capabilities.cpp
    node/containerizer/medios/isolators/linux/nnp.cpp
    node/containerizer/medios/isolators/namespaces/ipc.cpp
    node/containerizer/medios/isolators/namespaces/pid.cpp
    modules/gpu/volume.cpp
    node/containerizer/medios/isolators/volume/image.cpp
    node/containerizer/medios/linux_launcher.cpp
    node/containerizer/medios/provisioner/backends/aufs.cpp
    node/containerizer/medios/provisioner/backends/bind.cpp
    node/containerizer/medios/provisioner/backends/overlay.cpp
  )
endif()

if(MEDIOS_ENABLE_SECCOMP_ISOLATOR)
  list(APPEND MEDIOS_RUNTIME_SOURCES
    linux/seccomp/seccomp.cpp
    linux/seccomp/seccomp_parser.cpp
    node/containerizer/medios/isolators/linux/seccomp.cpp
  )
endif()

# node/main.cpp and local/local.cpp reach the executor JWT secret generator
# only under USE_SSL_SOCKET; it needs the JWT code libprocess compiles only
# in an SSL build.
if(MEDIOS_ENABLE_SSL)
  list(APPEND MEDIOS_RUNTIME_SOURCES
    authentication/executor/jwt_secret_generator.cpp
  )
endif()

# Shared libmedios (BUILD-002 decision C). This placeholder keeps configure
# focused on dependency and target skeleton wiring; compilation is BUILD-004.
add_library(medios SHARED
  cmake/medios_placeholder.cpp
  ${MEDIOS_RUNTIME_SOURCES}
  ${PUBLIC_PROTOBUF_SRC}
  ${INTERNAL_PROTOBUF_SRC}
)

target_include_directories(medios PUBLIC
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_BINARY_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/src
)

target_link_libraries(medios
  PUBLIC stout process protobuf::libprotobuf
  PRIVATE glog::glog gflags leveldb medios_compile_flags re2::re2
)

target_compile_definitions(medios
  PRIVATE
    LIBDIR="${CMAKE_BINARY_DIR}/src"
    PKGLIBEXECDIR="${CMAKE_BINARY_DIR}/src"
    PKGDATADIR="${CMAKE_SOURCE_DIR}/src"
    USE_CMAKE_BUILD_CONFIG
)

# PUBLIC: modules/gpu/nvml.hpp exposes the driver's nvmlDevice_t in its
# signatures, so the GPU module and the tests must compile against the same
# definition as libmedios. Both link medios and inherit these.
if(MEDIOS_ENABLE_NVML)
  target_include_directories(medios PUBLIC ${NVML_INCLUDE_DIR})
  target_compile_definitions(medios PUBLIC ENABLE_NVML)
endif()

# The definition and the header path are PUBLIC because the seccomp tests
# test the definition and include linux/seccomp/seccomp.hpp, which
# includes <seccomp.h>. The archive links into libmedios alone.
if(MEDIOS_ENABLE_SECCOMP_ISOLATOR)
  target_compile_definitions(medios PUBLIC ENABLE_SECCOMP_ISOLATOR)
  target_include_directories(medios PUBLIC ${LIBSECCOMP_INSTALL_DIR}/include)
  target_link_libraries(medios PRIVATE libseccomp)
endif()

# Precompiled headers (#177). Covers the high-frequency stable headers that
# every libmedios TU re-parses from scratch otherwise. Picked from include
# frequency across src/**/*.cpp; thresholds: std headers always, third-party
# heavy headers (glog, stout, process, mesos) above ~25 uses. Internal headers
# excluded — they change too often.
target_precompile_headers(medios PRIVATE
  <algorithm>
  <map>
  <memory>
  <set>
  <string>
  <vector>

  <glog/logging.h>

  <stout/error.hpp>
  <stout/foreach.hpp>
  <stout/hashmap.hpp>
  <stout/nothing.hpp>
  <stout/option.hpp>
  <stout/os.hpp>
  <stout/path.hpp>
  <stout/stringify.hpp>
  <stout/strings.hpp>
  <stout/try.hpp>
  <stout/unreachable.hpp>

  <process/collect.hpp>
  <process/defer.hpp>
  <process/dispatch.hpp>
  <process/future.hpp>
  <process/id.hpp>
  <process/owned.hpp>
  <process/process.hpp>

  <medios/medios.hpp>
  <medios/type_utils.hpp>
)

add_subdirectory(controller)
add_subdirectory(node)
add_subdirectory(modules/gpu)

add_executable(medios-execute cli/execute.cpp)
target_link_libraries(medios-execute PRIVATE medios medios_compile_flags)

# Starts an in-process cluster (controller + allocator + node) for testing.
# Execution is real only up to the containerizer; pair with --resources to
# advertise hardware the host does not have. See docs/Simulation.md (Sim B).
add_executable(medios-local local/main.cpp)
target_link_libraries(medios-local PRIVATE medios medios_compile_flags)

add_executable(medios-fetcher launcher/fetcher.cpp)
target_link_libraries(medios-fetcher PRIVATE medios medios_compile_flags)

add_executable(medios-executor launcher/executor.cpp)
target_link_libraries(medios-executor PRIVATE medios medios_compile_flags)
target_compile_definitions(medios-executor
  PRIVATE
    PKGLIBEXECDIR="${CMAKE_BINARY_DIR}/src"
)

# DR-030: medios-docker-executor — Docker runtime backend executor.
# Parallel to medios-executor; selected by the DockerShellContainerizer for
# tasks with container.type == DOCKER.
add_executable(medios-docker-executor
  launcher/docker_executor/docker_executor.cpp
  launcher/docker_executor/docker_exec.cpp
)
target_link_libraries(medios-docker-executor
  PRIVATE medios medios_compile_flags
)

# Default executor for v1::ExecutorInfo::DEFAULT (task groups).
# Restored to unblock 7 tests in test_node + test_controller (#111).
add_executable(medios-default-executor launcher/default_executor.cpp)
target_link_libraries(medios-default-executor PRIVATE medios medios_compile_flags)
target_compile_definitions(medios-default-executor
  PRIVATE
    PKGLIBEXECDIR="${CMAKE_BINARY_DIR}/src"
)

# TCP checks exec this from the node's launcher_dir, the same way HTTP
# checks exec curl. It carries its own main(), so it must not also be a
# source of the shared library.
add_executable(medios-tcp-connect checks/tcp_connect.cpp)
target_link_libraries(medios-tcp-connect PRIVATE medios medios_compile_flags)

add_executable(medios-containerizer node/containerizer/medios/main.cpp)
target_link_libraries(medios-containerizer PRIVATE medios medios_compile_flags)

# The containerizer execs this from the node's launcher_dir for every
# container with a TTY and every DEBUG-class nested container.
add_executable(medios-io-switchboard node/containerizer/medios/io/switchboard_main.cpp)
target_link_libraries(medios-io-switchboard PRIVATE medios medios_compile_flags)
