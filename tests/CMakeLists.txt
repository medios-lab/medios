include(GoogleTest)

# --- Helper include path for ported upstream test helpers (#112) ---
# INTERFACE-only: future test executables that #include "tests/foo.hpp" link
# against medios-test-helpers to put the top-level tests/ directory on their
# include path. Helper .cpp bodies live in medios-test-helpers-impl below.
add_library(medios-test-helpers INTERFACE)
target_include_directories(medios-test-helpers INTERFACE ${CMAKE_SOURCE_DIR})

# --- Helper .cpp bodies (#112) ---
set(MEDIOS_TEST_HELPERS_IMPL_SRCS
  allocator.cpp
  module.cpp
  cluster.cpp
  containerizer.cpp
  controller/mock_controller_api_subscriber.cpp
  environment.cpp
  flags.cpp
  active_user_test_helper.cpp
  kill_policy_test_helper.cpp
  medios.cpp
  mock_node.cpp
  mock_registrar.cpp
  resources_utils.cpp
  utils.cpp
)

# tests/containerizer/rootfs.cpp / .hpp are Linux-only — the header
# `#error`s on non-Linux. Mac builds (PHASE-A dev loop) exclude them;
# the docker-archive tests that use them are gated by the existing
# ROOT_/CGROUPS_/etc. filters at runtime.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND MEDIOS_TEST_HELPERS_IMPL_SRCS
    containerizer/rootfs.cpp
    containerizer/capabilities_test_helper.cpp)
endif()

add_library(medios-test-helpers-impl STATIC
  ${MEDIOS_TEST_HELPERS_IMPL_SRCS}
)
target_link_libraries(medios-test-helpers-impl
  PUBLIC
    medios-test-helpers
    medios
    GTest::gmock
    GTest::gtest
)
target_include_directories(medios-test-helpers-impl
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
# Path macros upstream Mesos passes via its build system. Point everything at
# the build tree so tests resolve helper binaries and libraries from there.
target_compile_definitions(medios-test-helpers-impl
  PRIVATE
    LIBDIR="${CMAKE_BINARY_DIR}/src"
    PKGLIBEXECDIR="${CMAKE_BINARY_DIR}/src"
    PKGMODULEDIR="${CMAKE_BINARY_DIR}/src"
    SBINDIR="${CMAKE_BINARY_DIR}/src"
    TESTLIBEXECDIR="${CMAKE_BINARY_DIR}/tests"
    PKGDATADIR="${CMAKE_SOURCE_DIR}/src"
    SOURCE_DIR="${CMAKE_SOURCE_DIR}"
    VERSION="${PROJECT_VERSION}"
  # PUBLIC: tests/medios.hpp includes <process/ssl/gtest.hpp>, whose
  # SSL-enabled body resolves BUILD_DIR in every test that includes it.
  PUBLIC
    BUILD_DIR="${CMAKE_BINARY_DIR}"
)

# --- Test modules (SHARED libs loaded by tests::initModules) ---
# initModules() in tests/module.cpp expects these in ${CMAKE_BINARY_DIR}/src/.libs/.
# Format per pair: <source-stem>|<lib-name-without-underscores>. Mismatch arises
# because module.cpp drops underscores for some names (http_authenticator
# → testhttpauthenticator) but keeps them for others (container_logger
# → testcontainer_logger).
set(_TEST_MODULES
  "isolator|testisolator"
  "container_logger|testcontainer_logger"
  "hook|testhook"
  "anonymous|testanonymous"
  "allocator|testallocator"
  "resource_estimator|testresource_estimator"
  "authorizer|testauthorizer"
  "http_authenticator|testhttpauthenticator"
  "controller_contender|testcontrollercontender"
  "controller_detector|testcontrollerdetector"
)
foreach(_PAIR IN LISTS _TEST_MODULES)
  string(REPLACE "|" ";" _PAIR_LIST ${_PAIR})
  list(GET _PAIR_LIST 0 _SRC_STEM)
  list(GET _PAIR_LIST 1 _LIB_NAME)
  add_library(${_LIB_NAME} SHARED modules/test_${_SRC_STEM}_module.cpp)
  target_link_libraries(${_LIB_NAME} PRIVATE medios)
  target_include_directories(${_LIB_NAME}
    PRIVATE
      ${CMAKE_BINARY_DIR}/include
      ${CMAKE_SOURCE_DIR}
  )
  set_target_properties(${_LIB_NAME} PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/src/.libs"
  )
  list(APPEND _TEST_MODULE_TARGETS ${_LIB_NAME})
endforeach()

# The shared test main() dlopens every one of these modules at startup
# (tests::initModules), but they are runtime-loaded, not link-time deps — so
# `cmake --build build --target test_X` would otherwise build the test binary
# without them and the binary exits 1 (silently) when the dlopen fails. Every
# test links medios-test-helpers-impl, so hanging the module targets off it
# makes any single-target test build pull them in. A full build built them
# anyway; this fixes the isolated-target build.
add_dependencies(medios-test-helpers-impl ${_TEST_MODULE_TARGETS})

# --- test-helper subcommand dispatcher (consumed by getTestHelperPath()) ---
# Hosts KillPolicyTestHelper, ActiveUserTestHelper, HttpServerTestHelper as
# Subcommand classes; the test binary invokes `<test-helper> <subcommand>`.
# On Linux it also hosts CapabilitiesTestHelper.
add_executable(test-helper
  test_helper_main.cpp
  kill_policy_test_helper.cpp
  active_user_test_helper.cpp
  http_server_test_helper.cpp
)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_sources(test-helper PRIVATE containerizer/capabilities_test_helper.cpp)
endif()
target_link_libraries(test-helper PRIVATE medios)
target_link_libraries(test-helper PRIVATE medios-test-helpers)
target_include_directories(test-helper
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
set_target_properties(test-helper PROPERTIES
  OUTPUT_NAME test-helper
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/src"
)

# --- Node tests (ported from legacy upstream tests, #111) ---
add_executable(test_node
  node_tests.cpp
  main.cpp
)
# Two GetExecutorInfo tests assert the branch Node::getExecutorInfo takes
# when it can resolve launcher_dir/medios-executor on disk. Without the
# dependency, building this target alone yields a binary whose result
# depends on what else happens to be in the tree.
add_dependencies(test_node
  test-helper medios-executor medios-fetcher medios-containerizer)
target_link_libraries(test_node
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_node
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_node
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- GPU isolator module tests (medios#432) ---
# The module is dlopened at test time through ModuleManager, never linked.
add_executable(test_gpu_module
  gpu_module_tests.cpp
  # discovery.cpp is compiled in rather than taken from the module, so the
  # tests exercise it directly. It holds no global state, unlike nvml.cpp,
  # which stays in libmedios precisely so one process has one driver
  # initialization.
  ${CMAKE_SOURCE_DIR}/src/modules/gpu/discovery.cpp
  main.cpp
)
add_dependencies(test_gpu_module test-helper medios_gpu)
target_compile_definitions(test_gpu_module
  PRIVATE MEDIOS_GPU_MODULE_PATH="$<TARGET_FILE:medios_gpu>"
)
target_link_libraries(test_gpu_module
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_gpu_module
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_gpu_module
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# The same cases again, all of them in one process. `gtest_discover_tests` gives
# every case a process of its own, so anything a case leaves behind in
# process-global state is invisible to it, and the binary can fail when a
# developer runs it directly while `ctest` stays green. That is what happened
# with the module manager's record of a module's parameters (medios#472). It
# runs the whole binary in 0.14 s and is the only entry here that would catch
# that again.
add_test(NAME gpu_module_tests_in_one_process
  COMMAND test_gpu_module
)
set_tests_properties(gpu_module_tests_in_one_process PROPERTIES
  TIMEOUT 600
  ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Device-declaration seam tests (medios#450, M2) ---
add_executable(test_device_declaration
  device_declaration_tests.cpp
  main.cpp
)
add_dependencies(test_device_declaration test-helper)
target_link_libraries(test_device_declaration
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_device_declaration
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_device_declaration
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Stores file tests (DR-036, medios#529) ---
add_executable(test_stores
  stores_tests.cpp
  node_store_files_tests.cpp
  main.cpp
)
add_dependencies(test_stores test-helper)
target_link_libraries(test_stores
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_stores
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_stores
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Task principal: validation, authorizer, controller (DR-037) ---
add_executable(test_task_principal
  task_principal_tests.cpp
  main.cpp
)
add_dependencies(test_task_principal test-helper)
target_link_libraries(test_task_principal
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_task_principal
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_task_principal
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Task progress and executor ownership (DR-038) ---
add_executable(test_task_progress
  task_progress_tests.cpp
  task_observer_tests.cpp
  task_finish_tests.cpp
  main.cpp
)
add_dependencies(test_task_progress test-helper)
target_link_libraries(test_task_progress
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_task_progress
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_task_progress
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Executor progress reading and stall kill ---
add_executable(test_executor_progress
  executor_progress_tests.cpp
  executor_observer_tests.cpp
  executor_finish_tests.cpp
  main.cpp
)
# The progress, observer and finish cases fork launcher_dir/medios-executor
# and medios-containerizer. Without the dependency, building this target
# alone leaves those binaries against the previous libmedios.so, and every
# executor they launch dies at startup, which reads as a hang in the test.
add_dependencies(test_executor_progress test-helper medios-executor medios-containerizer)
target_link_libraries(test_executor_progress
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_executor_progress
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_executor_progress
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- os::killtree: group stopped first, process table read until stable ---
# Forks its own process trees and needs nothing but stout.
add_executable(test_killtree
  killtree_tests.cpp
)
target_link_libraries(test_killtree
  PRIVATE
    stout
    GTest::gtest_main
)
gtest_discover_tests(test_killtree
  DISCOVERY_TIMEOUT 10
  PROPERTIES TIMEOUT 60
)

# --- filesystem/linux isolator tests (medios#459) ---
add_executable(test_filesystem_linux
  filesystem_linux_tests.cpp
  main.cpp
)
add_dependencies(test_filesystem_linux test-helper medios-fetcher)
target_link_libraries(test_filesystem_linux
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_filesystem_linux
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_filesystem_linux
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- filesystem/posix store links (medios#602) ---
add_executable(test_filesystem_posix
  filesystem_posix_tests.cpp
  main.cpp
)
add_dependencies(test_filesystem_posix
  test-helper medios-fetcher medios-containerizer)
target_link_libraries(test_filesystem_posix
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_filesystem_posix
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_filesystem_posix
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Controller tests (ported from legacy upstream tests, #111) ---
add_executable(test_controller
  controller_tests.cpp
  controller_stores_tests.cpp
  controller_progress_policy_tests.cpp
  controller_task_observer_tests.cpp
  main.cpp
)
target_link_libraries(test_controller
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_controller
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_controller
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 600
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Docker OCI spec tests (kept-OCI surface after DR-021 strip) ---
add_executable(test_docker_spec
  containerizer/docker_spec_tests.cpp
  main.cpp
)
target_link_libraries(test_docker_spec
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_docker_spec
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_docker_spec
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 60
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- docker_shell containerizer status surface ---
# Hermetic: forks a stub binary named medios-docker-executor out of a temp
# launcher_dir, so no docker daemon is required.
add_executable(test_docker_shell_containerizer
  containerizer/docker_shell_containerizer_tests.cpp
  main.cpp
)
target_link_libraries(test_docker_shell_containerizer
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_docker_shell_containerizer
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_docker_shell_containerizer
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 120
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Docker image provisioner tests (kept-OCI surface) ---
add_executable(test_provisioner_docker
  containerizer/provisioner_docker_tests.cpp
  main.cpp
)
target_link_libraries(test_provisioner_docker
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_provisioner_docker
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_provisioner_docker
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- posix/cpu, posix/mem and posix/rlimits isolator tests ---
add_executable(test_posix_isolators
  containerizer/cpu_isolator_tests.cpp
  containerizer/memory_isolator_tests.cpp
  containerizer/posix_rlimits_isolator_tests.cpp
  main.cpp
)
add_dependencies(test_posix_isolators test-helper)
target_link_libraries(test_posix_isolators
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_posix_isolators
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_posix_isolators
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- disk/du isolator tests ---
add_executable(test_disk_du
  disk_quota_tests.cpp
  main.cpp
)
add_dependencies(test_disk_du test-helper)
target_link_libraries(test_disk_du
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_disk_du
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_disk_du
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Linux isolators: capabilities, nnp, namespaces/ipc, namespaces/pid ---
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(test_linux_isolators
    containerizer/capabilities_tests.cpp
    containerizer/isolator_tests.cpp
    containerizer/linux_capabilities_isolator_tests.cpp
    containerizer/linux_nnp_isolator_tests.cpp
    main.cpp
  )
  add_dependencies(test_linux_isolators test-helper)
  target_link_libraries(test_linux_isolators
    PRIVATE
      medios-test-helpers-impl
      GTest::gmock
      GTest::gtest
  )
  target_include_directories(test_linux_isolators
    PRIVATE ${CMAKE_BINARY_DIR}/include
  )
  gtest_discover_tests(test_linux_isolators
    DISCOVERY_TIMEOUT 30
    DISCOVERY_MODE PRE_TEST
    PROPERTIES
      TIMEOUT 180
      ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
  )
endif()

# --- linux/seccomp isolator and profile parser tests ---
if(MEDIOS_ENABLE_SECCOMP_ISOLATOR)
  add_executable(test_linux_seccomp
    containerizer/linux_seccomp_isolator_tests.cpp
    containerizer/linux_seccomp_parser_tests.cpp
    main.cpp
  )
  add_dependencies(test_linux_seccomp test-helper)
  target_link_libraries(test_linux_seccomp
    PRIVATE
      medios-test-helpers-impl
      GTest::gmock
      GTest::gtest
  )
  target_include_directories(test_linux_seccomp
    PRIVATE ${CMAKE_BINARY_DIR}/include
  )
  gtest_discover_tests(test_linux_seccomp
    DISCOVERY_TIMEOUT 30
    DISCOVERY_MODE PRE_TEST
    PROPERTIES
      TIMEOUT 180
      ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
  )
endif()

# --- io switchboard server and containerizer attach tests ---
add_executable(test_io_switchboard
  containerizer/io_switchboard_tests.cpp
  main.cpp
)
add_dependencies(test_io_switchboard
  medios-containerizer
  medios-executor
  medios-io-switchboard
)
target_link_libraries(test_io_switchboard
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_io_switchboard
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_io_switchboard
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Smoke: controller subprocess + /version (BUILD-007 / #68) ---
add_executable(test_controller_smoke smoke/test_controller_smoke.cpp)
add_dependencies(test_controller_smoke medios-controller)
target_include_directories(test_controller_smoke
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
target_compile_definitions(test_controller_smoke
  PRIVATE MEDIOS_CONTROLLER_BINARY="$<TARGET_FILE:medios-controller>"
)
target_link_libraries(test_controller_smoke PRIVATE GTest::gtest_main)
set_target_properties(test_controller_smoke PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/smoke
)

gtest_discover_tests(test_controller_smoke
  DISCOVERY_TIMEOUT 10
  PROPERTIES TIMEOUT 10
)

# --- E2E: hello-world (BUILD-006 / #56) ---
# Spawns controller + node + medios-execute; asserts the TASK_STARTING →
# TASK_RUNNING → TASK_FINISHED sequence. Longer timeout because it runs
# a real container task launch end-to-end.
add_executable(test_hello_world
  e2e/test_hello_world.cpp
  e2e/cluster_harness.cpp
)
add_dependencies(test_hello_world medios-controller medios-node medios-execute)
target_include_directories(test_hello_world
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
target_compile_definitions(test_hello_world
  PRIVATE
    MEDIOS_CONTROLLER_BINARY="$<TARGET_FILE:medios-controller>"
    MEDIOS_NODE_BINARY="$<TARGET_FILE:medios-node>"
    MEDIOS_EXECUTE_BINARY="$<TARGET_FILE:medios-execute>"
)
target_link_libraries(test_hello_world PRIVATE GTest::gtest_main)
set_target_properties(test_hello_world PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/e2e
)

gtest_discover_tests(test_hello_world
  DISCOVERY_TIMEOUT 10
  PROPERTIES TIMEOUT 120
)

# --- Mutual TLS end-to-end (#57, #389) ---
# Only in an SSL build: it links libprocess for the certificate helpers
# and the daemons it spawns need USE_SSL_SOCKET to read LIBPROCESS_SSL_*.
# Timeout covers the refusal test waiting out DAEMON_STARTUP_TIMEOUT.
if(MEDIOS_ENABLE_SSL)
  add_executable(test_tls
    e2e/test_tls.cpp
    e2e/cluster_harness.cpp
  )
  add_dependencies(test_tls medios-controller medios-node)
  target_include_directories(test_tls
    PRIVATE ${CMAKE_BINARY_DIR}/include
  )
  target_compile_definitions(test_tls
    PRIVATE
      MEDIOS_CONTROLLER_BINARY="$<TARGET_FILE:medios-controller>"
      MEDIOS_NODE_BINARY="$<TARGET_FILE:medios-node>"
      MEDIOS_EXECUTE_BINARY="$<TARGET_FILE:medios-execute>"
  )
  target_link_libraries(test_tls PRIVATE process GTest::gtest_main)
  set_target_properties(test_tls PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/e2e
  )

  gtest_discover_tests(test_tls
    DISCOVERY_TIMEOUT 10
    PROPERTIES TIMEOUT 120
  )
endif()

# --- DR-030 R1-R4: docker_shell end-to-end ---
# Auto-skipped on hosts without a reachable Docker daemon
# (SetUp() probes `docker info` and calls GTEST_SKIP() if unavailable).
add_executable(test_docker_shell
  e2e/test_docker_shell.cpp
  e2e/cluster_harness.cpp
)
add_dependencies(test_docker_shell
  medios-controller medios-node medios-execute medios-docker-executor)
target_include_directories(test_docker_shell
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
target_compile_definitions(test_docker_shell
  PRIVATE
    MEDIOS_CONTROLLER_BINARY="$<TARGET_FILE:medios-controller>"
    MEDIOS_NODE_BINARY="$<TARGET_FILE:medios-node>"
    MEDIOS_EXECUTE_BINARY="$<TARGET_FILE:medios-execute>"
)
target_link_libraries(test_docker_shell PRIVATE GTest::gtest_main)
set_target_properties(test_docker_shell PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/e2e
)

gtest_discover_tests(test_docker_shell
  DISCOVERY_TIMEOUT 10
  PROPERTIES TIMEOUT 120
)

# --- Command executor lifecycle (ported from legacy upstream tests, #111) ---
# tests/main.cpp is the custom Medios test main that registers
# process::FilterTestEventListener before running tests.
add_executable(test_command_executor
  command_executor_tests.cpp
  main.cpp
)
add_dependencies(test_command_executor test-helper)
target_link_libraries(test_command_executor
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_command_executor
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_command_executor
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 300
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Fetcher tests (CommandInfo.uris fetch path; cache subsystem dropped) ---
add_executable(test_fetcher
  fetcher_tests.cpp
  main.cpp
)
add_dependencies(test_fetcher test-helper medios-fetcher)
target_link_libraries(test_fetcher
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_fetcher
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_fetcher
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 300
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- State backend tests (in-memory + leveldb; ported from legacy upstream tests, #111) ---
# No MediosTest fixture; tests inherit from ::testing::Test and
# stout's TemporaryDirectoryTest directly. Still uses the custom test
# main for libprocess setup.
add_executable(test_state
  state_tests.cpp
  main.cpp
)
target_link_libraries(test_state
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_state
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_state
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 60
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Registrar tests (ported from legacy upstream tests, #111) ---
# Driver-light; exercises controller's registrar plus quota/weights
# state machines via the MediosTest fixture.
add_executable(test_registrar
  registrar_tests.cpp
  main.cpp
)
target_link_libraries(test_registrar
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_registrar
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_registrar
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 120
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Hierarchical (DRF) allocator tests (ported from legacy upstream tests, #111) ---
# Driver-light; exercises DRF, roles, weights, quota, allocation filters.
add_executable(test_hierarchical_allocator
  hierarchical_allocator_tests.cpp
  main.cpp
)
target_link_libraries(test_hierarchical_allocator
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_hierarchical_allocator
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_hierarchical_allocator
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 300
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- DRF / random sorter tests + scale benchmarks ("Sim A", #296) ---
# Unit cases run on every ctest pass. The two BENCHMARK_ cases
# (BENCHMARK_FullSort, BENCHMARK_HierarchyFullSort) are skipped by the
# BenchmarkFilter unless invoked on demand:
#   LIBPROCESS_IP=127.0.0.1 ./test_sorter --benchmark --gtest_filter='*BENCHMARK*'
add_executable(test_sorter
  sorter_tests.cpp
  main.cpp
)
target_link_libraries(test_sorter
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_sorter
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_sorter
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 300
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Hierarchical allocator scale benchmarks ("Sim A", #290) ---
# At-scale driver for the real HierarchicalDRFAllocator: thousands of
# synthetic nodes/frameworks, timed allocation cycles, in-memory, GPU-free.
# Built but deliberately NOT registered with gtest_discover_tests, so it
# does not run on every `ctest` pass. Invoke on demand:
#   LIBPROCESS_IP=127.0.0.1 ./test_hierarchical_allocator_benchmarks \
#     --gtest_filter='BENCHMARK_*'
add_executable(test_hierarchical_allocator_benchmarks
  hierarchical_allocator_benchmarks.cpp
  main.cpp
)
target_link_libraries(test_hierarchical_allocator_benchmarks
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_hierarchical_allocator_benchmarks
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

# --- Controller scale benchmarks ("Sim B", #289) ---
# Drives the real controller + allocator + scheduler API in-process under load:
# node reregistration delay, GetState query, actor responsiveness, metrics.
# Faked execution via TestContainerizer; advertise resources via node flags.
# Built but deliberately NOT registered with gtest_discover_tests. On demand:
#   LIBPROCESS_IP=127.0.0.1 ./test_controller_benchmarks \
#     --benchmark --gtest_filter='*BENCHMARK*'
add_executable(test_controller_benchmarks
  controller_benchmarks.cpp
  main.cpp
)
target_link_libraries(test_controller_benchmarks
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_controller_benchmarks
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

# --- Docker executor diagnostics unit tests (#298) ---
# Hermetic: exercises the subprocess capture/decode helpers via `sh -c`, no
# docker. docker_exec.cpp is compiled in directly (it is not part of libmedios).
add_executable(test_docker_exec
  docker_exec_tests.cpp
  ${CMAKE_SOURCE_DIR}/src/launcher/docker_executor/docker_exec.cpp
  main.cpp
)
target_link_libraries(test_docker_exec
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_docker_exec
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_docker_exec
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 120
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Resource metadata unit tests (#324) ---
# Hermetic: exercises Resources arithmetic/matching with opaque metadata on a
# resource. No cluster, no GPU.
add_executable(test_resources_metadata
  resources_metadata_tests.cpp
  main.cpp
)
target_link_libraries(test_resources_metadata
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_resources_metadata
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_resources_metadata
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 60
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Attribute unit tests (ported from upstream, #403) ---
# Hermetic: Attributes::parse + equality across SCALAR/RANGES/TEXT values.
# Carried test-for-test from upstream src/tests/attributes_tests.cpp.
add_executable(test_attributes
  attributes_tests.cpp
  main.cpp
)
target_link_libraries(test_attributes
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_attributes
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_attributes
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 60
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Offer-constraints filter unit tests (ported from upstream, #403) ---
# Hermetic: filter-level evaluation of scheduler-declared offer constraints
# (MESOS-10161) against node attributes. Carried test-for-test from upstream
# src/tests/master/offer_constraints_filter_tests.cpp. No cluster, no RE2
# beyond the filter's own limits.
add_executable(test_offer_constraints_filter
  offer_constraints_filter_tests.cpp
  main.cpp
)
target_link_libraries(test_offer_constraints_filter
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_offer_constraints_filter
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_offer_constraints_filter
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 60
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Reservation/format resource arithmetic tests (ported from upstream, #403) ---
# Hermetic: Resources reservation arithmetic (add/subtract/contains across
# static/dynamic reservations and labels), RESERVE/UNRESERVE operation
# application, stripped-resource forms, and pre/post-refinement format
# validation + downgrade. Carried from upstream src/tests/resources_tests.cpp
# (reservation-relevant suites). Guards the arithmetic that Resource metadata
# (#324) modified.
add_executable(test_resources_reservation
  resources_reservation_tests.cpp
  main.cpp
)
target_link_libraries(test_resources_reservation
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_resources_reservation
  PRIVATE ${CMAKE_BINARY_DIR}/include
)
gtest_discover_tests(test_resources_reservation
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 60
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Operator HTTP API tests (ported from legacy upstream tests, #111) ---
# Largest of the spine ports: controller's /api/v1 operator endpoints
# (GET_STATE, RESERVE/UNRESERVE, CREATE/DESTROY_VOLUMES, MAINTENANCE,
# QUOTA, WEIGHTS, GET_NODES, etc).
add_executable(test_api
  api_tests.cpp
  main.cpp
)
target_link_libraries(test_api
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_api
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_api
  DISCOVERY_TIMEOUT 60
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 300
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Scheduler HTTP API tests (ported from legacy upstream tests, #111) ---
# Exercises the controller's /api/v1/scheduler endpoint that V1 scheduler
# libraries (and any HTTP framework) post to (SUBSCRIBE, ACCEPT, etc).
add_executable(test_scheduler_http_api
  scheduler_http_api_tests.cpp
  main.cpp
)
target_link_libraries(test_scheduler_http_api
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_scheduler_http_api
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_scheduler_http_api
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Executor HTTP API tests (ported from legacy upstream tests, #111) ---
# Driver-using; exercises the node's /api/v1/executor endpoint that
# the V1 executor library posts to (SUBSCRIBE, UPDATE, MESSAGE, etc).
add_executable(test_executor_http_api
  executor_http_api_tests.cpp
  main.cpp
)
target_link_libraries(test_executor_http_api
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_executor_http_api
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_executor_http_api
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Resource offers tests (ported from legacy upstream tests, #111) ---
# Driver-using; exercises offer lifecycle: declined → reoffered, framework
# stop → reoffered, task-info errors → reoffered, explicit /request endpoint.
add_executable(test_resource_offers
  resource_offers_tests.cpp
  main.cpp
)
target_link_libraries(test_resource_offers
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_resource_offers
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_resource_offers
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 120
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Upgrade tests (ported from legacy upstream tests, #111) ---
# Driver-using; exercises version-gated registry upgrade paths. The
# #87-catcher: if MINIMUM_NODE_VERSION ever regresses, these fail.
add_executable(test_upgrade
  upgrade_tests.cpp
  main.cpp
)
target_link_libraries(test_upgrade
  PRIVATE
    medios-test-helpers-impl
    GTest::gmock
    GTest::gtest
)
target_include_directories(test_upgrade
  PRIVATE ${CMAKE_BINARY_DIR}/include
)

gtest_discover_tests(test_upgrade
  DISCOVERY_TIMEOUT 30
  DISCOVERY_MODE PRE_TEST
  PROPERTIES
    TIMEOUT 120
    ENVIRONMENT "LIBPROCESS_IP=127.0.0.1"
)

# --- Gang-placement simulation (#316) ---
# Spatial-only feasibility experiment: naive first-fit vs
# most-constrained-first (anchor) vs a best-fit-decreasing reference
# over synthetic clusters. Standalone main, no gtest, no medios deps,
# deliberately NOT registered with ctest. On demand:
#   ./sim_gang_placement [--trials N] [--nodes M] [--seed S]
#   ./sim_gang_placement --selfcheck   # reference vs exhaustive search
add_executable(sim_gang_placement
  sim_gang_placement.cpp
)

# --- Multi-framework contention simulation (#363) ---
add_executable(sim_framework_contention
  sim_framework_contention.cpp
  main.cpp
)
target_link_libraries(sim_framework_contention
  PRIVATE medios-test-helpers-impl GTest::gmock GTest::gtest)
target_include_directories(sim_framework_contention
  PRIVATE ${CMAKE_BINARY_DIR}/include)
gtest_discover_tests(sim_framework_contention
  DISCOVERY_TIMEOUT 60 DISCOVERY_MODE PRE_TEST
  PROPERTIES TIMEOUT 600 ENVIRONMENT "LIBPROCESS_IP=127.0.0.1")

# --- Etcd leader election (#359) ---
add_executable(test_etcd_election
  etcd_election_tests.cpp
  main.cpp
)
target_link_libraries(test_etcd_election
  PRIVATE medios-test-helpers-impl GTest::gmock GTest::gtest)
target_include_directories(test_etcd_election
  PRIVATE ${CMAKE_BINARY_DIR}/include)
gtest_discover_tests(test_etcd_election
  DISCOVERY_TIMEOUT 60 DISCOVERY_MODE PRE_TEST
  PROPERTIES TIMEOUT 300 ENVIRONMENT "LIBPROCESS_IP=127.0.0.1")

# --- Controller restart contract (DR-010) ---
add_executable(test_controller_restart
  controller_restart_tests.cpp
  main.cpp
)
target_link_libraries(test_controller_restart
  PRIVATE medios-test-helpers-impl GTest::gmock GTest::gtest)
target_include_directories(test_controller_restart
  PRIVATE ${CMAKE_BINARY_DIR}/include)
gtest_discover_tests(test_controller_restart
  DISCOVERY_TIMEOUT 60 DISCOVERY_MODE PRE_TEST
  PROPERTIES TIMEOUT 300 ENVIRONMENT "LIBPROCESS_IP=127.0.0.1")

# --- Ports resource regression (#335) ---
add_executable(test_ports_resource
  ports_resource_tests.cpp
  main.cpp
)
target_link_libraries(test_ports_resource
  PRIVATE medios-test-helpers-impl GTest::gmock GTest::gtest)
target_include_directories(test_ports_resource
  PRIVATE ${CMAKE_BINARY_DIR}/include)
gtest_discover_tests(test_ports_resource
  DISCOVERY_TIMEOUT 60 DISCOVERY_MODE PRE_TEST
  PROPERTIES TIMEOUT 300 ENVIRONMENT "LIBPROCESS_IP=127.0.0.1")
