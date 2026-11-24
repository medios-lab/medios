cmake_minimum_required(VERSION 3.28)
# 3.24 originally required for FetchContent OVERRIDE_FIND_PACKAGE (BUILD-010
# / #76). 3.28 raised for FetchContent_Declare EXCLUDE_FROM_ALL per DR-033
# (#182), used in cmake/Dependencies.cmake to keep vendored subprojects'
# install rules from polluting medios's install tree. Ubuntu 24.04 ships
# 3.28; macOS via `brew install cmake` gives current.

# Boost 1.83 still has subdirectories with older cmake_minimum_required()
# values. CMake 4.x needs an explicit policy floor for those vendored deps.
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

# DR-020 (#35) - Linux/POSIX only.
if(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "Windows")
  message(FATAL_ERROR "Medios is Linux/POSIX only (DR-020 / #35)")
endif()

project(medios
  VERSION 1.0.0
  DESCRIPTION "Thin fork of Apache Mesos for agentic and AI workloads"
  LANGUAGES C CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# DR-033 (#182): install layout. `cmake --install build --prefix <p>`
# produces a relocatable <p>/bin + <p>/lib tree. Installed binaries find
# their libraries via @loader_path/../lib (macOS) or $ORIGIN/../lib
# (Linux), with no LD_LIBRARY_PATH / system config / fixed prefix required.
#
# - GNUInstallDirs gives portable ${CMAKE_INSTALL_BINDIR}/${CMAKE_INSTALL_LIBDIR}.
# - CMAKE_SKIP_INSTALL_RPATH ON disables CMake's automatic install-time rpath
#   rewrite ENTIRELY. We then rewrite rpaths ourselves in the install(CODE)
#   block at the bottom of this file, via install_name_tool (macOS) or
#   patchelf (Linux). Why disable CMake's version:
#     * macOS: CMake invokes install_name_tool with all -delete_rpath args
#       in a single atomic command; ANY missing rpath in the binary aborts
#       the whole rewrite (and leaves absolute build-tree rpaths in place).
#     * Linux: CMake validates that the existing RUNPATH literally matches
#       the path string it expects to delete; any mismatch aborts the install
#       script with "the file ... does not contain ... as was expected".
#   Both behaviors are too strict for our build, where vendored deps' rpath
#   structure doesn't reliably match CMake's reconstructed expectation. Our
#   own rewrite handles each binary one-at-a-time and tolerates per-target
#   failures.
# - Build-tree binaries keep their auto-computed absolute rpaths so the
#   build itself can invoke build-tree tools like protoc.
include(GNUInstallDirs)
set(CMAKE_SKIP_INSTALL_RPATH ON)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)

# Linux: locate patchelf at configure time. We deliberately do NOT use
# `find_program(... REQUIRED)` because most of the build (compile, link,
# ctest) doesn't need patchelf — only `cmake --install` does. Missing
# patchelf is detected at install-script time with a clear error message
# (see install(CODE) block below) so a developer building without intent
# to install isn't blocked.
if(NOT APPLE AND NOT WIN32)
  find_program(MEDIOS_PATCHELF_EXECUTABLE patchelf)
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

option(MEDIOS_ENABLE_SANITIZERS "Enable ASan + UBSan" OFF)
option(MEDIOS_BUILD_TESTS "Build tests" ON)

# NVML (NVIDIA Management Library) is what GPU discovery and the NVIDIA
# isolator talk to. The wrapper in src/modules/gpu/nvml.cpp dlopens the
# driver library at runtime, so a build with this ON still runs on hosts
# without a driver; what the option controls is whether the wrapper is
# compiled against the driver header at all. OFF compiles it as a stub that
# reports NVML unavailable everywhere. Explicit rather than autodetected:
# a GPU build that quietly degraded to the stub is how #453 happened.
option(MEDIOS_ENABLE_NVML
  "Compile the NVML wrapper against the driver header (nvml.h); dlopened at runtime"
  OFF)
set(MEDIOS_NVML_INCLUDE_DIR "" CACHE PATH
  "Directory containing nvml.h, if not in a default location")
if(MEDIOS_ENABLE_NVML)
  find_path(NVML_INCLUDE_DIR nvml.h
    HINTS ${MEDIOS_NVML_INCLUDE_DIR}
    PATHS /usr/local/cuda/include /usr/include
    PATH_SUFFIXES nvidia/gdk
  )
  if(NOT NVML_INCLUDE_DIR)
    message(FATAL_ERROR
      "MEDIOS_ENABLE_NVML=ON but nvml.h was not found. Looked under "
      "MEDIOS_NVML_INCLUDE_DIR='${MEDIOS_NVML_INCLUDE_DIR}', "
      "/usr/local/cuda/include and /usr/include (also .../nvidia/gdk). "
      "Install the CUDA toolkit headers or pass -DMEDIOS_NVML_INCLUDE_DIR.")
  endif()
  message(STATUS "NVML: enabled, header at ${NVML_INCLUDE_DIR}")
else()
  message(STATUS
    "NVML: disabled (MEDIOS_ENABLE_NVML=OFF); GPU discovery compiles as a stub")
endif()

# The `linux/seccomp` isolator. Off by default, as upstream had it: a
# node offers it only from a build that asked for it. ON vendors
# libseccomp (cmake/Dependencies.cmake).
option(MEDIOS_ENABLE_SECCOMP_ISOLATOR
  "Build the linux/seccomp isolator and vendor libseccomp (Linux only)"
  OFF)
if(MEDIOS_ENABLE_SECCOMP_ISOLATOR AND NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR
    "MEDIOS_ENABLE_SECCOMP_ISOLATOR=ON needs a Linux build; seccomp is a "
    "Linux kernel facility.")
endif()

# TLS on the libprocess transport. OFF by default like the other feature
# options: a build that wants TLS says so and fails at configure when
# OpenSSL is missing, rather than taking whichever shape the host's
# libraries happen to permit. ON compiles the OpenSSL socket into
# libprocess and links OpenSSL from the system (find_package, not vendored;
# FAC3-0022 lists it as the one unavoidable TLS dependency). Whether a
# given process speaks TLS is then decided at runtime by LIBPROCESS_SSL_*;
# see docs/TLS.md.
option(MEDIOS_ENABLE_SSL
  "Compile libprocess with TLS (OpenSSL); LIBPROCESS_SSL_* turns it on at runtime"
  OFF)
if(MEDIOS_ENABLE_SSL)
  find_package(OpenSSL REQUIRED)
  message(STATUS
    "SSL: enabled, OpenSSL ${OPENSSL_VERSION} (${OPENSSL_SSL_LIBRARY})")
else()
  message(STATUS
    "SSL: disabled (MEDIOS_ENABLE_SSL=OFF); libprocess speaks plaintext only")
endif()

include(${CMAKE_SOURCE_DIR}/cmake/Dependencies.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/include/medios)
configure_file(
  ${CMAKE_SOURCE_DIR}/cmake/medios_version.hpp.in
  ${CMAKE_BINARY_DIR}/include/medios/version.hpp
  @ONLY
)

string(TIMESTAMP MEDIOS_BUILD_DATE "%Y-%m-%d %H:%M:%S")
string(TIMESTAMP MEDIOS_BUILD_TIME "%s")

execute_process(
  COMMAND whoami
  OUTPUT_VARIABLE MEDIOS_BUILD_USER
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE MEDIOS_GIT_SHA
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

execute_process(
  COMMAND git rev-parse --abbrev-ref HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE MEDIOS_GIT_BRANCH
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

execute_process(
  COMMAND git describe --tags --exact-match
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE MEDIOS_GIT_TAG
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

if(NOT "${MEDIOS_GIT_SHA}" STREQUAL "")
  set(BUILD_GIT_SHA "${MEDIOS_GIT_SHA}")
endif()
if(NOT "${MEDIOS_GIT_BRANCH}" STREQUAL "")
  set(BUILD_GIT_BRANCH "${MEDIOS_GIT_BRANCH}")
endif()
if(NOT "${MEDIOS_GIT_TAG}" STREQUAL "")
  set(BUILD_GIT_TAG "${MEDIOS_GIT_TAG}")
endif()

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/src/common)
configure_file(
  ${CMAKE_SOURCE_DIR}/cmake/build_config.hpp.in
  ${CMAKE_BINARY_DIR}/src/common/build_config.hpp
  @ONLY
)
configure_file(
  ${CMAKE_SOURCE_DIR}/cmake/git_version.hpp.in
  ${CMAKE_BINARY_DIR}/src/common/git_version.hpp
  @ONLY
)

# OpenAPI spec embedding (#261). Embed the per-daemon OpenAPI 3.x JSON specs
# as string constants so the daemons can serve them at /openapi.json and
# /.well-known/openapi.json without a runtime filesystem dependency.
file(READ "${CMAKE_SOURCE_DIR}/api/openapi/controller.openapi.json"
     MEDIOS_CONTROLLER_OPENAPI_JSON)
file(READ "${CMAKE_SOURCE_DIR}/api/openapi/node.openapi.json"
     MEDIOS_NODE_OPENAPI_JSON)
configure_file(
  ${CMAKE_SOURCE_DIR}/cmake/openapi_embedded.hpp.in
  ${CMAKE_BINARY_DIR}/src/common/openapi_embedded.hpp
  @ONLY
)

# In-tree libraries restored in BUILD-001 (#51); ported per DR-019 in PHASE-C
# (#41). libprocess keeps its own ENABLE_SSL switch; MEDIOS_ENABLE_SSL is
# the one a build sets, so mirror it rather than expose two knobs.
set(ENABLE_SSL ${MEDIOS_ENABLE_SSL} CACHE BOOL "Mirrors MEDIOS_ENABLE_SSL" FORCE)
set(ENABLE_LIBEVENT OFF CACHE BOOL "Use libev for PHASE-A" FORCE)
set(ENABLE_INSTALL_MODULE_DEPENDENCIES OFF CACHE BOOL "" FORCE)

# BUILD-007 keeps new tests in the top-level tests/ harness; legacy
# stout/libprocess test trees are not part of this CTest surface.
set(_MEDIOS_BUILD_ROOT_TESTS ${MEDIOS_BUILD_TESTS})
set(MEDIOS_BUILD_TESTS OFF)
add_subdirectory(stout)
add_subdirectory(libprocess)
set(MEDIOS_BUILD_TESTS ${_MEDIOS_BUILD_ROOT_TESTS})
unset(_MEDIOS_BUILD_ROOT_TESTS)

add_subdirectory(src)

if(MEDIOS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# DR-033 (#182): unified install(CODE) block. Copies medios binaries +
# libmedios + libprocess + all vendored runtime shared libraries into
# <prefix>/bin and <prefix>/lib, then rewrites rpaths to be relocatable.
#
# We bypass install(TARGETS) entirely. Reasons:
#   * macOS: CMake's install-time rpath rewrite via install_name_tool issues
#     all -delete_rpath args in one atomic command; ANY missing rpath in
#     the binary aborts the rewrite and leaves absolute build-tree paths.
#   * Linux: CMake's file(RPATH_CHANGE) validates the existing RUNPATH
#     literally matches the OLD_RPATH string it reconstructed; any
#     mismatch (e.g. due to vendored deps' link order) aborts the install
#     script with "could not write new RPATH ... as was expected".
#   Both behaviors were too strict against the rpath structure FetchContent
#   vendored deps actually produce. The unified install(CODE) approach
#   does copy + patchelf in one pass with no per-target friction.
#
# End state: every installed binary/lib has a single relocatable rpath.
#   - macOS executables in bin/: @loader_path/../lib
#   - macOS dylibs in lib/:      @loader_path
#   - Linux executables in bin/: $ORIGIN/../lib
#   - Linux .so libs in lib/:    $ORIGIN
#
# Linux requires patchelf. Detected at configure time (above); enforced
# at install-script time with a clear FATAL_ERROR if missing.

install(CODE "set(MEDIOS_PATCHELF_EXECUTABLE \"${MEDIOS_PATCHELF_EXECUTABLE}\")")
install(CODE "set(MEDIOS_BUILD_DIR \"${CMAKE_BINARY_DIR}\")")
install(CODE [[
  # file(GLOB) in step 4 returns absolute paths, and the bin/ vs lib/
  # discrimination there is a string comparison against these two. A
  # relative --prefix would make every comparison false and give bin/
  # the library rpath, so absolutize the prefix before deriving them.
  get_filename_component(_PREFIX "${CMAKE_INSTALL_PREFIX}" ABSOLUTE)
  set(_BIN_DIR "${_PREFIX}/bin")
  set(_LIB_DIR "${_PREFIX}/lib")

  if(NOT APPLE AND NOT MEDIOS_PATCHELF_EXECUTABLE)
    message(FATAL_ERROR
      "DR-033 install layer requires patchelf on Linux to rewrite "
      "installed binaries' DT_RUNPATH to $ORIGIN/../lib. Install it "
      "with `sudo apt install patchelf` (Debian/Ubuntu), `sudo dnf "
      "install patchelf` (Fedora/RHEL), or build from "
      "https://github.com/NixOS/patchelf, then re-run "
      "`cmake -B build` and `cmake --install build --prefix ...`. "
      "Build/test does not require patchelf; only `cmake --install` "
      "does. See DR-033 (#182) and docs/DEVELOPMENT.md.")
  endif()

  file(MAKE_DIRECTORY "${_BIN_DIR}")
  file(MAKE_DIRECTORY "${_LIB_DIR}")

  # 1. Copy medios executables to <prefix>/bin.
  #
  # Paths are fixed by the build layout in src/CMakeLists.txt and
  # src/{controller,node}/CMakeLists.txt. Listed explicitly so an
  # accidental rename in those files surfaces here as a missing-file
  # error rather than silently dropping the binary.
  set(_MEDIOS_BIN_TARGETS
    "src/controller/medios-controller"
    "src/node/medios-node"
    "src/medios-execute"
    "src/medios-fetcher"
    "src/medios-executor"
    "src/medios-default-executor"
    "src/medios-docker-executor"
    "src/medios-containerizer"
    "src/medios-tcp-connect"
    "src/medios-io-switchboard"
  )
  foreach(_rel IN LISTS _MEDIOS_BIN_TARGETS)
    set(_src "${MEDIOS_BUILD_DIR}/${_rel}")
    if(NOT EXISTS "${_src}")
      message(FATAL_ERROR "DR-033 install: expected binary not found: ${_src}")
    endif()
    file(INSTALL "${_src}" DESTINATION "${_BIN_DIR}" USE_SOURCE_PERMISSIONS)
  endforeach()

  # 2. Copy libmedios + libprocess to <prefix>/lib.
  if(APPLE)
    set(_LIB_EXT "dylib")
  else()
    set(_LIB_EXT "so")
  endif()
  # The GPU module (src/modules/gpu) is a loadable library the node's
  # --modules manifest names at <prefix>/lib/libmedios_gpu.so; it is
  # built into src/.libs and installs with the project's own libraries.
  file(GLOB _MEDIOS_OWN_LIBS
    "${MEDIOS_BUILD_DIR}/src/libmedios.${_LIB_EXT}*"
    "${MEDIOS_BUILD_DIR}/libprocess/src/libprocess.${_LIB_EXT}*"
    "${MEDIOS_BUILD_DIR}/src/.libs/libmedios_gpu.${_LIB_EXT}"
  )
  if(_MEDIOS_OWN_LIBS)
    file(INSTALL ${_MEDIOS_OWN_LIBS}
      DESTINATION "${_LIB_DIR}"
      USE_SOURCE_PERMISSIONS
    )
  endif()

  # 3. Copy vendored runtime shared libraries to <prefix>/lib.
  # All .so/.dylib files under build/_deps/. Vendored projects'
  # own install() rules are suppressed in cmake/Dependencies.cmake
  # via EXCLUDE_FROM_ALL on FetchContent_Declare, so we copy only
  # the runtime shared libraries we actually need.
  if(APPLE)
    file(GLOB_RECURSE _MEDIOS_VENDORED_LIBS
      "${MEDIOS_BUILD_DIR}/_deps/*.dylib"
    )
  else()
    file(GLOB_RECURSE _MEDIOS_VENDORED_LIBS
      "${MEDIOS_BUILD_DIR}/_deps/*.so"
      "${MEDIOS_BUILD_DIR}/_deps/*.so.[0-9]*"
    )
  endif()
  if(_MEDIOS_VENDORED_LIBS)
    file(INSTALL ${_MEDIOS_VENDORED_LIBS}
      DESTINATION "${_LIB_DIR}"
      USE_SOURCE_PERMISSIONS
    )
  endif()

  # 4. Glob the just-installed files for the rpath rewrite.
  if(APPLE)
    file(GLOB _MEDIOS_INSTALLED_BINS "${_BIN_DIR}/*")
    file(GLOB _MEDIOS_INSTALLED_LIBS "${_LIB_DIR}/*.dylib")
  else()
    file(GLOB _MEDIOS_INSTALLED_BINS "${_BIN_DIR}/*")
    file(GLOB _MEDIOS_INSTALLED_LIBS "${_LIB_DIR}/*.so" "${_LIB_DIR}/*.so.[0-9]*")
  endif()

  foreach(_target IN LISTS _MEDIOS_INSTALLED_BINS _MEDIOS_INSTALLED_LIBS)
    if(IS_SYMLINK "${_target}")
      continue()
    endif()

    # Choose the new rpath: executables in bin/ load libs in ../lib;
    # libraries in lib/ load sibling libs in the same directory.
    get_filename_component(_target_dir "${_target}" DIRECTORY)
    if(APPLE)
      if(_target_dir STREQUAL _BIN_DIR)
        set(_new_rpath "@loader_path/../lib")
      else()
        set(_new_rpath "@loader_path")
      endif()
    else()
      if(_target_dir STREQUAL _BIN_DIR)
        set(_new_rpath "$ORIGIN/../lib")
      else()
        set(_new_rpath "$ORIGIN")
      endif()
    endif()

    if(APPLE)
      # Extract current LC_RPATH entries from Mach-O headers. otool format:
      #   cmd LC_RPATH
      #   cmdsize <n>
      #          path <PATH> (offset 12)
      execute_process(
        COMMAND otool -l "${_target}"
        OUTPUT_VARIABLE _otool_out
        ERROR_QUIET
      )

      string(REGEX MATCHALL "path [^\n]+ \\(offset" _path_lines "${_otool_out}")

      foreach(_line IN LISTS _path_lines)
        string(REGEX REPLACE "^path " "" _rpath "${_line}")
        string(REGEX REPLACE " \\(offset$" "" _rpath "${_rpath}")

        # Skip rpaths that are already loader-relative; only delete
        # absolute build-tree paths.
        if(_rpath MATCHES "^@")
          continue()
        endif()

        execute_process(
          COMMAND install_name_tool -delete_rpath "${_rpath}" "${_target}"
          OUTPUT_QUIET ERROR_QUIET
          RESULT_VARIABLE _itrc
        )
        # _itrc != 0 is fine — rpath wasn't present; move on.
      endforeach()

      execute_process(
        COMMAND install_name_tool -add_rpath "${_new_rpath}" "${_target}"
        OUTPUT_QUIET ERROR_QUIET
      )
    else()
      # Linux: patchelf --set-rpath replaces DT_RUNPATH/DT_RPATH entirely
      # in a single operation, no atomic-failure issue. No need to
      # enumerate the old rpaths.
      #
      # The vendored-lib glob in the earlier install(CODE) block scoops
      # up every .so / .so.N* under build/_deps, which can include things
      # patchelf cannot rewrite (Python C extensions, non-ELF binaries,
      # etc. — e.g. abseil ships an entropy.so under random_internal
      # that isn't a regular shared library). Treat per-target patchelf
      # failures as warnings, not fatal errors: the install proceeds and
      # the file lands in lib/ even if its rpath wasn't rewritten. If a
      # file we ACTUALLY load at runtime can't be patched, the binary
      # will fail to load and the cause will be visible at startup.
      execute_process(
        COMMAND "${MEDIOS_PATCHELF_EXECUTABLE}"
                --force-rpath
                --set-rpath "${_new_rpath}"
                "${_target}"
        RESULT_VARIABLE _peerc
        OUTPUT_VARIABLE _peout
        ERROR_VARIABLE _peerr
      )
      if(NOT _peerc EQUAL 0)
        message(WARNING
          "patchelf could not rewrite rpath on ${_target} "
          "(probably not an ELF file we need at runtime): ${_peerr}")
      endif()
    endif()
  endforeach()
]])
