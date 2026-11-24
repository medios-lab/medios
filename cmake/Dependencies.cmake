include(FetchContent)

# Per FAC3-0022 (#44): no protobuf-grpc, no gRPC.
# Per FAC3-0023 (#50): vendor everything via FetchContent for clone-and-build.
set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL
  "Avoid network updates for already-populated FetchContent dependencies."
)
set(BUILD_TESTING OFF CACHE INTERNAL "Disable vendored dependency test targets." FORCE)

# DR-033 (#182): suppress install() rules emitted by vendored FetchContent
# subprojects (protobuf, abseil, curl, zlib, leveldb, glog, ...). Without
# this, `cmake --install` walks into each vendored subdirectory and runs
# its install rules — installing their headers to <prefix>/include, cmake
# config files to <prefix>/lib/cmake, etc. We only want their runtime
# shared libraries in our install tree, which we handle ourselves via the
# install(CODE ...) glob in the root CMakeLists.txt.
#
# The mechanism is EXCLUDE_FROM_ALL on each FetchContent_Declare (CMake
# 3.28+). Per the docs: "Any install rules defined in the subdirectory
# or below will be ignored when installing the parent directory." This
# is per-subproject, unlike CMAKE_SKIP_INSTALL_RULES which is source-dir
# global only and cannot be scoped.

# --- Boost (selective) ---
# Boost.Regex is intentionally excluded (#496, FAC3-0022): Medios uses RE2 and
# std::regex exclusively.
# WARNING (#675): Never add `serialization` (or any Boost library that transitively
# depends on Boost::serialization) to BOOST_INCLUDE_LIBRARIES — Boost.Serialization
# carries an unfixed HIGH vulnerability (CVE-2026-11460) with a public exploit.
set(BOOST_INCLUDE_LIBRARIES circular_buffer filesystem icl lexical_cast range system uuid variant)
set(BOOST_ENABLE_CMAKE ON)
FetchContent_Declare(boost
  URL https://github.com/boostorg/boost/releases/download/boost-1.83.0/boost-1.83.0.tar.xz
  URL_HASH SHA256=c5a0688e1f0c05f354bbd0b32244d36085d9ffc9f932e8a18983a9908096f614
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  OVERRIDE_FIND_PACKAGE
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(boost)

if(NOT TARGET boost)
  add_library(boost INTERFACE)
  target_link_libraries(boost INTERFACE
    Boost::filesystem
    Boost::circular_buffer
    Boost::icl
    Boost::lexical_cast
    Boost::range
    Boost::system
    Boost::uuid
    Boost::variant
  )
endif()

# --- zlib ---
set(ZLIB_BUILD_EXAMPLES OFF CACHE INTERNAL "")
FetchContent_Declare(zlib
  GIT_REPOSITORY https://github.com/madler/zlib.git
  GIT_TAG 99623ef7097352da7993ba614161fd6a311e046e # v1.4
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(zlib)

# --- Protobuf (NOT protobuf-grpc) ---
set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "")
set(protobuf_BUILD_EXAMPLES OFF CACHE INTERNAL "")
set(protobuf_BUILD_SHARED_LIBS ON CACHE INTERNAL "")
FetchContent_Declare(protobuf
  GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
  GIT_TAG 4a2aef570deb2bfb8927426558701e8bfc26f2a4 # v25.3
  PATCH_COMMAND ${CMAKE_COMMAND}
    -DSRC=<SOURCE_DIR>
    -P ${CMAKE_SOURCE_DIR}/cmake/patches/abseil-arm64-msse41-fix.cmake
  OVERRIDE_FIND_PACKAGE
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(protobuf)

if(NOT TARGET protobuf)
  add_library(protobuf INTERFACE)
  target_link_libraries(protobuf INTERFACE protobuf::libprotobuf)
  target_include_directories(protobuf INTERFACE ${protobuf_SOURCE_DIR}/src)
endif()

# --- gflags ---
FetchContent_Declare(gflags
  GIT_REPOSITORY https://github.com/gflags/gflags.git
  GIT_TAG 8f30ebfb2b206a8bd3a97ffe3b59009a952343e6 # v2.3.1
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(gflags)

if(NOT TARGET gflags AND TARGET gflags::gflags)
  add_library(gflags INTERFACE)
  target_link_libraries(gflags INTERFACE gflags::gflags)
elseif(NOT TARGET gflags AND TARGET gflags_shared)
  add_library(gflags INTERFACE)
  target_link_libraries(gflags INTERFACE gflags_shared)
elseif(NOT TARGET gflags AND TARGET gflags_static)
  add_library(gflags INTERFACE)
  target_link_libraries(gflags INTERFACE gflags_static)
endif()

# --- glog ---
set(WITH_GFLAGS ON CACHE INTERNAL "")
FetchContent_Declare(glog
  GIT_REPOSITORY https://github.com/google/glog.git
  GIT_TAG 34b8da6496aec6a98277808701cfa834fae9801f # v0.7.0
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(glog)

if(NOT TARGET glog AND TARGET glog::glog)
  add_library(glog INTERFACE)
  target_link_libraries(glog INTERFACE glog::glog)
endif()

# --- GoogleTest ---
if(MEDIOS_BUILD_TESTS)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG b514bdc898e2951020cbdca1304b75f5950d1f59 # v1.15.2
    EXCLUDE_FROM_ALL
  )
  set(gtest_force_shared_crt ON CACHE INTERNAL "")
  FetchContent_MakeAvailable(googletest)
  if(NOT TARGET googletest AND TARGET GTest::gtest)
    add_library(googletest INTERFACE)
    target_link_libraries(googletest INTERFACE GTest::gtest)
  endif()
endif()

# --- picojson ---
set(FETCHCONTENT_UPDATES_DISCONNECTED_PICOJSON ON CACHE BOOL "" FORCE)
FetchContent_Declare(picojson
  GIT_REPOSITORY https://github.com/kazuho/picojson.git
  GIT_TAG 111c9be5188f7350c2eac9ddaedd8cca3d7bf394 # v1.3.0+PR135 (CVE-2018-11793)
  EXCLUDE_FROM_ALL
)

FetchContent_GetProperties(picojson)
if(NOT picojson_SOURCE_DIR)
  set(picojson_SOURCE_DIR "${FETCHCONTENT_BASE_DIR}/picojson-src")
endif()
if(NOT picojson_POPULATED AND NOT EXISTS "${picojson_SOURCE_DIR}/picojson.h")
  if(POLICY CMP0169)
    cmake_policy(PUSH)
    cmake_policy(SET CMP0169 OLD)
  endif()
  FetchContent_Populate(picojson)
  if(POLICY CMP0169)
    cmake_policy(POP)
  endif()
endif()

if(NOT TARGET picojson)
  add_library(picojson INTERFACE)
  target_include_directories(picojson INTERFACE ${picojson_SOURCE_DIR})
endif()

# --- RE2 ---
set(RE2_BUILD_TESTING OFF CACHE INTERNAL "")
FetchContent_Declare(re2
  GIT_REPOSITORY https://github.com/google/re2.git
  GIT_TAG 3a8436ac436124a57a4e22d5c8713a2d42b381d7 # 2023-03-01
  PATCH_COMMAND ${CMAKE_COMMAND}
    -DSRC=<SOURCE_DIR>
    -P ${CMAKE_SOURCE_DIR}/cmake/patches/re2-simplify-empty-width.cmake
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(re2)

if(NOT TARGET re2::re2 AND TARGET re2)
  add_library(re2::re2 ALIAS re2)
endif()

# --- cURL ---
set(BUILD_CURL_EXE OFF CACHE INTERNAL "")
set(BUILD_EXAMPLES OFF CACHE INTERNAL "")
set(BUILD_LIBCURL_DOCS OFF CACHE INTERNAL "")
set(BUILD_MISC_DOCS OFF CACHE INTERNAL "")
set(ENABLE_CURL_MANUAL OFF CACHE INTERNAL "")
set(CURL_USE_LIBPSL OFF CACHE INTERNAL "")
set(CURL_USE_LIBSSH2 OFF CACHE INTERNAL "")
# The fetcher downloads https URIs (#270, #646), so cURL needs a TLS
# backend. OpenSSL 3 is the one TLS dependency FAC3-0022 accepts, linked from
# the system as libprocess does. CURL_CA_BUNDLE stays "auto": cURL compiles in
# the first CA bundle it finds on the build host, which on Ubuntu and macOS is
# the path the node host uses. Major version 3 on every platform: Ubuntu
# 24.04 ships only 3, while Homebrew links 4 by default, so macOS configures
# with -DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3).
find_package(OpenSSL 3.0...<4 REQUIRED)
set(CURL_USE_OPENSSL ON CACHE INTERNAL "")
# Off so TLS does not also bring in HTTP/2 from a host libnghttp2.
set(USE_NGHTTP2 OFF CACHE INTERNAL "")
# Left unset, cURL runs its own find_package(ZLIB): nothing when the build
# host has no system zlib, but a bare, unexported ZLIB::ZLIB imported target
# when it does, which collides with the vendored one above and fails cURL's
# own install(EXPORT) at generate time (medios#508). Off rather than pointed
# at the vendored target: nothing in this tree sets CURLOPT_ACCEPT_ENCODING or
# otherwise asks curl to decompress a response, so the capability this
# removes is one nothing here exercises, and turning it off is what stops the
# build consulting the host's zlib at all (FAC3-0022).
set(CURL_ZLIB OFF CACHE INTERNAL "")
# Every protocol except http and, with a TLS backend, https is turned off
# (#386), one CURL_DISABLE_* option per protocol. cURL 8.23.0 removed the
# HTTP_ONLY switch that set these, and an unknown cache variable is ignored
# without a warning (#681). FetcherTest.CurlProtocolsAreHttpAndHttps fails
# if a later release adds a protocol this list doesn't name. SCP and SFTP
# need libssh, which is not built.
set(CURL_USE_LIBSSH OFF CACHE INTERNAL "")
set(CURL_ENABLE_NTLM OFF CACHE INTERNAL "")
foreach(_curl_protocol
    DICT FILE FTP GOPHER IMAP IPFS LDAP LDAPS MQTT POP3 RTSP SMTP TELNET TFTP
    WEBSOCKETS)
  set(CURL_DISABLE_${_curl_protocol} ON CACHE INTERNAL "")
endforeach()
FetchContent_Declare(curl
  URL https://curl.se/download/curl-8.23.0.tar.xz
  URL_HASH SHA256=8dadd7d2536a314f982ad05a651b713f426bf7d02cc2c76cffb9796f2218e6c5
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(curl)

if(TARGET CURL::libcurl AND NOT TARGET libcurl)
  add_library(libcurl INTERFACE)
  target_link_libraries(libcurl INTERFACE CURL::libcurl)
elseif(TARGET libcurl_static AND NOT TARGET libcurl)
  add_library(libcurl INTERFACE)
  target_link_libraries(libcurl INTERFACE libcurl_static)
endif()

# --- LevelDB ---
set(LEVELDB_BUILD_TESTS OFF CACHE INTERNAL "")
set(LEVELDB_BUILD_BENCHMARKS OFF CACHE INTERNAL "")
set(LEVELDB_INSTALL OFF CACHE INTERNAL "")
FetchContent_Declare(leveldb
  GIT_REPOSITORY https://github.com/google/leveldb.git
  GIT_TAG 99b3c03b3284f5886f9ef9a4ef703d57373e61be # 1.23
  PATCH_COMMAND ${CMAKE_COMMAND}
    -DSRC=<SOURCE_DIR>
    -P ${CMAKE_SOURCE_DIR}/cmake/patches/leveldb-cxx20-memory-order.cmake
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(leveldb)

if(TARGET leveldb)
  set_property(TARGET leveldb PROPERTY INTERFACE_COMPILE_OPTIONS "")
endif()

# --- llhttp ---
# Replaces http-parser (#412): http-parser is archived/unmaintained and
# carries an unfixed request-smuggling CVE (CVE-2020-8287) with no upstream
# fix ever landing. llhttp is Node.js's own replacement for this exact
# library. Its release tags ship pre-generated C source, so this is a normal
# FetchContent vendor like the other deps rather than http-parser's manual
# populate (which was only needed because http-parser ships a bare Makefile,
# not a CMakeLists.txt).
set(LLHTTP_BUILD_SHARED_LIBS OFF CACHE INTERNAL "")
set(LLHTTP_BUILD_STATIC_LIBS ON CACHE INTERNAL "")
FetchContent_Declare(llhttp
  GIT_REPOSITORY https://github.com/nodejs/llhttp.git
  GIT_TAG 0e815792b167a9bd8ace259b95b7da953776c288 # release/v9.4.3
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(llhttp)

# --- libev ---
FetchContent_Declare(libev
  URL http://dist.schmorp.de/libev/libev-4.33.tar.gz
  URL_HASH SHA256=507eb7b8d1015fbec5b935f34ebed15bf346bed04a11ab82b8eee848c4205aea
  PATCH_COMMAND ${CMAKE_COMMAND}
    -DSRC=<SOURCE_DIR>
    -P ${CMAKE_SOURCE_DIR}/cmake/patches/libev-nosigmask-polarity.cmake
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  EXCLUDE_FROM_ALL
)

FetchContent_GetProperties(libev)
if(NOT libev_POPULATED)
  if(POLICY CMP0169)
    cmake_policy(PUSH)
    cmake_policy(SET CMP0169 OLD)
  endif()
  FetchContent_Populate(libev)
  if(POLICY CMP0169)
    cmake_policy(POP)
  endif()
endif()

if(NOT TARGET libev)
  add_library(libev STATIC ${libev_SOURCE_DIR}/ev.c)
  target_include_directories(libev PUBLIC ${libev_SOURCE_DIR})
  target_compile_definitions(libev PUBLIC EV_STANDALONE=1)
endif()

# --- elfio (header-only ELF parser) ---
# Used by stout/elf.hpp for the Nvidia GPU isolator's library dependency
# resolution. Was previously a placeholder INTERFACE library with no
# include path; surfaced as a fatal include error on 2026-05-12 BUILD-007
# real-compile pass against src/node/containerizer/medios/isolators/gpu/volume.cpp.
FetchContent_Declare(elfio
  GIT_REPOSITORY https://github.com/serge1/ELFIO.git
  GIT_TAG 06097d962cbbbc1b2e6ea8aab25aac7f6108df26 # Release_3.9
  PATCH_COMMAND ${CMAKE_COMMAND}
    -DSRC=<SOURCE_DIR>
    -P ${CMAKE_SOURCE_DIR}/cmake/patches/elfio-section-bounds.cmake
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(elfio)

if(NOT TARGET elfio AND TARGET elfio::elfio)
  add_library(elfio INTERFACE)
  target_link_libraries(elfio INTERFACE elfio::elfio)
elseif(NOT TARGET elfio)
  add_library(elfio INTERFACE)
  target_include_directories(elfio INTERFACE ${elfio_SOURCE_DIR})
endif()

# --- libseccomp (MEDIOS_ENABLE_SECCOMP_ISOLATOR only) ---
# An autotools project, so it builds as an ExternalProject into a static
# library rather than through FetchContent. PIC because libmedios is shared.
# Version 2.4.4 is a parameter. Upstream pinned 2.3.3, whose syscall
# table has no clone3, and the seccomp parser refuses a profile naming a
# syscall libseccomp cannot resolve. A profile has to name clone3 to
# return ENOSYS for it, which is how glibc 2.34 and later fall back to
# clone. 2.5.x would add newer syscalls but needs gperf to build, which
# is not a build prerequisite here.
if(MEDIOS_ENABLE_SECCOMP_ISOLATOR)
  include(ExternalProject)
  set(LIBSECCOMP_INSTALL_DIR ${CMAKE_BINARY_DIR}/_deps/libseccomp-install)
  ExternalProject_Add(libseccomp-build
    URL https://github.com/seccomp/libseccomp/releases/download/v2.4.4/libseccomp-2.4.4.tar.gz
    URL_HASH SHA256=4e79738d1ef3c9b7ca9769f1f8b8d84fc17143c2c1c432e53b9c64787e0ff3eb
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    PREFIX ${CMAKE_BINARY_DIR}/_deps/libseccomp
    CONFIGURE_COMMAND <SOURCE_DIR>/configure
      --disable-shared --enable-static --with-pic
      --prefix=${LIBSECCOMP_INSTALL_DIR}
    BUILD_COMMAND make
    INSTALL_COMMAND make install
    BUILD_BYPRODUCTS ${LIBSECCOMP_INSTALL_DIR}/lib/libseccomp.a
  )

  # The imported target names the include directory before the build
  # creates it, and CMake refuses a missing one.
  file(MAKE_DIRECTORY ${LIBSECCOMP_INSTALL_DIR}/include)
  add_library(libseccomp STATIC IMPORTED GLOBAL)
  add_dependencies(libseccomp libseccomp-build)
  set_target_properties(libseccomp PROPERTIES
    IMPORTED_LOCATION ${LIBSECCOMP_INSTALL_DIR}/lib/libseccomp.a
    INTERFACE_INCLUDE_DIRECTORIES ${LIBSECCOMP_INSTALL_DIR}/include
  )
endif()

# --- Lightweight placeholders for restored upstream CMake fragments ---
# BUILD-004 will replace these with real FetchContent entries or remove the
# corresponding legacy includes when compilation starts.
foreach(target apr libarchive libcurl concurrentqueue svn)
  if(NOT TARGET ${target})
    add_library(${target} INTERFACE)
  endif()
endforeach()

# --- Not vendored ---
# - OpenSSL 3: linked from the system. cURL always needs it; libprocess also
#   links it when MEDIOS_ENABLE_SSL is ON.
# - OTel SDK (DR-016 #17): observability comes in PHASE-B.
# - gRPC: explicitly never added per FAC3-0022 (#44).
