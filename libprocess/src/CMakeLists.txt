# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# SOURCE FILES FOR THE PROCESS LIBRARY.
#######################################
set(PROCESS_SRC
  authenticator.cpp
  authenticator_manager.cpp
  clock.cpp
  firewall.cpp
  gtest_constants.cpp
  help.cpp
  http.cpp
  http_proxy.cpp
  io.cpp
  latch.cpp
  logging.cpp
  memory_profiler.cpp
  metrics/metrics.cpp
  mime.cpp
  pid.cpp
  process.cpp
  profiler.cpp
  reap.cpp
  socket.cpp
  subprocess.cpp
  time.cpp)


list(APPEND PROCESS_SRC
  posix/subprocess.cpp)

if (ENABLE_LIBEVENT)
  list(APPEND PROCESS_SRC
    posix/libevent/libevent.cpp
    posix/libevent/libevent_poll.cpp)
else ()
  list(APPEND PROCESS_SRC
    posix/libev/libev.cpp
    posix/libev/libev_poll.cpp)
endif ()

list(APPEND PROCESS_SRC
  posix/io.cpp
  posix/poll_socket.cpp)

if (ENABLE_SSL)
  list(APPEND PROCESS_SRC
    jwt.cpp
    jwt_authenticator.cpp
    openssl.cpp
    ssl/utilities.cpp)

  if (ENABLE_LIBEVENT)
    list(APPEND PROCESS_SRC
      posix/libevent/libevent_ssl_socket.cpp)
  else ()
    list(APPEND PROCESS_SRC
      ssl/openssl_socket.cpp)
  endif ()
endif ()

add_library(process ${PROCESS_SRC})

target_link_libraries(
  process PUBLIC
  stout
  llhttp::llhttp)

# USE_SSL_SOCKET changes the shape of public headers (socket.hpp, http.hpp,
# authenticator.hpp), so every consumer of libprocess has to see it, not
# only the sources compiled here. process/ssl/*.hpp and process/jwt.hpp
# include OpenSSL headers, hence the PUBLIC link.
if (ENABLE_SSL)
  target_compile_definitions(process PUBLIC USE_SSL_SOCKET)
  target_link_libraries(process PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif ()

  # We need openssl and pthreads if we have ENABLE_LIBEVENT
  # Otherwise libprocess will not link properly and we get undefined reference
if (ENABLE_LIBEVENT)
  set(LIBEVENT_DEPENDENCIES libevent libevent_openssl libevent_pthreads)
else ()
  set(LIBEVENT_DEPENDENCIES libev)
endif ()

# Link libraries
target_link_libraries(
  process PRIVATE
  concurrentqueue
  ${LIBEVENT_DEPENDENCIES})

target_compile_definitions(
  process PRIVATE
  $<$<BOOL:${ENABLE_LOCK_FREE_RUN_QUEUE}>:LOCK_FREE_RUN_QUEUE>
  $<$<BOOL:${ENABLE_LOCK_FREE_EVENT_QUEUE}>:LOCK_FREE_EVENT_QUEUE>
  $<$<BOOL:${ENABLE_LAST_IN_FIRST_OUT_FIXED_SIZE_SEMAPHORE}>:LAST_IN_FIRST_OUT_FIXED_SIZE_SEMAPHORE>
  $<$<PLATFORM_ID:Linux>:LIBPROCESS_ALLOW_JEMALLOC>)

target_include_directories(process PUBLIC ../include)
target_include_directories(process PRIVATE .)
target_link_libraries(process PRIVATE medios_compile_flags)

# DR-033 (#182): libprocess install is handled by the install(CODE) block
# in the root CMakeLists, alongside libmedios and the vendored deps. The
# previous install(TARGETS) approach lost to CMake's install-time rpath
# rewrite, which validates the existing RUNPATH string and bails on any
# mismatch with what CMake expects. The unified install(CODE) approach
# copies and patchelf's everything in one pass with no per-target friction.
