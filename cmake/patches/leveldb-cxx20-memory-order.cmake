file(READ "${SRC}/util/env_posix.cc" _CONTENT)
string(REPLACE
  "std::memory_order::memory_order_relaxed"
  "std::memory_order_relaxed"
  _CONTENT "${_CONTENT}")
file(WRITE "${SRC}/util/env_posix.cc" "${_CONTENT}")

include("${CMAKE_CURRENT_LIST_DIR}/leveldb-footer-decode-bounds.cmake")
