# MEDIOS PATCH for issues #423 and #676:
#
# 1. LevelDB 1.23 `Footer::DecodeFrom(Slice* input)` (`table/format.cc`)
#    offsets `input->data() + kEncodedLength - 8` without checking
#    `input->size() < kEncodedLength`, reading uninitialized stack bytes when
#    a truncated `.ldb` file (< 48 bytes) is opened. Fixed upstream in
#    commit 068d5ee1a3ac40dabd00d211d5013af44be55bea (#423).
# 2. LevelDB 1.23 `Hash()` (`util/hash.cc`) uses `while (data + 4 <= limit)`
#    pointer-addition overflow check instead of `while (limit - data >= 4)`.
#    Fixed upstream in commit 578eeb702ec056be97ef9f2ac2fa92efbb00d394 (#423).
# 3. LevelDB 1.23 `DecodeEntry()` (`table/block.cc`) validates `*non_shared`
#    and `*value_length` by summing them in `uint32_t` before comparing against
#    `limit - p`, allowing unsigned wraparound and out-of-bounds reads on
#    corrupt block entries. Fixed upstream in commit
#    29465588d3dfdc80fba5e198037582b8fdb5dd60 (#676).
#
# Idempotent: subsequent runs on an already-patched tree are no-ops.

set(_FORMAT_FILE "${SRC}/table/format.cc")
if(EXISTS "${_FORMAT_FILE}")
  file(READ "${_FORMAT_FILE}" _FORMAT_CONTENT)
  string(FIND "${_FORMAT_CONTENT}" "footer too short" _ALREADY_PATCHED)
  if(_ALREADY_PATCHED EQUAL -1)
    string(REPLACE
      "Status Footer::DecodeFrom(Slice* input) {\n  const char* magic_ptr = input->data() + kEncodedLength - 8;"
      "Status Footer::DecodeFrom(Slice* input) {\n  if (input->size() < kEncodedLength) {\n    return Status::Corruption(\"not an sstable (footer too short)\");\n  }\n  const char* magic_ptr = input->data() + kEncodedLength - 8;"
      _FORMAT_CONTENT "${_FORMAT_CONTENT}")
    file(WRITE "${_FORMAT_FILE}" "${_FORMAT_CONTENT}")
  endif()

  file(READ "${_FORMAT_FILE}" _VERIFY_FORMAT)
  string(FIND "${_VERIFY_FORMAT}" "footer too short" _VERIFY_FORMAT_POS)
  if(_VERIFY_FORMAT_POS EQUAL -1)
    message(FATAL_ERROR "leveldb-footer-decode-bounds.cmake failed to patch ${_FORMAT_FILE}")
  endif()
endif()

set(_HASH_FILE "${SRC}/util/hash.cc")
if(EXISTS "${_HASH_FILE}")
  file(READ "${_HASH_FILE}" _HASH_CONTENT)
  string(REPLACE
    "while (data + 4 <= limit)"
    "while (limit - data >= 4)"
    _HASH_CONTENT "${_HASH_CONTENT}")
  file(WRITE "${_HASH_FILE}" "${_HASH_CONTENT}")

  file(READ "${_HASH_FILE}" _VERIFY_HASH)
  string(FIND "${_VERIFY_HASH}" "while (limit - data >= 4)" _VERIFY_HASH_POS)
  if(_VERIFY_HASH_POS EQUAL -1)
    message(FATAL_ERROR "leveldb-footer-decode-bounds.cmake failed to patch ${_HASH_FILE}")
  endif()
endif()

set(_BLOCK_FILE "${SRC}/table/block.cc")
if(EXISTS "${_BLOCK_FILE}")
  file(READ "${_BLOCK_FILE}" _BLOCK_CONTENT)
  string(FIND "${_BLOCK_CONTENT}" "static_cast<uint32_t>(limit - p) - *non_shared < *value_length" _BLOCK_ALREADY_PATCHED)
  if(_BLOCK_ALREADY_PATCHED EQUAL -1)
    string(REPLACE
      "  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {"
      "  if (static_cast<uint32_t>(limit - p) < *non_shared ||\n      static_cast<uint32_t>(limit - p) - *non_shared < *value_length) {"
      _BLOCK_CONTENT "${_BLOCK_CONTENT}")
    file(WRITE "${_BLOCK_FILE}" "${_BLOCK_CONTENT}")
  endif()

  file(READ "${_BLOCK_FILE}" _VERIFY_BLOCK)
  string(FIND "${_VERIFY_BLOCK}" "static_cast<uint32_t>(limit - p) - *non_shared < *value_length" _VERIFY_BLOCK_POS)
  if(_VERIFY_BLOCK_POS EQUAL -1)
    message(FATAL_ERROR "leveldb-footer-decode-bounds.cmake failed to patch ${_BLOCK_FILE}")
  endif()
endif()
