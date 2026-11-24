# MEDIOS PATCH for issue #420:
#
# ELFIO Release_3.9 (`elfio/elfio_dynamic.hpp`):
# Guard `generic_get_entry_dyn` against malformed ELF `SHT_DYNAMIC` sections
# whose declared `sh_entsize` (`get_entry_size()`) is smaller than `sizeof(T)`
# or whose `get_data()` buffer is null / truncated relative to
# `index` and `get_entry_size()` (using overflow-safe division; upstream
# commits ee891ca / 7a1de75).
#
# Idempotent: subsequent runs on an already-patched tree are no-ops.

set(_DYN_FILE "${SRC}/elfio/elfio_dynamic.hpp")
if(EXISTS "${_DYN_FILE}")
  file(READ "${_DYN_FILE}" _DYN_CONTENT)
  string(FIND "${_DYN_CONTENT}" "dynamic_section->get_entry_size() < sizeof( T )" _ALREADY_PATCHED)
  if(_ALREADY_PATCHED EQUAL -1)
    string(REPLACE
      "        if ( dynamic_section->get_data() == 0 ||
             ( index + 1 ) * dynamic_section->get_entry_size() >
                 dynamic_section->get_size() ) {"
      "        if ( dynamic_section->get_data() == 0 ||
             dynamic_section->get_entry_size() < sizeof( T ) ||
             dynamic_section->get_size() < sizeof( T ) ||
             index > ( dynamic_section->get_size() - sizeof( T ) ) /
                         dynamic_section->get_entry_size() ) {"
      _DYN_CONTENT "${_DYN_CONTENT}")
    file(WRITE "${_DYN_FILE}" "${_DYN_CONTENT}")
  endif()

  file(READ "${_DYN_FILE}" _VERIFY_CONTENT)
  string(FIND "${_VERIFY_CONTENT}" "dynamic_section->get_entry_size() < sizeof( T )" _VERIFY_POS)
  if(_VERIFY_POS EQUAL -1)
    message(FATAL_ERROR "elfio-section-bounds.cmake failed to patch ${_DYN_FILE}")
  endif()
endif()
