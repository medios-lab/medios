# MEDIOS PATCH for issue #422:
#
# RE2 2023-03-01 (`3a8436ac436124a57a4e22d5c8713a2d42b381d7`, final standalone
# Abseil-free RE2 release compatible with protobuf v25.3's Abseil 20230802.1):
# Backports upstream commit e66463312e1d30d427bbde6c40e7fd627dcfb82e in
# `re2/simplify.cc` so `SimplifyWalker::SimplifyRepeat` caps counted
# repetitions of empty-width sub-expressions (or concatenations/alternations
# thereof) at `min = std::min(min, 1); max = std::min(max, 1);` instead of
# expanding up to 1000 copies at each nesting level (quadratic blowup on
# `($){1000}{1000}`).
#
# Idempotent: subsequent runs on an already-patched tree are no-ops.

set(_SIMPLIFY_FILE "${SRC}/re2/simplify.cc")
if(EXISTS "${_SIMPLIFY_FILE}")
  file(READ "${_SIMPLIFY_FILE}" _SIMPLIFY_CONTENT)
  string(FIND "${_SIMPLIFY_CONTENT}" "is_empty_op" _ALREADY_PATCHED)
  if(_ALREADY_PATCHED EQUAL -1)
    string(REPLACE
      "#include <string>"
      "#include <algorithm>\n#include <string>"
      _SIMPLIFY_CONTENT "${_SIMPLIFY_CONTENT}")
    string(REPLACE
      "Regexp* SimplifyWalker::SimplifyRepeat(Regexp* re, int min, int max,
                                       Regexp::ParseFlags f) {
  // x{n,} means at least n matches of x."
      "Regexp* SimplifyWalker::SimplifyRepeat(Regexp* re, int min, int max,
                                       Regexp::ParseFlags f) {
  // MEDIOS PATCH (#422, upstream e664633): For an empty-width op OR a
  // concatenation or alternation of empty-width ops, cap the repetition count
  // at 1.
  auto is_empty_op = [](Regexp* sub) {
    return (sub->op() == kRegexpBeginLine || sub->op() == kRegexpEndLine ||
            sub->op() == kRegexpWordBoundary || sub->op() == kRegexpNoWordBoundary ||
            sub->op() == kRegexpBeginText || sub->op() == kRegexpEndText);
  };
  if (is_empty_op(re) ||
      ((re->op() == kRegexpConcat || re->op() == kRegexpAlternate) &&
       std::all_of(re->sub(), re->sub() + re->nsub(), is_empty_op))) {
    min = std::min(min, 1);
    max = std::min(max, 1);
  }

  // x{n,} means at least n matches of x."
      _SIMPLIFY_CONTENT "${_SIMPLIFY_CONTENT}")
    file(WRITE "${_SIMPLIFY_FILE}" "${_SIMPLIFY_CONTENT}")
  endif()

  file(READ "${_SIMPLIFY_FILE}" _VERIFY_CONTENT)
  string(FIND "${_VERIFY_CONTENT}" "is_empty_op" _VERIFY_POS)
  if(_VERIFY_POS EQUAL -1)
    message(FATAL_ERROR "re2-simplify-empty-width.cmake failed to patch ${_SIMPLIFY_FILE}")
  endif()
endif()
