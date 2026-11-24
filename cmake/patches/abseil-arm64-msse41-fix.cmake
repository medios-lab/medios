# MEDIOS PATCH for issue #173:
#
# Vendored abseil (protobuf v25.3) emits per-architecture HWAES flags using
# `-Xarch_x86_64 -msse4.1 -Xarch_arm64 -march=armv8-a+crypto`, intended for
# Apple universal (multi-arch) builds. CMake deduplicates adjacent identical
# `-Xarch_x86_64` tokens, which leaves `-msse4.1` naked and applies it to
# every arch. Apple Clang on macOS 15 (Sequoia) rejects `-msse4.1` for
# arm64 with a hard "unsupported option" error.
#
# Apple Clang on macOS 14 (Sonoma) has historically tolerated this via
# `-Wno-unused-command-line-argument` (which abseil appends), but Sequoia's
# clang variant errors regardless.
#
# Fix: disable the `if(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES [[Clang]])`
# multi-arch branch entirely. The file then falls through to the simple
# `elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm.*|aarch64")` branch which
# sets only `${ABSL_RANDOM_HWAES_ARM64_FLAGS}` — no cross-arch leakage.
#
# Tradeoff: explicit Apple universal builds with CMAKE_OSX_ARCHITECTURES set
# to a multi-arch value would lose abseil's HWAES per-arch dispatch. Medios
# does not target universal builds; this is acceptable.
#
# Idempotent: the replaced token doesn't appear post-patch, so subsequent
# invocations are no-ops.

set(_FILE
    "${SRC}/third_party/abseil-cpp/absl/copts/AbseilConfigureCopts.cmake")

if(EXISTS "${_FILE}")
  file(READ "${_FILE}" _CONTENT)
  string(REPLACE
    "if(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES [[Clang]])"
    "if(FALSE)  # MEDIOS PATCH (#173): bypass Apple multi-arch branch; fall through to arch-specific simple branch."
    _CONTENT "${_CONTENT}")
  file(WRITE "${_FILE}" "${_CONTENT}")
endif()
