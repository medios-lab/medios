# MEDIOS PATCH for issue #497:
#
# libev 4.33 `ev_signal_start` (`ev.c`) has an inverted `EVFLAG_NOSIGMASK`
# check (`if (origflags & EVFLAG_NOSIGMASK)` instead of
# `if (!(origflags & EVFLAG_NOSIGMASK))`). Fixed upstream in libev CVS
# rev 1.539.
#
# Idempotent: the replaced token does not appear post-patch.

set(_EV_FILE "${SRC}/ev.c")
if(EXISTS "${_EV_FILE}")
  file(READ "${_EV_FILE}" _EV_CONTENT)
  string(REPLACE
    "        if (origflags & EVFLAG_NOSIGMASK)\n          {\n            sigemptyset (&sa.sa_mask);"
    "        if (!(origflags & EVFLAG_NOSIGMASK))\n          {\n            sigemptyset (&sa.sa_mask);"
    _EV_CONTENT "${_EV_CONTENT}")
  file(WRITE "${_EV_FILE}" "${_EV_CONTENT}")

  file(READ "${_EV_FILE}" _VERIFY_EV)
  string(FIND "${_VERIFY_EV}" "if (!(origflags & EVFLAG_NOSIGMASK))" _VERIFY_EV_POS)
  if(_VERIFY_EV_POS EQUAL -1)
    message(FATAL_ERROR "libev-nosigmask-polarity.cmake failed to patch ${_EV_FILE}")
  endif()
endif()
