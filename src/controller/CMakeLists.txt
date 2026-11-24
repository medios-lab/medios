add_executable(medios-controller main.cpp)
target_link_libraries(medios-controller PRIVATE medios medios_compile_flags)

# DR-033 (#182): install is handled by the install(CODE) block in the
# root CMakeLists. No per-target install(TARGETS) — we copy and patchelf
# manually to bypass CMake's install-time rpath validation.
