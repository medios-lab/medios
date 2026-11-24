# GPU isolator module: a shared library the node dlopens through --modules.
# It links against libmedios to resolve the module and isolator interfaces;
# libmedios does not link against it and no core target depends on it.
# Output goes next to the test modules so one --modules path covers both.
add_library(medios_gpu SHARED gpu_isolator.cpp discovery.cpp)
target_link_libraries(medios_gpu PRIVATE medios)
target_include_directories(medios_gpu
  PRIVATE
    ${CMAKE_BINARY_DIR}/include
    ${CMAKE_SOURCE_DIR}
)
set_target_properties(medios_gpu PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/src/.libs"
)
