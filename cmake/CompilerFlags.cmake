add_library(medios_compile_flags INTERFACE)
target_compile_features(medios_compile_flags INTERFACE cxx_std_20)

target_compile_options(medios_compile_flags INTERFACE
  -Wall
  -Wextra
  # Suppress noisy upstream warnings; revisit post-DR-019 port.
  -Wno-unused-parameter
  -Wno-deprecated-declarations
)

if(MEDIOS_ENABLE_SANITIZERS)
  target_compile_options(medios_compile_flags INTERFACE
    -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer
  )
  target_link_options(medios_compile_flags INTERFACE
    -fsanitize=address -fsanitize=undefined
  )
endif()
