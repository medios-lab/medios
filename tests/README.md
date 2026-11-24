# tests

Canonical CMake/CTest GoogleTest suite for Medios, built when `-DMEDIOS_BUILD_TESTS=ON` (default `ON` in top-level `CMakeLists.txt`).

## Overview

- **Active test surface:** Every target in [`tests/CMakeLists.txt`](CMakeLists.txt) registered via `gtest_discover_tests` (or `add_test`) is part of the `ctest --test-dir build --output-on-failure` suite.
- **Relationship to legacy `src/tests/`:** The upstream Mesos `src/tests/` directory was stripped per `DR-013` (#14), and the kept spine tests were ported here (#111, #403). The legacy `stout/tests/` and `libprocess/src/tests/` trees remain in-tree as unbuilt upstream reference archives (`CMakeLists.txt` forces `MEDIOS_BUILD_TESTS=OFF` across `add_subdirectory(stout)` and `add_subdirectory(libprocess)`).
- **Test modules (`tests/modules/`):** `_TEST_MODULES` in `tests/CMakeLists.txt` builds shared libraries into `${CMAKE_BINARY_DIR}/src/.libs/` (`testisolator`, `testcontainer_logger`, `testhook`, `testanonymous`, `testallocator`, `testresource_estimator`, `testauthorizer`, `testhttpauthenticator`, `testcontrollercontender`, `testcontrollerdetector`), which `tests::initModules()` (`tests/module.cpp`) loads at test binary startup. Upstream `src/tests/anonymous_tests.cpp` was not ported after `STRIP-005` (PR #36); anonymous module loading is exercised at startup by `tests::initModules()` via `testanonymous` (`tests/modules/test_anonymous_module.cpp`).
