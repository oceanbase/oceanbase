
#  unittests
file (GLOB_RECURSE UNIT_TESTS "../src/*_test.cpp")
add_executable (unittests ${UNIT_TESTS}
    fixtures/fixtures.cpp
)
if (DIST_CONTAINS_SSE)
  target_compile_definitions (unittests PRIVATE ENABLE_SSE=1)
endif ()
if (DIST_CONTAINS_AVX)
  target_compile_definitions (unittests PRIVATE ENABLE_AVX=1)
endif ()
if (DIST_CONTAINS_AVX2)
  target_compile_definitions (unittests PRIVATE ENABLE_AVX2=1)
endif ()
if (DIST_CONTAINS_AVX512)
  target_compile_definitions (unittests PRIVATE ENABLE_AVX512=1)
endif ()
target_include_directories (unittests PRIVATE "./fixtures")
target_link_libraries (unittests PRIVATE Catch2::Catch2WithMain vsag simd)
add_dependencies (unittests spdlog Catch2)

# function tests
file (GLOB FUNCTION_TESTS "test_*.cpp")
add_executable (functests
    ${FUNCTION_TESTS}
    fixtures/fixtures.cpp
)
target_include_directories (functests PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/spdlog/install/include
    ${HDF5_INCLUDE_DIRS})
target_link_libraries (functests PRIVATE Catch2::Catch2WithMain vsag simd)
add_dependencies (functests spdlog Catch2)

# test performance tool, cannot compile with _GLIBCXX_USE_CXX11_ABI=0
if (ENABLE_TOOLS AND ENABLE_TESTS AND ENABLE_CXX11_ABI)
  add_executable (test_performance performance/test_performance.cpp)
  target_link_libraries (test_performance PRIVATE vsag simd libhdf5_cpp.a libhdf5.a z)
  add_dependencies (test_performance hdf5)
endif ()
