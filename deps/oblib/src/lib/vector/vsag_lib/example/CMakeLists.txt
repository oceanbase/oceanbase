add_executable(hnsw_example hnsw_example.cpp default_allocator.cpp)
target_compile_options(hnsw_example PRIVATE -std=c++17)
target_link_libraries(hnsw_example PRIVATE ob_vsag_static_test vsag_test dl roaring fmt)
target_include_directories(hnsw_example BEFORE PRIVATE ${VSAG_LIB_DIR}/_deps/roaringbitmap-src/include/)
