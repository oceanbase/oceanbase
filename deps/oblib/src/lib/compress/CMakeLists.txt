oblib_add_library(compress
  ob_compressor.cpp
  ob_compressor_pool.cpp
  ob_stream_compressor.cpp
  none/ob_none_compressor.cpp
  snappy_1_2_2/ob_snappy_compressor_1_2_2.cpp
  zlib/ob_zlib_compressor.cpp
  zstd/ob_zstd_compressor.cpp
  zstd/ob_zstd_stream_compressor.cpp
  zstd_1_3_8/ob_zstd_compressor_1_3_8.cpp
  zstd_1_3_8/ob_zstd_stream_compressor_1_3_8.cpp)

add_subdirectory(zstd)
add_subdirectory(zstd_1_3_8)
add_subdirectory(lz4)
add_subdirectory(snappy)

target_link_libraries(compress PUBLIC lz4 snappy zstd zstd_1_3_8)
