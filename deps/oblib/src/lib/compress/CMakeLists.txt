add_subdirectory(zstd)
add_subdirectory(zstd_1_3_8)
add_subdirectory(lz4)
add_subdirectory(snappy)
add_subdirectory(zlib)

ob_lib_add_target(oblib_compress
  ob_compressor.cpp
  ob_compressor_pool.cpp
  ob_stream_compressor.cpp
  none/ob_none_compressor.cpp
  snappy/ob_snappy_compressor.cpp
  zlib/ob_zlib_compressor.cpp
  zstd/ob_zstd_compressor.cpp
  zstd/ob_zstd_stream_compressor.cpp
  zstd_1_3_8/ob_zstd_compressor_1_3_8.cpp
  zstd_1_3_8/ob_zstd_stream_compressor_1_3_8.cpp
)