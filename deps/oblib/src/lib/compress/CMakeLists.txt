if(${ARCHITECTURE} STREQUAL "x86_64")
  set(QPL_COMPILE_FILE_C "qpl/ob_qpl_compressor.cpp")
  set(QPL_COMPILE_FILE_H "qpl/ob_qpl_compressor.h")
  set(QPL_LIB "qpl")
endif()

oblib_add_library(compress
  ob_compressor.cpp
  ob_compressor.h
  ob_compress_util.h
  ob_compressor_pool.cpp
  ob_compressor_pool.h
  ob_stream_compressor.cpp
  ob_stream_compressor.h
  none/ob_none_compressor.cpp
  none/ob_none_compressor.h
  snappy/ob_snappy_compressor.cpp
  snappy/ob_snappy_compressor.h
  zlib/ob_zlib_compressor.cpp
  zlib/ob_zlib_compressor.h
  zstd/ob_zstd_compressor.cpp
  zstd/ob_zstd_compressor.h
  zstd/ob_zstd_stream_compressor.cpp
  zstd/ob_zstd_stream_compressor.h
  zstd_1_3_8/ob_zstd_compressor_1_3_8.cpp
  zstd_1_3_8/ob_zstd_compressor_1_3_8.h
  zstd_1_3_8/ob_zstd_stream_compressor_1_3_8.cpp
  zstd_1_3_8/ob_zstd_stream_compressor_1_3_8.h
  ${QPL_COMPILE_FILE_C}
  ${QPL_COMPILE_FILE_H})

add_subdirectory(zstd)
add_subdirectory(zstd_1_3_8)
add_subdirectory(lz4)
add_subdirectory(snappy)
add_subdirectory(zlib)
if(${ARCHITECTURE} STREQUAL "x86_64")
  add_subdirectory(qpl)
endif()

target_link_libraries(compress PUBLIC lz4 snappy zlib zstd zstd_1_3_8 ${QPL_LIB})
