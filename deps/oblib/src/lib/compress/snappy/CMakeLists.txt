ob_lib_add_target(oblib_compress_snappy
  snappy-c.cpp
  snappy.cpp
  snappy-sinksource.cpp
  snappy-stubs-internal.cpp)
disable_pch(oblib_compress_snappy)