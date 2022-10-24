oblib_add_library(snappy
  snappy_src/config.h
  snappy_src/snappy-c.cpp
  snappy_src/snappy-c.h
  snappy_src/snappy.cpp
  snappy_src/snappy.h
  snappy_src/snappy-internal.h
  snappy_src/snappy-sinksource.cpp
  snappy_src/snappy-sinksource.h
  snappy_src/snappy-stubs-internal.cpp
  snappy_src/snappy-stubs-internal.h
  snappy_src/snappy-stubs-public.h)

target_link_libraries(snappy PUBLIC oblib_base_base)

