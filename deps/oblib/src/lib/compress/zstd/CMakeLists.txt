# Define object library zstd
add_library(zstd_objs OBJECT
  entropy_common.c
  error_private.c
  fse_compress.c
  fse_decompress.c
  huf_compress.c
  huf_decompress.c
  xxhash.c
  zbuff_compress.c
  zbuff_decompress.c
  zstd_common.c
  zstd_compress.c
  zstd_decompress.c
  zstd_v01.c
  zstd_v02.c
  zstd_v03.c
  zstd_v04.c
  zstd_v05.c
  zstd_v06.c
  zstd_v07.c
  ob_zstd_wrapper.cpp)

target_compile_options(zstd_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_objs PUBLIC oblib_base_base)
disable_pch(zstd_objs)

add_custom_command(
  OUTPUT zstd_objs.o
  COMMAND ${LD_BIN} -r $<TARGET_OBJECTS:zstd_objs> -o zstd_objs.tmp.o
  COMMAND ${OBJCOPY_BIN} --localize-hidden zstd_objs.tmp.o zstd_objs.o
  DEPENDS zstd_objs
  BYPRODUCTS zstd_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_objs.o)
ob_lib_add_target(oblib_compress_zstd zstd_objs.o)
