add_library(zstd_1_3_8_objs OBJECT
  debug.c
  entropy_common.c
  error_private.c
  fse_compress.c
  fse_decompress.c
  hist.c
  huf_compress.c
  huf_decompress.c
  pool.c
  threading.c
  xxhash.c
  zstd_common.c
  zstd_compress.c
  zstd_compress_internal.h
  zstd_ddict.c
  zstd_decompress_block.c
  zstd_decompress.c
  zstd_double_fast.c
  zstd_fast.c
  zstd_lazy.c
  zstd_ldm.c
  zstdmt_compress.c
  zstd_opt.c
  ob_zstd_wrapper.cpp)

target_compile_definitions(zstd_1_3_8_objs PRIVATE -DZSTDERRORLIB_VISIBILITY= -DZDICTLIB_VISIBILITY= -DZSTDLIB_VISIBILITY=)
target_compile_options(zstd_1_3_8_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_1_3_8_objs PUBLIC oblib_base_base)
disable_pch(zstd_1_3_8_objs)

add_custom_command(
  OUTPUT zstd_1_3_8_objs.o
  COMMAND "${LD_BIN}" -r $<TARGET_OBJECTS:zstd_1_3_8_objs> -o zstd_1_3_8_objs.tmp.o
  COMMAND "${OBJCOPY_BIN}" --localize-hidden zstd_1_3_8_objs.tmp.o zstd_1_3_8_objs.o
  DEPENDS zstd_1_3_8_objs
  BYPRODUCTS zstd_1_3_8_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_1_3_8_objs.o)
ob_lib_add_target(oblib_compress_zstd_1_3_8 zstd_1_3_8_objs.o)
