add_library(zstd_1_3_8_objs OBJECT
  zstd_src/bitstream.h
  zstd_src/compiler.h
  zstd_src/cpu.h
  zstd_src/debug.c
  zstd_src/debug.h
  zstd_src/entropy_common.c
  zstd_src/error_private.c
  zstd_src/error_private.h
  zstd_src/fse_compress.c
  zstd_src/fse_decompress.c
  zstd_src/fse.h
  zstd_src/hist.c
  zstd_src/hist.h
  zstd_src/huf_compress.c
  zstd_src/huf_decompress.c
  zstd_src/huf.h
  zstd_src/mem.h
  ob_zstd_compressor_1_3_8.h
  ob_zstd_stream_compressor_1_3_8.h
  ob_zstd_wrapper.h
  zstd_src/pool.c
  zstd_src/pool.h
  zstd_src/threading.c
  zstd_src/threading.h
  zstd_src/xxhash.c
  zstd_src/xxhash.h
  zstd_src/zstd_common.c
  zstd_src/zstd_compress.c
  zstd_src/zstd_compress_internal.h
  zstd_src/zstd_ddict.c
  zstd_src/zstd_ddict.h
  zstd_src/zstd_decompress_block.c
  zstd_src/zstd_decompress_block.h
  zstd_src/zstd_decompress.c
  zstd_src/zstd_decompress_internal.h
  zstd_src/zstd_double_fast.c
  zstd_src/zstd_double_fast.h
  zstd_src/zstd_errors.h
  zstd_src/zstd_fast.c
  zstd_src/zstd_fast.h
  zstd_src/zstd.h
  zstd_src/zstd_internal.h
  zstd_src/zstd_lazy.c
  zstd_src/zstd_lazy.h
  zstd_src/zstd_ldm.c
  zstd_src/zstd_ldm.h
  zstd_src/zstdmt_compress.c
  zstd_src/zstdmt_compress.h
  zstd_src/zstd_opt.c
  zstd_src/zstd_opt.h
  ob_zstd_wrapper.cpp
  ob_zstd_wrapper.h)


target_compile_definitions(zstd_1_3_8_objs PRIVATE -DZSTDERRORLIB_VISIBILITY= -DZDICTLIB_VISIBILITY= -DZSTDLIB_VISIBILITY=)
target_compile_options(zstd_1_3_8_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_1_3_8_objs PUBLIC oblib_base_base)

add_custom_command(
  OUTPUT zstd_1_3_8_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zstd_1_3_8_objs> -o zstd_1_3_8_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zstd_1_3_8_objs.tmp.o zstd_1_3_8_objs.o
  DEPENDS zstd_1_3_8_objs
  BYPRODUCTS zstd_1_3_8_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_1_3_8_objs.o)
oblib_add_library(zstd_1_3_8 zstd_1_3_8_objs.o)
