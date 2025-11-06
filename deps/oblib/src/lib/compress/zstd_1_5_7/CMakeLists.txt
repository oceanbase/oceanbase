add_library(zstd_1_5_7_objs OBJECT
  zstd_src/debug.c
  zstd_src/entropy_common.c
  zstd_src/error_private.c
  zstd_src/fse_decompress.c
  zstd_src/pool.c
  zstd_src/threading.c
  zstd_src/xxhash.c
  zstd_src/zstd_common.c
  zstd_src/fse_compress.c
  zstd_src/hist.c
  zstd_src/huf_compress.c
  zstd_src/zstd_compress.c
  zstd_src/zstd_compress_literals.c
  zstd_src/zstd_compress_sequences.c
  zstd_src/zstd_compress_superblock.c
  zstd_src/zstd_double_fast.c
  zstd_src/zstd_fast.c
  zstd_src/zstd_lazy.c
  zstd_src/zstd_ldm.c
  zstd_src/zstd_opt.c
  zstd_src/zstd_preSplit.c
  zstd_src/zstdmt_compress.c
  zstd_src/huf_decompress.c
  zstd_src/zstd_ddict.c
  zstd_src/zstd_decompress.c
  zstd_src/zstd_decompress_block.c
  zstd_src/cover.c
  zstd_src/divsufsort.c
  zstd_src/fastcover.c
  zstd_src/zdict.c
  zstd_src/zbuff_common.c
  zstd_src/zbuff_compress.c
  zstd_src/zbuff_decompress.c
  zstd_src/zstd_v01.c
  zstd_src/zstd_v02.c
  zstd_src/zstd_v03.c
  zstd_src/zstd_v04.c
  zstd_src/zstd_v05.c
  zstd_src/zstd_v06.c
  zstd_src/zstd_v07.c
  ob_zstd_wrapper_1_5_7.cpp)


target_compile_definitions(zstd_1_5_7_objs PRIVATE -DZSTDERRORLIB_VISIBILITY= -DZDICTLIB_VISIBILITY= -DZSTDLIB_VISIBILITY=)
target_compile_options(zstd_1_5_7_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_1_5_7_objs PUBLIC oblib_base)

add_custom_command(
  OUTPUT zstd_1_5_7_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zstd_1_5_7_objs> -o zstd_1_5_7_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zstd_1_5_7_objs.tmp.o zstd_1_5_7_objs.o
  DEPENDS zstd_1_5_7_objs
  BYPRODUCTS zstd_1_5_7_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_1_5_7_objs.o)
oblib_add_library(zstd_1_5_7 zstd_1_5_7_objs.o)
