# Define object library zstd
add_library(zstd_objs OBJECT
  zstd_src/bitstream.h
  zstd_src/entropy_common.c
  zstd_src/error_private.c
  zstd_src/error_private.h
  zstd_src/fse_compress.c
  zstd_src/fse_decompress.c
  zstd_src/fse.h
  zstd_src/huf_compress.c
  zstd_src/huf_decompress.c
  zstd_src/huf.h
  zstd_src/mem.h
  zstd_src/xxhash.c
  zstd_src/xxhash.h
  zstd_src/zbuff_compress.c
  zstd_src/zbuff_decompress.c
  zstd_src/zbuff.h
  zstd_src/zstd_common.c
  zstd_src/zstd_compress.c
  zstd_src/zstd_decompress.c
  zstd_src/zstd_errors.h
  zstd_src/zstd.h
  zstd_src/zstd_internal.h
  zstd_src/zstd_legacy.h
  zstd_src/zstd_opt.h
  zstd_src/zstd_v01.c
  zstd_src/zstd_v01.h
  zstd_src/zstd_v02.c
  zstd_src/zstd_v02.h
  zstd_src/zstd_v03.c
  zstd_src/zstd_v03.h
  zstd_src/zstd_v04.c
  zstd_src/zstd_v04.h
  zstd_src/zstd_v05.c
  zstd_src/zstd_v05.h
  zstd_src/zstd_v06.c
  zstd_src/zstd_v06.h
  zstd_src/zstd_v07.c
  zstd_src/zstd_v07.h
  ob_zstd_wrapper.cpp
  ob_zstd_wrapper.h)

target_compile_options(zstd_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_objs PUBLIC oblib_base_base)

add_custom_command(
  OUTPUT zstd_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zstd_objs> -o zstd_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zstd_objs.tmp.o zstd_objs.o
  DEPENDS zstd_objs
  BYPRODUCTS zstd_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_objs.o)
oblib_add_library(zstd zstd_objs.o)
