add_library(zlib_lite_objs OBJECT
  zlib_lite_src/adler32.c
  zlib_lite_src/compress.c
  zlib_lite_src/crc32.c
  zlib_lite_src/crc32.h
  zlib_lite_src/deflate.c
  zlib_lite_src/deflate.h
  zlib_lite_src/gzclose.c
  zlib_lite_src/gzguts.h
  zlib_lite_src/gzlib.c
  zlib_lite_src/gzread.c
  zlib_lite_src/gzwrite.c
  zlib_lite_src/infback.c
  zlib_lite_src/inffast.c
  zlib_lite_src/inffast.h
  zlib_lite_src/inffixed.h
  zlib_lite_src/inflate.c
  zlib_lite_src/inflate.h
  zlib_lite_src/inftrees.c
  zlib_lite_src/inftrees.h
  zlib_lite_src/trees.c
  zlib_lite_src/trees.h
  zlib_lite_src/uncompr.c
  zlib_lite_src/zconf.h
  zlib_lite_src/zlib.h
  zlib_lite_src/zutil.c
  zlib_lite_src/zutil.h
  zlib_lite_src/codec_deflate_qpl.cpp
  zlib_lite_src/codec_deflate_qpl.h)

target_compile_options(zlib_lite_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zlib_lite_objs PUBLIC oblib_base_base)
add_custom_command(
  OUTPUT zlib_lite_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zlib_lite_objs> -o zlib_lite_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zlib_lite_objs.tmp.o zlib_lite_objs.o
  DEPENDS zlib_lite_objs
  BYPRODUCTS zlib_lite_objs.o
  COMMAND_EXPAND_LISTS
  )

if(${ARCHITECTURE} STREQUAL "x86_64")
  target_include_directories(zlib_lite_objs PUBLIC "${DEP_DIR}/include/")
  target_link_libraries(zlib_lite_objs PUBLIC "${DEP_DIR}/lib64/libqpl.a")
  target_link_libraries(zlib_lite_objs PUBLIC "${DEP_DIR}/lib64/libusdm_drv.a")
  target_compile_definitions(zlib_lite_objs PUBLIC -DENABLE_QPL_COMPRESSION)
endif()

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zlib_lite_objs.o)
oblib_add_library(zlib_lite zlib_lite_objs.o)
