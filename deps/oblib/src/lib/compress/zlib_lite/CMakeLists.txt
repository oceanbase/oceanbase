oblib_add_library(zlib_lite
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

if(${ARCHITECTURE} STREQUAL "x86_64")
  target_include_directories(zlib_lite PUBLIC "${DEP_DIR}/include/")
  target_link_libraries(zlib_lite PUBLIC "${DEP_DIR}/lib64/libqpl.a")
  target_link_libraries(zlib_lite PUBLIC "${DEP_DIR}/lib64/libusdm_drv.a")
  target_compile_definitions(zlib_lite PUBLIC -DENABLE_QPL_COMPRESSION)
endif()

target_link_libraries(zlib_lite)
