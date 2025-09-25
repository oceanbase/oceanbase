add_library(zlib_lite_src OBJECT
  zlib_lite_src/adler32.c
  zlib_lite_src/compress.c
  zlib_lite_src/crc32.c
  zlib_lite_src/deflate.c
  zlib_lite_src/deflate.h
  zlib_lite_src/gzclose.c
  zlib_lite_src/gzlib.c
  zlib_lite_src/gzread.c
  zlib_lite_src/gzwrite.c
  zlib_lite_src/infback.c
  zlib_lite_src/inffast.c
  zlib_lite_src/inflate.c
  zlib_lite_src/inftrees.c
  zlib_lite_src/trees.c
  zlib_lite_src/uncompr.c
  zlib_lite_src/zutil.c
)
target_include_directories(zlib_lite_src PUBLIC "${DEP_DIR}/include/")
set(ZLIB_LITE_COMPILE_DEFINITIONS -fvisibility=hidden)
if (CPP_STANDARD_20)
  set(ZLIB_LITE_COMPILE_DEFINITIONS ${ZLIB_LITE_COMPILE_DEFINITIONS} -Wno-deprecated-non-prototype)
endif()
target_compile_options(zlib_lite_src PRIVATE ${ZLIB_LITE_COMPILE_DEFINITIONS})

oblib_add_library(zlib_lite zlib_lite_src)
