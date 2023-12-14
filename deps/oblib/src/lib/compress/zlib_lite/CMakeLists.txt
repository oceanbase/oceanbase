add_library(zlib_lite_objs OBJECT
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
  zlib_lite_src/inftrees.c
  zlib_lite_src/trees.c
  zlib_lite_src/uncompr.c
  zlib_lite_src/zutil.c
  zlib_lite_adaptor.cpp)

# We can not put the file codec_deflate_qpl.cpp into zlib_lite_objs.
# I got some link error messages like below:
# ld.lld: error: relocation refers to a symbol in a discarded section: __clang_call_terminate
add_library(zlib_lite_qpl
  codec_deflate_qpl.cpp)

target_compile_options(zlib_lite_objs PRIVATE -fvisibility=hidden)

if(${ARCHITECTURE} STREQUAL "x86_64")
  target_include_directories(zlib_lite_objs PUBLIC "${DEP_DIR}/include/")
  target_compile_options(zlib_lite_objs PUBLIC -DENABLE_QPL_COMPRESSION)

  target_include_directories(zlib_lite_qpl PUBLIC "${DEP_DIR}/include/")
  target_compile_options(zlib_lite_qpl PUBLIC -DENABLE_QPL_COMPRESSION)
  target_link_libraries(zlib_lite_qpl PUBLIC "${DEP_DIR}/lib64/libqpl.a")
  target_link_libraries(zlib_lite_qpl PUBLIC "${DEP_DIR}/lib64/libusdm_drv.a")

  message(STATUS "zlib_lite use qpl")
else()
  message(STATUS "zlib_lite use zlib compressor")
endif()

add_custom_command(
  OUTPUT zlib_lite_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zlib_lite_objs> -o zlib_lite_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zlib_lite_objs.tmp.o zlib_lite_objs.o
  DEPENDS zlib_lite_objs
  BYPRODUCTS zlib_lite_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zlib_lite_objs.o)

oblib_add_library(zlib_lite zlib_lite_objs.o)
target_link_libraries(zlib_lite zlib_lite_qpl)
