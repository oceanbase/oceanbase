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

add_library(zlib_lite_adaptor OBJECT
  zlib_lite_adaptor.cpp
)

# We can not put the file codec_deflate_qpl.cpp into zlib_lite_objs.
# I got some link error messages like below:
# ld.lld: error: relocation refers to a symbol in a discarded section: __clang_call_terminate
add_library(zlib_lite_qpl
  codec_deflate_qpl.cpp
)

set(ZLIB_LITE_COMPILE_DEFINITIONS -fvisibility=hidden)
if (CPP_STANDARD_20)
  set(ZLIB_LITE_COMPILE_DEFINITIONS ${ZLIB_LITE_COMPILE_DEFINITIONS} -Wno-deprecated-non-prototype)
endif()

target_compile_options(zlib_lite_src PRIVATE ${ZLIB_LITE_COMPILE_DEFINITIONS})
target_compile_options(zlib_lite_adaptor PRIVATE ${ZLIB_LITE_COMPILE_DEFINITIONS})

if(${ARCHITECTURE} STREQUAL "x86_64")
  target_include_directories(zlib_lite_src PUBLIC "${DEP_DIR}/include/")
  target_include_directories(zlib_lite_adaptor PUBLIC "${DEP_DIR}/include/")
  target_compile_options(zlib_lite_adaptor PUBLIC -DENABLE_QPL_COMPRESSION)

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
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zlib_lite_src> $<TARGET_OBJECTS:zlib_lite_adaptor> -o zlib_lite_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zlib_lite_objs.tmp.o zlib_lite_objs.o
  DEPENDS zlib_lite_src zlib_lite_adaptor
  BYPRODUCTS zlib_lite_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zlib_lite_objs.o)

oblib_add_library(zlib_lite zlib_lite_objs.o)
target_link_libraries(zlib_lite zlib_lite_qpl)
