# ============================================================
# Zstd 1.3.8 版本支持（使用预编译的 .a 库）
# ============================================================

set(ZSTD_138_LIB "${DEP_DIR}/lib/zstd_138/libzstd_138.a")

add_library(zstd_1_3_8_objs OBJECT
  ob_zstd_wrapper.cpp
)

target_compile_definitions(zstd_1_3_8_objs PRIVATE -DZSTDERRORLIB_VISIBILITY= -DZDICTLIB_VISIBILITY= -DZSTDLIB_VISIBILITY=)
target_compile_options(zstd_1_3_8_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_1_3_8_objs PUBLIC oblib_base)

add_custom_command(
  OUTPUT zstd_1_3_8_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zstd_1_3_8_objs> ${ZSTD_138_LIB} -o zstd_1_3_8_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zstd_1_3_8_objs.tmp.o zstd_1_3_8_objs.o
  DEPENDS zstd_1_3_8_objs ${ZSTD_138_LIB}
  BYPRODUCTS zstd_1_3_8_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_1_3_8_objs.o)
oblib_add_library(zstd_1_3_8 zstd_1_3_8_objs.o)
