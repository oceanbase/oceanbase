# ============================================================
# Zstd 1.5.7 版本支持（使用预编译的 .a 库）
# ============================================================

set(ZSTD_157_LIB "${DEP_DIR}/lib/zstd_157/libzstd_157.a")

add_library(zstd_1_5_7_objs OBJECT
  ob_zstd_wrapper_1_5_7.cpp
)


target_compile_definitions(zstd_1_5_7_objs PRIVATE -DZSTDERRORLIB_VISIBILITY= -DZDICTLIB_VISIBILITY= -DZSTDLIB_VISIBILITY=)
target_compile_options(zstd_1_5_7_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_1_5_7_objs PUBLIC oblib_base)

add_custom_command(
  OUTPUT zstd_1_5_7_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zstd_1_5_7_objs> ${ZSTD_157_LIB} -o zstd_1_5_7_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zstd_1_5_7_objs.tmp.o zstd_1_5_7_objs.o
  DEPENDS zstd_1_5_7_objs ${ZSTD_157_LIB}
  BYPRODUCTS zstd_1_5_7_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_1_5_7_objs.o)
oblib_add_library(zstd_1_5_7 zstd_1_5_7_objs.o)
