# ============================================================
# Zstd 1.1.1 版本支持（使用预编译的 .a 库）
# ============================================================

set(ZSTD_LIB "${DEP_DIR}/lib/zstd/libzstd.a")

add_library(zstd_objs OBJECT
  ob_zstd_wrapper.cpp
)

target_compile_options(zstd_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zstd_objs PUBLIC oblib_base)

add_custom_command(
  OUTPUT zstd_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:zstd_objs> ${ZSTD_LIB} -o zstd_objs.tmp.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden zstd_objs.tmp.o zstd_objs.o
  DEPENDS zstd_objs ${ZSTD_LIB}
  BYPRODUCTS zstd_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zstd_objs.o)
oblib_add_library(zstd zstd_objs.o)
