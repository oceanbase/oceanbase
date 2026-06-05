# ============================================================
# SNAPPY 1.0.3 版本支持（使用预编译的 .a 库）
# ============================================================

set(SNAPPY_LIB "${DEP_DIR}/lib/snappy/libsnappy.a")

oblib_add_library(snappy
  ob_snappy_compressor.cpp
)
target_link_libraries(snappy PUBLIC oblib_base ${SNAPPY_LIB})

