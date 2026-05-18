# ============================================================
# LZ4 多版本支持（使用预编译的 .a 库）
# ============================================================

set(lz4_171_LIB "${DEP_DIR}/lib/lz4/lz4_171.a")
set(LZ4_191_LIB "${DEP_DIR}/lib/lz4/lz4_191.a")

add_library(lz4_171_wrapper OBJECT
  lz4_171_wrapper.cpp
)
target_link_libraries(lz4_171_wrapper PUBLIC oblib_base)
target_compile_options(lz4_171_wrapper PRIVATE -fvisibility=hidden)

add_library(lz4_191_wrapper OBJECT
  lz4_191_wrapper.cpp
)
target_link_libraries(lz4_191_wrapper PUBLIC oblib_base)
target_compile_options(lz4_191_wrapper PRIVATE -fvisibility=hidden)

# merge lz4_171_wrapper and liblz4_171
add_custom_command(
  OUTPUT lz4_171_merged.a.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:lz4_171_wrapper> ${lz4_171_LIB} -o lz4_171_merged.a.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden lz4_171_merged.a.o
  DEPENDS lz4_171_wrapper ${lz4_171_LIB}
  DEPENDS $<TARGET_OBJECTS:lz4_171_wrapper>
  COMMAND_EXPAND_LISTS
)

# merge lz4_191_wrapper and liblz4_191
add_custom_command(
  OUTPUT lz4_191_merged.a.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:lz4_191_wrapper> ${LZ4_191_LIB} -o lz4_191_merged.a.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden lz4_191_merged.a.o
  DEPENDS lz4_191_wrapper ${LZ4_191_LIB}
  DEPENDS $<TARGET_OBJECTS:lz4_191_wrapper>
  COMMAND_EXPAND_LISTS
)

add_library(lz4_objs OBJECT
  ob_lz4_compressor.cpp
  ob_lz4_compressor_191.cpp
  ob_lz4_stream_compressor.cpp
  ob_lz4_stream_compressor_191.cpp)
target_link_libraries(lz4_objs PUBLIC oblib_base)

add_custom_command(
  OUTPUT lz4-all.a.o
  COMMAND ${OB_LD_BIN} -r lz4_171_merged.a.o lz4_191_merged.a.o $<TARGET_OBJECTS:lz4_objs> -o lz4-all.a.o
  DEPENDS lz4_171_merged.a.o lz4_191_merged.a.o lz4_objs
  DEPENDS $<TARGET_OBJECTS:lz4_objs>
  COMMAND_EXPAND_LISTS
)

oblib_add_library(lz4 lz4-all.a.o)
oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/lz4-all.a.o)
