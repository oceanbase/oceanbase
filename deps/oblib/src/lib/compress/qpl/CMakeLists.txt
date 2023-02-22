include_directories("${DEP_DIR}/include/")
link_libraries("${DEP_DIR}/lib/libqpl.a")
link_libraries("${DEP_DIR}/lib/libaccel-config.a")
link_libraries("${DEP_DIR}/lib/libuuid.a")

oblib_add_library(qpl
  qpl_src/qpl_low_level_api.c
  qpl_src/qpl_low_level_api.h)

target_link_libraries(qpl)
