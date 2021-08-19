# # Define object library zip
ob_lib_add_target(oblib_compress_zip
  zip.c
)

target_compile_options(oblib_compress_zip PRIVATE -Wno-shift-negative-value -Wno-implicit-function-declaration)
disable_pch(oblib_compress_zip)
target_link_libraries(oblib_compress_zip)