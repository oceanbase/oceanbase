ob_lib_add_target(oblib_compress_zlib
  adler32.c
  compress.c
  crc32.c
  deflate.c
  gzclose.c
  gzlib.c
  gzread.c
  gzwrite.c
  infback.c
  inffast.c
  inflate.c
  inftrees.c
  trees.c
  uncompr.c
  zutil.c
)

target_compile_options(oblib_compress_zlib PRIVATE -Wno-shift-negative-value -Wno-implicit-function-declaration)
disable_pch(oblib_compress_zlib)
target_link_libraries(oblib_compress_zlib)

