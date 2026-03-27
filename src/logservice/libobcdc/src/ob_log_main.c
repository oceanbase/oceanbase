/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <stdio.h>
#include <stdlib.h>

#ifdef ARC_X86
const char my_interp[] __attribute__((section(".interp")))
    = "/lib64/ld-linux-x86-64.so.2";
#else
const char my_interp[] __attribute__((section(".interp")))
    = "/lib64/ld-linux-aarch64.so.1";
#endif

const char* build_version();
const char* build_date();
const char* build_time();
const char* build_flags();
const char* build_info();

int so_main()
{
  #ifndef ENABLE_SANITY
    const char *extra_flags = "";
  #else
    const char *extra_flags = "|Sanity";
  #endif
  fprintf(stdout, "\n");

  fprintf(stdout, "libobcdc (%s %s)\n",   PACKAGE_STRING, RELEASEID);
  fprintf(stdout, "\n");

  fprintf(stdout, "BUILD_VERSION: %s\n",    build_version());
  fprintf(stdout, "BUILD_TIME: %s %s\n",  build_date(), build_time());
  fprintf(stdout, "BUILD_FLAGS: %s%s\n",    build_flags(), extra_flags);
  fprintf(stdout, "BUILD_INFO: %s\n",    build_info());
  exit(0);
}

void __attribute__((constructor)) ob_log_init()
{
}
