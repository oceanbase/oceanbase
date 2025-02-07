/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * entrance of libobcdc.so
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
