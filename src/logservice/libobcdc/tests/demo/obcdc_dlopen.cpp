/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <iostream>
#include <dlfcn.h>

using namespace std;

#define LOG(msg) \
    do { \
      std::cout << msg << std::endl; \
    } while (0)

int main(int argc, char **argv)
{
  const char* lib_path = "lib/libobcdc.so";
  void *libobcdc = dlopen(lib_path, RTLD_LOCAL | RTLD_LAZY);
  char *errstr;
  errstr = dlerror();
  if (errstr != NULL) {
    LOG("[ERROR][DL]A dynamic linking error occurred");
    LOG(errstr);
    return 1;
  }
  dlclose(libobcdc);
  return 0;
}
