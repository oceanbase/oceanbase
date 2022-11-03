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
 */

#include <iostream>
#include <fstream>
#include "lib/alloc/ob_common_allocator.h"
#include "lib/allocator/ob_malloc.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

void *ptrs[10240];
enum { ALLOC = 1, FREE = 2 };

int main(int argc, char *argv[])
{
  if (argc < 2) {
    cout << "USAGE: ./alloc_trace trace_file" << endl;
    return 0;
  }

  fstream fh(argv[1], ios::in);
  if (!fh) {
    cout << "open file fail" << argv[1] << endl;
    return 0;
  }
  int type, idx, size;
  int line = 0;
  while (fh >> type >> idx) {
    cout << "line: " << line << endl;
    line++;
    if (type == ALLOC) {
      void *ptr = NULL;
      if (fh >> size) {
        ptr = ob_malloc(size);
        if (idx >= 0) {
          ptrs[idx] = ptr;
        }
      } else {
        return 1;
      }
    } else if (type == FREE) {
      ob_free(ptrs[idx]);
    } else {
      return 2;
    }
  }
  fh.close();

  return 0;
}
