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

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_malloc.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

int main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);
  ObHashSet<int64_t> set;
  ObHashSet<int64_t>::iterator iter;
  set.create(10);

  for (int64_t i = 0; i < 10; i++)
  {
    assert(OB_SUCCESS == set.set_refactored(i));
  }

  for (int64_t i = 0; i < 10; i++)
  {
    assert(OB_HASH_EXIST == set.exist_refactored(i));
  }

  int64_t i = 0;
  for (iter = set.begin(); iter != set.end(); iter++, i++)
  {
    assert(i == iter->first);
    fprintf(stderr, "%s\n", typeid(iter->second).name());
  }
}
