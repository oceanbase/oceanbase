/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/hash/ob_hashset.h"


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
