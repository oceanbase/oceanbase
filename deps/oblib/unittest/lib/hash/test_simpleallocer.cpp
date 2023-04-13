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

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_serialization.h"
#include "lib/allocator/ob_malloc.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <ext/hash_map>
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

std::set<void *> ptr_set;
class MyAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size)
  {
    void *ptr = ob_malloc(size, "test");
    ptr_set.insert(ptr);
    return ptr;
  }
  void *alloc(const int64_t , const ObMemAttr &)
    { return NULL; }
  void free(void *ptr)
  {
    ptr_set.erase(ptr);
  }
};

TEST(TestSimpleAllocer, allocate)
{
  SimpleAllocer<int, 10, SpinMutexDefendMode, MyAllocator> alloc;
  const int N = 100;
  int *ptrs[N];
  int i = N;
  while (i--) {
    int *ptr = alloc.alloc();
    EXPECT_TRUE(ptr != NULL);
    ptrs[i] = ptr;
  }
  i= N;
  EXPECT_TRUE(!ptr_set.empty());
  while (i--) {
    alloc.free(ptrs[i]);
  }
  EXPECT_TRUE(ptr_set.empty());
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("ERROR");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
