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

#include <gtest/gtest.h>
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_delay_free_allocator.h"

namespace oceanbase
{
namespace common
{
TEST(ObDelayFreeAllocator, test_delay_free_allocator)
{
  int ret = OB_SUCCESS;
  ObDelayFreeAllocator allocator;
  const int64_t local_array_size = 100000;
  void *data = NULL;
  void *array[local_array_size];
  int64_t i = 0;
  int64_t size = 1000;

  // test invalid init
  ret = allocator.init("DelayFreeAlloc", false, -1);
  EXPECT_NE(OB_SUCCESS, ret);

  // test invalid alloc and free
  data = allocator.alloc(size);
  EXPECT_EQ(NULL, data);
  allocator.free(data);

  // test init
  ret = allocator.init("DelayFreeAlloc", false, 0);
  EXPECT_EQ(OB_SUCCESS, ret);

  // test repeatly init
  ret = allocator.init("DelayFreeAlloc", false, 0);
  EXPECT_NE(OB_SUCCESS, ret);

  // test destroy
  allocator.destroy();

  // test init
  ret = allocator.init("DelayFreeAlloc", true, 0);
  EXPECT_EQ(OB_SUCCESS, ret);

  // test normal alloc
  data = allocator.alloc(size);
  EXPECT_TRUE(data != NULL);

  // test free
  allocator.free(data);

  // test invalid free
  allocator.free(NULL);

  // test alloc and free
  for (i = 0; i < local_array_size; i++) {
    if (i >= 10000) {
      allocator.free(array[i - 10000]);
      array[i - 10000] = NULL;
    }
    array[i] = allocator.alloc(size);
    EXPECT_TRUE(array[i] != NULL);
  }

  for (i = local_array_size - 1; i >= 90000; i--) {
    allocator.free(array[i]);
    array[i] = 0;
  }

  for (i = 0; i < local_array_size; i++) {
    array[i] = allocator.alloc(size);
    EXPECT_TRUE(array[i] != NULL);
  }

  for (i = 0; i < local_array_size; i++) {
    allocator.free(array[i]);
    array[i] = NULL;
  }

  // test alloc large size
  array[0] = allocator.alloc(1024 * 1024 * 4);
  EXPECT_TRUE(array[0] != NULL);

  // test reset
  allocator.reset();
  EXPECT_EQ(0, allocator.get_total_size());
  EXPECT_EQ(0, allocator.get_memory_fragment_size());

}

}
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
