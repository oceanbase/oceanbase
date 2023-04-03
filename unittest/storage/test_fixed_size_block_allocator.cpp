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
#include "lib/allocator/ob_fixed_size_block_allocator.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

class FixedSizeBlockAllocatorTest: public ::testing::Test
{
public:
  FixedSizeBlockAllocatorTest();
  virtual ~FixedSizeBlockAllocatorTest();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  FixedSizeBlockAllocatorTest(const FixedSizeBlockAllocatorTest &other);
  FixedSizeBlockAllocatorTest& operator=(const FixedSizeBlockAllocatorTest &other);
};

FixedSizeBlockAllocatorTest::FixedSizeBlockAllocatorTest()
{
}

FixedSizeBlockAllocatorTest::~FixedSizeBlockAllocatorTest()
{
}

void FixedSizeBlockAllocatorTest::SetUp()
{
}

void FixedSizeBlockAllocatorTest::TearDown()
{
}

TEST(FixedSizeBlockAllocator, basic_test)
{
  const int64_t block_size = 1 << 13; // 8K
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  int64_t total_block_num = 10;
  allocator.init(total_block_num);

  EXPECT_EQ(total_block_num, allocator.get_total_block_num());

  void *buf = allocator.alloc();
  EXPECT_TRUE(NULL != buf);
  EXPECT_EQ(total_block_num - 1, allocator.get_free_block_num());

  allocator.free(buf);
  buf = NULL;
  EXPECT_EQ(total_block_num, allocator.get_free_block_num());

  allocator.destroy();
}

TEST(FixedSizeBlockAllocator, multiple_alloc)
{
  const int64_t block_size = 1 << 13; // 8K
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  int64_t total_block_num = 10;
  allocator.init(total_block_num);

  void *buff[total_block_num];
  void *last_ptr = NULL;
  for (int i = 0; i < total_block_num; i++) {
    buff[i] = allocator.alloc();
    EXPECT_EQ(total_block_num - i - 1, allocator.get_free_block_num());
    EXPECT_NE(last_ptr, buff[i]);
    last_ptr = buff[i];
  }

  for (int i = 0; i < total_block_num; i++) {
    allocator.free(buff[i]);
    EXPECT_EQ(i + 1, allocator.get_free_block_num());
  }

  allocator.destroy();
}

TEST(FixedSizeBlockAllocator, invalid_init)
{
  // block number is negative
  const int64_t block_size = 1 << 13; // 8K
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  int ret = allocator.init(-1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  EXPECT_EQ(0, allocator.get_total_block_num());

  allocator.destroy();

  // block size is too large
  const int64_t large_block_size = ObFixedSizeBlockAllocator<block_size>::MAX_MEMORY_ALLOCATION * 2;
  auto &large_allocator = ObFixedSizeBlockAllocator<large_block_size>::get_instance();

  ret = large_allocator.init(10);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  EXPECT_EQ(0, large_allocator.get_total_block_num());

  large_allocator.destroy();
}

TEST(FixedSizeBlockAllocator, exceed_max_block_num)
{
  const int64_t block_size = OB_DEFAULT_MACRO_BLOCK_SIZE * 2; // 4M
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  const int64_t block_num = allocator.MAX_MEMORY_ALLOCATION / OB_DEFAULT_MACRO_BLOCK_SIZE;

  int ret = allocator.init(block_num);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(block_num / 2, allocator.get_total_block_num());

  allocator.destroy();
}

TEST(FixedSizeBlockAllocator, free_invalid_block)
{
  const int64_t block_size = 1 << 13; // 8K
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  int64_t total_block_num = 10;
  allocator.init(total_block_num);

  void *buf = allocator.alloc();
  EXPECT_TRUE(NULL != buf);
  EXPECT_EQ(total_block_num - 1, allocator.get_free_block_num());
  EXPECT_EQ(total_block_num, allocator.get_total_block_num());

  void *invalid_buf = NULL;
  allocator.free(invalid_buf);
  EXPECT_EQ(total_block_num - 1, allocator.get_free_block_num());

  int temp = 10;
  invalid_buf = &temp;
  allocator.free(invalid_buf);
  EXPECT_EQ(total_block_num - 1, allocator.get_free_block_num());

  allocator.destroy();
}

TEST(FixedSizeBlockAllocator, resize)
{
  const int64_t block_size = 1 << 13; // 8K
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  int64_t total_block_num = 3;
  allocator.init(total_block_num);

  void *buf = allocator.alloc();
  EXPECT_TRUE(NULL != buf);
  EXPECT_EQ(total_block_num - 1, allocator.get_free_block_num());
  EXPECT_EQ(total_block_num, allocator.get_total_block_num());

  allocator.free(buf);
  EXPECT_EQ(total_block_num, allocator.get_free_block_num());
  EXPECT_EQ(total_block_num, allocator.get_total_block_num());

  int64_t alloc_cnt = total_block_num + 1;
  void *buf_array[alloc_cnt];
  void *last_ptr = NULL;
  for (int i = 0; i < alloc_cnt; i++) {
    buf_array[i] = allocator.alloc();
    EXPECT_NE(buf_array[i], last_ptr);
    last_ptr = buf_array[i];
    if (i < total_block_num) { // not reach total_block_num yet
      EXPECT_EQ(total_block_num - i - 1, allocator.get_free_block_num());
      EXPECT_EQ(total_block_num, allocator.get_total_block_num());
    } else { // reach total_block_num, automatically expand
      EXPECT_EQ(total_block_num * 2 - i - 1, allocator.get_free_block_num());
      EXPECT_EQ(total_block_num * 2, allocator.get_total_block_num());
    }
  }

  for (int i = 0; i < alloc_cnt; i++) {
    allocator.free(buf_array[i]);
    EXPECT_EQ(total_block_num * 2 - alloc_cnt + i + 1, allocator.get_free_block_num());
    EXPECT_EQ(total_block_num * 2, allocator.get_total_block_num());
  }
  allocator.destroy();
}

TEST(FixedSizeBlockAllocator, multiple_resize)
{
  const int64_t block_size = 1 << 13; // 8K
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();

  int64_t total_block_num = 3;
  allocator.init(total_block_num);

  int64_t curr_total_block_num = total_block_num;
  void * buf_arr[4 * total_block_num];
  for (int i = 0; i < 4 * total_block_num; i++) {
    buf_arr[i] = allocator.alloc();
    EXPECT_TRUE(NULL != buf_arr[i]);

    if (i < total_block_num) {
      curr_total_block_num = total_block_num;
    } else if (i < 2 * total_block_num) {
      curr_total_block_num = total_block_num * 2;
    } else {
      curr_total_block_num = total_block_num * 4;
    }
    EXPECT_EQ(curr_total_block_num - i - 1, allocator.get_free_block_num());
    EXPECT_EQ(curr_total_block_num, allocator.get_total_block_num());
  }

  for (int i = 0; i < 4 * total_block_num; i++) {
    allocator.free(buf_arr[i]);

    EXPECT_EQ(i + 1, allocator.get_free_block_num());
    EXPECT_EQ(4 * total_block_num, allocator.get_total_block_num());
  }

  allocator.destroy();
}

TEST(FixedSizeBlockAllocator, overlimit_resize)
{
  const int64_t block_size = ObFixedSizeBlockAllocator<2048>::MAX_MEMORY_ALLOCATION / 2;
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();
  allocator.init(1);

  void * ptr_arr[3];
  for (int i =0; i < 3; i++)
  {
    ptr_arr[i] = allocator.alloc();
    if (i == 1) {
      EXPECT_TRUE(NULL != ptr_arr[i]);
      EXPECT_EQ(0, allocator.get_free_block_num());
      EXPECT_EQ(2, allocator.get_total_block_num());
    } else if (i == 2)
    {
      EXPECT_TRUE(NULL == ptr_arr[i]);
      EXPECT_EQ(0, allocator.get_free_block_num());
      EXPECT_EQ(2, allocator.get_total_block_num());
    }
  }
  allocator.destroy();
}

TEST(FixedSizeBlockMemoryContext, simple)
{
  const int64_t block_size = 1 << 13; // 8K
  int64_t block_num = 3;
  auto &allocator = ObFixedSizeBlockAllocator<block_size>::get_instance();
  allocator.init(block_num);
  int ret = OB_SUCCESS;

  {
    ObFixedSizeBlockMemoryContext<block_size> mem_ctx;
    ret = mem_ctx.init();
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(block_num, allocator.get_total_block_num());

    void *buf[block_num + 1];
    for (int i = 0; i < block_num + 1; i++) {
      buf[i] = mem_ctx.alloc();
      EXPECT_TRUE(NULL != buf[i]);
      EXPECT_EQ(i + 1, mem_ctx.get_used_block_num());

      if (i < block_num) {
        EXPECT_EQ(block_num - i - 1, allocator.get_free_block_num());
      } else { // underlying ObFixedSizeBlockAllocator will automatically expand capacity
        EXPECT_EQ(block_num * 2 - i - 1, allocator.get_free_block_num());
      }
    }
    EXPECT_EQ(block_num - 1, allocator.get_free_block_num());
    EXPECT_EQ(block_num + 1, mem_ctx.get_used_block_num());

    for (int i = 0; i < block_num - 2; i++) {
      mem_ctx.free(buf[i]);
      EXPECT_EQ(block_num + i, allocator.get_free_block_num());
      EXPECT_EQ(block_num - i, mem_ctx.get_used_block_num());
    }
    // mem_ctx destruct should alert error as not all blocks are freed
  }
  EXPECT_EQ(block_num, allocator.get_free_block_num());
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
