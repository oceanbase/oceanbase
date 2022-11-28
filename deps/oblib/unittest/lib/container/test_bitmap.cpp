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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#include "lib/container/ob_bitmap.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{
class TestObBitmap: public ::testing::Test
{
public:
  static constexpr uint64_t bits_per_memblock()
  {
    return ObBitmap::BITS_PER_BLOCK * ObBitmap::BLOCKS_PER_MEM_BLOCK;
  }
  TestObBitmap() {};
  virtual ~TestObBitmap() {};

  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
protected:
  ModulePageAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestObBitmap);
};

TEST_F(TestObBitmap, basic_funcs)
{
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(128));
  EXPECT_EQ(128, bitmap.valid_bits_);
  EXPECT_EQ(bits_per_memblock(), bitmap.capacity());
  EXPECT_EQ(0, bitmap.block_index(10));
  EXPECT_EQ(10, bitmap.bit_index(10));
  EXPECT_EQ(1, bitmap.block_index(70));
  EXPECT_EQ(6, bitmap.bit_index(70));

  EXPECT_EQ(0, bitmap.block_index(63));
  EXPECT_EQ(1, bitmap.block_index(64));

  EXPECT_EQ(static_cast<ObBitmap::size_type>(1), bitmap.bit_mask(0));
  EXPECT_EQ(static_cast<ObBitmap::size_type>(2), bitmap.bit_mask(1));
  EXPECT_EQ(static_cast<ObBitmap::size_type>(4), bitmap.bit_mask(2));
  EXPECT_EQ(static_cast<ObBitmap::size_type>(8), bitmap.bit_mask(3));

  EXPECT_TRUE(bitmap.is_all_false());
  EXPECT_FALSE(bitmap.is_all_true());
  EXPECT_EQ(OB_SUCCESS, bitmap.set(0, true));
  EXPECT_FALSE(bitmap.is_all_false());
  EXPECT_EQ(OB_SUCCESS, bitmap.flip(0));
  EXPECT_TRUE(bitmap.is_all_false());
  EXPECT_EQ(OB_SUCCESS, bitmap.flip(0));
  EXPECT_FALSE(bitmap.is_all_false());
  EXPECT_EQ(OB_SUCCESS, bitmap.wipe(0));
  EXPECT_TRUE(bitmap.is_all_false());
  bitmap.reuse();
}

TEST_F(TestObBitmap, and_or_not)
{
  ObBitmap left(allocator_);
  EXPECT_EQ(OB_SUCCESS, left.init(512));
  ObBitmap right(allocator_);
  EXPECT_EQ(OB_SUCCESS, right.init(512));
  for (int64_t i = 10; i < 20; ++i) {
    EXPECT_EQ(OB_SUCCESS, left.set(i, true));
    EXPECT_EQ(OB_SUCCESS, right.set(i+5, true));
  }

  EXPECT_EQ(10, left.popcnt());
  EXPECT_EQ(OB_SUCCESS, left.bit_and(right));
  EXPECT_EQ(5, left.popcnt());

  left.reuse();
  right.reuse();
  left.expand_size(bits_per_memblock() + 1);
  right.expand_size(bits_per_memblock() + 1);
  EXPECT_EQ(left.capacity(), bits_per_memblock() * 2);
  EXPECT_EQ(right.capacity(), bits_per_memblock() * 2);
  EXPECT_TRUE(left.is_all_false());
  EXPECT_TRUE(right.is_all_false());
  left.expand_size(bits_per_memblock() + 1024);
  right.expand_size(bits_per_memblock() + 1024);
  EXPECT_EQ(left.capacity(), bits_per_memblock() * 2);
  EXPECT_EQ(right.capacity(), bits_per_memblock() * 2);

  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_EQ(OB_SUCCESS, left.set(i, true));
    EXPECT_EQ(OB_SUCCESS, right.set(i+5, true));
  }
  EXPECT_EQ(OB_SUCCESS, left.bit_not());
  EXPECT_EQ(bits_per_memblock() + 1024 - 10, left.popcnt());
  ObBitmap::MemBlock *walk = left.header_;
  ObBitmap::size_type popcnt = 0;
  int64_t counted_bits = 0;
  while (NULL != walk && counted_bits < left.valid_bits_) {
    for (ObBitmap::size_type i = 0; i < ObBitmap::BLOCKS_PER_MEM_BLOCK; ++i) {
      popcnt += __builtin_popcountl(walk->bits_[i]);
      counted_bits += ObBitmap::BITS_PER_BLOCK;
    }
    walk = walk->next_;
  }
  EXPECT_EQ(popcnt, left.popcnt());
  EXPECT_EQ(OB_SUCCESS, left.bit_not());
  EXPECT_EQ(10, left.popcnt());
  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_TRUE(left.test(i));
  }
  EXPECT_FALSE(left.test(4089));
  EXPECT_FALSE(left.test(4101));

  EXPECT_EQ(OB_SUCCESS, left.bit_and(right));
  EXPECT_EQ(5, left.popcnt());
  EXPECT_EQ(OB_SUCCESS, left.bit_or(right));
  EXPECT_EQ(10, left.popcnt());
}

TEST_F(TestObBitmap, load_from_array)
{
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(512));
  EXPECT_EQ(512, bitmap.size());
  ObBitmap::size_type data[] = {7, 7, 7, 7, 7};
  EXPECT_EQ(OB_SUCCESS, bitmap.load_blocks_from_array(data, 64 * 5));
  EXPECT_EQ(64 * 5, bitmap.size());
  EXPECT_EQ(3 * 5, bitmap.popcnt());

  bitmap.expand_size(4096);
  ObBitmap::size_type data2[64];
  for (int64_t i = 0; i < 64; i++) {
    data2[i] = 7;
  }
  EXPECT_EQ(OB_SUCCESS, bitmap.load_blocks_from_array(data2, 64 * 64));
  EXPECT_EQ(64 * 64, bitmap.size());
  EXPECT_EQ(3 * 64, bitmap.popcnt());
}

} // end of namespace unittest
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
