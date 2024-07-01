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
#include <chrono>
#define private public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#include "common/ob_target_specific.h"
#include "lib/container/ob_bitmap.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{

class TestObBitmap: public ::testing::Test
{
public:
  TestObBitmap() {};
  virtual ~TestObBitmap() {};
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase()
  {
    LOG_INFO("Supported cpu instructions",
              K(common::is_arch_supported(ObTargetArch::Default)),
              K(common::is_arch_supported(ObTargetArch::SSE42)),
              K(common::is_arch_supported(ObTargetArch::AVX)),
              K(common::is_arch_supported(ObTargetArch::AVX2)),
              K(common::is_arch_supported(ObTargetArch::AVX512)));
  }
  static void TearDownTestCase() {}
protected:
  ModulePageAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestObBitmap);

  inline bool test(const int64_t idx, uint64_t *data = nullptr) const
  {
    if (nullptr == data) {
      data = data_;
    }
    return data[idx / 64] & (1LU << (idx % 64));
  }

  inline void set(const int64_t idx, uint64_t *data = nullptr)
  {
    if (nullptr == data) {
      data = data_;
    }
    data[idx / 64] |= 1LU << (idx % 64);
  }
  uint64_t *data_;
};

TEST_F(TestObBitmap, basic_funcs)
{
  const uint64_t bitmap_size = 1666;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  EXPECT_EQ(bitmap_size, bitmap.valid_bytes_);
  EXPECT_EQ(2048, bitmap.capacity());
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

TEST_F(TestObBitmap, load_from_array)
{
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(512));
  EXPECT_EQ(512, bitmap.size());
  ObBitmap::size_type data[] = {0, 0, 0, 0, 10};
  EXPECT_EQ(OB_SUCCESS, bitmap.load_blocks_from_array(data, 64 * 5));
  EXPECT_EQ(64 * 5, bitmap.size());
  EXPECT_EQ(2, bitmap.popcnt());
  EXPECT_TRUE(bitmap.test(4 * 64 + 1));
  EXPECT_TRUE(bitmap.test(4 * 64 + 3));
}

TEST_F(TestObBitmap, find_first_last_true_pos)
{
  const uint64_t bitmap_size = 8193;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitmap.set(i, true));
  }
  int64_t true_pos;
  EXPECT_EQ(OB_SUCCESS, bitmap.next_valid_idx(0, 8193, false, true_pos));
  EXPECT_EQ(4090, true_pos);
  EXPECT_EQ(OB_SUCCESS, bitmap.next_valid_idx(0, 8193, true, true_pos));
  EXPECT_EQ(4099, true_pos);
  bitmap.set(1, true);
  bitmap.set(8192, true);
  EXPECT_EQ(OB_SUCCESS, bitmap.next_valid_idx(0, 8193, false, true_pos));
  EXPECT_EQ(1, true_pos);
  EXPECT_EQ(OB_SUCCESS, bitmap.next_valid_idx(0, 8193, true, true_pos));
  EXPECT_EQ(8192, true_pos);
}

TEST_F(TestObBitmap, is_all_false_in_range)
{
  const uint64_t bitmap_size = 8193;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitmap.set(i, true));
  }
  EXPECT_EQ(OB_SUCCESS, bitmap.bit_not());
  EXPECT_TRUE(bitmap.is_all_false(4090, 4099));
  for (int64_t i = 1; i < 4090; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitmap.set(i, false));
  }
  EXPECT_TRUE(bitmap.is_all_false(1, 4099));
  EXPECT_EQ(OB_SUCCESS, bitmap.set(4093, true));
  EXPECT_FALSE(bitmap.is_all_false(4090, 4099));
}

TEST_F(TestObBitmap, is_all_true_in_range)
{
  const uint64_t bitmap_size = 8193;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitmap.set(i, true));
  }
  EXPECT_TRUE(bitmap.is_all_true(4090, 4099));
}

TEST_F(TestObBitmap, append_bit_map)
{
  const uint64_t bitmap_size = 8193;
  const uint64_t append_bitmap_size = 998;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitmap.set(i, true));
  }
  EXPECT_EQ(OB_SUCCESS, bitmap.bit_not());
  EXPECT_FALSE(bitmap.is_all_false(3007, 3997));
  EXPECT_FALSE(bitmap.is_all_false(4007, 4997));
  EXPECT_EQ(OB_SUCCESS, bitmap.bit_not());
  ObBitmap append_bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, append_bitmap.init(append_bitmap_size));
  for (int64_t i = 7; i < 998; ++i) {
    EXPECT_EQ(OB_SUCCESS, append_bitmap.set(i, true));
  }

  EXPECT_EQ(OB_SUCCESS, bitmap.append_bitmap(append_bitmap, 3000, false));
  EXPECT_EQ(OB_SUCCESS, bitmap.bit_not());
  EXPECT_TRUE(bitmap.is_all_false(3007, 3997));
  EXPECT_EQ(OB_SUCCESS, bitmap.bit_not());
  EXPECT_EQ(OB_SUCCESS, bitmap.append_bitmap(append_bitmap, 4000, true));
  EXPECT_EQ(OB_SUCCESS, bitmap.bit_not());
  EXPECT_TRUE(bitmap.is_all_false(3195, 4185));
}

TEST_F(TestObBitmap, get_row_ids)
{
  const uint64_t bitmap_size = 8193;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  EXPECT_EQ(OB_SUCCESS, bitmap.set(3000, true));
  EXPECT_EQ(OB_SUCCESS, bitmap.set(4000, true));
  for (int64_t i = 4090; i < 4100; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitmap.set(i, true));
  }
  int32_t *row_ids = reinterpret_cast<int32_t *>(allocator_.alloc(800));
  int64_t row_count = 0;
  int64_t from = 0;
  EXPECT_EQ(OB_SUCCESS, bitmap.get_row_ids(row_ids, row_count, from, 3000, INT64_MAX));
  EXPECT_EQ(0, row_count);
  EXPECT_EQ(3000, from);

  from = 0;
  EXPECT_EQ(OB_SUCCESS, bitmap.get_row_ids(row_ids, row_count, from, 4001, INT64_MAX));
  EXPECT_EQ(2, row_count);
  EXPECT_EQ(4001, from);
  EXPECT_EQ(3000, row_ids[0]);
  EXPECT_EQ(4000, row_ids[1]);

  from = 0;
  EXPECT_EQ(OB_SUCCESS, bitmap.get_row_ids(row_ids, row_count, from, 8193, INT64_MAX));
  EXPECT_EQ(12, row_count);
  EXPECT_EQ(8193, from);
  EXPECT_EQ(3000, row_ids[0]);
  EXPECT_EQ(4000, row_ids[1]);
  for (int64_t i = 2; i < 12; ++i) {
    EXPECT_EQ(4090 + i - 2, row_ids[i]);
  }

  int ret = OB_SUCCESS;
  from = 0;
  EXPECT_EQ(OB_SUCCESS, bitmap.get_row_ids(row_ids, row_count, from, 8193, 10));
  EXPECT_EQ(10, row_count);
  EXPECT_EQ(4098, from);
  EXPECT_EQ(3000, row_ids[0]);
  EXPECT_EQ(4000, row_ids[1]);
  for (int64_t i = 2; i < 10; ++i) {
    EXPECT_EQ(4090 + i - 2, row_ids[i]);
  }

  from = 3500;
  EXPECT_EQ(OB_SUCCESS, bitmap.get_row_ids(row_ids, row_count, from, 8193, INT64_MAX));
  EXPECT_EQ(11, row_count);
  EXPECT_EQ(8193, from);
  EXPECT_EQ(4000, row_ids[0]);
  for (int64_t i = 1; i < 11; ++i) {
    EXPECT_EQ(4090 + i - 1, row_ids[i]);
  }

  from = 3500;
  EXPECT_EQ(OB_SUCCESS, bitmap.get_row_ids(row_ids, row_count, from, 8193, 7));
  EXPECT_EQ(7, row_count);
  EXPECT_EQ(4096, from);
  EXPECT_EQ(4000, row_ids[0]);
  for (int64_t i = 1; i < 7; ++i) {
    EXPECT_EQ(4090 + i - 1, row_ids[i]);
  }
}

TEST_F(TestObBitmap, from_bits_mask)
{
  const uint64_t bitmap_size = 8193;
  const uint64_t mem_size = 2000;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  data_ = static_cast<uint64_t *>(allocator_.alloc(mem_size));
  MEMSET(static_cast<void *>(data_), 0, mem_size);
  const uint64_t test_size = 32 * 5 + 31;
  for (int i = 0; i < test_size; ++i) {
    if ((i % 2) == 1) {
      set(i);
    }
  }
  set(4);
  set(166);
  set(190);

  int64_t offset = 123;
  OK(bitmap.from_bits_mask(offset, offset + test_size, reinterpret_cast<uint8_t *>(data_)));
  for (int i = offset; i < (test_size + offset); ++i) {
    const int64_t index = i - offset;
    if ((index % 2) == 1 || index == 4 || index == 166 || index == 190) {
      ASSERT_TRUE(bitmap.test(i));
    } else {
      ASSERT_FALSE(bitmap.test(i));
    }
  }

  offset = 0;
  bitmap.reuse();
  OK(bitmap.from_bits_mask(offset, offset + test_size, reinterpret_cast<uint8_t *>(data_)));
  for (int i = offset; i < (test_size + offset); ++i) {
    const int64_t index = i - offset;
    if ((index % 2) == 1 || index == 4 || index == 166 || index == 190) {
      ASSERT_TRUE(bitmap.test(i));
    } else {
      ASSERT_FALSE(bitmap.test(i));
    }
  }
}

TEST_F(TestObBitmap, to_bits_mask)
{
  const uint64_t bitmap_size = 8193;
  const uint64_t mem_size = 2000;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  data_ = static_cast<uint64_t *>(allocator_.alloc(mem_size));
  MEMSET(static_cast<void *>(data_), 0, mem_size);
  for (int i = 0; i < 8000; ++i) {
    if ((i % 2) == 1) {
      bitmap.set(i);
    }
  }
  bitmap.set(2);
  bitmap.set(4);
  bitmap.set(6);
  OK(bitmap.to_bits_mask(6, 8000, false, reinterpret_cast<uint8_t *>(data_)));
  for (int64_t i = 0; i < 7994; ++i) {
    const uint64_t idx = i + 6;
    if ((idx % 2) == 1 || idx == 2 || idx == 4 || idx == 6) {
      ASSERT_TRUE(test(i));
    } else {
      ASSERT_FALSE(test(i));
    }
  }

  MEMSET(static_cast<void *>(data_), 0, mem_size);
  bitmap.to_bits_mask(6, 8000, true, reinterpret_cast<uint8_t *>(data_));
  for (int64_t i = 0; i < 7994; ++i) {
    const uint64_t idx = i + 6;
    if ((idx % 2) == 1 || idx == 2 || idx == 4 || idx == 6) {
      ASSERT_FALSE(test(i));
    } else {
      ASSERT_TRUE(test(i));
    }
  }
}

TEST_F(TestObBitmap, bitmap_filter)
{
  const int64_t row_size = 1000;
  const int64_t uint64_byte_size = (row_size + 63) / 64 * 8;
  const bool has_null = true;
  uint64_t *null_vector = static_cast<uint64_t *>(allocator_.alloc(uint64_byte_size));
  uint64_t *skip = static_cast<uint64_t *>(allocator_.alloc(uint64_byte_size));
  uint64_t *data = static_cast<uint64_t *>(allocator_.alloc(row_size * 8));
  MEMSET(static_cast<void *>(null_vector), 0, uint64_byte_size);
  MEMSET(static_cast<void *>(skip), 0, uint64_byte_size);
  for (int64_t i = 0; i < row_size; ++i) {
    if ((i % 2) == 0) {
      set(i, skip);
    }
    if ((i % 3) == 0) {
      set(i, null_vector);
    }
    if ((i % 5) == 0) {
      data[i] = 0;
    } else {
      data[i] = 1;
    }
  }
  ObBitmap::filter(has_null, reinterpret_cast<uint8_t *>(null_vector), data, row_size, reinterpret_cast<uint8_t *>(skip));
  for (int64_t i = 0; i < row_size; ++i) {
    if ((i % 2) == 0 || (i % 3 == 0) || (i % 5 == 0)) {
      EXPECT_TRUE(test(i, skip));
    } else {
      EXPECT_FALSE(test(i, skip));
    }
  }
}

TEST_F(TestObBitmap, benchmark_bitmap_filter)
{
  const int64_t row_size = 256;
  const int64_t uint64_byte_size = (row_size + 63) / 64 * 8;
  const bool has_null = true;
  uint64_t *null_vector = static_cast<uint64_t *>(allocator_.alloc(uint64_byte_size));
  uint64_t *skip = static_cast<uint64_t *>(allocator_.alloc(uint64_byte_size));
  uint64_t *data = static_cast<uint64_t *>(allocator_.alloc(row_size * 8));
  auto start = std::chrono::high_resolution_clock::now();
  size_t loop_time = 1000000;
  while (loop_time--) {
    ObBitmap::filter(has_null, reinterpret_cast<uint8_t *>(null_vector), data, row_size, reinterpret_cast<uint8_t *>(skip));
  }
  auto end = std::chrono::high_resolution_clock::now();
  LOG_INFO("benchmark_bitmap_filter costs ns",
            K(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));
}

// For 8192 rows:
// AVX512: the time cost decreased from ~10000ns -> ~300ns
// BMI2: the time cost decreased from ~10000ns -> ~700ns
TEST_F(TestObBitmap, benchmark_from_bits_mask)
{
  const uint64_t bitmap_size = 8192;
  const uint64_t mem_size = 1024;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  data_ = static_cast<uint64_t *>(allocator_.alloc(mem_size));
  MEMSET(static_cast<void *>(data_), 0, mem_size);
  for (int i = 0; i < 8192; ++i) {
    if ((i % 2) == 1) {
      set(i);
    }
  }

  auto start = std::chrono::high_resolution_clock::now();
  bitmap.from_bits_mask(0, 8192, reinterpret_cast<uint8_t *>(data_));
  auto end = std::chrono::high_resolution_clock::now();
  LOG_INFO("benchmark_from_bits_mask costs ns",
            K(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));

  auto start2 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 8192; ++i) {
    if (test(i)) {
      bitmap.set(i);
    }
  }
  auto end2 = std::chrono::high_resolution_clock::now();
  LOG_INFO("benchmark_from_bits_mask control group costs ns",
            K(std::chrono::duration_cast<std::chrono::nanoseconds>(end2 - start2).count()));
}

// For 8192 rows:
// AVX512: the time cost decreased from ~9000ns -> ~200ns
// BMI2: the time cost decreased from ~9000ns -> ~400ns
TEST_F(TestObBitmap, benchmark_to_bits_mask)
{
  const uint64_t bitmap_size = 8192;
  const uint64_t mem_size = 1024;
  ObBitmap bitmap(allocator_);
  EXPECT_EQ(OB_SUCCESS, bitmap.init(bitmap_size));
  data_ = static_cast<uint64_t *>(allocator_.alloc(mem_size));
  MEMSET(static_cast<void *>(data_), 0, mem_size);
  for (int i = 0; i < 8192; ++i) {
    if ((i % 2) == 1) {
      bitmap.set(i);
    }
  }

  auto start = std::chrono::high_resolution_clock::now();
  bitmap.to_bits_mask(0, 8192, false, reinterpret_cast<uint8_t *>(data_));
  auto end = std::chrono::high_resolution_clock::now();
  LOG_INFO("benchmark_to_bits_mask costs ns",
            K(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));

  auto start2 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 8192; ++i) {
    if (bitmap.test(i)) {
      set(i);
    }
  }
  auto end2 = std::chrono::high_resolution_clock::now();
  LOG_INFO("benchmark_to_bits_mask control group costs ns",
            K(std::chrono::duration_cast<std::chrono::nanoseconds>(end2 - start2).count()));
}

} // end of namespace unittest
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  system("rm -f test_bitmap.log*");
  OB_LOGGER.set_file_name("test_bitmap.log", true, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
