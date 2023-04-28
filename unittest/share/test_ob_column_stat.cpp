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

#include <cstdlib>

#include "gtest/gtest.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase;
using namespace common;

int64_t random_number(int64_t min, int64_t max)
{
  static int dev_random_fd = -1;
  char *next_random_byte = NULL;
  int bytes_to_read = 0;
  int64_t random_value = 0;

  assert(max >= min);

  if (dev_random_fd == -1)
  {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    assert(dev_random_fd != -1);
  }

  next_random_byte = (char *)&random_value;
  bytes_to_read = sizeof(random_value);

  do
  {
    int bytes_read = 0;
    bytes_read = static_cast<int32_t>(read(dev_random_fd, next_random_byte, bytes_to_read));
    bytes_to_read -= bytes_read;
    next_random_byte += bytes_read;
  } while(bytes_to_read > 0);

  return min + (abs(static_cast<int32_t>(random_value)) % (max - min + 1));
}

typedef int (*MAKE_OBJECT_FUNC)(common::ObIAllocator &allocator, const int64_t seed, ObObj &value);

int make_int(common::ObIAllocator &allocator, const int64_t seed, ObObj &value)
{
  UNUSED(allocator);
  value.set_int(seed);
  return OB_SUCCESS;
}

int make_utf8mb4_ci_str(common::ObIAllocator &allocator, const int64_t seed, ObObj &value)
{
  char *str_value = (char*)allocator.alloc(100);
  snprintf(str_value, 100, "00000%05ld", seed);
  value.set_varchar(str_value, 10);
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  return OB_SUCCESS;
}

int make_binary_str(common::ObIAllocator &allocator, const int64_t seed, ObObj &value)
{
  char *str_value = (char*)allocator.alloc(100);
  snprintf(str_value, 100, "00000%05ld", seed);
  value.set_varchar(str_value, 10);
  value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  return OB_SUCCESS;
}

int add_some_values(const int64_t start, const int64_t end, MAKE_OBJECT_FUNC func, ObColumnStat &stat)
{
  int ret = OB_SUCCESS;
  DefaultPageAllocator arena;
  ObObj value;
  for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
    if (OB_FAIL(func(arena, i, value))) {
      COMMON_LOG(WARN, "generate object value failed.", K(ret), K(i));
    } else if (OB_FAIL(stat.add_value(value))) {
      COMMON_LOG(WARN, "add value failed.", K(stat), K(value));
    }
  }
  return ret;
}

 void test_estimate_ndv(ObColumnStat &stat, const int64_t count, MAKE_OBJECT_FUNC func)
{
  int ret = OB_SUCCESS;
  int64_t approximate_lower = count * 90 / 100;
  int64_t approximate_upper = count * 110 / 100;

  int64_t start = 0;
  ret = add_some_values(start, count / 2, func, stat);
  ASSERT_EQ(OB_SUCCESS, ret);

  start = std::min(count / 4, count);
  ret = add_some_values(start, count, func, stat);
  ASSERT_EQ(OB_SUCCESS, ret);
  stat.finish();

  int64_t ndv = stat.get_num_distinct();
  fprintf(stderr, "ndv=[%ld]\n", ndv);
  ASSERT_TRUE(ndv >= approximate_lower && ndv <= approximate_upper);

  start = std::min(count / 3, count / 2);
  ret = add_some_values(start, count * 2, func, stat);
  ASSERT_EQ(OB_SUCCESS, ret);
  stat.finish();

  ndv = stat.get_num_distinct();
  fprintf(stderr, "ndv=[%ld]\n", ndv);
  approximate_lower = count * 2 * 90 / 100;
  approximate_upper = count * 2 * 110 / 100;
  ASSERT_TRUE(ndv >= approximate_lower && ndv <= approximate_upper);
}

TEST(ObColumnStat, deep_copy)
{
  ObColumnStat stat;
  ObObj min_value,max_value;
  min_value.set_int(1000);
  max_value.set_int(2000);
  char bitmap[256];
  memset(bitmap, 1, 256);
  stat.set_table_id(1);
  stat.set_partition_id(2);
  stat.set_column_id(16);
  stat.set_num_null(100);
  stat.set_num_distinct(0);
  stat.set_version(1);
  stat.set_min_value(min_value);
  stat.set_max_value(max_value);
  stat.set_llc_bitmap(bitmap, 256);
  int64_t size = stat.get_deep_copy_size();
  ASSERT_EQ(size, static_cast<int64_t>(sizeof(ObColumnStat) + 256));

  DefaultPageAllocator arena;
  char * buf = (char*)arena.alloc(size);
  int64_t pos = 0;
  ObColumnStat dst;
  int ret = dst.deep_copy(stat, buf, size, pos);
  ASSERT_EQ(ret, 0);
  ASSERT_LE(pos, size);
  ASSERT_EQ(dst.get_table_id(), stat.get_table_id());
  ASSERT_EQ(dst.get_partition_id(), stat.get_partition_id());
  ASSERT_EQ(dst.get_column_id(), stat.get_column_id());
  ASSERT_EQ(dst.get_num_null(), stat.get_num_null());
  ASSERT_EQ(dst.get_num_distinct(), stat.get_num_distinct());
  ASSERT_EQ(dst.get_version(), stat.get_version());
  ASSERT_TRUE(dst.get_min_value() == stat.get_min_value());
  ASSERT_TRUE(dst.get_max_value() == stat.get_max_value());
  ASSERT_TRUE(memcmp(dst.get_llc_bitmap(), stat.get_llc_bitmap(), 256) == 0);
}

TEST(ObColumnStat, add_value)
{
  DefaultPageAllocator arena;
  ObColumnStat stat(arena);

  stat.set_table_id(1);
  stat.set_partition_id(2);
  stat.set_column_id(16);
  stat.set_version(1);

  int64_t NDV = random_number(1000, 15000);
  int64_t approximate_lower = NDV * 90 / 100;
  int64_t approximate_upper = NDV * 110 / 100;
  ObObj value;
  for (int64_t i = 0; i < (NDV * 50 / 100); ++i) {
    value.set_int(i);
    stat.add_value(value);
  }
  for (int64_t i = 200; i < NDV; ++i) {
    value.set_int(i);
    stat.add_value(value);
  }
  stat.finish();
  int64_t min_int = 0, max_int = 0;
  stat.get_min_value().get_int(min_int);
  stat.get_max_value().get_int(max_int);
  ASSERT_EQ(min_int, 0);
  ASSERT_EQ(max_int, NDV-1);
  int64_t ndv = stat.get_num_distinct();
  COMMON_LOG(INFO, "print", K(ndv), K(approximate_lower), K(approximate_upper));
  ASSERT_TRUE(ndv >= approximate_lower && ndv <= approximate_upper);

  NDV = NDV * 2;
  for (int64_t i = 600; i < NDV; ++i) {
    value.set_int(i);
    stat.add_value(value);
  }
  stat.finish();
  stat.get_max_value().get_int(max_int);
  ASSERT_EQ(max_int, NDV-1);
  ndv = stat.get_num_distinct();
  approximate_lower = NDV * 90 / 100;
  approximate_upper = NDV * 110 / 100;
  COMMON_LOG(INFO, "print", K(ndv), K(approximate_lower), K(approximate_upper));
  ASSERT_TRUE(ndv >= approximate_lower && ndv <= approximate_upper);
}

TEST(ObColumnStat, ndv)
{
  DefaultPageAllocator arena;
  ObColumnStat stat(arena);
  test_estimate_ndv(stat, 20000, make_utf8mb4_ci_str);
  stat.reset();
  test_estimate_ndv(stat, 20000, make_binary_str);
}


int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
