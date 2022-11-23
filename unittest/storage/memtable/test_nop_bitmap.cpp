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

#include "storage/memtable/ob_memtable_iterator.h"
#include <cstdio>
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestNopBitMap, test1)
{
  ObNopBitMap bitmap;
  bitmap.init(23, 5);
  for (int64_t i = 3; i < 20; ++i) {
    bitmap.set_false(i);
  }
  ASSERT_FALSE(bitmap.is_empty());
  ObObj cells[23];
  for (int64_t i = 0; i < 23; ++i) {
    cells[i].set_int(1L);
  }
  bitmap.set_nop_obj(cells);
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  for (int64_t i = 5; i < 20; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  for (int64_t i = 20; i < 23; ++i) {
    ASSERT_TRUE(cells[i].is_nop_value());
  }
  ObNopBitMap bitmap1;
  bitmap1.init(64, 16);
  for (int64_t i = 16; i < 64; ++i) {
    bitmap1.set_false(i);
  }
  ASSERT_TRUE(bitmap1.is_empty());
}

TEST(TestNopBitMap, test_boundary)
{
  const int64_t cell_cnt = 128;
  ObNopBitMap bitmap;
  bitmap.init(cell_cnt, 64);
  for (int64_t i = 0; i < 72; ++i) {
    bitmap.set_false(i);
  }
  ObObj cells[cell_cnt];
  for (int64_t i = 0; i < cell_cnt; ++i) {
    cells[i].set_int(1L);
  }
  bitmap.set_nop_obj(cells);
  for (int64_t i = 0; i < 72; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  for (int64_t i = 72; i < cell_cnt; ++i) {
    ASSERT_TRUE(cells[i].is_nop_value());
  }
}

TEST(TestNopBitMap, test_long_bitmap)
{
  ObNopBitMap bitmap;
  bitmap.init(120, 23);
  for (int64_t i = 0; i < 50; ++i) {
    bitmap.set_false(i);
  }
  for (int64_t i = 72; i < 120; ++i) {
    bitmap.set_false(i);
  }
  ObObj cells[120];
  for (int64_t i = 0; i < 120; ++i) {
    cells[i].set_int(1L);
  }
  bitmap.set_nop_obj(cells);
  for (int64_t i  = 0; i < 23; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  for (int64_t i = 23; i < 50; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  for (int64_t i = 50; i < 72; ++i) {
    ASSERT_TRUE(cells[i].is_nop_value());
  }
  for (int64_t i = 72; i < 120; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
}

TEST(TestNopBitMap, test_long_key)
{
  ObNopBitMap bitmap;
  bitmap.init(120, 65);
  ObObj cells[120];
  for (int64_t i = 0; i < 120; ++i) {
    cells[i].set_int(1L);
  }
  bitmap.set_nop_obj(cells);
  for (int64_t i  = 0; i < 65; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  for (int64_t i = 65; i < 120; ++i) {
    ASSERT_TRUE(cells[i].is_nop_value());
  }
  bitmap.reuse();
  for (int64_t i = 0; i < 120; ++i) {
    cells[i].set_int(1L);
  }
  for (int64_t i = 0; i < 65; ++i) {
    bitmap.set_false(i);
  }
  bitmap.set_nop_obj(cells);
  for (int64_t i = 65; i < 120; ++i) {
    ASSERT_TRUE(cells[i].is_nop_value());
  }
}

TEST(TestNopBitMap, smoke_test)
{
  ObNopBitMap bitmap;
  bitmap.init(7, 0);
  ASSERT_TRUE(bitmap.test(2));
  for (int64_t i = 1; i < 7; ++i) {
    bitmap.set_false(i);
  }
  ObObj cells[7];
  for (int64_t i = 0; i < 7; ++i) {
    cells[i].set_int(1L);
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  ASSERT_FALSE(bitmap.is_empty());
  bitmap.set_nop_obj(cells);
  ASSERT_TRUE(cells[0].is_nop_value());
  for (int64_t i = 1; i < 6; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }

  bitmap.reuse();
  bitmap.set_nop_obj(cells);
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_TRUE(cells[i].is_nop_value());
  }

  for (int64_t i = 0; i < 7; ++i) {
    cells[i].set_int(i);
  }
  for (int64_t i = 0; i < 6; ++i) {
    bitmap.set_false(i);
  }
  bitmap.set_nop_obj(cells);
  for (int64_t i = 0; i < 6; ++i) {
    ASSERT_FALSE(cells[i].is_nop_value());
  }
  ASSERT_TRUE(cells[6].is_nop_value());

  bitmap.reuse();
  for (int64_t i = 0; i < 7; ++i) {
    cells[i].set_int(i);
  }
  bitmap.set_false(0);
  bitmap.set_false(2);
  bitmap.set_false(4);
  bitmap.set_false(6);
  ASSERT_FALSE(bitmap.is_empty());
  bitmap.set_nop_obj(cells);
  ASSERT_TRUE(cells[1].is_nop_value());
  ASSERT_TRUE(cells[3].is_nop_value());
  ASSERT_TRUE(cells[5].is_nop_value());
  ASSERT_FALSE(cells[0].is_nop_value());
  ASSERT_FALSE(cells[4].is_nop_value());
  ASSERT_FALSE(cells[2].is_nop_value());
  ASSERT_FALSE(cells[6].is_nop_value());

  bitmap.reuse();
  for (int64_t i = 0; i < 7; ++i) {
    bitmap.set_false(i);
  }
  ASSERT_TRUE(bitmap.is_empty());
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_nop_bitmap.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
