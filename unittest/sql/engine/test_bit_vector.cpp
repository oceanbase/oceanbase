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

#include <cstring>

#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "src/sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace sql
{
  class ObTestBitVector: public ::testing::Test
  {
  public:
    ObTestBitVector() {}
    ~ObTestBitVector() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(ObTestBitVector);
  };

void expect_range(ObBitVector *dest_bit_vector, int64_t start, int64_t middle, int64_t end) {
  for (int64_t i = 0; i < start; i++) {
    EXPECT_EQ(0, dest_bit_vector->at(i));
  }
  for (int64_t i = start; i < middle; i++) {
    EXPECT_EQ(1, dest_bit_vector->at(i));
    dest_bit_vector->unset(i);
  }
  EXPECT_EQ(0, dest_bit_vector->at(middle));
  for (int64_t i = middle + 1; i < end; i++) {
    EXPECT_EQ(1, dest_bit_vector->at(i));
    dest_bit_vector->unset(i);
  }
  for (int64_t i = end; i < end + 100; i++) {
    EXPECT_EQ(0, dest_bit_vector->at(i));
  }
}

void test_range(ObBitVector *dest_bit_vector, ObBitVector *src_bit_vector, int64_t start,
                int64_t end)
{
  for (int i = 0; i < 2000; i++) {
    src_bit_vector->set(i);
  }

  int64_t middle = (start + end) / 2;
  dest_bit_vector->set_all(start, end);
  dest_bit_vector->unset(middle);
  expect_range(dest_bit_vector, start, middle, end);

  dest_bit_vector->set_all(start, end);
  dest_bit_vector->unset_all(start, end);
  for (int64_t i = 0; i < end + 100; i++) {
    EXPECT_EQ(0, dest_bit_vector->at(i));
  }

  src_bit_vector->unset(middle);
  dest_bit_vector->deep_copy(*src_bit_vector, start, end);
  expect_range(dest_bit_vector, start, middle, end);

  dest_bit_vector->bit_or(*src_bit_vector, start, end);
  expect_range(dest_bit_vector, start, middle, end);
  src_bit_vector->set(middle);

  for (int64_t i = start; i < end; i++) {
    dest_bit_vector->set(i);
  }
  EXPECT_EQ(1, dest_bit_vector->is_all_true(start, end));
  if (start > 0) {
    EXPECT_EQ(0, dest_bit_vector->is_all_true(start - 1, end));
  }
  EXPECT_EQ(0, dest_bit_vector->is_all_true(start, end + 1));
  for (int64_t i = start; i < end; i++) {
    dest_bit_vector->unset(i);
  }
}

TEST(ObTestBitVector, bit_or_range)
{
  char src_buf[1024];
  char dest_buf[1024];
  MEMSET(src_buf, 0, 1024);
  MEMSET(dest_buf, 0, 1024);
  ObBitVector *src_bit_vector = new (src_buf) ObBitVector;
  ObBitVector *dest_bit_vector = new (dest_buf) ObBitVector;

  test_range(dest_bit_vector, src_bit_vector, 13, 40);
  test_range(dest_bit_vector, src_bit_vector, 13, 63);
  test_range(dest_bit_vector, src_bit_vector, 13, 64);
  test_range(dest_bit_vector, src_bit_vector, 13, 127);
  test_range(dest_bit_vector, src_bit_vector, 13, 128);
  test_range(dest_bit_vector, src_bit_vector, 13, 258);
  test_range(dest_bit_vector, src_bit_vector, 0, 50);
  test_range(dest_bit_vector, src_bit_vector, 0, 100);
  test_range(dest_bit_vector, src_bit_vector, 0, 63);
  test_range(dest_bit_vector, src_bit_vector, 0, 64);
  test_range(dest_bit_vector, src_bit_vector, 0, 0);
  test_range(dest_bit_vector, src_bit_vector, 64, 64);
  test_range(dest_bit_vector, src_bit_vector, 64, 127);

}
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
