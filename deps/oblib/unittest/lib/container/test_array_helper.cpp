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
#include "lib/utility/utility.h"
#include "lib/container/ob_array_helper.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class TestArrayHelper : public ::testing::Test
{
public:
  TestArrayHelper() {}
  virtual ~TestArrayHelper() {}

  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestArrayHelper, common)
{
  // not init
  ObArrayHelper<int64_t> not_init_array;
  ASSERT_EQ(OB_INNER_STAT_ERROR, not_init_array.push_back(1));
  int64_t temp = 0;
  ASSERT_EQ(OB_INNER_STAT_ERROR, not_init_array.pop_back(temp));
  ASSERT_EQ(OB_INNER_STAT_ERROR, not_init_array.remove(1));
  ASSERT_EQ(OB_INNER_STAT_ERROR, not_init_array.at(1, temp));
  ObArray<int64_t> temp_array;
  ASSERT_EQ(OB_INNER_STAT_ERROR, not_init_array.assign(temp_array));

  // test push
  const int64_t size = 10;
  int64_t buf[size];
  ObArrayHelper<int64_t> array(size, buf);
  for (int64_t i = 0; i < size; ++i) {
    ASSERT_EQ(OB_SUCCESS, array.push_back(i));
  }
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.push_back(size));
  ASSERT_EQ(size, array.count());
  ASSERT_EQ(size, array.capacity());
  ASSERT_EQ(size, array.count());
  ASSERT_EQ(buf, array.get_base_address());

  // test iter
  for (int64_t i = 0; i < array.count(); ++i) {
    int64_t value;
    ASSERT_EQ(OB_SUCCESS, array.at(i, value));
    ASSERT_EQ(i, value);
    ASSERT_EQ(i, array.at(i));
  }
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.at(10, temp));
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.at(-1, temp));

  // test remove
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.remove(10));
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.remove(-1));
  ASSERT_EQ(OB_SUCCESS, array.remove(9));
  ASSERT_EQ(OB_SUCCESS, array.remove(0));
  ASSERT_EQ(OB_SUCCESS, array.remove(4));
  ASSERT_EQ(7, array.count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(i + 1, array.at(i));
  }
  for (int64_t i = 4; i < array.count(); ++i) {
    ASSERT_EQ(i + 2, array.at(i));
  }
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, array.remove(0));
  }
  ASSERT_EQ(0, array.count());

  array.reuse();
  for (int64_t i = 0; i < 11; ++i) {
    ASSERT_EQ(OB_SUCCESS, temp_array.push_back(i));
  }
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.assign(temp_array));
  temp_array.pop_back();
  ASSERT_EQ(OB_SUCCESS, array.assign(temp_array));
  ASSERT_EQ(array.count(), temp_array.count());
  for (int64_t i = 0; i < temp_array.count(); ++i) {
    array.pop_back();
  }
  ASSERT_EQ(0, array.count());
  ASSERT_EQ(OB_SUCCESS, array.assign(temp_array));
  ASSERT_EQ(array.count(), temp_array.count());
  for (int64_t i = 0; i < temp_array.count(); ++i) {
    int64_t temp = 0;
    ASSERT_EQ(OB_SUCCESS, array.pop_back(temp));
    ASSERT_EQ(temp, 9 - i);
  }
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, array.pop_back(temp));
  ASSERT_EQ(0, array.count());
}
}//end namespace common
}//end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
