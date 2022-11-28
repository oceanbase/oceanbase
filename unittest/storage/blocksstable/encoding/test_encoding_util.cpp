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

#include<gtest/gtest.h>
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

static constexpr int test_src_array[2][3] = {{1, 2, 3}, {4, 5, 6}};
static int test_dst_array[2][3];

template <int A, int B>
struct TestArrayInit
{
  bool operator()()
  {
    test_dst_array[A][B] = test_src_array[A][B];
    return true;
  }
};

bool test_dst_array_init = ObNDArrayIniter<TestArrayInit, 2, 3>::apply();

static int test_product = Multiply_T<2, 3>::value_;

TEST(ObMultiDimArray_T, multi_dimension_array)
{
  ASSERT_EQ(6, test_dst_array[1][2]);

  ObMultiDimArray_T<int64_t, 3>::ArrType array_1d = {1, 2, 3};
  ASSERT_EQ(1, array_1d[0]);
  ASSERT_EQ(3, array_1d[2]);

  ObMultiDimArray_T<int64_t, 2, 4>array_2d{{{1, 2, 3, 4}, {5, 6, 7, 8}}};
  ASSERT_EQ(1, array_2d[0][0]);
  ASSERT_EQ(8, array_2d[1][3]);

  ASSERT_EQ(6, test_product);
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}