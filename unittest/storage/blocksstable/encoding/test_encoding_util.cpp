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
#define protected public
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"
#include "lib/timezone/ob_timezone_info.h"

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

TEST(ObMultiDimArray_T, timestamp_with_time_zone)
{
  // for timestamp with time zone, we need use binary_equal to build encoding hash table.
  ObEncodingHashTableBuilder hash_builder;
  ASSERT_EQ(OB_SUCCESS, hash_builder.create(8, 8));
  ObColDatums time_arr;
  ObStorageDatum t1, t2;

  ObTimeZoneInfo time_zone_info1, time_zone_info2;
  time_zone_info1.set_timezone("+00:00");
  time_zone_info2.set_timezone("+8:00");
  ObTimeConvertCtx time_convert_ctx1(&time_zone_info1,
                                      ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT,
                                      true);
  ObTimeConvertCtx time_convert_ctx2(&time_zone_info2,
                                      ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT,
                                      true);
  ObOTimestampData otime_data1, otime_data2;
  ObScale scale;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 00:37:25",
                                                          time_convert_ctx1,
                                                          ObTimestampTZType,
                                                          otime_data1,
                                                          scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 08:37:25",
                                                          time_convert_ctx2,
                                                          ObTimestampTZType,
                                                          otime_data2,
                                                          scale));
  ObObj obj1, obj2;
  obj1.set_timestamp_tz(otime_data1);
  obj2.set_timestamp_tz(otime_data2);
  t1.from_obj(obj1);
  t2.from_obj(obj2);
  ASSERT_EQ(OB_SUCCESS, time_arr.push_back(t1));
  ASSERT_EQ(OB_SUCCESS, time_arr.push_back(t2));

  ObColDesc col_desc;
  col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
  col_desc.col_type_.set_type(ObObjType::ObTimestampTZType);
  col_desc.col_type_.set_collation_type(CS_TYPE_BINARY);

  ASSERT_EQ(OB_SUCCESS, hash_builder.build(time_arr, col_desc));
  ASSERT_EQ(2, hash_builder.list_cnt_);
}


}
}

int main(int argc, char **argv)
{
  system("rm -f test_encoding_util.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_encoding_util.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}