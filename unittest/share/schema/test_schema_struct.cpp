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
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
static const int64_t BUF_SIZE = 1024*10;
TEST(ObSchemaStructTest, hash_map)
{
  ObArenaAllocator allocator;
  ColumnHashMap map(allocator);
  int32_t value = 0;
  int ret = map.get(1, value);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = map.set(1, 20);
  ASSERT_EQ(OB_NOT_INIT, ret);

  int64_t bucket_num = 100;
  ret = map.init(bucket_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = map.init(bucket_num);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  ret = map.get(1, value);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);
  ret = map.set(1, 20);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = map.set(1, 30);
  ASSERT_EQ(OB_HASH_EXIST, ret);
  ret = map.get(1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(20, value);

  for (int64_t i = 1; i < 1000; ++i) {
    ret = map.set(i+1, 88);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = map.get(i+1, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(88, value);
  }
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
//  OB_LOGGER.set_file_name("test_schema.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
