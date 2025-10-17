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
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "share/table/ob_ttl_util.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

class TestTTLUtil: public ::testing::Test
{
public:
  TestTTLUtil();
  virtual ~TestTTLUtil();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTTLUtil);
};

TestTTLUtil::TestTTLUtil()
{
}

TestTTLUtil::~TestTTLUtil()
{
}

void TestTTLUtil::SetUp()
{
}

void TestTTLUtil::TearDown()
{
}

TEST_F(TestTTLUtil, test_get_ttl_column)
{
  ObSEArray<ObString, 8> ttl_columns;
  // case 1: "c +  INTERVAL 40 MINUTE"
  ObString ttl_def("c +  INTERVAL 40 MINUTE");
  ASSERT_EQ(OB_SUCCESS, ObTTLUtil::get_ttl_columns(ttl_def, ttl_columns));
  ASSERT_EQ(1, ttl_columns.count());
  ASSERT_EQ(ObString::make_string("c"), ttl_columns.at(0));

  // case 2: "c +  INTERVAL 40 MINUTE, b +  INTERVAL 40 MINUTE"
  ttl_columns.reset();
  ObString ttl_def_2("c +  INTERVAL 40 MINUTE, b +  INTERVAL 40 MINUTE");
  ASSERT_EQ(OB_SUCCESS, ObTTLUtil::get_ttl_columns(ttl_def_2, ttl_columns));
  ASSERT_EQ(2, ttl_columns.count());
  ASSERT_EQ(ObString::make_string("c"), ttl_columns.at(0));
  ASSERT_EQ(ObString::make_string("b"), ttl_columns.at(1));

  // case 3: "INTERVAL 40 MINUTE"
  ttl_columns.reset();
  ObString ttl_def_3("INTERVAL 40 MINUTE");
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObTTLUtil::get_ttl_columns(ttl_def_3, ttl_columns));

  // case 4: "  INTERVAL 40 MINUTE"
  ttl_columns.reset();
  ObString ttl_def_4("  INTERVAL 40 MINUTE");
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObTTLUtil::get_ttl_columns(ttl_def_4, ttl_columns));
}

TEST_F(TestTTLUtil, test_kv_attribute_to_json)
{
int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObKVAttr kv_attr;
  ObString json_str;
  kv_attr.type_ = ObKVAttr::ObTTLTableType::HBASE;

  // Case 1: Both ttl and max_version are 0
  kv_attr.ttl_ = 0;
  kv_attr.max_version_ = 0;
  kv_attr.is_disable_ = false;
  ret = ObTTLUtil::format_kv_attributes_to_json_str(allocator, kv_attr, json_str);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ("{\"Hbase\": {\"State\": \"enable\"}}", json_str.ptr());

  // Case 2: Only ttl
  kv_attr.ttl_ = 86400;
  kv_attr.max_version_ = 0;
  ret = ObTTLUtil::format_kv_attributes_to_json_str(allocator, kv_attr, json_str);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ("{\"Hbase\": {\"TimeToLive\": 86400, \"State\": \"enable\"}}", json_str.ptr());

  // Case 3: Only max_version
  kv_attr.ttl_ = 0;
  kv_attr.max_version_ = 3;
  ret = ObTTLUtil::format_kv_attributes_to_json_str(allocator, kv_attr, json_str);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ("{\"Hbase\": {\"MaxVersions\": 3, \"State\": \"enable\"}}", json_str.ptr());

  // Case 4: Both ttl and max_version
  kv_attr.ttl_ = 86400;
  kv_attr.max_version_ = 3;
  ret = ObTTLUtil::format_kv_attributes_to_json_str(allocator, kv_attr, json_str);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ("{\"Hbase\": {\"TimeToLive\": 86400, \"MaxVersions\": 3, \"State\": \"enable\"}}", json_str.ptr());

  // Case 5: Disabled state
  kv_attr.is_disable_ = true;
  ret = ObTTLUtil::format_kv_attributes_to_json_str(allocator, kv_attr, json_str);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ("{\"Hbase\": {\"TimeToLive\": 86400, \"MaxVersions\": 3, \"State\": \"disable\"}}", json_str.ptr());

}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ttl_util.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}