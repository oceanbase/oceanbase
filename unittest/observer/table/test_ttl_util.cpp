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
#include "lib/container/ob_se_array.h"

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

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ttl_util.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}