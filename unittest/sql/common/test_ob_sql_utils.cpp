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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase
{
using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace test 
{
class ObSQLUtilsTest: public ::testing::Test
{
  public:
	  ObSQLUtilsTest(){}
    virtual ~ObSQLUtilsTest(){}
    virtual void SetUp(){}
    virtual void TearDown(){}
  private:
    // disallow copy
    ObSQLUtilsTest(const ObSQLUtilsTest &other);
    ObSQLUtilsTest& operator=(const ObSQLUtilsTest &other);
  private:
    // data members
};
TEST_F(ObSQLUtilsTest, make_field_name)
{
  ObString name;
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSQLUtils::make_field_name(NULL, -1, CS_TYPE_UTF8MB4_GENERAL_CI, NULL, name));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSQLUtils::make_field_name(NULL, 0, CS_TYPE_UTF8MB4_GENERAL_CI, NULL, name));
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(NULL, 0, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_TRUE(name.empty());
  const char *buf1 = "adf";
  int64_t len1 = 3;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string("adf"));
  ASSERT_EQ(name.length(), 3);
  
  buf1 = "  adf";
  len1 = 5;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string("adf"));
  ASSERT_EQ(name.length(), 3);
  
  buf1 = "\t\r\naaa";
  len1 = 6;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string("aaa"));
  ASSERT_EQ(name.length(), 3);

  buf1 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";
  len1 = 255;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string(buf1));
	printf("prt:%s, len:%d\n", name.ptr(), name.length());
  ASSERT_EQ(name.length(), 255);
  
  const char *buf2 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111122222";
  int64_t len2 = 261;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf2, len2, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string(buf1));
  ASSERT_EQ(name.length(), 255);

  buf2 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111222222";
  len2 = 272;
  const char *buf3 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111222";
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf2, len2, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string(buf3));
  ASSERT_EQ(name.length(), 255);
	printf("prt:%s, len:%d\n", name.ptr(), name.length());

}

}
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
