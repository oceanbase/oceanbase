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

#include "tools/ob_error/src/ob_error.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace unittest {
class TestObError : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

TEST_F(TestObError, test_mgr)
{
  ObErrorInfoMgr mgr;

  EXPECT_FALSE(mgr.insert_os_error(nullptr, "abc", 1));
  EXPECT_FALSE(mgr.insert_os_error("abc", nullptr, 1));
  EXPECT_FALSE(mgr.insert_mysql_error(nullptr, "abc", 1, "abc", "abc", "abc", 1));
  EXPECT_FALSE(mgr.insert_mysql_error("abc", nullptr, 1, "abc", "abc", "abc", 1));
  EXPECT_FALSE(mgr.insert_mysql_error("abc", "abc", 1, nullptr, "abc", "abc", 1));
  EXPECT_FALSE(mgr.insert_mysql_error("abc", "abc", 1, "abc", nullptr, "abc", 1));
  EXPECT_FALSE(mgr.insert_mysql_error("abc", "abc", 1, "abc", "abc", nullptr, 1));
  EXPECT_FALSE(mgr.insert_oracle_error(nullptr, "abc", "abc", "abc", ORA, 1, 1));
  EXPECT_FALSE(mgr.insert_oracle_error("abc", nullptr, "abc", "abc", ORA, 1, 1));
  EXPECT_FALSE(mgr.insert_oracle_error("abc", "abc", nullptr, "abc", ORA, 1, 1));
  EXPECT_FALSE(mgr.insert_oracle_error("abc", "abc", "abc", nullptr, ORA, 1, 1));
  EXPECT_FALSE(mgr.insert_oracle_error("abc", "abc", "abc", "abc", NONE, 1, 1));
  EXPECT_FALSE(mgr.insert_ob_error(nullptr, "abc", "abc", "abc", 1));
  EXPECT_FALSE(mgr.insert_ob_error("abc", nullptr, "abc", "abc", 1));
  EXPECT_FALSE(mgr.insert_ob_error("abc", "abc", nullptr, "abc", 1));
  EXPECT_FALSE(mgr.insert_ob_error("abc", "abc", "abc", nullptr, 1));
}

TEST_F(TestObError, test_adder)
{
  ObErrorInfoMgr mgr;
  EXPECT_TRUE(init_global_info());
  // test_add_os_info
  EXPECT_FALSE(add_os_info(-1, &mgr));
  EXPECT_FALSE(add_os_info(OS_MAX_ERROR_CODE, &mgr));

  EXPECT_TRUE(add_os_info(1, nullptr));
  EXPECT_TRUE(add_os_info(100, &mgr));
  EXPECT_TRUE(add_os_info(0, &mgr));

  // test_add_oracle_info
  EXPECT_FALSE(add_oracle_info(NONE, -1, -1, &mgr));
  EXPECT_FALSE(add_oracle_info(ORA, 100, -1, &mgr));
  EXPECT_FALSE(add_oracle_info(PLS, 100, -1, &mgr));
  EXPECT_FALSE(add_oracle_info(ORA, 600, 3000, &mgr));
  EXPECT_FALSE(add_oracle_info(MY, 600, 3000, &mgr));
  EXPECT_FALSE(add_oracle_info(NONE, 100, -1, &mgr));

  EXPECT_TRUE(add_oracle_info(ORA, 600, -1, nullptr));
  EXPECT_TRUE(add_oracle_info(ORA, 600, -1, &mgr));
  EXPECT_TRUE(add_oracle_info(ORA, 600, 4000, &mgr));
  EXPECT_TRUE(add_oracle_info(ORA, 100, 4000, &mgr));
  EXPECT_TRUE(add_oracle_info(PLS, 100, 4000, &mgr));
  EXPECT_TRUE(add_oracle_info(NONE, 600, 3000, &mgr));

  // test_add_mysql_info
  EXPECT_FALSE(add_mysql_info(-1, &mgr));
  EXPECT_FALSE(add_mysql_info(OB_MAX_ERROR_CODE, &mgr));
  EXPECT_FALSE(add_mysql_info(1000, &mgr));
  EXPECT_FALSE(add_mysql_info(6300, &mgr));

  EXPECT_TRUE(add_mysql_info(1017, &mgr));

  // test_add_ob_info
  EXPECT_FALSE(add_ob_info(-1, &mgr));
  EXPECT_FALSE(add_ob_info(OB_MAX_ERROR_CODE, &mgr));
  EXPECT_FALSE(add_ob_info(1000, &mgr));
  EXPECT_FALSE(add_ob_info(6500, &mgr));

  EXPECT_TRUE(add_ob_info(4100, &mgr));
}

TEST_F(TestObError, test_parser)
{
  // test_parse_error_code
  int error_code = -1;
  parse_error_code("abc123", error_code);
  EXPECT_EQ(-1, error_code);
  parse_error_code(nullptr, error_code);
  EXPECT_EQ(-1, error_code);
  parse_error_code("9999999999999", error_code);  // overflow
  EXPECT_EQ(-1, error_code);
  parse_error_code("123", error_code);
  EXPECT_EQ(123, error_code);

  // test_parse_facility
  Fac facility = NONE;
  parse_facility("ABC", facility);
  EXPECT_EQ(NONE, facility);

  facility = NONE;
  parse_facility("ORa", facility);
  EXPECT_EQ(ORA, facility);

  facility = NONE;
  parse_facility("mY", facility);
  EXPECT_EQ(MY, facility);
}
}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}