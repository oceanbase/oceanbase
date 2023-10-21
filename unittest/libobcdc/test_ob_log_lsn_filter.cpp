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

 #include "gtest/gtest.h"
 #include "ob_log_lsn_filter.h"


using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
class TestObLogLsnFilter : public ::testing::Test
{
public:
  TestObLogLsnFilter() {}
  ~TestObLogLsnFilter() {}

  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObLogLsnFilter, basic)
{
  ObLogLsnFilter lsn_filter1;
  EXPECT_EQ(OB_SUCCESS, lsn_filter1.init("1002.1001.1"));
  EXPECT_EQ(true, lsn_filter1.filter(1002, 1001, 1));
  EXPECT_EQ(false, lsn_filter1.filter(1001, 1001, 2));

  ObLogLsnFilter lsn_filter2;
  EXPECT_EQ(OB_SUCCESS, lsn_filter2.init("1002.1001.4|1004.1001.5"));
  EXPECT_EQ(true, lsn_filter2.filter(1002, 1001, 4));
  EXPECT_EQ(true, lsn_filter2.filter(1004, 1001, 5));
  EXPECT_EQ(false, lsn_filter2.filter(1005, 1, 2));

  ObLogLsnFilter lsn_filter3;
  EXPECT_EQ(OB_INVALID_DATA, lsn_filter3.init("xx.3.2"));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_filter3.init("1002.1001.4;1004.1001.5"));

  ObLogLsnFilter lsn_filter4;
  EXPECT_EQ(OB_SUCCESS, lsn_filter4.init("|"));
  EXPECT_EQ(false, lsn_filter4.filter(1,1,1));
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}