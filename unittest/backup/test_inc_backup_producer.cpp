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

#include <cstdlib>

#include "gtest/gtest.h"

#define private public

#include "ob_inc_backup_producer.h"

using namespace oceanbase;
using namespace common;
using namespace tools;

namespace oceanbase
{
namespace unittest
{

class TestObIncBackUpProducer: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST(TestObIncBackUpProducer, confirm_tb_white_list)
{
  ObIncBackUpProducer producer;
  const int64_t tb_white_list_len = 1024;
  char tb_white_list[tb_white_list_len];

  const char *tenant_white_list0 = "*";
  EXPECT_EQ(OB_SUCCESS, producer.confirm_tb_white_list_(tenant_white_list0, tb_white_list, tb_white_list_len));
  EXPECT_STREQ("*.*.*", tb_white_list);

  const char *tenant_white_list1 = "tt1";
  EXPECT_EQ(OB_SUCCESS, producer.confirm_tb_white_list_(tenant_white_list1, tb_white_list, tb_white_list_len));
  EXPECT_STREQ("tt1.*.*", tb_white_list);

  const char *tenant_white_list2 = "tt1*";
  EXPECT_EQ(OB_SUCCESS, producer.confirm_tb_white_list_(tenant_white_list2, tb_white_list, tb_white_list_len));
  EXPECT_STREQ("tt1*.*.*", tb_white_list);

  const char *tenant_white_list3 = "tt1*|tt2*|tt3*";
  EXPECT_EQ(OB_SUCCESS, producer.confirm_tb_white_list_(tenant_white_list3, tb_white_list, tb_white_list_len));
  EXPECT_STREQ("tt1*.*.*|tt2*.*.*|tt3*.*.*", tb_white_list);

  const char *tenant_white_list4 = "tt1*|tt2|tt3|tt4*";
  EXPECT_EQ(OB_SUCCESS, producer.confirm_tb_white_list_(tenant_white_list4, tb_white_list, tb_white_list_len));
  EXPECT_STREQ("tt1*.*.*|tt2.*.*|tt3.*.*|tt4*.*.*", tb_white_list);

  const char *tenant_white_list5 = "tt1|tt2|tt3|tt4*";
  EXPECT_EQ(OB_SUCCESS, producer.confirm_tb_white_list_(tenant_white_list5, tb_white_list, tb_white_list_len));
  EXPECT_STREQ("tt1.*.*|tt2.*.*|tt3.*.*|tt4*.*.*", tb_white_list);
}

}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_inc_backup_producer.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  //oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
