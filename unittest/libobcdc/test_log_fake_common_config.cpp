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
 *
 * Test of ObLogFakeCommonConfig
 */

#include <gtest/gtest.h>
#include "ob_log_fake_common_config.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
class TestLogFakeCommonConfig : public ::testing::Test
{
public:
  TestLogFakeCommonConfig() {}
  ~TestLogFakeCommonConfig() {}

  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestLogFakeCommonConfig, common_test)
{
  ObLogFakeCommonConfig fake_config;
  EXPECT_EQ(OB_OBLOG, fake_config.get_server_type());
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
