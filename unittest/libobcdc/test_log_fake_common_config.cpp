/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
