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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "observer/ob_check_params.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace observer
{
class TestEndpointIngressService : public testing::Test
{
public:
  TestEndpointIngressService()
  {}
  virtual ~TestEndpointIngressService()
  {}
  virtual void SetUp(){};
  virtual void TearDown()
  {}
  virtual void TestBody()
  {}
};

TEST_F(TestEndpointIngressService, ingress_service)
{
  int ret = OB_SUCCESS;
  // bool strict_check = false;
  // strict_check = GCONF.strict_check_os_params;
  // LOG_WARN("", K(strict_check), K(GCONF.strict_check_os_params));
  // ret = check_os_params(strict_check);
  // CheckAllParams::check_all_params(strict_check);
  // int64_t value = 0;
  // const char* str1 = "/proc/sys/vm/max_map_count";
  // obj.read_one_int(str1 , value);
  // EXPECT_EQ(value, 65536);
  // bool res1 = obj.is_path_valid(str1);
  // EXPECT_EQ(res1, true);
  // bool res2 = obj.is_path_valid("/proc/1/sys/vm/max_map_count");
  // EXPECT_EQ(res2, false);
  // obj.check_all_params(false);
}
}  // namespace rootserver
}  // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#undef private