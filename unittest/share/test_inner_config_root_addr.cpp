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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include "share/ob_inner_config_root_addr.h"
#include "mock_mysql_proxy.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using ::testing::Return;
using testing::_;

class TestInnerConfigRootAddr : public ::testing::Test
{
public:
  TestInnerConfigRootAddr();
  ~TestInnerConfigRootAddr();
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  ObServerConfig &config_;
  MockMySQLProxy sql_proxy_;
  ObInnerConfigRootAddr ic_;
};
TestInnerConfigRootAddr::TestInnerConfigRootAddr() : config_(ObServerConfig::get_instance()) {}
TestInnerConfigRootAddr::~TestInnerConfigRootAddr() {}

void TestInnerConfigRootAddr::SetUp()
{
  int ret = ic_.init(sql_proxy_, config_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestInnerConfigRootAddr, fetch)
{

  ObSEArray<ObRootAddr, 16> rs_list;
  ObSEArray<ObRootAddr, 16> readonly_rs_list;

  ObInnerConfigRootAddr ic;
  ASSERT_NE(OB_SUCCESS, ic.fetch(rs_list, readonly_rs_list));

  config_.rootservice_list.set_value("0.0.0.0");
  ASSERT_NE(OB_SUCCESS, ic_.fetch(rs_list, readonly_rs_list));
  config_.rootservice_list.set_value("10.125.224.12:888");
  ASSERT_EQ(OB_SUCCESS, ic_.fetch(rs_list, readonly_rs_list));
  config_.rootservice_list.set_value("10.125.224.12:888:");
  ASSERT_EQ(OB_SUCCESS, ic_.fetch(rs_list, readonly_rs_list));
  config_.rootservice_list.set_value("10.125.224.12:888:999");
  ASSERT_EQ(OB_SUCCESS, ic_.fetch(rs_list, readonly_rs_list));

  config_.rootservice_list.set_value("127.0.0.1:555:666;127.0.0.2:777:888");
  ASSERT_EQ(2, config_.rootservice_list.size());
  ASSERT_EQ(OB_SUCCESS, ic_.fetch(rs_list, readonly_rs_list));
  ASSERT_EQ(2, rs_list.count());
  ObAddr rs1(ObAddr::IPV4, "127.0.0.1", 555);
  ASSERT_EQ(rs1, rs_list.at(0).server_);
  ASSERT_EQ(666, rs_list.at(0).sql_port_);
  ObAddr rs2(ObAddr::IPV4, "127.0.0.2", 777);
  ASSERT_EQ(rs2, rs_list.at(1).server_);
  ASSERT_EQ(888, rs_list.at(1).sql_port_);
}

TEST_F(TestInnerConfigRootAddr, store)
{
  ObSEArray<ObRootAddr, 16> rs_list;
  ObSEArray<ObRootAddr, 16> readonly_rs_list;
  ObInnerConfigRootAddr ic;
  ASSERT_NE(OB_SUCCESS, ic.store(rs_list, readonly_rs_list, true));
  ASSERT_NE(OB_SUCCESS, ic_.store(rs_list, readonly_rs_list, true));

  config_.rootservice_list.set_value("127.0.0.1:555:555;127.0.0.2:555:555");
  int ret = ic_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);


  config_.rootservice_list.set_value("127.0.0.2:555:555;127.0.0.1:555:555");
  ret = ic_.store(rs_list, readonly_rs_list, true);
  ASSERT_EQ(OB_SUCCESS, ret);


  EXPECT_CALL(sql_proxy_, write(_, _, _))
      .WillOnce(Return(OB_SUCCESS));
  config_.rootservice_list.set_value("127.0.0.3:555:555;127.0.0.1:555:555");
  ret = ic_.store(rs_list, readonly_rs_list, true);
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
