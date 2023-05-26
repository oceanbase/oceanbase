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
#include "lib/stat/ob_session_stat.h"
#include <gtest/gtest.h>
#include "schema/db_initializer.h"


namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestAllClusterProxy : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  DBInitializer db_initer_;
};

void TestAllClusterProxy::SetUp()
{
  int ret = db_initer_.init();

  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);
}
TEST_F(TestAllClusterProxy, update_and_load)
{
  int ret = OB_SUCCESS;
  ObServerConfig &config = ObServerConfig::get_instance();
  GCTX.config_ = &config;
  config.cluster_id = 0;
  ObClusterInfoProxy proxy;
  ObClusterInfo info;
  info.cluster_id_ = 0;
  info.cluster_role_ = PRIMARY_CLUSTER;
  proxy.update(db_initer_.get_sql_proxy(), info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObClusterInfo dest_info;
  ASSERT_EQ(OB_SUCCESS, proxy.load(db_initer_.get_sql_proxy(), dest_info));
  ASSERT_EQ(LEADER, dest_info.cluster_role_);
  ASSERT_EQ(true, dest_info.login_name_.is_empty());
  ASSERT_EQ(true, dest_info.login_passwd_.is_empty());

  info.reset();
  dest_info.reset();
  info.cluster_id_ = 0;
  info.cluster_role_ = STANDBY_CLUSTER;
  info.login_name_.assign("lili");
  info.login_passwd_.assign("hello");
  proxy.update(db_initer_.get_sql_proxy(), info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, proxy.load(db_initer_.get_sql_proxy(), dest_info));
  ASSERT_EQ(0, info != dest_info);
  ASSERT_EQ(dest_info.login_passwd_, info.login_passwd_);

}
TEST_F(TestAllClusterProxy, set_redo_option)
{
  int ret = OB_SUCCESS;
  ObClusterRedoOption redo_option;
  ret = redo_option.construct_redo_option(ObString::make_string("async"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, redo_option.is_sync_);
  ret = redo_option.construct_redo_option(ObString::make_string("sync"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, redo_option.is_sync_);
  ret = redo_option.construct_redo_option(ObString::make_string("sync    net_timeout =    10"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, redo_option.is_sync_);
  ASSERT_EQ(10, redo_option.net_timeout_);
  ret = redo_option.construct_redo_option(ObString::make_string("    sync  = 1   net_timeout =    10"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = redo_option.construct_redo_option(ObString::make_string("reopen = 10 sync    net_timeout =    100000"));
  ASSERT_EQ(true, redo_option.is_sync_);
  ASSERT_EQ(100000, redo_option.net_timeout_);
  ASSERT_EQ(10, redo_option.reopen_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = redo_option.construct_redo_option(ObString::make_string("reopen = 0 max_failure = 11 sync    net_timeout =    10"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, redo_option.is_sync_);
  ASSERT_EQ(10, redo_option.net_timeout_);
  ASSERT_EQ(0, redo_option.reopen_);
  ASSERT_EQ(11, redo_option.max_failure_);
  ret = redo_option.construct_redo_option(ObString::make_string("reopen = 0 max_failure=11   net_timeout =    10"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, redo_option.is_sync_);
  ASSERT_EQ(10, redo_option.net_timeout_);
  ASSERT_EQ(0, redo_option.reopen_);
  ASSERT_EQ(11, redo_option.max_failure_);

  ret = redo_option.construct_redo_option(ObString::make_string("reopen = 0 max_failure=11   net_timeout =   async"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, redo_option.net_timeout_);
  ret = redo_option.construct_redo_option(ObString::make_string("    synnet_timeout =    10"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = redo_option.construct_redo_option(ObString::make_string("    sync  async net_timeout =    10"));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, redo_option.is_sync_);
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
