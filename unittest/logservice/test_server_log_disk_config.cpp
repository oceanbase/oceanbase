/**
 * Copyright (c) 2026 OceanBase
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
#define private public
#include "logservice/ob_server_log_block_mgr.h"
#include "rootserver/ob_root_service.h"
#undef private
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/ob_rpc_struct.h"
#include "share/config/ob_server_config.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace common
{

class TestServerLogDiskConfig : public ::testing::Test
{
public:
  void SetUp() override
  {
    original_log_block_mgr_ = GCTX.log_block_mgr_;
    original_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    original_enable_upgrade_mode_ = GCONF.enable_upgrade_mode;
    ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
    ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(1001));
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(1002));
  }

  void TearDown() override
  {
    GCTX.log_block_mgr_ = original_log_block_mgr_;
    ObClusterVersion::get_instance().update_cluster_version(original_cluster_version_);
    GCONF.enable_upgrade_mode = original_enable_upgrade_mode_;
    EventItem disable_errsim;
    ASSERT_EQ(OB_SUCCESS, EventTable::set_event(
        "ERRSIM_SKIP_SHARED_LOG_POOL_CONFIG_CHECK", disable_errsim));
    GCONF.server_log_disk_utilization_limit_threshold.set_value("95");
    GCONF.server_log_disk_utilization_threshold.set_value("0");
    for (const uint64_t tenant_id : {1001, 1002}) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      ASSERT_TRUE(tenant_config.is_valid());
      ASSERT_TRUE(tenant_config->log_retention_time.set_value("2h"));
    }
  }

private:
  logservice::ObServerLogBlockMgr *original_log_block_mgr_;
  uint64_t original_cluster_version_;
  bool original_enable_upgrade_mode_;
};

TEST_F(TestServerLogDiskConfig, validate_watermarks)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(1001));
  ASSERT_TRUE(tenant_config.is_valid());
  EXPECT_EQ(0, GCONF.server_log_disk_utilization_threshold.get_value());
  EXPECT_EQ(95, GCONF.server_log_disk_utilization_limit_threshold.get_value());
  EXPECT_EQ(2L * 60L * 60L * 1000L * 1000L, tenant_config->log_retention_time.get_value());
  EXPECT_TRUE(GCONF.server_log_disk_utilization_threshold.check());
  EXPECT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.check());

  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("-1"));
  EXPECT_FALSE(GCONF.server_log_disk_utilization_threshold.check());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("99"));
  EXPECT_TRUE(GCONF.server_log_disk_utilization_threshold.check());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("100"));
  EXPECT_FALSE(GCONF.server_log_disk_utilization_threshold.check());

  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("80"));
  EXPECT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.check());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("79"));
  EXPECT_FALSE(GCONF.server_log_disk_utilization_limit_threshold.check());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("0"));
  EXPECT_FALSE(GCONF.server_log_disk_utilization_limit_threshold.check());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("100"));
  EXPECT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.check());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("101"));
  EXPECT_FALSE(GCONF.server_log_disk_utilization_limit_threshold.check());

  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  EXPECT_TRUE(GCONF.server_log_disk_utilization_threshold.check());
  ASSERT_TRUE(tenant_config->log_retention_time.set_value("0s"));
  EXPECT_TRUE(tenant_config->log_retention_time.check());
  // User tenant 1002 and its meta tenant 1001 keep independent retention settings.
  omt::ObTenantConfigGuard user_config(TENANT_CONF(1002));
  ASSERT_TRUE(user_config.is_valid());
  EXPECT_EQ(2L * 60L * 60L * 1000L * 1000L, user_config->log_retention_time.get_value());
  ASSERT_TRUE(user_config->log_retention_time.set_value("4h"));
  EXPECT_TRUE(user_config->log_retention_time.check());
  EXPECT_EQ(0, tenant_config->log_retention_time.get_value());
  EXPECT_FALSE(user_config->log_retention_time.set_value("-1s")
               && user_config->log_retention_time.check());
}

TEST_F(TestServerLogDiskConfig, validate_shared_reclaim_config_change)
{
  logservice::ObServerLogBlockMgr block_mgr;
  rootserver::ObRootService root_service;
  GCTX.log_block_mgr_ = &block_mgr;
  obrpc::ObAdminSetConfigItem item;
  ASSERT_EQ(OB_SUCCESS, item.value_.assign("80"));

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_2_5_7);
  GCONF.enable_upgrade_mode = false;
  EXPECT_EQ(OB_OP_NOT_ALLOW, root_service.check_server_log_disk_utilization_threshold_(item));

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_2_5_8);
  GCONF.enable_upgrade_mode = true;
  EXPECT_EQ(OB_OP_NOT_ALLOW, root_service.check_server_log_disk_utilization_threshold_(item));
  GCONF.enable_upgrade_mode = false;
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_2_5_8 + 1);
  GCONF.enable_upgrade_mode = true;
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_2_5_7);
  GCONF.enable_upgrade_mode = true;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  ASSERT_EQ(OB_SUCCESS, item.value_.assign("85"));
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));
  // A nonzero current configuration remains supported regardless of upgrade mode.
  EXPECT_TRUE(block_mgr.is_shared_log_pool_supported());
  GCONF.enable_upgrade_mode = false;
  EXPECT_TRUE(block_mgr.is_shared_log_pool_supported());
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));
  GCONF.enable_upgrade_mode = true;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  ASSERT_EQ(OB_SUCCESS, item.value_.assign("0"));
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));
  EXPECT_FALSE(block_mgr.is_shared_log_pool_supported());

  // A previously committed V2 meta keeps first enable available after restart.
  block_mgr.has_v2_meta_ = true;
  ASSERT_EQ(OB_SUCCESS, item.value_.assign("80"));
  EXPECT_TRUE(block_mgr.is_shared_log_pool_supported());
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));
  ASSERT_EQ(OB_SUCCESS, item.value_.assign("invalid"));
  EXPECT_EQ(OB_INVALID_ARGUMENT, root_service.check_server_log_disk_utilization_threshold_(item));
  block_mgr.has_v2_meta_ = false;

  EventItem enable_errsim;
  enable_errsim.error_code_ = -1;
  enable_errsim.trigger_freq_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event(
      "ERRSIM_SKIP_SHARED_LOG_POOL_CONFIG_CHECK", enable_errsim));
  ASSERT_EQ(OB_SUCCESS, item.value_.assign("80"));
  EXPECT_EQ(OB_SUCCESS, root_service.check_server_log_disk_utilization_threshold_(item));
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
