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
#define private public  // 获取私有成员
#include "share/index_usage/ob_index_usage_info_mgr.h"
#include "lib/utility/ob_test_util.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::storage;

class TestIndexUsageInfo: public ::testing::Test
{
public:
  TestIndexUsageInfo(): mgr_() {}
  virtual ~TestIndexUsageInfo() {}
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

    oceanbase::omt::ObTenantMeta tenant_meta;
    uint64_t tenant_id = 1888;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::construct_default_tenant_meta(tenant_id, tenant_meta));
    ASSERT_EQ(OB_SUCCESS, GCTX.omt_->create_tenant(tenant_meta, false));
  }
  static void TearDownTestCase()
  {
    uint64_t tenant_id = 1888;
    bool lock_succ = false;
    // ASSERT_EQ(OB_SUCCESS, GCTX.omt_->remove_tenant(tenant_id, lock_succ));
    while (OB_EAGAIN == GCTX.omt_->remove_tenant(tenant_id, lock_succ));

    MockTenantModuleEnv::get_instance().destroy();
  }
  void SetUp()
  {
    mgr_.is_enabled_ = true;
    mgr_.is_sample_mode_=false;
    ASSERT_EQ(OB_SUCCESS, mgr_.init(1888)); // mock user tenant id
    ASSERT_EQ(OB_SUCCESS, mgr_.start());
  }
  void TearDown()
  {
    mgr_.stop();
    mgr_.destroy();
  }
  int64_t check_size() {
    int64_t i = 0;
    int64_t total_size = 0;
    for (i = 0; i < mgr_.hashmap_count_; ++i) {
      total_size += mgr_.index_usage_map_[i].size();
    }
    return total_size;
  }
private:
  ObIndexUsageInfoMgr mgr_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestIndexUsageInfo);
};

TEST_F(TestIndexUsageInfo, test_init)
{
  ObIndexUsageInfoMgr mgr;
  ASSERT_FALSE(mgr.is_inited_);
  ASSERT_FALSE(mgr.report_task_.get_is_inited());
  ASSERT_FALSE(mgr.refresh_conf_task_.get_is_inited());

  ASSERT_EQ(nullptr, mgr.report_task_.get_mgr());
  ASSERT_EQ(nullptr, mgr.report_task_.get_sql_proxy());

  ASSERT_TRUE(mgr_.is_inited_);
  ASSERT_TRUE(mgr_.report_task_.get_is_inited());
  ASSERT_TRUE(mgr_.refresh_conf_task_.get_is_inited());
  ASSERT_EQ(&mgr_, mgr_.report_task_.get_mgr());
  ASSERT_EQ(&mgr_, mgr_.refresh_conf_task_.get_mgr());
  ASSERT_EQ(GCTX.sql_proxy_, mgr_.report_task_.get_sql_proxy());
}

TEST_F(TestIndexUsageInfo, test_update)
{
  int64_t mock_tenant_id = 1888;
  int64_t mock_index_id = 500002;

  ASSERT_TRUE(mgr_.is_inited_);
  ASSERT_EQ(0, check_size());
  mgr_.is_sample_mode_=false;
  mgr_.is_enabled_=true;
  mgr_.min_tenant_data_version_=DATA_VERSION_4_3_0_0;
  mgr_.update(mock_tenant_id, mock_index_id);
  ASSERT_EQ(1, check_size());
}

TEST_F(TestIndexUsageInfo, test_sample_filter)
{
  // about 10% sample ratio
  bool is_filter = true;
  int64_t count = 0;
  mgr_.is_sample_mode_=true;
  for (int64_t i = 0; i < 20; i++) {
    is_filter = mgr_.sample_filterd(i);
    if (!is_filter) {
      count++;
    }
  }
  ASSERT_TRUE(count < 20);
  mgr_.is_sample_mode_=false;
  count = 0;
  for (int64_t i = 0; i < 20; i++) {
    is_filter = mgr_.sample_filterd(i);
    if (!is_filter) {
      count++;
    }
  }
  ASSERT_TRUE(count == 20);
}

TEST_F(TestIndexUsageInfo, test_refresh_config)
{
  mgr_.is_enabled_ = false;
  mgr_.is_sample_mode_=false;
  mgr_.max_entries_=1000;
  mgr_.refresh_config();
  ASSERT_EQ(true, mgr_.is_enabled_);
  ASSERT_EQ(true, mgr_.is_sample_mode_);
  ASSERT_EQ(30000, mgr_.max_entries_);
}

TEST_F(TestIndexUsageInfo, test_destory)
{
  mgr_.destroy();
  ASSERT_FALSE(mgr_.is_inited_);
  ASSERT_FALSE(mgr_.report_task_.get_is_inited());
  ASSERT_FALSE(mgr_.refresh_conf_task_.get_is_inited());
  ASSERT_EQ(nullptr, mgr_.index_usage_map_);
  ASSERT_EQ(nullptr, mgr_.report_task_.get_sql_proxy());
  ASSERT_EQ(nullptr, mgr_.report_task_.get_mgr());
  ASSERT_EQ(nullptr, mgr_.refresh_conf_task_.get_mgr());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("TestIndexUsageInfo.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
