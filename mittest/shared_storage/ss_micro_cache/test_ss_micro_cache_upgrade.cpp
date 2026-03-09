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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheUpgrade : public ::testing::Test
{
public:
  TestSSMicroCacheUpgrade() {}
  virtual ~TestSSMicroCacheUpgrade() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

private:
  ObSSMicroCache *micro_cache_;
};

void TestSSMicroCacheUpgrade::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheUpgrade::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheUpgrade::SetUp()
{
}

void TestSSMicroCacheUpgrade::TearDown()
{
}

TEST_F(TestSSMicroCacheUpgrade, test_upgrade_micro_cache_file)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_upgrade_micro_cache_file");
  const uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  ObTenantFileManager *tnt_file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_manager);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  // Expand tenant disk size to 16GB
  {
    const int64_t target_tenant_disk_size = 16LL * 1024 * 1024 * 1024; // 16GB

    int64_t required_server_disk_size = target_tenant_disk_size;
    if (is_sys_tenant(tenant_id)) {
      required_server_disk_size += OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }
    required_server_disk_size += 1024 * 1024 * 1024L; // 1GB buffer

    const int64_t current_server_disk_size = OB_SERVER_DISK_SPACE_MGR.get_total_disk_size();
    if (current_server_disk_size < required_server_disk_size) {
      ASSERT_EQ(OB_SUCCESS, OB_SERVER_DISK_SPACE_MGR.resize(required_server_disk_size));
    }

    // Update tenant unit config (required for expand_data_disk_size to work)
    omt::ObTenant *tenant = nullptr;
    ASSERT_NE(nullptr, GCTX.omt_);
    ASSERT_EQ(OB_SUCCESS, GCTX.omt_->get_tenant(tenant_id, tenant));
    ASSERT_NE(nullptr, tenant);
    share::ObUnitInfoGetter::ObTenantConfig new_unit;
    ASSERT_EQ(OB_SUCCESS, new_unit.assign(tenant->get_unit()));
    int64_t config_data_disk_size = target_tenant_disk_size;
    if (is_sys_tenant(tenant_id)) {
      config_data_disk_size -= OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }
    new_unit.config_.resource_.data_disk_size_ = config_data_disk_size;
    ASSERT_EQ(OB_SUCCESS, GCTX.omt_->update_tenant_unit(new_unit));

    // Expand tenant disk size
    bool succ_resize = false;
    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->resize_total_disk_size_(target_tenant_disk_size, succ_resize));
    ASSERT_EQ(true, succ_resize);
    ASSERT_EQ(target_tenant_disk_size, tnt_disk_space_mgr->get_total_disk_size());
  }

  const int64_t total_disk_size = tnt_disk_space_mgr->total_disk_size_;
  const int64_t large_pct = 20;
  const int64_t small_pct = 5;
  const int64_t large_micro_cache_size = total_disk_size * large_pct / 100;
  const int64_t small_micro_cache_size = MAX(total_disk_size * small_pct / 100, ObTenantDiskSpaceManager::DEF_MICRO_CACHE_MIN_START_SIZE);
  LOG_INFO("TEST_CASE: start test_upgrade_micro_cache_file", K(total_disk_size),
    "micro_cache_size", tnt_disk_space_mgr->micro_cache_file_size_,
    K(large_micro_cache_size), K(small_micro_cache_size));

  // 1. Adjust micro cache size to 20% of total disk size
  {
    if (tenant_config.is_valid()) {
      tenant_config->_ss_micro_cache_size_max_percentage = large_pct;
    }
    ASSERT_EQ(large_pct, ObTenantDiskSpaceManager::get_micro_cache_size_pct(tenant_id));

    const int64_t current_size = tnt_disk_space_mgr->micro_cache_file_size_;
    const int64_t delta_size = large_micro_cache_size - current_size;
    if (delta_size > 0) {
      {
        ObMutexGuard guard(tnt_disk_space_mgr->disk_size_lock_);
        ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->resize_micro_cache_file_nolock_(
            current_size, delta_size, true/*do_fallocate*/));
      }
      ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(
          tnt_disk_space_mgr->micro_cache_file_size_));
    }
    ASSERT_GE(tnt_disk_space_mgr->micro_cache_file_size_, large_micro_cache_size);
    ASSERT_GE(micro_cache->cache_file_size_, large_micro_cache_size);
  }
  LOG_INFO("TEST_CASE: ", "total_disk_size", tnt_disk_space_mgr->total_disk_size_,
    "micro_cache_size", tnt_disk_space_mgr->micro_cache_file_size_);

  // 2. Test basic functionality of micro cache
  {
    ObSSMicroCacheMicroStat &micro_stat = micro_cache->cache_stat_.micro_stat_;
    const int64_t micro_size = 12 * 1024;
    const int64_t micro_cnt = 10000;
    const uint64_t tablet_id = 100;

    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_add_micro_block(tablet_id, micro_cnt, micro_size, micro_size, add_cnt));
    ASSERT_GT(add_cnt, 0);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt, micro_size, micro_size, get_cnt));
    ASSERT_GT(get_cnt, 0);
    ASSERT_GT(micro_stat.total_micro_cnt_, 0);
    ASSERT_GT(micro_stat.valid_micro_cnt_, 0);
  }

  // 3. destroy disk_space_mgr and micro_cache
  {
    micro_cache->stop();
    tnt_disk_space_mgr->stop();
    tnt_file_manager->stop();
    micro_cache->wait();
    tnt_disk_space_mgr->wait();
    tnt_file_manager->wait();
    micro_cache->destroy();
    tnt_disk_space_mgr->destroy();
    tnt_file_manager->destroy();
    ASSERT_EQ(0, tnt_disk_space_mgr->total_disk_size_);
    ASSERT_EQ(0, tnt_disk_space_mgr->micro_cache_file_size_);
  }

  // 4. Adjust micro cache size pct to 5% and reinitialize
  {
    if (tenant_config.is_valid()) {
      tenant_config->_ss_micro_cache_size_max_percentage = small_pct;
    }

    int64_t data_disk_size = total_disk_size;
    if (is_sys_tenant(tenant_id)) {
      data_disk_size -= OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }

    ASSERT_EQ(OB_SUCCESS, tnt_file_manager->init(MTL_ID()));
    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->init(MTL_ID(), data_disk_size));
    const int64_t SS_STARTUP_CACHE_FILE_SIZE = 810 * 1024 * 1024L; // 810MB
    ASSERT_EQ(SS_STARTUP_CACHE_FILE_SIZE, tnt_disk_space_mgr->micro_cache_file_size_);

    ASSERT_EQ(OB_SUCCESS, tnt_file_manager->start());
    ASSERT_EQ(large_micro_cache_size, tnt_disk_space_mgr->micro_cache_file_size_);

    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->start());
    ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), tnt_disk_space_mgr->micro_cache_file_size_));
    ASSERT_EQ(OB_SUCCESS, micro_cache->start());

    ASSERT_EQ(small_pct, tenant_config->_ss_micro_cache_size_max_percentage);
    ASSERT_EQ(large_micro_cache_size, micro_cache->cache_file_size_);
    ASSERT_GT(large_micro_cache_size, small_micro_cache_size);
  }

  // 5. Test basic functionality of micro cache after restart
  {
    ObSSMicroCacheMicroStat &micro_stat = micro_cache->cache_stat_.micro_stat_;
    const int64_t micro_size = 12 * 1024;
    const int64_t micro_cnt = 1000;
    const uint64_t tablet_id = 200;

    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_add_micro_block(tablet_id, micro_cnt, micro_size, micro_size, add_cnt));
    ASSERT_GT(add_cnt, 0);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

    int64_t get_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_get_micro_block_meta(tablet_id, micro_cnt, micro_size, micro_size, get_cnt));
    ASSERT_GT(get_cnt, 0);
    ASSERT_GT(micro_stat.total_micro_cnt_, 0);
    ASSERT_GT(micro_stat.valid_micro_cnt_, 0);
  }
  LOG_INFO("TEST_CASE: finish test_upgrade_micro_cache_file");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_upgrade.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_upgrade.log", true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}