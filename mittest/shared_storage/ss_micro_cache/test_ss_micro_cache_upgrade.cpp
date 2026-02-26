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
  const int64_t total_disk_size = tnt_disk_space_mgr->total_disk_size_;
  const int64_t large_pct = 20;
  const int64_t small_pct = 5;
  const int64_t large_micro_cache_size = total_disk_size * large_pct / 100;
  const int64_t small_micro_cache_size = MAX(total_disk_size * small_pct / 100, ObTenantDiskSpaceManager::DEF_MICRO_CACHE_MIN_START_SIZE);
  const int64_t TIMEOUT_S = 10;
  LOG_INFO("TEST_CASE: start test_upgrade_micro_cache_file", K(total_disk_size), K(large_micro_cache_size), K(small_micro_cache_size));

  // 1. adjust micro cache size pct to 20%
  {
    if (tenant_config.is_valid()) {
      tenant_config->_ss_micro_cache_size_max_percentage = large_pct;
    }

    bool is_change = false;
    const int64_t start_time_s = ObTimeUtility::current_time_s();
    do {
      const int64_t micro_cache_size = tnt_disk_space_mgr->micro_cache_file_size_;
      if (micro_cache_size == large_micro_cache_size) {
        is_change = true;
      }
      usleep(1000);
    } while ((!is_change) && (ObTimeUtility::current_time_s() - start_time_s < TIMEOUT_S));
    ASSERT_EQ(true, is_change);
  }
  LOG_INFO("TEST_CASE: ", "total_disk_size", tnt_disk_space_mgr->total_disk_size_,
    "micro_cache_size", tnt_disk_space_mgr->micro_cache_file_size_);

  // 2. test basic functionality of micro cache
  {
    ObSSMicroCacheMicroStat &micro_stat = micro_cache->cache_stat_.micro_stat_;
    const int64_t micro_size = 12 * 1024;
    const int64_t micro_cnt = 10000;

    // 2.1 add some data into micro_cache
    const uint64_t tablet_id = 100;
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_add_micro_block(tablet_id, micro_cnt, micro_size, micro_size, add_cnt));
    ASSERT_GT(add_cnt, 0);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

    // 2.2 get micro meta
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

  // 4. adjust micro cache size pct to 5%, and reinitialize disk_space_mgr and micro_cache
  {
    if (tenant_config.is_valid()) {
      tenant_config->_ss_micro_cache_size_max_percentage = small_pct;
    }

    // Note: total_disk_size recorded before already includes hidden_sys_data_disk_config_size
    // for sys_tenant. But init() will add it again, so we need to subtract it first.
    int64_t data_disk_size = total_disk_size;
    if (is_sys_tenant(tenant_id)) {
      data_disk_size -= OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }

    // Step 1: init tnt_file_manager and tnt_disk_space_mgr
    // At this point, micro_cache_file_size_ is calculated based on current config (5%)
    ASSERT_EQ(OB_SUCCESS, tnt_file_manager->init(MTL_ID()));
    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->init(MTL_ID(), data_disk_size));
    ASSERT_EQ(small_micro_cache_size, tnt_disk_space_mgr->micro_cache_file_size_);
    ASSERT_LT(small_micro_cache_size, large_micro_cache_size);

    // Step 2: start tnt_file_manager first
    // This will check if micro cache file exists and update micro_cache_file_size_ to actual file size
    ASSERT_EQ(OB_SUCCESS, tnt_file_manager->start());
    // After start, micro_cache_file_size_ should be updated to actual file size (large_micro_cache_size)
    ASSERT_EQ(large_micro_cache_size, tnt_disk_space_mgr->micro_cache_file_size_);

    // Step 3: start tnt_disk_space_mgr
    ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->start());

    // Step 4: init and start micro_cache with updated micro_cache_file_size_
    ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), tnt_disk_space_mgr->micro_cache_file_size_));
    ASSERT_EQ(OB_SUCCESS, micro_cache->start());

    LOG_INFO("TEST_CASE: ", "total_disk_size", tnt_disk_space_mgr->total_disk_size_,
      "micro_cache_size", tnt_disk_space_mgr->micro_cache_file_size_);
    // verify: current config is 5%, but current cache_file_size is large_micro_cache_size (from existing file)
    ASSERT_EQ(small_pct, tenant_config->_ss_micro_cache_size_max_percentage);
    ASSERT_EQ(large_micro_cache_size, micro_cache->cache_file_size_);
  }

  // 5. test basic functionality of micro cache
  {
    ObSSMicroCacheMicroStat &micro_stat = micro_cache->cache_stat_.micro_stat_;
    const int64_t micro_size = 12 * 1024;
    const int64_t micro_cnt = 1000;

    // 5.1 add some data into micro_cache
    const uint64_t tablet_id = 200;
    int64_t add_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::batch_add_micro_block(tablet_id, micro_cnt, micro_size, micro_size, add_cnt));
    ASSERT_GT(add_cnt, 0);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

    // 5.2 get micro meta
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