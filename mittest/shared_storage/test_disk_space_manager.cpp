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
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

class TestDiskSpaceManager : public ::testing::Test
{
public:
  TestDiskSpaceManager() = default;
  virtual ~TestDiskSpaceManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestDiskSpaceManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestDiskSpaceManager::TearDownTestCase()
{
    int ret = OB_SUCCESS;
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    }
    MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestDiskSpaceManager, macro_block_test)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L - ObDiskSpaceManager::DEFAULT_SERVER_TENANT_ID_DISK_SIZE; // 20GB - reserved_size
  bool succ_resize = false;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size, succ_resize));
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // 1.alloc disk_size
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, cache_stat));
  int64_t ori_used_size = cache_stat.used_;
  int64_t disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                               ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, cache_stat));
  ASSERT_EQ(ori_used_size+ disk_size, cache_stat.used_);
  disk_size = 100L * 1024L * 1024L * 1024L; // 100GB
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                                               ObDiskSpaceType::FILE));

  // 2.free disk_size
  disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, cache_stat));
  ASSERT_EQ(ori_used_size, cache_stat.used_);
  disk_size = 100L * 1024L * 1024L * 1024L; // 100GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, cache_stat));
  ASSERT_EQ(0, cache_stat.used_);

  // 3.resize tenant_disk_size
  disk_size = 20L * 1024L * 1024L * 1024L - ObDiskSpaceManager::DEFAULT_SERVER_TENANT_ID_DISK_SIZE; // 20GB
  succ_resize = false;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(disk_size, succ_resize));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_total_disk_size());
  disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  succ_resize = false;
  ASSERT_EQ(OB_NOT_SUPPORTED, tenant_disk_space_mgr->resize_total_disk_size(disk_size, succ_resize));
}

TEST_F(TestDiskSpaceManager, test_tmp_file_space_manager)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L - ObDiskSpaceManager::DEFAULT_SERVER_TENANT_ID_DISK_SIZE; // 20GB
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());
  // 1.alloc tmp_file_size
  int64_t disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                               ObDiskSpaceType::FILE));
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(disk_size, cache_stat.used_);

  // 2.free tmp_file_size
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(0, cache_stat.used_);
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(0, cache_stat.used_);

  // 3.alloc tmp_file_size fail
  disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                                               ObDiskSpaceType::FILE));

  // 4.alloc max_available_disk_size
  const int64_t macro_cache_free_size = tenant_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(macro_cache_free_size, ObSSMacroCacheType::TMP_FILE,
                                                               ObDiskSpaceType::FILE));
  disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                                               ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(macro_cache_free_size, ObSSMacroCacheType::TMP_FILE,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  // After freeing: used_ becomes 0 (scatter dirs were already cleared in step 2's over-release protection)
  ASSERT_EQ(0, cache_stat.used_);

  // 5.resize tenant_disk_size
  disk_size = 20L * 1024L * 1024L * 1024L - ObDiskSpaceManager::DEFAULT_SERVER_TENANT_ID_DISK_SIZE; // 20GB
  bool succ_resize = false;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(disk_size, succ_resize));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_total_disk_size());
}

TEST_F(TestDiskSpaceManager, test_meta_file_space_manager)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L - ObDiskSpaceManager::DEFAULT_SERVER_TENANT_ID_DISK_SIZE; // 20GB
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());
  // 1.alloc meta_file_size
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::META_FILE, cache_stat));
  int64_t ori_used_size = cache_stat.used_;
  int64_t disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::META_FILE,
                                                               ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::META_FILE, cache_stat));
  ASSERT_EQ(ori_used_size + disk_size, cache_stat.used_);

  // 2.free meta_file_size
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObSSMacroCacheType::META_FILE,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::META_FILE, cache_stat));
  ASSERT_EQ(ori_used_size, cache_stat.used_);

  // 3.alloc max_available_disk_size
  const int64_t macro_cache_free_size = tenant_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(macro_cache_free_size, ObSSMacroCacheType::META_FILE,
                                                               ObDiskSpaceType::FILE));
  disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::META_FILE,
                                                                               ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(macro_cache_free_size, ObSSMacroCacheType::META_FILE,
                                                              ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::META_FILE, cache_stat));
  ASSERT_EQ(0, cache_stat.used_);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_disk_space_manager.log*");
  OB_LOGGER.set_file_name("test_disk_space_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
