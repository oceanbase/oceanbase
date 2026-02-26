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

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

class TestAutoExpandDisk : public ::testing::Test
{
public:
TestAutoExpandDisk() = default;
  virtual ~TestAutoExpandDisk() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestAutoExpandDisk::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
}

void TestAutoExpandDisk::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestAutoExpandDisk, test_auto_expand_disk_task)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 17L * 1024L * 1024L * 1024L; // 17GB
  int64_t datafile_size = OB_SERVER_DISK_SPACE_MGR.get_total_disk_size(); // 20GB
  bool succ_resize = false;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size, succ_resize));
  omt::ObTenant *tenant = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.omt_->get_tenant(1, tenant));
  share::ObUnitInfoGetter::ObTenantConfig new_unit;
  ASSERT_EQ(OB_SUCCESS, new_unit.assign(tenant->get_unit()));
  new_unit.actual_data_disk_size_ = total_disk_size;
  ASSERT_EQ(OB_SUCCESS, GCTX.omt_->update_tenant_unit(new_unit));
  ASSERT_EQ(0, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_size());
  ASSERT_EQ(DataDiskSuggestedOperationType::TYPE::NONE, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_operation());
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());
  int64_t reserved_disk_size = OB_SERVER_DISK_SPACE_MGR.get_reserved_disk_size();
  ASSERT_EQ(datafile_size - total_disk_size - reserved_disk_size, OB_SERVER_DISK_SPACE_MGR.get_free_disk_size());

  // disable evict_task
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  macro_cache_mgr->evict_task_.is_inited_ = false;

  // 1.alloc TMP_FILE exceeds 90% of macro_cache_size
  ObSSMacroCacheStat cache_stat;
  const int64_t macro_cache_size = tenant_disk_space_mgr->get_macro_cache_size();
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  int64_t disk_size = macro_cache_size * 91 / 100;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                               ObDiskSpaceType::FILE));
  bool is_expand = false;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->need_expand_disk_size(is_expand));
  ASSERT_TRUE(is_expand);

  // 2.auto expand data_disk_size
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_DISK_SPACE_MGR.auto_expand_data_disk_size(MTL_ID(), ObDiskSpaceManager::DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH));
  total_disk_size += ObDiskSpaceManager::DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH;
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // 3.check expand data_disk_size success
  ASSERT_EQ(datafile_size - total_disk_size - reserved_disk_size, OB_SERVER_DISK_SPACE_MGR.get_free_disk_size());
  ASSERT_EQ(0, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_size());
  ASSERT_EQ(DataDiskSuggestedOperationType::TYPE::EXPAND, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_operation());

  // 4.check expand datafile_size
  int64_t new_datafile_size = 25L * 1024L * 1024L * 1024L; // 25GB
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_DISK_SPACE_MGR.resize(new_datafile_size));
  ASSERT_EQ(0, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_size());
  ASSERT_EQ(DataDiskSuggestedOperationType::TYPE::NONE, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_operation());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_auto_expand_disk.log*");
  OB_LOGGER.set_file_name("test_auto_expand_disk.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}