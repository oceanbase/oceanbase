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

TEST_F(TestDiskSpaceManager, basic_test)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size));

  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // 1.alloc disk_size
  int64_t disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::PRIVATE_DATA_MACRO, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_private_macro_alloc_size());
  disk_size = 100L * 1024L * 1024L * 1024L; // 100GB
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::PRIVATE_DATA_MACRO, false/*is_tmp_file_read_cache*/));

  // 2.free disk_size
  disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObStorageObjectType::PRIVATE_DATA_MACRO, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(0, tenant_disk_space_mgr->get_private_macro_alloc_size());
  disk_size = 100L * 1024L * 1024L * 1024L; // 100GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObStorageObjectType::PRIVATE_DATA_MACRO, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(0, tenant_disk_space_mgr->get_private_macro_alloc_size());

  // 3.resize tenant_disk_ size
  disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(disk_size));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_total_disk_size());
  disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  ASSERT_EQ(OB_NOT_SUPPORTED, tenant_disk_space_mgr->resize_total_disk_size(disk_size));
}

TEST_F(TestDiskSpaceManager, test_tmp_file_space_manager)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());
  // 1.alloc tmp_file_size
  int64_t tmp_file_write_cache_reserved_size = 1024L * 1024L * 1024L; // 1GB
  // ASSERT_EQ(tmp_file_write_cache_reserved_size, tenant_disk_space_mgr->get_tmp_file_write_cache_reserved_size());
  int64_t preread_cache_reserved_size = 2L * 1024L * 1024L * 1024L; // 2GB
  // ASSERT_EQ(preread_cache_reserved_size, tenant_disk_space_mgr->get_preread_cache_reserved_size());

  int64_t disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::TMP_FILE, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_tmp_file_write_cache_alloc_size());
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::TMP_FILE, true/*is_tmp_file_read_cache*/));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_tmp_file_read_cache_alloc_size());
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_major_macro_read_cache_alloc_size());
  const int64_t tmp_file_reserved_disk_size = tenant_disk_space_mgr->get_tmp_file_write_cache_reserved_size();
  int64_t tmp_file_free_disk_size = tenant_disk_space_mgr->get_tmp_file_write_cache_reserved_size() +
                                    tenant_disk_space_mgr->get_preread_cache_reserved_size() -
                                    tenant_disk_space_mgr->get_reserved_disk_size(tmp_file_reserved_disk_size) - 3 * disk_size;
  ASSERT_EQ(tmp_file_free_disk_size, tenant_disk_space_mgr->get_tmp_file_write_free_disk_size());
  tmp_file_free_disk_size = tenant_disk_space_mgr->get_preread_cache_reserved_size() - 2 * disk_size;
  ASSERT_EQ(tmp_file_free_disk_size, tenant_disk_space_mgr->get_preread_free_disk_size());

  // 2.free tmp_file_size
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObStorageObjectType::TMP_FILE, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(0, tenant_disk_space_mgr->get_tmp_file_write_cache_alloc_size());
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObStorageObjectType::TMP_FILE, true/*is_tmp_file_read_cache*/));
  ASSERT_EQ(0, tenant_disk_space_mgr->get_tmp_file_read_cache_alloc_size());

  // 3.alloc tmp_file_size fail
  disk_size = 2L * 1024L * 1024L * 1024L; // 2GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::TMP_FILE, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::TMP_FILE, true/*is_tmp_file_read_cache*/));

  // 4.alloc max_available_disk_size
  disk_size = tenant_disk_space_mgr->get_tmp_file_write_free_disk_size();
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::TMP_FILE, false/*is_tmp_file_read_cache*/));
  disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::TMP_FILE, false/*is_tmp_file_read_cache*/));

  // 5.resize tenant_disk_size
  disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(disk_size));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_total_disk_size());
  disk_size = 1024L * 1024L * 1024L; // 1GB
  // ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_tmp_file_write_cache_reserved_size());
  disk_size = 2L * 1024L * 1024L * 1024L; // 2GB
  // ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_preread_cache_reserved_size());
}

TEST_F(TestDiskSpaceManager, test_meta_file_space_manager)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());
  // 1.alloc meta_file_size
  int64_t disk_size = 1L * 1024L * 1024L; // 1MB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::PRIVATE_TABLET_META, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_meta_file_alloc_size());
  const int64_t meta_file_reserved_disk_size = tenant_disk_space_mgr->get_meta_reserved_disk_size();
  int64_t meta_file_free_disk_size = meta_file_reserved_disk_size - tenant_disk_space_mgr->get_reserved_disk_size(meta_file_reserved_disk_size) - disk_size;
  ASSERT_EQ(meta_file_free_disk_size, tenant_disk_space_mgr->get_meta_file_free_disk_size());
  // alloc_file_size PRIVATE_TABLET_CURRENT_VERSION do nothing, because create tablet_id dir has been alloced size
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(meta_file_free_disk_size, tenant_disk_space_mgr->get_meta_file_free_disk_size());

  // 2.free meta_file_size
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObStorageObjectType::PRIVATE_TABLET_META, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(0, tenant_disk_space_mgr->get_meta_file_alloc_size());
  // free_file_size PRIVATE_TABLET_CURRENT_VERSION
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::PRIVATE_TABLET_META, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(disk_size, tenant_disk_space_mgr->get_meta_file_alloc_size());
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->free_file_size(disk_size, ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION, false/*is_tmp_file_read_cache*/));
  ASSERT_EQ(0, tenant_disk_space_mgr->get_meta_file_alloc_size());

  // 3.alloc meta_file_size not fail
  disk_size = 1L * 1024L * 1024L * 1024L; // 1GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObStorageObjectType::PRIVATE_TABLET_META, false/*is_tmp_file_read_cache*/));
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
