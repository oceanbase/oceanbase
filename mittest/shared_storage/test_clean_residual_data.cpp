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
// This test is used to check whether the residual data is cleaned up after all tests are completed.
class TestCleanResidualData : public ::testing::Test
{
public:
  TestCleanResidualData() = default;
  virtual ~TestCleanResidualData() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestCleanResidualData::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestCleanResidualData::TearDownTestCase()
{
  const int failed_count = ::testing::UnitTest::GetInstance()->failed_test_count();
  if (failed_count == 0) {
    int ret = OB_SUCCESS;
    const char *uri = "test_io_manager_rw";
    const int buf_size = 100;
    char file_uri[buf_size];
    file_uri[0] = '\0';
    share::ObDeviceConfig device_config;
    share::ObBackupStorageInfo storage_info;
    common::ObBackupIoAdapter io_adapter;
    common::ObStorageType type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
    bool is_exist_before_clean = false;
    bool is_exist_after_clean = true;
    if (OB_FAIL(share::ObDeviceConfigMgr::get_instance().get_device_config(
            share::ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config))) {
      LOG_WARN("failed to get device config", KR(ret));
    } else if (OB_FAIL(get_storage_type_from_path(device_config.path_, type))) {
      LOG_WARN("failed to get storage type", KR(ret), KCSTRING(device_config.path_));
    } else if (OB_FAIL(storage_info.set(
                   type, device_config.endpoint_, device_config.access_info_, device_config.extension_))) {
      LOG_WARN("failed to set storage info", KR(ret), KCSTRING(device_config.path_));
    } else if (OB_FAIL(databuff_printf(file_uri, buf_size, "%s/%s", device_config.path_, uri))) {
      LOG_WARN("failed to get file uri", KR(ret), KCSTRING(uri), KCSTRING(device_config.path_));
    } else if (OB_FAIL(io_adapter.is_exist(file_uri, &storage_info, is_exist_before_clean))) {
      LOG_WARN("failed to check file exist before clean", KR(ret), KCSTRING(file_uri));
    } else {
      // Ensure that the file exists
      ASSERT_TRUE(is_exist_before_clean);
    }
    if (FAILEDx(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    } else if (OB_FAIL(io_adapter.is_exist(file_uri, &storage_info, is_exist_after_clean))) {
      LOG_WARN("failed to check file exist after clean", KR(ret), KCSTRING(file_uri));
    } else {
      // Ensure that the file does not exist
      ASSERT_FALSE(is_exist_after_clean);
    }
  }
  MockTenantModuleEnv::get_instance().destroy();
}

// Upload a file to test if it is cleared when all tests pass.
TEST_F(TestCleanResidualData, clean_residual_data_test)
{
  int ret = OB_SUCCESS;
  const char *uri = "test_io_manager_rw";
  const int buf_size = 100;
  char file_uri[buf_size];
  file_uri[0] = '\0';
  const int64_t size = 10;
  char buf[size] = "123456789";
  const uint64_t storage_id = 2;
  const common::ObStorageUsedMod mod = common::ObStorageUsedMod::STORAGE_USED_DATA;
  const common::ObStorageIdMod storage_id_mod(storage_id, mod);
  share::ObDeviceConfig device_config;
  share::ObBackupStorageInfo storage_info;
  common::ObBackupIoAdapter io_adapter;
  common::ObStorageType type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
  if (OB_FAIL(
          share::ObDeviceConfigMgr::get_instance().get_device_config(share::ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config))) {
    LOG_WARN("failed to get device config", KR(ret));
  } else if (OB_FAIL(get_storage_type_from_path(device_config.path_, type))) {
    LOG_WARN("failed to get storage type", KR(ret), KCSTRING(device_config.path_));
  } else if (OB_FAIL(storage_info.set(
                 type, device_config.endpoint_, device_config.access_info_, device_config.extension_))) {
    LOG_WARN("failed to set storage info", KR(ret), KCSTRING(device_config.path_));
  } else if (OB_FAIL(databuff_printf(file_uri, buf_size, "%s/%s", device_config.path_, uri))) {
    LOG_WARN("failed to get file uri", KR(ret), KCSTRING(uri), KCSTRING(device_config.path_));
  } else if (OB_FAIL(io_adapter.write_single_file(file_uri, &storage_info, buf, size, storage_id_mod))) {
    LOG_WARN("failed to write single file", KR(ret), KCSTRING(file_uri), K(size), K(storage_id_mod));
  }
}
}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_clean_residual_data.log*");
  OB_LOGGER.set_file_name("test_clean_residual_data.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}