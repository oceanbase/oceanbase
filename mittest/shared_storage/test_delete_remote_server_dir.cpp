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
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestDeleteServerDir : public ::testing::Test
{
public:
  TestDeleteServerDir() = default;
  virtual ~TestDeleteServerDir() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestDeleteServerDir::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestDeleteServerDir::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestDeleteServerDir, delete_server_dir)
{
  int ret = OB_SUCCESS;
  const char *file_name = "test_delete_server_dir";
  const int buf_size = 1024;
  char file_uri[buf_size];
  char file_uri_reserve[buf_size];
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  file_uri[0] = '\0';
  file_uri_reserve[0] = '\0';
  const int64_t size = 10;
  char buf[size] = "123456789";
  const uint64_t storage_id = 2;
  const common::ObStorageUsedMod mod = common::ObStorageUsedMod::STORAGE_USED_DATA;
  const common::ObStorageIdMod storage_id_mod(storage_id, mod);
  share::ObDeviceConfig device_config;
  share::ObBackupStorageInfo storage_info;
  common::ObBackupIoAdapter io_adapter;
  common::ObStorageType type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
  bool is_exist_before_delete = false;
  bool is_exist_after_delete = false;
  bool is_exist_reserve = false;

  // get device config
  ASSERT_EQ(OB_SUCCESS, share::ObDeviceConfigMgr::get_instance().get_device_config(share::ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config));
  ASSERT_EQ(OB_SUCCESS, get_storage_type_from_path(device_config.path_, type));
  ASSERT_EQ(OB_SUCCESS, storage_info.set(type, device_config.endpoint_, device_config.access_info_, device_config.extension_, OB_INVALID_DEST_ID));
  // get file uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(file_uri, buf_size, "%s/%s_%ld/%s_%ld/%s", device_config.path_,
                                         CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, file_name));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(file_uri_reserve, buf_size, "%s/%s_%ld/%s_%ld/%s", device_config.path_,
                                         CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id + 1, file_name));
  // write file
  ASSERT_EQ(OB_SUCCESS, io_adapter.write_single_file(file_uri, &storage_info, buf, size, storage_id_mod));
  ASSERT_EQ(OB_SUCCESS, io_adapter.write_single_file(file_uri_reserve, &storage_info, buf, size, storage_id_mod));
  LOG_INFO("write file successfully", KR(ret), KCSTRING(file_uri), KCSTRING(file_uri_reserve), K(size), K(storage_id_mod), KCSTRING(device_config.path_));
  // check if file exists before delete
  ASSERT_EQ(OB_SUCCESS, io_adapter.is_exist(file_uri, &storage_info, is_exist_before_delete));
  ASSERT_TRUE(is_exist_before_delete);
  // check if reserve file exists
  ASSERT_EQ(OB_SUCCESS, io_adapter.is_exist(file_uri_reserve, &storage_info, is_exist_reserve));
  ASSERT_TRUE(is_exist_reserve);

  ObArray<uint64_t> server_ids;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_server_ids(server_ids));
  ASSERT_EQ(2, server_ids.count());
  ASSERT_TRUE((server_id == server_ids.at(0)) || (server_id == server_ids.at(1)));
  ASSERT_TRUE(((server_id + 1) == server_ids.at(0)) || ((server_id + 1) == server_ids.at(1)));

  // delete server dir, NOTE: reserve file should not be deleted
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_remote_server_dir(server_id));
  // check if file exists after delete
  ASSERT_EQ(OB_SUCCESS, io_adapter.is_exist(file_uri, &storage_info, is_exist_after_delete));
  ASSERT_FALSE(is_exist_after_delete);
  // check if reserve file exists
  ASSERT_EQ(OB_SUCCESS, io_adapter.is_exist(file_uri_reserve, &storage_info, is_exist_reserve));
  ASSERT_TRUE(is_exist_reserve);
}
}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_delete_remote_server_dir.log*");
  OB_LOGGER.set_file_name("test_delete_remote_server_dir.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}