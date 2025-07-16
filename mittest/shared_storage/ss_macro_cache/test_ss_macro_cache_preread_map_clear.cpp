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

#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "observer/ob_server.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::storage;
using namespace oceanbase::observer;

class MyObSSPrereadMapUpdateCallback
{
public:
  MyObSSPrereadMapUpdateCallback() {}
  virtual ~MyObSSPrereadMapUpdateCallback() {}
  void operator()(PrereadMapPairType &entry);
  bool ret_;

};

void MyObSSPrereadMapUpdateCallback::operator()(PrereadMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObPrereadNode &preread_node = entry.second;
  preread_node.timestamp_us_ = ObTimeUtility::current_time_us() - ObPrereadCacheManager::PREREAD_ENTRY_EXPIRATION_TIME_US - 100;
  ret_ = ret;
}

class ObPrereadMapClearTest : public ::testing::Test
{
public:
  ObPrereadMapClearTest() = default;
  virtual ~ObPrereadMapClearTest() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();

  void check_exist(ObTenantFileManager *tenant_file_mgr, const int64_t i, const ObSSMacroCacheType cur_type, const bool exist)
  {
    MacroBlockId macro_id;
    bool is_exist = false;
    switch (cur_type) {
      case ObSSMacroCacheType::MACRO_BLOCK:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(MACRO_BLOCK_TABLET_ID);       //tablet_id
        macro_id.set_third_id(MACRO_BLOCK_SERVER_ID);        //server_id
        macro_id.set_macro_transfer_seq(MACRO_BLOCK_TRANSFER_SEQ);   // transfer_seq
        macro_id.set_tenant_seq(i);           // tenant_seq
        break;
      case ObSSMacroCacheType::META_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
        macro_id.set_second_id(META_FILE_LS_ID);  // ls_id
        macro_id.set_third_id(META_FILE_TABLET_ID); // tablet_id
        macro_id.set_meta_transfer_seq(META_FILE_TRANSFER_SEQ); //transfer_seq
        macro_id.set_meta_version_id(i); // meta_version_id
        break;
      case ObSSMacroCacheType::TMP_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(TMP_FILE_ID); // tmp_file_id
        macro_id.set_third_id(i); // segment_id
        break;
      case ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(HOT_TABLET_TABLET_ID);
        macro_id.set_third_id(HOT_TABLET_SERVER_ID); // server_id
        macro_id.set_macro_transfer_seq(HOT_TABLET_TRANSFER_SEQ); // transfer_seq
        macro_id.set_tenant_seq(i); // tenant_seq
        break;
      default:
        ASSERT_TRUE(false);
        break;
    }
    ASSERT_TRUE(macro_id.is_valid());
    tenant_file_mgr->get_preread_cache_mgr().is_exist_in_preread_map(macro_id, is_exist);
    ASSERT_TRUE(is_exist == exist);
  }

  void macro_cache_preread(ObTenantFileManager *tenant_file_mgr, const int64_t i, const ObSSMacroCacheType cache_type)
  {
    MacroBlockId macro_id;
    uint64_t tablet_id = 0;
    MyObSSPrereadMapUpdateCallback update_callback;
    // put or write
    switch (cache_type) {
      case ObSSMacroCacheType::MACRO_BLOCK:
        tablet_id = MACRO_BLOCK_TABLET_ID;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(tablet_id);       //tablet_id
        macro_id.set_third_id(MACRO_BLOCK_SERVER_ID);             //server_id
        macro_id.set_macro_transfer_seq(MACRO_BLOCK_TRANSFER_SEQ);   // transfer_seq
        macro_id.set_tenant_seq(i);           // tenant_seq
        break;
      case ObSSMacroCacheType::META_FILE:
        tablet_id = META_FILE_TABLET_ID;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
        macro_id.set_second_id(META_FILE_LS_ID);  // ls_id
        macro_id.set_third_id(tablet_id); // tablet_id
        macro_id.set_meta_transfer_seq(META_FILE_TRANSFER_SEQ); //transfer_seq
        macro_id.set_meta_version_id(i); // meta_version_id
        break;
      case ObSSMacroCacheType::TMP_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(TMP_FILE_ID); // tmp_file_id
        macro_id.set_third_id(i); // segment_id
        break;
      case ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK:
        tablet_id = HOT_TABLET_TABLET_ID;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(tablet_id);
        macro_id.set_third_id(HOT_TABLET_SERVER_ID); //server_id
        macro_id.set_macro_transfer_seq(HOT_TABLET_TRANSFER_SEQ); // transfer_seq
        macro_id.set_tenant_seq(i); // tenant_seq
        break;
      default:
        ASSERT_TRUE(false);
        break;
    }
    // preread
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->push_to_preread_queue(macro_id, ObTabletID(tablet_id)));
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_preread_cache_mgr().preread_map_.atomic_refactored(macro_id, update_callback));
    ASSERT_EQ(OB_SUCCESS, update_callback.ret_);
  }

  void test_preread_map_clear(ObTenantFileManager *tenant_file_mgr, const ObSSMacroCacheType cache_type)
  {
    const int64_t num = 20;
    const char *cur_type_name = get_ss_macro_cache_type_str(cache_type);
    bool is_exist = false;
    int64_t preread_map_size = 0, preread_map_size_af_clear = 0;
    // init status
    preread_map_size = tenant_file_mgr->get_preread_cache_mgr().preread_map_.size();
    LOG_INFO("test_preread_map_clear_info", "preread_map_size_init", preread_map_size, K(cur_type_name));
    // preread
    for (int64_t i = 0; i < num; i++) {
      macro_cache_preread(tenant_file_mgr, i, cache_type);
    }
    // before clear
    preread_map_size = tenant_file_mgr->get_preread_cache_mgr().preread_map_.size();
    LOG_INFO("test_preread_map_clear_info", "preread_map_size_before_clear", preread_map_size, K(cur_type_name));
    // check exist
    for (int64_t i = 0; i < num; i++) {
      is_exist = true;
      check_exist(tenant_file_mgr, i, cache_type, is_exist);
    }
    // clear
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_preread_cache_mgr().do_preread_map_clear_work());
    // after clear
    preread_map_size_af_clear = tenant_file_mgr->get_preread_cache_mgr().preread_map_.size();
    LOG_INFO("test_preread_map_clear_info", "preread_map_size_after_clear", preread_map_size_af_clear, K(cur_type_name));
    LOG_INFO("test_preread_map_clear_info", K(preread_map_size - preread_map_size_af_clear), K(num), K(cur_type_name));
    ASSERT_TRUE(preread_map_size - preread_map_size_af_clear == num);
    // check file exist
    for (int64_t i = 0; i < num; i++) {
      is_exist = false;
      check_exist(tenant_file_mgr, i, cache_type, is_exist);
    }
  }

public:
  static const uint64_t MACRO_BLOCK_TABLET_ID = 200004; // tablet_id for MACRO_BLOCK
  static const uint64_t TMP_FILE_ID = 100; // tmp_file_id for TMP_FILE
  static const uint64_t HOT_TABLET_TABLET_ID = 200005; // tablet_id for HOT_TABLET_MACRO_BLOCK
  static const uint64_t META_FILE_TABLET_ID = 200001; // tablet_id for META_FILE
  static const uint64_t META_FILE_LS_ID = 1001; // ls_id for META_FILE
  static const uint64_t META_FILE_LS_EPOCH_ID = 1; // ls_epoch_id for META_FILE
  static const uint64_t MACRO_BLOCK_TRANSFER_SEQ = 0; // transfer_seq for MACRO_BLOCK
  static const uint64_t HOT_TABLET_TRANSFER_SEQ = 0; // transfer_seq for HOT_TABLET_MACRO_BLOCK
  static const uint64_t META_FILE_TRANSFER_SEQ = 0; // transfer_seq for META_FILE
  static const uint64_t MACRO_BLOCK_SERVER_ID = 1; // server_id for MACRO_BLOCK
  static const uint64_t HOT_TABLET_SERVER_ID = 2; // server_id for HOT_TABLET_MACRO_BLOCK

};

void ObPrereadMapClearTest::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void ObPrereadMapClearTest::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(ObPrereadMapClearTest, test_clear)
{
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  tenant_file_mgr->get_preread_cache_mgr().preread_task_.is_inited_ = false; // stop preread task

  for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
      cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
    if (ObSSMacroCacheType::META_FILE == cache_type ||
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_type) {
      // META_FILE is not supported
      // HOT_TABLET_MACRO_BLOCK is not used in preread map clear test
      continue;
    }
    test_preread_map_clear(tenant_file_mgr, cache_type);
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_preread_map_clear.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_preread_map_clear.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
