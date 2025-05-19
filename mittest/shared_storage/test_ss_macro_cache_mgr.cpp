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

class TestSSMacroCacheMgr : public ::testing::Test
{
public:
  TestSSMacroCacheMgr() {}
  virtual ~TestSSMacroCacheMgr() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void check_macro_cache_meta(const MacroBlockId &macro_id,
                              const ObSSMacroCacheInfo &cache_info);
};

void TestSSMacroCacheMgr::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMacroCacheMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMacroCacheMgr::SetUp()
{
}

void TestSSMacroCacheMgr::TearDown()
{
}

void TestSSMacroCacheMgr::check_macro_cache_meta(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info)
{
  static int64_t call_times = 0;
  call_times++;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr) << "call_times: " << call_times;
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle)) << "call_times: " << call_times;
  ASSERT_TRUE(meta_handle.is_valid()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.effective_tablet_id_, meta_handle()->get_effective_tablet_id()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.size_, meta_handle()->get_size()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.cache_type_, meta_handle()->get_cache_type()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.is_write_cache_, meta_handle()->get_is_write_cache()) << "call_times: " << call_times;
}

TEST_F(TestSSMacroCacheMgr, put_update_erase)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200001;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // ObSSMacroCacheInfo
  uint64_t effective_tablet_id = 200001;
  uint32_t size = 1 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);

  // 1. put
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 2. update
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/);
  cache_info.effective_tablet_id_ = 200002;
  cache_info.size_ = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update(macro_id, cache_info, update_param));
  check_macro_cache_meta(macro_id, cache_info);

  // 3. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, put_or_update_and_batch_update)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200001;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // ObSSMacroCacheInfo
  uint64_t effective_tablet_id = 200001;
  uint32_t size = 1 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);

  // 1. put_or_update trigger put
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 2. put_or_update trigger update
  cache_info.effective_tablet_id_ = 200002;
  cache_info.size_ = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 3. put_or_update another macro_id
  MacroBlockId macro_id_2;
  macro_id_2.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id_2.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id_2.set_second_id(tablet_id);
  macro_id_2.set_third_id(server_id);
  macro_id_2.set_macro_transfer_seq(0); // transfer_seq
  macro_id_2.set_tenant_seq(101); // tenant_seq
  ASSERT_TRUE(macro_id_2.is_valid());

  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id_2, cache_info));
  check_macro_cache_meta(macro_id_2, cache_info);

  // 4. batch update
  cache_info.effective_tablet_id_ = 200003;

  ObArray<MacroBlockId> macro_id_arr;
  ASSERT_EQ(OB_SUCCESS, macro_id_arr.push_back(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_id_arr.push_back(macro_id_2));
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/);
  for (int64_t i = 0; i < macro_id_arr.count(); ++i) {
    const MacroBlockId &cur_macro_id = macro_id_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update(cur_macro_id, cache_info, update_param));
  }
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);

  // 5. update_effective_tablet_id
  cache_info.effective_tablet_id_ = 200004;
  ObTabletID cur_tablet_id(cache_info.effective_tablet_id_);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update_effective_tablet_id(macro_id_arr, cur_tablet_id));
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_mgr.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
