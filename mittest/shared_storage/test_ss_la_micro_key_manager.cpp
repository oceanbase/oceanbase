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
using namespace oceanbase::common::hash;

class TestLAMicroKeyManager : public ::testing::Test
{
public:
  TestLAMicroKeyManager() = default;
  virtual ~TestLAMicroKeyManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestLAMicroKeyManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestLAMicroKeyManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLAMicroKeyManager, test_push_micro_key_to_hashset)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache* micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  /*
  prewarm for KVCache micro key:
  physical_key_mode: private_macro micro key -> no
  physical_key_mode: shared_major_meta micro key -> yes
  logical_key_mode: shared_major_data micro key -> yes
  */
  MacroBlockId macro_id;
  macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  macro_id.set_second_id(100);  //tablet_id
  macro_id.set_third_id(101);   //seq_id
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(102);  //tenant_seq
  ObSSMicroBlockCacheKey micro_key;
  ObSSMicroBlockCacheKeyMeta micro_meta;
  micro_key = ObSSMicroBlockCacheKey(ObSSMicroBlockId(macro_id, 103/*offset*/, 104/*size*/));
  micro_meta = ObSSMicroBlockCacheKeyMeta(micro_key, 98/*data_crc*/, 97/*data_size*/, 1/*is_in_l1*/);
  // test 1: push PRIVATE_DATA_MACRO
  ASSERT_EQ(0, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
  ASSERT_EQ(OB_HASH_NOT_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));
  ASSERT_EQ(OB_SUCCESS, micro_cache->latest_access_micro_key_mgr_.push_latest_access_micro_key_to_hashset(micro_meta));
  ASSERT_EQ(0, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
  ASSERT_EQ(OB_HASH_NOT_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));

  // test 2: push PRIVATE_META_MACRO
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_META_MACRO));
  micro_key = ObSSMicroBlockCacheKey(ObSSMicroBlockId(macro_id, 103/*offset*/, 104/*size*/));
  micro_meta = ObSSMicroBlockCacheKeyMeta(micro_key, 98/*data_crc*/, 97/*data_size*/, 1/*is_in_l1*/);
  ASSERT_EQ(OB_HASH_NOT_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));
  ASSERT_EQ(OB_SUCCESS, micro_cache->latest_access_micro_key_mgr_.push_latest_access_micro_key_to_hashset(micro_meta));
  ASSERT_EQ(0, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
  ASSERT_EQ(OB_HASH_NOT_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));

  // test 3: push SHARED_MAJOR_META_MACRO
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
  micro_key = ObSSMicroBlockCacheKey(ObSSMicroBlockId(macro_id, 103/*offset*/, 104/*size*/));
  micro_meta = ObSSMicroBlockCacheKeyMeta(micro_key, 98/*data_crc*/, 97/*data_size*/, 1/*is_in_l1*/);
  ASSERT_EQ(OB_HASH_NOT_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));
  ASSERT_EQ(OB_SUCCESS, micro_cache->latest_access_micro_key_mgr_.push_latest_access_micro_key_to_hashset(micro_meta));
  ASSERT_EQ(1, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
  ASSERT_EQ(OB_HASH_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));

  // test 4: push SHARED_MAJOR_DATA_MACRO
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  ObLogicMicroBlockId logic_macro_id;
  logic_macro_id.init(105/*offset*/, ObLogicMacroBlockId(106/*data_seq*/, 107/*logic_version*/, 108/*tablet_id*/));
  micro_key = ObSSMicroBlockCacheKey(logic_macro_id, 109/*crc*/);
  micro_meta = ObSSMicroBlockCacheKeyMeta(micro_key, 98/*data_crc*/, 97/*data_size*/, 1/*is_in_l1*/);
  ASSERT_EQ(OB_HASH_NOT_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));
  ASSERT_EQ(OB_SUCCESS, micro_cache->latest_access_micro_key_mgr_.push_latest_access_micro_key_to_hashset(micro_meta));
  ASSERT_EQ(2, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
  ASSERT_EQ(OB_HASH_EXIST, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.exist_refactored(micro_meta));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_la_micro_key_manager.log*");
  OB_LOGGER.set_file_name("test_la_micro_key_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
