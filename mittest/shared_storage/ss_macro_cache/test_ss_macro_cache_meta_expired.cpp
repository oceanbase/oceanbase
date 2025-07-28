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

class ObMetaMacroExpirationTest : public ::testing::Test
{
public:
  ObMetaMacroExpirationTest() = default;
  virtual ~ObMetaMacroExpirationTest() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  void write_data_meta_macro(ObMacroType macro_type, MacroBlockId &macro_id);

public:
  static const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
  static const uint64_t TABLET_ID = 200001; // tablet_id for MACRO_BLOCK
  static const uint64_t SERVER_ID = 1; // server_id for MACRO_BLOCK
  ObStorageObjectWriteInfo write_info_;
  char write_buf_[WRITE_IO_SIZE];
};

void ObMetaMacroExpirationTest::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void ObMetaMacroExpirationTest::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void ObMetaMacroExpirationTest::SetUp()
{
  // construct write info
  write_buf_[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf_, 'a', mid_offset);
  memset(write_buf_ + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info_.io_desc_.set_wait_event(1);
  write_info_.buffer_ = write_buf_;
  write_info_.offset_ = 0;
  write_info_.size_ = WRITE_IO_SIZE;
  write_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info_.mtl_tenant_id_ = MTL_ID();
}

void ObMetaMacroExpirationTest::write_data_meta_macro(ObMacroType macro_type, MacroBlockId &macro_id)
{
  ObStorageObjectHandle write_object_handle;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  if (macro_type == ObMacroType::DATA_MACRO) {
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  } else if (macro_type == ObMacroType::META_MACRO) {
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_META_MACRO);
  }
  macro_id.set_second_id(TABLET_ID); // tablet_id
  macro_id.set_third_id(SERVER_ID); // server_id
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(0); // tenant_seq
  write_info_.set_effective_tablet_id(TABLET_ID); // effective_tablet_id
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.set_is_write_cache(true); // write cache
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
}

TEST_F(ObMetaMacroExpirationTest, test_expiration)
{
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObArray<MacroBlockId> tablet_data_macros;
  ObArray<MacroBlockId> tablet_meta_macros;
  ObArray<MacroBlockId> tablet_data_macros_expired;
  ObArray<MacroBlockId> tablet_meta_macros_expired;
  bool is_exist = false;

  tenant_file_mgr->calibrate_disk_space_task_.is_inited_ = false; // shut down calibrate task
  MacroBlockId data_macro_id;
  MacroBlockId meta_macro_id;
  write_data_meta_macro(ObMacroType::DATA_MACRO, data_macro_id);
  write_data_meta_macro(ObMacroType::META_MACRO, meta_macro_id);

  tenant_file_mgr->list_local_private_macro_file(TABLET_ID, 0/*transfer_seq*/, ObMacroType::DATA_MACRO, tablet_data_macros);
  tenant_file_mgr->list_local_private_macro_file(TABLET_ID, 0/*transfer_seq*/, ObMacroType::META_MACRO, tablet_meta_macros);
  // before expire, there are some data and meta macros
  LOG_INFO("before expire", K(tablet_data_macros.count()), K(tablet_meta_macros.count()));
  ASSERT_EQ(1, tablet_data_macros.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(data_macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(1, tablet_meta_macros.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  ObTenantConfig *tenant_config = ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(MTL_ID());
  ASSERT_NE(nullptr, tenant_config);
  tenant_config->_ss_local_cache_expiration_time = 1 * 1000L * 1000L; // 1s

  sleep(5); // sleep 5s, ensure the expiration time is reached
  macro_cache_mgr->expire_task_.runTimerTask();   // run expire task: write cache -> read cache
  sleep(5); // ensure write cache -> read cache is done
  macro_cache_mgr->expire_task_.runTimerTask();   // run expire task: delete local file

  // after expire, data macros should be expired, meta macros should not be expired
  tenant_file_mgr->list_local_private_macro_file(TABLET_ID, 0/*transfer_seq*/, ObMacroType::DATA_MACRO, tablet_data_macros_expired);
  tenant_file_mgr->list_local_private_macro_file(TABLET_ID, 0/*transfer_seq*/, ObMacroType::META_MACRO, tablet_meta_macros_expired);
  LOG_INFO("after expire", K(tablet_data_macros_expired.count()), K(tablet_meta_macros_expired.count()));

  ASSERT_EQ(0, tablet_data_macros_expired.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(data_macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  // meta macro does not participate in expiration
  ASSERT_EQ(1, tablet_meta_macros_expired.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_meta_expired.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_meta_expired.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
