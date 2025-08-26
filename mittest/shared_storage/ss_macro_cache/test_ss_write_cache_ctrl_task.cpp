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

class TestSSWriteCacheCtrlTask : public ::testing::Test
{
public:
  TestSSWriteCacheCtrlTask() : write_info_(), read_info_(), write_buf_(), read_buf_() {}
  virtual ~TestSSWriteCacheCtrlTask() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSWriteCacheCtrlTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
}

void TestSSWriteCacheCtrlTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSWriteCacheCtrlTask::SetUp()
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

  // construct read info
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1);
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = WRITE_IO_SIZE;
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = MTL_ID();
}

void TestSSWriteCacheCtrlTask::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

TEST_F(TestSSWriteCacheCtrlTask, write_cache_threshold)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  // disable write_cache_ctrl_task_, and trigger do_write_cache_ctrl_work() by function call
  macro_cache_mgr->write_cache_ctrl_task_.is_inited_ = false;

  int ret = OB_SUCCESS;
  const uint64_t tablet_id = 200;
  const uint64_t server_id = 1;
  const uint64_t transfer_seq = 0;

  MacroBlockId last_private_macro;

  // 1. write PRIVATE_DATA_MACRO (write cache) to local cache
  const int64_t macro_cnt = 10;
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
    macro_id.set_second_id(tablet_id); // tablet_id
    macro_id.set_third_id(server_id); // server_id
    macro_id.set_macro_transfer_seq(transfer_seq); // transfer_seq
    macro_id.set_tenant_seq(i);  //tenant_seq
    ASSERT_TRUE(macro_id.is_valid());
    last_private_macro = macro_id;
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

    write_info_.set_is_write_cache(true); // write cache
    write_info_.set_effective_tablet_id(tablet_id);
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info_, write_object_handle));
    write_object_handle.reset();
  }

  // 2. simulate reach write cache threshold
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);
  const int64_t write_cache_threshold_size = tnt_disk_space_mgr->get_macro_cache_size()
                                             * ObSSMacroCacheMgr::WRITE_CACHE_THRESHOLD / 100;
  macro_cache_mgr->inc_write_cache_size(last_private_macro, write_cache_threshold_size);

  // 3. trigger do_write_cache_ctrl_work() by function call
  macro_cache_mgr->do_write_cache_ctrl_work();

  // 4. check if write cache is translated to read cache
  const int64_t start_us = ObTimeUtility::current_time_us();
  bool is_completed = false;
  while (!is_completed) {
    sleep(1); // sleep 1s
    bool has_write_cache = false;
    for (int64_t i = 0; (i < macro_cnt) && !has_write_cache; ++i) {
      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      macro_id.set_second_id(tablet_id); // tablet_id
      macro_id.set_third_id(server_id); // server_id
      macro_id.set_macro_transfer_seq(transfer_seq); // transfer_seq
      macro_id.set_tenant_seq(i);  //tenant_seq
      ASSERT_TRUE(macro_id.is_valid());
      ObSSMacroCacheMetaHandle meta_handle;
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
      if (meta_handle()->get_is_write_cache()) {
        has_write_cache = true;
      }
    }
    if (!has_write_cache) {
      is_completed = true;
    }
    if ((ObTimeUtility::current_time_us() - start_us) > (2 * 60 * 1000L * 1000L)) { // 2min
      OB_LOG(WARN, "wait too long");
      ASSERT_TRUE(false); // let case fail
    }
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_write_cache_ctrl_task.log*");
  OB_LOGGER.set_file_name("test_ss_write_cache_ctrl_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
