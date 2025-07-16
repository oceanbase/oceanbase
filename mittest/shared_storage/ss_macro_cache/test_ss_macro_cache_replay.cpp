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

class TestSSMacroCacheReplay : public ::testing::Test
{
public:
  TestSSMacroCacheReplay() : write_info_(), read_info_(), write_buf_(), read_buf_() {}
  virtual ~TestSSMacroCacheReplay() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void write_tmp_file_data(const MacroBlockId &macro_id,
                           const int64_t offset,
                           const int64_t size,
                           const int64_t valid_length,
                           const bool is_sealed,
                           const char *buffer);
  void read_and_compare_data(const MacroBlockId &macro_id,
                             const int64_t offset,
                             const int64_t size);

public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSMacroCacheReplay::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSMacroCacheReplay::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMacroCacheReplay::SetUp()
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

void TestSSMacroCacheReplay::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

void TestSSMacroCacheReplay::write_tmp_file_data(
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size,
    const int64_t valid_length,
    const bool is_sealed,
    const char *buffer)
{
  static int64_t call_times = 0;
  call_times++;
  ObStorageObjectHandle write_object_handle;
  ASSERT_TRUE(macro_id.is_valid()) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id)) << "call_times: " << call_times;
  write_info_.offset_ = offset;
  write_info_.size_ = size;
  write_info_.set_tmp_file_valid_length(valid_length);
  if (is_sealed) {
    write_info_.io_desc_.set_sealed();
  } else {
    write_info_.io_desc_.set_unsealed();
  }
  write_info_.buffer_ = buffer;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait()) << "call_times: " << call_times;
}

void TestSSMacroCacheReplay::read_and_compare_data(
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size)
{
  static int64_t call_times = 0;
  call_times++;
  ObStorageObjectHandle read_object_handle;
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = offset;
  read_info_.size_ = size;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  memset(read_buf_, 0, WRITE_IO_SIZE);
}

TEST_F(TestSSMacroCacheReplay, replay_in_background)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  const int64_t tmp_file_id = 100;
  const int64_t tablet_id = 200001;

  // prepare local tmp file and local private macro, for read in the following steps
  MacroBlockId local_tmp_file_macro_id;
  local_tmp_file_macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  local_tmp_file_macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  local_tmp_file_macro_id.set_second_id(tmp_file_id); // tmp_file_id
  local_tmp_file_macro_id.set_third_id(1); // segment_id
  ASSERT_TRUE(local_tmp_file_macro_id.is_valid());
  write_tmp_file_data(local_tmp_file_macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_data(local_tmp_file_macro_id, 0/*offset*/, 8192/*size*/);
  bool is_exist = false;
  file_manager->is_exist_local_file(local_tmp_file_macro_id, 0/*ls_epoch_id*/, is_exist);
  ASSERT_TRUE(is_exist);

  MacroBlockId local_private_macro_macro_id;
  local_private_macro_macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  local_private_macro_macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  local_private_macro_macro_id.set_second_id(tablet_id); // tablet_id
  local_private_macro_macro_id.set_third_id(1); // server_id
  local_private_macro_macro_id.set_macro_transfer_seq(0); // transfer_seq
  local_private_macro_macro_id.set_tenant_seq(100);  //tenant_seq
  ASSERT_TRUE(local_private_macro_macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(local_private_macro_macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
  read_and_compare_data(local_private_macro_macro_id, write_info_.offset_, write_info_.size_);
  file_manager->is_exist_local_file(local_private_macro_macro_id, 0/*ls_epoch_id*/, is_exist);
  ASSERT_TRUE(is_exist);

  // set is_ckpt_replayed_ to false, so as to simulate ckpt has not been replayed
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ATOMIC_STORE(&(macro_cache_mgr->is_ckpt_replayed_), false);


  // 1. before finishing replay, all background task (except replay task) should not work
  uint64_t ori_run_cnt = macro_cache_mgr->flush_task_.run_cnt_;
  macro_cache_mgr->flush_task_.runTimerTask();
  uint64_t cur_run_cnt = macro_cache_mgr->flush_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->evict_task_.run_cnt_;
  macro_cache_mgr->evict_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->evict_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->expire_task_.run_cnt_;
  macro_cache_mgr->expire_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->expire_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->write_cache_ctrl_task_.run_cnt_;
  macro_cache_mgr->write_cache_ctrl_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->write_cache_ctrl_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->ckpt_task_.run_cnt_;
  macro_cache_mgr->ckpt_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->ckpt_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->tablet_stat_task_.run_cnt_;
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->tablet_stat_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);
  ori_run_cnt = file_manager->calibrate_disk_space_task_.run_cnt_;
  file_manager->calibrate_disk_space_task_.runTimerTask();
  cur_run_cnt = file_manager->calibrate_disk_space_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt, cur_run_cnt);

  // 2. before finishing replay, tmp file and private macro should write through
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id); // tmp_file_id
  macro_id.set_third_id(2); // segment_id
  ASSERT_TRUE(macro_id.is_valid());
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_data(macro_id, 0/*offset*/, 8192/*size*/);
  is_exist = false;
  file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist);
  ASSERT_FALSE(is_exist);

  macro_id.reset();
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(1); // server_id
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(200);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
  read_and_compare_data(macro_id, write_info_.offset_, write_info_.size_);
  file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist);
  ASSERT_FALSE(is_exist);

  // 3. before finishing replay, can read local file
  read_and_compare_data(local_tmp_file_macro_id, write_info_.offset_, write_info_.size_);
  read_and_compare_data(local_private_macro_macro_id, write_info_.offset_, write_info_.size_);

  // 4. before finishing replay, can delete local file
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::delete_local_file(local_tmp_file_macro_id, 0/*ls_epoch_id*/,
            true/*is_print_log*/, true/*is_del_seg_meta*/, false/*is_logical_delete*/));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::delete_local_file(local_private_macro_macro_id, 0/*ls_epoch_id*/,
            true/*is_print_log*/, true/*is_del_seg_meta*/, false/*is_logical_delete*/));



  // set is_ckpt_replayed_ to true, so as to simulate ckpt has been replayed
  ATOMIC_STORE(&(macro_cache_mgr->is_ckpt_replayed_), true);

  // 5. after finishing replay, tmp file and private macro should write local
  macro_id.reset();
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id); // tmp_file_id
  macro_id.set_third_id(3); // segment_id
  ASSERT_TRUE(macro_id.is_valid());
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_data(macro_id, 0/*offset*/, 8192/*size*/);
  is_exist = false;
  file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist);
  ASSERT_TRUE(is_exist);

  macro_id.reset();
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(1); // server_id
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(300);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
  read_and_compare_data(macro_id, write_info_.offset_, write_info_.size_);
  file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist);
  ASSERT_TRUE(is_exist);

  // 6. after finishing replay, all background task should work
  ori_run_cnt = macro_cache_mgr->flush_task_.run_cnt_;
  macro_cache_mgr->flush_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->flush_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->evict_task_.run_cnt_;
  macro_cache_mgr->evict_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->evict_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->expire_task_.run_cnt_;
  macro_cache_mgr->expire_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->expire_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->write_cache_ctrl_task_.run_cnt_;
  macro_cache_mgr->write_cache_ctrl_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->write_cache_ctrl_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->ckpt_task_.run_cnt_;
  macro_cache_mgr->ckpt_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->ckpt_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
  ori_run_cnt = macro_cache_mgr->tablet_stat_task_.run_cnt_;
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  cur_run_cnt = macro_cache_mgr->tablet_stat_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
  ori_run_cnt = file_manager->calibrate_disk_space_task_.run_cnt_;
  file_manager->calibrate_disk_space_task_.runTimerTask();
  cur_run_cnt = file_manager->calibrate_disk_space_task_.run_cnt_;
  ASSERT_EQ(ori_run_cnt + 1, cur_run_cnt);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_replay.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_replay.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
