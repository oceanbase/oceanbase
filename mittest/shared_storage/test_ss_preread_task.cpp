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
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
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

class TestSSPreReadTask : public ::testing::Test
{
public:
  TestSSPreReadTask() {}
  virtual ~TestSSPreReadTask() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  static const int64_t WRITE_IO_SIZE = 257 * 1024; // 257KB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSPreReadTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSPreReadTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPreReadTask::SetUp()
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

  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->clear_mem_macro_cache());
}

void TestSSPreReadTask::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

TEST_F(TestSSPreReadTask, basic_pre_read)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  // 1. write tmp_file to object_storage
  const int64_t tmp_file_cnt = 100;
  MacroBlockId macro_ids[tmp_file_cnt];
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    macro_ids[i].set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_ids[i].set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    macro_ids[i].set_second_id(i); // tmp_file_id
    macro_ids[i].set_third_id(1); // segment_id
    ASSERT_TRUE(macro_ids[i].is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_ids[i]));
    ObSSObjectStorageWriter object_storage_writer;
    ASSERT_EQ(OB_SUCCESS, object_storage_writer.aio_write(write_info_, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
    // Note: TMP_FILE has no effective_tablet_id
    ASSERT_EQ(OB_SUCCESS, file_manager->push_to_preread_queue(macro_ids[i],
                                        ObTabletID(ObTabletID::INVALID_TABLET_ID)));
  }

  // 2. wait preread_task read tmp file to local cache.
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          KR(preread_cache_mgr.preread_queue_.size()), KR(preread_task.segment_files_.count()),
          KR(preread_task.async_read_list_.get_curr_total()), KR(preread_task.async_write_list_.get_curr_total()));
      break;
    }
  }
  // 3. read and compare the read data with the written data
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    read_info_.macro_block_id_ = macro_ids[i];
    ObStorageObjectHandle read_object_handle;
    ObSSTmpFileReader tmp_file_reader;
    ASSERT_EQ(OB_SUCCESS, tmp_file_reader.aio_read(read_info_, read_object_handle));
    ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
    ASSERT_NE(nullptr, read_object_handle.get_buffer());
    ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
    ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), WRITE_IO_SIZE));

    ObIOFlag flag;
    ASSERT_EQ(OB_SUCCESS, read_object_handle.get_io_handle().get_io_flag(flag));
    // check read tmp_file from mem_macro_cache, and it will set_sync
    ASSERT_TRUE(flag.is_sync());
  }

  // 4. wait preread_next_segment_file finish
  start_us = ObTimeUtility::current_time();
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          KR(preread_cache_mgr.preread_queue_.size()), KR(preread_task.segment_files_.count()),
          KR(preread_task.async_read_list_.get_curr_total()), KR(preread_task.async_write_list_.get_curr_total()));
      break;
    }
  }

  const int64_t max_parallel_cnt = file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_;
  ASSERT_EQ(max_parallel_cnt, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(0, preread_cache_mgr.preread_queue_.size());
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.async_write_list_.get_curr_total());
  // delete all tmp file
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(macro_ids[i]));
  }
}

TEST_F(TestSSPreReadTask, preread_without_effective_tablet_id)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);

  // 1. write one macro to object storage
  uint64_t tablet_id = 200001;
  uint64_t data_seq = 1;
  uint64_t reorganization_scn = 0;

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_META_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(data_seq); // data_seq
  macro_id.set_fourth_id(reorganization_scn); // reorganization_scn
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSShareMacroWriter share_macro_writer;
  ASSERT_EQ(OB_SUCCESS, share_macro_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // 2. read and cache-miss, which should trigger preread
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_effective_tablet_id(ObTabletID(ObTabletID::INVALID_TABLET_ID));
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 3. wait preread task preread macro to local cache (mem_macro_cache)
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          K(preread_cache_mgr.preread_queue_.size()), K(preread_task.segment_files_.count()),
          K(preread_task.async_read_list_.get_curr_total()), K(preread_task.async_write_list_.get_curr_total()));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->check_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);
}

TEST_F(TestSSPreReadTask, preread_and_gc_parallel)
{
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, file_manager);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, file_manager->calibrate_disk_space_task_.calibrate_disk_space());
  // tmp_file write_cache and read_cache size
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(0, cache_stat.used_);
  // construct macro_id
  MacroBlockId file_id;
  const int64_t tmp_file_id = 100;
  const int64_t segment_id = 101;
  file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  file_id.set_second_id(tmp_file_id); // tmp_file_id
  file_id.set_third_id(segment_id); // segment_id
  ObStorageObjectHandle write_object_handle;
  // write file to object storage
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ObSSObjectStorageWriter object_storage_writer;
  ASSERT_EQ(OB_SUCCESS, object_storage_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  // read file from object storage
  ObPreReadFileMeta file_meta(file_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), 0);
  ObSSPreReadEntry preread_entry(preread_task.allocator_);
  ASSERT_EQ(OB_SUCCESS, preread_entry.init(file_meta));
  ASSERT_EQ(OB_SUCCESS, preread_task.do_async_read_entry(preread_entry));
  ASSERT_EQ(OB_SUCCESS, preread_entry.read_handle_.wait());
  // test1: preread_write,read_whole,GC,update_to_normal
  // push to preread map
  ObPrereadNode preread_node = ObPrereadNode(ObPrereadNodeStatus::FAKE, 0/*file_length*/);
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.set_refactored(file_id, preread_node));
  // create dir and get dir size
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  int64_t expected_disk_size = 0;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  // write
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  int64_t ori_mem_macro_cache_size = mem_macro_cache->cache_stat_.macro_blk_stat().valid_macro_size_;
  ASSERT_EQ(OB_SUCCESS, preread_task.do_write_entry(preread_entry));
  int64_t cur_mem_macro_cache_size = mem_macro_cache->cache_stat_.macro_blk_stat().valid_macro_size_;
  ASSERT_EQ(ori_mem_macro_cache_size + preread_entry.read_handle_.get_data_size(), cur_mem_macro_cache_size);
  // ASSERT_EQ(OB_SUCCESS, preread_entry.write_handle_.wait());
  // expected_disk_size += WRITE_IO_SIZE;
  // ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  // ASSERT_EQ(expected_disk_size, cache_stat.used_);
  // // read_whole
  // ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.set_need_preread(file_id, false/*is_not_need_preread*/));
  // // GC
  // ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(file_id));
  // ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  // ASSERT_EQ(0, cache_stat.used_);
  // // update_to_normal
  // ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.update_to_normal_status(file_id, preread_entry.write_handle_.get_data_size()));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_preread_task.log*");
  OB_LOGGER.set_file_name("test_ss_preread_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
