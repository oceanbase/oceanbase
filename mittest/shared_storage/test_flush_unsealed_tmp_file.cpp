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

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
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

class TestFlushUnsealedFile : public ::testing::Test
{
public:
  TestFlushUnsealedFile() {}
  virtual ~TestFlushUnsealedFile() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  static void write_dir();
  static void append_write_tmp_file(const int64_t offset, const int64_t valid_length);
public:
  class TestThread : public Threads
  {
  public:
    TestThread(ObTenantBase *tenant_base) : tenant_base_(tenant_base) {}
    virtual void run(int64_t idx) final
    {
      int ret = OB_SUCCESS;
      ObTenantEnv::set_tenant(tenant_base_);
      ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
      ASSERT_NE(nullptr, tenant_file_mgr);
      if (idx % 2 == 0) {
        LOG_INFO("start flush tmp file");
        ObTenantFileManager* file_manager = MTL(ObTenantFileManager*);
        ASSERT_NE(nullptr, file_manager);
        ObSSTmpFileFlushTask &flush_task = file_manager->tmp_file_flush_task_;
        MacroBlockId macro_id;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(tmp_file_id_); // tmp_file_id
        macro_id.set_third_id(0);             // segment_file_id
        ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->push_to_flush_queue(macro_id, 0/*ls_epoch_id*/, false/*is_sealed*/));
        // wait flush_queue's tmp file flush success
        const int64_t start_us = ObTimeUtility::current_time();
        const int64_t timeout_us = 20 * 1000 * 1000L;
        while ((file_manager->flush_queue_.size() != 0) ||
              (flush_task.seg_files_.count() != 0) ||
              (flush_task.free_list_.get_curr_total() != ObSSTmpFileFlushTask::MAX_FLUSH_PARALLELISM)) {
          ob_usleep(10 * 1000);
          if (timeout_us + start_us < ObTimeUtility::current_time()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("waiting time is too long", KR(ret),
                K(file_manager->flush_queue_.size()), K(flush_task.seg_files_.count()),
                K(flush_task.async_read_list_.get_curr_total()), K(flush_task.async_write_list_.get_curr_total()));
            break;
          }
        }
        LOG_INFO("finish flush tmp file");
      } else {
        LOG_INFO("start append tmp file");
        const int64_t append_cnt = 100;
        for (int64_t i = 1; i <= append_cnt; i++) {
          ob_usleep(ObSSTmpFileFlushTask::SLOW_SCHEDULE_INTERVAL_US / 10);
          append_write_tmp_file(i * file_size_, (i + 1) * file_size_);
        }
        LOG_INFO("finish append tmp file");
        int64_t file_len = 0;
        MacroBlockId macro_id;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(tmp_file_id_); // tmp_file_id
        macro_id.set_third_id(0);             // segment_file_id
        ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(macro_id, 0/*ls_epoch_id*/, file_len));
        ASSERT_EQ((append_cnt + 1) * file_size_, file_len);
        TmpFileMetaHandle meta_handle;
        bool is_meta_exist = false;
        TmpFileSegId seg_id(tmp_file_id_, 0);
        ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
        ASSERT_TRUE(is_meta_exist);
        ASSERT_TRUE(meta_handle.is_valid());
        ASSERT_TRUE(meta_handle.get_tmpfile_meta()->is_valid());
        ASSERT_EQ(2, meta_handle.get_tmpfile_meta()->ref_cnt_);
        ASSERT_EQ((append_cnt + 1) * file_size_, meta_handle.get_target_length());
        ASSERT_EQ((append_cnt + 1) * file_size_, meta_handle.get_valid_length());
        ASSERT_TRUE(meta_handle.is_in_local());
      }
    }

  private:
    ObTenantBase *tenant_base_;
  };

public:
  static const int64_t tmp_file_id_ = 114;
  static const int64_t file_size_ = 8 * 1024L; // 8KB
  static const int64_t thread_cnt_ = 2;
};

void TestFlushUnsealedFile::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
}

void TestFlushUnsealedFile::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestFlushUnsealedFile::SetUp()
{
}

void TestFlushUnsealedFile::TearDown()
{
}

void TestFlushUnsealedFile::append_write_tmp_file(const int64_t offset, const int64_t valid_length)
{
  int ret = OB_SUCCESS;
  char write_buf[file_size_];
  MEMSET(write_buf, 'a', file_size_);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = offset;
  write_info.size_ = file_size_;
  write_info.tmp_file_valid_length_ = valid_length;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id_); // tmp_file_id
  macro_id.set_third_id(0);             // segment_file_id
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->append_file(write_info, write_object_handle));
}

TEST_F(TestFlushUnsealedFile, find_unsealed_tmp_file_to_flush)
{
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager*);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(nullptr, file_manager);
  ASSERT_NE(nullptr, disk_space_mgr);
  file_manager->tmp_file_flush_task_.is_inited_ = false; // need disable tmp file flush task
  const int64_t cur_timestamp_us = ObTimeUtility::current_time_s();
  TmpFileMetaHandle cur_meta_handle;
  ASSERT_EQ(OB_SUCCESS, cur_meta_handle.set_tmpfile_meta(true/*is_in_local*/, 8192/*valid_length*/, 8192/*target_length*/));
  const int64_t seg_cnt = 10;
  const int64_t tmp_file_id = 113;
  for (int64_t i = 0; i < seg_cnt; ++i) {
    TmpFileSegId seg_id(tmp_file_id, i);
    ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().insert_meta(seg_id, cur_meta_handle));
    TmpFileMetaHandle meta_handle;
    bool is_meta_exist = false;
    ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
    ASSERT_TRUE(is_meta_exist);
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_TRUE(meta_handle.get_tmpfile_meta()->is_valid());
    meta_handle.get_tmpfile_meta()->append_timestamp_us_ = cur_timestamp_us - 2 * ObSegmentFileManager::UNSEALED_TMP_FILE_FLUSH_THRESHOLD;
  }
  const int64_t tmp_file_write_reserved_size = disk_space_mgr->tmp_file_write_cache_reserved_size_ + 100;
  // when tmp_file_write_cache_alloc_size exceed reserved_size 5%, can flush unsealed tmp file to object storage
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(tmp_file_write_reserved_size, ObStorageObjectType::TMP_FILE, false/*is_read_cache*/));
  // test_find_unsealed_tmp_file_to_flush
  ObArray<TmpFileSegId> seg_ids;
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().find_unsealed_tmp_file_to_flush(seg_ids));
  ASSERT_EQ(seg_cnt, seg_ids.count());
  // test_find_tmp_file_flush_task
  file_manager->find_tmp_file_flush_task_.runTimerTask();
  ASSERT_EQ(seg_cnt, file_manager->flush_queue_.size());
  for (int64_t i = 0; i < seg_cnt; ++i) {
    TmpFileSegId seg_id(tmp_file_id, i);
    ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().delete_meta(seg_id));
    MacroBlockId tmp_file;
    tmp_file.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    tmp_file.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    tmp_file.set_second_id(tmp_file_id);
    tmp_file.set_third_id(i);
    ASSERT_EQ(OB_SUCCESS, file_manager->delete_local_file(tmp_file));
  }
  while (file_manager->flush_queue_.size() > 0) {
    ObLink *ptr = nullptr;
    file_manager->flush_queue_.pop(ptr);
  }
}

TEST_F(TestFlushUnsealedFile, find_unsealed_tmp_file_to_stop_flush_threshold_percent)
{
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager*);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(nullptr, file_manager);
  ASSERT_NE(nullptr, disk_space_mgr);
  file_manager->tmp_file_flush_task_.is_inited_ = false; // need disable tmp file flush task
  const int64_t seg_cnt = 60;
  const int64_t tmp_file_id = 113;
  for (int64_t i = 0; i < seg_cnt; ++i) {
    TmpFileSegId seg_id(tmp_file_id, i);
    TmpFileMetaHandle cur_meta_handle;
    ASSERT_EQ(OB_SUCCESS, cur_meta_handle.set_tmpfile_meta(true/*is_in_local*/, OB_DEFAULT_MACRO_BLOCK_SIZE-1/*valid_length*/, OB_DEFAULT_MACRO_BLOCK_SIZE-1/*target_length*/));
    ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().insert_meta(seg_id, cur_meta_handle));
    TmpFileMetaHandle meta_handle;
    bool is_meta_exist = false;
    ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
    ASSERT_TRUE(is_meta_exist);
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_TRUE(meta_handle.get_tmpfile_meta()->is_valid());
    const int64_t cur_timestamp_us = ObTimeUtility::current_time();
    meta_handle.get_tmpfile_meta()->append_timestamp_us_ = cur_timestamp_us - 2 * ObSegmentFileManager::UNSEALED_TMP_FILE_FLUSH_THRESHOLD;
  }
  // test_find_unsealed_tmp_file_to_flush
  //const int64_t tmp_file_write_reserved_size = disk_space_mgr->tmp_file_write_cache_reserved_size_;
  //ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(tmp_file_write_reserved_size, ObStorageObjectType::TMP_FILE, false/*is_read_cache*/));
  ObArray<TmpFileSegId> seg_ids;
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().find_unsealed_tmp_file_to_flush(seg_ids));
  // because find_unsealed_tmp_file_to_flush min thrshold percent is 4%, so 5%(512MB)->4%(409.6MB) only need find 51 tmp_files(each 2MB)
  const int64_t expected_find_flush_seg_cnt = 51;
  ASSERT_EQ(expected_find_flush_seg_cnt, seg_ids.count());
  // evict the oldest segment file(append timsatmp is smallest), so segment_id is less than expected_find_flush_seg_cnt
  for(int64_t i =0; i < seg_ids.size(); i++) {
    ASSERT_EQ(tmp_file_id, seg_ids.at(i).tmp_file_id_);
    ASSERT_LE(seg_ids.at(i).segment_id_, expected_find_flush_seg_cnt);
  }
  // test_find_tmp_file_flush_task
  // when tmp_file_write_cache_alloc_size exceed 5%, can flush unsealed tmp file to object storage
  file_manager->find_tmp_file_flush_task_.runTimerTask();
  ASSERT_EQ(expected_find_flush_seg_cnt, file_manager->flush_queue_.size());
  for (int64_t i = 0; i < seg_cnt; ++i) {
    TmpFileSegId seg_id(tmp_file_id, i);
    ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().delete_meta(seg_id));
    MacroBlockId tmp_file;
    tmp_file.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    tmp_file.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    tmp_file.set_second_id(tmp_file_id);
    tmp_file.set_third_id(i);
    ASSERT_EQ(OB_SUCCESS, file_manager->delete_local_file(tmp_file));
  }
  while (file_manager->flush_queue_.size() > 0) {
    ObLink *ptr = nullptr;
    file_manager->flush_queue_.pop(ptr);
  }
}

TEST_F(TestFlushUnsealedFile, flush_and_append_concurrently)
{
  append_write_tmp_file(0, file_size_);
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, file_manager);
  file_manager->tmp_file_flush_task_.is_inited_ = true; // need enable tmp file flush task
  file_manager->tmp_file_flush_task_.schedule_tmp_file_flush_task();
  // test flush and append concurrently
  TestFlushUnsealedFile::TestThread flush_append_threads(ObTenantEnv::get_tenant());
  flush_append_threads.set_thread_count(thread_cnt_);
  flush_append_threads.start();
  flush_append_threads.wait();
  flush_append_threads.destroy();
  MacroBlockId tmp_file;
  tmp_file.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  tmp_file.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  tmp_file.set_second_id(tmp_file_id_);
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(tmp_file));
}

TEST_F(TestFlushUnsealedFile, flush_seal_and_unseal_file_concurrently)
{
  int ret = OB_SUCCESS;
  append_write_tmp_file(0, file_size_);
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, file_manager);
  //file_manager->tmp_file_flush_task_.is_inited_ = true; // need enable tmp file flush task
  //file_manager->tmp_file_flush_task_.schedule_tmp_file_flush_task();
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id_); // tmp_file_id
  macro_id.set_third_id(0);             // segment_file_id
  // 1. first push sealed macro id to flush queue, then push unsealed macro id to flush queue
  ASSERT_EQ(OB_SUCCESS, file_manager->push_to_flush_queue(macro_id, 0 /*ls_epoch_id*/, true/*is_sealed*/));
  ob_usleep(1000*1000);
  ASSERT_EQ(OB_SUCCESS, file_manager->push_to_flush_queue(macro_id, 0 /*ls_epoch_id*/, false/*is_sealed*/));
  // 2. wait flushing tmp file to object storage
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  ObSSTmpFileFlushTask &flush_task = file_manager->tmp_file_flush_task_;
  while ((file_manager->flush_queue_.size() != 0) ||
         (flush_task.seg_files_.count() != 0) ||
         (flush_task.free_list_.get_curr_total() != ObSSTmpFileFlushTask::MAX_FLUSH_PARALLELISM)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          K(file_manager->flush_queue_.size()), K(flush_task.seg_files_.count()),
          K(flush_task.async_read_list_.get_curr_total()), K(flush_task.async_write_list_.get_curr_total()));
      break;
    }
  }
  // 3. when sealed macro id first flush, sealed file will flush to remote object storage, and seg meta will be deleted.
  bool is_meta_exist = true;
  TmpFileSegId seg_id(tmp_file_id_, 0);
  TmpFileMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
  ASSERT_FALSE(is_meta_exist);
  bool is_file_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(macro_id));

  append_write_tmp_file(0, file_size_);
  // 4. first push unsealed macro id to flush queue, then push sealed macro id to flush queue
  ASSERT_EQ(OB_SUCCESS, file_manager->push_to_flush_queue(macro_id, 0 /*ls_epoch_id*/, false/*is_sealed*/));
  ob_usleep(1000*1000);
  ASSERT_EQ(OB_SUCCESS, file_manager->push_to_flush_queue(macro_id, 0 /*ls_epoch_id*/, true/*is_sealed*/));
  // 5. wait flushing tmp file to object storage
  start_us = ObTimeUtility::current_time();
  while ((file_manager->flush_queue_.size() != 0) ||
         (flush_task.seg_files_.count() != 0) ||
         (flush_task.free_list_.get_curr_total() != ObSSTmpFileFlushTask::MAX_FLUSH_PARALLELISM)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          K(file_manager->flush_queue_.size()), K(flush_task.seg_files_.count()),
          K(flush_task.async_read_list_.get_curr_total()), K(flush_task.async_write_list_.get_curr_total()));
      break;
    }
  }
  // 6. when unsealed macro id first flush, unsealed file(segid_valid_length file name) will flush to remote object storage, sealed file flush to remote failed, and seg meta is_in_local will be false.
  is_meta_exist = false;
  meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
  ASSERT_TRUE(is_meta_exist);
  ASSERT_FALSE(meta_handle.is_in_local());
  ASSERT_EQ(8 * 1024, meta_handle.get_valid_length());
  is_file_exist = true;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_FALSE(is_file_exist);
  // maybe sealed file flush succed too, so sealed seg file existed in remotr file
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_FALSE(is_file_exist);
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE));
  macro_id.set_fourth_id(meta_handle.get_valid_length());
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(macro_id));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_flush_unsealed_tmp_file.log*");
  OB_LOGGER.set_file_name("test_flush_unsealed_tmp_file.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
