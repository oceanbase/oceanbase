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
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
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
  static void wait_tmp_file_flush();
public:
  class TestThread : public Threads
  {
  public:
    TestThread(ObTenantBase *tenant_base) : tenant_base_(tenant_base) {}
    virtual void run(int64_t idx) final
    {
      int ret = OB_SUCCESS;
      ObTenantEnv::set_tenant(tenant_base_);
      ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
      ASSERT_NE(nullptr, macro_cache_mgr);
      ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
      ASSERT_NE(nullptr, tenant_file_mgr);
      if (idx % 2 == 0) {
        LOG_INFO("start flush tmp file");
        MacroBlockId macro_id;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(tmp_file_id_); // tmp_file_id
        macro_id.set_third_id(0);             // segment_file_id
        ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->push_to_flush_map(macro_id, 0/*ls_epoch_id*/));
        // wait flush_map's tmp file flush success
        wait_tmp_file_flush();
        LOG_INFO("finish flush tmp file");
      } else {
        LOG_INFO("start append tmp file");
        const int64_t append_cnt = 100;
        for (int64_t i = 1; i <= append_cnt; i++) {
          ob_usleep(ObSSMacroCacheFlushTask::SLOW_SCHEDULE_INTERVAL_US / 10);
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
        ASSERT_EQ((append_cnt + 1) * file_size_, meta_handle.get_target_valid_length());
        ASSERT_EQ((append_cnt + 1) * file_size_, meta_handle.get_valid_length());
        ASSERT_TRUE(meta_handle.is_in_local());
      }
    }

  private:
    ObTenantBase *tenant_base_;
  };

public:
  static const int64_t tmp_file_id_ = 114;
  static const int64_t file_size_;
  static const int64_t thread_cnt_ = 2;
};

const int64_t TestFlushUnsealedFile::file_size_ = 8 * 1024L; // 8KB

void TestFlushUnsealedFile::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
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
  write_info.set_tmp_file_valid_length(valid_length);
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
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
}

void TestFlushUnsealedFile::wait_tmp_file_flush()
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObSSMacroCacheFlushTask &flush_task = macro_cache_mgr->flush_task_;
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((macro_cache_mgr->flush_map_.size() != 0) ||
        (flush_task.files_to_flush_.count() != 0) ||
        (flush_task.free_list_.get_curr_total() != ObSSMacroCacheFlushTask::MAX_FLUSH_PARALLELISM)) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret),
          K(macro_cache_mgr->flush_map_.size()), K(flush_task.files_to_flush_.count()),
          K(flush_task.async_read_list_.get_curr_total()), K(flush_task.async_write_list_.get_curr_total()));
      break;
    }
  }
}

TEST_F(TestFlushUnsealedFile, flush_and_append_concurrently)
{
  append_write_tmp_file(0, file_size_);
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, file_manager);
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
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, file_manager);
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id_); // tmp_file_id
  macro_id.set_third_id(0);             // segment_file_id
  // 1.1 first set_sealed and try push macro id to flush map, then try push macro id to flush map
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->seal_and_push_to_flush_map(macro_id, 0 /*ls_epoch_id*/));
  ob_usleep(2 * 1000L * 1000L);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->push_to_flush_map(macro_id, 0 /*ls_epoch_id*/));
  // 1.2 wait flushing tmp file to object storage
  wait_tmp_file_flush();
  // 1.3 when seal first, sealed file will be flushed to remote object storage.
  //     seg meta will be reserved until this seg is evicted by macro_cache_mgr.
  //     seg meta is_flushed_seg_sealed will be true.
  bool is_meta_exist = true;
  TmpFileSegId seg_id(tmp_file_id_, 0);
  TmpFileMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
  ASSERT_TRUE(is_meta_exist);
  {
    SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
    ASSERT_TRUE(meta_handle.get_tmpfile_meta()->is_flushed_seg_sealed_);
    ASSERT_EQ(file_size_, meta_handle.get_tmpfile_meta()->flushed_valid_length_);
  }
  bool is_file_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE));
  macro_id.set_fourth_id(meta_handle.get_valid_length());
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_FALSE(is_file_exist);
  // Note: must delete_tmp_file with file_size, which would delete tmp file meta.
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(macro_id, file_size_));

  macro_id.reset();
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id_); // tmp_file_id
  macro_id.set_third_id(0);             // segment_file_id
  append_write_tmp_file(0, file_size_);
  // 2.1 first try push macro id to flush map, then seal and try push macro id to flush map
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->push_to_flush_map(macro_id, 0 /*ls_epoch_id*/));
  ob_usleep(2 * 1000L * 1000L);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->seal_and_push_to_flush_map(macro_id, 0 /*ls_epoch_id*/));
  // 2.2 wait flushing tmp file to object storage
  wait_tmp_file_flush();
  // 2.3 when unsealed macro id first flush, unsealed file (with valid_length suffix) will be flushed
  //     to remote object storage. seg meta is_in_local will be true and local cache file will be
  //     reserved, until this seg is evicted by macro_cache_mgr.
  //     then, sealed file will be flushed to remote object storage. similarly, seg meta is_in_local
  //     will be true and local cache file will be reserved, until this seg is evicted by macro_cache_mgr.
  //     seg meta is_flushed_seg_sealed will be true.
  is_meta_exist = false;
  meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
  ASSERT_TRUE(is_meta_exist);
  ASSERT_TRUE(meta_handle.is_in_local());
  {
    SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
    ASSERT_TRUE(meta_handle.get_tmpfile_meta()->is_flushed_seg_sealed_);
    ASSERT_EQ(file_size_, meta_handle.get_tmpfile_meta()->flushed_valid_length_);
  }
  ASSERT_EQ(file_size_, meta_handle.get_valid_length());
  is_file_exist = true;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE));
  macro_id.set_fourth_id(meta_handle.get_valid_length());
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_TRUE(is_file_exist);

  // 2.4 trigger evict, seg meta and local cache file will be deleted
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  meta_handle.reset();
  const uint8_t idx = static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE);
  macro_cache_mgr->macro_caches_[idx]->evict(macro_id, true/*is_write_cache*/);
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist));
  ASSERT_FALSE(is_meta_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_file_exist));
  ASSERT_FALSE(is_file_exist);

  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(macro_id, file_size_));
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
