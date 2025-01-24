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

class TestSSTmpFileFlushTask : public ::testing::Test
{
public:
  TestSSTmpFileFlushTask() {}
  virtual ~TestSSTmpFileFlushTask() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  static const int64_t WRITE_IO_SIZE = 16 * 1024; // 16KB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSTmpFileFlushTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
}

void TestSSTmpFileFlushTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSTmpFileFlushTask::SetUp()
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
  write_info_.tmp_file_valid_length_ = WRITE_IO_SIZE;
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

void TestSSTmpFileFlushTask::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

TEST_F(TestSSTmpFileFlushTask, basic_flush)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSTmpFileFlushTask &flush_task = file_manager->tmp_file_flush_task_;

  // 1. write
  const int64_t tmp_file_cnt = 100;
  MacroBlockId macro_ids[tmp_file_cnt];
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), i));
    macro_ids[i].set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_ids[i].set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    macro_ids[i].set_second_id(i); // tmp_file_id
    macro_ids[i].set_third_id(1); // segment_id
    ASSERT_TRUE(macro_ids[i].is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_ids[i]));

    ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
    ASSERT_NE(nullptr, file_manager);
    write_info_.buffer_ = write_buf_;
    write_info_.offset_ = 0;
    write_info_.size_ = WRITE_IO_SIZE / 2;
    write_info_.tmp_file_valid_length_ = WRITE_IO_SIZE / 2;
    write_info_.io_desc_.set_unsealed();
    ASSERT_EQ(OB_SUCCESS, file_manager->async_append_file(write_info_, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

    write_info_.buffer_ = write_buf_ + WRITE_IO_SIZE / 2;
    write_info_.offset_ = WRITE_IO_SIZE / 2;
    write_info_.size_ = WRITE_IO_SIZE / 2;
    write_info_.tmp_file_valid_length_ = WRITE_IO_SIZE;
    write_info_.io_desc_.set_sealed();
    ASSERT_EQ(OB_SUCCESS, file_manager->async_append_file(write_info_, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
    ObIOFlag flag;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.io_handle_.get_io_flag(flag));
    ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.seal_object(macro_ids[i], 0/*ls_epoch_id*/));
  }

  // 2. wait flushing sealed tmp file to object storage
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
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

    // except read from tmp file read cache
    ObIOFlag flag;
    ASSERT_EQ(OB_SUCCESS, read_object_handle.get_io_handle().get_io_flag(flag));
    ASSERT_FALSE(flag.is_sync());

    ObPrereadCacheManager &preread_cache_mgr = file_manager->get_preread_cache_mgr();
    bool is_exist = false;
    ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.is_exist_in_lru(macro_ids[i], is_exist));
    ASSERT_TRUE(is_exist);
  }

  const int64_t max_parallel_cnt = ObSSTmpFileFlushTask::MAX_FLUSH_PARALLELISM;
  ASSERT_EQ(max_parallel_cnt, flush_task.free_list_.get_curr_total());
  ASSERT_EQ(0, flush_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, flush_task.async_write_list_.get_curr_total());

  // all sealed segments are flushed to object storage, and there is no unsealed segment.
  // thus, seg_meta_map should be empty
  ASSERT_TRUE(file_manager->get_segment_file_mgr().seg_meta_map_.empty());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_tmp_file_flush_task.log*");
  OB_LOGGER.set_file_name("test_ss_tmp_file_flush_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
