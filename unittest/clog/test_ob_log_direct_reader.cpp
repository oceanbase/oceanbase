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

#include "lib/file/file_directory_utils.h"
#include "clog/ob_log_direct_reader.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_log_file_pool.h"
#include "clog/ob_clog_mgr.h"
#include "share/redolog/ob_log_file_reader.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase::common;

namespace oceanbase {
using namespace clog;
namespace unittest {

class TestObLogDirectReader : public ::testing::Test {
public:
  TestObLogDirectReader();
  virtual ~TestObLogDirectReader();
  virtual void SetUp();
  virtual void TearDown();

protected:
  int fd_;
  char log_path_[1024];
  char shm_path_[1024];
  ObBaseLogBufferCtrl* log_ctrl_;
  ObBaseLogBuffer* log_buffer_;
  ObLogDir log_dir_;
  ObLogWriteFilePool write_pool_;
  ObLogCache log_cache_;
  ObTailCursor tail_cursor_;
  ObLogDirectReader log_reader_;
};

TestObLogDirectReader::TestObLogDirectReader()
    : fd_(-1),
      log_ctrl_(NULL),
      log_buffer_(NULL),
      log_dir_(),
      write_pool_(),
      log_cache_(),
      tail_cursor_(),
      log_reader_()
{}

TestObLogDirectReader::~TestObLogDirectReader()
{}

void TestObLogDirectReader::SetUp()
{
  int ret = OB_SUCCESS;
  enum ObLogWritePoolType type = ObLogWritePoolType::CLOG_WRITE_POOL;
  getcwd(log_path_, 1024);
  strcat(log_path_, "/test_log_direct_reader");
  getcwd(shm_path_, 1024);
  strcat(shm_path_, "/test_log_direct_reader/shm_buf");

  system("rm -rf test_log_direct_reader");

  ret = log_dir_.init(log_path_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_pool_.init(&log_dir_, 64 * 1024 * 1024, type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_pool_.get_fd(1, fd_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_LOG_FILE_READER.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_reader_.init(log_path_, shm_path_, true, &log_cache_, &tail_cursor_, type);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObBaseLogBufferMgr::get_instance().get_buffer(shm_path_, log_ctrl_);
  ASSERT_EQ(OB_SUCCESS, ret);
  log_buffer_ = log_ctrl_->base_buf_;

  MEMSET(log_ctrl_->data_buf_, 'A', log_buffer_->buf_len_);
  ob_pwrite(fd_, log_ctrl_->data_buf_, log_buffer_->buf_len_, 0);
  MEMSET(log_ctrl_->data_buf_, 'B', log_buffer_->buf_len_);
  ob_pwrite(fd_, log_ctrl_->data_buf_, log_buffer_->buf_len_, log_buffer_->buf_len_);
}

void TestObLogDirectReader::TearDown()
{
  if (fd_ >= 0) {
    write_pool_.close_fd(1, fd_);
    fd_ = -1;
  }
  log_reader_.destroy();
  OB_LOG_FILE_READER.destroy();
}

TEST_F(TestObLogDirectReader, read_from_io)
{
  int ret = OB_SUCCESS;
  ObReadParam read_param;
  ObReadBuf read_buf;
  ObReadRes read_res;
  ObReadCost read_cost;

  char* test_buf = (char*)ob_malloc_align(4096, 2 * 1024 * 1024, ObModIds::TEST);
  ASSERT_TRUE(NULL != test_buf);
  ret = log_reader_.alloc_buf(ObModIds::OB_LOG_DIRECT_READER_COMPRESS_ID, read_buf);
  ASSERT_EQ(OB_SUCCESS, ret);

  log_buffer_->file_write_pos_.file_id_ = 1;
  log_buffer_->file_write_pos_.file_offset_ = 1024 * 1024;
  log_buffer_->file_flush_pos_.file_id_ = 1;
  log_buffer_->file_flush_pos_.file_offset_ = 4096 * 2 + 1;
  int64_t shm_data_len =
      log_buffer_->file_write_pos_.file_offset_ - lower_align(log_buffer_->file_flush_pos_.file_offset_, 4096);
  MEMSET(log_ctrl_->data_buf_, 'X', shm_data_len);
  log_ctrl_->data_buf_[0] = 'A';
  ob_pwrite(fd_, log_ctrl_->data_buf_, shm_data_len, lower_align(log_buffer_->file_flush_pos_.file_offset_, 4096));

  // no shm read
  read_param.file_id_ = 1;
  read_param.offset_ = 0;
  read_param.read_len_ = 4096;
  ret = log_reader_.read_data_direct(read_param, read_buf, read_res, read_cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ob_pread(fd_, test_buf, read_param.read_len_, read_param.offset_);
  ASSERT_EQ(0, MEMCMP(test_buf, read_res.buf_, read_param.read_len_));

  // half shm read
  read_param.file_id_ = 1;
  read_param.offset_ = 0;
  read_param.read_len_ = 4096 * 4;
  ret = log_reader_.read_data_direct(read_param, read_buf, read_res, read_cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ob_pread(fd_, test_buf, read_param.read_len_, read_param.offset_);
  ASSERT_EQ(0, MEMCMP(test_buf, read_res.buf_, read_param.read_len_));

  // right half shm read
  read_param.file_id_ = 1;
  read_param.offset_ = 512 * 1024;
  read_param.read_len_ = 1024 * 1024;
  ret = log_reader_.read_data_direct(read_param, read_buf, read_res, read_cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ob_pread(fd_, test_buf, read_param.read_len_, read_param.offset_);
  ASSERT_EQ(0, MEMCMP(test_buf, read_res.buf_, read_param.read_len_));

  // only shm read
  read_param.file_id_ = 1;
  read_param.offset_ = 4096 * 4;
  read_param.read_len_ = 4096;
  ret = log_reader_.read_data_direct(read_param, read_buf, read_res, read_cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ob_pread(fd_, test_buf, read_param.read_len_, read_param.offset_);
  ASSERT_EQ(0, MEMCMP(test_buf, read_res.buf_, read_param.read_len_));

  log_reader_.free_buf(read_buf);
  ob_free_align(test_buf);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_direct_reader.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_direct_reader");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
