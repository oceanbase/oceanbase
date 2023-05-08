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

#include <gtest/gtest.h>
#include <random>
#include <string>
#include <pthread.h>

#define private public
#include "logservice/palf/log_group_buffer.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/palf/log_entry_header.h"
#undef private
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;

namespace unittest
{

class TestLogGroupBuffer : public ::testing::Test
{
public:
  TestLogGroupBuffer();
  virtual ~TestLogGroupBuffer();
  virtual void SetUp();
  virtual void TearDown();
protected:
  int64_t  palf_id_;
  LogGroupBuffer log_group_buffer_;
};

TestLogGroupBuffer::TestLogGroupBuffer()
    : palf_id_(1)
{
}

TestLogGroupBuffer::~TestLogGroupBuffer()
{
}

void TestLogGroupBuffer::SetUp()
{
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  // init MTL
  ObTenantBase tbase(1001);
  ObTenantEnv::set_tenant(&tbase);
}

void TestLogGroupBuffer::TearDown()
{
  PALF_LOG(INFO, "TestLogGroupBuffer has TearDown");
  PALF_LOG(INFO, "TearDown success");
  log_group_buffer_.destroy();
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
}

TEST_F(TestLogGroupBuffer, test_init)
{
  LSN start_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.init(start_lsn));
  start_lsn.val_ = 0;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INIT_TWICE, log_group_buffer_.init(start_lsn));
}

TEST_F(TestLogGroupBuffer, test_get_buffer_pos)
{
  LSN lsn;
  int64_t start_pos = -1;
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.get_buffer_pos_(lsn, start_pos));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.get_buffer_pos_(lsn, start_pos));
  lsn.val_ = 50;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_group_buffer_.get_buffer_pos_(lsn, start_pos));
  lsn.val_ = 110;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.get_buffer_pos_(lsn, start_pos));
  EXPECT_EQ(10, start_pos);
}

TEST_F(TestLogGroupBuffer, test_can_handle_new_log)
{
  LSN lsn;
  int64_t len = 1024;
  LSN reuse_lsn;
  EXPECT_EQ(false, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  reuse_lsn.val_ = 100;
  EXPECT_EQ(false, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
  lsn.val_ = 200;
  len = 0;
  EXPECT_EQ(false, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
  len = 1024;
  reuse_lsn.reset();
  EXPECT_EQ(false, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
  reuse_lsn.val_ = 110;
  lsn.val_ = start_lsn.val_ - 1;
  EXPECT_EQ(false, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
  lsn.val_ = reuse_lsn.val_ + log_group_buffer_.get_available_buffer_size();
  EXPECT_EQ(false, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
  lsn.val_ = start_lsn.val_;
  EXPECT_EQ(true, log_group_buffer_.can_handle_new_log(lsn, len, reuse_lsn));
}

TEST_F(TestLogGroupBuffer, test_get_log_buf)
{
  LSN lsn;
  int64_t len = 0;
  LogWriteBuf log_buf;
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.get_log_buf(lsn, len, log_buf));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.get_log_buf(lsn, len, log_buf));
  lsn.val_ = 100;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.get_log_buf(lsn, len, log_buf));
  len = 1024;
  lsn.val_ = start_lsn.val_ - 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.get_log_buf(lsn, len, log_buf));
  lsn.val_ = start_lsn.val_;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.get_log_buf(lsn, len, log_buf));
}

TEST_F(TestLogGroupBuffer, test_fill)
{
  LSN lsn;
  char data[1024];
  int64_t len = 0;
  LSN reuse_lsn(1024);
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.fill(lsn, data, len));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill(lsn, data, len));
  lsn = start_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill(lsn, NULL, len));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill(lsn, data, len));
  len = 100;
  lsn.val_ = start_lsn.val_ - 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_group_buffer_.fill(lsn, data, len));
  lsn.val_ = reuse_lsn.val_ - len - 1;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_group_buffer_.fill(lsn, data, len));
  lsn = reuse_lsn;
  len = log_group_buffer_.get_available_buffer_size() + 10;
  EXPECT_EQ(OB_EAGAIN, log_group_buffer_.fill(lsn, data, len));
  len = 1024;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn, data, len));
  int64_t used_size = len;
  const int64_t buf_size = log_group_buffer_.get_available_buffer_size();
  LSN buf_end_lsn = reuse_lsn + (buf_size - (reuse_lsn.val_ - start_lsn.val_));
  while (lsn + len < buf_end_lsn) {
    EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn, data, len));
    lsn.val_ += len;
  }
  EXPECT_GT(lsn + len, buf_end_lsn);
}

TEST_F(TestLogGroupBuffer, test_fill_padding)
{
  LSN lsn;
  const int64_t padding_valid_data_len = LogEntryHeader::PADDING_LOG_ENTRY_SIZE;
  char padding_valid_data[padding_valid_data_len];
  int64_t len = 0;
  LSN reuse_lsn(1024);
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
  lsn = reuse_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
  len = 100;
  lsn.val_ = start_lsn.val_ - 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
  lsn.val_ = start_lsn.val_;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
  lsn = reuse_lsn;
  len = log_group_buffer_.get_available_buffer_size() + 1;
  EXPECT_EQ(OB_EAGAIN, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
  len = 1024;
  int64_t used_size = len;
  const int64_t buf_size = log_group_buffer_.get_available_buffer_size();
  LSN buf_end_lsn = reuse_lsn + (buf_size - (reuse_lsn.val_ - start_lsn.val_));
  while (lsn + len < buf_end_lsn) {
    EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill_padding_body(lsn, padding_valid_data, padding_valid_data_len, len));
    lsn.val_ += len;
  }
  EXPECT_GT(lsn + len, buf_end_lsn);
}

TEST_F(TestLogGroupBuffer, test_fill_padding_cross_bround)
{
  const int64_t padding_valid_data_len = LogEntryHeader::PADDING_LOG_ENTRY_SIZE;
  char padding_valid_data[padding_valid_data_len];
  memset(padding_valid_data, 'c', padding_valid_data_len);
  int64_t len = 0;
  LSN start_lsn(0);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  const int64_t unittest_log_buffer_size = 4 * 1024 * 1024;
  log_group_buffer_.available_buffer_size_ = log_group_buffer_.reserved_buffer_size_ = unittest_log_buffer_size;
  // LSN为0，提交3条1M日志
  const int64_t log_size = 1*1024*1024;
  char *buf = reinterpret_cast<char*>(ob_malloc(1*1024*1024, "unittest"));
  ASSERT_NE(nullptr, buf);
  LSN lsn0(0);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn0, buf, log_size));
  LSN lsn1(1*1024*1024);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn1, buf, log_size));
  LSN lsn2(2*1024*1024);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn2, buf, log_size));

  // 更新reuse lsn为1M, 继续提交2M的padding日志
  LSN lsn3(3*1024*1024);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(lsn1));
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill_padding_body(lsn3, padding_valid_data,
      padding_valid_data_len, 2*log_size));
  LSN lsn4(5*1024*1024);
  // 预期已经没有可用空间
  EXPECT_EQ(OB_EAGAIN, log_group_buffer_.fill(lsn4, buf, 1));
  // |5.5|2|3|4|5.0|
  // 判断5.0的数据是否是padding, 5号日志为2M, 其余日志为1M
  EXPECT_EQ(0, memcmp(padding_valid_data, log_group_buffer_.data_buf_+3*1024*1024, padding_valid_data_len));
  char *zero_data = reinterpret_cast<char*>(ob_malloc(2*log_size, "unittest"));
  ASSERT_NE(nullptr, zero_data);
  memset(zero_data, PADDING_LOG_CONTENT_CHAR, 2*1024*1024);
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_+3*1024*1024+1024, zero_data, 1*1024*1024-1*1024));
  // 更新reuse_lsn为4M-LogEntryHeader::PADDING_LOG_ENTRY_SIZE，此时正好可以将padding日志的有效日志体放在group_buffer尾部
  LSN reuse_lsn = LSN(unittest_log_buffer_size - LogEntryHeader::PADDING_LOG_ENTRY_SIZE);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  LSN lsn5(reuse_lsn);
  // 提交2M的padding日志，有效日志长度为padding_valid_data_len
  // 2.x表示2号日志被复写部分
  // |5.x|2.x|3|4|5.0|, 5.0开始的地方为reuse_lsn
  // 故意将5.x的一些数据搞坏，验证fill_padding_body是否会清0
  memset(log_group_buffer_.data_buf_, 'x', 1*1024);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill_padding_body(lsn5, padding_valid_data, padding_valid_data_len, 2*log_size));
  // 预期log_group_buffer的尾部数据恰好和padding_valid_data一样
  // log_group_buffer的padding剩余数据和padding_valid_data一样
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_+lsn5.val_, padding_valid_data, LogEntryHeader::PADDING_LOG_ENTRY_SIZE));
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_, padding_valid_data+LogEntryHeader::PADDING_LOG_ENTRY_SIZE, padding_valid_data_len-LogEntryHeader::PADDING_LOG_ENTRY_SIZE));
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_+LogEntryHeader::PADDING_LOG_ENTRY_SIZE, zero_data, 2*log_size-padding_valid_data_len));

  // 更新reuse_lsn为4M-LogEntryHeader::PADDING_LOG_ENTRY_SIZE+10，此时padding日志的有效日志体需要放在group_buffer的首尾
  memset(log_group_buffer_.data_buf_, 'x', 1*1024);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn+10));
  LSN lsn6(reuse_lsn+10);
  const int64_t tail_padding_log_size = LogEntryHeader::PADDING_LOG_ENTRY_SIZE - 10;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill_padding_body(lsn6, padding_valid_data, padding_valid_data_len, 2*log_size));
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_+lsn6.val_, padding_valid_data, tail_padding_log_size));
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_, padding_valid_data+tail_padding_log_size, padding_valid_data_len-tail_padding_log_size));
  EXPECT_EQ(0, memcmp(log_group_buffer_.data_buf_+padding_valid_data_len-tail_padding_log_size, zero_data, 2*log_size-padding_valid_data_len));

  ob_free(buf);
  buf = nullptr;
  ob_free(zero_data);
  zero_data = nullptr;
}

TEST_F(TestLogGroupBuffer, test_check_log_buf_wrapped)
{
  LSN lsn;
  int64_t len = 0;
  bool is_wrapped = false;
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  lsn = start_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  len = 10;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  lsn = start_lsn - 1;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  lsn = start_lsn + 10;
  len = 1024;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  EXPECT_FALSE(is_wrapped);
  len = log_group_buffer_.get_available_buffer_size();
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.check_log_buf_wrapped(lsn, len, is_wrapped));
  EXPECT_TRUE(is_wrapped);
}

TEST_F(TestLogGroupBuffer, test_to_leader)
{
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.to_leader());
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.to_leader());
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_group_buffer_.to_leader());
}

TEST_F(TestLogGroupBuffer, test_to_follower)
{
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.to_follower());
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.to_follower());
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.to_follower());
}

TEST_F(TestLogGroupBuffer, test_read_data)
{
  LSN lsn(100);
  char data[1024];
  int64_t len = 100;
  LSN reuse_lsn(0);
  LSN start_lsn(0);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn, data, len));
  LSN read_begin_lsn(0);
  int64_t in_read_size = 100;
  int64_t out_read_size = 0;
  char *out_buf = (char*)malloc(1024);
  // read nothing because reuse_lsn <= read_begin_lsn
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(0, out_read_size);
  reuse_lsn.val_ = 200;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  // read data success
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(in_read_size, out_read_size);
  // fill data at lsn(40M) with len 50.
  lsn.val_ = reuse_lsn.val_ + log_group_buffer_.get_available_buffer_size() - 200;
  len = 50;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn, data, len));
  // read data at lsn(50)
  read_begin_lsn.val_ = 50;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(in_read_size, out_read_size);
  // read data at lsn(49), this pos has been re-written
  read_begin_lsn.val_ = 49;
  out_read_size = 0;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(0, out_read_size);
  // truncate at lsn(40M + 1000)
  LSN truncate_lsn(1000 + log_group_buffer_.get_available_buffer_size());
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.truncate(truncate_lsn));
  // read data at lsn(49), this pos has been re-written
  out_read_size = 0;
  read_begin_lsn.val_ = 49;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  // truncate at lsn(0)
//  truncate_lsn.val_ = 0;
//  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.truncate(truncate_lsn));
  // read data at lsn(500)
  out_read_size = 0;
  read_begin_lsn.val_ = 500;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(0, out_read_size);
  // fill data at lsn 500 - 600
  lsn = truncate_lsn + 500;
  len = 100;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn, data, len));
  reuse_lsn = truncate_lsn + 600;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  // read data at lsn(500), read nothing because of last_truncate_max_lsn_
  out_read_size = 0;
  read_begin_lsn = truncate_lsn + 500;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(100, out_read_size);
  // truncate at lsn(40M + 40M)
  truncate_lsn.val_ = 2 * log_group_buffer_.get_available_buffer_size();
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.truncate(truncate_lsn));
  reuse_lsn = truncate_lsn + 100;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  // fill data at lsn 40M - 600
  lsn.val_ = truncate_lsn.val_ + log_group_buffer_.get_available_buffer_size() - 50;
  len = 100;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill(lsn, data, len));
  // update reuse_lsn
  reuse_lsn = lsn + len;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  // read above wrapped data, expect success
  out_read_size = 0;
  read_begin_lsn = lsn;
  in_read_size = 100;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.read_data(read_begin_lsn, in_read_size, out_buf, out_read_size));
  EXPECT_EQ(in_read_size, out_read_size);

  free(out_buf);
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./test_log_group_buffer.log*");
  OB_LOGGER.set_file_name("test_log_group_buffer.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_group_buffer");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
