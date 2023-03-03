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
  // init MTL
  ObTenantBase tbase(1001);
  ObTenantEnv::set_tenant(&tbase);
}

void TestLogGroupBuffer::TearDown()
{
  PALF_LOG(INFO, "TestLogGroupBuffer has TearDown");
  PALF_LOG(INFO, "TearDown success");
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
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.get_buffer_pos_(lsn, start_pos));
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
  int64_t len = 0;
  LSN reuse_lsn(1024);
  EXPECT_EQ(OB_NOT_INIT, log_group_buffer_.fill_padding_body(lsn, len));
  LSN start_lsn(100);
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.init(start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill_padding_body(lsn, len));
  lsn = reuse_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill_padding_body(lsn, len));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_group_buffer_.fill_padding_body(lsn, len));
  len = 100;
  lsn.val_ = start_lsn.val_ - 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_group_buffer_.fill_padding_body(lsn, len));
  lsn.val_ = start_lsn.val_;
  EXPECT_EQ(OB_SUCCESS, log_group_buffer_.inc_update_reuse_lsn(reuse_lsn));
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_group_buffer_.fill_padding_body(lsn, len));
  lsn = reuse_lsn;
  len = log_group_buffer_.get_available_buffer_size() + 1;
  EXPECT_EQ(OB_EAGAIN, log_group_buffer_.fill_padding_body(lsn, len));
  len = 1024;
  int64_t used_size = len;
  const int64_t buf_size = log_group_buffer_.get_available_buffer_size();
  LSN buf_end_lsn = reuse_lsn + (buf_size - (reuse_lsn.val_ - start_lsn.val_));
  while (lsn + len < buf_end_lsn) {
    EXPECT_EQ(OB_SUCCESS, log_group_buffer_.fill_padding_body(lsn, len));
    lsn.val_ += len;
  }
  EXPECT_GT(lsn + len, buf_end_lsn);
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

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./test_log_group_buffer.log*");
  OB_LOGGER.set_file_name("test_log_group_buffer.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_group_buffer");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
