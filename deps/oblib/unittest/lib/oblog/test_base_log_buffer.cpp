/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/oblog/ob_base_log_buffer.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{

TEST(ObBaseLogBuffer, normal)
{
  int ret = OB_SUCCESS;
  ObBaseLogBufferMgr &buf_mgr = ObBaseLogBufferMgr::get_instance();
  char cur_dir[128];
  char shm_path[128];
  ObBaseLogBufferCtrl *log_ctrl = NULL;
  ObBaseLogBuffer *log_buf = NULL;

  system("rm -rf ./test_blb");
  system("mkdir test_blb");

  getcwd(cur_dir, 128);
  sprintf(shm_path, "%s/test_blb/shm_buf", cur_dir);

  //not absolute dir
  strcpy(shm_path, "./test_blb");
  ret = buf_mgr.get_buffer(shm_path, log_ctrl);
  ASSERT_EQ(OB_SUCCESS, ret);

  buf_mgr.destroy();

  //invalid argument;
  ret = buf_mgr.get_buffer(NULL, log_ctrl);
  ASSERT_NE(OB_SUCCESS, ret);

  //first get buffer
  ret = buf_mgr.get_buffer(shm_path, log_ctrl);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != log_ctrl);
  log_buf = log_ctrl->base_buf_;
  memcpy(log_ctrl->data_buf_, shm_path, strlen(shm_path));
  log_buf->file_write_pos_.file_offset_ = (uint32_t) strlen(shm_path);

  //second get buffer
  ret = buf_mgr.get_buffer(shm_path, log_ctrl);
  ASSERT_EQ(OB_SUCCESS, ret);

  //double destroy
  buf_mgr.destroy();
  buf_mgr.destroy();

  //try again
  ret = buf_mgr.get_buffer(shm_path, log_ctrl);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != log_ctrl);
  log_buf = log_ctrl->base_buf_;
  ASSERT_TRUE(((int64_t) (log_buf) % (4 * 1024)) == 0);
  ASSERT_TRUE(memcmp(log_ctrl->data_buf_, shm_path, strlen(shm_path)) == 0);
  ASSERT_TRUE(log_buf->file_write_pos_.file_offset_ == strlen(shm_path));
}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
