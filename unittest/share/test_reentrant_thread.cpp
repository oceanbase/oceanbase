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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "lib/thread/ob_reentrant_thread.h"

namespace oceanbase
{
using namespace common;
namespace share
{

class TestReentrantThread : public ObReentrantThread
{
public:
  TestReentrantThread() : sleeped_cnt_(0) {}

  volatile int64_t sleeped_cnt_;
  void run()
  {
    while (!stop_) {
      usleep(10000);
      sleeped_cnt_++;
    }
  }
  int blocking_run() { return ObReentrantThread::blocking_run(); }
};

TEST(TestReentrantThread, all)
{
  TestReentrantThread thread;
  ASSERT_NE(OB_SUCCESS, thread.create(0));

  const int64_t thread_cnt = ObReentrantThread::TID_ARRAY_DEF_SIZE * 2;
  ASSERT_EQ(OB_SUCCESS, thread.create(thread_cnt));
  ASSERT_EQ(OB_SUCCESS, thread.destroy());

  ASSERT_EQ(OB_SUCCESS, thread.create(thread_cnt));
  ASSERT_EQ(0, thread.running_cnt_);
  ASSERT_EQ(OB_SUCCESS, thread.start());
  usleep(10000);
  ASSERT_EQ(thread_cnt, thread.running_cnt_);
  ASSERT_EQ(OB_SUCCESS, thread.stop());
  ASSERT_EQ(OB_SUCCESS, thread.wait());
  ASSERT_EQ(0, thread.running_cnt_);

  // start again
  ASSERT_EQ(OB_SUCCESS, thread.start());
  usleep(10000);
  ASSERT_EQ(thread_cnt, thread.running_cnt_);

  ASSERT_EQ(OB_SUCCESS, thread.destroy());
  ASSERT_EQ(0, thread.running_cnt_);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
