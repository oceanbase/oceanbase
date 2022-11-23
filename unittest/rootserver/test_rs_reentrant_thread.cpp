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

#define USING_LOG_PREFIX RS

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#define private public
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/ob_cluster_version.h"
#include "rootserver/ob_rs_reentrant_thread.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;

class ObTestThread : public ObRsReentrantThread
{
public: 
  ObTestThread() : ObRsReentrantThread(true) {}
  virtual int blocking_run() override { BLOCKING_RUN_IMPLEMENT(); }

  virtual void run3() override
  {
    while (!has_set_stop()) {
      ::usleep(10000);
    }
  }
};

class TestRsReentrantThread : public ::testing::Test
{
public:
  TestRsReentrantThread() {}
  virtual ~TestRsReentrantThread() {}
  virtual void SetUp() {}
  virtual void TearDown() {}

  int start_thread(int64_t thread_num)
  {
    int ret = OB_SUCCESS;
    threads_.reset();

    for (int64_t i = 0; (i < thread_num) && OB_SUCC(ret); ++i) {
      ObTestThread *thread = new ObTestThread();
      if (OB_FAIL(thread->create(1, "test"))) {
        LOG_WARN("fail to create thread", KR(ret));
      } else if (OB_FAIL(thread->start())) {
        LOG_WARN("fail to start thread", KR(ret));
      } else if (OB_FAIL(threads_.push_back(thread))) {
        LOG_WARN("fail to push back", KR(ret));
      } 
    }

    return ret;
  }

  int stop_thread()
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; (i < threads_.count()) && OB_SUCC(ret); ++i) {
      threads_[i]->stop();
      threads_[i]->wait();
      if (OB_FAIL(threads_[i]->destroy())) {
        LOG_WARN("fail to destroy");
      }
      delete threads_[i];
    }
    threads_.reset();
    return ret;
  }
  
  ObSEArray<ObTestThread *, 17> threads_;
};

TEST_F(TestRsReentrantThread, common)
{
  for (int64_t i = 1; i < 200; i = i + 7) {
    ASSERT_EQ(OB_SUCCESS, start_thread(i));
    ::usleep(100000);
    ASSERT_EQ(i, ObRsReentrantThread::check_thread_set_.get_thread_count());
    ObRsReentrantThread::check_thread_set_.loop_operation(ObRsReentrantThread::check_alert);
    ASSERT_EQ(OB_SUCCESS, stop_thread());
    ::usleep(100000);
    ObRsReentrantThread::check_thread_set_.loop_operation(ObRsReentrantThread::check_alert);
    ASSERT_EQ(0, ObRsReentrantThread::check_thread_set_.get_thread_count());
  }
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}
