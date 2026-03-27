/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/lock/ob_thread_cond.h"
#include "deps/oblib/src/lib/thread/threads.h"

namespace oceanbase
{
namespace common
{
class TestThreadCondStress: public lib::ThreadPool
{
public:
  TestThreadCondStress(ObThreadCond &cond, const bool is_wait);
  virtual ~TestThreadCondStress();
  void run1() final;
private:
  ObThreadCond &cond_;
  bool is_wait_;
};

TestThreadCondStress::TestThreadCondStress(ObThreadCond &cond, const bool is_wait)
 : cond_(cond),
   is_wait_(is_wait)
{
}

TestThreadCondStress::~TestThreadCondStress()
{
}

void TestThreadCondStress::run1()
{
  int ret = OB_SUCCESS;

  if (is_wait_) {
    while(!has_set_stop()) {
      ret = cond_.lock();
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = cond_.wait();
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = cond_.unlock();
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  } else {
    while(!has_set_stop()) {
      ret = cond_.lock();
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = cond_.signal();
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = cond_.unlock();
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
}

TEST(ObThreadCond, normal)
{
  int ret = OB_SUCCESS;
  ObThreadCond cond;

  //destroy when not init
  cond.destroy();

  //repeatedly init
  ret = cond.init(ObWaitEventIds::UNITEST_COND_WAIT);
  ASSERT_EQ(OB_SUCCESS, ret);

  //empty signal
  ret = cond.lock();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cond.signal();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cond.broadcast();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cond.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);

  //wait timeout
  ret = cond.lock();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cond.wait(1);
  ASSERT_EQ(OB_TIMEOUT, ret);
  ret = cond.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);

  //repeatly destroy
  cond.destroy();
  cond.destroy();
}

TEST(ObThreadCond, stress)
{
  int ret = OB_SUCCESS;
  ObThreadCond cond;
  ret = cond.init(ObWaitEventIds::UNITEST_COND_WAIT);
  ASSERT_EQ(OB_SUCCESS, ret);
  TestThreadCondStress wait_stress(cond, true);
  TestThreadCondStress sig_stress(cond, false);
  wait_stress.set_thread_count(8);
  sig_stress.set_thread_count(4);
  wait_stress.start();
  sig_stress.start();
  sleep(1);
  wait_stress.stop();
  wait_stress.wait();
  sig_stress.stop();
  sig_stress.wait();
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
