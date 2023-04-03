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
#include <pthread.h>
#define private public
#include "lib/lock/ob_latch.h"
#undef private
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/stat/ob_session_stat.h"
#include "share/cache/ob_kv_storecache.h"
#include "lib/random/ob_random.h"
namespace oceanbase
{
namespace common
{
class TestLatchStress: public share::ObThreadPool
{
public:
  TestLatchStress();
  virtual ~TestLatchStress();
  virtual void run1();
  int64_t round0_;
  int64_t value0_;
  ObLatch lock0_;
  obsys::ObRWLock lock_;
  pthread_mutex_t mutex_;
};

TestLatchStress::TestLatchStress()
  : round0_(0),
    value0_(0),
    lock0_(),
    lock_()
{
  pthread_mutex_init(&mutex_, NULL);
}

TestLatchStress::~TestLatchStress()
{
}

void TestLatchStress::run(obsys::CThread *thread, void *arg)
{

  int ret = OB_SUCCESS;
  int64_t timeout = ObTimeUtility::current_time();

  if (0 == ((uint64_t)arg) % 2) {
    for (int i = 0; i < 10000; i++) {
      timeout = ObTimeUtility::current_time() + 10;
      ret = lock0_.wrlock(ObLatchIds::ROW_LOCK, timeout);
      if (OB_SUCC(ret)) {
        usleep(static_cast<int32_t>(ObRandom::rand(0, 10)));
        if (0 == ((uint64_t)arg) % 4) {
          ret = lock0_.wr2rdlock();
          ASSERT_EQ(OB_SUCCESS, ret);
          usleep(1);
        }
        lock0_.unlock();
      }

      //ret = lock1_.wrlock(1, INT64_MAX);
      //ASSERT_EQ(OB_SUCCESS, ret);
      //lock1_.wrunlock(1);

      //ret = pthread_mutex_lock(&mutex_);
      //ASSERT_EQ(0, ret);
      //pthread_mutex_unlock(&mutex_);

      //obsys::ObWLockGuard guard(lock_);
      //ASSERT_EQ(OB_SUCCESS, ret);
    }
  } else {
    for (int i = 0; i < 10000; i++) {

      ret = lock0_.rdlock(ObLatchIds::ROW_LOCK);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = lock0_.rdlock(ObLatchIds::ROW_LOCK);
      ASSERT_EQ(OB_SUCCESS, ret);
      lock0_.unlock();
      lock0_.unlock();

      //ret = lock1_.rdlock(INT64_MAX);
      //ASSERT_EQ(OB_SUCCESS, ret);
      //lock1_.rdunlock();

      //ret = pthread_mutex_lock(&mutex_);
      //ASSERT_EQ(0, ret);
      //pthread_mutex_unlock(&mutex_);

      //obsys::ObRLockGuard guard(lock_);
      //ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
}

TEST(ObLatch, invalid)
{
  int ret = OB_SUCCESS;
  ObLatch latch;
  uint32_t uid = 0;

  ret = latch.try_rdlock(ObLatchIds::LATCH_END);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = latch.try_wrlock(ObLatchIds::LATCH_END, 0);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = latch.try_wrlock(0, &uid);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = latch.rdlock(ObLatchIds::LATCH_END);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = latch.wrlock(ObLatchIds::LATCH_END);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = latch.wr2rdlock();
  ASSERT_NE(OB_SUCCESS, ret);
  ret = latch.unlock();
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObLatch, normal)
{
  int ret = OB_SUCCESS;
  ObLatch latch;
  uint32_t wid = 0;

  ret = latch.rdlock(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = latch.rdlock(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  wid = latch.get_wid();
  ASSERT_EQ(0, wid);
  ASSERT_TRUE(latch.is_locked());
  ASSERT_TRUE(latch.is_rdlocked());
  ret = latch.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = latch.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_FALSE(latch.is_locked());
  ASSERT_FALSE(latch.is_rdlocked());

  ret = latch.wrlock(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  wid = latch.get_wid();
  ASSERT_EQ(GETTID(), wid);
  ASSERT_TRUE(latch.is_locked());
  ASSERT_FALSE(latch.is_rdlocked());
  ret = latch.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);

  //lock timeout
  ret = latch.wrlock(ObLatchIds::DEFAULT_MUTEX);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = latch.wrlock(ObLatchIds::DEFAULT_MUTEX, 1);
  ASSERT_EQ(OB_TIMEOUT, ret);
  ret = latch.rdlock(ObLatchIds::DEFAULT_MUTEX, 1);
  ASSERT_EQ(OB_TIMEOUT, ret);
  ret = latch.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);

  //lock bound
  for (int64_t i = 0; i < ObLatch::MAX_READ_LOCK_CNT; ++i) {
    ret = latch.rdlock(ObLatchIds::DEFAULT_MUTEX);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = latch.rdlock(ObLatchIds::DEFAULT_MUTEX);
  ASSERT_NE(OB_SUCCESS, ret);
  for (int64_t i = 0; i < ObLatch::MAX_READ_LOCK_CNT; ++i) {
    ret = latch.unlock();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = 0; i < ObLatch::MAX_READ_LOCK_CNT; ++i) {
    ret = latch.try_rdlock(ObLatchIds::DEFAULT_MUTEX);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = latch.try_rdlock(ObLatchIds::DEFAULT_MUTEX);
  ASSERT_NE(OB_SUCCESS, ret);
  for (int64_t i = 0; i < ObLatch::MAX_READ_LOCK_CNT; ++i) {
    ret = latch.unlock();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  //just print longer than 1s log
  ObSessionStatEstGuard est_guard(1, 1);
  ret = latch.wrlock(ObLatchIds::DEFAULT_MUTEX);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = latch.wrlock(ObLatchIds::DEFAULT_MUTEX, ObTimeUtility::current_time() + 2000 * 1000);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = latch.unlock();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST(ObLatch, multithread)
{
  TestLatchStress stress;
  stress.set_thread_count(32);
  uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
  stress.start();
  stress.wait();
  uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  printf("time: %ld us\n", (end_time - begin_time));
}


}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
