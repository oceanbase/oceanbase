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

#include <pthread.h>
#include "lib/lock/ob_latch.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/random/ob_random.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/thread/thread_pool.h"
#include "gtest/gtest.h"
#define private public
#include "lib/worker.h"

namespace oceanbase
{
namespace common
{

int k = 0;
const int max_cnt = 1000;

template <class MUTEX>
class TestLatchContend: public lib::ThreadPool
{
public:
  TestLatchContend() {}
  virtual ~TestLatchContend() {}
  void run1() final
  {
    int i = 0;
    while(1) {
      {
        mutex_.lock();
        for (int j = 0; j < 10; ++j) {
          ObRandom::rand(1, 1000);
        }
        if (i ==  max_cnt) {
          break;
        }
        ++i;
        ++k;
        mutex_.unlock();
      }
    }
    mutex_.unlock();
  }

private:
  MUTEX mutex_;
};

const static int64_t MUTEX_THR = 16;
TEST(ObLatch, ob_mutex_contend)
{
  k = 0;
  TestLatchContend<lib::ObMutex> stress;
  stress.set_thread_count(MUTEX_THR);
  stress.start();
  stress.wait();
  ASSERT_TRUE(k == MUTEX_THR * max_cnt);
}

struct RWLockTestParam
{
  RWLockTestParam()
      : cycle_(0),
        ratio_(0),
        r_load_(0),
        w_load_(0),
        w2r_ratio_(INT64_MAX)
  {}

  RWLockTestParam(const int32_t cycle,
      const int32_t ratio,
      const int32_t r_load,
      const int32_t w_load,
      const int64_t w2r_ratio = INT64_MAX)
      : cycle_(cycle),
        ratio_(ratio),
        r_load_(r_load),
        w_load_(w_load),
        w2r_ratio_(w2r_ratio)
  {
  }
  int32_t cycle_;
  int32_t ratio_;
  int32_t r_load_;
  int32_t w_load_;
  int64_t w2r_ratio_;
};

class RWLockWithTimeout
{
  public:
    RWLockWithTimeout(bool has_timeout,
        uint32_t latch_id = ObLatchIds::DEFAULT_SPIN_RWLOCK)
        : latch_(), has_timeout_(has_timeout), latch_id_(latch_id)
    {
    }
    ~RWLockWithTimeout()
    {
    }
  public:
    int rdlock() { return latch_.rdlock(latch_id_); }
    int wrlock()
    {
      // timeout 100us
      int64_t abs_lock_timeout = has_timeout_ ? common::ObTimeUtility::current_time() + 100 : INT64_MAX;
      return latch_.wrlock(latch_id_, abs_lock_timeout);
    }
    int wr2rdlock() { return latch_.wr2rdlock(); }
    int unlock() { return latch_.unlock(); }
  private:
    ObLatch latch_;
    bool has_timeout_;
    uint32_t latch_id_;
};

DEFINE_HAS_MEMBER(wr2rdlock)

int64_t k_rd = 0;
template <class RWLOCK>
class TestRWLockContend: public lib::ThreadPool
{
public:
  explicit TestRWLockContend(const bool has_timeout = false) : lock_(has_timeout) {}
  virtual ~TestRWLockContend() {}

  void run1() final
  {
    int ret = OB_SUCCESS;
    int i = 0;
    while(i < param_.cycle_) {
      if (i % param_.ratio_ != 0) { // read
        if (OB_SUCC(lock_.rdlock())) {
          for (int j = 0; j < param_.r_load_; j++) {
            ObRandom::rand(1,1000);
          }
          lock_.unlock();
        }
      } else { // write
        if (OB_SUCC(lock_.wrlock())) {
          k_rd++;
          for (int j = 0; j < param_.w_load_; j++) {
            ObRandom::rand(1,1000);
          }
          if (0 == i % param_.w2r_ratio_) {
            ASSERT_EQ(OB_SUCCESS, wr2rdlock_wrap(lock_, BoolType<HAS_MEMBER(RWLOCK, wr2rdlock)>()));
          }
          lock_.unlock();
        }
      }
      ++i;
    }
  }

  inline int wr2rdlock_wrap(RWLOCK &lock, FalseType)
  {
    UNUSED(lock);
    return OB_SUCCESS;
  }

  inline int wr2rdlock_wrap(RWLOCK &lock, TrueType)
  {
    return lock.wr2rdlock();
  }

  void set_param(const RWLockTestParam param)
  {
    param_ = param;
  }

private:
  RWLOCK lock_;
  ObRandom rand_;
  RWLockTestParam param_;
};

const static int32_t cycles = 1000;
int32_t ratios[] = {4};
int32_t r_loads[] = {50};
int32_t w_loads[] = {100};
const static int64_t MAX_RW_TH = 16;

TEST(ObLatch, ob_latch_rw_contend)
{
  k_rd = 0;
  TestRWLockContend<common::SpinRWLock> stress;
  stress.set_thread_count(MAX_RW_TH);
  stress.set_param(RWLockTestParam(cycles, ratios[0], r_loads[0], w_loads[0]));
  stress.start();
  stress.wait();
  ASSERT_TRUE(k_rd == cycles * MAX_RW_TH / ratios[0]);
}

struct CriticalSec
{
  CriticalSec() {}
  ~CriticalSec() {}
  union {
    SpinRWLock lock_;
    char *data_;
  };
  int64_t ref_cnt_;
};

static const int64_t core_test_th_cnt = 4;

class ObLatchTestRun : public lib::ThreadPool
{
public:
  ObLatchTestRun()
  {
    buf_ = (char *)malloc(sizeof(CriticalSec) * 2);
    memset(buf_, 0, sizeof(CriticalSec) * 2);
    cs_ = new (buf_) CriticalSec();
    pthread_barrier_init(&start_, NULL, core_test_th_cnt);
  }
  virtual ~ObLatchTestRun() { free(buf_); }
  void run1() final;
  void set_ref_cnt(const int64_t ref_cnt);
  CriticalSec *get_sec() const { return cs_; }
  char *get_addr() { return out_; }

private:
  char *buf_;
  CriticalSec *cs_;
  char out_[10] = "Great!";
  pthread_barrier_t start_;
};

void ObLatchTestRun::set_ref_cnt(const int64_t ref_cnt)
{
  cs_->ref_cnt_ = ref_cnt;
}

static const int64_t for_cnt = 5;

void ObLatchTestRun::run1()
{
  pthread_barrier_wait(&start_);
  bool release = false;

  for (int i = 0; i < for_cnt; i++) {
    cs_->lock_.wrlock();
    cs_->ref_cnt_--;
    if (cs_->ref_cnt_ <= 0) {
      release = true;
    }
    cs_->lock_.unlock();
    if (release) {
      cs_->data_ = out_;
    }
  }
}

TEST(ObLatch, core)
{
  ObLatchTestRun test;
  test.set_thread_count(core_test_th_cnt);
  test.set_ref_cnt(core_test_th_cnt * for_cnt);
  test.start();
  test.wait();
  ASSERT_TRUE(test.get_sec()->data_ == test.get_addr());
}

TEST(ObLatch, timeout)
{
  TestRWLockContend<RWLockWithTimeout> stress(true);
  stress.set_thread_count(10);
  stress.set_param(RWLockTestParam(10, 2, 10, 10));
  stress.start();
  stress.wait();
}

TEST(ObLatch,wr2rdlock)
{
  TestRWLockContend<RWLockWithTimeout> stress;
  stress.set_thread_count(10);
  stress.set_param(RWLockTestParam(10, 2, 10, 10, 1));
  stress.start();
  stress.wait();
}

TEST(ObLatch, invaid_unlock)
{
  lib::ObMutex mutex;
  common::SpinRWLock rwlock;

  ASSERT_EQ(OB_ERR_UNEXPECTED, mutex.unlock());
  ASSERT_EQ(OB_ERR_UNEXPECTED, rwlock.unlock());
}

#ifdef ENABLE_LATCH_DIAGNOSE
void *run(void *arg)
{
  ObLockDiagnose &ld = *(ObLockDiagnose*)arg;
  ObLDHandle handle;
  ld.lock(ObLDLockType::rdlock, handle);
  ld.print();

  handle.reset();
  std::cout << "end of run" << std::endl;
  return nullptr;
}

TEST(ObLatch, diagnose)
{
  ObLockDiagnose ld;

  ObLDHandle handles[2];
  ld.lock(ObLDLockType::rdlock, handles[0]);
  ld.lock(ObLDLockType::rdlock, handles[1]);

  handles[0].reset();

  pthread_t th;
  pthread_create(&th, nullptr, run, &ld);
  pthread_join(th, nullptr);

  handles[1].reset();

  ObLDHandle handle;
  ld.lock(ObLDLockType::wrlock, handle);

  handle.reset();

  ld.print();

  std::cout << "done" << std::endl;

}
#endif

}
}

int main(int argc, char **argv)
{
  ::oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
