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

#include <string.h>
#include <gtest/gtest.h>
#include <pthread.h>
#define private public
#include "logservice/palf/fixed_sliding_window.h"
#undef private
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"


namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;

const int64_t getth_cnt = 28;
const int64_t slideth_cnt = 4;
const int64_t trun_cnt = 4;
const int64_t sw_size = 1024;
const int64_t slot_per_th = 2048;

class LogDummyData : public palf::FixedSlidingWindowSlot
{
public:
  LogDummyData(): can_remove(false),log_id(-1), get_cnt_(0) {}
  ~LogDummyData() {}
  bool can_be_slid()
  {
    if (-1 == log_id) {
      return false;
    } else {
      return can_remove;
    }
  }
  void reset()
  {
    log_id = -1;
    can_remove = false;
    get_cnt_ = 0;
  }
  bool can_remove;
  int64_t log_id;
  int64_t get_cnt_;
  common::RWLock lock_;
};

class LogTaskDummyCallBack : public palf::ISlidingCallBack
{
public:
  LogTaskDummyCallBack() {}
  ~LogTaskDummyCallBack() {}

  int sliding_cb(const int64_t sn, const FixedSlidingWindowSlot *data) {
    int ret = common::OB_SUCCESS;
    const LogDummyData *ptr = dynamic_cast<const LogDummyData *>(data);
    int64_t val = ptr->log_id;
    PALF_LOG(INFO, "sliding cb success", K(sn), K(val));
    return ret;
  }
};

class Runnable
{
public:
  Runnable() {}
  virtual ~Runnable() { }
  virtual void routine() = 0;
public:
  void run()
  {
    if (0 != pthread_create(&thread_, NULL, pthread_routine_, this)){
      PALF_LOG_RET(ERROR, OB_ERR_SYS, "create thread fail", K(thread_));
    } else {
      PALF_LOG(INFO, "create thread success", K(thread_));
    }
  }
  void join()
  {
    pthread_join(thread_, NULL);
  }
  static void* pthread_routine_(void *arg)
  {
    Runnable *runnable = static_cast<Runnable*>(arg);
    runnable->routine();
    return NULL;
  }
  pthread_t thread_;
  int64_t th_id;
};

class GetRunnable : public Runnable
{
public:
  virtual void routine()
  {
    for (int64_t i = start_; i < start_ + cnt_; ++i) {
      LogDummyData *val = NULL;
      int ret = common::OB_SUCCESS;
      while (OB_FAIL(sw_->get(i, val))) {
        if (ret == common::OB_ERR_OUT_OF_LOWER_BOUND) {
          break;
        }
        usleep(2000);
      }
      if (ret != common::OB_ERR_OUT_OF_LOWER_BOUND) {
        common::RWLock::WLockGuard guard(val->lock_);
        if (val->log_id == -1) {
          val->log_id = i;
          val->get_cnt_++;
        } else {
          EXPECT_EQ(i, val->log_id);
          val->get_cnt_++;
          if (val->get_cnt_ >= 4) {
            val->can_remove = true;
          }
        }
        PALF_LOG(INFO, "after get", K(i), K(th_id));
        EXPECT_EQ(common::OB_SUCCESS, sw_->revert(i));
      }
    }
  }
  int64_t cnt_;
  int64_t start_;
  FixedSlidingWindow<LogDummyData> *sw_;
};

class SlideRunnable : public Runnable
{
public:
  virtual void routine()
  {
    int ret = common::OB_SUCCESS;
    int64_t timeout_us = 1 * 1000 * 1000;
    LogTaskDummyCallBack cb;
    PALF_LOG(INFO, "before slide", K(th_id));
    int64_t start_sn = sw_->get_begin_sn();
    while (start_sn < 8193) {
      int get_ret = common::OB_SUCCESS;
      LogDummyData *val = NULL;
      get_ret = sw_->get(start_sn, val);
      EXPECT_EQ(common::OB_SUCCESS, sw_->slide(timeout_us, &cb));
      usleep(500);
      if (OB_SUCCESS == get_ret) {
        sw_->revert(start_sn);
      }
      usleep(500);
      start_sn = sw_->get_begin_sn();
    }
  }
  FixedSlidingWindow<LogDummyData> *sw_;
};

class TruncateRunnable : public Runnable
{
public:
  virtual void routine()
  {
    LogDummyData *val = NULL;
    int ret = common::OB_SUCCESS;
    int64_t barrier = t_id_ - 300;
    while (OB_FAIL(sw_->get(barrier, val))) {
        if (ret == common::OB_ERR_OUT_OF_LOWER_BOUND) {
          break;
        }
        usleep(1000);
    }
    if (common::OB_ERR_OUT_OF_LOWER_BOUND != ret) {
      {
        common::RWLock::RLockGuard guard(val->lock_);
        if (val->can_remove == false) {
          EXPECT_EQ(common::OB_SUCCESS, sw_->truncate(t_id_));
        }
        EXPECT_EQ(common::OB_SUCCESS, sw_->revert(barrier));
      }
      // do get after truncating
      GetRunnable get_threads[getth_cnt];
      for (int64_t i = 0; i < getth_cnt; ++i) {
        GetRunnable &get_th = get_threads[i];
        get_th.cnt_ = slot_per_th;
        get_th.start_ = barrier;
        get_th.sw_ = sw_;
        get_th.th_id = i;
      }
      for (int64_t i = 0; i < getth_cnt; ++i) {
        PALF_LOG(INFO, "run get thread:", K(i));
        get_threads[i].run();
      }
      for (int64_t i = 0; i < getth_cnt; ++i) {
        PALF_LOG(INFO, "join get thread:", K(i));
        get_threads[i].join();
      }
    }
  }
  int64_t t_id_;
  FixedSlidingWindow<LogDummyData> *sw_;
};


// single thread function test
TEST(TestBaseSlidingWindow, test_base_sliding_window)
{
  // sliding window construct and destruct
  common::ObILogAllocator *alloc_mgr = NULL;
  ObTenantMutilAllocatorMgr::get_instance().init();
  EXPECT_EQ(common::OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(common::OB_SERVER_TENANT_ID, alloc_mgr));
  FixedSlidingWindow<LogDummyData> sw0;
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, sw0.init(10, 100, alloc_mgr));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, sw0.init(-2, 100, alloc_mgr));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, sw0.init(10, 1, alloc_mgr));
  sw0.destroy();

  FixedSlidingWindow<LogDummyData> sw1;
  EXPECT_EQ(common::OB_SUCCESS, sw1.init(10, 8, alloc_mgr));
  sw1.destroy();

  // get and revert
  FixedSlidingWindow<LogDummyData> sw2;
  EXPECT_EQ(common::OB_SUCCESS, sw2.init(10, 8, alloc_mgr));
  for(int64_t i = 8; i < 22; ++i) {
    LogDummyData *val(NULL);
    if (i >= 10 && i < 18) {
      EXPECT_EQ(common::OB_SUCCESS, sw2.get(i, val));
      EXPECT_NE((LogDummyData*)NULL, val);
      EXPECT_EQ(common::OB_SUCCESS, sw2.revert(i));
    } else if (i < 10) {
      EXPECT_EQ(common::OB_ERR_OUT_OF_LOWER_BOUND, sw2.get(i, val));
      EXPECT_EQ(NULL, val);
    } else {
      EXPECT_EQ(common::OB_ERR_OUT_OF_UPPER_BOUND, sw2.get(i, val));
      EXPECT_EQ(NULL, val);
      EXPECT_EQ(common::OB_ERR_UNEXPECTED, sw2.revert(i));
    }
  }
  sw2.destroy();

  // get, slide and revert
  FixedSlidingWindow<LogDummyData> sw3;
  sw3.init(10, 8, alloc_mgr);
  for (int64_t i = 10; i < 18; ++i) {
    LogDummyData *val(NULL);
    EXPECT_EQ(common::OB_SUCCESS, sw3.get(i, val));
    EXPECT_NE((LogDummyData*)NULL, val);
    val->log_id = i;
    val->can_remove = true;
    if (i != 17) {
      EXPECT_EQ(common::OB_SUCCESS, sw3.revert(i));
    }
  }
  LogDummyData *val(NULL);
  int64_t timeout_us = 5000 * 1000;
  LogTaskDummyCallBack cb;
  EXPECT_EQ(common::OB_SUCCESS, sw3.slide(timeout_us, &cb));
  // begin: 18, end:25
  EXPECT_EQ(18, sw3.get_begin_sn());
  EXPECT_EQ(common::OB_ERR_OUT_OF_UPPER_BOUND, sw3.get(25, val));
  // begin: 18, end: 26
  EXPECT_EQ(common::OB_SUCCESS, sw3.revert(17));
  EXPECT_EQ(18, sw3.get_begin_sn());
  EXPECT_EQ(common::OB_ERR_UNEXPECTED, sw3.revert(26));

  // forward truncate
  FixedSlidingWindow<LogDummyData> sw4;
  const int64_t sw_size = 32;
  sw4.init(10, sw_size, alloc_mgr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, sw4.truncate_and_reset_begin_sn(5));
  LogDummyData *tr_val(NULL);
  EXPECT_EQ(OB_SUCCESS, sw4.get(10, tr_val));
  tr_val->log_id = 10;
  tr_val->can_remove = true;
  EXPECT_EQ(OB_SUCCESS, sw4.slide(timeout_us, &cb));
  EXPECT_EQ(11, sw4.get_begin_sn());
  EXPECT_EQ(OB_ERR_UNEXPECTED, sw4.truncate_and_reset_begin_sn(20));
  EXPECT_EQ(OB_SUCCESS, sw4.revert(10));

  EXPECT_EQ(OB_SUCCESS, sw4.truncate_and_reset_begin_sn(50));
  EXPECT_EQ(50, sw4.get_begin_sn());
  EXPECT_EQ(50 + sw_size, sw4.end_sn_);
  EXPECT_EQ(OB_SUCCESS, sw4.truncate_and_reset_begin_sn(50));
  EXPECT_EQ(50, sw4.get_begin_sn());
  EXPECT_EQ(50 + sw_size, sw4.end_sn_);
  EXPECT_EQ(OB_SUCCESS, sw4.truncate_and_reset_begin_sn(60));
  EXPECT_EQ(60, sw4.get_begin_sn());
  EXPECT_EQ(60 + sw_size, sw4.end_sn_);
}


// test get, revert, slide and truncate concurrent
TEST(TestConcurrentSlidingWindow, test_concurrent_sliding_window)
{
  // logid range[1, 8192], each thread 2048
  // 0-3 threads [1, 2048], 4-7 threads [1025, 3072], ...

  common::ObILogAllocator *alloc_mgr = NULL;
  ObTenantMutilAllocatorMgr::get_instance().init();
  EXPECT_EQ(common::OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(common::OB_SERVER_TENANT_ID, alloc_mgr));
  FixedSlidingWindow<LogDummyData> sw;
  EXPECT_EQ(common::OB_SUCCESS, sw.init(1, sw_size, alloc_mgr));

  GetRunnable get_threads[getth_cnt];
  SlideRunnable slide_threads[slideth_cnt];
  TruncateRunnable trun_threads[trun_cnt];
  for (int64_t i = 0; i < getth_cnt; ++i) {
    GetRunnable &get_th = get_threads[i];
    get_th.cnt_ = slot_per_th;
    get_th.start_ = ((i/4) * sw_size) + 1;
    get_th.sw_ = &sw;
    get_th.th_id = i;
  }

  for (int64_t i = 0; i < slideth_cnt; ++i) {
    SlideRunnable &slide_th = slide_threads[i];
    slide_th.sw_ = &sw;
    slide_th.th_id = i;
  }

  for (int64_t i = 0; i < trun_cnt; ++i) {
    TruncateRunnable &trun_th = trun_threads[i];
    trun_th.sw_ = &sw;
    trun_th.t_id_ = i * slot_per_th + 500;
    trun_th.th_id = i;
  }

  for (int64_t i = 0; i < getth_cnt; ++i) {
    PALF_LOG(INFO, "run get thread:", K(i));
    get_threads[i].run();
  }
  for (int64_t i = 0; i < slideth_cnt; ++i) {
    PALF_LOG(INFO, "run slide thread:", K(i));
    slide_threads[i].run();
  }
  for (int64_t i = 0; i < trun_cnt; ++i) {
    PALF_LOG(INFO, "run truncate thread:", K(i));
    trun_threads[i].run();
  }

  for (int64_t i = 0; i < getth_cnt; ++i) {
    PALF_LOG(INFO, "join get thread:", K(i));
    get_threads[i].join();
  }
  for (int64_t i = 0; i < slideth_cnt; ++i) {
    PALF_LOG(INFO, "join slide thread:", K(i));
    slide_threads[i].join();
  }
  for (int64_t i = 0; i < trun_cnt; ++i) {
    PALF_LOG(INFO, "join truncate thread:", K(i));
    trun_threads[i].join();
  }

  sw.destroy();
}

// reproduce bug
// TEST(TestBaseSlidingWindow, test_concurrent_get_slide)
// {
//   // sliding window construct and destruct
//   common::ObILogAllocator *alloc_mgr = NULL;
//   ObTenantMutilAllocatorMgr::get_instance().init();
//   EXPECT_EQ(common::OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(common::OB_SERVER_TENANT_ID, alloc_mgr));

//   // 1. set [0, 128) can slide
//   const int64_t size = 128;
//   FixedSlidingWindow<LogDummyData> sw3;
//   EXPECT_EQ(OB_SUCCESS, sw3.init(0, size, alloc_mgr));
//   for (int64_t i = 0; i < size; ++i) {
//     LogDummyData *val(NULL);
//     EXPECT_EQ(common::OB_SUCCESS, sw3.get(i, val));
//     EXPECT_NE((LogDummyData*)NULL, val);
//     val->log_id = i;
//     val->can_remove = true;
//     EXPECT_EQ(common::OB_SUCCESS, sw3.revert(i));
//   }

//   // 2. create and run a thread to wait in get(10)
//   PALF_FIXED_SW_GET_HUNG = true;
//   GetRunnable get_thread;
//   get_thread.cnt_ = 1;
//   get_thread.start_ = 10;
//   get_thread.sw_ = &sw3;
//   get_thread.run();
//   sleep(1);

//   // 3. slide to [128, 256)
//   LogTaskDummyCallBack cb;
//   EXPECT_EQ(common::OB_SUCCESS, sw3.slide(10 * 1000 * 1000, &cb));
//   EXPECT_EQ(128, sw3.get_begin_sn());
//   EXPECT_EQ(256, sw3.get_end_sn());

//   // 4. slide to [256, 384)
//   for (int64_t i = size; i < 2 * size; ++i) {
//     LogDummyData *val(NULL);
//     EXPECT_EQ(common::OB_SUCCESS, sw3.get(i, val));
//     EXPECT_NE((LogDummyData*)NULL, val);
//     val->log_id = i;
//     val->can_remove = true;
//     EXPECT_EQ(common::OB_SUCCESS, sw3.revert(i));
//   }
//   EXPECT_EQ(common::OB_SUCCESS, sw3.slide(10 * 1000 * 1000, &cb));
//   EXPECT_EQ(2 * size, sw3.get_begin_sn());
//   EXPECT_EQ(3 * size, sw3.get_end_sn());

//   // 5. resume the thread and join
//   PALF_FIXED_SW_GET_HUNG = false;
//   sleep(1);
//   // 6. continue to slide
//   for (int64_t j = 2; j < 100; j++) {
//     for (int64_t i = j * size; i < (j+1) * size; ++i) {
//       LogDummyData *val(NULL);
//       EXPECT_EQ(common::OB_SUCCESS, sw3.get(i, val));
//       EXPECT_NE((LogDummyData*)NULL, val);
//       val->log_id = i;
//       val->can_remove = true;
//       EXPECT_EQ(common::OB_SUCCESS, sw3.revert(i));
//     }
//     EXPECT_EQ(common::OB_SUCCESS, sw3.slide(10 * 1000 * 1000, &cb));
//     EXPECT_EQ((j+1) * size, sw3.get_begin_sn());
//     EXPECT_EQ((j+2) * size, sw3.get_end_sn());
//   }
//   get_thread.join();
//   sw3.destroy();
// }

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_fixed_sliding_window.log");
  OB_LOGGER.set_file_name("test_fixed_sliding_window.log", true, true, "test_fixed_sliding_window.rs.log");
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_fixed_sliding_window");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
