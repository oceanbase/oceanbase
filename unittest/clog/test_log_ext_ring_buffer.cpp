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

#include <cstdlib>
#include <pthread.h>

#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"

#include "clog/ob_log_ext_ring_buffer.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase {
namespace unittest {

class Runnable {
public:
  virtual ~Runnable()
  {}
  virtual void routine() = 0;

public:
  void run()
  {
    pthread_create(&thread_, NULL, pthread_routine_, this);
  }
  void join()
  {
    pthread_join(thread_, NULL);
  }

private:
  static void* pthread_routine_(void* arg)
  {
    Runnable* runnable = static_cast<Runnable*>(arg);
    runnable->routine();
    return NULL;
  }
  pthread_t thread_;
};

class Data : public clog::ObILogExtRingBufferData {
public:
  virtual ~Data()
  {}
  virtual void destroy()
  {
    delete this;
  }
  virtual bool can_overwrite(const ObILogExtRingBufferData* data)
  {
    UNUSED(data);
    return true;
  }
  virtual bool can_be_removed()
  {
    return true;
  }
  virtual int on_pop()
  {
    return OB_SUCCESS;
  }
  int64_t data_;
};

class SetRunnable : public Runnable {
public:
  virtual void routine()
  {
    for (int64_t idx = 0; idx < cnt_; ++idx) {
      int64_t sn = s_ + idx;
      Data* d = new Data();
      rb_->set(sn, d);
    }
  }
  clog::ObLogExtRingBuffer* rb_;
  int64_t s_;
  int64_t cnt_;
};
class PopRunnable : public Runnable {
public:
  virtual void routine()
  {
    // int64_t begin_sn = -1;
    // while ((begin_sn = rb_->get_start_id()) < (s_ + cnt_)) {
    //  int ret = rb_->pop();
    //  UNUSED(ret);
    //}
  }
  clog::ObLogExtRingBuffer* rb_;
  int64_t s_;
  int64_t cnt_;
};
TEST(RINGBUFFER_BASE, debug3)
{
  const int64_t s = 10000;
  const int64_t setthcnt = 16;
  const int64_t popthcnt = 16;
  const int64_t th_size = 500000;

  int ret = OB_SUCCESS;
  clog::ObLogExtRingBuffer rb;
  ret = rb.init(s);
  EXPECT_EQ(OB_SUCCESS, ret);

  SetRunnable set_runnable[setthcnt];
  PopRunnable pop_runnable[popthcnt];
  for (int64_t idx = 0; idx < setthcnt; ++idx) {
    SetRunnable& r = set_runnable[idx];
    r.rb_ = &rb;
    r.s_ = s + idx * th_size;
    r.cnt_ = th_size;
  }
  for (int64_t idx = 0; idx < popthcnt; ++idx) {
    PopRunnable& r = pop_runnable[idx];
    r.rb_ = &rb;
    r.s_ = s;
    r.cnt_ = th_size * setthcnt;
  }

  // Run.
  for (int64_t idx = 0; idx < popthcnt; ++idx) {
    PopRunnable& r = pop_runnable[idx];
    r.run();
  }
  for (int64_t idx = 0; idx < setthcnt; ++idx) {
    SetRunnable& r = set_runnable[idx];
    r.run();
  }

  // Join.
  for (int64_t idx = 0; idx < popthcnt; ++idx) {
    PopRunnable& r = pop_runnable[idx];
    r.join();
  }
  for (int64_t idx = 0; idx < setthcnt; ++idx) {
    SetRunnable& r = set_runnable[idx];
    r.join();
  }

  ret = rb.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}

/*
 * Test Truncate.
 */
TEST(RingBuffer, TruncateTest1)
{

  int ret = OB_SUCCESS;
  clog::ObLogExtRingBuffer rb;
  ret = rb.init(0);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Ringbuffer empty and truncate to 5.
  ret = rb.truncate(5);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(5, rb.get_start_id());

  // Insert 5-10 and trucate to 11.
  {
    for (int64_t sn = 5; sn <= 10; ++sn) {
      Data* d = new Data();
      ret = rb.set(sn, d);
      EXPECT_EQ(OB_SUCCESS, ret);
    }
    ret = rb.truncate(11);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(11, rb.get_start_id());
  }

  // Insert 11-20 and trucate to 16.
  {
    for (int64_t sn = 11; sn <= 20; ++sn) {
      Data* d = new Data();
      ret = rb.set(sn, d);
      EXPECT_EQ(OB_SUCCESS, ret);
    }
    ret = rb.truncate(16);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(16, rb.get_start_id());
  }

  // Truncate to 1001.
  ret = rb.truncate(1001);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1001, rb.get_start_id());

  ret = rb.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc, argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  testing::FLAGS_gtest_filter = "*Truncate*";
  return RUN_ALL_TESTS();
}
