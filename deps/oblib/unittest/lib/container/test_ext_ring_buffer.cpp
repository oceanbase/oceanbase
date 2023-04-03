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
#include "lib/stat/ob_diagnose_info.h"
#include "lib/container/ob_ext_ring_buffer.h"
#include "lib/container/ob_ext_ring_buffer_impl.h"
#include "lib/coro/testing.h"

using namespace oceanbase;
using namespace common;
using namespace erb;


namespace oceanbase
{
namespace unittest
{

struct PopCondA
{
  bool operator()(int64_t val) { UNUSED(val); return true; }
  bool operator()(int64_t oldptr, int *newptr) { UNUSED(oldptr); UNUSED(newptr); return true; }
};
struct SetCondA
{
  bool operator()(int *oldptr, int *newptr) { UNUSED(oldptr); UNUSED(newptr); return true; }
};
TEST(RINGBUFFER_BASE, debug)
{
  typedef PtrSlot<int> SlotT;
  ObExtendibleRingBufferBase<int*, SlotT> rb_base;

  RingBufferAlloc alloc;
  int64_t begin_sn = 7000;
  int ret = rb_base.init(begin_sn, &alloc);
  EXPECT_EQ(OB_SUCCESS, ret);

  // set and get
  int *ptr = reinterpret_cast<int*>(8);
  int64_t cnt = 200000;
  for (int64_t idx = begin_sn; idx < cnt; ++idx) {
    bool set = false;
    SetCondA setcond;
    ret = rb_base.set(idx, ptr, setcond, set);
    EXPECT_TRUE(set);
    EXPECT_EQ(OB_SUCCESS, ret);
    int *ptr_val = NULL;
    ret = rb_base.get(idx, ptr_val);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(ptr, ptr_val);
  }

  // pop
  PopCondA cond;
  bool popped = false;
  int *ptr_val = NULL;
  for (int64_t idx = begin_sn; idx < cnt; ++idx) {
    ret = rb_base.pop(cond, ptr_val, popped);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(true, popped);
    EXPECT_EQ(ptr, ptr_val) << "current idx:" << idx;
  }


  ret = rb_base.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}


struct PopCondB
{
  bool operator()(int64_t val) { UNUSED(val); return true; }
  bool operator()(int64_t oldptr, int64_t newptr) { UNUSED(oldptr); UNUSED(newptr); return true; }
};
struct SetCondB
{
  bool operator()(int64_t oldval, int64_t newval) { UNUSED(oldval); UNUSED(newval); return true; }
};
TEST(RINGBUFFER_BASE, debug2)
{

  typedef ValSlot<int64_t> SlotT;
  ObExtendibleRingBufferBase<int64_t, SlotT> rb_base;

  RingBufferAlloc alloc;
  int64_t begin_sn = 10000;
  int ret = rb_base.init(begin_sn, &alloc);
  EXPECT_EQ(OB_SUCCESS, ret);

  // set and get
  int64_t cnt = 1000000;
  for (int64_t idx = begin_sn; idx < cnt; ++idx) {
    SetCondB setcond;
    bool set = false;
    ret = rb_base.set(idx, idx, setcond, set);
    EXPECT_TRUE(set);
    EXPECT_EQ(OB_SUCCESS, ret);
    int64_t val = 0;
    ret = rb_base.get(idx, val);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(val, idx);
  }

  // pop
  PopCondB cond;
  bool popped = false;
  int64_t val = 0;
  for (int64_t idx = begin_sn; idx < cnt; ++idx) {
    ret = rb_base.pop(cond, val, popped);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(true, popped);
    EXPECT_EQ(val, idx);
  }


  ret = rb_base.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}

typedef ValSlot<int64_t> SlotT;
typedef ObExtendibleRingBufferBase<int64_t, SlotT> I64RBBASE;
class SetRunnable : public cotesting::DefaultRunnable
{
public:
  void run1() final
  {
    for (int64_t idx = 0; idx < cnt_; ++idx) {
      int64_t sn = s_ + idx;
      EXPECT_EQ(OB_SUCCESS, rb_->set(sn, sn));
    }
  }
  I64RBBASE *rb_;
  int64_t s_;
  int64_t cnt_;
};
class PopRunnable : public cotesting::DefaultRunnable
{
public:
  struct PopCond
  {
    int64_t expect_;
/*
    bool operator()(const int64_t &val)
    {
      return (expect_ == val);
    }
*/
    bool operator()(const int64_t begin_sn, const int64_t val)
    {
      UNUSED(begin_sn);
      return (expect_ == val);
    }
  };
  void run1() final
  {
    PopCond cond;
    int64_t begin_sn = -1;
    while ((begin_sn = rb_->begin_sn()) < (s_ + cnt_)) {
      cond.expect_ = begin_sn;
      int64_t val = 0;
      bool popped = false;
      int ret = rb_->pop(cond, val, popped);
      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        EXPECT_EQ(OB_SUCCESS, ret);
      } else if (!popped) {
        // Pass.
      } else {
        EXPECT_EQ(begin_sn, val);
      }
    }
  }
  I64RBBASE *rb_;
  int64_t s_;
  int64_t cnt_;
};
TEST(RINGBUFFER_BASE, debug3)
{
  const int64_t s = 10000;
  const int64_t setthcnt = 1;
  const int64_t popthcnt = 100;
  const int64_t th_size = 100;

  I64RBBASE rb;
  RingBufferAlloc alloc;
  int ret = rb.init(s, &alloc);
  EXPECT_EQ(OB_SUCCESS, ret);

  SetRunnable set_runnable[setthcnt];
  PopRunnable pop_runnable[popthcnt];
  for (int64_t idx = 0; idx < setthcnt; ++idx) {
    SetRunnable &r = set_runnable[idx];
    r.rb_ = &rb;
    r.s_ = s + idx * th_size;
    r.cnt_ = th_size;
  }
  for (int64_t idx = 0; idx < popthcnt; ++idx) {
    PopRunnable &r = pop_runnable[idx];
    r.rb_ = &rb;
    r.s_ = s;
    r.cnt_ = th_size * setthcnt;
  }

  // Run.
  for (int64_t idx = 0; idx < popthcnt; ++idx) {
    PopRunnable &r = pop_runnable[idx];
    r.start();
  }
  for (int64_t idx = 0; idx < setthcnt; ++idx) {
    SetRunnable &r = set_runnable[idx];
    r.start();
  }

  // Join.
  for (int64_t idx = 0; idx < popthcnt; ++idx) {
    PopRunnable &r = pop_runnable[idx];
    r.wait();
  }
  for (int64_t idx = 0; idx < setthcnt; ++idx) {
    SetRunnable &r = set_runnable[idx];
    r.wait();
  }

  ret = rb.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}

struct CondC
{
  bool operator()(const int *ptr) { UNUSED(ptr); return true; }
  bool operator()(int64_t oldptr, int *newptr) { UNUSED(oldptr); UNUSED(newptr); return true; }
};
TEST(RingBuffer, debug)
{
  typedef ObExtendibleRingBuffer<int> RBT;
  RBT rb;
  rb.init(100);

  int *ptr = reinterpret_cast<int*>(2);
  rb.set(101, ptr);
  rb.set(10100, ptr);
  rb.get(101, ptr);
  rb.get(10100, ptr);
  CondC cond;
  bool popped;
  for (int64_t idx = 100; idx < 10100; ++idx) {
    rb.pop(cond, ptr, popped);
  }

  int64_t b = rb.begin_sn();
  int64_t e = rb.end_sn();
  e = b;
  UNUSED(e);
  rb.destroy();
}

// Hazrd Ptr Tests.
struct TypeA : public erb::HazardBase
{
public:
  virtual int purge()
  {
    return 0;
  }
};
TEST(HazPtr, Basic0)
{
  // Acquire & release.
  erb::HazardPtr<2> hazptr;
  int err = hazptr.init();
  EXPECT_EQ(common::OB_SUCCESS, err);

  TypeA ta;
  TypeA *target = &ta;

  const int64_t lmt = 10000000;
  int64_t cnt = 0;
  int64_t start = ObTimeUtility::current_time();
  while (++cnt < lmt) {
    TypeA *ptr = hazptr.acquire(target, 0);
    EXPECT_EQ(target, ptr);
    hazptr.revert(0);
  }
  int64_t end = ObTimeUtility::current_time();
  LIB_LOG(ERROR, ">>>", K(cnt), K(end - start), K((double)cnt / ((double)(end - start) / 1000000)));

  err = hazptr.destroy();
  EXPECT_EQ(common::OB_SUCCESS, err);
}

struct TypeB : public erb::HazardBase
{
public:
  virtual int purge()
  {
    if (!alloc_) {
      LIB_LOG(ERROR, "err alloc state");
    }
    alloc_ = false;
    return 0;
  }
  bool alloc_;
};
class PurgeTestA : public cotesting::DefaultRunnable
{
public:
  void run1() final
  {
    while (!ATOMIC_LOAD(&acquire_)) { ::usleep(1000000); }
    hazptr_->acquire(data_, 0);
    while (ATOMIC_LOAD(&acquire_)) { ::usleep(1000000); }
    hazptr_->revert(0);
  }
  bool acquire_;
  TypeB *data_;
  erb::HazardPtr<1> *hazptr_;
};

#include <vector>
TEST(HazPtr, Basic1)
{
  // Purge test.

  erb::HazardPtr<1> hazptr;
  int err = hazptr.init();
  EXPECT_EQ(common::OB_SUCCESS, err);

  // Alloc all.
  const int64_t obj_cnt = 1000;
  TypeB objects[obj_cnt];
  for (int64_t idx = 0, cnt = obj_cnt; idx < cnt; ++idx) {
    objects[idx].alloc_ = true;
  }
  // Acquire half, odd ones.
  std::vector<PurgeTestA*> purge_runnable;
  for (int64_t idx = 0, cnt = obj_cnt; idx < cnt; ++idx) {
    if (0 != idx % 2) {
      PurgeTestA *r = new PurgeTestA();
      ATOMIC_STORE(&(r->acquire_), true);
      r->data_ = objects + idx;
      r->hazptr_ = &hazptr;
      purge_runnable.push_back(r);
      r->start();
    }
  }
  for (int64_t idx = 0, cnt = purge_runnable.size(); idx < cnt; ++idx) {
    while (!ATOMIC_LOAD(&(purge_runnable[idx]->acquire_))) { ::usleep(100000); }
  }
  // Retire half, even ones.
  for (int64_t idx = 0, cnt = obj_cnt; idx < cnt; ++idx) {
    if (0 == idx % 2) {
      err = hazptr.retire(&objects[idx]);
      EXPECT_EQ(common::OB_SUCCESS, err);
    }
  }
  // Purge.
  err = hazptr.purge();
  EXPECT_EQ(common::OB_SUCCESS, err);
  // Check them.
  for (int64_t idx = 0, cnt = obj_cnt; idx < cnt; ++idx) {
    if (0 != idx % 2) {
      EXPECT_TRUE(objects[idx].alloc_) << idx;
    }
    else {
      EXPECT_FALSE(objects[idx].alloc_) << idx;
    }
  }
  // Release protection.
  for (int64_t idx = 0, cnt = purge_runnable.size(); idx < cnt; ++idx) {
    ATOMIC_STORE(&(purge_runnable[idx]->acquire_), false);
  }
  // Retire half, odd ones.
  for (int64_t idx = 0, cnt = obj_cnt; idx < cnt; ++idx) {
    if (0 != idx % 2) {
      err = hazptr.retire(&objects[idx]);
      EXPECT_EQ(common::OB_SUCCESS, err);
    }
  }
  // Join.
  for (int64_t idx = 0, cnt = purge_runnable.size(); idx < cnt; ++idx) {
    purge_runnable[idx]->wait();
    delete purge_runnable[idx];
  }
  // Purge.
  err = hazptr.purge();
  EXPECT_EQ(common::OB_SUCCESS, err);
  // Check them.
  for (int64_t idx = 0, cnt = obj_cnt; idx < cnt; ++idx) {
    EXPECT_FALSE(objects[idx].alloc_) << idx;
  }

  err = hazptr.destroy();
  EXPECT_EQ(common::OB_SUCCESS, err);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("info");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
  return 0;
}
