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

#ifndef OCEANBASE_MVCC_OB_ROW_LATCH_
#define OCEANBASE_MVCC_OB_ROW_LATCH_
#include "lib/lock/ob_latch.h"
#include "lib/stat/ob_latch_define.h"

namespace oceanbase
{
namespace memtable
{
#define USE_SIMPLE_ROW_LATCH 1
#if USE_SIMPLE_ROW_LATCH
struct ObRowLatch
{
  ObRowLatch(): locked_(false) {}
  ~ObRowLatch() {}
  struct Guard
  {
    Guard(ObRowLatch& host): host_(host) { host.lock();}
    ~Guard() { host_.unlock(); }
    ObRowLatch& host_;
  };
  bool is_locked() const { return ATOMIC_LOAD(&locked_); }
  bool try_lock() { return !ATOMIC_TAS(&locked_, true); }
  void lock() {
    while(!try_lock())
      ;
  }
  void unlock() { ATOMIC_STORE(&locked_, false); }
  bool locked_;
};
#else
struct ObRowLatch
{
ObRowLatch(): latch_() {}
~ObRowLatch() {}
  struct Guard
  {
  Guard(ObRowLatch& host): host_(host) { host.lock();}
   ~Guard() { host_.unlock(); }
    ObRowLatch& host_;
  };
  bool is_locked() const { return latch_.is_locked(); }
  bool try_lock()
  {
    //try_wrlock成功之后返回OB_SUCCESS;
    return (common::OB_SUCCESS == latch_.try_wrlock(common::ObLatchIds::ROW_CALLBACK_LOCK));
  }
  void lock() { (void)latch_.wrlock(common::ObLatchIds::ROW_CALLBACK_LOCK); }
  void unlock() { (void)latch_.unlock(); }
  common::ObLatch latch_;
};
#endif
typedef ObRowLatch::Guard ObRowLatchGuard;
}; // end namespace mvcc
}; // end namespace oceanbase

#endif /* OCEANBASE_MVCC_OB_ROW_LATCH_ */
