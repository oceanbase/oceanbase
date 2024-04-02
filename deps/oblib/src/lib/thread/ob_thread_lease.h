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

#ifndef OCEANBASE_OB_THREAD_LEASE_H__
#define OCEANBASE_OB_THREAD_LEASE_H__

#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{

/* state transfer: IDLE <------> HANDLING <------> READY
 *  - IDLE:     idle state, no thread handling
 *  - HANDLING: thread is handling
 *  - READY:    thread is handling and new data need to handle
 *
 *  1. IDLE -> HANDLING:                      thread succ to acquire lease and is handling
 *  2. HANDLING -> READY or READY -> READY:   new data need to handle when thread is handling
 *  3. READY -> HANDLING:                     thread fail to revoke lease as new data arrived
 *  4. HANDLING -> IDLE:                      thread succ to revoke lease
 **/
class ObThreadLease
{
public:
  typedef enum { IDLE = 0, READY = 1, HANDLING = 2 } StatusType;

public:
  ObThreadLease() : status_(IDLE) {}
  ~ObThreadLease() { status_ = IDLE; }

  // for unittest
  StatusType value() const { return status_; }

  void reset()
  {
    status_ = IDLE;
  }

public:
  bool is_idle() const
  {
    return IDLE == ATOMIC_LOAD(&status_);
  }
  bool acquire()
  {
    bool bool_ret = false;
    bool done = false;
    StatusType st = status_;

    while (! done) {
      switch (st) {
        case IDLE:
          // IDLE -> HANDLING
          st = ATOMIC_CAS(&status_, IDLE, HANDLING);
          done = (IDLE == st);
          bool_ret = done;
          break;

          // HANDLING -> READY
        case HANDLING:
          st = ATOMIC_CAS(&status_, HANDLING, READY);
          done = (HANDLING == st);
          break;

          // READY -> READY
        case READY:
          done = true;
          break;

          // unexpected
        default:
          done = true;
          bool_ret = false;
      }
    }
    return bool_ret;
  }

  bool revoke()
  {
    bool bool_ret = false;
    bool done = false;
    StatusType st = status_;

    while (! done) {
      switch (st) {
          // HANDLING -> IDLE
        case HANDLING:
          st = ATOMIC_CAS(&status_, HANDLING, IDLE);
          done = (HANDLING == st);
          bool_ret = done;
          break;

          // READY -> HANDLING
        case READY:
          st = ATOMIC_CAS(&status_, READY, HANDLING);
          done = (READY == st);
          break;

          // return true
        default:
          done = true;
          bool_ret = true;
      }
    }
    return bool_ret;
  }

private:
  StatusType status_;
};

} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_OB_THREAD_LEASE_H__ */
