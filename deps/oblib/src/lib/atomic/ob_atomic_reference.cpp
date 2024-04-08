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

#include "lib/atomic/ob_atomic.h"
#include "lib/atomic/ob_atomic_reference.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

/*
 * -----------------------------------------------------------ObAtomicReference-----------------------------------------------------
 */
ObAtomicReference::ObAtomicReference()
{
  atomic_num_.seq = 0;
  atomic_num_.ref = 0;
}

ObAtomicReference::~ObAtomicReference()
{
}

void ObAtomicReference::reset()
{
  //The seq must NOT be reset
  atomic_num_.ref = 0;
}

int ObAtomicReference::inc_ref_cnt()
{
  int ret = OB_SUCCESS;
  AtomicInt64 atomic_old = { 0 };
  AtomicInt64 atomic_new = { 0 };
  while (OB_SUCC(ret)) {
#if defined(__aarch64__)
    atomic_old.atomic = ATOMIC_FAAx(&atomic_num_.atomic, 0, 0);
#else
    atomic_old.atomic = ATOMIC_LOAD(&atomic_num_.atomic);
#endif
    atomic_new.atomic = atomic_old.atomic;

    atomic_new.ref += 1;
    if (OB_UNLIKELY(0 == atomic_new.ref)) {
      ret = OB_INTEGER_PRECISION_OVERFLOW;
      COMMON_LOG(WARN, "The reference count is overflow, ", K(ret));
    } else {
      if (ATOMIC_BCAS(&(atomic_num_.atomic), atomic_old.atomic, atomic_new.atomic)) {
        break;
      } else {
        PAUSE();
      }
    }
  }
  return ret;
}

int ObAtomicReference::check_seq_num_and_inc_ref_cnt(const uint32_t seq_num)
{
  int ret = OB_SUCCESS;
  AtomicInt64 atomic_old = { 0 };
  AtomicInt64 atomic_new = { 0 };
  while (OB_SUCC(ret)) {
#if defined(__aarch64__)
    atomic_old.atomic = ATOMIC_FAAx(&atomic_num_.atomic, 0, 0);
#else
    atomic_old.atomic = ATOMIC_LOAD(&atomic_num_.atomic);
#endif
    atomic_new.atomic = atomic_old.atomic;
    atomic_new.ref += 1;

    if (OB_UNLIKELY(0 == atomic_new.ref)) {
      ret = OB_INTEGER_PRECISION_OVERFLOW;
      COMMON_LOG(WARN, "The reference count is overflow, ", K(ret));
    } else if (OB_UNLIKELY(seq_num != atomic_old.seq) || OB_UNLIKELY(0 == atomic_old.ref)) {
      ret = OB_EAGAIN;
      //normal case, do not print log
    } else if (ATOMIC_BCAS(&(atomic_num_.atomic), atomic_old.atomic, atomic_new.atomic)) {
      break;
    } else {
      PAUSE();
    }
  }
  return ret;
}

int ObAtomicReference::check_and_inc_ref_cnt()
{
  int ret = OB_SUCCESS;
  AtomicInt64 atomic_old = { 0 };
  AtomicInt64 atomic_new = { 0 };
  while (OB_SUCC(ret)) {
#if defined(__aarch64__)
    atomic_old.atomic = ATOMIC_FAAx(&atomic_num_.atomic, 0, 0);
#else
    atomic_old.atomic = ATOMIC_LOAD(&atomic_num_.atomic);
#endif
    atomic_new.atomic = atomic_old.atomic;
    atomic_new.ref += 1;

    if (OB_UNLIKELY(0 == atomic_new.ref)) {
      ret = OB_INTEGER_PRECISION_OVERFLOW;
      COMMON_LOG(WARN, "The reference count is overflow, ", K(ret));
    } else if (OB_UNLIKELY(0 == atomic_old.ref)) {
      ret = OB_EAGAIN;
      //normal case, do not print log
    } else if (ATOMIC_BCAS(&(atomic_num_.atomic), atomic_old.atomic, atomic_new.atomic)) {
      break;
    } else {
      PAUSE();
    }
  }
  return ret;
}

int ObAtomicReference::dec_ref_cnt_and_inc_seq_num(uint32_t &ref_cnt)
{
  int ret = OB_SUCCESS;
  AtomicInt64 atomic_old = { 0 };
  AtomicInt64 atomic_new = { 0 };
  while (OB_SUCC(ret)) {
#if defined(__aarch64__)
    atomic_old.atomic = ATOMIC_FAAx(&atomic_num_.atomic, 0, 0);
#else
    atomic_old.atomic = ATOMIC_LOAD(&atomic_num_.atomic);
#endif
    atomic_new.atomic = atomic_old.atomic;

    if (OB_UNLIKELY(0 == atomic_old.ref)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "The reference count is 0, ", K(ret));
    } else {
      atomic_new.ref -= 1;
      if (0 == atomic_new.ref) {
        atomic_new.seq += 1;
      }

      if (ATOMIC_BCAS(&(atomic_num_.atomic), atomic_old.atomic, atomic_new.atomic)) {
        break;
      } else {
        PAUSE();
      }
    }
  }

  if (OB_SUCC(ret)) {
    ref_cnt = atomic_new.ref;
  }
  return ret;
}

bool ObAtomicReference::try_inc_seq_num()
{
  bool seq_num_increased = false;
  AtomicInt64 atomic_old = { 0 };
  AtomicInt64 atomic_new = { 0 };

#if defined(__aarch64__)
  atomic_old.atomic = ATOMIC_FAAx(&atomic_num_.atomic, 0, 0);
#else
  atomic_old.atomic = ATOMIC_LOAD(&atomic_num_.atomic);
#endif
  if (1 == atomic_old.ref) {
    atomic_new.ref = 0;
    atomic_new.seq = atomic_old.seq + 1;
    if (ATOMIC_BCAS(&(atomic_num_.atomic), atomic_old.atomic, atomic_new.atomic)) {
      seq_num_increased = true;
    }
  }
  return seq_num_increased;
}

bool ObAtomicReference::try_check_and_inc_seq_num(const uint32_t seq_num)
{
  bool seq_num_increased = false;
  AtomicInt64 atomic_old = { 0 };
  AtomicInt64 atomic_new = { 0 };

#if defined(__aarch64__)
  atomic_old.atomic = ATOMIC_FAAx(&atomic_num_.atomic, 0, 0);
#else
  atomic_old.atomic = ATOMIC_LOAD(&atomic_num_.atomic);
#endif
  if (seq_num == atomic_old.seq && 2 == atomic_old.ref) {
    atomic_new.ref = 0;
    atomic_new.seq = atomic_old.seq + 1;
    if (ATOMIC_BCAS(&(atomic_num_.atomic), atomic_old.atomic, atomic_new.atomic)) {
      seq_num_increased = true;
    }
  }
  return seq_num_increased;
}

}
}
