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

#ifndef ENABLE_SANITY
#else
#include "lib/alloc/ob_latch_v2.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"


namespace oceanbase
{
namespace common
{
/**
 * -------------------------------------------------------ObLatchMutexV2---------------------------------------------------------------
 */
ObLatchMutexV2::ObLatchMutexV2()
  : lock_()
{
}

ObLatchMutexV2::~ObLatchMutexV2()
{
  if (0 != lock_.val()) {
    COMMON_LOG(DEBUG, "invalid lock,", K(lock_.val()), K(lbt()));
  }
}

int ObLatchMutexV2::try_lock(
    const uint32_t latch_id,
    const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(ret));
  } else {
    if (!ATOMIC_BCAS(&lock_.val(), 0, (WRITE_MASK | uid))) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObLatchMutexV2::lock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  uint64_t i = 0;
  uint64_t spin_cnt = 0;
  bool waited = false;
  uint64_t yield_cnt = 0;
  const uint32_t uid = static_cast<uint32_t>(GETTID());

  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
        || OB_UNLIKELY(0 == uid)
        || OB_UNLIKELY(uid >= WRITE_MASK)
        || OB_UNLIKELY(abs_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(abs_timeout_us), K(ret), K(lbt()));
  } else {
    while (OB_SUCC(ret)) {
      //spin
      i = low_try_lock(OB_LATCHES[latch_id].max_spin_cnt_, (WRITE_MASK | uid));
      spin_cnt += i;

      if (OB_LIKELY(i < OB_LATCHES[latch_id].max_spin_cnt_)) {
        //success lock
        ++spin_cnt;
        break;
      } else if (yield_cnt < OB_LATCHES[latch_id].max_yield_cnt_) {
        sched_yield();
        ++yield_cnt;
        continue;
      } else {
        //wait
        waited = true;
        // latch mutex wait is an atomic wait event
        if (OB_FAIL(wait(abs_timeout_us, uid))) {
          if (OB_TIMEOUT != ret) {
            COMMON_LOG(WARN, "Fail to wait the latch, ", K(ret));
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObLatchMutexV2::wait(const int64_t abs_timeout_us, const uint32_t uid)
{
  // performance critical, do not double check the parameters
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  int lock = 0;

  while (OB_SUCC(ret)) {
    timeout = abs_timeout_us - ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      COMMON_LOG(DEBUG, "wait latch mutex timeout, ", K(abs_timeout_us), K(lbt()), K(get_wid()), K(ret));
    } else {
      lock = lock_.val();
      if (WAIT_MASK == (lock & WAIT_MASK)
          || 0 != (lock = ATOMIC_CAS(&lock_.val(), (lock | WRITE_MASK), (lock | WAIT_MASK)))) {
        ret = lock_.wait((lock | WAIT_MASK), timeout);
      }
      if (OB_LIKELY(OB_TIMEOUT != ret)) {
        // spin try lock
        if (MAX_SPIN_CNT_AFTER_WAIT >
            low_try_lock(MAX_SPIN_CNT_AFTER_WAIT, (WAIT_MASK | WRITE_MASK | uid))) {
          ret = OB_SUCCESS;
          break;
        }
      } else  {
        COMMON_LOG(DEBUG, "wait latch mutex timeout",
            K(abs_timeout_us), K(lbt()), K(get_wid()), K(ret));
      }
    }
  }
  return ret;
}

int ObLatchMutexV2::unlock()
{
  int ret = OB_SUCCESS;
  uint32_t lock = ATOMIC_SET(&lock_.val(), 0);

  if (OB_UNLIKELY(0 == lock)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid lock,", K(lock), K(ret));
  } else if (OB_UNLIKELY(0 != (lock & WAIT_MASK))) {
    lock_.wake(1);
  }
  return ret;
}

int64_t ObLatchMutexV2::to_string(char *buf, const int64_t buf_len)
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, "lock_", lock_.val());
  return pos;
}

}
}
#endif
