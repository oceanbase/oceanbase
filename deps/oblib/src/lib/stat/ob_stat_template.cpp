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

#include "ob_stat_template.h"

namespace oceanbase
{
namespace common
{

DIRWLock::DIRWLock()
#ifdef NDEBUG
  : lock_(0)
#else
  : lock_(0),
    wlock_tid_(0)
#endif
{
}

DIRWLock::~DIRWLock()
{
  if (OB_UNLIKELY(lock_ != 0)) {
#ifdef NDEBUG
    COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "DIRWLock exit with lock", K_(lock));
#else
    COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "DIRWLock exit with lock", K_(lock), K_(wlock_tid));
#endif
  }
}

int DIRWLock::try_rdlock()
{
  int ret = OB_EAGAIN;
  uint32_t lock = ATOMIC_LOAD(&lock_);
  if (0 == (lock & WRITE_MASK)) {
    if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int DIRWLock::try_wrlock()
{
  int ret = OB_EAGAIN;
  if (ATOMIC_BCAS(&lock_, 0, WRITE_MASK)) {
#ifndef NDEBUG
    if (OB_UNLIKELY(wlock_tid_ != 0)) {
      COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "wlock mismatch", K(GETTID()), K_(wlock_tid));
    }
    wlock_tid_ = GETTID();
#endif
    ret = OB_SUCCESS;
  }
  return ret;
}

void DIRWLock::rdlock()
{
  uint32_t lock = 0;
  while (1) {
    lock = ATOMIC_LOAD(&lock_);
    if (0 == (lock & WRITE_MASK)) {
      if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
        break;
      }
    }
    PAUSE();
  }
}

void DIRWLock::wrlock()
{
  while (!ATOMIC_BCAS(&lock_, 0, WRITE_MASK)) {
    PAUSE();
  }
#ifndef NDEBUG
  if (OB_UNLIKELY(wlock_tid_ != 0)) {
    COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "wlock mismatch", K(GETTID()), K_(wlock_tid));
  }
  wlock_tid_ = GETTID();
#endif
}

void DIRWLock::wr2rdlock()
{
  while (!ATOMIC_BCAS(&lock_, WRITE_MASK, 1)) {
    PAUSE();
  }
}

void DIRWLock::unlock()
{
  uint32_t lock = 0;
  lock = ATOMIC_LOAD(&lock_);
  if (0 != (lock & WRITE_MASK)) {
#ifndef NDEBUG
    if (OB_UNLIKELY(wlock_tid_ != GETTID())) {
      COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "wlock mismatch", K(GETTID()), K_(wlock_tid));
    }
    wlock_tid_ = 0;
#endif
    ATOMIC_STORE(&lock_, 0);
  } else {
    ATOMIC_AAF(&lock_, -1);
  }
#ifdef ENABLE_DEBUG_LOG
  if (ATOMIC_LOAD(&lock_) > WRITE_MASK) {
    abort();
  }
#endif
}

}
}
