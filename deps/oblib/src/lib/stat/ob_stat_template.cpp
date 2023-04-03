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
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{

DIRWLock::DIRWLock()
  : lock_(0)
{
}

DIRWLock::~DIRWLock()
{
}

int DIRWLock::try_rdlock()
{
  int ret = OB_EAGAIN;
  uint32_t lock = lock_;
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
    ret = OB_SUCCESS;
  }
  return ret;
}

void DIRWLock::rdlock()
{
  uint32_t lock = 0;
  while (1) {
    lock = lock_;
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
  lock = lock_;
  if (0 != (lock & WRITE_MASK)) {
    ATOMIC_STORE(&lock_, 0);
  } else {
    ATOMIC_AAF(&lock_, -1);
  }
}

}
}
