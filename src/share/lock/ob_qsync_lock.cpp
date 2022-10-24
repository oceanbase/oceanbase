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

#include "share/lock/ob_qsync_lock.h"

namespace oceanbase
{
namespace share
{
int ObQSyncLock::init(const lib::ObMemAttr &mem_attr)
{
  return qsync_.init(mem_attr);
}

void ObQSyncLock::destroy()
{
  qsync_.destroy();
}

int ObQSyncLock::rdlock()
{
  do {
    if (OB_UNLIKELY(0 != ATOMIC_LOAD(&write_flag_))) {
      sched_yield();
    } else {
      const int64_t idx = qsync_.acquire_ref();
      if (OB_UNLIKELY(0 != ATOMIC_LOAD(&write_flag_))) {
        qsync_.release_ref(idx);
        sched_yield();
      } else {
        break;
      }
    }
  } while (true);
  return common::OB_SUCCESS;
}

void ObQSyncLock::rdunlock()
{
  qsync_.release_ref();
}

int ObQSyncLock::wrlock()
{
  do {
    if (!ATOMIC_BCAS(&write_flag_, 0, 1)) {
      sched_yield();
    } else {
      // write priority try sync to succ
      while (!qsync_.try_sync()) {
        sched_yield();
      }
      break;
    }
  } while (true);
  return common::OB_SUCCESS;
}

void ObQSyncLock::wrunlock()
{
  ATOMIC_STORE(&write_flag_, 0);
}

}
}
