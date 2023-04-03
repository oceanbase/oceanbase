/**
 * Copyright (c) 2021, 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_qsync_lock.h"

namespace oceanbase
{
namespace common
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
  int ret = common::OB_SUCCESS;

  do {
    if (common::OB_EAGAIN == (ret = try_rdlock())) {
      sched_yield();
    }
  } while (common::OB_EAGAIN == ret);

  return ret;
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
      bool sync_success = false;
      for (int64_t i = 0; !sync_success && i < TRY_SYNC_COUNT; i++) {
        sync_success = qsync_.try_sync();
      }
      if (sync_success) {
        break;
      } else {
        ATOMIC_STORE(&write_flag_, 0);
        sched_yield();
      }
    }
  } while (true);
  return common::OB_SUCCESS;
}

void ObQSyncLock::wrunlock()
{
  ATOMIC_STORE(&write_flag_, 0);
}

int ObQSyncLock::try_rdlock()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != ATOMIC_LOAD(&write_flag_))) {
    ret = OB_EAGAIN;
  } else {
    const int64_t idx = qsync_.acquire_ref();
    if (OB_UNLIKELY(0 != ATOMIC_LOAD(&write_flag_))) {
      qsync_.release_ref(idx);
      ret = OB_EAGAIN;
    } else {
      // success, do nothing
    }
  }
  return ret;
}
}
}
