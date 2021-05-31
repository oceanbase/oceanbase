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

#include "ob_row_lock.h"

#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"

namespace oceanbase {
namespace memtable {
void ObRowLock::after_unlock(const ObMemtableKey* key)
{
  row_.set_lock_delayed_cleanout(false);
  get_global_lock_wait_mgr().wakeup(*key);
}

bool ObRowLock::is_locked()
{
  if (GCONF._enable_fast_commit) {
    row_.cleanout_rowlock();
  }
  return is_locked_();
}

bool ObRowLock::is_exclusive_locked_by(const uint32_t uid)
{
  row_.cleanout_rowlock();
  return is_exclusive_locked_by_(uid);
}

uint32_t ObRowLock::get_exclusive_uid()
{
  row_.cleanout_rowlock();
  return get_exclusive_uid_();
}

int ObRowLock::try_exclusive_lock(const uint32_t uid)
{
  row_.cleanout_rowlock();
  return try_exclusive_lock_(uid);
}

int ObRowLock::exclusive_lock(const uint32_t uid, const int64_t abs_timeout)
{
  row_.cleanout_rowlock();
  return exclusive_lock_(uid, abs_timeout);
}

};  // namespace memtable
};  // end namespace oceanbase
