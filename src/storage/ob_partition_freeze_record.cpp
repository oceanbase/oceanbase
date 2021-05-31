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

#include "storage/ob_partition_freeze_record.h"

#include "lib/atomic/ob_atomic.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "storage/ob_i_partition_group.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
using namespace common;
using namespace memtable;

using transaction::ObTransVersion;

namespace storage {

ObFreezeRecord::ObFreezeRecord()
    : snapshot_version_(ObTransVersion::INVALID_TRANS_VERSION),
      emergency_(false),
      active_protection_clock_(INT64_MAX),
      storage_info_(),
      upper_limit_(INT64_MAX),
      is_valid_(false),
      frozen_memtable_handle_(),
      new_memtable_handle_(),
      freeze_ts_(OB_INVALID_FREEZE_TS)
{}

ObFreezeRecord::~ObFreezeRecord()
{}

bool ObFreezeRecord::available() const
{
  return OB_INVALID_FREEZE_TS == freeze_ts_;
}

int ObFreezeRecord::get_snapshot_version(int64_t& snapshot_version) const
{
  int ret = OB_SUCCESS;

  if (ObTransVersion::INVALID_TRANS_VERSION == snapshot_version_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    snapshot_version = snapshot_version_;
  }

  return ret;
}

int ObFreezeRecord::get_active_protection_clock(int64_t& active_protection_clock) const
{
  int ret = OB_SUCCESS;

  if (ObTransVersion::INVALID_TRANS_VERSION == snapshot_version_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    active_protection_clock = active_protection_clock_;
  }

  return ret;
}

void ObFreezeRecord::clear()
{
  ObSpinLockGuard guard(lock_);
  frozen_memtable_handle_.reset();
  new_memtable_handle_.reset();
  freeze_ts_ = OB_INVALID_FREEZE_TS;
}

// deprecated
bool ObFreezeRecord::need_raise_memstore(const int64_t trans_version) const
{
  bool bool_ret = false;
  bool need_retry = false;

  MEM_BARRIER();
  do {
    WEAK_BARRIER();

    need_retry = false;
    if (ObTransVersion::INVALID_TRANS_VERSION == ATOMIC_LOAD(&snapshot_version_) ||
        trans_version <= ATOMIC_LOAD(&snapshot_version_)) {
      bool_ret = false;
    } else if ((bool)ATOMIC_LOAD(&is_valid_)) {
      bool_ret = true;
    } else {
      need_retry = true;
      usleep(1);
    }
  } while (need_retry);

  return bool_ret;
}

int ObFreezeRecord::set_freeze_upper_limit(const int64_t upper_limit)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;

  // set limit
  upper_limit_ = upper_limit;

  MEM_BARRIER();
  // safety check
  do {
    WEAK_BARRIER();

    need_retry = false;

    if (ObTransVersion::INVALID_TRANS_VERSION == ATOMIC_LOAD(&snapshot_version_) ||
        upper_limit > ATOMIC_LOAD(&snapshot_version_)) {
    } else if ((bool)ATOMIC_LOAD(&is_valid_)) {
      ret = OB_EAGAIN;
    } else {
      need_retry = true;
      usleep(1);
    }
  } while (need_retry);

  if (OB_EAGAIN == ret) {
    // roll back
    upper_limit_ = INT64_MAX;
  }

  return ret;
}

int ObFreezeRecord::submit_freeze(ObMemtable& frozen_memtable, const int64_t freeze_ts)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (OB_UNLIKELY(!available())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "concurrent freeze is unavilable", K(ret));
  } else if (0 == freeze_ts || OB_INVALID_FREEZE_TS == freeze_ts) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "freeze_ts is invalid id", K(ret), K(freeze_ts));
  } else {
    freeze_ts_ = freeze_ts;
    frozen_memtable_handle_.set_table(&frozen_memtable);
  }

  return ret;
}

int ObFreezeRecord::submit_new_active_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMemtable* mt = NULL;

  ObSpinLockGuard guard(lock_);

  if (OB_FAIL(handle.get_memtable(mt))) {
    STORAGE_LOG(WARN, "get memtable from handle fail", K(ret), K(handle));
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "memtable is NULL when submitting freeze", K(ret));
  } else {
    new_memtable_handle_.assign(handle);
  }

  return ret;
}

int ObFreezeRecord::get_pending_frozen_memtable(ObMemtable*& frozen_memtable)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);

  if (!frozen_memtable_handle_.is_valid()) {
    frozen_memtable = NULL;
  } else if (OB_FAIL(frozen_memtable_handle_.get_memtable(frozen_memtable))) {
    STORAGE_LOG(WARN, "get frozen memtable handle fail", K(ret), K(frozen_memtable_handle_));
  }

  return ret;
}

int ObFreezeRecord::get_pending_frozen_memtable(ObMemtable*& frozen_memtable, ObMemtable*& active_memtable)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);

  if (!frozen_memtable_handle_.is_valid() || !new_memtable_handle_.is_valid()) {
    frozen_memtable = active_memtable = NULL;
  } else if (OB_FAIL(frozen_memtable_handle_.get_memtable(frozen_memtable))) {
    STORAGE_LOG(WARN, "get frozen memtable handle fail", K(ret), K(frozen_memtable_handle_));
  } else if (OB_FAIL(new_memtable_handle_.get_memtable(active_memtable))) {
    STORAGE_LOG(WARN, "get new memtable handle fail", K(ret), K(new_memtable_handle_));
  }

  return ret;
}

void ObFreezeRecord::dirty_trans_marked(
    ObMemtable* const memtable, const int64_t cb_cnt, const bool finish, const int64_t applied_log_ts, bool& cleared)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "memtable is NULL", K(ret));
  } else if (FALSE_IT(memtable->log_applied(applied_log_ts))) {
  } else if (OB_FAIL(memtable->add_pending_cb_count(cb_cnt, finish))) {
    STORAGE_LOG(ERROR, "add pending trx count failed", K(ret));
  } else if (memtable->can_be_minor_merged()) {
    cleared = true;
    clear();
  }
}

int64_t ObFreezeRecord::get_freeze_ts() const
{
  return freeze_ts_;
}

}  // namespace storage
}  // namespace oceanbase
