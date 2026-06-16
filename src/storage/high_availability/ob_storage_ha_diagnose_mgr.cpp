/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase;
using namespace storage;
using namespace share;
using namespace common;

ObHADiagFlushItem::ObHADiagFlushItem()
  : ls_id_(),
    snapshot_()
{
}

void ObHADiagFlushItem::reset()
{
  ls_id_.reset();
  snapshot_.reset();
}

ObStorageHADiagMgr::ObStorageHADiagMgr()
  : is_inited_(false),
    inflight_lock_(ObLatchIds::OB_STORAGE_HA_DIAGNOSE_MGR_LOCK),
    inflight_keys_(),
    queue_lock_(ObLatchIds::OB_STORAGE_HA_DIAGNOSE_MGR_LOCK),
    pending_(),
    dropped_count_(0)
{
}

int ObStorageHADiagMgr::mtl_init(ObStorageHADiagMgr *&storage_ha_diag_mgr)
{
  return storage_ha_diag_mgr->init(MTL_ID());
}

int ObStorageHADiagMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageHADiagMgr has already been inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(inflight_keys_.create(INFLIGHT_BUCKET_NUM, "HAInflight", "HAInflight", tenant_id))) {
    LOG_WARN("failed to create inflight keys set", K(ret), K(tenant_id));
  } else {
    pending_.set_attr(ObMemAttr(tenant_id, "HADiagnoseQueue"));
    is_inited_ = true;
  }
  return ret;
}

void ObStorageHADiagMgr::destroy()
{
  is_inited_ = false;
  {
    SpinWLockGuard guard(queue_lock_);
    pending_.reset();
    dropped_count_ = 0;
  }
  SpinWLockGuard guard(inflight_lock_);
  (void) inflight_keys_.destroy();
}

int ObStorageHADiagMgr::add_inflight(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagMgr not inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inflight ls id", K(ret), K(ls_id));
  } else {
    SpinWLockGuard guard(inflight_lock_);
    const int tmp_ret = inflight_keys_.set_refactored(ls_id, 1);
    if (OB_SUCCESS != tmp_ret && OB_HASH_EXIST != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to add inflight ls id", K(ret), K(ls_id));
    }
  }
  return ret;
}

int ObStorageHADiagMgr::remove_inflight(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagMgr not inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inflight ls id", K(ret), K(ls_id));
  } else {
    SpinWLockGuard guard(inflight_lock_);
    const int tmp_ret = inflight_keys_.erase_refactored(ls_id);
    if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to remove inflight ls id", K(ret), K(ls_id));
    }
  }
  return ret;
}

int ObStorageHADiagMgr::get_inflight_snapshot(ObIArray<ObLSID> &out) const
{
  int ret = OB_SUCCESS;
  out.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagMgr not inited", K(ret));
  } else {
    SpinRLockGuard guard(inflight_lock_);
    for (common::hash::ObHashSet<ObLSID, common::hash::NoPthreadDefendMode>::const_iterator it =
             inflight_keys_.begin();
         OB_SUCC(ret) && it != inflight_keys_.end();
         ++it) {
      if (OB_FAIL(out.push_back(it->first))) {
        LOG_WARN("failed to push back ls id", K(ret), "ls_id", it->first);
      }
    }
  }
  return ret;
}

int ObStorageHADiagMgr::submit_flush(const ObHADiagFlushItem &item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagMgr not inited", K(ret));
  } else if (OB_UNLIKELY(!item.ls_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flush item", K(ret), K(item));
  } else {
    bool overflowed = false;
    int64_t dropped_now = 0;
    {
      SpinWLockGuard guard(queue_lock_);
      if (pending_.count() >= MAX_PENDING) {
        overflowed = true;
        dropped_count_++;
        dropped_now = dropped_count_;
      } else if (OB_FAIL(pending_.push_back(item))) {
        LOG_WARN("failed to push flush item", K(ret));
      }
    }
    if (overflowed) {
      ret = OB_SIZE_OVERFLOW;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_WARN("ha diag flush queue full, dropping item", K(ret), K(item), K(dropped_now));
      }
    }
  }
  return ret;
}

int ObStorageHADiagMgr::drain_pending(ObIArray<ObHADiagFlushItem> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagMgr not inited", K(ret));
  } else {
    SpinWLockGuard guard(queue_lock_);
    if (pending_.empty()) {
      // nothing to drain
    } else if (OB_FAIL(out.assign(pending_))) {
      LOG_WARN("failed to assign pending batch", K(ret));
    } else {
      pending_.reset();
    }
  }
  return ret;
}
