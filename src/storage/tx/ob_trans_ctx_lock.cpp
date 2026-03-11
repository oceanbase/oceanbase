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

#include "ob_trans_ctx_lock.h"
#include "ob_trans_service.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace obrpc;

namespace transaction
{
int CtxLock::init(ObTransCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ctx_ = ctx;
  }

  return ret;
}

void CtxLock::reset()
{
  ctx_ = NULL;
}

void CtxLock::before_unlock(CtxLockArg &arg)
{
  if (NULL != ctx_) {
    ctx_->before_unlock(arg);
  }
}

void CtxLock::after_unlock(CtxLockArg &arg)
{
  if (NULL != ctx_) {
    ctx_->after_unlock(arg);
  }
}

int CtxLock::wrlock_ctx(const int64_t timeout)
{
  int ret = ctx_lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK, timeout);
  lock_start_ts_ = ObClockGenerator::getClock();
  return ret;
}

int CtxLock::try_wrlock_ctx()
{
  int ret = ctx_lock_.try_wrlock(common::ObLatchIds::TRANS_CTX_LOCK);
  if (OB_SUCC(ret)) {
    lock_start_ts_ = ObClockGenerator::getClock();
  }
  return ret;
}

int CtxLock::wrlock_access(const int64_t timeout)
{
  return access_lock_.wrlock(common::ObLatchIds::TRANS_ACCESS_LOCK, timeout);
}

int CtxLock::try_wrlock_access()
{
  return access_lock_.try_wrlock(common::ObLatchIds::TRANS_ACCESS_LOCK);
}

void CtxLock::unlock_access()
{
  access_lock_.unlock();
}
void CtxLock::unlock_ctx()
{
  const int64_t lock_start_ts = lock_start_ts_;
  CtxLockArg arg;
  before_unlock(arg);
  ctx_lock_.unlock();
  if (lock_start_ts > 0) {
    const int64_t lock_ts = ObClockGenerator::getClock() - lock_start_ts;
    if (lock_ts > WARN_LOCK_TS) {
      TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "ctx lock too much time", K(arg.trans_id_),
                    K(arg.ls_id_), K(lock_ts), K(lbt()));
    }
  }
  after_unlock(arg);
}

int CtxLock::wrlock_flush_redo(const int64_t timeout)
{
  return flush_redo_lock_.wrlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK, timeout);
}

int CtxLock::try_wrlock_flush_redo()
{
  return flush_redo_lock_.try_wrlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK);
}

int CtxLock::rdlock_flush_redo(const int64_t timeout)
{
  return flush_redo_lock_.rdlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK, timeout);
}

int CtxLock::try_rdlock_flush_redo()
{
  return flush_redo_lock_.try_rdlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK);
}

void CtxLock::unlock_flush_redo()
{
  flush_redo_lock_.unlock();
}

CtxLockGuard::~CtxLockGuard()
{
  reset();
}

void CtxLockGuard::set(CtxLock &lock, uint8_t mode)
{
  reset();
  lock_ = &lock;
  do_lock_(mode, false, 0);
}

void CtxLockGuard::do_lock_(int mode, bool try_lock, int64_t timeout_us)
{
  request_ts_ = ObTimeUtility::fast_current_time();
  int64_t timeout = timeout_us > 0 ? (request_ts_ + timeout_us) : INT64_MAX;
  int ret = OB_SUCCESS;
  if (mode & ACCESS) {
    if (try_lock) {
      if (OB_SUCC(lock_->try_wrlock_access())) {
        mode_ |= ACCESS;
      }
    } else if (OB_SUCC(lock_->wrlock_access(timeout))) {
      mode_ |= ACCESS;
    }
  }
  if (OB_SUCC(ret) && (mode & REDO_FLUSH_X)) {
    if (try_lock) {
      if (OB_SUCC(lock_->try_wrlock_flush_redo())) {
        mode_ |= REDO_FLUSH_X;
      }
    } else if (OB_SUCC(lock_->wrlock_flush_redo(timeout))) {
      mode_ |= REDO_FLUSH_X;
    }
  }
  if (OB_SUCC(ret) && (mode & REDO_FLUSH_R)) {
    if (try_lock) {
      if (OB_SUCC(lock_->try_rdlock_flush_redo())) {
        mode_ |= REDO_FLUSH_R;
      }
    } else if (OB_SUCC(lock_->rdlock_flush_redo(timeout))) {
      mode_ |= REDO_FLUSH_R;
    }
  }
  if (OB_SUCC(ret) && (mode & CTX)) {
    if (try_lock) {
      if (OB_SUCC(lock_->try_wrlock_ctx())) {
        mode_ |= CTX;
      }
    } else if (OB_SUCC(lock_->wrlock_ctx(timeout))) {
      mode_ |= CTX;
    }
  }
  hold_ts_ = ObTimeUtility::fast_current_time();
  locked_ = OB_SUCC(ret);
}

ERRSIM_POINT_DEF(EN_SLOW_CTX_LOCK_THRESHOLD)
void CtxLockGuard::reset()
{
  if (NULL != lock_) {
    if (mode_ & MODE::CTX) {
      lock_->unlock_ctx();
    }
    if ((mode_ & MODE::REDO_FLUSH_X) || (mode_ & MODE::REDO_FLUSH_R)) {
      lock_->unlock_flush_redo();
    }
    if (mode_ & MODE::ACCESS) {
      lock_->unlock_access();
    }
    int64_t release_ts = ObTimeUtility::fast_current_time();
    const int64_t slow_threshold = EN_SLOW_CTX_LOCK_THRESHOLD ? (-EN_SLOW_CTX_LOCK_THRESHOLD) * 1_ms : 50_ms;
    if (release_ts - request_ts_ > slow_threshold) {
      TRANS_LOG_RET(WARN, OB_SUCCESS, "[slow tx ctx lock]",
                    KP(lock_), K(mode_),
                    "trans_id", lock_->ctx_->get_trans_id(),
                    "ls_id", lock_->ctx_->get_ls_id(),
                    "request_used", hold_ts_ - request_ts_,
                    "hold_used", release_ts - hold_ts_);
    }
    lock_ = NULL;
  }
}

} // transaction
} // oceanbase
