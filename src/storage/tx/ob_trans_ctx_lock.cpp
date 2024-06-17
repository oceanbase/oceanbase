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
#include "ob_trans_ctx.h"
#include "ob_trans_service.h"
#include "common/ob_clock_generator.h"

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

thread_local pid_t lock_thread_id_ = 0;
#define CHECK_LOCK(the_lock)                                            \
  {                                                                     \
    int64_t tid = - (OB_E(EventTable::EN_CHECK_TX_CTX_LOCK) OB_SUCCESS); \
    if (tid) {                                                          \
      if (lock_thread_id_ == 0) {                                       \
        lock_thread_id_ = (pid_t)syscall(__NR_gettid);                  \
      }                                                                 \
      const int64_t diff = tid - lock_thread_id_;                       \
      if (diff == 0) {                                                  \
        TRANS_LOG(INFO, "[CHECK_LOCK]", "lock", the_lock,               \
                  K(tid), K(lock_thread_id_), K(lbt()));                \
      }                                                                 \
    }                                                                   \
  }

int CtxLock::lock(const int64_t timeout_us)
{
  ATOMIC_INC(&waiting_lock_cnt_);
  int ret = OB_SUCCESS;
  int64_t timeout = timeout_us >= 0 ? (ObTimeUtility::current_time() + timeout_us) : INT64_MAX;
  if (OB_SUCC(access_lock_.wrlock(common::ObLatchIds::TRANS_ACCESS_LOCK, timeout))) {
    CHECK_LOCK(access_lock_);
    if (OB_SUCC(flush_redo_lock_.wrlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK, timeout))) {
      if (OB_SUCC(ctx_lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK, timeout))) {
        lock_start_ts_ = ObClockGenerator::getClock();
      } else {
        flush_redo_lock_.unlock();
        access_lock_.unlock();
      }
    } else {
      access_lock_.unlock();
    }
  }
  ATOMIC_DEC(&waiting_lock_cnt_);
  return ret;
}

int CtxLock::try_lock()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(access_lock_.try_wrlock(common::ObLatchIds::TRANS_ACCESS_LOCK))) {
    CHECK_LOCK(access_lock_);
    if (OB_SUCC(flush_redo_lock_.try_wrlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK))) {
      if (OB_SUCC(ctx_lock_.try_wrlock(common::ObLatchIds::TRANS_CTX_LOCK))) {
        lock_start_ts_ = ObClockGenerator::getClock();
      } else {
        flush_redo_lock_.unlock();
        access_lock_.unlock();
      }
    } else {
      access_lock_.unlock();
    }
  }
  return ret;
}

int CtxLock::try_rdlock_ctx()
{
  int ret = ctx_lock_.try_rdlock(common::ObLatchIds::TRANS_CTX_LOCK);
  lock_start_ts_ = 0;
  return ret;
}

void CtxLock::unlock()
{
  unlock_access();
  unlock_flush_redo();
  unlock_ctx();
}

int CtxLock::wrlock_ctx()
{
  int ret = ctx_lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK);
  lock_start_ts_ = ObClockGenerator::getClock();
  return ret;
}

int CtxLock::wrlock_access()
{
  CHECK_LOCK(access_lock_);
  return access_lock_.wrlock(common::ObLatchIds::TRANS_ACCESS_LOCK);
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

int CtxLock::wrlock_flush_redo()
{
  int ret = flush_redo_lock_.wrlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK);
  return ret;
}

int CtxLock::try_wrlock_flush_redo()
{
    int ret = flush_redo_lock_.try_wrlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK);
    return ret;
}

int CtxLock::rdlock_flush_redo()
{
  int ret = flush_redo_lock_.rdlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK);
  return ret;
}

int CtxLock::try_rdlock_flush_redo()
{
  int ret = flush_redo_lock_.try_rdlock(common::ObLatchIds::TRANS_FLUSH_REDO_LOCK);
  return ret;
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
  mode_ = mode;
  do_lock_(true);
}

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

    lock_ = NULL;
    int64_t release_ts = ObTimeUtility::fast_current_time();
    if (release_ts - request_ts_ > 50_ms) {
      TRANS_LOG_RET(WARN, OB_SUCCESS, "[slow ctx lock]",
                    "request_used", hold_ts_ - request_ts_,
                    "hold_used", release_ts - hold_ts_, K(lbt()));
    }
  }
}

} // transaction
} // oceanbase
