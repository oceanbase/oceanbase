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

int CtxLock::lock()
{
  int ret = lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK);
  lock_start_ts_ = ObClockGenerator::getClock();
  return ret;
}

int CtxLock::lock(const int64_t timeout_us)
{
  int ret = lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK, ObTimeUtility::current_time() + timeout_us);
  lock_start_ts_ = ObClockGenerator::getClock();
  return ret;
}

int CtxLock::try_lock()
{
  int ret = lock_.try_wrlock(common::ObLatchIds::TRANS_CTX_LOCK);
  if (OB_SUCCESS == ret) {
    lock_start_ts_ = ObClockGenerator::getClock();
  }
  return ret;
}

void CtxLock::unlock()
{
  CtxLockArg arg;
  before_unlock(arg);
  lock_.unlock();
  const int64_t lock_ts = ObClockGenerator::getClock() - lock_start_ts_;
  if (lock_start_ts_ > 0 && lock_ts > WARN_LOCK_TS) {
    lock_start_ts_ = 0;
    TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "ctx lock too much time", K(arg.trans_id_), K(lock_ts), K(lbt()));
  }
  after_unlock(arg);
}

CtxLockGuard::~CtxLockGuard()
{
  reset();
}

void CtxLockGuard::set(CtxLock &lock)
{
  reset();
  lock_ = &lock;
  lock_->lock();
}

void CtxLockGuard::reset()
{
  if (NULL != lock_) {
    lock_->unlock();
    lock_ = NULL;
  }
}

} // transaction
} // oceanbase
