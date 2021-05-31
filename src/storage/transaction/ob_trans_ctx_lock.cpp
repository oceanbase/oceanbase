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

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace obrpc;

namespace transaction {
int CtxLock::init(ObTransCtx* ctx)
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

void CtxLock::before_unlock(CtxLockArg& arg)
{
  if (NULL != ctx_) {
    ctx_->before_unlock(arg);
  }
}

void CtxLock::after_unlock(CtxLockArg& arg)
{
  if (NULL != ctx_) {
    ctx_->after_unlock(arg);
  }
}

int CtxLock::lock()
{
  return lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK);
}

int CtxLock::lock(const int64_t timeout_us)
{
  return lock_.wrlock(common::ObLatchIds::TRANS_CTX_LOCK, ObTimeUtility::current_time() + timeout_us);
}

int CtxLock::try_lock()
{
  return lock_.try_wrlock(common::ObLatchIds::TRANS_CTX_LOCK);
}

void CtxLock::unlock()
{
  CtxLockArg arg;
  before_unlock(arg);
  lock_.unlock();
  after_unlock(arg);
}

CtxLockGuard::~CtxLockGuard()
{
  reset();
}

void CtxLockGuard::set(CtxLock& lock)
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

void TransTableSeqLock::write_lock()
{
  seq_++;
  MEM_BARRIER();
}

void TransTableSeqLock::write_unlock()
{
  MEM_BARRIER();
  if (seq_ & 1L) {
    seq_++;
  }
}

uint64_t TransTableSeqLock::read_begin()
{
  uint64_t seq = ATOMIC_LOAD(&seq_);
  MEM_BARRIER();
  return seq;
}

bool TransTableSeqLock::read_retry(const uint64_t seq)
{
  MEM_BARRIER();
  if (ATOMIC_LOAD(&seq_) != seq) {
    return true;
  }
  return seq & 1L;
}

CtxTransTableLockGuard::~CtxTransTableLockGuard()
{
  reset();
}

void CtxTransTableLockGuard::set(CtxLock& lock, TransTableSeqLock& seqlock)
{
  reset();

  CtxLockGuard::set(lock);
  lock_ = &seqlock;
  lock_->write_lock();
}

void CtxTransTableLockGuard::reset()
{
  if (lock_) {
    lock_->write_unlock();
    lock_ = NULL;
  }
  CtxLockGuard::reset();
}

}  // namespace transaction
}  // namespace oceanbase
