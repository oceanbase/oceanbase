/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/lock/ob_bucket_qsync_lock.h"
#include "lib/lock/ob_qsync_lock.h"

namespace oceanbase
{
namespace common
{
ObBucketQSyncLock::ObBucketQSyncLock()
    : bucket_cnt_(0),
      locks_(NULL),
      is_inited_(false)
{
}

ObBucketQSyncLock::~ObBucketQSyncLock()
{
  destroy();
}

int ObBucketQSyncLock::init(
  const uint64_t bucket_cnt,
  const uint64_t tenant_id,
  const lib::ObLabel &label)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "The ObBucketQSyncLock has been inited, ", K(ret));
  } else if (OB_UNLIKELY(0 == bucket_cnt) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(bucket_cnt), K(label), K(tenant_id), K(ret));
  } else {
    bucket_cnt_ = bucket_cnt;
    ObMemAttr mem_attr(tenant_id, label);
    void *buf = NULL;
    if (OB_UNLIKELY(NULL == (buf = ob_malloc(bucket_cnt_ * sizeof(ObQSyncLock), mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "Fail to allocate memory, ", K(bucket_cnt_), K(ret));
    } else {
      locks_ = new (buf) ObQSyncLock[bucket_cnt_];

      for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt_; ++i) {
        if (OB_FAIL(locks_[i].init(mem_attr))) {
          SHARE_LOG(WARN, "Fail to init qsync lock", K(ret), K(bucket_cnt_), K(i));
        }
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObBucketQSyncLock::destroy()
{
  is_inited_ = false;
  if (nullptr != locks_) {
    for (uint64_t i = 0; i < bucket_cnt_; ++i) {
      locks_[i].~ObQSyncLock();
    }
    ob_free(locks_);
    locks_ = NULL;
  }
  bucket_cnt_ = 0;
}

int ObBucketQSyncLock::rdlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(locks_[bucket_idx].rdlock())) {
    SHARE_LOG(WARN, "Fail to read lock latch, ", K(bucket_idx), K(ret));
  }
  return ret;
}

int ObBucketQSyncLock::wrlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(locks_[bucket_idx].wrlock())) {
    SHARE_LOG(WARN, "Fail to write lock latch, ", K(bucket_idx), K(ret));
  }
  return ret;
}

int ObBucketQSyncLock::rdunlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else {
    locks_[bucket_idx].rdunlock();
  }
  return ret;
}

int ObBucketQSyncLock::wrunlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else {
    locks_[bucket_idx].wrunlock();
  }
  return ret;
}

int ObBucketQSyncLock::wrlock_all()
{
  int ret = OB_SUCCESS;
  int64_t last_succ_idx = -1;
  const int64_t start_ts = ObClockGenerator::getClock();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt_; ++i) {
      if (OB_SUCC(locks_[i].wrlock())) {
        last_succ_idx = i;
      } else {
        if(OB_EAGAIN != ret) {
          SHARE_LOG(WARN, "failed to try wrlock", K(ret), K(i), K(bucket_cnt_));
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i <= last_succ_idx; ++i) {
        locks_[i].wrunlock();
      }
    }
  }

  const int64_t cost_ts = ObClockGenerator::getClock() - start_ts;
  SHARE_LOG(DEBUG, "wrlock all", K(bucket_cnt_), K(cost_ts), K(ret));
  return ret;
}

int ObBucketQSyncLock::wrunlock_all()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObClockGenerator::getClock();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt_; ++i) {
      locks_[i].wrunlock();
    }
  }
  const int64_t cost_ts = ObClockGenerator::getClock() - start_ts;
  SHARE_LOG(DEBUG, "unlock all", K(bucket_cnt_), K(cost_ts), K(ret));
  return ret;
}

int ObBucketQSyncLock::try_rdlock_all()
{
  int ret = OB_SUCCESS;
  int64_t last_succ_idx = -1;
  const int64_t start_ts = ObClockGenerator::getClock();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt_; ++i) {
      ret = locks_[i].try_rdlock();
      if (OB_SUCCESS == ret) {
        last_succ_idx = i;
      } else if (OB_EAGAIN != ret) {
        SHARE_LOG(WARN, "failed to try wrlock", K(ret), K(i), K(bucket_cnt_));
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i <= last_succ_idx; ++i) {
        locks_[i].rdunlock();
      }
    }
  }
  const int64_t cost_ts = ObClockGenerator::getClock() - start_ts;
  SHARE_LOG(DEBUG, "try lock all", K(bucket_cnt_), K(cost_ts), K(ret));
  return ret;
}

int ObBucketQSyncLock::rdunlock_all()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObClockGenerator::getClock();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ObBucketQSyncLock not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt_; ++i) {
      locks_[i].rdunlock();
    }
  }
  const int64_t cost_ts = ObClockGenerator::getClock() - start_ts;
  SHARE_LOG(DEBUG, "unlock all", K(bucket_cnt_), K(cost_ts), K(ret));
  return ret;
}
} //namespace common
} //namespace oceanbae
