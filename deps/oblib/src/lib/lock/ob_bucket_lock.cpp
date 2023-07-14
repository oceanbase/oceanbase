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

#include "lib/lock/ob_bucket_lock.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace common
{

ObBucketLock::ObBucketLock()
    : bucket_cnt_(0),
      latch_cnt_(0),
      latches_(NULL),
      latch_id_(ObLatchIds::DEFAULT_BUCKET_LOCK),
      is_inited_(false)
{
}

ObBucketLock::~ObBucketLock()
{
  destroy();
}

int ObBucketLock::init(
  const uint64_t bucket_cnt,
  const uint32_t latch_id,
  const lib::ObLabel &label,
  const uint64_t tenant_id)
{
  attr_.label_ = label;
  attr_.tenant_id_ = tenant_id;
  return init(bucket_cnt, latch_id, attr_);
}

int ObBucketLock::init(
  const uint64_t bucket_cnt,
  const uint32_t latch_id,
  const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObBucketLock has been inited, ", K(ret));
  } else if (OB_UNLIKELY(0 == bucket_cnt)
    || OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
    || OB_UNLIKELY(OB_INVALID_ID == attr.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(bucket_cnt), K(latch_id), K(ret));
  } else {
    attr_ = attr;
    bucket_cnt_ = bucket_cnt;
    latch_cnt_ = bucket_cnt_ / 8 + 1;
    void *buf = NULL;
    if (OB_UNLIKELY(NULL == (buf = ob_malloc(latch_cnt_ * sizeof(ObLatch), attr_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "Fail to allocate memory, ", K_(latch_cnt), K(ret));
    } else {
      latches_ = new (buf) ObLatch[latch_cnt_];
      latch_id_ = latch_id;
      is_inited_ = true;
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObBucketLock::destroy()
{
  is_inited_ = false;
  if (NULL != latches_) {
    for (int64_t i = latch_cnt_ - 1; i >= 0; --i) {
      latches_[i].~ObLatch();
    }
    ob_free(latches_);
    latches_ = NULL;
  }
  latch_cnt_ = 0;
  bucket_cnt_ = 0;
  latch_id_ = ObLatchIds::DEFAULT_BUCKET_LOCK;
}

int ObBucketLock::try_rdlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(latches_[bucket_to_latch_idx(bucket_idx)].try_rdlock(latch_id_))) {
    if (OB_EAGAIN != ret) {
      COMMON_LOG(ERROR, "Fail to try lock latch, ", K(ret));
    }
  }
  return ret;
}

int ObBucketLock::try_wrlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(latches_[bucket_to_latch_idx(bucket_idx)].try_wrlock(latch_id_))) {
    if (OB_EAGAIN != ret) {
      COMMON_LOG(ERROR, "Fail to try lock latch, ", K(ret));
    }
  }
  return ret;
}

int ObBucketLock::rdlock(const uint64_t bucket_idx, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(latches_[bucket_to_latch_idx(bucket_idx)].rdlock(latch_id_, abs_timeout_us))) {
    COMMON_LOG(WARN, "Fail to read lock latch, ", K(bucket_idx), K_(latch_id), K(abs_timeout_us), K(ret));
  }
  return ret;
}

int ObBucketLock::wrlock(const uint64_t bucket_idx, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret), K(lbt()));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(latches_[bucket_to_latch_idx(bucket_idx)].wrlock(latch_id_, abs_timeout_us))) {
    COMMON_LOG(WARN, "Fail to write lock latch, ", K(bucket_idx), K_(latch_id), K(abs_timeout_us), K(ret));
  }
  return ret;
}

int ObBucketLock::unlock(const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if(OB_UNLIKELY(bucket_idx >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(bucket_idx), K_(bucket_cnt), K(ret));
  } else if (OB_FAIL(latches_[bucket_to_latch_idx(bucket_idx)].unlock())) {
    COMMON_LOG(WARN, "Fail to unlock latch, ", K(bucket_idx), K(ret));
  }
  return ret;
}

int ObBucketLock::rdlock_latch_idx(const uint64_t latch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(latch_idx >= latch_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(latch_idx), K(latch_cnt_));
  } else if (OB_FAIL(latches_[latch_idx].rdlock(latch_id_))) {
    COMMON_LOG(WARN, "failed to rd lock", K(ret), K(latch_idx), K(latch_id_));
  }
  return ret;
}

int ObBucketLock::wrlock_latch_idx(const uint64_t latch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(latch_idx >= latch_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(latch_idx), K(latch_cnt_));
  } else if (OB_FAIL(latches_[latch_idx].wrlock(latch_id_))) {
    COMMON_LOG(WARN, "failed to wrlock", K(ret), K(latch_idx), K(latch_id_));
  }
  return ret;
}

int ObBucketLock::unlock_latch_idx(const uint64_t latch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(latch_idx >= latch_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(latch_idx), K(latch_cnt_));
  } else if (OB_FAIL(latches_[latch_idx].unlock())) {
    COMMON_LOG(WARN, "failed to unlock", K(ret), K(latch_idx), K(latch_id_));
  }
  return ret;
}

int ObBucketLock::wrlock_all()
{
  int ret = OB_SUCCESS;
  int64_t last_succ_idx = -1;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < latch_cnt_; ++i) {
      int tmp_ret = latches_[i].wrlock(latch_id_);
      if (OB_SUCCESS == tmp_ret) {
        last_succ_idx = i;
      } else {
        ret = tmp_ret;
        if(OB_EAGAIN != tmp_ret) {
          COMMON_LOG(WARN, "failed to try wrlock", K(ret), K(i), K(latch_cnt_), K(latch_id_));
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = last_succ_idx; i >= 0; --i) {
        latches_[i].unlock();
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  COMMON_LOG(DEBUG, "wrlock all", K_(latch_id), K_(latch_cnt), K(cost_ts), K(ret));
  return ret;
}

int ObBucketLock::try_wrlock_all()
{
  const bool is_write_lock = true;
  return try_lock_all(is_write_lock);
}

int ObBucketLock::try_rdlock_all()
{
  const bool is_write_lock = false;
  return try_lock_all(is_write_lock);
}

int ObBucketLock::try_lock_all(const bool is_write_lock)
{
  int ret = OB_SUCCESS;
  int64_t last_succ_idx = -1;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < latch_cnt_; ++i) {
      if (is_write_lock) {
        ret = latches_[i].try_wrlock(latch_id_);
      } else {
        ret = latches_[i].try_rdlock(latch_id_);
      }
      if (OB_SUCCESS == ret) {
        last_succ_idx = i;
      } else if(OB_EAGAIN != ret) {
        COMMON_LOG(WARN, "failed to try wrlock", K(ret), K(is_write_lock), K(i), K(latch_cnt_), K(latch_id_));
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = last_succ_idx; i >= 0; --i) {
        latches_[i].unlock();
      }
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  COMMON_LOG(DEBUG, "try lock all", K(is_write_lock), K_(latch_id), K_(latch_cnt), K(cost_ts), K(ret));
  return ret;
}

int ObBucketLock::unlock_all()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObBucketLock has not been inited, ", K(ret));
  } else {
    for (int64_t i = latch_cnt_ - 1; OB_SUCC(ret) && i >= 0; --i) {
      latches_[i].unlock();
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  COMMON_LOG(DEBUG, "unlock all", K_(latch_id), K_(latch_cnt), K(cost_ts), K(ret));
  return ret;
}

uint64_t ObBucketLock::get_latch_idx(const uint64_t hash_value) const
{
  uint64_t latch_idx = 0;
  latch_idx = bucket_to_latch_idx(get_bucket_idx(hash_value));
  return latch_idx;
}

ObMultiBucketLockGuard::ObMultiBucketLockGuard(ObBucketLock &lock, const bool is_write_lock)
 : lock_(lock),
   is_write_lock_(is_write_lock),
   latch_array_()
{
};

ObMultiBucketLockGuard::~ObMultiBucketLockGuard()
{
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < latch_array_.count(); ++i) {
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = lock_.unlock_latch_idx(latch_array_.at(i))))) {
      COMMON_LOG_RET(ERROR, tmp_ret, "Fail to unlock bucket, ", K(tmp_ret), "latch_idx", latch_array_.at(i));
    }
  }
};

int ObMultiBucketLockGuard::lock_multi_buckets(ObIArray<uint64_t> &hash_array)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> latch_idx_set;
  int64_t next_lock_idx = 0;

  if (latch_array_.count() > 0) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "cannot lock twice", K(ret), "count", latch_array_.count());
  } else if (OB_FAIL(latch_idx_set.create(hash_array.count()))) {
    COMMON_LOG(WARN, "failed to create latch idx set", K(ret), "count", hash_array.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hash_array.count(); ++i) {
      const uint64_t latch_idx = lock_.get_latch_idx(hash_array.at(i));
      if (OB_FAIL(latch_idx_set.set_refactored(latch_idx))) {
        COMMON_LOG(WARN, "failed to set latch idx set", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (common::hash::ObHashSet<uint64_t>::iterator it = latch_idx_set.begin();
        OB_SUCC(ret) && it != latch_idx_set.end();
        ++it) {
      uint64_t latch_idx = it->first;
      if (OB_FAIL(latch_array_.push_back(latch_idx))) {
        COMMON_LOG(WARN, "failed to add latch idx", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    std::sort(&latch_array_[0], &latch_array_[0] + latch_array_.count());
    for (; OB_SUCC(ret) && next_lock_idx < latch_array_.count(); ++next_lock_idx) {
      const uint64_t latch_idx = latch_array_.at(next_lock_idx);
      if (is_write_lock_) {
        if (OB_FAIL(lock_.wrlock_latch_idx(latch_idx))) {
          COMMON_LOG(WARN, "failed to wrlock latch idx", K(ret), K(next_lock_idx), K(latch_idx));
        }
      } else {
        if (OB_FAIL(lock_.rdlock_latch_idx(latch_idx))) {
          COMMON_LOG(WARN, "failed to rdlock latch idx", K(ret), K(next_lock_idx), K(latch_idx));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // rollback
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < next_lock_idx; ++i) {
      const uint64_t latch_idx = latch_array_.at(i);
      if(OB_SUCCESS != (tmp_ret = lock_.unlock_latch_idx(latch_idx))) {
        COMMON_LOG(ERROR, "failed to unlock latch", K(ret), K(i), K(latch_idx));
      }
    }
    latch_array_.reset();
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
