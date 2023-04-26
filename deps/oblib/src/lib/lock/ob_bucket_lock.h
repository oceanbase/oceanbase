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

#ifndef OB_BUCKET_LOCK_H_
#define OB_BUCKET_LOCK_H_
#include "lib/lock/ob_latch.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/container/ob_array.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
class ObBucketLock
{
public:
  ObBucketLock();
  virtual ~ObBucketLock();
  int init(
      const uint64_t bucket_cnt,
      const uint32_t latch_id = ObLatchIds::DEFAULT_BUCKET_LOCK,
      const lib::ObLabel &label = ObModIds::BUCKET_LOCK,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  int init(
      const uint64_t bucket_cnt,
      const uint32_t latch_id,
      const lib::ObMemAttr &attr);
  void destroy();
  int try_rdlock(const uint64_t bucket_idx);
  int try_wrlock(const uint64_t bucket_idx);
  int rdlock(const uint64_t bucket_idx, const int64_t abs_timeout_us = INT64_MAX);
  int wrlock(const uint64_t bucket_idx, const int64_t abs_timeout_us = INT64_MAX);
  int unlock(const uint64_t bucket_idx);
  int try_wrlock_all();
  int try_rdlock_all();
  int wrlock_all();
  int unlock_all();
  TO_STRING_KV(K_(bucket_cnt), K_(latch_cnt), K_(latch_id));
private:
  friend class ObMultiBucketLockGuard;
  friend class ObBucketHashRLockGuard;
  friend class ObBucketHashWLockGuard;
  uint64_t get_latch_count() const { return latch_cnt_; }
  uint64_t get_bucket_count() const { return bucket_cnt_; }
  uint64_t get_latch_idx(const uint64_t hash_value) const;
  OB_INLINE uint64_t get_bucket_idx(const uint64_t hash_value) const;
  int rdlock_latch_idx(const uint64_t latch_idx);
  int wrlock_latch_idx(const uint64_t latch_idx);
  int unlock_latch_idx(const uint64_t latch_idx);
  OB_INLINE uint64_t bucket_to_latch_idx(const uint64_t bucket_idx) const;
  int try_lock_all(const bool is_write_lock);
private:
  lib::ObMemAttr attr_;
  uint64_t bucket_cnt_;
  uint64_t latch_cnt_;
  ObLatch *latches_;
  uint32_t latch_id_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketLock);
};

class ObBucketRLockGuard final
{
public:
  [[nodiscard]] ObBucketRLockGuard(ObBucketLock &lock, const uint64_t bucket_index)
     : lock_(lock),
       index_(bucket_index),
       ret_(OB_SUCCESS),
       lock_start_ts_(0)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock(index_)))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to read lock bucket, ", K_(index), K_(ret));
    } else {
      lock_start_ts_ = common::ObClockGenerator::getClock();
    }
  };
  ~ObBucketRLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock(index_)))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock bucket, ", K_(ret));
      } else {
        const int64_t lock_end_ts = common::ObClockGenerator::getClock();
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          STORAGE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  const uint64_t index_;
  int ret_;
  int64_t lock_start_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketRLockGuard);
};

class ObBucketWLockGuard final
{
public:
  [[nodiscard]] ObBucketWLockGuard(ObBucketLock &lock, const uint64_t bucket_index)
     : lock_(lock),
       index_(bucket_index),
       ret_(OB_SUCCESS),
       lock_start_ts_(0)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock(index_)))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to write lock bucket, ", K_(index), K_(ret));
    } else {
      lock_start_ts_ = common::ObClockGenerator::getClock();
    }
  };
  ~ObBucketWLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock(index_)))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock bucket, ", K_(index), K_(ret));
      } else {
        const int64_t lock_end_ts = common::ObClockGenerator::getClock();
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          STORAGE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  const uint64_t index_;
  int ret_;
  int64_t lock_start_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketWLockGuard);
};

class ObBucketWLockAllGuard final
{
public:
  [[nodiscard]] ObBucketWLockAllGuard(ObBucketLock &lock)
     : lock_(lock),
       ret_(OB_SUCCESS),
       lock_start_ts_(0)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock_all()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to try write lock all buckets", K_(ret));
    } else {
      lock_start_ts_ = ObTimeUtility::current_time();
    }
  };
  ~ObBucketWLockAllGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock_all()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock all buckets, ", K_(ret));
      } else {
        const int64_t lock_end_ts = ObTimeUtility::current_time();
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          STORAGE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  int ret_;
  int64_t lock_start_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketWLockAllGuard);
};

class ObBucketTryWLockAllGuard final
{
public:
  [[nodiscard]] ObBucketTryWLockAllGuard(ObBucketLock &lock)
     : lock_(lock),
       ret_(OB_SUCCESS),
       lock_start_ts_(0)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.try_wrlock_all()))) {
      if (OB_EAGAIN != ret_) {
        COMMON_LOG_RET(WARN, ret_, "Fail to try read lock all buckets", K_(ret), K(lbt()));
      } else {
        if (REACH_COUNT_INTERVAL(1000)) {// print one log per 1000 times.
          COMMON_LOG_RET(WARN, ret_, "fail to lock all buckets, need to try again", K_(ret), K(lbt()));
        }
      }
    } else {
      lock_start_ts_ = ObTimeUtility::current_time();
    }
  };
  ~ObBucketTryWLockAllGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock_all()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock all buckets, ", K_(ret));
      } else {
        const int64_t lock_end_ts = ObTimeUtility::current_time();
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          STORAGE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  int ret_;
  int64_t lock_start_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketTryWLockAllGuard);
};

class ObBucketTryRLockAllGuard final
{
public:
  [[nodiscard]] ObBucketTryRLockAllGuard(ObBucketLock &lock)
     : lock_(lock),
       ret_(OB_SUCCESS),
       lock_start_ts_(0)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.try_rdlock_all()))) {
      if (OB_EAGAIN != ret_) {
        COMMON_LOG_RET(WARN, ret_, "Fail to try read lock all buckets", K_(ret), K(lbt()));
      } else {
        if (REACH_COUNT_INTERVAL(1000)) {// print one log per 1000 times.
          COMMON_LOG_RET(WARN, ret_, "fail to lock all buckets, need to try again", K_(ret), K(lbt()));
        }
      }
    } else {
      lock_start_ts_ = common::ObClockGenerator::getClock();
    }
  };
  ~ObBucketTryRLockAllGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock_all()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock all buckets, ", K_(ret));
      } else {
        const int64_t lock_end_ts = common::ObClockGenerator::getClock();
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          STORAGE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  int ret_;
  int64_t lock_start_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketTryRLockAllGuard);
};

class ObMultiBucketLockGuard final
{
public:
  [[nodiscard]] ObMultiBucketLockGuard(ObBucketLock &lock, const bool is_write_lock);
  ~ObMultiBucketLockGuard();
  int lock_multi_buckets(ObIArray<uint64_t> &hash_array);
private:
  ObBucketLock &lock_;
  bool is_write_lock_;
  ObArray<uint64_t> latch_array_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiBucketLockGuard);
};

class ObBucketHashRLockGuard final
{
public:
  [[nodiscard]] ObBucketHashRLockGuard(ObBucketLock &lock, const uint64_t hash_value)
    : guard_(lock, lock.get_bucket_idx(hash_value))
  {
  }
  ~ObBucketHashRLockGuard() {}
  inline int get_ret() const { return guard_.get_ret(); }
private:
  ObBucketRLockGuard guard_;
  DISALLOW_COPY_AND_ASSIGN(ObBucketHashRLockGuard);
};

class ObBucketHashWLockGuard final
{
public:
  [[nodiscard]] ObBucketHashWLockGuard(ObBucketLock &lock, const uint64_t hash_value)
    : guard_(lock, lock.get_bucket_idx(hash_value))
  {
  }
  ~ObBucketHashWLockGuard() {}
  inline int get_ret() const { return guard_.get_ret(); }
private:
  ObBucketWLockGuard guard_;
  DISALLOW_COPY_AND_ASSIGN(ObBucketHashWLockGuard);
};

uint64_t ObBucketLock::bucket_to_latch_idx(const uint64_t bucket_idx) const
{
  return bucket_idx / 8;
}


uint64_t ObBucketLock::get_bucket_idx(const uint64_t hash_value) const
{
  uint64_t bucket_idx = 0;
  if (0 != bucket_cnt_) {
    bucket_idx = hash_value % bucket_cnt_;
  }
  return bucket_idx;
}
} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_BUCKET_LOCK_H_ */
