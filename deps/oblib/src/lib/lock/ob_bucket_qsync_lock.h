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
#ifndef OCEANBASE_SHARE_LOCK_OB_BUCKET_QSYNC_LOCK_H_
#define OCEANBASE_SHARE_LOCK_OB_BUCKET_QSYNC_LOCK_H_

#include "common/ob_clock_generator.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObQSyncLock;

class ObBucketQSyncLock
{
public:
  ObBucketQSyncLock();
  virtual ~ObBucketQSyncLock();
  int init(
      const uint64_t bucket_cnt,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID,
      const lib::ObLabel &label = ObModIds::BUCKET_LOCK);
  void destroy();
  int rdlock(const uint64_t bucket_idx);
  int wrlock(const uint64_t bucket_idx);
  int rdunlock(const uint64_t bucket_idx);
  int wrunlock(const uint64_t bucket_idx);
  int wrlock_all();
  int wrunlock_all();
  int try_rdlock_all();
  int rdunlock_all();
  TO_STRING_KV(K_(bucket_cnt), K_(is_inited));
private:
  friend class ObMultiBucketQSyncLockGuard;
  friend class ObBucketHashQSyncRLockGuard;
  friend class ObBucketHashQSyncWLockGuard;
  inline uint64_t get_bucket_idx(const uint64_t hash_value) const;
  int try_lock_all(const bool is_write_lock);
private:
  uint64_t bucket_cnt_;
  ObQSyncLock *locks_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketQSyncLock);
};

uint64_t ObBucketQSyncLock::get_bucket_idx(const uint64_t hash_value) const
{
  uint64_t bucket_idx = 0;
  if (0 != bucket_cnt_) {
    bucket_idx = hash_value % bucket_cnt_;
  }
  return bucket_idx;
}

class ObBucketQSyncRLockGuard final
{
public:
  [[nodiscard]] ObBucketQSyncRLockGuard(ObBucketQSyncLock &lock, const uint64_t bucket_index)
     : lock_(lock),
       index_(bucket_index),
       lock_start_ts_(0),
       ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock(index_)))) {
      SHARE_LOG_RET(WARN, ret_, "Fail to read lock bucket, ", K_(index), K_(ret));
    } else {
      lock_start_ts_ = ObClockGenerator::getClock();;
    }
  };
  ~ObBucketQSyncRLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdunlock(index_)))) {
        SHARE_LOG_RET(WARN,  ret_, "Fail to unlock bucket, ", K_(ret));
      } else {
        const int64_t lock_end_ts = ObClockGenerator::getClock();;
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          SHARE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketQSyncLock &lock_;
  const uint64_t index_;
  int64_t lock_start_ts_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketQSyncRLockGuard);
};

class ObBucketQSyncWLockGuard final
{
public:
  [[nodiscard]] ObBucketQSyncWLockGuard(ObBucketQSyncLock &lock, const uint64_t bucket_index)
     : lock_(lock),
       index_(bucket_index),
       lock_start_ts_(0),
       ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock(index_)))) {
      SHARE_LOG_RET(WARN, ret_, "Fail to write lock bucket, ", K_(index), K_(ret));
    } else {
      lock_start_ts_ = ObClockGenerator::getClock();;
    }
  };
  ~ObBucketQSyncWLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrunlock(index_)))) {
        SHARE_LOG_RET(WARN, ret_, "Fail to unlock bucket, ", K_(index), K_(ret));
      } else {
        const int64_t lock_end_ts = ObClockGenerator::getClock();;
        if (lock_end_ts - lock_start_ts_ > 5 * 1000 * 1000) {
          SHARE_LOG(INFO, "bucket lock handle cost too much time",
                                            K_(lock_start_ts),
                                            "cost_us", lock_end_ts - lock_start_ts_,
                                            KCSTRING(lbt()));
        }
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketQSyncLock &lock_;
  const uint64_t index_;
  int64_t lock_start_ts_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketQSyncWLockGuard);
};

class ObBucketHashQSyncRLockGuard final
{
public:
  [[nodiscard]] ObBucketHashQSyncRLockGuard(ObBucketQSyncLock &lock, const uint64_t hash_value)
    : guard_(lock, lock.get_bucket_idx(hash_value))
  {
  }
  ~ObBucketHashQSyncRLockGuard() {}
  inline int get_ret() const { return guard_.get_ret(); }
private:
  ObBucketQSyncRLockGuard guard_;
  DISALLOW_COPY_AND_ASSIGN(ObBucketHashQSyncRLockGuard);
};

class ObBucketHashQSyncWLockGuard final
{
public:
  [[nodiscard]] ObBucketHashQSyncWLockGuard(ObBucketQSyncLock &lock, const uint64_t hash_value)
    : guard_(lock, lock.get_bucket_idx(hash_value))
  {
  }
  ~ObBucketHashQSyncWLockGuard() {}
  inline int get_ret() const { return guard_.get_ret(); }
private:
  ObBucketQSyncWLockGuard guard_;
  DISALLOW_COPY_AND_ASSIGN(ObBucketHashQSyncWLockGuard);
};

class ObBucketQSyncWLockAllGuard final
{
public:
  [[nodiscard]] ObBucketQSyncWLockAllGuard(ObBucketQSyncLock &lock)
     : lock_(lock),
       lock_start_ts_(0),
       ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock_all()))) {
      SHARE_LOG_RET(WARN, ret_, "Fail to try write lock all buckets", K_(ret));
    } else {
      lock_start_ts_ = ObClockGenerator::getClock();;
    }
  };
  ~ObBucketQSyncWLockAllGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrunlock_all()))) {
        SHARE_LOG_RET(WARN, ret_, "Fail to unlock all buckets, ", K_(ret));
      } else {
        const int64_t lock_end_ts = ObClockGenerator::getClock();;
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
  ObBucketQSyncLock &lock_;
  int64_t lock_start_ts_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketQSyncWLockAllGuard);
};

class ObBucketTryQSyncRLockAllGuard final
{
public:
  [[nodiscard]] ObBucketTryQSyncRLockAllGuard(ObBucketQSyncLock &lock)
     : lock_(lock),
       lock_start_ts_(0),
       ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.try_rdlock_all()))) {
      if (OB_EAGAIN != ret_) {
        SHARE_LOG_RET(WARN, ret_, "Fail to try read lock all buckets", K_(ret), K(lbt()));
      } else {
        if (REACH_COUNT_INTERVAL(1000)) {// print one log per 1000 times.
          SHARE_LOG_RET(WARN, ret_, "fail to lock all buckets, need to try again", K_(ret), K(lbt()));
        }
      }
    } else {
      lock_start_ts_ = ObClockGenerator::getClock();;
    }
  };
  ~ObBucketTryQSyncRLockAllGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdunlock_all()))) {
        SHARE_LOG_RET(WARN, ret_, "Fail to unlock all buckets, ", K_(ret));
      } else {
        const int64_t lock_end_ts = ObClockGenerator::getClock();;
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
  ObBucketQSyncLock &lock_;
  int64_t lock_start_ts_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketTryQSyncRLockAllGuard);
};
} // namespace common
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOCK_OB_BUCKET_QSYNC_LOCK_H_ */
