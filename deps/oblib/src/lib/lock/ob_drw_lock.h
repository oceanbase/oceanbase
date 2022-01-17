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

#ifndef OB_DRW_LOCK_H_
#define OB_DRW_LOCK_H_
#include "lib/lock/ob_latch.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
namespace common {
class DRWLock {
public:
  explicit DRWLock(const uint32_t latch_id = ObLatchIds::DEFAULT_DRW_LOCK) : latches_(), latch_id_(latch_id)
  {}
  ~DRWLock()
  {}
  inline int rdlock();
  inline int try_rdlock();
  inline int rdunlock();
  inline int wrlock();
  inline int wrlock(const int64_t timeout_us);
  inline int wrunlock();
  inline int try_wrlock();
  class RDLockGuard {
  public:
    explicit RDLockGuard(DRWLock& rwlock) : rwlock_(rwlock), ret_(OB_SUCCESS)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.rdlock()))) {
        COMMON_LOG(WARN, "Fail to read lock, ", K_(ret));
      }
    }
    ~RDLockGuard()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.rdunlock()))) {
          COMMON_LOG(WARN, "Fail to read unlock, ", K_(ret));
        }
      }
    }
    inline int get_ret() const
    {
      return ret_;
    }

  private:
    DRWLock& rwlock_;
    int ret_;

  private:
    DISALLOW_COPY_AND_ASSIGN(RDLockGuard);
  };
  class WRLockGuard {
  public:
    explicit WRLockGuard(DRWLock& rwlock) : rwlock_(rwlock), ret_(OB_SUCCESS)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrlock()))) {
        COMMON_LOG(WARN, "Fail to write lock, ", K_(ret));
      }
    }
    ~WRLockGuard()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrunlock()))) {
          COMMON_LOG(WARN, "Fail to write unlock, ", K_(ret));
        }
      }
    }
    inline int get_ret() const
    {
      return ret_;
    }

  private:
    DRWLock& rwlock_;
    int ret_;

  private:
    DISALLOW_COPY_AND_ASSIGN(WRLockGuard);
  };
  class WRRetryLockGuard {
  public:
    WRRetryLockGuard(DRWLock& rwlock) : rwlock_(rwlock), ret_(OB_SUCCESS)
    {
      const int64_t start_us = ObTimeUtility::current_time();
      int64_t retry_timeout_us = BASE_US;
      while (OB_SUCCESS != (ret_ = rwlock_.wrlock(retry_timeout_us))) {
        int64_t current_us = ObTimeUtility::current_time();
        retry_timeout_us = (retry_timeout_us + BASE_US < UPPER_US) ? (retry_timeout_us + BASE_US) : UPPER_US;
        if (10000000 /*10s*/ < (current_us - start_us)) {
          COMMON_LOG(WARN, "acquire wrlock use too much time, ", "total_time_us", (current_us - start_us));
        }
        usleep(100000 /*100ms*/);
      }
    }
    ~WRRetryLockGuard()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrunlock()))) {
          COMMON_LOG(WARN, "Fail to write unlock, ", K_(ret));
        }
      }
    }
    inline int get_ret() const
    {
      return ret_;
    }

  private:
    static const int64_t BASE_US = 100 * 1000;        // 100 ms
    static const int64_t UPPER_US = 3 * 1000 * 1000;  // 3s
  private:
    DRWLock& rwlock_;
    int ret_;

  private:
    DISALLOW_COPY_AND_ASSIGN(WRRetryLockGuard);
  };

private:
  struct AlignedLatch {
    ObLatch latch_;
  } CACHE_ALIGNED;
  AlignedLatch latches_[OB_MAX_CPU_NUM];
  uint32_t latch_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(DRWLock);
};

/**
 * --------------------------------------------------------Inline
 * Methods-----------------------------------------------------------
 */
inline int DRWLock::rdlock()
{
  return latches_[get_itid() % OB_MAX_CPU_NUM].latch_.rdlock(latch_id_);
}

inline int DRWLock::rdunlock()
{
  return latches_[get_itid() % OB_MAX_CPU_NUM].latch_.unlock();
}

inline int DRWLock::wrlock()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t i = 0;
  for (i = 0; i < OB_MAX_CPU_NUM; ++i) {
    if (OB_FAIL(latches_[i].latch_.wrlock(latch_id_))) {
      COMMON_LOG(WARN, "Fail to lock latch, ", K(i), K(ret));
      break;
    }
  }
  if (OB_FAIL(ret)) {
    for (--i; i >= 0; --i) {
      if (OB_SUCCESS != (tmp_ret = latches_[i].latch_.unlock())) {
        COMMON_LOG(WARN, "Fail to unlock latch, ", K(i), K(tmp_ret));
      }
    }
  }
  return ret;
}

inline int DRWLock::wrlock(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + timeout_us;
  int64_t i = 0;
  for (i = 0; i < OB_MAX_CPU_NUM; ++i) {
    if (OB_FAIL(latches_[i].latch_.wrlock(latch_id_, abs_timeout_us))) {
      COMMON_LOG(WARN, "Fail to lock latch, ", K(i), K(ret));
      break;
    }
  }
  if (OB_FAIL(ret)) {
    for (--i; i >= 0; --i) {
      if (OB_SUCCESS != (tmp_ret = latches_[i].latch_.unlock())) {
        COMMON_LOG(WARN, "Fail to unlock latch, ", K(i), K(tmp_ret));
      }
    }
  }
  return ret;
}

inline int DRWLock::wrunlock()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = OB_MAX_CPU_NUM - 1; i >= 0; --i) {
    if (OB_SUCCESS != (tmp_ret = latches_[i].latch_.unlock())) {
      ret = tmp_ret;
      COMMON_LOG(WARN, "Fail to unlock latch, ", K(i), K(tmp_ret));
    }
  }
  return ret;
}

inline int DRWLock::try_rdlock()
{
  return latches_[get_itid() % OB_MAX_CPU_NUM].latch_.try_rdlock(latch_id_);
}

inline int DRWLock::try_wrlock()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t i = 0;
  for (i = 0; i < OB_MAX_CPU_NUM; ++i) {
    if (OB_FAIL(latches_[i].latch_.try_wrlock(latch_id_))) {
      COMMON_LOG(WARN, "Fail to try lock latch, ", K(i), K(ret));
      break;
    }
  }
  if (OB_FAIL(ret)) {
    for (--i; i >= 0; --i) {
      if (OB_SUCCESS != (tmp_ret = latches_[i].latch_.unlock())) {
        COMMON_LOG(WARN, "Fail to try unlock latch, ", K(i), K(tmp_ret));
      }
    }
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase

#endif /* OB_DRW_LOCK_H_ */
