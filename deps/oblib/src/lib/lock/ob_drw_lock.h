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
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
class DRWLock
{
public:
  explicit DRWLock(const uint32_t latch_id = ObLatchIds::DEFAULT_DRW_LOCK) : latches_(), latch_id_(latch_id) {}
  ~DRWLock() {}
  inline int rdlock();
  inline int try_rdlock();
  inline int rdunlock();
  inline int wrlock(int64_t timeout=INT64_MAX);
  inline int wrunlock();
  inline int try_wrlock();
  class RDLockGuard
  {
  public:
    [[nodiscard]] explicit RDLockGuard(DRWLock &rwlock): rwlock_(rwlock), ret_(OB_SUCCESS)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.rdlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to read lock, ", K_(ret));
      }
    }
    ~RDLockGuard()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.rdunlock()))) {
          COMMON_LOG_RET(WARN, ret_, "Fail to read unlock, ", K_(ret));
        }
      }
    }
    inline int get_ret() const { return ret_; }
  private:
    DRWLock &rwlock_;
    int ret_;
  private:
    DISALLOW_COPY_AND_ASSIGN(RDLockGuard);
  };
  class WRLockGuard
  {
  public:
    [[nodiscard]] explicit WRLockGuard(DRWLock &rwlock): rwlock_(rwlock), ret_(OB_SUCCESS)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to write lock, ", K_(ret));
      }
    }
    ~WRLockGuard()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrunlock()))) {
          COMMON_LOG_RET(WARN, ret_, "Fail to write unlock, ", K_(ret));
        }
      }
    }
    inline int get_ret() const { return ret_; }
  private:
    DRWLock &rwlock_;
    int ret_;
  private:
    DISALLOW_COPY_AND_ASSIGN(WRLockGuard);
  };
  class WRLockGuardRetryTimeout
  {
  public:
    [[nodiscard]] explicit WRLockGuardRetryTimeout(DRWLock &rwlock, int64_t timeout): rwlock_(rwlock), ret_(OB_SUCCESS)
    {
      while(OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrlock(timeout)))) {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          COMMON_LOG_RET(WARN, ret_, "Fail to write lock for 10s, ", K_(ret));
        }
        ob_usleep(static_cast<uint32_t>(timeout));
      }
    }
    ~WRLockGuardRetryTimeout()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = rwlock_.wrunlock()))) {
          COMMON_LOG_RET(WARN, ret_, "Fail to write unlock, ", K_(ret));
        }
      }
    }
    inline int get_ret() const { return ret_; }
  private:
    DRWLock &rwlock_;
    int ret_;
  private:
    DISALLOW_COPY_AND_ASSIGN(WRLockGuardRetryTimeout);
  };
private:
  struct AlignedLatch
  {
    ObLatch latch_;
  } CACHE_ALIGNED;
  AlignedLatch latches_[OB_MAX_CPU_NUM];
  uint32_t latch_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(DRWLock);
};

/**
 * --------------------------------------------------------Inline Methods-----------------------------------------------------------
 */
inline int DRWLock::rdlock()
{
  return latches_[get_itid() % OB_MAX_CPU_NUM].latch_.rdlock(latch_id_);
}

inline int DRWLock::rdunlock()
{
  return latches_[get_itid() % OB_MAX_CPU_NUM].latch_.unlock();
}

inline int DRWLock::wrlock(int64_t timeout)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t i = 0;
  for (i = 0; i < OB_MAX_CPU_NUM; ++i) {
    if (OB_FAIL(latches_[i].latch_.wrlock(latch_id_, timeout))) {
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

}//common
}//oceanbase

#endif /* OB_DRW_LOCK_H_ */
