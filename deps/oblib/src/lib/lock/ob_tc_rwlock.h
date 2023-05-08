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

#ifndef OCEANBASE_LOCK_TC_RWLOCK_H_
#define OCEANBASE_LOCK_TC_RWLOCK_H_
#include "lib/lock/ob_tc_ref.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/lock/ob_latch.h"
#include "lib/time/ob_time_utility.h"

#define USE_TCRWLOCK 1
namespace oceanbase
{
namespace common
{
#if USE_TCRWLOCK

class LWaitCond
{
public:
  LWaitCond(): futex_() {}
  ~LWaitCond() {}
  void signal() {
    auto &ready = futex_.val();
    if (!ATOMIC_LOAD(&ready) && ATOMIC_BCAS(&ready, 0, 1)) {
      futex_.wake(INT32_MAX);
    }
  }
  bool wait(int64_t timeout)  {
    auto& ready = futex_.val();
    if (!ATOMIC_LOAD(&ready)) {
      futex_.wait(0, timeout);
    } else {
      ATOMIC_STORE(&ready, 0);
    }
    return ready;
  }
private:
  lib::ObFutex futex_;
};

class TCRWLock
{
public:
  enum { WRITE_MASK = 1<<31 };
  struct RLockGuard
  {
    [[nodiscard]] explicit RLockGuard(TCRWLock& lock): lock_(lock) { lock_.rdlock(); }
    ~RLockGuard() { lock_.rdunlock(); }
    TCRWLock& lock_;
  };
  struct RLockGuardWithTimeout
  {
    [[nodiscard]] explicit RLockGuardWithTimeout(TCRWLock &lock, const int64_t abs_timeout_us, int &ret): lock_(lock), need_unlock_(true)
    {
      if (OB_FAIL(lock_.rdlock(abs_timeout_us))) {
        need_unlock_ = false;
        COMMON_LOG(WARN, "RLockGuardWithTimeout failed", K(ret), K(abs_timeout_us));
      } else {
        need_unlock_ = true;
      }
    }
    ~RLockGuardWithTimeout()
    {
      if (need_unlock_) {
        lock_.rdunlock();
      }
    }
    TCRWLock &lock_;
    bool need_unlock_;
  };
  struct WLockGuard
  {
    [[nodiscard]] explicit WLockGuard(TCRWLock& lock): lock_(lock) { lock_.wrlock(); }
    ~WLockGuard() { lock_.wrunlock(); }
    TCRWLock& lock_;
  };
  struct WLockGuardWithTimeout
  {
    [[nodiscard]] explicit WLockGuardWithTimeout(TCRWLock &lock, const int64_t abs_timeout_us, int &ret): lock_(lock), need_unlock_(true)
    {
      if (OB_FAIL(lock_.wrlock(abs_timeout_us))) {
        need_unlock_ = false;
        COMMON_LOG(WARN, "WLockGuardWithTimeout failed", K(ret), K(abs_timeout_us));
      } else {
        need_unlock_ = true;
      }
    }
    ~WLockGuardWithTimeout()
    {
      if (need_unlock_) {
        lock_.wrunlock();
      }
    }
    TCRWLock &lock_;
    bool need_unlock_;
  };
  struct WLockGuardWithRetry
  {
    [[nodiscard]] explicit WLockGuardWithRetry(TCRWLock &lock, const int64_t abs_timeout_us) : lock_(lock)
    {
      lock_.wrlock_with_retry(abs_timeout_us);
    }
    ~WLockGuardWithRetry() { lock_.wrunlock(); }
    TCRWLock &lock_;
  };
  struct WLockGuardWithRetryInterval
  {
    [[nodiscard]] explicit WLockGuardWithRetryInterval(TCRWLock &lock,
                                         const int64_t try_thresold_us,
                                         const uint32_t retry_interval_us)
      : lock_(lock)
    {
      int ret = OB_SUCCESS;
      bool locked = false;
      int64_t abs_timeout_us = 0;
      while (!locked) {
        abs_timeout_us = common::ObTimeUtility::current_time() + try_thresold_us;
        if (OB_FAIL(lock_.wrlock(abs_timeout_us))) {
          // sleep retry_interval_us when wrlock failed, then retry
          ::usleep(retry_interval_us);
        } else {
          locked = true;
        }
      }
    }
    ~WLockGuardWithRetryInterval() { lock_.wrunlock(); }
    TCRWLock &lock_;
  };

  TCRWLock(const uint32_t latch_id=ObLatchIds::DEFAULT_SPIN_RWLOCK, TCRef &tcref=get_global_tcref())
    : latch_(), latch_id_(latch_id), tcref_(tcref), write_id_(0), read_ref_(0) {}
  void set_latch_id(const uint32_t latch_id) { latch_id_ = latch_id; }
  int rdlock(const int64_t abs_timeout_us = INT64_MAX)
  {
    int ret = OB_SUCCESS;
    while(OB_SUCC(ret) && !try_rdlock()) {
      if (OB_SUCC(latch_.rdlock(latch_id_, abs_timeout_us))) {
        ret = latch_.unlock();
      }
    }
    if (OB_SUCC(ret)) {
      // record in try_rdlock will be overwrited by latch_.rdlock, so record again.
      IGNORE_RETURN ObLatch::reg_lock((uint32_t*)&(latch_.lock_));
    }
    return ret;
  }
  inline bool try_rdlock()
  {
    bool locked = false;
    if (OB_LIKELY(0 == ATOMIC_LOAD(&write_id_))) {
      get_tcref().inc_ref(&read_ref_);
      if (OB_LIKELY(0 == ATOMIC_LOAD(&write_id_))) {
        locked = true;
        IGNORE_RETURN ObLatch::reg_lock((uint32_t*)&(latch_.lock_));
      } else {
        get_tcref().dec_ref(&read_ref_);
        lcond_.signal();
      }
    }
    return locked;
  }
  inline int rdunlock()
  {
    int ret = OB_SUCCESS;
    get_tcref().dec_ref(&read_ref_);
    IGNORE_RETURN ObLatch::unreg_lock((uint32_t*)&(latch_.lock_));
    lcond_.signal();
    return ret;
  }
  inline int wrlock(const int64_t abs_timeout_us = INT64_MAX)
  {
    int ret = OB_SUCCESS;
    if (OB_SUCC(latch_.wrlock(latch_id_, abs_timeout_us))) {
      int32_t itid = (int32_t)get_itid() + 1;
      ATOMIC_STORE(&write_id_, itid);
      get_tcref().sync(&read_ref_);
      int64_t ttl = 0;
      // although we know that waiting myself is meanless,
      // but it is helpful for us to understand the lock logic.
      ObLatch::current_wait = (uint32_t*)&(latch_.lock_);
      while(0 != ATOMIC_LOAD(&read_ref_)
            && (ttl = abs_timeout_us - ObTimeUtility::current_time()) >= 0) {
        lcond_.wait(std::min(ttl, (int64_t)10 * 1000));
      }
      if (ttl < 0) {
        ret = OB_EAGAIN;
        ATOMIC_STORE(&write_id_, 0);
        (void)latch_.unlock();
      } else {
        ATOMIC_STORE(&write_id_, itid | WRITE_MASK);
      }
      ObLatch::current_wait = nullptr;
    }
    return ret;
  }
  inline int wrlock_with_retry(const int64_t abs_timeout_us)
  {
    int ret = OB_SUCCESS;
    bool locked = false;
    const int64_t WRLOCK_FAILED_SLEEP_US = 1 * 1000; // 1ms
    while (!locked) {
      if (OB_FAIL(wrlock(abs_timeout_us))) {
        COMMON_LOG(WARN, "wrlock_with_retry failed", K(ret), K(abs_timeout_us));
        ::usleep(WRLOCK_FAILED_SLEEP_US);
      } else {
        locked = true;
      }
    }
    return ret;
  }
  inline int wrunlock()
  {
    int ret = OB_SUCCESS;
    ATOMIC_STORE(&write_id_, 0);
    ret = latch_.unlock();
    return ret;
  }
  inline bool try_wrlock()
  {
    bool locked = false;
    int32_t itid = (int32_t)get_itid() + 1;
    if (OB_SUCCESS == latch_.try_wrlock(latch_id_)) {
      ATOMIC_STORE(&write_id_, itid);
      get_tcref().sync(&read_ref_);
      if (0 != (ATOMIC_LOAD(&read_ref_))) {
        ATOMIC_STORE(&write_id_, 0);
      } else {
        ATOMIC_STORE(&write_id_, itid | WRITE_MASK);
        locked = true;
      }
      if (!locked) {
        latch_.unlock();
      }
    }
    return locked;
  }
  int unlock() {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY((ATOMIC_LOAD(&write_id_) & WRITE_MASK) != 0)) {
      ret = wrunlock();
    } else {
      ret = rdunlock();
    }
    return ret;
  }
  inline bool try_rdlock(int64_t &slot_id)
  {
    bool locked = false;
    if (OB_LIKELY(0 == ATOMIC_LOAD(&write_id_))) {
      get_tcref().inc_ref(&read_ref_, slot_id);
      if (OB_LIKELY(0 == ATOMIC_LOAD(&write_id_))) {
        locked = true;
        IGNORE_RETURN ObLatch::reg_lock((uint32_t*)&(latch_.lock_));
      } else {
        get_tcref().dec_ref(&read_ref_, slot_id);
        lcond_.signal();
      }
    }
    return locked;
  }
  int rdlock(const int64_t abs_timeout_us, int64_t &slot_id)
  {
    int ret = OB_SUCCESS;
    while(OB_SUCC(ret) && !try_rdlock(slot_id)) {
      if (OB_SUCC(latch_.rdlock(latch_id_, abs_timeout_us))) {
        ret = latch_.unlock();
      }
    }
    if (OB_SUCC(ret)) {
      // record in try_rdlock will be overwrited by latch_.rdlock, so record again.
      IGNORE_RETURN ObLatch::reg_lock((uint32_t*)&(latch_.lock_));
    }
    return ret;
  }
  inline int rdunlock(int64_t slot_id)
  {
    int ret = OB_SUCCESS;
    get_tcref().dec_ref(&read_ref_, slot_id);
    IGNORE_RETURN ObLatch::unreg_lock((uint32_t*)&(latch_.lock_));
    lcond_.signal();
    return ret;
  }
  TCRef& get_tcref() { return tcref_; }
private:
  ObLatch latch_;
  uint32_t latch_id_;
  TCRef &tcref_;
  LWaitCond lcond_;
  int32_t write_id_;
  int32_t read_ref_;
};

class TCWLockGuard
{
public:
  [[nodiscard]] explicit TCWLockGuard(const TCRWLock &lock)
      : lock_(const_cast<TCRWLock&>(lock)), ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to write lock, ", K_(ret));
    }
  }
  ~TCWLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrunlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock, ", K_(ret));
      }
    }
  }
  inline int get_ret() const { return ret_; }
private:
  TCRWLock &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(TCWLockGuard);
};

class TCRLockGuard
{
public:
  [[nodiscard]] explicit TCRLockGuard(const TCRWLock &lock)
      : lock_(const_cast<TCRWLock&>(lock)), ret_(OB_SUCCESS), slot_id_(0)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock(INT64_MAX, slot_id_)))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to read lock, ", K_(ret));
    }
  }
  ~TCRLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdunlock(slot_id_)))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to unlock, ", K_(ret));
      }
    }
  }
  inline int get_ret() const { return ret_; }
private:
  TCRWLock &lock_;
  int ret_;
  int64_t slot_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(TCRLockGuard);
};

typedef TCRWLock RWLock;
#else
class RWLock
{
public:
  [[nodiscard]]  struct RLockGuard
  {
    explicit RLockGuard(RWLock& lock): lock_(lock) { lock_.rdlock(); }
    ~RLockGuard() { lock_.rdunlock(); }
    RWLock& lock_;
  };
  struct RLockGuardWithTimeout
  {
    [[nodiscard]]  explicit RLockGuardWithTimeout(RWLock &lock, const int64_t abs_timeout_us, int &ret): lock_(lock), need_unlock_(true)
    {
      if (OB_FAIL(lock_.rdlock(abs_timeout_us))) {
        need_unlock_ = false;
        COMMON_LOG(WARN, "RLockGuardWithTimeout failed", K(ret), K(abs_timeout_us));
      } else {
        need_unlock_ = true;
      }
    }
    ~RLockGuardWithTimeout()
    {
      if (need_unlock_) {
        lock_.rdunlock();
      }
    }
    RWLock &lock_;
    bool need_unlock_;
  };

  struct WLockGuard
  {
    [[nodiscard]] explicit WLockGuard(RWLock& lock): lock_(lock) { lock_.wrlock(); }
    ~WLockGuard() { lock_.wrunlock(); }
    RWLock& lock_;
  };
  struct WLockGuardWithTimeout
  {
    [[nodiscard]] explicit WLockGuardWithTimeout(RWLock &lock, const int64_t abs_timeout_us, int &ret): lock_(lock), need_unlock_(true)
    {
      if (OB_FAIL(lock_.wrlock(abs_timeout_us))) {
        need_unlock_ = false;
        COMMON_LOG(WARN, "WLockGuardWithTimeout failed", K(ret), K(abs_timeout_us));
      } else {
        need_unlock_ = true;
      }
    }
    ~WLockGuardWithTimeout()
    {
      if (need_unlock_) {
        lock_.wrunlock();
      }
    }
    RWLock &lock_;
    bool need_unlock_;
  };
  struct WLockGuardWithRetry
  {
    [[nodiscard]] explicit WLockGuardWithRetry(RWLock &lock, const int64_t abs_timeout_us) : lock_(lock)
    {
      lock_.wrlock_with_retry(abs_timeout_us);
    }
    ~WLockGuardWithRetry() { lock_.wrunlock(); }
    RWLock &lock_;
  };

  explicit RWLock(const uint32_t latch_id): latch_(), latch_id_(latch_id) {}
  ~RWLock() {}
  void set_latch_id(const uint32_t latch_id) { latch_id_ = latch_id; }
  int rdlock(const int64_t abs_timeout_us = INT64_MAX)
  {
    return latch_.rdlock(latch_id_, abs_timeout_us);
  }
  inline bool try_rdlock()
  {
    return latch_.try_rdlock(latch_id_) == OB_SUCCESS;
  }
  inline int rdunlock()
  {
    return latch_.unlock();
  }
  inline int wrlock(const int64_t abs_timeout_us = INT64_MAX)
  {
    return latch_.wrlock(latch_id_, abs_timeout_us);
  }
  inline int wrlock_with_retry(const int64_t abs_timeout_us)
  {
    int ret = OB_SUCCESS;
    bool locked = false;
    const int64_t WRLOCK_FAILED_SLEEP_US = 1 * 1000; // 1ms
    while (!locked) {
      if (OB_FAIL(wrlock(abs_timeout_us))) {
        COMMON_LOG(WARN, "wrlock_with_retry failed", K(ret), K(abs_timeout_us));
        ::usleep(WRLOCK_FAILED_SLEEP_US);
      } else {
        locked = true;
      }
    }
    return ret;
  }
  inline int wrunlock()
  {
    return latch_.unlock();
  }
  inline bool try_wrlock()
  {
    return OB_LIKELY(latch_.try_wrlock(latch_id_) == OB_SUCCESS);
  }
private:
  ObLatch latch_;
  uint32_t latch_id_;
};
#endif
}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_LOCK_TC_RWLOCK_H_ */
