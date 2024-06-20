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

#ifndef  OCEANBASE_LOCK_LATCH_H_
#define  OCEANBASE_LOCK_LATCH_H_

#include "lib/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_futex.h"
#include "lib/stat/ob_latch_define.h"
#ifdef ENABLE_LATCH_DIAGNOSE
#include<mutex>
#include "lib/list/ob_dlist.h"
#endif

namespace oceanbase
{
namespace common
{
extern bool USE_CO_LATCH;

#define HOLD_LOCK_INC()             \
  do {                              \
  } while(0)

#define HOLD_LOCK_DEC()             \
  do {                              \
  } while(0)

#define TRY_LOCK_RECORD_STAT(latch_id, spin_cnt, ret)                             \
  do {                                                                            \
    if (record_stat_ && lib::is_diagnose_info_enabled()) {                        \
      ObDiagnoseTenantInfo *di = ObDiagnoseTenantInfo::get_local_diagnose_info(); \
      if (NULL != di) {                                                           \
        ObLatchStat *p_latch_stat = di->get_latch_stats().get_or_create_item(latch_id); \
        if (OB_ISNULL(p_latch_stat)) {                                              \
        } else {                                                                    \
          ObLatchStat &latch_stat = *p_latch_stat;                                  \
          if (OB_SUCC(ret)) {                                                       \
            ++latch_stat.immediate_gets_;                                           \
          } else {                                                                  \
            ++latch_stat.immediate_misses_;                                         \
          }                                                                         \
          latch_stat.spin_gets_ += spin_cnt;                                        \
        } \
      }                                                                           \
    }                                                                             \
  } while(0)

#define LOCK_RECORD_STAT(latch_id, waited, spin_cnt, yield_cnt)                            \
  do {                                                                                     \
    if (record_stat_ && lib::is_diagnose_info_enabled()) {                                 \
      ObDiagnoseTenantInfo *di = ObDiagnoseTenantInfo::get_local_diagnose_info();          \
      if (NULL != di) {                                                                    \
        ObLatchStat *p_latch_stat = di->get_latch_stats().get_or_create_item(latch_id);      \
        if (OB_ISNULL(p_latch_stat)) {                                                       \
        } else {                                                                             \
          ObLatchStat &latch_stat = *p_latch_stat;                                           \
          ++latch_stat.gets_;                                                                \
          latch_stat.spin_gets_ += spin_cnt;                                                 \
          latch_stat.sleeps_ += yield_cnt;                                                   \
          if (OB_UNLIKELY(waited)) {                                                         \
            ++latch_stat.misses_;                                                            \
            ObDiagnoseSessionInfo *dsi = ObDiagnoseSessionInfo::get_local_diagnose_info();   \
            if (NULL != dsi) {                                                               \
              latch_stat.wait_time_ += dsi->get_curr_wait().wait_time_;                      \
              if (dsi->get_curr_wait().wait_time_ > 1000 * 1000) {                           \
                COMMON_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "The Latch wait too much time, ", \
                    K(dsi->get_curr_wait()), KCSTRING(lbt()));                               \
              }                                                                              \
            }                                                                                \
          }                                                                                   \
        } \
      }                                                                                    \
    }                                                                                      \
  } while(0)

struct ObLatchWaitMode
{
  enum ObLatchWaitModeEnum
  {
    NOWAIT = 0,
    READ_WAIT = 1,
    WRITE_WAIT = 2
  };
};

class ObLatchMutex
{
public:
  ObLatchMutex();
  ~ObLatchMutex();
  int lock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX);
  int try_lock(
      const uint32_t latch_id,
      const uint32_t *puid = NULL);
  int wait(const int64_t abs_timeout_us, const uint32_t uid);
  int unlock();
  inline bool is_locked();
  inline uint32_t get_wid();
  int64_t to_string(char* buf, const int64_t buf_len);
  void enable_record_stat(bool enable) { record_stat_ = enable; }
  bool need_record_stat() const { return record_stat_; }

private:
  OB_INLINE uint64_t low_try_lock(const int64_t max_spin_cnt, const uint32_t lock_value);

private:
  static const int64_t MAX_SPIN_CNT_AFTER_WAIT = 1;
  static const uint32_t WRITE_MASK = 1<<30;
  static const uint32_t WAIT_MASK = 1<<31;
  lib::ObFutex lock_;
  //volatile int32_t lock_;
  bool record_stat_;
};

class ObLatch;

struct ObWaitProc : public ObDLinkBase<ObWaitProc>
{
  ObWaitProc(ObLatch &latch, const uint32_t wait_mode)
    : addr_(&latch),
      mode_(wait_mode),
      wait_(0)
  {
  }
  virtual ~ObWaitProc()
  {
  }
  bool is_valid() const
  {
    return OB_LIKELY(NULL != addr_)
              && OB_LIKELY(ObLatchWaitMode::READ_WAIT == mode_
                  || ObLatchWaitMode::WRITE_WAIT == mode_);
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  ObLatch *addr_;
  int32_t mode_;
  volatile int32_t wait_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObWaitProc);
};

class ObLatchWaitQueue
{
public:
  static ObLatchWaitQueue &get_instance();
  template<typename LowTryLock>
  int wait(
      ObWaitProc &proc,
      const uint32_t latch_id,
      const uint32_t uid,
      LowTryLock &lock_func,
      LowTryLock &lock_func_ignore,
      const int64_t abs_timeout_us);
  int wake_up(ObLatch &latch, const bool only_rd_wait = false);

private:
  struct ObLatchBucket
  {
    ObDList<ObWaitProc> wait_list_;
    ObLatchMutex lock_;
    ObLatchBucket() : wait_list_(), lock_()
    {
    }
  } CACHE_ALIGNED;

  ObLatchWaitQueue();
  virtual ~ObLatchWaitQueue();

  inline void lock_bucket(ObLatchBucket &bucket)
  {
    bucket.lock_.lock(ObLatchIds::LATCH_WAIT_QUEUE_LOCK);
  }

  inline void unlock_bucket(ObLatchBucket &bucket)
  {
    bucket.lock_.unlock();
  }

  template<typename LowTryLock>
  int try_lock(
      ObLatchBucket &bucket,
      ObWaitProc &proc,
      const uint32_t latch_id,
      const uint32_t uid,
      LowTryLock &lock_func);

private:
  static const uint64_t LATCH_MAP_BUCKET_CNT = 3079;
  ObLatchBucket wait_map_[LATCH_MAP_BUCKET_CNT];

private:
  DISALLOW_COPY_AND_ASSIGN(ObLatchWaitQueue);
};

class TCRWLock;

class ObLatch
{
  friend class TCRWLock;
public:
  ObLatch(const bool need_record_stat = true)
    : lock_(0)
      , record_stat_(need_record_stat)
  {
  }

  ~ObLatch()
  {
    if (0 != lock_) {
      COMMON_LOG(DEBUG, "invalid lock,", K(lock_), KCSTRING(lbt()));
    }
  }
  int try_rdlock(const uint32_t latch_id);
  int try_wrlock(const uint32_t latch_id, const uint32_t *puid = NULL);
  int rdlock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX);
  int wrlock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX,
      const uint32_t *puid = NULL);
  int wr2rdlock(const uint32_t *puid = NULL);
  int unlock(const uint32_t *puid = NULL);
  inline bool is_locked() const;
  inline bool is_rdlocked() const;
  inline bool is_wrlocked() const;
  inline bool is_wrlocked_by(const uint32_t *puid = NULL) const;
  inline uint32_t get_wid() const;
  inline uint32_t get_rdcnt() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  void enable_record_stat(bool enable) { record_stat_ = enable; }
  bool need_record_stat() const { return record_stat_; }
  uint32_t val() const { return lock_; }
  OB_INLINE static int reg_lock(uint32_t* latch_addr)
  {
    int ret = -1;
    ret = (max_lock_slot_idx++) % ARRAYSIZEOF(current_locks);
    current_locks[ret] = latch_addr;
    return ret;
  }
  OB_INLINE static int unreg_lock(uint32_t* latch_addr)
  {
    int ret = -1;
    UNUSED(latch_addr);
    //if (max_lock_slot_idx > 0
    //    && latch_addr == current_locks[(max_lock_slot_idx - 1) % ARRAYSIZEOF(current_locks)]) {
    //    ret = (--max_lock_slot_idx % ARRAYSIZEOF(current_locks));
    //    current_locks[ret] = nullptr;
    //}
    return ret;
  }
  OB_INLINE static void clear_lock()
  {
    max_lock_slot_idx = 0;
  }
  static thread_local uint32_t* current_locks[16];
  static thread_local uint32_t* current_wait;
  static thread_local uint8_t max_lock_slot_idx;
private:
  template<typename LowTryLock>
  OB_INLINE int low_lock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us,
      const uint32_t uid,
      const uint32_t wait_mode,
      LowTryLock &lock_func,
      LowTryLock &lock_func_ignore);

  struct LowTryRDLock
  {
    explicit LowTryRDLock(const bool ignore) : ignore_(ignore) {}
    inline int operator()(volatile uint32_t *latch, const uint32_t lock, const uint32_t uid, bool &conflict);
  private:
    const bool ignore_;
  };

  struct LowTryWRLock
  {
    explicit LowTryWRLock(const bool ignore) : ignore_(ignore) {}
    inline int operator()(volatile uint32_t *latch, const uint32_t lock, const uint32_t uid, bool &conflict);
  private:
    const bool ignore_;
  };

  friend class ObLatchWaitQueue;
  static const uint32_t WRITE_MASK = 1<<30;
  static const uint32_t WAIT_MASK = 1<<31;
  static const uint32_t MAX_READ_LOCK_CNT = 1<<24;
  volatile uint32_t lock_;
  bool record_stat_;
};

struct ObLDLockType
{
 enum Type
 {
   rdlock = 0,
   wrlock
 };
};

#ifndef ENABLE_LATCH_DIAGNOSE
class ObLDHandle
{
public:
  ObLDHandle() {}
  ~ObLDHandle() {}
  int64_t to_string(char*, const int64_t) const { return 0; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLDHandle);
};
#else
class ObLDHandleNode;
class ObLDSlot
{
public:
  void add(ObLDHandleNode *node);
  void remove(ObLDHandleNode *node);
  std::mutex mutex_;
  common::ObDList<ObLDHandleNode> node_list_;
};

class ObLDHandleNode : public common::ObDLinkBase<ObLDHandleNode>
{
public:
  ObLDHandleNode();
  ObLDSlot *slot_;
  int tid_;
  ObLDLockType::Type type_;
  char lbt_[512];
private:
  DISALLOW_COPY_AND_ASSIGN(ObLDHandleNode);
};

class ObLDHandle
{
public:
  ObLDHandle()
    : node_(nullptr) {}
  void reset();
  int64_t to_string(char*, const int64_t) const { return 0; }
  ObLDHandle &operator=(ObLDHandle &&other)
  {
    node_ = other.node_;
    return *this;
  }
  ObLDHandleNode *node_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLDHandle);
};
#endif

#ifndef ENABLE_LATCH_DIAGNOSE
class ObLockDiagnose
{
public:
  void lock(const ObLDLockType::Type, ObLDHandle &) {}
  void print() {}
};
#else
class ObLockDiagnose
{
public:
  ~ObLockDiagnose();
  void lock(const ObLDLockType::Type type, ObLDHandle &handle);
  void print();
  ObLDSlot slots_[32];
};
#endif

class ObLDLatch : public ObLatch
{
public:
  ObLDLatch();
  void set_diagnose(bool diagnose)
  {
    diagnose_ = diagnose;
  }
  int try_rdlock(ObLDHandle &handle, const uint32_t latch_id);
  int try_wrlock(ObLDHandle &handle, const uint32_t latch_id, const uint32_t *puid = NULL);
  int rdlock(
      ObLDHandle &handle,
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX);
  int wrlock(
      ObLDHandle &handle,
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX,
      const uint32_t *puid = NULL);
  int unlock(ObLDHandle &handle, const uint32_t *puid = NULL);
  bool diagnose_;
  ObLockDiagnose ld_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLDLatch);
};

class ObLatchMutexGuard
{
public:
  [[nodiscard]] ObLatchMutexGuard(ObLatchMutex &lock, const uint32_t latch_id)
      : lock_(lock), ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.lock(latch_id)))) {
      COMMON_LOG_RET(ERROR, ret_, "lock error", K(latch_id), K(ret_));
    }
  }
  ~ObLatchMutexGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG_RET(ERROR, ret_, "unlock error", K(ret_));
      }
    }
  }
  int get_ret() const { return ret_; }

private:
  ObLatchMutex &lock_;
  int ret_;
  DISALLOW_COPY_AND_ASSIGN(ObLatchMutexGuard);
};

class ObLatchRGuard
{
public:
  [[nodiscard]] ObLatchRGuard(ObLatch &lock, const uint32_t latch_id)
      : lock_(lock),
        ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock(latch_id)))) {
      COMMON_LOG_RET(ERROR, ret_, "lock error, ", K(latch_id), K(ret_));
    }
  }
  ~ObLatchRGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG_RET(ERROR, ret_, "unlock error, ", K(ret_));
      }
    }
  }
  int get_ret() const { return ret_; }

private:
  ObLatch &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLatchRGuard);
};

class ObLatchWGuard
{
public:
  [[nodiscard]] ObLatchWGuard(ObLatch &lock, const uint32_t latch_id)
      : lock_(lock),
        ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock(latch_id)))) {
      COMMON_LOG_RET(ERROR, ret_, "lock error, ", K(latch_id), K(ret_));
    }
  }
  ~ObLatchWGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG_RET(ERROR, ret_, "unlock error, ", K(ret_));
      }
    }
  }
  int get_ret() const { return ret_; }

private:
  ObLatch &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLatchWGuard);
};

/**
 * --------------------------------------------------------Inline methods---------------------------------------------------------
 */

OB_INLINE uint64_t ObLatchMutex::low_try_lock(const int64_t max_spin_cnt, const uint32_t lock_value)
{
  uint64_t spin_cnt = 0;
  for (; spin_cnt < max_spin_cnt; ++spin_cnt) {
    if (0 == lock_.val()) {
      if (ATOMIC_BCAS(&lock_.val(), 0, lock_value)) {
        IGNORE_RETURN ObLatch::reg_lock((uint32_t*)(&lock_.val()));
        break;
      }
    }
    PAUSE();
  }
  return spin_cnt;
}

inline bool ObLatchMutex::is_locked()
{
  return 0 != ATOMIC_LOAD(&lock_.val());
}

inline uint32_t ObLatchMutex::get_wid()
{
  uint32_t lock = ATOMIC_LOAD(&lock_.val());
  return (lock & ~(WAIT_MASK | WRITE_MASK));
}

inline bool ObLatch::is_locked() const
{
  return 0 != ATOMIC_LOAD(&lock_);
}

inline bool ObLatch::is_rdlocked() const
{
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return 0 == (lock & WRITE_MASK) && (lock & (~WAIT_MASK)) > 0;
}

inline bool ObLatch::is_wrlocked() const
{
  const uint32_t lock = ATOMIC_LOAD(&lock_);
  return 0 != (lock & WRITE_MASK);
}

inline bool ObLatch::is_wrlocked_by(const uint32_t *puid) const
{
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return 0 != (lock & WRITE_MASK) && uid == (lock & ~(WAIT_MASK | WRITE_MASK));
}

inline uint32_t ObLatch::get_wid() const
{
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return (0 == (lock & WRITE_MASK)) ? 0 : (lock & ~(WAIT_MASK | WRITE_MASK));
}

inline uint32_t ObLatch::get_rdcnt() const
{
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return (0 == (lock & WRITE_MASK)) ? (lock & ~(WAIT_MASK | WRITE_MASK)) : 0;
}

inline int ObLatch::LowTryRDLock::operator()(volatile uint32_t *latch,
    const uint32_t lock, const uint32_t uid, bool &conflict)
{
  UNUSED(uid);
  int ret = OB_EAGAIN;
  // argument ignore_ is used to determine whether need to
  // ignore the WAIT_MASK when spin try lock (i.e. CAS operation)
  if ((0 == (lock & WRITE_MASK)) && (ignore_ || (0 == (lock & WAIT_MASK)))) {
    if ((lock & (~WAIT_MASK)) < MAX_READ_LOCK_CNT) {
      conflict = false;
      if (ATOMIC_BCAS(latch, lock, lock + 1)) {
        ret = OB_SUCCESS;
        IGNORE_RETURN reg_lock((uint32_t*)latch);
      }
    } else {
      conflict = true;
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(ERROR, "Too many read locks, ", K(lock), K(ret));
    }
  } else {
    conflict = true;
  }
  return ret;
}

inline int ObLatch::LowTryWRLock::operator()(volatile uint32_t *latch,
    const uint32_t lock, const uint32_t uid, bool &conflict)
{
  int ret = OB_EAGAIN;
  // argument ignore_ is used to determine whether need to
  // ignore the WAIT_MASK when spin try lock (i.e. CAS operation)
  if (0 == lock || (ignore_ && (WAIT_MASK == lock))) {
    conflict = false;
    if (ATOMIC_BCAS(latch, lock, (lock | (WRITE_MASK | uid)))) {
      ret = OB_SUCCESS;
      IGNORE_RETURN reg_lock((uint32_t*)latch);
    }
  } else {
    conflict = true;
  }
  return ret;
}

}
}

#endif //OCEANBASE_COMMON_SPIN_RWLOCK_H_
