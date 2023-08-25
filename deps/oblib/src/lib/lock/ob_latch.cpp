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

#include "lib/lock/ob_latch.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/worker.h"


namespace oceanbase
{
namespace common
{
bool USE_CO_LATCH = false;
thread_local uint32_t* ObLatch::current_locks[16];
thread_local uint32_t* ObLatch::current_wait = nullptr;
thread_local uint8_t ObLatch::max_lock_slot_idx = 0;

class ObLatchWaitEventGuard : public ObWaitEventGuard
{
public:
  explicit ObLatchWaitEventGuard(
    const int64_t event_no,
    const uint64_t timeout_ms = 0,
    const int64_t p1 = 0,
    uint32_t* p2_addr = 0,
    const int64_t p3 = 0,
    const bool is_atomic = false
  ) : ObWaitEventGuard(event_no, timeout_ms, p1, OB_ISNULL(p2_addr) ? 0 : *p2_addr, p3, is_atomic)
  {
    ObLatch::current_wait = p2_addr;
  }
  ~ObLatchWaitEventGuard() { ObLatch::current_wait = nullptr; }
};
/**
 * -------------------------------------------------------ObLatchMutex---------------------------------------------------------------
 */
ObLatchMutex::ObLatchMutex()
  : lock_()
    ,record_stat_(true)
{
}

ObLatchMutex::~ObLatchMutex()
{
  if (0 != lock_.val()) {
    COMMON_LOG(DEBUG, "invalid lock,", K(lock_.val()), KCSTRING(lbt()));
  }
}

int ObLatchMutex::try_lock(
    const uint32_t latch_id,
    const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(ret));
  } else {
    if (!ATOMIC_BCAS(&lock_.val(), 0, (WRITE_MASK | uid))) {
      ret = OB_EAGAIN;
    } else {
      IGNORE_RETURN ObLatch::reg_lock((uint32_t*)&lock_.val());
    }
    if (need_record_stat()) {
      TRY_LOCK_RECORD_STAT(latch_id, 1, ret);
    }
  }
  HOLD_LOCK_INC();
  return ret;
}

int ObLatchMutex::lock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  uint64_t i = 0;
  uint64_t spin_cnt = 0;
  bool waited = false;
  uint64_t yield_cnt = 0;
  const uint32_t uid = static_cast<uint32_t>(GETTID());

  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
        || OB_UNLIKELY(0 == uid)
        || OB_UNLIKELY(uid >= WRITE_MASK)
        || OB_UNLIKELY(abs_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(abs_timeout_us), K(ret), KCSTRING(lbt()));
  } else {
    while (OB_SUCC(ret)) {
      //spin
      i = low_try_lock(OB_LATCHES[latch_id].max_spin_cnt_, (WRITE_MASK | uid));
      spin_cnt += i;

      if (OB_LIKELY(i < OB_LATCHES[latch_id].max_spin_cnt_)) {
        //success lock
        ++spin_cnt;
        break;
      } else if (yield_cnt < OB_LATCHES[latch_id].max_yield_cnt_) {
        sched_yield();
        ++yield_cnt;
        continue;
      } else {
        //wait
        waited = true;
        // latch mutex wait is an atomic wait event
        ObLatchWaitEventGuard wait_guard(
            ObLatchDesc::wait_event_idx(latch_id),
            abs_timeout_us / 1000,
            reinterpret_cast<uint64_t>(this),
            (uint32_t*)&lock_.val(),
            0,
            true /*is_atomic*/);
        if (OB_FAIL(wait(abs_timeout_us, uid))) {
          if (OB_TIMEOUT != ret) {
            COMMON_LOG(WARN, "Fail to wait the latch, ", K(ret));
          }
        } else {
          break;
        }
      }
    }
    if (need_record_stat()) {
      LOCK_RECORD_STAT(latch_id, waited, spin_cnt, yield_cnt);
    }
  }
  HOLD_LOCK_INC();
  return ret;
}

int ObLatchMutex::wait(const int64_t abs_timeout_us, const uint32_t uid)
{
  // performance critical, do not double check the parameters
  int ret = OB_SUCCESS;
  ObDiagnoseSessionInfo *dsi = ObDiagnoseSessionInfo::get_local_diagnose_info();
  int64_t timeout = 0;
  int lock = 0;

  while (OB_SUCC(ret)) {
    timeout = abs_timeout_us - ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      COMMON_LOG(DEBUG, "wait latch mutex timeout, ", K(abs_timeout_us), KCSTRING(lbt()), K(get_wid()), K(ret));
    } else {
      if (NULL != dsi) {
        ++dsi->get_curr_wait().p3_;
      }
      lock = lock_.val();
      if (WAIT_MASK == (lock & WAIT_MASK)
          || 0 != (lock = ATOMIC_CAS(&lock_.val(), (lock | WRITE_MASK), (lock | WAIT_MASK)))) {
        ret = lock_.wait((lock | WAIT_MASK), timeout);
      }
      if (OB_LIKELY(OB_TIMEOUT != ret)) {
        // spin try lock
        if (MAX_SPIN_CNT_AFTER_WAIT >
            low_try_lock(MAX_SPIN_CNT_AFTER_WAIT, (WAIT_MASK | WRITE_MASK | uid))) {
          ret = OB_SUCCESS;
          break;
        }
      } else  {
        COMMON_LOG(DEBUG, "wait latch mutex timeout",
            K(abs_timeout_us), KCSTRING(lbt()), K(get_wid()), K(ret));
      }
    }
  }
  return ret;
}

int ObLatchMutex::unlock()
{
  int ret = OB_SUCCESS;
  uint32_t lock = ATOMIC_SET(&lock_.val(), 0);
  IGNORE_RETURN ObLatch::unreg_lock((uint32_t*)&lock_.val());
  if (OB_UNLIKELY(0 == lock)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid lock,", K(lock), K(ret));
  } else if (OB_UNLIKELY(0 != (lock & WAIT_MASK))) {
    lock_.wake(1);
  }
  HOLD_LOCK_DEC();
  return ret;
}

int64_t ObLatchMutex::to_string(char *buf, const int64_t buf_len)
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, "lock_", lock_.val());
  return pos;
}

/**
 * -------------------------------------------------------ObLatchWaitQueue---------------------------------------------------------------
 */
ObLatchWaitQueue::ObLatchWaitQueue()
  : wait_map_()
{
}

ObLatchWaitQueue::~ObLatchWaitQueue()
{
}

ObLatchWaitQueue &ObLatchWaitQueue::get_instance()
{
  static ObLatchWaitQueue instance_;
  return instance_;
}

template<typename LowTryLock>
int ObLatchWaitQueue::wait(
    ObWaitProc &proc,
    const uint32_t latch_id,
    const uint32_t uid,
    LowTryLock &lock_func,
    LowTryLock &lock_func_ignore,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  // performance critical, do not double check the parameters
  if (OB_UNLIKELY(!proc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(proc), K(uid), K(abs_timeout_us), K(ret));
  } else {
    ObLatch &latch = *proc.addr_;
    uint64_t pos = reinterpret_cast<uint64_t>((&latch)) % LATCH_MAP_BUCKET_CNT;
    ObLatchBucket &bucket = wait_map_[pos];
    int64_t timeout = 0;
    bool conflict = false;
    struct timespec ts;
    ObDiagnoseSessionInfo *dsi = ObDiagnoseSessionInfo::get_local_diagnose_info();

    //check if need wait
    if (OB_FAIL(try_lock(bucket, proc, latch_id, uid, lock_func))) {
      if (OB_EAGAIN != ret) {
        COMMON_LOG(ERROR, "Fail to try lock, ", K(ret));
      }
    }

    //wait signal or timeout
    while (OB_EAGAIN == ret) {
      timeout = abs_timeout_us - ObTimeUtility::current_time();
      if (OB_UNLIKELY(timeout <= 0)) {
        ret = OB_TIMEOUT;
        COMMON_LOG(DEBUG, "Wait latch timeout, ", K(abs_timeout_us), KCSTRING(lbt()), K(latch.get_wid()), K(ret));
      } else {
        if (NULL != dsi) {
          ++dsi->get_curr_wait().p3_;
        }

        {
          ObLatchWaitEventGuard wait_guard(
              ObWaitEventIds::LATCH_WAIT_QUEUE_LOCK_WAIT,
              abs_timeout_us / 1000,
              reinterpret_cast<uint64_t>(this),
              (uint32_t*)&latch.lock_,
              0,
              true /*is_atomic*/);
          ts.tv_sec = timeout / 1000000;
          ts.tv_nsec = 1000 * (timeout % 1000000);
          // futex_wait is an atomic wait event
          if (ETIMEDOUT == (tmp_ret = futex_wait(&proc.wait_, 1, &ts))) {
            tmp_ret = OB_TIMEOUT;
          }
        }

        if (OB_TIMEOUT != tmp_ret) {
          //try lock
          conflict = false;
          while(!conflict) {
            if (OB_SUCC(lock_func_ignore(&latch.lock_, latch.lock_, uid, conflict))) {
              break;
            } else if (OB_EAGAIN != ret) {
              COMMON_LOG(WARN, "Fail to lock through lock func ignore, ", K(ret), K(tmp_ret));
              break;
            }
            PAUSE();
          }

          if (OB_EAGAIN == ret) {
            if (OB_FAIL(try_lock(bucket, proc, latch_id, uid, lock_func_ignore))) {
              if (OB_EAGAIN != ret) {
                COMMON_LOG(ERROR, "Fail to try lock, ", K(ret), K(tmp_ret), K(proc));
              }
            }
          }
        } else {
          ret = OB_TIMEOUT;
          COMMON_LOG(DEBUG, "Wait latch timeout, ", K(abs_timeout_us), K(timeout), KCSTRING(lbt()),
            K(latch.get_wid()), K(ret));
        }
      }
    }

    //remove proc from wait list if it is necessary, in the common case, we do not need remove
    if (OB_UNLIKELY(1 == proc.wait_)) {
      lock_bucket(bucket);
      if (1 == proc.wait_) {
        if (OB_ISNULL(bucket.wait_list_.remove(&proc))) {
          //should not happen
          tmp_ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to remove proc, ", K(tmp_ret));
        } else {
          //if there is not any wait proc, clear the wait mask
          bool has_wait = false;
          for (ObWaitProc *iter = bucket.wait_list_.get_first();
              iter != bucket.wait_list_.get_header();
              iter = iter->get_next()) {
            if (iter->addr_ == proc.addr_) {
              has_wait = true;
              break;
            }
          }

          if (!has_wait) {
            ATOMIC_ANDF(&latch.lock_, ~latch.WAIT_MASK);
          }
        }
      }
      unlock_bucket(bucket);
    }
  }

  return ret;
}

int ObLatchWaitQueue::wake_up(ObLatch &latch, const bool only_rd_wait)
{
  int ret = OB_SUCCESS;
  uint64_t pos = reinterpret_cast<uint64_t>((&latch)) % LATCH_MAP_BUCKET_CNT;
  ObLatchBucket &bucket = wait_map_[pos];
  ObWaitProc *iter = NULL;
  ObWaitProc *tmp = NULL;
  int32_t wait_mode = ObLatchWaitMode::NOWAIT;
  volatile int32_t *pwait = NULL;
  uint32_t wake_cnt = 0;
  uint32_t actual_wake_cnt = 0;
  ObDList<ObWaitProc> wake_list;
  bool has_wait = true;

  lock_bucket(bucket);
  // wake_cnt is used to count the waiters need to be waked by
  // futex_wake one time, but could not used to denote the
  // actual waked count. Here, we need to wake up at least one
  // waiter (if there are waiters timeout and out of queue without
  // doing unlock), so we add ctual_wake_cnt, which is used to
  // indicate the count of actual waked waiter by futex_wake.
  // This loop won't end, until at least one waiter is waked up
  // or wait queue of this latch is empty or only_rd_wait is
  // true (i.e. only wake the continuous rdlock waiters ahead of
  // the wait queue).
  do {
    wake_cnt = 0;
    // get all wait proc which need wake up
    for (iter = bucket.wait_list_.get_first(); OB_SUCC(ret) && iter != bucket.wait_list_.get_header(); ) {
      if (iter->addr_ == &latch) {
        wait_mode = iter->mode_;
        if (ObLatchWaitMode::READ_WAIT == wait_mode
            || (ObLatchWaitMode::WRITE_WAIT == wait_mode && 0 == wake_cnt && !only_rd_wait)) {
          tmp = iter->get_next();
          if (OB_ISNULL(bucket.wait_list_.remove(iter))) {
            //should not happen
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(ERROR, "Fail to remove iter from wait list, ", K(ret));
          } else if (OB_UNLIKELY(false == wake_list.add_last(iter))) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(ERROR, "add to wake list failed", K(ret));
          } else {
            ++wake_cnt;
          }
          iter = tmp;
        }

        if (ObLatchWaitMode::WRITE_WAIT == wait_mode && (wake_cnt > 0 || only_rd_wait)) {
          break;
        }
      } else {
        //other latch in the same bucket, just ignore
        iter = iter->get_next();
      }
    }

    //check if there is other waiting in the waiting list
    has_wait = false;
    for (; iter != bucket.wait_list_.get_header(); iter = iter->get_next()) {
      if (iter->addr_ == &latch) {
        has_wait = true;
        break;
      }
    }
    if (!has_wait) {
      //no wait, clear the wait mask
      ATOMIC_ANDF(&latch.lock_, ~latch.WAIT_MASK);
    }

    // do futex wake
    for (iter = wake_list.get_first(); iter != wake_list.get_header();) {
      tmp = iter->get_next();
      if (OB_ISNULL(wake_list.remove(iter))) {
        //should not happen
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "Fail to remove iter from wait list, ", K(ret));
      } else {
        pwait = &iter->wait_;
        //the proc.wait_ must be set to 0 at last, once the 0 is set, the *iter may be not valid any more
        MEM_BARRIER();
        *pwait = 0;
        // a thread waits using sys futex
        if (1 == futex_wake(pwait, 1)) {
          ++actual_wake_cnt;
        }
      }
      iter = tmp;
    }
  } while (actual_wake_cnt == 0 && !only_rd_wait && has_wait);

  unlock_bucket(bucket);

  return ret;
}

template<typename LowTryLock>
int ObLatchWaitQueue::try_lock(
    ObLatchBucket &bucket,
    ObWaitProc &proc,
    const uint32_t latch_id,
    const uint32_t uid,
    LowTryLock &lock_func)
{
  // performance critical, do not double check the parameters
  int ret = OB_SUCCESS;
  uint32_t lock = 0;
  bool conflict = false;
  ObLatch &latch = *(proc.addr_);

  lock_bucket(bucket);
  while (true) {
    lock = latch.lock_;
    if (OB_SUCC(lock_func(&latch.lock_, lock, uid, conflict))) {
      break;
    } else if (OB_EAGAIN == ret) {
      if (conflict) {
        if (ATOMIC_BCAS(&latch.lock_, lock, lock | latch.WAIT_MASK)) {
          break;
        }
      }
    } else {
      COMMON_LOG(WARN, "Fail to lock through lock func, ", K(ret));
      break;
    }
    PAUSE();
  }

  if (OB_EAGAIN == ret) {
    //fail to lock, add the proc to wait list
    if (ObLatchPolicy::LATCH_READ_PREFER == OB_LATCHES[latch_id].policy_
        && ObLatchWaitMode::READ_WAIT == proc.mode_) {
      if (NULL == proc.get_prev()
          && NULL == proc.get_next()
          && !bucket.wait_list_.add_first(&proc)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
      }
    } else {
      if (NULL == proc.get_prev()
          && NULL == proc.get_next()
          && !bucket.wait_list_.add_last(&proc)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
      }
    }
    proc.wait_ = 1;
  }

  unlock_bucket(bucket);

  return ret;
}

/**
 * -------------------------------------------------------ObLatch---------------------------------------------------------------
 */
#ifdef ENABLE_LATCH_DIAGNOSE
ObLDHandleNode::ObLDHandleNode()
  : slot_(nullptr)
{}

void ObLDHandle::reset()
{
  if (node_ != nullptr) {
    abort_unless(node_->slot_ != nullptr);
    node_->slot_->remove(node_);
    delete node_;
    node_ = nullptr;
  }
}

void ObLDSlot::add(ObLDHandleNode *node)
{
  abort_unless(nullptr == node->slot_);
  node->slot_ = this;
  mutex_.lock();
  node_list_.add_first(node);
  mutex_.unlock();
}

void ObLDSlot::remove(ObLDHandleNode *node)
{
  mutex_.lock();
  node_list_.remove(node);
  mutex_.unlock();
  node->slot_ = nullptr;
}

ObLockDiagnose::~ObLockDiagnose()
{
  for (int i = 0; i< ARRAYSIZEOF(slots_); i++) {
    auto &mutex = slots_[i].mutex_;
    mutex.lock();
    DLIST_FOREACH_NORET(node, slots_[i].node_list_) {
      delete node;
    }
    slots_[i].node_list_.clear();
    mutex.unlock();
  }
}

void ObLockDiagnose::lock(const ObLDLockType::Type type, ObLDHandle &handle)
{
  abort_unless(nullptr == handle.node_);
  auto *node = new ObLDHandleNode();
  abort_unless(node != nullptr);
  node->tid_ = GETTID();
  node->type_ = type;
  lbt(node->lbt_, sizeof(node->lbt_));
  int idx = GETTID() % ARRAYSIZEOF(slots_);
  slots_[idx].add(node);
  handle.node_ = node;
}

void ObLockDiagnose::print()
{
  for (int i = 0; i< ARRAYSIZEOF(slots_); i++) {
    auto &mutex = slots_[i].mutex_;
    mutex.lock();
    DLIST_FOREACH_NORET(node, slots_[i].node_list_) {
      COMMON_LOG(INFO, "dump operation",
                 "ptr", node,
                 "tid", node->tid_,
                 "lock_type", node->type_,
                 "lbt", node->lbt_);
    }
    mutex.unlock();
  }
}
#endif

ObLatch::ObLatch()
  : lock_(0)
    , record_stat_(true)
{
}

ObLatch::~ObLatch()
{
  if (0 != lock_) {
    COMMON_LOG(DEBUG, "invalid lock,", K(lock_), KCSTRING(lbt()));
  }
}

int ObLatch::try_rdlock(const uint32_t latch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(ret));
  } else {
    ret = OB_EAGAIN;
    uint64_t i = 0;
    uint32_t lock = 0;
    do {
      lock = lock_;
      if (0 == (lock & WRITE_MASK)) {
        if ((lock & (~WAIT_MASK)) >= MAX_READ_LOCK_CNT) {
          ret = OB_SIZE_OVERFLOW;
          COMMON_LOG(ERROR, "Too many read locks, ", K(lock), K(ret));
          break;
        } else {
          if (ObLatchPolicy::LATCH_FIFO == OB_LATCHES[latch_id].policy_) {
        	if (0 != (lock & WAIT_MASK)) {
        	  ret = OB_EAGAIN;
        	  break;
        	}
          }
          ++i;
          if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
            ret = OB_SUCCESS;
            IGNORE_RETURN reg_lock((uint32_t*)&lock_);
            break;
          }
        }
      } else {
        break;
      }
      PAUSE();
    } while (true);
    if (need_record_stat()) {
      TRY_LOCK_RECORD_STAT(latch_id, i, ret);
    }
  }
  HOLD_LOCK_INC();
  return ret;
}

int ObLatch::try_wrlock(const uint32_t latch_id, const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(ret), KCSTRING(lbt()));
  } else {
    if (!ATOMIC_BCAS(&lock_, 0, (WRITE_MASK | uid))) {
      ret = OB_EAGAIN;
    } else {
      IGNORE_RETURN reg_lock((uint32_t*)&lock_);
    }
    if (need_record_stat()) {
      TRY_LOCK_RECORD_STAT(latch_id, 1, ret);
    }
  }
  HOLD_LOCK_INC();
  return ret;
}

int ObLatch::rdlock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  //the uid is unused concurrently
  const uint32_t uid = 1;
  static LowTryRDLock low_try_rdlock(false);
  static LowTryRDLock low_try_rdlock_ignore(true);
  if (OB_FAIL(low_lock(
      latch_id,
      abs_timeout_us,
      uid,
      ObLatchWaitMode::READ_WAIT,
      ObLatchPolicy::LATCH_FIFO == OB_LATCHES[latch_id].policy_ ? low_try_rdlock : low_try_rdlock_ignore,
      low_try_rdlock_ignore))) {
    if (OB_TIMEOUT != ret) {
      COMMON_LOG(WARN, "Fail to low lock, ", K(ret));
    }
  }
  HOLD_LOCK_INC();
  return ret;
}


int ObLatch::wrlock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us,
    const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  const uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  LowTryWRLock low_try_wrlock(false);
  LowTryWRLock low_try_wrlock_ignore(true);
  if (OB_FAIL(low_lock(
      latch_id,
      abs_timeout_us,
      uid,
      ObLatchWaitMode::WRITE_WAIT,
      low_try_wrlock,
      low_try_wrlock_ignore))) {
    if (OB_TIMEOUT != ret) {
      COMMON_LOG(WARN, "Fail to low lock, ", K(ret));
    }
  }
  HOLD_LOCK_INC();
  return ret;
}

int ObLatch::wr2rdlock(const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  if (!is_wrlocked_by(puid)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "The latch is not write locked, ", K(ret));
  } else {
    uint32_t lock = lock_;
    while (!ATOMIC_BCAS(&lock_, lock, (lock & WAIT_MASK) + 1)) {
      lock = lock_;
      PAUSE();
    }
    bool only_rd_wait = true;
    if (OB_FAIL(ObLatchWaitQueue::get_instance().wake_up(*this, only_rd_wait))) {
      COMMON_LOG(ERROR, "Fail to wake up latch wait queue, ", K(this), K(ret));
    }
  }
  return ret;
}

int ObLatch::unlock(const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t lock = ATOMIC_LOAD(&lock_);

  if (0 != (lock & WRITE_MASK)) {
    uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
    uint32_t wid = (lock & ~(WAIT_MASK | WRITE_MASK));
    if (NULL != puid && uid != wid) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "The latch is not write locked by the uid, ", K(uid), K(wid), KCSTRING(lbt()), K(ret));
    } else {
      lock = ATOMIC_ANDF(&lock_, WAIT_MASK);
      IGNORE_RETURN unreg_lock((uint32_t*)&lock_);
    }
  } else if ((lock & (~WAIT_MASK)) > 0) {
    lock = ATOMIC_AAF(&lock_, -1);
    IGNORE_RETURN unreg_lock((uint32_t*)&lock_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid lock,", K(lock), K(ret));
  }
  if (OB_SUCCESS == ret && WAIT_MASK == lock) {
    if (OB_FAIL(ObLatchWaitQueue::get_instance().wake_up(*this))) {
      COMMON_LOG(ERROR, "Fail to wake up latch wait queue, ", K(this), K(ret));
    }
  }
  HOLD_LOCK_DEC();
  return ret;
}

template<typename LowTryLock>
OB_INLINE int ObLatch::low_lock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us,
    const uint32_t uid,
    const uint32_t wait_mode,
    LowTryLock &lock_func,
    LowTryLock &lock_func_ignore)
{
  int ret = OB_SUCCESS;
  uint64_t i = 0;
  uint32_t lock = 0;
  uint64_t spin_cnt = 0;
  uint64_t yield_cnt = 0;
  bool waited = false;
  bool conflict = false;

  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(abs_timeout_us <= 0)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)
      || OB_UNLIKELY(ObLatchWaitMode::READ_WAIT != wait_mode
          && ObLatchWaitMode::WRITE_WAIT != wait_mode)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      //spin
      for (i = 0; OB_SUCC(ret) && i < OB_LATCHES[latch_id].max_spin_cnt_; ++i) {
        lock = lock_;
        if (OB_SUCC(lock_func(&lock_, lock, uid, conflict))) {
          break;
        } else if (OB_EAGAIN == ret) {
          //retry
          ret = OB_SUCCESS;
        }
        PAUSE();
      }
      spin_cnt += i;

      if (OB_FAIL(ret)) {
        //fail
      } else if (i < OB_LATCHES[latch_id].max_spin_cnt_) {
        //success lock
        ++spin_cnt;
        break;
      } else if (yield_cnt < OB_LATCHES[latch_id].max_yield_cnt_) {
        //yield and retry
        sched_yield();
        ++yield_cnt;
        continue;
      } else {
        //wait
        waited = true;
        ObLatchWaitEventGuard wait_guard(
          ObLatchDesc::wait_event_idx(latch_id),
          abs_timeout_us / 1000,
          reinterpret_cast<uint64_t>(this),
          (uint32_t*)&lock_,
          0);
        ObWaitProc proc(*this, wait_mode);
        if (OB_FAIL(ObLatchWaitQueue::get_instance().wait(
            proc,
            latch_id,
            uid,
            lock_func,
            lock_func_ignore,
            abs_timeout_us))) {
          if (OB_TIMEOUT != ret) {
            COMMON_LOG(WARN, "Fail to wait the latch, ", K(ret));
          }
        } else {
          break;
        }
      }
    }
    if (need_record_stat()) {
      LOCK_RECORD_STAT(latch_id, waited, spin_cnt, yield_cnt);
    }
  }
  return ret;
}

int64_t ObWaitProc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, KP_(addr), K_(mode));
  return pos;
}

int64_t ObLatch::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, "lock_", static_cast<uint32_t>(lock_));
  return pos;
}

ObLDLatch::ObLDLatch()
  : diagnose_(false)
{}

int ObLDLatch::try_rdlock(ObLDHandle &handle, const uint32_t latch_id)
{
  int ret = OB_SUCCESS;
  ret = ObLatch::try_rdlock(latch_id);
#ifdef ENABLE_LATCH_DIAGNOSE
  if (OB_SUCC(ret) && OB_UNLIKELY(diagnose_)) {
    ld_.lock(ObLDLockType::rdlock, handle);
  }
#else
  UNUSED(handle);
#endif
  return ret;
}

int ObLDLatch::try_wrlock(ObLDHandle &handle, const uint32_t latch_id,
                          const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  ret = ObLatch::try_wrlock(latch_id, puid);
#ifdef ENABLE_LATCH_DIAGNOSE
  if (OB_SUCC(ret) && OB_UNLIKELY(diagnose_)) {
    ld_.lock(ObLDLockType::wrlock, handle);
  }
#else
  UNUSED(handle);
#endif
  return ret;
}

int ObLDLatch::rdlock(
    ObLDHandle &handle,
    const uint32_t latch_id,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ret = ObLatch::rdlock(latch_id, abs_timeout_us);
#ifdef ENABLE_LATCH_DIAGNOSE
  if (OB_SUCC(ret) && OB_UNLIKELY(diagnose_)) {
    ld_.lock(ObLDLockType::rdlock, handle);
  }
#else
  UNUSED(handle);
#endif
  return ret;
}

int ObLDLatch::wrlock(
    ObLDHandle &handle,
    const uint32_t latch_id,
    const int64_t abs_timeout_us,
    const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  ret = ObLatch::wrlock(latch_id, abs_timeout_us, puid);
#ifdef ENABLE_LATCH_DIAGNOSE
  if (OB_SUCC(ret) && OB_UNLIKELY(diagnose_)) {
    ld_.lock(ObLDLockType::wrlock, handle);
  }
#else
  UNUSED(handle);
#endif
  return ret;
}

int ObLDLatch::unlock(ObLDHandle &handle, const uint32_t *puid)
{
#ifdef ENABLE_LATCH_DIAGNOSE
  handle.reset();
#else
  UNUSED(handle);
#endif
  return ObLatch::unlock(puid);
}

}
}
