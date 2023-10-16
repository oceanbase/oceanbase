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

#ifndef OCEANBASE_LOGSERVICE_FIXED_SLIDING_WINDOW_
#define OCEANBASE_LOGSERVICE_FIXED_SLIDING_WINDOW_

#include "share/ob_define.h"                                      // OB_SUCC ...
#include "share/allocator/ob_tenant_mutil_allocator.h"            // ObILogAllocator
#include "lib/lock/ob_tc_rwlock.h"                                // RWLock
#include "lib/lock/ob_small_spin_lock.h"                          // ObByteLock
#include "common/ob_clock_generator.h"                            // ObClockGenerator

namespace oceanbase
{
namespace palf
{
class FixedSlidingWindowSlot
{
public:
  FixedSlidingWindowSlot(): ref_(0) {}
  virtual ~FixedSlidingWindowSlot() {}
public:
  virtual bool can_be_slid() = 0;
  virtual void reset() = 0;
  int64_t ref_;
  ObByteLock slot_lock_;
};

class ISlidingCallBack
{
public:
  ISlidingCallBack() {}
  virtual ~ISlidingCallBack() {}
public:
  virtual int sliding_cb(const int64_t sn, const FixedSlidingWindowSlot *data) = 0;
};

struct SlidingCond
{
  SlidingCond(ISlidingCallBack *cb) : cb_(cb), ret_(common::OB_SUCCESS) { }
  // Sliding condition.
  // NULL can't be slid, and slide() should return common::OB_EAGAIN.
  // if can_be_slid() returns false, slide() should return common::OB_EAGAIN.
  bool operator()(const int64_t sn, FixedSlidingWindowSlot *data)
  {
    bool ret = false;
    ret_ = common::OB_SUCCESS;
    if (!data->can_be_slid()) {
      // can_be_slid() returns false, return false
      ret = false;
    } else if (common::OB_SUCCESS != (ret_ = cb_->sliding_cb(sn, data))) {
      ret = false;
    } else {
      // can slide, err code is common::OB_SUCCESS.
      ret = true;
    }
    return ret;
  }
  ISlidingCallBack *cb_;
  int ret_;
};

// static bool PALF_FIXED_SW_GET_HUNG = false;
template <typename T = FixedSlidingWindowSlot>
class FixedSlidingWindow
{
public:
  FixedSlidingWindow():
    is_inited_(false),
    begin_sn_(common::OB_INVALID_LOG_ID),
    end_sn_(common::OB_INVALID_LOG_ID),
    size_(common::OB_INVALID_SIZE),
    last_slide_fail_warn_ts_us_(common::OB_INVALID_TIMESTAMP),
    array_(NULL),
    alloc_mgr_(NULL) {}

  virtual ~FixedSlidingWindow()
  {
    destroy();
  }
  // init a FixedSlidingWindow
  // desc: init a fixed length sliding window
  // @param[in] start_id: start log id
  // @param[in] size: length of sliding window, must be 2^n(n>0)
  // @return
  // - common::OB_SUCCESS
  // - common::OB_INIT_TWICE: sliding window has been inited
  // - common::OB_INVALID_ARGUMENT
  // - common::OB_ALLOCATE_MEMORY_FAILED
  int init(const int64_t start_id, const int64_t size, common::ObILogAllocator *alloc_mgr)
  {
    int ret = common::OB_SUCCESS;
    if (is_inited_) {
      ret = common::OB_INIT_TWICE;
    } else if (0 > start_id || 1 >= size || 0 != (size&(size-1))  || NULL == alloc_mgr) {
      ret = common::OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", KR(ret), K(start_id), K(size), K(alloc_mgr));
    } else if (NULL == (array_ = static_cast<T*>
              (alloc_mgr->ge_alloc(sizeof(T) * size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "alloc FixedSlidingWindow failed", KR(ret));
    } else {
      for (int64_t i = 0; i < size; ++i) {
        new(array_+i) T;
        array_[i].reset();
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      begin_sn_ = start_id;
      end_sn_ = start_id + size;
      size_ = size;
      alloc_mgr_ = alloc_mgr;
      PALF_LOG(INFO, "FixedSlidingWindow init success", KR(ret), K(begin_sn_), K(end_sn_), K(size_), K(alloc_mgr));
    }
    return ret;
  }

  // desc: reset all LogTask in sliding window if ref count == 0 and free memory
  // not thread safety
  void destroy()
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      PALF_LOG(WARN, "FixedSlidingWindow not init", K(is_inited_));
    } else if (OB_FAIL(clear_())) {
      PALF_LOG(WARN, "fail to destroy FixedSlidingWindow");
    } else {
      is_inited_ = false;
      alloc_mgr_->ge_free(array_);
      array_ = NULL;
      PALF_LOG(INFO, "FixedSlidingWindow destroy success", K(begin_sn_), K(end_sn_));
      begin_sn_ = common::OB_INVALID_LOG_ID;
      end_sn_ = common::OB_INVALID_LOG_ID;
      size_ = common::OB_INVALID_SIZE;
      last_slide_fail_warn_ts_us_ = common::OB_INVALID_TIMESTAMP;
    }
  }

public:
  // desc: get pointer of slidingwindow[g_id], inc the reference count of slidingwindow[g_id].
  // Note that:
  // 1. slidingwindow[g_id] may contain a valid value or is empty
  // 2. There is no set() interface, caller could write slidingwindow[g_id] with ptr "val"
  //    returned by get() and should guarantee that Lock in data itself will be used to
  //    control the get()/write concurrent access
  // @param[in] g_id: should be in [begin_sn, end_sn)
  // @param[out] val: pointer to data saved in slidingwindow[g_id]
  // @return
  // - common::OB_SUCCESS: get log ptr success
  // - common::OB_NOT_INIT
  // - common::OB_ERR_OUT_OF_LOWER_BOUND: g_id < begin_sn_
  // - common::OB_ERR_OUT_OF_UPPER_BOUND: g_id >= end_sn_
  // - common::OB_ERR_UNEXPECTED: fail to retire refcount when range has changed, must be bug
  int get(const int64_t g_id, T *&val)
  {
    int ret = common::OB_SUCCESS;
    int is_locked = common::OB_ERROR;

    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      PALF_LOG(WARN, "FixedSlidingWindow not init", KR(ret));
    } else if (OB_FAIL(is_locked = lock_.rdlock())) {
      // pass
    } else if (OB_SUCC(check_id_in_range_(g_id))) {
      int64_t idx = calc_idx_(g_id);
      T *tmp_ptr = &(array_[idx]);
      // for unittest
      // while (true == PALF_FIXED_SW_GET_HUNG && g_id == 10) {
      //   ob_usleep(1000);
      //   PALF_LOG(TRACE, "sw get hung", K(g_id));
      // }
      ATOMIC_INC(&(array_[idx].ref_));
      // double check to avoid array_[idx] has been slid
      // Note that we do not require begin_sn_ and end_dn_ have not changed during get(),
      // we just require that idx is still in the legal range and array_[idx] hasn't been reset by slide() before return val
      if (OB_SUCC(check_id_in_range_(g_id))) {
        val = tmp_ptr;
        PALF_LOG(TRACE, "get succ", K(g_id), K(array_[idx].ref_));
      } else if (OB_SUCC(revert_(g_id))) {
        // begin_sn_ inc and greater than g_id, so dec ref count and return common::OB_ERR_OUT_OF_LOWER_BOUND
        // must call revert rather than ATOMIC_DEC(&array_[idx].ref_);
        PALF_LOG(INFO, "get fail and revert", K(g_id), K(begin_sn_), K(array_[idx].ref_));
        ret = common::OB_ERR_OUT_OF_LOWER_BOUND;
      } else {
        PALF_LOG(ERROR, "fail to retire refcount when range has changed", KR(ret), K(g_id), K(begin_sn_), K(end_sn_));
      }
    }

    if (common::OB_SUCCESS == is_locked) {
      lock_.rdunlock();
    }
    return ret;
  }

  // desc: revert reference count of slidingwindow[r_id], may inc end_sn_
  // @param[in] r_id: should be in [begin_sn_ - size_, end_sn_)
  // @return
  // - common::OB_SUCCESS
  // - common::OB_NOT_INIT
  // - common::OB_ERR_UNEXPECTED: r_id >= end_sn_ , r_id < (begin_sn_ - size_), ref_ < 0, must be bug
  int revert(const int64_t r_id)
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      PALF_LOG(WARN, "FixedSlidingWindow not init", KR(ret));
    } else if (OB_UNLIKELY(r_id < (get_begin_sn() - size_))) {
      ret = common::OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "FixedSlidingWindow revert error", KR(ret), K(r_id), K(begin_sn_), K(end_sn_));
    } else {
      ret = revert_(r_id);
    }
    return ret;
  }

private:
  int revert_(const int64_t r_id)
  {
    int ret = common::OB_SUCCESS;
    // if r_id >= end_sn_, then slidingwindow[r_id] must haven't be getted, and revert(r_id) makes no sense.
    if (OB_UNLIKELY(r_id >= get_end_sn_())) {
      ret = common::OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "FixedSlidingWindow revert error", KR(ret), K(r_id), K(begin_sn_), K(end_sn_));
    } else {
      // if r_id's slot is same as end_sn_ and r_id < begin_sn_, it means this slot can be reclaimed if ref count == 0
      // ===========constant array============
      //           |not  reclaimed|
      // |----|----|----|----|----|----|----|
      // |log |log |    |    |    |log |log |
      // |----|----|----|----|----|----|----|
      //            / |              |
      //        r_id  end_sn_       begin_sn_
      int64_t idx = calc_idx_(r_id);
      int64_t tmp_id = r_id;
      int64_t curr_ref = common::OB_INVALID_COUNT;
      if (OB_UNLIKELY(0 > (curr_ref = ATOMIC_SAF(&(array_[idx].ref_), 1)))) {
        ret = common::OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "FixedSlidingWindow revert error", KR(ret), K(r_id), K(begin_sn_), K(end_sn_), K(curr_ref));
      } else if (0 == curr_ref) {
        int64_t tmp_end = get_end_sn_();
        PALF_LOG(TRACE, "revert zero", K(r_id), K(begin_sn_), K(tmp_end));
        for (int64_t i = idx; i == calc_idx_(tmp_end) && tmp_id < get_begin_sn();) {
          // slot mutex lock for revert/slide, revert/revert
          ObByteLockGuard guard(array_[i].slot_lock_);
          PALF_LOG(TRACE, "try revert reset", K(i), K(tmp_end), K(begin_sn_));
          // double check end_sn_ to avoid that revert operation inc wrong end_sn_
          if (!(tmp_end == get_end_sn_() && 0 == ATOMIC_LOAD(&(array_[i].ref_)))) {
            break;
          } else {
            // Order is vital: 1. reset slot 2. inc end_sn_, for get/revert mutex
            PALF_LOG(TRACE, "do revert reset", K(i), K(tmp_end), K(begin_sn_));
            array_[i].reset();
            if (ATOMIC_BCAS(&(end_sn_), tmp_end, (tmp_end+1))) {
              i = calc_idx_(i+1);
              ++tmp_end;
              ++tmp_id;
            } else {
              // defensive code
              ret = OB_ERR_UNEXPECTED;
              PALF_LOG(ERROR, "end_sn_ changed during reverting", KR(ret), K(end_sn_), K(tmp_end), K(tmp_id));
              break;
            }
          }
        }
      } else {
        PALF_LOG(TRACE, "revert succ", K(r_id), K(curr_ref), K(begin_sn_), K(end_sn_));
      }
    }
    return ret;
  }

public:
  // desc: slide the slidingwindow[begin_sn_] continuously until timeout
  // For each log, first execute sliding_cb() and then inc begin_sn_
  // @param[in] timeout_us: slide continuously until timeout
  // @param[in] cb: sliding callback ptr
  // @return:
  // - common::OB_SUCCESS
  // - common::OB_NOT_INIT
  // - common::OB_ERR_UNEXPECTED: must be bug
  int slide(const int64_t timeout_us, ISlidingCallBack *cb)
  {
    int ret = common::OB_SUCCESS;
    // TODO: waiting time will be prolonged by lock timeout
    int64_t begin_time = common::ObClockGenerator::getClock();
    RLockGuardWithTimeout guard(lock_, begin_time + timeout_us, ret);
    SlidingCond cond(cb);

    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      PALF_LOG(WARN, "FixedSlidingWindow not init", KR(ret));
    } else if (OB_FAIL(ret)) {
      // truncate lock timeout
      ret = common::OB_SUCCESS;
    } else {
      while (OB_SUCC(ret)) {
        int64_t curr_begin = get_begin_sn();
        int64_t curr_end = get_end_sn_();
        int64_t idx = calc_idx_(curr_begin);
        ObByteLockGuard guard(array_[idx].slot_lock_);
        if (curr_end - curr_begin <= 0 || curr_begin != get_begin_sn()) {
          // pass and retry until timeout
          // check slide condition and execute sliding_cb before inc begin_sn
        } else if (!cond(curr_begin, &(array_[idx]))) {
          ret = cond.ret_;
          break;
        } else {
          // begin_sn_ and end_sn_ locate in same index and ref count == 0,
          // indicate that slidingwindow is full and we should recliam this slot after slide
          // so reset LogTask and inc end_sn_

          // Order is vital!!! lock next slot before inc begin_sn_,
          // avoid concurrent sliding in next slot do not see updated end_sn_, therefore end_sn_ will not be incremented forever.
          int64_t next_idx = calc_idx_(curr_begin + 1);
          ObByteLockGuard slide_guard(array_[next_idx].slot_lock_);

          // Order is vital!!! For get/slide mutex, first update begin_sn_, then check ref count.
          if (!ATOMIC_BCAS(&(begin_sn_), curr_begin, (curr_begin+1))) {
            // defensive code
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "begin_sn_ changed when sliding", KR(ret), K(begin_sn_), K(curr_begin));
          }
          // Order is vital!!! For revert/slide mutex, first update begin_sn_, then get_end_sn
          curr_end = get_end_sn_();
          PALF_LOG(TRACE, "slide plus begin", K(curr_begin), K(curr_end), K(array_[idx].ref_));
          if (0 == ATOMIC_LOAD(&(array_[idx].ref_)) && calc_idx_(curr_end) == idx) {
            // Order is vital: 1. reset slot 2. inc end_sn_, for get/slide mutex
            array_[idx].reset();
            PALF_LOG(TRACE, "before slide reset", K(curr_begin), K(curr_end), K(array_[idx].ref_));
            if (!ATOMIC_BCAS(&(end_sn_), curr_end, (curr_end+1))) {
              // defensive code
              ret = OB_ERR_UNEXPECTED;
              PALF_LOG(ERROR, "end_sn_ changed when sliding", KR(ret), K(end_sn_), K(curr_end));
            }
            PALF_LOG(TRACE, "after slide reset", K(ret), K(curr_begin), K(curr_end), K(begin_sn_),
                K(end_sn_), K(array_[idx].ref_));
          }
        }
        if (OB_SUCC(ret) && (timeout_us != 0) && ((common::ObClockGenerator::getClock() - begin_time) > timeout_us)) {
          break;
        }
      }
      PALF_LOG(TRACE, "slide end", K(ret), K(begin_sn_), K(end_sn_));
    }

    return ret;
  }

  // desc: truncate slidingwindow[t_id, end_sn)
  // @param[in] t_id: truncate begin id, t_id should >= begin_sn_
  // @return:
  // - common::OB_SUCCESS
  // - common::OB_NOT_INIT
  // - common::OB_ERR_UNEXPECTED: t_id < begin_sn_, must be bug
  int truncate(const int64_t t_id) const
  {
    int ret = common::OB_SUCCESS;
    PALF_LOG(INFO, "FixedSlidingWindow begin truncate");
    WLockGuard guard(lock_);
    PALF_LOG(INFO, "FixedSlidingWindow truncate lock success");
    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      PALF_LOG(WARN, "FixedSlidingWindow not init", KR(ret));
    } else if (OB_FAIL(check_id_in_range_(t_id))) {
      if (common::OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        ret = common::OB_SUCCESS;
      } else if (common::OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        ret = common::OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "truncate id is smaller than begin_sn_", KR(ret), K(t_id), K(begin_sn_), K(end_sn_));
      } else {
        // pass
      }
    } else if (OB_FAIL(range_truncate_(t_id, get_end_sn_()))) {
      PALF_LOG(ERROR, "range_truncate_failed", KR(ret));
    } else {
      PALF_LOG(INFO, "FixedSlidingWindow truncate success", K(ret), K(t_id), K(begin_sn_), K(end_sn_));
    }
    return ret;
  }

  // @desc: forward truncate sliding window and reset begin_sn_
  // @param[in] t_id: truncate begin id, t_id should >= begin_sn_
  // @return:
  // - common::OB_SUCCESS
  // - common::OB_NOT_INIT
  // - common::OB_INVALID_ARGUMENT: t_id < begin_sn_
  int truncate_and_reset_begin_sn(const int64_t t_id)
  {
    int ret = common::OB_SUCCESS;
    PALF_LOG(INFO, "FixedSlidingWindow begin forward_truncate");
    WLockGuard guard(lock_);
    PALF_LOG(INFO, "FixedSlidingWindow forward_truncate lock success", K(begin_sn_), K(end_sn_), K(t_id));
    if (IS_NOT_INIT) {
      ret = common::OB_SUCCESS;
      PALF_LOG(WARN, "FixedSlidingWindow not init", KR(ret));
    } else if (t_id < get_begin_sn()) {
      ret = common::OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(t_id), K_(begin_sn), K(ret));
    } else if (calc_idx_(get_begin_sn()) != calc_idx_(get_end_sn_())) {
      ret = common::OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "ref count is not zero before forward_truncate", KR(ret), K(begin_sn_), K(end_sn_));
    } else if (OB_FAIL(check_id_in_range_(t_id))) {
      if (OB_FAIL(range_truncate_(get_begin_sn(), get_end_sn_()))) {
        PALF_LOG(ERROR, "reand_truncate failed", K_(begin_sn), K_(end_sn), K(t_id));
      } else {
        begin_sn_ = t_id;
        end_sn_ = t_id + size_;
      }
    } else if (OB_FAIL(range_truncate_(get_begin_sn(), t_id))) {
      PALF_LOG(ERROR, "range_truncate failed", KR(ret), K(t_id));
    } else {
      begin_sn_ = t_id;
      end_sn_ = begin_sn_ + size_;
      PALF_LOG(INFO, "truncate_and_reset_begin_sn success", K(begin_sn_), K(end_sn_), K(t_id));
    }
    return ret;
  }

  int64_t get_begin_sn() const { return ATOMIC_LOAD(&(begin_sn_)); }
  int64_t get_end_sn() const { return get_end_sn_(); }

private:
  int clear_() const
  {
    WLockGuard guard(lock_);
    int64_t begin_sn = get_begin_sn();
    return range_truncate_(get_begin_sn(), get_end_sn_());
  }

  // truncate sliding window [begin_tid, end_tid)
  // caller should ensure args are valid
  int range_truncate_(const int64_t begin_tid, const int64_t end_tid) const
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      PALF_LOG(WARN, "FixedSlidingWindow not init", KR(ret));
    } else {
      // wait ref count == 0 and reset to end_sn_
      // during truncating end_sn_ may change, but we don't handle changing end_sn_
      // because revert will inc end_sn_ and reset object
      int64_t idx = 0;
      for (int64_t i = begin_tid; i < end_tid; ++i) {
        idx = calc_idx_(i);
        while (0 != ATOMIC_LOAD(&(array_[idx].ref_))) {
          ob_usleep(1000);
        }
        array_[idx].reset();
      }
    }
    return ret;
  }

  int64_t get_end_sn_() const { return ATOMIC_LOAD(&(end_sn_)); }
  int check_id_in_range_(const int64_t id) const
  {
    int ret = common::OB_SUCCESS;
    // id in [begin_sn_, end_sn_) is legal
    if (id < get_begin_sn()) {
      ret = common::OB_ERR_OUT_OF_LOWER_BOUND;
    } else if(id >= get_end_sn_()) {
      ret = common::OB_ERR_OUT_OF_UPPER_BOUND;
    }
    return ret;
  }
  int64_t calc_idx_(const int64_t log_id) const
  {
    return log_id & (size_ - 1);
  }

private:
  typedef common::RWLock::RLockGuardWithTimeout RLockGuardWithTimeout;
  typedef common::RWLock::WLockGuard WLockGuard;
  typedef common::RWLock::RLockGuard RLockGuard;

private:
  // for truncate mutex, during truncating, can not get and slide
  mutable common::RWLock lock_;
  bool is_inited_;
  // ============ls================
  //       ___________________
  //      |                   |
  // |----|----|----|----|----|----|----|
  // |    |log |log |    |log |    |    |
  // |----|----|----|----|----|----|----|
  //      |_|_________________| |
  //        |                   |
  //      begin_sn_           end_sn_
  // slidingwindow range [begin_sn_, end_sn_)
  int64_t begin_sn_;
  int64_t end_sn_;
  int64_t size_;
  int64_t last_slide_fail_warn_ts_us_;
  T *array_;
  ObILogAllocator *alloc_mgr_;
  DISALLOW_COPY_AND_ASSIGN(FixedSlidingWindow);
};

} // namespace palf
} // namespace oceanbase
#endif
