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

#ifndef OCEANBASE_ALLOCATOR_OB_RETIRE_STATION_H_
#define OCEANBASE_ALLOCATOR_OB_RETIRE_STATION_H_
#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
class QClock
{
public:
  enum { MAX_QCLOCK_SLOT_NUM = OB_MAX_CPU_NUM * 64 };
  struct ClockSlot
  {
    ClockSlot(): clock_(UINT64_MAX) {}
    ~ClockSlot() {}
    uint64_t clock_;
    bool set_clock(uint64_t x) { return ATOMIC_BCAS(&clock_, UINT64_MAX, x);}
    void clear_clock() { ATOMIC_STORE(&clock_, UINT64_MAX); }
    uint64_t load_clock() { return ATOMIC_LOAD(&clock_); }
  };
  QClock(): clock_(1), qclock_(0) {}
  ~QClock() {}
  uint64_t enter_critical() {
    uint64_t slot_id = get_slot_id();
    const uint64_t begin_id = slot_id;
    while(!locate(slot_id)->set_clock(get_clock())) {
      sched_yield();
      slot_id = (slot_id + 1) % MAX_QCLOCK_SLOT_NUM;
      if (OB_UNLIKELY(begin_id == slot_id)) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "QClock slot maybe not enough", K(begin_id), K(MAX_QCLOCK_SLOT_NUM));
        }
      }
    }
    return slot_id;
  }
  void leave_critical(uint64_t slot_id) {
    if (UINT64_MAX != slot_id) {
      locate(slot_id)->clear_clock();
    }
  }

  uint64_t wait_quiescent(uint64_t clock) {
    uint64_t ret_clock = get_clock();
    while(!is_quiescent(clock)) {
      PAUSE();
    }
    inc_clock();
    return ret_clock;
  }

  bool try_quiescent(uint64_t &clock) {
    uint64_t cur_clock = get_clock();
    bool ret = false;
    if (is_quiescent(clock)) {
      clock = cur_clock;
      inc_clock();
      ret = true;
    }
    return ret;
  }
private:
  bool is_quiescent(uint64_t clock) {
    uint64_t cur_clock = get_clock();
    return clock < cur_clock && (clock < get_qclock() || clock < update_qclock(calc_quiescent_clock(cur_clock)));
  }
private:
  uint64_t get_slot_id() { return get_itid(); }
  ClockSlot* locate(uint64_t id) { return clock_array_ + (id % MAX_QCLOCK_SLOT_NUM); }
  uint64_t inc_clock() { return ATOMIC_AAF(&clock_, 1); }
  uint64_t get_clock() { return ATOMIC_LOAD(&clock_); }
  uint64_t get_qclock() { return ATOMIC_LOAD(&qclock_); }
  uint64_t update_qclock(uint64_t new_clock) {
    ATOMIC_STORE(&qclock_, new_clock);
    return new_clock;
  }
  uint64_t calc_quiescent_clock(uint64_t cur_clock) {
    uint64_t qclock = cur_clock;
    for(int64_t i = 0; i < MAX_QCLOCK_SLOT_NUM; i++){
      uint64_t tclock = locate(i)->load_clock();
      if (tclock < qclock) {
        qclock = tclock;
      }
    }
    return qclock;
  }
private:
  uint64_t clock_ CACHE_ALIGNED;
  uint64_t qclock_ CACHE_ALIGNED;
  ClockSlot clock_array_[MAX_QCLOCK_SLOT_NUM] CACHE_ALIGNED;
};

inline QClock& get_global_qclock()
{
  static QClock __global_qclock;
  return __global_qclock;
}

class HazardList
{
public:
  typedef ObLink Link;
  HazardList(): size_(0), head_(NULL), tail_(NULL) {}
  virtual ~HazardList() {}

public:
  int64_t size() { return size_; }

  void move_to(HazardList& target) {
    target.concat(*this);
    this->clear_all();
  }

  Link* pop() {
    Link* p = NULL;
    if (NULL != head_) {
      p = head_;
      head_ = head_->next_;
      if (NULL == head_) {
        tail_ = NULL;
      }
    }
    if (NULL != p) {
      size_--;
    }
    return p;
  }
  void push(Link* p) {
    if (NULL != p) {
      p->next_ = NULL;
      if (NULL == tail_) {
        head_ = tail_ = p;
      } else {
        tail_->next_ = p;
        tail_ = p;
      }
      size_++;
    }
  }

private:
  Link* get_head() { return head_; }
  Link* get_tail() { return tail_; }

  void concat(HazardList& that) {
    if (NULL == tail_) {
      head_ = that.get_head();
      tail_ = that.get_tail();
    } else if (NULL != that.get_tail()) {
      tail_->next_ = that.get_head();
      tail_ = that.get_tail();
    }
    size_ += that.size();
  }

  void clear_all() {
    size_ = 0;
    head_ = NULL;
    tail_ = NULL;
  }

private:
  int64_t size_;
  Link* head_;
  Link* tail_;
};


// RetireStation is a data structure that guarantees memory safety through delayed delete
// using a thread local retire list
//
class RetireStation
{
public:
  typedef HazardList List;
  enum { MAX_RETIRE_SLOT_NUM = OB_MAX_CPU_NUM * 32 };

  struct LockGuard
  {
    explicit LockGuard(uint64_t& lock): lock_(lock) {
      while(1 == ATOMIC_TAS(&lock_, 1)) {
        sched_yield();
      }
    }
    ~LockGuard() {
      ATOMIC_STORE(&lock_, 0);
    }
    uint64_t& lock_;
  };

  struct RetireList
  {
    RetireList(): lock_(0), retire_clock_(0) {}
    ~RetireList() {}
    void retire(List& reclaim_list, List& retire_list, int64_t limit, QClock& qclock) {
      LockGuard lock_guard(lock_);
      retire_list.move_to(prepare_list_);
      // Force recovery to prevent memory explosion when it is greater than limit
      if (prepare_list_.size() > limit) {
        retire_clock_ = qclock.wait_quiescent(retire_clock_);
        retire_list_.move_to(reclaim_list);
        prepare_list_.move_to(retire_list_);
      // Try to retire once every 64 rounds but don’t die. The purpose of setting it to 63 is to prevent try_quiescent when prepare_list is empty.
      } else if (63 == prepare_list_.size() % 64 && qclock.try_quiescent(retire_clock_)) {
        retire_list_.move_to(reclaim_list);
        prepare_list_.move_to(retire_list_);
      }
    }
    uint64_t lock_;
    uint64_t retire_clock_;
    List retire_list_;
    List prepare_list_;
  };

  RetireStation(QClock& qclock, int64_t retire_limit)
    : retire_limit_(retire_limit), qclock_(qclock) {}
  virtual ~RetireStation() {}

  void retire(List& reclaim_list, List& retire_list) {
    get_retire_list().retire(reclaim_list, retire_list, retire_limit_, qclock_);
  }
  void retire(List& reclaim_list, List& retire_list, int64_t limit) {
    get_retire_list().retire(reclaim_list, retire_list, limit, qclock_);
  }

  void purge(List& reclaim_list) {
    List retire_list;
    for(int i = 0; i < 2; i++) {
      for(int64_t id = 0; id < MAX_RETIRE_SLOT_NUM; id++) {
        retire_list_[id].retire(reclaim_list, retire_list, -1, qclock_);
      }
    }
  }

private:
  RetireList& get_retire_list() { return retire_list_[get_itid() % MAX_RETIRE_SLOT_NUM];}
private:
  int64_t retire_limit_;
  QClock& qclock_;
  RetireList retire_list_[MAX_RETIRE_SLOT_NUM];
};

class QClockGuard
{
public:
  explicit QClockGuard(QClock& qclock=get_global_qclock()): qclock_(qclock), slot_id_(qclock_.enter_critical()) {}
  ~QClockGuard() { qclock_.leave_critical(slot_id_); }
private:
  QClock& qclock_;
  uint64_t slot_id_;
};


}; // end namespace allocator
}; // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_RETIRE_STATION_H_ */
