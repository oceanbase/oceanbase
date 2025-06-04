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

#ifndef OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_
#define OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_

#include "lib/queue/ob_link_queue.h"
#include "lib/lock/ob_scond.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/resource/ob_affinity_ctrl.h"

namespace oceanbase
{
namespace common
{

template <int PRIOS>
class ObPriorityQueue
{
public:
  enum { PRIO_CNT = PRIOS };

  ObPriorityQueue() : sem_(), queue_(), size_(0), limit_(INT64_MAX) {}
  ~ObPriorityQueue() {}

  void set_limit(int64_t limit) { limit_ = limit; }
  inline int64_t size() const { return ATOMIC_LOAD(&size_); }

  int push(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    if (ATOMIC_FAA(&size_, 1) > limit_) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "push error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(queue_[priority].push(data))) {
      // do nothing
    } else {
      IGNORE_RETURN sem_.signal();
    }
    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&size_, -1);
    }
    return ret;
  }

  int push_front(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    ATOMIC_FAA(&size_, 1);
    if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "push front error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(queue_[priority].push_front(data))) {
      // do nothing
    } else {
      IGNORE_RETURN sem_.signal();
    }
    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&size_, -1);
    }
    return ret;
  }

  int pop(ObLink*& data, int64_t timeout_us)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    if (OB_UNLIKELY(timeout_us < 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "timeout is invalid", K(ret), K(timeout_us));
    } else {
      for(int i = 0; OB_ENTRY_NOT_EXIST == ret  && i < PRIO_CNT; i++) {
        if (OB_SUCCESS == queue_[i].pop(data)) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret)) {
        auto key = sem_.get_key();
        {
          common::ObBKGDSessInActiveGuard inactive_guard;
          sem_.wait(key, timeout_us);
        }
        data = NULL;
      } else {
        (void)ATOMIC_FAA(&size_, -1);
      }
    }
    return ret;
  }

  void destroy()
  {
    clear();
  }

  void clear()
  {
    ObLink* p = NULL;
    while(OB_SUCCESS == pop(p, 0))
      ;
  }

  ObLinkQueue* get_queue(const int i)
  {
    return &queue_[i];
  }

  int64_t get_prio_cnt() const
  {
    return PRIO_CNT;
  }
private:
  SimpleCond sem_;
  ObLinkQueue queue_[PRIO_CNT];
  int64_t size_ CACHE_ALIGNED;
  int64_t limit_ CACHE_ALIGNED;
  DISALLOW_COPY_AND_ASSIGN(ObPriorityQueue);
};

template <int HIGH_PRIOS, int NORMAL_PRIOS=0, int LOW_PRIOS=0, int MAX_QUEUE_NUM=1>
class ObPriorityQueue2
{
public:
  enum { PRIO_CNT = HIGH_PRIOS + NORMAL_PRIOS + LOW_PRIOS };

  ObPriorityQueue2() : sq_(), queue_num_(1), limit_(INT64_MAX) {
    mq_[0] = &sq_;
    for (int i = 1; i < MAX_QUEUE_NUM; i++) {
      mq_[i] = NULL;
    }
  }
  ~ObPriorityQueue2() {}

  void set_limit(int64_t limit) { limit_ = limit; }
  int init(int32_t queue_num) {
    int ret = OB_SUCCESS;

    if (queue_num <= 0 || queue_num > OB_MAX_NUMA_NUM) {
        queue_num_ = 1;
        COMMON_LOG(WARN, "failed to set invalid queue_num, fallback to 1", K(queue_num), K(queue_num_));
    } else {
        queue_num_ = queue_num;
    }

    if (is_mq_enabled()) {
      int i;
      ObMemAttr attr;
      attr.label_ = "ObPrioQueue2";
      attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
      attr.tenant_id_ = OB_SERVER_TENANT_ID;

      for (i = 0; i < queue_num && OB_SUCCESS == ret; i++) {
        void *buf = NULL;

        attr.numa_id_ = i % AFFINITY_CTRL.get_num_nodes();
        buf = ob_malloc(sizeof(Queue_), attr);
        if (!buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          mq_[i] = new (buf) Queue_();
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          for (int j = i - 1; j >= 0; j--) {
            ob_delete(mq_[j]);
          }
        }

        mq_[0] = &sq_;
        queue_num_ = 1;
        COMMON_LOG(WARN, "failed to initialize mq_, fallback to sq_", K(queue_num), K(queue_num_));
      }
    }

    return ret;
  }
  void destroy() {
    if (is_mq_enabled()) {
      for (int i = 0; i < queue_num_; i++) {
        ob_delete(mq_[i]);
      }
    }
  }
  int32_t get_queue_num() { return queue_num_; }
  inline int64_t size() const {
    int64_t sum = 0;

    for (int i = 0; i < queue_num_; i++) {
      sum += ATOMIC_LOAD(&mq_[i]->size);
    }

    return sum;
  }
  int64_t queue_size(const int prio) const
  {
    int64_t sum = 0;

    for (int i = 0; i < queue_num_; i++) {
      sum += mq_[i]->q[prio].size();
    }

    return sum;
  }
  int64_t get_prio_cnt() const
  {
    return PRIO_CNT;
  }
  ObLinkQueue* get_queue(const int queue_idx, const int prio)
  {
    ObLinkQueue *queue = NULL;

    if (queue_idx >= 0 && queue_idx < queue_num_) {
        queue = &mq_[queue_idx]->q[prio];
    }

    return queue;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "total_size=%ld ", size());
    for(int i = 0; i < queue_num_; i++) {
      for(int j = 0; j < PRIO_CNT; j++) {
        common::databuff_printf(buf, buf_len, pos, "queue[%d][%d]=%ld ", i, j, mq_[i]->q[j].size());
      }
    }
    return pos;
  }

  int push(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    int to_push_idx = queue_num_ <= 1 ? 0 : AFFINITY_CTRL.get_tls_node() % queue_num_;
    int64_t extra;

    if (priority < HIGH_PRIOS) {
      extra = 2048;
    } else if (priority < NORMAL_PRIOS + HIGH_PRIOS) {
      extra = 1024;
    } else {
      extra = 0;
    }

    if (ATOMIC_FAA(&mq_[to_push_idx]->size, 1) > limit_ + extra) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "push error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(mq_[to_push_idx]->q[priority].push(data))) {
      // do nothing
    } else {
      if (priority < HIGH_PRIOS) {
        mq_[to_push_idx]->cond.signal(1, 0);
      } else if (priority < NORMAL_PRIOS + HIGH_PRIOS) {
        mq_[to_push_idx]->cond.signal(1, 1);
      } else {
        mq_[to_push_idx]->cond.signal(1, 2);
      }
    }

    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&mq_[to_push_idx]->size, -1);
    }
    return ret;
  }

  int pop(ObLink*& data, int64_t timeout_us)
  {
    return do_pop(data, PRIO_CNT, timeout_us);
  }

  int pop_normal(ObLink*& data, int64_t timeout_us)
  {
    return do_pop(data, HIGH_PRIOS + NORMAL_PRIOS, timeout_us);
  }

  int pop_high(ObLink*& data, int64_t timeout_us)
  {
    return do_pop(data, HIGH_PRIOS, timeout_us);
  }

private:

  inline int try_pop(ObLink*& data, int64_t plimit, int to_pop_idx)
  {
    int ret = OB_ENTRY_NOT_EXIST;

    for(int i = 0; OB_ENTRY_NOT_EXIST == ret  && i < plimit; i++) {
      if (OB_SUCCESS == mq_[to_pop_idx]->q[i].pop(data)) {
        ret = OB_SUCCESS;
      }
    }

    return ret;
  }
  inline int wait_queue(int to_pop_idx, int64_t timeout_us) {
    ObBKGDSessInActiveGuard inactive_guard;

    return mq_[to_pop_idx]->cond.wait(timeout_us);
  }
  inline int try_steal(ObLink*& data, int64_t plimit, int start_idx, int &pop_success_idx) {
    int ret = OB_ENTRY_NOT_EXIST;

    for (int i = start_idx + 1; OB_ENTRY_NOT_EXIST == ret && i < start_idx + queue_num_; i++) {
      int to_steal_pop_idx = i % queue_num_;
      if (OB_SUCC(try_pop(data, plimit, to_steal_pop_idx))) {
        pop_success_idx = to_steal_pop_idx;
        COMMON_LOG(DEBUG, "steal succeeded ", K(to_steal_pop_idx), K(queue_num_), K(ret));
      }
    }

    return ret;
  }
  inline int do_pop(ObLink*& data, int64_t plimit, int64_t timeout_us)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    int to_pop_idx = queue_num_ <= 1 ? 0 : AFFINITY_CTRL.get_tls_node() % queue_num_;
    int pop_success_idx;

    if (OB_UNLIKELY(timeout_us < 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "timeout is invalid", K(ret), K(timeout_us));
    } else {
      if (plimit <= HIGH_PRIOS) {
        mq_[to_pop_idx]->cond.prepare(0);
      } else if (plimit <= NORMAL_PRIOS + HIGH_PRIOS) {
        mq_[to_pop_idx]->cond.prepare(1);
      } else {
        mq_[to_pop_idx]->cond.prepare(2);
      }
      if (OB_SUCC(try_pop(data, plimit, to_pop_idx))) {
        mq_[to_pop_idx]->is_queue_idle = false;
        pop_success_idx = to_pop_idx;
      } else if (OB_SUCCESS == wait_queue(to_pop_idx, (is_mq_enabled() && mq_[to_pop_idx]->is_queue_idle) ? timeout_us / 10 : timeout_us)) {
        mq_[to_pop_idx]->is_queue_idle = false;
      } else if (is_mq_enabled()) {
        mq_[to_pop_idx]->is_queue_idle = true;
        ret = try_steal(data, plimit, to_pop_idx, pop_success_idx);
      }

      if (OB_SUCCESS == ret) {
        // try_pop or try_steal success
        (void)ATOMIC_FAA(&mq_[pop_success_idx]->size, -1);
      } else {
        data = NULL;
      }
    }
    return ret;
  }
  inline bool is_mq_enabled() {
    return queue_num_ > 1;
  };

  class Queue_ {
  public:
    Queue_(): size(0), is_queue_idle(false) {}
    ~Queue_() {}
    SCondTemp<3> cond;
    ObLinkQueue q[PRIO_CNT];
    int64_t size;
    bool is_queue_idle;

  } CACHE_ALIGNED;
  Queue_ *mq_[MAX_QUEUE_NUM];
  Queue_ sq_;
  int32_t queue_num_;
  int64_t limit_;
  DISALLOW_COPY_AND_ASSIGN(ObPriorityQueue2);
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_
