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

#ifndef OCEANBASE_MYSQL_OB_DL_QUEUE_H_
#define OCEANBASE_MYSQL_OB_DL_QUEUE_H_

#include "lib/allocator/ob_allocator.h"
#include "share/ob_define.h"
#include "ob_ra_queue.h"
#include "lib/lock/ob_rwlock.h"
#include "lib/function/ob_function.h"

namespace oceanbase
{
namespace common
{
using Ref = ObRaQueue::Ref;
class ObLeafQueue : public ObRaQueue
{
public:
  enum { N_LEAF_REF = 128 * 1024 };
  ObLeafQueue() : ObRaQueue()
  {
    memset(ref_, 0, sizeof(ref_));
  }
  ~ObLeafQueue() { memset(ref_, 0, sizeof(ref_)); }
  int push(void* p, int64_t& leaf_idx, int64_t root_idx, int64_t& seq, int64_t leaf_queue_size);

private:
  void wait_ref_clear(int64_t seq) {
    while(0 != ATOMIC_LOAD(ref_ + seq % N_LEAF_REF)) {
      ob_usleep(1000);
    }
  }
  int64_t xref(int64_t seq, int64_t x) {
    return ATOMIC_AAF(ref_ + seq % N_LEAF_REF, x);
  }
  int64_t ref_[N_LEAF_REF] CACHE_ALIGNED;
};

using ObRWLock = obsys::ObRWLock;
class ObRootQueue : public ObRaQueue
{
public:
  enum { N_ROOT_SIZE = 16 * 1024};
  ObRootQueue() : ObRaQueue()
  {
    for (int64_t i=0; i<N_ROOT_SIZE; i++) {
      new(&SlotLock_[i]) ObRWLock(obsys::WRITE_PRIORITY);
    }
  }
  ~ObRootQueue()
  {
    for (int64_t i=0; i<N_ROOT_SIZE; i++) {
      SlotLock_[i].~ObRWLock();
    }
  }
  ObLeafQueue** get_root_array() {
    return reinterpret_cast<ObLeafQueue**>(array_);
  }

  void* get(uint64_t seq, Ref* ref) {
    void* ret = NULL;
    if (NULL != array_) {
      void** addr = get_addr(seq);
      SlotLock_[seq % N_ROOT_SIZE].rlock()->lock();
      ATOMIC_STORE(&ret, *addr);
      if (NULL == ret) {
        ATOMIC_STORE(addr, NULL);
      } else {
        ref->set(seq, ret);
      }
    }
    if (NULL != ret) {
      // xref(seq, 1);
      ATOMIC_STORE(get_addr(seq), ret);
    }

    return ret;
  }
  void revert(Ref* ref) {
    if (NULL != ref) {
      uint64_t idx = ref->idx_;
      ref->reset();
      if (NULL != array_) {
        SlotLock_[idx % N_ROOT_SIZE].rlock()->unlock();
      }
    }
  }
  void* pop() {
    void* p = NULL;
    if (NULL == array_) {
      // ret = OB_NOT_INIT;
    } else {
      uint64_t pop_limit = 0;
      bool lock_succ = !SlotLock_[pop_ % N_ROOT_SIZE].wlock()->trylock();
      if (lock_succ) {
        uint64_t pop_idx = faa_bounded(&pop_, &push_, pop_limit);
        if (pop_idx < pop_limit) {
          void** addr = get_addr(pop_idx);
          while(NULL == (p = ATOMIC_LOAD(addr)) || !ATOMIC_BCAS(addr, p, NULL));
        } else {
          // ret = OB_SIZE_OVERFLOW;
        }
        SlotLock_[pop_idx % N_ROOT_SIZE].wlock()->unlock();
      }
    }
    return p;
  }

  bool is_null_leaf(int64_t seq) {
    void* ret = NULL;
    bool is_null = true;
    if (NULL != array_) {
      void** addr = get_addr(seq);
      ATOMIC_STORE(&ret, *addr);
      if (ret == NULL) {
        is_null = true;
      } else {
        is_null = false;
      }
    }
    return is_null;
  }

private:
  ObRWLock SlotLock_[N_ROOT_SIZE];
};

using Leaf_Ref = ObRaQueue::Ref;
using Root_Ref = ObRaQueue::Ref;
class ObDlQueue
{
public:
  static const int64_t DEFAULT_ROOT_QUEUE_SIZE = 16 * 1024; // 16k
  static const int64_t DEFAULT_LEAF_QUEUE_SIZE = 64 * 1024; // 64k
  static const int64_t DEFAULT_IDLE_LEAF_QUEUE_NUM = 8;
  static const int64_t RETRY_TIMES = 3; // push retry 3 times
  static const uint64_t RELEASE_WAIT_TIMES = 3; //3
  struct DlRef
  {
    DlRef(): root_ref_(), leaf_ref_()
    {}
    ~DlRef()
    {
      root_ref_.reset();
      leaf_ref_.reset();
    }
    void reset() {
      root_ref_.reset();
      leaf_ref_.reset();
    }
    bool is_not_null() {
      bool is_not_null = false;
      if (root_ref_.idx_ != -1 || leaf_ref_.idx_ != -1) {
        is_not_null = true;
      }
      return is_not_null;
    }
    Root_Ref root_ref_;
    Leaf_Ref leaf_ref_;
  };
  ObDlQueue() :
    rq_()
  {
    rq_cur_idx_ = 0;
    start_idx_ = 0;
    end_idx_ = 0;
    tenant_id_ = 0;
    release_wait_times_ = 0;
    root_queue_size_ = ObDlQueue::DEFAULT_ROOT_QUEUE_SIZE;
    leaf_queue_size_ = ObDlQueue::DEFAULT_LEAF_QUEUE_SIZE;
    idle_leaf_queue_num_ = ObDlQueue::DEFAULT_IDLE_LEAF_QUEUE_NUM;
  }
  ~ObDlQueue()
  {
    destroy();
  }
  int init(const char *label, uint64_t tenant_id,
          int64_t root_queue_size = ObDlQueue::DEFAULT_ROOT_QUEUE_SIZE,
          int64_t leaf_queue_size = ObDlQueue::DEFAULT_LEAF_QUEUE_SIZE,
          int64_t idle_leaf_queue_num = ObDlQueue::DEFAULT_IDLE_LEAF_QUEUE_NUM);
  int prepare_alloc_queue();

  int clear_leaf_queue(int64_t idx, int64_t size,
                      const std::function<void(void*)> &freeCallback)
  {
    int ret = OB_SUCCESS;
    void* req = NULL;
    int64_t count = 0;
    ObLeafQueue* leaf_rec = NULL;
    Root_Ref tmp_ref;

    if (OB_FAIL(get_leaf_queue(idx, leaf_rec, &tmp_ref))) {
      SERVER_LOG(WARN, "failed to get second level queue pointer", K(idx), K(ret));
    } else if (leaf_rec == NULL) {
      // do nothing
    } else {
      while (count++ < size && NULL != (req = leaf_rec->pop())) {
        // Call the callback function to release memory
        freeCallback(req);
      }
      int64_t start_idx = idx * leaf_queue_size_ + leaf_rec->get_pop_idx();
      set_start_idx(start_idx);
    }

    revert_leaf_queue(&tmp_ref, leaf_rec);
    return ret;
  }

  int push(void* p, int64_t& seq);
  ObLeafQueue* pop();
  void* get(uint64_t seq, DlRef* ref);
  void revert(DlRef* ref);
  void destroy();
  uint64_t get_push_idx() const { return rq_.get_push_idx(); }
  uint64_t get_pop_idx() const { return rq_.get_pop_idx(); }
  uint64_t get_cur_idx() const { return rq_cur_idx_; }
  void set_start_idx(int64_t idx) { ATOMIC_STORE(&start_idx_, idx);}
  void set_end_idx(int64_t idx) { ATOMIC_STORE(&end_idx_, idx);}
  int64_t get_start_idx() const { return start_idx_; }
  int64_t get_end_idx();
  int64_t get_capacity();
  int64_t get_size_used();

  int get_leaf_size_used(int64_t idx, int64_t &size);
  int need_clean_leaf_queue(int64_t idx, bool &need_clean);
  int release_record(int64_t release_cnt, const std::function<void(void*)> &free_callback,
                              bool is_destroyed);

private:
  int construct_leaf_queue();
  int get_leaf_queue(const int64_t idx, ObLeafQueue *&leaf_queue, Root_Ref* ref);
  void revert_leaf_queue(Root_Ref* ref, ObLeafQueue *&leaf_queue);

  volatile uint64_t rq_cur_idx_ CACHE_ALIGNED;
  volatile int64_t start_idx_ CACHE_ALIGNED;
  volatile int64_t end_idx_ CACHE_ALIGNED;
  uint64_t tenant_id_ CACHE_ALIGNED;
  ObRootQueue rq_;
  uint64_t release_wait_times_;
  lib::ObLabel label_;

  int64_t root_queue_size_;
  int64_t leaf_queue_size_;
  int64_t idle_leaf_queue_num_;
};

}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_MYSQL_OB_DL_QUEUE_H_ */
