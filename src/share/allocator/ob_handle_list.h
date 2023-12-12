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

#ifndef OCEANBASE_ALLOCATOR_OB_ACTIVE_LIST_H_
#define OCEANBASE_ALLOCATOR_OB_ACTIVE_LIST_H_
#include "lib/queue/ob_link.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObHandleList
{
public:
  typedef ObDLink DLink;
  struct DList
  {
    DLink head_;
    DLink tail_;
    DList() {
      head_.prev_ = NULL;
      head_.next_ = &tail_;
      tail_.prev_ = &head_;
      tail_.next_ = NULL;
    }

    DLink* next(DLink* iter) {
      if (NULL == iter) {
        iter = &head_;
      }
      iter = (DLink*)iter->next_;
      if (&tail_ == iter) {
        iter = NULL;
      }
      return iter;
    }
    DLink* prev(DLink* iter) {
      if (NULL == iter) {
        iter = &tail_;
      }
      iter = (DLink*)iter->prev_;
      if (&head_ == iter) {
        iter = NULL;
      }
      return iter;
    }
    static void dlink_insert(ObDLink* cur, ObDLink* x)
    {
      ObDLink* next = (ObDLink*)cur->next_;
      x->prev_ = cur;
      x->next_ = next;
      next->prev_ = x;
      cur->next_ = x;
    }

    static void dlink_del(ObDLink* x)
    {
      ObDLink* prev = (ObDLink*)x->prev_;
      ObDLink* next = (ObDLink*)x->next_;
      prev->next_ = next;
      next->prev_ = prev;
    }

    void add(DLink* x) {
      dlink_insert(&head_, x);
    }
    void del(DLink* x) {
      dlink_del(x);
    }
    template<typename Pred>
        void add(DLink* x, Pred& pred) {
      DLink* cur = &head_;
      DLink* next = NULL;
      while(&tail_ != (next = (DLink*)cur->next_)) {
        if (pred.ge(next)) {
          break;
        }
        cur = next;
      }
      dlink_insert(cur, x);
    }
  };
  struct Handle
  {
    enum {INIT = 0, ACTIVE = 1,  FROZEN = 2};
    int freeze_stat_;
    int64_t id_;
    int64_t clock_;
    DLink total_list_;
    DLink active_list_;
    void reset() {
      COMMON_LOG(DEBUG, "reset list");
      freeze_stat_ = INIT;
      id_ = -1;
      clock_ = INT64_MAX;
      total_list_.next_ = NULL;
      total_list_.prev_ = NULL;
      active_list_.next_ = NULL;
      active_list_.prev_ = NULL;
    }
    bool ge(DLink* x) {
      Handle* next = CONTAINER_OF(x, Handle, active_list_);
      return clock_ >= next->clock_;
    }
    void set_id(int64_t id) { ATOMIC_STORE(&id_, id); }
    int64_t get_id() const { return ATOMIC_LOAD(&id_); }
    bool is_id_valid() const { return get_id() >= 0; }
    void set_clock(int64_t clock) { ATOMIC_STORE(&clock_, clock); }
    int64_t get_clock() const { return ATOMIC_LOAD(&clock_); }
    bool set_active() {
      if (ATOMIC_LOAD(&freeze_stat_) == INIT) {
        ATOMIC_STORE(&freeze_stat_, ACTIVE);
      }
      return is_active();
    }
    void set_frozen() { ATOMIC_STORE(&freeze_stat_, FROZEN); }
    bool is_active() const { return ATOMIC_LOAD(&freeze_stat_) == ACTIVE; }
    bool is_frozen() const { return ATOMIC_LOAD(&freeze_stat_) == FROZEN; }
    TO_STRING_KV(K_(freeze_stat), K_(id), K_(clock));
  };
  ObHandleList(): id_(0), hazard_(INT64_MAX), total_count_(0) {
  }
  ~ObHandleList() {}
  void init_handle(Handle& handle);
  void destroy_handle(Handle& handle);
  void set_active(Handle& handle); 
  void set_frozen(Handle& handle);
  bool is_empty() const { return ATOMIC_LOAD(&total_count_) <= 0; }
  int64_t hazard() const { return ATOMIC_LOAD(&hazard_); }
  DLink* next(DLink* iter) { return total_list_.next(iter); }
  DLink* prev(DLink* iter) { return total_list_.prev(iter); }
protected:
  void set_frozen_(Handle& handle);
  int64_t alloc_id() { return ATOMIC_AAF(&id_, 1); }
private:
  void update_hazard();
  int64_t calc_hazard();
private:
  int64_t id_;
  DList total_list_;
  DList active_list_;
  int64_t hazard_;
  int64_t total_count_;
};

}; // end namespace common
}; // end namespace oceanbase
#endif /* OCEANBASE_ALLOCATOR_OB_ACTIVE_LIST_H_ */
