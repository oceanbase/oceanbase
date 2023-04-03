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

#ifndef OCEANBASE_LIST_DLINK_H_
#define OCEANBASE_LIST_DLINK_H_
#include "lib/ob_define.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
// class CDLink
// {
// public:
//   CDLink(): next_(this), prev_(this) {}
//   ~CDLink() {}
//   CDLink* get_next() { return (CDLink*)clear_last_bit((uint64_t)next_); }

//   // -EINVAL: prev is delete by other thread
//   // 0:  insert succ
//   int insert(CDLink* prev)
//   {
//     int err = 0;
//     while(-EAGAIN == (err = do_insert(prev, this)))
//       ;
//     return err;
//   }

//   // -ENOENT: this node is delete by other thread
//   // 0: delete succ
//   int del()
//   {
//     int err = 0;
//     while(-EAGAIN == (err = do_del(search_direct_prev(ATOMIC_LOAD(&prev_), this), this)))
//       ;
//     return err;
//   }

// private:
//   static CDLink* search_direct_prev(CDLink* prev, CDLink* target)
//   {
//     CDLink* next = NULL;
//     while((next = (CDLink*)clear_last_bit((uint64_t)ATOMIC_LOAD(&prev->next_))) != target) {
//       prev = next;
//     }
//     return prev;
//   }

//   static void try_correct_prev_link(CDLink* target, CDLink* prev)
//   {
//     while(true) {
//       CDLink* old_prev = ATOMIC_LOAD(&target->prev_);
//       if (ATOMIC_LOAD(&prev->next_) != target) {
//         break;
//       } else if (old_prev == prev) {
//         break;
//       } else if (ATOMIC_BCAS(&target->prev_, old_prev, prev)) {
//         break;
//       }
//     }
//   }

//   static int do_insert(CDLink* prev, CDLink* target)
//   {
//     int err = 0;
//     CDLink* next = ATOMIC_LOAD(&prev->next_);
//     if (is_last_bit_set((uint64_t)next)) {
//       err = -EINVAL;
//     } else {
//       target->next_ = next;
//       target->prev_ = prev;
//       if (!ATOMIC_BCAS(&prev->next_, next, target)) {
//         err = -EAGAIN;
//       } else {
//         try_correct_prev_link(next, target);
//       }
//     }
//     return err;
//   }

//   static int do_del(CDLink* prev, CDLink* target)
//   {
//     int err = 0;
//     CDLink* next = (CDLink*)set_last_bit((uint64_t*)(&target->next_));
//     if (is_last_bit_set((uint64_t)next)) {
//       err = -ENOENT;
//     } else if (!ATOMIC_BCAS(&prev->next_, target, next)) {
//       err = -EAGAIN;
//       ATOMIC_STORE(&target->next_, (CDLink*)clear_last_bit((uint64_t)next));
//     } else {
//       try_correct_prev_link(next, prev);
//     }
//     return err;
//   }

//   static uint64_t set_last_bit(uint64_t* addr)
//   {
//     uint64_t v = 0;
//     while(true) {
//       v = ATOMIC_LOAD(addr);
//       if (0 != (v & 1)) {
//         break;
//       } else if (ATOMIC_BCAS(addr, v, v | 1)) {
//         break;
//       }
//     }
//     return v;
//   }
//   static bool is_last_bit_set(uint64_t x)
//   {
//     return x & 1;
//   }
//   static uint64_t clear_last_bit(uint64_t x)
//   {
//     return x & ~1;
//   }
//   CDLink* next_;
//   CDLink* prev_;
// };

// class QSync
// {
// public:
//   enum { MAX_REF_CNT = OB_MAX_CPU_NUM };
//   struct Ref
//   {
//     Ref(): ref_(0) {}
//     ~Ref() {}
//     int64_t ref_ CACHE_ALIGNED;
//   };
//   QSync() {}
//   ~QSync() {}

//   int64_t acquire_ref()
//   {
//     int64_t idx = icpu_id();
//     ref(idx, 1);
//     return idx;
//   }

//   void release_ref(int64_t idx)
//   {
//     ref(idx, -1);
//   }

//   void sync()
//   {
//     for(int64_t i = 0; i < MAX_REF_CNT; i++) {
//       Ref* ref = ref_array_ + i;
//       while(ATOMIC_LOAD(&ref->ref_) != 0)
//         ;
//     }
//   }

// private:
//   void ref(int64_t idx, int64_t x)
//   {
//     Ref* ref = ref_array_ + idx;
//     (void)ATOMIC_FAA(&ref->ref_, x);
//   }
// private:
//   Ref ref_array_[MAX_REF_CNT];
// };

// class CDLinkSet
// {
// public:
//   enum { MAX_HEAD_NUM = OB_MAX_CPU_NUM };
//   class Iterator
//   {
//   public:
//     Iterator(CDLinkSet& linkset): linkset_(linkset), ref_(linkset_.acquire_ref()), cur_(NULL) {}
//     ~Iterator() { linkset_.release_ref(ref_); }
//     CDLink* next() { return cur_ = linkset_.next(cur_); }
//   private:
//     CDLinkSet& linkset_;
//     int64_t ref_;
//     CDLink* cur_;
//   };

//   friend class Iterator;
//   CDLinkSet()
//   {
//     for(int64_t i = 1; i < MAX_HEAD_NUM; i++){
//       thread_head_[i].insert(thread_head_);
//     }
//   }
//   ~CDLinkSet() {}

//   int64_t size()
//   {
//     Iterator iter(*this);
//     int64_t cnt = 0;
//     while(NULL != iter.next()) {
//       cnt++;
//     }
//     return cnt;
//   }

//   void add(CDLink* p)
//   {
//     int64_t ref = qsync_.acquire_ref();
//     p->insert(thread_head_ + icpu_id());
//     qsync_.release_ref(ref);
//   }

//   void del(CDLink* p)
//   {
//     int64_t ref = qsync_.acquire_ref();
//     p->del();
//     qsync_.release_ref(ref);
//     qsync_.sync();
//   }

// private:
//   int64_t acquire_ref() { return qsync_.acquire_ref(); }
//   void release_ref(int64_t ref) { qsync_.release_ref(ref); }

//   CDLink* next(CDLink* p)
//   {
//     CDLink* ret = p ?: thread_head_;
//     while(!is_normal_link(ret = ret->get_next())) {
//       if (thread_head_ == ret) {
//         ret = NULL;
//         break;
//       }
//     }
//     return ret;
//   }

//   bool is_normal_link(CDLink* link)
//   {
//     return link < thread_head_ || link >= thread_head_ + MAX_HEAD_NUM;
//   }
// private:
//   QSync qsync_;
//   CDLink thread_head_[MAX_HEAD_NUM];
// };

}; // end namespace list
}; // end namespace oceanbase


#endif /* OCEANBASE_LIST_DLINK_H_ */
