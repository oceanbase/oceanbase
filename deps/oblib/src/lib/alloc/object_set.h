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

#ifndef _OCEABASE_LIB_ALLOC_OBJECT_SET_H_
#define _OCEABASE_LIB_ALLOC_OBJECT_SET_H_

#include "alloc_struct.h"
#include "alloc_assist.h"
#include "abit_set.h"
#include "block_set.h"
#include "lib/lock/ob_mutex.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{
namespace common
{
class ObAllocator;
}
namespace lib
{
class __MemoryContext__;
class ObTenantCtxAllocator;
class IBlockMgr;
class ISetLocker;
class ObjectSet
{
  friend class common::ObAllocator;
  static const uint32_t META_CELLS = (AOBJECT_META_SIZE - 1) / AOBJECT_CELL_BYTES + 1;
  static const uint32_t MIN_FREE_CELLS = META_CELLS + 1+ (15 / AOBJECT_CELL_BYTES);  // next, prev pointer
  static const double FREE_LISTS_BUILD_RATIO;
  static const double BLOCK_CACHE_RATIO;

  typedef AObject *FreeList;
  typedef ABitSet BitMap;

public:
  ObjectSet(__MemoryContext__ *mem_context=nullptr,
            const uint32_t ablock_size=INTACT_NORMAL_AOBJECT_SIZE,
            const bool enable_dirty_list=false);
  ~ObjectSet();

  // main interfaces
  AObject *alloc_object(const uint64_t size, const ObMemAttr &attr);
  void free_object(AObject *obj);
  AObject *realloc_object(AObject *obj, const uint64_t size, const ObMemAttr &attr);
  void reset();

  // statistics
  uint64_t get_alloc_bytes() const;
  uint64_t get_hold_bytes() const;
  uint64_t get_allocs() const;

  void lock();
  void unlock();

  // statistics
  void set_block_mgr(IBlockMgr *blk_mgr) { blk_mgr_ = blk_mgr; }
  IBlockMgr *get_block_mgr() { return blk_mgr_; }
  void set_locker(ISetLocker *locker) { locker_ = locker; }
  inline int64_t get_normal_hold() const;
  inline int64_t get_normal_used() const;
  inline int64_t get_normal_alloc() const;
  bool check_has_unfree(char *first_label, char *first_bt);

private:
  AObject *alloc_normal_object(const uint32_t cls, const ObMemAttr &attr);
  AObject *alloc_big_object(const uint64_t size, const ObMemAttr &attr);

  ABlock *alloc_block(const uint64_t size, const ObMemAttr &attr);
  void free_block(ABlock *block);

  AObject *get_free_object(const uint32_t cls);
  void add_free_object(AObject *obj);

  void free_big_object(AObject *obj);
  void take_off_free_object(AObject *obj);
  void free_normal_object(AObject *obj);

  bool build_free_lists();

  inline AObject *split_obj(AObject *obj, const uint32_t cls, AObject *&remainder);
  inline AObject *merge_obj(AObject *obj);

  void do_free_object(AObject *obj);
  void do_free_dirty_list();

private:
  __MemoryContext__ *mem_context_;
  ISetLocker *locker_;
  IBlockMgr *blk_mgr_;

  ABlock *blist_;

  AObject *last_remainder_;

  BitMap *bm_;
  FreeList *free_lists_;

  lib::ObMutex dirty_list_mutex_;
  AObject *dirty_list_;
  int64_t dirty_objs_;

  uint64_t alloc_bytes_;
  uint64_t used_bytes_;
  uint64_t hold_bytes_;
  uint64_t allocs_;

  uint64_t normal_alloc_bytes_;
  uint64_t normal_used_bytes_;
  uint64_t normal_hold_bytes_;

  uint32_t ablock_size_;
  bool enable_dirty_list_;
  uint32_t cells_per_block_;

  DISALLOW_COPY_AND_ASSIGN(ObjectSet);
} CACHE_ALIGNED; // end of class ObjectSet

inline void ObjectSet::lock()
{
  ObDisableDiagnoseGuard diagnose_disable_guard;
  locker_->lock();
}

inline void ObjectSet::unlock()
{
  ObDisableDiagnoseGuard diagnose_disable_guard;
  locker_->unlock();
}

inline uint64_t ObjectSet::get_alloc_bytes() const
{
  return alloc_bytes_;
}

inline uint64_t ObjectSet::get_hold_bytes() const
{
  return hold_bytes_;
}

inline uint64_t ObjectSet::get_allocs() const
{
  return allocs_;
}

inline int64_t ObjectSet::get_normal_hold() const
{
  return normal_hold_bytes_;
}

inline int64_t ObjectSet::get_normal_used() const
{
  return normal_used_bytes_;
}

inline int64_t ObjectSet::get_normal_alloc() const
{
  return normal_alloc_bytes_;
}

class ObjectSetV2
{
public:
  int calc_sc_idx(const int64_t x)
  {
    if (OB_LIKELY(x <= 1024)) {
      return (((x + 63) & ~63) >> 6) - 1;
    } else {
      return 8 * sizeof(int64_t) - __builtin_clzll(x - 1) - 11 + 16;
    }
  }
  ObjectSetV2() {
    memset(scs, 0, sizeof(scs));
  }
  ~ObjectSetV2();
  void set_block_mgr(IBlockMgr *blk_mgr) { blk_mgr_ = blk_mgr; }
  IBlockMgr *get_block_mgr() { return blk_mgr_; }
  AObject *alloc_object(const uint64_t size, const ObMemAttr &attr);
  void free_object(AObject *obj, ABlock *block);
  ABlock *alloc_block(const uint64_t size, const ObMemAttr &attr);
  void free_block(ABlock *block);
  void do_cleanup();
  void do_free_object(AObject *obj, ABlock *block);

  class Lock
  {
  public:
    static constexpr int32_t WAIT_MASK = 1<<31;
    static constexpr int32_t WRITE_MASK = 1<<30;
    Lock() : v_(0), wait_cnt_(0)
    {}
    void lock()
    {
      const int32_t MAX_TRY_CNT = 16;
      bool locked = false;
      const int32_t tid = static_cast<uint32_t>(GETTID());
      for (int i = 0; i < MAX_TRY_CNT; ++i) {
        if (ATOMIC_BCAS(&v_, 0, tid | WRITE_MASK)) {
          locked = true;
          break;
        }
        sched_yield();
      }
      if (OB_UNLIKELY(!locked)) {
        wait(tid);
      }
    }
    void unlock()
    {
      int32_t v = ATOMIC_SET(&v_, 0);
      if (OB_UNLIKELY(WAIT_MASK == (v & WAIT_MASK))) {
        futex_wake(&v_, 1);
      }
    }
    void wait(const int32_t tid)
    {
      static constexpr timespec TIMEOUT = {0, 100 * 1000 * 1000};
      ATOMIC_INC(&wait_cnt_);
      while (true) {
        const int32_t v = v_;
        if (WAIT_MASK == (v & WAIT_MASK) || ATOMIC_BCAS(&v_, v | WRITE_MASK, v | WAIT_MASK)) {
          futex_wait(&v_, v | WRITE_MASK | WAIT_MASK, &TIMEOUT);
        }
        if (ATOMIC_BCAS(&v_, 0, tid | WRITE_MASK | WAIT_MASK)) {
          break;
        }
      }
      ATOMIC_DEC(&wait_cnt_);
    }
    bool trylock() { return false; }
    void enable_record_stat(bool) {}
    int32_t get_wait_cnt() const { return wait_cnt_; }
  private:
    int32_t v_;
    int32_t wait_cnt_;
  };
  struct SizeClass
  {
    void push_avail(ABlock* blk)
    {
      if (NULL == avail_) {
        blk->prev_ = blk;
        blk->next_ = blk;
        avail_ = blk;
      } else {
        blk->prev_ = avail_->prev_;
        avail_->prev_->next_ = blk;
        blk->next_ = avail_;
        avail_->prev_ = blk;
      }
    }
    ABlock *pop_avail()
    {
      ABlock *blk = avail_;
      if (NULL != blk) {
        remove_avail(blk);
      }
      return blk;
    }
    void remove_avail(ABlock *blk)
    {
      if (blk == blk->next_) {
        avail_ = NULL;
      } else {
        blk->prev_->next_ = blk->next_;
        blk->next_->prev_ = blk->prev_;
        if (blk == avail_) { avail_ = avail_->next_; }
      }
    }
    Lock lock_;
    AObject *local_free_;
    ABlock *avail_;
  };
#define B_SIZE 64
  static constexpr int BIN_SIZE_MAP[] = {
B_SIZE,
B_SIZE * 2,
B_SIZE * 3,
B_SIZE * 4,
B_SIZE * 5,
B_SIZE * 6,
B_SIZE * 7,
B_SIZE * 8,
B_SIZE * 9,
B_SIZE * 10,
B_SIZE * 11,
B_SIZE * 12,
B_SIZE * 13,
B_SIZE * 14,
B_SIZE * 15,
B_SIZE * 16,
2048, 4096, 8192
};
  SizeClass scs[sizeof(BIN_SIZE_MAP)/sizeof(BIN_SIZE_MAP[0])];

  IBlockMgr *blk_mgr_;
};


} // end of namespace lib
} // end of namespace oceanbase
#endif /* _OCEABASE_LIB_ALLOC_OBJECT_SET_H_ */
