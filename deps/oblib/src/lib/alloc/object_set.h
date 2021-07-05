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

namespace oceanbase {
namespace common {
class ObAllocator;
}
namespace lib {
class MemoryContext;
class ObTenantCtxAllocator;
class IBlockMgr;
class ISetLocker;
class ObjectSet {
  friend class common::ObAllocator;
  static const uint32_t META_CELLS = (AOBJECT_META_SIZE - 1) / AOBJECT_CELL_BYTES + 1;
  static const uint32_t MIN_FREE_CELLS = META_CELLS + 1 + (15 / AOBJECT_CELL_BYTES);  // next, prev pointer
  static const double FREE_LISTS_BUILD_RATIO;
  static const double BLOCK_CACHE_RATIO;

  typedef AObject* FreeList;
  typedef ABitSet BitMap;

public:
  ObjectSet(MemoryContext* mem_context = nullptr, const uint32_t ablock_size = INTACT_NORMAL_AOBJECT_SIZE);
  ~ObjectSet();

  // main interfaces
  AObject* alloc_object(const uint64_t size, const ObMemAttr& attr);
  void free_object(AObject* obj);
  AObject* realloc_object(AObject* obj, const uint64_t size, const ObMemAttr& attr);
  void reset();

  // statistics
  uint64_t get_alloc_bytes() const;
  uint64_t get_hold_bytes() const;
  uint64_t get_allocs() const;

  void lock();
  void unlock();

  // statistics
  void set_block_mgr(IBlockMgr* blk_mgr)
  {
    blk_mgr_ = blk_mgr;
  }
  IBlockMgr* get_block_mgr()
  {
    return blk_mgr_;
  }
  void set_mod_set(common::ObLocalModSet* mod_set)
  {
    mod_set_ = mod_set;
  }
  void set_locker(ISetLocker* locker)
  {
    locker_ = locker;
  }
  inline int64_t get_normal_hold() const;
  inline int64_t get_normal_used() const;
  inline int64_t get_normal_alloc() const;

private:
  AObject* alloc_normal_object(const uint32_t cls, const ObMemAttr& attr);
  AObject* alloc_big_object(const uint64_t size, const ObMemAttr& attr);

  ABlock* alloc_block(const uint64_t size, const ObMemAttr& attr);
  void free_block(ABlock* block);

  AObject* get_free_object(const uint32_t cls);
  void add_free_object(AObject* obj);

  void free_big_object(AObject* obj);
  void take_off_free_object(AObject* obj);
  void free_normal_object(AObject* obj);

  bool build_free_lists();

  inline AObject* split_obj(AObject* obj, const uint32_t cls, AObject*& remainder);
  inline AObject* merge_obj(AObject* obj);

  void do_free_object(AObject* obj);
  void do_free_dirty_list();

private:
  MemoryContext* mem_context_;
  ISetLocker* locker_;
  common::ObLocalModSet* mod_set_;
  IBlockMgr* blk_mgr_;

  ABlock* blist_;

  AObject* last_remainder_;

  BitMap* bm_;
  FreeList* free_lists_;

  lib::ObMutex dirty_list_mutex_;
  AObject* dirty_list_;
  int64_t dirty_objs_;

  uint64_t alloc_bytes_;
  uint64_t used_bytes_;
  uint64_t hold_bytes_;
  uint64_t allocs_;

  uint64_t normal_alloc_bytes_;
  uint64_t normal_used_bytes_;
  uint64_t normal_hold_bytes_;

  uint32_t ablock_size_;
  uint32_t cells_per_block_;

  DISALLOW_COPY_AND_ASSIGN(ObjectSet);
} CACHE_ALIGNED;  // end of class ObjectSet

inline void ObjectSet::lock()
{
  locker_->lock();
}

inline void ObjectSet::unlock()
{
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

AObject* ObjectSet::split_obj(AObject* obj, const uint32_t cls, AObject*& remainder)
{
  AObject* new_obj = NULL;

  remainder = NULL;
  if (NULL == obj) {
  } else if (obj->nobjs_ < cls + META_CELLS) {
    new_obj = obj;
  } else {
    remainder = new (obj->phy_next(cls)) AObject();
    remainder->nobjs_prev_ = static_cast<uint16_t>(cls);
    remainder->nobjs_ = static_cast<uint16_t>(obj->nobjs_ - cls);
    remainder->obj_offset_ = static_cast<uint16_t>(obj->obj_offset_ + cls);
    obj->nobjs_ = static_cast<uint16_t>(cls);
    new_obj = obj;

    if (!remainder->is_last(cells_per_block_)) {
      AObject* next = remainder->phy_next(remainder->nobjs_);
      abort_unless(next->is_valid());
      next->nobjs_prev_ = remainder->nobjs_;
    }
  }
  return new_obj;
}

AObject* ObjectSet::merge_obj(AObject* obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->is_valid());

  AObject* prev_obj = NULL;
  AObject* next_obj = NULL;
  AObject* next_next_obj = NULL;

  if (0 != obj->obj_offset_) {
    prev_obj = obj->phy_next(-obj->nobjs_prev_);
    abort_unless(prev_obj->is_valid());
    if (prev_obj == last_remainder_) {
      prev_obj = NULL;
    } else if (!prev_obj->in_use_) {
      take_off_free_object(prev_obj);
    }
  }

  if (!obj->is_last(cells_per_block_)) {
    next_obj = obj->phy_next(obj->nobjs_);
    abort_unless(next_obj->is_valid());
    if (next_obj != last_remainder_ && !next_obj->in_use_) {
      take_off_free_object(next_obj);
    }
  }

  if (NULL != next_obj && next_obj != last_remainder_ && !next_obj->in_use_) {
    if (!next_obj->is_last(cells_per_block_)) {
      next_next_obj = next_obj->phy_next(next_obj->nobjs_);
      abort_unless(next_next_obj->is_valid());
    }
  }

  AObject* head = NULL != prev_obj && !prev_obj->in_use_ ? prev_obj : obj;
  AObject* tail = next_obj != NULL && next_obj != last_remainder_ && !next_obj->in_use_ ? next_next_obj : next_obj;

  if (NULL != tail) {
    head->nobjs_ = static_cast<uint16_t>(tail->obj_offset_ - head->obj_offset_);
    tail->nobjs_prev_ = head->nobjs_;
  } else {
    head->nobjs_ = static_cast<uint16_t>(cells_per_block_ - head->obj_offset_);
  }

  return head;
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

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_OBJECT_SET_H_ */
