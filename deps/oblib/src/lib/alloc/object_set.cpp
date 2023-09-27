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

#include "object_set.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/alloc/alloc_failed_reason.h"
#include "lib/ob_abort.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/rc/context.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

const double ObjectSet::FREE_LISTS_BUILD_RATIO = 0.0;
const double ObjectSet::BLOCK_CACHE_RATIO = 0.0;

const static int BT_BUF_LEN = 256;

void __attribute__((weak)) has_unfree_callback(char *info)
{
  _OB_LOG_RET(ERROR, OB_ERROR, "HAS UNFREE PTR!!! %s", info);
}

ObjectSet::ObjectSet(__MemoryContext__ *mem_context, const uint32_t ablock_size,
  const bool enable_dirty_list)
  : mem_context_(mem_context), locker_(nullptr),
    blk_mgr_(nullptr), blist_(NULL), last_remainder_(NULL),
    bm_(NULL), free_lists_(NULL),
    dirty_list_mutex_(common::ObLatchIds::ALLOC_OBJECT_LOCK), dirty_list_(nullptr), dirty_objs_(0),
    alloc_bytes_(0), used_bytes_(0), hold_bytes_(0), allocs_(0),
    normal_alloc_bytes_(0), normal_used_bytes_(0),
    normal_hold_bytes_(0), ablock_size_(ablock_size), enable_dirty_list_(enable_dirty_list),
    cells_per_block_(AllocHelper::cells_per_block(ablock_size))
{}

ObjectSet::~ObjectSet()
{
  reset();
}

AObject *ObjectSet::alloc_object(
    const uint64_t size, const ObMemAttr &attr)
{
  const uint64_t adj_size = MAX(size, MIN_AOBJECT_SIZE);
  const uint64_t all_size = align_up2(adj_size + AOBJECT_META_SIZE, 16);

  const int64_t ctx_id = blk_mgr_->get_ctx_id();
  abort_unless(ctx_id == attr.ctx_id_);
  if (OB_UNLIKELY(enable_dirty_list_)) {
    do_free_dirty_list();
  }

  AObject *obj = NULL;
  if (OB_UNLIKELY(size >= UINT32_MAX) || OB_UNLIKELY(0 == size)) {
    // not support
    auto &afc = g_alloc_failed_ctx();
    afc.reason_ = SINGLE_ALLOC_SIZE_OVERFLOW;
    afc.alloc_size_ = size;
  } else if (all_size <= ablock_size_) {
    const uint32_t cls = (uint32_t)(1+ ((all_size - 1) / AOBJECT_CELL_BYTES));
    obj = alloc_normal_object(cls, attr);
    if (NULL != obj) {
      normal_alloc_bytes_ += size;
      normal_used_bytes_ += obj->nobjs_ * AOBJECT_CELL_BYTES;
    }
  } else {
    obj = alloc_big_object(adj_size, attr);
    abort_unless(NULL == obj || obj->in_use_);
  }

  if (NULL != obj) {
    abort_unless(obj->in_use_);
    abort_unless(obj->is_valid());

    reinterpret_cast<uint64_t&>(obj->data_[size]) = AOBJECT_TAIL_MAGIC_CODE;
    obj->alloc_bytes_ = static_cast<uint32_t>(size);

    if (attr.label_.str_ != nullptr) {
      STRNCPY(&obj->label_[0], attr.label_.str_, sizeof(obj->label_));
      obj->label_[sizeof(obj->label_) - 1] = '\0';
    } else {
      MEMSET(obj->label_, '\0', sizeof(obj->label_));
    }
    allocs_++;
    alloc_bytes_ += size;
    used_bytes_ += obj->hold(cells_per_block_);
  }

  return obj;
}

AObject *ObjectSet::realloc_object(
    AObject *obj, const uint64_t size, const ObMemAttr &attr)
{
  AObject *new_obj = NULL;
  uint64_t copy_size = 0;

  if (NULL == obj) {
    new_obj = alloc_object(size, attr);
  } else {
    abort_unless(obj->is_valid());
    if (obj->is_large_ != 0) {
      copy_size = MIN(obj->alloc_bytes_, size);
    } else {
      copy_size = MIN(
          size, (obj->nobjs_ - META_CELLS) * AOBJECT_CELL_BYTES);
    }

    new_obj = alloc_object(size, attr);
    if (NULL != new_obj && copy_size != 0) {
      memmove(new_obj->data_, obj->data_, copy_size);
    }

    do_free_object(obj);
  }

  return new_obj;
}

AObject *ObjectSet::alloc_normal_object(const uint32_t cls, const ObMemAttr &attr)
{
  AObject *obj = NULL;

  // best fit
  if (NULL != bm_
      && NULL != free_lists_
      && bm_->isset(cls)) {
    obj = free_lists_[cls];
    take_off_free_object(obj);
    obj->in_use_ = true;
  }

  // last remainder
  if (NULL == obj && NULL != last_remainder_) {
    if (cls <= last_remainder_->nobjs_) {
      obj = split_obj(last_remainder_, cls, last_remainder_);
      obj->in_use_ = true;
    }
  }

  // next first fit
  if (NULL == obj && NULL != bm_ && NULL != free_lists_) {
    obj = get_free_object(cls);
    if (obj != NULL) {
      obj->in_use_ = true;
    }
  }

  // alloc new block, split into two part:
  //
  //   1. with cls cells that return to user
  //   2. the remainder as last remainder
  //
  if (NULL == obj) {
    if (NULL != bm_
        && NULL != free_lists_
        && NULL != last_remainder_) {
      AObject * const lrback = last_remainder_;
      last_remainder_ = NULL;
      add_free_object(merge_obj(lrback));
    }

    ABlock *block = alloc_block(ablock_size_, attr);
    if (NULL != block) {
      normal_hold_bytes_ += ablock_size_;

      AObject *remainder = NULL;
      obj = new (block->data()) AObject();
      obj->nobjs_ = static_cast<uint16_t>(cells_per_block_);
      obj = split_obj(obj, cls, remainder);
      obj->in_use_ = true;
      if (remainder != NULL) {
        last_remainder_ = remainder;
      }
    }
  }

  return obj;
}

AObject *ObjectSet::get_free_object(const uint32_t cls)
{
  AObject *obj = NULL;

  if (NULL != bm_ && NULL != free_lists_) {
    const int ffs = bm_->find_first_significant(cls);
    if (ffs >= 0) {
      if (free_lists_[ffs] != NULL) {
        obj = free_lists_[ffs];
      }
    }

    if (ffs >= 0 && NULL != obj && ffs >= static_cast<int32_t>(cls)) {
      if (NULL != last_remainder_
          && obj->block() == last_remainder_->block()
          && (obj->phy_next(obj->nobjs_) == last_remainder_
              || last_remainder_->phy_next(last_remainder_->nobjs_) == obj)) {
        last_remainder_ = merge_obj(last_remainder_);
      } else {
        if (NULL != last_remainder_) {
          last_remainder_ = merge_obj(last_remainder_);
          add_free_object(last_remainder_);
          last_remainder_ = NULL;
        }
        take_off_free_object(obj);
        last_remainder_ = obj;
      }
      obj = split_obj(last_remainder_, cls, last_remainder_);
    }
  }

  if (NULL != obj) {
    abort_unless(obj->is_valid());
  }

  return obj;
}

void ObjectSet::add_free_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->is_valid());

  if (OB_ISNULL(bm_) || OB_ISNULL(free_lists_)) {
  } else if (obj->nobjs_ >= MIN_FREE_CELLS) {
    AObject *&head = free_lists_[obj->nobjs_];
    if (bm_->isset(obj->nobjs_)) {
      obj->prev_ = head->prev_;
      obj->next_ = head;
      obj->prev_->next_ = obj;
      obj->next_->prev_ = obj;
    } else {
      bm_->set(obj->nobjs_);
      obj->prev_ = obj;
      obj->next_ = obj;
      head = obj;
    }
  }
}

void ObjectSet::take_off_free_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->is_valid());

  if (OB_ISNULL(bm_) || OB_ISNULL(free_lists_)) {
  } else if (!!obj->in_use_) {
  } else if (obj->nobjs_ < MIN_FREE_CELLS) {
  } else if (obj->next_ == obj) {
    bm_->unset(obj->nobjs_);
  } else {
    AObject *&head = free_lists_[obj->nobjs_];
    if (head == obj) {
      head = head->next_;
    }
    obj->prev_->next_ = obj->next_;
    obj->next_->prev_ = obj->prev_;
  }
}

void ObjectSet::free_normal_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->is_valid());

  normal_alloc_bytes_ -= obj->alloc_bytes_;
  normal_used_bytes_ -= obj->nobjs_ * AOBJECT_CELL_BYTES;

  AObject *newobj = merge_obj(obj);
  auto ctx_id = blk_mgr_->get_ctx_id();
  auto tenant_id = blk_mgr_->get_tenant_id();
  if (newobj->nobjs_ == cells_per_block_) {
    normal_hold_bytes_ -= ablock_size_;
    free_block(newobj->block());
  } else if (OB_ISNULL(last_remainder_)) {
    last_remainder_ = newobj;
  } else {
    add_free_object(newobj);
  }
}

ABlock *ObjectSet::alloc_block(const uint64_t size, const ObMemAttr &attr)
{
  ABlock *block = blk_mgr_->alloc_block(size, attr);

  if (NULL != block) {
    hold_bytes_ += block->hold();
    if (NULL != blist_) {
      block->prev_ = blist_->prev_;
      block->next_ = blist_;
      blist_->prev_->next_ = block;
      blist_->prev_ = block;
    } else {
      block->prev_ = block->next_ = block;
      blist_ = block;
    }
    block->obj_set_ = this;
    block->ablock_size_ = ablock_size_;
    block->mem_context_ = reinterpret_cast<int64_t>(mem_context_);
  }

  return block;
}

void ObjectSet::free_block(ABlock *block)
{
  abort_unless(NULL != block);
  abort_unless(block->is_valid());

  if (block == blist_) {
    blist_ = blist_->next_;
    if (block == blist_) {
      blist_ = NULL;
    }
  }

  block->prev_->next_ = block->next_;
  block->next_->prev_ = block->prev_;

  hold_bytes_ -= block->hold();
  // The pbmgr shouldn't be NULL or there'll be memory leak.
  blk_mgr_->free_block(block);
}

AObject *ObjectSet::alloc_big_object(const uint64_t size, const ObMemAttr &attr)
{
  AObject *obj = NULL;
  ABlock *block = alloc_block(size + AOBJECT_META_SIZE, attr);

  if (NULL != block) {
    obj = new (block->data()) AObject();
    obj->is_large_ = true;
    obj->in_use_ = true;
    obj->alloc_bytes_ = static_cast<uint32_t>(size);
  }

  return obj;
}

void ObjectSet::free_big_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(NULL != obj->block());
  abort_unless(obj->is_valid());

  free_block(obj->block());
}

void ObjectSet::free_object(AObject *obj)
{
  abort_unless(obj != NULL);
  abort_unless(obj->is_valid());
  abort_unless(
      AOBJECT_TAIL_MAGIC_CODE
      == reinterpret_cast<uint64_t&>(obj->data_[obj->alloc_bytes_]));
  abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE ||
               obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
  abort_unless(obj->in_use_);

#ifdef ERRSIM
  if (0 != (OB_E(EventTable::EN_RESET_FREE_MEMORY) 0)) {
    memset(obj->data_, 0xAA, obj->alloc_bytes_);
  }
#endif
  const int64_t ctx_id = blk_mgr_->get_ctx_id();
  ObDisableDiagnoseGuard diagnose_disable_guard;
  if (OB_UNLIKELY(enable_dirty_list_)) {
    if (locker_->trylock()) {
      do_free_object(obj);
      do_free_dirty_list();
      locker_->unlock();
    } else {
      dirty_list_mutex_.lock();
      if (dirty_objs_ < 1000) {
        dirty_objs_++;
        if (OB_ISNULL(dirty_list_)) {
          obj->next_ = nullptr;
        } else {
          obj->next_ = dirty_list_;
        }
        dirty_list_ = obj;
        dirty_list_mutex_.unlock();
      } else {
        dirty_list_mutex_.unlock();
        locker_->lock();
        do_free_object(obj);
        do_free_dirty_list();
        locker_->unlock();
      }
    }
  } else {
    locker_->lock();
    do_free_object(obj);
    locker_->unlock();
  }
}

void ObjectSet::do_free_object(AObject *obj)
{
  const int64_t hold = obj->hold(cells_per_block_);
  const int64_t used = obj->alloc_bytes_;

  alloc_bytes_ -= obj->alloc_bytes_;
  used_bytes_ -= hold;

  obj->in_use_ = false;
  obj->on_malloc_sample_ = false;
  if (!obj->is_large_) {
    free_normal_object(obj);
  } else {
    free_big_object(obj);
  }

  // 1. havn't created free lists
  // 2. touch free lists build ratio
  if (OB_ISNULL(bm_) && OB_ISNULL(free_lists_)) {
    if (normal_hold_bytes_ > ablock_size_
        && (normal_used_bytes_
          < static_cast<double>(normal_hold_bytes_)
          * (1. - FREE_LISTS_BUILD_RATIO))) {
      build_free_lists();
      last_remainder_ = NULL;
    }
  } else if (OB_NOT_NULL(blist_) && blist_->next_ == blist_) {
    abort_unless(0 == alloc_bytes_);
    abort_unless(0 == used_bytes_);
    if (free_lists_ != nullptr) {
      ABlock *free_list_block = nullptr;
      AChunk *chunk = AChunk::ptr2chunk(free_lists_);
      abort_unless(chunk->is_valid());
      free_list_block = chunk->ptr2blk(free_lists_);
      free_block(free_list_block);
      free_lists_ = nullptr;
    }
    bm_ = nullptr;
  }
}

void ObjectSet::do_free_dirty_list()
{
  if (OB_NOT_NULL(dirty_list_)) {
    AObject *list = nullptr;
    {
      ObMutexGuard g(dirty_list_mutex_);
      list = dirty_list_;
      dirty_list_ = nullptr;
      dirty_objs_ = 0;
    }
    while (OB_NOT_NULL(list)) {
      AObject *obj = list;
      list = list->next_;
      do_free_object(obj);
    }
  }
}

bool ObjectSet::check_has_unfree(char *first_label)
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  bool has_unfree = false;

  if (blist_ != NULL) {
    ABlock *free_list_block = nullptr;
    if (free_lists_ != nullptr) {
      AChunk *chunk = AChunk::ptr2chunk(free_lists_);
      abort_unless(chunk->is_valid());
      free_list_block = chunk->ptr2blk(free_lists_);
    }
    ABlock *block = blist_;
    // Filter the block to which the freelist itself belongs
    // Check whether the objects of the block are all! in_use (object_set may cache an 8k block)
    do {
      if (block != free_list_block) {
        AObject *obj = reinterpret_cast<AObject *>(block->data());
        while (true) {
          bool tmp_has_unfree = obj->in_use_;
          if (OB_UNLIKELY(tmp_has_unfree)) {
            if ('\0' == first_label[0]) {
              STRCPY(first_label, obj->label_);
            }
            if (!has_unfree) {
              has_unfree = true;
            }
          }
          // It will jump out directly if one case is found is_large indicates that
          // the object occupies a block exclusively, and the effect is equivalent to is_last
          if (has_unfree || obj->is_large_ || obj->is_last(cells_per_block_)) {
            break;
          }
          obj = obj->phy_next(obj->nobjs_);
        }
        if (has_unfree) {
          break;
        }
      }
      block = block->next_;
    } while (block != blist_);
  }

  return has_unfree;
}

void ObjectSet::reset()
{
  const bool context_check = mem_context_ != nullptr;
  const static int buf_len = 256;
  char buf[buf_len] = {'\0'};
  char first_label[AOBJECT_LABEL_SIZE + 1] = {'\0'};
  bool has_unfree = check_has_unfree(first_label);
  if (has_unfree) {
    if (context_check) {
      const StaticInfo &static_info = mem_context_->get_static_info();
      const DynamicInfo &dynamic_info = mem_context_->get_dynamic_info();
      int64_t pos = snprintf(buf, buf_len,
                             "context: %p, label: %s, static_id: 0x%lx, "
                             "static_info:{filename: %s, line: %d, function: %s}, "
                             "dynamic_info:{tid: %ld, cid: %ld, create_time: %ld}",
                             mem_context_, first_label,
                             mem_context_->get_static_id(),
                             static_info.filename_, static_info.line_, static_info.function_,
                             dynamic_info.tid_, dynamic_info.cid_, dynamic_info.create_time_);
      buf[pos] = '\0';
    }
    has_unfree_callback(buf);
  }

  while (NULL != blist_) {
    free_block(blist_);
  }

  bm_ = NULL;
  free_lists_ = NULL;
  last_remainder_ = NULL;

  alloc_bytes_ = 0;
  used_bytes_ = 0;
  allocs_ = 0;

  normal_alloc_bytes_ = 0;
  normal_used_bytes_ = 0;
  normal_hold_bytes_ = 0;
  hold_bytes_ = 0;
}

bool ObjectSet::build_free_lists()
{
  abort_unless(NULL == bm_ && NULL == free_lists_);
  ObMemAttr attr;
  attr.tenant_id_ = blk_mgr_->get_tenant_id();
  attr.ctx_id_ = blk_mgr_->get_ctx_id();
  attr.label_ = common::ObModIds::OB_OBJ_FREELISTS;
  ABlock *new_block = alloc_block(sizeof (FreeList) * (cells_per_block_ + 1) +
      sizeof (BitMap) + BitMap::buf_len(cells_per_block_ + 1), attr);

  OB_LOG(DEBUG, "build free lists", KCSTRING(common::lbt()));

  if (NULL != new_block) {
    //new_block->label_ = common::ObModIds::OB_OBJ_FREELISTS;
    char *new_block_data = new_block->data();
    free_lists_ = new (new_block_data) FreeList();
    bm_ = new (&new_block_data[sizeof (FreeList) * (cells_per_block_ + 1)])
      BitMap(cells_per_block_ + 1,
             &new_block_data[sizeof (FreeList) * (cells_per_block_ + 1) + sizeof (BitMap)]);

    // the new block is at tail of blist_, the BREAK and CONTINUE is
    // necessary otherwise the code will be very ugly and bug prone.
    for (ABlock *block = blist_;
         block != new_block;
         block = block->next_) {
      AObject *obj = reinterpret_cast<AObject*>(block->data());
      abort_unless(obj->is_valid());

      // ignore large object
      if (!obj->is_large_) {
        for (;;) {
          while (obj->in_use_ && !obj->is_last(cells_per_block_)) {
            AObject *next_obj = obj->phy_next(obj->nobjs_);
            next_obj->nobjs_prev_ = obj->nobjs_;
            obj = next_obj;
            abort_unless(obj->is_valid());
          }
          if (obj->is_last(cells_per_block_)) {
            obj->nobjs_ = static_cast<uint16_t>(cells_per_block_ - obj->obj_offset_);
            if (!obj->in_use_) {
              add_free_object(obj);
            }
            break;
          }
          // the first object not in use
          AObject *first = obj;
          obj = obj->phy_next(obj->nobjs_);
          abort_unless(obj->is_valid());
          while (!obj->in_use_ && !obj->is_last(cells_per_block_)) {
            obj = obj->phy_next(obj->nobjs_);
            abort_unless(obj->is_valid());
          }
          if (obj->is_last(cells_per_block_) && !obj->in_use_) {
            first->nobjs_ = static_cast<uint16_t>(cells_per_block_ - first->obj_offset_);
            add_free_object(first);
            break;
          } else {
            first->nobjs_ = static_cast<uint16_t>(obj->obj_offset_ - first->obj_offset_);
            obj->nobjs_prev_ = first->nobjs_;
            add_free_object(first);
          }
        } // for one block
      } // if (!obj->is_large_)
    } // for all block
  }

  return new_block != NULL;
}

AObject *ObjectSet::split_obj(AObject *obj, const uint32_t cls, AObject *&remainder)
{
  AObject *new_obj = NULL;

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
      AObject *next = remainder->phy_next(remainder->nobjs_);
      abort_unless(next->is_valid());
      next->nobjs_prev_ = remainder->nobjs_;
    }
  }
  return new_obj;
}

AObject *ObjectSet::merge_obj(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->is_valid());

  AObject *prev_obj = NULL;
  AObject *next_obj = NULL;
  AObject *next_next_obj = NULL;

  if (0 != obj->obj_offset_) {
    prev_obj = obj->phy_next(-obj->nobjs_prev_);
    abort_unless(prev_obj->is_valid());
    if (prev_obj == last_remainder_) {
      last_remainder_ = nullptr;
    } else if (!prev_obj->in_use_) {
      take_off_free_object(prev_obj);
    }
  }

  if (!obj->is_last(cells_per_block_)) {
    next_obj = obj->phy_next(obj->nobjs_);
    abort_unless(next_obj->is_valid());
    if (next_obj == last_remainder_) {
      last_remainder_ = nullptr;
    } else if (!next_obj->in_use_) {
      take_off_free_object(next_obj);
    }
  }

  if (NULL != next_obj && !next_obj->in_use_) {
    if (!next_obj->is_last(cells_per_block_)) {
      next_next_obj = next_obj->phy_next(next_obj->nobjs_);
      abort_unless(next_next_obj->is_valid());
    }
  }

  AObject *head = NULL != prev_obj && !prev_obj->in_use_ ? prev_obj : obj;
  AObject *tail = next_obj != NULL && !next_obj->in_use_ ? next_next_obj : next_obj;

  if (NULL != tail) {
    head->nobjs_ = static_cast<uint16_t>(tail->obj_offset_ - head->obj_offset_);
    tail->nobjs_prev_ = head->nobjs_;
  } else {
    head->nobjs_ = static_cast<uint16_t>(cells_per_block_ - head->obj_offset_);
  }

  return head;
}
