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
#include "lib/alloc/object_mgr.h"
#include "lib/utility/ob_backtrace.h"
#include "lib/alloc/memory_sanity.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

void __attribute__((weak)) has_unfree_callback(char *info)
{
  _OB_LOG_RET(ERROR, OB_ERROR, "HAS UNFREE PTR!!! %s", info);
}

int ObjectSet::calc_sc_idx(const int64_t x)
{
  if (OB_LIKELY(x <= B_MID_SIZE)) {
    return (x-1)>>6;
  } else if (x <= B_MID_SIZE*16) {
    return ((x-1)>>11) + 16;
  } else {
    const int64_t up_align = (x + ABLOCK_SIZE) & ~(ABLOCK_SIZE -1);
    for (int idx = MIN_LARGE_IDX; idx <= MAX_LARGE_IDX; ++idx) {
      if (up_align == BIN_SIZE_MAP(idx)) {
        return idx;
      }
    }
    return OTHER_SC_IDX;
  }
}

void ObjectSet::do_cleanup()
{
  for (int i = 0; i < SIZE_CLASS_CNT; ++i) {
    // clean local_free
    AObject *obj = NULL;
    {
      SizeClass &sc = scs[i];
      sc.lock_.lock();
      DEFER(sc.lock_.unlock());
      obj = sc.local_free_;
      sc.local_free_ = NULL;
    }
    if (NULL != obj) {
      ABlock *block = obj->block();
      while (obj != NULL) {
        AObject *next = obj->next_;
        do_free_object(obj, block);
        obj = next;
      }
    }
  }
}

bool ObjectSet::check_has_unfree(ABlock* block, char *first_label, char *first_bt)
{
  bool has_unfree = false;
  AObject *obj = reinterpret_cast<AObject *>(block->data());
  int64_t cells_per_block = AllocHelper::cells_per_block(block->ablock_size_);
  while (true) {
    bool tmp_has_unfree = obj->in_use_;
    if (OB_UNLIKELY(tmp_has_unfree)) {
      if ('\0' == first_label[0]) {
        STRNCPY(first_label, obj->label_, AOBJECT_LABEL_SIZE);
        first_label[AOBJECT_LABEL_SIZE] = '\0';
      }
      if (obj->on_malloc_sample_ && '\0' == first_bt[0]) {
        void *addrs[AOBJECT_BACKTRACE_COUNT];
        MEMCPY((char*)addrs, obj->bt(), AOBJECT_BACKTRACE_SIZE);
        IGNORE_RETURN parray(first_bt, MAX_BACKTRACE_LENGTH, (int64_t*)addrs, AOBJECT_BACKTRACE_COUNT);
      }
      if (!has_unfree) {
        has_unfree = true;
      }
    }
    // It will jump out directly if one case is found is_large indicates that
    // the object occupies a block exclusively, and the effect is equivalent to is_last
    if (has_unfree || obj->is_large_ || obj->is_last(cells_per_block)) {
      break;
    }
    obj = obj->phy_next(obj->nobjs_);
  }
  return has_unfree;
}

bool ObjectSet::check_has_unfree(char *first_label, char *first_bt)
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  bool has_unfree = false;
  for (int i = 0; i < ARRAYSIZEOF(scs) && !has_unfree; ++i) {
    SizeClass &sc = scs[i];
    sc.lock_.lock();
    DEFER(sc.lock_.unlock());
    ABlock *block = sc.avail_blist_;
    if (NULL == block) {
      block = sc.full_blist_;
    }
    if (NULL != block) {
      has_unfree = true;
      check_has_unfree(block, first_label, first_bt);
    }
  }
  return has_unfree;
}


void ObjectSet::reset()
{
  do_cleanup();
  char first_label[AOBJECT_LABEL_SIZE + 1] = {'\0'};
  char first_bt[MAX_BACKTRACE_LENGTH] = {'\0'};
  if (check_has_unfree(first_label, first_bt)) {
    const static int buf_len = 512;
    char buf[buf_len] = {'\0'};
    snprintf(buf, buf_len, "label: %s, backtrace: %s", first_label, first_bt);
    has_unfree_callback(buf);
  }
  for (int i = 0; i < ARRAYSIZEOF(scs); ++i) {
    ABlock *block = NULL;
    while (OB_NOT_NULL(block = scs[i].pop_avail())) {
      free_block(block);
    }
    while (OB_NOT_NULL(block = scs[i].pop_full())) {
      free_block(block);
    }
  }
}

ObjectSet::~ObjectSet()
{
  reset();
}
AObject *ObjectSet::alloc_object(const uint64_t size, const ObMemAttr &attr)
{
  AObject *obj = NULL;
  const uint64_t all_size = size + AOBJECT_META_SIZE + attr.extra_size_;
  const int sc_idx = calc_sc_idx(all_size);
  if (OB_UNLIKELY(sc_idx == OTHER_SC_IDX)) {
    ABlock *block = alloc_block(all_size, attr);
    if (NULL != block) {
      obj = new (block->data()) AObject();
      obj->is_large_ = true;
      obj->block_ = block;
      SizeClass &sc = scs[OTHER_SC_IDX];
      sc.lock_.lock();
      DEFER(sc.lock_.unlock());
      sc.push_full(block);
    }
  } else {
    SizeClass &sc = scs[sc_idx];
    sc.lock_.lock();
    DEFER(sc.lock_.unlock());
    AObject *&local_free = sc.local_free_;
    if (OB_LIKELY(local_free != NULL)) {
l_local:
      obj = local_free;
      local_free = obj->next_;
    } else if (OB_LIKELY(sc.avail_blist_ != NULL)) {
      int64_t freelist_cnt = 0;
      ABlock *block = sc.pop_avail();
      AObject *freelist = block->freelist_.popall(&freelist_cnt);
      if (OB_LIKELY(freelist_cnt != block->max_cnt_)) {
        local_free = freelist;
        block->status_ = ABlock::FULL;
        sc.push_full(block);
        goto l_local;
      } else {
        block->freelist_.reset(freelist, freelist_cnt);
        sc.push_avail(block);
        goto l_new;
      }
    } else {
l_new:
      const int bin_size = BIN_SIZE_MAP(sc_idx);
      int64_t ablock_size = BLOCK_SIZE_MAP(sc_idx);
      ABlock *block = alloc_block(ablock_size, attr);
      if (OB_LIKELY(block != NULL)) {
        block->sc_idx_ = sc_idx;
        block->freelist_.reset();
        block->max_cnt_ = ablock_size/bin_size;
        char *data = block->data();
        int32_t cls = bin_size/AOBJECT_CELL_BYTES;
        AObject *freelist = NULL;
        AObject *cur = NULL;
        for (int i = 0; i < block->max_cnt_; i++) {
          cur = new (data + bin_size * i) AObject();
          cur->block_ = block;
          cur->nobjs_ = cls;
          cur->obj_offset_ = cls * i;
          cur->next_ = freelist;
          freelist = cur;
        }
        cur->nobjs_ = ablock_size/AOBJECT_CELL_BYTES - cur->obj_offset_;
        block->status_ = ABlock::FULL;
        sc.push_full(block);
        local_free = freelist;
        goto l_local;
      }
    }
  }
  if (obj) {
    reinterpret_cast<uint64_t&>(obj->data_[size]) = AOBJECT_TAIL_MAGIC_CODE;
    obj->alloc_bytes_ = size;
    obj->in_use_ = true;
    obj->set_label(attr.label_.str_);
  }
  return obj;
}

void ObjectSet::free_object(AObject *obj, ABlock *block)
{
  do_free_object(obj, block);
}

void ObjectSet::do_free_object(AObject *obj, ABlock *block)
{
  obj->in_use_ = false;
  obj->on_malloc_sample_ = false;
  if (OB_UNLIKELY(obj->is_large_)) {
    SizeClass &sc = scs[OTHER_SC_IDX];
    sc.lock_.lock();
    DEFER(sc.lock_.unlock());
    sc.remove_full(block);
    free_block(block);
  } else {
    int64_t max_cnt = block->max_cnt_;
    int64_t freelist_cnt = block->freelist_.push(obj);
    if (OB_LIKELY(1 != freelist_cnt && max_cnt != freelist_cnt)) {
      return;
    }
    const int sc_idx = block->sc_idx_;
    bool need_free = false;
    {
      SizeClass &sc = scs[sc_idx];
      sc.lock_.lock();
      DEFER(sc.lock_.unlock());
      if (1 == freelist_cnt) {
        if (ABlock::FULL == block->status_) {
          block->status_ = ABlock::PARTITIAL;
          sc.remove_full(block);
          sc.push_avail(block);
        } else if (ABlock::EMPTY == block->status_ ) {
          need_free = true;
        }
      } else if (max_cnt == freelist_cnt) {
        if (ABlock::FULL == block->status_) {
          block->status_ = ABlock::EMPTY;
          sc.remove_full(block);
        } else if (ABlock::PARTITIAL == block->status_) {
          block->status_ = ABlock::EMPTY;
          sc.remove_avail(block);
          need_free = true;
        }
      }
    }
    if (need_free) {
      free_block(block);
    }
  }
}

ABlock *ObjectSet::alloc_block(const uint64_t size, const ObMemAttr &attr)
{
  ABlock *block = blk_mgr_->alloc_block(size, attr);
  if (OB_LIKELY(block != NULL)) {
    block->obj_set_ = this;
    block->ablock_size_ = size;
  }
  return block;
}

void ObjectSet::free_block(ABlock *block)
{
  blk_mgr_->free_block(block);
}

AObject *ObjectSet::realloc_object(AObject *obj, const uint64_t size, const ObMemAttr &attr)
{
  AObject *new_obj = NULL;

  if (NULL == obj) {
    new_obj = alloc_object(size, attr);
  } else {
    abort_unless(obj->is_valid());
    uint64_t copy_size = MIN(obj->alloc_bytes_, size);
    new_obj = alloc_object(size, attr);
    if (NULL != new_obj) {
      if (copy_size != 0) {
        memmove(new_obj->data_, obj->data_, copy_size);
      }
      do_free_object(obj, obj->block());
    }
  }

  return new_obj;
}