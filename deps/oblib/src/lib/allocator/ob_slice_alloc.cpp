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

#include "ob_slice_alloc.h"

#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace common {

void ObBlockSlicer::print_leak_slice() {
  if (OB_ISNULL(slice_alloc_)) {
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid slice allocator", KP(this));
    return;
  }

  int32_t limit = slice_alloc_->get_bsize();
  int32_t slice_size = slice_alloc_->get_isize();
  int64_t isize = lib::align_up2((int32_t)sizeof(Item) + slice_size, 16);
  int64_t total = (limit - (int32_t)sizeof(*this)) / (isize + (int32_t)sizeof(void *));
  char *istart = (char *)lib::align_up2((uint64_t)((char *)(this + 1) + sizeof(void *) * total), 16);
  if (istart + isize * total > ((char *)this) + limit) {
    total--;
  }

  LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "", K(limit), K(slice_size), K(isize), K(total), KP(istart));

  for (int32_t i = 0; i < total; i++) {
    Item *item = (Item *)(istart + i * isize);
    void *slice = (void *)(item + 1);
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "", KP(item), KP(slice));
    if (flist_.is_in_queue(item)) {
      // this item has been freed
    } else {
      LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "leak info : ", KP(item), KP(slice));
    }
  }
}

void ObSliceAlloc::destroy()
{
  for (int i = MAX_ARENA_NUM - 1; i >= 0; i--) {
    Arena &arena = arena_[i];
    Block *old_blk = arena.clear();
    if (NULL != old_blk) {
      blk_ref_[ObBlockSlicer::hash((uint64_t)old_blk) % MAX_REF_NUM].sync();
      if (old_blk->release()) {
        blk_list_.add(&old_blk->dlink_);
      }
    }
  }
  ObDLink *dlink = nullptr;
  dlink = blk_list_.top();
  while (OB_NOT_NULL(dlink)) {
    Block *blk = CONTAINER_OF(dlink, Block, dlink_);
    if (blk->recycle()) {
      destroy_block(blk);
      dlink = blk_list_.top();
    } else {
      blk->print_leak_slice();
      _LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,
               "there was memory leak, stock=%d, total=%d, remain=%d",
               blk->stock(), blk->total(), blk->remain());
      dlink = nullptr;  // break
    }
  }
  tmallocator_ = NULL;
  bsize_ = 0;
}

} // namespace common
} // namespace oceanbase
