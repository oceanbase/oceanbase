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

#include "lib/alloc/alloc_assist.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/resource/achunk_mgr.h"
#include "lib/ob_define.h"

using namespace oceanbase::lib;

#if MEMCHK_LEVEL >= 1

static const int64_t CHECK_RETRY_COUNT = 3;

static const ABlock* ptr2block(const void* ptr)
{
  const ABlock* block = NULL;
  const AChunk* const chunk = CHUNK_MGR.ptr2chunk(ptr);

  if (NULL != chunk) {
    abort_unless(chunk->MAGIC_CODE_ == ACHUNK_MAGIC_CODE);

    const uint64_t block_offset = ((uint64_t)ptr - (uint64_t)chunk->data_) % INTACT_ABLOCK_SIZE;

    block = (ABlock*)((uint64_t)ptr - block_offset);
    while ((char*)block > (char*)chunk && block->check_magic_code()) {
      block = block->phy_next(-1);
    }

    if ((char*)block <= (char*)chunk) {
      abort_unless(false);
      block = NULL;
    }
  }

  return block;
}

void check_ptr(const void* ptr, const int64_t len)
{
  const ABlock* const block = ptr2block(ptr);

  if (NULL != block) {
    const AObject* obj = (AObject*)block->data_;
    const uint64_t magic_code = obj->MAGIC_CODE_;
    if (AOBJECT_MAGIC_CODE == magic_code || FREE_AOBJECT_MAGIC_CODE == magic_code) {
      for (int i = 0; NULL == obj && i < CHECK_RETRY_COUNT; ++i) {
        obj = (AObject*)block->data_;
        int32_t nobjs = obj->nobjs_;
        uint32_t next_offset = nobjs + obj->obj_offset_;

        while (NULL != obj && next_offset * AOBJECT_CELL_BYTES < (char*)ptr - (char*)block->data_) {
          // if this object meta is invalid, we'll re-traverse
          // current block.
          if ((obj->MAGIC_CODE_ != AOBJECT_MAGIC_CODE && obj->MAGIC_CODE_ != FREE_AOBJECT_MAGIC_CODE) ||
              next_offset >= CELLS_PER_BLOCK) {
            obj = NULL;
          } else {
            obj = obj->phy_next(nobjs);
            nobjs = obj->nobjs_;
            next_offset = nobjs + obj->obj_offset_;
          }
        }

        // do the check after we've found out the correct object.
        if (NULL != obj) {
          abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE);
          abort_unless(obj->data_ <= ptr);
          abort_unless((char*)ptr + len <= obj->data_ + obj->alloc_bytes_);
        }
      }
    } else if (magic_code == BIG_AOBJECT_MAGIC_CODE) {
      abort_unless(obj->data_ <= ptr);
      abort_unless((char*)ptr + len <= obj->data_ + obj->alloc_bytes_);
    } else {
      abort_unless(false);
    }
  }
}

#endif
