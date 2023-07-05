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

#include "block_set.h"

#include <sys/mman.h>
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/ob_define.h"
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/alloc/ob_malloc_callback.h"

using namespace oceanbase;
using namespace oceanbase::lib;

void BlockSet::Lock::lock()
{
  int64_t tid = common::get_itid() + 1;
  while (!ATOMIC_BCAS(&tid_, 0, tid)) {
    sched_yield();
  }
}

BlockSet::BlockSet()
    : mutex_(common::ObLatchIds::ALLOC_BLOCK_LOCK), clist_(NULL),
      avail_bm_(BLOCKS_PER_CHUNK+1, avail_bm_buf_),
      total_hold_(0), total_payload_(0), total_used_(0), tallocator_(NULL),
      chunk_free_list_(false/*with_mutex*/), locker_(nullptr)
{
  chunk_free_list_.set_max_chunk_cache_size(0);
}

BlockSet::~BlockSet()
{
  reset();
}

bool BlockSet::check_has_unfree()
{
  return clist_ != NULL;
}

void BlockSet::reset()
{
  while (NULL != clist_) {
    free_chunk(clist_);
  }
  //MEMSET(block_list_, 0, sizeof(block_list_));
  clist_ = nullptr;
  avail_bm_.clear();
  LockGuard lock(cache_shared_lock_);
  for (AChunk *chunk = nullptr; (chunk = chunk_free_list_.pop()) != nullptr;) {
    uint64_t payload = 0;
    UNUSED(ATOMIC_FAA(&total_hold_, -chunk->hold(&payload)));
    UNUSED(ATOMIC_FAA(&total_payload_, -payload));
    if (chunk->washed_size_ != 0) {
      tallocator_->update_wash_stat(-1, -chunk->washed_blks_, -chunk->washed_size_);
    }
    tallocator_->free_chunk(chunk, attr_);
  }
  cache_shared_lock_.reset();
}

void BlockSet::set_tenant_ctx_allocator(ObTenantCtxAllocator &allocator)
{
  if (&allocator != tallocator_) {
    reset();
    tallocator_ = &allocator;
    attr_ = ObMemAttr(allocator.get_tenant_id(), nullptr, allocator.get_ctx_id());
  }
}

ABlock *BlockSet::alloc_block(const uint64_t size, const ObMemAttr &attr)
{
  const uint64_t alloc_size = size;
  const uint64_t all_size   = alloc_size;
  const uint32_t cls        = (uint32_t)(1 + (all_size - 1) / ABLOCK_SIZE);
  ABlock *block             = NULL;

  if (size >= UINT32_MAX) {
    // not support
    auto &afc = g_alloc_failed_ctx();
    afc.reason_ = SINGLE_ALLOC_SIZE_OVERFLOW;
    afc.alloc_size_ = size;
  } else if (0 == size) {
    // size is zero
  } else if (cls <= BLOCKS_PER_CHUNK) {
    // can fit in a chunk
    block = get_free_block(cls, attr);
  } else {
    AChunk *chunk = alloc_chunk(all_size, attr);
    if (chunk) {
      block = new (chunk->data_) ABlock();
      block->in_use_ = true;
      block->is_large_ = true;
      chunk->mark_blk_offset_bit(chunk->blk_offset(block));
    }
  }

  if (OB_NOT_NULL(block)) {
    block->alloc_bytes_ = size;
    uint64_t payload = 0;
    block->hold(&payload);
    UNUSED(ATOMIC_FAA(&total_used_, payload));
    if (OB_NOT_NULL(malloc_callback)) {
      (*malloc_callback)(attr, size);
      for (auto *p = malloc_callback->next(); p != malloc_callback; p = p->next()) {
        (*p)(attr, size);
      }
    }
  }

  return block;
}

void BlockSet::free_block(ABlock *const block)
{
  if (NULL == block) {
    // nothing
  } else {
    abort_unless(block->is_valid());
    uint64_t payload = 0;
    block->hold(&payload);
    UNUSED(ATOMIC_FAA(&total_used_, -payload));
    AChunk *chunk = block->chunk();
    abort_unless(chunk->is_valid());
    if (!!block->is_large_) {
      free_chunk(chunk);
    } else {
      if (OB_NOT_NULL(malloc_callback)) {
        (*malloc_callback)(attr_, -block->alloc_bytes_);
        for (auto *p = malloc_callback->next(); p != malloc_callback; p = p->next()) {
          (*p)(attr_, -block->alloc_bytes_);
        }
      }
      ABlock *prev_block = NULL;
      ABlock *next_block = NULL;

      int offset = chunk->blk_offset(block);
      int prev_offset = -1;
      if (!chunk->is_first_blk_offset(offset, &prev_offset)) {
        prev_block = chunk->offset2blk(prev_offset);
        if (!prev_block->in_use_ && !prev_block->is_washed_) {
          take_off_free_block(prev_block, offset - prev_offset, chunk);
          block->clear_magic_code();
          chunk->unmark_blk_offset_bit(offset);
          chunk->unmark_unused_blk_offset_bit(prev_offset);
        }
      }

      int next_offset = -1;
      if (!chunk->is_last_blk_offset(offset, &next_offset)) {
        next_block = chunk->offset2blk(next_offset);
        if (!next_block->in_use_ && !next_block->is_washed_) {
          take_off_free_block(next_block, chunk->blk_nblocks(next_block), chunk);
          next_block->clear_magic_code();
          chunk->unmark_blk_offset_bit(next_offset);
          chunk->unmark_unused_blk_offset_bit(next_offset);
        }
      }

      ABlock *head = NULL != prev_block && !prev_block->in_use_ && !prev_block->is_washed_ ? prev_block : block;

      // head won't been NULL,
      if (head != NULL) {
        head->in_use_ = false;
        // copy a temp
        chunk->mark_unused_blk_offset_bit(chunk->blk_offset(head));
        if (chunk->is_all_blks_unused()) {
          if (0 != chunk->washed_size_) {
            int offset = 0;
            do {
              ABlock *unused_block = chunk->offset2blk(offset);
              int next_offset = -1;
              bool is_last = chunk->is_last_blk_offset(offset, &next_offset);
              abort_unless(!unused_block->in_use_);
              // don't allow to take off head twice
              if (!unused_block->is_washed_ && head != unused_block) {
                take_off_free_block(unused_block, chunk->blk_nblocks(unused_block), chunk);
              }
              if (is_last) break;
              offset = next_offset;
            } while (true);
          }
          free_chunk(chunk);
        } else {
          int head_nblocks = chunk->blk_nblocks(head);
          add_free_block(head, head_nblocks, chunk);
        }
      }
    }
  }
}

void BlockSet::add_free_block(ABlock *block, int nblocks, AChunk *chunk)
{
  abort_unless(NULL != block && !block->in_use_ && !block->is_washed_);
  int offset = chunk->blk_offset(block);
  chunk->mark_blk_offset_bit(offset);
  chunk->mark_unused_blk_offset_bit(offset);

#if MEMCHK_LEVEL >= 1
  int expect_nblocks = chunk->blk_nblocks(block);
  abort_unless(nblocks == expect_nblocks);
#endif
  ABlock *&blist = block_list_[nblocks];
  if (avail_bm_.isset(nblocks)) {
    block->prev_ = blist->prev_;
    block->next_ = blist;
    block->prev_->next_ = block;
    block->next_->prev_ = block;
  } else {
    block->prev_ = block->next_ = block;
    blist = block;
    avail_bm_.set(nblocks);
  }
}

ABlock* BlockSet::get_free_block(const int cls, const ObMemAttr &attr)
{
  ABlock *block = NULL;

  const int ffs = avail_bm_.find_first_significant(cls);
  if (ffs >= 0) {
    if (NULL != block_list_[ffs]) {  // exist
      block = block_list_[ffs];
      if (block->next_ != block) {  // not the only one
        block->prev_->next_ = block->next_;
        block->next_->prev_ = block->prev_;
        block_list_[ffs] = block->next_;
      } else {
        avail_bm_.unset(ffs);
        block_list_[ffs] = NULL;
      }
      block->in_use_ = true;
    }
  }

  // put back into another block list if need be.
  if (NULL != block && ffs > cls) {
    AChunk *chunk = block->chunk();
    // contruct a new block at right position
    int offset = chunk->blk_offset(block);
    ABlock *next_block = new (chunk->offset2blk(offset + cls)) ABlock();

    add_free_block(next_block, ffs - cls, chunk);
  }

  if (block == NULL && ffs < 0) {
    if (add_chunk(attr)) {
      block = get_free_block(cls, attr);
    }
  }

  if (NULL != block) {
    AChunk *chunk = block->chunk();
    chunk->unmark_unused_blk_offset_bit(chunk->blk_offset(block));
  }

  return block;
}

void BlockSet::take_off_free_block(ABlock *block, int nblocks, AChunk *chunk)
{
  abort_unless(NULL != block && !block->in_use_);

#if MEMCHK_LEVEL >= 1
  int expect_nblocks = chunk->blk_nblocks(block);
  abort_unless(nblocks == expect_nblocks);
#endif
  if (block->next_ != block) {
    block->next_->prev_ = block->prev_;
    block->prev_->next_ = block->next_;
    if (block == block_list_[nblocks]) {
      block_list_[nblocks] = block->next_;
    }
  } else {
    avail_bm_.unset(nblocks);
    block_list_[nblocks] = NULL;
  }
}

AChunk *BlockSet::alloc_chunk(const uint64_t size, const ObMemAttr &attr)
{
  AChunk *chunk = NULL;
  if (OB_NOT_NULL(tallocator_)) {
    const uint64_t all_size = AChunkMgr::aligned(size);
    if (INTACT_ACHUNK_SIZE == all_size && chunk_free_list_.count() > 0) {
      LockGuard lock(cache_shared_lock_);
      chunk = chunk_free_list_.pop();
    }
    if (nullptr == chunk) {
      chunk = tallocator_->alloc_chunk(static_cast<int64_t>(size), attr);
      if (chunk != nullptr) {
        uint64_t payload = 0;
        UNUSED(ATOMIC_FAA(&total_hold_, chunk->hold(&payload)));
        UNUSED(ATOMIC_FAA(&total_payload_, payload));
      }
    }
    if (NULL != chunk) {
      if (NULL != clist_) {
        chunk->prev_ = clist_->prev_;
        chunk->next_ = clist_;
        clist_->prev_->next_ = chunk;
        clist_->prev_ = chunk;
      } else {
        chunk->prev_ = chunk->next_ = chunk;
        clist_ = chunk;
      }
      chunk->block_set_ = this;
    }
  }
  return chunk;
}

bool BlockSet::add_chunk(const ObMemAttr &attr)
{
  AChunk *chunk = alloc_chunk(ACHUNK_SIZE, attr);
  if (NULL != chunk) {
    ABlock *block = new (chunk->data_) ABlock();
    add_free_block(block, BLOCKS_PER_CHUNK, chunk);
  }
  return NULL != chunk;
}

void BlockSet::free_chunk(AChunk *const chunk)
{
  abort_unless(NULL != chunk);
  abort_unless(chunk->is_valid());
  abort_unless(NULL != chunk->next_);
  abort_unless(NULL != chunk->prev_);
  abort_unless(NULL != clist_);
  abort_unless(chunk->is_all_blks_unused());
  if (chunk == clist_) {
    clist_ = clist_->next_;
  }

  if (chunk == clist_) {
    clist_ = NULL;
  } else {
    chunk->next_->prev_ = chunk->prev_;
    chunk->prev_->next_ = chunk->next_;
  }
  uint64_t payload = 0;
  const uint64_t hold = chunk->hold(&payload);
  bool freed = false;
  if (INTACT_ACHUNK_SIZE == hold) {
    LockGuard lock(cache_shared_lock_);
    freed = chunk_free_list_.push(chunk);
  }
  if (!freed) {
    if (OB_NOT_NULL(tallocator_)) {
      UNUSED(ATOMIC_FAA(&total_hold_, -hold));
      UNUSED(ATOMIC_FAA(&total_payload_, -payload));
      if (chunk->washed_size_ != 0) {
        tallocator_->update_wash_stat(-1, -chunk->washed_blks_, -chunk->washed_size_);
      }
      tallocator_->free_chunk(chunk, attr_);
    }
  }
}

int64_t BlockSet::sync_wash(int64_t wash_size)
{
#if !defined(MADV_DONTNEED)
  return 0;
#endif
  const ssize_t ps = get_page_size();
  bool has_ignore = false;
  int64_t washed_size = 0;
  int64_t washed_blks = 0;
  int64_t related_chunks = 0;
  int cls = avail_bm_.nbits() - 1;
  while (washed_size < wash_size && cls >=1 && (cls = avail_bm_.find_first_most_significant(cls)) != -1) {
    ABlock *head = block_list_[cls];
    if (nullptr == head) {
    } else {
      ABlock *block = head;
      ABlock *next = block->next_;
      do {
        block = next;
        AChunk *chunk = block->chunk();
        if (chunk->is_hugetlb_) {
          _OB_LOG(DEBUG, "cannot be applied to Huge TLB pages");
          has_ignore = true;
        } else {
        #if MEMCHK_LEVEL >= 1
          abort_unless(!block->in_use_ && !block->is_washed_);
          int nblocks = chunk->blk_nblocks(block);
          abort_unless(nblocks == cls);
        #endif
          char *data = chunk->blk_data(block);
          int64_t len = cls * ABLOCK_SIZE;
          if ((reinterpret_cast<uint64_t>(data) & (ps - 1)) != 0 ||
              (len & (ps - 1)) != 0) {
            _OB_LOG(DEBUG, "cannot be applied to non-multiple of page-size, page_size: %zd", ps);
            has_ignore = true;
          } else {
            int result = 0;
            do {
              result = ::madvise(data, len, MADV_DONTNEED);
            } while (result == -1 && errno == EAGAIN);
            if (-1 == result) {
              _OB_LOG_RET(WARN, OB_ERR_SYS, "madvise failed, errno: %d", errno);
              has_ignore = true;
            } else {
              take_off_free_block(block, cls, chunk);
              block->is_washed_ = true;
              if (0 == chunk->washed_blks_) {
                abort_unless(0 == chunk->washed_size_);
                related_chunks++;
              }
              chunk->washed_size_ += len;
              chunk->washed_blks_++;
              washed_blks += 1;
              washed_size += len;
            }
          }
        }
        next = next->next_;
      } while (washed_size < wash_size && block != head);
    }
    cls--;
  }
#if MEMCHK_LEVEL >= 1
  if (wash_size == INT64_MAX && !has_ignore) {
    abort_unless(-1 == avail_bm_.find_first_significant(1));
  }
#endif
  if (washed_size > 0) {
    UNUSED(ATOMIC_FAA(&total_hold_, -washed_size));
    UNUSED(ATOMIC_FAA(&total_payload_, -washed_size));
    tallocator_->update_hold(-washed_size);
    tallocator_->update_wash_stat(related_chunks, washed_blks, washed_size);
  }
#if MEMCHK_LEVEL >= 1
  if (0 == washed_size && ABLOCK_SIZE & (ps - 1)) {
    abort_unless(total_payload_ == total_used_);
  }
#endif
  return washed_size;
}
