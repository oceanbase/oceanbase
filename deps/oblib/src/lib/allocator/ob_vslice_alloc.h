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

#ifndef OCEANBASE_ALLOCATOR_OB_VSLICE_ALLOC_H_
#define OCEANBASE_ALLOCATOR_OB_VSLICE_ALLOC_H_

#include "lib/allocator/ob_block_alloc_mgr.h"
#include "lib/allocator/ob_slice_alloc.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_utility.h"

namespace oceanbase {
namespace common {
extern ObBlockAllocMgr default_blk_alloc;

class ObBlockVSlicer {
public:
  typedef ObBlockVSlicer Host;
  struct Item {
    Item(Host* host, int64_t size) : host_(host), size_(size)
    {}
    ~Item()
    {}
    Host* host_;
    int64_t size_;
  };
  ObBlockVSlicer(int64_t blk_size) : blk_size_(blk_size), ref_(0), pos_(0)
  {}
  ~ObBlockVSlicer()
  {}
  int64_t get_blk_size() const
  {
    return blk_size_;
  }
  int64_t get_using_size()
  {
    return ATOMIC_LOAD(&ref_);
  }
  Item* alloc_item(int64_t size, int64_t& leak_pos)
  {
    Item* p = NULL;
    int64_t alloc_size = size + sizeof(*p);
    int64_t pos = ATOMIC_FAA(&pos_, alloc_size);
    int64_t limit = blk_size_ - sizeof(*this);
    if (pos + alloc_size <= limit) {
      p = (Item*)(base_ + pos);
      new (p) Item(this, alloc_size);
    } else if (pos <= limit) {
      leak_pos = pos;
    }
    return p;
  }
  bool free_item(Item* p)
  {
    return 0 == ATOMIC_AAF(&ref_, -p->size_);
  }
  bool freeze(int64_t& old_pos)
  {
    old_pos = ATOMIC_FAA(&pos_, blk_size_ + 1);
    return (old_pos < blk_size_);
  }
  bool retire(int64_t val)
  {
    return 0 == ATOMIC_AAF(&ref_, val);
  }

private:
  int64_t blk_size_ CACHE_ALIGNED;
  int64_t ref_ CACHE_ALIGNED;
  int64_t pos_ CACHE_ALIGNED;
  char base_[] CACHE_ALIGNED;
};

class ObVSliceAlloc {
public:
  enum { MAX_ARENA_NUM = 32, DEFAULT_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE };
  typedef ObBlockAllocMgr BlockAlloc;
  typedef ObBlockVSlicer Block;
  typedef ObSimpleSync Sync;
  class Arena : public Sync {
  public:
    Arena() : blk_(NULL)
    {}
    Block* blk()
    {
      return ATOMIC_LOAD(&blk_);
    }
    bool cas(Block* ov, Block* nv)
    {
      return ATOMIC_BCAS(&blk_, ov, nv);
    }
    Block* clear()
    {
      return ATOMIC_TAS(&blk_, NULL);
    }

  private:
    Block* blk_;
  } CACHE_ALIGNED;

public:
  ObVSliceAlloc() : nway_(0), bsize_(0), blk_alloc_(default_blk_alloc)
  {}
  ObVSliceAlloc(const ObMemAttr& attr, int block_size = DEFAULT_BLOCK_SIZE, BlockAlloc& blk_alloc = default_blk_alloc)
      : nway_(1), bsize_(block_size), mattr_(attr), blk_alloc_(blk_alloc)
  {}
  ~ObVSliceAlloc()
  {}
  int init(int block_size, BlockAlloc& block_alloc, const ObMemAttr& attr)
  {
    int ret = OB_SUCCESS;
    new (this) ObVSliceAlloc(attr, block_size, block_alloc);
    return ret;
  }
  void set_nway(int nway)
  {
    if (nway <= 0) {
      nway = 1;
    } else if (nway > MAX_ARENA_NUM) {
      nway = MAX_ARENA_NUM;
    }
    ATOMIC_STORE(&nway_, nway);
    purge_extra_cached_block(nway);
  }
  bool can_alloc_block(int64_t size) const
  {
    return blk_alloc_.can_alloc_block(size);
  }
  int64_t hold()
  {
    return blk_alloc_.hold();
  }
  int64_t limit()
  {
    return blk_alloc_.limit();
  }
  void* alloc(int64_t size)
  {
    Block::Item* ret = NULL;
    int64_t aligned_size = upper_align(size, 16);
    if (bsize_ <= aligned_size + sizeof(Block) + sizeof(Block::Item)) {
      int64_t direct_size = aligned_size + sizeof(Block) + sizeof(Block::Item);
      Block* blk = NULL;
      void* buf = blk_alloc_.alloc_block(direct_size, mattr_);
      if (NULL != buf) {
        blk = new (buf) Block(direct_size);
        int64_t leak_pos = -1;
        ret = blk->alloc_item(aligned_size, leak_pos);
        int64_t old_pos = INT64_MAX;
        if (blk->freeze(old_pos)) {
          if (blk->retire(old_pos)) {
            destroy_block(blk);
          }
        }
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      while (NULL == ret && OB_SUCCESS == tmp_ret) {
        Block* blk2destroy = NULL;
        int64_t leak_pos = -1;
        Arena& arena = arena_[get_itid() % nway_];
        arena.ref(1);
        Block* blk = arena.blk();
        if (NULL != blk) {
          if (NULL == (ret = blk->alloc_item(aligned_size, leak_pos))) {
            arena.cas(blk, NULL);
            if (-1 != leak_pos) {
              blk2destroy = blk;
            }
          }
        } else {
          Block* nb = prepare_block();
          if (NULL == nb) {
            // alloc block fail, end
            tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            if (!arena.cas(NULL, nb)) {
              // no need wait sync
              destroy_block(nb);
            }
          }
        }
        arena.ref(-1);
        if (blk2destroy) {
          arena.sync();
          if (blk2destroy->retire(leak_pos)) {
            destroy_block(blk2destroy);
          }
        }
      }
    }
    return NULL == ret ? NULL : (void*)(ret + 1);
  }
  void free(void* p)
  {
    if (bsize_ > 0 && NULL != p) {
      Block::Item* item = (Block::Item*)p - 1;
      Block* blk = item->host_;
      if (blk->free_item(item)) {
        destroy_block(blk);
      }
    }
  }
  void purge_extra_cached_block(int keep)
  {
    for (int i = MAX_ARENA_NUM - 1; i >= keep; i--) {
      Arena& arena = arena_[i];
      Block* old_blk = arena.clear();
      if (NULL != old_blk) {
        int64_t old_pos = INT64_MAX;
        if (old_blk->freeze(old_pos)) {
          arena.sync();
          if (old_blk->retire(old_pos)) {
            destroy_block(old_blk);
          }
        }
      }
    }
  }
  double get_block_using_ratio(void* p)
  {
    double ratio = 1.0;
    if (NULL != p) {
      Block::Item* item = (Block::Item*)p - 1;
      Block* blk = item->host_;
      int64_t total_bytes = blk->get_blk_size();
      int64_t using_bytes = blk->get_using_size();
      if (using_bytes > 0) {
        ratio = 1.0 * using_bytes / total_bytes;
      }
    }
    return ratio;
  }

private:
  Block* prepare_block()
  {
    Block* blk = NULL;
    void* buf = NULL;
    if (NULL != (buf = blk_alloc_.alloc_block(bsize_, mattr_))) {
      blk = new (buf) Block(bsize_);
    }
    return blk;
  }
  void destroy_block(Block* blk)
  {
    blk_alloc_.free_block(blk, blk->get_blk_size());
  }

protected:
  int nway_ CACHE_ALIGNED;
  int64_t bsize_;
  ObMemAttr mattr_;
  Arena arena_[MAX_ARENA_NUM];
  BlockAlloc& blk_alloc_;
};

};  // end namespace common
};  // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_VSLICE_ALLOC_H_ */
