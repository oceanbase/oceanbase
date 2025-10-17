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

namespace oceanbase
{
namespace common
{
extern ObBlockAllocMgr default_blk_alloc;
class ObBlockVSlicer
{
  template<int ARENA_NUM> friend class ObVSliceAllocT;
public:
  enum { K = INT64_MAX };
  static const uint32_t ITEM_MAGIC_CODE = 0XCCEEDDF1;
  static const uint32_t ITEM_MAGIC_CODE_MASK = 0XFFFFFFF0;
  typedef ObBlockVSlicer Host;
  struct Item
  {
    Item(Host* host, int64_t size): MAGIC_CODE_(ITEM_MAGIC_CODE), host_(host), size_(size) {}
    ~Item() {}
    uint32_t MAGIC_CODE_;
    Host* host_;
    int64_t size_;
  } __attribute__((aligned (16)));;
  ObBlockVSlicer(ObIAllocator* vslice_alloc, int64_t blk_size)
    : vslice_alloc_(vslice_alloc), arena_(nullptr), blk_size_(blk_size), ref_(K), pos_(0) {}
  ~ObBlockVSlicer() {}
  int64_t get_blk_size() const { return blk_size_; }
  int64_t get_using_size()
  {
    int64_t v = ATOMIC_LOAD(&ref_);
    return v > K ? (v - K) : v;
  }
  int64_t get_limit() const { return blk_size_ - sizeof(*this); }
  Item* alloc_item(int64_t size, int64_t &leak_pos) {
    Item* p = NULL;
    int64_t alloc_size = size + sizeof(*p);
    int64_t pos = ATOMIC_FAA(&pos_, alloc_size);
    if (pos + alloc_size <= get_limit()) {
      p = (Item*)(base_ + pos);
      new(p)Item(this, alloc_size);
      ATOMIC_FAA(&ref_, alloc_size);
    } else if (pos <= get_limit()) {
      leak_pos = pos;
    }
    return p;
  }
  bool free_item(Item* p, bool &can_purge)
  {
    int64_t nv = ATOMIC_AAF(&ref_, -p->size_);
    can_purge = (K == nv);
    return 0 == nv;
  }
  bool freeze() {
    int64_t old_pos = ATOMIC_FAA(&pos_, blk_size_ + 1);
    return (old_pos <= get_limit());
  }
  bool retire() { return 0 == ATOMIC_AAF(&ref_, -K); }
  ObIAllocator* get_vslice_alloc() { return vslice_alloc_; }
private:
  ObIAllocator* vslice_alloc_;
  void *arena_;
  int64_t blk_size_ CACHE_ALIGNED;
  int64_t ref_ CACHE_ALIGNED;
  int64_t pos_ CACHE_ALIGNED;
  char base_[] CACHE_ALIGNED;
};

template<int ARENA_NUM = 32>
class ObVSliceAllocT : public ObIAllocator
{
  friend class ObBlockVSlicer;
public:
  enum { MAX_ARENA_NUM = 32, DEFAULT_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE };
  static_assert(ARENA_NUM > 0 && ARENA_NUM <= MAX_ARENA_NUM, "ARENA_NUM must be in range [1, MAX_ARENA_NUM]");
  typedef ObBlockAllocMgr BlockAlloc;
  typedef ObBlockVSlicer Block;
  typedef ObSimpleSync Sync;
  class Arena: public Sync
  {
  public:
    Arena(): blk_(NULL) {}
    Block* blk() { return ATOMIC_LOAD(&blk_); }
    bool cas(Block* ov, Block* nv) { return ATOMIC_BCAS(&blk_, ov, nv); }
    Block* clear() { return ATOMIC_TAS(&blk_, NULL); }
  private:
    Block* blk_;
  } CACHE_ALIGNED;
public:
  ObVSliceAllocT(): nway_(0), bsize_(0), blk_alloc_(default_blk_alloc) {}
  ObVSliceAllocT(const ObMemAttr &attr, const int64_t block_size = DEFAULT_BLOCK_SIZE,
      BlockAlloc &blk_alloc = default_blk_alloc, int nway = 1)
    : bsize_(block_size), mattr_(attr), blk_alloc_(blk_alloc)
  {
    set_nway(nway);
  }
  virtual ~ObVSliceAllocT() override { destroy(); }
  int init(int64_t block_size, BlockAlloc& block_alloc, const ObMemAttr& attr) {
    int ret = OB_SUCCESS;
    new(this)ObVSliceAllocT(attr, block_size, block_alloc);
    return ret;
  }
  void destroy() {
    purge_extra_cached_block(0, true);
    bsize_ = 0;
  }
  void purge() {
    purge_extra_cached_block(0);
  }
  void set_nway(int nway) {
    if (nway <= 0) {
      nway = 1;
    } else if (nway > ARENA_NUM) {
      nway = ARENA_NUM;
    }
    ATOMIC_STORE(&nway_,  nway);
    purge_extra_cached_block(nway);
  }
  bool can_alloc_block(int64_t size) const {
    return blk_alloc_.can_alloc_block(size);
  }
  void set_limit(int64_t limit) { blk_alloc_.set_limit(limit); }
  int64_t hold() const { return blk_alloc_.hold(); }
  int64_t limit() { return blk_alloc_.limit(); }
  int64_t used() const { return hold(); }
  virtual void* alloc(const int64_t size, const ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc(size);
  }
  virtual void* alloc(const int64_t size) override {
#ifdef OB_USE_ASAN
    return ::malloc(size);
#else
    Block::Item* ret = NULL;
    int64_t aligned_size = upper_align(size, 16);
    if (bsize_ <= aligned_size + sizeof(Block) + sizeof(Block::Item)) {
      int64_t direct_size = aligned_size + sizeof(Block) + sizeof(Block::Item);
      Block *blk = NULL;
      void *buf = blk_alloc_.alloc_block(direct_size, mattr_);
      if (NULL != buf) {
        blk = new(buf)Block(this, direct_size);
        int64_t leak_pos = -1;
        ret = blk->alloc_item(aligned_size, leak_pos);
        if (blk->freeze()) {
          if (blk->retire()) {
            destroy_block(blk);
          }
        }
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      while(NULL == ret && OB_SUCCESS == tmp_ret) {
        Block *blk2destroy = NULL;
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
          arena.ref(-1);
        } else {
          arena.ref(-1);
          Block* nb = prepare_block();
          if (NULL == nb) {
            // alloc block fail, end
            tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            nb->arena_ = &arena;
            if (!arena.cas(NULL, nb)) {
              // no need wait sync
              destroy_block(nb);
            }
          }
        }
        if (blk2destroy) {
          arena.sync();
          if (blk2destroy->retire()) {
            destroy_block(blk2destroy);
          }
        }
      }
    }
    return NULL == ret? NULL: (void*)(ret + 1);
#endif
  }
  virtual void free(void* p) override {

#ifdef OB_USE_ASAN
    ::free(p);
#else
    if (NULL != p) {
      Block::Item* item = (Block::Item*)p - 1;
      abort_unless(Block::ITEM_MAGIC_CODE == item->MAGIC_CODE_);
      Block* blk = item->host_;
      Arena *arena = (Arena*)blk->arena_;
#ifndef NDEBUG
      abort_unless(blk->get_vslice_alloc() == static_cast<ObIAllocator*>(this));
      abort_unless(bsize_ != 0);
#else
      if (static_cast<ObIAllocator*>(this) != blk->get_vslice_alloc()) {
        LIB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "blk is freed or alloced by different vslice_alloc", K(this), K(blk->get_vslice_alloc()));
        return;
      }
#endif
      item->MAGIC_CODE_ = item->MAGIC_CODE_ & Block::ITEM_MAGIC_CODE_MASK;
      bool can_purge = false;
      if (blk->free_item(item, can_purge)) {
        destroy_block(blk);
      } else if (can_purge) {
        purge_extra_cached_block(*arena);
      }
    }

#endif
  }
  virtual void set_attr(const ObMemAttr &attr) override {
    mattr_ = attr;
  }

  void purge_extra_cached_block(Arena &arena, bool need_check = false) {
    arena.ref(1);
    Block* old_blk = arena.clear();
    if (NULL != old_blk) {
      if (old_blk->freeze()) {
        arena.ref(-1);
        arena.sync();
        if (old_blk->retire()) {
          destroy_block(old_blk);
        } else if (need_check) {
          // can not monitor all leak !!!
          LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "there was memory leak", K(old_blk->ref_));
        }
      } else {
        arena.ref(-1);
      }
    } else {
     arena.ref(-1);
    }
  }
  void purge_extra_cached_block(int keep, bool need_check = false) {
    for(int i = ARENA_NUM - 1; i >= keep; i--) {
      purge_extra_cached_block(arena_[i], need_check);
    }
  }
  double get_block_using_ratio(void* p) {
    double ratio = 1.0;
#ifndef OB_USE_ASAN
    if (NULL != p) {
      Block::Item* item = (Block::Item*)p - 1;
      Block* blk = item->host_;
      int64_t total_bytes = blk->get_blk_size();
      int64_t using_bytes = blk->get_using_size();
      if (using_bytes > 0) {
        ratio = 1.0 * using_bytes / total_bytes;
      }
    }
#endif
    return ratio;
  }
private:
  Block* prepare_block() {
    Block *blk = NULL;
    void *buf = NULL;
    if (NULL != (buf = blk_alloc_.alloc_block(bsize_, mattr_))) {
      blk = new(buf)Block(this, bsize_);
    }
    return blk;
  }
  void destroy_block(Block* blk) {
    blk_alloc_.free_block(blk, blk->get_blk_size());
  }
protected:
  int nway_ CACHE_ALIGNED;
  int64_t bsize_;
  ObMemAttr mattr_;
  Arena arena_[ARENA_NUM];
  BlockAlloc &blk_alloc_;
};

using ObVSliceAlloc = ObVSliceAllocT<>; // keep compatibility
}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_VSLICE_ALLOC_H_ */
