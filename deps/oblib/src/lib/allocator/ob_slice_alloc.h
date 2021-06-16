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

#ifndef OCEANBASE_ALLOCATOR_OB_SLICE_ALLOC_H_
#define OCEANBASE_ALLOCATOR_OB_SLICE_ALLOC_H_
#include "lib/queue/ob_link.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_lock_guard.h"
#include "lib/allocator/ob_block_alloc_mgr.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace common {
extern ObBlockAllocMgr default_blk_alloc;

class ObSimpleSync {
public:
  ObSimpleSync() : ref_(0)
  {}
  int32_t ref(int32_t x)
  {
    return ATOMIC_AAF(&ref_, x);
  }
  void sync()
  {
    while (ATOMIC_LOAD(&ref_) > 0) {
      PAUSE();
    }
  }

private:
  int32_t ref_;
};

class ObDListWithLock {
public:
  typedef ObDLink DLink;
  ObDListWithLock() : lock_()
  {
    head_.next_ = &head_;
    head_.prev_ = &head_;
  }
  ~ObDListWithLock()
  {}
  void add(DLink* p)
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    add_(p);
  }
  void del(DLink* p)
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    del_(p);
  }
  DLink* top()
  {
    DLink* p = (DLink*)ATOMIC_LOAD(&head_.next_);
    return &head_ == p ? NULL : p;
  }

private:
  void add_(DLink* p)
  {
    DLink* prev = head_.prev_;
    DLink* next = &head_;
    p->prev_ = prev;
    p->next_ = next;
    prev->next_ = p;
    next->prev_ = p;
  }
  void del_(DLink* p)
  {
    DLink* prev = p->prev_;
    DLink* next = (DLink*)p->next_;
    prev->next_ = next;
    next->prev_ = prev;
  }

private:
  mutable common::ObSpinLock lock_;
  DLink head_ CACHE_ALIGNED;
};

class ObEmbedFixedQueue {
public:
  ObEmbedFixedQueue() : push_(0), pop_(0), capacity_(0)
  {}
  ~ObEmbedFixedQueue()
  {}
  void init(uint32_t capacity)
  {
    capacity_ = capacity;
    memset(data_, 0, sizeof(void*) * capacity);
  }
  void push(void* p)
  {
    void** addr = data_ + ATOMIC_FAA(&push_, 1) % capacity_;
    while (!ATOMIC_BCAS(addr, NULL, p)) {
      PAUSE();
    }
  }
  void* pop()
  {
    void* p = NULL;
    void** addr = data_ + ATOMIC_FAA(&pop_, 1) % capacity_;
    while (NULL == (p = ATOMIC_TAS(addr, NULL))) {
      PAUSE();
    }
    return p;
  }

private:
  uint32_t push_ CACHE_ALIGNED;
  uint32_t pop_ CACHE_ALIGNED;
  uint32_t capacity_ CACHE_ALIGNED;
  void* data_[];
};

/*
  shared [K, K+N]   ->  private [0, N]   ->  flying [-K, -K+N]   ->   shared
                  acquire              release                  recycle
  1. shared block must be linked in list
  2. private block can be used for alloc, or can be destroyed.
  3. flying block is a transient, difference with private state is that private block has no chance to trigger link to
  list.
 */
class ObStockCtrl {
public:
  enum { K = INT32_MAX / 2 };
  ObStockCtrl() : total_(0), stock_(0)
  {}
  ~ObStockCtrl()
  {}
  void init_stock(int32_t n)
  {
    total_ = n;
    stock_ = n;
  }

public:
  uint32_t total()
  {
    return total_;
  }
  bool acquire()
  {
    return dec_if_gt(K, K) > K;
  }
  bool release()
  {
    return faa(-K) > 0;
  }
  bool recyle()
  {
    return inc_if_lt(2 * K, -K + total_) == -K + total_;
  }
  bool alloc_stock()
  {
    return dec_if_gt(1, 0) > 0;
  }
  bool free_stock(bool& first_free)
  {
    int32_t ov = cas_or_inc(K + total_ - 1, -K + total_, 1);
    first_free = (ov == -K);
    return ov == K + total_ - 1;
  }

private:
  int32_t faa(int32_t x)
  {
    return ATOMIC_FAA(&stock_, x);
  }
  int32_t aaf(int32_t x)
  {
    return ATOMIC_AAF(&stock_, x);
  }
  int32_t dec_if_gt(int32_t x, int32_t b)
  {
    int32_t ov = ATOMIC_LOAD(&stock_);
    int32_t nv = 0;
    while (ov > b && ov != (nv = ATOMIC_VCAS(&stock_, ov, ov - x))) {
      ov = nv;
    }
    return ov;
  }
  int32_t inc_if_lt(int32_t x, int32_t b)
  {
    int32_t ov = ATOMIC_LOAD(&stock_);
    int32_t nv = 0;
    while (ov < b && ov != (nv = ATOMIC_VCAS(&stock_, ov, ov + x))) {
      ov = nv;
    }
    return ov;
  }
  int32_t cas_or_inc(int32_t cmp, int32_t val, int32_t x)
  {
    int32_t ov = ATOMIC_LOAD(&stock_);
    int32_t nv = 0;
    while (
        (cmp == ov) ? (ov != (nv = ATOMIC_VCAS(&stock_, cmp, val))) : (ov != (nv = ATOMIC_VCAS(&stock_, ov, ov + x)))) {
      ov = nv;
    }
    return ov;
  }

private:
  int32_t total_ CACHE_ALIGNED;
  int32_t stock_ CACHE_ALIGNED;
};

class ObBlockSlicer : public ObStockCtrl {
public:
  typedef ObEmbedFixedQueue FList;
  typedef ObBlockSlicer Host;
  struct Item {
  public:
    Item(Host* host) : host_(host)
    {}
    ~Item()
    {}
    ObBlockSlicer* host_;
  } __attribute__((aligned(16)));
  ;

public:
  ObDLink dlink_ CACHE_ALIGNED;

public:
  ObBlockSlicer(int32_t limit, int32_t slice_size, void* tmallocator = NULL) : tmallocator_(tmallocator)
  {
    int64_t isize = lib::align_up2((int32_t)sizeof(Item) + slice_size, 16);
    int64_t total = (limit - (int32_t)sizeof(*this)) / (isize + (int32_t)sizeof(void*));
    char* istart = (char*)lib::align_up2((uint64_t)((char*)(this + 1) + sizeof(void*) * total), 16);
    if (istart + isize * total > ((char*)this) + limit) {
      total--;
    }
    flist_.init((int32_t)total);
    for (int32_t i = 0; i < total; i++) {
      flist_.push((void*)new (istart + i * isize) Item(this));
    }
    init_stock((int32_t)total);
    hash_ = cal_hash_((uint64_t)this);
  }
  ~ObBlockSlicer()
  {
    tmallocator_ = NULL;
  }
  uint64_t cal_hash_(uint64_t h)
  {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccd;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53;
    h ^= h >> 33;
    return h;
  }
  Item* alloc_item()
  {
    return alloc_stock() ? (Item*)flist_.pop() : NULL;
  }
  bool free_item(Item* p, bool& first_free)
  {
    flist_.push(p);
    return free_stock(first_free);
  }
  uint64_t hash() const
  {
    return hash_;
  }
  void* get_tmallocator()
  {
    return tmallocator_;
  }

private:
  uint64_t hash_;
  void* tmallocator_;
  FList flist_ CACHE_ALIGNED;
};

class ObSliceAlloc {
public:
  enum { MAX_ARENA_NUM = 32, MAX_REF_NUM = 4096, DEFAULT_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE };
  typedef ObSimpleSync Sync;
  typedef ObBlockSlicer Block;
  typedef ObBlockAllocMgr BlockAlloc;
  typedef ObDListWithLock BlockList;
  class Arena {
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
  ObSliceAlloc() : nway_(0), bsize_(0), isize_(0), slice_limit_(0), blk_alloc_(default_blk_alloc), tmallocator_(NULL)
  {}
  ObSliceAlloc(const int size, const ObMemAttr& attr, int block_size = DEFAULT_BLOCK_SIZE,
      BlockAlloc& blk_alloc = default_blk_alloc, void* tmallocator = NULL)
      : nway_(1), bsize_(block_size), isize_(size), attr_(attr), blk_alloc_(blk_alloc), tmallocator_(tmallocator)
  {
    slice_limit_ = block_size - (int32_t)sizeof(Block) - (int32_t)sizeof(Block::Item);
    LIB_LOG(INFO, "ObSliceAlloc init finished", K(bsize_), K(isize_), K(slice_limit_), KP(tmallocator_));
  }
  ~ObSliceAlloc()
  {
    tmallocator_ = NULL;
  }
  int init(const int size, const int block_size, BlockAlloc& block_alloc, const ObMemAttr& attr)
  {
    int ret = common::OB_SUCCESS;
    new (this) ObSliceAlloc(size, attr, block_size, block_alloc, NULL);
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
  void try_purge()
  {
    if (NULL == blk_list_.top()) {
      purge_extra_cached_block(0);
    }
  }
  int64_t limit()
  {
    return blk_alloc_.limit();
  }
  int64_t hold()
  {
    return blk_alloc_.hold();
  }
  void* alloc()
  {
    Block::Item* ret = NULL;
    int tmp_ret = OB_SUCCESS;
    if (isize_ > slice_limit_) {
      // slice_size is larger than block_size
      tmp_ret = OB_ERR_UNEXPECTED;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        LIB_LOG(ERROR, "slice size is larger than block size, unexpected !", K(tmp_ret), K(isize_), K(slice_limit_));
      }
    }
    while (NULL == ret && OB_SUCCESS == tmp_ret) {
      Arena& arena = arena_[get_itid() % nway_];
      Block* blk = arena.blk();
      if (NULL == blk) {
        Block* new_blk = prepare_block();
        if (NULL == new_blk) {
          // alloc block fail, end
          tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          if (!arena.cas(NULL, new_blk)) {
            release_block(new_blk);
          }
        }
      } else {
        Block* blk2release = NULL;
        int64_t slot_idx = blk->hash() % MAX_REF_NUM;
        blk_ref_[slot_idx].ref(1);
        if (blk == arena.blk()) {
          if (NULL == (ret = blk->alloc_item())) {
            if (arena.cas(blk, NULL)) {
              blk2release = blk;
            }
          }
        }
        blk_ref_[slot_idx].ref(-1);
        if (NULL != blk2release) {
          blk_ref_[blk2release->hash() % MAX_REF_NUM].sync();
          release_block(blk2release);
        }
      }
    }
    return NULL == ret ? NULL : (void*)(ret + 1);
  }
  void free(void* p)
  {
    if (NULL != p) {
      Block::Item* item = (Block::Item*)p - 1;
      Block* blk = item->host_;
      bool first_free = false;
      bool need_destroy = blk->free_item(item, first_free);
      if (need_destroy) {
        destroy_block(blk);
      } else if (first_free) {
        add_to_blist(blk);
      }
    }
  }
  void purge_extra_cached_block(int keep)
  {
    for (int i = MAX_ARENA_NUM - 1; i >= keep; i--) {
      Arena& arena = arena_[i];
      Block* old_blk = arena.clear();
      if (NULL != old_blk) {
        blk_ref_[old_blk->hash() % MAX_REF_NUM].sync();
        release_block(old_blk);
      }
    }
  }
  int64_t to_string(char* buf, const int64_t limit) const
  {
    return snprintf(buf,
        limit,
        "SliceAlloc: nway=%d bsize/isize=%d/%d limit=%d attr=%s",
        nway_,
        bsize_,
        isize_,
        slice_limit_,
        to_cstring(attr_));
  }

private:
  void release_block(Block* blk)
  {
    if (blk->release()) {
      add_to_blist(blk);
    }
  }
  void add_to_blist(Block* blk)
  {
    blk_list_.add(&blk->dlink_);
    if (blk->recyle()) {
      destroy_block(blk);
    }
  }
  Block* get_from_blist()
  {
    Block* ret = NULL;
    ObDLink* dlink = NULL;
    do {
      reclaim_sync_.ref(1);
      if (NULL != (dlink = blk_list_.top())) {
        Block* blk = CONTAINER_OF(dlink, Block, dlink_);
        if (blk->acquire()) {
          ret = blk;
        }
      }
      reclaim_sync_.ref(-1);
    } while (NULL == ret && dlink != NULL);
    if (NULL != ret) {
      blk_list_.del(&ret->dlink_);
    }
    return ret;
  }
  Block* prepare_block()
  {
    Block* ret_blk = get_from_blist();
    if (NULL == ret_blk) {
      void* ptr = NULL;
      if (NULL != (ptr = blk_alloc_.alloc_block(bsize_, attr_))) {
        ret_blk = new (ptr) Block(bsize_, isize_, tmallocator_);
      }
    }
    return ret_blk;
  }
  void destroy_block(Block* blk)
  {
    blk_list_.del(&blk->dlink_);
    reclaim_sync_.sync();
    blk_alloc_.free_block(blk, bsize_);
    // try_purge();
  }

protected:
  int nway_ CACHE_ALIGNED;
  int32_t bsize_;
  int32_t isize_;
  int32_t slice_limit_;
  ObMemAttr attr_;
  Sync reclaim_sync_ CACHE_ALIGNED;
  BlockList blk_list_ CACHE_ALIGNED;
  Arena arena_[MAX_ARENA_NUM];
  Sync blk_ref_[MAX_REF_NUM];
  BlockAlloc& blk_alloc_;
  void* tmallocator_;
};
};     // end namespace common
};     // end namespace oceanbase
#endif /* OCEANBASE_ALLOCATOR_OB_SLICE_ALLOC_H_ */
