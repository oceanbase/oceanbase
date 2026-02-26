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

#ifndef OCEANBASE_COMMON_OB_KVCACHE_HAZARD_DOMAIN_H_
#define OCEANBASE_COMMON_OB_KVCACHE_HAZARD_DOMAIN_H_

#include "lib/container/ob_bit_set.h"
#include "lib/lock/ob_mutex.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kvcache_hazard_pointer.h"
#include "share/cache/ob_kvcache_store.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace common {

class HazardPointer;
class ObKVMemBlockHandle;

class HazptrTLCache final : HazptrList {
public:
  static constexpr int64_t CACHE_SIZE = 128;
  static constexpr int64_t CACHE_ALLOC_BATCH_SIZE = CACHE_SIZE / 2;
  static constexpr int64_t CACHE_FREE_BATCH_SIZE = CACHE_SIZE / 2;
  int acquire_hazptr(HazardPointer*& hazptr);
  void release_hazptr(HazardPointer* hazptr);
  static HazptrTLCache& get_instance();
  void flush();

private:
  friend class HazptrHolder;

  HazptrTLCache();
  ~HazptrTLCache();
};

class HazptrHolder final {
public:
  HazptrHolder();
  ~HazptrHolder();
  DISABLE_COPY_ASSIGN(HazptrHolder);
  int protect(bool& success, ObKVMemBlockHandle* mb_handle, int32_t seq_num);
  int protect(bool& success, ObKVMemBlockHandle* mb_handle);
  void release(); // just release protection
  void reset(); // release protection and return hazard pointer
  void move_from(HazptrHolder& other);
  ObKVMemBlockHandle* get_mb_handle() const;
  bool is_valid() const;
  int assign(const HazptrHolder& other);
  TO_STRING_KV(KP_(hazptr));

private:
  friend class HazardDomain;

  int hazptr_protect(bool& success, ObKVMemBlockHandle* mb_handle, int32_t seq_num);
  int hazptr_protect(bool& success, ObKVMemBlockHandle* mb_handle);
  void hazptr_release();
  void hazptr_reset();
  ObKVMemBlockHandle* hazptr_get_mb_handle() const;
  int hazptr_assign(const HazptrHolder& other);
  void hazptr_move_from(HazptrHolder& other);
  int refcnt_protect(bool& success, ObKVMemBlockHandle* mb_handle, int32_t seq_num);
  int refcnt_protect(bool& success, ObKVMemBlockHandle* mb_handle);
  void refcnt_release();
  void refcnt_reset();
  void refcnt_move_from(HazptrHolder& other);
  ObKVMemBlockHandle* refcnt_get_mb_handle() const;
  int refcnt_assign(const HazptrHolder& other);

union {
  struct {
    union {
      HazardPointer* hazptr_;
      SharedHazptr shared_hazptr_;
    };
    bool is_shared_;
  };
  ObKVMemBlockHandle* mb_handle_;
};
};

template<typename T>
// requires requires(T obj, const T const_obj, T* ptr) {
//   { const_obj.get_next() } -> std::same_as<obj*>;
//   { obj.set_next(ptr) } -> std::same_as<void>;
//   { const_obj.get_next_atomic() } -> std::same_as<obj*>;
//   { obj.set_next_atomic(ptr) } -> std::same_as<void>;
// }
class FixedTinyAllocator
{
public:
  FixedTinyAllocator() = delete;
  FixedTinyAllocator(const ObMemAttr& attr) : attr_(attr), last_wash_time_us_(ObTimeUtility::current_time_us()) {}
  ~FixedTinyAllocator();
  DISABLE_COPY_ASSIGN(FixedTinyAllocator);
  int alloc(SList<T>& list, int64_t num);
  // avoid fragment
  void free_slow(SList<T>& list);
  // avoid fragment
  void choose_and_free(SList<T>& list, int64_t num_to_free);
  char* print_info(char* buffer, int64_t len) const;
  void wash();
  // may tranverse the same T object twice
  template<typename F>
  int for_each(F& func);

private:
  struct Block {
    struct Wrapper {
      T obj_;
      Block* block_;
    };
    constexpr static int64_t SIZEOF_BLOCK = 40;
    constexpr static int64_t MAX_NUM = (OB_MALLOC_NORMAL_BLOCK_SIZE - SIZEOF_BLOCK) / sizeof(Wrapper);

    static Block& get_block(const T& obj) {
      return *((const Wrapper&)obj).block_;
    }
    Block();
    ~Block() = default;
    Block* get_next() const { return next_; }
    void set_next(Block* next) { next_ = next; }
    Block* get_next_atomic() const { return ATOMIC_LOAD_RLX(&next_); }
    void set_next_atomic(Block* next) { ATOMIC_STORE_RLX(&next_, next); }
    template<typename F>
    int for_each(F& func) {
      int ret = OB_SUCCESS;
      for (int i = 0; OB_SUCC(ret) && i < MAX_NUM; ++i) {
        ret = func(all_objs_[i].obj_);
      }
      return ret;
    }

    int64_t allocated_time_us_;
    Block* next_;
    SList<T> list_;
    Wrapper all_objs_[0];
  };
  static_assert(sizeof(Block) == Block::SIZEOF_BLOCK, "sizeof(Block) is not equal to SIZEOF_BLOCK");
  using BlockList = SList<Block>;

  constexpr static int64_t RESERVE_THRESHOLD = Block::MAX_NUM / 4 * 3;
  constexpr static int64_t WASH_THRESHOLD = Block::MAX_NUM * 4;

  int prepare_block();

  const ObMemAttr& attr_;
  // wash() may move using_blocks to recycling_blocks and reorder recycling_blocks
  // tranverse() need to tranverse all recycling_blocks and using_blocks
  // this lock is used to make sure reclaim() and wash() are not running at the same time
  lib::ObMutex blocks_mu_;
  BlockList blocks_;
  SList<T> avai_list_;
  double memory_efficiency_;
  int64_t last_wash_time_us_;
};

struct HazardDomain final {
public:
  int init(int64_t mb_handle_num);
  void reset_retire_list();
  int acquire_hazptrs(HazptrList& list, int64_t num);
  void release_hazptrs(HazptrList& list);
  void choose_and_release_hazptrs(HazptrList& list, int64_t num_to_release);
  int retire(ObKVMemBlockHandle* mb_handle);
  // all the retired memblock must have the same `inst_`
  int retire(ObLink* head, ObLink* tail, uint64_t retire_size);
  template<typename F>
  int reclaim(F func);
  static HazardDomain& get_instance();
  void print_info() const;
  void wash();
  int64_t get_retired_size() { return ATOMIC_LOAD_RLX(&retired_memory_size_); }

private:
  friend class ObKVGlobalCache;

  struct TranverseCallback {
    TranverseCallback(ObBitSet<>& bit_set) : bit_set_(bit_set)
    {}
    int operator()(HazardPointer& hazptr)
    {
      int ret = OB_SUCCESS;
      ObKVMemBlockHandle* mb_handle = hazptr.get_mb_handle();
      if (mb_handle != nullptr && OB_FAIL(bit_set_.add_member(handle_index_of(mb_handle)))) {
        COMMON_LOG(WARN, "bit_set_.add_member failed", K(ret));
      }
      return ret;
    }

    ObBitSet<>& bit_set_;
  };

  HazardDomain();
  ~HazardDomain() { HazptrTLCache::get_instance().flush(); }
  DISABLE_COPY_ASSIGN(HazardDomain);

  // used to allocate bit_set and hazard pointer
  ObMemAttr bit_set_attr_;
  ObMemAttr hazptr_attr_;
  FixedTinyAllocator<HazardPointer> hazptr_allocator_;
  ObBitSet<> bit_set_;
  lib::ObMutex bit_set_mu_;
  ObLink* retire_list_;
  int64_t retired_memory_size_;
};

template <typename T>
FixedTinyAllocator<T>::~FixedTinyAllocator()
{
  while (!blocks_.is_empty()) {
    Block* block = blocks_.pop();
    // if (Block::MAX_NUM != block->list_.get_size()) {
    //   COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "MEMORY LEAK! object is not freed", K(block), K(block->list_.get_size()));
    // }
    ob_free(block);
  }
}

template <typename T>
int FixedTinyAllocator<T>::alloc(SList<T>& list, int64_t num)
{
  int ret = OB_SUCCESS;

  if (list.get_size() > 0 || num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "head or tail is not null", K(list), K(num));
  } else {
    do {
      list = avai_list_.pop_ts(num);
    } while (OB_UNLIKELY(list.get_size() == 0) && OB_SUCC(prepare_block()));
  }

  return ret;
}

template <typename T>
void FixedTinyAllocator<T>::free_slow(SList<T>& list)
{
  choose_and_free(list, list.get_size());
}

template<typename T>
void FixedTinyAllocator<T>::choose_and_free(SList<T>& list, int64_t num_to_free)
{
  SList<T> to_block;
  T* top;
  int64_t num_freed = 0;
  int64_t size = list.get_size();
  if (list.get_size() < num_to_free) {
    num_to_free = list.get_size();
  } else {
    while (num_freed < num_to_free && !list.is_empty()) {
      top = list.top();
      Block& block = Block::get_block(*top);
      to_block.push(list.pop());
      for (typename SList<T>::ErasableIterator iter = list.begin();
           iter != list.end() && num_freed < num_to_free;) {
        if (&block == &Block::get_block(*iter)) {
          to_block.push(list.erase(iter));
          ++num_freed;
        } else {
          ++iter;
        }
      }
      block.list_.push_ts(to_block);
    }
  }
}

template <typename T>
char* FixedTinyAllocator<T>::print_info(char* buffer, int64_t len) const
{
  return buffer + snprintf(buffer,
                      len,
                      "blocks size: %ld, free list size: %ld, memory_efficiency: %lf",
                      blocks_.get_size_ts(),
                      avai_list_.get_size_ts(),
                      memory_efficiency_);
}

template <typename T>
void FixedTinyAllocator<T>::wash()
{
  int ret = OB_SUCCESS;
  Block* block;
  int64_t start_time_us = ObTimeUtility::current_time_us();
  int64_t freed_blocks_num = 0;
  int64_t unused_obj_num = 0;
  SList<T> objs;
  lib::ObMutexGuard blocks_guard(blocks_mu_);
  BlockList blocks = blocks_.pop_all_ts();
  for (typename BlockList::ErasableIterator iter = blocks.begin(); iter != blocks.end();) {
    int64_t list_size = iter->list_.get_size_ts();
    if (Block::MAX_NUM == list_size) {
      Block* block = blocks.erase(iter);
      // COMMON_LOG(INFO, "free block", KP(block));
      ob_free(block);
      ++freed_blocks_num;
    } else {
      unused_obj_num += list_size;
      if (RESERVE_THRESHOLD > list_size) {
        objs = iter->list_.pop_all_ts();
        avai_list_.push_ts(objs);
      }
      ++iter;
    }
  }
  blocks_.push_ts(blocks);

  int64_t last_wash_time_us_ = ObTimeUtility::current_time_us();
  int64_t wash_time_us = last_wash_time_us_ - start_time_us;
  memory_efficiency_ =
      (blocks_.get_size_ts() * Block::MAX_NUM - unused_obj_num) / double(blocks_.get_size_ts() * Block::MAX_NUM);
  COMMON_LOG(INFO, "allocator wash time ", K(wash_time_us), K(freed_blocks_num), K_(memory_efficiency));
}

template <typename T>
template <typename F>
int FixedTinyAllocator<T>::for_each(F& func)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard blocks_guard(blocks_mu_);
  FOREACH(iter, blocks_)
  {
    if (OB_FAIL(iter->for_each(func))) {
      COMMON_LOG(WARN, "failed to call func", K(typeid(F).name()));
      break;
    }
  }
  return ret;
}

template<typename T>
FixedTinyAllocator<T>::Block::Block(): next_(nullptr)
{
  for (int i = 0; i < MAX_NUM; ++i) {
    new (&all_objs_[i].obj_) T();
    list_.push(&all_objs_[i].obj_);
    all_objs_[i].block_ = this;
  }
  allocated_time_us_ = ObTimeUtil::current_time_us();
}

template <typename T>
int FixedTinyAllocator<T>::prepare_block()
{
  int ret = OB_SUCCESS;
  Block *new_block = nullptr;
  SList<T> list;

  if (OB_ISNULL(new_block = (Block*)ob_malloc(OB_MALLOC_NORMAL_BLOCK_SIZE, attr_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate new block", K(ret), K(attr_));
  } else {
    // COMMON_LOG(INFO, "new block", KP(new_block), K(ret), K(attr_));
    new (new_block) Block();
    list = new_block->list_;
    new_block->list_.reset();
    blocks_.push_front_ts(new_block);
    this->avai_list_.push_ts(list);
  }

  return ret;
}

template<typename F>
int HazardDomain::reclaim(F func)
{
  int ret = OB_SUCCESS;
  ObLink *reclaim_list, *prev, *curr, *next, *retire_list;
  ObKVMemBlockHandle* mb_handle;
  uint64_t reclaimed_memory_size = 0, retired_memory_size = ATOMIC_LOAD_RLX(&retired_memory_size_);

  // COMMON_LOG(INFO, "[KV_CACHE_HAZARD_DOMAIN] start to reclaim", K(free_list_size_), K(retired_memory_size));
  if (OB_UNLIKELY(!GCONF._enable_kvcache_hazard_pointer)) {
  } else if (retired_memory_size > 0) {
    reclaim_list = ATOMIC_TAS(&retire_list_, nullptr);
    // MEM_BARRIER();
    if (OB_ISNULL(reclaim_list)) {
    } else {
      {
        lib::ObMutexGuard bit_set_guard(bit_set_mu_);

        bit_set_.clear_all();

        TranverseCallback callback(bit_set_);
        ret = hazptr_allocator_.for_each(callback);

        for (prev = nullptr, curr = reclaim_list; OB_SUCC(ret) && OB_NOT_NULL(curr); curr = next) {
          next = curr->next_;
          mb_handle = CONTAINER_OF(curr, ObKVMemBlockHandle, retire_link_);
          if (!bit_set_.has_member(handle_index_of(mb_handle))) {
            if (OB_ISNULL(prev)) {
              reclaim_list = next;
            } else {
              prev->next_ = next;
            }
            reclaimed_memory_size += mb_handle->mem_block_->get_hold_size();
            ATOMIC_SAF(&mb_handle->inst_->status_.retired_size_, mb_handle->mem_block_->get_hold_size());
            func(mb_handle);
          } else {
            prev = curr;
          }
        }
      }  // bit_set_mu_

      if (OB_SUCC(ret) && nullptr != prev) {
        // return reclaim_list to retire_list
        ObLink*& tail = prev;
        retire_list = ATOMIC_LOAD(&retire_list_);
        do {
          tail->next_ = retire_list;
        } while (tail->next_ != (retire_list = ATOMIC_VCAS(&retire_list_, retire_list, reclaim_list)));
      }
      ATOMIC_SAF(&retired_memory_size_, reclaimed_memory_size);
      COMMON_LOG(INFO, "[KV_CACHE_HAZARD_DOMAIN] finish reclaim", K(retired_memory_size_), K(reclaimed_memory_size));
    }
  }

  return ret;
}

};  // namespace common
};  // namespace oceanbase

#endif
