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

#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase {
namespace common {
volatile int64_t ObObjFreeListList::once_ = 0;
volatile ObObjFreeListList* ObObjFreeListList::instance_ = NULL;
const int64_t ObObjFreeListList::MAX_NUM_FREELIST = 1024;
pthread_key_t ObObjFreeListList::objpool_key_;
const int64_t ObObjFreeList::THREAD_CACHE_TOTAL_SIZE = 4 * 1024 * 1024;
const int64_t ObObjFreeList::THREAD_CACHE_LOW_MARK = 4;
static const uint8_t ITEM_MAGIC = 0xFF;

static inline int64_t upper_round(const int64_t value, const int64_t align)
{
  return (value + align - 1L) & ~(align - 1L);
}

static inline ObThreadCache*& get_thread_cache(const int64_t thread_cache_idx)
{
  static __thread char g_thread_caches_buf[sizeof(ObThreadCache*) * ObObjFreeListList::MAX_NUM_FREELIST];
  static __thread ObThreadCache** g_thread_caches = NULL;
  if (OB_ISNULL(g_thread_caches)) {
    g_thread_caches = reinterpret_cast<ObThreadCache**>(g_thread_caches_buf);
    if (0 != pthread_setspecific(ObObjFreeListList::objpool_key_, (void*)(g_thread_caches))) {
      OB_LOG(ERROR, "failed to pthread_setspecific");
    }
  }
  return g_thread_caches[thread_cache_idx];
}

void thread_shutdown_cleanup(void* arg)
{
  ObThreadCache** thread_caches = reinterpret_cast<ObThreadCache**>(arg);
  if (NULL != thread_caches) {
    int64_t freelist_count = ObObjFreeListList::get_freelists().get_freelist_count();
    for (int64_t i = 0; i < freelist_count && i < ObObjFreeListList::MAX_NUM_FREELIST; ++i) {
      if (NULL != thread_caches[i]) {
        thread_caches[i]->f_->destroy_cur_thread_cache();
      }
    }
  }
}

inline ObChunkInfo*& ObObjFreeList::get_chunk_info(void* item)
{
  return *reinterpret_cast<ObChunkInfo**>(reinterpret_cast<char*>(item) + type_size_ - sizeof(void*));
}

ObObjFreeList::ObObjFreeList()
    : freelists_(NULL),
      tc_allocator_(NULL),
      is_inited_(false),
      only_global_(false),
      label_(nullptr),
      thread_cache_idx_(0),
      name_(NULL),
      type_size_(0),
      type_size_base_(0),
      alignment_(0),
      obj_count_base_(0),
      obj_count_per_chunk_(0),
      chunk_byte_size_(0),
      chunk_count_high_mark_(0),
      chunk_count_low_mark_(0),
      used_(0),
      allocated_(0),
      allocated_base_(0),
      used_base_(0),
      nr_thread_cache_(0),
      nr_orphaned_thread_cache_(0),
      clock_(1)
{}

#ifdef DOUBLE_FREE_CHECK
inline int64_t ObObjFreeList::get_chunk_item_magic_idx(
    void* item, ObChunkInfo** chunk_info, const bool do_check /* = false */)
{
  int64_t idx = 0;

  if (NULL == *chunk_info) {
    *chunk_info = get_chunk_info(item);
  }
  idx = (reinterpret_cast<int64_t>(item) - reinterpret_cast<int64_t>((*chunk_info)->head_)) / type_size_;
  if (do_check &&
      (idx >= obj_count_per_chunk_ ||
          (0 != (reinterpret_cast<int64_t>(item) - reinterpret_cast<int64_t>((*chunk_info)->head_)) % type_size_))) {
    _OB_LOG(ERROR,
        "Invalid address:%p, chunk_addr:%p, type_size:%ld, obj_count:%ld, idx:%ld",
        item,
        (*chunk_info)->head_,
        type_size_,
        obj_count_per_chunk_,
        idx);
  }

  return idx;
}

inline void ObObjFreeList::set_chunk_item_magic(ObChunkInfo* chunk_info, void* item)
{
  int64_t idx = 0;
  idx = get_chunk_item_magic_idx(item, &chunk_info);
  if (0 != chunk_info->item_magic_[idx]) {
    OB_LOG(ERROR, "the chunk is free, but the magic is not zero", "magic", chunk_info->item_magic_[idx]);
  }
  chunk_info->item_magic_[idx] = ITEM_MAGIC;
}

inline void ObObjFreeList::clear_chunk_item_magic(ObChunkInfo* chunk_info, void* item)
{
  int64_t idx = 0;
  idx = get_chunk_item_magic_idx(item, &chunk_info, true);
  if (ITEM_MAGIC != chunk_info->item_magic_[idx]) {
    OB_LOG(ERROR,
        "the chunk is used, but without the right magic",
        "actual_magic",
        chunk_info->item_magic_[idx],
        "expected_magic",
        ITEM_MAGIC);
  }
  chunk_info->item_magic_[idx] = 0;
}
#else
inline void ObObjFreeList::set_chunk_item_magic(ObChunkInfo* chunk_info, void* item)
{
  UNUSED(chunk_info);
  UNUSED(item);
}

inline void ObObjFreeList::clear_chunk_item_magic(ObChunkInfo* chunk_info, void* item)
{
  UNUSED(chunk_info);
  UNUSED(item);
}
#endif

inline ObChunkInfo* ObObjFreeList::chunk_create(ObThreadCache* thread_cache)
{
  void* chunk_addr = NULL;
  void* curr = NULL;
  void* next = NULL;
  ObChunkInfo* chunk_info = NULL;

  if (NULL == (chunk_addr = ob_malloc_align(alignment_, chunk_byte_size_, label_))) {
    OB_LOG(ERROR, "failed to allocate chunk", K_(chunk_byte_size));
  } else {
    chunk_info = new (reinterpret_cast<char*>(chunk_addr) + type_size_ * obj_count_per_chunk_) ObChunkInfo();
#ifdef DOUBLE_FREE_CHECK
    memset(chunk_info->item_magic_, 0, chunk_byte_size_ - type_size_ * obj_count_per_chunk_ - sizeof(ObChunkInfo));
#endif
    chunk_info->tid_ = pthread_self();
    chunk_info->head_ = chunk_addr;
    chunk_info->type_size_ = type_size_;
    chunk_info->obj_count_ = obj_count_per_chunk_;
    chunk_info->length_ = chunk_byte_size_;
    chunk_info->allocated_ = 0;
    chunk_info->thread_cache_ = thread_cache;
    chunk_info->link_ = Link<ObChunkInfo>();

    curr = chunk_info->head_;
    chunk_info->inner_free_list_ = curr;
    for (int64_t i = 1; i < obj_count_per_chunk_; i++) {
      next = reinterpret_cast<void*>(reinterpret_cast<char*>(curr) + type_size_);
      *reinterpret_cast<void**>(curr) = next;
      curr = next;
    }
    *reinterpret_cast<void**>(curr) = NULL;

    (void)ATOMIC_FAA(&allocated_, obj_count_per_chunk_);
    (void)ATOMIC_FAA(&ObObjFreeListList::get_freelists().mem_total_, chunk_byte_size_);

    thread_cache->nr_chunks_++;
    thread_cache->nr_total_ += obj_count_per_chunk_;
    (void)ATOMIC_FAA(&thread_cache->nr_free_, obj_count_per_chunk_);
  }

  return chunk_info;
}

inline void ObObjFreeList::chunk_delete(ObThreadCache* thread_cache, ObChunkInfo* chunk_info)
{
  void* chunk_addr = chunk_info->head_;
  if (OB_UNLIKELY(0 != chunk_info->allocated_)) {
    OB_LOG(ERROR, "the chunk allocated size isn't 0 when it deleting", K(chunk_info->allocated_));
  }
  thread_cache->nr_chunks_--;
  thread_cache->nr_total_ -= obj_count_per_chunk_;
  (void)ATOMIC_FAA(&thread_cache->nr_free_, -obj_count_per_chunk_);
  ob_free_align(chunk_addr);
  (void)ATOMIC_FAA(&allocated_, -obj_count_per_chunk_);
  (void)ATOMIC_FAA(&ObObjFreeListList::get_freelists().mem_total_, -chunk_byte_size_);
}

inline void* ObObjFreeList::alloc_from_chunk(ObChunkInfo* chunk_info)
{
  void* item = NULL;

  if (NULL != (item = chunk_info->inner_free_list_)) {
    chunk_info->inner_free_list_ = *reinterpret_cast<void**>(item);
    get_chunk_info(item) = chunk_info;
    chunk_info->allocated_++;
  }

  return item;
}

inline void ObObjFreeList::free_to_chunk(ObThreadCache* thread_cache, void* item)
{
  ObChunkInfo* chunk_info = NULL;

  chunk_info = get_chunk_info(item);
  chunk_info->allocated_--;

  *reinterpret_cast<void**>(item) = chunk_info->inner_free_list_;
  chunk_info->inner_free_list_ = item;

  if (0 == chunk_info->allocated_) {
    if (chunk_info == thread_cache->active_chunk_) {
      thread_cache->active_chunk_ = NULL;
    } else if (1 == obj_count_per_chunk_) {
      // The following chunk's conditional is necessary because if the
      // chunk only contains one object, then it never gets inserted
      // into the non-full chunk list.
      thread_cache->full_chunk_list_.remove(chunk_info);
    } else {
      thread_cache->non_full_chunk_list_.remove(chunk_info);
    }

    // keep enough object thread cache, if one object is in one chunk, we free this chunk
    if (thread_cache->nr_chunks_ > chunk_count_high_mark_ ||
        thread_cache->nr_chunks_ * chunk_byte_size_ > THREAD_CACHE_TOTAL_SIZE) {
      chunk_delete(thread_cache, chunk_info);
      // flush more large object
      if (1 == obj_count_per_chunk_) {
        // must meet both size and number of cached objects constrains
        while (thread_cache->nr_chunks_ * chunk_byte_size_ > THREAD_CACHE_TOTAL_SIZE ||
               thread_cache->nr_chunks_ > chunk_count_low_mark_) {
          // scanning from tail until meet conditions
          chunk_info = thread_cache->non_full_chunk_list_.tail_;
          if (NULL != chunk_info) {
            thread_cache->non_full_chunk_list_.remove(chunk_info);
            chunk_delete(thread_cache, chunk_info);
          } else {
            break;
          }
        }
      }
    } else {
      if (NULL == thread_cache->active_chunk_) {
        thread_cache->active_chunk_ = chunk_info;
      } else {
        thread_cache->non_full_chunk_list_.push(chunk_info);
      }
    }
  } else if (obj_count_per_chunk_ - 1 == chunk_info->allocated_ && chunk_info != thread_cache->active_chunk_) {
    thread_cache->full_chunk_list_.remove(chunk_info);
    thread_cache->non_full_chunk_list_.push(chunk_info);
  }
}

inline void* ObObjFreeList::alloc_from_cache(ObThreadCache* thread_cache)
{
  void* ret = NULL;

  if (NULL != thread_cache->active_chunk_ && NULL != (ret = alloc_from_chunk(thread_cache->active_chunk_))) {
    // do nothing
  } else {
    if (NULL != thread_cache->active_chunk_) {
      thread_cache->full_chunk_list_.push(thread_cache->active_chunk_);
      thread_cache->active_chunk_ = NULL;
    }
    thread_cache->active_chunk_ = thread_cache->non_full_chunk_list_.pop();

    if (NULL != thread_cache->active_chunk_ && NULL != (ret = alloc_from_chunk(thread_cache->active_chunk_))) {
      // do nothing
    }
  }

  return ret;
}

inline void* ObObjFreeList::privatize_public_freelist(ObThreadCache* thread_cache, const bool alloc)
{
  void* ret = NULL;
  ObFreeObject* item = NULL;

  if (NULL != thread_cache->outer_free_list_.head() &&
      NULL != (item = reinterpret_cast<ObFreeObject*>(thread_cache->outer_free_list_.popall()))) {
    SLL<ObFreeObject> org_list;
    SLL<ObFreeObject> invert_list;
    org_list.head_ = item;
    if (alloc) {
      ret = org_list.pop();
    }
    while (NULL != (item = org_list.pop())) {
      invert_list.push(item);
    }
    while (NULL != (item = invert_list.pop())) {
      free_to_chunk(thread_cache, item);
    }
  }

  return ret;
}

inline void* ObObjFreeList::alloc_from_public_freelist(ObThreadCache* thread_cache)
{
  return privatize_public_freelist(thread_cache, true);
}

inline void* ObObjFreeList::alloc_from_orphaned_thread_cache(ObThreadCache* thread_cache)
{
  int ret = OB_SUCCESS;
  void* item = NULL;
  ObThreadCache* orphaned_thread_cache = NULL;
  if (nr_orphaned_thread_cache_ > 0) {
    if (OB_FAIL(mutex_acquire(&lock_))) {
      OB_LOG(WARN, "failed to lock of freelist", K(name_));
    } else {
      if (nr_orphaned_thread_cache_ > 0) {
        orphaned_thread_cache = orphaned_thread_cache_list_.pop();
        nr_orphaned_thread_cache_--;
      }
      mutex_release(&lock_);

      if (NULL != orphaned_thread_cache) {
        privatize_thread_cache(thread_cache, orphaned_thread_cache);
        tc_allocator_->free_void(orphaned_thread_cache);
        item = alloc_from_cache(thread_cache);
      }
    }
  }
  return item;
}

inline void* ObObjFreeList::alloc_from_new_chunk(ObThreadCache* thread_cache)
{
  void* ret = NULL;
  ObChunkInfo* chunk_info = NULL;

  if (NULL != (chunk_info = chunk_create(thread_cache))) {
    if (NULL != thread_cache->active_chunk_) {
      thread_cache->full_chunk_list_.push(thread_cache->active_chunk_);
      thread_cache->active_chunk_ = NULL;
    }
    thread_cache->active_chunk_ = chunk_info;

    if (NULL != (ret = alloc_from_chunk(thread_cache->active_chunk_))) {
      // do nothing
    }
  }

  return ret;
}

int ObObjFreeList::init(const char* name, const int64_t obj_size, const int64_t obj_count,
    const int64_t alignment /* = 16 */, const ObMemCacheType cache_type /* = OP_RECLAIM */)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice");
  } else if (OB_ISNULL(name) || OB_UNLIKELY(obj_size <= 0) || OB_UNLIKELY(obj_count <= 0) ||
             OB_UNLIKELY(obj_count < 0) || OB_UNLIKELY(alignment <= 0) ||
             OB_UNLIKELY(alignment > OB_MALLOC_NORMAL_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(name), K(obj_size), K(obj_count), K(alignment));
  } else if (OB_FAIL(obj_free_list_.init(name, 0))) {
    OB_LOG(WARN, "failed to initialize atomic free list", K(name));
  } else if (OB_FAIL(mutex_init(&lock_))) {
    OB_LOG(WARN, "failed to init mutex");
  } else {
    alignment_ = alignment;
    obj_count_base_ = (OP_GLOBAL == cache_type) ? 0 : obj_count;
    type_size_base_ = obj_size;
    only_global_ = (OP_RECLAIM != cache_type);
    name_ = name;

    // Make sure we align *all* the objects in the allocation,
    // not just the first one, each chunk with an extra
    // pointer to store the pointer of chunk info
    if (OB_UNLIKELY(only_global_)) {
      type_size_ = upper_round(obj_size, alignment);
    } else {
      type_size_ = upper_round(obj_size + sizeof(void*), alignment);
    }

    const int64_t ob_malloc_header_size = 32;  // reserved size for ob_malloc header
    int64_t meta_size = only_global_ ? ob_malloc_header_size : sizeof(ObChunkInfo) + ob_malloc_header_size;
    int64_t real_type_size = type_size_;
#ifdef DOUBLE_FREE_CHECK
    // enlarge type_size to hold a item_magic
    real_type_size = type_size_ + sizeof(uint8_t);
#endif

    // find fit chunk size
    // object_size < 128K, one chunk with multiply object; object_size > 128K, one chunk with one object
    int64_t min_chunk_size = upper_round(real_type_size + meta_size, alignment);
    int64_t low_mark_chunk_size = upper_round(real_type_size * THREAD_CACHE_LOW_MARK + meta_size, alignment);
    if (low_mark_chunk_size <= OB_MALLOC_MIDDLE_BLOCK_SIZE * THREAD_CACHE_LOW_MARK * 2) {
      chunk_byte_size_ = upper_round(low_mark_chunk_size, OB_MALLOC_NORMAL_BLOCK_SIZE);
      obj_count_per_chunk_ = (chunk_byte_size_ - meta_size) / real_type_size;
    } else {
      OB_LOG(INFO, "big object pool to initialize", K(obj_size));
      obj_count_per_chunk_ = 1;
      if (min_chunk_size > OB_MALLOC_BIG_BLOCK_SIZE) {
        chunk_byte_size_ = min_chunk_size;
      } else {
        chunk_byte_size_ = upper_round(min_chunk_size, OB_MALLOC_NORMAL_BLOCK_SIZE);
      }
    }
    chunk_byte_size_ -= ob_malloc_header_size;

    int64_t low_mark_obj_count = std::min(obj_count, std::max(obj_count / 4, THREAD_CACHE_LOW_MARK));
    chunk_count_low_mark_ = (low_mark_obj_count + obj_count_per_chunk_ - 1) / obj_count_per_chunk_;
    chunk_count_high_mark_ = (obj_count_base_ + obj_count_per_chunk_ - 1) / obj_count_per_chunk_;

    used_ = 0;
    allocated_ = 0;
    allocated_base_ = 0;
    used_base_ = 0;

    nr_thread_cache_ = 0;
    nr_orphaned_thread_cache_ = 0;
    is_inited_ = true;
  }

  return ret;
}

inline void* ObObjFreeList::global_alloc()
{
  void* chunk_addr = NULL;
  void* item = NULL;
  void* next = NULL;
  bool break_loop = false;

  do {
    if (obj_free_list_.empty()) {
      if (NULL == (chunk_addr = ob_malloc_align(alignment_, chunk_byte_size_, label_))) {
        OB_LOG(ERROR, "failed to allocate chunk", K_(chunk_byte_size));
        break_loop = true;
      } else {
        (void)ATOMIC_FAA(&allocated_, obj_count_per_chunk_);
        (void)ATOMIC_FAA(&ObObjFreeListList::get_freelists().mem_total_, chunk_byte_size_);

        item = chunk_addr;
        break_loop = true;

        // free each of the new elements
        for (int64_t i = 1; i < obj_count_per_chunk_; i++) {
          next = reinterpret_cast<void*>(reinterpret_cast<char*>(item) + i * type_size_);
          obj_free_list_.push(next);
        }
      }
    } else if (NULL != (item = obj_free_list_.pop())) {
      break_loop = true;
    }
  } while (!break_loop);

  return item;
}

inline void ObObjFreeList::global_free(void* item)
{
  if (OB_LIKELY(NULL != item)) {
    obj_free_list_.push(item);
  }
}

ObThreadCache* ObObjFreeList::init_thread_cache()
{
  int ret = OB_SUCCESS;
  ObThreadCache* thread_cache = NULL;

  if (NULL == (thread_cache = reinterpret_cast<ObThreadCache*>(tc_allocator_->alloc_void()))) {
    OB_LOG(ERROR, "failed to allocate memory for thread cache");
  } else {
    new (thread_cache) ObThreadCache();
    thread_cache->inner_free_list_ = SLL<ObFreeObject>();
    thread_cache->f_ = this;
    thread_cache->active_chunk_ = NULL;
    thread_cache->full_chunk_list_ = DLL<ObChunkInfo>();
    thread_cache->non_full_chunk_list_ = Queue<ObChunkInfo>();
    thread_cache->clock_ = UINT64_MAX;

    // this lock will only be accessed when initializing
    // thread cache, so it won't damage performance
    if (OB_FAIL(mutex_acquire(&lock_))) {
      OB_LOG(WARN, "failed to lock of freelist", K(name_));
    } else if (OB_FAIL(thread_cache->outer_free_list_.init(name_, 0))) {
      mutex_release(&lock_);
      OB_LOG(WARN, "failed to init outer freelist", K(name_));
    } else {
      get_thread_cache(thread_cache_idx_) = thread_cache;
      thread_cache_list_.push(thread_cache);
      nr_thread_cache_++;
      mutex_release(&lock_);
    }
  }

  return thread_cache;
}

int ObObjFreeList::destroy_cur_thread_cache()
{
  int ret = OB_SUCCESS;
  ObChunkInfo* chunk_info = NULL;
  ObThreadCache* cur_thread_cache = NULL;
  void* item = NULL;

  // unregister thread cahce
  if (!OB_ISNULL(cur_thread_cache = get_thread_cache(thread_cache_idx_))) {
    if (OB_FAIL(mutex_acquire(&lock_))) {
      OB_LOG(WARN, "failed to lock of freelist", K(name_));
    } else {
      thread_cache_list_.remove(cur_thread_cache);
      nr_thread_cache_--;
      mutex_release(&lock_);

      if (only_global_) {
        while (NULL != (item = cur_thread_cache->inner_free_list_.pop())) {
          obj_free_list_.push(item);
          cur_thread_cache->nr_free_--;
        }
        tc_allocator_->free_void(cur_thread_cache);
      } else {
        if (NULL != cur_thread_cache->outer_free_list_.head()) {
          privatize_public_freelist(cur_thread_cache, false);
        }
        if (NULL != (chunk_info = cur_thread_cache->active_chunk_)) {
          if (0 == chunk_info->allocated_) {
            chunk_delete(cur_thread_cache, chunk_info);
            cur_thread_cache->active_chunk_ = NULL;
          }
        }

        if (0 == cur_thread_cache->nr_chunks_) {
          tc_allocator_->free_void(cur_thread_cache);
        } else {
          if (OB_FAIL(mutex_acquire(&lock_))) {
            OB_LOG(WARN, "failed to lock of freelist", K(name_));
          } else {
            orphaned_thread_cache_list_.push(cur_thread_cache);
            nr_orphaned_thread_cache_++;
            mutex_release(&lock_);
          }
        }
      }
    }
  }

  return ret;
}

inline void ObObjFreeList::privatize_thread_cache(ObThreadCache* cur_thread_cache, ObThreadCache* src_thread_cache)
{
  ObChunkInfo* chunk_info = NULL;

  // modify the owner thread cache of the chunks in source thread cache
  if (NULL != (chunk_info = src_thread_cache->active_chunk_)) {
    ATOMIC_STORE(&chunk_info->thread_cache_, cur_thread_cache);
  }
  chunk_info = src_thread_cache->full_chunk_list_.head_;
  while (NULL != chunk_info) {
    ATOMIC_STORE(&chunk_info->thread_cache_, cur_thread_cache);
    chunk_info = src_thread_cache->full_chunk_list_.next(chunk_info);
  }
  chunk_info = src_thread_cache->non_full_chunk_list_.head_;
  while (NULL != chunk_info) {
    ATOMIC_STORE(&chunk_info->thread_cache_, cur_thread_cache);
    chunk_info = src_thread_cache->non_full_chunk_list_.next(chunk_info);
  }

  // after modifying the owner of chunks, wait until no other threads use the old owner thread cache
  rcu_sync();

  // nobody uses the src thread cache, current thread cache can private it
  if (NULL != src_thread_cache->outer_free_list_.head()) {
    privatize_public_freelist(src_thread_cache, false);
  }
  if (NULL != (chunk_info = src_thread_cache->active_chunk_)) {
    if (0 == chunk_info->allocated_) {
      chunk_delete(src_thread_cache, chunk_info);
      src_thread_cache->active_chunk_ = NULL;
    } else if (chunk_info->allocated_ == obj_count_per_chunk_) {
      cur_thread_cache->full_chunk_list_.push(chunk_info);
    } else {
      cur_thread_cache->non_full_chunk_list_.push(chunk_info);
    }
  }
  while (NULL != (chunk_info = src_thread_cache->full_chunk_list_.pop())) {
    cur_thread_cache->full_chunk_list_.push(chunk_info);
  }
  while (NULL != (chunk_info = src_thread_cache->non_full_chunk_list_.pop())) {
    cur_thread_cache->non_full_chunk_list_.push(chunk_info);
  }

  // merge the stat to current thread cache
  cur_thread_cache->nr_chunks_ += src_thread_cache->nr_chunks_;
  cur_thread_cache->nr_total_ += src_thread_cache->nr_total_;
  (void)ATOMIC_FAA(&cur_thread_cache->nr_malloc_, src_thread_cache->nr_malloc_);
  (void)ATOMIC_FAA(&cur_thread_cache->nr_free_, src_thread_cache->nr_free_);
}

inline void* ObObjFreeList::reclaim_alloc(ObThreadCache* thread_cache)
{
  void* ret = NULL;

  if (OB_LIKELY(NULL != thread_cache)) {
    // get from local chunk freelist first
    if (NULL != (ret = alloc_from_cache(thread_cache))) {
      // do nothing
    }
    // fetch memory from outer_free_list
    else if (NULL != (ret = alloc_from_public_freelist(thread_cache))) {
      // do nothing
    }
    // fetch memory from orphaned thread cache
    else if (NULL != (ret = alloc_from_orphaned_thread_cache(thread_cache))) {
      // do nothing
    }
    // add new chunk
    else if (NULL != (ret = alloc_from_new_chunk(thread_cache))) {
      // do nothing
    }

    if (NULL != ret) {
      (void)ATOMIC_FAA(&thread_cache->nr_free_, -1);
      (void)ATOMIC_FAA(&thread_cache->nr_malloc_, 1);
      set_chunk_item_magic(NULL, ret);
    }
  }

  return ret;
}

inline void* ObObjFreeList::tc_alloc(ObThreadCache* thread_cache)
{
  void* ret = NULL;
  if (OB_LIKELY(NULL != thread_cache)) {
    if (NULL != (ret = thread_cache->inner_free_list_.pop())) {
      thread_cache->nr_free_--;
      thread_cache->nr_malloc_++;
    } else if (only_global_) {
      if (NULL != (ret = global_alloc())) {
        thread_cache->nr_malloc_++;
      }
    }
  }
  return ret;
}

void* ObObjFreeList::alloc()
{
  void* ret = NULL;
  ObThreadCache* thread_cache = NULL;

  // no thread cache, create it
  if (OB_ISNULL(thread_cache = get_thread_cache(thread_cache_idx_))) {
    if (OB_ISNULL(thread_cache = init_thread_cache())) {
      OB_LOG(ERROR, "failed to init object free list thread cache");
      ret = NULL;  // allocate failed
    }
  }

  if (only_global_) {
    ret = tc_alloc(thread_cache);
  } else {
    ret = reclaim_alloc(thread_cache);
  }

  return ret;
}

inline void ObObjFreeList::reclaim_free(ObThreadCache* cur_thread_cache, void* item)
{
  ObChunkInfo* chunk_info = NULL;
  ObThreadCache* thread_cache = NULL;

  chunk_info = get_chunk_info(item);
  clear_chunk_item_magic(chunk_info, item);
  rcu_read_lock(cur_thread_cache);
  thread_cache = ATOMIC_LOAD(&chunk_info->thread_cache_);
  (void)ATOMIC_FAA(&thread_cache->nr_free_, 1);
  (void)ATOMIC_FAA(&thread_cache->nr_malloc_, -1);
  if (thread_cache == cur_thread_cache) {
    thread_cache->f_->free_to_chunk(thread_cache, item);
    if (NULL != thread_cache->outer_free_list_.head()) {
      privatize_public_freelist(thread_cache, false);
    }
  } else {
    thread_cache->outer_free_list_.push(item);
  }
  rcu_read_unlock(cur_thread_cache);
}

inline void ObObjFreeList::tc_free(ObThreadCache* cur_thread_cache, void* item)
{
  if (obj_count_base_ > 0) {
    // free all thread cache obj upto global free list if it's overflow
    if (OB_UNLIKELY(cur_thread_cache->nr_free_ >= obj_count_base_)) {
      void* next = NULL;
      obj_free_list_.push(item);
      // keep half of obj_count_base_
      int64_t low_watermark = obj_count_base_ / 2;
      while (cur_thread_cache->nr_free_ > low_watermark && NULL != (next = cur_thread_cache->inner_free_list_.pop())) {
        obj_free_list_.push(next);
        cur_thread_cache->nr_free_--;
      }
    } else {
      cur_thread_cache->inner_free_list_.push(reinterpret_cast<ObFreeObject*>(item));
      cur_thread_cache->nr_free_++;
    }
  } else {
    global_free(item);
  }

  /**
   * For global allocate mode, maybe thread A allocates memory and thread B frees it.
   * So when thread B frees, B's thread cache maybe NULL. The thread_cache->nr_malloc_
   * isn't the actual malloc number of this thread, maybe it's negative.
   */
  cur_thread_cache->nr_malloc_--;
}

void ObObjFreeList::free(void* item)
{
  ObThreadCache* thread_cache = NULL;
  if (OB_LIKELY(NULL != item)) {
    // no thread cache, create it
    if (OB_ISNULL(thread_cache = get_thread_cache(thread_cache_idx_))) {
      if (OB_ISNULL(thread_cache = init_thread_cache())) {
        OB_LOG(ERROR, "failed to init object free list thread cache");
      }
    }

    if (OB_LIKELY(NULL != thread_cache)) {
      if (only_global_) {
        tc_free(thread_cache, item);
      } else {
        reclaim_free(thread_cache, item);
      }
    }
  }
}

inline void ObObjFreeList::update_used()
{
  int64_t used = 0;
  mutex_acquire(&lock_);
  ObThreadCache* next_thread_cache = thread_cache_list_.head_;
  while (NULL != next_thread_cache) {
    used += next_thread_cache->nr_malloc_;
    next_thread_cache = thread_cache_list_.next(next_thread_cache);
  }
  mutex_release(&lock_);
  used_ = used;
}

ObObjFreeListList::ObObjFreeListList() : tc_meta_allocator_(ObModIds::OB_CONCURRENCY_OBJ_POOL)
{
  mem_total_ = 0;
  nr_freelists_ = 0;
  is_inited_ = false;
}

int ObObjFreeListList::create_freelist(ObObjFreeList*& freelist, const char* name, const int64_t obj_size,
    const int64_t obj_count, const int64_t alignment /* = 16 */, const ObMemCacheType cache_type /* = OP_RECLAIM */,
    const bool is_meta /* = false */, const lib::ObLabel& label)
{
  int ret = OB_SUCCESS;
  freelist = NULL;

  if (OB_ISNULL(name) || OB_UNLIKELY(obj_size <= 0) || OB_UNLIKELY(obj_count < 0) || OB_UNLIKELY(alignment <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(name), K(obj_size), K(obj_count), K(alignment));
  } else if (!is_meta && OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "global object freelist list is not initialized success");
  } else if (nr_freelists_ >= ObObjFreeListList::MAX_NUM_FREELIST) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(ERROR,
        "can't allocate object freelist, freelist is full",
        "max_freelist_num",
        ObObjFreeListList::MAX_NUM_FREELIST,
        K(ret));
  } else {
    if (OB_UNLIKELY(is_meta)) {
      freelist = new (std::nothrow) ObObjFreeList();
    } else {
      freelist = fl_allocator_.alloc();
    }
    if (OB_ISNULL(freelist)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "failed to allocate memory for freelist", K(ret));
    } else {
      freelist->thread_cache_idx_ = get_next_free_slot();
      if (freelist->thread_cache_idx_ >= ObObjFreeListList::MAX_NUM_FREELIST) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(ERROR,
            "can't allocate object freelist, freelist is full",
            "max_freelist_num",
            ObObjFreeListList::MAX_NUM_FREELIST,
            K(ret));
        if (OB_UNLIKELY(is_meta)) {
          delete freelist;
        } else {
          fl_allocator_.free(freelist);
        }
        freelist = NULL;
      } else {
        freelist->label_ = label;
        freelist->freelists_ = &freelists_;
        freelists_.push(freelist);
        if (OB_UNLIKELY(is_meta)) {
          freelist->tc_allocator_ = &tc_meta_allocator_;
        } else {
          freelist->tc_allocator_ = &tc_allocator_;
        }
        if (OB_FAIL(freelist->init(name, obj_size, obj_count, alignment, cache_type))) {
          OB_LOG(WARN, "failed to initialize object freelist", K(name), K(obj_size), K(obj_count), K(alignment));
        }
      }
    }
  }

  return ret;
}

void ObObjFreeListList::snap_baseline()
{
  ObAtomicSLL<ObObjFreeList>& fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList* fl = fll.head();
  while (NULL != fl) {
    fl->allocated_base_ = fl->allocated_;
    fl->used_base_ = fl->used_;
    fl = fll.next(fl);
  }
}

void ObObjFreeListList::dump_baselinerel()
{
  int ret = OB_SUCCESS;
  ObAtomicSLL<ObObjFreeList>& fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList* fl = fll.head();
  int64_t pos = 0;
  int64_t len = OB_MALLOC_NORMAL_BLOCK_SIZE;
  char* buf = reinterpret_cast<char*>(ob_malloc(len, ObModIds::OB_CONCURRENCY_OBJ_POOL));
  int64_t alloc_size = 0;

  if (NULL != buf) {
    databuff_printf(
        buf, len, pos, "\n     allocated      |       in-use       |  count  | type size  |   free list name\n");
    databuff_printf(
        buf, len, pos, "  relative to base  |  relative to base  |         |            |                 \n");
    databuff_printf(buf,
        len,
        pos,
        "--------------------|--------------------|---------|------------|----------------------------------\n");

    while (NULL != fl && OB_SUCCESS == ret) {
      alloc_size = fl->allocated_ - fl->allocated_base_;
      if (0 != alloc_size) {
        fl->update_used();
        ret = databuff_printf(buf,
            len,
            pos,
            " %18ld | %18ld | %7ld | %10ld | memory/%s\n",
            (fl->allocated_ - fl->allocated_base_) * fl->type_size_,
            (fl->used_ - fl->used_base_) * fl->type_size_,
            fl->used_ - fl->used_base_,
            fl->type_size_,
            fl->name_ ? fl->name_ : "<unknown>");
      }
      fl = fll.next(fl);
    }
    _OB_LOG(INFO, "dump object freelist baseline relative:%s", buf);
    ob_free(buf);
  }
}

int ObObjFreeListList::get_info(ObVector<ObObjFreeList*>& flls)
{
  int ret = OB_SUCCESS;
  ObAtomicSLL<ObObjFreeList>& fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList* fl = fll.head();
  while (OB_SUCC(ret) && NULL != fl) {
    fl->update_used();
    if (OB_FAIL(flls.push_back(fl))) {
      OB_LOG(WARN, "failed to push back fl", K(ret));
    } else {
      fl = fll.next(fl);
    }
  }
  return ret;
}

void ObObjFreeListList::dump()
{
  char buf[8192];
  buf[sizeof(buf) - 1] = '\0';
  to_string(buf, sizeof(buf) - 1);
  _OB_LOG(INFO, "dump object freelist statistic:%s", buf);
}

int64_t ObObjFreeListList::to_string(char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  ObAtomicSLL<ObObjFreeList>& fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList* fl = fll.head();
  int64_t pos = 0;

  databuff_printf(
      buf, len, pos, "\n     allocated      |        in-use      | type size  | cache type |   free list name\n");
  databuff_printf(buf,
      len,
      pos,
      "--------------------|--------------------|------------|------------|----------------------------------\n");
  while (NULL != fl && OB_SUCC(ret)) {
    fl->update_used();
    ret = databuff_printf(buf,
        len,
        pos,
        " %'18ld | %'18ld | %'10ld | %10s | %s\n",
        fl->allocated_ * fl->type_size_,
        fl->used_ * fl->type_size_,
        fl->type_size_,
        fl->only_global_ ? (fl->obj_count_base_ > 0 ? "tc" : "global") : "reclaim",
        fl->name_ ? fl->name_ : "<unknown>");
    fl = fll.next(fl);
  }
  return pos;
}

}  // end of namespace common
}  // end of namespace oceanbase
