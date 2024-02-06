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

#ifndef _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_
#define _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_

#include <stdint.h>
#include <stdlib.h>
#include <functional>
#include <sys/mman.h>
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/abit_set.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"
#include "lib/lock/ob_mutex.h"
#include "lib/ob_lib_config.h"

namespace oceanbase
{
namespace lib
{

class ProtectedStackAllocator;
class ObMemoryCutter;
struct AChunk;

static const uint64_t MAXADDR = (1L << 52);
static const uint64_t CHUNK_BITMAP_SIZE = MAXADDR / MEMCHK_CHUNK_ALIGN;

// A stack style chunk list, support push and pop operations.
class AChunkList
{
  friend class ObMemoryCutter;
  DISALLOW_COPY_AND_ASSIGN(AChunkList);

public:
  static const int64_t DEFAULT_MAX_CHUNK_CACHE_SIZE = 1L<<30;
  AChunkList(const bool with_mutex = true)
    : max_chunk_cache_size_(DEFAULT_MAX_CHUNK_CACHE_SIZE),
      mutex_(common::ObLatchIds::ALLOC_CHUNK_LOCK),
      header_(NULL), hold_(0), pushes_(0), pops_(0), with_mutex_(with_mutex)
  {
    mutex_.enable_record_stat(false);
#ifdef OB_USE_ASAN
    max_chunk_cache_size_ = 0;
#endif
  }
  virtual ~AChunkList()
  {}

  void set_max_chunk_cache_size(const int64_t max_cache_size)
  {
#ifdef OB_USE_ASAN
    UNUSED(max_cache_size);
    max_chunk_cache_size_ = 0;
#else
    max_chunk_cache_size_ = max_cache_size;
#endif
  }

  inline bool push(AChunk *chunk)
  {
    bool bret = false;
    ObDisableDiagnoseGuard disable_diagnose_guard;
    if (with_mutex_) {
      mutex_.lock();
    }
    DEFER(if (with_mutex_) {mutex_.unlock();});
    int64_t hold = chunk->hold();
    if (hold_ + hold <= max_chunk_cache_size_) {
      hold_ += hold;
      pushes_++;
      if (NULL == header_) {
        chunk->prev_ = chunk;
        chunk->next_ = chunk;
        header_ = chunk;
      } else {
        chunk->prev_ = header_->prev_;
        chunk->next_ = header_;
        chunk->prev_->next_ = chunk;
        chunk->next_->prev_ = chunk;
      }
      bret = true;
    }
    return bret;
  }
  inline AChunk *pop()
  {
    AChunk *chunk = NULL;
    if (!OB_ISNULL(header_)) {
      ObDisableDiagnoseGuard disable_diagnose_guard;
      if (with_mutex_) {
        mutex_.lock();
      }
      DEFER(if (with_mutex_) {mutex_.unlock();});
      if (!OB_ISNULL(header_)) {
        chunk = header_;
        hold_ -= chunk->hold();
        pops_++;
        if (header_->next_ != header_) {
          header_->prev_->next_ = header_->next_;
          header_->next_->prev_ = header_->prev_;
          header_ = header_->next_;
        } else {
          header_ = NULL;
        }
      }
    }
    return chunk;
  }

  inline int64_t count() const
  {
    return pushes_ - pops_;
  }

  inline int64_t hold() const
  {
    return hold_;
  }

  inline int64_t get_pushes() const
  {
    return pushes_;
  }

  inline int64_t get_pops() const
  {
    return pops_;
  }

private:
  int64_t max_chunk_cache_size_;
  ObMutex mutex_;
  AChunk *header_;
  int64_t hold_;
  int64_t pushes_;
  int64_t pops_;
  const bool with_mutex_;
}; // end of class AChunkList

const char *const use_large_pages_confs[] =
{
  "true",
  "false",
  "only"
};

class ObLargePageHelper
{
public:
  static const int INVALID_LARGE_PAGE_TYPE = -1;
  static const int NO_LARGE_PAGE = 0;
  static const int PREFER_LARGE_PAGE = 1;
  static const int ONLY_LARGE_PAGE = 2;
public:
  static void set_param(const char *param);
  static int get_type();
private:
  static int large_page_type_;
};

class AChunkMgr
{
  friend class ProtectedStackAllocator;
  friend class ObMemoryCutter;
private:
  static constexpr int64_t DEFAULT_LIMIT = 4L << 30;  // 4GB
  static constexpr int64_t ACHUNK_ALIGN_SIZE = INTACT_ACHUNK_SIZE;
  static constexpr int64_t NORMAL_ACHUNK_SIZE = INTACT_ACHUNK_SIZE;
  static constexpr int64_t LARGE_ACHUNK_SIZE = INTACT_ACHUNK_SIZE << 1;
public:
  static constexpr int64_t DEFAULT_LARGE_CHUNK_CACHE_SIZE = 128L << 20;
  static AChunkMgr &instance();

public:
  AChunkMgr();

  AChunk *alloc_chunk(
      const uint64_t size = ACHUNK_SIZE,
      bool high_prio = false);
  void free_chunk(AChunk *chunk);
  AChunk *alloc_co_chunk(const uint64_t size = ACHUNK_SIZE);
  void free_co_chunk(AChunk *chunk);
  static OB_INLINE uint64_t aligned(const uint64_t size);
  static OB_INLINE uint64_t hold(const uint64_t size);
  void set_max_chunk_cache_size(const int64_t max_cache_size)
  { free_list_.set_max_chunk_cache_size(max_cache_size); }
  void set_max_large_chunk_cache_size(const int64_t max_cache_size)
  { large_free_list_.set_max_chunk_cache_size(max_cache_size); }

  inline static AChunk *ptr2chunk(const void *ptr);
  bool update_hold(int64_t bytes, bool high_prio);
  virtual int madvise(void *addr, size_t length, int advice);

  inline void set_limit(int64_t limit);
  inline int64_t get_limit() const;
  inline void set_urgent(int64_t limit);
  inline int64_t get_urgent() const;
  inline int64_t get_hold() const;
  inline int64_t get_total_hold() const { return ATOMIC_LOAD(&total_hold_); }
  inline int64_t get_used() const;
  inline int64_t get_free_chunk_count() const;
  inline int64_t get_free_chunk_pushes() const;
  inline int64_t get_free_chunk_pops() const;
  inline int64_t get_freelist_hold() const;
  inline int64_t get_large_freelist_hold() const;
  inline int64_t get_maps()  { return maps_; }
  inline int64_t get_unmaps()  { return unmaps_; }
  inline int64_t get_large_maps()  { return large_maps_; }
  inline int64_t get_large_unmaps()  { return large_unmaps_; }
  inline int64_t get_huge_maps()  { return huge_maps_; }
  inline int64_t get_huge_unmaps()  { return huge_unmaps_; }
  inline int64_t get_shadow_hold() const { return ATOMIC_LOAD(&shadow_hold_); }

private:
  typedef ABitSet ChunkBitMap;

private:
  void *direct_alloc(const uint64_t size, const bool can_use_huge_page, bool &huge_page_used, const bool alloc_shadow);
  void direct_free(const void *ptr, const uint64_t size);
  // wrap for mmap
  void *low_alloc(const uint64_t size, const bool can_use_huge_page, bool &huge_page_used, const bool alloc_shadow);
  void low_free(const void *ptr, const uint64_t size);

protected:
  AChunkList free_list_;
  AChunkList large_free_list_;
  ChunkBitMap *chunk_bitmap_;

  int64_t limit_;
  int64_t urgent_;
  int64_t hold_; // Including the memory occupied by free_list, limited by memory_limit
  int64_t total_hold_; // Including virtual memory, just for statifics.

  int64_t maps_;
  int64_t unmaps_;
  int64_t large_maps_;
  int64_t large_unmaps_;
  int64_t huge_maps_;
  int64_t huge_unmaps_;
  int64_t shadow_hold_;
}; // end of class AChunkMgr

OB_INLINE AChunk *AChunkMgr::ptr2chunk(const void *ptr)
{
  return AChunk::ptr2chunk(ptr);
}

OB_INLINE uint64_t AChunkMgr::aligned(const uint64_t size)
{
  return AChunk::aligned(size);
}

OB_INLINE uint64_t AChunkMgr::hold(const uint64_t size)
{
  return AChunk::calc_hold(size);
}

inline void AChunkMgr::set_limit(int64_t limit)
{
  limit_ = limit;
}

inline int64_t AChunkMgr::get_limit() const
{
  return limit_;
}

inline void AChunkMgr::set_urgent(int64_t urgent)
{
  urgent_ = urgent;
}

inline int64_t AChunkMgr::get_urgent() const
{
  return urgent_;
}

inline int64_t AChunkMgr::get_hold() const
{
  return hold_;
}

inline int64_t AChunkMgr::get_used() const
{
  return hold_ - get_freelist_hold() - get_large_freelist_hold();
}

inline int64_t AChunkMgr::get_free_chunk_count() const
{
  return free_list_.count();
}

inline int64_t AChunkMgr::get_free_chunk_pushes() const
{
  return free_list_.get_pushes();
}

inline int64_t AChunkMgr::get_free_chunk_pops() const
{
  return free_list_.get_pops();
}

inline int64_t AChunkMgr::get_freelist_hold() const
{
  return free_list_.hold();
}

inline int64_t AChunkMgr::get_large_freelist_hold() const
{
  return large_free_list_.hold();
}

} // end of namespace lib
} // end of namespace oceanbase

#define CHUNK_MGR (oceanbase::lib::AChunkMgr::instance())



#endif /* _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_ */
