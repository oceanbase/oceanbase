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

namespace oceanbase {
namespace lib {

class ObMemoryCutter;
struct AChunk;

static const uint64_t MAXADDR = (1L << 52);
static const uint64_t CHUNK_BITMAP_SIZE = MAXADDR / MEMCHK_CHUNK_ALIGN;

// A stack style chunk list, support push and pop operations.
class AChunkList {
  friend class ObMemoryCutter;
  DISALLOW_COPY_AND_ASSIGN(AChunkList);

public:
  static const int DEFAULT_MAX_CHUNK_CACHE_CNT = 500;
  AChunkList(const bool with_mutex = true)
      : max_chunk_cache_cnt_(DEFAULT_MAX_CHUNK_CACHE_CNT),
        mutex_(common::ObLatchIds::ALLOC_CHUNK_LOCK),
        header_(NULL),
        pushes_(0),
        pops_(0),
        with_mutex_(with_mutex)
  {}
  virtual ~AChunkList()
  {}

  void set_max_chunk_cache_cnt(const int cnt)
  {
    max_chunk_cache_cnt_ = cnt;
  }

  inline bool push(AChunk* chunk)
  {
    bool bret = false;
    if (count() < max_chunk_cache_cnt_) {
      if (with_mutex_) {
        mutex_.lock();
      }
      if (count() < max_chunk_cache_cnt_) {
        pushes_++;
        chunk->next_ = header_;
        header_ = chunk;
        bret = true;
      }
      if (with_mutex_) {
        mutex_.unlock();
      }
    }
    return bret;
  }
  inline AChunk* pop()
  {
    AChunk* chunk = NULL;
    if (!OB_ISNULL(header_)) {
      if (with_mutex_) {
        mutex_.lock();
      }
      if (!OB_ISNULL(header_)) {
        chunk = header_;
        pops_++;
        header_ = header_->next_;
      }
      if (with_mutex_) {
        mutex_.unlock();
      }
    }
    return chunk;
  }

  inline int64_t count() const
  {
    return pushes_ - pops_;
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
  int32_t max_chunk_cache_cnt_;
  ObMutex mutex_;
  AChunk* header_;
  int64_t pushes_;
  int64_t pops_;
  const bool with_mutex_;
};  // end of class AChunkList

const char* const use_large_pages_confs[] = {"true", "false", "only"};

class ObLargePageHelper {
public:
  static const int INVALID_LARGE_PAGE_TYPE = -1;
  static const int NO_LARGE_PAGE = 0;
  static const int PREFER_LARGE_PAGE = 1;
  static const int ONLY_LARGE_PAGE = 2;

public:
  static void set_param(const char* param);
  static int get_type();

private:
  static int large_page_type_;
};

class AChunkMgr {
  friend class ObMemoryCutter;

private:
  static const int64_t DEFAULT_LIMIT = 4L << 30;           // 4GB
  static const int64_t LARGE_CHUNK_FREE_SPACE = 2L << 30;  // 2GB

public:
#if MEMCHK_LEVEL >= 1
  static const uint64_t ALIGN_SIZE = MEMCHK_CHUNK_ALIGN;
#else
  static const uint64_t ALIGN_SIZE = INTACT_ACHUNK_SIZE;
#endif

  static AChunkMgr& instance();

public:
  AChunkMgr();

  AChunk* alloc_chunk(const uint64_t size = ACHUNK_SIZE, bool high_prio = false);
  void free_chunk(AChunk* chunk);
  static OB_INLINE uint64_t aligned(const uint64_t size);
  void set_max_chunk_cache_cnt(const int cnt)
  {
    free_list_.set_max_chunk_cache_cnt(cnt);
  }

#if MEMCHK_LEVEL >= 1
  inline const AChunk* ptr2chunk(const void* ptr);
#endif

  inline void set_limit(int64_t limit);
  inline int64_t get_limit() const;
  inline void set_urgent(int64_t limit);
  inline int64_t get_urgent() const;
  inline int64_t get_hold() const;
  inline int64_t get_used() const;
  inline int64_t get_free_chunk_count() const;
  inline int64_t get_free_chunk_pushes() const;
  inline int64_t get_free_chunk_pops() const;
  inline int64_t get_freelist_hold() const;
  inline int64_t get_maps()
  {
    return maps_;
  }
  inline int64_t get_unmaps()
  {
    return unmaps_;
  }
  inline int64_t get_large_maps()
  {
    return large_maps_;
  }
  inline int64_t get_large_unmaps()
  {
    return large_unmaps_;
  }

private:
  typedef ABitSet ChunkBitMap;

private:
  void* direct_alloc(const uint64_t size);
  void direct_free(const void* ptr, const uint64_t size);
  // wrap for mmap
  void* low_alloc(const uint64_t size);
  void low_free(const void* ptr, const uint64_t size);

  bool update_hold(int64_t bytes, bool high_prio);

protected:
  AChunkList free_list_;
  ChunkBitMap* chunk_bitmap_;

  int64_t limit_;
  int64_t urgent_;
  int64_t hold_;  // Including the memory occupied by free_list

  int64_t maps_;
  int64_t unmaps_;
  int64_t large_maps_;
  int64_t large_unmaps_;
};  // end of class AChunkMgr

#if MEMCHK_LEVEL >= 1
const AChunk* AChunkMgr::ptr2chunk(const void* ptr)
{
  AChunk* chunk = NULL;
  if (chunk_bitmap_ != NULL) {
    uint64_t aligned_addr = (uint64_t)(ptr) & ~(MEMCHK_CHUNK_ALIGN - 1);
    if (chunk_bitmap_->isset((int)(aligned_addr >> MEMCHK_CHUNK_ALIGN_BITS))) {
      chunk = (AChunk*)aligned_addr;
    }
  }
  return chunk;
}
#endif

OB_INLINE uint64_t AChunkMgr::aligned(const uint64_t size)
{
  const uint64_t all_size = align_up2(size + ACHUNK_HEADER_SIZE, ALIGN_SIZE);
  return all_size;
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
  return hold_ - get_freelist_hold();
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
  return free_list_.count() * INTACT_ACHUNK_SIZE;
}

}  // end of namespace lib
}  // end of namespace oceanbase

#define CHUNK_MGR (oceanbase::lib::AChunkMgr::instance())

#endif /* _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_ */
