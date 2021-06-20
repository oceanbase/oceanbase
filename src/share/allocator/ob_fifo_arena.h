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

#ifndef OCEANBASE_SHARE_FIFO_ARENA_H_
#define OCEANBASE_SHARE_FIFO_ARENA_H_

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase {
namespace common {
class ObMemstoreAllocatorMgr;
class ObActiveList;
class ObFifoArena {
public:
  static int64_t total_hold_;
  struct Page;
  struct Ref {
    void set_page(Page* page)
    {
      next_ = NULL;
      page_ = page;
      allocated_ = 0;
    }
    void add_alloc_size(int64_t size)
    {
      ATOMIC_FAA(&allocated_, size);
    }
    Ref* next_;
    Page* page_;
    int64_t allocated_;
  };

  struct Page {
    void set(int64_t size)
    {
      self_ref_.set_page(this);
      limit_ = size - sizeof(*this);
      pos_ = 0;
      ref_ = 0;
    }
    int64_t hold()
    {
      return limit_ + sizeof(*this);
    }
    int64_t xref(int64_t x)
    {
      return ATOMIC_AAF(&ref_, x);
    }
    char* alloc(bool& need_switch, int64_t size)
    {
      char* ret = NULL;
      int64_t pos = 0;
      int64_t limit = ATOMIC_LOAD(&limit_);
      if ((pos = ATOMIC_LOAD(&pos_)) <= limit) {
        pos = ATOMIC_FAA(&pos_, size);
        ret = (pos + size <= limit) ? buf_ + pos : NULL;
      }
      need_switch = pos <= limit && (NULL == ret);
      if (need_switch) {
        self_ref_.add_alloc_size(-pos);
      }
      return ret;
    }
    Ref* frozen()
    {
      Ref* ref = NULL;
      bool need_switch = false;
      (void)alloc(need_switch, ATOMIC_LOAD(&limit_) + 1);
      if (need_switch) {
        ref = &self_ref_;
      }
      return ref;
    }
    int64_t get_actual_hold_size();
    Ref self_ref_;
    int64_t limit_;
    int64_t pos_;
    int64_t ref_;
    char buf_[0];
  };
  struct LockGuard {
    LockGuard(int64_t& lock) : lock_(lock)
    {
      while (ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    ~LockGuard()
    {
      ATOMIC_STORE(&lock_, 0);
    }
    int64_t& lock_;
  };
  struct Handle {
    enum { MAX_NWAY = 32 };
    void reset()
    {
      lock_ = 0;
      memset(ref_, 0, sizeof(ref_));
      allocated_ = 0;
    }
    Ref* get_match_ref(int64_t idx, Page* page)
    {
      Ref* ref = ATOMIC_LOAD(ref_ + idx);
      if (NULL != ref && page != ref->page_) {
        ref = NULL;
      }
      return ref;
    }
    void* alloc(bool& need_switch, Ref* ref, Page* page, int64_t size)
    {
      void* ptr = NULL;
      if (NULL != (ptr = page->alloc(need_switch, size))) {
        ref->add_alloc_size(size);
      }
      return ptr;
    }
    void* ref_and_alloc(int64_t idx, bool& need_switch, Page* page, int64_t size)
    {
      void* ptr = NULL;
      Ref* ref = NULL;
      if (NULL != (ref = (Ref*)page->alloc(need_switch, size + sizeof(*ref)))) {
        ref->set_page(page);
        ref->add_alloc_size(size + sizeof(*ref));
        add_ref(idx, ref);
        ptr = (void*)(ref + 1);
      }
      return ptr;
    }
    void add_ref(int64_t idx, Ref* ref)
    {
      Ref* old_ref = ATOMIC_TAS(ref_ + idx, ref);
      ATOMIC_STORE(&ref->next_, old_ref);
    }
    int64_t get_allocated() const
    {
      return ATOMIC_LOAD(&allocated_);
    }
    void add_allocated(int64_t size)
    {
      ATOMIC_FAA(&allocated_, size);
    }
    TO_STRING_KV(K_(allocated));
    int64_t lock_;
    Ref* ref_[MAX_NWAY];
    int64_t allocated_;
  };

public:
  enum {
    MAX_CACHED_GROUP_COUNT = 16,
    MAX_CACHED_PAGE_COUNT = MAX_CACHED_GROUP_COUNT * Handle::MAX_NWAY,
    PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE + sizeof(Page) + sizeof(Ref)
  };
  ObFifoArena()
      : allocator_(NULL),
        nway_(0),
        allocated_(0),
        reclaimed_(0),
        hold_(0),
        retired_(0),
        last_base_ts_(0),
        last_reclaimed_(0),
        lastest_memstore_threshold_(0)
  {
    memset(cur_pages_, 0, sizeof(cur_pages_));
  }
  ~ObFifoArena()
  {
    reset();
  }

public:
  int init(uint64_t tenant_id);
  void reset();
  void update_nway_per_group(int64_t nway);
  void* alloc(int64_t idx, Handle& handle, int64_t size);
  void free(Handle& ref);
  int64_t allocated() const
  {
    return ATOMIC_LOAD(&allocated_);
  }
  int64_t retired() const
  {
    return ATOMIC_LOAD(&retired_);
  }
  int64_t hold() const
  {
    int64_t rsize = ATOMIC_LOAD(&reclaimed_);
    int64_t asize = ATOMIC_LOAD(&allocated_);
    return asize - rsize;
  }
  uint64_t get_tenant_id() const
  {
    return attr_.tenant_id_;
  }

  void set_memstore_threshold(int64_t memstore_threshold);
  bool need_do_writing_throttle() const;

private:
  ObQSync& get_qs()
  {
    static ObQSync s_qs;
    return s_qs;
  }
  int64_t get_way_id()
  {
    return icpu_id() % ATOMIC_LOAD(&nway_);
  }
  int64_t get_idx(int64_t grp_id, int64_t way_id)
  {
    return (grp_id % MAX_CACHED_GROUP_COUNT) * Handle::MAX_NWAY + way_id;
  }

  struct ObWriteThrottleInfo {
  public:
    ObWriteThrottleInfo()
    {
      reset();
    }
    ~ObWriteThrottleInfo()
    {}
    void reset();
    void reset_period_stat_info();
    void record_limit_event(int64_t interval);
    int check_and_calc_decay_factor(int64_t memstore_threshold, int64_t trigger_percentage, int64_t alloc_duration);
    TO_STRING_KV(K(decay_factor_), K(alloc_duration_), K(trigger_percentage_), K(memstore_threshold_),
        K(period_throttled_count_), K(period_throttled_time_), K(total_throttled_count_), K(total_throttled_time_));

  public:
    // control info
    double decay_factor_;
    int64_t alloc_duration_;
    int64_t trigger_percentage_;
    int64_t memstore_threshold_;
    // stat info
    int64_t period_throttled_count_;
    int64_t period_throttled_time_;
    int64_t total_throttled_count_;
    int64_t total_throttled_time_;
  };

private:
  void release_ref(Ref* ref);
  Page* alloc_page(int64_t size);
  void free_page(Page* ptr);
  void retire_page(int64_t way_id, Handle& handle, Page* ptr);
  void destroy_page(Page* page);
  void shrink_cached_page(int64_t nway);
  void speed_limit(int64_t cur_mem_hold, int64_t alloc_size);
  int64_t get_throttling_interval(int64_t cur_mem_hold, int64_t alloc_size, int64_t trigger_mem_limit);
  int64_t get_actual_hold_size(Page* page);
  int64_t get_writing_throttling_trigger_percentage_() const;
  int64_t get_writing_throttling_maximum_duration_() const;

private:
  static const int64_t MAX_WAIT_INTERVAL = 20 * 1000 * 1000;  // 20s
  static const int64_t MEM_SLICE_SIZE = 2 * 1024 * 1024;      // Bytes per usecond
  static const int64_t MIN_INTERVAL = 20000;
  static const int64_t DEFAULT_TRIGGER_PERCENTAGE = 100;
  static const int64_t DEFAULT_DURATION = 60 * 60 * 1000 * 1000L;  // us
  lib::ObMemAttr attr_;
  ObIAllocator* allocator_;
  int64_t nway_;
  int64_t allocated_;
  int64_t reclaimed_;
  int64_t hold_;  // for single tenant
  int64_t retired_;
  int64_t last_base_ts_;

  int64_t last_reclaimed_;
  Page* cur_pages_[MAX_CACHED_PAGE_COUNT];
  ObWriteThrottleInfo throttle_info_;
  int64_t lastest_memstore_threshold_;  // Save the latest memstore_threshold
  DISALLOW_COPY_AND_ASSIGN(ObFifoArena);
};

}  // namespace common
}  // end of namespace oceanbase

#endif
