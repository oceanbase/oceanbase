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

#ifndef OB_SERVER_OBJECT_POOL_H_
#define OB_SERVER_OBJECT_POOL_H_

/*****************************************************************************
 * Global object buffer pool, each type of independent buffer pool.
 * A cache queue with twice the number of cores, and threads are mapped to the cache queue according to the thread ID.
 *
 * sop_borrow(type) is used to obtain an object
 * sop_return(type, ptr) return an object
 *
 * sop_borrow(type) is equivalent to ObServerObjectPool<type>::get_instance().borrow_object()
 * sop_return(type, ptr) is equivalent to ObServerObjectPool<type>::get_instance().return_object(ptr)
 *
 * Through the virtual table __all_virtual_server_object_pool,
 * you can view the information of each cache queue of all object cache pools,
 * The meaning of each column of the virtual table refers to the comment of each attribute in ObPoolArenaHead
 *
 *****************************************************************************/

#include <lib/utility/ob_macro_utils.h>
#include <lib/thread_local/ob_tsi_utils.h>
#include <lib/atomic/ob_atomic.h>
#include <lib/utility/utility.h>
#include <lib/thread/ob_cache_line_segregated_array.h>
#include "lib/ob_running_mode.h"

#define PTR_META2OBJ(x) reinterpret_cast<T*>(reinterpret_cast<char*>(x) + sizeof(Meta));
#define PTR_OBJ2META(y) reinterpret_cast<Meta*>(reinterpret_cast<char*>(y) - sizeof(Meta));

namespace oceanbase {
namespace common {

struct ObPoolArenaHead {
  ObLatch lock;             // Lock, 4 Bytes
  int32_t borrow_cnt;       // Number of calls
  int32_t return_cnt;       // Number of calls
  int32_t miss_cnt;         // Number of direct allocations
  int32_t miss_return_cnt;  // Number of returns directly allocated
  int16_t free_num;         // Number of caches
  int16_t reserved1;
  int64_t last_borrow_ts;       // Time of last visit
  int64_t last_return_ts;       // Time of last visit
  int64_t last_miss_ts;         // The time of the last direct allocation
  int64_t last_miss_return_ts;  // The return time of the last direct allocation
  void* next;                   // Point to the first free object
  TO_STRING_KV(KP(this), K(lock), K(borrow_cnt), K(return_cnt), K(miss_cnt), K(miss_return_cnt), K(last_borrow_ts),
      K(last_return_ts), K(last_miss_ts), K(last_miss_return_ts), KP(next));
  void reset()
  {
    new (&lock) ObLatch();
    borrow_cnt = 0;
    return_cnt = 0;
    miss_cnt = 0;
    miss_return_cnt = 0;
    free_num = 0;
    reserved1 = 0;
    last_borrow_ts = 0;
    last_return_ts = 0;
    last_miss_ts = 0;
    last_miss_return_ts = 0;
    next = NULL;
  }
};
typedef ObCacheLineSegregatedArray<ObPoolArenaHead> ObPoolArenaArray;

class ObServerObjectPoolRegistry {
public:
  class ArenaIterator {
  public:
    bool is_end()
    {
      return pool_idx_ >= ObServerObjectPoolRegistry::pool_num_;
    }
    int next()
    {
      int ret = OB_SUCCESS;
      if (is_end()) {
        ret = OB_ITER_END;
        COMMON_LOG(ERROR,
            "iterator has reached the end",
            K(ret),
            K(pool_idx_),
            K(arena_idx_),
            K(ObServerObjectPoolRegistry::pool_num_));
      } else {
        arena_idx_++;
        if (arena_idx_ >= ObServerObjectPoolRegistry::pool_list_[pool_idx_].array->size()) {
          pool_idx_++;
          arena_idx_ = 0;
        }
      }
      return ret;
    }
    int get_pool_arena(const char*& type_name, ObPoolArenaHead*& arena_head)
    {
      int ret = OB_SUCCESS;
      if (is_end()) {
        ret = OB_ITER_END;
        COMMON_LOG(ERROR,
            "iterator has reached the end",
            K(ret),
            K(pool_idx_),
            K(arena_idx_),
            K(ObServerObjectPoolRegistry::pool_num_));
      } else {
        PoolPair& pair = ObServerObjectPoolRegistry::pool_list_[pool_idx_];
        type_name = pair.type_name;
        arena_head = &(*pair.array)[arena_idx_];
      }
      return ret;
    }
    int64_t get_arena_idx()
    {
      return arena_idx_;
    }

    int init(int64_t pool_idx, int64_t arena_idx)
    {
      int ret = OB_SUCCESS;
      if (pool_idx < 0 || arena_idx < 0) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(ERROR, "invalid arguments", K(ret), K(pool_idx), K(arena_idx));
      } else {
        pool_idx_ = pool_idx;
        arena_idx_ = arena_idx;
      }
      return ret;
    }

  public:
    TO_STRING_KV(K_(pool_idx), K_(arena_idx));

  private:
    int64_t pool_idx_;
    int64_t arena_idx_;
  };

public:
  static int alloc_iterator(ArenaIterator& iter)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(iter.init(0, 0))) {
      COMMON_LOG(ERROR, "ArenaIterator init failed", K(ret));
    }
    return ret;
  }
  static int add(const char* type_name, ObPoolArenaArray* array)
  {
    int ret = OB_SUCCESS;
    if (NULL == type_name || NULL == array) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "invalid argument", K(ret), K(type_name), K(array), K(pool_num_));
    } else if (pool_num_ < MAX_POOL_NUM) {
      pool_list_[pool_num_].type_name = type_name;
      pool_list_[pool_num_].array = array;
      pool_num_++;
    } else {
      ret = OB_ARRAY_OUT_OF_RANGE;
      COMMON_LOG(WARN,
          "WARNING OUT OF BOUND, no harm, but an ServerObjectPool can't be monitored",
          K(ret),
          K(+MAX_POOL_NUM),
          K(pool_num_),
          K(type_name),
          KP(array),
          K(array));
    }
    return ret;
  }

private:
  struct PoolPair {
    const char* type_name;
    ObPoolArenaArray* array;
  };
  static const int64_t MAX_POOL_NUM = 1L << 21;
  static PoolPair pool_list_[MAX_POOL_NUM];
  static int64_t pool_num_;
};

/**
 * ObServerObjectPool is a global singleton used to cache Objects.
 * The main use is to cache large objects that are expensive to construct and destruct.
 * This type of cached objects uses a cache linked list with the number of cores multiplied by two.
 */
template <class T>
class ObServerObjectPool {
public:
  static ObServerObjectPool& get_instance()
  {
    static ObServerObjectPool instance_;
    return instance_;
  }

  T* borrow_object()
  {
    T* ctx = NULL;
    int64_t itid = get_itid();
    int64_t aid = itid % arena_.size();
    Meta* cmeta = NULL;
    ObPoolArenaHead& arena = arena_[aid];
    int64_t cur_ts = ObTimeUtility::current_time();
    {  // Enter the critical area of the arena, the timestamp is obtained outside the lock, and minimize the length of
       // the critical area
      ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
      cmeta = static_cast<Meta*>(arena.next);
      if (NULL != cmeta) {
        arena.next = static_cast<void*>(cmeta->next);
        arena.borrow_cnt++;
        arena.free_num--;
        arena.last_borrow_ts = cur_ts;
      } else {
        arena.miss_cnt++;
        arena.last_miss_ts = cur_ts;
      }
    }
    if (NULL != cmeta) {
      ctx = PTR_META2OBJ(cmeta);
    }
    if (NULL == ctx) {
      char* p = static_cast<char*>(ob_malloc(item_size_, ObModIds::OB_SERVER_OBJECT_POOL));
      if (NULL == p) {
        COMMON_LOG(ERROR, "allocate memory failed", K(typeid(T).name()), K(item_size_), K(itid), K(aid), K(arena));
      } else {
        Meta* cmeta = reinterpret_cast<Meta*>(p);
        cmeta->next = NULL;
        cmeta->arena_id = -aid;
        cmeta->magic = 0xFEDCFEDC01230123;
        ctx = PTR_META2OBJ(p);
        new (ctx) T();
      }
    }
    return ctx;
  }

  void return_object(T* x)
  {
    if (NULL == x) {
      COMMON_LOG(ERROR, "allocate memory failed", K(typeid(T).name()), K(item_size_), K(get_itid()));
    } else {
      Meta* cmeta = PTR_OBJ2META(x);
      int64_t aid = cmeta->arena_id;
      if (aid >= 0) {
        x->reset();
        ObPoolArenaHead& arena = arena_[aid];
        int64_t cur_ts = ObTimeUtility::current_time();
        {  // Enter the critical area of the arena, the timestamp is obtained outside the lock, and minimize the length
           // of the critical area
          ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
          cmeta->next = static_cast<Meta*>(arena.next);
          arena.next = static_cast<void*>(cmeta);
          arena.return_cnt++;
          arena.free_num++;
          arena.last_return_ts = cur_ts;
        }
      } else {
        x->~T();
        ob_free(cmeta);
        ObPoolArenaHead& arena = arena_[-aid];
        int64_t cur_ts = ObTimeUtility::current_time();
        {  // Enter the critical area of the arena, the timestamp is obtained outside the lock, and minimize the length
           // of the critical area
          ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
          arena.miss_return_cnt++;
          arena.last_miss_return_ts = cur_ts;
        }
      }
    }
  }

private:
  /**
   * Roughly allocate 16 available objects to each entry during Pool construction
   * The memory is directly ob_malloc out of the total size at one time
   * All objects are stuffed into each entrance
   * Because it is a global singleton, these tasks are completed at the time of program startup
   */
  ObServerObjectPool()
  {
    // If the assignment logic of buf_ below is not reached, buf_ will not be initialized
    buf_ = NULL;
    int ret = OB_SUCCESS;
    cnt_per_arena_ = lib::is_mini_mode() ? 16 : 128;
    int64_t s = (sizeof(T) + sizeof(Meta));         // Each cached object header has a Meta field to store necessary
                                                    // information and linked list pointers
    item_size_ = upper_align(s, CACHE_ALIGN_SIZE);  // Align according to the cache line to ensure that there will be no
                                                    // false sharing between objects
    if (OB_FAIL(ObCacheLineSegregatedArrayBase::get_instance().alloc_array(arena_))) {
      COMMON_LOG(ERROR, "alloc_array failed", K(ret), K(typeid(T).name()), K(item_size_));
    } else if ((buf_ = ob_malloc(arena_.size() * cnt_per_arena_ * item_size_, ObModIds::OB_SERVER_OBJECT_POOL)) ==
               NULL) {
      COMMON_LOG(ERROR,
          "allocate memory failed",
          K(ret),
          K(typeid(T).name()),
          K(item_size_),
          K(arena_.size()),
          K(+cnt_per_arena_));
    } else {
      char* p = reinterpret_cast<char*>(buf_);
      for (int64_t i = 0; i < arena_.size(); ++i) {
        Meta* pmeta = NULL;
        for (int64_t j = 0; j < cnt_per_arena_; ++j) {
          Meta* cmeta = reinterpret_cast<Meta*>(p);
          cmeta->next = pmeta;
          cmeta->arena_id = i;
          cmeta->magic = 0xFEDCFEDC01240124;
          pmeta = cmeta;
          new (p + sizeof(Meta)) T();
          p += item_size_;
        }
        ObPoolArenaHead& arena = arena_[i];
        arena.reset();
        arena.next = static_cast<void*>(pmeta);
        arena.free_num = static_cast<int16_t>(cnt_per_arena_);
      }
      if (OB_FAIL(ObServerObjectPoolRegistry::add(typeid(T).name(),
              &arena_))) {  // Register to the global list, display and print the log in the virtual table
        COMMON_LOG(
            WARN, "add to pool registry failed, can't be monitored", K(ret), K(typeid(T).name()), KP(this), K(this));
      } else {
        COMMON_LOG(INFO,
            "register server object pool finish",
            "tpye_name",
            typeid(T).name(),
            "type_size",
            sizeof(T),
            KP(this),
            K(this));
      }
    }
  }

  ~ObServerObjectPool()
  {}

private:
  struct Meta {
    Meta* next;
    int64_t arena_id;
    int64_t magic;
    char padding__[8];
  };
  ObPoolArenaArray arena_;
  int64_t cnt_per_arena_;
  int64_t item_size_;
  void* buf_;
};

#define sop_borrow(type) ObServerObjectPool<type>::get_instance().borrow_object()
#define sop_return(type, ptr) ObServerObjectPool<type>::get_instance().return_object(ptr)

}  // namespace common
}  // namespace oceanbase
#endif  // OB_SERVER_OBJECT_POOL_H_
