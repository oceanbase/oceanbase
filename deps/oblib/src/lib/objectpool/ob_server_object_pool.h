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
 * 全局对象缓存池，每个类型独立的缓存池。
 * 共核数 2 倍的缓存队列，线程根据线程 ID 映射到缓存队列上。
 *
 *   sop_borrow(type) 用于获取一个对象
 *   sop_return(type, ptr) 归还一个对象
 *
 *   sop_borrow(type) 等价于 ObServerObjectPool<type>::get_instance().borrow_object()
 *   sop_return(type, ptr) 等价于 ObServerObjectPool<type>::get_instance().return_object(ptr)
 *
 * 通过虚拟表 __all_virtual_server_object_pool 可以查看所有对象缓存池的每个缓存队列的信息，
 * 虚拟表每列的含义参考 ObPoolArenaHead 里每个属性的注释
 *
 *****************************************************************************/

#include <lib/utility/ob_macro_utils.h>
#include <lib/thread_local/ob_tsi_utils.h>
#include <lib/atomic/ob_atomic.h>
#include <lib/utility/utility.h>
#include <lib/cpu/ob_cpu_topology.h>
#include "lib/ob_running_mode.h"

#define PTR_META2OBJ(x) reinterpret_cast<T*>(reinterpret_cast<char*>(x) + sizeof(Meta));
#define PTR_OBJ2META(y) reinterpret_cast<Meta*>(reinterpret_cast<char*>(y) - sizeof(Meta));

namespace oceanbase
{
namespace common
{

struct ObPoolArenaHead
{
  ObLatch lock;  // Lock, 4 Bytes
  int32_t borrow_cnt; // Number of calls
  int32_t return_cnt; // Number of calls
  int32_t miss_cnt; // Number of direct allocations
  int32_t miss_return_cnt; // Number of returns directly allocated
  int16_t free_num;  // Number of caches
  int64_t all_using_cnt;
  int16_t reserved1;
  int64_t last_borrow_ts; // Time of last visit
  int64_t last_return_ts; // Time of last visit
  int64_t last_miss_ts; // The time of the last direct allocation
  int64_t last_miss_return_ts; // The return time of the last direct allocation
  void *next; // Point to the first free object
  TO_STRING_KV(
      KP(this),
      K(lock),
      K(borrow_cnt),
      K(return_cnt),
      K(miss_cnt),
      K(miss_return_cnt),
      K(last_borrow_ts),
      K(last_return_ts),
      K(last_miss_ts),
      K(last_miss_return_ts),
      KP(next));
  void reset() {
    new (&lock) ObLatch();
    borrow_cnt = 0;
    return_cnt = 0;
    miss_cnt = 0;
    miss_return_cnt = 0;
    free_num = 0;
    all_using_cnt = 0;
    reserved1 = 0;
    last_borrow_ts = 0;
    last_return_ts = 0;
    last_miss_ts = 0;
    last_miss_return_ts = 0;
    next = NULL;
  }
};

class ObServerObjectPoolRegistry
{
public:
  class ArenaIterator
  {
  public:
    bool is_end() {
      return pool_idx_ >= ObServerObjectPoolRegistry::pool_num_;
    }
    int next() {
      int ret = OB_SUCCESS;
      if (is_end()) {
        ret = OB_ITER_END;
        COMMON_LOG(ERROR, "iterator has reached the end", K(ret), K(pool_idx_), K(arena_idx_),
            K(ObServerObjectPoolRegistry::pool_num_));
      } else {
        arena_idx_++;
        if (arena_idx_ >= ObServerObjectPoolRegistry::pool_list_[pool_idx_].array_size) {
          pool_idx_++;
          arena_idx_ = 0;
        }
      }
      return ret;
    }
    int get_pool_arena(const char* &type_name, ObPoolArenaHead* &arena_head) {
      int ret = OB_SUCCESS;
      if (is_end()) {
        ret = OB_ITER_END;
        COMMON_LOG(ERROR, "iterator has reached the end", K(ret), K(pool_idx_), K(arena_idx_),
            K(ObServerObjectPoolRegistry::pool_num_));
      } else {
        PoolPair &pair = ObServerObjectPoolRegistry::pool_list_[pool_idx_];
        type_name = pair.type_name;
        arena_head = &pair.array[arena_idx_];
      }
      return ret;
    }
    int64_t get_arena_idx() {return arena_idx_;}
    /**
     * 初始化
     */
    int init(int64_t pool_idx, int64_t arena_idx) {
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
  static int alloc_iterator(ArenaIterator &iter) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(iter.init(0, 0))) {
      COMMON_LOG(ERROR, "ArenaIterator init failed", K(ret));
    }
    return ret;
  }
  static int add(const char* type_name, ObPoolArenaHead *array, int32_t array_size) {
    int ret = OB_SUCCESS;
    if (NULL == type_name || NULL == array) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(ERROR, "invalid argument", K(ret), KP(type_name), KP(array), K(pool_num_));
    } else if (pool_num_ < MAX_POOL_NUM) {
      pool_list_[pool_num_].type_name = type_name;
      pool_list_[pool_num_].array = array;
      pool_list_[pool_num_].array_size = array_size;
      pool_num_++;
    } else {
      ret = OB_ARRAY_OUT_OF_RANGE;
      COMMON_LOG(WARN, "WARNING OUT OF BOUND, no harm, but an ServerObjectPool can't be monitored",
          K(ret), K(+MAX_POOL_NUM), K(pool_num_), KCSTRING(type_name), KP(array), K(array));
    }
    return ret;
  }
private:
  struct PoolPair {
    const char * type_name;
    ObPoolArenaHead *array;
    int32_t array_size;
  };
  // This structure is used to index all object pools. It is currently invalid and needs to be redesigned later
  static const int64_t MAX_POOL_NUM = 1024;
  static PoolPair pool_list_[MAX_POOL_NUM];
  static int64_t pool_num_;
};

/**
 * ObServerObjectPool 是全局单例，用于缓存 Object，主要用处是缓存构造和析构代价大的大对象。
 * 此类会缓存对象，使用核数乘以 2 个数的缓存链表。
 *
 * 接口：
 *   borrow_object: 获取对象
 *   return_object: 归还对象
 */
template <class T>
class ObServerObjectPool
{
public:
  T* borrow_object() {
    T *ctx = NULL;
    if (OB_LIKELY(is_inited_)) {
      Meta *cmeta = NULL;
      int64_t itid = get_itid();
      int64_t aid = itid % arena_num_;
      ObPoolArenaHead &arena = arena_[aid];
      int64_t cur_ts = ObClockGenerator::getClock();
      { // Enter the critical area of the arena, the timestamp is obtained outside the lock, and minimize the length of the critical area
        ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
        cmeta = static_cast<Meta*>(arena.next);
        if (NULL != cmeta) {
          arena.next = static_cast<void*>(cmeta->next);
          arena.borrow_cnt++;
          arena.free_num--;
          arena.all_using_cnt++;
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
        ObMemAttr attr(tenant_id_, ObModIds::OB_SERVER_OBJECT_POOL);
        SET_USE_500(attr);
        char *p = static_cast<char*>(ob_malloc(item_size_, attr));
        if (NULL == p) {
          COMMON_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "allocate memory failed", K(typeid(T).name()), K(item_size_));
        } else {
          Meta *cmeta = reinterpret_cast<Meta*>(p);
          cmeta->next = NULL;
          cmeta->arena_id = -(aid + 1);
          cmeta->magic = 0xFEDCFEDC01230123;
          ctx = PTR_META2OBJ(p);
          new (ctx) T();
          ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
          arena.all_using_cnt++;
        }
      }
    }
    return ctx;
  }

  void return_object(T* x) {
    if (NULL == x) {
      COMMON_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "allocate memory failed", K(typeid(T).name()), K(item_size_), K(get_itid()));
    } else if (OB_UNLIKELY(!is_inited_)){
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected return", K(typeid(T).name()), K(item_size_), K(get_itid()));
    } else {
      Meta *cmeta = PTR_OBJ2META(x);
      int64_t aid = cmeta->arena_id;
      if (aid >= 0) {
        x->reset();
        ObPoolArenaHead &arena = arena_[aid];
        int64_t cur_ts = ObClockGenerator::getClock();
        { // Enter the critical area of the arena, the timestamp is obtained outside the lock, and minimize the length of the critical area
          ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
          cmeta->next = static_cast<Meta*>(arena.next);
          arena.next = static_cast<void*>(cmeta);
          arena.return_cnt++;
          arena.free_num++;
          arena.all_using_cnt--;
          arena.last_return_ts = cur_ts;
        }
      } else {
        x->~T();
        ob_free(cmeta);
        ObPoolArenaHead &arena = arena_[-(aid + 1)];
        int64_t cur_ts = ObClockGenerator::getClock();
        { // Enter the critical area of the arena, the timestamp is obtained outside the lock, and minimize the length of the critical area
          ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
          arena.miss_return_cnt++;
          arena.all_using_cnt--;
          arena.last_miss_return_ts = cur_ts;
        }
      }
    }
  }

  /**
   * 粗暴的在 Pool 构造时给每个入口分配 16 个可用的对象
   * 内存直接根据总大小一次性 ob_malloc 出来
   * 所有对象塞到各个入口中
   * 因为是全局单例，所以是在程序启动时机完成了这些工作
   * TODO: 改为按需分配
   */
  ObServerObjectPool(const int64_t tenant_id, const bool regist, const bool is_mini_mode,
                     const int64_t cpu_count)
    : tenant_id_(tenant_id), regist_(regist), is_mini_mode_(is_mini_mode),
      cpu_count_(cpu_count), arena_num_(0),
      arena_(NULL), cnt_per_arena_(0), item_size_(0), buf_(nullptr), is_inited_(false)
  {}

  int init()
  {
    int ret = OB_SUCCESS;
    const bool is_mini = is_mini_mode_;
    arena_num_ = min(64/*upper_bound*/, max(4/*lower_bound*/, static_cast<int32_t>(cpu_count_) * 2));
    //If the assignment logic of buf_ below is not reached, buf_ will not be initialized
    buf_ = NULL;
    cnt_per_arena_ = is_mini ? 8 : 64;
    int64_t s = (sizeof(T) + sizeof(Meta)); // Each cached object header has a Meta field to store necessary information and linked list pointers
    item_size_ = upper_align(s, CACHE_ALIGN_SIZE); // Align according to the cache line to ensure that there will be no false sharing between objects
    ObMemAttr attr(tenant_id_, ObModIds::OB_SERVER_OBJECT_POOL);
    SET_USE_500(attr);
    void *ptr = NULL;
    if (OB_ISNULL(ptr = ob_malloc(sizeof(ObPoolArenaHead) * arena_num_,
                                  SET_USE_500(ObMemAttr(tenant_id_, "PoolArenaArray"))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "allocate memory failed", K(ret), K(typeid(T).name()));
    } else if ((buf_ = ob_malloc(arena_num_ * cnt_per_arena_ * item_size_, attr)) == NULL) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "allocate memory failed", K(ret), K(typeid(T).name()), K(item_size_),
                 K(arena_num_), K(+cnt_per_arena_));
    } else {
      arena_ = (ObPoolArenaHead*)ptr;
      char *p = reinterpret_cast<char*>(buf_);
      for (int64_t i = 0; i < arena_num_; ++i) {
        Meta *pmeta = NULL;
        for (int64_t j = 0; j < cnt_per_arena_; ++j) {
          Meta *cmeta = reinterpret_cast<Meta*>(p);
          cmeta->next = pmeta;
          cmeta->arena_id = i;
          cmeta->magic = 0xFEDCFEDC01240124;
          pmeta = cmeta;
          new (p + sizeof(Meta)) T();
          p += item_size_;
        }
        ObPoolArenaHead &arena = arena_[i];
        arena.reset();
        arena.next = static_cast<void*>(pmeta);
        arena.free_num = static_cast<int16_t>(cnt_per_arena_);
      }
      if (regist_) {
        // Register to the global list, display and print the log in the virtual table
        if (OB_FAIL(ObServerObjectPoolRegistry::add(typeid(T).name(), arena_, arena_num_))) {
          COMMON_LOG(WARN, "add to pool registry failed, can't be monitored", K(ret), K(typeid(T).name()), KP(this), K(this));
        } else {
          COMMON_LOG(INFO, "register server object pool finish",
                     "tpye_name", typeid(T).name(),
                     "type_size", sizeof(T),
                     KP(this), K(this));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
    return ret;
  }

  void destroy() {
    if (is_inited_) {
      bool has_unfree = false;
      for (int64_t i = 0; !has_unfree && i < arena_num_; ++i) {
        ObPoolArenaHead &arena = arena_[i];
        ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
        has_unfree = arena.all_using_cnt > 0;
      }
      if (!has_unfree) {
        for (int64_t i = 0; i < arena_num_; ++i) {
          Meta *meta = NULL;
          {
            ObPoolArenaHead &arena = arena_[i];
            ObLatchWGuard lock_guard(arena.lock, ObLatchIds::SERVER_OBJECT_POOL_ARENA_LOCK);
            meta = static_cast<Meta*>(arena.next);
            arena.next = NULL;
          }
          while (meta != NULL) {
            T *x = PTR_META2OBJ(meta);
            x->reset();
            x->~T();
            meta = meta->next;
          }
        }
        if (buf_ != NULL) {
          ob_free(buf_);
          buf_ = NULL;
        }
        if (arena_ != NULL) {
          ob_free(arena_);
          arena_ = NULL;
        }
        is_inited_ = false;
      }
      if (has_unfree) {
        COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "server object leak", K(tenant_id_), K(typeid(T).name()));
      }
    }
  }

  ~ObServerObjectPool() {
    destroy();
  }
private:
  struct Meta
  {
    Meta * next;
    int64_t arena_id;
    int64_t magic;
    char padding__[8];
  };
  const int64_t tenant_id_;
  const bool regist_;
  const bool is_mini_mode_;
  const int64_t cpu_count_;
  int32_t arena_num_;
  ObPoolArenaHead *arena_;
  int64_t cnt_per_arena_;
  int64_t item_size_;
  void *buf_;
  bool is_inited_;
};

template<typename T>
inline ObServerObjectPool<T>& get_server_object_pool() {
  class Wrapper {
  public:
    Wrapper()
      : instance_(OB_SERVER_TENANT_ID, true/*regist*/, lib::is_mini_mode(),
                  get_cpu_count())
    {
      instance_.init(); // is_inited_ will be checked all invokes
    }
    ObServerObjectPool<T> instance_;
  };
  static Wrapper w;
  return w.instance_;
}

#define sop_borrow(type) get_server_object_pool<type>().borrow_object()
#define sop_return(type, ptr) get_server_object_pool<type>().return_object(ptr)


}
}
#endif // OB_SERVER_OBJECT_POOL_H_
