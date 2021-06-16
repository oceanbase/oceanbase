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

#ifndef OB_LIB_CONCURRENCY_OBJPOOL_H_
#define OB_LIB_CONCURRENCY_OBJPOOL_H_

#include <typeinfo>
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_atomic_list.h"
#include "lib/list/ob_intrusive_list.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_vector.h"
#include "lib/coro/co.h"

DEFINE_HAS_MEMBER(OP_LOCAL_NUM);
DEFINE_HAS_MEMBER(OP_LABEL);

namespace oceanbase {
namespace common {
struct ObThreadCache;
class ObObjFreeList;
class ObObjFreeListList;
class ObFixedMemAllocator;

extern void thread_shutdown_cleanup(void*);

enum ObMemCacheType { OP_GLOBAL, OP_TC, OP_RECLAIM };

struct ObChunkInfo {
  ObChunkInfo()
  {
    memset(this, 0, sizeof(ObChunkInfo));
  }

  ~ObChunkInfo()
  {}

  pthread_t tid_;

  int64_t type_size_;
  int64_t obj_count_;
  int64_t allocated_;
  int64_t length_;

  // inner free list will only be
  // accessed by creator-thread
  void* inner_free_list_;
  void* head_;

  ObThreadCache* thread_cache_;

  LINK(ObChunkInfo, link_);

#ifdef DOUBLE_FREE_CHECK
  // magic code for each item,
  // it's used to check double-free issue.
  uint8_t item_magic_[0];
#endif
};

struct ObFreeObject {
  SLINK(ObFreeObject, link_);
};

struct ObThreadCache {
  ObThreadCache()
  {
    memset(this, 0, sizeof(ObThreadCache));
    clock_ = UINT64_MAX;
  }

  ~ObThreadCache()
  {}

  TO_STRING_KV(K_(f), K_(nr_total), K_(nr_malloc), K_(nr_free), K_(nr_chunks), K_(clock));

  // the address must be aligned with 16 bytes if it uses cas 128
  //
  // outer free list will be accessed by:
  // - creator-thread, as a producer-thread
  // - consumer-thread
  ObAtomicList outer_free_list_;

  // inner free list for tc alloc
  SLL<ObFreeObject> inner_free_list_;

  ObObjFreeList* f_;

  // using for memory reclaim algorithm
  int64_t nr_total_;
  int64_t nr_free_;
  int64_t nr_malloc_;

  int64_t nr_chunks_;
  ObChunkInfo* active_chunk_;
  Queue<ObChunkInfo> non_full_chunk_list_;
  DLL<ObChunkInfo> full_chunk_list_;

  LINK(ObThreadCache, link_);

  uint64_t clock_;
};

class ObObjFreeList {
  friend class ObObjFreeListList;
  friend struct ObThreadCache;

public:
  ObObjFreeList();
  ~ObObjFreeList()
  {
    (void)mutex_destroy(&lock_);
  }

  // alignment must be a power of 2
  int init(const char* name, const int64_t type_size, const int64_t obj_count, const int64_t alignment = 16,
      const ObMemCacheType cache_type = OP_RECLAIM);
  void* alloc();
  void free(void* item);
  const char* get_name() const
  {
    return name_;
  }
  int64_t get_allocated() const
  {
    return allocated_;
  }
  int64_t get_allocated_base() const
  {
    return allocated_base_;
  }
  int64_t get_type_size() const
  {
    return type_size_;
  }
  int64_t get_used() const
  {
    return used_;
  }
  int64_t get_used_base() const
  {
    return used_base_;
  }
  int64_t get_chunk_count() const
  {
    return chunk_count_high_mark_;
  }
  int64_t get_chunk_byte_size() const
  {
    return chunk_byte_size_;
  }
  int destroy_cur_thread_cache();

  TO_STRING_KV(K_(name), K_(is_inited), K_(only_global), K_(label), K_(thread_cache_idx), K_(type_size),
      K_(type_size_base), K_(alignment), K_(obj_count_base), K_(obj_count_per_chunk), K_(chunk_byte_size),
      K_(chunk_count_high_mark), K_(chunk_count_low_mark), K_(used), K_(allocated), K_(allocated_base), K_(used_base),
      K_(nr_thread_cache), K_(nr_orphaned_thread_cache), K_(clock));

  SLINK(ObObjFreeList, link_);

private:
  void show_info(const char* file, const int line, const char* tag, ObThreadCache* thread_cache);
  ObChunkInfo*& get_chunk_info(void* item);
#ifdef DOUBLE_FREE_CHECK
  int64_t get_chunk_item_magic_idx(void* item, ObChunkInfo** chunk_info, const bool do_check = false);
#endif
  void set_chunk_item_magic(ObChunkInfo* chunk_info, void* item);
  void clear_chunk_item_magic(ObChunkInfo* chunk_info, void* item);
  ObChunkInfo* chunk_create(ObThreadCache* thread_cache);
  void chunk_delete(ObThreadCache* thread_cache, ObChunkInfo* chunk_info);
  void* alloc_from_chunk(ObChunkInfo* chunk_info);
  void free_to_chunk(ObThreadCache* thread_cache, void* item);
  void* alloc_from_cache(ObThreadCache* thread_cache);
  void* privatize_public_freelist(ObThreadCache* thread_cache, const bool alloc);
  void* alloc_from_public_freelist(ObThreadCache* thread_cache);
  void* alloc_from_orphaned_thread_cache(ObThreadCache* thread_cache);
  void* alloc_from_new_chunk(ObThreadCache* thread_cache);

  void* global_alloc();
  void global_free(void* item);
  void* reclaim_alloc(ObThreadCache* thread_cache);
  void reclaim_free(ObThreadCache* cur_thread_cache, void* item);
  void* tc_alloc(ObThreadCache* thread_cache);
  void tc_free(ObThreadCache* cur_thread_cache, void* item);
  ObThreadCache* init_thread_cache();
  void privatize_thread_cache(ObThreadCache* cur_thread_cache, ObThreadCache* src_thread_cache);

  void update_used();

  uint64_t inc_clock()
  {
    return ATOMIC_AAF(&clock_, 1);
  }
  uint64_t get_clock()
  {
    return ATOMIC_LOAD(&clock_);
  }

  void rcu_read_lock(ObThreadCache* thread_cache)
  {
    WEAK_BARRIER();
    ATOMIC_STORE(&thread_cache->clock_, get_clock());
  }

  void rcu_read_unlock(ObThreadCache* thread_cache)
  {
    WEAK_BARRIER();
    ATOMIC_STORE(&thread_cache->clock_, UINT64_MAX);
  }

  void rcu_sync()
  {
    uint64_t clock = inc_clock();
    while (!is_quiescent(clock)) {
      PAUSE();
    }
  }

  bool is_quiescent(uint64_t cur_clock)
  {
    bool ret = true;
    mutex_acquire(&lock_);
    ObThreadCache* next_thread_cache = thread_cache_list_.head_;
    while (NULL != next_thread_cache) {
      if (ATOMIC_LOAD(&next_thread_cache->clock_) < cur_clock) {
        ret = false;
        break;
      }
      next_thread_cache = thread_cache_list_.next(next_thread_cache);
    }
    mutex_release(&lock_);
    return ret;
  }

private:
  static const int64_t THREAD_CACHE_TOTAL_SIZE;
  static const int64_t THREAD_CACHE_LOW_MARK;

  ObAtomicList obj_free_list_;  // global object freelist for global alloc

  ObAtomicSLL<ObObjFreeList>* freelists_;  // global free list list

  ObFixedMemAllocator* tc_allocator_;

  bool is_inited_;
  bool only_global_;
  lib::ObLabel label_;
  int64_t thread_cache_idx_;
  const char* name_;

  // actual size of each object, each object in one chunk, each chunk includes
  // an extra void* to store instance of ObChunkInfo.
  int64_t type_size_;
  // user specified size of object
  int64_t type_size_base_;
  int64_t alignment_;

  // user specified number of elements
  int64_t obj_count_base_;
  // number of elements of each chunk
  int64_t obj_count_per_chunk_;

  // byte size of one block allocated from ob_malloc()
  int64_t chunk_byte_size_;
  // memory block count, blocks are allocated from ob_malloc()
  int64_t chunk_count_high_mark_;
  int64_t chunk_count_low_mark_;

  int64_t used_;
  int64_t allocated_;
  int64_t allocated_base_;
  int64_t used_base_;

  // number of thread cache in one object freelist
  int64_t nr_thread_cache_;
  DLL<ObThreadCache> thread_cache_list_;
  int64_t nr_orphaned_thread_cache_;
  DLL<ObThreadCache> orphaned_thread_cache_list_;
  ::ObMutex0 lock_;

  uint64_t clock_;

  DISALLOW_COPY_AND_ASSIGN(ObObjFreeList);
};

// Allocator for fixed size memory blocks.
class ObFixedMemAllocator {
public:
  ObFixedMemAllocator()
  {
    fl_ = NULL;
  }
  virtual ~ObFixedMemAllocator()
  {}

  // Creates a new global allocator.
  // @param name identification tag used for mem tracking .
  // @param obj_size size of memory blocks to be allocated.
  // @param obj_count number of units to be allocated if free pool is
  //        empty.
  // @param alignment of objects must be a power of 2.
  // initialize the parameters of the allocator.
  int init(ObObjFreeListList& fll, const char* name, const int64_t obj_size, const int64_t obj_count,
      const int64_t alignment, const ObMemCacheType cache_type = OP_RECLAIM, const bool is_meta = false,
      const lib::ObLabel& label = common::ObModIds::OB_CONCURRENCY_OBJ_POOL);

  int init(const char* name, const int64_t obj_size, const int64_t obj_count, const int64_t alignment,
      const ObMemCacheType cache_type = OP_RECLAIM, const bool is_meta = false,
      const lib::ObLabel& label = common::ObModIds::OB_CONCURRENCY_OBJ_POOL);

  // Allocate a block of memory (size specified during construction
  // of Allocator.
  virtual void* alloc_void()
  {
    void* ret = NULL;
    if (OB_LIKELY(NULL != fl_)) {
      ret = fl_->alloc();
    }
    return ret;
  }

  // Deallocate a block of memory allocated by the Allocator.
  virtual void free_void(void* ptr)
  {
    if (OB_LIKELY(NULL != fl_) && OB_LIKELY(NULL != ptr)) {
      fl_->free(ptr);
      ptr = NULL;
    }
  }

protected:
  ObObjFreeList* fl_;

  DISALLOW_COPY_AND_ASSIGN(ObFixedMemAllocator);
};

template <class T = void>
struct __is_default_constructible__;

template <>
struct __is_default_constructible__<void> {
protected:
  // Put base typedefs here to avoid pollution
  struct twoc {
    char a, b;
  };
  template <bool>
  struct test {
    typedef char type;
  };

public:
  static bool const value = false;
};

template <>
struct __is_default_constructible__<>::test<true> {
  typedef twoc type;
};

template <class T>
struct __is_default_constructible__ : __is_default_constructible__<> {
private:
  template <class U>
  static typename test<!!sizeof(::new U())>::type sfinae(U*);
  template <class U>
  static char sfinae(...);

public:
  static bool const value = sizeof(sfinae<T>(0)) > 1;
};

template <class T>
struct ObClassConstructor {
  template <class Type, bool Cond = true>
  struct Constructor {
    Type* operator()(void* ptr)
    {
      return new (ptr) Type();
    }
  };

  template <class Type>
  struct Constructor<Type, false> {
    Type* operator()(void* ptr)
    {
      return reinterpret_cast<Type*>(ptr);
    }
  };

  T* operator()(void* ptr)
  {
    Constructor<T, __is_default_constructible__<T>::value> construct;
    return construct(ptr);
  }
};

#define RND16(x) (((x) + 15) & ~15)

// Allocator for Class objects. It uses a prototype object to do
// fast initialization. Prototype of the template class is created
// when the fast allocator is created. This is instantiated with
// default (no argument) constructor. Constructor is not called for
// the allocated objects. Instead, the prototype is just memory
// copied onto the new objects. This is done for performance reasons.
template <class T>
class ObClassAllocator : public ObFixedMemAllocator {
public:
  ObClassAllocator()
  {
    fl_ = NULL;
  }
  virtual ~ObClassAllocator()
  {}

  // Create a new global class specific ClassAllocator.
  // @param name some identifying name, used for mem tracking purposes.
  // @param obj_count number of units to be allocated if free pool is
  //        empty.
  // @param alignment of objects must be a power of 2.
  // @param is_meta is allocator for meta
  static ObClassAllocator<T>* get(const int64_t obj_count = 64, const ObMemCacheType cache_type = OP_RECLAIM,
      const lib::ObLabel& label = common::ObModIds::OB_CONCURRENCY_OBJ_POOL, const int64_t alignment = 16,
      const bool is_meta = false)
  {
    ObClassAllocator<T>* instance = NULL;
    while (OB_UNLIKELY(once_ < 2)) {
      if (ATOMIC_BCAS(&once_, 0, 1)) {
        instance = new (std::nothrow) ObClassAllocator<T>();
        if (OB_LIKELY(NULL != instance)) {
          if (common::OB_SUCCESS !=
              instance->init(
                  typeid(T).name(), RND16(sizeof(T)), obj_count, RND16(alignment), cache_type, is_meta, label)) {
            _OB_LOG(ERROR, "failed to init class allocator %s", typeid(T).name());
            delete instance;
            instance = NULL;
            ATOMIC_BCAS(&once_, 1, 0);
          } else {
            instance_ = instance;
            (void)ATOMIC_BCAS(&once_, 1, 2);
          }
        } else {
          (void)ATOMIC_BCAS(&once_, 1, 0);
        }
      }
    }
    return (ObClassAllocator<T>*)instance_;
  }

  // Allocates objects of the templated type.
  virtual T* alloc()
  {
    T* ret = NULL;
    void* ptr = NULL;
    if (OB_LIKELY(NULL != fl_) && OB_LIKELY(NULL != (ptr = fl_->alloc()))) {
      ObClassConstructor<T> construct;
      ret = construct(ptr);
    }
    return ret;
  }

  // Deallocates objects of the templated type.
  // @param ptr pointer to be freed.
  virtual void free(T* ptr)
  {
    if (OB_LIKELY(NULL != fl_) && OB_LIKELY(NULL != ptr)) {
      ptr->~T();
      fl_->free(ptr);
      ptr = NULL;
    }
  }

  // Deallocate objects of the templated type via the inherited
  // interface using void pointers.
  // @param ptr pointer to be freed.
  virtual void free_void(void* ptr)
  {
    free(reinterpret_cast<T*>(ptr));
  }

protected:
  static volatile int64_t once_;  // for creating singleton instance
  static volatile ObClassAllocator<T>* instance_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObClassAllocator);
};

// Allocator for meta of ObObjFreeList, ObChunkInfo, ObThreadCache. The other
// objects need created object pools with these three objects. But these three
// objects are also allocated form their object pools. The object pool is
// implied with these three objects. So the object pools of these three objects
// need an especial allocator to allocate themself.
template <class T>
class ObMetaAllocator : public ObFixedMemAllocator {
public:
  // Create a new class specific ClassAllocator.
  // @param mod allocate mode.
  explicit ObMetaAllocator(const lib::ObLabel& label)
  {
    free_list_ = NULL;
    allocator_.set_label(label);
    (void)mutex_init(&allocator_lock_);
  }

  virtual ~ObMetaAllocator()
  {
    (void)mutex_destroy(&allocator_lock_);
  }

  // Allocates objects of the templated type.
  T* alloc()
  {
    int ret = OB_SUCCESS;
    T* obj = NULL;
    void* ptr = NULL;
    if (OB_SUCC(mutex_acquire(&allocator_lock_))) {
      if (OB_LIKELY(NULL != free_list_)) {
        ptr = free_list_;
        free_list_ = *(reinterpret_cast<void**>(ptr));
      } else {
        ptr = allocator_.alloc_aligned(sizeof(T), 16);
      }
      mutex_release(&allocator_lock_);
    }
    if (OB_LIKELY(NULL != ptr)) {
      obj = new (ptr) T();
    }
    return obj;
  }

  // Deallocates objects of the templated type.
  // @param ptr pointer to be freed.
  void free(T* ptr)
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(NULL != ptr)) {
      ptr->~T();
      if (OB_SUCC(mutex_acquire(&allocator_lock_))) {
        *(reinterpret_cast<void**>(ptr)) = free_list_;
        free_list_ = ptr;
        mutex_release(&allocator_lock_);
      }
    }
  }

  // Allocate objects of the templated type via the inherited interface
  // using void pointers.
  virtual void* alloc_void()
  {
    return reinterpret_cast<void*>(alloc());
  }

  // Deallocate objects of the templated type via the inherited
  // interface using void pointers.
  // @param ptr pointer to be freed.
  virtual void free_void(void* ptr)
  {
    free(reinterpret_cast<T*>(ptr));
  }

private:
  void* free_list_;
  PageArena<char> allocator_;
  ::ObMutex0 allocator_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObMetaAllocator);
};

class ObObjFreeListList {
  friend class ObObjFreeList;

public:
  static const int64_t MAX_NUM_FREELIST;
  static pthread_key_t objpool_key_;

  ~ObObjFreeListList()
  {}

  int create_freelist(ObObjFreeList*& free_list, const char* name, const int64_t obj_size, const int64_t obj_count,
      const int64_t alignment = 16, const ObMemCacheType cache_type = OP_RECLAIM, const bool is_meta = false,
      const lib::ObLabel& label = common::ObModIds::OB_CONCURRENCY_OBJ_POOL);

  static ObObjFreeListList& get_freelists()
  {
    ObObjFreeListList* instance = NULL;
    while (OB_UNLIKELY(once_ < 2)) {
      if (ATOMIC_BCAS(&once_, 0, 1)) {
        instance = new (std::nothrow) ObObjFreeListList();
        if (OB_LIKELY(NULL != instance)) {
          if (common::OB_SUCCESS != instance->init()) {
            _OB_LOG(ERROR, "failed to init object freelist list");
            delete instance;
            instance = NULL;
            (void)ATOMIC_BCAS(&once_, 1, 0);
          } else {
            instance_ = instance;
            (void)ATOMIC_BCAS(&once_, 1, 2);
          }
        } else {
          (void)ATOMIC_BCAS(&once_, 1, 0);
        }
      }
    }

    return (ObObjFreeListList&)*instance_;
  }

  int64_t get_next_free_slot()
  {
    return ATOMIC_FAA(&nr_freelists_, 1);
  }
  int64_t get_freelist_count()
  {
    return ATOMIC_LOAD(&nr_freelists_);
  }
  int get_info(ObVector<ObObjFreeList*>& flls);
  void dump();
  void dump_baselinerel();
  void snap_baseline();
  int64_t to_string(char* buf, const int64_t len) const;

private:
  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(fl_allocator_.init(*this, "ObjFreeList", sizeof(ObObjFreeList), 64, 16, OP_GLOBAL, true))) {
      OB_LOG(WARN, "failed to init ObjFreeList allocator", K(ret));
    } else if (OB_FAIL(tc_allocator_.init(*this, "ObThreadCache", sizeof(ObThreadCache), 64, 16, OP_RECLAIM, true))) {
      OB_LOG(WARN, "failed to init ObThreadCache allocator", K(ret));
    } else if (0 != pthread_key_create(&objpool_key_, nullptr)) {
      OB_LOG(WARN, "failed to init pthread key", K(ret));
    } else if (OB_FAIL(CO_THREAD_ATEXIT([]() {
                 void* ptr = pthread_getspecific(objpool_key_);
                 thread_shutdown_cleanup(ptr);
               }))) {
      OB_LOG(WARN, "failed to register exit callback", K(ret));
    } else {
      is_inited_ = true;
    }
    return ret;
  }

private:
  static volatile int64_t once_;  // for creating singleton instance

  ObObjFreeListList();

  static volatile ObObjFreeListList* instance_;
  ObAtomicSLL<ObObjFreeList> freelists_;  // global free list list
  volatile int64_t nr_freelists_;
  volatile int64_t mem_total_;

  bool is_inited_;
  ObMetaAllocator<ObThreadCache> tc_meta_allocator_;

  ObClassAllocator<ObObjFreeList> fl_allocator_;
  ObClassAllocator<ObThreadCache> tc_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObObjFreeListList);
};

inline int ObFixedMemAllocator::init(ObObjFreeListList& fll, const char* name, const int64_t obj_size,
    const int64_t obj_count, const int64_t alignment, const ObMemCacheType cache_type /* = OP_RECLAIM */,
    const bool is_meta /* = false */, const lib::ObLabel& label /* = common::ObModIds::OB_CONCURRENCY_OBJ_POOL */)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fll.create_freelist(fl_, name, obj_size, obj_count, alignment, cache_type, is_meta, label))) {
    // do nothing
  } else if (OB_ISNULL(fl_)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

inline int ObFixedMemAllocator::init(const char* name, const int64_t obj_size, const int64_t obj_count,
    const int64_t alignment, const ObMemCacheType cache_type /* = OP_RECLAIM */, const bool is_meta /* = false */,
    const lib::ObLabel& label /* = common::ObModIds::OB_CONCURRENCY_OBJ_POOL */)
{
  return init(ObObjFreeListList::get_freelists(), name, obj_size, obj_count, alignment, cache_type, is_meta, label);
}

struct ObThreadFreeList {
  ObThreadFreeList() : reserved_size_(64), allocated_(0), freelist_(NULL)
  {}
  ~ObThreadFreeList()
  {}

  int64_t reserved_size_;
  int64_t allocated_;
  void* freelist_;
};

template <class T>
volatile int64_t ObClassAllocator<T>::once_ = 0;
template <class T>
volatile ObClassAllocator<T>* ObClassAllocator<T>::instance_ = NULL;

template <class T>
struct OPNum {
  template <class Type, bool Cond = true>
  struct GetOPLocalNum {
    static const int64_t v = Type::OP_LOCAL_NUM;
  };

  template <class Type>
  struct GetOPLocalNum<Type, false> {
    static const int64_t v = sizeof(Type) < 16 * 1024 ? 32 : 8;
  };

  static const int64_t LOCAL_NUM = GetOPLocalNum<T, HAS_MEMBER(T, OP_LOCAL_NUM)>::v;

  template <class Type, bool Cond = true>
  struct GetOPLabel {
    static const int64_t v = Type::OP_LABEL;
  };

  template <class Type>
  struct GetOPLabel<Type, false> {
    static constexpr const char* v = common::ObModIds::OB_CONCURRENCY_OBJ_POOL;
  };

  static constexpr const char* LABEL = GetOPLabel<T, HAS_MEMBER(T, OP_LABEL)>::v;
};

template <class T>
template <class Type>
constexpr const char* OPNum<T>::GetOPLabel<Type, false>::v;

template <class T>
constexpr const char* OPNum<T>::LABEL;

template <class Type, bool Cond = true>
struct Constructor {
  Type* operator()(void* ptr)
  {
    return new (ptr) Type();
  }
};

template <class Type>
struct Constructor<Type, false> {
  Type* operator()(void* ptr)
  {
    return reinterpret_cast<Type*>(ptr);
  }
};

template <class T>
static T* call_constructor(T* ptr)
{
  Constructor<T, __is_default_constructible__<T>::value> construct;
  return construct(ptr);
}

template <class T>
inline void call_destructor(T* ptr)
{
  ptr->~T();
}

// object pool user interface
// Warning: 1. the cached memory can't be freed even through the thread is dead.
//          2. double free is very terriable.
//          3. because object pool uses singleton, please only use one of global,
//             tc or reclaim interfaces for each object type in the whole procject.
// Note:
// op_alloc,op_tc_alloc and op_reclaim_alloc call the default constructor if it exist,
// else it just reinterpret_cast ptr.
//
// op_alloc_args,op_tc_alloc_args and op_reclaim_args call the constructor with args.
// It uses placement new to construct instance, if args is null and there isn't public
// default constructor, compiler isn't happy.
//
// op_alloc_args uses global object freelist, save memory but performance is poor.
// op_tc_alloc_args uses thread local object free list, perfromance is better but
//   waste some memory.
// op_reclaim_alloc_args uses thread local object free list and with memory reclaim,
//   performace is good and object waste less memory.

// global pool allocator interface
#define op_alloc_args(type, args...)                                                    \
  ({                                                                                    \
    type* ret = NULL;                                                                   \
    common::ObClassAllocator<type>* instance = common::ObClassAllocator<type>::get(     \
        common::OPNum<type>::LOCAL_NUM, common::OP_GLOBAL, common::OPNum<type>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                  \
      void* tmp = instance->alloc_void();                                               \
      if (OB_LIKELY(NULL != tmp)) {                                                     \
        ret = new (tmp) type(args);                                                     \
      }                                                                                 \
    }                                                                                   \
    ret;                                                                                \
  })

#define op_alloc(type)                                                                  \
  ({                                                                                    \
    STATIC_ASSERT(sizeof(::new type()) > 0, "check new operator failed!");              \
    type* ret = NULL;                                                                   \
    common::ObClassAllocator<type>* instance = common::ObClassAllocator<type>::get(     \
        common::OPNum<type>::LOCAL_NUM, common::OP_GLOBAL, common::OPNum<type>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                  \
      ret = instance->alloc();                                                          \
    }                                                                                   \
    ret;                                                                                \
  })

#define op_free(ptr)                                                                                            \
  ({                                                                                                            \
    common::ObClassAllocator<__typeof__(*ptr)>* instance = common::ObClassAllocator<__typeof__(*ptr)>::get(     \
        common::OPNum<__typeof__(*ptr)>::LOCAL_NUM, common::OP_GLOBAL, common::OPNum<__typeof__(*ptr)>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                                          \
      instance->free(ptr);                                                                                      \
    }                                                                                                           \
  })

// thread cache pool allocator interface
#define op_tc_alloc_args(type, args...)                                             \
  ({                                                                                \
    type* ret = NULL;                                                               \
    common::ObClassAllocator<type>* instance = common::ObClassAllocator<type>::get( \
        common::OPNum<type>::LOCAL_NUM, common::OP_TC, common::OPNum<type>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                              \
      void* tmp = instance->alloc_void();                                           \
      if (OB_LIKELY(NULL != tmp)) {                                                 \
        ret = new (tmp) type(args);                                                 \
      }                                                                             \
    }                                                                               \
    ret;                                                                            \
  })

#define op_tc_alloc(type)                                                           \
  ({                                                                                \
    type* ret = NULL;                                                               \
    common::ObClassAllocator<type>* instance = common::ObClassAllocator<type>::get( \
        common::OPNum<type>::LOCAL_NUM, common::OP_TC, common::OPNum<type>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                              \
      ret = instance->alloc();                                                      \
    }                                                                               \
    ret;                                                                            \
  })

#define op_tc_free(ptr)                                                                                     \
  ({                                                                                                        \
    common::ObClassAllocator<__typeof__(*ptr)>* instance = common::ObClassAllocator<__typeof__(*ptr)>::get( \
        common::OPNum<__typeof__(*ptr)>::LOCAL_NUM, common::OP_TC, common::OPNum<__typeof__(*ptr)>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                                      \
      instance->free(ptr);                                                                                  \
    }                                                                                                       \
  })

// thread cache pool and reclaim allocator interface
#define op_reclaim_alloc_args(type, args...)                                             \
  ({                                                                                     \
    type* ret = NULL;                                                                    \
    common::ObClassAllocator<type>* instance = common::ObClassAllocator<type>::get(      \
        common::OPNum<type>::LOCAL_NUM, common::OP_RECLAIM, common::OPNum<type>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                   \
      void* tmp = instance->alloc_void();                                                \
      if (OB_LIKELY(NULL != tmp)) {                                                      \
        ret = new (tmp) type(args);                                                      \
      }                                                                                  \
    }                                                                                    \
    ret;                                                                                 \
  })

#define op_reclaim_alloc(type)                                                           \
  ({                                                                                     \
    STATIC_ASSERT(sizeof(::new type()) > 0, "check new operator failed!");               \
    type* ret = NULL;                                                                    \
    common::ObClassAllocator<type>* instance = common::ObClassAllocator<type>::get(      \
        common::OPNum<type>::LOCAL_NUM, common::OP_RECLAIM, common::OPNum<type>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                   \
      ret = instance->alloc();                                                           \
    }                                                                                    \
    ret;                                                                                 \
  })

#define op_reclaim_free(ptr)                                                                                     \
  ({                                                                                                             \
    common::ObClassAllocator<__typeof__(*ptr)>* instance = common::ObClassAllocator<__typeof__(*ptr)>::get(      \
        common::OPNum<__typeof__(*ptr)>::LOCAL_NUM, common::OP_RECLAIM, common::OPNum<__typeof__(*ptr)>::LABEL); \
    if (OB_LIKELY(NULL != instance)) {                                                                           \
      instance->free(ptr);                                                                                       \
    }                                                                                                            \
  })

}  // end of namespace common
}  // end of namespace oceanbase

#endif  // OB_LIB_CONCURRENCY_OBJPOOL_H_
