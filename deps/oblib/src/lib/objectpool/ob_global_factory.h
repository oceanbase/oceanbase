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

#ifndef LIB_OBJECTPOOL_OB_GLOBAL_FACTORY_
#define LIB_OBJECTPOOL_OB_GLOBAL_FACTORY_
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/list/ob_dlist.h"
namespace oceanbase
{
namespace common
{
class ObTCBlock;
class ObTCBlock: public common::ObDLinkBase<common::ObTCBlock>
{
public:
  static ObTCBlock *new_block(int32_t obj_size, const lib::ObLabel &label);
  static void delete_block(ObTCBlock *blk);
  static ObTCBlock *get_block_by_obj(void *obj);
  static int32_t get_batch_count_by_obj_size(int32_t obj_size);

  bool can_delete() const;
  void *alloc();
  void free(void *ptr);
  bool is_empty() const { return NULL == freelist_; }
  // for stat purpose only
  int32_t get_in_use_count() const { return in_use_count_; }
  int32_t get_obj_size() const { return obj_size_; }
  static int32_t get_obj_num_by_obj_size(int32_t obj_size);
private:
  explicit ObTCBlock(int32_t obj_size);
  ~ObTCBlock() {};
  struct FreeNode
  {
    FreeNode *next_;
  };
  void freelist_push(void *ptr);
  void *freelist_pop();
  static int32_t get_obj_num_by_aligned_obj_size(int32_t aligned_obj_size);
  static int32_t get_aligned_obj_size(int32_t obj_size);
private:
  static const int64_t BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE; // ~2MB
  static const uint32_t OBJ_SIZE_ALIGN_SHIFT = sizeof(void *);
  static const uint32_t OBJ_SIZE_ALIGN_MASK = (~0U) << OBJ_SIZE_ALIGN_SHIFT;
private:
  int32_t obj_size_;
  int32_t in_use_count_;
  FreeNode *freelist_;
  char data_[0];
};

inline ObTCBlock *ObTCBlock::new_block(int32_t obj_size, const lib::ObLabel &label)
{
  ObTCBlock *blk_ret = NULL;
  ObMemAttr memattr(OB_SERVER_TENANT_ID, label);
  void *ptr = ob_malloc(BLOCK_SIZE, memattr);
  if (OB_ISNULL(ptr)) {
    LIB_LOG_RET(WARN, common::OB_ALLOCATE_MEMORY_FAILED, "no memory");
  } else {
    blk_ret = new(ptr) ObTCBlock(obj_size);
  }
  return blk_ret;
}

inline void ObTCBlock::delete_block(ObTCBlock *blk)
{
  if (OB_ISNULL(blk) || OB_UNLIKELY(false == blk->can_delete())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "block is NULL or block can not be deleted", K(blk));
  } else {
    blk->~ObTCBlock();
    ob_free(blk);
  }
}

inline int32_t ObTCBlock::get_batch_count_by_obj_size(int32_t obj_size)
{
  // Checking the parameters here is just to hit the log
  if (OB_UNLIKELY(obj_size < 0)) {
    LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "obj_size < 0, is invalid");
  } else {}
  int32_t batch_count = static_cast<int32_t>(BLOCK_SIZE) / obj_size;
  if (batch_count < 2) {
    batch_count = 2;
  } else if (batch_count > 32) {
    batch_count = 32;
  } else {}
  return batch_count;
}

inline ObTCBlock *ObTCBlock::get_block_by_obj(void *obj)
{
  return *reinterpret_cast<ObTCBlock **>(reinterpret_cast<char *>(obj) - sizeof(void *));
}

inline void ObTCBlock::freelist_push(void *ptr)
{
  if (OB_ISNULL(ptr)) {
    LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument, ptr is NULL");
  } else {
    FreeNode *node = reinterpret_cast<FreeNode *>(ptr);
    node->next_ = freelist_;
    freelist_ = node;
  }
}

inline void *ObTCBlock::freelist_pop()
{
  void *ptr_ret = NULL;
  if (NULL != freelist_) {
    ptr_ret = freelist_;
    freelist_ = freelist_->next_;
  } else {}
  return ptr_ret;
}

inline int32_t ObTCBlock::get_aligned_obj_size(int32_t obj_size)
{
  /* we make the object address 8 bytes aligned */
  int32_t aligned_obj_size = static_cast<int32_t>(obj_size + sizeof(void *));
  if (0 != (aligned_obj_size % OBJ_SIZE_ALIGN_SHIFT)) {
    aligned_obj_size = (aligned_obj_size + OBJ_SIZE_ALIGN_SHIFT) & OBJ_SIZE_ALIGN_MASK;
  } else {}
  return aligned_obj_size;
}

inline int32_t ObTCBlock::get_obj_num_by_aligned_obj_size(int32_t aligned_obj_size)
{
  return static_cast<int32_t>((BLOCK_SIZE - sizeof(ObTCBlock)) / aligned_obj_size);
}

inline int32_t ObTCBlock::get_obj_num_by_obj_size(int32_t obj_size)
{
  return get_obj_num_by_aligned_obj_size(get_aligned_obj_size(obj_size));
}

inline ObTCBlock::ObTCBlock(int32_t obj_size)
    : obj_size_(obj_size),
      in_use_count_(0),
      freelist_(NULL),
      data_()
{
  int ret = OB_SUCCESS;
  int32_t aligned_obj_size = get_aligned_obj_size(obj_size);
  int32_t obj_count = get_obj_num_by_aligned_obj_size(aligned_obj_size);
  for (int32_t i = 0; OB_SUCC(ret) && i < obj_count; ++i) {
    char *ptr = &data_[i * aligned_obj_size];
    *reinterpret_cast<void **>(ptr) = this; // pointer to this block
    ptr += sizeof(void *);
    freelist_push(ptr);
  }
  if (OB_UNLIKELY(0 == obj_count)) {
    LIB_LOG(ERROR, "object too large", K(obj_size), K(aligned_obj_size));
  } else {}
}

inline bool ObTCBlock::can_delete() const
{
  return (0 == in_use_count_);
}

inline void *ObTCBlock::alloc()
{
  void *ptr_ret = freelist_pop();
  if (NULL != ptr_ret) {
    ++in_use_count_;
  }
  return ptr_ret;
}

inline void ObTCBlock::free(void *ptr)
{
  if (OB_ISNULL(ptr)) {
    LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "ptr is NULL");
  } else if (OB_UNLIKELY((char *)ptr - (char *)this >= BLOCK_SIZE)) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "size is larger than BLOCK_SIZE", K(ptr), K(this),
            K(((char *)ptr - (char *)this)), LITERAL_K(BLOCK_SIZE));
  } else {
    freelist_push(ptr);
    --in_use_count_;
  }
}
////////////////////////////////////////////////////////////////
template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
class ObGlobalFactory;

template<typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
class ObGlobalFreeList
{
  typedef typename ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::create_method_t create_method_t;
public:
  ObGlobalFreeList()
    : lock_(ObLatchIds::OB_GLOBAL_FREE_LIST_LOCK),
      next_cache_line_(0),
      objs_cache_(),
      empty_blocks_(),
      nonempty_blocks_()
  {
  }
  virtual ~ObGlobalFreeList() {}

  void push_range(common::ObDList<T> &range, int32_t num_to_move)
  {
    int32_t num = range.get_size();
    while (num >= num_to_move) {
      common::ObDList<T> subrange;
      range.pop_range(num_to_move, subrange);
      num -= num_to_move;
      ObRecursiveMutexGuard guard(lock_);
      if (next_cache_line_ < CACHE_SIZE) {
        objs_cache_[next_cache_line_++].push_range(subrange);
      } else {
        // cache is full
        release_objs(subrange);
        break;
      }
    } // end while
    if (0 < num) {
      ObRecursiveMutexGuard guard(lock_);
      release_objs(range);
    }
  }

  int pop_range(int32_t num, int32_t num_to_move,
                int32_t obj_size, create_method_t creator,
                common::ObDList<T> &range)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(range.get_size() > num_to_move)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "range size is larger than number to move",
              K(range.get_size()), K(num_to_move));
    } else {
      ObRecursiveMutexGuard guard(lock_);
      if (num == num_to_move) {
        if (next_cache_line_ > 0) {
          range.push_range(objs_cache_[--next_cache_line_]);
        } else {
          // cache is empty
          ret = create_objs(num, obj_size, creator, range);
        }
      } else {
        // allocate num new objects from blocks
        ret = create_objs(num, obj_size, creator, range);
      }
    }
    return ret;
  }

  void stat()
  {
    ObRecursiveMutexGuard guard(lock_);
    LIB_LOG(INFO, "[GFACTORY_STAT] freelist next_cache_line",
            "next_cache_line", next_cache_line_);
    LIB_LOG(INFO, "[GFACTORY_STAT] freelist empty_blocks_num",
            "empty_blocks_num", empty_blocks_.get_size());
    LIB_LOG(INFO, "[GFACTORY_STAT] freelist nonempty_blocks_num",
            "nonempty_blocks_num", nonempty_blocks_.get_size());
    DLIST_FOREACH_NORET(blk, nonempty_blocks_) {
      if (OB_ISNULL(blk)) {
        LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "nonempty_block is NULL");
      } else {
        const int32_t obj_size = blk->get_obj_size();
        LIB_LOG(INFO, "[GFACTORY_STAT] nonempty_block",
                K(obj_size), "in_use_count", blk->get_in_use_count(),
                "total", blk->get_obj_num_by_obj_size(obj_size));
      }
    }
  }
private:
  // types and constants
  static const int64_t CACHE_SIZE = 64;
private:
  // function members
  void release_objs(common::ObDList<T> &range)
  {
    DLIST_REMOVE_ALL_NORET(ptr, range) {
      ObTCBlock *blk = NULL;
      if (OB_ISNULL(ptr)) {
        LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj ptr is NULL");
      } else if (OB_ISNULL(blk = ObTCBlock::get_block_by_obj(ptr))) {
        LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "block is NULL");
      } else {
        if (blk->is_empty()) {
          // move from empty list to nonempty list
          empty_blocks_.remove(blk);
          if (OB_UNLIKELY(false == nonempty_blocks_.add_last(blk))) {
            LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "fail to add block to nonempty_blocks_'s tail");
          }
        } else {}
        ptr->~T();
        blk->free(ptr);
        if (blk->can_delete()) {
          nonempty_blocks_.remove(blk);
          ObTCBlock::delete_block(blk);
        } else {}
      }
    }
  }

  int create_objs(int32_t num,
                  int32_t obj_size,
                  create_method_t creator,
                  common::ObDList<T> &range)
  {
    int ret = OB_SUCCESS;
    for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      if (nonempty_blocks_.get_size() <= 0) {
        //
        ObTCBlock *blk = ObTCBlock::new_block(obj_size, LABEL);
        if (OB_ISNULL(blk)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(ERROR, "fail to alloc block", K(ret));
        } else {
          void *ptr = blk->alloc();
          if (OB_ISNULL(ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LIB_LOG(ERROR, "fail to alloc memory from block", K(ret));
          } else {
            T *obj = creator(ptr);
            if (OB_UNLIKELY(false == range.add_first(obj))) {
              ret = OB_ERR_UNEXPECTED;
              LIB_LOG(ERROR, "fail to add obj to range list head", K(ret));
            } else {
              if (blk->is_empty()) {
                if (OB_UNLIKELY(false == empty_blocks_.add_last(blk))) {
                  ret = OB_ERR_UNEXPECTED;
                  LIB_LOG(ERROR, "fail to add block to empty block list tail", K(ret));
                }
              } else {
                if (OB_UNLIKELY(false == nonempty_blocks_.add_last(blk))) {
                  ret = OB_ERR_UNEXPECTED;
                  LIB_LOG(ERROR, "fail to add block to nonempty block list tail", K(ret));
                }
              }
            }
          }
        }
      } else {
        ObTCBlock *blk = nonempty_blocks_.get_first();
        if (OB_ISNULL(blk)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(ERROR, "fail to alloc block", K(ret));
        } else {
          void *ptr = blk->alloc();
          if (OB_ISNULL(ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LIB_LOG(ERROR, "fail to alloc memory from block", K(ret));
          } else {
            T *obj = creator(ptr);
            if (OB_UNLIKELY(false == range.add_first(obj))) {
              ret = OB_ERR_UNEXPECTED;
              LIB_LOG(ERROR, "fail to add obj to range list head", K(ret));
            } else {
              if (blk->is_empty()) {
                nonempty_blocks_.remove(blk);
                if (OB_UNLIKELY(false == empty_blocks_.add_last(blk))) {
                  ret = OB_ERR_UNEXPECTED;
                  LIB_LOG(ERROR, "fail to add block to empty block list tail", K(ret));
                }
              } else {}
            }
          }
        }
      }
    } // end for
    return ret;
  }
private:
  // data members
  ObRecursiveMutex lock_;
  int32_t next_cache_line_;
  common::ObDList<T> objs_cache_[CACHE_SIZE];

  common::ObDList<ObTCBlock> empty_blocks_;
  common::ObDList<ObTCBlock> nonempty_blocks_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGlobalFreeList);
};
////////////////////////////////////////////////////////////////
template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
class ObGlobalFactory
{
public:
  typedef T *(*create_method_t)(void *ptr);

  static ObGlobalFactory *get_instance();

  int register_class(int32_t type_id, create_method_t creator, int32_t obj_size);
  int init();

  int get_objs(int32_t type_id, int32_t num, common::ObDList<T> &objs);
  void put_objs(int32_t type_id, common::ObDList<T> &objs);

  int32_t get_batch_count(int32_t type_id) const {return batch_count_[type_id];};
  int32_t get_obj_size(int32_t type_id) const {return obj_size_[type_id];};

  void stat();
private:
  // types and constants
  typedef ObGlobalFactory<T, MAX_CLASS_NUM, LABEL> self_t;
  typedef ObGlobalFreeList<T, MAX_CLASS_NUM, LABEL> freelist_t;
private:
  // function members
  ObGlobalFactory()
  {
    memset(create_methods_, 0, sizeof(create_methods_));
    memset(obj_size_, 0, sizeof(obj_size_));
    memset(batch_count_, 0, sizeof(batch_count_));
  };
  virtual ~ObGlobalFactory() {};
private:
  // static members
  static self_t *INSTANCE;
  // data members
  freelist_t freelists_[MAX_CLASS_NUM];
  create_method_t create_methods_[MAX_CLASS_NUM];
  int32_t obj_size_[MAX_CLASS_NUM];
  int32_t batch_count_[MAX_CLASS_NUM];
private:
  DISALLOW_COPY_AND_ASSIGN(ObGlobalFactory);
};

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
int ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::register_class(int32_t type_id,
                                                             create_method_t creator,
                                                             int32_t obj_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > type_id) || OB_UNLIKELY(type_id >= MAX_CLASS_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid type_id", K(ret), K(type_id), K(MAX_CLASS_NUM));
  } else if (OB_ISNULL(creator) || OB_UNLIKELY(0 >= obj_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments", K(ret), K(creator), K(obj_size));
  } else if (OB_UNLIKELY(NULL != create_methods_[type_id])
             || OB_UNLIKELY(0 != obj_size_[type_id])) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "class already registered", K(ret), K(type_id));
  } else {
    create_methods_[type_id] = creator;
    obj_size_[type_id] = obj_size;
  }
  return ret;
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
int ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::init()
{
  int ret = OB_SUCCESS;
  for (int64_t type_id = 0; OB_SUCC(ret) && type_id < MAX_CLASS_NUM; ++type_id) {
    if (NULL == create_methods_[type_id]) {
      //_OB_LOG(WARN, "class not registered, type_id=%d", type_id);
    } else {
      batch_count_[type_id] = ObTCBlock::get_batch_count_by_obj_size(obj_size_[type_id]);
    }
  }
  return ret;
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
ObGlobalFactory<T, MAX_CLASS_NUM, LABEL> *ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::INSTANCE = NULL;

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
ObGlobalFactory<T, MAX_CLASS_NUM, LABEL> *ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::get_instance()
{
  if (OB_ISNULL(INSTANCE)) {
    INSTANCE = new(std::nothrow) self_t();
    if (OB_ISNULL(INSTANCE)) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "failed to create singleton instance");
    } else {}
  } else {}
  return INSTANCE;
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
int ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::get_objs(int32_t type_id, int32_t num,
                                                        common::ObDList<T> &objs)
{
  freelist_t &l = freelists_[type_id];
  return l.pop_range(num, get_batch_count(type_id), get_obj_size(type_id),
                     create_methods_[type_id], objs);
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
void ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::put_objs(int32_t type_id,
                                                        common::ObDList<T> &objs)
{
  freelist_t &l = freelists_[type_id];
  l.push_range(objs, get_batch_count(type_id));
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL>
void ObGlobalFactory<T, MAX_CLASS_NUM, LABEL>::stat()
{
  int ret = OB_SUCCESS;
  for (int32_t type_id = 0; OB_SUCC(ret) && type_id < MAX_CLASS_NUM; ++type_id) {
    if (NULL != create_methods_[type_id]) {
      LIB_LOG(INFO, "[GFACTORY_STAT]", K(type_id),
              "obj_size", obj_size_[type_id],
              "batch_count", batch_count_[type_id],
              "creator", create_methods_[type_id]);
      freelists_[type_id].stat();
    }
  }
}
} // end namespace common
} // end namespace oceanbase

// utility for register class to global factory
#define DEFINE_CREATOR(T, D)                                                              \
  static T* tc_factory_create_##D(void *ptr)                                              \
  {                                                                                       \
    T* obj_ret = new(ptr) D();                                                            \
    return obj_ret;                                                                       \
  }


#define REGISTER_CREATOR(FACTORY, T, D, TYPE_ID)                                          \
  DEFINE_CREATOR(T, D);                                                                   \
  struct D##RegisterToFactory                                                             \
  {                                                                                       \
    D##RegisterToFactory()                                                                \
    {                                                                   \
      FACTORY::get_instance()->register_class(TYPE_ID, tc_factory_create_##D, sizeof(D)); \
      if (0) {                                                          \
        LIB_LOG(INFO, "register class " #D , "type_id", TYPE_ID,        \
                "gfactory", FACTORY::get_instance());                   \
      }                                                                 \
                                                                        \
    }                                                                   \
  } register_##D;

#endif /* LIB_OBJECTPOOL_OB_GLOBAL_FACTORY_ */
