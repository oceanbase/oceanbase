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

#ifndef OB_FIXED_SIZE_BLOCK_ALLOCATOR_H_
#define OB_FIXED_SIZE_BLOCK_ALLOCATOR_H_
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/ob_running_mode.h"

namespace oceanbase
{
namespace common
{
struct ObFixedSizeBufAllocInfo
{
  void * start_;
  uint64_t len_;
};

// Singleton class that designed for callers who ask fixed size memory allocation.
// For example, frequently asking 2MB memory for macro blocks in merging
// It maintains a large memory buffer and divided by the fixed block size.
// All the free block buffer are recorded in a queue where
// caller can acquire and release the pre divided block buffer.
template<int64_t SIZE>
class ObFixedSizeBlockAllocator
{
public:
  virtual ~ObFixedSizeBlockAllocator();
  int init(const int64_t block_num, const lib::ObLabel &label = ObModIds::OB_FIXED_SIZE_BLOCK_ALLOCATOR);
  void destroy();

  static ObFixedSizeBlockAllocator &get_instance();

  void *alloc();
  void free(void *ptr);

  // return if the given point is in the allocator range
  bool contains(void* ptr);
  int64_t get_free_block_num() const;
  OB_INLINE int64_t get_total_block_num() const { return total_block_num_; }
  OB_INLINE int64_t get_max_block_num() const { return max_block_num_; }

private:
  ObFixedSizeBlockAllocator();
  int init_max_block_num();
  int expand(const int64_t block_num);
  bool contains_internal(void* ptr) const;

public:
  static const int64_t MAX_MEMORY_ALLOCATION = OB_MAX_SYS_BKGD_THREAD_NUM * 2 * OB_DEFAULT_MACRO_BLOCK_SIZE; //256MB

private:
  static const int64_t MAX_MEMORY_IN_MINI_MODE = 64 * OB_DEFAULT_MACRO_BLOCK_SIZE; //128MB

  bool is_inited_;
  ObSpinLock lock_;
  int64_t total_block_num_;
  int64_t max_block_num_;
  ObArenaAllocator allocator_;
  ObFixedQueue<void> free_blocks_;
  ObList<ObFixedSizeBufAllocInfo, ObArenaAllocator> block_buf_list_;

  DISALLOW_COPY_AND_ASSIGN (ObFixedSizeBlockAllocator);
};

// In old days, ObPartitionBaseDataObReader acquires 2MB memory buffer without
// calling free() explicitly. Memory is released only in destructor. While now,
// ObFixedSizeBlockAllocator is a singleton class, we need a Guard class to help
// free memory automatically.
template<int64_t SIZE>
class ObFixedSizeBlockMemoryContext
{
  static constexpr int INIT_BLOCK_NUM = 8;
public:
  ObFixedSizeBlockMemoryContext();
  virtual ~ObFixedSizeBlockMemoryContext();

  int init();
  void destroy();

  void *alloc();
  void free(void *ptr);

  inline ObFixedSizeBlockAllocator<SIZE> & get_allocator() const
  {
    return fsb_allocator_;
  }
  ;
  inline int64_t get_block_size() const
  {
    return SIZE;
  }
  int64_t get_used_block_num();

private:
  bool is_inited_;
  int64_t used_block_num_;
  ObFixedSizeBlockAllocator<SIZE> & fsb_allocator_;

  DISALLOW_COPY_AND_ASSIGN (ObFixedSizeBlockMemoryContext);
};

typedef ObFixedSizeBlockAllocator<OB_DEFAULT_MACRO_BLOCK_SIZE> ObMacroBlockSizeMemoryAllocator;
typedef ObFixedSizeBlockMemoryContext<OB_DEFAULT_MACRO_BLOCK_SIZE> ObMacroBlockSizeMemoryContext;

// ======== Implementation of ObFixedSizeBlockAllocator ========

template<int64_t SIZE>
ObFixedSizeBlockAllocator<SIZE>::ObFixedSizeBlockAllocator() :
    is_inited_(false),
    lock_(common::ObLatchIds::FIXED_SIZE_ALLOCATOR_LOCK),
    total_block_num_(0),
    max_block_num_(0),
    allocator_(SET_USE_UNEXPECTED_500(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_FIXED_SIZE_BLOCK_ALLOCATOR))),
    free_blocks_(),
    block_buf_list_(allocator_)
{
}

template<int64_t SIZE>
int ObFixedSizeBlockAllocator<SIZE>::init(const int64_t block_num, const lib::ObLabel &label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    //COMMON_LOG(WARN, "fixed size block allocator is inited twice", K(ret));
  } else if (block_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(block_num));
  } else if (OB_FAIL(init_max_block_num())) {
    COMMON_LOG(WARN, "init max block number fail", K(ret));
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, label);
    SET_USE_UNEXPECTED_500(attr);
    ObSpinLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      if (OB_FAIL(free_blocks_.init(max_block_num_, global_default_allocator, attr))) {
        COMMON_LOG(WARN, "fail to init free blocks queue", K(ret));
      } else {
        allocator_.set_attr(attr);
        if (OB_FAIL(expand(block_num))) {
          COMMON_LOG(WARN, "fail to init cached block buffer", K(ret), K(block_num));
        }
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }

    if (IS_NOT_INIT) {
      destroy();
    }
  }
  return ret;
}

template<int64_t SIZE>
ObFixedSizeBlockAllocator<SIZE>::~ObFixedSizeBlockAllocator()
{
  destroy();
}

template<int64_t SIZE>
void ObFixedSizeBlockAllocator<SIZE>::destroy()
{
  free_blocks_.destroy();
  block_buf_list_.destroy();
  allocator_.reset();
  total_block_num_ = 0;
  max_block_num_ = 0;
  is_inited_ = false;
}

template<int64_t SIZE>
ObFixedSizeBlockAllocator<SIZE> &ObFixedSizeBlockAllocator<SIZE>::get_instance()
{
  static ObFixedSizeBlockAllocator<SIZE> instance_;
  return instance_;
}

template<int64_t SIZE>
void *ObFixedSizeBlockAllocator<SIZE>::alloc()
{
  void* ret_buf = NULL;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block allocator is not inited", K(ret));
  } else {
    while (OB_ISNULL(ret_buf) && OB_SUCC(ret)) {
      // total_block_num_ is only changed in expand() after new buf created
      // so it is safe to fetch the value first
      int64_t curr_total_block_num = total_block_num_;
      if (OB_FAIL(free_blocks_.pop(ret_buf))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // reach maximum block num, need expand capacity
          ObSpinLockGuard guard(lock_);
          ret = expand(curr_total_block_num * 2);
        } else {
          COMMON_LOG(WARN, "failed to pop from cache blocks", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret_buf = NULL;
  }
  return ret_buf;
}

template<int64_t SIZE>
void ObFixedSizeBlockAllocator<SIZE>::free(void *ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block allocator is not inited", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    if (!contains_internal(ptr)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      COMMON_LOG(ERROR, "ptr doesn't belong to the allocator", K(ret), K(ptr));
    } else {
      if (OB_FAIL(free_blocks_.push(ptr))) {
        ret = OB_ERR_UNEXPECTED;
        // should not happen
        COMMON_LOG(ERROR, "failed to push back block buf", K(ret), K(ptr));
      }
    }
  }
}

template<int64_t SIZE>
int64_t ObFixedSizeBlockAllocator<SIZE>::get_free_block_num() const
{
  int64_t ret_num = 0;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block allocator is not inited", K(ret));
  } else {
    ret_num = free_blocks_.get_curr_total();
  }
  return ret_num;
}

template<int64_t SIZE>
bool ObFixedSizeBlockAllocator<SIZE>::contains(void* ptr)
{
  bool ret_contains = false;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block allocator is not inited", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    ret_contains = contains_internal(ptr);
  }
  return ret_contains;
}

template<int64_t SIZE>
int ObFixedSizeBlockAllocator<SIZE>::init_max_block_num()
{
  int ret = OB_SUCCESS;
  const int64_t max_memory = lib::is_mini_mode() ? MAX_MEMORY_IN_MINI_MODE : MAX_MEMORY_ALLOCATION;
  if (SIZE > max_memory) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "block size is too large", K(ret), K(SIZE), K(max_memory));
  } else {
    max_block_num_ = max_memory / SIZE;
  }
  return ret;
}

template<int64_t SIZE>
int ObFixedSizeBlockAllocator<SIZE>::expand(const int64_t block_num)
{
  // private method used internally, no need to check is_inited_
  int ret = OB_SUCCESS;
  if (block_num <= total_block_num_) {
    // do nothing
  } else if (max_block_num_ == total_block_num_) {
    ret = OB_EXCEED_MEM_LIMIT;
    COMMON_LOG(WARN, "reach maximum block num, ", K(ret), K_(total_block_num),
        K(max_block_num_), K(SIZE));
  } else {
    int64_t expand_num = 0;
    if (block_num > max_block_num_) {
      expand_num = max_block_num_ - total_block_num_;
    } else {
      int64_t min_expand_len = std::min(2 * total_block_num_, max_block_num_);
      expand_num = std::max(min_expand_len, block_num) - total_block_num_;
    }

    uint64_t expand_len = expand_num * SIZE;
    void *expand_start = NULL;

    if (OB_ISNULL(expand_start = allocator_.alloc(expand_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "failed to allocate block buffer, ", K(ret));
    } else if (OB_FAIL(
        block_buf_list_.push_back(ObFixedSizeBufAllocInfo { expand_start, expand_len }))) {
      COMMON_LOG(ERROR, "failed to push back cached block buffer, ", K(ret), K(expand_start),
          K(expand_len));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expand_num; ++i) {
        void * ptr = (void*) ((uint64_t) expand_start + i * SIZE);
        if (OB_FAIL(free_blocks_.push(ptr))) {
          COMMON_LOG(WARN, "fail to push cached block ptr, ", K(ret), K(ptr));
        }
      }
      if (OB_SUCC(ret)) {
        total_block_num_ += expand_num;
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(expand_start)) {
      allocator_.free(expand_start);
    }
  }

  return ret;
}


template<int64_t SIZE>
bool ObFixedSizeBlockAllocator<SIZE>::contains_internal(void* ptr) const
{
  // internal use, no need to check is_inited_
  bool ret_contains = false;
  ObList<ObFixedSizeBufAllocInfo, ObArenaAllocator>::const_iterator it = block_buf_list_.begin();
  for (; !ret_contains && it != block_buf_list_.end(); it++) {
    void *start = it->start_;
    uint64_t len = it->len_;
    // check ptr is in any cached buf range and can align with the block_size
    if (((uint64_t) ptr) >= ((uint64_t) start) && ((uint64_t) ptr) < ((uint64_t) start) + len
        && 0 == ((uint64_t) ptr - (uint64_t) start) % SIZE) {
      ret_contains = true;
    }
  }
  return ret_contains;
}

// ======== Implementation of ObFixedSizeBlockAllocatorGuard ========

template<int64_t SIZE>
ObFixedSizeBlockMemoryContext<SIZE>::ObFixedSizeBlockMemoryContext() :
    is_inited_(false),
    used_block_num_(0),
    fsb_allocator_(ObFixedSizeBlockAllocator<SIZE>::get_instance())
{
}

template<int64_t SIZE>
ObFixedSizeBlockMemoryContext<SIZE>::~ObFixedSizeBlockMemoryContext()
{
  destroy();
}

template<int64_t SIZE>
int ObFixedSizeBlockMemoryContext<SIZE>::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "fixed size block memory context is inited twice, ", K(ret));
  } else if (OB_FAIL(fsb_allocator_.init(INIT_BLOCK_NUM))) {
    if (ret == OB_INIT_TWICE) {
      // fsb_allocator_ is a singleton class so it is expected that already been inited
      ret = OB_SUCCESS;
    } else {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "fixed size block allocator is not inited, ", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    used_block_num_ = 0;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

template<int64_t SIZE>
void ObFixedSizeBlockMemoryContext<SIZE>::destroy()
{
  if (0 != used_block_num_) {
    COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "not all block be freed, potential memory leak!", K(used_block_num_));
  }
  used_block_num_ = 0;
  is_inited_ = false;
}

template<int64_t SIZE>
void *ObFixedSizeBlockMemoryContext<SIZE>::alloc()
{
  void* ret_buf = NULL;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block memory context is not inited", K(ret));
  } else {
    ret_buf = fsb_allocator_.alloc();
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(ret_buf)) {
    ATOMIC_INC(&used_block_num_);
  }
  return ret_buf;
}

template<int64_t SIZE>
void ObFixedSizeBlockMemoryContext<SIZE>::free(void *ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block memory context is not inited", K(ret));
  } else if (!fsb_allocator_.contains(ptr)) {
    ret = OB_DATA_OUT_OF_RANGE;
    COMMON_LOG(ERROR, "block address is out of range", K(ret), K(ptr));
  } else {
    fsb_allocator_.free(ptr);
    ATOMIC_DEC(&used_block_num_);
  }
}

template<int64_t SIZE>
int64_t ObFixedSizeBlockMemoryContext<SIZE>::get_used_block_num()
{
  int ret = OB_SUCCESS;
  int64_t ret_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "fixed size block memory context is not inited", K(ret));
  } else {
    ret_num = ATOMIC_LOAD(&used_block_num_);
  }
  return ret_num;
}

} /* namespace common */
} /* namespace oceanbase*/

#endif /* OB_FIXED_SIZE_BLOCK_ALLOCATOR_H_ */
