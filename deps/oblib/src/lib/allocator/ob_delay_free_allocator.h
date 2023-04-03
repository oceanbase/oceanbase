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

#ifndef OB_DELAY_FREE_ALLOCATOR_H_
#define OB_DELAY_FREE_ALLOCATOR_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace common
{
//NOT thread safe.
//A double linked 2MB MemBlock. There is a variable "obj_count_" in each
//MemBlock, which records the current active object number in this MemBlock. After each object
//alloc from the MemBlock, the obj_count_ add 1; After each object of the MemBlock free, the
//obj_count_ sub 1. If the "obj_count_" of a MemBlock become 0 after a free action, this MemBlock
//can be recycled. After expire duration time, a recycled MemBlock can be freed.
class ObDelayFreeMemBlock: public common::ObDLinkBase<ObDelayFreeMemBlock>
{
public:
  explicit ObDelayFreeMemBlock(char *end);
  virtual ~ObDelayFreeMemBlock();
  inline int64_t remain() const;
  inline int64_t data_size() const;
  inline int64_t capacity() const;
  inline int64_t size() const;
  inline bool can_recycle() const;
  inline bool can_expire(const int64_t expire_duration_us) const;
  inline void *alloc(const int64_t size);
  inline void free(void *ptr);
  inline void reuse();
private:
  int64_t free_timestamp_;
  int64_t obj_count_;
  char *end_;
  char *current_;
  char data_[0];
private:
  DISALLOW_COPY_AND_ASSIGN(ObDelayFreeMemBlock);
};

//Thread safe depends on the initialize parameters.
//There are two double linked memory block list in it, working memblock list and free memblock
//list. The memory is allocated from working memblock list. If a memblock can be recycled, it will
//be moved from working list to free list. If a memblock in free list can be retired, it will be moved
//from free list to working list. If the size of free list exceeds MAX_CACHE_MEMORY_SIZE, the expi-
//red memblock will be freed.
class ObDelayFreeAllocator: public ObIAllocator
{
public:
  ObDelayFreeAllocator();
  virtual ~ObDelayFreeAllocator();
  int init(const lib::ObLabel &label, const bool is_thread_safe, const int64_t expire_duration_us);
  void destroy();
  virtual void *alloc(const int64_t sz);
  virtual void *alloc(const int64_t sz, const ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc(sz);
  }
  virtual void free(void *ptr);
  virtual void set_label(const lib::ObLabel &label);
  void reset();
  inline int64_t get_memory_fragment_size() const;
  inline int64_t get_total_size() const;
private:
  struct DataMeta
  {
    int64_t data_len_;
    ObDelayFreeMemBlock *mem_block_;
  };
  static const int64_t MEM_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t MEM_BLOCK_DATA_SIZE = MEM_BLOCK_SIZE - sizeof(ObDelayFreeMemBlock);
  static const int64_t MAX_CACHE_MEMORY_SIZE = OB_MALLOC_BIG_BLOCK_SIZE * 32;
private:
  ObDelayFreeMemBlock *alloc_block(const int64_t data_size);
  void free_block(ObDelayFreeMemBlock *block);
  void free_list(ObDList<ObDelayFreeMemBlock> &list);
private:
  ObDList<ObDelayFreeMemBlock> working_list_;
  ObDList<ObDelayFreeMemBlock> free_list_;
  int64_t cache_memory_size_;
  int64_t memory_fragment_size_;
  int64_t total_size_;
  int64_t expire_duration_us_;
  lib::ObLabel label_;
  ObTCMalloc allocator_;
  lib::ObMutex mutex_;
  bool inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDelayFreeAllocator);
};

//----------------------------------------------inline function implement-------------------------------------------

inline int64_t ObDelayFreeMemBlock::remain() const
{
  return end_ - current_;
}

inline int64_t ObDelayFreeMemBlock::data_size() const
{
  return current_ - data_;
}

inline int64_t ObDelayFreeMemBlock::capacity() const
{
  return end_ - data_;
}

inline int64_t ObDelayFreeMemBlock::size() const
{
  return end_ - data_ + sizeof(ObDelayFreeMemBlock);
}

inline bool ObDelayFreeMemBlock::can_recycle() const
{
  return 0 == obj_count_;
}

inline bool ObDelayFreeMemBlock::can_expire(const int64_t expire_duration_us) const
{
  return 0 == obj_count_ && ::oceanbase::common::ObTimeUtility::current_time() - free_timestamp_ >= expire_duration_us;
}

inline void *ObDelayFreeMemBlock::alloc(const int64_t size)
{
  char *ptr_ret = NULL;
  if (size <= remain()) {
    ptr_ret = current_;
    current_ += size;
    ++obj_count_;
  }
  return ptr_ret;
}

inline void ObDelayFreeMemBlock::free(void *ptr)
{
  UNUSED(ptr);
  --obj_count_;
  free_timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
}

inline void ObDelayFreeMemBlock::reuse()
{
  free_timestamp_ = 0;
  obj_count_ = 0;
  prev_ = NULL;
  next_ = NULL;
  // end_ doesn't need reset
  current_ = data_;
}

inline int64_t ObDelayFreeAllocator::get_memory_fragment_size() const
{
  return memory_fragment_size_;
}

inline int64_t ObDelayFreeAllocator::get_total_size() const
{
  return total_size_;
}

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_DELAY_FREE_ALLOCATOR_H_ */
