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

#include "lib/allocator/ob_delay_free_allocator.h"
#include "lib/oblog/ob_log.h"


namespace oceanbase
{
namespace common
{
ObDelayFreeMemBlock::ObDelayFreeMemBlock(char *end)
  : ObDLinkBase<ObDelayFreeMemBlock>(), free_timestamp_(0), obj_count_(0), end_(end)
{
  current_ = data_;
}

ObDelayFreeMemBlock::~ObDelayFreeMemBlock()
{
}

ObDelayFreeAllocator::ObDelayFreeAllocator()
  : cache_memory_size_(0),
    memory_fragment_size_(0),
    total_size_(0),
    expire_duration_us_(0),
    label_(nullptr),
    mutex_(ObLatchIds::OB_DELAY_FREE_ALLOCATOR_LOCK),
    inited_(false)
{
}

ObDelayFreeAllocator::~ObDelayFreeAllocator()
{
  destroy();
}

int ObDelayFreeAllocator::init(
  const lib::ObLabel &label,
  const bool is_thread_safe,
  const int64_t expire_duration_us)
{
  UNUSED(is_thread_safe);
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LIB_ALLOC_LOG(WARN, "The ObBlockLinkMemoryAllocator has been inited.");
  } else if (expire_duration_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_ALLOC_LOG(WARN,
                  "Invaid arguments, ",
                  "label",
                  label,
                  "expire_duration_us",
                  expire_duration_us);
  } else {
    label_ = label;
    allocator_.set_label(label);
    expire_duration_us_ = expire_duration_us;
    inited_ = true;
  }

  return ret;
}

void ObDelayFreeAllocator::destroy()
{
  if (inited_) {
    free_list(working_list_);
    free_list(free_list_);
    cache_memory_size_ = 0;
    memory_fragment_size_ = 0;
    total_size_ = 0;
    expire_duration_us_ = 0;
    label_ = nullptr;
    inited_ = false;
  }
}

void *ObDelayFreeAllocator::alloc(const int64_t size)
{
  void *ptr_ret = NULL;
  ObDelayFreeMemBlock *block = NULL;
  int64_t data_size = size + sizeof(DataMeta);

  if (!inited_) {
    LIB_ALLOC_LOG_RET(WARN, OB_NOT_INIT, "The ObBlockLinkMemoryAllocator has not been inited.");
  } else {
    //alloc memory
    lib::ObMutexGuard guard(mutex_);
    if (working_list_.is_empty()) {
      block = alloc_block(data_size);
    } else {
      block = working_list_.get_last();
    }

    if (NULL == block) {
      LIB_ALLOC_LOG_RET(ERROR, OB_ERROR, "cannot malloc first meta memory block.");
    } else if (data_size <= block->remain()) {
      ptr_ret = block->alloc(data_size);
    } else if (NULL != (block = alloc_block(data_size))) {
      ptr_ret = block->alloc(data_size);
    } else {
      LIB_ALLOC_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "cannot malloc memory ", "size", size);
    }

    // set meta value
    if (NULL != ptr_ret) {
      DataMeta *meta = reinterpret_cast<DataMeta *>(ptr_ret);
      meta->data_len_ = data_size;
      meta->mem_block_ = block;
      ptr_ret = static_cast<void *>(meta + 1);
    }
  }
  return ptr_ret;
}

void ObDelayFreeAllocator::free(void *ptr)
{
  if (!inited_) {
    LIB_ALLOC_LOG_RET(WARN, OB_NOT_INIT, "The ObBlockLinkMemoryAllocator has not been inited.");
  } else if (NULL == ptr) {
    LIB_ALLOC_LOG_RET(WARN, OB_ERROR, "the free ptr is NULL");
  } else {
    lib::ObMutexGuard guard(mutex_);
    DataMeta *meta = reinterpret_cast<DataMeta *>(ptr) - 1;
    ObDelayFreeMemBlock *block = meta->mem_block_;

    if (NULL == block) {
      LIB_ALLOC_LOG_RET(ERROR, OB_ERROR, "the free ptr has null mem block ptr");
    } else {
      if (meta->data_len_ > 0) {
        memory_fragment_size_ += meta->data_len_;
        meta->data_len_ = 0;
        block->free(ptr);
        ptr = NULL;
        if (block->can_recycle()) {
          free_block(block);
        }
      }
    }
  }
}

void ObDelayFreeAllocator::set_label(const lib::ObLabel &label)
{
  label_ = label;
  allocator_.set_label(label);
}

void ObDelayFreeAllocator::reset()
{
  lib::ObMutexGuard guard(mutex_);

  free_list(working_list_);
  free_list(free_list_);
  cache_memory_size_ = 0;
  total_size_ = 0;
  memory_fragment_size_ = 0;
  // NOT reset expire_duration_us_
  // NOT reset label_
  // NOT reset inited_
}

ObDelayFreeMemBlock *ObDelayFreeAllocator::alloc_block(const int64_t data_size)
{
  ObDelayFreeMemBlock *block_ret = NULL;
  char *ptr = NULL;

  if (!free_list_.is_empty()
      && free_list_.get_first()->capacity() >= data_size
      && free_list_.get_first()->can_expire(expire_duration_us_)) {
    block_ret = free_list_.remove_first();
    cache_memory_size_ -= block_ret->size();
    block_ret->reuse();
  } else {
    int64_t mem_block_size = 0;
    if (data_size <= MEM_BLOCK_DATA_SIZE) {
      mem_block_size = MEM_BLOCK_SIZE;
    } else {
      mem_block_size = data_size + sizeof(ObDelayFreeMemBlock);
    }

    if (NULL == (ptr = static_cast<char *>(allocator_.alloc(mem_block_size)))) {
      LIB_ALLOC_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "cannot malloc memory of ", "mem_block_size", mem_block_size);
    } else if (NULL == (block_ret = new(ptr) ObDelayFreeMemBlock(ptr + mem_block_size))) {
      LIB_ALLOC_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "placement new for MemBlock failed.");
    } else {
      total_size_ += mem_block_size;
    }
  }

  if (NULL != block_ret) {
    working_list_.add_last(block_ret);
  }

  return block_ret;
}

void ObDelayFreeAllocator::free_block(ObDelayFreeMemBlock *block)
{
  if (NULL == block) {
    LIB_ALLOC_LOG_RET(WARN, OB_ERR_UNEXPECTED, "The free block is NULL.");
  } else {
    working_list_.remove(block);
    free_list_.add_last(block);

    // if there are too many blocks in the free list, try free some of them if possible
    memory_fragment_size_ -= block->data_size();
    cache_memory_size_ += block->size();
    while (cache_memory_size_ > MAX_CACHE_MEMORY_SIZE
           && (!free_list_.is_empty())
           && NULL != (block = free_list_.get_first())
           && block->can_expire(expire_duration_us_)) {
      free_list_.remove_first();
      total_size_ -= block->size();
      cache_memory_size_ -= block->size();
      allocator_.free(block);
      block = NULL;
    }
  }
}

void ObDelayFreeAllocator::free_list(ObDList<ObDelayFreeMemBlock> &list)
{
  ObDelayFreeMemBlock *block = NULL;
  while (NULL != (block = list.remove_first())) {
    allocator_.free(block);
    block = NULL;
  }
}

} /* namespace common */
} /* namespace oceanbase */
