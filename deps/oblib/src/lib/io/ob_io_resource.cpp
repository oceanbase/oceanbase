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

#include "lib/io/ob_io_resource.h"
#include "lib/ob_running_mode.h"
#include "lib/io/ob_io_manager.h"

namespace oceanbase {
using namespace lib;
namespace common {

template <int64_t SIZE>
int ObIOMemoryPool<SIZE>::init(const int64_t block_count, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(block_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret));
  } else {
    allocator_ = &allocator;
    capacity_ = block_count;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(pool_.init(capacity_))) {
    COMMON_LOG(WARN, "fail to init memory pool", K(ret));
  } else if (OB_ISNULL(begin_ptr_ = reinterpret_cast<char*>(allocator_->alloc(capacity_ * SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to allocate block memory", K(ret));
  } else if (OB_FAIL(init_bitmap(capacity_, *allocator_))) {
    COMMON_LOG(WARN, "fail to init bitmap", K(ret), K(capacity_));
  } else {
    char* buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity_; ++i) {
      buf = begin_ptr_ + i * SIZE;
      if (OB_FAIL(pool_.push(buf))) {
        COMMON_LOG(WARN, "fail to push memory block to pool", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    free_count_ = capacity_;
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

template <int64_t SIZE>
void ObIOMemoryPool<SIZE>::destroy()
{
  is_inited_ = false;
  free_count_ = 0;
  pool_.destroy();

  if (nullptr != allocator_) {
    if (nullptr != begin_ptr_) {
      allocator_->free(begin_ptr_);
      begin_ptr_ = nullptr;
    }
    if (nullptr != bitmap_) {
      allocator_->free(bitmap_);
      bitmap_ = nullptr;
    }
  }
  allocator_ = nullptr;
}

template <int64_t SIZE>
int ObIOMemoryPool<SIZE>::alloc(void*& ptr)
{
  int ret = OB_SUCCESS;
  char* ret_ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(pool_.pop(ret_ptr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to pop memory block", K(ret));
    }
  } else {
    const int64_t idx = (ret_ptr - begin_ptr_) / SIZE;
    if (OB_UNLIKELY(idx < 0 || idx >= capacity_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid block index", K(ret), K(idx), K(ret_ptr), K(begin_ptr_));
    } else if (OB_UNLIKELY(bitmap_test(idx))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "block not free", K(ret), K(idx));
    } else {
      bitmap_set(idx);
      ATOMIC_DEC(&free_count_);
      ptr = ret_ptr;
    }
  }
  return ret;
}

template <int64_t SIZE>
int ObIOMemoryPool<SIZE>::free(void* ptr)
{
  int ret = OB_SUCCESS;
  const int64_t idx = ((char*)ptr - begin_ptr_) / SIZE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (nullptr == ptr || idx < 0 || idx >= capacity_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(idx), KP(ptr));
  } else if (OB_UNLIKELY(!bitmap_test(idx))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "double free", K(ret));
  } else {
    bitmap_unset(idx);
    if (OB_FAIL(pool_.push((char*)ptr))) {
      COMMON_LOG(WARN, "fail to pop memory block", K(ret));
    } else {
      ATOMIC_INC(&free_count_);
    }
  }
  return ret;
}

template <int64_t SIZE>
bool ObIOMemoryPool<SIZE>::contain(void* ptr)
{
  bool bret = (uint64_t)ptr > 0 && ((char*)ptr >= begin_ptr_) && ((char*)ptr < begin_ptr_ + capacity_ * SIZE) &&
              ((char*)ptr - begin_ptr_) % SIZE == 0;
  return bret;
}

template <int64_t SIZE>
int ObIOMemoryPool<SIZE>::init_bitmap(const int64_t block_count, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const int64_t bitmap_size = block_count % 8 == 0 ? block_count / 8 : block_count / 8 + 1;
  if (OB_ISNULL(bitmap_ = reinterpret_cast<uint8_t*>(allocator.alloc(bitmap_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to allocate bitmap memory", K(ret));
  } else {
    MEMSET(bitmap_, 0, bitmap_size);
    const int64_t empty_bit_count = bitmap_size * 8 - block_count;
    for (int64_t i = 0; i < empty_bit_count; ++i) {
      bitmap_set(block_count + i);
    }
  }
  return ret;
}

template <int64_t SIZE>
void ObIOMemoryPool<SIZE>::bitmap_unset(const int64_t idx)
{
  uint8_t old_byte = 0;
  uint8_t new_byte = 0;
  do {
    uint8_t mask = 1;
    old_byte = bitmap_[idx / 8];
    new_byte = old_byte & ~(mask << (idx % 8));
  } while (ATOMIC_CAS(&bitmap_[idx / 8], old_byte, new_byte) != old_byte);
}

template <int64_t SIZE>
void ObIOMemoryPool<SIZE>::bitmap_set(const int64_t idx)
{
  uint8_t old_byte = 0;
  uint8_t new_byte = 0;
  do {
    uint8_t mask = 1;
    old_byte = bitmap_[idx / 8];
    new_byte = old_byte | (mask << (idx % 8));
  } while (ATOMIC_CAS(&bitmap_[idx / 8], old_byte, new_byte) != old_byte);
}

template <int64_t SIZE>
bool ObIOMemoryPool<SIZE>::bitmap_test(const int64_t idx)
{
  uint8_t mask = 1;
  bool bret = 0 != (bitmap_[idx / 8] & (mask << (idx % 8)));
  return bret;
}

/*
 * -----------------------------------ObIOAllocator------------------------------------------
 */
ObIOAllocator::ObIOAllocator() : allocator_(), limit_(0), is_inited_(false)
{}

ObIOAllocator::~ObIOAllocator()
{
  destroy();
}

int ObIOAllocator::init(const int64_t mem_limit, const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "io allocator is inited twice", K(ret));
  } else if (mem_limit <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(mem_limit), K(page_size));
  } else if (OB_FAIL(allocator_.init(mem_limit, mem_limit, OB_SERVER_TENANT_ID, ObModIds::OB_SSTABLE_AIO, page_size))) {
    COMMON_LOG(WARN, "failed to init fifo allocator", K(ret), K(mem_limit), K(page_size));
  } else if (OB_FAIL(micro_pool_.init(
                 !lib::is_mini_mode() ? DEFAULT_MICRO_POOL_COUNT * 1 : MINI_MODE_MICRO_POOL_COUNT * 1, allocator_))) {
    COMMON_LOG(WARN, "failed to init micro block memory pool", K(ret));
  } else {
    const double MACRO_POOL_RATIO = 0.2;
    int64_t macro_pool_count = mem_limit * MACRO_POOL_RATIO / MACRO_POOL_BLOCK_SIZE;
    macro_pool_count = MAX(macro_pool_count, DEFAULT_MACRO_POOL_COUNT);
    macro_pool_count = MIN(macro_pool_count, MAX_MACRO_POOL_COUNT);
    if (OB_FAIL(
            macro_pool_.init(!lib::is_mini_mode() ? macro_pool_count : MINI_MODE_MACRO_POOL_COUNT * 2, allocator_))) {
      COMMON_LOG(WARN, "failed to init macro block memory pool", K(ret));
    } else {
      COMMON_LOG(INFO, "succ to init io macro pool", K(mem_limit), K(macro_pool_count));
    }
  }
  if (OB_SUCC(ret)) {
    limit_ = mem_limit;
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObIOAllocator::destroy()
{
  is_inited_ = false;
  limit_ = 0;
  micro_pool_.destroy();
  macro_pool_.destroy();
  allocator_.destroy();
}

void* ObIOAllocator::alloc(const int64_t size)
{
  void* ret_buf = NULL;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "io allocator is not inited", K(ret));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(size));
  } else {
    // try pop from cache block pool if size equals cache block size
    if (size <= micro_pool_.get_block_size()) {
      ret = micro_pool_.alloc(ret_buf);
    } else if (size == macro_pool_.get_block_size()) {
      ret = macro_pool_.alloc(ret_buf);
    }
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(ERROR, "failed to alloc buf from fixed size pool", K(ret), K(size));
      } else {
        ret = OB_SUCCESS;
      }
    }

    // get buf from normal allocator
    if (OB_SUCC(ret) && OB_ISNULL(ret_buf)) {
      if (OB_ISNULL(ret_buf = allocator_.alloc(size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "failed to allocate io mem block", K(ret), K(size));
      }
    }
  }
  return ret_buf;
}

void ObIOAllocator::free(void* ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "io allocator is not inited", K(ret));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(ptr));
  } else {
    // check if the ptr was assigned from cached block
    if (micro_pool_.contain(ptr)) {
      micro_pool_.free(ptr);
    } else if (macro_pool_.contain(ptr)) {
      macro_pool_.free(ptr);
    } else {
      allocator_.free(ptr);
    }
    ptr = NULL;
  }
}

int64_t ObIOAllocator::allocated()
{
  return allocator_.allocated();
}

/**
 * ---------------------------------------- ObIOPool -------------------------------
 */
template <typename T>
int ObIOPool<T>::init(const int64_t count, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  void* tmp = NULL;
  T* item = NULL;
  // invoked in init, don't check inited_
  if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(count));
  } else if (OB_FAIL(pool_.init(count))) {
    COMMON_LOG(WARN, "Fail to init pool, ", K(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_ISNULL(tmp = allocator.alloc(sizeof(T)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "Fail to allocate memory, ", K(ret));
      } else {
        item = new (tmp) T;
        if (OB_FAIL(item->init())) {
          COMMON_LOG(WARN, "fail to construct item", K(ret));
        } else if (OB_FAIL(pool_.push(item))) {
          COMMON_LOG(WARN, "Fail to push item to pool, ", K(ret));
        }
      }
    }
  }
  return ret;
}
template <typename T>
void ObIOPool<T>::destroy()
{
  int tmp_ret = OB_SUCCESS;
  T* ptr = NULL;

  while (OB_SUCCESS == tmp_ret) {
    ptr = NULL;
    tmp_ret = pool_.pop(ptr);
    if (OB_SUCCESS == tmp_ret) {
      ptr->~T();
    } else if (OB_ENTRY_NOT_EXIST != tmp_ret && OB_NOT_INIT != tmp_ret) {
      COMMON_LOG(ERROR, "failed to pop", K(tmp_ret));
    }
  }

  pool_.destroy();
}

/**
 * --------------------------------------- ObIOResourceManager -----------------------
 */
ObIOResourceManager::ObIOResourceManager() : inited_(false)
{}

ObIOResourceManager::~ObIOResourceManager()
{}

int ObIOResourceManager::init(const int64_t mem_limit)
{
  int ret = OB_SUCCESS;

  const int64_t page_size = OB_MALLOC_BIG_BLOCK_SIZE;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (mem_limit <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(mem_limit));
  } else if (OB_FAIL(master_allocator_.init(
                 mem_limit, mem_limit, OB_SERVER_TENANT_ID, ObModIds::OB_IO_CONTROL, page_size))) {
    COMMON_LOG(WARN, "fail to init master_allocator", K(ret), K(mem_limit), K(page_size));
  } else if (OB_FAIL(io_allocator_.init(mem_limit, page_size))) {
    COMMON_LOG(WARN, "Fail to init io allocator, ", K(ret), K(mem_limit));
  } else {
    inited_ = true;
  }

  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObIOResourceManager::destroy()
{
  io_allocator_.destroy();
  master_allocator_.destroy();
  inited_ = false;
}

void* ObIOResourceManager::alloc_memory(const int64_t size)
{
  return io_allocator_.alloc(size);
}

void ObIOResourceManager::free_memory(void* ptr)
{
  return io_allocator_.free(ptr);
}

int ObIOResourceManager::alloc_master(const int64_t request_count, ObIOMaster*& master)
{
  int ret = OB_SUCCESS;
  master = nullptr;
  void* buf = nullptr;
  ObIOMaster* tmp_master = nullptr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(request_count <= 0 || request_count > MAX_IO_BATCH_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(request_count));
  } else if (OB_ISNULL(buf = master_allocator_.alloc(sizeof(ObIOMaster) + request_count * sizeof(ObIORequest)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to alloc memory for io_master", K(ret));
  } else {
    tmp_master = new (buf) ObIOMaster;
    for (int64_t i = 0; i < request_count; ++i) {
      void* req_buf = reinterpret_cast<char*>(buf) + sizeof(ObIOMaster) + i * sizeof(ObIORequest);
      tmp_master->requests_[i] = new (req_buf) ObIORequest;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_master->init())) {
    COMMON_LOG(WARN, "fail to init io_master", K(ret));
    tmp_master->~ObIOMaster();
  } else {
    master = tmp_master;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    master_allocator_.free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObIOResourceManager::free_master(ObIOMaster* master)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(master)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(master));
  } else {
    master_allocator_.free(master);
  }
  return ret;
}

int ObIOResourceManager::alloc_request(ObDiskMemoryStat& mem_stat, ObIORequest*& req)
{
  int ret = OB_SUCCESS;
  UNUSED(mem_stat);
  req = nullptr;
  void* buf = nullptr;
  static ObIORequest req_template;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf = master_allocator_.alloc(sizeof(ObIORequest)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to alloc memory for io_request", K(ret));
  } else {
    MEMCPY(buf, &req_template, sizeof(ObIORequest));
    req = reinterpret_cast<ObIORequest*>(buf);
  }
  return ret;
}

int ObIOResourceManager::free_request(ObIORequest* req)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(req));
  } else {
    req->destroy();  // safe cause only destroy when init failed or normal finished(all requests came back)
    master_allocator_.free(req);
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
