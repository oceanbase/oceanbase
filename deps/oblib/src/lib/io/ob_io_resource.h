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

#ifndef OB_IO_RESOURCE_H
#define OB_IO_RESOURCE_H

#include "lib/io/ob_io_common.h"

namespace oceanbase {
namespace common {

class ObDisk;
struct ObDiskMemoryStat;
class ObIORequest;
class ObIOMaster;

template <int64_t SIZE>
class ObIOMemoryPool {
public:
  ObIOMemoryPool()
      : is_inited_(false), capacity_(0), free_count_(0), allocator_(nullptr), begin_ptr_(nullptr), bitmap_(nullptr)
  {}
  ~ObIOMemoryPool()
  {
    destroy();
  }
  int init(const int64_t block_count, ObIAllocator& allocator);
  void destroy();
  int alloc(void*& ptr);
  int free(void* ptr);
  bool contain(void* ptr);
  int64_t get_block_size() const
  {
    return SIZE;
  }

private:
  int init_bitmap(const int64_t block_count, ObIAllocator& allocator);
  void bitmap_unset(const int64_t idx);
  void bitmap_set(const int64_t idx);
  bool bitmap_test(const int64_t idx);

private:
  bool is_inited_;
  int64_t capacity_;
  int64_t free_count_;
  ObIAllocator* allocator_;
  ObFixedQueue<char> pool_;
  char* begin_ptr_;
  uint8_t* bitmap_;
};

class ObIOAllocator final {
public:
  ObIOAllocator();
  virtual ~ObIOAllocator();
  int init(const int64_t mem_limit, const int64_t page_size);
  void destroy();
  void* alloc(const int64_t size);
  void free(void* ptr);
  int64_t allocated();

private:
  static const int64_t MICRO_POOL_BLOCK_SIZE = 16L * 1024L + 2 * DIO_READ_ALIGN_SIZE;
  static const int64_t MACRO_POOL_BLOCK_SIZE = 2L * 1024L * 1024L + DIO_READ_ALIGN_SIZE;
  static const int64_t DEFAULT_MICRO_POOL_COUNT = 10L * 1000L;
  static const int64_t MAX_MICRO_POOL_COUNT = 100L * 1000L;
  static const int64_t DEFAULT_MACRO_POOL_COUNT = OB_MAX_SYS_BKGD_THREAD_NUM;
  static const int64_t MAX_MACRO_POOL_COUNT = OB_MAX_SYS_BKGD_THREAD_NUM * 4;
  static const int64_t MINI_MODE_MICRO_POOL_COUNT = 1L * 1000L;
  static const int64_t MINI_MODE_MACRO_POOL_COUNT = 16;

  ObConcurrentFIFOAllocator allocator_;
  int64_t limit_;
  bool is_inited_;
  ObIOMemoryPool<MICRO_POOL_BLOCK_SIZE> micro_pool_;
  ObIOMemoryPool<MACRO_POOL_BLOCK_SIZE> macro_pool_;
};

template <typename T>
class ObIOPool {
public:
  int init(const int64_t count, ObIAllocator& allocator);
  void destroy();
  int push(T* ptr)
  {
    return pool_.push(ptr);
  }
  int pop(T*& ptr)
  {
    return pool_.pop(ptr);
  }
  int64_t size() const
  {
    return pool_.capacity();
  }

private:
  ObFixedQueue<T> pool_;
};

class ObIOResourceManager {
public:
  static const int64_t MAX_CHANNEL_CNT = common::OB_MAX_DISK_NUMBER;
  ObIOResourceManager();
  virtual ~ObIOResourceManager();
  int init(const int64_t mem_limit);
  void destroy();

  void* alloc_memory(const int64_t size);
  void free_memory(void* ptr);
  int alloc_master(const int64_t request_count, ObIOMaster*& master);
  int free_master(ObIOMaster* master);
  int alloc_request(ObDiskMemoryStat& mem_stat, ObIORequest*& req);
  int free_request(ObIORequest* req);
  ObIOAllocator* get_allocator()
  {
    return &io_allocator_;
  }

private:
  bool inited_;
  ObIOAllocator io_allocator_;  // for limit memory of ObDisk
  ObConcurrentFIFOAllocator master_allocator_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif
