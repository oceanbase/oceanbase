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

#ifndef OCEANBASE_COMMON_THREAD_MEMPOOL_H_
#define OCEANBASE_COMMON_THREAD_MEMPOOL_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase {
namespace common {
struct DefaultAllocator {
  void* malloc(const int32_t nbyte)
  {
    ObMemAttr memattr;
    memattr.label_ = ObModIds::OB_THREAD_MEM_POOL;
    return ob_malloc(nbyte, memattr);
  }
  void free(void* ptr)
  {
    ob_free(ptr);
  };
};
class ObMemList {
  static const int64_t WARN_ALLOC_NUM = 1024;
  typedef DefaultAllocator MemAllocator;
  struct MemBlock {
    MemBlock* next;
  };

public:
  explicit ObMemList(const int32_t fixed_size);
  ~ObMemList();

public:
  void* get();
  void put(void* ptr, const int32_t max_free_num);
  int64_t inc_ref_cnt();
  int64_t dec_ref_cnt();
  int64_t get_ref_cnt();

private:
  MemAllocator alloc_;
  MemBlock* header_;
  int32_t size_;
  const int32_t fixed_size_;
  int64_t ref_cnt_;
  pthread_spinlock_t spin_;
};
class ObThreadMempool {
  static const pthread_key_t INVALID_THREAD_KEY = INT32_MAX;

public:
  static const int32_t DEFAULT_MAX_FREE_NUM = 0;

public:
  ObThreadMempool();
  ~ObThreadMempool();

public:
  int init(const int32_t fixed_size, const int32_t max_free_num);
  int destroy();
  void* alloc();
  void free(void* ptr);
  void set_max_free_num(const int32_t max_free_num);

private:
  static void destroy_thread_data_(void* ptr);

private:
  pthread_key_t key_;
  int32_t fixed_size_;
  int32_t max_free_num_;
};

extern void thread_mempool_init();
extern void thread_mempool_destroy();
extern void thread_mempool_set_max_free_num(const int32_t max_free_num);
extern void* thread_mempool_alloc();
extern void thread_mempool_free(void* ptr);
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_THREAD_MEMPOOL_H_
