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

#ifndef OCEANBASE_COMMON_THREAD_BUFFER_
#define OCEANBASE_COMMON_THREAD_BUFFER_

#include <pthread.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
/**
 * Provide a memory allocate mechanism by allocate
 * a buffer associate with specific thread, whenever
 * this buffer allocate by user function in a thread
 * and free when thread exit.
 */
class ThreadSpecificBuffer
{
public:
  explicit ThreadSpecificBuffer(const int32_t size = MAX_THREAD_BUFFER_SIZE);
  ~ThreadSpecificBuffer();
  class Buffer
  {
  public:
    Buffer(char *start, const int32_t size)
        : end_of_storage_(start + size), end_(start) {}
    char *ptr();
    int write(const char *bytes, const int32_t size);
    int advance(const int32_t size);
    void reset() ;
    int32_t used() const { return static_cast<int32_t>(end_ - start_); }
    int32_t remain() const { return static_cast<int32_t>(end_of_storage_ - end_); }
    int32_t capacity() const { return static_cast<int32_t>(end_of_storage_ - start_); }
    char *current() const { return end_; }
  private:
    char *end_of_storage_;
    char *end_;
    char start_[0];
  };
  Buffer *get_buffer() const;
private:
  int create_thread_key();
  int delete_thread_key();
  static void destroy_thread_key(void *ptr);
  DISALLOW_COPY_AND_ASSIGN(ThreadSpecificBuffer);
private:
  static const int32_t MAX_THREAD_BUFFER_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
private:
  pthread_key_t key_;
  int32_t size_;
};

char *get_tc_buffer(int64_t &size);

} // end namespace chunkserver
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_THREAD_BUFFER_
