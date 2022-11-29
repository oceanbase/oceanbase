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

#include "lib/thread_local/thread_buffer.h"
#include <stdint.h>
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"

namespace
{
const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
}

namespace oceanbase
{
namespace common
{
ThreadSpecificBuffer::ThreadSpecificBuffer(const int32_t size)
    : key_(INVALID_THREAD_KEY), size_(size)
{
  create_thread_key();
}

ThreadSpecificBuffer::~ThreadSpecificBuffer()
{
  delete_thread_key();
}

int ThreadSpecificBuffer::create_thread_key()
{
  int ret = pthread_key_create(&key_, destroy_thread_key);
  if (0 != ret) {
    _OB_LOG(ERROR, "cannot create thread key:%d bt=%s", ret, lbt());
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERR_SYS;
}

int ThreadSpecificBuffer::delete_thread_key()
{
  int ret = -1;
  if (INVALID_THREAD_KEY != key_) {
    ret = pthread_key_delete(key_);
  }
  if (0 != ret) {
    _OB_LOG(WARN, "delete thread key key_ failed:%d", ret);
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERR_SYS;
}

void ThreadSpecificBuffer::destroy_thread_key(void *ptr)
{
  _OB_LOG(INFO, "delete thread specific buffer, ptr=%p", ptr);
  if (NULL != ptr) {
    ob_free(ptr);
    ptr = NULL;
  }
}

ThreadSpecificBuffer::Buffer *ThreadSpecificBuffer::get_buffer() const
{
  Buffer *buffer = NULL;
  if (INVALID_THREAD_KEY == key_ || size_ <= 0) {
    _OB_LOG_RET(ERROR, OB_NOT_INIT, "thread key must be initialized "
              "and size must great than zero, key:%u,size:%d", key_, size_);
  } else {
    void *ptr = pthread_getspecific(key_);
    if (NULL != ptr) {
      // got exist ptr;
      buffer = reinterpret_cast<Buffer *>(ptr);
    } else {
      ptr = ob_malloc(size_ + sizeof(Buffer), ObModIds::OB_THREAD_BUFFER);
      if (NULL == ptr) {
        // malloc failed;
        _OB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "malloc thread specific memory failed.");
      } else {
        int ret = pthread_setspecific(key_, ptr);
        if (0 != ret) {
          _OB_LOG(ERROR, "pthread_setspecific failed:%d", ret);
          ob_free(ptr);
          ptr = NULL;
        } else {
          _OB_LOG(DEBUG, "new thread specific buffer, addr=%p size=%d this=%p", ptr, size_, this);
          buffer = new(ptr) Buffer(static_cast<char *>(ptr) + sizeof(Buffer), size_);
        }
      }
    }
  }

  return buffer;
}

int ThreadSpecificBuffer::Buffer::advance(const int32_t size)
{
  int ret = OB_SUCCESS;
  if (size < 0) {
    if (end_ + size < start_) {
      ret = OB_INVALID_ARGUMENT;
    }
  } else if (end_ + size > end_of_storage_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    end_ += size;
  }
  if (end_ < start_ || end_ > end_of_storage_) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

void ThreadSpecificBuffer::Buffer::reset()
{
  end_ = start_;
}

char *ThreadSpecificBuffer::Buffer::ptr()
{
  return start_;
}

int ThreadSpecificBuffer::Buffer::write(const char *bytes, const int32_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bytes) || size < 0 || size > remain()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(end_, bytes, size);
    ret = advance(size);
  }
  return ret;
}

char *get_tc_buffer(int64_t &size)
{
  static ThreadSpecificBuffer thread_buffer;
  ThreadSpecificBuffer::Buffer *buffer = thread_buffer.get_buffer();
  char* buf = NULL;
  if (NULL != buffer) {
    buf = buffer->current();
    size = buffer->remain();
  }
  return buf;
}

} // end namespace chunkserver
} // end namespace oceanbase
