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

#include "ob_buffer_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObBufferArena::ObBufferArena()
    : is_inited_(false),
      align_size_(OB_INVALID_SIZE),
      header_size_(OB_INVALID_SIZE),
      trailer_size_(OB_INVALID_SIZE),
      buffer_size_(OB_INVALID_SIZE),
      buffer_count_(OB_INVALID_COUNT),
      alloc_start_(NULL),
      alloc_bytes_(OB_INVALID_SIZE)
{}

ObBufferArena::~ObBufferArena()
{
  if (NULL != alloc_start_) {
    ob_free(alloc_start_);
    alloc_start_ = NULL;
  }
  align_size_ = OB_INVALID_SIZE;
  header_size_ = OB_INVALID_SIZE;
  trailer_size_ = OB_INVALID_SIZE;
  buffer_size_ = OB_INVALID_SIZE;
  buffer_count_ = OB_INVALID_COUNT;
  alloc_bytes_ = OB_INVALID_SIZE;
  is_inited_ = false;
}

int ObBufferArena::init(const char* label, int64_t align_size, int64_t header_size, int64_t trailer_size,
    int64_t buffer_size, int64_t buffer_count)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!is2n(align_size) || header_size < 0 || trailer_size < 0 || buffer_size <= 0 || buffer_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (alloc_start_ = (char*)ob_malloc(buffer_size * buffer_count + align_size, label))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    align_size_ = align_size;
    header_size_ = header_size;
    trailer_size_ = trailer_size;
    buffer_size_ = buffer_size;
    buffer_count_ = buffer_count;
    alloc_bytes_ = 0;
    is_inited_ = true;
  }
  return ret;
}

char* ObBufferArena::alloc()
{
  int tmp_ret = OB_SUCCESS;
  char* ptr = NULL;
  char* ret_ptr = NULL;
  if (!is_inited_) {
    tmp_ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBufferArena not init", "ret", tmp_ret, K_(alloc_start));
  } else {
    if (alloc_bytes_ + align_size_ < buffer_size_ * buffer_count_) {
      ptr = (alloc_start_ + alloc_bytes_);
      ptr = (char*)(((int64_t)ptr + align_size_) & ~(align_size_ - 1));
      alloc_bytes_ += buffer_size_;
    }
    ret_ptr = ptr + header_size_;
  }
  return ret_ptr;
}
};  // end namespace clog
};  // end namespace oceanbase
