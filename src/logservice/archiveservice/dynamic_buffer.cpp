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

#include "dynamic_buffer.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_utility.h"
#include "share/rc/ob_tenant_base.h"
#include <cstdint>

namespace oceanbase
{
namespace archive
{
DynamicBuffer::DynamicBuffer(const int64_t MAX_SIZE) :
  DynamicBuffer("DynamicBuffer", MAX_SIZE)
{}

DynamicBuffer::DynamicBuffer(const char *label, const int64_t MAX_SIZE) :
  buf_size_(0),
  buf_(NULL),
  ref_(0),
  buf_limit_(MAX_SIZE),
  buf_gen_timestamp_(OB_INVALID_TIMESTAMP),
  buf_size_usage_(),
  label_(label)
{
  buf_size_usage_.set_attr(SET_USE_500(label));
}

DynamicBuffer::~DynamicBuffer()
{
  destroy();
}

char *DynamicBuffer::acquire(const int64_t size)
{
  char *buf = NULL;
  int64_t real_size = 0;
  if (size <= 0 || size > MAX_BUF_SIZE) {
    ARCHIVE_LOG_RET(WARN, OB_NOT_SUPPORTED, "get buf oversize, not support", K(size));
  } else if (FALSE_IT(real_size = size <= BASIC_BUF_SIZE ? size : upper_align_(size))) {
  } else if (0 != ATOMIC_LOAD(&ref_)) {
    ARCHIVE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "buffer ref not zero", KPC(this));
  } else if (NULL == buf_ || buf_size_ < size) {
    free_();
    alloc_(real_size);
  }
  if (NULL != buf_ && buf_size_ >= size) {
    buf = buf_;
    ATOMIC_INC(&ref_);
    add_statistic_(real_size);
  }
  return buf;
}

void DynamicBuffer::reclaim(void *ptr)
{
  if (ptr != buf_) {
    ARCHIVE_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid reclaim ptr", K(ptr), KPC(this));
  } else {
    ATOMIC_DEC(&ref_);
    if (buf_size_ <= BASIC_BUF_SIZE) {
      free_();
    } else if (need_wash_()) {
      wash_();
    }
  }
}

void DynamicBuffer::purge()
{
  if (NULL != buf_) {
    if (0 == ATOMIC_LOAD(&ref_)) {
      free_();
    } else {
      ARCHIVE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "buffer ref not zero, memory may leak", KPC(this));
    }
  }
}

bool DynamicBuffer::alloc_from(void *ptr)
{
  return ptr == buf_;
}

int DynamicBuffer::assign(const DynamicBuffer &other)
{
  buf_size_ = other.buf_size_;
  buf_ = other.buf_;
  ref_ = other.ref_;
  buf_limit_ = other.buf_limit_;
  buf_gen_timestamp_ = other.buf_gen_timestamp_;
  total_usage_ = other.total_usage_;
  label_ = other.label_;
  return buf_size_usage_.assign(other.buf_size_usage_);
}

// can not free memory in destroy, buf may be referenced
void DynamicBuffer::destroy()
{
  if (0 == ATOMIC_LOAD(&ref_)) {
    free_();
  }
  buf_limit_ = 0;
  buf_gen_timestamp_ = OB_INVALID_TIMESTAMP;
  buf_size_usage_.destroy();
}

int64_t DynamicBuffer::upper_align_(const int64_t input) const
{
  int64_t size = 0;
  if (input % BASIC_BUF_SIZE == 0) {
    size = input;
  } else {
    size = BASIC_BUF_SIZE;
    while (size < input) {
      size = size << 1;
      size = std::min(size, buf_limit_);
    }
  }
  return size;
}

void DynamicBuffer::alloc_(const int64_t size)
{
  void *data = NULL;
  if (OB_ISNULL(data = share::mtl_malloc(size, label_.ptr()))) {
    ARCHIVE_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc failed", K(size));
  } else {
    buf_ = (char *)data;
    buf_size_ = size;
    buf_gen_timestamp_ = common::ObTimeUtility::fast_current_time();
  }
}

void DynamicBuffer::free_()
{
  if (NULL != buf_) {
    share::mtl_free(buf_);
    buf_ = NULL;
    buf_size_ = 0;
  }
}

void DynamicBuffer::add_statistic_(const int64_t size)
{
  if (buf_size_usage_.count() == 0) {
    init_statstic_();
  }
  int64_t index = get_usage_index_(size);
  if (index >= buf_size_usage_.count()) {
    ARCHIVE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "get usage index failed", K(index), K(buf_size_usage_));
  } else {
    buf_size_usage_[index]++;
    total_usage_++;
  }
}

void DynamicBuffer::init_statstic_()
{
  int ret = OB_SUCCESS;
  if (buf_size_usage_.count() > 0) {
    buf_size_usage_.reset();
  }
  int64_t size = BASIC_BUF_SIZE;
  int64_t count = 1;
  while (size < buf_limit_) {
    size = size << 1;
    size = std::min(size, buf_limit_);
    count++;
  }
  for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
    ret = buf_size_usage_.push_back(0);
  }
  if (OB_FAIL(ret)) {
    buf_size_usage_.reset();
  }
}

void DynamicBuffer::purge_statistic_()
{
  for (int64_t i = 0; i < buf_size_usage_.count(); i++) {
    buf_size_usage_[i] = 0;
  }
  total_usage_ = 0;
}

int64_t DynamicBuffer::get_usage_index_(const int64_t input) const
{
  int64_t index = 1;
  int64_t size = BASIC_BUF_SIZE;
  while (size < input) {
    size = size << 1;
    index++;
  }
  return index - 1;
}

void DynamicBuffer::wash_()
{
  free_();
  purge_statistic_();
}

bool DynamicBuffer::need_wash_() const
{
  bool bret = false;
  const int64_t cur_timestamp = common::ObTimeUtility::fast_current_time();
  if (0 != ATOMIC_LOAD(&ref_) || NULL == buf_ || buf_size_ < BASIC_BUF_SIZE) {
    // ref not zero or buf is NULL, no need wash
  } else if (cur_timestamp - buf_gen_timestamp_ < WASH_INTERVAL) {
    // not reach wash interval, no need wash
  } else if (get_target_buf_size_() < buf_size_) {
    bret = true;
  }
  return bret;
}

int64_t DynamicBuffer::get_target_buf_size_() const
{
  int64_t size = BASIC_BUF_SIZE;
  if (total_usage_ < 1) {
    // do nothing
  } else {
    int64_t sum = 0;
    for (int64_t i = 0; i < buf_size_usage_.count(); i++) {
      sum += buf_size_usage_.at(i);
      if (sum * 100 / total_usage_ > WASH_THREASHOLD) {
        size = BASIC_BUF_SIZE * (1 << i);
        break;
      }
    }
  }
  return size;
}

} // namespace archive
} // namespace oceanbase
