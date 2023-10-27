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

#include "large_buffer_pool.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include <cstdint>

namespace oceanbase
{
namespace archive
{
LargeBufferPool::LargeBufferPool() :
  inited_(false),
  total_limit_(0),
  label_(),
  array_(),
  rwlock_()
{
  array_.set_attr(SET_USE_500("LargeBufferPool"));
}

LargeBufferPool::~LargeBufferPool()
{
  destroy();
}

int LargeBufferPool::init(const char *label, const int64_t total_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "LargeBufferPool init twice", K(ret), K(inited_));
  } else if (OB_UNLIKELY(total_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(total_limit));
  } else if (OB_FAIL(label_.assign(label))) {
    ARCHIVE_LOG(WARN, "assign failed", K(ret));
  } else {
    total_limit_ = total_limit;
    inited_ = true;
    ARCHIVE_LOG(INFO, "LargeBufferPool init succ", KPC(this));
  }
  return ret;
}

void LargeBufferPool::destroy()
{
  WLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  while (! array_.empty()) {
    if (OB_FAIL(array_.at(array_.count() - 1).purge())) {
      ARCHIVE_LOG(ERROR, "buffer node purge failed, memory may leak", K(ret));
    }
    array_.pop_back();
  }
  inited_ = false;
  total_limit_ = 0;
  label_.reset();
}

char *LargeBufferPool::acquire(const int64_t size)
{
  char *data = NULL;
  int ret = OB_SUCCESS;

  common::ObTimeGuard time_guard("acquire", 10 * 1000L);
  WLockGuard guard(rwlock_);
  {
    if (OB_UNLIKELY(! inited_)) {
      ret = OB_NOT_INIT;
      ARCHIVE_LOG(WARN, "large buffer pool not init", K(ret));
    } else if (OB_FAIL(acquire_(size, data))) {
      ARCHIVE_LOG(WARN, "acquire failed", K(ret));
    }
  }

  if (NULL == data && OB_SUCC(ret)) {
    if (OB_FAIL(reserve_())) {
      ARCHIVE_LOG(WARN, "reserve failed", K(ret));
    } else if (OB_FAIL(acquire_(size, data))) {
      ARCHIVE_LOG(WARN, "acquire failed", K(ret));
    }
  }
  return data;
}

void LargeBufferPool::reclaim(void *ptr)
{
  common::ObTimeGuard time_guard("reclaim", 10 * 1000L);
  WLockGuard guard(rwlock_);
  if (NULL != ptr) {
    reclaim_(ptr);
  }
}

void LargeBufferPool::weed_out()
{
  int ret = OB_SUCCESS;
  bool is_purged = false;
  const int64_t purge_threshold =
    common::ObTimeUtility::fast_current_time() - BUFFER_PURGE_THRESHOLD;
  common::ObTimeGuard time_guard("weed_out", 10 * 1000L);
  WLockGuard guard(rwlock_);
  for (int64_t i = array_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    BufferNode &node = array_.at(i);
    is_purged = false;
    if (OB_FAIL(node.check_and_purge(purge_threshold, is_purged))) {
      ARCHIVE_LOG(WARN, "node check and purge failed", K(ret), K(node));
    } else if (! is_purged) {
      break;
    }
  }
}

int LargeBufferPool::acquire_(const int64_t size, char *&data)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; NULL == data && i < array_.count(); i++) {
    bool issued = false;
    if (NULL == (data = array_.at(i).acquire(size, issued))) {
      if (! issued) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      }
    }
  }
  return ret;
}

void LargeBufferPool::reclaim_(void *ptr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < array_.count(); i++) {
    if (array_.at(i).reclaim(ptr)) {
      break;
    }
  }
}

int LargeBufferPool::reserve_()
{
  int ret = OB_SUCCESS;
  BufferNode node(label_.ptr());
  if (OB_FAIL(array_.push_back(node))) {
    ARCHIVE_LOG(WARN, "push back failed", K(ret));
  }
  return ret;
}

LargeBufferPool::BufferNode::BufferNode(const char *label) :
  issued_(false),
  last_used_timestamp_(OB_INVALID_TIMESTAMP),
  buffer_(label),
  rwlock_()
{}

LargeBufferPool::BufferNode::BufferNode(const BufferNode &other)
{
  assign(other);
}

// BufferNode is the element of SEArray, and its contents will be transfered by assign function in SEArray reserve,
// so the buffer_ can not be freed in deconstruct function
LargeBufferPool::BufferNode::~BufferNode()
{
  // do nothing, buffer_ will be freed only with purge function
}

// maybe optimize lock usage
char *LargeBufferPool::BufferNode::acquire(const int64_t size, bool &issued)
{
  WLockGuard guard(rwlock_);
  char *data = NULL;
  issued = issued_;
  if (! issued) {
    if (NULL != (data = buffer_.acquire(size))) {
      issued_ = true;
      last_used_timestamp_ = common::ObTimeUtility::fast_current_time();
    }
  }
  return data;
}

bool LargeBufferPool::BufferNode::reclaim(void *ptr)
{
  WLockGuard guard(rwlock_);
  bool bret = false;
  if (issued_ && buffer_.alloc_from(ptr)) {
    buffer_.reclaim(ptr);
    issued_ = false;
    bret = true;
  }
  return bret;
}

int LargeBufferPool::BufferNode::purge()
{
  WLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  if (issued_) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "buffer issued, memory may leak", K(issued_));
  } else {
    buffer_.purge();
  }
  return ret;
}

int LargeBufferPool::BufferNode::assign(const BufferNode &other)
{
  issued_ = other.issued_;
  last_used_timestamp_ = other.last_used_timestamp_;
  return buffer_.assign(other.buffer_);
}

int LargeBufferPool::BufferNode::check_and_purge(const int64_t purge_threshold, bool &is_purged)
{
  is_purged = false;
  WLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  if (issued_) {
  } else if (purge_threshold <= last_used_timestamp_) {
    // do nothing
  } else {
    buffer_.purge();
    is_purged = true;
  }
  return ret;
}

} // namespace archive
} // namespace oceanbase
