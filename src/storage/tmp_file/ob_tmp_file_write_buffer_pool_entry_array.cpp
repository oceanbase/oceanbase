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
#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_write_buffer_pool_entry_array.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace tmp_file
{

int ObPageEntry::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = State::N;
  static const int64_t INV = State::INVALID;
  static const int64_t INITED = State::INITED;
  static const int64_t LOADING = State::LOADING;
  static const int64_t CACHED = State::CACHED;
  static const int64_t DIRTY = State::DIRTY;
  static const int64_t WRITE_BACK = State::WRITE_BACK;
  static const int64_t MAX = State::MAX;

  static const int64_t STATE_MAP[State::MAX][Ops::MAX] = {
  // ALLOC,     LOAD,      LOAD_FAIL,   LOAD_SUCC,  DELETE,   WRITE,    WRITE_BACK,   WRITE_BACK_FAILED,   WRITE_BACK_SUCC
    {INITED,    N,         N,           N,          INV,      N,        N,            N,                   N},          //INVALID
    {N,         LOADING,   N,           N,          INV,      DIRTY,    N,            N,                   N},          //INITED
    {N,         N,         INITED,      CACHED,     N,        N,        N,            N,                   N},          //LOADING
    {N,         N,         N,           N,          INV,      DIRTY,    N,            N,                   CACHED},     //CACHED
    {N,         N,         N,           N,          INV,      DIRTY,    WRITE_BACK,   N,                   N},          //DIRTY
    {N,         N,         N,           N,          INV,      DIRTY,    WRITE_BACK,   DIRTY,               CACHED}      //WRITE_BACK
  };

  if (OB_UNLIKELY(!Ops::is_valid(op))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid operation", KR(ret), K(op));
  } else if (OB_UNLIKELY(!State::is_valid(state_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid state", KR(ret), K(state_));
  } else {
    const int64_t n_stat = STATE_MAP[state_][op];
    if (OB_UNLIKELY(!State::is_valid(n_stat))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("invalid state transition", KR(ret), K(state_), K(op), K(n_stat));
    } else {
      state_ = n_stat;
    }
  }
  return ret;
}

int ObTmpWriteBufferPoolEntryArray::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tmp file entry array init twice", KR(ret));
  } else {
    buckets_.set_attr(ObMemAttr(MTL_ID(), "TmpFileEntBktAr"));
    size_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpWriteBufferPoolEntryArray::destroy()
{
  if (IS_INIT) {
    buckets_.reset();
    size_ = 0;
    is_inited_ = false;
  }
}

int ObTmpWriteBufferPoolEntryArray::add_new_bucket_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buckets_.push_back(ObArray<ObPageEntry>()))) {
    LOG_WARN("fail to push back new bucket", KR(ret), K(buckets_.size()), K(size_));
  } else {
    int64_t bucket_idx = buckets_.size() - 1;
    buckets_[bucket_idx].set_attr(ObMemAttr(MTL_ID(), "TmpFileEntBkt"));
    buckets_[bucket_idx].set_block_allocator(allocator_);

    if (OB_FAIL(buckets_[bucket_idx].reserve(MAX_BUCKET_CAPACITY))) {
      LOG_WARN("fail to do reserve for new bucket", KR(ret), K(buckets_.size()), K(size_));
    }
  }
  return ret;
}

int ObTmpWriteBufferPoolEntryArray::push_back(const ObPageEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t bucket_idx = size_ / MAX_BUCKET_CAPACITY;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file entry array not init", KR(ret));
  } else {
    if (bucket_idx >= buckets_.size()) {
      if (OB_FAIL(add_new_bucket_())) {
        LOG_WARN("fail to add new bucket", KR(ret), K(size_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(bucket_idx >= buckets_.size() || bucket_idx < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid bucket index", KR(ret), K(bucket_idx), K(buckets_.size()), K(size_));
    } else if (OB_FAIL(buckets_[bucket_idx].push_back(entry))) {
      LOG_WARN("fail to push back entry", KR(ret), K(bucket_idx), K(entry), K(size_));
    }
  }

  if (OB_SUCC(ret)) {
    size_ += 1;
  }
  return ret;
}

void ObTmpWriteBufferPoolEntryArray::pop_back()
{
  if (size_ > 0) {
    const int64_t bucket_idx = buckets_.size() - 1;
    buckets_[bucket_idx].pop_back();
    size_ -= 1;

    if (buckets_[bucket_idx].size() == 0) {
      buckets_[bucket_idx].reset();
      buckets_.pop_back();
      LOG_DEBUG("pop back a bucket", K(buckets_.size()), K(size_));
    }
  }
}

}  // end namespace tmp_file
}  // end namespace oceanbase
