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

#include "storage/tmp_file/ob_tmp_file_write_cache_page_array.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace tmp_file
{

int ObTmpFilePageArray::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tmp file page array init twice", KR(ret));
  } else {
    allocator_.set_attr(ObMemAttr(MTL_ID(), "TmpFileEntBkt"));
    buckets_.set_attr(ObMemAttr(MTL_ID(), "TmpFileEntBktAr"));
    size_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpFilePageArray::destroy()
{
  if (IS_INIT) {
    buckets_.reset();
    size_ = 0;
    is_inited_ = false;
  }
}

int ObTmpFilePageArray::add_new_bucket_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObArray<ObTmpFilePage *> *bucket = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArray<ObTmpFilePage>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for new bucket", KR(ret), K(buckets_.size()), K(size_));
  } else if (FALSE_IT(bucket = new (buf) ObArray<ObTmpFilePage *>())) {
  } else if (OB_FAIL(buckets_.push_back(bucket))) {
    LOG_WARN("fail to push back new bucket", KR(ret), K(buckets_.size()), K(size_));
  } else {
    int64_t bucket_idx = buckets_.size() - 1;
    buckets_[bucket_idx]->set_attr(ObMemAttr(MTL_ID(), "TmpFileEntBkt"));
    buckets_[bucket_idx]->set_block_allocator(allocator_);

    if (OB_FAIL(buckets_[bucket_idx]->reserve(MAX_BUCKET_CAPACITY))) {
      LOG_WARN("fail to do reserve for new bucket", KR(ret), K(buckets_.size()), K(size_));
    }
  }
  return ret;
}

int ObTmpFilePageArray::push_back(ObTmpFilePage *page)
{
  int ret = OB_SUCCESS;
  int64_t bucket_idx = size_ / MAX_BUCKET_CAPACITY;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file page array not init", KR(ret));
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
    } else if (OB_FAIL(buckets_[bucket_idx]->push_back(page))) {
      LOG_WARN("fail to push back page", KR(ret), K(bucket_idx), K(size_), KPC(page));
    }
  }

  if (OB_SUCC(ret)) {
    size_ += 1;
  }
  return ret;
}

void ObTmpFilePageArray::pop_back()
{
  if (size_ > 0) {
    const int64_t bucket_idx = buckets_.size() - 1;
    buckets_[bucket_idx]->pop_back();
    size_ -= 1;

    if (buckets_[bucket_idx]->size() == 0) {
      buckets_[bucket_idx]->reset();
      allocator_.free(buckets_[bucket_idx]);
      buckets_[bucket_idx] = nullptr;
      buckets_.pop_back();
      LOG_DEBUG("pop back a bucket", K(buckets_.size()), K(size_));
    }
  }
}

}  // end namespace tmp_file
}  // end namespace oceanbase
