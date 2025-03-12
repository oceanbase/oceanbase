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

#include "storage/tmp_file/ob_tmp_file_write_buffer_index_cache.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace tmp_file
{
ObTmpFileWBPIndexCache::ObTmpFileWBPIndexCache() :
  ObTmpFileCircleArray(), bucket_array_allocator_(nullptr), bucket_allocator_(nullptr),
  page_buckets_(nullptr) ,
  fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD), wbp_(nullptr),
  sparsify_count_(0),
  ignored_push_count_(0),
  max_bucket_array_capacity_(MAX_BUCKET_ARRAY_CAPACITY) {}

ObTmpFileWBPIndexCache::~ObTmpFileWBPIndexCache()
{
  destroy();
}

int ObTmpFileWBPIndexCache::init(const int64_t fd, ObTmpWriteBufferPool* wbp,
                                 ObIAllocator *wbp_index_cache_allocator,
                                 ObIAllocator *wbp_index_cache_bkt_allocator)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  uint64_t tenant_id = MTL_ID();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmp file fd", KR(ret), K(fd));
  } else if (OB_ISNULL(wbp) || OB_ISNULL(wbp_index_cache_allocator) ||
             OB_ISNULL(wbp_index_cache_bkt_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), KP(wbp), KP(wbp_index_cache_allocator),
                                 KP(wbp_index_cache_bkt_allocator));
  } else if (OB_ISNULL(buf = wbp_index_cache_allocator->alloc(sizeof(ObArray<ObTmpFilePageIndexBucket*>),
                                                              lib::ObMemAttr(tenant_id, "TmpFileIdxCache")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for temporary file page index bucket",
              KR(ret), K(fd), K(tenant_id), K(sizeof(ObArray<ObTmpFilePageIndexBucket*>)));
  } else if (FALSE_IT(page_buckets_ = new (buf) ObArray<ObTmpFilePageIndexBucket*>())) {
  } else if (FALSE_IT(page_buckets_->set_attr(lib::ObMemAttr(tenant_id, "TmpFileIdxCache")))) {
  } else if (OB_FAIL(page_buckets_->prepare_allocate(INIT_BUCKET_ARRAY_CAPACITY, nullptr))) {
    page_buckets_->destroy();
    wbp_index_cache_allocator->free(buf);
    page_buckets_ = nullptr;
    LOG_WARN("fail to prepare allocate array", KR(ret), K(fd));
  } else {
    is_inited_ = true;
    fd_ = fd;
    wbp_ = wbp;
    bucket_allocator_ = wbp_index_cache_bkt_allocator;
    bucket_array_allocator_ = wbp_index_cache_allocator;
    left_ = 0;
    right_ = -1;
    size_ = 0;
    capacity_ = INIT_BUCKET_ARRAY_CAPACITY;
  }
  return ret;
}

void ObTmpFileWBPIndexCache::reset()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    left_ = 0;
    right_ = -1;
    size_ = 0;
    capacity_ = 0;
    sparsify_count_ = 0;
    ignored_push_count_ = 0;
    if (OB_ISNULL(page_buckets_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page buckets is null", KR(ret), KPC(this));
    } else if (OB_ISNULL(bucket_allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket allocator is null", KR(ret), KPC(this));
    } else {
      for (int64_t i = 0; i < page_buckets_->count(); ++i) {
        if (OB_NOT_NULL(page_buckets_->at(i))) {
          page_buckets_->at(i)->destroy();
          bucket_allocator_->free(page_buckets_->at(i));
          page_buckets_->at(i) = nullptr;
        }
      }
      page_buckets_->reset();
    }
  }
}

void ObTmpFileWBPIndexCache::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    reset();
    is_inited_ = false;
    fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    wbp_ = nullptr;
    if (OB_ISNULL(page_buckets_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page buckets is null", KR(ret), KPC(this));
    } else if (OB_ISNULL(bucket_array_allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket array allocator is null", KR(ret), KPC(this));
    } else {
      page_buckets_->destroy();
      bucket_array_allocator_->free(page_buckets_);
      page_buckets_ = nullptr;
      bucket_allocator_ = nullptr;
      bucket_array_allocator_ = nullptr;
    }
  }
}

int ObTmpFileWBPIndexCache::push(const uint32_t page_index)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (0 == capacity_ || is_full()) {
    if (max_bucket_array_capacity_ <= capacity_) {
      if (OB_FAIL(sparsify_())) {
        LOG_WARN("fail to sparsify", KR(ret), KPC(this));
      }
    } else if (OB_FAIL(expand_())) {
      LOG_WARN("fail to expand array", KR(ret), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    if (ignored_push_count_ < ((1 << sparsify_count_) - 1)) {
      ignored_push_count_ += 1;
    } else if (FALSE_IT(ignored_push_count_ = 0)) {
    } else if (is_empty() || (OB_NOT_NULL(page_buckets_->at(right_)) && page_buckets_->at(right_)->is_full())) {
      // alloc a new bucket
      inc_pos_(right_);
      uint64_t tenant_id = MTL_ID();
      void *buf = nullptr;
      ObTmpFilePageIndexBucket* bucket = nullptr;
      if (OB_ISNULL(buf = bucket_allocator_->alloc(sizeof(ObTmpFilePageIndexBucket),
                                                   lib::ObMemAttr(tenant_id, "TmpFileIdxBkt")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for temporary file page index bucket",
                 KR(ret), K(tenant_id), K(sizeof(ObTmpFilePageIndexBucket)), KPC(this));
      } else if (FALSE_IT(bucket = new (buf) ObTmpFilePageIndexBucket())) {
      } else if (OB_FAIL(bucket->init(fd_, wbp_))) {
        LOG_WARN("fail to init temporary file page index bucket", KR(ret), KPC(this));
      } else if (OB_FAIL(bucket->push(page_index))) {
        LOG_WARN("fail to push page_index", KR(ret), K(page_index), KPC(this));
      } else {
        page_buckets_->at(right_) = bucket;
      }

      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(bucket)) {
          bucket->destroy();
          bucket_allocator_->free(bucket);
          page_buckets_->at(right_) = nullptr;
        }
        dec_pos_(right_);
      } else {
        size_ += 1;
      }
    } else if (OB_ISNULL(page_buckets_->at(right_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KR(ret), K(page_buckets_->count()), KPC(this));
    } else if (OB_FAIL(page_buckets_->at(right_)->push(page_index))) { // bucket is not full
      LOG_WARN("fail to push page index", KR(ret), K(page_index), KPC(page_buckets_->at(right_)), KPC(this));
    }
  }

  return ret;
}

int ObTmpFileWBPIndexCache::truncate(const int64_t truncate_page_virtual_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_empty()) {
    const int64_t logic_begin_pos = left_;
    const int64_t logic_end_pos = get_logic_tail_();
    bool truncate_over = false;
    for (int64_t i = logic_begin_pos; OB_SUCC(ret) && !truncate_over && i <= logic_end_pos; i++) {
      int64_t bkt_min_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      int64_t bkt_min_page_index = ObTmpFileGlobal::INVALID_PAGE_ID;
      ObTmpFilePageIndexBucket *&cur_bucket = page_buckets_->at(i % capacity_);
      if (OB_ISNULL(cur_bucket)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), K(i), K(capacity_), K(page_buckets_->count()), KPC(this));
      } else if (OB_UNLIKELY((bkt_min_page_index = cur_bucket->get_min_page_index()) ==
                             ObTmpFileGlobal::INVALID_PAGE_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page index", KR(ret), KPC(cur_bucket), KPC(this));
      } else if (OB_FAIL(wbp_->get_page_virtual_id(fd_, bkt_min_page_index, bkt_min_page_virtual_id))) {
        LOG_WARN("fail to get page virtual id in file", KR(ret), K(bkt_min_page_index), KPC(this));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == bkt_min_page_virtual_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page virtual id", KR(ret), K(bkt_min_page_virtual_id), KPC(this));
      } else if (i != logic_begin_pos) {
        // truncate previous bucket
        if (truncate_page_virtual_id < bkt_min_page_virtual_id) {
          truncate_over = true;
          ObTmpFilePageIndexBucket *&previous_bucket = page_buckets_->at(get_previous_pos(i % capacity_));
          if (OB_ISNULL(previous_bucket)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", KR(ret), K(fd_), K(i), K(capacity_), KP(previous_bucket),
                     K(truncate_page_virtual_id), K(bkt_min_page_virtual_id),
                     K(logic_begin_pos), K(logic_end_pos), KPC(this));
          } else if (OB_FAIL(previous_bucket->truncate(truncate_page_virtual_id))) {
            LOG_WARN("fail to truncate bucket", KR(ret), K(fd_), K(truncate_page_virtual_id),
                     K(bkt_min_page_virtual_id), KPC(previous_bucket), KPC(this));
          } else if (OB_UNLIKELY(previous_bucket->is_empty())) {
            // when truncate_page_virtual_id is smaller than min_page_virtual_id of cur bucket,
            // there must exist at least one page in previous bucket whose virtual page id is larger than
            // or equal to truncate_page_virtual_id (which means this page index doesn't need to be truncated).
            // however, truncate all page indexes of previous bucket will not cause some problems.
            // thus, we just allow code continue to run
            previous_bucket->destroy();
            bucket_allocator_->free(previous_bucket);
            previous_bucket = nullptr;
            inc_pos_(left_);
            size_ -= 1;
          }
        } else { // truncate_page_virtual_id >= bkt_min_page_virtual_id
          ObTmpFilePageIndexBucket *&previous_bucket = page_buckets_->at(get_previous_pos(i % capacity_));
          if (OB_ISNULL(previous_bucket)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", KR(ret), K(fd_), K(i), K(capacity_),KP(previous_bucket),
                      K(truncate_page_virtual_id), K(bkt_min_page_virtual_id),
                      K(logic_begin_pos), K(logic_end_pos), KPC(this));
          } else {
            previous_bucket->destroy();
            bucket_allocator_->free(previous_bucket);
            previous_bucket = nullptr;
            inc_pos_(left_);
            size_ -= 1;
          }
        }
      }
      if (i == logic_end_pos && truncate_page_virtual_id > bkt_min_page_virtual_id) {
        if (FAILEDx(cur_bucket->truncate(truncate_page_virtual_id))) {
          LOG_WARN("fail to truncate bucket", KR(ret), K(fd_), K(truncate_page_virtual_id),
                   K(logic_begin_pos), K(logic_end_pos), K(bkt_min_page_virtual_id), KPC(cur_bucket), KPC(this));
        } else if (cur_bucket->is_empty()) {
          cur_bucket->destroy();
          bucket_allocator_->free(cur_bucket);
          cur_bucket = nullptr;
          inc_pos_(left_);
          size_ -= 1;
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (is_empty()) {
      reset();
    } else {
      shrink_();
    }
  }

  return ret;
}

// 1. if 'target_page_virtual_id' is smaller than virtual page id of first page in cache,
//    page_index will be INVALID_PAGE_ID.
//    in this case, caller should directly search write buffer pool with the beginning page index of file
// 2. if 'target_page_virtual_id' is in the range of cached pages but the page index of it doesn't exist in cache,
//    the cache will find a page whose virtual id is smaller than and closest to 'target_page_virtual_id'
//    and then iterate the page index of wbp from the closest page index to find the according page_index
int ObTmpFileWBPIndexCache::binary_search(const int64_t target_page_virtual_id, uint32_t &page_index)
{
  int ret = OB_SUCCESS;
  page_index = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_LIKELY(!is_empty())) {
    int64_t left_pos = left_;
    int64_t right_pos = get_logic_tail_();
    ObTmpFilePageIndexBucket *target_bucket = nullptr;
    bool find = false;
    while (left_pos <= right_pos && !find && OB_SUCC(ret)) {
      // find the bucket whose min_page_virtual_id is small than and closest to target_page_virtual_id
      const int64_t logic_mid = left_pos + (right_pos - left_pos) / 2;
      const int64_t mid = logic_mid % capacity_;
      ObTmpFilePageIndexBucket *mid_bucket = page_buckets_->at(mid);
      int64_t min_page_virtual_id = -1;
      if (OB_ISNULL(mid_bucket)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), K(fd_), K(left_pos), K(right_pos), K(logic_mid), K(mid),
                 KP(mid_bucket), K(target_page_virtual_id), KPC(this));
      } else if (OB_UNLIKELY(mid_bucket->get_min_page_index() == ObTmpFileGlobal::INVALID_PAGE_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page index", KR(ret), K(left_pos), K(right_pos),
                 K(logic_mid), K(mid), KPC(mid_bucket), KPC(this));
      } else if (OB_FAIL(wbp_->get_page_virtual_id(fd_, mid_bucket->get_min_page_index(), min_page_virtual_id))) {
        LOG_WARN("fail to get page virtual id in file", KR(ret), K(fd_), K(left_pos), K(right_pos),
                 K(logic_mid), K(mid), KPC(mid_bucket), KPC(this));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == min_page_virtual_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page virtual id", KR(ret), K(fd_), K(min_page_virtual_id), KPC(mid_bucket), KPC(this));
      } else if (min_page_virtual_id <= target_page_virtual_id) {
        target_bucket = mid_bucket;
        if (min_page_virtual_id == target_page_virtual_id) {
          find = true;
          page_index = mid_bucket->get_min_page_index();
        } else {
          left_pos = logic_mid + 1;
        }
      } else {
        right_pos = logic_mid - 1;
      }
    } // end while

    if (OB_FAIL(ret)) {
    } else if (find) {
      // do nothing
    } else if (OB_ISNULL(target_bucket)) {
      // page_index = ObTmpFileGlobal::INVALID_PAGE_ID;
      LOG_DEBUG("the target page_index might be removed from cache", K(fd_), K(target_page_virtual_id), KPC(this));
    } else if (OB_FAIL(target_bucket->binary_search(target_page_virtual_id, page_index))) {
      LOG_WARN("fail to binary search page index", KR(ret), K(fd_), K(target_page_virtual_id),
               KPC(target_bucket), KPC(this));
    }
  }
  return ret;
}

int ObTmpFileWBPIndexCache::expand_()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("start to expand tmp file wbp index cache", KPC(this));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(max_bucket_array_capacity_ == capacity_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("cannot expand array any more", KR(ret), K(fd_), K(capacity_), K(max_bucket_array_capacity_));
  } else if (OB_UNLIKELY(capacity_ != 0 && !is_full())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expand array when it is not full", KR(ret), K(fd_), K(size_), K(capacity_));
  } else {
    int64_t new_capacity = 0 == capacity_ ?
                           INIT_BUCKET_ARRAY_CAPACITY:
                           MIN(capacity_ * 2, max_bucket_array_capacity_);
    if (OB_FAIL(page_buckets_->prepare_allocate(new_capacity, nullptr))) {
      LOG_WARN("fail to prepare allocate array", KR(ret), K(fd_), K(new_capacity));
    } else if (!is_empty() && right_ < left_) {
      if (OB_UNLIKELY(right_ + capacity_ >= new_capacity)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid capacity", KR(ret), K(fd_), K(right_), K(capacity_), K(new_capacity));
      } else {
        for (int64_t i = 0; i <= right_; ++i) {
          ObTmpFilePageIndexBucket*& cur_bucket = page_buckets_->at(i);
          ObTmpFilePageIndexBucket*& new_bucket = page_buckets_->at(capacity_ + i);
          new_bucket = cur_bucket;
          page_buckets_->at(i) = nullptr;
        }
        right_ += capacity_;
      }
    }

    if (OB_SUCC(ret)) {
      capacity_ = new_capacity;
    }
  }
  LOG_DEBUG("expand tmp file wbp index cache over", KR(ret), KPC(this));
  return ret;
}

void ObTmpFileWBPIndexCache::shrink_()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("start to shrink tmp file wbp index cache", KPC(this));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(get_logic_tail_() - left_ + 1 != size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache pos is wrong", KR(ret), K(fd_), K(left_), K(right_), K(size_), K(capacity_));
  } else if (size_ > capacity_ / SHRINK_THRESHOLD ||
             INIT_BUCKET_ARRAY_CAPACITY == capacity_) {
    // no need to shrink
  } else {
    int64_t new_capacity = capacity_ / 2;
    void *buf = nullptr;
    uint64_t tenant_id = MTL_ID();
    ObArray<ObTmpFilePageIndexBucket*> *new_buckets = nullptr;

    if (OB_UNLIKELY(size_ > new_capacity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid new_capacity", KR(ret), K(new_capacity), KPC(this));
    } else if (OB_ISNULL(buf = bucket_array_allocator_->alloc(sizeof(ObArray<ObTmpFilePageIndexBucket*>),
                                                       lib::ObMemAttr(tenant_id, "TmpFileIdxCache")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for temporary file page index bucket",
                KR(ret), K(fd_), K(tenant_id), K(sizeof(ObArray<ObTmpFilePageIndexBucket*>)));
    } else if (FALSE_IT(new_buckets = new (buf) ObArray<ObTmpFilePageIndexBucket*>())) {
    } else if (FALSE_IT(new_buckets->set_attr(lib::ObMemAttr(tenant_id, "TmpFileIdxCache")))) {
    } else if (OB_FAIL(new_buckets->prepare_allocate(new_capacity, nullptr))) {
      new_buckets->destroy();
      bucket_array_allocator_->free(buf);
      new_buckets = nullptr;
      LOG_WARN("fail to prepare allocate array", KR(ret), K(fd_));
    } else {
      for (int64_t i = left_; i <= get_logic_tail_() && OB_SUCC(ret); i++) {
        ObTmpFilePageIndexBucket *bucket = page_buckets_->at(i % capacity_);
        new_buckets->at(i - left_) = bucket;
      } // end for
      page_buckets_->destroy();
      bucket_array_allocator_->free(page_buckets_);
      left_ = 0;
      right_ = size_ - 1;
      capacity_ = new_capacity;
      page_buckets_ = new_buckets;
      LOG_DEBUG("successfully shrink tmp file page index cache", K(fd_), KPC(this));
    }
  }
  LOG_DEBUG("shrink tmp file wbp index cache over", KR(ret), KPC(this));
}

int ObTmpFileWBPIndexCache::sparsify_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max_bucket_array_capacity_ != capacity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity", KR(ret), K(fd_), K(capacity_));
  } else if (OB_UNLIKELY(capacity_ % 2 != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("capacity should be even", KR(ret), K(fd_), K(capacity_));
  } else if (OB_UNLIKELY(!is_full())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache is not full when try to eliminate half pages", KR(ret), K(fd_), K(size_), K(capacity_));
  } else {
    int64_t cur_bucket_pos = left_;
    for (int64_t i = left_; i <= get_logic_tail_() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(page_buckets_->at(i % capacity_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), K(fd_), K(cur_bucket_pos), K(i), KPC(this));
      } else if (OB_FAIL(page_buckets_->at(i % capacity_)->shrink_half())) {
        LOG_WARN("fail to shrink half", KR(ret), K(fd_), K(cur_bucket_pos), K(i),
                 KPC(page_buckets_->at(i % capacity_)), KPC(this));
      } else if ((i - left_) % 2 == 0) {
        page_buckets_->at(cur_bucket_pos % capacity_) = page_buckets_->at(i % capacity_);
      } else {
        if (OB_FAIL(page_buckets_->at(cur_bucket_pos % capacity_)->merge(*page_buckets_->at(i % capacity_)))) {
          LOG_WARN("fail to merge two buckets", KR(ret), K(fd_), K(cur_bucket_pos), K(i),
                   KPC(page_buckets_->at(cur_bucket_pos % capacity_)), KPC(page_buckets_->at(i % capacity_)),
                   KPC(this));
        } else {
          page_buckets_->at(i % capacity_)->destroy();
          bucket_allocator_->free(page_buckets_->at(i % capacity_));
          cur_bucket_pos++;
        }
      }

      if (OB_SUCC(ret) && i > (get_logic_tail_() - left_) / 2 + left_) {
        page_buckets_->at(i % capacity_) = nullptr;
      }
    }

    if (OB_SUCC(ret)) {
      right_ = (cur_bucket_pos - 1) % capacity_;
      size_ /= 2;
      sparsify_count_ += 1;
      ignored_push_count_ = 0;
    }
  }
  LOG_INFO("sparsify tmp file wbp index cache over", KR(ret), KPC(this));
  return ret;
}

ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::ObTmpFilePageIndexBucket() :
  page_indexes_(), fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
  wbp_(nullptr), min_page_index_(ObTmpFileGlobal::INVALID_PAGE_ID) {}

ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::~ObTmpFilePageIndexBucket()
{
  destroy();
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::init(int64_t fd, ObTmpWriteBufferPool* wbp)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(fd == ObTmpFileGlobal::INVALID_TMP_FILE_FD) || OB_ISNULL(wbp)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), KP(wbp));
  } else if (FALSE_IT(page_indexes_.set_attr(ObMemAttr(MTL_ID(), "TmpFileIdxBkt")))) {
  } else if (OB_FAIL(page_indexes_.prepare_allocate(BUCKET_CAPACITY, ObTmpFileGlobal::INVALID_PAGE_ID))) {
    LOG_WARN("fail to prepare allocate array", KR(ret));
  } else {
    is_inited_ = true;
    fd_ = fd;
    wbp_ = wbp;
    min_page_index_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    left_ = 0;
    right_ = -1;
    size_ = 0;
    capacity_ = BUCKET_CAPACITY;
  }
  return ret;
}

void ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::destroy()
{
  is_inited_ = false;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  wbp_ = nullptr;
  left_ = 0;
  right_ = -1;
  size_ = 0;
  capacity_ = 0;
  min_page_index_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  page_indexes_.reset();
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::push(const uint32_t page_index)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_full())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("bucket is full", KR(ret), KPC(this));
  } else {
    if (OB_UNLIKELY(is_empty())) {
      min_page_index_ = page_index;
    }
    inc_pos_(right_);
    page_indexes_[right_] = page_index;
    size_ += 1;
  }

  return ret;
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::pop_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pop a empty array", KR(ret), KPC(this));
  } else {
    page_indexes_[left_] = ObTmpFileGlobal::INVALID_PAGE_ID;
    inc_pos_(left_);
    size_ -= 1;

    if (is_empty()) {
      destroy();
    } else {
      min_page_index_ = page_indexes_[left_];
    }
  }
  return ret;
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::truncate(const int64_t truncate_page_virtual_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt binary search in an empty array", KR(ret), KPC(this));
  } else {
    const int64_t logic_begin_pos = left_;
    const int64_t logic_end_pos = get_logic_tail_();
    bool truncate_over = false;
    for (int64_t i = logic_begin_pos; OB_SUCC(ret) && !truncate_over && i <= logic_end_pos; i++) {
      int64_t page_virtual_id = -1;
      if (OB_UNLIKELY(page_indexes_[i % capacity_] == ObTmpFileGlobal::INVALID_PAGE_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page index", KR(ret), K(page_indexes_[i % capacity_]), KPC(this));
      } else if (OB_FAIL(wbp_->get_page_virtual_id(fd_, page_indexes_[i % capacity_], page_virtual_id))) {
        LOG_WARN("fail to get page virtual id in file", KR(ret), K(page_indexes_[i % capacity_]), KPC(this));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == page_virtual_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page virtual id", KR(ret), K(page_virtual_id), KPC(this));
      } else if (page_virtual_id < truncate_page_virtual_id) {
        if (OB_FAIL(pop_())) {
          LOG_WARN("fail to pop", KR(ret), KPC(this));
        }
      } else {
        truncate_over = true;
      }
    } // end for
  }
  return ret;
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::binary_search(
  const int64_t target_page_virtual_id, uint32_t &page_index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt binary search in an empty array", KR(ret), K(size_));
  } else {
    bool find = false;
    int64_t left_pos = left_;
    int64_t right_pos = get_logic_tail_();
    uint32_t target_page_index = ObTmpFileGlobal::INVALID_PAGE_ID;
    int64_t cur_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    while (left_pos <= right_pos && !find && OB_SUCC(ret)) {
      const int64_t logic_mid = left_pos + (right_pos - left_pos) / 2;
      const int64_t mid = logic_mid % capacity_;
      const uint32_t mid_page = page_indexes_[mid];
      if (OB_FAIL(wbp_->get_page_virtual_id(fd_, mid_page, cur_page_virtual_id))) {
        LOG_WARN("fail to get page virtual id in file", KR(ret), K(logic_mid), K(mid), K(mid_page), KPC(this));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == cur_page_virtual_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid page virtual id", KR(ret), K(cur_page_virtual_id), KPC(this));
      } else if (cur_page_virtual_id <= target_page_virtual_id) {
        target_page_index = mid_page;
        if (cur_page_virtual_id == target_page_virtual_id) {
          find = true;
        } else {
          left_pos = logic_mid + 1;
        }
      } else {
        right_pos = logic_mid - 1;
      }
    } // end while

    if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == target_page_index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot find the target page index", KR(ret), K(page_indexes_[left_]),
                                                    K(target_page_index),
                                                    K(target_page_virtual_id), KPC(this));
    } else if (find) {
      page_index = target_page_index;
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == target_page_index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target page index doesn't exist in bucket", KR(ret), K(left_), K(page_indexes_[left_]),
                                                            K(target_page_index),
                                                            K(target_page_virtual_id), KPC(this));
    } else {
      int64_t cur_page_index = target_page_index;
      while (OB_SUCC(ret) && !find) {
        uint32_t next_page_index = ObTmpFileGlobal::INVALID_PAGE_ID;
        int64_t page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        if (OB_FAIL(wbp_->get_page_virtual_id(fd_, cur_page_index, cur_page_virtual_id))) {
          LOG_WARN("fail to get virtual page id", KR(ret), K(fd_), K(cur_page_index), KPC(this));
        } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == cur_page_virtual_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid page virtual id", KR(ret), K(cur_page_virtual_id), KPC(this));
        } else if (OB_FAIL(wbp_->get_next_page_id(fd_, cur_page_index, ObTmpFilePageUniqKey(cur_page_virtual_id),
                                                  next_page_index))) {
          LOG_WARN("fail to get next page id", KR(ret), K(fd_), K(cur_page_index), K(cur_page_virtual_id), KPC(this));
        } else if (ObTmpFileGlobal::INVALID_PAGE_ID == next_page_index) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("attempt to find a non-existent page in write buffer pool", KR(ret),
                    K(target_page_virtual_id), KPC(this));
        } else if (OB_FAIL(wbp_->get_page_virtual_id(fd_, next_page_index, page_virtual_id))) {
          LOG_WARN("fail to get virtual page id", KR(ret), K(fd_), K(next_page_index), KPC(this));
        } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == page_virtual_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid page virtual id", KR(ret), K(page_virtual_id), KPC(this));
        } else if (page_virtual_id == target_page_virtual_id) {
          page_index = next_page_index;
          find = true;
        } else if (page_virtual_id < target_page_virtual_id) {
          cur_page_index = next_page_index;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lose target page in wbp", KR(ret), K(next_page_index), K(page_virtual_id),
                                              K(target_page_virtual_id), KPC(this));
        }
      } // end while
    }
  }
  return ret;
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::shrink_half()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ % 2 != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("capacity_ should be even", KR(ret), KPC(this));
  } else if (size_ <= capacity_ / 2) {
    // no need to shrink, do nothing
  } else {
    const int64_t eliminate_num = size_ - capacity_ / 2;
    const int64_t eliminate_modulus = size_ / eliminate_num;
    int64_t remain_eliminate_num = eliminate_num;
    int64_t logic_pos = left_;
    for (int64_t i = left_; i <= get_logic_tail_(); i++) {
      if ((i - left_) % eliminate_modulus != 0 || 0 == remain_eliminate_num) {
        page_indexes_[logic_pos % capacity_] = page_indexes_[i % capacity_];
        logic_pos++;
      } else { // eliminate current page index
        remain_eliminate_num--;
      }
    }

    for (int64_t i = logic_pos; i <= get_logic_tail_(); i++) {
      page_indexes_[i % capacity_] = ObTmpFileGlobal::INVALID_PAGE_ID;
    }

    right_ = (logic_pos - 1) % capacity_;
    size_ -= eliminate_num;

    if (!is_empty()) {
      min_page_index_ = page_indexes_[left_];
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tmp file page index bucket is unexpected empty", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::merge(ObTmpFilePageIndexBucket& other)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(other.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to merge an empty array", KR(ret), K(other.size_), KPC(this));
  } else if (OB_UNLIKELY(other.size_ + size_ > capacity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to merge array with too many elements", KR(ret), K(size_), K(other.size_), K(capacity_), KPC(this));
  } else if (OB_UNLIKELY(!is_empty())) {
    int64_t bkt_min_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    int64_t other_bkt_min_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    if (OB_FAIL(wbp_->get_page_virtual_id(fd_, min_page_index_, bkt_min_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(bkt_min_page_virtual_id), KPC(this));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == bkt_min_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid page virtual id", KR(ret), K(bkt_min_page_virtual_id), KPC(this));
    } else if (OB_FAIL(wbp_->get_page_virtual_id(fd_, other.min_page_index_, other_bkt_min_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(other_bkt_min_page_virtual_id), KPC(this));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == other_bkt_min_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid page virtual id", KR(ret), K(other_bkt_min_page_virtual_id), KPC(this));
    } else if (OB_UNLIKELY(bkt_min_page_virtual_id > other_bkt_min_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("attempt to merge a bucket whose min_page_virtual_id is less that itself", KR(ret),
              K(min_page_index_), K(other_bkt_min_page_virtual_id), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = other.left_; OB_SUCC(ret) && i <= other.get_logic_tail_(); i++) {
      uint32_t page_index = other.page_indexes_[i % other.capacity_];
      if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == page_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid page index", KR(ret), K(page_index), K(i), KPC(this));
      } else if (OB_FAIL(push(page_index))) {
        LOG_WARN("fail to push a page index", KR(ret), K(page_index), KPC(this));
      }
    }
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
