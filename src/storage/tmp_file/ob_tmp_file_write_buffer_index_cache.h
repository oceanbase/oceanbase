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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_WRITE_BUFFER_INDEX_CACHE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_WRITE_BUFFER_INDEX_CACHE_H_

#include "lib/container/ob_array.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpWriteBufferPool;

class ObTmpFileCircleArray
{
public:
  ObTmpFileCircleArray() : is_inited_(false), left_(0), right_(-1), size_(0), capacity_(0) {}
  virtual ~ObTmpFileCircleArray() {};
  virtual int push(const uint32_t page_index) = 0;
  virtual int truncate(const int64_t truncate_page_virtual_id) = 0;
  virtual int binary_search(const int64_t target_page_virtual_id, uint32_t &page_index) = 0;

public:
  OB_INLINE int64_t size() const { return size_; }
  OB_INLINE bool is_empty() const { return size_ == 0; }
  virtual OB_INLINE bool is_full() const { return size_ == capacity_; }
  TO_STRING_KV(K(is_inited_), K(left_), K(right_), K(size_), K(capacity_));

protected:
  OB_INLINE void inc_pos_(int64_t &pos) { pos = pos == capacity_ - 1 ? 0 : pos + 1; }
  OB_INLINE void dec_pos_(int64_t &pos) { pos = pos == 0 ? capacity_ - 1 : pos - 1; }
  OB_INLINE int64_t get_previous_pos(const int64_t pos) const { return pos == 0 ? capacity_ - 1 : pos - 1; }
  OB_INLINE int64_t get_logic_tail_() const { return get_logic_pos_(right_); }
  OB_INLINE int64_t get_logic_pos_(const int64_t pos) const
  {
    return pos >= left_ ? pos : capacity_ + pos;
  }

protected:
  bool is_inited_;
  int64_t left_; // close interval
  int64_t right_; // close interval
  int64_t size_; // record valid number of array
  int64_t capacity_; // record the capacity of array
};

// Attention:
// ObTmpFileWBPIndexCache caches page indexes of a tmp file in write buffer pool.
// When ObTmpFileWBPIndexCache reaches the limitation of capacity,
// it will eliminates half page indexes of buckets for caching new page index.
// thus, the range of cached indexes might be not continuous.
class ObTmpFileWBPIndexCache : public ObTmpFileCircleArray
{
public:
  ObTmpFileWBPIndexCache();
  ~ObTmpFileWBPIndexCache();
  int init(const int64_t fd, ObTmpWriteBufferPool* wb,
           ObIAllocator *wbp_index_cache_allocator,
           ObIAllocator *wbp_index_cache_bkt_allocator);
  void destroy();
  void reset();
  virtual int push(const uint32_t page_index) override;
  // truncate page virtual id is open interval
  virtual int truncate(const int64_t truncate_page_virtual_id) override;
  // truncate page virtual id is close interval
  virtual int binary_search(const int64_t target_page_virtual_id, uint32_t &page_index) override;
  virtual OB_INLINE bool is_full() const override
  {
    return ObTmpFileCircleArray::is_full() &&
           right_ >= 0 && right_ < capacity_ &&
           OB_NOT_NULL(page_buckets_) &&
           OB_NOT_NULL(page_buckets_->at(right_)) &&
           page_buckets_->at(right_)->is_full();
  }
  INHERIT_TO_STRING_KV("ObTmpFileCircleArray", ObTmpFileCircleArray,
                       K(fd_), KP(wbp_), KP(page_buckets_),
                       KP(bucket_array_allocator_), KP(bucket_allocator_));

private:
  class ObTmpFilePageIndexBucket : public ObTmpFileCircleArray
  {
  public:
    ObTmpFilePageIndexBucket();
    ~ObTmpFilePageIndexBucket();
    int init(int64_t fd, ObTmpWriteBufferPool* wbp);
    void destroy();
    virtual int push(const uint32_t page_index) override;
    // truncate offset is open interval
    virtual int truncate(const int64_t truncate_page_virtual_id) override;
    // target offset is close interval
    virtual int binary_search(const int64_t target_page_virtual_id, uint32_t &page_index) override;
    int shrink_half();
    int merge(ObTmpFilePageIndexBucket& other);
    OB_INLINE int64_t get_min_page_index() const { return min_page_index_; }
    INHERIT_TO_STRING_KV("ObTmpFileCircleArray", ObTmpFileCircleArray,
                         K(fd_), KP(wbp_), K(min_page_index_));
  private:
    int pop_();
  private:
    // due to each page index in bucket directs to a 8KB page in wbp,
    // a bucket could indicate a 256KB data and hold on 128B memory
    static const int64_t BUCKET_CAPACITY = 1 << 5;

  private:
    common::ObArray<uint32_t> page_indexes_;
    int64_t fd_;
    ObTmpWriteBufferPool* wbp_;
    int64_t min_page_index_;
  };

private:
  // due to each bucket could indicate a 256KB data in wbp,
  // the cache could indicate a 256MB data and hold on (128 + 8)*2^10 = 136KB memory at most
  static const int64_t MAX_BUCKET_ARRAY_CAPACITY = 1 << 10;
  static const int64_t INIT_BUCKET_ARRAY_CAPACITY = 1 << 3; // indicates 2MB data in wbp at most
  static const int64_t SHRINK_THRESHOLD = 4; // attention: this value must be larger than 2

private:
  int expand_();
  void shrink_();
  int sparsify_();

private:
  ObIAllocator *bucket_array_allocator_;
  ObIAllocator *bucket_allocator_;
  common::ObArray<ObTmpFilePageIndexBucket*> *page_buckets_;
  int64_t fd_;
  ObTmpWriteBufferPool* wbp_;
  int64_t max_bucket_array_capacity_;   // only allowed to modify this var in unit test!!!
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_WRITE_BUFFER_INDEX_CACHE_H_
