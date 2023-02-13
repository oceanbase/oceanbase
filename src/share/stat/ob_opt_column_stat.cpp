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

#define USING_LOG_PREFIX SQL_OPT

#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_column_stat.h"
namespace oceanbase {
namespace common {
using namespace sql;

int ObHistBucket::deep_copy(const ObHistBucket &src,
                            char *buf,
                            const int64_t buf_len,
                            int64_t & pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(endpoint_value_.deep_copy(src.endpoint_value_, buf, buf_len, pos))) {
    LOG_WARN("deep copy obobj failed", K(ret));
  } else {
    endpoint_repeat_count_ = src.endpoint_repeat_count_;
    endpoint_num_ = src.endpoint_num_;
  }
  return ret;
}

void ObHistogram::reset()
{
  type_ = ObHistType::INVALID_TYPE;
  sample_size_ = -1;
  density_ = -1;
  bucket_cnt_ = 0;
  buckets_.reset();
}

const char *ObHistogram::get_type_name() const
{
  if (ObHistType::FREQUENCY == type_) {
    return "Frequence Histogram";
  } else if (ObHistType::HEIGHT_BALANCED == type_) {
    return "Height Balanced Histogram";
  } else if (ObHistType::TOP_FREQUENCY == type_) {
    return "Top Frequence Histogram";
  } else if (ObHistType::HYBIRD == type_) {
    return "Hybrid Histogram";
  } else {
    return "Invalid";
  }
}

int64_t ObHistogram::deep_copy_size() const
{
  int64_t size = sizeof(*this);
  for (int64_t i = 0; i < buckets_.count(); ++i) {
    size += sizeof(ObHistBucket) + buckets_.at(i).deep_copy_size();
  }
  return size;
}

int ObHistogram::deep_copy(const ObHistogram &src, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  type_ = src.type_;
  sample_size_ = src.sample_size_;
  density_ = src.density_;
  bucket_cnt_ = src.bucket_cnt_;
  int64_t copy_size = src.deep_copy_size();
  if (OB_UNLIKELY(copy_size  + pos > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer size is not enough", K(ret), K(copy_size), K(pos), K(buf_len));
  } else if (!src.buckets_.empty()) {
    ObHistBucket *new_buckets = new (buf + pos) ObHistBucket[src.buckets_.count()];
    buckets_ = ObArrayWrap<ObHistBucket>(new_buckets, src.buckets_.count());
    pos += sizeof(ObHistBucket) * src.buckets_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < buckets_.count(); ++i) {
      if (OB_FAIL(buckets_.at(i).deep_copy(src.buckets_.at(i), buf, buf_len, pos))) {
        LOG_WARN("deep copy bucket failed", K(ret), K(buf_len), K(pos));
      }
    }
  }
  return ret;
}

int ObHistogram::prepare_allocate_buckets(ObIAllocator &allocator, const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buckets_.allocate_array(allocator, bucket_size))) {
    LOG_WARN("failed to prepare allocate buckets", K(ret));
  }
  return ret;
}

int ObHistogram::add_bucket(const ObHistBucket &bucket)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bucket_cnt_ >= buckets_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(bucket_cnt_), K(buckets_));
  } else {
    buckets_.at(bucket_cnt_++) = bucket;
  }
  return ret;
}

int ObHistogram::assign_buckets(const ObIArray<ObHistBucket> &buckets)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buckets_.count() != buckets.count() || bucket_cnt_ != buckets.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(buckets_), K(buckets), K(bucket_cnt_));
  } else {
    for (int64_t i = 0; i < buckets.count(); ++i) {
      buckets_.at(i) = buckets.at(i);
    }
  }
  return ret;
}

int ObHistogram::assign(const ObHistogram &other)
{
  int ret = OB_SUCCESS;
  type_ = other.type_;
  sample_size_ = other.sample_size_;
  density_ = other.density_;
  bucket_cnt_ = other.bucket_cnt_;
  pop_freq_ = other.pop_freq_;
  pop_count_ = other.pop_count_;
  return buckets_.assign(other.buckets_);
}

ObOptColumnStat::ObOptColumnStat()
    : table_id_(0),
      partition_id_(0),
      column_id_(0),
      object_type_(StatLevel::INVALID_LEVEL),
      version_(0),
      num_null_(0),
      num_not_null_(0),
      num_distinct_(0),
      avg_length_(0),
      min_value_(),
      max_value_(),
      llc_bitmap_size_(0),
      llc_bitmap_(NULL),
      histogram_(),
      last_analyzed_(0),
      cs_type_(CS_TYPE_INVALID)
{
  min_value_.set_null();
  max_value_.set_null();
}

ObOptColumnStat::ObOptColumnStat(ObIAllocator &allocator)
    : table_id_(0),
      partition_id_(0),
      column_id_(0),
      object_type_(StatLevel::INVALID_LEVEL),
      version_(0),
      num_null_(0),
      num_not_null_(0),
      num_distinct_(0),
      avg_length_(0),
      min_value_(),
      max_value_(),
      llc_bitmap_size_(0),
      llc_bitmap_(NULL),
      histogram_(),
      last_analyzed_(0),
      cs_type_(CS_TYPE_INVALID)
{
  min_value_.set_null();
  max_value_.set_null();
  if (NULL == (llc_bitmap_ = static_cast<char*>(allocator.alloc(ObColumnStat::NUM_LLC_BUCKET)))) {
    COMMON_LOG(WARN, "allocate memory for llc_bitmap_ failed.");
  } else {
    llc_bitmap_size_ = ObColumnStat::NUM_LLC_BUCKET;
    MEMSET(llc_bitmap_, 0, llc_bitmap_size_);
  }
}

int64_t ObOptColumnStat::size() const
{
  int64_t base_size = sizeof(ObOptColumnStat);
  base_size += min_value_.get_deep_copy_size();
  base_size += max_value_.get_deep_copy_size();
  base_size += histogram_.deep_copy_size();
  base_size += llc_bitmap_size_;

  return base_size;
}

int ObOptColumnStat::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(buf), K(buf_len), K(size()), K(ret));
  } else {
    ObOptColumnStat *stat = new (buf) ObOptColumnStat();
    int64_t pos = sizeof(*this);
    if (OB_FAIL(stat->deep_copy(*this, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy column stat failed.", K(ret));
    } else {
      value = stat;
    }
  }
  return ret;
}

int ObOptColumnStat::deep_copy(const ObOptColumnStat &src, char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;

  // assign base members no need to handle memory.
  table_id_ = src.table_id_;
  partition_id_ = src.partition_id_;
  column_id_ = src.column_id_;
  object_type_ = src.object_type_;
  version_ = src.version_;
  num_null_ = src.num_null_;
  num_not_null_ = src.num_not_null_;
  num_distinct_ = src.num_distinct_;
  avg_length_ = src.avg_length_;
  last_analyzed_ = src.last_analyzed_;
  cs_type_ = src.cs_type_;

  if (!src.is_valid() || nullptr == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(src), KP(buf), K(size), K(ret));
  } else if (OB_FAIL(min_value_.deep_copy(src.min_value_, buf, size, pos))) {
    LOG_WARN("deep copy min_value_ failed.", K_(src.min_value), K(ret));
  } else if (OB_FAIL(max_value_.deep_copy(src.max_value_, buf, size, pos))) {
    LOG_WARN("deep copy max_value_ failed.", K_(src.max_value), K(ret));
  } else if (OB_FAIL(histogram_.deep_copy(src.histogram_, buf, size, pos))) {
    LOG_WARN("failed to deep copy histogram", K(ret));
  } else if (pos + src.llc_bitmap_size_ > size) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("llc bitmap size overflow", K(ret), K(pos), K(src.llc_bitmap_size_), K(size));
  } else {
    llc_bitmap_ = buf + pos;
    llc_bitmap_size_ = src.llc_bitmap_size_;
    MEMCPY(llc_bitmap_, src.llc_bitmap_, src.llc_bitmap_size_);
    pos += llc_bitmap_size_;
  }
  return ret;
}

/**
 * @brief ObHistogram::calc_density
 *  basically, we assume non-popular values are uniformly distributed
 * @param row_count
 * @param pop_row_count
 * @param ndv
 * @param pop_ndv
 */
void ObHistogram::calc_density(ObHistType hist_type,
                               const int64_t row_count,
                               const int64_t pop_row_count,
                               const int64_t ndv,
                               const int64_t pop_ndv)
{
  if (ObHistType::FREQUENCY == hist_type ||
      ObHistType::TOP_FREQUENCY == hist_type ||
      ndv <= pop_ndv || row_count == pop_row_count ) {
    density_ = 1.0 / (row_count * 2);
  } else {
    density_ = (1.0 * (row_count - pop_row_count)) / ((ndv - pop_ndv) * row_count);
  }
}


}
}
