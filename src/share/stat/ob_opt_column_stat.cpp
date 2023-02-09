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
#include "share/stat/ob_stat_item.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"
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
  if (NULL != buckets_) {
    for (int64_t i = 0; i < bucket_size_; ++i) {
      buckets_[i].~ObHistBucket();
    }
    get_allocator().free(buckets_);
    inner_allocator_.reset();
    buckets_ = NULL;
    bucket_size_ = 0;
    max_bucket_size_ = 0;
  }
  pop_freq_ = 0;
  pop_count_ = 0;
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
  for (int64_t i = 0; i < bucket_size_; ++i) {
    size += sizeof(ObHistBucket) + buckets_[i].deep_copy_size();
  }
  return size;
}

int ObHistogram::deep_copy(const ObHistogram &src, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  type_ = src.type_;
  sample_size_ = src.sample_size_;
  density_ = src.density_;
  int64_t copy_size = src.deep_copy_size();
  if (OB_UNLIKELY(copy_size  + pos > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer size is not enough", K(ret), K(copy_size), K(pos), K(buf_len));
  } else if (src.bucket_size_ > 0 && src.buckets_ != NULL) {
    buckets_ = new (buf + pos) ObHistBucket[src.bucket_size_];
    bucket_size_ = src.bucket_size_;
    max_bucket_size_ = src.bucket_size_;
    pos += sizeof(ObHistBucket) * bucket_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_size_; ++i) {
      if (OB_FAIL(buckets_[i].deep_copy(src.buckets_[i], buf, buf_len, pos))) {
        LOG_WARN("deep copy bucket failed", K(ret), K(buf_len), K(pos));
      }
    }
  }
  return ret;
}

int ObHistogram::assign_buckets(const ObHistBucket *buckets, const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (buckets == NULL || bucket_size == 0) {
    //do nothing
  } else if (buckets_ != NULL || OB_UNLIKELY(bucket_size_ > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(buckets_), K(bucket_size_));
  } else if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObHistBucket) * bucket_size))) {
    COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "allocate memory for buckets failed.");
  } else {
    buckets_ = new (buf) ObHistBucket[bucket_size];
    bucket_size_ = bucket_size;
    max_bucket_size_ = bucket_size;
    for (int64_t i = 0; i < bucket_size_; ++i) {
      buckets_[i].endpoint_repeat_count_ = buckets[i].endpoint_repeat_count_;
      buckets_[i].endpoint_num_ = buckets[i].endpoint_num_;
      buckets_[i].endpoint_value_ = buckets[i].endpoint_value_;
    }
  }
  return ret;
}

//the endpoint value is shallow copy!!!!!!!!!
int ObHistogram::add_buckets(const ObIArray<ObHistBucket> &buckets)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (buckets.empty()) {
    //do nothing
  } else if (buckets_ != NULL || OB_UNLIKELY(bucket_size_ > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(buckets_), K(bucket_size_));
  } else if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObHistBucket) * buckets.count()))) {
    COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "allocate memory for buckets failed.");
  } else {
    buckets_ = new (buf) ObHistBucket[buckets.count()];
    bucket_size_ = buckets.count();
    max_bucket_size_ = buckets.count();
    for (int64_t i = 0; i < bucket_size_; ++i) {
      buckets_[i].endpoint_repeat_count_ = buckets.at(i).endpoint_repeat_count_;
      buckets_[i].endpoint_num_ = buckets.at(i).endpoint_num_;
      buckets_[i].endpoint_value_ = buckets.at(i).endpoint_value_;
    }
  }
  return ret;
}

//the endpoint value is shallow copy!!!!!!!!!
int ObHistogram::add_bucket(const ObHistBucket &bucket)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buckets_) && bucket_size_ != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(buckets_), K(bucket_size_), K(max_bucket_size_));
  } else if (bucket_size_ < max_bucket_size_) {
    if (OB_ISNULL(buckets_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(buckets_), K(bucket_size_), K(max_bucket_size_));
    } else {
      buckets_[bucket_size_].endpoint_repeat_count_ = bucket.endpoint_repeat_count_;
      buckets_[bucket_size_].endpoint_num_ = bucket.endpoint_num_;
      buckets_[bucket_size_].endpoint_value_ = bucket.endpoint_value_;
      ++ bucket_size_;
    }
  } else {
    void *buf = NULL;
    max_bucket_size_ = bucket_size_ == 0 ? 1 : 2 * bucket_size_;
    if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObHistBucket) * max_bucket_size_))) {
      COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "allocate memory for buckets failed.");
    } else {
      ObHistBucket *new_buckets = new (buf) ObHistBucket[max_bucket_size_];
      for (int64_t i = 0; i < bucket_size_; ++i) {
        new_buckets[i].endpoint_repeat_count_ = buckets_[i].endpoint_repeat_count_;
        new_buckets[i].endpoint_num_ = buckets_[i].endpoint_num_;
        new_buckets[i].endpoint_value_ = buckets_[i].endpoint_value_;
      }
      new_buckets[bucket_size_].endpoint_repeat_count_ = bucket.endpoint_repeat_count_;
      new_buckets[bucket_size_].endpoint_num_ = bucket.endpoint_num_;
      new_buckets[bucket_size_].endpoint_value_ = bucket.endpoint_value_;
      ++ bucket_size_;
      get_allocator().free(buckets_);
      buckets_ = new_buckets;
    }
  }
  return ret;
}

int ObHistogram::prepare_allocate_buckets(const int64_t buckets_num)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (buckets_ != NULL || OB_UNLIKELY(bucket_size_ > 0 || buckets_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(buckets_), K(bucket_size_), K(buckets_num));
  } else if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObHistBucket) * buckets_num))) {
    COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "allocate memory for buckets failed.");
  } else {
    buckets_ = new (buf) ObHistBucket[buckets_num];
    max_bucket_size_ = buckets_num;
  }
  return ret;
}

int64_t ObHistogram::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("Type", get_type_name(),
        K_(sample_size),
        K_(density),
        K_(bucket_size),
        K_(max_bucket_size),
        K_(buckets));
  if (buckets_ != NULL && bucket_size_ > 0 && max_bucket_size_ > 0) {
    for (int64_t i = 0; i < bucket_size_; ++i) {
      J_KV(K(buckets_[i]));
    }
  }
  J_OBJ_END();
  return pos;
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
      cs_type_(CS_TYPE_INVALID),
      inner_max_allocator_("OptColStatMax"),
      inner_min_allocator_("OptColStatMin")
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
      histogram_(allocator),
      last_analyzed_(0),
      cs_type_(CS_TYPE_INVALID),
      inner_max_allocator_("OptColStatMax"),
      inner_min_allocator_("OptColStatMin")
{
  min_value_.set_null();
  max_value_.set_null();
  if (NULL == (llc_bitmap_ = static_cast<char*>(allocator.alloc(ObColumnStat::NUM_LLC_BUCKET)))) {
    COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "allocate memory for llc_bitmap_ failed.");
  } else {
    llc_bitmap_size_ = ObColumnStat::NUM_LLC_BUCKET;
    MEMSET(llc_bitmap_, 0, llc_bitmap_size_);
  }
}

void ObOptColumnStat::reset()
{
  table_id_ = 0;
  partition_id_ = 0;
  column_id_ = 0;
  object_type_ = StatLevel::INVALID_LEVEL;
  version_ = 0;
  num_null_ = 0;
  num_not_null_ = 0;
  num_distinct_ = 0;
  avg_length_ = 0;
  llc_bitmap_size_ = 0;
  llc_bitmap_ = NULL;
  last_analyzed_ = 0;
  cs_type_ = CS_TYPE_INVALID;
  inner_max_allocator_.reset();
  inner_min_allocator_.reset();
  histogram_.reset();
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

int ObOptColumnStat::deep_copy(char *buf, const int64_t buf_len, ObOptColumnStat *&value) const
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

OB_DEF_SERIALIZE(ObOptColumnStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              partition_id_,
              column_id_,
              num_distinct_,
              num_null_,
              num_not_null_,
              min_value_,
              max_value_,
              llc_bitmap_size_,
              avg_length_,
              object_type_);
  if (llc_bitmap_size_ != 0 && llc_bitmap_size_ < buf_len - pos) {
    MEMCPY(buf + pos, llc_bitmap_, llc_bitmap_size_);
    pos += llc_bitmap_size_;
  }
  return ret;

}

OB_DEF_SERIALIZE_SIZE(ObOptColumnStat) {
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              partition_id_,
              column_id_,
              num_distinct_,
              num_null_,
              num_not_null_,
              min_value_,
              max_value_,
              llc_bitmap_size_,
              avg_length_,
              object_type_);
  if (llc_bitmap_size_ !=0)
    len += llc_bitmap_size_;
  return len;
}

OB_DEF_DESERIALIZE(ObOptColumnStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              table_id_,
              partition_id_,
              column_id_,
              num_distinct_,
              num_null_,
              num_not_null_,
              min_value_,
              max_value_,
              llc_bitmap_size_,
              avg_length_,
              object_type_);
  if (llc_bitmap_size_ !=0 && data_len - pos >= llc_bitmap_size_) {
    memcpy(llc_bitmap_, buf + pos, llc_bitmap_size_);
    pos += llc_bitmap_size_;
  }
  return ret;
}

// shallow copy
int ObOptColumnStat::merge_column_stat(const ObOptColumnStat &other)
{
  int ret = OB_SUCCESS;
  if (table_id_ != other.get_table_id() ||
      partition_id_ != other.get_partition_id() ||
      column_id_ != other.get_column_id()) {
    // do not merge stats that not match.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the key not match", K(ret));
  } else {
    double avg_len = 0;
    other.get_avg_len(avg_len);
    merge_avg_len(avg_len, other.get_num_not_null() + other.get_num_null());
    num_null_ += other.get_num_null();
    num_not_null_ += other.get_num_not_null();
    const ObObj &min_val = other.get_min_value();
    const ObObj &max_val = other.get_max_value();
    if (!min_val.is_null() && (min_value_.is_null() || min_val < min_value_)) {
      inner_min_allocator_.reuse();
      if (OB_FAIL(ob_write_obj(inner_min_allocator_, min_val, min_value_))) {
        LOG_WARN("fail to deep copy obj", K(ret));
      }
    }
    if (OB_SUCC(ret) && (!max_val.is_null() && (max_value_.is_null() || max_val > max_value_))) {
      inner_max_allocator_.reuse();
      if (OB_FAIL(ob_write_obj(inner_max_allocator_, max_val, max_value_))) {
        LOG_WARN("fail to deep copy obj", K(ret));
      }
    }
    // llc
    if (llc_bitmap_size_ == other.get_llc_bitmap_size()) {
      ObGlobalNdvEval::update_llc(llc_bitmap_, other.get_llc_bitmap());
    }
    // do not process histogram
  }
  return ret;
}

// deep copy max/min, using inner_allocator
int ObOptColumnStat::merge_obj(ObObj &obj)
{
  int ret = OB_SUCCESS;
  //calc avg_len: avg_len should update before num_null, null_not_null -- since these are use in merge_avg_len.
  // the following code is abandoned, since it is not accurate.
  // set calc avg_row_len using ObExprSysOpOpnsize::calc_sys_op_opnsize by datum.
  // and set avg_row_len outer side this function.
  //int64_t row_len  = 0;
  //row_len = sizeof(ObObj) + obj.get_deep_copy_size();
  //merge_avg_len(row_len, 1);

  if (obj.is_null()) {
    num_null_++;
  } else {
    num_not_null_++;
    // max/min
    if (min_value_.is_null() || obj < min_value_) {
      inner_min_allocator_.reuse();
      if (OB_FAIL(ob_write_obj(inner_min_allocator_, obj, min_value_))) {
        LOG_WARN("fail to deep copy obj", K(ret));
      }
    }
    if (OB_SUCC(ret) && (max_value_.is_null() || obj > max_value_)) {
      inner_max_allocator_.reuse();
      if (OB_FAIL(ob_write_obj(inner_max_allocator_, obj, max_value_))) {
        LOG_WARN("fail to deep copy obj", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // calc llc.
      uint64_t hash_value = 0;
      hash_value = obj.is_string_type() ?
                   obj.varchar_hash(obj.get_collation_type(), hash_value) :
                   obj.hash(hash_value);
      if (OB_FAIL(ObAggregateProcessor::llc_add_value(hash_value, llc_bitmap_, llc_bitmap_size_))) {
        LOG_WARN("fail to calc llc", K(ret));
      }
      //don't need to get call ObGlobalNdvEval::get_ndv_from_llc here, call it later.
    }
  }

  return ret;
}

// deep copy max/min
/*int ObOptColumnStat::deep_copy_max_min_obj()
{
  int ret = OB_SUCCESS;
  inner_min_allocator_.reuse();
  inner_max_allocator_.reuse();
  if (OB_FAIL(ob_write_obj(inner_max_allocator_, max_value_, max_value_))) {
    LOG_WARN("fail to deep copy obj", K(ret));
  } else if (OB_FAIL(ob_write_obj(inner_min_allocator_, min_value_, min_value_))) {
    LOG_WARN("fail to deep copy obj", K(ret));
  }
  return ret;
}*/
/* the max_alloc and min_alloc must be res-able, only for future usage.
int ObOptColumnStat::merge_obj(ObObj &obj,
                               ObIAllocator &max_alloc,
                               ObIAllocator &min_alloc)
{
  int ret = OB_SUCCESS;
  int64_t row_len  = 0;

  if (obj.is_null()) {
    num_null_++;
  } else {
    num_not_null_++;
  }
  // max/min
  if (OB_FAIL(merge_max_val(obj, max_alloc))) {
    LOG_WARN("fail to merge max value", K(ret));
  } else if (OB_FAIL(merge_min_val(obj, min_alloc))) {
    LOG_WARN("fail to merge min value", K(ret));
  }
  // avg_len
  row_len = sizeof(ObObj) + obj.get_deep_copy_size();
  merge_avg_len(row_len, 1);

  // calc llc.
  if (OB_SUCC(ret)) {
    uint64_t hash_value = 0;
    hash_value = obj.is_string_type() ?
                 obj.varchar_hash(obj.get_collation_type(), hash_value) :
                 obj.hash(hash_value);
    if (OB_FAIL(ObAggregateProcessor::llc_add_value(hash_value, llc_bitmap_, llc_bitmap_size_))) {
      LOG_WARN("fail to calc llc", K(ret));
    } else {
      num_distinct_ = ObGlobalNdvEval::get_ndv_from_llc(llc_bitmap_);
    }
  }
  return ret;
}*/

// this two function couldn't guarantee the alloc is reuseable. So do not use it.
// only for future usage.
/*int ObOptColumnStat::merge_min_val(const common::ObObj &min_val,
                                   common::ObIAllocator &alloc) {
  int ret = OB_SUCCESS;
  LOG_TRACE("MERGE min val", K(min_val), K(min_value_));
  if (min_value_.is_null() || min_val < min_value_) {
    alloc.reuse();
    if (OB_FAIL(ob_write_obj(alloc, min_val, min_value_))) {
      LOG_WARN("fail to deep copy obj", K(ret));
    }
  }
  return ret;
}

int ObOptColumnStat::merge_max_val(const common::ObObj &max_val,
                                   common::ObIAllocator &alloc) {
  int ret = OB_SUCCESS;
  LOG_TRACE("MERGE max val", K(max_val), K(max_value_));
  if (max_value_.is_null() || max_val > max_value_) {
    alloc.reuse();
    if (OB_FAIL(ob_write_obj(alloc, max_val, max_value_))) {
      LOG_WARN("fail to deep copy obj", K(ret));
    }
  }
  return ret;
}*/

}
}
