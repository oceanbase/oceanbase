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

#include "common/ob_range.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

#include "share/stat/ob_opt_column_stat.h"

namespace oceanbase {
namespace common {
using namespace sql;

int ObHistogram::Bucket::deep_copy(char* buf, const int64_t buf_len, Bucket*& value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer should not be null", K(ret));
  } else if (OB_UNLIKELY(buf_len < deep_copy_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size not enough", K(ret));
  } else {
    Bucket* bucket = new (buf) Bucket(endpoint_repeat_count_, endpoint_num_);
    int64_t pos = sizeof(*this);
    if (OB_FAIL(bucket->endpoint_value_.deep_copy(endpoint_value_, buf, buf_len, pos))) {
      LOG_WARN("deep copy obobj failed", K(ret));
    } else {
      value = bucket;
    }
  }
  return ret;
}

int64_t ObHistogram::deep_copy_size()
{
  int64_t size = sizeof(*this);
  for (int64_t i = 0; i < buckets_.count(); ++i) {
    if (buckets_.at(i) != nullptr) {
      size += sizeof(Bucket) + buckets_.at(i)->deep_copy_size();
    }
  }
  return size;
}

int ObHistogram::deep_copy(char* buf, const int64_t buf_len, ObHistogram*& value)
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer should not be null", K(ret));
  } else if (OB_UNLIKELY(buf_len < deep_copy_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size not enough", K(ret));
  } else {
    ObHistogram* histogram = new (buf) ObHistogram;
    int64_t pos = sizeof(*this);
    for (int64_t i = 0; OB_SUCC(ret) && i < buckets_.count(); ++i) {
      Bucket* bucket = nullptr;
      if (buckets_.at(i) == nullptr) {
        // do nothing
      } else if (OB_FAIL(buckets_.at(i)->deep_copy(buf + pos, buf_len - pos, bucket))) {
        LOG_WARN("deep copy bucket failed", K(ret));
      } else if (OB_FAIL(histogram->buckets_.push_back(bucket))) {
        LOG_WARN("push back buckets failed", K(ret));
      } else {
        pos += buckets_.at(i)->deep_copy_size();
      }
    }
    if (OB_SUCC(ret) && histogram != nullptr) {
      histogram->type_ = type_;
      histogram->sample_size_ = sample_size_;
      histogram->density_ = density_;
      histogram->bucket_cnt_ = bucket_cnt_;
      value = histogram;
    }
  }
  return ret;
}

ObOptColumnStat::ObOptColumnStat()
    : table_id_(0),
      partition_id_(0),
      column_id_(0),
      object_type_(StatLevel::INVALID_LEVEL),
      version_(0),
      num_null_(0),
      num_distinct_(0),
      min_value_(),
      max_value_(),
      object_buf_(nullptr),
      histogram_(nullptr),
      last_analyzed_(0),
      allocator_(nullptr)
{
  min_value_.set_min_value();
  max_value_.set_max_value();
}

ObOptColumnStat::ObOptColumnStat(ObIAllocator& allocator)
    : table_id_(0),
      partition_id_(0),
      column_id_(0),
      object_type_(StatLevel::INVALID_LEVEL),
      version_(0),
      num_null_(0),
      num_distinct_(0),
      min_value_(),
      max_value_(),
      object_buf_(nullptr),
      histogram_(nullptr),
      last_analyzed_(0),
      allocator_(&allocator)
{
  min_value_.set_min_value();
  max_value_.set_max_value();
  if (NULL == (object_buf_ = static_cast<char*>(allocator.alloc(MAX_OBJECT_SERIALIZE_SIZE * 2)))) {
    COMMON_LOG(WARN, "allocate memory for object_buf_ failed.");
  } else {
    MEMSET(object_buf_, 0, MAX_OBJECT_SERIALIZE_SIZE * 2);
  }
}

int ObOptColumnStat::store_max_value(const common::ObObj& max)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (NULL == object_buf_) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP_(object_buf), K(ret));
  } else if (max.get_serialize_size() > MAX_OBJECT_SERIALIZE_SIZE) {
    // ignore
  } else if (OB_FAIL(
                 max_value_.deep_copy(max, object_buf_ + MAX_OBJECT_SERIALIZE_SIZE, MAX_OBJECT_SERIALIZE_SIZE, pos))) {
    COMMON_LOG(WARN, "deep copy max object failed.", KP_(object_buf), K(max), K(pos), K(ret));
  }
  return ret;
}

int ObOptColumnStat::store_min_value(const common::ObObj& min)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (NULL == object_buf_) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP_(object_buf), K(ret));
  } else if (min.get_serialize_size() > MAX_OBJECT_SERIALIZE_SIZE) {
    // ignore
  } else if (OB_FAIL(min_value_.deep_copy(min, object_buf_, MAX_OBJECT_SERIALIZE_SIZE, pos))) {
    COMMON_LOG(WARN, "deep copy max object failed.", KP_(object_buf), K(min), K(pos), K(ret));
  }
  return ret;
}

int64_t ObOptColumnStat::size() const
{
  int64_t base_size = sizeof(ObOptColumnStat);
  base_size += min_value_.get_deep_copy_size();
  base_size += max_value_.get_deep_copy_size();
  if (histogram_ != nullptr) {
    base_size += histogram_->deep_copy_size();
  }
  return base_size;
}

int ObOptColumnStat::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(size()), K(ret));
  } else {
    ObOptColumnStat* stat = new (buf) ObOptColumnStat();
    int64_t pos = sizeof(*this);
    if (OB_FAIL(stat->deep_copy(*this, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy column stat failed.", K(ret));
    } else {
      value = stat;
    }
  }
  return ret;
}

int ObOptColumnStat::deep_copy(const ObOptColumnStat& src, char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;

  // assign base members no need to handle memory.
  table_id_ = src.table_id_;
  partition_id_ = src.partition_id_;
  column_id_ = src.column_id_;
  object_type_ = src.object_type_;
  num_null_ = src.num_null_;
  num_distinct_ = src.num_distinct_;

  if (!src.is_valid() || nullptr == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(src), KP(buf), K(size), K(ret));
  } else if (OB_FAIL(min_value_.deep_copy(src.min_value_, buf, size, pos))) {
    LOG_WARN("deep copy min_value_ failed.", K_(src.min_value), K(ret));
  } else if (OB_FAIL(max_value_.deep_copy(src.max_value_, buf, size, pos))) {
    LOG_WARN("deep copy max_value_ failed.", K_(src.max_value), K(ret));
  } else if (src.histogram_ == nullptr) {
    // do nothing if no histogram
  } else {
    ObHistogram* histogram = nullptr;
    if (OB_FAIL(src.histogram_->deep_copy(buf + pos, size - pos, histogram))) {
      LOG_WARN("deep copy histogram failed.", K_(src.max_value), K(ret));
    } else {
      histogram_ = histogram;
    }
  }
  return ret;
}

// range_density = selected_rows / total_rows_in_histogram
// should deal null outside
int ObHistogram::get_density_between_range(const ObObj& startobj, const ObObj& endobj, const ObBorderFlag& border,
    const ObDataTypeCastParams& dtc_params, double& range_density) const
{
  int ret = OB_SUCCESS;

  int64_t start_idx = OB_INVALID_INDEX;
  int64_t end_idx = OB_INVALID_INDEX;

  bool add_default_density = false;
  range_density = 0;

  bool left_is_equal = false;
  bool right_is_equal = false;

  if (OB_FAIL(get_bucket_bound_idx(
          startobj, border.inclusive_start() ? BoundType::LOWER : BoundType::UPPER, dtc_params, start_idx))) {
    LOG_WARN("failed to get start upper bound", K(ret));
  } else if (OB_FAIL(get_bucket_bound_idx(
                 endobj, border.inclusive_end() ? BoundType::UPPER : BoundType::LOWER, dtc_params, end_idx))) {
    LOG_WARN("failed to get end upper bound", K(ret));
  }
  if (OB_SUCC(ret) && start_idx >= 0 && start_idx < buckets_.count()) {
    int64_t result = true;
    if (OB_ISNULL(buckets_.at(start_idx))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("bucket is null", K(ret));
    } else if (OB_FAIL(compare_bound(startobj, buckets_.at(start_idx)->endpoint_value_, dtc_params, result))) {
      LOG_WARN("failed to compare bound", K(ret));
    } else if (0 == result) {
      left_is_equal = true;
    }
  }

  if (OB_SUCC(ret) && end_idx > 0 && end_idx <= buckets_.count()) {
    int64_t result = true;
    if (OB_ISNULL(buckets_.at(end_idx - 1))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("bucket is null", K(ret));
    } else if (OB_FAIL(compare_bound(endobj, buckets_.at(end_idx - 1)->endpoint_value_, dtc_params, result))) {
      LOG_WARN("failed to compare bound", K(ret));
    } else if (0 == result) {
      right_is_equal = true;
    }
  }

  if (OB_SUCC(ret) && (!left_is_equal || !right_is_equal)) {
    add_default_density = true;
  }

  if (OB_SUCC(ret)) {

    // un-popular value
    bool need_loop = true;
    if (start_idx < 0 || start_idx >= buckets_.count()) {
      // do nothing
    } else if (OB_ISNULL(buckets_.at(start_idx))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("bucket is null", K(ret));
    } else if (1 == end_idx - start_idx && OB_FAIL(bucket_is_popular(*(buckets_.at(start_idx)), need_loop))) {
      LOG_WARN("failed to check popular", K(ret));
    } else if (!need_loop) {
      add_default_density = true;
    } else {
      double sum = 0;
      double total = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < buckets_.count(); i++) {
        double cur_bnum = static_cast<double>(buckets_.at(i)->endpoint_num_);
        total += cur_bnum;
        if (i >= start_idx && i < end_idx) {
          sum += cur_bnum;
        }
      }
      range_density += sum / total;
    }
  }

  if (OB_SUCC(ret) && add_default_density) {
    range_density += get_density();
  }
  return ret;
}

int ObHistogram::bucket_is_popular(const Bucket& bkt, bool& is_popular) const
{
  int ret = OB_SUCCESS;
  is_popular = false;
  switch (type_) {
    case Type::FREQUENCY:
    case Type::HEIGHT_BALANCED: {
      is_popular = bkt.endpoint_num_ > 1;
      break;
    }
    case Type::HYBIRD: {
      // is_popular = bkt.repeat_count_ > (get_sample_size() / get_bucket_count());
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid histogram type", K(ret), K(type_));
      break;
    }
  }
  return ret;
}

int ObHistogram::compare_bound(
    const ObObj& left_obj, const ObObj& right_obj, const ObDataTypeCastParams& dtc_params, int64_t& result) const
{
  int ret = OB_SUCCESS;
  ObObjType compare_type;
  if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(compare_type, left_obj.get_type(), right_obj.get_type()))) {
    LOG_WARN("failed to get compare type", K(ret));
  } else {
    ObArenaAllocator arena(ObModIds::OB_BUFFER);
    ObCollationType cmp_cs_type = ObCharset::get_system_collation();
    ObCastCtx cast_ctx(&arena, &dtc_params, CM_WARN_ON_FAIL, cmp_cs_type);

    if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(
            result, left_obj, right_obj, cast_ctx, compare_type, cmp_cs_type))) {
      LOG_WARN("compare obj failed", K(ret));
    }
  }
  return ret;
}

int ObHistogram::get_bucket_bound_idx(
    const ObObj& value, BoundType btype, const ObDataTypeCastParams& dtc_params, int64_t& idx) const
{
  int ret = OB_SUCCESS;

  int64_t left = 0;
  int64_t right = get_buckets().count();
  while (OB_SUCC(ret) && left < right) {
    int64_t mid = (right + left) / 2;
    bool is_find = true;
    int64_t eq_cmp = 0;
    if (OB_ISNULL(buckets_.at(mid))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("bucket is null", K(ret));
    } else if (OB_FAIL(compare_bound(buckets_.at(mid)->endpoint_value_, value, dtc_params, eq_cmp))) {
      LOG_WARN("failed to compare", K(ret));
    } else {
      if (BoundType::LOWER == btype) {
        is_find = (eq_cmp >= 0);
      } else if (BoundType::UPPER == btype) {
        is_find = (eq_cmp > 0);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong bound type", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (is_find) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
  }
  if (OB_SUCC(ret)) {
    idx = left;
  }
  return ret;
}

int ObOptColumnStat::add_bucket(int64_t repeat_count, const ObObj& value, int64_t num_elements)
{
  int ret = OB_SUCCESS;
  char* bucket_buf = nullptr;
  if (OB_ISNULL(histogram_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("histogram and allocator should not be null when adding bucket", K(ret), K_(histogram));
  } else if (OB_ISNULL(
                 bucket_buf = static_cast<char*>(allocator_->alloc(sizeof(Bucket) + value.get_deep_copy_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for histogram bucket failed", K(ret));
  } else {
    Bucket* bucket = new (bucket_buf) Bucket(repeat_count, num_elements);
    int64_t pos = sizeof(Bucket);
    if (value.get_serialize_size() > MAX_OBJECT_SERIALIZE_SIZE) {
      // ignore
    } else if (OB_FAIL(bucket->endpoint_value_.deep_copy(value, bucket_buf, MAX_OBJECT_SERIALIZE_SIZE, pos))) {
      LOG_WARN("deep copy max object failed.", KP(bucket_buf), K(value), K(pos), K(ret));
    } else if (OB_FAIL(histogram_->buckets_.push_back(bucket))) {
      LOG_WARN("push back buckets failed", K(ret));
    }
  }

  return ret;
}

int ObOptColumnStat::init_histogram(const ObHistogram& basic_histogram_info)
{
  int ret = OB_SUCCESS;
  if (histogram_ == nullptr) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator is null when trying to allocate memory for histogram", K(ret));
    } else {
      void* histogram_buf = allocator_->alloc(sizeof(ObHistogram));
      if (OB_ISNULL(histogram_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for bucket value failed", K(ret));
      } else {
        histogram_ = new (histogram_buf) ObHistogram;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(histogram_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("histogram is still NULL after initialization", K(ret), K_(histogram));
  } else {
    histogram_->type_ = basic_histogram_info.type_;
    histogram_->bucket_cnt_ = basic_histogram_info.bucket_cnt_;
    histogram_->density_ = basic_histogram_info.density_;
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
