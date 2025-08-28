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

#include "share/stat/ob_opt_external_column_stat_builder.h"
#include "common/object/ob_obj_compare.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/hive/ob_hive_fm_sketch_utils.h"
#include "share/stat/hive/ob_hive_hll_utils.h"
#include <algorithm>
#include <cstring>

namespace oceanbase {
namespace share {

ObOptExternalColumnStatBuilder::ObOptExternalColumnStatBuilder(
    common::ObIAllocator &allocator)
    : allocator_(allocator), tenant_id_(common::OB_INVALID_ID),
      catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(), table_name_(),
      partition_value_(), column_name_(), num_null_(0), num_not_null_(0),
      num_distinct_(0), avg_length_(0), min_value_(), max_value_(),
      bitmap_size_(0), bitmap_(nullptr), last_analyzed_(0),
      cs_type_(common::ObCollationType::CS_TYPE_INVALID), min_obj_buf_(nullptr),
      min_obj_buf_size_(0), max_obj_buf_(nullptr), max_obj_buf_size_(0),
      bitmap_type_(ObExternalBitmapType::UNKNOWN), hive_fm_sketch_(nullptr),
      hive_hll_(nullptr), final_bitmap_(nullptr), final_bitmap_size_(0),
      final_ndv_(0), is_bitmap_finalized_(false), is_basic_info_set_(false),
      is_stat_info_set_(false), is_min_value_set_(false),
      is_max_value_set_(false), is_bitmap_set_(false) {}

ObOptExternalColumnStatBuilder::~ObOptExternalColumnStatBuilder() { reset(); }

void ObOptExternalColumnStatBuilder::reset() {
  tenant_id_ = common::OB_INVALID_ID;
  catalog_id_ = OB_INTERNAL_CATALOG_ID;
  database_name_.reset();
  table_name_.reset();
  partition_value_.reset();
  column_name_.reset();
  num_null_ = 0;
  num_not_null_ = 0;
  num_distinct_ = 0;
  avg_length_ = 0;
  min_value_.set_null();
  max_value_.set_null();
  bitmap_size_ = 0;
  bitmap_ = nullptr;
  last_analyzed_ = 0;
  cs_type_ = common::ObCollationType::CS_TYPE_INVALID;
  bitmap_type_ = ObExternalBitmapType::UNKNOWN;

  // Clean up deserialized bitmap objects
  if (OB_NOT_NULL(hive_fm_sketch_)) {
    hive_fm_sketch_->~ObHiveFMSketch();
    hive_fm_sketch_ = nullptr;
  }
  if (OB_NOT_NULL(hive_hll_)) {
    hive_hll_->~ObHiveHLL();
    hive_hll_ = nullptr;
  }

  // Clean up finalized bitmap
  final_bitmap_ =
      nullptr; // Note: this memory should be managed by the allocator
  final_bitmap_size_ = 0;
  final_ndv_ = 0;
  is_bitmap_finalized_ = false;

  is_basic_info_set_ = false;
  is_stat_info_set_ = false;
  is_min_value_set_ = false;
  is_max_value_set_ = false;
  is_bitmap_set_ = false;
}

int ObOptExternalColumnStatBuilder::set_basic_info(
    uint64_t tenant_id, uint64_t catalog_id,
    const common::ObString &database_name, const common::ObString &table_name,
    const common::ObString &partition_value,
    const common::ObString &column_name) {
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  catalog_id_ = catalog_id;
  column_name_ = column_name;
  database_name_ = database_name;
  table_name_ = table_name;
  partition_value_ = partition_value;
  is_basic_info_set_ = true;

  return ret;
}

int ObOptExternalColumnStatBuilder::set_stat_info(
    int64_t num_null, int64_t num_not_null, int64_t num_distinct,
    int64_t avg_length, int64_t last_analyzed,
    common::ObCollationType cs_type) {
  int ret = OB_SUCCESS;
  num_null_ = num_null;
  num_not_null_ = num_not_null;
  num_distinct_ = num_distinct;
  avg_length_ = avg_length;
  last_analyzed_ = last_analyzed;
  cs_type_ = cs_type;
  is_stat_info_set_ = true;
  return ret;
}

int ObOptExternalColumnStatBuilder::set_min_value(
    const common::ObObj &min_value) {
  int ret = OB_SUCCESS;
  min_value_ = min_value;
  is_min_value_set_ = true;
  return ret;
}

int ObOptExternalColumnStatBuilder::set_max_value(
    const common::ObObj &max_value) {
  int ret = OB_SUCCESS;
  max_value_ = max_value;
  is_max_value_set_ = true;
  return ret;
}

int ObOptExternalColumnStatBuilder::set_bitmap(const char *bitmap,
                                               int64_t bitmap_size) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bitmap) || bitmap_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap parameters", K(ret), KP(bitmap), K(bitmap_size));
  } else if (bitmap_type_ == ObExternalBitmapType::HIVE_FM) {
    // For Hive FM bitmap, create and initialize the FM sketch object directly
    if (OB_ISNULL(hive_fm_sketch_)) {
      hive_fm_sketch_ = OB_NEWx(ObHiveFMSketch, &allocator_, allocator_);
      if (OB_ISNULL(hive_fm_sketch_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hive fm sketch", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      if (OB_FAIL(ObHiveFMSketchUtils::deserialize_fm_sketch(
              bitmap, bitmap_size, pos, *hive_fm_sketch_))) {
        LOG_WARN("failed to deserialize FM bitmap", K(ret), K(bitmap_size));
      } else {
        is_bitmap_set_ = true;
        LOG_DEBUG("successfully set Hive FM bitmap", K(bitmap_size));
      }
    }
  } else if (bitmap_type_ == ObExternalBitmapType::HIVE_HLL) {
    // For Hive HLL bitmap, deserialize and store the HLL object directly
    int64_t pos = 0;
    if (OB_FAIL(ObHiveHLLUtils::deserialize_hll(bitmap, bitmap_size, pos,
                                                allocator_, hive_hll_))) {
      LOG_WARN("failed to deserialize HLL bitmap", K(ret), K(bitmap_size));
    } else {
      is_bitmap_set_ = true;
    }
  } else {
    // For other bitmap types, use the original logic
    if (OB_FAIL(alloc_bitmap(bitmap_size))) {
      LOG_WARN("failed to allocate bitmap", K(ret), K(bitmap_size));
    } else {
      MEMCPY(bitmap_, bitmap, bitmap_size);
      is_bitmap_set_ = true;
    }
  }
  return ret;
}

int ObOptExternalColumnStatBuilder::merge_column_stat(
    const ObOptExternalColumnStat &other) {
  int ret = OB_SUCCESS;

  // Merge basic statistical values
  num_null_ += other.get_num_null();
  num_not_null_ += other.get_num_not_null();
  num_distinct_ = num_distinct_ > other.get_num_distinct()
                      ? num_distinct_
                      : other.get_num_distinct();

  // Calculate weighted average length
  int64_t total_rows = num_null_ + num_not_null_;
  int64_t other_total_rows = other.get_num_null() + other.get_num_not_null();
  if (total_rows > 0 && other_total_rows > 0) {
    avg_length_ = (avg_length_ * (total_rows - other_total_rows) +
                   other.get_avg_length() * other_total_rows) /
                  total_rows;
  } else if (other_total_rows > 0) {
    avg_length_ = other.get_avg_length();
  }

  // Merge min/max values
  if (OB_FAIL(merge_min_value(other.get_min_value()))) {
    LOG_WARN("failed to merge min value", K(ret));
  } else if (OB_FAIL(merge_max_value(other.get_max_value()))) {
    LOG_WARN("failed to merge max value", K(ret));
  } else if (other.get_bitmap_size() > 0 && other.get_bitmap() != nullptr) {
    if (OB_FAIL(merge_bitmap(other.get_bitmap(), other.get_bitmap_size()))) {
      LOG_WARN("failed to merge bitmap", K(ret));
    }
  }

  // Update last analyzed time to the latest
  last_analyzed_ = last_analyzed_ > other.get_last_analyzed()
                       ? last_analyzed_
                       : other.get_last_analyzed();

  return ret;
}

int ObOptExternalColumnStatBuilder::merge_stat_values(int64_t num_null,
                                                      int64_t num_not_null,
                                                      int64_t num_distinct,
                                                      int64_t avg_length) {
  int ret = OB_SUCCESS;

  // Merge basic statistical values
  int64_t old_total_rows = num_null_ + num_not_null_;
  int64_t new_total_rows = num_null + num_not_null;

  num_null_ += num_null;
  num_not_null_ += num_not_null;

  // For num_distinct, take the maximum (this is a simplification, real merging
  // would need HyperLogLog)
  num_distinct_ = num_distinct_ > num_distinct ? num_distinct_ : num_distinct;

  // Calculate weighted average length
  int64_t total_rows = num_null_ + num_not_null_;
  if (total_rows > 0) {
    if (old_total_rows > 0 && new_total_rows > 0) {
      avg_length_ =
          (avg_length_ * old_total_rows + avg_length * new_total_rows) /
          total_rows;
    } else if (new_total_rows > 0) {
      avg_length_ = avg_length;
    }
    // If old_total_rows > 0 but new_total_rows == 0, keep existing avg_length_
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::merge_min_value(
    const common::ObObj &min_value) {
  int ret = OB_SUCCESS;
  if (!min_value.is_null()) {
    if (OB_FAIL(merge_min_max(min_value_, min_value, true /* is_cmp_min */))) {
      LOG_WARN("failed to merge min value", K(ret), K(min_value),
               K(min_value_));
    } else {
      is_min_value_set_ = true;
    }
  }
  return ret;
}

int ObOptExternalColumnStatBuilder::merge_max_value(
    const common::ObObj &max_value) {
  int ret = OB_SUCCESS;
  if (!max_value.is_null()) {
    if (OB_FAIL(merge_min_max(max_value_, max_value, false /* is_cmp_min */))) {
      LOG_WARN("failed to merge max value", K(ret), K(max_value),
               K(max_value_));
    } else {
      is_max_value_set_ = true;
    }
  }
  return ret;
}

int ObOptExternalColumnStatBuilder::merge_bitmap(const char *bitmap,
                                                 int64_t bitmap_size) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bitmap) || bitmap_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap parameters", K(ret), KP(bitmap), K(bitmap_size));
  } else if (!is_bitmap_set_) {
    // First bitmap - handle differently based on type
    if (bitmap_type_ == ObExternalBitmapType::HIVE_AUTO_DETECT) {
      // Auto-detect Hive bitmap type for the first bitmap
      ObExternalBitmapType detected_type =
          detect_hive_bitmap_type(bitmap, bitmap_size);
      bitmap_type_ = detected_type;
      LOG_TRACE("auto-detected Hive bitmap type for first bitmap",
                K(bitmap_type_), K(bitmap_size));
    } else if (bitmap_type_ == ObExternalBitmapType::OTHER) {
      // Non-Hive bitmap, no detection needed
      LOG_DEBUG("processing non-hive bitmap, no type detection",
                K(bitmap_size));
    }

    // Copy the first bitmap
    if (OB_FAIL(set_bitmap(bitmap, bitmap_size))) {
      LOG_WARN("failed to set first bitmap", K(ret), K(bitmap_type_));
    }
  } else {
    // Subsequent bitmaps - merge based on established type
    switch (bitmap_type_) {
    case ObExternalBitmapType::HIVE_FM:
      if (OB_FAIL(merge_hive_fm_bitmap(bitmap, bitmap_size))) {
        LOG_WARN("failed to merge Hive FM bitmap", K(ret));
      }
      break;

    case ObExternalBitmapType::HIVE_HLL:
      if (OB_FAIL(merge_hive_hll_bitmap(bitmap, bitmap_size))) {
        LOG_WARN("failed to merge Hive HLL bitmap", K(ret));
      }
      break;

    case ObExternalBitmapType::OTHER:
      if (OB_FAIL(merge_other_bitmap(bitmap, bitmap_size))) {
        LOG_WARN("failed to merge other bitmap", K(ret));
      }
      break;

    case ObExternalBitmapType::HIVE_AUTO_DETECT:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap type should have been determined by now", K(ret),
               K(bitmap_type_));
      break;

    case ObExternalBitmapType::UNKNOWN:
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown bitmap type cannot be merged", K(ret), K(bitmap_type_));
      break;
    }
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::finalize_bitmap() {
  int ret = OB_SUCCESS;
  // Initialize with current num_distinct_ in case no bitmap objects exist
  final_ndv_ = num_distinct_;
  final_bitmap_ = nullptr;
  final_bitmap_size_ = 0;
  if (is_bitmap_finalized_) {
    // do nothing
  } else if (OB_NOT_NULL(hive_fm_sketch_)) {
    // Calculate NDV from FM sketch and serialize
    if (OB_FAIL(hive_fm_sketch_->estimate_num_distinct_values(final_ndv_))) {
      LOG_WARN("failed to estimate num distinct values from FM sketch", K(ret));
    } else if (OB_FAIL(ObHiveFMSketchUtils::get_serialize_size(
                   hive_fm_sketch_, final_bitmap_size_))) {
      LOG_WARN("failed to get FM sketch serialize size", K(ret));
    } else if (final_bitmap_size_ > 0) {
      final_bitmap_ = static_cast<char *>(allocator_.alloc(final_bitmap_size_));
      if (OB_ISNULL(final_bitmap_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for FM bitmap", K(ret),
                 K(final_bitmap_size_));
      } else {
        int64_t serialize_pos = 0;
        if (OB_FAIL(ObHiveFMSketchUtils::serialize_fm_sketch(
                final_bitmap_, final_bitmap_size_, serialize_pos,
                hive_fm_sketch_))) {
          LOG_WARN("failed to serialize FM sketch", K(ret));
        } else {
          LOG_DEBUG("successfully finalized FM bitmap", K(final_bitmap_size_),
                    K(final_ndv_));
        }
      }
    }
  } else if (OB_NOT_NULL(hive_hll_)) {
    // Calculate NDV from HLL and serialize
    final_ndv_ = hive_hll_->estimate_num_distinct_values();
    final_bitmap_size_ = ObHiveHLLUtils::get_serialize_size(hive_hll_);
    if (final_bitmap_size_ > 0) {
      final_bitmap_ = static_cast<char *>(allocator_.alloc(final_bitmap_size_));
      if (OB_ISNULL(final_bitmap_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for HLL bitmap", K(ret),
                 K(final_bitmap_size_));
      } else {
        int64_t serialize_pos = 0;
        if (OB_FAIL(ObHiveHLLUtils::serialize_hll(
                final_bitmap_, final_bitmap_size_, serialize_pos, hive_hll_))) {
          LOG_WARN("failed to serialize HLL", K(ret));
        } else {
          LOG_DEBUG("successfully finalized HLL bitmap", K(final_bitmap_size_),
                    K(final_ndv_));
        }
      }
    }
  } else if (is_bitmap_set_ && bitmap_ != nullptr && bitmap_size_ > 0) {
    // Use existing bitmap (for non-Hive cases)
    final_bitmap_size_ = bitmap_size_;
    final_bitmap_ = bitmap_;
  }

  if (OB_SUCC(ret)) {
    is_bitmap_finalized_ = final_bitmap_size_ > 0;
  }

  return ret;
}

int64_t ObOptExternalColumnStatBuilder::calculate_size() const {
  int64_t size = sizeof(ObOptExternalColumnStat);
  size += table_name_.length();
  size += database_name_.length();
  size += partition_value_.length();
  size += column_name_.length();
  size += min_value_.get_deep_copy_size();
  size += max_value_.get_deep_copy_size();

  // Use finalized bitmap size if available, otherwise estimate
  if (is_bitmap_finalized_) {
    size += final_bitmap_size_;
  }
  return size;
}

int ObOptExternalColumnStatBuilder::build(
    ObIAllocator &allocator, ObOptExternalColumnStat *&stat) const {
  int ret = OB_SUCCESS;
  int64_t size = calculate_size();
  char *buf = static_cast<char *>(allocator.alloc(size));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for external column stat", K(ret), K(size));
  } else if (!is_basic_info_set_) {
    ret = OB_NOT_INIT;
    LOG_WARN("basic info not set", K(ret));
  } else {
    stat = new (buf) ObOptExternalColumnStat();
    int64_t pos = sizeof(ObOptExternalColumnStat);

    // Set basic information
    stat->set_tenant_id(tenant_id_);
    stat->set_catalog_id(catalog_id_);

    // Deep copy strings using buffer
    if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
            buf, size, pos, database_name_, stat->database_name_))) {
      LOG_WARN("failed to deep copy database name", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, size, pos, table_name_, stat->table_name_))) {
      LOG_WARN("failed to deep copy table name", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, size, pos, partition_value_, stat->partition_value_))) {
      LOG_WARN("failed to deep copy partition value", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, size, pos, column_name_, stat->column_name_))) {
      LOG_WARN("failed to deep copy column name", K(ret));
    } else {
      // Set statistical information using finalized values
      stat->set_num_null(num_null_);
      stat->set_num_not_null(num_not_null_);
      stat->set_num_distinct(final_ndv_);
      stat->set_avg_length(avg_length_);
      stat->set_last_analyzed(last_analyzed_);
      stat->set_collation_type(cs_type_);
      // Deep copy min/max values
      if (OB_FAIL(stat->min_value_.deep_copy(min_value_, buf, size, pos))) {
        LOG_WARN("failed to deep copy min value", K(ret));
      } else if (OB_FAIL(
                     stat->max_value_.deep_copy(max_value_, buf, size, pos))) {
        LOG_WARN("failed to deep copy max value", K(ret));
      }
      if (is_bitmap_finalized_ && final_bitmap_ != nullptr && final_bitmap_size_ > 0) {
        if (OB_UNLIKELY(pos + final_bitmap_size_ > size)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("final bitmap size is too large", K(ret), K(pos), K(final_bitmap_size_), K(size));
        } else {
          MEMCPY(buf + pos, final_bitmap_, final_bitmap_size_);
          stat->set_bitmap(buf + pos, final_bitmap_size_);
          pos += final_bitmap_size_;
        }
      }
    }

    if (OB_FAIL(ret)) {
      stat = nullptr;
    }
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::copy_obj_to_buffer(common::ObObj &dest,
                                                       const common::ObObj &src,
                                                       bool is_min_obj) {
  int ret = OB_SUCCESS;
  int64_t size = src.get_deep_copy_size();
  int64_t pos = 0;
  if (OB_FAIL(ensure_min_max_buffer(is_min_obj, size))) {
    LOG_WARN("failed to ensure min/max buffer", K(ret), K(is_min_obj), K(size));
  } else {
    char *buf = is_min_obj ? min_obj_buf_ : max_obj_buf_;
    int64_t buf_size = is_min_obj ? min_obj_buf_size_ : max_obj_buf_size_;
    if (OB_FAIL(dest.deep_copy(src, buf, buf_size, pos))) {
      LOG_WARN("failed to copy obj to buffer", K(ret), K(dest), K(src), K(is_min_obj));
    }
  }
  return ret;
}

int ObOptExternalColumnStatBuilder::merge_min_max(common::ObObj &cur,
                                                  const common::ObObj &other,
                                                  bool is_cmp_min) {
  int ret = OB_SUCCESS;
  int cmp_result = 0;

  if (cur.is_null() || other.is_null()) {
    if (!other.is_null()) {
      if (OB_FAIL(copy_obj_to_buffer(cur, other, is_cmp_min))) {
        LOG_WARN("failed to copy obj to buffer", K(ret), K(cur), K(other), K(is_cmp_min));
      }
    }
  } else if (OB_FAIL(cur.compare(other, cmp_result))) {
    LOG_WARN("failed to compare min/max value", K(ret), K(cur), K(other));
  } else if (is_cmp_min) {
    if (cmp_result > 0) {
      if (OB_FAIL(copy_obj_to_buffer(cur, other, is_cmp_min))) {
        LOG_WARN("failed to copy obj to buffer", K(ret), K(cur), K(other), K(is_cmp_min));
      }
    }
  } else {
    if (cmp_result < 0) {
      if (OB_FAIL(copy_obj_to_buffer(cur, other, is_cmp_min))) {
        LOG_WARN("failed to copy obj to buffer", K(ret), K(cur), K(other), K(is_cmp_min));
      }
    }
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::alloc_bitmap(int64_t size) {
  int ret = OB_SUCCESS;

  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap size", K(ret), K(size));
  } else {
    // Allocate new bitmap
    bitmap_ = static_cast<char *>(allocator_.alloc(size));
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for bitmap", K(ret), K(size));
    } else {
      MEMSET(bitmap_, 0, size);
      bitmap_size_ = size;
    }
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::ensure_min_max_buffer(
    bool is_min, int64_t required_size) {
  int ret = OB_SUCCESS;
  char **buf = is_min ? &min_obj_buf_ : &max_obj_buf_;
  int64_t *size = is_min ? &min_obj_buf_size_ : &max_obj_buf_size_;

  // Set minimum allocation size to 1K for the first allocation
  const int64_t min_alloc_size = 1024; // 1K
  int64_t alloc_size = std::max(required_size, min_alloc_size);

  // Check if current buffer is sufficient
  if (OB_NOT_NULL(*buf) && *size >= required_size) {
    // Current buffer is sufficient, no need to reallocate
  } else {
    // Need to allocate or reallocate buffer
    if (OB_NOT_NULL(*buf)) {
      allocator_.free(*buf);
      *buf = nullptr;
      *size = 0;
    }

    // Allocate new buffer with appropriate size
    if (OB_ISNULL(*buf = static_cast<char *>(allocator_.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for min/max obj buf", K(ret),
               K(alloc_size), K(required_size), K(is_min));
    } else {
      *size = alloc_size;
    }
  }

  return ret;
}



int ObOptExternalColumnStatBuilder::merge_other_bitmap(const char *bitmap,
                                                       int64_t bitmap_size) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bitmap) || bitmap_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid other bitmap parameters", K(ret), KP(bitmap),
             K(bitmap_size));
  } else if (!is_bitmap_set_ || bitmap_ == nullptr) {
    // First non-Hive bitmap, just copy it (no type detection needed)
    if (OB_FAIL(set_bitmap(bitmap, bitmap_size))) {
      LOG_WARN("failed to set other bitmap", K(ret));
    }
  } else {
    // For non-Hive bitmaps, we currently don't support merging
    // This could be extended in the future for other bitmap types
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("merging of non-hive bitmaps not supported yet", K(ret),
             K(bitmap_type_), K(bitmap_size));
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::set_bitmap_type(
    ObExternalBitmapType bitmap_type) {
  int ret = OB_SUCCESS;
  bitmap_type_ = bitmap_type;
  LOG_DEBUG("set bitmap type", K(bitmap_type), "type_name",
            (bitmap_type == ObExternalBitmapType::HIVE_AUTO_DETECT)
                ? "HIVE_AUTO_DETECT"
            : (bitmap_type == ObExternalBitmapType::HIVE_FM)  ? "HIVE_FM"
            : (bitmap_type == ObExternalBitmapType::HIVE_HLL) ? "HIVE_HLL"
            : (bitmap_type == ObExternalBitmapType::OTHER)    ? "OTHER"
                                                              : "UNKNOWN");
  return ret;
}

ObExternalBitmapType
ObOptExternalColumnStatBuilder::detect_hive_bitmap_type(const char *bitmap,
                                                        int64_t bitmap_size) {
  ObExternalBitmapType bitmap_type = ObExternalBitmapType::UNKNOWN;

  if (OB_ISNULL(bitmap) || bitmap_size <= 0) {
    bitmap_type = ObExternalBitmapType::UNKNOWN;
  } else if (bitmap_size >= ObHiveHLLUtils::HEADER_SIZE &&
             memcmp(bitmap, ObHiveHLLUtils::MAGIC,
                    ObHiveHLLUtils::MAGIC_SIZE) == 0) {
    // Check for HLL magic string
    bitmap_type = ObExternalBitmapType::HIVE_HLL;
  } else if (bitmap_size >= 2 &&
             memcmp(bitmap, ObHiveFMSketchUtils::MAGIC, 2) == 0) {
    // Check for FM magic string
    bitmap_type = ObExternalBitmapType::HIVE_FM;
  } else {
    bitmap_type = ObExternalBitmapType::UNKNOWN;
  }

  return bitmap_type;
}

int ObOptExternalColumnStatBuilder::merge_hive_fm_bitmap(const char *bitmap,
                                                         int64_t bitmap_size) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bitmap) || bitmap_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Hive FM bitmap parameters", K(ret), KP(bitmap),
             K(bitmap_size));
  } else if (OB_ISNULL(hive_fm_sketch_)) {
    // First FM bitmap - create and deserialize the sketch object
    hive_fm_sketch_ = OB_NEWx(ObHiveFMSketch, &allocator_, allocator_);
    if (OB_ISNULL(hive_fm_sketch_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate hive fm sketch", K(ret));
    } else {
      int64_t pos = 0;
      if (OB_FAIL(ObHiveFMSketchUtils::deserialize_fm_sketch(
              bitmap, bitmap_size, pos, *hive_fm_sketch_))) {
        LOG_WARN("failed to deserialize first FM bitmap", K(ret),
                 K(bitmap_size));
      } else {
        is_bitmap_set_ = true;
        LOG_DEBUG("successfully set first Hive FM bitmap", K(bitmap_size));
      }
    }
  } else {
    // Subsequent FM bitmaps - deserialize and merge with existing sketch
    ObHiveFMSketch incoming_sketch(allocator_);
    int64_t pos = 0;
    if (OB_FAIL(ObHiveFMSketchUtils::deserialize_fm_sketch(
            bitmap, bitmap_size, pos, incoming_sketch))) {
      LOG_WARN("failed to deserialize incoming FM bitmap", K(ret),
               K(bitmap_size));
    } else if (OB_FAIL(hive_fm_sketch_->merge_estimators(&incoming_sketch))) {
      LOG_WARN("failed to merge FM sketches", K(ret));
    } else {
      LOG_DEBUG("successfully merged Hive FM bitmap", K(bitmap_size));
    }
  }

  return ret;
}

int ObOptExternalColumnStatBuilder::merge_hive_hll_bitmap(const char *bitmap,
                                                          int64_t bitmap_size) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bitmap) || bitmap_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Hive HLL bitmap parameters", K(ret), KP(bitmap),
             K(bitmap_size));
  } else if (OB_ISNULL(hive_hll_)) {
    // First HLL bitmap - deserialize and store the HLL object
    int64_t pos = 0;
    if (OB_FAIL(ObHiveHLLUtils::deserialize_hll(bitmap, bitmap_size, pos,
                                                allocator_, hive_hll_))) {
      LOG_WARN("failed to deserialize first HLL bitmap", K(ret),
               K(bitmap_size));
    } else {
      is_bitmap_set_ = true;
      LOG_DEBUG("successfully set first Hive HLL bitmap", K(bitmap_size));
    }
  } else {
    // Subsequent HLL bitmaps - deserialize and merge with existing HLL
    ObHiveHLL *incoming_hll = nullptr;
    int64_t pos = 0;
    if (OB_FAIL(ObHiveHLLUtils::deserialize_hll(bitmap, bitmap_size, pos,
                                                allocator_, incoming_hll))) {
      LOG_WARN("failed to deserialize incoming HLL bitmap", K(ret),
               K(bitmap_size));
    } else if (OB_FAIL(hive_hll_->merge(incoming_hll))) {
      LOG_WARN("failed to merge HLL bitmaps", K(ret));
    } else {
      LOG_DEBUG("successfully merged Hive HLL bitmap", K(bitmap_size));
    }

    // Clean up allocated incoming HLL object
    if (OB_NOT_NULL(incoming_hll)) {
      incoming_hll->~ObHiveHLL();
    }
  }

  return ret;
}

} // end of namespace share
} // end of namespace oceanbase