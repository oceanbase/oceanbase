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

#include "share/stat/ob_column_stat.h"
#include <math.h>
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase {
namespace common {

ObColumnStat::ObColumnStat()
    : table_id_(0),
      partition_id_(0),
      column_id_(0),
      version_(0),
      last_rebuild_version_(0),
      num_null_(0),
      num_distinct_(0),
      min_value_(),
      max_value_(),
      llc_bitmap_size_(0),
      llc_bitmap_(NULL),
      object_buf_(NULL),
      is_modified_(false)
{
  min_value_.set_min_value();
  max_value_.set_max_value();
}

ObColumnStat::ObColumnStat(common::ObIAllocator& allocator)
    : table_id_(0),
      partition_id_(0),
      column_id_(0),
      version_(0),
      last_rebuild_version_(0),
      num_null_(0),
      num_distinct_(0),
      min_value_(),
      max_value_(),
      llc_bitmap_size_(0),
      llc_bitmap_(NULL),
      object_buf_(NULL),
      is_modified_(false)
{
  min_value_.set_min_value();
  max_value_.set_max_value();
  if (NULL == (llc_bitmap_ = static_cast<char*>(allocator.alloc(NUM_LLC_BUCKET)))) {
    COMMON_LOG(WARN, "allocate memory for llc_bitmap_ failed.");
  } else if (NULL == (object_buf_ = static_cast<char*>(allocator.alloc(MAX_OBJECT_SERIALIZE_SIZE * 2)))) {
    COMMON_LOG(WARN, "allocate memory for object_buf_ failed.");
  } else {
    llc_bitmap_size_ = NUM_LLC_BUCKET;
    MEMSET(llc_bitmap_, 0, llc_bitmap_size_);
    MEMSET(object_buf_, 0, MAX_OBJECT_SERIALIZE_SIZE * 2);
  }
}

ObColumnStat::~ObColumnStat()
{
  // free llc_bitmap_;
  // free object_buf_;
}

void ObColumnStat::reset()
{
  table_id_ = 0;
  partition_id_ = 0;
  column_id_ = 0;
  version_ = 0;
  last_rebuild_version_ = 0;
  num_null_ = 0;
  num_distinct_ = 0;
  min_value_.set_min_value();
  max_value_.set_max_value();
  MEMSET(llc_bitmap_, 0, llc_bitmap_size_);
  MEMSET(object_buf_, 0, MAX_OBJECT_SERIALIZE_SIZE * 2);
  is_modified_ = false;
}

int64_t ObColumnStat::size() const
{
  return get_deep_copy_size();
}

int ObColumnStat::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(size()), K(ret));
  } else {
    ObColumnStat* stat = new (buf) ObColumnStat();
    int64_t pos = sizeof(*this);
    if (OB_FAIL(stat->deep_copy(*this, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy column stat failed.", K(ret));
    } else {
      value = stat;
    }
  }
  return ret;
}

int ObColumnStat::add_value(const common::ObObj& value)
{
  int ret = OB_SUCCESS;

  if (!is_writable()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN,
        "CAN NOT add value to column stat only for read.",
        KP_(llc_bitmap),
        K_(llc_bitmap_size),
        KP_(object_buf),
        K(ret));
  } else {
    if (value.is_null()) {
      is_modified_ = true;
      ++num_null_;
    } else {
      bool need_comp = true;
      if (OB_SUCC(ret)) {
        if (min_value_.is_min_value() || value.compare(min_value_) < 0) {
          if (OB_FAIL(store_min_value(value))) {
            COMMON_LOG(WARN, "store min value object failed.", K(ret));
          } else {
            is_modified_ = true;
            need_comp = false;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (max_value_.is_max_value() || (need_comp && value.compare(max_value_) > 0)) {
          if (OB_FAIL(store_max_value(value))) {
            COMMON_LOG(WARN, "store min value object failed.", K(ret));
          } else {
            is_modified_ = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        calc_llc_value(value);
      }
    }

    // do not accumulate num_row_ which will be set after merge phase complete.
    // do not calculate average_row_size_, same as above.
  }

  return ret;
}

int ObColumnStat::add(const ObColumnStat& other)
{
  int ret = OB_SUCCESS;

  if (!is_writable()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN,
        "CAN NOT add value to column stat only for read.",
        KP_(llc_bitmap),
        K_(llc_bitmap_size),
        KP_(object_buf),
        K(ret));
  } else {
    if (0 != other.num_null_) {
      is_modified_ = true;
      num_null_ += other.num_null_;
    }
    if (OB_SUCC(ret)) {
      if (!other.max_value_.is_max_value() && !max_value_.is_max_value()) {
        if (max_value_.compare(other.max_value_) < 0) {
          if (OB_FAIL(store_max_value(other.max_value_))) {
            COMMON_LOG(WARN, "store max value object failed.", K(ret));
          } else {
            is_modified_ = true;
          }
        }
      } else if (other.max_value_.is_max_value() && max_value_.is_max_value()) {
        // do nothing
      } else {
        if (max_value_.is_max_value()) {
          if (OB_FAIL(store_max_value(other.max_value_))) {
            COMMON_LOG(WARN, "store max value object failed.", K(ret));
          } else {
            is_modified_ = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!other.min_value_.is_min_value() && !min_value_.is_min_value()) {
        if (min_value_.compare(other.min_value_) > 0) {
          if (OB_FAIL(store_min_value(other.min_value_))) {
            COMMON_LOG(WARN, "store min value object failed.", K(ret));
          } else {
            is_modified_ = true;
          }
        }
      } else if (other.min_value_.is_min_value() && min_value_.is_min_value()) {
        // do nothing
      } else {
        if (min_value_.is_min_value()) {
          if (OB_FAIL(store_min_value(other.min_value_))) {
            COMMON_LOG(WARN, "store min value object failed.", K(ret));
          } else {
            is_modified_ = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < NUM_LLC_BUCKET; i++) {
        if (static_cast<uint8_t>(other.llc_bitmap_[i]) > static_cast<uint8_t>(llc_bitmap_[i])) {
          llc_bitmap_[i] = other.llc_bitmap_[i];
          is_modified_ = true;
        }
      }
    }
  }

  return ret;
}

int ObColumnStat::finish()
{
  int ret = OB_SUCCESS;
  if (NULL == llc_bitmap_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "CAN NOT set column stat only for read.", KP_(llc_bitmap), K(ret));
  } else if (is_modified_) {
    //  use HyperLogLog Counting estimate number of distinct values.
    double sum_of_pmax = 0;
    double alpha = select_alpha_value(NUM_LLC_BUCKET);
    int64_t empty_bucket_num = 0;
    for (int64_t i = 0; i < NUM_LLC_BUCKET; ++i) {
      sum_of_pmax += (1 / pow(2, (llc_bitmap_[i])));
      if (llc_bitmap_[i] == 0) {
        ++empty_bucket_num;
      }
    }

    double estimate_ndv = (alpha * NUM_LLC_BUCKET * NUM_LLC_BUCKET) / sum_of_pmax;
    num_distinct_ = static_cast<int64_t>(estimate_ndv);
    // check if estimate result too tiny or large.
    if (estimate_ndv <= 5 * NUM_LLC_BUCKET / 2) {
      if (0 != empty_bucket_num) {
        // use linear count
        num_distinct_ = static_cast<int64_t>(NUM_LLC_BUCKET * log(NUM_LLC_BUCKET / double(empty_bucket_num)));
      }
    }

    if (estimate_ndv > (static_cast<double>(LARGE_NDV_NUMBER) / 30)) {
      num_distinct_ = static_cast<int64_t>((0 - pow(2, 32)) * log(1 - estimate_ndv / LARGE_NDV_NUMBER));
    }
  }
  return ret;
}

int ObColumnStat::deep_copy(const ObColumnStat& src, char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;

  // assign base members no need to handle memory..
  table_id_ = src.table_id_;
  partition_id_ = src.partition_id_;
  column_id_ = src.column_id_;
  version_ = src.version_;
  last_rebuild_version_ = src.last_rebuild_version_;
  num_null_ = src.num_null_;
  num_distinct_ = src.num_distinct_;

  if (!src.is_valid() || NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(src), KP(buf), K(size), K(ret));
  } else if (OB_FAIL(min_value_.deep_copy(src.min_value_, buf, size, pos))) {
    COMMON_LOG(WARN, "deep copy min_value_ failed.", K_(src.min_value), K(ret));
  } else if (OB_FAIL(max_value_.deep_copy(src.max_value_, buf, size, pos))) {
    COMMON_LOG(WARN, "deep copy max_value_ failed.", K_(src.max_value), K(ret));
  } else if (pos + src.llc_bitmap_size_ > size) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "llc_bitmap_size_ overflow.", K_(src.llc_bitmap_size), K(ret));
  } else {
    // copy llc bitmap.
    llc_bitmap_ = buf + pos;
    llc_bitmap_size_ = src.llc_bitmap_size_;
    MEMCPY(llc_bitmap_, src.llc_bitmap_, src.llc_bitmap_size_);
    pos += llc_bitmap_size_;
  }

  return ret;
}

int64_t ObColumnStat::get_deep_copy_size() const
{
  int64_t base_size = sizeof(ObColumnStat);
  base_size += min_value_.get_deep_copy_size();
  base_size += max_value_.get_deep_copy_size();
  base_size += llc_bitmap_size_;
  return base_size;
}

int ObColumnStat::store_llc_bitmap(const char* bitmap, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == llc_bitmap_ || NULL == bitmap || size > llc_bitmap_size_) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP_(llc_bitmap), K_(llc_bitmap_size), KP(bitmap), K(size), K(ret));
  } else {
    MEMCPY(llc_bitmap_, bitmap, size);
    llc_bitmap_size_ = size;
  }
  return ret;
}

int ObColumnStat::store_max_value(const common::ObObj& max)
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

int ObColumnStat::store_min_value(const common::ObObj& min)
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

void ObColumnStat::calc_llc_value(const common::ObObj& value)
{
  uint64_t h = value.hash(0);
  //  Mask out the k least significant bits as bucket NO
  uint64_t hash_bucket = h;
  uint64_t total_bucket_bits = TOTAL_BUCKET_BITS;
  uint64_t bucket = 0;
  while (total_bucket_bits > 0) {
    bucket ^= (hash_bucket & (NUM_LLC_BUCKET - 1));
    hash_bucket >>= BUCKET_BITS;
    total_bucket_bits -= BUCKET_BITS;
  }

  const uint64_t pmax = trailing_zeroes(h >> BUCKET_BITS);  // pmax <= 64;
  if (pmax > static_cast<uint8_t>(llc_bitmap_[bucket])) {
    llc_bitmap_[bucket] = static_cast<uint8_t>(pmax);
    is_modified_ = true;
  }
}

uint64_t ObColumnStat::trailing_zeroes(const uint64_t num)
{
  uint64_t pos = 0;
  if (0 == num) {
    pos = HASH_VALUE_MAX_BITS;
  } else {
    while (((num >> pos) & 0x1) == 0) {
      pos += 1;
    }
  }
  return pos + 1;
}

double ObColumnStat::select_alpha_value(const int64_t num_bucket)
{
  double ret = 0.0;
  switch (num_bucket) {
    case 16:
      ret = 0.673;
      break;
    case 32:
      ret = 0.697;
      break;
    case 64:
      ret = 0.709;
      break;
    default:
      ret = 0.7213 / (1 + 1.079 / double(num_bucket));
      break;
  }
  return ret;
}

}  // end of namespace common
}  // end of namespace oceanbase
