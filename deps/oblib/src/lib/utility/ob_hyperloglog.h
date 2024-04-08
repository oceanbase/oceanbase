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

#ifndef OCEANBASE_COMMON_OB_HYPERLOGLOG_H_
#define OCEANBASE_COMMON_OB_HYPERLOGLOG_H_

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{

class ObHyperLogLogCalculator
{
public:
  ObHyperLogLogCalculator()
  : alloc_(nullptr), buckets_(nullptr), n_bucket_(0), n_bit_(0), n_count_(0)
  {}

  // 1. firstly init the HyperLogLogCalculator
  // 2. set repeatly, one is for one value, one is for batch values
  // 3. estimate distinct count
  inline int init(ObIAllocator *alloc, int64_t n_bit);
  inline void set(uint64_t hash_val);
  inline void sets(uint64_t *hash_vals, int64_t count);
  inline char *get_buckets() { return buckets_; }
  inline int64_t get_bucket_num() { return n_bucket_; }
  inline uint64_t estimate();
  void reuse()
  {
    memset(buckets_, 0, 1ULL << n_bit_);
    n_count_ = 0;
  }

  void destroy()
  {
    if (OB_NOT_NULL(alloc_) &&  OB_NOT_NULL(buckets_)) {
      alloc_->free(buckets_);
    }
    alloc_ = nullptr;
    buckets_ = nullptr;
    n_bucket_ = 0;
    n_bit_ = 0;
    n_count_ = 0;
  }

  TO_STRING_KV(K_(n_bucket), K_(n_bit), K_(n_count), KP_(alloc), KP_(buckets));

private:
  inline int32_t calc_leading_zero(uint64_t hash_value)
  {
    return std::min(static_cast<int32_t>(MAX_HASH_VALUE_LENGTH - n_bit_), static_cast<int32_t>(__builtin_clzll(hash_value << n_bit_))) + 1;
  }

  inline double hll_alpha_multiple_m_square(const uint64_t m)
  {
    double alpha = 0.0;
    switch (m) {
    case 16:
      alpha = 0.673;
      break;
    case 32:
      alpha = 0.697;
      break;
    case 64:
      alpha = 0.709;
      break;
    default:
      alpha = 0.7213 / (1 + 1.079 / static_cast<double>(m));
      break;
    }
    return  alpha * static_cast<double>(m) * static_cast<double>(m);
  }

private:
  static const int64_t MIN_HLL_BIT = 4;
  static const int64_t MAX_HLL_BIT = 16;
  static const int64_t MAX_HASH_VALUE_LENGTH = 64;
  ObIAllocator *alloc_;
  char *buckets_; // save the count of previous 0 + 1, max value is 256, only left 50 bits
  int64_t n_bucket_;
  int32_t n_bit_;
  int64_t n_count_; // the times of set or sets
};

inline int ObHyperLogLogCalculator::init(ObIAllocator *alloc, int64_t n_bit)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid allocator", K(ret));
  } else if (MIN_HLL_BIT > n_bit || MAX_HLL_BIT < n_bit) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid bit count", K(ret), K(n_bit));
  } else {
    alloc_ = alloc;
    n_bit_ = n_bit;
    n_bucket_ = 1 << n_bit_;
    buckets_ = (char*)alloc_->alloc(n_bucket_ * sizeof(char));
    memset(buckets_, 0, n_bucket_ * sizeof(char));
    if (OB_ISNULL(buckets_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to allocate buckets", K(ret), K(n_bucket_));
    }
  }
  return ret;
}

inline void ObHyperLogLogCalculator::set(uint64_t hash_value)
{
  int ret = OB_SUCCESS;
  int32_t n_leading_zero = calc_leading_zero(hash_value);
  uint64_t bucket_index = hash_value >> (MAX_HASH_VALUE_LENGTH - n_bit_);
  if (n_bucket_ <= bucket_index) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(DEBUG, "unexpected status: hash value is invalid",
      K(n_leading_zero), K(bucket_index), K(n_bucket_), K(hash_value));
  } else if (n_leading_zero > static_cast<uint8_t>(buckets_[bucket_index])) {
    // 理论上pmax不会超过65.
    buckets_[bucket_index] = n_leading_zero;
    COMMON_LOG(DEBUG, "hll add value", K(n_leading_zero), K(bucket_index));
  } else if (n_leading_zero >= MAX_HASH_VALUE_LENGTH) {
    COMMON_LOG(ERROR, "unexpected status: bucket exceed max bucket",
      K(n_leading_zero), K(static_cast<uint8_t>(buckets_[bucket_index])));
  }
  ++n_count_;
}

inline void ObHyperLogLogCalculator::sets(uint64_t *hash_vals, int64_t count)
{
  int ret = OB_SUCCESS;
  int32_t n_leading_zero = 0;
  uint64_t bucket_index = 0;
  for (int64_t i = 0; i < count; i++) {
    int64_t hash_value = hash_vals[i];
    n_leading_zero = calc_leading_zero(hash_value);
    bucket_index = hash_value >> (MAX_HASH_VALUE_LENGTH - n_bit_);
    if (n_bucket_ <= bucket_index) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(DEBUG, "unexpected status: hash value is invalid",
        K(n_leading_zero), K(bucket_index), K(n_bucket_), K(hash_value));
    } else if (n_leading_zero > static_cast<uint8_t>(buckets_[bucket_index])) {
      // 理论上pmax不会超过65.
      buckets_[bucket_index] = n_leading_zero;
      COMMON_LOG(DEBUG, "hll add value", K(n_leading_zero), K(bucket_index));
    } else if (n_leading_zero >= MAX_HASH_VALUE_LENGTH) {
      COMMON_LOG(ERROR, "unexpected status: bucket exceed max bucket",
        K(n_leading_zero), K(static_cast<uint8_t>(buckets_[bucket_index])));
    }
    ++n_count_;
  }
}

// E = alpha(m) *  m^2 * ( sigma( 1 / 2^buckets_[j] ) )^-1
inline uint64_t ObHyperLogLogCalculator::estimate()
{
  int ret = OB_SUCCESS;
  double sum_of_pmax = 0.0;
  uint64_t num_empty_buckets = 0;
  ObString::obstr_size_t n_buckets = 1 << n_bit_;
  // 1. calculate the sum of bucket from 1 to buckets
  for (int64_t i = 0; i < n_buckets; ++i) {
    sum_of_pmax += 1.0 / static_cast<double>((1ULL << (buckets_[i])));
    if (buckets_[i] == 0) {
      ++num_empty_buckets;
    }
  }
  // 2. alpha(m) * m^2 / SIGMA
  double estimate_ndv = 0 < sum_of_pmax ? hll_alpha_multiple_m_square(n_buckets) / sum_of_pmax : 0;
  if (OB_UNLIKELY(estimate_ndv > n_count_)) {
    // COMMON_LOG(WARN, "before estimate ndv value overflows", K(estimate_ndv), K(n_count_));
    estimate_ndv = n_count_;
    // COMMON_LOG(WARN, "after estimate ndv value overflows", K(estimate_ndv), K(n_count_));
  } else {
    if (estimate_ndv <= 2.5 * static_cast<double>(n_buckets)) {
      if (0 != num_empty_buckets) {
        // use linear count
        estimate_ndv = static_cast<double>(n_buckets)
            * log(static_cast<double>(n_buckets) / static_cast<double>(num_empty_buckets));
      }
    }
  }
  return (uint64_t)estimate_ndv;
}

} // end namespace common
} // end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_