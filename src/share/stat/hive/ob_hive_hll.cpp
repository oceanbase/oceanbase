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

#include "share/stat/hive/ob_hive_hll.h"
#include "lib/oblog/ob_log.h"
#include "share/stat/hive/ob_hive_hll_constants.h"
#include <algorithm>
#include <cmath>
#include <map>

#define USING_LOG_PREFIX SQL_ENG

namespace oceanbase {
namespace share {

// ===== Builder Implementation =====

ObHiveHLL::Builder::Builder()
    : num_register_index_bits_(14),    // Default p=14
      encoding_(EncodingType::SPARSE), // Default SPARSE encoding
      bit_packing_(true),              // Default enable bit packing
      no_bias_(true)                   // Default enable bias correction
{}

ObHiveHLL::Builder &ObHiveHLL::Builder::set_num_register_index_bits(int32_t p) {
  num_register_index_bits_ = p;
  return *this;
}

ObHiveHLL::Builder &ObHiveHLL::Builder::set_encoding(EncodingType encoding) {
  encoding_ = encoding;
  return *this;
}

ObHiveHLL::Builder &ObHiveHLL::Builder::enable_bit_packing(bool enable) {
  bit_packing_ = enable;
  return *this;
}

ObHiveHLL::Builder &ObHiveHLL::Builder::enable_no_bias(bool enable) {
  no_bias_ = enable;
  return *this;
}

ObHiveHLL::Builder &ObHiveHLL::Builder::set_size_optimized() {
  // p=10 = ~1kb per vector or smaller
  num_register_index_bits_ = 10;
  return *this;
}

int ObHiveHLL::Builder::build(common::ObIAllocator &allocator,
                              ObHiveHLL *&hll) {
  int ret = OB_SUCCESS;

  if (num_register_index_bits_ < ObHiveHLLConstants::MIN_P_VALUE ||
      num_register_index_bits_ > ObHiveHLLConstants::MAX_P_VALUE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid p value", K(ret), K(num_register_index_bits_),
             K(ObHiveHLLConstants::MIN_P_VALUE),
             K(ObHiveHLLConstants::MAX_P_VALUE));
  } else {
    void *ptr = allocator.alloc(sizeof(ObHiveHLL));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for ObHiveHLL", K(ret));
    } else {
      hll = new (ptr) ObHiveHLL(num_register_index_bits_, encoding_,
                                bit_packing_, no_bias_, allocator);
      if (OB_FAIL(hll->init())) {
        LOG_WARN("Failed to initialize ObHiveHLL", K(ret));
        hll->~ObHiveHLL();
        hll = nullptr;
      }
    }
  }

  return ret;
}

// ===== ObHiveHLL Implementation =====

ObHiveHLL::ObHiveHLL(int32_t p, EncodingType encoding, bool bit_packing,
                     bool no_bias, common::ObIAllocator &allocator)
    : p_(p), m_(1 << p), alpha_mm_(0.0f), no_bias_(no_bias),
      bit_packing_(bit_packing), encoding_(encoding),
      encoding_switch_threshold_(0), dense_register_(nullptr),
      sparse_register_(nullptr), cached_count_(-1), invalidate_count_(false),
      allocator_(allocator) {
  // Calculate encoding switch threshold
  if (bit_packing_) {
    // The threshold should be less than 12K bytes for p = 14.
    // In sparse mode after serialization, entries are compressed and delta
    // encoded as varints. Worst case size of varints is 5 bytes. Hence, 12K/5
    // ~= 2400 entries.
    encoding_switch_threshold_ = ((m_ * 6) / 8) / 5;
  } else {
    // If bit packing is disabled, all register values take 8 bits.
    // For p=14, 16K/5 = 3200 entries in sparse map can be allowed.
    encoding_switch_threshold_ = m_ / 3;
  }

  // Use the same alpha calculation as in Java implementation
  // For efficiency alpha is multiplied by m^2
  alpha_mm_ = 0.7213f / (1 + 1.079f / m_);
  alpha_mm_ = alpha_mm_ * m_ * m_;
}

ObHiveHLL::~ObHiveHLL() {
  if (OB_NOT_NULL(dense_register_)) {
    dense_register_->~ObHiveHLLDenseRegister();
  }
  if (OB_NOT_NULL(sparse_register_)) {
    sparse_register_->~ObHiveHLLSparseRegister();
  }
}

int ObHiveHLL::init() {
  int ret = OB_SUCCESS;

  if (encoding_ == EncodingType::SPARSE) {
    void *ptr = allocator_.alloc(sizeof(ObHiveHLLSparseRegister));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for sparse register", K(ret));
    } else {
      sparse_register_ = new (ptr) ObHiveHLLSparseRegister();
      if (OB_FAIL(sparse_register_->init(p_, ObHiveHLLConstants::P_PRIME_VALUE,
                                         ObHiveHLLConstants::Q_PRIME_VALUE))) {
        LOG_WARN("Failed to initialize sparse register", K(ret));
        sparse_register_->~ObHiveHLLSparseRegister();
        sparse_register_ = nullptr;
      }
    }
  } else {
    void *ptr = allocator_.alloc(sizeof(ObHiveHLLDenseRegister));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for dense register", K(ret));
    } else {
      dense_register_ =
          new (ptr) ObHiveHLLDenseRegister(p_, bit_packing_, allocator_);
      if (OB_FAIL(dense_register_->init())) {
        LOG_WARN("Failed to initialize dense register", K(ret));
        dense_register_->~ObHiveHLLDenseRegister();
        dense_register_ = nullptr;
      }
    }
  }

  return ret;
}

int ObHiveHLL::add(uint64_t hashcode) {
  int ret = OB_SUCCESS;
  bool updated = false;

  if (encoding_ == EncodingType::SPARSE) {
    if (OB_ISNULL(sparse_register_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Sparse register is null", K(ret));
    } else if (OB_FAIL(sparse_register_->add(hashcode, updated))) {
      LOG_WARN("Failed to add to sparse register", K(ret), K(hashcode));
    } else if (updated) {
      invalidate_count_ = true;
    }

    // Check if we need to switch from SPARSE to DENSE encoding
    if (OB_SUCC(ret) &&
        sparse_register_->is_size_greater_than(encoding_switch_threshold_)) {
      if (OB_FAIL(
              sparse_to_dense_register(sparse_register_, dense_register_))) {
        LOG_WARN("Failed to convert sparse to dense register", K(ret));
      } else {
        encoding_ = EncodingType::DENSE;
        sparse_register_->~ObHiveHLLSparseRegister();
        sparse_register_ = nullptr;
        invalidate_count_ = true;
      }
    }
  } else {
    if (OB_ISNULL(dense_register_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Dense register is null", K(ret));
    } else if (OB_FAIL(dense_register_->add(hashcode, updated))) {
      LOG_WARN("Failed to add to dense register", K(ret), K(hashcode));
    } else if (updated) {
      invalidate_count_ = true;
    }
  }

  return ret;
}

int64_t ObHiveHLL::estimate_num_distinct_values() {
  // FMSketch treats the ndv of all nulls as 1 but hll treats the ndv as 0.
  // In order to get rid of divide by 0 problem, we follow FMSketch
  int64_t cnt = count();
  return cnt > 0 ? cnt : 1;
}

int64_t ObHiveHLL::count() {
  // Compute count only if the register values are updated else return the
  // cached count
  if (invalidate_count_ || cached_count_ < 0) {
    if (encoding_ == EncodingType::SPARSE) {
      // If encoding is still SPARSE use linear counting with increased
      // accuracy (as we use pPrime bits for register index)
      int32_t m_prime = 1 << sparse_register_->get_p_prime();
      cached_count_ =
          linear_count(m_prime, m_prime - sparse_register_->get_size());
    } else {
      // For DENSE encoding, use bias table lookup for HLLNoBias algorithm
      // else fallback to HLLOriginal algorithm
      double sum = dense_register_->get_sum_inverse_pow2();
      int64_t num_zeros = dense_register_->get_num_zeroes();

      // Cardinality estimate from normalized bias corrected harmonic mean
      cached_count_ = static_cast<int64_t>(alpha_mm_ * (1.0 / sum));

      // When bias correction is enabled
      if (no_bias_) {
        if (cached_count_ <= 5 * m_) {
          cached_count_ = cached_count_ - estimate_bias(cached_count_);
        }

        int64_t h = cached_count_;
        if (num_zeros != 0) {
          h = linear_count(m_, num_zeros);
        }

        if (h < get_threshold()) {
          cached_count_ = h;
        }
      } else {
        // HLL algorithm shows stronger bias for values in (2.5 * m) range.
        // To compensate for this short range bias, linear counting is used
        // for values before this short range.
        if (cached_count_ <= static_cast<int64_t>(2.5 * m_)) {
          // For short range use linear counting
          if (num_zeros != 0) {
            cached_count_ = linear_count(m_, num_zeros);
          }
        }
        // Note: Long range bias correction for 32-bit hash is not needed
        // since we use 64-bit hashing by default
      }
    }
    invalidate_count_ = false;
  }

  return cached_count_;
}

int ObHiveHLL::merge(ObHiveHLL *other) {
  int ret = OB_SUCCESS;
  EncodingType other_encoding;
  if (OB_ISNULL(other)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Other HLL is null", K(ret));
  } else if (p_ != other->p_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Cannot merge HLLs with different p values", K(ret), K_(p),
             K(other->p_));
  } else if (OB_FALSE_IT(other_encoding = other->get_encoding())) {
  } else if (encoding_ == EncodingType::SPARSE &&
             other_encoding == EncodingType::SPARSE) {
    // Both are SPARSE
    if (OB_FAIL(sparse_register_->merge(other->get_sparse_register()))) {
      LOG_WARN("Failed to merge sparse registers", K(ret));
    } else {
      // Check if we need to switch to DENSE after merge
      if (sparse_register_->is_size_greater_than(encoding_switch_threshold_)) {
        if (OB_FAIL(
                sparse_to_dense_register(sparse_register_, dense_register_))) {
          LOG_WARN("Failed to convert sparse to dense register after merge",
                   K(ret));
        } else {
          encoding_ = EncodingType::DENSE;
          sparse_register_->~ObHiveHLLSparseRegister();
          sparse_register_ = nullptr;
        }
      }
    }
  } else if (encoding_ == EncodingType::DENSE &&
             other_encoding == EncodingType::DENSE) {
    // Both are DENSE
    if (OB_FAIL(dense_register_->merge(other->get_dense_register()))) {
      LOG_WARN("Failed to merge dense registers", K(ret));
    }
  } else if (encoding_ == EncodingType::SPARSE &&
             other_encoding == EncodingType::DENSE) {
    // This is SPARSE, other is DENSE - convert this to DENSE and merge
    if (OB_FAIL(sparse_to_dense_register(sparse_register_, dense_register_))) {
      LOG_WARN("Failed to convert sparse to dense register", K(ret));
    } else {
      sparse_register_->~ObHiveHLLSparseRegister();
      sparse_register_ = nullptr;
      encoding_ = EncodingType::DENSE;

      if (OB_FAIL(dense_register_->merge(other->get_dense_register()))) {
        LOG_WARN("Failed to merge dense registers", K(ret));
      }
    }
  } else if (encoding_ == EncodingType::DENSE &&
             other_encoding == EncodingType::SPARSE) {
    // This is DENSE, other is SPARSE - convert other to DENSE and merge
    ObHiveHLLDenseRegister *other_dense = nullptr;
    if (OB_FAIL(sparse_to_dense_register(other->get_sparse_register(),
                                         other_dense))) {
      LOG_WARN("Failed to convert other sparse to dense register", K(ret));
    } else if (OB_FAIL(dense_register_->merge(other_dense))) {
      LOG_WARN("Failed to merge dense registers", K(ret));
    }

    if (OB_NOT_NULL(other_dense)) {
      other_dense->~ObHiveHLLDenseRegister();
    }
  }

  if (OB_SUCC(ret)) {
    invalidate_count_ = true;
  }

  return ret;
}

int ObHiveHLL::squash(common::ObIAllocator &allocator, int32_t new_p,
                      ObHiveHLL *&result) {
  int ret = OB_SUCCESS;

  if (new_p > p_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Cannot squash to bigger p", K(ret), K(new_p), K_(p));
  } else if (new_p == p_) {
    result = this;
  } else {
    // Create new HLL with smaller p and DENSE encoding
    Builder builder;
    builder.set_num_register_index_bits(new_p)
        .set_encoding(EncodingType::DENSE)
        .enable_no_bias(no_bias_)
        .enable_bit_packing(bit_packing_);
    if (OB_FAIL(builder.build(allocator, result))) {
      LOG_WARN("Failed to build squashed HLL", K(ret));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to build squashed HLL", K(ret));
    } else {
      // Extract low bits from current register to new register
      if (encoding_ == EncodingType::SPARSE) {
        if (OB_FAIL(sparse_register_->extract_low_bits_to(
                result->get_dense_register()))) {
          LOG_WARN("Failed to extract low bits from sparse register", K(ret));
        }
      } else {
        if (OB_FAIL(dense_register_->extract_low_bits_to(
                result->get_dense_register()))) {
          LOG_WARN("Failed to extract low bits from dense register", K(ret));
        }
      }
    }
  }

  return ret;
}

double ObHiveHLL::get_standard_error() const {
  return 1.04 / std::sqrt(static_cast<double>(m_));
}

void ObHiveHLL::set_count(int64_t count) {
  cached_count_ = count;
  invalidate_count_ = true;
}

int ObHiveHLL::set_dense_register(const int8_t *register_data, int32_t size) {
  int ret = OB_SUCCESS;

  if (encoding_ != EncodingType::DENSE || OB_ISNULL(dense_register_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not in dense mode or dense register is null", K(ret));
  } else if (OB_FAIL(dense_register_->set_register(register_data, size))) {
    LOG_WARN("Failed to set dense register data", K(ret));
  } else {
    invalidate_count_ = true;
  }

  return ret;
}

int ObHiveHLL::sparse_to_dense_register(
    ObHiveHLLSparseRegister *sparse_register,
    ObHiveHLLDenseRegister *&dense_register) {
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  if (OB_ISNULL(sparse_register)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sparse register is null", K(ret));
  } else if (OB_ISNULL(ptr =
                           allocator_.alloc(sizeof(ObHiveHLLDenseRegister)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for dense register", K(ret));
  } else {
    dense_register =
        new (ptr) ObHiveHLLDenseRegister(p_, bit_packing_, allocator_);
    if (OB_FAIL(dense_register->init())) {
      LOG_WARN("Failed to initialize dense register", K(ret));
      dense_register->~ObHiveHLLDenseRegister();
      dense_register = nullptr;
    } else if (OB_FAIL(sparse_register->extract_low_bits_to(dense_register))) {
      LOG_WARN("Failed to extract low bits from sparse to dense", K(ret));
      dense_register->~ObHiveHLLDenseRegister();
      dense_register = nullptr;
    }
  }
  return ret;
}

int64_t ObHiveHLL::estimate_bias(int64_t raw_count) {
  int64_t ret = 0;
  if (p_ < 4 ||
      p_ >= 4 + ObHiveHLLConstants::RAW_ESTIMATE_DATA_PRECISION_COUNT) {
    ret = 0; // No bias correction for out of range p values
  } else {
    const double *raw_estimate_for_p =
        ObHiveHLLConstants::RAW_ESTIMATE_DATA[p_ - 4];
    const double *bias_for_p = ObHiveHLLConstants::BIAS_DATA[p_ - 4];

    // Compute distance and store it in sorted map
    std::map<double, int32_t> est_index_map;
    for (int32_t i = 0; i < ObHiveHLLConstants::RAW_ESTIMATE_DATA_MAX_SIZE;
         ++i) {
      double distance =
          std::pow(static_cast<double>(raw_count) - raw_estimate_for_p[i], 2);
      est_index_map[distance] = i;
    }

    // Take top-k closest neighbors and compute the bias corrected cardinality
    double bias_sum = 0.0;
    int32_t k_neighbors = ObHiveHLLConstants::K_NEAREST_NEIGHBOR;
    for (const auto &entry : est_index_map) {
      bias_sum += bias_for_p[entry.second];
      --k_neighbors;
      if (k_neighbors <= 0) {
        break;
      }
    }
    // 0.5 added for rounding off
    ret = static_cast<int64_t>(
        (bias_sum / ObHiveHLLConstants::K_NEAREST_NEIGHBOR) + 0.5);
  }

  return ret;
}

int64_t ObHiveHLL::linear_count(int32_t m_val, int64_t num_zeros) {
  int64_t ret = 0;
  if (num_zeros <= 0) {
    ret = 0;
  } else {
    ret = static_cast<int64_t>(
        std::round(m_val * std::log(m_val / static_cast<double>(num_zeros))));
  }
  return ret;
}

int64_t ObHiveHLL::get_threshold() const {
  int64_t ret = 0;
  if (p_ < 4 || p_ >= 4 + ObHiveHLLConstants::THRESHOLD_DATA_SIZE) {
    ret = 0;
  } else {
    ret =
        static_cast<int64_t>(ObHiveHLLConstants::THRESHOLD_DATA[p_ - 4] + 0.5);
  }
  return ret;
}

const char *ObHiveHLL::get_encoding_name() const {
  return (encoding_ == EncodingType::SPARSE) ? "SPARSE" : "DENSE";
}

} // namespace share
} // namespace oceanbase