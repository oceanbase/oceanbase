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

#include "share/stat/hive/ob_hive_fm_sketch.h"
#include "lib/charset/ob_charset.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/stat/hive/ob_hive_fm_sketch_utils.h"
#include <cmath>
#define USING_LOG_PREFIX SQL_ENG

namespace oceanbase {
namespace share {

const double ObHiveFMSketch::PHI = 0.77351;
ObHiveFMSketch::ObHiveFMSketch(common::ObIAllocator &allocator)
    : allocator_(allocator), bit_vectors_(nullptr), a_values_(nullptr),
      b_values_(nullptr), num_bit_vectors_(0), is_inited_(false) {}

ObHiveFMSketch::~ObHiveFMSketch() {
  if (is_inited_) {
    destroy();
  }
  is_inited_ = false;
}

int ObHiveFMSketch::init(int32_t num_bit_vectors) {
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("FMSketch already initialized", K(ret));
  } else if (num_bit_vectors <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid number of bit vectors", K(ret), K(num_bit_vectors));
  } else {
    num_bit_vectors_ = num_bit_vectors;

    // Allocate bit vectors
    void *bit_vectors_buf =
        allocator_.alloc(sizeof(BitVectorType) * num_bit_vectors);
    if (OB_ISNULL(bit_vectors_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for bit vectors", K(ret));
    } else {
      bit_vectors_ = static_cast<BitVectorType *>(bit_vectors_buf);
      // Initialize each bit vector using placement new
      for (int32_t i = 0; OB_SUCC(ret) && i < num_bit_vectors; ++i) {
        new (bit_vectors_ + i) BitVectorType();
        if (!bit_vectors_[i].is_valid()) {
          ret = bit_vectors_[i].get_init_err();
          LOG_WARN("Failed to initialize bit vector", K(ret), K(i));
        }
      }
    }

    // Allocate hash coefficients
    if (OB_SUCC(ret)) {
      void *a_buf = allocator_.alloc(sizeof(int32_t) * num_bit_vectors);
      void *b_buf = allocator_.alloc(sizeof(int32_t) * num_bit_vectors);
      if (OB_ISNULL(a_buf) || OB_ISNULL(b_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for hash coefficients", K(ret));
      } else {
        a_values_ = static_cast<int32_t *>(a_buf);
        b_values_ = static_cast<int32_t *>(b_buf);
        if (OB_FAIL(init_hash_coefficients())) {
          LOG_WARN("Failed to initialize hash coefficients", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObHiveFMSketch::reset() {
  if (is_inited_ && bit_vectors_ != nullptr) {
    for (int32_t i = 0; i < num_bit_vectors_; ++i) {
      bit_vectors_[i].reset();
    }
  }
}

int ObHiveFMSketch::destroy() {
  int ret = OB_SUCCESS;
  if (bit_vectors_ != nullptr) {
    for (int32_t i = 0; i < num_bit_vectors_; ++i) {
      bit_vectors_[i].~BitVectorType();
    }
    bit_vectors_ = nullptr;
  }
  if (a_values_ != nullptr) {
    a_values_ = nullptr;
  }
  if (b_values_ != nullptr) {
    b_values_ = nullptr;
  }
  return ret;
}

int ObHiveFMSketch::add_to_estimator(int64_t v) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bit_vectors_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FMSketch not initialized", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < num_bit_vectors_; ++i) {
      int32_t hash = generate_hash(v, i);
      int32_t index = find_first_one_bit(hash);

      if (OB_FAIL(bit_vectors_[i].add_member(index))) {
        LOG_WARN("Failed to add member to bit vector", K(ret), K(i), K(index));
      }
    }
  }

  return ret;
}

int ObHiveFMSketch::estimate_num_distinct_values(int64_t &num_distinct_values) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bit_vectors_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FMSketch not initialized", K(ret));
  } else {
    int32_t sum_least_sig_zero = 0;
    double avg_least_sig_zero;

    for (int32_t i = 0; i < num_bit_vectors_; ++i) {
      int32_t least_sig_zero = find_next_clear_bit(bit_vectors_[i], 0);
      sum_least_sig_zero += least_sig_zero;
    }

    avg_least_sig_zero =
        sum_least_sig_zero / (num_bit_vectors_ * 1.0) - (log(PHI) / log(2.0));
    num_distinct_values = static_cast<int64_t>(pow(2.0, avg_least_sig_zero));
  }

  return ret;
}

int ObHiveFMSketch::add_to_estimator_pcsa(int64_t v) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bit_vectors_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FMSketch not initialized", K(ret));
  } else {
    int32_t hash = generate_hash_for_pcsa(v);
    int32_t rho = hash / num_bit_vectors_;
    int32_t index = find_first_one_bit(rho);

    if (OB_FAIL(bit_vectors_[hash % num_bit_vectors_].add_member(index))) {
      LOG_WARN("Failed to add member to bit vector for PCSA", K(ret), K(index));
    }
  }

  return ret;
}

int ObHiveFMSketch::estimate_num_distinct_values_pcsa(
    int64_t &num_distinct_values) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bit_vectors_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FMSketch not initialized", K(ret));
  } else {
    int64_t S = 0;
    for (int32_t i = 0; i < num_bit_vectors_; ++i) {
      int32_t index = 0;
      while (bit_vectors_[i].has_member(index) && index < BIT_VECTOR_SIZE) {
        index = index + 1;
      }
      S = S + index;
    }

    num_distinct_values = static_cast<int64_t>((num_bit_vectors_ / PHI) *
                                               pow(2.0, S / num_bit_vectors_));
  }

  return ret;
}

int ObHiveFMSketch::merge_estimators(ObHiveFMSketch *other) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(bit_vectors_) || OB_ISNULL(other) ||
      OB_ISNULL(other->bit_vectors_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Other FMSketch is null or not initialized", K(ret));
  } else if (num_bit_vectors_ != other->num_bit_vectors_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Cannot merge with other FMSketch", K(ret));
  } else {
    // Bitwise OR the bitvector with the bitvector in the other estimator
    for (int32_t i = 0; OB_SUCC(ret) && i < num_bit_vectors_; ++i) {
      if (OB_FAIL(bit_vectors_[i].add_members2(other->bit_vectors_[i]))) {
        LOG_WARN("Failed to merge bit vectors", K(ret), K(i));
      }
    }
  }

  return ret;
}

int32_t ObHiveFMSketch::generate_hash(int64_t v, int32_t hash_num) {
  int32_t mod =
      static_cast<int32_t>(static_cast<uint32_t>((1 << BIT_VECTOR_SIZE)) - 1);
  int64_t temp_hash =
      static_cast<int64_t>(a_values_[hash_num]) * v + b_values_[hash_num];
  temp_hash %= mod;
  int32_t hash = static_cast<int32_t>(temp_hash);

  // Hash function should map the long value to 0...2^L-1.
  // Hence hash value has to be non-negative.
  if (hash < 0) {
    hash = hash + mod;
  }
  return hash;
}

int32_t ObHiveFMSketch::generate_hash_for_pcsa(int64_t v) {
  int32_t mod = (1 << (BIT_VECTOR_SIZE - 1)) - 1;
  int64_t temp_hash = static_cast<int64_t>(a_values_[0]) * v + b_values_[0];
  temp_hash %= mod;
  int32_t hash = static_cast<int32_t>(temp_hash);

  // Hash function should map the long value to 0...2^L-1.
  // Hence hash value has to be non-negative.
  if (hash < 0) {
    hash = hash + mod + 1;
  }
  return hash;
}

int32_t ObHiveFMSketch::find_first_one_bit(int32_t hash) {
  int32_t index;
  // Find the index of the least significant bit that is 1
  for (index = 0; index < BIT_VECTOR_SIZE; ++index) {
    if (hash % 2 != 0) {
      break;
    }
    hash = hash >> 1;
  }
  return index;
}

int ObHiveFMSketch::init_hash_coefficients() {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(a_values_) || OB_ISNULL(b_values_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hash coefficient arrays are null", K(ret));
  } else {
    // Use fixed seeds for reproducible results
    // Java Random uses Linear Congruential Generator
    uint64_t a_seed = 99397;
    uint64_t b_seed = 9876413;

    for (int32_t i = 0; i < num_bit_vectors_; ++i) {
      int32_t rand_val;

      // Generate odd values for a[i]
      do {
        a_seed = (a_seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
        rand_val = static_cast<int32_t>(a_seed >> 16);
      } while (rand_val % 2 == 0);
      a_values_[i] = rand_val;

      // Generate odd values for b[i]
      do {
        b_seed = (b_seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
        rand_val = static_cast<int32_t>(b_seed >> 16);
      } while (rand_val % 2 == 0);
      b_values_[i] = rand_val;

      // Ensure positive values
      if (a_values_[i] < 0) {
        a_values_[i] = a_values_[i] + (1 << (BIT_VECTOR_SIZE - 1));
      }
      if (b_values_[i] < 0) {
        b_values_[i] = b_values_[i] + (1 << (BIT_VECTOR_SIZE - 1));
      }
    }
  }

  return ret;
}

// Helper function to find next clear bit (equivalent to
// FastBitSet.nextClearBit)
int32_t ObHiveFMSketch::find_next_clear_bit(const BitVectorType &bit_vector,
                                            int32_t start_pos) {
  int32_t iret = BIT_VECTOR_SIZE;
  for (int32_t i = start_pos; i < BIT_VECTOR_SIZE; ++i) {
    if (!bit_vector.has_member(i)) {
      iret = i;
      break;
    }
  }
  return iret;
}

int ObHiveFMSketch::serialize(char *buf, const int64_t buf_len,
                              int64_t &pos) const {
  int ret = OB_SUCCESS;

  if (OB_FAIL(
          ObHiveFMSketchUtils::serialize_fm_sketch(buf, buf_len, pos, this))) {
    LOG_WARN("Failed to serialize FMSketch", K(ret));
  }

  return ret;
}

int ObHiveFMSketch::deserialize(const char *buf, const int64_t data_len,
                                int64_t &pos) {
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObHiveFMSketchUtils::deserialize_fm_sketch(buf, data_len, pos,
                                                         *this))) {
    LOG_WARN("Failed to deserialize FMSketch", K(ret));
  }

  return ret;
}

} // namespace share
} // namespace oceanbase