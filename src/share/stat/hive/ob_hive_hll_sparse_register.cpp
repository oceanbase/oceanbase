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

#define USING_LOG_PREFIX SQL_ENG

#include "share/stat/hive/ob_hive_hll_sparse_register.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace share {

ObHiveHLLSparseRegister::ObHiveHLLSparseRegister()
    : p_(0), p_prime_(0), q_prime_(0) {
  // do nothing
}

ObHiveHLLSparseRegister::~ObHiveHLLSparseRegister() {
  if (sparse_map_.created()) {
    sparse_map_.destroy();
  }
}

int ObHiveHLLSparseRegister::init(int32_t p, int32_t pp, int32_t qp) {
  int ret = OB_SUCCESS;
  p_ = p;
  p_prime_ = pp;
  q_prime_ = qp;

  // Calculate masks for bit extraction
  mask_ = ((1 << p_prime_) - 1) ^ ((1 << p_) - 1);
  p_prime_mask_ = (1 << p_prime_) - 1;
  q_prime_mask_ = (1 << q_prime_) - 1;

  // Initialize the sparse map
  if (OB_FAIL(sparse_map_.create(1024, "HllSparseReg"))) {
    LOG_WARN("Failed to create sparse map", K(ret));
  }
  return ret;
}

int ObHiveHLLSparseRegister::add(uint64_t hashcode, bool &updated) {
  int ret = OB_SUCCESS;

  int32_t encoded_hash = encode_hash(hashcode);
  int32_t key = encoded_hash & p_prime_mask_;
  int8_t value = static_cast<int8_t>(encoded_hash >> p_prime_);
  int8_t nr = 0;

  // If MSB is set to 1 then next q_prime MSB bits contains the value of
  // number of zeroes.
  // If MSB is set to 0 then number of zeroes is contained within p_prime - p
  // bits.
  if (encoded_hash < 0) {
    nr = static_cast<int8_t>(value & q_prime_mask_);
  } else {
    nr = static_cast<int8_t>(__builtin_ctzll(encoded_hash >> p_) + 1);
  }

  if (OB_FAIL(set(key, nr, updated))) {
    LOG_WARN("Failed to set value", K(ret), K(key), K(nr));
  }
  return ret;
}

int32_t ObHiveHLLSparseRegister::encode_hash(uint64_t hashcode) {
  // x = p' - p
  int32_t x = static_cast<int32_t>(hashcode & mask_);
  if (x == 0) {
    // More bits should be considered for finding q (longest zero runs)
    // Set MSB to 1
    int32_t ntr = __builtin_ctzll(hashcode >> p_) + 1;
    uint64_t new_hash_code = hashcode & p_prime_mask_;
    new_hash_code |= static_cast<uint64_t>(ntr) << p_prime_;
    new_hash_code |= 0x80000000UL;
    return static_cast<int32_t>(new_hash_code);
  } else {
    // q is contained within p' - p
    // Set MSB to 0
    return static_cast<int32_t>(hashcode & 0x7FFFFFFFULL);
  }
}

bool ObHiveHLLSparseRegister::is_size_greater_than(int32_t s) const {
  return sparse_map_.size() > s;
}

int ObHiveHLLSparseRegister::merge(ObHiveHLLRegister *hll_register) {
  int ret = OB_SUCCESS;
  ObHiveHLLSparseRegister *hsr =
      dynamic_cast<ObHiveHLLSparseRegister *>(hll_register);
  if (OB_ISNULL(hsr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid register type for merge", K(hll_register));
  } else {
    bool updated = false;
    // Retain only the largest value for a register index
    SparseMapType::const_iterator iter = hsr->sparse_map_.begin();
    for (; OB_SUCC(ret) && iter != hsr->sparse_map_.end(); ++iter) {
      int32_t key = iter->first;
      int8_t value = iter->second;
      if (OB_FAIL(set(key, value, updated))) {
        LOG_WARN("Failed to set value", K(ret), K(key), K(value));
      }
    }
  }
  return ret;
}

int ObHiveHLLSparseRegister::set(int32_t key, int8_t value, bool &updated) {
  int ret = OB_SUCCESS;
  int8_t contained_value = 0;

  // Check if key exists and get current value
  ret = sparse_map_.get_refactored(key, contained_value);
  if (OB_SUCC(ret)) {
    // Key exists, check if new value is larger
    if (value > contained_value) {
      if (OB_FAIL(sparse_map_.set_refactored(key, value, 1))) { // overwrite
        LOG_WARN("Failed to update sparse map", K(ret), K(key), K(value));
      } else {
        updated = true;
      }
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    // Key doesn't exist, insert new entry
    if (OB_FAIL(sparse_map_.set_refactored(key, value))) {
      LOG_WARN("Failed to insert into sparse map", K(ret), K(key), K(value));
    } else {
      updated = true;
    }
  } else {
    LOG_WARN("Failed to get from sparse map", K(ret), K(key));
  }
  return ret;
}

int ObHiveHLLSparseRegister::extract_low_bits_to(ObHiveHLLRegister *dest) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Destination register is null", K(ret));
  } else {
    bool updated = false;
    SparseMapType::const_iterator iter = sparse_map_.begin();
    for (; OB_SUCC(ret) && iter != sparse_map_.end(); ++iter) {
      int32_t idx = iter->first;
      int8_t lr = iter->second; // this can be a max of 65, never > 127
      if (lr != 0) {
        // Should be a no-op for sparse
        if (OB_FAIL(dest->add((1ULL << (p_ + lr - 1)) | idx, updated))) {
          LOG_WARN("Failed to add to destination register", K(ret), K(idx),
                   K(lr));
        }
      }
    }
  }
  return ret;
}

int32_t ObHiveHLLSparseRegister::get_p() const { return p_; }

} // namespace share
} // namespace oceanbase