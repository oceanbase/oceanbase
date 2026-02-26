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

#include "share/stat/hive/ob_hive_hll_dense_register.h"
#include "lib/oblog/ob_log.h"
#include "share/stat/hive/ob_hive_hll_constants.h"
#define USING_LOG_PREFIX SQL_ENG

namespace oceanbase {
namespace share {

ObHiveHLLDenseRegister::ObHiveHLLDenseRegister(int32_t p, bool bit_pack,
                                               common::ObIAllocator &allocator)
    : register_(nullptr), max_register_value_(0), p_(p), m_(1 << p),
      bit_pack_(bit_pack), allocator_(allocator) {
  if (!bit_pack_) {
    max_register_value_ =
        0xFF; // Set to max byte value when bit packing is disabled
  }
}

ObHiveHLLDenseRegister::~ObHiveHLLDenseRegister() {}

int ObHiveHLLDenseRegister::init() {
  int ret = OB_SUCCESS;

  if (p_ < 4 || p_ > 16) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid p value", K(ret), K_(p));
  } else {
    register_ = static_cast<int8_t *>(allocator_.alloc(m_ * sizeof(int8_t)));
    if (OB_ISNULL(register_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for register", K(ret), K_(m));
    } else {
      MEMSET(register_, 0, m_ * sizeof(int8_t));
    }
  }

  return ret;
}

int ObHiveHLLDenseRegister::add(uint64_t hashcode, bool &updated) {
  int ret = OB_SUCCESS;
  updated = false;

  // LSB p bits
  const int32_t register_idx = static_cast<int32_t>(hashcode & (m_ - 1));

  // MSB 64 - p bits
  const uint64_t w = hashcode >> p_;

  // longest run of trailing zeroes
  const int32_t lr = __builtin_ctzll(w) + 1;

  ret = set(register_idx, static_cast<int8_t>(lr), updated);

  return ret;
}

int ObHiveHLLDenseRegister::set(int32_t idx, int8_t value, bool &updated) {
  int ret = OB_SUCCESS;
  updated = false;

  if (OB_ISNULL(register_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Register not initialized", K(ret));
  } else if (idx < 0 || idx >= m_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid register index", K(ret), K(idx), K_(m));
  } else if (value > register_[idx]) {
    // Update max register value
    if (value > max_register_value_) {
      max_register_value_ = value;
    }

    // Set register value
    register_[idx] = value;
    updated = true;
  }

  return ret;
}

int ObHiveHLLDenseRegister::merge(ObHiveHLLRegister *hll_register) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(hll_register)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("HLL register is null", K(ret));
  } else {
    ObHiveHLLDenseRegister *hdr =
        dynamic_cast<ObHiveHLLDenseRegister *>(hll_register);
    if (OB_ISNULL(hdr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Specified register is not instance of ObHiveHLLDenseRegister",
               K(ret));
    } else if (OB_ISNULL(register_) || OB_ISNULL(hdr->register_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Register not initialized", K(ret));
    } else if (m_ != hdr->m_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The size of register sets of HyperLogLogs to be merged does "
               "not match",
               K(ret), K_(m), K(hdr->m_));
    } else {
      const int8_t *in_register = hdr->get_register();

      // Compare register values and store the max register value
      for (int32_t i = 0; i < m_; ++i) {
        const int8_t cb = register_[i];
        const int8_t ob = in_register[i];
        register_[i] = (ob > cb) ? ob : cb;
      }

      // Update max register value
      if (hdr->get_max_register_value() > max_register_value_) {
        max_register_value_ = hdr->get_max_register_value();
      }
    }
  }

  return ret;
}

// this is a lossy invert of the function above, which produces a hashcode
// which collides with the current winner of the register (we lose all higher
// bits, but we get all bits useful for lesser p-bit options)

// +-------------|-------------+
// |xxxx100000000|1000000000000|  (lr=9 + idx=1024)
// +-------------|-------------+
//                \/
// +---------------|-----------+
// |xxxx10000000010|00000000000|  (lr=2 + idx=0)
// +---------------|-----------+

// This shows the relevant bits of the original hash value
// and how the conversion is moving bits from the index value
// over to the leading zero computation
int ObHiveHLLDenseRegister::extract_low_bits_to(ObHiveHLLRegister *dest) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dest)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Destination register is null", K(ret));
  } else if (OB_ISNULL(register_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Register not initialized", K(ret));
  } else {
    for (int32_t idx = 0; OB_SUCC(ret) && idx < m_; ++idx) {
      int8_t lr = register_[idx]; // this can be a max of 65, never > 127
      if (lr != 0) {
        bool updated = false;
        uint64_t hash_value = (1ULL << (p_ + lr - 1)) | idx;
        if (OB_FAIL(dest->add(hash_value, updated))) {
          LOG_WARN("Failed to add hash value to destination", K(ret),
                   K(hash_value));
        }
      }
    }
  }

  return ret;
}

int32_t ObHiveHLLDenseRegister::get_p() const { return p_; }

int32_t ObHiveHLLDenseRegister::get_num_zeroes() const {
  int32_t num_zeroes = 0;
  if (OB_NOT_NULL(register_)) {
    for (int32_t i = 0; i < m_; ++i) {
      if (register_[i] == 0) {
        ++num_zeroes;
      }
    }
  }
  return num_zeroes;
}

double ObHiveHLLDenseRegister::get_sum_inverse_pow2() const {
  double sum = 0.0;
  if (OB_NOT_NULL(register_)) {
    for (int32_t i = 0; i < m_; ++i) {
      sum += ObHiveHLLConstants::get_inverse_pow2_data(register_[i]);
    }
  }
  return sum;
}

int ObHiveHLLDenseRegister::set_register(const int8_t *register_data,
                                         int32_t size) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(register_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Register data is null", K(ret));
  } else if (size != m_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Register size mismatch", K(ret), K(size), K_(m));
  } else if (OB_ISNULL(register_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Register not initialized", K(ret));
  } else {
    MEMCPY(register_, register_data, m_ * sizeof(int8_t));

    // Recalculate max register value
    max_register_value_ = 0;
    for (int32_t i = 0; i < m_; ++i) {
      if (register_[i] > max_register_value_) {
        max_register_value_ = register_[i];
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase