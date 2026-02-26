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

#ifndef OCEANBASE_SHARE_OB_HIVE_HLL_H_
#define OCEANBASE_SHARE_OB_HIVE_HLL_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/stat/hive/ob_hive_hll_dense_register.h"
#include "share/stat/hive/ob_hive_hll_register.h"
#include "share/stat/hive/ob_hive_hll_sparse_register.h"

namespace oceanbase {
namespace share {

/**
 * This is an implementation of the following variants of hyperloglog (HLL)
 * algorithm:
 * - Original HLL algorithm from Flajolet et. al
 * - HLLNoBias - Google's implementation of bias correction based on lookup
 * table
 * - HLL++ - Google's implementation of HLL++ algorithm that uses SPARSE
 * registers
 *
 * The algorithm automatically switches from SPARSE to DENSE encoding when
 * necessary.
 */
class ObHiveHLL {
public:
  enum class EncodingType { SPARSE = 0, DENSE = 1 };

  /**
   * @brief Builder pattern for constructing ObHiveHLL
   */
  class Builder {
  public:
    Builder();
    ~Builder() = default;

    Builder &set_num_register_index_bits(int32_t p);
    Builder &set_encoding(EncodingType encoding);
    Builder &enable_bit_packing(bool enable);
    Builder &enable_no_bias(bool enable);
    Builder &set_size_optimized(); // p=10 for ~1kb per vector

    int build(common::ObIAllocator &allocator, ObHiveHLL *&hll);

  private:
    int32_t num_register_index_bits_;
    EncodingType encoding_;
    bool bit_packing_;
    bool no_bias_;
  };

public:
  ObHiveHLL(int32_t p, EncodingType encoding, bool bit_packing, bool no_bias,
            common::ObIAllocator &allocator);
  ~ObHiveHLL();

  /**
   * @brief Initialize the HLL registers
   */
  int init();

  /**
   * @brief Add a hash value to the HLL
   * @param hashcode 64-bit hash value to add
   * @return OB_SUCCESS on success, error code otherwise
   */
  int add(uint64_t hashcode);

  /**
   * @brief Estimate the number of distinct values
   * @return estimated cardinality (>= 1 to avoid divide by zero)
   */
  int64_t estimate_num_distinct_values();

  /**
   * @brief Get the estimated count
   * @return estimated cardinality
   */
  int64_t count();

  /**
   * @brief Merge another HyperLogLog into this one
   * @param other the HLL to merge
   * @return OB_SUCCESS on success, error code otherwise
   */
  int merge(ObHiveHLL *other);

  /**
   * @brief Reduce accuracy to a smaller p value
   * @param new_p new p value (must be <= current p)
   * @param result output parameter for the reduced HLL
   * @param allocator memory allocator
   * @return OB_SUCCESS on success, error code otherwise
   */
  int squash(common::ObIAllocator &allocator, int32_t new_p,
             ObHiveHLL *&result);

  /**
   * @brief Get the standard error for this HLL
   * @return standard error
   */
  double get_standard_error() const;

  /**
   * @brief Set the cached count (for testing/deserialization)
   * @param count the count to set
   */
  void set_count(int64_t count);

  // Getters
  int32_t get_num_register_index_bits() const { return p_; }
  int32_t get_num_registers() const { return m_; }
  EncodingType get_encoding() const { return encoding_; }
  bool is_bit_packing_enabled() const { return bit_packing_; }
  bool is_no_bias_enabled() const { return no_bias_; }
  int32_t get_encoding_switch_threshold() const {
    return encoding_switch_threshold_;
  }

  // Access to internal registers (for serialization/testing)
  ObHiveHLLDenseRegister *get_dense_register() const { return dense_register_; }
  ObHiveHLLSparseRegister *get_sparse_register() const {
    return sparse_register_;
  }

  /**
   * @brief Set the internal registers from external data
   * Used for deserialization
   */
  int set_dense_register(const int8_t *register_data, int32_t size);
  int set_sparse_register(
      const common::hash::ObHashMap<int32_t, int8_t> &sparse_map);

  TO_STRING_KV("encoding", get_encoding_name(), K_(p));

private:
  /**
   * @brief Convert sparse register to dense register
   * @param sparse_register the sparse register to convert
   * @param dense_register output parameter for the converted register
   * @return OB_SUCCESS on success, error code otherwise
   */
  int sparse_to_dense_register(ObHiveHLLSparseRegister *sparse_register,
                               ObHiveHLLDenseRegister *&dense_register);

  /**
   * @brief Estimate bias using lookup table
   * @param raw_count cardinality before bias correction
   * @return bias-corrected cardinality
   */
  int64_t estimate_bias(int64_t raw_count);

  /**
   * @brief Linear counting formula
   * @param m_val number of registers
   * @param num_zeros number of zero registers
   * @return linear count estimate
   */
  int64_t linear_count(int32_t m_val, int64_t num_zeros);

  /**
   * @brief Get threshold for bias correction
   * @return threshold value
   */
  int64_t get_threshold() const;

  /**
   * @brief Get encoding type name for printing
   * @return encoding name string
   */
  const char *get_encoding_name() const;

private:
  // Number of register index bits (4-16)
  int32_t p_;

  // Number of registers (2^p)
  int32_t m_;

  // Alpha constant multiplied by m^2 for efficiency
  float alpha_mm_;

  // Enable/disable bias correction using table lookup
  bool no_bias_;

  // Enable/disable bit packing
  bool bit_packing_;

  // Current encoding type
  EncodingType encoding_;

  // Threshold to switch from SPARSE to DENSE encoding
  int32_t encoding_switch_threshold_;

  // Dense register (non-null when encoding is DENSE)
  ObHiveHLLDenseRegister *dense_register_;

  // Sparse register (non-null when encoding is SPARSE)
  ObHiveHLLSparseRegister *sparse_register_;

  // Cached count to avoid repeated computation
  int64_t cached_count_;

  // Flag to indicate if cached count needs to be invalidated
  bool invalidate_count_;

  // Memory allocator
  common::ObIAllocator &allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObHiveHLL);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_HIVE_HLL_H_