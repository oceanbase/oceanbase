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

#ifndef OCEANBASE_SHARE_STAT_OB_FM_SKETCH_H_
#define OCEANBASE_SHARE_STAT_OB_FM_SKETCH_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "sql/resolver/expr/ob_raw_expr.h" // for ObSqlBitSet

namespace oceanbase {
namespace share {

class ObHiveFMSketchUtils;

class ObHiveFMSketch {
public:
  /* We want a,b,x to come from a finite field of size 0 to k, where k is a
   * prime number. 2^p - 1 is prime for p = 31. Hence bitvectorSize has to
   * be 31. Pick k to be 2^p -1. If a,b,x didn't come from a finite field ax1 +
   * b mod k and ax2 + b mod k will not be pair wise independent. As a
   * consequence, the hash values will not distribute uniformly from 0 to 2^p-1
   * thus introducing errors in the estimates.
   */
  static const int32_t BIT_VECTOR_SIZE = 31;

  // Refer to Flajolet-Martin'86 for the value of phi
  static const double PHI;

  typedef sql::ObSqlBitSet<BIT_VECTOR_SIZE> BitVectorType;

public:
  explicit ObHiveFMSketch(common::ObIAllocator &allocator);
  ~ObHiveFMSketch();

  // Initialize with specified number of bit vectors
  int init(int32_t num_bit_vectors);

  // Reset the estimator to its original state
  void reset();

  int destroy();

  // Add a value to the estimator
  int add_to_estimator(int64_t v);

  // Estimate the number of distinct values using Flajolet-Martin algorithm
  int estimate_num_distinct_values(int64_t &num_distinct_values);

  // PCSA (Probabilistic Counting with Stochastic Averaging) variant methods
  int add_to_estimator_pcsa(int64_t v);
  int estimate_num_distinct_values_pcsa(int64_t &num_distinct_values);

  // Merge with another estimator
  int merge_estimators(ObHiveFMSketch *other);

  // Serialization and deserialization
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);

  // Getters
  int32_t get_num_bit_vectors() const { return num_bit_vectors_; }
  int32_t get_bit_vector_size() const { return BIT_VECTOR_SIZE; }
  TO_STRING_KV(K_(num_bit_vectors), K_(is_inited));

private:
  // Generate hash value for standard FM algorithm
  OB_INLINE int32_t generate_hash(int64_t v, int32_t hash_num);

  // Generate hash value for PCSA algorithm
  OB_INLINE int32_t generate_hash_for_pcsa(int64_t v);

  // Find the position of the least significant 1 bit
  OB_INLINE int32_t find_first_one_bit(int32_t hash);

  // Find the next clear bit (equivalent to FastBitSet.nextClearBit)
  static int32_t find_next_clear_bit(const BitVectorType &bit_vector,
                                     int32_t start_pos);

  // Initialize random coefficients a and b
  int init_hash_coefficients();

private:
  common::ObIAllocator &allocator_;
  BitVectorType *bit_vectors_;
  int32_t *a_values_; // Hash function coefficients
  int32_t *b_values_; // Hash function coefficients
  int32_t num_bit_vectors_;
  bool is_inited_;
  friend class ObHiveFMSketchUtils;
  DISALLOW_COPY_AND_ASSIGN(ObHiveFMSketch);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_OB_FM_SKETCH_H_