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

#ifndef OCEANBASE_SHARE_OB_HLL_SPARSE_REGISTER_H_
#define OCEANBASE_SHARE_OB_HLL_SPARSE_REGISTER_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/stat/hive/ob_hive_hll_register.h"

namespace oceanbase {
namespace share {

class ObHiveHLLSparseRegister : public ObHiveHLLRegister {
public:
  // Sparse map storing register index -> leading zero count + 1
  typedef common::hash::ObHashMap<int32_t, int8_t> SparseMapType;

public:
  ObHiveHLLSparseRegister();
  virtual ~ObHiveHLLSparseRegister();

  int init(int32_t p, int32_t pp, int32_t qp);
  virtual int add(uint64_t hashcode, bool &updated) override;
  virtual int merge(ObHiveHLLRegister *hll_register) override;
  virtual int extract_low_bits_to(ObHiveHLLRegister *dest) override;
  virtual int32_t get_p() const override;

  bool is_size_greater_than(int32_t s) const;
  int set(int32_t key, int8_t value, bool &updated);
  int32_t get_p_prime() const { return p_prime_; }
  int32_t get_q_prime() const { return q_prime_; }
  int64_t get_size() const { return sparse_map_.size(); }

  // Get access to sparse map for serialization
  const SparseMapType &get_sparse_map() const { return sparse_map_; }

  TO_STRING_KV(K_(p), K_(p_prime), K_(q_prime));

private:
  /**
   * Encode a 64-bit hashcode into a 32-bit int following the HLL sparse format.
   *
   * Input: 64 bit hashcode
   * |---------w-------------| |------------p'----------|
   * 10101101.......1010101010 10101010101 01010101010101
   *                                       |------p-----|
   *
   * Output: 32 bit int
   * |b| |-q'-|  |------------p'----------|
   *  1  010101  01010101010 10101010101010
   *                         |------p-----|
   *
   * The default values of p', q' and b are 25, 6, 1 (total 32 bits)
   * respectively. This function will return an int encoded in the following
   * format:
   *
   * p  - LSB p bits represent the register index
   * p' - LSB p' bits are used for increased accuracy in estimation
   * q' - q' bits after p' are left as such from the hashcode if b = 0 else
   *      q' bits encodes the longest trailing zero runs from in (w-p) input
   * bits b  - 0 if longest trailing zero run is contained within (p'-p) bits 1
   * if longest trailing zero run is computed from (w-p) input bits and its
   * value is stored in q' bits
   */
  int32_t encode_hash(uint64_t hashcode);

private:
  SparseMapType sparse_map_;

  // Number of register bits
  int32_t p_;

  // New number of register bits for higher accuracy
  int32_t p_prime_;

  // Number of bits to store the number of zero runs
  int32_t q_prime_;

  // Masks for quicker extraction of p, p_prime, q_prime values
  int32_t mask_;
  int32_t p_prime_mask_;
  int32_t q_prime_mask_;

  DISALLOW_COPY_AND_ASSIGN(ObHiveHLLSparseRegister);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_HLL_SPARSE_REGISTER_H_