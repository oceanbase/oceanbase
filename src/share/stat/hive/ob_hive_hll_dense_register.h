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

#ifndef OCEANBASE_SHARE_OB_HIVE_HLL_DENSE_REGISTER_H_
#define OCEANBASE_SHARE_OB_HIVE_HLL_DENSE_REGISTER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/stat/hive/ob_hive_hll_register.h"

namespace oceanbase {
namespace share {

class ObHiveHLLDenseRegister : public ObHiveHLLRegister {
public:
  ObHiveHLLDenseRegister(int32_t p, bool bit_pack,
                         common::ObIAllocator &allocator);
  virtual ~ObHiveHLLDenseRegister();

  // Interface from ObHiveHLLRegister
  virtual int add(uint64_t hashcode, bool &updated) override;
  virtual int merge(ObHiveHLLRegister *hll_register) override;
  virtual int extract_low_bits_to(ObHiveHLLRegister *dest) override;
  virtual int32_t get_p() const override;

  // Dense register specific methods
  int set(int32_t idx, int8_t value, bool &updated);
  int32_t size() const { return m_; }
  int32_t get_num_zeroes() const;
  int8_t get_max_register_value() const { return max_register_value_; }
  double get_sum_inverse_pow2() const;

  // Access methods
  int8_t *get_register() { return register_; }
  const int8_t *get_register() const { return register_; }
  int set_register(const int8_t *register_data, int32_t size);

  // Initialization method (should be called after construction)
  int init();

  TO_STRING_KV(K_(p), K_(m), K_(max_register_value));

private:
private:
  // 2^p number of bytes for register
  int8_t *register_;

  // max value stored in register is cached to determine the bit width for bit
  // packing
  int8_t max_register_value_;

  // number of register bits
  int32_t p_;

  // m = 2^p
  int32_t m_;

  // whether to use bit packing optimization
  bool bit_pack_;

  // allocator for memory management
  common::ObIAllocator &allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObHiveHLLDenseRegister);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_HIVE_HLL_DENSE_REGISTER_H_