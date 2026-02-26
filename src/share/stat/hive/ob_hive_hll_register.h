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

#ifndef OCEANBASE_SHARE_OB_HIVE_HLL_REGISTER_H_
#define OCEANBASE_SHARE_OB_HIVE_HLL_REGISTER_H_

#include "lib/ob_define.h"

namespace oceanbase {
namespace share {

class ObHiveHLLRegister {
public:
  ObHiveHLLRegister() = default;
  virtual ~ObHiveHLLRegister() = default;

  /**
   * Add a hash value to the HLL register
   * @param hashcode the hash value to add
   * @param updated true if the register was updated, false otherwise
   */
  virtual int add(uint64_t hashcode, bool &updated) = 0;

  /**
   * Merge another HLL register into this one
   * @param hll_register the register to merge
   */
  virtual int merge(ObHiveHLLRegister *hll_register) = 0;

  /**
   * Extract low bits and add them to destination register
   * @param dest the destination register
   */
  virtual int extract_low_bits_to(ObHiveHLLRegister *dest) = 0;

  /**
   * Get the number of register bits (p parameter)
   * @return the p parameter value
   */
  virtual int32_t get_p() const = 0;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_HIVE_HLL_REGISTER_H_