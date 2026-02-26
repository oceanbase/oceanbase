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

#ifndef OCEANBASE_SHARE_OB_HIVE_HLL_CONSTANTS_H_
#define OCEANBASE_SHARE_OB_HIVE_HLL_CONSTANTS_H_

#include "lib/ob_define.h"
#include <cmath>

namespace oceanbase {
namespace share {

class ObHiveHLLConstants {
public:
  // Range of register index bits
  static const int32_t MIN_P_VALUE = 4;
  static const int32_t MAX_P_VALUE = 16;

  // Constants for SPARSE encoding
  static const int32_t P_PRIME_VALUE = 25;
  static const int32_t Q_PRIME_VALUE = 6;

  // Data for HLL++ bias correction
  static const int32_t K_NEAREST_NEIGHBOR = 6;

  // Threshold data for bias correction
  static const int32_t THRESHOLD_DATA_SIZE = 15;
  static const double THRESHOLD_DATA[THRESHOLD_DATA_SIZE];

  // Raw estimate data for bias correction (precision 4 to 18)
  static const int32_t RAW_ESTIMATE_DATA_PRECISION_COUNT = 15;
  static const int32_t RAW_ESTIMATE_DATA_MAX_SIZE = 201;
  static const double RAW_ESTIMATE_DATA[RAW_ESTIMATE_DATA_PRECISION_COUNT]
                                       [RAW_ESTIMATE_DATA_MAX_SIZE];

  // Bias data for bias correction (precision 4 to 18)
  static const double BIAS_DATA[RAW_ESTIMATE_DATA_PRECISION_COUNT]
                               [RAW_ESTIMATE_DATA_MAX_SIZE];

  // Inverse power of 2 data for calculations
  static const int32_t INVERSE_POW2_DATA_SIZE = 64;
  static const double INVERSE_POW2_DATA[INVERSE_POW2_DATA_SIZE];

  static double get_inverse_pow2_data(int32_t index) {
    if (index < 0 || index >= INVERSE_POW2_DATA_SIZE) {
      return std::pow(2.0, -index);
    }
    return INVERSE_POW2_DATA[index];
  }

private:
  ObHiveHLLConstants() = delete;
  ~ObHiveHLLConstants() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObHiveHLLConstants);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_HIVE_HLL_CONSTANTS_H_