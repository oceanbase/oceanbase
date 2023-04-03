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

#ifndef OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_
#define OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObEditDistance
{
public:
  static int cal_edit_distance(const char *a, const char *b, int64_t a_len, int64_t b_len,  int64_t &edit_dist) {
    const int64_t a_count = a_len;
    const int64_t b_count = b_len;
    if (0 == a_count * b_count) {
      edit_dist = a_count + b_count;
    } else {
      int64_t dp[b_count + 1];
      int64_t temp[b_count + 1];
      for (int64_t i = 0; i <= b_count; ++i) {
        dp[i] = i;
      }
      for (int64_t i = 1; i <= a_count; ++i) {
        for (int64_t j = 0; j <= b_count; ++j) {
          temp[j] = dp[j];
        }
        dp[0] = i;
        for (int64_t j = 1; j <= b_count; ++j) {
          if (a[i - 1] == b[j - 1]) {
            dp[j] = temp[j-1];
          } else {
            int64_t temp_min = temp[j] < temp[j - 1] ? temp[j] : temp[j - 1];
            dp[j] = 1 + (temp_min < dp[j - 1] ? temp_min : dp[j - 1]);
          }
        }
      }
      edit_dist = dp[b_count];
    }
    return OB_SUCCESS;
  }
};
} // end namespace common
} // end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_EDIT_DISTANCE_H_