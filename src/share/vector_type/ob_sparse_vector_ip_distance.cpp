/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_sparse_vector_ip_distance.h"
namespace oceanbase
{
namespace common
{

int ObSparseVectorIpDistance::spiv_ip_distance_func(const ObMapType *a, const ObMapType *b, double &distance)
{
  int ret = OB_SUCCESS;
  double square = 0;
  distance = 0;
  if (OB_ISNULL(a) || OB_ISNULL(b)) {
    ret = OB_ERR_NULL_VALUE;
    LIB_LOG(WARN, "invalid null pointer", K(ret), KP(a), KP(b));
  } else {
    uint32_t len_a = a->cardinality();
    uint32_t len_b = b->cardinality();
    uint32_t *keys_a = reinterpret_cast<uint32_t *>(a->get_key_array()->get_data());
    uint32_t *keys_b = reinterpret_cast<uint32_t *>(b->get_key_array()->get_data());
    float *values_a = reinterpret_cast<float *>(a->get_value_array()->get_data());
    float *values_b = reinterpret_cast<float *>(b->get_value_array()->get_data());
    for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < len_a && j < len_b;) {
      if (keys_a[i] == keys_b[j]) {
        distance += values_a[i] * values_b[j];
        i++;
        j++;
      } else if (keys_a[i] < keys_b[j]) {
        i++;
      } else {
        j++;
      }
      if (OB_UNLIKELY(0 != ::isinf(distance))) {
        ret = OB_NUMERIC_OVERFLOW;
        LIB_LOG(WARN, "value is overflow", K(ret), K(distance));
      }
    }
  }
  return ret;
}

}
}