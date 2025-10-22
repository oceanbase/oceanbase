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

#include "ob_vector_ip_similarity.h"
namespace oceanbase
{
namespace common
{
template <>
int ObVectorIPSimilarity<float>::ip_similarity_func(const float *a, const float *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double distance = 0;
  bool is_norm = true;
  if (OB_FAIL(ObVectorIpDistance<float>::ip_distance_func(a, b, len, distance))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else {
    similarity = get_ip_similarity(distance);
  }
  return ret;
}

template <>
int ObVectorIPSimilarity<uint8_t>::ip_similarity_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &similarity)
{
  int ret = OB_SUCCESS;
  double distance = 0;
  if (OB_FAIL(ObVectorIpDistance<uint8_t>::ip_distance_func(a, b, len, distance))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else {
    similarity = get_ip_similarity(distance);
  }
  return ret;
}

}
}
