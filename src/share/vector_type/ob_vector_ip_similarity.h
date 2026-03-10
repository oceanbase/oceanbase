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

#ifndef OCEANBASE_LIB_OB_VECTOR_IP_SIMILARITY_H_
#define OCEANBASE_LIB_OB_VECTOR_IP_SIMILARITY_H_

#include "ob_vector_ip_distance.h"

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObVectorIPSimilarity
{
  // vector a and vector b should be normalized before calling ip_similarity_func
  static int ip_similarity_func(const T *a, const T *b, const int64_t len, double &similarity);
  static double get_ip_similarity(double distance);
};

template <typename T>
OB_INLINE double ObVectorIPSimilarity<T>::get_ip_similarity(double distance)
{
  return (1 + distance) / 2;
}

} // common
} // oceanbase
#endif
