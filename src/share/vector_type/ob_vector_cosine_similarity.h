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

#ifndef OCEANBASE_LIB_OB_VECTOR_COSINE_SIMILARITY_H_
#define OCEANBASE_LIB_OB_VECTOR_COSINE_SIMILARITY_H_

#include "ob_vector_ip_distance.h"

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObVectorCosineSimilarity
{
  static int cosine_similarity_func(const T *a, const T *b, const int64_t len, double &similarity);
  static double get_cosine_similarity(double distance);
};

template <typename T>
OB_INLINE double ObVectorCosineSimilarity<T>::get_cosine_similarity(double distance) 
{
  if (distance > 1.0) {
    distance = 1.0;
  } else if (distance < -1.0) {
    distance = -1.0;
  }
  return (1 + distance) / 2;
}

} // common
} // oceanbase
#endif
