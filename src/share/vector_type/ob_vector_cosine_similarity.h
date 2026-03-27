/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
