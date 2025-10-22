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

#ifndef OCEANBASE_LIB_OB_VECTOR_L2_SIMILARITY_H_
#define OCEANBASE_LIB_OB_VECTOR_L2_SIMILARITY_H_

#include "ob_vector_l2_distance.h"

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObVectorL2Similarity
{
  // vector a and vector b should be normalized before calling l2_similarity_func
  static int l2_similarity_func(const T *a, const T *b, const int64_t len, double &similarity);
  static double get_l2_similarity(double distance);
};

template <typename T>
OB_INLINE double ObVectorL2Similarity<T>::get_l2_similarity(double distance) 
{
  return 1 / (1 + distance);
}

} // common
} // oceanbase
#endif
