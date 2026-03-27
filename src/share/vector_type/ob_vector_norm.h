/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_VECTOR_NORM_H_
#define OCEANBASE_LIB_OB_VECTOR_NORM_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"

namespace oceanbase
{
namespace common
{
struct ObVectorNorm
{
  static int vector_norm_square_func(const float *a, const int64_t len, double &norm_square);
  static int vector_norm_func(const float *a, const int64_t len, double &norm);

  // normal func
  OB_INLINE static int vector_norm_square_normal(const float *a, const int64_t len, double &norm_square);
  // TODO(@jingshui) add simd func
};
} // common
} // oceanbase
#endif
