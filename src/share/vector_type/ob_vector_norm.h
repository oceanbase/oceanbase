/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
