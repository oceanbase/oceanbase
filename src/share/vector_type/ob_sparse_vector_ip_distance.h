/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_SPARSE_VECTOR_IP_DISTANCE_H_
#define OCEANBASE_LIB_OB_SPARSE_VECTOR_IP_DISTANCE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"
#include "ob_vector_op_common.h"
#include "lib/udt/ob_map_type.h"

namespace oceanbase
{
namespace common
{
struct ObSparseVectorIpDistance
{
  static int spiv_ip_distance_func(const ObMapType *a, const ObMapType *b, double &distance);
  // OB_INLINE static int spiv_ip_distance_normal(const ObMapType *a, const ObMapType *b, double &distance);
};


}  // namespace common
}  // namespace oceanbase
#endif
