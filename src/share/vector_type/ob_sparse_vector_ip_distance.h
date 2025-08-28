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

