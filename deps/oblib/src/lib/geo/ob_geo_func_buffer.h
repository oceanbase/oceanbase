/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 * This file contains implementation for ob_geo_func_buffer.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_BUFFER_
#define OCEANBASE_LIB_OB_GEO_FUNC_BUFFER_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

enum class ObGeoBufferStrategyStateType
{
  JR_ER_PC = 0, // join_round, end_round, point_circle
  JR_ER_PS = 1, // join_round, end_round, point_square
  JR_EF_PC = 2, // join_round, end_flat, point_circle
  JR_EF_PS = 3, // join_round, end_flat, point_square
  JM_ER_PC = 4, // join_miter, end_round, point_circle
  JM_ER_PS = 5, // join_miter, end_round, point_square
  JM_EF_PC = 6, // join_miter, end_flat, point_circle
  JM_EF_PS = 7, // join_miter, end_flat, point_square
};

class ObGeoFuncBuffer
{
public:
  ObGeoFuncBuffer();
  ~ObGeoFuncBuffer();
  static int eval(const common::ObGeoEvalCtx &gis_context, common::ObGeometry *&result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_BUFFER