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
 * This file contains implementation for ob_geo_func_centroid.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_CENTROID_
#define OCEANBASE_LIB_OB_GEO_FUNC_CENTROID_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncCentroid
{
public:
  ObGeoFuncCentroid();
  ~ObGeoFuncCentroid();
  static int eval(const common::ObGeoEvalCtx &gis_context, ObGeometry *&result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_CENTROID_