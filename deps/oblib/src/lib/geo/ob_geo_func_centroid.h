/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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