/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_DISTANCE_
#define OCEANBASE_LIB_OB_GEO_FUNC_DISTANCE_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncDistance
{
public:
  ObGeoFuncDistance();
  virtual ~ObGeoFuncDistance() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, double &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_DISTANCE_