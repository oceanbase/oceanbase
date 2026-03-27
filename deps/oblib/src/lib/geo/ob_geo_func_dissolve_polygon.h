/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_DISSOLVEPOLYGON_
#define OCEANBASE_LIB_OB_GEO_FUNC_DISSOLVEPOLYGON_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncDissolvePolygon
{
public:
  ObGeoFuncDissolvePolygon();
  virtual ~ObGeoFuncDissolvePolygon() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, common::ObGeometry *&result);
};
} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_DISSOLVEPOLYGON_