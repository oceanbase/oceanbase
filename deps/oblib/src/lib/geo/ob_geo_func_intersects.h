/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_INTERSECTS_
#define OCEANBASE_LIB_OB_GEO_FUNC_INTERSECTS_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncIntersects
{
public:
  ObGeoFuncIntersects();
  virtual ~ObGeoFuncIntersects() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, bool &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_INTERSECTS_