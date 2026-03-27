/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_area.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_AREA_
#define OCEANBASE_LIB_OB_GEO_FUNC_AREA_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncArea
{
public:
  ObGeoFuncArea();
  ~ObGeoFuncArea();
  static int eval(const common::ObGeoEvalCtx &gis_context, double &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_AREA_