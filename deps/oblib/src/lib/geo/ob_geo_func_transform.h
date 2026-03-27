/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_transform.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_TRANSFORM
#define OCEANBASE_LIB_OB_GEO_FUNC_TRANSFORM

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncTransform
{
public:
  ObGeoFuncTransform();
  ~ObGeoFuncTransform();
  static int eval(const common::ObGeoEvalCtx &gis_context, common::ObGeometry *&result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_TRANSFORM