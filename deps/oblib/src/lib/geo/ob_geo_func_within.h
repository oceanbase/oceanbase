/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_within.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_WITHIN_
#define OCEANBASE_LIB_OB_GEO_FUNC_WITHIN_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncWithin
{
public:
  ObGeoFuncWithin();
  virtual ~ObGeoFuncWithin() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, bool &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_WITHIN_