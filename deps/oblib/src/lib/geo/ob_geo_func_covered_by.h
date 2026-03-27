/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_covered_by.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_COVERED_BY_H_
#define OCEANBASE_LIB_OB_GEO_FUNC_COVERED_BY_H_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncCoveredBy
{
public:
  ObGeoFuncCoveredBy();
  ~ObGeoFuncCoveredBy();
  static int eval(const common::ObGeoEvalCtx &gis_context, bool &resul);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_COVERED_BY_H_