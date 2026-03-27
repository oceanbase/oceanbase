/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_correct.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_CORRECT_
#define OCEANBASE_LIB_OB_GEO_FUNC_CORRECT_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncCorrect
{
public:
  ObGeoFuncCorrect();
  ~ObGeoFuncCorrect();
  static int eval(const common::ObGeoEvalCtx &gis_context, int &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_CORRECT_