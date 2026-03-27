/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_testy.
 */

#ifndef OCEANBASE_SQL_OB_GEO_FUNC_TESTY_H_
#define OCEANBASE_SQL_OB_GEO_FUNC_TESTY_H_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace sql
{

extern int g_test_geo_unary_func_result[3][8];
extern int g_test_geo_binary_func_result[3][8][3][8];

class ObGeoFuncMockY
{
public:
  ObGeoFuncMockY();
  virtual ~ObGeoFuncMockY() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, int &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_GEO_FUNC_TESTY_H_