/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_GEO_FUNC_TESTX_H_
#define OCEANBASE_SQL_OB_GEO_FUNC_TESTX_H_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace sql
{

class ObGeoFuncTypeX
{
public:
  ObGeoFuncTypeX();
  virtual ~ObGeoFuncTypeX() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, int &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_GEO_FUNC_TESTX_H_