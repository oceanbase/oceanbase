/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_ISVALID_
#define OCEANBASE_LIB_OB_GEO_FUNC_ISVALID_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncIsValid
{
public:
  ObGeoFuncIsValid();
  ~ObGeoFuncIsValid();
  static int eval(const common::ObGeoEvalCtx &gis_context, bool &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_ISVALID_