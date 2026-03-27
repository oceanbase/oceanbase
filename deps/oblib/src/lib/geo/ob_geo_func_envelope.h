/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_GEO_OB_GEO_FUNC_ENVELOPE_
#define OCEANBASE_LIB_GEO_OB_GEO_FUNC_ENVELOPE_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncEnvelope
{
public:
  ObGeoFuncEnvelope();
  ~ObGeoFuncEnvelope();
  static int eval(const common::ObGeoEvalCtx &gis_context, ObCartesianBox &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_GEO_OB_GEO_FUNC_ENVELOPE_