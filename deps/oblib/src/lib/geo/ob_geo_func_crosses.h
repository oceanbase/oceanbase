/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_CROSSES_
#define OCEANBASE_LIB_OB_GEO_FUNC_CROSSES_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{
class ObGeoFuncCrosses
{
public:
  ObGeoFuncCrosses();
  virtual ~ObGeoFuncCrosses() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, ObGeoFuncResWithNull &result);
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_LIB_OB_GEO_FUNC_CROSSES_