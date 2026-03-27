/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_TOUCHES_
#define OCEANBASE_LIB_OB_GEO_FUNC_TOUCHES_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{
class ObGeoFuncTouches
{
public:
  ObGeoFuncTouches();
  virtual ~ObGeoFuncTouches() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, bool &result);
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_LIB_OB_GEO_FUNC_TOUCHES_