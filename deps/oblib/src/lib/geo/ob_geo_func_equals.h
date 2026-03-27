/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_EQUALS_
#define OCEANBASE_LIB_OB_GEO_FUNC_EQUALS_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncEquals
{
public:
  ObGeoFuncEquals();
  virtual ~ObGeoFuncEquals() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, bool &result);
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_LIB_OB_GEO_FUNC_EQUALS_