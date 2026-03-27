/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_overlaps.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_OVERLAPS_
#define OCEANBASE_LIB_OB_GEO_FUNC_OVERLAPS_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{
class ObGeoFuncOverlaps
{
public:
  ObGeoFuncOverlaps();
  virtual ~ObGeoFuncOverlaps() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, ObGeoFuncResWithNull &result);
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_LIB_OB_GEO_FUNC_OVERLAPS_