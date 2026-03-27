/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_cache_point.h"

namespace oceanbase
{
namespace common
{

int ObCachedGeoPoint::intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  bool is_intersects = false;
  if (!is_inited() && OB_FAIL(init())) {
    LOG_WARN("cached polygon init failed", K(ret));
  } else if (OB_FAIL(ObCachedGeomBase::check_any_vertexes_in_geo(geo, res))) {
    LOG_WARN("fail to check whether is there any point from cached poly in geo.", K(ret));
  }

  return ret;
}

} // namespace common
} // namespace oceanbase