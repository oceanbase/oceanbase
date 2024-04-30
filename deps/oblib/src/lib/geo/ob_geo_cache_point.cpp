/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_cache_point.h"
#include "lib/geo/ob_geo_point_location_visitor.h"
#include "ob_point_location_analyzer.h"

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