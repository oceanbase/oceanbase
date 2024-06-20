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
#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_vertex_collect_visitor.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_point_location_visitor.h"

namespace oceanbase
{
namespace common
{

int ObCachedGeomBase::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(origin_geo_) || OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null.", KP(origin_geo_), KP(allocator_), K(ret));
  } else if (!is_inited_) {
    ObGeoVertexCollectVisitor vertex_visitor(vertexes_);
    if (OB_FAIL(origin_geo_->do_visit(vertex_visitor))) {
      LOG_WARN("failed to collect geo vertexes", K(ret));
    } else {
      x_min_ = vertex_visitor.get_x_min();
      x_max_ = vertex_visitor.get_x_max();
      is_inited_ = true;
    }
  }

  return ret;
}

int ObCachedGeomBase::intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Intersects>::geo_func::eval(gis_context, res))) {
    LOG_WARN("eval st intersection failed", K(ret));
  } else if (geo.type() == ObGeoType::POINT
            && origin_geo_->type() == ObGeoType::POINT
            && res == true
            && OB_FAIL(ObGeoTypeUtil::eval_point_box_intersects(gis_context.get_srs(), &geo, origin_geo_, res))) {
    LOG_WARN("eval box intersection failed", K(ret));
  }
  return ret;
}
int ObCachedGeomBase::contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Within>::gis_func::eval(gis_context, res))) {
    LOG_WARN("eval Within functor failed", K(ret));
  }
  return ret;
}
int ObCachedGeomBase::cover(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGeoFunc<ObGeoFuncType::CoveredBy>::geo_func::eval(gis_context, res))) {
    LOG_WARN("eval st coveredBy failed", K(ret));
  }
  return ret;
}
int ObCachedGeomBase::within(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Within>::gis_func::eval(gis_context, res))) {
    LOG_WARN("eval st withIn failed", K(ret));
  }
  return ret;
}

// check whether is there any point from cached poly in geo
int ObCachedGeomBase::check_any_vertexes_in_geo(ObGeometry& geo, bool &res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else {
    int size = get_vertexes().size();
    for (uint32_t i = 0; i < size && OB_SUCC(ret) && !res; i++) {
      ObGeoPointLocationVisitor point_loc_visitor(get_vertexes()[i]);
      if (OB_FAIL(geo.do_visit(point_loc_visitor))) {
        LOG_WARN("failed to do point location visitor", K(ret));
      } else if (point_loc_visitor.get_point_location() == ObPointLocation::INTERIOR
                || point_loc_visitor.get_point_location() == ObPointLocation::BOUNDARY) {
        res = true;
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase