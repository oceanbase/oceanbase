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
#include "lib/geo/ob_geo_cache_linestring.h"
#include "lib/geo/ob_point_location_analyzer.h"
#include "lib/geo/ob_geo_point_location_visitor.h"
#include "lib/geo/ob_geo_segment_collect_visitor.h"
#include "lib/geo/ob_geo_vertex_collect_visitor.h"

namespace oceanbase
{
namespace common
{

int ObCachedGeoLinestring::init()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ObGeoSegmentCollectVisitor seg_visitor(&line_segments_);
    if (OB_FAIL(ObCachedGeomBase::init())) {
      LOG_WARN("cache geom base init failed", K(ret));
    } else if (OB_FAIL(get_cached_geom()->do_visit(seg_visitor))) {
      LOG_WARN("do segment visit failed", K(ret));
    } else if (OB_ISNULL(lAnalyzer_)) {
      ObLineIntersectionAnalyzer *buf = static_cast<ObLineIntersectionAnalyzer *>(allocator_->alloc(sizeof(ObLineIntersectionAnalyzer)));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc point location analyzer failed", K(ret));
      } else {
        lAnalyzer_ = new(buf) ObLineIntersectionAnalyzer(this, rtree_);
      }
    }
  }
  return ret;
}

int ObCachedGeoLinestring::intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  ObGeoDimension dim = ObGeoDimension::MAX_DIMENSION;
  bool has_dimension = false;
  if (!is_inited() && OB_FAIL(init())) {
    LOG_WARN("cached polygon init failed", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::get_geo_dimension(&geo, dim))) {
    LOG_WARN("fail to get geo dimension.", K(ret));
  } else if (OB_FAIL(lAnalyzer_->segment_intersection_query(&geo))) { // check if lines intersect
    LOG_WARN("calculate segment intersection failed", K(ret));
  } else if (lAnalyzer_->is_intersects()) {
    res = lAnalyzer_->is_intersects();
  } else if (dim == ObGeoDimension::TWO_DIMENSION) {
    // check if cached line in geo polygon
    for (uint32_t i = 0; i < get_vertexes().size() && OB_SUCC(ret) && !res; i++) {
      ObGeoPointLocationVisitor point_loc_visitor(get_vertexes()[i]);
      if (OB_FAIL(geo.do_visit(point_loc_visitor))) {
        LOG_WARN("failed to do point location visitor", K(ret));
      } else if (point_loc_visitor.get_point_location() == ObPointLocation::INTERIOR
                || point_loc_visitor.get_point_location() == ObPointLocation::BOUNDARY) {
        res = true;
      }
    }
  }
  if (OB_FAIL(ret) || res) {
  } else if (OB_SUCC(ObGeoTypeUtil::has_dimension(geo, ObGeoDimension::ZERO_DIMENSION, has_dimension)) && has_dimension) {
    // check if geo point on cached line
    input_vertexes_.reset();
    ObGeoVertexCollectVisitor vertex_visitor(input_vertexes_);
    if (OB_FAIL(geo.do_visit(vertex_visitor))) {
      LOG_WARN("failed to collect geo vertexes", K(ret));
    } else {
      for (uint32_t i = 0; i < input_vertexes_.size() && OB_SUCC(ret) && !res; i++) {
        ObGeoPointLocationVisitor point_loc_visitor(input_vertexes_[i]);
        if (OB_FAIL(get_cached_geom()->do_visit(point_loc_visitor))) {
          LOG_WARN("failed to do point location visitor", K(ret));
        } else if (point_loc_visitor.get_point_location() == ObPointLocation::INTERIOR
                  || point_loc_visitor.get_point_location() == ObPointLocation::BOUNDARY) {
          res = true;
        }
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase