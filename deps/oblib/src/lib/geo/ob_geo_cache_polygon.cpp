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
#include "lib/geo/ob_geo_cache_polygon.h"
#include "lib/geo/ob_point_location_analyzer.h"
#include "lib/geo/ob_geo_segment_intersect_analyzer.h"
#include "lib/geo/ob_geo_point_location_visitor.h"
#include "lib/geo/ob_geo_segment_collect_visitor.h"
#include "lib/geo/ob_geo_vertex_collect_visitor.h"

namespace oceanbase
{
namespace common
{

// find rings_rtree in range [start, end)
int ObRingsRtree::get_ring_strat_idx(int p_idx, int &start, int& end)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be inited", K(ret));
  } else if (poly_count_ != ring_count_.size() || p_idx > poly_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong ring count", K(ret), K(poly_count_), K(ring_count_.size()), K(p_idx));
  } else {
    start = 0;
    end = 0;
    int last_size = 0;
    for (int i = 0; i < p_idx && OB_SUCC(ret); ++i) {
      start += last_size;
      end += ring_count_[i];
      last_size = ring_count_[i];
    }
    if (OB_FAIL(ret)) {
    } else if (start >= end || end > rtrees_.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong range", K(ret), K(start), K(end), K(rtrees_.size()));
    }
  }
  return ret;
}

int ObCachedGeoPolygon::init()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    if (OB_FAIL(ObCachedGeomBase::init())) {
      LOG_WARN("cache geom base init failed", K(ret));
    }
  }
  return ret;
}

int ObCachedGeoPolygon::init_point_analyzer()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else if (OB_ISNULL(pAnalyzer_)) {
    segments_.reset();
    ObGeoSegmentCollectVisitor seg_visitor(&segments_);
    if (OB_FAIL(get_cached_geom()->do_visit(seg_visitor))) {
      LOG_WARN("do segment visit failed", K(ret));
    } else {
      ObPointLocationAnalyzer *buf = static_cast<ObPointLocationAnalyzer *>(get_allocator()->alloc(sizeof(ObPointLocationAnalyzer)));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc point location analyzer failed", K(ret));
      } else {
        pAnalyzer_ = new(buf) ObPointLocationAnalyzer(this, seg_rtree_);
      }
    }
  }
  return ret;
}

int ObCachedGeoPolygon::alloc_rtree(ObSegRtree*& rtree_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObSegRtree *buf = static_cast<ObSegRtree *>(get_allocator()->alloc(sizeof(ObSegRtree)));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc segment rtree failed", K(ret));
    } else {
      rtree_ptr = new(buf) ObSegRtree(this);
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IRING, typename T_RING, typename T_INNER_RING>
int ObCachedGeoPolygon::polygon_init_rings_rtree(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  int ring_size = geo->size();
  if (ring_size == 0) {
    // do nothing
  } else if (OB_FAIL(rings_rtree_.ring_count_.push_back(ring_size))) {
    LOG_WARN("fail to record polygon ring count", K(geo->size()), K(ret));
  } else {
    const T_BIN *polygon = reinterpret_cast<const T_BIN*>(geo->val());
    T_IRING ring;
    ObString data(sizeof(T_RING), reinterpret_cast<const char *>(&polygon->exterior_ring()));
    ring.set_data(data);
    int seg_start_idx = rings_rtree_.ring_segments_.size();
    ObGeoSegmentCollectVisitor tmp_seg_visitor(&rings_rtree_.ring_segments_);
    ObSegRtree* tmp_rtree = nullptr;
    int rtree_size_old = rings_rtree_.rtrees_.size();
    // record exterior_ring first
    if (OB_FAIL(ring.do_visit(tmp_seg_visitor))) {
      OB_LOG(WARN,"failed to do geog polygon exterior ring visit", K(ret));
    } else if (OB_FAIL(alloc_rtree(tmp_rtree))) {
      LOG_WARN("alloc segment rtree failed", K(ret));
    } else if (OB_FAIL(tmp_rtree->construct_rtree_index(rings_rtree_.ring_segments_, seg_start_idx))) {
      LOG_WARN("construct rtree index failed", K(ret));
    } else if (OB_FAIL(rings_rtree_.rtrees_.push_back(tmp_rtree))) {
      LOG_WARN("push back rtree index failed", K(ret));
    } else {
      const T_INNER_RING &rings = polygon->inner_rings();
      typename T_INNER_RING::iterator iter = rings.begin();
      for ( ; iter != rings.end() && OB_SUCC(ret); ++iter) {
        data.assign_ptr(reinterpret_cast<const char *>(iter.operator->()), sizeof(T_RING));
        ring.set_data(data);
        tmp_rtree = nullptr;
        seg_start_idx = rings_rtree_.ring_segments_.size();
        if (OB_FAIL(ring.do_visit(tmp_seg_visitor))) {
          OB_LOG(WARN,"failed to do geog polygon inner ring visit", K(ret));
        } else if (OB_FAIL(alloc_rtree(tmp_rtree))) {
          LOG_WARN("alloc segment rtree failed", K(ret));
        } else if (OB_FAIL(tmp_rtree->construct_rtree_index(rings_rtree_.ring_segments_, seg_start_idx))) {
          LOG_WARN("construct rtree index failed", K(ret));
        } else if (OB_FAIL(rings_rtree_.rtrees_.push_back(tmp_rtree))) {
          LOG_WARN("push back rtree index failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (rings_rtree_.rtrees_.size() - rtree_size_old != ring_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check rtree size failed", K(rings_rtree_.rtrees_.size()), K(rtree_size_old), K(ring_size), K(ret));
      }
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IITEM, typename T_ITEM, typename T_IRING, typename T_RING, typename T_INNER_RING>
int ObCachedGeoPolygon::multipolygon_init_rings_rtree()
{
  int ret = OB_SUCCESS;
  bool ret_bool = true;
  const T_IBIN *multi_poly = reinterpret_cast<const T_IBIN*>(origin_geo_);
  uint32_t size = multi_poly->size();
  if (size == 0) {
    // do nothing
  } else {
    const T_BIN *items = reinterpret_cast<const T_BIN*>(origin_geo_->val());
    typename T_BIN::iterator iter = items->begin();
    T_IITEM item;
    for ( ; iter != items->end() && OB_SUCC(ret); ++iter) {
      rings_rtree_.poly_count_++;
      ObString data(sizeof(T_ITEM), reinterpret_cast<char *>(iter.operator->()));
      item.set_data(data);
      ret = polygon_init_rings_rtree<T_IITEM, T_ITEM, T_IRING, T_RING,  T_INNER_RING>(&item);
    }
  }
  return ret;
}

int ObCachedGeoPolygon::init_rings_rtree()
{
  int ret = OB_SUCCESS;
  if (rings_rtree_.inited_) {
  } else if (OB_ISNULL(origin_geo_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else if (origin_geo_->type() == ObGeoType::POLYGON) {
    rings_rtree_.poly_count_ = 1;
    if (origin_geo_->crs() == ObGeoCRS::Cartesian) {
      ret = polygon_init_rings_rtree<ObIWkbGeomPolygon, ObWkbGeomPolygon,
      ObIWkbGeomLinearRing, ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(static_cast<ObIWkbGeomPolygon*>(origin_geo_));
    } else {
      ret = polygon_init_rings_rtree<ObIWkbGeogPolygon, ObWkbGeogPolygon,
      ObIWkbGeogLinearRing, ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(static_cast<ObIWkbGeogPolygon*>(origin_geo_));
    }
  } else {
    if (origin_geo_->crs() == ObGeoCRS::Cartesian) {
      ret = multipolygon_init_rings_rtree<ObIWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObIWkbGeomPolygon, ObWkbGeomPolygon,
      ObIWkbGeomLinearRing, ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>();
    } else {
      ret = multipolygon_init_rings_rtree<ObIWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObIWkbGeogPolygon, ObWkbGeogPolygon,
      ObIWkbGeogLinearRing, ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>();
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("rings rtree fail to init", K(ret));
  } else if (!rings_rtree_.inited_) {
    rings_rtree_.inited_ = true;
  }
  return ret;
}

int ObCachedGeoPolygon::init_line_analyzer()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else if (OB_ISNULL(lAnalyzer_)) {
    ObLineIntersectionAnalyzer *buf = static_cast<ObLineIntersectionAnalyzer *>(get_allocator()->alloc(sizeof(ObLineIntersectionAnalyzer)));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc line segment intersection analyzer failed", K(ret));
    } else {
      lAnalyzer_ = new(buf) ObLineIntersectionAnalyzer(this, rtree_);
      // collect line segments
      ObGeoSegmentCollectVisitor seg_visitor(&line_segments_);
      if (OB_FAIL(get_cached_geom()->do_visit(seg_visitor))) {
        LOG_WARN("do segment visit failed", K(ret));
      }
    }
  } else {
    lAnalyzer_->reset_flag();
  }
  return ret;
}

int ObCachedGeoPolygon::eval_point_intersects(ObGeometry& geo, bool &res)
{
  int ret = OB_SUCCESS;
  input_vertexes_.reset();
  ObGeoVertexCollectVisitor vertex_coll(input_vertexes_);
  bool is_intersects = false;
  if (OB_FAIL(geo.do_visit(vertex_coll))) {
    LOG_WARN("collect points failed", K(ret));
  } else {
    for (uint32_t i = 0; i < input_vertexes_.size() && OB_SUCC(ret) && !is_intersects; ++i) {
    // check each point position to polygon, i is point idx, p_idx is polygon idx
      for (int p_idx = 1; p_idx <= rings_rtree_.poly_count_ && OB_SUCC(ret) && !is_intersects; ++p_idx) {
        ObPointLocation poly_pos = ObPointLocation::INVALID;
        if (OB_FAIL(get_point_position_in_polygon(p_idx, input_vertexes_[i], poly_pos))) {
          LOG_WARN("calculate point position to polygon failed", K(ret), K(i), K(p_idx));
        } else if (poly_pos == ObPointLocation::INTERIOR || poly_pos == ObPointLocation::BOUNDARY) {
          is_intersects = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      res = is_intersects;
    }
  }
  return ret;
}

int ObCachedGeoPolygon::eval_point_contains(ObGeometry& geo, bool &res, bool is_cover)
{
  int ret = OB_SUCCESS;
  input_vertexes_.reset();
  ObGeoVertexCollectVisitor vertex_coll(input_vertexes_);
  res = false;
  bool res_for_each_point = false;
  bool end_check = false;
  if (OB_FAIL(geo.do_visit(vertex_coll))) {
    LOG_WARN("collect points failed", K(ret));
  } else {
    for (uint32_t i = 0; i < input_vertexes_.size() && OB_SUCC(ret) && !end_check; ++i) {
    // check each point position to polygon, i is point idx, p_idx is polygon idx, make sure every point is is within at least one of the polygon
      res_for_each_point = false;
      int p_idx = 1;
      for (; p_idx <= rings_rtree_.poly_count_ && OB_SUCC(ret) && !res_for_each_point; ++p_idx) {
        ObPointLocation poly_pos = ObPointLocation::INVALID;
        if (OB_FAIL(get_point_position_in_polygon(p_idx, input_vertexes_[i], poly_pos))) {
          LOG_WARN("calculate point position to polygon failed", K(ret), K(i), K(p_idx));
        } else if (poly_pos == ObPointLocation::INTERIOR) {
          res_for_each_point = true;
          is_cover = true;
        } else if (poly_pos == ObPointLocation::BOUNDARY) {
          res_for_each_point = true;
        }
      }
      if (p_idx > rings_rtree_.poly_count_ && !res_for_each_point) {
        end_check = true;
        res = false;
      }
    }

    if (OB_SUCC(ret) && !end_check) {
      res = is_cover;
    }
  }
  return ret;
}

int ObCachedGeoPolygon::get_point_position_in_polygon(int p_idx, const ObPoint2d &test_point, ObPointLocation &pos)
{
  int ret = OB_SUCCESS;
  int start = 0;
  int end = 0;
  if (OB_FAIL(rings_rtree_.get_ring_strat_idx(p_idx, start, end))) {
    LOG_WARN("fail to get range", K(ret));
  } else {
    pos = ObPointLocation::INVALID;
    for (int i = start; i < end && OB_SUCC(ret) && pos == ObPointLocation::INVALID; ++i) {
      if (OB_ISNULL(rings_rtree_.rtrees_[i])) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("rtree is null", K(ret), K(i));
      } else {
        ObPointLocationAnalyzer tmp_pAnalyzer(this, *rings_rtree_.rtrees_[i]);
        if (i == start) {
          // exterior ring
          if (OB_FAIL(tmp_pAnalyzer.calculate_point_position(test_point))) {
            LOG_WARN("calculate point position failed", K(ret), K(input_vertexes_[i]));
          } else if (tmp_pAnalyzer.get_position() == ObPointLocation::EXTERIOR) {
            // outside the exterior ring
            pos = ObPointLocation::EXTERIOR;
          } else if (tmp_pAnalyzer.get_position() == ObPointLocation::BOUNDARY) {
            pos = ObPointLocation::BOUNDARY;
          } // if is ObPointLocation::INTERIOR, need to check inner rings
        } else if (OB_FAIL(tmp_pAnalyzer.calculate_point_position(test_point))) {
          LOG_WARN("calculate point position failed", K(ret), K(input_vertexes_[i]));
        } else if (tmp_pAnalyzer.get_position() == ObPointLocation::INTERIOR) {
          // inside a hole => outside the polygon
          pos = ObPointLocation::EXTERIOR;
        } else if (tmp_pAnalyzer.get_position() == ObPointLocation::BOUNDARY) {
          pos = ObPointLocation::BOUNDARY;
        }
      }
    }

    if (OB_SUCC(ret) && pos == ObPointLocation::INVALID) {
      pos = ObPointLocation::INTERIOR;
    }
  }
  return ret;
}

int ObCachedGeoPolygon::inner_eval_intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  ObGeoDimension dim = ObGeoDimension::MAX_DIMENSION;
  // 1. check points(geo) location
  input_vertexes_.reset();
  ObGeoVertexCollectVisitor vertex_coll(input_vertexes_);
  bool is_intersects = false;
  if (OB_FAIL(geo.do_visit(vertex_coll))) {
    LOG_WARN("collect points failed", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::get_geo_dimension(&geo, dim))) {
    LOG_WARN("fail to get geo dimension.", K(ret));
  } else if (OB_ISNULL(pAnalyzer_) && OB_FAIL(init_point_analyzer())) {
    LOG_WARN("fail to init_point_Analyzer", K(ret));
  } else {
    for (uint32_t i = 0; i < input_vertexes_.size() && OB_SUCC(ret) && !is_intersects; ++i) {
      if (OB_FAIL(pAnalyzer_->calculate_point_position(input_vertexes_[i]))) {
        LOG_WARN("calculate point position failed", K(ret), K(input_vertexes_[i]));
      } else {
        is_intersects = pAnalyzer_->get_position() == ObPointLocation::BOUNDARY
                        || pAnalyzer_->get_position() == ObPointLocation::INTERIOR;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (dim == ObGeoDimension::ZERO_DIMENSION || is_intersects) {
    res = is_intersects;
  } else if (OB_ISNULL(lAnalyzer_) && OB_FAIL(init_line_analyzer())) { // dim of geo is 1 or 2
    LOG_WARN("fail to init_line_Analyzer", K(ret));
  } else if (OB_FAIL(lAnalyzer_->segment_intersection_query(&geo))) { // 2. check ling segment intersection
    LOG_WARN("calculate segment intersection failed", K(ret));
  } else if (lAnalyzer_->is_intersects()) {
    res = lAnalyzer_->is_intersects();
  } else if (dim == ObGeoDimension::TWO_DIMENSION) {
    if (OB_FAIL(ObCachedGeomBase::check_any_vertexes_in_geo(geo, res))) {
      LOG_WARN("fail to check whether is there any point from cached poly in geo.", K(ret));
    }
  }

  return ret;
}

int ObCachedGeoPolygon::get_farthest_point_position(ObVertexes& vertexes, ObPointLocation& farthest_position, bool& has_point_internal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pAnalyzer_) && OB_FAIL(init_point_analyzer())) {
    LOG_WARN("fail to init_point_Analyzer", K(ret));
  } else {
    for (uint32_t i = 0; i < vertexes.size() && OB_SUCC(ret) && (farthest_position != ObPointLocation::EXTERIOR); ++i) {
      if (OB_FAIL(pAnalyzer_->calculate_point_position(vertexes[i]))) {
        LOG_WARN("calculate point position failed", K(ret), K(vertexes[i]));
      } else if (pAnalyzer_->get_position() == ObPointLocation::INVALID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong position.", K(i), K(ret));
      } else if (farthest_position == ObPointLocation::INVALID || farthest_position < pAnalyzer_->get_position()) {
        farthest_position = pAnalyzer_->get_position();
      }
      if (!has_point_internal && OB_SUCC(ret) && pAnalyzer_->get_position() == ObPointLocation::INTERIOR) {
        has_point_internal = true;
      }
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IITEM, typename T_ITEM>
bool ObCachedGeoPolygon::multi_polygons_has_inner_rings()
{
  bool ret_bool = true;
  const T_IBIN *multi_poly = reinterpret_cast<const T_IBIN*>(origin_geo_);
  uint32_t size = multi_poly->size();
  if (size == 0) {
    ret_bool = false;
  } else if (size == 1) {
    const T_BIN *items = reinterpret_cast<const T_BIN*>(origin_geo_->val());
    typename T_BIN::iterator iter = items->begin();
    T_IITEM item;
    ObString data(sizeof(T_ITEM), reinterpret_cast<char *>(iter.operator->()));
    item.set_data(data);
    ret_bool = (item.size() > 1);
  }
  return ret_bool;
}

bool ObCachedGeoPolygon::has_inner_rings()
{
  bool ret_bool = true;
  if (origin_geo_->crs() == ObGeoCRS::Cartesian) {
    if (origin_geo_->type() == ObGeoType::POLYGON) {
      ret_bool = ((static_cast<ObIWkbGeomPolygon*>(origin_geo_))->size() <= 1);
    } else {
      ret_bool = multi_polygons_has_inner_rings<ObIWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObIWkbGeomPolygon, ObWkbGeomPolygon>();
    }
  } else {
    if (origin_geo_->type() == ObGeoType::POLYGON) {
      ret_bool = ((static_cast<ObIWkbGeogPolygon*>(origin_geo_))->size() <= 1);
    } else {
      ret_bool = multi_polygons_has_inner_rings<ObIWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObIWkbGeogPolygon, ObWkbGeogPolygon>();
    }
  }
  return ret_bool;
}

// check contains or cover
int ObCachedGeoPolygon::inner_eval_contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res, bool eval_contains)
{
  int ret = OB_SUCCESS;
  // 1. check points(geo) location
  input_vertexes_.reset();
  ObGeoVertexCollectVisitor vertex_coll(input_vertexes_);
  ObPointLocation farthest_position = ObPointLocation::INVALID;
  bool has_point_internal = false;
  ObGeoDimension dim = ObGeoDimension::MAX_DIMENSION;
  if (OB_FAIL(ObGeoTypeUtil::get_geo_dimension(&geo, dim))) {
    LOG_WARN("fail to get geo dimension.", K(ret));
  } else if (OB_FAIL(geo.do_visit(vertex_coll))) {
    LOG_WARN("collect points failed", K(ret));
  } else if (OB_FAIL(get_farthest_point_position(input_vertexes_, farthest_position, has_point_internal))) {
    LOG_WARN("fail to get farthest point position.", K(ret));
  } else if (farthest_position == ObPointLocation::EXTERIOR) {
    res = false;
  } else if (dim == ObGeoDimension::ZERO_DIMENSION) {
    // 2. if points, make sure at least one point is interior
    res = eval_contains ? has_point_internal : true;
  } else if (OB_FAIL(init_line_analyzer())) { // dim of geo is 1 or 2
    LOG_WARN("fail to init_line_Analyzer", K(ret));
  } else if (OB_FALSE_IT(lAnalyzer_->set_intersects_analyzer_type(true))) {
  } else if (OB_FAIL(lAnalyzer_->segment_intersection_query(&geo))) { // 2. check ling segment intersection
    LOG_WARN("calculate segment intersection failed", K(ret));
  } else if ((dim == ObGeoDimension::TWO_DIMENSION || !has_inner_rings())
            && lAnalyzer_->has_external_intersects()) {
    res = false;
  } else if (lAnalyzer_->is_intersects() && !lAnalyzer_->has_internal_intersects()) {
    res = false;
  } else if (lAnalyzer_->is_intersects()) {
    if (eval_contains && OB_FAIL(ObCachedGeomBase::contains(geo, gis_context, res))) {
      LOG_WARN("fail to check contains by base", K(ret));
    } else if (!eval_contains && OB_FAIL(ObCachedGeomBase::cover(geo, gis_context, res))) {
      LOG_WARN("fail to check contains by base", K(ret));
    }
  } else if (dim == ObGeoDimension::TWO_DIMENSION) {
    bool any_point_in = false;
    if (OB_FAIL(ObCachedGeomBase::check_any_vertexes_in_geo(geo, any_point_in))) {
      LOG_WARN("fail to check whether is there any point from cached poly in geo.", K(ret));
    } else {
      res = !any_point_in;
    }
  } else {
    res = true;
  }
  return ret;
}

// check if CachedPolygon catains geo
int ObCachedGeoPolygon::contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ && OB_FAIL(init())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else if (ObGeoTypeUtil::is_point(geo)) {
    if (!rings_rtree_.inited_ && OB_FAIL(init_rings_rtree())) {
      LOG_WARN("fail to init rings rtree", K(ret));
    } else if (OB_FAIL(eval_point_contains(geo, res, false))) {
      LOG_WARN("fail to get point position", K(ret));
    }
  } else if (!check_valid_ && OB_FAIL(check_valid(gis_context))) {
    LOG_WARN("cached polygon fail to check valid", K(ret));
  } else if (is_valid_) {
    if (OB_FAIL(inner_eval_contains(geo, gis_context, res, true))) {
      LOG_WARN("fail to check contains.", K(ret));
    }
  } else if (OB_FAIL(ObCachedGeomBase::contains(geo, gis_context, res))) {
    LOG_WARN("cache geom base check contains", K(ret));
  }
  return ret;
}

// check if CachedPolygon catains geo
int ObCachedGeoPolygon::cover(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ && OB_FAIL(init())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else if (ObGeoTypeUtil::is_point(geo)) {
    if (!rings_rtree_.inited_ && OB_FAIL(init_rings_rtree())) {
      LOG_WARN("fail to init rings rtree", K(ret));
    } else if (OB_FAIL(eval_point_contains(geo, res, true))) {
      LOG_WARN("fail to get point position", K(ret));
    }
  } else if (!check_valid_ && OB_FAIL(check_valid(gis_context))) {
    LOG_WARN("cached polygon fail to check valid", K(ret));
  } else if (is_valid_) {
    if (OB_FAIL(inner_eval_contains(geo, gis_context, res, false))) {
      LOG_WARN("fail to check contains.", K(ret));
    }
  } else if (OB_FAIL(ObCachedGeomBase::cover(geo, gis_context, res))) {
    LOG_WARN("cache geom base check cover", K(ret));
  }
  return ret;
}

int ObCachedGeoPolygon::intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res)
{
  int ret = OB_SUCCESS;
  if (ObGeoTypeUtil::is_point(geo)) {
    if (!rings_rtree_.inited_ && OB_FAIL(init_rings_rtree())) {
      LOG_WARN("fail to init rings rtree", K(ret));
    } else if (OB_FAIL(eval_point_intersects(geo, res))) {
      LOG_WARN("fail to get point position", K(ret));
    }
  } else if (!is_inited_ && OB_FAIL(init())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached polygon must be inited", K(ret));
  } else if (!check_valid_ && OB_FAIL(check_valid(gis_context))) {
    LOG_WARN("cached polygon fail to check valid", K(ret));
  } else if (is_valid_) {
    if (OB_FAIL(inner_eval_intersects(geo, gis_context, res))) {
      LOG_WARN("fail to check contains.", K(ret));
    }
  } else if (OB_FAIL(ObCachedGeomBase::intersects(geo, gis_context, res))) {
    LOG_WARN("cache geom base check contains", K(ret));
  }
  return ret;
}

int ObCachedGeoPolygon::check_valid(ObGeoEvalCtx& gis_context)
{
  int ret = OB_SUCCESS;
  if (!check_valid_) {
    bool invalid_for_cache = false;
    if (OB_FAIL(ObGeoTypeUtil::polygon_check_self_intersections(*(gis_context.get_allocator()), *origin_geo_, srs_, invalid_for_cache))) {
      LOG_WARN("cached polygon fail to check valid", K(ret));
    } else {
      check_valid_ = true;
      is_valid_ = !invalid_for_cache;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase