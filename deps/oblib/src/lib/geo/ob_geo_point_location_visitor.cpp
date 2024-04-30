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
#include "ob_geo_point_location_visitor.h"
#include "ob_geo_topology_calculate.h"
#include "ob_point_location_analyzer.h"

namespace oceanbase {
namespace common {

bool ObGeoPointLocationVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo)) {
    bret = false;
  }
  return bret;
}

template<typename T>
int ObGeoPointLocationVisitor::calculate_ring_intersects_cnt(T &ext, uint32_t &intersects_cnt, bool &is_on_boundary)
{
  int ret = OB_SUCCESS;
  if (ext.size() <= 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exterior ring", K(ret), K(ext.size()));
  } else {
    typename T::const_iterator iter_befor = ext.begin();
    typename T::const_iterator iter_cur = ext.begin() + 1;
    typename T::const_iterator iter_end = ext.end();
    ObPoint2d begin;
    ObPoint2d end;
    for ( ; iter_cur != iter_end && OB_SUCC(ret) && !is_on_boundary; ++iter_befor, ++iter_cur) {
      begin.x = (iter_befor)->get_x();
      begin.y = (iter_befor)->get_y();
      end.x = (iter_cur)->get_x();
      end.y = (iter_cur)->get_y();
      if (LineIntersect::POINT_INTERSECT == ObGeoTopology::calculate_point_inersect_horizontally(begin, end,
                                                                                                 test_point_, is_on_boundary)) {
        ++intersects_cnt;
      }
    }
  }
  return ret;
}

template <typename T_BIN, typename T_RINGS>
int ObGeoPointLocationVisitor::calculate_point_location_in_polygon(T_BIN *poly)
{
  int ret = OB_SUCCESS;
  ObPointLocation pos = ObPointLocation::INVALID;
  typename T_RINGS::value_type &ext = poly->exterior_ring();
  bool is_on_boundary = false;
  uint32_t intersects_cnt = 0;
  if (OB_FAIL(calculate_ring_intersects_cnt(ext, intersects_cnt, is_on_boundary))) {
    LOG_WARN("failed to get intersects cnt", K(ret), K(ext.size()));
  } else {
    if (is_on_boundary) {
      pos = ObPointLocation::BOUNDARY;
    } else if (intersects_cnt % 2 == 0) {
      pos = ObPointLocation::EXTERIOR;
    } else {
      intersects_cnt = 0;
      T_RINGS &inner_rings = poly->inner_rings();
      typename T_RINGS::iterator iterInnerRing = inner_rings.begin();
      typename T_RINGS::iterator iter_end = inner_rings.end();
      for (; iterInnerRing != iter_end && OB_SUCC(ret) && pos == ObPointLocation::INVALID; ++iterInnerRing) {
        if (OB_FAIL(calculate_ring_intersects_cnt(*iterInnerRing, intersects_cnt, is_on_boundary))) {
          LOG_WARN("failed to get intersects cnt", K(ret), K(iterInnerRing->size()));
        } else if (is_on_boundary) {
          pos = ObPointLocation::BOUNDARY;
        } else if (intersects_cnt % 2 == 1) {
          pos = ObPointLocation::EXTERIOR;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (pos == ObPointLocation::INVALID) {
        // test point is in exterior ring and outside inner rings
        point_location_ = ObPointLocation::INTERIOR;
      } else {
        point_location_ = pos;
      }
    }
  }

  return ret;
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeomPolygon *geo)
{
  return calculate_point_location_in_polygon<ObWkbGeomPolygon, ObWkbGeomPolygonInnerRings>(reinterpret_cast<ObWkbGeomPolygon *>(geo->val()));
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeogPolygon *geo)
{
  return calculate_point_location_in_polygon<ObWkbGeogPolygon, ObWkbGeogPolygonInnerRings>(reinterpret_cast<ObWkbGeogPolygon *>(geo->val()));
}

int ObGeoPointLocationVisitor::visit(ObIWkbPoint *geo)
{
  int ret = OB_SUCCESS;
  ObPointLocation pos = ObPointLocation::INVALID;
  if (geo->x() == test_point_.x && geo->y() == test_point_.y) {
    point_location_ = ObPointLocation::BOUNDARY;
  } else {
    point_location_ = ObPointLocation::EXTERIOR;
  }
  return ret;
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  if (point_location_ == ObPointLocation::INVALID) {
    point_location_ = ObPointLocation::EXTERIOR;
  }
  return OB_SUCCESS;
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  if (point_location_ == ObPointLocation::INVALID) {
    point_location_ = ObPointLocation::EXTERIOR;
  }
  return OB_SUCCESS;
}


template<typename T_IBIN>
int ObGeoPointLocationVisitor::calculate_point_location_in_linestring(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  const typename T_IBIN::value_type *line = reinterpret_cast<const typename T_IBIN::value_type*>(geo->val());
  int geo_size = geo->size();
  if (geo_size > 1) {
    typename T_IBIN::value_type::iterator iter = line->begin();
    typename T_IBIN::value_type::iterator iter_after = line->begin() + 1;
    ObSegment seg;
    for (uint32_t i = 0; i < geo_size - 1 && OB_SUCC(ret) && point_location_ != ObPointLocation::BOUNDARY && point_location_ != ObPointLocation::INTERIOR; ++i, ++iter, ++iter_after) {
      seg.begin.x = iter->template get<0>();
      seg.begin.y = iter->template get<1>();
      seg.end.x = (iter_after)->template get<0>();
      seg.end.y = (iter_after)->template get<1>();
      if (test_point_.x > std::max(seg.begin.x, seg.end.x) || test_point_.y > std::max(seg.begin.y, seg.end.y)
        ||test_point_.x < std::min(seg.begin.x, seg.end.x) || test_point_.y < std::min(seg.begin.y, seg.end.y)) {
        point_location_ = ObPointLocation::EXTERIOR;
      } else if ((seg.begin.x == test_point_.x && seg.begin.y == test_point_.y)
        ||(seg.end.x == test_point_.x && seg.end.y == test_point_.y)) {
        point_location_ = ObPointLocation::BOUNDARY;
      } else if (PointPosition::ON == ObGeoTopology::calculate_point_position(seg.begin, seg.end, test_point_)) {
        point_location_ = ObPointLocation::INTERIOR;
      } else {
        point_location_ = ObPointLocation::EXTERIOR;
      }
    }
  }
  return ret;
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeomLineString *geo)
{
  return calculate_point_location_in_linestring<ObIWkbGeomLineString>(geo);
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeogLineString *geo)
{
  return calculate_point_location_in_linestring<ObIWkbGeogLineString>(geo);
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  if (point_location_ == ObPointLocation::INVALID) {
    point_location_ = ObPointLocation::EXTERIOR;
  }
  return OB_SUCCESS;
}

int ObGeoPointLocationVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  if (point_location_ == ObPointLocation::INVALID) {
    point_location_ = ObPointLocation::EXTERIOR;
  }
  return OB_SUCCESS;
}

} // namespace common
} // namespace oceanbase