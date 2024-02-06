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
#include "ob_geo_to_s2_visitor.h"
#include "ob_srs_info.h"

namespace oceanbase {
namespace common {

bool ObWkbToS2Visitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo)) {
    bret = false;
  }
  return bret;
}

void ObWkbToS2Visitor::add_cell_from_point(S2Point point)
{
  S2CellId cell_id = S2CellId(point).parent(options_.max_level());
  S2cells_.push_back(cell_id);
}

void ObWkbToS2Visitor::add_cell_from_point(S2LatLng point)
{
  S2CellId cell_id = S2CellId(point).parent(options_.max_level());
  S2cells_.push_back(cell_id);
}

template<typename T_IBIN>
S2Cell* ObWkbToS2Visitor::MakeS2Point(T_IBIN *geo)
{
  S2LatLng latlng = S2LatLng::FromDegrees(geo->y(), geo->x());
  add_cell_from_point(latlng);
  mbr_ = mbr_.is_empty() ? S2LatLngRect(latlng, latlng) : mbr_.Union(S2LatLngRect(latlng, latlng));
  S2Cell* p = new S2Cell(latlng);
  return p;
}

double ObWkbToS2Visitor::stToUV(double s)
{
  double u = 0.0;
  if (s >= 0.5) {
    u = (1.0 / 3.0) * (4.0 * s * s - 1.0);
  } else {
    u = (1.0 / 3.0) * (1.0 - 4.0 * (1.0 - s) * (1.0 - s));
  }
  return u;
}

bool ObWkbToS2Visitor::exceedsBounds(double x, double y)
{
  double is_exceed = false;
  if (OB_ISNULL(bound_)) {
    is_exceed = true;
  } else {
    double deltaX = OB_GEO_BOUNDS_DELTA * (bound_->maxX_ - bound_->minX_);
    double deltaY = OB_GEO_BOUNDS_DELTA * (bound_->maxY_ - bound_->minY_);
    if (x < bound_->minX_ + deltaX ||
        x > bound_->maxX_ - deltaX) {
      is_exceed = true;
    } else if (y < bound_->minY_ + deltaY ||
               y > bound_->maxY_ - deltaY) {
      is_exceed = true;
    }
  }
  return is_exceed;
}

S2Point ObWkbToS2Visitor::MakeS2PointFromXy(double x, double y)
{
  S2Point ret{-1, -1, -1};
  if (!exceedsBounds(x, y)) {
    double s = (x - bound_->minX_) / (bound_->maxX_ - bound_->minX_);
    double t = (y - bound_->minY_) / (bound_->maxY_ - bound_->minY_);
    double u = stToUV(s);
    double v = stToUV(t);
    ret = {1, u, v};
  } else {
    invalid_ = true;
  }
  return ret;
}

template<typename T_IBIN>
S2Cell* ObWkbToS2Visitor::MakeProjS2Point(T_IBIN *geo)
{
  S2Point point = MakeS2PointFromXy(geo->x(), geo->y());
  S2Cell* p = NULL;
  if (!invalid_) {
    p = new S2Cell(point);
    add_cell_from_point(point);
  }
  return p;
}

template<typename T_IBIN, typename T_BIN>
S2Polyline* ObWkbToS2Visitor::MakeS2Polyline(T_IBIN *geo)
{
  std::vector<S2LatLng> vertices;
  const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
  typename T_BIN::iterator iter = line->begin();
  for ( ; iter != line->end(); iter++) {
    S2LatLng latlng = S2LatLng::FromDegrees(iter->template get<1>(),
                                            iter->template get<0>());
    add_cell_from_point(latlng);
    vertices.push_back(latlng);
  }
  S2Polyline* ptr = new S2Polyline(vertices);
  return ptr;
}

template<typename T_IBIN, typename T_BIN>
S2Polyline* ObWkbToS2Visitor::MakeProjS2Polyline(T_IBIN *geo)
{
  std::vector<S2Point> vertices;
  const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
  typename T_BIN::iterator iter = line->begin();
  for ( ; iter != line->end(); iter++) {
    S2Point p = MakeS2PointFromXy(iter->template get<0>(),
                                  iter->template get<1>());
    add_cell_from_point(p);
    vertices.push_back(p);
  }
  S2Polyline* ptr = new S2Polyline(vertices);
  return ptr;
}

template<typename T_IBIN, typename T_BIN,
         typename T_BIN_RING, typename T_BIN_INNER_RING>
S2Polygon* ObWkbToS2Visitor::MakeS2Polygon(T_IBIN *geo)
{
  T_BIN& poly = *(T_BIN *)(geo->val());
  T_BIN_RING& exterior = poly.exterior_ring();
  T_BIN_INNER_RING& inner_rings = poly.inner_rings();
  std::vector<std::unique_ptr<S2Loop>> s2poly;
  if (poly.size() != 0) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = exterior.begin();
    for (; iter != exterior.end(); ++iter) {
      S2LatLng latlng = S2LatLng::FromDegrees(iter->template get<1>(), iter->template get<0>());
      add_cell_from_point(latlng);
      vertices.push_back(S2Point(latlng));
    }
    S2Loop *loop = new S2Loop(vertices);
    loop->Normalize();
    s2poly.emplace_back(loop);
  }

  typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
  for (; iterInnerRing != inner_rings.end(); ++iterInnerRing) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
    for (; iter != (*iterInnerRing).end(); ++iter) {
      S2LatLng latlng = S2LatLng::FromDegrees(iter->template get<1>(), iter->template get<0>());
      add_cell_from_point(latlng);
      vertices.push_back(S2Point(latlng));
    }
    S2Loop *loop = new S2Loop(vertices);
    loop->Normalize();
    s2poly.emplace_back(loop);
  }

  S2Polygon* py = new S2Polygon(std::move(s2poly));
  return py;
}

template<typename T_IBIN, typename T_BIN,
         typename T_BIN_RING, typename T_BIN_INNER_RING>
S2Polygon* ObWkbToS2Visitor::MakeProjS2Polygon(T_IBIN *geo)
{
  T_BIN& poly = *(T_BIN *)(geo->val());
  T_BIN_RING& exterior = poly.exterior_ring();
  T_BIN_INNER_RING& inner_rings = poly.inner_rings();
  std::vector<std::unique_ptr<S2Loop>> s2poly;
  if (poly.size() != 0) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = exterior.begin();
    for (; iter != exterior.end(); ++iter) {
      S2Point tmp = MakeS2PointFromXy(iter->template get<0>(), iter->template get<1>());
      add_cell_from_point(tmp);
      vertices.push_back(tmp);
    }
    S2Loop *loop = new S2Loop(vertices);
    loop->Normalize();
    s2poly.emplace_back(loop);
  }

  typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
  for (; iterInnerRing != inner_rings.end(); ++iterInnerRing) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
    for (; iter != (*iterInnerRing).end(); ++iter) {
      S2Point tmp = MakeS2PointFromXy(iter->template get<0>(), iter->template get<1>());
      add_cell_from_point(tmp);
      vertices.push_back(tmp);
    }
    S2Loop *loop = new S2Loop(vertices);
    loop->Normalize();
    s2poly.emplace_back(loop);
  }

  S2Polygon* py = new S2Polygon(std::move(s2poly));
  return py;
}


int ObWkbToS2Visitor::visit(ObIWkbGeogPoint *geo)
{
  INIT_SUCC(ret);
  if (geo->length() < (WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
  } else {
    s2v_.emplace_back(MakeS2Point<ObIWkbGeogPoint>(geo));
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeomPoint *geo)
{
  INIT_SUCC(ret);
  if (!invalid_) {
    s2v_.emplace_back(MakeProjS2Point(geo));
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeogLineString *geo)
{
  INIT_SUCC(ret);
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
  } else {
    S2Polyline *polyline = MakeS2Polyline<ObIWkbGeogLineString, ObWkbGeogLineString>(geo);
    s2v_.emplace_back(polyline);
    mbr_ = mbr_.is_empty() ? polyline->GetRectBound() : mbr_.Union(polyline->GetRectBound());
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeomLineString *geo)
{
  INIT_SUCC(ret);
  if (!invalid_) {
    s2v_.emplace_back(MakeProjS2Polyline<ObIWkbGeomLineString, ObWkbGeomLineString>(geo));
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeogPolygon *geo)
{
  INIT_SUCC(ret);
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
  } else {
    S2Polygon *polygon = MakeS2Polygon<ObIWkbGeogPolygon, ObWkbGeogPolygon,
                                    ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(geo);
    s2v_.emplace_back(polygon);
    mbr_ = mbr_.is_empty() ? polygon->GetRectBound() : mbr_.Union(polygon->GetRectBound());
  }

  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeomPolygon *geo)
{
  INIT_SUCC(ret);
  if (!invalid_) {
    if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
    } else {
      s2v_.emplace_back(MakeProjS2Polygon<ObIWkbGeomPolygon, ObWkbGeomPolygon,
                                          ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(geo));
    }
  }
  return ret;
}

int64_t ObWkbToS2Visitor::get_cellids(ObS2Cellids &cells, bool is_query, bool need_buffer,
                                      S1Angle distance)
{
  INIT_SUCC(ret);
  if (invalid_) {
    if (OB_FAIL(cells.push_back(exceedsBoundsCellID))) {
      LOG_WARN("fail to push_back cellid", K(ret));
    }
  } else {
    S2CellUnion cellids;
    S2RegionCoverer coverer(options_);
    uint32_t s2v_size = s2v_.size();

    for (int i = 0; i < s2v_size; i++) {
      S2CellUnion tmp = coverer.GetCovering(*s2v_[i]);
      cellids = cellids.Union(tmp);
    }
    if (need_buffer) {
      const int max_level_diff = 2;
      cellids.Expand(distance, max_level_diff);
    }
    if (s2v_size > 1) {
      cellids.Normalize();
    }
    S2CellId prev_id = S2CellId::None();
    for (int i = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
      if (OB_FAIL(cells.push_back(cellids[i].id()))) {
        LOG_WARN("fail to push_back cellid", K(ret));
      }
      if (OB_SUCC(ret) && is_query) {
        int level = cellids[i].level();
        while (OB_SUCC(ret) && (level -= options_.level_mod()) >= options_.min_level()) {
          S2CellId ancestor_id = cellids[i].parent(level);
          if (prev_id != S2CellId::None() && prev_id.level() > level &&
              prev_id.parent(level) == ancestor_id) {
            break;
          }
          if (OB_FAIL(cells.push_back(ancestor_id.id()))) {
            LOG_WARN("fail to push_back cellid", K(ret));
          }
        }
      }
      prev_id = cellids[i];
    }
    if (OB_SUCC(ret) && has_reset_ && OB_FAIL(cells.push_back(exceedsBoundsCellID))) {
      LOG_WARN("fail to push_back cellid", K(ret));
    }
  }
  return ret;
}

int64_t ObWkbToS2Visitor::get_inner_cover_cellids(ObS2Cellids &cells)
{
  INIT_SUCC(ret);
  if (invalid_) {
    if (OB_FAIL(cells.push_back(exceedsBoundsCellID))) {
      LOG_WARN("fail to push_back cellid", K(ret));
    }
  } else {
    S2CellUnion cellids(S2cells_);
    cellids.Normalize();
    for (int i = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
      if (OB_FAIL(cells.push_back(cellids[i].id()))) {
        LOG_WARN("fail to push_back cellid", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObWkbToS2Visitor::get_mbr(S2LatLngRect &mbr, bool need_buffer, S1Angle distance)
{
  INIT_SUCC(ret);
  if (invalid_ || has_reset_) {
    mbr = S2LatLngRect::Full();
  } else {
    if (need_buffer) {
      mbr_ = mbr_.ExpandedByDistance(distance);
    }
    // avoid rounding errors in mbr_ calculation
    mbr = mbr_.ExpandedByDistance(S1Angle::Degrees(DBL_EPSILON));
  }
  return ret;
}

void ObWkbToS2Visitor::reset()
{
  s2v_.clear();
  mbr_ = S2LatLngRect::Empty();
  S2cells_.clear();
  invalid_ = false;
  has_reset_ = true;
}


} // namespace common
} // namespace oceanbase