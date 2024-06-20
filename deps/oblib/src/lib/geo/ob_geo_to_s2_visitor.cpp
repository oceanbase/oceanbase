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
#include "lib/hash/ob_hashset.h"
#include "lib/geo/ob_geo_dispatcher.h"

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

int ObWkbToS2Visitor::add_cell_from_point(S2Point point)
{
  int ret = OB_SUCCESS;
  S2CellId cell_id = S2CellId(point).parent(options_.max_level());
  if (OB_FAIL(vector_push_back<S2CellId>(S2cells_, cell_id))) {
    LOG_WARN("failed to add cell id", K(ret));
  }
  return ret;
}

int ObWkbToS2Visitor::add_cell_from_point(S2LatLng point)
{
  int ret = OB_SUCCESS;
  S2CellId cell_id = S2CellId(point).parent(options_.max_level());
  if (OB_FAIL(vector_push_back<S2CellId>(S2cells_, cell_id))) {
    LOG_WARN("failed to add cell id", K(ret));
  }
  return ret;
}

template<typename T_IBIN>
int ObWkbToS2Visitor::MakeS2Point(T_IBIN *geo, S2Cell *&res)
{
  int ret = OB_SUCCESS;
  S2LatLng latlng = S2LatLng::FromDegrees(geo->y(), geo->x());
  if (OB_FAIL(add_cell_from_point(latlng))) {
    LOG_WARN("failed to add cell from point", K(ret));
  } else {
    mbr_ = mbr_.is_empty() ? S2LatLngRect(latlng, latlng) : mbr_.Union(S2LatLngRect(latlng, latlng));
    S2Cell* p = new S2Cell(latlng);
    if (OB_ISNULL(p)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc s2cell", K(ret));
    } else {
      res = p;
    }
  }
  return ret;
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
int ObWkbToS2Visitor::MakeProjS2Point(T_IBIN *geo, S2Cell *&res)
{
  int ret = OB_SUCCESS;
  S2Point point = MakeS2PointFromXy(geo->x(), geo->y());
  S2Cell* p = NULL;
  if (!invalid_) {
    if (OB_FAIL(add_cell_from_point(point))) {
      LOG_WARN("failed to add cell from point", K(ret));
    } else {
      p = new S2Cell(point);
      if (OB_ISNULL(p)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc s2cell", K(ret));
      } else {
        res = p;
      }
    }
  }
  return ret;
}

template<typename T_IBIN>
int ObWkbToS2Visitor::MakeS2Polyline(T_IBIN *geo, S2Polyline *&res)
{
  int ret = OB_SUCCESS;
  std::vector<S2LatLng> vertices;
  const typename T_IBIN::value_type *line = reinterpret_cast<const typename T_IBIN::value_type *>(geo->val());
  typename T_IBIN::value_type::iterator iter = line->begin();
  for ( ; iter != line->end() && OB_SUCC(ret); iter++) {
    S2LatLng latlng = S2LatLng::FromDegrees(iter->template get<1>(),
                                            iter->template get<0>());
    if (OB_FAIL(add_cell_from_point(latlng))) {
      LOG_WARN("failed to add cell from point", K(ret));
    } else if (OB_FAIL(vector_push_back<S2LatLng>(vertices, latlng))) {
      LOG_WARN("failed to add vertice", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    S2Polyline* ptr = new S2Polyline(vertices);
    if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc s2cell", K(ret));
    } else {
      res = ptr;
    }
  }

  return ret;
}

template<typename T_IBIN>
int ObWkbToS2Visitor::MakeProjS2Polyline(T_IBIN *geo, S2Polyline *&res)
{
  int ret = OB_SUCCESS;
  std::vector<S2Point> vertices;
  const typename T_IBIN::value_type *line = reinterpret_cast<const typename T_IBIN::value_type *>(geo->val());
  typename T_IBIN::value_type::iterator iter = line->begin();
  for ( ; iter != line->end() && OB_SUCC(ret); iter++) {
    S2Point p = MakeS2PointFromXy(iter->template get<0>(),
                                  iter->template get<1>());
    if (OB_FAIL(add_cell_from_point(p))) {
      LOG_WARN("failed to add cell from point", K(ret));
    } else if (OB_FAIL(vector_push_back<S2Point>(vertices, p))) {
      LOG_WARN("failed to add vertice", K(ret));
    }
  }

 if (OB_SUCC(ret)) {
    S2Polyline* ptr = new S2Polyline(vertices);
    if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc s2cell", K(ret));
    } else {
      res = ptr;
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN,
         typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbToS2Visitor::MakeS2Polygon(T_IBIN *geo, S2Polygon *&res)
{
  int ret = OB_SUCCESS;
  T_BIN& poly = *(T_BIN *)(geo->val());
  T_BIN_RING& exterior = poly.exterior_ring();
  T_BIN_INNER_RING& inner_rings = poly.inner_rings();
  std::vector<std::unique_ptr<S2Loop>> s2poly;
  if (poly.size() != 0) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = exterior.begin();
    for (; iter != exterior.end() && OB_SUCC(ret); ++iter) {
      S2LatLng latlng = S2LatLng::FromDegrees(iter->template get<1>(), iter->template get<0>());
      S2Point tmp = S2Point(latlng);
      if (OB_FAIL(add_cell_from_point(latlng))) {
        LOG_WARN("failed to add cell from point", K(ret));
      } else if (OB_FAIL(vector_push_back<S2Point>(vertices, tmp))) {
        LOG_WARN("failed to add vertice", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      S2Loop *loop = new S2Loop(vertices);
      if (OB_ISNULL(loop)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc s2cell", K(ret));
      } else {
        loop->Normalize();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(vector_emplace_back(s2poly, loop))) {
          LOG_WARN("failed to add loop", K(ret));
        }
      }
    }
  }

  typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
  for (; iterInnerRing != inner_rings.end() && OB_SUCC(ret); ++iterInnerRing) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
    for (; iter != (*iterInnerRing).end() && OB_SUCC(ret); ++iter) {
      S2LatLng latlng = S2LatLng::FromDegrees(iter->template get<1>(), iter->template get<0>());
      S2Point tmp = S2Point(latlng);
      if (OB_FAIL(add_cell_from_point(latlng))) {
        LOG_WARN("failed to add cell from point", K(ret));
      } else if (OB_FAIL(vector_push_back<S2Point>(vertices, tmp))) {
        LOG_WARN("failed to add vertice", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      S2Loop *loop = new S2Loop(vertices);
      if (OB_ISNULL(loop)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc s2cell", K(ret));
      } else {
        loop->Normalize();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(vector_emplace_back(s2poly, loop))) {
          LOG_WARN("failed to add loop", K(ret));
        }
      }
    }
  }

  S2Polygon* py = new S2Polygon(std::move(s2poly));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(py)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc s2cell", K(ret));
  } else {
    res = py;
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN,
         typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbToS2Visitor::MakeProjS2Polygon(T_IBIN *geo, S2Polygon *&res)
{
  int ret = OB_SUCCESS;
  T_BIN& poly = *(T_BIN *)(geo->val());
  T_BIN_RING& exterior = poly.exterior_ring();
  T_BIN_INNER_RING& inner_rings = poly.inner_rings();
  std::vector<std::unique_ptr<S2Loop>> s2poly;
  if (poly.size() != 0) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = exterior.begin();
    for (; iter != exterior.end() && OB_SUCC(ret); ++iter) {
      S2Point tmp = MakeS2PointFromXy(iter->template get<0>(), iter->template get<1>());
      if (OB_FAIL(add_cell_from_point(tmp))) {
        LOG_WARN("failed to add cell from point", K(ret));
      } else if (OB_FAIL(vector_push_back<S2Point>(vertices, tmp))) {
        LOG_WARN("failed to add vertice", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      S2Loop *loop = new S2Loop(vertices);
      if (OB_ISNULL(loop)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc s2cell", K(ret));
      } else {
        loop->Normalize();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(vector_emplace_back(s2poly, loop))) {
          LOG_WARN("failed to add loop", K(ret));
        }
      }
    }
  }

  typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
  for (; iterInnerRing != inner_rings.end() && OB_SUCC(ret); ++iterInnerRing) {
    std::vector<S2Point> vertices;
    typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
    for (; iter != (*iterInnerRing).end() && OB_SUCC(ret); ++iter) {
      S2Point tmp = MakeS2PointFromXy(iter->template get<0>(), iter->template get<1>());
      if (OB_FAIL(add_cell_from_point(tmp))) {
        LOG_WARN("failed to add cell from point", K(ret));
      } else if (OB_FAIL(vector_push_back<S2Point>(vertices, tmp))) {
        LOG_WARN("failed to add vertice", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      S2Loop *loop = new S2Loop(vertices);
      if (OB_ISNULL(loop)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc s2cell", K(ret));
      } else {
        loop->Normalize();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(vector_emplace_back(s2poly, loop))) {
          LOG_WARN("failed to add loop", K(ret));
        }
      }
    }
  }

  S2Polygon* py = new S2Polygon(std::move(s2poly));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(py)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc s2cell", K(ret));
  } else {
    res = py;
  }
  return ret;
}


int ObWkbToS2Visitor::visit(ObIWkbGeogPoint *geo)
{
  INIT_SUCC(ret);
  S2Cell *res = NULL;
  if (geo->length() < (WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
  } else if (OB_FAIL(MakeS2Point<ObIWkbGeogPoint>(geo, res))) {
    LOG_WARN("failed to make s2 point", K(ret));
  } else if (OB_FAIL(vector_emplace_back<S2Cell>(s2v_, res))) {
    LOG_WARN("failed to add s2 cell", K(ret));
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeomPoint *geo)
{
  INIT_SUCC(ret);
  S2Cell *cell = nullptr;
  if (!invalid_) {
    if (OB_FAIL(MakeProjS2Point(geo, cell))) {
      LOG_WARN("failed to make s2 point", K(ret));
    } else if (OB_FAIL(vector_emplace_back<S2Cell>(s2v_, cell))) {
      LOG_WARN("failed to add s2 cell", K(ret));
    }
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeogLineString *geo)
{
  INIT_SUCC(ret);
  S2Polyline *polyline = nullptr;
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
  } else if (OB_FAIL(MakeS2Polyline<ObIWkbGeogLineString>(geo, polyline))) {
    LOG_WARN("failed to make s2 poly line", K(ret), K(geo->length()));
  } else if (OB_FAIL(vector_emplace_back<S2Polyline>(s2v_, polyline))) {
    LOG_WARN("failed to add s2 cell", K(ret));
  } else {
    mbr_ = mbr_.is_empty() ? polyline->GetRectBound() : mbr_.Union(polyline->GetRectBound());
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeomLineString *geo)
{
  INIT_SUCC(ret);
  if (!invalid_) {
    S2Polyline *line = nullptr;
    if (OB_FAIL(MakeProjS2Polyline<ObIWkbGeomLineString>(geo, line))) {
      LOG_WARN("failed to make s2 poly line", K(ret));
    } else if (OB_FAIL(vector_emplace_back<S2Polyline>(s2v_, line))) {
      LOG_WARN("failed to add s2 cell", K(ret));
    }
  }
  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeogPolygon *geo)
{
  INIT_SUCC(ret);
  S2Polygon *polygon = nullptr;
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
  } else if ((ret = MakeS2Polygon<ObIWkbGeogPolygon, ObWkbGeogPolygon,
                                  ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(geo, polygon)) != OB_SUCCESS) {
    LOG_WARN("failed to make s2 poly", K(ret), K(geo->length()));
  } else if (OB_FAIL(vector_emplace_back<S2Polygon>(s2v_, polygon))) {
    LOG_WARN("failed to add s2 polygon", K(ret));
  } else {
    mbr_ = mbr_.is_empty() ? polygon->GetRectBound() : mbr_.Union(polygon->GetRectBound());
  }

  return ret;
}

int ObWkbToS2Visitor::visit(ObIWkbGeomPolygon *geo)
{
  INIT_SUCC(ret);
  if (!invalid_) {
    S2Polygon *poly = nullptr;
    if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid swkb length", K(ret), K(geo->length()));
    } else if ((ret = MakeProjS2Polygon<ObIWkbGeomPolygon, ObWkbGeomPolygon,
                                        ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(geo, poly)) != OB_SUCCESS) {
      LOG_WARN("failed to make s2 poly", K(ret), K(geo->length()));
    } else if (OB_FAIL(vector_emplace_back<S2Polygon>(s2v_, poly))) {
      LOG_WARN("failed to add s2 polygon", K(ret));
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

int64_t ObWkbToS2Visitor::get_cellids_and_unrepeated_ancestors(ObS2Cellids &cells,
                                                               ObS2Cellids &ancestors,
                                                               bool need_buffer,
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
    hash::ObHashSet<uint64_t> cellid_set;
    if (OB_FAIL(cellid_set.create(128, "CellidSet", "HashNode"))) {
      LOG_WARN("failed to create cellid set", K(ret));
    } else if (!cellid_set.created()) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail to init cellid set", K(ret));
    } else {
      for (int i = 0; i < s2v_size && OB_SUCC(ret); i++) {
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
        int hash_ret = cellid_set.exist_refactored(cellids[i].id());
        if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(cellid_set.set_refactored(cellids[i].id()))) {
            LOG_WARN("failed to add cellid into set", K(ret));
          } else if (OB_FAIL(cells.push_back(cellids[i].id()))) {
            LOG_WARN("fail to push_back cellid", K(ret));
          }
          if (OB_SUCC(ret)) {
            int level = cellids[i].level();
            while (OB_SUCC(ret) && (level -= options_.level_mod()) >= options_.min_level()) {
              S2CellId ancestor_id = cellids[i].parent(level);
              if (prev_id != S2CellId::None() && prev_id.level() > level &&
                  prev_id.parent(level) == ancestor_id) {
                break;
              }
              int ancestor_hash_ret = cellid_set.exist_refactored(ancestor_id.id());
              if (OB_HASH_NOT_EXIST == ancestor_hash_ret) {
                if (OB_FAIL(cellid_set.set_refactored(ancestor_id.id()))) {
                  LOG_WARN("failed to add cellid into set", K(ret));
                } else if (OB_FAIL(ancestors.push_back(ancestor_id.id()))) {
                  LOG_WARN("fail to push_back cellid", K(ret));
                }
              } else if (OB_HASH_EXIST != ancestor_hash_ret) {
                ret = ancestor_hash_ret;
                LOG_WARN("fail to check if key exist", K(ret), K(i));
              }
            }
          }
        } else if (OB_HASH_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to check if key exist", K(ret), K(i));
        }
        prev_id = cellids[i];
      }
      if (OB_SUCC(ret) && has_reset_ && OB_FAIL(cells.push_back(exceedsBoundsCellID))) {
        LOG_WARN("fail to push_back cellid", K(ret));
      }
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

template <typename ElementType>
int ObWkbToS2Visitor::vector_push_back(std::vector<ElementType> &vector, ElementType &element)
{
  int ret = OB_SUCCESS;
  try {
    vector.push_back(element);
  } catch(...) {
    ret = ob_boost_geometry_exception_handle();
  }
  return ret;
}

int ObWkbToS2Visitor::vector_emplace_back(std::vector<std::unique_ptr<S2Loop>> &vector, S2Loop *element)
{
  int ret = OB_SUCCESS;
  try {
    vector.emplace_back(element);
  } catch(...) {
    ret = ob_boost_geometry_exception_handle();
  }
  return ret;
}
template <typename ElementType>
int ObWkbToS2Visitor::vector_emplace_back(std::vector<std::unique_ptr<S2Region>> &vector, ElementType *element)
{
  int ret = OB_SUCCESS;
  try {
    vector.emplace_back(element);
  } catch(...) {
    ret = ob_boost_geometry_exception_handle();
  }
  return ret;
}

} // namespace common
} // namespace oceanbase