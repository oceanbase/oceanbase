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
#include "ob_geo_grid_visitor.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
bool ObGeoGridVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo) || !geo->is_tree() || OB_ISNULL(grid_)) {
    bret = false;
  }
  reset_duplicate_point();
  return bret;
}

bool ObGeoGridVisitor::is_duplicate_point(double x, double y)
{
  bool bret = false;
  if (grid_->x_size > 0) {
    if (!use_floor_) {
      x = rint((x - grid_->x_ip) / grid_->x_size) * grid_->x_size + grid_->x_ip;
    } else {
      x = floor((x - grid_->x_ip) / grid_->x_size) * grid_->x_size + grid_->x_ip;
    }
  }
  if (grid_->y_size > 0) {
    if (!use_floor_) {
      y = rint((y - grid_->y_ip) / grid_->y_size) * grid_->y_size + grid_->y_ip;
    } else {
      y = floor((y - grid_->y_ip) / grid_->y_size) * grid_->y_size + grid_->y_ip;
    }
  }
  bool x_equals = fabs(x - last_x_) <= 1e-12;
  bool y_equals = fabs(y - last_y_) <= 1e-12;
  if ((std::isnan(last_x_) && std::isnan(last_y_)) || !x_equals || !y_equals) {
    last_x_ = x;
    last_y_ = y;
  } else {
    bret = true;
  }
  return bret;
}

void ObGeoGridVisitor::reset_duplicate_point()
{
  last_x_ = NAN;
  last_y_ = NAN;
}

template<typename POINT>
void ObGeoGridVisitor::point_visit(POINT *geo)
{
  if (!is_duplicate_point(geo->x(), geo->y())) {
    geo->x(last_x_);
    geo->y(last_y_);
  }
}

int ObGeoGridVisitor::visit(ObCartesianPoint *geo)
{
  point_visit(geo);
  return OB_SUCCESS;
}

template<typename LINE>
void ObGeoGridVisitor::multi_point_visit(LINE &geo, int32_t min_point)
{
  int32_t valid_idx = 0;
  int64_t sz = geo.size();
  for (int32_t i = 0; i < sz; ++i) {
    if (!is_duplicate_point(geo[i].template get<0>(), geo[i].template get<1>())) {
      geo[valid_idx].template set<0>(last_x_);
      geo[valid_idx].template set<1>(last_y_);
      ++valid_idx;
    }
  }
  if (valid_idx < min_point) {
    geo.clear();
  } else {
    geo.resize(valid_idx);
  }
  reset_duplicate_point();
}

int ObGeoGridVisitor::visit(ObCartesianLineString *geo)
{
  multi_point_visit(*geo, LINESTRING_MIN_POINT);
  return OB_SUCCESS;
}

template<typename POLYGON, typename RING>
void ObGeoGridVisitor::polygon_visit(POLYGON &geo)
{
  if (geo.size() > 0) {
    RING &ext_ring = geo.exterior_ring();
    multi_point_visit(ext_ring, RING_MIN_POINT);
    if (ext_ring.empty()) {
      // if ext ring is invalid, then total polygon is invalid
      geo.interior_rings().clear();
    } else if (geo.inner_ring_size() > 0) {
      uint64_t inner_sz = geo.inner_ring_size();
      int32_t valid_inner_ring = 0;
      for (uint32_t i = 0; i < inner_sz; ++i) {
        RING &inner_ring = geo.inner_ring(i);
        multi_point_visit(inner_ring, RING_MIN_POINT);
        if (!inner_ring.empty()) {
          geo.interior_rings()[valid_inner_ring++] = inner_ring;
        }
      }
      geo.interior_rings().resize(valid_inner_ring);
    }
  }
}

int ObGeoGridVisitor::visit(ObCartesianPolygon *geo)
{
  polygon_visit<ObCartesianPolygon, ObCartesianLinearring>(*geo);
  return OB_SUCCESS;
}

int ObGeoGridVisitor::visit(ObCartesianMultipoint *geo)
{
  multi_point_visit(*geo, 1);
  return OB_SUCCESS;
}

int ObGeoGridVisitor::visit(ObCartesianMultilinestring *geo)
{
  int32_t valid_line = 0;
  ObCartesianMultilinestring &line = *geo;
  uint64_t sz = line.size();
  for (int32_t i = 0; i < sz; ++i) {
    multi_point_visit(line[i], LINESTRING_MIN_POINT);
    if (line[i].size() != 0) {
      if (valid_line != i) {
        line[valid_line] = line[i];
      }
      ++valid_line;
    }
  }
  if (valid_line) {
    line.resize(valid_line);
  } else {
    line.clear();
  }
  return OB_SUCCESS;
}

int ObGeoGridVisitor::visit(ObCartesianMultipolygon *geo)
{
  int32_t valid_poly = 0;
  ObCartesianMultipolygon &poly = *geo;
  uint64_t sz = poly.size();
  for (int32_t i = 0; i < sz; ++i) {
    polygon_visit<ObCartesianPolygon, ObCartesianLinearring>(poly[i]);
    if (poly[i].size() != 0) {
      if (valid_poly != i) {
        poly[valid_poly] = poly[i];
      }
      ++valid_poly;
    }
  }
  if (valid_poly) {
    poly.resize(valid_poly);
  } else {
    poly.clear();
  }
  return OB_SUCCESS;
}

int ObGeoGridVisitor::visit(ObCartesianGeometrycollection *geo)
{
  int ret = OB_SUCCESS;
  int32_t valid_geo = 0;
  for (int32_t i = 0; i < geo->size() && OB_SUCC(ret); i++) {
    if (OB_FAIL((*geo)[i].do_visit(*this))) {
      LOG_WARN("failed to do tree item visit", K(ret));
    } else if (!(*geo)[i].is_empty()) {
      if (valid_geo != i) {
        (*geo)[valid_geo] = (*geo)[i];
      }
      ++valid_geo;
    }
  }
  if (valid_geo) {
    geo->resize(valid_geo);
  } else {
    geo->clear();
  }
  return ret;
}

int ObGeoGridVisitor::finish(ObGeometry *geo)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

}  // namespace common
}  // namespace oceanbase