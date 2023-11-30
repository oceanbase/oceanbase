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
#include "ob_geo_box_clip_visitor.h"
#include "lib/geo/ob_geo_func_register.h"

namespace oceanbase
{
namespace common
{
ObBoxPosition ObGeoBoxClipVisitor::get_position(double x, double y)
{
  ObBoxPosition position = ObBoxPosition::INVALID;
  if (x > xmin_ && x < xmax_ && y > ymin_ && y < ymax_) {
    position = ObBoxPosition::INSIDE;
  } else if (x < xmin_ || x > xmax_ || y < ymin_ || y > ymax_) {
    position = ObBoxPosition::OUTSIDE;
  } else {
    uint8_t pos = 0;
    if (x == xmin_) {
      pos |= ObBoxPosition::LEFT_EDGE;
    } else if (x == xmax_) {
      pos |= ObBoxPosition::RIGHT_EDGE;
    }
    if (y == ymax_) {
      pos |= ObBoxPosition::TOP_EDGE;
    } else if (y == ymin_) {
      pos |= ObBoxPosition::BOTTOM_EDGE;
    }
    position = ObBoxPosition(pos);
  }
  return position;
}

ObGeoType ObGeoBoxClipVisitor::get_result_basic_type()
{
  ObGeoType type = ObGeoType::GEOMETRY;
  if (res_geo_->empty()) {
    type = ObGeoType::GEOMETRYCOLLECTION;
  } else {
    type = res_geo_->front().type();
    for (uint32_t i = 1; i < res_geo_->size() && type != ObGeoType::GEOMETRYCOLLECTION; ++i) {
      if (type != (*res_geo_)[i].type()) {
        type = ObGeoType::GEOMETRYCOLLECTION;
      }
    }
  }
  return type;
}

int ObGeoBoxClipVisitor::get_geometry(ObGeometry *&geo)
{
  int ret = OB_SUCCESS;
  ObGeoType type = get_result_basic_type();
  geo = nullptr;
  if (res_geo_->size() == 1) {
    // Point/Polygon/Linestring
    geo = &(res_geo_->front());
  } else if (type == ObGeoType::GEOMETRYCOLLECTION) {
    geo = res_geo_;
  } else {
    // MultiPoint/MultiPolygon/MultiLinestring
    if (type == ObGeoType::POINT) {
      ObCartesianMultipoint *mpt =
          OB_NEWx(ObCartesianMultipoint, allocator_, res_geo_->get_srid(), *allocator_);
      if (OB_ISNULL(mpt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < res_geo_->size(); ++i) {
        if (OB_FAIL(mpt->push_back(
                *reinterpret_cast<ObWkbGeomInnerPoint const *>((*res_geo_)[i].val())))) {
          LOG_WARN("failed to add point to multipoint", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        geo = mpt;
      }
    } else if (type == ObGeoType::POLYGON) {
      ObCartesianMultipolygon *mpy =
          OB_NEWx(ObCartesianMultipolygon, allocator_, res_geo_->get_srid(), *allocator_);
      if (OB_ISNULL(mpy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < res_geo_->size(); ++i) {
        if (OB_FAIL(
                mpy->push_back(*reinterpret_cast<ObCartesianPolygon const *>(&(*res_geo_)[i])))) {
          LOG_WARN("failed to add point to multipoint", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        geo = mpy;
      }
    } else if (type == ObGeoType::LINESTRING) {
      ObCartesianMultilinestring *mls =
          OB_NEWx(ObCartesianMultilinestring, allocator_, res_geo_->get_srid(), *allocator_);
      if (OB_ISNULL(mls)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < res_geo_->size(); ++i) {
        if (OB_FAIL(mls->push_back(
                *reinterpret_cast<ObCartesianLineString const *>(&(*res_geo_)[i])))) {
          LOG_WARN("failed to add point to multipoint", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        geo = mls;
      }
    }
  }
  return ret;
}

bool ObGeoBoxClipVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo)) {
    bret = false;
  } else if (OB_ISNULL(res_geo_)) {
    // Prevents a new res_geo_ from being created twice when accessing multi geometry
    res_geo_ = OB_NEWx(ObCartesianGeometrycollection, allocator_, geo->get_srid(), *allocator_);
    if (OB_ISNULL(res_geo_)) {
      bret = false;
    }
  }
  return bret;
}

int ObGeoBoxClipVisitor::visit(ObCartesianPoint *geo)
{
  int ret = OB_SUCCESS;
  if (geo->is_empty()) {
    // do nothing
  } else if (get_position(geo->x(), geo->y()) == ObBoxPosition::INSIDE) {
    if (OB_FAIL(res_geo_->push_back(*geo))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit(ObCartesianMultipoint *geo)
{
  int ret = OB_SUCCESS;
  if (!geo->is_empty()) {
    ObCartesianMultipoint mpt(geo->get_srid(), *allocator_);
    for (uint32_t i = 0; i < geo->size() && OB_SUCC(ret); ++i) {
      const ObWkbGeomInnerPoint &pt = (*geo)[i];
      if (get_position(pt.get<0>(), pt.get<1>()) == ObBoxPosition::INSIDE) {
        if (OB_FAIL(mpt.push_back(pt))) {
          LOG_WARN("fail to push geometry", K(ret), K(geo->size()), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && !mpt.empty()) {
      // only push basic type (point/polygon/linestring) into res_geo_
      for (uint32_t i = 0; OB_SUCC(ret) && i < mpt.size(); ++i) {
        ObCartesianPoint *pt = OB_NEWx(ObCartesianPoint,
            allocator_,
            mpt[i].get<0>(),
            mpt[i].get<1>(),
            geo->get_srid(),
            allocator_);
        if (OB_ISNULL(pt)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for geometry", K(ret));
        } else if (OB_FAIL(res_geo_->push_back(*pt))) {
          LOG_WARN("fail to push back geometry", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObGeoBoxClipVisitor::clip_point_to_single_edge(
    ObWkbGeomInnerPoint &out_pt, const ObWkbGeomInnerPoint &in_pt, double edge, bool is_x_edge)
{
  if (is_x_edge) {
    if (in_pt.get<0>() == edge) {
      out_pt.set<0>(in_pt.get<0>());
      out_pt.set<1>(in_pt.get<1>());
    } else if (in_pt.get<0>() != out_pt.get<0>()) {
      double out_y = out_pt.get<1>();
      double out_x = out_pt.get<0>();
      double in_y = in_pt.get<1>();
      double in_x = in_pt.get<0>();
      out_pt.set<1>(out_y + (in_y - out_y) * (edge - out_x) / (in_x - out_x));
      out_pt.set<0>(edge);
    }
  } else {
    if (in_pt.get<1>() == edge) {
      out_pt.set<0>(in_pt.get<0>());
      out_pt.set<1>(in_pt.get<1>());
    } else if (in_pt.get<1>() != out_pt.get<1>()) {
      double out_x = out_pt.get<0>();
      double out_y = out_pt.get<1>();
      double in_x = in_pt.get<0>();
      double in_y = in_pt.get<1>();
      out_pt.set<0>(out_x + (in_x - out_x) * (edge - out_y) / (in_y - out_y));
      out_pt.set<1>(edge);
    }
  }
}

/**
 * @brief Get the intersection point of the line that intersects the box
 * @param out_pt the point where the line is outside the Box
 * @param in_pt the point where the line is inside the Box
 * @param pt intersection point
 */
void ObGeoBoxClipVisitor::clip_point_to_edges(
    const ObWkbGeomInnerPoint &out_pt, const ObWkbGeomInnerPoint &in_pt, ObWkbGeomInnerPoint &pt)
{
  pt = out_pt;
  if (pt.get<0>() < xmin_) {
    clip_point_to_single_edge(pt, in_pt, xmin_, true);
  } else if (pt.get<0>() > xmax_) {
    clip_point_to_single_edge(pt, in_pt, xmax_, true);
  }

  if (pt.get<1>() < ymin_) {
    clip_point_to_single_edge(pt, in_pt, ymin_, false);
  } else if (pt.get<1>() > ymax_) {
    clip_point_to_single_edge(pt, in_pt, ymax_, false);
  }
}

bool ObGeoBoxClipVisitor::same_edge_positions(ObBoxPosition pos1, ObBoxPosition pos2)
{
  return ObBoxPosition(pos1 & pos2) > ObBoxPosition::OUTSIDE;
}

int ObGeoBoxClipVisitor::construct_intersect_line(const ObCartesianLineString &line,
    int32_t first_inside_idx, int32_t last_idx, ObCartesianLineString &intersect_line)
{
  int ret = OB_SUCCESS;
  for (int32_t j = first_inside_idx; OB_SUCC(ret) && j < last_idx; ++j) {
    if (OB_FAIL(intersect_line.push_back(line[j]))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::line_visit_inside_or_edge(const ObCartesianLineString &line, int &idx,
    ObCartesianMultilinestring *mls, ObBoxPosition pos, ObCartesianLineString &new_line,
    bool &completely_inside)
{
  int ret = OB_SUCCESS;
  int32_t first_inside_idx = idx;
  ObBoxPosition prev_pos = ObBoxPosition::INVALID;
  int64_t sz = line.size();
  for (++idx; OB_SUCC(ret) && pos != ObBoxPosition::OUTSIDE && idx < sz; ++idx) {
    prev_pos = pos;
    pos = get_position(line[idx].get<0>(), line[idx].get<1>());
    if (pos == ObBoxPosition::INSIDE) {
      // do nothing
    } else if (pos == ObBoxPosition::OUTSIDE) {
      ObWkbGeomInnerPoint cur_pt_edge;
      clip_point_to_edges(line[idx], line[idx - 1], cur_pt_edge);
      ObBoxPosition clip_pos = get_position(cur_pt_edge.get<0>(), cur_pt_edge.get<1>());
      bool clip_box = !cur_pt_edge.equals(line[idx]) && !same_edge_positions(clip_pos, prev_pos);
      if (first_inside_idx < idx - 1 || !new_line.empty() || clip_box) {
        if (OB_FAIL(construct_intersect_line(line, first_inside_idx, idx, new_line))) {
          LOG_WARN("fail to construct intersect line", K(ret), K(first_inside_idx), K(idx));
        } else if (clip_box && OB_FAIL(new_line.push_back(cur_pt_edge))) {
          LOG_WARN("fail to push back geometry", K(ret));
        } else if (OB_FAIL(mls->push_back(new_line))) {
          LOG_WARN("fail to push back geometry", K(ret));
        } else {
          new_line.clear();
        }
      }
    } else {  // on edge
      if (same_edge_positions(prev_pos, pos)) {
        if (first_inside_idx < idx - 1 || !new_line.empty()) {
          if (OB_FAIL(construct_intersect_line(line, first_inside_idx, idx, new_line))) {
            LOG_WARN("fail to construct intersect line", K(ret), K(first_inside_idx), K(idx));
          } else if (OB_FAIL(mls->push_back(new_line))) {
            LOG_WARN("fail to push back geometry", K(ret));
          } else {
            new_line.clear();
          }
        }
        first_inside_idx = idx;
      }
    }
  }
  if (pos == ObBoxPosition::OUTSIDE) {
    --idx;  // keep idx = last inside/on edge point
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (first_inside_idx == 0 && idx >= sz) {
    // all points are inside
    completely_inside = true;
    if (OB_FAIL(mls->push_back(line))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  } else if (pos != ObBoxPosition::OUTSIDE
             && (first_inside_idx < idx - 1 || !new_line.empty())) {
    if (OB_FAIL(construct_intersect_line(line, first_inside_idx, idx, new_line))) {
      LOG_WARN("fail to construct intersect line", K(ret), K(first_inside_idx), K(idx));
    } else if (OB_FAIL(mls->push_back(new_line))) {
      LOG_WARN("fail to push back geometry", K(ret));
    } else {
      new_line.clear();
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::line_visit_outside(const ObCartesianLineString &line, int &idx,
    ObCartesianMultilinestring *mls, ObBoxPosition pos, ObCartesianLineString &new_line)
{
  int ret = OB_SUCCESS;
  int64_t sz = line.size();
  double x = line[idx].get<0>();
  double y = line[idx].get<1>();
  ++idx;
  // find point inside/on edge
  if (x < xmin_) {
    while (idx < sz && line[idx].get<0>() < xmin_) {
      ++idx;
    }
  } else if (x > xmax_) {
    while (idx < sz && line[idx].get<0>() > xmax_) {
      ++idx;
    }
  } else if (y < ymin_) {
    while (idx < sz && line[idx].get<1>() < ymin_) {
      ++idx;
    }
  } else if (y > ymax_) {
    while (idx < sz && line[idx].get<1>() > ymax_) {
      ++idx;
    }
  }
  if (idx < sz) {
    x = line[idx].get<0>();
    y = line[idx].get<1>();
    pos = get_position(x, y);
    ObWkbGeomInnerPoint pt;
    clip_point_to_edges(line[idx - 1], line[idx], pt);
    if (pos == ObBoxPosition::INSIDE) {
      // first inside point
      if (OB_FAIL(new_line.push_back(pt))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
    } else if (pos == ObBoxPosition::OUTSIDE) {
      // Two outside points line[idx] and line[idx - 1] may connected through the box
      // need to clip two side
      ObWkbGeomInnerPoint to_pt;
      clip_point_to_edges(line[idx], pt, to_pt);
      ObBoxPosition prev_pos = get_position(pt.get<0>(), pt.get<1>());
      pos = get_position(to_pt.get<0>(), to_pt.get<1>());
      if (!pt.equals(to_pt)  // not in same pos
          && prev_pos > ObBoxPosition::OUTSIDE
          && pos > ObBoxPosition::OUTSIDE            // intersects with box
          && !same_edge_positions(prev_pos, pos)) {  // not in same edge
        if (OB_FAIL(new_line.push_back(pt))) {
          LOG_WARN("fail to push back geometry", K(ret));
        } else if (OB_FAIL(new_line.push_back(to_pt))) {
          LOG_WARN("fail to push back geometry", K(ret));
        } else if (OB_FAIL(mls->push_back(new_line))) {
          LOG_WARN("fail to push back geometry", K(ret));
        } else {
          new_line.clear();
        }
      }
    } else {  // EDGE
      ObBoxPosition prev_pos = get_position(pt.get<0>(), pt.get<1>());
      if (!same_edge_positions(prev_pos, pos)) {
        if (OB_FAIL(new_line.push_back(pt))) {
          LOG_WARN("fail to push back geometry", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::line_visit(
    const ObCartesianLineString &line, ObCartesianMultilinestring *&mls, bool &completely_inside)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mls)) {
    mls = OB_NEWx(ObCartesianMultilinestring, allocator_);
    if (OB_ISNULL(mls)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for geometry", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t sz = line.size();
    int32_t idx = 0;
    ObCartesianLineString new_line(line.get_srid(), *allocator_);
    completely_inside = false;
    while (OB_SUCC(ret) && idx < sz) {
      ObBoxPosition pos = get_position(line[idx].get<0>(), line[idx].get<1>());
      if (pos == ObBoxPosition::OUTSIDE) {
        if (OB_FAIL(line_visit_outside(line, idx, mls, pos, new_line))) {
          LOG_WARN("fail to do line visit outside", K(ret), K(pos), K(idx));
        }
      } else {  // edge or inside
        if (OB_FAIL(line_visit_inside_or_edge(line, idx, mls, pos, new_line, completely_inside))) {
          LOG_WARN("fail to do line visit inside or edge", K(ret), K(pos), K(idx));
        }
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit(ObCartesianLineString *geo)
{
  int ret = OB_SUCCESS;
  ObCartesianMultilinestring *mls = nullptr;
  bool inside = false;  // unused
  if (geo->is_empty()) {
    // do nothing
  } else if (OB_FAIL(line_visit(*geo, mls, inside))) {
    LOG_WARN("fail to do line visit", K(ret));
  } else if (!mls->empty()) {
    for (uint32_t i = 0; OB_SUCC(ret) && i < mls->size(); ++i) {
      if (OB_FAIL(res_geo_->push_back((*mls)[i]))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit(ObCartesianMultilinestring *geo)
{
  int ret = OB_SUCCESS;
  ObCartesianMultilinestring *mls = nullptr;
  bool inside = false;  // unused
  if (!geo->is_empty()) {
    for (uint32_t i = 0; OB_SUCC(ret) && i < geo->size(); ++i) {
      if (OB_FAIL(line_visit((*geo)[i], mls, inside))) {
        LOG_WARN("fail to do line visit", K(ret));
      }
    }
    if (OB_SUCC(ret) && !mls->empty()) {
      for (uint32_t i = 0; OB_SUCC(ret) && i < mls->size(); ++i) {
        if (OB_FAIL(res_geo_->push_back((*mls)[i]))) {
          LOG_WARN("fail to push back geometry", K(ret));
        }
      }
    }
  }
  return ret;
}

// After cutting the ring in the POLYGON as a line, we may get a MULTILINESTRING.
// Try to connect the first and last lines of the MULTILINESTRING
// e.g. POLYGON ((5 10,0 0,10 0,5 10)) --visit_line--> MULTILINESTRING ((5 10,0 0),(10 0,5 10))
//      MULTILINESTRING ((5 10,0 0),(10 0,5 10)) --reconnect--> LINESTRING (10 0,5 10,0 0)
int ObGeoBoxClipVisitor::reconnect_multi_line(ObCartesianMultilinestring &mls)
{
  int ret = OB_SUCCESS;
  if (mls.size() < 2) {
    // do nothing
  } else {
    ObCartesianLineString &first = mls.front();
    ObCartesianLineString &last = mls[mls.size() - 1];
    ObWkbGeomInnerPoint prev_pt = last[last.size() - 1];
    if (!first.empty() && !last.empty() && first[0].equals(last.back())) {
      for (uint32_t i = 0; OB_SUCC(ret) && i < first.size(); ++i) {
        if (!first[i].equals(prev_pt)) {
          prev_pt = first[i];
          if (OB_FAIL(last.push_back(prev_pt))) {
            LOG_WARN("fail to push back geometry", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        mls[0] = last;
        mls.resize(mls.size() - 1);
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::box_to_linearring(ObCartesianLinearring &ring)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(xmin_, ymin_)))) {
    LOG_WARN("failt to push back geometry", K(ret));
  } else if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(xmin_, ymax_)))) {
    LOG_WARN("failt to push back geometry", K(ret));
  } else if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(xmax_, ymax_)))) {
    LOG_WARN("failt to push back geometry", K(ret));
  } else if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(xmax_, ymin_)))) {
    LOG_WARN("failt to push back geometry", K(ret));
  } else if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(xmin_, ymin_)))) {
    LOG_WARN("failt to push back geometry", K(ret));
  }
  return ret;
}

void ObGeoBoxClipVisitor::to_next_edge(ObBoxPosition &pos)
{
  switch (pos) {
    case ObBoxPosition::LEFT_EDGE:
    case ObBoxPosition::BOTTOMLEFT_CORNER: {
      pos = ObBoxPosition::TOP_EDGE;
      break;
    }
    case ObBoxPosition::TOP_EDGE:
    case ObBoxPosition::TOPLEFT_CORNER: {
      pos = ObBoxPosition::RIGHT_EDGE;
      break;
    }
    case ObBoxPosition::RIGHT_EDGE:
    case ObBoxPosition::TOPRIGHT_CORNER: {
      pos = ObBoxPosition::BOTTOM_EDGE;
      break;
    }
    case ObBoxPosition::BOTTOM_EDGE:
    case ObBoxPosition::BOTTOMRIGHT_CORNER: {
      pos = ObBoxPosition::LEFT_EDGE;
      break;
    }
    default: {
      // do nothing
    }
  }
}

/**
 * @brief Close the line in the Box into a ring by adding points
 * @param ring line to close
 * @param x1 The x-coordinate of the starting point of the ring
 * @param y1 The y-coordinate of the starting point of the ring
 * @param x2 The x-coordinate of the ending point of the ring
 * @param y2 The y-coordinate of the ending point of the ring
 * @return int ret code
 */
int ObGeoBoxClipVisitor::close_ring(
    ObCartesianLinearring &ring, double x1, double y1, double x2, double y2)
{
  int ret = OB_SUCCESS;
  ObBoxPosition pos = get_position(x1, y1);
  ObBoxPosition end_pos = get_position(x2, y2);
  bool is_closed = false;
  while (OB_SUCC(ret) && !is_closed) {
    // closed when starting point and ending point on the same edge
    // and points are in correct clockwise order
    is_closed = (pos & end_pos)
                && ((x1 == xmin_ && y2 >= y1) || (y1 == ymax_ && x2 >= x1)
                    || (x1 == xmax_ && y2 <= y1) || (y1 == ymin_ && x2 <= x1));
    if (is_closed) {
      if (x1 != x2 || y1 != y2) {
        if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(x2, y2)))) {
          LOG_WARN("fail to push back geometry", K(ret));
        }
      }
    } else if (pos & ObBoxPosition::OUTSIDE || end_pos & ObBoxPosition::OUTSIDE
               || pos & ObBoxPosition::INSIDE || end_pos & ObBoxPosition::INSIDE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("point should be on boundary", K(ret));
    } else {
      to_next_edge(pos);
      if (pos & ObBoxPosition::LEFT_EDGE) {
        x1 = xmin_;
      } else if (pos & ObBoxPosition::TOP_EDGE) {
        y1 = ymax_;
      } else if (pos & ObBoxPosition::RIGHT_EDGE) {
        x1 = xmax_;
      } else {
        y1 = ymin_;
      }
      if (OB_FAIL(ring.push_back(ObWkbGeomInnerPoint(x1, y1)))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief Distance of first point of linestring to last point of ring along rectangle edges
 * @param x1 x-coordinate of last point of ring
 * @param y1 y-coordinate of last point of ring
 * @param x2 x-coordinate of first point of linestring
 * @param y2 y-coordinate of first point of linestring
 * @param dist distance
 * @return int ret code
 */
int ObGeoBoxClipVisitor::distance(double x1, double y1, double x2, double y2, double &dist)
{
  int ret = OB_SUCCESS;
  ObBoxPosition pos = get_position(x1, y1);
  ObBoxPosition end_pos = get_position(x2, y2);
  bool is_end = false;
  while (OB_SUCC(ret) && !is_end) {
    is_end = (pos & end_pos)
             && ((x1 == xmin_ && y2 >= y1) || (y1 == ymax_ && x2 >= x1) || (x1 == xmax_ && y2 <= y1)
                 || (y1 == ymin_ && x2 <= x1));
    if (is_end) {
      dist += fabs(x1 - x2) + fabs(y1 - y2);
    } else if (pos & ObBoxPosition::OUTSIDE || end_pos & ObBoxPosition::OUTSIDE
               || pos & ObBoxPosition::INSIDE || end_pos & ObBoxPosition::INSIDE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("point should be on boundary", K(ret));
    } else {
      to_next_edge(pos);
      if (pos & ObBoxPosition::LEFT_EDGE) {
        dist += x1 - xmin_;
        x1 = xmin_;
      } else if (pos & ObBoxPosition::TOP_EDGE) {
        dist += ymax_ - y1;
        y1 = ymax_;
      } else if (pos & ObBoxPosition::RIGHT_EDGE) {
        dist += xmax_ - x1;
        x1 = xmax_;
      } else {
        dist += y1 - ymin_;
        y1 = ymin_;
      }
    }
  }
  return ret;
}

void ObGeoBoxClipVisitor::reverse_ring(ObCartesianLineString &ring, uint32_t start, uint32_t end)
{
  for (uint32_t i = start, j = end; i < j; ++i, --j) {
    ObWkbGeomInnerPoint tmp_pt = ring[i];
    ring[i] = ring[j];
    ring[j] = tmp_pt;
  }
}

void ObGeoBoxClipVisitor::reorder_ring(ObCartesianLinearring &ring)
{
  int32_t min_pos = 0;
  // find the point closest to the bottom left
  for (uint32_t i = 1; i < ring.size(); ++i) {
    if (ring[i].get<0>() < ring[min_pos].get<0>()) {
      min_pos = i;
    } else if (ring[i].get<0>() == ring[min_pos].get<0>()
               && ring[i].get<1>() < ring[min_pos].get<1>()) {
      min_pos = i;
    }
  }
  if (min_pos != 0) {
    // reorder ring, let the min_pos point be the starting point
    reverse_ring(ring, 0, min_pos - 1);
    reverse_ring(ring, min_pos, ring.size() - 2);
    reverse_ring(ring, 0, ring.size() - 2);
    // make sure that the first point and end point are same (closed ring)
    ring.back() = ring.front();
  }
}

int ObGeoBoxClipVisitor::make_polygon_ext_ring(ObCartesianMultilinestring &mls, ObCartesianMultipolygon &new_mpy)
{
  int ret = OB_SUCCESS;
  if (mls.empty()) {
    ObCartesianPolygon poly;
    if (OB_FAIL(box_to_linearring(poly.exterior_ring()))) {
      LOG_WARN("fail to convert box to ring", K(ret));
    } else if (OB_FAIL(new_mpy.push_back(poly))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  } else {
    ObCartesianLinearring ring;
    while (OB_SUCC(ret) && (!mls.empty() || !ring.empty())) {
      double dist = 0.0;
      if (ring.empty()) {
        for (int i = 0; i < mls[0].size() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(ring.push_back(mls[0][i]))) {
            LOG_WARN("failt to push back geometry", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          mls.pop_front();
        }
      }
      if (OB_SUCC(ret)
          && OB_FAIL(distance(ring.back().get<0>(), ring.back().get<1>(), ring.front().get<0>(),
              ring.front().get<1>(), dist))) {
        LOG_WARN("fail to get distance", K(ret));
      }
      double min_dist = -1;
      int32_t min_pos = 0;
      for (int i = 0; OB_SUCC(ret) && i < mls.size(); ++i) {
        double cur_dist = 0.0;
        if (OB_FAIL(distance(ring.back().get<0>(), ring.back().get<1>(),
                mls[i][0].get<0>(), mls[i][0].get<1>(), cur_dist))) {
          LOG_WARN("fail to get distance", K(ret), K(i), K(mls.size()));
        } else if (min_dist < 0 || cur_dist < min_dist) {
          min_dist = cur_dist;
          min_pos = i;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (min_dist < 0 || dist < min_dist) {
        if (OB_FAIL(close_ring(ring, ring.back().get<0>(), ring.back().get<1>(),
                ring.front().get<0>(), ring.front().get<1>()))) {
          LOG_WARN("fail to close ring", K(ret));
        } else if (FALSE_IT(reorder_ring(ring))) {
        } else if (ring.size() < 4) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("Invalid number of ring", K(ret), K(ring.size()));
        } else {
          ObCartesianPolygon poly;
          poly.exterior_ring() = ring;
          if (OB_FAIL(new_mpy.push_back(poly))) {
            LOG_WARN("fail to push back geometry", K(ret));
          }
          ring.clear();
        }
      } else {
        ObCartesianLineString &min_line = mls[min_pos];
        if (OB_FAIL(close_ring(ring, ring.back().get<0>(), ring.back().get<1>(),
                min_line.front().get<0>(), min_line.front().get<1>()))) {
          LOG_WARN("fail to close ring", K(ret));
        }
        for (uint32_t i = 1; OB_SUCC(ret) && i < min_line.size(); ++i) {
          if (OB_FAIL(ring.push_back(min_line[i]))) {
            LOG_WARN("fail to push back geometry", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(mls.remove(min_pos))) {
          LOG_WARN("fail to remove linestring", K(ret));
        }
      }
    }
  }
  return ret;
}

// mls -> ext rings
// mpy -> inner rings
int ObGeoBoxClipVisitor::make_polygons(
    ObCartesianMultilinestring &mls, ObCartesianMultipolygon &mpy, ObCartesianMultipolygon &new_mpy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(make_polygon_ext_ring(mls, new_mpy))) {
    LOG_WARN("fail to make polygon exterior ring", K(ret));
  }
  // merge mpy (inner rings) to new_mpy
  for (uint32_t i = 0; OB_SUCC(ret) && i < mpy.size(); ++i) {
    ObCartesianPolygon &poly = mpy[i];
    ObCartesianLinearring &ext_ring = poly.exterior_ring();
    if (new_mpy.size() == 1) {
      if (OB_FAIL(new_mpy[0].interior_rings().push_back(ext_ring))) {
        LOG_WARN("fail to push back linearring", K(ret));
      }
    } else {
      for (uint32_t j = 0; OB_SUCC(ret) && j < new_mpy.size(); ++j) {
        bool is_covered_by = false;
        ObGeoEvalCtx gis_context(allocator_);
        ObCartesianLineString *tmp_line = reinterpret_cast<ObCartesianLineString *>(&ext_ring);
        if (OB_FAIL(gis_context.append_geo_arg(tmp_line))
            || OB_FAIL(gis_context.append_geo_arg(&new_mpy[j]))) {
          LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::CoveredBy>::geo_func::eval(
                       gis_context, is_covered_by))) {
          LOG_WARN("eval Within functor failed", K(ret));
        } else if (is_covered_by && OB_FAIL(new_mpy[j].interior_rings().push_back(ext_ring))) {
          LOG_WARN("fail to push back linearring", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObGeoBoxClipVisitor::reverse_mls(ObCartesianMultilinestring &mls)
{
  for (uint32_t i = 0; i < mls.size(); ++i) {
    ObCartesianLineString &ls = mls[i];
    reverse_ring(mls[i], 0, mls[i].size() - 1);
  }

  for (uint32_t i = 0, j = mls.size() - 1; i < j; ++i, --j) {
    ObCartesianLineString ls = mls[i];
    mls[i] = mls[j];
    mls[j] = ls;
  }
}

// check is ring in counter-clock-wise
bool ObGeoBoxClipVisitor::is_ring_ccw(ObCartesianLineString &ring)
{
  bool bret = false;
  int64_t sz = ring.size();
  // a ring at least has 4 points, including close point
  if (sz >= 4) {
    const ObWkbGeomInnerPoint *up_high_pt = &ring[0];
    const ObWkbGeomInnerPoint *up_low_pt = nullptr;
    double prev_y = up_high_pt->get<1>();
    uint32_t up_high_idx = 0;
    for (uint32_t i = 1; i < sz; ++i) {
      double cur_y = ring[i].get<1>();
      if (cur_y > prev_y && cur_y >= up_high_pt->get<1>()) {
        up_high_idx = i;
        up_high_pt = &ring[i];
        up_low_pt = &ring[i - 1];
      }
      prev_y = cur_y;
    }
    if (up_high_idx != 0) {
      --sz;
      uint32_t down_low_idx = (up_high_idx + 1) % sz;
      while (down_low_idx != up_high_idx && ring[down_low_idx].get<1>() == up_high_pt->get<1>()) {
        down_low_idx = (down_low_idx + 1) % sz;
      }
      const ObWkbGeomInnerPoint *down_low_pt = &ring[down_low_idx];
      uint32_t down_high_idx = down_low_idx > 0 ? down_low_idx - 1 : sz - 1;
      const ObWkbGeomInnerPoint *down_high_pt = &ring[down_high_idx];
      if (up_high_pt->equals(*down_high_pt)) {
        if (!up_low_pt->equals(*up_high_pt) && !down_low_pt->equals(*up_high_pt)
            && !up_low_pt->equals(*down_low_pt)) {
          double diff_x1 = up_high_pt->get<0>() - up_low_pt->get<0>();
          double diff_x2 = down_low_pt->get<0>() - up_high_pt->get<0>();
          double diff_y1 = up_high_pt->get<1>() - up_low_pt->get<1>();
          double diff_y2 = down_low_pt->get<1>() - up_high_pt->get<1>();
          bret = (diff_x1 * diff_y2 - diff_x2 * diff_y1) > 0;
        }
      } else {
        bret = (down_high_pt->get<0>() - up_high_pt->get<0>()) < 0;
      }
    }
  }
  return bret;
}

int ObGeoBoxClipVisitor::visit_polygon_inner_ring(
    const ObCartesianLinearring &inner_ring, ObCartesianMultipolygon &ext_mpy, ObCartesianMultilinestring &ext_mls, bool &is_within)
{
  int ret = OB_SUCCESS;
  ObCartesianMultilinestring *inner_mls = nullptr;
  bool is_inside = false;
  ObCartesianPolygon tmp_py;
  tmp_py.exterior_ring() = inner_ring;
  if (OB_FAIL(line_visit(inner_ring, inner_mls, is_inside))) {
    LOG_WARN("fail to do line visit", K(ret));
  } else if (is_inside) {
    // becomes exterior ring
    if (OB_FAIL(ext_mpy.push_back(tmp_py))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  } else {
    if (inner_mls->empty()) {
      double xmid = xmin_ + (xmax_ - xmin_) / 2;
      double ymid = ymin_ + (ymax_ - ymin_) / 2;
      ObCartesianPoint pt(xmid, ymid);
      ObGeoEvalCtx gis_context(allocator_);
      if (OB_FAIL(gis_context.append_geo_arg(&pt))
          || OB_FAIL(gis_context.append_geo_arg(&tmp_py))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(
                     ObGeoFunc<ObGeoFuncType::Within>::gis_func::eval(gis_context, is_within))) {
        LOG_WARN("eval Within functor failed", K(ret));
      }
    } else {
      if (!is_ring_ccw(tmp_py.exterior_ring())) {
        reverse_mls(*inner_mls);
      }
      if (OB_FAIL(reconnect_multi_line(*inner_mls))) {
        LOG_WARN("fail to reconnect multi line", K(ret));
      }
      // merge inner_mls into ext_mls
      for (int i = 0; OB_SUCC(ret) && i < inner_mls->size(); ++i) {
        if (OB_FAIL(ext_mls.push_back((*inner_mls)[i]))) {
          LOG_WARN("failt to push back geometry", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        allocator_->free(inner_mls);
        inner_mls = nullptr;
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit_polygon_ext_ring(const ObCartesianLinearring &ext_ring,
    bool &is_inner_inside, bool &is_ext_inside, ObCartesianMultilinestring *&ext_mls)
{
  int ret = OB_SUCCESS;
  ObCartesianPolygon tmp_py;
  tmp_py.exterior_ring() = ext_ring;
  if (OB_FAIL(line_visit(tmp_py.exterior_ring(), ext_mls, is_ext_inside))) {
    LOG_WARN("fail to do line visit", K(ret));
  } else if (ext_mls->empty()) {
    // ext ring completely outside, but maybe inner ring is inside/on edge
    double xmid = xmin_ + (xmax_ - xmin_) / 2;
    double ymid = ymin_ + (ymax_ - ymin_) / 2;
    ObCartesianPoint pt(xmid, ymid);
    ObGeoEvalCtx gis_context(allocator_);
    if (OB_FAIL(gis_context.append_geo_arg(&pt)) || OB_FAIL(gis_context.append_geo_arg(&tmp_py))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Within>::gis_func::eval(
                   gis_context, is_inner_inside))) {
      LOG_WARN("eval Within functor failed", K(ret));
    }
  } else {
    if (is_ring_ccw(tmp_py.exterior_ring())) {
      reverse_mls(*ext_mls);
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit_polygon(ObCartesianPolygon &poly, ObCartesianMultipolygon *&mpy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mpy)) {
    mpy = OB_NEWx(ObCartesianMultipolygon, allocator_, poly.get_srid(), *allocator_);
    if (OB_ISNULL(mpy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for geometry", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObCartesianMultilinestring *ext_mls = nullptr;
    bool is_inner_inside = true;
    bool is_ext_inside = false;
    if (OB_FAIL(visit_polygon_ext_ring(poly.exterior_ring(), is_inner_inside, is_ext_inside, ext_mls))) {
      LOG_WARN("fail to visit polygon exterior ring", K(ret), K(is_inner_inside), K(is_ext_inside));
    } else if (!is_inner_inside) {
      // do nothing
    } else if (is_ext_inside) {
      // completely inside
      if (OB_FAIL(mpy->push_back(poly))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
    } else if (OB_FAIL(reconnect_multi_line(*ext_mls))) {
      LOG_WARN("fail to reconnect multi line", K(ret));
    } else {
      ObGeomVector<oceanbase::common::ObCartesianLinearring> &inner_rings = poly.interior_rings();
      ObCartesianMultipolygon tmp_mpy;
      bool is_within = false;
      for (uint32_t i = 0; OB_SUCC(ret) && i < inner_rings.size() && !is_within; ++i) {
        if (OB_FAIL(visit_polygon_inner_ring(inner_rings[i], tmp_mpy, *ext_mls, is_within))) {
          LOG_WARN("fail to visit polygon inner ring",
              K(ret),
              K(i),
              K(inner_rings.size()),
              K(is_within));
        }
      }
      if (OB_SUCC(ret) && !is_within) {
        ObCartesianMultipolygon new_mpy;
        if (OB_FAIL(make_polygons(*ext_mls, tmp_mpy, new_mpy))) {
          LOG_WARN("fail to make polygons", K(ret));
        }
        for (int i = 0; OB_SUCC(ret) && i < new_mpy.size(); ++i) {
          if (OB_FAIL(mpy->push_back(new_mpy[i]))) {
            LOG_WARN("fail to push back geometry", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit(ObCartesianPolygon *geo)
{
  int ret = OB_SUCCESS;
  ObCartesianMultipolygon *mpy = nullptr;
  if (geo->is_empty()) {
    // do nothing
  } else if (OB_FAIL(visit_polygon(*geo, mpy))) {
    LOG_WARN("fail to visit polygon", K(ret));
  } else if (!mpy->empty()) {
    for (uint32_t i = 0; OB_SUCC(ret) && i < mpy->size(); ++i) {
      if (OB_FAIL(res_geo_->push_back((*mpy)[i]))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
    }
  }
  return ret;
}

int ObGeoBoxClipVisitor::visit(ObCartesianMultipolygon *geo)
{
  int ret = OB_SUCCESS;
  if (!geo->is_empty()) {
    ObCartesianMultipolygon *mpy = nullptr;
    for (uint32_t i = 0; OB_SUCC(ret) && i < geo->size(); ++i) {
      if (OB_FAIL(visit_polygon((*geo)[i], mpy))) {
        LOG_WARN("fail to do line visit", K(ret));
      } else {
      }
    }
    if (OB_SUCC(ret) && !mpy->empty()) {
      for (uint32_t i = 0; OB_SUCC(ret) && i < mpy->size(); ++i) {
        if (OB_FAIL(res_geo_->push_back((*mpy)[i]))) {
          LOG_WARN("fail to push back geometry", K(ret));
        }
      }
    }
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase