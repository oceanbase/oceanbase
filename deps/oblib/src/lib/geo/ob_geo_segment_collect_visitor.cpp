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
#include "ob_geo_segment_collect_visitor.h"

namespace oceanbase {
namespace common {

bool ObGeoSegmentCollectVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo)) {
    bret = false;
  }
  return bret;
}

template<typename T_IBIN>
int ObGeoSegmentCollectVisitor::collect_line_segment(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  bool is_new_start = true;
  uint32_t start_idx = 0;
  uint32_t end_idx = 0;
  ObPoint2d start;
  ObPoint2d end;
  QuadDirection last_dir = QuadDirection::INVALID_QUAD;
  const typename T_IBIN::value_type *line = reinterpret_cast<const typename T_IBIN::value_type*>(geo->val());
  int64_t node_num = geo->size();
  typename T_IBIN::value_type::iterator iter = line->begin();
  typename T_IBIN::value_type::iterator iter_after = line->begin() + 1;
  for (int64_t i = 0; i < node_num - 1 && OB_SUCC(ret); ++i, ++iter, ++iter_after) {
    QuadDirection curr_dir = QuadDirection::INVALID_QUAD;
    start.x = iter->template get<0>();
    start.y = iter->template get<1>();
    end.x = iter_after->template get<0>();
    end.y = iter_after->template get<1>();
    if (start.x == end.x && start.y == end.y) {
      // do nothing
    } else if (OB_FAIL(line_segments_->verts_.push_back(start))) {
      LOG_WARN("failed to append segment", K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_quadrant_direction(start, end, curr_dir))) {
      LOG_WARN("failed to get point direction", K(ret), K(start.x), K(start.y), K(end.x), K(end.y));
    } else {
      if (is_new_start) {
        is_new_start  = false;
        last_dir = curr_dir;
        start_idx = line_segments_->verts_.size() - 1;
      } else if (curr_dir != last_dir) {
        // generate new line segment
        ObLineSegment seg;
        seg.verts = &line_segments_->verts_;
        seg.begin = start_idx;
        seg.end = line_segments_->verts_.size() - 1;
        // last end point is current start point
        start_idx = seg.end;
        last_dir = curr_dir;
        if (OB_FAIL(line_segments_->segs_.push_back(seg))) {
          LOG_WARN("failed to append segment", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && line_segments_->verts_.size() > 0) {
    uint32_t last_idx = line_segments_->verts_.size() - 1;
    if (line_segments_->verts_[last_idx].x == end.x && line_segments_->verts_[last_idx].y == end.y) {
      // do nothing
    } else if (OB_FAIL(line_segments_->verts_.push_back(end))) {
      LOG_WARN("failed to append segment", K(ret));
    }
    if (OB_SUCC(ret) && start_idx < line_segments_->verts_.size() - 1) {
    // generate last new line segment
      ObLineSegment seg;
      seg.verts = &line_segments_->verts_;
      seg.begin = start_idx;
      seg.end = line_segments_->verts_.size() - 1;
      // last end point is current start point
      start_idx = seg.end;
      if (OB_FAIL(line_segments_->segs_.push_back(seg))) {
        LOG_WARN("failed to append segment", K(ret));
      }
    }
  }
  return ret;
}


template<typename T_IBIN>
int ObGeoSegmentCollectVisitor::collect_segment(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  const typename T_IBIN::value_type *line = reinterpret_cast<const typename T_IBIN::value_type*>(geo->val());
  uint32_t node_num = geo->size();
  typename T_IBIN::value_type::iterator iter = line->begin();
  typename T_IBIN::value_type::iterator iter_after = line->begin() + 1;
  for (uint32_t i = 0; i < node_num - 1 && OB_SUCC(ret); ++i, ++iter, ++iter_after) {
    ObSegment seg;
    seg.begin.x = iter->template get<0>();
    seg.begin.y = iter->template get<1>();
    seg.end.x = iter_after->template get<0>();
    seg.end.y = iter_after->template get<1>();
    if (seg.begin.x == seg.end.x && seg.begin.y == seg.end.y) {
      // do nothing
    } else if (OB_FAIL(segments_->push_back(seg))) {
      LOG_WARN("failed to append segment", K(ret));
    }
  }
  return ret;
}

int ObGeoSegmentCollectVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;
  if (is_collect_mono_) {
    if (OB_ISNULL(line_segments_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("line segments is null", K(ret));
    } else if (OB_FAIL(collect_line_segment(geo))) {
      LOG_WARN("failed to collect line segment", K(ret));
    }
  } else if (OB_ISNULL(segments_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("line segments is null", K(ret));
  } else if (OB_FAIL(collect_segment(geo))) {
    LOG_WARN("failed to collect segment", K(ret));
  }
  return ret;
}

int ObGeoSegmentCollectVisitor::visit(ObIWkbGeogLineString *geo)
{
  int ret = OB_SUCCESS;
  if (is_collect_mono_) {
    if (OB_ISNULL(line_segments_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("line segments is null", K(ret));
    } else if (OB_FAIL(collect_line_segment(geo))) {
      LOG_WARN("failed to collect line segment", K(ret));
    }
  } else if (OB_ISNULL(segments_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("line segments is null", K(ret));
  } else if (OB_FAIL(collect_segment(geo))) {
    LOG_WARN("failed to collect segment", K(ret));
  }
  return ret;
}

int ObGeoSegmentCollectVisitor::visit(ObIWkbGeomLinearRing *geo)
{
  int ret = OB_SUCCESS;
  if (is_collect_mono_) {
    if (OB_ISNULL(line_segments_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("line segments is null", K(ret));
    } else if (OB_FAIL(collect_line_segment(geo))) {
      LOG_WARN("failed to collect line segment", K(ret));
    }
  } else if (OB_ISNULL(segments_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("line segments is null", K(ret));
  } else if (OB_FAIL(collect_segment(geo))) {
    LOG_WARN("failed to collect segment", K(ret));
  }
  return ret;
}

int ObGeoSegmentCollectVisitor::visit(ObIWkbGeogLinearRing *geo)
{
  int ret = OB_SUCCESS;
  if (is_collect_mono_) {
    if (OB_ISNULL(line_segments_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("line segments is null", K(ret));
    } else if (OB_FAIL(collect_line_segment(geo))) {
      LOG_WARN("failed to collect line segment", K(ret));
    }
  } else if (OB_ISNULL(segments_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("line segments is null", K(ret));
  } else if (OB_FAIL(collect_segment(geo))) {
    LOG_WARN("failed to collect segment", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase