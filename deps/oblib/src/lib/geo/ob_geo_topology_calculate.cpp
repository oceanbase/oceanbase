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
#include "ob_geo_topology_calculate.h"

namespace oceanbase {
namespace common {

LineIntersect ObGeoTopology::calculate_point_inersect_horizontally(const ObPoint2d &start, const ObPoint2d &end,
                                                                   const ObPoint2d &target, bool &is_on_boundary)
{
  LineIntersect res = LineIntersect::NO_INTERSCT;
  if (start.x < target.x && end.x < target.x) {
    // line is on left of point
    res = LineIntersect::NO_INTERSCT;
  } else if (end.x == target.x && end.y == target.y) {
    res = LineIntersect::POINT_INTERSECT;
    is_on_boundary = true;
  } else if (start.y == end.y && start.y == target.y) {
    // line is horizontal
    if (target.x >= std::min(start.x, end.x) && target.x <= std::max(start.x, end.x)) {
      res = LineIntersect::POINT_INTERSECT;
      is_on_boundary = true;
    }
  } else if (((start.y > target.y) && (end.y <= target.y)) ||
            ((end.y > target.y) && (start.y <= target.y))) {
    PointPosition pos = calculate_point_position(start, end, target);
    if (pos == PointPosition::ON) {
      res = LineIntersect::POINT_INTERSECT;
      is_on_boundary = true;
    } else if ((pos == PointPosition::LEFT && start.y < end.y)
               || (pos == PointPosition::RIGHT && start.y > end.y)) {
      res = LineIntersect::POINT_INTERSECT;
    }
  }
  return res;
}

PointPosition ObGeoTopology::calculate_point_position(const ObPoint2d &start, const ObPoint2d &end,
                                                      const ObPoint2d &target)
{
  PointPosition res = PointPosition::UNKNOWN;
  double constexpr DP_SAFE_EPSILON =  1e-15;

  double detsum;
  double const detleft = (start.x - target.x) * (end.y - target.y);
  double const detright = (start.y - target.y) * (end.x - target.x);
  double const det = detleft - detright;

  if(detleft > 0.0) {
    if(detright <= 0.0) {
      if(det < 0) {
        res = PointPosition::RIGHT;
      } else if(det > 0) {
        res = PointPosition::LEFT;
      } else {
        res = PointPosition::ON;
      }
    } else {
      detsum = detleft + detright;
    }
  } else if (detleft < 0.0) {
    if(detright >= 0.0) {
      if(det < 0) {
        res = PointPosition::RIGHT;
      } else if(det > 0) {
        res = PointPosition::LEFT;
      } else {
        res = PointPosition::ON;
      }
    } else {
      detsum = -detleft - detright;
    }
  } else {
    if(det < 0) {
      res = PointPosition::RIGHT;
    } else if(det > 0) {
      res = PointPosition::LEFT;
    } else {
      res = PointPosition::ON;
    }
  }
  if (res == PointPosition::UNKNOWN) {
    double err_range = DP_SAFE_EPSILON * detsum;
    if((det >= err_range) || (-det >= err_range) || det == 0) {
      if(det < 0) {
        res = PointPosition::RIGHT;
      } else if(det > 0) {
        res = PointPosition::LEFT;
      } else {
        res = PointPosition::ON;
      }
    }
  }
  return res;
}
// start 0 is query box, start 1 is testline
int ObGeoTopology::calculate_segment_intersect(const ObPoint2d &start0, const ObPoint2d &end0,
                                               const ObPoint2d &start1, const ObPoint2d &end1,
                                               LineIntersect &res)
{
  int ret = OB_SUCCESS;
  // check box intersects
  ObCartesianBox box0(std::min(start0.x, end0.x), std::min(start0.y, end0.y),
                      std::max(start0.x, end0.x), std::max(start0.y, end0.y));
  ObCartesianBox box1(std::min(start1.x, end1.x), std::min(start1.y, end1.y),
                      std::max(start1.x, end1.x), std::max(start1.y, end1.y));
  if (!box0.Intersects(box1)) {
    res = LineIntersect::NO_INTERSCT;
  } else {
    PointPosition p1_start_pos = calculate_point_position(start0, end0, start1);
    PointPosition p1_end_pos = calculate_point_position(start0, end0, end1);
    if (p1_start_pos == PointPosition::UNKNOWN || p1_end_pos == PointPosition::UNKNOWN) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("got a double precision problem", K(ret), K(start0), K(end0),
                                                 K(start1), K(end1), K(p1_start_pos), K(p1_end_pos));
    } else if (p1_start_pos == p1_end_pos && p1_start_pos != PointPosition::ON) {
      // both points of line1 are on the same side of line0
      res = LineIntersect::NO_INTERSCT;
    } else {
      PointPosition p0_start_pos = calculate_point_position(start1, end1, start0);
      PointPosition p0_end_pos = calculate_point_position(start1, end1, end0);
      if (p0_start_pos == PointPosition::UNKNOWN || p0_end_pos == PointPosition::UNKNOWN) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("got a double precision problem", K(ret), K(start0), K(end0),
                                                  K(start1), K(end1), K(p0_start_pos), K(p0_end_pos));
      } else if (p0_start_pos == p0_end_pos && p0_start_pos != PointPosition::ON) {
        // both points of line0 are on the same side of line1
        res = LineIntersect::NO_INTERSCT;
      } else if (p0_start_pos == PointPosition::ON && p0_start_pos == p0_end_pos
                 && p0_start_pos == p1_end_pos && p0_start_pos == p1_start_pos) {
        // The two line segments coincide exactly.
        res = LineIntersect::LINE_INTERSECT;
      } else if (p0_start_pos == PointPosition::ON || p0_end_pos == PointPosition::ON
                 || p1_end_pos == PointPosition::ON || p1_start_pos == PointPosition::ON) {
        // at least exist a point on another line
        // needed in contains
        res = LineIntersect::END_POINT_INTERSECT;
      } else {
        res = LineIntersect::POINT_INTERSECT;
      }
    }
  }

  return ret;
}

int ObGeoTopology::calculate_line_segments_intersect(const ObLineSegment& seg1, const ObLineSegment& seg2,
                                                     LineIntersect &res)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (res == LineIntersect::LINE_INTERSECT || res == LineIntersect::POINT_INTERSECT || res == LineIntersect::END_POINT_INTERSECT) {
    // return res
  } else if (seg1.end - seg1.begin == 1 && seg2.end - seg2.begin == 1) {
    if (OB_FAIL(calculate_segment_intersect((*seg1.verts)[seg1.begin], (*seg1.verts)[seg1.end],
                                            (*seg2.verts)[seg2.begin], (*seg2.verts)[seg2.end],
                                            res))) {
      LOG_WARN("get segment intersect result failed", K(ret));
    }
  } else {
    uint32_t middle1 = (seg1.begin + seg1.end) / 2;
    uint32_t middle2 = (seg2.begin + seg2.end) / 2;
    if (middle1 > seg1.begin) {
      ObLineSegment forward_seg1 = seg1;
      forward_seg1.end = middle1;
      if (middle2 > seg2.begin) {
        ObLineSegment forward_seg2 = seg2;
        forward_seg2.end = middle2;
        if (OB_FAIL(calculate_line_segments_intersect(forward_seg1, forward_seg2, res))) {
          LOG_WARN("calculate_line_segments_intersect failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && middle2 < seg2.end) {
        ObLineSegment back_seg2 = seg2;
        back_seg2.begin = middle2;
        if (OB_FAIL(calculate_line_segments_intersect(forward_seg1, back_seg2, res))) {
          LOG_WARN("calculate_line_segments_intersect failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (middle1 < seg1.end) {
      ObLineSegment back_seg1 = seg1;
      back_seg1.begin = middle1;
      if (OB_SUCC(ret) && middle2 > seg2.begin) {
        ObLineSegment forward_seg2 = seg2;
        forward_seg2.end = middle2;
        if (OB_FAIL(calculate_line_segments_intersect(back_seg1, forward_seg2, res))) {
          LOG_WARN("calculate_line_segments_intersect failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && middle2 < seg2.end) {
        ObLineSegment back_seg2 = seg2;
        back_seg2.begin = middle2;
        if (OB_FAIL(calculate_line_segments_intersect(back_seg1, back_seg2, res))) {
          LOG_WARN("calculate_line_segments_intersect failed", K(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase