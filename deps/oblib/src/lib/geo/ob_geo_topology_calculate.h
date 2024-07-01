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

#ifndef OCEANBASE_LIB_GEO_OB_TOPOLOGY_
#define OCEANBASE_LIB_GEO_OB_TOPOLOGY_

#include "lib/string/ob_string.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase {
namespace common {

// point relative position to line
enum PointPosition {
  LEFT = 0,
  RIGHT = 1,
  ON = 2,
  UNKNOWN = 3,
};

enum LineIntersect {
  NO_INTERSCT = 0,
  POINT_INTERSECT = 1,
  END_POINT_INTERSECT = 2, // Intersects at a endpoint.
  LINE_INTERSECT = 3,
};

class ObGeoTopology {
public:
  static LineIntersect calculate_point_inersect_horizontally(const ObPoint2d &start, const ObPoint2d &end,
                                                             const ObPoint2d &target, bool &is_on_boundary);
  static PointPosition calculate_point_position(const ObPoint2d &start, const ObPoint2d &end,
                                                const ObPoint2d &target);
  static int calculate_segment_intersect(const ObPoint2d &start0, const ObPoint2d &end0,
                                         const ObPoint2d &start1, const ObPoint2d &end1,
                                         LineIntersect &res);
  static int calculate_line_segments_intersect(const ObLineSegment& seg1, const ObLineSegment& seg2,
                                               LineIntersect &res);
  static bool is_same(const ObPoint2d &point, const ObPoint2d &target) {return point.x == target.x && point.y == target.y;}
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_TOPOLOGY_
