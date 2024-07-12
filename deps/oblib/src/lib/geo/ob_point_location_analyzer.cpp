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
#include "ob_point_location_analyzer.h"
#include "ob_geo_topology_calculate.h"

namespace oceanbase {
namespace common {

int ObPointLocationAnalyzer::calculate_point_position(const ObPoint2d &test_point)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_geo_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("rtree index or cache geo is null", K(cache_geo_), K(ret));
  } else if (!rtree_index_.is_built()) {
    if (OB_ISNULL(cache_geo_->get_segments())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null.", K(ret));
    } else if (OB_FAIL(rtree_index_.construct_rtree_index(*(cache_geo_->get_segments())))) {
      LOG_WARN("construct rtree index failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    clear_result();
    std::vector<RtreeNodeValue> res;
    ObCartesianBox box;
    box.set_box(std::min(cache_geo_->get_x_min() - 1.0, test_point.x), test_point.y,
                std::max(cache_geo_->get_x_max() + 1.0, test_point.x), test_point.y);
    if (OB_FAIL(rtree_index_.query(QueryRelation::INTERSECTS, box, res))) {
      LOG_WARN("failed to query rtree", K(ret));
    } else {
      bool is_on_boundary = false;
      for (uint32_t i = 0; i < res.size() && OB_SUCC(ret) && !is_on_boundary; i++) {
        const ObSegment* seg1 = res[i].second;
        if (seg1 != NULL
            && LineIntersect::POINT_INTERSECT == ObGeoTopology::calculate_point_inersect_horizontally(seg1->begin, seg1->end,
                                                                                                      test_point, is_on_boundary)) {
          intersect_cnt_++;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_on_boundary) {
          position_ = ObPointLocation::BOUNDARY;
        } else if ((intersect_cnt_ % 2) == 1) {
          position_ = ObPointLocation::INTERIOR;
        } else {
          position_ = ObPointLocation::EXTERIOR;
        }
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase