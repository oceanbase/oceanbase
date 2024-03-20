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
#include "ob_geo_segment_intersect_analyzer.h"
#include "ob_geo_segment_collect_visitor.h"

namespace oceanbase {
namespace common {

bool ObLineIntersectionAnalyzer::is_check_done(LineIntersect inter_res)
{
  bool res = false;
  if (check_all_intesect_) {
    res = has_internal_intersect_ && has_external_intersect_;
  } else {
    res = is_intersect_;
  }
  return res;
}

int ObLineIntersectionAnalyzer::segment_intersection_query(ObGeometry *geo)
{
  int ret = OB_SUCCESS;
  is_intersect_ = false;
  if (!rtree_index_.is_built()) {
    ObLineSegments* line_segs = cache_geo_->get_line_segments();
    if (OB_ISNULL(line_segs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null", K(ret));
    } else if (OB_FAIL(rtree_index_.construct_rtree_index(line_segs->segs_))) {
      LOG_WARN("construct rtree index failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObLineSegments input_segments;
    ObGeoSegmentCollectVisitor seg_visitor(&input_segments);
    if (OB_FAIL(geo->do_visit(seg_visitor))) {
      LOG_WARN("failed to get input segment", K(ret));
    } else {
      LineIntersect inter_res = LineIntersect::NO_INTERSCT;
      // query rtree to check intersesctiion
      for (uint32_t i = 0; i < input_segments.segs_.size() && OB_SUCC(ret) && !is_check_done(inter_res); i++) {
        ObCartesianBox box;
        std::vector<RtreeNodeValue> res;
        inter_res = LineIntersect::NO_INTERSCT;
        if (OB_FAIL(input_segments.segs_[i].get_box(box))) {
          LOG_WARN("failed to get segment box", K(ret));
        } else if (OB_FAIL(rtree_index_.query(QueryRelation::INTERSECTS, box, res))) {
          LOG_WARN("failed to query rtree", K(ret));
        } else {
          for (uint32_t j = 0; j < res.size() && OB_SUCC(ret) && !is_check_done(inter_res); j++) {
            const ObLineSegment* seg1 = res[j].second;
            if (OB_FAIL(ObGeoTopology::calculate_line_segments_intersect(*seg1, input_segments.segs_[i], inter_res))) {
              LOG_WARN("failed to get segment box", K(ret));
            } else if (inter_res != LineIntersect::NO_INTERSCT) {
              is_intersect_ = true;
              if (inter_res == LineIntersect::POINT_INTERSECT) {
                has_external_intersect_ = 1;
              } else {
                has_internal_intersect_ = 1;
              }
            }
          }
        }
      }
    }

  }
  return ret;
}

} // namespace common
} // namespace oceanbase