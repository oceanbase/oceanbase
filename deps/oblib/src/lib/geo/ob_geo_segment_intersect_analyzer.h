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

#ifndef OCEANBASE_LIB_GEO_SEG_INTERSECTS_ANALYZER_
#define OCEANBASE_LIB_GEO_SEG_INTERSECTS_ANALYZER_

#include "lib/geo/ob_geo_cache.h"
#include "ob_geo_topology_calculate.h"
#include "ob_geo_rstar_tree.h"

namespace oceanbase {
namespace common {
class ObLineIntersectionAnalyzer {
public:
  typedef std::pair<ObCartesianBox, ObLineSegment *> RtreeNodeValue;
public:
  ObLineIntersectionAnalyzer(ObCachedGeomBase *cache_geo, ObRstarTree<ObLineSegment> &rtree_index)
    : cache_geo_(cache_geo),
      rtree_index_(rtree_index),
      flags_(0) {}

  virtual ~ObLineIntersectionAnalyzer() {}
  int segment_intersection_query(ObGeometry *geo);
  bool is_intersects() { return is_intersect_; }
  void set_intersects_analyzer_type(bool check_all_intesect) { check_all_intesect_ = check_all_intesect;}
  void reset_flag() { flags_ = 0; }
  bool is_check_done(LineIntersect inter_res);
  bool has_external_intersects() { return check_all_intesect_ && has_external_intersect_;}
  bool has_internal_intersects() { return check_all_intesect_ && has_internal_intersect_;}
  bool set_after_visitor() { return false; }
private:
  ObCachedGeomBase *cache_geo_;
  ObRstarTree<ObLineSegment> &rtree_index_;
  union {
    struct {
      uint8_t is_intersect_ : 1;
      uint8_t has_internal_intersect_ : 1; // the intersection point is a endpoints
      uint8_t has_external_intersect_ : 1; // the intersection point is not endpoints
      uint8_t check_all_intesect_: 1;      // need to distinguish intersection types
      uint8_t reserved_ : 4;
    };

    uint8_t flags_;
  };
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_SEG_INTERSECTS_ANALYZER_
