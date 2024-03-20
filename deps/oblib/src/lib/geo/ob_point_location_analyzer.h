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

#ifndef OCEANBASE_LIB_GEO_POINT_LOCATION_ANALYZER_
#define OCEANBASE_LIB_GEO_POINT_LOCATION_ANALYZER_

#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_rstar_tree.h"

namespace oceanbase {
namespace common {

class ObPointLocationAnalyzer {
public:
  typedef std::pair<ObCartesianBox, ObSegment *> RtreeNodeValue;
public:
  ObPointLocationAnalyzer(ObCachedGeomBase *cache_geo, ObRstarTree<ObSegment> &rtree_index)
    : cache_geo_(cache_geo),
      rtree_index_(rtree_index),
      position_(ObPointLocation::INVALID),
      intersect_cnt_(0) {}

  virtual ~ObPointLocationAnalyzer() {}
  int calculate_point_position(const ObPoint2d &test_point);
  ObPointLocation get_position() { return position_; }
  inline void clear_result() { position_ = ObPointLocation::INVALID; intersect_cnt_ = 0;}
  void update_farthest_position();
private:
  ObCachedGeomBase *cache_geo_;
  ObRstarTree<ObSegment> &rtree_index_;
  ObPointLocation position_;
  uint32_t intersect_cnt_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_POINT_LOCATION_ANALYZER_
