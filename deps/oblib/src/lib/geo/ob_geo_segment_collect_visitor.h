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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_SEGMENT_COLLECT_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_SEGMENT_COLLECT_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_cache.h"

namespace oceanbase
{
namespace common
{

class ObGeoSegmentCollectVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoSegmentCollectVisitor(ObLineSegments *segments)
  : line_segments_(segments),
    segments_(nullptr),
    is_collect_mono_(true) {}
  ObGeoSegmentCollectVisitor(ObSegments *segments)
  : line_segments_(nullptr),
    segments_(segments),
    is_collect_mono_(false) {}

  virtual ~ObGeoSegmentCollectVisitor() {}
  bool prepare(ObGeometry *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeomLinearRing *geo);
  int visit(ObIWkbGeogLinearRing *geo);
  int visit(ObIWkbGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  bool is_end(ObIWkbGeomLineString *geo) override { UNUSED(geo); return true;}
  bool is_end(ObIWkbGeogLineString *geo) override { UNUSED(geo); return true;}
  bool is_end(ObIWkbGeomLinearRing *geo) override { UNUSED(geo); return true;}
  bool is_end(ObIWkbGeogLinearRing *geo) override { UNUSED(geo); return true;}
  bool set_after_visitor() { return false; }
private:
  template<typename T_IBIN>
  int collect_line_segment(T_IBIN *geo);
  template<typename T_IBIN>
  int collect_segment(T_IBIN *geo);
  ObLineSegments *line_segments_;
  ObSegments *segments_;
  bool is_collect_mono_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoSegmentCollectVisitor);
};

} // namespace common
} // namespace oceanbase

#endif