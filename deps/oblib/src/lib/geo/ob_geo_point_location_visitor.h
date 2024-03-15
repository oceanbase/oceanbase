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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_POINT_LOCATION_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_POINT_LOCATION_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_cache.h"

namespace oceanbase
{
namespace common
{
class ObGeoPointLocationVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoPointLocationVisitor(ObPoint2d &test_point)
  : test_point_(test_point),
    point_location_(ObPointLocation::INVALID) {}
  virtual ~ObGeoPointLocationVisitor() {}
  bool prepare(ObGeometry *geo);
  int visit(ObIWkbGeomPolygon *geo);
  int visit(ObIWkbGeogPolygon *geo);
  int visit(ObIWkbPoint *geo);
  int visit(ObIWkbGeogMultiPoint *geo);
  int visit(ObIWkbGeomMultiPoint *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeomMultiLineString *geo);
  int visit(ObIWkbGeogMultiLineString *geo);

  int visit(ObIWkbGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  bool is_end(ObIWkbGeogLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObGeometry *geo) { UNUSED(geo); return (point_location_ == ObPointLocation::INTERIOR
                                                      || point_location_ == ObPointLocation::BOUNDARY); }
  inline ObPointLocation get_point_location() { return point_location_; }
  bool set_after_visitor() { return false; }

private:
  template<typename T>
  int calculate_ring_intersects_cnt(T &ext, uint32_t &intersects_cnt, bool &is_on_boundary);

  template <typename T_BIN, typename T_RINGS>
  int calculate_point_location_in_polygon(T_BIN *poly);
  template<typename T_IBIN>
  int calculate_point_location_in_linestring(T_IBIN *line);
  ObPoint2d &test_point_;
  ObPointLocation point_location_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoPointLocationVisitor);
};

} // namespace common
} // namespace oceanbase

#endif