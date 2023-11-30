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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_GRID_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_GRID_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{
class ObGeoGridVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObGeoGridVisitor(const ObGeoGrid *grid, bool use_floor = false)
      : last_x_(NAN),
        last_y_(NAN),
        grid_(grid),
        use_floor_(use_floor)
  {}

  virtual ~ObGeoGridVisitor() { }

  bool prepare(ObGeometry *geo) override;
  // wkb
  int visit(ObCartesianPoint *geo) override;
  int visit(ObCartesianLineString *geo) override;
  int visit(ObCartesianPolygon *geo) override;
  int visit(ObCartesianMultipoint *geo) override;
  int visit(ObCartesianMultilinestring *geo) override;
  int visit(ObCartesianMultipolygon *geo) override;
  int visit(ObCartesianGeometrycollection *geo) override;

  bool is_end(ObCartesianLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObCartesianPolygon *geo) override { UNUSED(geo); return true; }
  bool is_end(ObCartesianMultipoint *geo) override { UNUSED(geo); return true; }
  bool is_end(ObCartesianMultilinestring *geo) override { UNUSED(geo); return true; }
  bool is_end(ObCartesianMultipolygon *geo) override { UNUSED(geo); return true; }
  bool is_end(ObCartesianGeometrycollection *geo) override { UNUSED(geo); return true; }

  int finish(ObGeometry *geo) override;
private:
  static const uint32_t RING_MIN_POINT = 4;
  static const uint32_t LINESTRING_MIN_POINT = 2;

  template<typename POLYGON, typename RING>
  void polygon_visit(POLYGON &geo);
  template<typename LINE>
  void multi_point_visit(LINE &geo, int32_t min_point);
  template<typename POINT>
  void point_visit(POINT *geo);
  bool is_duplicate_point(double x, double y);
  void reset_duplicate_point();

  double last_x_;
  double last_y_;
  const ObGeoGrid *grid_;
  bool use_floor_;

  DISALLOW_COPY_AND_ASSIGN(ObGeoGridVisitor);
};
}  // namespace common
}  // namespace oceanbase

#endif