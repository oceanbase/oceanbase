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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_SIMPLIFY_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_SIMPLIFY_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{
class ObGeoSimplifyVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObGeoSimplifyVisitor(double tolerance = 0.0, bool keep_collapsed = false)
      : tolerance_(tolerance), keep_collapsed_(keep_collapsed)
  {}

  virtual ~ObGeoSimplifyVisitor()
  {}

  bool prepare(ObGeometry *geo) override;
  int visit(ObCartesianPoint *geo) override;
  int visit(ObCartesianLineString *geo) override;
  int visit(ObCartesianPolygon *geo) override;
  int visit(ObCartesianMultipoint *geo) override;
  int visit(ObCartesianMultilinestring *geo) override;
  int visit(ObCartesianMultipolygon *geo) override;
  int visit(ObCartesianGeometrycollection *geo) override;

  bool is_end(ObGeometry *geo) override
  {
    UNUSED(geo);
    return true;
  }

  int finish(ObGeometry *geo) override;

private:
  static const uint32_t RING_MIN_POINT = 4;
  static const uint32_t LINESTRING_MIN_POINT = 2;

  template<typename POLYGON, typename RING>
  int polygon_visit(POLYGON &geo);
  template<typename LINE>
  int multi_point_visit(LINE &geo, int32_t min_point);
  template<typename LINE>
  void quick_simplify(LINE &line, ObArray<bool> &kept_idx, int32_t &kept_sz, int32_t min_point, int32_t l,
      int32_t r, double tolerance);
  template<typename LINE>
  void find_split_point(LINE &line, int32_t l, int32_t r, double max_distance, int32_t &split_idx);

  double tolerance_;
  bool keep_collapsed_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoSimplifyVisitor);
};
}  // namespace common
}  // namespace oceanbase

#endif