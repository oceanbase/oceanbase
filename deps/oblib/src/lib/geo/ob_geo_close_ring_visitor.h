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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_CLOSE_RING_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_CLOSE_RING_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace common
{

class ObGeoCloseRingVisitor : public ObEmptyGeoVisitor
{
public:
  // need_convert: Configure whether conversion is required when srs type is GEOGRAPHIC_SRS.
  explicit ObGeoCloseRingVisitor() {}
  virtual ~ObGeoCloseRingVisitor() {}
  bool prepare(ObGeometry *geo) override { return geo != nullptr; }

  // tree
  int visit(ObPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }

  int visit(ObGeometrycollection *geo) { UNUSED(geo); return OB_SUCCESS; }

  int visit(ObGeographLinearring *geo) override;
  int visit(ObCartesianLinearring *geo) override;

  int visit(ObMultipolygon *geo) { UNUSED(geo); return OB_SUCCESS; }

  bool is_end(ObLinearring *geo) override { UNUSED(geo); return true; }

private:
  template<typename PolyTree>
  int visit_poly(PolyTree *geo);
  DISALLOW_COPY_AND_ASSIGN(ObGeoCloseRingVisitor);
};

} // namespace common
} // namespace oceanbase

#endif