/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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