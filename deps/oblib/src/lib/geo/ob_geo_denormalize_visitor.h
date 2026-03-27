/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_LIB_GEO_OB_GEO_DENORMALIZE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_DENORMALIZE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"
namespace oceanbase
{
namespace common
{

// NOTE: for st_transform only, which need denormalize cartesian tree geometry
//       the denormalize action for other gis expr is within wkb visitor
class ObGeoDeNormalizeVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoDeNormalizeVisitor() {}
  virtual ~ObGeoDeNormalizeVisitor() {}
  bool prepare(ObGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianPoint *geo);
  int visit(ObCartesianMultipoint *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianLineString *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianLinearring *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianMultilinestring *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianMultipolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObCartesianGeometrycollection *geo) { UNUSED(geo); return OB_SUCCESS; }

private:
  int denormalize(ObCartesianPoint *point);

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeoDeNormalizeVisitor);
};

} // namespace common
} // namespace oceanbase

#endif //OCEANBASE_LIB_GEO_OB_GEO_DENORMALIZE_VISITOR_