/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_GEO_OB_GEO_LONGTITUDE_CORRECT_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_LONGTITUDE_CORRECT_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{


namespace common
{

class ObGeoLongtitudeCorrectVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoLongtitudeCorrectVisitor(const ObSrsItem *srs)
    : srs_(srs) {}
  virtual ~ObGeoLongtitudeCorrectVisitor() {}
  bool prepare(ObGeometry *geo) { UNUSED(geo); return true; }

  int visit(ObGeographLineString *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObGeographLinearring *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObGeographPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObGeometrycollection *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObGeographPoint *geo);
private:
  const ObSrsItem *srs_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoLongtitudeCorrectVisitor);
};

} // namespace common
} // namespace oceanbase

#endif