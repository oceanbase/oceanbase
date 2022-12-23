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