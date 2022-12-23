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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_REVERSE_COORDINATE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_REVERSE_COORDINATE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"

namespace oceanbase
{


namespace common
{

class ObGeoReverseCoordinateVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoReverseCoordinateVisitor() {}
  virtual ~ObGeoReverseCoordinateVisitor() {}
  bool prepare(ObGeometry *geo);
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObIWkbGeogLineString *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObIWkbGeogLinearRing *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObIWkbGeogPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObIWkbGeogMultiPoint *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObIWkbGeogMultiLineString *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObIWkbGeogMultiPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObIWkbGeogCollection *geo) { UNUSED(geo); return OB_SUCCESS; }

private:
  int reverse_point_coordinate(ObIWkbGeogPoint *geo);

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeoReverseCoordinateVisitor);
};

} // namespace common
} // namespace oceanbase

#endif