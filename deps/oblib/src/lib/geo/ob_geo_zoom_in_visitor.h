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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_ZOOM_IN_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_ZOOM_IN_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{


namespace common
{

class ObGeoZoomInVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoZoomInVisitor(uint32_t zoom_in_value) : zoom_in_value_(zoom_in_value) {}
  virtual ~ObGeoZoomInVisitor() {}
  bool prepare(ObGeometry *geo) { UNUSED(geo); return true; }
  int visit(ObGeometry *geo) override { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObGeographPoint *geo);
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObCartesianPoint *geo);
  int visit(ObIWkbGeomPoint *geo);
private:
  template<typename T_Point>
  int zoom_in_point(T_Point *geo);

  uint32_t zoom_in_value_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoZoomInVisitor);
};

} // namespace common
} // namespace oceanbase

#endif