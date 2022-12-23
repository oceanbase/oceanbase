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