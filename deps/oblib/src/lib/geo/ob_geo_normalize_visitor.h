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
#ifndef OCEANBASE_LIB_GEO_OB_GEO_NORMALIZE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_NORMALIZE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"
namespace oceanbase
{
namespace common
{

class ObGeoNormalizeVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoNormalizeVisitor(const ObSrsItem *srs, bool no_srs = false)
    : srs_(srs), no_srs_(no_srs) {}
  virtual ~ObGeoNormalizeVisitor() {}
  bool prepare(ObGeometry *geo);
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }

private:
  int normalize(ObIWkbPoint *geo);

private:
  const ObSrsItem *srs_;
  bool no_srs_; // for st_transform, only proj4text is given
  DISALLOW_COPY_AND_ASSIGN(ObGeoNormalizeVisitor);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_GEO_NORMALIZE_VISITOR_