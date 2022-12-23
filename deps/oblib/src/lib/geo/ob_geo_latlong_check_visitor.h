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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_LATLONG_CHECK_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_LATLONG_CHECK_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace common
{

class ObGeoLatlongCheckVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoLatlongCheckVisitor(const ObSrsItem *srs)
    : srs_(srs),
      changed_(false) {}
  virtual ~ObGeoLatlongCheckVisitor() {}
  bool has_changed() { return changed_; }
  bool prepare(ObGeometry *geo) override;
  int visit(ObIWkbGeogPoint *geo) override;
  int visit(ObGeometry *geo) override { UNUSED(geo); return OB_SUCCESS; } // all cartesian geometry is in range
  bool is_end(ObGeometry *geo) override { UNUSED(geo); return OB_SUCCESS; }
  int visit(ObGeographPoint *geo) override;

  template<typename Geo_type>
  int calculate_point_range(Geo_type *geo);

private:
  const ObSrsItem *srs_;
  bool changed_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoLatlongCheckVisitor);
};

} // namespace common
} // namespace oceanbase

#endif