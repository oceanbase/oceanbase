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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_COORDINATE_RANGE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_COORDINATE_RANGE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace common
{

class ObGeoCoordinateRangeVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoCoordinateRangeVisitor(const ObSrsItem *srs, bool is_normalized = true)
    : srs_(srs), is_lati_out_range_(false), is_long_out_range_(false),
      value_out_range_(NAN), is_normalized_(is_normalized) {}
  virtual ~ObGeoCoordinateRangeVisitor() {}
  bool is_latitude_out_of_range() { return is_lati_out_range_; }
  bool is_longtitude_out_of_range() { return is_long_out_range_; }
  double value_out_of_range() { return value_out_range_; }
  bool prepare(ObGeometry *geo) override;
  int visit(ObIWkbGeogPoint *geo) override;
  int visit(ObGeometry *geo) override { UNUSED(geo); return OB_SUCCESS; } // all cartesian geometry is in range
  bool is_end(ObGeometry *geo) override { UNUSED(geo); return is_lati_out_range_ || is_long_out_range_; }
  void reset();

  int visit(ObGeographPoint *geo) override;

  template<typename Geo_type>
  int calculate_point_range(Geo_type *geo);

private:
  const ObSrsItem *srs_;
  bool is_lati_out_range_;
  bool is_long_out_range_;
  double value_out_range_;
  bool is_normalized_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoCoordinateRangeVisitor);
};

} // namespace common
} // namespace oceanbase

#endif