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

#define USING_LOG_PREFIX LIB
#include "ob_geo_zoom_in_visitor.h"

namespace oceanbase {
namespace common {

template<typename T_Point>
int ObGeoZoomInVisitor::zoom_in_point(T_Point *geo)
{
  if (!is_calc_zoom_) {
    uint32_t count = 0;
    while (count++ < zoom_in_value_) {
      double longti = geo->x();
      double lati = geo->y();
      longti *= 10;
      lati *= 10;
      geo->x(longti);
      geo->y(lati); 
    }
  } else {
    uint32_t count = 0;
    double nx_tmp = geo->x();
    double ny_tmp = geo->y();
    while (nx_tmp != 0.0 && std::fabs(nx_tmp) < ZOOM_IN_THRESHOLD) {
      nx_tmp *= 10;
      count++;
    }
    zoom_in_value_ = count > zoom_in_value_ ? count : zoom_in_value_;
    count = 0;
    while (ny_tmp != 0.0 && std::fabs(ny_tmp) < ZOOM_IN_THRESHOLD) {
      ny_tmp *= 10;
      count++;
    }
    zoom_in_value_ = count > zoom_in_value_ ? count : zoom_in_value_;
  }
  return OB_SUCCESS;
}

int ObGeoZoomInVisitor::visit(ObGeographPoint *geo)
{
  return zoom_in_point(geo);
}

int ObGeoZoomInVisitor::visit(ObIWkbGeogPoint *geo)
{
  return zoom_in_point(geo);
}

int ObGeoZoomInVisitor::visit(ObCartesianPoint *geo)
{
  return zoom_in_point(geo);
}

int ObGeoZoomInVisitor::visit(ObIWkbGeomPoint *geo)
{
  return zoom_in_point(geo);
}
} // namespace common
} // namespace oceanbase