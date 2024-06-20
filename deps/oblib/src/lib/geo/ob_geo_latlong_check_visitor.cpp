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

#define USING_LOG_PREFIX SQL
#include "ob_geo_latlong_check_visitor.h"
#include <cmath>

namespace oceanbase {
namespace common {

double ObGeoLatlongCheckVisitor::ob_normalize_latitude(double lat)
{
  bool modified = false;
  const double TOLERANCE = 1e-10; // according to pg
  if (lat > 90.0 && (lat - 90) <= TOLERANCE) {
    lat = 90.0;
    modified = true;
  } else if (lat < -90.0 && (-90 - lat) <= TOLERANCE) {
    lat = -90.0;
    modified = true;
  }

  if (!modified) {
    if (lat > 360.0) {
      lat = remainder(lat, 360.0);
    }

    if (lat < -360.0) {
      lat = remainder(lat, -360.0);
    }

    if (lat > 180.0) {
      lat = 180.0 - lat;
    }

    if (lat < -180.0) {
      lat = -180.0 - lat;
    }

    if (lat > 90.0) {
      lat = 180.0 - lat;
    }

    if (lat < -90.0) {
      lat = -180.0 - lat;
    }
  }

  return lat;
}

double ObGeoLatlongCheckVisitor::ob_normalize_longitude(double lon)
{
  bool modified = false;
  const double TOLERANCE = 1e-10; // according to pg
  if (lon > 180.0 && (lon - 180) <= TOLERANCE) {
    lon = 180.0;
    modified = true;
  } else if (lon < -180.0 && (-180 - lon) <= TOLERANCE) {
    lon = -180.0;
    modified = true;
  }

  if (!modified) {
    if (lon > 360.0) {
      lon = remainder(lon, 360.0);
    }

    if (lon < -360.0) {
      lon = remainder(lon, -360.0);
    }

    if (lon > 180.0) {
      lon = -360.0 + lon;
    }

    if (lon < -180.0) {
      lon = 360 + lon;
    }

    if (lon == -180.0) {
      lon = 180.0;
    }

    if (lon == -360.0) {
      lon = 0.0;
    }
  }

  return lon;
}

bool ObGeoLatlongCheckVisitor::prepare(ObGeometry *geo)
{
  UNUSED(geo);
  int res = true;
  if (srs_ == NULL || srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    res = false;
  }
  return res;
}

template<typename Geo_type>
int ObGeoLatlongCheckVisitor::calculate_point_range(Geo_type *geo)
{
  double longti = geo->x();
  double lati = geo->y();
  if (longti < -180.0 || longti > 180.0 ||
      lati < -90.0 || lati > 90.0 ) {
    longti = ob_normalize_longitude(longti);
    lati = ob_normalize_latitude(lati);
    geo->x(longti);
    geo->y(lati);
    changed_ = true;
  }
  return OB_SUCCESS;
}

int ObGeoLatlongCheckVisitor::visit(ObIWkbGeogPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(srs_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else if (OB_FAIL(calculate_point_range(geo))){
    LOG_WARN("failed to calculate point range", K(ret));
  }
  return ret;
}

int ObGeoLatlongCheckVisitor::visit(ObGeographPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(srs_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else if (OB_FAIL(calculate_point_range(geo))) {
    LOG_WARN("failed to calculate point range", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase