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
#include "ob_geo_coordinate_range_visitor.h"

namespace oceanbase {
namespace common {

bool ObGeoCoordinateRangeVisitor::prepare(ObGeometry *geo)
{
  UNUSED(geo);
  if (srs_ == NULL || srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    return false;
  }
  return true;
}

/*
check whether the longitude or latitude of a geographical point is out of range or not
@param[in] Geometry object pointer
@return Returns OB_SUCCESS on success, error code otherwise.
*/
int ObGeoCoordinateRangeVisitor::calculate_point_range(const ObSrsItem *srs,
                                                       double longti,
                                                       double lati,
                                                       bool is_normalized,
                                                       ObGeoCoordRangeResult &result)
{
  int ret = OB_SUCCESS;
  // convert to radian in srs defined direction and meridian
  if (is_normalized) {
    longti -= srs->prime_meridian() * srs->angular_unit();
    if (!srs->is_longtitude_east()) {
      longti *= -1.0;
    }
  } else {
    longti *= srs->angular_unit();
  }
  if (longti <= -M_PI || longti > M_PI) {
    result.is_long_out_range_ = true;
    if (OB_FAIL(srs->from_radians_to_srs_unit(longti, result.value_out_range_))) {
      LOG_WARN("failed to convert radians to srs unit", K(ret), K(longti), K(srs));
    }
  } else {
    // convert to radian in srs defined direction and meridian
    if (is_normalized) {
      if (!srs->is_latitude_north()) {
        lati *= -1.0;
      }
    } else {
      lati *= srs->angular_unit();
    }
    if (lati < -M_PI_2 || lati > M_PI_2) {
      result.is_lati_out_range_ = true;
      if (OB_FAIL(srs->from_radians_to_srs_unit(lati, result.value_out_range_))) {
       LOG_WARN("failed to convert radians to srs unit", K(ret), K(lati), K(srs));
      }
    }
  }
  return ret;
}

void ObGeoCoordinateRangeVisitor::get_coord_range_result(ObGeoCoordRangeResult &result)
{
  result.is_lati_out_range_ = is_lati_out_range_;
  result.is_long_out_range_ = is_long_out_range_;
  result.value_out_range_ = value_out_range_;
}


int ObGeoCoordinateRangeVisitor::visit(ObIWkbGeogPoint *geo)
{
  int ret = OB_SUCCESS;
  ObGeoCoordRangeResult result;
  if (OB_ISNULL(srs_) || OB_ISNULL(geo)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs or geo is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else if (OB_FAIL(calculate_point_range(srs_, geo->x(), geo->y(), is_normalized_, result))){
    LOG_WARN("failed to calculate point range", K(ret));
  } else {
    is_lati_out_range_ = result.is_lati_out_range_;
    is_long_out_range_ = result.is_long_out_range_;
    value_out_range_ = result.value_out_range_;
  }
  return ret;
}

int ObGeoCoordinateRangeVisitor::visit(ObGeographPoint *geo)
{
  int ret = OB_SUCCESS;
  ObGeoCoordRangeResult result;
  if (OB_ISNULL(srs_) || OB_ISNULL(geo)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs or geo is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else if (OB_FAIL(calculate_point_range(srs_, geo->x(), geo->y(), is_normalized_, result))) {
    LOG_WARN("failed to calculate point range", K(ret));
  } else {
    is_lati_out_range_ = result.is_lati_out_range_;
    is_long_out_range_ = result.is_long_out_range_;
    value_out_range_ = result.value_out_range_;
  }
  return ret;
}

void ObGeoCoordinateRangeVisitor::reset()
{
  is_lati_out_range_ = false;
  is_long_out_range_ = false;
  value_out_range_ = NAN;
}

} // namespace common
} // namespace oceanbase