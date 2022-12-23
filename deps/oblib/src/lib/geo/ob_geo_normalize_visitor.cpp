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
#include "ob_geo_normalize_visitor.h"

namespace oceanbase {
namespace common {

bool ObGeoNormalizeVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (!no_srs_ && (OB_ISNULL(geo) || OB_ISNULL(srs_))) {
    bret = false;
  }
  return bret;
}

int ObGeoNormalizeVisitor::normalize(ObIWkbPoint *geo)
{
  INIT_SUCC(ret);
  double nx = 1.0;
  double ny = 1.0;
  ObWkbGeogPoint* point = reinterpret_cast<ObWkbGeogPoint*>(geo->val());

  if (OB_ISNULL(point)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null geo value", K(ret));
  } else if (no_srs_) {
    nx = geo->x() * M_PI / 180.0;
    ny = geo->y() * M_PI / 180.0;
  } else {
    if (OB_FAIL(srs_->latitude_convert_to_radians(geo->y(), ny))) {
      LOG_WARN("normalize y failed", K(ret));
    } else if (OB_FAIL(srs_->longtitude_convert_to_radians(geo->x(), nx))) {
      LOG_WARN("normalize x failed", K(ret));
    } else {
      uint32_t count = 0;
      double nx_tmp = nx;
      double ny_tmp = ny;
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
  }

  if (OB_SUCC(ret)) {
    point->set<0>(nx);
    point->set<1>(ny);
  }
  return ret;
}

int ObGeoNormalizeVisitor::visit(ObIWkbGeogPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(normalize(geo))){
    LOG_WARN("failed to normalize geographical point", K(ret));
  }
  return ret;
}

// for st_transform
int ObGeoNormalizeVisitor::visit(ObIWkbGeomPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(normalize(geo))){
    LOG_WARN("failed to normalize cartesian point", K(ret));
  }
  return ret;
}
} // namespace common
} // namespace oceanbase