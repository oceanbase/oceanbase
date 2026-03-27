/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_longtitude_correct_visitor.h"

namespace oceanbase {
namespace common {

int ObGeoLongtitudeCorrectVisitor::visit(ObGeographPoint *geo)
{
  int32_t ret = OB_SUCCESS;
  if (OB_ISNULL(srs_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else {
    double longti = geo->x();
    longti -= srs_->prime_meridian() * srs_->angular_unit();
    if (!srs_->is_longtitude_east()) {
      longti *= -1.0;
    }
    if (longti <= -M_PI) { // longtitude < -180
      geo->x(longti + 2.0 * M_PI);
    } else if (longti > M_PI) {
      geo->x(longti - 2.0 * M_PI);
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase