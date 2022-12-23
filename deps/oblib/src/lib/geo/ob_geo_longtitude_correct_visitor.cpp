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