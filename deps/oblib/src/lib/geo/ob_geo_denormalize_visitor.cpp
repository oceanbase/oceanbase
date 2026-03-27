/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_denormalize_visitor.h"

namespace oceanbase {
namespace common {

int ObGeoDeNormalizeVisitor::denormalize(ObCartesianPoint *point)
{
  INIT_SUCC(ret);
  double nx = 1.0;
  double ny = 1.0;

  nx = point->x() * 180.0 / M_PI;
  ny = point->y() * 180.0 / M_PI;

  point->set<0>(nx);
  point->set<1>(ny);

  return ret;
}

int ObGeoDeNormalizeVisitor::visit(ObCartesianPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(denormalize(geo))){
    LOG_WARN("failed to denormalize cartesian point", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase