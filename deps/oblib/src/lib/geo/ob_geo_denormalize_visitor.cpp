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