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
#include "ob_geo_reverse_coordinate_visitor.h"

namespace oceanbase {
namespace common {

bool ObGeoReverseCoordinateVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo)) {
    bret = false;
  }
  return bret;
}

int ObGeoReverseCoordinateVisitor::reverse_point_coordinate(ObIWkbGeogPoint *geo)
{
  int ret = OB_SUCCESS;
  double x = geo->x();
  double y = geo->y();
  ObWkbGeogPoint* inner_pt = reinterpret_cast<ObWkbGeogPoint*>(const_cast<char*>(geo->val()));
  if (OB_ISNULL(inner_pt)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null geo value");
  } else {
    inner_pt->set<0>(y);
    inner_pt->set<1>(x);
  }
  return ret;
}

int ObGeoReverseCoordinateVisitor::visit(ObIWkbGeogPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reverse_point_coordinate(geo))){
    LOG_WARN("failed to calculate point range", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase