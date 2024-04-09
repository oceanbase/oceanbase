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
#include "ob_geo_affine_visitor.h"

namespace oceanbase
{
namespace common
{

bool ObGeoAffineVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if ((OB_ISNULL(geo) || OB_ISNULL(affine_))) {
    bret = false;
  }
  return bret;
}

template<typename PtType>
void ObGeoAffineVisitor::affine(PtType *point)
{
  double x = point->x();
  double y = point->y();
  point->x(affine_->x_fac1 * x + affine_->y_fac1 * y + affine_->x_off);
  point->y(affine_->x_fac2 * x + affine_->y_fac2 * y + affine_->y_off);
}

int ObGeoAffineVisitor::visit(ObGeographPoint *geo)
{
  affine(geo);
  return OB_SUCCESS;
}

// for st_transform
int ObGeoAffineVisitor::visit(ObCartesianPoint *geo)
{
  affine(geo);
  return OB_SUCCESS;
}
}  // namespace common
}  // namespace oceanbase