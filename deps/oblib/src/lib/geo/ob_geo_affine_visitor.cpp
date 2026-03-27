/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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