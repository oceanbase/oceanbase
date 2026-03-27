/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_close_ring_visitor.h"


namespace oceanbase {
namespace common {

template<typename PolyTree>
int ObGeoCloseRingVisitor::visit_poly(PolyTree *geo)
{
  int ret = OB_SUCCESS;
  if (geo->front() != geo->back()) {
    if (OB_FAIL(geo->push_back(geo->front()))) {
      LOG_WARN("fail to push back point", K(ret));
    }
  }
  if (OB_SUCC(ret) && geo->size() < 4) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid geometry polygon", K(ret), K(geo->size()));
  }
  return ret;
}

int ObGeoCloseRingVisitor::visit(ObGeographLinearring *geo)
{
  return visit_poly(geo);
}

int ObGeoCloseRingVisitor::visit(ObCartesianLinearring *geo)
{
  return visit_poly(geo);
}

} // namespace common
} // namespace oceanbase