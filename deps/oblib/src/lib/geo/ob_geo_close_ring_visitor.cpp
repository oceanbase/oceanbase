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