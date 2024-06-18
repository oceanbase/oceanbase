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
#include "ob_geo_vertex_collect_visitor.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase {
namespace common {

bool ObGeoVertexCollectVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo)) {
    bret = false;
  }
  return bret;
}

int ObGeoVertexCollectVisitor::visit(ObIWkbPoint *geo)
{
  int ret = OB_SUCCESS;
  ObPoint2d vertex;
  vertex.x = geo->x();
  vertex.y = geo->y();
  if (OB_FAIL(vertexes_.push_back(vertex))) {
    LOG_WARN("failed to add vertex to cached geo", K(ret));
  } else {
    if (std::isnan(x_min_)) {
      x_min_ = vertex.x;
    } else {
      x_min_ = std::min(x_min_, vertex.x);
    }
    if (std::isnan(x_max_)) {
      x_max_ = vertex.x;
    } else {
      x_max_ = std::max(x_max_, vertex.x);
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase