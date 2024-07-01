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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_VERTEX_COLLECT_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_VERTEX_COLLECT_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{

class ObGeoVertexCollectVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoVertexCollectVisitor(ObVertexes &vertexes) : vertexes_(vertexes), x_min_(NAN), x_max_(NAN) {}
  virtual ~ObGeoVertexCollectVisitor() {}
  bool prepare(ObGeometry *geo);
  int visit(ObIWkbPoint *geo);
  int visit(ObIWkbGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  inline double get_x_min() { return x_min_; }
  inline double get_x_max() { return x_max_; }

private:
  ObVertexes &vertexes_;
  double x_min_;
  double x_max_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoVertexCollectVisitor);
};

} // namespace common
} // namespace oceanbase

#endif