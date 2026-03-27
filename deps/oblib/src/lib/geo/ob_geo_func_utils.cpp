/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_utils.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

int ObGeoFuncUtils::apply_bg_to_tree(const ObGeometry *g1, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  ObGeoToTreeVisitor geom_visitor(context.get_allocator());
  ObGeometry *geo1 = const_cast<ObGeometry *>(g1);
  if (OB_FAIL(geo1->do_visit(geom_visitor))) {
    LOG_WARN("failed to convert bin to tree", K(ret));
  } else {
    result = geom_visitor.get_geometry();
  }
  return ret;
}

} // sql
} // oceanbase
