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
 * This file contains implementation for ob_geo_func_utils.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_func_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_smart_call.h"
#include "lib/utility/ob_hang_fatal_error.h"

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
