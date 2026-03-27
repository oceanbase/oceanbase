/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_visitor.h"


namespace oceanbase {
namespace common {

int ObEmptyGeoVisitor::visit(ObGeometry *geo)
{
  int ret = OB_INVALID_ARGUMENT;
  LOG_WARN("invalid geometry type", K(ret), K(geo->type()), K(geo->crs()));
  return ret;
}

} // namespace common
} // namespace oceanbase