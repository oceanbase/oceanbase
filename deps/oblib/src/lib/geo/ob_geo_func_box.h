/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_geo_func_box.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_BOX_H_
#define OCEANBASE_LIB_OB_GEO_FUNC_BOX_H_

#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncBox
{
public:
  ObGeoFuncBox();
  ~ObGeoFuncBox();
  static int eval(const common::ObGeoEvalCtx &gis_context, common::ObGeogBox *&result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_BOX_H_