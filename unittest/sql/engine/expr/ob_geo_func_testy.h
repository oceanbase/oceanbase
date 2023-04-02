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
 * This file contains implementation for ob_geo_func_testy.
 */

#ifndef OCEANBASE_SQL_OB_GEO_FUNC_TESTY_H_
#define OCEANBASE_SQL_OB_GEO_FUNC_TESTY_H_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace sql
{

extern int g_test_geo_unary_func_result[3][8];
extern int g_test_geo_binary_func_result[3][8][3][8];

class ObGeoFuncMockY
{
public:
  ObGeoFuncMockY();
  virtual ~ObGeoFuncMockY() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, int &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_GEO_FUNC_TESTY_H_