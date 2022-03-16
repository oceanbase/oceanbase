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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/parser/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_cot.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>
namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{
ObExprCot::ObExprCot(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_COT, N_COT, 1, NOT_ROW_DIMENSION)
{
}
ObExprCot::~ObExprCot()
{
}
int ObExprCot::calc_result_type1(ObExprResType &type,
                                 ObExprResType &radian,
                                 ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, radian, type_ctx);
}
int ObExprCot::calc_result1(ObObj &result,
                            const ObObj &radian_obj,
                            ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (radian_obj.is_null()) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_ctx.calc_buf_ is NULL", K(ret));
  } else if (radian_obj.is_double()) {
    double arg = radian_obj.get_double();
    double tan_out = tan(arg);
    // to check tan(arg) is small enough to be zero
    if (0.0 == tan_out) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tan(x) is zero", K(ret));
    } else {
      double res = 1.0/tan_out;
      if (!std::isfinite(res)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tan(x) is like zero", K(ret));
      } else {
        result.set_double(res);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}
} /* sql */
} /* oceanbase */