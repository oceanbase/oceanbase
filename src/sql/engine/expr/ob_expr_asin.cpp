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
#include "ob_expr_asin.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {
ObExprAsin::ObExprAsin(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_ASIN, N_ASIN, 1, NOT_ROW_DIMENSION)
{}

ObExprAsin::~ObExprAsin()
{}

int ObExprAsin::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  return calc_trig_function_result_type1(type, type1, type_ctx);
}

int ObExprAsin::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(expr_ctx.calc_buf_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_UNLIKELY(obj.is_null())) {
    result.set_null();
  } else if (obj.is_number()) {
    number::ObNumber res_nmb;
    if (OB_FAIL(obj.get_number().asin(res_nmb, *(expr_ctx.calc_buf_)))) {
      LOG_WARN("fail to calc asin", K(obj), K(ret), K(res_nmb));
    } else {
      result.set_number(res_nmb);
    }
  } else if (obj.is_double()) {
    double arg = obj.get_double();
    if (arg > 1 || arg < -1) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    } else {
      double res = asin(arg);
      result.set_double(res);
    }
  }
  return ret;
}

DEF_CALC_TRIGONOMETRIC_EXPR(asin, arg > 1 || arg < -1, OB_ERR_ARGUMENT_OUT_OF_RANGE);

int ObExprAsin::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_asin_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
