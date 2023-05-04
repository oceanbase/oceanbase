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
#include <math.h>
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_power.h"
#include "sql/engine/expr/ob_expr_pow.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{
ObExprPower::ObExprPower(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_POWER, N_POWER, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprPower::calc_result_type2(ObExprResType &type,
                                   ObExprResType &type1,
                                   ObExprResType &type2,
                                   ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type2(type, type1, type2, type_ctx);
}

int calc_power_expr_oracle(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // power(base, exp)
  ObDatum *base = NULL;
  ObDatum *exp = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, base)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, exp))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (base->is_null() || exp->is_null()) {
    res_datum.set_null();
  } else {
    if (ObDoubleType == expr.datum_meta_.type_) {
      ret = ObExprPow::safe_set_double(res_datum,
                                       std::pow(base->get_double(),
                                       exp->get_double()));
    } else {
      const number::ObNumber base_nmb(base->get_number());
      const number::ObNumber exp_nmb(exp->get_number());
      number::ObNumber res_nmb;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      if (OB_FAIL(base_nmb.power(exp_nmb, res_nmb, alloc_guard.get_allocator()))) {
        LOG_WARN("calc power failed", K(ret), K(base_nmb), K(exp_nmb));
      } else {
        res_datum.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprPower::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK (2 == raw_expr.get_param_count());
  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode()) {
      rt_expr.eval_func_ = calc_power_expr_oracle;
    } else {
      rt_expr.eval_func_ = ObExprPow::calc_pow_expr;
    }
  }
  return ret;
}


} // namespace sql
} // namespace oceanbase

