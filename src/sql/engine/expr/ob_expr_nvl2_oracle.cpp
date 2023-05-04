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

#include "sql/engine/expr/ob_expr_nvl2_oracle.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

ObExprNvl2Oracle::ObExprNvl2Oracle(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_NVL2, N_NVL2, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNvl2Oracle::~ObExprNvl2Oracle()
{}

int ObExprNvl2Oracle::calc_result_type3(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        ObExprResType &type3,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type1);
  // nvl2 reference: https://docs.oracle.com/database/121/SQLRF/functions132.htm#SQLRF00685
  // the expr1 can have any data type
  // the type of expr1 and expr2 can't be LONG, but now ob doesn't support the LONG type.
  return ObExprOracleNvl::calc_nvl_oralce_result_type(type, type2, type3, type_ctx);
}

int ObExprNvl2Oracle::calc_nvl2_oracle_expr_batch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const int64_t batch_size) {
  LOG_DEBUG("eval nvl2 oracle batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum* results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatumVector args0;
  ObDatumVector args1;
  ObDatumVector args2;
  if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, batch_size, args0,
                                        args1, args2))) {
    LOG_WARN("eval batch args failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      eval_flags.set(i);
      ObDatum *arg0 = args0.at(i);
      ObDatum *arg1 = args1.at(i);
      ObDatum *arg2 = args2.at(i);
      if (!(arg0->is_null())) {
        results[i].set_datum(*arg1);
      } else {
        results[i].set_datum(*arg2);
      }
    }
  }

  return ret;
}

int ObExprNvl2Oracle::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr2;
  rt_expr.eval_batch_func_ = calc_nvl2_oracle_expr_batch;
  return ret;
}
} // namespace sql
} // namespqce oceanbase
