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

#define USING_LOG_PREFIX  SQL_ENG
#include "ob_expr_date.h"
#include "lib/timezone/ob_time_convert.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprDate::ObExprDate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DATE, N_DATE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDate::~ObExprDate()
{
}

int ObExprDate::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprDate::eval_date;

  return ret;
}

int ObExprDate::eval_date(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (param->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_date(param->get_date());
  }

  return ret;
}

} //namespace sql
} //namespace oceanbase
