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
#include "sql/engine/expr/ob_expr_pi.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

const double ObExprPi::mysql_pi_ = 3.14159265358979323846264338327950288;

ObExprPi::ObExprPi(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PI, N_PI, 0, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPi::~ObExprPi()
{
}

int ObExprPi::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_double();
  type.set_precision(-1);
  type.set_scale(6);
  return OB_SUCCESS;
}

int ObExprPi::eval_pi(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  expr_datum.set_double(mysql_pi_);
  return ret;
}

int ObExprPi::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprPi::eval_pi;
  return OB_SUCCESS;
}

} //namespace sql
} //namespace oceanbase
