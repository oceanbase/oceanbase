// Copyright 1999-2021 Alibaba Inc. All Rights Reserved.
// Author:
//   xiaofeng.lby@alipay.com
//  Normalizer:
//
// This file is for implementation of func pi

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_pi.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

const double ObExprPi::mysql_pi_ = 3.14159265358979323846264338327950288;

ObExprPi::ObExprPi(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PI, N_PI, 0, NOT_ROW_DIMENSION)
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

int ObExprPi::calc_result0(ObObj &result, ObExprCtx &expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  result.set_double(mysql_pi_);
  return ret;
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
