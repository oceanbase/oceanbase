/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_exists.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprExists::ObExprExists(ObIAllocator &alloc)
  : ObSubQueryRelationalExpr(alloc, T_OP_EXISTS, N_EXISTS, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprExists::~ObExprExists()
{
}

int ObExprExists::calc_result_type1(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!type1.is_int())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("type1 is not int", K(type1));
  } else {
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  }
  //flag
  type.set_result_flag(NOT_NULL_FLAG);
  return ret;
}

int ObExprExists::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &exists_eval;
  return ret;
};

int ObExprExists::exists_eval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_FAIL(check_exists(expr, ctx, exists))) {
    LOG_WARN("check exists failed", K(ret));
  } else {
    expr_datum.set_bool(exists);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
