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
#include "sql/engine/expr/ob_expr_not_exists.h"
#include "common/row/ob_row_iterator.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprNotExists::ObExprNotExists(ObIAllocator &alloc)
  : ObSubQueryRelationalExpr(alloc, T_OP_NOT_EXISTS, N_NOT_EXISTS, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNotExists::~ObExprNotExists()
{
}

int ObExprNotExists::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!type1.is_int())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    type.set_int32();
    type.set_result_flag(NOT_NULL_FLAG);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  }
  return ret;
}

int ObExprNotExists::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &not_exists_eval;
  return ret;
};

int ObExprNotExists::not_exists_eval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_FAIL(check_exists(expr, ctx, exists))) {
    LOG_WARN("check exists failed", K(ret));
  } else {
    expr_datum.set_bool(!exists);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
