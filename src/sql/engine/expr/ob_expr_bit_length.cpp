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

#include "sql/engine/expr/ob_expr_bit_length.h"

#include <string.h>
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprBitLength::ObExprBitLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_BIT_LENGTH, N_BIT_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprBitLength::~ObExprBitLength()
{
}

int ObExprBitLength::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    text.set_calc_type(common::ObVarcharType);
    OX(ObExprOperator::calc_result_flag1(type, text));
  }
  return ret;
}

int ObExprBitLength::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bit length expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of bit length expr is null", K(ret), K(rt_expr.args_));
  } else {
    ObObjType text_type = rt_expr.args_[0]->datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(text_type);

    if (!ob_is_castable_type_class(type_class)) {
      rt_expr.eval_func_ = ObExprBitLength::calc_null;
    } else {
      CK(ObVarcharType == text_type);
      rt_expr.eval_func_ = ObExprBitLength::calc_bit_length;
    }
  }
  return ret;
}

int ObExprBitLength::calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
  return OB_SUCCESS;
}

int ObExprBitLength::calc_bit_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    expr_datum.set_int(static_cast<int64_t>(text_datum->len_ * 8));
  }
  return ret;
}

}
}
