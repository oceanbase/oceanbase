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

#include "sql/engine/expr/ob_expr_char_length.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace sql
{

ObExprCharLength::ObExprCharLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CHAR_LENGTH, N_CHAR_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCharLength::~ObExprCharLength()
{
}

int ObExprCharLength::calc_result_type1(ObExprResType &type,
                                        ObExprResType &text,
                                        ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);

  int ret = OB_SUCCESS;
  if (text.is_null()) {
    type.set_null();
  } else {
    if (!is_type_valid(text.get_type())) {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("the param is not castable", K(text), K(ret));
    } else {
      type.set_int();
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      if (!ob_is_text_tc(text.get_type())) {
        text.set_calc_type(ObVarcharType);
      }
      text.set_calc_collation_type(text.get_collation_type());
      text.set_calc_collation_level(text.get_collation_level());
    }
  }
  return ret;
}

int ObExprCharLength::eval_char_length(const ObExpr &expr, ObEvalCtx &ctx, 
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObObjTypeClass in_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
  if (!ob_is_castable_type_class(in_tc)) {
    res.set_null();
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else if (in_tc != ObTextTC) {
    const ObString &arg_str = arg->get_string();
    size_t length = ObCharset::strlen_char(expr.args_[0]->datum_meta_.cs_type_,
                                           arg_str.ptr(), arg_str.length());
    res.set_int(length);
  } else {
    int64_t char_len = 0;
    if (OB_FAIL(ObTextStringHelper::get_char_len(ctx, *arg,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), char_len))) {
      LOG_WARN("failed to read realdata", K(ret));
    } else {
      res.set_int(static_cast<size_t>(char_len));
    }
  }
  return ret;
}

int ObExprCharLength::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_char_length;
  return ret;
}
} // namespace sql
} // namespace oceanbase
