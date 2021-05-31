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
#include <string.h>
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_hextoraw.h"
#include "sql/engine/expr/ob_expr_unhex.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprHextoraw::ObExprHextoraw(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_HEXTORAW, N_HEXTORAW, 1)
{
  disable_operand_auto_cast();
}

ObExprHextoraw::~ObExprHextoraw()
{}

int ObExprHextoraw::calc_result_type1(ObExprResType& type, ObExprResType& text, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);

  type.set_collation_level(common::CS_LEVEL_NUMERIC);
  type.set_collation_type(common::CS_TYPE_BINARY);
  type.set_raw();

  common::ObObjType param_type = text.get_type();
  common::ObLength length = -1;
  const int64_t oracle_max_avail_len = 40;
  if (ob_is_numeric_type(param_type)) {
    length = oracle_max_avail_len;  // enough !
  } else if (ob_is_string_type(param_type)) {
    length = text.get_length() / 2 + (text.get_length() % 2);
  }
  if (length <= 0 || length > common::OB_MAX_VARCHAR_LENGTH) {
    length = common::OB_MAX_VARCHAR_LENGTH;
  }
  type.set_length(length);
  return common::OB_SUCCESS;
}

int ObExprHextoraw::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    cast_ctx.expect_obj_collation_ = CS_TYPE_BINARY;
    if (OB_FAIL(ObHexUtils::hextoraw(text, cast_ctx, result))) {
      LOG_WARN("fail to calc", K(ret), K(text));
    } else if (OB_LIKELY(!result.is_null())) {
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprHextoraw::calc_hextoraw_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(ObDatumHexUtils::hextoraw(
                 expr, *arg, expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_, ctx, res_datum))) {
    LOG_WARN("hextoraw failed", K(ret));
  }
  return ret;
}

int ObExprHextoraw::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_hextoraw_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
