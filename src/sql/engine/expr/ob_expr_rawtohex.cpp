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
#include "sql/engine/expr/ob_expr_rawtohex.h"
#include "sql/engine/expr/ob_expr_hex.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprRawtohex::ObExprRawtohex(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_RAWTOHEX, N_RAWTOHEX, 1)
{
  disable_operand_auto_cast();
}

ObExprRawtohex::~ObExprRawtohex()
{}

int ObExprRawtohex::calc_result_type1(ObExprResType& type, ObExprResType& text, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (session->use_static_typing_engine()) {
      ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
      OZ(params.push_back(&text));
      OZ(aggregate_string_type_and_charset_oracle(*session, params, type));
    } else {
      type.set_varchar();
      type.set_collation_level(common::CS_LEVEL_COERCIBLE);
      type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
    }
  }

  // calc length now...
  if (OB_SUCC(ret)) {
    common::ObObjType param_type = text.get_type();
    common::ObLength length = -1;
    if (ob_is_text_tc(param_type) && share::is_oracle_mode()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "RAW", ob_obj_type_str(param_type));
      LOG_WARN("ORA-00932: inconsistent datatypes: expected - got LOB", K(ret));
    } else if (ob_is_string_type(param_type)) {
      length = 2 * text.get_length();
    } else if (ob_is_numeric_type(param_type) || ob_is_temporal_type(param_type)) {
      length = 300;  // enough !
    }
    if (length <= 0 || length > common::OB_MAX_VARCHAR_LENGTH) {
      length = common::OB_MAX_VARCHAR_LENGTH;
    }
    type.set_length(length);
  }

  return ret;
}

int ObExprRawtohex::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(ObHexUtils::rawtohex(text, cast_ctx, result))) {
      LOG_WARN("fail to calc", K(ret), K(text));
    } else if (OB_LIKELY(!result.is_null())) {
      if (!ObCharset::is_cs_nonascii(result_type_.get_collation_type())) {
        result.set_collation(result_type_);
      } else {
        OZ(convert_result_collation(result_type_, result, expr_ctx.calc_buf_));
      }
    }
  }
  return ret;
}

int ObExprRawtohex::calc_rawtohex_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
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
  } else {
    const ObString& in_str = arg->get_string();
    if (OB_FAIL(ObDatumHexUtils::rawtohex(expr, in_str, ctx, res_datum))) {
      LOG_WARN("rawtohex failed", K(ret));
    }
  }
  return ret;
}

int ObExprRawtohex::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_rawtohex_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
