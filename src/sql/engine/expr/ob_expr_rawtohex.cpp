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
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_rawtohex.h"
#include "sql/engine/expr/ob_expr_hex.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

int internal_calc_result_type(common::ObObj &result, const ObObj &text,
                              const ObExprResType &result_type, ObExprCtx &expr_ctx)
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
      if (!ObCharset::is_cs_nonascii(result_type.get_collation_type())) {
        result.set_collation(result_type);
      } else {
        OZ (ObStringExprOperator::convert_result_collation(result_type, result, expr_ctx.calc_buf_));
      }
    }
  }
  return ret;
}

int internal_calc_result_length(ObExprResType &type, ObExprResType &text)
{
  int ret = OB_SUCCESS;
  common::ObObjType param_type = text.get_type();
  common::ObLength length = -1;
  if (ob_is_text_tc(param_type) && lib::is_oracle_mode()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "RAW", ob_obj_type_str(param_type));
    LOG_WARN("ORA-00932: inconsistent datatypes: expected - got LOB", K(ret));
  } else if (ob_is_string_type(param_type) || ob_is_raw(param_type)) {
    length = 2 * text.get_length();
    if (text.get_length_semantics() == LS_CHAR) {
      length *= 4;
    }
  } else if (ob_is_numeric_type(param_type) || ob_is_temporal_type(param_type) || ob_is_otimestamp_type(param_type)) {
    length = 300;//enough !
  }
  if (length <= 0 || length > common::OB_MAX_VARCHAR_LENGTH) {
    length = common::OB_MAX_VARCHAR_LENGTH;
  }
  type.set_length(length);
  return ret;
}

ObExprRawtohex::ObExprRawtohex(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_RAWTOHEX, N_RAWTOHEX, 1, VALID_FOR_GENERATED_COL)
{
  disable_operand_auto_cast();
}

ObExprRawtohex::~ObExprRawtohex()
{
}

int ObExprRawtohex::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    type.set_varchar();
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
    type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());
  }
  //calc length now...
  if (OB_SUCC(ret)) {
    ret = internal_calc_result_length(type, text);
  }

  return ret;
}

int ObExprRawtohex::calc_rawtohex_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const ObString &in_str = arg->get_string();
    if (OB_FAIL(ObDatumHexUtils::rawtohex(expr, in_str, ctx, res_datum))) {
      LOG_WARN("rawtohex failed", K(ret));
    }
  }
  return ret;
}

int ObExprRawtohex::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_rawtohex_expr;
  return ret;
}


ObExprRawtonhex::ObExprRawtonhex(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_RAWTONHEX, N_RAWTONHEX, 1, VALID_FOR_GENERATED_COL)
{
  disable_operand_auto_cast();
}

ObExprRawtonhex::~ObExprRawtonhex()
{
}

int ObExprRawtonhex::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
    type.set_nvarchar2();
    type.set_length_semantics(LS_CHAR);
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(nls_param.nls_nation_collation_);
  }

  //calc length now...
  if (OB_SUCC(ret)) {
    ret = internal_calc_result_length(type, text);
  }

  return ret;
}

int ObExprRawtonhex::calc_rawtonhex_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const ObString &in_str = arg->get_string();
    // notice we reuse @func(rawtohex), because @func(rawtonhex) is same as @func(rawtohex)
    if (OB_FAIL(ObDatumHexUtils::rawtohex(expr, in_str, ctx, res_datum))) {
      LOG_WARN("rawtonhex failed", K(ret));
    }
  }
  return ret;
}

int ObExprRawtonhex::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_exr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_exr.eval_func_ = calc_rawtonhex_expr;
  return ret;
}

}
}
