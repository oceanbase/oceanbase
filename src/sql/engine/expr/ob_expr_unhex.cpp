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
#include "sql/engine/expr/ob_expr_unhex.h"
#include <string.h>
#include "sql/parser/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprUnhex::ObExprUnhex(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_UNHEX, N_UNHEX, 1)
{}

ObExprUnhex::~ObExprUnhex()
{}

int ObExprUnhex::calc(ObObj& result, const ObObj& text, common::ObCastCtx& cast_ctx, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!text.is_string_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. not string type ", K(text), K(common::lbt()));
  } else {
    ObString str;
    EXPR_GET_VARCHAR_V2(text, str);
    if (OB_FAIL(ret)) {
      LOG_WARN("invalid input format. need varchar.", K(ret), K(text));
    } else if (OB_FAIL(ObHexUtils::unhex(str, cast_ctx, result))) {
      // when ret is OB_ERR_INVALID_HEX_NUMBER and sql_mode is not strict return null
      if (OB_ERR_INVALID_HEX_NUMBER == ret) {
        ObCastMode default_cast_mode = CM_NONE;
        const ObSQLSessionInfo* session = expr_ctx.my_session_;
        if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is NULL", K(ret));
        } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, default_cast_mode))) {
          LOG_WARN("failed to get default cast mode", K(ret));
        } else if (CM_IS_WARN_ON_FAIL(default_cast_mode)) {
          ret = OB_SUCCESS;
          result.set_null();
        } else {
          ret = OB_ERR_INVALID_HEX_NUMBER;
          LOG_WARN("Failed to unhex", K(ret), K(str));
        }
      } else {
        // when ret is other error code
        LOG_WARN("Failed to unhex", K(ret), K(str));
      }
    } else {
      // when ret is success do nothing
    }
  }
  return ret;
}

int ObExprUnhex::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, text, cast_ctx, expr_ctx))) {
      LOG_WARN("fail to calc", K(ret), K(text));
    } else if (!result.is_null()) {
      result.set_meta_type(result_type_.get_obj_meta());
    } else {
    }
  }
  return ret;
}

int ObExprUnhex::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprUnhex::eval_unhex;

  return ret;
}

int ObExprUnhex::eval_unhex(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("eval radian arg failed", K(ret), K(expr));
  } else if (param->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(ObDatumHexUtils::unhex(expr, param->get_string(), ctx, res_datum))) {
    // when ret is OB_ERR_INVALID_HEX_NUMBER and sql_mode is not strict return null
    if (OB_ERR_INVALID_HEX_NUMBER == ret) {
      ObCastMode default_cast_mode = CM_NONE;
      const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, default_cast_mode))) {
        LOG_WARN("failed to get default cast mode", K(ret));
      } else if (CM_IS_WARN_ON_FAIL(default_cast_mode)) {
        ret = OB_SUCCESS;
        res_datum.set_null();
      } else {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("fail to eval unhex", K(ret), K(expr), K(*param));
      }
    } else {
      // ret is other error code
      LOG_WARN("fail to eval unhex", K(ret), K(expr), K(*param));
    }
  } else {
    // ret is success
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
