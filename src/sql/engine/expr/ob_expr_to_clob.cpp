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
#include "sql/engine/expr/ob_expr_to_clob.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_item_type.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprToClob::ObExprToClob(ObIAllocator& alloc) : ObExprToCharCommon(alloc, T_FUN_SYS_TO_CLOB, N_TO_CLOB, 1)
{}

ObExprToClob::~ObExprToClob()
{}

int ObExprToClob::calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();

  if (OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (ob_is_null(text.get_type())) {
    type.set_null();
  } else if (ob_is_string_tc(text.get_type()) || ob_is_clob(text.get_type(), text.get_collation_type()) ||
             ob_is_raw(text.get_type()) || ob_is_numeric_type(text.get_type()) ||
             ob_is_oracle_datetime_tc(text.get_type()) || ob_is_rowid_tc(text.get_type())) {
    type.set_clob();
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_collation_type(nls_param.nls_collation_);
    if (type_ctx.get_session()->use_static_typing_engine()) {
      if (!text.is_clob()) {
        text.set_calc_type(ObVarcharType);
      }
      text.set_calc_collation_type(nls_param.nls_collation_);
    } else {
      if (ob_is_string_tc(text.get_type()) || ob_is_rowid_tc(text.get_type())) {
        text.set_calc_type(common::ObVarcharType);
        text.set_calc_collation_type(nls_param.nls_collation_);
      } else if (ob_is_clob(text.get_type(), text.get_collation_type())) {
        text.set_calc_collation_type(nls_param.nls_collation_);
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("wrong type of argument in function to_clob", K(ret), K(text));
  }

  return ret;
}

int ObExprToClob::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    ;
    LOG_WARN("varchar buffer not init", K(ret));
    ;
  } else if (text.is_null_oracle()) {
    result.set_null();
  } else {
    ObString str;
    switch (text.get_type_class()) {
      case ObRawTC: {
        EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
        if (OB_ISNULL(cast_ctx.allocator_v2_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
        } else if (OB_FAIL(ObHexUtils::rawtohex(text, cast_ctx, result))) {
          LOG_WARN("fail to calc", K(ret), K(text));
        } else {
          if (ObCharset::is_cs_nonascii(result_type_.get_collation_type())) {
            OZ(convert_result_collation(result_type_, result, expr_ctx.calc_buf_));
          }
          str = result.get_string();
        }
        break;
      }
      case ObIntTC:
      case ObUIntTC:
      case ObFloatTC:
      case ObDoubleTC:
      case ObNumberTC: {
        if (OB_FAIL(number_to_char(result, &text, 1, expr_ctx))) {
          LOG_WARN("failed to convert number to char", K(ret));
        } else {
          str = result.get_string();
        }
        break;
      }
      case ObDateTimeTC:
      case ObOTimestampTC: {
        if (OB_FAIL(datetime_to_char(result, &text, 1, expr_ctx))) {
          LOG_WARN("failed to convert datetime to char", K(ret));
        } else {
          str = result.get_string();
        }
        break;
      }
      case ObStringTC: {
        str = text.get_string();
        break;
      }
      case ObTextTC: {
        if (CS_TYPE_BINARY == text.get_collation_type()) {
          ret = OB_ERR_UNEXPECTED;  // won't come here
          LOG_WARN("wrong type of argument in function to_clob", K(ret));
        } else {
          str = text.get_string();
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;  // won't come here
        LOG_WARN("wrong type of argument in function to_clob", K(ret), K(text.get_type()));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (str.length() < 0 || str.length() > common::OB_MAX_LONGTEXT_LENGTH) {
        ret = OB_ERR_UNEXPECTED;  // won't come here
        LOG_WARN("wrong length of result in function to_clob", K(ret));
      } else {
        result.set_lob_value(ObLongTextType, str.ptr(), str.length());
        result.set_collation_type(expr_ctx.my_session_->get_nls_collation());
        LOG_DEBUG("succ to calc", K(text), K(result));
      }
    }
  }

  return ret;
}

int ObExprToClob::calc_to_clob_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt or arg res type", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    res.set_datum(*arg);
    if (!res.is_null() && res.len_ > OB_MAX_LONGTEXT_LENGTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong length of result in function to_clob", K(ret), K(res.len_));
    }
  }
  return ret;
}

int ObExprToClob::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_clob_expr;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
