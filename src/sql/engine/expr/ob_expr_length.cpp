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

#include "sql/engine/expr/ob_expr_length.h"

#include <string.h>
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprLength::ObExprLength(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LENGTH, N_LENGTH, 1, NOT_ROW_DIMENSION)
{}

ObExprLength::~ObExprLength()
{}

int ObExprLength::calc_result_type1(ObExprResType& type, ObExprResType& text, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (share::is_oracle_mode()) {
      const ObAccuracy& acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
      type.set_number();
      type.set_scale(acc.get_scale());
      type.set_precision(acc.get_precision());
      if (!text.is_string_type()) {
        text.set_calc_type(ObVarcharType);
        text.set_calc_collation_type(session->get_nls_collation());
        text.set_calc_collation_level(CS_LEVEL_IMPLICIT);
      }
    } else {
      type.set_int();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
      text.set_calc_type(common::ObVarcharType);
    }
    OX(ObExprOperator::calc_result_flag1(type, text));
  }
  return ret;
}

int ObExprLength::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass type_class = ob_obj_type_class(text.get_type());
  if (!ob_is_castable_type_class(type_class)) {
    result.set_null();
  } else {
    if (share::is_oracle_mode()) {
      ObString m_text = text.get_string();
      size_t c_len =
          ObCharset::strlen_char(text.get_collation_type(), m_text.ptr(), static_cast<int64_t>(m_text.length()));
      number::ObNumber num;
      if (OB_FAIL(num.from(static_cast<int64_t>(c_len), *(expr_ctx.calc_buf_)))) {
        LOG_WARN("copy number fail", K(ret));
      } else {
        result.set_number(num);
      }
    } else {
      TYPE_CHECK(text, ObVarcharType);
      ObString m_text = text.get_string();
      result.set_int(static_cast<int64_t>(m_text.length()));
    }
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprLength::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ret = calc(result, text, expr_ctx);
  return ret;
}

int ObExprLength::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("length expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of length expr is null", K(ret), K(rt_expr.args_));
  } else {
    ObObjType text_type = rt_expr.args_[0]->datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(text_type);

    if (!ob_is_castable_type_class(type_class)) {
      rt_expr.eval_func_ = ObExprLength::calc_null;
    } else if (share::is_oracle_mode()) {
      rt_expr.eval_func_ = ObExprLength::calc_oracle_mode;
    } else {
      CK(ObVarcharType == text_type);
      rt_expr.eval_func_ = ObExprLength::calc_mysql_mode;
    }
  }
  return ret;
}

int ObExprLength::calc_null(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
  return OB_SUCCESS;
}
int ObExprLength::calc_oracle_mode(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    ObString m_text = text_datum->get_string();
    size_t c_len = ObCharset::strlen_char(
        expr.args_[0]->datum_meta_.cs_type_, m_text.ptr(), static_cast<int64_t>(m_text.length()));
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber num;
    if (OB_FAIL(num.from(static_cast<int64_t>(c_len), tmp_alloc))) {
      LOG_WARN("copy number fail", K(ret));
    } else {
      expr_datum.set_number(num);
    }
  }
  return ret;
}
int ObExprLength::calc_mysql_mode(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    expr_datum.set_int(static_cast<int64_t>(text_datum->len_));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
