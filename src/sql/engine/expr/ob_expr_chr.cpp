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

#include "sql/engine/expr/ob_expr_chr.h"
#include "sql/session/ob_sql_session_info.h"

#include <string.h>
#include "lib/oblog/ob_log.h"
#include "sql/parser/ob_item_type.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprChr::ObExprChr(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_CHR, N_CHR, 1)
{}

ObExprChr::~ObExprChr()
{}

inline int ObExprChr::calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
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
      type.set_default_collation_type();
    }
  }
  if (OB_SUCC(ret)) {
    type.set_length(4);
    type.set_length_semantics(common::LS_BYTE);
    text.set_calc_type(common::ObDoubleType);
  }
  return ret;
}

int ObExprChr::calc(ObObj& result, const ObObj& text, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(text, ObDoubleType);
    double double_val = text.get_double();
    ObString str_result;
    if (OB_FAIL(number2varchar(str_result, double_val, alloc))) {
      LOG_WARN("fail to convert number 2 varchar", K(ret), K(double_val));
    } else if (str_result.empty()) {
      result.set_null();
    } else {
      result.set_varchar(str_result);
    }
  }
  return ret;
}

int ObExprChr::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, *expr_ctx.calc_buf_))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else if (OB_LIKELY(!result.is_null())) {
    result.set_collation_type(result_type_.get_collation_type());
    result.set_collation_level(result_type_.get_collation_level());
  } else {
  }
  return ret;
}

int ObExprChr::number2varchar(ObString& str_result, const double double_val, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  int64_t int_val = static_cast<int64_t>(double_val);
  // for chr(n), floor(n) in [0, UINT32_MAX]. If floor(n) not satisfy this scope, raise numeric
  // overflow. If 0 < n < 1, floor(n) also satisfy the scope, but oracle raise nurmeric overflow.
  if (floor(double_val) < 0 || floor(double_val) > UINT32_MAX || (double_val > 0 && double_val < 1)) {
    ret = OB_INTEGER_PRECISION_OVERFLOW;
    str_result.reset();
  } else {
    uint32_t num = static_cast<uint32_t>(int_val);
    int length = 0;
    if (num & 0xFF000000UL) {
      length = 4;
    } else if (num & 0xFF0000UL) {
      length = 3;
    } else if (num & 0xFF00UL) {
      length = 2;
    } else {
      length = 1;
    }
    char* buf = static_cast<char*>(alloc.alloc(length));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      for (int i = 0; i < length; ++i) {
        buf[length - i - 1] = static_cast<char>(num >> (8 * i));
      }
      str_result.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
    }
  }
  return ret;
}

int ObExprChr::calc_chr_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    ObExprStrResAlloc res_alloc(expr, ctx);
    double arg_double = arg->get_double();
    ObString res_str;
    if (OB_FAIL(ObExprChr::number2varchar(res_str, arg_double, res_alloc))) {
      LOG_WARN("number2varchar failed", K(ret), K(arg_double));
    } else if (res_str.empty()) {
      res_datum.set_null();
    } else {
      res_datum.set_string(res_str);
    }
  }
  return ret;
}

int ObExprChr::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_chr_expr;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
