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
#include "objit/common/ob_item_type.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprChr::ObExprChr(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_CHR, N_CHR, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprChr::~ObExprChr()
{
}

inline int ObExprChr::calc_result_type1(ObExprResType &type,
                                        ObExprResType &text,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    int64_t mbmaxlen = 0;
    text.set_calc_type(common::ObDoubleType);
    type.set_varchar();
    type.set_collation_level(common::CS_LEVEL_COERCIBLE);
    type.set_collation_type(session->get_nls_collation());
    type.set_length_semantics(session->get_actual_nls_length_semantics());
    if (LS_BYTE == type.get_length_semantics()) {
      OZ(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen));
      OX(type.set_length(mbmaxlen));
    } else {
      type.set_length(1);
    }
  }
  return ret;
}

int ObExprChr::number2varchar(ObString &str_result,
                              const double double_val,
                              ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t int_val = static_cast<int64_t>(double_val);
  // 对于chr(n), floor(n) ∈ [0, UINT32_MAX]，如果不在这个范围内会报错numeric overflow。
  // 奇怪的是当 0 < n < 1时，n也是满足条件的，但是oracle也报了numeric overflow，可能跟
  // oracle的执行框架有关，这里做了兼容。
  if (floor(double_val) < 0 ||
      floor(double_val) > UINT32_MAX ||
      (double_val > 0 && double_val < 1)) {
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
    char *buf = static_cast<char *>(alloc.alloc(length));
    if(OB_ISNULL(buf)) {
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

int ObExprChr::calc_chr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
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

int ObExprChr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_chr_expr;
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
