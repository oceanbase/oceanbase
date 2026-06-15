/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_chr.h"

#include "src/sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

static int number2varchar(ObString &str_result,
                          const double double_val,
                          ObIAllocator &alloc,
                          const ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  int64_t int_val = static_cast<int64_t>(double_val);
  int64_t maxmb_len = 0;
  bool is_single_byte = false;
  // 对于chr(n), floor(n) ∈ [0, UINT32_MAX]，如果不在这个范围内会报错numeric overflow。
  // 奇怪的是当 0 < n < 1时，n也是满足条件的，但是oracle也报了numeric overflow，可能跟
  // oracle的执行框架有关，这里做了兼容。
  if (floor(double_val) < 0 || floor(double_val) > UINT32_MAX
      || (double_val > 0 && double_val < 1)) {
    ret = OB_INTEGER_PRECISION_OVERFLOW;
    str_result.reset();
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll_type, maxmb_len))) {
    LOG_WARN("failed to get mbmaxlen by coll", K(ret), K(coll_type));
  } else if (FALSE_IT(is_single_byte = (1 == maxmb_len))) {
  } else {
    uint32_t num = static_cast<uint32_t>(int_val);
    if (is_single_byte) {
      num %= 256;
    }
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

static int number2nchar(ObString &str_result,
                        const number::ObCompactNumber &number_val,
                        ObIAllocator &alloc,
                        const ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  const number::ObNumber num(number_val);
  int64_t int_val = 0;
  ObCharsetType charset_type = ObCharset::charset_type_by_coll(coll_type);
  num.extract_valid_int64_with_trunc(int_val);
  // 与 chr 保持行为一致
  if (int_val < 0 || int_val > UINT32_MAX
      || (int_val == 0 && !num.is_zero())) {
    ret = OB_INTEGER_PRECISION_OVERFLOW;
    str_result.reset();
  } else if (CHARSET_UTF16 != charset_type) {
    // 当前 nls_nchar_character 仅支持 utf16
    ret = OB_ERR_UNSUPPORTED_CHARACTER_SET;
    LOG_WARN("nchr only supports UTF16 charset", K(ret), K(coll_type));
  } else {
    uint32_t num = static_cast<uint32_t>(int_val);
    int32_t wchar = static_cast<int32_t>(num);
    int32_t length = 0;
    char *buf = static_cast<char *>(alloc.alloc(ObCharset::MAX_MB_LEN));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::wc_mb(coll_type, wchar, buf, ObCharset::MAX_MB_LEN, length))) {
      LOG_WARN("invalid code point to nchar", K(ret), K(wchar), K(coll_type));
      // oracle 在 utf16 情况下，对超过 65536 的值都做取模，这里 ob 选择对合法的 unicode 进行转换，不合法的取模再转换，不进行报错
      num %= 65536;
      wchar = static_cast<int32_t>(num);
      if (OB_FAIL(ObCharset::wc_mb(coll_type, wchar, buf, ObCharset::MAX_MB_LEN, length))) {
        LOG_WARN("failed to convert code point to nchar after mod", K(ret), K(wchar), K(coll_type));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      str_result.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
    }
  }
  return ret;
}

static int calc_chr_nchr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  sql::ObSQLSessionInfo *session = NULL;
  const bool is_nchr = (T_FUN_SYS_NCHR == expr.type_);
  ObCollationType coll_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (FALSE_IT(coll_type = is_nchr ? session->get_nls_collation_nation()
                                          : session->get_nls_collation())) {
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObString res_str;
    if (is_nchr) {
      const number::ObCompactNumber &arg_number = arg->get_number();
      if (OB_FAIL(number2nchar(res_str, arg_number, res_alloc, coll_type))) {
        LOG_WARN("number2nchar failed", K(ret), K(arg_number), K(coll_type));
      }
    } else {
      const double arg_double = arg->get_double();
      if (OB_FAIL(number2varchar(res_str, arg_double, res_alloc, coll_type))) {
        LOG_WARN("number2varchar failed", K(ret), K(arg_double), K(coll_type));
      }
    }
    if (OB_FAIL(ret)) {
      // noop
    } else if (res_str.empty()) {
      res_datum.set_null();
    } else {
      res_datum.set_string(res_str);
    }
  }
  return ret;
}

ObExprChr::ObExprChr(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_CHR, N_CHR, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL)
{
}

ObExprChr::~ObExprChr()
{
}

int ObExprChr::calc_result_typeN(ObExprResType &type,
                                 ObExprResType *texts,
                                 int64_t param_num,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("chr requires one argument", K(ret), K(param_num));
  } else if (param_num > 1) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("chr requires exactly one argument", K(ret), K(param_num));
  } else {
    const ObSQLSessionInfo *session = type_ctx.get_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      int64_t mbmaxlen = 0;
      texts[0].set_calc_type(common::ObDoubleType);
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
  }
  return ret;
}

int ObExprChr::calc_chr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return calc_chr_nchr_expr(expr, ctx, res_datum);
}

int ObExprChr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_chr_expr;
  return ret;
}

ObExprNchr::ObExprNchr(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_NCHR, N_NCHR, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL)
{
}

ObExprNchr::~ObExprNchr()
{
}

int ObExprNchr::calc_result_typeN(ObExprResType &type,
                                  ObExprResType *texts,
                                  int64_t param_num,
                                  common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nchr requires one argument", K(ret), K(param_num));
  } else if (param_num > 1) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("nchr requires exactly one argument", K(ret), K(param_num));
  } else {
    const ObSQLSessionInfo *session = type_ctx.get_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      texts[0].set_calc_type(common::ObNumberType);
      type.set_nvarchar2();
      type.set_collation_level(common::CS_LEVEL_COERCIBLE);
      type.set_collation_type(session->get_nls_collation_nation());
      type.set_length_semantics(LS_CHAR);
      type.set_length(1);
    }
  }
  return ret;
}

int ObExprNchr::calc_nchr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return calc_chr_nchr_expr(expr, ctx, res_datum);
}

int ObExprNchr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_nchr_expr;
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
