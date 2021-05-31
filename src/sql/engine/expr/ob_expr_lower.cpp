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
#include "sql/engine/expr/ob_expr_lower.h"

#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprLowerUpper::ObExprLowerUpper(ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num)
    : ObStringExprOperator(alloc, type, name, param_num)
{}

ObExprLower::ObExprLower(ObIAllocator& alloc) : ObExprLowerUpper(alloc, T_FUN_SYS_LOWER, N_LOWER, 1)
{}

ObExprUpper::ObExprUpper(ObIAllocator& alloc) : ObExprLowerUpper(alloc, T_FUN_SYS_UPPER, N_UPPER, 1)
{}

ObExprNlsLower::ObExprNlsLower(ObIAllocator& alloc)
    : ObExprLowerUpper(alloc, T_FUN_SYS_NLS_LOWER, N_NLS_LOWER, PARAM_NUM_UNKNOWN)
{}

ObExprNlsUpper::ObExprNlsUpper(ObIAllocator& alloc)
    : ObExprLowerUpper(alloc, T_FUN_SYS_NLS_UPPER, N_NLS_UPPER, PARAM_NUM_UNKNOWN)
{}

int ObExprLowerUpper::calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (is_oracle_mode()) {
      ObSEArray<ObExprResType*, 1, ObNullAllocator> param;
      OZ(param.push_back(&text));
      OZ(aggregate_string_type_and_charset_oracle(*session, param, type));
      OZ(deduce_string_param_calc_type_and_charset(*session, type, param));
    } else {
      if (ObTinyTextType == text.get_type()) {
        type.set_type(ObVarcharType);
      } else if (text.is_lob()) {
        type.set_type(ObLongTextType);
      } else {
        type.set_varchar();
      }
      text.set_calc_type(type.get_type());
      const common::ObLengthSemantics default_length_semantics =
          (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                               : common::LS_BYTE);
      if (ObTinyTextType == text.get_type()) {
        type.set_length_semantics(255);
      } else {
        type.set_length_semantics(text.is_varchar_or_char() ? text.get_length_semantics() : default_length_semantics);
      }
      ret = aggregate_charsets_for_string_result(type, &text, 1, type_ctx.get_coll_type());
      OX(text.set_calc_collation_type(type.get_collation_type()));
    }
    OX(type.set_calc_accuracy(text.get_accuracy()));
    OX(type.set_length(text.get_length()));
  }

  return ret;
}

int ObExprLowerUpper::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (text.is_null()) {
    result.set_null();
  } else if ((lib::is_oracle_mode() && ObVarcharType != text.get_type() && ObCharType != text.get_type() &&
                 !ob_is_nstring_type(text.get_type()) && !ob_is_text_tc(text.get_type())) ||
             (!lib::is_oracle_mode() && ObVarcharType != text.get_type() && !text.is_lob())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(text.get_type()), K(ret));
  } else {
    ObString m_text = text.get_string();
    if (OB_FAIL(calc(result, m_text, result_type_.get_collation_type(), *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to calc lower", K(ret), K(text));
    } else {
      if (!result.is_null()) {
        result.set_collation(result_type_);
      }
    }
  }
  return ret;
}

// For oracle only functions nls_lower/nls_upper
int ObExprLowerUpper::calc_result_typeN(
    ObExprResType& type, ObExprResType* texts, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (param_num <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at least one parameter", K(ret), K(param_num));
  } else if (param_num > 2) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at most two parameters", K(ret), K(param_num));
  } else if (OB_ISNULL(texts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(texts));
  } else {
    ObSEArray<ObExprResType*, 1, ObNullAllocator> param;
    OZ(param.push_back(&texts[0]));
    OZ(aggregate_string_type_and_charset_oracle(*session, param, type));
    OZ(deduce_string_param_calc_type_and_charset(*session, type, param));

    OX(type.set_calc_accuracy(texts[0].get_accuracy()));
    OX(type.set_length(texts[0].get_length()));
  }

  return ret;
}

int ObExprLowerUpper::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(objs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(expr_ctx.calc_buf_), K(objs));
  } else if (param_num <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at least one parameter", K(ret), K(param_num));
  } else if (param_num > 2) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at most two parameters", K(ret), K(param_num));
  } else if (!share::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this function is only available in oracle mode", K(ret));
  } else {
    const ObObj& first_text = objs[0];
    if (first_text.is_null()) {
      result.set_null();
    } else if (!ob_is_string_type(first_text.get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(first_text.get_type()), K(ret));
    } else {
      const ObString& m_text = first_text.get_string();
      // Set the collation according to nls_parameter
      ObCollationType cs_type = result_type_.get_collation_type();
      if (param_num == 2) {
        const ObObj& second_text_obj = objs[1];
        if (second_text_obj.is_null()) {
          result.set_null();
        } else {
          const ObString& second_text = second_text_obj.get_string();
          if (false == ObExprOperator::is_valid_nls_param(second_text)) {
            ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
            LOG_WARN("invalid nls parameter", K(ret), K(second_text));
          } else {
            // Do nothing, we only support BINARY for now
          }
        }
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("failed to calc_resultN", K(ret));
      } else if (OB_FAIL(calc(result, m_text, cs_type, *expr_ctx.calc_buf_))) {
        LOG_WARN("failed to calc nls_lower/nls_upper", K(ret), K(m_text));
      } else {
        if (!result.is_null()) {
          result.set_collation(result_type_);
          result.set_collation_type(cs_type);
        }
      }
    }
  }

  return ret;
}

int ObExprLowerUpper::calc(ObObj& result, const ObString& text, ObCollationType cs_type, ObIAllocator& string_buf) const
{
  int ret = OB_SUCCESS;
  ObString str_result;
  if (text.empty()) {
    str_result.reset();
  } else {
    int64_t buf_len = text.length() * get_case_mutiply(cs_type);
    char* buf = reinterpret_cast<char*>(string_buf.alloc(buf_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", "size", buf_len);
    } else {
      MEMCPY(buf, text.ptr(), text.length());
      int32_t out_len = 0;
      char* src_str = (get_case_mutiply(cs_type) > 1) ? const_cast<char*>(text.ptr()) : buf;
      if (OB_FAIL(calc(cs_type, src_str, text.length(), buf, buf_len, out_len))) {
        LOG_WARN("failed to calc", K(ret));
      }
      str_result.assign(buf, static_cast<int32_t>(out_len));
    }
  }

  if (OB_SUCC(ret)) {
    result.set_common_value(str_result);
    result.set_meta_type(get_result_type());
  }

  return ret;
}

int ObExprLower::calc(
    const ObCollationType cs_type, char* src, int32_t src_len, char* dst, int32_t dst_len, int32_t& out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::casedn(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}

int32_t ObExprLower::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN("invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->casedn_multiply;
  }
  return mutiply_num;
}

int ObExprUpper::calc(
    const ObCollationType cs_type, char* src, int32_t src_len, char* dst, int32_t dst_len, int32_t& out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::caseup(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}

int32_t ObExprUpper::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN("invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->caseup_multiply;
  }
  return mutiply_num;
}

int ObExprLowerUpper::cg_expr_common(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  ObObjType text_type = ObMaxType;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lower/upper expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of lower/upper expr is null", K(ret), K(rt_expr.args_));
  } else if (FALSE_IT(text_type = rt_expr.args_[0]->datum_meta_.type_)) {
  } else if ((is_oracle_mode() && ObVarcharType != text_type && ObCharType != text_type &&
                 !ob_is_nstring_type(text_type) && !ob_is_text_tc(text_type)) ||
             (!is_oracle_mode() && ObVarcharType != text_type && ObLongTextType != text_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(text_type), K(ret));
  }
  return ret;
}

int ObExprLowerUpper::cg_expr_nls_common(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (!is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this function is only supported in Oracle mode", K(ret));
  } else if (rt_expr.arg_cnt_ <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at lease one parameter", K(ret), K(rt_expr.arg_cnt_));
  } else if (rt_expr.arg_cnt_ > 2) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at most two parameters", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) ||
             (rt_expr.arg_cnt_ == 2 && OB_ISNULL(rt_expr.args_[1]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of lower/upper expr is null", K(ret), K(rt_expr.args_));
  }
  for (int i = 0; OB_SUCC(ret) && i < rt_expr.arg_cnt_; ++i) {
    ObObjType text_type = rt_expr.args_[i]->datum_meta_.type_;
    if (!ob_is_string_type(text_type)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(text_type), K(ret));
    }
  }
  return ret;
}

int ObExprLower::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("lower expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprLower::calc_lower;
  }
  return ret;
}

int ObExprUpper::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("upper expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprUpper::calc_upper;
  }
  return ret;
}

int ObExprLowerUpper::calc_common(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool lower, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  ObDatum* text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    ObString m_text = text_datum->get_string();
    if (cs_type == CS_TYPE_INVALID) {
      cs_type = expr.datum_meta_.cs_type_;
    }
    ObString str_result;
    if (m_text.empty()) {
      str_result.reset();
    } else {
      int32_t buf_len = m_text.length();
      if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("charset is null", K(ret), K(cs_type));
      } else {
        buf_len *= (lower ? ObCharset::get_charset(cs_type)->casedn_multiply
                          : ObCharset::get_charset(cs_type)->caseup_multiply);
      }
      if (OB_SUCC(ret)) {
        char* buf = expr.get_str_res_mem(ctx, buf_len);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", "size", buf_len);
        } else {
          MEMCPY(buf, m_text.ptr(), m_text.length());
          int32_t out_len = 0;
          char* src_str = (buf_len != m_text.length()) ? const_cast<char*>(m_text.ptr()) : buf;
          if (lower) {
            out_len = static_cast<int32_t>(ObCharset::casedn(cs_type, src_str, m_text.length(), buf, buf_len));
          } else {
            out_len = static_cast<int32_t>(ObCharset::caseup(cs_type, src_str, m_text.length(), buf, buf_len));
          }
          str_result.assign(buf, static_cast<int32_t>(out_len));
        }
      }
    }
    if (OB_SUCC(ret)) {
      expr_datum.set_string(str_result);
    }
  }
  return ret;
}

int ObExprLowerUpper::calc_nls_common(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool lower)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.arg_cnt_ <= 0)) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper requires at least one parameter", K(ret), K(expr.arg_cnt_));
  } else if (OB_UNLIKELY(expr.arg_cnt_ > 2)) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper requires at most 2 parameters", K(ret), K(expr.arg_cnt_));
  } else if (expr.arg_cnt_ == 1) {
    if (OB_FAIL(calc_common(expr, ctx, expr_datum, lower, CS_TYPE_INVALID))) {
      LOG_WARN("failed to call calc_common", K(ret));
    }
  } else {  // expr.arg_cnt_ == 2
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    ObDatum* param_datum = NULL;
    if (OB_ISNULL(expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get nls parameter", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum))) {
      LOG_WARN("eval nls parameter failed", K(ret));
    } else if (param_datum->is_null()) {
      // Second param_datum is null, set result null as well.
      expr_datum.set_null();
    } else {
      const ObString& m_param = param_datum->get_string();
      if (OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(m_param))) {
        ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
        LOG_WARN("invalid nls parameter", K(ret), K(m_param));
      } else {
        // Should set cs_type here, but for now, we do nothing
        // since nls parameter only support BINARY
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_common(expr, ctx, expr_datum, lower, cs_type))) {
          LOG_WARN("failed to call calc_common", K(ret), K(cs_type));
        }
      }
    }
  }

  return ret;
}

int ObExprLower::calc_lower(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return calc_common(expr, ctx, expr_datum, true, CS_TYPE_INVALID);
}

int ObExprUpper::calc_upper(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return calc_common(expr, ctx, expr_datum, false, CS_TYPE_INVALID);
}

int ObExprNlsLower::calc(
    const ObCollationType cs_type, char* src, int32_t src_len, char* dst, int32_t dst_len, int32_t& out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::casedn(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}

int32_t ObExprNlsLower::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN("invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->casedn_multiply;
  }
  return mutiply_num;
}

int ObExprNlsLower::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_nls_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("lower expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprNlsLower::calc_lower;
  }
  return ret;
}

int ObExprNlsLower::calc_lower(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return calc_nls_common(expr, ctx, expr_datum, true);
}

int ObExprNlsUpper::calc(
    const ObCollationType cs_type, char* src, int32_t src_len, char* dst, int32_t dst_len, int32_t& out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::caseup(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}

int32_t ObExprNlsUpper::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN("invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->casedn_multiply;
  }
  return mutiply_num;
}

int ObExprNlsUpper::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_nls_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("lower expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprNlsUpper::calc_upper;
  }
  return ret;
}

int ObExprNlsUpper::calc_upper(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return calc_nls_common(expr, ctx, expr_datum, false);
}

}  // namespace sql
}  // namespace oceanbase
