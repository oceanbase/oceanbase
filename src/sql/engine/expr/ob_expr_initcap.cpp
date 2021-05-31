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

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_initcap.h"
#include "sql/parser/ob_item_type.h"
#include "common/data_buffer.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprInitcap::ObExprInitcap(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_INITCAP, N_INITCAP, 1)
{}

ObExprInitcap::~ObExprInitcap()
{}

int ObExprInitcap::calc_result_type1(ObExprResType& type, ObExprResType& text, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObBasicSessionInfo* session = type_ctx.get_session();
  ObSEArray<ObExprResType*, 1, ObNullAllocator> params;

  CK(OB_NOT_NULL(session));
  OZ(params.push_back(&text));
  OZ(aggregate_string_type_and_charset_oracle(*session, params, type, true));
  OZ(deduce_string_param_calc_type_and_charset(*session, type, params));
  if (OB_SUCC(ret)) {
    if (type.is_varchar() || type.is_nvarchar2()) {
      type.set_length(
          text.get_length() > OB_MAX_ORACLE_VARCHAR_LENGTH ? OB_MAX_ORACLE_VARCHAR_LENGTH : text.get_length());
    } else if (type.is_char() || type.is_nchar()) {
      type.set_length(
          text.get_length() > OB_MAX_ORACLE_CHAR_LENGTH_BYTE ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : text.get_length());
    } else {
      type.set_length(text.get_length());
    }
  }
  return ret;
}

int ObExprInitcap::calc_result1(ObObj& result, const ObObj& text_obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObString text_str;
  ObString res_str;
  if (text_obj.is_null_oracle()) {
    result.set_null();
  } else if (OB_FAIL(text_obj.get_string(text_str))) {
    LOG_WARN("get string from obj failed", K(ret), K(text_obj));
  } else if (OB_FAIL(initcap_string(text_str, text_obj.get_collation_type(), expr_ctx.calc_buf_, res_str))) {
    LOG_WARN("initcap string failed", K(ret), K(text_obj));
  } else {
    ObObjType res_type = get_result_type().get_type();
    if (ob_is_text_tc(res_type)) {
      result.set_lob_value(res_type, res_str.ptr(), res_str.length());
    } else {
      result.set_string(result_type_.get_type(), res_str);
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprInitcap::initcap_string(
    const ObString& text, const ObCollationType cs_type, ObIAllocator* allocator, ObString& res_str)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else {

    int64_t case_multiply =
        std::max(ObCharset::get_charset(cs_type)->caseup_multiply, ObCharset::get_charset(cs_type)->casedn_multiply);
    int64_t buf_len = case_multiply * text.length();
    char* buf = static_cast<char*>(allocator->alloc(buf_len));
    int64_t pos = 0;

    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(text.length()), K(case_multiply), K(cs_type), K(ret));
    } else {
      ObStringScanner scanner(text, cs_type);
      ObString cur_letter;
      int32_t wchar = 0;
      bool has_first_letter = false;

      if (1 == case_multiply) {
        MEMCPY(buf, text.ptr(), text.length());
      }

      while (OB_SUCC(ret)) {
        if (OB_ITER_END == (ret = scanner.next_character(cur_letter, wchar))) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("fail to get next character", K(ret), K(scanner));
        } else {
          bool is_alphanumeric =
              (wchar <= INT8_MAX && ob_isalnum(ObCharset::get_charset(CS_TYPE_UTF8MB4_GENERAL_CI), wchar));
          if (is_alphanumeric) {
            char* src_ptr = (1 == case_multiply) ? buf + pos : cur_letter.ptr();
            int64_t buf_remain = cur_letter.length() * case_multiply;
            int64_t write_len = has_first_letter
                                    ? ObCharset::casedn(cs_type, src_ptr, cur_letter.length(), buf + pos, buf_remain)
                                    : ObCharset::caseup(cs_type, src_ptr, cur_letter.length(), buf + pos, buf_remain);
            if (OB_UNLIKELY(0 == write_len)) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              pos += write_len;
            }
            has_first_letter = true;
          } else {
            has_first_letter = false;
            MEMCPY(buf + pos, cur_letter.ptr(), cur_letter.length());
            pos += cur_letter.length();
          }
        }
      }
      if (OB_SUCC(ret)) {
        res_str.assign_ptr(buf, pos);
      }
    }
  }

  return ret;
}

int calc_initcap_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg_datum = NULL;
  ObString res_str;
  char* res_buf = NULL;
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  int64_t case_multiply =
      std::max(ObCharset::get_charset(cs_type)->caseup_multiply, ObCharset::get_charset(cs_type)->casedn_multiply);
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("eval arg 0 failed", K(ret), K(expr));
  } else if (arg_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, arg_datum->len_ * case_multiply))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    ObDataBuffer buf_alloc(res_buf, arg_datum->len_ * case_multiply);
    if (OB_FAIL(ObExprInitcap::initcap_string(
            arg_datum->get_string(), expr.args_[0]->datum_meta_.cs_type_, &buf_alloc, res_str))) {
      LOG_WARN("initcap string failed", K(ret), K(arg_datum->get_string()));
    } else if (0 == res_str.length()) {
      // initcap is only for oracle mode. set res be null when string length is 0.
      res_datum.set_null();
    } else {
      res_datum.set_string(res_str);
    }
  }
  return ret;
}

int ObExprInitcap::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_initcap_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
