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
 * This file contains implementation for _st_asewkb expr.
 */

#define USING_LOG_PREFIX  SQL_ENG
#include "ob_expr_prefix_pattern.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_substr.h"
//#include "lib/charset/ob_charset.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprPrefixPattern::ObExprPrefixPattern(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_PREFIX_PATTERN, N_PREFIX_PATTERN, 3, NOT_VALID_FOR_GENERATED_COL)
{
}

int ObExprPrefixPattern::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(3 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprPrefixPattern::eval_prefix_pattern;
  }
  return ret;
}

int ObExprPrefixPattern::eval_prefix_pattern(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum) {
  int ret = OB_SUCCESS;
  ObDatum *pattern = NULL;
  ObDatum *len_param = NULL;
  ObDatum *escape = NULL;
  ObCollationType escape_coll = expr.args_[2]->datum_meta_.cs_type_;
  int64_t result_len = 0;
  int64_t prefix_len = 0;
  ObString escape_str, result_str;
  bool is_valid = true;
  if (OB_FAIL(expr.eval_param_value(ctx, pattern, len_param, escape))) {
    LOG_WARN("eval args failed", K(ret));
  } else if (OB_ISNULL(pattern) || OB_ISNULL(len_param) || OB_ISNULL(escape)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are unexpected null", K(ret), K(pattern), K(len_param), K(escape));
  } else if (len_param->is_null() || pattern->is_null()) {
    is_valid = false;
    LOG_WARN("the lenth or escape param is not valid", K(ret), K(len_param), K(escape));
  } else if (is_mysql_mode() && escape->is_null()) {
    escape_coll = CS_TYPE_UTF8MB4_BIN;
    escape_str = ObString::make_string("\\");
  } else {
    escape_str = escape->get_string();
  }
  if (OB_SUCC(ret)) {
    if (is_oracle_mode()) {
      const number::ObNumber len_nmb(len_param->get_number());
      if (OB_FAIL(len_nmb.extract_valid_int64_with_trunc(prefix_len))) {
        LOG_WARN("extract_valid_uint64_with_trunc failed", K(ret), K(len_nmb));
      }
    } else {
      prefix_len = len_param->get_int();
    }
  }
  bool is_result_batch_ascii = false;
  if (OB_FAIL(ret)) {
  } else if (is_valid && OB_FAIL(calc_prefix_pattern(pattern->get_string(),
                                            expr.args_[0]->datum_meta_.cs_type_,
                                            escape_str,
                                            escape_coll,
                                            prefix_len,
                                            result_len,
                                            is_valid))) {
    LOG_WARN("fail to calc prefix pattern", K(ret));
  } else if (!is_valid) {
    expr_datum.set_null();
  } else if (OB_FAIL(ObExprSubstr::substr(
               result_str, pattern->get_string(), 1, result_len,
               expr.args_[0]->datum_meta_.cs_type_,
               storage::can_do_ascii_optimize(expr.args_[0]->datum_meta_.cs_type_), false,
               is_result_batch_ascii))) {
    LOG_WARN("get substr failed", K(ret));
  } else if (OB_UNLIKELY(result_str.length() <= 0) && lib::is_oracle_mode()) {
    expr_datum.set_null();
  } else {
    expr_datum.set_string(result_str);
  }
  return ret;
}

int ObExprPrefixPattern::calc_prefix_pattern(const ObString &pattern,
                                             ObCollationType pattern_coll,
                                             const ObString &escape,
                                             ObCollationType escape_coll,
                                             int64_t prefix_len,
                                             int64_t &result_len,
                                             bool &is_valid) {
  int ret = OB_SUCCESS;
  int32_t escape_wc = 0;
  int32_t wildcard_wc = 0;
  const ObCharsetInfo *cs = NULL;
  bool empty_escape = escape.empty();
  if (!empty_escape && 1 != escape.length()) {
    is_valid = false;
  } else if (OB_FAIL(ObCharset::mb_wc(escape_coll, escape, escape_wc))) {
    if (ret == OB_ERR_INCORRECT_STRING_VALUE) {
      //escape param have characters that can't be transformed to unicode
      ret = OB_SUCCESS;
      is_valid = false;
    } else {
      LOG_WARN("failed to convert escape to wc", K(ret), K(escape_coll), K(escape));
    }
  } else if (OB_FAIL(ObCharset::mb_wc(CS_TYPE_UTF8MB4_BIN, ObString::make_string("%"), wildcard_wc))) {
    LOG_WARN("failed to convert '%' to wc", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(cs = ObCharset::get_charset(pattern_coll)) || OB_ISNULL(cs->cset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected charset", K(ret), K(pattern_coll));
  } else {
    int64_t len_without_escape = 0;
    int64_t escape_count = 0;
    bool find_wildcard = false;
    bool find_escape = false;
    const char *buf_start = pattern.ptr();
    const char *buf_end = pattern.ptr() + pattern.length();
    int error = 0;
    is_valid = true;
    while (OB_SUCC(ret) && buf_start < buf_end && len_without_escape < prefix_len && !find_wildcard && is_valid) {
      int32_t byte_len = cs->cset->well_formed_len(cs, buf_start, buf_end, 1, &error);
      int32_t pattern_wc = 0;
      if (OB_UNLIKELY(0 != error)) {
        //pattern param have invalid character
        is_valid = false;
      } else if (OB_FAIL(ObCharset::mb_wc(pattern_coll, ObString(byte_len, buf_start), pattern_wc))) {
        if (ret == OB_ERR_INCORRECT_STRING_VALUE) {
          //pattern param have characters that can't be transformed to unicode
          ret = OB_SUCCESS;
          is_valid = false;
        } else {
          LOG_WARN("failed to convert pattern to wc", K(ret), K(pattern), K(pattern_coll));
        }
      } else if (find_escape) { //the prev character is escape
        find_escape = false;
        ++ len_without_escape;
      } else if (!empty_escape && pattern_wc == escape_wc) {
        find_escape = true;
        ++ escape_count;
      } else if (pattern_wc == wildcard_wc) {// find '%'
        find_wildcard = true;
        ++ len_without_escape;
      } else {
        ++ len_without_escape;
      }
      buf_start += byte_len;
    }
    result_len = len_without_escape + escape_count;
  }
  return ret;
}

int ObExprPrefixPattern::calc_result_type3(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprResType &type3,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if ((!ob_is_string_type(type1.get_type()) && !type1.is_null())
      || (!ob_is_string_type(type3.get_type()) && !type3.is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param type", K(ret), K(type1), K(type2), K(type3));
  } else {
    type1.set_calc_meta(type1.get_obj_meta());
    type2.set_calc_meta(type2.get_obj_meta());
    type3.set_calc_meta(type3.get_obj_meta());
    type.set_meta(type1.get_obj_meta());
    type.set_accuracy(type1.get_accuracy());
    if (lib::is_oracle_mode()) {
      type2.set_calc_type(ObNumberType);
      type2.set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    } else {
      type2.set_calc_type(ObIntType);
    }
  }
  return ret;
}

}
}