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

#include "sql/engine/expr/ob_expr_replace.h"

#include <limits.h>
#include <string.h>

#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{

ObExprReplace::ObExprReplace(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_REPLACE, N_REPLACE, TWO_OR_THREE, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprReplace::~ObExprReplace()
{
}

int ObExprReplace::calc_result_typeN(ObExprResType &type,
                             ObExprResType *types_array,
                             int64_t param_num,
                             common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (3 != param_num && !lib::is_oracle_mode()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Replace() should have three arguments in MySql Mode", K(ret), K(param_num), K(lib::is_oracle_mode()));
  } else if (2 != param_num && 3 != param_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Replace() should have three arguments in Oracle Mode", K(ret), K(param_num), K(lib::is_oracle_mode()));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. types_array or session null",
             K(ret), KP(types_array), KP(type_ctx.get_session()));
  } else {
    if (lib::is_oracle_mode()) {
      ObSEArray<ObExprResType*, 3, ObNullAllocator> params;
      OZ(params.push_back(&types_array[0]));
      OZ(aggregate_string_type_and_charset_oracle(
              *type_ctx.get_session(), params, type, PREFER_VAR_LEN_CHAR));
      if (OB_SUCC(ret) && type.is_varchar_or_char()) {
        type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
        OZ(params.push_back(&types_array[i]));
      }
      OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, params, LS_BYTE));
    } else {
      if (types_array[0].is_lob()) {
        type.set_type(types_array[0].get_type());
      } else {
        type.set_varchar();
        type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());
      }
      if (3 == param_num) {
        types_array[2].set_calc_type(ObVarcharType);
      }
      OZ(ObExprOperator::aggregate_charsets_for_string_result_with_comparison(
              type, types_array, param_num, type_ctx.get_coll_type()));
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
        types_array[i].set_calc_meta(type);
      }
    }
  }
  //deduce length
  if (OB_SUCC(ret)) {
    ObLength ori_len = 0;
    ObLength from_len = 0;
    ObLength to_len = 0;
    int64_t result_len = 0;
    if (lib::is_oracle_mode()) {
      result_len = types_array[0].get_calc_length();
      if (param_num == 2 || types_array[2].is_null()) {
        // do nothing
      } else {
        OX(result_len *= types_array[2].get_calc_length());
        if (OB_SUCC(ret) && (type.is_nchar() || type.is_nvarchar2())) {
          const ObCharsetInfo *cs = ObCharset::get_charset(type.get_collation_type());
          result_len = result_len * cs->mbmaxlen;
          result_len = MIN(result_len, OB_MAX_ORACLE_VARCHAR_LENGTH) / cs->mbminlen;
        }
      }
      OX(type.set_length(result_len));
    } else {
      ori_len = types_array[0].get_length();
      from_len = types_array[1].get_length();
      to_len = types_array[2].get_length();
      if (from_len == 0 || from_len >= to_len) {
        type.set_length(ori_len);
      } else {
        result_len = ori_len / from_len * to_len + ori_len % from_len;
        if (result_len < 0 || result_len > OB_MAX_VARCHAR_LENGTH) {
          result_len = OB_MAX_VARCHAR_LENGTH;
        }
        type.set_length(result_len);
      }
    }
  }
  return ret;
}

int ObExprReplace::replace(ObString &ret_str,
                           const ObCollationType cs_type,
                           const ObString &text,
                           const ObString &from,
                           const ObString &to,
                           ObExprStringBuf &string_buf)
{
  int ret = OB_SUCCESS;
  ObString dst_str;
  bool is_null = false;
  if (OB_UNLIKELY(text.length() <= 0)) {
    // Return empty string
  } else if (OB_UNLIKELY(from.length() <= 0) || OB_UNLIKELY(to.length() < 0)) {
    ret_str = text;
  } else if (OB_ISNULL(from.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. nullpointer(s)", K(ret), K(from), K(to));
  } else if (OB_UNLIKELY(text.length() < from.length()) ||
             OB_UNLIKELY(from == to)) {
    ret_str = text;
  } else if (OB_FAIL(ObSQLUtils::check_well_formed_str(text, cs_type, dst_str, is_null, false, false))
            || OB_FAIL(ObSQLUtils::check_well_formed_str(from, cs_type, dst_str, is_null, false, false))
            || OB_FAIL(ObSQLUtils::check_well_formed_str(to, cs_type, dst_str, is_null, false, false))) {
    LOG_WARN("check well formed str failed", K(ret));
  } else {
    ObSEArray<uint32_t, 4> locations(common::ObModIds::OB_SQL_EXPR_REPLACE,
                                     common::OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObString mb;
    int32_t wc;
    ObStringScanner scanner(text, cs_type, ObStringScanner::IGNORE_INVALID_CHARACTER);
    while (OB_SUCC(ret) && scanner.get_remain_str().length() >= from.length()) {
      if (0 == MEMCMP(scanner.get_remain_str().ptr(), from.ptr(), from.length())) {
        ret = locations.push_back(scanner.get_remain_str().ptr() - text.ptr());
        scanner.forward_bytes(from.length());
      } else if (OB_FAIL(scanner.next_character(mb, wc))) {
        LOG_WARN("get next character failed", K(ret));
      } else {
        //do nothing
      }
    }

    int64_t tot_length = 0;
    if (OB_FAIL(ret)) {
      ret_str.reset();
    } else if (locations.count() == 0) {
      ret_str = text;
    } else if (OB_UNLIKELY((OB_MAX_VARCHAR_LENGTH - text.length()) / locations.count() < (to.length() - from.length()))) {
      ret = OB_ERR_VARCHAR_TOO_LONG;
      LOG_ERROR("Result of replace() was larger than OB_MAX_VARCHAR_LENGTH.",
           K(text.length()), K(to.length()), K(from.length()), K(OB_MAX_VARCHAR_LENGTH), K(ret));
      ret_str.reset();
    } else if (OB_UNLIKELY((tot_length = text.length() + (to.length() - from.length()) * locations.count()) <= 0)) {
        // tot_length equals to 0 indicates that length_to is zero and "to" is empty string
      ret_str.reset();
    } else {
      char *buf = static_cast<char *>(string_buf.alloc(tot_length));
      int pos = 0;
      int text_pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed.", K(tot_length), K(ret));
      }

      for (int i = 0; OB_SUCC(ret) && i < locations.count(); i++) {
        MEMCPY(buf + pos, text.ptr() + text_pos, locations.at(i) - text_pos);
        pos += locations.at(i) - text_pos;
        text_pos = locations.at(i);
        MEMCPY(buf + pos, to.ptr(), to.length());
        pos += to.length();
        text_pos += from.length();
      }
      if (OB_SUCC(ret) && text_pos < text.length()) {
        MEMCPY(buf + pos, text.ptr() + text_pos, text.length() - text_pos);
      }
      ret_str.assign_ptr(buf, static_cast<int32_t>(tot_length));
    }
  }
  return ret;
}

int ObExprReplace::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_ || 3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &eval_replace;
  return ret;
}

int ObExprReplace::eval_replace(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObString res;
  const bool is_mysql = lib::is_mysql_mode();
  ObExprStrResAlloc alloc(expr, ctx);
  ObDatum *text = NULL;
  ObDatum *from = NULL;
  ObDatum *to = NULL;
  bool is_clob = expr.args_[0]->datum_meta_.is_clob();
  bool is_lob_res = ob_is_text_tc(expr.datum_meta_.type_);
  if (OB_FAIL(expr.eval_param_value(ctx, text, from, to))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (text->is_null()
             || (is_mysql && from->is_null())
             || (is_mysql && NULL != to && to->is_null())) {
    expr_datum.set_null();
  } else if (is_clob && (0 == text->len_)) {
    expr_datum.set_datum(*text);
  } else if (!is_lob_res) { // non text tc inputs
    if (OB_FAIL(replace(res,
                        expr.datum_meta_.cs_type_,
                        text->get_string(),
                        !from->is_null() ? from->get_string() : ObString(),
                        (NULL != to && !to->is_null()) ? to->get_string() : ObString(),
                        alloc))) {
      LOG_WARN("do replace failed", K(ret));
    } else {
      if (res.empty() && !is_mysql && !expr.args_[0]->datum_meta_.is_clob()) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(res);
      }
    }
  } else { // tc inputs
    ObString text_data;
    ObString to_data;
    ObString from_data;
    text_data = text->get_string();
    from_data = from->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *text,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), text_data))) {
      LOG_WARN("failed to get string data", K(ret), K(expr.args_[0]->datum_meta_));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *from,
                       expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), from_data))) {
      LOG_WARN("failed to get string data", K(ret), K(expr.args_[1]->datum_meta_));
    } else if (NULL == to) {
      to_data.reset();
    } else if (OB_FALSE_IT(to_data = to->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *to,
                       expr.args_[2]->datum_meta_, expr.args_[2]->obj_meta_.has_lob_header(), to_data))) {
      LOG_WARN("failed to get string data", K(ret), K(expr.args_[2]->datum_meta_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replace(res, expr.datum_meta_.cs_type_, text_data, from_data, to_data, temp_allocator))) {
        LOG_WARN("do replace for lob resutl failed", K(ret), K(expr.datum_meta_.type_));
      } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(expr, ctx, expr_datum, res))) {
        LOG_WARN("set lob result failed", K(ret));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
