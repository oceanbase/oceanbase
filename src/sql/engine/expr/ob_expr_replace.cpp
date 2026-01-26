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


#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset_string_helper.h"

#if OB_USE_MULTITARGET_CODE
#define INIT_SEARCHER \
  if (OB_SUCC(ret)) { \
    if (!from_val.empty() && \
        OB_FAIL(reinterpret_cast<ObStringSearcher *>(string_searcher)->init(from_val.ptr(), from_val.length()))) { \
      LOG_WARN("failed to init string searcher", K(ret)); \
    } \
  };
#else
#define INIT_SEARCHER do { /* do nothing */ } while (0);
#endif

#define FETCH_DATA_AND_INIT_SEARCHER(i) do { \
  from_val = (OB_NOT_NULL(from_vec) && !from_vec->is_null(i)) \
            ? from_vec->get_string(i) : ObString(); \
  to_val   = (OB_NOT_NULL(to_vec)   && !to_vec->is_null(i))   \
              ? to_vec->get_string(i)   : ObString(); \
  if (is_text_tc) { \
    if (OB_NOT_NULL(from_vec) && \
        OB_FAIL(ObTextStringHelper::read_real_string_data( \
          calc_alloc, from_vec, \
          expr.args_[1]->datum_meta_, \
          expr.args_[1]->obj_meta_.has_lob_header(), \
          from_val, i))) { \
      LOG_WARN("failed to get string data:from_vec", K(ret), K(expr.args_[1]->datum_meta_)); \
    } else if (OB_NOT_NULL(to_vec) && \
               OB_FAIL(ObTextStringHelper::read_real_string_data( \
                 calc_alloc, to_vec, \
                 expr.args_[2]->datum_meta_, \
                 expr.args_[2]->obj_meta_.has_lob_header(), \
                 to_val, i))) { \
      LOG_WARN("failed to get string data:to_vec", K(ret), K(expr.args_[2]->datum_meta_)); \
    } \
  } \
  INIT_SEARCHER; \
  if (OB_SUCC(ret)) { \
    should_preserve_text_when_empty_from = (is_enable && COMPAT_MYSQL57 == compat_type && from_val.empty()); \
  } \
} while (0);

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
        type.set_type(ObLongTextType);
      } else {
        type.set_varchar();
        type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());
      }
      if (3 == param_num) {
        types_array[2].set_calc_type(ObVarcharType);
      }
      OZ(ObExprOperator::aggregate_charsets_for_string_result_with_comparison(type, types_array, 1, type_ctx));
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
    int64_t max_len = 0;
    if (lib::is_oracle_mode()) {
      result_len = types_array[0].get_calc_length();
      if (param_num == 2 || types_array[2].is_null() || types_array[2].get_calc_length() == 0) {
        // do nothing
      } else {
        if (OB_SUCC(ret)) {
          result_len *= types_array[2].get_calc_length();
          if (result_len > OB_MAX_LONGTEXT_LENGTH) {
            result_len = OB_MAX_LONGTEXT_LENGTH;
          }
        }
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
        if (type.is_lob()) {
          max_len = ObAccuracy::DDL_DEFAULT_ACCURACY[type.get_type()].get_length();
        } else {
          max_len = OB_MAX_VARCHAR_LENGTH;
        }
        if (result_len < 0 || result_len > max_len) {
          result_len = max_len;
        }
        type.set_length(result_len);
      }
    }
  }
  return ret;
}

int ObExprReplace::replace_dispatch(common::ObString &result,
                                    const ObCollationType cs_type,
                                    const common::ObString &text,
                                    const common::ObString &from,
                                    const common::ObString &to,
                                    ObExprStringBuf &string_buf,
                                    void *string_searcher,
                                    const int64_t max_len,
                                    bool need_check_from_to)
{
  int ret = OB_SUCCESS;
#if OB_USE_MULTITARGET_CODE
  // try to optimize it with SIMD.
  if (cs_type == CS_TYPE_UTF8MB4_BIN && common::is_arch_supported(ObTargetArch::AVX2)) {
    if (OB_FAIL(replace_by_simd(result, cs_type, text, from, to, string_buf, string_searcher, max_len, need_check_from_to))) {
      LOG_WARN("replace_by_simd failed", K(ret));
    }
  } else if (OB_FAIL(replace(result, cs_type, text, from, to, string_buf, max_len))) {
    LOG_WARN("replace failed", K(ret));
  }
#else
  if (OB_FAIL(replace(result, cs_type, text, from, to, string_buf, max_len))) {
    LOG_WARN("replace failed", K(ret));
  }
#endif
  return ret;
}

int ObExprReplace::replace(ObString &ret_str,
                           const ObCollationType cs_type,
                           const ObString &text,
                           const ObString &from,
                           const ObString &to,
                           ObExprStringBuf &string_buf,
                           const int64_t max_len)
{
  int ret = OB_SUCCESS;
  ObString dst_str;
  bool is_null = false;
  if (OB_UNLIKELY(text.length() <= 0)) {
    // Return empty string
    ret_str = ObString();
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
    } else if (OB_UNLIKELY((max_len - text.length()) / locations.count() < (to.length() - from.length()))) {
      ret = OB_ERR_VARCHAR_TOO_LONG;
      LOG_ERROR("Result of replace() was larger than max_len.",
           K(text.length()), K(to.length()), K(from.length()), K(max_len), K(locations.count()), K(ret));
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

#if OB_USE_MULTITARGET_CODE
int ObExprReplace::replace_by_simd(common::ObString &ret_str,
                                   const ObCollationType cs_type,
                                   const common::ObString &text,
                                   const common::ObString &from,
                                   const common::ObString &to,
                                   ObExprStringBuf &string_buf,
                                   void *string_searcher,
                                   const int64_t max_len,
                                   bool need_check_from_to)
{
  int ret = OB_SUCCESS;
  ObString dst_str;
  bool is_null = false;
  if (OB_UNLIKELY(text.length() <= 0)) {
    // Return empty string
    ret_str = ObString();
  } else if (OB_UNLIKELY(from.length() <= 0) || OB_UNLIKELY(to.length() < 0)) {
    ret_str = text;
  } else if (OB_ISNULL(from.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. nullpointer(s)", K(ret), K(from), K(to));
  } else if (OB_UNLIKELY(text.length() < from.length()) ||
             OB_UNLIKELY(from == to)) {
    ret_str = text;
  } else if (need_check_from_to && (OB_FAIL(ObSQLUtils::check_well_formed_str(text, cs_type, dst_str, is_null, false, false))
            || OB_FAIL(ObSQLUtils::check_well_formed_str(from, cs_type, dst_str, is_null, false, false))
            || OB_FAIL(ObSQLUtils::check_well_formed_str(to, cs_type, dst_str, is_null, false, false)))) {
    LOG_WARN("check well formed str failed", K(ret));
  } else if (!need_check_from_to && (OB_FAIL(ObSQLUtils::check_well_formed_str(text, cs_type, dst_str, is_null, false, false)))) {
    LOG_WARN("check well formed str failed", K(ret));
  } else {
    ObSEArray<int64_t, 4> locations(common::ObModIds::OB_SQL_EXPR_REPLACE,
                                     common::OB_MALLOC_NORMAL_BLOCK_SIZE);
    int64_t location = 0;
    int64_t last_pos = 0;
    bool find = true;
    while (OB_SUCC(ret) && OB_LIKELY(last_pos + from.length() <= text.length())) {
      if (OB_FAIL(reinterpret_cast<ObStringSearcher *>(string_searcher)->instr(text.ptr() + last_pos, text.ptr() + text.length(), location, find))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("string_searcher_instr failed", K(ret));
      } else if (find) {
        // Notice that location is the location of from in text, so we need to add last_pos to get the real location
        location = location + last_pos;
        ret = locations.push_back(location);
        // And the next appearance should be after location + from.length()
        last_pos = location + from.length();
      } else {
        break;
      }
    }
    int64_t tot_length = 0;
    if (OB_FAIL(ret)) {
      ret_str.reset();
    } else if (locations.count() == 0) {
      ret_str = text;
    } else if (OB_UNLIKELY((max_len - text.length()) / locations.count() < (to.length() - from.length()))) {
      ret = OB_ERR_VARCHAR_TOO_LONG;
      LOG_WARN("Result of replace() was larger than max_len.",
           K(text.length()), K(to.length()), K(from.length()), K(max_len), K(locations.count()), K(ret));
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
        LOG_WARN("alloc memory failed.", K(tot_length), K(ret));
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
#endif

OB_INLINE int get_compat_type(VECTOR_EVAL_FUNC_ARG_DECL, bool &is_enable, ObCompatType& compat_type) {
  int ret = OB_SUCCESS;
  is_enable = false;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  uint64_t compat_version = 0;
  compat_type = COMPAT_MYSQL57;
  if (OB_FAIL(helper.get_compat_version(compat_version))) {
    LOG_WARN("failed to get compat version", K(ret));
  } else if (OB_FAIL(ObCompatControl::check_feature_enable(compat_version,
                          ObCompatFeatureType::FUNC_REPLACE_NULL, is_enable))) {
    LOG_WARN("failed to check feature enable", K(ret));
  } else if (OB_FAIL(session->get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  }
  return ret;
}

int ObExprReplace::cg_expr(ObExprCGCtx &, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr *text_expr = raw_expr.get_param_expr(0);
  const ObRawExpr *from_expr = raw_expr.get_param_expr(1);
  const ObRawExpr *to_expr = raw_expr.get_param_expr(2);
  CK(2 == rt_expr.arg_cnt_ || 3 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = &eval_replace;
    rt_expr.eval_vector_func_ = &eval_replace_vector;
  }
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
    ObExpr *from_expr = expr.args_[1];
    int64_t from_len = 0;
    if (!ob_is_text_tc(from_expr->datum_meta_.type_)) {
      from_len = from->len_;
    } else {
      ObLobLocatorV2 locator(from->get_string(), from_expr->obj_meta_.has_lob_header());
      if (OB_FAIL(locator.get_lob_data_byte_len(from_len))) {
        LOG_WARN("get lob data byte length failed", K(ret), K(locator));
      }
    }
    if (OB_FAIL(ret)){
    } else if (is_mysql && !from->is_null() && 0 == from_len) {
      ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
      const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
      uint64_t compat_version = 0;
      ObCompatType compat_type = COMPAT_MYSQL57;
      bool is_enable = false;
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session info is null", K(ret));
      } else if (OB_FAIL(helper.get_compat_version(compat_version))) {
        LOG_WARN("failed to get compat version", K(ret));
      } else if (OB_FAIL(ObCompatControl::check_feature_enable(compat_version,
                                              ObCompatFeatureType::FUNC_REPLACE_NULL, is_enable))) {
        LOG_WARN("failed to check feature enable", K(ret));
      } else if (OB_FAIL(session->get_compatibility_control(compat_type))) {
        LOG_WARN("failed to get compat type", K(ret));
      } else if (is_enable && COMPAT_MYSQL57 == compat_type) {
        expr_datum.set_datum(*text);
      } else {
        expr_datum.set_null();
      }
    } else {
      expr_datum.set_null();
    }
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
      if (res.empty() && !is_mysql && !is_clob) {
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
      int64_t max_len = ObAccuracy::DDL_DEFAULT_ACCURACY[expr.datum_meta_.get_type()].get_length();
      if (OB_FAIL(replace(res, expr.datum_meta_.cs_type_, text_data, from_data,
                          to_data, temp_allocator, max_len))) {
        LOG_WARN("do replace for lob resutl failed", K(ret), K(expr.datum_meta_.type_));
      } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(expr, ctx, expr_datum, res))) {
        LOG_WARN("set lob result failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprReplace::eval_replace_vector(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  ObString res;
  const bool is_mysql = lib::is_mysql_mode();
  ObExpr *text = expr.args_[0];
  ObExpr *from = expr.args_[1];
  ObExpr *to = (expr.arg_cnt_ > 2) ? expr.args_[2] : nullptr;
  if (OB_FAIL(from->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval from failed", K(ret));
  } else if (OB_NOT_NULL(to) && OB_FAIL(to->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval to failed", K(ret));
  } else if (OB_FAIL(text->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval text batch failed", K(ret));
  } else {
    VectorFormat text_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    bool both_from_and_to_const = false;
    if (2 == expr.arg_cnt_ && !expr.args_[1]->is_batch_result()) {
      both_from_and_to_const = true;
    } else if (3 == expr.arg_cnt_ && !expr.args_[1]->is_batch_result() && !expr.args_[2]->is_batch_result()) {
      both_from_and_to_const = true;
    } else {
      both_from_and_to_const = false;
    }
    if (both_from_and_to_const) {
      if (VEC_DISCRETE == text_format && VEC_DISCRETE == res_format) {
      ret = replace_vector_inner<StrDiscVec, ConstUniformFormat, ConstUniformFormat, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == text_format && VEC_DISCRETE == res_format) {
        ret = replace_vector_inner<StrUniVec, ConstUniformFormat, ConstUniformFormat, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == text_format && VEC_DISCRETE == res_format) {
        ret = replace_vector_inner<StrContVec, ConstUniformFormat, ConstUniformFormat, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == text_format && VEC_UNIFORM == res_format) {
        ret = replace_vector_inner<StrDiscVec, ConstUniformFormat, ConstUniformFormat, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == text_format && VEC_UNIFORM == res_format) {
        ret = replace_vector_inner<StrUniVec, ConstUniformFormat, ConstUniformFormat, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == text_format && VEC_UNIFORM == res_format) {
        ret = replace_vector_inner<StrContVec, ConstUniformFormat, ConstUniformFormat, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == text_format && VEC_CONTINUOUS == res_format) {
        ret = replace_vector_inner<StrDiscVec, ConstUniformFormat, ConstUniformFormat, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == text_format && VEC_CONTINUOUS == res_format) {
        ret = replace_vector_inner<StrUniVec, ConstUniformFormat, ConstUniformFormat, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == text_format && VEC_CONTINUOUS == res_format) {
        ret = replace_vector_inner<StrContVec, ConstUniformFormat, ConstUniformFormat, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = replace_vector_inner<ObVectorBase, ConstUniformFormat, ConstUniformFormat, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    } else {
      if (VEC_DISCRETE == text_format && VEC_DISCRETE == res_format) {
        ret = replace_vector_inner<StrDiscVec, ObVectorBase, ObVectorBase, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == text_format && VEC_DISCRETE == res_format) {
        ret = replace_vector_inner<StrUniVec, ObVectorBase, ObVectorBase, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == text_format && VEC_DISCRETE == res_format) {
        ret = replace_vector_inner<StrContVec, ObVectorBase, ObVectorBase, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == text_format && VEC_UNIFORM == res_format) {
        ret = replace_vector_inner<StrDiscVec, ObVectorBase, ObVectorBase, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == text_format && VEC_UNIFORM == res_format) {
        ret = replace_vector_inner<StrUniVec, ObVectorBase, ObVectorBase, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == text_format && VEC_UNIFORM == res_format) {
        ret = replace_vector_inner<StrContVec, ObVectorBase, ObVectorBase, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == text_format && VEC_CONTINUOUS == res_format) {
        ret = replace_vector_inner<StrDiscVec, ObVectorBase, ObVectorBase, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == text_format && VEC_CONTINUOUS == res_format) {
        ret = replace_vector_inner<StrUniVec, ObVectorBase, ObVectorBase, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == text_format && VEC_CONTINUOUS == res_format) {
        ret = replace_vector_inner<StrContVec, ObVectorBase, ObVectorBase, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = replace_vector_inner<ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    }

  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to eval_replace_vector", K(ret));
  }
  return ret;
}


template <typename TextVec, typename FromVec, typename ToVec, typename ResVec>
int ObExprReplace::replace_vector_inner(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObString res;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  const bool is_mysql = lib::is_mysql_mode();
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  bool is_text_tc = ob_is_text_tc(expr.datum_meta_.type_);

#if OB_USE_MULTITARGET_CODE
  ObStringSearcher string_searcher_entity;
  void *string_searcher = &string_searcher_entity;
#else
  void *string_searcher = nullptr;
#endif
  const TextVec *text_vec = static_cast<const TextVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  FromVec *from_vec = static_cast<FromVec *>(expr.args_[1]->get_vector(ctx));
  ToVec *to_vec = (expr.arg_cnt_ > 2) ? static_cast<ToVec *>(expr.args_[2]->get_vector(ctx)) : nullptr;
  constexpr bool both_from_and_to_const = std::is_same<FromVec, ConstUniformFormat>::value && std::is_same<ToVec, ConstUniformFormat>::value;
  ObString from_val;
  ObString to_val;
  bool is_enable;
  ObCompatType compat_type;
  if (OB_FAIL(get_compat_type(VECTOR_EVAL_FUNC_ARG_LIST, is_enable, compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  }
  // In MySQL mode, and `to` is NULL:
  // In Version 5.7, replace(text, '', NULL)     -> text
  // In Version 5.7, replace(text, 'from', NULL) -> NULL
  // In Version 8.0, replace(text, '', NULL)     -> NULL
  // use should_preserve_text_when_empty_from to tell from case1 and case3
  bool should_preserve_text_when_empty_from = false;

  // if both `from` and `to` are constants, fetch values only once
  if constexpr (both_from_and_to_const) {
    FETCH_DATA_AND_INIT_SEARCHER(0);
  }

  batch_info_guard.set_batch_size(bound.batch_size());
  bool first_eval = true;
  for (int64_t i = bound.start(); i < bound.end() && OB_SUCC(ret); i++) {
    ObExprStrResAlloc res_alloc(expr, ctx);
    batch_info_guard.set_batch_idx(i);
    if (!(skip.at(i) || eval_flags.at(i))) {
      if constexpr (!both_from_and_to_const) {
        FETCH_DATA_AND_INIT_SEARCHER(i);
      }
      if (OB_FAIL(ret)) {
      } else if (text_vec->is_null(i)
      || (is_mysql && OB_NOT_NULL(from_vec) && from_vec->is_null(i))
      || (is_mysql && OB_NOT_NULL(to_vec) && to_vec->is_null(i) && !should_preserve_text_when_empty_from)) {
        res_vec->set_null(i);
      } else if (!is_text_tc) {
        ObString text_val = text_vec->get_string(i);
        if (OB_FAIL(replace_dispatch(res, expr.datum_meta_.cs_type_, text_val,
                            from_val, to_val, res_alloc, string_searcher, OB_MAX_VARCHAR_LENGTH, first_eval || !both_from_and_to_const))) {
          LOG_WARN("failed to replace", K(ret));
        } else if (res.empty() && !is_mysql) {
          res_vec->set_null(i);
        } else {
          res_vec->set_string(i, res);
        }
      } else { // text tc
        ObTextStringVectorResult<ResVec> text_res(expr.datum_meta_.type_, &expr, &ctx, res_vec, i);
        ObString text_val = text_vec->get_string(i);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(calc_alloc, text_vec,
                    expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), text_val, i))) {
          LOG_WARN("failed to get string data:text_vec", K(ret), K(expr.args_[0]->datum_meta_));
        }

        if (OB_SUCC(ret)) {
          int64_t max_len = ObAccuracy::DDL_DEFAULT_ACCURACY[expr.datum_meta_.get_type()].get_length();
          if (OB_FAIL(replace_dispatch(res, expr.datum_meta_.cs_type_, text_val, from_val,
                              to_val, calc_alloc, string_searcher, max_len, first_eval || !both_from_and_to_const))) {
            LOG_WARN("do replace for lob result failed", K(ret), K(expr.datum_meta_.type_));
          } else if (OB_FAIL(text_res.init_with_batch_idx(res.length(), i))) {
            LOG_WARN("init lob result failed", K(ret));
          } else if (OB_FAIL(text_res.append(res.ptr(), res.length()))) {
            LOG_WARN("failed to append realdata", K(ret), K(res), K(text_res));
          } else {
            text_res.set_result();
          }
        }
      }
      if (OB_SUCC(ret)) {
        first_eval = false;
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprReplace, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_OB_COMPATIBILITY_VERSION);
  return ret;
}

} // namespace sql
} // namespace oceanbase
