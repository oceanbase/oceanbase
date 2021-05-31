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
#include "sql/engine/expr/ob_expr_trim.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"
namespace oceanbase {
using namespace common;
namespace sql {

ObExprTrim::ObExprTrim(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_TRIM, N_TRIM, MORE_THAN_ZERO)
{
  need_charset_convert_ = false;
}

ObExprTrim::ObExprTrim(ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num)
    : ObStringExprOperator(alloc, type, name, param_num)
{}

ObExprTrim::~ObExprTrim()
{}

int ObExprTrim::trim(ObString& result, const int64_t trim_type, const ObString& pattern, const ObString& text)
{
  int ret = OB_SUCCESS;
  int32_t start = 0;
  int32_t end = 0;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    start = 0;
    end = text.length();
  } else {
    switch (trim_type) {
      case TYPE_LRTRIM: {
        ret = lrtrim(text, pattern, start, end);
        break;
      }
      case TYPE_LTRIM: {
        end = text.length();
        ret = ltrim(text, pattern, start);
        break;
      }
      case TYPE_RTRIM: {
        start = 0;
        ret = rtrim(text, pattern, end);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type", K(trim_type), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    result = ObString(end - start, end - start, const_cast<char*>(text.ptr() + start));
  }
  return ret;
}

// for func Ltrim/Rtrim which has 2 string param
int ObExprTrim::trim2(common::ObString& result, const int64_t trim_type, const common::ObString& pattern,
    const common::ObString& text, const ObCollationType& cs_type,
    const ObFixedArray<size_t, ObIAllocator>& pattern_byte_num,
    const ObFixedArray<size_t, ObIAllocator>& pattern_byte_offset)
{
  int ret = OB_SUCCESS;
  int32_t start = 0;
  int32_t end = 0;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    start = 0;
    end = text.length();
  } else {
    switch (trim_type) {
      case TYPE_LTRIM: {
        end = text.length();
        ret = ltrim2(text, pattern, start, cs_type, pattern_byte_num, pattern_byte_offset);
        break;
      }
      case TYPE_RTRIM: {
        start = 0;
        ret = rtrim2(text, pattern, end, cs_type, pattern_byte_num, pattern_byte_offset);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type", K(trim_type), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    result = ObString(end - start, end - start, const_cast<char*>(text.ptr() + start));
  }
  return ret;
}

int ObExprTrim::calc(ObObj& result, const int64_t trim_type, const ObString& pattern, const ObString& text,
    const ObObjType& res_type, ObExprCtx& expr_ctx)
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  ObString varchar;
  if (OB_FAIL(trim(varchar, trim_type, pattern, text))) {
    LOG_WARN("do trim failed", K(ret));
  } else {
    if (share::is_oracle_mode() && (0 == varchar.length()) && !ob_is_text_tc(res_type)) {
      result.set_null();
    } else if (ob_is_text_tc(res_type)) {
      result.set_lob_value(res_type, varchar.ptr(), varchar.length());
    } else {
      result.set_varchar(varchar);
    }
  }
  return ret;
}

int ObExprTrim::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t trim_type_val = TYPE_LRTRIM;
  ObString pattern_val;
  char default_pattern_buffer[8];  // 8 is enough for all charset
  ObString text_val;
  bool is_result_null = false;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num <= 0 || param_num > 3) || OB_ISNULL(objs_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs_array));
  } else if (param_num == 1) {
    if (objs_array[0].is_null()) {
      result.set_null();
      is_result_null = true;
    } else {
      trim_type_val = TYPE_LRTRIM;
      int64_t out_len = 0;
      ObCollationType cs_type = (lib::is_oracle_mode() && !(expr_ctx.my_session_->use_static_typing_engine()))
                                    ? expr_ctx.my_session_->get_nls_collation()
                                    : result_type_.get_collation_type();
      if (OB_FAIL(fill_default_pattern(default_pattern_buffer, sizeof(default_pattern_buffer), cs_type, out_len))) {
      } else if (out_len <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected out length", K(ret), K(out_len));
      } else {
        pattern_val.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
      }
      text_val = objs_array[0].get_string();
    }
  } else if (param_num == 2) {
    if (OB_UNLIKELY(objs_array[0].is_null()) || OB_UNLIKELY(objs_array[1].is_null())) {
      result.set_null();
      is_result_null = true;
    } else {
      int64_t out_len = 0;
      ObCollationType cs_type = (lib::is_oracle_mode() && !(expr_ctx.my_session_->use_static_typing_engine()))
                                    ? expr_ctx.my_session_->get_nls_collation()
                                    : result_type_.get_collation_type();
      if (OB_FAIL(fill_default_pattern(default_pattern_buffer, sizeof(default_pattern_buffer), cs_type, out_len))) {
      } else if (out_len <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected out length", K(ret), K(out_len));
      } else {
        pattern_val.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
      }
      trim_type_val = objs_array[0].get_int();
      text_val = objs_array[1].get_string();
    }
  } else if (param_num == 3) {
    if (OB_UNLIKELY(objs_array[0].is_null()) || OB_UNLIKELY(objs_array[1].is_null()) ||
        OB_UNLIKELY(objs_array[2].is_null())) {
      result.set_null();
      is_result_null = true;
    } else {
      trim_type_val = objs_array[0].get_int();
      pattern_val = objs_array[1].get_string();
      text_val = objs_array[2].get_string();
    }
  }
  if (OB_LIKELY(OB_SUCC(ret) && !is_result_null)) {
    ObCollationType cs_type = result_type_.get_collation_type();
    if (share::is_oracle_mode() && ObCharset::strlen_char(cs_type, pattern_val.ptr(), pattern_val.length()) > 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("trim set should have only one character", K(ret), K(pattern_val));
    } else {
      ret = calc(result, trim_type_val, pattern_val, text_val, get_result_type().get_type(), expr_ctx);
      if (OB_LIKELY(OB_SUCC(ret) && !result.is_null())) {
        if (share::is_oracle_mode()) {
          if (OB_NOT_NULL(expr_ctx.my_session_)) {
            result.set_collation_type(expr_ctx.my_session_->get_nls_collation());
          }
          if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
            LOG_WARN("fail to convert result collation", K(ret));
          }
        }
        result.set_collation(result_type_);
      }
    }
  }
  return ret;
}

int ObExprTrim::lrtrim(const ObString src, const ObString pattern, int32_t& start, int32_t& end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pattern.length()));
  } else {
    int32_t i = 0;
    start = 0;
    end = src.length();
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    for (i = 0; OB_SUCC(ret) && i <= src_len - pattern_len; i += pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        start += pattern_len;
      } else {
        break;
      }
    }  // end for
    for (i = src_len - pattern_len; OB_SUCC(ret) && i >= start; i -= pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        end -= pattern_len;
      } else {
        break;
      }
    }  // end for
  }
  return ret;
}

// NOTE: pattern.length() MUST > 0
int ObExprTrim::ltrim(const ObString src, const ObString pattern, int32_t& start)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pattern.length()));
  } else {
    start = 0;
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    for (int32_t i = 0; OB_SUCC(ret) && i <= src_len - pattern_len; i += pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        start += pattern_len;
      } else {
        break;
      }
    }  // end for
  }
  return ret;
}

// NOTE: pattern.length() MUST > 0
int ObExprTrim::rtrim(const ObString src, const ObString pattern, int32_t& end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pattern.length()));
  } else {
    end = src.length();
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    for (int32_t i = src_len - pattern_len; OB_SUCC(ret) && i >= 0; i -= pattern_len) {
      if (0 == MEMCMP(src.ptr() + i, pattern.ptr(), pattern_len)) {
        end -= pattern_len;
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObExprTrim::ltrim2(const ObString src, const ObString pattern, int32_t& start, const ObCollationType& cs_type,
    const ObFixedArray<size_t, ObIAllocator>& pattern_byte_num,
    const ObFixedArray<size_t, ObIAllocator>& pattern_byte_offset)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invaild arguemnt", K(ret), K(pattern.length()));
  } else {
    int32_t i = 0;
    start = 0;
    int32_t src_len = src.length();
    int32_t pattern_len = pattern.length();
    bool is_match = false;
    do {
      is_match = false;
      for (i = 0; OB_SUCC(ret) && i < pattern_byte_num.count() && !is_match; ++i) {
        int32_t char_len = pattern_byte_num[i];
        int32_t pattern_offset = pattern_byte_offset[i];
        if (src_len - start < char_len) {
        } else if (0 == MEMCMP(src.ptr() + start, pattern.ptr() + pattern_offset, char_len)) {
          start += char_len;
          is_match = true;
        } else {
          continue;
        }
      }
    } while (is_match);
  }
  return ret;
}

int ObExprTrim::rtrim2(const ObString src, const ObString pattern, int32_t& end, const ObCollationType& cs_type,
    const ObFixedArray<size_t, ObIAllocator>& pattern_byte_num,
    const ObFixedArray<size_t, ObIAllocator>& pattern_byte_offset)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pattern.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invaild arguemnt", K(ret), K(pattern.length()));
  } else {
    int32_t i = 0;
    int32_t src_len = src.length();
    end = src_len;
    int32_t pattern_len = pattern.length();

    bool is_match = false;
    do {
      is_match = false;
      for (i = 0; OB_SUCC(ret) && i < pattern_byte_num.count() && !is_match; ++i) {
        int32_t char_len = pattern_byte_num[i];
        int32_t pattern_offset = pattern_byte_offset[i];
        if (end < char_len) {
        } else if (0 == MEMCMP(src.ptr() + end - char_len, pattern.ptr() + pattern_offset, char_len)) {
          end -= char_len;
          is_match = true;
        } else {
          continue;
        }
      }
    } while (is_match);
  }
  return ret;
}

inline int ObExprTrim::deduce_result_type(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx)
{
  int ret = OB_SUCCESS;
  CK(NULL != types);
  CK(NULL != type_ctx.get_session());
  CK(param_num >= 1 && param_num <= 3);

  ObExprResType* str_type = &types[param_num - 1];
  type.set_length(types[param_num - 1].get_length());
  ObExprResType* pattern_type = (3 == param_num) ? &types[1] : NULL;
  if (param_num > 2) {
    types[0].set_calc_type(ObIntType);
  }

  if (lib::is_oracle_mode() && type_ctx.get_session()->use_static_typing_engine()) {
    auto str_params = make_const_carray(str_type);
    LOG_DEBUG("str_type is", K(str_type));
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params, type, true));  // prefer varchar
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));
    if (NULL != pattern_type) {
      pattern_type->set_calc_meta(type);
    }
  } else {
    if (str_type->is_lob()) {
      type.set_type(str_type->get_type());
      str_type->set_calc_type(str_type->get_type());
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(str_type->get_type());
      }
    } else if (str_type->is_nstring()) {
      type.set_meta(*str_type);
      type.set_type_simple(ObNVarchar2Type);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    } else {
      const common::ObLengthSemantics default_length_semantics =
          (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                               : common::LS_BYTE);
      type.set_varchar();
      type.set_length_semantics(
          types[0].is_varchar_or_char() ? types[0].get_length_semantics() : default_length_semantics);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    }
    // deduce charset
    ObSEArray<ObExprResType, 2> tmp_types;
    OZ(tmp_types.push_back(*str_type));
    if (NULL != pattern_type) {
      OZ(tmp_types.push_back(*pattern_type));
    }
    OZ(aggregate_charsets_for_string_result_with_comparison(
        type, &tmp_types.at(0), tmp_types.count(), type_ctx.get_coll_type()));
    str_type->set_calc_collation_type(type.get_collation_type());
    str_type->set_calc_collation_level(type.get_collation_level());
    if (NULL != pattern_type) {
      pattern_type->set_calc_collation_type(type.get_collation_type());
      pattern_type->set_calc_collation_level(type.get_collation_level());
    }
  }

  return ret;
}

int ObExprTrim::fill_default_pattern(char* buf, const int64_t in_len, ObCollationType cs_type, int64_t& out_len)
{
  int ret = OB_SUCCESS;
  char default_pattern = ' ';
  const ObCharsetInfo* cs = ObCharset::get_charset(cs_type);
  if (OB_ISNULL(cs) || OB_ISNULL(cs->cset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected collation type", K(ret), K(cs_type), K(cs));
  } else if (NULL == buf || in_len < cs->mbminlen) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), KP(buf), K(in_len), K(cs->mbminlen));
  } else if (1 == cs->mbminlen) {
    *buf = default_pattern;
    out_len = 1;
  } else {
    // mbminlen is always enough
    cs->cset->fill(cs, buf, cs->mbminlen, default_pattern);
    out_len = cs->mbminlen;
  }
  return ret;
}

int ObExprTrim::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 3);
  rt_expr.eval_func_ = eval_trim;
  return ret;
}

int ObExprTrim::eval_trim(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    for (int64_t i = 0; i < expr.arg_cnt_; i++) {
      if (expr.locate_param_datum(ctx, i).is_null()) {
        has_null = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (has_null) {
    expr_datum.set_null();
  } else {
    int64_t trim_type = TYPE_LRTRIM;
    // may be called by ltrim() or rtrim()
    if (1 == expr.arg_cnt_) {
      if (T_FUN_SYS_LTRIM == expr.type_) {
        trim_type = TYPE_LTRIM;
      } else if (T_FUN_SYS_RTRIM == expr.type_) {
        trim_type = TYPE_RTRIM;
      }
    } else if (2 == expr.arg_cnt_) {
      if (T_FUN_SYS_LTRIM == expr.type_) {
        LOG_WARN("trim_type is ltrim");
        trim_type = TYPE_LTRIM;
      } else if (T_FUN_SYS_RTRIM == expr.type_) {
        LOG_WARN("trim type is rtrim");
        trim_type = TYPE_RTRIM;
      } else {
        trim_type = expr.locate_param_datum(ctx, 0).get_int();
        LOG_WARN("trim type is ", K(trim_type));
      }
    } else {
      trim_type = expr.locate_param_datum(ctx, 0).get_int();
    }

    ObString str;
    char default_pattern_buffer[8];
    ObString pattern;
    bool res_is_clob = false;
    if (2 == expr.arg_cnt_ && (T_FUN_SYS_LTRIM == expr.type_ || T_FUN_SYS_RTRIM == expr.type_)) {
      str = expr.locate_param_datum(ctx, expr.arg_cnt_ - 2).get_string();
      pattern = expr.locate_param_datum(ctx, expr.arg_cnt_ - 1).get_string();
      res_is_clob = lib::is_oracle_mode() && ob_is_text_tc(expr.args_[expr.arg_cnt_ - 2]->datum_meta_.type_) &&
                    (CS_TYPE_BINARY != expr.args_[expr.arg_cnt_ - 2]->datum_meta_.cs_type_);
    } else {
      str = expr.locate_param_datum(ctx, expr.arg_cnt_ - 1).get_string();
      res_is_clob = lib::is_oracle_mode() && ob_is_text_tc(expr.args_[expr.arg_cnt_ - 1]->datum_meta_.type_) &&
                    (CS_TYPE_BINARY != expr.args_[expr.arg_cnt_ - 1]->datum_meta_.cs_type_);
      if (3 == expr.arg_cnt_) {
        pattern = expr.locate_param_datum(ctx, 1).get_string();
        if (lib::is_oracle_mode() &&
            1 < ObCharset::strlen_char(expr.datum_meta_.cs_type_, pattern.ptr(), pattern.length())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("trim set should have only one character", K(ret), K(pattern));
        }
      } else {
        int64_t out_len = 0;
        if (OB_FAIL(fill_default_pattern(
                default_pattern_buffer, sizeof(default_pattern_buffer), expr.datum_meta_.cs_type_, out_len))) {
        } else if (out_len <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected out length", K(ret), K(out_len));
        } else {
          pattern.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
        }
      }
    }

    ObString output;
    // 2 param for Ltrim or Rtrim
    if (2 == expr.arg_cnt_ && (T_FUN_SYS_LTRIM == expr.type_ || T_FUN_SYS_RTRIM == expr.type_)) {
      ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
      ObCollationType cs_type = expr.datum_meta_.cs_type_;
      size_t pattern_len_in_char = ObCharset::strlen_char(cs_type, pattern.ptr(), pattern.length());
      ObFixedArray<size_t, ObIAllocator> pattern_byte_num(calc_alloc, pattern_len_in_char);
      ObFixedArray<size_t, ObIAllocator> pattern_byte_offset(calc_alloc, pattern_len_in_char + 1);
      if (OB_FAIL(ObExprUtil::get_mb_str_info(pattern, cs_type, pattern_byte_num, pattern_byte_offset))) {
        LOG_WARN("get_mb_str_info failed", K(ret), K(pattern), K(cs_type), K(pattern_len_in_char));
      } else if (!res_is_clob && (pattern_byte_num.count() + 1 != pattern_byte_offset.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size of pattern_byte_num and size of pattern_byte_offset should be same",
            K(ret),
            K(pattern_byte_num),
            K(pattern_byte_offset));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trim2(output, trim_type, pattern, str, cs_type, pattern_byte_num, pattern_byte_offset))) {
        LOG_WARN("do trim2 failed", K(ret));
      } else {
        if (output.empty() && lib::is_oracle_mode() && !res_is_clob) {
          expr_datum.set_null();
        } else {
          expr_datum.set_string(output);
        }
      }
    } else {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trim(output, trim_type, pattern, str))) {
        LOG_WARN("do trim failed", K(ret));
      } else {
        if (output.empty() && lib::is_oracle_mode() && !res_is_clob) {
          expr_datum.set_null();
        } else {
          expr_datum.set_string(output);
        }
      }
    }
  }
  return ret;
}

// Ltrim start
ObExprLtrim::ObExprLtrim(ObIAllocator& alloc)
    : ObExprTrim(alloc, T_FUN_SYS_LTRIM, N_LTRIM, (share::is_oracle_mode()) ? ONE_OR_TWO : 1)
{}

ObExprLtrim::ObExprLtrim(ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num)
    : ObExprTrim(alloc, type, name, param_num)
{}

ObExprLtrim::~ObExprLtrim()
{}

inline int ObExprLtrim::calc_result_type1(ObExprResType& res_type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_session());
  // old engine, keep old logic.
  if (OB_SUCC(ret) && !type_ctx.get_session()->use_static_typing_engine()) {
    if (type1.is_lob()) {
      res_type.set_type(type1.get_type());
    } else if (type1.is_nstring()) {
      res_type.set_meta(type1);
    } else {
      res_type.set_varchar();
      const common::ObLengthSemantics default_length_semantics =
          (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                               : common::LS_BYTE);
      res_type.set_length_semantics(
          type1.is_varchar_or_char() ? type1.get_length_semantics() : default_length_semantics);
    }
    if (type1.is_null()) {
      res_type.set_null();
    } else {
      res_type.set_length(type1.get_length());
      type1.set_calc_type(ObVarcharType);
      ret = aggregate_charsets_for_string_result_with_comparison(res_type, &type1, 1, type_ctx.get_coll_type());
    }
  }

  // static typing engine
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    OZ(ObExprTrim::deduce_result_type(res_type, &type1, 1, type_ctx));
  }
  return ret;
}

inline int ObExprLtrim::deduce_result_type(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx)
{
  int ret = OB_SUCCESS;
  CK(NULL != types);
  CK(NULL != type_ctx.get_session());
  CK(param_num >= 1 && param_num <= 2);

  ObExprResType* str_type = &types[0];
  type.set_length(types[0].get_length());
  ObExprResType* pattern_type = (2 == param_num) ? &types[1] : NULL;

  if (lib::is_oracle_mode() && type_ctx.get_session()->use_static_typing_engine()) {
    auto str_params = make_const_carray(str_type);
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params, type, true));  // prefer varchar
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));
    if (NULL != pattern_type) {
      pattern_type->set_calc_meta(type);
    }
  } else {
    if (str_type->is_lob()) {
      type.set_type(str_type->get_type());
      str_type->set_calc_type(str_type->get_type());
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(pattern_type->get_type());
      }
    } else if (str_type->is_nstring()) {
      type.set_meta(*str_type);
      type.set_type_simple(ObNVarchar2Type);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    } else {
      const common::ObLengthSemantics default_length_semantics =
          (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                               : common::LS_BYTE);
      type.set_varchar();
      type.set_length_semantics(
          types[0].is_varchar_or_char() ? types[0].get_length_semantics() : default_length_semantics);
      str_type->set_calc_type(ObVarcharType);
      if (NULL != pattern_type) {
        pattern_type->set_calc_type(ObVarcharType);
      }
    }
    // deduce charset
    ObSEArray<ObExprResType, 2> tmp_types;
    OZ(tmp_types.push_back(*str_type));
    if (NULL != pattern_type) {
      OZ(tmp_types.push_back(*pattern_type));
    }
    OZ(aggregate_charsets_for_string_result_with_comparison(
        type, &tmp_types.at(0), tmp_types.count(), type_ctx.get_coll_type()));
    str_type->set_calc_collation_type(type.get_collation_type());
    str_type->set_calc_collation_level(type.get_collation_level());
    if (NULL != pattern_type) {
      pattern_type->set_calc_collation_type(type.get_collation_type());
      pattern_type->set_calc_collation_level(type.get_collation_level());
    }
  }
  return ret;
}

int ObExprLtrim::calc_result1(ObObj& res_obj, const ObObj& obj1, ObExprCtx& expr_ctx) const
{
  return calc(res_obj, obj1, expr_ctx, TYPE_LTRIM);
}

// @param: trim_type_use
//  1: LTRIM or 2: RTRIM
int ObExprLtrim::calc(ObObj& res_obj, const ObObj& obj1, ObExprCtx& expr_ctx, int64_t trim_type_use) const
{
  int ret = OB_SUCCESS;
  ObString pattern_val;
  char default_pattern_buffer[8];  // 8 is enough for all charset
  ObString text_val;
  bool is_result_null = false;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("calc_buf in not init", K(ret), K(expr_ctx.calc_buf_));
  } else {
    if (obj1.is_null()) {
      res_obj.set_null();
      is_result_null = true;
    } else {
      int64_t out_len = 0;
      ObCollationType cs_type = (lib::is_oracle_mode() && !(expr_ctx.my_session_->use_static_typing_engine()))
                                    ? expr_ctx.my_session_->get_nls_collation()
                                    : result_type_.get_collation_type();
      if (OB_FAIL(fill_default_pattern(default_pattern_buffer, sizeof(default_pattern_buffer), cs_type, out_len))) {
      } else if (out_len <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected out length", K(ret), K(out_len));
      } else {
        pattern_val.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
      }
      text_val = obj1.get_string();
    }
  }

  if (OB_LIKELY(OB_SUCC(ret) && !is_result_null)) {
    ret = ObExprTrim::calc(res_obj, trim_type_use, pattern_val, text_val, get_result_type().get_type(), expr_ctx);
    if (OB_LIKELY(OB_SUCC(ret) && !res_obj.is_null())) {
      if (share::is_oracle_mode()) {
        if (OB_NOT_NULL(expr_ctx.my_session_)) {
          res_obj.set_collation_type(expr_ctx.my_session_->get_nls_collation());
        }
        if (OB_FAIL(convert_result_collation(result_type_, res_obj, expr_ctx.calc_buf_))) {
          LOG_WARN("fail to convert result collation", K(ret));
        }
      }
      res_obj.set_collation(result_type_);
    }
  }

  return ret;
}

int ObExprLtrim::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t trim_type_val = TYPE_LTRIM;
  ObString pattern_val;
  char default_pattern_buffer[8];  // 8 is enough for all charset
  ObString text_val;
  bool is_result_null = false;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num <= 0 || param_num > 2 || OB_ISNULL(objs_array))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs_array));
  } else if (1 == param_num) {
    if (objs_array[0].is_null()) {
      result.set_null();
      is_result_null = true;
    } else {
      int64_t out_len = 0;
      ObCollationType cs_type = (lib::is_oracle_mode() && !(expr_ctx.my_session_->use_static_typing_engine()))
                                    ? expr_ctx.my_session_->get_nls_collation()
                                    : result_type_.get_collation_type();
      if (OB_FAIL(fill_default_pattern(default_pattern_buffer, sizeof(default_pattern_buffer), cs_type, out_len))) {
      } else if (out_len <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected out length", K(ret), K(out_len));
      } else {
        pattern_val.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
      }
    }
    text_val = objs_array[0].get_string();
  } else if (param_num == 2) {
    if (OB_UNLIKELY(objs_array[0].is_null()) || OB_UNLIKELY(objs_array[1].is_null())) {
      result.set_null();
      is_result_null = true;
    } else {
      pattern_val = objs_array[1].get_string();
      text_val = objs_array[0].get_string();
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret && !is_result_null)) {
    ObCollationType cs_type = result_type_.get_collation_type();
    ret = calc_oracle_mode(
        result, trim_type_val, pattern_val, text_val, get_result_type().get_type(), expr_ctx, param_num, cs_type);
    if (OB_LIKELY(OB_SUCCESS == ret && !result.is_null())) {
      if (share::is_oracle_mode()) {
        if (OB_NOT_NULL(expr_ctx.my_session_)) {
          result.set_collation_type(expr_ctx.my_session_->get_nls_collation());
        }
        if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
          LOG_WARN("fail to convert result collation", K(ret));
        }
      }
      result.set_collation(result_type_);
    }
  }

  return ret;
}

int ObExprLtrim::calc_oracle_mode(ObObj& result, const int64_t trim_type, const ObString& pattern, const ObString& text,
    const ObObjType& res_type, ObExprCtx& expr_ctx, const int64_t param_num, const ObCollationType& cs_type)
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  ObString varchar;
  if (1 == param_num) {
    if (OB_FAIL(trim(varchar, trim_type, pattern, text))) {
      LOG_WARN("do trim failed", K(ret));
    } else {
      if (share::is_oracle_mode() && (0 == varchar.length()) && !ob_is_text_tc(res_type)) {
        result.set_null();
      } else if (ob_is_text_tc(res_type)) {
        result.set_lob_value(res_type, varchar.ptr(), varchar.length());
      } else {
        result.set_varchar(varchar);
      }
    }
  } else if (2 == param_num) {
    size_t pattern_len_in_char = ObCharset::strlen_char(cs_type, pattern.ptr(), pattern.length());
    ObFixedArray<size_t, ObIAllocator> pattern_byte_num(expr_ctx.calc_buf_, pattern_len_in_char);
    ObFixedArray<size_t, ObIAllocator> pattern_byte_offset(expr_ctx.calc_buf_, pattern_len_in_char + 1);
    if (OB_FAIL(ObExprUtil::get_mb_str_info(pattern, cs_type, pattern_byte_num, pattern_byte_offset))) {
      LOG_WARN("get_mb_str_info failed", K(ret), K(pattern), K(cs_type), K(pattern_len_in_char));
    } else if (!ob_is_text_tc(res_type) && (pattern_byte_num.count() + 1 != pattern_byte_offset.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("size of pattern_byte_num and size of pattern_byte_offset should be same",
          K(ret),
          K(pattern_byte_num),
          K(pattern_byte_offset));
    }
    if (OB_FAIL(trim2(varchar, trim_type, pattern, text, cs_type, pattern_byte_num, pattern_byte_offset))) {
      LOG_WARN("do trim2 failed", K(ret));
    } else {
      if (share::is_oracle_mode() && 0 == varchar.length() && !ob_is_text_tc(res_type)) {
        result.set_null();
      } else if (ob_is_text_tc(res_type)) {
        result.set_lob_value(res_type, varchar.ptr(), varchar.length());
      } else {
        result.set_varchar(varchar);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num));
  }
  return ret;
}

int ObExprLtrim::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_ || 2 == rt_expr.arg_cnt_);
  // trim type is detected by expr type in ObExprTrim::eval_trim
  rt_expr.eval_func_ = &ObExprTrim::eval_trim;
  return ret;
}

// Rtrim start
ObExprRtrim::ObExprRtrim(ObIAllocator& alloc)
    : ObExprLtrim(alloc, T_FUN_SYS_RTRIM, N_RTRIM, (share::is_oracle_mode()) ? ONE_OR_TWO : 1)
{}

ObExprRtrim::~ObExprRtrim()
{}

inline int ObExprRtrim::calc_result_type1(ObExprResType& res_type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  return ObExprLtrim::calc_result_type1(res_type, type1, type_ctx);
}

inline int ObExprRtrim::calc_result1(ObObj& res_obj, const ObObj& obj1, ObExprCtx& expr_ctx) const
{
  return ObExprLtrim::calc(res_obj, obj1, expr_ctx, TYPE_RTRIM);
}

int ObExprRtrim::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t trim_type_val = TYPE_RTRIM;
  ObString pattern_val;
  char default_pattern_buffer[8];  // 8 is enough for all charset
  ObString text_val;
  bool is_result_null = false;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num <= 0 || param_num > 2 || OB_ISNULL(objs_array))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs_array));
  } else if (1 == param_num) {
    if (objs_array[0].is_null()) {
      result.set_null();
      is_result_null = true;
    } else {
      int64_t out_len = 0;
      ObCollationType cs_type = (lib::is_oracle_mode() && !(expr_ctx.my_session_->use_static_typing_engine()))
                                    ? expr_ctx.my_session_->get_nls_collation()
                                    : result_type_.get_collation_type();
      if (OB_FAIL(fill_default_pattern(default_pattern_buffer, sizeof(default_pattern_buffer), cs_type, out_len))) {
      } else if (out_len <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected out length", K(ret), K(out_len));
      } else {
        pattern_val.assign_ptr(default_pattern_buffer, static_cast<int32_t>(out_len));
      }
    }
    text_val = objs_array[0].get_string();
  } else if (param_num == 2) {
    if (OB_UNLIKELY(objs_array[0].is_null()) || OB_UNLIKELY(objs_array[1].is_null())) {
      result.set_null();
      is_result_null = true;
    } else {
      pattern_val = objs_array[1].get_string();
      text_val = objs_array[0].get_string();
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret && !is_result_null)) {
    ObCollationType cs_type = result_type_.get_collation_type();
    ret = ObExprLtrim::calc_oracle_mode(
        result, trim_type_val, pattern_val, text_val, get_result_type().get_type(), expr_ctx, param_num, cs_type);
    if (OB_LIKELY(OB_SUCCESS == ret && !result.is_null())) {
      if (share::is_oracle_mode()) {
        if (OB_NOT_NULL(expr_ctx.my_session_)) {
          result.set_collation_type(expr_ctx.my_session_->get_nls_collation());
        }
        if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
          LOG_WARN("fail to convert result collation", K(ret));
        }
      }
      result.set_collation(result_type_);
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
