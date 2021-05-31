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

#include "lib/charset/ob_ctype.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_substrb.h"
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprSubstrb::ObExprSubstrb(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SUBSTRB, N_SUBSTRB, TWO_OR_THREE)
{}

ObExprSubstrb::~ObExprSubstrb()
{}

int ObExprSubstrb::calc_result_length_in_byte(const ObExprResType& type, ObExprResType* types_array, int64_t param_num,
    ObCollationType cs_type, int64_t& res_len) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
  expr_ctx.calc_buf_ = &allocator;

  ObObj pos_obj;
  ObObj len_obj;
  int64_t pos_val = -1;
  int64_t len_val = -1;
  int64_t mbmaxlen = -1;
  int64_t str_len_in_byte = -1;

  if (OB_ISNULL(types_array)) {
    ret = OB_NOT_INIT;
    LOG_WARN("types_array is NULL", K(ret));
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == cs_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cs_type is not valid", K(cs_type), K(ret));
  } else if (OB_UNLIKELY(2 != param_num && 3 != param_num)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("param_num invalid", K(param_num), K(ret));
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
    LOG_WARN("get mbmaxlen failed", K(cs_type), K(ret));
  } else {
    if (types_array[0].is_column()) {
      str_len_in_byte = types_array[0].get_length() * mbmaxlen;
    } else {
      str_len_in_byte = types_array[0].get_length();
    }

    pos_obj = types_array[1].get_param();
    if (2 == param_num) {
      len_obj.set_int(str_len_in_byte);
    } else {
      len_obj = types_array[2].get_param();
    }

    // create table substrb_test_tbl(c1 varchar2(64 char), c2 varchar2(7 byte)
    // GENERATED ALWAYS AS (substrb(c1, -1, 8)) VIRTUAL);
    // using the above table-building statement, when the parameter of substrb is negative, pos_obj is null
    if (pos_obj.is_null_oracle()) {
      pos_val = 1;
    } else {
      if (OB_FAIL(ObExprUtil::get_trunc_int64(pos_obj, expr_ctx, pos_val))) {
        LOG_WARN("get int64 failed", K(pos_obj), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (len_obj.is_null_oracle()) {
        len_val = str_len_in_byte;
      } else {
        if (OB_FAIL(ObExprUtil::get_trunc_int64(len_obj, expr_ctx, len_val))) {
          LOG_WARN("get int64 failed", K(len_obj), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 > pos_val) {
        pos_val = pos_val + str_len_in_byte;
      } else {
        // start counting from 1
        pos_val = pos_val - 1;
      }

      if ((pos_val > str_len_in_byte - 1) || (0 >= len_val)) {
        res_len = 0;
      } else if (pos_val + len_val >= str_len_in_byte) {
        res_len = str_len_in_byte - pos_val;
      } else {
        res_len = len_val;
      }
      if (type.is_varchar() || type.is_nvarchar2()) {
        res_len = res_len > OB_MAX_ORACLE_VARCHAR_LENGTH ? OB_MAX_ORACLE_VARCHAR_LENGTH : res_len;
      } else if (type.is_char() || type.is_nchar()) {
        res_len = res_len > OB_MAX_ORACLE_CHAR_LENGTH_BYTE ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : res_len;
      }
    }
  }
  return ret;
}

int ObExprSubstrb::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_array, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (!is_oracle_mode()) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("substrb is only for oracle mode", K(ret));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("types_array or session is null", K(ret));
  } else if (OB_UNLIKELY(2 != param_num && 3 != param_num)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("substrb should have two or three arguments", K(param_num), K(ret));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("types_array or session is null", K(ret), KP(types_array));
  } else {
    if (2 == param_num) {
      types_array[1].set_calc_type(ObNumberType);
      types_array[1].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    } else if (3 == param_num) {
      types_array[1].set_calc_type(ObNumberType);
      types_array[1].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
      types_array[2].set_calc_type(ObNumberType);
      types_array[2].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
    if (types_array[0].is_raw()) {
      types_array[0].set_calc_meta(types_array[0].get_obj_meta());
      type.set_raw();
    } else {
      ObSEArray<ObExprResType*, 1, ObNullAllocator> types;
      OZ(types.push_back(&types_array[0]));
      OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), types, type, true));
      OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, types));
      int64_t result_len = types_array[0].get_length();
      OZ(calc_result_length_in_byte(type, types_array, param_num, type_ctx.get_coll_type(), result_len));
      OX(type.set_length(result_len));
      OX(type.set_length_semantics(LS_BYTE));  // always byte semantics
    }
  }
  return ret;
}

int ObExprSubstrb::calc_result2(ObObj& result, const ObObj& text, const ObObj& start_pos, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc_buf is NULL", K(ret));
  } else if (text.is_null_oracle() || start_pos.is_null_oracle()) {
    result.set_null();
  } else {
    ObObjType res_type = get_result_type().get_type();
    TYPE_CHECK(start_pos, ObNumberType);
    ObString res_str;
    const ObString& str_val = text.get_varchar();
    int64_t start_pos_val = 0;
    if (is_called_in_sql()) {
      OZ(ObExprUtil::get_trunc_int64(start_pos, expr_ctx, start_pos_val));
    } else {
      OZ(ObExprUtil::get_round_int64(start_pos, expr_ctx, start_pos_val));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc(res_str,
                   str_val,
                   start_pos_val,
                   text.get_string().length() - start_pos_val + 1,
                   result_type_.get_collation_type(),
                   *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to calc for substrb", K(ret), K(text), K(start_pos), K(text.get_string().length()));
    } else if (res_str.empty() && !text.is_clob()) {
      result.set_null();
    } else {
      if (ob_is_text_tc(res_type)) {
        result.set_lob_value(res_type, res_str.ptr(), res_str.length());
      } else {
        result.set_varchar(res_str);
      }
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprSubstrb::calc_result3(
    ObObj& result, const ObObj& text, const ObObj& start_pos, const ObObj& length, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc_buf is NULL", K(ret));
  } else if (text.is_null_oracle() || start_pos.is_null_oracle() || length.is_null_oracle()) {
    result.set_null();
  } else {
    TYPE_CHECK(start_pos, ObNumberType);
    TYPE_CHECK(length, ObNumberType);
    const ObString& str_val = text.get_varchar();
    ObObjType res_type = get_result_type().get_type();
    int64_t start_pos_val = 0;
    int64_t length_val = 0;
    ObString res_str;
    if (is_called_in_sql()) {
      OZ(ObExprUtil::get_trunc_int64(start_pos, expr_ctx, start_pos_val));
      OZ(ObExprUtil::get_trunc_int64(length, expr_ctx, length_val));
    } else {
      OZ(ObExprUtil::get_round_int64(start_pos, expr_ctx, start_pos_val));
      OZ(ObExprUtil::get_round_int64(length, expr_ctx, length_val));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc(res_str,
                   str_val,
                   start_pos_val,
                   length_val,
                   result_type_.get_collation_type(),
                   *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to calc for substrb", K(text), K(start_pos), K(length), K(ret));
    } else if (res_str.empty() && !text.is_clob()) {
      result.set_null();
    } else {
      if (ob_is_text_tc(res_type)) {
        result.set_lob_value(res_type, res_str.ptr(), res_str.length());
      } else {
        result.set_varchar(res_str);
      }
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprSubstrb::calc(ObString& res_str, const ObString& text, int64_t start, int64_t length, ObCollationType cs_type,
    ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  res_str.reset();
  int64_t text_len = text.length();
  int64_t res_len = 0;
  if (OB_UNLIKELY(0 >= text_len || 0 >= length)) {
    // empty result string
    res_str.reset();
  } else if (OB_ISNULL(text.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text.ptr() is null", K(ret));
  } else {
    if (0 == start) {
      // the oracle starting position parameter 0 and 1 are equivalent
      start = 1;
    }
    start = (start > 0) ? (start - 1) : (start + text_len);
    if (OB_UNLIKELY(start < 0 || start >= text_len)) {
      res_str.reset();
    } else {
      char* buf = static_cast<char*>(alloc.alloc(text_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(text_len), K(ret));
      } else {
        MEMCPY(buf, text.ptr(), text_len);
        res_len = min(length, text_len - start);
        // oracle will set the illegal byte as a space
        if (OB_FAIL(handle_invalid_byte(buf, text_len, start, res_len, ' ', cs_type))) {
          LOG_WARN("handle invalid byte failed", K(start), K(res_len), K(cs_type));
        } else {
          res_str.assign_ptr(buf + start, static_cast<int32_t>(res_len));
        }
      }
    }
  }
  LOG_DEBUG("calc substrb done", K(ret), K(res_str));
  return ret;
}

int ObExprSubstrb::handle_invalid_byte(
    char* ptr, const int64_t text_len, int64_t& start, int64_t& len, char reset_char, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  int64_t mbminlen = 0;
  if (OB_FAIL(ObCharset::get_mbminlen_by_coll(cs_type, mbminlen))) {
    LOG_WARN("get mbminlen failed", K(cs_type), K(ret));
  } else {
    if (mbminlen > 1) {  // utf16: mbminlen is 2
      if (OB_FAIL(ignore_invalid_byte(ptr, text_len, start, len, cs_type))) {
        LOG_WARN("ignore_invalid_byte failed", K(ret));
      }
    } else {  // utf8/gbk/gb18030: mbminlen is 1
      if (OB_FAIL(reset_invalid_byte(ptr, text_len, start, len, reset_char, cs_type))) {
        LOG_WARN("reset_invalid_byte failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_UNLIKELY(len % mbminlen != 0)) {
      int64_t hex_len = 0;
      char hex_buf[1024] = {0};  // just print 512 bytes
      OZ(common::hex_print(ptr + start, len, hex_buf, sizeof(hex_buf), hex_len));
      if (OB_SUCC(ret)) {
        const char* charset_name = ObCharset::charset_name(cs_type);
        int64_t charset_name_len = strlen(charset_name);
        ret = OB_ERR_INVALID_CHARACTER_STRING;
        LOG_USER_ERROR(OB_ERR_INVALID_CHARACTER_STRING,
            static_cast<int>(charset_name_len),
            charset_name,
            static_cast<int>(hex_len),
            hex_buf);
      }
    }
  }
  return ret;
}

int ObExprSubstrb::ignore_invalid_byte(
    char* ptr, const int64_t text_len, int64_t& start, int64_t& len, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(len));
  } else if (0 > len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("len should be greater than zero", K(len), K(ret));
  } else if (len == 0) {
    // do nothing
  } else {
    int64_t end = start + len;
    int64_t boundary_pos = 0;
    int64_t boundary_len = 0;
    OZ(get_well_formatted_boundary(cs_type, ptr, text_len, start, boundary_pos, boundary_len));
    if (OB_SUCC(ret)) {
      if (boundary_len < 0) {
        // invalid character found, do nothing.
      } else {
        if (start > boundary_pos) {
          start = boundary_pos + boundary_len;
        }
        if (start >= end) {
          len = 0;
        } else {
          len = end - start;
          OZ(get_well_formatted_boundary(cs_type, ptr + start, text_len - start, len, boundary_pos, boundary_len));
          if (OB_SUCC(ret)) {
            if (boundary_len < 0) {
              // invalid character found, do nothing.
            } else {
              len = boundary_pos;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprSubstrb::reset_invalid_byte(
    char* ptr, const int64_t text_len, int64_t start, int64_t len, char reset_char, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  int64_t well_formatted_start = start;
  int64_t well_formatted_len = len;
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(len));
  } else if (0 > len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("len should be greater than zero", K(len), K(ret));
  } else if (0 == len) {
    // do nothing
  } else if (OB_FAIL(ignore_invalid_byte(ptr, text_len, well_formatted_start, well_formatted_len, cs_type))) {
    LOG_WARN("ignore invalid byte failed", K(ret));
  } else {
    for (int64_t i = start; i < well_formatted_start; i++) {
      ptr[i] = reset_char;
    }
    for (int64_t i = well_formatted_start + well_formatted_len; i < start + len; i++) {
      ptr[i] = reset_char;
    }
  }
  return ret;
}

int ObExprSubstrb::get_well_formatted_boundary(
    ObCollationType cs_type, char* ptr, const int64_t len, int64_t pos, int64_t& boundary_pos, int64_t& boundary_len)
{
  const int64_t MAX_CHAR_LEN = 8;
  int ret = OB_SUCCESS;
  if (pos < 0 || pos > len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid position", K(ret), K(pos), K(len));
  } else if (NULL == ptr || len == 0) {
    boundary_pos = 0;
    boundary_len = 0;
  } else {
    int32_t error = 0;
    OZ(ObCharset::well_formed_len(cs_type, ptr, pos, boundary_pos, error));
    if (OB_SUCC(ret)) {
      boundary_len = 0;
      for (int64_t i = 1; OB_SUCC(ret) && i <= min(MAX_CHAR_LEN, len - boundary_pos); i++) {
        OZ(ObCharset::well_formed_len(cs_type, ptr + boundary_pos, i, boundary_len, error));
        if (OB_SUCC(ret)) {
          if (i == boundary_len) {
            break;
          }
        }
        boundary_len = 0;
      }
      if (boundary_pos + boundary_len < pos) {
        // invalid character found
        boundary_len = -1;
      }
    }
  }
  return ret;
}

int ObExprSubstrb::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_array is null", K(ret));
  } else {
    if (2 == param_num) {
      ret = calc_result2(result, objs_array[0], objs_array[1], expr_ctx);
    } else if (3 == param_num) {
      ret = calc_result3(result, objs_array[0], objs_array[1], objs_array[2], expr_ctx);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument number for substrb()", K(param_num), K(ret));
    }
  }
  return ret;
}

int ObExprSubstrb::calc_substrb_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* src = NULL;
  ObDatum* start = NULL;
  ObDatum* len = NULL;
  if (OB_UNLIKELY(2 != expr.arg_cnt_ && 3 != expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("arg_cnt must be 2 or 3", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, src, start, len))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (src->is_null() || start->is_null() || (3 == expr.arg_cnt_ && len->is_null())) {
    res.set_null();
  } else {
    const ObString& src_str = src->get_string();
    ObString res_str;
    int64_t start_int = 0;
    int64_t len_int = src_str.length();
    const ObCollationType& cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObExprStrResAlloc res_alloc(expr, ctx);
    if (OB_FAIL(ObExprUtil::trunc_num2int64(*start, start_int)) ||
        (3 == expr.arg_cnt_ && OB_FAIL(ObExprUtil::trunc_num2int64(*len, len_int)))) {
      LOG_WARN("trunc_num2int64 failed", K(ret));
    } else if (OB_FAIL(calc(res_str, src_str, start_int, len_int, cs_type, res_alloc))) {
      LOG_WARN("calc substrb failed", K(ret), K(src_str), K(start_int), K(len_int), K(cs_type));
    } else if (res_str.empty() && !expr.args_[0]->datum_meta_.is_clob()) {
      res.set_null();
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObExprSubstrb::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_substrb_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
