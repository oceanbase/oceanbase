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
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprSubstrb::ObExprSubstrb(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SUBSTRB, N_SUBSTRB, TWO_OR_THREE, VALID_FOR_GENERATED_COL)
{
}

ObExprSubstrb::~ObExprSubstrb()
{
}

int ObExprSubstrb::calc_result_length_in_byte(const ObExprResType &type,
                                              ObExprResType *types_array,
                                              int64_t param_num,
                                              int64_t &res_len) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
  expr_ctx.calc_buf_ = &allocator;

  ObObj pos_obj;
  int64_t pos_val = -1;
  int64_t len_val = -1;
  int64_t mbminlen = -1;
  int64_t str_len_in_byte = -1;
  ObCollationType cs_type = type.get_collation_type();
  bool is_from_pl = !is_called_in_sql();

  if (OB_ISNULL(types_array)) {
    ret = OB_NOT_INIT;
    LOG_WARN("types_array is NULL", K(ret));
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == cs_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cs_type is not valid", K(cs_type), K(ret));
  } else if (OB_UNLIKELY(2 != param_num && 3 != param_num)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("param_num invalid", K(param_num), K(ret));
  } else if (OB_FAIL(ObCharset::get_mbminlen_by_coll(cs_type, mbminlen))) {
    LOG_WARN("get mbminlen failed", K(cs_type), K(ret));
  } else {
    str_len_in_byte = types_array[0].get_calc_length();

    pos_obj = types_array[1].get_param();

    // create table substrb_test_tbl(c1 varchar2(64 char), c2 varchar2(7 byte)
    // GENERATED ALWAYS AS (substrb(c1, -1, 8)) VIRTUAL);
    // 使用如上建表语句时，substrb的参数是负数时，pos_obj是null
    //
    if (pos_obj.is_null_oracle()) {
      pos_val = 1;
    } else {
      if (OB_FAIL(ObExprUtil::get_trunc_int64(pos_obj, expr_ctx, pos_val))) {
        if(lib::is_oracle_mode()) {
          if (0 == pos_val) {
            pos_val = 1;
          }
          ret = OB_SUCCESS;
          LOG_WARN("ignore failure when calc pos_val oracle mode", K(ret));
        }
        LOG_WARN("get int64 failed", K(pos_obj), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      len_val = str_len_in_byte;
      if (param_num == 3 && !types_array[2].get_param().is_null()) {
        const ObObj &len_obj = types_array[2].get_param();
        if (OB_FAIL(ObExprUtil::get_trunc_int64(len_obj, expr_ctx, len_val))) {
          if(lib::is_oracle_mode()) {
            if (0 == len_val) {
              len_val = str_len_in_byte;
            }
            ret = OB_SUCCESS;
            LOG_WARN("ignore failure when calc len_val oracle mode", K(ret));
          }
          LOG_WARN("get int64 failed", K(len_obj), K(ret));
        }
        if (OB_SUCC(ret) && (type.is_nchar() || type.is_nvarchar2())) {
          len_val /= mbminlen;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 > pos_val) {
        pos_val = pos_val + str_len_in_byte;
      } else {
        // 用户从1开始计数
        pos_val = pos_val - 1;
      }

      if ((pos_val > str_len_in_byte - 1) || (0 >= len_val)) {
        res_len = 0;
      } else if (pos_val + len_val >= str_len_in_byte ){
        res_len = str_len_in_byte - pos_val;
      } else {
        res_len = len_val;
      }
      if (type.is_varchar() || type.is_nvarchar2()) {
        res_len = res_len > OB_MAX_ORACLE_VARCHAR_LENGTH ?
                    OB_MAX_ORACLE_VARCHAR_LENGTH : res_len;
      } else if (type.is_char() || type.is_nchar()) {
        int64_t max_len = (is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                      : OB_MAX_ORACLE_CHAR_LENGTH_BYTE);
        res_len = res_len > max_len ? max_len : res_len;
      }
    }
  }
  return ret;
}

int ObExprSubstrb::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_array,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
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
      types_array[0].set_calc_accuracy(types_array[0].get_accuracy());
      type.set_raw();
    } else {
      ObSEArray<ObExprResType*, 1, ObNullAllocator> types;
      ObLengthSemantics real_length_semantics = LS_INVALIED;
      OZ(types.push_back(&types_array[0]));
      OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), types, type,
                                                  PREFER_VAR_LEN_CHAR));
      OX(real_length_semantics = type.get_length_semantics());
      if (type.is_varchar_or_char()) {
        OX(type.set_length_semantics(LS_BYTE)); // use byte semantics to deduce param length
      }
      OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, types));
      OX(type.set_length_semantics(real_length_semantics));
    }
    if (OB_SUCC(ret)) {
      int64_t result_len = types_array[0].get_calc_length();
      OZ(calc_result_length_in_byte(type, types_array, param_num, result_len));
      OX(type.set_length(result_len));
    }
  }
  return ret;
}

int ObExprSubstrb::calc(ObString &res_str, const ObString &text,
                        int64_t start, int64_t length, ObCollationType cs_type,
                        ObIAllocator &alloc)
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
      start = 1;//
    }
    start = (start > 0) ? (start - 1) : (start + text_len);
    if (OB_UNLIKELY(start < 0 || start >= text_len)) {
      res_str.reset();
    } else {
      char* buf = static_cast<char *>(alloc.alloc(text_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(text_len), K(ret));
      } else {
        MEMCPY(buf, text.ptr(), text_len);
        res_len = min(length, text_len - start);
        // 标准Oracle会将非法的byte设置为空格
        if (OB_FAIL(handle_invalid_byte(buf, text_len, start, res_len, ' ', cs_type, false))) {
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

int ObExprSubstrb::handle_invalid_byte(char* ptr,
                                       const int64_t text_len,
                                       int64_t &start,
                                       int64_t &len,
                                       char reset_char,
                                       ObCollationType cs_type,
                                       const bool force_ignore_invalid_byte)
{
  int ret = OB_SUCCESS;
  int64_t mbminlen = 0;
  if (OB_FAIL(ObCharset::get_mbminlen_by_coll(cs_type, mbminlen))) {
    LOG_WARN("get mbminlen failed", K(cs_type), K(ret));
  } else {
    if (force_ignore_invalid_byte || mbminlen > 1) { // utf16: mbminlen is 2
      if (OB_FAIL(ignore_invalid_byte(ptr, text_len, start, len, cs_type))) {
        LOG_WARN("ignore_invalid_byte failed", K(ret));
      }
    } else { // utf8/gbk/gb18030: mbminlen is 1
      if (OB_FAIL(reset_invalid_byte(ptr, text_len, start, len, reset_char, cs_type))) {
        LOG_WARN("reset_invalid_byte failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_UNLIKELY(len % mbminlen != 0)) {
      // 防御性代码，防止hang
      // eg: a -> '0061'
      //     substrb(utf16_a, 2, 1), well_formed_len()方法认为'61'是一个合法的utf16字符
      //     是不符合预期的，这是我们底层字符集函数的缺陷
      //     由于不确定well_formed_len()是否有其他坑，这里进行防御，防止hang
      int64_t hex_len = 0;
      char hex_buf[1024] = {0}; // just print 512 bytes
      OZ(common::hex_print(ptr + start, len, hex_buf, sizeof(hex_buf), hex_len));
      if (OB_SUCC(ret)) {
        const char *charset_name = ObCharset::charset_name(cs_type);
        int64_t charset_name_len = strlen(charset_name);
        ret = OB_ERR_INVALID_CHARACTER_STRING;
        LOG_USER_ERROR(OB_ERR_INVALID_CHARACTER_STRING, static_cast<int>(charset_name_len),
                      charset_name, static_cast<int>(hex_len), hex_buf);
      }
    }
  }
  return ret;
}

int ObExprSubstrb::ignore_invalid_byte(char* ptr,
                                       const int64_t text_len,
                                       int64_t &start,
                                       int64_t &len,
                                       ObCollationType cs_type)
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
    OZ(get_well_formatted_boundary(
            cs_type, ptr, text_len, start, boundary_pos, boundary_len));
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
          OZ(get_well_formatted_boundary(
                  cs_type, ptr + start, text_len - start, len, boundary_pos, boundary_len));
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

int ObExprSubstrb::reset_invalid_byte(char* ptr,
                                      const int64_t text_len,
                                      int64_t start,
                                      int64_t len,
                                      char reset_char,
                                      ObCollationType cs_type)
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
  } else if (OB_FAIL(ignore_invalid_byte(
              ptr, text_len, well_formatted_start, well_formatted_len, cs_type))) {
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

int ObExprSubstrb::get_well_formatted_boundary(ObCollationType cs_type,
                                               char *ptr,
                                               const int64_t len,
                                               int64_t pos,
                                               int64_t &boundary_pos,
                                               int64_t &boundary_len)
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

int ObExprSubstrb::calc_substrb_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *src = NULL;
  ObDatum *start = NULL;
  ObDatum *len = NULL;
  if (OB_UNLIKELY(2 != expr.arg_cnt_ && 3 != expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("arg_cnt must be 2 or 3", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, src, start, len))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (src->is_null() || start->is_null() || (3 == expr.arg_cnt_ && len->is_null())) {
    res.set_null();
  } else {
    const ObString &src_str = src->get_string();
    ObString res_str;
    int64_t start_int = 0;
    int64_t len_int = src_str.length();
    const ObCollationType &cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObExprStrResAlloc res_alloc(expr, ctx);
    if (expr.is_called_in_sql_ 
         && (OB_FAIL(ObExprUtil::trunc_num2int64(*start, start_int)) ||
        (3 == expr.arg_cnt_ && OB_FAIL(ObExprUtil::trunc_num2int64(*len, len_int))))) {
      LOG_WARN("trunc_num2int64 failed", K(ret));
    } else if (!expr.is_called_in_sql_  
                && (OB_FAIL(ObExprUtil::round_num2int64(*start, start_int))
                || (3 == expr.arg_cnt_ && OB_FAIL(ObExprUtil::round_num2int64(*len, len_int))))) {
      LOG_WARN("round_num2int64 failed", K(ret));
    } else if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      if (OB_FAIL(calc(res_str, src_str, start_int, len_int, cs_type, res_alloc))) {
        LOG_WARN("calc substrb failed", K(ret), K(src_str), K(start_int), K(len_int), K(cs_type));
      } else if (res_str.empty() && !expr.args_[0]->datum_meta_.is_clob()) {
        res.set_null();
      } else {
        res.set_string(res_str);
      }
    } else { // text tc
      if (0 == start_int) {
        //
        start_int = 1;
      }
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      int64_t total_byte_len = 0;

      // read as binary
      const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
      ObTextStringIter input_iter(expr.args_[0]->datum_meta_.type_, CS_TYPE_BINARY, src->get_string(), has_lob_header);
      if (OB_FAIL(input_iter.init(0, NULL, &calc_alloc))) {
        LOG_WARN("Lob: init input_iter failed ", K(ret), K(input_iter));
      } else if (OB_FAIL(input_iter.get_byte_len(total_byte_len))) {
        LOG_WARN("Lob: get input byte len failed", K(ret));
      } else {
        len_int = len == NULL ? total_byte_len : len_int;
      }
      int64_t result_byte_len = MIN((start_int >= 0 ? total_byte_len - start_int + 1 : -start_int), len_int);
      if (!input_iter.is_outrow_lob()) {
        // ObExprSubstrb::calc alloc memory from result buffer at least as input text len.
        result_byte_len = MAX(result_byte_len, total_byte_len);
      }
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res);
      char *buf = NULL;
      int64_t buf_size = 0;
      if (OB_FAIL(ret)) {
      } else if (len_int < 0 || start_int > total_byte_len) {
        if (OB_FAIL(output_result.init(0))) { // fill empty lob result
          LOG_WARN("Lob: init stringtext result failed", K(ret));
        } else {
          output_result.set_result();
        }
      } else if (OB_FAIL(output_result.init(result_byte_len))) {
        LOG_WARN("Lob: init stringtext result failed", K(ret));
      } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
        LOG_WARN("Lob: stringtext result reserve buffer failed", K(ret));
      } else {
        input_iter.set_start_offset((start_int >= 0 ? (start_int - 1) : total_byte_len + start_int));
        input_iter.set_access_len(len_int);
        ObTextStringIterState state;
        ObString src_block_data;
        while (OB_SUCC(ret)
               && buf_size > 0
               && (state = input_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
          ObDataBuffer data_buf(buf, buf_size);
          if (!input_iter.is_outrow_lob()) {
            ObString inrow_result;
            if (OB_FAIL(calc(inrow_result, src_block_data, start_int, len_int, cs_type, data_buf))) {
              LOG_WARN("get substr failed", K(ret));
            } else if (FALSE_IT(MEMMOVE(buf, inrow_result.ptr(), inrow_result.length()))) {
            } else if (OB_FAIL(output_result.lseek(inrow_result.length(), 0))) {
              LOG_WARN("Lob: append result failed", K(ret), K(output_result), K(src_block_data));
            }
          // outrow lobs, only use calc for handle invalid bytes
          } else {
            if (OB_FAIL(calc(res_str, src_block_data, 0, src_block_data.length(), cs_type, data_buf))) {
              LOG_WARN("calc substrb failed", K(ret), K(src_str), K(start_int), K(len_int), K(cs_type));
            } else if (OB_FAIL(output_result.lseek(res_str.length(), 0))) {
              LOG_WARN("result lseek failed", K(ret));
            } else {
              buf += res_str.length();
              buf_size -= res_str.length();
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
          ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
                input_iter.get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
        } else {
          output_result.set_result();
        }
      }
    }
  }
  return ret;
}

int ObExprSubstrb::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  LOG_WARN("is called sql", K(is_called_in_sql_));
  rt_expr.eval_func_ = calc_substrb_expr;
  return ret;
}

} /* sql */
} /* oceanbase */
