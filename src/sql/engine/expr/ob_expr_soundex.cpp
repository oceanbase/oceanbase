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

#include "sql/engine/expr/ob_expr_soundex.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
ObExprSoundex::ObExprSoundex(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_SOUNDEX, N_SOUNDEX, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

int ObExprSoundex::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const int64_t oracle_result_length = 4;
  const ObSQLSessionInfo *session = NULL;
  ObObjType param_calc_type = type1.get_type();
  ObCollationType param_calc_cs_type = type1.get_collation_type();

  ObObjType res_type = ObMaxType;
  ObCollationType res_cs_type = CS_TYPE_INVALID;
  int64_t res_length = OB_INVALID_SIZE;
  const ObLengthSemantics res_len_semantics = LS_CHAR;
  ObRawExpr *raw_expr = NULL;
  if (OB_ISNULL(session = type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (is_oracle_mode()) {
    ObSessionNLSParams nls_param = session->get_session_nls_params();
    if (type1.is_string_type()) {
      res_type = type1.is_nstring() ? ObNVarchar2Type : ObVarcharType;
      res_cs_type = type1.is_nstring() ? nls_param.nls_nation_collation_ : nls_param.nls_collation_;
      if (ObCharset::is_cs_nonascii(type1.get_collation_type())) {
        param_calc_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      }
    } else {
      res_type = ObVarcharType;
      res_cs_type = nls_param.nls_collation_;
      param_calc_type = ObVarcharType;
      param_calc_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    }
    res_length = oracle_result_length;
  } else if (OB_ISNULL(raw_expr = get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw expr is null", K(ret));
  } else {
    if (type1.is_string_type() || type1.is_enum_or_set()) {
      if (ObCharset::is_cs_nonascii(type1.get_collation_type())) {
        param_calc_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      }
      if (type1.is_enum_or_set()) {
        res_type = ObVarcharType;
        res_length = MAX(MIN_RESULT_LENGTH, type1.get_length());
        param_calc_type = ObVarcharType;
      } else if (type1.is_character_type()) {
        res_type = ObVarcharType;
        // min length of result is 4.
        res_length = MAX(MIN_RESULT_LENGTH, type1.get_length());
      } else if (ObTinyTextType == type1.get_type()) {
        res_type = ObVarcharType;
        res_length = OB_MAX_BINARY_LENGTH;
      } else {
        res_type = ObLongTextType;
        res_length = OB_MAX_LONGTEXT_LENGTH;
      }
      if (0 == raw_expr->get_extra()) {
        // calc result type for first time, record collation_type of original param.
        res_cs_type = type1.get_collation_type();
        raw_expr->set_extra(static_cast<uint64_t>(res_cs_type));
      } else if (OB_UNLIKELY(raw_expr->get_extra() >= static_cast<uint64_t>(CS_TYPE_MAX))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collationt type", K(ret), K(raw_expr->get_extra()));
      } else {
        // If collation_type of param is nonascii, implicit cast will be added above it to cast to utf8.
        // To avoid set res_cs_type to utf8, which is different from the last one and makes calc_result_type unstable,
        // we store the res_cs_type in raw_expr->extra_.
        res_cs_type = static_cast<ObCollationType>(raw_expr->get_extra());
      }
    } else {
      param_calc_type = ObVarcharType;
      param_calc_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      res_type = ObVarcharType;
      res_length = OB_MAX_BINARY_LENGTH;
      res_cs_type = type_ctx.get_coll_type();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(ObMaxType == param_calc_type || CS_TYPE_INVALID == param_calc_cs_type
                  || ObMaxType == res_type || CS_TYPE_INVALID == res_cs_type || res_length <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("some value not set", K(ret), K(type1), K(param_calc_type), K(param_calc_cs_type),
               K(res_type), K(res_cs_type), K(res_length));
    } else {
      type1.set_calc_type(param_calc_type);
      type1.set_calc_collation_type(param_calc_cs_type);
      type.set_type(res_type);
      type.set_collation_type(res_cs_type);
      type.set_collation_level(type1.get_collation_level());
      type.set_length(res_length);
      type.set_length_semantics(res_len_semantics);
    }
  }
  return ret;
}

/*
* convert character to soundex code. case insensitive.
* B, F, P, V              => 1
* C, G, J, K, Q, S, X, Z  => 2
* D, T                    => 3
* L                       => 4
* M, N                    => 5
* R                       => 6
*/
// 0 for A, E, I, O, U, H, W, Y and -1 for other character which will all be discarded.
int8_t ObExprSoundex::get_character_code(const int32_t wchar)
{
  static const int64_t alphabet_num = 26;
  const int32_t lower_alphabet_min = static_cast<int32_t>('a');
  const int32_t lower_alphabet_max = static_cast<int32_t>('z');
  const int32_t upper_alphabet_min = static_cast<int32_t>('A');
  const int32_t upper_alphabet_max = static_cast<int32_t>('Z');
  static const int8_t convert_map[alphabet_num] = {
    0, 1, 2, 3, 0, 1, 2,  /* a ~ g */
    0, 0, 2, 2, 4, 5, 5,  /* h ~ n */
    0, 1, 2, 6, 2, 3,     /* o ~ t */
    0, 1, 0, 2, 0, 2      /* u ~ z */
  };
  int8_t res = -1;
  if (wchar >= lower_alphabet_min && wchar <= lower_alphabet_max) {
    res = convert_map[wchar - lower_alphabet_min];
  } else if (wchar >= upper_alphabet_min && wchar <= upper_alphabet_max) {
    res = convert_map[wchar - upper_alphabet_min];
  }
  return res;
}

int ObExprSoundex::convert_str_to_soundex(const ObString &input,
                                          const ObCollationType input_cs_type,
                                          const bool use_original_algo,
                                          const bool fix_min_len,
                                          char *buf, const int64_t len, int64_t &pos,
                                          bool &is_first, int8_t &last_soundex_code)
{
  int ret = OB_SUCCESS;
  ObStringScanner scanner(input, input_cs_type);
  ObString tmp_str;
  int32_t wchar = 0;
  int8_t soundex_code = 0;
  int8_t pre_code = last_soundex_code;
  if (OB_UNLIKELY(len < MIN_RESULT_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not enough", K(ret), K(len));
  }
  while(OB_SUCC(ret) && !(fix_min_len && pos >= MIN_RESULT_LENGTH)) {
    if (OB_FAIL(scanner.next_character(tmp_str, wchar))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("get next character failed", K(ret), K(input));
      }
    } else if (FALSE_IT(soundex_code = get_character_code(wchar))) {
    } else if (0 == pos && is_first) {
      if (soundex_code >= 0) {
        // only expect alphabetic character as beginning of result.
        if (wchar <= static_cast<int32_t>('z') && wchar >= static_cast<int32_t>('a')) {
          buf[pos++] = wchar - 'a' + 'A';
        } else if (wchar <= static_cast<int32_t>('Z') && wchar >= static_cast<int32_t>('A')) {
          buf[pos++] = wchar;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected character", K(ret), K(wchar), K(soundex_code));
        }
        pre_code = soundex_code;
        is_first = false;
      } else {
        // ignore nonalphabetic character
      }
    } else {
      if (soundex_code > 0) {
        // middle alphabetic character, ignore if same as pre_code.
        if (pre_code != soundex_code) {
          if (OB_UNLIKELY(pos >= len)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("buf is not enough", K(ret), K(len), K(pos), K(input));
          } else {
            buf[pos++] = soundex_code + '0';
            pre_code = soundex_code;
          }
        }
      } else {
        // middle nonalphabetic character
        // just ignore if use original algorithm, otherwise reset pre_code.
        pre_code = use_original_algo ? pre_code : 0;
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    // pad zero if result shorter than MIN_RESULT_LENGTH and not empry
    if (pos > 0) {
      while(pos < MIN_RESULT_LENGTH) {
        buf[pos++] = '0';
      }
      last_soundex_code = pre_code;
    }
  }
  return ret;
}

int ObExprSoundex::calc(const ObString &input, const ObCollationType intput_cs_type,
                        const ObCollationType res_cs_type,
                        ObIAllocator &tmp_alloc, ObIAllocator &res_alloc,
                        ObString &out)
{
  int ret = OB_SUCCESS;
  const bool need_charset_convert = ObCharset::is_cs_nonascii(res_cs_type);
  const bool is_oracle_mode = lib::is_oracle_mode();
  const int64_t buf_len = is_oracle_mode ? MIN_RESULT_LENGTH : MAX(MIN_RESULT_LENGTH, input.length());
  char *buf = NULL;
  int64_t pos = 0;
  if (need_charset_convert) {
    buf = static_cast<char *>(tmp_alloc.alloc(buf_len));
  } else {
    buf = static_cast<char *>(res_alloc.alloc(buf_len));
  }
  bool is_first = true;
  int8_t last_soundex_code = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(buf_len));
  } else if (OB_FAIL(convert_str_to_soundex(input, intput_cs_type, !is_oracle_mode,
                                            is_oracle_mode, buf, buf_len, pos,
                                            is_first, last_soundex_code))) {
    LOG_WARN("calc soundex failed", K(ret));
  } else if (need_charset_convert) {
    if (OB_FAIL(ObExprUtil::convert_string_collation(ObString(pos, buf),
                                                    CS_TYPE_UTF8MB4_GENERAL_CI,
                                                    out,
                                                    res_cs_type,
                                                    res_alloc))) {
      LOG_WARN("convert string collation failed", K(ret));
    }
  } else {
    out.assign_ptr(buf, pos);
  }
  return ret;
}

int ObExprSoundex::calc_text(const ObDatum &input_datum,
                             const ObObjType input_type,
                             const ObObjType res_type,
                             const ObCollationType input_cs_type,
                             const ObCollationType res_cs_type,
                             const bool input_has_lob_header,
                             ObIAllocator &tmp_alloc, ObIAllocator &res_alloc,
                             ObString &out,
                             bool has_lob_header)
{
  int ret = OB_SUCCESS;
  const bool need_charset_convert = ObCharset::is_cs_nonascii(res_cs_type);
  const bool is_oracle_mode = lib::is_oracle_mode();
  char *buf = NULL;
  int64_t pos = 0;

  ObTextStringIter input_iter(input_type, input_cs_type, input_datum.get_string(), input_has_lob_header);
  ObTextStringResult out_result(res_type, has_lob_header, &res_alloc);
  int64_t buf_size = 0;
  int64_t data_len = 0;
  if (OB_FAIL(input_iter.init(0, NULL, &tmp_alloc))) {
    LOG_WARN("init input_iter failed ", K(ret), K(input_iter));
  } else if (OB_FAIL(input_iter.get_byte_len(data_len))) {
    LOG_WARN("get input iter data len failed ", K(ret), K(input_iter));
  } else if (FALSE_IT(buf_size = is_oracle_mode ? MIN_RESULT_LENGTH : MAX(MIN_RESULT_LENGTH, data_len))) {
  } else if (OB_FAIL(out_result.init(buf_size))) {
    LOG_WARN("init lob result failed", K(ret), K(out_result), K(buf_size));
  } else if (OB_FAIL(out_result.get_reserved_buffer(buf, buf_size))) {
    LOG_WARN("get empty buffer failed", K(ret), K(buf_size));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(buf_size));
  } else {
    bool is_first = true;
    int8_t last_soundex_code = 0;
    ObTextStringIterState state;
    ObString input_data;
    ObString block_out;
    char *block_buf = NULL;
    while (OB_SUCC(ret)
            && buf_size > 0
            && (state = input_iter.get_next_block(input_data)) == TEXTSTRING_ITER_NEXT) {
      ObDataBuffer buf_alloc(buf, buf_size);
      int64_t block_buf_len = is_oracle_mode
                              ? MIN_RESULT_LENGTH : MAX(MIN_RESULT_LENGTH, input_data.length());
      if (need_charset_convert) {
        block_buf = static_cast<char *>(tmp_alloc.alloc(block_buf_len));
      } else {
        block_buf = static_cast<char *>(buf_alloc.alloc(block_buf_len));
      }
      if (OB_FAIL(convert_str_to_soundex(input_data, input_cs_type, !is_oracle_mode,
                                        is_oracle_mode, block_buf, block_buf_len, pos,
                                        is_first, last_soundex_code))) {
        LOG_WARN("calc soundex failed", K(ret));
      } else if (need_charset_convert) {
        if (OB_FAIL(ObExprUtil::convert_string_collation(ObString(pos, block_buf),
                                                        CS_TYPE_UTF8MB4_GENERAL_CI,
                                                        block_out,
                                                        res_cs_type,
                                                        buf_alloc))) {
          LOG_WARN("convert string collation failed", K(ret));
        } else if (OB_FAIL(out_result.lseek(block_out.length(), 0))) {
          LOG_WARN("result lseek failed", K(ret));
        }
      } else { // nocharset convert
        if (OB_FAIL(out_result.lseek(pos, 0))) {
          LOG_WARN("result lseek failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (need_charset_convert) {
          buf = buf + block_out.length();
          buf_size = buf_size - block_out.length();
        } else {
          buf = buf + pos;
          buf_size = buf_size - pos;
        }
      }
    }
    out_result.get_result_buffer(out);
  }
  return ret;
}

int ObExprSoundex::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  CK(NULL != rt_expr.args_ && NULL != rt_expr.args_[0]);
  CK(ob_is_string_type(rt_expr.datum_meta_.type_));
  rt_expr.eval_func_ = eval_soundex;
  return ret;
}

int ObExprSoundex::eval_soundex(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_guard(ctx);
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (param->is_null()) {
    expr_datum.set_null();
  } else {
    const ObCollationType res_cs_type = expr.datum_meta_.cs_type_;
    const ObCollationType input_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObString out;
    const ObObjType input_type = expr.args_[0]->datum_meta_.type_;
    const ObObjType res_type = expr.datum_meta_.type_;
    if (!ob_is_text_tc(input_type) && !ob_is_text_tc(res_type)) {
      ret = calc(param->get_string(), input_cs_type, res_cs_type,
                 tmp_alloc_guard.get_allocator(),
                 expr_res_alloc, out);
    } else { // text tc
      const bool input_has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
      bool has_lob_header = expr.obj_meta_.has_lob_header();
      ret = calc_text(*param, input_type, res_type, input_cs_type, res_cs_type,
                      input_has_lob_header,
                      tmp_alloc_guard.get_allocator(),
                      expr_res_alloc, out, has_lob_header);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("calc soundex failed", K(ret));
    } else if (out.empty() && is_oracle_mode()) {
      expr_datum.set_null();
    } else {
      expr_datum.set_string(out);
    }
  }

  return ret;
}

} // sql
} // oceanbase
