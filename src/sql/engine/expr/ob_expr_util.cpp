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
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "common/ob_smart_call.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/charset/ob_charset.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObExprUtil::get_int64_from_obj(const ObObj &obj,
                                   ObExprCtx &expr_ctx,
                                   const bool is_trunc, //true: trunc; false: round
                                   int64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  ObObjType type = obj.get_type();
  if (OB_LIKELY(ob_is_int_tc(type))) {
    out = obj.get_int();
  } else if (ob_is_uint_tc(type)) {
    uint64_t value = obj.get_uint64();
    out = (value > INT64_MAX) ? INT64_MAX : static_cast<int64_t>(value);
  } else if (OB_UNLIKELY(obj.is_number())) {
    number::ObNumber nmb = obj.get_number();
    ret = get_int64_from_num(nmb, expr_ctx, is_trunc, out);
  } else if (ob_is_decimal_int(obj.get_type())) {
    bool is_valid_int64 = false;
    ret = wide::check_range_valid_int64(obj.get_decimal_int(), obj.get_int_bytes(), is_valid_int64, out);
    if (OB_SUCC(ret) && !is_valid_int64) {
      out = INT64_MAX;
    }
  } else {
    // 除了 number 之外的类型，强制转换成 number
    // 并 trunc 成一个整数
    number::ObNumber nmb;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_NUMBER_V2(obj, nmb);
    if (OB_SUCC(ret)) {
      ret = get_int64_from_num(nmb, expr_ctx, is_trunc, out);
    }
  }
  return ret;
}

int ObExprUtil::get_trunc_int64(const ObObj &obj, ObExprCtx &expr_ctx, int64_t &out)
{
  return get_int64_from_obj(obj, expr_ctx, true, out);
}

int ObExprUtil::get_round_int64(const ObObj &obj, ObExprCtx &expr_ctx, int64_t &out)
{
  return get_int64_from_obj(obj, expr_ctx, false, out);
}

int ObExprUtil::get_trunc_int64(number::ObNumber &nmb, ObExprCtx &expr_ctx, int64_t &out)
{
  return get_int64_from_num(nmb, expr_ctx, true, out);
}

int ObExprUtil::get_round_int64(number::ObNumber &nmb, ObExprCtx &expr_ctx, int64_t &out)
{
  return get_int64_from_num(nmb, expr_ctx, false, out);
}

int ObExprUtil::get_int64_from_num(number::ObNumber &nmb,
                                   ObExprCtx &expr_ctx,
                                   const bool is_trunc, //true: trunc; false: round
                                   int64_t &out)
{
  int ret = OB_SUCCESS;
  int64_t tmp_int = 0;
  uint64_t tmp_uint = 0;
  number::ObNumber *pnmb = &nmb;
  number::ObNumber newmb;
  if (OB_UNLIKELY(!nmb.is_integer())) {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    //copy is essential since if we did not do that, obj will be modified
    if (OB_FAIL(newmb.from(nmb, cast_ctx))) {
      LOG_WARN("copy nmb failed", K(ret), K(nmb));
    } else if (is_trunc && OB_FAIL(newmb.trunc(0))) {
      LOG_WARN("trunc failed", K(ret), K(nmb));
    } else if (!is_trunc && OB_FAIL(newmb.round(0))) {
      LOG_WARN("round failed", K(ret), K(nmb));
    } else {
      pnmb = &newmb;
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_ISNULL(pnmb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. null pointer", K(ret));
  } else if (pnmb->is_valid_int64(tmp_int)) {
    out = tmp_int;
  } else if (pnmb->is_valid_uint64(tmp_uint)) {
    out = (tmp_uint > INT64_MAX) ? INT64_MAX : static_cast<int64_t>(tmp_uint);
  } else {
    //即使number取值超过INT64值域也不报错,
    //select substr('abcd',-18446744073709551615) from dual; 期望返回NULL
    out = INT64_MAX;
  }
    // ret = OB_ERR_TRUNCATED_WRONG_VALUE;
    // if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
    //   ret = OB_SUCCESS;
    //   out = 0;
    // }
  return ret;
}

int ObExprUtil::trunc_num2int64(const common::number::ObNumber &nmb, int64_t &v)
{
  int ret = OB_SUCCESS;
  ObNumStackOnceAlloc tmp_alloc;
  number::ObNumber trunc_nmb;
  const number::ObNumber *nmb_val = &nmb;

  if (OB_UNLIKELY(!nmb.is_integer())) {
    if (OB_FAIL(trunc_nmb.from(nmb, tmp_alloc))) {
      LOG_WARN("copy number failed", K(ret));
    } else if (OB_FAIL(trunc_nmb.trunc(0))) {
      LOG_WARN("truncate number failed");
    } else {
      nmb_val = &trunc_nmb;
    }
  }

  if (OB_SUCC(ret)) {
    if (nmb_val->is_valid_int64(v)) {
      // %v is set, do nothing
    } else {
      // num_val out of int64 range, set to INT64_MIN or INT64_MAX
      v = nmb_val->is_negative() ? INT64_MIN : INT64_MAX;
    }
  }
  return ret;
}

int ObExprUtil::trunc_decint2int64(const ObDecimalInt *decint, const int32_t int_bytes, const ObScale scale, int64_t &v)
{
#define TRUNC_DECINT_CASE(len, int_type)                                                           \
  case len: {                                                                                      \
    ret = wide::scale_down_decimalint_for_trunc(*reinterpret_cast<const int_type *>(decint),       \
                                                scale, tmp_res);                                   \
  } break

  int ret = OB_SUCCESS;
  ObDecimalIntBuilder tmp_res;
  bool is_valid = false;
  switch (int_bytes) {
  TRUNC_DECINT_CASE(4, int32_t);
  TRUNC_DECINT_CASE(8, int64_t);
  TRUNC_DECINT_CASE(16, int128_t);
  TRUNC_DECINT_CASE(32, int256_t);
  TRUNC_DECINT_CASE(64, int512_t);
  default: {
    ret = OB_ERR_UNEXPECTED;
  }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("scale down decimal int for truncating failed", K(ret), K(int_bytes), K(scale));
  } else if (OB_FAIL(wide::check_range_valid_int64(tmp_res.get_decimal_int(), tmp_res.get_int_bytes(), is_valid, v))) {
    LOG_WARN("check valid int64_t failed", K(ret));
  } else if (is_valid) {
    // do nothing
  } else {
    v = wide::is_negative(tmp_res.get_decimal_int(), tmp_res.get_int_bytes()) ? INT64_MIN : INT64_MAX;
  }

  return ret;
#undef TRUNC_DECINT_CASE
}

int ObExprUtil::round_num2int64(const common::number::ObNumber &nmb, int64_t &v)
{
  int ret = OB_SUCCESS;
  ObNumStackOnceAlloc tmp_alloc;
  number::ObNumber round_nmb;
  const number::ObNumber *nmb_val = &nmb;

  if (OB_UNLIKELY(!nmb.is_integer())) {
    if (OB_FAIL(round_nmb.from(nmb, tmp_alloc))) {
      LOG_WARN("copy number failed", K(ret));
    } else if (OB_FAIL(round_nmb.round(0))) {
      LOG_WARN("truncate number failed");
    } else {
      nmb_val = &round_nmb;
    }
  }

  if (OB_SUCC(ret)) {
    if (nmb_val->is_valid_int64(v)) {
      // %v is set, do nothing
    } else {
      // num_val out of int64 range, set to INT64_MIN or INT64_MAX
      v = nmb_val->is_negative() ? INT64_MIN : INT64_MAX;
    }
  }
  return ret;
}

int ObExprUtil::round_decint2int64(const ObDecimalInt *decint, const int32_t int_bytes, const ObScale scale, int64_t &v)
{
#define ROUND_DECINT_CASE(len, int_byte)                                                           \
  case len: {                                                                                      \
    ret = wide::scale_down_decimalint_for_round(*reinterpret_cast<const int_byte *>(decint),       \
                                                scale, tmp_res);                                   \
  } break

  int ret = OB_SUCCESS;
  ObDecimalIntBuilder tmp_res;
  bool is_valid_int64 = false;
  switch (int_bytes) {
  ROUND_DECINT_CASE(4, int32_t);
  ROUND_DECINT_CASE(8, int64_t);
  ROUND_DECINT_CASE(16, int128_t);
  ROUND_DECINT_CASE(32, int256_t);
  ROUND_DECINT_CASE(64, int512_t);
  default: {
    ret = OB_ERR_UNEXPECTED;
  }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("scale down decimalint for rounding failed", K(ret), K(int_bytes), K(scale));
  } else if (OB_FAIL(wide::check_range_valid_int64(tmp_res.get_decimal_int(),
                                                   tmp_res.get_int_bytes(), is_valid_int64, v))) {
    LOG_WARN("check valid int64 failed", K(ret));
  } else if (is_valid_int64) {
    // do nothing
  } else {
    v = v = wide::is_negative(tmp_res.get_decimal_int(), tmp_res.get_int_bytes()) ? INT64_MIN : INT64_MAX;
  }
  return ret;
#undef ROUND_DECINT_CASE
}

int ObExprUtil::get_int_param_val(ObDatum *datum, bool is_decint, int64_t &int_val)
{
  int ret = OB_SUCCESS;
  if (NULL != datum && !datum->is_null()) {
    if (is_decint) {
      bool is_valid_int64 = false;
      if (OB_FAIL(wide::check_range_valid_int64(datum->get_decimal_int(), datum->get_int_bytes(),
                                                is_valid_int64, int_val))) {
        LOG_WARN("check valid int64 failed", K(ret));
      } else if (OB_UNLIKELY(!is_valid_int64)) {
        if (wide::is_negative(datum->get_decimal_int(), datum->get_int_bytes())) {
          int_val = INT64_MIN;
        } else {
          int_val = INT64_MAX;
        }
      }
    } else if (lib::is_mysql_mode()) {
      int_val = datum->get_int();
    } else if (OB_FAIL(trunc_num2int64(*datum, int_val))) {
      LOG_WARN("truncate number 2 int failed", K(ret), K(*datum));
    }
  }
  return ret;
}

int ObExprUtil::kmp(const char *pattern,
                    const int64_t pattern_len,
                    const char *text,
                    const int64_t text_len,
                    const int64_t nth_appearance,
                    const int32_t *next, /* calculated, size same with pattern */
                    int64_t &result)
{
  int ret = OB_SUCCESS;
  result = -1;
  if (OB_ISNULL(pattern) || OB_ISNULL(text) ||
       OB_UNLIKELY(pattern_len <= 0) || OB_UNLIKELY(text_len <= 0) || OB_ISNULL(next)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(pattern_len), K(text_len));
  } else if (OB_LIKELY(pattern_len <= text_len)) {
    int64_t i = 0; // for pattern
    int64_t j = 0; // for text
    int64_t t = 0; // times matched
    while (j < text_len) {
      if (i == -1 || pattern[i] == text[j]) {
        ++j;
        if (++i == pattern_len) { // matched
          if (++t == nth_appearance) { // find nth apperance
            result = j - i;
            break;
          }
          // reset start pos to first character
          i = 0;
        }
      } else {
        i = next[i];
      }
    }
  }
  return ret;
}

int ObExprUtil::kmp_next(const char *pattern, const int64_t pattern_len, int32_t *next)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pattern) || OB_UNLIKELY(pattern_len <= 0) || OB_ISNULL(next)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(pattern_len));
  } else {
    // calc the kmp next array of pattern, next[i] represents the common maximum length of
    // the prefix and suffix of the substring before the (i-1)th position of the pattern.
    // for example, the next array of "ABCABB" is [-1, 0, 0, 0, 1, 2].
    // next[5] = 2, the substring is ABCAB, perfix set is {A,AB,ABC,ABCA},
    // suffix set is {B,AB,CAB,BCAB}, the maximun length of common substring is "AB",
    // which length is 2.
    next[0] = -1;
    int i = 0;
    int k = -1;
    while (i < pattern_len - 1) {
      if (k == -1 || pattern[i] == pattern[k]) {
        next[++i] = ++k;
      } else {
        k = next[k];
      }
    }
  }
  return ret;
}

int ObExprUtil::kmp_reverse(const char *pattern,
                            const int64_t pattern_len,
                            const char *text,
                            const int64_t text_len,
                            const int64_t nth_appearance,
                            const int32_t *next, /* calculated, size same with pattern */
                            int64_t &result)
{
  int ret = OB_SUCCESS;
  result = -1;
  if (OB_ISNULL(pattern) || OB_ISNULL(text) ||
       OB_UNLIKELY(pattern_len <= 0) || OB_UNLIKELY(text_len <= 0) || OB_ISNULL(next)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(pattern_len), K(text_len));
  } else if (OB_LIKELY(pattern_len <= text_len)) {
    int64_t i = pattern_len - 1; // for pattern
    int64_t j = text_len - 1; // for text
    int64_t t = 0; // times matched
    while (j >= 0) {
      if (i == pattern_len || pattern[i] == text[j]) {
        --j;
        if (--i == -1) { // matched
          if (--t == nth_appearance) { // find nth apperance
            result = j + 1;
            break;
          }
          // reset start pos to last character
          i = pattern_len - 1;
        }
      } else {
        i = next[i];
      }
    }
  }
  return ret;
}

int ObExprUtil::kmp_next_reverse(const char *pattern,
                                 const int64_t pattern_len,
                                 int32_t *next)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pattern) || OB_UNLIKELY(pattern_len <= 0) || OB_ISNULL(next)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(pattern_len));
  } else {
    // reverse next array for kmp, ABCABB's reverse next array is [4, 5, 5, 4, 5, 6].
    next[pattern_len - 1] = pattern_len;
    int i = pattern_len - 1;
    int k = pattern_len;
    while (i >= 1) {
      if (k == pattern_len || pattern[i] == pattern[k]) {
        next[--i] = --k;
      } else {
        k = next[k];
      }
    }
  }
  return ret;
}

// 获取multiple bytes字符串中每个字符的字节offset
// 以及每个mb字符占用的字节数
// byte_offsets中第一个元素是第二个字符的起始字节位置，最后一个元素是字符串的长度
// byte_num中每个元素对应输入str每个字符所占的字节数
// ori_str: abc
// ori_str_byte_offsets: [0, 1, 2, 3]
// ori_str_byte_num: [1, 1, 1]
int ObExprUtil::get_mb_str_info(const ObString &str,
                                ObCollationType cs_type,
                                ObIArray<size_t> &byte_num,
                                ObIArray<size_t> &byte_offsets)
{
  int ret = OB_SUCCESS;
  size_t byte_index = 0;
  size_t str_char_len = -1;
  int64_t well_formed_len = 0;
  if (CS_TYPE_INVALID == cs_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cs_type", K(ret), K(cs_type));
  } else if (0 == str.length()) {
    // do nothing
  } else if (OB_ISNULL(str.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str.ptr is null", K(ret));
  } else if (OB_FAIL(ObCharset::well_formed_len(cs_type,
                                                str.ptr(),
                                                str.length(),
                                                well_formed_len))) {
    LOG_WARN("invalid string for charset", K(str), K(well_formed_len));
  } else {
    if (OB_FAIL(byte_offsets.push_back(0))) {
      LOG_WARN("byte_offsets.push_back failed", K(ret), K(byte_offsets));
    } else {
      str_char_len = ObCharset::strlen_char(cs_type, str.ptr(), str.length());
      int64_t last_byte_index = 0;
      while (OB_SUCC(ret) && (byte_index < str.length())) {
        byte_index = ObCharset::charpos(cs_type, str.ptr() + last_byte_index,
                                        str.length() - last_byte_index, 1);
        byte_index = last_byte_index + byte_index;
        last_byte_index = byte_index;
        if (OB_FAIL(byte_offsets.push_back(byte_index))) {
          LOG_WARN("byte_offsets.push_back failed", K(ret), K(byte_offsets));
        }
      }

      // 获取每个字符占用的字节数
      for (size_t i = 1; OB_SUCC(ret) && (i < byte_offsets.count()); ++i) {
        if (OB_FAIL(byte_num.push_back(byte_offsets.at(i) - byte_offsets.at(i-1)))) {
          LOG_WARN("byte_num.push_back failed", K(ret), K(byte_num));
        }
      }
      LOG_DEBUG("get_byte_offset", K(ret), K(str), K(byte_offsets), K(byte_num));
    }
  }
  return ret;
}

double ObExprUtil::round_double(double val, int64_t dec)
{
  const double pow_val = std::pow(10.0, static_cast<double>(std::abs(dec)));
  double val_div_tmp = val / pow_val;
  double val_mul_tmp = val * pow_val;
  double res = 0.0;
  if (dec < 0 && std::isinf(pow_val)) {
    res = 0.0;
  } else if (dec >= 0 && !std::isfinite(val_mul_tmp)) {
    res = val;
  } else {
    res = dec < 0 ? rint(val_div_tmp) * pow_val : rint(val_mul_tmp) / pow_val;
  }
  LOG_DEBUG("round double done", K(val), K(dec), K(res));
  return res;
}

uint64_t ObExprUtil::round_uint64(uint64_t val, int64_t dec)
{
  uint64_t res = 0;
  if (dec >= 0) {
    // 整形没有小数点后位数，直接赋值
    res = val;
  } else {
    dec = -dec;
    // 20 is digit num of (UINT64_MAX)18446744073709551615
    if (dec >= 20) {
      res = 0;
    } else {
      // val: 250, dec: 2, div: 100, tmp: 200
      uint64_t div = 1;
      for (int64_t i = 0; i < dec; ++i) {
        div *= 10;
      }
      uint64_t tmp = val / div * div;
      res = (val - tmp) >= (div / 2) ? tmp + div : tmp;
    }
  }
  LOG_DEBUG("round int64 done", K(val), K(dec), K(res));
  return res;
}

double ObExprUtil::trunc_double(double val, int64_t dec)
{
  const double pow_val = std::pow(10, static_cast<double>(std::abs(dec)));
  volatile double val_div_tmp = val / pow_val;
  volatile double val_mul_tmp = val * pow_val;
  volatile double res = 0.0;
  if (dec < 0 && std::isinf(pow_val)) {
    res = 0.0;
  } else if (dec >= 0 && !std::isfinite(val_mul_tmp)) {
    res = val;
  } else {
    if (val >= 0) {
      res = dec < 0 ? floor(val_div_tmp) * pow_val : floor(val_mul_tmp) / pow_val;
    } else {
      res = dec < 0 ? ceil(val_div_tmp) * pow_val : ceil(val_mul_tmp) / pow_val;
    }
  }
  LOG_DEBUG("trunc int64 done", K(val), K(dec), K(res));
  return res;
}

int ObExprUtil::set_expr_ascii_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                                      const ObString &ascii, const int64_t datum_idx,
                                      const bool is_ascii,
                                      const common::ObCollationType src_coll_type)
{
  int ret = OB_SUCCESS;
  if (!ob_is_text_tc(expr.datum_meta_.type_)) {
    if (ascii.empty()) {
      if (lib::is_oracle_mode()) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(ObString());
      }
    } else {
      ObArenaAllocator temp_allocator;
      ObString out;
      char *buf = NULL;
      if (is_ascii && !ObCharset::is_cs_nonascii(expr.datum_meta_.cs_type_)) {
        out = ascii;
      } else if (OB_FAIL(ObCharset::charset_convert(temp_allocator, ascii, src_coll_type,
                                            expr.datum_meta_.cs_type_, out))) {
        LOG_WARN("charset convert failed", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, out.length(), datum_idx))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(out.length()));
      } else {
        MEMCPY(buf, out.ptr(), out.length());
        expr_datum.set_string(buf, out.length());
      }
    }
  } else { // text tc
    ObArenaAllocator temp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString out;
    char *buf = NULL;
    ObTextStringDatumResult res(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
    if (ascii.empty()) {
      if (lib::is_oracle_mode()) {
        expr_datum.set_null();
      } else if (OB_FAIL(res.init_with_batch_idx(0, datum_idx))) {
        LOG_WARN("init text str result failed", K(ret));
      } else {
        res.set_result();
      }
    } else {
      if (is_ascii && !ObCharset::is_cs_nonascii(expr.datum_meta_.cs_type_)) {
        out = ascii;
      } else if (OB_FAIL(ObCharset::charset_convert(temp_allocator, ascii, src_coll_type,
                                                    expr.datum_meta_.cs_type_, out))) {
        LOG_WARN("charset convert failed", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res.init_with_batch_idx(out.length(), datum_idx))) {
        LOG_WARN("init text str result failed");
      } else if (OB_FAIL(res.append(out.ptr(), out.length()))) {
        LOG_WARN("append text str result failed", K(ret));
      } else {
        res.set_result();
      }
    }
  }
  return ret;
}

// for non batch mode
int ObExprUtil::set_expr_ascii_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                                      const ObString &ascii_str, const bool is_ascii,
                                      const common::ObCollationType src_coll_type)
{
  return set_expr_ascii_result(expr, ctx, expr_datum, ascii_str, ctx.get_batch_idx(),
                               is_ascii, src_coll_type);
}

int ObExprUtil::need_convert_string_collation(const ObCollationType &in_collation,
                                              const ObCollationType &out_collation,
                                              bool &need_convert)
{
  int ret = OB_SUCCESS;
  need_convert = true;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(in_collation)
                  || !ObCharset::is_valid_collation(out_collation))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_collation), K(out_collation));
  } else if (ObCharset::charset_type_by_coll(in_collation)
             == ObCharset::charset_type_by_coll(out_collation) ||
             CS_TYPE_BINARY == in_collation ||
             CS_TYPE_BINARY == out_collation) {
    need_convert = false;
  } else {
    // do nothing
  }
  return ret;
}


int ObExprUtil::convert_string_collation(const ObString &in_str,
                                         const ObCollationType &in_collation,
                                         ObString &out_str,
                                         const ObCollationType &out_collation,
                                         ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  bool need_convert = false;
  if (in_str.empty()) {
    out_str.reset();
  } else if (OB_FAIL(need_convert_string_collation(in_collation, out_collation,
                                                   need_convert))) {
    LOG_WARN("check need_convert_string_collation failed", K(ret));
  } else if (!need_convert) {
    out_str = in_str;
  } else {
    char *buf = NULL;
    int32_t buf_len = in_str.length() * ObCharset::CharConvertFactorNum;
    uint32_t result_len = 0;
    if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(in_collation, in_str.ptr(), in_str.length(),
                                                  out_collation, buf, buf_len,
                                                  result_len))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      out_str.assign_ptr(buf, result_len);
    }
  }
  return ret;
}

int ObExprUtil::deep_copy_str(const ObString &src, ObString &out, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char *buf = reinterpret_cast<char*>(alloc.alloc(src.length()));
  OV(OB_NOT_NULL(buf), OB_ALLOCATE_MEMORY_FAILED);
  if (OB_SUCC(ret)) {
    MEMCPY(buf, src.ptr(), src.length());
    out.assign_ptr(buf, src.length());
  }
  return ret;
}

int ObExprUtil::eval_stack_overflow_check(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &)
{
  int ret = OB_SUCCESS;

  // control the occupied stack size by event to simulate stack overflow in testing.
  int stack_size = OB_E(EventTable::EN_STACK_OVERFLOW_CHECK_EXPR_STACK_SIZE) 0;
  stack_size = std::abs(stack_size);
  char *cur_stack[stack_size];
  if (stack_size > 0) {
    LOG_DEBUG("cur stack", KP(cur_stack));
  }

  if (OB_FAIL(SMART_CALL(expr.eval_param_value(ctx)))) {
    LOG_WARN("evaluate parameters value failed", K(ret), KP(cur_stack));
  } else {
    // Since stack overflow check expr has the same ObDatum with child,
    // need do nothing here.
  }
  return ret;
}

int ObExprUtil::eval_batch_stack_overflow_check(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("stack overflow check failed", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("evaluate batch failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObExprUtil::convert_utf8_charset(ObIAllocator& allocator,
                                     const ObCollationType& from_collation,
                                     const ObString &from_string,
                                     ObString &to_string)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  //一个字符最多4个byte
  int32_t buf_len = from_string.length() * ObCharset::CharConvertFactorNum;
  uint32_t result_len = 0;
  if (0 == buf_len) {
  } else if (from_collation == CS_TYPE_UTF8MB4_BIN) {
    to_string = from_string;
  } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator.alloc(buf_len))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else if (OB_FAIL(ObCharset::charset_convert(from_collation,
                                                from_string.ptr(),
                                                from_string.length(),
                                                CS_TYPE_UTF8MB4_BIN,
                                                buf,
                                                buf_len,
                                                result_len))) {
    LOG_WARN("charset convert failed", K(ret), K(from_collation), K(CS_TYPE_UTF8MB4_BIN));
  } else {
    to_string.assign(buf, static_cast<int32_t>(result_len));
  }
  return ret;
}

int ObSolidifiedVarsContext::get_local_tz_info(const sql::ObBasicSessionInfo *session, const common::ObTimeZoneInfo *&tz_info)
{
  int ret = OB_SUCCESS;
  tz_info = NULL;
  if (NULL != local_session_var_ && NULL == local_tz_wrap_) {
    ObSessionSysVar *local_var = NULL;
    //init local tz_wrap
    if (OB_FAIL(local_session_var_->get_local_var(SYS_VAR_TIME_ZONE, local_var))) {
      LOG_WARN("get local var failed", K(ret));
    } else if (NULL != local_var) {
      const ObTZInfoMap *tz_info_map = NULL;
      if (OB_ISNULL(tz_info_map = session->get_timezone_info()->get_tz_info_map())) {
        ObTZMapWrap tz_map_wrap;
        if (OB_SUCC(OTTZ_MGR.get_tenant_tz(session->get_effective_tenant_id(), tz_map_wrap))) {
          tz_info_map = tz_map_wrap.get_tz_map();
        } else {
          LOG_WARN("get tz info map failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(local_tz_wrap_ = OB_NEWx(ObTimeZoneInfoWrap, alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc tz wrap failed", K(ret));
        } else if (OB_FAIL(local_tz_wrap_->init_time_zone(local_var->val_.get_string(),
                                        OB_INVALID_VERSION,
                                        *(const_cast<ObTZInfoMap *>(tz_info_map))))) {
          LOG_WARN("tz_wrap init_time_zone failed", K(ret), K(local_var->val_.get_string()));
        }
      }
    }
  }
  if (NULL != local_tz_wrap_) {
    tz_info = local_tz_wrap_->get_time_zone_info();
  }
  return ret;
}

DEF_TO_STRING(ObSolidifiedVarsContext)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(local_session_var));
  J_OBJ_END();
  return pos;
}

ObSolidifiedVarsGetter::ObSolidifiedVarsGetter(const ObExpr &expr, const ObEvalCtx &ctx, const ObBasicSessionInfo *session)
  : local_session_var_(NULL),
  session_(session)
{
  ObPhysicalPlanCtx * phy_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_NOT_NULL(phy_ctx)) {
    if (OB_SUCCESS == (phy_ctx->get_local_session_vars(expr.local_session_var_id_, local_session_var_))) {
    }
  }
}

int ObSolidifiedVarsGetter::get_dtc_params(ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  const common::ObTimeZoneInfo *tz_info = NULL;
  dtc_params = ObBasicSessionInfo::create_dtc_params(session_);
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_time_zone_info(tz_info))) {
    LOG_WARN("get time zone info failed", K(ret));
  } else if (NULL != local_session_var_
             && OB_FAIL(ObSQLUtils::merge_solidified_var_into_dtc_params(local_session_var_->get_local_vars(),
                                                                  tz_info,
                                                                  dtc_params))) {
    LOG_WARN("fail to create local dtc params", K(ret));
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_time_zone_info(const common::ObTimeZoneInfo *&tz_info)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSessionSysVar *local_var = NULL;
  tz_info = NULL;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (NULL != local_session_var_
             && OB_FAIL(const_cast<ObSolidifiedVarsContext *>(local_session_var_)->get_local_tz_info(session_, tz_info))) {
    LOG_WARN("get local tz_info failed", K(ret));
  } else if (NULL == tz_info) {
    tz_info = session_->get_timezone_info();
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_sql_mode(ObSQLMode &sql_mode)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (FALSE_IT(sql_mode = session_->get_sql_mode())) {
  } else if (NULL != local_session_var_
             && OB_FAIL(ObSQLUtils::merge_solidified_var_into_sql_mode(local_session_var_->get_local_vars(),
                                                                       sql_mode))) {
    LOG_WARN("try get local sql mode failed", K(ret));
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_local_nls_date_format(ObString &format)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *sys_var = NULL;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_local_var(SYS_VAR_NLS_DATE_FORMAT, sys_var))) {
    LOG_WARN("fail to get local var", K(ret));
  } else if (NULL != sys_var) {
    if (OB_FAIL(sys_var->val_.get_string(format))) {
      LOG_WARN("fail to get nls_timestamp_tz_format str value", K(ret), KPC(sys_var));
    }
  } else {
    format = session_->get_local_nls_date_format();
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_local_nls_timestamp_format(ObString &format)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *sys_var = NULL;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_local_var(SYS_VAR_NLS_TIMESTAMP_FORMAT, sys_var))) {
    LOG_WARN("fail to get local var", K(ret));
  } else if (NULL != sys_var) {
    if (OB_FAIL(sys_var->val_.get_string(format))) {
      LOG_WARN("fail to get nls_timestamp_tz_format str value", K(ret), KPC(sys_var));
    }
  } else {
    format = session_->get_local_nls_timestamp_format();
  }
  return ret;
}
int ObSolidifiedVarsGetter::get_local_nls_timestamp_tz_format(ObString &format)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *sys_var = NULL;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_local_var(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT, sys_var))) {
    LOG_WARN("fail to get local var", K(ret));
  } else if (NULL != sys_var) {
    if (OB_FAIL(sys_var->val_.get_string(format))) {
      LOG_WARN("fail to get nls_timestamp_tz_format str value", K(ret), KPC(sys_var));
    }
  } else {
    format = session_->get_local_nls_timestamp_tz_format();
  }
  return ret;
}
int ObSolidifiedVarsGetter::get_local_var(ObSysVarClassType var_type, ObSessionSysVar *&sys_var)
{
  int ret = OB_SUCCESS;
  sys_var = NULL;
  if (NULL != local_session_var_
      && NULL != local_session_var_->get_local_vars()
      && OB_FAIL(local_session_var_->get_local_vars()->get_local_var(var_type, sys_var))) {
    LOG_WARN("fail to get local var", K(ret));
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_local_nls_format_by_type(const ObObjType type, ObString &format_str)
{
  int ret = OB_SUCCESS;
  switch (type) {
    case ObDateTimeType:
      if (OB_FAIL(get_local_nls_date_format(format_str))) {
        LOG_WARN("failed to get local nls date format", K(ret));
      }
      break;
    case ObTimestampNanoType:
    case ObTimestampLTZType:
      if (OB_FAIL(get_local_nls_timestamp_format(format_str))) {
        LOG_WARN("failed to get local nls timestamp format", K(ret));
      }
      break;
    case ObTimestampTZType:
      if (OB_FAIL(get_local_nls_timestamp_tz_format(format_str))) {
        LOG_WARN("failed to get local nls timestamp_tz format", K(ret));
      }
      break;
    default:
      ret = OB_INVALID_DATE_VALUE;
      SQL_SESSION_LOG(WARN, "invalid argument. wrong type for source.", K(ret), K(type));
      break;
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_max_allowed_packet(int64_t &max_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (FALSE_IT(session_->get_max_allowed_packet(max_size))) {
  } else if (NULL != local_session_var_
             && OB_FAIL(ObSQLUtils::merge_solidified_var_into_max_allowed_packet(local_session_var_->get_local_vars(),
                                                                              max_size))) {
    LOG_WARN("try get local max allowed packet failed", K(ret));
  }
  return ret;
}

int ObSolidifiedVarsGetter::get_compat_version(uint64_t &compat_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(session_->get_compatibility_version(compat_version))) {
    LOG_WARN("failed to get compat version", K(ret));
  } else if (NULL != local_session_var_
             && OB_FAIL(ObSQLUtils::merge_solidified_var_into_compat_version(local_session_var_->get_local_vars(),
                                                                             compat_version))) {
    LOG_WARN("try get local compat version failed", K(ret));
  }
  return ret;
}
