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

int ObExprUtil::get_int_param_val(ObDatum *datum, int64_t &int_val)
{
  int ret = OB_SUCCESS;
  if (NULL != datum && !datum->is_null()) {
    if (lib::is_mysql_mode()) {
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
  const double pow_val = std::pow(10, static_cast<double>(std::abs(dec)));
  volatile double val_div_tmp = val / pow_val;
  volatile double val_mul_tmp = val * pow_val;
  volatile double res = 0.0;
  if (dec < 0 && std::isinf(pow_val)) {
    res = 0.0;
  } else if (dec >= 0 && std::isinf(val_mul_tmp)) {
    res = val;
  } else {
    res = dec < 0 ? rint(val_div_tmp) * pow_val : rint(val_mul_tmp) / pow_val;
  }
  LOG_DEBUG("round double done", K(val), K(dec), K(res));
  return res;
}

double ObExprUtil::round_double_nearest(double val, int64_t dec)
{
  const double pow_val = std::pow(10, static_cast<double>(std::abs(dec)));
  volatile double val_div_tmp = val / pow_val;
  volatile double val_mul_tmp = val * pow_val;
  volatile double res = 0.0;
  if (dec < 0 && std::isinf(pow_val)) {
    res = 0.0;
  } else if (dec >= 0 && std::isinf(val_mul_tmp)) {
    res = val;
  } else {
    res = dec < 0 ? std::round(val_div_tmp) * pow_val : std::round(val_mul_tmp) / pow_val;
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
  } else if (dec >= 0 && std::isinf(val_mul_tmp)) {
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
