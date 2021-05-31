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
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObExprUtil::get_int64_from_obj(const ObObj& obj, ObExprCtx& expr_ctx,
    const bool is_trunc,  // true: trunc; false: round
    int64_t& out)
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
    // Types other than number, cast to number and trunc into an integer
    number::ObNumber nmb;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_NUMBER_V2(obj, nmb);
    if (OB_SUCC(ret)) {
      ret = get_int64_from_num(nmb, expr_ctx, is_trunc, out);
    }
  }
  return ret;
}

int ObExprUtil::get_trunc_int64(const ObObj& obj, ObExprCtx& expr_ctx, int64_t& out)
{
  return get_int64_from_obj(obj, expr_ctx, true, out);
}

int ObExprUtil::get_round_int64(const ObObj& obj, ObExprCtx& expr_ctx, int64_t& out)
{
  return get_int64_from_obj(obj, expr_ctx, false, out);
}

int ObExprUtil::get_trunc_int64(number::ObNumber& nmb, ObExprCtx& expr_ctx, int64_t& out)
{
  return get_int64_from_num(nmb, expr_ctx, true, out);
}

int ObExprUtil::get_round_int64(number::ObNumber& nmb, ObExprCtx& expr_ctx, int64_t& out)
{
  return get_int64_from_num(nmb, expr_ctx, false, out);
}

int ObExprUtil::get_int64_from_num(number::ObNumber& nmb, ObExprCtx& expr_ctx,
    const bool is_trunc,  // true: trunc; false: round
    int64_t& out)
{
  int ret = OB_SUCCESS;
  int64_t tmp_int = 0;
  uint64_t tmp_uint = 0;
  number::ObNumber* pnmb = &nmb;
  number::ObNumber newmb;
  if (OB_UNLIKELY(!nmb.is_integer())) {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    // copy is essential since if we did not do that, obj will be modified
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
    // do nothing
  } else if (OB_ISNULL(pnmb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. null pointer", K(ret));
  } else if (pnmb->is_valid_int64(tmp_int)) {
    out = tmp_int;
  } else if (pnmb->is_valid_uint64(tmp_uint)) {
    out = (tmp_uint > INT64_MAX) ? INT64_MAX : static_cast<int64_t>(tmp_uint);
  } else {
    out = INT64_MAX;
  }
  // ret = OB_ERR_TRUNCATED_WRONG_VALUE;
  // if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
  //   ret = OB_SUCCESS;
  //   out = 0;
  // }
  return ret;
}

int ObExprUtil::trunc_num2int64(const common::number::ObNumber& nmb, int64_t& v)
{
  int ret = OB_SUCCESS;
  ObNumStackOnceAlloc tmp_alloc;
  number::ObNumber trunc_nmb;
  const number::ObNumber* nmb_val = &nmb;

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

int ObExprUtil::get_int_param_val(ObDatum* datum, int64_t& int_val)
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

int kmp_next(const char* x, int64_t m, ObArray<int64_t>& next)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(x) || OB_UNLIKELY(m <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(m), K(ret));
  } else {
    int64_t i = 0;
    int64_t j = -1;
    next[0] = -1;
    while (i < m) {
      while (-1 < j && x[i] != x[j]) {
        j = next[j];
      }
      i++;
      j++;
      if (x[i] == x[j]) {
        next[i] = next[j];
      } else {
        next[i] = j;
      }
    }
  }
  return ret;
}

int ObExprUtil::kmp(const char* x, int64_t m, const char* y, int64_t n, int64_t count, int64_t& result)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t t = 0;
  ObArray<int64_t> next;
  result = -1;

  if (OB_ISNULL(x) || OB_ISNULL(y) || OB_UNLIKELY(m <= 0) || OB_UNLIKELY(n <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else if (OB_FAIL(next.prepare_allocate(m + 1))) {
    LOG_WARN("allocate fail", K(m), K(ret));
  } else if (m <= n) {
    // preprocessing
    if (OB_SUCC(kmp_next(x, m, next))) {
      // searching
      i = j = t = 0;
      while (j < n && -1 == result) {
        while (-1 < i && x[i] != y[j]) {
          i = next[i];
        }
        i++;
        j++;
        if (i >= m) {
          t++;
          // find nth apperance
          if (t == count) {
            result = j - i;
          }
          i = 0;
        }
      }
    }
  }
  return ret;
}

/**
 * next array is reversed, for example
 * [2, 2, 2, 3] to [3, 2, 2, 2]
 * kmp_reverse is changed according to next array
 * because next array needs one more space than delim.length
 */
int kmp_next_reverse(const char* x, int64_t m, ObArray<int64_t>& next)
{
  int ret = OB_SUCCESS;
  int64_t i = m - 1;
  int64_t j = m;

  if (OB_ISNULL(x) || OB_UNLIKELY(m <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else {
    next[0] = m;
    while (0 <= i) {
      while (j < m && x[i] != x[j]) {
        j = next[m - 1 - j];
      }
      i--;
      j--;
      if (x[i] == x[j]) {
        next[m - 1 - i] = next[m - 1 - j];
      } else {
        next[m - 1 - i] = j;
      }
    }
  }

  return ret;
}

/**
 * read reversed next array
 * next[i] to next[m-1-i]
 */
int ObExprUtil::kmp_reverse(const char* x, int64_t m, const char* y, int64_t n, int64_t count, int64_t& result)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t t = 0;
  ObArray<int64_t> next;
  result = -1;

  if (OB_ISNULL(x) || OB_ISNULL(y) || OB_UNLIKELY(m <= 0) || OB_UNLIKELY(n <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(m), K(n), K(ret));
  } else if (OB_FAIL(next.prepare_allocate(m + 1))) {
    LOG_WARN("allocate fail", K(m), K(ret));
  } else if (m <= n) {
    // preprocessing
    ret = kmp_next_reverse(x, m, next);

    // searching from back to front
    i = m - 1;
    j = n - 1;
    t = 0;
    while (0 <= j && -1 == result) {
      while (i < m && x[i] != y[j]) {
        i = next[m - 1 - i];
      }
      i--;
      j--;
      if (0 > i) {
        t--;
        // find nth apperance
        if (t == count) {
          result = j + 1;
        }
        i = m - 1;
      }
    }
  }
  return ret;
}

// Get the byte offset of each character in the multiple bytes string
// And the number of bytes occupied by each mb character
// The first element in byte_offsets is the starting byte position of
// the second character, and the last element is the length of the string
// Each element in byte_num corresponds to the
// number of bytes occupied by each character of input str
// ori_str: abc
// ori_str_byte_offsets: [0, 1, 2, 3]
// ori_str_byte_num: [1, 1, 1]
int ObExprUtil::get_mb_str_info(
    const ObString& str, ObCollationType cs_type, ObIArray<size_t>& byte_num, ObIArray<size_t>& byte_offsets)
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
  } else if (OB_FAIL(ObCharset::well_formed_len(cs_type, str.ptr(), str.length(), well_formed_len))) {
    LOG_WARN("invalid string for charset", K(str), K(well_formed_len));
  } else {
    if (OB_FAIL(byte_offsets.push_back(0))) {
      LOG_WARN("byte_offsets.push_back failed", K(ret), K(byte_offsets));
    } else {
      str_char_len = ObCharset::strlen_char(cs_type, str.ptr(), str.length());
      int64_t last_byte_index = 0;
      while (OB_SUCC(ret) && (byte_index < str.length())) {
        byte_index = ObCharset::charpos(cs_type, str.ptr() + last_byte_index, str.length() - last_byte_index, 1);
        byte_index = last_byte_index + byte_index;
        last_byte_index = byte_index;
        if (OB_FAIL(byte_offsets.push_back(byte_index))) {
          LOG_WARN("byte_offsets.push_back failed", K(ret), K(byte_offsets));
        }
      }

      // Get the number of bytes occupied by each character
      for (size_t i = 1; OB_SUCC(ret) && (i < byte_offsets.count()); ++i) {
        if (OB_FAIL(byte_num.push_back(byte_offsets.at(i) - byte_offsets.at(i - 1)))) {
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

int ObExprUtil::set_expr_ascii_result(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, const ObString& ascii)
{
  int ret = OB_SUCCESS;
  if (ascii.empty()) {
    if (lib::is_oracle_mode()) {
      expr_datum.set_null();
    } else {
      expr_datum.set_string(ObString());
    }
  } else {
    char* buf = NULL;
    const int64_t buf_len = ascii.length() * 4;  // max 4 byte for one ascii char.
    const ObCharsetInfo* cs = ObCharset::get_charset(expr.datum_meta_.cs_type_);
    if (OB_ISNULL(cs) || OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected collation", K(ret), K(expr.datum_meta_));
    } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, 1 == cs->mbminlen ? ascii.length() : buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(cs->mbminlen), K(ascii.length()));
    } else if (1 == cs->mbminlen) {
      // copy ASCII string to result directly
      if (buf != ascii.ptr()) {
        memmove(buf, ascii.ptr(), ascii.length());
      }
      expr_datum.set_string(buf, ascii.length());
    } else {
      // need charset convert
      const char* src = ascii.ptr();
      const int64_t src_len = ascii.length();
      if (!(buf >= src + src_len || src >= buf + buf_len)) {
        // overlap
        ObIAllocator& alloc = ctx.get_reset_tmp_alloc();
        void* mem = alloc.alloc(src_len);
        if (OB_ISNULL(mem)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(src_len));
        } else {
          MEMCPY(mem, src, src_len);
          src = static_cast<char*>(mem);
        }
      }
      uint32_t res_len = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObCharset::charset_convert(
                     CS_TYPE_UTF8MB4_BIN, src, src_len, expr.datum_meta_.cs_type_, buf, buf_len, res_len))) {
        LOG_WARN("convert to collation failed", K(ret), K(expr.datum_meta_), K(ObString(src_len, src)));
      } else {
        expr_datum.set_string(buf, res_len);
      }
    }
  }
  return ret;
}

int ObExprUtil::need_convert_string_collation(
    const ObCollationType& in_collation, const ObCollationType& out_collation, bool& need_convert)
{
  int ret = OB_SUCCESS;
  need_convert = true;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(in_collation) || !ObCharset::is_valid_collation(out_collation))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_collation), K(out_collation));
  } else if (ObCharset::charset_type_by_coll(in_collation) == ObCharset::charset_type_by_coll(out_collation) ||
             CS_TYPE_BINARY == in_collation || CS_TYPE_BINARY == out_collation) {
    need_convert = false;
  } else {
    // do nothing
  }
  return ret;
}

int ObExprUtil::convert_string_collation(const ObString& in_str, const ObCollationType& in_collation, ObString& out_str,
    const ObCollationType& out_collation, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  bool need_convert = false;
  if (in_str.empty()) {
    out_str.reset();
  } else if (OB_FAIL(need_convert_string_collation(in_collation, out_collation, need_convert))) {
    LOG_WARN("check need_convert_string_collation failed", K(ret));
  } else if (!need_convert) {
    out_str = in_str;
  } else {
    char* buf = NULL;
    const int32_t CharConvertFactorNum = 4;
    int32_t buf_len = in_str.length() * CharConvertFactorNum;
    uint32_t result_len = 0;
    if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(
                   in_collation, in_str.ptr(), in_str.length(), out_collation, buf, buf_len, result_len))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      out_str.assign_ptr(buf, result_len);
    }
  }
  return ret;
}

int ObExprUtil::deep_copy_str(const ObString& src, ObString& out, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  char* buf = reinterpret_cast<char*>(alloc.alloc(src.length()));
  OV(OB_NOT_NULL(buf), OB_ALLOCATE_MEMORY_FAILED);
  if (OB_SUCC(ret)) {
    MEMCPY(buf, src.ptr(), src.length());
    out.assign_ptr(buf, src.length());
  }
  return ret;
}

int ObExprUtil::eval_generated_column(const ObExpr& rt_expr, ObEvalCtx& eval_ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param_datum = NULL;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(rt_expr.args_[0]->eval(eval_ctx, param_datum))) {
    LOG_WARN("failed to eval", K(ret));
  } else {
    expr_datum.set_datum(*param_datum);
    LOG_DEBUG("generated column evaluated", K(*param_datum));
  }
  return ret;
}

int ObExprUtil::eval_stack_overflow_check(const ObExpr& expr, ObEvalCtx& ctx, ObDatum&)
{
  int ret = OB_SUCCESS;

  // control the occupied stack size by event to simulate stack overflow in testing.
  int stack_size = E(EventTable::EN_STACK_OVERFLOW_CHECK_EXPR_STACK_SIZE) 0;
  stack_size = std::abs(stack_size);
  char* cur_stack[stack_size];
  if (stack_size > 0) {
    LOG_DEBUG("cur stack", KP(cur_stack));
  }

  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("stack overflow check failed", K(ret), KP(cur_stack));
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters value failed", K(ret), KP(cur_stack));
  } else {
    // Since stack overflow check expr has the same ObDatum with child,
    // need do nothing here.
  }
  return ret;
}
