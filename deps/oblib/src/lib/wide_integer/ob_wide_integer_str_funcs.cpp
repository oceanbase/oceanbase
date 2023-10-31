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

#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
namespace wide
{

const int512_t str_helper::DEC_INT32_MIN = -get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_32);
const int512_t str_helper::DEC_INT32_MAX = get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_32);
const int512_t str_helper::DEC_INT64_MIN = -get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_64);
const int512_t str_helper::DEC_INT64_MAX = get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_64);
const int512_t str_helper::DEC_INT128_MIN = -get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_128);
const int512_t str_helper::DEC_INT128_MAX = get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_128);
const int512_t str_helper::DEC_INT256_MIN = -get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_256);
const int512_t str_helper::DEC_INT256_MAX = get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_256);
const int512_t str_helper::DEC_INT512_MIN = -get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_512);
const int512_t str_helper::DEC_INT512_MAX = get_scale_factor<int512_t>(MAX_PRECISION_DECIMAL_INT_512);

static int to_string_(const ObDecimalInt *decint, const int32_t int_bytes, char *buf,
               const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(decint)) {
    ret = OB_INVALID_ARGUMENT;
    return ret;
  }
  switch (int_bytes) {
  case sizeof(int32_t): {
    int32_t val = *(decint->int32_v_);
    ret = databuff_printf(buf, buf_len, pos, "%d", val);
    break;
  }
  case sizeof(int64_t): {
    int64_t val = *(decint->int64_v_);
    ret = databuff_printf(buf, buf_len, pos, "%ld", val);
    break;
  }
  case sizeof(int128_t): {
    const int128_t *val = static_cast<const int128_t *>(decint->int128_v_);
    ret = to_string(*val, buf, buf_len, pos);
    break;
  }
  case sizeof(int256_t): {
    const int256_t *val = static_cast<const int256_t *>(decint->int256_v_);
    ret = to_string(*val, buf, buf_len, pos);
    break;
  }
  case sizeof(int512_t): {
    const int512_t *val = static_cast<const int512_t *>(decint->int512_v_);
    ret = to_string(*val, buf, buf_len, pos);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    break;
  }
  }
  return ret;
}

static inline int64_t calc_sci_exp_str_size(int64_t sci_exp)
{
  int64_t ret = 0;
  if (sci_exp <= 9) {
    ret = 3; // e.g. E-3
  } else if (sci_exp <= 99) {
    ret = 4; // e.g. E-89
  } else {
    ret = 5; // e.g. E-101
  }
  return ret;
}
static int to_sci_string_(char *buf, const int64_t buf_len, const bool is_neg, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char sci_fmt_buf[number::ObNumber::SCI_NUMBER_LENGTH] = {0};
  int64_t sci_pos = 0, orig_pos = pos;
  bool dot_visited = false, exp_is_neg = false, carry = false;
  int64_t pre_zero_count = 0, sci_exp = 0, exp_str_size = 0, integer_cnt = 0;
  if (is_neg) {
    sci_fmt_buf[sci_pos++] = '-';
    orig_pos++;
  }
  if (buf[orig_pos] == '.') {
    dot_visited = true;
    orig_pos++;
  }
  // find first non zero digit
  while (buf[orig_pos] == '0' && orig_pos < buf_len) {
    orig_pos++;
    pre_zero_count++;
  }
  sci_fmt_buf[sci_pos++] = buf[orig_pos++];
  sci_fmt_buf[sci_pos++] = '.';
  if (dot_visited) {
    sci_exp = pre_zero_count + 1;
    exp_is_neg = true;
    exp_str_size = calc_sci_exp_str_size(sci_exp);
    // cp 1 more digit for rounding
    int64_t digit_end = number::ObNumber::SCI_NUMBER_LENGTH - exp_str_size + 1;
    while (sci_pos < digit_end) {
      if (orig_pos >= buf_len) {
        // padding zeros
        sci_fmt_buf[sci_pos++] = '0';
      } else {
        sci_fmt_buf[sci_pos++] = buf[orig_pos++];
      }
    }
  } else {
    integer_cnt++;
    for (int64_t idx = orig_pos; !dot_visited && idx < buf_len; idx++) {
      if (buf[idx] == '.') {
        dot_visited = true;
      } else {
        integer_cnt++;
      }
    }
    sci_exp = integer_cnt - 1;
    exp_str_size = calc_sci_exp_str_size(sci_exp);
    // cp 1 more for rounding
    int64_t digit_end = number::ObNumber::SCI_NUMBER_LENGTH - exp_str_size + 1;
    while (sci_pos < digit_end) {
      if (buf[orig_pos] == '.') {
        // do nothing
      } else if (orig_pos >= buf_len) {
        sci_fmt_buf[sci_pos++] = '0';
      } else {
        sci_fmt_buf[sci_pos++] = buf[orig_pos++];
      }
    } // for end
  }
  // round last digit
  if (sci_fmt_buf[sci_pos - 1] >= '5' && sci_fmt_buf[sci_pos - 1] <= '9') {
    sci_pos -= 1;
    carry = true;
    int64_t first_digit_idx = 0;
    for (int64_t idx = sci_pos - 1; carry && idx >= 0; idx--) {
      if (sci_fmt_buf[idx] == '.' || sci_fmt_buf[idx] == '-') {
        if (sci_fmt_buf[idx] == '-') {
          first_digit_idx = idx + 1;
        }
      } else if (sci_fmt_buf[idx] == '9') {
        sci_fmt_buf[idx] = '0';
      } else {
        sci_fmt_buf[idx] = sci_fmt_buf[idx] + 1;
        carry = false;
      }
    } // for end
    if (carry) {
      // e.g. 9.99999999999999999999999999999999998E+39 => 10.000000000000000000000000000000000E+40
      //      => 1.0000000000000000000000000000000000E+41
      if (exp_is_neg) {
        sci_exp -= 1;
      } else {
        sci_exp += 1;
      }

      int64_t new_exp_str_size = calc_sci_exp_str_size(sci_exp);
      if (new_exp_str_size < exp_str_size) {
        exp_str_size = new_exp_str_size;
        // if carry, exp size may be changed, fill 0 between sci_pos and exp_start_idx
        // e.g. 9.99999999999999999999999999999999998E-10
        //      => 10.000000000000000000000000000000000E-10
        //      => 1.0000000000000000000000000000000000E-9
        // len("1.0000000000000000000000000000000000E-9") = 39, need fill zero
        for (int64_t idx = sci_pos; idx < number::ObNumber::SCI_NUMBER_LENGTH - exp_str_size;
             idx++) {
          sci_fmt_buf[idx] = '0';
        }
      }
      sci_fmt_buf[first_digit_idx] = '1';
    }
  } else {
    sci_pos -= 1;
  }
  // print sci exponent
  int64_t exp_idx = number::ObNumber::SCI_NUMBER_LENGTH - exp_str_size;
  sci_fmt_buf[exp_idx++] = 'E';
  if (exp_is_neg) {
    sci_fmt_buf[exp_idx++] = '-';
  } else {
    sci_fmt_buf[exp_idx++] = '+';
  }
  exp_idx = number::ObNumber::SCI_NUMBER_LENGTH - 1;
  while (sci_exp) {
    sci_fmt_buf[exp_idx--] = '0' + sci_exp % 10;
    sci_exp /= 10;
  }
  MEMCPY(buf, sci_fmt_buf, number::ObNumber::SCI_NUMBER_LENGTH);
  pos += number::ObNumber::SCI_NUMBER_LENGTH;
  return ret;
}

int to_string(const ObDecimalInt *decint, const int32_t int_bytes, const int64_t scale, char *buf,
              const int64_t buf_len, int64_t &pos, const bool need_to_sci /* false */)
{
  int ret = OB_SUCCESS;
  int64_t length = 0, org_length = 0;
  bool zero_val = false;
  bool is_neg = false;
  int64_t orig_pos = pos;
  if (int_bytes == 0) { // zero value
    buf[pos++] = '0';
    length = 1;
    zero_val = true;
  } else if (OB_ISNULL(decint)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid null decimal int", K(ret), K(decint));
  } else if (OB_FAIL(to_string_(decint, int_bytes, buf + pos, buf_len - pos, org_length))) {
    // do nothing
  } else if (OB_FALSE_IT(length = org_length)) {
  } else if (str_helper::is_negative(decint, int_bytes)) {
    length--;
    pos++;
    is_neg = true;
  }
  bool oracle_mode = lib::is_oracle_mode();
  bool need_transform_sci = false;
  if (OB_SUCC(ret) && !zero_val) {
    if (scale > 0) {
      if (oracle_mode) {
        // in oracle mode,
        if (OB_UNLIKELY(scale >= length && pos + scale + 1 > buf_len)
            || OB_UNLIKELY(scale < length && pos + length + 1 > buf_len)) {
          // in oracle, -0.0011 displays as -.0011, 0.0011 displays as -.0011
          ret = OB_SIZE_OVERFLOW;
        }
      } else if (OB_UNLIKELY(scale >= length && pos + scale + 2 > buf_len)
                 || OB_UNLIKELY(scale < length && pos + length + 1 > buf_len)) {
        ret = OB_SIZE_OVERFLOW;
      }
      if (OB_FAIL(ret)) {
      } else if (scale >= length) {
        if (oracle_mode) {
          str_helper::prepend_chars(buf + pos, length, scale - length + 1, '0');
          buf[pos] = '.';
          pos += scale + 1;
        } else {
          str_helper::prepend_chars(buf + pos, length, scale - length + 2, '0');
          buf[pos + 1] = '.';
          pos += scale + 2;
        }
      } else {
        str_helper::prepend_chars(buf + pos + length - scale, scale, 1, '.');
        pos += length + 1;
      }
      if (oracle_mode) {
        // remove tailing zeros for decimal part
        while (buf[pos-1] == '0') pos--;
        if (buf[pos - 1] == '.') { // 3.0000 => 3
          pos--;
        }
        if (pos == orig_pos) { // zero value
          buf[pos++] = '0';
        }
      }
    } else if (scale < 0) {
      if (OB_UNLIKELY(-scale + length + pos > buf_len)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        str_helper::append_chars(buf + pos, length, -scale, '0');
        pos += length + (-scale);
      }
    } else {
      pos += length;
    }
    if (oracle_mode && need_to_sci && (pos - orig_pos) > number::ObNumber::SCI_NUMBER_LENGTH) {
      // transform to scientific notation
      int64_t tmp_pos = orig_pos;
      if (OB_FAIL(to_sci_string_(buf, pos - orig_pos, is_neg, tmp_pos))) {
        COMMON_LOG(WARN, "to_sci_string_ failed", K(ret));
      } else {
        pos = tmp_pos;
      }
    }
  }
  return ret;
}

static int parse_sci_exp(const char *buf, const int64_t buf_len, int64_t &pos, int64_t &sci_exp)
{
  int ret = OB_SUCCESS;
  sci_exp = 0;
  bool is_neg = false;
  if (buf[pos] == '+') {
    is_neg = false;
    pos++;
  } else if (buf[pos] == '-') {
    is_neg = true;
    pos++;
  }
  bool calculated = false;
  for (; OB_SUCC(ret) &&  pos < buf_len; pos++) {
    if (buf[pos] >= '0' && buf[pos] <= '9') {
      sci_exp = (sci_exp << 3) + (sci_exp << 1) + buf[pos] - '0';
      calculated = true;
    } else {
      ret = OB_INVALID_NUMERIC;
      COMMON_LOG(WARN, "invalid character for exponent", K(ret), K(buf[pos]));
    }
  }
  if (is_neg) {
    sci_exp = -sci_exp;
  }
  return ret;
}

static int set_zero_value(ObIAllocator &allocator, int32_t &val_len, ObDecimalInt *&decint)
{
  int ret = OB_SUCCESS;
  void *data = nullptr;
  if (OB_ISNULL(data = allocator.alloc(sizeof(int32_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    *(reinterpret_cast<int32_t *>(data)) = 0;
    decint = reinterpret_cast<ObDecimalInt *>(data);
    val_len = sizeof(int32_t);
  }
  return ret;
}
int from_string(const char *buf, const int64_t length, ObIAllocator &allocator,
             int16_t &scale, int16_t &precision, int32_t &val_len, ObDecimalInt *&decint)
{
  static const uint64_t constexpr DIGITS10_CNT = 18;
  int ret = OB_SUCCESS;
  int512_t tmp_res;
  int64_t pos = 0;
  bool is_neg = false;
  scale = 0;
  precision = 0;
  int64_t sci_exp = 0;
  bool is_sci_parse = false;
  bool parse_end = false;
  bool visited_floating_point = false;
  uint64_t tmp_sum;
  int64_t buf_len = length;
  static const uint64_t pows[DIGITS10_CNT + 1] =
  { 1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000 };

   if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_NUMERIC;
  } else {
    // trim spaces
    while (pos < buf_len && isspace(buf[pos])) pos++;
    while (pos < buf_len && isspace(buf[buf_len - 1])) buf_len--;
    if (pos >= buf_len) {
      ret = OB_INVALID_NUMERIC;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (buf[pos] == '-') {
    is_neg = true;
    pos++;
  } else if (buf[pos] == '+') {
    pos++;
  }

  while (OB_SUCC(ret) && pos < buf_len)
  {
    tmp_sum = 0;
    int i = 0;
    for(; OB_SUCC(ret) && pos < buf_len && i < DIGITS10_CNT; pos++) {
      if (parse_end) {
        ret = OB_INVALID_NUMERIC;
        COMMON_LOG(WARN, "parse already ended", K(ret));
      } else if (buf[pos] == 'e' || buf[pos] == 'E') {
        // parse sci exponent
        pos++;
        if (pos >= buf_len) {
          // for example: '1e' as decimal int
          // if arrived here, we have 'pos >= buf_len', no need to parse anymore.
          ret = OB_INVALID_NUMERIC;
        } else if (OB_FAIL(parse_sci_exp(buf, buf_len, pos, sci_exp))) {
          COMMON_LOG(WARN, "parse sci exp failed", K(ret));
        } else {
          parse_end = true;
        }
        is_sci_parse = true;
      } else if (buf[pos] == '.') {
        if (visited_floating_point) {
          ret = OB_INVALID_NUMERIC;
        } else {
          visited_floating_point = true;
        }
      } else if (buf[pos] >= '0' && buf[pos] <= '9') {
        precision++;
        tmp_sum = tmp_sum * 10 + buf[pos] - '0';
        i++;
        if (visited_floating_point) {
          scale++;
        }
      } else {
        ret = OB_INVALID_NUMERIC;
        COMMON_LOG(WARN, "unexpected char", K(buf[pos]));
      }
    } // for end
    tmp_res = tmp_res * pows[i] + tmp_sum;
  }
  if (precision <= 0) {
    // hasn't met digits yet, invalid number
    // e.g.
    // parse '+' as decimal_int
    // parse 'e' as decimal_int
    // parse '.e12' as decimal_int
    ret = OB_INVALID_NUMERIC;
  }
  int warning = ret;
  ret = OB_SUCCESS;
  bool as_zero = false;
  if (is_sci_parse) {
    // check if numeric over flow
    int int_part_len = (precision > scale ? (precision - scale) : 0);
    if ((int_part_len > 0 && sci_exp + int_part_len <= number::ObNumber::MIN_SCI_SIZE)
        || (int_part_len <= 0 && sci_exp - (scale - precision) <= number::ObNumber::MIN_SCI_SIZE)) {
      as_zero = true;
    } else if (int_part_len + sci_exp > number::ObNumber::MAX_SCI_SIZE && tmp_res != 0) {
      ret = OB_NUMERIC_OVERFLOW;
    }
  }
  if (as_zero) {
    tmp_res = 0;
    precision = 1;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    if (sci_exp != 0) { scale = scale - sci_exp; }
    void *data = NULL;
    if (is_neg) { tmp_res = -tmp_res; }
    unsigned cp_sz = ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    data = allocator.alloc(cp_sz);
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to allocate memory", K(ret), K(cp_sz));
    } else {
      MEMCPY(data, tmp_res.items_, cp_sz);
      decint = reinterpret_cast<ObDecimalInt *>(data);
      val_len = cp_sz;
    }
    if (warning != OB_SUCCESS) { ret = warning; }
  }
  return ret;
}

} // end namespace wide
} // end namespace common
} // end namespace oceanbase
