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

#define USING_LOG_PREFIX LIB
#include "lib/number/ob_number_v2.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"
#include "lib/worker.h"
#include <assert.h>
#include <type_traits>
#include "lib/charset/ob_dtoa.h"
#include <algorithm>
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/utility/ob_hang_fatal_error.h"

namespace oceanbase
{
namespace common
{
using namespace lib;
namespace number
{
const char *ObNumber::DIGIT_FORMAT = "%09u";
const char *ObNumber::BACK_DIGIT_FORMAT[DIGIT_LEN] =
{
  "%09u",
  "%08u",
  "%07u",
  "%06u",
  "%05u",
  "%04u",
  "%03u",
  "%02u",
  "%01u",
};

const int ObNumber::MIN_ROUND_DIGIT_COUNT[] = {
    ObNumber::FLOATING_SCALE / ObNumber::DIGIT_LEN,
    common::OB_MAX_NUMBER_PRECISION_INNER / ObNumber::DIGIT_LEN
};

const char ObNumber::FLOATING_ZEROS[ObNumber::FLOATING_SCALE + 1] =
    "000000000000000000000000000000000000000000000000000000000000000000000000";

const ObNumber &ObNumber::get_pi()
{
  // len, flag, exp, sign
  static Desc desc(10, 0, EXP_ZERO, POSITIVE);
  static uint32_t digits[] = {3,141592653,589793238,462643383,279502884,197169399,375105820,974944592,307816406,286208998};
  static ObNumber pi(desc.desc_, reinterpret_cast<uint32_t*>(digits));
  return pi;
}

const ObString NUMBER_ERRMSG("ERROR NUM");//9 byte

// ObNumber 序列化反序列化实现，完全参考了 ObObj 中的代码
DEFINE_SERIALIZE(ObNumber)
{
  return serialization::encode_number_type(buf, buf_len, pos, d_, digits_);
}

DEFINE_DESERIALIZE(ObNumber)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(serialization::decode_number_type(buf, data_len, pos, d_, digits_))) {
    d_.reserved_ = 0; // reserved_ is always 0.
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObNumber)
{
  return serialization::encode_length_number_type(d_);
}

const ObNumber &ObNumber::get_positive_one()
{
  struct Init
  {
    explicit Init(ObNumber &v)
    {
      static StackAllocator sa;
      v.from("1", sa);
    };
  };
  static ObNumber one;
  static Init init(one);
  return one;
}

const ObNumber &ObNumber::get_positive_zero_dot_five()
{
  struct Init
  {
    explicit Init(ObNumber &v)
    {
      static StackAllocator sa;
      v.from("0.5", sa);
    };
  };
  static ObNumber zero_dot_five;
  static Init init(zero_dot_five);
  return zero_dot_five;
}

const ObNumber &ObNumber::get_zero()
{
  struct Init
  {
    explicit Init(ObNumber &v)
    {
      v.set_zero();
    };
  };
  static ObNumber one;
  static Init init(one);
  return one;
}

uint32_t *ObNumber::alloc_(IAllocator &allocator, const int64_t num, const lib::ObMemAttr *attr)
{
  if (0 >= num) {
    digits_ = NULL;
  } else {
    digits_ = NULL != attr ? allocator.alloc(num * sizeof(uint32_t), *attr) : allocator.alloc(num * sizeof(uint32_t));
  }
  d_.reserved_ = 0; // keep reserved_ always 0.
  return digits_;
}

bool ObNumber::is_equal(const number::ObNumber::Desc &this_desc,
  const uint32_t *this_digits, const number::ObNumber::Desc &other_desc,
  const uint32_t *other_digits)
{
  bool bret = false;
  if (this_desc.se_ == other_desc.se_ && this_desc.len_ == other_desc.len_) {
    bret = uint32equal(this_digits, other_digits, this_desc.len_) ;
  }
  return bret;
}

int ObNumber::compare(const number::ObNumber::Desc &this_desc,
  const uint32_t *this_digits, const number::ObNumber::Desc &other_desc,
  const uint32_t *other_digits)
{
  int ret = 0;
  if (this_desc.se_ == other_desc.se_) {
    if (this_desc.len_ == other_desc.len_) {
      ret = uint32cmp(this_digits, other_digits, this_desc.len_);
    } else if (this_desc.len_ < other_desc.len_) {
      if (0 == (ret = uint32cmp(this_digits, other_digits, this_desc.len_))) {
        ret = -1;
      }
    } else {
      if (0 == (ret = uint32cmp(this_digits, other_digits, other_desc.len_))) {
        ret = 1;
      }
    }
    if (NEGATIVE == this_desc.sign_) {
      ret = -ret;
    }
  } else if (this_desc.se_ < other_desc.se_) {
    ret = -1;
  } else {
    ret = 1;
  }
  return ret;
}

/*
  对于整型数值，可以做如下的优化：
  计算每一个进制位上的对应的数值（整型最多有两位），并根据整型参数填充Number的DESC（符号位、exp等）。
  比如对于1000000001，进制位10^9，不断除以10^9，可以计算出两个进制位上的数值为(1，1)，即digit数组为
  [1, 1]，并且可以知道指数值为1，符号位的值为1，digit的有效长度为2
*/
template <class IntegerT>
int ObNumber::from_integer_(const IntegerT value, IAllocator &allocator) {
  int ret = OB_SUCCESS;
  static const uint64_t MAX_DIGITS[3] = {1L,
           1000000000L,
           1000000000000000000L};
  if (0 == value) {
    set_zero();
  } else {
    Desc desc;
    uint64_t abs_val = 0;

    if (std::is_signed<IntegerT>::value) { // 如果是符号整数，调用abs
  #ifdef __clang__
  #pragma clang diagnostic push
  #pragma clang diagnostic ignored "-Wabsolute-value"
  #endif
      abs_val = (uint64_t)std::labs(value);
  #ifdef __clang__
  #pragma clang diagnostic pop
  #endif

    } else { // 否则直接赋值
      abs_val = value;
    }
    uint64_t exp = 0;
    uint32_t digits[OB_MAX_INTEGER_DIGIT] = {0};
    uint8_t idx = 0;
    uint8_t zero_cnt = 0;

    for (int i = 2; i >= 0; i--) {
      if (abs_val >= MAX_DIGITS[i]) {
        idx += zero_cnt;
        digits[idx++] = uint32_t(abs_val / MAX_DIGITS[i]);
        exp = std::max(exp, (uint64_t)i);
        abs_val %= MAX_DIGITS[i];
        zero_cnt = 0;
      } else if (exp > 0) {
        zero_cnt++;
      }
    }

    desc.exp_ = ((uint8_t)exp + EXP_ZERO) & 0x7f;
    if (value >= 0) {
      desc.sign_ = POSITIVE;
    } else {
      desc.sign_ = NEGATIVE;
      desc.exp_ = 0x7f & (~desc.exp_);
      ++desc.exp_;
    }
    desc.len_ = idx;
    desc.reserved_ = 0;

    uint32_t* digit_mem = NULL;
    if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * idx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc obnumber digit memory", K(ret), K(digit_mem), K(idx));
    } else {
      MEMCPY(digit_mem, digits, ITEM_SIZE(digits_)*idx);
      assign(desc.desc_, (uint32_t*)digit_mem);
    }
  }
  return ret;
}

int ObNumber::from_(const int64_t value, IAllocator &allocator)
{
  return from_integer_(value, allocator);
}

int ObNumber::from_(const uint64_t value, IAllocator &allocator)
{
  return from_integer_(value, allocator);
}

int ObNumber::from_(const char *str, IAllocator &allocator, int16_t *precision, int16_t *scale, const bool do_rounding)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t length = (NULL == str) ? 0 : strlen(str);
  ret = from_(str, length, allocator, warning, NULL, precision, scale, NULL, do_rounding);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

/* from_sci -- from scientific notation
 *
 * str -- IN, scientific notation string representation
 * length -- IN, the length of the input string
 * allocator -- IN, used to alloc ObNumber
 * precision -- OUT, precision of the converted result
 * scale -- OUT, scale of the converted result
 * This function converts a scientific notation string into the internal number format
 * It's more efficient to call from_ if the string is a normal numeric string */
int ObNumber::from_sci_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
     int16_t *precision, int16_t *scale, const bool do_rounding)
{
  int ret = OB_SUCCESS;
  char full_str[MAX_PRINTABLE_SIZE] = {0};
  char digit_str[MAX_PRINTABLE_SIZE] = {0};
  bool is_neg = false;
  int32_t nth = 0;
  int32_t i_nth = 0;
  int32_t i = 0;
  int32_t e_value = 0;
  int32_t valid_len = 0;
  int32_t dec_n_zero = 0;
  bool e_neg = false;
  bool as_zero = false;
  bool has_digit = false;
  char tmpstr[MAX_PRINTABLE_SIZE] = {0};
  char cur = '\0';
  if (OB_UNLIKELY(NULL == str || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(length), KP(str), K(ret));
  } else {
    /* parse str like 1.2e3
     * part 1:allow str like 000123=> 123(valid length is 3); if valid length >126, ignore the rest
     * part 2:if '.' exists, part 2 must not be empty: str like 1.e3 is illegal
     * part 3:part 3's value + length of part1 <= 126
     * */
    for (i = 0; i < length && isspace(str[i]); ++i);
    if ('-' == str[i]) {
      is_neg = true;
      full_str[nth++] = '-';
      i++;
    } else if ('+' == str[i]) {
      i++;
    }
    /* 000123 -> 123 */
    while ('0' == str[i] && i + 1 < length) {
      i++;
      has_digit = true;
    }
    cur = str[i];
    while(i + 1 < length && cur <= '9' && cur >= '0') {
      if (i_nth < MAX_PRECISION) {
        digit_str[i_nth++] = cur;
        cur = str[++i];
        has_digit = true;
      } else {
        /* ignore the rest */
        i++;
      }
      valid_len++;
    }
  }

  if (OB_SUCC(ret) && cur == '.' && valid_len < MAX_PRECISION && i + 1 < length) {
    cur = str[++i];
    if (0 == valid_len) {
      /* 0.000123 -> dec_n_zero = 3 */
      while ('0' == cur && i + 1 < length) {
        valid_len--;
        dec_n_zero++;
        cur = str[++i];
        has_digit = true;
      }
    }
    /* 1.23  0.0123 0.123 -> digit_str:'123' */
    while(i + 1 < length && cur <= '9' && cur >= '0') {
      if (i_nth < MAX_PRECISION) {
        digit_str[i_nth++] = cur;
      } else {
        /* ignore the rest */
      }
      cur = str[++i];
      if (0 >= valid_len) {
        valid_len--;
      }
    }
  }

  if (OB_SUCC(ret) && (has_digit || 0 < i_nth) 
                   && ('e' == cur || 'E' == cur) 
                   && is_valid_sci_tail_(str, length, i)) {
    LOG_DEBUG("ObNumber from sci", K(ret), K(i), K(cur), K(is_neg), K(nth), KCSTRING(digit_str), K(i_nth),
      K(valid_len), K(dec_n_zero));
    if (0 == i || i >= length - 1) {
      if (i_nth > 0) {
        memcpy(full_str + nth, digit_str, i_nth);
        nth += i_nth;
      }
      warning = OB_INVALID_NUMERIC;
    } else if (0 == valid_len || 0 == i_nth) {
      // `i_nth = 0` means all digits are zero.
      /* ignore e's value; only do the check*/
      cur = str[++i];
      if ('-' == cur || '+' == cur) {
        if (i < length - 1) {
          cur = str[++i];
        }
      }
      if (cur <= '9' && cur >= '0') {
        while (cur <= '9' && cur >= '0' && (++i < length)) {
          cur = str[i];
        }
      } else {
        /* 0e */
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("Number from sci invalid exponent", K(ret), K(cur), K(i));
      }
    } else {
      cur = str[++i];
      switch (cur) {
      case '-':
        e_neg = true;
        /* go through */
      case '+':
        if (i < length - 1) {
          cur = str[++i];
        }
        break;
      }
      /* Oracle max valid length of string is 255(exponent of the value is [-253, 255])
       * exponent of number's legal range is [-130, 125]
       * so e_value's valid range can't larger than 3 digits */
      int e_cnt = 0;
      while (i < length && cur <= '9' && cur >= '0') {
        if (e_cnt < 4) {
          e_value = e_neg ? (e_value * 10 - (cur - '0')) : (e_value * 10 + cur - '0');
        }
        e_cnt++;
        if (++i >= length) {
          break;
        }
        cur = str[i];
      }

      LOG_DEBUG("ObNumber from sci E", K(warning), K(e_neg), K(e_cnt), K(e_value), K(valid_len), K(i));
      if (0 == e_cnt) {
        warning = OB_INVALID_NUMERIC;
        e_value = 0;
      }
      if ((valid_len >= 0 && (e_value + valid_len <= MIN_SCI_SIZE))
          ||(valid_len < 0 && (e_value - dec_n_zero <= MIN_SCI_SIZE))) {
        as_zero = true;
      } else if (e_value + valid_len > MAX_SCI_SIZE) {
        ret = OB_NUMERIC_OVERFLOW;
      } else if (valid_len <= 0) {
        /* 0.01234e-5 */
        if (e_value < 0) {
          nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "0.%0*d%s", 0 - e_value + dec_n_zero, 0, digit_str);
          LOG_DEBUG("ObNumber sci", KCSTRING(tmpstr), K(nth), KCSTRING(full_str), K(e_value), K(dec_n_zero), KCSTRING(digit_str));
        } else {
          if (dec_n_zero - e_value > 0) {
            /* 0.00012e2 -> 0.012 */
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "0.%0*d%s", dec_n_zero - e_value, 0, digit_str);
            LOG_DEBUG("ObNumber sci", KCSTRING(tmpstr), K(e_value), K(dec_n_zero));
          } else if (e_value < dec_n_zero + i_nth) {
            /* 0.001234e4 -> 12.34
             * e_value - dec_n_zero = 4 - 2 = 2
             * fmt str: %2.s%s, digit_str, digit_str + 2*/
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%.*s.%s", e_value - dec_n_zero, digit_str, digit_str + e_value - dec_n_zero);
            LOG_DEBUG("ObNumber sci", KCSTRING(tmpstr), KCSTRING(full_str), K(nth), K(e_value), K(dec_n_zero), KCSTRING(digit_str));
          } else {
            /* 0.001234e8 -> 123400
             * e_value - dec_n_zero - i_nth = 8 - 2 - 4 = 2 */
            if (e_value - dec_n_zero - i_nth > 0) {
              snprintf(tmpstr, MAX_PRINTABLE_SIZE, "%0*d", e_value - dec_n_zero - i_nth, 0);
            }
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%s%s", digit_str, tmpstr);
            LOG_DEBUG("ObNumber sci", KCSTRING(tmpstr), KCSTRING(digit_str), KCSTRING(full_str), K(nth), K(e_value), K(i_nth), K(dec_n_zero));
          }
        }
      } else {
        if (e_value >= 0) {
          /* 12.34e5 -> 1234000
           * e_value - (i_nth - valid_len) = 5 - (4 - 2) = 0*/
          if (e_value - (i_nth - valid_len) > 0) {
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%s%0*d",
                digit_str, e_value - (i_nth - valid_len), 0);
            LOG_DEBUG("ObNumber sci", KCSTRING(tmpstr), KCSTRING(full_str), K(nth), K(e_value), K(i_nth), K(valid_len));
          } else if (e_value - (i_nth - valid_len) == 0) {
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%s", digit_str);
            LOG_DEBUG("ObNumber sci", KCSTRING(full_str), K(nth), K(e_value), K(i_nth), K(valid_len));
          } else {
            /* 12.345e2 -> 1234.5
             * valid_len + e_value = 2 + 2 = 4
             * fmt_str: %4.s, digit_str, digit_str + 4*/
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%.*s.%s",
                valid_len + e_value, digit_str, digit_str + valid_len + e_value);
            LOG_DEBUG("ObNumber sci", K(valid_len + e_value), KCSTRING(full_str), K(nth), K(e_value), KCSTRING(digit_str), K(valid_len));
          }
        } else {
          if (valid_len + e_value > 0)
          {
            /* 12.34e-1 -> 1.234
             * valid_len + e_value = 2 - 1 = 1
             * fmt_str: %1.s, digit_str, digit_str + 1*/
            //sprintf(tmpstr, "%%%d.s.%%s", valid_len + e_value);
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%.*s.%s",
                valid_len + e_value, digit_str, digit_str + valid_len + e_value);
            LOG_DEBUG("ObNumber sci", K(valid_len + e_value), KCSTRING(full_str), K(nth), K(e_value), KCSTRING(digit_str), K(valid_len));
          } else {
            /* 12.34e-4 -> 0.001234
             * 0 - (valid_len + e_value) = 0 - (2 - 4) = 2 */
            if (valid_len + e_value < 0) {
              snprintf(tmpstr, MAX_PRINTABLE_SIZE, "%0*d", 0 - (valid_len + e_value), 0);
            }
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "0.%s%s", tmpstr, digit_str);
            LOG_DEBUG("ObNumber sci", KCSTRING(tmpstr), KCSTRING(full_str), K(nth), K(e_value), KCSTRING(digit_str), K(valid_len));
          }
        }
      }
    }
    if (OB_SUCC(ret) || !is_oracle_mode()) {
      LOG_DEBUG("Number from sci last ", K(cur), K(i), "str", ObString(length, str),
               K(length), K(valid_len), K(ret), K(warning));
      while (cur == ' ' && i < length - 1) {
        cur = str[++i];
      }
      if (cur != ' ' && i <= length - 1) {
        warning = OB_INVALID_NUMERIC;
        LOG_WARN("invalid numeric string", K(ret), K(i), K(length), K(cur), "str", ObString(length, str));
      }
      if ((OB_SUCCESS != warning) && is_oracle_mode() && OB_SUCC(ret)) {
        ret = warning;
      } else if (OB_FAIL(ret) && !is_oracle_mode()) {
        as_zero = true;
//        warning = OB_ERR_DOUBLE_TRUNCATED;
        warning = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("ObNumber sci final", K(ret), K(warning), K(full_str), K(nth), K(as_zero), K(e_neg), K(e_value), K(valid_len), K(i), K(i_nth));
        if (as_zero || 0 == valid_len || 0 == i_nth) {
          full_str[0] = '0';
          nth = 1;
          set_zero();
        } else {
          int tmp_warning = OB_SUCCESS;
          ret = from_(full_str, nth, allocator, tmp_warning, NULL, precision, scale, NULL, do_rounding);
          if (OB_SUCC(ret) && OB_SUCCESS != warning) {
            warning = tmp_warning;
          }
        }
      }
    }
  } else {
    ret = from_(str, length, allocator, warning, NULL, precision, scale, NULL, do_rounding);
  }
  return ret;
}

int ObNumber::from_v1_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
    ObNumberFmtModel *fmt, int16_t *precision, int16_t *scale, const lib::ObMemAttr *attr)
{
  int ret = OB_SUCCESS;
  uint32_t digits[MAX_CALC_LEN];
  ObNumberBuilder nb;
  nb.number_.set_digits(digits);
  nb.number_.d_.reserved_ = 0;
  if (OB_UNLIKELY(NULL == str || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(length), KP(str), K(ret));
  } else if (OB_FAIL(nb.build(str, length, warning, fmt, precision, scale))) {
    LIB_LOG(WARN, "number build from fail", K(ret), K(length), "str", ObString(length, str));
  } else if (OB_FAIL(is_oracle_mode()
                     ? nb.number_.round_scale_oracle_(MAX_SCALE, true, precision, scale)
                     : nb.number_.round_scale_(FLOATING_SCALE, true))) {
    LIB_LOG(WARN, "round scale fail", K(ret), K(length), "str", ObString(length, str));
  } else if (OB_FAIL(exp_check_(nb.number_.get_desc()))) {
    LIB_LOG(WARN, "exponent precision check fail", K(ret));
    if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
      set_zero();
      ret = OB_SUCCESS;
    }
  } else if (0 == nb.number_.d_.len_) {
    set_zero();
  } else if (OB_UNLIKELY(nb.number_.d_.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LIB_LOG(WARN,  "out of range, ret = %d , length = %d",
                  K(ret), K(nb.number_.d_.len_));
  } else if (OB_ISNULL(digits_ = alloc_(allocator, nb.number_.d_.len_, attr))) {
    _OB_LOG(WARN, "alloc digits fail, length=%hhu", nb.number_.d_.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, nb.number_.get_digits(), nb.number_.d_.len_ * ITEM_SIZE(digits_));
    d_ = nb.number_.d_;
    d_.reserved_ = 0;
  }
  return ret;
}

int ObNumber::from_v2_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
    ObNumberFmtModel *fmt, int16_t *precision, int16_t *scale, const lib::ObMemAttr *attr, const bool do_rounding)
{
  int ret = OB_SUCCESS;
  uint32_t digits[MAX_CALC_LEN] = {};
  ObNumberBuilder nb;
  nb.number_.set_digits(digits);
  nb.number_.d_.reserved_ = 0;
  if (OB_ISNULL(str) || OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(length), KP(str), K(ret));
  } else if (OB_FAIL(nb.build_v2(str, length, warning, fmt, precision, scale))) {
    LIB_LOG(WARN, "number build from fail", K(ret), K(length), "str", ObString(length, str));
  } else if (do_rounding && OB_FAIL(nb.number_.round_scale_v3_(is_oracle_mode() ? MAX_SCALE: FLOATING_SCALE, true, false))) {
    _LIB_LOG(WARN, "round scale fail, ret=%d str=[%.*s]", ret, static_cast<int32_t>(length), str);
  } else if (OB_FAIL(exp_check_(nb.number_.get_desc(), lib::is_oracle_mode()))) {
    LIB_LOG(WARN, "exponent precision check fail", K(nb.number_), K(ret));
    if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
      set_zero();
      ret = OB_SUCCESS;
    }
  } else if (0 == nb.number_.d_.len_) {
    set_zero();
  } else if (OB_UNLIKELY(nb.number_.d_.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LIB_LOG(WARN,  "out of range", K(ret), K(nb.number_.d_.len_));
  } else if (OB_ISNULL(digits_ = alloc_(allocator, nb.number_.d_.len_, attr))) {
    _OB_LOG(WARN, "alloc digits fail, length=%hhu", nb.number_.d_.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, nb.number_.get_digits(), nb.number_.d_.len_ * ITEM_SIZE(digits_));
    d_ = nb.number_.d_;
    d_.reserved_ = 0;
  }
  return ret;
}

/*
 * [start, floating_point-1] : int range
 * [floating_point+1, end-1] : decimal range
 */
int ObNumber::find_point_range_(const char *str, const int64_t length,
    int64_t &start_idx, int64_t &floating_point, int64_t &end_idx,
    bool &negative, int32_t &warning, int16_t *precision, int16_t *scale)
{
  int ret = OB_SUCCESS;
  int64_t tmp_precision = 0;
  int64_t tmp_scale = 0;
  int64_t tmp_start_idx = 0;
  if (OB_UNLIKELY(NULL == str || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(length), KP(str), K(ret));
  } else {
    const char *str_ptr = str;
    const char *str_end = str + length;

    for (; str_ptr != str_end && isspace(*str_ptr); ++str_ptr); /* ignore leading space */
    for (; str_ptr != str_end && isspace(*(str_end - 1)); --str_end); /* ignore tailer space */
    if (OB_UNLIKELY(str_ptr == str_end)) {
      //empty string is enabled
      warning = OB_INVALID_NUMERIC;
    } else {
      bool contains_sign = false;
      /* check positive or negative */
      if (*str_ptr == '+') {
        ++str_ptr;
        contains_sign = true;
      } else if (*str_ptr == '-') {
        ++str_ptr;
        negative = true;
        contains_sign = true;
      }
      int64_t str_length = str_end - str_ptr;
      // In oracle mode, the number is invalid when str is '+', '-' or '.' without any digits.
      // e.g: ' +', '-.' '+. '
      if (lib::is_oracle_mode() &&
          ((0 == str_length && contains_sign) || (1 == str_length && *str_ptr == '.'))) {
        ret = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "invalid number", K(ret), KCSTRING(str));
      }

      if (OB_SUCC(ret)) {
        for (; str_ptr != str_end && *str_ptr == '0'; ++str_ptr); /* ignore leading zero */
        if (str_ptr == str_end) {
          // Handleing all zeros cases.  E.G: '0' or '000'.
          // Make end_idx and start_idx point to same position so that consequent
          // construct_digits_() would generate 0 num.
          // Make start_idx point to last 0.
          start_idx = str_end - 1 - str;
          tmp_start_idx = start_idx;
          floating_point = start_idx;
          end_idx = start_idx;
        } else {

          start_idx = str_ptr - str;
          tmp_start_idx = ((*str_ptr == '.') ? (start_idx - 1) : start_idx);

          for (; str_ptr != str_end && ('0' <= *str_ptr && *str_ptr <= '9'); ++str_ptr);

          if (str_ptr != str_end && *str_ptr == '.') { /* find floating point */
            floating_point = str_ptr - str;
            ++str_ptr;
            for (; str_ptr != str_end && ('0' <= *str_ptr && *str_ptr <= '9'); ++str_ptr);
            end_idx = str_ptr - str;
          } else {
            floating_point = str_ptr - str;
            end_idx = floating_point;
          }

          if (str_ptr != str_end) {
            warning = OB_INVALID_NUMERIC;
          }
        }
      }
    }
    if (NULL != precision && NULL != scale) {
      *scale = static_cast<int16_t>((end_idx - floating_point >= 1) ? (end_idx - floating_point - 1) : 0);
      if (*scale > ObNumber::FLOATING_SCALE) {
        *scale = ObNumber::FLOATING_SCALE;
      }
      *precision = static_cast<int16_t>(floating_point - tmp_start_idx + *scale);
      tmp_precision = *precision;
      tmp_scale = *scale;
    }
  }
  const ObString tmp_string(length, str);
  LIB_LOG(DEBUG, "succ to find_point_range_", K(tmp_string), K(start_idx),
                K(tmp_start_idx), K(floating_point), K(end_idx), K(negative),
                K(ret), K(warning), K(tmp_precision), K(tmp_scale));
  return ret;
}

int ObNumber::construct_digits_(const char *str, const int64_t start_idx,
    const int64_t floating_point, const int64_t end_idx, uint32_t *digits, int64_t &exp, int64_t &len)
{
  int ret = OB_SUCCESS;
  const int64_t POWS[] = {1, 10, 100, 1000, 10000, 100000, 1000000, \
                          10000000, 100000000, 1000000000, 10000000000};

  int64_t di_id = 0;
  int step = 0;
  bool has_integer_part = false;
  uint32_t *curr_digit = digits + di_id;
  if (start_idx < floating_point) {
    const char *str_ptr = str + start_idx;
    const char *str_end = str + floating_point;
    int integer_length = (floating_point - start_idx - 1) / ObNumber::MAX_STORE_LEN + 1; // = floor[(end - start_idx + 1) / MAX_STORE_LEN]
    step = (floating_point - start_idx - 1) % ObNumber::MAX_STORE_LEN + 1;
    for (; str_ptr < str_end && di_id < len; str_ptr += step, ++di_id, step = ObNumber::MAX_STORE_LEN) {
      *curr_digit = ObFastAtoi<uint32_t>::atoi_positive_unchecked(str_ptr, str_ptr + step);
//      LIB_LOG(DEBUG, "push to construct_digits_", "di_id", di_id, "digit", *curr_digit, "curr_idx", str_ptr - str, K(step));
      ++curr_digit;
    }
    exp = integer_length - 1;
    has_integer_part = true;
  }
  if (di_id < len && floating_point + 1 < end_idx) {
    const char *str_ptr = str + floating_point + 1;
    const char *str_end = str + end_idx;
    if (!has_integer_part) {
      for (; str_ptr != str_end && *str_ptr == '0'; ++str_ptr); // ignore leading zero in decimal part
      int decimal_zero = ((str_ptr - str) - floating_point - 1) / ObNumber::MAX_STORE_LEN + 1;
      exp = -decimal_zero;
      step = ObNumber::MAX_STORE_LEN - ((str_ptr - str) - floating_point - 1) % ObNumber::MAX_STORE_LEN;
    } else {
      step = ObNumber::MAX_STORE_LEN;
    }
    for (; str_ptr < str_end && di_id < len; str_ptr += step, ++di_id, step = ObNumber::MAX_STORE_LEN) {
      if (str_ptr + step < str_end) {
        *curr_digit = ObFastAtoi<uint32_t>::atoi_positive_unchecked(str_ptr, str_ptr + step);
      } else {
        *curr_digit = ObFastAtoi<uint32_t>::atoi_positive_unchecked(str_ptr, str_end);
        *curr_digit *= POWS[step - (str_end - str_ptr)];
      }
//        LIB_LOG(DEBUG, "push to construct_digits_", "di_id", di_id, "digit", *curr_digit, "curr_idx", str_ptr - str, K(step));
      ++curr_digit;
    }

  }

  curr_digit = digits + di_id - 1;
  while (curr_digit >= digits && 0 == *curr_digit) {
    --curr_digit;
  }
  len = (curr_digit >= digits ? (curr_digit - digits + 1) : 0);
  LIB_LOG(DEBUG, "succ to construct_digits_", K(exp), K(len));
  return ret;
}

int ObNumber::from_v3_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
    ObNumberFmtModel *fmt, int16_t *precision, int16_t *scale, const lib::ObMemAttr *attr, const bool do_rounding)
{
  int ret = OB_SUCCESS;
  UNUSED(fmt);
  uint32_t digits[OB_CALC_BUFFER_SIZE] ={}; // 只保存最多 72 位有效数字，占位 9 个uint32，多余的数字四舍五入
  bool negative = false;
  int64_t start_idx = -1;
  int64_t floating_point = -1;
  int64_t end_idx = -1;
  int64_t exp = 0;
  int64_t len = OB_CALC_BUFFER_SIZE;

  Desc d;
  /* Step 1: find floating point and integer & decimal range */
  if (OB_FAIL(find_point_range_(str, length, start_idx, floating_point, end_idx, negative, warning,
              precision, scale))) {
    LOG_WARN("find floating point and range error\n");
  /* Step 2: construct digits with fixed length, ignore overflowed, return exp and len */
  } else if (OB_FAIL(construct_digits_(str, start_idx, floating_point, end_idx, digits, exp, len))) {
    LOG_WARN("fail to construct digits\n");
  /* Step 3: calc desc : promise exp and len are in range */
  } else {
    d.desc_ = 0;
    d.sign_ = negative ? ObNumber::NEGATIVE : ObNumber::POSITIVE;
    d.len_ = (uint8_t)len;
    d.reserved_ = 0; // keep always 0.
    d.exp_ = 0x7f & (uint8_t)(EXP_ZERO + exp);
    if (d.sign_ == NEGATIVE) {
      d.exp_ = (0x7f & ~d.exp_) + 1;
    }
  }

  if (OB_SUCC(ret)) {
    d_.desc_ = d.desc_;
    if (0 == d.len_) {
      set_zero();
    /* Step 5: construct ObNumber */
    } else if (OB_ISNULL(digits_ = alloc_(allocator, d.len_, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG(ERROR, "alloc digits fail, length=%hhu", d.len_);
    } else {
      MEMCPY(digits_, digits, d.len_ * sizeof(uint32_t));

      /* Step 6: normalize to 72 digits and check */
      if (do_rounding && OB_FAIL(round_scale_v3_(is_oracle_mode() ?
                      (-MIN_SCI_SIZE): FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret));
      } else if (OB_FAIL(exp_check_(d_, lib::is_oracle_mode()))) {
        LOG_WARN("exponent precision check fail", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          set_zero();
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    set_zero();
  }
  return ret;
}
int ObNumber::from_(const uint32_t desc, const ObCalcVector &vector, IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);

  ObCalcVector normalized_vector = vector;
  if (OB_FAIL(normalized_vector.normalize())) {
    LOG_WARN("normalized_vector.normalize() fails", K(ret));
  }
  Desc d;
  d.desc_ = desc;
  d.len_ = (uint8_t)std::min(+MAX_STORE_LEN, normalized_vector.size());
  if (OB_FAIL(ret)) {
    LOG_WARN("Previous normalize() fails", K(ret));
  } else if (0 == d.len_) {
    set_zero();
  } else if (OB_UNLIKELY(d.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    _OB_LOG(WARN, "out of range, ret = %d , length = %d",
                  ret, (int)d.len_);
  } else if (OB_ISNULL(digits_ = alloc_(allocator, d.len_))) {
    LOG_WARN("alloc digits fail", K(d.len_));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, normalized_vector.get_digits(), d.len_ * ITEM_SIZE(digits_));
    d_.sign_ = d.sign_;
    d_.exp_ = d.exp_;
    if (OB_FAIL(normalize_(digits_, d.len_))) {
      _OB_LOG(WARN, "normalize [%s] fail, ret=%d", to_cstring(*this), ret);
    } else if (OB_FAIL(round_scale_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true))) {
      LOG_WARN("round scale fail", K(ret), K(*this));
    } else if (OB_FAIL(exp_check_(d_, lib::is_oracle_mode()))) {
      LOG_WARN("exponent precision check fail", K(ret), K(*this));
      if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
        set_zero();
        ret = OB_SUCCESS;
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObNumber::from_v2_(const uint32_t desc, const ObCalcVector &vector, IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObCalcVector normalized_vector = vector;
  if (OB_FAIL(normalized_vector.normalize())) {
    LOG_WARN("normalized_vector.normalize() fails", K(ret));
  }
  Desc d;
  d.desc_ = desc;
  d.len_ = (uint8_t)std::min(+MAX_STORE_LEN, normalized_vector.size());
  if (OB_FAIL(ret)) {
    LOG_WARN("Previous normalize() fails", K(ret));
  } else if (0 == d.len_) {
    set_zero();
  } else if (OB_UNLIKELY(d.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    _OB_LOG(WARN, "out of range, ret = %d , length = %d", ret, (int)d.len_);
  } else if (OB_ISNULL(digits_ = alloc_(allocator, d.len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc digits fail", K(d.len_));
  } else {
    MEMCPY(digits_, normalized_vector.get_digits(), d.len_ * ITEM_SIZE(digits_));
    d_.sign_ = d.sign_;
    d_.exp_ = d.exp_;
    if (OB_FAIL(normalize_(digits_, d.len_))) {
      _OB_LOG(WARN, "normalize [%s] fail, ret=%d", to_cstring(*this), ret);
    } else if (OB_FAIL(round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(*this));
    } else if (OB_FAIL(exp_check_(d_, lib::is_oracle_mode()))) {
      LOG_WARN("exponent precision check fail", K(ret), K(*this));
      if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
        set_zero();
        ret = OB_SUCCESS;
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObNumber::from_(
    const int16_t precision,
    const int16_t scale,
    const char *str,
    const int64_t length,
    IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);

  if (OB_UNLIKELY(MIN_PRECISION > precision
      || MAX_PRECISION < precision
      || MIN_SCALE > scale
      || MAX_SCALE < scale
      || NULL == str
      || 0 >= length)) {
    LOG_WARN("invalid param",
             K(precision), K(scale), KP(str), K(length));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint32_t digits[MAX_CALC_LEN];
    ObNumberBuilder nb;
    nb.number_.set_digits(digits);
    nb.number_.d_.reserved_ = 0;

    if (OB_FAIL(nb.build(str, length, warning))) {
      _OB_LOG(WARN, "number build from fail, ret=%d str=[%.*s]", ret, (int)length, str);
    } else if (OB_FAIL(nb.number_.check_and_round(precision, scale))) {
      _OB_LOG(WARN, "check and round fail, ret=%d str=[%.*s]", ret, (int)length, str);
    } else if (0 == nb.number_.d_.len_) {
      set_zero();
    } else if (OB_UNLIKELY(nb.number_.d_.len_ > ObNumber::MAX_CALC_LEN)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      _OB_LOG(WARN, "out of range, ret = %d , length = %d",
                    ret, (int)nb.number_.d_.len_);
    } else if (OB_ISNULL(digits_ = alloc_(allocator, nb.number_.d_.len_))) {
      LOG_WARN("alloc digits fail", K(nb.number_.d_.len_));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(digits_, nb.number_.get_digits(), nb.number_.d_.len_ * ITEM_SIZE(digits_));
      d_ = nb.number_.d_;
      d_.reserved_ = 0;
    }
  }
  if (OB_FAIL(ret)) {
    // pass
  } else if (OB_SUCCESS != warning) {
    ret = warning;
  } else { /* Do nothing */ }
  return ret;
}

int ObNumber::from_(const ObNumber &other, IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);

  if (&other == this) {
    // assign to self
  } else if (0 == other.d_.len_) {
    set_zero();
  } else if (OB_UNLIKELY(other.d_.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    _OB_LOG(WARN, "out of range, ret = %d , length = %d",
                  ret, (int)other.d_.len_);
  } else if (OB_ISNULL(digits_ = alloc_(allocator, other.d_.len_))) {
    _OB_LOG(DEBUG, "alloc digits fail, length=%hhu", other.d_.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, other.digits_, other.d_.len_ * ITEM_SIZE(digits_));
    d_ = other.d_;
    d_.reserved_ = 0;
  }
  return ret;
}

int ObNumber::deep_copy(const ObNumber &other, IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  uint32_t *digits = NULL;
  if (&other == this) {
    // assign to self
  } else if (0 == other.d_.len_) {
    set_zero();
  } else if (OB_UNLIKELY(other.d_.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    _OB_LOG(WARN, "out of range, ret = %d , length = %d",
                  ret, (int)other.d_.len_);
  } else if (OB_ISNULL(digits = alloc_(allocator, other.d_.len_))) {
    _OB_LOG(DEBUG, "alloc digits fail, length=%hhu", other.d_.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits, other.digits_, other.d_.len_ * ITEM_SIZE(digits));
    d_ = other.d_;
    d_.reserved_ = 0;
    digits_ = digits;
    OB_LOG(DEBUG, "deep_copy", K(other));
  }
  return ret;
}

int ObNumber::deep_copy_v3(const ObNumber &other, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  uint32_t *digits = NULL;
  if (&other == this) {
    // assign to self
  } else if (0 == other.d_.len_) {
    set_zero();
  } else if (OB_UNLIKELY(other.d_.len_ > ObNumber::MAX_CALC_LEN)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    _OB_LOG(WARN, "out of range, ret = %d , length = %d",
                  ret, (int)other.d_.len_);
  } else if (OB_ISNULL(digits = (uint32_t *)allocator.alloc(sizeof(uint32_t) * other.d_.len_))) {
    _OB_LOG(DEBUG, "alloc digits fail, length=%hhu", other.d_.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits, other.digits_, other.d_.len_ * ITEM_SIZE(digits));
    d_ = other.d_;
    d_.reserved_ = 0; // keep always 0.
    digits_ = digits;
//    OB_LOG(DEBUG, "deep_copy_v3", K(other));
  }
  return ret;
}

int ObNumber::deep_copy_to_allocator_(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  number::ObNumber tmp;
  if (OB_FAIL(tmp.from(*this, allocator))) {
    LOG_WARN("failed: depp copy *this to tmp with given allocator",
             KPC(this), K(tmp), K(ret));
  } else {
    *this = tmp;
  }
  return ret;
}

int ObNumber::from_(const ObNumber &other, uint32_t *digits)
{
  int ret = OB_SUCCESS;
  if (&other == this) {
    // assign to self
  } else if (0 == other.d_.len_) {
    set_zero();
  } else if (OB_UNLIKELY(NULL == digits || MAX_STORE_LEN < other.d_.len_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    digits_ = digits;
    MEMCPY(digits_, other.digits_, other.d_.len_ * ITEM_SIZE(digits_));
    d_ = other.d_;
    d_.reserved_ = 0;
  }
  return ret;
}

int ObNumber::floor(const int64_t scale)
{
  int ret = OB_SUCCESS;
  int int_len = 0;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);
  if (OB_UNLIKELY(0 != scale)) {
    LOG_WARN("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= d_.len_)) {
    ret = OB_NOT_INIT;
  } else {
    if (POSITIVE == d_.sign_) {
      int_len = d_.se_ - POSITIVE_EXP_BOUNDARY;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else if (NEGATIVE == d_.sign_) {
      int_len = NEGATIVE_EXP_BOUNDARY - d_.se_;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else { /* Do nothing */ }
    if (int_len > 0 && int_len < d_.len_) {
      if (POSITIVE == d_.sign_) {
        for (int64_t i = int_len; i < d_.len_; ++i) {
          digits_[i] = 0;
        }
      } else if (NEGATIVE == d_.sign_) {
        for (int64_t i = int_len; i < d_.len_; ++i) {
          digits_[i] = 0;
        }
        for (int64_t i = int_len - 1; i >= 0; --i) {
          if ((BASE - 1) != digits_[i]) {
            digits_[i] += 1;
            break;
          } else {
            digits_[i] = 0;
          }
        }
        if (0 == digits_[0] && NEGATIVE == d_.sign_) {
          for (int64_t i = (int_len - 1); i >= 0; --i) {
            digits_[i + 1] = digits_[i];
          }
          digits_[0] = 1;
          --d_.exp_;
        }
      } else { /* Do nothing */ }
      if (int_len < MAX_STORE_LEN) {
        d_.len_ = static_cast<unsigned char>(int_len);
      }
    } else if (-1 == int_len && OB_SUCCESS == ret) {
      for (int64_t i = 0; i < d_.len_; ++i) {
        digits_[i] = 0;
      }
      if (POSITIVE == d_.sign_) {
        set_zero();
      } else if (NEGATIVE == d_.sign_) {
        digits_[0] = 1;
        d_.se_ = 0x40;
        d_.len_ = 1;
      }
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObNumber::ceil(const int64_t scale)
{
  int ret = OB_SUCCESS;
  int int_len = 0;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);
  if (OB_UNLIKELY(0 != scale)) {
    LOG_WARN("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= d_.len_)) {
    ret = OB_NOT_INIT;
  } else {
    if (POSITIVE == d_.sign_) {
      int_len = d_.se_ - POSITIVE_EXP_BOUNDARY;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else if (NEGATIVE == d_.sign_) {
      int_len = NEGATIVE_EXP_BOUNDARY - d_.se_;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else { /* Do nothing */ }
    if (int_len > 0 && int_len < d_.len_) {
      for (int64_t i = int_len; i < d_.len_; ++i) {
        digits_[i] = 0;
      }
      d_.len_ = static_cast<uint8_t>(int_len);
      if (POSITIVE == d_.sign_) {
        for (int64_t i = int_len - 1; i >= 0; --i) {
          if ((BASE - 1) != digits_[i]) {
            digits_[i] += 1;
            break;
          } else {
            digits_[i] = 0;
          }
        }
        if (0 == digits_[0]) {
          // since int_len is less than len_, so the loop below is safe,
          // we won't get a buffer overflow problem.
          for (int64_t i = (int_len - 1); i >= 0; --i) {
            digits_[i + 1] = digits_[i];
          }
          digits_[0] = 1;
          ++d_.len_;
          ++d_.exp_;
        }
      }
    } else if (int_len < 0) {
      for (int64_t i = 0; i < d_.len_; ++i) {
        digits_[i] = 0;
      }
      if (POSITIVE == d_.sign_) {
        digits_[0] = 1;
        d_.se_ = 0xc0;
        d_.len_ = 1;
      } else if (NEGATIVE == d_.sign_) {
        set_zero();
      }
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObNumber::trunc(const int64_t scale)
{
  int ret = OB_SUCCESS;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);

  if (OB_UNLIKELY(MIN_SCALE > scale
      || MAX_SCALE < scale)) {
    LOG_WARN("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= d_.len_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(trunc_scale_(scale, false))) {
    LOG_WARN("trunc scale failed", K(*this), K(ret));
  } else {
    LOG_DEBUG("trunc scale succ", K(*this), K(scale));
    // do nothing
  }
  return ret;
}

int ObNumber::round_v1(const int64_t scale)
{
  int ret = OB_SUCCESS;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);
  if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= d_.len_)) {
    ret = OB_NOT_INIT;
  } else if (is_oracle_mode()) {
    if (OB_FAIL(round_scale_oracle_(scale, false))) {
      //_OB_LOG(WARN, "Buffer overflow, %s", to_cstring(*this));
    }
  } else {
    if (OB_UNLIKELY(MIN_SCALE > scale
        || MAX_SCALE < scale)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param", K(scale), K(ret));
    } else if (OB_FAIL(round_scale_(scale, false))) {
    //_OB_LOG(WARN, "Buffer overflow, %s", to_cstring(*this));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObNumber::check_and_round(const int64_t precision, const int64_t scale)
{
  int ret = OB_SUCCESS;
  ObTRecover recover_guard(*this, OB_SUCCESS, ret);

  if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= d_.len_)) {
    ret = OB_NOT_INIT;
  } else if (INT64_MAX != precision
             && OB_FAIL(round_scale_v3_(scale, false, false))) {
    //_OB_LOG(WARN, "Buffer overflow, %s", to_cstring(*this));
  } else if (INT64_MAX != precision
             && INT64_MAX != scale
             && OB_FAIL(check_precision_(precision, scale))) {
//    _OB_LOG(WARN, "Precision overflow, %s", to_cstring(*this));
  } else {
    // do nothing
  }
  return ret;
}

int64_t ObNumber::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (d_.len_ > 0 && OB_ISNULL(digits_)) {
    databuff_printf(buffer, length, pos, "WARN the pointer is null");
  } else {
    databuff_printf(buffer, length, pos, "\"sign=%hhu exp=%hhu se=0x%hhx len=%hhu digits=[", d_.sign_,
                    d_.exp_, d_.se_, d_.len_);
    for (uint8_t i = 0; i < d_.len_; ++i) {
      databuff_printf(buffer, length, pos, "%u,", digits_[i]);
    }
    databuff_printf(buffer, length, pos, "]\"");
  }

  return pos;
}

bool ObNumber::is_valid_uint64(uint64_t &uint64) const
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(&bret, NULL, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WARN_RET(OB_ERROR, "can't to check the param range", K(bret));
  } else {
    uint64 = tmp_int_parts;
    bret = bret && (0 == tmp_decimal_parts);//Should not use if-test.
  }

  return bret;
}

bool ObNumber::is_valid_int64(int64_t &int64) const
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(NULL, &bret, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WARN_RET(OB_ERROR, "can't to check the param range", K(bret));
  } else {
    int64 = is_negative() ? (-1 * tmp_int_parts) : tmp_int_parts;
    bret = bret && (0 == tmp_decimal_parts);
  }

  return bret;
}

// 小数点儿后全部截断
int ObNumber::extract_valid_int64_with_trunc(int64_t &value) const
{
  int ret = common::OB_SUCCESS;
  if (!is_valid_int64(value)) {
    number::ObNumber tmp_number;
    char buf_alloc[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, ObNumber::MAX_BYTE_LEN);
    //need deep copy before round
    if (OB_FAIL(tmp_number.from(*this, allocator))) {
      LOG_WARN("fail to deep_copy", K(ret), K(tmp_number));
    } else if (OB_FAIL(tmp_number.trunc(0))) {
      LOG_WARN("fail to trunc", K(ret), K(tmp_number));
    } else if (!tmp_number.is_valid_int64(value)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid const type for array index", K(tmp_number), K(ret));
    }
  }

  return ret;
}

int ObNumber::extract_valid_uint64_with_trunc(uint64_t &value) const
{
  int ret = common::OB_SUCCESS;
  if (!is_valid_uint64(value)) {
    number::ObNumber tmp_number;
    char buf_alloc[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, ObNumber::MAX_BYTE_LEN);
    //need deep copy before round
    if (OB_FAIL(tmp_number.from(*this, allocator))) {
      LOG_WARN("fail to deep_copy", K(ret), K(tmp_number));
    } else if (OB_FAIL(tmp_number.trunc(0))) {
      LOG_WARN("fail to trunc", K(ret), K(tmp_number));
    } else if (!tmp_number.is_valid_uint64(value)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid const type for array index", K(tmp_number), K(ret));
    }
  }

  return ret;
}

int ObNumber::extract_valid_int64_with_round(int64_t &value) const
{
  int ret = common::OB_SUCCESS;
  if (!is_valid_int64(value)) {
    number::ObNumber tmp_number;
    char buf_alloc[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, ObNumber::MAX_BYTE_LEN);
    //need deep copy before round
    if (OB_FAIL(tmp_number.from(*this, allocator))) {
      LOG_WARN("fail to deep_copy", K(ret), K(tmp_number));
    } else if (OB_FAIL(tmp_number.round(0))) {
      LOG_WARN("fail to trunc", K(ret), K(tmp_number));
    } else if (!tmp_number.is_valid_int64(value)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid const type for array index", K(tmp_number), K(ret));
    }
  }
  return ret;
}

int ObNumber::extract_valid_uint64_with_round(uint64_t &value) const
{
  int ret = common::OB_SUCCESS;
  if (!is_valid_uint64(value)) {
    number::ObNumber tmp_number;
    char buf_alloc[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, ObNumber::MAX_BYTE_LEN);
    //need deep copy before round
    if (OB_FAIL(tmp_number.from(*this, allocator))) {
      LOG_WARN("fail to deep_copy", K(ret), K(tmp_number));
    } else if (OB_FAIL(tmp_number.round(0))) {
      LOG_WARN("fail to trunc", K(ret), K(tmp_number));
    } else if (!tmp_number.is_valid_uint64(value)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid const type for array index", K(tmp_number), K(ret));
    }
  }
  return ret;
}

int ObNumber::width_bucket(const ObNumber &start, const ObNumber &end, const ObNumber &bucket, ObNumber &value, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber bucket_num, res;
  char buf_bucket[ObNumber::MAX_BYTE_LEN];
  char buf_res[ObNumber::MAX_BYTE_LEN];
  char buf_alloc1[ObNumber::MAX_BYTE_LEN];
  char buf_alloc2[ObNumber::MAX_BYTE_LEN];
  char buf_alloc3[ObNumber::MAX_BYTE_LEN];
  ObDataBuffer allocator_bucket(buf_bucket, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_res(buf_res, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator1(buf_alloc1, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator2(buf_alloc2, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator3(buf_alloc3, ObNumber::MAX_BYTE_LEN);
  if (OB_FAIL(bucket_num.from(bucket, allocator_bucket))) {
    LOG_WARN("copy bucket number failed", K(bucket_num), K(ret));
  } else if (OB_FAIL(bucket_num.floor(0))) {
    LOG_WARN("bucket floor failed", K(bucket_num), K(ret));
  } else if (OB_UNLIKELY(bucket_num.is_zero() || bucket_num.is_negative())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bucket < 1", K(bucket_num), K(ret));
  } else {
    ObNumber range,offset;
    if (start == end) {
      if (*this < start) {
        if (OB_FAIL(res.from(static_cast<int64_t>(0), allocator_res))) {
          LOG_WARN("create number 0 failed", K(res), K(ret));
        }
      } else {
        ObNumber temp;
        if(OB_FAIL(temp.from(static_cast<int64_t>(1), allocator1))) {
          LOG_WARN("create number 1 failed", K(res), K(temp), K(ret));
        } else if (OB_FAIL(bucket_num.add(temp, res, allocator_res))) {
          LOG_WARN("bucket add 1 failed", K(bucket_num), K(res), K(ret));
        }
      }
    } else {
      if (start > end) {
        if (OB_FAIL(start.sub(end, range, allocator1))) {
          LOG_WARN("start sub end failed", K(start), K(end), K(range), K(ret));
        } else if(OB_FAIL(start.sub(*this, offset, allocator2))) {
          LOG_WARN("start sub target failed", K(start), K(offset), K(ret));
        }
      } else {
        if (OB_FAIL(end.sub(start, range, allocator1))) {
          LOG_WARN("end sub start failed", K(start), K(end), K(range), K(ret));
        } else if(OB_FAIL(sub(start, offset, allocator2))) {
          LOG_WARN("end sub target failed", K(start), K(offset), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (offset.is_negative()) {
          if (OB_FAIL(res.from(static_cast<int64_t>(0), allocator_res))) {
            LOG_WARN("assign 0 to res failed", K(res), K(ret));
          }
        } else if (offset >= range) {
          ObNumber temp;
          if (OB_FAIL(temp.from(static_cast<int64_t>(1), allocator3))) {
            LOG_WARN("create number 1 failed", K(temp), K(ret));
          } else if (OB_FAIL(bucket_num.add(temp, res, allocator_res))) {
            LOG_WARN("bucket add 1 failed", K(bucket_num), K(temp), K(res), K(ret));
          }
        } else {
          ObNumber q;
          if (OB_FAIL(range.div(bucket_num, q, allocator3))) {
            LOG_WARN("width_bucket:range div bucket failed", K(range), K(bucket_num), K(q), K(ret));
          } else  if (OB_FAIL(offset.div(q, res, allocator_res))) {
            LOG_WARN("width_bucket:offset div q failed", K(offset), K(q), K(res), K(ret));
          } else if (OB_FAIL(res.floor(0))) {
            LOG_WARN("width_bucket:value floor failed", K(res), K(ret));
          } else {
            ObNumber temp;
            allocator1.free();
            allocator2.free();
            std::swap(allocator2, allocator_res);
            if (OB_FAIL(temp.from(static_cast<int64_t>(1), allocator1))) {
              LOG_WARN("create number 1 failed", K(temp), K(ret));
            } else if (OB_FAIL(res.add(temp, res, allocator_res))) {
              LOG_WARN("value add 1 failed", K(res), K(temp), K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(value.from(res, allocator))) {
        LOG_WARN("copy res failed", K(value), K(res), K(ret));
      }
    }
  }
  return ret;
}

bool ObNumber::is_valid_int() const
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(NULL, &bret, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WARN_RET(OB_ERROR, "can't to check the param range", K(bret));
  } else {
    bret = bret && (0 == tmp_decimal_parts);
  }

  return bret;
}

bool ObNumber::is_int_parts_valid_int64(int64_t &int_parts, int64_t &decimal_parts) const
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(NULL, &bret, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WARN_RET(OB_ERROR, "can't to check the param range", K(bret));
  } else {
    decimal_parts = tmp_decimal_parts;
    int_parts = is_negative() ? (-1 * tmp_int_parts) : tmp_int_parts;
  }
  LOG_DEBUG("is int parts valid int64", K(*this), K(int_parts), K(decimal_parts));
  return bret;
}

int ObNumber::check_range(bool *is_valid_uint64, bool *is_valid_int64,
                           uint64_t &int_parts, uint64_t &decimal_parts) const
{
  /*
   * 为了注释能短点，这回就用中文写了。不是我英文水平不行啊。^_^
   * 对于一个负数，它绝对不可能是一个合法的uint64
   * -value < -x 等价于 value > x 因此，取绝对值来判断
   * 对于一个正数，uint64的范围比int64范围大。
   * 这几种情况，上限都不同，放到了THRESHOLD数组里。
   * 正数、负数；uint64、int64；四种组合情况
   * 其中负数与uint64这种情况直接枪毙，因此THRESHOLD只有三个upperbound。
   *
   *
   */

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((NULL == is_valid_uint64) && (NULL == is_valid_int64))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the param is invalid", K(ret));
  } else {
    bool *is_valid = NULL;
    static const int FLAG_INT64 = 0;
    static const int FLAG_UINT64 = 1;
    static const int SIGN_NEG = 0;
    static const int SIGN_POS = 1;
    int flag = FLAG_INT64;
    if (NULL != is_valid_uint64) {
      is_valid = is_valid_uint64;
      flag = FLAG_UINT64;
    } else {
      is_valid = is_valid_int64;
    }
    *is_valid = true;
    if (OB_UNLIKELY(is_zero())) {
      //do nothing
    } else {
      int sign = is_negative() ? SIGN_NEG : SIGN_POS;
      if ((SIGN_NEG == sign) && (FLAG_UINT64 == flag)) {
        *is_valid = false; //there are no neg guys in uint64 !
      }
      uint64_t value = 0;
      static uint64_t ABS_INT64_MIN = 9223372036854775808ULL;
                                           //int64        uint64
      static uint64_t THRESHOLD[2][2] = {{ABS_INT64_MIN, 0}, //neg
                                         {INT64_MAX,     UINT64_MAX}}; //pos
      ObDigitIterator di;
      di.assign(d_.desc_, digits_);
      uint32_t digit = 0;
      bool from_integer = false;
      bool last_decimal = false;
      while (OB_SUCC(di.get_next_digit(digit, from_integer, last_decimal))) {
        if (from_integer) {
          //In case of overflow we can not write "value * BASE + digit <= THRESHOLD[index]"
          if (*is_valid && value <= (THRESHOLD[sign][flag] - digit) / BASE) {
            value = value * BASE + digit;
          } else {
            *is_valid = false; //no break
          }
        } else {
          decimal_parts = digit;
          break;
        }
      }
      ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      int_parts = value; //no matter valid or not, we set int_parts always here.
    }
  }
  return ret;
}

int ObNumber::check_precision_(const int64_t precision, const int64_t scale)
{
  int ret = OB_SUCCESS;
  int64_t limit = precision - scale;
  ObDigitIterator di;
  di.assign(d_.desc_, digits_);
  int64_t integer_counter = 0;
  int64_t decimal_zero_counter = 0;
  uint32_t digit = 0;
  bool head_flag = true;
  bool from_integer = false;
  bool last_decimal = false;
  while (OB_SUCCESS == (ret = di.get_next_digit(digit, from_integer, last_decimal))) {
    if (from_integer) {
      if (head_flag) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }
      if (OB_UNLIKELY(integer_counter > limit)) {
        _OB_LOG(WARN, "Precision=%ld scale=%ld integer_number=%ld precision overflow %s",
                  precision, scale, integer_counter, to_cstring(*this));
        ret = OB_INTEGER_PRECISION_OVERFLOW;
        break;
      }
    } else {
      if (0 != digit) {
        decimal_zero_counter += (DIGIT_LEN - get_digit_len(digit));
      } else {
        decimal_zero_counter += DIGIT_LEN;
      }
      if (decimal_zero_counter >= -limit) {
        break;
      }
    }
    head_flag = false;
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  if (OB_UNLIKELY(OB_SUCCESS == ret
      && decimal_zero_counter < -limit)) {
    LOG_WARN("precision overflow ",
             K(precision), K(scale), K(decimal_zero_counter), K(*this));
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
  }
  return ret;
}

int ObNumber::round_scale_(const int64_t scale, const bool using_floating_scale)
{
  int ret = OB_SUCCESS;
  ObDigitIterator di;
  di.assign(d_.desc_, digits_);

  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  uint32_t integer_digits[MAX_CALC_LEN];
  int64_t integer_length = 0;
  int64_t integer_counter = 0;
  uint32_t decimal_digits[MAX_CALC_LEN];
  int64_t decimal_length = 0;
  while (OB_SUCC(di.get_next_digit(digit, from_integer, last_decimal))) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= integer_length
        || MAX_CALC_LEN <= decimal_length)) {
      LOG_WARN("buffer size overflow",
                K(integer_length), K(decimal_length));
      ret = OB_NUMERIC_OVERFLOW;
      break;
    }
    if (from_integer) {
      if (0 == integer_length) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }

      integer_digits[integer_length++] = digit;
    } else if (0 <= scale) {
      if (using_floating_scale
          && 0 == digit
          && 0 == integer_length
          && 0 == decimal_length) {
        continue;
      }
      decimal_digits[decimal_length++] = digit;
      if ((decimal_length * DIGIT_LEN) > scale) {
//        LOG_DEBUG("Number need round decimal", K(decimal_length), K(scale), K(integer_counter));
        break;
      }
    } else {
      break;
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  int64_t floating_scale = scale;
  if (OB_SUCCESS == ret
      && using_floating_scale) {
    floating_scale -= integer_counter;
  }
//  LOG_DEBUG("Number before rebuild", K(is_oracle_mode()), K(using_floating_scale),
//      K(scale), K(floating_scale), K(integer_counter), K(integer_length),
//      K(decimal_length), KPC(this), K(lbt()));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 < floating_scale) {
    if (OB_FAIL(round_decimal_(floating_scale, decimal_digits, decimal_length))) {
      LOG_ERROR("fail to get round_decimal");
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, decimal_length);
    }
  } else if (0 > floating_scale) {
    if (OB_FAIL(round_integer_(-floating_scale, integer_digits, integer_length))) {
      LOG_ERROR("fail to get round_integer");
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
    }

  } else if (0 == floating_scale) {
    if (0 < decimal_length) {
      if ((5 * BASE / 10) <= decimal_digits[0]) {
        decimal_digits[0] = BASE;
      } else {
        decimal_digits[0] = 0;
      }
      ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, 1);
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObNumber::round_scale_v2_(const int64_t scale, const bool using_floating_scale,
    const bool for_oracle_to_char,
    int16_t *res_precision/*NULL*/, int16_t *res_scale/*NULL*/)
{
  static const uint64_t ROUND_POWS_DESC[] = {
      1000000000,
      100000000,
      10000000,
      1000000,
      100000,
      10000,
      1000,
      100,
      10,
  };

  static const uint64_t ROUND_POWS_ASC[] = {
      1000000000,
      10,
      100,
      1000,
      10000,
      100000,
      1000000,
      10000000,
      100000000
  };

  int ret = OB_SUCCESS;
  if (is_zero()) {
    //do nothing
  } else {
    const int64_t expr_value = get_decode_exp(d_);
    //xxx_length means xx digit array length
    const int64_t integer_length = (expr_value >= 0 ? (expr_value + 1) : 0);
    //e.g. 1,000000000,000000000,   digits=[1], expr_value=2, len_=1, integer_length=3, valid_integer_length=1
    const int64_t valid_integer_length = (d_.len_ > integer_length ? integer_length : d_.len_);
    //xxx_count means xx digit count
    const int64_t integer_count = (valid_integer_length > 0 ? (get_digit_len_v2(digits_[0]) + (integer_length - 1) * DIGIT_LEN) : 0);

    //len_ > integer_length means have decimal
    const int64_t valid_decimal_length = (d_.len_ > integer_length ? (d_.len_ - integer_length) : 0);
    //e.g. 0.000000000 000000001, digits=[1], expr_value=-2, len_=1, decimal_length=2, valid_decimal_length=1
    const int64_t decimal_length = valid_decimal_length + ((0 == integer_length && !using_floating_scale) ? (0 - expr_value - 1) : 0);

    int64_t floating_scale = scale;
    if (is_oracle_mode()) {
      const int64_t decimal_prefix_zero_count = ((0 == integer_length) ? (0 - expr_value - 1 + DIGIT_LEN - get_digit_len_v2(digits_[0])) : 0);
      int64_t valid_precision = 0;
      if (for_oracle_to_char) {
        valid_precision = OB_MAX_NUMBER_PRECISION_INNER - 1 - (is_decimal() ? 1 : 0) - (is_negative() ? 1 : 0);
      } else {
        valid_precision = (OB_MAX_NUMBER_PRECISION_INNER - ((0 == integer_count ? decimal_prefix_zero_count : integer_count) % 2));
      }
      int64_t r_precision = 0;
      int64_t r_scale = 0;
      int64_t decimal_non_zero_count = 0;
      int64_t tail_decimal_zero_count = 0;
      if (valid_decimal_length > 0) {
        remove_back_zero(digits_[d_.len_ - 1], tail_decimal_zero_count);
        decimal_non_zero_count = (DIGIT_LEN * valid_decimal_length
            - (0 == integer_length ? (DIGIT_LEN - get_digit_len_v2(digits_[0])) : 0)
            - tail_decimal_zero_count);
      }
      const int64_t decimal_count = decimal_length * DIGIT_LEN - tail_decimal_zero_count;

      if (using_floating_scale) {
        if (integer_count > 0) {
          floating_scale = valid_precision - integer_count;
        } else if (decimal_non_zero_count > valid_precision) {
          /* decimal_cnt - non-zero decimal digit + valid decimal number */
          floating_scale = MIN(scale, (decimal_count - decimal_non_zero_count + valid_precision));
        }
        r_precision = PRECISION_UNKNOWN_YET;
        r_scale = ORA_NUMBER_SCALE_UNKNOWN_YET;
      } else {
        if (integer_count > 0) {
          if (scale > 0) {
            /* 1..30th.123  scale = 30 floating_scale = 0  res_p = 30 res_s = 0
             * 1..20th.123  scale = 10 floating_scale = 10 res_p = 20
             * 1...42th.123 scale = 10 floating_scale = -2 res_s = 10 res_p = 40, res_s = -2
             * 1.000..20th0123  scale = 22 floating_scale = 22 res_s = 22 res_p = 22*/
            floating_scale = MIN(valid_precision - integer_count, scale);
            r_precision = MIN(valid_precision, integer_count + decimal_count);
            r_scale = floating_scale < 0 ? floating_scale : MIN(floating_scale, decimal_count);
          } else {
            /* 1234.345 scale = -2, res_p = 2 res_s = -2
             * 1234.324 scale = -10 res_p = 0 res_s = -10
             * 1...42th.123 scale = -1 res_p = 40, res_s = -2*/
            r_precision = MIN(valid_precision, MAX(integer_count + scale, 0));
            r_scale = MIN(integer_count - valid_precision, scale);
          }
        } else if (decimal_non_zero_count > valid_precision) {
          /* total_length - non-zero decimal digit + valid decimal number */
          floating_scale = MIN(scale, (decimal_count - decimal_non_zero_count + valid_precision));
          /* 0.345 scale = -2, res_p = 2 res_s = -2
           * 1234.324 scale = -10 res_p = 0 res_s = -10
           * 1...42th.123 scale = -1 res_p = 40, res_s = -2*/
          if (scale > 0) {
            r_precision = MIN(valid_precision, decimal_non_zero_count - (scale - decimal_count));
          } else {
            r_precision = 0;
          }
          r_scale = floating_scale;
        }
      }

      if (NULL != res_precision && NULL != res_scale) {
        if (using_floating_scale
            || (r_precision <= valid_precision
                && r_precision >= 0
                && r_scale <= OB_MAX_NUMBER_SCALE
                && r_scale >= OB_MIN_NUMBER_SCALE)) {
          *res_precision = static_cast<int16_t>(r_precision);
          *res_scale = static_cast<int16_t>(r_scale);
        } else {
          LOG_WARN("got invalid precision or scale on oracle mode, use builded",
              K(r_precision), K(r_scale));
        }
      }
      LOG_DEBUG("Number before rebuild v2", K(is_oracle_mode()), K(using_floating_scale), K(scale),
                K(floating_scale), K(expr_value), K(integer_length), K(valid_integer_length), K(integer_count),
                K(decimal_length), K(valid_decimal_length), K(decimal_count), K(decimal_prefix_zero_count),
                K(decimal_non_zero_count), K(r_precision), K(r_scale), KPC(this), KCSTRING(lbt()));
    } else {
      if (using_floating_scale && valid_integer_length > 0) {
        floating_scale -= integer_count;
      }
      LOG_DEBUG("Number before rebuild v2", K(is_oracle_mode()), K(using_floating_scale), K(scale),
                K(floating_scale), K(expr_value), K(integer_length), K(valid_integer_length),
                K(decimal_length), K(valid_decimal_length), KPC(this), KCSTRING(lbt()));
    }

    bool need_normalize = false;
    if (floating_scale > 0) {
      //round decimal
      const int64_t round_length = floating_scale / DIGIT_LEN;
      if (round_length >= decimal_length) {
        // do nothing
//        LOG_DEBUG("with no round", K(decimal_length), K(round_length), K(valid_decimal_length));
      } else if (decimal_length > valid_decimal_length
                 && round_length < (decimal_length - valid_decimal_length)) {
        //e.g. value = 0.000000000 000000000 000000001 000000002, digits=[1,2], expr_value=-3, len_=2,
        //     decimal_length=4, valid_decimal_length=2
        //round(value, 8), round(value, 9), round(value, 17)
        set_zero();
      } else {
        //e.g. value = 1.000000000 000000000 000000001 000000002, digits=[1,0,0,1,2], expr_value=0, len_=5,
        //     decimal_length=4, valid_decimal_length=4
        const int64_t round = floating_scale - round_length * DIGIT_LEN;
        d_.len_ -= (decimal_length - (round_length + 1));
        uint32_t &digit_round = *(digits_ + d_.len_ - 1);
        const uint64_t round_pows = ROUND_POWS_DESC[round];
        const uint32_t residue = digit_round % round_pows;
        need_normalize = true;
        if (residue >= round_pows / 2) {
          digit_round += (round_pows - residue);
        } else {
          digit_round -= residue;
        }
      }
    } else if (floating_scale < 0) {
      floating_scale = 0 - floating_scale;
      const int64_t round_length = floating_scale / DIGIT_LEN;
      const int64_t round = floating_scale - round_length * DIGIT_LEN;
      int64_t round_integer_length = round_length + (0 == round ? 0 : 1);
      if (integer_length < round_integer_length) {
        set_zero();
      } else if (integer_length > valid_integer_length
                 && round_integer_length <= (integer_length - valid_integer_length)) {
        //e.g. 123,000000456, 000000000,000000000,
        //     digits=[123, 456], expr_value=3, len_=2, integer_length=4, valid_integer_length=2
        //round(value, -8),round(value, -18),
        //do nothing
      } else {
        need_normalize = true;
        d_.len_ = integer_length - round_integer_length + 1;
        uint32_t &digit_round = *(digits_ + d_.len_ - 1);
        const uint64_t round_pows = ROUND_POWS_ASC[round];
        uint32_t residue = digit_round % round_pows;
        if (residue >= round_pows / 2) {
          digit_round += (round_pows - residue);
        } else {
          digit_round -= residue;
        }
      }
    } else {
      if (0 == decimal_length) {
        //do nothing
      } else if (valid_integer_length > 0) {
        need_normalize = true;
        d_.len_ = valid_integer_length;
        uint32_t &digit_round = *(digits_ + valid_integer_length);
        if (digit_round >= (BASE / 2)) {
          digits_[d_.len_ - 1] += 1;
        }
      } else {
        uint32_t &digit_round = *(digits_ + 0);
        if (digit_round >= (BASE / 2) && get_decode_exp(d_) == -1) {
          set_one();
        } else {
          set_zero();
        }
      }
    }

    if (need_normalize) {
      if (OB_FAIL(normalize_v2_(true, true))) {
        LOG_ERROR("fail to normalize_v2_", KPC(this), K(ret));
      }
    }
  }
  LOG_DEBUG("finish round_scale_v2_", KPC(this));
  return ret;
}


int ObNumber::round_scale_v3_(const int64_t scale, const bool using_floating_scale,
    const bool for_oracle_to_char,
    int16_t *res_precision/*NULL*/, int16_t *res_scale/*NULL*/)
{
  static const uint64_t ROUND_POWS[] = {
      1000000000,
      100000000,
      10000000,
      1000000,
      100000,
      10000,
      1000,
      100,
      10,
      1000000000,
  };

  int ret = OB_SUCCESS;
  if (is_zero()) {
    //do nothing
  } else {
    LOG_DEBUG("before round_scale_v3_", KPC(this), K(scale), K(using_floating_scale), K(for_oracle_to_char));
    const int64_t digit_0_len = get_digit_len_v2(digits_[0]);
    const int64_t expr_value = get_decode_exp(d_);
    //xxx_length means xx digit array length
    const int64_t integer_length = (expr_value >= 0 ? (expr_value + 1) : 0);
    //e.g. 1,000000000,000000000,   digits=[1], expr_value=2, len_=1, integer_length=3, valid_integer_length=1
    const int64_t valid_integer_length = (d_.len_ > integer_length ? integer_length : d_.len_);
    //xxx_count means xx digit count
    const int64_t integer_count = (valid_integer_length > 0 ? (digit_0_len + (integer_length - 1) * DIGIT_LEN) : 0);

    //len_ > integer_length means have decimal
    const int64_t valid_decimal_length = (d_.len_ > integer_length ? (d_.len_ - integer_length) : 0);
    //e.g. 0.000000000 000000001, digits=[1], expr_value=-2, len_=1, decimal_length=2, valid_decimal_length=1
    const int64_t decimal_length = valid_decimal_length + ((0 == integer_length && !using_floating_scale) ? (0 - expr_value - 1) : 0);

    int64_t floating_scale = scale;

    if (is_oracle_mode()) {
      const int64_t decimal_prefix_zero_count = ((0 == integer_length) ? (0 - expr_value - 1 + DIGIT_LEN - digit_0_len) : 0);
      int64_t valid_precision = 0;
      if (for_oracle_to_char) {
        // Todo: 当number小数部分包含前缀0时(e.g:0.00012345)，oracle to_char 转换有效数字需包含前缀0，
        // e.g:
        //   select cast(0.00012345678901234567890123456789012345678901111 as varchar(100)) from dual;
        //   Oracle result: .000123456789012345678901234567890123457
        // oracle在to_char计算时，长度大于40的number会进行科学计数法转换，前缀0会去除。OB暂时没有兼容Oracle行为(科学计数法与精度联动），
        // 因此在number to char转换出现前缀0时（decimal_prefix_zero_count > 0），保持与修改前一致
        valid_precision = OB_MAX_NUMBER_PRECISION_INNER - is_negative()
                           - (has_decimal() ? (decimal_prefix_zero_count > 0 ? 2 : 1) : 0);
      } else {
        valid_precision = (OB_MAX_NUMBER_PRECISION_INNER - ((0 == integer_count ? decimal_prefix_zero_count : integer_count) % 2));
      }
      int64_t r_precision = 0;
      int64_t r_scale = 0;
      int64_t decimal_non_zero_count = 0;
      int64_t tail_decimal_zero_count = 0;
      if (valid_decimal_length > 0) {
        remove_back_zero(digits_[d_.len_ - 1], tail_decimal_zero_count);
        decimal_non_zero_count = (DIGIT_LEN * valid_decimal_length
            - (0 == integer_length ? (DIGIT_LEN - digit_0_len) : 0)
            - tail_decimal_zero_count);
      }
      const int64_t decimal_count = decimal_length * DIGIT_LEN - tail_decimal_zero_count;

      if (using_floating_scale) {
        if (integer_count > 0) {
          floating_scale = valid_precision - integer_count;
        } else if (decimal_non_zero_count > valid_precision) {
          /* decimal_cnt - non-zero decimal digit + valid decimal number */
          floating_scale = MIN(scale, (decimal_count - decimal_non_zero_count + valid_precision));
        }
        r_precision = PRECISION_UNKNOWN_YET;
        r_scale = ORA_NUMBER_SCALE_UNKNOWN_YET;
      } else {
        if (integer_count > 0) {
          if (scale > 0) {
            /* 1..30th.123  scale = 30 floating_scale = 0  res_p = 30 res_s = 0
             * 1..20th.123  scale = 10 floating_scale = 10 res_p = 20
             * 1...42th.123 scale = 10 floating_scale = -2 res_s = 10 res_p = 40, res_s = -2
             * 1.000..20th0123  scale = 22 floating_scale = 22 res_s = 22 res_p = 22*/
            floating_scale = MIN(valid_precision - integer_count, scale);
            r_precision = MIN(valid_precision, integer_count + decimal_count);
            r_scale = floating_scale < 0 ? floating_scale : MIN(floating_scale, decimal_count);
          } else {
            /* 1234.345 scale = -2, res_p = 2 res_s = -2
             * 1234.324 scale = -10 res_p = 0 res_s = -10
             * 1...42th.123 scale = -1 res_p = 40, res_s = -2*/
            r_precision = MIN(valid_precision, MAX(integer_count + scale, 0));
            r_scale = MIN(integer_count - valid_precision, scale);
          }
        } else if (decimal_non_zero_count > valid_precision) {
          /* total_length - non-zero decimal digit + valid decimal number */
          floating_scale = MIN(scale, (decimal_count - decimal_non_zero_count + valid_precision));
          /* 0.345 scale = -2, res_p = 2 res_s = -2
           * 1234.324 scale = -10 res_p = 0 res_s = -10
           * 1...42th.123 scale = -1 res_p = 40, res_s = -2*/
          if (scale > 0) {
            r_precision = MIN(valid_precision, decimal_non_zero_count - (scale - decimal_count));
          } else {
            r_precision = 0;
          }
          r_scale = floating_scale;
        }
      }

      if (NULL != res_precision && NULL != res_scale) {
        if (OB_LIKELY(using_floating_scale
                      || (r_precision <= valid_precision
                          && r_precision >= 0
                          && r_scale <= OB_MAX_NUMBER_SCALE
                          && r_scale >= OB_MIN_NUMBER_SCALE))) {
          *res_precision = static_cast<int16_t>(r_precision);
          *res_scale = static_cast<int16_t>(r_scale);
        } else {
          LOG_WARN("got invalid precision or scale on oracle mode, use builded",
              K(r_precision), K(r_scale));
        }
      }
//      LOG_DEBUG("Number before rebuild v2", K(is_oracle_mode()), K(using_floating_scale), K(scale),
//                K(floating_scale), K(expr_value), K(integer_length), K(valid_integer_length), K(integer_count),
//                K(decimal_length), K(valid_decimal_length), K(decimal_count), K(decimal_prefix_zero_count),
//                K(decimal_non_zero_count), K(r_precision), K(r_scale), KPC(this), K(lbt()));
    } else {
      if (using_floating_scale && valid_integer_length > 0) {
        floating_scale -= integer_count;
      }
//      LOG_DEBUG("Number before rebuild v3", K(is_oracle_mode()), K(using_floating_scale), K(scale),
//                K(floating_scale), K(expr_value), K(integer_length), K(valid_integer_length),
//                K(decimal_length), K(valid_decimal_length), KPC(this));
    }

    int32_t digit_id = OB_INVALID_INDEX;
    int32_t pow_id = OB_INVALID_INDEX;
    if (floating_scale > 0) {
      //round decimal
      const int64_t round_length = floating_scale / DIGIT_LEN;
      if (round_length >= decimal_length) {
        // do nothing
//        LOG_DEBUG("with no round", K(decimal_length), K(round_length), K(valid_decimal_length));
      } else if (decimal_length > valid_decimal_length
                 && round_length < (decimal_length - valid_decimal_length)) {
        //e.g. value = 0.000000000 000000000 000000001 000000002, digits=[1,2], expr_value=-3, len_=2,
        //     decimal_length=4, valid_decimal_length=2
        //round(value, 8), round(value, 9), round(value, 17)
        set_zero();
      } else {
        //e.g. value = 1.000000000 000000000 000000001 000000002, digits=[1,0,0,1,2], expr_value=0, len_=5,
        //     decimal_length=4, valid_decimal_length=4
        const int64_t round = floating_scale - round_length * DIGIT_LEN;
        d_.len_ -= (decimal_length - (round_length + 1));
        digit_id = d_.len_ - 1;
        pow_id = round;
      }
    } else if (floating_scale < 0) {
      floating_scale = 0 - floating_scale;
      const int64_t round_length = floating_scale / DIGIT_LEN;
      const int64_t round = floating_scale - round_length * DIGIT_LEN;
      int64_t round_integer_length = round_length + (0 == round ? 0 : 1);
      if (integer_length < round_integer_length) {
        set_zero();
      } else if (integer_length > valid_integer_length
                 && round_integer_length <= (integer_length - valid_integer_length)) {
        //e.g. 123,000000456, 000000000,000000000,
        //     digits=[123, 456], expr_value=3, len_=2, integer_length=4, valid_integer_length=2
        //round(value, -8),round(value, -18),
        //do nothing
      } else {
        d_.len_ = integer_length - round_integer_length + 1;
        digit_id = d_.len_ - 1;
        pow_id = 9 - round;
      }
    } else {
      if (0 == decimal_length) {
        //do nothing
      } else if (valid_integer_length > 0) {
        d_.len_ = valid_integer_length;
        digit_id = valid_integer_length;
        pow_id = 0;
      } else {
        uint32_t &digit_round = *(digits_ + 0);
        if (digit_round >= (BASE / 2) && get_decode_exp(d_) == -1) {
          set_one();
        } else {
          set_zero();
        }
      }
    }

    if (digit_id != OB_INVALID_INDEX && pow_id != OB_INVALID_INDEX) {
      uint32_t &digit_value = *(digits_ + digit_id);
      const uint64_t tmp_round_pows = ROUND_POWS[pow_id];
      const uint32_t residue = digit_value % tmp_round_pows;
      bool need_normalize = false;
      if (residue >= tmp_round_pows / 2) {
        digit_value += (ROUND_POWS[pow_id] - residue);
        need_normalize = (digit_value == BASE);
      } else {
        digit_value -= residue;
        if (digit_value == 0) {
          uint32_t *tmp_digit = digits_ + d_.len_ - 1;
          while (tmp_digit >= digits_ && 0 == *tmp_digit) {
            --tmp_digit;
          }
          if (tmp_digit < digits_) {
            set_zero();
          } else {
            d_.len_ = tmp_digit - digits_ + 1;
          }
        }
      }

      if (need_normalize) {
        --digit_id;
        while (digit_id >= 0) {
          uint32_t &tmp_digit = digits_[digit_id];
          if (tmp_digit == MAX_VALUED_DIGIT) {
            --digit_id;
          } else {
            ++tmp_digit;
            break;
          }
        }
        if (digit_id < 0) {
          digits_[0] = 1;
          d_.len_ = 1;
          // check exp overflow ... ?
          if (NEGATIVE == d_.sign_) {
            --d_.exp_;
          } else {
            ++d_.exp_;
          }
        } else {
          d_.len_ = digit_id + 1;
        }
      }
    }
  }
  LOG_DEBUG("finish round_scale_v3_", KPC(this), K(scale), K(using_floating_scale), K(for_oracle_to_char));
  return ret;
}

int ObNumber::round_precision(const int64_t precision)
{
  static const uint64_t ROUND_POWS[] = {
      1000000000,
      100000000,
      10000000,
      1000000,
      100000,
      10000,
      1000,
      100,
      10,
      1000000000,
  };

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(precision < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("precision is neg, it should not happened", KPC(this), K(precision));
  } else if (is_zero()) {
    //do nothing
  } else {
    const int64_t digit_0_len = get_digit_len_v2(digits_[0]);
    const int64_t expr_value = get_decode_exp(d_);
    //xxx_length means xx digit array length
    const int64_t integer_length = (expr_value >= 0 ? (expr_value + 1) : 0);
    //e.g. 1,000000000,000000000,   digits=[1], expr_value=2, len_=1, integer_length=3, valid_integer_length=1
    const int64_t valid_integer_length = (d_.len_ > integer_length ? integer_length : d_.len_);
    //xxx_count means xx digit count
    const int64_t integer_count = (valid_integer_length > 0 ? (digit_0_len + (integer_length - 1) * DIGIT_LEN) : 0);

    //len_ > integer_length means have decimal
    const int64_t valid_decimal_length = (d_.len_ > integer_length ? (d_.len_ - integer_length) : 0);
    //e.g. 0.000000000 000000001, digits=[1], expr_value=-2, len_=1, decimal_length=2, valid_decimal_length=1
    const int64_t decimal_length = valid_decimal_length + ((0 == integer_length) ? (0 - expr_value - 1) : 0);

    const int64_t decimal_prefix_zero_count = ((0 == integer_length) ? (0 - expr_value - 1 + DIGIT_LEN - digit_0_len) : 0);
    int64_t decimal_non_zero_count = 0;
    int64_t tail_decimal_zero_count = 0;
    if (valid_decimal_length > 0) {
      remove_back_zero(digits_[d_.len_ - 1], tail_decimal_zero_count);
      decimal_non_zero_count = (DIGIT_LEN * valid_decimal_length
          - (0 == integer_length ? (DIGIT_LEN - digit_0_len) : 0)
          - tail_decimal_zero_count);
    }
    const int64_t decimal_count = decimal_length * DIGIT_LEN - tail_decimal_zero_count;
    int64_t floating_scale = 0;
    if (integer_count > 0) {
      floating_scale = precision + 1 - integer_count;
    } else {
      floating_scale = (precision + 1) + (decimal_count - decimal_non_zero_count);
    }


    LOG_DEBUG("Number before round_precision", K(precision), K(floating_scale),
              K(expr_value), K(integer_length), K(valid_integer_length), K(integer_count),
              K(decimal_length), K(valid_decimal_length), K(decimal_count), K(decimal_prefix_zero_count),
              K(decimal_non_zero_count), KPC(this));

    int32_t digit_id = OB_INVALID_INDEX;
    int32_t pow_id = OB_INVALID_INDEX;
    if (floating_scale > 0) {
      //round decimal
      const int64_t round_length = floating_scale / DIGIT_LEN;
      if (round_length >= decimal_length) {
        // do nothing
//        LOG_DEBUG("with no round", K(decimal_length), K(round_length), K(valid_decimal_length));
      } else if (decimal_length > valid_decimal_length
                 && round_length < (decimal_length - valid_decimal_length)) {
        //e.g. value = 0.000000000 000000000 000000001 000000002, digits=[1,2], expr_value=-3, len_=2,
        //     decimal_length=4, valid_decimal_length=2
        //round(value, 8), round(value, 9), round(value, 17)
        set_zero();
      } else {
        //e.g. value = 1.000000000 000000000 000000001 000000002, digits=[1,0,0,1,2], expr_value=0, len_=5,
        //     decimal_length=4, valid_decimal_length=4
        const int64_t round = floating_scale - round_length * DIGIT_LEN;
        d_.len_ -= (decimal_length - (round_length + 1));
        digit_id = d_.len_ - 1;
        pow_id = round;
      }
    } else if (floating_scale < 0) {
      floating_scale = 0 - floating_scale;
      const int64_t round_length = floating_scale / DIGIT_LEN;
      const int64_t round = floating_scale - round_length * DIGIT_LEN;
      int64_t round_integer_length = round_length + (0 == round ? 0 : 1);
      if (integer_length < round_integer_length) {
        set_zero();
      } else if (integer_length > valid_integer_length
                 && round_integer_length <= (integer_length - valid_integer_length)) {
        //e.g. 123,000000456, 000000000,000000000,
        //     digits=[123, 456], expr_value=3, len_=2, integer_length=4, valid_integer_length=2
        //round(value, -8),round(value, -18),
        //do nothing
      } else {
        d_.len_ = integer_length - round_integer_length + 1;
        digit_id = d_.len_ - 1;
        pow_id = 9 - round;
      }
    } else {
      if (0 == decimal_length) {
        //do nothing
      } else if (valid_integer_length > 0) {
        d_.len_ = valid_integer_length;
        digit_id = valid_integer_length;
        pow_id = 0;
      } else {
        uint32_t &digit_round = *(digits_ + 0);
        if (digit_round >= (BASE / 2) && get_decode_exp(d_) == -1) {
          set_one();
        } else {
          set_zero();
        }
      }
    }

    if (digit_id != OB_INVALID_INDEX && pow_id != OB_INVALID_INDEX) {
      uint32_t &digit_value = *(digits_ + digit_id);
      const uint64_t tmp_round_pows = ROUND_POWS[pow_id];
      const uint32_t residue = digit_value % tmp_round_pows;
      bool need_normalize = false;
      if (residue >= tmp_round_pows / 2) {
        digit_value += (ROUND_POWS[pow_id] - residue);
        need_normalize = (digit_value == BASE);
      } else {
        digit_value -= residue;
        if (digit_value == 0) {
          uint32_t *tmp_digit = digits_ + d_.len_ - 1;
          while (tmp_digit >= digits_ && 0 == *tmp_digit) {
            --tmp_digit;
          }
          if (tmp_digit < digits_) {
            set_zero();
          } else {
            d_.len_ = tmp_digit - digits_ + 1;
          }
        }
      }

      if (need_normalize) {
        --digit_id;
        while (digit_id >= 0) {
          uint32_t &tmp_digit = digits_[digit_id];
          if (tmp_digit == MAX_VALUED_DIGIT) {
            --digit_id;
          } else {
            ++tmp_digit;
            break;
          }
        }
        if (digit_id < 0) {
          digits_[0] = 1;
          d_.len_ = 1;
          // check exp overflow ... ?
          if (NEGATIVE == d_.sign_) {
            --d_.exp_;
          } else {
            ++d_.exp_;
          }
        } else {
          d_.len_ = digit_id + 1;
        }
      }
    }
  }
  LOG_DEBUG("finish round_precision", KPC(this), K(precision));
  return ret;
}



int ObNumber::round_scale_oracle_(const int64_t scale, const bool using_floating_scale,
                                  int16_t *res_precision, int16_t *res_scale)
{
  int ret = OB_SUCCESS;
  ObDigitIterator di;
  di.assign(d_.desc_, digits_);

  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  uint32_t integer_digits[MAX_CALC_LEN];
  int64_t integer_length = 0;
  int64_t integer_counter = 0;
  bool integer_is_zero = true;
  uint32_t decimal_digits[MAX_CALC_LEN];
  int64_t decimal_length = 0;
  int64_t decimal_not_zero_length = 0;
  int64_t last_decimal_counter = 0;
  int64_t valid_precision = OB_MAX_NUMBER_PRECISION_INNER;
  int64_t r_precision = 0;
  int64_t r_scale = 0;
  int64_t decimal_prefix_zero = 0;
  while (OB_SUCC(di.get_next_digit(digit, from_integer, last_decimal))) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= integer_length
        || MAX_CALC_LEN <= decimal_length)) {
      LOG_WARN("buffer size overflow",
                K(integer_length), K(decimal_length));
      ret = OB_NUMERIC_OVERFLOW;
      break;
    }
    if (from_integer) {
      if (0 == integer_length) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }
      if (integer_is_zero && digit != 0) {
        integer_is_zero = false;
      }
      integer_digits[integer_length++] = digit;
    } else if (0 <= scale) {
      if (0 == digit
          && 0 == integer_length
          && 0 == decimal_not_zero_length) {
        decimal_prefix_zero += DIGIT_LEN;
        if (using_floating_scale) {
          continue;
        }
      } else {
        if (0 == decimal_not_zero_length && !last_decimal) {
          decimal_prefix_zero += DIGIT_LEN - get_digit_len(digit);
          decimal_not_zero_length += get_digit_len(digit);
        } else if (last_decimal) {
          remove_back_zero(digit, last_decimal_counter);
          decimal_not_zero_length += DIGIT_LEN - last_decimal_counter;
        } else {
          decimal_not_zero_length += DIGIT_LEN;
        }
      }
      decimal_digits[decimal_length++] = digit;
      if ((decimal_length * DIGIT_LEN) > (using_floating_scale ? (scale - integer_counter) : scale)) {
//        LOG_DEBUG("Number need trunc decimal on oralce mode", K(decimal_length),
//            K(scale), K(integer_counter), K(decimal_not_zero_length), K(using_floating_scale),
//            K(decimal_prefix_zero));
        break;
      }
    } else {
      break;
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  valid_precision -= (0 == integer_counter ? decimal_prefix_zero : integer_counter) % 2;
  int64_t decimal_cnt = decimal_length * DIGIT_LEN - last_decimal_counter;
  LOG_DEBUG("Number process on oracle mode", K(ret), K(using_floating_scale), K(scale),
      K(integer_counter), K(decimal_length), K(decimal_not_zero_length), K(valid_precision),
      K(decimal_cnt), K(last_decimal_counter));
  int64_t floating_scale = scale;
  if (OB_SUCC(ret)) {
    if (using_floating_scale) {
      if (integer_counter > 0) {
        floating_scale = valid_precision - integer_counter;
      } else if (decimal_not_zero_length > valid_precision) {
        /* decimal_cnt - non-zero decimal digit + valid decimal number */
        floating_scale = MIN(scale,
            (decimal_cnt - decimal_not_zero_length + valid_precision));
      }
      r_precision = PRECISION_UNKNOWN_YET;
      r_scale = ORA_NUMBER_SCALE_UNKNOWN_YET;
    } else {
      if (integer_counter > 0) {
        if (scale > 0) {
          /* 1..30th.123  scale = 30 floating_scale = 0  res_p = 30 res_s = 0
           * 1..20th.123  scale = 10 floating_scale = 10 res_p = 20
           * 1...42th.123 scale = 10 floating_scale = -2 res_s = 10 res_p = 40, res_s = -2
           * 1.000..20th0123  scale = 22 floating_scale = 22 res_s = 22 res_p = 22*/
          floating_scale = MIN(valid_precision - integer_counter, scale);
          r_precision = MIN(valid_precision, integer_counter + decimal_cnt);
          r_scale = floating_scale < 0 ? floating_scale : MIN(floating_scale, decimal_cnt);
        } else {
          /* 1234.345 scale = -2, res_p = 2 res_s = -2
           * 1234.324 scale = -10 res_p = 0 res_s = -10
           * 1...42th.123 scale = -1 res_p = 40, res_s = -2*/
          r_precision = MIN(valid_precision, MAX(integer_counter + scale, 0));
          r_scale = MIN(integer_counter - valid_precision, scale);
        }
      } else if (decimal_not_zero_length > valid_precision) {
        /* total_length - non-zero decimal digit + valid decimal number */
        floating_scale = MIN(scale, (decimal_cnt - decimal_not_zero_length + valid_precision));
        /* 0.345 scale = -2, res_p = 2 res_s = -2
         * 1234.324 scale = -10 res_p = 0 res_s = -10
         * 1...42th.123 scale = -1 res_p = 40, res_s = -2*/
        if (scale > 0) {
          r_precision = MIN(valid_precision, decimal_not_zero_length - (scale - decimal_cnt));
        } else {
          r_precision = 0;
        }
        r_scale = floating_scale;
      }
    }
    if (NULL != res_precision && NULL != res_scale) {
      if (using_floating_scale
          || (r_precision <= valid_precision
              && r_precision >= 0
              && r_scale <= OB_MAX_NUMBER_SCALE
              && r_scale >= OB_MIN_NUMBER_SCALE)) {
        *res_precision = static_cast<int16_t>(r_precision);
        *res_scale = static_cast<int16_t>(r_scale);
      } else {
        LOG_WARN("got invalid precision or scale on oracle mode, use builded",
            K(r_precision), K(r_scale));
      }
    }
//    LOG_DEBUG("Number round with:", K(using_floating_scale), K(floating_scale), K(decimal_length),
//        K(r_precision), K(r_scale));
    if (0 < floating_scale) {
      if (OB_FAIL(round_decimal_(floating_scale, decimal_digits, decimal_length))) {
        LOG_ERROR("fail to get round_decimal");
      } else {
        ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, decimal_length);
      }
    } else if (0 > floating_scale) {
      if (OB_FAIL(round_integer_(-floating_scale, integer_digits, integer_length))) {
        LOG_ERROR("fail to get round_integer");
      } else {
        ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
      }
    } else if (0 == floating_scale) {
      if (0 < decimal_length) {
        if ((5 * BASE / 10) <= decimal_digits[0]) {
          decimal_digits[0] = BASE;
        } else {
          decimal_digits[0] = 0;
        }
        ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, 1);
      } else {
        ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
      }
    }
  }
  return ret;
}

//if number=x.5  number.round_even_number() = nearest even number
int ObNumber::round_even_number()
{
  int ret = OB_SUCCESS;
  if (is_zero()) {
    //do nothing
  } else {
    const int64_t expr_value = get_decode_exp(d_);
    //xxx_length means xx digit array length
    const int64_t integer_length = (expr_value >= 0 ? (expr_value + 1) : 0);
    //e.g. 1,000000000,000000000,   digits=[1], expr_value=2, len_=1, integer_length=3, valid_integer_length=1
    const int64_t valid_integer_length = (d_.len_ > integer_length ? integer_length : d_.len_);
    //xxx_count means xx digit count
    //const int64_t integer_count = (valid_integer_length > 0 ? (get_digit_len_v2(digits_[0]) + (integer_length - 1) * DIGIT_LEN) : 0);

    //len_ > integer_length means have decimal
    const int64_t valid_decimal_length = (d_.len_ > integer_length ? (d_.len_ - integer_length) : 0);
    //e.g. 0.000000000 000000001, digits=[1], expr_value=-2, len_=1, decimal_length=2, valid_decimal_length=1
    const int64_t decimal_length = valid_decimal_length + ((0 == integer_length) ? (0 - expr_value - 1) : 0);

    bool need_normalize = false;
    if (0 == decimal_length) {
      //do nothing
    } else {
      if (valid_integer_length > 0) {
        need_normalize = true;
        d_.len_ = valid_integer_length;
        uint32_t &digit_round = *(digits_ + valid_integer_length);
        uint32_t &lowest_number_integer = *(digits_ + valid_integer_length - 1);
        if ((BASE / 2) < digit_round) {
          digits_[d_.len_ - 1] += 1;
        } else if ((BASE / 2) == digit_round) {
          if (1 < decimal_length || (1 == decimal_length && 1 == (lowest_number_integer % 2))) {
            digits_[d_.len_ - 1] += 1;
          }
        }
      } else {
        uint32_t &digit_round = *(digits_ + 0);
        if (get_decode_exp(d_) == -1 && ((digit_round > (BASE / 2)) || ((BASE / 2) == digit_round && 1 < decimal_length))){
          set_one();
        } else {
          set_zero();
        }
      }
    }
    if (need_normalize) {
      if (OB_FAIL(normalize_v3_(true, true))) {
        LOG_ERROR("fail to normalize_v3_", KPC(this), K(ret));
      }
    }
  }

  LOG_DEBUG("finish round_even_number_", KPC(this));
  return ret;
}

int ObNumber::round_integer_(
    const int64_t scale, // >= 1
    uint32_t *integer_digits,
    int64_t &integer_length)
{
  static const uint64_t ROUND_POWS[] = {0, 10, 100, 1000, 10000, 100000,
                                        1000000, 10000000, 100000000};
  int ret = OB_SUCCESS;
  if (OB_ISNULL(integer_digits) || OB_UNLIKELY(0 == DIGIT_LEN || scale < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null or the DIGIT_LEN IS ZERO or the scale < 1", K(ret));
  } else {
    int64_t round_length = scale / DIGIT_LEN;
    int64_t round = scale % DIGIT_LEN;
    LOG_DEBUG("round_integer_", K(round_length), K(round), K(integer_length));
    if (integer_length < round_length) {
      integer_length = 0;
    } else {
      if (0 == round) { // scale >= 9, round_length >= 1
        integer_length -= (round_length - 1);
        if ((5 * BASE / 10) <= integer_digits[integer_length - 1]) {
          integer_digits[integer_length - 1] = BASE;
        } else {
          integer_digits[integer_length - 1] = 0;
        }
      } else {
        integer_length -= round_length;
        uint64_t roundv = (uint64_t)integer_digits[integer_length - 1]
                          + 5 * ROUND_POWS[round] / 10;
        roundv /= ROUND_POWS[round];
        roundv *= ROUND_POWS[round];
        integer_digits[integer_length - 1] = (uint32_t)roundv;
      }
    }
  }
  return ret;
}

int ObNumber::round_decimal_(
    const int64_t scale, // >= 1
    uint32_t *decimal_digits,
    int64_t &decimal_length)
{
  static const uint64_t ROUND_POWS[] = {0, 100000000, 10000000, 1000000,
                                        100000, 10000, 1000, 100, 10};
  int ret = OB_SUCCESS;
  if (OB_ISNULL(decimal_digits) || OB_UNLIKELY(0 == DIGIT_LEN || scale < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null or the DIGIT_LEN IS ZERO or the scale < 1", K(ret));
  } else {
    int64_t round_length = scale / DIGIT_LEN;
    int64_t round = scale % DIGIT_LEN;

    if (decimal_length <= round_length) {
      // do nothing
      LOG_DEBUG("with no round", K(decimal_length), K(round_length));
    } else {
      decimal_length = std::min(decimal_length, round_length + 1);
      if (0 == round) { // scale >= 9, decimal_length >= 2
        if ((5 * BASE / 10) <= decimal_digits[decimal_length - 1]) {
          decimal_digits[decimal_length - 1] = BASE;
        } else {
          decimal_digits[decimal_length - 1] = 0;
        }
      } else {
        uint64_t roundv = (uint64_t)decimal_digits[decimal_length - 1] + 5 * ROUND_POWS[round] / 10;
        roundv /= ROUND_POWS[round];
        roundv *= ROUND_POWS[round];
        decimal_digits[decimal_length - 1] = (uint32_t)roundv;
      }
    }
  }

  return ret;
}

int ObNumber::trunc_scale_(int64_t scale, bool using_floating_scale)
{
  int ret = OB_SUCCESS;
  ObDigitIterator di;
  di.assign(d_.desc_, digits_);
  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  uint32_t integer_digits[MAX_CALC_LEN];
  int64_t integer_length = 0;
  int64_t integer_counter = 0;
  uint32_t decimal_digits[MAX_CALC_LEN];
  int64_t decimal_length = 0;
  while (OB_SUCC(di.get_next_digit(digit, from_integer, last_decimal))) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= integer_length
        || MAX_CALC_LEN <= decimal_length)) {
      LOG_WARN("buffer size overflow", K(integer_length), K(decimal_length));
      ret = OB_NUMERIC_OVERFLOW;
      break;
    }
    if (from_integer) {
      if (0 == integer_length) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }
      integer_digits[integer_length ++] = digit;
    } else if (0 <= scale) {
      if (using_floating_scale
          && 0 == digit
          && 0 == integer_length
          && 0 == decimal_length) {
        continue;
      }
      decimal_digits[decimal_length ++] = digit;
      if ((decimal_length * DIGIT_LEN) > scale) {
        break;
      }
    } else {
      break;
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  int64_t floating_scale = scale;
  if (OB_SUCCESS == ret
      && using_floating_scale) {
    floating_scale -= integer_counter;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 < floating_scale) {
    if (OB_FAIL(trunc_decimal_(floating_scale, decimal_digits, decimal_length))) {
      LOG_ERROR("fail to get trunc decimal", K(ret));
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, decimal_length);
    }
  } else if (0 > floating_scale) {
    // todo ouxing
    if (OB_FAIL(trunc_integer_(-floating_scale, integer_digits, integer_length))) {
      LOG_ERROR("fail to get trunc integer", K(ret));
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
    }

  } else if (0 == floating_scale) {
    // todo ouxing
    ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
  } else { /* Do nothing */ }
  return ret;
}

int ObNumber::trunc_integer_(
    const int64_t scale,
    uint32_t *integer_digits,
    int64_t &integer_length)
{
  int ret = OB_SUCCESS;
  static const uint64_t TRUNC_POWS[] = {0, 10, 100, 1000, 10000, 100000,
                                        1000000, 10000000, 100000000};
  if (OB_UNLIKELY(0 == DIGIT_LEN || scale <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "DIGIT_LEN is zero or scale is not positive", K(ret), K(scale));
  } else if (integer_length > 0) {
    //tips: in terms of "3333123456789555555555.123"
    //integer_length = 3 and integer_digits[0]=3333, integer_digits[1] = 123456789 integer_digits[2] = 555555555
    int64_t trunc_length = (scale - 1) / DIGIT_LEN + 1;
    int64_t trunc = scale % DIGIT_LEN;
    if (integer_length < trunc_length) {
      integer_length = 0;
    } else {
      if (0 == trunc) { //such as "3333123456789555555555.123" and scale = 9
        integer_length -= trunc_length;
      } else { //such as "3333123456789555555555.123" and scale = 3, change 555555555 to 555555000
        //integer_length >= trunc_length and trunc_length > 0 , so it holds that
        //integer_length - trunc_length + 1 - 1 = integer_length - trunc_length >= 0 and
        //integer_length - trunc_length + 1 - 1 = integer_length - trunc_length <  integer_length
        //we need to check the bound here and can use integer_digits[integer_length - 1] safety
        integer_length -= (trunc_length - 1);
        uint64 truncdv = static_cast<uint64_t>(integer_digits[integer_length - 1]);
        truncdv /= TRUNC_POWS[trunc];
        truncdv *= TRUNC_POWS[trunc];
        integer_digits[integer_length - 1] = static_cast<uint32_t>(truncdv);
      }
    }
  }
  return ret;
}

int ObNumber::trunc_decimal_(
    const int64_t scale,
    uint32_t *decimal_digits,
    int64_t &decimal_length)
{
  int ret = OB_SUCCESS;
  static const uint64_t TRUNC_POWS[] = {0, 100000000, 10000000, 1000000,
                                        100000, 10000, 1000, 100, 10};
  if (OB_UNLIKELY(0 == DIGIT_LEN || scale <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the DIGIT_LEN is zero or scale is not positive", K(ret), K(scale));
  } else if (decimal_length > 0) {
    int64_t trunc_length = (scale - 1) / DIGIT_LEN + 1;
    int64_t trunc = scale % DIGIT_LEN;
    if (decimal_length < trunc_length) {
      // do nothing
    } else {
      decimal_length = trunc_length;
      if (0 == trunc) {
        //such as : "1.123456789123456789" when scale = 9 trunc will be 0
        //decimal_length will be changed from 2 to 1 (trunc_length).
        //and, we do nothing here.
      } else {
        uint64 truncdv = static_cast<uint64_t>(decimal_digits[decimal_length - 1]);
        truncdv /= TRUNC_POWS[trunc];
        truncdv *= TRUNC_POWS[trunc];
        decimal_digits[decimal_length - 1] = static_cast<uint32_t>(truncdv);
      }
    }
  }
  return ret;
}

// handle decoded digits without tail flag
int ObNumber::normalize_(uint32_t *digits, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null", K(ret));
  } else {
    int64_t tmp_length = length;
    int64_t start = 0;
    for (int64_t i = 0; i < tmp_length; ++i) {
      if (0 != digits[i]) {
        break;
      }
      ++start;
    }

    uint32_t carry = 0;
    for (int64_t i = tmp_length - 1; i >= start; --i) {
      digits[i] += carry;
      if (BASE <= digits[i]) {
        digits[i] -= (uint32_t)BASE;
        carry = 1;
      } else {
        carry = 0;
      }
    }
    // carry may generate extra zero, so reduce zero on tail, must after handle carry
    int64_t end = tmp_length - 1;
    for (int64_t i = tmp_length - 1; i >= start; --i) {
      if (0 != digits[i]
          && BASE != digits[i]) {
        break;
      }
      --end;
    }
    tmp_length = end - start + 1;
    tmp_length = (0 < tmp_length) ? tmp_length : 0;

    if (0 < carry) {
      if (OB_UNLIKELY(0 != tmp_length)) {
        LOG_WARN("unexpected length", K(tmp_length));
        ret = OB_ERR_UNEXPECTED;
      } else {
        digits_[0] = carry;
        d_.len_ = 1;
        if (NEGATIVE == d_.sign_) {
          --d_.exp_;
        } else {
          ++d_.exp_;
        }
      }
    } else {
      if (digits_ != &digits[start]) {
        memmove(digits_, &digits[start], tmp_length * ITEM_SIZE(digits_));
      }
      d_.len_ = (uint8_t)tmp_length;
      if (0 == d_.len_) {
        set_zero();
      }
    }
  }

  return ret;
}

// handle decoded digits without tail flag
int ObNumber::normalize_v2_(const bool from_calc/*true*/, const bool only_normalize_tailer /*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null", K(ret));
  } else {
    int64_t tmp_length = d_.len_;
    int64_t start = 0;
    if (!only_normalize_tailer) {
      for (int64_t i = 0; i < tmp_length; ++i) {
        if (0 != digits_[i]) {
          break;
        }
        ++start;
      }
    }

    uint32_t carry = 0;
    int64_t end = tmp_length - 1;
    if (from_calc) {
      for (int64_t i = tmp_length - 1; i >= start; --i) {
        digits_[i] += carry;
        if (digits_[i] >= BASE) {
          digits_[i] -= (uint32_t)BASE;
          carry = 1;
        } else {
          carry = 0;
        }
      }

      // carry may generate extra zero, so reduce zero on tail, must after handle carry
      for (int64_t i = tmp_length - 1; i >= start; --i) {
        if (0 != digits_[i]) {
          break;
        }
        --end;
      }
    }
    tmp_length = end - start + 1;
    if (tmp_length < 0) {
      tmp_length = 0;
    }

    if (0 < carry) {
      if (OB_UNLIKELY(0 != tmp_length)) {
        LOG_WARN("unexpected length", K(tmp_length));
        ret = OB_ERR_UNEXPECTED;
      } else {
        digits_[0] = carry;
        d_.len_ = 1;
        if (NEGATIVE == d_.sign_) {
          --d_.exp_;
        } else {
          ++d_.exp_;
        }
      }
    } else {
      if (digits_ != digits_ + start) {
        memmove(digits_, digits_ + start, tmp_length * ITEM_SIZE(digits_));
      }
      d_.len_ = (uint8_t)tmp_length;
      if (0 == d_.len_) {
        set_zero();
      }
    }
  }

  return ret;
}

// handle decoded digits without tail flag
int ObNumber::normalize_v3_(const bool from_calc/*true*/, const bool only_normalize_tailer /*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null", K(ret));
  } else {
    int64_t tmp_length = d_.len_;
    int64_t start = 0;
    if (!only_normalize_tailer) {
      for (int64_t i = 0; i < tmp_length && 0 == digits_[i]; ++i) {
        ++start;
      }
    }

    uint32_t carry = 0;
    int64_t end = tmp_length - 1;
    if (from_calc) {
      for (int64_t i = end; i >= start; --i) {
        uint32_t &tmp_digit = digits_[i];
        tmp_digit += carry;
        if (BASE == tmp_digit) {
          tmp_digit = 0;
          --end;
          carry = 1;
        } else if (0 == tmp_digit) {
          --end;
          carry = 0;
        } else {
          carry = 0;
          break;
        }
      }
    }
    tmp_length = end - start + 1;
    if (tmp_length < 0) {
      tmp_length = 0;
    }

    if (carry > 0) {
      if (OB_UNLIKELY(0 != tmp_length)) {
        LOG_WARN("unexpected length", K(tmp_length));
        ret = OB_ERR_UNEXPECTED;
      } else {
        digits_[0] = carry;
        d_.len_ = 1;
        if (NEGATIVE == d_.sign_) {
          --d_.exp_;
        } else {
          ++d_.exp_;
        }
      }
    } else {
      if (0 == tmp_length) {
        set_zero();
      } else {
        if (digits_ != digits_ + start) {
          memmove(digits_, digits_ + start, tmp_length * ITEM_SIZE(digits_));
        }
        d_.len_ = (uint8_t)tmp_length;
      }
    }
  }
  return ret;
}

int ObNumber::rebuild_digits_(
    const uint32_t *integer_digits,
    const int64_t integer_length,
    const uint32_t *decimal_digits,
    const int64_t decimal_length)
{
  int ret = OB_SUCCESS;
  uint32_t digits[MAX_CALC_LEN] = {};
  int64_t length = 0;
  if (NULL != integer_digits
      && 0 < integer_length) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= (length + integer_length))) {
      _OB_LOG(WARN, "buffer size overflow, cap=%ld length=%ld integer_length=%ld",
                MAX_CALC_LEN, length, integer_length);
      ret = OB_NUMERIC_OVERFLOW;
    } else {
      MEMCPY(digits, integer_digits, integer_length * ITEM_SIZE(digits_));
      length = integer_length;
    }
  }
  if (OB_SUCCESS == ret
      && NULL != decimal_digits
      && 0 < decimal_length) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= (length + decimal_length))) {
      _OB_LOG(WARN, "buffer size overflow, cap=%ld length=%ld decimal_length=%ld",
                MAX_CALC_LEN, length, decimal_length);
      ret = OB_NUMERIC_OVERFLOW;
    } else {
      MEMCPY(&digits[length], decimal_digits, decimal_length * ITEM_SIZE(digits));
      length += decimal_length;
    }
  }
  if (OB_SUCC(ret)) {
    ret = normalize_(digits, length);
  }
  OB_LOG(DEBUG, "succ to rebuild_digits_", K(integer_length), K(decimal_length), K(length), KPC(this));
  return ret;
}

const char *ObNumber::format() const
{
  // TODO: To Be Removed. Use stack local instead.
  static const int64_t BUFFER_NUM = 64;
  static const int64_t BUFFER_SIZE = MAX_PRINTABLE_SIZE;
  char *buffers = (char*)GET_TSI_MULT(ByteBuf<BUFFER_NUM * BUFFER_SIZE>, 3);
  RLOCAL(uint64_t, i);
  char *buffer = &buffers[0] + (i++ % BUFFER_NUM) * BUFFER_SIZE;
  int64_t length = 0;
  if (OB_ISNULL(buffers)) {
    buffer = nullptr;
  } else if(OB_UNLIKELY(OB_SUCCESS != format(buffer, BUFFER_SIZE, length, -1))) {
    LOG_ERROR_RET(OB_ERROR, "fail to format buffer");
  } else {
    buffer[length] = '\0';
  }
  return buffer;
}

int ObNumber::format_v1(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const
{
  int ret = OB_SUCCESS;
  uint32_t digits[MAX_STORE_LEN] = {0};
  bool prev_from_integer = true;
  int64_t pad_zero_count = scale;
  if (is_oracle_mode()) {
    pad_zero_count = 0;
  }
  ObNumber buf_nmb;
  const ObNumber *nmb = this;
  if (scale >= 0) {
    if (OB_FAIL(buf_nmb.from_(*this, digits))) {
    } else if (OB_FAIL(buf_nmb.round(scale))) {
    } else {
      nmb = &buf_nmb;
    }
  }
  if (OB_SUCC(ret) && 0 == nmb->d_.len_) {
    ret = databuff_printf(buf, buf_len, pos, "0");
  } else {
    if (OB_SUCC(ret)) {
      if (nmb->is_negative()) {
        ret = databuff_printf(buf, buf_len, pos, "-");
      }
      // oracle模式下小数的整数部分不补0，0.2345用.2345表示，-0.2345用-.2345表示
      if (OB_SUCCESS == ret && nmb->is_decimal() && !is_oracle_mode()) {
        ret = databuff_printf(buf, buf_len, pos, "0");
      }
      if (OB_SUCC(ret)) {
        ObDigitIterator di;
        di.assign(nmb->get_desc_value(), nmb->get_digits());
        uint32_t digit = 0;
        bool head_flag = true;
        bool from_integer = false;
        bool last_decimal = false;
        int tmp_ret = OB_SUCCESS;//used for iteration of get_next_digit
        while (OB_SUCCESS == ret
               && OB_SUCCESS == (tmp_ret = di.get_next_digit(digit, from_integer, last_decimal))) {
          if (prev_from_integer && !from_integer) {
            // dot
            ret = databuff_printf(buf, buf_len, pos, ".");
          }
          if (OB_SUCC(ret)) {
            if (!from_integer && !last_decimal) {
              pad_zero_count -= DIGIT_LEN;
            }
            if (head_flag && from_integer) {
              // first integer
              ret = databuff_printf(buf, buf_len, pos, "%u", digit);
            } else if (last_decimal) {
              // last decimal
              int64_t counter = 0;
              uint32_t tmp = remove_back_zero(digit, counter);
              ret = databuff_printf(buf, buf_len, pos, ObNumber::BACK_DIGIT_FORMAT[counter], tmp);
              pad_zero_count -= (DIGIT_LEN - counter);
            } else {
              // normal digit
              ret = databuff_printf(buf, buf_len, pos, DIGIT_FORMAT, digit);
            }
          }

          if (OB_SUCC(ret)) {
            prev_from_integer = from_integer;
            head_flag = false;
          }
        }//end while. end iteration
        if (OB_SUCC(ret)) {
          ret = (OB_ITER_END == tmp_ret) ? OB_SUCCESS : tmp_ret;
        }
      }
    }
  }
  // pad zero.
  if (OB_SUCCESS == ret && pad_zero_count > 0) {
    if (prev_from_integer) {
      ret = databuff_printf(buf, buf_len, pos, ".");
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(pad_zero_count > FLOATING_SCALE)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("the param is invalid", K(ret), K(pad_zero_count));
      } else {
        char zeros[FLOATING_SCALE + 1] = "000000000000000000000000000000000000000000000000000000000000000000000000";
        zeros[pad_zero_count] = 0;
        ret = databuff_printf(buf, buf_len, pos, "%s", zeros);
      }
    }
  }
  return ret;
}

int ObNumber::format_int64(char *buf, int64_t &pos, const int16_t scale, bool &is_finish) const
{
  int ret = OB_SUCCESS;
  is_finish = true;
  const int64_t orig_pos = pos;
  if (is_zero()) {
    buf[pos++] = '0';
  } else {
    if (is_negative()) {
      buf[pos++] = '-';
    }
    const int32_t expr_value = get_decode_exp(d_);
    if (!is_integer(expr_value)) {
      is_finish = false;
    } else if (is_int32(expr_value)) {
      const int64_t format_length = ObFastFormatInt::format_unsigned(digits_[0], buf + pos);
      if (OB_UNLIKELY(format_length > DIGIT_LEN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("digits is unexpected", "digit", digits_[0], KPC(this), K(ret));
        //print number will not report error ret, we need make use find it
        MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
      } else {
        pos += format_length;
      }
    } else if (is_int64_without_expr(expr_value)) {
      const int64_t format_length = ObFastFormatInt::format_unsigned(digits_[0], buf + pos);
      if (OB_UNLIKELY(format_length > DIGIT_LEN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("digits is unexpected", "digit", digits_[0], KPC(this), K(ret));
        //print number will not report error ret, we need make use find it
        MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
      } else {
        pos += format_length;
        ObFastFormatInt ffi(digits_[1]);
        if (OB_UNLIKELY(ffi.length() > DIGIT_LEN)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("digits is unexpected", "digit", digits_[1], KPC(this), K(ret));
          //print number will not report error ret, we need make use find it
          MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
        } else {
          MEMSET(buf + pos, '0', DIGIT_LEN - ffi.length());
          MEMCPY(buf + pos + DIGIT_LEN - ffi.length(), ffi.ptr(), ffi.length());
          pos += DIGIT_LEN;
        }
      }
    } else if (is_int64_with_expr(expr_value)) {
      const int64_t format_length = ObFastFormatInt::format_unsigned(digits_[0], buf + pos);
      if (OB_UNLIKELY(format_length > DIGIT_LEN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("digits is unexpected", "digit", digits_[0], KPC(this), K(ret));
        //print number will not report error ret, we need make use find it
        MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
      } else {
        pos += format_length;
        MEMSET(buf + pos, '0', DIGIT_LEN);
        pos += DIGIT_LEN;
      }
    } else {
      is_finish = false;
    }
  }

  if (OB_SUCC(ret) && is_finish) {
    // pad zero.
    if (!is_oracle_mode() && scale > 0) {
      buf[pos++] = '.';
      if (OB_UNLIKELY(scale > FLOATING_SCALE)) {
        is_finish = false;
        pos = orig_pos;
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scale is to large", K(scale), K(ret));
      } else {
        MEMCPY(buf + pos, FLOATING_ZEROS, scale);
        pos += scale;
        buf[pos] = '\0';
      }
    } else {
      buf[pos] = '\0';
    }
    ObString tmp_str(pos - orig_pos, buf);
    LOG_DEBUG("finish format int64", KPC(this), K(scale), K(is_oracle_mode()), K(tmp_str));
  } else {
    pos = orig_pos;
  }
  return ret;
}

int ObNumber::format_v2(
              char *buf,
              const int64_t buf_len,
              int64_t &pos,
              int16_t scale,
              const bool need_to_sci/*false*/) const
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  const int64_t orig_pos = pos;
  const int64_t max_need_size = get_max_format_length() + ((!is_oracle_mode() && scale > 0) ? scale : 0);
  if (OB_ISNULL(buf) || OB_UNLIKELY(max_need_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", KP(buf), K(max_need_size), K(ret));
  } else if (OB_UNLIKELY((buf_len - pos) < max_need_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_TRACE("size is overflow", K(buf_len), K(pos), K(max_need_size),
                 KPC(this), K(scale), K(ret));
  } else if (OB_FAIL(format_int64(buf, pos, scale, is_finish))) {
    LOG_ERROR("format_int64 failed", KPC(this), K(ret));
  } else if (is_finish) {
    //fast path succ
  } else {
    pos = orig_pos;
    bool prev_from_integer = true;
    int64_t pad_zero_count = (is_oracle_mode() ? 0 : scale);
    ObNumber buf_nmb;
    uint32_t digits[MAX_STORE_LEN] = {0};
    const ObNumber *nmb = this;
    if (!is_integer(get_decode_exp(d_)) && scale >= 0) {
      if (OB_FAIL(buf_nmb.from_(*this, digits))) {
      } else if (OB_FAIL(buf_nmb.round(scale))) {
      } else if (OB_UNLIKELY(buf_len - pos < buf_nmb.get_max_format_length())) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("size is overflow", "buf_size", buf_len - pos,
                 "max_need_size", buf_nmb.get_max_format_length(), K(scale),
                 K(buf_nmb), K(ret));
      } else {
        nmb = &buf_nmb;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (nmb->is_zero()) {
      buf[pos++] = '0';
    } else {
      if (is_negative()) {
        buf[pos++] = '-';
      }
      // oracle模式下小数的整数部分不补0，0.2345用.2345表示，-0.2345用-.2345表示
      if (nmb->is_decimal() && !is_oracle_mode()) {
        buf[pos++] = '0';
      }

      ObDigitIterator di;
      di.assign(nmb->get_desc_value(), nmb->get_digits());
      uint32_t digit = 0;
      ObDigitIterator::NextDigitEnum nd_enum = ObDigitIterator::NextDigitEnum::ND_END;
      while (OB_SUCC(ret) && ObDigitIterator::NextDigitEnum::ND_END != (nd_enum = di.get_next_digit(digit))) {
        switch (nd_enum) {
          case ObDigitIterator::NextDigitEnum::ND_HEAD_INTEGER: {
            const int64_t format_length = ObFastFormatInt::format_unsigned(digit, buf + pos);
            if (OB_UNLIKELY(format_length > DIGIT_LEN)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("digits is unexpected", "digit", digit, KPC(this), K(ret));
              //print number will not report error ret, we need make use find it
              MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
            } else {
              pos += format_length;
            }
            break;
          }
          case ObDigitIterator::NextDigitEnum::ND_BODY_DECIMAL:
            if (prev_from_integer) {
              // dot
              buf[pos++] = '.';
              prev_from_integer = false;
            }
            pad_zero_count -= DIGIT_LEN;
          case ObDigitIterator::NextDigitEnum::ND_BODY_INTEGER: {
            // normal digit
            ObFastFormatInt ffi(digit);
            if (OB_UNLIKELY(ffi.length() > DIGIT_LEN)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("digits is unexpected", "ffi", ffi.str(), K(digit), KPC(this), K(ret));
              //print number will not report error ret, we need make use find it
              MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
            } else {
              MEMSET(buf + pos, '0', DIGIT_LEN - ffi.length());
              MEMCPY(buf + pos + DIGIT_LEN - ffi.length(), ffi.ptr(), ffi.length());
              pos += DIGIT_LEN;
            }
            break;
          }
          case ObDigitIterator::NextDigitEnum::ND_TAIL_DECIMAL: {
            // last decimal
            if (prev_from_integer) {
              // dot
              buf[pos++] = '.';
              prev_from_integer = false;
            }
            ObFastFormatInt ffi(digit);
            if (OB_UNLIKELY(ffi.length() > DIGIT_LEN)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("digits is unexpected", "ffi", ffi.str(), K(digit), KPC(this), K(ret));
              //print number will not report error ret, we need make use find it
              MEMCPY(buf + pos, NUMBER_ERRMSG.ptr(), NUMBER_ERRMSG.length());
            } else {
              const int64_t tail_zero_count = ffi.get_tail_zero_count();
              MEMSET(buf + pos, '0', DIGIT_LEN - ffi.length());
              MEMCPY(buf + pos + DIGIT_LEN - ffi.length(), ffi.ptr(), ffi.length() - tail_zero_count);
              pos += (DIGIT_LEN - tail_zero_count);
              pad_zero_count -= (DIGIT_LEN - tail_zero_count);
            }
            break;
          }
          default: {
            LOG_ERROR("it should not arrive here", K(nd_enum));
          }
        }
      }//end while. end iteration
    }

    // pad zero.
    if (OB_SUCC(ret) && pad_zero_count > 0) {
      if (prev_from_integer) {
        buf[pos++] = '.';
      }
      if (OB_UNLIKELY(pad_zero_count > FLOATING_SCALE)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("the param is invalid", K(ret), K(pad_zero_count));
      } else if (OB_UNLIKELY(pos + pad_zero_count > buf_len)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("size is overflow", K(pos), K(pad_zero_count), K(buf_len), K(ret));
      } else {
        MEMCPY(buf + pos, FLOATING_ZEROS, pad_zero_count);
        pos += pad_zero_count;
      }
    }

    if (OB_SUCC(ret)) {
      buf[pos] = '\0';
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(pos >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("size is overflow, it should not happened", K(pos), K(buf_len), KPC(this),
                K(scale), K(ret));
      OB_ASSERT(pos < buf_len);
    } else if (OB_UNLIKELY(pos - orig_pos > max_need_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("calc size is too litter", "write_len", pos - orig_pos,
                K(max_need_size), K(buf_len), KPC(this), K(scale), K(ret));
      OB_ASSERT(pos - orig_pos <= max_need_size);
    } else if (is_oracle_mode() && need_to_sci && pos - orig_pos > SCI_NUMBER_LENGTH) {
      ObString num_str(pos - orig_pos, buf + orig_pos);
      pos = orig_pos;
      if (OB_FAIL(to_sci_str_(num_str, buf, buf_len, pos))) {
        LOG_ERROR("fail to conv to sci str", K(buf_len), K(orig_pos));
      }
    }
  }
  return ret;
}

int ObNumber::to_sci_str_(ObString &num_str, char *buf,
                          const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t COSNT_BUF_SIZE = 256;
  char ptr[COSNT_BUF_SIZE];
  int64_t origin = pos;
  int64_t str_len = num_str.length();
  if (OB_UNLIKELY(pos > buf_len || buf_len < 0 || pos < 0 || OB_ISNULL(buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value", K(ret), K(pos), K(buf_len), KP(buf));
  } else if (OB_UNLIKELY(buf_len - pos < SCI_NUMBER_LENGTH || str_len >= COSNT_BUF_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value", K(ret), K(pos));
  } else {
    MEMCPY(ptr, num_str.ptr(), str_len);
    int64_t raw_pos = 0;
    // exponent part of the str
    char pow_str[6];
    int64_t pow_pos = 0;
    // exponent value
    int64_t pow_size = 0;
    bool pre_dot = false;
    bool is_negative = false;
    // exponent value str width
    int64_t width_count = 0;
    pow_str[pow_pos++] = 'E';
    pow_str[pow_pos++] = '+';
    if ('-' == ptr[raw_pos]) {
      raw_pos++;
      buf[pos++] = '-';
      is_negative = true;
    }
    if ('.' == ptr[raw_pos]) {
      raw_pos++;
      pre_dot = true;
      pow_str[pow_pos - 1] = '-';
    }
    int64_t zero_count = 0;
    //find the first non-zero number
    while ('0' == ptr[raw_pos] && raw_pos < str_len) {
      raw_pos++;
      zero_count++;
    }
    buf[pos++] = ptr[raw_pos++];
    buf[pos++] = '.';
    // determine the exponent part and the number part according to
    // whether a decimal point appears in the front
    if (pre_dot) {
      pow_size = zero_count + 1;
      if (pow_size >= 0 && pow_size <= 9) {
        width_count = 1;
      } else if (pow_size >= 10 && pow_size <= 99) {
        width_count = 2;
      } else {
        width_count = 3;
      }
      // fill 0 if the sci number is less than 40 bytes
      while (pos < SCI_NUMBER_LENGTH - width_count - pow_pos + origin) {
        if (raw_pos >= str_len) {
          buf[pos++] = '0';
        } else {
          buf[pos++] = ptr[raw_pos++];
        }
      }
    } else if (!pre_dot && 0 == zero_count) {
      int64_t count = 0;
      // if the number is greater than 10, always need to traverse the number string array
      // to get the final exponent value. stop traversing when the decimal point appears.
      // the exponent value will affect the number of bytes
      for (int64_t i = raw_pos; i < str_len && ptr[i] != '.'; ++i) {
        count++;
      }
      if (count >= 0 && count <= 9) {
        width_count = 1;
      } else if (count >= 10 && count <= 99) {
        width_count = 2;
      } else {
        width_count = 3;
      }
      while (pos < SCI_NUMBER_LENGTH - pow_pos - width_count + origin) {
        if (raw_pos >= str_len) {
          buf[pos++] = '0';
        } else if ('.' == ptr[raw_pos]) {
          raw_pos++;
        } else {
          buf[pos++] = ptr[raw_pos++];
        }
      }
      pow_size = count;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the number raw str is unexpected", K(ret));
    }
    // round to the last digit and handle the carry
    if (OB_SUCC(ret)) {
      int64_t carry = 0;
      int64_t carry_pos = pos;
      // if it is a negative number, need to ignore the leading '-'
      int64_t digit_start_pos = is_negative ? origin + 1 : origin;
      if (raw_pos < str_len && ptr[raw_pos] >= '5' && ptr[raw_pos] <= '9') {
        carry = 1;
        carry_pos--;
        while (carry && carry_pos >= digit_start_pos && OB_SUCC(ret)) {
          if (buf[carry_pos] >= '0' && buf[carry_pos] <= '8') {
            buf[carry_pos] = (char)((int)buf[carry_pos] + carry);
            carry = 0;
            carry_pos--;
          } else if ('9' == buf[carry_pos]) {
            carry = 1;
            buf[carry_pos--] = '0';
          } else if ('.' == buf[carry_pos]) {
            carry_pos--;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("It's unexpected to round the number sci", K(ret));
          }
        }
        // if there is a carry in the last digit, move one byte to the right
        // eg: 10.000000000000000000000000000000000E+1 --> 1.0000000000000000000000000000000000E+2
        if (1 == carry && digit_start_pos - 1 == carry_pos && OB_SUCC(ret)) {
          for (int64_t i = pos - 1; i >= digit_start_pos + 1; --i) {
            if (buf[i - 1] != '.') {
              buf[i] = buf[i - 1];
            }
          }
          buf[digit_start_pos] = '1';
          buf[digit_start_pos + 1] = '.';
          ++pow_size;
        }
      }
    }
    // fill exponent part
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(pow_str, sizeof(pow_str), pow_pos, "%ld", pow_size))) {
        LOG_WARN("fail to generate pow str", K(ret));
      } else {
        for (int i = 0; i < pow_pos; ++i) {
          buf[pos++] = pow_str[i];
        }
      }
    }
    // check pos
    if (OB_SUCC(ret)) {
      if (str_len > SCI_NUMBER_LENGTH && pos - origin != SCI_NUMBER_LENGTH) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the value of pos is invalid after number to char in oracle mode",
                 KCSTRING(buf), K(pos), K(origin), K(ret));
      }
    }
  }
  return ret;
}

//oracle max store 20 bytes number(39~40 number). when using to_char, only store 40 number at most
int ObNumber::format_with_oracle_limit(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const
{
  int ret = common::OB_SUCCESS;
  bool need_to_sci = true;
  number::ObNumber tmp_number;
  char buf_alloc[ObNumber::MAX_BYTE_LEN];
  ObDataBuffer allocator(buf_alloc, ObNumber::MAX_BYTE_LEN);
  //need deep copy before round
  if (OB_FAIL(tmp_number.from(*this, allocator))) {
    LOG_WARN("fail to deep_copy", K(ret), K(tmp_number));
  } else if (OB_FAIL(tmp_number.round_for_sci((-MIN_SCI_SIZE), true))) {
    LOG_WARN("fail to round", K(ret), K(tmp_number));
  } else if (OB_FAIL(tmp_number.format_v2(buf, buf_len, pos, scale, need_to_sci))) {
    LOG_WARN("fail to format", K(ret), K(tmp_number));
  }
  return ret;
}

int ObNumber::get_npi_(double n, ObNumber& out, ObIAllocator &alloc, const bool do_rounding) const
{
  int ret = OB_SUCCESS;

  // remember to change this
  const int64_t LOCAL_ALLOC_TIMES = 1;
  const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
  char local_buf[LOCAL_BUF_SIZE];
  ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

  const int64_t MAX_DOUBLE_PRINT_SIZE = 512;
  char buf[MAX_DOUBLE_PRINT_SIZE] = {0};
  (void)ob_gcvt_opt(n, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1), buf, NULL, lib::is_oracle_mode(), TRUE);

  ObNumber n_obnum;
  ObNumber pi = get_pi();
  size_t str_len = strlen(buf);
  if (0 >= str_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("len of number string should not less than zero", K(ret), K(str_len));
  } else if (OB_FAIL(n_obnum.from_sci_opt(buf, str_len, local_alloc, NULL, NULL, do_rounding))) {
    LOG_WARN("n_obnum.from(n) failed", K(n), K(ret));
  } else if (OB_FAIL(pi.mul_v3(n_obnum, out, alloc, true, do_rounding))) {
    LOG_WARN("pi.mul(n_obnum) failed", K(n_obnum), K(pi), K(out));
  }

  return ret;
}

int ObNumber::get_npi_(int64_t n, ObNumber& out, ObIAllocator &alloc, const bool do_rounding) const
{
  int ret = OB_SUCCESS;

  // remember to change this
  const int64_t LOCAL_ALLOC_TIMES = 1;
  const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
  char local_buf[LOCAL_BUF_SIZE];
  ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

  ObNumber n_obnum;
  ObNumber pi = get_pi();
  if (OB_FAIL(n_obnum.from(n, local_alloc))) {
    LOG_WARN("n_obnum.from(n) failed", K(n), K(ret));
  } else if (OB_FAIL(pi.mul_v3(n_obnum, out, alloc, true, do_rounding))) {
    LOG_WARN("pi.mul(n_obnum) failed", K(n_obnum), K(pi), K(out));
  } else {
    // done
  }
  return ret;
}

// 根据前一项的阶乘计算本项阶乘，每次只用乘两个数即可,不需要从头计算
// 见sin/cos的泰勒展开式
int ObNumber::simple_factorial_for_sincos_(int64_t start, ObIAllocator &allocator, ObNumber &result) const
{
  int ret = OB_SUCCESS;
  ObNumber y;
  result.set_one();
  // remember to change this
  const int64_t LOCAL_ALLOC_TIMES = 3;
  const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
  char local_buf[LOCAL_BUF_SIZE];
  ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

  if (OB_FAIL(y.from(start, local_alloc))) {
    LOG_WARN("y.from(start) failed", K(y), K(start), K(ret));
  } else if (OB_FAIL(result.mul_v3(y, result, local_alloc, true, false))) {
    LOG_WARN("result.mul_v3(y) failed", K(result), K(y), K(ret));
  } else if (OB_FAIL(y.add_v3(ObNumber::get_positive_one(), y, local_alloc, true, false))) {
    LOG_WARN("y.add(1) failed", K(y), K(ret));
  } else if (OB_FAIL(result.mul_v3(y, result, allocator, true, false))) {
    LOG_WARN("result.mul(y) failed", K(result), K(y), K(ret));
  } else {
    // done
    LOG_DEBUG("factorial done", K(result));
  }
  return ret;
}

int ObNumber::taylor_series_sin_(const ObNumber &transformed_x, ObNumber &out, ObIAllocator &allocator, const ObNumber &range_low, const ObNumber &range_high) const
{
  int ret = OB_SUCCESS;

  if (!(range_low <= transformed_x && transformed_x <= range_high)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transform x before do taylor series expansion", K(transformed_x));
  } else {
    // remember to change this
    const int64_t LOCAL_ALLOC_TIMES = 5;
    const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
    char local_buf[LOCAL_BUF_SIZE];
    ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

    ObNumber iter_result;
    ObNumber divisor;
    ObNumber square_x;
    ObNumber tmp_out;

    if (OB_FAIL(iter_result.from(transformed_x, local_alloc))) {
      LOG_WARN("iter_result.from(transformed_x) failed", K(iter_result), K(transformed_x), K(ret));
    } else if (OB_FAIL(square_x.from(transformed_x, local_alloc))) {
      LOG_WARN("square_x.from(transformed_x) failed", K(square_x), K(transformed_x), K(ret));
    } else if (OB_FAIL(square_x.mul_v3(transformed_x, square_x, local_alloc, true, false))) {
      LOG_WARN("square_x.mul(transformed_x) failed", K(square_x), K(transformed_x), K(ret));
    } else if (OB_FAIL(tmp_out.from(transformed_x, local_alloc))) {
      LOG_WARN("tmp_out.from(transformed_x) failed", K(tmp_out), K(transformed_x), K(ret));
    } else if (OB_FAIL(divisor.from(ObNumber::get_positive_one(), local_alloc))) {
      LOG_WARN("divisor.from(1) failed", K(divisor), K(ret));
    } else {
      size_t idx = 1;
      size_t start_div_num = 2;
      const int64_t LOCAL_ALLOC_TIMES_FOR_LOOP = 4;
      const int64_t LOCAL_BUF_SIZE_FOR_LOOP = LOCAL_ALLOC_TIMES_FOR_LOOP * MAX_CALC_BYTE_LEN;
      char local_buf_for_loop_1[LOCAL_BUF_SIZE_FOR_LOOP];
      char local_buf_for_loop_2[LOCAL_BUF_SIZE_FOR_LOOP];
      ObDataBuffer local_alloc_for_loop_1(local_buf_for_loop_1, LOCAL_BUF_SIZE_FOR_LOOP);
      ObDataBuffer local_alloc_for_loop_2(local_buf_for_loop_2, LOCAL_BUF_SIZE_FOR_LOOP);

      LOG_DEBUG("start to calc taylor series expansion for sin", K(iter_result), K(tmp_out), K(square_x), K(divisor));
      while (OB_SUCC(ret) && (false == iter_result.is_zero())) {
        if (OB_FAIL(iter_result.mul_v3(square_x, iter_result, local_alloc_for_loop_1, true, false))) {
          LOG_WARN("iter_result.mul_v3(square_x) failed", K(ret), K(iter_result), K(square_x));
        } else if (OB_FAIL(simple_factorial_for_sincos_(start_div_num, local_alloc_for_loop_1, divisor))) {
          LOG_WARN("simple_factorial_for_sincos_(start_div_num) failed", K(ret));
        } else if (OB_FAIL(iter_result.div_v3(divisor, iter_result, local_alloc_for_loop_1, ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
          LOG_WARN("iter_result.div_v3(divisor) failed", K(ret), K(iter_result));
        } else {
          if (idx & 1) { // 奇数次
            if (OB_FAIL(tmp_out.sub_v3(iter_result, tmp_out, local_alloc_for_loop_1, true, false))) {
              LOG_WARN("tmp_out.sub_v3(iter_result) failed", K(ret), K(tmp_out));
            }
          } else {
            if (OB_FAIL(tmp_out.add_v3(iter_result, tmp_out, local_alloc_for_loop_1, true, false))) {
              LOG_WARN("tmp_out.add_v3(iter_result) failed", K(ret), K(tmp_out));
            }
          }
          LOG_DEBUG("iteration computing", K(iter_result), K(tmp_out));
          local_alloc_for_loop_2.free();
          std::swap(local_alloc_for_loop_1, local_alloc_for_loop_2);
          idx += 1;
          start_div_num += 2;
        }
      }
      LOG_DEBUG("iteration done", K(tmp_out));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(out.from(tmp_out, allocator))) {
          LOG_WARN("out.from(tmp_out) failed", K(out), K(tmp_out));
        }
      }
    }
  }
  return ret;
}

// 1. 将输入转换到[0, 2π]
// 2. 将输入转换到[0, π]
// 3. 通过泰勒展开计算sin结果
int ObNumber::sin(ObNumber &out, ObIAllocator &allocator, const bool do_rounding) const
{
  int ret = OB_SUCCESS;
  bool neg = false;;
  ObNumber pi;
  ObNumber range_low;     // 0
  ObNumber range_high;    // 2π
  ObNumber tmp_out;
  ObNumber transformed_x; // between [0, π]
  ObNumber x = *this;

  // remember to change this
  const int64_t LOCAL_ALLOC_TIMES = 7;
  const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
  char local_buf[LOCAL_BUF_SIZE];
  ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

  if (x.is_zero()) {
    if (OB_FAIL(out.from(static_cast<int64_t>(0), allocator))) {
      LOG_WARN("out.from(0) failed", K(ret), K(out));
    }
  } else if (OB_FAIL(get_npi_(static_cast<int64_t>(2), range_high, local_alloc, false))) {
    LOG_WARN("get_npi_(2) failed", K(ret));
  } else if (OB_FAIL(get_npi_(static_cast<int64_t>(1), pi, local_alloc, false))) {
    LOG_WARN("get_npi_(1) failed", K(ret));
  } else {
    range_low.set_zero();
    if (x.is_negative()) {
      neg = !neg;
      if (OB_FAIL(x.negate(x, local_alloc))) {
        LOG_WARN("x.negate() failed", K(x), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!(range_low <= x && x <= range_high)) {
        if (OB_FAIL(x.rem(range_high, transformed_x, local_alloc))) {
          LOG_WARN("x.rem(range_high) failed", K(ret), K(x), K(range_high));
        }
      } else {
        if (OB_FAIL(transformed_x.from(x, local_alloc))) {
          LOG_WARN("transformed_x.from(x) failed", K(x), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // transformed_x now in [0, 2π]
      // make it in [0, π]
      if (pi < transformed_x) {
        neg = !neg;
        if (OB_FAIL(transformed_x.sub_v3(pi, transformed_x, local_alloc, true, false))) {
          LOG_WARN("transformed_x.sub_v3(pi) failed", K(transformed_x), K(pi));
        }
      }
    }
    LOG_DEBUG("transform input done, start to taylor series expansion", K(neg), K(transformed_x), K(x));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(taylor_series_sin_(transformed_x, tmp_out, local_alloc, range_low, pi))) {
        LOG_WARN("taylor_series_sin_(transformed_x) failed", K(ret), K(transformed_x));
      } else {
        if (neg) {
          if (OB_FAIL(tmp_out.negate(tmp_out, local_alloc))) {
            LOG_WARN("tmp_out.negate() failed", K(ret), K(tmp_out));
          }
        }
        if (OB_SUCC(ret)) {
          if (do_rounding) {
            if (OB_FAIL(tmp_out.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
              LOG_WARN("round scale fail", K(ret), K(tmp_out));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(out.from(tmp_out, allocator))) {
            LOG_WARN("out.from(tmp_out) failed", K(ret), K(tmp_out));
          }
        }
      }
    }
  }
  LOG_DEBUG("sin done", K(out));
  return ret;
}

int ObNumber::taylor_series_cos_(const ObNumber &transformed_x, ObNumber &out, ObIAllocator &allocator, const ObNumber &range_low, const ObNumber &range_high) const
{
  int ret = OB_SUCCESS;

  if (!(range_low <= transformed_x && transformed_x <= range_high)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transform x before do taylor series expansion", K(transformed_x));
  } else {
    // remember to change this
    const int64_t LOCAL_ALLOC_TIMES = 6;
    const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
    char local_buf[LOCAL_BUF_SIZE];
    ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

    ObNumber iter_result;
    ObNumber divisor;
    ObNumber square_x;
    ObNumber two;
    ObNumber one = ObNumber::get_positive_one();
    ObNumber tmp_out;

    if (OB_FAIL(square_x.from(transformed_x, local_alloc))) {
      LOG_WARN("square_x.from failed", K(square_x), K(transformed_x), K(ret));
    } else if (OB_FAIL(square_x.mul_v3(transformed_x, square_x, local_alloc, true, false))) {
      LOG_WARN("square_x.mul failed", K(square_x), K(transformed_x), K(ret));
    } else if (OB_FAIL(two.from(static_cast<int64_t>(2), local_alloc))) {
      LOG_WARN("two.from(2) failed", K(ret));
    } else if (OB_FAIL(divisor.from(two, local_alloc))) {
      LOG_WARN("divisor.from(1) failed", K(divisor), K(ret));
    } else if (OB_FAIL(square_x.div_v3(two, iter_result, local_alloc, ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("square_x.div_v3(two) failed", K(square_x), K(ret));
    } else if (OB_FAIL(one.sub_v3(iter_result, tmp_out, local_alloc, true, false))) {
      LOG_WARN("one.sub_v3(iter_result) failed", K(iter_result), K(one), K(ret));
    } else {
      size_t idx = 2;
      size_t start_div_num = 3;

      const int64_t LOCAL_ALLOC_TIMES_FOR_LOOP = 4;
      const int64_t LOCAL_BUF_SIZE_FOR_LOOP = LOCAL_ALLOC_TIMES_FOR_LOOP * MAX_CALC_BYTE_LEN;
      char local_buf_for_loop_1[LOCAL_BUF_SIZE_FOR_LOOP];
      char local_buf_for_loop_2[LOCAL_BUF_SIZE_FOR_LOOP];
      ObDataBuffer local_alloc_for_loop_1(local_buf_for_loop_1, LOCAL_BUF_SIZE_FOR_LOOP);
      ObDataBuffer local_alloc_for_loop_2(local_buf_for_loop_2, LOCAL_BUF_SIZE_FOR_LOOP);

      LOG_DEBUG("before while", K(iter_result), K(tmp_out), K(square_x), K(divisor));
      while (OB_SUCC(ret) && (false == iter_result.is_zero())) {
        if (OB_FAIL(iter_result.mul_v3(square_x, iter_result, local_alloc_for_loop_1, true, false))) {
          LOG_WARN("iter_result.mul_v3(square_x) failed", K(ret), K(iter_result));
        } else if (OB_FAIL(simple_factorial_for_sincos_(start_div_num, local_alloc_for_loop_1, divisor))) {
          LOG_WARN("simple_factorial_for_sincos_(start_div_num) failed", K(ret));
        } else if (OB_FAIL(iter_result.div_v3(divisor, iter_result, local_alloc_for_loop_1, ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
          LOG_WARN("iter_result.div_v3(divisor) failed", K(ret), K(iter_result), K(divisor));
        } else {
          if (idx & 1) { // 奇数次循环
            if (OB_FAIL(tmp_out.sub_v3(iter_result, tmp_out, local_alloc_for_loop_1, true, false))) {
              LOG_WARN("tmp_out.sub_v3(iter_result) failed", K(ret), K(tmp_out), K(iter_result));
            }
          } else {
            if (OB_FAIL(tmp_out.add_v3(iter_result, tmp_out, local_alloc_for_loop_1, true, false))) {
              LOG_WARN("tmp_out.add_v3(iter_result) failed", K(ret), K(tmp_out), K(iter_result));
            }
          }
          LOG_DEBUG("iteration computing", K(idx), K(tmp_out));
          local_alloc_for_loop_2.free();
          std::swap(local_alloc_for_loop_1, local_alloc_for_loop_2);
          idx += 1;
          start_div_num += 2;
        }
      }
      LOG_DEBUG("iteration done", K(tmp_out));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(out.from(tmp_out, allocator))) {
          LOG_WARN("out.from(tmp_out) failed", K(out), K(tmp_out));
        }
      }
    }
  }
  return ret;
}

// 1. 将输入转换到[-π/2, 3π/2]
// 2. 将输入转换到[-π/2, π/2]
// 3. 通过泰勒展开计算cos结果
// 也可以通过cos(x) = sin(x+π/2)，或者cos(x) = sqrt(1 - sin(x)^2)来计算cos，
// 这样速度更快，但是精度不如用泰勒展开
int ObNumber::cos(ObNumber &out, ObIAllocator &allocator, const bool do_rounding) const
{
  int ret = OB_SUCCESS;

  if (is_zero()) {
    if (OB_FAIL(out.from(static_cast<int64_t>(1), allocator))) {
      LOG_WARN("out.from(1) failed", K(ret));
    }
  } else {
    // remember to change this
    const int64_t LOCAL_ALLOC_TIMES = 10;
    const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
    char local_buf[LOCAL_BUF_SIZE];
    ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

    bool neg = false;
    ObNumber x = *this;
    ObNumber pi;
    ObNumber tmp_out;
    ObNumber half_pi;     // π/2
    ObNumber double_pi;   // 2π
    ObNumber range_low;   // -π/2
    ObNumber range_high;  // 3π/2
    ObNumber transformed_x;

    if (x.is_negative()) {
      // cos(x)是偶函数，不需要设置neg
      if (OB_FAIL(x.negate(x, local_alloc))) {
        LOG_WARN("x.negate failed", K(ret), K(x));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_npi_(static_cast<double>(-1)/2, range_low, local_alloc, false))) {
        LOG_WARN("get_npi_(-1/2) failed", K(ret));
      } else if (OB_FAIL(get_npi_(static_cast<double>(1)/2, half_pi, local_alloc, false))) {
        LOG_WARN("get_npi_(1/2) failed", K(ret));
      } else if (OB_FAIL(get_npi_(static_cast<double>(3)/2, range_high, local_alloc, false))) {
        LOG_WARN("get_npi_(3/2) failed", K(ret));
      } else if (OB_FAIL(get_npi_(static_cast<int64_t>(2), double_pi, local_alloc, false))) {
        LOG_WARN("get_npi_(2) failed", K(ret));
      } else if (OB_FAIL(get_npi_(static_cast<int64_t>(1), pi, local_alloc, false))) {
        LOG_WARN("get_npi_(1) failed", K(ret));
      } else {
        if (range_low <= x && x <= range_high) {
          if (OB_FAIL(transformed_x.from(x, local_alloc))) {
            LOG_WARN("transformed_x.from(x) failed", K(ret), K(transformed_x));
          }
        } else {
          if (OB_FAIL(x.rem(double_pi, transformed_x, local_alloc))) {
            LOG_WARN("x.rem(double_pi) failed", K(ret), K(x), K(double_pi));
          } else if (range_high < transformed_x) {
            if (OB_FAIL(transformed_x.sub_v3(double_pi, transformed_x, local_alloc, true, false))) {
              LOG_WARN("transformed_x.sub_v3(double_pi) failed", K(ret), K(transformed_x), K(double_pi));
            }
          }
        }

        if (OB_SUCC(ret)) {
          // transformed_x no in[-π/2, 3π/2]
          // make in [-π/2, π/2]
          if (half_pi < transformed_x) {
            neg = !neg;
            if (OB_FAIL(transformed_x.sub_v3(pi, transformed_x, local_alloc, true, false))) {
              LOG_WARN("transformed_x.sub_v3(pi) failed", K(ret), K(transformed_x), K(pi));
            }
          }
        }

        LOG_DEBUG("transform x done, starto taylor series expansion", K(double_pi), K(range_low),
                                                                     K(range_high), K(transformed_x),
                                                                     K(x), K(tmp_out));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(taylor_series_cos_(transformed_x, tmp_out, local_alloc, range_low, half_pi))) {
            LOG_WARN("taylor_series_cos_(transformed_x) failed", K(ret), K(transformed_x), K(tmp_out));
          }
          if (OB_SUCC(ret)) {
            if (do_rounding) {
              if (OB_FAIL(tmp_out.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
                LOG_WARN("round scale fail", K(ret), K(tmp_out));
              }
            }
            if (OB_SUCC(ret)) {
              if (neg) {
                if (OB_FAIL(tmp_out.negate(tmp_out, local_alloc))) {
                  LOG_WARN("tmp_out.negate() failed", K(ret), K(tmp_out));
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(out.from(tmp_out, allocator))) {
                LOG_WARN("out.from(tmp_out) failed", K(ret), K(tmp_out));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// 直接使用sin/cos来计算tan
int ObNumber::tan(ObNumber &out, ObIAllocator &allocator, const bool do_rounding) const
{
  int ret = OB_SUCCESS;

  // remember to change this
  const int64_t LOCAL_ALLOC_TIMES = 3;
  const int64_t LOCAL_BUF_SIZE = LOCAL_ALLOC_TIMES * MAX_CALC_BYTE_LEN;
  char local_buf[LOCAL_BUF_SIZE];
  ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);

  bool sin_is_zero = false;
  bool cos_is_zero = false;
  ObNumber sin_out;
  ObNumber cos_out;
  ObNumber tmp_out;

  if (OB_FAIL(sin(sin_out, local_alloc, false))) {
    LOG_WARN("sin(x) failed", K(*this), K(ret));
  } else if (sin_out.is_zero()) {
    sin_is_zero = true;
  }

  if (OB_SUCC(ret) && !sin_is_zero) {
    if (OB_FAIL(cos(cos_out, local_alloc, false))) {
      LOG_WARN("cos(x) failed", K(*this), K(ret));
    } else if (cos_out.is_zero()) {
      cos_is_zero = true;
    }
  }

  LOG_DEBUG("start to calc sin/cos", K(*this), K(sin_out), K(cos_out), K(sin_is_zero), K(cos_is_zero));
  if (OB_SUCC(ret)) {
    if (sin_is_zero) {
      out.set_zero();
    } else if (cos_is_zero) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("cos(x) is zero", K(*this), K(ret));
    } else {
      if (OB_FAIL(sin_out.div_v3(cos_out, tmp_out, local_alloc, OB_MAX_DECIMAL_DIGIT, false))) {
        LOG_WARN("sin/cos failed", K(sin_out), K(cos_out), K(ret));
      } else if (do_rounding) {
        if (OB_FAIL(tmp_out.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
          LOG_WARN("tmp_out.round_scale_v3_() fail", K(ret), K(tmp_out));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(out.from(tmp_out, allocator))) {
          LOG_WARN("out.from(tmp_out) failed", K(ret), K(tmp_out));
        }
      }
    }
  }
  return ret;
}

//asin(x) = atan(x/sqrt(1-x^2))
int ObNumber::asin(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  const int64_t LOCAL_ALLOCATE_TIME = 8;
  const char half_pi_buf[] = "1.57079632679489661923132169163975144209858469968755291048747229615";
  char buf_alloc_local[ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME];
  ObDataBuffer allocator_local(buf_alloc_local, ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME);
  ObNumber res;
  ObNumber one;

  if (OB_FAIL(one.from(static_cast<int64_t>(1), allocator_local))) {
    LOG_WARN("create const one failed", K(one), K(ret));
  } else if (abs_compare(one) >= 0) {
    if (OB_UNLIKELY(1 == abs_compare(one))) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("parameter abs larger than 1", K(ret));
    } else if (0 == compare(one)) {
      if (OB_FAIL(res.from(half_pi_buf, allocator,NULL,NULL,false))) {
        LOG_WARN("res from pi/2 failed", K(res), K(ret));
      }
    } else {
      if (OB_FAIL(res.from(half_pi_buf, allocator_local,NULL,NULL,false))) {
        LOG_WARN("res from pi/2 failed", K(res), K(ret));
      } else if(OB_FAIL(res.negate(res, allocator))) {
        LOG_WARN("res from pi/2 negate failed", K(res), K(ret));
      }
    }
  } else {
    ObNumber atan_arg;
    if (OB_FAIL(mul_v3(*this, atan_arg, allocator_local, true, false))) {
      LOG_WARN("*this mul *this failed", K(*this), K(atan_arg), K(ret));
    } else if (OB_FAIL(one.sub_v3(atan_arg, atan_arg, allocator_local, true, false))) {
      LOG_WARN("one sub atan_arg failed", K(one), K(atan_arg), K(ret));
    } else if (OB_FAIL(atan_arg.sqrt(atan_arg, allocator_local, false))) {
      LOG_WARN("atan_arg sqrt failed", K(atan_arg), K(ret));
    } else if (OB_FAIL(div_v3(atan_arg, atan_arg, allocator_local, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("*this div atan_arg failed", K(*this), K(atan_arg), K(ret));
    } else if (OB_FAIL(atan_arg.atan(res, allocator, false))) {
      LOG_WARN("atan_arg atan failed", K(atan_arg), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "asin [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

//acos(x) = pi/2 - asin(x)
int ObNumber::acos(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  const int64_t LOCAL_ALLOCATE_TIME = 2;
  const char half_pi_buf[] = "1.57079632679489661923132169163975144209858469968755291048747229615";
  char buf_alloc_local[ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME];
  ObDataBuffer allocator_local(buf_alloc_local, ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME);
  ObNumber res;

  if (OB_UNLIKELY((*this) > static_cast<int64_t>(1) || (*this) < static_cast<int64_t>(-1))) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("parameter abs larger than 1", K(ret));
  } else if ((*this) == static_cast<int64_t>(1)) {
    if (OB_FAIL(res.from(static_cast<int64_t>(0), allocator))) {
      LOG_WARN("res from const 1 failed", K(res), K(ret));
    }
  } else {
    ObNumber half_pi;
    if (OB_FAIL(half_pi.from(half_pi_buf, allocator_local,NULL,NULL,false))) {
      LOG_WARN("half_pi from const pi/2 failed", K(half_pi), K(ret));
    } else if (OB_FAIL(asin(res, allocator_local, false))) {
      LOG_WARN("res=this->asin failed", K(*this), K(res), K(ret));
    } else if (OB_FAIL(half_pi.sub_v3(res, res, allocator, true, false))) {
      LOG_WARN("res = half_pi sub res failed", K(res), K(half_pi), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "acos [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

//parameter reduction : atan(x) = 2atan(x/(1+sqrt(1+x^2)))
//taylor series: atan(x) = x-(x^3)/3+(x^5)/5-(x^7)/7+... when |x|<1
int ObNumber::atan(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_LOOP_ALLOCATE_TIME = 5;
  const int64_t MAX_TAYLOR_SERIES_COUNT = 200;
  const char PARAM_MAX_VALUE[5] = "0.15";
  char buf_alloc_doublex[ObNumber::MAX_BYTE_LEN];
  char buf_alloc_this[ObNumber::MAX_BYTE_LEN];
  char buf_alloc_res[ObNumber::MAX_BYTE_LEN];
  char buf_alloc_tmp[ObNumber::MAX_BYTE_LEN];
  char buf_alloc_iter1[ObNumber::MAX_BYTE_LEN * MAX_LOOP_ALLOCATE_TIME];
  char buf_alloc_iter2[ObNumber::MAX_BYTE_LEN];
  char buf_alloc_const1[ObNumber::MAX_BYTE_LEN];
  char buf_alloc_const2[ObNumber::MAX_BYTE_LEN];
  ObDataBuffer allocator_doublex(buf_alloc_doublex, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_this(buf_alloc_this, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_res(buf_alloc_res, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_tmp(buf_alloc_tmp, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_iter1(buf_alloc_iter1, ObNumber::MAX_BYTE_LEN * MAX_LOOP_ALLOCATE_TIME);
  ObDataBuffer allocator_iter2(buf_alloc_iter2, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_const1(buf_alloc_const1, ObNumber::MAX_BYTE_LEN);
  ObDataBuffer allocator_const2(buf_alloc_const2, ObNumber::MAX_BYTE_LEN);
  ObNumber res;
  ObNumber copy_this;
  ObNumber param_bound;
  const char half_pi_buf[] = "1.57079632679489661923132169163975144209858469968755291048747229615";

  if (OB_FAIL(res.from(static_cast<int64_t>(0), allocator_res))) {
    LOG_WARN("res from const 0 failed", K(res), K(ret));
  } else if (OB_FAIL(param_bound.from(PARAM_MAX_VALUE, allocator_const1))) {
    LOG_WARN("create const 0.5 failed", K(param_bound), K(ret));
  } else if (OB_FAIL(copy_this.from(*this, allocator_this))) {
    LOG_WARN("copy *this failed", K(copy_this), K(ret));
  } else {
    ObNumber one;
    ObNumber tmp;
    uint8_t reduction_count = 0;
    //parameter reduction: arctan x = 2arctan(x/(1+sqrt(1+x*x)))
    while (OB_SUCC(ret) && 1 == copy_this.abs_compare(param_bound)) {
      if (0 == reduction_count && OB_FAIL(one.from(static_cast<int64_t>(1),allocator_const2))) {
        LOG_WARN("create const 1 failed", K(one), K(ret));
      } else if (OB_FAIL(tmp.from(copy_this, allocator_iter1))) {
        LOG_WARN("tmp copy from copy_this failed", K(copy_this), K(tmp), K(ret));
      } else if (OB_FAIL(tmp.mul_v3(copy_this, tmp, allocator_iter1, true, false))) {
        if (OB_INTEGER_PRECISION_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          copy_this.set_zero();
          allocator_res.free();
          if (OB_FAIL(res.from(half_pi_buf, allocator_res,NULL,NULL,false))) {
            LOG_WARN("res from pi/2 failed", K(res), K(ret));
          } else if (is_negative()) {
            res = res.negate();
          }
        } else {
          LOG_WARN("tmp = tmp mul copy_this failed", K(copy_this), K(tmp), K(ret));
        }
      } else {
        if (OB_FAIL(tmp.add_v3(one, tmp, allocator_iter1, true, false))) {
          LOG_WARN("tmp add one failed", K(copy_this), K(tmp), K(one), K(ret));
        } else if (OB_FAIL(tmp.sqrt(tmp, allocator_iter1, false))) {
          LOG_WARN("tmp sqrt failed", K(copy_this), K(tmp), K(ret));
        } else if (OB_FAIL(tmp.add_v3(one, tmp, allocator_iter1, true, false))) {
          LOG_WARN("tmp add one failed", K(copy_this), K(tmp), K(one), K(ret));
        } else if (OB_FAIL(copy_this.div_v3(tmp, copy_this, allocator_tmp, OB_MAX_DECIMAL_DIGIT, false))) {
          LOG_WARN("copy_this div tmp failed", K(copy_this), K(tmp), K(ret));
        } else {
          std::swap(allocator_tmp, allocator_this);
          allocator_tmp.free();
          allocator_iter1.free();
          reduction_count++;
        }
      }
    }
    ObNumber doublex;
    ObNumber taylor_series;
    ObNumber iter_const;
    int64_t count = 0;
    allocator_const1.free();
    //taylor series: atan(x) = x-(x^3)/3+(x^5)/5-(x^7)/7+... when |x|<1
    if (OB_FAIL(taylor_series.from(copy_this, allocator_iter2))) {
      LOG_WARN("taylor series from copy_this failed", K(copy_this), K(taylor_series), K(ret));
    } else if (OB_FAIL(copy_this.mul_v3(copy_this, doublex, allocator_doublex, true, false))) {
      LOG_WARN("doublex = copy_this*copy_this failed", K(copy_this), K(doublex), K(ret));
    } else if (OB_FAIL(iter_const.from(static_cast<int64_t>(1), allocator_const1))){
      LOG_WARN("iter_const from 1 failed", K(iter_const), K(ret));
    } else {
      while (OB_SUCC(ret) && !taylor_series.is_zero() && count < MAX_TAYLOR_SERIES_COUNT) {
        if (OB_FAIL(res.add_v3(taylor_series, res, allocator_tmp, true, false))) {
          LOG_WARN("res add taylor failed", K(res), K(taylor_series), K(ret));
        } else if (OB_FAIL(taylor_series.mul_v3(iter_const, taylor_series, allocator_iter1, true, false))) {
          LOG_WARN("taylor_series mul iter_const failed", K(taylor_series), K(iter_const), K(ret));
        } else {
          std::swap(allocator_tmp, allocator_res);
          allocator_tmp.free();
          allocator_iter2.free();
          allocator_const1.free();
          count++;
          if (OB_FAIL(iter_const.from(static_cast<int64_t>(2 * count + 1), allocator_const1))) {
            LOG_WARN("iter_const from 2*count+1 failed", K(count), K(iter_const), K(ret));
          } else if (OB_FAIL(taylor_series.div_v3(iter_const, taylor_series, allocator_iter1, OB_MAX_DECIMAL_DIGIT, false))) {
            LOG_WARN("taylor_series div iter_const failed", K(taylor_series), K(iter_const), K(ret));
          } else if (OB_FAIL(taylor_series.mul_v3(doublex, taylor_series, allocator_iter1, true, false))) {
            LOG_WARN("taylor_series mul doublex failed", K(taylor_series), K(doublex), K(ret));
          } else if (OB_FAIL(taylor_series.negate(taylor_series, allocator_iter2))) {
            LOG_WARN("taylor_series negate failed", K(taylor_series), K(ret));
          } else {
            allocator_iter1.free();
          }
        }
      }
      while (OB_SUCC(ret) && reduction_count > 0) {
        if (OB_FAIL(res.add_v3(res, res, allocator_tmp, true, false))) {
          LOG_WARN("res=res+res failed", K(res), K(ret));
        } else {
          std::swap(allocator_tmp, allocator_res);
          allocator_tmp.free();
          reduction_count--;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
    } else if (OB_FAIL(value.from(res, allocator))){
      LOG_WARN("value copy from res failed", K(ret), K(res));
    }
  }
  _OB_LOG(DEBUG, "atan [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

int ObNumber::atan2(const ObNumber &other, ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  const char half_pi_buf[] = "1.57079632679489661923132169163975144209858469968755291048747229615";
  const char pi_buf[] = "3.14159265358979323846264338327950288419716939937510582097494459231";
  if (is_zero()) {
    if (OB_UNLIKELY(other.is_zero())) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("atan2 numeric overflow", K(ret));
    } else if (other.is_negative()) {
      if (OB_FAIL(res.from(pi_buf, allocator, NULL, NULL, false))) {
        LOG_WARN("res from pi failed", K(ret), K(res));
      }
    } else if (OB_FAIL(res.from("0", allocator, NULL, NULL, false))) {
      LOG_WARN("res from 0 failed", K(ret), K(res));
    }
  } else if (other.is_zero()) {
    if (OB_FAIL(res.from(half_pi_buf, allocator,NULL,NULL,false))) {
      LOG_WARN("res from pi/2 failed", K(res), K(ret));
    } else if (is_negative()) {
      res = res.negate();
    }
  } else {
    bool other_negative = other.is_negative();
    int64_t buf_len = other_negative ? ObNumber::MAX_BYTE_LEN * 3 : ObNumber::MAX_BYTE_LEN;
    char buf_alloc1[buf_len];
    ObDataBuffer allocator1(buf_alloc1, buf_len);
    ObNumber quotient;
    if (OB_FAIL(div_v3(other, quotient, allocator1, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("this div other failed", K(*this), K(other), K(ret));
    } else if (OB_FAIL(quotient.atan(res, other_negative ? allocator1 : allocator, false))) {
      LOG_WARN("quotient atan_ failed", K(quotient), K(ret));
    } else if (other_negative) {
      ObNumber num_pi;
      if (OB_FAIL(num_pi.from(pi_buf, allocator1, NULL, NULL, false))) {
        LOG_WARN("num from pi failed", K(ret));
      } else if (is_negative() && OB_FAIL(res.sub_v3(num_pi, res, allocator, true, false))) {
        LOG_WARN("res sub pi failed", K(ret));
      } else if (!is_negative() && OB_FAIL(res.add_v3(num_pi, res, allocator, true, false))) {
        LOG_WARN("res add pi failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "atan2 [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

int ObNumber::add_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc augend_desc;
  Desc addend_desc;
  augend_desc.desc_ = d_.desc_;
  addend_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("add_", K(ret), KPC(this), K(other));
  if (is_zero()) {
    ret = res.deep_copy(other, allocator);
  } else if (other.is_zero()) {
    ret = res.deep_copy(*this, allocator);
  } else if (augend_desc.sign_ == addend_desc.sign_) {
    ObCalcVector augend;
    ObCalcVector addend;
    int64_t shift = exp_integer_align_(augend_desc, addend_desc);
    exp_shift_(shift, augend_desc);
    exp_shift_(shift, addend_desc);
    if (OB_FAIL(augend.init(augend_desc.desc_, digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else if (OB_FAIL(addend.init(addend_desc.desc_, other.digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else {
      ObCalcVector sum;
      int64_t sum_size = std::max(augend.size(), addend.size()) + 1;
      if (OB_FAIL(sum.ensure(sum_size))) {
        LOG_WARN("Fail to ensure sum_size", K(ret));
      } else if (OB_FAIL(poly_poly_add(augend, addend, sum))) {
        _OB_LOG(WARN, "[%s] add [%s] fail ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_max_(augend_desc, addend_desc);
        bool carried = (0 != sum.at(0));
        exp_shift_(-shift + (carried ? 1 : 0), res_desc);
        ret = res.from_v2_(res_desc.desc_, sum, allocator);
      }
    }
  } else {
    ObNumber subtrahend;
    StackAllocator stack_allocator;
    if (OB_FAIL(other.negate(subtrahend, stack_allocator))) {
      _OB_LOG(WARN, "nagate [%s] fail, ret=%d", to_cstring(other), ret);
    } else {
      ret = sub_(subtrahend, res, allocator);
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "add_ [%s] + [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::sub_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc minuend_desc;
  Desc subtrahend_desc;
  minuend_desc.desc_ = d_.desc_;
  subtrahend_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("sub", K(ret), KPC(this), K(other));
  if (is_zero()) {
    ret = other.negate_(res, allocator);
  } else if (other.is_zero()) {
    ret = res.from_(*this, allocator);
  } else if (minuend_desc.sign_ == subtrahend_desc.sign_) {
    ObCalcVector minuend;
    ObCalcVector subtrahend;
    int64_t shift = exp_integer_align_(minuend_desc, subtrahend_desc);
    exp_shift_(shift, minuend_desc);
    exp_shift_(shift, subtrahend_desc);
    if (OB_FAIL(minuend.init(minuend_desc.desc_, digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else if (OB_FAIL(subtrahend.init(subtrahend_desc.desc_, other.digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else {
      ObCalcVector remainder;
      int64_t remainder_size = std::max(minuend.size(), subtrahend.size());

      bool sub_negative = false;
      if (OB_FAIL(remainder.ensure(remainder_size))) {
        LOG_WARN("remainder.ensure(remainder_size) fails", K(ret));
      } else if (OB_FAIL(poly_poly_sub(minuend, subtrahend, remainder, sub_negative))) {
        _OB_LOG(WARN, "[%s] sub [%s] fail ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_max_(minuend_desc, subtrahend_desc);
        for (int64_t i = 0; i < remainder.size() - 1; ++i) {
          if (0 != remainder.at(i)) {
            break;
          }
          ++shift;
        }
        exp_shift_(-shift, res_desc);

        bool arg_negative = (NEGATIVE == minuend_desc.sign_);
        if (sub_negative == arg_negative) {
          res_desc.sign_ = POSITIVE;
        } else {
          res_desc.sign_ = NEGATIVE;
        }

        if (res_desc.sign_ != minuend_desc.sign_) {
          res_desc.exp_ = (0x7f & ~res_desc.exp_);
          ++res_desc.exp_;
        }

        ret = res.from_v2_(res_desc.desc_, remainder, allocator);
      }
    }

  } else {
    ObNumber addend;
    StackAllocator stack_allocator;
    if (OB_FAIL(other.negate(addend, stack_allocator))) {
      _OB_LOG(WARN, "nagate [%s] fail, ret=%d", to_cstring(other), ret);
    } else {
      ret = add_(addend, res, allocator);
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "sub_ [%s] - [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::calc_desc_and_check_(const uint32_t base_desc, Desc &desc, int64_t exp, uint8_t len) const
{
  int ret = OB_SUCCESS;
  Desc d;
  d.desc_ = base_desc;
  d.len_ = len;
  d.reserved_ = 0; // keep always 0.
  d.exp_ = 0x7f & (uint8_t)(EXP_ZERO + exp);
  if (d.sign_ == NEGATIVE) {
    d.exp_ = (0x7f & (~d.exp_));
    ++d.exp_;
  }
  if (OB_FAIL(exp_check_(d, lib::is_oracle_mode()))) {
  } else {
    desc = d;
  }
  return ret;
}

int ObNumber::add_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc augend_desc;
  Desc addend_desc;
  augend_desc.desc_ = d_.desc_;
  addend_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("add_v2_", K(ret), KPC(this), K(other));
  if (is_zero()) {
    ret = res.deep_copy(other, allocator);
  } else if (other.is_zero()) {
    ret = res.deep_copy(*this, allocator);
  } else if (augend_desc.sign_ == addend_desc.sign_) {
    int64_t augend_exp = get_decode_exp(augend_desc.desc_);
    int64_t addend_exp = get_decode_exp(addend_desc.desc_);

    if (augend_exp >= addend_exp + OB_MAX_DECIMAL_DIGIT) {
      ret = res.deep_copy(*this, allocator);
    } else if (addend_exp >= augend_exp + OB_MAX_DECIMAL_DIGIT) {
      ret = res.deep_copy(other, allocator);
    } else {
      int64_t sum_exp = std::max(augend_exp, addend_exp);
      int32_t offset = 0;

      uint32_t *augend_digits = digits_;
      uint32_t *addend_digits = other.digits_;
      uint32_t sum_digits[OB_CALC_BUFFER_SIZE] = {};

      for (int64_t i = 1, curr_exp = sum_exp, augend_id = 0, addend_id = 0;
          i <= OB_MAX_DECIMAL_DIGIT;
          ++i, --curr_exp) {
        uint32_t &tmp_digit = sum_digits[i];
        if (curr_exp == augend_exp && augend_id < augend_desc.len_) {
          tmp_digit += augend_digits[augend_id];
          --augend_exp;
          ++augend_id;
        }
        if (curr_exp == addend_exp && addend_id < addend_desc.len_) {
          tmp_digit += addend_digits[addend_id];
          --addend_exp;
          ++addend_id;
        }
//        LOG_DEBUG("add_v2 ", K(sum_digits[i]), K(i), K(curr_exp), K(augend_exp), K(augend_id), K(addend_exp), K(addend_id));
      }

      for (int64_t i = OB_MAX_DECIMAL_DIGIT; i > 0; --i) {
        if (sum_digits[i] >= BASE) {
          sum_digits[i] -= BASE;
          ++sum_digits[i - 1];
        }
      }

      if (sum_digits[0] != 0) {
        offset = 0;
        ++sum_exp;
//        normalize_digit_(sum_digits, OB_MAX_DECIMAL_DIGIT);
      } else {
        offset = 1;
      }

      uint8_t len = OB_MAX_DECIMAL_DIGIT;
      while (len >= offset && sum_digits[len] == 0) {
        --len;
      }
      len += (1 - offset);

      Desc sum_desc;
      uint32_t *digit_mem = NULL;
      if (OB_FAIL(calc_desc_and_check_(d_.desc_,sum_desc, sum_exp, len))) {
        LOG_WARN("fail to calc_desc_and_check_", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          res.set_zero();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * len, K(ret));
      } else {
         MEMCPY(digit_mem, sum_digits + offset, sizeof(uint32_t) * len);
         res.assign(sum_desc.desc_, digit_mem);
      }
    }
  } else {
    ObNumber subtrahend = other.negate();
    ret = sub_v2_(subtrahend, res, allocator);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(res.round_scale_v2_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "add_v2 [%s] + [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::add_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
                     const bool strict_mode/*true*/, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  const bool use_oracle_mode = is_oracle_mode();
  LOG_DEBUG("add_v3", K(ret), KPC(this), K(other));
  if (OB_UNLIKELY(is_zero())) {
    ret = res.deep_copy_v3(other, allocator);
  } else if (OB_UNLIKELY(other.is_zero())) {
    ret = res.deep_copy_v3(*this, allocator);
  } else if (d_.sign_ == other.d_.sign_) {
    const int64_t this_exp = get_decode_exp(d_);
    const int64_t other_exp = get_decode_exp(other.d_);

    if (OB_UNLIKELY(this_exp >= other_exp + OB_MAX_DECIMAL_DIGIT)) {
      ret = res.deep_copy_v3(*this, allocator);
    } else if (OB_UNLIKELY(other_exp >= this_exp + OB_MAX_DECIMAL_DIGIT)) {
      ret = res.deep_copy_v3(other, allocator);
    } else {
      Desc augend_desc(d_.desc_);
      Desc addend_desc(other.d_.desc_);
      int64_t sum_exp = this_exp;
      int64_t augend_exp = this_exp;
      int64_t addend_exp = other_exp;
      uint32_t *augend_digits = digits_;
      uint32_t *addend_digits = other.digits_;

      if (this_exp < other_exp || (this_exp == other_exp && d_.len_ < other.d_.len_)) {
        //make augend is always the larger expr one
        augend_desc.desc_ = other.d_.desc_;
        addend_desc.desc_ = d_.desc_;
        augend_exp = other_exp;
        addend_exp = this_exp;
        augend_digits = other.digits_;
        addend_digits = digits_;
        sum_exp = other_exp;
      }
      int32_t offset = 1;
      uint32_t tmp_sum_digits[OB_CALC_BUFFER_SIZE];//[0] store carry bit
      uint32_t *sum_digits = NULL;
      if (strict_mode) {
        sum_digits = tmp_sum_digits;
      } else {
        if (OB_ISNULL(sum_digits = (uint32_t *)allocator.alloc(sizeof(uint32_t) * OB_CALC_BUFFER_SIZE))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * OB_CALC_BUFFER_SIZE, K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        MEMSET(sum_digits, 0, sizeof(uint32_t) * OB_CALC_BUFFER_SIZE);
        MEMCPY(sum_digits + 1, augend_digits, min(augend_desc.len_, OB_MAX_DECIMAL_DIGIT) * sizeof(uint32_t));

        //inverse traversal
        const int64_t cur_augend_exp = augend_exp - (augend_desc.len_ - 1);
        int64_t cur_addend_exp = addend_exp - (addend_desc.len_ - 1);
        int64_t cur_addend_id = addend_desc.len_ - 1;
        int64_t cur_exp = std::min(cur_augend_exp, cur_addend_exp);
        uint8_t sum_len = sum_exp - cur_exp + 1;
        if (sum_len > OB_MAX_DECIMAL_DIGIT) {
          const int64_t movex = sum_len - OB_MAX_DECIMAL_DIGIT;
          cur_exp += movex;
          cur_addend_exp += movex;
          cur_addend_id -= movex;
          sum_len = OB_MAX_DECIMAL_DIGIT;
        }

        uint32_t carry = 0;
        bool check_sum_len = true;
        for (int64_t i = sum_len; i > 0; --i, ++cur_exp) {
          uint32_t &tmp_digit = sum_digits[i];
          tmp_digit += carry;
          if (cur_exp == cur_addend_exp && cur_addend_id >= 0) {
            tmp_digit += addend_digits[cur_addend_id];
            ++cur_addend_exp;
            --cur_addend_id;
          }

          if (tmp_digit >= BASE) {
            tmp_digit -= BASE;
            carry = 1;
          } else {
            carry = 0;
          }

          if (check_sum_len) {
            if (0 == tmp_digit) {
              --sum_len;
            } else {
              check_sum_len = false;
            }
          }
        }

        if (carry != 0) {
          sum_digits[0] = 1;
          ++sum_exp;
          ++sum_len;
          offset = 0;
        }

        Desc sum_desc;
        uint32_t *digit_mem = NULL;
        if (OB_FAIL(calc_desc_and_check(d_.desc_, sum_exp, sum_len, sum_desc, use_oracle_mode))) {
          LOG_WARN("fail to calc_desc_and_check_", K(ret));
          if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
            res.set_zero();
            ret = OB_SUCCESS;
          }
        } else if (strict_mode) {
          if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * sum_len))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * sum_len, K(ret));
          } else {
            MEMCPY(digit_mem, sum_digits + offset, sizeof(uint32_t) * sum_len);
            res.assign(sum_desc.desc_, digit_mem);
          }
        } else {
          res.assign(sum_desc.desc_, sum_digits + offset);
        }
      }
    }
  } else {
    ret = sub_v3(other.negate(), res, allocator, strict_mode, do_rounding);
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && res.need_round_after_arithmetic(use_oracle_mode)
        && OB_FAIL(res.round_scale_v3_(use_oracle_mode ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "add_v3 [%s] + [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::sub_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc minuend_desc;
  Desc subtrahend_desc;
  minuend_desc.desc_ = d_.desc_;
  subtrahend_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("sub_v2_", K(ret), KPC(this), K(other));
  if (is_zero()) {
    ret = other.negate_v2_(res, allocator);
  } else if (other.is_zero()) {
    ret = res.deep_copy(*this, allocator);
  } else if (minuend_desc.sign_ == subtrahend_desc.sign_) {

    int64_t minuend_exp = get_decode_exp(minuend_desc.desc_);
    int64_t subtrahend_exp = get_decode_exp(subtrahend_desc.desc_);

    if (minuend_exp >= subtrahend_exp + OB_MAX_DECIMAL_DIGIT) {
      ret = res.deep_copy(*this, allocator);
    } else if (subtrahend_exp >= minuend_exp + OB_MAX_DECIMAL_DIGIT) {
      ret = other.negate_v2_(res, allocator);
    } else {
      int64_t sum_exp = std::max(minuend_exp, subtrahend_exp);
      int32_t sum_digits[OB_CALC_BUFFER_SIZE] = {};
      int first_digit_flag = 0;
      bool arg_negative = (NEGATIVE == minuend_desc.sign_);
      bool sub_negative = false;
      uint32_t *minuend_digits = digits_;
      uint32_t *subtrahend_digits = other.digits_;

      for (int64_t i=0, curr_exp = sum_exp, minuend_id = 0, subtrahend_id = 0;
          i <= OB_MAX_DECIMAL_DIGIT;
          ++i, --curr_exp) {
        int32_t &tmp_digit = sum_digits[i];
        if (curr_exp == minuend_exp && minuend_id < minuend_desc.len_) {
          tmp_digit += minuend_digits[minuend_id];
          --minuend_exp;
          ++minuend_id;
        }
        if (curr_exp == subtrahend_exp && subtrahend_id < subtrahend_desc.len_) {
          tmp_digit -= subtrahend_digits[subtrahend_id];
          --subtrahend_exp;
          ++subtrahend_id;
        }
        if (0 == first_digit_flag && tmp_digit != 0) {
          first_digit_flag = (tmp_digit > 0 ? 1 : -1);
        }
//        LOG_DEBUG("sub_v2 ", K(sum_digits[i]), K(i), K(curr_exp), K(minuend_exp), K(minuend_id), K(subtrahend_exp), K(subtrahend_id), K(first_digit_flag));
      }

      sub_negative = (-1 == first_digit_flag);
      if (sub_negative) {
        for (int64_t i = OB_MAX_DECIMAL_DIGIT; i >= 0; --i) {
          int32_t &tmp_digit = sum_digits[i];
          tmp_digit = 0 - tmp_digit;
          if (tmp_digit < 0) {
            tmp_digit += BASE;
            ++sum_digits[i - 1];//not -1
          }
        }
      } else {
        for (int64_t i = OB_MAX_DECIMAL_DIGIT; i >= 0; --i) {
          int32_t &tmp_digit = sum_digits[i];
          if (tmp_digit < 0) {
            tmp_digit += BASE;
            --sum_digits[i - 1];
          }
        }
      }

//      normalize_digit_(sum_digits, OB_MAX_DECIMAL_DIGIT);
      int64_t start_id = 0;
      for (; start_id <= OB_MAX_DECIMAL_DIGIT && sum_digits[start_id] == 0; ++start_id, --sum_exp);
      int32_t len = (OB_MAX_DECIMAL_DIGIT - start_id + 1);
      for (int64_t i = OB_MAX_DECIMAL_DIGIT; i >= start_id && sum_digits[i] == 0; --len, --i);

      if (start_id > OB_MAX_DECIMAL_DIGIT) {
        res.set_zero();
        ret = OB_SUCCESS;
      } else {
        uint32_t *digit_mem = NULL;
        Desc sum_desc;
        Desc tmp_desc;
        tmp_desc.desc_ = d_.desc_;
        tmp_desc.sign_ = (arg_negative == sub_negative ? POSITIVE : NEGATIVE);
        if (OB_FAIL(calc_desc_and_check_(tmp_desc.desc_, sum_desc, sum_exp, len))) {
          LOG_WARN("fail to assign desc part", K(ret));
          if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
            res.set_zero();
            ret = OB_SUCCESS;
          }
        } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * len, K(ret));
        } else {
          MEMCPY(digit_mem, sum_digits + start_id, sizeof(uint32_t) * len);
          res.assign(sum_desc.desc_, (uint32_t*)digit_mem);
        }
      }
    }
  } else {
    ObNumber addend = other.negate();
    ret = add_v2_(addend, res, allocator);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(res.round_scale_v2_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "sub_v2 [%s] - [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::sub_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
                     const bool strict_mode/*true*/, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  const bool use_oracle_mode = is_oracle_mode();
  LOG_DEBUG("sub_v3", K(ret), KPC(this), K(other));
  if (OB_UNLIKELY(is_zero())) {
    ret = other.negate_v3_(res, allocator);
  } else if (OB_UNLIKELY(other.is_zero())) {
    ret = res.deep_copy_v3(*this, allocator);
  } else if (d_.sign_ == other.d_.sign_) {
    const int64_t this_exp = get_decode_exp(d_);
    const int64_t other_exp = get_decode_exp(other.d_);

    if (OB_UNLIKELY(this_exp >= other_exp + OB_MAX_DECIMAL_DIGIT)) {
      ret = res.deep_copy_v3(*this, allocator);
    } else if (OB_UNLIKELY(other_exp >= this_exp + OB_MAX_DECIMAL_DIGIT)) {
      ret = other.negate_v3_(res, allocator);
    } else {
      int64_t start_id = 0;
      bool sub_negative = false;
      bool is_equal = false;
      if (this_exp > other_exp) {
        sub_negative = false;
      } else if (this_exp < other_exp) {
        sub_negative = true;
      } else {
        const int64_t min_id = std::min(d_.len_, other.d_.len_)  - 1;
        while (start_id <= min_id && digits_[start_id] == other.digits_[start_id]) {
          ++start_id;
        }
        if (start_id > min_id) {
          if (d_.len_ == other.d_.len_) {
            is_equal = true;
          } else {
            sub_negative = (d_.len_ < other.d_.len_);
          }
        } else {
          sub_negative = (digits_[start_id] < other.digits_[start_id]);
        }
      }
//      LOG_DEBUG("sub_v3", K(is_equal), K(sub_negative), K(start_id));

      if (is_equal) {
        res.set_zero();
      } else {
        Desc minuend_desc(d_.desc_);
        Desc subtrahend_desc(other.d_.desc_);
        int64_t minuend_exp = this_exp;
        int64_t subtrahend_exp = other_exp;
        uint32_t *minuend_digits = digits_;
        uint32_t *subtrahend_digits = other.digits_;
        if (sub_negative) {
          //always make the minuend is larger one
          minuend_desc.desc_ = other.d_.desc_;
          subtrahend_desc.desc_ = d_.desc_;
          minuend_exp = other_exp;
          subtrahend_exp = this_exp;
          minuend_digits = other.digits_;
          subtrahend_digits = digits_;
        }

        int32_t tmp_sum_digits[OB_CALC_BUFFER_SIZE];
        int32_t *sum_digits = NULL;
        if (strict_mode) {
          sum_digits = tmp_sum_digits;
        } else {
          if (OB_ISNULL(sum_digits = (int32_t *)allocator.alloc(sizeof(uint32_t) * OB_CALC_BUFFER_SIZE))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * OB_CALC_BUFFER_SIZE, K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          MEMSET(sum_digits, 0, sizeof(uint32_t) * OB_CALC_BUFFER_SIZE);
          MEMCPY(sum_digits + start_id, minuend_digits + start_id, (minuend_desc.len_ - start_id) * sizeof(uint32_t));

          int64_t sum_exp = minuend_exp;
          const int64_t cur_minuend_exp = minuend_exp - (minuend_desc.len_ - 1);
          int64_t cur_subtrahend_exp = subtrahend_exp - (subtrahend_desc.len_ - 1);
          int64_t cur_subtrahend_id = subtrahend_desc.len_ - 1;
          int64_t cur_exp = std::min(cur_minuend_exp, cur_subtrahend_exp);
          uint8_t sum_len = sum_exp - cur_exp + 1;
          if (sum_len > OB_MAX_DECIMAL_DIGIT) {
            const int64_t movex = sum_len - OB_MAX_DECIMAL_DIGIT;
            cur_exp += movex;
            cur_subtrahend_exp += movex;
            cur_subtrahend_id -= movex;
            sum_len = OB_MAX_DECIMAL_DIGIT;
          }

//          LOG_DEBUG("sub_v3", K(cur_subtrahend_exp), K(cur_subtrahend_id), K(sum_exp), K(cur_exp), K(sum_len));

          int32_t carry = 0;
          bool check_sum_len = true;
          for (int64_t i = sum_len - 1; i >= start_id; --i, ++cur_exp) {
            int32_t &tmp_digit = sum_digits[i];
//            LOG_DEBUG("sub_v3", K(i), K(cur_exp), K(tmp_digit), K(carry), K(cur_subtrahend_exp), K(cur_subtrahend_id), K(subtrahend_digits[cur_subtrahend_id]));
            tmp_digit += carry;

            if (cur_exp == cur_subtrahend_exp && cur_subtrahend_id >= start_id) {
              tmp_digit -= subtrahend_digits[cur_subtrahend_id--];
              ++cur_subtrahend_exp;
            }

            if (tmp_digit < 0) {
              tmp_digit += BASE;
              carry = -1;
            } else {
              carry = 0;
            }

            if (check_sum_len) {
              if (0 == tmp_digit) {
                --sum_len;
              } else {
                check_sum_len = false;
              }
            }
          }

          sum_digits[sum_len] = BASE;//end flag
          int32_t *tmp_digit = sum_digits + start_id;
          while (0 == *tmp_digit) {
            ++tmp_digit;
          }
          int64_t new_start_id = tmp_digit -  sum_digits;
          sum_len -= new_start_id;
          const int64_t new_sum_exp = sum_exp - new_start_id;
//          LOG_DEBUG("sub_v3", K(start_id), K(new_start_id), K(sum_len), K(new_sum_exp));

          if (sum_len <= 0) {
            res.set_zero();
          } else {
            uint32_t *digit_mem = NULL;
            Desc sum_desc(d_);
            sum_desc.sign_ = ((NEGATIVE == minuend_desc.sign_) == sub_negative ? POSITIVE : NEGATIVE);
            if (OB_FAIL(calc_desc_and_check(sum_desc.desc_, new_sum_exp, sum_len, sum_desc, use_oracle_mode))) {
              LOG_WARN("fail to assign desc part", K(ret));
              if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
                res.set_zero();
                ret = OB_SUCCESS;
              }
            } else if (strict_mode) {
              if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * sum_len))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * sum_len, K(ret));
              } else {
                MEMCPY(digit_mem, sum_digits + new_start_id, sizeof(uint32_t) * sum_len);
                res.assign(sum_desc.desc_, (uint32_t*)digit_mem);
              }
            } else {
              res.assign(sum_desc.desc_, (uint32_t*)(sum_digits + new_start_id));
            }
          }
        }
      }
    }
  } else {
    ret = add_v3(other.negate(), res, allocator, strict_mode, do_rounding);
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && res.need_round_after_arithmetic(use_oracle_mode)
        && OB_FAIL(res.round_scale_v3_(use_oracle_mode ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "sub_v3 [%s] - [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}


int ObNumber::negate_(ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  if (is_zero()) {
    res.set_zero();
  } else {
    ObCalcVector cv;
    if (OB_FAIL(cv.init(d_.desc_, digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else {
      int64_t size2alloc = d_.len_;
      if (POSITIVE == d_.sign_) {
        res.d_.sign_ = NEGATIVE;
      } else {
        res.d_.sign_ = POSITIVE;
      }
      res.d_.exp_ = (0x7f & ~d_.exp_);
      ++res.d_.exp_;

      if (OB_ISNULL(res.digits_ = res.alloc_(allocator, size2alloc))) {
        _OB_LOG(WARN, "alloc digits fail, length=%ld", size2alloc);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(res.normalize_(cv.get_digits(), cv.size()))) {
        _OB_LOG(WARN, "normalize [%s] fail ret=%d", to_cstring(res), ret);
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "negate [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

int ObNumber::negate_v2_(ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  if (is_zero()) {
    res.set_zero();
  } else {
    res = this->negate();
    if (OB_ISNULL(res.digits_ = res.alloc_(allocator, d_.len_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc digits fail", K(d_.len_), K(ret));
    } else {
      MEMCPY(res.digits_, digits_, d_.len_ * sizeof(uint32_t));
    }
  }

  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "negate_v2 [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

int ObNumber::negate_v3_(ObNumber &value, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  if (is_zero()) {
    res.set_zero();
  } else {
    res = this->negate();
    if (OB_ISNULL(res.digits_ = (uint32_t *)allocator.alloc(d_.len_ * sizeof(uint32_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc digits fail", K(d_.len_), K(ret));
    } else {
      MEMCPY(res.digits_, digits_, d_.len_ * sizeof(uint32_t));
    }
  }

  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "negate_v3 [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

int ObNumber::mul_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc multiplicand_desc;
  Desc multiplier_desc;
  multiplicand_desc.desc_ = d_.desc_;
  multiplier_desc.desc_ = other.d_.desc_;
  if (is_zero() || other.is_zero()) {
    res.set_zero();
  } else {
    ObCalcVector multiplicand;
    ObCalcVector multiplier;
    if (OB_FAIL(multiplicand.init(multiplicand_desc.desc_, digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else if (OB_FAIL(multiplier.init(multiplier_desc.desc_, other.digits_))) {
      LOG_WARN("fail to assign values", K(ret));
    } else {
      ObCalcVector product;
      int64_t product_size = multiplicand.size() + multiplier.size();
      if (OB_FAIL(product.ensure(product_size))) {
        LOG_WARN("product.ensure(product_size) fails", K(ret));
      } else if (OB_FAIL(poly_poly_mul(multiplicand, multiplier, product))) {
        _OB_LOG(WARN, "[%s] mul [%s] fail, ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_mul_(multiplicand_desc, multiplier_desc);
        bool carried = (0 != product.at(0));
        exp_shift_((carried ? 1 : 0), res_desc);
        ret = res.from_v2_(res_desc.desc_, product, allocator);
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "mul [%s] * [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::mul_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator,
                      const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc multiplicand_desc;
  Desc multiplier_desc;
  multiplicand_desc.desc_ = d_.desc_;
  multiplier_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("mul_v2_", K(ret), KPC(this), K(other));
  if (is_zero() || other.is_zero()) {
    res.set_zero();
  } else {
    int64_t sum_exp = get_decode_exp(multiplicand_desc.desc_) + get_decode_exp(multiplier_desc.desc_);
    uint32_t *multiplicand_digits = digits_;
    uint32_t *multiplier_digits = other.digits_;
    uint64_t sum_digits[OB_MULT_BUFFER_SIZE] = {};
    int64_t multiplicand_len = multiplicand_desc.len_;
    int64_t multiplier_len = multiplier_desc.len_;
    int64_t sum_len = multiplicand_len + multiplier_len;
    int64_t digits_len = 0;
    int offset = 0;

    for (int64_t i = 0; i < multiplicand_len; ++i) {
      for (int64_t j = 0; j < multiplier_len; ++j) {
        int64_t k = i + j + 1;
        sum_digits[k] += (uint64_t)multiplicand_digits[i] * multiplier_digits[j];
        if (sum_digits[k] >= CARRY_BOUND) {
          uint64_t carry = 0;
          for (int64_t id = k; id >= 0; --id) {
            uint64_t tmp = carry + sum_digits[id];
            carry = tmp / BASE;
            sum_digits[id] = tmp - carry * BASE;
            if (0 == carry) {
              break;
            }
          }
        }
      }
    }

    for (int64_t i = sum_len - 1; i >= 1; --i) {
      if (sum_digits[i] >= BASE) {
        int64_t tmp = sum_digits[i] / BASE;
        sum_digits[i] -= tmp * BASE;
        sum_digits[i-1] += tmp;
      }
    }

    if (sum_digits[0] != 0) {
      ++sum_exp;
    } else {
      offset = 1;
    }
    while ((sum_len - 1) >= offset && sum_digits[sum_len - 1] == 0) {
      --sum_len;
    }
    digits_len = sum_len - offset;

    if (digits_len > OB_MAX_DECIMAL_DIGIT) {
      digits_len = OB_MAX_DECIMAL_DIGIT;
//      normalize_digit_(sum_digits + offset, OB_MAX_DECIMAL_DIGIT);
    }

    Desc sum_desc, tmp_desc;
    tmp_desc.desc_ = d_.desc_;
    tmp_desc.sign_ = (multiplicand_desc.sign_ == multiplier_desc.sign_ ? POSITIVE : NEGATIVE);
    uint32_t *digit_mem = NULL;
    if (OB_FAIL(calc_desc_and_check_(tmp_desc.desc_, sum_desc, sum_exp, (uint8_t)digits_len))) {
      LOG_WARN("fail to assign desc part", K(ret));
      if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
        res.set_zero();
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * digits_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * digits_len, K(ret));
    } else {
      for (int64_t i = 0; i < digits_len; ++i) {
        digit_mem[i] = static_cast<uint32_t>(sum_digits[i + offset]);
      }
      res.assign(sum_desc.desc_, digit_mem);
    }
  }
  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
      } else {
        value = res;
      }
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "mul_v2 [%s] * [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::mul_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
    const bool strict_mode/*true*/, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  const bool use_oracle_mode = is_oracle_mode();
  LOG_DEBUG("mul_v3_", K(ret), KPC(this), K(other));
  if (is_zero() || other.is_zero()) {
    res.set_zero();
  } else {
    Desc multiplicand_desc(d_.desc_);
    Desc multiplier_desc(other.d_.desc_);
    const uint32_t *multiplicand_digits = digits_;
    const uint32_t *multiplier_digits = other.digits_;
    uint64_t sum_digits[OB_MULT_BUFFER_SIZE] = {};
    int64_t multiplicand_len = multiplicand_desc.len_;
    int64_t multiplier_len = multiplier_desc.len_;

    /* 保证 for 循环先循环长度较小的 */
    if (d_.len_ > other.d_.len_) {
      multiplicand_digits = other.digits_;
      multiplier_digits = digits_;
      multiplicand_len = multiplier_desc.len_;
      multiplier_len = multiplicand_desc.len_;
    }
    int64_t sum_exp = get_decode_exp(multiplicand_desc.desc_) + get_decode_exp(multiplier_desc.desc_);
    int64_t sum_len = multiplier_desc.len_ + multiplicand_desc.len_;
    int64_t digits_len = 0;
    int offset = 0;
    bool check_sum_len = true;

    uint32_t tmp_res_digits[OB_CALC_BUFFER_SIZE];
    uint32_t *res_digits = NULL;
    if (strict_mode) {
      res_digits = tmp_res_digits;
    } else {
      if (OB_ISNULL(res_digits = (uint32_t *)allocator.alloc(sizeof(uint32_t) * OB_CALC_BUFFER_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * OB_CALC_BUFFER_SIZE, K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (sum_len <= OB_MAX_DECIMAL_DIGIT) {
        MEMSET(res_digits + sum_len, 0, sizeof(uint32_t) * (OB_MAX_DECIMAL_DIGIT - sum_len + 1));
      }

      const uint32_t *multiplicand_di = multiplicand_digits;
      for (int64_t i = 0; i < multiplicand_len; ++i) {
        const uint64_t tmp_multiplicand_di = static_cast<uint64_t>(*multiplicand_di++);
        const uint32_t *tmp_multiplier_di = multiplier_digits;
        uint64_t *sum_di = sum_digits + (i + 1);
        for (int64_t j = 0; j < multiplier_len; ++j) {
          (*sum_di++) += tmp_multiplicand_di * (*tmp_multiplier_di++);
        }
      }


      uint64_t carry = 0;
      for (int64_t i = sum_len - 1; i >= 0; --i) {
        uint64_t tmp_digit = sum_digits[i];
        tmp_digit += carry;
        if (tmp_digit >= BASE) {
          carry = tmp_digit / BASE;
          tmp_digit -= carry * BASE;
        } else {
          carry = 0;
        }
        if (check_sum_len) {
          if (0 == tmp_digit) {
            -- sum_len;
          } else {
            check_sum_len = false;
          }
        }
        if (i <= OB_MAX_DECIMAL_DIGIT) {
          res_digits[i] = static_cast<uint32_t>(tmp_digit);
        }
      }

      if (res_digits[0] != 0) {
        ++sum_exp;
      }  else {
        offset = 1;
      }

      digits_len = sum_len - offset;
      if (digits_len > OB_MAX_DECIMAL_DIGIT) {
        digits_len = OB_MAX_DECIMAL_DIGIT;
      }

      Desc sum_desc(d_);
      sum_desc.sign_ = (multiplicand_desc.sign_ == multiplier_desc.sign_ ? POSITIVE : NEGATIVE);
      uint32_t *digit_mem = NULL;
      if (OB_FAIL(calc_desc_and_check(sum_desc.desc_, sum_exp, (uint8_t)digits_len, sum_desc, use_oracle_mode))) {
        LOG_WARN("fail to assign desc part", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          res.set_zero();
          ret = OB_SUCCESS;
        }
      } else if (strict_mode) {
        if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * digits_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * digits_len, K(ret));
        } else {
          MEMCPY(digit_mem, res_digits + offset, sizeof(uint32_t) * digits_len);
          res.assign(sum_desc.desc_, digit_mem);
        }
      } else {
        res.assign(sum_desc.desc_, res_digits + offset);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && res.need_round_after_arithmetic(use_oracle_mode)
        && OB_FAIL(res.round_scale_v3_(use_oracle_mode ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "mul_v3 [%s] * [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}


//int ObNumber::div_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
//{
//  int ret = OB_SUCCESS;
//  ObNumber res;
//  Desc dividend_desc;
//  Desc divisor_desc;
//  dividend_desc.desc_ = d_.desc_;
//  divisor_desc.desc_ = other.d_.desc_;
//  if (OB_UNLIKELY(other.is_zero())) {
//    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
//    ret = OB_DIVISION_BY_ZERO;
//  } else if (is_zero()) {
//    res.set_zero();
//  } else {
//    ObCalcVector dividend;
//    ObCalcVector divisor;
//    int64_t shift = get_extend_length_remainder_(dividend_desc, divisor_desc);
//    shift = (MAX_STORE_LEN <= shift) ? shift : (MAX_STORE_LEN - shift);
//    shift += get_decimal_extend_length_(dividend_desc);
//    exp_shift_(shift, dividend_desc);
//    if (OB_FAIL(dividend.init(dividend_desc.desc_, digits_))) {
//      LOG_WARN("fail to assign values", K(ret));
//    } else if (OB_FAIL(divisor.init(divisor_desc.desc_, other.digits_))) {
//      LOG_WARN("fail to assign values", K(ret));
//    } else {
//      ObCalcVector quotient;
//      ObCalcVector remainder;
//      int64_t quotient_size = dividend.size() - divisor.size() + 1;
//      int64_t remainder_size = divisor.size();
//      if (OB_FAIL(quotient.ensure(quotient_size))) {
//        LOG_WARN("quotient.ensure(quotient_size) fails", K(ret));
//      } else if (OB_FAIL(remainder.ensure(remainder_size))) {
//        LOG_WARN("remainder.ensure(remainder_size) fails", K(ret));
//      } else if (OB_FAIL(poly_poly_div(dividend, divisor, quotient, remainder))) {
//        _OB_LOG(WARN, "[%s] div [%s] fail ret=%d", to_cstring(*this), to_cstring(other), ret);
//      } else {
//        Desc res_desc = exp_div_(dividend_desc, divisor_desc);
//        for (int64_t i = 0; i < quotient.size(); ++i) {
//          if (0 != quotient.at(i)) {
//            break;
//          }
//          ++shift;
//        }
//        exp_shift_(-shift, res_desc);
//        ret = res.from_v2_(res_desc.desc_, quotient, allocator);
//      }
//    }
//  }
//  if (OB_SUCC(ret)) {
//    value = res;
//  }
//  _OB_LOG(DEBUG, "div: [%s] / [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
//  return ret;
//}

int ObNumber::div_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator, const int32_t qscale,
                      const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc dividend_desc;
  Desc divisor_desc;
  dividend_desc.desc_ = d_.desc_;
  divisor_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("div_v2_", K(ret), KPC(this), K(other));
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else {
    /* remove ObCalcVector */
    ObDivArray dividend, divisor, quotient;
    int32_t extend_len = other.d_.len_ + qscale;
    extend_len = std::min(extend_len, +OB_DIV_BUFFER_SIZE);
    dividend.from(digits_, d_.len_, std::min(+OB_DIV_BUFFER_SIZE, extend_len));
    divisor.from(other.digits_, other.d_.len_, other.d_.len_);
    dividend.div(divisor, quotient);

    if (quotient.is_zero()) {
      res.set_zero();
    } else {
      int64_t sum_exp = get_decode_exp(dividend_desc.desc_) - get_decode_exp(divisor_desc.desc_);
      uint32_t sum_digits[OB_MULT_BUFFER_SIZE] = {};
      quotient.get_uint32_digits(sum_digits, OB_MULT_BUFFER_SIZE);

      int32_t offset = 0;
      int32_t sum_len = quotient.len_;
      while (offset < quotient.len_ && sum_digits[offset] == 0) {
        ++offset;
        --sum_exp;
        --sum_len;
      }
      if (sum_len > OB_MAX_DECIMAL_DIGIT) {
        sum_len = OB_MAX_DECIMAL_DIGIT;
//        normalize_digit_(sum_digits + offset, sum_len);
      }
      int32_t end = offset + sum_len - 1; // remove tailing zero
      while (sum_digits[end] == 0) {
        --end;
        --sum_len;
      }

      Desc tmp_desc, sum_desc;
      uint32_t *digit_mem = NULL;
      tmp_desc.desc_ = d_.desc_;
      tmp_desc.sign_ = (dividend_desc.sign_ == divisor_desc.sign_ ? POSITIVE : NEGATIVE);
      if (OB_FAIL(calc_desc_and_check_(tmp_desc.desc_, sum_desc, sum_exp, (uint8_t)sum_len))) {
        LOG_WARN("fail to assign desc part", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          res.set_zero();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * sum_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * sum_len, K(ret));
      } else {
        MEMCPY(digit_mem, sum_digits + offset, sizeof(uint32_t) * sum_len);
        res.assign(sum_desc.desc_, digit_mem);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
      } else {
        value = res;
      }
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "div_v2: [%s] / [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::div_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
    const int32_t qscale/*OB_MAX_DECIMAL_DIGIT*/, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  const bool use_oracle_mode = is_oracle_mode();
  LOG_DEBUG("div_v3_", K(ret), KPC(this), K(other));
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else {
    Desc dividend_desc(d_);
    Desc divisor_desc(other.d_);

    ObDivArray dividend;
    ObDivArray divisor;
    ObDivArray quotient;
    int32_t extend_len = other.d_.len_ + qscale;
    extend_len = std::min(extend_len, +OB_DIV_BUFFER_SIZE);
    dividend.from(digits_, d_.len_, std::min(+OB_DIV_BUFFER_SIZE, extend_len));
    divisor.from(other.digits_, other.d_.len_, other.d_.len_);
    dividend.div_v2(divisor, quotient);

    if (quotient.is_zero()) {
      res.set_zero();
    } else {
      int64_t sum_exp = get_decode_exp(dividend_desc.desc_) - get_decode_exp(divisor_desc.desc_);
      uint32_t sum_digits[OB_MULT_BUFFER_SIZE] = {};
      quotient.get_uint32_digits(sum_digits, OB_MULT_BUFFER_SIZE);

      int32_t offset = 0;
      int32_t sum_len = quotient.len_;
      while (offset < quotient.len_ && sum_digits[offset] == 0) {
        ++offset;
        --sum_exp;
        --sum_len;
      }
      if (sum_len > OB_MAX_DECIMAL_DIGIT) {
        sum_len = OB_MAX_DECIMAL_DIGIT;
      }
      const int32_t end = offset + sum_len - 1; // remove tailing zero
      uint32_t *tmp_sum_digits = sum_digits + end;
      while (*tmp_sum_digits == 0) {
        --tmp_sum_digits;
      }
      sum_len -= (sum_digits + end - tmp_sum_digits);

      uint32_t *digit_mem = NULL;
      Desc sum_desc(d_);
      sum_desc.sign_ = (dividend_desc.sign_ == divisor_desc.sign_ ? POSITIVE : NEGATIVE);
      if (OB_FAIL(calc_desc_and_check(sum_desc.desc_, sum_exp, (uint8_t)sum_len, sum_desc, use_oracle_mode))) {
        LOG_WARN("fail to assign desc part", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          res.set_zero();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * sum_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * sum_len, K(ret));
      } else {
        MEMCPY(digit_mem, sum_digits + offset, sizeof(uint32_t) * sum_len);
        res.assign(sum_desc.desc_, digit_mem);
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("round scale before", K(res));
    if (do_rounding && res.need_round_after_arithmetic(use_oracle_mode)
        && OB_FAIL(res.round_scale_v3_(use_oracle_mode ? MAX_SCALE : FLOATING_SCALE, true, false))) {
      LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "div_v3: [%s] / [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

//sinh(x)=(e^x - e^(-x)) / 2
int ObNumber::sinh(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  const int64_t LOCAL_ALLOCATE_TIME = 4;
  char buf_alloc_local[ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME];
  ObDataBuffer allocator_local(buf_alloc_local, ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME);
  ObNumber res;
  ObNumber e_power_x;
  ObNumber e_power_negate_x;
  const ObNumber const_zero_dot_five = get_positive_zero_dot_five();

  if (OB_FAIL(e_power(e_power_x, allocator_local, false))) {
    LOG_WARN("calc power(e,x) falied", K(ret), K(*this));
  } else if (e_power_x.is_zero()) {
    ObNumber negate_x;
    if (OB_FAIL(negate(negate_x, allocator_local))) {
      LOG_WARN("negate x failed", K(ret), K(*this));
    } else if (OB_FAIL(negate_x.e_power(e_power_negate_x, allocator_local, false))) {
      LOG_WARN("calc power(e,-x) failed", K(ret), K(negate_x));
    } else if (OB_FAIL(e_power_negate_x.mul_v3(const_zero_dot_five, res, allocator_local, true, false))) {
      LOG_WARN("e_power_negate_x mul 0.5 failed", K(ret), K(e_power_negate_x));
    } else if (OB_FAIL(res.negate(res, allocator))) {
      LOG_WARN("res negate failed", K(ret), K(res));
    }
  } else {
    const ObNumber const_one = get_positive_one();
    if (OB_FAIL(const_one.div_v3(e_power_x, e_power_negate_x, allocator_local, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("calc power(e,-x) by 1/power(e,x) failed", K(ret), K(e_power_x));
    } else if (OB_FAIL(e_power_x.sub_v3(e_power_negate_x, res, allocator_local, true, false))) {
      LOG_WARN("e_power_x add e_power_negate_x failed", K(ret), K(e_power_x), K(e_power_negate_x));
    } else if (OB_FAIL(res.mul_v3(const_zero_dot_five, res, allocator, true, false))) {
      LOG_WARN("res mul 0.5 failed", K(ret), K(res));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "sinh [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

//cosh(x)=(e^x + e^(-x)) / 2
int ObNumber::cosh(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  const int64_t LOCAL_ALLOCATE_TIME = 3;
  char buf_alloc_local[ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME];
  ObDataBuffer allocator_local(buf_alloc_local, ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME);
  ObNumber res;
  ObNumber e_power_x;
  ObNumber e_power_negate_x;
  const ObNumber const_zero_dot_five = get_positive_zero_dot_five();

  if (OB_FAIL(e_power(e_power_x, allocator_local, false))) {
    LOG_WARN("calc power(e,x) falied", K(ret), K(*this));
  } else if (e_power_x.is_zero()) {
    ObNumber negate_x;
    if (OB_FAIL(negate(negate_x, allocator_local))) {
      LOG_WARN("negate x failed", K(ret), K(*this));
    } else if (OB_FAIL(negate_x.e_power(e_power_negate_x, allocator_local, false))) {
      LOG_WARN("calc power(e,-x) failed", K(ret), K(negate_x));
    } else if (OB_FAIL(e_power_negate_x.mul_v3(const_zero_dot_five, res, allocator, true, false))) {
      LOG_WARN("e_power_negate_x mul 0.5 failed", K(ret), K(e_power_negate_x));
    }
  } else {
    const ObNumber const_one = get_positive_one();
    ObNumber sum;
    if (OB_FAIL(const_one.div_v3(e_power_x, e_power_negate_x, allocator_local, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("calc power(e,-x) by 1/power(e,x) failed", K(ret), K(e_power_x));
    } else if (OB_FAIL(e_power_x.add_v3(e_power_negate_x, sum, allocator_local, true, false))) {
      LOG_WARN("e_power_x add e_power_negate_x failed", K(ret), K(e_power_x), K(e_power_negate_x));
    } else if (OB_FAIL(sum.mul_v3(const_zero_dot_five, res, allocator, true, false))) {
      LOG_WARN("sum mul 0.5 failed", K(ret), K(sum));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "cosh [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

//tanh(x)= sinh(x) / cosh(x) = (e^x - e^(-x)) / (e^x + e^(-x))
int ObNumber::tanh(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  const int64_t LOCAL_ALLOCATE_TIME = 4;
  char buf_alloc_local[ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME];
  ObDataBuffer allocator_local(buf_alloc_local, ObNumber::MAX_BYTE_LEN * LOCAL_ALLOCATE_TIME);
  ObNumber res;
  ObNumber e_power_x;
  ObNumber e_power_negate_x;

  if (OB_FAIL(e_power(e_power_x, allocator_local, false))) {
    LOG_WARN("calc power(e,x) falied", K(ret), K(*this));
  } else if (e_power_x.is_zero()) {
    ObNumber negate_x;
    if (OB_FAIL(negate(negate_x, allocator_local))) {
      LOG_WARN("negate x failed", K(ret), K(*this));
    } else if (OB_FAIL(negate_x.e_power(e_power_negate_x, allocator_local, false))) {
      LOG_WARN("calc power(e,-x) failed", K(ret), K(negate_x));
    } else if (OB_FAIL(res.from("-1", allocator))) {
      LOG_WARN("result from -1 failed", K(ret));
    }
  } else {
    const ObNumber const_one = get_positive_one();
    ObNumber sum;
    ObNumber diff;
    if (OB_FAIL(const_one.div_v3(e_power_x, e_power_negate_x, allocator_local, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("calc power(e,-x) by 1/power(e,x) failed", K(ret), K(e_power_x));
    } else if (OB_FAIL(e_power_x.add_v3(e_power_negate_x, sum, allocator_local, true, false))) {
      LOG_WARN("e_power_x add e_power_negate_x failed", K(ret), K(e_power_x), K(e_power_negate_x));
    } else if (OB_FAIL(e_power_x.sub_v3(e_power_negate_x, diff, allocator_local, true, false))) {
      LOG_WARN("e_power_x sub e_power_negate_x failed", K(ret), K(e_power_x), K(e_power_negate_x));
    } else if (OB_FAIL(diff.div_v3(sum, res, allocator, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("diff div sum failed", K(ret), K(diff), K(sum));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(res.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("round scale fail", K(ret), K(res));
    } else {
      value = res;
    }
  }
  _OB_LOG(DEBUG, "tanh [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

//int ObNumber::rem_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
//{
//  int ret = OB_SUCCESS;
//  ObNumber res;
//  Desc dividend_desc;
//  Desc divisor_desc;
//  dividend_desc.desc_ = d_.desc_;
//  divisor_desc.desc_ = other.d_.desc_;
//  int cmp_ret = 0;
//  if (OB_UNLIKELY(other.is_zero())) {
//    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
//    ret = OB_DIVISION_BY_ZERO;
//  } else if (is_zero()) {
//    res.set_zero();
//  } else if (0 >= (cmp_ret = abs_compare(other))) {
//    if (0 == cmp_ret) {
//      res.set_zero();
//    } else {
//      res.from(*this, allocator);
//    }
//  } else {
//    int64_t shift = std::max(get_decimal_extend_length_(dividend_desc),
//                             get_decimal_extend_length_(divisor_desc));
//    exp_shift_(shift, dividend_desc);
//    exp_shift_(shift, divisor_desc);
//
//    ObCalcVector dividend;
//    ObCalcVector divisor;
//    if (OB_FAIL(dividend.init(dividend_desc.desc_, digits_))) {
//      LOG_WARN("fail to assign values", K(ret));
//    } else if (OB_FAIL(divisor.init(divisor_desc.desc_, other.digits_))) {
//      LOG_WARN("fail to assign values", K(ret));
//    } else {
//      ObCalcVector dividend_amplify;
//      ObCalcVector *dividend_ptr = NULL;
//      if (dividend.size() < divisor.size()) {
//        _OB_LOG(WARN, "dividend_size=%ld must not less than divisor_size=%ld", dividend.size(),
//                divisor.size());
//        ret = OB_ERR_UNEXPECTED;
//      } else if (dividend.size() > divisor.size()) {
//        dividend_ptr = &dividend;
//      } else {
//        ObCalcVector divisor_amplify;
//        if (OB_FAIL(divisor_amplify.ensure(divisor.size() + 1))) {
//          LOG_WARN("divisor_amplify.ensure() fails", K(ret));
//        } else if (OB_FAIL(poly_mono_mul(divisor, BASE - 1, divisor_amplify))) {
//          _OB_LOG(WARN, "[%s] mul [%lu] fail, ret=%d", to_cstring(divisor), BASE - 1, ret);
//        } else {
//          int64_t sum_size = std::max(dividend.size(), divisor_amplify.size()) + 1;
//          if (OB_FAIL(dividend_amplify.ensure(sum_size))) {
//            LOG_WARN("ensure() fails", K(ret));
//          } else if (OB_FAIL(poly_poly_add(dividend, divisor_amplify, dividend_amplify))) {
//            _OB_LOG(WARN, "[%s] add [%s] fail, ret=%d",
//                    to_cstring(dividend), to_cstring(divisor_amplify), ret);
//          } else {
//            dividend_ptr = &dividend_amplify;
//          }
//        }
//      }
//      if (OB_SUCC(ret)) {
//        ObCalcVector quotient;
//        ObCalcVector remainder;
//        int64_t quotient_size = dividend_ptr->size() - divisor.size() + 1;
//        int64_t remainder_size = divisor.size();
//        if (OB_FAIL(quotient.ensure(quotient_size))) {
//          LOG_WARN("ensure() fails", K(ret));
//        } else if (OB_FAIL(remainder.ensure(remainder_size))) {
//          LOG_WARN("ensure() fails", K(ret));
//        } else if (OB_FAIL(poly_poly_div(*dividend_ptr, divisor, quotient, remainder))) {
//          _OB_LOG(WARN, "[%s] div [%s] fail ret=%d",
//                  to_cstring(*dividend_ptr), to_cstring(divisor), ret);
//        } else {
//          Desc res_desc = exp_rem_(dividend_desc, divisor_desc);
//          for (int64_t i = 0; i < remainder.size() - 1; ++i) {
//            if (0 != remainder.at(i)) {
//              break;
//            }
//            ++shift;
//          }
//          exp_shift_(-shift, res_desc);
//          ret =  res.from_v2_(res_desc.desc_, remainder, allocator);
//        }
//      }
//    }
//  }
//  if (OB_SUCC(ret)) {
//    value = res;
//  }
//  _OB_LOG(DEBUG, "[%s] %% [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
//  return ret;
//}

int ObNumber::rem_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc dividend_desc(d_);
  Desc divisor_desc(other.d_);
  LOG_DEBUG("rem_v2_", K(ret), KPC(this), K(other));
  int cmp_ret = 0;
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else if (0 >= (cmp_ret = abs_compare(other))) {
    if (0 == cmp_ret) {
      res.set_zero();
    } else {
      res.from(*this, allocator);
    }
  } else { // promise : dividend > divisor
    // remove ObCalcVector
    ObDivArray dividend, divisor, rem;
    int32_t dividend_move = d_.len_ - 1;
    int32_t dividend_exp = get_decode_exp(dividend_desc.desc_) - dividend_move;
    int32_t divisor_move = other.d_.len_ - 1;
    int32_t divisor_exp = get_decode_exp(divisor_desc.desc_) - divisor_move;
    dividend_move += std::max(0, dividend_exp - divisor_exp);
    divisor_move += std::max(0, divisor_exp - dividend_exp);

    dividend.from(digits_, d_.len_, dividend_move + 1);
    divisor.from(other.digits_, other.d_.len_, divisor_move + 1);

    dividend.rem(divisor, rem);

    if (rem.is_zero()) {
      res.set_zero();
    } else {
      int64_t sum_exp = get_decode_exp(divisor_desc.desc_);
      uint32_t sum_digits[OB_REM_BUFFER_SIZE] = {};
      rem.get_uint32_digits(sum_digits, OB_REM_BUFFER_SIZE);
      int32_t offset = 0;
      int32_t sum_len = rem.len_;
      while (offset < rem.len_ && sum_digits[offset] == 0) {
        ++offset;
        --sum_exp;
        --sum_len;
      }
      if (sum_len > OB_MAX_DECIMAL_DIGIT) {
        sum_len = OB_MAX_DECIMAL_DIGIT;
//        normalize_digit_(sum_digits + offset, OB_MAX_DECIMAL_DIGIT);
      }
      int32_t end = offset + sum_len - 1;
      while (sum_digits[end] == 0) {
        --end;
        --sum_len;
      }
      uint32_t *digit_mem = NULL;
      Desc sum_desc = exp_rem_(dividend_desc, divisor_desc);
      if (OB_FAIL(calc_desc_and_check(sum_desc.desc_, sum_exp, (uint8_t)sum_len, sum_desc, is_oracle_mode()))) {
        LOG_WARN("fail to assign desc part", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          res.set_zero();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * sum_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * sum_len, K(ret));
      } else {
        MEMCPY(digit_mem, sum_digits + offset, sizeof(uint32_t) * sum_len);
        res.assign(sum_desc.desc_, (uint32_t*)digit_mem);
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "rem_v2: [%s] %% [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}


int ObNumber::rem_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc dividend_desc;
  Desc divisor_desc;
  dividend_desc.desc_ = d_.desc_;
  divisor_desc.desc_ = other.d_.desc_;
  LOG_DEBUG("rem_v3_", K(ret), KPC(this), K(other));
  int cmp_ret = 0;
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else if (0 >= (cmp_ret = abs_compare(other))) {
    if (0 == cmp_ret) {
      res.set_zero();
    } else {
      res.deep_copy_v3(*this, allocator);
    }
  } else { // promise : dividend > divisor
    // remove ObCalcVector
    ObDivArray dividend, divisor, rem;
    int32_t dividend_move = d_.len_ - 1;
    int32_t dividend_exp = get_decode_exp(dividend_desc.desc_) - dividend_move;
    int32_t divisor_move = other.d_.len_ - 1;
    int32_t divisor_exp = get_decode_exp(divisor_desc.desc_) - divisor_move;
    dividend_move += std::max(0, dividend_exp - divisor_exp);
    divisor_move += std::max(0, divisor_exp - dividend_exp);

    dividend.from(digits_, d_.len_, dividend_move + 1);
    divisor.from(other.digits_, other.d_.len_, divisor_move + 1);

    dividend.rem(divisor, rem);
    if (rem.is_zero()) {
      res.set_zero();
    } else {
      int64_t sum_exp = get_decode_exp(divisor_desc.desc_);
      uint32_t sum_digits[OB_REM_BUFFER_SIZE] = {};
      rem.get_uint32_digits(sum_digits, OB_REM_BUFFER_SIZE);
      int32_t offset = 0;
      int32_t sum_len = rem.len_;
      while (offset < rem.len_ && sum_digits[offset] == 0) {
        ++offset;
        --sum_exp;
        --sum_len;
      }
      if (sum_len > OB_MAX_DECIMAL_DIGIT) {
        sum_len = OB_MAX_DECIMAL_DIGIT;
//        normalize_digit_(sum_digits + offset, OB_MAX_DECIMAL_DIGIT);
      }
      int32_t end = offset + sum_len - 1;
      while (sum_digits[end] == 0) {
        --end;
        --sum_len;
      }
      uint32_t *digit_mem = NULL;
      Desc sum_desc = exp_rem_(dividend_desc, divisor_desc);
      if (OB_FAIL(calc_desc_and_check(sum_desc.desc_, sum_exp, (uint8_t)sum_len, sum_desc, is_oracle_mode()))) {
        LOG_WARN("fail to assign desc part", K(ret));
        if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
          res.set_zero();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(digit_mem = (uint32_t *)allocator.alloc(sizeof(uint32_t) * sum_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc mem", "size", sizeof(uint32_t) * sum_len, K(ret));
      } else {
        MEMCPY(digit_mem, sum_digits + offset, sizeof(uint32_t) * sum_len);
        res.assign(sum_desc.desc_, (uint32_t*)digit_mem);
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "rem_v3: [%s] %% [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::sqrt_first_guess_(ObNumber &value, ObIAllocator &allocator) const
{
  // use std::sqrt(double) can get a much better guess and need much fewer loops
  // in sqrt(). Not doing this for now because we can't do ObNumber <=> double
  // convertion in ObNumber conveniently.
  // same as pg's sqrt_var: half highest digit, half exp.
  int ret = OB_SUCCESS;
  ObNumber res;
  uint32_t digits[1] = {0};
  if (is_zero()) {
    // should not happen. sqrt(0) should be handled as a special case without
    // using sqrt_first_guess_
    res.set_zero();
  } else if (is_negative()) {
    LOG_WARN("cannot take sqrt guess of negative arg", KPC(this), K(ret));
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
  } else {
    Desc guess_desc;
    guess_desc.desc_ = d_.desc_;
    // value must be positive here
    int64_t decoded_exp = guess_desc.exp_ - EXP_ZERO;
    // handle exp=-1 specially:
    // 0.36 == (exp=-1, digits=[360000000,]),
    // -1/2 = 0, 360000000/2 = 180000000,
    // (exp=0, digits=[180000000,]) == 180000000 is very inappropriate.
    // guess exp from exp -1 should be -1.
    // guess of 0.36 will be (exp=-1, digits=[180000000,]) == 0.18
    int64_t decoded_new_exp = (decoded_exp == -1) ? -1 : (decoded_exp / 2);
    guess_desc.exp_ = decoded_new_exp + EXP_ZERO;
    guess_desc.len_ = 1;
    guess_desc.reserved_ = 0; // keep always 0.
    uint32_t digit = get_digits()[0] / 2;
    if (0 == digit) {
      digit = 1;  // avoid 0 guess
    }
    digits[0] = digit;
    res.assign(guess_desc.desc_, digits);
    LOG_DEBUG("sqrt_first_guess_, main path", KPC(this), K(res), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(value.from(res, allocator))) {
      LOG_WARN("failed: deep_copy res to value", K(ret));
    }
  }
  _OB_LOG(DEBUG, "sqrt_first_guess_ [%s], ret=%d [%s]", format(), ret, value.format());
  return ret;
}

int ObNumber::sqrt(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  number::ObNumber result;

  // this check is necessary to avoid "quotient = arg (0) / guess (0)"
  // OB_DIVISION_BY_ZERO error
  if (is_zero()) {
    if (OB_FAIL(result.from(number::ObNumber::get_zero(), allocator))) {
      LOG_WARN("set result to 0 failed", K(ret));
    }
  } else if (is_negative()) {
    LOG_WARN("cannot take sqrt of negative arg", KPC(this), K(ret));
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
  } else {
    const int NUM_ONE_TIME_CLAC = 2;
    const int ONE_TIME_BUFFER_SIZE = NUM_ONE_TIME_CLAC * MAX_CALC_BYTE_LEN;
    char one_time_buffer[ONE_TIME_BUFFER_SIZE];
    // use one_time_allocator for all intermediate allocations and make sure
    // that the passed-in allocator is only used once for the final result
    ObDataBuffer one_time_allocator(one_time_buffer, ONE_TIME_BUFFER_SIZE);

    number::ObNumber guess;
    if (OB_FAIL(sqrt_first_guess_(guess, one_time_allocator))) {
      LOG_WARN("failed: sqrt_first_guess_", KPC(this), K(ret));
    } else {
      // main loop of Newton's algorithm
      number::ObNumber quotient;
      number::ObNumber const_zero_point_five;
      number::ObNumber new_guess;
      if (OB_FAIL(const_zero_point_five.from("0.5", one_time_allocator))) {
        LOG_WARN("fail to initialize const_zero_point_five", K(ret));
      }

      // max number of ObNumber calculations in the following loop. update this
      // when modifying the following loop
      const int NUM_MAX_CALC_IN_LOOP = 3;
      const int LOOP_BUFFER_SIZE = NUM_MAX_CALC_IN_LOOP * MAX_CALC_BYTE_LEN;
      char loop_buffer_1[LOOP_BUFFER_SIZE];
      char loop_buffer_2[LOOP_BUFFER_SIZE];
      ObDataBuffer loop_allocator_current(loop_buffer_1, LOOP_BUFFER_SIZE);
      ObDataBuffer loop_allocator_next(loop_buffer_2, LOOP_BUFFER_SIZE);

      bool guess_is_answer = false;
      while (OB_SUCC(ret) && !guess_is_answer) {
        if (OB_FAIL(div(guess, quotient, loop_allocator_current, OB_MAX_DECIMAL_DIGIT, false))) {
          LOG_WARN("failed: quotient = this / guess", KPC(this), K(guess), K(ret));
        } else if (OB_FAIL(guess.add_v3(quotient, new_guess, loop_allocator_current, true, false))) {
          // new guess is the average of current guess and quotient
          LOG_WARN("failed: new_guess = guess + quotient", K(guess), K(quotient), K(ret));
        } else if (OB_FAIL(new_guess.mul(const_zero_point_five, new_guess, loop_allocator_current, false))) {
          // *0.5 is faster than /2 ?
          LOG_WARN("failed: new_guess *= 0.5", K(new_guess), K(ret));
        } else {
          // why not use "quotient == guess" as end condition:
          // - arg                = 3.16227766016837933199889354443271853372
          // - guess              = 1.77827941003892280122542119519268484474
          // - quotient           = 1.77827941003892280122542119519268484473
          // - (guess+quotient)/2 = 1.77827941003892280122542119519268484474 = guess
          // here, guess != quotient, infinite loop. Using guess == new_guess
          // can avoid this.
          if (new_guess.is_equal(guess)) {
            guess_is_answer = true;
          } else {
            guess = new_guess;
          }
          loop_allocator_next.free();
          std::swap(loop_allocator_current, loop_allocator_next);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(result.from(guess, allocator))) {
          LOG_WARN("failed: deep copy guess to result", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding && OB_FAIL(result.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE,
                                                      true, false))) {
      LOG_WARN("result.round_scale_v3_() fail", K(ret), K(result));
    } else {
      value = result;
    }
  }

  _OB_LOG(DEBUG, "sqrt [%s], ret=%d [%s], do_rounding=%d",
          format(), ret, value.format(), do_rounding);
  return ret;
}

int ObNumber::ln(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  number::ObNumber result;

  // check special value
  if (is_zero()) {
    LOG_WARN("cannot get logarithm of zero", KPC(this), K(ret));
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
  } else if (is_negative()) {
    LOG_WARN("cannot get logarithm of a negative number", KPC(this), K(ret));
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    // TODO check if this is 1 ?
  } else {
    // number of times one_time_allocator is used in this function. update this
    // when modifying this function
    const int NUM_ONE_TIME_CLAC = 14;
    const int ONE_TIME_BUFFER_SIZE = NUM_ONE_TIME_CLAC * MAX_CALC_BYTE_LEN;
    char one_time_buffer[ONE_TIME_BUFFER_SIZE];
    ObDataBuffer one_time_allocator(one_time_buffer, ONE_TIME_BUFFER_SIZE);

    // max number of ObNumber calculations in all loops in this function. update
    // this when modifying while loop in this fun.
    const int NUM_MAX_CALC_IN_LOOP = 4;
    const int LOOP_BUFFER_SIZE = NUM_MAX_CALC_IN_LOOP * MAX_CALC_BYTE_LEN;
    char loop_buffer_1[LOOP_BUFFER_SIZE];
    char loop_buffer_2[LOOP_BUFFER_SIZE];
    ObDataBuffer loop_allocator_current(loop_buffer_1, LOOP_BUFFER_SIZE);
    ObDataBuffer loop_allocator_next(loop_buffer_2, LOOP_BUFFER_SIZE);

    // Reduce *this into range (0.9, 1.1) with repeated sqrt() operations.
    number::ObNumber const_two;
    number::ObNumber const_zero_point_nine;
    number::ObNumber const_one_point_one;
    number::ObNumber reduced_arg;
    number::ObNumber reduction_compensation;
    if (OB_FAIL(const_two.from((int64_t)2, one_time_allocator))) {
      LOG_WARN("fail to initialize const_two", K(ret));
    } else if (OB_FAIL(const_zero_point_nine.from("0.9", one_time_allocator))) {
      LOG_WARN("fail to initialize const_zero_point_nine", K(ret));
    } else if (OB_FAIL(const_one_point_one.from("1.1", one_time_allocator))) {
      LOG_WARN("fail to initialize const_one_point_one", K(ret));
    } else if (OB_FAIL(reduced_arg.from(*this, one_time_allocator))) {
      LOG_WARN("failed: deep copy this to reduced_arg", KPC(this), K(ret));
    } else if (OB_FAIL(reduction_compensation.from(const_two, one_time_allocator))) {
      // reduction_compensation starts with 2 instead of 1, because the
      // following taylor series will get 0.5 * ln(reduced_arg), not
      // ln(reduced_arg)
      LOG_WARN("failed to initialize reduction_compensation", K(ret));
    } else {
      // note: here this and reduced_arg > 0
      while (OB_SUCC(ret) && reduced_arg.compare(const_zero_point_nine) <= 0) {
        if (OB_FAIL(reduced_arg.sqrt(reduced_arg, loop_allocator_current, false))) {
          LOG_WARN("sqrt reduced_arg failed", K(reduced_arg), K(ret));
        } else if (OB_FAIL(reduction_compensation.mul(const_two, reduction_compensation,
                                                      loop_allocator_current, false))) {
          LOG_WARN("reduction_compensation *= 2 failed", K(reduction_compensation), K(ret));
        } else {
          loop_allocator_next.free();
          std::swap(loop_allocator_current, loop_allocator_next);
        }
      }
      while (OB_SUCC(ret) && reduced_arg.compare(const_one_point_one) >= 0) {
        if (OB_FAIL(reduced_arg.sqrt(reduced_arg, loop_allocator_current, false))) {
          LOG_WARN("sqrt reduced_arg failed", K(reduced_arg), K(ret));
        } else if (OB_FAIL(reduction_compensation.mul(const_two, reduction_compensation,
                                                      loop_allocator_current, false))) {
          LOG_WARN("reduction_compensation *= 2 failed", K(reduction_compensation), K(ret));
        } else{
          loop_allocator_next.free();
          std::swap(loop_allocator_current, loop_allocator_next);
        }
      }

      if (OB_SUCC(ret)) {
        // loop_calc_allocator will be reuse. move numbers that are used later
        // to one_time_allocator
        if (OB_FAIL(reduced_arg.deep_copy_to_allocator_(one_time_allocator))) {
          LOG_WARN("failed: deep copy reduced_arg to one_time_allocator", K(ret));
        } else if (OB_FAIL(reduction_compensation.deep_copy_to_allocator_(one_time_allocator))) {
          LOG_WARN("failed: deep copy reduction_compensation to one_time_allocator", K(ret));
        }
      }

      // Taylor series: 0.5 * ln(reduced_arg) = 0.5 * ln((1+z)/(1-z)) = z + z^3/3 + z^5/5 + ...,
      // where z = (reduced_arg-1)/(reduced_arg+1)
      if (OB_SUCC(ret)) {
        // initialize vars
        number::ObNumber series_number;
        // z = (reduced_arg-1)/(reduced_arg+1)
        number::ObNumber z;
        // z_square = z^2
        number::ObNumber z_square;
        // tmp is only used to store (reduced_arg+1) when calculating z =
        // (reduced_arg-1)/(reduced_arg+1)
        number::ObNumber tmp;
        // term_x_exponent is z^i
        number::ObNumber term_x_exponent;
        // term is z^i/i
        number::ObNumber term;
        if (OB_FAIL(series_number.from(number::ObNumber::get_positive_one(), one_time_allocator))) {
          LOG_WARN("initialize series_number to 1 failed", K(ret));
        } else if (OB_FAIL(reduced_arg.sub_v3(number::ObNumber::get_positive_one(), z,
                                              one_time_allocator, true, false))) {
          LOG_WARN("failed: z=reduced_arg-1", K(reduced_arg), K(z), K(ret));
        } else if (OB_FAIL(reduced_arg.add_v3(number::ObNumber::get_positive_one(), tmp,
                                              one_time_allocator, true, false))) {
          LOG_WARN("failed: tmp=reduced_arg+1", K(reduced_arg), K(tmp), K(ret));
        } else if (OB_FAIL(z.div(tmp, z, one_time_allocator, OB_MAX_DECIMAL_DIGIT, false))) {
          // z = (reduced_arg-1)/(reduced_arg+1) now
          LOG_WARN("failed: z/=tmp, where tmp = reduced_arg+1", K(z), K(tmp), K(ret));
        } else if (OB_FAIL(z.mul(z, z_square, one_time_allocator, false))) {
          LOG_WARN("failed: z_square = z*z", K(z), K(ret));
        } else if (OB_FAIL(term_x_exponent.from(z, one_time_allocator))) {
          LOG_WARN("failed: deep copy z to term_x_exponent", K(ret));
        } else if (OB_FAIL(result.from(z, one_time_allocator))) {
          LOG_WARN("failed: deep copy z to result", K(z), K(ret));
        } else {
          bool term_reachs_zero = false;
          while (OB_SUCC(ret) && !term_reachs_zero) {
            // main loop of the Taylor series
            if (OB_FAIL(series_number.add_v3(const_two, series_number,
                                             loop_allocator_current, true, false))) {
              LOG_WARN("failed: series_number+=2", K(series_number), K(ret));
            } else if (OB_FAIL(term_x_exponent.mul(z_square, term_x_exponent,
                                                   loop_allocator_current, false))) {
              LOG_WARN("failed: term_x_exponent *= z_square", K(term_x_exponent), K(z_square), K(ret));
            } else if (OB_FAIL(term_x_exponent.div(series_number, term, loop_allocator_current,
                                                   OB_MAX_DECIMAL_DIGIT, false))) {
              LOG_WARN("failed: term = term_x_exponent / series_number, K(ret)",
                       K(term_x_exponent), K(series_number));
            } else {  // now we have the term of this loop
              if (term.is_zero()) {
                term_reachs_zero = true;
              } else {
                if (OB_FAIL(result.add_v3(term, result, loop_allocator_current, true, false))) {
                  LOG_WARN("failed: result += term", K(result), K(term), K(ret));
                }
              }
              // this iteration is ending. data from last iteration will not
              // be used any more
              loop_allocator_next.free();
              // should swap internal pointers which point to loop_buffer_1,
              // loop_buffer_2
              std::swap(loop_allocator_current, loop_allocator_next);
            }
          }

          if (OB_SUCC(ret)) {
            // don't use one_time_allocator here, because result is shallow-copied
            // to value later.
            if (OB_FAIL(result.mul(reduction_compensation, result, allocator, false))) {
              LOG_WARN("failed: result *= reduction_compensation",
                       K(result), K(reduction_compensation), K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(result.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE,
                                         true, false))) {
        LOG_WARN("result.round_scale_v3_() fail", K(ret), K(result));
      } else {
        value = result;
      }
    } else {
      value = result;
    }
  }

  _OB_LOG(DEBUG, "ln [%s], ret=%d [%s], do_rounding=%d",
          format(), ret, value.format(), do_rounding);
  return ret;
}

int ObNumber::e_power(ObNumber &value, ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  LOG_DEBUG("ObExprPower, e_power", K(*this), KPC(this), K(do_rounding));

  int ret = OB_SUCCESS;
  number::ObNumber result;

  // number of times one_time_allocator is used in this function. update this
  // when modifying this function
  const int NUM_ONE_TIME_CLAC = 9;
  const int ONE_TIME_BUFFER_SIZE = NUM_ONE_TIME_CLAC * MAX_CALC_BYTE_LEN;
  char one_time_buffer[ONE_TIME_BUFFER_SIZE];
  ObDataBuffer one_time_allocator(one_time_buffer, ONE_TIME_BUFFER_SIZE);

  // max number of ObNumber calculations in all loops in this function. update
  // this when modifying while loop in this fun.
  const int NUM_MAX_CALC_IN_LOOP = 4;
  const int LOOP_BUFFER_SIZE = NUM_MAX_CALC_IN_LOOP * MAX_CALC_BYTE_LEN;
  char loop_buffer_1[LOOP_BUFFER_SIZE];
  char loop_buffer_2[LOOP_BUFFER_SIZE];
  ObDataBuffer loop_allocator_current(loop_buffer_1, LOOP_BUFFER_SIZE);
  ObDataBuffer loop_allocator_next(loop_buffer_2, LOOP_BUFFER_SIZE);

  if (is_zero()) {
    // note: result.set_one() will raise error if not initialized.
    if (OB_FAIL(result.from(number::ObNumber::get_positive_one(), one_time_allocator))) {
      LOG_WARN("set result to 1 failed", K(ret));
    }
  } else {
    number::ObNumber exponent_new;
    if (OB_FAIL(exponent_new.from(*this, one_time_allocator))) {
      LOG_WARN("deep copy failed", KPC(this), K(ret));
    } else {
      // Reduce exponent_new to the range [-0.01, 0.01] by dividing by 2^n, to
      // improve the convergence rate of the Taylor series.
      const char* small_fraction = "0.01";
      number::ObNumber small_fraction_number;
      number::ObNumber const_two;
      int times_exponent_div_2 = 0;
      if (OB_FAIL(small_fraction_number.from(small_fraction, one_time_allocator))) {
        LOG_WARN("initialize small_fraction_number failed", K(ret));
      } else if (OB_FAIL(const_two.from((int64_t)2, one_time_allocator))) {
        LOG_WARN("fail to initialize const_two", K(ret));
      } else {
        while (OB_SUCC(ret) && exponent_new.abs_compare(small_fraction_number) > 0) {
          if (OB_FAIL(exponent_new.div(const_two, exponent_new,
                                       loop_allocator_current, OB_MAX_DECIMAL_DIGIT, false))) {
            LOG_WARN("exponent_new /= 2 failed", K(exponent_new), K(ret));
          } else {
            times_exponent_div_2 ++;

            loop_allocator_next.free();
            std::swap(loop_allocator_current, loop_allocator_next);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(exponent_new.deep_copy_to_allocator_(one_time_allocator))) {
            LOG_WARN("failed: move exponent_new to one_time_allocator", K(ret));
          }
        }

        LOG_DEBUG("reduce exponent_new done", K(exponent_new), K(times_exponent_div_2),
                  KCSTRING(exponent_new.format()), K(ret));
      }

      if (OB_SUCC(ret)) {
        // Use the Taylor series: exp(x) = 1 + x + x^2/2! + x^3/3! + ..., here x
        // is exponent_new
        number::ObNumber term;  // current term in the Taylor series
        number::ObNumber series_index;
        if (OB_FAIL(result.from(number::ObNumber::get_positive_one(), one_time_allocator))) {
          LOG_WARN("deep copy positive one failed", K(ret));
        } else if (OB_FAIL(term.from(exponent_new, one_time_allocator))) {
          LOG_WARN("deep copy failed", K(exponent_new), K(ret));
        } else if (OB_FAIL(series_index.from(number::ObNumber::get_positive_one(), one_time_allocator))) {
          LOG_WARN("deep copy positive one failed", K(ret));
        } else {
          LOG_DEBUG("begin taylor series",
                    K(result), KCSTRING(result.format()),
                    K(term), KCSTRING(term.format()),
                    K(series_index), KCSTRING(series_index.format()),
                    K(ret));
          do {
            if (OB_FAIL(result.add_v3(term, result, loop_allocator_current, true, false))) {
              LOG_WARN("failed: result += term", K(ret));
            } else if (OB_FAIL(term.mul(exponent_new, term, loop_allocator_current, false))) {
              LOG_WARN("failed: term *= exponent_new", K(ret));
            } else if (OB_FAIL(series_index.add_v3(number::ObNumber::get_positive_one(),
                                                   series_index, loop_allocator_current, true, false))) {
              LOG_WARN("failed: series_index += 1", K(ret));
            } else if (OB_FAIL(term.div(series_index, term, loop_allocator_current,
                                        OB_MAX_DECIMAL_DIGIT, false))) {
              LOG_WARN("failed: term /= series_index", K(ret));
            } else {
              loop_allocator_next.free();
              std::swap(loop_allocator_current, loop_allocator_next);
            }
            LOG_DEBUG("in taylor series",
                      K(result), KCSTRING(result.format()),
                      K(term), KCSTRING(term.format()),
                      K(series_index), KCSTRING(series_index.format()),
                      K(ret));
            // end condition: term is so small that it is rounded to 0 in div()
          } while (OB_SUCC(ret) && !term.is_zero());
          LOG_DEBUG("after taylor series",
                    K(result), KCSTRING(result.format()),
                    K(term), KCSTRING(term.format()),
                    K(series_index), KCSTRING(series_index.format()),
                    K(ret));

          if (OB_SUCC(ret)) {
            if (OB_FAIL(result.deep_copy_to_allocator_(one_time_allocator))) {
              LOG_WARN("failed: move result to one_time_allocator", K(ret));
            }
          }

          LOG_DEBUG("after taylor series, result.deep_copy_to_allocator_",
                    K(result), KCSTRING(result.format()), K(ret));

          // Compensate for the argument range reduction
          while (OB_SUCC(ret) && times_exponent_div_2-- > 0) {
            if (OB_FAIL(result.mul(result, result, loop_allocator_current, false))) {
              LOG_WARN("failed: result *= result", K(ret));
            } else {
              loop_allocator_next.free();
              std::swap(loop_allocator_current, loop_allocator_next);
            }
            LOG_DEBUG("squaring result", K(result), KCSTRING(result.format()), K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(result.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("result.round_scale_v3_() fail", K(ret), K(result));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(value.from(result, allocator))) {
        LOG_WARN("failed: deep copy result to value", K(ret));
      }
    }

  }

  _OB_LOG(DEBUG, "e_power [%s], ret=%d [%s]", format(), ret, value.format());
  return ret;
}

int ObNumber::round_remainder(const ObNumber &other, ObNumber &value, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN * 2];
  ObDataBuffer allocator2(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN * 2);
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(ERROR, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else {
    /*a.round_remainder(b) = a - b * N;
      q = a / b;
      N = q.round_even_number;
      p = b * N;
    */
    ObNumber q,p;
    if (OB_FAIL(div(other, q, allocator2))) {
      LOG_WARN("division failed", K(*this), K(other));
    } else if (OB_FAIL(q.round_even_number())) {
      LOG_WARN("q round_even_number failed", K(*this), K(other), K(q));
    } else if (OB_FAIL(other.mul(q, p, allocator2))) {
      LOG_WARN("b multiply q failed", K(*this), K(other), K(q), K(p));
    } else if (OB_FAIL(sub_v3(p, res, allocator))) {
      LOG_WARN("a subtract (b*q) failed", K(*this), K(other), K(q), K(p));
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "round_remainder_,[%s] %% [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::power(const int64_t exponent, ObNumber &value,
                    ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  number::ObNumber result;

  // max number of times one_time_allocator is used in this function. update
  // this when modifying this function
  const int NUM_ONE_TIME_CLAC = 3;
  const int ONE_TIME_BUFFER_SIZE = NUM_ONE_TIME_CLAC * MAX_CALC_BYTE_LEN;
  char one_time_buffer[ONE_TIME_BUFFER_SIZE];
  ObDataBuffer one_time_allocator(one_time_buffer, ONE_TIME_BUFFER_SIZE);

  // check special cases
  bool done_in_special_cases = true;
  if (is_zero() && exponent < 0) {
    ret = OB_NUMERIC_OVERFLOW;
    LOG_WARN("division by zero (base is 0 and exponent is negative)", K(ret),
             KPC(this), K(exponent));
  } else {
    switch (exponent) {
      case 0:
        if (OB_FAIL(result.from(number::ObNumber::get_positive_one(), one_time_allocator))) {
          LOG_WARN("copy one failed", K(ret));
        }
        break;
      case 1:
        if (OB_FAIL(result.from(*this, one_time_allocator))) {
          LOG_WARN("copy base failed", K(ret));
        }
        break;
      case -1:
        if (OB_FAIL(number::ObNumber::get_positive_one().div(*this, result, one_time_allocator,
                                                             OB_MAX_DECIMAL_DIGIT, false))) {
          LOG_WARN("result=1/base failed", KPC(this), K(ret));
        }
        break;
      case 2:
        if (OB_FAIL(mul(*this, result, one_time_allocator, false))) {
          LOG_WARN("result=base^2 failed", KPC(this), K(ret));
        }
        break;
      default:
        if (is_zero()) {
          if (OB_FAIL(result.from(number::ObNumber::get_zero(), one_time_allocator))) {
            LOG_WARN("copy zero failed", K(ret));
          }
        } else {
          done_in_special_cases = false;
        }
        break;
    }
  }

  if (OB_SUCC(ret) && not done_in_special_cases) {
    bool is_exponent_negative = (exponent < 0);
    int abs_exponent = std::abs(exponent);

    number::ObNumber base_product;
    if (OB_FAIL(base_product.from(*this, one_time_allocator))) {
        LOG_WARN("failed: deep copy this to base_product", K(ret));
    } else if ((abs_exponent & 1) != 0) {
      if (OB_FAIL(result.from(*this, one_time_allocator))) {
        LOG_WARN("failed: deep copy base to result", K(ret));
      }
    } else {
      if (OB_FAIL(result.from(number::ObNumber::get_positive_one(), one_time_allocator))) {
        LOG_WARN("failed: deep copy 1 to result", K(ret));
      }
    }
    // max number of ObNumber calculations in all loops in this function. update
    // this when modifying while loop in this fun.
    const int NUM_MAX_CALC_IN_LOOP = 1;
    const int LOOP_BUFFER_SIZE = NUM_MAX_CALC_IN_LOOP * MAX_CALC_BYTE_LEN;
    char loop_buffer_1[LOOP_BUFFER_SIZE];
    char loop_buffer_2[LOOP_BUFFER_SIZE];
    ObDataBuffer loop_allocator_current(loop_buffer_1, LOOP_BUFFER_SIZE);
    ObDataBuffer loop_allocator_next(loop_buffer_2, LOOP_BUFFER_SIZE);

    // in the following while loop, when abs_exponent & 1 == 0, "result *=
    // base_product" is not calculated and result is still store in the old
    // allocator. because of this, we can't store it in loop_allocator_current
    // without deep-copy. And to avoid deep-copying result in each loop where
    // abs_exponent & 1 == 0, we use separated allocators for result.
    const int RESULT_BUFFER_SIZE = MAX_CALC_BYTE_LEN;
    char result_buffer_1[RESULT_BUFFER_SIZE];
    char result_buffer_2[RESULT_BUFFER_SIZE];
    ObDataBuffer result_allocator_current(result_buffer_1, RESULT_BUFFER_SIZE);
    ObDataBuffer result_allocator_next(result_buffer_2, RESULT_BUFFER_SIZE);
    while (OB_SUCC(ret) and (abs_exponent >>= 1) > 0) {
      if (OB_FAIL(base_product.mul(base_product, base_product, loop_allocator_current, false))) {
        LOG_WARN("failed: square base_product", K(ret));
      } else if (abs_exponent & 1) {
        if (OB_FAIL(result.mul(base_product, result, result_allocator_current, false))) {
          LOG_WARN("failed: result *= base_product", K(ret));
        } else {
          result_allocator_next.free();
          std::swap(result_allocator_current, result_allocator_next);
        }
      }

      // TODO check overflow(positive exponent)/underflow(negative exponent)
      if (OB_SUCC(ret)) {
        loop_allocator_next.free();
        std::swap(loop_allocator_current, loop_allocator_next);
      }
    }

    if (OB_SUCC(ret)) {
      if (is_exponent_negative) {
        if (OB_FAIL(number::ObNumber::get_positive_one().div(result, result, one_time_allocator,
                                                             OB_MAX_DECIMAL_DIGIT, false))) {
          LOG_WARN("failed: result = 1 / result", K(ret));
        }
      } else if (OB_FAIL(result.deep_copy_to_allocator_(one_time_allocator))) {
        LOG_WARN("failed: move result to one_time_allocator", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(result.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("result.round_scale_v3_() fail", K(ret), K(result));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(value.from(result, allocator))) {
        LOG_WARN("failed: deep copy result to value", K(ret));
      }
    }
  }

  _OB_LOG(DEBUG, "power (int exponent) ( [%s], [%ld] ), do_rounding=%d, ret=%d [%s]",
          (ret == OB_SUCCESS ? format() : "error"), exponent, do_rounding, ret,
          (ret == OB_SUCCESS ? result.format() : "error"));
  return ret;
}

int ObNumber::power(const ObNumber &exponent, ObNumber &value,
                    ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  int ret = OB_SUCCESS;
  number::ObNumber result;

  LOG_DEBUG("power() start", KPC(this), K(exponent), K(do_rounding));

  // max number of times one_time_allocator is used in this function. update
  // this when modifying this function
  const int NUM_ONE_TIME_CLAC = 3;
  const int ONE_TIME_BUFFER_SIZE = NUM_ONE_TIME_CLAC * MAX_CALC_BYTE_LEN;
  char one_time_buffer[ONE_TIME_BUFFER_SIZE];
  ObDataBuffer one_time_allocator(one_time_buffer, ONE_TIME_BUFFER_SIZE);

  // if exponent is an integer and can store in an int64_t, use power(int)
  int64_t exponent_int = 0;
  // TODO which method to use? is_integer(), is_valid_int(), ...
  if (exponent.is_integer() && OB_SUCCESS == exponent.cast_to_int64(exponent_int)) {
    LOG_DEBUG("use power(int)", KPC(this), K(exponent_int));
    if (OB_FAIL(power(exponent_int, result, one_time_allocator, false))) {
      LOG_WARN("failed: power(int)", KPC(this), K(exponent_int), K(result), K(ret));
    }
  } else if (is_zero()){
    if (exponent.is_negative()) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("calc power failed, exp is negative and base is zero", K(ret));
    } else if (exponent.is_zero()) {
      // 0 ^ 0 = 1 is handled in power(int), not here.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot reach here", K(ret));
    } else {
      if (OB_FAIL(result.from(number::ObNumber::get_zero(), one_time_allocator))) {
        LOG_WARN("failed: copy zero to result", K(ret));
      }
    }
    // TODO check if base is 1 ?
  } else {
    // the main path: result = e^(ln(base)*exponent)
    if (OB_FAIL(ln(result, one_time_allocator, false))) {
      LOG_WARN("failed: result = ln(base)", K(ret));
    } else if (OB_FAIL(result.mul(exponent, result, one_time_allocator, false))) {
      LOG_WARN("failed: result *= exponent", K(ret));
    } else if (OB_FAIL(result.e_power(result, one_time_allocator, false))) {
      LOG_WARN("failed: result = e^result", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(result.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("result.round_scale_v3_() fail", K(ret), K(result));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(value.from(result, allocator))) {
        LOG_WARN("failed: deep copy result to value", K(ret));
      }
    }
  }

  _OB_LOG(DEBUG, "power ( [%s], [%s] ), do_rounding=%d, ret=%d [%s]",
          format(), exponent.format(), do_rounding, ret, result.format());
  return ret;
}

int ObNumber::log(const ObNumber &base, ObNumber &value,
                  ObIAllocator &allocator, const bool do_rounding/*true*/) const
{
  // log_b(x) = ln(x)/ln(b)
  int ret = OB_SUCCESS;
  number::ObNumber result;

  LOG_DEBUG("log() start", KPC(this), K(base), K(do_rounding));

  // max number of times one_time_allocator is used in this function. update
  // this when modifying this function
  const int NUM_ONE_TIME_CLAC = 3;
  const int ONE_TIME_BUFFER_SIZE = NUM_ONE_TIME_CLAC * MAX_CALC_BYTE_LEN;
  char one_time_buffer[ONE_TIME_BUFFER_SIZE];
  ObDataBuffer one_time_allocator(one_time_buffer, ONE_TIME_BUFFER_SIZE);

  // check arguments
  if (base.is_zero() || base.is_negative()) {
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    LOG_WARN("the base of logarithm has to be positive", K(base), K(ret));
  } else if (base.compare(number::ObNumber::get_positive_one()) == 0) {
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    LOG_WARN("the base of logarithm can't be 1", K(base), K(ret));
    // log_b(x), the validity of x will be check in ln(x)
  } else {
    number::ObNumber ln_x;
    number::ObNumber ln_base;
    if (OB_FAIL(ln(ln_x, one_time_allocator, false))) {
      LOG_WARN("failed: ln_x = ln(this)", KPC(this), K(ret));
    } else if (OB_FAIL(base.ln(ln_base, one_time_allocator, false))) {
      LOG_WARN("failed: ln_base = ln(base)", K(ret));
    } else if (OB_FAIL(ln_x.div(ln_base, result, one_time_allocator, OB_MAX_DECIMAL_DIGIT, false))) {
      LOG_WARN("failed: result = ln_x / ln_base", K(ln_x), K(ln_base), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (do_rounding) {
      if (OB_FAIL(result.round_scale_v3_(is_oracle_mode() ? MAX_SCALE : FLOATING_SCALE, true, false))) {
        LOG_WARN("result.round_scale_v3_() fail", K(ret), K(result));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(value.from(result, allocator))) {
        LOG_WARN("failed: deep copy result to value", K(ret));
      }
    }
  }

  _OB_LOG(DEBUG, "log ( [%s], [%s] ), do_rounding=%d, ret=%d [%s]",
          format(), base.format(), do_rounding, ret, result.format());
  return ret;
}

// int64_t range [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
bool ObNumber::is_int64() const
{
  bool b_ret = true;
  if (is_zero()) {
    b_ret = true;
  } else {
    const int32_t exp = get_decode_exp(d_);
    if (!is_integer(exp)) { //exponential must great than or equal to 0
      b_ret = false;
    } else if (exp > 2) { //exponential must less than or equal to 2
      b_ret = false;
    } else if (2 == exp){
      const uint32_t MAX_HIGH_NUM = 9;
      const uint32_t MAX_MID_NUM = 223372036;
      const uint32_t MAX_POSITIVE_LOW_NUM = 854775807;
      const uint32_t MAX_NEGATIVE_LOW_NUM = 854775808;
      // no need to check digits_ max length because it is already guaranteed in is_integer(exp)
      // that: exp + 1 >= d_.len_. So here max length of digits_ is 3.
      if (digits_[0] > MAX_HIGH_NUM) {
        b_ret = false;
      } else if (d_.len_ >= 2 && MAX_HIGH_NUM == digits_[0] && digits_[1] > MAX_MID_NUM) {
        b_ret = false;
      } else if (d_.len_ >= 3 && MAX_HIGH_NUM == digits_[0] && MAX_MID_NUM == digits_[1]) {
        if (is_negative() && digits_[2] > MAX_NEGATIVE_LOW_NUM) {
          b_ret = false;
        } else if (!is_negative() && digits_[2] > MAX_POSITIVE_LOW_NUM) {
          b_ret = false;
        } else {
          b_ret = true;
        }
      } else {
        b_ret = true;
      }
    } else { // exponential is 0 or 1
      b_ret = true;
    }
  }
  return b_ret;
}

int ObNumber::cast_to_int64(int64_t &value) const
{
  int ret = OB_SUCCESS;
  bool is_valid_integer = true;
  uint64_t tmp_value = 0;
  if (is_zero()) {
    value = 0;
  } else if (!is_int64()) {
    is_valid_integer = false;
  } else {
    const int32_t expr_value = get_decode_exp(d_);
    switch (expr_value) {
      case 0: {
        if (1 == d_.len_) {
          tmp_value = digits_[0];
        }
        break;
      }
      case 1: {
        if (1 == d_.len_) {
          tmp_value = digits_[0] * BASE;
        } else if (2 == d_.len_) {
          tmp_value = digits_[0] * BASE + digits_[1];
        }
        break;
      }
      case 2: {
        switch (d_.len_) {
          case 1: {
            const uint64_t tmp_v1 = digits_[0] * BASE;
            tmp_value = tmp_v1 * BASE;
            break;
          }
          case 2: {
            const uint64_t tmp_v1 = digits_[0] * BASE;
            const uint64_t tmp_v2 = tmp_v1 * BASE;
            const uint64_t tmp_v3 = digits_[1] * BASE;
            tmp_value = tmp_v2  + tmp_v3;
            break;
          }
          case 3: {
            const uint64_t tmp_v1 = digits_[0] * BASE;
            const uint64_t tmp_v2 = tmp_v1 * BASE;
            const uint64_t tmp_v3 = digits_[1] * BASE;
            tmp_value = tmp_v2  + tmp_v3 + digits_[2];
            break;
          }
          default : {
            is_valid_integer = false;
            break;
          }
        }
        break;
      }
      default : {
        is_valid_integer = false;
        break;
      }
    }

    if (is_valid_integer) {
      value = is_negative() ? 0 - tmp_value : tmp_value;
      LOG_DEBUG("finish cast_to_int64", K(tmp_value), K(is_valid_integer), KPC(this), K(value));
    }
  }

  if (OB_UNLIKELY(!is_valid_integer)) {
    ret = OB_INTEGER_PRECISION_OVERFLOW;
    LOG_DEBUG("this is not valid integer number", KPC(this), K(ret));
  }
  return ret;
}

/**
 * check whether a sci format string has a valid exponent part
 * valid : 1.8E-1/1.8E1   invalid : 1.8E, 1.8Ea, 1.8E-a
 * @param str     string need to parse
 * @param length  length of str
 * @param e_pos   index of 'E'
 */
bool ObNumber::is_valid_sci_tail_(const char *str, 
                                 const int64_t length, 
                                 const int64_t e_pos)
{
  bool res = false;
  if (e_pos == length - 1) {
    //like 1.8e, false
  } else if (e_pos < length - 1) {
    if ('+' == str[e_pos + 1] || '-' == str[e_pos + 1]) {
      if (e_pos < length - 2 && str[e_pos + 2] >= '0' && str[e_pos + 2] <= '9') {
        res = true;
      } else {
        //like 1.8e+, false
      }
    } else if (str[e_pos + 1] >= '0' && str[e_pos + 1] <= '9') {
      res = true;
    } else {
      //like 1.8ea, false
    }
  }
  return res;
}

void ObNumber::set_one()
{
  if (OB_ISNULL(digits_)) {
    _OB_LOG_RET(ERROR, OB_ERROR, "number digit ptr is null where set to one!");
    right_to_die_or_duty_to_live();
  } else {
    d_.len_ = 1;
    digits_[0] = 1;
    d_.se_ = (POSITIVE == d_.sign_ ? 0xc0 : 0x40);
  }
}

int64_t ObNumber::get_digit_len_v2(const uint32_t d)
{
  int64_t ret = 0;
  if (OB_UNLIKELY(BASE <= d)) {
    LIB_LOG(ERROR, "d is out of range");
  } else {
    ret = ob_fast_digits10(d);
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObIntegerBuilder::ObIntegerBuilder() : exp_(0),
                                       digit_pos_(ObNumber::MAX_CALC_LEN - 1),
                                       digit_idx_(0)
{
}

ObIntegerBuilder::~ObIntegerBuilder()
{
}

int ObIntegerBuilder::push(const uint8_t d, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_length() > ObNumber::MAX_CALC_LEN)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else {
    if (0 == digit_idx_) {
      // Init current digit on first use
      digits_[digit_pos_] = 0;
    }

    // 1234
    // push [4]: 4 = 4
    // push [3]: 3*10 + 4= 34
    // push [2]: 2*100 + 34 = 234
    // push [1]: 1*1000 + 234 = 1234
    static const uint32_t POWS[ObNumber::DIGIT_LEN] = {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000
    };

    digits_[digit_pos_] += d * POWS[digit_idx_++];

    if (ObNumber::DIGIT_LEN <= digit_idx_) {
      if (!reduce_zero
          || 0 != digits_[ObNumber::MAX_CALC_LEN - 1]) {
        --digit_pos_;
      }
      digit_idx_ = 0;
      ++exp_;
    }
  }
  return ret;
}

int ObIntegerBuilder::push_digit(const uint32_t d, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null", K(ret));
  } else if (OB_UNLIKELY(get_length() > ObNumber::MAX_CALC_LEN)) {
    ret = OB_NUMERIC_OVERFLOW;
    LOG_WARN("numeric_overflow", K(digit_idx_), K(digit_pos_), K(ret));
  } else {
    digits_[digit_pos_] = d;
    if (!reduce_zero || 0 != digits_[ObNumber::MAX_CALC_LEN - 1]) {
      --digit_pos_ ;
    }
    ++ exp_;
  }
  return ret;
}


int64_t ObIntegerBuilder::get_exp() const
{
  return (0 == digit_idx_) ? (exp_ - 1) : exp_;
}

int64_t ObIntegerBuilder::get_length() const
{
  return (0 == digit_idx_) ? (ObNumber::MAX_CALC_LEN - digit_pos_ - 1) :
         (ObNumber::MAX_CALC_LEN - digit_pos_);
}

const uint32_t *ObIntegerBuilder::get_digits() const
{
  return (0 == get_length()) ? NULL : &digits_[ObNumber::MAX_CALC_LEN - get_length()];
}

void ObIntegerBuilder::reset()
{
  exp_ = 0;
  digit_pos_ = ObNumber::MAX_CALC_LEN - 1;
  digit_idx_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObDecimalBuilder::ObDecimalBuilder() : exp_(-1),
                                       digit_pos_(0),
                                       digit_idx_(0)
{
}

ObDecimalBuilder::~ObDecimalBuilder()
{
}

int ObDecimalBuilder::push(const uint8_t d, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_length() >= ObNumber::MAX_CALC_LEN)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else {
    if (0 == digit_idx_) {
      // Init current digit on first use
      digits_[digit_pos_] = 0;
    }

    // 0.1234
    // push [1]: 0 + 1*100000000 = 100000000
    // push [2]: 100000000 + 2*10000000 = 120000000
    // push [3]: 120000000 + 3*1000000 = 123000000
    // push [4]: 123000000 + 4*100000 = 123400000
    static const uint32_t POWS[ObNumber::DIGIT_LEN] = {
        100000000,
        10000000,
        1000000,
        100000,
        10000,
        1000,
        100,
        10,
        1
    };
    digits_[digit_pos_] += d * POWS[digit_idx_++];

    if (ObNumber::DIGIT_LEN <= digit_idx_) {
      if (!reduce_zero
          || 0 != digits_[0]) {
        digit_pos_ += 1;
      } else {
        --exp_;
      }
      digit_idx_ = 0;
    }
  }
  return ret;
}

int ObDecimalBuilder::push_digit(const uint32_t d, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_length() >= ObNumber::MAX_CALC_LEN)) {
    ret = OB_NUMERIC_OVERFLOW;
    LOG_WARN("numeric_overflow", K(digit_idx_), K(digit_pos_), K(ret));
  } else {
    digits_[digit_pos_] = d;
    if (!reduce_zero || 0 != digits_[0]) {
      ++digit_pos_;
    }
    else {
      --exp_;
    }
  }
  return ret;
}

int64_t ObDecimalBuilder::get_exp() const
{
  return exp_;
}

int64_t ObDecimalBuilder::get_length() const
{
  return (0 == digit_idx_) ? digit_pos_ : (digit_pos_ + 1);
}

const uint32_t *ObDecimalBuilder::get_digits() const
{
  return (0 == get_length()) ? NULL : &digits_[0];
}

void ObDecimalBuilder::reset()
{
  exp_ = -1;
  digit_pos_ = 0;
  digit_idx_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

void ObNumberBuilder::reset()
{
  number_.set_zero();
}

int ObNumberBuilder::build(const char *str,
                           int64_t length,
                           int &warning,
                           ObNumberFmtModel *fmt,
                           int16_t *precision,
                           int16_t *scale)
{
  int ret = OB_SUCCESS;
  reset();
  bool negative = false;
  int64_t integer_start = -1;
  int64_t integer_end = -1;
  int64_t decimal_start = -1;
  bool integer_zero = false;
  bool decimal_zero = false;
  char new_str[ObNumber::MAX_TOTAL_SCALE];
  int64_t comma_cnt = 0;
  if (OB_ISNULL(number_.get_digits())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "digits_ should not be null when this func is invoked", K(ret));
  } else {
    if (OB_ISNULL(fmt)) {
      if (OB_FAIL(find_point_(str, length, integer_start, integer_end, decimal_start,
          negative, integer_zero, decimal_zero, warning))) {
        LIB_LOG(WARN, "lookup fail", K(ret), K(length), "str", ObString(length, str));
      }
    } else {
      if (OB_FAIL(find_point_(str, fmt, &new_str[0], length, integer_start, integer_end, decimal_start,
          negative, integer_zero, decimal_zero, comma_cnt))) {
        LIB_LOG(WARN, "lookup fail ", K(ret), "str", ObString(length, str), KCSTRING(fmt->fmt_str_), K(fmt->has_b_), K(fmt->has_currency_),
            K(fmt->has_d_), K(fmt->has_sign_));
      } else {
        LIB_LOG(DEBUG, "lookup success with fmt", K(ret), KCSTRING(fmt->fmt_str_), K(fmt->has_b_), K(fmt->has_currency_),
            K(fmt->has_d_), K(fmt->has_sign_), KCSTRING(new_str), K(length), K(integer_start), K(integer_end),
            K(decimal_start), K(negative), K(integer_zero), K(decimal_zero));
        str = &new_str[0];
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_integer_(str, integer_start, integer_end, decimal_zero, fmt))) {
      LIB_LOG(WARN, "build integer fail", K(ret), K(length), "str", ObString(length, str), K(integer_start), K(integer_end), K(decimal_zero));
    } else if (OB_FAIL(build_decimal_(str, length, decimal_start, integer_zero))) {
      LIB_LOG(WARN, "build decimal fail",  K(ret), K(length), "str", ObString(length, str), K(decimal_start), K(integer_zero));
    } else if (OB_UNLIKELY(ib_.get_length() < 0 || db_.get_length() < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "unexpected integer length or decimal length. ",
                    K(ret), K(ib_.get_length()), K(db_.get_length()));
    } else if (OB_UNLIKELY(ib_.get_length() + db_.get_length() > ObNumber::MAX_CALC_LEN)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LIB_LOG(WARN, "out of range, integer length, decimal length",
                    K(ret), K(ib_.get_length()), K(db_.get_length()));
    } else {
      if (!negative) {
        number_.d_.sign_ = ObNumber::POSITIVE;
      } else {
        number_.d_.sign_ = ObNumber::NEGATIVE;
      }

      if (0 != ib_.get_length()) {
        number_.d_.exp_ = 0x7f & (uint8_t)(ObNumber::EXP_ZERO + ib_.get_exp());
      } else {
        number_.d_.exp_ = 0x7f & (uint8_t)(ObNumber::EXP_ZERO + db_.get_exp());
      }

      number_.d_.len_ = (uint8_t)(ib_.get_length() + db_.get_length());
      if (negative) {
        number_.d_.exp_ = (0x7f & ~number_.d_.exp_);
        ++number_.d_.exp_;
      }

      if (NULL != number_.get_digits()
          && 0 < ib_.get_length()) {
        MEMCPY(number_.get_digits(), ib_.get_digits(), ib_.get_length() * ITEM_SIZE(number_.get_digits()));
      }
      if (NULL != number_.get_digits()
          && 0 < db_.get_length()) {
        MEMCPY(&number_.get_digits()[ib_.get_length()], db_.get_digits(), db_.get_length() * ITEM_SIZE(number_.get_digits()));
      }

      if (NULL != precision && NULL != scale) {
        *scale = static_cast<int16_t>((length - decimal_start == -1) ? 0 : (length - decimal_start));
        if (*scale > ObNumber::FLOATING_SCALE) {
          *scale = ObNumber::FLOATING_SCALE;
        }
        *precision = static_cast<int16_t>(integer_end - integer_start + 1 - comma_cnt  + *scale);
      }
      ret = number_.normalize_(number_.get_digits(), number_.d_.len_);
    }
  }
  return ret;
}


int ObNumberBuilder::build_v2(const char *str,
                           int64_t length,
                           int &warning,
                           ObNumberFmtModel *fmt,
                           int16_t *precision,
                           int16_t *scale)
{
  int ret = OB_SUCCESS;
  reset();
  bool negative = false;
  int64_t integer_start = -1;
  int64_t integer_end = -1;
  int64_t decimal_start = -1;
  bool integer_zero = false;
  bool decimal_zero = false;
  char new_str[ObNumber::MAX_TOTAL_SCALE];
  int64_t comma_cnt = 0;
  if (OB_ISNULL(number_.get_digits())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "digits_ should not be null when this func is invoked", K(ret));
  } else {
    if (OB_ISNULL(fmt)) {
      if (OB_FAIL(lib::is_oracle_mode()
          ? find_point_v2_(str, length, integer_start, integer_end, decimal_start,
                           negative, integer_zero, decimal_zero, warning)
          : find_point_(str, length, integer_start, integer_end, decimal_start,
                        negative, integer_zero, decimal_zero, warning))) {
        LIB_LOG(WARN, "lookup fail", K(ret), K(length), "str", ObString(length, str), "is_oracle_mode", lib::is_oracle_mode());
      }
    } else {
      //used for to_number(format)
      if (OB_FAIL(find_point_(str, fmt, &new_str[0], length, integer_start, integer_end, decimal_start,
          negative, integer_zero, decimal_zero, comma_cnt))) {
        LIB_LOG(WARN, "lookup fail ", K(ret), "str", ObString(length, str), KCSTRING(fmt->fmt_str_), K(fmt->has_b_), K(fmt->has_currency_),
            K(fmt->has_d_), K(fmt->has_sign_), K(fmt->has_x_));
      } else {
        LIB_LOG(DEBUG, "lookup success with fmt", K(ret), KCSTRING(fmt->fmt_str_), K(fmt->has_b_), K(fmt->has_currency_),
            K(fmt->has_d_), K(fmt->has_sign_), K(fmt->has_x_), KCSTRING(new_str), K(length), K(integer_start), K(integer_end),
            K(decimal_start), K(negative), K(integer_zero), K(decimal_zero));
        str = &new_str[0];
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_integer_v2_(str, integer_start, integer_end, decimal_zero, integer_zero, warning, fmt))) {
      LIB_LOG(WARN, "build integer fail", K(ret), K(length), "str", ObString(length, str), K(integer_start), K(integer_end), K(decimal_zero), K(integer_zero));
    } else if (OB_FAIL(build_decimal_v2_(str, length, decimal_start, integer_zero, decimal_zero, warning))) {
      LIB_LOG(WARN, "build decimal fail",  K(ret), K(length), "str", ObString(length, str), K(decimal_start), K(integer_zero), K(decimal_zero));
    } else if (OB_UNLIKELY(ib_.get_length() < 0) || OB_UNLIKELY(db_.get_length() < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "unexpected integer length or decimal length. ",
                    K(ret), K(ib_.get_length()), K(db_.get_length()));
    } else if (OB_UNLIKELY(ib_.get_length() + db_.get_length() > ObNumber::MAX_CALC_LEN)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LIB_LOG(WARN, "out of range, integer length, decimal length",
                    K(ret), K(ib_.get_length()), K(db_.get_length()));
    } else {
      number_.d_.sign_ = 0x01 & (negative ? ObNumber::NEGATIVE : ObNumber::POSITIVE);
      number_.d_.len_ = (uint8_t)(ib_.get_length() + db_.get_length());
      number_.d_.exp_ = 0x7f & (uint8_t)(ObNumber::EXP_ZERO + (ib_.get_length() != 0 ? ib_.get_exp() : db_.get_exp()));
      if (negative) {
        number_.d_.exp_ = (0x7f & ~number_.d_.exp_);
        ++number_.d_.exp_;
      }
      MEMCPY(number_.get_digits(), ib_.get_digits(), ib_.get_length() * ITEM_SIZE(number_.get_digits()));
      MEMCPY(number_.get_digits() + ib_.get_length(), db_.get_digits(), db_.get_length() * ITEM_SIZE(number_.get_digits()));

      if (NULL != precision && NULL != scale) {
        *scale = static_cast<int16_t>((length - decimal_start == -1) ? 0 : (length - decimal_start));
        if (*scale > ObNumber::FLOATING_SCALE) {
          *scale = ObNumber::FLOATING_SCALE;
        }
        *precision = static_cast<int16_t>(integer_end - integer_start + 1 - comma_cnt  + *scale);
      }
      ret = number_.normalize_v3_(false);
    }
  }
  LIB_LOG(DEBUG, "succ to build_v2", K(number_), K(ret), K(warning));
  return ret;
}

int ObNumberBuilder::build_integer_(const char *str, const int64_t integer_start,
                                    const int64_t integer_end, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  ib_.reset();
  int64_t skiped_zero_counter = 0;
  if (OB_UNLIKELY(integer_start <= integer_end && NULL == str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else if (integer_start >= 0 && integer_end >= 0) {
    for (int64_t i = integer_end; i >= integer_start; --i) {
      char c = str[i];
      if ('0' == c) {
        ++skiped_zero_counter;
        continue;
      } else {
        for (int64_t j = 0; j < skiped_zero_counter; ++j) {
          if (OB_FAIL(ib_.push(0, reduce_zero))) {
            LOG_WARN("push to integer builder fail", K(ret), K(j));
            break;
          }
        }
        if (OB_FAIL(ret)) {
          break;
        }
        skiped_zero_counter = 0;
      }
      if (OB_FAIL(ib_.push((uint8_t)(c - '0'), reduce_zero))) {
        LOG_WARN("push to integer builder fail", K(ret), K(c));
        break;
      }
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObNumberBuilder::build_integer_v2_(const char *str, const int64_t integer_start,
    const int64_t integer_end, const bool reduce_zero, const bool integer_zero, int &warning)
{
  int ret = OB_SUCCESS;
  static const uint32_t POWS[ObNumber::DIGIT_LEN] = {1, 10, 100, 1000, 10000, 100000,
                                                    1000000, 10000000, 100000000};

  if (integer_zero) {
    ib_.reset();
  } else if (OB_UNLIKELY(integer_start <= integer_end) && OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null", K(integer_start), K(integer_end), K(ret));
  } else if (OB_LIKELY(integer_start >= 0) && OB_LIKELY(integer_end >= 0)) {
    uint32_t tmp = 0;
    int64_t idx = 0;
    int64_t non_zero_start = integer_start;
    while (non_zero_start <= integer_end && str[non_zero_start] == '0') {
      ++non_zero_start;
    }
    for (int64_t i = integer_end; OB_SUCC(ret) && i >= non_zero_start; --i) {
      int32_t tmp_value = str[i] - '0';
      if (OB_UNLIKELY(tmp_value < 0) || OB_UNLIKELY(tmp_value > 9)) {
        warning = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "ObNumber got format error: got ", K(ret), K(warning), K(str[i]), K(tmp_value));
      } else {
        tmp += static_cast<uint32_t>(POWS[idx++] * tmp_value);
        if (idx >= ObNumber::DIGIT_LEN || i == non_zero_start) {
          if (OB_FAIL(ib_.push_digit(tmp, reduce_zero))) {
            LOG_WARN("push to integer builder fail", K(ret), K(i));
            break;
          }
          tmp = 0;
          idx = 0;
        }
      }
    }
  }
  return ret;
}

int ObNumberBuilder::hex_to_num_(char c, int32_t &val) const
{
  int ret = OB_SUCCESS;

  if (c >= '0' && c <= '9') {
    val = c - '0';
  } else if (c >= 'a' && c <= 'z') {
    val = c - 'a' + 10;
  } else if (c >= 'A' && c <= 'Z') {
    val = c - 'A' + 10;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bad hexadecimal character", K(ret));
  }
  return ret;
}

int ObNumberBuilder::multiply_(int32_t multiplier, char *str, int32_t &len) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else if (len >= ObNumber::MAX_TOTAL_SCALE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("str exceeds the max length");
  } else {
    int32_t carry = 0;
    int32_t i = len;
    int32_t tmp_str_idx = 0;
    char tmp_str[ObNumber::MAX_TOTAL_SCALE] = {0};
    while (OB_SUCC(ret) && --i >= 0) {
      int val = 0;
      if (OB_FAIL(hex_to_num_(str[i], val))) {
        LOG_ERROR("failed to hex_to_num_", K(ret));
      } else {
        val = val * multiplier + carry;
        carry = std::floor(val / 10);
        tmp_str[tmp_str_idx++] = (val % 10) + '0';
      }
    }
    if (OB_SUCC(ret)) {
      while (carry > 0) {
        tmp_str[tmp_str_idx++] = (carry % 10) + '0';
        carry = std::floor(carry / 10);
      }
      // reverse tmp str
      len = 0;
      i = --tmp_str_idx;
      for (; i >= 0; --i) {
        str[len++] = tmp_str[i];
      }
    }
  }
  return ret;
}

int ObNumberBuilder::add_hex_str_(const char *str1, int32_t str1_len,
                                  char *str2, int32_t &str2_len) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(str1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else if (str1_len >= ObNumber::MAX_TOTAL_SCALE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("str exceeds the max length");
  } else {
    int32_t carry = 0;
    int32_t i = 0;
    int val = 0;
    int32_t tmp_str_idx = 0;
    char tmp_str[ObNumber::MAX_TOTAL_SCALE] = {0};
    while (OB_SUCC(ret) && i < str1_len && i < str2_len) {
      int32_t val1 = 0;
      int32_t val2 = 0;
      if (OB_FAIL(hex_to_num_(str1[str1_len - i - 1], val1))
         || OB_FAIL(hex_to_num_(str2[str2_len - i - 1], val2))) {
        LOG_ERROR("failed to hex_to_num_", K(ret));
      } else {
        val = val1 + val2 + carry;
        carry = std::floor(val / 10);
        tmp_str[tmp_str_idx++] = (val % 10) + '0';
        i++;
      }
    }
    while(OB_SUCC(ret) && i < str1_len) {
      int32_t val1 = 0;
      if (OB_FAIL(hex_to_num_(str1[str1_len - i - 1], val1))) {
        LOG_ERROR("failed to hex_to_num_", K(ret));
      } else {
        val = val1 + carry;
        carry = std::floor(val / 10);
        tmp_str[tmp_str_idx++] = (val % 10) + '0';
        i++;
      }
    }
    while(OB_SUCC(ret) && i < str2_len) {
      int32_t val2 = 0;
      if (OB_FAIL(hex_to_num_(str2[str2_len - i - 1], val2))) {
        LOG_ERROR("failed to hex_to_num_", K(ret));
      } else {
        val = val2 + carry;
        carry = std::floor(val / 10);
        tmp_str[tmp_str_idx++] = (val % 10) + '0';
        i++;
      }
    }
    if (OB_SUCC(ret)) {
      while (carry > 0) {
        tmp_str[tmp_str_idx++] = (carry % 10) + '0';
        carry = std::floor(carry / 10);
      }
      // reverse tmp str
      str2_len = 0;
      i = --tmp_str_idx;
      for (; i >= 0; --i) {
        str2[str2_len++] = tmp_str[i];
      }
    }
  }
  return ret;
}

int ObNumberBuilder::hex_to_dec_(const char *hex_str, int32_t hex_len,
                                 char *dec_str, int32_t &dec_len) const
{
  int ret = OB_SUCCESS;
  int32_t i = 0;
  char str[ObNumber::MAX_TOTAL_SCALE] = {0};
  while (OB_SUCC(ret) && i < hex_len) {
    int32_t j = i;
    str[0] = hex_str[hex_len - 1 - i];
    int32_t str_len = 1;
    while (OB_SUCC(ret) && j--) {
      if (OB_FAIL(multiply_(16, str, str_len))) {
        LOG_WARN("failed to multiply_", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ret = add_hex_str_(str, str_len, dec_str, dec_len);
      i++;
    }
  }

  return ret;
}

int ObNumberBuilder::build_hex_integer_(const char *str, const int64_t integer_start,
                                        const int64_t integer_end, const bool reduce_zero,
                                        ObNumberFmtModel * fmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fmt)) {
    ret = build_integer_(str, integer_start, integer_end, reduce_zero);
  } else {
    // the maximum length of Oracle to_number(123, 'xxx') format is 63
    const int32_t MAX_FORMAT_LEN = 64;
    int64_t c_p = fmt->fmt_len_ - 1;
    ib_.reset();
    if (OB_UNLIKELY(c_p >= MAX_FORMAT_LEN)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("the format exceeds the max length");
    } else if (OB_UNLIKELY(integer_start <= integer_end && NULL == str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("the pointer is null");
    } else if (integer_start >= 0 && integer_end >= 0) {
      int32_t new_len = 0;
      bool digit_appeared = false;
      int64_t i = integer_start;
      char hex_str[ObNumber::MAX_TOTAL_SCALE] = {0};
      char dec_str[ObNumber::MAX_TOTAL_SCALE] = {0};
      for (; i <= integer_end && c_p >= 0; ++i) {
        char c = str[i];
        switch(c) {
          case ',':
            /* do nothing */
            break;
          case '0':
            if (!digit_appeared) {
              /* do nothing */
            } else {
              hex_str[new_len++] = c;
              --c_p;
            }
            break;
          case 'a'...'f':
          case 'A'...'F':
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            digit_appeared = true;
            hex_str[new_len++] = c;
            --c_p;
            break;
          default:
            ret = OB_INVALID_NUMERIC;
            LOG_WARN("ObNumber got str error: got ", K(ret), K(c));
            break;
        }
      }
      if (i < integer_end) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("integer part is longer than fmt str", K(ret), K(i), K(c_p));
      }
      int32_t dec_len = 0;
      if (OB_FAIL(hex_to_dec_(hex_str, new_len, dec_str, dec_len))) {
        LOG_WARN("failed to hex_to_dec", K(ret));
      }
      i = dec_len - 1;
      for(; i >= 0; --i) {
        char c = dec_str[i];
        if (OB_FAIL(ib_.push((uint8_t)(c - '0'), reduce_zero))) {
          LOG_WARN("push to integer builder fail", K(ret), K(c));
          break;
        }
      }
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObNumberBuilder::build_integer_(const char *str, const int64_t integer_start,
                                    const int64_t integer_end, const bool reduce_zero,
                                    ObNumberFmtModel * fmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fmt)) {
    ret = build_integer_(str, integer_start, integer_end, reduce_zero);
  } else {
    ib_.reset();
    int64_t skiped_zero_counter = 0;
    if (OB_UNLIKELY(integer_start <= integer_end && NULL == str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("the pointer is null");
    } else if (integer_start >= 0 && integer_end >= 0) {
      int64_t c_p = fmt->fmt_len_ - 1;
      char* fmt_str = fmt->fmt_str_;
      bool got_comma_in_fmt = false;
      bool pass_first_comma = false;
      if (fmt->dc_position_ >= 0) {
        c_p = fmt->dc_position_ - 1;
      }
      //comma:       4   32  1        4   32
      //to_number('76,669,,83,5', '999,999,,999')=>76669835
      //digit:     81 654  32 1    987 654  321
      //check from right to left, before comma appears at fmt str for the first time, all commas in number_str can be ignored
      //after the first appearance, commas in number_str must match with the fmt str
      //so as the upper case, comma 1 can be ignored, comma 2\3\4 are match with fmt str
      int64_t i = integer_end;
      for (; i >= integer_start && c_p >= 0; --i) {
        char c = str[i];
        if ('0' == c) {
          ++skiped_zero_counter;
          --c_p;
          if (got_comma_in_fmt && !pass_first_comma) {
            pass_first_comma = true;
          }
          continue;
        } else if (',' == c) {
          if (pass_first_comma && ',' != fmt_str[c_p]) {
            ret = OB_INVALID_NUMERIC;
            LOG_WARN("comma(s) not match", K(ret), K(c_p), K(i), K(fmt_str[c_p]));
            break;
          } else if (',' == fmt_str[c_p]) {
            got_comma_in_fmt = true;
            --c_p;
          }
//          LOG_DEBUG("ignore comma", K(ret), K(c_p), K(i), K(fmt_str[c_p]));
          continue;
        } else if (',' == fmt_str[c_p]) {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("got error during build integer", K(ret), K(i), K(fmt_str[c_p]), K(c_p));
          break;
        } else {
          if (got_comma_in_fmt && !pass_first_comma) {
            pass_first_comma = true;
          }
          for (int64_t j = 0; j < skiped_zero_counter; ++j) {
            if (OB_FAIL(ib_.push(0, reduce_zero))) {
              LOG_WARN("push to integer builder fail", K(ret), K(j));
              break;
            }
          }
          if (OB_FAIL(ret)) {
            break;
          }
          skiped_zero_counter = 0;
          --c_p;
        }
        if (OB_FAIL(ib_.push((uint8_t)(c - '0'), reduce_zero))) {
          LOG_WARN("push to integer builder fail", K(ret), K(c));
          break;
        }
      }
      /* fmt 0: there must be a digit that matches with the 0 */
      if (c_p >= 0) {
        while(c_p >= 0) {
          if (fmt_str[c_p] == '0') {
            ret = OB_INVALID_NUMERIC;
            LOG_WARN("there has no digit that matches the 0(s) in the fmt str",
                K(ret), K(i), K(c_p));
            break;
          }
          c_p--;
        }
      }
      if (i >= integer_start) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("integer part is longer than fmt str", K(ret), K(i), K(c_p));
      }
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObNumberBuilder::build_integer_v2_(const char *str, const int64_t integer_start,
    const int64_t integer_end, const bool reduce_zero, const bool integer_zero,
    int &warning, ObNumberFmtModel * fmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fmt)) {
    ret = build_integer_v2_(str, integer_start, integer_end, reduce_zero, integer_zero, warning);
  } else if (fmt->has_x_) {
    ret = build_hex_integer_(str, integer_start, integer_end, reduce_zero, fmt);
  } else {
    ret = build_integer_(str, integer_start, integer_end, reduce_zero, fmt);
  }
  return ret;
}

int ObNumberBuilder::build_decimal_(const char *str, const int64_t length,
                                    const int64_t decimal_start, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  db_.reset();
  int64_t skiped_zero_counter = 0;
  if (decimal_start < length && OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else {
    for (int64_t i = decimal_start; i < length; ++i) {
      char c = str[i];
      if ('0' == c) {
        ++skiped_zero_counter;
        continue;
      } else {
        for (int64_t j = 0; j < skiped_zero_counter; ++j) {
          if (OB_FAIL(db_.push(0, reduce_zero))) {
            LOG_WARN("push to decimal builder fail", K(ret), K(j));
            break;
          }
        }
        if (OB_FAIL(ret)) {
          break;
        }
        skiped_zero_counter = 0;
      }
      if (OB_FAIL(db_.push((uint8_t)(c - '0'), reduce_zero))) {
        LOG_WARN("push to decimal builder fail", K(ret), K(c));
        break;
      }
    }
  }

  return ret;
}


int ObNumberBuilder::build_decimal_v2_(const char *str, const int64_t length,
    const int64_t decimal_start, const bool reduce_zero, const bool decimal_zero, int &warning)
{
  int ret = OB_SUCCESS;
  static const uint32_t POWS[ObNumber::DIGIT_LEN] = {100000000, 10000000, 1000000,
                                                     100000, 10000, 1000, 100, 10, 1};
  if (decimal_zero) {
    db_.reset();
  } else if (OB_UNLIKELY(decimal_start <= length) && OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null", K(decimal_start), K(length), K(ret));
  } else {
    uint32_t tmp = 0;
    int64_t idx = 0;
    int64_t decimal_end = length - 1;
    while (decimal_end >= decimal_start && str[decimal_end] == '0') {
      --decimal_end;
    }
    for (int64_t i = decimal_start; OB_SUCC(ret) && i <= decimal_end; ++i) {
      int32_t tmp_value = str[i] - '0';
      if (OB_UNLIKELY(tmp_value < 0) || OB_UNLIKELY(tmp_value > 9)) {
        warning = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "ObNumber got format error: got ", K(ret), K(warning), K(str[i]), K(tmp_value));
      } else {
        tmp += static_cast<uint32_t>(POWS[idx++] * tmp_value);
        if (idx >= ObNumber::DIGIT_LEN || i == decimal_end) {
          if (OB_FAIL(db_.push_digit(tmp, reduce_zero))) {
            LOG_WARN("push to decimal builder fail", K(ret), K(i));
            break;
          }
          tmp = 0;
          idx = 0;
        }
      }
    }
  }
  return ret;
}

int ObNumberBuilder::find_point_(
    const char *str,
    int64_t &length,
    int64_t &integer_start,
    int64_t &integer_end,
    int64_t &decimal_start,
    bool &negative,
    bool &integer_zero,
    bool &decimal_zero,
    int &warning)
{
  int ret = OB_SUCCESS;
  int64_t i_integer_start = -2;
  int64_t dot_idx = -1;
  bool b_negative = false;
  bool b_integer_zero = true;
  bool b_decimal_zero = true;
  bool sign_appeared = false;
  bool digit_appeared = false;
  bool dot_appeared = false;
  int64_t i = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the str pointer is null", K(ret));
  } else {
    for (i = 0; i < length && isspace(str[i]); ++i);
    for (; i + 1 < length && isspace(str[length - 1]); --length);
    if (OB_UNLIKELY(i == length)) {
      ret = OB_INVALID_NUMERIC;
    } else {
      for (; OB_SUCCESS == warning && i < length; ++i) {
        char c = str[i];
        switch (c) {
          case '-':
          case '+':
            if (sign_appeared || digit_appeared || dot_appeared) {
              warning = OB_INVALID_NUMERIC;
//              LOG_DEBUG("invalid numeric", K(sign_appeared), K(digit_appeared), K(dot_appeared));
            } else {
              if ('-' == c) {
                b_negative = true;
              }
              sign_appeared = true;
            }
            break;
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            if (!dot_appeared) {
              b_integer_zero = false;
            } else {
              b_decimal_zero = false;
            }
            /* no break. */
          case '0':
            if (-2 == i_integer_start) {
              i_integer_start = i;
            }
            digit_appeared = true;
            break;
          case '.':
            if (dot_appeared) {
              warning = OB_INVALID_NUMERIC;
//              LOG_DEBUG("invalid numeric", K(dot_appeared));
            } else {
              if (!digit_appeared) {
                //".95" means "0.95"
                // i_integer_start and i_integer_end both will be -1
                i_integer_start = -1;
              }
              dot_appeared = true;
              dot_idx = i;
            }
            break;
          default:
            warning = OB_INVALID_NUMERIC;
//            _LOG_DEBUG("invalid numeric default, c=%x", c);
            break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != warning) {
        length = i - 1;
      }
      if (!dot_appeared) {
        dot_idx = length;
      }
      negative = b_negative;
      integer_zero = b_integer_zero;
      decimal_zero = b_decimal_zero;
      integer_start = i_integer_start;
      integer_end   = dot_idx - 1;
      decimal_start = dot_idx + 1;
      ObString tmp_str(length, str);
      LOG_DEBUG("find v1", K(tmp_str), K(negative), K(integer_zero),
                K(decimal_zero), K(integer_start), K(dot_idx), K(i), K(length));
    }
  }

  return ret;
}

int ObNumberBuilder::find_point_v2_(
    const char *str,
    int64_t &length,
    int64_t &integer_start,
    int64_t &integer_end,
    int64_t &decimal_start,
    bool &negative,
    bool &integer_zero,
    bool &decimal_zero,
    int &warning)
{
  int ret = OB_SUCCESS;
  int64_t i_integer_start = -2;
  int64_t dot_idx = -1;
  bool b_negative = false;
  bool b_integer_zero = true;
  bool b_decimal_zero = true;
  int64_t i = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the str pointer is null", K(ret));
  } else {
    for (i = 0; i < length && isspace(str[i]); ++i);
    for (; i + 1 < length && isspace(str[length - 1]); --length);
    if (OB_UNLIKELY(i == length)) {
      if (0 == i) {
        ret = OB_INVALID_NUMERIC;
      } else {
        //empty string is enabled
        warning = OB_INVALID_NUMERIC;
      }
    } else {
      if ('-' == str[i]) {
        b_negative = true;
        ++i;
      } else if ('+' == str[i]) {
        ++i;
      }
      if (i == length) {
        dot_idx = length;
      } else {
        const char *dot_pos = static_cast<const char *>(memchr(str + i, '.', length - i));
        if (NULL == dot_pos) {
          dot_idx = length;
          i_integer_start = i;
          b_integer_zero = false;
          b_decimal_zero = true;
        } else {
          dot_idx = dot_pos - str;
          i_integer_start = (i == dot_idx) ? -1 : i;
          //todo can be used
          b_integer_zero = ((dot_idx == i
                             || ((dot_idx - i) == static_cast<int64_t>(strspn(str + i, "0"))))
                            ? true
                            : false);
          b_decimal_zero = (((dot_idx == length - 1)
                              || ((length - 1 - dot_idx) == static_cast<int64_t>(strspn(str + dot_idx + 1, "0"))))
                            ? true
                            : false);
        }
      }
    }
    if (OB_SUCC(ret)) {
      negative = b_negative;
      integer_zero = b_integer_zero;
      decimal_zero = b_decimal_zero;
      integer_start = i_integer_start;
      integer_end   = dot_idx - 1;
      decimal_start = dot_idx + 1;
//      ObString tmp_str(length, str);
//      LOG_DEBUG("find v2", K(tmp_str), K(negative), K(integer_zero),
//          K(decimal_zero), K(integer_start), K(dot_idx), K(i), K(length));
    }
  }
  return ret;
}

int ObNumberBuilder::find_point_(
    const char *str,
    ObNumberFmtModel *fmt,
    char* new_str,
    int64_t &length,
    int64_t &integer_start,
    int64_t &integer_end,
    int64_t &decimal_start,
    bool &negative,
    bool &integer_zero,
    bool &decimal_zero,
    int64_t &comma_cnt)
{
  int ret = OB_SUCCESS;
  int64_t i_integer_start = -2;
  int64_t dot_idx = -1;
  bool b_negative = false;
  bool b_integer_zero = true;
  bool b_decimal_zero = true;
  bool sign_appeared = false;
  bool digit_appeared = false;
  bool dot_appeared = false;
  bool dollar_appeared = false;
  int64_t heading_space = 0;
  int64_t trailing_space = 0;
  int64_t i = 0;
  int64_t new_len = 0;
  int64_t b_comma_cnt = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "the str pointer is null", K(ret));
  } else {
    for (i = 0; i < length && isspace(str[i]); ++i);
    heading_space = i;
    for (; i + 1 < length && isspace(str[length - 1]); --length, ++trailing_space);

    if (OB_UNLIKELY(i == length)) {
      //to_number('     ', '9B9.9') => 0
      if (!fmt->has_b_ || fmt->has_currency_ || fmt->has_sign_
          || heading_space + trailing_space != fmt->fmt_len_ + 1) {
        ret = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "ObNumber got format error", K(ret), K(fmt->has_b_), K(heading_space), K(trailing_space), K(fmt->fmt_len_));
      } else {
        new_str[0] = '0';
        digit_appeared = true;
        dot_idx = 0;
        length = 1;
        i = 1;
        LIB_LOG(WARN, "ObNumber format:cast spaces to zero", K(ret));
      }
    } else {
      for (; OB_SUCCESS == ret && i < length; ++i) {
        char c = str[i];
        switch (c) {
          case '-':
          case '+':
            if (sign_appeared
                || ((!fmt->has_sign_ || fmt->sign_position_ == FmtFirst) && (digit_appeared || dot_appeared))
                || (fmt->sign_position_ == FmtLast && (i != (length - 1)))) {
              ret = OB_INVALID_NUMERIC;
              LIB_LOG(WARN, "ObNumber got format error", K(ret), K(fmt->sign_position_),
                  K(digit_appeared), K(dot_appeared), K(i));
            } else {
              if ('-' == c) {
                b_negative = true;
              } else if (!fmt->has_sign_) {
                ret = OB_INVALID_NUMERIC;
                LIB_LOG(WARN, "ObNumber got format error:no sign in fmt str, but got '+'", K(ret));
              }
              sign_appeared = true;
            }
            break;
          case 'a'...'f':
          case 'A'...'F':
            if (!fmt->has_x_) {
              ret = OB_INVALID_NUMERIC;
              LIB_LOG(WARN, "ObNumber got format error: got ", K(ret), K(c));
            }
            /* no break. */
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            if (!dot_appeared) {
              b_integer_zero = false;
            } else {
              b_decimal_zero = false;
            }
            /* no break. */
          case '0':
            if (-2 == i_integer_start) {
              i_integer_start = new_len;
            }
            digit_appeared = true;
            new_str[new_len++] = c;
            break;
          case '.':
            if (dot_appeared) {
              ret = OB_INVALID_NUMERIC;
              LIB_LOG(WARN, "ObNumber got format error:dot appeared more than one time");
            } else {
              if (!digit_appeared) {
                //".95" means "0.95"
                // i_integer_start and i_integer_end both will be -1
                i_integer_start = -1;
              }
              dot_appeared = true;
              dot_idx = new_len;
              new_str[new_len++] = c;
            }
            break;
          case ',':
            if (dot_appeared) {
              ret = OB_INVALID_NUMERIC;
              LIB_LOG(WARN, "ObNumber got format error:comma appears at decimal part");
            } else if (fmt->has_comma_) {
              new_str[new_len++] = ',';
              ++b_comma_cnt;
            } else {
              //ignore comma
            }
            break;
          case '$':
            if (dollar_appeared || dot_appeared || digit_appeared) {
              ret = OB_INVALID_NUMERIC;
              LIB_LOG(WARN, "ObNumber got format error:$ can only be the first valid character");
            } else {
              dollar_appeared = true;
            }
            break;
          default:
            ret = OB_INVALID_NUMERIC;
            LIB_LOG(WARN, "ObNumber got format error: got ", K(ret), K(c));
            break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!digit_appeared && (dollar_appeared || sign_appeared)) {
        ret = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "ObNumber got error according to fmt ", K(ret), K(digit_appeared),
            K(dollar_appeared));
      } else if ((fmt->has_sign_ && !sign_appeared)
                 || (fmt->has_currency_ && !dollar_appeared)) {
        ret = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "ObNumber got error according to fmt", K(ret), K(fmt->has_sign_),
            K(sign_appeared), K(fmt->has_currency_), K(dollar_appeared));
      } else if (fmt->has_x_ && (b_negative || dot_appeared)) {
        ret = OB_INVALID_NUMERIC;
        LIB_LOG(WARN, "ObNumber got error according to fmt ", K(ret), K(dot_appeared),
            K(fmt->has_x_));
      } else {
        if (!dot_appeared) {
          dot_idx = new_len;
        }
        negative = b_negative;
        integer_zero = b_integer_zero;
        decimal_zero = b_decimal_zero;
        integer_start = i_integer_start;
        integer_end   = dot_idx - 1;
        decimal_start = dot_idx + 1;
        comma_cnt = b_comma_cnt;
        //to_number('12.34', '9.99') => error
        //to_number('1.32', '9999.9') => error
        //to_number('1.32', '9999.999') => 1.32
        if (((fmt->dc_position_ > 0) && ((integer_end - integer_start + 1 - comma_cnt) > fmt->dc_position_))
            || (fmt->has_x_ && new_len > fmt->fmt_len_)
            || (dot_appeared && ((new_len - decimal_start) >
                    (fmt->dc_position_ > 0 ? (fmt->fmt_len_ - fmt->dc_position_ - 1) : 0)))) {
          LIB_LOG(WARN, "ObNumber got error according to fmt", K(integer_end), K(integer_start),
              K(fmt->dc_position_), K(dot_appeared), K(new_len), K(decimal_start),
              K(fmt->fmt_len_), K(fmt->dc_position_), K(comma_cnt));
          ret = OB_INVALID_NUMERIC;
        }
        LIB_LOG(DEBUG, "ObNumber process fmt success", KCSTRING(new_str), K(length), K(new_len),
            K(dot_appeared));
        length = new_len;
      }
    }
  }
  return ret;
}

int64_t ObNumberBuilder::get_exp() const
{
  return number_.d_.exp_;
}

int64_t ObNumberBuilder::get_length() const
{
  return number_.d_.len_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int ObDigitIterator::get_next_digit(uint32_t &digit, bool &from_integer, bool &last_decimal)
{
  int ret = OB_SUCCESS;
  if (0 <= iter_exp_) {
    from_integer = true;
    if (iter_idx_ < iter_len_) {
      ret = get_digit(iter_idx_++, digit);
    } else {
      digit = 0;
    }
    --iter_exp_;
  } else {
    from_integer = false;
    if (-1 == iter_exp_) {
      if (iter_idx_ >= iter_len_) {
        ret = OB_ITER_END;
      } else {
        ret = get_digit(iter_idx_++, digit);
        last_decimal = (iter_idx_ == iter_len_);
      }
    } else {
      digit = 0;
      ++iter_exp_;
    }
  }
  return ret;
}

ObDigitIterator::NextDigitEnum ObDigitIterator::get_next_digit(uint32_t &digit)
{
  NextDigitEnum nd_enum = ND_END;
  if (0 <= iter_exp_) {
    nd_enum = (0 == iter_idx_ ? ND_HEAD_INTEGER : ND_BODY_INTEGER);
    if (iter_idx_ < iter_len_) {
      digit = number_.get_digits()[iter_idx_++];
    } else {
      digit = 0;
    }
    --iter_exp_;
  } else {
    if (-1 == iter_exp_) {
      if (iter_idx_ < iter_len_) {
        digit = number_.get_digits()[iter_idx_++];
        nd_enum = (iter_idx_ == iter_len_ ? ND_TAIL_DECIMAL : ND_BODY_DECIMAL);
      } else {
        nd_enum = ND_END;
      }
    } else {
      digit = 0;
      ++iter_exp_;
      nd_enum = ND_BODY_DECIMAL;
    }
  }
  return nd_enum;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObCalcVector::ObCalcVector() : base_(ObNumber::BASE),
                               length_(0),
                               digits_(buffer_)
{
}

ObCalcVector::~ObCalcVector()
{
}

ObCalcVector::ObCalcVector(const ObCalcVector &other)
{
  LIB_LOG(DEBUG, "copy assignment invoked");
  *this = other;
}

ObCalcVector &ObCalcVector::operator =(const ObCalcVector &other)
{
  LIB_LOG(DEBUG, "operator = invoked");
  if (this != &other) {
    base_ = other.base_;
    length_ = other.length_;
    digits_ = other.digits_;
  }
  return *this;
}

int ObCalcVector::init(const uint32_t desc, uint32_t *digits)
{
  ObDigitIterator di;
  int ret = OB_SUCCESS;
  di.assign(desc, digits);
  bool head_zero = true;
  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  length_ = 0;
  digits_ = buffer_;
  while (OB_SUCCESS == (ret = di.get_next_digit(digit, from_integer, last_decimal))) {
    if (head_zero) {
      if (0 == digit) {
        continue;
      } else {
        head_zero = false;
      }
    }
    digits_[length_ ++] = digit;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

uint64_t ObCalcVector::at(const int64_t idx) const
{
  uint64_t ret_digit = 0;
  if (OB_ISNULL(digits_)) {
    LOG_ERROR_RET(OB_ERROR, "the pointer is null");
  } else if (OB_UNLIKELY(idx <0 || idx > length_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "the param is invalid");
  } else {
    ret_digit = digits_[idx];
  }
  return ret_digit;
}

uint64_t ObCalcVector::base() const
{
  return base_;
}

void ObCalcVector::set_base(const uint64_t base)
{
  base_ = base;
}

int64_t ObCalcVector::size() const
{
  return length_;
}

uint32_t *ObCalcVector::get_digits()
{
  return digits_;
}

int ObCalcVector::normalize()
{
  int64_t i = 0;
  int ret = OB_SUCCESS;
  if (length_ > 0 && OB_ISNULL(digits_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else {
    for (; i < length_; ++i) {
      if (0 != digits_[i]) {
        break;
      }
    }
    length_ = length_ - i;
    if (0 == length_) {
      digits_ = NULL;
    } else {
      digits_ = &digits_[i];
    }
  }

  return ret;
}

int ObCalcVector::set(const int64_t idx, const uint64_t digit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx
      || idx >= length_
      || base_ <= digit
      || NULL == digits_)) {
    LOG_ERROR("invalid param ",
              K(idx), K(length_), K(digit), K(base_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    digits_[idx] = (uint32_t)digit;
  }
  return ret;
}

int ObCalcVector::ensure(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else if (OB_UNLIKELY(ObNumber::MAX_CALC_LEN < size)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else if (OB_UNLIKELY(digits_ < &buffer_[0]
             || digits_ > &buffer_[ObNumber::MAX_CALC_LEN - 1])) {
    LOG_ERROR("digits is read only ", K(digits_), K(buffer_));
    ret = OB_ERR_READ_ONLY;
  } else {
    length_ = size;
  }
  return ret;
}

int ObCalcVector::resize(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("the pointer is null");
  } else if (OB_UNLIKELY(ObNumber::MAX_CALC_LEN < size)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else if (OB_UNLIKELY(digits_ < &buffer_[0]
             || digits_ > &buffer_[ObNumber::MAX_CALC_LEN - 1])) {
    LOG_ERROR("digits is read only", K(digits_), K(buffer_));
    ret = OB_ERR_READ_ONLY;
  } else {
    MEMSET(digits_, 0, size * ITEM_SIZE(digits_));
    length_ = size;
  }
  return ret;
}

ObCalcVector ObCalcVector::ref(const int64_t start, const int64_t end) const
{
  ObCalcVector ret_calc_vec;
  if (OB_ISNULL(digits_)) {
    LOG_ERROR_RET(OB_ERROR, "the pinter is null");
  } else {
    ret_calc_vec.length_ = end - start + 1;
    ret_calc_vec.digits_ = &digits_[start];
    ret_calc_vec.set_base(this->base());
  }

  return ret_calc_vec;
}

int ObCalcVector::assign(const ObCalcVector &other, const int64_t start, const int64_t end)
{
  int ret = OB_SUCCESS;
  if (0 < (end - start + 1)) {
    if (OB_ISNULL(digits_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR( "the pointer is null");
    } else {
      MEMCPY(&digits_[start], other.digits_, (end - start + 1) * ITEM_SIZE(digits_));
    }
  }
  return ret;
}

int64_t ObCalcVector::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  if ((length_ > 0 && OB_ISNULL(digits_)) || length_ < 0) {
    databuff_printf(buffer, length, pos, "the value is invalid");
  } else {
    databuff_printf(buffer, length, pos, "\"{length=%ld digits_ptr=%p buffer_ptr=%p digits=[",
                    length_, digits_, buffer_);
    for (int64_t i = 0; i < length_; ++i) {
      databuff_printf(buffer, length, pos, "%u,", digits_[i]);
    }
    databuff_printf(buffer, length, pos, "]}\"");
  }

  return pos;
}
}
}
}
