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

#include "lib/charset/ob_ctype.h"
#include "lib/charset/ob_dtoa.h"

#define CUTOFF (UINT64_MAX / 10)
#define CUTLIM (UINT64_MAX % 10)
#define DIGITS_IN_ULONGLONG 20

static uint64_t d10[DIGITS_IN_ULONGLONG] = {1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL,
    10000000000000000000ULL};

int64_t ob_strntoll_8bit(const char* str, size_t len, int base, char** endptr, int* err)
{
  int is_negative;
  uint64_t cutoff;
  unsigned int cutlim;
  uint64_t tmp_res;
  const char *s, *e;
  const char* save;
  int overflow;

  *err = 0;

  s = str;
  e = str + len;

  for (; s < e && 0x20 == *s; s++)
    ;

  if (s == e) {
    *err = EDOM;
    if (endptr != NULL) {
      *endptr = (char*)str;
    }
    return 0L;
  }

  if ('-' == *s) {
    is_negative = 1;
    ++s;
  } else if ('+' == *s) {
    is_negative = 0;
    ++s;
  } else {
    is_negative = 0;
  }

  save = s;

  cutoff = UINT64_MAX / (unsigned int)base;
  cutlim = (unsigned int)(UINT64_MAX % (unsigned int)base);

  overflow = 0;
  tmp_res = 0;
  for (; s != e; s++) {
    unsigned char c = *s;
    if (c >= '0' && c <= '9') {
      c -= '0';
    } else if (c >= 'A' && c <= 'Z') {
      c = c - 'A' + 10;
    } else if (c >= 'a' && c <= 'z') {
      c = c - 'a' + 10;
    } else {
      break;
    }
    if (c >= base) {
      break;
    }
    if (tmp_res > cutoff || (tmp_res == cutoff && c > cutlim)) {
      overflow = 1;
    } else {
      tmp_res *= (uint64_t)base;
      tmp_res += c;
    }
  }

  if (s == save) {
    *err = EDOM;
    if (endptr != NULL) {
      *endptr = (char*)str;
    }
    return 0L;
  }

  if (endptr != NULL) {
    *endptr = (char*)s;
  }

  if (is_negative) {
    if (tmp_res > (uint64_t)INT64_MIN) {
      overflow = 1;
    }
  } else if (tmp_res > (uint64_t)INT64_MAX) {
    overflow = 1;
  }

  if (overflow) {
    *err = ERANGE;
    return is_negative ? INT64_MIN : INT64_MAX;
  }

  return (is_negative ? -((int64_t)tmp_res) : (int64_t)tmp_res);
}

uint64_t ob_strntoull_8bit(const char* str, size_t len, int base, char** endptr, int* err)
{
  int is_negative;
  uint64_t cutoff;
  unsigned int cutlim;
  uint64_t tmp_res;
  const char *s, *e;
  const char* save;
  int overflow;

  *err = 0;

  s = str;
  e = str + len;

  for (; s < e && 0x20 == *s; s++)
    ;

  if (s == e) {
    *err = EDOM;
    if (endptr != NULL) {
      *endptr = (char*)str;
    }
    return 0L;
  }

  if (*s == '-') {
    is_negative = 1;
    ++s;
  } else if (*s == '+') {
    is_negative = 0;
    ++s;
  } else {
    is_negative = 0;
  }

  save = s;

  cutoff = (UINT64_MAX / (unsigned int)base);
  cutlim = (unsigned int)(UINT64_MAX % (unsigned int)base);

  overflow = 0;
  tmp_res = 0;
  for (; s != e; s++) {
    unsigned char c = *s;
    if (c >= '0' && c <= '9') {
      c -= '0';
    } else if (c >= 'A' && c <= 'Z') {
      c = c - 'A' + 10;
    } else if (c >= 'a' && c <= 'z') {
      c = c - 'a' + 10;
    } else {
      break;
    }
    if (c >= base) {
      break;
    }
    if (tmp_res > cutoff || (tmp_res == cutoff && c > cutlim)) {
      overflow = 1;
    } else {
      tmp_res *= (unsigned int)base;
      tmp_res += c;
    }
  }

  if (s == save) {
    *err = EDOM;
    if (endptr != NULL) {
      *endptr = (char*)str;
    }
    return 0L;
  }

  if (endptr != NULL) {
    *endptr = (char*)s;
  }

  if (overflow) {
    err[0] = ERANGE;
    return UINT64_MAX;
  } else if (is_negative) {
    err[0] = ERANGE;
    return -((int64_t)tmp_res);
  } else {
    return ((int64_t)tmp_res);
  }

  return tmp_res;
}

double ob_strntod_8bit(char* str, size_t len, char** end, int* err)
{
  if (len > UINT16_MAX) {
    len = UINT16_MAX;
  }
  *end = str + len;
  return ob_strtod(str, end, err);
}

uint64_t ob_strntoull10rnd_8bit(const char* str_ptr, size_t str_len, int unsigned_flag, char** end_ptr, int* error)
{
  const char *dot_ptr, *end_ptr9, *beg_ptr, *end = str_ptr + str_len;
  unsigned long long value_ull;
  unsigned long value_ul;
  unsigned char ch;
  int shift_value = 0, digits_value = 0, negative_sign, addon_value;

  for (; str_ptr < end && (*str_ptr == ' ' || *str_ptr == '\t'); str_ptr++)
    ;

  if (str_ptr >= end) {
    goto ret_edom;
  }

  if ((negative_sign = (*str_ptr == '-')) || *str_ptr == '+') {
    if (++str_ptr == end) {
      goto ret_edom;
    }
  }

  beg_ptr = str_ptr;
  end_ptr9 = (str_ptr + 9) > end ? end : (str_ptr + 9);
  for (value_ul = 0; str_ptr < end_ptr9 && (ch = (unsigned char)(*str_ptr - '0')) < 10; str_ptr++) {
    value_ul = value_ul * 10 + ch;
  }

  if (str_ptr >= end) {
    *end_ptr = (char*)str_ptr;
    if (negative_sign) {
      if (unsigned_flag) {
        *error = value_ul ? MY_ERRNO_ERANGE : 0;
        return 0;
      } else {
        *error = 0;
        return (uint64_t)(int64_t) - (int64_t)value_ul;
      }
    } else {
      *error = 0;
      return (uint64_t)value_ul;
    }
  }

  digits_value = str_ptr - beg_ptr;

  for (dot_ptr = NULL, value_ull = value_ul; str_ptr < end; str_ptr++) {
    if ((ch = (unsigned char)(*str_ptr - '0')) < 10) {
      if (value_ull < CUTOFF || (value_ull == CUTOFF && ch <= CUTLIM)) {
        value_ull = value_ull * 10 + ch;
        digits_value++;
        continue;
      }

      if (value_ull == CUTOFF) {
        value_ull = UINT64_MAX;
        addon_value = 1;
        str_ptr++;
      } else {
        addon_value = (*str_ptr >= '5');
      }
      if (!dot_ptr) {
        for (; str_ptr < end && (ch = (unsigned char)(*str_ptr - '0')) < 10; shift_value++, str_ptr++)
          ;
        if (str_ptr < end && *str_ptr == '.') {
          str_ptr++;
          for (; str_ptr < end && (ch = (unsigned char)(*str_ptr - '0')) < 10; str_ptr++)
            ;
        }
      } else {
        shift_value = dot_ptr - str_ptr;
        for (; str_ptr < end && (ch = (unsigned char)(*str_ptr - '0')) < 10; str_ptr++)
          ;
      }
      goto exp;
    }

    if (*str_ptr == '.') {
      if (dot_ptr) {
        addon_value = 0;
        goto exp;
      } else {
        dot_ptr = str_ptr + 1;
      }
      continue;
    }

    break;
  }
  shift_value = dot_ptr ? dot_ptr - str_ptr : 0;
  addon_value = 0;

exp:

  if (!digits_value) {
    str_ptr = beg_ptr;
    goto ret_edom;
  }

  if (negative_sign && unsigned_flag) {
    goto ret_sign;
  }
  if (str_ptr < end && (*str_ptr == 'e' || *str_ptr == 'E')) {
    str_ptr++;
    if (str_ptr < end) {
      int negative_sign_exp, exponent;
      if ((negative_sign_exp = (*str_ptr == '-')) || *str_ptr == '+') {
        if (++str_ptr == end)
          goto ret_sign;
      }
      for (exponent = 0; str_ptr < end && (ch = (unsigned char)(*str_ptr - '0')) < 10; str_ptr++) {
        exponent = exponent * 10 + ch;
      }
      shift_value += negative_sign_exp ? -exponent : exponent;
    }
  }

  if (shift_value == 0) {
    if (addon_value) {
      if (value_ull == UINT64_MAX) {
        goto ret_too_big;
      }
      value_ull++;
    }
    goto ret_sign;
  }

  if (shift_value < 0) {
    uint64_t d, r, d_half;

    if (-shift_value >= DIGITS_IN_ULONGLONG) {
      goto ret_zero;
    }

    d = d10[-shift_value];
    r = value_ull % d;
    value_ull /= d;
    d_half = d / 2;
    if (r >= d_half) {
      value_ull++;
    }
    goto ret_sign;
  }

  if (shift_value > DIGITS_IN_ULONGLONG) {
    if (!value_ull) {
      goto ret_sign;
    }
    goto ret_too_big;
  }

  for (; shift_value > 0; shift_value--, value_ull *= 10) {
    if (value_ull > CUTOFF) {
      goto ret_too_big;
    }
  }

ret_sign:
  *end_ptr = (char*)str_ptr;

  if (!unsigned_flag) {
    if (negative_sign) {
      if (value_ull > (uint64_t)INT64_MIN) {
        *error = MY_ERRNO_ERANGE;
        return (uint64_t)INT64_MIN;
      }
      *error = 0;
      return (uint64_t) - (int64_t)value_ull;
    } else {
      if (value_ull > (uint64_t)INT64_MAX) {
        *error = MY_ERRNO_ERANGE;
        return (uint64_t)INT64_MAX;
      }
      *error = 0;
      return value_ull;
    }
  }

  if (negative_sign && value_ull) {
    *error = MY_ERRNO_ERANGE;
    return 0;
  }
  *error = 0;
  return value_ull;

ret_zero:
  *end_ptr = (char*)str_ptr;
  *error = 0;
  return 0;

ret_edom:
  *end_ptr = (char*)str_ptr;
  *error = MY_ERRNO_EDOM;
  return 0;

ret_too_big:
  *end_ptr = (char*)str_ptr;
  *error = MY_ERRNO_ERANGE;
  return unsigned_flag ? UINT64_MAX : negative_sign ? (uint64_t)INT64_MIN : (uint64_t)INT64_MAX;
}

size_t ob_scan_8bit(const char* str, const char* end, int sq)
{
  const char* str_begin = str;
  switch (sq) {
    case OB_SEQ_INTTAIL:
      if (str < end && *str == '.') {
        for (str++; str != end && *str == '0'; str++)
          ;
        return (size_t)(str - str_begin);
      }
      return 0;

    case OB_SEQ_SPACES:
      for (; str < end; str++) {
        if (' ' != *str) {
          break;
        }
      }
      return (size_t)(str - str_begin);
    default:
      return 0;
  }
}

//========================================================================

int ob_like_range_simple(const ObCharsetInfo* cs, const char* str, size_t str_len, int escape, int w_one, int w_many,
    size_t res_length, char* min_str, char* max_str, size_t* min_length, size_t* max_length)
{
  const char* end = str + str_len;
  char* min_org = min_str;
  char* min_end = min_str + res_length;
  size_t charlen = res_length / cs->mbmaxlen;

  for (; str != end && min_str != min_end && charlen > 0; str++, charlen--) {
    if (*str == escape && str + 1 != end) {
      str++; /* Skip escape */
      *min_str++ = *max_str++ = *str;
      continue;
    }
    if (*str == w_one) { /* '_' in SQL */
      *min_str++ = '\0'; /* This should be min char */
      *max_str++ = (char)cs->max_sort_char;
      continue;
    }
    if (*str == w_many) { /* '%' in SQL */
      /* Calculate length of keys */
      *min_length = ((cs->state & OB_CS_BINSORT) ? (size_t)(min_str - min_org) : res_length);
      *max_length = res_length;
      do {
        *min_str++ = 0;
        *max_str++ = (char)cs->max_sort_char;
      } while (min_str != min_end);
      return 0;
    }
    *min_str++ = *max_str++ = *str;
  }

  *min_length = *max_length = (size_t)(min_str - min_org);
  while (min_str != min_end) {
    *min_str++ = *max_str++ = ' '; /* Because if key compression */
  }
  return 0;
}

//=================================================================

void ob_fill_8bit(const ObCharsetInfo* cs __attribute__((unused)), char* str, size_t len, int fill)
{
  memset(str, fill, len);
}

int64_t ob_strntoll(const char* str, size_t str_len, int base, char** end, int* err)
{
  return ob_strntoll_8bit(str, str_len, base, end, err);
}

int64_t ob_strntoull(const char* str, size_t str_len, int base, char** end, int* err)
{
  return ob_strntoull_8bit(str, str_len, base, end, err);
}

const unsigned char* skip_trailing_space(const unsigned char* str, size_t len)
{
  const unsigned char* end = str + len;
  const int64_t SIZEOF_INT = 4;
  const int64_t SPACE_INT = 0x20202020;
  if (len > 20) {
    const unsigned char* end_words =
        (const unsigned char*)(int64_t)(((uint64_t)(int64_t)end) / SIZEOF_INT * SIZEOF_INT);
    const unsigned char* start_words =
        (const unsigned char*)(int64_t)((((uint64_t)(int64_t)str) + SIZEOF_INT - 1) / SIZEOF_INT * SIZEOF_INT);
    ob_charset_assert(((uint64_t)(int64_t)str) >= SIZEOF_INT);
    if (end_words > str) {
      while (end > end_words && end[-1] == 0x20) {
        end--;
      }
      if (end[-1] == 0x20 && start_words < end_words) {
        while (end > start_words && ((unsigned*)end)[-1] == SPACE_INT) {
          end -= SIZEOF_INT;
        }
      }
    }
  }
  while (end > str && end[-1] == 0x20) {
    end--;
  }
  return (end);
}
