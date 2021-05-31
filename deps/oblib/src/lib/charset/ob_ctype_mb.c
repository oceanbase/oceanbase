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

#ifdef __cplusplus
extern "C" int check_stack_overflow_in_c(int* check_overflow);
#endif

static void __attribute__((noinline)) pad_max_char_help(char* str, char* end, char* buf, char buf_len)
{
  do {
    if ((str + buf_len) <= end) {
      /* Enough space for the characer */
      memcpy(str, buf, buf_len);
      str += buf_len;
    } else {
      /*
        There is no space for whole multibyte
        character, then add trailing spaces.
      */
      *str++ = ' ';
    }
  } while (str < end);
}
static void pad_max_char(const ObCharsetInfo* cs, char* str, char* end)
{
  char buf[10];
  char buf_len;
  if (!(cs->state & OB_CS_UNICODE)) {
    if (cs->max_sort_char <= 255) {
      memset(str, cs->max_sort_char, end - str);
      return;
    }
    buf[0] = cs->max_sort_char >> 8;
    buf[1] = cs->max_sort_char & 0xFF;
    buf_len = 2;
  } else {
    buf_len = cs->cset->wc_mb(cs->max_sort_char, (unsigned char*)buf, (unsigned char*)buf + sizeof(buf));
  }
  ob_charset_assert(buf_len > 0);
  pad_max_char_help(str, end, buf, buf_len);
}

int ob_like_range_mb_help(const ObCharsetInfo* cs, size_t res_length, char** min_str_, char** max_str_, char** min_org_,
    char** min_end_, size_t* min_length, size_t* max_length, char** max_end_)
{
  char* min_str = *min_str_;
  char* max_str = *max_str_;
  char* max_end = *max_end_;
  char* min_end = *min_end_;
  char* min_org = *min_org_;
  *min_length = ((cs->state & OB_CS_BINSORT) ? (size_t)(min_str - min_org) : res_length);
  *max_length = res_length;
  do {
    *min_str++ = (char)cs->min_sort_char;
  } while (min_str != min_end);
  *max_length = res_length;
  pad_max_char(cs, max_str, max_end);
  *min_str_ = min_str;
  *max_str_ = max_str;
  *max_end_ = max_end;
  *min_end_ = min_end;
  *min_org_ = min_org;
  return 0;
}

int ob_like_range_mb(const ObCharsetInfo* cs, const char* str, size_t str_len, int escape, int w_one, int w_many,
    size_t res_length, char* min_str, char* max_str, size_t* min_length, size_t* max_length)
{
  unsigned int mb_len;
  const char* end = str + str_len;
  char* min_org = min_str;
  char* min_end = min_str + res_length;
  char* max_end = max_str + res_length;
  size_t maxcharlen = res_length / cs->mbmaxlen;

  for (; str != end && min_str != min_end && maxcharlen; maxcharlen--) {
    /* We assume here that escape, w_any, w_namy are one-byte characters */
    if (*str == escape && str + 1 != end) {
      str++;                                      /* Skip escape */
    } else if (*str == w_one || *str == w_many) { /* '_' and '%' in SQL */
      return ob_like_range_mb_help(
          cs, res_length, &min_str, &max_str, &min_org, &min_end, min_length, max_length, &max_end);
    }
    if ((mb_len = cs->cset->ismbchar(str, end - str)) > 1) {
      if (str + mb_len > end || min_str + mb_len > min_end) {
        break;
      }
      while (mb_len--) {
        *min_str++ = *max_str++ = *str++;
      }
    } else {
      // TODO support contractions
      *min_str++ = *max_str++ = *str++;
    }
  }

  *min_length = *max_length = (size_t)(min_str - min_org);
  while (min_str != min_end)
    *min_str++ = *max_str++ = ' '; /* Because if key compression */
  return 0;
}

int ob_wildcmp_mb(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr, const char* wild_str_ptr,
    const char* wild_end_ptr, int escape_char, int w_one_char, int w_many_char)
{
  return ob_wildcmp_mb_impl(cs, str_ptr, str_end_ptr, wild_str_ptr, wild_end_ptr, escape_char, w_one_char, w_many_char);
}

/*
** Compare string against string with wildcard
**	0 if matched
**	-1 if not matched with wildcard
**	 1 if matched with wildcard
*/

#define INC_PTR(cs, A, B)                                 \
  uint32_t temp_len = cs->cset->ismbchar(A, ((B) - (A))); \
  A += (temp_len) ? temp_len : 1;

#define likeconv(s, A) (unsigned char)(s)->sort_order[(unsigned char)(A)]

int ob_wildcmp_mb_impl(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr, const char* wild_str_ptr,
    const char* wild_end_ptr, int escape_char, int w_one_char, int w_many_char)
{
  int cmp_result = -1; /* Not found, using wildcards */

  /* if (my_string_stack_guard && my_string_stack_guard(recurse_level)) */
  /*    return 1; */
  // if (0 != check_stack_overflow_in_c(is_stack_overlow) || *is_stack_overlow) {
  //  return 1;
  //}
  while (wild_str_ptr != wild_end_ptr) {
    while (*wild_str_ptr != w_many_char && *wild_str_ptr != w_one_char) {
      int l;
      if (*wild_str_ptr == escape_char && wild_str_ptr + 1 != wild_end_ptr)
        wild_str_ptr++;
      if ((l = cs->cset->ismbchar(wild_str_ptr, wild_end_ptr - wild_str_ptr))) {
        if (str_ptr + l > str_end_ptr || memcmp(str_ptr, wild_str_ptr, l) != 0)
          return 1;
        str_ptr += l;
        wild_str_ptr += l;
      } else if (str_ptr == str_end_ptr || likeconv(cs, *wild_str_ptr++) != likeconv(cs, *str_ptr++))
        return (1); /* No match */
      if (wild_str_ptr == wild_end_ptr)
        return (str_ptr != str_end_ptr); /* Match if both are at end */
      cmp_result = 1;                    /* Found an anchor char */
    }
    if (*wild_str_ptr == w_one_char) {
      do {
        if (str_ptr == str_end_ptr) /* Skip one char if possible */
          return (cmp_result);
        INC_PTR(cs, str_ptr, str_end_ptr);
      } while (++wild_str_ptr < wild_end_ptr && *wild_str_ptr == w_one_char);
      if (wild_str_ptr == wild_end_ptr)
        break;
    }
    if (*wild_str_ptr == w_many_char) { /* Found w_many_char */
      unsigned char cmp;
      const char* mb = wild_str_ptr;
      int mb_len = 0;

      wild_str_ptr++;
      /* Remove any '%' and '_' from the wild search string */
      for (; wild_str_ptr != wild_end_ptr; wild_str_ptr++) {
        if (*wild_str_ptr == w_many_char)
          continue;
        if (*wild_str_ptr == w_one_char) {
          if (str_ptr == str_end_ptr)
            return (-1);
          INC_PTR(cs, str_ptr, str_end_ptr);
          continue;
        }
        break; /* Not a wild character */
      }
      if (wild_str_ptr == wild_end_ptr)
        return (0); /* Ok if w_many_char is last */
      if (str_ptr == str_end_ptr)
        return -1;

      if ((cmp = *wild_str_ptr) == escape_char && wild_str_ptr + 1 != wild_end_ptr)
        cmp = *++wild_str_ptr;

      mb = wild_str_ptr;
      mb_len = cs->cset->ismbchar(wild_str_ptr, wild_end_ptr - wild_str_ptr);
      INC_PTR(cs, wild_str_ptr, wild_end_ptr); /* This is compared trough cmp */
      cmp = likeconv(cs, cmp);
      do {
        for (;;) {
          if (str_ptr >= str_end_ptr)
            return -1;
          if (mb_len) {
            if (str_ptr + mb_len <= str_end_ptr && memcmp(str_ptr, mb, mb_len) == 0) {
              str_ptr += mb_len;
              break;
            }
          } else if (!cs->cset->ismbchar(str_ptr, str_end_ptr - str_ptr) && likeconv(cs, *str_ptr) == cmp) {
            str_ptr++;
            break;
          }
          INC_PTR(cs, str_ptr, str_end_ptr);
        }
        {
          int tmp = ob_wildcmp_mb_impl(
              cs, str_ptr, str_end_ptr, wild_str_ptr, wild_end_ptr, escape_char, w_one_char, w_many_char);
          if (tmp <= 0)
            return (tmp);
        }
      } while (str_ptr != str_end_ptr && wild_str_ptr[0] != w_many_char);
      return (-1);
    }
  }
  return (str_ptr != str_end_ptr ? 1 : 0);
}

unsigned int ob_instr_mb(const ObCharsetInfo* cs, const char* base, size_t base_len, const char* str, size_t str_len,
    ob_match_info* match, uint32_t nmatch)
{
  const char *end, *base_begin;
  int res = 0;
  if (str_len <= base_len) {
    if (!str_len) {
      if (nmatch) {
        match->beg = 0;
        match->end = 0;
        match->mb_len = 0;
      }
      return 1; /* Empty string is always found */
    }
    base_begin = base;
    end = base + base_len - str_len + 1;
    while (base < end) {
      int mb_len;
      if (!cs->coll->strnncoll(cs, (unsigned char*)base, str_len, (unsigned char*)str, str_len)) {
        if (nmatch) {
          match[0].beg = 0;
          match[0].end = (size_t)(base - base_begin);
          match[0].mb_len = res;
          if (nmatch > 1) {
            match[1].beg = match[0].end;
            match[1].end = match[0].end + str_len;
            match[1].mb_len = 0; /* Not computed */
          }
        }
        return 2;
      }
      mb_len = (mb_len = cs->cset->ismbchar(base, end - base)) ? mb_len : 1;
      base += mb_len;
      base_len -= mb_len;
      res++;
    }
  }
  return 0;
}

size_t ob_numchars_mb(const ObCharsetInfo* cs, const char* str, size_t len)
{
  size_t count = 0;
  while (len > 0) {
    uint32_t mb_len = cs->cset->ismbchar(str, len);
    mb_len = (mb_len > 0) ? mb_len : 1;
    str += mb_len;
    len -= mb_len;
    count++;
  }
  return count;
}

size_t ob_charpos_mb(const ObCharsetInfo* cs, const char* str, size_t len, size_t pos)
{
  const char* str_begin = str;
  size_t err_result = len + 2;
  while (pos > 0 && len > 0) {
    uint32_t mb_len = cs->cset->ismbchar(str, len);
    mb_len = (mb_len > 0) ? mb_len : 1;
    str += mb_len;
    len -= mb_len;
    pos--;
  }
  return (size_t)(pos > 0 ? err_result : str - str_begin);
}

size_t ob_max_bytes_charpos_mb(
    const ObCharsetInfo* cs, const char* str, size_t str_len, size_t max_bytes, size_t* char_len)
{
  const char* str_begin = str;
  while (max_bytes > 0 && str_len > 0) {
    uint32_t mb_len = cs->cset->ismbchar(str, str_len);
    uint32_t bytes = mb_len ? mb_len : 1;
    if (max_bytes < bytes) {
      break;
    } else {
      str += bytes;
      str_len -= bytes;
      max_bytes -= bytes;
      ++*char_len;
    }
  }
  return (size_t)(str - str_begin);
}

int ob_mb_ctype_mb(
    const ObCharsetInfo* cs __attribute__((unused)), int* ctype, const unsigned char* s, const unsigned char* e)
{
  ob_wc_t wc;
  int res = cs->cset->mb_wc(s, e, &wc);
  if (res <= 0 || wc > 0xFFFF) {
    *ctype = 0;
  } else {
    *ctype = ob_uni_ctype[wc >> 8].ctype ? ob_uni_ctype[wc >> 8].ctype[wc & 0xFF] : ob_uni_ctype[wc >> 8].pctype;
  }
  return res;
}

//==========================================================================

size_t ob_lengthsp_8bit(const char* str, size_t str_len)
{
  const char* end = (const char*)skip_trailing_space((const unsigned char*)str, str_len);
  return (size_t)(end - str);
}

/* BINARY collations handlers for MB charsets */

int ob_strnncoll_mb_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* str1, size_t str1_len,
    const unsigned char* str2, size_t str2_len)
{
  size_t len = str1_len < str2_len ? str1_len : str2_len;
  int cmp = memcmp(str1, str2, len);
  return cmp ? cmp : (int)(str1_len - str2_len);
}

int ob_strnncollsp_mb_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* str1, size_t str1_len,
    const unsigned char* str2, size_t str2_len)
{
  const unsigned char* end;
  size_t length;
  int res;

  end = str1 + (length = (str1_len < str2_len ? str1_len : str2_len));
  while (str1 < end) {
    if (*str1++ != *str2++)
      return ((int)str1[-1] - (int)str2[-1]);
  }
  res = 0;
  if (str1_len != str2_len) {
    int swap = 1;
    if (str1_len < str2_len) {
      str1_len = str2_len;
      str1 = str2;
      swap = -1;
      res = -res;
    }
    for (end = str1 + str1_len - length; str1 < end; str1++) {
      if (*str1 != ' ')
        return (*str1 < ' ') ? -swap : swap;
    }
  }
  return res;
}

//=====================================================================

static int ob_wildcmp_mb_bin_impl_help(const ObCharsetInfo* cs, const char** str_ptr_, const char** str_end_ptr_,
    const char** wild_str_ptr_, const char** wild_end_ptr_, int escape_char, int w_one_char, int w_many_char,
    int* cmp_result, int* has_returned)
{
  const char* wild_str_ptr = *wild_str_ptr_;
  const char* str_end_ptr = *str_end_ptr_;
  const char* str_ptr = *str_ptr_;
  const char* wild_end_ptr = *wild_end_ptr_;
  *has_returned = 0;
  int ret = 0;
  while (*wild_str_ptr == escape_char || (*wild_str_ptr != w_many_char && *wild_str_ptr != w_one_char)) {
    int l;
    if (*wild_str_ptr == escape_char && wild_str_ptr + 1 != wild_end_ptr)
      wild_str_ptr++;
    if ((l = cs->cset->ismbchar(wild_str_ptr, wild_end_ptr - wild_str_ptr))) {
      if (str_ptr + l > str_end_ptr || memcmp(str_ptr, wild_str_ptr, l) != 0) {
        *has_returned = 1;
        ret = 1;
        break;
      }
      str_ptr += l;
      wild_str_ptr += l;
    } else if (str_ptr == str_end_ptr || *wild_str_ptr++ != *str_ptr++) {
      *has_returned = 1;
      ret = 1;
      break;
    }
    if (wild_str_ptr == wild_end_ptr) {
      *has_returned = 1;
      ret = (str_ptr != str_end_ptr);
      break;
    }
    *cmp_result = 1;
  }
  *wild_str_ptr_ = wild_str_ptr;
  *str_end_ptr_ = str_end_ptr;
  *str_ptr_ = str_ptr;
  *wild_end_ptr_ = wild_end_ptr;
  return ret;
}

static int ob_wildcmp_mb_bin_impl(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr,
    const char* wild_str_ptr, const char* wild_end_ptr, int escape_char, int w_one_char, int w_many_char,
    int recurse_level)
{
  int cmp_result = -1;

  while (wild_str_ptr != wild_end_ptr) {
    int has_returned = 0;
    int tmp = ob_wildcmp_mb_bin_impl_help(cs,
        &str_ptr,
        &str_end_ptr,
        &wild_str_ptr,
        &wild_end_ptr,
        escape_char,
        w_one_char,
        w_many_char,
        &cmp_result,
        &has_returned);
    if (has_returned) {
      return tmp;
    }
    if (*wild_str_ptr == w_one_char) {
      do {
        if (str_ptr == str_end_ptr)
          return (cmp_result);
        INC_PTR(cs, str_ptr, str_end_ptr);
      } while (++wild_str_ptr < wild_end_ptr && *wild_str_ptr == w_one_char);
      if (wild_str_ptr == wild_end_ptr)
        break;
    }
    if (*wild_str_ptr == w_many_char) {
      unsigned char cmp;
      const char* mb = wild_str_ptr;
      int mb_len = 0;

      wild_str_ptr++;
      for (; wild_str_ptr != wild_end_ptr; wild_str_ptr++) {
        if (*wild_str_ptr == w_many_char)
          continue;
        if (escape_char != w_one_char && *wild_str_ptr == w_one_char) {
          if (str_ptr == str_end_ptr)
            return (-1);
          INC_PTR(cs, str_ptr, str_end_ptr);
          continue;
        }
        break;
      }
      if (wild_str_ptr == wild_end_ptr)
        return (0);
      if (str_ptr == str_end_ptr)
        return -1;

      if ((cmp = *wild_str_ptr) == escape_char && wild_str_ptr + 1 != wild_end_ptr)
        cmp = *++wild_str_ptr;

      mb = wild_str_ptr;
      mb_len = cs->cset->ismbchar(wild_str_ptr, wild_end_ptr - wild_str_ptr);
      INC_PTR(cs, wild_str_ptr, wild_end_ptr);
      do {
        for (;;) {
          if (str_ptr >= str_end_ptr)
            return -1;
          if (mb_len) {
            if (str_ptr + mb_len <= str_end_ptr && memcmp(str_ptr, mb, mb_len) == 0) {
              str_ptr += mb_len;
              break;
            }
          } else if (!cs->cset->ismbchar(str_ptr, str_end_ptr - str_ptr) && *str_ptr == cmp) {
            str_ptr++;
            break;
          }
          INC_PTR(cs, str_ptr, str_end_ptr);
        }
        {
          int tmp = ob_wildcmp_mb_bin_impl(cs,
              str_ptr,
              str_end_ptr,
              wild_str_ptr,
              wild_end_ptr,
              escape_char,
              w_one_char,
              w_many_char,
              recurse_level + 1);
          if (tmp <= 0)
            return (tmp);
        }
      } while (str_ptr != str_end_ptr && (wild_str_ptr >= wild_end_ptr || wild_str_ptr[0] != w_many_char));
      return (-1);
    }
  }
  return (str_ptr != str_end_ptr ? 1 : 0);
}

int ob_wildcmp_mb_bin(const ObCharsetInfo* cs, const char* str, const char* str_end, const char* wildstr,
    const char* wildend, int escape, int w_one, int w_many)
{
  return ob_wildcmp_mb_bin_impl(cs, str, str_end, wildstr, wildend, escape, w_one, w_many, 1);
}

// ================================================================

void ob_hash_sort_mb_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* key, size_t len,
    uint64_t* nr1, uint64_t* nr2, const int calc_end_space, hash_algo hash_algo)
{
  const unsigned char* pos = key;

  /*
     Remove trailing spaces. We have to do this to be able to compare
    'A ' and 'A' as identical
  */
  if (!calc_end_space) {
    key = skip_trailing_space(key, len);
  } else {
    key += len;
  }
  int length = (int)(key - pos);
  if (NULL == hash_algo) {
    for (; pos < (unsigned char*)key; pos++) {
      nr1[0] ^= (uint64_t)((((uint64_t)nr1[0] & 63) + nr2[0]) * ((uint64_t)*pos)) + (nr1[0] << 8);
      nr2[0] += 3;
    }
  } else {
    nr1[0] = hash_algo((void*)pos, length, nr1[0]);
  }
}
