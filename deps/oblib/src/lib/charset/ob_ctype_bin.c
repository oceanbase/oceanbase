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

static unsigned char ctype_bin[] = {0x00,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x28,
    0x28,
    0x28,
    0x28,
    0x28,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x20,
    0x48,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x84,
    0x84,
    0x84,
    0x84,
    0x84,
    0x84,
    0x84,
    0x84,
    0x84,
    0x84,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x81,
    0x81,
    0x81,
    0x81,
    0x81,
    0x81,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x01,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x10,
    0x82,
    0x82,
    0x82,
    0x82,
    0x82,
    0x82,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x02,
    0x10,
    0x10,
    0x10,
    0x10,
    0x20,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00};

/* Dummy array for toupper / tolower / sortorder */

static unsigned char bin_char_array[] = {0x00,
    0x01,
    0x02,
    0x03,
    0x04,
    0x05,
    0x06,
    0x07,
    0x08,
    0x09,
    0x0a,
    0x0b,
    0x0c,
    0x0d,
    0x0e,
    0x0f,
    0x10,
    0x11,
    0x12,
    0x13,
    0x14,
    0x15,
    0x16,
    0x17,
    0x18,
    0x19,
    0x1a,
    0x1b,
    0x1c,
    0x1d,
    0x1e,
    0x1f,
    0x20,
    0x21,
    0x22,
    0x23,
    0x24,
    0x25,
    0x26,
    0x27,
    0x28,
    0x29,
    0x2a,
    0x2b,
    0x2c,
    0x2d,
    0x2e,
    0x2f,
    0x30,
    0x31,
    0x32,
    0x33,
    0x34,
    0x35,
    0x36,
    0x37,
    0x38,
    0x39,
    0x3a,
    0x3b,
    0x3c,
    0x3d,
    0x3e,
    0x3f,
    0x40,
    0x41,
    0x42,
    0x43,
    0x44,
    0x45,
    0x46,
    0x47,
    0x48,
    0x49,
    0x4a,
    0x4b,
    0x4c,
    0x4d,
    0x4e,
    0x4f,
    0x50,
    0x51,
    0x52,
    0x53,
    0x54,
    0x55,
    0x56,
    0x57,
    0x58,
    0x59,
    0x5a,
    0x5b,
    0x5c,
    0x5d,
    0x5e,
    0x5f,
    0x60,
    0x61,
    0x62,
    0x63,
    0x64,
    0x65,
    0x66,
    0x67,
    0x68,
    0x69,
    0x6a,
    0x6b,
    0x6c,
    0x6d,
    0x6e,
    0x6f,
    0x70,
    0x71,
    0x72,
    0x73,
    0x74,
    0x75,
    0x76,
    0x77,
    0x78,
    0x79,
    0x7a,
    0x7b,
    0x7c,
    0x7d,
    0x7e,
    0x7f,
    0x80,
    0x81,
    0x82,
    0x83,
    0x84,
    0x85,
    0x86,
    0x87,
    0x88,
    0x89,
    0x8a,
    0x8b,
    0x8c,
    0x8d,
    0x8e,
    0x8f,
    0x90,
    0x91,
    0x92,
    0x93,
    0x94,
    0x95,
    0x96,
    0x97,
    0x98,
    0x99,
    0x9a,
    0x9b,
    0x9c,
    0x9d,
    0x9e,
    0x9f,
    0xa0,
    0xa1,
    0xa2,
    0xa3,
    0xa4,
    0xa5,
    0xa6,
    0xa7,
    0xa8,
    0xa9,
    0xaa,
    0xab,
    0xac,
    0xad,
    0xae,
    0xaf,
    0xb0,
    0xb1,
    0xb2,
    0xb3,
    0xb4,
    0xb5,
    0xb6,
    0xb7,
    0xb8,
    0xb9,
    0xba,
    0xbb,
    0xbc,
    0xbd,
    0xbe,
    0xbf,
    0xc0,
    0xc1,
    0xc2,
    0xc3,
    0xc4,
    0xc5,
    0xc6,
    0xc7,
    0xc8,
    0xc9,
    0xca,
    0xcb,
    0xcc,
    0xcd,
    0xce,
    0xcf,
    0xd0,
    0xd1,
    0xd2,
    0xd3,
    0xd4,
    0xd5,
    0xd6,
    0xd7,
    0xd8,
    0xd9,
    0xda,
    0xdb,
    0xdc,
    0xdd,
    0xde,
    0xdf,
    0xe0,
    0xe1,
    0xe2,
    0xe3,
    0xe4,
    0xe5,
    0xe6,
    0xe7,
    0xe8,
    0xe9,
    0xea,
    0xeb,
    0xec,
    0xed,
    0xee,
    0xef,
    0xf0,
    0xf1,
    0xf2,
    0xf3,
    0xf4,
    0xf5,
    0xf6,
    0xf7,
    0xf8,
    0xf9,
    0xfa,
    0xfb,
    0xfc,
    0xfd,
    0xfe,
    0xff};

size_t ob_numchars_8bit(
    const ObCharsetInfo* cs __attribute__((unused)), const char* str __attribute__((unused)), size_t len)
{
  return len;
}

size_t ob_charpos_8bit(const ObCharsetInfo* cs __attribute__((unused)), const char* str __attribute__((unused)),
    size_t len __attribute__((unused)), size_t pos)
{
  return pos;
}

size_t ob_max_bytes_charpos_8bit(const ObCharsetInfo* cs __attribute__((unused)),
    const char* str __attribute__((unused)), size_t len __attribute__((unused)), size_t max_bytes, size_t* char_len)
{
  *char_len = max_bytes;
  return max_bytes;
}

size_t ob_well_formed_len_8bit(const char* str __attribute__((unused)), size_t len, size_t nchars, int* error)
{
  size_t nbytes = len;
  *error = 0;
  return nbytes < nchars ? nbytes : nchars;
}

size_t ob_lengthsp_binary(const char* str __attribute__((unused)), size_t len)
{
  return len;
}

static int ob_mb_wc_bin(const unsigned char* str, const unsigned char* end __attribute__((unused)), ob_wc_t* wc)
{
  if (str >= end) {
    return OB_CS_ERR_TOOSMALL;
  }

  *wc = str[0];
  return 1;
}

static int ob_wc_mb_bin(ob_wc_t wc, unsigned char* s, unsigned char* e __attribute__((unused)))
{
  if (s >= e) {
    return OB_CS_ERR_TOOSMALL;
  }

  if (wc < 256) {
    s[0] = (char)wc;
    return 1;
  }
  return OB_CS_ERR_ILLEGAL_UNICODE;
}

static int ob_mb_ctype_8bit(const ObCharsetInfo* cs, int* ctype, const unsigned char* s, const unsigned char* e)
{
  if (s >= e) {
    *ctype = 0;
    return OB_CS_ERR_TOOSMALL;
  }
  *ctype = cs->ctype[*s + 1];
  return 1;
}

static size_t ob_case_bin(const ObCharsetInfo* cs __attribute__((unused)), char* src __attribute__((unused)),
    size_t srclen, char* dst __attribute__((unused)), size_t dstlen __attribute__((unused)))
{
  return srclen;
}

//========================================================================

static int ob_strnncoll_binary(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* s, size_t slen,
    const unsigned char* t, size_t tlen)
{
  size_t len = slen < tlen ? slen : tlen;
  int cmp = memcmp(s, t, len);
  return cmp ? cmp : (int)(slen - tlen);
}

/*
  Compare two strings. Result is sign(first_argument - second_argument)

  SYNOPSIS
    ob_strnncollsp_binary()
    cs			Chararacter set
    s			String to compare
    slen		Length of 's'
    t			String to compare
    tlen		Length of 't'

  NOTE
   This function is used for real binary strings, i.e. for
   BLOB, BINARY(N) and VARBINARY(N).
   It compares trailing spaces as spaces.

  RETURN
  < 0	s < t
  0	s == t
  > 0	s > t
*/

static int ob_strnncollsp_binary(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* s, size_t slen,
    const unsigned char* t, size_t tlen)
{
  return ob_strnncoll_binary(cs, s, slen, t, tlen);
}

static size_t ob_strnxfrm_8bit_bin(const ObCharsetInfo* cs __attribute__((unused)), unsigned char* dst, size_t dstlen,
    uint32_t nweights, const unsigned char* src, size_t srclen, int* is_valid_unicode)
{
  *is_valid_unicode = 1;
  srclen = (srclen < dstlen ? srclen : dstlen);
  srclen = (srclen < nweights ? srclen : nweights);
  if (dst != src && srclen > 0) {
    memcpy(dst, src, srclen);
  }
  return srclen;
}

#define likeconv(s, A) (A)
#define INC_PTR(cs, A, B) (A)++

static int ob_wildcmp_bin_impl(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr,
    const char* wild_str, const char* wild_end, int escape_char, int w_one_char, int w_many_char, int recurse_level)
{
  int cmp_result = -1;

  while (wild_str != wild_end) {
    while (*wild_str != w_many_char && *wild_str != w_one_char) {
      if (*wild_str == escape_char && wild_str + 1 != wild_end) {
        wild_str++;
      }
      if (str_ptr == str_end_ptr || likeconv(cs, *wild_str++) != likeconv(cs, *str_ptr++)) {
        return 1;
      }
      if (wild_str == wild_end) {
        return str_ptr != str_end_ptr;
      }
      cmp_result = 1;
    }
    if (*wild_str == w_one_char) {
      do {
        if (str_ptr == str_end_ptr) {
          return (cmp_result);
        }
        INC_PTR(cs, str_ptr, str_end_ptr);
      } while (++wild_str < wild_end && *wild_str == w_one_char);
      if (wild_str == wild_end) {
        break;
      }
    }
    if (*wild_str == w_many_char) {
      unsigned char cmp = 0;
      wild_str++;
      for (; wild_str != wild_end; wild_str++) {
        if (*wild_str == w_many_char) {
          continue;
        }
        if (*wild_str == w_one_char) {
          if (str_ptr == str_end_ptr) {
            return (-1);
          }
          INC_PTR(cs, str_ptr, str_end_ptr);
          continue;
        }
        break;
      }
      if (wild_str == wild_end) {
        return (0);
      }
      if (str_ptr == str_end_ptr) {
        return (-1);
      }

      if ((cmp = *wild_str) == escape_char && wild_str + 1 != wild_end) {
        cmp = *++wild_str;
      }

      INC_PTR(cs, wild_str, wild_end);
      cmp = likeconv(cs, cmp);
      do {
        while (str_ptr != str_end_ptr && (unsigned char)likeconv(cs, *str_ptr) != cmp) {
          str_ptr++;
        }
        if (str_ptr++ == str_end_ptr) {
          return -1;
        }
        do {
          int tmp = ob_wildcmp_bin_impl(
              cs, str_ptr, str_end_ptr, wild_str, wild_end, escape_char, w_one_char, w_many_char, recurse_level + 1);
          if (tmp <= 0) {
            return tmp;
          }
        } while (0);
      } while (str_ptr != str_end_ptr && wild_str[0] != w_many_char);
      return -1;
    }
  }
  return str_ptr != str_end_ptr ? 1 : 0;
}

int ob_wildcmp_bin(const ObCharsetInfo* cs, const char* str, const char* str_end, const char* wildstr,
    const char* wildend, int escape, int w_one, int w_many)
{
  return ob_wildcmp_bin_impl(cs, str, str_end, wildstr, wildend, escape, w_one, w_many, 1);
}

static uint32_t ob_instr_bin(const ObCharsetInfo* cs __attribute__((unused)), const char* b, size_t b_length,
    const char* s, size_t s_length, ob_match_info* match, uint32_t nmatch)
{
  register const unsigned char *str, *search, *end, *search_end;

  if (s_length <= b_length) {
    if (!s_length) {
      if (nmatch) {
        match->beg = 0;
        match->end = 0;
        match->mb_len = 0;
      }
      return 1; /* Empty string is always found */
    }

    str = (const unsigned char*)b;
    search = (const unsigned char*)s;
    end = (const unsigned char*)b + b_length - s_length + 1;
    search_end = (const unsigned char*)s + s_length;

  skip:
    while (str != end) {
      if ((*str++) == (*search)) {
        register const unsigned char *i, *j;

        i = str;
        j = search + 1;

        while (j != search_end)
          if ((*i++) != (*j++))
            goto skip;

        if (nmatch > 0) {
          match[0].beg = 0;
          match[0].end = (size_t)(str - (const unsigned char*)b - 1);
          match[0].mb_len = match[0].end;

          if (nmatch > 1) {
            match[1].beg = match[0].end;
            match[1].end = match[0].end + s_length;
            match[1].mb_len = match[1].end - match[1].beg;
          }
        }
        return 2;
      }
    }
  }
  return 0;
}

void ob_hash_sort_bin(const ObCharsetInfo* cs __attribute__((unused)), const unsigned char* key, size_t len,
    uint64_t* nr1, uint64_t* nr2, const int calc_end_space __attribute__((unused)), hash_algo hash_algo)
{
  const unsigned char* pos = key;

  key += len;
  if (NULL == hash_algo) {
    for (; pos < (unsigned char*)key; pos++) {
      nr1[0] ^= (uint64_t)((((uint64_t)nr1[0] & 63) + nr2[0]) * ((uint32_t)*pos)) + (nr1[0] << 8);
      nr2[0] += 3;
    }
  } else {
    nr1[0] = hash_algo((void*)pos, (int)(key - pos), nr1[0]);
  }
}

//========================================================================

static ObCharsetHandler ob_charset_handler = {NULL,
    ob_numchars_8bit,
    ob_charpos_8bit,
    ob_max_bytes_charpos_8bit,
    ob_well_formed_len_8bit,
    ob_lengthsp_binary,
    ob_mb_wc_bin,
    ob_wc_mb_bin,
    ob_mb_ctype_8bit,
    ob_case_bin,
    ob_case_bin,
    ob_fill_8bit,
    ob_strntoll_8bit,
    ob_strntoull_8bit,
    ob_strntod_8bit,
    ob_strntoull10rnd_8bit,
    ob_scan_8bit};

static ObCollationHandler ob_collation_binary_handler = {
    ob_strnncoll_binary,
    ob_strnncollsp_binary,
    ob_strnxfrm_8bit_bin,
    ob_like_range_simple,
    ob_wildcmp_bin,
    ob_instr_bin,
    ob_hash_sort_bin,
};

ObCharsetInfo ob_charset_bin = {63,
    0,
    0,                                              /* number        */
    OB_CS_COMPILED | OB_CS_BINSORT | OB_CS_PRIMARY, /* state */
    "binary",                                       /* cs name    */
    "binary",                                       /* name          */
    "",                                             /* comment       */
    NULL,                                           /* tailoring     */
    ctype_bin,                                      /* ctype         */
    bin_char_array,                                 /* to_lower      */
    bin_char_array,                                 /* to_upper      */
    NULL,                                           /* sort_order    */
    NULL,                                           /* caseinfo     */
    NULL,                                           /* state_map    */
    NULL,                                           /* ident_map    */
    1,                                              /* strxfrm_multiply */
    1,                                              /* caseup_multiply  */
    1,                                              /* casedn_multiply  */
    1,                                              /* mbminlen      */
    1,                                              /* mbmaxlen      */
    0,                                              /* min_sort_char */
    255,                                            /* max_sort_char */
    0,                                              /* pad char      */
    0,                                              /* escape_with_backslash_is_dangerous */
    1,                                              /* levels_for_compare */
    1,                                              /* levels_for_order   */
    &ob_charset_handler,
    &ob_collation_binary_handler};
