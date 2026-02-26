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
#include "lib/charset/ob_mysql_global.h"
#include "lib/charset/ob_ctype.h"
#include "lib/charset/ob_ctype_cp932_tab.h"



extern "C" {
static unsigned ismbchar_cp932(const ObCharsetInfo *cs __attribute__((unused)),
                               const char *p, const char *e) {
  return (iscp932head((uint8_t)*p) && (e - p) > 1 && iscp932tail((uint8_t)p[1])
              ? 2
              : 0);
}

static unsigned mbcharlen_cp932(const ObCharsetInfo *cs __attribute__((unused)),
                                unsigned c) {
  return (iscp932head((uint8_t)c) ? 2 : 1);
}
}  // extern "C"

#define cp932code(c, d) \
  ((((unsigned)(uint8_t)(c)) << 8) | (unsigned)(uint8_t)(d))

static int ob_strnncoll_cp932_internal(const ObCharsetInfo *cs __attribute__((unused)),
                                       const uint8_t **a_res, size_t a_length,
                                       const uint8_t **b_res, size_t b_length) {
  const uint8_t *a = *a_res;
  const uint8_t *b = *b_res;
  const uint8_t *a_end = a + a_length;
  const uint8_t *b_end = b + b_length;
  while (a < a_end && b < b_end) {
    if (ismbchar_cp932(cs, static_cast<const char *>(static_cast<const void*>(a)),
                       static_cast<const char *>(static_cast<const void*>(a_end))) &&
        ismbchar_cp932(cs, static_cast<const char *>(static_cast<const void*>(b)),
                       static_cast<const char *>(static_cast<const void*>(b_end)))) {
      unsigned a_char = cp932code(*a, *(a + 1));
      unsigned b_char = cp932code(*b, *(b + 1));
      if (a_char != b_char) return a_char - b_char;
      a += 2;
      b += 2;
    } else {
      if (sort_order_cp932[*a] != sort_order_cp932[*b])
        return sort_order_cp932[*a] - sort_order_cp932[*b];
      a++;
      b++;
    }
  }
  *a_res = a;
  *b_res = b;
  return 0;
}

extern "C" {
static int ob_strnncoll_cp932(const ObCharsetInfo *cs __attribute__((unused)), const uint8_t *a,
                              size_t a_length, const uint8_t *b,
                              size_t b_length, bool b_is_prefix) {
  int res = ob_strnncoll_cp932_internal(cs, &a, a_length, &b, b_length);
  if (b_is_prefix && a_length > b_length) a_length = b_length;
  return res ? res : (int)(a_length - b_length);
}

static int ob_strnncollsp_cp932(const ObCharsetInfo *cs __attribute__((unused)), const uint8_t *a,
                                size_t a_length, const uint8_t *b,
                                size_t b_length, bool diff_if_only_endspace_difference) {
  const uint8_t *a_end = a + a_length;
  const uint8_t *b_end = b + b_length;
  int res = ob_strnncoll_cp932_internal(cs, &a, a_length, &b, b_length);

  if (!res && (a != a_end || b != b_end)) {
    int swap = 1;
    /*
      Check the next not space character of the longer key. If it's < ' ',
      then it's smaller than the other key.
    */
    if (a == a_end) {
      /* put shorter key in a */
      a_end = b_end;
      a = b;
      swap = -1; /* swap sign of result */
      res = -res;
    }
    for (; a < a_end; a++) {
      if (*a != u' ') return (*a < u' ') ? -swap : swap;
    }
  }
  return res;
}
}  // extern "C"

extern "C" {
static int ob_mb_wc_cp932(const ObCharsetInfo *cs __attribute__((unused)), ob_wc_t *pwc,
                          const uint8_t *s, const uint8_t *e) {
  if (s >= e) return OB_CS_TOOSMALL;

  int hi = s[0];
  if (hi < 0x80) /* ASCII: [00-7F] -> [U+0000..U+007F] */
  {
    *pwc = hi;
    return 1;
  }

  /* JIS-X-0201 Half width Katakana: [A1..DF] -> [U+FF61..U+FF9F] */
  if (hi >= 0xA1 && hi <= 0xDF) {
    *pwc = cp932_to_unicode[hi];
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  /* JIS-X-0208-MS [81..9F,E0..FC][40..7E,80..FC] */
  if (!(pwc[0] = cp932_to_unicode[(hi << 8) + s[1]]))
    return (iscp932head(hi) && iscp932tail(s[1])) ? -2 : OB_CS_ILSEQ;

  return 2;
}


static int ob_wc_mb_cp932(const ObCharsetInfo *cs  __attribute__((unused)), ob_wc_t wc,
                          uint8_t *s, uint8_t *e) {
  int code;

  if ((int)wc < 0x80) /* ASCII: [U+0000..U+007F] -> [00-7F] */
  {
    /*
      This branch is for performance purposes on ASCII range,
      to avoid using unicode_to_cp932[]: about 10% improvement.
    */
    if (s >= e) return OB_CS_TOOSMALL;
    s[0] = (uint8_t)wc;
    return 1;
  }

  if (wc > 0xFFFF ||
      !(code = unicode_to_cp932[wc])) /* Bad Unicode code point */
    return OB_CS_ILUNI;

  if (code <= 0xFF) {
    /* JIS-X-0201 HALF WIDTH KATAKANA [U+FF61..U+FF9F] -> [A1..DF] */
    if (s >= e) return OB_CS_TOOSMALL;
    s[0] = code;
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;
  s[0] = code >> 8;
  s[1] = code & 0xFF;/* JIS-X-0208(MS) */
  return 2;
}

static size_t ob_numcells_cp932(const ObCharsetInfo *cs  __attribute__((unused)),
                                const char *str, const char *str_end) {
  size_t clen = 0;
  const uint8_t *b = static_cast<const uint8_t *>(static_cast<const void*>(str));
  const uint8_t *e = static_cast<const uint8_t *>(static_cast<const void*>(str_end));

  for (clen = 0; b < e;) {
    if (*b >= 0xA1 && *b <= 0xDF) {
      clen++;
      b++;
    } else if (*b > 0x7F) {
      clen += 2;
      b += 2;
    } else {
      clen++;
      b++;
    }
  }
  return clen;
}

/*
  Returns a well formed length of a cp932 string.
  cp932 additional characters are also accepted.
*/

static size_t ob_well_formed_len_cp932(const ObCharsetInfo *cs  __attribute__((unused)),
                                       const char *b, const char *e, size_t pos,
                                       int *error) {
  const char *b0 = b;
  *error = 0;
  while (pos-- && b < e) {
    /*
      Cast to int8_t for extra safety.
      "char" can be unsigned by default
      on some platforms.
    */
    if (((int8_t)b[0]) >= 0) {
      /* Single byte ascii character */
      b++;
    } else if (iscp932head((uint8_t)*b) && (e - b) > 1 &&
               iscp932tail((uint8_t)b[1])) {
      /* Double byte character */
      b += 2;
    } else if (((uint8_t)*b) >= 0xA1 && ((uint8_t)*b) <= 0xDF) {
      /* Half width kana */
      b++;
    } else {
      /* Wrong byte sequence */
      *error = 1;
      break;
    }
  }
  return (size_t)(b - b0);
}
}  // extern "C"

static ObCollationHandler ob_collation_cp932_ci_handler = {
    nullptr, /* init */
    nullptr,
    ob_strnncoll_cp932,
    ob_strnncollsp_cp932,
    ob_strnxfrm_mb,
    ob_strnxfrmlen_simple,
    NULL,
    ob_like_range_mb,
    ob_wildcmp_mb, /* wildcmp  */
    ob_strcasecmp_8bit,
    ob_instr_mb,
    ob_hash_sort_simple,
    ob_propagate_simple};

static ObCharsetHandler ob_charset_cp932_handler = {nullptr, /* init */
                                                ismbchar_cp932,
                                                mbcharlen_cp932,
                                                ob_numchars_mb,
                                                ob_charpos_mb,
                                                ob_max_bytes_charpos_mb,
                                                ob_well_formed_len_cp932,
                                                ob_lengthsp_8bit,
                                                // my_numcells_cp932,
                                                ob_mb_wc_cp932, /* mb_wc */
                                                ob_wc_mb_cp932, /* wc_mb */
                                                ob_mb_ctype_mb,
                                                // my_caseup_str_mb,
                                                // my_casedn_str_mb,
                                                ob_caseup_mb,
                                                ob_casedn_mb,
                                                ob_fill_8bit,
                                                ob_strntol_8bit,
                                                ob_strntoul_8bit,
                                                ob_strntoll_8bit,
                                                ob_strntoull_8bit,
                                                ob_strntod_8bit,
                                                ob_strntoull10rnd_8bit,
                                                ob_scan_8bit,
                                                skip_trailing_space};

ObCharsetInfo ob_charset_cp932_japanese_ci = {
    95,
    0,
    0,                                               /* number */
    OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_STRNXFRM, /* state      */
    "cp932",                                         /* cs name    */
    "cp932_japanese_ci",                             /* m_coll_name */
    "SJIS for Windows Japanese",                     /* comment    */
    nullptr,                                         /* tailoring */
    nullptr,                                         /* coll_param */
    ctype_cp932,
    to_lower_cp932,
    to_upper_cp932,
    sort_order_cp932,
    nullptr,            /* uca          */
    nullptr,            /* tab_to_uni   */
    nullptr,            /* tab_from_uni */
    &ob_caseinfo_cp932, /* caseinfo     */
    nullptr,            /* state_map    */
    nullptr,            /* ident_map    */
    1,                  /* strxfrm_multiply */
    1,                  /* caseup_multiply  */
    1,                  /* casedn_multiply  */
    1,                  /* mbminlen   */
    2,                  /* mbmaxlen   */
    1,                  /* mbmaxlenlen*/
    0,                  /* min_sort_char */
    0xFCFC,             /* max_sort_char */
    ' ',                /* pad char      */
    true,               /* escape_with_backslash_is_dangerous */
    1,                  /* levels_for_compare */
    1,                  // unsure
    &ob_charset_cp932_handler,
    &ob_collation_cp932_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_cp932_bin = {
    96,
    0,
    0,                              /* number */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state */
    "cp932",                        /* cs name    */
    "cp932_bin",                    /* m_coll_name */
    "SJIS for Windows Japanese",    /* comment    */
    nullptr,                        /* tailoring */
    nullptr,                        /* coll_param */
    ctype_cp932,
    to_lower_cp932,
    to_upper_cp932,
    nullptr,            /* sort_order   */
    nullptr,            /* uca          */
    nullptr,            /* tab_to_uni   */
    nullptr,            /* tab_from_uni */
    &ob_caseinfo_cp932, /* caseinfo     */
    nullptr,            /* state_map    */
    nullptr,            /* ident_map    */
    1,                  /* strxfrm_multiply */
    1,                  /* caseup_multiply  */
    1,                  /* casedn_multiply  */
    1,                  /* mbminlen   */
    2,                  /* mbmaxlen   */
    1,                  /* mbmaxlenlen*/
    0,                  /* min_sort_char */
    0xFCFC,             /* max_sort_char */
    ' ',                /* pad char      */
    true,               /* escape_with_backslash_is_dangerous */
    1,                  /* levels_for_compare */
    1,                  // unsure
    &ob_charset_cp932_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};