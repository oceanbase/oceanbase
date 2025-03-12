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
#include "ob_ctype_big5_tab.h"

static uint16 big5strokexfrm(uint16 i) {
  // storke order
  if ((i == 0xA440) || (i == 0xA441))
    return 0xA440;
  else if (((i >= 0xA442) && (i <= 0xA453)) || ((i >= 0xC940) && (i <= 0xC944)))
    return 0xA442;
  else if (((i >= 0xA454) && (i <= 0xA47E)) || ((i >= 0xC945) && (i <= 0xC94C)))
    return 0xA454;
  else if (((i >= 0xA4A1) && (i <= 0xA4FD)) || ((i >= 0xC94D) && (i <= 0xC962)))
    return 0xA4A1;
  else if (((i >= 0xA4FE) && (i <= 0xA5DF)) || ((i >= 0xC963) && (i <= 0xC9AA)))
    return 0xA4FE;
  else if (((i >= 0xA5E0) && (i <= 0xA6E9)) || ((i >= 0xC9AB) && (i <= 0xCA59)))
    return 0xA5E0;
  else if (((i >= 0xA6EA) && (i <= 0xA8C2)) || ((i >= 0xCA5A) && (i <= 0xCBB0)))
    return 0xA6EA;
  else if ((i == 0xA260) || ((i >= 0xA8C3) && (i <= 0xAB44)) ||
           ((i >= 0xCBB1) && (i <= 0xCDDC)))
    return 0xA8C3;
  else if ((i == 0xA259) || (i == 0xF9DA) || ((i >= 0xAB45) && (i <= 0xADBB)) ||
           ((i >= 0xCDDD) && (i <= 0xD0C7)))
    return 0xAB45;
  else if ((i == 0xA25A) || ((i >= 0xADBC) && (i <= 0xB0AD)) ||
           ((i >= 0xD0C8) && (i <= 0xD44A)))
    return 0xADBC;
  else if ((i == 0xA25B) || (i == 0xA25C) || ((i >= 0xB0AE) && (i <= 0xB3C2)) ||
           ((i >= 0xD44B) && (i <= 0xD850)))
    return 0xB0AE;
  else if ((i == 0xF9DB) || ((i >= 0xB3C3) && (i <= 0xB6C2)) ||
           ((i >= 0xD851) && (i <= 0xDCB0)))
    return 0xB3C3;
  else if ((i == 0xA25D) || (i == 0xA25F) || (i == 0xC6A1) || (i == 0xF9D6) ||
           (i == 0xF9D8) || ((i >= 0xB6C3) && (i <= 0xB9AB)) ||
           ((i >= 0xDCB1) && (i <= 0xE0EF)))
    return 0xB6C3;
  else if ((i == 0xF9DC) || ((i >= 0xB9AC) && (i <= 0xBBF4)) ||
           ((i >= 0xE0F0) && (i <= 0xE4E5)))
    return 0xB9AC;
  else if ((i == 0xA261) || ((i >= 0xBBF5) && (i <= 0xBEA6)) ||
           ((i >= 0xE4E6) && (i <= 0xE8F3)))
    return 0xBBF5;
  else if ((i == 0xA25E) || (i == 0xF9D7) || (i == 0xF9D9) ||
           ((i >= 0xBEA7) && (i <= 0xC074)) || ((i >= 0xE8F4) && (i <= 0xECB8)))
    return 0xBEA7;
  else if (((i >= 0xC075) && (i <= 0xC24E)) || ((i >= 0xECB9) && (i <= 0xEFB6)))
    return 0xC075;
  else if (((i >= 0xC24F) && (i <= 0xC35E)) || ((i >= 0xEFB7) && (i <= 0xF1EA)))
    return 0xC24F;
  else if (((i >= 0xC35F) && (i <= 0xC454)) || ((i >= 0xF1EB) && (i <= 0xF3FC)))
    return 0xC35F;
  else if (((i >= 0xC455) && (i <= 0xC4D6)) || ((i >= 0xF3FD) && (i <= 0xF5BF)))
    return 0xC455;
  else if (((i >= 0xC4D7) && (i <= 0xC56A)) || ((i >= 0xF5C0) && (i <= 0xF6D5)))
    return 0xC4D7;
  else if (((i >= 0xC56B) && (i <= 0xC5C7)) || ((i >= 0xF6D6) && (i <= 0xF7CF)))
    return 0xC56B;
  else if (((i >= 0xC5C8) && (i <= 0xC5F0)) || ((i >= 0xF7D0) && (i <= 0xF8A4)))
    return 0xC5C8;
  else if (((i >= 0xC5F1) && (i <= 0xC654)) || ((i >= 0xF8A5) && (i <= 0xF8ED)))
    return 0xC5F1;
  else if (((i >= 0xC655) && (i <= 0xC664)) || ((i >= 0xF8EE) && (i <= 0xF96A)))
    return 0xC655;
  else if (((i >= 0xC665) && (i <= 0xC66B)) || ((i >= 0xF96B) && (i <= 0xF9A1)))
    return 0xC665;
  else if (((i >= 0xC66C) && (i <= 0xC675)) || ((i >= 0xF9A2) && (i <= 0xF9B9)))
    return 0xC66C;
  else if (((i >= 0xC676) && (i <= 0xC678)) || ((i >= 0xF9BA) && (i <= 0xF9C5)))
    return 0xC676;
  else if (((i >= 0xC679) && (i <= 0xC67C)) || ((i >= 0xF9C7) && (i <= 0xF9CB)))
    return 0xC679;
  else if ((i == 0xC67D) || ((i >= 0xF9CC) && (i <= 0xF9CF)))
    return 0xC67D;
  else if (i == 0xF9D0)
    return 0xF9D0;
  else if ((i == 0xC67E) || (i == 0xF9D1))
    return 0xC67E;
  else if ((i == 0xF9C6) || (i == 0xF9D2))
    return 0xF9C6;
  else if (i == 0xF9D3)
    return 0xF9D3;
  else if (i == 0xF9D4)
    return 0xF9D4;
  else if (i == 0xF9D5)
    return 0xF9D5;
  return 0xA140;
}


static int ob_strnncoll_big5_internal(const unsigned char **a_res, const unsigned char **b_res,
                                      size_t length) {
  const unsigned char *a = *a_res, *b = *b_res;

  while (length--) {
    if ((length > 0) && isbig5code(*a, *(a + 1)) && isbig5code(*b, *(b + 1))) {
      if (*a != *b || *(a + 1) != *(b + 1))
        return ((int)big5code(*a, *(a + 1)) - (int)big5code(*b, *(b + 1)));
      a += 2;
      b += 2;
      length--;
    } else if (sort_order_big5[*a++] != sort_order_big5[*b++])
      return ((int)sort_order_big5[a[-1]] - (int)sort_order_big5[b[-1]]);
  }
  *a_res = a;
  *b_res = b;
  return 0;
}

/* Compare strings */
extern "C" {
static int ob_strnncoll_big5(const ObCharsetInfo *cs [[maybe_unused]],
                             const unsigned char *a, size_t a_length, const unsigned char *b,
                             size_t b_length, bool b_is_prefix) {
  size_t length = std::min(a_length, b_length);
  int res = ob_strnncoll_big5_internal(&a, &b, length);
  return res ? res : (int)((b_is_prefix ? length : a_length) - b_length);
}

/* compare strings, ignore end space */

static int ob_strnncollsp_big5(const ObCharsetInfo *cs [[maybe_unused]],
                               const unsigned char *a, size_t a_length, const unsigned char *b,
                               size_t b_length, bool diff_if_only_endspace_difference) {
  size_t length = std::min(a_length, b_length);
  int res = ob_strnncoll_big5_internal(&a, &b, length);

  if (!res && a_length != b_length) {
    const unsigned char *end;
    int swap = 1;
    /*
      Check the next not space character of the longer key. If it's < ' ',
      then it's smaller than the other key.
    */
    if (a_length < b_length) {
      /* put longer key in a */
      a_length = b_length;
      a = b;
      swap = -1; /* swap sign of result */
      res = -res;
    }
    for (end = a + a_length - length; a < end; a++) {
      if (*a != ' ') return (*a < ' ') ? -swap : swap;
    }
  }
  return res;
}

static size_t ob_strnxfrm_big5(const ObCharsetInfo *cs, unsigned char *dst,
                               size_t dstlen, uint nweights, const unsigned char *src,
                               size_t srclen, uint flags, bool *is_valid_unicode) {
  unsigned char *d0 = dst;
  unsigned char *de = dst + dstlen;
  const unsigned char *se = src + srclen;
  const unsigned char *sort_order = cs->sort_order;

  for (; dst < de && src < se && nweights; nweights--) {
    if (cs->cset->ismbchar(cs, (const char *)src, (const char *)se)) {
      /*
        Note, it is safe not to check (src < se)
        in the code below, because ismbchar() would
        not return TRUE if src was too short
      */
      uint16 e = big5strokexfrm((uint16)big5code(*src, *(src + 1)));
      *dst++ = getbig5head(e);
      if (dst < de) *dst++ = getbig5tail(e);
      src += 2;
    } else
      *dst++ = sort_order ? sort_order[*src++] : *src++;
  }
  return ob_strxfrm_pad(cs, d0, dst, de, nweights, flags);
}

static unsigned int ismbchar_big5(const ObCharsetInfo *cs [[maybe_unused]],
                          const char *p, const char *e) {
  return (hasbig5head(*(p)) && (e) - (p) > 1 && hasbig5tail(*((p) + 1)) ? 2 : 0);
}

static unsigned int mbcharlen_big5(const ObCharsetInfo *cs [[maybe_unused]], uint c) {
  return (hasbig5head(c) ? 2 : 1);
}


/*
  Returns a well formed length of a BIG5 string.
  CP950 and SCS additional characters are also accepted.
*/
static size_t ob_well_formed_len_big5(const ObCharsetInfo *cs [[maybe_unused]],
                                      const char *b, const char *e, size_t pos,
                                      int *error) {
  const char *b0 = b;
  const char *emb = e - 1; /* Last possible end of an MB character */

  *error = 0;
  while (pos-- && b < e) {
    if ((unsigned char)b[0] < 128) {
      /* Single byte ascii character */
      b++;
    } else if ((b < emb) && isbig5code((unsigned char)*b, (unsigned char)b[1])) {
      /* Double byte character */
      b += 2;
    } else {
      /* Wrong byte sequence */
      *error = 1;
      break;
    }
  }
  return (size_t)(b - b0);
}

}

static ObUnicaseInfo ob_caseinfo_big5 = {0xFFFF, ob_caseinfo_pages_big5};

int func_big5_uni_onechar(int code) {
  if ((code >= 0xA140) && (code <= 0xC7FC))
    return (tab_big5_uni0[code - 0xA140]);
  if ((code >= 0xC940) && (code <= 0xF9DC))
    return (tab_big5_uni1[code - 0xC940]);
  return (0);
}

extern "C" {

static int func_uni_big5_onechar(int code) {
  if ((code >= 0x00A2) && (code <= 0x00F7))
    return (tab_uni_big50[code - 0x00A2]);
  if ((code >= 0x02C7) && (code <= 0x0451))
    return (tab_uni_big51[code - 0x02C7]);
  if ((code >= 0x2013) && (code <= 0x22BF))
    return (tab_uni_big52[code - 0x2013]);
  if ((code >= 0x2460) && (code <= 0x2642))
    return (tab_uni_big53[code - 0x2460]);
  if ((code >= 0x3000) && (code <= 0x3129))
    return (tab_uni_big54[code - 0x3000]);
  if ((code >= 0x32A3) && (code <= 0x32A3))
    return (tab_uni_big55[code - 0x32A3]);
  if ((code >= 0x338E) && (code <= 0x33D5))
    return (tab_uni_big56[code - 0x338E]);
  if ((code >= 0x4E00) && (code <= 0x9483))
    return (tab_uni_big57[code - 0x4E00]);
  if ((code >= 0x9577) && (code <= 0x9FA4))
    return (tab_uni_big58[code - 0x9577]);
  if ((code >= 0xFA0C) && (code <= 0xFA0D))
    return (tab_uni_big59[code - 0xFA0C]);
  if ((code >= 0xFE30) && (code <= 0xFFFD))
    return (tab_uni_big510[code - 0xFE30]);
  return (0);
}
static int ob_wc_mb_big5(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t wc,
                           unsigned char *s, unsigned char *e) {
  int code;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((int)wc < 0x80) {
    s[0] = (unsigned char)wc;
    return 1;
  }

  if (!(code = func_uni_big5_onechar(wc))) return OB_CS_ILUNI;

  if (s + 2 > e) return OB_CS_TOOSMALL;

  s[0] = code >> 8;
  s[1] = code & 0xFF;

  return 2;
}
static int ob_mb_wc_big5(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t *pwc,
                         const unsigned char *s, const unsigned char *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) {
    pwc[0] = hi;
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (!(pwc[0] = func_big5_uni_onechar((hi << 8) + s[1]))) return -2;

  return 2;
}

}

static ObCollationHandler ob_collation_big5_chinese_ci_handler = {
    NULL, /* init */
    NULL,
    ob_strnncoll_big5,
    ob_strnncollsp_big5,
    ob_strnxfrm_big5,
    ob_strnxfrmlen_simple,
    NULL,
    ob_like_range_mb,
    ob_wildcmp_mb,
    ob_strcasecmp_mb,
    ob_instr_mb,
    ob_hash_sort_simple,
    ob_propagate_simple};

static ObCharsetHandler ob_charset_big5_handler = {NULL,
  ismbchar_big5,
  mbcharlen_big5,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb, /* max_byptes charpos  */
  ob_well_formed_len_big5,
  ob_lengthsp_8bit,
  ob_mb_wc_big5, /* mb_wc */
  ob_wc_mb_big5, /* wc_mb */
  ob_mb_ctype_mb,
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


ObCharsetInfo ob_charset_big5_chinese_ci = {
    1,
    0,
    0,                                               /* number     */
    OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_STRNXFRM, /* state      */
    "big5",                                          /* cs name    */
    "big5_chinese_ci",                               /* m_coll_name */
    "Big5 Traditional Chinese",                      /* comment    */
    NULL,                                         /* tailoring */
    NULL,                                         /* coll_param */
    ctype_big5,
    to_lower_big5,
    to_upper_big5,
    sort_order_big5,
    NULL,              /* uca          */
    NULL,              /* tab_to_uni   */
    NULL,              /* tab_from_uni */
    &ob_caseinfo_big5, /* caseinfo     */
    NULL,           /* state_map    */
    NULL,           /* ident_map    */
    1,                 /* strxfrm_multiply */
    1,                 /* caseup_multiply  */
    1,                 /* casedn_multiply  */
    1,                 /* mbminlen   */
    2,                 /* mbmaxlen   */
    1,                 /* mbmaxlenlen   */
    0,                 /* min_sort_char */
    0xF9D5,            /* max_sort_char */
    ' ',               /* pad char      */
    true,              /* escape_with_backslash_is_dangerous */
    1,                 /* levels_for_compare */
    1,                 /* levels_for_order */
    &ob_charset_big5_handler,
    &ob_collation_big5_chinese_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_big5_bin = {
    84,
    0,
    0,                              /* number     */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state */
    "big5",                         /* cs name    */
    "big5_bin",                     /* m_coll_name */
    "Big5 Traditional Chinese",     /* comment    */
    NULL,                        /* tailoring */
    NULL,                        /* coll_param */
    ctype_big5,
    to_lower_big5,
    to_upper_big5,
    NULL,
    NULL,           /* uca          */
    NULL,           /* tab_to_uni   */
    NULL,           /* tab_from_uni */
    &ob_caseinfo_big5, /* caseinfo     */
    NULL,           /* state_map    */
    NULL,           /* ident_map    */
    1,                 /* strxfrm_multiply */
    1,                 /* caseup_multiply  */
    1,                 /* casedn_multiply  */
    1,                 /* mbminlen   */
    2,                 /* mbmaxlen   */
    1,                 /* mbmaxlenlen   */
    0,                 /* min_sort_char */
    0xF9FE,            /* max_sort_char */
    ' ',               /* pad char      */
    true,              /* escape_with_backslash_is_dangerous */
    1,                 /* levels_for_compare */
    1,                 /* levels_for_order */
    &ob_charset_big5_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};
