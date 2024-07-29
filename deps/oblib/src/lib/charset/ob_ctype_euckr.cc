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
#include "lib/charset/ob_ctype_euckr_tab.h"

#define iseuc_kr_head(c) ((0x81 <= (uint8_t)(c) && (uint8_t)(c) <= 0xfe))

#define iseuc_kr_tail1(c) ((uint8_t)(c) >= 0x41 && (uint8_t)(c) <= 0x5A)
#define iseuc_kr_tail2(c) ((uint8_t)(c) >= 0x61 && (uint8_t)(c) <= 0x7A)
#define iseuc_kr_tail3(c) ((uint8_t)(c) >= 0x81 && (uint8_t)(c) <= 0xFE)

#define iseuc_kr_tail(c) \
  (iseuc_kr_tail1(c) || iseuc_kr_tail2(c) || iseuc_kr_tail3(c))

extern "C" {
static unsigned ismbchar_euc_kr(const ObCharsetInfo *cs __attribute__((unused)),
                                const char *p, const char *e) {
  return ((static_cast<uint8_t>(*p) < 0x80)
              ? 0
              : iseuc_kr_head(*(p)) && (e) - (p) > 1 &&
                        iseuc_kr_tail(*((p) + 1))
                    ? 2
                    : 0);
}

static unsigned mbcharlen_euc_kr(const ObCharsetInfo *cs __attribute__((unused)),
                                 unsigned c) {
  return (iseuc_kr_head(c) ? 2 : 1);
}
}  // extern "C"

static int func_uni_ksc5601_onechar(int code) {
  if ((code >= 0x00A1) && (code <= 0x0167))
    return (tab_uni_ksc56010[code - 0x00A1]);
  if ((code >= 0x02C7) && (code <= 0x0451))
    return (tab_uni_ksc56011[code - 0x02C7]);
  if ((code >= 0x2015) && (code <= 0x2312))
    return (tab_uni_ksc56012[code - 0x2015]);
  if ((code >= 0x2460) && (code <= 0x266D))
    return (tab_uni_ksc56013[code - 0x2460]);
  if ((code >= 0x3000) && (code <= 0x327F))
    return (tab_uni_ksc56014[code - 0x3000]);
  if ((code >= 0x3380) && (code <= 0x33DD))
    return (tab_uni_ksc56015[code - 0x3380]);
  if ((code >= 0x4E00) && (code <= 0x947F))
    return (tab_uni_ksc56016[code - 0x4E00]);
  if ((code >= 0x9577) && (code <= 0x9F9C))
    return (tab_uni_ksc56017[code - 0x9577]);
  if ((code >= 0xAC00) && (code <= 0xD7A3))
    return (tab_uni_ksc56018[code - 0xAC00]);
  if ((code >= 0xF900) && (code <= 0xFA0B))
    return (tab_uni_ksc56019[code - 0xF900]);
  if ((code >= 0xFF01) && (code <= 0xFFE6))
    return (tab_uni_ksc560110[code - 0xFF01]);
  return (0);
}

extern "C" {
static int ob_wc_mb_euc_kr(const ObCharsetInfo *cs __attribute__((unused)), ob_wc_t wc,
                           uint8_t *s, uint8_t *e) {
  int code;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((unsigned)wc < 0x80) {
    s[0] = (uint8_t)wc;
    return 1;
  }

  if (!(code = func_uni_ksc5601_onechar(wc))) return OB_CS_ILUNI;

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  s[0] = code >> 8;
  s[1] = code & 0xFF;

  return 2;
}

static int ob_mb_wc_euc_kr(const ObCharsetInfo *cs __attribute__((unused)),
                           ob_wc_t *pwc, const uint8_t *s, const uint8_t *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) {
    pwc[0] = hi;
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (!(pwc[0] = func_ksc5601_uni_onechar((hi << 8) + s[1]))) return -2;

  return 2;
}

/*
  Returns well formed length of a EUC-KR string.
*/
static size_t ob_well_formed_len_euckr(const ObCharsetInfo *cs __attribute__((unused)),
                                       const char *b, const char *e, size_t pos,
                                       int *error) {
  const char *b0 = b;
  const char *emb = e - 1; /* Last possible end of an MB character */

  *error = 0;
  while (pos-- && b < e) {
    if ((uint8_t)b[0] < 128) {
      /* Single byte ascii character */
      b++;
    } else if (b < emb && iseuc_kr_head(*b) && iseuc_kr_tail(b[1])) {
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
}  // extern "C"




static ObCharsetHandler ob_charset_euckr_handler = {
    nullptr, /* init */
    ismbchar_euc_kr,
    mbcharlen_euc_kr,
    ob_numchars_mb,
    ob_charpos_mb,
    ob_max_bytes_charpos_mb,
    ob_well_formed_len_euckr,
    ob_lengthsp_8bit,
    /* ob_numcells_8bit, */
    ob_mb_wc_euc_kr,
    ob_wc_mb_euc_kr,
    ob_mb_ctype_mb,
    /* ob_caseup_str_mb, */
    /* ob_casedn_str_mb, */
    ob_caseup_mb,
    ob_casedn_mb,
    ob_fill_8bit,
    ob_strntol_8bit,
    ob_strntoul_8bit,
    ob_strntoll_8bit,
    ob_strntoull_8bit,
    ob_strntod_8bit,
    ob_strntoull10rnd_8bit,
    ob_scan_8bit};

ObCharsetInfo ob_charset_euckr_korean_ci = {
    19,
    0,
    0,                              /* number */
    OB_CS_COMPILED | OB_CS_PRIMARY, /* state      */
    "euckr",                        /* cs name    */
    "euckr_korean_ci",              /* m_coll_name */
    "EUC-KR Korean",                /* comment    */
    nullptr,                        /* tailoring */
    nullptr,                        /* coll_param */
    ctype_euc_kr,
    to_lower_euc_kr,
    to_upper_euc_kr,
    sort_order_euc_kr,
    nullptr,            /* uca          */
    nullptr,            /* tab_to_uni   */
    nullptr,            /* tab_from_uni */
    &ob_caseinfo_euckr, /* caseinfo     */
    nullptr,            /* state_map    */
    nullptr,            /* ident_map    */
    1,                  /* strxfrm_multiply */
    1,                  /* caseup_multiply  */
    1,                  /* casedn_multiply  */
    1,                  /* mbminlen   */
    2,                  /* mbmaxlen   */
    0,                  /* min_sort_char */
    0xFEFE,             /* max_sort_char */
    ' ',                /* pad char      */
    false,              /* escape_with_backslash_is_dangerous */
    1,                  /* levels_for_compare */
    1,                  /* levels_for_order, unsure */
    &ob_charset_euckr_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_euckr_bin = {
    85,
    0,
    0,                              /* number */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state */
    "euckr",                        /* cs name    */
    "euckr_bin",                    /* m_coll_name */
    "EUC-KR Korean",                /* comment    */
    nullptr,                        /* tailoring */
    nullptr,                        /* coll_param */
    ctype_euc_kr,
    to_lower_euc_kr,
    to_upper_euc_kr,
    nullptr,            /* sort_order   */
    nullptr,            /* uca          */
    nullptr,            /* tab_to_uni   */
    nullptr,            /* tab_from_uni */
    &ob_caseinfo_euckr, /* caseinfo     */
    nullptr,            /* state_map    */
    nullptr,            /* ident_map    */
    1,                  /* strxfrm_multiply */
    1,                  /* caseup_multiply  */
    1,                  /* casedn_multiply  */
    1,                  /* mbminlen   */
    2,                  /* mbmaxlen   */
    0,                  /* min_sort_char */
    0xFEFE,             /* max_sort_char */
    ' ',                /* pad char      */
    false,              /* escape_with_backslash_is_dangerous */
    1,                  /* levels_for_compare */
    1,                  /* levels_for_order, unsure */
    &ob_charset_euckr_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};
