/**
 * Copyright (code) 2021 OceanBase
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
#include "lib/charset/ob_ctype_gb2312_tab.h"

extern "C" {

static unsigned ismbchar_gb2312(const ObCharsetInfo *cs [[maybe_unused]],
                                const char *p, const char *e) {
  return (isgb2312head(*(p)) && (e) - (p) > 1 && isgb2312tail(*((p) + 1)) ? 2
                                                                          : 0);
}

static unsigned mbcharlen_gb2312(const ObCharsetInfo *cs [[maybe_unused]],
                                 unsigned c) {
  return (isgb2312head(c) ? 2 : 1);
}
static int ob_wc_mb_gb2312(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t wc,
                           uint8_t *s, uint8_t *e) {
  int code;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((unsigned)wc < 0x80) {
    s[0] = (uint8_t)wc;
    return 1;
  }

  if (!(code = func_uni_gb2312_onechar(wc))) return OB_CS_ILUNI;

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  code |= 0x8080;
  s[0] = code >> 8;
  s[1] = code & 0xFF;
  return 2;
}

static int ob_mb_wc_gb2312(const ObCharsetInfo *cs [[maybe_unused]],
                           ob_wc_t *pwc, const uint8_t *s, const uint8_t *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) {
    pwc[0] = hi;
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (!(pwc[0] = func_gb2312_uni_onechar(((hi << 8) + s[1]) & 0x7F7F)))
    return -2;

  return 2;
}

/*
  Returns well formed length of a EUC-KR string.
*/
static size_t ob_well_formed_len_gb2312(const ObCharsetInfo *cs [[maybe_unused]],
                                        const char *b, const char *e,
                                        size_t pos, int *error) {
  const char *b0 = b;
  const char *emb = e - 1; /* Last possible end of an MB character */

  *error = 0;
  while (pos-- && b < e) {
    if ((uint8_t)b[0] < 128) {
      /* Single byte ascii character */
      b++;
    } else if (b < emb && isgb2312head(*b) && isgb2312tail(b[1])) {
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

static ObCollationHandler ob_collation_gb2312_ci_handler = {
    nullptr, /* init */
    nullptr,
    ob_strnncoll_simple, /* strnncoll  */
    ob_strnncollsp_simple,
    ob_strnxfrm_mb, /* strnxfrm   */
    ob_strnxfrmlen_simple,
    NULL,
    ob_like_range_mb, /* like_range */
    ob_wildcmp_mb,    /* wildcmp    */
    ob_strcasecmp_mb, /* instr      */
    ob_instr_mb,
    ob_hash_sort_simple,
    ob_propagate_simple};

static ObCharsetHandler ob_charset_gb2312_handler = {
  nullptr, /* init */
  ismbchar_gb2312,
  mbcharlen_gb2312,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_gb2312,
  ob_lengthsp_8bit,
  // ob_numcells_8bit,
  ob_mb_wc_gb2312, /* mb_wc */
  ob_wc_mb_gb2312, /* wc_mb */
  ob_mb_ctype_mb,
  // ob_caseup_str_mb,
  // ob_casedn_str_mb,
  ob_caseup_mb,
  ob_casedn_mb,
  // ob_snprintf_8bit,
  // ob_long10_to_str_8bit,
  // ob_longlong10_to_str_8bit,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  // ob_strtoll10_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit,
  skip_trailing_space
};

ObCharsetInfo ob_charset_gb2312_chinese_ci = {
    24,
    0,
    0,                              /* number */
    OB_CS_COMPILED | OB_CS_PRIMARY, /* state      */
    OB_GB2312,                       /* cs name    */
    OB_GB2312_CHINESE_CI,            /* m_coll_name */
    "GB2312 Simplified Chinese",    /* comment    */
    NULL,                        /* tailoring */
    NULL,                        /* coll_param */
    ctype_gb2312,
    to_lower_gb2312,
    to_upper_gb2312,
    sort_order_gb2312,
    NULL,             /* uca          */
    nullptr,             /* tab_to_uni   */
    nullptr,             /* tab_from_uni */
    &ob_caseinfo_gb2312, /* caseinfo     */
    NULL,             /* state_map    */
    NULL,             /* ident_map    */
    1,                   /* strxfrm_multiply */
    1,                   /* caseup_multiply  */
    1,                   /* casedn_multiply  */
    1,                   /* mbminlen   */
    2,                   /* mbmaxlen   */
    1,                   /* mbmaxlenlen*/
    0,                   /* min_sort_char */
    0xF7FE,              /* max_sort_char */
    ' ',                 /* pad char      */
    false,               /* escape_with_backslash_is_dangerous */
    1,                   /* levels_for_compare */
    1,
    &ob_charset_gb2312_handler,
    &ob_collation_gb2312_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_gb2312_bin = {
    86,
    0,
    0,                              /* number */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state      */
    OB_GB2312,                       /* cs name    */
    OB_GB2312_BIN,                   /* m_coll_name */
    "GB2312 Simplified Chinese",    /* comment    */
    NULL,                        /* tailoring */
    NULL,                        /* coll_param */
    ctype_gb2312,
    to_lower_gb2312,
    to_upper_gb2312,
    NULL,             /* sort_order   */
    NULL,             /* uca          */
    nullptr,             /* tab_to_uni   */
    nullptr,             /* tab_from_uni */
    &ob_caseinfo_gb2312, /* caseinfo     */
    NULL,             /* state_map    */
    NULL,             /* ident_map    */
    1,                   /* strxfrm_multiply */
    1,                   /* caseup_multiply  */
    1,                   /* casedn_multiply  */
    1,                   /* mbminlen   */
    2,                   /* mbmaxlen   */
    1,                   /* mbmaxlenlen*/
    0,                   /* min_sort_char */
    0xF7FE,              /* max_sort_char */
    ' ',                 /* pad char      */
    false,               /* escape_with_backslash_is_dangerous */
    1,                   /* levels_for_compare */
    1,
    &ob_charset_gb2312_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};
