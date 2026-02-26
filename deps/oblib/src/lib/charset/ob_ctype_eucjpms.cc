
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
#include "lib/charset/ob_ctype_eucjpms_tab.h"

extern "C" {
static unsigned ismbchar_eucjpms(const ObCharsetInfo *cs __attribute__((unused)),
                                 const char *p, const char *e) {
  return ((static_cast<uint8_t>(*p) < 0x80)
              ? 0
              : iseucjpms(*(p)) && (e) - (p) > 1 && iseucjpms(*((p) + 1))
                    ? 2
                    : iseucjpms_ss2(*(p)) && (e) - (p) > 1 && iskata(*((p) + 1))
                          ? 2
                          : iseucjpms_ss3(*(p)) && (e) - (p) > 2 &&
                                    iseucjpms(*((p) + 1)) &&
                                    iseucjpms(*((p) + 2))
                                ? 3
                                : 0);
}

static unsigned mbcharlen_eucjpms(const ObCharsetInfo *cs __attribute__((unused)),
                                  unsigned c) {
  return (iseucjpms(c) ? 2 : iseucjpms_ss2(c) ? 2 : iseucjpms_ss3(c) ? 3 : 1);
}
}  // extern "C"

extern "C" {
static int ob_mb_wc_eucjpms(const ObCharsetInfo *cs __attribute__((unused)),
                            ob_wc_t *pwc, const uint8_t *s, const uint8_t *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) /* ASCII code set: [00..7F] -> [U+0000..U+007F] */
  {
    *pwc = hi;
    return 1;
  }

  if (hi >= 0xA1 && hi <= 0xFE) /* JIS X 0208 code set: [A1..FE][A1..FE] */
  {
    if (s + 2 > e) return OB_CS_TOOSMALL2;
    return (*pwc = jisx0208_eucjpms_to_unicode[(hi << 8) + s[1]])
               ? 2
               : (s[1] < 0xA1 || s[1] > 0xFE) ? OB_CS_ILSEQ : -2;
  }

  /* JIS-X-0201 HALF WIDTH KATAKANA: [8E][A1..DF] -> [U+FF61..U+FF9F] */
  if (hi == 0x8E) {
    if (s + 2 > e) return OB_CS_TOOSMALL2;
    if (s[1] < 0xA1 || s[1] > 0xDF) return OB_CS_ILSEQ;
    *pwc = 0xFEC0 + s[1]; /* 0xFFC0 = 0xFF61 - 0xA1 */
    return 2;
  }

  if (hi == 0x8F) /* JIS X 0212 code set: [8F][A1..FE][A1..FE] */
  {
    if (s + 3 > e) return OB_CS_TOOSMALL3;
    return (*pwc = jisx0212_eucjpms_to_unicode[(((int)s[1]) << 8) + s[2]])
               ? 3
               : (s[1] < 0xA1 || s[1] > 0xFE || s[2] < 0xA1 || s[2] > 0xFE)
                     ? OB_CS_ILSEQ
                     : -3;
  }

  return OB_CS_ILSEQ;
}

static int ob_wc_mb_eucjpms(const ObCharsetInfo *cs __attribute__((unused)), ob_wc_t wc,
                            uint8_t *s, uint8_t *e) {
  int jp;

  if ((int)wc < 0x80) /* ASCII [00-7F] */
  {
    if (s >= e) return OB_CS_TOOSMALL;
    *s = (uint8_t)wc;
    return 1;
  }

  if (wc > 0xFFFF) return OB_CS_ILUNI;

  if ((jp = unicode_to_jisx0208_eucjpms[wc])) /* JIS-X-0208 MS */
  {
    if (s + 2 > e) return OB_CS_TOOSMALL2;
    s[0] = jp >> 8;
    s[1] = jp & 0xFF;
    return 2;
  }

  if ((jp = unicode_to_jisx0212_eucjpms[wc])) /* JIS-X-0212 MS */
  {
    if (s + 3 > e) return OB_CS_TOOSMALL3;
    s[0] = 0x8F;
    (s+1)[0] = jp >> 8;
    (s+1)[1] = jp & 0xFF;
    return 3;
  }

  if (wc >= 0xFF61 && wc <= 0xFF9F) /* Half width Katakana */
  {
    if (s + 2 > e) return OB_CS_TOOSMALL2;
    s[0] = 0x8E;
    s[1] = (uint8_t)(wc - 0xFEC0);
    return 2;
  }

  return OB_CS_ILUNI;
}

/*
  EUCJPMS encoding subcomponents:
  [x00-x7F]			# ASCII/JIS-Roman (one-byte/character)
  [x8E][xA0-xDF]		# half-width katakana (two bytes/char)
  [x8F][xA1-xFE][xA1-xFE]	# JIS X 0212-1990 (three bytes/char)
  [xA1-xFE][xA1-xFE]		# JIS X 0208:1997 (two bytes/char)
*/

static size_t ob_well_formed_len_eucjpms(const ObCharsetInfo *cs __attribute__((unused)),
                                         const char *beg, const char *end,
                                         size_t pos, int *error) {
  const uint8_t *b = static_cast<const uint8_t *>(static_cast<const void*>(beg));
  *error = 0;

  for (; pos && b < static_cast<const uint8_t *>(static_cast<const void*>(end)); pos--, b++) {
    const char *chbeg;
    unsigned ch = *b;

    if (ch <= 0x7F) /* one byte */
      continue;

    chbeg = static_cast<const char *>(static_cast<const void*>(b++));
    if (b >= static_cast<const uint8_t *>(static_cast<const void*>(end))) /* need more bytes */
      return (unsigned)(chbeg - beg);            /* unexpected EOL  */

    if (ch == 0x8E) /* [x8E][xA0-xDF] */
    {
      if (*b >= 0xA0 && *b <= 0xDF) continue;
      *error = 1;
      return (unsigned)(chbeg - beg); /* invalid sequence */
    }

    if (ch == 0x8F) /* [x8F][xA1-xFE][xA1-xFE] */
    {
      ch = *b++;
      if (b >= static_cast<const uint8_t *>(static_cast<const void*>(end))) {
        *error = 1;
        return (unsigned)(chbeg - beg); /* unexpected EOL */
      }
    }

    if (ch >= 0xA1 && ch <= 0xFE && *b >= 0xA1 &&
        *b <= 0xFE) /* [xA1-xFE][xA1-xFE] */
      continue;
    *error = 1;
    return (size_t)(chbeg - beg); /* invalid sequence */
  }
  return (size_t)(b - static_cast<const uint8_t *>(static_cast<const void*>(beg)));
}

static size_t ob_numcells_eucjpms(const ObCharsetInfo *cs __attribute__((unused)),
                                  const char *str, const char *str_end) {
  size_t clen;
  const uint8_t *b = static_cast<const uint8_t *>(static_cast<const void*>(str));
  const uint8_t *e = static_cast<const uint8_t *>(static_cast<const void*>(str_end));

  for (clen = 0; b < e;) {
    if (*b == 0x8E) {
      clen++;
      b += 2;
    } else if (*b == 0x8F) {
      clen += 2;
      b += 3;
    } else if (*b & 0x80) {
      clen += 2;
      b += 2;
    } else {
      clen++;
      b++;
    }
  }
  return clen;
}
}  // extern "C"

static ObCharsetHandler ob_charset_eucjpms_handler=
{
  NULL,
  ismbchar_eucjpms,
  mbcharlen_eucjpms,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_eucjpms,
  ob_lengthsp_8bit,
  //ob_numcells_8bit,
  ob_mb_wc_eucjpms,
  ob_wc_mb_eucjpms,
  ob_mb_ctype_mb,
  //ob_case_str_bin,
  //ob_case_str_bin,
  ob_caseup_ujis,
  ob_casedn_ujis,
  //ob_snprintf_8bit,
  //ob_long10_to_str_8bit,
  //ob_longlong10_to_str_8bit,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  //ob_strtoll10_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit,
  skip_trailing_space
};

static ObCollationHandler ob_collation_eucjpms_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_simple,
  ob_strnncollsp_simple,
  ob_strnxfrm_mb,
  ob_strnxfrmlen_simple,
  NULL,
  ob_like_range_mb,
  ob_wildcmp_mb,
  ob_strcasecmp_mb,
  ob_instr_mb,
  ob_hash_sort_simple,
  ob_propagate_simple
};

ObCharsetInfo ob_charset_eucjpms_japanese_ci = {
    97,
    0,
    0,                              /* number       */
    OB_CS_COMPILED | OB_CS_PRIMARY, /* state        */
    "eucjpms",                      /* cs name    */
    "eucjpms_japanese_ci",          /* m_coll_name */
    "UJIS for Windows Japanese",    /* comment      */
    nullptr,                        /* tailoring    */
    nullptr,                        /* coll_param   */
    ctype_eucjpms,
    to_lower_eucjpms,
    to_upper_eucjpms,
    sort_order_eucjpms,
    nullptr,              /* uca          */
    nullptr,              /* tab_to_uni   */
    nullptr,              /* tab_from_uni */
    &my_caseinfo_eucjpms, /* caseinfo    */
    nullptr,              /* state_map    */
    nullptr,              /* ident_map    */
    1,                    /* strxfrm_multiply */
    1,                    /* caseup_multiply  */
    2,                    /* casedn_multiply  */
    1,                    /* mbminlen     */
    3,                    /* mbmaxlen     */
    1,                    /* mbmaxlenlen*/
    0,                    /* min_sort_char */
    0xFEFE,               /* max_sort_char */
    ' ',                  /* pad_char      */
    false,                /* escape_with_backslash_is_dangerous */
    1,                    /* levels_for_compare */
    1,                    /* levels_for_order   */
    &ob_charset_eucjpms_handler,
    &ob_collation_eucjpms_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_eucjpms_bin = {
    98,
    0,
    0,                              /* number       */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state        */
    "eucjpms",                      /* cs name    */
    "eucjpms_bin",                  /* m_coll_name */
    "UJIS for Windows Japanese",    /* comment      */
    nullptr,                        /* tailoring    */
    nullptr,                        /* coll_param   */
    ctype_eucjpms,
    to_lower_eucjpms,
    to_upper_eucjpms,
    nullptr,              /* sort_order   */
    nullptr,              /* uca          */
    nullptr,              /* tab_to_uni   */
    nullptr,              /* tab_from_uni */
    &my_caseinfo_eucjpms, /* caseinfo    */
    nullptr,              /* state_map    */
    nullptr,              /* ident_map    */
    1,                    /* strxfrm_multiply */
    1,                    /* caseup_multiply  */
    2,                    /* casedn_multiply  */
    1,                    /* mbminlen     */
    3,                    /* mbmaxlen     */
    1,                    /* mbmaxlenlen*/
    0,                    /* min_sort_char */
    0xFEFE,               /* max_sort_char */
    ' ',                  /* pad_char      */
    false,                /* escape_with_backslash_is_dangerous */
    1,                    /* levels_for_compare */
    1,                    /* levels_for_order   */
    &ob_charset_eucjpms_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};