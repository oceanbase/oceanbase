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
#include "lib/charset/ob_ctype_ujis_tab.h"

static unsigned int ismbchar_ujis(const ObCharsetInfo *cs __attribute__((unused)),
		 const char* p, const char *e)
{
  return ((static_cast<uint8_t>(*p) < 0x80)
              ? 0
              : isujis(*(p)) && (e) - (p) > 1 && isujis(*((p) + 1))
                    ? 2
                    : isujis_ss2(*(p)) && (e) - (p) > 1 && iskata(*((p) + 1))
                          ? 2
                          : isujis_ss3(*(p)) && (e) - (p) > 2 &&
                                    isujis(*((p) + 1)) && isujis(*((p) + 2))
                                ? 3
                                : 0);
}

static unsigned int mbcharlen_ujis(const ObCharsetInfo *cs __attribute__((unused)),
                          unsigned int c)
{
  return (isujis(c) ? 2 : isujis_ss2(c) ? 2 : isujis_ss3(c) ? 3 : 1);
}

static size_t ob_well_formed_len_ujis(const ObCharsetInfo *cs __attribute__((unused)),
                              const char *beg, const char *end,
                              size_t pos, int *error)
{
  const uint8_t *b = static_cast<const uint8_t*>(static_cast<const void*>(beg));

  for (*error = 0; pos && b < static_cast<const uint8_t*>(static_cast<const void*>(end)); pos--, b++) {
    const char *chbeg;
    unsigned ch = *b;

    if (ch <= 0x7F) /* one byte */
      continue;

    chbeg = static_cast<const char *>(static_cast<const void*>(b++));
    if (b >= static_cast<const uint8_t *>(static_cast<const void*>(end))) /* need more bytes */
    {
      *error = 1;
      return (size_t)(chbeg - beg); /* unexpected EOL  */
    }

    if (ch == 0x8E) /* [x8E][xA0-xDF] */
    {
      if (*b >= 0xA0 && *b <= 0xDF) continue;
      *error = 1;
      return (size_t)(chbeg - beg); /* invalid sequence */
    }

    if (ch == 0x8F) /* [x8F][xA1-xFE][xA1-xFE] */
    {
      ch = *b++;
      if (b >= static_cast<const uint8_t *>(static_cast<const void*>(end))) {
        *error = 1;
        return (size_t)(chbeg - beg); /* unexpected EOL */
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

static int ob_mb_wc_ujis(const ObCharsetInfo *cs __attribute__((unused)),
	                   ob_wc_t *pwc, const unsigned char *s, const unsigned char *e)
{
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) /* ASCII code set: [00..7F] -> [U+0000..U+007F] */
  {
    *pwc = hi;
    return 1;
  }

  if (hi >= 0xA1 && hi <= 0xFE) /* JIS-X-0208 code set: [A1..FE][A1..FE] */
  {
    if (s + 2 > e) return OB_CS_TOOSMALL2;
    return (*pwc = jisx0208_eucjp_to_unicode[(hi << 8) + s[1]])
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
    return (*pwc = jisx0212_eucjp_to_unicode[(((int)s[1]) << 8) + s[2]])
               ? 3
               : (s[1] < 0xA1 || s[1] > 0xFE || s[2] < 0xA1 || s[2] > 0xFE)
                     ? OB_CS_ILSEQ
                     : -3;
  }

  return OB_CS_ILSEQ;
}

static int ob_wc_mb_ujis(const ObCharsetInfo *cs  __attribute__((unused)),
	      ob_wc_t wc, unsigned char *s, unsigned char *e)
{
  int jp;

  if ((int)wc < 0x80) /* ASCII [00-7F] */
  {
    if (s >= e) return OB_CS_TOOSMALL;
    *s = (uint8_t)wc;
    return 1;
  }

  if (wc > 0xFFFF) return OB_CS_ILUNI;

  if ((jp = unicode_to_jisx0208_eucjp[wc])) /* JIS-X-0208 */
  {
    if (s + 2 > e) return OB_CS_TOOSMALL2;
    OB_PUT_MB2(s, jp);
    return 2;
  }

  if ((jp = unicode_to_jisx0212_eucjp[wc])) /* JIS-X-0212 */
  {
    if (s + 3 > e) return OB_CS_TOOSMALL3;
    s[0] = 0x8F;
    OB_PUT_MB2(s + 1, jp);
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



static const ObUnicaseInfoChar *get_case_info_for_ch(const ObCharsetInfo *cs,
                                                        unsigned plane,
                                                        unsigned page,
                                                        unsigned offs) {
  const ObUnicaseInfoChar *p;
  return (p = cs->caseinfo->page[page + plane * 256]) ? &p[offs & 0xFF]
                                                      : nullptr;
}

static size_t ob_casefold_ujis(const ObCharsetInfo *cs, char *src, size_t srclen,
                               char *dst, size_t dstlen [[maybe_unused]],
                               const uint8_t *map, size_t is_upper) {
  char *srcend = src + srclen, *dst0 = dst;

  while (src < srcend) {
    size_t mblen = cs->cset->ismbchar(cs, src, srcend);
    if (mblen) {
      const ObUnicaseInfoChar *ch;
      ch = (mblen == 2)
               ? get_case_info_for_ch(cs, 0, (uint8_t)src[0], (uint8_t)src[1])
               : get_case_info_for_ch(cs, 1, (uint8_t)src[1], (uint8_t)src[2]);
      if (ch) {
        int code = is_upper ? ch->toupper : ch->tolower;
        src += mblen;
        if (code > 0xFFFF) *dst++ = (char)(uint8_t)((code >> 16) & 0xFF);
        if (code > 0xFF) *dst++ = (char)(uint8_t)((code >> 8) & 0xFF);
        *dst++ = (char)(uint8_t)(code & 0xFF);
      } else {
        if (mblen == 3) *dst++ = *src++;
        *dst++ = *src++;
        *dst++ = *src++;
      }
    } else {
      *dst++ = (char)map[(uint8_t)*src++];
    }
  }
  return (size_t)(dst - dst0);
}

size_t ob_caseup_ujis(const ObCharsetInfo *cs, char *src, size_t srclen, char *dst, size_t dstlen) {
  assert(dstlen >= srclen * cs->caseup_multiply);
  assert(src != dst || cs->caseup_multiply == 1);
  return ob_casefold_ujis(cs, src, srclen, dst, dstlen, cs->to_upper, 1);
}

size_t ob_casedn_ujis(const ObCharsetInfo *cs, char *src, size_t srclen,
                      char *dst, size_t dstlen) {
  assert(dstlen >= srclen * cs->casedn_multiply);
  assert(src != dst || cs->casedn_multiply == 1);
  return ob_casefold_ujis(cs, src, srclen, dst, dstlen, cs->to_lower, 0);
}



static ObCollationHandler ob_collation_ujis_ci_handler =
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

static ObCharsetHandler ob_charset_ujis_handler=
{
  NULL,
  ismbchar_ujis,
  mbcharlen_ujis,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_ujis,
  ob_lengthsp_8bit,
  /* ob_numcells_8bit, */
  ob_mb_wc_ujis,
  ob_wc_mb_ujis,
  ob_mb_ctype_mb,
  /* ob_caseup_str_mb, */
  /* ob_casedn_str_mb, */
  ob_caseup_ujis,
  ob_casedn_ujis,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit,
  skip_trailing_space
};

ObCharsetInfo ob_charset_ujis_japanese_ci=
{
    12,0,0,
    OB_CS_COMPILED|OB_CS_PRIMARY,
    "ujis",
    "ujis_japanese_ci",
    "",
    NULL,
    NULL,
    ctype_ujis,
    to_lower_ujis,
    to_upper_ujis,
    sort_order_ujis,
    NULL,
    NULL,
    NULL,
    &ob_caseinfo_ujis,
    NULL,
    NULL,
    1,
    1,
    2,
    1,
    3,
    1,
    0,
    0xFEFE,
    ' ',
    false,
    1,
    1, // unsure
    &ob_charset_ujis_handler,
    &ob_collation_ujis_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_ujis_bin = {
    91,
    0,
    0,
    OB_CS_COMPILED|OB_CS_BINSORT,
    "ujis",
    "ujis_bin",
    "EUC-JP Japanese",
    nullptr,
    nullptr,
    ctype_ujis,
    to_lower_ujis,
    to_upper_ujis,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    &ob_caseinfo_ujis,
    nullptr,
    nullptr,
    1,
    1,
    2,
    1,
    3,
    1,
    0,
    0xFEFE,
    ' ',
    false,
    1,
    1,
    &ob_charset_ujis_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};