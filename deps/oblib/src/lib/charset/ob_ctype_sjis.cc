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
#include "lib/charset/ob_ctype_sjis_tab.h"
#include "lib/charset/ob_template_helper.h"



extern "C" {
static uint ismbchar_sjis(const ObCharsetInfo *cs [[maybe_unused]],
                          const char *p, const char *e) {
  return (issjishead((uchar)*p) && (e - p) > 1 && issjistail((uchar)p[1]) ? 2
                                                                          : 0);
}

static uint mbcharlen_sjis(const ObCharsetInfo *cs [[maybe_unused]], uint c) {
  return (issjishead((uchar)c) ? 2 : 1);
}
}  // extern "C"

static ObUnicaseInfo ob_caseinfo_sjis = {0xFFFF, ob_caseinfo_pages_sjis};

/**
 * @brief 对两个SJIS编码的字符串进行比较
 *
 * @param cs SJIS编码的字符集信息
 * @param a_res 指向第一个字符串的指针的指针
 * @param a_length 第一个字符串的长度
 * @param b_res 指向第二个字符串的指针的指针
 * @param b_length 第二个字符串的长度
 * @return int 返回值为0表示两个字符串相等，小于0表示第一个字符串小于第二个字符串，大于0表示第一个字符串大于第二个字符串
 */
static int ob_strnncoll_sjis_internal(const ObCharsetInfo *cs,
                                      const uchar **a_res, size_t a_length,
                                      const uchar **b_res, size_t b_length) {
  const uchar *a = *a_res, *b = *b_res;
  const uchar *a_end = a + a_length;
  const uchar *b_end = b + b_length;
  while (a < a_end && b < b_end) {
    if (ismbchar_sjis(cs, pointer_cast<const char *>(a),
                      pointer_cast<const char *>(a_end)) &&
        ismbchar_sjis(cs, pointer_cast<const char *>(b),
                      pointer_cast<const char *>(b_end))) {
      uint a_char = sjiscode(*a, *(a + 1));
      uint b_char = sjiscode(*b, *(b + 1));
      if (a_char != b_char) return (int)a_char - (int)b_char;
      a += 2;
      b += 2;
    } else {
      if (sort_order_sjis[(uchar)*a] != sort_order_sjis[(uchar)*b])
        return sort_order_sjis[(uchar)*a] - sort_order_sjis[(uchar)*b];
      a++;
      b++;
    }
  }
  *a_res = a;
  *b_res = b;

  return 0;
}


extern "C" {
  static int ob_strnncoll_sjis(const ObCharsetInfo *cs, const uchar* a,
                               size_t a_length, const uchar *b, size_t b_length, bool b_is_prefix) {
    int res = ob_strnncoll_sjis_internal(cs, &a, a_length, &b, b_length);
    if (b_is_prefix && a_length > b_length) a_length = b_length;
    return res ? res : (int)(a_length - b_length);
  }

  static int ob_strnncollsp_sjis(const ObCharsetInfo *cs, const uchar* a,
                                size_t a_length, const uchar *b, size_t b_length, bool b_is_prefix) {
    const uchar *a_end = a + a_length, *b_end = b + b_length;
    int res = ob_strnncoll_sjis_internal(cs, &a, a_length, &b, b_length);

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
        swap = -1;
        res = -res;
      }
      for (; a < a_end; ++a) {
        if (*a != ' ') return (*a < ' ') ? -swap : swap;
      }
    }
    return res;
  }
} // extern "C"

/**
  Scans an SJIS character from the input string
  and converts to Unicode code point.

  @param[in]  cs           Character set info, unused
  @param[out] pwc          Unicode code point
  @param[in]  s            Beginning of the input string
  @param[in]  e            End of the input string

  @retval   OB_CS_TOOSMALL If the string was too short to scan a character
  @retval   1              If a 1-byte character was scanned
  @retval   2              If a 2-byte character was scanned
  @retval  -2              If a 2-byte unassigned character was scanned
  @retval   OB_CS_ILSEQ    If a wrong byte sequence was found
*/
extern "C" {
static int ob_mb_wc_sjis(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t *pwc,
                         const uchar *s, const uchar *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) /* ASCII: [00..7F] -> [U+0000..U+007F] */
  {
    *pwc = hi;
    return 1;
  }

  /* JIS-X-0201 Half width Katakana: [A1..DF] -> [U+FF61..U+FF9F] */
  if (hi >= 0xA1 && hi <= 0xDF) {
    *pwc = sjis_to_unicode[hi];
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  /* JIS-X-0208 [81..9F,E0..FC][40..7E,80..FC] */
  if (!(pwc[0] = sjis_to_unicode[(hi << 8) + s[1]]))
    return (issjishead(hi) && issjistail(s[1])) ? -2 : OB_CS_ILSEQ;

  return 2;
}


/**
  Puts the given Unicode character into an SJIS string.

  @param[in] cs             Character set info, unused
  @param[in] wc             Unicode code point
  @param[in] s              Beginning of the out string
  @param[in] e              End of the out string

  @retval   OB_CS_TOOSMALL If the string was too short to put a character
  @retval   1              If a 1-byte character was put
  @retval   2              If a 2-byte character was put
  @retval   OB_CS_ILUNI    If the Unicode character does not exist in SJIS
*/
static int ob_wc_mb_sjis(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t wc,
                         uchar *s, uchar *e) {
  int code;

  if ((int)wc < 0x80) /* ASCII: [U+0000..U+007F] -> [00-7F] */
  {
    /*
      This branch is for performance purposes on ASCII range,
      to avoid using unicode_to_cp932[]: about 10% improvement.
    */
    if (wc == 0x5c) {
      /*
         Special case when converting from Unicode to SJIS:
         U+005C -> [81][5F] FULL WIDTH REVERSE SOLIDUS
      */
      code = 0x815F;
      goto mb;
    }
    if (s >= e) return OB_CS_TOOSMALL;
    s[0] = (uchar)wc; /* ASCII */
    return 1;
  }

  if (wc > 0xFFFF || !(code = unicode_to_sjis[wc])) /* Bad Unicode code point */
    return OB_CS_ILUNI;

  if (code <= 0xFF) {
    /* JIS-X-0201 HALF WIDTH KATAKANA [U+FF61..U+FF9F] -> [A1..DF] */
    if (s >= e) return OB_CS_TOOSMALL;
    s[0] = code;
    return 1;
  }

mb:
  if (s + 2 > e) return OB_CS_TOOSMALL2;

  OB_PUT_MB2(s, code); /* JIS-X-0208 */
  return 2;
}

static size_t ob_numcells_sjis(const ObCharsetInfo *cs [[maybe_unused]],
                               const char *str, const char *str_end) {
  size_t clen;
  const uchar *b = (const uchar *)str;
  const uchar *e = (const uchar *)str_end;

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
  Returns a well formed length of a SJIS string.
  CP932 additional characters are also accepted.
*/
static size_t ob_well_formed_len_sjis(const ObCharsetInfo *cs [[maybe_unused]],
                                      const char *b, const char *e, size_t pos,
                                      int *error) {
  const char *b0 = b;
  *error = 0;
  while (pos-- && b < e) {
    if ((uchar)b[0] < 128) {
      /* Single byte ascii character */
      b++;
    } else if (issjishead((uchar)*b) && (e - b) > 1 &&
               issjistail((uchar)b[1])) {
      /* Double byte character */
      b += 2;
    } else if (((uchar)*b) >= 0xA1 && ((uchar)*b) <= 0xDF) {
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
} // extern "C"



static ObCollationHandler ob_collation_sjis_ci_handler = {
  NULL, /* init */
  NULL, /* uninit */
  ob_strnncoll_sjis, /* strnn */
  ob_strnncollsp_sjis, /* strnncollsp */
  ob_strnxfrm_mb, /* strnxfrm */
  ob_strnxfrmlen_simple, /* strnxfrmlen */
  NULL,  /* strnxfrm_varlen */
  ob_like_range_mb, /* like range */
  ob_wildcmp_mb, /* wildcmp  */
  NULL, /* strcase cmp strcasecmp_8bit*/
  ob_instr_mb, /* instr */
  ob_hash_sort_simple, /* hash sort */
  ob_propagate_simple /* propagate */
};

static ObCharsetHandler ob_sjis_charset_handler =
{
  NULL,
  ismbchar_sjis,   /* ismbchar */
  mbcharlen_sjis,  /* mbcharlen */
  ob_numchars_mb,  /* numchars */
  ob_charpos_mb,   /* charpos 这里charpos =  */
  ob_max_bytes_charpos_mb, /* max_byptes charpos  */
  ob_well_formed_len_sjis, /* well_formed_len */
  ob_lengthsp_8bit, /* lengthsp */
  /*numcelss is ignoreed */
  ob_mb_wc_sjis, /* mb_wc */
  ob_wc_mb_sjis, /* wc_mb */
  ob_mb_ctype_mb, /* cypte */
  // ob_caseup_str_mb,
 //  ob_casedn_str_mb, /* case down */
  ob_caseup_mb, /* case up */
  ob_casedn_mb,
  // my_snprintf_8bit, /* sprintf */
  // my_long10_to_str_8bit, /* long10 to str */
  // my_longlong10_to_str_8bit, /* long long 10 to str */
  ob_fill_8bit,    /* fill */
  ob_strntol_8bit, /* strntol */
  ob_strntoul_8bit, /* strntoul*/
  ob_strntoll_8bit,  /* strntoll */
  ob_strntoull_8bit, /* strntoull */
  ob_strntod_8bit, /* strntod  */
  // ob_strtoll10_8bit, /* strtoll10 */
  ob_strntoull10rnd_8bit, /* strntoull10rnd */
  ob_scan_8bit, /* scan */
  skip_trailing_space
};

ObCharsetInfo ob_charset_sjis_japanese_ci = {
  13,
  0,
  0, /* number */
  OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_STRNXFRM |
      OB_CS_NONASCII,   /* state */
  "sjis",               /* cs name    */
  "sjis_japanese_ci",   /* m_coll_name */
  "Shift-JIS Japanese", /* comment    */
  NULL,              /* tailoring */
  NULL,              /* coll_param */
  ctype_sjis,
  to_lower_sjis,
  to_upper_sjis,
  sort_order_sjis,
  NULL,           /* uca          */
  NULL,           /* tab to uni*/
  NULL,           /* tab from uni */
  &ob_caseinfo_sjis, /* caseinfo     */
  NULL,           /* state_map    */
  NULL,           /* ident_map    */
  1,                 /* strxfrm_multiply */
  1,                 /* caseup_multiply  */
  1,                 /* casedn_multiply  */
  1,                 /* mbminlen   */
  2,                 /* mbmaxlen   */
  1,                 /* mbmaxlenlen   */
  0,                 /* min_sort_char */
  0xFCFC,            /* max_sort_char */
  ' ',               /* pad char      */
  true,              /* escape_with_backslash_is_dangerous */
  1,                 /* levels_for_compare */
  1, //TODO                /* levels_for_order */
  &ob_sjis_charset_handler,
  &ob_collation_sjis_ci_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_sjis_bin = {
  88,
  0,
  0,                                               /* number */
  OB_CS_COMPILED | OB_CS_BINSORT | OB_CS_NONASCII, /* state  */
  "sjis",                                          /* cs name    */
  "sjis_bin",                                      /* m_coll_name */
  "Shift-JIS Japanese",                            /* comment    */
  NULL,                                         /* tailoring */
  NULL,                                         /* coll_param */
  ctype_sjis,
  to_lower_sjis,
  to_upper_sjis,
  NULL,           /* sort_order   */
  NULL,           /* uca          */
  NULL,           /* tab to uni*/
  NULL,           /* tab from uni */
  &ob_caseinfo_sjis, /* caseinfo     */
  NULL,           /* state_map    */
  NULL,           /* ident_map    */
  1,                 /* strxfrm_multiply */
  1,                 /* caseup_multiply  */
  1,                 /* casedn_multiply  */
  1,                 /* mbminlen   */
  2,                 /* mbmaxlen   */
  1,                 /* mbmaxlenlen   */
  0,                 /* min_sort_char */
  0xFCFC,            /* max_sort_char */
  ' ',               /* pad char      */
  true,              /* escape_with_backslash_is_dangerous */
  1,                 /* levels_for_compare */
  1, //TODO              /* levels_for_order */
  &ob_sjis_charset_handler,
  &ob_collation_mb_bin_handler,
  PAD_SPACE
};
