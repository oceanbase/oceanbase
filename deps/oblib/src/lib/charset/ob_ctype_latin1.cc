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

#include "lib/charset/ob_ctype.h"
#include "lib/charset/ob_ctype_latin1_tab.h"

static int ob_mb_wc_latin1(const ObCharsetInfo *cs __attribute__((unused)),
                           ob_wc_t *pwc, const unsigned char *str,
                          const unsigned char *end) {

  if (str >= end) return OB_CS_TOOSMALL;
  *pwc = cs_to_uni[*str];
  return (!pwc[0] && str[0]) ? -1 : 1;

}

static int ob_wc_mb_latin1(const ObCharsetInfo *cs  __attribute__((unused)),
                           ob_wc_t wc, unsigned char *str, unsigned char *end) {
  const unsigned char *pl;

  if (str >= end) return OB_CS_TOOSMALL;

  if (wc > 0xFFFF) return OB_CS_ILUNI;

  pl = uni_to_cs[wc >> 8];
  str[0] = pl ? pl[wc & 0xFF] : '\0';
  return (!str[0] && wc) ? OB_CS_ILUNI : 1;
}

static ObCharsetHandler ob_charset_latin1_handler=
{
  NULL,
  ob_ismbchar_8bit,
  ob_mbcharlen_8bit,
  ob_numchars_8bit,
  ob_charpos_8bit,
  ob_max_bytes_charpos_8bit,
  ob_well_formed_len_8bit,
  ob_lengthsp_8bit,
  //ob_numcells_8bit,
  ob_mb_wc_latin1,
  ob_wc_mb_latin1,
  ob_mb_ctype_8bit,
  //ob_case_str_bin,
  //ob_case_str_bin,
  ob_caseup_8bit,
  ob_casedn_8bit,
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

ObCharsetInfo ob_charset_latin1 = {
    8,0,0,
    OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_SWEDISH_CI,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_latin1_german1_ci = {
    5,0,0,
    OB_CS_COMPILED | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_GERMAN1_CI,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1_german1_ci,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_latin1_danish_ci = {
    15,0,0,
    OB_CS_COMPILED | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_DANISH_CI,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1_danish_ci,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_latin1_general_ci = {
    48,0,0,
    OB_CS_COMPILED | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_GENERAL_CI,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1_general_ci,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_latin1_general_cs = {
    49,0,0,
    OB_CS_COMPILED | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_GENERAL_CS,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1_general_cs,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_latin1_spanish_ci = {
    94,0,0,
    OB_CS_COMPILED | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_SPANISH_CI,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1_spanish_ci,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_simple_ci_handler,
    PAD_SPACE};

/*
 * This file is the latin1 character set with German sorting
 *
 * The modern sort order is used, where:
 *
 * 'ä'  ->  "ae"
 * 'ö'  ->  "oe"
 * 'ü'  ->  "ue"
 * 'ß'  ->  "ss"
 */

/*
  Some notes about the following comparison rules:
  By definition, my_strnncoll_latin1_de() must work exactly as if one had called
  my_strnxfrm_latin1_de() on both strings and compared the resulting strings.

  This means that Ä must also match ÁE and Aè, because my_strxnfrm_latin1_de()
  will convert both to AE.

  The other option would be to not do any accent removal in
  sort_order_latin_de[] at all.
*/

extern "C" {
static int ob_strnncoll_latin1_de(const ObCharsetInfo *cs [[maybe_unused]],
                                  const uint8_t *a, size_t a_length,
                                  const uint8_t *b, size_t b_length,
                                  bool b_is_prefix) {
  const uint8_t *a_end = a + a_length;
  const uint8_t *b_end = b + b_length;
  uint8_t a_char = 0;
  uint8_t a_extend = 0;
  uint8_t b_char = 0;
  uint8_t b_extend = 0;

  while ((a < a_end || a_extend) && (b < b_end || b_extend)) {
    if (a_extend) {
      a_char = a_extend;
      a_extend = 0;
    } else {
      a_extend = combo2map[*a];
      a_char = combo1map[*a++];
    }
    if (b_extend) {
      b_char = b_extend;
      b_extend = 0;
    } else {
      b_extend = combo2map[*b];
      b_char = combo1map[*b++];
    }
    if (a_char != b_char) return (int)a_char - (int)b_char;
  }
  /*
    A simple test of string lengths won't work -- we test to see
    which string ran out first
  */
  return ((a < a_end || a_extend) ? (b_is_prefix ? 0 : 1)
                                  : (b < b_end || b_extend) ? -1 : 0);
}

static int ob_strnncollsp_latin1_de(const ObCharsetInfo *cs [[maybe_unused]],
                                    const uint8_t *a, size_t a_length,
                                    const uint8_t *b, size_t b_length,
                                    bool diff_if_only_endspace_difference) {
  const uint8_t *a_end = a + a_length;
  const uint8_t *b_end = b + b_length;
  uint8_t a_char = 0;
  uint8_t a_extend = 0;
  uint8_t b_char = 0;
  uint8_t b_extend = 0;

  while ((a < a_end || a_extend) && (b < b_end || b_extend)) {
    if (a_extend) {
      a_char = a_extend;
      a_extend = 0;
    } else {
      a_extend = combo2map[*a];
      a_char = combo1map[*a++];
    }
    if (b_extend) {
      b_char = b_extend;
      b_extend = 0;
    } else {
      b_extend = combo2map[*b];
      b_char = combo1map[*b++];
    }
    if (a_char != b_char) return (int)a_char - (int)b_char;
  }
  /* Check if double character last */
  if (a_extend) return 1;
  if (b_extend) return -1;

  int res = 0;
  if (a != a_end || b != b_end) {
    int swap = 1;
    if (diff_if_only_endspace_difference){
      res=1;
    }
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
      if (*a != ' ') return (*a < ' ') ? -swap : swap;
    }
  }
  return res;
}

static size_t ob_strnxfrm_latin1_de(const ObCharsetInfo *cs, uint8_t *dst,
                                    size_t dstlen, unsigned nweights,
                                    const uint8_t *src, size_t srclen,
                                    unsigned flags, bool* is_valid_unicode) {
  uint8_t *de = dst + dstlen;
  const uint8_t *se = src + srclen;
  uint8_t *d0 = dst;
  *is_valid_unicode = 1;
  for (; src < se && dst < de && nweights; src++, nweights--) {
    uint8_t chr = combo1map[*src];
    *dst++ = chr;
    if ((chr = combo2map[*src]) && dst < de) {
      *dst++ = chr;
    }
  }
  return ob_strxfrm_pad(cs, d0, dst, de, nweights, flags);
}

static void ob_hash_sort_latin1_de(const struct ObCharsetInfo *cs,
                                    const unsigned char *key, size_t len,
                                    ulong *nr1, ulong *nr2,
                                    const bool calc_end_space, hash_algo hash_algo) {
  /*
    Remove end space. We have to do this to be able to compare
    'AE' and 'Ä' as identical
  */
  const unsigned char *end = calc_end_space ? key + len : skip_trailing_space(cs, key, len);
  unsigned char data[HASH_BUFFER_LENGTH];
  int length = 0;

  if (NULL == hash_algo) {
    uint64_t tmp1 = *nr1;
    uint64_t tmp2 = *nr2;
    for (; key < end; key++) {
      unsigned X = (unsigned)combo1map[(unsigned)*key];
      tmp1 ^= (uint64_t)((((unsigned)tmp1 & 63) + tmp2) * X) + (tmp1 << 8);
      tmp2 += 3;
      if ((X = combo2map[*key])) {
        tmp1 ^= (uint64_t)((((unsigned)tmp1 & 63) + tmp2) * X) + (tmp1 << 8);
        tmp2 += 3;
      }
    }
    *nr1 = tmp1;
    *nr2 = tmp2;
  } else {
    length = (int)((unsigned char*)end - key) > HASH_BUFFER_LENGTH ?
                HASH_BUFFER_LENGTH : (int)((unsigned char*)end - key);
    int j = 0;
    for (int i = 0; i < length; i++, key++) {
      unsigned X = (unsigned)combo2map[(unsigned)*key];
      data[j++] = combo1map[(unsigned int) *key];
      if (X) {
        data[j++] = X;
      }
    }
    nr1[0] = hash_algo(&data, j, nr1[0]);
  }
}
}  // extern "C"

static ObCollationHandler ob_collation_german2_ci_handler = {
    NULL, /* init */
    NULL,
    ob_strnncoll_latin1_de,
    ob_strnncollsp_latin1_de,
    ob_strnxfrm_latin1_de,
    ob_strnxfrmlen_simple,
    NULL,
    ob_like_range_simple,
    ob_wildcmp_8bit,
    NULL,
    ob_instr_simple,
    ob_hash_sort_latin1_de,
    ob_propagate_complex};

ObCharsetInfo ob_charset_latin1_german2_ci = {
    31,0,0,
    OB_CS_COMPILED | OB_CS_CI,
    OB_LATIN1,
    OB_LATIN1_GERMAN2_CI,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    sort_order_latin1_de,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    2,
    1,
    1,
    1,
    1,
    1,
    0,
    247,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_german2_ci_handler,
    PAD_SPACE};


ObCharsetInfo ob_charset_latin1_bin = {
    47,0,0,
    OB_CS_COMPILED | OB_CS_BINSORT,
    OB_LATIN1,
    OB_LATIN1_BIN,
    "cp1252 West European",
    NULL,
    NULL,
    ctype_latin1,
    to_lower_latin1,
    to_upper_latin1,
    NULL,
    NULL,
    NULL,
    NULL,
    &ob_unicase_default,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0xFF,
    ' ',
    0,
    1,
    1,
    &ob_charset_latin1_handler,
    &ob_collation_8bit_bin_handler,
    PAD_SPACE};
