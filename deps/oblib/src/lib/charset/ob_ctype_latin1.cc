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
#include "lib/utility/ob_macro_utils.h"
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
  //NULL,
  NULL,
  ob_mbcharlen_8bit,
  ob_numchars_8bit,
  ob_charpos_8bit,
  ob_max_bytes_charpos_8bit,
  ob_well_formed_len_8bit,
  ob_lengthsp_binary,
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
  ob_scan_8bit
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
    //NULL,
    //NULL,
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
    //NULL,
    //NULL,
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
