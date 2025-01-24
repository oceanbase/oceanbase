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
#include "lib/charset/ob_ctype_ascii_tab.h"

static ObCharsetHandler ob_charset_ascii_handler = {
    ob_cset_init_8bit,
    ob_ismbchar_8bit,
    ob_mbcharlen_8bit,
    ob_numchars_8bit,
    ob_charpos_8bit,
    ob_max_bytes_charpos_8bit,
    ob_well_formed_len_ascii,
    ob_lengthsp_8bit,
    //ob_numcells_8bit,
    ob_mb_wc_8bit,
    ob_wc_mb_8bit,
    ob_mb_ctype_8bit,
    //ob_caseup_str_8bit,
    //ob_casedn_str_8bit,
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

ObCharsetInfo ob_charset_ascii = {
  11,0,0,
  OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_PUREASCII,
  "ascii",
  "ascii_general_ci",
  "US ASCII",
  NULL,
  NULL,
  ctype_ascii_general_ci,
  to_lower_ascii_general_ci,
  to_upper_ascii_general_ci,
  sort_order_ascii_general_ci,
  NULL,
  to_uni_ascii_general_ci,
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
  255,
  ' ',
  false,
  1,
  1,
  &ob_charset_ascii_handler,
  &ob_collation_8bit_simple_ci_handler,
  PAD_SPACE};

ObCharsetInfo ob_charset_ascii_bin = {
  65,0,0,
  OB_CS_COMPILED | OB_CS_BINSORT | OB_CS_PUREASCII,
  "ascii",
  "ascii_bin",
  "US ASCII",
  NULL,
  NULL,
  ctype_ascii_bin,
  to_lower_ascii_bin,
  to_upper_ascii_bin,
  NULL,
  NULL,
  to_uni_ascii_bin,
  nullptr,
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
  255,
  ' ',
  false,
  1,
  1,
  &ob_charset_ascii_handler,
  &ob_collation_8bit_bin_handler,
  PAD_SPACE};