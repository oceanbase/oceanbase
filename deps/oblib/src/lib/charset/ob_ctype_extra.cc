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
 #include "lib/charset/ob_ctype_dec8_tab.h"
 #include "lib/charset/ob_ctype_cp850_tab.h"
 #include "lib/charset/ob_ctype_hp8_tab.h"
 #include "lib/charset/ob_ctype_macroman_tab.h"
 #include "lib/charset/ob_ctype_swe7_tab.h"


static ObCharsetHandler ob_charset_8bit_handler = {
    ob_cset_init_8bit,
    NULL,
    ob_mbcharlen_8bit,
    ob_numchars_8bit,
    ob_charpos_8bit,
    ob_max_bytes_charpos_8bit,
    ob_well_formed_len_8bit,
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


ObCharsetInfo ob_charset_dec8_swedish_ci = {
  3,0,0,
  OB_CS_COMPILED|OB_CS_PRIMARY,
  "dec8",                     /* csname */
  "dec8_swedish_ci",                    /* m_collname */
  "DEC West European",                   /* comment */
  NULL,                    /* tailoring */
  NULL,                    /* coll_param */
  ctype_dec8_swedish_ci,                   /* ctype         */
  to_lower_dec8_swedish_ci,                /* to_lower */
  to_upper_dec8_swedish_ci,                /* to_upper */
  sort_order_dec8_swedish_ci,            /* sort_order */
  NULL,                    /* uca */
  to_uni_dec8_swedish_ci,                  /* to_uni        */
  NULL,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  NULL,                    /* state map        */
  NULL,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_simple_ci_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_dec8_bin = {
  69,0,0,
  OB_CS_COMPILED|OB_CS_BINSORT,
  "dec8",                     /* csname */
  "dec8_bin",                    /* m_collname */
  "DEC West European",                   /* comment */
  NULL,                    /* tailoring */
  NULL,                    /* coll_param */
  ctype_dec8_bin,                   /* ctype         */
  to_lower_dec8_bin,                /* to_lower */
  to_upper_dec8_bin,                /* to_upper */
  NULL,                     /* sort_order */
  NULL,                    /* uca */
  to_uni_dec8_bin,                  /* to_uni        */
  NULL,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  NULL,                    /* state map        */
  NULL,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_bin_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_cp850_general_ci = {
  4,0,0,
  OB_CS_COMPILED | OB_CS_PRIMARY,
  "cp850",                    /* csname */
  "cp850_general_ci",         /* m_collname */
  "DOS West European",        /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_cp850_general_ci,                   /* ctype         */
  to_lower_cp850_general_ci,                /* to_lower */
  to_upper_cp850_general_ci,                /* to_upper */
  sort_order_cp850_general_ci,            /* sort_order */
  nullptr,                    /* uca */
  to_uni_cp850_general_ci,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_simple_ci_handler,
  PAD_SPACE                   /* pad_attribute */
};


ObCharsetInfo ob_charset_cp850_bin = {
  80,0,0,
  OB_CS_COMPILED | OB_CS_BINSORT,
  "cp850",                     /* csname */
  "cp850_bin",                    /* m_collname */
  "DOS West European",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_cp850_bin,                   /* ctype         */
  to_lower_cp850_bin,                /* to_lower */
  to_upper_cp850_bin,                /* to_upper */
  nullptr,                     /* sort_order */
  nullptr,                    /* uca */
  to_uni_cp850_bin,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_bin_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_hp8_english_ci = {
  6,0,0,
  OB_CS_COMPILED | OB_CS_PRIMARY,
  "hp8",                     /* csname */
  "hp8_english_ci",                    /* m_collname */
  "HP West European",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_hp8_english_ci,                   /* ctype         */
  to_lower_hp8_english_ci,                /* to_lower */
  to_upper_hp8_english_ci,                /* to_upper */
  sort_order_hp8_english_ci,            /* sort_order */
  nullptr,                    /* uca */
  to_uni_hp8_english_ci,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_simple_ci_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_hp8_bin = {
  72,0,0,
  OB_CS_COMPILED | OB_CS_BINSORT,
  "hp8",                     /* csname */
  "hp8_bin",                    /* m_collname */
  "HP West European",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_hp8_bin,                   /* ctype         */
  to_lower_hp8_bin,                /* to_lower */
  to_upper_hp8_bin,                /* to_upper */
  nullptr,                     /* sort_order */
  nullptr,                    /* uca */
  to_uni_hp8_bin,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_bin_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_macroman_general_ci = {
  39,0,0,
  OB_CS_COMPILED | OB_CS_PRIMARY,
  "macroman",                     /* csname */
  "macroman_general_ci",                    /* m_collname */
  "Mac West European",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_macroman_general_ci,                   /* ctype         */
  to_lower_macroman_general_ci,                /* to_lower */
  to_upper_macroman_general_ci,                /* to_upper */
  sort_order_macroman_general_ci,            /* sort_order */
  nullptr,                    /* uca */
  to_uni_macroman_general_ci,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_simple_ci_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_macroman_bin = {
  53,0,0,
  OB_CS_COMPILED | OB_CS_BINSORT,
  "macroman",                     /* csname */
  "macroman_bin",                    /* m_collname */
  "Mac West European",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_macroman_bin,                   /* ctype         */
  to_lower_macroman_bin,                /* to_lower */
  to_upper_macroman_bin,                /* to_upper */
  nullptr,                     /* sort_order */
  nullptr,                    /* uca */
  to_uni_macroman_bin,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_bin_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_swe7_swedish_ci = {
  10,0,0,
  OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_NONASCII,
  "swe7",                     /* csname */
  "swe7_swedish_ci",                    /* m_collname */
  "7bit Swedish",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_swe7_swedish_ci,                   /* ctype         */
  to_lower_swe7_swedish_ci,                /* to_lower */
  to_upper_swe7_swedish_ci,                /* to_upper */
  sort_order_swe7_swedish_ci,            /* sort_order */
  nullptr,                    /* uca */
  to_uni_swe7_swedish_ci,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_simple_ci_handler,
  PAD_SPACE                   /* pad_attribute */
};

ObCharsetInfo ob_charset_swe7_bin = {
  82,0,0,
  OB_CS_COMPILED | OB_CS_BINSORT | OB_CS_NONASCII,
  "swe7",                     /* csname */
  "swe7_bin",                    /* m_collname */
  "7bit Swedish",                   /* comment */
  nullptr,                    /* tailoring */
  nullptr,                    /* coll_param */
  ctype_swe7_bin,                   /* ctype         */
  to_lower_swe7_bin,                /* to_lower */
  to_upper_swe7_bin,                /* to_upper */
  nullptr,                     /* sort_order */
  nullptr,                    /* uca */
  to_uni_swe7_bin,                  /* to_uni        */
  nullptr,                    /* from_uni         */
  &ob_unicase_default,        /* caseinfo         */
  nullptr,                    /* state map        */
  nullptr,                    /* ident map        */
  1,                          /* strxfrm_multiply */
  1,                          /* caseup_multiply  */
  1,                          /* casedn_multiply  */
  1,                          /* mbminlen         */
  1,                          /* mbmaxlen         */
  1,                          /* mbmaxlenlen      */
  0,                          /* min_sort_char    */
  255,                        /* max_sort_char    */
  ' ',                        /* pad_char         */
  false,                      /* escape_with_backslash_is_dangerous */
  1,                          /* levels_for_compare */
  1,                          /* levels_for_order */
  &ob_charset_8bit_handler,
  &ob_collation_8bit_bin_handler,
  PAD_SPACE                   /* pad_attribute */
};