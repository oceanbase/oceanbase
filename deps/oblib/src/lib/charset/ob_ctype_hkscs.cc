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
#include "lib/charset/ob_ctype_hkscs_tab.h"
#include "lib/charset/ob_ctype_hkscs31_tab.h"
#include "lib/charset/ob_template_helper.h"

extern "C" {
static unsigned int ismbchar_hkscs(const ObCharsetInfo *cs [[maybe_unused]],
                                    const char *p, const char *e) {
  return (ishkscshead(*(p)) && (e) - (p) > 1 && ishkscstail(*((p) + 1)) ? 2 : 0);
}

static unsigned int mbcharlen_hkscs(const ObCharsetInfo *cs [[maybe_unused]], uint c) {
  return (ishkscshead(c) ? 2 : 1);
}


/*
  Returns a well formed length of a hkscs string.
  CP950 and HKSCS additional characters are also accepted.
*/
static size_t ob_well_formed_len_hkscs(const ObCharsetInfo *cs [[maybe_unused]],
                                      const char *b, const char *e, size_t pos,
                                      int *error) {
  const char *b0 = b;
  const char *emb = e - 1; /* Last possible end of an MB character */

  *error = 0;
  while (pos-- && b < e) {
    if ((unsigned char)b[0] < 128) {
      /* Single byte ascii character */
      b++;
    } else if ((b < emb) && ishkscscode((unsigned char)*b, (unsigned char)b[1])) {
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

static ObUnicaseInfo ob_caseinfo_hk = {0xFFFF, ob_caseinfo_pages_hkscs};


extern "C" {
// unicode to hkscs
static int ob_wc_mb_hkscs(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t wc,
                           unsigned char *s, unsigned char *e) {
  int code;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((int)wc < 0x80) {
    s[0] = (uchar)wc;
    return 1;
  }

  if (!(code = func_uni_hkscs_onechar(wc))) return OB_CS_ILUNI;

  if (s + 2 > e) return OB_CS_TOOSMALL;

  s[0] = code >> 8;
  s[1] = code & 0xFF;

  return 2;
}
// hkscs to unicode
static int ob_mb_wc_hkscs(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t *pwc,
                         const unsigned char *s, const unsigned char *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) {
    pwc[0] = hi;
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (!(pwc[0] = func_hkscs_uni_onechar((hi << 8) + s[1]))) return -2;

  return 2;
}

// unicode to hkscs31
static int ob_wc_mb_hkscs31(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t wc,
                           unsigned char *s, unsigned char *e) {
  int code;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((int)wc < 0x80) {
    s[0] = (uchar)wc;
    return 1;
  }

  if (!(code = func_uni_hkscs31_onechar(wc))) return OB_CS_ILUNI;

  if (s + 2 > e) return OB_CS_TOOSMALL;

  s[0] = code >> 8;
  s[1] = code & 0xFF;

  return 2;
}

// hkscs31 to unicode
static int ob_mb_wc_hkscs31(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t *pwc,
                         const unsigned char *s, const unsigned char *e) {
  int hi;

  if (s >= e) return OB_CS_TOOSMALL;

  if ((hi = s[0]) < 0x80) {
    pwc[0] = hi;
    return 1;
  }

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (!(pwc[0] = func_hkscs31_uni_onechar((hi << 8) + s[1]))) return -2;

  return 2;
}

}

bool hkscs_init(ObCharsetInfo *cs, ObCharsetLoader *loader) {
  bool succ = true;
  pair<decltype(hkscs_to_uni_map.begin()), bool> ret;
  if (hkscs_to_uni_map.size() == 0) {
    for (int i = 0; i < size_of_hkscs_to_uni_map_array && succ; ++i) {
      ret = hkscs_to_uni_map.insert(hkscs_to_uni_map_array[i]);
      succ = succ && ret.second;
    }
  }

  if (succ && uni_to_hkscs_map.size() == 0) {
    for (int i = 0; i < size_of_uni_to_hkscs_map_array && succ; ++i) {
      ret = uni_to_hkscs_map.insert(uni_to_hkscs_map_array[i]);
      succ = succ && ret.second;
    }
  }
  hkscs_to_uni_map.rehash(20019);
  uni_to_hkscs_map.rehash(20019);
  return succ;
}

bool hkscs31_init(ObCharsetInfo *cs, ObCharsetLoader *loader) {
  bool succ = true;
  pair<decltype(hkscs31_to_uni_map.begin()), bool> ret;
  if (hkscs31_to_uni_map.size() == 0) {
    for (int i = 0; i < size_of_hkscs31_to_uni_map_array && succ; ++i) {
	    ret = hkscs31_to_uni_map.insert(hkscs31_to_uni_map_array[i]);
      succ = succ && ret.second;
    }
  }
  pair<decltype(uni_to_hkscs31_map.begin()), bool> rett;
  if (succ && uni_to_hkscs31_map.size() == 0) {
    for (int i = 0; i < size_of_uni_to_hkscs31_map_array && succ; ++i) {
	    rett = uni_to_hkscs31_map.insert(uni_to_hkscs31_map_array[i]);
      succ = succ && rett.second;
	 }
  }
  hkscs31_to_uni_map.rehash(20019);
  uni_to_hkscs31_map.rehash(20019);
  return succ;
}

static ObCharsetHandler ob_charset_hkscs_handler = {
  hkscs_init,
  ismbchar_hkscs,
  mbcharlen_hkscs,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb, /* max_bytes charpos  */
  ob_well_formed_len_hkscs,
  ob_lengthsp_8bit,
  ob_mb_wc_hkscs, /* mb_wc */
  ob_wc_mb_hkscs, /* wc_mb */
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
  skip_trailing_space
};

static ObCharsetHandler ob_charset_hkscs31_handler = {
  hkscs31_init,
  ismbchar_hkscs,
  mbcharlen_hkscs,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb, /* max_bytes charpos  */
  ob_well_formed_len_hkscs,
  ob_lengthsp_8bit,
  ob_mb_wc_hkscs31, /* mb_wc */
  ob_wc_mb_hkscs31, /* wc_mb */
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
  skip_trailing_space
};

ObCharsetInfo ob_charset_hkscs_bin = {
    152,
    0,
    0,                              /* number     */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state */
    "hkscs",                         /* cs name    */
    "hkscs_bin",                     /* m_coll_name */
    "HKSCS 2000 Traditional Chinese",     /* comment    */
    NULL,                        /* tailoring */
    NULL,                        /* coll_param */
    ctype_hkscs,
    to_lower_hkscs,
    to_upper_hkscs,
    NULL,
    NULL,           /* uca          */
    NULL,           /* tab_to_uni   */
    NULL,           /* tab_from_uni */
    &ob_caseinfo_hk, /* caseinfo     */
    NULL,           /* state_map    */
    NULL,           /* ident_map    */
    1,                 /* strxfrm_multiply */
    1,                 /* caseup_multiply  */
    1,                 /* casedn_multiply  */
    1,                 /* mbminlen   */
    2,                 /* mbmaxlen   */
    1,                 /* mbmaxlenlen   */
    0,                 /* min_sort_char */
    0xFEFE,            /* max_sort_char */
    ' ',               /* pad char      */
    true,              /* escape_with_backslash_is_dangerous */
    1,                 /* levels_for_compare */
    1,                 /* levels_for_order */
    &ob_charset_hkscs_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_hkscs31_bin = {
    153,
    0,
    0,                              /* number     */
    OB_CS_COMPILED | OB_CS_BINSORT, /* state */
    "hkscs31",                         /* cs name    */
    "hkscs31_bin",                     /* m_coll_name */
    "HKSCS 2001 Traditional Chinese",     /* comment    */
    NULL,                        /* tailoring */
    NULL,                        /* coll_param */
    ctype_hkscs,
    to_lower_hkscs,
    to_upper_hkscs,
    NULL,
    NULL,           /* uca          */
    NULL,           /* tab_to_uni   */
    NULL,           /* tab_from_uni */
    &ob_caseinfo_hk, /* caseinfo     */
    NULL,           /* state_map    */
    NULL,           /* ident_map    */
    1,                 /* strxfrm_multiply */
    1,                 /* caseup_multiply  */
    1,                 /* casedn_multiply  */
    1,                 /* mbminlen   */
    2,                 /* mbmaxlen   */
    1,                 /* mbmaxlenlen   */
    0,                 /* min_sort_char */
    0xFEFE,            /* max_sort_char */
    ' ',               /* pad char      */
    true,              /* escape_with_backslash_is_dangerous */
    1,                 /* levels_for_compare */
    1,                 /* levels_for_order */
    &ob_charset_hkscs31_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};
