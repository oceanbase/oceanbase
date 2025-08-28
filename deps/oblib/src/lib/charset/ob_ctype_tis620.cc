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
#include "lib/charset/ob_ctype_tis620_tab.h"
#include "lib/charset/ob_ctype_tis620.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/charset/ob_tis620ci_sort_scanner.h"

/*
  Convert thai string to "Standard C String Function" sortable string

  SYNOPSIS
    thai2sortable()
    tstr		String to convert. Does not have to end with \0
    len			Length of tstr
*/

static size_t thai2sortable(uchar *tstr, size_t len) {
  uchar *p;
  size_t tlen;
  uchar l2bias;

  tlen = len;
  l2bias = 256 - 8;
  for (p = tstr; tlen > 0; p++, tlen--) {
    uchar c = *p;

    if (isthai(c)) {
      const int *t_ctype0 = t_ctype[c];

      if (isconsnt(c)) l2bias -= 8;
      if (isldvowel(c) && tlen != 1 && isconsnt(p[1])) {
        /* simply swap between leading-vowel and consonant */
        *p = p[1];
        p[1] = c;
        tlen--;
        p++;
        continue;
      }

      /* if found level 2 char (L2_GARAN,L2_TONE*,L2_TYKHU) move to last */
      if (t_ctype0[1] >= L2_GARAN) {
        /*
          l2bias use to control position weight of l2char
          example (*=l2char) XX*X must come before X*XX
        */
        memmove((char *)p, (char *)(p + 1), tlen - 1);
        tstr[len - 1] = l2bias + t_ctype0[1] - L2_GARAN + 1;
        p--;
        continue;
      }
    } else {
      l2bias -= 8;
      *p = to_lower_tis620[c];
    }
  }
  return len;
}

/*
  strncoll() replacement, compare 2 string, both are converted to sortable
  string

  NOTE:
    We can't cut strings at end \0 as this would break comparison with
    LIKE characters, where the min range is stored as end \0

  Arg: 2 Strings and it compare length
  Ret: strcmp result
*/

extern "C" {
static int ob_strnncoll_tis620(const ObCharsetInfo *cs [[maybe_unused]],
                               const uchar *s1, size_t len1, const uchar *s2,
                               size_t len2, bool s2_is_prefix) {
  uchar buf[80];
  uchar *tc1, *tc2;
  int res = 0;

  if (s2_is_prefix && len1 > len2) len1 = len2;

  tc1 = buf;
  if ((len1 + len2 + 2) > (int)sizeof(buf)) {
      Tis620CiSortScanner scanner1(s1, len1);
      Tis620CiSortScanner scanner2(s2, len2);
      uchar ch1;
      uchar ch2;
      while (scanner1.has_next() && scanner2.has_next()) {
         scanner1.get_next_character(ch1);
         scanner2.get_next_character(ch2);
         if (ch1 != ch2) {
            res = ((int)ch1 - (int)ch2);
            return res;
         }
      }
      if (scanner1.has_next()) {
          res = 1;
      }
      if (scanner2.has_next()) {
          res = -1;
      }
  } else {
      tc2 = tc1 + len1 + 1;
      memcpy(tc1, s1, len1);
      tc1[len1] = 0; /* if length(s1)> len1, need to put 'end of string' */
      memcpy(tc2, s2, len2);
      tc2[len2] = 0; /* put end of string */
      thai2sortable(tc1, len1);
      thai2sortable(tc2, len2);
      res = strcmp((char *)tc1, (char *)tc2);
  }
  return res;
}

static int ob_strnncollsp_tis620(const ObCharsetInfo *cs [[maybe_unused]],
                                 const uchar *a0, size_t a_length,
                                 const uchar *b0, size_t b_length,
                                 bool diff_if_only_endspace_difference __attribute__((unused))) {
  uchar buf[80], *end, *a, *b, *alloced = NULL;
  size_t length;
  int res = 0;

  a = buf;
  if ((a_length + b_length + 2) > (int)sizeof(buf)) {
    Tis620CiSortScanner scanner1(a0, a_length);
    Tis620CiSortScanner scanner2(b0, b_length);
    uchar ch1;
    uchar ch2;
    while (scanner1.has_next() && scanner2.has_next()) {
        scanner1.get_next_character(ch1);
        scanner2.get_next_character(ch2);
        if (ch1 != ch2) {
          res = ((int)ch1 - (int)ch2);
          return res;
        }
    }
    /*
      Check the next not space character of the longer key. If it's < ' ',
      then it's smaller than the other key.
    */
    while (scanner1.has_next()) {
      scanner1.get_next_character(ch1);
      if (ch1 != ' ') {
        res = (ch1 < ' ') ? -1 : 1;
        return res;
      }
    }
    while (scanner2.has_next()) {
      scanner2.get_next_character(ch2);
      if (ch2 != ' ') {
        res = (ch2 < ' ') ? 1 : -1;
        return res;
      }
    }
  } else {
    b = a + a_length + 1;
    memcpy(a, a0, a_length);
    a[a_length] = 0; /* if length(a0)> len1, need to put 'end of string' */
    memcpy(b, b0, b_length);
    b[b_length] = 0; /* put end of string */
    a_length = thai2sortable(a, a_length);
    b_length = thai2sortable(b, b_length);

    end = a + (length = std::min(a_length, b_length));
    while (a < end) {
      if (*a++ != *b++) {
        res = ((int)a[-1] - (int)b[-1]);
        return res;
      }
    }
    if (a_length != b_length) {
      int swap = 1;
      /*
        Check the next not space character of the longer key. If it's < ' ',
        then it's smaller than the other key.
      */
      if (a_length < b_length) {
        /* put shorter key in s */
        a_length = b_length;
        a = b;
        swap = -1; /* swap sign of result */
        res = -res;
      }
      for (end = a + a_length - length; a < end; a++) {
        if (*a != ' ') {
          res = (*a < ' ') ? -swap : swap;
          return res;
        }
      }
    }
  }
  return res;
}

/*
  strnxfrm replacement, convert Thai string to sortable string

  Arg: Destination buffer, source string, dest length and source length
  Ret: Converted string size
*/

static size_t ob_strnxfrm_tis620(const ObCharsetInfo *cs, uchar *dst,
                                 size_t dstlen, uint nweights, const uchar *src,
                                 size_t srclen, uint flags,bool* is_valid_unicode) {
  size_t dstlen0 = dstlen;
  size_t min_len = std::min(dstlen, srclen);
  size_t len = 0;
  *is_valid_unicode = 1;
  /*
    We don't use strmake here, since it requires one more character for
    the terminating '\0', while this function itself and the following calling
    functions do not require it
  */
  while (len < min_len) {
    if (!(dst[len] = src[len])) break;
    len++;
  }

  len = thai2sortable(dst, len);
  dstlen = std::min(dstlen, size_t(nweights));
  len = std::min(len, size_t(dstlen));
  len = ob_strxfrm_pad(cs, dst, dst + len, dst + dstlen, (uint)(dstlen - len),
                       flags);
  if ((flags & OB_STRXFRM_PAD_TO_MAXLEN) && len < dstlen0) {
    size_t fill_length = dstlen0 - len;
    cs->cset->fill(cs, (char *)dst + len, fill_length, cs->pad_char);
    len = dstlen0;
  }
  return len;
}
}  // extern "C"

extern "C" {
static int ob_mb_wc_tis620(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t *wc,
                           const uchar *str, const uchar *end) {
  if (str >= end) return OB_CS_TOOSMALL;

  *wc = cs_to_uni_tis620[*str];
  return (!wc[0] && str[0]) ? -1 : 1;
}

static int ob_wc_mb_tis620(const ObCharsetInfo *cs [[maybe_unused]], ob_wc_t wc,
                           uchar *str, uchar *end) {
  unsigned char *pl;

  if (str >= end) return OB_CS_TOOSMALL;

  pl = uni_to_cs_tis620[(wc >> 8) & 0xFF];
  str[0] = pl ? pl[wc & 0xFF] : '\0';
  return (!str[0] && wc) ? OB_CS_ILUNI : 1;
}
}  // extern "C"

static ObCollationHandler ob_collation_tis620_handler = {
    NULL, /* init */
    NULL,
    ob_strnncoll_tis620,
    ob_strnncollsp_tis620,
    ob_strnxfrm_tis620,
    ob_strnxfrmlen_simple,
    NULL,//varlen
    ob_like_range_simple,
    ob_wildcmp_8bit, /* wildcmp   */
    NULL,//ob_strcasecmp_8bit,
    ob_instr_simple, /* QQ: To be fixed */
    ob_hash_sort_simple,
    ob_propagate_simple};

static ObCharsetHandler ob_charset_tis620_handler = {
    NULL,           /* init */
    ob_ismbchar_8bit,           /* ismbchar  */
    ob_mbcharlen_8bit, /* mbcharlen */
    ob_numchars_8bit,
    ob_charpos_8bit,
    ob_max_bytes_charpos_8bit,
    ob_well_formed_len_8bit,
    ob_lengthsp_8bit,
    //ob_numcells_8bit,
    ob_mb_wc_tis620, /* mb_wc     */
    ob_wc_mb_tis620, /* wc_mb     */
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


ObCharsetInfo ob_charset_tis620_thai_ci = {
    18,
    0,
    0,                                               /* number    */
    OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_STRNXFRM, /* state     */
    "tis620",                                        /* cs name    */
    "tis620_thai_ci",                                /* m_coll_name */
    "TIS620 Thai",                                   /* comment   */
    NULL,                                         /* tailoring */
    NULL,                                         /* coll_param */
    ctype_tis620,
    to_lower_tis620,
    to_upper_tis620,
    sort_order_tis620,
    NULL,             /* uca          */
    NULL,             /* tab_to_uni   */
    NULL,             /* tab_from_uni */
    &ob_unicase_default, /* caseinfo     */
    NULL,             /* state_map    */
    NULL,             /* ident_map    */
    4,                   /* strxfrm_multiply */
    1,                   /* caseup_multiply  */
    1,                   /* casedn_multiply  */
    1,                   /* mbminlen   */
    1,                   /* mbmaxlen   */
    1,
    0,                   /* min_sort_char */
    255,                 /* max_sort_char */
    ' ',                 /* pad char      */
    false,               /* escape_with_backslash_is_dangerous */
    1,                   /* levels_for_compare */
    1,
    &ob_charset_tis620_handler,
    &ob_collation_tis620_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_tis620_bin = {
    89,
    0,
    0,
    OB_CS_COMPILED | OB_CS_BINSORT,
    "tis620",
    "tis620_bin",
    "TIS620 Thai",
    NULL,
    NULL,
    ctype_tis620,
    to_lower_tis620,
    to_upper_tis620,
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
    255,
    ' ',
    false,
    1,
    1,
    &ob_charset_tis620_handler,
    &ob_collation_8bit_bin_handler,
    PAD_SPACE};

void Tis620CiSortScanner::adjust_position(int cur, int next)
{
  cur_ = cur;
  next_ = next;
}

void Tis620CiSortScanner::move_next(uchar& ch, uint64_t& move_count) {
  uchar *p;
  size_t tlen;
  uchar l2bias;

  tlen = capacity_;
  uchar* tstr = const_cast<uchar*>(str_);
  l2bias = 256 - 8;
  uint64_t count = 0;
  for (p = tstr; tlen > 0; p++, tlen--) {
    uchar c = *p;

    if (isthai(c)) {
      const int *t_ctype0 = t_ctype[c];

      if (isconsnt(c)) l2bias -= 8;
      if (isldvowel(c) && tlen != 1 && isconsnt(p[1])) {
        tlen--;
        p++;
        continue;
      }

      if (t_ctype0[1] >= L2_GARAN) {
        ch = l2bias + t_ctype0[1] - L2_GARAN + 1;
        count++;
        if (count == move_count) {
          move_count++;
          break;
        }
      }
    } else {
      l2bias -= 8;
    }
  }
}
bool Tis620CiSortScanner::has_next() {
  if ((count_ == capacity_) ||
      ((count_ + move_count_-1)==capacity_)) {
    state_ = END_STATE;
  }
  return !(state_ & END_STATE);
}
int Tis620CiSortScanner::get_next_character(uchar& ch) {
    while (true) {
        if ((state_ & YEILD_STATE) || (state_ & END_STATE)) {
            if ((state_ & YEILD_STATE) && !(state_ & MOVE_STATE)) {
                count_++;
            }
            break;
        }
        if (INIT_STATE & state_) {
            if (cur_ + next_ >= capacity_) {
                if ((count_ == capacity_) ||
                    ((count_ + move_count_-1)==capacity_)) {
                  state_ = END_STATE;
                } else if (move_count_ == 0) {
                  move_count_ = 1;
                  state_ = MOVE_STATE;
                } else {
                  state_ = MOVE_STATE;
                }
            } else {
                uchar c = *(str_ + cur_ + next_);
                if (isthai(c)) {
                    state_ = ISTHAT_STATE;
                } else {
                    state_ = NOT_ISTHAT_STATE;
                }
            }
        } else if (ISTHAT_STATE & state_) {
            const uchar *p = (str_ + cur_ + next_);
            uchar c = *p;
            const int *t_ctype0 = t_ctype[c];
            if (isldvowel(c) && p < (str_+capacity_-1) && isconsnt(p[1])) {
                ch = p[1];
                adjust_position(cur_+1, -1);
                state_ = SWAP_STATE | YEILD_STATE;
            } else if (t_ctype0[1] >= L2_GARAN) {
                adjust_position(cur_+1, 0);
                state_ = INIT_STATE;
            } else {
                ch = c;
                adjust_position(cur_+1, 0);
                state_ = INIT_STATE | YEILD_STATE;
            }
        } else if (NOT_ISTHAT_STATE & state_) {
            uchar c = *(str_ + cur_ + next_);
            ch = to_lower_tis620[c];
            adjust_position(cur_+1, 0);
            state_ = INIT_STATE | YEILD_STATE;
        } else if (SWAP_STATE & state_) {
            ch = *(str_ + cur_ + next_);
            adjust_position(cur_+1, 0);
            state_ = INIT_STATE | YEILD_STATE;
        } else if (MOVE_STATE & state_) {
          if (version_ == MYSQL4) {
            state_ = INIT_STATE | MOVE_STATE | YEILD_STATE;
            move_next(ch, move_count_);
          } else if (version_ == MYSQL5x) {
            state_ = INIT_STATE | MOVE_STATE | YEILD_STATE;
            if ((count_ + move_count_) == capacity_) {
              move_next(ch, move_count_);
            } else {
              ch = str_[capacity_-1];
              move_count_++;
            }
          }
        }
    }
    state_ &= (~(YEILD_STATE | MOVE_STATE));
    return state_;
}

void debug_tis620_sortkey(const uchar *str, size_t len, uchar *dst, size_t dst_len, int version)
{
  Tis620CiSortScanner scanner(str, len,  version);
  uchar ch;
  size_t idx = 0;
  while (scanner.has_next()) {
    scanner.get_next_character(ch);
    if (idx < dst_len) {
      dst[idx] = ch;
      idx++;
    }
  }
}