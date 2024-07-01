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
 /*
*
* Version: $Id
*
* Authors:
*      - initial release
*
*/

#include "lib/charset/ob_ctype.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/charset/ob_uctype.h"
#include "lib/charset/mb_wc.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/charset/ob_ctype_utf8_tab.h"

#define IS_CONTINUATION_BYTE(code) (((code) >> 6) == 0x02)

static int ob_valid_mbcharlen_utf8mb3(const uchar *s, const uchar *e)
{
  uchar c;
  ob_charset_assert(s < e);
  c= s[0];
  if (c < 0x80)
    return 1;
  if (c < 0xc2)
    return OB_CS_ILSEQ;
  if (c < 0xe0)
  {
    if (s+2 > e) /* We need 2 characters */
      return OB_CS_TOOSMALL2;
    if (!(IS_CONTINUATION_BYTE(s[1])))
      return OB_CS_ILSEQ;
    return 2;
  }
  ob_charset_assert(c < 0xf0);
  if (s+3 > e) /* We need 3 characters */
    return OB_CS_TOOSMALL3;
  if (!(IS_CONTINUATION_BYTE(s[1]) && IS_CONTINUATION_BYTE(s[2]) && (c >= 0xe1 || s[1] >= 0xa0)))
    return OB_CS_ILSEQ;
  return 3;
}

static int ob_valid_mbcharlen_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)), const uchar *s, const uchar *e)
{
  uchar c;
  if (s >= e)
    return OB_CS_TOOSMALL;
  c= s[0];
  if (c < 0xf0)
    return ob_valid_mbcharlen_utf8mb3(s, e);
  if (c < 0xf5)
  {
    if (s + 4 > e) /* We need 4 characters */
      return OB_CS_TOOSMALL4;
    if (!(IS_CONTINUATION_BYTE(s[1]) &&
          IS_CONTINUATION_BYTE(s[2]) &&
          IS_CONTINUATION_BYTE(s[3]) &&
          (c >= 0xf1 || s[1] >= 0x90) &&
          (c <= 0xf3 || s[1] <= 0x8F)))
      return OB_CS_ILSEQ;
    return 4;
  }
  return OB_CS_ILSEQ;
}

static uint ob_ismbchar_utf8mb4(const ObCharsetInfo *cs, const char *b, const char *e)
{
  int res= ob_valid_mbcharlen_utf8mb4(cs, (const uchar*)b, (const uchar*)e);
  return (res > 1) ? res : 0;
}


static uint ob_mbcharlen_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)), uint c)
{
  if (c < 0x80)
    return 1;
  if (c < 0xc2)
    return 0; /* Illegal mb head */
  if (c < 0xe0)
    return 2;
  if (c < 0xf0)
    return 3;
  if (c < 0xf8)
    return 4;
  return 0; /* Illegal mb head */;
}

static size_t ob_well_formed_len_utf8mb4(const ObCharsetInfo *cs,
                                         const char *b, const char *e,
                                         size_t pos, int *error)
{
  const char *b_start= b;
  *error= 0;
  while (pos)
  {
    int mb_len;
    if ((mb_len= ob_valid_mbcharlen_utf8mb4(cs, (uchar*) b, (uchar*) e)) <= 0)
    {
      *error= b < e ? 1 : 0;
      break;
    }
    b+= mb_len;
    pos--;
  }
  return (size_t) (b - b_start);
}

static int ob_mb_wc_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)),
                 ob_wc_t * pwc, const uchar *s, const uchar *e)
{
  uchar c;
  if (s >= e)
    return OB_CS_TOOSMALL;
  c= s[0];
  if (c < 0x80)
  {
    *pwc= c;
    return 1;
  }
  else if (c < 0xc2)
    return OB_CS_ILSEQ;
  else if (c < 0xe0)
  {
    if (s + 2 > e) /* We need 2 characters */
      return OB_CS_TOOSMALL2;
    if (!(IS_CONTINUATION_BYTE(s[1])))
      return OB_CS_ILSEQ;
    *pwc= ((ob_wc_t) (c & 0x1f) << 6) | (ob_wc_t) (s[1] ^ 0x80);
    return 2;
  }
  else if (c < 0xf0)
  {
    if (s + 3 > e) /* We need 3 characters */
      return OB_CS_TOOSMALL3;
    if (!(IS_CONTINUATION_BYTE(s[1]) && IS_CONTINUATION_BYTE(s[2]) &&
          (c >= 0xe1 || s[1] >= 0xa0)))
      return OB_CS_ILSEQ;
    *pwc= ((ob_wc_t) (c & 0x0f) << 12)   |
          ((ob_wc_t) (s[1] ^ 0x80) << 6) |
           (ob_wc_t) (s[2] ^ 0x80);
    return 3;
  }
  else if (c < 0xf5)
  {
    if (s + 4 > e) /* We need 4 characters */
      return OB_CS_TOOSMALL4;
    if (!(IS_CONTINUATION_BYTE(s[1]) &&
          IS_CONTINUATION_BYTE(s[2]) &&
          IS_CONTINUATION_BYTE(s[3]) &&
          (c >= 0xf1 || s[1] >= 0x90) &&
          (c <= 0xf3 || s[1] <= 0x8F)))
      return OB_CS_ILSEQ;
    *pwc = ((ob_wc_t) (c & 0x07) << 18)    |
           ((ob_wc_t) (s[1] ^ 0x80) << 12) |
           ((ob_wc_t) (s[2] ^ 0x80) << 6)  |
            (ob_wc_t) (s[3] ^ 0x80);
    return 4;
  }
  return OB_CS_ILSEQ;
}

static int ob_wc_mb_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)),
                 ob_wc_t w_char, unsigned char *r, unsigned char *end)
{
  int bytes = 0;
  int ret = 0;
  int64_t len = (int64_t)(end - r);
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_CS_TOOSMALL;
  } else if (w_char < 0x80) { //7	U+0000	U+007F	1	0xxxxxxx
    bytes = 1;
  } else if (w_char < 0x800) {//11	U+0080	U+07FF	2	110xxxxx	10xxxxxx
    bytes = 2;
  } else if (w_char < 0x10000) {//16	U+0800	U+FFFF	3	1110xxxx	10xxxxxx	10xxxxxx
    bytes = 3;
  } else if (w_char < 0x200000) {// 21	U+10000	U+1FFFFF 4	11110xxx	10xxxxxx	10xxxxxx	10xxxxxx
    bytes = 4;
  } else {
    ret = OB_CS_ILUNI;
  }
  if (OB_UNLIKELY(ret != 0)) {
    //do nothing
  } else if (OB_UNLIKELY(bytes > len)) {
    ret = OB_CS_TOOSMALLN(bytes);
  } else {
    switch (bytes) {
      case 4: r[3] = (unsigned char) (0x80 | (w_char & 0x3f)); w_char >>= 6; w_char |= 0x10000;
      case 3: r[2] = (unsigned char) (0x80 | (w_char & 0x3f)); w_char >>= 6; w_char |= 0x800;
      case 2: r[1] = (unsigned char) (0x80 | (w_char & 0x3f)); w_char >>= 6; w_char |= 0xc0;
      case 1: r[0] = (unsigned char) w_char;
    }
    ret = bytes;
  }
  return ret;
}

static inline void ob_tolower_utf8mb4(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page = uni_plane->page[(*wc >> 8)];
    if (OB_NOT_NULL(page)) {
      *wc= page[*wc & 0xFF].tolower;
  }
  }
  return;
}


static inline void ob_toupper_utf8mb4(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page = uni_plane->page[(*wc >> 8)];
    if (OB_NOT_NULL(page)) {
      *wc= page[*wc & 0xFF].toupper;
  }
  }
  return;
}

static size_t ob_caseup_utf8mb4(const ObCharsetInfo *cs, char *src, size_t srclen,
                  char *dst, size_t dstlen)
{
  ob_wc_t w_char;
  int srcres = 0, dstres = 0;
  char *srcend = src + srclen, *dstend = dst + dstlen, *dst0 = dst;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  ob_charset_assert(src != dst || cs->caseup_multiply == 1);
  while ((src < srcend)) {
    if ((srcres = ob_mb_wc_utf8mb4(cs, &w_char, (unsigned char *) src, (unsigned char*) srcend)) <= 0) {
      break;
    }
    ob_toupper_utf8mb4(uni_plane, &w_char);
    if ((dstres = ob_wc_mb_utf8mb4(cs, w_char, (unsigned char*) dst, (unsigned char*) dstend)) <= 0) {
      break;
    } else {
      src += srcres;
      dst += dstres;
    }
  }
  return (size_t) (dst - dst0);
}

static size_t ob_casedn_utf8mb4(const ObCharsetInfo *cs,
                  char *src, size_t srclen,
                  char *dst, size_t dstlen)
{
  ob_wc_t w_char;
  int srcres = 0, dstres = 0;
  char *srcend = src + srclen, *dstend= dst + dstlen, *dst0= dst;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  ob_charset_assert(src != dst || cs->casedn_multiply == 1);
  while ((src < srcend) &&
         (srcres = ob_mb_wc_utf8mb4(cs, &w_char,
                                   (unsigned char*) src, (unsigned char*) srcend)) > 0) {
    ob_tolower_utf8mb4(uni_plane, &w_char);
    if ((dstres = ob_wc_mb_utf8mb4(cs, w_char, (unsigned char*) dst, (unsigned char*) dstend)) <= 0) {
      break;
    } else {
      src += srcres;
      dst += dstres;
    }
  }
  return (size_t) (dst - dst0);
}


static inline int bincmp_utf8mb4(const unsigned char *src, const unsigned char *se,
                                 const unsigned char *dst, const unsigned char *te)
{
  int srclen = (int) (se - src), dstlen= (int) (te - dst);
  int len = srclen < dstlen ? srclen : dstlen;
  int cmp = memcmp(src, dst, len);
  return cmp ? cmp : srclen - dstlen;
}

static inline void ob_tosort_unicode(ObUnicaseInfo *uni_plane, ob_wc_t *wc, unsigned int flags)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page;
    if (OB_NOT_NULL(page = uni_plane->page[*wc >> 8])) {
      *wc= (flags & OB_CS_LOWER_SORT) ?
           page[*wc & 0xFF].tolower :
           page[*wc & 0xFF].sort;
    }
  } else {
    *wc = OB_CS_REPLACEMENT_CHARACTER;
  }
}

static int ob_strnncoll_utf8mb4(const ObCharsetInfo *cs,
                     const unsigned char *src, size_t srclen,
                     const unsigned char *dst, size_t dstlen,
                     bool t_is_prefix)
{
  ob_wc_t src_wc = 0, dst_wc = 0;
  const unsigned char *se = src + srclen;
  const unsigned char *te = dst + dstlen;
  ObUnicaseInfo *uni_plane = cs->caseinfo;
  while ( src < se && dst < te ) {
    int s_res = ob_mb_wc_utf8mb4(cs, &src_wc, src, se);
    int t_res = ob_mb_wc_utf8mb4(cs, &dst_wc, dst, te);
    if ( s_res <= 0 || t_res <= 0 ) {
      return bincmp_utf8mb4(src, se, dst, te);
    }
    ob_tosort_unicode(uni_plane, &src_wc, cs->state);
    ob_tosort_unicode(uni_plane, &dst_wc, cs->state);
    if ( src_wc != dst_wc ) {
      return src_wc > dst_wc ? 1 : -1;
    } else {
      src += s_res;
      dst += t_res;
    }
  }
  return (int) (t_is_prefix ? (dst - te) : ((se - src) - (te - dst)));
}

int __attribute__ ((noinline))  ob_strnncollsp_utf8mb4_help(
    const unsigned char **s_, size_t srclen,
    const unsigned char **t_, size_t dstlen,
    const unsigned char **se_, const unsigned char **te_,
    bool diff_if_only_endspace_difference, int *has_returned, int *res_)
{
  const unsigned char *src = *s_;
  const unsigned char *dst = *t_;
  const unsigned char *se = *se_;
  const unsigned char *te = *te_;
  int res = *res_;
  *has_returned = 0;
  int swap= 1;
  if (srclen != dstlen) {
    if (diff_if_only_endspace_difference) {
      res= 1;
    }
    if (srclen < dstlen) {
      srclen= dstlen;
      src= dst;
      se= te;
      swap= -1;
      res= -res;
    }
    while (src < se) {
      if (*src != ' ') {
        *has_returned = 1;
        break;
      }
      src++;
    }
  }
  *s_ = src;
  *t_ = dst;
  *se_ = se;
  *te_ = te;
  *res_ = res;
  if (*has_returned == 1) {
    return (!diff_if_only_endspace_difference && (*src < ' ')) ? -swap : swap;
  }
  return 0;
}

static int ob_strnncollsp_utf8mb4(const ObCharsetInfo *cs,
                       const unsigned char *src, size_t srclen,
                       const unsigned char *dst, size_t dstlen,
                       bool diff_if_only_endspace_difference)
{
  int res;
  ob_wc_t src_wc = 0, dst_wc = 0;
  const unsigned char *se= src + srclen, *te= dst + dstlen;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  while ( src < se && dst < te ) {
    int s_res= ob_mb_wc_utf8mb4(cs, &src_wc, src, se);
    int t_res= ob_mb_wc_utf8mb4(cs, &dst_wc, dst, te);
    if ( s_res <= 0 || t_res <= 0 ) {
      return bincmp_utf8mb4(src, se, dst, te);
    }
    ob_tosort_unicode(uni_plane, &src_wc, cs->state);
    ob_tosort_unicode(uni_plane, &dst_wc, cs->state);
    if ( src_wc != dst_wc ) {
      return src_wc > dst_wc ? 1 : -1;
    } else {
      src +=s_res;
      dst +=t_res;
    }
  }
  srclen = (size_t) (se-src);
  dstlen = (size_t) (te-dst);
  res= 0;
  int has_returned = 0;
  int tmp = ob_strnncollsp_utf8mb4_help(
      &src, srclen,
      &dst, dstlen,
      &se, &te,
      diff_if_only_endspace_difference, &has_returned, &res);
  return has_returned ? tmp : res;
}

static size_t ob_strxfrm_pad_nweights_unicode(unsigned char *str, unsigned char *strend, size_t nweights)
{
  ob_charset_assert(str && str <= strend);
  unsigned char *str0 = str;
  for (; str < strend && nweights; nweights--) {
    *str++= 0x00;
    if (str < strend) {
      *str++= 0x20;
    }
  }
  return str - str0;
}

static size_t ob_strxfrm_pad_unicode(unsigned char *str, unsigned char *strend)
{
  unsigned char *str0= str;
  ob_charset_assert(str && str <= strend);
  while (str < strend) {
    *str++= 0x00;
    if (str < strend) {
      *str++= 0x20;
    }
  }
  return str - str0;
}

void ob_strnxfrm_unicode_help(uchar **dst,
                              uchar **de,
                              uint nweights,
                              uint flags,
                              uchar **dst0)
{
  if (*dst < *de && nweights && (flags & OB_STRXFRM_PAD_WITH_SPACE))
    *dst += ob_strxfrm_pad_nweights_unicode(*dst, *de, nweights);
  ob_strxfrm_desc_and_reverse(*dst0, *dst, flags, 0);
  if ((flags & OB_STRXFRM_PAD_TO_MAXLEN) && *dst < *de)
    *dst += ob_strxfrm_pad_unicode(*dst, *de);
}
size_t ob_strnxfrm_unicode(const ObCharsetInfo *cs,
                    uchar *dst, size_t dstlen, uint nweights,
                    const uchar *src, size_t srclen, uint flags, bool *is_valid_unicode)
{
  ob_wc_t wc;
  int res;
  uchar *dst0= dst;
  uchar *de= dst + dstlen;
  const uchar *se= src + srclen;
  ObUnicaseInfo *uni_plane= (cs->state & OB_CS_BINSORT) ? NULL : cs->caseinfo;
  wc = 0;
  ob_charset_assert(src);
  *is_valid_unicode = 1;
  for (; dst < de && nweights; nweights--)
  {
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    if (uni_plane)
      ob_tosort_unicode(uni_plane, &wc, cs->state);
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
  }
  ob_strnxfrm_unicode_help(&dst,&de, nweights, flags, &dst0);
  return dst - dst0;
}

size_t ob_varlen_encoding_for_memcmp(const struct ObCharsetInfo* cs,
                              uchar* dst, size_t dst_len, uint nweights,
                              const uchar *src, size_t src_len,
                              bool *is_valid_unicode, bool is_sort)
{
  ob_wc_t wc;
  int res;
  uchar *dst0= dst;
  uchar *de= dst + dst_len;
  const uchar *se= src + src_len;
  ObUnicaseInfo *uni_plane= (cs->state & OB_CS_BINSORT) ? NULL : cs->caseinfo;
  wc = 0;
  ob_charset_assert(src);
  *is_valid_unicode = 1;
  for (;*is_valid_unicode && src < se && dst < de && nweights; nweights--)
  {
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    // if is 0x000000
    if (wc == 0x00000000) {
      res = cs->cset->wc_mb(cs, wc, dst, de);
      dst += res;
      wc = 0x00000001;
    }
    if (1 == is_sort && NULL != uni_plane)
      ob_tosort_unicode(uni_plane, &wc, cs->state);
    // replace code above with code below.
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
  }
  // adds 0x00, 0x00
  *dst = 0x00;
  *(dst+1) = 0x00;
  dst += 2;
  //ob_strnxfrm_unicode_help(&dst,&de, nweights, flags, &dst0);
  return dst - dst0;
}

uint16_t find_space_char_count(const uchar* src, const uchar* se)
{
  int space_cnt = 1;
  while ((src + space_cnt) < se && *(src + space_cnt) == 0x20) space_cnt++;
  if ((src + space_cnt) < se) return space_cnt;
  else return 0;
}

size_t ob_varlen_encoding_for_spacecmp(const struct ObCharsetInfo* cs,
                              uchar* dst, size_t dst_len, uint nweights,
                              const uchar *src, size_t src_len,
                              bool *is_valid_unicode, bool is_sort)
{
  ob_wc_t wc;
  int res;
  uchar *dst0= dst;
  uchar *de= dst + dst_len;
  const uchar *se= src + src_len;
  ObUnicaseInfo *uni_plane= (cs->state & OB_CS_BINSORT) ? NULL : cs->caseinfo;
  wc = 0;
  ob_charset_assert(src);
  *is_valid_unicode = 1;
  uint16_t space_cnt = 0xFFFF;
  for (;*is_valid_unicode && src < se && dst < de && nweights; nweights--)
  {
    // for reslovable multiple bytes, only space's first byte is 0x20,
    // in utf8 encoding scheme.
    if (*src == 0x20) {
      space_cnt = find_space_char_count(src, se);
      if (space_cnt == 0) break;
      *(dst) = 0x20;
      if (*(src+space_cnt) > 0x20){
        *(dst+1)=0x21;
        // flip
        uint16_t tmp_cnt = space_cnt ^ 0xFFFF;
        *(dst+2)=*((uchar*)&tmp_cnt+1);
        *(dst+3)=*(&tmp_cnt);
      } else {
        *(dst+1) = 0x19;
        *(dst+2)=*((uchar*)&space_cnt+1);
        *(dst+3)=*(&space_cnt);
      }
      dst += 4;
      src += space_cnt;
    }
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    if (1 == is_sort && NULL != uni_plane)
      ob_tosort_unicode(uni_plane, &wc, cs->state);
    // replace code above with code below.
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
    //abort();
  }
  // adds 0x20, 0x20
  *dst = 0x20;
  *(dst+1) = 0x20;
  dst += 2;
  //ob_strnxfrm_unicode_help(&dst,&de, nweights, flags, &dst0);
  return dst - dst0;
}
size_t ob_strnxfrm_unicode_varlen(const struct ObCharsetInfo* cs,
                             uchar* dst, size_t dst_len, uint nweights,
                             const uchar *src, size_t srclen,
                             bool is_memcmp, bool *is_valid_unicode)
{
  if (is_memcmp) {
    return ob_varlen_encoding_for_memcmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 1);
  } else {
    return ob_varlen_encoding_for_spacecmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 1);
  }
}

static int ob_wildcmp_unicode_impl_help(const ObCharsetInfo *cs,
                        const char **str_,const char **str_end_,
                        ob_charset_conv_mb_wc mb_wc,
                        const char **wild_str_, const char **wild_end_,
                        int escape_char, int w_one, int w_many,
                        ob_wc_t *s_wc_,
                        ob_wc_t *w_wc_,
                        int *scan_,
                        int *result_,
                        ObUnicaseInfo *weights,
                        int *has_returned)
{
   int ret = 0;
   const char *str = *str_;
   const char *str_end = *str_end_;
   const char *wild_str = *wild_str_;
   const char *wild_end = *wild_end_;
   ob_wc_t src_wc = *s_wc_;
   ob_wc_t w_wc = *w_wc_;
   int scan = *scan_;
   int result = *result_;
   *has_returned = 0;
   while (1) {
     bool escaped= 0;
     if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                      (const unsigned char*)wild_end)) <= 0) {
       ret = 1;
       *has_returned = 1;
       break;
     } else if (w_wc != (ob_wc_t) escape_char && w_wc == (ob_wc_t) w_many) {
       result = 1;
       break;
     }
     wild_str += scan;
     if (w_wc ==  (ob_wc_t) escape_char && wild_str < wild_end) {
       if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                        (const unsigned char*)wild_end)) <= 0) {
         ret = 1;
         *has_returned = 1;
         break;
       } else {
          wild_str += scan;
       escaped = 1;
     }
     }
     if ((scan = mb_wc(cs, &src_wc, (const unsigned char*)str,
                      (const unsigned char*)str_end)) <= 0) {
       ret = 1;
       *has_returned = 1;
       break;
     } else {
     str += scan;
     }

     if (!escaped && w_wc == (ob_wc_t) w_one) {
       result = 1;
     } else {
       if (weights) {
         ob_tosort_unicode(weights, &src_wc, cs->state);
         ob_tosort_unicode(weights, &w_wc, cs->state);
       }
       if (src_wc != w_wc) {
         ret = 1;
         *has_returned = 1;
         break;
     }
     }
     if (wild_str == wild_end) {
       ret = (str != str_end);
       *has_returned = 1;
       break;
     }
   }
   *str_ = str;
   *str_end_ = str_end;
   *wild_str_ = wild_str;
   *wild_end_ = wild_end;
   *s_wc_ = src_wc;
   *w_wc_ = w_wc;
   *scan_ = scan;
   *result_ = result;
   return ret;
}

static int ob_wildcmp_unicode_impl(const ObCharsetInfo *cs,
                        const char *str,const char *str_end,
                        const char *wild_str,const char *wild_end,
                        int escape_char, int w_one, int w_many,
                        ObUnicaseInfo *weights, int recurse_level)
{
  int result= -1;
  ob_wc_t src_wc = 0;
  ob_wc_t w_wc = 0;
  int scan = 0;
  int (*mb_wc)(const ObCharsetInfo *, ob_wc_t *, const unsigned char *, const unsigned char *);
  mb_wc= cs->cset->mb_wc;

  while (wild_str != wild_end) {
    int has_returned = 0;
    int tmp = ob_wildcmp_unicode_impl_help(cs,
        &str,&str_end,
        mb_wc,
        &wild_str, &wild_end,
        escape_char,
        w_one,
        w_many,
        &src_wc,
        &w_wc,
        &scan,
        &result,
        weights,
        &has_returned);
    if (has_returned == 1) {
      return tmp;
    } else if (w_wc == (ob_wc_t) w_many) {
      while (wild_str != wild_end) {
        if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                         (const unsigned char*)wild_end)) <= 0) {
          return 1;
        } else if (w_wc == (ob_wc_t)w_many) {
          wild_str+= scan;
          continue;
        } else if (w_wc == (ob_wc_t)w_one) {
          wild_str+= scan;
          if ((scan= mb_wc(cs, &src_wc, (const unsigned char*)str,
                           (const unsigned char*)str_end)) <=0) {
            return 1;
          } else {
          str+= scan;
          continue;
        }
        }
        break;
      }

      if (wild_str == wild_end) {
        return 0;
      } else if (str == str_end) {
        return -1;
      } else if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                       (const unsigned char*)wild_end)) <=0) {
        return 1;
      } else {
        wild_str+= scan;
      }

      if (w_wc ==  (ob_wc_t)escape_char) {
        if (wild_str < wild_end) {
          if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                           (const unsigned char*)wild_end)) <=0) {
            return 1;
          } else {
            wild_str+= scan;
          }
        }
      }

      while (1) {
        while (str != str_end) {
          if ((scan= mb_wc(cs, &src_wc, (const unsigned char*)str,
                           (const unsigned char*)str_end)) <=0) {
            return 1;
          } else if (weights) {
            ob_tosort_unicode(weights, &src_wc, cs->state);
            ob_tosort_unicode(weights, &w_wc, cs->state);
          }
          if (src_wc == w_wc) {
            break;
          } else {
          str+= scan;
        }
        }
        if (str == str_end) {
          return -1;
        } else {
        str+= scan;
        }
        result= ob_wildcmp_unicode_impl(cs, str, str_end, wild_str, wild_end,
                                        escape_char, w_one, w_many,
                                        weights, recurse_level + 1);
        if (result <= 0) return result;
      }
    }
  }
  return (str != str_end ? 1 : 0);
}

int ob_wildcmp_unicode(const ObCharsetInfo *cs,
                   const char *str,const char *str_end,
                   const char *wild_str,const char *wild_end,
                   int escape_char, int w_one, int w_many,
                   ObUnicaseInfo *weights)
{
  return ob_wildcmp_unicode_impl(cs, str, str_end, wild_str, wild_end, escape_char, w_one, w_many, weights, 1);
}

static int ob_wildcmp_utf8mb4(const ObCharsetInfo *cs,
                   const char *str, const char *strend,
                   const char *wild_str, const char *wild_end,
                   int escape_char, int w_one, int w_many)
{
  return ob_wildcmp_unicode(cs, str, strend, wild_str, wild_end, escape_char, w_one, w_many, cs->caseinfo);
}



static inline void ob_hash_add(unsigned long int *n1, unsigned long int *n2, unsigned int ch)
{
  n1[0]^= (((n1[0] & 63) + n2[0]) * (ch)) + (n1[0] << 8);
  n2[0]+= 3;
}


static void ob_hash_sort_utf8mb4(const ObCharsetInfo *cs, const unsigned char *src, size_t srclen,
               unsigned long int *n1, unsigned long int *n2, const bool calc_end_space, hash_algo hash_algo)
{
  ob_wc_t wc;
  int res;
  const unsigned char *end= src + srclen;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  int length = 0;
  unsigned char data[HASH_BUFFER_LENGTH];
  if (!calc_end_space) {
    while (end > src && end[-1] == ' ')
      end--;
  }

  if (NULL == hash_algo) {
    while ((res= ob_mb_wc_utf8mb4(cs, &wc, (unsigned char*) src, (unsigned char*) end)) > 0) {
      ob_tosort_unicode(uni_plane, &wc, cs->state);
      ob_hash_add(n1, n2, (unsigned int) (wc & 0xFF));
      ob_hash_add(n1, n2, (unsigned int) (wc >> 8)  & 0xFF);
      if (wc > 0xFFFF) {
        ob_hash_add(n1, n2, (unsigned int) (wc >> 16) & 0xFF);
      }
      src+= res;
    }
  } else {
    while ((res= ob_mb_wc_utf8mb4(cs, &wc, (unsigned char*) src, (unsigned char*) end)) > 0) {
      ob_tosort_unicode(uni_plane, &wc, cs->state);
      if (length > HASH_BUFFER_LENGTH - 2 || (HASH_BUFFER_LENGTH - 2 == length && wc > 0xFFFF)) {
        *n1 = hash_algo((void*) &data, length, *n1);
        length = 0;
      }
      data[length++] = (unsigned char)wc;
      data[length++] = (unsigned char)(wc >> 8);
      if (wc > 0xFFFF) {
        data[length++] = (unsigned char)(wc >> 16);
      }
      src+= res;
    }
    if (length > 0) {
      *n1 = hash_algo((void*) &data, length, *n1);
    }
  }
}

size_t ob_strnxfrm_unicode_full_bin(const ObCharsetInfo *cs,
                             unsigned char *dst, size_t dstlen, unsigned int nweights,
                             const unsigned char *src, size_t srclen, unsigned int flags, bool *is_valid_unicode)
{
  ob_wc_t wc;
  unsigned char *dst0= dst;
  unsigned char *de= dst + dstlen;
  const unsigned char *se = src + srclen;
  wc = 0;
  ob_charset_assert(src);
  ob_charset_assert(cs->state & OB_CS_BINSORT);
  *is_valid_unicode = 1;
  for ( ; dst < de && nweights; nweights--) {
    int res;
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
  }
  if (flags & OB_STRXFRM_PAD_WITH_SPACE)
  {
    for ( ; dst < de && nweights; nweights--)
    {
      *dst++= 0x00;
      if (dst < de)
      {
        *dst++= 0x00;
        if (dst < de)
          *dst++= 0x20;
      }
    }
  }
  ob_strxfrm_desc_and_reverse(dst0, dst, flags, 0);
  if (flags & OB_STRXFRM_PAD_TO_MAXLEN)
  {
    while (dst < de)
    {
      *dst++= 0x00;
      if (dst < de)
      {
        *dst++= 0x00;
        if (dst < de)
          *dst++= 0x20;
      }
    }
  }
  return dst - dst0;
}

size_t ob_strnxfrm_unicode_full_bin_varlen(const struct ObCharsetInfo* cs,
                             uchar* dst, size_t dst_len, uint nweights,
                             const uchar *src, size_t srclen,
                             bool is_memcmp, bool *is_valid_unicode)
{
  if (is_memcmp) {
    return ob_varlen_encoding_for_memcmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 0);
  } else {
    return ob_varlen_encoding_for_spacecmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 0);
  }
}

size_t ob_strnxfrmlen_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)), size_t len)
{
  return (len * 2 + 2) / 4;
}

size_t ob_strnxfrmlen_unicode_full_bin(const ObCharsetInfo *cs, size_t len)
{
  return ((len + 3) / cs->mbmaxlen) * 3;
}

int ob_mb_wc_utf8mb4_thunk(const ObCharsetInfo *cs __attribute__((unused)),
                           ob_wc_t *pwc, const uchar *s, const uchar *e) {
  return ob_mb_wc_utf8mb4(cs, pwc, s, e);
}


ObCharsetHandler ob_charset_utf8mb4_handler=
{
  ob_ismbchar_utf8mb4,
  ob_mbcharlen_utf8mb4,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_utf8mb4,
  ob_lengthsp_8bit,
  ob_mb_wc_utf8mb4,
  ob_wc_mb_utf8mb4,
  ob_mb_ctype_mb,
  ob_caseup_utf8mb4,
  ob_casedn_utf8mb4,
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

static ObCollationHandler ob_collation_utf8mb4_general_ci_handler=
{
  NULL,
  NULL,
  ob_strnncoll_utf8mb4,
  ob_strnncollsp_utf8mb4,
  ob_strnxfrm_unicode,
  ob_strnxfrmlen_utf8mb4,
  ob_strnxfrm_unicode_varlen,
  ob_like_range_mb,
  ob_wildcmp_utf8mb4,
  NULL,
  ob_instr_mb,
  ob_hash_sort_utf8mb4,
  ob_propagate_complex
};

static ObCollationHandler ob_collation_utf8mb4_bin_handler =
{
  NULL,
  NULL,
  ob_strnncoll_mb_bin,
  ob_strnncollsp_mb_bin,
  ob_strnxfrm_unicode_full_bin,
  ob_strnxfrmlen_unicode_full_bin,
  NULL,
  ob_like_range_mb,
  ob_wildcmp_mb_bin,
  NULL,
  ob_instr_mb,
  ob_hash_sort_mb_bin,
  ob_propagate_simple
};

ObCharsetInfo ob_charset_utf8mb4_general_ci=
{
  45,0,0,
  OB_CS_COMPILED|OB_CS_PRIMARY|OB_CS_STRNXFRM|OB_CS_UNICODE|OB_CS_UNICODE_SUPPLEMENT|OB_CS_CI,
  OB_UTF8MB4,
  OB_UTF8MB4_GENERAL_CI,
  "UTF-8 Unicode",
  NULL,
  NULL,
  ctype_utf8mb4,
  to_lower_utf8mb4,
  to_upper_utf8mb4,
  to_upper_utf8mb4,
  NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  1,
  1,
  1,
  1,
  4,
  1,
  0,
  0xFFFF,
  ' ',
  0,
  1,
  1,
  &ob_charset_utf8mb4_handler,
  &ob_collation_utf8mb4_general_ci_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_utf8mb4_bin=
{
  46,0,0,
  OB_CS_COMPILED|OB_CS_BINSORT|OB_CS_STRNXFRM|
  OB_CS_UNICODE|OB_CS_UNICODE_SUPPLEMENT,
  OB_UTF8MB4,
  OB_UTF8MB4_BIN,
  "UTF-8 Unicode",
  NULL,
  NULL,
  ctype_utf8mb4,
  to_lower_utf8mb4,
  to_upper_utf8mb4,
  NULL,
  NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  1,
  1,
  1,
  1,
  4,
  1,
  0,
  0xFFFF,
  ' ',
  0,
  1,
  1,
  &ob_charset_utf8mb4_handler,
  &ob_collation_utf8mb4_bin_handler,
  PAD_SPACE
};
