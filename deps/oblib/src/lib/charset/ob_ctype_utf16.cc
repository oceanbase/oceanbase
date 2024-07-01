
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
#include "lib/charset/str_uca_type.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/charset/ob_template_helper.h"

#define OB_UTF16_HIGH_HEAD(x)  ((((uchar) (x)) & 0xFC) == 0xD8)
#define OB_UTF16_LOW_HEAD(x)   ((((uchar) (x)) & 0xFC) == 0xDC)
#define OB_UTF16_SURROGATE(x)  (((x) & 0xF800) == 0xD800)

#define OB_UTF16_WC2(a, b)       ((a << 8) + b)

static inline int
ob_bincmp(const unsigned char *str, const unsigned char *se,
          const unsigned char *t, const unsigned char *te)
{
  int s_len= (int) (se - str), t_len= (int) (te - t);
  int len= OB_MIN(s_len, t_len);
  int cmp= memcmp(str, t, len);
  return cmp ? cmp : s_len - t_len;
}

static inline void
ob_tosort_utf16(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page;
    if ((page = uni_plane->page[*wc >> 8])) {
      *wc= page[*wc & 0xFF].sort;
    }
  } else {
    *wc= OB_CS_REPLACEMENT_CHARACTER;
  }
}

static unsigned int ob_mbcharlen_utf16(const ObCharsetInfo *cs  __attribute__((unused)),
                   unsigned int c __attribute__((unused)))
{
  ob_charset_assert(0);
  return OB_UTF16_HIGH_HEAD(c) ? 4 : 2;
}

static unsigned int ob_ismbchar_utf16(const ObCharsetInfo *cs, const char *begin, const char *end)
{
  ob_wc_t wc;
  int res= cs->cset->mb_wc(cs, &wc, (const unsigned char *) begin, (const unsigned char *) end);
  return (unsigned int) (res > 0 ? res : 0);
}

static size_t ob_numchars_utf16(const ObCharsetInfo *cs,
                  const char *begin, const char *end)
{
  size_t nchars= 0;
  size_t char_len= ob_ismbchar_utf16(cs, begin, end);
  while (char_len != 0) {
    begin+= char_len;
    nchars++;
    char_len= ob_ismbchar_utf16(cs, begin, end);
  }
  return nchars;
}

static size_t
ob_charpos_utf16(const ObCharsetInfo *cs,
                 const char *begin, const char *end, size_t pos)
{
  const char *b0= begin;
  unsigned int char_len;
  while (pos) {
    if (!(char_len= ob_ismbchar(cs, begin, end))) {
      return (end + 2 - b0); 
    } else {
      begin += char_len;
      pos--;
    }
  }
  return (size_t) (pos ? (end + 2 - b0) : (begin - b0));
}

static size_t
ob_well_formed_len_utf16(const ObCharsetInfo *cs,
                         const char *begin, const char *end,
                         size_t nchars, int *error)
{
  const char *b0= begin;
  unsigned int char_len;
  *error= 0;
  while (nchars) {
    if (!(char_len = cs->cset->ismbchar(cs, begin, end))) {
      *error= begin < end ? 1 : 0;
      break;
    } else {
      begin += char_len;
      nchars--;
    }
  }
  return (size_t) (begin - b0);
}

static size_t
ob_lengthsp_mb2(const ObCharsetInfo *cs __attribute__((unused)),
                const char *ptr, size_t length)
{
  const char *end= ptr + length;
  while (end - ptr > 1 && end[-1] == ' ' && end[-2] == '\0')
    end-= 2;
  return (size_t) (end - ptr);
}

#define OB_UTF16_WC4(a, begin, c, d) (((a & 3) << 18) + (begin << 10) + \
                                  ((c & 3) << 8) + d + 0x10000)

static int
ob_utf16_uni(const ObCharsetInfo *cs __attribute__((unused)),
             ob_wc_t *pwc, const unsigned char *str, const unsigned char *end)
{
  if (2 > end - str) {
    return OB_CS_TOOSMALL2;
  } else if (OB_UTF16_HIGH_HEAD(*str))  {
    if (4 > end - str) {
      return OB_CS_TOOSMALL4;
    } else if (!OB_UTF16_LOW_HEAD(str[2]))  {
      return OB_CS_ILSEQ;
    } else {
      *pwc= OB_UTF16_WC4(str[0], str[1], str[2], str[3]);
      return 4;
  }
  } else if (OB_UTF16_LOW_HEAD(*str)) {
    return OB_CS_ILSEQ;
  } else {
    *pwc= OB_UTF16_WC2(str[0], str[1]);
  }
  return 2;
}

static int
ob_uni_utf16(const ObCharsetInfo *cs __attribute__((unused)),
             ob_wc_t wc, unsigned char *str, unsigned char *end)
{
  if (wc <= 0xFFFF) {
    if (2 > end - str) {
      return OB_CS_TOOSMALL2;
    } else if (OB_UTF16_SURROGATE(wc)) {
      return OB_CS_ILUNI;
    } else {
      *str++= (unsigned char) (wc >> 8);
      *str= (unsigned char) (wc & 0xFF);
      return 2;
  }
  } else if (wc <= 0x10FFFF) {
    if (4 > end - str) {
      return OB_CS_TOOSMALL4;
    } else {
      *str++= (unsigned char) ((wc-= 0x10000) >> 18) | 0xD8;
      *str++= (unsigned char) (wc >> 10) & 0xFF;
      *str++= (unsigned char) ((wc >> 8) & 3) | 0xDC;
      *str= (unsigned char) wc & 0xFF;
      return 4;
    }
  }

  return OB_CS_ILUNI;
}

static inline void
ob_toupper_utf16(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  const ObUnicaseInfoChar *page;
  if ((*wc <= uni_plane->maxchar) && (page= uni_plane->page[*wc >> 8]))
    *wc= page[*wc & 0xFF].toupper;
}

static inline void
ob_tolower_utf16(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  const ObUnicaseInfoChar *page;
  if ((*wc <= uni_plane->maxchar) && (page= uni_plane->page[*wc >> 8])) {
    *wc= page[*wc & 0xFF].tolower;
  }
  return;
}

static size_t
ob_caseup_utf16(const ObCharsetInfo *cs, char *src, size_t srclen,
                char *dst __attribute__((unused)),
                size_t dst_len __attribute__((unused)))
{
  ob_wc_t wc;
  int res;
  char *srcend= src + srclen;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  ob_charset_assert(src == dst && srclen == dst_len);

  while ((src < srcend) &&
         (res= cs->cset->mb_wc(cs, &wc, (unsigned char *) src, (unsigned char *) srcend)) > 0) {
    ob_toupper_utf16(uni_plane, &wc);
    if (res != cs->cset->wc_mb(cs, wc, (unsigned char *) src, (unsigned char *) srcend)) {
      break;
    } else {
      src+= res;
    }
  }
  return srclen;
}

static size_t
ob_casedn_utf16(const ObCharsetInfo *cs, char *src, size_t srclen,
                char *dst __attribute__((unused)),
                size_t dst_len __attribute__((unused)))
{
  ob_wc_t wc;
  int res;
  char *srcend= src + srclen;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  ob_charset_assert(src == dst && srclen == dst_len);

  while ((src < srcend) &&
         (res= cs->cset->mb_wc(cs, &wc, (unsigned char *) src, (unsigned char *) srcend)) > 0) {
    ob_tolower_utf16(uni_plane, &wc);
    if (res != cs->cset->wc_mb(cs, wc, (unsigned char *) src, (unsigned char *) srcend))
      break;
    src+= res;
  }
  return srclen;
}

static void
ob_fill_mb2(const ObCharsetInfo *cs, char *str, size_t s_len, int fill)
{
  char buf[10];
  int buf_len;

  ob_charset_assert((s_len % 2) == 0);

  buf_len= cs->cset->wc_mb(cs, (ob_wc_t) fill, (unsigned char*) buf,
                          (unsigned char*) buf + sizeof(buf));

  ob_charset_assert(buf_len > 0);

  while (s_len >= (size_t) buf_len) {
    memcpy(str, buf, (size_t) buf_len);
    str+= buf_len;
    s_len-= buf_len;
  }

  while (s_len) {
    *str++= 0x00;
    s_len--;
  }
}

static long
ob_strntol_mb2_or_mb4(const ObCharsetInfo *cs,
                      const char *nptr, size_t l, int base,
                      char **end_ptr, int *err)
{
  int      negative= 0;
  int      overflow;
  int      cnv;
  ob_wc_t  wc;
  unsigned int cut_lim;
  uint32_t cut_off;
  uint32_t res;
  const unsigned char *str= (const unsigned char*) nptr;
  const unsigned char *end= (const unsigned char*) nptr+l;
  const unsigned char *save;

  *err=0;
  while (TRUE) {
    cnv = cs->cset->mb_wc(cs, &wc, str, end);
    if (cnv > 0) {
      if (wc == ' ') {
        //do nothing
      } else if (wc == '\t') {
        //do nothing
      } else if (wc == '-') {
        negative = !negative;
      } else if (wc == '+') {
        //do nothing
      } else {
        break;
      }
    } else  {
      if (end_ptr != NULL) *end_ptr= (char*) str;
      err[0]= (cnv==OB_CS_ILSEQ) ? EILSEQ : EDOM;
      return 0;
    }
    str+= cnv;
  }

  res = 0;
  overflow = 0;
  cut_off = ((uint32_t)~0L) / (uint32_t) base;
  cut_lim = (unsigned int) (((uint32_t)~0L) % (uint32_t) base);
  save = str;

  do {
    cnv= cs->cset->mb_wc(cs, &wc, str, end);
    if ( cnv > 0) {
      str += cnv;
      if (wc >= '0' && wc <= '9') {
        wc-= '0';
      } else if (wc >= 'A' && wc <= 'Z') {
        wc= wc - 'A' + 10;
      } else if (wc >= 'a' && wc <= 'z') {
        wc= wc - 'a' + 10;
      } else {
        break;
      }
      if ((int)wc >= base) {
        break;
      } else if ((res == cut_off && wc > cut_lim) || res > cut_off) {
        overflow= 1;
      } else {
        res*= (uint32_t) base;
        res+= wc;
      }
    } else if (cnv == OB_CS_ILSEQ) {
      if (NULL != end_ptr) {
        *end_ptr = (char*) str;
      }
      err[0]= EILSEQ;
      return 0;
    } else {
      break;
    }
  } while(TRUE);

  if (end_ptr != NULL) {
    *end_ptr = (char *) str;
  }
  if (str == save) {
    err[0]= EDOM;
    return 0L;
  } else if (negative) {
    if (res > (uint32_t) INT_MIN32)
      overflow= 1;
  } else if (res > INT_MAX32) {
    overflow= 1;
  }

  if (overflow) {
    err[0]= ERANGE;
    return negative ? INT_MIN32 : INT_MAX32;
  }

  return (negative ? -((long) res) : (long) res);
}

static unsigned long int
ob_strntoul_mb2_or_mb4(const ObCharsetInfo *cs,
                       const char *nptr, size_t l, int base,
                       char **end_ptr, int *err)
{
  int      negative= 0;
  int      overflow;
  int      cnv;
  ob_wc_t  wc;
  unsigned int cut_lim;
  uint32_t cut_off;
  uint32_t res;
  const unsigned char *str= (const unsigned char*) nptr;
  const unsigned char *end= (const unsigned char*) nptr + l;
  const unsigned char *save;

  *err= 0;
   while (TRUE) {
    cnv= cs->cset->mb_wc(cs, &wc, str, end);
    if (cnv > 0) {
      if (wc == ' ') {
        //do nothing
      } else if (wc == '\t') {
        //do nothing
      } else if (wc == '-') {
        negative = !negative;
      } else if (wc == '+') {
        //do nothing
      } else {
        break;
      }
    } else  {
      if (NULL != end_ptr) {
        *end_ptr= (char*)str;
      }
      err[0]= (cnv == OB_CS_ILSEQ) ? EILSEQ : EDOM;
      return 0;
    }
    str+= cnv;
  }

  overflow= 0;
  res= 0;
  save= str;
  cut_off= ((uint32_t)~0L) / (uint32_t) base;
  cut_lim= (unsigned int) (((uint32_t)~0L) % (uint32_t) base);

  while (TRUE) {
    cnv= cs->cset->mb_wc(cs, &wc, str, end);
    if (cnv > 0) {
      str+= cnv;
      if (wc >= '0' && wc <= '9') {
        wc-= '0';
      } else if (wc >= 'A' && wc <= 'Z') {
        wc= wc - 'A' + 10;
      } else if (wc >= 'a' && wc <= 'z') {
        wc= wc - 'a' + 10;
      } else {
        break;
      }
      if ((int) base <= wc ) {
        break;
      } else if ((res == cut_off && wc > cut_lim) || res > cut_off) {
        overflow = 1;
      } else {
        res*= (uint32_t) base;
        res+= wc;
      }
    } else if (cnv == OB_CS_ILSEQ) {
      if (end_ptr != NULL ) {
        *end_ptr= (char*)str;
      }
      err[0]= EILSEQ;
      return 0;
    } else {
      break;
    }
  }

  if (end_ptr != NULL)
    *end_ptr= (char *) str;

  if (str == save) {
    err[0]= EDOM;
    return 0L;
  } else if (overflow) {
    err[0]= (ERANGE);
    return (~(uint32_t) 0);
  }

  return (negative ? -((long) res) : (long) res);
}

static longlong
ob_strntoll_mb2_or_mb4(const ObCharsetInfo *cs,
                       const char *nptr, size_t l, int base,
                       char **end_ptr, int *err)
{
  int      negative=0;
  int      overflow;
  int      cnv;
  ob_wc_t  wc;
  uint64_t    cut_off;
  unsigned int cut_lim;
  uint64_t    res;
  const unsigned char *str= (const unsigned char*) nptr;
  const unsigned char *end= (const unsigned char*) nptr+l;
  const unsigned char *save;

  *err= 0;
  while (TRUE) {
    cnv=cs->cset->mb_wc(cs,&wc,str,end);
    if (cnv > 0) {
      if (wc == ' ') {
        //do nothing
      } else if (wc == '\t') {
        //do nothing
      } else if (wc == '-') {
        negative = !negative;
      } else if (wc == '+') {
        //do nothing
      } else {
        break;
      }
    } else {
      if (NULL != end_ptr) {
        *end_ptr = (char*)str;
      }
      err[0] = (cnv == OB_CS_ILSEQ) ? EILSEQ : EDOM;
      return 0;
    }
    str+=cnv;
  }

  overflow = 0;
  res = 0;
  save = str;
  cut_off = (~(uint64_t) 0) / (unsigned long int) base;
  cut_lim = (unsigned int) ((~(uint64_t) 0) % (unsigned long int) base);

  while (TRUE) {
    cnv=cs->cset->mb_wc(cs,&wc,str,end);
    if (cnv > 0) {
      str+=cnv;
      if ( wc>='0' && wc<='9') {
        wc -= '0';
      } else if ( wc>='A' && wc<='Z') {
        wc = wc - 'A' + 10;
      } else if ( wc>='a' && wc<='z') {
        wc = wc - 'a' + 10;
      } else {
        break;
      }
      if ((int)wc >= base) {
        break;
      } else if (res > cut_off || (res == cut_off && wc > cut_lim)) {
        overflow = 1;
      } else {
        res *= (uint64_t) base;
        res += wc;
      }
    } else if (cnv == OB_CS_ILSEQ) {
      if (NULL != end_ptr )
        *end_ptr = (char*)str;
      err[0]=EILSEQ;
      return 0;
    } else {
      break;
    }
  }

  if (end_ptr != NULL) {
    *end_ptr = (char *) str;
  }

  if (str == save) {
    err[0]=EDOM;
    return 0L;
  }

  if (negative) {
    if (res  > (uint64_t) LONGLONG_MIN) {
      overflow = 1;
  }
  } else if (res > (uint64_t) LONGLONG_MAX) {
    overflow = 1;
  }

  if (overflow) {
    err[0]=ERANGE;
    return negative ? LONGLONG_MIN : LONGLONG_MAX;
  }

  return (negative ? -((int64_t)res) : (int64_t)res);
}

static ulonglong
ob_strntoull_mb2_or_mb4(const ObCharsetInfo *cs,
                        const char *nptr, size_t l, int base,
                        char **end_ptr, int *err)
{
  int      negative = 0;
  int      overflow = 0;
  int      cnv;
  ob_wc_t  wc;
  uint64_t    cut_off;
  unsigned int cut_lim;
  uint64_t    res = 0;
  const unsigned char *str= (const unsigned char*) nptr;
  const unsigned char *end= (const unsigned char*) nptr + l;
  const unsigned char *save;

  *err= 0;
  while (TRUE) {
    cnv = cs->cset->mb_wc(cs, &wc, str, end);
    if (cnv > 0) {
      if ( wc>='0' && wc<='9') {
        wc -= '0';
      } else if ( wc>='A' && wc<='Z') {
        wc = wc - 'A' + 10;
      } else if ( wc>='a' && wc<='z') {
        wc = wc - 'a' + 10;
      } else {
        break;
      }
    } else  {
      err[0]= (cnv==OB_CS_ILSEQ) ? EILSEQ : EDOM;
      if (NULL != end_ptr) {
        *end_ptr = (char*)str;
      }
      return 0;
    }
    str+=cnv;
  }

  save = str;
  cut_off = (~(uint64_t) 0) / (unsigned long int) base;
  cut_lim = (unsigned int) ((~(uint64_t) 0) % (unsigned long int) base);

  while(TRUE) {
    cnv=cs->cset->mb_wc(cs,&wc,str,end);
    if ( cnv> 0 ) {
      str+=cnv;
      if ( wc>='0' && wc<='9') {
        wc -= '0';
      } else if ( wc>='A' && wc<='Z') {
        wc = wc - 'A' + 10;
      } else if ( wc>='a' && wc<='z') {
        wc = wc - 'a' + 10;
      } else {
        break;
      }
      if ((int)wc >= base) {
        break;
      } else if (res > cut_off || (res == cut_off && wc > cut_lim)) {
        overflow = 1;
      } else {
        res *= (uint64_t) base;
        res += wc;
      }
    } else if (OB_CS_ILSEQ == cnv) {
      err[0] = EILSEQ;
      if (NULL != end_ptr) {
        *end_ptr = (char*)str;
      }
      return 0;
    } else {
      break;
    }
  } 

  if (NULL != end_ptr) {
    *end_ptr = (char *) str;
  }
  if (str == save) {
    err[0]= EDOM;
    return 0L;
  } else if (overflow) {
    err[0]= ERANGE;
    return (~(uint64_t) 0);
  }
  return (negative ? -((int64_t) res) : (int64_t) res);
}

static double
ob_strntod_mb2_or_mb4(const ObCharsetInfo *cs,
                      char *nptr, size_t length,
                      char **end_ptr, int *err)
{
  char     buf[256];
  double   res;
  char *begin= buf;
  const unsigned char *str= (const unsigned char*) nptr;
  const unsigned char *end;
  ob_wc_t  wc;
  int     cnv;

  *err= 0;
  
  if (length >= sizeof(buf)) {
    length= sizeof(buf) - 1;
  }

  end= str + length;
  cnv= cs->cset->mb_wc(cs,&wc,str,end);
  while (cnv > 0) {
    str+= cnv;
    if (wc > (int) (unsigned char) 'e' || !wc) {
      break;                                    
    } else {    
      *begin++= (char) wc;
      cnv= cs->cset->mb_wc(cs,&wc,str,end);
    }
  }
  *end_ptr = begin;
  res= ob_strtod(buf, end_ptr, err);
  *end_ptr= nptr + cs->mbminlen * (size_t) (*end_ptr - buf);
  return res;
}

static ulonglong
ob_strntoull10rnd_mb2_or_mb4(const ObCharsetInfo *cs,
                             const char *nptr, size_t length,
                             int unsign_fl,
                             char **end_ptr, int *err)
{
  char  buf[256], *begin= buf;
  ob_wc_t  wc;
  int     cnv;
  uint64_t res;
  const unsigned char *end;
  const unsigned char *str= (const unsigned char*) nptr;

  if (length >= sizeof(buf)) {
    length= sizeof(buf)-1;
  }
  end= str + length;
  cnv= cs->cset->mb_wc(cs,&wc,str,end);
  while (cnv > 0) {
    str += cnv;
    if (wc > (int) (unsigned char) 'e' || !wc) {
      break;                            
    } else {
      *begin++= (char) wc;
      cnv= cs->cset->mb_wc(cs,&wc,str,end);
    }
  }

  res= ob_strntoull10rnd_8bit(cs, buf, begin - buf, unsign_fl, end_ptr, err);
  *end_ptr= (char*) nptr + cs->mbminlen * (size_t) (*end_ptr - buf);
  return res;
}

static size_t
ob_scan_mb2(const ObCharsetInfo *cs,
            const char *str, const char *end, int sequence_type)
{
  const char *str0= str;
  ob_wc_t wc;
  int res;

  switch (sequence_type) {
  case OB_SEQ_SPACES:
    for (res= cs->cset->mb_wc(cs, &wc, (const unsigned char *) str, (const unsigned char *) end);
         res > 0 && wc == ' ';
         str+= res, res= cs->cset->mb_wc(cs, &wc, (const unsigned char *) str, (const unsigned char *) end)) {
    }
    return (size_t) (str - str0);
  default:
    return 0;
  }
}

static int
ob_strnncoll_utf16_bin(const ObCharsetInfo *cs,
                       const unsigned char *str, size_t s_len,
                       const unsigned char *t, size_t t_len,
                       bool t_is_prefix)
{
  int s_res,t_res;
  ob_wc_t UNINIT_VAR(s_wc), UNINIT_VAR(t_wc);
  const unsigned char *se=str+s_len;
  const unsigned char *te=t+t_len;

  while ( str < se && t < te ) {
    s_res= cs->cset->mb_wc(cs, &s_wc, str, se);
    t_res= cs->cset->mb_wc(cs, &t_wc, t, te);

    if (s_res <= 0 || t_res <= 0) {
      return ob_bincmp(str, se, t, te);
    } else if (s_wc != t_wc) {
      return s_wc > t_wc ? 1 : -1;
    } else {
      str+= s_res;
      t+= t_res;
    }
  }
  return (int) (t_is_prefix ? (t - te) : ((se - str) - (te - t)));
}

static int
ob_strnncollsp_utf16_bin(const ObCharsetInfo *cs,
                         const unsigned char *str, size_t s_len,
                         const unsigned char *t, size_t t_len,
                         bool diff_if_only_endspace_difference)
{
  int res;
  ob_wc_t UNINIT_VAR(s_wc), UNINIT_VAR(t_wc);
  const unsigned char *se= str + s_len, *te= t + t_len;

  if (!diff_if_only_endspace_difference) {
    ob_charset_assert((s_len % 2) == 0);
    ob_charset_assert((t_len % 2) == 0);
  }

  while (str < se && t < te) {
    int s_res= cs->cset->mb_wc(cs, &s_wc, str, se);
    int t_res= cs->cset->mb_wc(cs, &t_wc, t, te);

    if (s_res <= 0 || t_res <= 0) {
      return ob_bincmp(str, se, t, te);
    } else if (s_wc != t_wc) {
      return s_wc > t_wc ? 1 : -1;
    } else {
      str+= s_res;
      t+= t_res;
    }
  }

  s_len= (size_t) (se - str);
  t_len= (size_t) (te - t);
  res= 0;

  if (s_len != t_len) {
    int s_res, swap= 1;
    if (diff_if_only_endspace_difference) {
      return s_len < t_len ? -1 : 1;
    } else if (s_len < t_len) {
      s_len= t_len;
      str= t;
      se= te;
      swap= -1;
      res= -res;
    }

    while (str < se) {
      if ((s_res= cs->cset->mb_wc(cs, &s_wc, str, se)) < 0) {
        return 0;
      } else if (s_wc != ' ') {
        return (s_wc < ' ') ? -swap : swap;
      } else {
        str += s_res;
      }
    }
  }
  return res;
}

static int
ob_wildcmp_utf16_bin(const ObCharsetInfo *cs,
                     const char *str,const char *str_end,
                     const char *wild_str,const char *wild_end,
                     int escape_char, int w_one, int w_many)
{
  return ob_wildcmp_unicode(cs, str, str_end, wild_str, wild_end,
                            escape_char, w_one, w_many, NULL);
}

static void
ob_hash_sort_utf16_bin(const ObCharsetInfo *cs,
                       const unsigned char *pos, size_t len,
                       unsigned long int *nr1, unsigned long int *nr2,
                       const bool calc_end_space, hash_algo hash_algo)
{
  const unsigned char *end= pos + (calc_end_space ? len : cs->cset->lengthsp(cs, (const char *) pos, len));

  if (NULL == hash_algo) {
    while (pos < end) {
      nr1[0]^=(unsigned long int) ((((unsigned int) nr1[0] & 63)+nr2[0]) *
        ((unsigned int)*pos)) + (nr1[0] << 8);
      nr2[0]+=3;
      pos++;
    }
  } else {
    nr1[0] = hash_algo((void*)pos, (int)(end - pos), nr1[0]);
  }
}

static int
ob_strnncoll_utf16(const ObCharsetInfo *cs,
                   const unsigned char *str, size_t s_len,
                   const unsigned char *t, size_t t_len,
                   bool t_is_prefix)
{
  int s_res, t_res;
  ob_wc_t UNINIT_VAR(s_wc), UNINIT_VAR(t_wc);
  const unsigned char *se= str + s_len;
  const unsigned char *te= t + t_len;
  ObUnicaseInfo *uni_plane= cs->caseinfo;

  while (str < se && t < te) {
    s_res= cs->cset->mb_wc(cs, &s_wc, str, se);
    t_res= cs->cset->mb_wc(cs, &t_wc, t, te);

    if (s_res <= 0 || t_res <= 0) {
      return ob_bincmp(str, se, t, te);
    } else {
      ob_tosort_utf16(uni_plane, &s_wc);
      ob_tosort_utf16(uni_plane, &t_wc);
    }
    if (s_wc != t_wc) {
      return  s_wc > t_wc ? 1 : -1;
    } else {
      str+= s_res;
      t+= t_res;
    }
  }
  return (int) (t_is_prefix ? (t - te) : ((se - str) - (te - t)));
}

static int
ob_strnncollsp_utf16(const ObCharsetInfo *cs,
                     const unsigned char *str, size_t s_len,
                     const unsigned char *t, size_t t_len,
                     bool diff_if_only_endspace_difference)
{
  int res;
  ob_wc_t UNINIT_VAR(s_wc), UNINIT_VAR(t_wc);
  const unsigned char *se= str + s_len, *te= t + t_len;
  ObUnicaseInfo *uni_plane= cs->caseinfo;

  ob_charset_assert((s_len % 2) == 0);
  ob_charset_assert((t_len % 2) == 0);

  while (str < se && t < te) {
    int s_res= cs->cset->mb_wc(cs, &s_wc, str, se);
    int t_res= cs->cset->mb_wc(cs, &t_wc, t, te);

    if (s_res <= 0 || t_res <= 0) {
      return ob_bincmp(str, se, t, te);
    } else {
    ob_tosort_utf16(uni_plane, &s_wc);
    ob_tosort_utf16(uni_plane, &t_wc);
    }
    if (s_wc != t_wc) {
      return s_wc > t_wc ? 1 : -1;
    } else {
      str+= s_res;
      t+= t_res;
    }
  }

  s_len= (size_t) (se - str);
  t_len= (size_t) (te - t);
  res= 0;

  if (s_len != t_len) {
    int s_res, swap= 1;
    if (diff_if_only_endspace_difference) {
      return s_len < t_len ? -1 : 1;
    } else if (s_len < t_len) {
      s_len= t_len;
      str= t;
      se= te;
      swap= -1;
      res= -res;
    }

    while (str < se) {
      if ((s_res= cs->cset->mb_wc(cs, &s_wc, str, se)) < 0) {
        ob_charset_assert(0);
        return 0;
      } else if (s_wc != ' ') {
        return (s_wc < ' ') ? -swap : swap;
      } else {
        str+= s_res;
      }
    }
  }
  return res;
}

static int
ob_wildcmp_utf16_ci(const ObCharsetInfo *cs,
                    const char *str,const char *str_end,
                    const char *wild_str,const char *wild_end,
                    int escape_char, int w_one, int w_many)
{
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  return ob_wildcmp_unicode(cs, str, str_end, wild_str, wild_end,
                            escape_char, w_one, w_many, uni_plane);
}

static void
ob_hash_sort_utf16(const ObCharsetInfo *cs, const unsigned char *str, size_t s_len,
                   unsigned long int *n1, unsigned long int *n2, const bool calc_end_space, hash_algo hash_algo)
{
  ob_wc_t wc;
  int res;
  unsigned int length = 0;
  unsigned char data[HASH_BUFFER_LENGTH];
  const unsigned char *end= str + (calc_end_space ? s_len : cs->cset->lengthsp(cs, (const char *) str, s_len));
  ObUnicaseInfo *uni_plane= cs->caseinfo;

  if (NULL == hash_algo) {
    while ((str < end) && (res= cs->cset->mb_wc(cs, &wc, (unsigned char *) str, (unsigned char *) end)) > 0)
    {
      ob_tosort_utf16(uni_plane, &wc);
      n1[0]^= (((n1[0] & 63) + n2[0]) * (wc & 0xFF)) + (n1[0] << 8);
      n2[0]+= 3;
      n1[0]^= (((n1[0] & 63) + n2[0]) * (wc >> 8)) + (n1[0] << 8);
      n2[0]+= 3;
      str+= res;
    }
  } else {
    while ((str < end) && (res= cs->cset->mb_wc(cs, &wc, (unsigned char *) str, (unsigned char *) end)) > 0)
    {
      ob_tosort_utf16(uni_plane, &wc);
      if (length > HASH_BUFFER_LENGTH - 2)
      {
        n1[0] = hash_algo((void*) &data, length, n1[0]);
        length = 0;
      }
      data[length++] = (unsigned char)wc;
      data[length++] = (unsigned char)(wc >> 8);
      str+= res;
    }
    if (length > 0) {
      n1[0] = hash_algo((void*) &data, length, n1[0]);
    }
  }
}

bool
ob_like_range_generic(const ObCharsetInfo *cs,
                      const char *ptr, size_t ptr_length,
                      char escape_char, char w_one, char w_many,
                      size_t res_length,
                      char *min_str,char *max_str,
                      size_t *min_length,size_t *max_length)
{
  const char *min_org = min_str;
  const char *max_org = max_str;
  char *min_end= min_str + res_length;
  char *max_end= max_str + res_length;
  const char *end = ptr + ptr_length;
  size_t char_len= res_length / cs->mbmaxlen;
  size_t res_length_diff;
  const ObContractions *contractions= ob_charset_get_contractions(cs, 0);

  while (char_len > 0) {
    ob_wc_t wc, wc2;
    int res = cs->cset->mb_wc(cs, &wc, (unsigned char*) ptr, (unsigned char*) end);
    if (res <= 0) {
      if (res == OB_CS_ILSEQ) {
        return TRUE; 
      } else {
        break; 
      }
    } else {
      ptr+= res;
    }

    if (wc == (ob_wc_t) escape_char) {
      res = cs->cset->mb_wc(cs, &wc, (unsigned char*) ptr, (unsigned char*) end);
      if (res <= 0) {
        if (res == OB_CS_ILSEQ) {
          return TRUE; 
        }
      } else {
        ptr+= res;
      }
      res= cs->cset->wc_mb(cs, wc, (unsigned char*) min_str, (unsigned char*) min_end);
      if (res <= 0) {
        goto PAD_SET_LEN; 
      } else {
        min_str+= res;
        res= cs->cset->wc_mb(cs, wc, (unsigned char*) max_str, (unsigned char*) max_end);
      }
      if (res <= 0) {
        goto PAD_SET_LEN; 
      } else {
        max_str+= res;
        continue;
      }
    } else if (wc == (ob_wc_t) w_one) {
      res= cs->cset->wc_mb(cs, cs->min_sort_char, (unsigned char*) min_str, (unsigned char*) min_end);
      if (res <= 0) {
        goto PAD_SET_LEN;
      } else {
        min_str+= res;
        res= cs->cset->wc_mb(cs, cs->max_sort_char, (unsigned char*) max_str, (unsigned char*) max_end);
      }
      if ( res <= 0) {
        goto PAD_SET_LEN;
      } else {
        max_str+= res;
        continue;
      }
    } else if ((ob_wc_t) w_many == wc) {
      *min_length= ((cs->state & OB_CS_BINSORT) ? (size_t) (min_str - min_org) : res_length);
      *max_length= res_length;
      goto PAD_MIN_MAX;
    }
    res= cs->cset->mb_wc(cs, &wc2, (unsigned char*) ptr, (unsigned char*) end);
    if (contractions &&
        ob_uca_can_be_contraction_head(contractions, wc) &&
        (res) > 0) {
      uint16_t *weight;
      if ((wc2 == (ob_wc_t) w_one || wc2 == (ob_wc_t) w_many)) {
        *min_length= *max_length= res_length;
        goto PAD_MIN_MAX;
      } else if ((weight= ob_uca_contraction2_weight(contractions, wc, wc2)) && 
                  weight[0] &&
                  ob_uca_can_be_contraction_tail(contractions, wc2))  {
        if (char_len == 1) {
          *min_length= *max_length= res_length;
          goto PAD_MIN_MAX;
        } else {
          char_len--;
          ptr+= res;
          res= cs->cset->wc_mb(cs, wc, (unsigned char*) min_str, (unsigned char*) min_end);
        }
        if (res <= 0) {
          goto PAD_SET_LEN;
        } else {
          min_str+= res;
        }
        res= cs->cset->wc_mb(cs, wc, (unsigned char*) max_str, (unsigned char*) max_end);
        if (res <= 0) {
          goto PAD_SET_LEN;
        } else {
          max_str+= res;
          wc= wc2;
        }
      }
    }
    res= cs->cset->wc_mb(cs, wc, (unsigned char*) min_str, (unsigned char*) min_end);
    if (res <= 0) {
      goto PAD_SET_LEN;
    } else {
      min_str+= res;
      res= cs->cset->wc_mb(cs, wc, (unsigned char*) max_str, (unsigned char*) max_end);
    }
    if (res <= 0) {
      goto PAD_SET_LEN;
    } else {
      max_str+= res;
      char_len--;
    }
  }

PAD_SET_LEN:
  *min_length= (size_t) (min_str - min_org);
  *max_length= (size_t) (max_str - max_org);

PAD_MIN_MAX:
  res_length_diff= res_length % cs->mbminlen;
  cs->cset->fill(cs, min_str, min_end - min_str - res_length_diff, cs->min_sort_char);
  cs->cset->fill(cs, max_str, max_end - max_str - res_length_diff, cs->max_sort_char);

  if (res_length_diff != 0) {
    memset(min_end - res_length_diff, 0, res_length_diff);
    memset(max_end - res_length_diff, 0, res_length_diff);
  }
  return FALSE;
}

ObCharsetHandler ob_charset_utf16_handler=
{
  ob_ismbchar_utf16,   
  ob_mbcharlen_utf16,  
  ob_numchars_utf16,
  ob_charpos_utf16,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_utf16,
  ob_lengthsp_mb2,
  ob_utf16_uni,        
  ob_uni_utf16,        
  ob_mb_ctype_mb,
  ob_caseup_utf16,
  ob_casedn_utf16,
  ob_fill_mb2,
  ob_strntol_mb2_or_mb4,
  ob_strntoul_mb2_or_mb4,
  ob_strntoll_mb2_or_mb4,
  ob_strntoull_mb2_or_mb4,
  ob_strntod_mb2_or_mb4,
  ob_strntoull10rnd_mb2_or_mb4,
  ob_scan_mb2
};

static ObCollationHandler ob_collation_utf16_bin_handler =
{
  NULL,
  NULL,
  ob_strnncoll_utf16_bin,
  ob_strnncollsp_utf16_bin,
  ob_strnxfrm_unicode_full_bin,
  ob_strnxfrmlen_unicode_full_bin,
  NULL,
  ob_like_range_generic,
  ob_wildcmp_utf16_bin,
  NULL,
  ob_instr_mb,
  ob_hash_sort_utf16_bin,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_utf16_general_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_utf16,
  ob_strnncollsp_utf16,
  ob_strnxfrm_unicode,
  ob_strnxfrmlen_simple,
  NULL,/*ob_strnxfrm_unicode_varlen_utf16,*/
  ob_like_range_generic,
  ob_wildcmp_utf16_ci,
  NULL,
  ob_instr_mb,
  ob_hash_sort_utf16,
  ob_propagate_simple
};

ObCharsetInfo ob_charset_utf16_bin=
{
  55,0,0,              
  OB_CS_COMPILED|OB_CS_BINSORT|OB_CS_STRNXFRM|OB_CS_UNICODE|OB_CS_NONASCII,
  OB_UTF16,             
  OB_UTF16_BIN,         
  "UTF-16 Unicode",    
  NULL,                 
  NULL,              
  NULL,                
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
  2,                   
  4,                   
  1,
  0,                   
  0xFFFF,              
  ' ',                 
  0,                   
  1,                   
  1,                   
  &ob_charset_utf16_handler,
  &ob_collation_utf16_bin_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_utf16_general_ci=
{
  54,0,0,              
  OB_CS_COMPILED|OB_CS_PRIMARY|OB_CS_STRNXFRM|OB_CS_UNICODE|OB_CS_NONASCII|OB_CS_CI,
  OB_UTF16,             
  OB_UTF16_GENERAL_CI,  
  "UTF-16 Unicode",    
  NULL,                
  NULL,                
  NULL,                
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
  2,                   
  4,                   
  1,
  0,                   
  0xFFFF,              
  ' ',                 
  0,                   
  1,                   
  1,                   
  &ob_charset_utf16_handler,
  &ob_collation_utf16_general_ci_handler,
  PAD_SPACE
};
