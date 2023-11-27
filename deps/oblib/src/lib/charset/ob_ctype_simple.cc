
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

#define CUTOFF  (UINT64_MAX / 10)
#define CUTLIM  (UINT64_MAX % 10)
#define SPACE_INT 0x20202020
#define DIGITS_IN_ULONGLONG 20

static ulonglong d10[DIGITS_IN_ULONGLONG]=
{
  1,
  10,
  100,
  1000,
  10000,
  100000,
  1000000,
  10000000,
  100000000,
  1000000000,
  10000000000ULL,
  100000000000ULL,
  1000000000000ULL,
  10000000000000ULL,
  100000000000000ULL,
  1000000000000000ULL,
  10000000000000000ULL,
  100000000000000000ULL,
  1000000000000000000ULL,
  10000000000000000000ULL
};

long ob_strntol_8bit(const ObCharsetInfo *cs,
                     const char *nptr, size_t l, int base,
                     char **end_ptr, int *err)
{

  const char *save, *s = nptr, *e = nptr+l;
  unsigned char c;
  unsigned int cut_lim;
  *err= 0;
  uint32 cut_off;
  while (s<e && ob_isspace(cs, *s)) {
    s++;
  }

  int overflow;
  uint32 i;
  int neg;

  if (s == e) {
    goto NO_CONV;
  } else if (*s == '-') {
    neg = 1;
    ++s;
  } else if (*s == '+') {
    neg = 0;
    ++s;
  } else {
    neg = 0;
  }

  cut_lim = (unsigned int) (((uint32)~0L) % (uint32) base);
  cut_off = ((uint32)~0L) / (uint32) base;
  save = s;
  for (i = 0, overflow = 0, c = *s; s != e; c = *++s) {
    if (c>='0' && c<='9') {
      c -= '0';
    } else if (c>='A' && c<='Z') {
      c = c - 'A' + 10;
    } else if (c>='a' && c<='z') {
      c = c - 'a' + 10;
    } else {
      break;
    }

    if (c >= base) {
      break;
    } else if (i > cut_off || (i == cut_off && c > cut_lim)) {
      overflow = 1;
    } else {
      i *= (uint32) base;
      i += c;
    }
  }

  if (s == save) {
    goto NO_CONV;
  } else if (end_ptr != NULL) {
    *end_ptr = (char *) s;
  }

  if (neg) { 
    if (i  > (uint32) INT_MIN32) {
      overflow = 1;
  }
  } else if (i > INT_MAX32) {
    overflow = 1;
  }

  if (overflow) {
    err[0]= ERANGE;
    return neg ? INT_MIN32 : INT_MAX32;
  }

  return (neg ? -((long) i) : (long) i);

NO_CONV:
  err[0]= EDOM;
  if (end_ptr != NULL) {
    *end_ptr = (char *) nptr;
  }
  return 0L;
}


ulong ob_strntoul_8bit(const ObCharsetInfo *cs,
                       const char *nptr, size_t l, int base,
                       char **end_ptr, int *err)
{
  int neg;
  unsigned char c;
  const char *save, *e = nptr+l,  *s = nptr;
  uint32 cut_off;
  unsigned int cut_lim;

  *err= 0;

  while (s<e && ob_isspace(cs, *s)) {
    s++;
  }

  int overflow = 0;
  uint32 i = 0;

  if (s==e) {
    goto NO_CONV;
  } else if (*s == '-') {
    neg = 1;
    ++s;
  } else if (*s == '+') {
    neg = 0;
    ++s;
  } else {
    neg = 0;
  }

  save = s;
  cut_off = ((uint32)~0L) / (uint32) base;
  cut_lim = (unsigned int) (((uint32)~0L) % (uint32) base);

  for (c = *s; s != e; c = *++s) {
    if (c>='0' && c<='9') {
      c -= '0';
    } else if (c>='a' && c<='z') {
      c = c - 'a' + 10;
    } else if (c>='A' && c<='Z') {
      c = c - 'A' + 10;
    } else {
      break;
    }
    if (c >= base) {
      break;
    } else if (i > cut_off || (i == cut_off && c > cut_lim)) {
      overflow = 1;
    } else {
      i *= (uint32) base;
      i += c;
    }
  }

  if (s == save) {
    goto NO_CONV;
  } else if (end_ptr != NULL) {
    *end_ptr = (char *) s;
  }

  if (neg) {
    err[0]= ERANGE;
    return 0;
  } else if (overflow) {
    err[0]= ERANGE;
    return (~(uint32) 0);
  } else {
    return ((long) i);
  }

NO_CONV:
  err[0]= EDOM;
  if (end_ptr != NULL) {
    *end_ptr = (char *) nptr;
  }
  return 0L;
}


longlong ob_strntoll_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                          const char *nptr, size_t l, int base,
                          char **end_ptr,int *err)
{
  ulonglong cut_off;
  unsigned int cut_lim;
  const char *s = nptr, *e = nptr+l, *save;
  *err= 0;

  while (s<e && ob_isspace(cs,*s)) {
    s++;
  }

  int overflow = 0;
  ulonglong i = 0;
  int neg;
  if (s == e) {
    goto NO_CONV;
  } else if (*s == '-') {
    neg = 1;
    ++s;
  } else if (*s == '+') {
    neg = 0;
    ++s;
  } else {
    neg = 0;
  }

  save = s;

  cut_lim = (unsigned int) ((~(ulonglong) 0) % (unsigned long int) base);
  cut_off = (~(ulonglong) 0) / (unsigned long int) base;


  while (s != e) {
    unsigned char c= *s;
    if (c>='0' && c<='9')
      c -= '0';
    else if (c>='A' && c<='Z')
      c = c - 'A' + 10;
    else if (c>='a' && c<='z')
      c = c - 'a' + 10;
    else
      break;
    if (c >= base)
      break;
    if (i > cut_off || (i == cut_off && c > cut_lim))
      overflow = 1;
    else {
      i *= (ulonglong) base;
      i += c;
    }
    s++;
  }

  if (s == save) {
    goto NO_CONV;
  } else if (end_ptr != NULL) {
    *end_ptr = (char *) s;
  }

  if (neg) {
    if (i  > (ulonglong) LONGLONG_MIN) {
      overflow = 1;
    }
  } else if (i > (ulonglong) LONGLONG_MAX) {
    overflow = 1;
  }

  if (overflow) {
    err[0]= ERANGE;
    return neg ? LONGLONG_MIN : LONGLONG_MAX;
  } else {
    return (neg ? -((longlong) i) : (longlong) i);
  }

NO_CONV:
  err[0]= EDOM;
  if (end_ptr != NULL) {
    *end_ptr = (char *) nptr;
  }
  return 0L;
}


ulonglong ob_strntoull_8bit(const ObCharsetInfo *cs,
                            const char *nptr, size_t l, int base,
                            char **end_ptr, int *err)
{

  ulonglong cut_off;
  unsigned int cut_lim;
  const char *s = nptr, *e = nptr + l, *save;
  *err= 0;

  while (s<e && ob_isspace(cs,*s)) {
    s++;
  }

  int overflow = 0;
  ulonglong i = 0;
  int neg;

  if (s == e) {
    goto NO_CONV;
  } else if (*s == '-') {
    neg = 1;
    ++s;
  } else if (*s == '+') {
    neg = 0;
    ++s;
  } else {
    neg = 0;
  }

  save = s;

  cut_off = (~(ulonglong) 0) / (unsigned long int) base;
  cut_lim = (unsigned int) ((~(ulonglong) 0) % (unsigned long int) base);


  while (s != e) {
    unsigned char c= *s;
    if (c>='0' && c<='9') {
      c -= '0';
    } else if (c>='a' && c<='z') {
      c = c - 'a' + 10;
    } else if (c>='A' && c<='Z') {
      c = c - 'A' + 10;
    } else {
      break;
    }
    if (c >= base) {
      break;
    } else if (i > cut_off || (i == cut_off && c > cut_lim)) {
      overflow = 1;
    } else {
      i *= (ulonglong) base;
      i += c;
    }
    s++;
  }

  if (s == save) {
    goto NO_CONV;
  } else if (NULL != end_ptr) {
    *end_ptr = (char *) s;
  }

  if (overflow) {
    err[0]= ERANGE;
    return (~(ulonglong) 0);
  } else if (neg) {
    err[0]= ERANGE;
    return -((longlong) i);
  } else {
    return ((longlong) i);
  }

NO_CONV:
  err[0]= EDOM;
  if (NULL != end_ptr) {
    *end_ptr = (char *) nptr;
  }
  return 0L;
}



double ob_strntod_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                       char *str, size_t len,
                       char **end, int *err)
{
  if (len == INT_MAX32) {
    len= 65535;          
  }               
  *end= str + len;
  return ob_strtod(str, end, err);
}

ulonglong
ob_strntoull10rnd_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                       const char *str, size_t len, int unsigned_flag,
                       char **end_ptr, int *err)
{
  const char *dot, *end9, *beg, *end= str + len;
  ulonglong ull;
  unsigned long int ul;
  unsigned char ch;
  int shift = 0, digits = 0, neg, addon;

  while (str < end && (*str == ' ' || *str == '\t')) {
    str++;
  }

  if (str >= end) {
    goto RET_EDOM;
  } else if ((neg= (*str == '-')) || *str=='+')    {
    if (++str == end) {
      goto RET_EDOM;
    }
  }

  beg= str;
  end9= (str + 9) > end ? end : (str + 9);

  for (ul= 0 ; str < end9 && (ch= (unsigned char) (*str - '0')) < 10; str++) {
    ul= ul * 10 + ch;
  }

  if (str >= end) {
    *end_ptr= (char*) str;
    if (neg) {
      if (unsigned_flag) {
        *err= ul ? OB_ERRNO_ERANGE : 0;
        return 0;
      } else {
        *err= 0;
        return (ulonglong) (longlong) -(long) ul;
      }
    } else {
      *err=0;
      return (ulonglong) ul;
    }
  }

  digits= str - beg;

  for (dot= NULL, ull= ul; str < end; str++) {
    if ((ch= (unsigned char) (*str - '0')) < 10) {
      if (ull < CUTOFF || (ull == CUTOFF && ch <= CUTLIM)) {
        ull= ull * 10 + ch;
        digits++;
        continue;
      } else if (ull == CUTOFF) {
        ull= ULONGLONG_MAX;
        addon= 1;
        str++;
      } else {
        addon= (*str >= '5');
      }
      if (!dot) {
        while (str < end && (ch= (unsigned char) (*str - '0')) < 10) {
          shift++;
          str++;
        }
        if (str < end && *str == '.') {
          str++;
          while (str < end && (ch= (unsigned char) (*str - '0')) < 10) {
            str++;
          }
        }
      } else {
        shift= dot - str;
        while (str < end && (ch= (unsigned char) (*str - '0')) < 10) {
          str++;
        }
      }
      goto EXP;
    }

    if (*str == '.') {
      if (dot) {
        addon= 0;
        goto EXP;
      } else {
        dot= str + 1;
      }
      continue;
    }
    break;
  }
  shift= dot ? dot - str : 0;   
  addon= 0;

EXP:      
  if (!digits) {
    str= beg;
    goto RET_EDOM;
  } else if (neg && unsigned_flag) {
    goto RET_SIGN;
  } else if (str < end && (*str == 'e' || *str == 'E')) {
    str++;
    if (str < end) {
      int neg_exp, exponent;
      if ((neg_exp= (*str == '-')) || *str=='+') {
        if (++str == end) {
          goto RET_SIGN;
        }
      }
      for (exponent= 0 ;
           str < end && (ch= (unsigned char) (*str - '0')) < 10;
           str++) {
        exponent= exponent * 10 + ch;
      }
      shift+= neg_exp ? -exponent : exponent;
    }
  }

  if (shift == 0) {
    if (addon) {
      if (ull == ULONGLONG_MAX) {
        goto RET_TOO_LARGE;
      } else {
        ull++;
      }
    }
    goto RET_SIGN;
  }

  if (shift < 0) {
    ulonglong d, r, d_half;

    if (-shift >= DIGITS_IN_ULONGLONG) {
      goto RET_ZERO;   
    } else {
      d= d10[-shift];
      r= ull % d;
      d_half = d / 2;
      ull /= d;
      if (r >= d_half) {
        ull++;
      }
      goto RET_SIGN;
    }
  }

  if (shift > DIGITS_IN_ULONGLONG)    {
    if (!ull) {
      goto RET_SIGN;
    } else {
      goto RET_TOO_LARGE;
    }
  }

  while (shift > 0) {
    if (ull > CUTOFF) {
      goto RET_TOO_LARGE;
    } else {
      shift--;
      ull*= 10;
    }
  }

RET_SIGN:
  *end_ptr= (char*) str;

  if (!unsigned_flag) {
    if (neg) {
      if (ull > (ulonglong) LONGLONG_MIN) {
        *err= OB_ERRNO_ERANGE;
        return (ulonglong) LONGLONG_MIN;
      } else {
        *err= 0;
        return (ulonglong) -(longlong) ull;
      }
    } else {
      if (ull > (ulonglong) LONGLONG_MAX) {
        *err= OB_ERRNO_ERANGE;
        return (ulonglong) LONGLONG_MAX;
      } else {
        *err= 0;
        return ull;
      }
    }
  }

  if (neg && ull) {
    *err= OB_ERRNO_ERANGE;
    return 0;
  } else {
    *err= 0;
    return ull;
  }

RET_ZERO:
  *end_ptr= (char*) str;
  *err= 0;
  return 0;

RET_EDOM:
  *end_ptr= (char*) str;
  *err= OB_ERRNO_EDOM;
  return 0;

RET_TOO_LARGE:
  *end_ptr= (char*) str;
  *err= OB_ERRNO_ERANGE;
  return unsigned_flag ?
         ULONGLONG_MAX :
         neg ? (ulonglong) LONGLONG_MIN : (ulonglong) LONGLONG_MAX;
}

void ob_strxfrm_desc_and_reverse(unsigned char *str, unsigned char *str_end,
                                 unsigned int flags, unsigned int level)
{
  if (flags & (OB_STRXFRM_DESC_LEVEL1 << level)) {
    if (flags & (OB_STRXFRM_REVERSE_LEVEL1 << level)) {
      for (str_end--; str <= str_end;) {
        unsigned char tmp= *str;
        *str++= ~*str_end;
        *str_end--= ~tmp;
      }
    } else {
      while (str < str_end) {
        *str= ~*str;
        str++;
      }
    }
  } else if (flags & (OB_STRXFRM_REVERSE_LEVEL1 << level)) {
    for (str_end--; str < str_end;) {
      unsigned char tmp= *str;
      *str++= *str_end;
      *str_end--= tmp;
    }
  }
}

size_t ob_scan_8bit(const ObCharsetInfo *cs, const char *str, const char *end,
                    int sq)
{
  const char *str0= str;
  switch (sq) {
  case OB_SEQ_INTTAIL:
    if (str < end && *str == '.') {
      for(str++ ; str != end && *str == '0' ; str++);
      return (size_t) (str - str0);
    } else {
      return 0;
    }

  case OB_SEQ_SPACES:
    for ( ; str < end ; str++) {
      if (!ob_isspace(cs,*str)) {
        break;
      }
    }
    return (size_t) (str - str0);
  default:
    return 0;
  }
}

size_t ob_strxfrm_pad_desc_and_reverse(const ObCharsetInfo *cs,
                                unsigned char *str, unsigned char *frm_end, unsigned char *str_end,
                                unsigned int nweights, unsigned int flags, unsigned int level)
{
  if (nweights && frm_end < str_end && (flags & OB_STRXFRM_PAD_WITH_SPACE)) {
    unsigned int fill_len= OB_MIN((unsigned int) (str_end - frm_end), nweights * cs->mbminlen);
    cs->cset->fill(cs, (char*) frm_end, fill_len, cs->pad_char);
    frm_end+= fill_len;
  }
  ob_strxfrm_desc_and_reverse(str, frm_end, flags, level);
  if ((flags & OB_STRXFRM_PAD_TO_MAXLEN) && frm_end < str_end) {
    unsigned int fill_len= str_end - frm_end;
    cs->cset->fill(cs, (char*) frm_end, fill_len, cs->pad_char);
    frm_end= str_end;
  }
  return frm_end - str;
}

size_t ob_strnxfrmlen_simple(const ObCharsetInfo *cs, size_t len)
{
  return len * (cs->strxfrm_multiply ? cs->strxfrm_multiply : 1);
}

bool ob_like_range_simple(const ObCharsetInfo *cs,
                          const char *ptr, size_t ptr_len,
                          pbool escape_char, pbool w_one, pbool w_many,
                          size_t res_len,
                          char *min_str,char *max_str,
                          size_t *min_len, size_t *max_len)
{
  const char *end= ptr + ptr_len;
  char *min_org=min_str;
  char *min_end=min_str+res_len;
  size_t charlen= res_len / cs->mbmaxlen;

  for (; ptr != end && min_str != min_end && charlen > 0 ; ptr++, charlen--) {
    if (*ptr == escape_char && ptr+1 != end) {
      ptr++;
      *min_str++= *max_str++ = *ptr;
      continue;
    } else if (*ptr == w_one) {
      *min_str++='\0';
      *max_str++= (char) cs->max_sort_char;
      continue;
    } else if (*ptr == w_many) {
      *min_len= ((cs->state & OB_CS_BINSORT) ?
                    (size_t) (min_str - min_org) :
                    res_len);
      *max_len= res_len;
      do {
        *min_str++= 0;
        *max_str++= (char) cs->max_sort_char;
      } while (min_str != min_end);
      return 0;
    }
    *min_str++= *max_str++ = *ptr;
  }

  *min_len= *max_len = (size_t) (min_str - min_org);
  while (min_str != min_end) {
    *min_str++= *max_str++ = ' ';        
  }
  return 0;
}

bool ob_propagate_simple(const ObCharsetInfo *cs __attribute__((unused)),
                            const unsigned char *str __attribute__((unused)),
                            size_t len __attribute__((unused)))
{
  return 1;
}

bool ob_propagate_complex(const ObCharsetInfo *cs __attribute__((unused)),
                             const unsigned char *str __attribute__((unused)),
                             size_t len __attribute__((unused)))
{
  return 0;
}

void ob_fill_8bit(const ObCharsetInfo *cs __attribute__((unused)),
     char *s, size_t l, int fill)
{
  memset(s, fill, l);
}

int64_t ob_strntoll(const char *ptr, size_t len, int base, char **end, int *err)
{
  return ob_strntoll_8bit(&ob_charset_bin, ptr, len, base, end, err);
}

int64_t ob_strntoull(const char *ptr, size_t len, int base, char **end, int *err)
{
  return ob_strntoull_8bit(&ob_charset_bin, ptr, len, base, end, err);
}

void ob_hash_sort_simple(const ObCharsetInfo *cs,
                         const unsigned char *key, size_t len,
                         unsigned long int *nr1, unsigned long int *nr2,
                         const bool calc_end_space, hash_algo hash_algo)
{
  unsigned char *sort_order=cs->sort_order;
  const unsigned char *end;
  unsigned char data[HASH_BUFFER_LENGTH];
  int length = 0;
  end= calc_end_space ? key + len : skip_trailing_space(key, len, 0);

  if (NULL == hash_algo) {
    for (; key < (unsigned char*) end ; key++) {
      nr1[0]^=(unsigned long int) ((((unsigned int) nr1[0] & 63)+nr2[0]) *
        ((unsigned int) sort_order[(unsigned int) *key])) + (nr1[0] << 8);
      nr2[0]+=3;
    }
  } else {
    while (key < (unsigned char*)end) {
      length = (int)((unsigned char*)end - key) > HASH_BUFFER_LENGTH ?
                HASH_BUFFER_LENGTH : (int)((unsigned char*)end - key);
      for (int i = 0; i < length; i++, key++) {
        data[i] = sort_order[(unsigned int) *key];
      }
      nr1[0] = hash_algo(&data, length, nr1[0]);
    }
  }
}

#define SPACE_INT 0x20202020

const unsigned char *skip_trailing_space(const unsigned char *ptr,size_t len, bool is_utf16 /*false*/)
{
  const unsigned char *end= ptr + len;
  if (len > 20 && !is_utf16) {
    const unsigned char *end_words= (const unsigned char *)(int_ptr)
      (((ulonglong)(int_ptr)end) / SIZEOF_INT * SIZEOF_INT);
    const unsigned char *start_words= (const unsigned char *)(int_ptr)
       ((((ulonglong)(int_ptr)ptr) + SIZEOF_INT - 1) / SIZEOF_INT * SIZEOF_INT);
    ob_charset_assert(((ulonglong)(int_ptr)ptr) >= SIZEOF_INT);
    if (end_words > ptr) {
      while (end > end_words && end[-1] == 0x20) {
        end--;
      }
      if (end[-1] == 0x20 && start_words < end_words) {
        while (end > start_words && ((unsigned *)end)[-1] == SPACE_INT) {
          end -= SIZEOF_INT;
        }
      }
    }
  }
  if (is_utf16) {
      while (end - 1 > ptr && end[-2] == 0x00 && end[-1] == 0x20)
        end-=2;
  } else {
    while (end > ptr && end[-1] == 0x20)
      end--;
  }
  return (end);
}

size_t ob_strxfrm_pad(const ObCharsetInfo *cs, unsigned char *str, unsigned char *frm_end,
                      unsigned char *str_end, unsigned int nweights, unsigned int flags) {
  if (nweights && frm_end < str_end && (flags & OB_STRXFRM_PAD_WITH_SPACE)) {
    unsigned int fill_len = OB_MIN((unsigned int)(str_end - frm_end), nweights * cs->mbminlen);
    cs->cset->fill(cs, (char *)frm_end, fill_len, cs->pad_char);
    frm_end += fill_len;
  }
  if ((flags & OB_STRXFRM_PAD_TO_MAXLEN) && frm_end < str_end) {
    size_t fill_len = str_end - frm_end;
    cs->cset->fill(cs, (char *)frm_end, fill_len, cs->pad_char);
    frm_end = str_end;
  }
  return frm_end - str;
}


size_t ob_caseup_8bit(const ObCharsetInfo *cs __attribute__((unused)),
    char* src __attribute__((unused)), size_t srclen __attribute__((unused)),
    char* dst __attribute__((unused)), size_t dstlen __attribute__((unused))){
  const char *end = src + srclen;
  ob_charset_assert(src == dst && srclen == dstlen);
  for (; src != end; src++) *src = ob_toupper(cs,*src);
  return srclen;
}

size_t ob_casedn_8bit(const ObCharsetInfo *cs __attribute__((unused)),
    char* src __attribute__((unused)), size_t srclen __attribute__((unused)),
    char* dst __attribute__((unused)), size_t dstlen __attribute__((unused))){
  char *end = src + srclen;
  ob_charset_assert(src == dst && srclen == dstlen);
  for (; src != end; src++) *src = ob_tolower(cs,*src);
  return srclen;
}

int ob_strnncoll_simple(const ObCharsetInfo *cs __attribute__((unused)),
                        const unsigned char *s, size_t slen,
                        const unsigned char *t, size_t tlen,
                        bool is_prefix)
{
  size_t len = (slen > tlen) ? tlen : slen;
  if (is_prefix && slen > tlen) slen = tlen;
  while (len--) {
    if(ob_sort_order(cs,*s)!=ob_sort_order(cs,*t)) {
      return (int)ob_sort_order(cs,*s) - (int)ob_sort_order(cs,*t);
    }
    s++;
    t++;
  }
  return slen > tlen ? 1 : slen < tlen ? -1 : 0;
}

static int ob_strnncollsp_simple(const ObCharsetInfo *cs
                          __attribute__((unused)),
                          const unsigned char *s, size_t slen,
                          const unsigned char *t, size_t tlen,
                          bool diff_if_only_endspace_difference
                          __attribute__((unused)))
{
  size_t len = (slen > tlen) ? tlen : slen;
  for (size_t i = 0; i < len; i++){
    if(ob_sort_order(cs,*s)!=ob_sort_order(cs,*t)) {
      return (int)ob_sort_order(cs,*s) - (int)ob_sort_order(cs,*t);
    }
    s++;
    t++;
  }
  int res = 0;
  if (slen != tlen) {
    int swap = 1;
    if (diff_if_only_endspace_difference){
      res=1;
    }
    /*
      Check the next not space character of the longer key. If it's < ' ',
      then it's smaller than the other key.
    */
    if (slen < tlen) {
      slen = tlen;
      s = t;
      swap = -1;
      res = -res;
    }
    /*
    "a"  == "a "
    "a\0" < "a"
    "a\0" < "a "
    */
    for (const unsigned char* end = s + slen - len; s < end; s++) {
      if (ob_sort_order(cs,*s) != ob_sort_order(cs,(int)(' ')))
        return ob_sort_order(cs,*s) < ob_sort_order(cs,(int)(' ')) ? -swap : swap;
    }
  }
  return res;
}

static size_t ob_strnxfrm_simple(const ObCharsetInfo* cs __attribute__((unused)), unsigned char* dst, size_t dstlen,
    unsigned int nweights, const unsigned char* src, size_t srclen, unsigned int flags, bool* is_valid_unicode)
{
  unsigned char *dst0 = dst;
  const unsigned char *end;
  const unsigned char *remainder;
  size_t frmlen;
  frmlen = dstlen > nweights ? nweights : dstlen;
  frmlen = frmlen > srclen ? srclen : frmlen;
  end = src + frmlen;
  remainder = src + (frmlen % 8);
  *is_valid_unicode = 1;
  for (; src < remainder;) *dst++ = ob_sort_order(cs,*src++);
  while(src < end) {
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
    *dst++ = ob_sort_order(cs,*src++);
  }
  return ob_strxfrm_pad_desc_and_reverse(cs, dst0, dst, dst0 + dstlen, nweights - srclen, flags, 0);
}

#define likeconv(s, A) ob_sort_order(cs,A)
#define INC_PTR(cs, A, B) (A)++

static int ob_wildcmp_8bit_impl(const ObCharsetInfo* cs, const char* str_ptr, const char* str_end_ptr,
    const char* wild_str, const char* wild_end, int escape_char, int w_one_char, int w_many_char, int recurse_level)
{
  int cmp_result = -1;

  while (wild_str != wild_end) {
    while ((*wild_str == escape_char) || (*wild_str != w_many_char && *wild_str != w_one_char)) {
      if (*wild_str == escape_char && wild_str + 1 != wild_end) {
        wild_str++;
      }
      if (str_ptr == str_end_ptr || likeconv(cs, *wild_str++) != likeconv(cs, *str_ptr++)) {
        return 1;
      }
      if (wild_str == wild_end) {
        return str_ptr != str_end_ptr;
      }
      cmp_result = 1;
    }
    if (*wild_str == w_one_char) {
      do {
        if (str_ptr == str_end_ptr) {
          return (cmp_result);
        }
        INC_PTR(cs, str_ptr, str_end_ptr);
      } while (++wild_str < wild_end && *wild_str == w_one_char);
      if (wild_str == wild_end) {
        break;
      }
    }
    if (*wild_str == w_many_char) {
      unsigned char cmp = 0;
      wild_str++;
      for (; wild_str != wild_end; wild_str++) {
        if (*wild_str == w_many_char) {
          continue;
        }
        if (*wild_str == w_one_char) {
          if (str_ptr == str_end_ptr) {
            return (-1);
          }
          INC_PTR(cs, str_ptr, str_end_ptr);
          continue;
        }
        break;
      }
      if (wild_str == wild_end) {
        return (0);
      }
      if (str_ptr == str_end_ptr) {
        return (-1);
      }

      if ((cmp = *wild_str) == escape_char && wild_str + 1 != wild_end) {
        cmp = *++wild_str;
      }

      INC_PTR(cs, wild_str, wild_end);
      cmp = likeconv(cs, cmp);
      do {
        while (str_ptr != str_end_ptr && (unsigned char)likeconv(cs, *str_ptr) != cmp) {
          str_ptr++;
        }
        if (str_ptr++ == str_end_ptr) {
          return -1;
        }
        do {
          int tmp = ob_wildcmp_8bit_impl(
              cs, str_ptr, str_end_ptr, wild_str, wild_end, escape_char, w_one_char, w_many_char, recurse_level + 1);
          if (tmp <= 0) {
            return tmp;
          }
        } while (0);
      } while (str_ptr != str_end_ptr);
      return -1;
    }
  }
  return str_ptr != str_end_ptr ? 1 : 0;
}

int ob_wildcmp_8bit(const ObCharsetInfo* cs, const char* str, const char* str_end, const char* wildstr,
    const char* wildend, int escape, int w_one, int w_many)
{
  return ob_wildcmp_8bit_impl(cs, str, str_end, wildstr, wildend, escape, w_one, w_many, 1);
}

uint32_t ob_instr_simple(const ObCharsetInfo* cs , const char* b, size_t b_length,
    const char* s, size_t s_length, ob_match_t* match, unsigned int nmatch)
{
  register const unsigned char *str, *search, *end, *search_end;

  if (s_length <= b_length) {
    if (!s_length) {
      if (nmatch) {
        match->beg = 0;
        match->end = 0;
        match->mb_len = 0;
      }
      return 1; /* Empty string is always found */
    }

    str = (const unsigned char*)b;
    search = (const unsigned char*)s;
    end = (const unsigned char*)b + b_length - s_length + 1;
    search_end = (const unsigned char*)s + s_length;

  skip:
    while (str != end) {
      if (ob_sort_order(cs,*str++) == ob_sort_order(cs,*search)) {
        register const unsigned char *i, *j;

        i = str;
        j = search + 1;

        while (j != search_end)
          if (ob_sort_order(cs,*i++) != ob_sort_order(cs,*j++))
            goto skip;

        if (nmatch > 0) {
          match[0].beg = 0;
          match[0].end = (size_t)(str - (const unsigned char*)b - 1);
          match[0].mb_len = match[0].end;

          if (nmatch > 1) {
            match[1].beg = match[0].end;
            match[1].end = match[0].end + s_length;
            match[1].mb_len = match[1].end - match[1].beg;
          }
        }
        return 2;
      }
    }
  }
  return 0;
}

ObCollationHandler ob_collation_8bit_simple_ci_handler = {
    NULL, /* init */
    NULL,
    ob_strnncoll_simple,
    ob_strnncollsp_simple,
    ob_strnxfrm_simple,
    ob_strnxfrmlen_simple,
    NULL,//varlen
    ob_like_range_simple,
    ob_wildcmp_8bit,
    NULL,//ob_strcasecmp_8bit,
    ob_instr_simple,
    ob_hash_sort_simple,
    ob_propagate_simple};

#undef likeconv
#undef INC_PTR