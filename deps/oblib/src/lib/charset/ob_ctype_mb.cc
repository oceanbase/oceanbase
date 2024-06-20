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

static void __attribute__ ((noinline)) pad_max_char_help(char *str, char *end, char *buf, char buf_len)
{
  do {
    if ((str + buf_len) <= end) {  
      memcpy(str, buf, buf_len);
      str+= buf_len;
    } else {
      *str++= ' ';
    }
  } while (str < end);
}

static void pad_max_char(const ObCharsetInfo *cs, char *str, char *end)
{
  char buf[10];
  char buf_len;
  if (!(cs->state & OB_CS_UNICODE)) {
    if (cs->max_sort_char <= 255) {
      memset(str, cs->max_sort_char, end - str);
      return;
    }
    buf_len= 2;
    buf[0]= cs->max_sort_char >> 8;
    buf[1]= cs->max_sort_char & 0xFF;
  } else {
    buf_len= cs->cset->wc_mb(cs, cs->max_sort_char, (unsigned char*) buf,
                            (unsigned char*) buf + sizeof(buf));
  }
  ob_charset_assert(buf_len > 0);
  pad_max_char_help(str, end ,buf, buf_len);
}

bool ob_like_range_mb_help(const ObCharsetInfo *cs,
       size_t res_length,
       char **min_str_,char **max_str_,
       char **min_org_, char **min_end_,
       size_t *min_length,size_t *max_length, char **max_end_)
{
  char *min_str = *min_str_;
  char *max_str = *max_str_;
  char *min_end = *min_end_;
  char *max_end = *max_end_;
  char *min_org = *min_org_;
  *min_length = ((cs->state & OB_CS_BINSORT) ? (size_t) (min_str - min_org) : res_length);
  *max_length = res_length;
  do {
    *min_str++ = (char) cs->min_sort_char;
  } while (min_str != min_end);
  *max_length = res_length;
  pad_max_char(cs, max_str, max_end);
  *min_str_ = min_str;
  *max_str_ = max_str;
  *min_end_ = min_end;
  *max_end_ = max_end;
  *min_org_ = min_org;
  return 0;
}

bool ob_like_range_mb(const ObCharsetInfo *cs,
                      const char *ptr,size_t ptr_length,
                      pbool escape_char, pbool w_one, pbool w_many,
                      size_t res_length,
                      char *min_str,char *max_str,
                      size_t *min_length,size_t *max_length)
{
  unsigned int mb_len;
  const char *end= ptr + ptr_length;
  char *min_org= min_str;
  char *min_end= min_str + res_length;
  char *max_end= max_str + res_length;
  size_t max_char_len= res_length / cs->mbmaxlen;
  const ObContractions *contractions= ob_charset_get_contractions(cs, 0);

  for (; ptr != end && min_str != min_end && max_char_len ; max_char_len--) {
    if (*ptr == escape_char && ptr+1 != end) {
      ptr++;                                      
    } else if (*ptr == w_one ||  *ptr == w_many) {
      return ob_like_range_mb_help(cs,res_length, &min_str,&max_str, &min_org, &min_end, min_length, max_length, &max_end);
    }
    mb_len= ob_ismbchar(cs, ptr, end);
    if ( mb_len > 1) {
      if (min_str+mb_len > min_end ||
          ptr+mb_len > end) {
        break;
      }
      while (mb_len--) {
       *min_str++= *max_str++= *ptr++;
      }
    } else {
      if (contractions && ptr + 1 < end &&
          ob_uca_can_be_contraction_head(contractions, (unsigned char) *ptr)) {
        if (ptr[1] == w_one || ptr[1] == w_many) {
          return ob_like_range_mb_help(cs,res_length, &min_str,&max_str, &min_org, &min_end, min_length, max_length, &max_end);
        } else if (ob_uca_can_be_contraction_tail(contractions, (unsigned char) ptr[1]) &&
                   ob_uca_contraction2_weight(contractions, (unsigned char) ptr[0], ptr[1])) {
          if (max_char_len == 1 || min_str + 1 >= min_end) {
            return ob_like_range_mb_help(cs,res_length, &min_str,&max_str, &min_org, &min_end, min_length, max_length, &max_end);
          }
          max_char_len--;
          *min_str++= *max_str++= *ptr++;
        }
      }
      *min_str++= *max_str++= *ptr++;
    }
  }

  *min_length= *max_length = (size_t) (min_str - min_org);
  while (min_end != min_str) {
    *min_str++= *max_str++= ' ';
  }             
  return 0;
}

int ob_wildcmp_mb(const ObCharsetInfo *cs,
                  const char *str,const char *str_end,
                  const char *wild_str,const char *wild_end,
                  int escape_char, int w_one, int w_many)
{
  return ob_wildcmp_mb_impl(cs, str, str_end, wild_str, wild_end, escape_char, w_one, w_many, 1);
}

#define INC_PTR(cs,A,B) A+=(ob_ismbchar(cs,A,B) ? ob_ismbchar(cs,A,B) : 1)

#define likeconv(s,A) (unsigned char) (s)->sort_order[(unsigned char) (A)]

int ob_wildcmp_mb_impl(const ObCharsetInfo *cs,
                       const char *str,const char *str_end,
                       const char *wild_str,const char *wild_end,
                       int escape_char, int w_one, int w_many, int recurse_level)
{
  int result= -1;
  while (wild_str != wild_end) {
    while ((*wild_str == escape_char) || (*wild_str != w_many && *wild_str != w_one)) {
      int l;
      if (*wild_str == escape_char && wild_str+1 != wild_end) {
        wild_str++;
      }
      if ((l = ob_ismbchar(cs, wild_str, wild_end))) {
        if (str+l > str_end || memcmp(str, wild_str, l) != 0)
          return 1;
        str += l;
        wild_str += l;
      } else if (str == str_end || likeconv(cs,*wild_str++) != likeconv(cs,*str++)) {
       return(1);
      }
      if (wild_str == wild_end) {
       return (str != str_end);
      }
      result=1;
    }
    if (*wild_str == w_one) {
      do {
        if (str == str_end) {
          return (result);
        }
        INC_PTR(cs,str,str_end);
      } while (++wild_str < wild_end && *wild_str == w_one);
      if (wild_end == wild_str)
        break;
    }
    if (*wild_str == w_many) {
      unsigned char cmp;
      const char* mb = wild_str;
      int mb_len=0;
      wild_str++;
      for (; wild_str != wild_end ; wild_str++)
      {
        if (*wild_str == w_many)
          continue;
        if (*wild_str == w_one) {
          if (str == str_end)
            return (-1);
          INC_PTR(cs,str,str_end);
          continue;
        }
        break;
      }
      if (wild_str == wild_end) {
        return(0);
      } else if (str == str_end) {
        return -1;
      } else if ((cmp= *wild_str) == escape_char && wild_str+1 != wild_end) {
       cmp= *++wild_str;
      }

      mb=wild_str;
      mb_len= ob_ismbchar(cs, wild_str, wild_end);
      INC_PTR(cs,wild_str,wild_end);
      cmp=likeconv(cs,cmp);
      while (true) {
        while (TRUE) {
          if (str >= str_end) {
            return -1;
          } else if (mb_len) {
            if (str+mb_len <= str_end && memcmp(str, mb, mb_len) == 0)
            {
              str += mb_len;
              break;
            }
          } else if (!ob_ismbchar(cs, str, str_end) &&
                   likeconv(cs,*str) == cmp) {
            str++;
            break;
          }
          INC_PTR(cs,str, str_end);
        }
        {
          int tmp=ob_wildcmp_mb_impl(cs,str,str_end,
                                          wild_str,wild_end,escape_char,w_one,
                                            w_many, recurse_level + 1);
          if (tmp <= 0)
            return (tmp);
        }
        if (str == str_end) {
          return -1;
        } else if (wild_str != wild_end && wild_str[0] == w_many) {
          return -1;
      }
      }
      return(-1);
    }
  }
  return (str != str_end ? 1 : 0);
}

unsigned int __attribute__ ((noinline)) ob_instr_mb_help(size_t s_length, ob_match_t *match, unsigned int nmatch)
{
  if (!s_length) {
    if (nmatch) {
      match->beg= 0;
      match->end= 0;
      match->mb_len= 0;
    }
    return 1;
  }
  return 0;
}

unsigned int ob_instr_mb(const ObCharsetInfo *cs,
                 const char *b, size_t b_length,
                 const char *s, size_t s_length,
                 ob_match_t *match, unsigned int nmatch)
{
  const char *end, *b0;
  int res= 0;
  if (s_length <= b_length) {
    unsigned int tmp = ob_instr_mb_help(s_length, match, nmatch);
    if (tmp) {
      return tmp;
    }
    b0= b;
    end= b+b_length-s_length+1;
    while (b < end) {
      int mb_len;
      if (!cs->coll->strnncoll(cs, (unsigned char*) b, s_length,
                               (unsigned char*) s, s_length, 0)) {
        if (nmatch) {
          match[0].beg= 0;
          match[0].end= (size_t) (b-b0);
          match[0].mb_len= res;
          if (nmatch > 1) {
            match[1].beg= match[0].end;
            match[1].end= match[0].end+s_length;
            match[1].mb_len= 0;
          }
        }
        return 2;
      }
      mb_len= (mb_len= ob_ismbchar(cs, b, end)) ? mb_len : 1;
      b+= mb_len;
      b_length-= mb_len;
      res++;
    }
  }
  return 0;
}

uint ob_mbcharlen_ptr(const struct ObCharsetInfo *cs, const char *s, const char *e)
{
  uint len = ob_mbcharlen(cs, (uchar)*s);
  if (len == 0 && ob_mbmaxlenlen(cs) == 2 && s + 1 < e) {
    len = ob_mbcharlen_2(cs, (uchar)*s, (uchar) * (s + 1));
    assert(len == 0 || len == 2 || len == 4);
  }
  return len;
}

size_t ob_numchars_mb(const ObCharsetInfo *cs __attribute__((unused)), const char *pos, const char *end)
{
  size_t count= 0;
  while (pos < end) {
    unsigned int mb_len;
    pos+= (mb_len= ob_ismbchar(cs,pos,end)) ? mb_len : 1;
    count++;
  }
  return count;
}

size_t ob_charpos_mb(const ObCharsetInfo *cs __attribute__((unused)), const char *pos, const char *end, size_t length)
{
  const char *start= pos;
  while (length && pos < end) {
    unsigned int mb_len;
    pos+= (mb_len= ob_ismbchar(cs, pos, end)) ? mb_len : 1;
    length--;
  }
  return (size_t) (length ? end+2-start : pos-start);
}

static inline const ObUnicaseInfoChar*
get_case_info_for_ch(const ObCharsetInfo *cs, unsigned int page, unsigned int offs)
{
  const ObUnicaseInfoChar *p;
  return cs->caseinfo ? ((p= cs->caseinfo->page[page]) ? &p[offs] : NULL) :  NULL;
}

size_t ob_max_bytes_charpos_mb(const ObCharsetInfo *cs __attribute__((unused)), const char *pos, const char *end, size_t max_bytes, size_t *char_len)
{
  const char *start= pos;
  while (max_bytes && pos < end) {
    unsigned int mb_len;
    unsigned int bytes;
    mb_len = ob_ismbchar(cs, pos, end);
    bytes = mb_len ? mb_len : 1;
    if (max_bytes < bytes) {
      break;
    } else {
      pos += bytes;
      max_bytes -= bytes;
      ++*char_len;
    }
  }
  return (size_t) (pos-start);
}

int ob_mb_ctype_mb(const ObCharsetInfo *cs __attribute__((unused)), int *ctype,
                   const unsigned char *s, const unsigned char *e)
{
  ob_wc_t wc;
  int res = cs->cset->mb_wc(cs, &wc, s, e);
  if (res <= 0 || wc > 0xFFFF) {
    *ctype = 0;
  } else {
    *ctype = ob_uni_ctype[wc >> 8].ctype ?
        ob_uni_ctype[wc >> 8].ctype[wc & 0xFF] :
        ob_uni_ctype[wc >> 8].pctype;
  }
  return res;
}

size_t ob_casedn_mb(const ObCharsetInfo *cs, char *src, size_t src_len,
                    char *dst __attribute__((unused)),
                    size_t dstlen __attribute__((unused)))
{
  uint32_t l;
  unsigned char *map=cs->to_lower;
  char *src_end = src + src_len;

  while (src < src_end) {
    l = cs->cset->ismbchar(cs, src, src_end);
    if ( 0 != l ) {
      const ObUnicaseInfoChar *ch = get_case_info_for_ch(cs, (unsigned char) src[0], (unsigned char) src[1]);
      if (ch) {
        *src++= ch->tolower >> 8;
        *src++= ch->tolower & 0xFF;
      } else {
        src+= l;
      }
    } else {
      *src= (char) map[(unsigned char)*src];
      src++;
    }
  }
  return src_len;
}

size_t ob_caseup_mb(const ObCharsetInfo *cs, char *src, size_t src_len,
                    char *dst __attribute__((unused)),
                    size_t dstlen __attribute__((unused)))
{
  uint32_t l;
  unsigned char *map= cs->to_upper;
  char *src_end= src + src_len;
  while (src < src_end) {
    l = cs->cset->ismbchar(cs, src, src_end);
    if ( 0 != l ) {
      const ObUnicaseInfoChar *ch = get_case_info_for_ch(cs, (unsigned char) src[0], (unsigned char) src[1]);
      if (ch) {
        *src++= ch->toupper >> 8;
        *src++= ch->toupper & 0xFF;
      } else {
        src+= l;
      }
    } else {
      *src=(char) map[(unsigned char) *src];
      src++;
    }
  }
  return src_len;
}


const ObContractions *ob_charset_get_contractions(const ObCharsetInfo *cs, int level)
{
  return NULL;
}

bool ob_uca_can_be_contraction_head(const ObContractions *c, ob_wc_t wc)
{
  return false;
}

bool ob_uca_can_be_contraction_tail(const ObContractions *c, ob_wc_t wc)
{
  return false;
}

uint16_t *ob_uca_contraction2_weight(const ObContractions *list, ob_wc_t wc1, ob_wc_t wc2)
{
  return NULL;
}

size_t ob_lengthsp_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                        const char *ptr, size_t length)
{
  const char *end;
  end= (const char *) skip_trailing_space((const unsigned char *)ptr, length, 0);
  return (size_t) (end-ptr);
}

int ob_strnncoll_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                    const unsigned char *s, size_t slen,
                    const unsigned char *t, size_t tlen,
                    bool t_is_prefix)
{
  size_t len= OB_MIN(slen,tlen);
  int cmp= memcmp(s,t,len);
  return cmp ? cmp : (int) ((t_is_prefix ? len : slen) - tlen);
}

int __attribute__ ((noinline))  ob_strnncollsp_mb_bin_help(
    const unsigned char **a_, size_t a_length,
    const unsigned char **b_, size_t b_length,
    const unsigned char **end_,
    bool diff_if_only_endspace_difference, int *has_returned, int *res_, size_t length)
{
  *has_returned = 0;
  const unsigned char *a = *a_;
  const unsigned char *b = *b_;
  const unsigned char *end = *end_;
  int res = *res_;
  int swap= 1;
  if (a_length != b_length) {
    if (diff_if_only_endspace_difference) {
      res= 1;                              
    }
    if (a_length < b_length) {
       a_length= b_length;
       a= b;
      swap= -1;           
       res= -res;
    }
    for (end= a + a_length-length; a < end ; a++) {
       if (*a != ' ') {
         *has_returned = 1;
         break;
       }
     }
  }
  *a_ = a;
  *b_ = b;
  *end_ = end;
  *res_ = res;
  if (*has_returned == 1) {
    return (!diff_if_only_endspace_difference && *a < ' ') ? -swap : swap;
  }
  return 0;
}

int ob_strnncollsp_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                      const unsigned char *a, size_t a_length,
                      const unsigned char *b, size_t b_length,
                      bool diff_if_only_endspace_difference)
{
  const unsigned char *end;
  size_t length;
  int res;

  end= a + (length= OB_MIN(a_length, b_length));
  while (a < end) {
    if (*a++ != *b++) {
      return ((int) a[-1] - (int) b[-1]);
    }
  }
  res= 0;
  int has_returned = 0;
  int tmp = ob_strnncollsp_mb_bin_help(
      &a, a_length,
      &b, b_length,
      &end,
      diff_if_only_endspace_difference, &has_returned, &res, length);
  return has_returned == 1 ? tmp : res;
}

#define ob_strnxfrm_mb_non_ascii_char(cs, dst, src, se)                  \
{                                                                        \
  switch (cs->cset->ismbchar(cs, (const char*) src, (const char*) se)) { \
  case 4:                                                                \
    *dst++= *src++;                                                      \
                                                         \
  case 3:                                                                \
    *dst++= *src++;                                                      \
                                                         \
  case 2:                                                                \
    *dst++= *src++;                                                      \
                                                         \
  case 0:                                                                \
    *dst++= *src++;     \
  }                                                                      \
}

size_t ob_strnxfrm_mb(const ObCharsetInfo *cs,
               unsigned char *dst, size_t dstlen, unsigned int nweights,
                      const unsigned char *src, size_t src_len, unsigned int flags, bool *is_valid_unicode)
{
  unsigned char *d0= dst;
  unsigned char *de= dst + dstlen;
  const unsigned char *se= src + src_len;
  const unsigned char *sort_order= cs->sort_order;

  *is_valid_unicode = 1;

  if (dstlen >= src_len && nweights >= src_len) {
    if (sort_order) {  
      while (src < se) {
        if (*src < 128) {
          *dst++= sort_order[*src++];
        } else {
          ob_strnxfrm_mb_non_ascii_char(cs, dst, src, se);
        }
        nweights--;
      }
    } else {
      while (src < se) {
        if (*src < 128) {
          *dst++= *src++;
        } else {
          ob_strnxfrm_mb_non_ascii_char(cs, dst, src, se);
        }
        nweights--;
      }
    }
    goto pad;
  }

  for (; src < se && nweights && dst < de; nweights--) {
    int chlen;
    if (*src < 128 ||
        !(chlen= cs->cset->ismbchar(cs, (const char*) src, (const char*) se))) {
        
      *dst++= sort_order ? sort_order[*src++] : *src++;
    } else {
      int len= (dst + chlen <= de) ? chlen : de - dst;
      memcpy(dst, src, len);
      dst+= len;
      src+= len;
    }
  }

pad:
  return ob_strxfrm_pad_desc_and_reverse(cs, d0, dst, de, nweights, flags, 0);
}


#define INC_PTR(cs,A,B) A+=(ob_ismbchar(cs,A,B) ? ob_ismbchar(cs,A,B) : 1)

static int ob_wildcmp_mb_bin_impl_help(const ObCharsetInfo *cs, const char **str_,
    const char **str_end_, const char **wildstr_, const char **wildend_, int escape_char,
    int w_one, int w_many, int *result, int *has_returned)
{
  const char *wild_str = *wildstr_;
  const char *str_end = *str_end_;
  const char *str = *str_;
  const char *wild_end = *wildend_;
  *has_returned = 0;
  int ret = 0;
  int l;
  while (*wild_str == escape_char || (*wild_str != w_many && *wild_str != w_one)) {
    if (*wild_str == escape_char && wild_str + 1 != wild_end) {
      wild_str++;
    }
    if ((l = ob_ismbchar(cs, wild_str, wild_end))) {
      if (str + l > str_end || memcmp(str, wild_str, l) != 0) {
        *has_returned = 1;
        ret = 1;
        break;
      }
      str += l;
      wild_str += l;
    } else if (str == str_end || *wild_str++ != *str++) {
      *has_returned = 1;
      ret = 1;
      break;
    }
    if (wild_str == wild_end) {
      *has_returned = 1;
      ret = (str != str_end);
      break;
    } else {
      *result = 1;   
    }
  }
  *wildstr_ = wild_str;
  *str_end_ = str_end;
  *str_ = str;
  *wildend_ = wild_end;
  return ret;
}

static int ob_wildcmp_mb_bin_impl(const ObCharsetInfo *cs, const char *str,
    const char *str_end, const char *wild_str, const char *wild_end, int escape_char,
    int w_one, int w_many, int recurse_level)
{
  int result = -1;
  while (wild_str != wild_end) {
    int has_returned = 0;
    int tmp = ob_wildcmp_mb_bin_impl_help(cs, &str,&str_end, &wild_str, &wild_end, escape_char,w_one,w_many, &result, &has_returned);
    if (has_returned) {
      return tmp;
    } else if (*wild_str == w_one) {
      do {
        if (str == str_end) {
          return (result);
        } else {
          INC_PTR(cs, str, str_end);
        }
      } while (++wild_str < wild_end && *wild_str == w_one);
      if (wild_str == wild_end) {
        break;
      }
    }
    if (*wild_str == w_many) {
      unsigned char cmp;
      const char* mb = wild_str;
      int mb_len = 0;
      wild_str++;
      for (; wild_str != wild_end; wild_str++) {
        if (*wild_str == w_many) {
          continue;
        } else if (escape_char != w_one && *wild_str == w_one) {
          if (str == str_end) {
            return (-1);
          } else {
            INC_PTR(cs, str, str_end);
            continue;
          }
        } else {
          break;  
        }
      }
      if (wild_str == wild_end) {
        return (0);   
      } else if (str == str_end) {
        return -1;
      } else if ((cmp = *wild_str) == escape_char && wild_str + 1 != wild_end) {
        cmp = *++wild_str;
      }

      mb = wild_str;
      mb_len = ob_ismbchar(cs, wild_str, wild_end);
      INC_PTR(cs, wild_str, wild_end);   
      do {
        while(TRUE) {
          if (str >= str_end) {
            return -1;
          } else if (mb_len) {
            if (str + mb_len <= str_end && memcmp(str, mb, mb_len) == 0) {
              str += mb_len;
              break;
            }
          } else if (!ob_ismbchar(cs, str, str_end) && *str == cmp) {
            str++;
            break;
          }
          INC_PTR(cs, str, str_end);
        }
        {
          int tmp = ob_wildcmp_mb_bin_impl(cs, str, str_end, wild_str, wild_end,
              escape_char, w_one, w_many, recurse_level + 1);
          if (tmp <= 0) {
            return (tmp);
          }
        }          
      } while (str != str_end && (wild_str >= wild_end || wild_str[0] != w_many));
      return (-1);
    }
  }
  return (str != str_end ? 1 : 0);
}

int ob_wildcmp_mb_bin(const ObCharsetInfo *cs,
                  const char *str,const char *str_end,
                  const char *wild_str,const char *wild_end,
                  int escape_char, int w_one, int w_many)
{
  return ob_wildcmp_mb_bin_impl(cs, str, str_end,
                                wild_str, wild_end,
                                escape_char, w_one, w_many, 1);
}

void ob_hash_sort_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                    const unsigned char *key, size_t len,unsigned long int *nr1, unsigned long int *nr2,
                    const bool calc_end_space, hash_algo hash_algo)
{
  const unsigned char *pos = key;

  if (!calc_end_space) {
    key= skip_trailing_space(key, len, 0);
  } else {
    key += len;
  }
  int length = (int)(key - pos);
  if (NULL == hash_algo) {
    while (pos < (unsigned char*) key) {
      nr1[0]^=(unsigned long int) ((((unsigned int) nr1[0] & 63)+nr2[0]) *
        ((unsigned int)*pos)) + (nr1[0] << 8);
      nr2[0]+=3;
      pos++;
    }
  } else {
    nr1[0] = hash_algo((void*)pos, length, nr1[0]);
  }
}

ObCollationHandler ob_collation_mb_bin_handler = {
  NULL,
  NULL,
  ob_strnncoll_mb_bin,
  ob_strnncollsp_mb_bin,
  ob_strnxfrm_mb,
  ob_strnxfrmlen_simple,
  NULL,
  ob_like_range_mb,
  ob_wildcmp_mb_bin,
  NULL,
  ob_instr_mb,
  ob_hash_sort_mb_bin,
  ob_propagate_simple
};


#undef INC_PTR
#undef likeconv
