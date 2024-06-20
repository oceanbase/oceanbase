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
#include "lib/charset/ob_ctype_bin_tab.h"

unsigned int ob_mbcharlen_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                               unsigned int c __attribute__((unused)))
{
  return 1;
}

size_t ob_numchars_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                        const char *begin, const char *end)
{
  return (size_t) (end - begin);
}

size_t ob_charpos_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                       const char *begin  __attribute__((unused)),
                       const char *end  __attribute__((unused)),
                       size_t pos)
{
  return pos;
}

size_t ob_max_bytes_charpos_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                       const char *begin  __attribute__((unused)),
                       const char *end  __attribute__((unused)),
                       size_t max_bytes,
                       size_t *char_len)
{
  *char_len = max_bytes;
  return max_bytes;
}

size_t ob_well_formed_len_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                               const char *start, const char *end,
                               size_t nchars, int *error)
{
  size_t nbytes= (size_t) (end-start);
  *error= 0;
  return OB_MIN(nbytes, nchars);
}

size_t ob_lengthsp_binary(const ObCharsetInfo *cs __attribute__((unused)),
                          const char *ptr __attribute__((unused)),
                          size_t length)
{
  return length;
}

static int ob_mb_wc_bin(const ObCharsetInfo *cs __attribute__((unused)),
                        ob_wc_t *wc,
                        const unsigned char *str,
                        const unsigned char *end __attribute__((unused)))
{
  if (str >= end) {
    return OB_CS_TOOSMALL;
  } else {
    *wc=str[0];
  }
  return 1;
}


static int ob_wc_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                        ob_wc_t wc,
                        unsigned char *str,
                        unsigned char *end __attribute__((unused)))
{
  if (str >= end) {
    return OB_CS_TOOSMALL;
  } else if (wc < 256) {
    str[0] = (char) wc;
    return 1;
  }
  return OB_CS_ILUNI;
}

int ob_mb_ctype_8bit(const ObCharsetInfo *cs, int *ctype,
                   const unsigned char *str, const unsigned char *end)
{
  if (str >= end) {
    *ctype= 0;
    return OB_CS_TOOSMALL;
  }
  *ctype= cs->ctype[*str + 1];
  return 1;
}

static size_t ob_case_bin(const ObCharsetInfo *cs __attribute__((unused)),
                          char *src __attribute__((unused)),
                          size_t srclen,
                          char *dst __attribute__((unused)),
                          size_t dstlen __attribute__((unused)))
{
  return srclen;
}


static int ob_strnncoll_8bit_bin(const ObCharsetInfo *cs __attribute__((unused)),
                               const uchar *s, size_t slen,
                               const uchar *t, size_t tlen,
                               bool t_is_prefix)
{
  size_t len= OB_MIN(slen,tlen);
  int cmp= memcmp(s,t,len);
  return cmp ? cmp : (int)((t_is_prefix ? len : slen) - tlen);
}

static int ob_strnncoll_binary(const ObCharsetInfo *cs __attribute__((unused)),
                               const unsigned char *str, size_t s_len,
                               const unsigned char *t, size_t t_len,
                               bool t_is_prefix)
{
  size_t len = OB_MIN(s_len,t_len);
  int cmp = memcmp(str,t,len);
  return cmp ? cmp : (int)((t_is_prefix ? len : s_len) - t_len);
}

static int ob_strnncollsp_binary(const ObCharsetInfo *cs
                                 __attribute__((unused)),
                                 const unsigned char *str, size_t s_len,
                                 const unsigned char *t, size_t t_len,
                                 bool diff_if_only_endspace_difference
                                 __attribute__((unused)))
{
  return ob_strnncoll_binary(cs,str,s_len,t,t_len,0);
}

static int ob_strnncollsp_8bit_bin(const ObCharsetInfo *cs
                                 __attribute__((unused)),
                                 const unsigned char *str, size_t s_len,
                                 const unsigned char *t, size_t t_len,
                                 bool diff_if_only_endspace_difference
                                 __attribute__((unused)))
{
  const unsigned char *end;
  size_t len = s_len < t_len ? s_len : t_len;
  int res;
  end = str + len;
  while (str < end) {
    if (*str++ != *t++) return ((int)str[-1] - (int)t[-1]);
  }
  res = 0;
  if (s_len != t_len) {
    int swap = 1;
    if (diff_if_only_endspace_difference){
      res=1;
    }
    if (s_len < t_len) {
      s_len = t_len;
      str = t;
      swap = -1;
      res = -res;
    }
    for (end = str + s_len - len; str < end; str++) {
      if (*str != ' ') return (*str < ' ') ? -swap : swap;
    }
  }
  return res;
}

static size_t
ob_strnxfrm_8bit_bin(const ObCharsetInfo *cs,
                     unsigned char * dst, size_t dstlen, unsigned int nweights,
                     const unsigned char *src, size_t srclen, unsigned int flags,
                     bool *is_valid_unicode)
{
  set_if_smaller(srclen, dstlen);
  set_if_smaller(srclen, nweights);
  *is_valid_unicode = 1;
  if (dst != src) {
    memcpy(dst, src, srclen);
  }
  return ob_strxfrm_pad_desc_and_reverse(cs, dst, dst + srclen, dst + dstlen,
                                         nweights - srclen, flags, 0);
}

#define likeconv(str,A) (A)
#define INC_PTR(cs,A,B) (A)++

static
int ob_wildcmp_bin_impl(const ObCharsetInfo *cs,
                        const char *str,const char *str_end,
                        const char *wild_str,const char *wild_end,
                        int escape_char, int w_one, int w_many, int recurse_level)
{
  int result= -1;
  while (wild_str != wild_end) {
    while ((*wild_str == escape_char) ||  (*wild_str != w_many && *wild_str != w_one)) {
      if (*wild_str == escape_char && wild_str+1 != wild_end) {
        wild_str++;
      }
      if (str == str_end || likeconv(cs,*wild_str++) != likeconv(cs,*str++)) {
        return(1);			 
      } else if (wild_str == wild_end) {
        return(str != str_end);
      } else {
        result=1;
      }
    }
    if (*wild_str == w_one) {
      do {
        if (str == str_end) {
          return(result);
        } else {
          INC_PTR(cs,str,str_end);
        }
      } while (++wild_str < wild_end && *wild_str == w_one);
      if (wild_str == wild_end) break;
    }
    if (*wild_str == w_many) {
      unsigned char cmp;
      wild_str++;  
      for (; wild_str != wild_end ; wild_str++) {
        if (*wild_str == w_many) {
          continue;
        } else if (*wild_str == w_one) {
          if (str == str_end) {
            return(-1);
          } else {
            INC_PTR(cs,str,str_end);
            continue;
          }
        }
        break;
      }
      if (wild_str == wild_end) {
        return(0);
      } else if (str == str_end) {
        return(-1);
      } else if ((cmp= *wild_str) == escape_char && wild_str+1 != wild_end) {
        cmp= *++wild_str;
      }

      INC_PTR(cs,wild_str,wild_end);	 
      cmp=likeconv(cs,cmp);
      while (true) {
        while (str != str_end && (unsigned char) likeconv(cs,*str) != cmp) {
          str++;
        }
        if (str++ == str_end) {
          return(-1);
        }
        {
          int tmp=ob_wildcmp_bin_impl(cs,str,str_end,
                                      wild_str,wild_end,escape_char,
                                      w_one, w_many, recurse_level + 1);
          if (tmp <= 0) {
            return(tmp);
          } else if (str == str_end) {
            return -1;
          } else if (wild_str != wild_end && wild_str[0] == w_many) {
            return -1;
          }
        }
      }
      return(-1);
    }
  }
  return(str != str_end ? 1 : 0);
}

int ob_wildcmp_bin(const ObCharsetInfo *cs,
                   const char *str,const char *str_end,
                   const char *wild_str,const char *wild_end,
                   int escape_char, int w_one, int w_many)
{
  return ob_wildcmp_bin_impl(cs, str, str_end,
                             wild_str, wild_end,
                             escape_char, w_one, w_many, 1);
}

static
unsigned int ob_instr_bin(const ObCharsetInfo *cs __attribute__((unused)),
                          const char *begin, size_t b_length,
                          const char *s, size_t s_length,
                          ob_match_t *match, unsigned int nmatch)
{
  const unsigned char *str, *search, *end, *search_end;

  if (s_length <= b_length) {
    if (!s_length) {
      if (nmatch) {
        match->beg= 0;
        match->end= 0;
        match->mb_len= 0;
      }
      return 1;
    }

    str= (const unsigned char*) begin;
    search= (const unsigned char*) s;
    end= (const unsigned char*) begin+b_length-s_length+1;
    search_end= (const unsigned char*) s + s_length;

loop:
    while (str != end) {
      if ( (*str++) == (*search)) {
        const unsigned char *i, *j;
        i= str;
        j= search+1;

        while (j != search_end) {
          if ((*i++) != (*j++))  {
            goto loop;
          }
        }
        if (nmatch > 0) {
          match[0].beg= 0;
          match[0].end= (size_t) (str- (const unsigned char*)begin-1);
          match[0].mb_len= match[0].end;

          if (nmatch > 1) {
            match[1].beg= match[0].end;
            match[1].end= match[0].end+s_length;
            match[1].mb_len= match[1].end-match[1].beg;
          }
        }
        return 2;
      }
    }
  }
  return 0;
}
void ob_hash_sort_8bit_bin(const ObCharsetInfo *cs __attribute__((unused)),
              const uchar *key, size_t len, ulong *nr1, ulong *nr2, const bool calc_end_space, hash_algo hash_algo)
{
  const uchar *pos = key;
  key += len;
  //trailing space to make 'A ' == 'A'
  if (!calc_end_space) {
    key = skip_trailing_space(pos, len, 0);
  }
  if (NULL == hash_algo)
  {
    for (; pos < (uchar*) key ; pos++)
    {
      nr1[0]^=(ulong) ((((uint) nr1[0] & 63)+nr2[0]) *
        ((uint)*pos)) + (nr1[0] << 8);
      nr2[0]+=3;
    }
  } else {
    nr1[0] = hash_algo((void*)pos, (int)(key - pos), nr1[0]);
  }
}
void ob_hash_sort_bin(const ObCharsetInfo *cs __attribute__((unused)),
                      const unsigned char *key, size_t len,
                      unsigned long int *nr1, unsigned long int *nr2,
                      const bool calc_end_space,
                      hash_algo hash_algo)
{
  const unsigned char *pos = key;
  key+= len;
  if (NULL == hash_algo) {
    while (pos < (unsigned char*) key) {
      nr1[0]^=(unsigned long int) ((((unsigned int) nr1[0] & 63)+nr2[0]) *
        ((unsigned int)*pos)) + (nr1[0] << 8);
      nr2[0]+=3;
      pos++;
    }
  } else {
    nr1[0] = hash_algo((void*)pos, (int)(key - pos), nr1[0]);
  }
}

 

static ObCharsetHandler ob_charset_handler=
{
  NULL,
  ob_mbcharlen_8bit,
  ob_numchars_8bit,
  ob_charpos_8bit,
  ob_max_bytes_charpos_8bit,
  ob_well_formed_len_8bit,
  ob_lengthsp_binary,
  ob_mb_wc_bin,
  ob_wc_mb_bin,
  ob_mb_ctype_8bit,
  ob_case_bin,
  ob_case_bin,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit
};

ObCollationHandler ob_collation_8bit_bin_handler =
{
  NULL,			/* init */
  NULL,			/* uninit */
  ob_strnncoll_8bit_bin,
  ob_strnncollsp_8bit_bin,
  ob_strnxfrm_8bit_bin,
  ob_strnxfrmlen_simple,
  NULL,
  //ob_strnxfrmlen_simple,
  ob_like_range_simple,
  ob_wildcmp_bin,
  NULL, //ob_strcasecmp_bin,
  ob_instr_bin,
  ob_hash_sort_8bit_bin,
  ob_propagate_simple
};

ObCollationHandler ob_collation_binary_handler =
{
  NULL,
  NULL,
  ob_strnncoll_binary,
  ob_strnncollsp_binary,
  ob_strnxfrm_8bit_bin,
  ob_strnxfrmlen_simple,
  NULL,
  ob_like_range_simple,
  ob_wildcmp_bin,
  NULL, //ob_strcasecmp_bin,
  ob_instr_bin,
  ob_hash_sort_bin,
  ob_propagate_simple
};

ObCharsetInfo ob_charset_bin =
{
  63,0,0,
  OB_CS_COMPILED|OB_CS_BINSORT|OB_CS_PRIMARY,
  "binary",
  "binary",
  "",
  NULL,
  NULL,
  ctype_bin,
  bin_char_array,
  bin_char_array,
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
  0,
  0,
  1,
  1,
  &ob_charset_handler,
  &ob_collation_binary_handler,
  PAD_SPACE
};


#undef likeconv
#undef INC_PTR
