
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

#include "lib/charset/ob_mysql_global.h"
#include "lib/charset/ob_ctype.h"
#include "lib/charset/ob_ctype_gbk_tab.h"

#define isgbkhead(c) (0x81<=(unsigned char)(c) && (unsigned char)(c)<=0xfe)
#define isgbktail(c) ((0x40<=(unsigned char)(c) && (unsigned char)(c)<=0x7e) || \
                          (0x80<=(unsigned char)(c) && (unsigned char)(c)<=0xfe))

#define isgbkcode(c,d) (isgbkhead(c) && isgbktail(d))
#define gbkcode(c,d)   ((((unsigned int) (unsigned char) (c)) <<8) | (unsigned char)(d))
#define gbkhead(e)     ((unsigned char)(e>>8))
#define gbktail(e)     ((unsigned char)(e&0xff))

static uint16 gbksortorder(uint16 i)
{
  uint idx=gbktail(i);
  if (idx>0x7f) idx-=0x41;
  else idx-=0x40;
  idx+=(gbkhead(i)-0x81)*0xbe;
  return 0x8100+gbk_order[idx];
}


int ob_strnncoll_gbk_internal(const unsigned char **a_res, const unsigned char **b_res,
			      size_t length)
{
  const unsigned char *a= *a_res, *b= *b_res;
  unsigned int a_char,b_char;

  while (length--)
  {
    if ((length > 0) && isgbkcode(*a,*(a+1)) && isgbkcode(*b, *(b+1)))
    {
      a_char= gbkcode(*a,*(a+1));
      b_char= gbkcode(*b,*(b+1));
      if (a_char != b_char)
        return ((int) gbksortorder((uint16_t) a_char) -
		(int) gbksortorder((uint16_t) b_char));
      a+= 2;
      b+= 2;
      length--;
    }
    else if (sort_order_gbk[*a++] != sort_order_gbk[*b++])
      return ((int) sort_order_gbk[a[-1]] -
	      (int) sort_order_gbk[b[-1]]);
  }
  *a_res= a;
  *b_res= b;
  return 0;
}



int ob_strnncoll_gbk(const ObCharsetInfo *cs __attribute__((unused)),
		     const unsigned char *a, size_t a_length,
                     const unsigned char *b, size_t b_length,
                     bool b_is_prefix)
{
  size_t length = OB_MIN(a_length, b_length);
  int res= ob_strnncoll_gbk_internal(&a, &b, length);
  return res ? res : (int) ((b_is_prefix ? length : a_length) - b_length);
}


static int ob_strnncollsp_gbk(const ObCharsetInfo * cs __attribute__((unused)),
			      const unsigned char *a, size_t a_length,
			      const unsigned char *b, size_t b_length,
                              bool diff_if_only_endspace_difference)
{
  size_t length = OB_MIN(a_length, b_length);
  int res = ob_strnncoll_gbk_internal(&a, &b, length);

  if (!res && a_length != b_length) {
    const unsigned char *end;
    int swap= 1;
    if (diff_if_only_endspace_difference) {
      return a_length < b_length ? -1 : 1;
    } else if (a_length < b_length) {
      a_length = b_length;
      a = b;
      swap= -1;
      res= -res;
    }
    for (end= a + a_length-length; a < end ; a++) {
      if (*a != ' ') {
        return (*a < ' ') ? -swap : swap;
      }
    }
  }
  return res;
}


static size_t
ob_strnxfrm_gbk(const ObCharsetInfo *cs,
                unsigned char *dst, size_t dstlen, unsigned int nweights,
                const unsigned char *src, size_t srclen, unsigned int flags, bool *is_valid_unicode)
{
  unsigned char *d0= dst;
  unsigned char *de= dst + dstlen;
  const unsigned char *se= src + srclen;
  const unsigned char *sort_order= cs->sort_order;
  *is_valid_unicode = 1;

  for (; dst < de && src < se && nweights; nweights--) {
    if (cs->cset->ismbchar(cs, (const char*) src, (const char*) se)) {
      uint16_t e= gbksortorder((uint16_t) gbkcode(*src, *(src + 1)));
      *dst++= gbkhead(e);
      if (dst < de) {
        *dst++= gbktail(e);
      }
      src+= 2;
    } else {
      *is_valid_unicode = is_valid_ascii(*src);
      *dst++= sort_order ? sort_order[*src++] : *src++;
    }
  }
  return ob_strxfrm_pad_desc_and_reverse(cs, d0, dst, de, nweights, flags, 0);
}


size_t ob_varlen_encoding_gbk_for_memcmp(const struct ObCharsetInfo* cs,
                              unsigned char* dst, size_t dst_len, unsigned int nweights,
                              const unsigned char *src, size_t src_len,
                              bool *is_valid_unicode)
{
  unsigned char *d0= dst;
  unsigned char *de= dst + dst_len;
  const unsigned char *se= src + src_len;
  const unsigned char *sort_order= cs->sort_order;
  *is_valid_unicode = 1;

  for (; *is_valid_unicode && dst < de && src < se && nweights; nweights--)
  {
    if (isgbkhead(*(src)) && (se)-(src)>1 && isgbktail(*((src)+1)))
    {
      /*
        Note, it is safe not to check (src < se)
        in the code below, because ismbchar() would
        not return TRUE if src was too short
      */
      uint16_t e= gbksortorder((uint16_t) gbkcode(*src, *(src + 1)));
      *dst++= gbkhead(e);
      if (dst < de)
        *dst++= gbktail(e);
      src+= 2;
      if (e == 0) {
        *dst++ = 0x00;
        *dst++ = 0x01;
      }
    } else {
      *is_valid_unicode = is_valid_ascii(*src);
      uint16_t e = sort_order ? sort_order[*src++] : *src++;
      *dst++ = gbkhead(e);
      *dst++ = gbktail(e);
      if (e == 0) {
        *dst++ = 0x00;
        *dst++ = 0x01;
      }
    }
  }
  *dst++ = 0x00;
  *dst++ = 0x00;
  *dst++ = 0x00;
  *dst++ = 0x00;
  return dst - d0;
}

size_t ob_varlen_encoding_gbk_for_spacecmp(const struct ObCharsetInfo* cs,
                              unsigned char* dst, size_t dst_len, unsigned int nweights,
                              const unsigned char *src, size_t src_len,
                              bool *is_valid_unicode)
{
  unsigned char *d0= dst;
  unsigned char *de= dst + dst_len;
  const unsigned char *se= src + src_len;
  const unsigned char *sort_order= cs->sort_order;
  *is_valid_unicode = 1;

  // trim
  while (*(se-1) == 0x20 && se>src) se--;
  for (;*is_valid_unicode && dst < de && src < se && nweights; nweights--)
  {
    int16_t space_cnt = 0;
    uint16_t e = 0;
    while (*src == 0x20)
    {
      space_cnt++;
      src++;
    }
    if (isgbkhead(*(src)) && (se)-(src)>1 && isgbktail(*((src)+1)))
    {
      /*
        Note, it is safe not to check (src < se)
        in the code below, because ismbchar() would
        not return TRUE if src was too short
      */
      e = gbksortorder((uint16) gbkcode(*src, *(src + 1)));
      src+= 2;
    } else {
      *is_valid_unicode = is_valid_ascii(*src);
      e = sort_order ? sort_order[*src++] : *src++;
    }
    if (space_cnt != 0) {
      *dst++ = 0x00;
      *dst++ = 0x20;
      if (e > 0x20) {
        *dst++ = 0x00;
        *dst++ = 0x21;
        space_cnt = -space_cnt;
      } else {
        *dst++ = 0x00;
        *dst++ = 0x19;
      }
      *dst++ = ((unsigned char)(space_cnt >> 8));
      *dst++ = ((unsigned char)(space_cnt & 0xff));
    }
    *dst++ = gbkhead(e);
    *dst++ = gbktail(e);
  }
  *dst++ = 0x00;
  *dst++ = 0x20;
  *dst++ = 0x00;
  *dst++ = 0x20;

  return dst - d0;
}
size_t ob_strnxfrm_gbk_varlen(const struct ObCharsetInfo* cs,
                             unsigned char* dst, size_t dst_len, unsigned int nweights,
                             const unsigned char *src, size_t srclen,
                             bool is_memcmp, bool *is_valid_unicode)
{
  if (is_memcmp) {
    return ob_varlen_encoding_gbk_for_memcmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode);
  } else {
    return ob_varlen_encoding_gbk_for_spacecmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode);
  }
}


static unsigned int ismbchar_gbk(const ObCharsetInfo *cs __attribute__((unused)),
		 const char* p, const char *e)
{
  return (isgbkhead(*(p)) && (e)-(p)>1 && isgbktail(*((p)+1))? 2: 0);
}

static unsigned int mbcharlen_gbk(const ObCharsetInfo *cs __attribute__((unused)),
                          unsigned int c)
{
  return (isgbkhead(c)? 2 : 1);
}

static int
ob_wc_mb_gbk(const ObCharsetInfo *cs  __attribute__((unused)),
	      ob_wc_t wc, unsigned char *s, unsigned char *e)
{
  int code;

  if (s >= e) {
    return OB_CS_TOOSMALL;
  } else if ((unsigned int) wc < 0x80) {
    s[0]= (unsigned char) wc;
    return 1;
  } else if (!(code=func_uni_gbk_onechar(wc))) {
    return OB_CS_ILUNI;
  } else if (s+2>e) {
    return OB_CS_TOOSMALL2;
  }
  s[0] = code >> 8;
  s[1] = code & 0xFF;
  return 2;
}

static int ob_mb_wc_gbk(const ObCharsetInfo *cs __attribute__((unused)),
	                   ob_wc_t *pwc, const unsigned char *s, const unsigned char *e)
{
  int hi;
  if (s >= e) {
    return OB_CS_TOOSMALL;
  } else if ((hi = s[0]) < 0x80) {
    pwc[0]=hi;
    return 1;
  } else if (s+2>e) {
    return OB_CS_TOOSMALL2;
  } else if (!(pwc[0]=func_gbk_uni_onechar( (hi<<8) + s[1]))) {
    return -2;
  }

  return 2;
}


static size_t ob_well_formed_len_gbk(const ObCharsetInfo *cs __attribute__((unused)),
                              const char *b, const char *e,
                              size_t pos, int *error)
{
  const char *b0= b;
  const char *emb= e - 1;
  *error= 0;

  while (pos-- && b < e) {
    if ((unsigned char) b[0] < 128) {
      b++;
    } else  if ((b < emb) && isgbkcode((unsigned char)*b, (unsigned char)b[1])) {
      b+= 2;
    } else {
      *error= 1;
      break;
    }
  }
  return (size_t) (b - b0);
}

static ObCollationHandler ob_collation_gbk_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gbk,
  ob_strnncollsp_gbk,
  ob_strnxfrm_gbk,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gbk_varlen,
  ob_like_range_mb,
  ob_wildcmp_mb,
  NULL,
  ob_instr_mb,
  ob_hash_sort_simple,
  ob_propagate_simple
};

static ObCharsetHandler ob_charset_gbk_handler=
{
  ismbchar_gbk,
  mbcharlen_gbk,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_gbk,
  ob_lengthsp_8bit,
  /* ob_numcells_8bit, */
  ob_mb_wc_gbk,
  ob_wc_mb_gbk,
  ob_mb_ctype_mb,
  /* ob_caseup_str_mb, */
  /* ob_casedn_str_mb, */
  ob_caseup_mb,
  ob_casedn_mb,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit
};


ObCharsetInfo ob_charset_gbk_chinese_ci=
{
    28,0,0,
    OB_CS_COMPILED|OB_CS_PRIMARY|OB_CS_STRNXFRM|OB_CS_CI,
    "gbk",
    "gbk_chinese_ci",
    "",
    NULL,
    NULL,
    ctype_gbk,
    to_lower_gbk,
    to_upper_gbk,
    sort_order_gbk,
    NULL,
    &ob_caseinfo_gbk,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    2,
    1,
    0,
    0xA967,
    ' ',
    1,
    1,
    1,
    &ob_charset_gbk_handler,
    &ob_collation_gbk_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_gbk_bin=
{
    87,0,0,
    OB_CS_COMPILED|OB_CS_BINSORT,
    "gbk",
    "gbk_bin",
    "",
    NULL,
    NULL,
    ctype_gbk,
    to_lower_gbk,
    to_upper_gbk,
    NULL,
    NULL,
    &ob_caseinfo_gbk,
    NULL,
    NULL,
    1,
    1,
    1,
    1,
    2,
    1,
    0,
    0xFEFE,
    ' ',
    1,
    1,
    1,
    &ob_charset_gbk_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE
};
