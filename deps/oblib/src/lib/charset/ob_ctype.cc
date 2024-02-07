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

static uint32
ob_convert_internal(char *to, uint32 to_length,
                    const ObCharsetInfo *to_cs,
                    const char *from, uint32 from_length,
                    const ObCharsetInfo *from_cs,
                    bool trim_incomplete_tail,
                    const ob_wc_t replaced_char, unsigned int *errors)
{
  unsigned int error_num= 0;
  int cnvres;
  ob_wc_t wc;
  const unsigned char *from_end= (const unsigned char*) from + from_length;
  char *to_start= to;
  unsigned char *to_end= (unsigned char*) to + to_length;
  ob_charset_conv_wc_mb wc_mb= to_cs->cset->wc_mb;
  ob_charset_conv_mb_wc mb_wc= from_cs->cset->mb_wc;
  pbool conitnue = TRUE;
  while (conitnue) {
    if ((cnvres= (*mb_wc)(from_cs, &wc, (unsigned char*) from, from_end)) > 0) {
      from+= cnvres;
    } else if (cnvres == OB_CS_ILSEQ) {
      from++;
      wc= replaced_char;
      error_num++;
    } else if (cnvres > OB_CS_TOOSMALL) {
      from+= (-cnvres);
      wc= replaced_char;
      error_num++;
    } else {
      // Not enough characters
      if (!trim_incomplete_tail && (const unsigned char*) from < from_end) {
        error_num++;
        from++;
        wc= replaced_char;
      } else {
        break;
      }
    }

    pbool go = TRUE;
    while (go) {
      go = FALSE;
      if ((cnvres= (*wc_mb)(to_cs, wc, (unsigned char*) to, to_end)) > 0)
      to+= cnvres;
      else if (cnvres == OB_CS_ILUNI && wc != replaced_char) {
        error_num++;
        wc= replaced_char;
        go =  TRUE;
      } else {
        conitnue = FALSE;
      }
    }
  }
  *errors= error_num;
  return (uint32) (to - to_start);
}


uint32
ob_convert(char *to, uint32 to_length, const ObCharsetInfo *to_cs,
           const char *from, uint32 from_length,
           const ObCharsetInfo *from_cs,
           bool trim_incomplete_tail,
           const ob_wc_t replaced_char , uint *errors)
{
  uint32 length, length2;

  if ((to_cs->state | from_cs->state) & OB_CS_NONASCII) {
    return ob_convert_internal(to, to_length, to_cs, from, from_length, from_cs,
                               trim_incomplete_tail, replaced_char, errors);
  } else {
    length= length2= OB_MIN(to_length, from_length);
  }

#if defined(__i386__)
  while (length >= 4) {
    if ((*(uint32*)from) & 0x80808080) break;
    *((uint32*) to) = *((const uint32*) from);
    from += 4; 
    to += 4;
    length -= 4; 
  }
#endif /* __i386__ */

  while (TRUE) {
    if (!length) {
      *errors= 0;
      return length2;
    } else if (*((unsigned char*) from) > 0x7F)  {
      uint32 copied_length= length2 - length;
      to_length-= copied_length;
      from_length-= copied_length;
      return copied_length + ob_convert_internal(to, to_length, to_cs,
                                                 from, from_length, from_cs,
                                                 trim_incomplete_tail, replaced_char, errors);
    }
    *to++= *from++;
    length--;
  }

  return 0;          
}

char *strmake(char *dst, const char *src, size_t length) {
#ifdef EXTRA_DEBUG
  /*
 * 'length' is the maximum length of the string; the buffer needs
 * ​to be one character larger to accomodate the terminating '\0'
 * This is easy to get wrong, so we make sure we write to the
 * entire length of the buffer to identify incorrect buffer-sizes.
 * We only initialise the "unused" part of the buffer here, a) for
 * efficiency, and b) because dst==src is allowed, so initialising
 * the entire buffer would overwrite the source-string. Also, we
 * write a character rather than '\0' as this makes spotting these
 * problems in the results easier.
 *		                                      */
  unsigned int n = 0;
  while (n < length && src[n++])
    ;
  memset(dst + n, (int)'Z', length - n + 1);
#endif
  while (length--)
    if (!(*dst++ = *src++)) return dst - 1;
  *dst = 0;
  return dst;
}
