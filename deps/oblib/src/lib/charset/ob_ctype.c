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

static uint32_t ob_convert_internal(char* to, uint32_t to_length, const ObCharsetInfo* to_cs, const char* from,
    uint32_t from_length, const ObCharsetInfo* from_cs, uint32_t* errors)
{
  int res;
  ob_wc_t wc;
  const unsigned char* from_end = (const unsigned char*)from + from_length;
  char* to_start = to;
  unsigned char* to_end = (unsigned char*)to + to_length;
  ob_charset_conv_mb_wc mb_wc = from_cs->cset->mb_wc;
  ob_charset_conv_wc_mb wc_mb = to_cs->cset->wc_mb;
  uint32_t error_count = 0;

  while (1) {
    if ((res = (*mb_wc)((unsigned char*)from, from_end, &wc)) > 0) {
      from += res;
    } else if (res == OB_CS_ERR_ILLEGAL_SEQUENCE) {
      error_count++;
      from++;
      wc = '?';
    } else if (res > OB_CS_ERR_TOOSMALL) {
      /*
        A correct multibyte sequence detected
        But it doesn't have Unicode mapping.
      */
      error_count++;
      from += (-res);
      wc = '?';
    } else {
      break;  // Not enough characters
    }

  outp:
    if ((res = (*wc_mb)(wc, (unsigned char*)to, to_end)) > 0) {
      to += res;
    } else if (res == OB_CS_ERR_ILLEGAL_UNICODE && wc != '?') {
      error_count++;
      wc = '?';
      goto outp;
    } else {
      break;
    }
  }
  *errors = error_count;
  return (uint32_t)(to - to_start);
}

uint32_t ob_convert(char* to, uint32_t to_length, const ObCharsetInfo* to_cs, const char* from, uint32_t from_length,
    const ObCharsetInfo* from_cs, uint32_t* errors)
{
  uint32_t length, length2;
  /*
    If any of the character sets is not ASCII compatible,
    immediately switch to slow mb_wc->wc_mb method.
  */
  if ((to_cs->state | from_cs->state) & OB_CS_NONASCII) {
    return ob_convert_internal(to, to_length, to_cs, from, from_length, from_cs, errors);
  }

  length = length2 = to_length < from_length ? to_length : from_length;

  for (;; *to++ = *from++, length--) {
    if (!length) {
      *errors = 0;
      return length2;
    }
    if (*((unsigned char*)from) > 0x7F) { /* A non-ASCII character */
      uint32_t copied_length = length2 - length;
      to_length -= copied_length;
      from_length -= copied_length;
      return copied_length + ob_convert_internal(to, to_length, to_cs, from, from_length, from_cs, errors);
    }
  }

  return 0;
}
