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

#ifndef OB_CHARSET_STRING_HELPER_H
#define OB_CHARSET_STRING_HELPER_H


#include "lib/charset/ob_charset.h"
#include "lib/charset/mb_wc.h"
#include "lib/charset/ob_ctype_gbk_tab.h"
#include "lib/charset/ob_cypte_gb18030_tab.h"
#include "lib/charset/ob_ctype_latin1_tab.h"


namespace oceanbase
{
namespace common
{

//inlined charset char len functions
template<ObCharsetType cs_type>
inline int ob_charset_char_len(const unsigned char *s, const unsigned char *e) {
  return OB_LIKELY(s < e) ? 1 : OB_CS_TOOSMALL;
}

//inlined charset decode functions
template<ObCharsetType cs_type>
inline int ob_charset_decode_unicode(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value) {
  unicode_value = 0;
  return ob_charset_char_len<CHARSET_BINARY>(s, e);
}

//inlined charset encode functions
template<ObCharsetType cs_type>
inline int ob_charset_encode_unicode(ob_wc_t unicode_value, unsigned char *buf, unsigned char *buf_end) {
  return OB_CS_ILUNI;
}

//UTF8
template<>
inline int ob_charset_char_len<CHARSET_UTF8MB4>(const unsigned char *s, const unsigned char *e) {
  int mb_len = OB_CS_TOOSMALL;
  if (OB_LIKELY(s < e)) {
    unsigned char c = *s;
    if (c < 0x80) {
      mb_len = 1;
    } else if (c < 0xc2) {
      mb_len = 1; /* Illegal mb head */
    } else if (c < 0xe0) {
      mb_len = 2;
    } else if (c < 0xf0) {
      mb_len = 3;
    } else if (c < 0xf8) {
      mb_len = 4;
    } else {
      mb_len = 1; /* Illegal mb head */
    }
    if (s + mb_len > e) {
      mb_len = OB_CS_TOOSMALL;
    }
  }
  return mb_len; /* Illegal mb head */;
}

template<>
inline int ob_charset_decode_unicode<CHARSET_UTF8MB4>(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value)
{
  return ob_mb_wc_utf8_prototype<true, true>(&unicode_value, s, e);
}

template<>
inline int ob_charset_encode_unicode<CHARSET_UTF8MB4>(ob_wc_t unicode_value, unsigned char *s, unsigned char *e) {
  ob_wc_t wc = unicode_value;
  int bytes = 0;
  int ret = 0;
  int64_t len = (int64_t)(e - s);
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_CS_TOOSMALL;
  } else if (wc < 0x80) { //7	U+0000	U+007F	1	0xxxxxxx
    bytes = 1;
  } else if (wc < 0x800) {//11	U+0080	U+07FF	2	110xxxxx	10xxxxxx
    bytes = 2;
  } else if (wc < 0x10000) {//16	U+0800	U+FFFF	3	1110xxxx	10xxxxxx	10xxxxxx
    bytes = 3;
  } else if (wc < 0x200000) {// 21	U+10000	U+1FFFFF 4	11110xxx	10xxxxxx	10xxxxxx	10xxxxxx
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
      case 4: s[3] = (unsigned char) (0x80 | (wc & 0x3f)); wc >>= 6; wc |= 0x10000;
      case 3: s[2] = (unsigned char) (0x80 | (wc & 0x3f)); wc >>= 6; wc |= 0x800;
      case 2: s[1] = (unsigned char) (0x80 | (wc & 0x3f)); wc >>= 6; wc |= 0xc0;
      case 1: s[0] = (unsigned char) wc;
    }
    ret = bytes;
  }
  return ret;
}


//GBK
template<>
inline int ob_charset_char_len<CHARSET_GBK>(const unsigned char *s, const unsigned char *e) {
  int mb_len = OB_CS_TOOSMALL;
  if (OB_LIKELY(s < e)) {
    if (0x81 <= *s && *s <= 0xFE) {
      if (OB_LIKELY(s + 1 < e)) {
        mb_len = 2;
      }
    } else {
      mb_len = 1;
    }
  }
  return mb_len;
}

template<>
inline int ob_charset_decode_unicode<CHARSET_GBK>(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value)
{
  int mb_len = 2;
  int hi;
  if (s >= e) {
    mb_len = OB_CS_TOOSMALL;
  } else if ((hi = s[0]) < 0x80) {
    unicode_value=hi;
    mb_len = 1;
  } else if (s+2>e) {
    mb_len = OB_CS_TOOSMALL2;
  } else if (!(unicode_value=func_gbk_uni_onechar( (hi<<8) + s[1]))) {
    mb_len = -2;
  }

  return mb_len;
}

template<>
inline int ob_charset_encode_unicode<CHARSET_GBK>(ob_wc_t unicode_value, unsigned char *s, unsigned char *e) {
  ob_wc_t wc = unicode_value;
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

//GB18030


#define is_mb_1(c) ((unsigned char)(c) <= 0x7F)
#define is_mb_odd(c) (0x81 <= (unsigned char)(c) && (unsigned char)(c) <= 0xFE)
#define is_mb_even_2(c)                          \
  ((0x40 <= (unsigned char)(c) && (unsigned char)(c) <= 0x7E) || \
   (0x80 <= (unsigned char)(c) && (unsigned char)(c) <= 0xFE))
#define is_mb_even_4(c) (0x30 <= (unsigned char)(c) && (unsigned char)(c) <= 0x39)

#define MIN_MB_ODD_BYTE 0x81
#define MIN_MB_EVEN_BYTE_2 0x40
#define MIN_MB_EVEN_BYTE_4 0x30
#define UNI2_TO_GB4_DIFF 7456

template<>
inline int ob_charset_char_len<CHARSET_GB18030>(const unsigned char *s, const unsigned char *e) {
  int mb_len = OB_CS_TOOSMALL;
  if (OB_LIKELY(s < e)) {
    unsigned char high_c = *s;
    if (!(0x81 <= high_c && high_c <= 0xFE)) {
      mb_len = 1;
    } else if (OB_LIKELY(s + 1 < e)) {
      unsigned char low_c = *(s + 1);
      if ((0x40 <= low_c && low_c <= 0x7E) || (0x80 <= low_c && low_c <= 0xFE)) {
        mb_len = 2;
      } else if (0x30 <= low_c && low_c <= 0x39) {
        if (OB_LIKELY(s + 3 < e)) {
          mb_len = 4;
        }
      } else {
        mb_len = 1; /* Illegal low_c */
      }
    }
  }
  return mb_len;
}

template<>
inline int ob_charset_decode_unicode<CHARSET_GB18030>(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value)
{
  int mb_len = 0;
  unsigned int idx = 0;
  unsigned int cp = 0;

  if (s >= e) {
    mb_len = OB_CS_TOOSMALL;
  } else if (is_mb_1(s[0])) {
    unicode_value = s[0];
    mb_len = 1;
  } else if (!is_mb_odd(s[0])) {
    mb_len = OB_CS_ILSEQ;
  } else if (s + 2 > e) {
    mb_len = OB_CS_TOOSMALL2;
  } else if (is_mb_even_2(s[1])) {
    idx = (s[0] - MIN_MB_ODD_BYTE) * 192 + (s[1] - MIN_MB_EVEN_BYTE_2);
    unicode_value = tab_gb18030_2_uni[idx];
    mb_len = (unicode_value == 0) ? OB_CS_ILSEQ : 2;
  } else if (is_mb_even_4(s[1])) {
    if (s + 4 > e) {
      mb_len = OB_CS_TOOSMALL4;
    } else if (!(is_mb_odd(s[2]) && is_mb_even_4(s[3]))) {
      mb_len = OB_CS_ILSEQ;
    } else {
      idx = (s[0] - MIN_MB_ODD_BYTE) * 12600 +
            (s[1] - MIN_MB_EVEN_BYTE_4) * 1260 +
            (s[2] - MIN_MB_ODD_BYTE) * 10 + (s[3] - MIN_MB_EVEN_BYTE_4);


      if (idx < 0x334) {
        cp = tab_gb18030_4_uni[idx];
      } else if (idx <= 0x1D20) {
        cp = idx + 0x11E;
      } else if (idx < 0x2403) {
        cp = tab_gb18030_4_uni[idx - 6637];
      } else if (idx <= 0x2C40) {
        cp = idx + 0x240;
      } else if (idx < 0x4A63) {
        cp = tab_gb18030_4_uni[idx - 6637 - 2110];
      } else if (idx <= 0x82BC) {
        cp = idx + 0x5543;
      } else if (idx < 0x830E) {
        cp = tab_gb18030_4_uni[idx - 6637 - 2110 - 14426];
      } else if (idx <= 0x93D4) {
        cp = idx + 0x6557;
      } else if (idx < 0x94BE) {
        cp = tab_gb18030_4_uni[idx - 6637 - 2110 - 14426 - 4295];
      } else if (idx <= 0x98C3) {
        cp = idx + 0x656C;
      } else if (idx <= 0x99fb) {
        cp = tab_gb18030_4_uni[idx - 6637 - 2110 - 14426 - 4295 - 1030];
      } else if (idx >= 0x2E248 && idx <= 0x12E247) {
        cp = idx - 0x1E248;
      } else if ((idx > 0x99fb && idx < 0x2E248) ||
               (idx > 0x12E247 && idx <= 0x18398F)) {
        cp = 0x003F;
      }

      unicode_value = cp;
      mb_len = 4;
    }
  } else {
    mb_len = OB_CS_ILSEQ;
  }
  return mb_len;
}

template<>
inline int ob_charset_decode_unicode<CHARSET_GB18030_2022>(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value)
{
  uint idx = 0;
  uint cp = 0;

  if (s >= e) return OB_CS_TOOSMALL;

  if (is_mb_1(s[0])) {
    /* [0x00, 0x7F] */
    unicode_value = s[0];
    return 1;
  } else if (!is_mb_odd(s[0]))
    return OB_CS_ILSEQ;

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (is_mb_even_2(s[1])) {
    idx = (s[0] - MIN_MB_ODD_BYTE) * 192 + (s[1] - MIN_MB_EVEN_BYTE_2);
    unicode_value = tab_gb18030_2022_2_uni[idx];

    return (unicode_value == 0) ? OB_CS_ILSEQ : 2;
  } else if (is_mb_even_4(s[1])) {
    if (s + 4 > e) return OB_CS_TOOSMALL4;

    if (!(is_mb_odd(s[2]) && is_mb_even_4(s[3]))) return OB_CS_ILSEQ;

    idx = (s[0] - MIN_MB_ODD_BYTE) * 12600 +
          (s[1] - MIN_MB_EVEN_BYTE_4) * 1260 +
          (s[2] - MIN_MB_ODD_BYTE) * 10 + (s[3] - MIN_MB_EVEN_BYTE_4);

    if (idx < 0x334) /* [GB+81308130, GB+8130D330) */
      cp = tab_gb18030_2022_4_uni[idx];
    else if (idx <= 0x1D20)
      /* [GB+8130D330, GB+8135F436] */
      cp = idx + 0x11E;
    else if (idx < 0x2403)
      /* (GB+8135F436, GB+8137A839) */
      cp = tab_gb18030_2022_4_uni[idx - 6637];
    else if (idx <= 0x2C40)
      /* [GB+8137A839, GB+8138FD38] */
      cp = idx + 0x240;
    else if (idx < 0x4A63 + GB_2022_CNT_PART_1)
      /* (GB+8138FD38, GB+82359135) */
      cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110];
    else if (idx <= 0x82BC)
      /* [GB+82359135, GB+8336C738] */
      cp = idx + 0x5543;
    else if (idx < 0x830E)
      /* (GB+8336C738, GB+8336D030) */
      cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110 + GB_2022_CNT_PART_1 - 14426];
    else if (idx <= 0x93D4)
      /* [GB+8336D030, GB+84308534] */
      cp = idx + 0x6557;
    else if (idx < 0x94BE)
      /* (GB+84308534, GB+84309C38) */
      cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110 + GB_2022_CNT_PART_1 - 14426 - 4295];
    else if (idx <= 0x98C3 - GB_2022_CNT_PART_2)
      /* [GB+84309C38, GB+84318235] */
      cp = idx + 0x656C;
    else if (idx <= 0x99fb)
      /* (GB+84318235, GB+8431A439] */
      cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110 + GB_2022_CNT_PART_1 - 14426 - 4295 - 1030 + GB_2022_CNT_PART_2];
    else if (idx >= 0x2E248 && idx <= 0x12E247)
      /* [GB+90308130, GB+E3329A35] */
      cp = idx - 0x1E248;
    else if ((idx > 0x99fb && idx < 0x2E248) ||
             (idx > 0x12E247 && idx <= 0x18398F))
      /* (GB+8431A439, GB+90308130) and (GB+E3329A35, GB+FE39FE39) */
      cp = 0x003F;
    else
      ob_charset_assert(0);

    unicode_value = cp;
    return 4;
  } else
    return OB_CS_ILSEQ;
}

template<>
inline int ob_charset_encode_unicode<CHARSET_GB18030>(ob_wc_t unicode_value, unsigned char *s, unsigned char *e) {
  ob_wc_t wc = unicode_value;
  unsigned int idx = 0;
  unsigned int len = 2;
  uint16_t cp = 0;
  unsigned int err;

  if (s >= e) {
    return OB_CS_TOOSMALL;
  } else if (wc < 0x80) {
    s[0] = (unsigned char)wc;
    return 1;
  } else if (wc < 0x9FA6) {
    cp = tab_uni_gb18030_p1[wc - 0x80];
    if ((unsigned int)((cp >> 8) & 0xFF) < MIN_MB_ODD_BYTE) {
      idx = cp;
      len = 4;
    }
  } else if (wc <= 0xD7FF) {
    idx = wc - 0x5543;
    len = 4;
  } else if (wc < 0xE000) {
    return OB_CS_ILUNI;
  } else if (wc < 0xE865) {
    cp = tab_uni_gb18030_p2[wc - 0xE000];
    if ((unsigned int)((cp >> 8) & 0xFF) < MIN_MB_ODD_BYTE) {
      idx = cp + UNI2_TO_GB4_DIFF;
      len = 4;
    }
  } else if (wc <= 0xF92B) {
    idx = wc - 0x6557;
    len = 4;
  } else if (wc <= 0XFFFF) {
    cp = tab_uni_gb18030_p2[wc - 0xE000 - 4295];
    if ((unsigned int)((cp >> 8) & 0xFF) < MIN_MB_ODD_BYTE) {
      idx = cp + UNI2_TO_GB4_DIFF;
      len = 4;
    }
  } else if (wc <= 0x10FFFF) {
    idx = wc + 0x1E248;
    len = 4;
  } else {
    return OB_CS_ILUNI;
  }

  switch (len) {
    case 2:
      if (s + 2 > e) return OB_CS_TOOSMALL2;
      s[0] = (unsigned char)((cp >> 8) & 0xFF);
      s[1] = (unsigned char)(cp & 0xFF);
      return len;
    case 4:
      if (s + 4 > e) return OB_CS_TOOSMALL4;
      s[3] = (unsigned char)(idx % 10) + MIN_MB_EVEN_BYTE_4;
      idx /= 10;
      s[2] = (unsigned char)(idx % 126) + MIN_MB_ODD_BYTE;
      idx /= 126;
      s[1] = (unsigned char)(idx % 10) + MIN_MB_EVEN_BYTE_4;
      s[0] = (unsigned char)(idx / 10) + MIN_MB_ODD_BYTE;
      return len;
  }

  return OB_CS_ILUNI;
}

template<>
inline int ob_charset_encode_unicode<CHARSET_GB18030_2022>(ob_wc_t unicode_value, unsigned char *s, unsigned char *e) {
  ob_wc_t wc = unicode_value;
  unsigned int idx = 0;
  unsigned int len;
  uint16_t cp = 0;
  unsigned int err;

  if (s >= e) return OB_CS_TOOSMALL;

  if (wc < 0x80) {
    /* [0x00, 0x7F] */
    s[0] = (unsigned char)wc;
    return 1;
  }

  len = 2;
  if (wc < 0x9FBC) {
    /* [0x80, 0x9FBC) */
    cp = tab_uni_gb18030_2022_p1[wc - 0x80];
    if ((unsigned int)((cp >> 8) & 0xFF) < MIN_MB_ODD_BYTE) {
      idx = cp;
      len = 4;
    }
  } else if (wc <= 0xD7FF) {
    /* [0x9FBC, 0xD7FF] */
    idx = wc - 0x5543;
    len = 4;
  } else if (wc < 0xE000) {
    /* [0xD800, 0xE000) */
    return OB_CS_ILUNI;
  } else if (wc < 0xE865) {
    /* [0xE000, 0xE865) */
    cp = tab_uni_gb18030_2022_p2[wc - 0xE000];
    if ((unsigned int)((cp >> 8) & 0xFF) < MIN_MB_ODD_BYTE) {
      idx = cp + UNI2_TO_GB4_DIFF;
      len = 4;
    }
  } else if (wc <= 0xF92B) {
    /* [0xE865, 0xF92B] */
    idx = wc - 0x6557;
    len = 4;
  } else if (wc <= 0XFFFF) {
    /* (0xF92B, 0xFFFF] */
    cp = tab_uni_gb18030_2022_p2[wc - 0xE000 - 4295];
    if ((unsigned int)((cp >> 8) & 0xFF) < MIN_MB_ODD_BYTE) {
      idx = cp + UNI2_TO_GB4_DIFF;
      len = 4;
    }
  } else if (wc <= 0x10FFFF) {
    /* [0x10000, 0x10FFFF] */
    idx = wc + 0x1E248;
    len = 4;
  } else {
    /* Other */
    return OB_CS_ILUNI;
  }

  switch (len) {
    case 2:
      if (s + 2 > e) return OB_CS_TOOSMALL2;

      s[0] = (unsigned char)((cp >> 8) & 0xFF);
      s[1] = (unsigned char)(cp & 0xFF);

      return len;
    case 4:
      if (s + 4 > e) return OB_CS_TOOSMALL4;
      s[3] = (unsigned char)(idx % 10) + MIN_MB_EVEN_BYTE_4;
      idx /= 10;
      s[2] = (unsigned char)(idx % 126) + MIN_MB_ODD_BYTE;
      idx /= 126;
      s[1] = (unsigned char)(idx % 10) + MIN_MB_EVEN_BYTE_4;
      s[0] = (unsigned char)(idx / 10) + MIN_MB_ODD_BYTE;
      return len;
  }

  ob_charset_assert(0);
  return OB_CS_ILUNI;
}


#undef MIN_MB_ODD_BYTE
#undef MIN_MB_EVEN_BYTE_2
#undef MIN_MB_EVEN_BYTE_4
#undef UNI2_TO_GB4_DIFF

#undef is_mb_1
#undef is_mb_odd
#undef is_mb_even_2
#undef is_mb_even_4

//latin1
template<>
inline int ob_charset_decode_unicode<CHARSET_LATIN1>(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value)
{
  int mb_len = 0;
  if (s >= e) {
    mb_len = OB_CS_TOOSMALL;
  } else {
    unicode_value = cs_to_uni[*s];
    mb_len = (!unicode_value && s[0]) ? -1 : 1;
  }
  return mb_len;
}

//utf16
#define OB_UTF16_HIGH_HEAD(x)  ((((uchar) (x)) & 0xFC) == 0xD8)
#define OB_UTF16_LOW_HEAD(x)   ((((uchar) (x)) & 0xFC) == 0xDC)
#define OB_UTF16_SURROGATE(x)  (((x) & 0xF800) == 0xD800)
#define OB_UTF16_WC2(a, b)       ((a << 8) + b)
#define OB_UTF16_WC4(a, begin, c, d) (((a & 3) << 18) + (begin << 10) + \
                                  ((c & 3) << 8) + d + 0x10000)

template<>
inline int ob_charset_char_len<CHARSET_UTF16>(const unsigned char *s, const unsigned char *e)
{
  int mb_len = OB_CS_TOOSMALL;
  if (OB_LIKELY(s + 1 < e)) {
    if (OB_UTF16_HIGH_HEAD(*s)) {
      if (s + 3 < e) {
        mb_len = 4;
      }
    } else {
      mb_len = 2;
    }
  }
  return mb_len;
}

template<>
inline int ob_charset_decode_unicode<CHARSET_UTF16>(const unsigned char *s, const unsigned char *e, ob_wc_t &unicode_value)
{
  int mb_len = 2;
  if (2 > e - s) {
    mb_len = OB_CS_TOOSMALL2;
  } else if (OB_UTF16_HIGH_HEAD(*s))  {
    if (4 > e - s) {
      mb_len = OB_CS_TOOSMALL4;
    } else if (!OB_UTF16_LOW_HEAD(s[2]))  {
      mb_len = OB_CS_ILSEQ;
    } else {
      unicode_value= OB_UTF16_WC4(s[0], s[1], s[2], s[3]);
      mb_len = 4;
  }
  } else if (OB_UTF16_LOW_HEAD(*s)) {
    return OB_CS_ILSEQ;
  } else {
    unicode_value= OB_UTF16_WC2(s[0], s[1]);
  }
  return mb_len;
}


template<>
inline int ob_charset_encode_unicode<CHARSET_UTF16>(ob_wc_t unicode_value, unsigned char *s, unsigned char *e)
{
  ob_wc_t wc = unicode_value;
  if (wc <= 0xFFFF) {
    if (2 > e - s) {
      return OB_CS_TOOSMALL2;
    } else if (OB_UTF16_SURROGATE(wc)) {
      return OB_CS_ILUNI;
    } else {
      *s++= (unsigned char) (wc >> 8);
      *s= (unsigned char) (wc & 0xFF);
      return 2;
    }
  } else if (wc <= 0x10FFFF) {
    if (4 > e - s) {
      return OB_CS_TOOSMALL4;
    } else {
      *s++= (unsigned char) ((wc-= 0x10000) >> 18) | 0xD8;
      *s++= (unsigned char) (wc >> 10) & 0xFF;
      *s++= (unsigned char) ((wc >> 8) & 3) | 0xDC;
      *s= (unsigned char) wc & 0xFF;
      return 4;
    }
  }

  return OB_CS_ILUNI;
}

#undef OB_UTF16_HIGH_HEAD
#undef OB_UTF16_LOW_HEAD
#undef OB_UTF16_SURROGATE
#undef OB_UTF16_WC2
#undef OB_UTF16_WC4

/* Fast string scanner according to the charset
 * The basic idea is to make the decode function ObCharset::mb_wc inline
 * It will at least 2x fast than the non-inline version
 */
class ObFastStringScanner {
public:
  template<ObCharsetType CS_TYPE, typename HANDLE_FUNC, bool DO_DECODE = true>
  static int foreach_char_prototype(const ObString &str,
                                    HANDLE_FUNC &func,
                                    bool ignore_convert_failed = false,
                                    bool stop_when_truncated = false,
                                    int64_t *truncated_len = NULL)
  {
    int ret = OB_SUCCESS;
    const char* begin = str.ptr();
    const char* end = str.ptr() + str.length();
    int64_t step = 0;
    ob_wc_t unicode = -1;
    int32_t replace_wc = 0;
    for (; OB_SUCC(ret) && begin < end; begin += step) {
      if (DO_DECODE) {
        step = ob_charset_decode_unicode<CS_TYPE>(pointer_cast<const unsigned char*>(begin), pointer_cast<const unsigned char*>(end), unicode);
      } else {
        step = ob_charset_char_len<CS_TYPE>(pointer_cast<const unsigned char*>(begin), pointer_cast<const unsigned char*>(end));
      }
      if (OB_UNLIKELY(step <= 0)) {
        if (ignore_convert_failed && !(stop_when_truncated && step <= OB_CS_TOOSMALL)) {
          ret = OB_SUCCESS;
          step = 1;
          unicode = -1;
        } else if (step <= OB_CS_TOOSMALL) {
          ret = OB_ERR_DATA_TRUNCATED;
          if (OB_NOT_NULL(truncated_len)) {
            *truncated_len = end - begin;
          }
        } else {
          ret = OB_ERR_INCORRECT_STRING_VALUE;
        }
      }
      if (OB_SUCC(ret)) {
        ret = func(ObString(step, begin), unicode);
      }
    }
    return ret;
  }

  /*
   * Scan the input string which encoded by a given charset
   * During the scan, each character will be decoded and converted to unicode,
   * and then processed by the template function HANDLE_FUNC
   * HANDLE_FUNC is a function like int (*HANDLE_FUNC)(const ObString &encoded_char, const ob_wc_t &unicode_value)
   * If the convert_unicode set to false, "converted to unicode" is disabled which save cpus
   * Return OB_ERR_DATA_TRUNCATED if the last character is not completed, and truncated_len will be returned
   * Return OB_ERR_INCORRECT_STRING_VALUE if convert to unicode failed
   * Unit tests can be found at test_charset.cpp
   */
  template<typename HANDLE_FUNC>
  static int foreach_char(const ObString &str,
                          const ObCharsetType cs_type,
                          HANDLE_FUNC &func,
                          bool convert_unicode = true,
                          bool ignore_convert_failed = false,
                          bool stop_when_truncated = false,
                          int64_t *truncated_len = NULL)
  {
    int ret = OB_SUCCESS;
    switch (cs_type) {
    case CHARSET_UTF8MB4:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_UTF8MB4, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_UTF8MB4, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    case CHARSET_GBK:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_GBK, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_GBK, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    case CHARSET_GB18030:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_GB18030, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_GB18030, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    case CHARSET_GB18030_2022:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_GB18030_2022, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_GB18030, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    case CHARSET_UTF16:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_UTF16, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_UTF16, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    case CHARSET_LATIN1:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_LATIN1, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_LATIN1, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    case CHARSET_BINARY:
      ret = convert_unicode ?
            foreach_char_prototype<CHARSET_BINARY, HANDLE_FUNC, true>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len)
          : foreach_char_prototype<CHARSET_BINARY, HANDLE_FUNC, false>(str, func, ignore_convert_failed, stop_when_truncated, truncated_len);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  template<ObCharsetType CS_TYPE>
  struct Encoder {
    Encoder(char *buf, const int64_t buf_len, int64_t &pos, const ob_wc_t replaced_char) :
      ptr_(buf), end_(buf + buf_len), pos_(pos), replaced_char_(replaced_char)
    {}
    inline int operator()(const ObString &encoded_char, const ob_wc_t &unicode) {
      int ret = OB_SUCCESS;
      int write_len = ob_charset_encode_unicode<CS_TYPE>(unicode,
            pointer_cast<unsigned char *>(ptr_), pointer_cast<unsigned char *>(end_));
      if (write_len <= 0) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        pos_ += write_len;
        ptr_ += write_len;
      }
      return ret;
    }
    char *ptr_;
    char *end_;
    int64_t &pos_;
    ob_wc_t replaced_char_;
  };

  static int convert_charset(const ObString &str,
                             ObCollationType src_coll_type,
                             ObCollationType out_coll_type,
                             char *buf,
                             int64_t buf_len,
                             int64_t &pos,
                             const bool trim_incomplete_tail = true,
                             const bool report_error = true,
                             const ob_wc_t replaced_char = '?')
  {
    int ret = OB_SUCCESS;
    ObCharsetType in_cs_type = ObCharset::charset_type_by_coll(src_coll_type);
    ObCharsetType out_cs_type = ObCharset::charset_type_by_coll(out_coll_type);
    int64_t truncated_len = 0;
    bool stop_when_truncated = false;
    switch (out_cs_type) {
      case CHARSET_UTF8MB4: {
        Encoder<CHARSET_UTF8MB4> encoder(buf, buf_len, pos, replaced_char);
        ret = foreach_char(str, in_cs_type, encoder, true, !report_error, stop_when_truncated, &truncated_len);
        break;
      }
      case CHARSET_GBK: {
        Encoder<CHARSET_GBK> encoder(buf, buf_len, pos, replaced_char);
        ret = foreach_char(str, in_cs_type, encoder, true, !report_error, stop_when_truncated, &truncated_len);
        break;
      }
      case CHARSET_GB18030: {
        Encoder<CHARSET_GB18030> encoder(buf, buf_len, pos, replaced_char);
        ret = foreach_char(str, in_cs_type, encoder, true, !report_error, stop_when_truncated, &truncated_len);
        break;
      }
      case CHARSET_GB18030_2022: {
        Encoder<CHARSET_GB18030_2022> encoder(buf, buf_len, pos, replaced_char);
        ret = foreach_char(str, in_cs_type, encoder, true, !report_error, stop_when_truncated, &truncated_len);
        break;
      }
      case CHARSET_UTF16: {
        Encoder<CHARSET_UTF16> encoder(buf, buf_len, pos, replaced_char);
        ret = foreach_char(str, in_cs_type, encoder, true, !report_error, stop_when_truncated, &truncated_len);
        break;
      }
      default: {
        uint32_t result_len;
        ret = ObCharset::charset_convert(src_coll_type, str.ptr(), str.length(),
                                         out_coll_type, buf, buf_len,
                                         result_len, trim_incomplete_tail, report_error, replaced_char);
        pos = result_len;
        break;
      }
    }
    //handle_incomplete_tail
    if (OB_ERR_DATA_TRUNCATED == ret && truncated_len > 0) {
      if (!report_error || trim_incomplete_tail) {
        ret = OB_SUCCESS;
        if (!trim_incomplete_tail) {
          int32_t tmp_len = 0;
          if (pos + ObCharset::MAX_MB_LEN >= buf_len) {
            ret = OB_SIZE_OVERFLOW;
          } else if (OB_FAIL(ObCharset::wc_mb(out_coll_type, replaced_char, buf+pos, buf_len-pos, tmp_len))) {
          } else {
            pos += tmp_len;
          }
        }
      }
    }
    return ret;
  }
};


}
}



#endif // OB_CHARSET_STRING_HELPER_H
