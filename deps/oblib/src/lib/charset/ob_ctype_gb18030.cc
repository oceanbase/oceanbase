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
#include "lib/charset/ob_charset.h"
#include "lib/charset/ob_gb18030_2022_tab.h"
#include "lib/charset/ob_cypte_gb18030_tab.h"
#define is_mb_1(c) ((unsigned char)(c) <= 0x7F)
#define is_mb_odd(c) (0x81 <= (unsigned char)(c) && (unsigned char)(c) <= 0xFE)
#define is_mb_even_2(c)                          \
  ((0x40 <= (unsigned char)(c) && (unsigned char)(c) <= 0x7E) || \
   (0x80 <= (unsigned char)(c) && (unsigned char)(c) <= 0xFE))
#define is_mb_even_4(c) (0x30 <= (unsigned char)(c) && (unsigned char)(c) <= 0x39)

static const unsigned int MIN_MB_ODD_BYTE = 0x81;
static const unsigned int MIN_MB_EVEN_BYTE_2 = 0x40;
static const unsigned int MIN_MB_EVEN_BYTE_4 = 0x30;

static const unsigned int MAX_GB18030_DIFF = 0x18398F;


static const unsigned int UNI2_TO_GB4_DIFF = 7456;

static const unsigned int UNICASE_4_BYTE_OFFSET = 0x80;

static const unsigned int MIN_2_BYTE_UNICASE = 0xA000;
static const unsigned int MAX_2_BYTE_UNICASE = 0xDFFF;

static const unsigned int MIN_3_BYTE_FROM_UNI = 0x2E600;
static const unsigned int MAX_3_BYTE_FROM_UNI = 0x2E6FF;

static const unsigned int PINYIN_2_BYTE_START = 0x8140;
static const unsigned int PINYIN_2_BYTE_END = 0xFE9F;

static const unsigned int PINYIN_4_BYTE_1_START = 0x8138FD38;
static const unsigned int PINYIN_4_BYTE_1_END = 0x82359232;
static const unsigned int PINYIN_4_1_DIFF = 11328;

static const unsigned int PINYIN_4_BYTE_2_START = 0x95328236;
static const unsigned int PINYIN_4_BYTE_2_END = 0x98399836;
static const unsigned int PINYIN_4_2_DIFF = 254536;

static const unsigned int CHINESE_WEIGHT_BASE = 0xFFA00000;
static const unsigned int COMMON_WEIGHT_BASE = 0xFF000000;

typedef unsigned int(*GET_CHS_WEIGHT_FUNC)(unsigned int);


enum CASESENSITIVITY {
  INSENSITIVE = 0,
  SENSITIVE = 1,
};


static unsigned int diff_to_gb18030_4(unsigned char *dst, unsigned int dstlen, unsigned int diff) {
  ob_charset_assert(dstlen >= 4);

  if (diff > MAX_GB18030_DIFF || dstlen < 4) return 0;

  dst[3] = (unsigned char)(diff % 10) + MIN_MB_EVEN_BYTE_4;
  diff /= 10;
  dst[2] = (unsigned char)(diff % 126) + MIN_MB_ODD_BYTE;
  diff /= 126;
  dst[1] = (unsigned char)(diff % 10) + MIN_MB_EVEN_BYTE_4;
  dst[0] = (unsigned char)(diff / 10) + MIN_MB_ODD_BYTE;

  return 4;
}

static unsigned int gb18030_4_code_to_diff(unsigned int code) {
  unsigned int diff = 0;

  ob_charset_assert(is_mb_odd((code >> 24) & 0xFF));
  diff += ((code >> 24) & 0xFF) - MIN_MB_ODD_BYTE;
  diff *= 10;
  ob_charset_assert(is_mb_even_4((code >> 16) & 0xFF));
  diff += ((code >> 16) & 0xFF) - MIN_MB_EVEN_BYTE_4;
  diff *= 126;
  ob_charset_assert(is_mb_odd((code >> 8) & 0xFF));
  diff += ((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE;
  diff *= 10;
  ob_charset_assert(is_mb_even_4(code & 0xFF));
  diff += (code & 0xFF) - MIN_MB_EVEN_BYTE_4;

  return diff;
}

static unsigned int ob_ismbchar_gb18030(const ObCharsetInfo *cs __attribute__((unused)),
                                const char *p, const char *e) {
  ob_charset_assert(e > p);

  if (e - p <= 1 || !is_mb_odd(p[0])) {
    return 0;
  } else if (is_mb_even_2(p[1])) {
    return 2;
  } else if (e - p > 3 && is_mb_even_4(p[1]) && is_mb_odd(p[2]) &&
           is_mb_even_4(p[3])) {
    return 4;
  }

  return 0;
}

static inline unsigned int gb18030_chs_to_code(const unsigned char *src, size_t srclen) {
  unsigned int r = 0;

  ob_charset_assert(srclen == 1 || srclen == 2 || srclen == 4);

  switch (srclen) {
    case 1:
      r = src[0];
      break;
    case 2:
      r = (src[0] << 8) + src[1];
      break;
    case 4:
      r = (src[0] << 24) + (src[1] << 16) + (src[2] << 8) + src[3];
      break;
    default:
      ob_charset_assert(0);
  }

  return r;
}

static size_t code_to_gb18030_chs(unsigned char *dst, size_t dstlen, unsigned int code) {
  size_t i, len = 0;
  unsigned char *dst_end = dst + dstlen;
  unsigned char r[4];
  for (i = 0; code != 0; i++, code >>= 8) r[i] = (unsigned char)(code & 0xFF);

  ob_charset_assert(i == 1 || i == 2 || i == 4);
  for (; i > 0 && dst < dst_end; --i, ++len) *dst++ = r[i - 1];

  return len;
}

static unsigned int ob_mbcharlen_gb18030(const ObCharsetInfo *cs __attribute__((unused)),
                                 unsigned int c) {
  if (c <= 0xFF) {
    return !is_mb_odd(c);
  } else if (c > 0xFFFF || !is_mb_odd((c >> 8) & 0xFF)) {
    return 0;
  } else if (is_mb_even_2((c & 0xFF))) {
    return 2;
  } else if (is_mb_even_4((c & 0xFF))) {
    return 4;
  }
  return 0;
}

static size_t ob_well_formed_len_gb18030(
    const ObCharsetInfo *cs __attribute__((unused)), const char *b, const char *e,
    size_t pos, int *error) {
  const char *b0 = b;
  const char *emb = e - 1;

  *error = 0;

  while (pos-- && b < e) {
    if (is_mb_1((unsigned char)b[0])) {
      ++b;
    } else if (b < emb && is_mb_odd(b[0]) && is_mb_even_2(b[1])) {
      b += 2;
    } else if (b + 2 < emb && is_mb_odd(b[0]) && is_mb_even_4(b[1]) &&
             is_mb_odd(b[2]) && is_mb_even_4(b[3])) {
      b += 4;
    } else {
      *error = 1;
      break;
    }
  }

  return (size_t)(b - b0);
}

static inline unsigned int gb18030_4_chs_to_diff(const unsigned char *src) {
  return (src[0] - MIN_MB_ODD_BYTE) * 12600 +
         (src[1] - MIN_MB_EVEN_BYTE_4) * 1260 +
         (src[2] - MIN_MB_ODD_BYTE) * 10 + (src[3] - MIN_MB_EVEN_BYTE_4);
}

static int ob_mb_wc_gb18030(const ObCharsetInfo *cs __attribute__((unused)),
                            ob_wc_t *pwc, const unsigned char *s, const unsigned char *e) {
  unsigned int idx = 0;
  unsigned int cp = 0;

  if (s >= e) {
    return OB_CS_TOOSMALL;
  } else if (is_mb_1(s[0])) {
    *pwc = s[0];
    return 1;
  } else if (!is_mb_odd(s[0])) {
    return OB_CS_ILSEQ;
  } else if (s + 2 > e) {
    return OB_CS_TOOSMALL2;
  } else if (is_mb_even_2(s[1])) {
    idx = (s[0] - MIN_MB_ODD_BYTE) * 192 + (s[1] - MIN_MB_EVEN_BYTE_2);
    *pwc = tab_gb18030_2_uni[idx];
    return (*pwc == 0) ? OB_CS_ILSEQ : 2;
  } else if (is_mb_even_4(s[1])) {
    if (s + 4 > e) return OB_CS_TOOSMALL4;

    if (!(is_mb_odd(s[2]) && is_mb_even_4(s[3]))) return OB_CS_ILSEQ;

    idx = gb18030_4_chs_to_diff(s);

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
    } else {
      ob_charset_assert(0);
    }

    *pwc = cp;
    return 4;
  } else
    return OB_CS_ILSEQ;
}

static int ob_wc_mb_gb18030_chs(const ObCharsetInfo *cs __attribute__((unused)),
                                ob_wc_t wc, unsigned char *s, unsigned char *e) {
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
      err = diff_to_gb18030_4(s, 4, idx);
      ob_charset_assert(err != 0);
      return err != 0 ? len : OB_CS_ILUNI;
  }

  ob_charset_assert(0);
  return OB_CS_ILUNI;
}

static const ObUnicaseInfoChar *get_case_info(const ObCharsetInfo *cs,
                                                 const unsigned char *src,
                                                 size_t srclen) {
  const ObUnicaseInfoChar *p = NULL;
  unsigned int diff, code;

  ob_charset_assert(cs != NULL);

  switch (srclen) {
    case 1:
      return &cs->caseinfo->page[0][(unsigned char)src[0]];
    case 2:
      if (src[0] < ((MIN_2_BYTE_UNICASE >> 8) & 0xFF) ||
          src[0] > ((MAX_2_BYTE_UNICASE >> 8) & 0xFF))
        return NULL;

      p = cs->caseinfo->page[(unsigned char)src[0]];
      return p ? &p[(unsigned char)src[1]] : NULL;
    case 4:
      diff = gb18030_4_chs_to_diff(src);
      code = 0;

      if (diff < MIN_2_BYTE_UNICASE - UNICASE_4_BYTE_OFFSET)
        code = diff + UNICASE_4_BYTE_OFFSET;
      else if (diff >= MIN_3_BYTE_FROM_UNI && diff <= MAX_3_BYTE_FROM_UNI)
        code = (diff & 0xFFFF);
      else
        return NULL;

      p = cs->caseinfo->page[(code >> 8) & 0xFF];
      return p ? &p[code & 0xFF] : NULL;
  }

  ob_charset_assert(0);
  return NULL;
}

static unsigned int case_info_code_to_gb18030(unsigned int code) {
  if ((code >= MIN_2_BYTE_UNICASE && code <= MAX_2_BYTE_UNICASE) ||
      code < UNICASE_4_BYTE_OFFSET) {
    return code;
  } else {
    unsigned int r;
    unsigned char gbchs[4];

    if (code >= UNICASE_4_BYTE_OFFSET && code < MIN_2_BYTE_UNICASE) {
      code -= UNICASE_4_BYTE_OFFSET;
    } else if (code >= (MIN_3_BYTE_FROM_UNI & 0xFFFF) &&
             code <= (MAX_3_BYTE_FROM_UNI & 0xFFFF))
      code += (MIN_3_BYTE_FROM_UNI & 0xFF0000);
    else
      ob_charset_assert(0);

    r = diff_to_gb18030_4(gbchs, 4, code);
    ob_charset_assert(r == 4);

    return r == 4 ? gb18030_chs_to_code(gbchs, 4) : 0;
  }
}

static unsigned int get_casefolded_code(const ObCharsetInfo *cs, const unsigned char *src,
                                size_t srclen, size_t is_upper) {
  const ObUnicaseInfoChar *ch = get_case_info(cs, src, srclen);

  ob_charset_assert(srclen == 1 || srclen == 2 || srclen == 4);

  return ch ? case_info_code_to_gb18030(is_upper ? ch->toupper : ch->tolower)
            : 0;
}

static size_t ob_casefold_gb18030(const ObCharsetInfo *cs, char *src,
                                  size_t srclen, char *dst, size_t dstlen,
                                  const unsigned char *map, bool is_upper) {
  char *srcend = src + srclen;
  char *dst0 = dst;
  char *dst_end = dst + dstlen;

  while (src < srcend) {
    unsigned int mblen = ob_ismbchar_gb18030(cs, src, srcend);

    ob_charset_assert(dst < dst_end);
    if (mblen) {
      unsigned int code = get_casefolded_code(cs, (unsigned char *)src, mblen, is_upper);

      if (code != 0) {
        size_t mblen_dst =
            code_to_gb18030_chs((unsigned char *)dst, dst_end - dst, code);

        ob_charset_assert(dst + mblen_dst <= dst_end);
        src += mblen;
        dst += mblen_dst;
      } else {
        ob_charset_assert(mblen == 2 || mblen == 4);
        ob_charset_assert(dst + mblen <= dst_end);

        if (mblen == 4) {
          *dst++ = *src++;
          *dst++ = *src++;
        }

        *dst++ = *src++;
        *dst++ = *src++;
      }
    } else
      *dst++ = (char)map[(unsigned char)(*src++)];
  }

  return (size_t)(dst - dst0);
}

static size_t ob_caseup_gb18030(const ObCharsetInfo *cs, char *src,
                                size_t srclen, char *dst, size_t dstlen) {
  ob_charset_assert(cs != NULL);
  ob_charset_assert(src != dst || cs->caseup_multiply == 1);
  ob_charset_assert(dstlen >= srclen * cs->caseup_multiply);
  return ob_casefold_gb18030(cs, src, srclen, dst, dstlen, cs->to_upper, 1);
}

static size_t ob_casedn_gb18030(const ObCharsetInfo *cs, char *src,
                                size_t srclen, char *dst, size_t dstlen) {
  ob_charset_assert(cs != NULL);
  ob_charset_assert(src != dst || cs->casedn_multiply == 1);
  ob_charset_assert(dstlen >= srclen * cs->casedn_multiply);
  return ob_casefold_gb18030(cs, src, srclen, dst, dstlen, cs->to_lower, 0);
}

static unsigned int get_weight_if_chinese_character(unsigned int code) {
  if (code >= PINYIN_2_BYTE_START && code <= PINYIN_2_BYTE_END) {
    unsigned int idx = (((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE) * 0xBE + (code & 0xFF) -
               MIN_MB_EVEN_BYTE_2;
    if ((code & 0xFF) > 0x7F) idx -= 0x01;

    return CHINESE_WEIGHT_BASE + gb18030_2_weight_py[idx];
  } else if (code >= PINYIN_4_BYTE_1_START && code <= PINYIN_4_BYTE_1_END) {
    unsigned int idx = gb18030_4_code_to_diff(code) - PINYIN_4_1_DIFF;
    return CHINESE_WEIGHT_BASE + gb18030_4_weight_py_p1[idx];
  } else if (code >= PINYIN_4_BYTE_2_START && code <= PINYIN_4_BYTE_2_END) {
    unsigned int idx = gb18030_4_code_to_diff(code) - PINYIN_4_2_DIFF;
    return CHINESE_WEIGHT_BASE + gb18030_4_weight_py_p2[idx];
  }

  return CHINESE_WEIGHT_BASE;
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static unsigned int get_weight_for_mbchar(const ObCharsetInfo *cs, const unsigned char *src,
                                  size_t mblen) {
  unsigned int weight, caseup_code, code = gb18030_chs_to_code(src, mblen);
  ob_charset_assert(mblen == 2 || mblen == 4);
  if (code == 0xFE39FE39) return 0xFFFFFFFF;
  weight = GET_CHS_WEIGHT(code);
  if (weight > CHINESE_WEIGHT_BASE) return weight;
  if (CASESENSITIVE == INSENSITIVE) {
    caseup_code = get_casefolded_code(cs, src, mblen, 1);
    if (caseup_code == 0) caseup_code = code;
    code = caseup_code;
  }
  weight = (code <= 0xFFFF)
               ? code
               : COMMON_WEIGHT_BASE + gb18030_4_code_to_diff(code);
  return weight;
}

template<GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static int ob_strnncoll_gb18030_internal(const ObCharsetInfo *cs,
                                         const unsigned char **s_res, size_t s_length,
                                         const unsigned char **t_res, size_t t_length) {
  const unsigned char *s = *s_res;
  const unsigned char *t = *t_res;
  const unsigned char *se = s + s_length;
  const unsigned char *te = t + t_length;

  ob_charset_assert(cs != NULL);

  while (s < se && t < te) {
    unsigned int mblen_s = ob_ismbchar_gb18030(cs, (char *)s, (char *)se);
    unsigned int mblen_t = ob_ismbchar_gb18030(cs, (char *)t, (char *)te);

    if (mblen_s > 0 && mblen_t > 0) {
      unsigned int code_s = get_weight_for_mbchar<GET_CHS_WEIGHT, CASESENSITIVE>(cs, s, mblen_s);
      unsigned int code_t = get_weight_for_mbchar<GET_CHS_WEIGHT, CASESENSITIVE>(cs, t, mblen_t);

      if (code_s != code_t) return code_s > code_t ? 1 : -1;

      s += mblen_s;
      t += mblen_t;
    } else if (mblen_s == 0 && mblen_t == 0) {
      unsigned char so = cs->sort_order[*s++], to = cs->sort_order[*t++];
      if (so != to) return (int)(so - to);
    } else {
      return mblen_s == 0 ? -1 : 1;
    }
  }

  *s_res = s;
  *t_res = t;
  return 0;
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static int ob_strnncoll_gb18030(const ObCharsetInfo *cs, const unsigned char *s,
                                size_t s_length, const unsigned char *t,
                                size_t t_length, bool t_is_prefix) {
  int res = ob_strnncoll_gb18030_internal<GET_CHS_WEIGHT, CASESENSITIVE>(cs, &s, s_length, &t, t_length);
  if (res != 0) {
    return res;
  } else if (t_is_prefix && s_length > t_length) {
    return 0;
  }
  return (int)(s_length - t_length);
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static int ob_strnncollsp_gb18030(const ObCharsetInfo *cs, const unsigned char *s,
                                  size_t s_length, const unsigned char *t,
                                  size_t t_length, bool diff_if_only_endspace_difference) {
  const unsigned char *se = s + s_length, *te = t + t_length;
  int res = ob_strnncoll_gb18030_internal<GET_CHS_WEIGHT, CASESENSITIVE>(cs, &s, s_length, &t, t_length);

  if (!res && (s != se || t != te)) {
    int swap = 1;
    if (diff_if_only_endspace_difference) {
      return s_length < t_length ? -1 : 1;
    } else if (s_length < t_length) {
      s = t;
      se = te;
      swap = -1;
      res = -res;
    }

    for (; s < se; s++) {
      if (*s != 0x20) return (*s < 0x20) ? -swap : swap;
    }
  }

  return res;
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static size_t ob_strnxfrm_gb18030(const ObCharsetInfo *cs, unsigned char *dst,
                                  size_t dstlen, unsigned int nweights,
                                  const unsigned char *src, size_t srclen, unsigned int flags,
                                  bool *is_valid_unicode) {
  unsigned char *ds = dst;
  unsigned char *de = dst + dstlen;
  const unsigned char *se = src + srclen;
  const unsigned char *sort_order;
  *is_valid_unicode = 1;
  ob_charset_assert(cs != NULL);
  sort_order = cs->sort_order;

  for (; dst < de && src < se && nweights; nweights--) {
    unsigned int mblen = cs->cset->ismbchar(cs, (const char *)src, (const char *)se);

    if (mblen > 0) {
      unsigned int weight = get_weight_for_mbchar<GET_CHS_WEIGHT, CASESENSITIVE>(cs, src, mblen);
      dst += code_to_gb18030_chs(dst, de - dst, weight);
      src += mblen;
    } else {
      *is_valid_unicode = is_valid_ascii(*src);
      *dst++ = sort_order ? sort_order[*src] : *src;
      ++src;
    }
  }
  return ob_strxfrm_pad_desc_and_reverse(cs, ds, dst, de, nweights, flags, 0);
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT>
size_t ob_varlen_encoding_gb18030_for_memcmp(const struct ObCharsetInfo* cs,
                              unsigned char* dst, size_t dst_len, unsigned int nweights,
                              const unsigned char *src, size_t src_len,
                              bool *is_valid_unicode)
{
  unsigned char *dst0 = dst;
  unsigned char *de = dst + dst_len;
  const unsigned char *se = src + src_len;
  const unsigned char *sort_order;
  *is_valid_unicode = 1;
  ob_charset_assert(cs != NULL);
  sort_order = cs->sort_order;
  for (; *is_valid_unicode && dst < de && src < se && nweights; nweights--) {
    unsigned int mblen = cs->cset->ismbchar(cs, (const char *)src, (const char *)se);
    unsigned int weight = 0;
    if (mblen > 0) {
      weight = get_weight_for_mbchar<GET_CHS_WEIGHT, INSENSITIVE>(cs, src, mblen);
      dst += code_to_gb18030_chs(dst, de - dst, weight);
      src += mblen;
    } else {
      *is_valid_unicode = is_valid_ascii(*src);
      *dst++ = sort_order ? sort_order[*src] : *src;
      ++src;
    }
  }
  memset(dst, 0x00, 8);
  dst += 8;
  return dst - dst0;
}
uint16_t find_space_char_count_gb18030(const unsigned char* src, const unsigned char* se)
{
  int space_cnt = 1;
  while ((src + space_cnt) < se && *(src + space_cnt) == 0x20 ) space_cnt++;
  if ((src + space_cnt) < se) return space_cnt;
  else return 0;
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT>
size_t ob_varlen_encoding_gb18030_for_spacecmp(const struct ObCharsetInfo* cs,
                              unsigned char* dst, size_t dst_len, unsigned int nweights,
                              const unsigned char *src, size_t src_len,
                              bool *is_valid_unicode)
{
  unsigned char *dst0 = dst;
  unsigned char *de = dst + dst_len;
  const unsigned char *se = src + src_len;
  const unsigned char *sort_order;
  *is_valid_unicode = 1;
  ob_charset_assert(cs != NULL);
  sort_order = cs->sort_order;
  uint16_t space_cnt = 0xFFFF;
  for (; *is_valid_unicode && dst < de && src < se && nweights; nweights--) {
    // for reslovable multiple bytes, only space's first byte is 0x20,
    // in gb18030 encoding scheme
    if (*src == 0x20) {
      space_cnt = find_space_char_count_gb18030(src, se);
      if (space_cnt == 0) break;
      memset(dst, 0x00, 8);
      *(dst+3) = 0x20;
      if (*(src+space_cnt) > 0x20){
        *(dst+7)=0x21;
        // flip
        uint16_t tmp_cnt = space_cnt ^ 0xFFFF;
        *(dst+8)=*((unsigned char*)&tmp_cnt+1);
        *(dst+9)=*(&tmp_cnt);
      } else {
        *(dst+7) = 0x19;
        *(dst+8)=*((unsigned char*)&space_cnt+1);
        *(dst+9)=*(&space_cnt);
      }
      dst += 10;
      src += space_cnt;
    }
    unsigned int mblen = cs->cset->ismbchar(cs, (const char *)src, (const char *)se);
    unsigned int weight = 0;
    if (mblen > 0) {
      weight = get_weight_for_mbchar<GET_CHS_WEIGHT, INSENSITIVE>(cs, src, mblen);
      dst += code_to_gb18030_chs(dst, de - dst, weight);
      src += mblen;
    } else {
      *is_valid_unicode = is_valid_ascii(*src);
      *dst++ = sort_order ? sort_order[*src] : *src;
      ++src;
    }
  }
  // adds 0x20, 0x20
  memset(dst, 0x00, 8);
  *(dst+3) = 0x20;
  *(dst+7) = 0x20;
  dst += 8;
  return dst - dst0;
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT>
size_t ob_strnxfrm_gb18030_varlen(const struct ObCharsetInfo* cs,
                             unsigned char* dst, size_t dst_len, unsigned int nweights,
                             const unsigned char *src, size_t srclen,
                             bool is_memcmp, bool *is_valid_unicode)
{
  if (is_memcmp) {
    return ob_varlen_encoding_gb18030_for_memcmp<GET_CHS_WEIGHT>(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode);
  } else {
    return ob_varlen_encoding_gb18030_for_spacecmp<GET_CHS_WEIGHT>(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode);
  }
}

static unsigned int unicode_to_gb18030_code(const ObCharsetInfo *cs, int unicode) {
  unsigned char dst[4];
  unsigned int dst_len;
  int res;

  ob_charset_assert(cs != NULL);

  res = cs->cset->wc_mb(cs, unicode, dst, dst + 4);

  ob_charset_assert(res == 1 || res == 2 || res == 4);

  dst_len = (unsigned int)res;
  return gb18030_chs_to_code(dst, dst_len);
}

static size_t get_code_and_length(const ObCharsetInfo *cs, const char *s,
                                  const char *e, size_t *code) {
  size_t len;

  if (s >= e)  {
    return 0;
  } else if (is_mb_1(s[0])) {
    *code = s[0];
    return 1;
  } else if ((len = ob_ismbchar_gb18030(cs, s, e)) == 0) {
    return 0;
  }

  ob_charset_assert(len == 2 || len == 4);
  *code = gb18030_chs_to_code((const unsigned char *)s, len);
  return len;
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static unsigned int get_weight_for_gb18030_chs(const ObCharsetInfo *cs, const char *s,
                                       size_t s_len) {
  ob_charset_assert(s_len == 1 || s_len == 2 || s_len == 4);

  if (s_len == 1) {
    ob_charset_assert(is_mb_1(*s));
    return cs->sort_order[(unsigned char)*s];
  }

  return get_weight_for_mbchar<GET_CHS_WEIGHT, CASESENSITIVE>(cs, (const unsigned char *)s, s_len);
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static int ob_wildcmp_gb18030_impl(const ObCharsetInfo *cs, const char *str,
                                   const char *str_end, const char *wild_str,
                                   const char *wild_end, unsigned int escape_char, unsigned int w_one,
                                   unsigned int w_many, int recurse_level) {
  int result = -1;
  size_t s_gb, w_gb;
  size_t s_len = 0, w_len;



  while (wild_str != wild_end) {
    while (TRUE) {
      bool escaped = 0;
      if ((w_len = get_code_and_length(cs, wild_str, wild_end, &w_gb)) == 0)
        return 1;

      if (w_gb != escape_char && w_gb == w_many) {
        result = 1;
        break;
      }

      wild_str += w_len;
      if (w_gb == escape_char && wild_str < wild_end) {
        if ((w_len = get_code_and_length(cs, wild_str, wild_end, &w_gb)) == 0)
          return 1;

        wild_str += w_len;
        escaped = 1;
      }

      if ((s_len = get_code_and_length(cs, str, str_end, &s_gb)) == 0) return 1;
      str += s_len;

      if (!escaped && w_gb == w_one) {
        result = 1;
      } else {
        s_gb = get_weight_for_gb18030_chs<GET_CHS_WEIGHT, CASESENSITIVE>(cs, str - s_len, s_len);
        w_gb = get_weight_for_gb18030_chs<GET_CHS_WEIGHT, CASESENSITIVE>(cs, wild_str - w_len, w_len);
        if (s_gb != w_gb) return 1; /* No match */
      }

      if (wild_str == wild_end)
        return (str != str_end);
    }

        if (w_gb == w_many) {

      for (; wild_str != wild_end;) {
        if ((w_len = get_code_and_length(cs, wild_str, wild_end, &w_gb)) == 0) {
          return 1;
        } else if (w_gb == w_many) {
          wild_str += w_len;
          continue;
        } else if (w_gb == w_one) {
          wild_str += w_len;

          if ((s_len = get_code_and_length(cs, str, str_end, &s_gb)) == 0)
            return 1;
          str += s_len;
          continue;
        } else
        break;
      }

      if (wild_str == wild_end) return 0;
      else if (str == str_end) return -1;
      else if ((w_len = get_code_and_length(cs, wild_str, wild_end, &w_gb)) == 0)
        return 1;
      else wild_str += w_len;

      if (w_gb == escape_char) {
        if (wild_str < wild_end) {
          if ((w_len = get_code_and_length(cs, wild_str, wild_end, &w_gb)) == 0) {
            return 1;
          } else {
            wild_str += w_len;
          }
        }
      }

      while (TRUE) {
        while (str != str_end) {
          if ((s_len = get_code_and_length(cs, str, str_end, &s_gb)) == 0) {
            return 1;
          }

          s_gb = get_weight_for_gb18030_chs<GET_CHS_WEIGHT, CASESENSITIVE>(cs, str, s_len);
          w_gb = get_weight_for_gb18030_chs<GET_CHS_WEIGHT, CASESENSITIVE>(cs, wild_str - w_len, w_len);
          if (s_gb == w_gb) {
              break;
          } else  {
          str += s_len;
        }
        }

        if (str == str_end) {
            return -1;
        } else {
        str += s_len;
        }

        result = ob_wildcmp_gb18030_impl<GET_CHS_WEIGHT, CASESENSITIVE>(cs, str, str_end, wild_str, wild_end, escape_char,
                                    w_one, w_many, recurse_level + 1);
        if (result <= 0) return result;
      }
    }
  }

  return (str != str_end ? 1 : 0);
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static int ob_wildcmp_gb18030(const ObCharsetInfo *cs, const char *str,
                              const char *str_end, const char *wild_str,
                              const char *wild_end, int escape_char, int w_one,
                              int w_many) {
  unsigned int escape_gb, w_one_gb, w_many_gb;

  escape_gb = unicode_to_gb18030_code(cs, escape_char);
  w_one_gb = unicode_to_gb18030_code(cs, w_one);
  w_many_gb = unicode_to_gb18030_code(cs, w_many);

  return ob_wildcmp_gb18030_impl<GET_CHS_WEIGHT, CASESENSITIVE>(cs, str, str_end, wild_str, wild_end, escape_gb,
                                 w_one_gb, w_many_gb, 1);
}

template <GET_CHS_WEIGHT_FUNC GET_CHS_WEIGHT, CASESENSITIVITY CASESENSITIVE>
static void ob_hash_sort_gb18030(const ObCharsetInfo *cs, const unsigned char *s,
                                 size_t slen, ulong *n1, ulong *n2,
                                 const bool calc_end_space, hash_algo hash_algo) {
  const unsigned char *e = s + slen;
  unsigned long int tmp1, tmp2;
  size_t len;
  size_t s_gb;
  unsigned int ch;

  int length = 0;
  unsigned char data[HASH_BUFFER_LENGTH];

  if (!calc_end_space) {
    while (e > s && e[-1] == 0x20) e--;
  }

  tmp1 = *n1;
  tmp2 = *n2;

  if (NULL == hash_algo) {
    while ((len = get_code_and_length(cs, (const char *)s, (const char *)e,
                                      &s_gb)) != 0) {
      s_gb = get_weight_for_gb18030_chs<GET_CHS_WEIGHT, CASESENSITIVE>(cs, (const char *)s, len);

      ch = (s_gb & 0xFF);
      tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
      tmp2 += 3;

      ch = (s_gb >> 8) & 0xFF;
      tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
      tmp2 += 3;

      ch = (s_gb >> 16) & 0xFF;
      tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
      tmp2 += 3;

      ch = (s_gb >> 24) & 0xFF;
      tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
      tmp2 += 3;

      s += len;
    }
  } else {
    while ((len = get_code_and_length(cs, (const char *)s, (const char *)e,
                                      &s_gb)) != 0) {
      s_gb = get_weight_for_gb18030_chs<GET_CHS_WEIGHT, CASESENSITIVE>(cs, (const char *)s, len);
      if (length > HASH_BUFFER_LENGTH - 4) {
        tmp1 = hash_algo((void*) &data, length, tmp1);
        length = 0;
      }
      memcpy(data+length, &s_gb, 4);
      length += 4;
      s+= len;
    }
    if (length > 0) {
      tmp1 = hash_algo((void*) &data, length, tmp1);
    }
  }

  *n1 = tmp1;
  *n2 = tmp2;
}

static ObCollationHandler ob_collation_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_weight_if_chinese_character, INSENSITIVE>,
  ob_strnncollsp_gb18030<get_weight_if_chinese_character, INSENSITIVE>,
  ob_strnxfrm_gb18030<get_weight_if_chinese_character, INSENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_weight_if_chinese_character>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_weight_if_chinese_character, INSENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_weight_if_chinese_character, INSENSITIVE>,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_cs_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_weight_if_chinese_character, SENSITIVE>,
  ob_strnncollsp_gb18030<get_weight_if_chinese_character, SENSITIVE>,
  ob_strnxfrm_gb18030<get_weight_if_chinese_character, SENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_weight_if_chinese_character>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_weight_if_chinese_character, SENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_weight_if_chinese_character, SENSITIVE>,
  ob_propagate_simple
};

static ObCharsetHandler ob_charset_gb18030_handler = {
    ob_ismbchar_gb18030,
    ob_mbcharlen_gb18030,
    ob_numchars_mb,
    ob_charpos_mb,
    ob_max_bytes_charpos_mb,
    ob_well_formed_len_gb18030,
    ob_lengthsp_8bit,
    // my_numcells_mb,
    ob_mb_wc_gb18030,
    ob_wc_mb_gb18030_chs,
    ob_mb_ctype_mb,
    // my_caseup_str_mb,
    // my_casedn_str_mb,
    ob_caseup_gb18030,
    ob_casedn_gb18030,
    // my_snprintf_8bit,
    // my_long10_to_str_8bit,
    // my_longlong10_to_str_8bit,
    ob_fill_8bit,
    ob_strntol_8bit,
    ob_strntoul_8bit,
    ob_strntoll_8bit,
    ob_strntoull_8bit,
    ob_strntod_8bit,
    // my_strtoll10_8bit,
    ob_strntoull10rnd_8bit,
    ob_scan_8bit};

ObCharsetInfo ob_charset_gb18030_chinese_ci = {
    oceanbase::common::CS_TYPE_GB18030_CHINESE_CI,
    0,
    0,                                               /* number        */
    OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_STRNXFRM | OB_CS_CI,  /* state         */
    "gb18030",                                       /* cs name       */
    "gb18030_chinese_ci",                            /* name          */
    "",                                              /* comment       */
    NULL,                                            /* tailoring     */
    NULL,                                            /* coll param    */
    ctype_gb18030,                                   /* ctype         */
    to_lower_gb18030,                                /* lower         */
    to_upper_gb18030,                                /* UPPER         */
    sort_order_gb18030_ci,                              /* sort          */
    NULL,                                            /* uca           */
    // NULL,                                            /* tab_to_uni    */
    // NULL,                                            /* tab_from_uni  */
    &ob_caseinfo_gb18030,                            /* caseinfo      */
    NULL,                                            /* state_map     */
    NULL,                                            /* ident_map     */
    2,                                               /* strxfrm_multiply */
    2,                                               /* caseup_multiply  */
    2,                                               /* casedn_multiply  */
    1,                                               /* mbminlen      */
    4,                                               /* mbmaxlen      */
    2,                                               /* mbmaxlenlen   */
    0,                                               /* min_sort_char */
    0xFE39FE39,                                      /* max_sort_char */
    ' ',                                             /* pad char      */
    1, /* escape_with_backslash_is_dangerous */
    1, /* levels_for_compare */
    1,
    &ob_charset_gb18030_handler,
    &ob_collation_ci_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_gb18030_chinese_cs = {
    oceanbase::common::CS_TYPE_GB18030_CHINESE_CS,
    0,
    0,                                               /* number        */
    OB_CS_COMPILED | OB_CS_STRNXFRM,                 /* state         */
    "gb18030",                                       /* cs name       */
    "gb18030_chinese",                               /* name          */
    "",                                              /* comment       */
    NULL,                                            /* tailoring     */
    NULL,                                            /* coll param    */
    ctype_gb18030,                                   /* ctype         */
    to_lower_gb18030,                                /* lower         */
    to_upper_gb18030,                                /* UPPER         */
    sort_order_gb18030,                              /* sort          */
    NULL,                                            /* uca           */
    // NULL,                                            /* tab_to_uni    */
    // NULL,                                            /* tab_from_uni  */
    &ob_caseinfo_gb18030,                            /* caseinfo      */
    NULL,                                            /* state_map     */
    NULL,                                            /* ident_map     */
    2,                                               /* strxfrm_multiply */
    2,                                               /* caseup_multiply  */
    2,                                               /* casedn_multiply  */
    1,                                               /* mbminlen      */
    4,                                               /* mbmaxlen      */
    2,                                               /* mbmaxlenlen   */
    0,                                               /* min_sort_char */
    0xFE39FE39,                                      /* max_sort_char */
    ' ',                                             /* pad char      */
    1, /* escape_with_backslash_is_dangerous */
    1, /* levels_for_compare */
    1,
    &ob_charset_gb18030_handler,
    &ob_collation_cs_handler,
    PAD_SPACE};

ObCharsetInfo ob_charset_gb18030_bin = {
    oceanbase::common::CS_TYPE_GB18030_BIN,
    0,
    0,
    OB_CS_COMPILED | OB_CS_BINSORT,
    "gb18030",
    "gb18030_bin",
    "",
    NULL,
    NULL,
    ctype_gb18030,
    to_lower_gb18030,
    to_upper_gb18030,
    NULL,
    NULL,


    &ob_caseinfo_gb18030,
    NULL,
    NULL,
    1,
    2,
    2,
    1,
    4,
    2,                                               /* mbmaxlenlen   */
    0,
    0xFEFEFEFE,
    ' ',
    1,
    1,
    1,
    &ob_charset_gb18030_handler,
    &ob_collation_mb_bin_handler,
    PAD_SPACE};


// These four arrays will be init in ObCharset::init_charset()
uint16 tab_gb18030_2022_2_uni[sizeof(tab_gb18030_2_uni)/sizeof(uint16)];
uint16 tab_gb18030_2022_4_uni[sizeof(tab_gb18030_4_uni)/sizeof(uint16) + GB_2022_CNT_PART_1 + GB_2022_CNT_PART_2];
uint16 tab_uni_gb18030_2022_p1[sizeof(tab_uni_gb18030_p1)/sizeof(uint16) + GB_2022_CNT_PART_1];
uint16 tab_uni_gb18030_2022_p2[sizeof(tab_uni_gb18030_p2)/sizeof(uint16)];

static uint gb18030_2_idx(const uchar *s)
{
  return (s[0] - MIN_MB_ODD_BYTE) * 192 + (s[1] - MIN_MB_EVEN_BYTE_2);
}

static uint gb18030_2022_4_idx(const uchar *s)
{
  ob_charset_assert(is_mb_even_4(s[1]));
  ob_charset_assert(is_mb_odd(s[2]) && is_mb_even_4(s[3]));
  uint idx = gb18030_4_chs_to_diff(s);
  if (idx < 0x334) {
    /* [GB+81308130, GB+8130D330) */
    return idx;
  } else if (idx <= 0x1D20) {
    /* [GB+8130D330, GB+8135F436] */
    ob_charset_assert(0);
  } else if (idx < 0x2403) {
    /* (GB+8135F436, GB+8137A839) */
    return idx - 6637;
  } else if (idx <= 0x2C40) {
    /* [GB+8137A839, GB+8138FD38] */
    ob_charset_assert(0);
  } else if (idx < 0x4A63 + GB_2022_CNT_PART_1) {
    /* (GB+8138FD38, GB+82359135) */
    return idx - 6637 - 2110;
  } else if (idx <= 0x82BC) {
    /* [GB+82359135, GB+8336C738] */
    ob_charset_assert(0);
  } else if (idx < 0x830E) {
    /* (GB+8336C738, GB+8336D030) */
    return idx - 6637 - 2110 + GB_2022_CNT_PART_1 - 14426;
  } else if (idx <= 0x93D4) {
    /* [GB+8336D030, GB+84308534] */
    ob_charset_assert(0);
  } else if (idx < 0x94BE) {
    /* (GB+84308534, GB+84309C38) */
    return idx - 6637 - 2110 + GB_2022_CNT_PART_1 - 14426 - 4295;
  } else if (idx <= 0x98C3 - GB_2022_CNT_PART_2) {
    /* [GB+84309C38, GB+84318235] */
    ob_charset_assert(0);
  } else if (idx <= 0x99fb) {
    /* (GB+84318235, GB+8431A439] */
    return idx - 6637 - 2110 + GB_2022_CNT_PART_1 - 14426 - 4295 - 1030 + GB_2022_CNT_PART_2;
  } else {
    ob_charset_assert(0);
  }

  return OB_CS_ILUNI;
}

static uint unicode_2022_idx(uint wc)
{
  if (wc < 0x9FBC) {
    /* [0x80, 0x9FBC) */
    return wc - 0x80;
    /* [0x9FBC, 0xE000) */
    ob_charset_assert(0);
  } else if (wc < 0xE865) {
    /* [0xE000, 0xE865) */
    return wc - 0xE000;
  } else if (wc <= 0xF92B) {
    /* [0xE865, 0xF92B] */
    ob_charset_assert(0);
  } else if (wc <= 0XFFFF) {
    /* (0xF92B, 0xFFFF] */
    return wc - 0xE000 - 4295;
  } else {
    /* Other */
    ob_charset_assert(0);
  }
  return OB_CS_ILUNI;
}


//Swap the unicode for a 2-length GB18030 code and a 4-length GB18030 code
static void swap_code_for_gb18030_2022(const char *char_GB_2, const char *char_GB_4, uint16_t OLD_UNI_2, uint16_t OLD_UNI_4)
{
  int ret = 0;
  const unsigned char *GB_2 = reinterpret_cast<const unsigned char *>(char_GB_2);
  const unsigned char *GB_4 = reinterpret_cast<const unsigned char *>(char_GB_4);

  /* set 2-byte GB18030-2022 tab */
  {
    unsigned int idx = gb18030_2_idx(GB_2);
    ob_charset_assert(tab_gb18030_2022_2_uni[idx] == OLD_UNI_2);
    tab_gb18030_2022_2_uni[idx] = OLD_UNI_4;
  }
  /* set 4-byte GB18030-2022 tab */
  {
    unsigned int idx = gb18030_2022_4_idx(GB_4);
    ob_charset_assert(tab_gb18030_2022_4_uni[idx] == OLD_UNI_4);
    tab_gb18030_2022_4_uni[idx] = OLD_UNI_2;
  }

  /* set 2-byte UNICODE tab */
  {
    bool in_tab_p1 = (OLD_UNI_4 < 0x9FBC);
    uint idx = unicode_2022_idx(OLD_UNI_4);
    uint16 *tab_uni_gb18030_2022 = in_tab_p1 ? tab_uni_gb18030_2022_p1 : tab_uni_gb18030_2022_p2;
    uchar s[4];
    diff_to_gb18030_4(s, 4, in_tab_p1 ? tab_uni_gb18030_2022_p1[idx] : (tab_uni_gb18030_2022_p2[idx] + UNI2_TO_GB4_DIFF));
    ob_charset_assert(gb18030_chs_to_code(GB_4, 4) == gb18030_chs_to_code(s, 4));
    tab_uni_gb18030_2022[idx] = gb18030_chs_to_code(GB_2, 2);
  }
  /* set 4-byte UNICODE tab */
  {
    bool in_tab_p1 = (OLD_UNI_2 < 0x9FBC);
    uint idx = unicode_2022_idx(OLD_UNI_2);
    uint gb_code = gb18030_chs_to_code(GB_4, 4);
    uint16 *tab_uni_gb18030_2022 = in_tab_p1 ? tab_uni_gb18030_2022_p1 : tab_uni_gb18030_2022_p2;
    uchar s[4];
    uint tmp = in_tab_p1 ? gb18030_4_code_to_diff(gb_code) : gb18030_4_code_to_diff(gb_code) - UNI2_TO_GB4_DIFF;
    ob_charset_assert(gb18030_chs_to_code(GB_2, 2) == tab_uni_gb18030_2022[idx]);
    tab_uni_gb18030_2022[idx] = in_tab_p1 ? gb18030_4_code_to_diff(gb_code) : gb18030_4_code_to_diff(gb_code) - UNI2_TO_GB4_DIFF;
    ob_charset_assert((uint)((tab_uni_gb18030_2022[idx] >> 8) & 0xFF) < MIN_MB_ODD_BYTE);
  }
}

static int ob_mb_wc_gb18030_2022(const ObCharsetInfo *cs __attribute__((unused)),
                                 ob_wc_t *pwc, const uchar *s, const uchar *e) {
  uint idx = 0;
  uint cp = 0;

  if (s >= e) return OB_CS_TOOSMALL;

  if (is_mb_1(s[0])) {
    /* [0x00, 0x7F] */
    *pwc = s[0];
    return 1;
  } else if (!is_mb_odd(s[0]))
    return OB_CS_ILSEQ;

  if (s + 2 > e) return OB_CS_TOOSMALL2;

  if (is_mb_even_2(s[1])) {
    idx = (s[0] - MIN_MB_ODD_BYTE) * 192 + (s[1] - MIN_MB_EVEN_BYTE_2);
    *pwc = tab_gb18030_2022_2_uni[idx];

    return (*pwc == 0) ? OB_CS_ILSEQ : 2;
  } else if (is_mb_even_4(s[1])) {
    if (s + 4 > e) return OB_CS_TOOSMALL4;

    if (!(is_mb_odd(s[2]) && is_mb_even_4(s[3]))) return OB_CS_ILSEQ;

    idx = gb18030_4_chs_to_diff(s);

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

    *pwc = cp;
    return 4;
  } else
    return OB_CS_ILSEQ;
}

static int ob_wc_mb_gb18030_2022_chs(const ObCharsetInfo *cs __attribute__((unused)),
                                     ob_wc_t wc, unsigned char *s, unsigned char *e) {
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

      err = diff_to_gb18030_4(s, 4, idx);
      ob_charset_assert(err != 0);

      return err != 0 ? len : OB_CS_ILUNI;
  }

  ob_charset_assert(0);
  return OB_CS_ILUNI;
}

int init_gb18030_2022()
{
  int ret = OB_CS_SUCCESS;
  MEMCPY(tab_gb18030_2022_2_uni, tab_gb18030_2_uni, sizeof(tab_gb18030_2_uni));
  /**
   * In order to add [GB+82358F33, GB+82359134] and [GB+84318236, GB+84318537] to tab_gb18030_2022_4_uni,
   * split tab_gb18030_4_uni as [0, P1), [P1, P2), [P2, END]
   * P1 is the index of GB+82358F33
   * P2 is the index of GB+84309C38*/
  const static unsigned int P1 = 0x4A63 - 6637 - 2110;
  const static unsigned int P2 = 0x94BE - 6637 - 2110 - 14426 - 4295;

  /* Copy [GB+81308130, GB+8130D330), (GB+8135F436, GB+8137A839), (GB+8138FD38, GB+82358F33) */
  MEMCPY(tab_gb18030_2022_4_uni, tab_gb18030_4_uni, P1 * sizeof(uint16_t));
  /* Add [GB+82358F33, GB+82359134] */
  for (int i = 0; i < GB_2022_CNT_PART_1; i ++) {
    const static int UNI_FOR_P1 =  0x4A63 + 0x5543;
    tab_gb18030_2022_4_uni[i + P1] = i + UNI_FOR_P1;
  }
  /* Copy (GB+8336C738, GB+8336D030), (GB+84308534, GB+84309C38) */
  MEMCPY(tab_gb18030_2022_4_uni + P1 + GB_2022_CNT_PART_1,
         tab_gb18030_4_uni + P1,
         (P2 - P1) * sizeof(uint16_t));
  /* Add [GB+84318236, GB+84318537] */
  for (int i = GB_2022_CNT_PART_2 - 1; i >= 0; i --) {
    const static int UNI_FOR_GB84318537 =  0x98C3 + 0x656C;
    tab_gb18030_2022_4_uni[P2 + GB_2022_CNT_PART_1 + GB_2022_CNT_PART_2 - 1 - i] = UNI_FOR_GB84318537 - i;
  }
  /* Copy (GB+84318537, GB+8431A439] */
  MEMCPY(tab_gb18030_2022_4_uni + P2 + GB_2022_CNT_PART_1 + GB_2022_CNT_PART_2,
         tab_gb18030_4_uni + P2,
         sizeof(tab_gb18030_4_uni) - P2 * sizeof(uint16_t));

  /* Copy tab_uni_gb18030_p1 [U+80, U+9FA6) */
  MEMCPY(tab_uni_gb18030_2022_p1, tab_uni_gb18030_p1, sizeof(tab_uni_gb18030_p1));
  /* Add [U+9FA6, U+9FBB] */
  for (int i = 0; i < GB_2022_CNT_PART_1; i ++) {
    const static int GB_diff_FOR_U9FA6 =  0x9FA6 - 0x5543;
    tab_uni_gb18030_2022_p1[i + sizeof(tab_uni_gb18030_p1)/sizeof(uint16_t)] = i + GB_diff_FOR_U9FA6;
  }

  /* Copy tab_uni_gb18030_p2 */
  MEMCPY(tab_uni_gb18030_2022_p2, tab_uni_gb18030_p2, sizeof(tab_uni_gb18030_p2));

  /* Correct the different mapping between GB18030_2022 and GB18030 */
  // PART1
  swap_code_for_gb18030_2022("\xFE\x59", "\x82\x35\x90\x37", 0xE81E, 0x9FB4);
  swap_code_for_gb18030_2022("\xFE\x61", "\x82\x35\x90\x38", 0xE826, 0x9FB5);
  swap_code_for_gb18030_2022("\xFE\x66", "\x82\x35\x90\x39", 0xE82B, 0x9FB6);
  swap_code_for_gb18030_2022("\xFE\x67", "\x82\x35\x91\x30", 0xE82C, 0x9FB7);
  swap_code_for_gb18030_2022("\xFE\x6D", "\x82\x35\x91\x31", 0xE832, 0x9FB8);
  swap_code_for_gb18030_2022("\xFE\x7E", "\x82\x35\x91\x32", 0xE843, 0x9FB9);
  swap_code_for_gb18030_2022("\xFE\x90", "\x82\x35\x91\x33", 0xE854, 0x9FBA);
  swap_code_for_gb18030_2022("\xFE\xA0", "\x82\x35\x91\x34", 0xE864, 0x9FBB);

  // PART2
  swap_code_for_gb18030_2022("\xA6\xD9", "\x84\x31\x82\x36", 0xE78D, 0xFE10);
  swap_code_for_gb18030_2022("\xA6\xDA", "\x84\x31\x82\x38", 0xE78E, 0xFE12);
  swap_code_for_gb18030_2022("\xA6\xDB", "\x84\x31\x82\x37", 0xE78F, 0xFE11);
  swap_code_for_gb18030_2022("\xA6\xDC", "\x84\x31\x82\x39", 0xE790, 0xFE13);
  swap_code_for_gb18030_2022("\xA6\xDD", "\x84\x31\x83\x30", 0xE791, 0xFE14);
  swap_code_for_gb18030_2022("\xA6\xDE", "\x84\x31\x83\x31", 0xE792, 0xFE15);
  swap_code_for_gb18030_2022("\xA6\xDF", "\x84\x31\x83\x32", 0xE793, 0xFE16);
  swap_code_for_gb18030_2022("\xA6\xEC", "\x84\x31\x83\x33", 0xE794, 0xFE17);
  swap_code_for_gb18030_2022("\xA6\xED", "\x84\x31\x83\x34", 0xE795, 0xFE18);
  swap_code_for_gb18030_2022("\xA6\xF3", "\x84\x31\x83\x35", 0xE796, 0xFE19);

  return ret;
}

static unsigned int get_pinyin_weight_if_chinese_character_2022(unsigned int code) {
  if (code >= PINYIN_2_BYTE_START_2022 && code <= PINYIN_2_BYTE_END_2022) {
    unsigned int idx = (((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE) * 0xBE + (code & 0xFF) -
               MIN_MB_EVEN_BYTE_2;
    if ((code & 0xFF) > 0x7F) idx -= 0x01;
    return CHINESE_WEIGHT_BASE + gb18030_2022_2_pinyin_weight_py[idx];
  } else if (code >= PINYIN_4_BYTE_1_START_2022 && code <= PINYIN_4_BYTE_1_END_2022) {
    unsigned int idx = gb18030_4_code_to_diff(code) - PINYIN_4_1_DIFF_2022;
    return CHINESE_WEIGHT_BASE + gb18030_2022_4_pinyin_weight_py_p1[idx];
  } else if (code >= PINYIN_4_BYTE_2_START_2022 && code <= PINYIN_4_BYTE_2_END_2022) {
    unsigned int idx = gb18030_4_code_to_diff(code) - PINYIN_4_2_DIFF_2022;
    return CHINESE_WEIGHT_BASE + gb18030_2022_4_pinyin_weight_py_p2[idx];
  }
  return CHINESE_WEIGHT_BASE;
}

static unsigned int get_radical_weight_if_chinese_character_2022(unsigned int code) {
  if (code >= RADICAL_2_BYTE_START_2022 && code <= RADICAL_2_BYTE_END_2022) {
    unsigned int idx = (((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE) * 0xBE + (code & 0xFF) -
               MIN_MB_EVEN_BYTE_2;
    if ((code & 0xFF) > 0x7F) idx -= 0x01;
    return CHINESE_WEIGHT_BASE + gb18030_2022_2_radical_weight_py[idx];
  } else if (code >= RADICAL_4_BYTE_1_START_2022 && code <= RADICAL_4_BYTE_1_END_2022) {
    unsigned int idx = gb18030_4_code_to_diff(code) - RADICAL_4_1_DIFF_2022;
    return CHINESE_WEIGHT_BASE + gb18030_2022_4_radical_weight_py_p1[idx];
  } else if (code >= RADICAL_4_BYTE_2_START_2022 && code <= RADICAL_4_BYTE_2_END_2022) {
    unsigned int idx = gb18030_4_code_to_diff(code) - RADICAL_4_2_DIFF_2022;
    return CHINESE_WEIGHT_BASE + gb18030_2022_4_radical_weight_py_p2[idx];
  }
  return CHINESE_WEIGHT_BASE;
}

static unsigned int get_stroke_weight_if_chinese_character_2022(unsigned int code) {
  if (code >= STROKE_2_BYTE_START_2022 && code <= STROKE_2_BYTE_END_2022) {
    unsigned int idx = (((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE) * 0xBE + (code & 0xFF) -
               MIN_MB_EVEN_BYTE_2;
    if ((code & 0xFF) > 0x7F) idx -= 0x01;
    return CHINESE_WEIGHT_BASE + gb18030_2022_2_stroke_weight_py[idx];
  } else if (code >= STROKE_4_BYTE_1_START_2022 && code <= STROKE_4_BYTE_1_END_2022) {
    unsigned int idx = gb18030_4_code_to_diff(code) - STROKE_4_1_DIFF_2022;
    return CHINESE_WEIGHT_BASE + gb18030_2022_4_stroke_weight_py_p1[idx];
  } else if (code >= STROKE_4_BYTE_2_START_2022 && code <= STROKE_4_BYTE_2_END_2022) {
    unsigned int idx = gb18030_4_code_to_diff(code) - STROKE_4_2_DIFF_2022;
    return CHINESE_WEIGHT_BASE + gb18030_2022_4_stroke_weight_py_p2[idx];
  }
  return CHINESE_WEIGHT_BASE;
}

static ObCollationHandler ob_collation_2022_pinyin_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_pinyin_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnncollsp_gb18030<get_pinyin_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnxfrm_gb18030<get_pinyin_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_pinyin_weight_if_chinese_character_2022>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_pinyin_weight_if_chinese_character_2022, INSENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_pinyin_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_2022_pinyin_cs_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_pinyin_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnncollsp_gb18030<get_pinyin_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnxfrm_gb18030<get_pinyin_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_pinyin_weight_if_chinese_character_2022>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_pinyin_weight_if_chinese_character_2022, SENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_pinyin_weight_if_chinese_character_2022, SENSITIVE>,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_2022_radical_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_radical_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnncollsp_gb18030<get_radical_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnxfrm_gb18030<get_radical_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_radical_weight_if_chinese_character_2022>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_radical_weight_if_chinese_character_2022, INSENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_radical_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_2022_radical_cs_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_radical_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnncollsp_gb18030<get_radical_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnxfrm_gb18030<get_radical_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_radical_weight_if_chinese_character_2022>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_radical_weight_if_chinese_character_2022, SENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_radical_weight_if_chinese_character_2022, SENSITIVE>,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_2022_stroke_ci_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_stroke_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnncollsp_gb18030<get_stroke_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnxfrm_gb18030<get_stroke_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_stroke_weight_if_chinese_character_2022>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_stroke_weight_if_chinese_character_2022, INSENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_stroke_weight_if_chinese_character_2022, INSENSITIVE>,
  ob_propagate_simple
};

static ObCollationHandler ob_collation_2022_stroke_cs_handler =
{
  NULL,
  NULL,
  ob_strnncoll_gb18030<get_stroke_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnncollsp_gb18030<get_stroke_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnxfrm_gb18030<get_stroke_weight_if_chinese_character_2022, SENSITIVE>,
  ob_strnxfrmlen_simple,
  ob_strnxfrm_gb18030_varlen<get_stroke_weight_if_chinese_character_2022>,
  ob_like_range_mb,
  ob_wildcmp_gb18030<get_stroke_weight_if_chinese_character_2022, SENSITIVE>,
  NULL,
  ob_instr_mb,
  ob_hash_sort_gb18030<get_stroke_weight_if_chinese_character_2022, SENSITIVE>,
  ob_propagate_simple
};

static ObCharsetHandler ob_charset_gb18030_2022_handler =
{
  ob_ismbchar_gb18030,
  ob_mbcharlen_gb18030,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  ob_well_formed_len_gb18030,
  ob_lengthsp_8bit,
  ob_mb_wc_gb18030_2022,
  ob_wc_mb_gb18030_2022_chs,
  ob_mb_ctype_mb,
  ob_caseup_gb18030,
  ob_casedn_gb18030,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit
};

ObCharsetInfo ob_charset_gb18030_2022_bin =
{
  oceanbase::common::CS_TYPE_GB18030_BIN,
  0,
  0,
  OB_CS_COMPILED | OB_CS_BINSORT, /* state         */
  "gb18030_2022",                 /* cs name       */
  "gb18030_2022_bin",             /* name          */
  "",                             /* comment       */
  NULL,                           /* tailoring     */
  NULL,                           /* coll_param    */
  ctype_gb18030,                  /* ctype         */
  to_lower_gb18030,               /* lower         */
  to_upper_gb18030,               /* UPPER         */
  NULL,                           /* sort order    */
  NULL,                           /* uca           */
  &ob_caseinfo_gb18030,           /* caseinfo      */
  NULL,                           /* state_map     */
  NULL,                           /* ident_map     */
  1,                              /* strxfrm_multiply */
  2,                              /* caseup_multiply  */
  2,                              /* casedn_multiply  */
  1,                              /* mbminlen      */
  4,                              /* mbmaxlen      */
  2,                              /* mbmaxlenlen   */
  0,                              /* min_sort_char */
  0xFEFEFEFE,                     /* max_sort_char */
  ' ',                            /* pad char      */
  1,                              /* escape_with_backslash_is_dangerous */
  1,                              /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_mb_bin_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_gb18030_2022_pinyin_ci =
{
  oceanbase::common::CS_TYPE_GB18030_2022_PINYIN_CI,
  0,
  0,
  OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_STRNXFRM | OB_CS_CI, /* state         */
  "gb18030_2022",                                  /* cs name       */
  "gb18030_2022_chinese_ci",                       /* name          */
  "",                                              /* comment       */
  NULL,                                            /* tailoring     */
  NULL,                                            /* coll_param    */
  ctype_gb18030,                                   /* ctype         */
  to_lower_gb18030,                                /* lower         */
  to_upper_gb18030,                                /* UPPER         */
  sort_order_gb18030_ci,                           /* sort order    */
  NULL,                                            /* uca           */
  &ob_caseinfo_gb18030,                            /* caseinfo      */
  NULL,                                            /* state_map     */
  NULL,                                            /* ident_map     */
  2,                                               /* strxfrm_multiply */
  2,                                               /* caseup_multiply  */
  2,                                               /* casedn_multiply  */
  1,                                               /* mbminlen      */
  4,                                               /* mbmaxlen      */
  2,                                               /* mbmaxlenlen   */
  0,                                               /* min_sort_char */
  0xFE39FE39,                                      /* max_sort_char */
  ' ',                                             /* pad char      */
  1,                                               /* escape_with_backslash_is_dangerous */
  1,                                               /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_2022_pinyin_ci_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_gb18030_2022_pinyin_cs =
{
  oceanbase::common::CS_TYPE_GB18030_2022_PINYIN_CS,
  0,
  0,
  OB_CS_COMPILED | OB_CS_STRNXFRM,                 /* state         */
  "gb18030_2022",                                  /* cs name       */
  "gb18030_2022_chinese_cs",                       /* name          */
  "",                                              /* comment       */
  NULL,                                            /* tailoring     */
  NULL,                                            /* coll_param    */
  ctype_gb18030,                                   /* ctype         */
  to_lower_gb18030,                                /* lower         */
  to_upper_gb18030,                                /* UPPER         */
  sort_order_gb18030,                              /* sort order    */
  NULL,                                            /* uca           */
  &ob_caseinfo_gb18030,                            /* caseinfo      */
  NULL,                                            /* state_map     */
  NULL,                                            /* ident_map     */
  2,                                               /* strxfrm_multiply */
  2,                                               /* caseup_multiply  */
  2,                                               /* casedn_multiply  */
  1,                                               /* mbminlen      */
  4,                                               /* mbmaxlen      */
  2,                                               /* mbmaxlenlen   */
  0,                                               /* min_sort_char */
  0xFE39FE39,                                      /* max_sort_char */
  ' ',                                             /* pad char      */
  1,                                               /* escape_with_backslash_is_dangerous */
  1,                                               /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_2022_pinyin_cs_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_gb18030_2022_radical_ci =
{
  oceanbase::common::CS_TYPE_GB18030_2022_RADICAL_CI,
  0,
  0,
  OB_CS_COMPILED | OB_CS_STRNXFRM | OB_CS_CI,                 /* state         */
  "gb18030_2022",                                  /* cs name       */
  "gb18030_2022_radical_ci",                       /* name          */
  "",                                              /* comment       */
  NULL,                                            /* tailoring     */
  NULL,                                            /* coll_param    */
  ctype_gb18030,                                   /* ctype         */
  to_lower_gb18030,                                /* lower         */
  to_upper_gb18030,                                /* UPPER         */
  sort_order_gb18030_ci,                           /* sort order    */
  NULL,                                            /* uca           */
  &ob_caseinfo_gb18030,                            /* caseinfo      */
  NULL,                                            /* state_map     */
  NULL,                                            /* ident_map     */
  2,                                               /* strxfrm_multiply */
  2,                                               /* caseup_multiply  */
  2,                                               /* casedn_multiply  */
  1,                                               /* mbminlen      */
  4,                                               /* mbmaxlen      */
  2,                                               /* mbmaxlenlen   */
  0,                                               /* min_sort_char */
  0xFE39FE39,                                      /* max_sort_char */
  ' ',                                             /* pad char      */
  1,                                               /* escape_with_backslash_is_dangerous */
  1,                                               /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_2022_radical_ci_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_gb18030_2022_radical_cs =
{
  oceanbase::common::CS_TYPE_GB18030_2022_RADICAL_CS,
  0,
  0,
  OB_CS_COMPILED | OB_CS_STRNXFRM,                 /* state         */
  "gb18030_2022",                                  /* cs name       */
  "gb18030_2022_radical_cs",                       /* name          */
  "",                                              /* comment       */
  NULL,                                            /* tailoring     */
  NULL,                                            /* coll_param    */
  ctype_gb18030,                                   /* ctype         */
  to_lower_gb18030,                                /* lower         */
  to_upper_gb18030,                                /* UPPER         */
  sort_order_gb18030,                              /* sort order    */
  NULL,                                            /* uca           */
  &ob_caseinfo_gb18030,                            /* caseinfo      */
  NULL,                                            /* state_map     */
  NULL,                                            /* ident_map     */
  2,                                               /* strxfrm_multiply */
  2,                                               /* caseup_multiply  */
  2,                                               /* casedn_multiply  */
  1,                                               /* mbminlen      */
  4,                                               /* mbmaxlen      */
  2,                                               /* mbmaxlenlen   */
  0,                                               /* min_sort_char */
  0xFE39FE39,                                      /* max_sort_char */
  ' ',                                             /* pad char      */
  1,                                               /* escape_with_backslash_is_dangerous */
  1,                                               /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_2022_radical_cs_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_gb18030_2022_stroke_ci =
{
  oceanbase::common::CS_TYPE_GB18030_2022_STROKE_CI,
  0,
  0,
  OB_CS_COMPILED | OB_CS_STRNXFRM | OB_CS_CI,                 /* state         */
  "gb18030_2022",                                  /* cs name       */
  "gb18030_2022_stroke_ci",                        /* name          */
  "",                                              /* comment       */
  NULL,                                            /* tailoring     */
  NULL,                                            /* coll_param    */
  ctype_gb18030,                                   /* ctype         */
  to_lower_gb18030,                                /* lower         */
  to_upper_gb18030,                                /* UPPER         */
  sort_order_gb18030_ci,                           /* sort order    */
  NULL,                                            /* uca           */
  &ob_caseinfo_gb18030,                            /* caseinfo      */
  NULL,                                            /* state_map     */
  NULL,                                            /* ident_map     */
  2,                                               /* strxfrm_multiply */
  2,                                               /* caseup_multiply  */
  2,                                               /* casedn_multiply  */
  1,                                               /* mbminlen      */
  4,                                               /* mbmaxlen      */
  2,                                               /* mbmaxlenlen   */
  0,                                               /* min_sort_char */
  0xFE39FE39,                                      /* max_sort_char */
  ' ',                                             /* pad char      */
  1,                                               /* escape_with_backslash_is_dangerous */
  1,                                               /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_2022_stroke_ci_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_gb18030_2022_stroke_cs =
{
  oceanbase::common::CS_TYPE_GB18030_2022_STROKE_CS,
  0,
  0,
  OB_CS_COMPILED | OB_CS_STRNXFRM,                 /* state         */
  "gb18030_2022",                                  /* cs name       */
  "gb18030_2022_stroke_cs",                        /* name          */
  "",                                              /* comment       */
  NULL,                                            /* tailoring     */
  NULL,                                            /* coll_param    */
  ctype_gb18030,                                   /* ctype         */
  to_lower_gb18030,                                /* lower         */
  to_upper_gb18030,                                /* UPPER         */
  sort_order_gb18030,                              /* sort order    */
  NULL,                                            /* uca           */
  &ob_caseinfo_gb18030,                            /* caseinfo      */
  NULL,                                            /* state_map     */
  NULL,                                            /* ident_map     */
  2,                                               /* strxfrm_multiply */
  2,                                               /* caseup_multiply  */
  2,                                               /* casedn_multiply  */
  1,                                               /* mbminlen      */
  4,                                               /* mbmaxlen      */
  2,                                               /* mbmaxlenlen   */
  0,                                               /* min_sort_char */
  0xFE39FE39,                                      /* max_sort_char */
  ' ',                                             /* pad char      */
  1,                                               /* escape_with_backslash_is_dangerous */
  1,                                               /* levels_for_compare */
  1,
  &ob_charset_gb18030_2022_handler,
  &ob_collation_2022_stroke_cs_handler,
  PAD_SPACE
};
