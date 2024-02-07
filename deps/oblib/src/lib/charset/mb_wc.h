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

#ifndef MB_WC_INCLUDED
#define MB_WC_INCLUDED

/**
  @file mb_wc.h

  Definitions of mb_wc (multibyte to wide character, ie., effectively
  “parse a UTF-8 character”) functions for UTF-8 (both three- and four-byte).
  These are available both as inline functions, as C-style thunks so that they
  can fit into MY_CHARSET_HANDLER, and as functors.

  The functors exist so that you can specialize a class on them and get them
  inlined instead of having to call them through the function pointer in
  MY_CHARSET_HANDLER; mb_wc is in itself so cheap (the most common case is
  just a single byte load and a predictable compare) that the call overhead
  in a tight loop is significant, and these routines tend to take up a lot
  of CPU time when sorting. Typically, at the outermost level, you'd simply
  compare cs->cset->mb_wc with my_mb_wc_{utf8,utf8mb4}_thunk, and if so,
  instantiate your function with the given class. If it doesn't match,
  you can use Mb_wc_through_function_pointer, which calls through the
  function pointer as usual. (It will cache the function pointer for you,
  which is typically faster than looking it up all the time -- the compiler
  cannot always figure out on its own that it doesn't change.)

  The Mb_wc_* classes should be sent by _value_, not by reference, since
  they are never larger than two pointers (and usually simply zero).
*/
#include "lib/charset/ob_ctype.h"

#define ALWAYS_INLINE __attribute__((always_inline)) inline

template <bool RANGE_CHECK, bool SUPPORT_MB4>
static int ob_mb_wc_utf8_prototype(ob_wc_t *pwc, const unsigned char *s,
                                   const unsigned char *e);

/**
  Functor that converts a UTF-8 multibyte sequence (up to three bytes)
  to a wide character.
*/
struct Mb_wc_utf8 {
  Mb_wc_utf8() {}

  ALWAYS_INLINE
  int operator()(ob_wc_t *pwc, const unsigned char *s, const unsigned char *e) const {
    return ob_mb_wc_utf8_prototype</*RANGE_CHECK=*/true, /*SUPPORT_MB4=*/false>(
          pwc, s, e);
  }
};

/**
  Functor that converts a UTF-8 multibyte sequence (up to four bytes)
  to a wide character.
*/
struct Mb_wc_utf8mb4 {
  Mb_wc_utf8mb4() {}

  ALWAYS_INLINE
  int operator()(ob_wc_t *pwc, const unsigned char *s, const unsigned char *e) const {
    return ob_mb_wc_utf8_prototype</*RANGE_CHECK=*/true, /*SUPPORT_MB4=*/true>(
          pwc, s, e);
  }
};

/**
  Functor that uses a function pointer to convert a multibyte sequence
  to a wide character.
*/
class Mb_wc_through_function_pointer {
 public:
  explicit Mb_wc_through_function_pointer(const ObCharsetInfo *cs)
      : m_funcptr(cs->cset->mb_wc), m_cs(cs) {}

  int operator()(ob_wc_t *pwc, const unsigned char *s, const unsigned char *e) const {
    return m_funcptr(m_cs, pwc, s, e);
  }

 private:
  typedef int (*mbwc_func_t)(const ObCharsetInfo *, ob_wc_t *, const unsigned char *,
                             const unsigned char *);

  const mbwc_func_t m_funcptr;
  const ObCharsetInfo *const m_cs;
};

template <bool RANGE_CHECK, bool SUPPORT_MB4>
static ALWAYS_INLINE int ob_mb_wc_utf8_prototype(ob_wc_t *pwc, const unsigned char *s,
                                                 const unsigned char *e) {
  if (RANGE_CHECK && s >= e) return OB_CS_TOOSMALL;

  unsigned char c = s[0];
  if (c < 0x80) {
    *pwc = c;
    return 1;
  }

  if (c < 0xe0) {
    if (c < 0xc2)  // Resulting code point would be less than 0x80.
      return OB_CS_ILSEQ;

    if (RANGE_CHECK && s + 2 > e) return OB_CS_TOOSMALL2;

    if ((s[1] & 0xc0) != 0x80)  // Next byte must be a continuation byte.
      return OB_CS_ILSEQ;

    *pwc = ((ob_wc_t)(c & 0x1f) << 6) + (ob_wc_t)(s[1] & 0x3f);
    return 2;
  }

  if (c < 0xf0) {
    if (RANGE_CHECK && s + 3 > e) return OB_CS_TOOSMALL3;

    // Next two bytes must be continuation bytes.
    uint16 two_bytes;
    memcpy(&two_bytes, s + 1, sizeof(two_bytes));
    if ((two_bytes & 0xc0c0) != 0x8080)  // Endianness does not matter.
      return OB_CS_ILSEQ;

    *pwc = ((ob_wc_t)(c & 0x0f) << 12) + ((ob_wc_t)(s[1] & 0x3f) << 6) +
           (ob_wc_t)(s[2] & 0x3f);
    if (*pwc < 0x800) return OB_CS_ILSEQ;
    /*
      According to RFC 3629, UTF-8 should prohibit characters between
      U+D800 and U+DFFF, which are reserved for surrogate pairs and do
      not directly represent characters.
    */
    if (*pwc >= 0xd800 && *pwc <= 0xdfff) return OB_CS_ILSEQ;
    return 3;
  }

  if (SUPPORT_MB4) {
    if (RANGE_CHECK && s + 4 > e) /* We need 4 characters */
      return OB_CS_TOOSMALL4;

    /*
      This byte must be of the form 11110xxx, and the next three bytes
      must be continuation bytes.
    */
    uint32 four_bytes;
    memcpy(&four_bytes, s, sizeof(four_bytes));
#ifdef WORDS_BIGENDIAN
    if ((four_bytes & 0xf8c0c0c0) != 0xf0808080)
#else
    if ((four_bytes & 0xc0c0c0f8) != 0x808080f0)
#endif
      return OB_CS_ILSEQ;

    *pwc = ((ob_wc_t)(c & 0x07) << 18) + ((ob_wc_t)(s[1] & 0x3f) << 12) +
           ((ob_wc_t)(s[2] & 0x3f) << 6) + (ob_wc_t)(s[3] & 0x3f);
    if (*pwc < 0x10000 || *pwc > 0x10ffff) return OB_CS_ILSEQ;
    return 4;
  }

  return OB_CS_ILSEQ;
}

extern "C" int ob_mb_wc_utf8mb4_thunk(const ObCharsetInfo *cs, ob_wc_t *pwc,
                                      const unsigned char *s, const unsigned char *e);

#endif  // MB_WC_INCLUDED