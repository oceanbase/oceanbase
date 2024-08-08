/**
 * Copyright (code) 2021 OceanBase
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
#include "lib/charset/ob_uctype.h"
#include "lib/charset/mb_wc.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/charset/ob_ctype_utf8_tab.h"
#include "common/ob_target_specific.h"
#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

#define IS_CONTINUATION_BYTE(code) (((code) >> 6) == 0x02)
#define RET_ERR_IDX 1 

static int ob_valid_mbcharlen_utf8mb3(const uchar *s, const uchar *e)
{
  uchar c;
  ob_charset_assert(s < e);
  c= s[0];
  if (c < 0x80)
    return 1;
  if (c < 0xc2)
    return OB_CS_ILSEQ;
  if (c < 0xe0)
  {
    if (s+2 > e) /* We need 2 characters */
      return OB_CS_TOOSMALL2;
    if (!(IS_CONTINUATION_BYTE(s[1])))
      return OB_CS_ILSEQ;
    return 2;
  }
  ob_charset_assert(c < 0xf0);
  if (s+3 > e) /* We need 3 characters */
    return OB_CS_TOOSMALL3;
  if (!(IS_CONTINUATION_BYTE(s[1]) && IS_CONTINUATION_BYTE(s[2]) && (c >= 0xe1 || s[1] >= 0xa0)))
    return OB_CS_ILSEQ;
  return 3;
}

static int ob_valid_mbcharlen_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)), const uchar *s, const uchar *e)
{
  uchar c;
  if (s >= e)
    return OB_CS_TOOSMALL;
  c= s[0];
  if (c < 0xf0)
    return ob_valid_mbcharlen_utf8mb3(s, e);
  if (c < 0xf5)
  {
    if (s + 4 > e) /* We need 4 characters */
      return OB_CS_TOOSMALL4;
    if (!(IS_CONTINUATION_BYTE(s[1]) &&
          IS_CONTINUATION_BYTE(s[2]) &&
          IS_CONTINUATION_BYTE(s[3]) &&
          (c >= 0xf1 || s[1] >= 0x90) &&
          (c <= 0xf3 || s[1] <= 0x8F)))
      return OB_CS_ILSEQ;
    return 4;
  }
  return OB_CS_ILSEQ;
}

static uint ob_ismbchar_utf8mb4(const ObCharsetInfo *cs, const char *b, const char *e)
{
  int res= ob_valid_mbcharlen_utf8mb4(cs, (const uchar*)b, (const uchar*)e);
  return (res > 1) ? res : 0;
}


static uint ob_mbcharlen_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)), uint c)
{
  if (c < 0x80)
    return 1;
  if (c < 0xc2)
    return 0; /* Illegal mb head */
  if (c < 0xe0)
    return 2;
  if (c < 0xf0)
    return 3;
  if (c < 0xf8)
    return 4;
  return 0; /* Illegal mb head */;
}

static inline int ascii_u64(const uint8_t *data, int len)
{
    uint8_t orall = 0;

    if (len >= 16) {

        uint64_t or1 = 0, or2 = 0;
        const uint8_t *data2 = data+8;

        do {
            or1 |= *(const uint64_t *)data;
            or2 |= *(const uint64_t *)data2;
            data += 16;
            data2 += 16;
            len -= 16;
        } while (len >= 16);

        /*
         * Idea from Benny Halevy <bhalevy@scylladb.com>
         * - 7-th bit set   ==> orall = !(non-zero) - 1 = 0 - 1 = 0xFF
         * - 7-th bit clear ==> orall = !0 - 1          = 1 - 1 = 0x00
         */
        orall = !((or1 | or2) & 0x8080808080808080ULL) - 1;
    }

    while (len--)
        orall |= *data++;

    return orall < 0x80;
}


OB_DECLARE_DEFAULT_CODE(
  static size_t ob_well_formed_len_utf8mb4(const ObCharsetInfo *cs,
                                         const char *b, const char *e,
                                         size_t pos, int *error)
  {
    int len = (int)(e-b);
    if (OB_UNLIKELY(len <= 0)) {
      return 0;
    } else if (len>=15 && ascii_u64((const uint8_t *)b, len)) {
      return (size_t)len;
    }
    const char *b_start= b;
    *error= 0;
    while (pos)
    {
      int mb_len;
      if ((mb_len= ob_valid_mbcharlen_utf8mb4(cs, (uchar*) b, (uchar*) e)) <= 0)
      {
        *error= b < e ? 1 : 0;
        break;
      }
      b+= mb_len;
      pos--;
    }
    return (size_t) (b - b_start);
    
  }
)

static int inline utf8_naive(const unsigned char *data, int len)
{
    int err_pos = 1;

    while (len) {
        int bytes;
        const unsigned char byte1 = data[0];

        /* 00..7F */
        if (byte1 <= 0x7F) {
            bytes = 1;
        /* C2..DF, 80..BF */
        } else if (len >= 2 && byte1 >= 0xC2 && byte1 <= 0xDF &&
                (signed char)data[1] <= (signed char)0xBF) {
            bytes = 2;
        } else if (len >= 3) {
            const unsigned char byte2 = data[1];

            /* Is byte2, byte3 between 0x80 ~ 0xBF */
            const int byte2_ok = (signed char)byte2 <= (signed char)0xBF;
            const int byte3_ok = (signed char)data[2] <= (signed char)0xBF;

            if (byte2_ok && byte3_ok &&
                     /* E0, A0..BF, 80..BF */
                    ((byte1 == 0xE0 && byte2 >= 0xA0) ||
                     /* E1..EC, 80..BF, 80..BF */
                     (byte1 >= 0xE1 && byte1 <= 0xEC) ||
                     /* ED, 80..9F, 80..BF */
                     (byte1 == 0xED && byte2 <= 0x9F) ||
                     /* EE..EF, 80..BF, 80..BF */
                     (byte1 >= 0xEE && byte1 <= 0xEF))) {
                bytes = 3;
            } else if (len >= 4) {
                /* Is byte4 between 0x80 ~ 0xBF */
                const int byte4_ok = (signed char)data[3] <= (signed char)0xBF;

                if (byte2_ok && byte3_ok && byte4_ok &&
                         /* F0, 90..BF, 80..BF, 80..BF */
                        ((byte1 == 0xF0 && byte2 >= 0x90) ||
                         /* F1..F3, 80..BF, 80..BF, 80..BF */
                         (byte1 >= 0xF1 && byte1 <= 0xF3) ||
                         /* F4, 80..8F, 80..BF, 80..BF */
                         (byte1 == 0xF4 && byte2 <= 0x8F))) {
                    bytes = 4;
                } else {
                    return err_pos;
                }
            } else {
                return err_pos;
            }
        } else {
            return err_pos;
        }

        len -= bytes;
        err_pos += bytes;
        data += bytes;
    }

    return 0;
}

OB_DECLARE_SSE42_SPECIFIC_CODE(

static inline int ascii_simd(const uint8_t *data, int len) {
  if (len >= 32) {
    const uint8_t *data2 = data + 16;

    __m128i or1 = _mm_set1_epi8(0), or2 = or1;

    while (len >= 32) {
      __m128i input1 = _mm_loadu_si128((const __m128i *)data);
      __m128i input2 = _mm_loadu_si128((const __m128i *)data2);

      or1 = _mm_or_si128(or1, input1);
      or2 = _mm_or_si128(or2, input2);

      data += 32;
      data2 += 32;
      len -= 32;
    }

    or1 = _mm_or_si128(or1, or2);
    if (_mm_movemask_epi8(_mm_cmplt_epi8(or1, _mm_set1_epi8(0)))) return 0;
  }

  return ascii_u64(data, len);
}

static const int8_t _first_len_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

/* Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4) */
static const int8_t _first_range_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

/*
 * Range table, map range index to min and max values
 * Index 0    : 00 ~ 7F (First Byte, ascii)
 * Index 1,2,3: 80 ~ BF (Second, Third, Fourth Byte)
 * Index 4    : A0 ~ BF (Second Byte after E0)
 * Index 5    : 80 ~ 9F (Second Byte after ED)
 * Index 6    : 90 ~ BF (Second Byte after F0)
 * Index 7    : 80 ~ 8F (Second Byte after F4)
 * Index 8    : C2 ~ F4 (First Byte, non ascii)
 * Index 9~15 : illegal: i >= 127 && i <= -128
 */
static const uint8_t _range_min_tbl[] = {
    0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
    0xC2, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F,
};
static const uint8_t _range_max_tbl[] = {
    0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
    0xF4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
};

/*
 * Tables for fast handling of four special First Bytes(E0,ED,F0,F4), after
 * which the Second Byte are not 80~BF. It contains "range index adjustment".
 * +------------+---------------+------------------+----------------+
 * | First Byte | original range| range adjustment | adjusted range |
 * +------------+---------------+------------------+----------------+
 * | E0         | 2             | 2                | 4              |
 * +------------+---------------+------------------+----------------+
 * | ED         | 2             | 3                | 5              |
 * +------------+---------------+------------------+----------------+
 * | F0         | 3             | 3                | 6              |
 * +------------+---------------+------------------+----------------+
 * | F4         | 4             | 4                | 8              |
 * +------------+---------------+------------------+----------------+
 */
/* index1 -> E0, index14 -> ED */
static const int8_t _df_ee_tbl[] = {
    0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0,
};
/* index1 -> F0, index5 -> F4 */
static const int8_t _ef_fe_tbl[] = {
    0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};


/* 5x faster than naive method */
/* Return 0 - success, -1 - error, >0 - first error char(if RET_ERR_IDX = 1) */
int utf8_range(const unsigned char *data, int len)
{
#if  RET_ERR_IDX
    int err_pos = 1;
#endif

    if (len >= 16) {
        __m128i prev_input = _mm_set1_epi8(0);
        __m128i prev_first_len = _mm_set1_epi8(0);

        /* Cached tables */
        const __m128i first_len_tbl =
            _mm_loadu_si128((const __m128i *)_first_len_tbl);
        const __m128i first_range_tbl =
            _mm_loadu_si128((const __m128i *)_first_range_tbl);
        const __m128i range_min_tbl =
            _mm_loadu_si128((const __m128i *)_range_min_tbl);
        const __m128i range_max_tbl =
            _mm_loadu_si128((const __m128i *)_range_max_tbl);
        const __m128i df_ee_tbl =
            _mm_loadu_si128((const __m128i *)_df_ee_tbl);
        const __m128i ef_fe_tbl =
            _mm_loadu_si128((const __m128i *)_ef_fe_tbl);

        __m128i error = _mm_set1_epi8(0);

        while (len >= 16) {
            const __m128i input = _mm_loadu_si128((const __m128i *)data);

            /* high_nibbles = input >> 4 */
            const __m128i high_nibbles =
                _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

            /* first_len = legal character length minus 1 */
            /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
            /* first_len = first_len_tbl[high_nibbles] */
            __m128i first_len = _mm_shuffle_epi8(first_len_tbl, high_nibbles);

            /* First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF */
            /* range = first_range_tbl[high_nibbles] */
            __m128i range = _mm_shuffle_epi8(first_range_tbl, high_nibbles);

            /* Second Byte: set range index to first_len */
            /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
            /* range |= (first_len, prev_first_len) << 1 byte */
            range = _mm_or_si128(
                    range, _mm_alignr_epi8(first_len, prev_first_len, 15));

            /* Third Byte: set range index to saturate_sub(first_len, 1) */
            /* 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF */
            __m128i tmp;
            /* tmp = (first_len, prev_first_len) << 2 bytes */
            tmp = _mm_alignr_epi8(first_len, prev_first_len, 14);
            /* tmp = saturate_sub(tmp, 1) */
            tmp = _mm_subs_epu8(tmp, _mm_set1_epi8(1));
            /* range |= tmp */
            range = _mm_or_si128(range, tmp);

            /* Fourth Byte: set range index to saturate_sub(first_len, 2) */
            /* 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF */
            /* tmp = (first_len, prev_first_len) << 3 bytes */
            tmp = _mm_alignr_epi8(first_len, prev_first_len, 13);
            /* tmp = saturate_sub(tmp, 2) */
            tmp = _mm_subs_epu8(tmp, _mm_set1_epi8(2));
            /* range |= tmp */
            range = _mm_or_si128(range, tmp);

            /*
             * Now we have below range indices caluclated
             * Correct cases:
             * - 8 for C0~FF
             * - 3 for 1st byte after F0~FF
             * - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
             * - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
             *         3rd byte after F0~FF
             * - 0 for others
             * Error cases:
             *   9,10,11 if non ascii First Byte overlaps
             *   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error
             */

            /* Adjust Second Byte range for special First Bytes(E0,ED,F0,F4) */
            /* Overlaps lead to index 9~15, which are illegal in range table */
            __m128i shift1, pos, range2;
            /* shift1 = (input, prev_input) << 1 byte */
            shift1 = _mm_alignr_epi8(input, prev_input, 15);
            pos = _mm_sub_epi8(shift1, _mm_set1_epi8(0xEF));
            /*
             * shift1:  | EF  F0 ... FE | FF  00  ... ...  DE | DF  E0 ... EE |
             * pos:     | 0   1      15 | 16  17           239| 240 241    255|
             * pos-240: | 0   0      0  | 0   0            0  | 0   1      15 |
             * pos+112: | 112 113    127|       >= 128        |     >= 128    |
             */
            tmp = _mm_subs_epu8(pos, _mm_set1_epi8(0xF0));
            range2 = _mm_shuffle_epi8(df_ee_tbl, tmp);
            tmp = _mm_adds_epu8(pos, _mm_set1_epi8(0x70));
            range2 = _mm_add_epi8(range2, _mm_shuffle_epi8(ef_fe_tbl, tmp));

            range = _mm_add_epi8(range, range2);

            /* Load min and max values per calculated range index */
            __m128i minv = _mm_shuffle_epi8(range_min_tbl, range);
            __m128i maxv = _mm_shuffle_epi8(range_max_tbl, range);

            /* Check value range */
#if RET_ERR_IDX
            error = _mm_cmplt_epi8(input, minv);
            error = _mm_or_si128(error, _mm_cmpgt_epi8(input, maxv));
            /* 5% performance drop from this conditional branch */
            if (!_mm_testz_si128(error, error))
                break;
#else
            /* error |= (input < minv) | (input > maxv) */
            tmp = _mm_or_si128(
                      _mm_cmplt_epi8(input, minv),
                      _mm_cmpgt_epi8(input, maxv)
                  );
            error = _mm_or_si128(error, tmp);
#endif

            prev_input = input;
            prev_first_len = first_len;

            data += 16;
            len -= 16;
#if RET_ERR_IDX
            err_pos += 16;
#endif
        }

#if RET_ERR_IDX
        /* Error in first 16 bytes */
        if (err_pos == 1)
            goto do_naive;
#else
        if (!_mm_testz_si128(error, error))
            return -1;
#endif

        /* Find previous token (not 80~BF) */
        int32_t token4 = _mm_extract_epi32(prev_input, 3);
        const int8_t *token = (const int8_t *)&token4;
        int lookahead = 0;
        if (token[3] > (int8_t)0xBF)
            lookahead = 1;
        else if (token[2] > (int8_t)0xBF)
            lookahead = 2;
        else if (token[1] > (int8_t)0xBF)
            lookahead = 3;

        data -= lookahead;
        len += lookahead;
#if RET_ERR_IDX
        err_pos -= lookahead;
#endif
    }

    /* Check remaining bytes with naive method */
#if RET_ERR_IDX
    int err_pos2;
do_naive:
    err_pos2 = utf8_naive(data, len);
    if (err_pos2)
        return err_pos + err_pos2 - 1;
    return 0;
#else
    return utf8_naive(data, len);
#endif
}
static size_t ob_well_formed_len_utf8mb4(const ObCharsetInfo *cs,
                                         const char *b, const char *e,
                                         size_t pos, int *error)
{
    int len = (int)(e-b);
  if (OB_UNLIKELY(len <= 0)) {
    return 0;
  } else if (len>=15 && ascii_simd((const uint8_t *)b, len)) {
    return (size_t)len;
  }
  int err_pos = utf8_range((unsigned char *)b, len);
  if (err_pos == 0) {
    return len;
  } else {
    if (err_pos > 0)
      return err_pos - 1;
  }
  return 0;
  
}

)

OB_DECLARE_AVX2_SPECIFIC_CODE(

static inline int ascii_simd(const uint8_t *data, int len) {
  if (len >= 32) {
    const uint8_t *data2 = data + 16;

    __m128i or1 = _mm_set1_epi8(0), or2 = or1;

    while (len >= 32) {
      __m128i input1 = _mm_loadu_si128((const __m128i *)data);
      __m128i input2 = _mm_loadu_si128((const __m128i *)data2);

      or1 = _mm_or_si128(or1, input1);
      or2 = _mm_or_si128(or2, input2);

      data += 32;
      data2 += 32;
      len -= 32;
    }

    or1 = _mm_or_si128(or1, or2);
    if (_mm_movemask_epi8(_mm_cmplt_epi8(or1, _mm_set1_epi8(0)))) return 0;
  }

  return ascii_u64(data, len);
}
  /*
 * Map high nibble of "First Byte" to legal character length minus 1
 * 0x00 ~ 0xBF --> 0
 * 0xC0 ~ 0xDF --> 1
 * 0xE0 ~ 0xEF --> 2
 * 0xF0 ~ 0xFF --> 3
 */
static const int8_t _first_len_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

/* Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4) */
static const int8_t _first_range_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

/*
 * Range table, map range index to min and max values
 * Index 0    : 00 ~ 7F (First Byte, ascii)
 * Index 1,2,3: 80 ~ BF (Second, Third, Fourth Byte)
 * Index 4    : A0 ~ BF (Second Byte after E0)
 * Index 5    : 80 ~ 9F (Second Byte after ED)
 * Index 6    : 90 ~ BF (Second Byte after F0)
 * Index 7    : 80 ~ 8F (Second Byte after F4)
 * Index 8    : C2 ~ F4 (First Byte, non ascii)
 * Index 9~15 : illegal: i >= 127 && i <= -128
 */
static const uint8_t _range_min_tbl[] = {
    0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
    0xC2, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F,
    0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
    0xC2, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F,
};
static const uint8_t _range_max_tbl[] = {
    0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
    0xF4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
    0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
    0xF4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
};

/*
 * Tables for fast handling of four special First Bytes(E0,ED,F0,F4), after
 * which the Second Byte are not 80~BF. It contains "range index adjustment".
 * +------------+---------------+------------------+----------------+
 * | First Byte | original range| range adjustment | adjusted range |
 * +------------+---------------+------------------+----------------+
 * | E0         | 2             | 2                | 4              |
 * +------------+---------------+------------------+----------------+
 * | ED         | 2             | 3                | 5              |
 * +------------+---------------+------------------+----------------+
 * | F0         | 3             | 3                | 6              |
 * +------------+---------------+------------------+----------------+
 * | F4         | 4             | 4                | 8              |
 * +------------+---------------+------------------+----------------+
 */
/* index1 -> E0, index14 -> ED */
static const int8_t _df_ee_tbl[] = {
    0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0,
    0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0,
};
/* index1 -> F0, index5 -> F4 */
static const int8_t _ef_fe_tbl[] = {
    0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

static inline __m256i push_last_byte_of_a_to_b(__m256i a, __m256i b) {
  return _mm256_alignr_epi8(b, _mm256_permute2x128_si256(a, b, 0x21), 15);
}

static inline __m256i push_last_2bytes_of_a_to_b(__m256i a, __m256i b) {
  return _mm256_alignr_epi8(b, _mm256_permute2x128_si256(a, b, 0x21), 14);
}

static inline __m256i push_last_3bytes_of_a_to_b(__m256i a, __m256i b) {
  return _mm256_alignr_epi8(b, _mm256_permute2x128_si256(a, b, 0x21), 13);
}

/* 5x faster than naive method */
/* Return 0 - success, -1 - error, >0 - first error char(if RET_ERR_IDX = 1) */
int utf8_range_avx2(const unsigned char *data, int len)
{
#if  RET_ERR_IDX
    int err_pos = 1;
#endif

    if (len >= 32) {
        __m256i prev_input = _mm256_set1_epi8(0);
        __m256i prev_first_len = _mm256_set1_epi8(0);

        /* Cached tables */
        const __m256i first_len_tbl =
            _mm256_loadu_si256((const __m256i *)_first_len_tbl);
        const __m256i first_range_tbl =
            _mm256_loadu_si256((const __m256i *)_first_range_tbl);
        const __m256i range_min_tbl =
            _mm256_loadu_si256((const __m256i *)_range_min_tbl);
        const __m256i range_max_tbl =
            _mm256_loadu_si256((const __m256i *)_range_max_tbl);
        const __m256i df_ee_tbl =
            _mm256_loadu_si256((const __m256i *)_df_ee_tbl);
        const __m256i ef_fe_tbl =
            _mm256_loadu_si256((const __m256i *)_ef_fe_tbl);

#if !RET_ERR_IDX
        __m256i error1 = _mm256_set1_epi8(0);
        __m256i error2 = _mm256_set1_epi8(0);
#endif

        while (len >= 32) {
            const __m256i input = _mm256_loadu_si256((const __m256i *)data);

            /* high_nibbles = input >> 4 */
            const __m256i high_nibbles =
                _mm256_and_si256(_mm256_srli_epi16(input, 4), _mm256_set1_epi8(0x0F));

            /* first_len = legal character length minus 1 */
            /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
            /* first_len = first_len_tbl[high_nibbles] */
            __m256i first_len = _mm256_shuffle_epi8(first_len_tbl, high_nibbles);

            /* First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF */
            /* range = first_range_tbl[high_nibbles] */
            __m256i range = _mm256_shuffle_epi8(first_range_tbl, high_nibbles);

            /* Second Byte: set range index to first_len */
            /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
            /* range |= (first_len, prev_first_len) << 1 byte */
            range = _mm256_or_si256(
                    range, push_last_byte_of_a_to_b(prev_first_len, first_len));

            /* Third Byte: set range index to saturate_sub(first_len, 1) */
            /* 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF */
            __m256i tmp1, tmp2;

            /* tmp1 = (first_len, prev_first_len) << 2 bytes */
            tmp1 = push_last_2bytes_of_a_to_b(prev_first_len, first_len);
            /* tmp2 = saturate_sub(tmp1, 1) */
            tmp2 = _mm256_subs_epu8(tmp1, _mm256_set1_epi8(1));

            /* range |= tmp2 */
            range = _mm256_or_si256(range, tmp2);

            /* Fourth Byte: set range index to saturate_sub(first_len, 2) */
            /* 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF */
            /* tmp1 = (first_len, prev_first_len) << 3 bytes */
            tmp1 = push_last_3bytes_of_a_to_b(prev_first_len, first_len);
            /* tmp2 = saturate_sub(tmp1, 2) */
            tmp2 = _mm256_subs_epu8(tmp1, _mm256_set1_epi8(2));
            /* range |= tmp2 */
            range = _mm256_or_si256(range, tmp2);

            /*
             * Now we have below range indices caluclated
             * Correct cases:
             * - 8 for C0~FF
             * - 3 for 1st byte after F0~FF
             * - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
             * - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
             *         3rd byte after F0~FF
             * - 0 for others
             * Error cases:
             *   9,10,11 if non ascii First Byte overlaps
             *   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error
             */

            /* Adjust Second Byte range for special First Bytes(E0,ED,F0,F4) */
            /* Overlaps lead to index 9~15, which are illegal in range table */
            __m256i shift1, pos, range2;
            /* shift1 = (input, prev_input) << 1 byte */
            shift1 = push_last_byte_of_a_to_b(prev_input, input);
            pos = _mm256_sub_epi8(shift1, _mm256_set1_epi8(0xEF));
            /*
             * shift1:  | EF  F0 ... FE | FF  00  ... ...  DE | DF  E0 ... EE |
             * pos:     | 0   1      15 | 16  17           239| 240 241    255|
             * pos-240: | 0   0      0  | 0   0            0  | 0   1      15 |
             * pos+112: | 112 113    127|       >= 128        |     >= 128    |
             */
            tmp1 = _mm256_subs_epu8(pos, _mm256_set1_epi8(static_cast<uint8_t>(240)));
            range2 = _mm256_shuffle_epi8(df_ee_tbl, tmp1);
            tmp2 = _mm256_adds_epu8(pos, _mm256_set1_epi8(112));
            range2 = _mm256_add_epi8(range2, _mm256_shuffle_epi8(ef_fe_tbl, tmp2));

            range = _mm256_add_epi8(range, range2);

            /* Load min and max values per calculated range index */
            __m256i minv = _mm256_shuffle_epi8(range_min_tbl, range);
            __m256i maxv = _mm256_shuffle_epi8(range_max_tbl, range);

            /* Check value range */
#if RET_ERR_IDX
            __m256i error = _mm256_cmpgt_epi8(minv, input);
            error = _mm256_or_si256(error, _mm256_cmpgt_epi8(input, maxv));
            /* 5% performance drop from this conditional branch */
            if (!_mm256_testz_si256(error, error))
                break;
#else
            error1 = _mm256_or_si256(error1, _mm256_cmpgt_epi8(minv, input));
            error2 = _mm256_or_si256(error2, _mm256_cmpgt_epi8(input, maxv));
#endif

            prev_input = input;
            prev_first_len = first_len;

            data += 32;
            len -= 32;
#if RET_ERR_IDX
            err_pos += 32;
#endif
        }

#if RET_ERR_IDX
        /* Error in first 16 bytes */
        if (err_pos == 1)
            goto do_naive;
#else
        __m256i error = _mm256_or_si256(error1, error2);
        if (!_mm256_testz_si256(error, error))
            return -1;
#endif

        /* Find previous token (not 80~BF) */
        int32_t token4 = _mm256_extract_epi32(prev_input, 7);
        const int8_t *token = (const int8_t *)&token4;
        int lookahead = 0;
        if (token[3] > (int8_t)0xBF)
            lookahead = 1;
        else if (token[2] > (int8_t)0xBF)
            lookahead = 2;
        else if (token[1] > (int8_t)0xBF)
            lookahead = 3;

        data -= lookahead;
        len += lookahead;
#if RET_ERR_IDX
        err_pos -= lookahead;
#endif
    }

    /* Check remaining bytes with naive method */
#if RET_ERR_IDX
    int err_pos2;
do_naive:
    err_pos2 = utf8_naive(data, len);
    if (err_pos2)
        return err_pos + err_pos2 - 1;
    return 0;
#else
    return utf8_naive(data, len);
#endif
}

static size_t ob_well_formed_len_utf8mb4(const ObCharsetInfo *cs,
                                         const char *b, const char *e,
                                         size_t pos, int *error)
{
  int len = (int)(e-b);
  if (OB_UNLIKELY(len <= 0)) {
    return 0;
  } else if (len>=15 && ascii_simd((const uint8_t *)b, len)) {
    return (size_t)len;
  }
  int err_pos = utf8_range_avx2((unsigned char *)b, len);
  if (err_pos == 0) {
    return len;
  } else {
    if (err_pos > 0)
      return err_pos - 1;
  }
  return 0;
  
}
)



static int ob_mb_wc_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)),
                 ob_wc_t * pwc, const uchar *s, const uchar *e)
{
  uchar c;
  if (s >= e)
    return OB_CS_TOOSMALL;
  c= s[0];
  if (c < 0x80)
  {
    *pwc= c;
    return 1;
  }
  else if (c < 0xc2)
    return OB_CS_ILSEQ;
  else if (c < 0xe0)
  {
    if (s + 2 > e) /* We need 2 characters */
      return OB_CS_TOOSMALL2;
    if (!(IS_CONTINUATION_BYTE(s[1])))
      return OB_CS_ILSEQ;
    *pwc= ((ob_wc_t) (c & 0x1f) << 6) | (ob_wc_t) (s[1] ^ 0x80);
    return 2;
  }
  else if (c < 0xf0)
  {
    if (s + 3 > e) /* We need 3 characters */
      return OB_CS_TOOSMALL3;
    if (!(IS_CONTINUATION_BYTE(s[1]) && IS_CONTINUATION_BYTE(s[2]) &&
          (c >= 0xe1 || s[1] >= 0xa0)))
      return OB_CS_ILSEQ;
    *pwc= ((ob_wc_t) (c & 0x0f) << 12)   |
          ((ob_wc_t) (s[1] ^ 0x80) << 6) |
           (ob_wc_t) (s[2] ^ 0x80);
    return 3;
  }
  else if (c < 0xf5)
  {
    if (s + 4 > e) /* We need 4 characters */
      return OB_CS_TOOSMALL4;
    if (!(IS_CONTINUATION_BYTE(s[1]) &&
          IS_CONTINUATION_BYTE(s[2]) &&
          IS_CONTINUATION_BYTE(s[3]) &&
          (c >= 0xf1 || s[1] >= 0x90) &&
          (c <= 0xf3 || s[1] <= 0x8F)))
      return OB_CS_ILSEQ;
    *pwc = ((ob_wc_t) (c & 0x07) << 18)    |
           ((ob_wc_t) (s[1] ^ 0x80) << 12) |
           ((ob_wc_t) (s[2] ^ 0x80) << 6)  |
            (ob_wc_t) (s[3] ^ 0x80);
    return 4;
  }
  return OB_CS_ILSEQ;
}

static int ob_wc_mb_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)),
                 ob_wc_t w_char, unsigned char *r, unsigned char *end)
{
  int bytes = 0;
  int ret = 0;
  int64_t len = (int64_t)(end - r);
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_CS_TOOSMALL;
  } else if (w_char < 0x80) { //7	U+0000	U+007F	1	0xxxxxxx
    bytes = 1;
  } else if (w_char < 0x800) {//11	U+0080	U+07FF	2	110xxxxx	10xxxxxx
    bytes = 2;
  } else if (w_char < 0x10000) {//16	U+0800	U+FFFF	3	1110xxxx	10xxxxxx	10xxxxxx
    bytes = 3;
  } else if (w_char < 0x200000) {// 21	U+10000	U+1FFFFF 4	11110xxx	10xxxxxx	10xxxxxx	10xxxxxx
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
      case 4: r[3] = (unsigned char) (0x80 | (w_char & 0x3f)); w_char >>= 6; w_char |= 0x10000;
      case 3: r[2] = (unsigned char) (0x80 | (w_char & 0x3f)); w_char >>= 6; w_char |= 0x800;
      case 2: r[1] = (unsigned char) (0x80 | (w_char & 0x3f)); w_char >>= 6; w_char |= 0xc0;
      case 1: r[0] = (unsigned char) w_char;
    }
    ret = bytes;
  }
  return ret;
}

static inline void ob_tolower_utf8mb4(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page = uni_plane->page[(*wc >> 8)];
    if (OB_NOT_NULL(page)) {
      *wc= page[*wc & 0xFF].tolower;
  }
  }
  return;
}


static inline void ob_toupper_utf8mb4(ObUnicaseInfo *uni_plane, ob_wc_t *wc)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page = uni_plane->page[(*wc >> 8)];
    if (OB_NOT_NULL(page)) {
      *wc= page[*wc & 0xFF].toupper;
  }
  }
  return;
}

static size_t ob_caseup_utf8mb4(const ObCharsetInfo *cs, char *src, size_t srclen,
                  char *dst, size_t dstlen)
{
  ob_wc_t w_char;
  int srcres = 0, dstres = 0;
  char *srcend = src + srclen, *dstend = dst + dstlen, *dst0 = dst;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  ob_charset_assert(src != dst || cs->caseup_multiply == 1);
  while ((src < srcend)) {
    if ((srcres = ob_mb_wc_utf8mb4(cs, &w_char, (unsigned char *) src, (unsigned char*) srcend)) <= 0) {
      break;
    }
    ob_toupper_utf8mb4(uni_plane, &w_char);
    if ((dstres = ob_wc_mb_utf8mb4(cs, w_char, (unsigned char*) dst, (unsigned char*) dstend)) <= 0) {
      break;
    } else {
      src += srcres;
      dst += dstres;
    }
  }
  return (size_t) (dst - dst0);
}

static size_t ob_casedn_utf8mb4(const ObCharsetInfo *cs,
                  char *src, size_t srclen,
                  char *dst, size_t dstlen)
{
  ob_wc_t w_char;
  int srcres = 0, dstres = 0;
  char *srcend = src + srclen, *dstend= dst + dstlen, *dst0= dst;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  ob_charset_assert(src != dst || cs->casedn_multiply == 1);
  while ((src < srcend) &&
         (srcres = ob_mb_wc_utf8mb4(cs, &w_char,
                                   (unsigned char*) src, (unsigned char*) srcend)) > 0) {
    ob_tolower_utf8mb4(uni_plane, &w_char);
    if ((dstres = ob_wc_mb_utf8mb4(cs, w_char, (unsigned char*) dst, (unsigned char*) dstend)) <= 0) {
      break;
    } else {
      src += srcres;
      dst += dstres;
    }
  }
  return (size_t) (dst - dst0);
}


static inline int bincmp_utf8mb4(const unsigned char *src, const unsigned char *se,
                                 const unsigned char *dst, const unsigned char *te)
{
  int srclen = (int) (se - src), dstlen= (int) (te - dst);
  int len = srclen < dstlen ? srclen : dstlen;
  int cmp = memcmp(src, dst, len);
  return cmp ? cmp : srclen - dstlen;
}

static inline void ob_tosort_unicode(ObUnicaseInfo *uni_plane, ob_wc_t *wc, unsigned int flags)
{
  if (*wc <= uni_plane->maxchar) {
    const ObUnicaseInfoChar *page;
    if (OB_NOT_NULL(page = uni_plane->page[*wc >> 8])) {
      *wc= (flags & OB_CS_LOWER_SORT) ?
           page[*wc & 0xFF].tolower :
           page[*wc & 0xFF].sort;
    }
  } else {
    *wc = OB_CS_REPLACEMENT_CHARACTER;
  }
}

static int ob_strnncoll_utf8mb4(const ObCharsetInfo *cs,
                     const unsigned char *src, size_t srclen,
                     const unsigned char *dst, size_t dstlen,
                     bool t_is_prefix)
{
  ob_wc_t src_wc = 0, dst_wc = 0;
  const unsigned char *se = src + srclen;
  const unsigned char *te = dst + dstlen;
  ObUnicaseInfo *uni_plane = cs->caseinfo;
  while ( src < se && dst < te ) {
    int s_res = ob_mb_wc_utf8mb4(cs, &src_wc, src, se);
    int t_res = ob_mb_wc_utf8mb4(cs, &dst_wc, dst, te);
    if ( s_res <= 0 || t_res <= 0 ) {
      return bincmp_utf8mb4(src, se, dst, te);
    }
    ob_tosort_unicode(uni_plane, &src_wc, cs->state);
    ob_tosort_unicode(uni_plane, &dst_wc, cs->state);
    if ( src_wc != dst_wc ) {
      return src_wc > dst_wc ? 1 : -1;
    } else {
      src += s_res;
      dst += t_res;
    }
  }
  return (int) (t_is_prefix ? (dst - te) : ((se - src) - (te - dst)));
}

int __attribute__ ((noinline))  ob_strnncollsp_utf8mb4_help(
    const unsigned char **s_, size_t srclen,
    const unsigned char **t_, size_t dstlen,
    const unsigned char **se_, const unsigned char **te_,
    bool diff_if_only_endspace_difference, int *has_returned, int *res_)
{
  const unsigned char *src = *s_;
  const unsigned char *dst = *t_;
  const unsigned char *se = *se_;
  const unsigned char *te = *te_;
  int res = *res_;
  *has_returned = 0;
  int swap= 1;
  if (srclen != dstlen) {
    if (diff_if_only_endspace_difference) {
      res= 1;
    }
    if (srclen < dstlen) {
      srclen= dstlen;
      src= dst;
      se= te;
      swap= -1;
      res= -res;
    }
    while (src < se) {
      if (*src != ' ') {
        *has_returned = 1;
        break;
      }
      src++;
    }
  }
  *s_ = src;
  *t_ = dst;
  *se_ = se;
  *te_ = te;
  *res_ = res;
  if (*has_returned == 1) {
    return (!diff_if_only_endspace_difference && (*src < ' ')) ? -swap : swap;
  }
  return 0;
}

static int ob_strnncollsp_utf8mb4(const ObCharsetInfo *cs,
                       const unsigned char *src, size_t srclen,
                       const unsigned char *dst, size_t dstlen,
                       bool diff_if_only_endspace_difference)
{
  int res;
  ob_wc_t src_wc = 0, dst_wc = 0;
  const unsigned char *se= src + srclen, *te= dst + dstlen;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  while ( src < se && dst < te ) {
    int s_res= ob_mb_wc_utf8mb4(cs, &src_wc, src, se);
    int t_res= ob_mb_wc_utf8mb4(cs, &dst_wc, dst, te);
    if ( s_res <= 0 || t_res <= 0 ) {
      return bincmp_utf8mb4(src, se, dst, te);
    }
    ob_tosort_unicode(uni_plane, &src_wc, cs->state);
    ob_tosort_unicode(uni_plane, &dst_wc, cs->state);
    if ( src_wc != dst_wc ) {
      return src_wc > dst_wc ? 1 : -1;
    } else {
      src +=s_res;
      dst +=t_res;
    }
  }
  srclen = (size_t) (se-src);
  dstlen = (size_t) (te-dst);
  res= 0;
  int has_returned = 0;
  int tmp = ob_strnncollsp_utf8mb4_help(
      &src, srclen,
      &dst, dstlen,
      &se, &te,
      diff_if_only_endspace_difference, &has_returned, &res);
  return has_returned ? tmp : res;
}

static size_t ob_strxfrm_pad_nweights_unicode(unsigned char *str, unsigned char *strend, size_t nweights)
{
  ob_charset_assert(str && str <= strend);
  unsigned char *str0 = str;
  for (; str < strend && nweights; nweights--) {
    *str++= 0x00;
    if (str < strend) {
      *str++= 0x20;
    }
  }
  return str - str0;
}

static size_t ob_strxfrm_pad_unicode(unsigned char *str, unsigned char *strend)
{
  unsigned char *str0= str;
  ob_charset_assert(str && str <= strend);
  while (str < strend) {
    *str++= 0x00;
    if (str < strend) {
      *str++= 0x20;
    }
  }
  return str - str0;
}

void ob_strnxfrm_unicode_help(uchar **dst,
                              uchar **de,
                              uint nweights,
                              uint flags,
                              uchar **dst0)
{
  if (*dst < *de && nweights && (flags & OB_STRXFRM_PAD_WITH_SPACE))
    *dst += ob_strxfrm_pad_nweights_unicode(*dst, *de, nweights);
  ob_strxfrm_desc_and_reverse(*dst0, *dst, flags, 0);
  if ((flags & OB_STRXFRM_PAD_TO_MAXLEN) && *dst < *de)
    *dst += ob_strxfrm_pad_unicode(*dst, *de);
}
size_t ob_strnxfrm_unicode(const ObCharsetInfo *cs,
                    uchar *dst, size_t dstlen, uint nweights,
                    const uchar *src, size_t srclen, uint flags, bool *is_valid_unicode)
{
  ob_wc_t wc;
  int res;
  uchar *dst0= dst;
  uchar *de= dst + dstlen;
  const uchar *se= src + srclen;
  ObUnicaseInfo *uni_plane= (cs->state & OB_CS_BINSORT) ? NULL : cs->caseinfo;
  wc = 0;
  ob_charset_assert(src);
  *is_valid_unicode = 1;
  for (; dst < de && nweights; nweights--)
  {
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    if (uni_plane)
      ob_tosort_unicode(uni_plane, &wc, cs->state);
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
  }
  ob_strnxfrm_unicode_help(&dst,&de, nweights, flags, &dst0);
  return dst - dst0;
}

size_t ob_varlen_encoding_for_memcmp(const struct ObCharsetInfo* cs,
                              uchar* dst, size_t dst_len, uint nweights,
                              const uchar *src, size_t src_len,
                              bool *is_valid_unicode, bool is_sort)
{
  ob_wc_t wc;
  int res;
  uchar *dst0= dst;
  uchar *de= dst + dst_len;
  const uchar *se= src + src_len;
  ObUnicaseInfo *uni_plane= (cs->state & OB_CS_BINSORT) ? NULL : cs->caseinfo;
  wc = 0;
  ob_charset_assert(src);
  *is_valid_unicode = 1;
  for (;*is_valid_unicode && src < se && dst < de && nweights; nweights--)
  {
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    // if is 0x000000
    if (wc == 0x00000000) {
      res = cs->cset->wc_mb(cs, wc, dst, de);
      dst += res;
      wc = 0x00000001;
    }
    if (1 == is_sort && NULL != uni_plane)
      ob_tosort_unicode(uni_plane, &wc, cs->state);
    // replace code above with code below.
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
  }
  // adds 0x00, 0x00
  *dst = 0x00;
  *(dst+1) = 0x00;
  dst += 2;
  //ob_strnxfrm_unicode_help(&dst,&de, nweights, flags, &dst0);
  return dst - dst0;
}

uint16_t find_space_char_count(const uchar* src, const uchar* se)
{
  int space_cnt = 1;
  while ((src + space_cnt) < se && *(src + space_cnt) == 0x20) space_cnt++;
  if ((src + space_cnt) < se) return space_cnt;
  else return 0;
}

size_t ob_varlen_encoding_for_spacecmp(const struct ObCharsetInfo* cs,
                              uchar* dst, size_t dst_len, uint nweights,
                              const uchar *src, size_t src_len,
                              bool *is_valid_unicode, bool is_sort)
{
  ob_wc_t wc;
  int res;
  uchar *dst0= dst;
  uchar *de= dst + dst_len;
  const uchar *se= src + src_len;
  ObUnicaseInfo *uni_plane= (cs->state & OB_CS_BINSORT) ? NULL : cs->caseinfo;
  wc = 0;
  ob_charset_assert(src);
  *is_valid_unicode = 1;
  uint16_t space_cnt = 0xFFFF;
  for (;*is_valid_unicode && src < se && dst < de && nweights; nweights--)
  {
    // for reslovable multiple bytes, only space's first byte is 0x20,
    // in utf8 encoding scheme.
    if (*src == 0x20) {
      space_cnt = find_space_char_count(src, se);
      if (space_cnt == 0) break;
      *(dst) = 0x20;
      if (*(src+space_cnt) > 0x20){
        *(dst+1)=0x21;
        // flip
        uint16_t tmp_cnt = space_cnt ^ 0xFFFF;
        *(dst+2)=*((uchar*)&tmp_cnt+1);
        *(dst+3)=*(&tmp_cnt);
      } else {
        *(dst+1) = 0x19;
        *(dst+2)=*((uchar*)&space_cnt+1);
        *(dst+3)=*(&space_cnt);
      }
      dst += 4;
      src += space_cnt;
    }
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    if (1 == is_sort && NULL != uni_plane)
      ob_tosort_unicode(uni_plane, &wc, cs->state);
    // replace code above with code below.
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
    //abort();
  }
  // adds 0x20, 0x20
  *dst = 0x20;
  *(dst+1) = 0x20;
  dst += 2;
  //ob_strnxfrm_unicode_help(&dst,&de, nweights, flags, &dst0);
  return dst - dst0;
}
size_t ob_strnxfrm_unicode_varlen(const struct ObCharsetInfo* cs,
                             uchar* dst, size_t dst_len, uint nweights,
                             const uchar *src, size_t srclen,
                             bool is_memcmp, bool *is_valid_unicode)
{
  if (is_memcmp) {
    return ob_varlen_encoding_for_memcmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 1);
  } else {
    return ob_varlen_encoding_for_spacecmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 1);
  }
}

static int ob_wildcmp_unicode_impl_help(const ObCharsetInfo *cs,
                        const char **str_,const char **str_end_,
                        ob_charset_conv_mb_wc mb_wc,
                        const char **wild_str_, const char **wild_end_,
                        int escape_char, int w_one, int w_many,
                        ob_wc_t *s_wc_,
                        ob_wc_t *w_wc_,
                        int *scan_,
                        int *result_,
                        ObUnicaseInfo *weights,
                        int *has_returned)
{
   int ret = 0;
   const char *str = *str_;
   const char *str_end = *str_end_;
   const char *wild_str = *wild_str_;
   const char *wild_end = *wild_end_;
   ob_wc_t src_wc = *s_wc_;
   ob_wc_t w_wc = *w_wc_;
   int scan = *scan_;
   int result = *result_;
   *has_returned = 0;
   while (1) {
     bool escaped= 0;
     if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                      (const unsigned char*)wild_end)) <= 0) {
       ret = 1;
       *has_returned = 1;
       break;
     } else if (w_wc != (ob_wc_t) escape_char && w_wc == (ob_wc_t) w_many) {
       result = 1;
       break;
     }
     wild_str += scan;
     if (w_wc ==  (ob_wc_t) escape_char && wild_str < wild_end) {
       if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                        (const unsigned char*)wild_end)) <= 0) {
         ret = 1;
         *has_returned = 1;
         break;
       } else {
          wild_str += scan;
       escaped = 1;
     }
     }
     if ((scan = mb_wc(cs, &src_wc, (const unsigned char*)str,
                      (const unsigned char*)str_end)) <= 0) {
       ret = 1;
       *has_returned = 1;
       break;
     } else {
     str += scan;
     }

     if (!escaped && w_wc == (ob_wc_t) w_one) {
       result = 1;
     } else {
       if (weights) {
         ob_tosort_unicode(weights, &src_wc, cs->state);
         ob_tosort_unicode(weights, &w_wc, cs->state);
       }
       if (src_wc != w_wc) {
         ret = 1;
         *has_returned = 1;
         break;
     }
     }
     if (wild_str == wild_end) {
       ret = (str != str_end);
       *has_returned = 1;
       break;
     }
   }
   *str_ = str;
   *str_end_ = str_end;
   *wild_str_ = wild_str;
   *wild_end_ = wild_end;
   *s_wc_ = src_wc;
   *w_wc_ = w_wc;
   *scan_ = scan;
   *result_ = result;
   return ret;
}

static int ob_wildcmp_unicode_impl(const ObCharsetInfo *cs,
                        const char *str,const char *str_end,
                        const char *wild_str,const char *wild_end,
                        int escape_char, int w_one, int w_many,
                        ObUnicaseInfo *weights, int recurse_level)
{
  int result= -1;
  ob_wc_t src_wc = 0;
  ob_wc_t w_wc = 0;
  int scan = 0;
  int (*mb_wc)(const ObCharsetInfo *, ob_wc_t *, const unsigned char *, const unsigned char *);
  mb_wc= cs->cset->mb_wc;

  while (wild_str != wild_end) {
    int has_returned = 0;
    int tmp = ob_wildcmp_unicode_impl_help(cs,
        &str,&str_end,
        mb_wc,
        &wild_str, &wild_end,
        escape_char,
        w_one,
        w_many,
        &src_wc,
        &w_wc,
        &scan,
        &result,
        weights,
        &has_returned);
    if (has_returned == 1) {
      return tmp;
    } else if (w_wc == (ob_wc_t) w_many) {
      while (wild_str != wild_end) {
        if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                         (const unsigned char*)wild_end)) <= 0) {
          return 1;
        } else if (w_wc == (ob_wc_t)w_many) {
          wild_str+= scan;
          continue;
        } else if (w_wc == (ob_wc_t)w_one) {
          wild_str+= scan;
          if ((scan= mb_wc(cs, &src_wc, (const unsigned char*)str,
                           (const unsigned char*)str_end)) <=0) {
            return 1;
          } else {
          str+= scan;
          continue;
        }
        }
        break;
      }

      if (wild_str == wild_end) {
        return 0;
      } else if (str == str_end) {
        return -1;
      } else if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                       (const unsigned char*)wild_end)) <=0) {
        return 1;
      } else {
        wild_str+= scan;
      }

      if (w_wc ==  (ob_wc_t)escape_char) {
        if (wild_str < wild_end) {
          if ((scan= mb_wc(cs, &w_wc, (const unsigned char*)wild_str,
                           (const unsigned char*)wild_end)) <=0) {
            return 1;
          } else {
            wild_str+= scan;
          }
        }
      }

      while (1) {
        while (str != str_end) {
          if ((scan= mb_wc(cs, &src_wc, (const unsigned char*)str,
                           (const unsigned char*)str_end)) <=0) {
            return 1;
          } else if (weights) {
            ob_tosort_unicode(weights, &src_wc, cs->state);
            ob_tosort_unicode(weights, &w_wc, cs->state);
          }
          if (src_wc == w_wc) {
            break;
          } else {
          str+= scan;
        }
        }
        if (str == str_end) {
          return -1;
        } else {
        str+= scan;
        }
        result= ob_wildcmp_unicode_impl(cs, str, str_end, wild_str, wild_end,
                                        escape_char, w_one, w_many,
                                        weights, recurse_level + 1);
        if (result <= 0) return result;
      }
    }
  }
  return (str != str_end ? 1 : 0);
}

int ob_wildcmp_unicode(const ObCharsetInfo *cs,
                   const char *str,const char *str_end,
                   const char *wild_str,const char *wild_end,
                   int escape_char, int w_one, int w_many,
                   ObUnicaseInfo *weights)
{
  return ob_wildcmp_unicode_impl(cs, str, str_end, wild_str, wild_end, escape_char, w_one, w_many, weights, 1);
}

static int ob_wildcmp_utf8mb4(const ObCharsetInfo *cs,
                   const char *str, const char *strend,
                   const char *wild_str, const char *wild_end,
                   int escape_char, int w_one, int w_many)
{
  return ob_wildcmp_unicode(cs, str, strend, wild_str, wild_end, escape_char, w_one, w_many, cs->caseinfo);
}



static inline void ob_hash_add(unsigned long int *n1, unsigned long int *n2, unsigned int ch)
{
  n1[0]^= (((n1[0] & 63) + n2[0]) * (ch)) + (n1[0] << 8);
  n2[0]+= 3;
}


static void ob_hash_sort_utf8mb4(const ObCharsetInfo *cs, const unsigned char *src, size_t srclen,
               unsigned long int *n1, unsigned long int *n2, const bool calc_end_space, hash_algo hash_algo)
{
  ob_wc_t wc;
  int res;
  const unsigned char *end= src + srclen;
  ObUnicaseInfo *uni_plane= cs->caseinfo;
  int length = 0;
  unsigned char data[HASH_BUFFER_LENGTH];
  if (!calc_end_space) {
    while (end > src && end[-1] == ' ')
      end--;
  }

  if (NULL == hash_algo) {
    while ((res= ob_mb_wc_utf8mb4(cs, &wc, (unsigned char*) src, (unsigned char*) end)) > 0) {
      ob_tosort_unicode(uni_plane, &wc, cs->state);
      ob_hash_add(n1, n2, (unsigned int) (wc & 0xFF));
      ob_hash_add(n1, n2, (unsigned int) (wc >> 8)  & 0xFF);
      if (wc > 0xFFFF) {
        ob_hash_add(n1, n2, (unsigned int) (wc >> 16) & 0xFF);
      }
      src+= res;
    }
  } else {
    while ((res= ob_mb_wc_utf8mb4(cs, &wc, (unsigned char*) src, (unsigned char*) end)) > 0) {
      ob_tosort_unicode(uni_plane, &wc, cs->state);
      if (length > HASH_BUFFER_LENGTH - 2 || (HASH_BUFFER_LENGTH - 2 == length && wc > 0xFFFF)) {
        *n1 = hash_algo((void*) &data, length, *n1);
        length = 0;
      }
      data[length++] = (unsigned char)wc;
      data[length++] = (unsigned char)(wc >> 8);
      if (wc > 0xFFFF) {
        data[length++] = (unsigned char)(wc >> 16);
      }
      src+= res;
    }
    if (length > 0) {
      *n1 = hash_algo((void*) &data, length, *n1);
    }
  }
}

size_t ob_strnxfrm_unicode_full_bin(const ObCharsetInfo *cs,
                             unsigned char *dst, size_t dstlen, unsigned int nweights,
                             const unsigned char *src, size_t srclen, unsigned int flags, bool *is_valid_unicode)
{
  ob_wc_t wc;
  unsigned char *dst0= dst;
  unsigned char *de= dst + dstlen;
  const unsigned char *se = src + srclen;
  wc = 0;
  ob_charset_assert(src);
  ob_charset_assert(cs->state & OB_CS_BINSORT);
  *is_valid_unicode = 1;
  for ( ; dst < de && nweights; nweights--) {
    int res;
    if ((res= cs->cset->mb_wc(cs, &wc, src, se)) <= 0) {
      if (src < se) {
        *is_valid_unicode = 0;
      }
      break;
    }
    src+= res;
    if ((res= cs->cset->wc_mb(cs, wc, dst, de)) <= 0)
      break;
    dst+= res;
  }
  if (flags & OB_STRXFRM_PAD_WITH_SPACE)
  {
    for ( ; dst < de && nweights; nweights--)
    {
      *dst++= 0x00;
      if (dst < de)
      {
        *dst++= 0x00;
        if (dst < de)
          *dst++= 0x20;
      }
    }
  }
  ob_strxfrm_desc_and_reverse(dst0, dst, flags, 0);
  if (flags & OB_STRXFRM_PAD_TO_MAXLEN)
  {
    while (dst < de)
    {
      *dst++= 0x00;
      if (dst < de)
      {
        *dst++= 0x00;
        if (dst < de)
          *dst++= 0x20;
      }
    }
  }
  return dst - dst0;
}

size_t ob_strnxfrm_unicode_full_bin_varlen(const struct ObCharsetInfo* cs,
                             uchar* dst, size_t dst_len, uint nweights,
                             const uchar *src, size_t srclen,
                             bool is_memcmp, bool *is_valid_unicode)
{
  if (is_memcmp) {
    return ob_varlen_encoding_for_memcmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 0);
  } else {
    return ob_varlen_encoding_for_spacecmp(cs, dst, dst_len, nweights,
                              src, srclen, is_valid_unicode, 0);
  }
}

size_t ob_strnxfrmlen_utf8mb4(const ObCharsetInfo *cs __attribute__((unused)), size_t len)
{
  return (len * 2 + 2) / 4;
}

size_t ob_strnxfrmlen_unicode_full_bin(const ObCharsetInfo *cs, size_t len)
{
  return ((len + 3) / cs->mbmaxlen) * 3;
}

int ob_mb_wc_utf8mb4_thunk(const ObCharsetInfo *cs __attribute__((unused)),
                           ob_wc_t *pwc, const uchar *s, const uchar *e) {
  return ob_mb_wc_utf8mb4(cs, pwc, s, e);
}

typedef size_t (*well_formed_len_ptr)(const struct ObCharsetInfo *, const char *, const char *, size_t, int *);

well_formed_len_ptr select_well_formed_len() {
#if OB_USE_MULTITARGET_CODE
  if (oceanbase::common::is_arch_supported(oceanbase::ObTargetArch::AVX2)) {
    return specific::avx2::ob_well_formed_len_utf8mb4;
  } else if (oceanbase::common::is_arch_supported(oceanbase::ObTargetArch::SSE42)) {
    return specific::sse42::ob_well_formed_len_utf8mb4;
  } else {
    return specific::normal::ob_well_formed_len_utf8mb4;
  }
#else
  return specific::normal::ob_well_formed_len_utf8mb4;
#endif
}

ObCharsetHandler ob_charset_utf8mb4_handler=
{
  ob_ismbchar_utf8mb4,
  ob_mbcharlen_utf8mb4,
  ob_numchars_mb,
  ob_charpos_mb,
  ob_max_bytes_charpos_mb,
  select_well_formed_len(),
  ob_lengthsp_8bit,
  ob_mb_wc_utf8mb4,
  ob_wc_mb_utf8mb4,
  ob_mb_ctype_mb,
  ob_caseup_utf8mb4,
  ob_casedn_utf8mb4,
  ob_fill_8bit,
  ob_strntol_8bit,
  ob_strntoul_8bit,
  ob_strntoll_8bit,
  ob_strntoull_8bit,
  ob_strntod_8bit,
  //ob_strtoll10_8bit,
  ob_strntoull10rnd_8bit,
  ob_scan_8bit
};

static ObCollationHandler ob_collation_utf8mb4_general_ci_handler=
{
  NULL,
  NULL,
  ob_strnncoll_utf8mb4,
  ob_strnncollsp_utf8mb4,
  ob_strnxfrm_unicode,
  ob_strnxfrmlen_utf8mb4,
  ob_strnxfrm_unicode_varlen,
  ob_like_range_mb,
  ob_wildcmp_utf8mb4,
  NULL,
  ob_instr_mb,
  ob_hash_sort_utf8mb4,
  ob_propagate_complex
};

static ObCollationHandler ob_collation_utf8mb4_bin_handler =
{
  NULL,
  NULL,
  ob_strnncoll_mb_bin,
  ob_strnncollsp_mb_bin,
  ob_strnxfrm_unicode_full_bin,
  ob_strnxfrmlen_unicode_full_bin,
  NULL,
  ob_like_range_mb,
  ob_wildcmp_mb_bin,
  NULL,
  ob_instr_mb,
  ob_hash_sort_mb_bin,
  ob_propagate_simple
};

ObCharsetInfo ob_charset_utf8mb4_general_ci=
{
  45,0,0,
  OB_CS_COMPILED|OB_CS_PRIMARY|OB_CS_STRNXFRM|OB_CS_UNICODE|OB_CS_UNICODE_SUPPLEMENT|OB_CS_CI,
  OB_UTF8MB4,
  OB_UTF8MB4_GENERAL_CI,
  "UTF-8 Unicode",
  NULL,
  NULL,
  ctype_utf8mb4,
  to_lower_utf8mb4,
  to_upper_utf8mb4,
  to_upper_utf8mb4,
  NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  1,
  1,
  1,
  1,
  4,
  1,
  0,
  0xFFFF,
  ' ',
  0,
  1,
  1,
  &ob_charset_utf8mb4_handler,
  &ob_collation_utf8mb4_general_ci_handler,
  PAD_SPACE
};

ObCharsetInfo ob_charset_utf8mb4_bin=
{
  46,0,0,
  OB_CS_COMPILED|OB_CS_BINSORT|OB_CS_STRNXFRM|
  OB_CS_UNICODE|OB_CS_UNICODE_SUPPLEMENT,
  OB_UTF8MB4,
  OB_UTF8MB4_BIN,
  "UTF-8 Unicode",
  NULL,
  NULL,
  ctype_utf8mb4,
  to_lower_utf8mb4,
  to_upper_utf8mb4,
  NULL,
  NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  1,
  1,
  1,
  1,
  4,
  1,
  0,
  0xFFFF,
  ' ',
  0,
  1,
  1,
  &ob_charset_utf8mb4_handler,
  &ob_collation_utf8mb4_bin_handler,
  PAD_SPACE
};
