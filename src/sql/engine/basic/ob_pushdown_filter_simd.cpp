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

#define USING_LOG_PREFIX SQL_ENG

#if defined(__x86_64__)
#include <immintrin.h>
#endif

#include <stdlib.h>
#include "ob_pushdown_filter.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

#if defined(__x86_64__)
// Mark the null datums:
//   set %vec to 1 if NULL
//   keep %vec bit unchanged for not null
//
// ObDatum is composed by 8 byte ptr_ + 4 byte len_, so we can load 6 ObDatums' len_ to __m512i
// if we start from len_'s address. If we reinterpret the loaded __m512i as int32 array,
// the ObDatum::len_'s index is: 0, 3, 6, 9, 12, 15.
inline void mark_null_datums(const ObDatum *datums, const int64_t size, ObBitVector &vec)
{
  const static int64_t DATUMS_PRE_VEC = 6;
  const static int64_t LEN_FIELD_OFF = 8; // sizeof(ObDatum::ptr_)
  const static int64_t DATUMS_PER_LOOP = DATUMS_PRE_VEC * 4; // process 4 __m512i vector per loop

  static_assert(12 == sizeof(ObDatum), "unexpected ObDatum size");
  static_assert(DATUMS_PER_LOOP % CHAR_BIT == 0, "unexpected datums per loop");

  int64_t pos = 0;
  __m512i cmp_val = _mm512_set1_epi32(INT32_MAX);
  __m512i idx = _mm512_set_epi32(
      2, 2, 2, 2, // not used
      15 | 16, 12 | 16, 9 | 16, 6 | 16, 3 | 16, 0 | 16, // set fourth bit to 1 to select %b
      15, 12, 9, 6, 3, 0 // select %a
      );

  const int8_t cmp_mask = 6; // _MM_CMPINT_NLE

  for (; pos + DATUMS_PER_LOOP < size; pos += DATUMS_PER_LOOP) {
    const char *mem = (const char *)&datums[pos];
    __m512i a = _mm512_loadu_si512(mem + LEN_FIELD_OFF);
    __m512i b = _mm512_loadu_si512(mem + LEN_FIELD_OFF + sizeof(ObDatum) * DATUMS_PRE_VEC * 1);
    __m512i c = _mm512_permutex2var_epi32(a, idx, b);
    uint32_t r1 = _mm512_cmp_epu32_mask(c, cmp_val, cmp_mask);
    a = _mm512_loadu_si512(mem + LEN_FIELD_OFF + sizeof(ObDatum) * DATUMS_PRE_VEC * 2);
    b = _mm512_loadu_si512(mem + LEN_FIELD_OFF + sizeof(ObDatum) * DATUMS_PRE_VEC * 3);
    c = _mm512_permutex2var_epi32(a, idx, b);
    uint32_t r2 = _mm512_cmp_epu32_mask(c, cmp_val, cmp_mask);
    uint32_t r = (r2 << (DATUMS_PRE_VEC * 2)) | r1;
    *(uint16_t *)&vec.reinterpret_data<uint8_t>()[pos / CHAR_BIT] |= r;
    vec.reinterpret_data<uint8_t>()[pos / CHAR_BIT + 2] |= r >> 16;
  }

  // FIXME bin.lb: SIMD can still be used here
  for (; pos < size; pos++) {
    if (datums[pos].is_null()) {
      vec.set(pos);
    }
  }
}

// Mark zero values:
//   set %vec to 1 if is zero
//   keep %vec bit unchanged for non-zero.
inline void mark_zero_values(const uint64_t *values, const int64_t size, ObBitVector &vec)
{
  __m512i cmp_val = _mm512_set1_epi64(0);
  int64_t cnt = size / 8;
  for (int64_t i = 0; i < cnt; i++) {
    __m512i v = _mm512_loadu_si512(&values[i * 8]);
    const int8_t cmp_mask = 0; // _MM_CMPINT_EQ
    vec.reinterpret_data<uint8_t>()[i] |= _mm512_cmp_epi64_mask(v, cmp_val, cmp_mask);
  }
  for (int64_t i = size / 8 * 8; i < size; i++) {
    if (values[i] == 0) {
      vec.set(i);
    }
  }
}
#endif

void mark_filtered_datums_simd(const ObDatum *datums,
                                const uint64_t *values,
                                const int64_t size,
                                ObBitVector &bit_vec)
{
#if defined(__x86_64__)
  bit_vec.reset(size);
  mark_null_datums(datums, size, bit_vec);
  mark_zero_values(values, size, bit_vec);
#else
  UNUSEDx(datums, values, size, bit_vec);
  abort();
#endif
}

} // end namespace sql
} // end namespace oceanbase
