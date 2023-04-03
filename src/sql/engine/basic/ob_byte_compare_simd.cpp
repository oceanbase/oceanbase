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

namespace oceanbase
{
namespace sql
{
int fast_compare_simd(const unsigned char *s,
                      const unsigned char *t,
                      int64_t length,
                      int64_t &differ_at,
                      int64_t cache_ends)
{
#if defined(__x86_64__)
  __m128i vs = _mm_loadu_si128((__m128i *)&s[0]);
  __m128i vt = _mm_loadu_si128((__m128i *)&t[0]);
  __mmask16 x = _mm_cmpeq_epi8_mask(vs, vt);
  unsigned short val = *((unsigned short *)&x);
  if (val != 0xFFFF) {
    int val_x = (int)(val ^ 0xFFFF);
    int x_val = (val_x & -val_x);
    // LOG2(x_val);
    int y = ((unsigned)(8 * sizeof(unsigned long long) - __builtin_clzll((x_val)) - 1));
    differ_at = y + cache_ends;
    return s[y] - t[y];
  }
  return 0;
#else
  (void)s;
  (void)t;
  (void)length;
  (void)differ_at;
  (void)cache_ends;
  abort();
  return 0;
#endif
}

}  // namespace sql
}  // namespace oceanbase
