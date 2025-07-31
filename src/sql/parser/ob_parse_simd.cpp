/** * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/ob_define.h"
#include "ob_fast_parser.h"
#if defined(__GNUC__) && defined(__x86_64__)
#include "share/vector_type/ob_vector_op_common.h"
#endif

namespace oceanbase
{
namespace common
{

#if defined(__GNUC__) && defined(__x86_64__)
OB_INLINE static void get_first_non_hex_char_avx512(oceanbase::sql::ObRawSql& raw_sql)
{
  __m512i zero = _mm512_set1_epi8('0');
  __m512i nine = _mm512_set1_epi8('9');
  __m512i a = _mm512_set1_epi8('a');
  __m512i f = _mm512_set1_epi8('f');
  __m512i A = _mm512_set1_epi8('A');
  __m512i F = _mm512_set1_epi8('F');

  int64_t hex_pos = raw_sql.cur_pos_;
  while (hex_pos + 64 <= raw_sql.raw_sql_len_) {
    __m512i chars = _mm512_loadu_si512((__m512i *)(raw_sql.raw_sql_ + hex_pos));
    __mmask64 mask = (_mm512_cmpge_epu8_mask(chars, zero) & _mm512_cmple_epu8_mask(chars, nine)) |
                    (_mm512_cmpge_epu8_mask(chars, a) & _mm512_cmple_epu8_mask(chars, f)) |
                    (_mm512_cmpge_epu8_mask(chars, A) & _mm512_cmple_epu8_mask(chars, F));

    if (mask != 0xFFFFFFFFFFFFFFFF) {
      raw_sql.cur_pos_ = hex_pos + _tzcnt_u64(~mask);
      return;
    }

    hex_pos += 64;
  }

  for (; hex_pos < raw_sql.raw_sql_len_; ++hex_pos) {
    char ch = raw_sql.raw_sql_[hex_pos];
    if (!(ch != sql::INVALID_CHAR && sql::HEX_FLAGS[static_cast<uint8_t>(ch)])) {
      break;
    }
  }

  raw_sql.cur_pos_ = hex_pos;
  if (hex_pos >= raw_sql.raw_sql_len_) {
    raw_sql.search_end_ = true;
    raw_sql.cur_pos_ = raw_sql.raw_sql_len_;
  }
}


OB_INLINE static void get_first_non_hex_char_avx2(oceanbase::sql::ObRawSql& raw_sql)
{
  __m256i zero = _mm256_set1_epi8('0' - 1);
  __m256i nine = _mm256_set1_epi8('9' + 1);
  __m256i a = _mm256_set1_epi8('a' - 1);
  __m256i f = _mm256_set1_epi8('f' + 1);
  __m256i A = _mm256_set1_epi8('A' - 1);
  __m256i F = _mm256_set1_epi8('F' + 1);

  int64_t hex_pos = raw_sql.cur_pos_;
  while (hex_pos + 32 <= raw_sql.raw_sql_len_) {
    __m256i chars = _mm256_loadu_si256((__m256i *)(raw_sql.raw_sql_ + hex_pos));
    __m256i num_mask1 = _mm256_and_si256(_mm256_cmpgt_epi8(chars, zero), _mm256_cmpgt_epi8(nine, chars));
    __m256i num_mask2 = _mm256_and_si256(_mm256_cmpgt_epi8(chars, a), _mm256_cmpgt_epi8(f, chars));
    __m256i num_mask3 = _mm256_and_si256(_mm256_cmpgt_epi8(chars, A), _mm256_cmpgt_epi8(F, chars));
    int32_t mask = _mm256_movemask_epi8(_mm256_or_si256(_mm256_or_si256(num_mask1, num_mask2), num_mask3));
    if (mask != 0xFFFFFFFFFFFFFFFF) {
      raw_sql.cur_pos_ = hex_pos + _tzcnt_u32(~mask);
      return;
    }

    hex_pos += 32;
  }

  for (; hex_pos < raw_sql.raw_sql_len_; ++hex_pos) {
    char ch = raw_sql.raw_sql_[hex_pos];
    if (!(ch != sql::INVALID_CHAR && sql::HEX_FLAGS[static_cast<uint8_t>(ch)])) {
      break;
    }
  }

  raw_sql.cur_pos_ = hex_pos;
  if (hex_pos >= raw_sql.raw_sql_len_) {
    raw_sql.search_end_ = true;
    raw_sql.cur_pos_ = raw_sql.raw_sql_len_;
  }
}

void process_hex_chars_simd(oceanbase::sql::ObRawSql& raw_sql, bool& is_arch_supported)
{
  if (__builtin_cpu_supports("avx512f") || __builtin_cpu_supports("avx512bw") || __builtin_cpu_supports("avx512vl")) {
    get_first_non_hex_char_avx512(raw_sql);
  // TODO by qiyu.zd: avx2 path will be added after avx2 compilation flags are set correct
  // } else if (__builtin_cpu_supports("avx2") || __builtin_cpu_supports("avx")) {
  //   get_first_non_hex_char_avx2(raw_sql);
  } else {
    is_arch_supported = false;
  }
}

static const __m256i _9 = _mm256_set1_epi16(9);
static const __m256i _15 = _mm256_set1_epi16(0xf);

inline static __m256i char_nibble(const __m256i value) {
  __m256i and15 = _mm256_and_si256(value, _15);
  __m256i sr6 = _mm256_srai_epi16(value, 6);
  __m256i mul = _mm256_maddubs_epi16(sr6, _9);
  __m256i add = _mm256_add_epi16(mul, and15);
  return add;
}

// called in parse_node.c
extern "C" void ob_parse_binary_simd(const char **src, const char *end, char **dest)
{
  if (__builtin_cpu_supports("avx512f") || __builtin_cpu_supports("avx512bw") || __builtin_cpu_supports("avx512vl")) {
    __m512i zero = _mm512_set1_epi8('0');
    __m512i nine = _mm512_set1_epi8('9');
    __m512i extra = _mm512_set1_epi8(9);
    __m512i low_mask = _mm512_set1_epi16(0x00FF);
    __m512i high_mask = _mm512_set1_epi16(0xFF00);
    __m512i low_nibble_mask = _mm512_set1_epi8(0x0F);

    for (; *src + 64 <= end; *src += 64, *dest += 32) {
      __m512i input = _mm512_loadu_si512((__m512i*)*src);
      __m512i rawbytes = _mm512_add_epi64(_mm512_and_si512(input, low_nibble_mask),
                                          _mm512_maskz_loadu_epi8(~(_mm512_cmpge_epu8_mask(input, zero) & _mm512_cmple_epu8_mask(input, nine)), &extra));
      __m512i low_bytes = _mm512_slli_epi16(_mm512_and_si512(rawbytes, low_mask), 4);
      __m512i high_bytes = _mm512_srli_epi16(_mm512_and_si512(rawbytes, high_mask), 8);
      __m256i tmp = _mm512_cvtepi16_epi8(_mm512_or_si512(low_bytes, high_bytes));
      _mm256_storeu_si256((__m256i*)*dest, tmp);
    }
  }
  // TODO by qiyu.zd: avx2 path will be added after avx2 compilation flags are set correct
  // else if (__builtin_cpu_supports("avx2") || __builtin_cpu_supports("avx")) {
  //   const char *src_ = *src;
  //   char *dest_ = *dest;
  //   int len = (end - src_) / 2;
  //   static const __m256i A_MASK = _mm256_setr_epi8(
  //     0, -1, 2, -1, 4, -1, 6, -1, 8, -1, 10, -1, 12, -1, 14, -1,
  //     0, -1, 2, -1, 4, -1, 6, -1, 8, -1, 10, -1, 12, -1, 14, -1);
  //   static const __m256i B_MASK = _mm256_setr_epi8(
  //     1, -1, 3, -1, 5, -1, 7, -1, 9, -1, 11, -1, 13, -1, 15, -1,
  //     1, -1, 3, -1, 5, -1, 7, -1, 9, -1, 11, -1, 13, -1, 15, -1);
  //   const __m256i* src_256 = reinterpret_cast<const __m256i*>(src_);
  //   __m256i* dest_256 = reinterpret_cast<__m256i*>(dest_);
  //   while (len >= 32) {
  //     __m256i av1 = _mm256_lddqu_si256(src_256++); // 32 nibbles, 16 bytes
  //     __m256i av2 = _mm256_lddqu_si256(src_256++);

  //     // Separate high and low nibbles and extend into 16-bit elements
  //     __m256i a1 = _mm256_shuffle_epi8(av1, A_MASK);
  //     __m256i b1 = _mm256_shuffle_epi8(av1, B_MASK);
  //     __m256i a2 = _mm256_shuffle_epi8(av2, A_MASK);
  //     __m256i b2 = _mm256_shuffle_epi8(av2, B_MASK);

  //     // Convert ASCII values to nibbles
  //     a1 = char_nibble(a1);
  //     a2 = char_nibble(a2);
  //     b1 = char_nibble(b1);
  //     b2 = char_nibble(b2);

  //     // Nibbles to bytes
  //     __m256i a4_1 = _mm256_slli_epi16(a1, 4);
  //     __m256i a4_2 = _mm256_slli_epi16(a2, 4);
  //     __m256i a4orb_1 = _mm256_or_si256(a4_1, b1);
  //     __m256i a4orb_2 = _mm256_or_si256(a4_2, b2);
  //     __m256i pck1 = _mm256_packus_epi16(a4orb_1, a4orb_2); // lo1 lo2 hi1 hi2
  //     const int _0213 = 0b11011000;
  //     __m256i bytes = _mm256_permute4x64_epi64(pck1, _0213);

  //     _mm256_storeu_si256(dest_256++, bytes);
  //     len -= 32;
  //   }
  //   *src = reinterpret_cast<const char*>(src_256);
  //   *dest = reinterpret_cast<char *>(dest_256);
  // }
}


#endif
} // namespace common
} // namespace oceanbase
