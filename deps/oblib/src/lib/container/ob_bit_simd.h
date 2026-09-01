/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_BIT_SIMD_H_
#define OCEANBASE_COMMON_OB_BIT_SIMD_H_

#include <cstring>
#include "common/ob_target_specific.h"

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#elif OB_ARM_USE_MULTITARGET_CODE
#include <arm_neon.h>
#endif /* OB_USE_MULTITARGET_CODE */

namespace oceanbase
{
namespace common
{

OB_DECLARE_SSE42_SPECIFIC_CODE(

  inline int64_t popcnt_bytes(const uint8_t *__restrict__ data, const int64_t size) {
    int64_t count = 0;
    const uint8_t *end = data + size;
    const uint8_t *end64 = data + (size & ~static_cast<int64_t>(63));
    const __m128i zeros = _mm_setzero_si128();
    __m128i acc = _mm_setzero_si128();
    for (; data < end64; data += 64) {
      __m128i v0 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data));
      __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + 16));
      __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + 32));
      __m128i v3 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + 48));
      __m128i sum = _mm_add_epi8(_mm_add_epi8(v0, v1), _mm_add_epi8(v2, v3));
      acc = _mm_add_epi64(acc, _mm_sad_epu8(sum, zeros));
    }
    __m128i hi = _mm_unpackhi_epi64(acc, acc);
    count += _mm_cvtsi128_si64(_mm_add_epi64(acc, hi));
    for (; data < end; ++data) {
      count += *data;
    }
    return count;
  }

  // Counts set bits in [0, aligned_bits). `aligned_bits` must be a multiple of
  // 512 (SSE42 SIMD batch size). Caller handles the sub-512 remainder.
  inline int64_t popcnt_bits_aligned_512(const uint8_t *__restrict__ data, const int64_t aligned_bits) {
    const __m128i lookup = _mm_setr_epi8(
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
    const __m128i low_mask = _mm_set1_epi8(0x0f);
    const __m128i zeros = _mm_setzero_si128();
    __m128i acc = _mm_setzero_si128();
    for (int64_t i = 0; i < aligned_bits; i += 512) {
      __m128i v0 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + (i >> 3)));
      __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + (i >> 3) + 16));
      __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + (i >> 3) + 32));
      __m128i v3 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + (i >> 3) + 48));
      __m128i p0 = _mm_add_epi8(_mm_shuffle_epi8(lookup, _mm_and_si128(v0, low_mask)),
                                _mm_shuffle_epi8(lookup, _mm_and_si128(_mm_srli_epi16(v0, 4), low_mask)));
      __m128i p1 = _mm_add_epi8(_mm_shuffle_epi8(lookup, _mm_and_si128(v1, low_mask)),
                                _mm_shuffle_epi8(lookup, _mm_and_si128(_mm_srli_epi16(v1, 4), low_mask)));
      __m128i p2 = _mm_add_epi8(_mm_shuffle_epi8(lookup, _mm_and_si128(v2, low_mask)),
                                _mm_shuffle_epi8(lookup, _mm_and_si128(_mm_srli_epi16(v2, 4), low_mask)));
      __m128i p3 = _mm_add_epi8(_mm_shuffle_epi8(lookup, _mm_and_si128(v3, low_mask)),
                                _mm_shuffle_epi8(lookup, _mm_and_si128(_mm_srli_epi16(v3, 4), low_mask)));
      __m128i sum = _mm_add_epi8(_mm_add_epi8(p0, p1), _mm_add_epi8(p2, p3));
      acc = _mm_add_epi64(acc, _mm_sad_epu8(sum, zeros));
    }
    __m128i hi = _mm_unpackhi_epi64(acc, acc);
    return _mm_cvtsi128_si64(_mm_add_epi64(acc, hi));
  }

  inline int64_t popcnt_bits(const uint64_t *__restrict__ data, const int64_t size) {
    const int64_t end512 = size & ~static_cast<int64_t>(511);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t count = popcnt_bits_aligned_512(reinterpret_cast<const uint8_t *>(data), end512);
    for (int64_t i = end512; i < end64; i += 64) {
      count += _mm_popcnt_u64(data[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      uint64_t tail = data[end64 >> 6] & ((static_cast<uint64_t>(1) << rem_bits) - 1);
      count += _mm_popcnt_u64(tail);
    }
    return count;
  }

  inline int64_t popcnt_bits(const uint8_t *__restrict__ data, const int64_t size) {
    const int64_t end512 = size & ~static_cast<int64_t>(511);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const uint64_t *__restrict__ data64 = reinterpret_cast<const uint64_t *>(data);
    int64_t count = popcnt_bits_aligned_512(data, end512);
    for (int64_t i = end512; i < end64; i += 64) {
      count += _mm_popcnt_u64(data64[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      const int64_t rem_bytes = (rem_bits + 7) >> 3;
      uint64_t tail = 0;
      std::memcpy(&tail, data + (end64 >> 3), rem_bytes);
      tail &= (static_cast<uint64_t>(1) << rem_bits) - 1;
      count += _mm_popcnt_u64(tail);
    }
    return count;
  }

  template <bool IS_FLIP>
  inline void unpack_bits_to_bytes(const uint8_t *__restrict__ bits,
                                   uint8_t *__restrict__ bytes,
                                   const int64_t size) {
    const int64_t end16 = size & ~static_cast<int64_t>(15);
    const uint16_t *__restrict__ bits16 = reinterpret_cast<const uint16_t *>(bits);
    const __m128i bit_mask = _mm_setr_epi8(
        1, 2, 4, 8, 16, 32, 64, static_cast<char>(128),
        1, 2, 4, 8, 16, 32, 64, static_cast<char>(128));
    const __m128i shuffle_idx = _mm_setr_epi8(
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 1, 1, 1, 1);
    const __m128i one = _mm_set1_epi8(1);
    int64_t i = 0;
    for (; i < end16; i += 16) {
      uint16_t raw = IS_FLIP ? static_cast<uint16_t>(~bits16[i >> 4]) : bits16[i >> 4];
      __m128i v = _mm_set1_epi16(raw);
      __m128i bcast = _mm_shuffle_epi8(v, shuffle_idx);
      __m128i sel = _mm_and_si128(bcast, bit_mask);
      __m128i out = _mm_min_epu8(sel, one);
      _mm_storeu_si128(reinterpret_cast<__m128i *>(bytes + i), out);
    }
    for (; i < size; ++i) {
      uint8_t bit = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
      bytes[i] = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
    }
  }

  template <bool IS_FLIP>
  inline void pack_bytes_to_bits(const uint8_t *__restrict__ bytes,
                                 uint8_t *__restrict__ bits,
                                 const int64_t size) {
    const int64_t end16 = size & ~static_cast<int64_t>(15);
    uint16_t *__restrict__ bits16 = reinterpret_cast<uint16_t *>(bits);
    int64_t i = 0;
    for (; i < end16; i += 16) {
      __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes + i));
      __m128i hi = _mm_slli_epi16(v, 7);
      uint16_t m = static_cast<uint16_t>(_mm_movemask_epi8(hi));
      bits16[i >> 4] = IS_FLIP ? static_cast<uint16_t>(~m) : m;
    }
    for (; i + 8 <= size; i += 8) {
      uint8_t b = 0;
      for (int64_t j = 0; j < 8; ++j) {
        b = static_cast<uint8_t>(b | ((bytes[i + j] & 1) << j));
      }
      bits[i >> 3] = IS_FLIP ? static_cast<uint8_t>(~b) : b;
    }
    if (i < size) {
      const int64_t rem = size - i;
      uint8_t b = 0;
      for (int64_t j = 0; j < rem; ++j) {
        uint8_t bit = static_cast<uint8_t>(bytes[i + j] & 1);
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      const uint8_t mask = static_cast<uint8_t>((1u << rem) - 1);
      bits[i >> 3] = static_cast<uint8_t>((bits[i >> 3] & ~mask) | b);
    }
  }

  template <bool IS_FLIP>
  inline void bits_bit_or_bytes64(uint8_t *__restrict__ bits,
                                  const uint64_t *__restrict__ bytes,
                                  const int64_t size) {
    const __m128i zero = _mm_setzero_si128();
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    uint64_t *__restrict__ bits64 = reinterpret_cast<uint64_t *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      const uint64_t *__restrict__ p = bytes + i;
      uint64_t m = 0;
      for (int byte = 0; byte < 8; ++byte) {
        const uint64_t *__restrict__ q = p + 8 * byte;
        __m128i eq0 = _mm_cmpeq_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(q + 0)), zero);
        __m128i eq1 = _mm_cmpeq_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(q + 2)), zero);
        __m128i eq2 = _mm_cmpeq_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(q + 4)), zero);
        __m128i eq3 = _mm_cmpeq_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(q + 6)), zero);
        uint8_t bm = static_cast<uint8_t>(
            ((_mm_movemask_pd(_mm_castsi128_pd(eq0)) & 3))
          | ((_mm_movemask_pd(_mm_castsi128_pd(eq1)) & 3) << 2)
          | ((_mm_movemask_pd(_mm_castsi128_pd(eq2)) & 3) << 4)
          | ((_mm_movemask_pd(_mm_castsi128_pd(eq3)) & 3) << 6));
        m |= static_cast<uint64_t>(bm) << (8 * byte);
      }
      bits64[i >> 6] |= IS_FLIP ? m : ~m;
    }
    const int64_t end8 = size & ~static_cast<int64_t>(7);
    for (; i < end8; i += 8) {
      __m128i v0 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes + i));
      __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes + i + 2));
      __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes + i + 4));
      __m128i v3 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes + i + 6));
      __m128i eq0 = _mm_cmpeq_epi64(v0, zero);
      __m128i eq1 = _mm_cmpeq_epi64(v1, zero);
      __m128i eq2 = _mm_cmpeq_epi64(v2, zero);
      __m128i eq3 = _mm_cmpeq_epi64(v3, zero);
      uint8_t m = static_cast<uint8_t>(
          ((_mm_movemask_pd(_mm_castsi128_pd(eq0)) & 3))
        | ((_mm_movemask_pd(_mm_castsi128_pd(eq1)) & 3) << 2)
        | ((_mm_movemask_pd(_mm_castsi128_pd(eq2)) & 3) << 4)
        | ((_mm_movemask_pd(_mm_castsi128_pd(eq3)) & 3) << 6));
      m = IS_FLIP ? m : static_cast<uint8_t>(~m);
      bits[i >> 3] |= m;
    }
    if (i < size) {
      uint8_t b = 0;
      for (int64_t j = 0; j < size - i; ++j) {
        uint8_t bit = (bytes[i + j] != 0) ? 1 : 0;
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      bits[i >> 3] |= b;
    }
  }

)

OB_DECLARE_AVX2_SPECIFIC_CODE(

  inline int64_t popcnt_bytes(const uint8_t *__restrict__ data, const int64_t size) {
    int64_t count = 0;
    const uint8_t *end = data + size;
    const uint8_t *end128 = data + (size & ~static_cast<int64_t>(127));
    const __m256i zeros = _mm256_setzero_si256();
    __m256i acc = _mm256_setzero_si256();
    for (; data < end128; data += 128) {
      __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data));
      __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + 32));
      __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + 64));
      __m256i v3 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + 96));
      __m256i sum = _mm256_add_epi8(_mm256_add_epi8(v0, v1), _mm256_add_epi8(v2, v3));
      acc = _mm256_add_epi64(acc, _mm256_sad_epu8(sum, zeros));
    }
    __m128i s = _mm_add_epi64(_mm256_castsi256_si128(acc), _mm256_extracti128_si256(acc, 1));
    __m128i hi = _mm_unpackhi_epi64(s, s);
    count += _mm_cvtsi128_si64(_mm_add_epi64(s, hi));
    for (; data < end; ++data) {
      count += *data;
    }
    return count;
  }

  // Counts set bits in [0, aligned_bits). `aligned_bits` must be a multiple of
  // 1024 (AVX2 SIMD batch size). Caller handles the sub-1024 remainder.
  inline int64_t popcnt_bits_aligned_1024(const uint8_t *__restrict__ data, const int64_t aligned_bits) {
    const __m256i lookup = _mm256_setr_epi8(
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
    const __m256i low_mask = _mm256_set1_epi8(0x0f);
    const __m256i zeros = _mm256_setzero_si256();
    __m256i acc = _mm256_setzero_si256();
    for (int64_t i = 0; i < aligned_bits; i += 1024) {
      __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + (i >> 3)));
      __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + (i >> 3) + 32));
      __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + (i >> 3) + 64));
      __m256i v3 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + (i >> 3) + 96));
      __m256i p0 = _mm256_add_epi8(_mm256_shuffle_epi8(lookup, _mm256_and_si256(v0, low_mask)),
                                   _mm256_shuffle_epi8(lookup, _mm256_and_si256(_mm256_srli_epi16(v0, 4), low_mask)));
      __m256i p1 = _mm256_add_epi8(_mm256_shuffle_epi8(lookup, _mm256_and_si256(v1, low_mask)),
                                   _mm256_shuffle_epi8(lookup, _mm256_and_si256(_mm256_srli_epi16(v1, 4), low_mask)));
      __m256i p2 = _mm256_add_epi8(_mm256_shuffle_epi8(lookup, _mm256_and_si256(v2, low_mask)),
                                   _mm256_shuffle_epi8(lookup, _mm256_and_si256(_mm256_srli_epi16(v2, 4), low_mask)));
      __m256i p3 = _mm256_add_epi8(_mm256_shuffle_epi8(lookup, _mm256_and_si256(v3, low_mask)),
                                   _mm256_shuffle_epi8(lookup, _mm256_and_si256(_mm256_srli_epi16(v3, 4), low_mask)));
      __m256i sum = _mm256_add_epi8(_mm256_add_epi8(p0, p1), _mm256_add_epi8(p2, p3));
      acc = _mm256_add_epi64(acc, _mm256_sad_epu8(sum, zeros));
    }
    __m128i s = _mm_add_epi64(_mm256_castsi256_si128(acc), _mm256_extracti128_si256(acc, 1));
    __m128i hi = _mm_unpackhi_epi64(s, s);
    return _mm_cvtsi128_si64(_mm_add_epi64(s, hi));
  }

  inline int64_t popcnt_bits(const uint64_t *__restrict__ data, const int64_t size) {
    const int64_t end1024 = size & ~static_cast<int64_t>(1023);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t count = popcnt_bits_aligned_1024(reinterpret_cast<const uint8_t *>(data), end1024);
    for (int64_t i = end1024; i < end64; i += 64) {
      count += _mm_popcnt_u64(data[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      uint64_t tail = data[end64 >> 6] & ((static_cast<uint64_t>(1) << rem_bits) - 1);
      count += _mm_popcnt_u64(tail);
    }
    return count;
  }

  inline int64_t popcnt_bits(const uint8_t *__restrict__ data, const int64_t size) {
    const int64_t end1024 = size & ~static_cast<int64_t>(1023);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const uint64_t *__restrict__ data64 = reinterpret_cast<const uint64_t *>(data);
    int64_t count = popcnt_bits_aligned_1024(data, end1024);
    for (int64_t i = end1024; i < end64; i += 64) {
      count += _mm_popcnt_u64(data64[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      const int64_t rem_bytes = (rem_bits + 7) >> 3;
      uint64_t tail = 0;
      std::memcpy(&tail, data + (end64 >> 3), rem_bytes);
      tail &= (static_cast<uint64_t>(1) << rem_bits) - 1;
      count += _mm_popcnt_u64(tail);
    }
    return count;
  }

  template <bool IS_FLIP>
  inline void unpack_bits_to_bytes(const uint8_t *__restrict__ bits,
                                   uint8_t *__restrict__ bytes,
                                   const int64_t size) {
    const int64_t end32 = size & ~static_cast<int64_t>(31);
    const uint32_t *__restrict__ bits32 = reinterpret_cast<const uint32_t *>(bits);
    const __m256i bit_mask = _mm256_setr_epi8(
        1, 2, 4, 8, 16, 32, 64, static_cast<char>(128),
        1, 2, 4, 8, 16, 32, 64, static_cast<char>(128),
        1, 2, 4, 8, 16, 32, 64, static_cast<char>(128),
        1, 2, 4, 8, 16, 32, 64, static_cast<char>(128));
    const __m256i shuffle_idx = _mm256_setr_epi8(
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 1, 1, 1, 1,
        2, 2, 2, 2, 2, 2, 2, 2,
        3, 3, 3, 3, 3, 3, 3, 3);
    const __m256i one = _mm256_set1_epi8(1);
    int64_t i = 0;
    for (; i < end32; i += 32) {
      uint32_t raw = IS_FLIP ? ~bits32[i >> 5] : bits32[i >> 5];
      __m256i v = _mm256_set1_epi32(static_cast<int>(raw));
      __m256i bcast = _mm256_shuffle_epi8(v, shuffle_idx);
      __m256i sel = _mm256_and_si256(bcast, bit_mask);
      __m256i out = _mm256_min_epu8(sel, one);
      _mm256_storeu_si256(reinterpret_cast<__m256i *>(bytes + i), out);
    }
    for (; i < size; ++i) {
      uint8_t bit = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
      bytes[i] = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
    }
  }

  template <bool IS_FLIP>
  inline void pack_bytes_to_bits(const uint8_t *__restrict__ bytes,
                                 uint8_t *__restrict__ bits,
                                 const int64_t size) {
    const int64_t end32 = size & ~static_cast<int64_t>(31);
    uint32_t *__restrict__ bits32 = reinterpret_cast<uint32_t *>(bits);
    int64_t i = 0;
    for (; i < end32; i += 32) {
      __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes + i));
      __m256i hi = _mm256_slli_epi16(v, 7);
      uint32_t m = static_cast<uint32_t>(_mm256_movemask_epi8(hi));
      bits32[i >> 5] = IS_FLIP ? ~m : m;
    }
    for (; i + 8 <= size; i += 8) {
      uint8_t b = 0;
      for (int64_t j = 0; j < 8; ++j) {
        b = static_cast<uint8_t>(b | ((bytes[i + j] & 1) << j));
      }
      bits[i >> 3] = IS_FLIP ? static_cast<uint8_t>(~b) : b;
    }
    if (i < size) {
      const int64_t rem = size - i;
      uint8_t b = 0;
      for (int64_t j = 0; j < rem; ++j) {
        uint8_t bit = static_cast<uint8_t>(bytes[i + j] & 1);
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      const uint8_t mask = static_cast<uint8_t>((1u << rem) - 1);
      bits[i >> 3] = static_cast<uint8_t>((bits[i >> 3] & ~mask) | b);
    }
  }

  template <bool IS_FLIP>
  inline void bits_bit_or_bytes64(uint8_t *__restrict__ bits,
                                  const uint64_t *__restrict__ bytes,
                                  const int64_t size) {
    const __m256i zero = _mm256_setzero_si256();
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    uint64_t *__restrict__ bits64 = reinterpret_cast<uint64_t *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      const uint64_t *__restrict__ p = bytes + i;
      uint64_t m = 0;
      for (int k = 0; k < 16; ++k) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p + 4 * k));
        __m256i eq = _mm256_cmpeq_epi64(v, zero);
        uint64_t mk = static_cast<uint32_t>(_mm256_movemask_pd(_mm256_castsi256_pd(eq))) & 0xF;
        m |= mk << (4 * k);
      }
      bits64[i >> 6] |= IS_FLIP ? m : ~m;
    }
    const int64_t end8 = size & ~static_cast<int64_t>(7);
    for (; i < end8; i += 8) {
      __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes + i));
      __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes + i + 4));
      __m256i eq0 = _mm256_cmpeq_epi64(v0, zero);
      __m256i eq1 = _mm256_cmpeq_epi64(v1, zero);
      uint8_t m = static_cast<uint8_t>(
          (_mm256_movemask_pd(_mm256_castsi256_pd(eq0)) & 0xF)
        | ((_mm256_movemask_pd(_mm256_castsi256_pd(eq1)) & 0xF) << 4));
      m = IS_FLIP ? m : static_cast<uint8_t>(~m);
      bits[i >> 3] |= m;
    }
    if (i < size) {
      uint8_t b = 0;
      for (int64_t j = 0; j < size - i; ++j) {
        uint8_t bit = (bytes[i + j] != 0) ? 1 : 0;
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      bits[i >> 3] |= b;
    }
  }

)

OB_DECLARE_AVX512_SPECIFIC_CODE(

  inline int64_t popcnt_bytes(const uint8_t *__restrict__ data, const int64_t size) {
    int64_t count = 0;
    const uint8_t *end = data + size;
    const uint8_t *end256 = data + (size & ~static_cast<int64_t>(255));
    const __m512i zeros = _mm512_setzero_si512();
    __m512i acc = _mm512_setzero_si512();
    for (; data < end256; data += 256) {
      __m512i v0 = _mm512_loadu_si512(reinterpret_cast<const void *>(data));
      __m512i v1 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + 64));
      __m512i v2 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + 128));
      __m512i v3 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + 192));
      __m512i sum = _mm512_add_epi8(_mm512_add_epi8(v0, v1), _mm512_add_epi8(v2, v3));
      acc = _mm512_add_epi64(acc, _mm512_sad_epu8(sum, zeros));
    }
    count += _mm512_reduce_add_epi64(acc);
    for (; data < end; ++data) {
      count += *data;
    }
    return count;
  }

  // Counts set bits in [0, aligned_bits). `aligned_bits` must be a multiple of
  // 2048 (AVX512 SIMD batch size). Caller handles the sub-2048 remainder.
  inline int64_t popcnt_bits_aligned_2048(const uint8_t *__restrict__ data, const int64_t aligned_bits) {
    const __m512i lookup = _mm512_setr4_epi32(
        0x02010100, 0x03020201, 0x03020201, 0x04030302);
    const __m512i low_mask = _mm512_set1_epi8(0x0f);
    const __m512i zeros = _mm512_setzero_si512();
    __m512i acc = _mm512_setzero_si512();
    for (int64_t i = 0; i < aligned_bits; i += 2048) {
      __m512i v0 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + (i >> 3)));
      __m512i v1 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + (i >> 3) + 64));
      __m512i v2 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + (i >> 3) + 128));
      __m512i v3 = _mm512_loadu_si512(reinterpret_cast<const void *>(data + (i >> 3) + 192));
      __m512i p0 = _mm512_add_epi8(_mm512_shuffle_epi8(lookup, _mm512_and_si512(v0, low_mask)),
                                   _mm512_shuffle_epi8(lookup, _mm512_and_si512(_mm512_srli_epi16(v0, 4), low_mask)));
      __m512i p1 = _mm512_add_epi8(_mm512_shuffle_epi8(lookup, _mm512_and_si512(v1, low_mask)),
                                   _mm512_shuffle_epi8(lookup, _mm512_and_si512(_mm512_srli_epi16(v1, 4), low_mask)));
      __m512i p2 = _mm512_add_epi8(_mm512_shuffle_epi8(lookup, _mm512_and_si512(v2, low_mask)),
                                   _mm512_shuffle_epi8(lookup, _mm512_and_si512(_mm512_srli_epi16(v2, 4), low_mask)));
      __m512i p3 = _mm512_add_epi8(_mm512_shuffle_epi8(lookup, _mm512_and_si512(v3, low_mask)),
                                   _mm512_shuffle_epi8(lookup, _mm512_and_si512(_mm512_srli_epi16(v3, 4), low_mask)));
      __m512i sum = _mm512_add_epi8(_mm512_add_epi8(p0, p1), _mm512_add_epi8(p2, p3));
      acc = _mm512_add_epi64(acc, _mm512_sad_epu8(sum, zeros));
    }
    return _mm512_reduce_add_epi64(acc);
  }

  inline int64_t popcnt_bits(const uint64_t *__restrict__ data, const int64_t size) {
    const int64_t end2048 = size & ~static_cast<int64_t>(2047);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t count = popcnt_bits_aligned_2048(reinterpret_cast<const uint8_t *>(data), end2048);
    for (int64_t i = end2048; i < end64; i += 64) {
      count += _mm_popcnt_u64(data[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      uint64_t tail = data[end64 >> 6] & ((static_cast<uint64_t>(1) << rem_bits) - 1);
      count += _mm_popcnt_u64(tail);
    }
    return count;
  }

  inline int64_t popcnt_bits(const uint8_t *__restrict__ data, const int64_t size) {
    const int64_t end2048 = size & ~static_cast<int64_t>(2047);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const uint64_t *__restrict__ data64 = reinterpret_cast<const uint64_t *>(data);
    int64_t count = popcnt_bits_aligned_2048(data, end2048);
    for (int64_t i = end2048; i < end64; i += 64) {
      count += _mm_popcnt_u64(data64[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      const int64_t rem_bytes = (rem_bits + 7) >> 3;
      uint64_t tail = 0;
      std::memcpy(&tail, data + (end64 >> 3), rem_bytes);
      tail &= (static_cast<uint64_t>(1) << rem_bits) - 1;
      count += _mm_popcnt_u64(tail);
    }
    return count;
  }

  template <bool IS_FLIP>
  inline void unpack_bits_to_bytes(const uint8_t *__restrict__ bits,
                                   uint8_t *__restrict__ bytes,
                                   const int64_t size) {
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const __mmask64 *__restrict__ bits64 = reinterpret_cast<const __mmask64 *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      __mmask64 m = IS_FLIP ? ~bits64[i >> 6] : bits64[i >> 6];
      __m512i v = _mm512_movm_epi8(m);
      __m512i out = _mm512_abs_epi8(v);
      _mm512_storeu_si512(reinterpret_cast<void *>(bytes + i), out);
    }
    for (; i < size; ++i) {
      uint8_t bit = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
      bytes[i] = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
    }
  }

  template <bool IS_FLIP>
  inline void pack_bytes_to_bits(const uint8_t *__restrict__ bytes,
                                 uint8_t *__restrict__ bits,
                                 const int64_t size) {
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    __mmask64 *__restrict__ bits64 = reinterpret_cast<__mmask64 *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      __m512i v = _mm512_loadu_si512(reinterpret_cast<const void *>(bytes + i));
      __mmask64 m = _mm512_test_epi8_mask(v, v);
      bits64[i >> 6] = IS_FLIP ? ~m : m;
    }
    for (; i + 8 <= size; i += 8) {
      uint8_t b = 0;
      for (int64_t j = 0; j < 8; ++j) {
        b = static_cast<uint8_t>(b | ((bytes[i + j] & 1) << j));
      }
      bits[i >> 3] = IS_FLIP ? static_cast<uint8_t>(~b) : b;
    }
    if (i < size) {
      const int64_t rem = size - i;
      uint8_t b = 0;
      for (int64_t j = 0; j < rem; ++j) {
        uint8_t bit = static_cast<uint8_t>(bytes[i + j] & 1);
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      const uint8_t mask = static_cast<uint8_t>((1u << rem) - 1);
      bits[i >> 3] = static_cast<uint8_t>((bits[i >> 3] & ~mask) | b);
    }
  }

  template <bool IS_FLIP>
  inline void bits_bit_or_bytes64(uint8_t *__restrict__ bits,
                                  const uint64_t *__restrict__ bytes,
                                  const int64_t size) {
    const __m512i zero = _mm512_setzero_si512();
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    uint64_t *__restrict__ bits64 = reinterpret_cast<uint64_t *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      const uint64_t *__restrict__ p = bytes + i;
      uint64_t m =
          static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p +  0), zero, 4/*NEQ*/)))
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p +  8), zero, 4/*NEQ*/))) <<  8)
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p + 16), zero, 4/*NEQ*/))) << 16)
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p + 24), zero, 4/*NEQ*/))) << 24)
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p + 32), zero, 4/*NEQ*/))) << 32)
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p + 40), zero, 4/*NEQ*/))) << 40)
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p + 48), zero, 4/*NEQ*/))) << 48)
        | (static_cast<uint64_t>(static_cast<uint8_t>(_mm512_cmp_epi64_mask(_mm512_loadu_si512(p + 56), zero, 4/*NEQ*/))) << 56);
      bits64[i >> 6] |= IS_FLIP ? ~m : m;
    }
    const int64_t end8 = size & ~static_cast<int64_t>(7);
    for (; i < end8; i += 8) {
      __m512i v = _mm512_loadu_si512(bytes + i);
      __mmask8 m = _mm512_cmp_epi64_mask(v, zero, 4/*NEQ*/);
      m = IS_FLIP ? static_cast<__mmask8>(~m) : m;
      bits[i >> 3] |= static_cast<uint8_t>(m);
    }
    if (i < size) {
      uint8_t b = 0;
      for (int64_t j = 0; j < size - i; ++j) {
        uint8_t bit = (bytes[i + j] != 0) ? 1 : 0;
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      bits[i >> 3] |= b;
    }
  }

)

OB_DECLARE_NEON_SPECIFIC_CODE(

  inline int64_t popcnt_bytes(const uint8_t *__restrict__ data, const int64_t size) {
    int64_t count = 0;
    const uint8_t *end = data + size;
    const uint8_t *end64 = data + (size & ~static_cast<int64_t>(63));
    uint32x4_t acc = vdupq_n_u32(0);
    for (; data < end64; data += 64) {
      uint8x16_t r0 = vld1q_u8(data);
      uint8x16_t r1 = vld1q_u8(data + 16);
      uint8x16_t r2 = vld1q_u8(data + 32);
      uint8x16_t r3 = vld1q_u8(data + 48);
      uint8x16_t sum = vaddq_u8(vaddq_u8(r0, r1), vaddq_u8(r2, r3));
      acc = vpadalq_u16(acc, vpaddlq_u8(sum));
    }
    count += vaddvq_u32(acc);
    for (; data < end; ++data) {
      count += *data;
    }
    return count;
  }

  // Counts set bits in [0, aligned_bits). `aligned_bits` must be a multiple of
  // 512 (NEON SIMD batch size). Caller handles the sub-512 remainder.
  inline int64_t popcnt_bits_aligned_512(const uint8_t *__restrict__ data, const int64_t aligned_bits) {
    uint32x4_t acc = vdupq_n_u32(0);
    for (int64_t i = 0; i < aligned_bits; i += 512) {
      uint8x16_t r0 = vcntq_u8(vld1q_u8(data + (i >> 3)));
      uint8x16_t r1 = vcntq_u8(vld1q_u8(data + (i >> 3) + 16));
      uint8x16_t r2 = vcntq_u8(vld1q_u8(data + (i >> 3) + 32));
      uint8x16_t r3 = vcntq_u8(vld1q_u8(data + (i >> 3) + 48));
      uint8x16_t sum = vaddq_u8(vaddq_u8(r0, r1), vaddq_u8(r2, r3));
      acc = vpadalq_u16(acc, vpaddlq_u8(sum));
    }
    return vaddvq_u32(acc);
  }

  inline int64_t popcnt_bits(const uint64_t *__restrict__ data, const int64_t size) {
    const int64_t end512 = size & ~static_cast<int64_t>(511);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t count = popcnt_bits_aligned_512(reinterpret_cast<const uint8_t *>(data), end512);
    for (int64_t i = end512; i < end64; i += 64) {
      count += __builtin_popcountll(data[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      uint64_t tail = data[end64 >> 6] & ((static_cast<uint64_t>(1) << rem_bits) - 1);
      count += __builtin_popcountll(tail);
    }
    return count;
  }

  inline int64_t popcnt_bits(const uint8_t *__restrict__ data, const int64_t size) {
    const int64_t end512 = size & ~static_cast<int64_t>(511);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const uint64_t *__restrict__ data64 = reinterpret_cast<const uint64_t *>(data);
    int64_t count = popcnt_bits_aligned_512(data, end512);
    for (int64_t i = end512; i < end64; i += 64) {
      count += __builtin_popcountll(data64[i >> 6]);
    }
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      const int64_t rem_bytes = (rem_bits + 7) >> 3;
      uint64_t tail = 0;
      std::memcpy(&tail, data + (end64 >> 3), rem_bytes);
      tail &= (static_cast<uint64_t>(1) << rem_bits) - 1;
      count += __builtin_popcountll(tail);
    }
    return count;
  }

  template <bool IS_FLIP>
  inline void unpack_bits_to_bytes(const uint8_t *__restrict__ bits,
                                   uint8_t *__restrict__ bytes,
                                   const int64_t size) {
    const int64_t end128 = size & ~static_cast<int64_t>(127);
    static const uint8_t kBitMask[16] = {
        1, 2, 4, 8, 16, 32, 64, 128,
        1, 2, 4, 8, 16, 32, 64, 128};
    static const uint8_t kIdx[8][16] = {
        {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1},
        {2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3},
        {4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5},
        {6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7},
        {8, 8, 8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9},
        {10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11},
        {12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13, 13, 13, 13},
        {14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15}};
    const uint8x16_t mask = vld1q_u8(kBitMask);
    const uint8x16_t one = vdupq_n_u8(1);
    const uint8x16_t idx0 = vld1q_u8(kIdx[0]);
    const uint8x16_t idx1 = vld1q_u8(kIdx[1]);
    const uint8x16_t idx2 = vld1q_u8(kIdx[2]);
    const uint8x16_t idx3 = vld1q_u8(kIdx[3]);
    const uint8x16_t idx4 = vld1q_u8(kIdx[4]);
    const uint8x16_t idx5 = vld1q_u8(kIdx[5]);
    const uint8x16_t idx6 = vld1q_u8(kIdx[6]);
    const uint8x16_t idx7 = vld1q_u8(kIdx[7]);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t i = 0;
    for (; i < end128; i += 128) {
      uint8x16_t tbl = IS_FLIP ? vmvnq_u8(vld1q_u8(bits + (i >> 3))) : vld1q_u8(bits + (i >> 3));
      vst1q_u8(bytes + i +   0, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx0), mask), one));
      vst1q_u8(bytes + i +  16, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx1), mask), one));
      vst1q_u8(bytes + i +  32, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx2), mask), one));
      vst1q_u8(bytes + i +  48, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx3), mask), one));
      vst1q_u8(bytes + i +  64, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx4), mask), one));
      vst1q_u8(bytes + i +  80, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx5), mask), one));
      vst1q_u8(bytes + i +  96, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx6), mask), one));
      vst1q_u8(bytes + i + 112, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx7), mask), one));
    }
    if (i < end64) {
      uint8x8_t bits8 = IS_FLIP ? vmvn_u8(vld1_u8(bits + (i >> 3))) : vld1_u8(bits + (i >> 3));
      uint8x16_t tbl = vcombine_u8(bits8, bits8);
      vst1q_u8(bytes + i +  0, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx0), mask), one));
      vst1q_u8(bytes + i + 16, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx1), mask), one));
      vst1q_u8(bytes + i + 32, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx2), mask), one));
      vst1q_u8(bytes + i + 48, vminq_u8(vandq_u8(vqtbl1q_u8(tbl, idx3), mask), one));
      i += 64;
    }
    for (; i < size; ++i) {
      uint8_t bit = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
      bytes[i] = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
    }
  }

  template <bool IS_FLIP>
  inline void pack_bytes_to_bits(const uint8_t *__restrict__ bytes,
                                 uint8_t *__restrict__ bits,
                                 const int64_t size) {
    const int64_t end16 = size & ~static_cast<int64_t>(15);
    static const int8_t kShifts[16] = {
        0, 1, 2, 3, 4, 5, 6, 7,
        0, 1, 2, 3, 4, 5, 6, 7};
    const int8x16_t shifts = vld1q_s8(kShifts);
    int64_t i = 0;
    for (; i < end16; i += 16) {
      uint8x16_t v = vld1q_u8(bytes + i);
      uint8x16_t weighted = vshlq_u8(v, shifts);
      uint8_t lo = vaddv_u8(vget_low_u8(weighted));
      uint8_t hi = vaddv_u8(vget_high_u8(weighted));
      bits[(i >> 3) + 0] = IS_FLIP ? static_cast<uint8_t>(~lo) : lo;
      bits[(i >> 3) + 1] = IS_FLIP ? static_cast<uint8_t>(~hi) : hi;
    }
    for (; i + 8 <= size; i += 8) {
      uint8_t b = 0;
      for (int64_t j = 0; j < 8; ++j) {
        b = static_cast<uint8_t>(b | ((bytes[i + j] & 1) << j));
      }
      bits[i >> 3] = IS_FLIP ? static_cast<uint8_t>(~b) : b;
    }
    if (i < size) {
      const int64_t rem = size - i;
      uint8_t b = 0;
      for (int64_t j = 0; j < rem; ++j) {
        uint8_t bit = static_cast<uint8_t>(bytes[i + j] & 1);
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      const uint8_t mask = static_cast<uint8_t>((1u << rem) - 1);
      bits[i >> 3] = static_cast<uint8_t>((bits[i >> 3] & ~mask) | b);
    }
  }

  template <bool IS_FLIP>
  inline void bits_bit_or_bytes64(uint8_t *__restrict__ bits,
                                  const uint64_t *__restrict__ bytes,
                                  const int64_t size) {
    const uint64x2_t zero = vdupq_n_u64(0);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    uint64_t *__restrict__ bits64 = reinterpret_cast<uint64_t *>(bits);
    int64_t i = 0;
    const uint64x2_t w01 = {1, 2};
    const uint64x2_t w23 = {4, 8};
    const uint64x2_t w45 = {16, 32};
    const uint64x2_t w67 = {64, 128};
    for (; i < end64; i += 64) {
      const uint64_t *__restrict__ p = bytes + i;
      uint64_t m = 0;
      for (int byte = 0; byte < 8; ++byte) {
        const uint64_t *__restrict__ q = p + 8 * byte;
        uint64x2_t a0 = vandq_u64(vceqq_u64(vld1q_u64(q + 0), zero), w01);
        uint64x2_t a1 = vandq_u64(vceqq_u64(vld1q_u64(q + 2), zero), w23);
        uint64x2_t a2 = vandq_u64(vceqq_u64(vld1q_u64(q + 4), zero), w45);
        uint64x2_t a3 = vandq_u64(vceqq_u64(vld1q_u64(q + 6), zero), w67);
        uint64x2_t s = vaddq_u64(vaddq_u64(a0, a1), vaddq_u64(a2, a3));
        uint8_t bm = static_cast<uint8_t>(vgetq_lane_u64(s, 0) + vgetq_lane_u64(s, 1));
        m |= static_cast<uint64_t>(bm) << (8 * byte);
      }
      bits64[i >> 6] |= IS_FLIP ? m : ~m;
    }
    const int64_t end8 = size & ~static_cast<int64_t>(7);
    for (; i < end8; i += 8) {
      uint64x2_t v0 = vld1q_u64(bytes + i);
      uint64x2_t v1 = vld1q_u64(bytes + i + 2);
      uint64x2_t v2 = vld1q_u64(bytes + i + 4);
      uint64x2_t v3 = vld1q_u64(bytes + i + 6);
      uint64x2_t eq0 = vceqq_u64(v0, zero);
      uint64x2_t eq1 = vceqq_u64(v1, zero);
      uint64x2_t eq2 = vceqq_u64(v2, zero);
      uint64x2_t eq3 = vceqq_u64(v3, zero);
      uint8_t m = static_cast<uint8_t>(
          ((vgetq_lane_u64(eq0, 0) & 1))
        | ((vgetq_lane_u64(eq0, 1) & 1) << 1)
        | ((vgetq_lane_u64(eq1, 0) & 1) << 2)
        | ((vgetq_lane_u64(eq1, 1) & 1) << 3)
        | ((vgetq_lane_u64(eq2, 0) & 1) << 4)
        | ((vgetq_lane_u64(eq2, 1) & 1) << 5)
        | ((vgetq_lane_u64(eq3, 0) & 1) << 6)
        | ((vgetq_lane_u64(eq3, 1) & 1) << 7));
      m = IS_FLIP ? m : static_cast<uint8_t>(~m);
      bits[i >> 3] |= m;
    }
    if (i < size) {
      uint8_t b = 0;
      for (int64_t j = 0; j < size - i; ++j) {
        uint8_t bit = (bytes[i + j] != 0) ? 1 : 0;
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      bits[i >> 3] |= b;
    }
  }

)

OB_DECLARE_DEFAULT_CODE(

  inline int64_t popcnt_bytes(const uint8_t *__restrict__ data, const int64_t size) {
    int64_t count = 0;
    for (int64_t i = 0; i < size; ++i) {
      count += data[i];
    }
    return count;
  }

  // Counts set bits in [0, aligned_bits). `aligned_bits` must be a multiple of
  // 64 (default batch size). Caller handles the sub-64 remainder.
  inline int64_t popcnt_bits_aligned_64(const uint8_t *__restrict__ data, const int64_t aligned_bits) {
    int64_t count = 0;
    const uint64_t *__restrict__ data64 = reinterpret_cast<const uint64_t *>(data);
    for (int64_t i = 0; i < aligned_bits; i += 64) {
      count += __builtin_popcountll(data64[i >> 6]);
    }
    return count;
  }

  inline int64_t popcnt_bits(const uint64_t *__restrict__ data, const int64_t size) {
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t count = popcnt_bits_aligned_64(reinterpret_cast<const uint8_t *>(data), end64);
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      uint64_t tail = data[end64 >> 6] & ((static_cast<uint64_t>(1) << rem_bits) - 1);
      count += __builtin_popcountll(tail);
    }
    return count;
  }

  inline int64_t popcnt_bits(const uint8_t *__restrict__ data, const int64_t size) {
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t count = popcnt_bits_aligned_64(data, end64);
    if (end64 < size) {
      const int64_t rem_bits = size - end64;
      const int64_t rem_bytes = (rem_bits + 7) >> 3;
      uint64_t tail = 0;
      std::memcpy(&tail, data + (end64 >> 3), rem_bytes);
      tail &= (static_cast<uint64_t>(1) << rem_bits) - 1;
      count += __builtin_popcountll(tail);
    }
    return count;
  }

  template <bool IS_FLIP>
  inline void unpack_bits_to_bytes(const uint8_t *__restrict__ bits,
                                   uint8_t *__restrict__ bytes,
                                   const int64_t size) {
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const uint64_t *__restrict__ bits64 = reinterpret_cast<const uint64_t *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      uint64_t b = IS_FLIP ? ~bits64[i >> 6] : bits64[i >> 6];
      for (int64_t j = 0; j < 64; ++j) {
        bytes[i + j] = static_cast<uint8_t>((b >> j) & 1);
      }
    }
    for (; i < size; ++i) {
      uint8_t bit = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
      bytes[i] = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
    }
  }

  template <bool IS_FLIP>
  inline void pack_bytes_to_bits(const uint8_t *__restrict__ bytes,
                                 uint8_t *__restrict__ bits,
                                 const int64_t size) {
    constexpr uint64_t kMagic = 0x0102040810204080ULL;
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    const uint64_t *__restrict__ bytes64 = reinterpret_cast<const uint64_t *>(bytes);
    uint64_t *__restrict__ bits64 = reinterpret_cast<uint64_t *>(bits);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      uint64_t out = 0;
      for (int64_t k = 0; k < 8; ++k) {
        out |= ((bytes64[(i >> 3) + k] * kMagic) >> 56) << (8 * k);
      }
      bits64[i >> 6] = IS_FLIP ? ~out : out;
    }
    for (; i + 8 <= size; i += 8) {
      uint8_t b = static_cast<uint8_t>((bytes64[i >> 3] * kMagic) >> 56);
      bits[i >> 3] = IS_FLIP ? static_cast<uint8_t>(~b) : b;
    }
    if (i < size) {
      const int64_t rem = size - i;
      uint8_t b = 0;
      for (int64_t j = 0; j < rem; ++j) {
        uint8_t bit = static_cast<uint8_t>(bytes[i + j] & 1);
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      const uint8_t mask = static_cast<uint8_t>((1u << rem) - 1);
      bits[i >> 3] = static_cast<uint8_t>((bits[i >> 3] & ~mask) | b);
    }
  }

  template <bool IS_FLIP>
  inline void bits_bit_or_bytes64(uint8_t *__restrict__ bits,
                                  const uint64_t *__restrict__ bytes,
                                  const int64_t size) {
    uint64_t *__restrict__ bits64 = reinterpret_cast<uint64_t *>(bits);
    const int64_t end64 = size & ~static_cast<int64_t>(63);
    int64_t i = 0;
    for (; i < end64; i += 64) {
      uint64_t b = 0;
      for (int64_t j = 0; j < 64; ++j) {
        b |= static_cast<uint64_t>(bytes[i + j] != 0) << j;
      }
      bits64[i >> 6] |= IS_FLIP ? ~b : b;
    }
    for (; i + 8 <= size; i += 8) {
      uint8_t b = 0;
      for (int64_t j = 0; j < 8; ++j) {
        b = static_cast<uint8_t>(b | (static_cast<uint8_t>(bytes[i + j] != 0) << j));
      }
      bits[i >> 3] |= IS_FLIP ? static_cast<uint8_t>(~b) : b;
    }
    if (i < size) {
      uint8_t b = 0;
      for (int64_t j = 0; j < size - i; ++j) {
        uint8_t bit = (bytes[i + j] != 0) ? 1 : 0;
        bit = IS_FLIP ? static_cast<uint8_t>(bit ^ 1) : bit;
        b = static_cast<uint8_t>(b | (bit << j));
      }
      bits[i >> 3] |= b;
    }
  }

)

// Count bytes with value 1 within [data, data+size).
// All input bytes shall be 0 or 1; otherwise behavior is undefined.
OB_INLINE int64_t popcnt_bytes(const uint8_t *data, const int64_t size)
{
  return OB_DISPATCH_MULTITARGET_CODE(popcnt_bytes, data, size);
}

// Counts set bits in the first `size` bits of `data`.
// `data` is an array of 64-bit words holding LSB-first packed bits;
// caller must provide at least ceil(size / 64) readable words.
// Returns [0, size].
OB_INLINE int64_t popcnt_bits(const uint64_t *data, const int64_t size)
{
  return OB_DISPATCH_MULTITARGET_CODE(popcnt_bits, data, size);
}

// Counts set bits in the first `size` bits of `data`.
// `data` is a byte array holding LSB-first packed bits;
// caller must provide at least ceil(size / 8) readable bytes.
// Returns [0, size].
OB_INLINE int64_t popcnt_bits(const uint8_t *data, const int64_t size)
{
  return OB_DISPATCH_MULTITARGET_CODE(popcnt_bits, data, size);
}

// Unpack bit-packed input data into byte array.
// Bit i is stored in byte (i / 8) at bit-position (i % 8) in LSB-first order.
// Writes exactly `size` bytes to `bytes`; each output byte is 0 or 1.
// If IS_FLIP is true, invert each source bit for output (1 <-> 0).
template <bool IS_FLIP>
OB_INLINE void unpack_bits_to_bytes(const uint8_t *bits, uint8_t *bytes, const int64_t size)
{
  OB_DISPATCH_MULTITARGET_CODE(unpack_bits_to_bytes<IS_FLIP>, bits, bytes, size);
}

// Unpack bit-packed input data into byte array.
// Bit i is stored in word (i / 64) at bit-position (i % 64) in LSB-first order.
// Writes exactly `size` bytes to `bytes`; each output byte is 0 or 1.
// If IS_FLIP is true, invert each source bit for output (1 <-> 0).
template <bool IS_FLIP>
OB_INLINE void unpack_bits_to_bytes(const uint64_t *bits, uint8_t *bytes, const int64_t size)
{
  unpack_bits_to_bytes<IS_FLIP>(reinterpret_cast<const uint8_t *>(bits), bytes, size);
}

// Packs `size` 0/1 bytes from `bytes` into LSB-first bits in `bits`.
// All input bytes shall be 0 or 1; otherwise behavior is undefined.
// Modifies only bits in [0, size); bits at positions >= `size` are preserved.
// When IS_FLIP is true, invert each input byte (input 1 -> bit 0, input 0 -> bit 1).
template <bool IS_FLIP>
OB_INLINE void pack_bytes_to_bits(const uint8_t *bytes, uint8_t *bits, const int64_t size)
{
  OB_DISPATCH_MULTITARGET_CODE(pack_bytes_to_bits<IS_FLIP>, bytes, bits, size);
}

// Packs `size` 0/1 bytes from `bytes` into LSB-first bits in `bits`.
// All input bytes shall be 0 or 1; otherwise behavior is undefined.
// Modifies only bits in [0, size); bits at positions >= `size` are preserved.
// When IS_FLIP is true, invert each input byte (input 1 -> bit 0, input 0 -> bit 1).
template <bool IS_FLIP>
OB_INLINE void pack_bytes_to_bits(const uint8_t *bytes, uint64_t *bits, const int64_t size)
{
  pack_bytes_to_bits<IS_FLIP>(bytes, reinterpret_cast<uint8_t *>(bits), size);
}

// Bit-OR `size` uint64_t elements from `bytes` into LSB-first bits in `bits`.
// Each non-zero element produces a 1-bit; each zero element produces a 0-bit.
// When IS_FLIP is true, invert each bit from bytes before bit-OR.
template <bool IS_FLIP>
OB_INLINE void bits_bit_or_bytes64(uint8_t *bits, const uint64_t *bytes, const int64_t size)
{
  OB_DISPATCH_MULTITARGET_CODE(bits_bit_or_bytes64<IS_FLIP>, bits, bytes, size);
}

// Bit-OR `size` uint64_t elements from `bytes` into LSB-first bits in `bits`.
// Each non-zero element produces a 1-bit; each zero element produces a 0-bit.
// When IS_FLIP is true, invert each bit from bytes before bit-OR.
template <bool IS_FLIP>
OB_INLINE void bits_bit_or_bytes64(uint64_t *bits, const uint64_t *bytes, const int64_t size)
{
  bits_bit_or_bytes64<IS_FLIP>(reinterpret_cast<uint8_t *>(bits), bytes, size);
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_BIT_SIMD_H_
