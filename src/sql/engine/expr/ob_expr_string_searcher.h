/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Original author: liuzhenlong.lzl <> (137ca4f6dbd9)
// This file extracted StringSearcher from ob_expr_like.h/ob_expr_like.cpp to create a reusable class.
// Refactored to use SIMD traits for code sharing between AVX2 and NEON implementations.

#ifndef OCEANBASE_SQL_ENGINE_EXPR_STRING_SEARCHER_
#define OCEANBASE_SQL_ENGINE_EXPR_STRING_SEARCHER_

#include <cstddef>
#include "common/ob_target_specific.h"
#include "lib/utility/ob_macro_utils.h"

#if defined(__x86_64__)
#include "immintrin.h"
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace common
{

// ============================================================================
// SIMD Traits - defines platform-specific SIMD operations
// ============================================================================

OB_DECLARE_AVX2_SPECIFIC_CODE(
// AVX2 SIMD Traits (256-bit)
struct SimdTraitsAVX2 {
  static constexpr int SIMD_SIZE = 32;  // 256 bits = 32 bytes
  using VecType = __m256i;
  using MaskType = uint32_t;

  static OB_INLINE VecType set1(uint8_t val) {
    return _mm256_set1_epi8(static_cast<char>(val));
  }

  static OB_INLINE VecType load(const char *ptr) {
    return _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  }

  static OB_INLINE VecType cmpeq(VecType a, VecType b) {
    return _mm256_cmpeq_epi8(a, b);
  }

  static OB_INLINE VecType and_op(VecType a, VecType b) {
    return _mm256_and_si256(a, b);
  }

  static OB_INLINE MaskType movemask(VecType vec) {
    return static_cast<MaskType>(_mm256_movemask_epi8(vec));
  }

  static OB_INLINE MaskType has_match(VecType cmp_result) {
    return movemask(cmp_result);
  }
  static OB_INLINE bool matched(VecType cmp_result) {
    return movemask(cmp_result) != 0;
  }

  // vceqq_u8 / cmpeq result: all lanes 0xFF iff every byte pair matched
  static OB_INLINE bool cmp_all_equal(VecType cmp) {
    return movemask(cmp) == static_cast<MaskType>(~static_cast<MaskType>(0));
  }
};
)

OB_DECLARE_NEON_SPECIFIC_CODE(
// NEON SIMD Traits (128-bit, 16 bytes per vector)
struct SimdTraitsNEON {
  static constexpr int SIMD_SIZE = 16;  // 128 bits = 16 bytes
  using VecType = uint8x16_t;
  using MaskType = uint16_t;

  static OB_INLINE VecType set1(uint8_t val) {
    return vdupq_n_u8(val);
  }

  static OB_INLINE VecType load(const char *ptr) {
    return vld1q_u8(reinterpret_cast<const uint8_t *>(ptr));
  }

  static OB_INLINE VecType cmpeq(VecType a, VecType b) {
    return vceqq_u8(a, b);
  }

  static OB_INLINE VecType and_op(VecType a, VecType b) {
    return vandq_u8(a, b);
  }

  static OB_INLINE MaskType movemask(VecType cmp) {
    // vceqq_u8 returns 0xFF (match) or 0x00 (no match)
    // Shift right by 7: each byte becomes 0x01 or 0x00
    uint8x16_t bits = vshrq_n_u8(cmp, 7);

    // Use vector multiplication to compute mask bits
    const uint8_t POWER[8] = {1, 2, 4, 8, 16, 32, 64, 128};
    uint8x8_t power = vld1_u8(POWER);
    uint8x8_t lo = vget_low_u8(bits);
    uint8x8_t hi = vget_high_u8(bits);
    uint16_t mask_lo = static_cast<uint16_t>(vaddv_u8(vmul_u8(lo, power)));
    uint16_t mask_hi = static_cast<uint16_t>(vaddv_u8(vmul_u8(hi, power)));
    return mask_lo | (mask_hi << 8);
  }

  static OB_INLINE MaskType has_match(VecType cmp_result) {
    return movemask(cmp_result);
  }
  static OB_INLINE bool matched(VecType cmp_result) {
    return vmaxvq_u8(cmp_result) != 0;
  }

  // vceq_u8 result: 0xFF per lane if equal; min is 0xFF iff all lanes match
  static OB_INLINE bool cmp_all_equal(VecType cmp) {
    return vminvq_u8(cmp) == static_cast<uint8_t>(0xFF);
  }
};
)

// ============================================================================
// ObStringSearcher - Template class with SIMD traits
// ============================================================================

template <typename SimdTraits>
class ObStringSearcherImpl {
private:
  static constexpr int SIMD_SIZE = SimdTraits::SIMD_SIZE;
  using VecType = typename SimdTraits::VecType;
  using MaskType = typename SimdTraits::MaskType;

  bool compare_middle(const char *text_offset, size_t pattern_len) const;

  // aarch64: head/tail filter then prefix cache (first 16 bytes + mask); x86: compare_middle only
  bool verify_match_at(const char *at, const char *haystack_end) const;

public:
  ObStringSearcherImpl();

  int init(const char *pattern, size_t len);

  const char *get_pattern() const { return pattern_; }
  const char *get_pattern_end() const { return pattern_end_; }
  size_t get_pattern_length() const { return pattern_len_; }

  int instr(const char *text, const char *text_end, int64_t &res, bool &find) const;

  int is_substring(const char *text, const char *text_end, bool &res) const;

  int start_with(const char *text, const char *text_end, bool &res) const;

  int end_with(const char *text, const char *text_end, bool &res) const;

  int equal(const char *text, const char *text_end, bool &res) const;

  bool memequal_opt(const char *s1, const char *s2, size_t n) const;

private:
  template <typename T>
  static OB_INLINE bool memequal_plain(const char *p1, const char *p2) {
    return *reinterpret_cast<const T *>(p1) == *reinterpret_cast<const T *>(p2);
  }

  bool memequal_simd(const char *p1, const char *p2) const;

  const char *pattern_;
  const char *pattern_end_;
  size_t pattern_len_;
  uint8_t first_;
  uint8_t last_;
  VecType vfirst_;
  VecType vlast_;
#if defined(__aarch64__) && defined(__ARM_NEON)
  VecType needle_prefix_cache_;
  MaskType needle_prefix_mask_;
#endif
};

// ============================================================================
// Platform-specific type aliases (for backward compatibility)
// ============================================================================

#if defined(__x86_64__) && defined(OB_USE_MULTITARGET_CODE)
OB_DECLARE_AVX2_SPECIFIC_CODE(
  using ObStringSearcher = ObStringSearcherImpl<SimdTraitsAVX2>;
)
#else
namespace specific { namespace avx2 { } }
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
OB_DECLARE_NEON_SPECIFIC_CODE(
  using ObStringSearcher = ObStringSearcherImpl<SimdTraitsNEON>;
);
#else
namespace specific { namespace neon { } }
#endif

// UTF8 utility functions
int utf8_loc_to_locb(const char *str, size_t length, uint32_t loc, int64_t &locb);
int utf8_locb_to_loc(const char *str, size_t length, int64_t locb, uint32_t &loc);
size_t utf8_char_length(const char *s);

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_STRING_SEARCHER_