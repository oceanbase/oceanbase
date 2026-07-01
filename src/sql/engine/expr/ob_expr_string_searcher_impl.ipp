/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include <cstddef>

namespace oceanbase
{
namespace common
{

template <typename SimdTraits>
bool ObStringSearcherImpl<SimdTraits>::compare_middle(const char *text_offset, size_t pattern_len) const
{
  return 2 == pattern_len ||
         memequal_opt(text_offset + 1, pattern_ + 1, pattern_len - 2);
}

template <typename SimdTraits>
bool ObStringSearcherImpl<SimdTraits>::verify_match_at(const char *at, const char *haystack_end) const
{
#if defined(__aarch64__) && defined(__ARM_NEON)
  if (2 == pattern_len_) {
    return true;
  }
  const ptrdiff_t rem = haystack_end - at;
  if (rem < static_cast<ptrdiff_t>(pattern_len_)) {
    return false;
  }
  if (rem < 16) {
    return memequal_opt(at, pattern_, pattern_len_);
  }
  const VecType hay = SimdTraits::load(at);
  const VecType cmp = SimdTraits::cmpeq(hay, needle_prefix_cache_);
  const MaskType m = SimdTraits::movemask(cmp);
  if (pattern_len_ <= 16) {
    return (m & needle_prefix_mask_) == needle_prefix_mask_;
  }
  if (m != static_cast<MaskType>(0xFFFF)) {
    return false;
  }
  return memequal_opt(at + 16, pattern_ + 16, pattern_len_ - 16);
#else
  return compare_middle(at, pattern_len_);
#endif
}

template <typename SimdTraits>
ObStringSearcherImpl<SimdTraits>::ObStringSearcherImpl()
    : pattern_(nullptr), pattern_end_(nullptr), pattern_len_(0)
{}

template <typename SimdTraits>
int ObStringSearcherImpl<SimdTraits>::init(const char *pattern, size_t len)
{
  int ret = OB_SUCCESS;
  if (nullptr == pattern || 0 == len) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument. pattern is null.", K(ret), K(pattern), K(len));
  } else {
    pattern_ = pattern;
    pattern_end_ = pattern_ + len;
    pattern_len_ = len;

    first_ = *pattern;
    vfirst_ = SimdTraits::set1(first_);
    if (2 <= pattern_len_) {
      last_ = *(pattern_end_ - 1);
      vlast_ = SimdTraits::set1(last_);
#if defined(__aarch64__) && defined(__ARM_NEON)
      char buf[16];
      MEMSET(buf, 0, sizeof(buf));
      const size_t ncopy = MIN(pattern_len_, static_cast<size_t>(16));
      MEMCPY(buf, pattern_, ncopy);
      needle_prefix_cache_ = SimdTraits::load(buf);
      needle_prefix_mask_ = (pattern_len_ >= 16) ? static_cast<MaskType>(0xFFFF)
                                                 : static_cast<MaskType>((1u << pattern_len_) - 1);
#endif
    }
  }
  return ret;
}

template <typename SimdTraits>
int ObStringSearcherImpl<SimdTraits>::instr(const char *text, const char *text_end, int64_t &res, bool &find) const
{
  int ret = OB_SUCCESS;
  find = false;
  res = text_end - text;
  const char *text_cur = text;

  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument. pattern_ is null.", K(ret), K(pattern_), K(pattern_len_));
  } else if (text == text_end) {
    find = false;
    res = 0;
  } else if (1 == pattern_len_) {
    const char *simd_end = text + ((text_end - text) & ~(SIMD_SIZE - 1));
    for (; text_cur < simd_end; text_cur += SIMD_SIZE) {
      VecType first_block = SimdTraits::load(text_cur);
      VecType first_cmp = SimdTraits::cmpeq(first_block, vfirst_);
      MaskType mask;
      if ((mask = SimdTraits::has_match(first_cmp)) != 0) {
        find = true;
        res = text_cur - text + __builtin_ctz(mask);
        break;
      }
    }
  } else {
    const char *simd_end = text + ((text_end - (text + pattern_len_ - 1)) & ~(SIMD_SIZE - 1));
    for (; !find && text_cur < simd_end; text_cur += SIMD_SIZE) {
      const char *last_cur = text_cur + pattern_len_ - 1;
      VecType first_block = SimdTraits::load(text_cur);
      VecType last_block = SimdTraits::load(last_cur);
      VecType first_cmp = SimdTraits::cmpeq(first_block, vfirst_);
      VecType last_cmp = SimdTraits::cmpeq(last_block, vlast_);
      VecType combined = SimdTraits::and_op(first_cmp, last_cmp);
      MaskType mask;
      if ((mask = SimdTraits::has_match(combined)) != 0) {
        while (mask != 0) {
          int offset = __builtin_ctz(mask);
          if (verify_match_at(text_cur + offset, text_end)) {
            find = true;
            res = text_cur - text + offset;
            break;
          }
          mask &= (mask - 1);
        }
      }
    }
  }

  if (!find && text_end - text_cur >= static_cast<ptrdiff_t>(pattern_len_)) {
    void *ptr = MEMMEM(text_cur, text_end - text_cur, pattern_, pattern_len_);
    if (!OB_ISNULL(ptr)) {
      find = true;
      res = static_cast<char *>(ptr) - text;
    }
  }
  return ret;
}

template <typename SimdTraits>
int ObStringSearcherImpl<SimdTraits>::is_substring(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  res = false;
  const char *text_cur = text;

  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument. pattern_ is null.", K(ret), K(pattern_), K(pattern_len_));
  } else if (text == text_end) {
  } else if (1 == pattern_len_) {
    const char *simd_end = text + ((text_end - text) & ~(SIMD_SIZE - 1));
    for (; text_cur < simd_end; text_cur += SIMD_SIZE) {
      VecType first_block = SimdTraits::load(text_cur);
      VecType first_cmp = SimdTraits::cmpeq(first_block, vfirst_);
      if (SimdTraits::matched(first_cmp)) {
        res = true;
        break;
      }
    }
    if (!res) {
      for (; text_cur < text_end; text_cur++) {
        if (*text_cur == first_) {
          res = true;
          break;
        }
      }
    }
  } else {
    const char *simd_end = text + ((text_end - (text + pattern_len_ - 1)) & ~(SIMD_SIZE - 1));
    for (; !res && text_cur < simd_end; text_cur += SIMD_SIZE) {
      const char *last_cur = text_cur + pattern_len_ - 1;
      VecType first_block = SimdTraits::load(text_cur);
      VecType last_block = SimdTraits::load(last_cur);
      VecType first_cmp = SimdTraits::cmpeq(first_block, vfirst_);
      VecType last_cmp = SimdTraits::cmpeq(last_block, vlast_);
      VecType combined = SimdTraits::and_op(first_cmp, last_cmp);
      MaskType mask;
      if ((mask = SimdTraits::has_match(combined)) != 0) {
        while (mask != 0) {
          int offset = __builtin_ctz(mask);
          if (verify_match_at(text_cur + offset, text_end)) {
            res = true;
            break;
          }
          mask &= (mask - 1);
        }
      }
    }
  }

  if (!res && text_end - text_cur >= static_cast<ptrdiff_t>(pattern_len_)) {
    res = NULL != MEMMEM(text_cur, text_end - text_cur, pattern_, pattern_len_);
  }
  return ret;
}

template <typename SimdTraits>
int ObStringSearcherImpl<SimdTraits>::start_with(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument. pattern_ is null.", K(ret), K(pattern_), K(pattern_len_));
  } else if (pattern_len_ > static_cast<size_t>(text_end - text)) {
    res = false;
  } else {
    res = memequal_opt(text, pattern_, pattern_len_);
  }
  return ret;
}

template <typename SimdTraits>
int ObStringSearcherImpl<SimdTraits>::end_with(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument. pattern_ is null.", K(ret), K(pattern_), K(pattern_len_));
  } else if (pattern_len_ > static_cast<size_t>(text_end - text)) {
    res = false;
  } else {
    res = memequal_opt(text_end - pattern_len_, pattern_, pattern_len_);
  }
  return ret;
}

template <typename SimdTraits>
int ObStringSearcherImpl<SimdTraits>::equal(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument. pattern_ is null.", K(ret), K(pattern_), K(pattern_len_));
  } else if (pattern_len_ != static_cast<size_t>(text_end - text)) {
    res = false;
  } else {
    res = memequal_opt(text, pattern_, pattern_len_);
  }
  return ret;
}

template <typename SimdTraits>
bool ObStringSearcherImpl<SimdTraits>::memequal_opt(const char *s1, const char *s2, size_t n) const
{
  switch (n) {
  case 1: return *s1 == *s2;
  case 2: return memequal_plain<int16_t>(s1, s2);
  case 3: return memequal_plain<int16_t>(s1, s2) && memequal_plain<int8_t>(s1 + 2, s2 + 2);
  case 4: return memequal_plain<int32_t>(s1, s2);
  case 5: return memequal_plain<int32_t>(s1, s2) && memequal_plain<int8_t>(s1 + 4, s2 + 4);
  case 6: return memequal_plain<int32_t>(s1, s2) && memequal_plain<int16_t>(s1 + 4, s2 + 4);
  case 7: return memequal_plain<int32_t>(s1, s2) && memequal_plain<int16_t>(s1 + 4, s2 + 4)
              && memequal_plain<int8_t>(s1 + 6, s2 + 6);
  case 8: return memequal_plain<int64_t>(s1, s2);
  default: break;
  }

  if (n <= 16) {
    return memequal_plain<int64_t>(s1, s2) && memequal_plain<int64_t>(s1 + n - 8, s2 + n - 8);
  }

  while (n >= static_cast<size_t>(SIMD_SIZE * 2)) {
    if (memequal_simd(s1, s2) && memequal_simd(s1 + SIMD_SIZE, s2 + SIMD_SIZE)) {
      s1 += SIMD_SIZE * 2;
      s2 += SIMD_SIZE * 2;
      n -= SIMD_SIZE * 2;
    } else {
      return false;
    }
  }

  if (n >= static_cast<size_t>(SIMD_SIZE)) {
    if (!memequal_simd(s1, s2)) {
      return false;
    }
    s1 += SIMD_SIZE;
    s2 += SIMD_SIZE;
    n -= SIMD_SIZE;
  }

  // Compare the remaining n bytes (n in [1, SIMD_SIZE)). A single pair of 8-byte
  // overlapping reads only covers up to 16 bytes, so for AVX2 (SIMD_SIZE == 32) the
  // tail can be up to 31 bytes and would leave an uncompared middle gap; meanwhile a
  // plain forward 8-byte read overshoots the buffer when n < 8. Walk full 8-byte
  // words from the front, then issue one final 8-byte read anchored at the region end
  // (s1 + n) for the last < 8 bytes. When n < 8 that final read reaches back into the
  // >= SIMD_SIZE bytes already compared above, so it stays within both buffers.
  if (n > 0) {
    bool eq = true;
    size_t i = 0;
    for (; eq && i + 8 <= n; i += 8) {
      eq = memequal_plain<int64_t>(s1 + i, s2 + i);
    }
    if (eq && i < n) {
      eq = memequal_plain<int64_t>(s1 + n - 8, s2 + n - 8);
    }
    return eq;
  }
  return true;
}

template <typename SimdTraits>
bool ObStringSearcherImpl<SimdTraits>::memequal_simd(const char *p1, const char *p2) const
{
  VecType v1 = SimdTraits::load(p1);
  VecType v2 = SimdTraits::load(p2);
  VecType cmp = SimdTraits::cmpeq(v1, v2);
  return SimdTraits::cmp_all_equal(cmp);
}

} // namespace common
} // namespace oceanbase
