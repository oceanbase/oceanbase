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


// Original author: liuzhenlong.lzl <> (137ca4f6dbd9)
// This file extracted ObStringSearcher from ob_expr_like.h/ob_expr_like.cpp to create a reusable class.

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_string_searcher.h"

namespace oceanbase
{
namespace common
{
OB_DECLARE_AVX2_SPECIFIC_CODE(

int ObStringSearcher::init(const char *pattern, size_t len)
{
  int ret = OB_SUCCESS;
  if (nullptr == pattern || 0 == len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. pattern is null.", K(ret), K(pattern),
              K(len));
  } else {
    pattern_ = pattern;
    pattern_end_ = pattern_ + len;
    pattern_len_ = len;

    first_ = *pattern;
    vfirst_ = _mm256_set1_epi8(first_);
    if (2 <= pattern_len_) {
      last_ = *(pattern_end_ - 1);
      vlast_ = _mm256_set1_epi8(last_);
    }
  }
  return ret;
}


int ObStringSearcher::instr(const char *text, const char *text_end, int64_t &res, bool &find) const
{
  int ret = OB_SUCCESS;
  find = false;
  res = text_end - text;
  const char *text_cur = text;
  // `pattern_` will not be null because it is prepared in `set_instr_info()`.
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument. pattern_ is null.", K(ret), K(pattern_),
    //          K(pattern_len_));
    OB_LOG(WARN, "invalid argument. pattern_ is null.", K(ret), K(pattern_),
              K(pattern_len_));
  } else if (text == text_end) {
    // `text` is NULL, so `res` will be 0.
    find = false;
    res = 0;
  } else if (1 == pattern_len_) {
    // Here is the quick path when `pattern_len_` is 1.
    // All elements of `vfirst_`(__m128i) are first byte of `pattern_`.
    // We will compare `text` and `vfirst_` using avx(__mm256i) in each
    // iteration. Align the end of `text` based on `AVX2_SIZE`.
    const char *avx_end = text + ((text_end - text) & ~(AVX2_SIZE - 1));
    for (; text_cur < avx_end; text_cur += AVX2_SIZE) {
      __m256i first_block =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(text_cur));
      __m256i first_cmp = _mm256_cmpeq_epi8(first_block, vfirst_);
      uint32_t mask = _mm256_movemask_epi8(first_cmp);
      if (0 != mask) {
        find = true;
        res = text_cur - text + __builtin_ctz(mask);
        break;
      }
    }
  } else {
    // Here is the common path when `pattern_len_` is greater than 1.
    // First, find positions in `text` that are equal to the first and last
    // byte of `pattern_` at same time using avx(__mm256i). Then, use func
    // `memequal_opt` to compare middle bytes of `pattern_` and corresponding
    // bytes of `text`.
    // Align the end of `text` based on `AVX2_SIZE` and `pattern_len_`.
    const char *avx_end =
        text + ((text_end - (text + pattern_len_ - 1)) & ~(AVX2_SIZE - 1));
    for (; !find && text_cur < avx_end; text_cur += AVX2_SIZE) {
      const char *last_cur = text_cur + pattern_len_ - 1;
      __m256i first_block =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(text_cur));
      __m256i last_block =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(last_cur));
      __m256i first_cmp = _mm256_cmpeq_epi8(first_block, vfirst_);
      __m256i last_cmp = _mm256_cmpeq_epi8(last_block, vlast_);
      uint32_t mask =
          _mm256_movemask_epi8(_mm256_and_si256(first_cmp, last_cmp));
      while (mask != 0) {
        int offset = __builtin_ctz(mask);
        // The first and the last bytes match, so we don't need to compare
        // them again.
        if (2 == pattern_len_ ||
            memequal_opt(text_cur + offset + 1, pattern_ + 1,
                          pattern_len_ - 2)) {
          find = true;
          res = text_cur - text + offset;
          break;
        }
        mask &= (mask - 1);
      }
    }
  }
  // Handle the tail of text.
  if (!find && text_end - text_cur >= pattern_len_) {
    void *ptr = MEMMEM(text_cur, text_end - text_cur, pattern_, pattern_len_);
    if (!OB_ISNULL(ptr)) {
      find = true;
      res = static_cast<char *>(ptr) - text;
    }
  }
  return ret;
}


int ObStringSearcher::is_substring(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  res = false;
  const char *text_cur = text;
  // `pattern_` will not be null because it is prepared in `set_instr_info()`.
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. pattern_ is null.", K(ret), K(pattern_),
              K(pattern_len_));
  } else if (text == text_end) {
    // `text` is NULL, so `res` will be false.
  } else if (1 == pattern_len_) {
    // Here is the quick path when `pattern_len_` is 1.
    // All elements of `vfirst_`(__m128i) are first byte of `pattern_`.
    // We will compare `text` and `vfirst_` using avx(__mm256i) in each
    // iteration. Align the end of `text` based on `AVX2_SIZE`.
    const char *avx_end = text + ((text_end - text) & ~(AVX2_SIZE - 1));
    for (; text_cur < avx_end; text_cur += AVX2_SIZE) {
      __m256i first_block =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(text_cur));
      __m256i first_cmp = _mm256_cmpeq_epi8(first_block, vfirst_);
      uint32_t mask = _mm256_movemask_epi8(first_cmp);
      if (0 != mask) {
        res = true;
        break;
      }
    }
  } else {
    // Here is the common path when `pattern_len_` is greater than 1.
    // First, find positions in `text` that are equal to the first and last
    // byte of `pattern_` at same time using avx(__mm256i). Then, use func
    // `memequal_opt` to compare middle bytes of `pattern_` and corresponding
    // bytes of `text`.
    // Align the end of `text` based on `AVX2_SIZE` and `pattern_len_`.
    const char *avx_end =
        text + ((text_end - (text + pattern_len_ - 1)) & ~(AVX2_SIZE - 1));
    for (; !res && text_cur < avx_end; text_cur += AVX2_SIZE) {
      const char *last_cur = text_cur + pattern_len_ - 1;
      __m256i first_block =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(text_cur));
      __m256i last_block =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(last_cur));
      __m256i first_cmp = _mm256_cmpeq_epi8(first_block, vfirst_);
      __m256i last_cmp = _mm256_cmpeq_epi8(last_block, vlast_);
      uint32_t mask =
          _mm256_movemask_epi8(_mm256_and_si256(first_cmp, last_cmp));
      while (mask != 0) {
        int offset = __builtin_ctz(mask);
        // The first and the last bytes match, so we don't need to compare
        // them again.
        if (2 == pattern_len_ ||
            memequal_opt(text_cur + offset + 1, pattern_ + 1,
                          pattern_len_ - 2)) {
          res = true;
          break;
        }
        mask &= (mask - 1);
      }
    }
  }
  // Handle the tail of text.
  if (!res && text_end - text_cur >= pattern_len_) {
    res =
        NULL != MEMMEM(text_cur, text_end - text_cur, pattern_, pattern_len_);
  }
  return ret;
}

int ObStringSearcher::start_with(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  // pattern_ will not be null because it is prepared in set_instr_info().
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. pattern_ is null.", K(ret), K(pattern_),
              K(pattern_len_));
  } else if (pattern_len_ > text_end - text) {
    res = false;
  } else {
    res = memequal_opt(text, pattern_, pattern_len_);
  }
  return ret;
}

int ObStringSearcher::end_with(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  // pattern_ will not be null because it is prepared in set_instr_info().
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. pattern_ is null.", K(ret), K(pattern_),
              K(pattern_len_));
  } else if (pattern_len_ > text_end - text) {
    res = false;
  } else {
    res = memequal_opt(text_end - pattern_len_, pattern_, pattern_len_);
  }
  return ret;
}

// Determines if `text` equals with `pattern_`.
int ObStringSearcher::equal(const char *text, const char *text_end, bool &res) const
{
  int ret = OB_SUCCESS;
  // pattern_ will not be null because it is prepared in set_instr_info().
  if (nullptr == pattern_ || 0 == pattern_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. pattern_ is null.", K(ret), K(pattern_),
              K(pattern_len_));
  } else if (pattern_len_ != text_end - text) {
    res = false;
  } else {
    res = memequal_opt(text, pattern_, pattern_len_);
  }
  return res;
}

bool ObStringSearcher::memequal_opt(const char *s1, const char *s2, size_t n) const
{
  switch (n) {
  case 1:
    return *s1 == *s2;
  case 2:
    return memequal_plain<int16_t>(s1, s2);
  case 3:
    return memequal_plain<int16_t>(s1, s2) &&
            memequal_plain<int8_t>(s1 + 2, s2 + 2);
  case 4:
    return memequal_plain<int32_t>(s1, s2);
  case 5:
    return memequal_plain<int32_t>(s1, s2) &&
            memequal_plain<int8_t>(s1 + 4, s2 + 4);
  case 6:
    return memequal_plain<int32_t>(s1, s2) &&
            memequal_plain<int16_t>(s1 + 4, s2 + 4);
  case 7:
    return memequal_plain<int32_t>(s1, s2) &&
            memequal_plain<int16_t>(s1 + 4, s2 + 4) &&
            memequal_plain<int8_t>(s1 + 6, s2 + 6);
  case 8:
    return memequal_plain<int64_t>(s1, s2);
  default:
    break;
  }
  if (n <= 16) {
    return memequal_plain<int64_t>(s1, s2) &&
            memequal_plain<int64_t>(s1 + n - 8, s2 + n - 8);
  }
  while (n >= 64) {
    if (memequal_sse<4>(s1, s2)) {
      s1 += 64;
      s2 += 64;
      n -= 64;
    } else {
      return false;
    }
  }
  switch (n / 16) {
  case 3:
    if (!memequal_sse<1>(s1 + 32, s2 + 32)) {
      return false;
    }
  case 2:
    if (!memequal_sse<1>(s1 + 16, s2 + 16)) {
      return false;
    }
  case 1:
    if (!memequal_sse<1>(s1, s2)) {
      return false;
    }
  }
  return memequal_sse<1>(s1 + n - 16, s2 + n - 16);
}

// compare the values of two int8_t, int16_t or other comparable plain types.
template <typename T>
bool ObStringSearcher::memequal_plain(const char *p1, const char *p2) const
{
  return *reinterpret_cast<const T *>(p1) == *reinterpret_cast<const T *>(p2);
}

// compare two values by sse, cnt means the count of __m128i to compare.
template <int cnt>
bool ObStringSearcher::memequal_sse(const char *p1, const char *p2) const
{
  if (cnt == 1) {
    return 0xFFFF ==
            _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))));
  }
  if (cnt == 4) {
    return 0xFFFF ==
            _mm_movemask_epi8(_mm_and_si128(
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                        _mm_loadu_si128(
                            reinterpret_cast<const __m128i *>(p2))),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) +
                                        1),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) +
                                        1))),
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) +
                                        2),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) +
                                        2)),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) +
                                        3),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) +
                                        3)))));
  }
}

)

size_t utf8_char_length(const char *s)
{
  unsigned char lead = static_cast<unsigned char>(s[0]);
  if (lead >= 0x00 && lead <= 0x7F)
    return 1;
  else if (lead >= 0xC2 && lead <= 0xDF)
    return 2;
  else if (lead >= 0xE0 && lead <= 0xEF)
    return 3;
  else if (lead >= 0xF0 && lead <= 0xF4)
    return 4;
  return 0;
}

int utf8_loc_to_locb(const char *str, size_t length, uint32_t loc, int64_t &locb)
{
  // loc starts from 1, locb starts from 0
  int ret = OB_SUCCESS;
  locb = 0;
  int64_t l = 1;
  while (locb < length && l < loc) {
    size_t char_len = utf8_char_length(&str[locb]);
    locb += char_len;
    ++l;
  }
  if (l != loc) {
    // loc out of bound
    locb = -1;
  }
  return ret;
}

int utf8_locb_to_loc(const char *str, size_t length, int64_t locb, uint32_t &loc)
{
  int ret = OB_SUCCESS;
  int64_t lb = 0;
  loc = 1;
  while (lb < length && lb < locb) {
    size_t char_len = utf8_char_length(&str[lb]);
    lb += char_len;
    ++loc;
  }
  if (lb != locb) {
    // illegal locb
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal locb", K(ret), K(length), K(locb), K(loc));
  }
  return ret;
}
}
}