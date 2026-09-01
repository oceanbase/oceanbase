/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Original author: liuzhenlong.lzl <> (137ca4f6dbd9)
// This file extracted ObStringSearcher from ob_expr_like.h/ob_expr_like.cpp to create a reusable class.
// Refactored to use SIMD traits for code sharing between AVX2 and NEON implementations.

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_string_searcher.h"
#include "share/ob_errno.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/oblog/ob_log.h"

// Template member definitions must be compiled with AVX2 target on x86 multitarget
// (Clang: apply_to=function — do not put explicit template class inside this pragma).
#if defined(__x86_64__) && defined(OB_USE_MULTITARGET_CODE)
OB_BEGIN_AVX2_SPECIFIC_CODE
#include "sql/engine/expr/ob_expr_string_searcher_impl.ipp"
OB_END_TARGET_SPECIFIC_CODE
#elif defined(__aarch64__) && defined(__ARM_NEON)
#include "sql/engine/expr/ob_expr_string_searcher_impl.ipp"
#endif

namespace oceanbase
{
namespace common
{

// ============================================================================
// UTF8 utility functions (shared between all platforms)
// ============================================================================

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
  while (locb < static_cast<int64_t>(length) && l < static_cast<int64_t>(loc)) {
    size_t char_len = utf8_char_length(&str[locb]);
    locb += char_len;
    ++l;
  }
  if (l != static_cast<int64_t>(loc)) {
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
  while (lb < static_cast<int64_t>(length) && lb < locb) {
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

// ============================================================================
// Explicit template instantiation for platform-specific SIMD types
// ============================================================================

#if defined(__x86_64__) && defined(OB_USE_MULTITARGET_CODE)
template class ObStringSearcherImpl<specific::avx2::SimdTraitsAVX2>;
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
template class ObStringSearcherImpl<specific::neon::SimdTraitsNEON>;
#endif

} // namespace common
} // namespace oceanbase
