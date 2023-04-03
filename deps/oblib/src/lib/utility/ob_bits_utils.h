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

#ifndef _OB_BITS_UTILS_H
#define _OB_BITS_UTILS_H 1
#include <stdint.h>
#include <assert.h>
#include "ob_macro_utils.h"
namespace oceanbase
{
namespace common
{
OB_INLINE constexpr bool ob_is_power_of_two(uint32_t x)
{
  //assert(0 != x);
  return ((x & (x - 1)) == 0);
}

OB_INLINE constexpr uint32_t ob_ceiling_div(uint32_t x, uint32_t y)
{
  return (x + y - 1) / y;
}

OB_INLINE bool ob_is_aligned(void *memptr, uint32_t alignment)
{
  assert(ob_is_power_of_two(alignment));
  return ((reinterpret_cast<uint64_t>(memptr)%alignment) == 0);
}

OB_INLINE constexpr int64_t ob_aligned_to(int64_t x, uint32_t alignment)
{
  return (x + alignment - 1) / alignment * alignment;
}

OB_INLINE constexpr int64_t ob_aligned_to2(int64_t x, uint32_t alignment)
{
  //assert(ob_is_power_of_two(alignment));
  return (x + alignment - 1) & ~(static_cast<uint64_t>(alignment) - 1);
}

OB_INLINE void* ob_aligned_to(void* memptr, uint32_t alignment)
{
  return reinterpret_cast<void*>(ob_aligned_to2(reinterpret_cast<uint64_t>(memptr), alignment));
}

// count of ones
OB_INLINE uint64_t ob_popcount64(uint64_t v)
{
  int64_t cnt = 0;
#if __POPCNT__
  cnt = __builtin_popcountl(v);
#else
  if (0 != v) {
    v -= ((v >> 1) & 0x5555555555555555UL);
    v = (v & 0x3333333333333333UL) + ((v >> 2) & 0x3333333333333333UL);
    cnt = (((v + (v >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56;
  }
#endif
  return cnt;
}

OB_INLINE uint32_t ob_popcount32(uint32_t x)
{
#if __POPCNT__
  return __builtin_popcount(x);
#else
  /* 32-bit recursive reduction using SWAR...
       but first step is mapping 2-bit values
       into sum of 2 1-bit values in sneaky way
  */
  x -= ((x >> 1) & 0x55555555);
  x = (((x >> 2) & 0x33333333) + (x & 0x33333333));
  x = (((x >> 4) + x) & 0x0f0f0f0f);
  x += (x >> 8);
  x += (x >> 16);
  return(x & 0x0000003f);
#endif
}

// leader zero count
OB_INLINE uint32_t ob_lzc32(uint32_t x)
{
  x |= (x >> 1);
  x |= (x >> 2);
  x |= (x >> 4);
  x |= (x >> 8);
  x |= (x >> 16);
  return (32 - ob_popcount32(x));
}

// the min bits count to hold a value
OB_INLINE uint32_t ob_min_bits(uint32_t x)
{
  x |= (x >> 1);
  x |= (x >> 2);
  x |= (x >> 4);
  x |= (x >> 8);
  x |= (x >> 16);
  return ob_popcount32(x);
}

// next largest power of 2
OB_INLINE uint32_t ob_nlpo2(uint32_t x)
{
  x |= (x >> 1);
  x |= (x >> 2);
  x |= (x >> 4);
  x |= (x >> 8);
  x |= (x >> 16);
  return (x+1);
}

// the floor of the base 2 log of x
OB_INLINE uint32_t ob_floor_log2(uint32_t x)
{
  x |= (x >> 1);
  x |= (x >> 2);
  x |= (x >> 4);
  x |= (x >> 8);
  x |= (x >> 16);
  return (ob_popcount32(x >> 1));
}

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_BITS_UTILS_H */
