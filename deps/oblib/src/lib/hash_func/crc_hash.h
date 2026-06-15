/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_HASH_CRC_HASH_
#define OCEANBASE_LIB_HASH_CRC_HASH_

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#if defined(__SSE4_2__)
#define OB_CRC_HASH_USE_SSE42 1
#include <nmmintrin.h>
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#define OB_CRC_HASH_USE_ARM_CRC 1
#include <arm_acle.h>
#else
#define OB_CRC_HASH_USE_ZLIB 1
#include <zlib.h>
#endif

namespace oceanbase
{
namespace common
{

// 64-bit finalizer multiplier: odd, popcount = 32, good bit dispersion.
static const uint64_t OB_HASH_FMIX64_MULT = 0x2d358dccaa6c78a5ULL;

inline uint64_t ob_mul_u128(const uint64_t a, const uint64_t b, uint64_t *high)
{
#if defined(__SIZEOF_INT128__)
  const __uint128_t product = static_cast<__uint128_t>(a) * static_cast<__uint128_t>(b);
  *high = static_cast<uint64_t>(product >> 64);
  return static_cast<uint64_t>(product);
#else
  const uint64_t a_lo = static_cast<uint32_t>(a);
  const uint64_t a_hi = a >> 32;
  const uint64_t b_lo = static_cast<uint32_t>(b);
  const uint64_t b_hi = b >> 32;
  const uint64_t cross_1 = a_hi * b_lo;
  const uint64_t cross_2 = a_lo * b_hi;
  const uint64_t lo_lo = a_lo * b_lo;
  const uint64_t hi_hi = a_hi * b_hi;
  const uint64_t carry = ((lo_lo >> 32)
                          + static_cast<uint32_t>(cross_1)
                          + static_cast<uint32_t>(cross_2)) >> 32;
  *high = hi_hi + (cross_1 >> 32) + (cross_2 >> 32) + carry;
  return lo_lo + (cross_1 << 32) + (cross_2 << 32);
#endif
}

template <typename T>
inline T ob_load_unaligned(const void *src)
{
  T value;
  memcpy(&value, src, sizeof(T));
  return value;
}

template <size_t Width>
struct ObHashFinalize
{};

template <>
struct ObHashFinalize<4>
{
  inline uint32_t operator()(uint32_t value) const
  {
    value ^= value >> 16;
    value *= 0x85ebca6bU;
    value ^= value >> 13;
    value *= 0xc2b2ae35U;
    value ^= value >> 16;
    return value;
  }
};

template <>
struct ObHashFinalize<8>
{
  inline uint64_t operator()(const uint64_t value) const
  {
    uint64_t high = 0;
    const uint64_t low = ob_mul_u128(value ^ OB_HASH_FMIX64_MULT,
                                     OB_HASH_FMIX64_MULT, &high);
    return high ^ low;
  }
};

inline uint32_t crc_hash_32(const void *data, int32_t bytes, uint32_t hash)
{
#if defined(OB_CRC_HASH_USE_SSE42)
  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(data);
  while (bytes >= static_cast<int32_t>(sizeof(uint64_t))) {
    hash = static_cast<uint32_t>(_mm_crc32_u64(hash, ob_load_unaligned<uint64_t>(ptr)));
    ptr += sizeof(uint64_t);
    bytes -= sizeof(uint64_t);
  }
  if (bytes >= static_cast<int32_t>(sizeof(uint32_t))) {
    hash = _mm_crc32_u32(hash, ob_load_unaligned<uint32_t>(ptr));
    ptr += sizeof(uint32_t);
    bytes -= sizeof(uint32_t);
  }
  if (bytes >= static_cast<int32_t>(sizeof(uint16_t))) {
    hash = _mm_crc32_u16(hash, ob_load_unaligned<uint16_t>(ptr));
    ptr += sizeof(uint16_t);
    bytes -= sizeof(uint16_t);
  }
  if (bytes > 0) {
    hash = _mm_crc32_u8(hash, *ptr);
  }
  return hash;
#elif defined(OB_CRC_HASH_USE_ARM_CRC)
  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(data);
  while (bytes >= static_cast<int32_t>(sizeof(uint64_t))) {
    hash = __crc32cd(hash, ob_load_unaligned<uint64_t>(ptr));
    ptr += sizeof(uint64_t);
    bytes -= sizeof(uint64_t);
  }
  if (bytes >= static_cast<int32_t>(sizeof(uint32_t))) {
    hash = __crc32cw(hash, ob_load_unaligned<uint32_t>(ptr));
    ptr += sizeof(uint32_t);
    bytes -= sizeof(uint32_t);
  }
  if (bytes >= static_cast<int32_t>(sizeof(uint16_t))) {
    hash = __crc32ch(hash, ob_load_unaligned<uint16_t>(ptr));
    ptr += sizeof(uint16_t);
    bytes -= sizeof(uint16_t);
  }
  if (bytes > 0) {
    hash = __crc32cb(hash, *ptr);
  }
  return hash;
#else
  return crc32(hash, reinterpret_cast<const Bytef *>(data), bytes);
#endif
}

inline uint64_t crc_hash_64_unmixed(const void *data, int32_t length, uint64_t hash)
{
#if defined(OB_CRC_HASH_USE_ZLIB)
  return static_cast<uint64_t>(crc32(static_cast<uLong>(hash),
                                     reinterpret_cast<const Bytef *>(data), length));
#else
  if (length < static_cast<int32_t>(sizeof(uint64_t))) {
    return static_cast<uint64_t>(crc_hash_32(data, length, static_cast<uint32_t>(hash)));
  }
  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(data);
  const uint8_t *const tail = ptr + length - sizeof(uint64_t);
  while (ptr < tail) {
#if defined(OB_CRC_HASH_USE_SSE42)
    hash = _mm_crc32_u64(hash, ob_load_unaligned<uint64_t>(ptr));
#elif defined(OB_CRC_HASH_USE_ARM_CRC)
    hash = __crc32cd(hash, ob_load_unaligned<uint64_t>(ptr));
#endif
    ptr += sizeof(uint64_t);
  }
#if defined(OB_CRC_HASH_USE_SSE42)
  hash = _mm_crc32_u64(hash, ob_load_unaligned<uint64_t>(tail));
#elif defined(OB_CRC_HASH_USE_ARM_CRC)
  hash = __crc32cd(hash, ob_load_unaligned<uint64_t>(tail));
#endif
  return hash;
#endif
}

inline uint64_t crc_hash_64(const void *data, int32_t length, uint64_t hash)
{
  return ObHashFinalize<8>()(crc_hash_64_unmixed(data, length, hash));
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_HASH_CRC_HASH_
