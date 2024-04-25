// (C) 2010-2016 Alibaba Group Holding Limited.
//
// Authors:
// Normalizer:

#ifndef OCEANBASE_LIB_HASH_MURMURHASH_
#define OCEANBASE_LIB_HASH_MURMURHASH_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include "common/ob_target_specific.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_errno.h"

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace oceanbase
{
namespace common
{

#define VAL_LOAD(unused, idx)                                                  \
  __m512i val##idx = _mm512_loadu_si512(data + hash_idx + (idx - 1) * 8)
#define VAL_MUL(unused, idx)                                                   \
  val##idx = _mm512_mullo_epi64(val##idx, multiply512)
#define VAL_SR_XOR(unused, idx)                                                \
  val##idx = _mm512_xor_si512(val##idx, _mm512_srli_epi64(val##idx, rotate))
#define SEEDS(i) (IS_BATCH_SEED ? seeds[i] : seeds[0])
#define RETX(i) (long long)(SEEDS(hash_idx + i) ^ (KEY_LEN * multiply))
#define RET_LOAD1(unused, idx)                                                 \
  int64_t ret##idx = SEEDS(hash_idx + idx - 1) ^ (KEY_LEN * multiply) ^        \
                     uint64_t(data[hash_idx + idx - 1])
#define RET_LOAD2(unused, idx)                                                 \
  int64_t ret##idx = SEEDS(hash_idx + idx - 1) ^ (KEY_LEN * multiply) ^        \
                     (uint64_t(data[(hash_idx + idx) * 2 - 1]) << 8) ^         \
                     (uint64_t(data[(hash_idx + idx) * 2 - 2]))
#define RET_LOAD4(unused, idx)                                                 \
  int64_t ret##idx = SEEDS(hash_idx + idx - 1) ^ (KEY_LEN * multiply) ^        \
                     (uint64_t(data[(hash_idx + idx) * 4 - 1]) << 24) ^        \
                     (uint64_t(data[(hash_idx + idx) * 4 - 2]) << 16) ^        \
                     (uint64_t(data[(hash_idx + idx) * 4 - 3]) << 8) ^         \
                     (uint64_t(data[(hash_idx + idx) * 4 - 4]))
#define RET_DEF(unused, idx)                                                   \
  __m512i ret##idx {                                                           \
    RETX((idx - 1) * 8), RETX((idx - 1) * 8 + 1), RETX((idx - 1) * 8 + 2),     \
        RETX((idx - 1) * 8 + 3), RETX((idx - 1) * 8 + 4),                      \
        RETX((idx - 1) * 8 + 5), RETX((idx - 1) * 8 + 6),                      \
        RETX((idx - 1) * 8 + 7)                                                \
  }
#define RET_XOR(unused, idx) ret##idx = _mm512_xor_si512(ret##idx, val##idx)
#define RET_MUL(unused, idx) ret##idx *= multiply
#define RET_MUL_SIMD(unused, idx)                                              \
  ret##idx = _mm512_mullo_epi64(ret##idx, multiply512)
#define RET_SR_XOR(unused, idx)                                                \
  ret##idx = _mm512_xor_si512(ret##idx, _mm512_srli_epi64(ret##idx, rotate));
#define RET_STORE(unused, idx)                                                 \
  _mm512_storeu_si512(hashes + hash_idx + 8 * (idx - 1), ret##idx)
#define RET_CALC_STROE()                                                       \
  __m512i ret512{ret1, ret2, ret3, ret4, ret5, ret6, ret7, ret8};              \
  ret512 = _mm512_xor_si512(ret512, _mm512_srli_epi64(ret512, rotate));        \
  ret512 = _mm512_mullo_epi64(ret512, multiply512);                            \
  ret512 = _mm512_xor_si512(ret512, _mm512_srli_epi64(ret512, rotate));        \
  _mm512_storeu_si512(hashes + hash_idx, ret512);
#define BIT_OPS(...)                                                           \
  LST_DO2(VAL_LOAD, (;), ##__VA_ARGS__);                                       \
  LST_DO2(VAL_MUL, (;), ##__VA_ARGS__);                                        \
  LST_DO2(VAL_SR_XOR, (;), ##__VA_ARGS__);                                     \
  LST_DO2(VAL_MUL, (;), ##__VA_ARGS__);                                        \
  LST_DO2(RET_DEF, (;), ##__VA_ARGS__);                                        \
  LST_DO2(RET_XOR, (;), ##__VA_ARGS__);                                        \
  LST_DO2(RET_MUL_SIMD, (;), ##__VA_ARGS__);                                   \
  LST_DO2(RET_SR_XOR, (;), ##__VA_ARGS__);                                     \
  LST_DO2(RET_MUL_SIMD, (;), ##__VA_ARGS__);                                   \
  LST_DO2(RET_SR_XOR, (;), ##__VA_ARGS__);                                     \
  LST_DO2(RET_STORE, (;), ##__VA_ARGS__);

#define MURMURHASH64A(key, len, seed)                                          \
  {                                                                            \
    const uint64_t multiply = 0xc6a4a7935bd1e995;                              \
    const int rotate = 47;                                                     \
    uint64_t ret = seed ^ (len * multiply);                                    \
    const uint64_t *data = (const uint64_t *)key;                              \
    const uint64_t *end = data + (len / 8);                                    \
    for (; len >= 8; len -= 8) {                                               \
      uint64_t val = *data;                                                    \
      val *= multiply;                                                         \
      val ^= val >> rotate;                                                    \
      val *= multiply;                                                         \
      ret ^= val;                                                              \
      ret *= multiply;                                                         \
      ++data;                                                                  \
    }                                                                          \
    const unsigned char *data2 = (const unsigned char *)data;                  \
    while (len > 0) {                                                          \
      --len;                                                                   \
      ret ^= uint64_t(data2[len]) << (len * 8);                                \
      if (0 == len) {                                                          \
        ret *= multiply;                                                       \
      }                                                                        \
    }                                                                          \
    ret ^= ret >> rotate;                                                      \
    ret *= multiply;                                                           \
    ret ^= ret >> rotate;                                                      \
    return ret;                                                                \
  }

inline uint64_t murmurhash64A(const void *key, int32_t len, uint64_t seed)
{
  MURMURHASH64A(key, len, seed);
  #undef MURMURHASH64A
}

// The MurmurHash 2 from Austin Appleby, faster and better mixed (but weaker
// crypto-wise with one pair of obvious differential) than both Lookup3 and
// SuperFastHash. Not-endian neutral for speed.
uint32_t murmurhash2(const void *data, int32_t len, uint32_t hash);

// MurmurHash2, 64-bit versions, by Austin Appleby
// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.
// 64-bit hash for 64-bit platforms
uint64_t murmurhash64A(const void *key, int32_t len, uint64_t seed);

//public function, please only use this one
inline uint64_t murmurhash(const void *data, int32_t len, uint64_t hash)
{
  return murmurhash64A(data, len, hash);
}

inline uint64_t appname_hash(const void *data, int32_t len, uint64_t hash)
{
  return murmurhash64A(data, len, hash);
}

uint32_t fnv_hash2(const void *data, int32_t len, uint32_t hash);

OB_DECLARE_AVX512_SPECIFIC_CODE(
template<size_t KEY_LEN, bool IS_BATCH_SEED>
inline void murmurhash64A(const void *keys, uint64_t *hashes, size_t total_len, const uint64_t *seeds)
{
  int32_t key_cnt = total_len / KEY_LEN;
  uint32_t hash_idx = 0;

  const uint64_t multiply = 0xc6a4a7935bd1e995;
  const int rotate = 47;

  __m512i multiply512 = _mm512_set1_epi64(multiply);

  const unsigned char *data = static_cast<const unsigned char *>(keys);
  if (KEY_LEN == 1) {
    for (; key_cnt >= 8; key_cnt -= 8) {
      LST_DO2(RET_LOAD1, (;), 1, 2, 3, 4, 5, 6, 7, 8);
      LST_DO2(RET_MUL, (;), 1, 2, 3, 4, 5, 6, 7, 8);
      RET_CALC_STROE();
      hash_idx += 8;
    }
  } else if (KEY_LEN == 2) {
    for (; key_cnt >= 8; key_cnt -= 8) {
      LST_DO2(RET_LOAD2, (;), 1, 2, 3, 4, 5, 6, 7, 8);
      LST_DO2(RET_MUL, (;), 1, 2, 3, 4, 5, 6, 7, 8);
      RET_CALC_STROE();
      hash_idx += 8;
    }
  } else if (KEY_LEN == 4) {
    for (; key_cnt >= 8; key_cnt -= 8) {
      LST_DO2(RET_LOAD4, (;), 1, 2, 3, 4, 5, 6, 7, 8);
      LST_DO2(RET_MUL, (;), 1, 2, 3, 4, 5, 6, 7, 8);
      RET_CALC_STROE();
      hash_idx += 8;
    }
  } else if (KEY_LEN == 8) {
    const uint64_t *data = static_cast<const uint64_t *>(keys);
    for (; key_cnt >= 128; key_cnt -= 128) {
      BIT_OPS(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
      hash_idx += 128;
    }
    for (; key_cnt >= 64; key_cnt -= 64) {
      BIT_OPS(1, 2, 3, 4, 5, 6, 7, 8)
      hash_idx += 64;
    }
    for (; key_cnt >= 8; key_cnt -= 8) {
      BIT_OPS(1)
      hash_idx += 8;
    }
  }

  for (; key_cnt > 0; key_cnt--) {
    hashes[hash_idx] = common::murmurhash64A((char *)keys + hash_idx * KEY_LEN, KEY_LEN, SEEDS(hash_idx));
    hash_idx++;
  }
}
)

OB_DECLARE_DEFAULT_CODE(
template<size_t KEY_LEN, bool IS_BATCH_SEED>
inline void murmurhash64A(const void *keys, uint64_t *hashes, size_t total_len, const uint64_t *seeds)
{
  for (int i = 0; i < total_len / KEY_LEN; i++) {
    hashes[i] = common::murmurhash64A((char *)keys + i * KEY_LEN, KEY_LEN, SEEDS(i));
  }
}
)

template<size_t KEY_LEN, bool IS_BATCH_SEED>
inline int murmurhash64A(const void *keys, uint64_t *hashes, size_t total_len, const uint64_t *seeds)
{
  int ret = OB_SUCCESS;
  if (total_len % KEY_LEN != 0) {
    ret = OB_ERROR;
    COMMON_LOG(WARN, "total_len must be a multiple of KEY_LEN!");
    return ret;
  }
  if (KEY_LEN != 1 && KEY_LEN != 2 && KEY_LEN != 4 && KEY_LEN != 8 &&
      KEY_LEN != 16 && KEY_LEN != 32 && KEY_LEN != 64) {
    ret = OB_ERROR;
    COMMON_LOG(WARN, "KEY_LEN must be 1, 2, 4, 8, 16, 32 or 64!");
    return ret;
  }
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    common::specific::avx512::murmurhash64A<KEY_LEN, IS_BATCH_SEED>(keys, hashes, total_len, seeds);
  } else {
    common::specific::normal::murmurhash64A<KEY_LEN, IS_BATCH_SEED>(keys, hashes, total_len, seeds);
  }
#else
  common::specific::normal::murmurhash64A<KEY_LEN, IS_BATCH_SEED>(keys, hashes, total_len, seeds);
#endif
  return ret;
}

#undef VAL_LOAD
#undef VAL_MUL
#undef VAL_SR_XOR
#undef SEEDS
#undef RETX
#undef RET_LOAD1
#undef RET_LOAD2
#undef RET_LOAD4
#undef RET_DEF
#undef RET_XOR
#undef RET_MUL
#undef RET_MUL_SIMD
#undef RET_SR_XOR
#undef RET_STORE
#undef RET_CALC_STROE
#undef BIT_OPS
}//namespace common
}//namespace oceanbase
#endif // OCEANBASE_LIB_HASH_MURMURHASH_
