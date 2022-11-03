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

namespace oceanbase
{
namespace common
{

inline uint64_t murmurhash64A(const void *key, int32_t len, uint64_t seed)
{
  const uint64_t multiply = 0xc6a4a7935bd1e995;
  const int rotate = 47;

  uint64_t ret = seed ^ (len * multiply);

  const uint64_t *data = (const uint64_t *)key;
  const uint64_t *end = data + (len / 8);
  for (; len >= 8; len -= 8) {
    uint64_t val = *data;
    val *= multiply;
    val ^= val >> rotate;
    val *= multiply;
    ret ^= val;
    ret *= multiply;
    ++data;
  }

  const unsigned char *data2 = (const unsigned char *)data;
  while (len > 0) {
    --len;
    ret ^= uint64_t(data2[len]) << (len * 8);
    if (0 == len) {
      ret *= multiply;
    }
  }
  ret ^= ret >> rotate;
  ret *= multiply;
  ret ^= ret >> rotate;

  return ret;
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
}//namespace common
}//namespace oceanbase
#endif // OCEANBASE_LIB_HASH_MURMURHASH_
