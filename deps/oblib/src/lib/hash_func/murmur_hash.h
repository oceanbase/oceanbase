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

inline uint32_t rotl32 (uint32_t x, int8_t r)
{
  return (x << r) | (x >> (32 - r));
}

inline uint32_t getblock32 (const uint32_t *p, int i)
{
  return p[i];
}

inline uint32_t fmix32 (uint32_t h)
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

// used to calculate bucket partition of iceberg table
inline uint32_t murmurhash3_x86_32 (const void *key, int32_t len, uint32_t seed)
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 4;

  uint32_t ret = seed;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  //----------
  // body

  const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);

  for(int i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock32(blocks,i);

    k1 *= c1;
    k1 = rotl32(k1,15);
    k1 *= c2;

    ret ^= k1;
    ret = rotl32(ret,13);
    ret = ret*5+0xe6546b64;
  }

  //----------
  // tail

  const uint8_t * tail = (const uint8_t*)(data + nblocks*4);

  uint32_t k1 = 0;

  switch(len & 3)
  {
  case 3: k1 ^= tail[2] << 16;
  case 2: k1 ^= tail[1] << 8;
  case 1: k1 ^= tail[0];
          k1 *= c1;
          k1 = rotl32(k1,15);
          k1 *= c2;
          ret ^= k1;
  };

  //----------
  // finalization

  ret ^= len;

  ret = fmix32(ret);

  return ret;
}

}//namespace common
}//namespace oceanbase
#endif // OCEANBASE_LIB_HASH_MURMURHASH_
