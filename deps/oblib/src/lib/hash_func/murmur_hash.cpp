//-----------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby
// (public domain, cf. http://murmurhash.googlepages.com/)

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

#include "lib/hash_func/murmur_hash.h"
#include <stdint.h>

namespace oceanbase {
namespace common {
uint32_t murmurhash2(const void* key, int32_t len, uint32_t seed)
{
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.
  const uint32_t m = 0x5bd1e995;
  const int32_t r = 24;

  // Initialize the hash to a 'random' value
  uint32_t h = seed ^ len;

  // Mix 4 bytes at a time into the hash
  const unsigned char* data = static_cast<const unsigned char*>(key);

  while (len >= 4) {
    uint32_t k =
        ((uint32_t)data[0]) | (((uint32_t)data[1]) << 8) | (((uint32_t)data[2]) << 16) | (((uint32_t)data[3]) << 24);

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // Handle the last few bytes of the input array
  switch (len) {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  };

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.
  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

uint32_t fnv_hash2(const void* key, int32_t len, uint32_t seed)
{
  const int p = 16777619;
  int32_t hash = (int32_t)2166136261L;
  const char* data = static_cast<const char*>(key);
  for (int32_t i = 0; i < len; i++) {
    hash = (hash ^ data[i]) * p;
  }
  hash += hash << 13;
  hash ^= hash >> 7;
  hash += hash << 3;
  hash ^= hash >> 17;
  hash += hash << 5;
  hash ^= seed;
  return (uint32_t)hash;
}
}  // namespace common
}  // namespace oceanbase
