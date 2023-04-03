// (C) 2010-2016 Alibaba Group Holding Limited.
//
// Authors:
// Normalizer:
#include "lib/hash_func/murmur_hash.h"
#include <stdint.h>

namespace oceanbase
{
namespace common
{
uint32_t murmurhash2(const void *key, int32_t len, uint32_t seed)
{
  // 'multiply' and 'rotate' are not really 'magic'. 
  // They are mixing constants generated offline, and just happen to work well.
  const uint32_t multiply = 0x5bd1e995;
  const int32_t rotate = 24;

  // Initialize the hash to a 'random' value
  uint32_t ret = seed ^ len;

  // Mix four bytes into the hash at a time 
  const unsigned char *data = static_cast<const unsigned char *>(key);

  for(; len >= 4; len -= 4) {
    uint32_t val = ((uint32_t)data[0]) | (((uint32_t)data[1]) << 8)
                 | (((uint32_t)data[2]) << 16) | (((uint32_t)data[3]) << 24);
    val *= multiply;
    val ^= val >> rotate;
    val *= multiply;
    ret *= multiply;
    ret ^= val;
    data += 4;
  }

  // Mix the last few bytes into the hash, ensure the last few bytes are well-incorporated.
  while (len > 0) {
    --len;
    ret ^= data[len] << (8 * len);
    if (len == 0) {
      ret *= multiply;
    }
  }

  ret ^= ret >> 13;
  ret *= multiply;
  ret ^= ret >> 15;

  return ret;
}

uint32_t fnv_hash2(const void *key, int32_t len, uint32_t seed)
{
  const int p = 16777619;
  int32_t hash = (int32_t)2166136261L;
  const char *data = static_cast<const char *>(key);
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
}//namespace common
}//namespace oceanbase
