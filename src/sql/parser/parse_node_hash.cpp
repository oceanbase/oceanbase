/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/hash_func/murmur_hash.h"
#include "parse_node_hash.h"

// this is a C wrapper to call murmurhash in C++ definition
uint64_t murmurhash(const void *data, int32_t len, uint64_t hash)
{
  return oceanbase::common::murmurhash(data, len, hash);
}
