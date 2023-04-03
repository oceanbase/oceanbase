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

#ifndef OCEANBASE_COMMON_OB_PLACEMENT_HASHUTILS_
#define OCEANBASE_COMMON_OB_PLACEMENT_HASHUTILS_

#include "lib/hash/ob_hashutils.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
template <class K, uint64_t N, typename BlockAllocatorT, bool auto_free = false>
int placement_hash_search(const K keys[N], const ObBitSet<N, BlockAllocatorT, auto_free> &flags,
    const K &key, uint64_t &pos)
{
  int hash_ret = OB_SUCCESS;

  if (0 == N) {
    hash_ret = OB_HASH_NOT_EXIST;
  } else {
    pos = do_hash(key) % N;
    uint64_t i = 0;
    for (; i < N; i++, pos++) {
      if (pos == N) {
        pos = 0;
      }
      if (!flags.has_member(static_cast<int64_t>(pos))) {
        hash_ret = OB_HASH_NOT_EXIST;
        break;
      } else if (do_equal(keys[pos],  key)) {
        break;
      }
    }
    if (N == i) {
      hash_ret = OB_HASH_NOT_EXIST;
    }
  }
  return hash_ret;
}

  template <class K, uint64_t N, typename BlockAllocatorT, bool auto_free>
int placement_hash_check_pos(const K keys[N], ObBitSet<N, BlockAllocatorT, auto_free> &flags,
    uint64_t pos, const K &key, int flag, bool &exist)
{
  int ret = OB_SUCCESS;
  if (0 == N) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!flags.has_member(static_cast<int64_t>(pos))) {
    if (OB_FAIL(flags.add_member(static_cast<int64_t>(pos)))) {
    } else {
      exist = false;
    }
  } else if (keys[pos] == key) {
    exist = true;
    if (0 == flag) {
      ret = OB_HASH_EXIST;
    }
  } else {
    ret = OB_HASH_PLACEMENT_RETRY;
  }
  return ret;
}

template <class K, uint64_t N, typename BlockAllocatorT, bool auto_free>
int placement_hash_find_set_pos(const K keys[N], ObBitSet<N, BlockAllocatorT, auto_free> &flags,
    const K &key, int flag, uint64_t &pos, bool &exist)
{
  int hash_ret = OB_SUCCESS;
  if (0 == N) {
    hash_ret = OB_ERR_UNEXPECTED;
  } else {
    pos = do_hash(key) % N;
    uint64_t i = 0;
    for (; i < N; i++, pos++) {
      if (pos == N) {
        pos = 0;
      }
      hash_ret = placement_hash_check_pos<K, N, BlockAllocatorT, auto_free>(keys, flags, pos, key, flag, exist);
      if (hash_ret != OB_HASH_PLACEMENT_RETRY) {
        break;
      }
    }
    if (N == i) {
      OB_LOG(DEBUG, "hash buckets are full");
      hash_ret = OB_HASH_FULL;
    }
  }
  return hash_ret;
}
} // end namespace hash
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_PLACEMENT_HASHUTILS_
