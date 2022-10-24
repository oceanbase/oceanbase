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

#ifndef  OCEANBASE_COMMON_HASH_PLACEMENT_HASHMAP_
#define  OCEANBASE_COMMON_HASH_PLACEMENT_HASHMAP_

#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_placement_hashutils.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
template <class K, class V, uint64_t N = 1031, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false>
class ObPlacementHashMap
{
public:
  ObPlacementHashMap()
    : count_(0),
      keys_(reinterpret_cast<KeyArray&>(keys_buf_)),
      values_(reinterpret_cast<ValueArray&>(values_buf_)) {}
  /**
   * put a key value pair into HashMap
   * when flag = 0, do not overwrite existing <key,value> pair
   * when flag != 0 and overwrite_key = 0, overwrite existing value
   * when flag != 0 and overwrite_key != 0, overwrite existing <key,value> pair
   * @retval OB_SUCCESS success
   * @retval OB_HASH_EXIST key exist when flag = 0
   * @retval other errors
   */
  int set_refactored(const K &key, const V &value, int flag = 0, int overwrite_key = 0);
  /**
   * @retval OB_SUCCESS get the corresponding value of key
   * @retval OB_HASH_NOT_EXIST key does not exist
   * @retval other errors
   */
  int get_refactored(const K &key, V &value) const;
  /**
   * @retval value get the corresponding value of key
   * @retval NULL key does not exist
   */
  const V *get(const K &key) const;
  V *get(const K &key);
  void reset();
  int64_t count() const { return count_; }
  int64_t size() const { return count(); }
protected:
  template <typename KK, typename VV, uint64_t NN, bool MM>
  friend class ObIteratableHashMap;
  typedef K KeyArray[N];
  typedef K KType;
  typedef V ValueArray[N];
protected:
  int64_t count_;
  ObBitSet<N, BlockAllocatorT, auto_free> flags_;
  char keys_buf_[sizeof(K) * N];
  char values_buf_[sizeof(V) * N];
  // avoid default constructor for the performance
  KeyArray &keys_;
  ValueArray &values_;
};

template <class K, class V, uint64_t N, typename BlockAllocatorT, bool auto_free>
int ObPlacementHashMap<K, V, N, BlockAllocatorT, auto_free>::set_refactored(const K &key, const V &value,
    int flag/* = 0*/, int overwrite_key/* = 0*/)
{
  int ret = OB_SUCCESS;
  uint64_t pos = 0;
  bool exist = false;
  ret = placement_hash_find_set_pos<K, N, BlockAllocatorT, auto_free>(keys_, flags_, key, flag, pos, exist);
  if (pos >= N) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCC(ret)) {
    if (!exist) {
      K *kptr = &keys_[pos];
      V *vptr = &values_[pos];
      new(kptr) KType(key);
      new(vptr) V(value);
      ++count_;
    } else {
      if (0 == overwrite_key) {
        keys_[pos] = key;
      } else {
        // do nothing
      }
      values_[pos] = value;
    }
  } else {
    // do nothing
  }
  return ret;
}

template <class K, class V, uint64_t N, typename BlockAllocatorT, bool auto_free>
int ObPlacementHashMap<K, V, N, BlockAllocatorT, auto_free>::get_refactored(const K &key, V &value) const
{
  int ret = OB_SUCCESS;
  uint64_t pos = 0;
  ret = placement_hash_search<K, N, BlockAllocatorT, auto_free>(keys_, flags_, key, pos);
  if (OB_SUCC(ret)) {
    if (pos >= N) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = values_[pos];
    }
  } else {
    // do nothing
  }
  return ret;
}

template <class K, class V, uint64_t N, typename BlockAllocatorT, bool auto_free>
const V *ObPlacementHashMap<K, V, N, BlockAllocatorT, auto_free>::get(const K &key) const
{
  const V *ret = NULL;
  uint64_t pos = 0;
  int err = placement_hash_search<K, N, BlockAllocatorT, auto_free>(keys_, flags_, key, pos);
  if (OB_SUCCESS == err) {
    if (pos >= N) {
      // do nothing
    } else {
      ret = &values_[pos];
    }
  } else {
    // do nothing
  }
  return ret;
}

template <class K, class V, uint64_t N, typename BlockAllocatorT, bool auto_free>
V *ObPlacementHashMap<K, V, N, BlockAllocatorT, auto_free>::get(const K &key)
{
  V *ret = NULL;
  uint64_t pos = 0;
  int err = placement_hash_search<K, N, BlockAllocatorT, auto_free>(keys_, flags_, key, pos);
  if (OB_SUCCESS == err) {
    if (pos >= N) {
      // do nothing
    } else {
      ret = &values_[pos];
    }
  } else {
    // do nothing
  }
  return ret;
}

template <class K, class V, uint64_t N, typename BlockAllocatorT, bool auto_free>
void ObPlacementHashMap<K, V, N, BlockAllocatorT, auto_free>::reset()
{
  flags_.reuse();
  count_ = 0;
}

} // namespace hash
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_HASH_PLACEMENT_HASHMAP_
