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

#ifndef OCEANBASE_LIB_HASH_OB_PLACEMENT_HASH_SET_
#define OCEANBASE_LIB_HASH_OB_PLACEMENT_HASH_SET_

#include "lib/hash/ob_placement_hashutils.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
template <class K, uint64_t N = 1031, bool auto_free = false>
class ObPlacementHashSet
{
public:
  ObPlacementHashSet(): count_(0) {}
  ~ObPlacementHashSet() {}
  /**
   * @retval OB_SUCCESS       success
   * @retval OB_HASH_EXIST    key exist
   * @retval other            errors
   */
  int set_refactored(const K &key);
  /**
   * @retval OB_HASH_EXIST     key exists
   * @retval OB_HASH_NOT_EXIST key does not exist
   * @retval other             errors
   */
  int  exist_refactored(const K &key) const;
  void clear();
  void reset();
  int64_t count() const {return count_;};
protected:
  ObBitSet<N, ModulePageAllocator, auto_free> flags_;
  K keys_[N];
  int64_t count_;
};

template <class K, uint64_t N, bool auto_free>
int ObPlacementHashSet<K, N, auto_free>::set_refactored(const K &key)
{
  int ret = OB_SUCCESS;
  uint64_t pos = 0;
  bool exist = false;
  ret = placement_hash_find_set_pos<K, N, ModulePageAllocator, auto_free>(keys_, flags_, key, 0, pos, exist);
  if (OB_SUCC(ret)) {
    keys_[pos] = key;
    ++count_;
  }
  return ret;
}

template <class K, uint64_t N, bool auto_free>
int ObPlacementHashSet<K, N, auto_free>::exist_refactored(const K &key) const
{
  uint64_t pos = 0;
  int ret = placement_hash_search<K, N, ModulePageAllocator, auto_free>(keys_, flags_, key, pos);
  if (OB_SUCCESS == ret) {
    ret = OB_HASH_EXIST;
  }
  return ret;
}

template <class K, uint64_t N, bool auto_free>
void ObPlacementHashSet<K, N, auto_free>::clear()
{
  flags_.reuse();
  count_ = 0;
}

template <class K, uint64_t N, bool auto_free>
void ObPlacementHashSet<K, N, auto_free>::reset()
{
  flags_.reuse();
  count_ = 0;
}

} // namespace hash
} // namespace common
} // namespace oceanbase

#endif //OCEANBASE_LIB_HASH_OB_PLACEMENT_HASH_SET_
