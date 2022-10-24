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

#ifndef OCEANBASE_HASH_OB_ARRAY_INDEX_HASH_SET_
#define OCEANBASE_HASH_OB_ARRAY_INDEX_HASH_SET_

#include "lib/ob_define.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/container/ob_bit_set.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash_func/ob_hash_func.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
#define ARRAY_INDEX(pos) (index_[pos] - index_base_)

template <typename ItemArray, typename Item, uint64_t N = 1031>
class ObArrayIndexHashSet
{
public:
  typedef uint32_t IndexType;
  static const IndexType INVALID_INDEX = UINT32_MAX;

public:
  ObArrayIndexHashSet()
      : array_(NULL),
        collision_count_(0),
        index_base_(INVALID_INDEX)
  {
  }
  explicit ObArrayIndexHashSet(const ItemArray &array)
      : array_(&array),
        index_base_(INVALID_INDEX)
  {
    reset();
  }

  void init(const ItemArray &array)
  {
    array_ = &array;
  }

  // array[idx] must avaliable before call this func.
  // return OB_SUCCESS for success, OB_ENTRY_EXIST for
  // duplicate entry, or OB_ERROR
  inline int set_index(const uint64_t idx)
  {
    int ret = OB_SUCCESS;
    if (idx >= N) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const Item &item = (*array_)[idx];
      uint64_t pos = anchor_idx(item);
      uint64_t i = 0;

      for (; i < N; ++i) {
        if (OB_LIKELY(!has_member(index_[pos]))) {
          index_[pos] = static_cast<IndexType>(idx + index_base_);
          break;
        } else if (OB_LIKELY((*array_)[ARRAY_INDEX(pos)] == item)) {
          if (OB_UNLIKELY(idx != ARRAY_INDEX(pos))) {
            _OB_LOG(WARN, "idx %ld, exist idx %lu", idx,
                    static_cast<uint64_t>(ARRAY_INDEX(pos)));
            ret = OB_ENTRY_EXIST;
          }
          break;
        } else if (OB_UNLIKELY(N == ++pos)) {
          pos = 0;
        }
        collision_count_++;
      }

      if (OB_UNLIKELY(N == i)) {
        ret = OB_HASH_FULL;
        _OB_LOG(ERROR, "hash bucket are full, N %lu, ret %d", N, ret);
      }
    }
    return ret;
  }

  // return OB_SUCCESS if item existed, or OB_ENTRY_NOT_EXIST
  inline int get_index(const Item &item, uint64_t &idx) const
  {
    int ret = OB_ENTRY_NOT_EXIST;
    uint64_t pos = anchor_idx(item);
    uint64_t i = 0;
    idx = static_cast<uint64_t>(OB_INVALID_INDEX);

    for (; i < N; ++i) {
      if (OB_UNLIKELY(!has_member(index_[pos]))) {
        break;
      } else if (OB_LIKELY((*array_)[ARRAY_INDEX(pos)] == item)) {
        ret = OB_SUCCESS;
        idx = ARRAY_INDEX(pos);
        break;
      } else if (OB_UNLIKELY(N == ++pos)) {
        pos = 0;
      }
    }
    return ret;
  }

  inline uint64_t get_collision_count() const
  {
    return collision_count_;
  }

  inline void reset()
  {
    if (OB_UNLIKELY(index_base_ >= INVALID_INDEX - N)) {
      memset(index_, 0xff, sizeof(IndexType) * N);
      index_base_ = 0;
    } else {
      index_base_ += N;
    }
    collision_count_ = 0;
  }

  inline uint64_t anchor_idx(const Item &item) const
  {
    return do_hash(item) % N;
  }

  inline bool has_member(const IndexType index) const
  {
    return (index >= index_base_ && index < index_base_ + N);
  }

private:
  STATIC_ASSERT(N < UINT32_MAX, "unsupport N more than UINT32_MAX");

  const ItemArray *array_;
  IndexType index_[N];
  uint64_t collision_count_;
  uint64_t index_base_;

  DISALLOW_COPY_AND_ASSIGN(ObArrayIndexHashSet);
};
}; // end namespace hash
}; // end namespace common
}; // end namespace oceanbase
#endif // OCEANBASE_HASH_OB_ARRAY_INDEX_HASH_SET_
