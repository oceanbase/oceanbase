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

#ifndef _OCEABASE_LIB_ALLOC_BIT_SET_H_
#define _OCEABASE_LIB_ALLOC_BIT_SET_H_

#include <stdint.h>
#include <cstring>

#include "lib/utility/ob_macro_utils.h"
#include "lib/alloc/alloc_assist.h"

namespace oceanbase {
namespace lib {

// Thread unsafe, three level BitSet
class ABitSet {
public:
  ABitSet(int nbits, char* buf)
      : nbits_(nbits),
        use_zero_level_(nbits_ >= (1 << 6 << 6)),
        first_level_((uint64_t*)buf),
        n_first_level_(n_first_level(nbits_)),
        second_level_(first_level_ + n_first_level_),
        n_second_level_(n_second_level(nbits_))
  {
    abort_unless(nbits_ > (1 << 6));
    abort_unless(nbits_ <= (1 << 6 << 6 << 6));
    clear();
  }

  static constexpr int32_t n_second_level(int64_t nbits)
  {
    return ((nbits - 1) >> 6) + 1;
  }

  static constexpr int32_t n_first_level(int64_t nbits)
  {
    return ((n_second_level(nbits) - 1) >> 6) + 1;
  }

  static constexpr int64_t buf_len(int64_t nbits)
  {
    return n_second_level(nbits) * sizeof(second_level_[0]) + n_first_level(nbits) * sizeof(first_level_[0]);
  }

  void clear()
  {
    zero_level_ = 0;
    memset(first_level_, 0, n_first_level_ * sizeof(first_level_[0]));
    memset(second_level_, 0, n_second_level_ * sizeof(second_level_[0]));
  }

  // find first least significant bit start from start(includ).
  int find_first_significant(int start) const;

  void set(int idx)
  {
    if (idx >= nbits_) {
      // not allowed
    } else {
      int seg = idx >> 6;
      int pos = idx & ((1 << 6) - 1);
      second_level_[seg] |= 1UL << pos;

      pos = seg & ((1 << 6) - 1);
      seg = seg >> 6;
      first_level_[seg] |= 1UL << pos;

      zero_level_ |= 1UL << seg;
    }
  }

  void unset(int idx)
  {
    if (idx >= nbits_) {
      // not allowed
    } else {
      int seg = idx >> 6;
      int pos = idx & ((1 << 6) - 1);
      second_level_[seg] &= ~(1UL << pos);
      if (0 == second_level_[seg]) {
        pos = seg & ((1 << 6) - 1);
        seg = seg >> 6;
        first_level_[seg] &= ~(1UL << (pos));
        if (use_zero_level_ && 0 == first_level_[seg]) {
          zero_level_ &= ~(1UL << seg);
        }
      }
    }
  }
#ifdef isset
#undef isset
#endif
  bool isset(int idx)
  {
    bool ret = false;
    if (idx >= nbits_) {
      // not allowed
    } else {
      const int seg = idx >> 6;
      const int pos = idx & ((1 << 6) - 1);

      ret = second_level_[seg] & (1UL << pos);
    }
    return ret;
  }

private:
  int myffsl(uint64_t v, int pos) const
  {
    return ffsl(v & ~((1UL << pos) - 1));
  }

private:
  const int nbits_;
  const bool use_zero_level_;
  uint64_t zero_level_;
  uint64_t* const first_level_;
  const int32_t n_first_level_;
  uint64_t* const second_level_;
  const int32_t n_second_level_;
};

}  // namespace lib
}  // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_BIT_SET_H_ */
