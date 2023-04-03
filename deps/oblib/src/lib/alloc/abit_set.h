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
#include "lib/utility/ob_template_utils.h"
#include "lib/alloc/alloc_assist.h"

namespace oceanbase
{
namespace lib
{

// Thread unsafe, three level BitSet
class ABitSet
{
public:
  ABitSet(int nbits, char *buf);

  static constexpr int32_t n_second_level(int64_t nbits)
  {
    return static_cast<int32_t>(((nbits - 1) >> 6) + 1);
  }

  static constexpr int32_t n_first_level(int64_t nbits)
  {
    return static_cast<int32_t>(((n_second_level(nbits) - 1) >> 6) + 1);
  }

  static constexpr int64_t buf_len(int64_t nbits)
  {
    return n_second_level(nbits) * sizeof(second_level_[0]) +
      n_first_level(nbits) * sizeof(first_level_[0]);
  }

  void clear()
  {
    zero_level_ = 0;
    memset(first_level_, 0, n_first_level_ * sizeof(first_level_[0]));
    memset(second_level_, 0, n_second_level_ * sizeof (second_level_[0]));
  }

  int nbits() const { return nbits_; }

  // find first least significant bit start from start(includ).
  int find_first_significant(int start) const;
  // find first most significant bit start from end(includ).
  int find_first_most_significant(int start) const;

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
# undef isset
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
  static int myffsl(uint64_t v, int pos)
  {
    uint64_t tmp = v & ~((1UL << pos) - 1);
    int ret = __builtin_ctzll(tmp);
    int x = tmp ? ret + 1 : 0;
    int y = ffsl(v & ~((1UL << pos) - 1));
    assert(x == y);
    return x;
  }

  static int myrffsl(uint64_t v, int pos)
  {
    uint64_t tmp = v & ((2UL << pos) - 1);
    int ret = __builtin_clzll(tmp);
    return tmp ? 64 - ret : 0;
  }

private:
  const int nbits_;
  const bool use_zero_level_;
  uint64_t zero_level_;
  uint64_t *const first_level_;
  const int32_t n_first_level_;
  uint64_t *const second_level_;
  const int32_t n_second_level_;
};

template<int nbits>
class ASimpleBitSet
{
#define SHIFT_PER_SEG 6
#define NBITS_PER_SEG (1L<<SHIFT_PER_SEG)
STATIC_ASSERT(0 == (nbits & NBITS_PER_SEG - 1), "check nbits");
public:
  ASimpleBitSet()
  {
    memset(&bs_[0], 0 , sizeof(bs_));
  }
  void set(int idx)
  {
    int seg = idx >> SHIFT_PER_SEG;
    int pos = idx & (NBITS_PER_SEG - 1);
    bs_[seg] |= (1UL << pos);
  }
  void unset(int idx)
  {
    int seg = idx >> SHIFT_PER_SEG;
    int pos = idx & (NBITS_PER_SEG - 1);
    bs_[seg] &= ~(1UL << pos);
  }
  int max_bit_le(int from) const
  {
    int seg = from >> SHIFT_PER_SEG;
    int pos = from & (NBITS_PER_SEG - 1);
    int ret = 0;
    int i = seg;
    int start = pos;
    do {
      ret = max_bit_le(bs_[i], start);
      if (ret != -1) break;
      start = NBITS_PER_SEG - 1;
    } while (--i >= 0);
    return (i << SHIFT_PER_SEG) | ret;
  }
  int min_bit_ge(int from) const
  {
    int seg = from >> SHIFT_PER_SEG;
    int pos = from & (NBITS_PER_SEG - 1);
    int ret = 0;
    int i = seg;
    int start = pos;
    do {
      ret = min_bit_ge(bs_[i], start);
      if (ret != -1) break;
      start = 0;
    } while (++i < sizeof(bs_)/sizeof(bs_[0]));
    return (i << SHIFT_PER_SEG) | ret;
  }
  template<typename OpFunc>
  void combine(const ASimpleBitSet &other, OpFunc &&op)
  {
    int i = 0;
    do {
      bs_[i] = op(bs_[i], other.bs_[i]);
    } while (++i < sizeof(bs_)/sizeof(bs_[0]));
  }
private:
  static int max_bit_le(uint64_t v, int pos)
  {
    uint64_t tmp = v & ((2UL << pos) - 1);
    int ret = __builtin_clzll(tmp);
    return tmp ? NBITS_PER_SEG - ret - 1 : -1;
  }
  static int min_bit_ge(uint64_t v, int pos)
  {
    uint64_t tmp = v & ~((1UL << pos) - 1);
    int ret = __builtin_ctzll(tmp);
    return tmp ? ret : -1;
  }
private:
  uint64_t bs_[nbits/NBITS_PER_SEG];
};

} // end of namespace
} // end of namespace oceanbase


#endif /* _OCEABASE_LIB_ALLOC_BIT_SET_H_ */
