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

#ifndef OCEANBASE_ENGINE_OB_BIT_VECTOR_H_
#define OCEANBASE_ENGINE_OB_BIT_VECTOR_H_

#include <stdint.h>
#include <limits.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace sql
{

//
// Interpret data as bit vector, the caller's responsibility to make sure memory is enough
// and never out of bound.
//
// Note: all operation is aligned to word bytes (8), see:
//     reset(), is_superset_of(), is_subset_of().
//
// Why not ObBitSet?
// We need a simple enough bit vector to skip rows in vectorized execution.
// The bit vector is var-length and is preallocated before execution (may be allocated in CG),
// the ObBitSet need constructor, too complex for us. (BTW: ObFixedRowSet is fix-length).
struct ObBitVector
{
public:
  const static int64_t BYTES_PER_WORD = sizeof(uint64_t);
  const static int64_t DATA_ALIGN_SIZE = BYTES_PER_WORD;
  const static int64_t WORD_BITS = CHAR_BIT * BYTES_PER_WORD;

  ObBitVector() = default;
  ObBitVector(const ObBitVector&) = delete;
  ObBitVector& operator=(const ObBitVector&) = delete;
  ~ObBitVector() = default;

  inline static int64_t word_count(const int64_t size);
  inline static int64_t memory_size(const int64_t size);
  inline static int64_t byte_count(const int64_t size);

  void init(const int64_t size) { MEMSET(data_, 0, memory_size(size)); }
  void reset(const int64_t size) { init(size); }

  inline bool at(const int64_t idx) const;
  bool contain(const int64_t idx) const { return at(idx); }
  bool exist(const int64_t idx) const { return at(idx); }
  void deep_copy(const ObBitVector &src, const int64_t size)
  {
    MEMCPY(data_, src.data_, byte_count(size));
  }

  inline void set(const int64_t idx);
  inline void unset(const int64_t idx);

  // at(i) |= v;
  inline void bit_or_assign(const int64_t idx, const bool v);

  // bit vector is superset of %other
  inline bool is_superset_of(const ObBitVector &other, const int64_t size) const;
  // bit vector is subset of %other
  inline bool is_subset_of(const ObBitVector &other, const int64_t size) const;

  // check all bits is zero after bit_op(left_bit, right_bit)
  // e.g.:
  //   size: 8
  //   bit_op: l & (~r)
  //   return: 0 == ((l.data_[0] & (~r.data_[0])) & 0xFF)
  template <typename T>
  OB_INLINE static bool bit_op_zero(const ObBitVector &l,
                                    const ObBitVector &r,
                                    const int64_t size,
                                    T bit_op);
  // use param bit_op to calc l op r, assign the result to *this
  // e.g.:
  // bit_op : l & r
  // this->data_ = l.data_ & r.data_
  template <typename T>
  OB_INLINE void bit_calculate(const ObBitVector &l,
                               const ObBitVector &r,
                               const int64_t size,
                               T bit_op);

  OB_INLINE void bit_not(const int64_t size);
  // accumulate bit count, CPU consuming operation, same as:
  //   int64_t bit_cnt = 0;
  //   for (int64_t i = 0; i < size; i++) {
  //     bit_cnt += at(i);
  //   }
  inline int64_t accumulate_bit_cnt(const int64_t size) const;

  inline void set_all(const int64_t size);

  inline bool is_all_true(const int64_t size) const;

  inline bool is_all_false(const int64_t size) const;

  // You should known how ObBitVector implemented, when reinterpret data.
  template <typename T>
  T *reinterpret_data() { return reinterpret_cast<T *>(data_); }
  template <typename T>
  const T *reinterpret_data() const { return reinterpret_cast<const T *>(data_); }

  /**
   * access all valid flipped bit, flip a bit meaning change its value from 0 to 1 or from 1 to 0
   * and it access all 0 bit
   */
  template <typename OP>
  static OB_INLINE int flip_foreach(const ObBitVector &skip, int64_t size, OP op);

  /**
   * access all bit that it's 1
   */
  template <typename OP>
  static OB_INLINE int foreach(const ObBitVector &skip, int64_t size, OP op);
public:
  OB_INLINE static int64_t popcount64(uint64_t v);
private:
  template <bool IS_FLIP, typename OP>
  static OB_INLINE int inner_foreach(const ObBitVector &skip, int64_t size, OP op);
public:
  uint64_t data_[0];
};

inline ObBitVector *to_bit_vector(void *mem)
{
  return static_cast<ObBitVector *>(mem);
}

inline const ObBitVector *to_bit_vector(const void *mem)
{
  return static_cast<const ObBitVector *>(mem);
}

inline int64_t ObBitVector::word_count(const int64_t size)
{
  return (size + WORD_BITS - 1) / WORD_BITS;
}

inline int64_t ObBitVector::byte_count(const int64_t size)
{
  return (size + CHAR_BIT - 1) / CHAR_BIT;
}

inline int64_t ObBitVector::memory_size(const int64_t size)
{
  return word_count(size) * BYTES_PER_WORD;
}

inline bool ObBitVector::at(const int64_t idx) const
{
  OB_ASSERT(idx >= 0);
  // The modern compiler is smart enough to optimize this division to bit operations.
  // Do not bother yourself with that.
  return data_[idx / WORD_BITS] & (1LU << (idx % WORD_BITS));
}

inline void ObBitVector::bit_or_assign(const int64_t idx, const bool v)
{
  OB_ASSERT(idx >= 0);
  data_[idx / WORD_BITS] |= static_cast<uint64_t>(v) << (idx % WORD_BITS);
}

inline void ObBitVector::set(const int64_t idx)
{
  OB_ASSERT(idx >= 0);
  data_[idx / WORD_BITS] |= 1LU << (idx % WORD_BITS);
}

inline void ObBitVector::unset(const int64_t idx)
{
  OB_ASSERT(idx >= 0);
  data_[idx / WORD_BITS] &= ~(1LU << (idx % WORD_BITS));
}

// bit vector is superset of %other
inline bool ObBitVector::is_superset_of(const ObBitVector &other, const int64_t size) const
{
  const int64_t cnt = word_count(size);
  bool is = true;
  for (int64_t i = 0; i < cnt; i++) {
    if ((~data_[i]) & other.data_[i]) {
      is = false;
      break;
    }
  }
  return is;
}

// bit vector is subset of %other
inline bool ObBitVector::is_subset_of(const ObBitVector &other, const int64_t size) const
{
  return other.is_superset_of(*this, size);
}

template <typename T>
OB_INLINE bool ObBitVector::bit_op_zero(const ObBitVector &l,
                                        const ObBitVector &r,
                                        const int64_t size,
                                        T op)
{
  bool passed = true;
  const int64_t cnt = size / WORD_BITS;
  for (int64_t i = 0; i < cnt; i++) {
    if (OB_UNLIKELY(0 != op(l.data_[i], r.data_[i]))) {
      passed = false;
      break;
    }
  }
  if (passed && 0 != size % WORD_BITS) {
    passed = 0 == (op(l.data_[cnt], r.data_[cnt]) & ((1LU << (size % WORD_BITS)) - 1));
  }
  return passed;
}

template <typename T>
OB_INLINE void ObBitVector::bit_calculate(const ObBitVector &l,
                                          const ObBitVector &r,
                                          const int64_t size,
                                          T op)
{
  const int64_t cnt = size / WORD_BITS;
  for (int64_t i = 0; i < cnt; ++i) {
    data_[i] = op(l.data_[i], r.data_[i]);
  }
  if (0 != size % WORD_BITS) {
    uint64_t tmp = data_[cnt];
    data_[cnt] = ((op(l.data_[cnt], r.data_[cnt]) & ((1LU << (size % WORD_BITS)) - 1))
                  | (tmp & ~((1LU << (size % WORD_BITS)) - 1)));
  }
}

OB_INLINE void ObBitVector::bit_not(const int64_t size)
{
  const int64_t cnt = size / WORD_BITS;
  for (int64_t i = 0; i < cnt; ++i) {
    data_[i] = ~(uint64_t)(data_[i]);
  }
  if (0 != size % WORD_BITS) {
    uint64_t tmp = data_[cnt];
    data_[cnt] = (((~(uint64_t)(data_[cnt])) & ((1LU << (size % WORD_BITS)) - 1))
                  | (tmp & ~((1LU << (size % WORD_BITS)) - 1)));
  }
}

OB_INLINE int64_t ObBitVector::popcount64(uint64_t v)
{
  int64_t cnt = 0;
#if __POPCNT__
  cnt = __builtin_popcountl(v);
#else
  if (0 != v) {
    v = v - ((v >> 1) & 0x5555555555555555UL);
    v = (v & 0x3333333333333333UL) + ((v >> 2) & 0x3333333333333333UL);
    cnt = (((v + (v >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56;
  }
#endif
  return cnt;
}

inline int64_t ObBitVector::accumulate_bit_cnt(const int64_t size) const
{
  int64_t bit_cnt = 0;
  const int64_t cnt = size / WORD_BITS;
  const int64_t remain = size % WORD_BITS;
  for (int64_t i = 0; i < cnt; i++) {
    uint64_t v = data_[i];
    bit_cnt += popcount64(v);
  }
  if (remain > 0) {
    bit_cnt += popcount64(data_[cnt] & ((1LU << remain) - 1));
  }
  return bit_cnt;
}

inline void ObBitVector::set_all(const int64_t size)
{
  MEMSET(data_, 0xFF, BYTES_PER_WORD * (size / WORD_BITS));
  if ( 0 != size % WORD_BITS) {
    data_[size / WORD_BITS] |= (1LU << (size % WORD_BITS)) - 1;
  }
}

inline bool ObBitVector::is_all_true(const int64_t size) const
{
  bool is_all_true = true;
  const int64_t cnt = size / WORD_BITS;
  const int64_t remain = size % WORD_BITS;
  for (int64_t i = 0; is_all_true && i < cnt; i++) {
    uint64_t v = data_[i];
    is_all_true = (uint64_t(-1) == v);
  }
  if (is_all_true && remain > 0) {
    uint64_t mask = ((1LU << remain) - 1);
    is_all_true = ((data_[cnt] & mask) == mask);
  }
  return is_all_true;
}

inline bool ObBitVector::is_all_false(const int64_t size) const
{
  bool is_all_false = true;
  const int64_t cnt = size / WORD_BITS;
  const int64_t remain = size % WORD_BITS;
  for (int64_t i = 0; is_all_false && i < cnt; i++) {
    uint64_t v = data_[i];
    is_all_false = (0 == v);
  }
  if (is_all_false && remain > 0) {
    uint64_t mask = ((1LU << remain) - 1);
    is_all_false = ((data_[cnt] & mask) == 0);
  }
  return is_all_false;
}

template <bool IS_FLIP, typename OP>
OB_INLINE int ObBitVector::inner_foreach(const ObBitVector &skip, int64_t size, OP op)
{
  int ret = OB_SUCCESS;
  int64_t tmp_step = 0;
  typedef uint16_t StepType;
  const int64_t step_size = sizeof(StepType) * CHAR_BIT;
  int64_t word_cnt = ObBitVector::word_count(size);
  int64_t step = 0;
  const int64_t remain = size % ObBitVector::WORD_BITS;
  for (int64_t i = 0; i < word_cnt && OB_SUCC(ret); ++i) {
    uint64_t s_word = (IS_FLIP ? ~skip.data_[i] : skip.data_[i]);
    // bool all_bits = (IS_FLIP ? skip.data_[i] == 0 : (~skip.data_[i]) == 0);
    if (i >= word_cnt - 1 && remain > 0) {
      // all_bits = ((IS_FLIP ? skip.data_[i] : ~skip.data_[i]) & ((1LU << remain) - 1)) == 0;
      s_word = s_word & ((1LU << remain) - 1);
    }
    if (s_word > 0) {
      uint64_t tmp_s_word = s_word;
      tmp_step = step;
      do {
        uint16_t step_val = tmp_s_word & 0xFFFF;
        if (0xFFFF == step_val) {
          // no skip
          // last batch ?
          int64_t mini_cnt = step_size;
          if (tmp_step + step_size > size) {
            mini_cnt = size - tmp_step;
          }
          for (int64_t j = 0; j < mini_cnt; j++) {
            int64_t k = j + tmp_step;
            ret = op(k);
          }
        } else if (step_val > 0) {
          do {
            int64_t start_bit_idx = __builtin_ctz(step_val);
            int64_t k = start_bit_idx + tmp_step;
            ret = op(k);
            step_val &= (step_val - 1);
          } while (step_val > 0 && OB_SUCC(ret)); // end for, for one step size
        }
        tmp_step += step_size;
        tmp_s_word >>= step_size;
      } while (tmp_s_word > 0 && OB_SUCC(ret)); // one word-uint64_t
    }
    step += ObBitVector::WORD_BITS;
  } // end for
  return ret;
}

template <typename OP>
OB_INLINE int ObBitVector::flip_foreach(const ObBitVector &skip, int64_t size, OP op)
{
  return ObBitVector::inner_foreach<true, OP>(skip, size, op);
}

template <typename OP>
OB_INLINE int ObBitVector::foreach(const ObBitVector &skip, int64_t size, OP op)
{
  return ObBitVector::inner_foreach<false, OP>(skip, size, op);
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_BIT_VECTOR_H_
