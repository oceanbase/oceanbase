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
#include "src/sql/ob_eval_bound.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"

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


template<typename WordType>
struct ObBitVectorImpl
{
public:
  const static int64_t BYTES_PER_WORD = sizeof(WordType);
  const static int64_t DATA_ALIGN_SIZE = BYTES_PER_WORD;
  const static int64_t WORD_BITS = CHAR_BIT * BYTES_PER_WORD;
  ObBitVectorImpl() = default;
  ObBitVectorImpl(const ObBitVectorImpl&) = delete;
  ObBitVectorImpl& operator=(const ObBitVectorImpl&) = delete;
  ~ObBitVectorImpl() = default;
  inline static int64_t word_count(const int64_t size);
  inline static int64_t memory_size(const int64_t size);
  inline static int64_t byte_count(const int64_t size);
  // The unit of "size" is bit.
  void init(const int64_t size) { MEMSET(data_, 0, memory_size(size)); }
  void reset(const int64_t size) { init(size); }

  inline bool at(const int64_t idx) const;
  bool contain(const int64_t idx) const { return at(idx); }
  bool exist(const int64_t idx) const { return at(idx); }
  void deep_copy(const ObBitVectorImpl<WordType> &src, const int64_t size)
  {
    MEMCPY(data_, src.data_, byte_count(size));
  }
  inline void set(const int64_t idx);
  inline void unset(const int64_t idx);
    // at(i) |= v;
  inline void bit_or_assign(const int64_t idx, const bool v);

  // bit vector is superset of %other
  inline bool is_superset_of(const ObBitVectorImpl<WordType> &other, const int64_t size) const;
  // bit vector is subset of %other
  inline bool is_subset_of(const ObBitVectorImpl<WordType> &other, const int64_t size) const;

  // check all bits is zero after bit_op(left_bit, right_bit)
  // e.g.:
  //   size: 8
  //   bit_op: l & (~r)
  //   return: 0 == ((l.data_[0] & (~r.data_[0])) & 0xFF)
  template <typename T>
  OB_INLINE static bool bit_op_zero(const ObBitVectorImpl<WordType> &l,
                                    const ObBitVectorImpl<WordType> &r,
                                    const int64_t size,
                                    T bit_op);
  template <typename T>
  OB_INLINE static bool bit_op_zero(const ObBitVectorImpl<WordType> &l,
                                    const ObBitVectorImpl<WordType> &r,
                                    const EvalBound &bound,
                                    T op);
  // use param bit_op to calc l op r, assign the result to *this
  // e.g.:
  // bit_op : l & r
  // this->data_ = l.data_ & r.data_
  template <typename T>
  OB_INLINE void bit_calculate(const ObBitVectorImpl<WordType> &l,
                               const ObBitVectorImpl<WordType> &r,
                               const int64_t size,
                               T bit_op);

  template <typename T>
  OB_INLINE void bit_calculate(const ObBitVectorImpl<WordType> &l,
                               const ObBitVectorImpl<WordType> &r,
                               const EvalBound &bound,
                               T op);

  OB_INLINE void bit_not(const int64_t size);
  // accumulate bit count, CPU consuming operation, same as:
  //   int64_t bit_cnt = 0;
  //   for (int64_t i = 0; i < size; i++) {
  //     bit_cnt += at(i);
  //   }
  OB_INLINE void bit_not(const ObBitVectorImpl<WordType> &other, const int64_t size);
  OB_INLINE void bit_not(const ObBitVectorImpl<WordType> &other, const EvalBound &bound);

  inline int64_t accumulate_bit_cnt(const int64_t size) const;

  inline int64_t accumulate_bit_cnt(const EvalBound &bound) const;

  inline void set_all(const int64_t size);

  inline void set_all(char *data, const int64_t size);

  inline bool is_all_true(const int64_t size) const;

  inline bool is_all_false(const int64_t size) const;

  static inline void get_start_end_mask(const int64_t start_idx, const int64_t end_idx,
                                 WordType &start_mask, WordType &end_mask, int64_t &start_cnt,
                                 int64_t &end_cnt);

  inline void set_all(const int64_t start_idx, const int64_t end_idx);

  inline void unset_all(const int64_t start_idx, const int64_t end_idx);

  inline bool is_all_true(const int64_t start_idx, const int64_t end_idx) const;

  inline void deep_copy(const ObBitVectorImpl<WordType> &src, const int64_t start_idx, const int64_t end_idx);

  inline void bit_or(const ObBitVectorImpl<WordType> &src, const int64_t start_idx, const int64_t end_idx);


  // You should known how ObBitVectorImpl<WordType> implemented, when reinterpret data.
  template <typename T>
  T *reinterpret_data() { return reinterpret_cast<T *>(data_); }
  template <typename T>
  const T *reinterpret_data() const { return reinterpret_cast<const T *>(data_); }

  /**
   * access all valid flipped bit, flip a bit meaning change its value from 0 to 1 or from 1 to 0
   * and it access all 0 bit
   */
  template <typename OP>
  static OB_INLINE int flip_foreach(const ObBitVectorImpl<WordType> &skip, int64_t size, OP op);
  template <typename OP>
  static OB_INLINE int flip_foreach(const ObBitVectorImpl<WordType> &skip, const EvalBound &bound,
                                    OP op);

  /**
   * access all bit that it's 1
   */
  template <typename OP>
  static OB_INLINE int foreach(const ObBitVectorImpl<WordType> &skip, int64_t size, OP op);
  template <typename OP>
  static OB_INLINE int foreach(const ObBitVectorImpl<WordType> &skip, const EvalBound &bound,
                               OP op);
public:
  OB_INLINE static int64_t popcount64(uint64_t v);
private:
  /**
   * the pos in [start_idx, end_idx) will be traversed
   */
  template <bool IS_FLIP, typename OP>
  static OB_INLINE int inner_foreach(const ObBitVectorImpl<WordType> &skip, int64_t start_idx,
                                     int64_t end_idx, OP op);
  template <typename OP>
  static OB_INLINE int inner_foreach_one_word(const WordType &s_word, const int64_t step_size,
                                              int64_t &step, OP op);

public:
  WordType data_[0];
};

using ObBitVector = ObBitVectorImpl<uint64_t>;
using ObTinyBitVector = ObBitVectorImpl<uint8_t>;

inline ObBitVector *to_bit_vector(void *mem)
{
  return static_cast<ObBitVector *>(mem);
}

inline const ObBitVector *to_bit_vector(const void *mem)
{
  return static_cast<const ObBitVector *>(mem);
}

template<typename WordType>
inline int64_t ObBitVectorImpl<WordType>::word_count(const int64_t size)
{
  return (size + WORD_BITS - 1) / WORD_BITS;
}

template<typename WordType>
inline int64_t ObBitVectorImpl<WordType>::byte_count(const int64_t size)
{
  return (size + CHAR_BIT - 1) / CHAR_BIT;
}

template<typename WordType>
inline int64_t ObBitVectorImpl<WordType>::memory_size(const int64_t size)
{
  return word_count(size) * BYTES_PER_WORD;
}

template<typename WordType>
inline bool ObBitVectorImpl<WordType>::at(const int64_t idx) const
{
  OB_ASSERT(idx >= 0);
  // The modern compiler is smart enough to optimize this division to bit operations.
  // Do not bother yourself with that.
  return data_[idx / WORD_BITS] & (1LU << (idx % WORD_BITS));
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::bit_or_assign(const int64_t idx, const bool v)
{
  OB_ASSERT(idx >= 0);
  data_[idx / WORD_BITS] |= static_cast<WordType>(v) << (idx % WORD_BITS);
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::set(const int64_t idx)
{
  OB_ASSERT(idx >= 0);
  data_[idx / WORD_BITS] |= 1LU << (idx % WORD_BITS);
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::unset(const int64_t idx)
{
  OB_ASSERT(idx >= 0);
  data_[idx / WORD_BITS] &= ~(1LU << (idx % WORD_BITS));
}

template<typename WordType>
// bit vector is superset of %other
inline bool ObBitVectorImpl<WordType>::is_superset_of(const ObBitVectorImpl<WordType> &other, const int64_t size) const
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

template<typename WordType>
// bit vector is subset of %other
inline bool ObBitVectorImpl<WordType>::is_subset_of(const ObBitVectorImpl<WordType> &other, const int64_t size) const
{
  return other.is_superset_of(*this, size);
}

template<typename WordType>
template <typename T>
OB_INLINE bool ObBitVectorImpl<WordType>::bit_op_zero(const ObBitVectorImpl<WordType> &l,
                                        const ObBitVectorImpl<WordType> &r,
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

template<typename WordType>
template <typename T>
OB_INLINE bool ObBitVectorImpl<WordType>::bit_op_zero(const ObBitVectorImpl<WordType> &l,
                                        const ObBitVectorImpl<WordType> &r,
                                        const EvalBound &bound,
                                        T op)
{
  bool passed = true;
  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(bound.start(), bound.end(), start_mask, end_mask, start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    WordType only_mask = start_mask & end_mask;
    passed = 0 == (op(l.data_[start_cnt], r.data_[start_cnt]) & only_mask);
  } else {
    passed = 0 == (op(l.data_[start_cnt], r.data_[start_cnt]) & start_mask);
    for (int64_t i = start_cnt + 1; passed && i < end_cnt; i++) {
      passed = 0 == (op(l.data_[i], r.data_[i]));
    }
    if (passed && end_mask > 0) {
      passed = 0 == (op(l.data_[end_cnt], r.data_[end_cnt]) & end_mask);
    }
  }
  return passed;
}

template<typename WordType>
template <typename T>
OB_INLINE void ObBitVectorImpl<WordType>::bit_calculate(const ObBitVectorImpl<WordType> &l,
                                          const ObBitVectorImpl<WordType> &r,
                                          const int64_t size,
                                          T op)
{
  const int64_t cnt = size / WORD_BITS;
  for (int64_t i = 0; i < cnt; ++i) {
    data_[i] = op(l.data_[i], r.data_[i]);
  }
  if (0 != size % WORD_BITS) {
    WordType tmp = data_[cnt];
    data_[cnt] = ((op(l.data_[cnt], r.data_[cnt]) & ((1LU << (size % WORD_BITS)) - 1))
                  | (tmp & ~((1LU << (size % WORD_BITS)) - 1)));
  }
}

template<typename WordType>
template <typename T>
OB_INLINE void ObBitVectorImpl<WordType>::bit_calculate(const ObBitVectorImpl<WordType> &l,
                                          const ObBitVectorImpl<WordType> &r,
                                          const EvalBound &bound,
                                          T op)
{
  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(bound.start(), bound.end(), start_mask, end_mask, start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    WordType only_mask = start_mask & end_mask;
    WordType only_bits = op(l.data_[start_cnt], r.data_[start_cnt]) & only_mask;
    data_[start_cnt] = (data_[start_cnt] & ~only_mask) | only_bits;
  } else {
    WordType start_bits = op(l.data_[start_cnt], r.data_[start_cnt]) & start_mask;
    data_[start_cnt] = (data_[start_cnt] & ~start_mask) | start_bits;
    for (int64_t i = start_cnt + 1; i < end_cnt; i++) {
      data_[i] = op(l.data_[i], r.data_[i]);
    }
    if (end_mask > 0) {
      WordType end_bits = op(l.data_[end_cnt], r.data_[end_cnt]) & end_mask;
      data_[end_cnt] = (data_[end_cnt] & ~end_mask) | end_bits;
    }
  }
}

template<typename WordType>
OB_INLINE void ObBitVectorImpl<WordType>::bit_not(const int64_t size)
{
  const int64_t cnt = size / WORD_BITS;
  for (int64_t i = 0; i < cnt; ++i) {
    data_[i] = ~(WordType)(data_[i]);
  }
  if (0 != size % WORD_BITS) {
    WordType tmp = data_[cnt];
    data_[cnt] = (((~(WordType)(data_[cnt])) & ((1LU << (size % WORD_BITS)) - 1))
                  | (tmp & ~((1LU << (size % WORD_BITS)) - 1)));
  }
}

template<typename WordType>
OB_INLINE void ObBitVectorImpl<WordType>::bit_not(const ObBitVectorImpl<WordType> &other, const int64_t size)
{
  const int64_t cnt = size / WORD_BITS;
  for (int64_t i = 0; i < cnt; ++i) {
    data_[i] = ~(WordType)(other.data_[i]);
  }
  if (0 != size % WORD_BITS) {
    WordType tmp = data_[cnt];
    data_[cnt] = (((~(WordType)(other.data_[cnt])) & ((1LU << (size % WORD_BITS)) - 1))
                  | (tmp & ~((1LU << (size % WORD_BITS)) - 1)));
  }
}

template<typename WordType>
OB_INLINE void ObBitVectorImpl<WordType>::bit_not(const ObBitVectorImpl<WordType> &other, const EvalBound &bound)
{
  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(bound.start(), bound.end(), start_mask, end_mask, start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    WordType only_mask = start_mask & end_mask;
    WordType only_bits = (~other.data_[start_cnt]) & only_mask;
    data_[start_cnt] = (data_[start_cnt] & ~only_mask) | only_bits;
  } else {
    WordType start_bits = (~other.data_[start_cnt]) & start_mask;
    data_[start_cnt] = (data_[start_cnt] & ~start_mask) | start_bits;
    for (int64_t i = start_cnt + 1; i < end_cnt; i++) {
      data_[i] = (~other.data_[i]);
    }
    if (end_mask > 0) {
      WordType end_bits = (~other.data_[end_cnt]) & end_mask;
      data_[end_cnt] = (data_[end_cnt] & ~end_mask) | end_bits;
    }
  }
}

template<typename WordType>
OB_INLINE int64_t ObBitVectorImpl<WordType>::popcount64(uint64_t v)
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

template<typename WordType>
inline int64_t ObBitVectorImpl<WordType>::accumulate_bit_cnt(const int64_t size) const
{
  int64_t bit_cnt = 0;
  const int64_t cnt = size / WORD_BITS;
  const int64_t remain = size % WORD_BITS;
  for (int64_t i = 0; i < cnt; i++) {
    WordType v = data_[i];
    bit_cnt += popcount64(v);
  }
  if (remain > 0) {
    bit_cnt += popcount64(data_[cnt] & ((1LU << remain) - 1));
  }
  return bit_cnt;
}

template<typename WordType>
inline int64_t ObBitVectorImpl<WordType>::accumulate_bit_cnt(const EvalBound &bound) const
{
  int64_t bit_cnt = 0;
  const int64_t start = bound.start() / WORD_BITS;
  const int64_t end = bound.end() / WORD_BITS;
  for (int64_t i = start; i < end; ++i) {
    WordType v = data_[i];
    bit_cnt += popcount64(v);
  }
  const int64_t front = bound.start() % WORD_BITS;
  if (front > 0) {
    bit_cnt -= popcount64(data_[start] & ((1LU << front) - 1));
  }
  const int64_t back = bound.end() % WORD_BITS;
  if (back > 0) {
    bit_cnt += popcount64(data_[end] & ((1LU << back) - 1));
  }
  return bit_cnt;
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::set_all(const int64_t size)
{
  MEMSET(data_, 0xFF, BYTES_PER_WORD * (size / WORD_BITS));
  if ( 0 != size % WORD_BITS) {
    data_[size / WORD_BITS] |= (1LU << (size % WORD_BITS)) - 1;
  }
}

template<typename WordType>
inline bool ObBitVectorImpl<WordType>::is_all_true(const int64_t size) const
{
  bool is_all_true = true;
  const int64_t cnt = size / WORD_BITS;
  const int64_t remain = size % WORD_BITS;
  for (int64_t i = 0; is_all_true && i < cnt; i++) {
    WordType v = data_[i];
    is_all_true = (WordType(-1) == v);
  }
  if (is_all_true && remain > 0) {
    WordType mask = ((1LU << remain) - 1);
    is_all_true = ((data_[cnt] & mask) == mask);
  }
  return is_all_true;
}

template<typename WordType>
inline bool ObBitVectorImpl<WordType>::is_all_false(const int64_t size) const
{
  bool is_all_false = true;
  const int64_t cnt = size / WORD_BITS;
  const int64_t remain = size % WORD_BITS;
  for (int64_t i = 0; is_all_false && i < cnt; i++) {
    WordType v = data_[i];
    is_all_false = (0 == v);
  }
  if (is_all_false && remain > 0) {
    WordType mask = ((1LU << remain) - 1);
    is_all_false = ((data_[cnt] & mask) == 0);
  }
  return is_all_false;
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::get_start_end_mask(const int64_t start_idx, const int64_t end_idx,
                                            WordType &start_mask, WordType &end_mask,
                                            int64_t &start_cnt, int64_t &end_cnt)
{
  start_cnt = start_idx / WORD_BITS;
  const int64_t start_remain = start_idx % WORD_BITS;
  end_cnt = end_idx / WORD_BITS;
  const int64_t end_remain = end_idx % WORD_BITS;
  start_mask = WordType(-1) << start_remain;
  end_mask = (1LU << end_remain) - 1;
}

template<typename WordType>
inline bool ObBitVectorImpl<WordType>::is_all_true(const int64_t start_idx, const int64_t end_idx) const
{
  bool is_all_true = true;

  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(start_idx, end_idx, start_mask, end_mask,
                                                      start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    WordType one_word_mask = start_mask & end_mask;
    is_all_true = ((data_[start_cnt] & one_word_mask) == one_word_mask);
  } else {
    is_all_true = ((data_[start_cnt] & start_mask) == start_mask);
    for (int64_t i = start_cnt + 1; is_all_true && i < end_cnt; i++) {
      is_all_true = (WordType(-1) == data_[i]);
    }
    if (end_mask > 0) {
      is_all_true &= ((data_[end_cnt] & end_mask) == end_mask);
    }
  }
  return is_all_true;
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::set_all(const int64_t start_idx, const int64_t end_idx)
{
  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(start_idx, end_idx, start_mask, end_mask, start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    data_[start_cnt] |= start_mask & end_mask;
  } else {
    data_[start_cnt] |= start_mask;
    MEMSET(data_ + start_cnt + 1, 0xFF, (end_cnt - start_cnt - 1) * BYTES_PER_WORD);
    if (end_mask > 0) {
      data_[end_cnt] |= end_mask;
    }
  }
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::unset_all(const int64_t start_idx, const int64_t end_idx)
{
  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(start_idx, end_idx, start_mask, end_mask, start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    data_[start_cnt] &= (~(start_mask & end_mask));
  } else {
    data_[start_cnt] &= (~start_mask);
    MEMSET(data_ + start_cnt + 1, 0x0, (end_cnt - start_cnt - 1) * BYTES_PER_WORD);
    if (end_mask > 0) {
      data_[end_cnt] &= (~end_mask);
    }
  }
}


template<typename WordType>
inline void ObBitVectorImpl<WordType>::deep_copy(const ObBitVectorImpl<WordType> &src, const int64_t start_idx,
                                const int64_t end_idx)
{
  const WordType *src_data = src.data_;

  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(start_idx, end_idx, start_mask, end_mask, start_cnt, end_cnt);

  // Can be changed to a simple implementation, which is faster but not precise enough:
  // MEMCPY(data_ + start_cnt, src.data_ + start_cnt, byte_count(end_idx - start_end));
  if (start_cnt == end_cnt) {
    WordType one_word_mask = start_mask & end_mask;
    data_[start_cnt] = (data_[start_cnt] & ~one_word_mask) | (src_data[start_cnt] & one_word_mask);
  } else {
    data_[start_cnt] = (data_[start_cnt] & ~start_mask) | (src_data[start_cnt] & start_mask);
    MEMCPY(data_ + start_cnt + 1, src_data + start_cnt + 1, (end_cnt - start_cnt - 1) * BYTES_PER_WORD);
    if (end_mask > 0) {
      data_[end_cnt] = (data_[end_cnt] & ~end_mask) | (src_data[end_cnt] & end_mask);
    }
  }
}

template<typename WordType>
inline void ObBitVectorImpl<WordType>::bit_or(const ObBitVectorImpl<WordType> &src, const int64_t start_idx,
                                const int64_t end_idx)
{
  const WordType *src_data = src.data_;

  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(start_idx, end_idx, start_mask, end_mask, start_cnt, end_cnt);

  if (start_cnt == end_cnt) {
    data_[start_cnt] |= src_data[start_cnt] & (start_mask & end_mask);
  } else {
    data_[start_cnt] |= src_data[start_cnt] & start_mask;
    for (int64_t i = start_cnt + 1; i < end_cnt; i++) {
      data_[i] |= src_data[i];
    }
    if (end_mask > 0) {
      data_[end_cnt] |= src_data[end_cnt] & end_mask;
    }
  }
}

template <typename WordType>
template <typename OP>
OB_INLINE int ObBitVectorImpl<WordType>::inner_foreach_one_word(const WordType &s_word,
                                                                const int64_t step_size,
                                                                int64_t &step, OP op)
{
  int ret = OB_SUCCESS;
  if (s_word > 0) {
    WordType tmp_s_word = s_word;
    int64_t tmp_step = step;
    do {
      uint16_t step_val = tmp_s_word & 0xFFFF;
      if (0xFFFF == step_val) {
        for (int64_t j = 0; OB_SUCC(ret) && j < step_size; j++) {
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
  step += WORD_BITS;
  return ret;
}

template <typename WordType>
template <bool IS_FLIP, typename OP>
OB_INLINE int ObBitVectorImpl<WordType>::inner_foreach(const ObBitVectorImpl<WordType> &skip,
                                                       int64_t start_idx, int64_t end_idx, OP op)
{
  int ret = OB_SUCCESS;
  int64_t tmp_step = 0;
  typedef uint16_t StepType;
  const int64_t step_size = sizeof(StepType) * CHAR_BIT;

  int64_t start_cnt = 0;
  int64_t end_cnt = 0;
  WordType start_mask = 0;
  WordType end_mask = 0;
  get_start_end_mask(start_idx, end_idx, start_mask, end_mask, start_cnt, end_cnt);
  // eg. start_remain = 5, start_mask = 11111....11100000
  //                                                |   |
  //                                                 \ /
  //                                         nums of '0' == start_remain

  // eg. end_remain = 5, end_mask = 00000000....11111
  //                                            |   |
  //                                             \ /
  //                                     nums of '1' == end_remain
  int64_t step = WORD_BITS * start_cnt; // the bit pos offset of the first word
  if (start_cnt == end_cnt) {
    // if only one word, both start_mask and end_mask should be used
    WordType one_word_mask = start_mask & end_mask;
    WordType s_word = (IS_FLIP ? ~skip.data_[start_cnt] : skip.data_[start_cnt]);
    s_word = s_word & one_word_mask;
    ret = inner_foreach_one_word(s_word, step_size, step, op);
  } else {
    // process first word, which may not a complete word
    WordType s_word = (IS_FLIP ? ~skip.data_[start_cnt] : skip.data_[start_cnt]);
    if (start_mask > 0) {
      s_word = s_word & start_mask;
    }
    // process words in the middle, all of these are whole word
    if (OB_FAIL(inner_foreach_one_word(s_word, step_size, step, op))) {
    } else {
      for (int64_t i = start_cnt + 1; i < end_cnt && OB_SUCC(ret); ++i) {
        WordType s_word = (IS_FLIP ? ~skip.data_[i] : skip.data_[i]);
        ret = inner_foreach_one_word(s_word, step_size, step, op);
      }
    }
    if (OB_SUCC(ret)) {
      // if end_mask > 0, means there is a incomplete word in the last
      if (end_mask > 0) {
        WordType s_word = (IS_FLIP ? ~skip.data_[end_cnt] : skip.data_[end_cnt]);
        s_word = s_word & end_mask;
        ret = inner_foreach_one_word(s_word, step_size, step, op);
      }
    }
  }
  return ret;
}

template<typename WordType>
template <typename OP>
OB_INLINE int ObBitVectorImpl<WordType>::flip_foreach(const ObBitVectorImpl<WordType> &skip, int64_t size, OP op)
{
  return ObBitVectorImpl<WordType>::inner_foreach<true, OP>(skip, 0 /*start_idx*/, size, op);
}

template<typename WordType>
template <typename OP>
OB_INLINE int ObBitVectorImpl<WordType>::foreach(const ObBitVectorImpl<WordType> &skip, int64_t size, OP op)
{
  return ObBitVectorImpl<WordType>::inner_foreach<false, OP>(skip, 0 /*start_idx*/, size, op);
}

template <typename WordType>
template <typename OP>
OB_INLINE int ObBitVectorImpl<WordType>::flip_foreach(const ObBitVectorImpl<WordType> &skip,
                                                      const EvalBound &bound, OP op)
{
  return ObBitVectorImpl<WordType>::inner_foreach<true, OP>(skip, bound.start(), bound.end(), op);
}

template <typename WordType>
template <typename OP>
OB_INLINE int ObBitVectorImpl<WordType>::foreach (const ObBitVectorImpl<WordType> &skip,
                                                  const EvalBound &bound, OP op)
{
  return ObBitVectorImpl<WordType>::inner_foreach<false, OP>(skip, bound.start(), bound.end(), op);
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_BIT_VECTOR_H_
