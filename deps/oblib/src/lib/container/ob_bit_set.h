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

#ifndef OCEANBASE_LIB_CONTAINER_BITSET_
#define OCEANBASE_LIB_CONTAINER_BITSET_

#include <stdint.h>
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/container/ob_2d_array.h"

namespace oceanbase
{
namespace common
{

template <int64_t N = common::OB_DEFAULT_BITSET_SIZE, typename BlockAllocatorT = ModulePageAllocator>
class ObSegmentBitSet
{
public:
  template <int64_t, typename>
  friend class ObSegmentBitSet;

  typedef uint32_t BitSetWord;

  explicit ObSegmentBitSet(const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_BIT_SET));
  virtual ~ObSegmentBitSet() {}

  ObSegmentBitSet(const ObSegmentBitSet &other);
  template <int64_t O_N, typename O_ALLOC>
  ObSegmentBitSet &operator=(const ObSegmentBitSet<O_N, O_ALLOC> &other)
  {
    if (static_cast<void *>(this) != &other) {
      bitset_word_array_.assign(other.bitset_word_array_);
    }
    return *this;
  }

  BitSetWord get_bitset_word(int64_t index) const
  {
    BitSetWord word = 0;
    if (index < 0 || index >= bitset_word_array_.count()) {
      LIB_LOG(INFO, "bitmap word index exceeds the scope", K(index), K(bitset_word_array_.count()));
      //indeed,as private function, index would never be negative
      //just return 0
    } else {
      word = bitset_word_array_.at(index);
    }
    return word;
  }
  bool operator==(const ObSegmentBitSet &other) const
  {
    bool bool_ret = true;
    int64_t max_bitset_word_count = std::max(bitset_word_count(), other.bitset_word_count());
    for (int64_t i = 0; bool_ret && i < max_bitset_word_count; ++i) {
      if (this->get_bitset_word(i) != other.get_bitset_word(i)) {
        bool_ret = false;
      }
    }
    return bool_ret;
  }
  bool equal(const ObSegmentBitSet &other) const
  {
    return *this == other;
  }
  int prepare_allocate(int64_t bits)
  {
    return bitset_word_array_.prepare_allocate((bits / PER_BITSETWORD_BITS) + 1);
  }

  int reserve(int64_t bits)
  {
    return bitset_word_array_.reserve((bits / PER_BITSETWORD_BITS) + 1);
  }
  int add_member(int64_t index);
  bool has_member(int64_t index) const;
  bool is_empty() const
  {
    return (0 == bitset_word_array_.count());
  }

  void reuse()
  {
    bitset_word_array_.reuse();
  }
  void reset()
  {
    bitset_word_array_.reset();
  }

  int64_t bitset_word_count() const {return bitset_word_array_.count();}
  int64_t bit_count() const {return bitset_word_count() * PER_BITSETWORD_BITS;}

  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return bitset_word_array_.to_string(buf, buf_len);
  }
  void clear_all()
  {
    for (int64_t i = 0; i < bitset_word_array_.count(); i++) {
      bitset_word_array_.at(i) = 0;
    }
  }
  void set_all()
  {
    for (int64_t i = 0; i < bitset_word_array_.count(); i++) {
      bitset_word_array_.at(i) |= MAX_WORD;
    }
  }
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  static const int64_t PER_BITSETWORD_MOD_BITS = 5;
  static const int64_t PER_BITSETWORD_BITS = (1 << PER_BITSETWORD_MOD_BITS);
  static const int64_t PER_BITSETWORD_MASK = PER_BITSETWORD_BITS - 1;
  static const int64_t MAX_BITSETWORD = ((N <= 0 ? common::OB_DEFAULT_BITSET_SIZE : N) - 1) / PER_BITSETWORD_BITS + 1;
  static const int64_t WORD_BITS = 32;
  static const int64_t MAX_WORD = ((int64_t)1 << WORD_BITS) - 1;
  ObSegmentArray<BitSetWord, MAX_BITSETWORD, BlockAllocatorT> bitset_word_array_;
};

template <int64_t N, typename BlockAllocatorT>
ObSegmentBitSet<N, BlockAllocatorT>::ObSegmentBitSet(const BlockAllocatorT &alloc)
  : bitset_word_array_(alloc)
{
}

template <int64_t N, typename BlockAllocatorT>
inline bool ObSegmentBitSet<N, BlockAllocatorT>::has_member(int64_t index) const
{
  bool bool_ret = false;
  if (OB_UNLIKELY(index < 0)) {
    LIB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "negative bitmapset member not allowed", K(index));
    //just return false
  } else if (OB_UNLIKELY(index >= bit_count())) {
    //the bit is not set
  } else {
    bool_ret = ((bitset_word_array_.at(index >> PER_BITSETWORD_MOD_BITS)
                 & ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK))) != 0);
  }
  return bool_ret;
}

//return value: true if succ, false if fail
template <int64_t N, typename BlockAllocatorT>
int ObSegmentBitSet<N, BlockAllocatorT>::add_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "negative bitmapset member not allowed", K(index), K(ret));
    //just return false
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    if (OB_UNLIKELY(pos >= bitset_word_array_.count())) {
      for (int64_t i = bitset_word_array_.count(); OB_SUCC(ret) && i <= pos; ++i) {
        if (OB_FAIL(bitset_word_array_.push_back(0))) {
          LIB_LOG(WARN, "fail to push back element into array", K(index), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      BitSetWord &word = bitset_word_array_.at(pos);
      word |= ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
    }
  }
  return ret;
}

template <int64_t N = common::OB_DEFAULT_BITSET_SIZE>
class ObFixedBitSet
{
  OB_UNIS_VERSION(1);
public:
  typedef uint32_t BitSetWord;

  ObFixedBitSet() { MEMSET((void*)bitset_word_array_, 0, sizeof(bitset_word_array_)); }
  ~ObFixedBitSet() {}
  ObFixedBitSet(const ObFixedBitSet &other) { *this = other; }
  ObFixedBitSet &operator=(const ObFixedBitSet &other);
  inline bool operator==(const ObFixedBitSet &other) const;
  void reset() { MEMSET(bitset_word_array_, 0, sizeof(bitset_word_array_)); }

  inline int64_t bit_count() const { return MAX_BITSETWORD * PER_BITSETWORD_BITS; }
  inline int add_member(int64_t index);
  inline int del_member(int64_t index);
  inline int add_members(const ObFixedBitSet &other);
  inline bool has_member(int64_t index) const;
  int64_t num_members() const;
  bool is_empty() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int find_next(const int64_t start_pos, int64_t &pos);
  int find_prev(const int64_t start_pos, int64_t &pos);
  int find_first(int64_t &pos);
  void clear_all()
  {
    for (int64_t i = 0; i < MAX_BITSETWORD; i++) {
      bitset_word_array_[i] = 0;
    }
  }
private:
  static const int64_t PER_BITSETWORD_MOD_BITS = 5;
  static const int64_t PER_BITSETWORD_BITS = (1 << PER_BITSETWORD_MOD_BITS);
  static const int64_t PER_BITSETWORD_MASK = PER_BITSETWORD_BITS - 1;
  static const int64_t MAX_BITSETWORD = ((N <= 0 ? common::OB_DEFAULT_BITSET_SIZE : N) - 1) / PER_BITSETWORD_BITS + 1;
  BitSetWord bitset_word_array_[MAX_BITSETWORD];
};

template <int64_t N>
ObFixedBitSet<N> &ObFixedBitSet<N>::operator=(const ObFixedBitSet &other)
{
  if (this != &other) {
    MEMCPY((void*)bitset_word_array_, (void*)other.bitset_word_array_, sizeof(bitset_word_array_));
  }
  return *this;
}

template <int64_t N>
int ObFixedBitSet<N>::add_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0) || OB_UNLIKELY(index >= N)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "negative bitmapset member not allowed", K(index));
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    BitSetWord &word = bitset_word_array_[pos];
    word |= ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
  }
  return ret;
}
template <int64_t N>
int ObFixedBitSet<N>::del_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0) || OB_UNLIKELY(index >= N)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "negative bitmapset member not allowed", K(index));
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    BitSetWord &word = bitset_word_array_[pos];
    word &= ~((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
  }
  return ret;
}

template <int64_t N>
inline int ObFixedBitSet<N>::add_members(const ObFixedBitSet &other)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < MAX_BITSETWORD; ++i) {
    bitset_word_array_[i] |= other.bitset_word_array_[i];
  }
  return ret;
}

template <int64_t N>
inline bool ObFixedBitSet<N>::has_member(int64_t index) const
{
  bool bool_ret = false;
  if (OB_UNLIKELY(index < 0)) {
    LIB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "negative bitmapset member not allowed", K(index));
    //just return false
  } else if (OB_UNLIKELY(index >= bit_count())) {
    //the bit is not set
  } else {
    bool_ret = ((bitset_word_array_[index >> PER_BITSETWORD_MOD_BITS]
                 & ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK))) != 0);
  }
  return bool_ret;
}

template <int64_t N>
int64_t ObFixedBitSet<N>::num_members() const
{
  int64_t num = 0;
  BitSetWord word = 0;

  for (int64_t i = 0; i < MAX_BITSETWORD; i ++) {
    word = bitset_word_array_[i];
    if (0 == word) {
      //do nothing
    } else {
      word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
      word = (word & UINT32_C(0x33333333)) + ((word >> 2) & UINT32_C(0x33333333));
      word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 4) & UINT32_C(0x0f0f0f0f));
      word = (word & UINT32_C(0x00ff00ff)) + ((word >> 8) & UINT32_C(0x00ff00ff));
      word = (word & UINT32_C(0x0000ffff)) + ((word >> 16) & UINT32_C(0x0000ffff));
      num += (int64_t)word;
    }
  }
  return num;
}

template <int64_t N>
bool ObFixedBitSet<N>::is_empty() const
{
  bool bret = true;
  BitSetWord word = 0;

  for (int64_t i = 0; i < MAX_BITSETWORD; i ++) {
    word = bitset_word_array_[i];
    if (0 != word) {
      bret = false;
      break;
    }
  }
  return bret;
}

template <int64_t N>
bool ObFixedBitSet<N>::operator==(const ObFixedBitSet &other) const
{
  bool bool_ret = true;
  for (int64_t i = 0; bool_ret && i < MAX_BITSETWORD; ++i) {
    if (bitset_word_array_[i] != other.bitset_word_array_[i]) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

template <int64_t N>
int64_t ObFixedBitSet<N>::to_string(char *buf, const int64_t buf_len) const
{
  common::ObArrayWrap<BitSetWord> array_wrap(bitset_word_array_, MAX_BITSETWORD);
  return array_wrap.to_string(buf, buf_len);
}

template <int64_t N>
int ObFixedBitSet<N>::find_next(const int64_t start_pos, int64_t &find_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(-1 > start_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid start pos", K(ret), K(start_pos));
  } else {
    find_pos = -1;
    int64_t pos = start_pos + 1;
    int64_t which_word = pos >> PER_BITSETWORD_MOD_BITS;
    int64_t which_bit = pos % PER_BITSETWORD_BITS;
    if (OB_UNLIKELY(MAX_BITSETWORD <= which_word)) {
      // find_pos = -1;
    } else {
      BitSetWord this_word = bitset_word_array_[which_word];
      this_word &= (~static_cast<BitSetWord>(0) << which_bit);
      if (static_cast<BitSetWord>(0) != this_word) {
        find_pos = which_word * PER_BITSETWORD_BITS + __builtin_ctz(this_word);
      } else {
        for (++which_word; which_word < MAX_BITSETWORD; ++which_word) {
          this_word = bitset_word_array_[which_word];
          if (static_cast<BitSetWord>(0) != this_word) {
            find_pos = which_word * PER_BITSETWORD_BITS + __builtin_ctz(this_word);
            break;
          }
        }
        // if not found, find_pos = -1
      }
    }
  }
  return ret;
}

//   start_pos = 27     find_pos = 11
//       |                   |
//       v                   v
//  0001 1000 0000 0000 0000 1000 0000 0000
//        \___ delta = 15 __/
//
template <int64_t N>
int ObFixedBitSet<N>::find_prev(const int64_t start_pos, int64_t &find_pos)
{
  int ret = OB_SUCCESS;
  int64_t bitset_word_cnt = MAX_BITSETWORD;
  find_pos = -1;
  if (OB_UNLIKELY(0 > start_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid start pos", K(ret), K(start_pos));
  } else if (OB_UNLIKELY(0 == start_pos)) {
    // find_pos = -1
  } else if (OB_UNLIKELY(0 == bitset_word_cnt)) {
    // find_pos = -1
  } else {
    int64_t delta = 0;
    int64_t pos = start_pos - 1;
    if (pos >= bitset_word_cnt << PER_BITSETWORD_MOD_BITS) {
      pos = (bitset_word_cnt << PER_BITSETWORD_MOD_BITS) - 1;
    }
    int64_t which_word = pos >> PER_BITSETWORD_MOD_BITS;
    int64_t which_bit = pos % PER_BITSETWORD_BITS;
    BitSetWord this_word = bitset_word_array_[which_word];
    this_word = this_word << (PER_BITSETWORD_BITS - which_bit - 1);
    if (static_cast<BitSetWord>(0) != this_word) {
      delta = __builtin_clz(this_word);
    } else {
      delta = which_bit + 1;
      for (--which_word; 0 <= which_word; --which_word) {
        this_word = bitset_word_array_[which_word];
        if (static_cast<BitSetWord>(0) != this_word) {
          delta += __builtin_clz(this_word);
          break;
        } else {
          delta += PER_BITSETWORD_BITS;
        }
      }
    }
    find_pos = pos - delta;
  }
  return ret;
}

template <int64_t N>
int ObFixedBitSet<N>::find_first(int64_t &find_pos)
{
  return find_next(-1, find_pos);
}

OB_SERIALIZE_MEMBER_TEMP(template<int64_t N>, ObFixedBitSet<N>, bitset_word_array_);

template <int64_t N = common::OB_DEFAULT_BITSET_SIZE, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false>
class ObBitSet
{
public:
  template <int64_t, typename, bool>
  friend class ObBitSet;

  typedef uint32_t BitSetWord;

  explicit ObBitSet(const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_BIT_SET));
  virtual ~ObBitSet() {}

  ObBitSet(const ObBitSet &other);
  template <int64_t O_N, typename O_ALLOC, bool O_AUTO_FREE>
  ObBitSet &operator=(const ObBitSet<O_N, O_ALLOC, O_AUTO_FREE> &other);
  template <int64_t O_N, typename O_ALLOC, bool O_AUTO_FREE>
  int assign(const ObBitSet<O_N, O_ALLOC, O_AUTO_FREE> &other);
  bool operator==(const ObBitSet &other) const;
  bool equal(const ObBitSet &other) const;

  void set_attr(const lib::ObMemAttr &attr) { bitset_word_array_.set_attr(attr); }
  int prepare_allocate(int64_t bits)
  {
    return bitset_word_array_.prepare_allocate((bits / PER_BITSETWORD_BITS) + 1);
  }

  int reserve(int64_t bits)
  {
    return bitset_word_array_.reserve((bits / PER_BITSETWORD_BITS) + 1);
  }
  int add_member(int64_t index);
  int add_atomic_member(int64_t index);
  int del_member(int64_t index);
  bool has_member(int64_t index) const;
  bool is_empty() const;
  bool is_subset(const ObBitSet &other) const;
  bool is_superset(const ObBitSet &other) const;
  bool overlap(const ObBitSet &other) const;
  template <int64_t M, typename AnotherAlloc, bool another_auto_free = auto_free>
  int add_members2(const ObBitSet<M, AnotherAlloc, another_auto_free> &other)
  {
    int ret = OB_SUCCESS;
    int64_t this_count = bitset_word_array_.count();
    int64_t other_count = other.bitset_word_count();

    if (this_count < other_count) {
      //make up elements
      for (int64_t i = this_count; OB_SUCC(ret) && i < other_count; ++i) {
        if (OB_FAIL(bitset_word_array_.push_back(0))) {
          LIB_LOG(WARN, "fail to push back element into array", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < other_count; ++i) {
        bitset_word_array_.at(i) |= other.get_bitset_word(i);
      }
    }
    return ret;
  }
  // compare subset relationship between ObBitSet<N> and ObBitSet<M>
  template <int64_t M, typename AnotherAlloc, bool another_auto_free = auto_free>
  bool is_subset2(const ObBitSet<M, AnotherAlloc, another_auto_free> &other) const
  {
    bool bool_ret = true;
    if (is_empty()) {
      bool_ret = true;
    } else if (other.is_empty()) {
      bool_ret = false;
    } else if (bitset_word_array_.count() > other.bitset_word_count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < bitset_word_array_.count(); ++i) {
        if ((get_bitset_word(i) & ~(other.get_bitset_word(i))) != 0) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  // compare superset relationship between ObBitSet<N> and ObBitSet<M>
  template <int64_t M, typename AnotherAlloc, bool another_auto_free = auto_free>
  bool is_superset2(const ObBitSet<M, AnotherAlloc, another_auto_free> &other) const
  {
    bool bool_ret = true;
    if (other.is_empty()) {
      bool_ret =  true;
    } else if (is_empty()) {
      bool_ret = false;
    } else if (bitset_word_array_.count() < other.bitset_word_count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < other.bitset_word_count(); ++i) {
        if ((other.get_bitset_word(i)) & ~(get_bitset_word(i))) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  // compare overlap relationship between ObBitSet<N> and ObBitSet<M>
  template <int64_t M, typename AnotherAlloc, bool another_auto_free = auto_free>
  bool overlap2(const ObBitSet<M, AnotherAlloc, another_auto_free> &other) const
  {
    bool bool_ret = false;
    int64_t min_bitset_word_count = std::min(bitset_word_count(), other.bitset_word_count());
    for (int64_t i = 0; !bool_ret && i < min_bitset_word_count; ++i) {
      if ((get_bitset_word(i) & other.get_bitset_word(i)) != 0) {
        bool_ret = true;
      }
    }
    return bool_ret;
  }

  int add_members(const ObBitSet &other);
  void del_members(const ObBitSet &other); //added by ryan.ly 20150105
  int do_mask(int64_t begin_index, int64_t end_index);
  void reuse();
  void reset();
  int64_t num_members() const;
  int to_array(ObIArray<int64_t> &arr) const;
  int64_t bitset_word_count() const {return bitset_word_array_.count();}
  int64_t bit_count() const {return bitset_word_count() * PER_BITSETWORD_BITS;}
  //return index:-1 if not have one, or return the index of postition of peticulia bit
  int64_t get_first_peculiar_bit(const ObBitSet &other) const;

  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  BitSetWord get_bitset_word(int64_t index) const;
  int find_next(const int64_t start_pos, int64_t &pos);
  int find_prev(const int64_t start_pos, int64_t &pos);
  int find_first(int64_t &pos);
  void clear_all()
  {
    for (int64_t i = 0; i < bitset_word_array_.count(); i++) {
      bitset_word_array_.at(i) = 0;
    }
  }
  void set_all()
  {
    for (int64_t i = 0; i < bitset_word_array_.count(); i++) {
      bitset_word_array_.at(i) |= MAX_WORD;
    }
  }
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  static const int64_t PER_BITSETWORD_MOD_BITS = 5;
  static const int64_t PER_BITSETWORD_BITS = (1 << PER_BITSETWORD_MOD_BITS);
  static const int64_t PER_BITSETWORD_MASK = PER_BITSETWORD_BITS - 1;
  static const int64_t MAX_BITSETWORD = ((N <= 0 ? common::OB_DEFAULT_BITSET_SIZE : N) - 1) / PER_BITSETWORD_BITS + 1;
  static const int64_t WORD_BITS = 32;
  static const int64_t MAX_WORD = ((int64_t)1 << WORD_BITS) - 1;
  ObSEArray<BitSetWord, MAX_BITSETWORD, BlockAllocatorT, auto_free> bitset_word_array_;
};

template <int64_t N, typename BlockAllocatorT, bool auto_free>
ObBitSet<N, BlockAllocatorT, auto_free>::ObBitSet(const BlockAllocatorT &alloc)
  : bitset_word_array_((N + 1) >> PER_BITSETWORD_MOD_BITS, alloc)
{
}

//return value: true if succ, false if fail
template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::add_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "negative bitmapset member not allowed", K(index), K(ret));
    //just return false
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    if (OB_UNLIKELY(pos >= bitset_word_array_.count())) {
      for (int64_t i = bitset_word_array_.count(); OB_SUCC(ret) && i <= pos; ++i) {
        if (OB_FAIL(bitset_word_array_.push_back(0))) {
          LIB_LOG(WARN, "fail to push back element into array", K(index), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      BitSetWord &word = bitset_word_array_.at(pos);
      word |= ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
    }
  }
  return ret;
}

// atomic write member
template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::add_atomic_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "negative bitmapset member not allowed", K(index), K(ret));
    //just return false
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    if (OB_UNLIKELY(pos >= bitset_word_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "not supported extend bitset atomically", K(pos), K(ret));
    } else {
      int64_t tmp_index = index & PER_BITSETWORD_MASK;
      BitSetWord old_word = 0;
      BitSetWord new_word = 0;
      do {
        old_word = bitset_word_array_.at(pos);
        new_word = old_word | ((BitSetWord) 1 << tmp_index);
      } while (ATOMIC_CAS(&bitset_word_array_.at(pos), old_word, new_word) != old_word);
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::del_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "negative bitmapset member not allowed", K(index), K(ret));
    //just return false
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    if (OB_UNLIKELY(pos >= bitset_word_array_.count())) {
    } else {
      BitSetWord &word = bitset_word_array_.at(pos);
      word &= ~((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
typename ObBitSet<N, BlockAllocatorT, auto_free>::BitSetWord ObBitSet<N, BlockAllocatorT, auto_free>::get_bitset_word(
    int64_t index) const
{
  BitSetWord word = 0;
  if (index < 0 || index >= bitset_word_array_.count()) {
    LIB_LOG(INFO, "bitmap word index exceeds the scope", K(index), K(bitset_word_array_.count()));
    //indeed,as private function, index would never be negative
    //just return 0
  } else {
    word = bitset_word_array_.at(index);
  }
  return word;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::add_members(const ObBitSet &other)
{
  int ret = OB_SUCCESS;
  int64_t this_count = bitset_word_array_.count();
  int64_t other_count = other.bitset_word_array_.count();

  if (this_count < other_count) {
    //make up elements
    for (int64_t i = this_count; OB_SUCC(ret) && i < other_count; ++i) {
      if (OB_FAIL(bitset_word_array_.push_back(0))) {
        LIB_LOG(WARN, "fail to push back element into array", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < other_count; ++i) {
      bitset_word_array_.at(i) |= other.get_bitset_word(i);
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
void ObBitSet<N, BlockAllocatorT, auto_free>::del_members(const ObBitSet &other)
{
  for (int64_t i = 0; i < bitset_word_array_.count(); i++) {
    bitset_word_array_.at(i) &= ~other.get_bitset_word(i);
  }
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::do_mask(int64_t begin_index, int64_t end_index)
{
  int64_t max_bit_count = bitset_word_array_.count() * PER_BITSETWORD_BITS;
  int ret = OB_SUCCESS;
  if (begin_index < 0 || begin_index >= max_bit_count || end_index < 0 || end_index >=max_bit_count
      || begin_index >= end_index) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments", K(begin_index), K(end_index), K(max_bit_count), K(ret));
  } else {
    int64_t begin_word = begin_index / PER_BITSETWORD_BITS;
    int64_t end_word = end_index / PER_BITSETWORD_BITS;
    int64_t begin_pos = begin_index % PER_BITSETWORD_BITS;
    int64_t end_pos = end_index % PER_BITSETWORD_BITS;
    // clear words
    for (int64_t i = 0; i < begin_word; i++) {
      bitset_word_array_.at(i) = 0;
    }
    for (int64_t i = bitset_word_array_.count() - 1; i > end_word; i--) {
      bitset_word_array_.at(i) = 0;
    }
    // clear bits
    for (int64_t i = 0; i < begin_pos; ++i) {
      bitset_word_array_.at(begin_word) &= ~((BitSetWord) 1 << i);
    }
    for (int64_t i = 1 + end_pos; i < PER_BITSETWORD_BITS; ++i) {
      bitset_word_array_.at(end_word) &= ~((BitSetWord) 1 << i);
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
inline bool ObBitSet<N, BlockAllocatorT, auto_free>::has_member(int64_t index) const
{
  bool bool_ret = false;
  if (OB_UNLIKELY(index < 0)) {
    LIB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "negative bitmapset member not allowed", K(index));
    //just return false
  } else if (OB_UNLIKELY(index >= bit_count())) {
    //the bit is not set
  } else {
    bool_ret = ((bitset_word_array_.at(index >> PER_BITSETWORD_MOD_BITS)
                 & ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK))) != 0);
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int64_t ObBitSet<N, BlockAllocatorT, auto_free>::to_string(char *buf, const int64_t buf_len) const
{
  return bitset_word_array_.to_string(buf, buf_len);
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
bool ObBitSet<N, BlockAllocatorT, auto_free>::is_empty() const
{
  return (0 == bitset_word_array_.count());
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
bool ObBitSet<N, BlockAllocatorT, auto_free>::is_superset(const ObBitSet &other) const
{
  bool bool_ret = true;
  if (other.is_empty()) {
    bool_ret =  true;
  } else if (is_empty()) {
    bool_ret = false;
  } else if (bitset_word_array_.count() < other.bitset_word_array_.count()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; bool_ret && i < other.bitset_word_array_.count(); ++i) {
      if ((other.get_bitset_word(i)) & ~(get_bitset_word(i))) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
bool ObBitSet<N, BlockAllocatorT, auto_free>::is_subset(const ObBitSet &other) const
{
  bool bool_ret = true;
  if (is_empty()) {
    bool_ret = true;
  } else if (other.is_empty()) {
    bool_ret = false;
  } else if (bitset_word_array_.count() > other.bitset_word_array_.count()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; bool_ret && i < bitset_word_array_.count(); ++i) {
      if ((get_bitset_word(i) & ~(other.get_bitset_word(i))) != 0) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
bool ObBitSet<N, BlockAllocatorT, auto_free>::overlap(const ObBitSet &other) const
{
  bool bool_ret = false;
  int64_t min_bitset_word_count = std::min(bitset_word_count(), other.bitset_word_count());
  for (int64_t i = 0; !bool_ret && i < min_bitset_word_count; ++i) {
    if ((get_bitset_word(i) & other.get_bitset_word(i)) != 0) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
void ObBitSet<N, BlockAllocatorT, auto_free>::reuse()
{
  bitset_word_array_.reuse();
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
void ObBitSet<N, BlockAllocatorT, auto_free>::reset()
{
  bitset_word_array_.reset();
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int64_t ObBitSet<N, BlockAllocatorT, auto_free>::num_members() const
{
  int64_t num = 0;
  BitSetWord word = 0;

  for (int64_t i = 0; i < bitset_word_count(); i ++) {
    word = get_bitset_word(i);
    if (0 == word) {
      //do nothing
    } else {
      word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
      word = (word & UINT32_C(0x33333333)) + ((word >> 2) & UINT32_C(0x33333333));
      word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 4) & UINT32_C(0x0f0f0f0f));
      word = (word & UINT32_C(0x00ff00ff)) + ((word >> 8) & UINT32_C(0x00ff00ff));
      word = (word & UINT32_C(0x0000ffff)) + ((word >> 16) & UINT32_C(0x0000ffff));
      num += (int64_t)word;
    }
  }
  return num;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::to_array(ObIArray<int64_t> &arr) const
{
  int ret = OB_SUCCESS;
  arr.reuse();
  int64_t num = num_members();
  int64_t count = 0;
  int64_t max_bit_count = bitset_word_array_.count() * PER_BITSETWORD_BITS;
  for (int64_t i = 0; OB_SUCC(ret) && count < num && i < max_bit_count; ++i) {
    if (has_member(i)) {
      if (OB_FAIL(arr.push_back(i))) {
        LIB_LOG(WARN, "failed to push back i onto array", K(i), K(ret));
      } else {
        ++count;
      }
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int64_t ObBitSet<N, BlockAllocatorT, auto_free>::get_first_peculiar_bit(const ObBitSet &other) const
{
  int64_t index = -1;
  int64_t min_bit_count = std::min(bit_count(), other.bit_count());
  int64_t i = 0;
  for (i = 0; -1 == index && i < min_bit_count; ++i) {
    if (has_member(i) && !(other.has_member(i))) {
      index = i;
    }
  }
  if (-1 == index && i < bit_count()) {
    //not found upper, keep looking
    for (; -1 == index && i < bit_count(); ++i) {
      if (has_member(i)) {
        index = i;
      }
    }
  }
  return index;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
ObBitSet<N, BlockAllocatorT, auto_free>::ObBitSet(const ObBitSet &other)
{
  *this = other;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free> template <int64_t O_N, typename O_ALLOC, bool O_AUTO_FREE>
ObBitSet<N, BlockAllocatorT, auto_free> &ObBitSet<N, BlockAllocatorT, auto_free>::operator=(const ObBitSet<O_N, O_ALLOC, O_AUTO_FREE> &other)
{
  if (static_cast<void *>(this) != &other) {
    bitset_word_array_.assign(other.bitset_word_array_);
  }
  return *this;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free> template <int64_t O_N, typename O_ALLOC, bool O_AUTO_FREE>
int ObBitSet<N, BlockAllocatorT, auto_free>::assign(const ObBitSet<O_N, O_ALLOC, O_AUTO_FREE> &other)
{
  int ret = OB_SUCCESS;
  if (static_cast<void *>(this) != &other) {
    ret = bitset_word_array_.assign(other.bitset_word_array_);
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
bool ObBitSet<N, BlockAllocatorT, auto_free>::operator==(const ObBitSet &other) const
{
  bool bool_ret = true;
  int64_t max_bitset_word_count = std::max(bitset_word_count(), other.bitset_word_count());
  for (int64_t i = 0; bool_ret && i < max_bitset_word_count; ++i) {
    if (this->get_bitset_word(i) != other.get_bitset_word(i)) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
bool ObBitSet<N, BlockAllocatorT, auto_free>::equal(const ObBitSet &other) const
{
  return *this == other;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ret = bitset_word_array_.serialize(buf, buf_len, pos);
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ret = bitset_word_array_.deserialize(buf, data_len, pos);
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int64_t ObBitSet<N, BlockAllocatorT, auto_free>::get_serialize_size() const
{
  return bitset_word_array_.get_serialize_size();
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::find_next(const int64_t start_pos, int64_t &find_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(-1 > start_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid start pos", K(ret), K(start_pos));
  } else {
    find_pos = -1;
    int64_t pos = start_pos + 1;
    int64_t which_word = pos >> PER_BITSETWORD_MOD_BITS;
    int64_t which_bit = pos % PER_BITSETWORD_BITS;
    if (OB_UNLIKELY(bitset_word_array_.count() <= which_word)) {
      // find_pos = -1;
    } else {
      BitSetWord this_word = bitset_word_array_.at(which_word);
      this_word &= (~static_cast<BitSetWord>(0) << which_bit);
      if (static_cast<BitSetWord>(0) != this_word) {
        find_pos = which_word * PER_BITSETWORD_BITS + __builtin_ctz(this_word);
      } else {
        for (++which_word; which_word < bitset_word_array_.count(); ++which_word) {
          this_word = bitset_word_array_.at(which_word);
          if (static_cast<BitSetWord>(0) != this_word) {
            find_pos = which_word * PER_BITSETWORD_BITS + __builtin_ctz(this_word);
            break;
          }
        }
        // if not found, find_pos = -1
      }
    }
  }
  return ret;
}

//   start_pos = 27     find_pos = 11
//       |                   |
//       v                   v
//  0001 1000 0000 0000 0000 1000 0000 0000
//        \___ delta = 15 __/
//
template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::find_prev(const int64_t start_pos, int64_t &find_pos)
{
  int ret = OB_SUCCESS;
  int64_t bitset_word_cnt = bitset_word_count();
  find_pos = -1;
  if (OB_UNLIKELY(0 > start_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid start pos", K(ret), K(start_pos));
  } else if (OB_UNLIKELY(0 == start_pos)) {
    // find_pos = -1
  } else if (OB_UNLIKELY(0 == bitset_word_cnt)) {
    // find_pos = -1
  } else {
    int64_t delta = 0;
    int64_t pos = start_pos - 1;
    if (pos >= bitset_word_cnt << PER_BITSETWORD_MOD_BITS) {
      pos = (bitset_word_cnt << PER_BITSETWORD_MOD_BITS) - 1;
    }
    int64_t which_word = pos >> PER_BITSETWORD_MOD_BITS;
    int64_t which_bit = pos % PER_BITSETWORD_BITS;
    BitSetWord this_word = bitset_word_array_.at(which_word);
    this_word = this_word << (PER_BITSETWORD_BITS - which_bit - 1);
    if (static_cast<BitSetWord>(0) != this_word) {
      delta = __builtin_clz(this_word);
    } else {
      delta = which_bit + 1;
      for (--which_word; 0 <= which_word; --which_word) {
        this_word = bitset_word_array_.at(which_word);
        if (static_cast<BitSetWord>(0) != this_word) {
          delta += __builtin_clz(this_word);
          break;
        } else {
          delta += PER_BITSETWORD_BITS;
        }
      }
    }
    find_pos = pos - delta;
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT, bool auto_free>
int ObBitSet<N, BlockAllocatorT, auto_free>::find_first(int64_t &find_pos)
{
  return find_next(-1, find_pos);
}


}//end of common
}//end of namespace

#endif // OCEANBASE_LIB_CONTAINER_BITSET_
