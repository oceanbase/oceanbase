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

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_mutex.h"
namespace oceanbase
{
namespace common
{
template<uint64_t SIZE, bool IS_USE_LOCK = false>
class ObConcurrentBitset
{
private:
  static const uint64_t PER_WORD_BIT_NUM = 64;
  static const uint64_t FULL_WORD_VALUE = UINT64_MAX;
  static const uint16_t WORD_NUM = (SIZE / PER_WORD_BIT_NUM) + 1;
public:
  ObConcurrentBitset() : word_array_(), start_pos_(0), mutex_()
  {
    memset(word_array_, 0, sizeof(word_array_));
  }
  virtual ~ObConcurrentBitset() {};
  int set(uint64_t pos);
  int set_if_not_exist(uint64_t pos);
  int reset(uint64_t pos);
  int test(uint64_t pos, bool &is_exist);
  int find_and_set_first_zero_without_lock(uint64_t &pos);
  int find_and_set_first_zero_with_lock(uint64_t &pos);
  int find_and_set_first_zero(uint64_t &pos);
  int set_start_pos(uint64_t pos);
  uint64_t get_start_pos();
private:
  typedef lib::ObLockGuard<lib::ObMutex> LockGuard;
  uint64_t word_array_[WORD_NUM];
  uint64_t start_pos_; // to record the start of 0bit
  lib::ObMutex mutex_;
};

template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::set(uint64_t pos)
{
  int ret = OB_SUCCESS;
  uint64_t word_idx = pos / PER_WORD_BIT_NUM;
  if (word_idx >= WORD_NUM) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (IS_USE_LOCK) {//implement of lock
      LockGuard lock_guard(mutex_);
      word_array_[word_idx] |= (1ULL << (pos % PER_WORD_BIT_NUM));
    } else {//implement of lockfree
      bool is_succ = false;
      while(OB_SUCC(ret) && !is_succ) {
        uint64_t word = ATOMIC_LOAD(&word_array_[word_idx]);
        uint64_t new_word = word | (1ULL << (pos % PER_WORD_BIT_NUM));
        is_succ = ATOMIC_BCAS(&word_array_[word_idx], word, new_word);
      }
    }
  }
  return ret;
}

template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::set_start_pos(uint64_t pos)
{
  int ret = OB_SUCCESS;
  uint64_t word_idx = pos / PER_WORD_BIT_NUM;
  if (word_idx >= WORD_NUM) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (IS_USE_LOCK) {
      LockGuard lock_guard(mutex_);
      start_pos_ = pos;
    } else {
      ATOMIC_STORE(&start_pos_, pos);
    }
  }
  return ret;
}

template<uint64_t SIZE, bool IS_USE_LOCK >
uint64_t ObConcurrentBitset<SIZE, IS_USE_LOCK>::get_start_pos()
{
  uint64_t start_pos = 0;
  if (IS_USE_LOCK) {
    LockGuard lock_guard(mutex_);
    start_pos = start_pos_;
  } else {
    start_pos = ATOMIC_LOAD(&start_pos_);
  }
  return start_pos;
}

template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::set_if_not_exist(uint64_t pos)
{
  int ret = OB_SUCCESS;
  uint64_t word_idx = pos / PER_WORD_BIT_NUM;
  if (word_idx >= WORD_NUM) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (IS_USE_LOCK) {//implement of lock
      LockGuard lock_guard(mutex_);
      if (word_array_[word_idx] & (1ULL << (pos % PER_WORD_BIT_NUM))) {
        ret = OB_ENTRY_EXIST;
      } else {
        word_array_[word_idx] |= (1ULL << (pos % PER_WORD_BIT_NUM));
      }
    } else {//implement of lockfree
      bool is_succ = false;
      while(OB_SUCC(ret) && !is_succ) {
        uint64_t word = ATOMIC_LOAD(&word_array_[word_idx]);
        if (word & (1ULL << (pos % PER_WORD_BIT_NUM))) {//pos is set now
          ret = OB_ENTRY_EXIST;
        } else {
          uint64_t new_word = word | (1ULL << (pos % PER_WORD_BIT_NUM));
          is_succ = ATOMIC_BCAS(&word_array_[word_idx], word, new_word);
        }
      }
    }
  }
  return ret;
}

template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::reset(uint64_t pos)
{
  int ret = OB_SUCCESS;
  uint64_t word_idx = pos / PER_WORD_BIT_NUM;
  if (word_idx >= WORD_NUM) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (IS_USE_LOCK) {//implement of lock
      LockGuard lock_guard(mutex_);
      word_array_[word_idx] &= ~(1ULL << (pos % PER_WORD_BIT_NUM));
    } else {//implement of lockfree
      bool is_succ = false;
      while(!is_succ) {
        uint64_t word = ATOMIC_LOAD(&word_array_[word_idx]);
        uint64_t new_word = word & ~(1ULL << (pos % PER_WORD_BIT_NUM));
        is_succ = ATOMIC_BCAS(&word_array_[word_idx], word, new_word);
      }
    }
  }
  return ret;
}

template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::test(uint64_t pos, bool &is_exist)
{
  int ret = OB_SUCCESS;
  uint64_t word_idx = pos / PER_WORD_BIT_NUM;
  if (word_idx >= WORD_NUM) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (IS_USE_LOCK) {
      LockGuard lock_guard(mutex_);
      is_exist = static_cast<bool>(word_array_[pos / PER_WORD_BIT_NUM] & (1ULL << (pos % PER_WORD_BIT_NUM )));
    } else {
      is_exist = static_cast<bool>(ATOMIC_LOAD(&word_array_[pos / PER_WORD_BIT_NUM]) & (1ULL << (pos % PER_WORD_BIT_NUM )));
    }
  }
  return ret;
}

//search 0bit from start_pos(include)
template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::find_and_set_first_zero_without_lock(uint64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  if (IS_USE_LOCK) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "this function only used for lock free mod bitset");
  } else {
    bool is_succ = false;
    uint64_t start_pos = ATOMIC_FAA(&start_pos_, 1);
    start_pos %= (SIZE + 1);
    int64_t word_idx = start_pos / PER_WORD_BIT_NUM;
    uint64_t first_zero_idx = 0;
    uint64_t cur_word = 0;
    uint64_t found_word = 0;
    int64_t valid_idx = 0;
    if (word_idx >= WORD_NUM) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      for (int64_t i = 0; i < WORD_NUM && !is_succ; i++, word_idx++) {
        valid_idx = word_idx % WORD_NUM;
        while (((cur_word = ATOMIC_LOAD(&word_array_[valid_idx]) & FULL_WORD_VALUE) != FULL_WORD_VALUE)
               && !is_succ) {
          uint64_t start_pos_in_word = i == 0 ? start_pos % PER_WORD_BIT_NUM : 0;
          found_word = cur_word | ((1ULL << start_pos_in_word) - 1);//set [lowest_bit, start_pos_in_word) = 1
          first_zero_idx = __builtin_ctzl(~found_word);
          uint64_t new_word = cur_word | (1ULL << first_zero_idx);
          is_succ = ATOMIC_BCAS(&word_array_[valid_idx], cur_word ,new_word);
        }
      }

      if (is_succ) {
        pos = first_zero_idx + valid_idx * PER_WORD_BIT_NUM;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

template<uint64_t SIZE, bool IS_USE_LOCK >
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::find_and_set_first_zero_with_lock(uint64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (false == IS_USE_LOCK) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "this function only used for lock mod bitset");
  } else {
    bool is_found = false;
    uint64_t first_zero_idx = 0;
    uint64_t found_word = 0;
    int64_t valid_idx = 0;

    LockGuard lock_guard(mutex_);
    int64_t word_idx = start_pos_ / PER_WORD_BIT_NUM;
    for (int64_t i = 0; i < WORD_NUM + 1 && false == is_found; i++, word_idx++) {
      valid_idx = word_idx % WORD_NUM;
      if ((word_array_[valid_idx] & FULL_WORD_VALUE) != FULL_WORD_VALUE) {
        uint64_t start_pos_in_word = i == 0 ? start_pos_ % PER_WORD_BIT_NUM : 0;
        found_word = word_array_[valid_idx] | ((1ULL << start_pos_in_word) - 1);//set [lowest_bit, start_pos_in_word) = 1
        if ((found_word & FULL_WORD_VALUE) != FULL_WORD_VALUE) {//if start_pos_in_word is highest bit and equal to 1, stop judgement.
          first_zero_idx = __builtin_ctzl(~found_word);
          word_array_[valid_idx] |= (1ULL << first_zero_idx);
          is_found = true;
        }
      }
    }

    if (is_found) {
      pos = first_zero_idx + valid_idx * PER_WORD_BIT_NUM;
      start_pos_ = pos + 1;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

template<uint64_t SIZE, bool IS_USE_LOCK>
int ObConcurrentBitset<SIZE, IS_USE_LOCK>::find_and_set_first_zero(uint64_t &pos)
{
  int ret = OB_SUCCESS;
  if (IS_USE_LOCK) {
    ret = find_and_set_first_zero_with_lock(pos);
  } else {
    ret = find_and_set_first_zero_without_lock(pos);
  }
  return ret;
}

}
}
