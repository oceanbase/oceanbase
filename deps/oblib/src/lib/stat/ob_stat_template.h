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

#ifndef OB_STAT_TEMPLATE_H_
#define OB_STAT_TEMPLATE_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

class DIRWLock
{
public:
  DIRWLock();
  ~DIRWLock();
  int try_rdlock();
  int try_wrlock();
  void rdlock();
  void wrlock();
  void wr2rdlock();
  void unlock();
  inline uint32_t get_lock() {return lock_;}
private:
  static const uint32_t WRITE_MASK = 1<<30;
  volatile uint32_t lock_;
};
/**
 * ----------------------------------------------------template define---------------------------------------------------------
 */
//NOTE the class T must be a POD type
template<class T, int64_t N>
class ObStatArrayIter
{
public:
  ObStatArrayIter();
  int init(T *item, const int64_t curr_idx, int64_t max_item_idx);
  int get_next(const T *&item);
private:
  T *items_;
  int64_t curr_idx_;
  int64_t max_item_idx_;
};

//NOTE the class T must be a POD type
template<class T, int64_t N>
class ObStatArray
{
public:
  typedef ObStatArrayIter<T, N> Iterator;
public:
  ObStatArray();
  int add(const ObStatArray &other);
  T *get(const int64_t idx);
  int get_iter(Iterator &iter);
  void reset();
private:
  T items_[N];
  int64_t min_item_idx_;
  int64_t max_item_idx_;
};

//NOTE the class T must be a POD type
template<class T, int64_t N>
class ObStatHistoryIter
{
public:
  ObStatHistoryIter();
  int init(T *items, const int64_t start_pos, int64_t item_cnt);
  int get_next(T *&item);
  void reset();
private:
  T *items_;
  int64_t curr_;
  int64_t start_pos_;
  int64_t item_cnt_;
};

//NOTE the class T must be a POD type
template<class T, int64_t N>
class ObStatHistory
{
public:
  typedef ObStatHistoryIter<T, N> Iterator;
public:
  ObStatHistory();
  int push(const T &item);
  int add(const ObStatHistory &other);
  int get_iter(Iterator &iter);
  int get_last(T *&item);
  void reset();
private:
  T items_[N];
  int64_t curr_pos_;
  int64_t item_cnt_;
};

/**
 * ----------------------------------------------------template implementation---------------------------------------------------------
 */
//ObStatArray
template<class T, int64_t N>
ObStatArray<T, N>::ObStatArray()
  : min_item_idx_(N),
    max_item_idx_(-1)
{
  memset(items_, 0, sizeof(items_));
}

template<class T, int64_t N>
int ObStatArray<T, N>::add(const ObStatArray &other)
{
  int ret = common::OB_SUCCESS;
  int64_t i = 0;
  min_item_idx_ = std::min(min_item_idx_, other.min_item_idx_);
  max_item_idx_ = std::max(max_item_idx_, other.max_item_idx_);
  for (i = min_item_idx_; i <= max_item_idx_ && common::OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(items_[i].add(other.items_[i]))) {
      COMMON_LOG(WARN, "Fail to add other, ", K(ret));
    }
  }
  return ret;
}

template<class T, int64_t N>
T *ObStatArray<T, N>::get(const int64_t idx)
{
  T *item = NULL;
  if (idx >= 0 && idx < N) {
    item = &items_[idx];
    if (idx < min_item_idx_) {
      min_item_idx_ = idx;
    }
    if (idx > max_item_idx_) {
      max_item_idx_ = idx;
    }
  }
  return item;
}

template<class T, int64_t N>
int ObStatArray<T, N>::get_iter(Iterator &iter)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(iter.init(items_, min_item_idx_, max_item_idx_))) {
    COMMON_LOG(WARN, "init stat array iter failed, ", K(ret));
  }
  return ret;
}

template<class T, int64_t N>
void ObStatArray<T, N>::reset()
{
  if (min_item_idx_ < N) {
    memset(&(items_[min_item_idx_]), 0, sizeof(T) * (max_item_idx_ - min_item_idx_ + 1));
    min_item_idx_ = N;
    max_item_idx_ = -1;
  }
}


//ObStatArrayIter
template<class T, int64_t N>
ObStatArrayIter<T, N>::ObStatArrayIter()
  : items_(NULL),
    curr_idx_(0),
    max_item_idx_(0)
{
}

template<class T, int64_t N>
int ObStatArrayIter<T, N>::init(T *item, const int64_t curr_idx, int64_t max_item_idx)
{
  int ret = common::OB_SUCCESS;
  if (NULL == item || curr_idx < 0 || max_item_idx < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(item), K(curr_idx), K(max_item_idx), K(ret));
  } else {
    items_ = item;
    curr_idx_ = curr_idx;
    max_item_idx_ = max_item_idx;
  }
  return ret;
}

template<class T, int64_t N>
int ObStatArrayIter<T, N>::get_next(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (NULL == items_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The iter has no been inited, ", K(ret));
  } else {
    while (curr_idx_ <= max_item_idx_ && !items_[curr_idx_].is_valid()) {
      ++curr_idx_;
    }
    if (curr_idx_ > max_item_idx_) {
      ret = OB_ITER_END;
    } else {
      item = &items_[curr_idx_++];
    }
  }
  return ret;
}


//ObStatHistory
template<class T, int64_t N>
ObStatHistory<T, N>::ObStatHistory()
  : curr_pos_(0),
    item_cnt_(0)
{
  memset(items_, 0, sizeof(items_));
}

template<class T, int64_t N>
int ObStatHistory<T, N>::push(const T &item)
{
  int ret = common::OB_SUCCESS;
  items_[curr_pos_] = item;
  if (item_cnt_ < N) {
    ++item_cnt_;
  }
  curr_pos_ = (curr_pos_ + 1) % N;
  return ret;
}


template<class T, int64_t N>
int ObStatHistory<T, N>::add(const ObStatHistory &other)
{
  int ret = common::OB_SUCCESS;
  int64_t i = 0, j = 0, cnt = 0;
  if (other.item_cnt_ > 0) {
    T tmp[N];
    for (i = 0, j = 0; i < item_cnt_ && j < other.item_cnt_ && cnt < N;) {
      if (items_[(curr_pos_ - 1 - i + N) % N] > other.items_[(other.curr_pos_ - 1 - j + N) % N]) {
        tmp[cnt++] = items_[(curr_pos_ - 1 - i + N) % N];
        ++i;
      } else {
        tmp[cnt++] = other.items_[(other.curr_pos_ - 1 - j + N) % N];
        ++j;
      }
    }

    if (cnt < N) {
      for (; i < item_cnt_ && cnt < N; ++i, ++cnt) {
        tmp[cnt] = items_[(curr_pos_ - 1 - i + N) % N];
      }
      for (; j < other.item_cnt_ && cnt < N; ++j, ++cnt) {
        tmp[cnt] = other.items_[(other.curr_pos_ - 1 - j + N) % N];
      }
    }

    for (i = cnt - 1; i >= 0; --i) {
      items_[cnt - i - 1] = tmp[i];
    }
    item_cnt_ = cnt;
    curr_pos_ = cnt % N;
  }
  return ret;
}

template<class T, int64_t N>
int ObStatHistory<T, N>::get_iter(Iterator &iter)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(iter.init(items_, (curr_pos_ - 1 + N) % N, item_cnt_))) {
    COMMON_LOG(WARN, "init stat history iter failed, ", K(ret));
  }
  return ret;
}

template<class T, int64_t N>
int ObStatHistory<T, N>::get_last(T *&item)
{
  int ret = common::OB_SUCCESS;
  if (0 == item_cnt_) {
    ret = common::OB_ITEM_NOT_SETTED;
    COMMON_LOG(WARN, "The item has no been setted, ", K(ret));
  } else {
    item = &items_[(curr_pos_ - 1 + N) % N];
  }
  return ret;
}

template<class T, int64_t N>
void ObStatHistory<T, N>::reset()
{
  if (item_cnt_ > 0) {
    curr_pos_ = 0;
    item_cnt_ = 0;
    memset(items_, 0, sizeof(items_));
  }
}

//ObStatHistoryIter
template<class T, int64_t N>
ObStatHistoryIter<T, N>::ObStatHistoryIter()
  : items_(NULL),
    curr_(0),
    start_pos_(0),
    item_cnt_(0)
{
}


template<class T, int64_t N>
int ObStatHistoryIter<T, N>::init(T *items, const int64_t start_pos, int64_t item_cnt)
{
  int ret = common::OB_SUCCESS;
  if (NULL == items || start_pos < 0 || item_cnt < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(items), K(start_pos), K(item_cnt), K(ret));
  } else {
    items_ = items;
    start_pos_ = start_pos;
    item_cnt_ = item_cnt;
    curr_ = 0;
  }
  return ret;
}

template<class T, int64_t N>
int ObStatHistoryIter<T, N>::get_next(T *&item)
{
  int ret = common::OB_SUCCESS;
  if (curr_ >= item_cnt_) {
    ret = common::OB_ITER_END;
  } else {
    item = &items_[(start_pos_ - curr_ + item_cnt_) % item_cnt_];
    curr_++;
  }
  return ret;
}

template<class T, int64_t N>
void ObStatHistoryIter<T, N>::reset()
{
  items_ = NULL;
  start_pos_ = 0;
  item_cnt_ = 0;
  curr_ = 0;
}

}//common
}//oceanbase

#endif /* OB_STAT_TEMPLATE_H_ */
