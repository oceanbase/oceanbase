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

#ifndef OCEANBASE_COMMON_LIMIT_ARRAY_H_
#define OCEANBASE_COMMON_LIMIT_ARRAY_H_
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

#include "lib/oblog/ob_log.h"

#include "lib/container/ob_array_helper.h"

namespace oceanbase
{
namespace common
{
template <class T, int64_t N>
class LimitArray
{
public:
  LimitArray()
      : holder_index_(0), size_(0), index_(0), tail_holer_index_(0)
  {
  }
  bool expand(ObArrayHelper<T> &new_array)
  {
    bool res = true;
    if (new_array.capacity() <= 0 ||
        new_array.count() != 0 ||
        new_array.get_base_address() == NULL) {
      res = false;
    }
    if (res && holder_index_ >= N) {
      res = false;
    }
    if (res) {
      holder[holder_index_++] = new_array;
      size_ += new_array.capacity();
    }
    return res;
  }
  //bool shrink(int64_t size_)

  T *at(const int64_t index_in) const
  {
    int64_t index = index_in;
    T *res = NULL;
    if (index < index_) {
      int64_t array_pos = 0;
      while (res == NULL) {
        if (index < holder[array_pos].capacity()) {
          res = holder[array_pos].at(index);
          break;
        } else {
          index -= holder[array_pos].capacity();
          array_pos++;
        }
        if (index < 0 || array_pos >= N) {
          _OB_LOG(ERROR, "this can never be reached, bugs!!!");
          break;
        }
      }
    }
    return res;
  }

  bool push_back(const T &value)
  {
    bool res = true;
    if (index_ >= size_) {
      res = false;
    }
    if (res && (tail_holer_index_ >= N || tail_holer_index_ < 0)) {
      res = false;
      _OB_LOG(ERROR, "this can never be reached, bugs!!!");
    }
    if (res) {
      while (holder[tail_holer_index_].count() ==
             holder[tail_holer_index_].capacity()) {
        tail_holer_index_++;
      }
      if (tail_holer_index_ >= N || tail_holer_index_ < 0) {
        _OB_LOG(ERROR, "this can never be reached, bugs!!!");
        res = false;
      } else {
        res = holder[tail_holer_index_].push_back(value);
      }
    }
    if (res) { index_++; }
    return res;
  }
  T *pop()
  {
    T *res = NULL;
    if (index_ > 0 && index_ <= size_) {
      if (tail_holer_index_ < 0 || tail_holer_index_ >= N) {
        _OB_LOG(ERROR, "this can never be reached, bugs!!!");
      } else {
        while (holder[tail_holer_index_].count() == 0) {
          tail_holer_index_--;
        }
        if (tail_holer_index_ < 0) {
          _OB_LOG(ERROR, "this can never be reached, bugs!!!");
        } else {
          res = holder[tail_holer_index_].pop();
          index_--;
        }
      }
    }
    return res;
  }
  int64_t get_size() const
  {
    return size_;
  }
  int64_t get_index() const
  {
    return index_;
  }


private:
  ObArrayHelper<T> holder[N];
  int64_t holder_index_;
  int64_t size_;
  int64_t index_;
  int64_t tail_holer_index_;
};

}
}
#endif
