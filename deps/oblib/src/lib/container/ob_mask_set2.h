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

#ifndef OCEANBASE_COMMON_OB_MASK_SET2_
#define OCEANBASE_COMMON_OB_MASK_SET2_

#include "ob_bit_set.h"
#include "ob_iarray.h"

namespace oceanbase
{
namespace common
{

template<typename T>
class ObMaskSet2
{
public:
  ObMaskSet2() : is_inited_(false), array_(NULL), bitset_() {}
  ~ObMaskSet2() {}
  int init(const ObIArray<T> *array)
  {
    int ret = OB_SUCCESS;
    if (NULL == array) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(bitset_.reserve(array->count()))) {
    } else {
      array_ = array;
      is_inited_ = true;
    }
    return ret;
  }
  void reset()
  {
    is_inited_ = false;
    array_ = NULL;
    bitset_.reuse();
  }
  void clear()
  {
    bitset_.reuse();
  }
public:
  int mask(const T &key)
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else {
      bool hit = false;
      for (int64_t i = 0 ; OB_SUCCESS == ret && i < array_->count(); i++) {
        if (array_->at(i) == key) {
          hit = true;
          if (OB_FAIL(bitset_.add_member(i))) {
          }
          break;
        }
      }
      if (OB_SUCCESS == ret) {
        if (!hit) {
          ret = OB_MASK_SET_NO_NODE;
        }
      }
    }
    return ret;
  }
  int mask(const T &key, bool &is_new_mask)
  {
    int ret = OB_SUCCESS;
    bool tmp_new_mask = false;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else {
      bool hit = false;
      for (int64_t i = 0 ; OB_SUCCESS == ret && i < array_->count(); i++) {
        if (array_->at(i) == key) {
          hit = true;
          if (bitset_.has_member(i)) {
            tmp_new_mask = false;
          } else if (OB_FAIL(bitset_.add_member(i))) {
          } else {
            tmp_new_mask = true;
          }
          break;
        }
      }
      if (OB_SUCCESS == ret) {
        if (!hit) {
          ret = OB_MASK_SET_NO_NODE;
        } else {
          is_new_mask = tmp_new_mask;
        }
      }
    }
    return ret;
  }
  bool is_all_mask() const
  {
    bool bool_ret = false;
    if (is_inited_) {
      bool_ret = (array_->count() == bitset_.num_members());
    }
    return bool_ret;
  }
  int get_not_mask(ObIArray<T> &array) const
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else {
      array.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < array_->count(); ++i) {
        if (!bitset_.has_member(i)) {
          if (OB_FAIL(array.push_back(array_->at(i)))) {
          }
        }
      }
    }
    return ret;
  }
  bool is_mask(const T &key)
  {
    bool bool_ret = false;
    if (is_inited_) {
      for (int64_t i = 0; i < array_->count(); ++i) {
        if (array_->at(i) == key && bitset_.has_member(i)) {
          bool_ret = true;
          break;
        }
      }
    }
    return bool_ret;
  }
private:
  bool is_inited_;
  const ObIArray<T> *array_;
  ObBitSet<> bitset_;
};

} // common
} // oceanbase
#endif // OCEANBASE_COMMON_OB_MASK_SET_
