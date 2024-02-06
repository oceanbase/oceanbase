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

#pragma once

#include "share/table/ob_table_load_shared_allocator.h"
#include "share/table/ob_table_load_row.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace table
{
template<class T>
class ObTableLoadRowArray
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadRowArray() : allocator_handle_() {}
  ~ObTableLoadRowArray() {}

  int push_back(const T &obj_row);
  int assign(const ObTableLoadRowArray &other);
  void set_allocator(const ObTableLoadSharedAllocatorHandle &allocator_handle)
  {
    allocator_handle_ = allocator_handle;
  }
  T &at(int64_t idx);
  const T &at(int64_t idx) const;

  void reset() {
    array_.reset();
    allocator_handle_.reset();
  }
  bool empty() const {
    return array_.empty();
  }
  int64_t count() const {
    return array_.count();
  }
  TO_STRING_KV(K(array_.count()));

private:
  ObTableLoadSharedAllocatorHandle allocator_handle_;
  common::ObArray<T> array_;
};

template<class T>
int ObTableLoadRowArray<T>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < array_.count(); i ++) {
    OB_UNIS_ENCODE(array_.at(i));
  }
  return ret;
}

template<class T>
int ObTableLoadRowArray<T>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (!allocator_handle_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "allocator handle is null", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < count); ++i) {
      T row;
      row.set_allocator(allocator_handle_);
      OB_UNIS_DECODE(row);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(array_.push_back(row))) {
          OB_LOG(WARN, "fail to push back row to array", K(ret), K(row));
        }
      }
    }
  }
  return ret;
}

template<class T>
int64_t ObTableLoadRowArray<T>::get_serialize_size() const
{
  int len = 0;
  OB_UNIS_ADD_LEN(array_.count());
  for (int64_t i = 0; i < array_.count(); i ++) {
    OB_UNIS_ADD_LEN(array_.at(i));
  }
  return len;
}

template<class T>
int ObTableLoadRowArray<T>::push_back(const T &obj_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(array_.push_back(obj_row))) {
    OB_LOG(WARN, "failed to push back obj_row to array", KR(ret));
  }

  return ret;
}

template<class T>
int ObTableLoadRowArray<T>::assign(const ObTableLoadRowArray<T> &other)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(array_.assign(other.array_))) {
    OB_LOG(WARN, "failed to assign other array", KR(ret));
  } else {
    allocator_handle_ = other.allocator_handle_;
  }

  return ret;
}

template<class T>
T &ObTableLoadRowArray<T>::at(int64_t idx)
{
  return array_.at(idx);
}

template<class T>
const T &ObTableLoadRowArray<T>::at(int64_t idx) const
{
  return array_.at(idx);
}

typedef ObTableLoadRowArray<ObTableLoadObjRow> ObTableLoadObjRowArray;
typedef ObTableLoadRowArray<ObTableLoadTabletObjRow> ObTableLoadTabletObjRowArray;

}  // namespace table
}  // namespace oceanbase
