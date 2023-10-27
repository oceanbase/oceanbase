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
#include "observer/table_load/ob_table_load_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "common/object/ob_object.h"
#include "common/ob_tablet_id.h"
#include "ob_table_load_define.h"

namespace oceanbase
{
namespace table
{
template<class T>
class ObTableLoadRow
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadRow() : cells_(nullptr), count_(0) {}
  virtual ~ObTableLoadRow() {}
  void reset();
  int init(int64_t count, const ObTableLoadSharedAllocatorHandle &allocator_handle);
  int project(const ObIArray<int64_t> &idx_projector, ObTableLoadRow<T> &projected_row) const;
  int deep_copy(const ObTableLoadRow<T> &other, const ObTableLoadSharedAllocatorHandle &allocator_handle);
  // for deserialize()
  void set_allocator(const ObTableLoadSharedAllocatorHandle &allocator_handle)
  {
    allocator_handle_ = allocator_handle;
  }
  ObTableLoadSharedAllocatorHandle& get_allocator_handler()
  {
    return allocator_handle_;
  }
  ObTableLoadSequenceNo& get_sequence_no()
  {
    return seq_no_;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "seq=%lu ", seq_no_.sequence_no_);
    databuff_print_obj_array(buf, buf_len, pos, cells_, count_);
    return pos;
  }

private:
  static int allocate_cells(T *&cells, int64_t count,
      const ObTableLoadSharedAllocatorHandle &allocator_handle);

public:
  ObTableLoadSharedAllocatorHandle allocator_handle_;
  ObTableLoadSequenceNo seq_no_;
  T *cells_;
  int64_t count_;
};

template<class T>
void ObTableLoadRow<T>::reset()
{
  allocator_handle_.reset();
  seq_no_.reset();
  cells_ = nullptr;
  count_ = 0;
}

template<class T>
int ObTableLoadRow<T>::init(int64_t count,
    const ObTableLoadSharedAllocatorHandle &allocator_handle)
{
  int ret = OB_SUCCESS;
  T *cells = nullptr;

  reset();
  if (!allocator_handle) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "allocator is null", KR(ret));
  } else if (OB_FAIL(allocate_cells(cells, count, allocator_handle))) {
    OB_LOG(WARN, "failed to alloate cells", KR(ret), K(count));
  } else {
    allocator_handle_ = allocator_handle;
    cells_ = cells;
    count_ = count;
  }

  return ret;
}

template<class T>
int ObTableLoadRow<T>::allocate_cells(T *&cells, int64_t count,
      const ObTableLoadSharedAllocatorHandle &allocator_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!allocator_handle || (count <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "allocator is null", KR(ret));
  } else {
    cells = (T *)(allocator_handle->alloc(sizeof(T) * count));
    if (OB_ISNULL(cells)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate cells", K(count));
    } else {
      new (cells) T[count];
    }
  }

  return ret;
}

template<class T>
int ObTableLoadRow<T>::deep_copy(const ObTableLoadRow<T> &other,
    const ObTableLoadSharedAllocatorHandle &allocator_handle)
{

  int ret = OB_SUCCESS;
  T *cells = nullptr;

  reset();
  if (OB_FAIL(allocate_cells(cells, other.count_, allocator_handle))) {
    OB_LOG(WARN, "failed to allocate cells", KR(ret), K(other.count_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.count_; i ++) {
    if (OB_FAIL(observer::ObTableLoadUtils::deep_copy(other.cells_[i],
        cells[i], *allocator_handle))) {
      OB_LOG(WARN, "fail to deep copy object", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    allocator_handle_ = allocator_handle;
    seq_no_ = other.seq_no_;
    cells_ = cells;
    count_ = other.count_;
  }
  return ret;
}

template<class T>
int ObTableLoadRow<T>::project(const ObIArray<int64_t> &idx_projector, ObTableLoadRow<T> &projected_row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(projected_row.init(count_, allocator_handle_))) {
    OB_LOG(WARN, "failed to alloate cells", KR(ret), K(projected_row.count_));
  } else {
    for (int64_t j = 0; j < count_; ++j) {
      projected_row.cells_[j] = cells_[idx_projector.at(j)];
    }
    projected_row.seq_no_ = seq_no_;
  }
  return ret;
}

template<class T>
int ObTableLoadRow<T>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(seq_no_);
  OB_UNIS_ENCODE_ARRAY(cells_, count_);
  return ret;
}

template<class T>
int ObTableLoadRow<T>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(seq_no_);
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && (count > 0)) {
    T *cells = nullptr;
    if (OB_FAIL(allocate_cells(cells, count, allocator_handle_))) {
      OB_LOG(WARN, "failed to allocate cells", KR(ret), K(count));
    } else {
      OB_UNIS_DECODE_ARRAY(cells, count);
    }

    if (OB_SUCC(ret)) {
      cells_ = cells;
      count_ = count;
    }
  }

  return ret;
}

template<class T>
int64_t ObTableLoadRow<T>::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(seq_no_);
  OB_UNIS_ADD_LEN_ARRAY(cells_, count_);
  return len;
}

typedef ObTableLoadRow<common::ObObj> ObTableLoadObjRow;

class ObTableLoadTabletObjRow
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadTabletObjRow() {}
  void set_allocator(const ObTableLoadSharedAllocatorHandle &allocator_handle)
  {
    obj_row_.set_allocator(allocator_handle);
  }
  TO_STRING_KV(K_(tablet_id), K_(obj_row));
public:
  common::ObTabletID tablet_id_;
  ObTableLoadObjRow obj_row_;
};

} // namespace table
} // namespace oceanbase
