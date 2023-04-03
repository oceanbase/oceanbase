// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#pragma once

#include "share/table/ob_table_load_shared_allocator.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "common/object/ob_object.h"
#include "common/ob_tablet_id.h"

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
  int deep_copy_and_assign(const T *row, int64_t count,
      const ObTableLoadSharedAllocatorHandle &allocator_handle);
  // for deserialize()
  void set_allocator(const ObTableLoadSharedAllocatorHandle &allocator_handle)
  {
    allocator_handle_ = allocator_handle;
  }
  ObTableLoadSharedAllocatorHandle& get_allocator_handler()
  {
    return allocator_handle_;
  }
  TO_STRING_KV(K_(count));

private:
  static int allocate_cells(T *&cells, int64_t count,
      const ObTableLoadSharedAllocatorHandle &allocator_handle);

public:
  ObTableLoadSharedAllocatorHandle allocator_handle_;
  T *cells_;
  int64_t count_;
};

template<class T>
void ObTableLoadRow<T>::reset()
{
  allocator_handle_.reset();
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
int ObTableLoadRow<T>::deep_copy_and_assign(const T *row, int64_t count,
    const ObTableLoadSharedAllocatorHandle &allocator_handle)
{

  int ret = OB_SUCCESS;
  T *cells = nullptr;

  reset();
  if (OB_FAIL(allocate_cells(cells, count, allocator_handle))) {
    OB_LOG(WARN, "failed to allocate cells", KR(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    if (OB_FAIL(observer::ObTableLoadUtils::deep_copy(row[i],
        cells[i], allocator_handle->get_allocator()))) {
      OB_LOG(WARN, "fail to deep copy object", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    allocator_handle_ = allocator_handle;
    cells_ = cells;
    count_ = count;
  }

  return ret;
}

template<class T>
int ObTableLoadRow<T>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(cells_, count_);
  return ret;
}

template<class T>
int ObTableLoadRow<T>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
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
  OB_UNIS_ADD_LEN_ARRAY(cells_, count_);
  return len;
}

typedef ObTableLoadRow<common::ObObj> ObTableLoadObjRow;
typedef ObTableLoadRow<common::ObString> ObTableLoadStrRow;

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