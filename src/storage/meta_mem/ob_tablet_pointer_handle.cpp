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

#include "ob_tablet_pointer_handle.h"
#include "storage/meta_mem/ob_tablet_pointer_map.h"

namespace oceanbase
{
namespace storage
{

ObTabletPointerHandle::ObTabletPointerHandle()
  : ObResourceHandle<ObTabletPointer>::ObResourceHandle(),
    map_(nullptr),
    base_pointer_(nullptr),
    base_pointer_alloc_(nullptr)
{
}

ObTabletPointerHandle::ObTabletPointerHandle(ObTabletPointerMap &map)
    : ObResourceHandle<ObTabletPointer>::ObResourceHandle(),
      map_(&map),
      base_pointer_(nullptr),
      base_pointer_alloc_(nullptr)
{
}

ObTabletPointerHandle::ObTabletPointerHandle(
    ObResourceValueStore<ObTabletPointer> *ptr,
    ObTabletPointerMap *map)
    : ObResourceHandle<ObTabletPointer>::ObResourceHandle(),
      map_(map),
      base_pointer_(nullptr),
      base_pointer_alloc_(nullptr)
{
  abort_unless(common::OB_SUCCESS == set(ptr, map));
}

ObTabletPointerHandle::~ObTabletPointerHandle()
{
  reset();
}


ObTabletBasePointer* ObTabletPointerHandle::get_resource_ptr() const
{
  return (base_pointer_ == nullptr) ? static_cast<ObTabletBasePointer *>(ObResourceHandle<ObTabletPointer>::get_resource_ptr())
                                    : static_cast<ObTabletBasePointer *>(base_pointer_->get_value_ptr());
}

ObTabletPointer *ObTabletPointerHandle::get_tablet_pointer() const
{
  return ObResourceHandle<ObTabletPointer>::get_resource_ptr();
}

void ObTabletPointerHandle::reset()
{
  int ret = common::OB_SUCCESS;
  if (nullptr != ObResourceHandle<ObTabletPointer>::ptr_) {
    if (nullptr == map_) {
      STORAGE_LOG(ERROR, "map is null", K(ret), KP_(map));
    } else if (OB_FAIL(map_->dec_handle_ref(ObResourceHandle<ObTabletPointer>::ptr_))) {
      STORAGE_LOG(WARN, "fail to decrease handle reference count", K(ret));
    } else {
      ObResourceHandle<ObTabletPointer>::ptr_ = nullptr;
      base_pointer_ = nullptr;
      base_pointer_alloc_ = nullptr;
    }
  } else if (nullptr != base_pointer_) {
    int64_t ret_cnt = -1;
    if (OB_FAIL(base_pointer_->dec_ref_cnt(ret_cnt))) {
      STORAGE_LOG(WARN, "fail to decrease handle reference count", K(ret));
    } else if (ret_cnt == 0) {
      base_pointer_->get_value_ptr()->~ObSSTabletDummyPointer();
      base_pointer_->~ObResourceValueStore<ObSSTabletDummyPointer>();
      base_pointer_alloc_->free(base_pointer_->get_value_ptr());
    }
    base_pointer_ = nullptr;
    base_pointer_alloc_ = nullptr;
  }
}

bool ObTabletPointerHandle::is_valid() const
{
  // only one is valid;
  return (nullptr != ObResourceHandle<ObTabletPointer>::ptr_
      && nullptr != ObResourceHandle<ObTabletPointer>::ptr_->get_value_ptr()
      && nullptr != map_)
        ^
         (nullptr != base_pointer_
      && nullptr != base_pointer_->get_value_ptr()
      && nullptr != base_pointer_alloc_);
}

int ObTabletPointerHandle::assign(const ObTabletPointerHandle &other)
{
  int ret = common::OB_SUCCESS;
  if (this != &other) {
    if (OB_NOT_NULL(other.base_pointer_)) {
      base_pointer_ = other.base_pointer_;
      base_pointer_alloc_ = other.base_pointer_alloc_;
      base_pointer_->inc_ref_cnt();
    } else {
      if (OB_FAIL(set(other.ptr_, other.map_))) {
        STORAGE_LOG(WARN, "failed to set member", K(ret), K(other));
      }
    }
  }
  return ret;
}

int ObTabletPointerHandle::set(
    ObResourceValueStore<ObTabletPointer> *ptr,
    ObTabletPointerMap *map)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(ptr) || OB_ISNULL(map)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ptr), KP(map));
  } else if (OB_NOT_NULL(base_pointer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "base_pointer should be nullptr", K(ret), KPC(this));
  } else {
    reset();
    if (OB_FAIL(map->inc_handle_ref(ptr))) {
      STORAGE_LOG(WARN, "fail to inc tablet poiner", K(ret), KP(ptr), KP(map));
    } else {
      ObResourceHandle<ObTabletPointer>::ptr_ = ptr;
      map_ = map;
    }
  }
  return ret;
}


} // end namespace storage
} // end namespace oceanbase;
