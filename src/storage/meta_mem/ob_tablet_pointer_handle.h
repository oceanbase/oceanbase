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

#ifndef OCEANBASE_STORAGE_OB_TABLET_POINTER_HANDLE_H
#define OCEANBASE_STORAGE_OB_TABLET_POINTER_HANDLE_H

#include "storage/ob_resource_map.h"

namespace oceanbase
{
namespace storage
{

class ObTabletBasePointer;
class ObSSTabletDummyPointer;
class ObTabletPointer;
class ObTabletPointerMap;

class ObTabletPointerHandle : public ObResourceHandle<ObTabletPointer>
{
public:
  ObTabletPointerHandle();
  explicit ObTabletPointerHandle(ObTabletPointerMap &map);
  ObTabletPointerHandle(
      ObResourceValueStore<ObTabletPointer> *ptr,
      ObTabletPointerMap *map);
  ObTabletPointerHandle(
    ObResourceValueStore<ObSSTabletDummyPointer> *ptr,
    ObIAllocator *alloc):base_pointer_(ptr), base_pointer_alloc_(alloc)
    {
      base_pointer_->inc_ref_cnt();
    }
  virtual ~ObTabletPointerHandle();

public:
  virtual void reset() override;
  bool is_valid() const;
  int assign(const ObTabletPointerHandle &other);
  ObTabletBasePointer *get_resource_ptr() const;
  ObTabletPointer *get_tablet_pointer() const;
  TO_STRING_KV("ptr", ObResourceHandle<ObTabletPointer>::ptr_, KP_(map), KPC_(base_pointer), KP_(base_pointer_alloc));
private:
  int set(
      ObResourceValueStore<ObTabletPointer> *ptr,
      ObTabletPointerMap *map);

private:
  ObTabletPointerMap *map_;
  // Base_pointer_ is designed for SS_Tablet in local;
  // Allocated by task_allocator(base_pointer_alloc_);
  // should be nullptr for local_tablet (in is_valid());
  ObResourceValueStore<ObSSTabletDummyPointer> *base_pointer_;
  ObIAllocator *base_pointer_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletPointerHandle);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_POINTER_HANDLE_H
