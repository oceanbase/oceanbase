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
  virtual ~ObTabletPointerHandle();

public:
  virtual void reset() override;
  bool is_valid() const;
  int assign(const ObTabletPointerHandle &other);

  TO_STRING_KV("ptr", ObResourceHandle<ObTabletPointer>::ptr_, KP_(map));
private:
  int set(
      ObResourceValueStore<ObTabletPointer> *ptr,
      ObTabletPointerMap *map);

private:
  ObTabletPointerMap *map_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletPointerHandle);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_POINTER_HANDLE_H
