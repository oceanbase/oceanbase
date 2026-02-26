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

#ifndef OCEANBASE_STORAGE_OB_FLYING_POINTER_MAP_H_
#define OCEANBASE_STORAGE_OB_FLYING_POINTER_MAP_H_

#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_tablet_pointer_handle.h"
#include "storage/ob_resource_map.h"

namespace oceanbase
{
namespace storage
{
class ObFlyingTabletPointerMap final
{
  friend class ObTenantMetaMemMgr;
  typedef ObTabletPointerHandle* ObInnerTPHandlePtr;
public:
  ObFlyingTabletPointerMap(const int64_t capacity);
  int init(const uint64_t tenant_id);
  int set(const ObDieingTabletMapKey &key, ObTabletPointerHandle &handle);
  int check_exist(const ObDieingTabletMapKey &key, bool &is_exist);
  int erase(const ObDieingTabletMapKey &key);
  int64_t count() const { return map_.size(); }
  void destroy();
private:
  int inner_erase_(const ObDieingTabletMapKey &key);
private:
  bool is_inited_;
  int64_t capacity_;
  common::ObBucketLock bucket_lock_;
  common::hash::ObHashMap<ObDieingTabletMapKey, ObTabletPointerHandle> map_;
  DISALLOW_COPY_AND_ASSIGN(ObFlyingTabletPointerMap);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_FLYING_POINTER_MAP_H_ */
