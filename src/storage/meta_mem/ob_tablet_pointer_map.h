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

#ifndef OCEANBASE_STORAGE_OB_TABLET_POINTER_MAP_H_
#define OCEANBASE_STORAGE_OB_TABLET_POINTER_MAP_H_

#include "lib/allocator/page_arena.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_tablet_pointer_handle.h"
#include "storage/meta_mem/ob_update_tablet_pointer_param.h"
#include "storage/ob_resource_map.h"

namespace oceanbase
{
namespace storage
{

class ObTabletPointerMap : public ObResourceMap<ObTabletMapKey, ObTabletPointer>
{
public:
  typedef ObResourceMap<ObTabletMapKey, ObTabletPointer> ResourceMap;
  ObTabletPointerMap();
  int set(const ObTabletMapKey &key, ObTabletPointer &ptr);  // overwrite
  int erase(const ObTabletMapKey &key, ObTabletHandle &guard);
  int exist(const ObTabletMapKey &key, bool &is_exist);
  int get_meta_obj(const ObTabletMapKey &key, ObTabletHandle &guard);
  int get_meta_obj_with_filter(const ObTabletMapKey &key, ObITabletFilterOp &op, ObTabletHandle &guard);
  int get_meta_obj_with_external_memory(
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &guard,
      const bool force_alloc_new,
      ObITabletFilterOp *op);
  int try_get_in_memory_meta_obj(const ObTabletMapKey &key, bool &success, ObTabletHandle &guard);
  int try_get_in_memory_meta_obj_and_addr(
      const ObTabletMapKey &key,
      ObMetaDiskAddr &addr,
      ObTabletHandle &guard);
  int get_meta_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr);
  int get_attr_for_obj(const ObTabletMapKey &key, ObTabletHandle &guard);
  int get_tablet_pointer_initial_state(const ObTabletMapKey &key, bool &initial_state);
  int compare_and_swap_addr_and_object(
      const ObTabletMapKey &key,
      const ObTabletHandle &old_guard,
      const ObTabletHandle &new_guard,
      const ObUpdateTabletPointerParam &update_pointer_param);
  // TIPS:
  //  - only compare and swap pure address, but no reset object.
  // only used for replay and compat, others mustn't call this func
  int compare_and_swap_address_without_object(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &old_addr,
      const ObMetaDiskAddr &new_addr,
      const ObUpdateTabletPointerParam &update_pointer_param,
      const bool set_pool /* whether to set pool */,
      ObITenantMetaObjPool *pool);
  template <typename Operator> int for_each_value_store(Operator &op);
  int wash_meta_obj(const ObTabletMapKey &key, ObTabletHandle &guard, void *&free_obj);
  int64_t count() const { return ResourceMap::map_.size(); }
  OB_INLINE int64_t max_count() const { return ATOMIC_LOAD(&max_count_); }
  int advance_notify_ss_change_version(
      const ObTabletMapKey &key,
      const share::SCN &change_version);

private:
  static int read_from_disk(
      const bool is_full_load,
      const int64_t ls_epoch,
      const ObMetaDiskAddr &load_addr,
      common::ObArenaAllocator &allocator,
      char *&r_buf,
      int64_t &r_len);
  // used when tablet object and memory is hold by external allocator
  int load_meta_obj(
      const ObTabletMapKey &key,
      ObTabletPointer *meta_pointer,
      common::ObArenaAllocator &allocator,
      ObMetaDiskAddr &load_addr,
      ObTablet *t);
  // used when tablet object and memory is hold by t3m
  int load_meta_obj(
      const ObTabletMapKey &key,
      ObTabletPointer *meta_pointer,
      ObMetaDiskAddr &load_addr,
      ObTablet *&t);
  int load_and_hook_meta_obj_(const ObTabletMapKey &key, ObTabletPointerHandle &ptr_hdl, ObTabletHandle &guard);
  int try_get_in_memory_meta_obj_(
      const ObTabletMapKey &key,
      ObTabletPointerHandle &ptr_hdl,
      ObTabletHandle &guard,
      bool &is_in_memory);
  int try_get_in_memory_meta_obj_with_filter_(
      const ObTabletMapKey &key,
      ObITabletFilterOp &op,
      ObTabletPointerHandle &ptr_hdl,
      ObTabletHandle &guard,
      bool &is_in_memory);
  int inner_erase(const ObTabletMapKey &key);

public:
  using ObResourceMap<ObTabletMapKey, ObTabletPointer>::ObResourceMap;

private:
  int64_t max_count_;
};

// ATTENTION: operator should be read-only operations
template <typename Operator>
int ObTabletPointerMap::for_each_value_store(Operator &op)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!ResourceMap::is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMetaPointerMap has not been inited", K(ret));
  } else {
    bool locked = false;
    while (OB_SUCC(ret) && !locked) {
      common::ObBucketTryRLockAllGuard lock_guard(ResourceMap::bucket_lock_);
      if (OB_FAIL(lock_guard.get_ret()) && OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "fail to lock all tablet id set", K(ret));
      } else if (OB_EAGAIN == ret) {
        // try again after 1ms sleep.
        ob_usleep(1000);
        ret = common::OB_SUCCESS;
      } else {
        locked = true;
        if (OB_FAIL(ResourceMap::map_.foreach_refactored(op))) {
          STORAGE_LOG(WARN, "fail to foreach refactored", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_META_POINTER_MAP_H_ */
