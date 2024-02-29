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
#define USING_LOG_PREFIX STORAGE

#include "storage/meta_mem/ob_meta_pointer_map.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

template <>
int ObMetaPointerMap<ObTabletMapKey, ObTablet>::compare_and_swap_addr_and_object(
    const ObTabletMapKey &key,
    const ObTabletHandle &old_tablet_handle,
    const ObTabletHandle &new_tablet_handle)
{
  int ret = common::OB_SUCCESS;
  ObMetaPointerHandle<ObTabletMapKey, ObTablet> ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
  ObMetaObjGuard<ObTablet> ptr_guard;
  uint64_t hash_val = 0;

  if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_UNLIKELY(!key.is_valid()
               || !old_tablet_handle.is_valid()
               || !new_tablet_handle.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(old_tablet_handle), K(new_tablet_handle));
  } else if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
    } else if (OB_ISNULL(t_ptr = static_cast<ObTabletPointer *>(ptr_hdl.get_resource_ptr()))) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else if (OB_UNLIKELY(!t_ptr->is_in_memory())) {
      if (t_ptr->get_addr().is_disked()) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "old object has changed, which is not allowed", K(ret), KP(t_ptr));
      } else {
        // first time CAS
        if (!t_ptr->get_addr().is_none() || old_tablet_handle.get_obj() != new_tablet_handle.get_obj()) {
          ret = common::OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "this is not the first time cas", K(ret),
            K(t_ptr->get_addr()), K(old_tablet_handle), K(new_tablet_handle));
        } else {
          ret = common::OB_SUCCESS;
        }
      }
    } else if (OB_FAIL(t_ptr->get_in_memory_obj(ptr_guard))) {
      STORAGE_LOG(WARN, "fail to get object", K(ret), KP(t_ptr));
    } else if (OB_UNLIKELY(ptr_guard.get_obj() != old_tablet_handle.get_obj())) {
      ret = common::OB_NOT_THE_OBJECT;
      STORAGE_LOG(WARN, "old object has changed", KP(ptr_guard.get_obj()), K(old_tablet_handle));
    }

    if (OB_SUCC(ret)) {
      t_ptr->set_addr_with_reset_obj(new_tablet_handle.get_obj()->get_tablet_addr());
      t_ptr->set_obj(new_tablet_handle);
      t_ptr->set_tablet_space_usage(new_tablet_handle.get_obj()->get_tablet_meta().space_usage_);
    }
  }

  return ret;
}

template <>
int ObMetaPointerMap<ObTabletMapKey, ObTablet>::compare_and_swap_address_without_object(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr,
    const bool set_pool /* whether to set pool */,
    ObITenantMetaObjPool *pool)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<ObTabletMapKey, ObTablet> ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
  if (OB_UNLIKELY(!key.is_valid()
               || !old_addr.is_valid()
               || !new_addr.is_valid()
               || new_addr.is_none()
               || (set_pool && nullptr == pool))) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(old_addr), K(new_addr), K(set_pool), KP(pool));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
    } else if (OB_ISNULL(t_ptr = static_cast<ObTabletPointer *>(ptr_hdl.get_resource_ptr()))) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else if (OB_UNLIKELY(t_ptr->get_addr() != old_addr)) {
      ret = common::OB_NOT_THE_OBJECT;
      STORAGE_LOG(WARN, "old address has changed, need to get again", K(ret), KPC(t_ptr), K(old_addr));
    } else {
      t_ptr->set_addr_with_reset_obj(new_addr);
      if (set_pool) {
        t_ptr->set_obj_pool(*pool);
      }
    }
  }
  return ret;
}

template <>
int ObMetaPointerMap<ObTabletMapKey, ObTablet>::load_and_hook_meta_obj(
    const ObTabletMapKey &key,
    ObMetaPointerHandle<ObTabletMapKey, ObTablet> &ptr_hdl,
    ObMetaObjGuard<ObTablet> &guard)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaDiskAddr disk_addr;
  ObTabletPointer *t_ptr = static_cast<ObTabletPointer *>(ptr_hdl.get_resource_ptr());
  do {
    bool need_free_obj = false;
    ObTablet *t = nullptr;
    // Move load obj from disk out of the bucket lock, because
    // wash obj may acquire the bucket lock again, which cause dead lock.
    if (OB_FAIL(load_meta_obj(key, t_ptr, disk_addr, t))) {
      STORAGE_LOG(WARN, "load obj from disk fail", K(ret), K(key), KPC(t_ptr), K(lbt()));
    } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
      STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
    } else {
      ObMetaPointerHandle<ObTabletMapKey, ObTablet> tmp_ptr_hdl(*this);
      {
        common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
        if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
          }
          need_free_obj = true;
        } else if (t_ptr->is_in_memory()) {  // some other thread finish loading
          need_free_obj = true;
          if (OB_FAIL(t_ptr->get_in_memory_obj(guard))) {
            STORAGE_LOG(WARN, "fail to get meta object", K(ret), KP(t_ptr));
          }
        } else if (OB_UNLIKELY(disk_addr != t_ptr->get_addr()
            || t_ptr != tmp_ptr_hdl.get_resource_ptr()
            || t_ptr->get_addr() != tmp_ptr_hdl.get_resource_ptr()->get_addr())) {
          ret = OB_ITEM_NOT_MATCH;
          need_free_obj = true;
          if (REACH_TIME_INTERVAL(1000000)) {
            STORAGE_LOG(WARN, "disk address or pointer change", K(ret), K(disk_addr), KPC(t_ptr),
                KPC(tmp_ptr_hdl.get_resource_ptr()));
          }
        } else {
          if (OB_FAIL(t_ptr->hook_obj(t, guard))) {
            STORAGE_LOG(WARN, "fail to hook object", K(ret), KP(t_ptr));
          } else if (FALSE_IT(t_ptr->set_tablet_space_usage(t->get_tablet_meta().space_usage_))) {
          } else if (OB_FAIL(guard.get_obj()->assign_pointer_handle(ptr_hdl))) {
            STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret));
          }
        }
      } // write lock end
      if (need_free_obj) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(t_ptr->release_obj(t))) {
          STORAGE_LOG(ERROR, "fail to release object", K(ret), K(tmp_ret), KP(t_ptr));
        } else if (t_ptr != tmp_ptr_hdl.get_resource_ptr()) {
          t_ptr = static_cast<ObTabletPointer *>(tmp_ptr_hdl.get_resource_ptr());
          if (OB_TMP_FAIL(ptr_hdl.assign(tmp_ptr_hdl))) {
            STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret), K(tmp_ret), K(ptr_hdl), K(tmp_ptr_hdl));
          }
        }
      }
    }
  } while (OB_ITEM_NOT_MATCH == ret);
  return ret;
}

} // namespace storage
} // namespace oceanbase