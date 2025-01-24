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

#include "ob_flying_tablet_pointer_map.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{

ObFlyingTabletPointerMap::ObFlyingTabletPointerMap(const int64_t capacity)
    : is_inited_(false),
      capacity_(capacity),
      bucket_lock_(),
      map_()
{
}

int ObFlyingTabletPointerMap::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = 999;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(map_.create(bucket_num, "FlyTabletPtrMap", "FlyTabletPtrMap", tenant_id))) {
    LOG_WARN("fail to initialize external tablet cnt map");
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::DEFAULT_BUCKET_LOCK, ObMemAttr(tenant_id, "FlyTabletMapLk")))) {
    LOG_WARN("fail to init bucket lock", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFlyingTabletPointerMap::set(const ObDieingTabletMapKey &key, ObTabletPointerHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t ls_id = handle.get_resource_ptr()->get_ls()->get_ls_id().id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObResourceMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.set_refactored(key, handle))) {
    LOG_WARN("fail to set into ResourceMap", K(ret), K(key));
  } else {
    FLOG_INFO("success to push tablet_pointer to flying_map", K(ret), K(ls_id), K(key), KP(handle.get_resource_ptr()), KPC(handle.get_resource_ptr()), K(count()));
  }
  return ret;
}

int ObFlyingTabletPointerMap::check_exist(
    const ObDieingTabletMapKey &key,
    bool &is_exist)
{
  int ret = common::OB_SUCCESS;
  ObInnerTPHandlePtr handle_ptr;
  ObTabletPointer *t_ptr = nullptr;
  is_exist = false;
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObResourceMap has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    common::ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_ISNULL(handle_ptr = map_.get(key))) {
      is_exist = false;
      LOG_INFO("tablet handle not exist", K(ret), K(key));
    } else if (OB_ISNULL(t_ptr = handle_ptr->get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tablet pointer", K(ret), KP(t_ptr));
    } else {
      is_exist = true;
    }
  }
  return ret;
}

void ObFlyingTabletPointerMap::destroy()
{
  is_inited_ = false;
  bucket_lock_.destroy();
  for (common::hash::ObHashMap<ObDieingTabletMapKey, ObTabletPointerHandle>::iterator iter = map_.begin();
      iter != map_.end();
      ++iter) {
    iter->second.reset();
  }
  map_.destroy();
}

int ObFlyingTabletPointerMap::erase(const ObDieingTabletMapKey &key)
{
  int ret = common::OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObResourceMap has not been inited", K(ret));
  } else if (OB_FAIL(check_exist(key, is_exist))) {
    LOG_WARN("failed to check exist", K(ret), K(key));
  } else if (!is_exist) {
    LOG_WARN("this key is not exist, do not erase", K(ret), K(key));
  } else if (OB_FAIL(inner_erase_(key))) {
    LOG_WARN("fail to erase meta pointer", K(ret), K(key));
  }

  FLOG_INFO("success to remove tablet_pointer to flying_map", K(ret), K(key), K(count()));
  return ret;
}

int ObFlyingTabletPointerMap::inner_erase_(const ObDieingTabletMapKey &key)
{
  int ret = common::OB_SUCCESS;
  ObInnerTPHandlePtr handle_ptr;
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_ISNULL(handle_ptr = map_.get(key))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get from map", K(ret), K(key));
    } else if (!handle_ptr->get_resource_ptr()->need_remove_from_flying_()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet_pointer should not be erased when tablet has been referred",
        K(ret), KP(handle_ptr->get_resource_ptr()), KPC(handle_ptr->get_resource_ptr()) );
    } else if (OB_FAIL(map_.erase_refactored(key))) {
      LOG_WARN("fail to erase from map", K(ret));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
