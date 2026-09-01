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

#include "storage/meta_mem/ob_tablet_truncate_map.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
/* --------- ObWaitingFreeTruncatedTabletList --------- */
ObWaitingFreeTruncatedTabletList::ObWaitingFreeTruncatedTabletList()
  : spin_lock_(common::ObLatchIds::TENANT_META_MEM_MGR_LOCK),
    map_(),
    is_inited_(false)
{
}

int ObWaitingFreeTruncatedTabletList::init(const int64_t bucket_num, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(bucket_num), K(tenant_id));
  } else if (OB_FAIL(map_.create(bucket_num, "WaitingFreeMap", "WaitingFreeMap", tenant_id))) {
    LOG_WARN("fail to init map", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObWaitingFreeTruncatedTabletList::set(const ObTabletMapKey &key, const ObTabletHandle &tablet_hdl)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  SpinWLockGuard wguard(spin_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid()
                         || !tablet_hdl.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(key), K(tablet_hdl));
  } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tablet", K(ret), K(key), K(tablet_hdl));
  } else if (OB_FAIL(map_.set_refactored(key, tablet_hdl, 0/*don't overwrite*/))) {
    if (OB_HASH_EXIST == ret) { // IMPOSSIBLE
      LOG_ERROR("FATAL ERROR! duplicate key found, impossible", K(ret), K(key));
    } else {
      LOG_WARN("fail to set key", K(ret), K(key), K(tablet_hdl));
    }
  }
  return ret;
}

int ObWaitingFreeTruncatedTabletList::try_remove(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard wguard(spin_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(key));
  } else if (OB_FAIL(map_.erase_refactored(key))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to remove key", K(ret), K(key));
    } else {
      // entry not exists
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

void ObWaitingFreeTruncatedTabletList::destroy()
{
  is_inited_ = false;
  map_.destroy();
}

/* --------- ObPendingTruncateTabletMap --------- */
ObPendingTruncateTabletMap::MapValue::MapValue()
  : new_handle_(),
    old_handle_()
{
}

bool ObPendingTruncateTabletMap::MapValue::is_valid() const
{
  return new_handle_.is_valid() && old_handle_.is_valid();
}

int ObPendingTruncateTabletMap::MapValue::init(
    const ObTabletHandle &new_hdl,
    const ObTabletHandle &old_hdl)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!new_hdl.is_valid() || !old_hdl.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(new_hdl), K(old_hdl));
  } else if (OB_FAIL(new_handle_.assign(new_hdl))) {
    LOG_WARN("failed to assign new tablet handle", K(ret), K(new_hdl));
  } else if (OB_FAIL(old_handle_.assign(old_hdl))) {
    LOG_WARN("failed to assign old tablet handle", K(ret), K(old_hdl));
  }
  return ret;
}

int ObPendingTruncateTabletMap::MapValue::assign(const MapValue &other)
{
  int ret = OB_SUCCESS;

  if (this == &other) {
  } else if (OB_FAIL(new_handle_.assign(other.new_handle_))) {
    LOG_WARN("failed to assign new tablet handle", K(ret), K(other));
  } else if (OB_FAIL(old_handle_.assign(other.old_handle_))) {
    LOG_WARN("failed to assign old tablet handle", K(ret), K(other));
  }
  return ret;
}

void ObPendingTruncateTabletMap::MapValue::reset()
{
  old_handle_.reset();
  new_handle_.reset();
}

ObPendingTruncateTabletMap::ObPendingTruncateTabletMap()
  : map_(),
    is_inited_(false)
{
}

int ObPendingTruncateTabletMap::init(const int64_t bucket_num, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const char *label = "PendingTrctMap";

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(bucket_num), K(tenant_id));
  } else if (OB_FAIL(map_.create(bucket_num, label, label, tenant_id))) {
    LOG_WARN("fail to init map", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObPendingTruncateTabletMap::set(const ObTabletMapKey &key, const MapValue &value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(key), K(value));
  } else if (OB_FAIL(map_.set_refactored(key, value, 0/*don't overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      int tmp_ret = OB_SUCCESS;
      const MapValue *value_in_map = nullptr;
      if (OB_ISNULL(value_in_map = map_.get(key))) {
        tmp_ret = OB_HASH_NOT_EXIST; // IMPOSSIBLE
        LOG_WARN("fail to get key", K(ret), K(tmp_ret), K(key));
      } else {
        LOG_WARN("pending truncate tablet already exists, truncate tx is processing", K(ret), K(key), KPC(value_in_map),
          K(value));
      }
    } else {
      LOG_WARN("fail to set key", K(ret), K(key), K(value));
    }
  }
  return ret;
}

int ObPendingTruncateTabletMap::get(const ObTabletMapKey &key, MapValue &value) const
{
  int ret = OB_SUCCESS;
  const MapValue *value_in_map = nullptr;
  value.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(key));
  } else if (OB_ISNULL(value_in_map = map_.get(key))) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("fail to get key", K(ret), K(key));
  } else if (OB_UNLIKELY(!value_in_map->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect invalid tablet handle in map", K(ret), K(key),
      KPC(value_in_map));
  } else if (OB_FAIL(value.assign(*value_in_map))) {
    LOG_WARN("fail to assign tablet handle", K(ret), K(key), KPC(value_in_map));
  }
  return ret;
}

int ObPendingTruncateTabletMap::has_exist(const ObTabletMapKey &key, bool &b_ret) const
{
  int ret = OB_SUCCESS;
  const MapValue *tmp_val_ptr = nullptr;
  b_ret = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(key));
  } else if (OB_ISNULL(tmp_val_ptr = map_.get(key))) {
    b_ret = false;
  } else if (OB_UNLIKELY(!tmp_val_ptr->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tablet handle in map", K(ret), K(key), KPC(tmp_val_ptr));
  } else {
    b_ret = true;
  }
  return ret;
}

int ObPendingTruncateTabletMap::remove(const ObTabletMapKey &key, MapValue &val)
{
  int ret = OB_SUCCESS;
  val.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(key));
  } else if (OB_FAIL(map_.erase_refactored(key, &val))) {
    LOG_WARN("fail to remove key", K(ret), K(key));
  }
  return ret;
}

void ObPendingTruncateTabletMap::destroy()
{
  map_.destroy();
  is_inited_ = false;
}

} // end namespace storage
} // end namespace oceanbase
