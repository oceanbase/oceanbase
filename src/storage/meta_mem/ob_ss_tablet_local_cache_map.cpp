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

#include "storage/meta_mem/ob_ss_tablet_local_cache_map.h"

namespace oceanbase
{
// using namespace common::hash;
namespace storage
{

ObSSTabletLocalCacheMap::ObSSTabletLocalCacheMap()
 : is_inited_(false),
   bucket_lock_(),
   ss_tablet_map_()
{
}

int ObSSTabletLocalCacheMap::init(const int64_t bucket_num, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (bucket_num <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num), K(tenant_id));
  } else if (OB_FAIL(ss_tablet_map_.create(bucket_num, "MocSSTabletM", "MocSSTabletM", tenant_id))) {
    LOG_WARN("fail to initialize external tablet cnt map");
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::DEFAULT_BUCKET_LOCK, ObMemAttr(tenant_id, "ExTabletMapLk")))) {
    LOG_WARN("fail to init bucket lock", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletLocalCacheMap::check_exist(const ObSSTabletMapKey &key, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("did not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    ObTabletHandle tmp_hdl;
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ss_tablet_map_.get_refactored(key, tmp_hdl))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        exist = false;
      }
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObSSTabletLocalCacheMap::insert_or_update_ss_tablet(const ObSSTabletMapKey &key, const ObTabletHandle &tablet_hdl)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("did not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  // todo feidu inc_shared_storage add this warning_log;
  // } else if (count() >= SS_TABLET_CACHE_THRESHOLD) {
  //   ret = OB_NOT_ENOUGH_STORE;
  //   LOG_INFO("too many cached ss_tablet", K(ret), K(key));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ss_tablet_map_.set_refactored(key, tablet_hdl, 1/*overwrite*/))) {
      LOG_WARN("fail to inc ex_tablet_cnt_map", K(ret), K(key), K(tablet_hdl));
    }
  }
  return ret;
}

int ObSSTabletLocalCacheMap::fetch_tablet(const ObSSTabletMapKey &key, ObTabletHandle &tablet_hdl)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("did not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ss_tablet_map_.get_refactored(key, tablet_hdl))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("tablet not exist in ss_tablet", K(ret), K(key));
      } else {
        LOG_WARN("fail to get tablet from map", K(ret), K(key), K(tablet_hdl));
      }
    }
  }
  return ret;
}

int ObSSTabletLocalCacheMap::unreg_tablet_(const ObSSTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ss_tablet_map_.erase_refactored(key))) {
      LOG_WARN("fail to erase ex_tablet of the key", K(ret), K(key));
    }
  }
  return ret;
}

void ObSSTabletLocalCacheMap::destroy()
{
  is_inited_ = false;
  bucket_lock_.destroy();
  ss_tablet_map_.destroy();
}

} // end namespace storage
} // end namespace oceanbase