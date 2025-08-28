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

#include "storage/meta_mem/ob_external_tablet_cnt_map.h"

namespace oceanbase
{
// using namespace common::hash;
namespace storage
{

ObExternalTabletCntMap::ObExternalTabletCntMap()
 : is_inited_(false),
   bucket_lock_(),
   ex_tablet_map_()
{ 
}

int ObExternalTabletCntMap::init(const int64_t bucket_num, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (bucket_num <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num), K(tenant_id));
  } else if (OB_FAIL(ex_tablet_map_.create(bucket_num, "ExTabletCntMap", "ExTabletCntMap", tenant_id))) {
    LOG_WARN("fail to initialize external tablet cnt map");
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::DEFAULT_BUCKET_LOCK, ObMemAttr(tenant_id, "ExTabletMapLk")))) {
    LOG_WARN("fail to init bucket lock", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObExternalTabletCntMap::check_exist(const ObDieingTabletMapKey &key, bool &exist)
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
    int64_t curr_cnt = 0;
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ex_tablet_map_.get_refactored(key, curr_cnt))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        exist = false;
      }
    } else if (curr_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status, ex_tablet_cnt should not le 0", K(ret), K(key));
    } else if (curr_cnt > 0) {
      exist = true;
    }
  }
  return ret;
}

int ObExternalTabletCntMap::reg_tablet(const ObDieingTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("did not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    int64_t curr_cnt = 0;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ex_tablet_map_.get_refactored(key, curr_cnt))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(ex_tablet_map_.set_refactored(key, 1, 0/*overwrite*/))) {
          LOG_WARN("fail to set ex_tablet_cnt_map first time", K(ret), K(key));
        }
      }
    } else if (curr_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status, ex_tablet_cnt should not le 0", K(ret), K(key));
    } else if (OB_FAIL(ex_tablet_map_.set_refactored(key, curr_cnt + 1, 1/*overwrite*/))) {
      LOG_WARN("fail to inc ex_tablet_cnt_map", K(ret), K(key), K(curr_cnt + 1));
    }
  }
  return ret;
}

int ObExternalTabletCntMap::unreg_tablet(const ObDieingTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("did not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    int64_t curr_cnt = 0;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(ex_tablet_map_.get_refactored(key, curr_cnt))) {
      LOG_WARN("fail to get ex_tablet_cnt", K(ret), K(key));
    } else if (curr_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status, ex_tablet_cnt should not le 0", K(ret), K(key));
    } else if (0 == curr_cnt - 1) {
      if (OB_FAIL(ex_tablet_map_.erase_refactored(key))) {
        LOG_WARN("fail to erase ex_tablet of the key", K(ret), K(key), K(curr_cnt));
      }
    } else if (OB_FAIL(ex_tablet_map_.set_refactored(key, curr_cnt - 1, 1/*overwrite*/))) {
      LOG_WARN("fail to inc ex_tablet_cnt_map", K(ret), K(key), K(curr_cnt + 1));
    }
  }
  return ret;
}

void ObExternalTabletCntMap::destroy()
{
  is_inited_ = false;
  bucket_lock_.destroy();
  ex_tablet_map_.destroy();
}

} // end namespace storage
} // end namespace oceanbase