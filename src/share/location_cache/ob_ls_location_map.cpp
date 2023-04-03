/**
 * Copyright (c) 2021, 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE_LOCATION

#include "share/location_cache/ob_ls_location_map.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
using namespace common;
namespace share
{
int ObLSLocationMap::init()
{
  int ret = OB_SUCCESS;
  const char *OB_LS_LOCATION_MAP = "LSLocationMap";
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, OB_LS_LOCATION_MAP);
  void *buf = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSLocationMap init twice", KR(ret));
  } else if (OB_ISNULL(buckets_lock_ =
     (common::ObQSyncLock*)ob_malloc(sizeof(common::ObQSyncLock) * BUCKETS_CNT, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate ObQSyncLock memory, ", KR(ret), LITERAL_K(BUCKETS_CNT));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObLSLocation*) * BUCKETS_CNT, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate ObLSLocation memory, ", KR(ret), LITERAL_K(BUCKETS_CNT));
  } else {
    for (int64_t i = 0 ; i < BUCKETS_CNT; ++i) {
      new(buckets_lock_ + i) common::ObQSyncLock();
      if (OB_FAIL((buckets_lock_ + i)->init(mem_attr))) {
        LOG_WARN("buckets_lock_ init fail", K(ret), K(OB_SERVER_TENANT_ID));
        for (int64_t j = 0 ; j <= i; ++j) {
          (buckets_lock_ + j)->destroy();
        }
        ob_free(buf);
        buf = NULL;
        ob_free(buckets_lock_);
        buckets_lock_ = NULL;
        break;
      }
    }
    if (OB_LIKELY(ret == common::OB_SUCCESS)) {
      MEMSET(buf, 0, sizeof(ObLSLocation*) * BUCKETS_CNT);
      ls_buckets_ = new (buf) ObLSLocation*[BUCKETS_CNT];
      is_inited_ = true;
    }
  }

  return ret;
}

void ObLSLocationMap::destroy()
{
  is_inited_ = false;
  if (OB_NOT_NULL(ls_buckets_)) {
    ObLSLocation *ls_location = nullptr;
    ObLSLocation *next_ls = nullptr;
    for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
      ObQSyncLockWriteGuard guard(buckets_lock_[i]);
      ls_location = ls_buckets_[i];
      while (OB_NOT_NULL(ls_location)) {
        next_ls = (ObLSLocation*)ls_location->next_;
        op_free(ls_location);
        ls_location = next_ls;
      }
    }
    ob_free(ls_buckets_);
    ls_buckets_ = NULL;
  }
  if (OB_NOT_NULL(buckets_lock_)) {
    for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
      if (OB_NOT_NULL(buckets_lock_ + i)) {
        (buckets_lock_ + i)->~ObQSyncLock();
      }
    }
    ob_free(buckets_lock_);
    buckets_lock_ = nullptr;
  }
  size_ = 0;
}


// 1. from_rpc = true, it means leader from ls_location may be merged to current location.
// 2. from_rpc = false, it means leader from current location may be remained in ls_location.
// when ls has no leader, location cache will remain the last leader info.
int ObLSLocationMap::update(
    const bool from_rpc,
    const ObLSLocationCacheKey &key,
    ObLSLocation &ls_location)
{
  int ret = OB_SUCCESS;
  ObLSLocation *curr = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSLocationMap not init", KR(ret), K(ls_location));
  } else {
    int64_t pos = key.hash() % BUCKETS_CNT;
    ObQSyncLockWriteGuard guard(buckets_lock_[pos]);
    curr = ls_buckets_[pos];
    while (OB_NOT_NULL(curr)) {
      if (curr->get_cache_key() == key) {
        break;
      } else {
        curr = static_cast<ObLSLocation *>(curr->next_);
      }
    }

    if (OB_ISNULL(curr)) {
      // insert
      ObLSLocation *tmp = op_alloc(ObLSLocation);
      if (NULL == tmp) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("ls location memory alloc error", KR(ret), K(key), K(ls_location));
      } else if (OB_FAIL(tmp->deep_copy(ls_location))) {
        LOG_WARN("ls location deep copy error", KR(ret), K(ls_location));
      } else {
        if (common::ObClockGenerator::getClock() > tmp->get_last_access_ts() + MAX_ACCESS_TIME_UPDATE_THRESHOLD) {
          tmp->set_last_access_ts(common::ObClockGenerator::getClock());
        }
        tmp->next_ = ls_buckets_[pos];
        ls_buckets_[pos] = tmp;
        ATOMIC_INC(&size_);
      }
    } else if (from_rpc) {
      if (OB_FAIL(curr->merge_leader_from(ls_location))) {
        LOG_WARN("fail to merge leader from ls", KR(ret), KPC(curr), K(ls_location));
      }
    } else {
      if (OB_FAIL(ls_location.merge_leader_from(*curr))) {
        LOG_WARN("fail to merge leader from ls", KR(ret), K(ls_location), KPC(curr));
      } else if (OB_FAIL(curr->deep_copy(ls_location))) {
        LOG_WARN("ls location deep copy error", KR(ret), K(ls_location));
      }
    }
  }

  return ret;
}

int ObLSLocationMap::get(
    const ObLSLocationCacheKey &key,
    ObLSLocation &location) const
{
  int ret = OB_SUCCESS;
  ObLSLocation *ls_location = NULL;
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSLocationMap not init", KR(ret), K(key));
  } else {
    pos = key.hash() % BUCKETS_CNT;
    ObQSyncLockReadGuard bucket_guard(buckets_lock_[pos]);
    ls_location = ls_buckets_[pos];
    while (OB_NOT_NULL(ls_location)) {
      if (ls_location->get_cache_key() == key) {
        break;
      } else {
        ls_location = static_cast<ObLSLocation *>(ls_location->next_);
      }
    }

    if (OB_ISNULL(ls_location)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      if (common::ObClockGenerator::getClock() > ls_location->get_last_access_ts() + MAX_ACCESS_TIME_UPDATE_THRESHOLD) {
        ls_location->set_last_access_ts(common::ObClockGenerator::getClock());
      }
      if (OB_FAIL(location.deep_copy(*ls_location))) {
        LOG_WARN("ls location deep copy error", KR(ret), KPC(ls_location));
      }
    }
  }
  return ret;
}

int ObLSLocationMap::del(const ObLSLocationCacheKey &key)
{
  int ret = OB_SUCCESS;
  ObLSLocation *prev = NULL;
  ObLSLocation *ls_location = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSLocationMap not init", KR(ret), K(key));
  } else {
    int64_t pos = key.hash() % BUCKETS_CNT;
    //remove ls from map
    ObQSyncLockWriteGuard guard(buckets_lock_[pos]);
    ls_location = ls_buckets_[pos];
    while (OB_NOT_NULL(ls_location)) {
      if (ls_location->get_cache_key() == key) {
        break;
      } else {
        prev = ls_location;
        ls_location = (ObLSLocation *)ls_location->next_;
      }
    }

    if (OB_ISNULL(ls_location)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      if (OB_ISNULL(prev)) {
        // the first node
        ls_buckets_[pos] = (ObLSLocation *)ls_location->next_;
      } else {
        prev->next_ = ls_location->next_;
      }
      ls_location->next_ = NULL;
      op_free(ls_location);
      ATOMIC_DEC(&size_);
    }
  }

  return ret;
}

int ObLSLocationMap::check_and_generate_dead_cache(ObLSLocationArray &arr)
{
  int ret = OB_SUCCESS;
  ObLSLocation *ls_location = NULL;
  // ignore ret code
  for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
    ls_location = ls_buckets_[i];
    ObQSyncLockWriteGuard guard(buckets_lock_[i]);
    // foreach bucket
    while (OB_NOT_NULL(ls_location)) {
      if (common::ObClockGenerator::getClock() - ls_location->get_last_access_ts()
          > 60 * MAX_ACCESS_TIME_UPDATE_THRESHOLD /*10min*/) {
        if (OB_FAIL(arr.push_back(*ls_location))) {
          LOG_WARN("ls location array push back error", KR(ret), K(*ls_location));
        }
      }
      ls_location = (ObLSLocation *)ls_location->next_;
    }
  }
  return OB_SUCCESS;
}

int ObLSLocationMap::get_all(ObLSLocationArray &arr)
{
  int ret = OB_SUCCESS;
  ObLSLocation *ls_location = NULL;
  for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
    ls_location = ls_buckets_[i];
    ObQSyncLockReadGuard guard(buckets_lock_[i]);
    // foreach bucket
    while (OB_NOT_NULL(ls_location) && OB_SUCC(ret)) {
      if (OB_FAIL(arr.push_back(*ls_location))) {
        LOG_WARN("ls location array push back error", KR(ret), K(*ls_location));
      }
      ls_location = (ObLSLocation *)ls_location->next_;
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
