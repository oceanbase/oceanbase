/**
 * Copyright (c) 2022 OceanBase
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

#include "share/location_cache/ob_tablet_ls_map.h"
#include "lib/lock/ob_qsync_lock.h" // ObQSyncLock
#include "common/ob_clock_generator.h" // ObClockGenerator
#include "lib/objectpool/ob_concurrency_objpool.h" // op_alloc, op_free

namespace oceanbase
{
using namespace common;

namespace share
{

int ObTabletLSMap::init()
{
  int ret = OB_SUCCESS;
  const char *OB_TABLET_LS_MAP = "TabletLSMap";
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, OB_TABLET_LS_MAP);
  void *buf = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletLSMap init twice", KR(ret));
  } else if (OB_ISNULL(buckets_lock_ =
      (ObQSyncLock*)ob_malloc(sizeof(ObQSyncLock) * LOCK_SLOT_CNT, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate ObQSyncLock memory, ", KR(ret), LITERAL_K(LOCK_SLOT_CNT));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObTabletLSCache*) * BUCKETS_CNT, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate ObTabletLSCache memory, ", KR(ret), LITERAL_K(BUCKETS_CNT));
  } else {
    for (int64_t i = 0 ; i < LOCK_SLOT_CNT && OB_SUCC(ret); ++i) {
      new(buckets_lock_ + i) ObQSyncLock();
      if (OB_FAIL((buckets_lock_ + i)->init(mem_attr))) {
        LOG_WARN("buckets_lock_ init fail", KR(ret), K(OB_SERVER_TENANT_ID));
        for (int64_t j = 0 ; j <= i; ++j) {
          (buckets_lock_ + j)->~ObQSyncLock();
        }
        ob_free(buf);
        buf = NULL;
        ob_free(buckets_lock_);
        buckets_lock_ = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      MEMSET(buf, 0, sizeof(ObTabletLSCache*) * BUCKETS_CNT);
      ls_buckets_ = new (buf) ObTabletLSCache*[BUCKETS_CNT];
      is_inited_ = true;
    }
  }

  return ret;
}


void ObTabletLSMap::destroy()
{
  is_inited_ = false;
  if (OB_NOT_NULL(ls_buckets_)) {
    ObTabletLSCache *tablet_ls_cache = nullptr;
    ObTabletLSCache *next_ls = nullptr;
    for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
      ObQSyncLockWriteGuard guard(buckets_lock_[i % LOCK_SLOT_CNT]);
      tablet_ls_cache = ls_buckets_[i];
      while (OB_NOT_NULL(tablet_ls_cache)) {
        next_ls = (ObTabletLSCache*)tablet_ls_cache->next_;
        op_free(tablet_ls_cache);
        tablet_ls_cache = next_ls;
      }
    }
    ob_free(ls_buckets_);
    ls_buckets_ = NULL;
  }
  if (OB_NOT_NULL(buckets_lock_)) {
    for (int64_t i = 0; i < LOCK_SLOT_CNT; ++i) {
      if (OB_NOT_NULL(buckets_lock_ + i)) {
        (buckets_lock_ + i)->~ObQSyncLock();
      }
    }
    ob_free(buckets_lock_);
    buckets_lock_ = nullptr;
  }
  size_ = 0;
}

int ObTabletLSMap::update(
    const ObTabletLSCache &tablet_ls_cache,
    const bool update_only)
{
  int ret = OB_SUCCESS;
  ObTabletLSCache *curr = NULL;
  ObTabletLSCache *prev = NULL;
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLSMap not init", KR(ret), K(tablet_ls_cache));
  } else {
    const ObTabletLSKey &key = tablet_ls_cache.get_cache_key();
    pos = key.hash() % BUCKETS_CNT;
    ObQSyncLockWriteGuard guard(buckets_lock_[pos % LOCK_SLOT_CNT]);
    curr = ls_buckets_[pos];
    while (OB_NOT_NULL(curr)) {
      if (key == curr->get_cache_key()) {
        break;
      } else {
        curr = static_cast<ObTabletLSCache *>(curr->next_);
      }
    }

    if (OB_ISNULL(curr)) {
      // insert
      if (!update_only) {
        ObTabletLSCache *tmp = op_alloc(ObTabletLSCache);
        if (OB_ISNULL(tmp)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("ls location memory alloc error", KR(ret), K(key), K(tablet_ls_cache));
        } else if (OB_FAIL(tmp->assign(tablet_ls_cache))) {
          LOG_WARN("fail to assign tablet_ls_cache", KR(ret), K(tablet_ls_cache));
        } else {
          // try_update_access_ts_(tmp); // always update for insert
          tmp->next_ = ls_buckets_[pos];
          ls_buckets_[pos] = tmp;
          ATOMIC_INC(&size_);
        }
      }
    } else {
      // update
      if (curr->get_transfer_seq() >= tablet_ls_cache.get_transfer_seq()) {
        LOG_TRACE("current tablet-ls is new enough, just skip", KPC(curr), K(tablet_ls_cache));
      } else if (OB_FAIL(curr->assign(tablet_ls_cache))) {
        LOG_WARN("fail to assign tablet_ls_cache", KR(ret), K(tablet_ls_cache));
      } else {
        // try_update_access_ts_(curr); // always update for update
      }
    }
  }

  return ret;
}

int ObTabletLSMap::get(
    const ObTabletLSKey &key,
    ObTabletLSCache &tablet_ls_cache)
{
  int ret = OB_SUCCESS;
  ObTabletLSCache *curr = NULL;
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLSMap not init", KR(ret), K(key));
  } else {
    pos = key.hash() % BUCKETS_CNT;
    ObQSyncLockReadGuard bucket_guard(buckets_lock_[pos % LOCK_SLOT_CNT]);
    curr = ls_buckets_[pos];
    while (OB_NOT_NULL(curr)) {
      if (key == curr->get_cache_key()) {
        break;
      } else {
        curr = static_cast<ObTabletLSCache *>(curr->next_);
      }
    }

    if (OB_ISNULL(curr)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      // try_update_access_ts_(curr);
      if (OB_FAIL(tablet_ls_cache.assign(*curr))) {
        LOG_WARN("fail to assign tablet_ls_cache", KR(ret), KPC(curr));
      }
    }
  }
  return ret;
}

int ObTabletLSMap::get_all(common::ObIArray<ObTabletLSCache> &cache_array)
{
  int ret = OB_SUCCESS;
  ObTabletLSCache *tablet_ls_cache = NULL;
  for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
    ObQSyncLockReadGuard guard(buckets_lock_[i % LOCK_SLOT_CNT]);
    tablet_ls_cache = ls_buckets_[i];
    // foreach bucket
    while (OB_NOT_NULL(tablet_ls_cache) && OB_SUCC(ret)) {
      if (OB_FAIL(cache_array.push_back(*tablet_ls_cache))) {
        LOG_WARN("ls location array push back error", KR(ret), K(*tablet_ls_cache));
      } else {
        tablet_ls_cache = static_cast<ObTabletLSCache *>(tablet_ls_cache->next_);
      }
    }
  }
  return ret;
}

int ObTabletLSMap::get_tablet_ids(
    const uint64_t tenant_id,
    common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  tablet_ids.reset();
  ObTabletLSCache *tablet_ls_cache = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < BUCKETS_CNT; ++i) {
    ObQSyncLockReadGuard guard(buckets_lock_[i % LOCK_SLOT_CNT]);
    tablet_ls_cache = ls_buckets_[i];
    while (OB_NOT_NULL(tablet_ls_cache) && OB_SUCC(ret)) {
      if (tablet_ls_cache->get_tenant_id() == tenant_id) {
        if (OB_FAIL(tablet_ids.push_back(tablet_ls_cache->get_tablet_id()))) {
          LOG_WARN("fail to push back tablet_id", KR(ret), KPC(tablet_ls_cache));
        }
      }
      tablet_ls_cache = static_cast<ObTabletLSCache *>(tablet_ls_cache->next_);
    }
  }
  FLOG_INFO("get tablet_ids cost", KR(ret), K(tenant_id),
            "map_size", size_, "tablet_ids_cnt", tablet_ids.count(),
            "cost_us", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObTabletLSMap::del(const ObTabletLSKey &key)
{
  int ret = OB_SUCCESS;
  ObTabletLSCache *prev = NULL;
  ObTabletLSCache *tablet_ls_cache = NULL;
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLSMap not init", KR(ret), K(key));
  } else {
    pos = key.hash() % BUCKETS_CNT;
    ObQSyncLockWriteGuard guard(buckets_lock_[pos % LOCK_SLOT_CNT]);
    tablet_ls_cache = ls_buckets_[pos];
    while (OB_NOT_NULL(tablet_ls_cache)) {
      if (key == tablet_ls_cache->get_cache_key()) {
        break;
      } else {
        prev = tablet_ls_cache;
        tablet_ls_cache = (ObTabletLSCache *)tablet_ls_cache->next_;
      }
    }

    if (OB_ISNULL(tablet_ls_cache)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      if (OB_ISNULL(prev)) {
        // the first node
        ls_buckets_[pos] = (ObTabletLSCache *)tablet_ls_cache->next_;
      } else {
        prev->next_ = tablet_ls_cache->next_;
      }
      tablet_ls_cache->next_ = NULL;
      op_free(tablet_ls_cache);
      ATOMIC_DEC(&size_);
    }
  }

  return ret;
}

// void ObTabletLSMap::try_update_access_ts_(ObTabletLSCache *cache_ptr)
// {
//   OB_ASSERT(NULL != cache_ptr);
//   if (common::ObClockGenerator::getClock() > cache_ptr->get_last_access_ts() + MAX_ACCESS_TIME_UPDATE_THRESHOLD) {
//     cache_ptr->set_last_access_ts(common::ObClockGenerator::getClock());
//   }
// }

} // end namespace share
} // end namespace oceanbase
