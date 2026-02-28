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

#define USING_LOG_PREFIX RS

#include "ob_max_id_cache.h"
#include "src/observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase
{
namespace share
{

ObMaxIdCacheItem::ObMaxIdCacheItem(const ObMaxIdType &type, const uint64_t tenant_id) :
  min_id_(OB_INVALID_ID), size_(OB_INVALID_SIZE), tenant_id_(tenant_id), type_(type), latch_()
{
}

bool ObMaxIdCacheItem::cached_id_valid_()
{
  return min_id_ != OB_INVALID_ID && size_ != OB_INVALID_SIZE;
}

int ObMaxIdCacheItem::fetch_max_id(const uint64_t tenant_id, const ObMaxIdType max_id_type,
    uint64_t &id, const uint64_t size, ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (tenant_id != tenant_id_ || max_id_type != type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument not match", KR(ret), K(tenant_id), K(tenant_id_), K(max_id_type), K(type_));
  } else if (!cached_id_valid_() || size_ < size) {
    const uint64_t fetch_size = common::max(CACHE_SIZE, size);
    if (OB_FAIL(fetch_ids_from_inner_table_(fetch_size, sql_proxy))) {
      LOG_WARN("failed to fetch ids from inner table", KR(ret), K(tenant_id), K(fetch_size),
          K(max_id_type), K(size));
    }
  }
  if (FAILEDx(fetch_ids_by_cache_(size, id))) {
    LOG_WARN("failed to fetch ids from cache", KR(ret), K(size), K(max_id_type), K(tenant_id));
  }
  return ret;
}

int ObMaxIdCacheItem::fetch_ids_from_inner_table_(const uint64_t size, ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(sql_proxy));
  } else {
    uint64_t id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*sql_proxy);
    uint64_t old_min_id = ATOMIC_LOAD(&min_id_);
    if (OB_FAIL(id_fetcher.batch_fetch_new_max_id_from_inner_table(tenant_id_, type_, id, size))) {
      LOG_WARN("failed to batch fetch new max id from inner table", KR(ret), K(tenant_id_), K(type_), K(size));
    } else if (OB_INVALID_ID == id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("id is invalid", KR(ret), K(id), K(tenant_id_), K(type_), K(size));
    } else {
      ObLatchWGuard guard(latch_, ObLatchIds::MAX_ID_CACHE_LOCK);
      uint64_t new_min_id = id - size + 1;
      if (cached_id_valid_() && min_id_ + size_ == new_min_id) {
        size_ += size;
      } else if (old_min_id != min_id_) {
        ret = OB_EAGAIN;
        LOG_WARN("min_id_ changed, need try", KR(ret), K_(tenant_id), K(old_min_id), K_(min_id));
      } else if ((OB_INVALID_ID != min_id_) && (min_id_ > new_min_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("min_id_ revert is not expected", KR(ret), K_(tenant_id), K_(min_id), K(new_min_id));
      } else {
        LOG_INFO("max id cached renewed", KR(ret), K(tenant_id_), K(type_),
            K(min_id_), K(size_), K(new_min_id), K(size));
        min_id_ = new_min_id;
        size_ = size;
      }
    }
  }
  return ret;
}

int ObMaxIdCacheItem::fetch_ids_by_cache_(const uint64_t size, uint64_t &id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == min_id_ || OB_INVALID_SIZE == size_ || size_ < size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size out of range", KR(ret), K(min_id_), K(size_), K(size));
  } else {
    ObLatchWGuard guard(latch_, ObLatchIds::MAX_ID_CACHE_LOCK);
    id = min_id_;
    min_id_ += size;
    size_ -= size;
  }
  return ret;
}

ObMaxIdCache::ObMaxIdCache(const uint64_t tenant_id) : tenant_id_(tenant_id),
  object_id_cache_(OB_MAX_USED_OBJECT_ID_TYPE, tenant_id),
  normal_rowid_table_tablet_id_cache_(OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, tenant_id),
  extended_rowid_table_tablet_id_cache_(OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, tenant_id)
{
}

int ObMaxIdCache::fetch_max_id(const uint64_t tenant_id, const ObMaxIdType max_id_type,
    uint64_t &id, const uint64_t size, ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObMaxIdCacheItem *item = nullptr;
  if (tenant_id != tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id not equal", KR(ret), K(tenant_id), K(tenant_id_));
  } else if (OB_MAX_USED_OBJECT_ID_TYPE == max_id_type) {
    item = &object_id_cache_;
  } else if (OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE == max_id_type) {
    item = &normal_rowid_table_tablet_id_cache_;
  } else if (OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE == max_id_type) {
    item = &extended_rowid_table_tablet_id_cache_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache for max id type is not supported", KR(ret), K(max_id_type));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(item));
  } else if (OB_FAIL(item->fetch_max_id(tenant_id, max_id_type, id, size, sql_proxy))) {
    LOG_WARN("failed to fetch max id in item", KR(ret), K(tenant_id), K(max_id_type), K(size));
  }
  return ret;
}

ObMaxIdCacheMgr::ObMaxIdCacheMgr() : attr_(OB_SYS_TENANT_ID, ObLabel("MaxIdCache")), allocator_(attr_),
  inited_(false), sql_proxy_(nullptr)
{
}

ObMaxIdCacheMgr::~ObMaxIdCacheMgr()
{
  reset();
}

int ObMaxIdCacheMgr::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t HASHMAP_BUCKET_SIZE = 512;
  ObLatchWGuard guard(latch_, ObLatchIds::MAX_ID_CACHE_LOCK);
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(sql_proxy));
  } else if (OB_FAIL(tenant_caches_.create(HASHMAP_BUCKET_SIZE, attr_))) {
    LOG_WARN("failed to create hash map", KR(ret));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }
  return ret;
}

void ObMaxIdCacheMgr::reset()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLatchWGuard guard(latch_, ObLatchIds::MAX_ID_CACHE_LOCK);
  for (hashmap::iterator it = tenant_caches_.begin(); it != tenant_caches_.end(); it++) {
    ObMaxIdCache *cache = it->second;
    if (OB_TMP_FAIL(remove_cache_(cache))) {
      LOG_WARN("failed to remove cache", KR(tmp_ret), KP(cache), "tenant_id", it->first);
    }
  }
  if (OB_TMP_FAIL(tenant_caches_.destroy())) {
    LOG_WARN("failed to destroy tenant caches", KR(tmp_ret));
  }
  allocator_.reset();
}


int ObMaxIdCacheMgr::remove_cache_(ObMaxIdCache *cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(cache));
  } else {
    cache->~ObMaxIdCache();
    allocator_.free(cache);
  }
  return ret;
}

int ObMaxIdCacheMgr::fetch_max_id(const uint64_t tenant_id, const ObMaxIdType max_id_type, uint64_t &id,
    const uint64_t size, bool init_tenant_if_not_exist)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMaxIdCache *cache = nullptr;
  bool tenant_not_inited = false;
  if (OB_ISNULL(GCTX.root_service_))  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.root_service_));
  } else if (!GCTX.root_service_->in_service()) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("rs is shutdown", KR(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("max id cache mgr is not inited", KR(ret), K(inited_));
  } else {
    ObLatchRGuard guard(latch_, ObLatchIds::MAX_ID_CACHE_LOCK);
    if (OB_FAIL(tenant_caches_.get_refactored(tenant_id, cache))) {
      LOG_WARN("failed to get tenant cache", KR(ret), K(tenant_id), K(init_tenant_if_not_exist));
      tenant_not_inited = true;
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(cache));
    } else if (OB_FAIL(cache->fetch_max_id(tenant_id, max_id_type, id, size, sql_proxy_))) {
      LOG_WARN("failed to fetch max id", KR(ret), K(tenant_id), K(max_id_type), K(size));
    }
  }
  if (OB_HASH_NOT_EXIST == ret && tenant_not_inited && init_tenant_if_not_exist) {
    ret = OB_SUCCESS;
    {
      ObLatchWGuard guard(latch_, ObLatchIds::MAX_ID_CACHE_LOCK);
      if (OB_TMP_FAIL(add_tenant_(tenant_id))) {
        // other thread may init this tenant, ignore error
        LOG_WARN("failed to init new tenant", KR(tmp_ret));
      }
    }
    if (OB_FAIL(fetch_max_id(tenant_id, max_id_type, id, size, false/*init_tenant_if_not_exist*/))) {
      LOG_WARN("failed to fetch max id", KR(ret), K(tenant_id), K(max_id_type), K(size));
    }
  }
  return ret;
}

int ObMaxIdCacheMgr::add_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMaxIdCache *cache = OB_NEWx(ObMaxIdCache, &allocator_, tenant_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("max id cache mgr is not inited", KR(ret), K(inited_));
  } else if (OB_FAIL(tenant_caches_.set_refactored(tenant_id, cache))) {
    LOG_WARN("failed to put cache to tenant_caches_", KR(ret), K(tenant_id));
  }
  if (OB_FAIL(ret)) {
    cache->~ObMaxIdCache();
    allocator_.free(cache);
  }
  return ret;
}
}
}
