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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "lib/oblog/ob_log_module.h"
#include "ob_schema_store.h"
#include "share/inner_table/ob_inner_table_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
int ObSchemaStore::init(const uint64_t tenant_id,
                        const int64_t init_version_count,
                        const int64_t init_version_count_for_liboblog)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_mgr_cache_.init(init_version_count, ObSchemaMgrCache::REFRESH))) {
    LOG_WARN("init schema_mgr_cache fail", K(ret));
  } else if (OB_FAIL(schema_mgr_cache_for_liboblog_.init(init_version_count_for_liboblog,
                                                         ObSchemaMgrCache::FALLBACK))) {
    LOG_WARN("init schema_mgr_cache fail", K(ret));
  } else {
    tenant_id_ = tenant_id;
    refreshed_version_ = OB_CORE_SCHEMA_VERSION;
    received_version_ = OB_CORE_SCHEMA_VERSION;
    checked_sys_version_ = OB_INVALID_VERSION;
    baseline_schema_version_ = OB_INVALID_VERSION;
    LOG_INFO("[SCHEMA_STORE] schema store init", K(tenant_id));
  }
  return ret;
}

void ObSchemaStore::reset_version()
{
  refreshed_version_ = OB_INVALID_VERSION;
  received_version_ = OB_INVALID_VERSION;
  checked_sys_version_ = OB_INVALID_VERSION;
  baseline_schema_version_ = OB_INVALID_VERSION;
  LOG_INFO("[SCHEMA_STORE] schema store reset version", K_(tenant_id));
}

void ObSchemaStore::update_refreshed_version(int64_t version)
{
  if (version > refreshed_version_ || version > received_version_) {
    inc_update(&refreshed_version_, version);
    inc_update(&received_version_, version);
    LOG_INFO("[SCHEMA_STORE] schema store update version",
             K_(tenant_id), K(version), K_(refreshed_version), K_(received_version));
  }
}

void ObSchemaStore::update_received_version(int64_t version)
{
  if (version > received_version_) {
    inc_update(&received_version_, version);
    LOG_INFO("[SCHEMA_STORE] schema store update version",
             K_(tenant_id), K(version), K_(received_version));
  }
}

void ObSchemaStore::update_checked_sys_version(int64_t version)
{
  if (version > checked_sys_version_) {
    inc_update(&checked_sys_version_, version);
    LOG_INFO("[SCHEMA_STORE] schema store update version",
             K_(tenant_id), K(version), K_(checked_sys_version));
  }
}

void ObSchemaStore::update_baseline_schema_version(int64_t version)
{
  if (version > baseline_schema_version_) {
    inc_update(&baseline_schema_version_, version);
    LOG_INFO("[SCHEMA_STORE] schema store update version",
             K_(tenant_id), K(version), K_(baseline_schema_version));
  }
}

void ObSchemaStore::update_consensus_version(int64_t version)
{
  if (version > consensus_version_) {
    inc_update(&consensus_version_, version);
    LOG_INFO("[SCHEMA_STORE] schema store update version",
             K_(tenant_id), K(version), K_(consensus_version));
  }
}

int ObSchemaStoreMap::init(int64_t bucket_num)
{
  UNUSED(bucket_num);
  return map_.init(TENANT_MAP_BUCKET_NUM, ObModIds::OB_SCHEMA_MGR_CACHE_MAP, OB_SERVER_TENANT_ID);
}

void ObSchemaStoreMap::destroy()
{
  common::ObLinkArray::Seg* seg = map_.head();
  int64_t idx = 0;
  void** p = NULL;
  while(NULL != (p = map_.next(seg, idx))) {
    ObSchemaStore* schema_store = (ObSchemaStore*)*p;
    if (NULL != schema_store) {
      schema_store->~ObSchemaStore();
      schema_store = NULL;
    }
  }
}

int ObSchemaStoreMap::create(const uint64_t tenant_id,
                             const int64_t init_version_count,
                             const int64_t init_version_count_for_liboblog)
{
  int ret = OB_SUCCESS;
  ObSchemaStore** slot = NULL;
  ObSchemaStore* store = NULL;
  // Schema memory has not been split for 500 tenants
  common::ObMemAttr mattr(OB_SERVER_TENANT_ID, ObModIds::OB_SCHEMA_MGR_CACHE_MAP); 
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (slot = (ObSchemaStore**)map_.locate(tenant_id, /*alloc*/true))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to locate tenant slot");
  } else if (NULL == (store = (ObSchemaStore*)ob_malloc(sizeof(*store), mattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for schema_store");
  } else if (NULL == new(store)ObSchemaStore()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("construct schema_store fail");
  } else if (OB_FAIL(store->init(tenant_id, init_version_count, init_version_count_for_liboblog))) {
    LOG_WARN("fail to init schema store", K(ret));
  } else {
    *slot = store;
  }
  if (OB_FAIL(ret)) {
    if (NULL != store) {
      store->~ObSchemaStore();
      ob_free(store);
    }
  }
  return ret;
}

const ObSchemaStore* ObSchemaStoreMap::get(uint64_t tenant_id) const
{
  return const_cast<ObSchemaStoreMap*>(this)->get(tenant_id);
}
ObSchemaStore* ObSchemaStoreMap::get(uint64_t tenant_id)
{
  ObSchemaStore** slot = (ObSchemaStore**)map_.locate(tenant_id, /*alloc*/false);
  return slot? *slot: NULL;
}

int ObSchemaStoreMap::get_all_tenant(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  common::ObLinkArray::Seg* seg = map_.head();
  int64_t idx = 0;
  void** p = NULL;
  while(OB_NOT_NULL(p = map_.next(seg, idx)) && OB_SUCC(ret)) {
    ObSchemaStore* schema_store = (ObSchemaStore*)*p;
    if (OB_NOT_NULL(schema_store)) {
      if (OB_FAIL(tenant_ids.push_back(schema_store->tenant_id_))) {
        LOG_WARN("fail to push tenant_id to tenant_ids", KR(ret), K(schema_store->tenant_id_));
      }
    }
  }
  return ret;
}
}; // end namespace schema
}; // end namespace share
}; // end namespace oceanbase
