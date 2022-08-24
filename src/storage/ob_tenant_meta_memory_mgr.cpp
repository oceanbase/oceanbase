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

#include "ob_tenant_meta_memory_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share::schema;
using namespace oceanbase::lib;

ObTenantMetaMemoryMgr::ObTenantMetaMemoryMgr() : tenant_meta_memory_map_(), is_inited_(false)
{}

ObTenantMetaMemoryMgr::~ObTenantMetaMemoryMgr()
{
  destroy();
}

void ObTenantMetaMemoryMgr::destroy()
{
  lib::ObLockGuard<lib::ObMutex> guard(lock_);
  tenant_meta_memory_map_.destroy();
  is_inited_ = false;
}

int ObTenantMetaMemoryMgr::try_update_tenant_info(const uint64_t tenant_id, const int64_t schema_version)
{
  lib::ObLockGuard<lib::ObMutex> guard(lock_);
  ObArenaAllocator allocator(ObModIds::OB_META_MEMORY_BUFFER);
  int ret = OB_SUCCESS;
  TenantInfo tenant_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemoryMgr has not been inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(tenant_meta_memory_map_.get_refactored(tenant_id, tenant_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant meta memory", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    const int64_t tenant_memory_limit = lib::get_tenant_memory_limit(tenant_id);
    int64_t memory_percentage = 0;
    // tenant_schema does not need to check full schema
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id, schema_guard))) {
      if (OB_TENANT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("tenant is not exist, skip this task", K(tenant_id), K(schema_version));
      } else {
        LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
      }
    } else if (OB_FAIL(
                   schema_guard.get_tenant_meta_reserved_memory_percentage(tenant_id, allocator, memory_percentage))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;  // maybe the tenant is dropped
      } else {
        LOG_WARN("fail to get tenant meta reversed memory percentage", K(ret));
      }
    } else {
      const int64_t reserved_memory_size = tenant_memory_limit * memory_percentage / 100;
      const int64_t long_term_meta_reserved_size = reserved_memory_size / 2;
      const int64_t short_term_meta_reserved_size = reserved_memory_size / 2;
      if (schema_version > tenant_info.schema_version_ ||
          (reserved_memory_size > 0 && reserved_memory_size != tenant_info.memory_size_)) {
        tenant_info.schema_version_ = std::max(schema_version, tenant_info.schema_version_);
        tenant_info.memory_size_ = reserved_memory_size;
        if (OB_FAIL(ObMallocAllocator::get_instance()->set_tenant_ctx_idle(
                tenant_id, ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID, long_term_meta_reserved_size))) {
          if (OB_TENANT_NOT_EXIST != ret) {
            LOG_WARN("fail to set tenant ctx idle", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(ObMallocAllocator::get_instance()->set_tenant_ctx_idle(
                       tenant_id, ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID, short_term_meta_reserved_size))) {
          if (OB_TENANT_NOT_EXIST != ret) {
            LOG_WARN("fail to set tenant ctx idle", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(tenant_meta_memory_map_.set_refactored(tenant_id, tenant_info, true /*overwrite*/))) {
          LOG_WARN("fail to update tenant meta memory map", K(ret));
        } else {
          LOG_INFO("succ to update tenant meta reserved memory size",
              K(tenant_id),
              K(reserved_memory_size),
              K(long_term_meta_reserved_size),
              K(short_term_meta_reserved_size));
        }
      }
    }
  }

  return ret;
}

int ObTenantMetaMemoryMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantMetaMemoryMgr has been inited twice", K(ret));
  } else if (OB_FAIL(tenant_meta_memory_map_.create(DEFAULT_BUCKET_NUM, ObModIds::OB_META_MEMORY_MGR))) {
    LOG_WARN("fail to create map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObTenantMetaMemoryMgr& ObTenantMetaMemoryMgr::get_instance()
{
  static ObTenantMetaMemoryMgr instance;
  return instance;
}
