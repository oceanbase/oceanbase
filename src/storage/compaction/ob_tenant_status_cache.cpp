//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_tenant_status_cache.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
using namespace share;
namespace compaction
{
/********************************************ObTenantStatusCache impl******************************************/
bool ObTenantStatusCache::is_skip_merge_tenant() const
{
  bool bret = true;
  if (IS_INIT) {
    const ObTenantRole::Role &role = MTL_GET_TENANT_ROLE_CACHE();
    // remote tenant OR finish restore inner_table tenant
    bret = is_remote_tenant_ || (during_restore_ && is_standby_tenant(role));
  }
  return bret;
}

int ObTenantStatusCache::during_restore(bool &during_restore) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    if (REACH_THREAD_TIME_INTERVAL(30_s)) {
      LOG_INFO("not refresh valid tenant status", KR(ret), KPC(this));
    }
  } else {
    during_restore = during_restore_;
  }
  return ret;
}

int ObTenantStatusCache::init_or_refresh()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
      if (OB_FAIL(inner_refresh_restore_status())) {
        LOG_WARN("failed to refresh tenant restore status ", KR(ret));
      } else if (!during_restore_ && OB_FAIL(inner_refresh_remote_tenant())) {
        if (OB_NEED_WAIT != ret) {
          LOG_WARN("failed to refresh remote tenant", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      IGNORE_RETURN refresh_data_version();
      is_inited_ = true;
    }
  } else {
    if (during_restore_) { // restore tenant may change later, need refresh
      IGNORE_RETURN inner_refresh_restore_status();
    }
    IGNORE_RETURN inner_refresh_remote_tenant();
    IGNORE_RETURN refresh_data_version();
  }
  return ret;
}

int ObTenantStatusCache::refresh_tenant_config(
    const bool enable_adaptive_compaction,
    const bool enable_adaptive_merge_schedule)
{
  ATOMIC_SET(&enable_adaptive_compaction_, enable_adaptive_compaction);
  ATOMIC_SET(&enable_adaptive_merge_schedule_, enable_adaptive_merge_schedule);
  return init_or_refresh();
}

int ObTenantStatusCache::inner_refresh_remote_tenant()
{
  int ret = OB_SUCCESS;
  ObRestoreDataMode restore_data_mode;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info loader is null", K(ret));
  } else if (OB_FAIL(tenant_info_loader->get_restore_data_mode(restore_data_mode))) {
    if (REACH_TIME_INTERVAL(30_s)) {
      LOG_WARN("get restore_data_mode failed", K(ret));
    }
  } else if (restore_data_mode.is_remote_mode()) {
    is_remote_tenant_ = true;
    LOG_INFO("tenant restore data mode is remote, should not loop tablet to schedule", K(ret), "tenant_id", MTL_ID());
  } else {
    is_remote_tenant_ = false;
  }
  return ret;
}

int ObTenantStatusCache::inner_refresh_restore_status()
{
  int ret = OB_SUCCESS;
  if (REACH_THREAD_TIME_INTERVAL(REFRESH_TENANT_STATUS_INTERVAL)) {
    ObSchemaGetterGuard schema_guard;
    const ObSimpleTenantSchema *tenant_schema = nullptr;
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tenant schema is null", K(ret));
    } else if (tenant_schema->is_restore()) {
      ATOMIC_SET(&during_restore_, true);
    } else {
      ATOMIC_SET(&during_restore_, false);
    }
  }
  return ret;
}

int ObTenantStatusCache::get_min_data_version(uint64_t &min_data_version)
{
  int ret = OB_SUCCESS;
  min_data_version = min_data_version_;
  if (0 == min_data_version) { // force call GET_MIN_DATA_VERSION
    uint64_t compat_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
      LOG_WARN("fail to get data version", KR(ret));
    } else {
      uint64_t old_version = min_data_version_;
      while (old_version < compat_version) {
        if (ATOMIC_BCAS(&min_data_version_, old_version, compat_version)) {
          // success to assign data version
          break;
        } else {
          old_version = min_data_version_;
        }
      } // end of while
    }
    if (OB_SUCC(ret)) {
      min_data_version = min_data_version_;
    }
  }
  return ret;
}

int ObTenantStatusCache::refresh_data_version()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  const uint64_t cached_data_version = min_data_version_;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN_RET(ret, "fail to get data version");
  } else if (OB_UNLIKELY(compat_version < cached_data_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data version is unexpected smaller", KR(ret), K(compat_version), K(cached_data_version));
  } else if (compat_version > cached_data_version) {
    ATOMIC_STORE(&min_data_version_, compat_version);
    LOG_INFO("cache min data version", "old_data_version", cached_data_version,
             "new_data_version", compat_version);
  }
  return ret;
}

bool ObTenantStatusCache::enable_adaptive_compaction_with_cpu_load() const
{
  bool bret = enable_adaptive_compaction_;
  if (!bret || !enable_adaptive_merge_schedule()) {
    // do nothing
#ifdef ENABLE_DEBUG_LOG
  } else if (GCONF.enable_crazy_medium_compaction) {
    bret = true;
    LOG_DEBUG("set crazy medium, set enable_adaptive_compaction = true");
#endif
  } else if (MTL(ObTenantTabletStatMgr *)->is_high_tenant_cpu_load()) {
    bret = false;
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      FLOG_INFO("disable adaptive compaction due to the high load CPU", K(bret));
    }
  }
  return bret;
}

} // namespace compaction
} // namespace oceanbase
