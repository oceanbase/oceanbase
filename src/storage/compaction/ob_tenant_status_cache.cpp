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
#include "storage/tx/ob_ts_mgr.h"
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

bool ObTenantStatusCache::is_skip_window_compaction_tenant() const
{
  bool bret = true;
  if (IS_INIT) {
    const ObTenantRole::Role &role = MTL_GET_TENANT_ROLE_CACHE();
    // remote tenant OR standby tenant
    bret = is_remote_tenant_ || is_standby_tenant(role);
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
    const bool enable_adaptive_merge_schedule,
    const bool enable_window_compaction)
{
  ATOMIC_SET(&enable_adaptive_compaction_, enable_adaptive_compaction);
  ATOMIC_SET(&enable_adaptive_merge_schedule_, enable_adaptive_merge_schedule);
  ATOMIC_SET(&enable_window_compaction_, enable_window_compaction);
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

// See ObMediumCompactionScheduleFunc::get_valid_schema_version: some sys tables don't update schema version when doing dml,
// so we need get the lastest schema to do window compaction in case of adding new column when upgrading.
int ObTenantStatusCache::inner_refresh_window_schema_version()
{
  int ret = OB_SUCCESS;
  int64_t current_max_schema_version = 0;
  ObSchemaService *schema_service = nullptr;
  const int64_t timeout_us = 10 * 1000 * 1000; /*10s*/
  SCN gts_scn;
  bool is_external_consistent = true;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = MTL_ID();
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service or sql proxy is null", K(ret));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(schema_status.tenant_id_, timeout_us, gts_scn, is_external_consistent))) {
    LOG_WARN("failed to get gts sync", K(ret));
  } else if (OB_UNLIKELY(!is_external_consistent || !gts_scn.is_valid_and_not_min())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gts is invalid", K(ret), K(gts_scn), K(is_external_consistent));
  } else if (FALSE_IT(schema_status.snapshot_timestamp_ = gts_scn.get_val_for_inner_table_field())) {
  } else if (OB_FAIL(schema_service->fetch_schema_version(schema_status, *GCTX.sql_proxy_, current_max_schema_version))) {
    LOG_WARN("failed to fetch schema version", K(ret), K(schema_status));
  } else {
    ATOMIC_STORE(&window_schema_version_, current_max_schema_version);
    FLOG_INFO("[WIN-COMPACTION] Successfully refreshed window schema info", K(ret), K(gts_scn), KPC(this));
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

int ObTenantStatusCache::get_window_schema_version(int64_t &window_schema_version)
{
  int ret = OB_SUCCESS;
  window_schema_version = ATOMIC_LOAD(&window_schema_version_);
  if (OB_INVALID_VERSION == window_schema_version) {
    uint64_t min_data_version = 0;
    if (OB_FAIL(get_min_data_version(min_data_version))) { // promise to get valid min data version for this cache
      LOG_WARN("failed to get min data version", KR(ret));
    } else if (0 == min_data_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min data version is 0", KR(ret), K(min_data_version));
    } else if (OB_FAIL(inner_refresh_window_schema_version())) {
      LOG_WARN("failed to refresh window schema version", KR(ret));
    } else {
      window_schema_version = ATOMIC_LOAD(&window_schema_version_);
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
    ATOMIC_STORE(&window_schema_version_, OB_INVALID_VERSION); // mark window schema version as invalid when min_data_version is changed
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
