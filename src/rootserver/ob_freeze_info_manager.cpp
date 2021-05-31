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

#include "ob_freeze_info_manager.h"
#include "ob_root_service.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/utility.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_global_stat_proxy.h"
#include "rootserver/ob_zone_manager.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_global_stat_proxy.h"
#include "ob_snapshot_info_manager.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "storage/transaction/ob_weak_read_util.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;
////////////////////////////////////////////
// gc_snapshot should be less than frozen snapshot. major_snapshot and gc_snapshot should
// satisfy following conditions:
// 1. major_snapshot > gc_snapshot,  major_schema_version >= gc_schema_version;
// 2. gc_snapshot1 > gc_snapshot2, gc_schema_version1 >= gc_schema_version2
///////////////////////////////////////////
namespace rootserver {
void ObFreezeInfoManager::FreezeInfo::reset()
{
  is_loaded_ = false;
  fast_loaded_ = false;
  current_frozen_status_.reset();
  frozen_status_without_schema_version_.reset();
  frozen_schema_versions_.reset();
  latest_snapshot_gc_ts_ = 0;
  latest_snapshot_gc_schema_versions_.reset();
  latest_defined_frozen_version_ = 0;
}

void ObFreezeInfoManager::FreezeInfo::limited_reset()
{
  is_loaded_ = false;
  frozen_status_without_schema_version_.reset();
  frozen_schema_versions_.reset();
  latest_snapshot_gc_ts_ = 0;
  latest_snapshot_gc_schema_versions_.reset();
  latest_defined_frozen_version_ = 0;
}

int ObFreezeInfoManager::FreezeInfo::set_freeze_info_for_bootstrap(
    const int64_t frozen_version, const int64_t frozen_timestamp, const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  is_loaded_ = false;
  current_frozen_status_.frozen_version_ = frozen_version;
  current_frozen_status_.frozen_timestamp_ = frozen_timestamp;
  current_frozen_status_.cluster_version_ = cluster_version;
  if (OB_FAIL(frozen_schema_versions_.push_back(TenantIdAndSchemaVersion(OB_SYS_TENANT_ID, ORIGIN_SCHEMA_VERSION)))) {
    LOG_WARN("fail to push back", KR(ret));
  } else {
    latest_defined_frozen_version_ = frozen_version;
    LOG_INFO("reload freeze info success",
        K(current_frozen_status_),
        K(frozen_schema_versions_),
        K(frozen_status_without_schema_version_),
        K(latest_snapshot_gc_ts_),
        K(latest_snapshot_gc_schema_versions_),
        K(latest_defined_frozen_version_));
    is_loaded_ = true;
    fast_loaded_ = true;
  }
  return ret;
}

int ObFreezeInfoManager::FreezeInfo::set_current_frozen_status(const int64_t frozen_version,
    const int64_t frozen_timestamp, const int64_t cluster_version,
    const ObIArray<TenantIdAndSchemaVersion>& tenant_schemas)
{
  frozen_schema_versions_.reset();
  int ret = OB_SUCCESS;
  if (0 >= frozen_version || 0 >= frozen_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_version), K(frozen_timestamp));
  } else if (fast_loaded_ && frozen_version < current_frozen_status_.frozen_version_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_version), K(frozen_timestamp), K(current_frozen_status_));
  } else if (OB_FAIL(frozen_schema_versions_.assign(tenant_schemas))) {
    LOG_WARN("fail to assign frozen schema versions", KR(ret));
  } else {
    current_frozen_status_.frozen_version_ = frozen_version;
    current_frozen_status_.frozen_timestamp_ = frozen_timestamp;
    current_frozen_status_.cluster_version_ = cluster_version;
    fast_loaded_ = true;
    LOG_INFO("set current frozen status success",
        K(frozen_version),
        K(frozen_timestamp),
        K(cluster_version),
        K(tenant_schemas));
  }
  return ret;
}

bool ObFreezeInfoManager::FreezeInfo::is_valid() const
{
  return fast_loaded_ && is_loaded_;
}

int ObFreezeInfoManager::FreezeInfo::assign(const FreezeInfo& other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else {
    is_loaded_ = other.is_loaded_;
    fast_loaded_ = other.fast_loaded_;
    current_frozen_status_ = other.current_frozen_status_;
    latest_snapshot_gc_ts_ = other.latest_snapshot_gc_ts_;
    latest_defined_frozen_version_ = other.latest_defined_frozen_version_;
    if (OB_FAIL(frozen_schema_versions_.assign(other.frozen_schema_versions_))) {
      LOG_WARN("fail to assign", KR(ret));
    } else if (OB_FAIL(frozen_status_without_schema_version_.assign(other.frozen_status_without_schema_version_))) {
      LOG_WARN("fail to assign", KR(ret));
    } else if (OB_FAIL(latest_snapshot_gc_schema_versions_.assign(other.latest_snapshot_gc_schema_versions_))) {
      LOG_WARN("fail to assign", KR(ret));
    }
  }
  return ret;
}

/////////////////////////////////
int ObFreezeInfoManager::init(
    common::ObMySQLProxy* proxy, ObZoneManager* zone_manager, obrpc::ObCommonRpcProxy* common_rpc, ObRootService& rs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(proxy) || OB_ISNULL(zone_manager) || OB_ISNULL(common_rpc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(proxy), K(zone_manager), K(common_rpc));
  } else {
    is_inited_ = true;
    sql_proxy_ = proxy;
    zone_manager_ = zone_manager;
    common_rpc_proxy_ = common_rpc;
    rs_ = &rs;
    reset_freeze_info();
    LOG_INFO("Init ObFreezeInfoManager succeed", K(ret));
  }
  return ret;
}

void ObFreezeInfoManager::reset_freeze_info()
{
  SpinWLockGuard guard(freeze_info_lock_);
  freeze_info_.reset();
}

void ObFreezeInfoManager::set_freeze_info_invalid()
{
  LOG_INFO("set freeze info invalid, need reload");
  SpinWLockGuard guard(freeze_info_lock_);
  freeze_info_.set_invalid();
}

void ObFreezeInfoManager::destroy()
{
  ObSpinLockGuard guard(update_lock_);
  is_inited_ = false;
  reset_freeze_info();
}

int ObFreezeInfoManager::load_frozen_status()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_lock_);
  FreezeInfo tmp_freeze_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFreezeInfoManager not init", K(ret));
  } else if (is_freeze_info_valid()) {
    // nothing todo
  } else {
    ObFrozenStatus frozen_status;
    int64_t input_frozen_version = 0;
    ObArray<TenantIdAndSchemaVersion> schemas;
    if (OB_FAIL(freeze_info_proxy_.get_freeze_info_inner(*sql_proxy_, input_frozen_version, frozen_status))) {
      LOG_WARN("fail to get freeze info", K(ret));
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("reset return_code to success in bootstrap", K(ret));
        // a bootstrap or a restart with no major freeze has ever happend
        ret = OB_SUCCESS;
        if (OB_FAIL(tmp_freeze_info.set_current_frozen_status(
                ORIGIN_FROZEN_VERSION, ORIGIN_FROZEN_TIMESTAMP, GET_MIN_CLUSTER_VERSION(), schemas))) {
          LOG_WARN("fail to set current frozen status", KR(ret));
        }
      } else {
        LOG_WARN("fail to get freeze info", K(ret));
      }
    } else if (!frozen_status.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid frozen status", K(ret), K(frozen_status));
    } else {
      if (OB_FAIL(tmp_freeze_info.set_current_frozen_status(frozen_status.frozen_version_,
              frozen_status.frozen_timestamp_,
              frozen_status.cluster_version_,
              schemas))) {
        LOG_WARN("fail to set current frozen status", KR(ret), K(frozen_status));
      }
    }
  }
  if (OB_SUCC(ret)) {
    tmp_freeze_info.fast_loaded_ = true;
    if (OB_FAIL(set_simple_freeze_info(tmp_freeze_info))) {
      LOG_WARN("fail to assign", KR(ret), K(tmp_freeze_info));
    }
  }
  return ret;
}

bool ObFreezeInfoManager::is_freeze_info_valid() const
{
  bool bret = false;
  SpinRLockGuard guard(freeze_info_lock_);
  bret = freeze_info_.is_valid();
  return bret;
}

int ObFreezeInfoManager::try_reload()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_lock_);
  FreezeInfo freeze_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFreezeInfoManager not init", K(ret));
  } else if (is_freeze_info_valid()) {
    // nothing todo
  } else {
    LOG_INFO("invalid freeze info in memory, need reload");
    if (OB_FAIL(inner_reload(freeze_info))) {
      LOG_WARN("fail to reload freeze info", KR(ret));
    } else if (!freeze_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid freeze info", KR(ret), K(freeze_info));
    } else if (OB_FAIL(set_freeze_info(freeze_info))) {
      LOG_WARN("fail to assign", KR(ret), K(freeze_info));
    } else {
      LOG_INFO("try reload success", K(freeze_info));
    }
  }
  return ret;
}

int ObFreezeInfoManager::reload()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_lock_);
  FreezeInfo freeze_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFreezeInfoManager not init", K(ret));
  } else if (OB_FAIL(inner_reload(freeze_info))) {
    LOG_WARN("fail to reload freeze info", KR(ret));
  } else if (!freeze_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid freeze info", KR(ret), K(freeze_info));
  } else if (OB_FAIL(set_freeze_info(freeze_info))) {
    LOG_WARN("fail to assign", KR(ret), K(freeze_info));
  } else {
    LOG_INFO("reload success", K(freeze_info));
  }
  return ret;
}

int ObFreezeInfoManager::set_simple_freeze_info(const FreezeInfo& freeze_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard freeze_info_guard(freeze_info_lock_);
  if (!freeze_info.fast_loaded_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(freeze_info));
  } else {
    freeze_info_.reset();
    freeze_info_.current_frozen_status_ = freeze_info.current_frozen_status_;
    freeze_info_.fast_loaded_ = true;
    LOG_INFO("set new freeze info", K(freeze_info));
  }
  return ret;
}

int ObFreezeInfoManager::set_freeze_info(const FreezeInfo& freeze_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard freeze_info_guard(freeze_info_lock_);
  if (!freeze_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(freeze_info));
  } else if (OB_FAIL(freeze_info_.assign(freeze_info))) {
    LOG_WARN("fail to set freeze info, need reload", KR(ret));
    freeze_info_.set_invalid();
  } else {
    LOG_INFO("set new freeze info", K(freeze_info));
  }
  return ret;
}

int ObFreezeInfoManager::get_freeze_info_shadow(FreezeInfo& freeze_info) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard freeze_info_guard(freeze_info_lock_);
  if (!freeze_info_.is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K_(freeze_info));
  } else if (OB_FAIL(freeze_info.assign(freeze_info_))) {
    LOG_WARN("fail to assign", KR(ret));
  }
  return ret;
}

// reload will acquire latest freeze info from __all_freeze_info.
// If freeze info is empty, cluster should be in bootstrap or cluster never major freeze.
// At this time, current_frozen_status_ should fill in frozen_status of fronzen version 1.
int ObFreezeInfoManager::inner_reload(FreezeInfo& freeze_info)
{
  int ret = OB_SUCCESS;
  freeze_info.reset();
  bool is_bootstrap = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFreezeInfoManager not init", K(ret));
  } else {
    ObSimpleFrozenStatus simple_frozen_status;
    int64_t input_frozen_version = 0;
    ObGlobalStatProxy global_stat_proxy(*sql_proxy_);
    // step 1: acquire latest freeze info
    if (OB_FAIL(freeze_info_proxy_.get_freeze_info(*sql_proxy_, input_frozen_version, simple_frozen_status))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("reset return_code to success in bootstrap", K(ret));
        // a bootstrap or a restart with no major freeze has ever happend
        ret = OB_SUCCESS;
        if (OB_FAIL(freeze_info.set_freeze_info_for_bootstrap(
                ORIGIN_FROZEN_VERSION, ORIGIN_FROZEN_TIMESTAMP, GET_MIN_CLUSTER_VERSION()))) {
          LOG_WARN("fail to set freeze info for bootstrap", KR(ret), "cluster_version", GET_MIN_CLUSTER_VERSION());
        }
      } else {
        LOG_WARN("fail to get freeze info", K(ret));
      }
    } else if (!simple_frozen_status.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid frozen status", K(ret), K(simple_frozen_status));
    } else if (OB_FAIL(rs_->get_is_in_bootstrap(is_bootstrap))) {
      LOG_WARN("failed to get is in bootstrap", KR(ret));
    } else if (is_bootstrap) {
      if (OB_FAIL(freeze_info.set_freeze_info_for_bootstrap(simple_frozen_status.frozen_version_,
              simple_frozen_status.frozen_timestamp_,
              simple_frozen_status.cluster_version_))) {
        LOG_WARN("fail to set freeze info for bootstrap", KR(ret), K(simple_frozen_status));
      }
    } else {
      // normal reload
      ObFrozenStatus frozen_status;
      if (OB_FAIL(freeze_info_proxy_.get_freeze_info_v2(
              *sql_proxy_, input_frozen_version, frozen_status, freeze_info.frozen_schema_versions_))) {
        LOG_WARN("fail to get freeze info", K(ret));
      } else if (!frozen_status.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid frozen status", K(ret), K(frozen_status));
      } else {
        freeze_info.current_frozen_status_ = frozen_status;
      }
      // step 2: get freeze info which need set schema_version
      ObSimpleFrozenStatus frozen_info;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(freeze_info_proxy_.get_frozen_status_without_schema_version_v2(
                     *sql_proxy_, freeze_info.frozen_status_without_schema_version_))) {
        LOG_WARN("fail to get freeze info", K(ret));
      } else if (OB_FAIL(freeze_info_proxy_.get_max_frozen_status_with_schema_version_v2(*sql_proxy_, frozen_info))) {
        LOG_WARN("fail to get frozen info", KR(ret));
      } else {
        freeze_info.latest_defined_frozen_version_ = frozen_info.frozen_version_;
      }
      // step 3: get snapshot_gc info
      uint64_t tenant_id = 0;
      bool need_gc_schema_version = (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2230);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(global_stat_proxy.get_snapshot_info_v2(*sql_proxy_,
                     tenant_id,
                     need_gc_schema_version,
                     freeze_info.latest_snapshot_gc_ts_,
                     freeze_info.latest_snapshot_gc_schema_versions_))) {
        LOG_WARN("fail to get snapshot info", KR(ret));
      }
      if (OB_SUCC(ret)) {
        freeze_info.is_loaded_ = true;
        freeze_info.fast_loaded_ = true;
      } else {
        freeze_info.set_invalid();
      }
    }
  }
  return ret;
}

void ObFreezeInfoManager::unload()
{
  ObSpinLockGuard guard(update_lock_);
  SpinWLockGuard freeze_info_guard(freeze_info_lock_);
  freeze_info_.reset();
}

int ObFreezeInfoManager::get_max_frozen_version(int64_t& max_frozen_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_proxy", K(ret));
  } else {
    ObGlobalStatProxy global_stat_proxy(*sql_proxy_);
    int64_t frozen_version = 0;
    int64_t frozen_time = 0;
    if (OB_FAIL(global_stat_proxy.get_frozen_info(max_frozen_version))) {
      LOG_WARN("fail to get freeze info", K(ret));
    } else if (OB_FAIL(zone_manager_->get_frozen_info(frozen_version, frozen_time))) {  // for compat
      LOG_WARN("fail to get frozen info", K(ret));
    } else if (max_frozen_version < frozen_version) {
      max_frozen_version = frozen_version;
    }
  }
  return ret;
}

int ObFreezeInfoManager::get_freeze_info(const int64_t frozen_version, share::ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  FreezeInfo tmp_freeze_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (1 == frozen_version) {
    frozen_status.frozen_version_ = ORIGIN_FROZEN_VERSION;
    frozen_status.frozen_timestamp_ = ORIGIN_FROZEN_TIMESTAMP;
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get freeze info shadow", KR(ret));
  } else if (tmp_freeze_info.current_frozen_status_.is_valid() &&
             (frozen_version == tmp_freeze_info.current_frozen_status_.frozen_version_ || 0 == frozen_version)) {
    frozen_status = tmp_freeze_info.current_frozen_status_;
  } else if (OB_FAIL(freeze_info_proxy_.get_freeze_info(*sql_proxy_, frozen_version, frozen_status))) {
    LOG_WARN("fail to get freeze info", K(ret), K(frozen_version));
  } else if (frozen_status.frozen_version_ > tmp_freeze_info.current_frozen_status_.frozen_version_) {
    LOG_ERROR("get frozen version",
        "lastest_version_in_memory",
        tmp_freeze_info.current_frozen_status_.frozen_version_,
        "lastest_version_in_table",
        frozen_status.frozen_version_);
  }
  return ret;
}

int ObFreezeInfoManager::get_freeze_schema_versions(
    const int64_t tenant_id, const int64_t frozen_version, ObIArray<TenantIdAndSchemaVersion>& schema_versions)
{
  int ret = OB_SUCCESS;
  schema_versions.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (frozen_version <= 0 || tenant_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_version), K(tenant_id));
  } else if (OB_FAIL(get_schema_from_cache(tenant_id, frozen_version, schema_versions))) {
    LOG_WARN("fail to get from cache", KR(ret), K(tenant_id), K(frozen_version));
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    if (OB_FAIL(freeze_info_proxy_.get_freeze_schema_v2(*sql_proxy_, tenant_id, frozen_version, schema_versions))) {
      LOG_WARN("fail to get freeze schema", KR(ret), K(tenant_id), K(frozen_version));
    }
  }
  return ret;
}

int ObFreezeInfoManager::get_schema_from_cache(
    const int64_t tenant_id, const int64_t frozen_version, ObIArray<TenantIdAndSchemaVersion>& schema_versions)
{
  int ret = OB_SUCCESS;
  FreezeInfo tmp_freeze_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (tenant_id < 0 || frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(frozen_version));
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get shadow", KR(ret));
  } else {
    ObIArray<TenantIdAndSchemaVersion>& frozen_schema_versions = tmp_freeze_info.frozen_schema_versions_;
    if (frozen_schema_versions.count() <= 0 ||
        frozen_version != tmp_freeze_info.current_frozen_status_.frozen_version_) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("frozen schema is empty", KR(ret), K(frozen_schema_versions));
    } else if (tenant_id == 0) {
      if (OB_FAIL(schema_versions.assign(frozen_schema_versions))) {
        LOG_WARN("fail to assign schema version", KR(ret));
      }
    } else {
      for (int64_t i = 0; i < frozen_schema_versions.count() && OB_SUCC(ret); i++) {
        if (tenant_id == frozen_schema_versions.at(i).tenant_id_) {
          TenantIdAndSchemaVersion schema_info;
          schema_info.tenant_id_ = tenant_id;
          schema_info.schema_version_ = frozen_schema_versions.at(i).schema_version_;
          if (OB_FAIL(schema_versions.push_back(schema_info))) {
            LOG_WARN("fail to push back", KR(ret));
          }
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret) && schema_versions.count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("get invalid schema versions", KR(ret), K(schema_versions));
  }
  return ret;
}

int ObFreezeInfoManager::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret), K_(is_inited));
  }
  return ret;
}

// reserve one major snapshot
int ObFreezeInfoManager::set_freeze_info(
    const int64_t frozen_version, const int64_t tenant_id, const ObAddr& server_addr, const ObAddr& rs_addr)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  ObMySQLTransaction trans;
  ObGlobalStatProxy global_stat_proxy(trans);
  ObSpinLockGuard guard(update_lock_);
  FreezeInfo tmp_freeze_info;
  int64_t snapshot_gc_ts_in_inner_table = 0;
  ObSnapshotTableProxy snapshot_proxy;
  ObSnapshotInfo snapshot_info;
  uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get shadow", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(global_stat_proxy.select_gc_timestamp_for_update(trans, snapshot_gc_ts_in_inner_table))) {
    LOG_WARN("fail to select for update", KR(ret));
  } else if (OB_FAIL(snapshot_proxy.get_max_snapshot_info(*sql_proxy_, snapshot_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // nothing todo
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get max snapshot info", KR(ret));
    }
  }
  storage::ObFrozenStatus frozen_status;
  if (OB_FAIL(ret)) {
  } else {
    const int64_t MAJOR_FREEZE_INTERVAL = 10 * 1000L * 1000L;  // TODO: use config later
    int64_t major_snapshot = ObTimeUtility::current_time() + MAJOR_FREEZE_INTERVAL;
    int64_t retry_time = 0;
    while (((major_snapshot <= snapshot_gc_ts_in_inner_table) ||
               (major_snapshot <= tmp_freeze_info.current_frozen_status_.frozen_timestamp_) ||
               (major_snapshot <= snapshot_info.snapshot_ts_)) &&
           (retry_time < 3)) {
      major_snapshot += MAJOR_FREEZE_INTERVAL;
      retry_time++;
    }
    frozen_status.frozen_version_ = frozen_version;
    frozen_status.status_ = ORIGIN_FREEZE_STATUS;
    frozen_status.frozen_timestamp_ = major_snapshot;
    // cluster version should not rollback
    if (CLUSTER_VERSION_2276 <= cluster_version || 0 != tmp_freeze_info.current_frozen_status_.cluster_version_) {
      frozen_status.cluster_version_ = max(cluster_version, tmp_freeze_info.current_frozen_status_.cluster_version_);
    }
    if (major_snapshot <= snapshot_gc_ts_in_inner_table ||
        major_snapshot <= tmp_freeze_info.current_frozen_status_.frozen_timestamp_ ||
        major_snapshot <= snapshot_info.snapshot_ts_) {
      ret = OB_EAGAIN;
      LOG_WARN("get invalid major snapshot, try again",
          K(ret),
          K(snapshot_gc_ts_in_inner_table),
          K(major_snapshot),
          K(tmp_freeze_info.current_frozen_status_),
          K(snapshot_info));
    } else if (OB_FAIL(global_stat_proxy.set_frozen_version(frozen_version))) {
      LOG_WARN("fail to set frozen version", K(ret), K(frozen_version));
    } else if (OB_FAIL(freeze_info_proxy_.set_freeze_info(trans, frozen_status))) {
      LOG_WARN("fail to set freeze info", K(ret), K(frozen_status), K(tenant_id), K(server_addr), K(rs_addr));
    }
  }
  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
      LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tmp_freeze_info.frozen_status_without_schema_version_.push_back(frozen_status))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      tmp_freeze_info.current_frozen_status_ = frozen_status;
      tmp_freeze_info.frozen_schema_versions_.reset();
      if (OB_FAIL(set_freeze_info(tmp_freeze_info))) {
        LOG_WARN("fail to assign", KR(ret), K(tmp_freeze_info));
      } else {
        LOG_INFO("set new freeze info success", K(tmp_freeze_info.current_frozen_status_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_SET_FREEZE_INFO_FAILED) OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    set_freeze_info_invalid();
  }

  LOG_INFO("finish set freeze info", K(ret), K(frozen_version), K(tenant_id), K(server_addr), K(rs_addr));
  ROOTSERVICE_EVENT_ADD(
      "root_service", "root_major_freeze", K(ret), K(frozen_version), K(tenant_id), K(server_addr), K(rs_addr));
  return ret;
}

int ObFreezeInfoManager::broadcast_frozen_info()
{
  int ret = OB_SUCCESS;
  share::ObSimpleFrozenStatus frozen_status;
  if (OB_FAIL(get_freeze_info(0, frozen_status))) {
    LOG_WARN("fail to get freeze info", K(ret));
  } else if (OB_FAIL(zone_manager_->set_frozen_info(frozen_status.frozen_version_, frozen_status.frozen_timestamp_))) {
    LOG_WARN("fail to set frozen info", K(ret), K(frozen_status));
  }
  return ret;
}

// Ignore tenant of error schema to isolate tenants minor merge.
// This function will fix tenant schema version.
int ObFreezeInfoManager::process_invalid_schema_version()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_lock_);
  int bak_ret = OB_SUCCESS;
  int64_t begin_version = 0;
  int64_t end_version = 0;
  FreezeInfo freeze_info;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(zone_manager_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid zone manager", KR(ret));
  } else if (OB_FAIL(get_freeze_info_shadow(freeze_info))) {
    LOG_WARN("fail to get freeze info shadow", KR(ret));
  } else if (FALSE_IT(end_version = freeze_info.latest_defined_frozen_version_)) {
    // nothing todo
  } else if (OB_FAIL(zone_manager_->get_global_broadcast_version(begin_version))) {
    LOG_WARN("fail to get global broadcast version", KR(ret));
  } else {
    storage::ObFrozenStatus frozen_status;
    ObArray<TenantIdAndSchemaVersion> frozen_schemas;
    for (int64_t i = begin_version + 1; i <= end_version && OB_SUCC(ret); i++) {
      int64_t version = i;
      frozen_status.reset();
      frozen_schemas.reset();
      if (OB_FAIL(freeze_info_proxy_.get_freeze_info_v2(*sql_proxy_, version, frozen_status, frozen_schemas))) {
        LOG_WARN("fail to get freeze info", KR(ret), K(version));
        if (OB_SUCC(bak_ret)) {
          bak_ret = ret;
        }
        ret = OB_SUCCESS;
      } else {
        for (int64_t j = 0; j < frozen_schemas.count() && OB_SUCC(ret); j++) {
          if (OB_INVALID_SCHEMA_VERSION == frozen_schemas.at(j).schema_version_) {
            if (OB_FAIL(fix_tenant_schema_version(version, frozen_schemas.at(j).tenant_id_))) {
              LOG_WARN("fail to update tenant schema version", K(ret), "tenant_id", frozen_schemas.at(j).tenant_id_);
              if (OB_SUCC(bak_ret)) {
                bak_ret = ret;
              }
              ret = OB_SUCCESS;
            }
          }
        }  // end for j
      }    // end else
    }      // end for i
  }
  if (OB_SUCC(ret)) {
    ret = bak_ret;
  }
  return ret;
}

int ObFreezeInfoManager::fix_tenant_schema_version(const int64_t frozen_version, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_FIX_TENANT_SCHEMA_VERSION);
  obrpc::ObGetSchemaArg arg;
  arg.ignore_fail_ = true;
  ObTenantSchemaVersions tenant_schema;
  ObSchemaGetterGuard schema_guard;
  bool is_dropped = false;
  if (frozen_version <= 0 || tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_version), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("fail to check tenant has been dropped", KR(ret), K(tenant_id));
  } else if (is_dropped) {
    // Tenant is alread dropped, schema_version will be update to 1.
    if (OB_FAIL(freeze_info_proxy_.fix_tenant_schema(*sql_proxy_, frozen_version, tenant_id, OB_CORE_SCHEMA_VERSION))) {
      LOG_WARN("fail to fix schema for dropped tenant", KR(ret), K(frozen_version), K(tenant_id));
      // set invalid to trigger reload
      set_freeze_info_invalid();
    } else {
      LOG_INFO("fix tenant schema version success for dropped tenant", K(tenant_id), K(frozen_version));
      // set invalid to trigger reload
      set_freeze_info_invalid();
    }
  } else if (OB_FAIL(common_rpc_proxy_->get_tenant_schema_versions(arg, tenant_schema))) {
    LOG_WARN("fail to get tenant schema version", KR(ret));
  } else {
    ObIArray<share::TenantIdAndSchemaVersion>& tenant_schema_versions = tenant_schema.tenant_schema_versions_;
    for (int64_t i = 0; i < tenant_schema_versions.count() && OB_SUCC(ret); i++) {
      if (tenant_id != tenant_schema_versions.at(i).tenant_id_) {
        // nothing todo
      } else if (OB_INVALID_SCHEMA_VERSION == tenant_schema_versions.at(i).schema_version_) {
        ret = OB_EAGAIN;
        break;
      } else if (OB_FAIL(freeze_info_proxy_.fix_tenant_schema(*sql_proxy_,
                     frozen_version,
                     tenant_schema_versions.at(i).tenant_id_,
                     tenant_schema_versions.at(i).schema_version_))) {
        LOG_WARN("fail to fix tenant schema", KR(ret), K(frozen_version), K(tenant_schema_versions));
        // set invalid to trigger reload
        set_freeze_info_invalid();
        break;
      } else {
        LOG_WARN("fix tenant schema version success",
            K(ret),
            K(frozen_version),
            "schema",
            tenant_schema_versions.at(i).schema_version_);
        // set invalid to trigger reload
        set_freeze_info_invalid();
        break;
      }
    }
  }
  return ret;
}

int ObFreezeInfoManager::try_update_major_schema_version()
{
  ObSpinLockGuard guard(update_lock_);
  return inner_update_major_schema_version();
}

int ObFreezeInfoManager::inner_update_major_schema_version()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  bool need_process = true;
  storage::ObFrozenStatus status;
  ObSimpleFrozenStatus max_frozen_status_with_schema;
  FreezeInfo tmp_freeze_info;
  if (OB_FAIL(get_max_frozen_status_for_daily_merge(max_frozen_status_with_schema))) {
    LOG_WARN("fail to get max frozen status", KR(ret));
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get shadow", K(ret));
  } else {
    if (tmp_freeze_info.frozen_status_without_schema_version_.count() <= 0) {
      need_process = false;
    } else if (now < (tmp_freeze_info.frozen_status_without_schema_version_.at(0).frozen_timestamp_ +
                         UPDATE_SCHEMA_ADDITIONAL_INTERVAL)) {
      need_process = false;
    } else if (max_frozen_status_with_schema.frozen_version_ + 1 <
               tmp_freeze_info.frozen_status_without_schema_version_.at(0).frozen_version_) {
      // max_frozen_status_with_schema.frozen_version_ is max frozen version with legal schema.
      // We should not skip illegal version.
      need_process = false;
    } else {
      status = tmp_freeze_info.frozen_status_without_schema_version_.at(0);
      need_process = true;
    }
  }
  if (OB_SUCC(ret) && need_process) {
    // Use DDL to get latest schema_version
    int64_t frozen_version = status.frozen_version_;
    obrpc::ObTenantSchemaVersions tenant_schema_versions;
    if (frozen_version <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid frozen version", KR(ret), K(status));
    } else if (OB_FAIL(common_rpc_proxy_->update_freeze_schema_version(frozen_version, tenant_schema_versions))) {
      LOG_WARN("fail to update freze schema version", KR(ret));
      // set invalid to trigger reload
      set_freeze_info_invalid();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tmp_freeze_info.frozen_status_without_schema_version_.remove(0))) {
        LOG_WARN("fail to remove", K(ret));
      } else if (0 == tmp_freeze_info.frozen_status_without_schema_version_.count() &&
                 OB_FAIL(
                     tmp_freeze_info.frozen_schema_versions_.assign(tenant_schema_versions.tenant_schema_versions_))) {
        LOG_WARN("fail to assign schema versions", KR(ret), K(tenant_schema_versions));
      } else if (0 != tmp_freeze_info.frozen_status_without_schema_version_.count() &&
                 FALSE_IT(tmp_freeze_info.frozen_schema_versions_.reset())) {
        // nothing todo
      } else {
        tmp_freeze_info.latest_defined_frozen_version_ = status.frozen_version_;
        if (OB_FAIL(set_freeze_info(tmp_freeze_info))) {
          LOG_WARN("fail to assign", KR(ret), K(tmp_freeze_info));
        } else {
          LOG_INFO("update frozen schema success", "freeze_info", tmp_freeze_info);
        }
      }
      if (OB_SUCC(ret)) {
        ret = E(EventTable::EN_UPDATE_MAJOR_SCHEMA_FAIL) OB_SUCCESS;
      }

      if (OB_FAIL(ret)) {
        set_freeze_info_invalid();
      }
    }
  }
  return ret;
}

int ObFreezeInfoManager::gc_freeze_info()
{
  int ret = OB_SUCCESS;
  int64_t global_broadcast_version = 0;
  int64_t now = ObTimeUtility::current_time();
  const int64_t MAX_KEEP_INTERVAL = 30 * 24 * 60 * 60 * 1000L * 1000L;
  const int64_t MIN_REMAINED_VERSION_COUNT = 32;
  int64_t min_frozen_timestamp = now - MAX_KEEP_INTERVAL;
  if (OB_ISNULL(zone_manager_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(zone_manager_), K(sql_proxy_));
  } else if (OB_FAIL(zone_manager_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("fail to get global broadcast version", K(ret));
  } else if (global_broadcast_version <= MIN_REMAINED_VERSION_COUNT) {
    // nothing toodo
  } else {
    int64_t min_frozen_version = global_broadcast_version - MIN_REMAINED_VERSION_COUNT;
    if (OB_FAIL(freeze_info_proxy_.batch_delete(*sql_proxy_, min_frozen_timestamp, min_frozen_version))) {
      LOG_WARN(
          "fail to batch delete", K(ret), K(min_frozen_timestamp), K(global_broadcast_version), K(min_frozen_version));
    }
  }
  return ret;
}

int ObFreezeInfoManager::renew_snapshot_gc_ts()
{
  int ret = OB_SUCCESS;
  int64_t new_snapshot_gc_ts =
      ObTimeUtility::current_time() - transaction::ObWeakReadUtil::default_max_stale_time_for_weak_consistency();
  ObSpinLockGuard guard(update_lock_);
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2230) {
    if (OB_FAIL(renew_snapshot_gc_ts_223(new_snapshot_gc_ts))) {
      LOG_WARN("fail to renew snapshot gc ts", K(ret));
    }
  } else if (OB_FAIL(renew_snapshot_gc_ts_less_2230())) {
    LOG_WARN("fail to renew snapshot gc ts", KR(ret));
  }
  return ret;
}

int ObFreezeInfoManager::renew_snapshot_gc_ts_less_2230()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  FreezeInfo freeze_info;
  ObGlobalStatProxy global_stat_proxy(*sql_proxy_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema service", K(ret));
  } else if (OB_FAIL(get_freeze_info_shadow(freeze_info))) {
    LOG_WARN("fail to get freeze info", KR(ret));
  } else {
    obrpc::ObTenantSchemaVersions tenant_schema_versions;
    obrpc::ObGetSchemaArg arg;
    arg.ignore_fail_ = true;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(common_rpc_proxy_->get_tenant_schema_versions(arg, tenant_schema_versions))) {
      LOG_WARN("fail to get tenant schema version", KR(ret));
    } else {
      // Primary cluster snapshot_gc_ts and schema_version is in order.
      // Primary cluster snapshot_gc_ts and freeze info should be in order.
      if (now <= freeze_info.latest_snapshot_gc_ts_) {
        ret = OB_EAGAIN;
        LOG_WARN("invalid snaptshot gc time, retry", K(ret));
      } else if (0 == freeze_info.frozen_schema_versions_.count()) {
        // nothing todo
      } else if (now > freeze_info.current_frozen_status_.frozen_timestamp_) {
        if (!is_versions_in_order(
                freeze_info.frozen_schema_versions_, tenant_schema_versions.tenant_schema_versions_)) {
          ret = OB_EAGAIN;
          LOG_WARN("invalid versions",
              KR(ret),
              K(now),
              K(tenant_schema_versions),
              K(freeze_info.current_frozen_status_),
              K(freeze_info.frozen_schema_versions_));
        }
      } else {
        if (!is_versions_in_order(
                tenant_schema_versions.tenant_schema_versions_, freeze_info.frozen_schema_versions_)) {

          ret = OB_EAGAIN;
          LOG_WARN("invalid versions",
              KR(ret),
              K(now),
              K(tenant_schema_versions),
              K(freeze_info.current_frozen_status_),
              K(freeze_info.frozen_schema_versions_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BEFORE_RENEW_SNAPSHOT_FAIL) OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      const bool need_gc_schema_version = true;
      if (OB_FAIL(global_stat_proxy.set_snapshot_info_v2(
              *sql_proxy_, now, tenant_schema_versions.tenant_schema_versions_, need_gc_schema_version))) {
        LOG_WARN("fail to renew snapshot", KR(ret), K(now), K(tenant_schema_versions));
      } else if (OB_FAIL(freeze_info.latest_snapshot_gc_schema_versions_.assign(
                     tenant_schema_versions.tenant_schema_versions_))) {
        LOG_WARN("fail to set latest_snapshot_gc_schema_versions", KR(ret), K(now), K(tenant_schema_versions));
      } else {
        freeze_info.latest_snapshot_gc_ts_ = now;
        LOG_DEBUG("set new gc snapshot ts",
            "snapshot_gc_ts",
            freeze_info.latest_snapshot_gc_ts_,
            "gc_schema_version",
            freeze_info.latest_snapshot_gc_schema_versions_);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(set_freeze_info(freeze_info))) {
        LOG_WARN("fail to set freeze info", KR(ret), K(freeze_info));
      }
      if (OB_FAIL(ret)) {
        set_freeze_info_invalid();
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_RENEW_SNAPSHOT_FAIL) OB_SUCCESS;
  }
  return ret;
}

int ObFreezeInfoManager::get_latest_snapshot_gc_ts(int64_t& snapshot_gc_ts) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_snapshot_gc_ts_in_memory(snapshot_gc_ts))) {
    LOG_WARN("failed to get latest gc ts", K(ret), K(snapshot_gc_ts));
    ObGlobalStatProxy global_stat_proxy(*sql_proxy_);
    if (OB_FAIL(global_stat_proxy.get_snapshot_gc_ts(snapshot_gc_ts))) {
      LOG_WARN("failed to get gc ts in proxy", K(ret), K(snapshot_gc_ts));
    }
  }
  return ret;
}

int ObFreezeInfoManager::set_latest_snapshot_gc_ts(const int64_t time)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(freeze_info_lock_);
  if (!freeze_info_.is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    freeze_info_.latest_snapshot_gc_ts_ = time;
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_RENEW_SNAPSHOT_FAIL) OB_SUCCESS;
  }
  return ret;
}

int ObFreezeInfoManager::renew_snapshot_gc_ts_223(const int64_t gc_timestamp)
{
  int ret = OB_SUCCESS;
  ObGlobalStatProxy global_stat_proxy(*sql_proxy_);
  int64_t latest_snapshot_gc_ts = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner error", KR(ret));
  } else if (OB_FAIL(get_snapshot_gc_ts_in_memory(latest_snapshot_gc_ts))) {
    LOG_WARN("fail to get latest snapshot gc ts", KR(ret));
  } else if (gc_timestamp <= latest_snapshot_gc_ts) {
    ret = OB_EAGAIN;
    LOG_WARN("invalid snaptshot gc time, retry", K(ret), K(gc_timestamp), K(latest_snapshot_gc_ts));
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BEFORE_RENEW_SNAPSHOT_FAIL) OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(global_stat_proxy.set_snapshot_gc_ts(gc_timestamp))) {
    LOG_WARN("fail to renew snapshot", KR(ret), K(gc_timestamp));
    set_freeze_info_invalid();
  } else if (OB_FAIL(set_latest_snapshot_gc_ts(gc_timestamp))) {
    set_freeze_info_invalid();
    LOG_WARN("fail to set latest snapshot gc ts", KR(ret));
  } else {
    LOG_DEBUG("set new gc snapshot ts", K(gc_timestamp));
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_RENEW_SNAPSHOT_FAIL) OB_SUCCESS;
  }
  return ret;
}

int ObFreezeInfoManager::get_snapshot_gc_ts_in_memory(int64_t& gc_snapshot_version) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(freeze_info_lock_);
    gc_snapshot_version = freeze_info_.latest_snapshot_gc_ts_;
  }
  return ret;
}

int ObFreezeInfoManager::check_snapshot_gc_ts()
{
  int ret = OB_SUCCESS;
  int64_t snapshot_gc_ts = 0;
  int64_t delay = 0;
  // FIXME: only check primary cluster snapshot_gc_ts
  if (OB_SUCC(ret) && GCTX.can_be_parent_cluster()) {
    if (freeze_info_lock_.try_rdlock()) {
      if (OB_UNLIKELY(!freeze_info_.is_valid())) {
        freeze_info_lock_.unlock();
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("freeze info is unvalid", KR(ret));
      } else {
        snapshot_gc_ts = freeze_info_.latest_snapshot_gc_ts_;
        freeze_info_lock_.unlock();
        delay = snapshot_gc_ts == 0 ? 0 : ObTimeUtility::current_time() - snapshot_gc_ts;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          if (delay > SNAPSHOT_GC_TS_ERROR) {
            LOG_ERROR("rs_monitor_check : snapshot_gc_ts delay for a long time", K(snapshot_gc_ts), K(delay));
          } else if (delay > SNAPSHOT_GC_TS_WARN) {
            LOG_WARN("rs_monitor_check : snapshot_gc_ts delay for a long time", K(snapshot_gc_ts), K(delay));
          }
        }
      }
    }
  }
  return ret;
}
bool ObFreezeInfoManager::is_equal_array(
    const ObIArray<TenantIdAndSchemaVersion>& left, const ObIArray<TenantIdAndSchemaVersion>& right)
{
  bool bret = true;
  if (left.count() != right.count()) {
    bret = false;
  } else {
    for (int64_t i = 0; i < left.count(); i++) {
      if (left.at(i).tenant_id_ != right.at(i).tenant_id_ ||
          left.at(i).schema_version_ != right.at(i).schema_version_) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

int ObFreezeInfoManager::get_snapshot_info(const int64_t tenant_id, const ObIArray<TenantSnapshot>& new_snapshots,
    const ObIArray<TenantSnapshot>& old_snapshots, bool& need_update, TenantSnapshot& snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  }
  const TenantSnapshot* new_snapshot = NULL;
  const TenantSnapshot* old_snapshot = NULL;
  for (int64_t i = 0; i < new_snapshots.count(); i++) {
    if (new_snapshots.at(i).tenant_id_ == tenant_id) {
      new_snapshot = &new_snapshots.at(i);
      break;
    }
  }
  for (int64_t i = 0; i < old_snapshots.count(); i++) {
    if (old_snapshots.at(i).tenant_id_ == tenant_id) {
      old_snapshot = &old_snapshots.at(i);
      break;
    }
  }
  if (OB_ISNULL(new_snapshot)) {
    // nothing todo
    need_update = false;
  } else if (OB_ISNULL(old_snapshot)) {
    need_update = true;
    snapshot = *new_snapshot;
  } else if (new_snapshot->snapshot_ts_ > old_snapshot->snapshot_ts_) {
    need_update = true;
    snapshot = *new_snapshot;
  } else {
    need_update = false;
  }
  return ret;
}

bool ObFreezeInfoManager::is_leader_cluster()
{
  bool bret = false;
  if (!is_inited_) {
    LOG_WARN("freeze info not inited");
  } else {
    bret = (PRIMARY_CLUSTER == ObClusterInfoGetter::get_cluster_type_v2());
  }
  return bret;
}

int ObFreezeInfoManager::get_frozen_status_for_create_partition(
    const uint64_t tenant_id, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  bool found = false;
  FreezeInfo tmp_freeze_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get shadow", KR(ret));
  } else if (tmp_freeze_info.frozen_schema_versions_.count() > 0) {
    found = true;
    for (int64_t i = 0; i < tmp_freeze_info.frozen_schema_versions_.count(); i++) {
      if (tenant_id == tmp_freeze_info.frozen_schema_versions_.at(i).tenant_id_ &&
          OB_INVALID_SCHEMA_VERSION == tmp_freeze_info.frozen_schema_versions_.at(i).schema_version_) {
        found = false;
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("tenant schema not ready, refuse to create partition", KR(ret), K(tenant_id));
        break;
      }
    }
    if (found) {
      frozen_status = tmp_freeze_info.current_frozen_status_;
    }
  }
  if (OB_FAIL(ret) || found) {
  } else if (OB_FAIL(freeze_info_proxy_.get_max_frozen_status_with_integrated_schema_version_v2(
                 *sql_proxy_, frozen_status))) {
    LOG_WARN("fail to get max frozen status with schema version", KR(ret));
  }
  LOG_INFO("get frozen status for create partition",
      K(ret),
      K(frozen_status),
      "cost",
      ObTimeUtility::current_time() - start);

  return ret;
}

int ObFreezeInfoManager::get_max_frozen_status_for_daily_merge(ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  FreezeInfo tmp_freeze_info;
  bool found = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get shadow", KR(ret));
  } else if (tmp_freeze_info.frozen_schema_versions_.count() > 0) {
    found = true;
    for (int64_t i = 0; i < tmp_freeze_info.frozen_schema_versions_.count(); i++) {
      if (OB_INVALID_SCHEMA_VERSION == tmp_freeze_info.frozen_schema_versions_.at(i).schema_version_) {
        found = false;
        break;
      }
    }
    if (found) {
      frozen_status = tmp_freeze_info.current_frozen_status_;
    }
  }
  // ignore ret;
  if (found) {
  } else if (OB_FAIL(freeze_info_proxy_.get_max_frozen_status_with_integrated_schema_version_v2(
                 *sql_proxy_, frozen_status))) {
    LOG_WARN("fail to get max frozen status with schema version", KR(ret));
  }
  LOG_INFO(
      "get max frozen status for daily merge", K(ret), K(frozen_status), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

bool ObFreezeInfoManager::is_versions_in_order(
    const ObIArray<TenantIdAndSchemaVersion>& left_schema, const ObIArray<TenantIdAndSchemaVersion>& right_schema)
{
  bool bret = true;
  for (int64_t i = 0; i < right_schema.count() && bret; i++) {
    const int64_t right_schema_tenant_id = right_schema.at(i).tenant_id_;
    for (int64_t j = 0; j < left_schema.count() && bret; j++) {
      const int64_t left_schema_tenant_id = left_schema.at(j).tenant_id_;
      if (right_schema_tenant_id == left_schema_tenant_id) {
        // find same tenent
        if (right_schema.at(i).schema_version_ < left_schema.at(j).schema_version_ &&
            OB_INVALID_SCHEMA_VERSION != right_schema.at(i).schema_version_) {
          bret = false;
          LOG_WARN("snapshot schema_versions not in order",
              K(left_schema),
              K(right_schema),
              "tenant_id",
              right_schema_tenant_id);
        }
      } else if (right_schema_tenant_id < left_schema_tenant_id) {
        // tenant only in right_schema
        break;
      }
    }
  }
  return bret;
}

int ObFreezeInfoManager::check_snapshot_info_valid(
    const int64_t snapshot_gc_ts, const ObIArray<TenantIdAndSchemaVersion>& gc_schema_version, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObArray<TenantIdAndSchemaVersion> frozen_schema_version_smaller;
  ObArray<TenantIdAndSchemaVersion> frozen_schema_version_larger;
  FreezeInfo tmp_freeze_info;
  if (snapshot_gc_ts <= 0 || gc_schema_version.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_gc_ts), K(gc_schema_version));
  } else if (OB_FAIL(get_freeze_info_shadow(tmp_freeze_info))) {
    LOG_WARN("fail to get shadow", KR(ret));
  } else if (snapshot_gc_ts <= tmp_freeze_info.latest_snapshot_gc_ts_) {
    // can not rollback
    is_valid = false;
  } else if (OB_FAIL(freeze_info_proxy_.get_schema_info_less_than(
                 *sql_proxy_, snapshot_gc_ts, frozen_schema_version_smaller))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get frozen info less than", KR(ret), K(snapshot_gc_ts));
    }
  } else if (!is_versions_in_order(frozen_schema_version_smaller, gc_schema_version)) {
    is_valid = false;
    LOG_WARN("invalid versions fro gc snapshot ts",
        K(snapshot_gc_ts),
        K(gc_schema_version),
        K(frozen_schema_version_smaller));
  }
  if (OB_FAIL(ret) || !is_valid) {
  } else if (OB_FAIL(freeze_info_proxy_.get_schema_info_larger_than(
                 *sql_proxy_, snapshot_gc_ts, frozen_schema_version_larger))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get frozen info larger than", KR(ret), K(snapshot_gc_ts));
    }
  } else if (!is_versions_in_order(gc_schema_version, frozen_schema_version_larger)) {
    is_valid = false;
    LOG_WARN("invalid versions for gc snapshot ts",
        K(snapshot_gc_ts),
        K(gc_schema_version),
        K(frozen_schema_version_larger));
  }
  if (OB_FAIL(ret) || !is_valid) {
  } else if (!is_versions_in_order(tmp_freeze_info.latest_snapshot_gc_schema_versions_, gc_schema_version)) {
    is_valid = false;
    LOG_WARN("invalid versions for gc snapshot ts",
        K(snapshot_gc_ts),
        K(gc_schema_version),
        K(tmp_freeze_info.latest_snapshot_gc_schema_versions_));
  }
  return ret;
}

int ObFreezeInfoManager::get_latest_merger_frozen_status(share::ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  int64_t frozen_version = OB_INVALID_VERSION;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_manager_->get_global_last_merged_version(frozen_version))) {
    LOG_WARN("failed to get latest merge version", K(ret), K(frozen_version));
  } else if (OB_FAIL(get_freeze_info(frozen_version, frozen_status))) {
    LOG_WARN("failed to get frozen info", K(ret), K(frozen_version));
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
