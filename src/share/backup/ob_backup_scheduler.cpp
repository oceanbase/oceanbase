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

#define USING_LOG_PREFIX SERVER

#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_common_config.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_mgr.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/string/ob_sql_string.h"
#include "ob_backup_scheduler.h"
#include "rootserver/ob_root_backup.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "rootserver/ob_rs_event_history_table_operator.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
namespace share {

ObBackupScheduler::ObBackupScheduler()
    : is_inited_(false),
      arg_(),
      schema_service_(NULL),
      proxy_(NULL),
      is_cluster_backup_(false),
      schema_version_map_(),
      backup_snapshot_version_(0),
      backup_data_version_(0),
      frozen_timestamp_(0),
      max_backup_set_id_(0),
      root_backup_(NULL),
      freeze_info_manager_(nullptr),
      restore_point_service_(NULL)
{}

ObBackupScheduler::~ObBackupScheduler()
{}

int ObBackupScheduler::init(const obrpc::ObBackupDatabaseArg& arg, schema::ObMultiVersionSchemaService& schema_service,
    common::ObMySQLProxy& proxy, rootserver::ObRootBackup& root_backup,
    rootserver::ObFreezeInfoManager& freeze_info_manager, rootserver::ObRestorePointService& restore_point_service)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoProxy freeze_info_proxy;
  ObSimpleFrozenStatus frozen_status;
  const int64_t backup_snapshot_version = ObTimeUtil::current_time();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup scheduler init twice", K(ret), K(is_inited_));
  } else if (0 != arg.tenant_id_ || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup scheduler get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(schema_version_map_.create(MAX_TENANT_BUCKET, ObModIds::BACKUP))) {
    LOG_WARN("failed to create schema version map", K(ret));
  } else if (OB_FAIL(freeze_info_proxy.get_frozen_info_less_than(proxy, backup_snapshot_version, frozen_status))) {
    LOG_WARN("failed to get frozen info less than backup snapshot version", K(ret), K(backup_snapshot_version));
  } else if (OB_FAIL(init_frozen_schema_versions_(freeze_info_manager, frozen_status.frozen_version_))) {
    LOG_WARN("failed to init frozen schema versions", K(ret), K(frozen_status));
  } else {
    backup_snapshot_version_ = backup_snapshot_version;
    backup_data_version_ = frozen_status.frozen_version_;
    frozen_timestamp_ = frozen_status.frozen_timestamp_;
    arg_ = arg;
    schema_service_ = &schema_service;
    proxy_ = &proxy;
    is_cluster_backup_ = 0 == arg.tenant_id_ ? true : false;
    root_backup_ = &root_backup;
    freeze_info_manager_ = &freeze_info_manager;
    restore_point_service_ = &restore_point_service;
    is_inited_ = true;
    LOG_INFO("success init backup scheduler", K(arg), K(backup_snapshot_version), K(frozen_status));
  }
  return ret;
}

int ObBackupScheduler::start_schedule_backup()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBackupInfoManager info_manager;
  ObArray<ObBaseBackupInfoStruct> backup_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else if (OB_FAIL(check_gts_(tenant_ids))) {
    LOG_WARN("failed to check tenant ids", K(ret));
  } else if (OB_FAIL(check_log_archive_status())) {
    LOG_WARN("failed to check log archive status", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(info_manager.get_backup_info(backup_infos))) {
    LOG_WARN("failed to get bacskup info", K(ret));
  } else if (OB_FAIL(check_can_backup(backup_infos))) {
    LOG_WARN("failed to check can backup", K(ret), K(backup_infos));
  } else if (OB_FAIL(get_max_backup_set_id(backup_infos))) {
    LOG_WARN("failed to get max backup set id", K(ret), K(backup_infos));
  } else if (OB_FAIL(schedule_backup(tenant_ids, info_manager))) {
    LOG_WARN("failed to schedule backup", K(ret), K(backup_infos));
  }
  return ret;
}

int ObBackupScheduler::init_frozen_schema_versions_(
    rootserver::ObFreezeInfoManager& freeze_info_manager, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::TenantIdAndSchemaVersion> schema_versions;
  const uint64_t tenant_id = 0;  // for all tenants

  if (OB_FAIL(frozen_schema_version_map_.create(MAX_TENANT_BUCKET, ObModIds::BACKUP))) {
    LOG_WARN("failed to create schema version map", K(ret));
  } else if (OB_FAIL(freeze_info_manager.get_freeze_schema_versions(tenant_id, frozen_version, schema_versions))) {
    LOG_WARN("failed to get freeze schema versions", K(ret), K(frozen_version));
  } else if (schema_versions.empty()) {
    ret = OB_EAGAIN;
    LOG_WARN("frozen schema version is not ready, please retry later", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < schema_versions.count(); ++i) {
    const TenantIdAndSchemaVersion& version = schema_versions.at(i);
    if (OB_FAIL(frozen_schema_version_map_.set_refactored(version.tenant_id_, version.schema_version_))) {
      LOG_WARN("Failed to set schema version map", K(ret), K(version));
    }
  }
#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_SCHEDULER_GET_SCHEMA_VERSION_ERROR) OB_SUCCESS;
#endif

  FLOG_INFO("finish init frozen schema versions", K(ret), K(frozen_version), K(schema_versions));
  return ret;
}

int ObBackupScheduler::check_backup_schema_version_(const uint64_t tenant_id, const int64_t backup_schema_version)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  int64_t schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (tenant_id == OB_SYS_TENANT_ID) {
    // skip check sys tenant
  } else if (OB_SUCCESS != (hash_ret = frozen_schema_version_map_.get_refactored(tenant_id, schema_version))) {
    if (OB_HASH_NOT_EXIST == hash_ret) {
      LOG_INFO("tenant not freeze, skip check schema version", K(ret), K(tenant_id));
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
    }
  } else if (schema_version > backup_schema_version) {
    ret = OB_NOT_SUPPORTED;
    FLOG_ERROR("cannot backup tenant with freeze schema version larger than backup schema version",
        K(ret),
        K(tenant_id),
        K(schema_version),
        K(backup_schema_version));
  } else {
    LOG_INFO("check backup schema version", K(tenant_id), K(backup_schema_version), K(schema_version));
  }
  return ret;
}

int ObBackupScheduler::check_gts_(const ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    if (tenant_id == OB_SYS_TENANT_ID) {
      continue;
    } else if (OB_FAIL(check_gts_(tenant_id))) {
      LOG_WARN("fail to check_gts", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupScheduler::check_gts_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t gts_type = transaction::TS_SOURCE_LTS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard to determine tenant gts type", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_timestamp_service_type(tenant_id, gts_type))) {
    LOG_WARN("fail to get tenant gts switch", KR(ret), K(tenant_id));
  } else if (!transaction::is_ts_type_external_consistent(gts_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot backup tenant without GTS", K(ret), K(tenant_id), K(gts_type));
  }
  return ret;
}

int ObBackupScheduler::get_tenant_ids(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  int64_t backup_schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else {
    if (is_cluster_backup_) {
      if (OB_FAIL(get_tenant_schema_version(OB_SYS_TENANT_ID, backup_schema_version))) {
        LOG_WARN("failed to get tenant schem version", K(ret));
      } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                     OB_SYS_TENANT_ID, *schema_service_, backup_schema_version, guard))) {
        LOG_WARN("failed to get tenant schema guard", K(ret), K(backup_schema_version));
      } else if (OB_FAIL(guard.get_tenant_ids(tenant_ids))) {
        LOG_WARN("failed to get tenant ids", K(ret), K(arg_));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("backup do not support tenant backup, will support it later", K(ret));
      //      if (OB_FAIL(get_tenant_schema_version(tenant_id_, backup_schema_version))) {
      //        LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id_));
      //      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
      //          tenant_id_, guard, backup_schema_version))) {
      //        LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id_));
      //      } else if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
      //        LOG_WARN("failed to push tenant id into array", K(ret));
      //      } else if (OB_FAIL(tenant_ids.push_back(tenant_id_))) {
      //        LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id_));
      //      }
    }
  }
  return ret;
}

int ObBackupScheduler::check_can_backup(const ObIArray<ObBaseBackupInfoStruct>& infos)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (infos.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can backup get invalid argument", K(ret), K(infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      const ObBaseBackupInfoStruct& info = infos.at(i);
      if (0 == i) {
        if (OB_SYS_TENANT_ID != info.tenant_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first backup info should be sys tenant backup info", K(ret), K(info));
        }
      }

      if (OB_SUCC(ret)) {
        if (!info.backup_status_.is_stop_status()) {
          ret = OB_BACKUP_IN_PROGRESS;
          LOG_WARN("backup has already been started", K(ret), K(info));
        } else if (OB_FAIL(check_backup_task_infos_status(info))) {
          LOG_WARN("failed to check backup task infos", K(ret), K(info));
        }
      }
    }
  }
  return ret;
}

int ObBackupScheduler::schedule_backup(const ObIArray<uint64_t>& tenant_ids, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  const int64_t backup_snapshot_version = backup_snapshot_version_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup infos should not be empty", K(ret), K(tenant_ids));
  } else {
    const uint64_t tenant_id = tenant_ids.at(0);
    if (tenant_id != OB_SYS_TENANT_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first info should be sys tenant backup info", K(ret), K(tenant_id));
    } else if (is_cluster_backup_) {
      if (OB_FAIL(schedule_sys_tenant_backup(backup_snapshot_version, tenant_id, info_manager))) {
        LOG_WARN("failed to schedule sys tenant backup", K(ret));
      }
    }

    DEBUG_SYNC(BACKUP_INFO_PREPARE);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schedule_tenants_backup(backup_snapshot_version, tenant_ids, info_manager))) {
        LOG_WARN("failed to schedule tenants backup", K(ret));
      }
    }

    DEBUG_SYNC(BACKUP_INFO_SCHEDULER);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(start_backup(info_manager))) {
        LOG_WARN("failed to start backup", K(ret));
      }
    }

    root_backup_->update_prepare_flag(false /*is_prepare_flag*/);
    root_backup_->wakeup();

    if (tenant_ids.count() != 1) {
      ROOTSERVICE_EVENT_ADD("backup", "schedule_backup", "tenant_count", tenant_ids.count());
    } else {
      ROOTSERVICE_EVENT_ADD("backup", "schedule_backup", "tenant_id", tenant_ids.at(0));
    }
  }
  return ret;
}

int ObBackupScheduler::schedule_sys_tenant_backup(
    const int64_t backup_snapshot_version, const uint64_t tenant_id, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  int64_t backup_schema_version = 0;
  ObBaseBackupInfoStruct info;
  ObBaseBackupInfoStruct dest_info;
  ObBackupItemTransUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id || backup_snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id should be sys tenant id", K(ret), K(tenant_id), K(backup_snapshot_version));
  } else if (OB_FAIL(get_tenant_schema_version(tenant_id, backup_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else {
      dest_info = info;
      // dest_info.backup_dest_ added by log archive
      dest_info.backup_snapshot_version_ = backup_snapshot_version;
      dest_info.backup_schema_version_ = backup_schema_version;
      dest_info.backup_type_.type_ =
          arg_.is_incremental_ ? ObBackupType::INCREMENTAL_BACKUP : ObBackupType::FULL_BACKUP;
      dest_info.backup_data_version_ = backup_data_version_;
      dest_info.backup_status_.status_ = ObBackupInfoStatus::PREPARE;
      dest_info.backup_set_id_ = max_backup_set_id_;
      dest_info.encryption_mode_ = arg_.encryption_mode_;
      dest_info.passwd_ = arg_.passwd_;
      if (OB_FAIL(info_manager.check_can_update(info, dest_info))) {
        LOG_WARN("failed to check can update", K(ret));
      } else if (OB_FAIL(info_manager.update_backup_info(tenant_id, dest_info, updater))) {
        LOG_WARN("failed to update backup info", K(ret), K(tenant_id), K(dest_info));
      } else {
        root_backup_->update_prepare_flag(true /*is_prepare_flag*/);
      }
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupScheduler::schedule_tenant_backup(const int64_t backup_snapshot_version, const uint64_t tenant_id,
    const ObBaseBackupInfoStruct::BackupDest& backup_dest, ObISQLClient& sys_tenant_trans,
    ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  int64_t backup_schema_version = 0;
  ObBaseBackupInfoStruct info;
  ObBaseBackupInfoStruct dest_info;
  ObBackupItemTransUpdater updater;
  bool can_backup = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id || backup_snapshot_version <= 0 || backup_dest.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id should be sys tenant id", K(ret), K(tenant_id), K(backup_snapshot_version), K(backup_dest));
  } else if (OB_FAIL(get_tenant_schema_version(tenant_id, backup_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_can_backup(tenant_id, backup_schema_version, info_manager, can_backup))) {
    LOG_WARN("failed to check tenant can backup", K(ret), K(tenant_id));
  } else if (!can_backup) {
    // do nothing
  } else if (OB_FAIL(check_backup_schema_version_(tenant_id, backup_schema_version))) {
    LOG_WARN("failed to check check_backup_schema_version_",
        K(ret),
        K(tenant_id),
        K(backup_schema_version),
        K(backup_data_version_));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else {
      dest_info = info;
      dest_info.backup_dest_ = backup_dest;
      dest_info.backup_snapshot_version_ = backup_snapshot_version;
      dest_info.backup_schema_version_ = backup_schema_version;
      dest_info.backup_type_.type_ =
          arg_.is_incremental_ ? ObBackupType::INCREMENTAL_BACKUP : ObBackupType::FULL_BACKUP;
      dest_info.backup_data_version_ = backup_data_version_;
      dest_info.backup_status_.status_ = ObBackupInfoStatus::SCHEDULE;
      dest_info.backup_set_id_ = max_backup_set_id_;
      dest_info.encryption_mode_ = arg_.encryption_mode_;
      dest_info.passwd_ = arg_.passwd_;
      if (OB_FAIL(rootserver::ObBackupUtil::check_sys_tenant_trans_alive(info_manager, sys_tenant_trans))) {
        LOG_WARN("failed to check sys tenant trans alive", K(ret), K(info));
      } else if (OB_FAIL(info_manager.check_can_update(info, dest_info))) {
        LOG_WARN("failed to check can update", K(ret));
      } else if (OB_FAIL(info_manager.update_backup_info(tenant_id, dest_info, updater))) {
        LOG_WARN("failed to update backup info", K(ret), K(tenant_id), K(dest_info));
      }
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_backup_point(tenant_id))) {
        LOG_WARN("failed to create backup point", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupScheduler::schedule_tenants_backup(const int64_t backup_snapshot_version,
    const common::ObIArray<uint64_t>& tenant_ids, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct sys_backup_info;
  ObBaseBackupInfoStruct::BackupDest backup_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule get tenants backup get invalid argument", K(ret), K(tenant_ids));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(OB_SYS_TENANT_ID, updater, sys_backup_info))) {
      LOG_WARN("failed to get tenant backup info", K(ret));
    } else if ((is_cluster_backup_ && !sys_backup_info.backup_status_.is_prepare_status()) ||
               (!is_cluster_backup_ && !sys_backup_info.backup_status_.is_stop_status())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys backup info status is unexpected", K(ret), K(sys_backup_info));
    } else if (is_cluster_backup_) {
      backup_dest = sys_backup_info.backup_dest_;
    } else {
      // tenant backup, we may support it in 3.2 later
      // TODO() use tenant backup dest
    }

    if (OB_FAIL(ret)) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(schedule_tenant_backup(
                       backup_snapshot_version, tenant_id, backup_dest, updater.get_trans(), info_manager))) {
          LOG_WARN("failed to schedule tenant backup", K(ret), K(tenant_id));
        }
      }
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupScheduler::get_tenant_schema_version(const uint64_t tenant_id, int64_t& backup_schema_version)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  backup_schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant schema version get invalid argument", K(ret), K(tenant_id));
  } else {
    hash_ret = schema_version_map_.get_refactored(tenant_id, backup_schema_version);
    if (OB_SUCCESS == hash_ret) {
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("get schema version map error", K(ret), K(hash_ret), K(tenant_id));
    } else if (OB_FAIL(fetch_schema_version(tenant_id, backup_schema_version))) {
      LOG_WARN("failed to fetch schema version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_version_map_.set_refactored(tenant_id, backup_schema_version))) {
      LOG_WARN("failed to set backup schema version into map", K(ret), K(tenant_id), K(backup_schema_version));
    }
  }
  return ret;
}

int ObBackupScheduler::fetch_schema_version(const uint64_t tenant_id, int64_t& backup_schema_version)
{
  // TODO() backup weak read without snapshot_version is wrong, fix it later
  int ret = OB_SUCCESS;
  backup_schema_version = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    ObSqlString sql;
    bool did_use_weak = true;
    bool did_use_retry = false;
    bool check_sys_variable = true;
    ObSQLClientRetryWeak sql_client_retry_weak(
        proxy_, did_use_weak, did_use_retry, backup_snapshot_version_, check_sys_variable);
    if (OB_FAIL(sql.append_fmt("SELECT max(schema_version) as version FROM %s", OB_ALL_DDL_OPERATION_TNAME))) {
      LOG_WARN("append sql failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
    } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (ret == OB_ITER_END) {  // no record
        ret = OB_EMPTY_RESULT;
        LOG_WARN("select max(schema_version) return no row", K(ret));
      } else {
        LOG_WARN("fail to get schema version. iter quit. ", K(ret));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "version", backup_schema_version, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get backup schema_version: ", K(ret));
      } else {
        LOG_INFO("get backup schema version", K(tenant_id), K(backup_schema_version));
        // check if this is only one
        if (OB_ITER_END != (ret = result->next())) {
          LOG_WARN("fail to get all table schema. iter quit. ", K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBackupScheduler::start_backup(ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (!is_cluster_backup_) {
    // do nothing
  } else {
    ObBaseBackupInfoStruct info;
    ObBaseBackupInfoStruct dest_info;
    ObBackupItemTransUpdater updater;
    const uint64_t tenant_id = OB_SYS_TENANT_ID;

    if (OB_FAIL(updater.start(*proxy_))) {
      LOG_WARN("failed to start trans", K(ret));
    } else {
      if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
        LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
      } else if (!info.backup_status_.is_prepare_status()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant backup info status is unexpected", K(ret), K(info));
      } else {
        dest_info = info;
        dest_info.backup_status_.set_backup_status_scheduler();
        if (OB_FAIL(info_manager.check_can_update(info, dest_info))) {
          LOG_WARN("failed to check can update", K(ret));
        } else if (OB_FAIL(info_manager.update_backup_info(tenant_id, dest_info, updater))) {
          LOG_WARN("failed to update backup info", K(ret), K(tenant_id), K(dest_info));
        }
      }
      int tmp_ret = updater.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupScheduler::rollback_backup_infos(ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObArray<ObBaseBackupInfoStruct> backup_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_FAIL(info_manager.get_backup_info(backup_infos))) {
    LOG_WARN("failed to get backup info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_infos.count(); ++i) {
      const ObBaseBackupInfoStruct& info = backup_infos.at(i);
      if (OB_FAIL(rollback_backup_info(info, info_manager))) {
        LOG_WARN("failed to rollback backup info", K(ret), K(info));
      }
    }
  }

  return ret;
}

int ObBackupScheduler::rollback_backup_info(const ObBaseBackupInfoStruct& info, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObBaseBackupInfoStruct dest_info;
  ObBackupItemTransUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (!info.is_valid() || info.backup_status_.status_ > ObBackupInfoStatus::SCHEDULE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rollback backup info get invalid argument", K(ret), K(info));
  } else if (info.backup_status_.is_stop_status()) {
    // do nothing
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(info));
  } else {
    dest_info = info;
    dest_info.backup_status_.set_backup_status_stop();
    if (OB_FAIL(info_manager.update_backup_info(dest_info.tenant_id_, dest_info, updater))) {
      LOG_WARN("failed to update backup info", K(ret), K(dest_info));
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupScheduler::get_max_backup_set_id(const ObIArray<ObBaseBackupInfoStruct>& infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (infos.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info should not empty", K(ret), K(infos));
  } else {
    int64_t max_backup_set_id = 0;
    for (int64_t i = 0; i < infos.count(); ++i) {
      const ObBaseBackupInfoStruct& info = infos.at(i);
      if (info.backup_set_id_ > max_backup_set_id) {
        max_backup_set_id = info.backup_set_id_;
      }
    }
    max_backup_set_id_ = max_backup_set_id + 1;
  }
  return ret;
}

int ObBackupScheduler::check_backup_task_infos_status(const ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater tenant_task_updater;
  ObPGBackupTaskUpdater pg_task_updater;
  ObTenantBackupTaskInfo tenant_info;
  ObPGBackupTaskInfo pg_task_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_FAIL(tenant_task_updater.init(*proxy_))) {
    LOG_WARN("failed to init tenant task updater", K(ret), K(info));
  } else if (OB_FAIL(pg_task_updater.init(*proxy_))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(info));
  } else if (FALSE_IT(tmp_ret = tenant_task_updater.get_tenant_backup_task(
                          info.tenant_id_, info.backup_set_id_, info.incarnation_, tenant_info))) {
  } else if (OB_ENTRY_NOT_EXIST != tmp_ret) {
    ret = tmp_ret != OB_SUCCESS ? tmp_ret : OB_ERR_UNEXPECTED;
    LOG_ERROR("backup has remained infos, can not start backup", K(ret), K(tenant_info));
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tmp_ret = pg_task_updater.get_one_pg_task(
                          info.tenant_id_, info.incarnation_, info.backup_set_id_, pg_task_info))) {
  } else if (OB_ENTRY_NOT_EXIST != tmp_ret) {
    ret = tmp_ret != OB_SUCCESS ? tmp_ret : OB_ERR_UNEXPECTED;
    LOG_ERROR("backup has remained infos, can not start backup", K(ret), K(pg_task_info));
  }
  return ret;
}

int ObBackupScheduler::check_tenant_can_backup(
    const uint64_t tenant_id, const int64_t backup_schema_version, ObBackupInfoManager& info_manager, bool& can_backup)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObSimpleTenantSchema* tenant_schema = NULL;
  int64_t base_backup_version = 0;

  can_backup = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tenant can backup get invalid argument", K(ret), K(tenant_id), K(backup_schema_version));
  } else if (OB_FAIL(info_manager.get_base_backup_version(tenant_id, *proxy_, base_backup_version))) {
    LOG_WARN("failed to get base backup version", K(ret), K(tenant_id), K(base_backup_version));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard, backup_schema_version))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(backup_schema_version));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K(tenant_id));
  } else if (tenant_schema->is_dropping() || tenant_schema->is_restore() ||
             backup_data_version_ < base_backup_version) {
    can_backup = false;
    FLOG_INFO("tenant can no join in backup, skip backup",
        K(*tenant_schema),
        K(backup_data_version_),
        K(base_backup_version));
  }
  return ret;
}

int ObBackupScheduler::create_backup_point(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char name[OB_MAX_RESERVED_POINT_NAME_LENGTH] = {0};
  int64_t backup_schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create backup point get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_schema_version(tenant_id, backup_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(restore_point_service_->create_backup_point(
                 tenant_id, name, backup_snapshot_version_, backup_schema_version))) {
    if (OB_ERR_BACKUP_POINT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to create backup point", K(ret), K(name), K(tenant_id), K(backup_snapshot_version_));
    }
  }
  return ret;
}

int ObBackupScheduler::check_log_archive_status()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo sys_info;
  const bool for_update = false;
  ObLogArchiveBackupInfoMgr info_mgr;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(*proxy_, for_update, OB_SYS_TENANT_ID, sys_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (ObLogArchiveStatus::DOING != sys_info.status_.status_) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("failed to start backup", K(ret), K(sys_info), K(backup_snapshot_version_), K(frozen_timestamp_));
    if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                           ERROR_MSG_LENGTH,
                           pos,
                           "log archive is not doing. log archive status : %s.",
                           ObLogArchiveStatus::get_str(sys_info.status_.status_)))) {
      LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
    } else {
      LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, error_msg);
    }
  } else if (sys_info.status_.start_ts_ > frozen_timestamp_) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("failed to start backup", K(ret), K(sys_info), K(backup_snapshot_version_), K(frozen_timestamp_));

    if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                           ERROR_MSG_LENGTH,
                           pos,
                           "log archive start timestamp is bigger than frozen timestamp, need major freeze first. "
                           "start timestamp : %ld, "
                           "frozen timestamp : %ld .",
                           sys_info.status_.start_ts_,
                           frozen_timestamp_))) {
      LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
    } else {
      LOG_USER_ERROR(OB_BACKUP_CAN_NOT_START, error_msg);
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
