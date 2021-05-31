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
#include "ob_backup_data_clean.h"
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/config/ob_server_config.h"
#include "ob_root_backup.h"
#include "share/backup/ob_tenant_name_mgr.h"
#include "backup/ob_tenant_backup_data_clean_mgr.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver {

int64_t ObBackupDataCleanIdling::get_idle_interval_us()
{
  const int64_t backup_check_interval = 10 * 1000 * 1000;  // 10s
  return backup_check_interval;
}

ObBackupDataClean::ObBackupDataClean()
    : is_inited_(false),
      schema_service_(NULL),
      sql_proxy_(NULL),
      idling_(stop_),
      is_prepare_flag_(false),
      inner_error_(OB_SUCCESS),
      is_working_(false),
      backup_lease_service_(nullptr)
{}

ObBackupDataClean::~ObBackupDataClean()
{}

int ObBackupDataClean::init(share::schema::ObMultiVersionSchemaService& schema_service, ObMySQLProxy& sql_proxy,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int root_backup_thread_cnt = 1;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "root backup init twice", K(ret));
  } else if (OB_FAIL(create(root_backup_thread_cnt, "BackupDataClean"))) {
    LOG_WARN("create thread failed", K(ret), K(root_backup_thread_cnt));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
    LOG_INFO("backup data clean init success");
  }
  return ret;
}

int ObBackupDataClean::idle() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } else {
    LOG_INFO("backup data clean idle", "idle_time", idling_.get_idle_interval_us());
  }
  return ret;
}

void ObBackupDataClean::wakeup()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    idling_.wakeup();
  }
}

void ObBackupDataClean::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObBackupDataClean::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("failed to start", K(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

void ObBackupDataClean::run3()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupDataCleanTenant> data_clean_tenants;
  ObCurTraceId::init(GCONF.self_addr_);

  while (!stop_) {
    ret = OB_SUCCESS;
    data_clean_tenants.reset();
    inner_error_ = OB_SUCCESS;
    if (OB_FAIL(backup_lease_service_->check_lease())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(prepare_tenant_backup_clean())) {
      LOG_WARN("failed to prepare tenant backup clean", K(ret));
    } else if (OB_FAIL(get_need_clean_tenants(data_clean_tenants))) {
      LOG_WARN("failed to get need clean tenants", K(ret));
    } else if (OB_FAIL(do_clean_scheduler(data_clean_tenants))) {
      LOG_WARN("failed to do root scheduler", K(ret), K(data_clean_tenants));
    }

    cleanup_prepared_infos();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(idle())) {
        LOG_WARN("idle failed", K(ret));
      } else {
        continue;
      }
    }
  }
  FLOG_INFO("ObBackupDataClean stop");
  is_working_ = false;
}

int ObBackupDataClean::get_need_clean_tenants(ObIArray<ObBackupDataCleanTenant>& clean_tenants)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupDataCleanTenant> server_clean_tenants;
  hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> extern_clean_tenants_map;
  ObArray<ObSimpleBackupDataCleanTenant> deleted_clean_tenants;
  ObArray<ObSimpleBackupDataCleanTenant> new_deleted_clean_tenants;
  ObBackupCleanInfo sys_clean_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get clean info", K(ret));
    }
  } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_ &&
             ObBackupCleanInfoStatus::CANCEL != sys_clean_info.status_) {
    // do nothing
  } else if (OB_FAIL(extern_clean_tenants_map.create(MAX_BUCKET_NUM, ObModIds::BACKUP))) {
    LOG_WARN("failed to create clean tenants set", K(ret));
  } else if (OB_FAIL(get_server_clean_tenants(server_clean_tenants))) {
    LOG_WARN("failed to get server clean tenants", K(ret));
  } else if (OB_FAIL(get_deleted_clean_tenants(deleted_clean_tenants))) {
    LOG_WARN("failed to get deleted clean tenants", K(ret));
  } else if (OB_FAIL(get_extern_clean_tenants(extern_clean_tenants_map))) {
    LOG_WARN("failed to get extern clean tenants", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_clean_tenants.count(); ++i) {
      const ObBackupDataCleanTenant& server_clean_tenant = server_clean_tenants.at(i);
      ObSimpleBackupDataCleanTenant simple_clean_tenant;
      if (OB_FAIL(extern_clean_tenants_map.get_refactored(
              server_clean_tenant.simple_clean_tenant_.tenant_id_, simple_clean_tenant))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get refactored from map", K(ret), K(server_clean_tenant));
        }
      } else if (OB_FAIL(
                     extern_clean_tenants_map.erase_refactored(server_clean_tenant.simple_clean_tenant_.tenant_id_))) {
        LOG_WARN("failed to erase simple clean tenant", K(ret), K(server_clean_tenant));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& deleted_clean_tenant = deleted_clean_tenants.at(i);
      ObSimpleBackupDataCleanTenant simple_clean_tenant;
      if (OB_FAIL(extern_clean_tenants_map.get_refactored(deleted_clean_tenant.tenant_id_, simple_clean_tenant))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get refactored from map", K(ret), K(deleted_clean_tenant));
        }
      } else if (OB_FAIL(extern_clean_tenants_map.erase_refactored(deleted_clean_tenant.tenant_id_))) {
        LOG_WARN("failed to erase simple clean tenant", K(ret), K(deleted_clean_tenant));
      }
    }

    // add from server clean tenants
    for (int64_t i = 0; OB_SUCC(ret) && i < server_clean_tenants.count(); ++i) {
      const ObBackupDataCleanTenant& server_clean_tenant = server_clean_tenants.at(i);
      if (OB_FAIL(clean_tenants.push_back(server_clean_tenant))) {
        LOG_WARN("failed to push server clean tenant into clean tenants", K(ret), K(server_clean_tenant));
      }
    }

    // add deleted clean tenant already in inner table
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = deleted_clean_tenants.at(i);
      ObBackupDataCleanTenant clean_tenant;
      clean_tenant.simple_clean_tenant_ = simple_clean_tenant;
      if (OB_FAIL(clean_tenants.push_back(clean_tenant))) {
        LOG_WARN("failed to push server clean tenant into clean tenants", K(ret), K(clean_tenant));
      }
    }

    hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>::iterator iter;
    for (iter = extern_clean_tenants_map.begin(); OB_SUCC(ret) && iter != extern_clean_tenants_map.end(); ++iter) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = iter->second;
      if (OB_FAIL(new_deleted_clean_tenants.push_back(simple_clean_tenant))) {
        LOG_WARN("failed to push simple clean tenant info array", K(ret), K(simple_clean_tenant));
      }
    }

    // insert new deleted tenant into inner table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schedule_deleted_clean_tenants(new_deleted_clean_tenants))) {
        LOG_WARN("failed to schedule deleted clean tenants", K(ret));
      }
    }

    // add deleted tenants
    for (int64_t i = 0; OB_SUCC(ret) && i < new_deleted_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = new_deleted_clean_tenants.at(i);
      ObBackupDataCleanTenant clean_tenant;
      clean_tenant.simple_clean_tenant_ = simple_clean_tenant;
      if (OB_FAIL(clean_tenants.push_back(clean_tenant))) {
        LOG_WARN("failed to push server clean tenant into clean tenants", K(ret), K(clean_tenant));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_server_clean_tenants(ObIArray<ObBackupDataCleanTenant>& clean_tenants)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArray<uint64_t> all_tenant_ids;
  ObBackupCleanInfo sys_clean_info;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s

  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  int64_t calc_stmt_timeout = EXECUTE_TIMEOUT_US;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (all_tenant_ids.empty()) {
    // do nothing
  } else if (FALSE_IT(calc_stmt_timeout = (stmt_timeout * all_tenant_ids.count()))) {
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(calc_stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(calc_stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get clean info", K(ret));
      }
    } else if (ObBackupCleanInfoStatus::PREPARE == sys_clean_info.status_) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant_ids.count(); ++i) {
        const uint64_t tenant_id = all_tenant_ids.at(i);
        bool need_add = false;
        if (stop_) {
          ret = OB_RS_SHUTDOWN;
          LOG_WARN("rootservice shutdown", K(ret));
        } else if (OB_SYS_TENANT_ID == tenant_id) {
          need_add = true;
        } else if (OB_FAIL(get_server_need_clean_info(tenant_id, need_add))) {
          LOG_WARN("failed to get server need clean info", K(ret), K(tenant_id));
        }

        if (OB_FAIL(ret)) {
        } else if (!need_add) {
          // do nothing
        } else {
          ObBackupDataCleanTenant clean_tenant;
          clean_tenant.simple_clean_tenant_.tenant_id_ = tenant_id;
          clean_tenant.simple_clean_tenant_.is_deleted_ = false;
          if (OB_FAIL(clean_tenants.push_back(clean_tenant))) {
            LOG_WARN("failed to push clean tenant into array", K(ret), K(clean_tenant));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_all_tenant_ids(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  tenant_ids.reset();
  ObArray<uint64_t> tmp_tenant_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tenant_ids(tmp_tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tmp_tenant_ids.at(i);
      bool is_restore = false;
      if (OB_FAIL(guard.check_tenant_is_restore(tenant_id, is_restore))) {
        LOG_WARN("failed to check tenant is restore", K(ret), K(tenant_id));
      } else if (is_restore) {
        // do nothing
      } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_clean_info(
    const uint64_t tenant_id, ObISQLClient& sql_proxy, ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean info get invalid argument", K(ret), K(tenant_id), K(clean_info));
  } else if (OB_FAIL(updater.init(sql_proxy))) {
    LOG_WARN("failed to init tenant backup clean info updater", K(ret));
  } else if (OB_FAIL(updater.get_backup_clean_info(tenant_id, clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_clean_info(const uint64_t tenant_id, ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  clean_info.tenant_id_ = tenant_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_backup_clean_info(tenant_id, *sql_proxy_, clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupDataClean::get_server_need_clean_info(const uint64_t tenant_id, bool& need_add)
{
  int ret = OB_SUCCESS;
  need_add = false;
  ObMySQLTransaction trans;
  ObTenantBackupCleanInfoUpdater updater;
  ObBackupCleanInfo clean_info;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    clean_info.tenant_id_ = tenant_id;
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, clean_info))) {
      if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
        need_add = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
      }
    } else {
      need_add = true;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_tenant_backup_task_his_info(const ObBackupCleanInfo& clean_info, common::ObISQLClient& trans,
    common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_infos)
{
  int ret = OB_SUCCESS;
  tenant_infos.reset();
  ObBackupTaskHistoryUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task his info get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(updater.get_tenant_full_backup_tasks(clean_info.tenant_id_, tenant_infos))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(clean_info));
  } else {
    LOG_INFO("succeed get tenant backup task his info", K(tenant_infos));
  }
  return ret;
}

int ObBackupDataClean::get_tenant_backup_task_info(const share::ObBackupCleanInfo& clean_info,
    common::ObISQLClient& trans, common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_infos)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater updater;
  ObTenantBackupTaskInfo tenant_info;
  // can not tenant infos
  // TODO(yanfeng) need consider backup backupset clean
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task his info get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(updater.get_tenant_backup_task(clean_info.tenant_id_, tenant_info))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(clean_info));
  } else if (!tenant_info.is_valid() || !tenant_info.backup_type_.is_full_backup()) {
    // clean base cell is full backup set
    // do nothing
  } else if (OB_FAIL(tenant_infos.push_back(tenant_info))) {
    LOG_WARN("failed to add tenant info into array", K(ret), K(tenant_info));
  } else {
    LOG_INFO("succeed get tenant backup task info", K(tenant_info));
  }
  return ret;
}

/*
 *TODO
int ObBackupDataClean::get_tenant_backup_backupset_task_his_info(
    const share::ObBackupCleanInfo &clean_info,
    common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupBackupsetTaskInfo> &tenant_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task his info get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_full_task_items(
      clean_info.tenant_id_, clean_info.copy_id_, tenant_infos, trans))) {
    LOG_WARN("failed to get full backupset task items", KR(ret), K(clean_info));
  }
  return ret;
}
*/

int ObBackupDataClean::convert_backup_backupset_task_to_backup_task(
    const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& backup_backupset_tasks,
    common::ObIArray<share::ObTenantBackupTaskInfo>& backup_tasks)
{
  int ret = OB_SUCCESS;
  backup_tasks.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_backupset_tasks.count(); ++i) {
      ObTenantBackupTaskInfo info;
      const ObTenantBackupBackupsetTaskInfo& bb_info = backup_backupset_tasks.at(i);
      if (OB_FAIL(bb_info.convert_to_backup_task_info(info))) {
        LOG_WARN("failed to convert to backup task info", KR(ret), K(bb_info));
      } else if (OB_FAIL(backup_tasks.push_back(info))) {
        LOG_WARN("failed to push back backup info", KR(ret), K(info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_log_archive_info(const uint64_t tenant_id, common::ObISQLClient& trans,
    common::ObIArray<share::ObLogArchiveBackupInfo>& log_archive_infos)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr archive_info_mgr;
  const bool for_update = true;
  ObLogArchiveBackupInfo log_archive_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log archive info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(archive_info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, log_archive_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id));
  } else if (ObLogArchiveStatus::DOING != log_archive_info.status_.status_ &&
             ObLogArchiveStatus::INTERRUPTED != log_archive_info.status_.status_) {
    // do nothing
  } else if (OB_FAIL(log_archive_infos.push_back(log_archive_info))) {
    LOG_WARN("failed to push log archive info into array", K(ret), K(log_archive_info));
  }
  return ret;
}

int ObBackupDataClean::get_log_archive_history_info(const uint64_t tenant_id, common::ObISQLClient& trans,
    common::ObIArray<share::ObLogArchiveBackupInfo>& log_archive_infos)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr archive_info_mgr;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log archive info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(archive_info_mgr.get_log_archvie_history_infos(trans, tenant_id, for_update, log_archive_infos))) {
    LOG_WARN("failed to get log archive history infos", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupDataClean::get_extern_clean_tenants(
    hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>& clean_tenants_map)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObArray<ObTenantBackupTaskInfo> task_infos;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  hash::ObHashSet<ObBackupDest> backup_dest_set;
  ObMySQLTransaction trans;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean not init", K(ret));
  } else if (OB_FAIL(backup_dest_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create backup dest set", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get backup clean info", K(ret));
      }
    } else if (ObBackupCleanInfoStatus::STOP == sys_clean_info.status_) {
      // do nothing
    } else if (OB_FAIL(get_tenant_backup_task_his_info(sys_clean_info, trans, task_infos))) {
      LOG_WARN("failed to get tenant backup task his info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(get_tenant_backup_task_info(sys_clean_info, trans, task_infos))) {
      LOG_WARN("failed to get tenant backup task his info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(get_log_archive_history_info(OB_SYS_TENANT_ID, trans, log_archive_infos))) {
      LOG_WARN("failed to get log archive history info", K(ret));
    } else if (OB_FAIL(get_log_archive_info(OB_SYS_TENANT_ID, trans, log_archive_infos))) {
      LOG_WARN("failed to get log archive info", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }

    // get tenant info from log archive info
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
      const ObLogArchiveBackupInfo& info = log_archive_infos.at(i);
      ObBackupDest backup_dest;
      if (OB_FAIL(backup_dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", K(ret), K(info));
      } else {
        int hash_ret = backup_dest_set.exist_refactored(backup_dest);
        if (OB_HASH_EXIST == hash_ret) {
          // do nothing
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(get_archive_clean_tenant(info, clean_tenants_map))) {
            LOG_WARN("failed to get archive clean tenant", K(ret), K(info));
          } else if (OB_FAIL(backup_dest_set.set_refactored(backup_dest))) {
            LOG_WARN("failed to set backup dest", K(ret), K(info));
          }
        } else {
          ret = OB_SUCCESS != hash_ret ? hash_ret : OB_ERR_UNEXPECTED;
          LOG_WARN("failed to check exist from hash set", K(ret), K(backup_dest_set));
        }
      }
    }

    // get tenant info from backup tenant info
    backup_dest_set.reuse();

    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupTaskInfo& task_info = task_infos.at(i);
      int hash_ret = backup_dest_set.exist_refactored(task_info.backup_dest_);
      if (OB_HASH_EXIST == hash_ret) {
        // do nothing
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(get_backup_clean_tenant(task_info, clean_tenants_map))) {
          LOG_WARN("failed to get extern clean tenant", K(ret), K(task_info));
        } else if (OB_FAIL(backup_dest_set.set_refactored(task_info.backup_dest_))) {
          LOG_WARN("failed to set backup dest", K(ret), K(task_info));
        }
      } else {
        ret = hash_ret != OB_SUCCESS ? hash_ret : OB_ERR_UNEXPECTED;
        LOG_WARN("can not check exist from hash set", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_archive_clean_tenant(const share::ObLogArchiveBackupInfo& log_archive_info,
    hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>& clean_tenants_map)
{
  int ret = OB_SUCCESS;
  ObTenantNameSimpleMgr tenant_name_mgr;
  hash::ObHashSet<uint64_t> tenant_id_set;
  ObClusterBackupDest dest;
  // TODO() cluster dest
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean not init", K(ret));
  } else if (!log_archive_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get archive clean tenant get invalid argument", K(ret), K(log_archive_info));
  } else if (OB_FAIL(tenant_id_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create tenant id set", K(ret), K(log_archive_info));
  } else if (OB_FAIL(dest.set(log_archive_info.backup_dest_,
                 GCONF.cluster,
                 GCONF.cluster_id,
                 log_archive_info.status_.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(log_archive_info));
  } else if (OB_FAIL(tenant_name_mgr.init())) {
    LOG_WARN("faiuiled to init tenant_name mgr", K(ret));
  } else if (OB_FAIL(tenant_name_mgr.read_backup_file(dest))) {
    LOG_WARN("failed to read backup tenant name mgr", K(ret), K(dest));
  } else if (OB_FAIL(tenant_name_mgr.get_tenant_ids(tenant_id_set))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get tenant ids", K(ret), K(log_archive_info));
    }
  } else {
    for (hash::ObHashSet<uint64_t>::iterator iter = tenant_id_set.begin(); OB_SUCC(ret) && iter != tenant_id_set.end();
         ++iter) {
      ObSimpleBackupDataCleanTenant simple_clean_tenant;
      simple_clean_tenant.tenant_id_ = iter->first;
      simple_clean_tenant.is_deleted_ = true;
      int hash_ret = clean_tenants_map.set_refactored(simple_clean_tenant.tenant_id_, simple_clean_tenant);
      if (OB_SUCCESS == hash_ret) {
        // do nothing
      } else if (OB_HASH_EXIST == hash_ret) {
        ret = OB_SUCCESS;
      } else {
        ret = hash_ret;
        LOG_WARN("failed to set simple clean tenant info set", K(ret), K(simple_clean_tenant), K(log_archive_info));
      }
    }
  }

  return ret;
}

int ObBackupDataClean::get_backup_clean_tenant(const share::ObTenantBackupTaskInfo& task_info,
    hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>& clean_tenants_map)
{
  int ret = OB_SUCCESS;
  ObExternTenantInfoMgr tenant_info_mgr;
  ObClusterBackupDest backup_dest;
  char backup_dest_str[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObArray<ObExternTenantInfo> extern_tenant_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get extern clean tenant get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(task_info.backup_dest_.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", K(ret), K(task_info));
  } else if (OB_FAIL(backup_dest.set(backup_dest_str, GCONF.cluster, task_info.cluster_id_, task_info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(task_info));
  } else if (OB_FAIL(tenant_info_mgr.init(backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init tenant info mgr", K(ret), K(backup_dest));
  } else if (OB_FAIL(tenant_info_mgr.get_extern_tenant_infos(extern_tenant_infos))) {
    LOG_WARN("failed to get extern tenant infos", K(ret), K(backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extern_tenant_infos.count(); ++i) {
      ObSimpleBackupDataCleanTenant simple_clean_tenant;
      simple_clean_tenant.tenant_id_ = extern_tenant_infos.at(i).tenant_id_;
      simple_clean_tenant.is_deleted_ = true;
      int hash_ret = clean_tenants_map.set_refactored(simple_clean_tenant.tenant_id_, simple_clean_tenant);
      if (OB_SUCCESS == hash_ret) {
        // do nothing
      } else if (OB_HASH_EXIST == hash_ret) {
        ret = OB_SUCCESS;
      } else {
        ret = hash_ret;
        LOG_WARN("failed to set simple clean tenant info set", K(ret), K(simple_clean_tenant), K(task_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_clean_scheduler(ObIArray<ObBackupDataCleanTenant>& clean_tenants)
{
  LOG_INFO("start do clean scheduler", K(clean_tenants));
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (clean_tenants.empty()) {
    // do nothing
  } else if (OB_FAIL(do_schedule_clean_tenants(clean_tenants))) {
    LOG_WARN("failed to do schedule normal tenants", K(ret));
  } else if (OB_FAIL(do_check_clean_tenants_finished(clean_tenants))) {
    LOG_WARN("failed to do schedule sys tenant", K(ret));
  }
  return ret;
}

int ObBackupDataClean::do_schedule_clean_tenants(ObIArray<ObBackupDataCleanTenant>& clean_tenants)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t MAX_BUCKET_NUM = 1024;

  hash::ObHashSet<ObClusterBackupDest> cluster_backup_dest_set;
  LOG_INFO("start do clean scheduler", K(clean_tenants));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (clean_tenants.empty()) {
    // do nothing
  } else if (OB_FAIL(cluster_backup_dest_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create cluster backup dest set", K(ret), K(MAX_BUCKET_NUM));
  } else if (OB_FAIL(get_sys_tenant_backup_dest(cluster_backup_dest_set))) {
    LOG_WARN("failed to get sys tenant backup dest", K(ret));
  } else {
    // first clean normal tenant and then clean sys tenant
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenants.count(); ++i) {
      ObBackupCleanInfo clean_info;
      ObBackupDataCleanTenant& clean_tenant = clean_tenants.at(i);
      DEBUG_SYNC(BACKUP_DATA_CLEAN_STATUS_SCHEDULE);
      if (stop_) {
        ret = OB_RS_SHUTDOWN;
        LOG_WARN("rootservice shutdown", K(ret));
      } else if (OB_SUCCESS !=
                 (tmp_ret = do_tenant_clean_scheduler(cluster_backup_dest_set, clean_info, clean_tenant))) {
        LOG_WARN("failed to do tenant clean scheduler", K(tmp_ret), K(clean_tenant));
      } else if (OB_SUCCESS != (tmp_ret = do_with_status(clean_info, clean_tenant))) {
        LOG_WARN("failed to do tenant backup clean status", K(tmp_ret), K(clean_info), K(clean_tenant));
      }
      set_inner_error(tmp_ret);
    }
  }
  return ret;
}

int ObBackupDataClean::do_check_clean_tenants_finished(const common::ObIArray<ObBackupDataCleanTenant>& clean_tenants)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObArray<ObSimpleBackupDataCleanTenant> normal_clean_tenants;
  ObBackupDataCleanTenant sys_clean_tenant;
  int32_t clean_result = OB_SUCCESS;
  bool is_all_tasks_stopped = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (clean_tenants.empty()) {
    // do nothing
  } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_clean_info))) {
    LOG_WARN("failed to get sys clean info", K(ret), K(sys_clean_info));
  } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_ &&
             ObBackupCleanInfoStatus::CANCEL != sys_clean_info.status_) {
    // do nothing
  } else if (OB_FAIL(get_clean_tenants(clean_tenants, normal_clean_tenants, sys_clean_tenant))) {
    LOG_WARN("failed to get clean tenants", K(ret), K(clean_tenants));
  } else if (OB_FAIL(do_with_failed_tenant_clean_task(normal_clean_tenants, clean_result))) {
    LOG_WARN("failed to do with failed tenant clean task", K(ret));
  } else if (OB_FAIL(check_all_tenant_clean_tasks_stopped(normal_clean_tenants, is_all_tasks_stopped))) {
    LOG_WARN("failed ot check all tennat clean tasks stopped", K(ret));
  } else if (is_all_tasks_stopped && OB_FAIL(do_with_finished_tenant_clean_task(
                                         normal_clean_tenants, sys_clean_info, sys_clean_tenant, clean_result))) {
    LOG_WARN("failed to do with finished tenant clean task", K(ret));
  } else {
    wakeup();
  }
  return ret;
}

int ObBackupDataClean::do_with_status(const share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("rootservice shutdown", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(clean_info));
  } else {
    LOG_INFO("do with status", K(clean_info));
    const ObBackupCleanInfoStatus::STATUS& status = clean_info.status_;
    switch (status) {
      case ObBackupCleanInfoStatus::STOP:
        break;
      case ObBackupCleanInfoStatus::PREPARE:
        break;
      case ObBackupCleanInfoStatus::DOING:
        if (OB_FAIL(do_tenant_backup_clean(clean_info, clean_tenant))) {
          LOG_WARN("failed to do backup", K(ret), K(clean_info));
        }
        break;
      case ObBackupCleanInfoStatus::CANCEL:
        if (OB_FAIL(do_tenant_cancel_delete_backup(clean_info, clean_tenant))) {
          LOG_WARN("failed to do cancel delete backup", K(ret), K(clean_info));
        }
        break;
      case ObBackupCleanInfoStatus::MAX:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("backup clean info status is invalid", K(ret), K(clean_info));
        break;
    }
    LOG_INFO("doing backup clean", K(clean_info), K(ret));
  }
  return ret;
}

int ObBackupDataClean::prepare_tenant_backup_clean()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBackupCleanInfo sys_clean_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule get tenants backup get invalid argument", K(ret), K(tenant_ids));
  } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup clean info", K(ret));
    }
  } else if (ObBackupCleanInfoStatus::PREPARE != sys_clean_info.status_) {
    // do nothing
  } else if (OB_FAIL(mark_sys_tenant_backup_meta_data_deleted())) {
    LOG_WARN("failed to mark sys tenant backup meta data", K(ret), K(sys_clean_info));
  } else if (OB_FAIL(schedule_tenants_backup_data_clean(tenant_ids))) {
    LOG_WARN("failed to schedule tenant backup data clean", K(ret), K(tenant_ids));
  }

  set_inner_error(ret);
  return ret;
}

int ObBackupDataClean::mark_sys_tenant_backup_meta_data_deleted()
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObMySQLTransaction trans;
  ObBackupDataCleanTenant sys_clean_tenant;
  ObArray<ObTenantBackupTaskInfo> task_infos;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  CompareLogArchiveBackupInfo cmp;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (ObBackupCleanInfoStatus::PREPARE != sys_clean_info.status_) {
      // do nothing
    } else {
      sys_clean_tenant.simple_clean_tenant_.tenant_id_ = OB_SYS_TENANT_ID;
      sys_clean_tenant.simple_clean_tenant_.is_deleted_ = false;
      if (OB_FAIL(get_tenant_backup_task_his_info(sys_clean_info, trans, task_infos))) {
        LOG_WARN("failed to get backup task his info", K(ret), K(sys_clean_info));
      } else if (OB_FAIL(get_tenant_backup_task_info(sys_clean_info, trans, task_infos))) {
        LOG_WARN("failed to get backup task his info", K(ret), K(sys_clean_info));
      } else if (OB_FAIL(get_log_archive_history_info(sys_clean_info.tenant_id_, trans, log_archive_infos))) {
        LOG_WARN("failed to get log archive history info", K(ret), K(sys_clean_info));
      } else if (OB_FAIL(get_log_archive_info(sys_clean_info.tenant_id_, trans, log_archive_infos))) {
        LOG_WARN("failed to get log archive info", K(ret), K(sys_clean_info));
      } else if (FALSE_IT(std::sort(log_archive_infos.begin(), log_archive_infos.end(), cmp))) {
      } else if (OB_FAIL(get_backup_clean_elements(sys_clean_info, task_infos, log_archive_infos, sys_clean_tenant))) {
        LOG_WARN("failed to get backup clean elements", K(ret), K(sys_clean_info), K(task_infos));
      } else if (sys_clean_info.is_backup_set_clean() &&
                 !sys_clean_tenant.has_clean_backup_set(sys_clean_info.backup_set_id_)) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("can not clean backup set", K(ret), K(sys_clean_info), K(sys_clean_tenant));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true /*commit*/))) {
          LOG_WARN("failed to commit", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
          LOG_WARN("failed to rollback trans", K(tmp_ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != sys_clean_info.result_) {
          // do nothing
        } else if (OB_FAIL(
                       update_clog_gc_snaphost(sys_clean_info.clog_gc_snapshot_, sys_clean_info, sys_clean_tenant))) {
          LOG_WARN("failed to update clog gc snapshot", K(ret), K(sys_clean_info));
        } else if (OB_FAIL(mark_backup_meta_data_deleted(sys_clean_info, sys_clean_tenant))) {
          LOG_WARN("failed to mark backup meta data deleted", K(ret), K(sys_clean_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::schedule_tenants_backup_data_clean(const common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule get tenants backup get invalid argument", K(ret), K(tenant_ids));
  } else {
    DEBUG_SYNC(BACKUP_DATA_CLEAN_STATUS_PREPARE);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (ObBackupCleanInfoStatus::PREPARE != sys_clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys backup info status is unexpected", K(ret), K(sys_clean_info));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (stop_) {
          ret = OB_RS_SHUTDOWN;
          LOG_WARN("rootservice shutdown", K(ret));
        } else if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(schedule_tenant_backup_data_clean(tenant_id, sys_clean_info, trans))) {
          LOG_WARN("failed to schedule tenant backup", K(ret), K(tenant_id));
        }
      }
    }

    if (OB_SUCC(ret)) {

      dest_clean_info = sys_clean_info;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::DOING;
      if (OB_FAIL(update_clean_info(OB_SYS_TENANT_ID, sys_clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update clean info", K(ret), K(sys_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::schedule_tenant_backup_data_clean(
    const uint64_t tenant_id, const share::ObBackupCleanInfo& sys_clean_info, ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  int64_t parameter = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id should be sys tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    clean_info.tenant_id_ = tenant_id;
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    } else if (!clean_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup clean info should not be invalid", K(ret), K(clean_info));
    } else if (ObBackupCleanInfoStatus::PREPARE == clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("normal tenant backup clean info should not be PREPARE", K(ret), K(clean_info));
    } else if (ObBackupCleanInfoStatus::DOING == clean_info.status_) {
      // do nothing
    } else if (OB_FAIL(sys_clean_info.get_clean_parameter(parameter))) {
      LOG_WARN("failed to get clean parameter", K(ret), K(sys_clean_info));
    } else {
      dest_clean_info = clean_info;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::DOING;
      dest_clean_info.incarnation_ = sys_clean_info.incarnation_;
      dest_clean_info.job_id_ = sys_clean_info.job_id_;
      dest_clean_info.type_ = sys_clean_info.type_;
      dest_clean_info.start_time_ = ObTimeUtil::current_time();
      if (OB_FAIL(dest_clean_info.set_clean_parameter(parameter))) {
        LOG_WARN("failed to set clean parameter", K(ret), K(dest_clean_info), K(sys_clean_info));
      } else if (OB_FAIL(rootserver::ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
        LOG_WARN("failed to check sys tenant trans alive", K(ret), K(clean_info));
      } else if (OB_FAIL(update_clean_info(clean_info.tenant_id_, clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update backup clean info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_tenant_clean_scheduler(const hash::ObHashSet<ObClusterBackupDest>& cluster_backup_dest_set,
    ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  clean_tenant.backup_element_array_.reset();
  ObBackupCleanInfo sys_clean_info;
  ObArray<ObTenantBackupTaskInfo> task_infos;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  clean_info.reset();
  ObSimpleBackupDataCleanTenant& simple_clean_tenant = clean_tenant.simple_clean_tenant_;
  const int64_t start_ts = ObTimeUtil::current_time();
  // TODO copy_id
  const int64_t copy_id = 0;
  CompareLogArchiveBackupInfo log_archive_info_cmp;
  CompareBackupTaskInfo backup_task_info_cmp;
  LOG_INFO("start do tenant clean scheduler", K(clean_tenant));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant clean scheduler get invalid argument", K(ret), K(clean_tenant));
  } else {
    if (!simple_clean_tenant.is_deleted_) {
      if (OB_FAIL(do_scheduler_normal_tenant(clean_info, clean_tenant, task_infos, log_archive_infos))) {
        LOG_WARN("failed to do scheduler normal tenant", K(ret), K(clean_tenant));
      }
    } else {
      if (OB_FAIL(do_scheduler_deleted_tenant(
              cluster_backup_dest_set, clean_info, clean_tenant, task_infos, log_archive_infos))) {
        LOG_WARN("failed to do scheduler deleted tenant", K(ret), K(clean_info), K(clean_tenant));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_ ||
               ObBackupCleanInfoStatus::PREPARE == clean_info.status_) {
      // do nothing
    } else if (FALSE_IT(std::sort(log_archive_infos.begin(), log_archive_infos.end(), log_archive_info_cmp))) {
    } else if (FALSE_IT(std::sort(task_infos.begin(), task_infos.end(), backup_task_info_cmp))) {
    } else if (OB_FAIL(get_backup_clean_elements(clean_info, task_infos, log_archive_infos, clean_tenant))) {
      LOG_WARN("failed to get backup clean elements", K(ret), K(clean_tenant), K(task_infos));
    } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_tenant));
    } else if (OB_FAIL(update_clog_gc_snaphost(sys_clean_info.clog_gc_snapshot_, clean_info, clean_tenant))) {
      LOG_WARN("failed to update clog gc snapshot", K(ret), K(sys_clean_info), K(clean_tenant));
    }
  }

  LOG_INFO("finish prepare tenant clean info",
      "cost",
      ObTimeUtil::current_time() - start_ts,
      K(clean_tenant),
      K(clean_info));
  return ret;
}

// ObBackupdataCleanTeannt
//   -- ObBackupDataCleanElement[0]
//   -- ObbackupDest
//   -- log_archive_round_array (ascending order)
//   -- backup_set_id_array(descending order)
int ObBackupDataClean::get_backup_clean_elements(const share::ObBackupCleanInfo& clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo>& task_infos,
    const common::ObArray<share::ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo min_include_task_info;
  ;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean elements get invalid argument", K(ret), K(clean_tenant), K(clean_info));
  } else if (OB_FAIL(add_log_archive_infos(log_archive_infos, clean_tenant))) {
    LOG_WARN("failed to add log archive infos", K(ret), K(clean_info), K(clean_tenant));
  } else if (clean_info.is_backup_set_clean()) {
    if (OB_FAIL(
            add_delete_backup_set(clean_info, task_infos, log_archive_infos, clean_tenant, min_include_task_info))) {
      LOG_WARN("failed to add delete backup set", K(ret), K(clean_info), K(clean_tenant));
    }
  } else if (clean_info.is_expired_clean()) {
    if (OB_FAIL(
            add_expired_backup_set(clean_info, task_infos, log_archive_infos, clean_tenant, min_include_task_info))) {
      LOG_WARN("failed to add expired backup set", K(ret), K(clean_info), K(clean_tenant));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean info type is invalid", K(ret), K(clean_info));
  }

  if (OB_SUCC(ret)) {
    clean_tenant.clog_data_clean_point_ = min_include_task_info;
  }
  return ret;
}

int ObBackupDataClean::add_log_archive_infos(
    const common::ObIArray<share::ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
      const ObLogArchiveBackupInfo& log_archive_info = log_archive_infos.at(i);
      ObBackupDest backup_dest;
      ObLogArchiveRound log_archive_round;
      log_archive_round.log_archive_round_ = log_archive_info.status_.round_;
      log_archive_round.log_archive_status_ = log_archive_info.status_.status_;
      log_archive_round.start_ts_ = log_archive_info.status_.start_ts_;
      log_archive_round.checkpoint_ts_ = log_archive_info.status_.checkpoint_ts_;

      if (OB_FAIL(backup_dest.set(log_archive_info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", K(ret), K(log_archive_info));
      } else if (OB_FAIL(clean_tenant.set_backup_clean_archive_round(
                     GCONF.cluster_id, log_archive_info.status_.incarnation_, backup_dest, log_archive_round))) {
        LOG_WARN("failed to set backup clean element", K(ret), K(log_archive_info), K(clean_tenant));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_delete_backup_set(const share::ObBackupCleanInfo& clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo>& task_infos,
    const common::ObArray<ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant,
    ObTenantBackupTaskInfo& clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  int64_t cluster_max_backup_set_id = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup set get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(get_cluster_max_succeed_backup_set(cluster_max_backup_set_id))) {
    LOG_WARN("failed to get cluster max succeed backup set", K(ret), K(clean_info));
  } else {
    ObTenantBackupTaskInfo delete_backup_info;
    bool has_kept_last_succeed_data = false;
    bool is_continue = false;
    for (int64_t i = task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObTenantBackupTaskInfo& task_info = task_infos.at(i);
      is_continue = false;
      // consider can clean up
      if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
        if (task_info.backup_set_id_ == clean_info.backup_set_id_) {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean", K(ret), K(clean_info), K(cluster_max_backup_set_id), K(task_info));
        }
      } else if (task_info.backup_set_id_ == clean_info.backup_set_id_) {
        delete_backup_info = task_info;
        if (!clog_data_clean_point.is_valid() && task_info.is_result_succeed()) {
          clog_data_clean_point = task_info;
        }
      } else if (!task_info.is_result_succeed()) {
        // do nothing
      } else if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
        LOG_WARN("failed to check backupset contine with clog data", K(ret), K(task_info));
      } else if (!is_continue) {
        // do nothing
      } else {
        has_kept_last_succeed_data = true;
        clog_data_clean_point = task_info;
      }
    }

    if (OB_SUCC(ret)) {
      ObBackupSetId backup_set_id;
      backup_set_id.backup_set_id_ = delete_backup_info.backup_set_id_;
      if (!delete_backup_info.is_valid()) {
        // do nothing
        // The backup_set has been checked when the backup is initiated
        // Here, if keep_backup_info is invalid, it may be deleted and there will be a change in the future.
      } else {
        if (!delete_backup_info.is_result_succeed()) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
        } else if (!has_kept_last_succeed_data && delete_backup_info.backup_set_id_ >= cluster_max_backup_set_id) {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean",
              K(ret),
              K(clean_info),
              K(cluster_max_backup_set_id),
              K(delete_backup_info));
        } else if (clean_tenant.simple_clean_tenant_.is_deleted_) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
        } else if (has_kept_last_succeed_data) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
        } else {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean",
              K(ret),
              K(clean_info),
              K(cluster_max_backup_set_id),
              K(delete_backup_info),
              K(has_kept_last_succeed_data),
              K(clog_data_clean_point));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(clean_tenant.set_backup_clean_backup_set_id(delete_backup_info.cluster_id_,
                       delete_backup_info.incarnation_,
                       delete_backup_info.backup_dest_,
                       backup_set_id))) {
          LOG_WARN("failed to set backup clean element", K(ret), K(delete_backup_info), K(clean_tenant));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_expired_backup_set(const share::ObBackupCleanInfo& clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo>& task_infos,
    const common::ObArray<ObLogArchiveBackupInfo>& log_archive_infos, ObBackupDataCleanTenant& clean_tenant,
    ObTenantBackupTaskInfo& clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  bool has_kept_last_succeed_data = false;
  int64_t cluster_max_backup_set_id = 0;
  bool is_continue = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup set get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(get_cluster_max_succeed_backup_set(cluster_max_backup_set_id))) {
    LOG_WARN("failed to get cluster max succeed backup set", K(ret), K(clean_info));
  } else {
    for (int64_t i = task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      is_continue = false;
      const ObTenantBackupTaskInfo& task_info = task_infos.at(i);
      ObBackupSetId backup_set_id;
      backup_set_id.backup_set_id_ = task_info.backup_set_id_;
      bool need_add_backup_set_id = true;
      if (task_info.snapshot_version_ <= clean_info.expired_time_) {
        if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
        } else if (!task_info.is_result_succeed()) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
        } else if (!has_kept_last_succeed_data && task_info.backup_set_id_ >= cluster_max_backup_set_id) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
          if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
            LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
          } else if (!is_continue) {
            // do nothing
          } else {
            has_kept_last_succeed_data = true;
            clog_data_clean_point = task_info;
          }
        } else if (clean_tenant.simple_clean_tenant_.is_deleted_) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
          if (!clog_data_clean_point.is_valid()) {
            clog_data_clean_point = task_info;
          }
        } else if (has_kept_last_succeed_data) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
          if (!clog_data_clean_point.is_valid()) {
            clog_data_clean_point = task_info;
          }
        } else {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
          if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
            LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
          } else if (!is_continue) {
          } else {
            has_kept_last_succeed_data = true;
            clog_data_clean_point = task_info;
          }
        }
      } else {
        if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
        } else if (task_info.is_result_succeed()) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
          if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
            LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
          } else if (!is_continue) {
          } else {
            clog_data_clean_point = task_info;
          }
        } else {
          // do nothing
          need_add_backup_set_id = false;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (need_add_backup_set_id &&
                 OB_FAIL(clean_tenant.set_backup_clean_backup_set_id(
                     task_info.cluster_id_, task_info.incarnation_, task_info.backup_dest_, backup_set_id))) {
        LOG_WARN("failed to set backup clean element", K(ret), K(task_info), K(clean_tenant));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_tenant_backup_clean(
    const share::ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant)
{
  LOG_INFO("start do tenant backup clean", K(clean_info));
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean elements get invalid argument", K(ret), K(clean_tenant), K(clean_info));
  }

  DEBUG_SYNC(BACKUP_DATA_CLEAN_STATUS_DOING);
  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    // do nothing
  } else if (OB_FAIL(do_normal_tenant_backup_clean(clean_info, clean_tenant))) {
    LOG_WARN("failed to do sys tenant backup clean", K(ret), K(clean_info), K(clean_tenant));
  }

  return ret;
}

int ObBackupDataClean::do_normal_tenant_backup_clean(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  LOG_INFO("do normal tenant backup clean", K(clean_info), K(clean_tenant));
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_info.is_valid() ||
             OB_SYS_TENANT_ID == clean_tenant.simple_clean_tenant_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean elements get invalid argument", K(ret), K(clean_tenant), K(clean_info));
  } else if (OB_FAIL(mark_backup_meta_data_deleted(clean_info, clean_tenant))) {
    LOG_WARN("failed to mark backup data deleted", K(ret), K(clean_tenant), K(clean_info));
  } else if (OB_FAIL(delete_backup_data(clean_info, clean_tenant))) {
    LOG_WARN("failed to delete backup data", K(ret), K(clean_info), K(clean_tenant));
  } else if (OB_FAIL(delete_tenant_backup_meta_data(clean_info, clean_tenant))) {
    LOG_WARN("failed to delete backup meta data", K(ret), K(clean_tenant));
  } else if (OB_FAIL(try_clean_tenant_backup_dir(clean_tenant))) {
    LOG_WARN("failed to try clean tenant backup dir", K(ret), K(clean_info));
  }

  if (is_result_need_retry(ret)) {
    // do nothing
  } else if (OB_SUCCESS != (tmp_ret = update_normal_tenant_clean_result(clean_info, clean_tenant, ret))) {
    LOG_WARN("failed to update nomal tenant clean result", K(ret), K(clean_info), K(clean_tenant));
  }

  if (OB_SUCC(ret)) {
    ret = tmp_ret;
  }

  if (OB_SUCC(ret)) {
    wakeup();
  }

  LOG_INFO("finish do normal tenant backup clean",
      "cost_ts",
      ObTimeUtil::current_time() - start_ts,
      K(clean_tenant),
      K(ret));

  return ret;
}

int ObBackupDataClean::update_normal_tenant_clean_result(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant, const int32_t clean_result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupCleanInfo dest_clean_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_info.is_valid() ||
             OB_SYS_TENANT_ID == clean_tenant.simple_clean_tenant_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean elements get invalid argument", K(ret), K(clean_tenant), K(clean_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(clean_info));
  } else {
    dest_clean_info = clean_info;
    dest_clean_info.result_ = clean_result;
    dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
    dest_clean_info.end_time_ = ObTimeUtil::current_time();
    const uint64_t tenant_id = clean_tenant.simple_clean_tenant_.is_deleted_ ? OB_SYS_TENANT_ID : clean_info.tenant_id_;

    if (OB_SUCCESS != clean_result && OB_FAIL(dest_clean_info.error_msg_.assign(ob_strerror(clean_result)))) {
      LOG_WARN("failed to assign error msg", K(ret), K(dest_clean_info));
    }
    DEBUG_SYNC(BACKUP_DATA_NORMAL_TENANT_CLEAN_STATUS_DOING);

    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS == clean_result && OB_FAIL(delete_inner_table_his_data(clean_info, trans))) {
      LOG_WARN("failed to delete inner table his data", K(ret), K(clean_info));
    } else if (OB_FAIL(update_clean_info(tenant_id, clean_info, dest_clean_info))) {
      LOG_WARN("failed to update clean info", K(ret), K(clean_info), K(dest_clean_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_backup_meta_data_deleted(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(mark_extern_backup_infos_deleted(clean_info, clean_tenant))) {
    LOG_WARN("failed to mark extern backup infos deleted", K(ret), K(clean_info));
  } else if (OB_FAIL(mark_inner_table_his_data_deleted(clean_info, clean_tenant))) {
    LOG_WARN("failed to mark backup task his data deleted", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::mark_extern_backup_infos_deleted(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark_extern_backup_infos_deleted get invalid argument", K(ret), K(clean_info), K(clean_tenant));
  } else {
    const ObIArray<ObBackupDataCleanElement>& backup_element_array = clean_tenant.backup_element_array_;
    const int64_t clog_gc_snapshot = clean_info.clog_gc_snapshot_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement& clean_element = backup_element_array.at(i);
      if (OB_FAIL(mark_extern_backup_info_deleted(clean_info, clean_element))) {
        LOG_WARN("failed to makr extern backup info deleted", K(ret), K(clean_info), K(clean_element));
      } else if (OB_FAIL(mark_extern_clog_info_deleted(clean_info, clean_element, clog_gc_snapshot))) {
        LOG_WARN("failed to mark extern clog info deleted", K(ret), K(clean_info), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_extern_backup_info_deleted(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObClusterBackupDest backup_dest;
  ObArray<int64_t> backup_set_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(extern_backup_info_mgr.init(clean_info.tenant_id_, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(backup_dest), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.backup_set_id_array_.count(); ++i) {
      const ObBackupSetId& backup_set_id = clean_element.backup_set_id_array_.at(i);
      if (!backup_set_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set id is invalid ", K(ret), K(backup_set_id));
      } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
        // do nothing
      } else {
        if (OB_FAIL(backup_set_ids.push_back(backup_set_id.backup_set_id_))) {
          LOG_WARN("failed to push backup set id into array", K(ret), K(backup_set_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(extern_backup_info_mgr.mark_backup_info_deleted(backup_set_ids))) {
        LOG_WARN("failed to mark backup info deleted", K(ret), K(backup_set_ids));
      } else if (!extern_backup_info_mgr.is_extern_backup_infos_modified()) {
        // do nothing
      } else if (OB_FAIL(extern_backup_info_mgr.upload_backup_info())) {
        LOG_WARN("failed to upload backup info", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_extern_clog_info_deleted(const share::ObBackupCleanInfo& clean_info,
    const ObBackupDataCleanElement& clean_element, const int64_t clog_gc_snapshot)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_backup_info;
  ObClusterBackupDest backup_dest;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(log_archive_backup_info.mark_extern_log_archive_backup_info_deleted(
                 backup_dest, clean_info.tenant_id_, clog_gc_snapshot, *backup_lease_service_))) {
    LOG_WARN("failed to mark extern log archive backup info deleted", K(ret), K(backup_dest));
  }
  return ret;
}

int ObBackupDataClean::mark_inner_table_his_data_deleted(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const int64_t clog_gc_snapshot = clean_info.clog_gc_snapshot_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement& backup_clean_element = clean_tenant.backup_element_array_.at(i);
      if (OB_FAIL(mark_backup_task_his_data_deleted(clean_info, backup_clean_element, trans))) {
        LOG_WARN("failed to mark backup task his data deleted", K(ret), K(clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(mark_log_archive_stauts_his_data_deleted(clean_info, clog_gc_snapshot, trans))) {
        LOG_WARN("failed to mark log archive status his data deleted", K(ret), K(clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_backup_task_his_data_deleted(const share::ObBackupCleanInfo& clean_info,
    const ObBackupDataCleanElement& clean_element, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = clean_info.tenant_id_;
  const int64_t incarnation = clean_element.incarnation_;
  const ObBackupDest& backup_dest = clean_element.backup_dest_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.backup_set_id_array_.count(); ++i) {
      const ObBackupSetId& backup_set_id = clean_element.backup_set_id_array_.at(i);
      if (ObBackupDataCleanMode::CLEAN == backup_set_id.clean_mode_) {
        if (OB_FAIL(inner_mark_backup_task_his_data_deleted(
                tenant_id, incarnation, backup_set_id.backup_set_id_, backup_dest, trans))) {
          LOG_WARN("failed to do inner mark backup task his data deleted", K(ret), K(backup_set_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::inner_mark_backup_task_his_data_deleted(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const ObBackupDest& backup_dest, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  ObArray<ObTenantBackupTaskInfo> backup_tasks;

  if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(updater.get_need_mark_deleted_backup_tasks(
                 tenant_id, backup_set_id, incarnation, backup_dest, backup_tasks))) {
    LOG_WARN("failed to get need mark deleted backup tasks", K(ret), K(tenant_id), K(backup_set_id), K(backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_tasks.count(); ++i) {
      const ObTenantBackupTaskInfo& backup_task_info = backup_tasks.at(i);
      if (OB_FAIL(updater.mark_backup_task_deleted(
              backup_task_info.tenant_id_, backup_task_info.incarnation_, backup_task_info.backup_set_id_))) {
        LOG_WARN("failed to mark backup task deleted", K(ret), K(backup_task_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_log_archive_stauts_his_data_deleted(
    const share::ObBackupCleanInfo& clean_info, const int64_t clog_gc_snapshot, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = clean_info.tenant_id_;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  ObLogArchiveBackupInfoMgr log_archive_backup_info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_log_archive_history_info(tenant_id, trans, log_archive_infos))) {
    LOG_WARN("failed to get log archive history info", K(ret), K(clean_info));
  } else if (log_archive_infos.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
      const ObLogArchiveBackupInfo& archive_info = log_archive_infos.at(i);
      if (archive_info.status_.checkpoint_ts_ < clog_gc_snapshot) {
        if (OB_FAIL(log_archive_backup_info_mgr.mark_log_archive_history_info_deleted(archive_info, trans))) {
          LOG_WARN("failed to mark log archive history info deleted", K(ret), K(archive_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_backup_data(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  ObTenantBackupDataCleanMgr tenant_backup_data_clean_mgr;
  if (OB_FAIL(tenant_backup_data_clean_mgr.init(clean_tenant, this))) {
    LOG_WARN("failed to init tenant backup data clean mgr", K(ret), K(clean_info), K(clean_tenant));
  } else if (OB_FAIL(tenant_backup_data_clean_mgr.do_clean())) {
    LOG_WARN("failed to do clean", K(ret), K(clean_info), K(clean_tenant));
  }

  return ret;
}

int ObBackupDataClean::delete_tenant_backup_meta_data(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || OB_SYS_TENANT_ID == clean_info.tenant_id_ || !clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete tenant backup meta data get invalid argument", K(ret), K(clean_info), K(clean_tenant));
  } else if (OB_FAIL(delete_backup_extern_infos(clean_info, clean_tenant))) {
    LOG_WARN("failed to mark extern backup infos deleted", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::delete_backup_extern_infos(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark_extern_backup_infos_deleted get invalid argument", K(ret), K(clean_info), K(clean_tenant));
  } else {
    const ObIArray<ObBackupDataCleanElement>& backup_element_array = clean_tenant.backup_element_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement& clean_element = backup_element_array.at(i);
      if (OB_FAIL(delete_extern_backup_info_deleted(clean_info, clean_element))) {
        LOG_WARN("failed to makr extern backup info deleted", K(ret), K(clean_info), K(clean_element));
      } else if (OB_FAIL(delete_extern_clog_info_deleted(clean_info, clean_element))) {
        LOG_WARN("failed to mark extern clog info deleted", K(ret), K(clean_info), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_extern_backup_info_deleted(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObClusterBackupDest backup_dest;
  ObBackupPath prefix_path;
  ObBackupPath path;
  const ObStorageType device_type = clean_element.backup_dest_.device_type_;
  ObArray<ObExternBackupInfo> extern_backup_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    if (OB_FAIL(ObBackupPathUtil::get_cluster_prefix_path(backup_dest, prefix_path))) {
      LOG_WARN("failed to get cluster prefix path", K(ret), K(backup_dest), K(clean_info));
    } else if (OB_FAIL(ObBackupPathUtil::get_cluster_data_backup_info_path(backup_dest, path))) {
      LOG_WARN("failed to get cluster data backup info path", K(ret), K(backup_dest), K(clean_info));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_data_path(backup_dest, clean_info.tenant_id_, prefix_path))) {
      LOG_WARN("failed to get tenant prefix path", K(ret), K(backup_dest), K(clean_info));
    } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_backup_info_path(backup_dest, clean_info.tenant_id_, path))) {
      LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest), K(clean_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(prefix_path, backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete tmp files", K(ret), K(prefix_path));
  } else if (OB_FAIL(extern_backup_info_mgr.init(clean_info.tenant_id_, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(backup_dest), K(clean_info));
  } else if (OB_FAIL(extern_backup_info_mgr.delete_marked_backup_info())) {
    LOG_WARN("failed to delete marked backup info", K(ret), K(clean_element));
  } else if (extern_backup_info_mgr.is_empty()) {
    if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_file(path, backup_dest.get_storage_info(), device_type))) {
      LOG_WARN("failed to delete backup file", K(ret), K(backup_dest));
    }
  } else if (!extern_backup_info_mgr.is_extern_backup_infos_modified()) {
    if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_file(path, backup_dest.get_storage_info(), device_type))) {
      LOG_WARN("failed to touch backup file", K(ret), K(backup_dest));
    }
  } else {
    if (OB_FAIL(extern_backup_info_mgr.upload_backup_info())) {
      LOG_WARN("failed to upload backup info", K(ret), K(backup_dest));
    }
  }
  return ret;
}

int ObBackupDataClean::delete_extern_clog_info_deleted(
    const share::ObBackupCleanInfo& clean_info, const ObBackupDataCleanElement& clean_element)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_backup_info;
  ObClusterBackupDest backup_dest;
  ObBackupPath prefix_path;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    if (OB_FAIL(ObBackupPathUtil::get_cluster_prefix_path(backup_dest, prefix_path))) {
      LOG_WARN("failed to get cluster prefix path", K(ret), K(backup_dest), K(clean_info));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_path(backup_dest, clean_info.tenant_id_, prefix_path))) {
      LOG_WARN("failed to get tenant prefix path", K(ret), K(backup_dest), K(clean_info));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(prefix_path, backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete tmp files", K(ret), K(prefix_path));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_path(backup_dest, clean_info.tenant_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest), K(clean_info));
    // TODO
    // } else if (OB_FAIL(log_archive_backup_info.set_copy_id(clean_info.copy_id_))) {
    //   LOG_WARN("failed to set copy id", K(ret), K(clean_info));
  } else if (OB_FAIL(log_archive_backup_info.delete_marked_extern_log_archive_backup_info(
                 backup_dest, clean_info.tenant_id_, *backup_lease_service_))) {
    LOG_WARN("failed to mark extern log archive backup info deleted", K(ret), K(backup_dest));
  }

  return ret;
}

int ObBackupDataClean::delete_inner_table_his_data(
    const share::ObBackupCleanInfo& clean_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = clean_info.tenant_id_;
  const int64_t job_id = clean_info.job_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(delete_marked_backup_task_his_data(tenant_id, job_id, trans))) {
    LOG_WARN("failed to delete marked backup task his data", K(ret), K(tenant_id), K(clean_info));
  } else if (OB_FAIL(delete_marked_log_archive_status_his_data(tenant_id, trans))) {
    LOG_WARN("failed to delete marked log archive status his data", K(ret), K(tenant_id), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::delete_marked_backup_task_his_data(
    const uint64_t tenant_id, const int64_t job_id, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  ObArray<ObTenantBackupTaskInfo> tenant_backup_tasks;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(updater.get_mark_deleted_backup_tasks(tenant_id, tenant_backup_tasks))) {
    LOG_WARN("failed to get mark deeted backup tasks", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_backup_tasks.count(); ++i) {
      const ObTenantBackupTaskInfo& tenant_backup_task = tenant_backup_tasks.at(i);
      if (OB_FAIL(ObBackupTaskCleanHistoryOpertor::insert_task_info(job_id, tenant_backup_task, trans))) {
        LOG_WARN("failed to insert task into clean hisotry", K(ret), K(tenant_backup_task));
      } else if (OB_FAIL(updater.delete_backup_task(tenant_backup_task))) {
        LOG_WARN("failed to delete backup task", K(ret), K(tenant_backup_task));
      }
    }
  }

  return ret;
}

int ObBackupDataClean::delete_marked_log_archive_status_his_data(const uint64_t tenant_id, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_backup_info_mgr;
  if (OB_FAIL(log_archive_backup_info_mgr.delete_marked_log_archive_history_infos(tenant_id, trans))) {
    LOG_WARN("failed to delete marked log archive history infos", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupDataClean::update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo& src_clean_info,
    const share::ObBackupCleanInfo& dest_clean_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(update_clean_info(tenant_id, src_clean_info, dest_clean_info, *sql_proxy_))) {
    LOG_WARN("failed to update clean info", K(ret), K(src_clean_info), K(dest_clean_info));
  }
  return ret;
}

int ObBackupDataClean::update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo& src_clean_info,
    const share::ObBackupCleanInfo& dest_clean_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!src_clean_info.is_valid() || !dest_clean_info.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update clean info get invalid argument", K(ret), K(src_clean_info), K(dest_clean_info), K(tenant_id));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init tenant backup clean info updater", K(ret), K(src_clean_info));
  } else if (OB_FAIL(updater.update_backup_clean_info(tenant_id, src_clean_info, dest_clean_info))) {
    LOG_WARN("failed to update backup clean info", K(ret), K(src_clean_info), K(dest_clean_info));
  }
  return ret;
}

void ObBackupDataClean::cleanup_prepared_infos()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  bool need_clean = false;
  const int64_t EXECUTE_TIMEOUT_US = 100L * 100 * 1000;      // 100ms
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  int64_t calc_stmt_timeout = EXECUTE_TIMEOUT_US;
  ObBackupCleanInfo sys_clean_info;
  ObBackupCleanInfo dest_sys_clean_info;
  ObMySQLTransaction trans;
  ObArray<ObSimpleBackupDataCleanTenant> normal_clean_tenants;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (tenant_ids.empty()) {
    // do nothing
  } else if (OB_SYS_TENANT_ID != tenant_ids.at(0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first tenant id should be sys tenant", K(ret), K(tenant_ids));
  } else if (FALSE_IT(calc_stmt_timeout = tenant_ids.count() * EXECUTE_TIMEOUT_US)) {
  } else if (FALSE_IT(stmt_timeout =
                          (calc_stmt_timeout > MIN_EXECUTE_TIMEOUT_US ? calc_stmt_timeout : MIN_EXECUTE_TIMEOUT_US))) {
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("failed to set trx timeout us", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
        need_clean = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("faield to get backup clean info", K(ret));
      }
    } else if (OB_FAIL(check_need_cleanup_prepared_infos(sys_clean_info, need_clean))) {
      LOG_WARN("failed to check need clean prepared infos", K(ret));
    } else if (!need_clean) {
      // do nothing
    } else {
      // normal tenants, skip sys tenant
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(cleanup_tenant_prepared_infos(tenant_id, trans))) {
          LOG_WARN("failed to cleanup stoppped tenant infos", K(ret), K(tenant_id));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else {
          ObSimpleBackupDataCleanTenant simple_clean_tenant;
          simple_clean_tenant.tenant_id_ = tenant_id;
          simple_clean_tenant.is_deleted_ = false;
          if (OB_FAIL(normal_clean_tenants.push_back(simple_clean_tenant))) {
            LOG_WARN("failed to push simple clean tenant into array", K(ret), K(simple_clean_tenant));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(insert_clean_infos_into_history(normal_clean_tenants, trans))) {
          LOG_WARN("failed to insert clean infos into history", K(ret));
        } else if (OB_FAIL(reset_backup_clean_infos(normal_clean_tenants, trans))) {
          LOG_WARN("failed to reset backup clean infos", K(ret));
        } else {
          // result
          if (OB_SUCCESS != sys_clean_info.result_) {
            // do nothing
          } else if (OB_SUCCESS != inner_error_) {
            sys_clean_info.result_ = inner_error_;
            LOG_INFO("set sys clean info result failed", K(sys_clean_info), K(inner_error_));
          } else {
            sys_clean_info.result_ = OB_CANCELED;
            LOG_INFO("set sys clean info result cancel", K(ret), K(sys_clean_info));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(set_sys_clean_info_stop(sys_clean_info, trans))) {
          LOG_WARN("failed to set sys clean info stop", K(ret), K(sys_clean_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
}

int ObBackupDataClean::check_need_cleanup_prepared_infos(const ObBackupCleanInfo& sys_clean_info, bool& need_clean)
{
  int ret = OB_SUCCESS;
  need_clean = false;
  bool is_prepare_flag = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data cleando not init", K(ret));
  } else if (FALSE_IT(is_prepare_flag = get_prepare_flag())) {
  } else if (ObBackupCleanInfoStatus::PREPARE == sys_clean_info.status_ && !is_prepare_flag) {
    need_clean = true;
    LOG_INFO("sys tenant is in prepare status, need cleanup", K(ret), K(sys_clean_info), K(is_prepare_flag));
  }
  return ret;
}

int ObBackupDataClean::cleanup_tenant_prepared_infos(const uint64_t tenant_id, ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("rootservice shutdown", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is is invalid", K(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("failed to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    clean_info.tenant_id_ = tenant_id;
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    } else if (ObBackupCleanInfoStatus::STOP != clean_info.status_ &&
               ObBackupCleanInfoStatus::DOING != clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info status is unexpected", K(ret), K(clean_info));
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_) {
      // do nothing
    } else {
      dest_clean_info = clean_info;
      dest_clean_info.result_ = OB_CANCELED;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
      if (OB_FAIL(dest_clean_info.error_msg_.assign(ob_strerror(OB_CANCELED)))) {
        LOG_WARN("failed to assign error msg", K(ret), K(dest_clean_info));
      } else if (OB_FAIL(ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
        LOG_WARN("failed to check sys tenant trans alive", K(ret), K(clean_info));
      } else if (OB_FAIL(update_clean_info(clean_info.tenant_id_, clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update tenant backup info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::insert_tenant_backup_clean_info_history(
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant, common::ObISQLClient& sys_trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
  ObBackupCleanInfoHistoryUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (simple_clean_tenant.is_deleted_) {
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_trans, clean_info))) {
      LOG_WARN("failed to get backup data clean info", K(ret));
    }
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy_))) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, trans, clean_info))) {
        LOG_WARN("failed to get backup data clean info", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true /*commit*/))) {
          OB_LOG(WARN, "failed to commit", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
          OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObBackupCleanInfoStatus::STOP != clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean info status is unexpected, can not insert into task", K(ret), K(clean_info));
  } else if (clean_info.is_empty_clean_type()) {
    // do nothing
  } else if (OB_FAIL(updater.init(sys_trans))) {
    LOG_WARN("failed to init clean info history updater", K(ret));
  } else if (OB_FAIL(updater.insert_backup_clean_info(clean_info))) {
    LOG_WARN("failed to insert backup clean info into history", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::do_with_failed_tenant_clean_task(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, int32_t& clean_result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
  clean_result = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup data clean info", K(ret));
    } else if (OB_FAIL(
                   check_tenant_backup_clean_task_failed(normal_clean_tenants, sys_clean_info, trans, clean_result))) {
      LOG_WARN("failed to check tenant backup clean task failed", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(update_tenant_backup_clean_task_failed(normal_clean_tenants, trans, clean_result))) {
      LOG_WARN("failed to update tenant backup clean task failed", K(ret));
    } else if (OB_SUCCESS == clean_result) {
      // do nothing
    } else if (OB_SUCCESS == sys_clean_info.result_) {
      ObBackupCleanInfo dest_clean_info = sys_clean_info;
      dest_clean_info.result_ = clean_result;
      if (OB_FAIL(update_clean_info(OB_SYS_TENANT_ID, sys_clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update clean info", K(ret), K(sys_clean_info), K(dest_clean_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::check_tenant_backup_clean_task_failed(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
    const ObBackupCleanInfo& sys_clean_info, common::ObISQLClient& sys_tenant_trans, int32_t& result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SUCCESS != sys_clean_info.result_) {
    result = sys_clean_info.result_;
  } else if (OB_SUCCESS != inner_error_) {
    result = inner_error_;
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCCESS == result && OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = normal_clean_tenants.at(i);
      ObBackupCleanInfo tenant_clean_info;
      ObMySQLTransaction trans;
      tenant_clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal clean tenant should not be sys tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_tenant_trans, tenant_clean_info))) {
          LOG_WARN("failed to push tenant clean info into array", K(ret), K(tenant_clean_info));
        } else if (ObBackupCleanInfoStatus::STOP != tenant_clean_info.status_) {
          // do nothing
        } else if (OB_SUCCESS != tenant_clean_info.result_) {
          result = tenant_clean_info.result_;
        }
      } else {
        if (OB_FAIL(trans.start(sql_proxy_))) {
          LOG_WARN("fail to start trans", K(ret));
        } else {
          if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, trans, tenant_clean_info))) {
            LOG_WARN("failed to push tenant clean info into array", K(ret), K(simple_clean_tenant));
          } else if (OB_FAIL(rootserver::ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
            LOG_WARN("failed to check sys tenant trans alive", K(ret), K(tenant_clean_info));
          } else if (ObBackupCleanInfoStatus::STOP != tenant_clean_info.status_) {
            // do nothing
          } else if (OB_SUCCESS != tenant_clean_info.result_) {
            result = tenant_clean_info.result_;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans.end(true /*commit*/))) {
              OB_LOG(WARN, "failed to commit", K(ret));
            }
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
              OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::update_tenant_backup_clean_task_failed(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, common::ObISQLClient& sys_tenant_trans,
    const int32_t result)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SUCCESS == result) {
    // do nothing
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = normal_clean_tenants.at(i);
      ObBackupCleanInfo tenant_clean_info;
      ObMySQLTransaction trans;
      tenant_clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("clean tenant should not be sys clean tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_tenant_trans, tenant_clean_info))) {
          LOG_WARN("failed to push tenant clean info into array", K(ret), K(simple_clean_tenant));
        } else if (ObBackupCleanInfoStatus::DOING != tenant_clean_info.status_) {
          // do nothing
        } else {
          ObBackupCleanInfo dest_clean_info = tenant_clean_info;
          dest_clean_info.result_ = result;
          dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
          if (OB_FAIL(update_clean_info(OB_SYS_TENANT_ID, tenant_clean_info, dest_clean_info, sys_tenant_trans))) {
            LOG_WARN("failed to update clean info", K(ret), K(tenant_clean_info), K(dest_clean_info));
          }
        }
      } else {
        if (OB_FAIL(trans.start(sql_proxy_))) {
          LOG_WARN("fail to start trans", K(ret));
        } else {
          if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, trans, tenant_clean_info))) {
            LOG_WARN("failed to push tenant clean info into array", K(ret), K(simple_clean_tenant));
          } else if (ObBackupCleanInfoStatus::DOING != tenant_clean_info.status_) {
            // do nothing
          } else {
            ObBackupCleanInfo dest_clean_info = tenant_clean_info;
            dest_clean_info.result_ = result;
            dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
            if (OB_FAIL(ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
              LOG_WARN("failed to check sys tenant trans alive", K(ret), K(tenant_clean_info));
            } else if (OB_FAIL(update_clean_info(
                           simple_clean_tenant.tenant_id_, tenant_clean_info, dest_clean_info, trans))) {
              LOG_WARN("failed to update clean info", K(ret), K(tenant_clean_info), K(dest_clean_info));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans.end(true /*commit*/))) {
              OB_LOG(WARN, "failed to commit", K(ret));
            }
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
              OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_with_finished_tenant_clean_task(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants,
    const share::ObBackupCleanInfo& sys_clean_info, const ObBackupDataCleanTenant& sys_clean_tenant,
    const int32_t clean_result)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  ObBackupCleanInfo dest_sys_clean_info;
  ObMySQLTransaction trans;
  ObBackupInfoManager backup_info_manager;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(sys_clean_info));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_SUCCESS == clean_result) {
    if (OB_FAIL(delete_cluster_backup_meta_data(sys_clean_info, sys_clean_tenant, normal_clean_tenants))) {
      LOG_WARN("failed to delete backup meta data", K(ret), K(sys_clean_info), K(sys_clean_tenant));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    // push sys clean info into array
    dest_sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, dest_sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(insert_clean_infos_into_history(normal_clean_tenants, trans))) {
      LOG_WARN("failed to insert clean infos into history", K(ret), K(normal_clean_tenants));
    } else if (OB_FAIL(reset_backup_clean_infos(normal_clean_tenants, trans))) {
      LOG_WARN("failed to reset backup clean infos", K(ret), K(normal_clean_tenants));
    } else if (OB_FAIL(backup_info_manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
      LOG_WARN("failed to init backup info manager", K(ret));
    } else if (OB_SUCCESS == clean_result && sys_clean_info.is_expired_clean() &&
               OB_FAIL(backup_info_manager.update_last_delete_expired_data_snapshot(
                   sys_clean_info.tenant_id_, sys_clean_info.start_time_, trans))) {
      LOG_WARN("failed to update last delete expired data snapshot", K(ret), K(sys_clean_info));
    }
    DEBUG_SYNC(BACKUP_DATA_SYS_CLEAN_STATUS_DOING);
    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS == clean_result && OB_FAIL(delete_inner_table_his_data(dest_sys_clean_info, trans))) {
      LOG_WARN("failed to delete inner table his data", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(set_sys_clean_info_stop(dest_sys_clean_info, trans))) {
      LOG_WARN("failed to set sys clean info stop", K(ret), K(sys_clean_info));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true /*commit*/))) {
      OB_LOG(WARN, "failed to commit", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
      OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
    }
  }
  return ret;
}

int ObBackupDataClean::insert_clean_infos_into_history(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, common::ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tennat = normal_clean_tenants.at(i);
      if (OB_FAIL(insert_tenant_backup_clean_info_history(simple_clean_tennat, sys_tenant_trans))) {
        LOG_WARN("failed to insert tenant backup clean info", K(ret), K(simple_clean_tennat));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::reset_backup_clean_infos(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, common::ObISQLClient& sys_trans)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = normal_clean_tenants.at(i);
      ObMySQLTransaction trans;
      ObBackupCleanInfo dest_clean_info;
      ObBackupCleanInfo clean_info;
      clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal clean tenants should not has sys clean tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_trans, clean_info))) {
          LOG_WARN("failed to get backup clean info", K(ret));
        } else if (OB_FAIL(delete_backup_clean_info(OB_SYS_TENANT_ID, clean_info, sys_trans))) {
          LOG_WARN("failed to delete backup clean info", K(ret), K(clean_info));
        }
      } else {
        if (OB_FAIL(trans.start(sql_proxy_))) {
          LOG_WARN("failed to start trans", K(ret), K(clean_info));
        } else {
          if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, trans, clean_info))) {
            LOG_WARN("failed to get backup clean info", K(ret), K(simple_clean_tenant));
          } else {
            dest_clean_info = clean_info;
            dest_clean_info.start_time_ = 0;
            dest_clean_info.end_time_ = 0;
            dest_clean_info.error_msg_.reset();
            dest_clean_info.comment_.reset();
            dest_clean_info.type_ = ObBackupCleanType::EMPTY_TYPE;
            dest_clean_info.clog_gc_snapshot_ = 0;
            dest_clean_info.result_ = OB_SUCCESS;

            if (OB_FAIL(update_clean_info(simple_clean_tenant.tenant_id_, clean_info, dest_clean_info, trans))) {
              LOG_WARN("failed to update clean info", K(ret), K(clean_info), K(dest_clean_info));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans.end(true /*commit*/))) {
              OB_LOG(WARN, "failed to commit", K(ret));
            }
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
              OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::check_all_tenant_clean_tasks_stopped(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, bool& is_all_tasks_stopped)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  ObBackupCleanInfo dest_clean_info;
  is_all_tasks_stopped = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else {
    for (int64_t i = 0; is_all_tasks_stopped && OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = normal_clean_tenants.at(i);
      ObBackupCleanInfo tenant_clean_info;
      tenant_clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      ObMySQLTransaction trans;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        // do nothing
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal clean tenant should not has sys clean tenant", K(ret), K(simple_clean_tenant));
      } else if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("fail to start trans", K(ret));
      } else {
        if (simple_clean_tenant.is_deleted_) {
          if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, tenant_clean_info))) {
            LOG_WARN("failed to get backup clean info", K(ret), K(simple_clean_tenant));
          }
        } else {
          if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, trans, tenant_clean_info))) {
            LOG_WARN("failed to get backup clean info", K(ret), K(simple_clean_tenant));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (ObBackupCleanInfoStatus::STOP != tenant_clean_info.status_) {
          is_all_tasks_stopped = false;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            OB_LOG(WARN, "failed to commit", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
            OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::update_clog_gc_snaphost(
    const int64_t cluster_clog_gc_snapshot, ObBackupCleanInfo& clean_info, ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  int64_t clog_gc_snapshot = 0;
  ObBackupCleanInfo backup_clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (cluster_clog_gc_snapshot < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get clog gc snapshot get invalid argument", K(ret), K(cluster_clog_gc_snapshot));
  } else if (OB_FAIL(get_clog_gc_snapshot(clean_tenant, cluster_clog_gc_snapshot, clog_gc_snapshot))) {
    LOG_WARN("failed to get clog gc snapshot", K(ret), K(clean_tenant));
  } else if (clean_tenant.simple_clean_tenant_.is_deleted_) {
    clean_tenant.clog_gc_snapshot_ = clog_gc_snapshot;
    clean_info.clog_gc_snapshot_ = clog_gc_snapshot;
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    const uint64_t tenant_id =
        clean_tenant.simple_clean_tenant_.is_deleted_ ? OB_SYS_TENANT_ID : clean_tenant.simple_clean_tenant_.tenant_id_;
    backup_clean_info.tenant_id_ = clean_tenant.simple_clean_tenant_.tenant_id_;
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, backup_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_tenant));
    } else if (ObBackupCleanInfoStatus::STOP == backup_clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update clog gc snapshot get invalid argument", K(ret), K(backup_clean_info));
    } else {
      dest_clean_info = backup_clean_info;
      dest_clean_info.clog_gc_snapshot_ = clog_gc_snapshot;
      if (OB_FAIL(update_clean_info(tenant_id, backup_clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update clean info", K(ret), K(backup_clean_info), K(dest_clean_info));
      } else {
        clean_tenant.clog_gc_snapshot_ = clog_gc_snapshot;
        clean_info.clog_gc_snapshot_ = clog_gc_snapshot;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObBackupDataClean::get_clog_gc_snapshot(
    const ObBackupDataCleanTenant& clean_tenant, const int64_t cluster_clog_gc_snapshot, int64_t& clog_gc_snapshot)
{
  int ret = OB_SUCCESS;
  clog_gc_snapshot = INT64_MAX;
  ObExternBackupInfo extern_backup_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (cluster_clog_gc_snapshot < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get clog gc snapshot get invalid argument", K(ret), K(cluster_clog_gc_snapshot));
  } else if (!clean_tenant.clog_data_clean_point_.is_valid()) {
    // do nothing
  } else {
    ObClusterBackupDest cluster_backup_dest;
    ObExternBackupInfoMgr extern_backup_info_mgr;
    const ObTenantBackupTaskInfo& clog_data_clean_point = clean_tenant.clog_data_clean_point_;
    if (OB_FAIL(cluster_backup_dest.set(clog_data_clean_point.backup_dest_, clog_data_clean_point.incarnation_))) {
      LOG_WARN("failed to set cluster backup dest", K(ret), K(clog_data_clean_point));
    } else if (OB_FAIL(extern_backup_info_mgr.init(
                   clean_tenant.simple_clean_tenant_.tenant_id_, cluster_backup_dest, *backup_lease_service_))) {
      LOG_WARN("failed to init extern backup info mgr", K(ret), K(cluster_backup_dest));
    } else if (OB_FAIL(extern_backup_info_mgr.get_extern_full_backup_info(
                   clog_data_clean_point.backup_set_id_, extern_backup_info))) {
      if (OB_BACKUP_INFO_NOT_EXIST != ret) {
        LOG_WARN("failed to get extern full backup info", K(ret), K(extern_backup_info), K(clog_data_clean_point));
      } else {
        LOG_WARN("extern full clean info do not exist", K(ret), K(extern_backup_info), K(clog_data_clean_point));
        ret = OB_SUCCESS;
      }
    } else {
      clog_gc_snapshot = std::min(extern_backup_info.frozen_snapshot_version_, clog_gc_snapshot);
    }
  }

  if (OB_SUCC(ret)) {
    if (INT64_MAX == clog_gc_snapshot) {
      clog_gc_snapshot = 0;
    }

    if (cluster_clog_gc_snapshot > 0 && clog_gc_snapshot > 0) {
      clog_gc_snapshot = std::min(cluster_clog_gc_snapshot, clog_gc_snapshot);
    } else {
      clog_gc_snapshot = std::max(cluster_clog_gc_snapshot, clog_gc_snapshot);
    }

    LOG_INFO("get tenant backup clog gc snapshot",
        K(ret),
        K(clog_gc_snapshot),
        K(cluster_clog_gc_snapshot),
        K(clean_tenant));
  }
  return ret;
}

int ObBackupDataClean::get_deleted_clean_tenants(ObIArray<ObSimpleBackupDataCleanTenant>& deleted_tenants)
{
  int ret = OB_SUCCESS;
  deleted_tenants.reset();
  ObMySQLTransaction trans;
  ObBackupCleanInfo sys_clean_info;
  ObArray<ObBackupCleanInfo> deleted_tenant_clean_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      LOG_WARN("failed to get clean info", K(ret));
    } else if (OB_FAIL(get_deleted_tenant_clean_infos(trans, deleted_tenant_clean_infos))) {
      LOG_WARN("failed to get deleted tenant clean infos", K(ret), K(sys_clean_info));
    } else {
      ObSimpleBackupDataCleanTenant deleted_clean_tenant;
      for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tenant_clean_infos.count(); ++i) {
        const ObBackupCleanInfo& backup_clean_info = deleted_tenant_clean_infos.at(i);
        deleted_clean_tenant.reset();
        deleted_clean_tenant.tenant_id_ = backup_clean_info.tenant_id_;
        deleted_clean_tenant.is_deleted_ = true;
        if (OB_FAIL(deleted_tenants.push_back(deleted_clean_tenant))) {
          LOG_WARN("failed to push deleted clean tenant into array", K(ret), K(deleted_clean_tenant));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_deleted_tenant_clean_infos(
    ObISQLClient& trans, common::ObIArray<ObBackupCleanInfo>& deleted_tenant_clean_infos)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init tenant backup clean info updater", K(ret));
  } else if (OB_FAIL(updater.get_deleted_tenant_clean_infos(deleted_tenant_clean_infos))) {
    LOG_WARN("failed to get backup clean info", K(ret));
  }
  return ret;
}

int ObBackupDataClean::schedule_deleted_clean_tenants(
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& deleted_clean_tenants)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  ObMySQLTransaction trans;
  ObBackupCleanInfo sys_clean_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (deleted_clean_tenants.empty()) {
    // do nothing
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, trans, sys_clean_info))) {
      LOG_WARN("failed to get clean info", K(ret));
    } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_ &&
               ObBackupCleanInfoStatus::CANCEL != sys_clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schedule deleted clean tenants get unexpected status", K(ret), K(sys_clean_info));
    } else if (updater.init(trans)) {
      LOG_WARN("failed to init tenant backup clean info updater", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < deleted_clean_tenants.count(); ++i) {
        const ObSimpleBackupDataCleanTenant& simple_clean_tenant = deleted_clean_tenants.at(i);
        ObBackupCleanInfo clean_info = sys_clean_info;
        clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
        if (OB_FAIL(updater.insert_backup_clean_info(OB_SYS_TENANT_ID, clean_info))) {
          LOG_WARN("failed to insert backup clean info", K(ret), K(clean_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_clean_tenants(const common::ObIArray<ObBackupDataCleanTenant>& clean_tenants,
    common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants, ObBackupDataCleanTenant& sys_clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenants.count(); ++i) {
      const ObBackupDataCleanTenant& clean_tenant = clean_tenants.at(i);
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = clean_tenant.simple_clean_tenant_;
      if (OB_SYS_TENANT_ID == clean_tenant.simple_clean_tenant_.tenant_id_) {
        sys_clean_tenant = clean_tenant;
      } else if (OB_FAIL(normal_clean_tenants.push_back(simple_clean_tenant))) {
        LOG_WARN("failed to push clean tenant into array", K(ret), K(simple_clean_tenant));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_backup_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info, ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init updater", K(ret));
  } else if (OB_FAIL(updater.remove_clean_info(tenant_id, clean_info))) {
    LOG_WARN("failedto remove clean info", K(ret), K(tenant_id), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::set_sys_clean_info_stop(const ObBackupCleanInfo& backup_clean_info, ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfoHistoryUpdater updater;
  ObBackupCleanInfo dest_clean_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!backup_clean_info.is_valid() || OB_SYS_TENANT_ID != backup_clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set sys clean info stop get invalid argument", K(ret), K(backup_clean_info));
  } else if (ObBackupCleanInfoStatus::STOP == backup_clean_info.status_) {
    // do nothing
  } else {
    dest_clean_info = backup_clean_info;
    dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
    dest_clean_info.end_time_ = ObTimeUtil::current_time();
    const int32_t result = dest_clean_info.result_;

    if (OB_SUCCESS != result && OB_FAIL(dest_clean_info.error_msg_.assign(ob_strerror(result)))) {
      LOG_WARN("failed to assign strerror result", K(ret), K(dest_clean_info));
    } else if (OB_FAIL(updater.init(trans))) {
      LOG_WARN("failed to init updater", K(ret));
    } else if (OB_FAIL(updater.insert_backup_clean_info(dest_clean_info))) {
      LOG_WARN("failed to insert backup clean info into history", K(ret), K(dest_clean_info));
    } else {
      dest_clean_info.reset();
      dest_clean_info = backup_clean_info;
      dest_clean_info.start_time_ = 0;
      dest_clean_info.end_time_ = 0;
      dest_clean_info.error_msg_.reset();
      dest_clean_info.comment_.reset();
      dest_clean_info.type_ = ObBackupCleanType::EMPTY_TYPE;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
      dest_clean_info.result_ = OB_SUCCESS;
      dest_clean_info.clog_gc_snapshot_ = 0;

      if (OB_FAIL(update_clean_info(OB_SYS_TENANT_ID, backup_clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update clean info", K(ret), K(backup_clean_info), K(dest_clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::try_clean_tenant_backup_dir(const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = clean_tenant.simple_clean_tenant_.tenant_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id || !clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean normal tenant backup dir get invalid agument", K(ret), K(clean_tenant));
  } else {
    const ObIArray<ObBackupDataCleanElement>& backup_element_array = clean_tenant.backup_element_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement& clean_element = backup_element_array.at(i);
      if (OB_FAIL(clean_tenant_backup_dir(tenant_id, clean_element))) {
        LOG_WARN("failed to clean tenant backup dir", K(ret), K(tenant_id), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::clean_tenant_backup_dir(const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupPath path;
  const char* storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType& device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_data_path(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get tenant backup data path", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_path(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get tenant clog path", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_prefix_path(cluster_backup_dest, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(path.join(tenant_id))) {
    LOG_WARN("failed to join backup path", K(ret), K(path));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObBackupDataClean::clean_backup_tenant_info(const ObBackupDataCleanTenant& sys_clean_tenant,
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!sys_clean_tenant.is_valid() || OB_SYS_TENANT_ID != sys_clean_tenant.simple_clean_tenant_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean backup tenant get invalid argument", K(ret), K(sys_clean_tenant));
  } else {
    const ObIArray<ObBackupDataCleanElement>& backup_element_array = sys_clean_tenant.backup_element_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement& clean_element = backup_element_array.at(i);
      if (OB_FAIL(inner_clean_backup_tenant_info(clean_element, normal_clean_tenants))) {
        LOG_WARN("failed to clean backup tenant info", K(ret), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::inner_clean_backup_tenant_info(const ObBackupDataCleanElement& clean_element,
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants)
{
  int ret = OB_SUCCESS;
  const ObStorageType device_type = clean_element.backup_dest_.device_type_;
  ObExternTenantInfoMgr tenant_info_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObStorageUtil util(false /*need retry*/);
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(tenant_info_mgr.init(cluster_backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init tenant info mgr", K(ret), K(cluster_backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& normal_clean_tenant = normal_clean_tenants.at(i);
      if (normal_clean_tenant.is_deleted_) {
        bool is_exist = true;
        path.reset();
        if (OB_FAIL(ObBackupPathUtil::get_tenant_path(cluster_backup_dest, normal_clean_tenant.tenant_id_, path))) {
          LOG_WARN("failed to get tenant path", K(ret), K(cluster_backup_dest));
        } else if (OB_FAIL(util.is_exist(path.get_ptr(), cluster_backup_dest.get_storage_info(), is_exist))) {
          LOG_WARN("failed to check tenant path exist", K(ret), K(path));
        } else if (is_exist) {
          // do nothing
        } else if (OB_FAIL(tenant_info_mgr.delete_tenant_info(normal_clean_tenant.tenant_id_))) {
          LOG_WARN("failed to delete tenant info", K(ret), K(normal_clean_tenant));
        }
      }
    }

    if (OB_SUCC(ret)) {
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_tenant_info_path(cluster_backup_dest, path))) {
        LOG_WARN("failed to get tenatn info path", K(ret), K(cluster_backup_dest));
      } else if (tenant_info_mgr.is_empty()) {
        if (OB_FAIL(
                ObBackupDataCleanUtil::delete_backup_file(path, cluster_backup_dest.get_storage_info(), device_type))) {
          LOG_WARN("failed to delete backup file", K(ret), K(path), K(clean_element));
        }
      } else if (!tenant_info_mgr.is_extern_tenant_infos_modified()) {
        if (OB_FAIL(
                ObBackupDataCleanUtil::touch_backup_file(path, cluster_backup_dest.get_storage_info(), device_type))) {
          LOG_WARN("failed to touch backup file", K(ret), K(path), K(clean_element));
        }
      } else {
        if (OB_FAIL(tenant_info_mgr.upload_tenant_infos())) {
          LOG_WARN("failed to upload tenant infos", K(ret), K(cluster_backup_dest));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::touch_extern_tenant_name(const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  ObExternTenantInfoMgr tenant_info_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObStorageUtil util(false /*need retry*/);
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch extern tenant name get invalid argument", K(ret), K(clean_tenant));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      path.reset();
      cluster_backup_dest.reset();
      const ObBackupDataCleanElement& clean_element = clean_tenant.backup_element_array_.at(i);
      const ObStorageType device_type = clean_element.backup_dest_.device_type_;
      if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
        LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_name_info_path(cluster_backup_dest, path))) {
        LOG_WARN("failed to get tenant path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_file(
                     path, cluster_backup_dest.get_storage_info(), device_type))) {
        LOG_WARN("failed to touch backup file", K(ret), K(path), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::touch_extern_clog_info(const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupDataCleanStatics clean_statics;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch extern tenant name get invalid argument", K(ret), K(clean_tenant));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      path.reset();
      cluster_backup_dest.reset();
      const ObBackupDataCleanElement& clean_element = clean_tenant.backup_element_array_.at(i);
      const ObStorageType device_type = clean_element.backup_dest_.device_type_;
      if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
        LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
      } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info(cluster_backup_dest, path))) {
        LOG_WARN("failed to get tenant path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_dir_files(path,
                     cluster_backup_dest.get_storage_info(),
                     device_type,
                     clean_statics,
                     *backup_lease_service_))) {
        LOG_WARN("failed to touch backup file", K(ret), K(path), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_cluster_backup_meta_data(const share::ObBackupCleanInfo& clean_info,
    const ObBackupDataCleanTenant& clean_tenant,
    const common::ObIArray<ObSimpleBackupDataCleanTenant>& normal_clean_tenants)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!clean_info.is_valid() || OB_SYS_TENANT_ID != clean_info.tenant_id_ || !clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete cluster backup meta data get invalid argument", K(ret), K(clean_info), K(clean_tenant));
  } else if (OB_FAIL(delete_backup_extern_infos(clean_info, clean_tenant))) {
    LOG_WARN("failed to delete extern backup infos deleted", K(ret), K(clean_info));
  } else if (OB_FAIL(clean_backup_tenant_info(clean_tenant, normal_clean_tenants))) {
    LOG_WARN("failed to clean backup tenant info", K(ret), K(clean_tenant));
  } else if (OB_FAIL(touch_extern_tenant_name(clean_tenant))) {
    LOG_WARN("failed to touch extern tenant name", K(ret), K(clean_tenant));
  } else if (OB_FAIL(touch_extern_clog_info(clean_tenant))) {
    LOG_WARN("failed to touch extern clog info", K(ret), K(clean_tenant));
  }
  return ret;
}

int ObBackupDataClean::get_cluster_max_succeed_backup_set(int64_t& backup_set_id)
{
  int ret = OB_SUCCESS;
  backup_set_id = 0;
  ObBackupTaskHistoryUpdater updater;
  ObTenantBackupTaskInfo task_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(updater.get_tenant_max_succeed_backup_task(OB_SYS_TENANT_ID, task_info))) {
    if (OB_INVALID_BACKUP_SET_ID == ret) {
      backup_set_id = 0;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant max succeed backup task", K(ret));
    }
  } else {
    backup_set_id = task_info.backup_set_id_;
  }
  return ret;
}

int ObBackupDataClean::get_log_archive_info(
    const int64_t snapshot_version, const ObArray<ObLogArchiveBackupInfo>& log_infos, ObLogArchiveBackupInfo& log_info)
{
  int ret = OB_SUCCESS;
  log_info.reset();
  typedef ObArray<ObLogArchiveBackupInfo>::const_iterator ArrayIter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (log_infos.empty()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_infos.count() - 1; ++i) {
      if (log_infos.at(i).status_.start_ts_ >= log_infos.at(i + 1).status_.start_ts_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log infos is not sorted by start ts", K(ret), K(log_infos));
      }
    }
    if (OB_SUCC(ret)) {
      CompareLogArchiveSnapshotVersion cmp;
      ArrayIter iter = std::lower_bound(log_infos.begin(), log_infos.end(), snapshot_version, cmp);
      if (iter == log_infos.end()) {
        --iter;
      } else if (iter != log_infos.begin() && iter->status_.start_ts_ > snapshot_version) {
        --iter;
      }
      log_info = *iter;
    }
  }
  return ret;
}

int ObBackupDataClean::check_backupset_continue_with_clog_data(const ObTenantBackupTaskInfo& backup_task_info,
    const common::ObArray<ObLogArchiveBackupInfo>& log_archive_infos, bool& is_continue)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo log_archive_info;
  is_continue = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!backup_task_info.is_valid() || !backup_task_info.is_result_succeed()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check backupset contine with clog data get invalid argument", K(ret), K(backup_task_info));
  } else if (OB_FAIL(get_log_archive_info(backup_task_info.snapshot_version_, log_archive_infos, log_archive_info))) {
    LOG_WARN("failed to get log archive info", K(ret), K(backup_task_info));
  } else if (log_archive_info.status_.checkpoint_ts_ > backup_task_info.snapshot_version_ &&
             log_archive_info.status_.start_ts_ < backup_task_info.snapshot_version_) {
    is_continue = true;
  }
  return ret;
}

int ObBackupDataClean::check_can_do_task()
{
  // TODO () consider tenant cancel delete backup
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (stop_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("rs is stopping", K(ret));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, *sql_proxy_, sys_clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup clean info", K(ret));
    }
  } else if (ObBackupCleanInfoStatus::CANCEL == sys_clean_info.status_) {
    ret = OB_CANCELED;
    LOG_WARN("backup data clean has been canceled", K(ret), K(sys_clean_info));
  }
  return ret;
}

int ObBackupDataClean::do_tenant_cancel_delete_backup(
    const ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
      if (OB_FAIL(do_sys_tenant_cancel_delete_backup(clean_info))) {
        LOG_WARN("failed to do sys tenant cancel delete backup", K(ret), K(clean_info));
      }
    } else {
      if (OB_FAIL(do_normal_tenant_cancel_delete_backup(clean_info, clean_tenant))) {
        LOG_WARN("failedto do normal tenant cancel delete backup", K(ret), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_sys_tenant_cancel_delete_backup(const ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do_sys_tenant cancel delete backup get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(set_tenant_clean_info_cancel(clean_info))) {
    LOG_WARN("failed to set tenant clean info cancel", K(ret), K(clean_info));
  } else {
    wakeup();
  }
  return ret;
}

int ObBackupDataClean::set_tenant_clean_info_cancel(const ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant_ids;
  ObMySQLTransaction trans;
  const int64_t EXECUTE_TIMEOUT_US = 5 * 60L * 1000 * 1000;  // 300s
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != clean_info.tenant_id_ || ObBackupCleanInfoStatus::CANCEL != clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sys tenant cancel delete backup get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret), K(clean_info));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(clean_info.tenant_id_, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_info));
    } else if (OB_FAIL(clean_info.check_backup_clean_info_match(sys_clean_info))) {
      LOG_WARN("failed to check backup clean info match", K(ret), K(clean_info), K(sys_clean_info));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant_ids.count(); ++i) {
        const uint64_t tenant_id = all_tenant_ids.at(i);
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(set_normal_tenant_cancel(tenant_id, trans))) {
          LOG_WARN("failed to set normal tenant cancel", K(ret), K(tenant_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObBackupCleanInfo> deleted_tenant_clean_infos;
      if (OB_FAIL(get_deleted_tenant_clean_infos(trans, deleted_tenant_clean_infos))) {
        LOG_WARN("failed to get deleted tenant clean infos", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tenant_clean_infos.count(); ++i) {
          const ObBackupCleanInfo& delete_tenant_clean_info = deleted_tenant_clean_infos.at(i);
          if (OB_FAIL(set_deleted_tenant_cancel(delete_tenant_clean_info, trans))) {
            LOG_WARN("failed to set deleted tenant cancel", K(ret), K(delete_tenant_clean_info));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::set_normal_tenant_cancel(const uint64_t tenant_id, common::ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupCleanInfo clean_info;
  clean_info.tenant_id_ = tenant_id;
  ObBackupCleanInfo dest_clean_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set normal tenant cancel get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_info));
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_ ||
               ObBackupCleanInfoStatus::CANCEL == clean_info.status_) {
      // do nothing
    } else if (OB_FAIL(ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
      LOG_WARN("failed to check sys clean info trans alive", K(ret), K(tenant_id));
    } else {
      dest_clean_info = clean_info;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::CANCEL;
      if (OB_FAIL(update_clean_info(tenant_id, clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update clean info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::set_deleted_tenant_cancel(
    const ObBackupCleanInfo& clean_info, common::ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo dest_clean_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set deleted tenant cancel get invalid argument", K(ret), K(clean_info));
  } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_ ||
             ObBackupCleanInfoStatus::CANCEL == clean_info.status_) {
    // do nothing
  } else {
    dest_clean_info = clean_info;
    dest_clean_info.status_ = ObBackupCleanInfoStatus::CANCEL;
    if (OB_FAIL(update_clean_info(OB_SYS_TENANT_ID, clean_info, dest_clean_info, sys_tenant_trans))) {
      LOG_WARN("failed to update clean info", K(ret), K(clean_info), K(dest_clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::do_normal_tenant_cancel_delete_backup(
    const ObBackupCleanInfo& clean_info, const ObBackupDataCleanTenant& clean_tenant)
{
  int ret = OB_SUCCESS;
  const int32_t result = clean_info.result_ == OB_SUCCESS ? OB_CANCELED : clean_info.result_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_INVALID_ID == clean_info.tenant_id_ || OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do normal tenant cancel delete backup get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(update_normal_tenant_clean_result(clean_info, clean_tenant, result))) {
    LOG_WARN("failed to update normal tenant clean result", K(ret), K(clean_info), K(clean_tenant));
  }
  return ret;
}

int ObBackupDataClean::get_sys_tenant_backup_dest(hash::ObHashSet<ObClusterBackupDest>& cluster_backup_dest_set)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupTaskInfo> task_infos;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  ObMySQLTransaction trans;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = tenant_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (ObBackupCleanInfoStatus::STOP == sys_clean_info.status_ ||
               ObBackupCleanInfoStatus::PREPARE == sys_clean_info.status_) {
      // do nothing
    } else if (OB_FAIL(get_tenant_backup_task_his_info(sys_clean_info, trans, task_infos))) {
      LOG_WARN("failed to get backup task his info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(get_tenant_backup_task_info(sys_clean_info, trans, task_infos))) {
      LOG_WARN("failed to get archive info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(get_log_archive_history_info(tenant_id, trans, log_archive_infos))) {
      LOG_WARN("failed to get log archive history info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(get_log_archive_info(tenant_id, trans, log_archive_infos))) {
      LOG_WARN("failed to get archive info", K(ret), K(sys_clean_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }

    if (OB_SUCC(ret)) {
      // add task info backup dest
      ObClusterBackupDest cluster_backup_dest;
      for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
        cluster_backup_dest.reset();
        const ObTenantBackupTaskInfo& task_info = task_infos.at(i);
        if (OB_FAIL(cluster_backup_dest.set(task_info.backup_dest_, task_info.incarnation_))) {
          LOG_WARN("failed to set cluster backup dest", K(ret), K(task_info));
        } else if (OB_FAIL(cluster_backup_dest_set.set_refactored(cluster_backup_dest))) {
          LOG_WARN("failed to set cluster backup dest", K(ret), K(cluster_backup_dest));
        }
      }

      // add log archive dest
      for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
        cluster_backup_dest.reset();
        const ObLogArchiveBackupInfo& archive_info = log_archive_infos.at(i);
        if (OB_FAIL(cluster_backup_dest.set(archive_info.backup_dest_, archive_info.status_.incarnation_))) {
          LOG_WARN("failed to set cluster backup dest", K(ret), K(archive_info));
        } else if (OB_FAIL(cluster_backup_dest_set.set_refactored(cluster_backup_dest))) {
          LOG_WARN("failed to set cluster backup dest", K(ret), K(cluster_backup_dest));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_scheduler_normal_tenant(share::ObBackupCleanInfo& clean_info,
    ObBackupDataCleanTenant& clean_tenant, common::ObIArray<ObTenantBackupTaskInfo>& task_infos,
    common::ObIArray<ObLogArchiveBackupInfo>& log_archive_infos)
{
  int ret = OB_SUCCESS;
  clean_tenant.backup_element_array_.reset();
  ObMySQLTransaction trans;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  ObBackupCleanInfo sys_clean_info;
  clean_info.reset();
  ObSimpleBackupDataCleanTenant& simple_clean_tenant = clean_tenant.simple_clean_tenant_;
  // TODO() set timeout in trans method
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || clean_tenant.simple_clean_tenant_.is_deleted_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant clean scheduler get invalid argument", K(ret), K(clean_tenant));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(clean_tenant));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
    clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
    if (OB_FAIL(get_backup_clean_info(tenant_id, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_tenant));
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_ ||
               ObBackupCleanInfoStatus::PREPARE == clean_info.status_) {
      // do nothing
    } else if (OB_FAIL(get_tenant_backup_task_his_info(clean_info, trans, task_infos))) {
      LOG_WARN("failed to get backup task his info", K(ret), K(clean_info));
    } else if (OB_FAIL(get_tenant_backup_task_info(clean_info, trans, task_infos))) {
      LOG_WARN("failed to get archive info", K(ret), K(clean_info));
    } else if (OB_FAIL(get_log_archive_history_info(simple_clean_tenant.tenant_id_, trans, log_archive_infos))) {
      LOG_WARN("failed to get log archive history info", K(ret), K(simple_clean_tenant));
    } else if (OB_FAIL(get_log_archive_info(simple_clean_tenant.tenant_id_, trans, log_archive_infos))) {
      LOG_WARN("failed to get archive info", K(ret), K(simple_clean_tenant));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_scheduler_deleted_tenant(
    const common::hash::ObHashSet<ObClusterBackupDest>& cluster_backup_dest_set, share::ObBackupCleanInfo& clean_info,
    ObBackupDataCleanTenant& clean_tenant, common::ObIArray<ObTenantBackupTaskInfo>& task_infos,
    common::ObIArray<ObLogArchiveBackupInfo>& log_archive_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  clean_tenant.backup_element_array_.reset();
  ObBackupCleanInfo sys_clean_info;
  clean_info.reset();
  ObSimpleBackupDataCleanTenant& simple_clean_tenant = clean_tenant.simple_clean_tenant_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_tenant.simple_clean_tenant_.is_deleted_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant clean scheduler get invalid argument", K(ret), K(clean_tenant));
  } else {
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
    if (OB_FAIL(get_backup_clean_info(tenant_id, *sql_proxy_, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_tenant));
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_ ||
               ObBackupCleanInfoStatus::PREPARE == clean_info.status_) {
      // do nothing
    } else {
      for (hash::ObHashSet<ObClusterBackupDest>::const_iterator iter = cluster_backup_dest_set.begin();
           OB_SUCC(ret) && iter != cluster_backup_dest_set.end();
           ++iter) {
        const ObClusterBackupDest& cluster_backup_dest = iter->first;
        if (OB_SUCCESS != (tmp_ret = do_inner_scheduler_delete_tenant(
                               cluster_backup_dest, clean_tenant, task_infos, log_archive_infos))) {
          LOG_WARN("failed to do inner scheduler delete tenant", K(tmp_ret), K(cluster_backup_dest));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_inner_scheduler_delete_tenant(const ObClusterBackupDest& cluster_backup_dest,
    ObBackupDataCleanTenant& clean_tenant, common::ObIArray<ObTenantBackupTaskInfo>& task_infos,
    common::ObIArray<ObLogArchiveBackupInfo>& log_archive_infos)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  ObArray<ObExternBackupInfo> extern_backup_infos;
  ObExternLogArchiveBackupInfo archive_backup_info;
  ObArray<ObTenantLogArchiveStatus> status_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do inner scheduler delete tenant get invalid argument", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(extern_backup_info_mgr.init(
                 clean_tenant.simple_clean_tenant_.tenant_id_, cluster_backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(cluster_backup_dest), K(clean_tenant));
  } else if (OB_FAIL(extern_backup_info_mgr.get_extern_full_backup_infos(extern_backup_infos))) {
    LOG_WARN("failed to get extern full backup infos", K(ret), K(clean_tenant));
  } else if (OB_FAIL(log_archive_info_mgr.read_extern_log_archive_backup_info(
                 cluster_backup_dest, clean_tenant.simple_clean_tenant_.tenant_id_, archive_backup_info))) {
    LOG_WARN("failed to read extern log archive backup info", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(archive_backup_info.get_log_archive_status(status_array))) {
    LOG_WARN("failed to get log archive status", K(ret), K(clean_tenant));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_infos.count(); ++i) {
      const ObExternBackupInfo& extern_backup_info = extern_backup_infos.at(i);
      ObTenantBackupTaskInfo task_info;
      task_info.cluster_id_ = cluster_backup_dest.cluster_id_;
      task_info.tenant_id_ = clean_tenant.simple_clean_tenant_.tenant_id_;
      task_info.backup_set_id_ = extern_backup_info.full_backup_set_id_;
      task_info.incarnation_ = cluster_backup_dest.incarnation_;
      task_info.snapshot_version_ = extern_backup_info.backup_snapshot_version_;
      task_info.prev_full_backup_set_id_ = extern_backup_info.prev_full_backup_set_id_;
      task_info.prev_inc_backup_set_id_ = extern_backup_info.prev_inc_backup_set_id_;
      task_info.backup_type_.type_ = extern_backup_info.backup_type_;
      task_info.status_ = ObTenantBackupTaskInfo::FINISH;
      task_info.device_type_ = cluster_backup_dest.dest_.device_type_;
      task_info.backup_dest_ = cluster_backup_dest.dest_;
      task_info.encryption_mode_ = extern_backup_info.encryption_mode_;
      if (OB_FAIL(task_infos.push_back(task_info))) {
        LOG_WARN("failed to push task info into array", K(ret), K(task_info));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < status_array.count(); ++i) {
      ObTenantLogArchiveStatus& status = status_array.at(i);
      // set delete tenant archive status stop
      status.status_ = ObLogArchiveStatus::STOP;
      ObLogArchiveBackupInfo archive_info;
      archive_info.status_ = status;
      if (OB_FAIL(
              cluster_backup_dest.dest_.get_backup_dest_str(archive_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to get backup dest str", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(log_archive_infos.push_back(archive_info))) {
        LOG_WARN("failed to push archive info into array", K(ret), K(archive_info));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeed get task info and log archive info", K(task_infos), K(log_archive_infos));
    }
  }
  return ret;
}

bool ObBackupDataClean::is_result_need_retry(const int32_t result)
{
  bool b_ret = false;
  if (OB_NOT_INIT != result && OB_INVALID_ARGUMENT != result && OB_ERR_SYS != result && OB_INIT_TWICE != result &&
      OB_ERR_UNEXPECTED != result && OB_TENANT_HAS_BEEN_DROPPED != result && OB_NOT_SUPPORTED != result &&
      OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED != result && OB_SUCCESS != result && OB_CANCELED != result) {
    // do nothing
    // TODO() Need to consider the limit of the number of retry
    // When the upper limit is reached, it needs to actively fail to
    // The scenario where the backup medium is not used is not considered here for the time being
    b_ret = true;
  }
  return b_ret;
}

void ObBackupDataClean::set_inner_error(const int32_t result)
{
  if (OB_SUCCESS != result) {
    if (is_result_need_retry(result)) {
      wakeup();
    } else {
      inner_error_ = OB_SUCCESS == inner_error_ ? result : inner_error_;
    }
  }
}

bool ObBackupDataClean::get_prepare_flag() const
{
  const bool prepare_flag = ATOMIC_LOAD(&is_prepare_flag_);
  return prepare_flag;
}

void ObBackupDataClean::update_prepare_flag(const bool is_prepare_flag)
{
  const bool old_prepare_flag = get_prepare_flag();
  if (old_prepare_flag != is_prepare_flag) {
    ATOMIC_VCAS(&is_prepare_flag_, old_prepare_flag, is_prepare_flag);
  }
}

}  // namespace rootserver
}  // namespace oceanbase
