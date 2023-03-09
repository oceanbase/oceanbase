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
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_backup_backupset_operator.h"
#include "rootserver/backup/ob_cancel_delete_backup_scheduler.h"
#include "ob_rs_event_history_table_operator.h"

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
      reserve_min_backup_set_id_(0),
      backup_lease_service_(nullptr),
      backup_dest_(),
      backup_dest_option_(),
      backup_backup_dest_(),
      backup_backup_dest_option_(),
      is_update_reserved_backup_timestamp_(false),
      inner_table_version_(OB_BACKUP_INNER_TABLE_VMAX),
      sys_tenant_deleted_backup_set_(),
      retry_count_(0),
      sys_tenant_deleted_backup_round_(),
      sys_tenant_deleted_backup_piece_()
{}

ObBackupDataClean::~ObBackupDataClean()
{}

int ObBackupDataClean::init(share::schema::ObMultiVersionSchemaService &schema_service, ObMySQLProxy &sql_proxy,
    share::ObIBackupLeaseService &backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int root_backup_thread_cnt = 1;
  const int64_t MAX_BUCKET_NUM = 128;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "root backup init twice", K(ret));
  } else if (OB_FAIL(create(root_backup_thread_cnt, "BackupDataClean"))) {
    LOG_WARN("create thread failed", K(ret), K(root_backup_thread_cnt));
  } else if (OB_FAIL(sys_tenant_deleted_backup_set_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create backup set map", K(ret));
  } else if (OB_FAIL(sys_tenant_deleted_backup_round_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create sys tenant deleted backup round", K(ret));
  } else if (OB_FAIL(sys_tenant_deleted_backup_piece_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create sys tenant deleted backup piece", K(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    backup_lease_service_ = &backup_lease_service;
    inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;
    is_inited_ = true;
    FLOG_INFO("[BACKUP_CLEAN]backup data clean init success");
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
    FLOG_INFO("[BACKUP_CLEAN]backup data clean idle", "idle_time", idling_.get_idle_interval_us());
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

  while (!stop_) {
    ret = OB_SUCCESS;
    data_clean_tenants.reset();
    inner_error_ = OB_SUCCESS;
    sys_tenant_deleted_backup_set_.reuse();
    sys_tenant_deleted_backup_round_.reuse();
    sys_tenant_deleted_backup_piece_.reuse();
    ObCurTraceId::init(GCONF.self_addr_);

    if (OB_FAIL(set_current_backup_dest())) {
      LOG_WARN("failed to set current backup dest", K(ret));
    } else if (OB_FAIL(check_inner_table_version_())) {
      LOG_WARN("failed to check check_inner_table_version_", K(ret));
    } else if (OB_FAIL(prepare_tenant_backup_clean())) {
      LOG_WARN("failed to prepare tenant backup clean", K(ret));
    } else if (OB_FAIL(get_need_clean_tenants(data_clean_tenants))) {
      LOG_WARN("failed to get need clean tenants", K(ret));
    } else if (OB_FAIL(do_clean_scheduler(data_clean_tenants))) {
      LOG_WARN("failed to do root scheduler", K(ret), K(data_clean_tenants));
    }

    set_inner_error(ret);
    if (OB_SUCCESS != inner_error_) {
      if (OB_FAIL(update_sys_clean_info())) {
        LOG_WARN("failed to update sys clean info", K(ret), K(inner_error_));
      }
    }
    cleanup_prepared_infos();

    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", K(ret));
    } else {
      continue;
    }
  }
  FLOG_INFO("[BACKUP_CLEAN]ObBackupDataClean stop");
  is_working_ = false;
}

int ObBackupDataClean::update_sys_clean_info()
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_clean_info))) {
    LOG_WARN("failed to get sys clean info", K(ret), K(sys_clean_info));
  } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_) {
    // do nothing
  } else if (FALSE_IT(sys_clean_info.result_ = inner_error_)) {
  } else if (OB_FAIL(
                 ObTenantBackupCleanInfoOperator::update_clean_info(OB_SYS_TENANT_ID, sys_clean_info, *sql_proxy_))) {
    LOG_WARN("failed to update sys clean info result", K(ret), K(sys_clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_need_clean_tenants(ObIArray<ObBackupDataCleanTenant> &clean_tenants)
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
  } else if (OB_FAIL(get_clean_tenants_from_history_table(sys_clean_info, extern_clean_tenants_map))) {
    LOG_WARN("failed to get clean tenants from history table", K(ret), K(sys_clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_clean_tenants.count(); ++i) {
      const ObBackupDataCleanTenant &server_clean_tenant = server_clean_tenants.at(i);
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
      const ObSimpleBackupDataCleanTenant &deleted_clean_tenant = deleted_clean_tenants.at(i);
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
      const ObBackupDataCleanTenant &server_clean_tenant = server_clean_tenants.at(i);
      if (OB_FAIL(clean_tenants.push_back(server_clean_tenant))) {
        LOG_WARN("failed to push server clean tenant into clean tenants", K(ret), K(server_clean_tenant));
      }
    }

    // add deleted clean tenant already in inner table
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = deleted_clean_tenants.at(i);
      ObBackupDataCleanTenant clean_tenant;
      clean_tenant.simple_clean_tenant_ = simple_clean_tenant;
      if (OB_FAIL(clean_tenants.push_back(clean_tenant))) {
        LOG_WARN("failed to push server clean tenant into clean tenants", K(ret), K(clean_tenant));
      }
    }

    hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant>::iterator iter;
    for (iter = extern_clean_tenants_map.begin(); OB_SUCC(ret) && iter != extern_clean_tenants_map.end(); ++iter) {
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = iter->second;
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
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = new_deleted_clean_tenants.at(i);
      ObBackupDataCleanTenant clean_tenant;
      clean_tenant.simple_clean_tenant_ = simple_clean_tenant;
      if (OB_FAIL(clean_tenants.push_back(clean_tenant))) {
        LOG_WARN("failed to push server clean tenant into clean tenants", K(ret), K(clean_tenant));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_server_clean_tenants(ObIArray<ObBackupDataCleanTenant> &clean_tenants)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant_ids;
  ObBackupCleanInfo sys_clean_info;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (all_tenant_ids.empty()) {
    // do nothing
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, *sql_proxy_, sys_clean_info))) {
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
  }
  return ret;
}

int ObBackupDataClean::get_all_tenant_ids(ObIArray<uint64_t> &tenant_ids)
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
    const uint64_t tenant_id, const bool for_update, ObISQLClient &sql_proxy, ObBackupCleanInfo &clean_info)
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
  } else if (OB_FAIL(updater.get_backup_clean_info(tenant_id, for_update, clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_clean_info(const uint64_t tenant_id, ObBackupCleanInfo &clean_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  clean_info.tenant_id_ = tenant_id;
  const bool for_update = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, *sql_proxy_, clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupDataClean::get_server_need_clean_info(const uint64_t tenant_id, bool &need_add)
{
  int ret = OB_SUCCESS;
  need_add = false;
  ObTenantBackupCleanInfoUpdater updater;
  ObBackupCleanInfo clean_info;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean not init", K(ret));
  } else {
    clean_info.tenant_id_ = tenant_id;
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, *sql_proxy_, clean_info))) {
      if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
        need_add = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
      }
    } else {
      need_add = true;
    }
  }
  return ret;
}

int ObBackupDataClean::do_clean_scheduler(ObIArray<ObBackupDataCleanTenant> &clean_tenants)
{
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

int ObBackupDataClean::do_schedule_clean_tenants(ObIArray<ObBackupDataCleanTenant> &clean_tenants)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_CLEAN]start do clean scheduler", K(clean_tenants));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (clean_tenants.empty()) {
    // do nothing
  } else {
    // first clean normal tenant and then clean sys tenant
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenants.count(); ++i) {
      ObBackupCleanInfo clean_info;
      ObBackupDataCleanTenant &clean_tenant = clean_tenants.at(i);
      DEBUG_SYNC(BACKUP_DATA_CLEAN_STATUS_SCHEDULE);
      if (stop_) {
        ret = OB_RS_SHUTDOWN;
        LOG_WARN("rootservice shutdown", K(ret));
      } else if (OB_SUCCESS != (tmp_ret = do_tenant_clean_scheduler(clean_info, clean_tenant))) {
        LOG_WARN("failed to do tenant clean scheduler", K(tmp_ret), K(clean_tenant), K(clean_info));
      } else if (OB_SUCCESS != (tmp_ret = do_with_status(clean_info, clean_tenant))) {
        LOG_WARN("failed to do tenant backup clean status", K(tmp_ret), K(clean_info), K(clean_tenant));
      }
      set_inner_error(tmp_ret);
    }
  }
  return ret;
}

int ObBackupDataClean::do_check_clean_tenants_finished(const common::ObIArray<ObBackupDataCleanTenant> &clean_tenants)
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

int ObBackupDataClean::do_with_status(const share::ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant)
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
    FLOG_INFO("[BACKUP_CLEAN]do with status", K(clean_info));
    const ObBackupCleanInfoStatus::STATUS &status = clean_info.status_;
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
    FLOG_INFO("[BACKUP_CLEAN]doing backup clean", K(clean_info), K(ret));
  }
  return ret;
}

int ObBackupDataClean::prepare_tenant_backup_clean()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBackupCleanInfo sys_clean_info;
  // TODO(muwei.ym) use already marked info to delete, do not calculate it again when switch rs

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
  } else if (ObBackupCleanInfoStatus::STOP == sys_clean_info.status_ ||
             ObBackupCleanInfoStatus::CANCEL == sys_clean_info.status_) {
    // do nothing
  } else if (OB_FAIL(mark_sys_tenant_backup_meta_data_deleting())) {
    LOG_WARN("failed to mark sys tenant backup meta data", K(ret), K(sys_clean_info));
  } else if (OB_FAIL(schedule_tenants_backup_data_clean(tenant_ids))) {
    LOG_WARN("failed to schedule tenant backup data clean", K(ret), K(tenant_ids));
  }

  return ret;
}

int ObBackupDataClean::mark_sys_tenant_backup_meta_data_deleting()
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObBackupDataCleanTenant sys_clean_tenant;
  ObArray<ObTenantBackupTaskInfo> task_infos;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  CompareLogArchiveBackupInfo log_archive_info_cmp;
  CompareBackupTaskInfo backup_task_cmp;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, *sql_proxy_, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (ObBackupCleanInfoStatus::STOP == sys_clean_info.status_) {
      // do nothing
    } else if (0 != reserve_min_backup_set_id_ && ObBackupCleanInfoStatus::DOING == sys_clean_info.status_) {
      if (OB_FAIL(prepare_delete_backup_set(sys_clean_info))) {
        LOG_WARN("failed to prepare delete backup set", K(ret), K(sys_clean_info));
      } else if (OB_FAIL(prepare_delete_backup_piece_and_round(sys_clean_info))) {
        LOG_WARN("failed to prepare delete backup piece and round", K(ret), K(sys_clean_info));
      }
    } else {
      // In the prepare phase, mark deleting is be needed
      // remark deleting is required when switch RS occurs in the doing phase, reserve_min_backup_set_id_ = 0
      sys_clean_tenant.simple_clean_tenant_.tenant_id_ = OB_SYS_TENANT_ID;
      sys_clean_tenant.simple_clean_tenant_.is_deleted_ = false;
      if (OB_FAIL(get_all_tenant_backup_infos(
              sys_clean_info, sys_clean_tenant.simple_clean_tenant_, *sql_proxy_, task_infos, log_archive_infos))) {
        LOG_WARN("failed to get all tenant backup infos", K(ret), K(sys_clean_info));
      } else if (FALSE_IT(std::sort(task_infos.begin(), task_infos.end(), backup_task_cmp))) {
      } else if (FALSE_IT(std::sort(log_archive_infos.begin(), log_archive_infos.end(), log_archive_info_cmp))) {
      } else if (OB_FAIL(get_backup_clean_elements(
                     sys_clean_info, task_infos, log_archive_infos, true /*is_prepare_stage*/, sys_clean_tenant))) {
        LOG_WARN("failed to get backup clean elements", K(ret), K(sys_clean_info), K(task_infos));
      } else if (sys_clean_info.is_backup_set_clean() &&
                 !sys_clean_tenant.has_clean_backup_set(sys_clean_info.backup_set_id_, sys_clean_info.copy_id_)) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("can not clean backup set, because backup set is not existt", K(ret), K(sys_clean_info), K(sys_clean_tenant));
      }

      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != sys_clean_info.result_) {
          // do nothing
        } else if (OB_FAIL(
                       update_clog_gc_snaphost(sys_clean_info.clog_gc_snapshot_, sys_clean_info, sys_clean_tenant))) {
          LOG_WARN("failed to update clog gc snapshot", K(ret), K(sys_clean_info));
        } else if (FALSE_IT(reserve_min_backup_set_id_ =
                                sys_clean_tenant.clog_data_clean_point_
                                    .backup_set_id_)) {  // Set reserve_min_backup_set_id_ only in the prepare stage
        } else if (OB_FAIL(mark_backup_meta_data_deleting(sys_clean_info, sys_clean_tenant))) {
          LOG_WARN("failed to mark backup meta data deleted", K(ret), K(sys_clean_info));
        } else if (OB_FAIL(check_backup_dest_lifecycle(sys_clean_tenant))) {
          LOG_WARN("failed to check backup dest lifecycle", K(ret), K(sys_clean_info));
        }
        // Forbidden check backup dest lifecycle, because oss has bug which make observer core
        // else if (OB_FAIL(check_backup_dest_lifecycle(sys_clean_tenant))) {
        //  LOG_WARN("failed to check backup dest lifecycle", K(ret), K(sys_clean_info));
        // }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::schedule_tenants_backup_data_clean(const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  ObTimeoutCtx timeout_ctx;
  const bool for_update = true;

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
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (ObBackupCleanInfoStatus::STOP == sys_clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys backup info status is unexpected", K(ret), K(sys_clean_info));
    } else if (ObBackupCleanInfoStatus::DOING == sys_clean_info.status_ ||
               ObBackupCleanInfoStatus::CANCEL == sys_clean_info.status_) {
      // do nothing
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
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::schedule_tenant_backup_data_clean(
    const uint64_t tenant_id, const share::ObBackupCleanInfo &sys_clean_info, ObISQLClient &sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  int64_t parameter = 0;
  ObTimeoutCtx timeout_ctx;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id should be sys tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    clean_info.tenant_id_ = tenant_id;
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
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
      dest_clean_info.copy_id_ = sys_clean_info.copy_id_;
      if (OB_FAIL(dest_clean_info.set_clean_parameter(parameter))) {
        LOG_WARN("failed to set clean parameter", K(ret), K(dest_clean_info), K(sys_clean_info));
      } else if (OB_FAIL(rootserver::ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
        LOG_WARN("failed to check sys tenant trans alive", K(ret), K(clean_info));
      } else if (OB_FAIL(update_clean_info(clean_info.tenant_id_, clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update backup clean info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::check_clog_data_point(
    const ObBackupCleanInfo &sys_clean_info, const ObBackupDataCleanTenant &normal_clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!sys_clean_info.is_delete_obsolete() || !normal_clean_tenant.clog_data_clean_point_.is_valid() ||
      0 == sys_clean_info.clog_gc_snapshot_) {
    // do nothing
  } else if (0 == reserve_min_backup_set_id_ ||
             normal_clean_tenant.clog_data_clean_point_.backup_set_id_ < reserve_min_backup_set_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get clog data clean point error",
        K(ret),
        K_(normal_clean_tenant.clog_data_clean_point),
        K_(reserve_min_backup_set_id));
  }
  return ret;
}

int ObBackupDataClean::do_tenant_clean_scheduler(ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  clean_tenant.backup_element_array_.reset();
  ObBackupCleanInfo sys_clean_info;
  ObArray<ObTenantBackupTaskInfo> task_infos;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  const int64_t start_ts = ObTimeUtil::current_time();
  // TODO copy_id
  const int64_t copy_id = 0;
  CompareLogArchiveBackupInfo log_archive_info_cmp;
  CompareBackupTaskInfo backup_task_info_cmp;
  FLOG_INFO("[BACKUP_CLEAN]start do tenant clean scheduler", K(clean_tenant));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant clean scheduler get invalid argument", K(ret), K(clean_tenant));
  } else if (OB_FAIL(do_scheduler_normal_tenant(clean_info, clean_tenant, task_infos, log_archive_infos))) {
    LOG_WARN("failed to do scheduler normal tenant", K(ret), K(clean_info));
  } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_) {
    // do nothing
  } else if (FALSE_IT(std::sort(log_archive_infos.begin(), log_archive_infos.end(), log_archive_info_cmp))) {
  } else if (FALSE_IT(std::sort(task_infos.begin(), task_infos.end(), backup_task_info_cmp))) {
  } else if (OB_FAIL(get_backup_clean_elements(
                 clean_info, task_infos, log_archive_infos, false /*is_prepare_stage*/, clean_tenant))) {
    LOG_WARN("failed to get backup clean elements", K(ret), K(clean_tenant), K(task_infos));
  } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, sys_clean_info))) {
    LOG_WARN("failed to get backup clean info", K(ret), K(clean_tenant));
  } else if (OB_FAIL(check_clog_data_point(sys_clean_info, clean_tenant))) {
    LOG_WARN("failed to check clog data point", K(ret), K(sys_clean_info), K(clean_tenant));
  } else if (OB_FAIL(update_clog_gc_snaphost(sys_clean_info.clog_gc_snapshot_, clean_info, clean_tenant))) {
    LOG_WARN("failed to update clog gc snapshot", K(ret), K(sys_clean_info), K(clean_tenant));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]finish prepare tenant clean info",
        "cost",
        ObTimeUtil::current_time() - start_ts,
        K(clean_tenant),
        K(clean_info));
  }
  return ret;
}

// ObBackupdataCleanTeannt
//   -- ObBackupDataCleanElement[0]
//   -- ObbackupDest
//   -- log_archive_round_array (ascending order)
//   -- backup_set_id_array(descending order)
int ObBackupDataClean::get_backup_clean_elements(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, const bool is_prepare_stage,
    ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo min_include_task_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean elements get invalid argument", K(ret), K(clean_tenant), K(clean_info));
  } else if (clean_info.is_backup_set_clean()) {
    if (OB_FAIL(add_delete_backup_set(
            clean_info, task_infos, log_archive_infos, is_prepare_stage, clean_tenant, min_include_task_info))) {
      LOG_WARN("failed to add delete backup set", K(ret), K(clean_info), K(clean_tenant));
    }
  } else if (clean_info.is_delete_obsolete()) {
    if (OB_FAIL(add_obsolete_backup_sets(
            clean_info, task_infos, log_archive_infos, is_prepare_stage, clean_tenant, min_include_task_info))) {
      LOG_WARN("failed to add expired backup set", K(ret), K(clean_info), K(clean_tenant));
    }
  } else if (clean_info.is_delete_backup_piece()) {
    if (OB_FAIL(add_delete_backup_piece(clean_info, task_infos, log_archive_infos, clean_tenant))) {
      LOG_WARN("failed to add delete backup piece", K(ret), K(clean_info));
    }
  } else if (clean_info.is_delete_backup_round()) {
    if (OB_FAIL(add_delete_backup_round(clean_info, task_infos, log_archive_infos, clean_tenant))) {
      LOG_WARN("failed to add delete backup piece", K(ret), K(clean_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean info type is invalid", K(ret), K(clean_info));
  }

  if (OB_SUCC(ret)) {
    clean_tenant.clog_data_clean_point_ = min_include_task_info;
    FLOG_INFO("[BACKUP_CLEAN]succeed get clean tenant info", K(clean_tenant), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::add_log_archive_infos(
    const common::ObIArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
      const ObLogArchiveBackupInfo &log_archive_info = log_archive_infos.at(i);
      if (OB_FAIL(add_log_archive_info(log_archive_info, clean_tenant))) {
        LOG_WARN("failed to add log archive info", K(ret), K(log_archive_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_log_archive_info(
    const ObLogArchiveBackupInfo &log_archive_info, ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  const bool for_update = false;
  const bool is_backup_backup = log_archive_info.status_.copy_id_ > 0;
  ObArray<ObBackupPieceInfo> piece_infos;
  int64_t min_copies_num = INT64_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!log_archive_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add log archive info get invalid argument", K(ret), K(log_archive_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_round_backup_piece_infos(*sql_proxy_,
                 for_update,
                 log_archive_info.status_.tenant_id_,
                 log_archive_info.status_.incarnation_,
                 log_archive_info.status_.round_,
                 is_backup_backup,
                 piece_infos))) {
    LOG_WARN("failed to get round backup piece infos", K(ret), K(log_archive_info));
  } else {
    ObBackupDest backup_dest;
    ObBackupDestOpt backup_dest_option;
    ObLogArchiveRound log_archive_round;
    ObSimplePieceInfo simple_piece_info;
    log_archive_round.log_archive_round_ = log_archive_info.status_.round_;
    log_archive_round.log_archive_status_ = log_archive_info.status_.status_;
    log_archive_round.start_ts_ = log_archive_info.status_.start_ts_;
    log_archive_round.checkpoint_ts_ = log_archive_info.status_.checkpoint_ts_;
    log_archive_round.start_piece_id_ = log_archive_info.status_.start_piece_id_;
    log_archive_round.copy_id_ = log_archive_info.status_.copy_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_infos.count(); ++i) {
      simple_piece_info.reset();
      const ObBackupPieceInfo &piece_info = piece_infos.at(i);
      if (ObBackupFileStatus::BACKUP_FILE_DELETED == piece_info.file_status_) {
        // do nothing
      } else {
        simple_piece_info.round_id_ = piece_info.key_.round_id_;
        simple_piece_info.backup_piece_id_ = piece_info.key_.backup_piece_id_;
        simple_piece_info.checkpoint_ts_ = piece_info.checkpoint_ts_;
        simple_piece_info.create_date_ = piece_info.create_date_;
        simple_piece_info.file_status_ = piece_info.file_status_;
        simple_piece_info.max_ts_ = piece_info.max_ts_;
        simple_piece_info.start_ts_ = piece_info.start_ts_;
        simple_piece_info.status_ = piece_info.status_;
        if (0 == piece_info.key_.copy_id_) {
          if (OB_FAIL(get_backup_piece_file_copies_num(piece_info, simple_piece_info.copies_num_))) {
            LOG_WARN("failed to get backup piece file copies num", K(ret), K(piece_info));
          } else {
            min_copies_num = std::min(min_copies_num, simple_piece_info.copies_num_);
          }
        } else {
          // do nothing
        }

        if (OB_FAIL(ret)) {
        } else if (0 != STRCMP(piece_info.backup_dest_.ptr(), log_archive_info.backup_dest_)) {
          // do nothing
        } else if (OB_FAIL(log_archive_round.add_simpe_piece_info(simple_piece_info))) {
          LOG_WARN("failed to add simple piece info", K(ret), K(simple_piece_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (INT64_MAX == min_copies_num) {
        // do nothing
      } else {
        log_archive_round.copies_num_ = min_copies_num;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(backup_dest.set(log_archive_info.backup_dest_))) {
      LOG_WARN("failed to set backup dest", K(ret), K(log_archive_info));
    } else if (OB_FAIL(get_backup_dest_option(backup_dest, backup_dest_option))) {
      LOG_WARN("failed to get backup dest option", K(ret), K(backup_dest));
    } else if (OB_FAIL(clean_tenant.set_backup_clean_archive_round(GCONF.cluster_id,
                   log_archive_info.status_.incarnation_,
                   backup_dest,
                   log_archive_round,
                   backup_dest_option))) {
      LOG_WARN(
          "failed to set backup clean element", K(ret), K(log_archive_info), K(clean_tenant), K(backup_dest_option));
    }
  }
  return ret;
}

int ObBackupDataClean::add_delete_backup_set(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<ObLogArchiveBackupInfo> &log_archive_infos, const bool is_prepare_stage,
    ObBackupDataCleanTenant &clean_tenant, ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  int64_t cluster_max_backup_set_id = 0;
  const int64_t copy_id = clean_info.copy_id_;
  int64_t copies_num = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup set get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(get_cluster_max_succeed_backup_set(copy_id, cluster_max_backup_set_id))) {
    LOG_WARN("failed to get cluster max succeed backup set", K(ret), K(clean_info));
  } else {
    ObTenantBackupTaskInfo delete_backup_info;
    bool has_kept_last_succeed_data = false;
    bool is_continue = false;
    for (int64_t i = task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObTenantBackupTaskInfo &task_info = task_infos.at(i);
      is_continue = false;
      // consider can clean up
      if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
        if (task_info.backup_set_id_ == clean_info.backup_set_id_) {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean, because status of backup set is not finish",
              K(ret),
              K(clean_info),
              K(cluster_max_backup_set_id),
              K(task_info));
        }
      } else if (task_info.backup_set_id_ == clean_info.backup_set_id_) {
        delete_backup_info = task_info;
      } else if (!task_info.is_result_succeed() || !task_info.backup_type_.is_full_backup()) {
        // do nothing
      } else if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
        LOG_WARN("failed to check backupset contine with clog data", K(ret), K(task_info));
      } else if (!is_continue) {
        LOG_INFO("[BACKUP_CLEAN]backup set is not continue with clog data", K(task_info), K(log_archive_infos));
      } else {
        has_kept_last_succeed_data = true;
        clog_data_clean_point = task_info;
      }
    }

    if (OB_SUCC(ret)) {
      ObBackupDestOpt backup_dest_option;
      ObBackupSetId backup_set_id;
      backup_set_id.backup_set_id_ = delete_backup_info.backup_set_id_;
      backup_set_id.copy_id_ = delete_backup_info.copy_id_;
      if (!delete_backup_info.is_valid()) {
        // do nothing
        // The backup_set has been checked when the backup is initiated
        // Here, if keep_backup_info is invalid, it may be deleted and there will be a change in the future.
      } else {
        if (OB_FAIL(get_backup_dest_option(delete_backup_info.backup_dest_, backup_dest_option))) {
          LOG_WARN("failed to get backup dest option", K(ret), K(delete_backup_info));
        } else if (OB_SYS_TENANT_ID != clean_info.tenant_id_) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
          clean_tenant.clog_gc_snapshot_ =
              has_kept_last_succeed_data ? clog_data_clean_point.start_replay_log_ts_ : INT64_MAX;
        } else if (!delete_backup_info.is_result_succeed()) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
        } else if (!has_kept_last_succeed_data && delete_backup_info.backup_set_id_ >= cluster_max_backup_set_id &&
                   (delete_backup_info.backup_dest_ == backup_dest_ ||
                       delete_backup_info.backup_dest_ == backup_backup_dest_)) {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean, because of need to keep the last succeed backup set",
              K(ret),
              K(clean_info),
              K(cluster_max_backup_set_id),
              K(delete_backup_info));
        } else if (backup_dest_option.backup_copies_ > 1 &&
                   OB_FAIL(get_backup_set_file_copies_num(delete_backup_info, copies_num))) {
          LOG_WARN("failed to get backup set file copies num", K(ret), K(delete_backup_info));
        } else if (backup_dest_option.backup_copies_ != 1 && copies_num < backup_dest_option.backup_copies_) {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean, because current finish copies_num is less than backup_copies in backup_dest_option",
              K(ret),
              K(clean_info),
              K(cluster_max_backup_set_id),
              K(delete_backup_info),
              K(has_kept_last_succeed_data),
              K(clog_data_clean_point),
              "backup dest option copies num",
              backup_dest_option.backup_copies_,
              "already finish copies",
              copies_num);
        } else if (has_kept_last_succeed_data) {
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
          clean_tenant.clog_gc_snapshot_ = clog_data_clean_point.start_replay_log_ts_;
        } else if ((delete_backup_info.backup_dest_ != backup_dest_ &&
                       delete_backup_info.backup_dest_ != backup_backup_dest_) ||
                   clean_tenant.simple_clean_tenant_.is_deleted_) {
          // skip check with log continues
          // can delete log archive round without piece
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
          clean_tenant.clog_gc_snapshot_ = INT64_MAX;
        } else {
          ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
          LOG_WARN("backup set do not allow clean, because of need to keep last succeed backup set",
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
                       backup_set_id,
                       backup_dest_option))) {
          LOG_WARN("failed to set backup clean element", K(ret), K(delete_backup_info), K(clean_tenant));
        } else if (delete_backup_info.backup_dest_ != backup_dest_ &&
                   delete_backup_info.backup_dest_ != backup_backup_dest_) {
          // no need delete log info, log info should delete by delete backup piece or round
        } else {
          ObBackupDest backup_dest;
          for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
            backup_dest.reset();
            const ObLogArchiveBackupInfo &log_archive_info = log_archive_infos.at(i);
            if (OB_FAIL(backup_dest.set(log_archive_info.backup_dest_))) {
              LOG_WARN("failed to set backup dest", K(ret), K(log_archive_info));
            } else if (backup_dest != delete_backup_info.backup_dest_) {
              // do nothing
            } else if (OB_FAIL(add_log_archive_info(log_archive_info, clean_tenant))) {
              LOG_WARN("failed to add log archive info", K(ret), K(log_archive_info));
            }
          }
        }

        if (OB_SUCC(ret) && is_prepare_stage) {
          char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
          int trace_length = 0;
          if (FALSE_IT(trace_length =
                           ObCurTraceId::get_trace_id()->to_string(trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
          } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
          } else {
            FLOG_INFO("[BACKUP_CLEAN]succ add backup clean set", K(backup_set_id), K(clean_info));
            ROOTSERVICE_EVENT_ADD("backup_clean",
                "backup_set",
                "tenant_id",
                clean_info.tenant_id_,
                "backup_set_id",
                backup_set_id.backup_set_id_,
                "copy_id",
                backup_set_id.copy_id_,
                "trace_id",
                trace_id);
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_obsolete_backup_sets(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<ObLogArchiveBackupInfo> &log_archive_infos, const bool is_prepare_stage,
    ObBackupDataCleanTenant &clean_tenant, ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup set get invalid argument", K(ret), K(clean_info));
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_ && is_prepare_stage) {
    if (OB_FAIL(add_obsolete_backup_sets_in_prepare(
            clean_info, task_infos, log_archive_infos, clean_tenant, clog_data_clean_point))) {
      LOG_WARN("failed to add sys tenant obsolete backup sets", K(ret), K(clean_info));
    }
  } else {
    if (OB_FAIL(add_obsolete_backup_sets_in_doing(
            clean_info, task_infos, log_archive_infos, clean_tenant, clog_data_clean_point))) {
      LOG_WARN("failed to add normal tenant obsolete backup sets", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::add_obsolete_backup_sets_in_doing(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant,
    share::ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  bool is_continue = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(add_log_archive_infos(log_archive_infos, clean_tenant))) {
    LOG_WARN("failed to add log archive infos", K(ret), K(clean_info), K(clean_tenant));
  } else {
    for (int64_t i = task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      is_continue = false;
      const ObTenantBackupTaskInfo &task_info = task_infos.at(i);
      ObBackupSetId backup_set_id;
      backup_set_id.backup_set_id_ = task_info.backup_set_id_;
      backup_set_id.copy_id_ = task_info.copy_id_;
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
      ObBackupDestOpt backup_dest_option;
      bool can_delete = false;

      if (OB_FAIL(check_backup_set_id_can_be_deleted(clean_info.tenant_id_, backup_set_id, can_delete))) {
        LOG_WARN("failed to check backup set id can be deleted", K(ret), K(clean_info), K(backup_set_id));
      } else if (can_delete) {
        backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
      } else {
        backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
        if (!task_info.is_result_succeed() || !task_info.backup_type_.is_full_backup()) {
          // do nothing
        } else if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
          LOG_WARN("failed to check backup set continue with clog data", K(ret), K(task_info));
        } else if (!is_continue) {
        } else {
          clog_data_clean_point = task_info;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_backup_dest_option(task_info.backup_dest_, backup_dest_option))) {
          LOG_WARN("failed to get backup dest option", K(ret), K(task_info));
        } else if (OB_FAIL(clean_tenant.set_backup_clean_backup_set_id(task_info.cluster_id_,
                       task_info.incarnation_,
                       task_info.backup_dest_,
                       backup_set_id,
                       backup_dest_option))) {
          LOG_WARN("failed to set backup clean element", K(ret), K(task_info), K(clean_tenant));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_obsolete_backup_sets_in_prepare(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant,
    share::ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  bool has_kept_last_succeed_data = false;
  int64_t cluster_max_backup_set_id = 0;
  bool is_continue = false;
  const int64_t copy_id = clean_info.copy_id_;
  ObArray<ObBackupSetId> reverse_backup_set_ids;
  ObArray<share::ObTenantBackupTaskInfo> reverse_task_infos;
  ObBackupSetId backup_set_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup set get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(add_log_archive_infos(log_archive_infos, clean_tenant))) {
    LOG_WARN("failed to add log archive infos", K(ret), K(clean_info), K(clean_tenant));
  } else if (OB_FAIL(get_cluster_max_succeed_backup_set(copy_id, cluster_max_backup_set_id))) {
    LOG_WARN("failed to get cluster max succeed backup set", K(ret), K(clean_info));
  } else {
    // Traverse in reverse order by backup_set_id
    for (int64_t i = task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      is_continue = false;
      const ObTenantBackupTaskInfo &task_info = task_infos.at(i);
      if (task_info.snapshot_version_ <= clean_info.expired_time_) {
        if (OB_FAIL(deal_with_obsolete_backup_set(clean_info,
                task_info,
                log_archive_infos,
                cluster_max_backup_set_id,
                backup_set_id,
                has_kept_last_succeed_data,
                clog_data_clean_point))) {
          LOG_WARN("failed to deal with obsolete backup set", K(ret), K(task_info));
        }
      } else {
        // backup set is not obsolete
        if (ObTenantBackupTaskInfo::FINISH == task_info.status_ && !task_info.is_result_succeed()) {
          backup_set_id.backup_set_id_ = task_info.backup_set_id_;
          backup_set_id.copy_id_ = task_info.copy_id_;
          backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
        } else if (OB_FAIL(deal_with_effective_backup_set(clean_info,
                       task_info,
                       log_archive_infos,
                       backup_set_id,
                       has_kept_last_succeed_data,
                       clog_data_clean_point))) {
          LOG_WARN("failed to deal with effective backup set", K(ret), K(task_info), K(clean_info));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(reverse_task_infos.push_back(task_info))) {
        LOG_WARN("failed to push task info into array", K(ret), K(task_info));
      } else if (OB_FAIL(reverse_backup_set_ids.push_back(backup_set_id))) {
        LOG_WARN("failed to push backup set id into array", K(ret), K(backup_set_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_obsolete_backup_set_with_order(
                   clean_info, reverse_task_infos, reverse_backup_set_ids, clean_tenant, clog_data_clean_point))) {
      LOG_WARN("failed to add obsolete backup set with order", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::deal_with_obsolete_backup_set(const share::ObBackupCleanInfo &clean_info,
    const ObTenantBackupTaskInfo &task_info, const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos,
    const int64_t cluster_max_backup_set_id, ObBackupSetId &backup_set_id, bool &has_kept_last_succeed_data,
    share::ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  backup_set_id.reset();
  // has_kept_last_succeed_data and clog_data_clean_point is input parameter and output parameter

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!task_info.is_valid() || task_info.snapshot_version_ > clean_info.expired_time_ ||
             cluster_max_backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("deal with obsolete backup set get invalid argument",
        K(ret),
        K(task_info),
        K(clean_info),
        K(cluster_max_backup_set_id));
  } else {
    bool is_continue = false;
    backup_set_id.backup_set_id_ = task_info.backup_set_id_;
    backup_set_id.copy_id_ = task_info.copy_id_;
    backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
    ObBackupDestOpt backup_dest_option;
    int64_t copies_num = 0;
    bool can_delete = false;

    if (OB_FAIL(get_backup_dest_option(task_info.backup_dest_, backup_dest_option))) {
      LOG_WARN("failed to get backup dest option", K(ret), K(task_info));
    } else if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
    } else if (!task_info.is_result_succeed() || task_info.is_mark_deleted_) {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
    } else if (!has_kept_last_succeed_data && task_info.backup_set_id_ >= cluster_max_backup_set_id) {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
      if (!task_info.backup_type_.is_full_backup()) {
        // do nothing
      } else if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
        LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
      } else if (!is_continue) {
        LOG_INFO("[BACKUP_CLEAN]backup set is not continue with clog data", K(task_info), K(log_archive_infos));
      } else {
        has_kept_last_succeed_data = true;
        clog_data_clean_point = task_info;
      }
    } else if (backup_dest_option.backup_copies_ > 1 &&
               OB_FAIL(get_backup_set_file_copies_num(task_info, copies_num))) {
      LOG_WARN("failed to get backup set file copies num", K(ret), K(task_info));
    } else if (backup_dest_option.backup_copies_ != 1 && copies_num < backup_dest_option.backup_copies_) {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
      LOG_INFO("[BACKUP_CLEAN]backup copies is not reach required copies",
          K(task_info),
          "need copies",
          backup_dest_option.backup_copies_,
          "now copies",
          copies_num);
      if (!has_kept_last_succeed_data && task_info.backup_type_.is_full_backup()) {
        if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
          LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
        } else if (!is_continue) {
          LOG_INFO("[BACKUP_CLEAN]backup set is not continue with clog data", K(task_info), K(log_archive_infos));
        } else {
          has_kept_last_succeed_data = true;
          clog_data_clean_point = task_info;
        }
      }
    } else if (has_kept_last_succeed_data) {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;
    } else {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
      if (!task_info.backup_type_.is_full_backup()) {
        // do nothing
        has_kept_last_succeed_data = false;
        clog_data_clean_point.reset();
      } else if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
        LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
      } else if (!is_continue) {
        LOG_INFO("[BACKUP_CLEAN]backup set is not continue with clog data", K(task_info), K(log_archive_infos));
      } else {
        has_kept_last_succeed_data = true;
        clog_data_clean_point = task_info;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::deal_with_effective_backup_set(const share::ObBackupCleanInfo &clean_info,
    const ObTenantBackupTaskInfo &task_info, const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos,
    ObBackupSetId &backup_set_id, bool &has_kept_last_succeed_data,
    share::ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  backup_set_id.reset();
  bool is_continue = false;
  // has_kept_last_succeed_data and clog_data_clean_point is input parameter and output parameter

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!task_info.is_valid() ||
             (ObTenantBackupTaskInfo::FINISH == task_info.status_ && !task_info.is_result_succeed()) ||
             task_info.snapshot_version_ <= clean_info.expired_time_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("deal with effective backup set get invalid argument", K(ret), K(task_info), K(clean_info));
  } else {
    backup_set_id.backup_set_id_ = task_info.backup_set_id_;
    backup_set_id.copy_id_ = task_info.copy_id_;
    backup_set_id.clean_mode_ = ObBackupDataCleanMode::CLEAN;

    if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
    } else {
      backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
      if (!task_info.backup_type_.is_full_backup()) {
        // do nothing
        has_kept_last_succeed_data = false;
        clog_data_clean_point.reset();
      } else if (OB_FAIL(check_backupset_continue_with_clog_data(task_info, log_archive_infos, is_continue))) {
        LOG_WARN("failed to check backupset continue with clog data", K(ret), K(task_info));
      } else if (!is_continue) {
      } else {
        // last obsolete backup need relay on clog, so cannot set has_kept_last_succeed_data
        clog_data_clean_point = task_info;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_obsolete_backup_set_with_order(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &reverse_task_infos,
    const common::ObIArray<ObBackupSetId> &reverse_backup_set_ids, ObBackupDataCleanTenant &clean_tenant,
    share::ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (reverse_task_infos.count() != reverse_backup_set_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reverse backup task info count not equal to reverse backup set id count",
        K(ret),
        K(reverse_task_infos),
        K(reverse_backup_set_ids));
  } else {
    bool is_deleted_set_contine = true;
    ObBackupSetId new_backup_set_id;
    ObBackupDestOpt backup_dest_option;
    // Traverse in order of backup_set_id, reset clog_data_clean_point to ensure sequential deletion
    for (int64_t i = reverse_task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      backup_dest_option.reset();
      const ObTenantBackupTaskInfo &task_info = reverse_task_infos.at(i);
      const ObBackupSetId &backup_set_id = reverse_backup_set_ids.at(i);
      new_backup_set_id = backup_set_id;
      if (!is_deleted_set_contine) {
        if (task_info.is_result_succeed()) {
          new_backup_set_id.clean_mode_ = ObBackupDataCleanMode::TOUCH;
        }
      } else if (ObBackupDataCleanMode::CLEAN == backup_set_id.clean_mode_) {
        // do nothing
      } else {
        is_deleted_set_contine = false;
        clog_data_clean_point = task_info;
      }

      if (OB_FAIL(get_backup_dest_option(task_info.backup_dest_, backup_dest_option))) {
        LOG_WARN("failed to get backup dest option", K(ret), K(task_info));
      } else if (OB_FAIL(clean_tenant.set_backup_clean_backup_set_id(task_info.cluster_id_,
                     task_info.incarnation_,
                     task_info.backup_dest_,
                     new_backup_set_id,
                     backup_dest_option))) {
        LOG_WARN("failed to set backup clean element", K(ret), K(task_info), K(clean_tenant));
      } else if (OB_FAIL(add_deleting_backup_set_id_into_set(clean_info.tenant_id_, new_backup_set_id))) {
        LOG_WARN("failed to add deleting backup set id into set", K(ret), K(backup_set_id), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_delete_backup_piece(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  const bool for_update = false;
  const int64_t tenant_id = clean_info.tenant_id_;
  const int64_t backup_piece_id = clean_info.backup_piece_id_;
  const int64_t copy_id = clean_info.copy_id_;
  ObBackupPieceInfo backup_piece_info;
  ObBackupDest backup_dest;
  bool found_log_archive_info = false;
  bool found_backup_set = false;
  ObBackupDestOpt backup_dest_option;
  int64_t copies_num = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_backup_piece()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup piece get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_backup_piece(
                 *sql_proxy_, for_update, tenant_id, backup_piece_id, copy_id, backup_piece_info))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup piece info", K(ret), K(clean_info));
    }
  } else if (!backup_piece_info.is_valid() || !ObBackupUtils::can_backup_pieces_be_deleted(backup_piece_info.status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup piece info is unexpcted", K(ret), K(backup_piece_info));
  } else if (OB_FAIL(backup_dest.set(backup_piece_info.backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_piece_info));
  } else {
    clean_tenant.clog_gc_snapshot_ = backup_piece_info.max_ts_;
    // 1.find log archive info
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count() && !found_log_archive_info; ++i) {
      const ObLogArchiveBackupInfo &tmp_log_archive_info = log_archive_infos.at(i);
      ObBackupDest tmp_backup_dest;
      if (OB_FAIL(tmp_backup_dest.set(tmp_log_archive_info.backup_dest_))) {
        LOG_WARN("failed to set tmp backup dest", K(ret), K(tmp_log_archive_info));
      } else if (tmp_backup_dest == backup_dest &&
                 tmp_log_archive_info.status_.round_ == backup_piece_info.key_.round_id_ &&
                 tmp_log_archive_info.status_.copy_id_ == backup_piece_info.key_.copy_id_) {
        if (OB_FAIL(add_log_archive_info(tmp_log_archive_info, clean_tenant))) {
          LOG_WARN("failed to add log archive info", K(ret), K(tmp_log_archive_info));
        } else {
          found_log_archive_info = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      // 2. find succeed backup set witch start_replay_log_ts bigger than backup_piece_info's max_ts
      if (!found_log_archive_info) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete backup piece failed to find log archive info", K(ret), K(backup_piece_info), K(clean_info));
      } else if (backup_dest != backup_dest_ && backup_dest != backup_backup_dest_) {
        LOG_INFO("[BACKUP_CLEAN]backup piece dest is not equal to config backup dest or backup backupset dest, "
                 "delete it directly",
            K(backup_dest),
            K(backup_dest_),
            K(backup_backup_dest_));
      } else if (OB_FAIL(get_backup_dest_option(backup_dest, backup_dest_option))) {
        LOG_WARN("failed to get backup dest option", K(ret), K(backup_dest));
      } else if (backup_dest_option.backup_copies_ > 1 &&
                 OB_FAIL(get_backup_piece_file_copies_num(backup_piece_info, copies_num))) {
        LOG_WARN("failed to get backup piece file copies num", K(ret), K(backup_piece_info));
      } else if (backup_dest_option.backup_copies_ != 1 && copies_num < backup_dest_option.backup_copies_) {
        ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
        LOG_WARN("piece is not allowed to be deleted, because current finish copies_num is less than backup_copies in backup_dest_option",
            K(ret),
            K(copies_num),
            K_(backup_dest_option.backup_copies),
            K(backup_piece_info));
      } else {
        int64_t start_replay_log_ts = 0;
        for (int64_t i = 0; i < task_infos.count() && !found_backup_set; ++i) {
          const ObTenantBackupTaskInfo &task_info = task_infos.at(i);
          if (task_info.is_result_succeed() && task_info.backup_dest_ == backup_dest &&
              task_info.backup_type_.is_full_backup()) {
            start_replay_log_ts = task_info.start_replay_log_ts_;
            found_backup_set = true;
          }
        }

        if (found_backup_set) {
          if (start_replay_log_ts >= backup_piece_info.max_ts_) {
            LOG_INFO("[BACKUP_CLEAN]found suitable backup set, can delete backup piece", K(backup_piece_info));
          } else {
            ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
            LOG_WARN("piece is not allowed to be deleted, because it is found that the dependent start_replay_log_ts "
                "of backup set is less than the max_ts of piece",
                K(ret),
                K(backup_piece_info),
                K(start_replay_log_ts));
          }
        } else {
          if (clean_tenant.simple_clean_tenant_.is_deleted_) {
            LOG_INFO("[BACKUP_CLEAN]tenant is deleted, can delete backup piece directly", K(backup_piece_info));
          } else {
            ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
            LOG_WARN("piece is not allowed to be deleted, because no valid backup_set is found", K(ret), K(backup_piece_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_delete_backup_round(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos,
    const common::ObArray<share::ObLogArchiveBackupInfo> &log_archive_infos, ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  const int64_t backup_round_id = clean_info.backup_round_id_;
  const int64_t copy_id = clean_info.copy_id_;
  ObBackupDest backup_dest;
  ObLogArchiveBackupInfo log_archive_info;
  bool found_log_archive_info = false;
  bool found_backup_set = false;
  ObBackupDestOpt backup_dest_option;
  int64_t copies_num = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_backup_round()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add delete backup piece get invalid argument", K(ret), K(clean_info));
  } else {
    // 1.find log archive info
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count() && !found_log_archive_info; ++i) {
      const ObLogArchiveBackupInfo &tmp_log_archive_info = log_archive_infos.at(i);
      ObBackupDest tmp_backup_dest;
      if (tmp_log_archive_info.status_.round_ == backup_round_id && tmp_log_archive_info.status_.copy_id_ == copy_id) {
        if (OB_FAIL(add_log_archive_info(tmp_log_archive_info, clean_tenant))) {
          LOG_WARN("failed to add log archive info", K(ret), K(tmp_log_archive_info));
        } else if (OB_FAIL(backup_dest.set(tmp_log_archive_info.backup_dest_))) {
          LOG_WARN("failed to set backup dest", K(ret), K(tmp_log_archive_info));
        } else {
          log_archive_info = tmp_log_archive_info;
          clean_tenant.clog_gc_snapshot_ = log_archive_info.status_.checkpoint_ts_;
          found_log_archive_info = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      // 2. find succeed backup set witch start_replay_log_ts bigger than log_archive_info's check_point_ts
      if (!found_log_archive_info) {
        // do nothing
      } else if (backup_dest != backup_dest_ && backup_dest != backup_backup_dest_) {
        LOG_INFO("[BACKUP_CLEAN]backup round dest is not equal to config backup dest or backup backupset dest, "
                 "delete it directly",
            K(backup_dest),
            K(backup_dest_),
            K(backup_backup_dest_));
      } else if (OB_FAIL(get_backup_dest_option(backup_dest, backup_dest_option))) {
        LOG_WARN("failed to get backup dest option", K(ret), K(backup_dest));
      } else if (backup_dest_option.backup_copies_ > 1 &&
                 OB_FAIL(get_backup_round_copies_num(log_archive_info, copies_num))) {
        LOG_WARN("failed to get backup round copies num", K(ret), K(log_archive_info));
      } else if (backup_dest_option.backup_copies_ != 1 && copies_num < backup_dest_option.backup_copies_) {
        ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
        LOG_WARN("round is not allowed to be deleted, because current finish copies_num is less than backup_copies in backup_dest_option",
            K(ret),
            K(copies_num),
            K_(backup_dest_option.backup_copies),
            K(log_archive_info));
      } else {
        int64_t start_replay_log_ts = 0;
        for (int64_t i = 0; i < task_infos.count() && !found_backup_set; ++i) {
          const ObTenantBackupTaskInfo &task_info = task_infos.at(i);
          if (task_info.is_result_succeed() && task_info.backup_dest_ == backup_dest &&
              task_info.backup_type_.is_full_backup()) {
            start_replay_log_ts = task_info.start_replay_log_ts_;
            found_backup_set = true;
          }
        }

        if (found_backup_set) {
          if (start_replay_log_ts >= log_archive_info.status_.checkpoint_ts_) {
            LOG_INFO("[BACKUP_CLEAN]found suitable backup set, can delete backup round", K(log_archive_info));
          } else {
            ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
            LOG_WARN("round is not allowed to be deleted, because it is found that the dependent start_replay_log_ts "
                "of backup set is less than the max_ts of round",
                K(ret),
                K(log_archive_info),
                K(start_replay_log_ts));
          }
        } else {
          if (clean_tenant.simple_clean_tenant_.is_deleted_) {
            LOG_INFO("[BACKUP_CLEAN]tenant is deleted, can delete backup round directly", K(log_archive_info));
          } else {
            ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
            LOG_WARN("round is not allowed to be deleted, because no valid backup_set is found",
                K(ret),
                K(log_archive_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::do_tenant_backup_clean(
    const share::ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant)
{
  LOG_INFO("[BACKUP_CLEAN]start do tenant backup clean", K(clean_info));
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
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
{
  FLOG_INFO("[BACKUP_CLEAN]do normal tenant backup clean", K(clean_info), K(clean_tenant));
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
  } else if (OB_FAIL(mark_backup_meta_data_deleting(clean_info, clean_tenant))) {
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

  FLOG_INFO("[BACKUP_CLEAN]finish do normal tenant backup clean",
      "cost_ts",
      ObTimeUtil::current_time() - start_ts,
      K(clean_tenant),
      K(ret));

  return ret;
}

int ObBackupDataClean::update_normal_tenant_clean_result(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant, const int32_t clean_result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupCleanInfo dest_clean_info;
  ObTimeoutCtx timeout_ctx;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid() || !clean_info.is_valid() ||
             OB_SYS_TENANT_ID == clean_tenant.simple_clean_tenant_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean elements get invalid argument", K(ret), K(clean_tenant), K(clean_info));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    dest_clean_info = clean_info;
    dest_clean_info.result_ = clean_result;
    dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
    dest_clean_info.end_time_ = ObTimeUtil::current_time();
    const uint64_t tenant_id = clean_tenant.simple_clean_tenant_.is_deleted_ ? OB_SYS_TENANT_ID : clean_info.tenant_id_;
    if (OB_FAIL(set_comment(dest_clean_info.comment_))) {
      LOG_WARN("failed to set comment", K(ret), K(dest_clean_info));
    } else if (OB_FAIL(set_error_msg(clean_result, dest_clean_info.error_msg_))) {
      LOG_WARN("failed to set error msg", K(ret), K(dest_clean_info));
    }

    DEBUG_SYNC(BACKUP_DATA_NORMAL_TENANT_CLEAN_STATUS_DOING);

    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS == clean_result && OB_FAIL(delete_inner_table_his_data(clean_info, clean_tenant, trans))) {
      LOG_WARN("failed to delete inner table his data", K(ret), K(clean_info));
    } else if (OB_FAIL(update_clean_info(tenant_id, clean_info, dest_clean_info))) {
      LOG_WARN("failed to update clean info", K(ret), K(clean_info), K(dest_clean_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_backup_meta_data_deleting(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(mark_backup_set_infos_deleting(clean_info, clean_tenant))) {
    LOG_WARN("failed to mark backup set infos deleted", K(ret), K(clean_info));
  } else if (OB_FAIL(mark_log_archive_infos_deleting(clean_info, clean_tenant))) {
    LOG_WARN("failed to mark log archive inifos deleted", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::mark_backup_set_infos_deleting(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement &clean_element = clean_tenant.backup_element_array_.at(i);
      if (OB_FAIL(mark_backup_set_info_deleting(clean_info, clean_element))) {
        LOG_WARN("failed to mark backup set info deleted", K(ret), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_backup_set_info_deleting(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSetId> backup_set_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_need_delete_backup_set_ids(clean_element, backup_set_ids))) {
    LOG_WARN("failed to get need delete backup set ids", K(ret), K(clean_info));
  } else if (OB_FAIL(mark_backup_set_info_inner_table_deleting(clean_info, clean_element, backup_set_ids))) {
    LOG_WARN("failed to mark backup set info inner table deleted", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::mark_backup_set_info_inner_table_deleting(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = clean_info.tenant_id_;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObBackupTaskHistoryUpdater updater;
  UNUSED(clean_element);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(updater.init(trans))) {
      LOG_WARN("failed to init history updater", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
        const ObBackupSetId &backup_set_id = backup_set_ids.at(i);
        if (ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not mark backup set deleted", K(ret), K(backup_set_id));
        } else if (OB_FAIL(updater.mark_backup_set_info_deleting(
                       tenant_id, backup_set_id.backup_set_id_, backup_set_id.copy_id_))) {
          LOG_WARN("failed to mark backup set info deleting", K(ret), K(backup_set_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]succ mark backup set info deleting", K(tenant_id), K(backup_set_ids));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_source_backup_set_file_info(const uint64_t tenant_id, const int64_t incarnation,
    const ObBackupSetId &backup_set_id, ObBackupSetFileInfo &backup_set_file_info, bool &is_need_modify)
{
  int ret = OB_SUCCESS;
  int64_t src_copy_id = 0;  // src backup dest copy_id=0
  bool for_update = false;
  char conf_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  is_need_modify = false;
  backup_set_file_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(tenant_id,
                 backup_set_id.backup_set_id_,
                 incarnation,
                 src_copy_id,
                 for_update,
                 *sql_proxy_,
                 backup_set_file_info))) {
    LOG_WARN("failed to get tenant backup set file info", K(ret), K(backup_set_id));
  } else if (OB_FAIL(GCONF.backup_dest.copy(conf_backup_dest, sizeof(conf_backup_dest)))) {
    LOG_WARN("failed to get configure backup dest", K(ret));
  } else if (0 != strcmp(conf_backup_dest, backup_set_file_info.backup_dest_.ptr())) {
    LOG_INFO("[BACKUP_CLEAN]the source backup destination of the backup backup are not equal to the configured backup "
             "destination",
        K(conf_backup_dest),
        K_(backup_set_file_info.backup_dest));
  } else {
    is_need_modify = true;
  }

  return ret;
}

int ObBackupDataClean::get_source_backup_dest_from_piece_file(const common::ObIArray<ObBackupPieceInfoKey> &piece_keys,
    ObClusterBackupDest &cluster_backup_dest, bool &is_need_modify)
{
  int ret = OB_SUCCESS;
  bool for_update = false;
  int64_t src_copy_id = 0;
  char conf_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  is_need_modify = false;
  cluster_backup_dest.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (piece_keys.empty()) {
    // do nothing
  } else {
    ObBackupPieceInfo piece_info;
    ObBackupDest backup_dest;
    ObLogArchiveBackupInfoMgr log_archive_info_mgr;
    const share::ObBackupPieceInfoKey &piece_key = piece_keys.at(0);
    if (OB_FAIL(log_archive_info_mgr.get_backup_piece(
            *sql_proxy_, for_update, piece_key.tenant_id_, piece_key.backup_piece_id_, src_copy_id, piece_info))) {
      LOG_WARN("failed to get piece file info", K(ret), K(piece_key));
    } else if (OB_FAIL(GCONF.backup_dest.copy(conf_backup_dest, sizeof(conf_backup_dest)))) {
      LOG_WARN("failed to get backup dest", K(ret));
    } else if (0 != strcmp(conf_backup_dest, piece_info.backup_dest_.ptr())) {
      LOG_INFO("[BACKUP_CLEAN]backup_dest of backup_set is not current backup dest",
          K(conf_backup_dest),
          K_(piece_info.backup_dest));
    } else if (OB_FAIL(backup_dest.set(conf_backup_dest))) {
      LOG_WARN("failed to set backup dest", K(ret));
    } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, piece_key.incarnation_))) {
      LOG_WARN("failed to set cluster backup dest", K(ret));
    } else {
      is_need_modify = true;
    }
  }

  return ret;
}

// mark the status of the external file at the source destination of the backup backup as deleting
int ObBackupDataClean::mark_extern_source_backup_set_info_of_backup_backup(const uint64_t tenant_id,
    const int64_t incarnation, const ObBackupSetFileInfo &backup_set_file_info,
    const ObArray<ObBackupSetIdPair> &backup_set_id_pairs, const bool is_deleting)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest src_cluster_backup_dest;
  ObBackupDest src_backup_dest;
  ObExternBackupSetFileInfoMgr src_extern_backup_set_file_info_mgr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(src_backup_dest.set(backup_set_file_info.backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret));
  } else if (OB_FAIL(src_cluster_backup_dest.set(src_backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(src_backup_dest));
  } else if (OB_FAIL(src_extern_backup_set_file_info_mgr.init(
                 tenant_id, src_cluster_backup_dest, true /* is_backup_backup */, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set file info", K(ret), K(src_cluster_backup_dest));
  } else {
    if (is_deleting &&
        OB_FAIL(src_extern_backup_set_file_info_mgr.mark_backup_set_file_deleting(backup_set_id_pairs))) {
      LOG_WARN("failed to mark backup set file deleting", K(ret));
    } else if (!is_deleting &&
               OB_FAIL(src_extern_backup_set_file_info_mgr.mark_backup_set_file_deleted(backup_set_id_pairs))) {
      LOG_WARN("failed to mark backup set file deleted", K(ret));
    }
  }
  return ret;
}

int ObBackupDataClean::mark_extern_backup_set_info_deleting(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = clean_info.tenant_id_;
  const int64_t incarnation = clean_element.incarnation_;
  const ObBackupDest &backup_dest = clean_element.backup_dest_;
  ObArray<ObBackupSetIdPair> backup_set_id_pairs;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObExternBackupSetFileInfoMgr extern_backup_set_file_info_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupSetIdPair backup_set_id_pair;
  const bool is_backup_backup = clean_info.copy_id_ > 0 || clean_info.is_delete_obsolete_backup_backup();
  bool src_backup_dest_unknown = true;
  bool is_need_modify = false;  // the status of the external file at the source destination of the backup backup
  ObBackupSetFileInfo backup_set_file_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (backup_set_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      const ObBackupSetId &backup_set_id = backup_set_ids.at(i);
      backup_set_id_pair.reset();
      if (ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not mark backup set deleted", K(ret), K(backup_set_id));
      } else {
        backup_set_id_pair.backup_set_id_ = backup_set_id.backup_set_id_;
        backup_set_id_pair.copy_id_ = backup_set_id.copy_id_;
        if (OB_FAIL(backup_set_id_pairs.push_back(backup_set_id_pair))) {
          LOG_WARN("failed to push backup set id into array", K(ret), K(backup_set_id_pair));
        }
      }

      if (src_backup_dest_unknown && OB_SUCC(ret) && is_backup_backup) {
        if (OB_FAIL(get_source_backup_set_file_info(
                tenant_id, incarnation, backup_set_id, backup_set_file_info, is_need_modify))) {
          LOG_WARN("failed to get source backup set file info", K(ret), K(backup_set_id));
        } else {
          src_backup_dest_unknown = false;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(extern_backup_info_mgr.init(tenant_id, cluster_backup_dest, *backup_lease_service_))) {
        LOG_WARN("failed to init extern backup info mgr", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(extern_backup_info_mgr.mark_backup_info_deleted(backup_set_id_pairs))) {
        LOG_WARN("failed to mark backup info deleted", K(ret), K(clean_info));
      }
    }

    if (OB_SUCC(ret) && is_backup_backup && is_need_modify) {
      if (OB_FAIL(mark_extern_source_backup_set_info_of_backup_backup(
              tenant_id, incarnation, backup_set_file_info, backup_set_id_pairs, true /* is_deleting*/))) {
        LOG_WARN("failed to mark source backup set file info deleting", K(ret), K(clean_info));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(extern_backup_set_file_info_mgr.init(
                   tenant_id, cluster_backup_dest, is_backup_backup, *backup_lease_service_))) {
      LOG_WARN("failed to init extern backup set file info", K(ret), K(clean_info));
    } else if (OB_FAIL(extern_backup_set_file_info_mgr.mark_backup_set_file_deleting(backup_set_id_pairs))) {
      LOG_WARN("failed to mark backup set file deleting", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::mark_log_archive_infos_deleting(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement &clean_element = clean_tenant.backup_element_array_.at(i);
      if (OB_FAIL(mark_log_archive_info_deleting(clean_info, clean_element))) {
        LOG_WARN("failed to mark log archive inifo deleted", K(ret), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_log_archive_info_deleting(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupPieceInfoKey> backup_piece_keys;
  ObArray<ObLogArchiveRound> log_archive_rounds;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_need_delete_clog_round_and_piece(
                 clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
    LOG_WARN("failed to get need delete clog round and piece", K(ret), K(clean_info));
  } else if (OB_FAIL(mark_log_archive_info_inner_table_deleting(clean_info, backup_piece_keys, log_archive_rounds))) {
    LOG_WARN("failed to mark backup set info inner table deleted", K(clean_info));
  }

  return ret;
}

int ObBackupDataClean::mark_log_archive_info_inner_table_deleting(const share::ObBackupCleanInfo &clean_info,
    const common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys,
    const common::ObIArray<ObLogArchiveRound> &log_archive_rounds)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_info_mgr;
  ObMySQLTransaction trans;
  const bool for_update = true;
  ObLogArchiveBackupInfo info;
  ObBackupPieceInfo piece_info;
  const ObBackupFileStatus::STATUS dest_file_status = ObBackupFileStatus::BACKUP_FILE_DELETING;
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_rounds.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = log_archive_rounds.at(i);
      ObLogArchiveBackupInfoMgr log_info_mgr;
      info.reset();
      if (log_archive_round.copy_id_ > 0 && OB_FAIL(log_info_mgr.set_backup_backup())) {
        LOG_WARN("failed to set log info mgr copy id", K(ret), K(log_archive_round));
      } else if (OB_FAIL(log_info_mgr.get_log_archive_history_info(trans,
                     clean_info.tenant_id_,
                     log_archive_round.log_archive_round_,
                     log_archive_round.copy_id_,
                     for_update,
                     info))) {
        if (OB_ENTRY_NOT_EXIST == ret && log_archive_round.copy_id_ > 0) {
          // backup backuplog may not generate backup round info, skip it
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get log archive history info", K(ret), K(clean_info), K(log_archive_round));
        }
      } else if (OB_FAIL(log_info_mgr.mark_log_archive_history_info_deleted(info, trans))) {
        LOG_WARN("failed to mark log archive history info deleted", K(ret), K(info));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_keys.count(); ++i) {
      const ObBackupPieceInfoKey &piece_key = backup_piece_keys.at(i);
      piece_info.reset();
      ObLogArchiveBackupInfoMgr log_info_mgr;
      if (piece_key.copy_id_ > 0 && OB_FAIL(log_info_mgr.set_backup_backup())) {
        LOG_WARN("failed to set log info mgr copy id", K(ret), K(piece_key));
      } else if (OB_FAIL(log_info_mgr.get_backup_piece(trans, for_update, piece_key, piece_info))) {
        LOG_WARN("failed to get backup piece", K(ret), K(piece_key));
      } else if (ObBackupFileStatus::BACKUP_FILE_DELETING == piece_info.file_status_ ||
                 ObBackupFileStatus::BACKUP_FILE_DELETED == piece_info.file_status_) {
        // do nothing
      } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(piece_info.file_status_, dest_file_status))) {
        LOG_WARN("failed to check can change status", K(ret), K(piece_info));
      } else if (FALSE_IT(piece_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETING)) {
      } else if (OB_FAIL(log_info_mgr.update_backup_piece(trans, piece_info))) {
        LOG_WARN("failed to update backup piece", K(ret), K(piece_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]succ mark log archive deleting",
            K_(clean_info.tenant_id),
            K(log_archive_rounds),
            K(backup_piece_keys));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::mark_extern_log_archive_info_deleting(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys,
    const common::ObIArray<ObLogArchiveRound> &log_archive_rounds)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> round_ids;
  ObLogArchiveBackupInfoMgr log_info_mgr;
  const uint64_t tenant_id = clean_info.tenant_id_;
  const int64_t incarnation = clean_element.incarnation_;
  const ObBackupDest &backup_dest = clean_element.backup_dest_;
  ObClusterBackupDest cluster_backup_dest;
  const bool is_backup_backup = clean_info.copy_id_ > 0 || clean_info.is_delete_obsolete_backup_backup();
  ObClusterBackupDest src_cluster_backup_dest;
  bool is_need_modify = false;  // the status of the external file at the source destination of the backup backup

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_rounds.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = log_archive_rounds.at(i);
      if (OB_FAIL(round_ids.push_back(log_archive_round.log_archive_round_))) {
        LOG_WARN("failed to push log archive round into array", K(ret), K(log_archive_round));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(log_info_mgr.mark_extern_log_archive_backup_info_deleted(
                   cluster_backup_dest, tenant_id, round_ids, *backup_lease_service_))) {
      LOG_WARN("failed to mark extern log archive backup info deleted", K(ret), K(cluster_backup_dest));
    } else if (is_backup_backup) {
      if (OB_FAIL(get_source_backup_dest_from_piece_file(backup_piece_keys, src_cluster_backup_dest, is_need_modify))) {
        LOG_WARN("failed to get source backup piece file info", K(ret));
      } else if (is_need_modify && OB_FAIL(log_info_mgr.mark_extern_backup_piece_deleting(src_cluster_backup_dest,
                                       tenant_id,
                                       backup_piece_keys,
                                       is_backup_backup,
                                       *backup_lease_service_))) {
        LOG_WARN("failed to mark source extern backup piece deleting", K(ret));
      }
    }

    if (OB_SUCC(ret) &&
        OB_FAIL(log_info_mgr.mark_extern_backup_piece_deleting(
            cluster_backup_dest, tenant_id, backup_piece_keys, is_backup_backup, *backup_lease_service_))) {
      LOG_WARN("failed to mark extern backup piece deleting", K(ret), K(cluster_backup_dest));
    }
  }
  return ret;
}

int ObBackupDataClean::get_need_delete_backup_set_ids(
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObBackupSetId> &backup_set_ids)
{
  int ret = OB_SUCCESS;
  backup_set_ids.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.backup_set_id_array_.count(); ++i) {
      const ObBackupSetId &backup_set_id = clean_element.backup_set_id_array_.at(i);
      if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
        // do nothing
      } else if (OB_FAIL(backup_set_ids.push_back(backup_set_id))) {
        LOG_WARN("failed to push backup set id into array", K(ret), K(backup_set_id));
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("[BACKUP_CLEAN]succ get need delete backup set ids", K(backup_set_ids));
    }
  }
  return ret;
}

int ObBackupDataClean::get_need_delete_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
    common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  log_archive_rounds.reset();
  backup_piece_keys.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (clean_info.is_delete_backup_piece()) {
    if (OB_FAIL(get_tenant_delete_piece(clean_info, clean_element, backup_piece_keys))) {
      LOG_WARN("failed to get tenant delete piece", K(ret), K(clean_info), K(clean_element));
    }
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    if (OB_FAIL(get_sys_tenant_delete_clog_round_and_piece(
            clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
      LOG_WARN("failed to get sys tenant delete clog round and piece", K(ret), K(clean_info));
    }
  } else {
    if (OB_FAIL(get_normal_tenant_delete_clog_round_and_piece(
            clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
      LOG_WARN("failed to get normal tenant delete clog round and piece", K(ret), K(clean_info));
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("[BACKUP_CLEAN]get need delete clog round and piece",
        K_(clean_info.tenant_id),
        K(log_archive_rounds),
        K(backup_piece_keys));
  }
  return ret;
}

int ObBackupDataClean::get_sys_tenant_delete_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
    common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sys tenant delete clog round and piece get invalid argument", K(ret), K(clean_info));
  } else if (ObBackupCleanInfoStatus::PREPARE == clean_info.status_) {
    if (OB_FAIL(get_sys_tenant_prepare_clog_round_and_piece(
            clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
      LOG_WARN("failed to get sys tenant prepare delete clog round and piece", K(ret), K(clean_info));
    }
  } else if (ObBackupCleanInfoStatus::DOING == clean_info.status_) {
    if (OB_FAIL(get_sys_tenant_doing_clog_round_and_piece(
            clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
      LOG_WARN("failed to get sys tenant prepare delete clog round and piece", K(ret), K(clean_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean info status is unexpected", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_sys_tenant_prepare_clog_piece(const share::ObBackupCleanInfo &clean_info,
    const ObLogArchiveRound &log_archive_round, const ObBackupDataCleanElement &clean_element, bool &is_delete_inorder,
    common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  const int64_t backup_copies = clean_element.backup_dest_option_.backup_copies_;
  const bool overwrite_key = true;
  ObSimplePieceKey simple_piece_key;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !log_archive_round.is_valid() || !clean_element.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(clean_info), K(log_archive_round), K(clean_element));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && is_delete_inorder && j < log_archive_round.piece_infos_.count(); ++j) {
      const ObSimplePieceInfo &simple_piece_info = log_archive_round.piece_infos_.at(j);
      if (simple_piece_info.max_ts_ > clean_info.clog_gc_snapshot_ ||
          ObBackupPieceStatus::BACKUP_PIECE_FROZEN != simple_piece_info.status_ ||
          ObBackupFileStatus::BACKUP_FILE_COPYING == simple_piece_info.file_status_ ||
          simple_piece_info.copies_num_ < backup_copies) {
        is_delete_inorder = false;
      } else if (ObBackupFileStatus::BACKUP_FILE_DELETED == simple_piece_info.file_status_) {
        // do nothing
      } else {
        ObBackupPieceInfoKey piece_info_key;
        piece_info_key.backup_piece_id_ = simple_piece_info.backup_piece_id_;
        piece_info_key.copy_id_ = log_archive_round.copy_id_;
        piece_info_key.incarnation_ = clean_element.incarnation_;
        piece_info_key.round_id_ = log_archive_round.log_archive_round_;
        piece_info_key.tenant_id_ = clean_info.tenant_id_;

        simple_piece_key.reset();
        simple_piece_key.incarnation_ = piece_info_key.incarnation_;
        simple_piece_key.round_id_ = piece_info_key.round_id_;
        simple_piece_key.backup_piece_id_ = piece_info_key.backup_piece_id_;
        simple_piece_key.copy_id_ = piece_info_key.copy_id_;
        char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
        int trace_length = 0;
        if (OB_FAIL(backup_piece_keys.push_back(piece_info_key))) {
          LOG_WARN("failed to push piece info key into array", K(ret), K(simple_piece_info), K(piece_info_key));
        } else if (OB_FAIL(sys_tenant_deleted_backup_piece_.set_refactored_1(simple_piece_key, overwrite_key))) {
          LOG_WARN("failed to set simple piece key", K(ret), K(simple_piece_key));
        } else if (FALSE_IT(trace_length = ObCurTraceId::get_trace_id()->to_string(
                                trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
        } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
        } else {
          FLOG_INFO("[BACKUP_CLEAN]succ add backup clean piece", K(simple_piece_key), K(clean_info));
          ROOTSERVICE_EVENT_ADD("backup_clean",
              "backup_piece",
              "tenant_id",
              clean_info.tenant_id_,
              "round_id",
              simple_piece_key.round_id_,
              "backup_piece_id",
              simple_piece_key.backup_piece_id_,
              "copy_id",
              simple_piece_key.copy_id_,
              "trace_id",
              trace_id);
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_sys_tenant_prepare_clog_round(const share::ObBackupCleanInfo &clean_info,
    const ObLogArchiveRound &log_archive_round, const ObBackupDataCleanElement &clean_element, bool &is_delete_inorder,
    common::ObIArray<ObLogArchiveRound> &log_archive_rounds)
{
  int ret = OB_SUCCESS;
  const int64_t backup_copies = clean_element.backup_dest_option_.backup_copies_;
  const bool overwrite_key = true;
  ObSimpleArchiveRound simple_archive_round;
  int piece_num = log_archive_round.piece_infos_.count();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !log_archive_round.is_valid() || !clean_element.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(clean_info), K(log_archive_round), K(clean_element));
  } else if (ObLogArchiveStatus::STOP != log_archive_round.log_archive_status_) {
    // do nothing
  } else if (log_archive_round.checkpoint_ts_ > clean_info.clog_gc_snapshot_ ||
             log_archive_round.copies_num_ < backup_copies) {
    is_delete_inorder = false;
  } else if (piece_num < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece num is less than 0", K(piece_num), K(log_archive_round));
  } else {
    if (0 == piece_num) {
      is_delete_inorder = true;
    } else {
      const ObSimplePieceInfo &simple_piece_info = log_archive_round.piece_infos_.at(piece_num - 1);
      if (simple_piece_info.max_ts_ > clean_info.clog_gc_snapshot_) {
        is_delete_inorder = false;
      }
    }
    if (!is_delete_inorder) {
    } else if (OB_FAIL(log_archive_rounds.push_back(log_archive_round))) {
      LOG_WARN("failed to push log archive round into array", K(ret), K(log_archive_round));
    } else {
      char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
      int trace_length = 0;
      simple_archive_round.reset();
      simple_archive_round.incarnation_ = clean_element.incarnation_;
      simple_archive_round.round_id_ = log_archive_round.log_archive_round_;
      simple_archive_round.copy_id_ = log_archive_round.copy_id_;
      if (OB_FAIL(sys_tenant_deleted_backup_round_.set_refactored_1(simple_archive_round, overwrite_key))) {
        LOG_WARN("failed to set sys tenant deleted backup round", K(ret), K(simple_archive_round));
      } else if (FALSE_IT(trace_length =
                              ObCurTraceId::get_trace_id()->to_string(trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
      } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]succ add backup clean round", K(simple_archive_round), K(clean_info));
        ROOTSERVICE_EVENT_ADD("backup_clean",
            "backup_round",
            "tenant_id",
            clean_info.tenant_id_,
            "round_id",
            simple_archive_round.round_id_,
            "copy_id",
            simple_archive_round.copy_id_,
            "trace_id",
            trace_id);
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_sys_tenant_prepare_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
    common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  log_archive_rounds.reset();
  backup_piece_keys.reset();
  ObLogArchiveBackupInfoMgr log_info_mgr;
  bool is_delete_inorder = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || OB_SYS_TENANT_ID != clean_info.tenant_id_ ||
             ObBackupCleanInfoStatus::PREPARE != clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sys tenant delete clog round and piece get invalid argument", K(ret), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.log_archive_round_array_.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = clean_element.log_archive_round_array_.at(i);
      if (!is_delete_inorder) {
        break;
      } else if (clean_info.is_delete_backup_set() && 0 != log_archive_round.start_piece_id_) {
        // do nothing
      } else if (OB_FAIL(get_sys_tenant_prepare_clog_piece(
                     clean_info, log_archive_round, clean_element, is_delete_inorder, backup_piece_keys))) {
        LOG_WARN("get sys tenant delete clog piece", K(ret), K(clean_info), K(log_archive_round));
      } else if (OB_FAIL(get_sys_tenant_prepare_clog_round(
                     clean_info, log_archive_round, clean_element, is_delete_inorder, log_archive_rounds))) {
        LOG_WARN("get sys tenant delete clog round", K(ret), K(clean_info), K(log_archive_round), K(backup_piece_keys));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_sys_tenant_doing_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
    common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  ObBackupPieceInfoKey backup_piece_key;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (ObBackupCleanInfoStatus::DOING != clean_info.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean info status is unexpected", K(ret));
  } else {
    hash::ObHashSet<ObSimplePieceKey>::const_iterator piece_iter;
    for (piece_iter = sys_tenant_deleted_backup_piece_.begin();
         OB_SUCC(ret) && piece_iter != sys_tenant_deleted_backup_piece_.end();
         ++piece_iter) {
      backup_piece_key.reset();
      const ObSimplePieceKey &simple_piece_key = piece_iter->first;
      backup_piece_key.backup_piece_id_ = simple_piece_key.backup_piece_id_;
      backup_piece_key.copy_id_ = simple_piece_key.copy_id_;
      backup_piece_key.incarnation_ = simple_piece_key.incarnation_;
      backup_piece_key.round_id_ = simple_piece_key.round_id_;
      backup_piece_key.tenant_id_ = clean_info.tenant_id_;
      if (OB_FAIL(backup_piece_keys.push_back(backup_piece_key))) {
        LOG_WARN("failed to push backup piece key into array", K(ret), K(backup_piece_key), K(simple_piece_key));
      }
    }

    hash::ObHashSet<ObSimpleArchiveRound>::const_iterator round_iter;
    for (round_iter = sys_tenant_deleted_backup_round_.begin();
         OB_SUCC(ret) && round_iter != sys_tenant_deleted_backup_round_.end();
         ++round_iter) {
      const ObSimpleArchiveRound &simple_archive_round = round_iter->first;
      for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.log_archive_round_array_.count(); ++i) {
        const ObLogArchiveRound &tmp_archive_round = clean_element.log_archive_round_array_.at(i);
        if (tmp_archive_round.log_archive_round_ == simple_archive_round.round_id_ &&
            tmp_archive_round.copy_id_ == simple_archive_round.copy_id_) {
          if (OB_FAIL(log_archive_rounds.push_back(tmp_archive_round))) {
            LOG_WARN("failed to push log archive round into array", K(ret), K(tmp_archive_round));
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_normal_tenant_delete_clog_round_and_piece(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObLogArchiveRound> &log_archive_rounds,
    common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  log_archive_rounds.reset();
  backup_piece_keys.reset();
  const uint64_t tenant_id = clean_info.tenant_id_;
  const int64_t incarnation = clean_element.incarnation_;
  ObSimpleArchiveRound simple_archive_round;
  ObSimplePieceKey simple_piece_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get normal tenant delete clog round and piece get invalid argument", K(ret), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.log_archive_round_array_.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = clean_element.log_archive_round_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < log_archive_round.piece_infos_.count(); ++j) {
        const ObSimplePieceInfo &simple_piece_info = log_archive_round.piece_infos_.at(j);
        simple_piece_key.reset();
        simple_piece_key.incarnation_ = incarnation;
        simple_piece_key.round_id_ = simple_piece_info.round_id_;
        simple_piece_key.backup_piece_id_ = simple_piece_info.backup_piece_id_;
        simple_piece_key.copy_id_ = log_archive_round.copy_id_;
        const int hash_ret = sys_tenant_deleted_backup_piece_.exist_refactored(simple_piece_key);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          // do nothing
        } else if (OB_HASH_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("failed to check piece key exist", K(ret), K(simple_piece_key));
        } else {
          ObBackupPieceInfoKey piece_info_key;
          piece_info_key.backup_piece_id_ = simple_piece_info.backup_piece_id_;
          piece_info_key.copy_id_ = log_archive_round.copy_id_;
          piece_info_key.incarnation_ = incarnation;
          piece_info_key.round_id_ = log_archive_round.log_archive_round_;
          piece_info_key.tenant_id_ = tenant_id;
          if (OB_FAIL(backup_piece_keys.push_back(piece_info_key))) {
            LOG_WARN("failed to push piece info key into array", K(ret), K(simple_piece_info), K(piece_info_key));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        simple_archive_round.reset();
        simple_archive_round.incarnation_ = clean_element.incarnation_;
        simple_archive_round.round_id_ = log_archive_round.log_archive_round_;
        simple_archive_round.copy_id_ = log_archive_round.copy_id_;
        const int hash_ret = sys_tenant_deleted_backup_round_.exist_refactored(simple_archive_round);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          // do nothing
        } else if (OB_HASH_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("failed to check round exist", K(ret), K(simple_piece_key));
        } else if (OB_FAIL(log_archive_rounds.push_back(log_archive_round))) {
          LOG_WARN("failed to push log archive round into array", K(ret), K(log_archive_round));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_backup_data(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  ObTenantBackupDataCleanMgr tenant_backup_data_clean_mgr;
  if (OB_FAIL(tenant_backup_data_clean_mgr.init(clean_info, clean_tenant, this, sql_proxy_))) {
    LOG_WARN("failed to init tenant backup data clean mgr", K(ret), K(clean_info), K(clean_tenant));
  } else if (OB_FAIL(tenant_backup_data_clean_mgr.do_clean())) {
    LOG_WARN("failed to do clean", K(ret), K(clean_info), K(clean_tenant));
  }

  return ret;
}

int ObBackupDataClean::delete_tenant_backup_meta_data(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
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
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSetId> backup_set_ids;
  ObArray<ObLogArchiveRound> log_archive_rounds;
  ObArray<ObBackupPieceInfoKey> backup_piece_keys;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark_extern_backup_infos_deleted get invalid argument", K(ret), K(clean_info), K(clean_tenant));
  } else {
    const ObIArray<ObBackupDataCleanElement> &backup_element_array = clean_tenant.backup_element_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement &clean_element = backup_element_array.at(i);
      if (OB_FAIL(get_need_delete_backup_set_ids(clean_element, backup_set_ids))) {
        LOG_WARN("failed to get need delete backup set ids", K(ret), K(clean_info));
      } else if (OB_FAIL(get_need_delete_clog_round_and_piece(
                     clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
        LOG_WARN("failed to get need delete clog round and piece", K(ret), K(clean_info));
      } else if (OB_FAIL(delete_extern_tmp_files(clean_info, clean_element))) {
        LOG_WARN("failed to delete extern tmp files", K(ret), K(clean_info));
      } else if (OB_FAIL(mark_extern_backup_set_info_deleting(clean_info, clean_element, backup_set_ids))) {
        LOG_WARN("failed to mark extern backup set info deleted", K(ret), K(clean_info));
      } else if (OB_FAIL(mark_extern_log_archive_info_deleting(
                     clean_info, clean_element, backup_piece_keys, log_archive_rounds))) {
        LOG_WARN("failed to mark extern backup set info deleted", K(ret), K(clean_info));
      } else if (OB_FAIL(delete_extern_backup_info_deleted(clean_info, clean_element, backup_set_ids))) {
        LOG_WARN("failed to makr extern backup info deleted", K(ret), K(clean_info), K(clean_element));
      } else if (OB_FAIL(delete_extern_clog_info_deleted(clean_info, clean_element, log_archive_rounds))) {
        LOG_WARN("failed to mark extern clog info deleted", K(ret), K(clean_info), K(clean_element));
      } else if (OB_FAIL(mark_extern_backup_set_file_info_deleted(clean_info, clean_element, backup_set_ids))) {
        LOG_WARN("failed to mark extern backup set file info deleted", K(ret), K(clean_info));
      } else if (OB_FAIL(mark_extern_backup_piece_file_info_deleted(clean_info, clean_element, backup_piece_keys))) {
        LOG_WARN("failed to mark extern backup piece file info deleted", K(ret), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_extern_backup_info_deleted(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObClusterBackupDest current_backup_dest;
  ObArray<int64_t> set_ids;
  const bool is_backup_backup = clean_info.copy_id_ > 0 || clean_info.is_delete_obsolete_backup_backup();
  ObClusterBackupDest conf_backup_dest;
  ObClusterBackupDest conf_backup_backup_dest;
  const bool is_update_timestamp = clean_element.backup_dest_option_.auto_touch_reserved_backup_;
  bool can_delete_file = false;
  bool is_empty = false;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(current_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (backup_dest_.is_valid() && OB_FAIL(conf_backup_dest.set(backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest_));
  } else if (is_backup_backup) {
    if (OB_FAIL(backup_backup_dest_.is_valid() &&
                conf_backup_backup_dest.set(backup_backup_dest_, clean_element.incarnation_))) {
      LOG_WARN("failed to set original backup dest", K(ret), K(backup_backup_dest_));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      const ObBackupSetId &backup_set_id = backup_set_ids.at(i);
      if (ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set clean mode is unexpected", K(ret), K(backup_set_id));
      } else if (OB_FAIL(set_ids.push_back(backup_set_id.backup_set_id_))) {
        LOG_WARN("failed to push set id into array", K(ret), K(backup_set_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(extern_backup_info_mgr.init(clean_info.tenant_id_, current_backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(current_backup_dest), K(clean_info));
  } else if (OB_FAIL(extern_backup_info_mgr.delete_marked_backup_info(set_ids, is_empty))) {
    LOG_WARN("failed to delete marked backup info", K(ret), K(clean_element));
  } else if (!is_empty) {
    can_delete_file = false;
  } else if (!conf_backup_dest.is_valid()) {
    can_delete_file = true;
  } else if (OB_FAIL(extern_backup_info_mgr.get_backup_path(conf_backup_dest, path))) {
    LOG_WARN("failed to get backup path", K(ret), K(conf_backup_dest));
  } else if (OB_FAIL(check_can_delete_extern_info_file(
                 clean_info.tenant_id_, current_backup_dest, is_backup_backup, path, can_delete_file))) {
    LOG_WARN("failed to check can delete extern info file", K(ret), K(clean_info));
  }

  if (OB_FAIL(ret)) {
  } else if (!can_delete_file) {
    if (!is_update_timestamp) {
      // do nothing
    } else if (OB_FAIL(extern_backup_info_mgr.update_backup_info_file_timestamp())) {
      LOG_WARN("failed to update backup info file timestamp", K(ret));
    }
  } else {
    if (is_update_timestamp) {
      // do nothing
    } else if (OB_FAIL(extern_backup_info_mgr.delete_backup_info_file())) {
      LOG_WARN("failed to delete backup info file", K(ret));
    }
  }
  return ret;
}

int ObBackupDataClean::delete_extern_clog_info_deleted(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const ObIArray<ObLogArchiveRound> &log_archive_rounds)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObClusterBackupDest current_backup_dest;
  ObArray<int64_t> round_ids;
  const bool is_backup_backup = clean_info.copy_id_ > 0 || clean_info.is_delete_obsolete_backup_backup();
  ObClusterBackupDest conf_backup_dest;
  ObClusterBackupDest conf_backup_backup_dest;
  const int64_t tenant_id = clean_info.tenant_id_;
  const bool is_update_timestamp = clean_element.backup_dest_option_.auto_touch_reserved_backup_;
  bool is_empty = false;
  bool can_delete_file = false;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_rounds.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = log_archive_rounds.at(i);
      if (OB_FAIL(round_ids.push_back(log_archive_round.log_archive_round_))) {
        LOG_WARN("failed to push log archive round id into array", K(ret), K(log_archive_round));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(current_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (backup_dest_.is_valid() && OB_FAIL(conf_backup_dest.set(backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest_));
  } else if (is_backup_backup) {
    if (backup_backup_dest_.is_valid() &&
        OB_FAIL(conf_backup_backup_dest.set(backup_backup_dest_, clean_element.incarnation_))) {
      LOG_WARN("failed to set original backup dest", K(ret), K(backup_backup_dest_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mgr.delete_marked_extern_log_archive_backup_info(
                 current_backup_dest, tenant_id, round_ids, is_empty, *backup_lease_service_))) {
    LOG_WARN("failed to delete marked extern log archive backup info", K(current_backup_dest), K(clean_info));
  } else if (!is_empty) {
    can_delete_file = false;
  } else if (!conf_backup_dest.is_valid()) {
    can_delete_file = true;
  } else if (OB_FAIL(mgr.get_extern_backup_info_path(conf_backup_dest, clean_info.tenant_id_, path))) {
    LOG_WARN("failed to get backup path", K(ret), K(conf_backup_dest));
  } else if (OB_FAIL(check_can_delete_extern_info_file(
                 clean_info.tenant_id_, current_backup_dest, is_backup_backup, path, can_delete_file))) {
    LOG_WARN("failed to check can delete extern info file", K(ret), K(clean_info));
  }

  if (OB_FAIL(ret)) {
  } else if (!can_delete_file) {
    if (!is_update_timestamp) {
      // do nothing
    } else if (OB_FAIL(mgr.update_extern_backup_info_file_timestamp(current_backup_dest, tenant_id))) {
      LOG_WARN("failed to update backup info file timestamp", K(ret));
    }
  } else {
    if (is_update_timestamp) {
      // do nothing
    } else if (OB_FAIL(mgr.delete_extern_backup_info_file(current_backup_dest, tenant_id))) {
      LOG_WARN("failed to delete backup info file", K(ret));
    }
  }
  return ret;
}

int ObBackupDataClean::delete_extern_tmp_files(
    const share::ObBackupCleanInfo &clean_info, const ObBackupDataCleanElement &clean_element)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupPath cluster_prefix_path;
  ObBackupPath data_prefix_path;
  ObBackupPath clog_prefix_path;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    if (OB_FAIL(ObBackupPathUtil::get_cluster_prefix_path(cluster_backup_dest, cluster_prefix_path))) {
      LOG_WARN("failed to get cluster prefix path", K(ret), K(cluster_backup_dest), K(clean_info));
    } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(
                   cluster_prefix_path, cluster_backup_dest.get_storage_info()))) {
      LOG_WARN("failed to delete tmp files", K(ret), K(cluster_prefix_path));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_data_path(
            cluster_backup_dest, clean_info.tenant_id_, data_prefix_path))) {
      LOG_WARN("failed to get tenant prefix path", K(ret), K(cluster_backup_dest), K(clean_info));
    } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_path(
                   cluster_backup_dest, clean_info.tenant_id_, clog_prefix_path))) {
      LOG_WARN("failed to get tenant prefix path", K(ret), K(cluster_backup_dest), K(clean_info));
    } else if (OB_FAIL(
                   ObBackupDataCleanUtil::delete_tmp_files(data_prefix_path, cluster_backup_dest.get_storage_info()))) {
      LOG_WARN("failed to delete tmp files", K(ret), K(cluster_prefix_path));
    } else if (OB_FAIL(
                   ObBackupDataCleanUtil::delete_tmp_files(clog_prefix_path, cluster_backup_dest.get_storage_info()))) {
      LOG_WARN("failed to delete tmp files", K(ret), K(cluster_prefix_path));
    }
  }
  return ret;
}

int ObBackupDataClean::mark_extern_backup_set_file_info_deleted(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupSetId> &backup_set_ids)
{
  int ret = OB_SUCCESS;
  ObExternBackupSetFileInfoMgr extern_backup_set_file_info_mgr;
  ObClusterBackupDest current_backup_dest;
  const bool is_backup_backup = clean_info.copy_id_ > 0 || clean_info.is_delete_obsolete_backup_backup();
  ObArray<ObBackupSetIdPair> backup_set_id_pairs;
  ObBackupPath path;
  ObBackupSetIdPair backup_set_id_pair;
  bool src_backup_dest_unknown = true;
  bool is_need_modify = false;  // the status of the external file at the source destination of the backup backup
  ObBackupSetFileInfo backup_set_file_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(current_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      const ObBackupSetId &backup_set_id = backup_set_ids.at(i);
      backup_set_id_pair.reset();
      if (ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set clean mode is unexpected", K(ret), K(backup_set_id));
      } else {
        backup_set_id_pair.backup_set_id_ = backup_set_id.backup_set_id_;
        backup_set_id_pair.copy_id_ = backup_set_id.copy_id_;
        if (OB_FAIL(backup_set_id_pairs.push_back(backup_set_id_pair))) {
          LOG_WARN("failed to push backup set id pair into array", K(ret), K(backup_set_id_pair));
        }
      }

      if (src_backup_dest_unknown && OB_SUCC(ret) && is_backup_backup) {
        if (OB_FAIL(get_source_backup_set_file_info(clean_info.tenant_id_,
                clean_element.incarnation_,
                backup_set_id,
                backup_set_file_info,
                is_need_modify))) {
          LOG_WARN("failed to get source backup set file info", K(ret), K(backup_set_id));
        } else {
          src_backup_dest_unknown = false;
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_backup_backup && is_need_modify) {
    if (OB_FAIL(mark_extern_source_backup_set_info_of_backup_backup(clean_info.tenant_id_,
            clean_element.incarnation_,
            backup_set_file_info,
            backup_set_id_pairs,
            false /* is_deleting*/))) {
      LOG_WARN("failed to mark source backup set file info deleted", K(ret), K(clean_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(extern_backup_set_file_info_mgr.init(
                 clean_info.tenant_id_, current_backup_dest, is_backup_backup, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(current_backup_dest), K(clean_info));
  } else if (OB_FAIL(extern_backup_set_file_info_mgr.mark_backup_set_file_deleted(backup_set_id_pairs))) {
    LOG_WARN("failed to delete marked backup info", K(ret), K(clean_element));
  }
  return ret;
}

int ObBackupDataClean::mark_extern_backup_piece_file_info_deleted(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, const common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  ObClusterBackupDest current_backup_dest;
  ObClusterBackupDest src_cluster_backup_dest;
  const bool is_backup_backup = clean_info.copy_id_ > 0 || clean_info.is_delete_obsolete_backup_backup();
  bool is_need_modify = false;  // the status of the external file at the source destination of the backup backup

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(current_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (is_backup_backup) {
    if (OB_FAIL(get_source_backup_dest_from_piece_file(backup_piece_keys, src_cluster_backup_dest, is_need_modify))) {
      LOG_WARN("failed to get source backup piece file info", K(ret));
    } else if (is_need_modify && OB_FAIL(log_archive_info_mgr.mark_extern_backup_piece_deleted(src_cluster_backup_dest,
                                     clean_info.tenant_id_,
                                     backup_piece_keys,
                                     is_backup_backup,
                                     *backup_lease_service_))) {
      LOG_WARN("failed to mark source extern backup piece deleted", K(ret), K(clean_info), K(src_cluster_backup_dest));
    }
  }

  if (OB_SUCC(ret) &&
      OB_FAIL(log_archive_info_mgr.mark_extern_backup_piece_deleted(
          current_backup_dest, clean_info.tenant_id_, backup_piece_keys, is_backup_backup, *backup_lease_service_))) {
    LOG_WARN("failed to mark extern backup piece deleted", K(ret), K(clean_info), K(current_backup_dest));
  }

  return ret;
}

int ObBackupDataClean::delete_inner_table_his_data(
    const ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement &clean_element = clean_tenant.backup_element_array_.at(i);
      if (OB_FAIL(delete_marked_backup_task_his_data(clean_info, clean_element, trans))) {
        LOG_WARN("failed to delete marked backup task his data", K(ret), K(clean_info));
      } else if (OB_FAIL(delete_marked_log_archive_status_his_data(clean_info, clean_element, trans))) {
        LOG_WARN("failed to delete marked log archive stauts his data", K(ret), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_marked_backup_task_his_data(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSetId> backup_set_ids;
  ObBackupTaskHistoryUpdater updater;
  const int64_t tenant_id = clean_info.tenant_id_;
  const bool for_update = true;
  ObArray<ObTenantBackupTaskInfo> tenant_backup_tasks;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_need_delete_backup_set_ids(clean_element, backup_set_ids))) {
    LOG_WARN("failed to get need delete backup set ids", K(ret), K(clean_info));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("faile to init backup task history updater", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      tenant_backup_tasks.reset();
      const ObBackupSetId &backup_set_id = backup_set_ids.at(i);
      ObTenantBackupTaskInfo backup_task_info;
      if (ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set id clean mode is unexpected", K(ret), K(backup_set_id));
      } else if (OB_FAIL(updater.get_tenant_backup_task(tenant_id,
                     backup_set_id.backup_set_id_,
                     backup_set_id.copy_id_,
                     for_update,
                     tenant_backup_tasks))) {
        LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(backup_set_id));
      } else if (tenant_backup_tasks.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant backup tasks should not be empty", K(ret), K(backup_set_id), K(clean_info));
      } else if (FALSE_IT(backup_task_info = tenant_backup_tasks.at(tenant_backup_tasks.count() - 1))) {
        // 1.Delete backupset history table only has one task info
        // 2.Delete backup backupset history table may has multi task infos, but last info's status is SUCCESS OR FAILED
        // We use THE LAST task info to check should delete. We use backup set file info's file status to mutex
        // clean thread and backup backup thread
      } else if (OB_FAIL(ObBackupTaskCleanHistoryOpertor::insert_task_info(
                     clean_info.job_id_, backup_set_id.copy_id_, backup_task_info, trans))) {
        LOG_WARN("failed to insert task into clean history", K(ret), K(backup_task_info));
      } else if (OB_FAIL(
                     updater.delete_backup_set_info(tenant_id, backup_set_id.backup_set_id_, backup_set_id.copy_id_))) {
        LOG_WARN("failed to delete backup set info", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::delete_marked_log_archive_status_his_data(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObLogArchiveRound> log_archive_rounds;
  ObArray<ObBackupPieceInfoKey> backup_piece_keys;
  ObLogArchiveBackupInfo log_archive_info;
  ObBackupPieceInfo backup_piece_info;
  const bool for_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(get_need_delete_clog_round_and_piece(
                 clean_info, clean_element, log_archive_rounds, backup_piece_keys))) {
    LOG_WARN("failed to get need delete clog round and piece", K(ret), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_rounds.count(); ++i) {
      log_archive_info.reset();
      ObLogArchiveBackupInfoMgr log_archive_backup_info_mgr;
      const ObLogArchiveRound &log_archive_round = log_archive_rounds.at(i);
      if (log_archive_round.copy_id_ > 0 && OB_FAIL(log_archive_backup_info_mgr.set_backup_backup())) {
        LOG_WARN("failed to set copy id", K(ret), K(log_archive_round));
      } else if (OB_FAIL(log_archive_backup_info_mgr.get_log_archive_history_info(trans,
                     clean_info.tenant_id_,
                     log_archive_round.log_archive_round_,
                     log_archive_round.copy_id_,
                     for_update,
                     log_archive_info))) {
        if (OB_ENTRY_NOT_EXIST == ret && log_archive_round.copy_id_ > 0) {
          // backup backuplog may not generate backup round info, skip it
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get log archive history info", K(ret), K(clean_info), K(log_archive_round));
        }
      } else if (OB_FAIL(log_archive_backup_info_mgr.delete_log_archive_info(log_archive_info, trans))) {
        LOG_WARN("failed to delete clog archive info", K(ret), K(clean_info));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_keys.count(); ++i) {
      backup_piece_info.reset();
      ObLogArchiveBackupInfoMgr log_archive_backup_info_mgr;
      const ObBackupPieceInfoKey &piece_info_key = backup_piece_keys.at(i);
      if (piece_info_key.copy_id_ > 0 && OB_FAIL(log_archive_backup_info_mgr.set_backup_backup())) {
        LOG_WARN("failed to set copy id", K(ret), K(piece_info_key));
      } else if (OB_FAIL(log_archive_backup_info_mgr.get_backup_piece(
                     trans, for_update, piece_info_key, backup_piece_info))) {
        LOG_WARN("failed to get backup piece", K(ret), K(piece_info_key));
      } else if (FALSE_IT(backup_piece_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETED)) {
      } else if (OB_FAIL(log_archive_backup_info_mgr.update_backup_piece(trans, backup_piece_info))) {
        LOG_WARN("failed to update backup piece", K(ret), K(backup_piece_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo &src_clean_info,
    const share::ObBackupCleanInfo &dest_clean_info)
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

int ObBackupDataClean::update_clean_info(const uint64_t tenant_id, const share::ObBackupCleanInfo &src_clean_info,
    const share::ObBackupCleanInfo &dest_clean_info, common::ObISQLClient &trans)
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
  const bool for_update = true;
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
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, sys_clean_info))) {
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
            LOG_INFO("[BACKUP_CLEAN]set sys clean info result failed", K(sys_clean_info), K(inner_error_));
          } else {
            sys_clean_info.result_ = OB_CANCELED;
            LOG_INFO("[BACKUP_CLEAN]set sys clean info result cancel", K(ret), K(sys_clean_info));
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

int ObBackupDataClean::check_need_cleanup_prepared_infos(const ObBackupCleanInfo &sys_clean_info, bool &need_clean)
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
    LOG_INFO(
        "[BACKUP_CLEAN]sys tenant is in prepare status, need cleanup", K(ret), K(sys_clean_info), K(is_prepare_flag));
  }
  return ret;
}

int ObBackupDataClean::cleanup_tenant_prepared_infos(const uint64_t tenant_id, ObISQLClient &sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  const bool for_update = true;
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
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
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
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &sys_trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
  ObBackupCleanInfoHistoryUpdater updater;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (simple_clean_tenant.is_deleted_) {
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, sys_trans, clean_info))) {
      LOG_WARN("failed to get backup data clean info", K(ret));
    }
  } else {
    if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, for_update, *sql_proxy_, clean_info))) {
      LOG_WARN("failed to get backup data clean info", K(ret));
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
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, int32_t &clean_result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
  clean_result = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  const bool for_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, sys_clean_info))) {
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
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::check_tenant_backup_clean_task_failed(
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
    const ObBackupCleanInfo &sys_clean_info, common::ObISQLClient &sys_tenant_trans, int32_t &result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  const bool for_update = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SUCCESS != sys_clean_info.result_) {
    result = sys_clean_info.result_;
  } else if (OB_SUCCESS != inner_error_) {
    result = inner_error_;
  } else {
    for (int64_t i = 0; OB_SUCCESS == result && OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = normal_clean_tenants.at(i);
      ObBackupCleanInfo tenant_clean_info;
      tenant_clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal clean tenant should not be sys tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, sys_tenant_trans, tenant_clean_info))) {
          LOG_WARN("failed to push tenant clean info into array", K(ret), K(tenant_clean_info));
        } else if (ObBackupCleanInfoStatus::STOP != tenant_clean_info.status_) {
          // do nothing
        } else if (OB_SUCCESS != tenant_clean_info.result_) {
          result = tenant_clean_info.result_;
        }
      } else {
        if (OB_FAIL(
                get_backup_clean_info(simple_clean_tenant.tenant_id_, for_update, *sql_proxy_, tenant_clean_info))) {
          LOG_WARN("failed to push tenant clean info into array", K(ret), K(simple_clean_tenant));
        } else if (OB_FAIL(rootserver::ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
          LOG_WARN("failed to check sys tenant trans alive", K(ret), K(tenant_clean_info));
        } else if (ObBackupCleanInfoStatus::STOP != tenant_clean_info.status_) {
          // do nothing
        } else if (OB_SUCCESS != tenant_clean_info.result_) {
          result = tenant_clean_info.result_;
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::update_tenant_backup_clean_task_failed(
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, common::ObISQLClient &sys_tenant_trans,
    const int32_t result)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = MIN_EXECUTE_TIMEOUT_US;
  const bool for_update = true;
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
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = normal_clean_tenants.at(i);
      ObBackupCleanInfo tenant_clean_info;
      ObMySQLTransaction trans;
      tenant_clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("clean tenant should not be sys clean tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, sys_tenant_trans, tenant_clean_info))) {
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
          if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, for_update, trans, tenant_clean_info))) {
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
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants,
    const share::ObBackupCleanInfo &sys_clean_info, const ObBackupDataCleanTenant &sys_clean_tenant,
    const int32_t clean_result)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  ObBackupCleanInfo dest_sys_clean_info;
  ObMySQLTransaction trans;
  ObBackupInfoManager backup_info_manager;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SUCCESS == clean_result) {
    if (OB_FAIL(delete_cluster_backup_meta_data(sys_clean_info, sys_clean_tenant, normal_clean_tenants))) {
      LOG_WARN("failed to delete backup meta data", K(ret), K(sys_clean_info), K(sys_clean_tenant));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    // push sys clean info into array
    dest_sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, dest_sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(insert_clean_infos_into_history(normal_clean_tenants, trans))) {
      LOG_WARN("failed to insert clean infos into history", K(ret), K(normal_clean_tenants));
    } else if (OB_FAIL(reset_backup_clean_infos(normal_clean_tenants, trans))) {
      LOG_WARN("failed to reset backup clean infos", K(ret), K(normal_clean_tenants));
    } else if (OB_FAIL(backup_info_manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
      LOG_WARN("failed to init backup info manager", K(ret));
    } else if (OB_SUCCESS == clean_result && sys_clean_info.is_delete_obsolete()) {
      const ObBackupManageArg::Type type = sys_clean_info.is_delete_obsolete_backup()
                                               ? ObBackupManageArg::DELETE_OBSOLETE_BACKUP
                                               : ObBackupManageArg::DELETE_OBSOLETE_BACKUP_BACKUP;
      if (OB_FAIL(backup_info_manager.update_delete_obsolete_snapshot(
              sys_clean_info.tenant_id_, type, sys_clean_info.start_time_, trans))) {
        LOG_WARN("failed to update last delete obsolete snapshot", K(ret), K(sys_clean_info));
      }
    }
    DEBUG_SYNC(BACKUP_DATA_SYS_CLEAN_STATUS_DOING);
    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS == clean_result &&
               OB_FAIL(delete_inner_table_his_data(dest_sys_clean_info, sys_clean_tenant, trans))) {
      LOG_WARN("failed to delete inner table his data", K(ret), K(sys_clean_info));
    } else if (OB_FAIL(set_sys_clean_info_stop(dest_sys_clean_info, trans))) {
      LOG_WARN("failed to set sys clean info stop", K(ret), K(sys_clean_info));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_trans(trans))) {
      LOG_WARN("failed to commit trans", K(ret));
    }
  } else {
    int tmp_ret = trans.end(false /*commit*/);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    retry_count_ = 0;
  }
  return ret;
}

int ObBackupDataClean::insert_clean_infos_into_history(
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, common::ObISQLClient &sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_clean_tennat = normal_clean_tenants.at(i);
      if (OB_FAIL(insert_tenant_backup_clean_info_history(simple_clean_tennat, sys_tenant_trans))) {
        LOG_WARN("failed to insert tenant backup clean info", K(ret), K(simple_clean_tennat));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::reset_backup_clean_infos(
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, common::ObISQLClient &sys_trans)
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = normal_clean_tenants.at(i);
      ObMySQLTransaction trans;
      ObBackupCleanInfo dest_clean_info;
      ObBackupCleanInfo clean_info;
      ObTimeoutCtx timeout_ctx;
      clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal clean tenants should not has sys clean tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, sys_trans, clean_info))) {
          LOG_WARN("failed to get backup clean info", K(ret));
        } else if (OB_FAIL(delete_backup_clean_info(OB_SYS_TENANT_ID, clean_info, sys_trans))) {
          LOG_WARN("failed to delete backup clean info", K(ret), K(clean_info));
        }
      } else {
        if (OB_FAIL(start_trans(timeout_ctx, trans))) {
          LOG_WARN("failed to start trans", K(ret));
        } else {
          if (OB_FAIL(get_backup_clean_info(simple_clean_tenant.tenant_id_, for_update, trans, clean_info))) {
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
            dest_clean_info.copy_id_ = 0;

            if (OB_FAIL(update_clean_info(simple_clean_tenant.tenant_id_, clean_info, dest_clean_info, trans))) {
              LOG_WARN("failed to update clean info", K(ret), K(clean_info), K(dest_clean_info));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(commit_trans(trans))) {
              LOG_WARN("failed to commit trans", K(ret));
            }
          } else {
            int tmp_ret = trans.end(false /*commit*/);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
              ret = OB_SUCCESS == ret ? tmp_ret : ret;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::check_all_tenant_clean_tasks_stopped(
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, bool &is_all_tasks_stopped)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo dest_clean_info;
  is_all_tasks_stopped = true;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; is_all_tasks_stopped && OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = normal_clean_tenants.at(i);
      ObBackupCleanInfo tenant_clean_info;
      tenant_clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
      if (OB_SYS_TENANT_ID == simple_clean_tenant.tenant_id_) {
        // do nothing
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal clean tenant should not has sys clean tenant", K(ret), K(simple_clean_tenant));
      } else if (simple_clean_tenant.is_deleted_) {
        if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, *sql_proxy_, tenant_clean_info))) {
          LOG_WARN("failed to get backup clean info", K(ret), K(simple_clean_tenant));
        }
      } else {
        if (OB_FAIL(
                get_backup_clean_info(simple_clean_tenant.tenant_id_, for_update, *sql_proxy_, tenant_clean_info))) {
          LOG_WARN("failed to get backup clean info", K(ret), K(simple_clean_tenant));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (ObBackupCleanInfoStatus::STOP != tenant_clean_info.status_) {
        is_all_tasks_stopped = false;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::update_clog_gc_snaphost(
    const int64_t cluster_clog_gc_snapshot, ObBackupCleanInfo &clean_info, ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  int64_t clog_gc_snapshot = 0;
  ObBackupCleanInfo backup_clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (cluster_clog_gc_snapshot < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get clog gc snapshot get invalid argument", K(ret), K(cluster_clog_gc_snapshot));
  } else if (OB_FAIL(get_clog_gc_snapshot(clean_info, clean_tenant, cluster_clog_gc_snapshot, clog_gc_snapshot))) {
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
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, backup_clean_info))) {
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

int ObBackupDataClean::get_clog_gc_snapshot(const ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanTenant &clean_tenant, const int64_t cluster_clog_gc_snapshot, int64_t &clog_gc_snapshot)
{
  int ret = OB_SUCCESS;
  clog_gc_snapshot = INT64_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (cluster_clog_gc_snapshot < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get clog gc snapshot get invalid argument", K(ret), K(cluster_clog_gc_snapshot));
  } else if (clean_info.is_delete_backup_set()) {
    // delete failed backup set, so clog gc snapshot may be 0
    clog_gc_snapshot = clean_tenant.clog_gc_snapshot_;
  } else if (clean_info.is_delete_backup_piece() || clean_info.is_delete_backup_round()) {
    if (clean_tenant.clog_gc_snapshot_ <= 0 && !clean_tenant.backup_element_array_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("clean info is delete backup set or piece but clog gc snapshot is invalid",
          K(ret),
          K(clean_info),
          K(clean_tenant));
    } else {
      clog_gc_snapshot = clean_tenant.clog_gc_snapshot_;
    }
  } else if (!clean_tenant.clog_data_clean_point_.is_valid() ||
             0 == clean_tenant.clog_data_clean_point_.start_replay_log_ts_) {
    // do nothing
    clog_gc_snapshot = cluster_clog_gc_snapshot;
  } else if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
    clog_gc_snapshot = clean_tenant.clog_data_clean_point_.start_replay_log_ts_;
  } else {
    clog_gc_snapshot = std::min(clean_tenant.clog_data_clean_point_.start_replay_log_ts_, cluster_clog_gc_snapshot);
  }

  FLOG_INFO("[BACKUP_CLEAN]get tenant backup clog gc snapshot",
      K(ret),
      K(clog_gc_snapshot),
      K(cluster_clog_gc_snapshot),
      K(clean_tenant));
  return ret;
}

int ObBackupDataClean::get_deleted_clean_tenants(ObIArray<ObSimpleBackupDataCleanTenant> &deleted_tenants)
{
  int ret = OB_SUCCESS;
  deleted_tenants.reset();
  ObMySQLTransaction trans;
  ObBackupCleanInfo sys_clean_info;
  ObArray<ObBackupCleanInfo> deleted_tenant_clean_infos;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, sys_clean_info))) {
      LOG_WARN("failed to get clean info", K(ret));
    } else if (OB_FAIL(get_deleted_tenant_clean_infos(trans, deleted_tenant_clean_infos))) {
      LOG_WARN("failed to get deleted tenant clean infos", K(ret), K(sys_clean_info));
    } else {
      ObSimpleBackupDataCleanTenant deleted_clean_tenant;
      for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tenant_clean_infos.count(); ++i) {
        const ObBackupCleanInfo &backup_clean_info = deleted_tenant_clean_infos.at(i);
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
    ObISQLClient &trans, common::ObIArray<ObBackupCleanInfo> &deleted_tenant_clean_infos)
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
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &deleted_clean_tenants)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  ObMySQLTransaction trans;
  ObBackupCleanInfo sys_clean_info;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (deleted_clean_tenants.empty()) {
    // do nothing
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, sys_clean_info))) {
      LOG_WARN("failed to get clean info", K(ret));
    } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_ &&
               ObBackupCleanInfoStatus::CANCEL != sys_clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schedule deleted clean tenants get unexpected status", K(ret), K(sys_clean_info));
    } else if (updater.init(trans)) {
      LOG_WARN("failed to init tenant backup clean info updater", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < deleted_clean_tenants.count(); ++i) {
        const ObSimpleBackupDataCleanTenant &simple_clean_tenant = deleted_clean_tenants.at(i);
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

int ObBackupDataClean::get_clean_tenants(const common::ObIArray<ObBackupDataCleanTenant> &clean_tenants,
    common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants, ObBackupDataCleanTenant &sys_clean_tenant)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenants.count(); ++i) {
      const ObBackupDataCleanTenant &clean_tenant = clean_tenants.at(i);
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = clean_tenant.simple_clean_tenant_;
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
    const uint64_t tenant_id, const ObBackupCleanInfo &clean_info, ObISQLClient &trans)
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

int ObBackupDataClean::set_sys_clean_info_stop(const ObBackupCleanInfo &backup_clean_info, ObISQLClient &trans)
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
    if (OB_FAIL(set_comment(dest_clean_info.comment_))) {
      LOG_WARN("failed to set comment", K(ret), K(dest_clean_info));
    } else if (OB_FAIL(set_error_msg(result, dest_clean_info.error_msg_))) {
      LOG_WARN("failed to set error msg", K(ret), K(dest_clean_info));
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
      dest_clean_info.copy_id_ = 0;

      if (OB_FAIL(update_clean_info(OB_SYS_TENANT_ID, backup_clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update clean info", K(ret), K(backup_clean_info), K(dest_clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::try_clean_tenant_backup_dir(const ObBackupDataCleanTenant &clean_tenant)
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
    const ObIArray<ObBackupDataCleanElement> &backup_element_array = clean_tenant.backup_element_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement &clean_element = backup_element_array.at(i);
      if (OB_FAIL(clean_tenant_backup_dir(tenant_id, clean_element))) {
        LOG_WARN("failed to clean tenant backup dir", K(ret), K(tenant_id), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::clean_tenant_backup_dir(const uint64_t tenant_id, const ObBackupDataCleanElement &clean_element)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupPath path;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType &device_type = clean_element.backup_dest_.device_type_;

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

int ObBackupDataClean::clean_backup_tenant_info(const ObBackupDataCleanTenant &sys_clean_tenant,
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!sys_clean_tenant.is_valid() || OB_SYS_TENANT_ID != sys_clean_tenant.simple_clean_tenant_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean backup tenant get invalid argument", K(ret), K(sys_clean_tenant));
  } else {
    const ObIArray<ObBackupDataCleanElement> &backup_element_array = sys_clean_tenant.backup_element_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array.count(); ++i) {
      const ObBackupDataCleanElement &clean_element = backup_element_array.at(i);
      if (OB_FAIL(inner_clean_backup_tenant_info(clean_element, normal_clean_tenants))) {
        LOG_WARN("failed to clean backup tenant info", K(ret), K(clean_element));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::inner_clean_backup_tenant_info(const ObBackupDataCleanElement &clean_element,
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants)
{
  int ret = OB_SUCCESS;
  const ObStorageType device_type = clean_element.backup_dest_.device_type_;
  ObExternTenantInfoMgr tenant_info_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObStorageUtil util(false /*need retry*/);
  ObBackupPath path;
  ObArenaAllocator allocator;
  ObArray<ObString> file_name_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(tenant_info_mgr.init(cluster_backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init tenant info mgr", K(ret), K(cluster_backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_clean_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &normal_clean_tenant = normal_clean_tenants.at(i);
      if (normal_clean_tenant.is_deleted_) {
        path.reset();
        file_name_array.reset();
        allocator.reset();
        if (OB_FAIL(ObBackupPathUtil::get_tenant_path(cluster_backup_dest, normal_clean_tenant.tenant_id_, path))) {
          LOG_WARN("failed to get tenant path", K(ret), K(cluster_backup_dest));
        } else if (OB_FAIL(util.list_files(
                       path.get_ptr(), cluster_backup_dest.get_storage_info(), allocator, file_name_array))) {
          LOG_WARN("failed to list files", K(ret), K(path));
        } else if (!file_name_array.empty()) {
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

int ObBackupDataClean::touch_extern_tenant_name(const ObBackupDataCleanTenant &clean_tenant)
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
      const ObBackupDataCleanElement &clean_element = clean_tenant.backup_element_array_.at(i);
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

int ObBackupDataClean::touch_extern_clog_info(const ObBackupDataCleanTenant &clean_tenant)
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
      const ObBackupDataCleanElement &clean_element = clean_tenant.backup_element_array_.at(i);
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

int ObBackupDataClean::delete_cluster_backup_meta_data(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanTenant &clean_tenant,
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &normal_clean_tenants)
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

int ObBackupDataClean::get_cluster_max_succeed_backup_set(const int64_t copy_id, int64_t &backup_set_id)
{
  int ret = OB_SUCCESS;
  backup_set_id = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get cluster max succeed backup set get invalid argument", K(ret), K(copy_id));
  } else {
    if (0 == copy_id) {
      if (OB_FAIL(inner_get_cluster_max_succeed_backup_set(backup_set_id))) {
        LOG_WARN("failed to get cluster max succeed backup set", KR(ret));
      }
    } else {
      if (OB_FAIL(inner_get_cluster_max_succeed_backup_backup_set(copy_id, backup_set_id))) {
        LOG_WARN("failed to get cluster max succeed backup set", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::inner_get_cluster_max_succeed_backup_set(int64_t &backup_set_id)
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
      backup_set_id = INT64_MAX;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant max succeed backup task", K(ret));
    }
  } else {
    backup_set_id = task_info.backup_set_id_;
  }
  return ret;
}

int ObBackupDataClean::inner_get_cluster_max_succeed_backup_backup_set(const int64_t copy_id, int64_t &backup_set_id)
{
  int ret = OB_SUCCESS;
  ObTenantBackupBackupsetTaskInfo task_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_max_succeed_task(
                 OB_SYS_TENANT_ID, copy_id, task_info, *sql_proxy_))) {
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
    const int64_t snapshot_version, const ObArray<ObLogArchiveBackupInfo> &log_infos, ObLogArchiveBackupInfo &log_info)
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
      if (log_infos.at(i).status_.start_ts_ > log_infos.at(i + 1).status_.start_ts_) {
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

int ObBackupDataClean::check_backupset_continue_with_clog_data(const ObTenantBackupTaskInfo &backup_task_info,
    const common::ObArray<ObLogArchiveBackupInfo> &log_archive_infos, bool &is_continue)
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

int ObBackupDataClean::check_inner_table_version_()
{
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
  } else if (is_valid_backup_inner_table_version(inner_table_version_) &&
             inner_table_version_ >= OB_BACKUP_INNER_TABLE_V3) {
    // inner table version is new enough
  } else if (OB_FAIL(ObBackupInfoOperator::get_inner_table_version(*sql_proxy_, inner_table_version_))) {
    LOG_WARN("Failed to get inner table version", K(ret));
  } else if (inner_table_version_ >= OB_BACKUP_INNER_TABLE_V1 && inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    ret = OB_EAGAIN;
    LOG_INFO("[BACKUP_CLEAN]inner table version is too old, waiting backup inner table upgrade",
        K(ret),
        K(inner_table_version_));
  } else if (OB_FAIL(prepare_deleted_tenant_backup_infos())) {
    LOG_WARN("failed to prepare deleted tenant backup infos", K(ret));
  }

  return ret;
}

int ObBackupDataClean::check_can_do_task()
{
  // TODO (muwei.ym) consider tenant cancel delete backup
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
  const bool for_update = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (stop_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("rs is stopping", K(ret));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, *sql_proxy_, sys_clean_info))) {
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
    const ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
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

int ObBackupDataClean::do_sys_tenant_cancel_delete_backup(const ObBackupCleanInfo &clean_info)
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

int ObBackupDataClean::set_tenant_clean_info_cancel(const ObBackupCleanInfo &clean_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant_ids;
  ObMySQLTransaction trans;
  ObTimeoutCtx timeout_ctx;
  ObBackupCleanInfo sys_clean_info;
  sys_clean_info.tenant_id_ = OB_SYS_TENANT_ID;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != clean_info.tenant_id_ || ObBackupCleanInfoStatus::CANCEL != clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sys tenant cancel delete backup get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret), K(clean_info));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(clean_info.tenant_id_, for_update, trans, sys_clean_info))) {
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
          const ObBackupCleanInfo &delete_tenant_clean_info = deleted_tenant_clean_infos.at(i);
          if (OB_FAIL(set_deleted_tenant_cancel(delete_tenant_clean_info, trans))) {
            LOG_WARN("failed to set deleted tenant cancel", K(ret), K(delete_tenant_clean_info));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::set_normal_tenant_cancel(const uint64_t tenant_id, common::ObISQLClient &sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupCleanInfo clean_info;
  clean_info.tenant_id_ = tenant_id;
  ObBackupCleanInfo dest_clean_info;
  ObTimeoutCtx timeout_ctx;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set normal tenant cancel get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
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
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::set_deleted_tenant_cancel(
    const ObBackupCleanInfo &clean_info, common::ObISQLClient &sys_tenant_trans)
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
    const ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant)
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

int ObBackupDataClean::do_scheduler_normal_tenant(share::ObBackupCleanInfo &clean_info,
    ObBackupDataCleanTenant &clean_tenant, common::ObIArray<ObTenantBackupTaskInfo> &task_infos,
    common::ObIArray<ObLogArchiveBackupInfo> &log_archive_infos)
{
  int ret = OB_SUCCESS;
  clean_tenant.backup_element_array_.reset();
  ObBackupCleanInfo sys_clean_info;
  clean_info.reset();
  ObSimpleBackupDataCleanTenant &simple_clean_tenant = clean_tenant.simple_clean_tenant_;

  const bool for_update = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant clean scheduler get invalid argument", K(ret), K(clean_tenant));
  } else {
    uint64_t tenant_id = simple_clean_tenant.is_deleted_ ? OB_SYS_TENANT_ID : simple_clean_tenant.tenant_id_;
    clean_info.tenant_id_ = simple_clean_tenant.tenant_id_;
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, *sql_proxy_, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(clean_tenant));
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_ ||
               ObBackupCleanInfoStatus::PREPARE == clean_info.status_) {
      // do nothing
    } else if (OB_FAIL(get_all_tenant_backup_infos(
                   clean_info, clean_tenant.simple_clean_tenant_, *sql_proxy_, task_infos, log_archive_infos))) {
      LOG_WARN("failed to get all tenant backup infos", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::check_backup_dest_lifecycle(const ObBackupDataCleanTenant &clean_tenant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupDest current_backup_dest;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, sizeof(backup_dest_str)))) {
    LOG_WARN("failed to copy backup dest", K(ret));
  } else if (0 == strlen(backup_dest_str)) {
    // do nothing
  } else if (OB_FAIL(current_backup_dest.set(backup_dest_str))) {
    LOG_WARN("failed to set current backup dest", K(ret), K(backup_dest_str));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement &data_clean_element = clean_tenant.backup_element_array_.at(i);
      const ObBackupDest &backup_dest = data_clean_element.backup_dest_;
      if (current_backup_dest == backup_dest) {
        if (OB_SUCCESS !=
            (tmp_ret = ObBackupUtil::check_backup_dest_lifecycle(backup_dest, is_update_reserved_backup_timestamp_))) {
          LOG_WARN("failed to check backup dest lifecycle", K(ret), K(backup_dest));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_all_tenant_backup_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
    common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get all tenant backup task infos get invalid argument", K(ret), K(clean_info));
  } else if (clean_info.is_backup_set_clean() || clean_info.is_delete_backup_piece() ||
             clean_info.is_delete_backup_round()) {
    if (OB_FAIL(get_delete_backup_infos(
            clean_info, simple_clean_tenant, trans, tenant_backup_infos, tenant_backup_log_infos))) {
      LOG_WARN("failed to get delete backupset task infos", K(ret), K(clean_info));
    }
  } else if (clean_info.is_delete_obsolete_backup()) {
    if (OB_FAIL(get_delete_obsolete_backup_set_infos(
            clean_info, simple_clean_tenant, trans, tenant_backup_infos, tenant_backup_log_infos))) {
      LOG_WARN("failed to get delete obsolete backup set infos", K(ret), K(clean_info));
    }
  } else if (clean_info.is_delete_obsolete_backup_backup()) {
    if (OB_FAIL(get_delete_obsolete_backup_backupset_infos(
            clean_info, simple_clean_tenant, trans, tenant_backup_infos, tenant_backup_log_infos))) {
      LOG_WARN("failed to get delete obsolete backup backupset infos", K(ret), K(clean_info));
    }
  } else {
    // TODO(muwei.ym) add delete backup piece && auto delete
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean type is unexpected", K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_delete_backup_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
    common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos)
{
  int ret = OB_SUCCESS;
  tenant_backup_infos.reset();
  tenant_backup_log_infos.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backupset task infos get invalid argument", K(ret), K(clean_info));
  } else if (0 == clean_info.copy_id_) {
    if (OB_FAIL(get_tenant_backup_task_infos(clean_info, simple_clean_tenant, trans, tenant_backup_infos))) {
      LOG_WARN("failed to get tenant backup task infos", K(ret), K(clean_info));
    } else if (OB_FAIL(get_tenant_backup_log_infos(clean_info, simple_clean_tenant, trans, tenant_backup_log_infos))) {
      LOG_WARN("failed to get tenant backup piece infos", K(ret), K(clean_info));
    }
  } else {
    if (OB_FAIL(get_tenant_backup_backupset_task_infos(clean_info, simple_clean_tenant, trans, tenant_backup_infos))) {
      LOG_WARN("failed to get tenant backup backupset task infos", K(ret), K(clean_info));
    } else if (OB_FAIL(get_tenant_backup_backuplog_infos(
                   clean_info, simple_clean_tenant, trans, tenant_backup_log_infos))) {
      LOG_WARN("failed to get tenant backup backuppiece infos", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::get_delete_obsolete_backup_set_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
    common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos)
{
  int ret = OB_SUCCESS;
  tenant_backup_infos.reset();
  tenant_backup_log_infos.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret), K(clean_info));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_obsolete_backup()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backupset task infos get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(get_tenant_backup_task_infos(clean_info, simple_clean_tenant, trans, tenant_backup_infos))) {
    LOG_WARN("failed to get tenant backup task infos", K(ret), K(clean_info));
  } else if (OB_FAIL(get_tenant_backup_log_infos(clean_info, simple_clean_tenant, trans, tenant_backup_log_infos))) {
    LOG_WARN("failed to get tenant backup piece infos", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_delete_obsolete_backup_backupset_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_backup_infos,
    common::ObIArray<share::ObLogArchiveBackupInfo> &tenant_backup_log_infos)
{
  int ret = OB_SUCCESS;
  tenant_backup_infos.reset();
  tenant_backup_log_infos.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret), K(clean_info));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_obsolete_backup_backup()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backupset task infos get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(
                 get_tenant_backup_backupset_task_infos(clean_info, simple_clean_tenant, trans, tenant_backup_infos))) {
    LOG_WARN("failed to get tenant backup task infos", K(ret), K(clean_info));
  } else if (OB_FAIL(
                 get_tenant_backup_backuplog_infos(clean_info, simple_clean_tenant, trans, tenant_backup_log_infos))) {
    LOG_WARN("failed to get tenant backup piece infos", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_tenant_backup_task_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_infos)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater updater;
  ObBackupTaskHistoryUpdater his_updater;
  ObArray<share::ObTenantBackupTaskInfo> tmp_tenant_infos;
  ObTenantBackupTaskInfo tenant_info;
  const bool is_backup_backup = false;
  const bool for_update = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task his info get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(his_updater.init(trans))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init backup task updater", K(ret));
  } else if (OB_FAIL(his_updater.get_tenant_backup_tasks(clean_info.tenant_id_, is_backup_backup, tmp_tenant_infos))) {
    LOG_WARN("failed to get tenant full backup tasks", K(ret), K(clean_info));
  } else if (!simple_clean_tenant.is_deleted_) {
    if (OB_FAIL(updater.get_tenant_backup_task(clean_info.tenant_id_, for_update, tenant_info))) {
      LOG_WARN("failed to get tenant backup tasks", K(ret), K(clean_info));
    } else if (tenant_info.is_valid() && OB_FAIL(tmp_tenant_infos.push_back(tenant_info))) {
      LOG_WARN("failed to push tenant info into array", K(ret), K(tenant_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_infos.count(); ++i) {
      const ObTenantBackupTaskInfo &tenant_task_info = tmp_tenant_infos.at(i);
      if (clean_info.is_delete_obsolete_backup() && tenant_task_info.backup_dest_ != backup_dest_) {
        // do nothing
      } else if (OB_FAIL(tenant_infos.push_back(tenant_task_info))) {
        LOG_WARN("failed to push tenant task info into array", K(ret), K(tenant_task_info));
      }
    }
  }
  if (OB_SUCC(ret)) {
    FLOG_INFO("[BACKUP_CLEAN]succeed get tenant backup task his info", K(tenant_infos));
  }
  return ret;
}

int ObBackupDataClean::get_tenant_backup_backupset_task_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<share::ObTenantBackupTaskInfo> &tenant_infos)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObTenantBackupBackupsetTaskInfo> tenant_backupset_infos;
  ObArray<share::ObTenantBackupTaskInfo> his_tenant_backupset_infos;
  SimpleBackupBackupsetTenant simple_tenant;
  simple_tenant.tenant_id_ = simple_clean_tenant.tenant_id_;
  simple_tenant.is_dropped_ = simple_clean_tenant.is_deleted_;
  const bool is_backup_backup = true;
  ObBackupTaskHistoryUpdater his_updater;
  ObTenantBackupTaskInfo prev_task_info;
  ObTenantBackupTaskInfo tenant_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task his info get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(his_updater.init(trans))) {
    LOG_WARN("failed to init his updater", K(ret));
  } else if (OB_FAIL(his_updater.get_tenant_backup_tasks(
                 clean_info.tenant_id_, is_backup_backup, his_tenant_backupset_infos))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(clean_info));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_items(simple_tenant, tenant_backupset_infos, trans))) {
    LOG_WARN("failed to get full backupset task items", KR(ret), K(clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < his_tenant_backupset_infos.count(); ++i) {
      const ObTenantBackupTaskInfo &backup_task_info = his_tenant_backupset_infos.at(i);
      if ((clean_info.is_delete_obsolete_backup_backup() && backup_task_info.backup_dest_ != backup_backup_dest_) ||
          (clean_info.copy_id_ > 0 && backup_task_info.copy_id_ != clean_info.copy_id_)) {
        // do nothing
      } else if (OB_FAIL(tenant_infos.push_back(backup_task_info))) {
        LOG_WARN("failed to push back backup info", KR(ret), K(tenant_info));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_backupset_infos.count(); ++i) {
      tenant_info.reset();
      const ObTenantBackupBackupsetTaskInfo &backupset_info = tenant_backupset_infos.at(i);
      if (OB_FAIL(backupset_info.convert_to_backup_task_info(tenant_info))) {
        LOG_WARN("failed to convert to backup task info", KR(ret), K(backupset_info));
      } else if ((clean_info.is_delete_obsolete_backup_backup() && tenant_info.backup_dest_ != backup_backup_dest_) ||
                 (clean_info.copy_id_ > 0 && tenant_info.copy_id_ != clean_info.copy_id_)) {
        // do nothing
      } else if (OB_FAIL(tenant_infos.push_back(tenant_info))) {
        LOG_WARN("failed to push back backup info", KR(ret), K(tenant_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(duplicate_task_info(tenant_infos))) {
        LOG_WARN("failed to duplicate task infos", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_tenant_backup_log_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<ObLogArchiveBackupInfo> &tenant_backup_log_infos)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  const bool for_update = false;
  ObArray<ObLogArchiveBackupInfo> tmp_log_archive_infos;
  ObLogArchiveBackupInfo tmp_log_archive_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret), K(clean_info));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backupset task infos get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_log_archive_history_infos(
                 trans, clean_info.tenant_id_, for_update, tmp_log_archive_infos))) {
    LOG_WARN("failed to get log arhive history infos", K(ret), K(clean_info));
  } else if (!simple_clean_tenant.is_deleted_) {
    if (OB_FAIL(log_archive_info_mgr.get_log_archive_backup_info(
            trans, for_update, clean_info.tenant_id_, inner_table_version_, tmp_log_archive_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get log archive backup info", K(ret), K(clean_info));
      }
    } else if (ObLogArchiveStatus::STOP == tmp_log_archive_info.status_.status_) {
      // do nothing
    } else if (OB_FAIL(tmp_log_archive_infos.push_back(tmp_log_archive_info))) {
      LOG_WARN("failed to push log archive info into array", K(ret), K(tmp_log_archive_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_log_archive_infos.count(); ++i) {
      const ObLogArchiveBackupInfo &log_archive_info = tmp_log_archive_infos.at(i);
      ObBackupDest backup_dest;
      if (ObLogArchiveStatus::DOING != log_archive_info.status_.status_ &&
          ObLogArchiveStatus::INTERRUPTED != log_archive_info.status_.status_ &&
          ObLogArchiveStatus::STOP != log_archive_info.status_.status_) {
        // do nothing
      } else if (OB_FAIL(backup_dest.set(log_archive_info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", K(ret), K(log_archive_info));
      } else if (clean_info.is_delete_obsolete_backup() && backup_dest_ != backup_dest) {
        // do nothing
      } else if (OB_FAIL(tenant_backup_log_infos.push_back(log_archive_info))) {
        LOG_WARN("failed to push log archive info into array", K(ret), K(log_archive_info));
      }
    }
  }
  return ret;
}

// step1 get backup backup log info from piece info if exist
// step2 get log archive info from log_archive_history if exist
// step3 get log archive info from backup log table if exist
// NOTE:step1 get log archive info from piece because if piece_interval > 0,
// backup backup log will not record log archive info into history, so we use piece infos to get log archive info

int ObBackupDataClean::get_tenant_backup_backuplog_infos(const share::ObBackupCleanInfo &clean_info,
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, common::ObISQLClient &trans,
    common::ObIArray<ObLogArchiveBackupInfo> &tenant_backup_log_infos)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  const bool for_update = false;
  ObArray<ObLogArchiveBackupInfo> tmp_log_archive_infos;
  const int64_t GCONF_BACKUP_COPY_ID = 10001;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret), K(clean_info));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backupset task infos get invalid argument", K(ret), K(clean_info));
  } else if (clean_info.is_delete_backup_round()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("clean info is delete backup backup round, not support now", K(ret), K(clean_info));
  } else if (clean_info.is_delete_backup_set() && clean_info.copy_id_ > GCONF_BACKUP_COPY_ID) {
    // do nothing
  } else if (clean_info.is_delete_backup_piece()) {
    ObLogArchiveBackupInfo tmp_log_archive_info;
    if (OB_FAIL(log_archive_info_mgr.get_backup_log_archive_info_from_piece_info(trans,
            simple_clean_tenant.tenant_id_,
            clean_info.backup_piece_id_,
            clean_info.copy_id_,
            for_update,
            tmp_log_archive_info))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get backup log archive info from piece info", K(ret), K(clean_info));
      }
    } else if (OB_FAIL(tenant_backup_log_infos.push_back(tmp_log_archive_info))) {
      LOG_WARN("failed to push log archive info into array", K(ret), K(clean_info), K(tmp_log_archive_info));
    }
  } else {
    // clean info delete obsolete backup backup
    if (OB_FAIL(log_archive_info_mgr.get_backup_log_archive_info_from_original_piece_infos(
            trans, simple_clean_tenant.tenant_id_, for_update, tmp_log_archive_infos))) {
      LOG_WARN("failed to get backup log archive info from piece infos", K(ret), K(clean_info));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_log_archive_infos.count(); ++i) {
        const ObLogArchiveBackupInfo &log_archive_info = tmp_log_archive_infos.at(i);
        ObBackupDest backup_dest;
        if (ObLogArchiveStatus::DOING != log_archive_info.status_.status_ &&
            ObLogArchiveStatus::INTERRUPTED != log_archive_info.status_.status_ &&
            ObLogArchiveStatus::STOP != log_archive_info.status_.status_) {
          // do nothing
        } else if (OB_FAIL(backup_dest.set(log_archive_info.backup_dest_))) {
          LOG_WARN("failed to set backup dest", K(ret), K(log_archive_info));
        } else if (clean_info.is_delete_obsolete_backup_backup() && backup_backup_dest_ != backup_dest) {
          // do nothing
        } else if (OB_FAIL(tenant_backup_log_infos.push_back(log_archive_info))) {
          LOG_WARN("failed to push log archive info into array", K(ret), K(log_archive_info));
        }
      }
    }
  }

  return ret;
}

int ObBackupDataClean::get_backup_dest_option(const ObBackupDest &backup_dest, ObBackupDestOpt &backup_dest_option)
{
  int ret = OB_SUCCESS;
  backup_dest_option.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup dest option get invalid argument", K(ret), K(backup_dest));
  } else if (backup_dest == backup_dest_) {
    backup_dest_option = backup_dest_option_;
  } else if (backup_dest == backup_backup_dest_) {
    backup_dest_option = backup_backup_dest_option_;
  } else {
    backup_dest_option.auto_delete_obsolete_backup_ = true;
    backup_dest_option.is_valid_ = true;
  }
  return ret;
}

int ObBackupDataClean::set_current_backup_dest()
{
  int ret = OB_SUCCESS;
  const int64_t backup_dest_buf_len = OB_MAX_BACKUP_DEST_LENGTH;
  char backup_dest_buf[backup_dest_buf_len] = "";
  char backup_backup_dest_buf[backup_dest_buf_len] = "";

  backup_dest_.reset();
  backup_backup_dest_.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_buf, backup_dest_buf_len))) {
    LOG_WARN("failed to copy backup dest", K(ret));
  } else if (0 == strlen(backup_dest_buf)) {
    // do nothing
  } else if (OB_FAIL(backup_dest_.set(backup_dest_buf))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_dest_buf));
  } else if (OB_FAIL(backup_dest_option_.init(false /*is_backup_backup*/))) {
    LOG_WARN("failed to init backup dest option", K(ret));
  } else {
    LOG_INFO("[BACKUP_CLEAN]succ set current backup dest", K(backup_dest_buf));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_buf, backup_dest_buf_len))) {
    LOG_WARN("failed to copy backup dest", K(ret));
  } else if (0 == strlen(backup_backup_dest_buf)) {
    // do nothing
  } else if (OB_FAIL(backup_backup_dest_.set(backup_backup_dest_buf))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_dest_buf));
  } else if (OB_FAIL(backup_backup_dest_option_.init(true /*is_backup_backup*/))) {
    LOG_WARN("failed to init backup dest option", K(ret));
  } else {
    LOG_INFO("[BACKUP_CLEAN]succ set current backup backup dest", K(backup_backup_dest_buf));
  }

  return ret;
}

int ObBackupDataClean::get_clean_tenants_from_history_table(
    const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupTaskInfo> tenant_backup_tasks;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get clean tenants from history table get invalid argument", K(ret), K(clean_info));
  } else if (clean_info.is_delete_backup_set()) {
    if (OB_FAIL(get_delete_backup_set_tenants_from_history_table(clean_info, clean_tenants_map))) {
      LOG_WARN("failed to get delete backup set tenants from history table", K(ret), K(clean_info));
    }
  } else if (clean_info.is_delete_backup_piece()) {
    if (OB_FAIL(get_delete_backup_piece_tenants_from_history_table(clean_info, clean_tenants_map))) {
      LOG_WARN("failed to get delete backup piece tenants from history table", K(ret), K(clean_info));
    }
  } else if (clean_info.is_delete_backup_round()) {
    if (get_delete_backup_round_tenants_from_history_table(clean_info, clean_tenants_map)) {
      LOG_WARN("failed to get delete backup round tenants from history table", K(ret), K(clean_info));
    }
  } else {
    if (OB_FAIL(get_delete_obsolete_backup_tenants_from_history_table(clean_info, clean_tenants_map))) {
      LOG_WARN("failed to get delete obsolete backup tenants from history table", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupDataClean::get_delete_backup_set_tenants_from_history_table(
    const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObArray<uint64_t> tenant_ids;
  ObBackupTaskHistoryUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_info.is_backup_set_clean()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backup set tenants from history table get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init task history updater", K(ret));
  } else if (OB_FAIL(updater.get_tenant_ids_with_backup_set_id(clean_info.backup_set_id_, for_update, tenant_ids))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(clean_info));
  } else if (OB_FAIL(set_history_tenant_info_into_map(tenant_ids, clean_tenants_map))) {
    LOG_WARN("failed to set history tenant info into map", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_delete_backup_piece_tenants_from_history_table(
    const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_backup_piece()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backup set tenants from history table get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_tenant_ids_with_piece_id(
                 *sql_proxy_, clean_info.backup_piece_id_, clean_info.copy_id_, tenant_ids))) {
    LOG_WARN("failed to get tenant ids with piece id", K(ret), K(clean_info));
  } else if (OB_FAIL(set_history_tenant_info_into_map(tenant_ids, clean_tenants_map))) {
    LOG_WARN("failed to set history tenant info into map", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_delete_backup_round_tenants_from_history_table(
    const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObArray<uint64_t> tenant_ids;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_backup_round()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete backup set tenants from history table get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_tenant_ids_with_round_id(
                 *sql_proxy_, for_update, clean_info.backup_round_id_, clean_info.copy_id_, tenant_ids))) {
    LOG_WARN("failed to get tenant ids with round id", K(ret), K(clean_info));
  } else if (OB_FAIL(set_history_tenant_info_into_map(tenant_ids, clean_tenants_map))) {
    LOG_WARN("failed to set history tenant info into map", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::get_delete_obsolete_backup_tenants_from_history_table(
    const ObBackupCleanInfo &clean_info, hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> log_archive_tenant_ids;
  ObArray<uint64_t> backup_set_tenant_ids;
  ObBackupTaskHistoryUpdater updater;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  const bool is_backup_backup = clean_info.is_delete_obsolete_backup_backup();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!clean_info.is_valid() || !clean_info.is_delete_obsolete()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get delete obsolete backup tenants from history table get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_backup_tenant_ids_with_snapshot(
                 *sql_proxy_, clean_info.expired_time_, is_backup_backup, log_archive_tenant_ids))) {
    LOG_WARN("failed to get tenant ids with snapshot version", K(ret), K(clean_info));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", K(ret), K(clean_info));
  } else if (OB_FAIL(updater.get_tenant_ids_with_snapshot_version(
                 clean_info.expired_time_, is_backup_backup, backup_set_tenant_ids))) {
    LOG_WARN("failed to get tenant ids with snapshot version", K(ret), K(clean_info));
  } else if (OB_FAIL(set_history_tenant_info_into_map(log_archive_tenant_ids, clean_tenants_map))) {
    LOG_WARN("failed to set history tenant info into map", K(ret), K(clean_info));
  } else if (OB_FAIL(set_history_tenant_info_into_map(backup_set_tenant_ids, clean_tenants_map))) {
    LOG_WARN("failed to set history tenant info into map", K(ret), K(clean_info));
  }
  return ret;
}

int ObBackupDataClean::set_history_tenant_info_into_map(const common::ObIArray<uint64_t> &tenant_ids,
    hash::ObHashMap<uint64_t, ObSimpleBackupDataCleanTenant> &clean_tenants_map)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    ObSimpleBackupDataCleanTenant tmp_simple_clean_tenant;
    const int overwrite = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      tmp_simple_clean_tenant.reset();
      tmp_simple_clean_tenant.tenant_id_ = tenant_id;
      tmp_simple_clean_tenant.is_deleted_ = true;
      if (OB_FAIL(clean_tenants_map.set_refactored(tenant_id, tmp_simple_clean_tenant, overwrite))) {
        LOG_WARN("failed to set simple clean tenant into map", K(ret), K(tmp_simple_clean_tenant));
      }
    }
  }
  return ret;
}

bool ObBackupDataClean::is_result_need_retry(const int32_t result)
{
  bool b_ret = false;
  const int64_t MAX_RETRY_COUNT = 64;
  if (OB_NOT_INIT != result && OB_INVALID_ARGUMENT != result && OB_ERR_SYS != result && OB_INIT_TWICE != result &&
      OB_ERR_UNEXPECTED != result && OB_TENANT_HAS_BEEN_DROPPED != result && OB_NOT_SUPPORTED != result &&
      OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED != result && OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED != result && 
      OB_SUCCESS != result && OB_CANCELED != result &&
      retry_count_ < MAX_RETRY_COUNT) {
    // do nothing
    // TODO() Need to consider the limit of the number of retry
    // When the upper limit is reached, it needs to actively fail to
    // The scenario where the backup medium is not used is not considered here for the time being
    retry_count_++;
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

int ObBackupDataClean::commit_trans(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  }

  tmp_ret = trans.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObBackupDataClean::start_trans(ObTimeoutCtx &timeout_ctx, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_EXECUTE_TIMEOUT_US = 600L * 1000 * 1000;  // 600s
  int64_t stmt_timeout = MAX_EXECUTE_TIMEOUT_US;
  if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  }
  return ret;
}

int ObBackupDataClean::check_can_delete_extern_info_file(const uint64_t tenant_id,
    const ObClusterBackupDest &current_backup_dest, const bool is_backup_backup, const ObBackupPath &path,
    bool &can_delete_file)
{
  int ret = OB_SUCCESS;
  can_delete_file = false;
  bool is_dropped = false;
  bool is_exist = false;
  ObStorageUtil util(false /*need retry*/);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!current_backup_dest.is_valid() || path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can delete exten info file get invalid argument", K(ret), K(path), K(current_backup_dest));
  } else if (!is_backup_backup) {
    ObClusterBackupDest tmp_backup_dest;
    if (!backup_dest_.is_valid()) {
      can_delete_file = true;
    } else if (OB_FAIL(tmp_backup_dest.set(backup_dest_, current_backup_dest.incarnation_))) {
      LOG_WARN("failed to set cluster backup dest", K(ret), K(current_backup_dest));
    } else if (current_backup_dest.is_same(tmp_backup_dest)) {
      can_delete_file = false;
    }
  } else {
    ObClusterBackupDest tmp_backup_dest;
    if (!backup_backup_dest_.is_valid()) {
      can_delete_file = true;
    } else if (OB_FAIL(tmp_backup_dest.set(backup_backup_dest_, current_backup_dest.incarnation_))) {
      LOG_WARN("failed to set cluster backup dest", K(ret), K(current_backup_dest));
    } else if (current_backup_dest.is_same(tmp_backup_dest)) {
      can_delete_file = false;
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (can_delete_file) {
    // do nothing
  } else {
    if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
      LOG_WARN("failed to check if tenant has been dropped", K(ret), K(tenant_id));
    } else {
      can_delete_file = is_dropped;
    }

    if (OB_SUCC(ret) && can_delete_file) {
      if (!is_backup_backup) {
        // do nothing
      } else if (OB_FAIL(util.is_exist(path.get_ptr(), backup_dest_.storage_info_, is_exist))) {
        LOG_WARN("failed to check file exist", K(ret), K(path));
      } else {
        can_delete_file = !is_exist;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_set_file_copies_num(const ObTenantBackupTaskInfo &task_info, int64_t &copies_num)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  ObArray<ObBackupSetFileInfo> backup_set_file_infos;
  copies_num = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup set file copies num get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init history updater", K(ret));
  } else if (OB_FAIL(updater.get_backup_set_file_info_copies(
                 task_info.tenant_id_, task_info.incarnation_, task_info.backup_set_id_, backup_set_file_infos))) {
    LOG_WARN("failed to get backup set file info copies", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_file_infos.count(); ++i) {
      const ObBackupSetFileInfo &backup_set_file_info = backup_set_file_infos.at(i);
      if (0 == backup_set_file_info.copy_id_) {
        if (ObBackupFileStatus::BACKUP_FILE_DELETED == backup_set_file_info.file_status_ ||
            OB_SUCCESS != backup_set_file_info.result_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("backup set file status is unexpected", K(ret), K(backup_set_file_info));
        } else {
          copies_num++;
        }
      } else {
        if (ObBackupFileStatus::BACKUP_FILE_COPYING == backup_set_file_info.file_status_ ||
            ObBackupFileStatus::BACKUP_FILE_INCOMPLETE == backup_set_file_info.file_status_ ||
            OB_SUCCESS != backup_set_file_info.result_ ||
            backup_set_file_info.copy_id_ >= OB_TENANT_GCONF_DEST_START_COPY_ID) {
          // do nothing
        } else {
          copies_num++;
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_piece_file_copies_num(const ObBackupPieceInfo &backup_piece_info, int64_t &copies_num)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr archive_backup_info_mgr;
  ObArray<ObBackupPieceInfo> backup_piece_infos;
  copies_num = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!backup_piece_info.is_valid() || 0 != backup_piece_info.key_.copy_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup piece file copies num get invalid argument", K(ret), K(backup_piece_info));
  } else if (OB_FAIL(archive_backup_info_mgr.get_tenant_backup_piece_infos(*sql_proxy_,
                 backup_piece_info.key_.incarnation_,
                 backup_piece_info.key_.tenant_id_,
                 backup_piece_info.key_.round_id_,
                 backup_piece_info.key_.backup_piece_id_,
                 backup_piece_infos))) {
    LOG_WARN("failed to get backup piece copy list", K(ret), K(backup_piece_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); ++i) {
      const ObBackupPieceInfo &tmp_piece_info = backup_piece_infos.at(i);
      if (0 == tmp_piece_info.key_.copy_id_) {
        if (ObBackupFileStatus::BACKUP_FILE_DELETED == tmp_piece_info.file_status_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("backup piece info is unexpected", K(ret), K(tmp_piece_info));
        } else {
          copies_num++;
        }
      } else {
        if (ObBackupFileStatus::BACKUP_FILE_COPYING == tmp_piece_info.file_status_ ||
            ObBackupFileStatus::BACKUP_FILE_INCOMPLETE == tmp_piece_info.file_status_ ||
            tmp_piece_info.key_.copy_id_ >= OB_TENANT_GCONF_DEST_START_COPY_ID ||
            tmp_piece_info.checkpoint_ts_ < backup_piece_info.checkpoint_ts_) {
          // do nothing
        } else {
          copies_num++;
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::prepare_deleted_tenant_backup_infos()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObTenantNameSimpleMgr tenant_name_mgr;
  hash::ObHashSet<uint64_t> tenant_id_set;
  ObClusterBackupDest cluster_backup_dest;
  const int64_t MAX_BUCKET = 1024;
  ObArray<ObSimpleBackupDataCleanTenant> simple_tenants;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (is_valid_backup_inner_table_version(inner_table_version_) &&
             inner_table_version_ >= OB_BACKUP_INNER_TABLE_V3) {
    // inner table version is new enough
    // do nothing
  } else if (!backup_dest_.is_valid()) {
    // do nothing
  } else if (OB_FAIL(get_all_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest_));
  } else if (OB_FAIL(tenant_id_set.create(MAX_BUCKET))) {
    LOG_WARN("failed to create tenant is set", K(ret));
  } else if (OB_FAIL(tenant_name_mgr.init())) {
    LOG_WARN("failed to init tenant name mgr", K(ret));
  } else if (OB_FAIL(tenant_name_mgr.read_backup_file(cluster_backup_dest))) {
    LOG_WARN("failed to read backup file", K(ret));
  } else if (OB_FAIL(tenant_name_mgr.get_tenant_ids(tenant_id_set))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int hash_ret = tenant_id_set.exist_refactored(tenant_id);
      if (OB_HASH_EXIST == hash_ret) {
        if (OB_FAIL(tenant_id_set.erase_refactored(tenant_id))) {
          LOG_WARN("failed to erase tenant id", K(ret), K(tenant_id));
        }
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        // do nothing
      } else {
        ret = hash_ret;
      }
      if (OB_SUCC(ret)) {
        ObSimpleBackupDataCleanTenant simple_tenant;
        simple_tenant.tenant_id_ = tenant_id;
        simple_tenant.is_deleted_ = false;
        if (OB_FAIL(simple_tenants.push_back(simple_tenant))) {
          LOG_WARN("failed to push simple tenant into array", K(ret), K(simple_tenant));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // add delete backup tenant
      for (hash::ObHashSet<uint64_t>::iterator iter = tenant_id_set.begin();
           OB_SUCC(ret) && iter != tenant_id_set.end();
           ++iter) {
        const uint64_t tenant_id = iter->first;
        ObSimpleBackupDataCleanTenant simple_tenant;
        simple_tenant.tenant_id_ = tenant_id;
        simple_tenant.is_deleted_ = true;
        if (OB_FAIL(simple_tenants.push_back(simple_tenant))) {
          LOG_WARN("failed to push simple tenant into array", K(ret), K(simple_tenant));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_backup_infos_for_compatible(cluster_backup_dest, simple_tenants))) {
        LOG_WARN("failed to add backup infos for compatible", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(add_log_archive_infos_for_compatible(cluster_backup_dest, simple_tenants))) {
        LOG_WARN("failed to add log archive infos for compatible", K(ret), K(cluster_backup_dest));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(upgrade_backup_info())) {
        LOG_WARN("failed to upgrade backup info", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_backup_infos_for_compatible(const ObClusterBackupDest &cluster_backup_dest,
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &simple_tenants)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, int64_t> min_backup_set_log_ts;
  ObBackupTaskHistoryUpdater updater;
  ObArray<ObTenantBackupTaskInfo> backup_task_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || simple_tenants.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add backup infos for compatible get invalid argument", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(min_backup_set_log_ts.create(simple_tenants.count(), ObModIds::BACKUP))) {
    LOG_WARN("failed to create min backup set log ts map", K(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init updater", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_tenant = simple_tenants.at(i);
      if (OB_SYS_TENANT_ID == simple_tenant.tenant_id_) {
        // do nothing
      } else if (OB_FAIL(add_backup_infos_for_compatible_(cluster_backup_dest, simple_tenant, min_backup_set_log_ts))) {
        LOG_WARN("failed to add backup infos for compatible", K(ret), K(simple_tenant));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(remove_delete_expired_data_snapshot_(simple_tenant))) {
        LOG_WARN("failed to remvoe delete expired data snapshot", K(ret), K(simple_tenant));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(updater.get_tenant_backup_tasks(OB_SYS_TENANT_ID, false /*is_backup_backup*/, backup_task_infos))) {
        LOG_WARN("failed to get tenant backup tasks", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < backup_task_infos.count(); ++i) {
          ObTenantBackupTaskInfo backup_task_info = backup_task_infos.at(i);
          ObBackupSetFileInfo set_file_info;
          int64_t min_replay_log_ts = 0;
          int hash_ret = min_backup_set_log_ts.get_refactored(backup_task_info.backup_set_id_, min_replay_log_ts);
          if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
            ret = hash_ret;
            LOG_WARN("failed to get min replay log ts", K(ret), K(backup_task_info));
          } else if (FALSE_IT(backup_task_info.start_replay_log_ts_ = min_replay_log_ts)) {
          } else if (OB_FAIL(set_file_info.extract_from_backup_task_info(backup_task_info))) {
            LOG_WARN("failed to extract from backup task info", K(ret), K(backup_task_info));
          } else if (OB_FAIL(
                         ObBackupSetFilesOperator::insert_tenant_backup_set_file_info(set_file_info, *sql_proxy_))) {
            LOG_WARN("failed to insert tenant backup set file info", K(ret), K(set_file_info));
          } else if (OB_FAIL(updater.insert_tenant_backup_task(backup_task_info))) {
            LOG_WARN("failedto insert tenant backup task", K(ret), K(backup_task_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_backup_infos_for_compatible_(const ObClusterBackupDest &cluster_backup_dest,
    const ObSimpleBackupDataCleanTenant &simple_tenant, hash::ObHashMap<int64_t, int64_t> &min_backup_set_log_ts)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr backup_info_mgr;
  ObArray<ObExternBackupInfo> extern_backup_infos;
  ObTenantBackupTaskInfo backup_task_info;
  ObArray<ObTenantBackupTaskInfo> backup_task_infos;
  ObBackupTaskHistoryUpdater updater;
  ObBackupSetFileInfo set_file_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !simple_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add backup infos for compatible get invalid argument", K(ret), K(cluster_backup_dest), K(simple_tenant));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init updater", K(ret));
  } else if (OB_FAIL(backup_info_mgr.init(simple_tenant.tenant_id_, cluster_backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init backup info mgr", K(ret), K(simple_tenant), K(cluster_backup_dest));
  } else if (OB_FAIL(backup_info_mgr.get_extern_backup_infos(extern_backup_infos))) {
    LOG_WARN("failed to get extern backup infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_infos.count(); ++i) {
      const ObExternBackupInfo &extern_backup_info = extern_backup_infos.at(i);
      ObTenantBackupTaskInfo tmp_backup_task_info;
      if (OB_FAIL(check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (!simple_tenant.is_deleted_ && ObExternBackupInfo::DOING == extern_backup_info.status_) {
        // do nothing
      } else if (OB_FAIL(get_backup_task_info_from_extern_info(
                     simple_tenant.tenant_id_, cluster_backup_dest, extern_backup_info, backup_task_info))) {
        LOG_WARN(
            "failed to get backup task info from extern info", K(ret), K(cluster_backup_dest), K(extern_backup_info));
      } else if (OB_FAIL(backup_task_infos.push_back(backup_task_info))) {
        LOG_WARN("failed to push backup task info into array", K(ret), K(backup_task_info));
      }
    }

    if (OB_SUCC(ret)) {
      // add inner table
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_task_infos.count(); ++i) {
        ObTenantBackupTaskInfo &task_info = backup_task_infos.at(i);
        set_file_info.reset();
        int64_t start_replay_log_ts = 0;
        if (OB_FAIL(check_can_do_task())) {
          LOG_WARN("failed to check can do task", K(ret));
        } else if (ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V1 == task_info.compatible_ &&
                   0 == task_info.start_replay_log_ts_ && OB_SUCCESS == task_info.result_) {
          if (OB_FAIL(ObRootBackup::calculate_tenant_start_replay_log_ts(
                  task_info, *backup_lease_service_, start_replay_log_ts))) {
            LOG_WARN("failed to calculate tenant start replay log ts", K(ret), K(task_info));
          } else {
            task_info.start_replay_log_ts_ = start_replay_log_ts;
            int64_t tmp_replay_log_ts = 0;
            int64_t min_relay_log_ts = 0;
            const int64_t overwrite = 1;
            int hash_ret = min_backup_set_log_ts.get_refactored(task_info.backup_set_id_, tmp_replay_log_ts);
            if (OB_SUCCESS == hash_ret) {
              min_relay_log_ts = std::min(task_info.start_replay_log_ts_, tmp_replay_log_ts);
              if (OB_FAIL(
                      min_backup_set_log_ts.set_refactored(task_info.backup_set_id_, min_relay_log_ts, overwrite))) {
                LOG_WARN("failed to set replay log ts into map", K(ret), K(task_info));
              }
            } else if (OB_HASH_NOT_EXIST == hash_ret) {
              if (OB_FAIL(min_backup_set_log_ts.set_refactored(
                      task_info.backup_set_id_, task_info.start_replay_log_ts_, overwrite))) {
                LOG_WARN("failed to set replay log ts into map", K(ret), K(task_info));
              }
            } else {
              ret = hash_ret;
              LOG_WARN("failed to get min backup set log ts", K(ret), K(hash_ret), K(task_info));
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(set_file_info.extract_from_backup_task_info(task_info))) {
          LOG_WARN("failed to extract from backup task info", K(ret), K(task_info));
        } else if (OB_FAIL(ObBackupSetFilesOperator::insert_tenant_backup_set_file_info(set_file_info, *sql_proxy_))) {
          LOG_WARN("failed to insert tenant backup set file info", K(ret), K(set_file_info));
        } else if (OB_FAIL(updater.insert_tenant_backup_task(task_info))) {
          LOG_WARN("failedto insert tenant backup task", K(ret), K(task_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_task_info_from_extern_info(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObExternBackupInfo &extern_backup_info,
    ObTenantBackupTaskInfo &backup_task_info)
{
  int ret = OB_SUCCESS;
  backup_task_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup task info from extern info get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(extern_backup_info));
  } else {
    backup_task_info.snapshot_version_ = extern_backup_info.backup_snapshot_version_;
    backup_task_info.tenant_id_ = tenant_id;
    backup_task_info.backup_data_version_ = extern_backup_info.backup_data_version_;
    backup_task_info.backup_dest_ = cluster_backup_dest.dest_;
    backup_task_info.backup_schema_version_ = extern_backup_info.backup_schema_version_;
    backup_task_info.backup_set_id_ = extern_backup_info.inc_backup_set_id_;
    backup_task_info.backup_type_.type_ = extern_backup_info.backup_type_;
    backup_task_info.cluster_id_ = GCONF.cluster_id;
    backup_task_info.cluster_version_ = extern_backup_info.cluster_version_;
    backup_task_info.compatible_ = extern_backup_info.compatible_;
    backup_task_info.copy_id_ = 0;
    backup_task_info.date_ = extern_backup_info.date_;
    backup_task_info.device_type_ = cluster_backup_dest.dest_.device_type_;
    backup_task_info.encryption_mode_ = extern_backup_info.encryption_mode_;
    backup_task_info.end_time_ = 0;
    backup_task_info.incarnation_ = cluster_backup_dest.incarnation_;
    backup_task_info.is_mark_deleted_ = extern_backup_info.is_mark_deleted_;
    backup_task_info.macro_block_count_ = 0;
    backup_task_info.passwd_ = extern_backup_info.passwd_;
    backup_task_info.prev_backup_data_version_ = extern_backup_info.prev_backup_data_version_;
    backup_task_info.prev_full_backup_set_id_ = extern_backup_info.prev_full_backup_set_id_;
    backup_task_info.prev_inc_backup_set_id_ = extern_backup_info.prev_inc_backup_set_id_;
    backup_task_info.status_ = ObTenantBackupTaskInfo::FINISH;
    if (ObExternBackupInfo::DOING == extern_backup_info.status_ ||
        ObExternBackupInfo::FAILED == extern_backup_info.status_) {
      backup_task_info.result_ = OB_TENANT_HAS_BEEN_DROPPED;
    } else {
      backup_task_info.result_ = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBackupDataClean::add_log_archive_infos_for_compatible(const ObClusterBackupDest &cluster_backup_dest,
    const common::ObIArray<ObSimpleBackupDataCleanTenant> &simple_tenants)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_tenants.count(); ++i) {
      const ObSimpleBackupDataCleanTenant &simple_tenant = simple_tenants.at(i);
      if (!simple_tenant.is_deleted_) {
        // do nothing
      } else if (OB_FAIL(add_log_archive_infos_for_compatible_(cluster_backup_dest, simple_tenant))) {
        LOG_WARN("failed to add log archive infos for compatible", K(ret), K(cluster_backup_dest));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::add_log_archive_infos_for_compatible_(
    const ObClusterBackupDest &cluster_backup_dest, const ObSimpleBackupDataCleanTenant &simple_tenant)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfo archive_info;
  ObBackupPieceInfo piece;
  ObExternLogArchiveBackupInfo extern_info;
  ObArray<ObTenantLogArchiveStatus> archive_status_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !simple_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add backup infos for compatible get invalid argument", K(ret), K(cluster_backup_dest), K(simple_tenant));
  } else if (OB_FAIL(
                 mgr.read_extern_log_archive_backup_info(cluster_backup_dest, simple_tenant.tenant_id_, extern_info))) {
    LOG_WARN("failed to read extern log archive backup info", K(ret), K(simple_tenant), K(cluster_backup_dest));
  } else if (OB_FAIL(extern_info.get_log_archive_status(archive_status_array))) {
    LOG_WARN("failed to get log archive status", K(ret), K(simple_tenant));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < archive_status_array.count(); ++i) {
      archive_info.reset();
      piece.reset();
      ObTenantLogArchiveStatus &archive_status = archive_status_array.at(i);
      archive_status.status_ = ObLogArchiveStatus::STOP;
      archive_info.status_ = archive_status;

      if (OB_FAIL(check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(cluster_backup_dest.dest_.get_backup_dest_str(
                     archive_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to get backup destr", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(archive_info.get_piece_key(piece.key_))) {
        LOG_WARN("failed to get piece key", K(ret), K(archive_info));
      } else if (OB_FAIL(piece.backup_dest_.assign(archive_info.backup_dest_))) {
        LOG_WARN("failed to copy backup dest", K(ret), K(archive_info));
      } else {
        piece.create_date_ = 0;
        piece.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
        if (archive_info.status_.is_mark_deleted_) {
          piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETING;
        } else {
          piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
        }
        piece.start_ts_ = archive_info.status_.start_ts_;
        piece.checkpoint_ts_ = archive_info.status_.checkpoint_ts_;
        piece.max_ts_ = archive_info.status_.checkpoint_ts_;
        piece.compatible_ = ObTenantLogArchiveStatus::NONE;
        piece.start_piece_id_ = 0;
        if (OB_FAIL(mgr.update_backup_piece(*sql_proxy_, piece))) {
          LOG_WARN("failed to update backup piece", K(ret), K(piece));
        } else if (OB_FAIL(mgr.update_log_archive_status_history(
                       *sql_proxy_, archive_info, inner_table_version_, *backup_lease_service_))) {
          LOG_WARN("failed to upload log archive status history", K(ret), K(archive_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::get_backup_round_copies_num(
    const ObLogArchiveBackupInfo &archive_backup_info, int64_t &copies_num)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr archive_backup_info_mgr;
  const bool for_update = false;
  copies_num = INT64_MAX;
  const bool is_backup_backup = false;
  ObArray<ObBackupPieceInfo> backup_piece_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!archive_backup_info.is_valid() || 0 != archive_backup_info.status_.copy_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup piece file copies num get invalid argument", K(ret), K(archive_backup_info));
  } else if (OB_FAIL(archive_backup_info_mgr.get_round_backup_piece_infos(*sql_proxy_,
                 for_update,
                 archive_backup_info.status_.tenant_id_,
                 archive_backup_info.status_.incarnation_,
                 archive_backup_info.status_.round_,
                 is_backup_backup,
                 backup_piece_infos))) {
    LOG_WARN("failed to get round backup piece infos", K(ret), K(archive_backup_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); ++i) {
      const ObBackupPieceInfo &piece_info = backup_piece_infos.at(i);
      int64_t tmp_copies_num = 0;
      if (OB_FAIL(get_backup_piece_file_copies_num(piece_info, tmp_copies_num))) {
        LOG_WARN("failed to get backup piece file copies num", K(ret), K(piece_info));
      } else {
        copies_num = std::min(tmp_copies_num, copies_num);
      }
    }
  }
  return ret;
}

int ObBackupDataClean::upgrade_backup_info()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObBackupInfoManager info_manager;
  common::ObAddr scheduler_leader_addr;
  int64_t backup_schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
      LOG_WARN("failed to init info manager", K(ret), K(OB_SYS_TENANT_ID));
    } else if (OB_FAIL(check_can_do_task())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(
                   ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(trans, backup_schema_version))) {
      LOG_WARN("failed to get tenant name backup schema version", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::set_inner_table_version(trans, OB_BACKUP_INNER_TABLE_V3))) {
      LOG_WARN("failed to set_inner_table_version", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::set_tenant_name_backup_schema_version(trans, backup_schema_version))) {
      LOG_WARN("failed to set tenant name backup schema version", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        inner_table_version_ = OB_BACKUP_INNER_TABLE_V3;
        FLOG_INFO("[BACKUP_CLEAN]finish upgrade_backup_info", K(ret), K(inner_table_version_));
      }
    } else {
      int tmp_ret = trans.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObBackupDataClean::check_backup_set_id_can_be_deleted(
    const uint64_t tenant_id, const ObBackupSetId &backup_set_id, bool &can_deleted)
{
  int ret = OB_SUCCESS;
  can_deleted = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!backup_set_id.is_valid() || ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check backup set id can be deleted get invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else {
    int hash_ret = sys_tenant_deleted_backup_set_.exist_refactored(backup_set_id.backup_set_id_);
    if (OB_HASH_EXIST == hash_ret) {
      can_deleted = true;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      can_deleted = false;
    } else {
      ret = hash_ret;
      LOG_WARN("failed to check backup set id can be deleted", K(ret), K(tenant_id), K(backup_set_id));
    }
  }
  return ret;
}

int ObBackupDataClean::add_deleting_backup_set_id_into_set(const uint64_t tenant_id, const ObBackupSetId &backup_set_id)
{
  int ret = OB_SUCCESS;
  char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  int trace_length = 0;
  const bool overwirte_key = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!backup_set_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add deleting set id into set get invalid argument", K(ret));
  } else if (tenant_id != OB_SYS_TENANT_ID || ObBackupDataCleanMode::CLEAN != backup_set_id.clean_mode_) {
    // do nothing
  } else if (OB_FAIL(sys_tenant_deleted_backup_set_.set_refactored_1(backup_set_id.backup_set_id_, overwirte_key))) {
    LOG_WARN("failed to set backup set id into set", K(ret), K(backup_set_id), K(tenant_id));
  } else if (FALSE_IT(trace_length =
                          ObCurTraceId::get_trace_id()->to_string(trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
  } else {
    FLOG_INFO("[BACKUP_CLEAN]succ add backup clean set when obsolete clean", K(tenant_id), K(backup_set_id));
    ROOTSERVICE_EVENT_ADD("backup_clean",
        "backup_set",
        "tenant_id",
        tenant_id,
        "backup_set_id",
        backup_set_id.backup_set_id_,
        "copy_id",
        backup_set_id.copy_id_,
        "trace_id",
        trace_id);
  }
  return ret;
}

int ObBackupDataClean::remove_delete_expired_data_snapshot_(const ObSimpleBackupDataCleanTenant &simple_tenant)
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (!simple_tenant.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove delete expired data snapshot get invalid argument", K(ret), K(simple_tenant));
  } else if (simple_tenant.is_deleted_) {
    // do nothing
  } else if (OB_FAIL(info_manager.init(simple_tenant.tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", K(ret), K(simple_tenant));
  } else if (OB_FAIL(info_manager.delete_last_delete_epxired_data_snapshot(simple_tenant.tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to delete last delete epxired data snapshot", K(ret), K(simple_tenant));
  }
  return ret;
}

int ObBackupDataClean::get_tenant_delete_piece(const share::ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanElement &clean_element, common::ObIArray<ObBackupPieceInfoKey> &backup_piece_keys)
{
  int ret = OB_SUCCESS;
  ObBackupPieceInfoKey piece_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (clean_element.log_archive_round_array_.empty() || clean_element.log_archive_round_array_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete backup piece get unexpected round infos", K(ret), K(clean_element));
  } else {
    piece_key.backup_piece_id_ = clean_info.backup_piece_id_;
    piece_key.copy_id_ = clean_element.log_archive_round_array_.at(0).copy_id_;
    piece_key.incarnation_ = clean_element.incarnation_;
    piece_key.round_id_ = clean_element.log_archive_round_array_.at(0).log_archive_round_;
    piece_key.tenant_id_ = clean_info.tenant_id_;
    if (OB_FAIL(backup_piece_keys.push_back(piece_key))) {
      LOG_WARN("failed to push backup piece key into array", K(ret), K(piece_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_SYS_TENANT_ID == clean_info.tenant_id_) {
        char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
        int trace_length = 0;
        const bool overwrite_key = true;
        ObSimplePieceKey simple_piece_key;
        simple_piece_key.backup_piece_id_ = piece_key.backup_piece_id_;
        simple_piece_key.copy_id_ = piece_key.copy_id_;
        simple_piece_key.incarnation_ = piece_key.incarnation_;
        simple_piece_key.round_id_ = piece_key.round_id_;
        if (OB_FAIL(sys_tenant_deleted_backup_piece_.set_refactored_1(simple_piece_key, overwrite_key))) {
          LOG_WARN("failed to set simple piece key", K(ret), K(simple_piece_key), K(piece_key));
        } else if (FALSE_IT(trace_length = ObCurTraceId::get_trace_id()->to_string(
                                trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
        } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
        } else {
          FLOG_INFO("[BACKUP_CLEAN]succ add backup clean piece, clean type is delete piece",
              K(simple_piece_key),
              K(clean_info));
          ROOTSERVICE_EVENT_ADD("backup_clean",
              "backup_piece",
              "tenant_id",
              clean_info.tenant_id_,
              "round_id",
              simple_piece_key.round_id_,
              "backup_piece_id",
              simple_piece_key.backup_piece_id_,
              "copy_id",
              simple_piece_key.copy_id_,
              "trace_id",
              trace_id);
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::set_comment(ObBackupCleanInfo::Comment &comment)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char comment_str[common::MAX_TABLE_COMMENT_LENGTH] = "";
  int64_t pos = 0;
  comment.reset();
  int trace_length = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (GCONF.self_addr_.ip_port_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (FALSE_IT(trace_length =
                          ObCurTraceId::get_trace_id()->to_string(trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
  } else if (OB_FAIL(databuff_printf(
                 comment_str, MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, pos, "server:%s, trace_id:%s", ip, trace_id))) {
    LOG_WARN("failed to set comment", K(ret), K(ip), K(trace_id));
  } else if (OB_FAIL(comment.assign(comment_str))) {
    LOG_WARN("failed to assign comment", K(ret), K(comment_str));
  }
  return ret;
}

int ObBackupDataClean::set_error_msg(const int32_t result, ObBackupCleanInfo::ErrorMsg &error_msg)
{
  int ret = OB_SUCCESS;
  error_msg.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_SUCCESS != result && OB_FAIL(error_msg.assign(ob_strerror(result)))) {
    LOG_WARN("failed to set comment", K(ret), K(result));
  }
  return ret;
}

int ObBackupDataClean::prepare_delete_backup_set(const ObBackupCleanInfo &sys_clean_info)
{
  int ret = OB_SUCCESS;
  const ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::BACKUP_FILE_DELETING;
  ObArray<ObBackupSetFileInfo> backup_set_file_infos;
  sys_tenant_deleted_backup_set_.reuse();
  const bool is_backup_backup = sys_clean_info.is_delete_obsolete_backup_backup();
  const ObBackupDest *backup_dest_ptr = is_backup_backup ? &backup_backup_dest_ : &backup_dest_;
  const bool overwirte_key = true;
  ObBackupDest backup_dest;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare delete backup set get invalid argument", K(ret), K(sys_clean_info));
  } else if (!sys_clean_info.is_delete_obsolete()) {
    // do nothing
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_info_with_file_status(sys_clean_info.tenant_id_,
                 OB_START_INCARNATION,
                 false /*for_update*/,
                 file_status,
                 *sql_proxy_,
                 backup_set_file_infos))) {
    LOG_WARN("failed to get backup set info with file status", KR(ret), K(sys_clean_info), K(file_status));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_file_infos.count(); ++i) {
      char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
      int trace_length = 0;
      const ObBackupSetFileInfo &backup_set_file_info = backup_set_file_infos.at(i);
      backup_dest.reset();
      if (OB_FAIL(backup_dest.set(backup_set_file_info.backup_dest_.ptr()))) {
        LOG_WARN("failed to set backup dest", K(ret), K(backup_set_file_info));
      } else if (backup_dest != *backup_dest_ptr) {
        // do nothing
      } else if ((backup_set_file_info.copy_id_ == 0 && !is_backup_backup) ||
                 (backup_set_file_info.copy_id_ > 0 && is_backup_backup)) {
        if (OB_FAIL(sys_tenant_deleted_backup_set_.set_refactored_1(backup_set_file_info.backup_set_id_, overwirte_key))) {
          LOG_WARN("failed to set backup set id into set", K(ret), K(backup_set_file_info));
        } else if (FALSE_IT(trace_length = ObCurTraceId::get_trace_id()->to_string(
                                trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
        } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
        } else {
          FLOG_INFO("[BACKUP_CLEAN]succ add backup clean set when obsolete clean",
              K(backup_set_file_info),
              K(sys_clean_info));
          ROOTSERVICE_EVENT_ADD("backup_clean",
              "backup_set",
              "tenant_id",
              sys_clean_info.tenant_id_,
              "backup_set_id",
              backup_set_file_info.backup_set_id_,
              "copy_id",
              backup_set_file_info.copy_id_,
              "trace_id",
              trace_id);
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::prepare_delete_backup_piece_and_round(const ObBackupCleanInfo &sys_clean_info)
{
  int ret = OB_SUCCESS;
  const ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::BACKUP_FILE_DELETING;
  ObLogArchiveBackupInfoMgr log_archive_info_mgr;
  ObArray<ObBackupPieceInfo> backup_piece_infos;
  ObArray<ObLogArchiveBackupInfo> archive_infos;
  const bool is_backup_backup = sys_clean_info.is_delete_obsolete_backup_backup();
  const ObBackupDest *backup_dest_ptr = is_backup_backup ? &backup_backup_dest_ : &backup_dest_;
  const bool overwrite_key = true;
  const bool for_update = false;
  ObBackupDest backup_dest;
  ObSimplePieceKey simple_piece_key;
  ObSimpleArchiveRound simple_archive_round;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (ObBackupCleanInfoStatus::DOING != sys_clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare delete backup set get invalid argument", K(ret), K(sys_clean_info));
  } else if (!sys_clean_info.is_delete_obsolete() || !backup_dest_ptr->is_valid()) {
    // do nothing
  } else if (is_backup_backup && OB_FAIL(log_archive_info_mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup", K(ret));
  } else if (OB_FAIL(log_archive_info_mgr.get_tenant_backup_piece_infos_with_file_status(*sql_proxy_,
                 OB_START_INCARNATION,
                 sys_clean_info.tenant_id_,
                 file_status,
                 is_backup_backup,
                 backup_piece_infos))) {
    LOG_WARN("failed to get tenant backup piece infos with file stauts", K(ret), K(sys_clean_info));
  } else if (OB_FAIL(log_archive_info_mgr.get_backup_log_archive_history_infos(
                 *sql_proxy_, sys_clean_info.tenant_id_, for_update, archive_infos))) {
    LOG_WARN("failed to get backup log archive history infos", K(ret), K(sys_clean_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); ++i) {
      char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
      int trace_length = 0;
      const ObBackupPieceInfo &backup_piece_info = backup_piece_infos.at(i);
      backup_dest.reset();
      simple_piece_key.reset();
      if (OB_FAIL(backup_dest.set(backup_piece_info.backup_dest_.ptr()))) {
        LOG_WARN("failed to set backup dest", K(ret), K(backup_piece_info));
      } else if (backup_dest != *backup_dest_ptr) {
        // do nothing
      } else if ((backup_piece_info.key_.copy_id_ == 0 && !is_backup_backup) ||
                 (backup_piece_info.key_.copy_id_ > 0 && is_backup_backup)) {
        simple_piece_key.backup_piece_id_ = backup_piece_info.key_.backup_piece_id_;
        simple_piece_key.copy_id_ = backup_piece_info.key_.copy_id_;
        simple_piece_key.incarnation_ = backup_piece_info.key_.incarnation_;
        simple_piece_key.round_id_ = backup_piece_info.key_.round_id_;
        if (OB_FAIL(sys_tenant_deleted_backup_piece_.set_refactored_1(simple_piece_key, overwrite_key))) {
          LOG_WARN("failed to set tenant deleted backup piece", K(ret), K(simple_piece_key));
        } else if (FALSE_IT(trace_length = ObCurTraceId::get_trace_id()->to_string(
                                trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
        } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
        } else {
          FLOG_INFO("[BACKUP_CLEAN]succ add backup clean piece when auto obsolete clean",
              K(simple_piece_key),
              K(sys_clean_info));
          ROOTSERVICE_EVENT_ADD("backup_clean",
              "backup_piece",
              "tenant_id",
              sys_clean_info.tenant_id_,
              "round_id",
              simple_piece_key.round_id_,
              "backup_piece_id",
              simple_piece_key.backup_piece_id_,
              "copy_id",
              simple_piece_key.copy_id_,
              "trace_id",
              trace_id);
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < archive_infos.count(); ++i) {
      char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
      int trace_length = 0;
      const ObLogArchiveBackupInfo &archive_info = archive_infos.at(i);
      backup_dest.reset();
      simple_archive_round.reset();
      if (OB_FAIL(backup_dest.set(archive_info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", K(ret), K(archive_info));
      } else if (backup_dest != *backup_dest_ptr || !archive_info.status_.is_mark_deleted_) {
        // do nothing
      } else if ((archive_info.status_.copy_id_ == 0 && !is_backup_backup) ||
                 (archive_info.status_.copy_id_ > 0 && is_backup_backup)) {
        simple_archive_round.copy_id_ = archive_info.status_.copy_id_;
        simple_archive_round.incarnation_ = archive_info.status_.incarnation_;
        simple_archive_round.round_id_ = archive_info.status_.round_;
        if (OB_FAIL(sys_tenant_deleted_backup_round_.set_refactored_1(simple_archive_round, overwrite_key))) {
          LOG_WARN("failed to set tenant deleted backup round", K(ret), K(simple_archive_round));
        } else if (FALSE_IT(trace_length = ObCurTraceId::get_trace_id()->to_string(
                                trace_id, common::OB_MAX_TRACE_ID_BUFFER_SIZE))) {
        } else if (trace_length > OB_MAX_TRACE_ID_BUFFER_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get trace id", K(ret), K(*ObCurTraceId::get_trace_id()));
        } else {
          FLOG_INFO("[BACKUP_CLEAN]succ add backup clean round when auto obsolete clean",
              K(simple_archive_round),
              K(sys_clean_info));
          ROOTSERVICE_EVENT_ADD("backup_clean",
              "backup_round",
              "tenant_id",
              sys_clean_info.tenant_id_,
              "round_id",
              simple_archive_round.round_id_,
              "copy_id",
              simple_archive_round.copy_id_,
              "trace_id",
              trace_id);
        }
      }
    }
  }
  return ret;
}

int ObBackupDataClean::duplicate_task_info(common::ObIArray<share::ObTenantBackupTaskInfo> &task_infos)
{
  int ret = OB_SUCCESS;
  int64_t step = 0;
  ObHashSet<ObTenantBackupTaskItem> task_infos_set;
  ObArray<ObTenantBackupTaskItem> tmp_task_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (task_infos.empty()) {
    // do nothing
  } else if (OB_FAIL(task_infos_set.create(task_infos.count()))) {
    LOG_WARN("failed to create task info set", K(ret));
  } else {
    // remove same tasks
    for (int64_t i = task_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObTenantBackupTaskInfo &tmp_info = task_infos.at(i);
      int hash_ret = task_infos_set.exist_refactored(tmp_info);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(task_infos_set.set_refactored(tmp_info))) {
          LOG_WARN("failed to set task info into set", K(ret), K(tmp_info));
        } else if (OB_FAIL(tmp_task_infos.push_back(tmp_info))) {
          LOG_WARN("failed to push tmp info into array", K(ret), K(tmp_info));
        }
      } else if (OB_HASH_EXIST == hash_ret) {
        // do nothing
      } else {
        ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
      }
    }

    if (OB_SUCC(ret)) {
      CompareBackupTaskInfo backup_task_cmp;
      std::sort(tmp_task_infos.begin(), tmp_task_infos.end(), backup_task_cmp);
      if (OB_FAIL(task_infos.assign(tmp_task_infos))) {
        LOG_WARN("failed to assign task infos", K(ret), K(tmp_task_infos));
      }
    }
  }
  return ret;
}

int ObBackupDataClean::force_cancel(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCancelDeleteBackupScheduler cancel_delete_backup_scheduler;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean do not init", K(ret));
  } else if (OB_FAIL(cancel_delete_backup_scheduler.init(tenant_id, *sql_proxy_, this))) {
    LOG_WARN("failed to init backup data clean scheduler", K(ret), K(tenant_id));
  } else if (OB_FAIL(cancel_delete_backup_scheduler.start_schedule_cacel_delete_backup())) {
    LOG_WARN("failed to start schedule backup data clean", K(ret), K(tenant_id));
  }
  FLOG_WARN("force_cancel backup data clean", K(ret), K(tenant_id));
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
