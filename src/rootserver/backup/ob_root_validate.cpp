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
#include "rootserver/backup/ob_root_validate.h"
#include "rootserver/backup/ob_backup_data_mgr.h"
#include "share/ob_rpc_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace rootserver {

int64_t ObRootValidateIdling::get_idle_interval_us()
{
  const int64_t validate_idle_time = GCONF._backup_idle_time;
  return validate_idle_time;
}

ObRootValidate::ObRootValidate()
    : is_inited_(false),
      is_working_(false),
      is_prepare_flag_(false),
      backup_dest_(),
      config_(NULL),
      sql_proxy_(NULL),
      root_balancer_(NULL),
      server_mgr_(NULL),
      rebalance_mgr_(NULL),
      rpc_proxy_(NULL),
      idling_(stop_)
{}

ObRootValidate::~ObRootValidate()
{}

int ObRootValidate::init(common::ObServerConfig& config, common::ObMySQLProxy& sql_proxy, ObRootBalancer& root_balancer,
    ObServerManager& server_manager, ObRebalanceTaskMgr& rebalance_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int root_validate_thread_cnt = 1;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "root validate init twice", K(ret));
  } else if (OB_FAIL(create(root_validate_thread_cnt, "RootValidate"))) {
    LOG_WARN("create thread failed", K(ret), K(root_validate_thread_cnt));
  } else if (OB_FAIL(task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init backup validate task updater", K(ret));
  } else {
    config_ = &config;
    sql_proxy_ = &sql_proxy;
    root_balancer_ = &root_balancer;
    server_mgr_ = &server_manager;
    rebalance_mgr_ = &rebalance_mgr;
    rpc_proxy_ = &rpc_proxy;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
    LOG_INFO("root validate init success");
  }
  return ret;
}

int ObRootValidate::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("not init", K(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

int ObRootValidate::idle()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } else {
    LOG_INFO("root validate idle", "idle_time", idling_.get_idle_interval_us());
  }
  return ret;
}

void ObRootValidate::wakeup()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObRootValidate do not init");
  } else {
    idling_.wakeup();
    LOG_INFO("root validate wakeup");
  }
}

void ObRootValidate::stop()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObRootValidate do not init");
  } else {
    ObReentrantThread::stop();
    idling_.wakeup();
  }
}

void ObRootValidate::run3()
{
  int ret = OB_SUCCESS;
  bool can_do_work = false;
  const int64_t CHECK_TASK_STATUS_INTERVAL = 10 * 1000 * 1000;  // 10s
  common::ObArray<ObBackupValidateTaskInfo> tasks;
  ObCurTraceId::init(GCONF.self_addr_);

  while (!stop_) {
    ret = OB_SUCCESS;
    can_do_work = false;
    tasks.reset();

    // TODO: timeout task
    if (OB_FAIL(check_can_do_work(can_do_work))) {
      LOG_WARN("failed to check can do work", K(ret));
    } else if (!can_do_work) {
      LOG_WARN("rs leader may switch", K(ret));
    } else if (OB_FAIL(get_need_validate_tasks(tasks))) {
      LOG_WARN("failed to get need validate tenant ids", K(ret));
    } else if (tasks.empty()) {
      LOG_INFO("no validation job need to schedule", K(ret));
    } else if (OB_FAIL(do_root_scheduler(tasks))) {
      LOG_WARN("failed to do root scheduler", K(ret));
    }

    if (REACH_TIME_INTERVAL(CHECK_TASK_STATUS_INTERVAL)) {
      LOG_INFO("root validate check task is working");
    }

    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", K(ret));
    } else {
      continue;
    }
  }
}

int ObRootValidate::check_can_do_work(bool& can)
{
  int ret = OB_SUCCESS;
  can = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(backup_lease_service_->get_lease_status(can))) {
    LOG_WARN("failed to check can backup", K(ret));
  }
  return ret;
}

int ObRootValidate::check_tenant_has_been_dropped(uint64_t tenant_id, bool& dropped)
{
  int ret = OB_SUCCESS;
  dropped = false;
  schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, dropped))) {
    LOG_WARN("failed to check if tenant has been dropped", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootValidate::get_all_validate_tenants(
    const share::ObBackupValidateTaskInfo& task_info, common::ObIArray<ObBackupValidateTenant>& validate_tenants)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  validate_tenants.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get all validate tenants get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_all_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (OB_FAIL(validate_tenants.reserve(tenant_ids.count()))) {
    LOG_WARN("failed to reserve for validate tenants", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      bool dropped = false;
      const uint64_t tenant_id = tenant_ids.at(i);
      ObBackupValidateTenant validate_tenant;
      if (OB_SYS_TENANT_ID == tenant_id) {
        // do nothing
      } else if (OB_FAIL(check_tenant_has_been_dropped(tenant_id, dropped))) {
        LOG_WARN("failed to check tenant has been dropped", K(ret), K(tenant_id));
      } else if (FALSE_IT(validate_tenant.tenant_id_ = tenant_id)) {
        // set tenant id
      } else if (FALSE_IT(validate_tenant.is_dropped_ = dropped)) {
        // set tenant is dropped
      } else if (OB_FAIL(validate_tenants.push_back(validate_tenant))) {
        LOG_WARN("failed to push back validate tenant", K(ret), K(validate_tenant));
      }
    }
  }
  return ret;
}

int ObRootValidate::get_tenant_ids(
    const share::ObBackupValidateTaskInfo& task_info, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant_ids;
  ObTenantValidateTaskUpdater tenant_updater;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (OB_FAIL(tenant_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant_ids.count(); ++i) {
      ObTenantValidateTaskInfo tenant_task_info;
      const uint64_t tenant_id = all_tenant_ids.at(i);
      bool is_tenant_dropped = false;
      if (OB_SYS_TENANT_ID == tenant_id) {
        // do nothing
      } else if (OB_FAIL(check_tenant_has_been_dropped(tenant_id, is_tenant_dropped))) {
        LOG_WARN("failed to check tenant has been dropped", K(ret), K(tenant_id));
      } else if (FALSE_IT(tenant_updater.set_dropped_tenant(is_tenant_dropped))) {
        // set dropped tenant
      } else if (OB_FAIL(tenant_updater.get_task(task_info.job_id_,
                     tenant_id,
                     task_info.incarnation_,
                     task_info.backup_set_id_,
                     tenant_task_info))) {
        LOG_WARN("failed to get tenant task info", K(ret));
      } else if (OB_FAIL(tenant_ids.push_back(tenant_task_info.tenant_id_))) {
        LOG_WARN("failed to push back tenant ids", K(ret));
      }
    }
  }
  return ret;
}

int ObRootValidate::get_all_tenant_ids(common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObLogArchiveBackupInfo log_archive_info;
  ObLogArchiveBackupInfoMgr log_archive_mgr;
  ObBackupTaskHistoryUpdater backup_history_updater;
  ObArray<ObTenantBackupTaskInfo> tenant_backup_tasks;
  bool for_update = false;
  int64_t start_ts = 0;
  int64_t checkpoint_ts = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("validate scheduler do not init", K(ret));
  } else if (OB_FAIL(log_archive_mgr.get_log_archive_backup_info(
                 *sql_proxy_, for_update, OB_SYS_TENANT_ID, log_archive_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (OB_FAIL(backup_history_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup history updater", K(ret));
  } else if (FALSE_IT(start_ts = log_archive_info.status_.start_ts_)) {
    // do nothing
  } else if (FALSE_IT(checkpoint_ts = log_archive_info.status_.checkpoint_ts_)) {
    // do nothing
  } else if (OB_FAIL(backup_history_updater.get_all_tenant_backup_tasks_in_time_range(
                 start_ts, checkpoint_ts, tenant_backup_tasks))) {
    LOG_WARN("failed to get all tenant backup tasks in time range", K(start_ts), K(checkpoint_ts), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_backup_tasks.count(); ++i) {
      bool exists = false;
      const ObTenantBackupTaskInfo& tenant_backup_info = tenant_backup_tasks.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < tenant_ids.count(); ++j) {
        if (tenant_backup_info.tenant_id_ == tenant_ids.at(j)) {
          exists = true;
        }
      }
      if (OB_SUCC(ret) && !exists) {
        if (OB_FAIL(tenant_ids.push_back(tenant_backup_info.tenant_id_))) {
          LOG_WARN("failed to push back tenant id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootValidate::get_need_validate_tasks(common::ObIArray<share::ObBackupValidateTaskInfo>& tasks)
{
  int ret = OB_SUCCESS;
  tasks.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(task_updater_.get_all_tasks(tasks))) {
    LOG_WARN("failed to get all tasks", K(ret));
  }
  return ret;
}

int ObRootValidate::do_root_scheduler(const common::ObIArray<share::ObBackupValidateTaskInfo>& tasks)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (tasks.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObBackupValidateTaskInfo& task = tasks.at(i);
      if (OB_SUCCESS != (tmp_ret = do_scheduler(task))) {
        LOG_WARN("failed to do tenant scheduler", K(ret), K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObRootValidate::do_scheduler(const ObBackupValidateTaskInfo& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("root server shut down", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant scheduler get invalid argument", K(ret), K(task));
  } else if (OB_FAIL(do_with_status(task))) {
    LOG_WARN("failed to do with status", K(ret));
  }
  return ret;
}

int ObRootValidate::do_with_status(const ObBackupValidateTaskInfo& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("root service shutdown", K(ret));
  } else {
    switch (task.status_) {
      case ObBackupValidateTaskInfo::SCHEDULE: {
        if (OB_FAIL(do_schedule(task))) {
          LOG_WARN("failed to do schedule", K(ret));
        } else {
          LOG_INFO("root validate do schedule success", K(ret));
        }
        break;
      }
      case ObBackupValidateTaskInfo::DOING: {
        if (OB_FAIL(do_validate(task))) {
          LOG_WARN("failed to do validate", K(ret));
        } else {
          LOG_INFO("root validate do validate success", K(ret));
        }
        break;
      }
      case ObBackupValidateTaskInfo::FINISHED: {
        if (OB_FAIL(do_finish(task))) {
          LOG_WARN("failed to do finish", K(ret));
        } else {
          LOG_INFO("root validate do finish success", K(ret));
        }
        break;
      }
      case ObBackupValidateTaskInfo::CANCEL: {
        if (OB_FAIL(do_cancel(task))) {
          LOG_WARN("failed to do cancel", K(ret));
        } else {
          LOG_INFO("root validate do cancel success", K(ret));
        }
        break;
      }
      case ObBackupValidateTaskInfo::MAX:
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("validate status is invalid", K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObRootValidate::do_schedule(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(do_sys_tenant_schedule(task_info))) {
        LOG_WARN("failed to do sys tenant schedule", K(ret), K(tenant_id));
      }
    } else {
      if (OB_FAIL(do_normal_tenant_schedule(task_info))) {
        LOG_WARN("failed to do normal tenant schedule", K(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      ObBackupValidateTaskInfo cur_task_info;
      ObBackupValidateTaskUpdater task_updater;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(task_updater.init(trans))) {
          LOG_WARN("failed to init backup validate task updater", K(ret));
        } else if (OB_FAIL(task_updater.get_task(task_info.job_id_, cur_task_info))) {
          LOG_WARN("failed to get current task info", K(ret), K(task_info));
        } else if (OB_FAIL(ObBackupValidateTaskInfo::CANCEL == cur_task_info.status_ ||
                           ObBackupValidateTaskInfo::FINISHED == cur_task_info.status_)) {
          // already cancelled or finished
        } else {
          ObBackupValidateTaskInfo update_task_info = cur_task_info;
          update_task_info.status_ = ObBackupValidateTaskInfo::DOING;
          if (OB_FAIL(task_updater.update_task(cur_task_info, update_task_info))) {
            LOG_WARN("failed to update tenant validate info", K(ret));
          } else {
            wakeup();
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*commit*/))) {
            LOG_WARN("failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRootValidate::do_sys_tenant_schedule(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupTaskInfo> tenant_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("validate scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != task_info.tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schedule sys tenant validate get invalid argument", K(ret));
  } else if (OB_FAIL(get_all_tenant_backup_tasks(tenant_infos))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (OB_FAIL(schedule_tenants_validate(task_info, tenant_infos))) {
    LOG_WARN("failed to schedule tenants validate");
  }
  return ret;
}

int ObRootValidate::do_normal_tenant_schedule(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(task_info);
  return ret;
}

int ObRootValidate::do_validate(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(do_sys_tenant_validate(task_info))) {
        LOG_WARN("failed to do sys tenant validate", K(ret), K(tenant_id));
      } else {
        LOG_INFO("do sys tenant validate success", K(ret), K(tenant_id));
      }
    } else {
      if (OB_FAIL(do_normal_tenant_validate(task_info))) {
        LOG_WARN("failed to do normal tenant validate", K(ret), K(tenant_id));
      } else {
        LOG_INFO("do normal tenant validate success", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObRootValidate::do_sys_tenant_validate(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool all_finished = true;
  bool one_finished = false;
  ObArray<ObBackupValidateTenant> validate_tenants;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root validate get invalid argument", K(ret));
  } else if (OB_FAIL(get_all_validate_tenants(task_info, validate_tenants))) {
    LOG_WARN("failed to get all validate tenants", K(ret));
  } else if (validate_tenants.empty()) {
    LOG_INFO("tenant id empty, skip validation", K(ret));
  } else {
    int total_macro_block_count = 0;
    int finish_macro_block_count = 0;
    for (int64_t i = 0; i < validate_tenants.count(); ++i) {
      const ObBackupValidateTenant& validate_tenant = validate_tenants.at(i);
      ObTenantValidateTaskInfo tenant_info;
      if (OB_SUCCESS != (tmp_ret = do_tenant_scheduler(validate_tenant, task_info, tenant_info))) {
        LOG_WARN("failed to do tenant scheduler", K(ret), K(validate_tenant), K(task_info), K(tenant_info));
      } else {
        if (ObTenantValidateTaskInfo::FINISHED != tenant_info.status_) {
          all_finished = false;
        } else {
          one_finished = true;
        }
        total_macro_block_count += tenant_info.total_macro_block_count_;
        finish_macro_block_count += tenant_info.finish_macro_block_count_;
      }
      // Record the error code of the first error
      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      ObBackupValidateTaskInfo cur_task_info;
      ObBackupValidateTaskUpdater task_updater;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(task_updater.init(trans))) {
          LOG_WARN("failed to init backup validate task", K(ret));
        } else if (OB_FAIL(task_updater.get_task(task_info.job_id_, cur_task_info))) {
          LOG_WARN("failed to get current task", K(ret), K(task_info));
        } else if (ObBackupValidateTaskInfo::CANCEL == cur_task_info.status_ ||
                   ObBackupValidateTaskInfo::FINISHED == cur_task_info.status_) {
          // already cancelled or finished, nothing to do
        } else {
          share::ObBackupValidateTaskInfo update_info = cur_task_info;
          if (all_finished) {
            update_info.status_ = ObBackupValidateTaskInfo::FINISHED;
            update_info.progress_percent_ = 100;
          } else {
            update_info.status_ = ObBackupValidateTaskInfo::DOING;
            if (0 != total_macro_block_count) {
              update_info.progress_percent_ = finish_macro_block_count * 100 / total_macro_block_count;
            }
          }
          if (OB_FAIL(task_updater.update_task(cur_task_info, update_info))) {
            LOG_WARN("failed to update backup validate task info", K(ret));
          } else {
            if (one_finished) {
              wakeup();
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*commit*/))) {
            LOG_WARN("failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRootValidate::do_normal_tenant_validate(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const bool tenant_has_been_dropped = false;
  ObTenantValidateTaskInfo tenant_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root validate get invalid argument", K(ret));
  } else if (OB_FAIL(get_tenant_validate_task(task_info.job_id_,
                 task_info.tenant_id_,
                 task_info.incarnation_,
                 task_info.backup_set_id_,
                 tenant_info))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  } else {
    ObTenantValidate tenant_validate;
    if (ObTenantValidateTaskInfo::FINISHED == tenant_info.status_) {
      share::ObBackupValidateTaskInfo update_info = task_info;
      update_info.status_ = ObBackupValidateTaskInfo::FINISHED;
      if (OB_FAIL(task_updater_.update_task(task_info, update_info))) {
        LOG_WARN("failed to update backup validate task info", K(ret));
      } else {
        wakeup();
      }
    } else {
      if (OB_FAIL(tenant_validate.init(tenant_has_been_dropped,
              task_info.job_id_,
              task_info.tenant_id_,
              task_info.backup_set_id_,
              *this,
              *root_balancer_,
              *rebalance_mgr_,
              *server_mgr_,
              *sql_proxy_,
              *rpc_proxy_,
              *backup_lease_service_))) {
        LOG_WARN("failed to init tenant validate", K(ret));
      } else if (OB_FAIL(tenant_validate.do_scheduler())) {
        LOG_WARN("failed to do tenant scheduler", K(ret));
      }
    }
  }
  return ret;
}

int ObRootValidate::do_finish(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(do_sys_tenant_finish(task_info))) {
        LOG_WARN("failed to do sys tenant validate");
      }
    } else {
      if (OB_FAIL(do_normal_tenant_finish(task_info))) {
        LOG_WARN("failed to do normal tenant validate");
      }
    }
  }
  return ret;
}

int ObRootValidate::do_sys_tenant_finish(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObArray<ObBackupValidateTenant> validate_tenants;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!task_info.is_valid() || ObBackupValidateTaskInfo::FINISHED != task_info.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(ret));
  } else if (OB_FAIL(get_all_validate_tenants(task_info, validate_tenants))) {
    LOG_WARN("failed to get all validate tenants", K(ret), K(task_info));
  } else if (OB_FAIL(move_validate_task_to_his(validate_tenants, task_info))) {
    LOG_WARN("failed to move validate task to his", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < validate_tenants.count(); ++i) {
      const ObBackupValidateTenant& tenant = validate_tenants.at(i);
      ObPGValidateTaskUpdater pg_updater;
      if (OB_FAIL(pg_updater.init(*sql_proxy_, tenant.is_dropped_))) {
        LOG_WARN("failed to init pg validate task updater", K(ret), K(tenant));
      } else if (OB_FAIL(pg_updater.delete_all_pg_tasks(task_info.job_id_, tenant.tenant_id_))) {
        LOG_WARN("failed to delete all pg tasks", K(ret), K(task_info));
      }
    }
    LOG_INFO("do sys tenant finish", K(ret), K(task_info));
  }
  return ret;
}

int ObRootValidate::do_normal_tenant_finish(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!task_info.is_valid() || ObBackupValidateTaskInfo::FINISHED != task_info.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(ret));
  }
  return ret;
}

int ObRootValidate::do_cancel(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do cancel get invalid argument", K(ret));
  } else {
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(do_sys_tenant_cancel(task_info))) {
        LOG_WARN("failed to do sys tenant cancel", K(ret));
      }
    } else {
      if (OB_FAIL(do_normal_tenant_cancel(task_info))) {
        LOG_WARN("failed to do normal tenant cancel", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObBackupValidateTaskInfo update_task_info = task_info;
      update_task_info.status_ = ObBackupValidateTaskInfo::FINISHED;
      if (OB_FAIL(update_validate_task(task_info, update_task_info))) {
        LOG_WARN("failed to update tenant validate info", K(ret));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObRootValidate::do_sys_tenant_cancel(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupValidateTenant> validate_tenants;
  ObArray<ObTenantValidateTaskInfo> tenant_task_infos;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(get_all_validate_tenants(task_info, validate_tenants))) {
    LOG_WARN("failed to get all validate tenants", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < validate_tenants.count(); ++i) {
      ObTenantValidate tenant_validate;
      ObTenantValidateTaskInfo tenant_task_info;
      ObTenantValidateTaskInfo update_tenant_task_info;
      ObTenantValidateTaskUpdater tenant_updater;
      const ObBackupValidateTenant& validate_tenant = validate_tenants.at(i);
      if (OB_FAIL(tenant_updater.init(*sql_proxy_, validate_tenant.is_dropped_))) {
        LOG_WARN("failed to init tenant validate task updater", K(ret));
      } else if (OB_FAIL(tenant_updater.get_task(
                     task_info.job_id_, validate_tenant.tenant_id_, OB_START_INCARNATION, tenant_task_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("this tenant may not have been scheduled before", K(ret), K(task_info));
          ret = OB_SUCCESS;
        }
      } else if (ObTenantValidateTaskInfo::FINISHED == tenant_task_info.status_) {
        // do nothing, if already cancelled or finished
      } else if (FALSE_IT(update_tenant_task_info = tenant_task_info)) {
      } else if (FALSE_IT(update_tenant_task_info.status_ = ObTenantValidateTaskInfo::CANCEL)) {
      } else if (OB_FAIL(tenant_updater.update_task(tenant_task_info, update_tenant_task_info))) {
        LOG_WARN("failed to update tennat task info", K(ret));
      } else if (OB_FAIL(tenant_validate.init(validate_tenant.is_dropped_,
                     task_info.job_id_,
                     validate_tenant.tenant_id_,
                     task_info.backup_set_id_,
                     *this,
                     *root_balancer_,
                     *rebalance_mgr_,
                     *server_mgr_,
                     *sql_proxy_,
                     *rpc_proxy_,
                     *backup_lease_service_))) {
        LOG_WARN("failed to init tenant validate", K(ret));
      } else if (OB_FAIL(tenant_validate.do_scheduler())) {
        LOG_WARN("failed to do tenant scheduler", K(ret));
      }
    }
  }
  return ret;
}

int ObRootValidate::do_normal_tenant_cancel(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantValidate tenant_validate;
  ObTenantValidateTaskInfo tenant_task_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(get_tenant_validate_task(task_info.job_id_,
                 task_info.tenant_id_,
                 task_info.incarnation_,
                 task_info.backup_set_id_,
                 tenant_task_info))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  } else if (!tenant_task_info.is_valid() || ObTenantValidateTaskInfo::FINISHED == tenant_task_info.status_) {
    wakeup();
  } else if (ObTenantValidateTaskInfo::CANCEL == tenant_task_info.status_) {
    // do nothing
  } else {
    ObTenantValidateTaskUpdater tenant_updater;
    ObTenantValidateTaskInfo update_task = tenant_task_info;
    update_task.status_ = ObTenantValidateTaskInfo::CANCEL;
    if (ObTenantValidateTaskInfo::CANCEL == tenant_task_info.status_) {
      // do nothing
    } else if (ObTenantValidateTaskInfo::FINISHED == tenant_task_info.status_) {
      wakeup();
    } else if (OB_FAIL(tenant_updater.init(*sql_proxy_))) {
      LOG_WARN("failed to init tenant validate task updater", K(ret));
    } else if (OB_FAIL(tenant_updater.update_task(tenant_task_info, update_task))) {
      LOG_WARN("failed to update tenant validate task", K(ret));
    }
  }
  return ret;
}

int ObRootValidate::schedule_tenants_validate(const share::ObBackupValidateTaskInfo& task_info,
    const common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_backup_tasks.count(); ++i) {
      const ObTenantBackupTaskInfo& tenant_info = tenant_backup_tasks.at(i);
      bool is_dropped_tenant = false;
      if (OB_SYS_TENANT_ID == tenant_info.tenant_id_) {
        // skip
      } else if (OB_FAIL(check_tenant_has_been_dropped(tenant_info.tenant_id_, is_dropped_tenant))) {
        LOG_WARN("failed to check if tenant has been dropped", K(ret), K(tenant_info));
      } else if (OB_ALL_BACKUP_SET_ID == task_info.backup_set_id_ ||
                 task_info.backup_set_id_ == tenant_info.backup_set_id_) {
        if (OB_FAIL(schedule_tenant_validate(task_info, tenant_info, i, is_dropped_tenant))) {
          LOG_WARN("failed to schedule tenant validate", K(ret), K(tenant_info));
        }
      }
    }
  }
  return ret;
}

int ObRootValidate::do_tenant_scheduler(const ObBackupValidateTenant& validate_tenant,
    const ObBackupValidateTaskInfo& task_info, ObTenantValidateTaskInfo& tenant_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = validate_tenant.tenant_id_;
  bool is_dropped = validate_tenant.is_dropped_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant scheduler get invalid argument", K(ret), K(task_info));
  } else {
    if (is_dropped) {
      if (OB_FAIL(do_tenant_scheduler_when_dropped(validate_tenant, task_info))) {
        LOG_WARN("failed to do tenant scheduler when dropped", K(ret), K(validate_tenant));
      }
    } else {
      if (OB_FAIL(do_tenant_scheduler_when_not_dropped(validate_tenant, task_info))) {
        LOG_WARN("failed to do tenant scheduler when not dropped", K(ret), K(validate_tenant));
      }
    }
    if (OB_SUCC(ret)) {
      ObTenantValidateTaskUpdater tenant_updater;
      if (OB_FAIL(tenant_updater.init(*sql_proxy_, is_dropped))) {
        LOG_WARN("failed to init tenant validate", K(ret));
      } else if (OB_FAIL(tenant_updater.get_task(
                     task_info.job_id_, tenant_id, OB_START_INCARNATION, task_info.backup_set_id_, tenant_info))) {
        LOG_WARN("failed to get task", K(ret), K(task_info));
      }
    }
  }
  return ret;
}

int ObRootValidate::do_tenant_scheduler_when_dropped(
    const ObBackupValidateTenant& backup_validate_tenant, const ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskUpdater tenant_updater;
  ObTenantValidate tenant_validate;
  const uint64_t tenant_id = backup_validate_tenant.tenant_id_;
  const bool is_dropped = backup_validate_tenant.is_dropped_;
  ObTenantValidateTaskInfo tenant_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!is_dropped) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant should be dropped", K(ret), K(is_dropped));
  } else if (OB_FAIL(tenant_updater.init(*sql_proxy_, is_dropped))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(tenant_updater.get_task(
                 task_info.job_id_, tenant_id, OB_START_INCARNATION, task_info.backup_set_id_, tenant_info))) {
    ObArray<ObTenantBackupTaskInfo> backup_tasks;
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_ALL_BACKUP_SET_ID == task_info.backup_set_id_) {
        if (OB_FAIL(get_tenant_backup_tasks(tenant_id, backup_tasks))) {
          LOG_WARN("failed to get tenant backup tasks", K(ret), K(tenant_id));
        }
      } else {
        if (OB_FAIL(get_tenant_backup_task(tenant_id, task_info.backup_set_id_, backup_tasks))) {
          LOG_WARN("failed to get tenant backup task", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_tasks.count(); ++i) {
        const ObTenantBackupTaskInfo& backup_task = backup_tasks.at(i);
        if (OB_FAIL(schedule_tenant_validate(task_info, backup_task, i, is_dropped))) {
          LOG_WARN("failed to schedule dropped tenant validate", K(ret), K(task_info), K(backup_task), K(is_dropped));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_validate.init(is_dropped,
            task_info.job_id_,
            tenant_id,
            task_info.backup_set_id_,
            *this,
            *root_balancer_,
            *rebalance_mgr_,
            *server_mgr_,
            *sql_proxy_,
            *rpc_proxy_,
            *backup_lease_service_))) {
      LOG_WARN("failed to init tenant validate", K(ret));
    } else if (OB_FAIL(tenant_validate.do_scheduler())) {
      LOG_WARN("failed to do tenant scheduler", K(ret), K(tenant_info));
    } else {
      LOG_INFO("do dropped tenant scheduler success", K(ret), K(tenant_info));
    }
  }
  return ret;
}

int ObRootValidate::do_tenant_scheduler_when_not_dropped(
    const ObBackupValidateTenant& backup_validate_tenant, const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskUpdater tenant_updater;
  ObTenantValidate tenant_validate;
  ObTenantValidateTaskInfo tenant_info;
  const uint64_t tenant_id = backup_validate_tenant.tenant_id_;
  const bool is_dropped = backup_validate_tenant.is_dropped_;
  if (OB_FAIL(tenant_updater.init(*sql_proxy_, is_dropped))) {
    LOG_WARN("failed to init tenant task updater", K(ret));
  } else if (OB_FAIL(tenant_updater.get_task(
                 task_info.job_id_, tenant_id, OB_START_INCARNATION, task_info.backup_set_id_, tenant_info))) {
    LOG_WARN("failed to get tenant validate task info", K(ret));
  } else if (OB_FAIL(tenant_validate.init(is_dropped,
                 task_info.job_id_,
                 tenant_id,
                 task_info.backup_set_id_,
                 *this,
                 *root_balancer_,
                 *rebalance_mgr_,
                 *server_mgr_,
                 *sql_proxy_,
                 *rpc_proxy_,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init tenant validate", K(ret));
  } else if (OB_FAIL(tenant_validate.do_scheduler())) {
    LOG_WARN("failed to do tenant scheduler", K(ret), K(tenant_info));
  } else {
    LOG_INFO("do tenant scheduler success", K(ret), K(tenant_info));
  }
  return ret;
}

int ObRootValidate::schedule_tenant_validate(const share::ObBackupValidateTaskInfo& task_info,
    const ObTenantBackupTaskInfo& tenant_backup_task, const int64_t task_id, bool is_dropped_tenant)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskUpdater tenant_task_updater;
  ObTenantValidateTaskInfo tenant_task_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!tenant_backup_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule tenant validate get invalid argument", K(ret), K(tenant_backup_task));
  } else if (OB_FAIL(tenant_task_updater.init(*sql_proxy_, is_dropped_tenant))) {
    LOG_WARN("failed to init tenant task updater", K(ret));
  } else if (OB_FAIL(tenant_backup_task.backup_dest_.get_backup_dest_str(
                 tenant_task_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest", K(ret));
  } else {
    tenant_task_info.job_id_ = task_info.job_id_;
    tenant_task_info.task_id_ = task_id;
    tenant_task_info.tenant_id_ = tenant_backup_task.tenant_id_;
    tenant_task_info.incarnation_ = tenant_backup_task.incarnation_;
    tenant_task_info.backup_set_id_ = task_info.backup_set_id_;
    tenant_task_info.status_ = ObTenantValidateTaskInfo::SCHEDULE;
    tenant_task_info.start_time_ = ObTimeUtility::current_time();
    tenant_task_info.total_pg_count_ = tenant_backup_task.pg_count_;
    tenant_task_info.finish_pg_count_ = 0;
    tenant_task_info.total_partition_count_ = tenant_backup_task.partition_count_;
    tenant_task_info.finish_partition_count_ = 0;
    tenant_task_info.total_macro_block_count_ = tenant_backup_task.macro_block_count_;
    tenant_task_info.finish_macro_block_count_ = 0;
    tenant_task_info.log_size_ = 0;
    tenant_task_info.comment_.assign("if backup_set_id is 0, means validate all backup set");
    if (OB_FAIL(tenant_task_updater.insert_task(tenant_task_info))) {
      LOG_WARN("failed to insert tenant validate task", K(ret));
    } else {
      LOG_INFO("schedule tenant validate success", K(ret), K(tenant_task_info.tenant_id_));
    }
  }
  return ret;
}

int ObRootValidate::get_log_archive_time_range(const uint64_t tenant_id, int64_t& start_ts, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo log_archive_info;
  ObLogArchiveBackupInfoMgr log_archive_mgr;
  bool for_update = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(
                 log_archive_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, log_archive_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (FALSE_IT(start_ts = log_archive_info.status_.start_ts_)) {
    // do nothing
  } else if (FALSE_IT(checkpoint_ts = log_archive_info.status_.checkpoint_ts_)) {
    // do nothing
  }
  return ret;
}

int ObRootValidate::get_log_archive_round_of_snapshot_version(
    const uint64_t tenant_id, const int64_t snapshot_version, int64_t& log_archive_round)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  log_archive_round = 0;
  ObArray<ObLogArchiveBackupInfo> log_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(get_all_log_archive_backup_infos(tenant_id, log_infos))) {
    LOG_WARN("failed to get all log archive backup infos", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_infos.count(); ++i) {
      const ObLogArchiveBackupInfo& log_info = log_infos.at(i);
      const int64_t start_ts = log_info.status_.start_ts_;
      const int64_t checkpoint_ts = log_info.status_.checkpoint_ts_;
      if (snapshot_version >= start_ts && snapshot_version <= checkpoint_ts) {
        exist = true;
        log_archive_round = log_info.status_.round_;
        break;
      }
    }
    if (OB_SUCC(ret) && !exist) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObRootValidate::get_all_log_archive_backup_infos(
    const uint64_t tenant_id, common::ObIArray<ObLogArchiveBackupInfo>& log_infos)
{
  int ret = OB_SUCCESS;
  bool for_update = false;
  ObLogArchiveBackupInfoMgr log_archive_mgr;
  ObLogArchiveBackupInfo cur_info;
  ObArray<ObLogArchiveBackupInfo> his_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(log_archive_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, cur_info))) {
    LOG_WARN("failed to get log achive backup info", K(ret), K(tenant_id));
  } else if (OB_FAIL(log_archive_mgr.get_log_archvie_history_infos(*sql_proxy_, tenant_id, for_update, his_infos))) {
    LOG_WARN("failed to get log archive history infos", K(ret), K(tenant_id));
  } else if (OB_FAIL(log_infos.push_back(cur_info))) {
    LOG_WARN("failed to push back current log archive backup info", K(ret), K(tenant_id), K(cur_info));
  } else if (OB_FAIL(append(log_infos, his_infos))) {
    LOG_WARN("failed to push back history log archive backup infos", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootValidate::get_all_tenant_backup_tasks(common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  tenant_backup_tasks.reset();
  ObArray<ObTenantBackupTaskInfo> tmp_infos;
  ObBackupTaskHistoryUpdater backup_history_updater;
  ObArray<ObLogArchiveBackupInfo> log_archive_infos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("validate scheduler do not init", K(ret));
  } else if (OB_FAIL(backup_history_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup history updater", K(ret));
  } else if (OB_FAIL(get_all_log_archive_backup_infos(OB_SYS_TENANT_ID, log_archive_infos))) {
    LOG_WARN("failed to get all log archive backup infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_infos.count(); ++i) {
      tmp_infos.reset();
      const ObLogArchiveBackupInfo& log_archive_info = log_archive_infos.at(i);
      const int64_t start_ts = log_archive_info.status_.start_ts_;
      const int64_t checkpoint_ts = log_archive_info.status_.checkpoint_ts_;
      if (OB_FAIL(
              backup_history_updater.get_all_tenant_backup_tasks_in_time_range(start_ts, checkpoint_ts, tmp_infos))) {
        LOG_WARN("failed to get all tenant backup tasks in time range", K(start_ts), K(checkpoint_ts), K(ret));
      } else if (OB_FAIL(append(tenant_backup_tasks, tmp_infos))) {
        LOG_WARN("failed to add array", K(ret));
      }
    }
  }
  return ret;
}

int ObRootValidate::get_tenant_backup_tasks(
    const uint64_t tenant_id, common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupTaskInfo> tmp_backup_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_backup_tasks(tmp_backup_info))) {
    LOG_WARN("failed to get all tenant backup tasks", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_backup_info.count(); ++i) {
      const ObTenantBackupTaskInfo& backup_info = tmp_backup_info.at(i);
      if (backup_info.tenant_id_ == tenant_id) {
        if (OB_FAIL(tenant_backup_tasks.push_back(backup_info))) {
          LOG_WARN("failed to push back backup info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootValidate::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    common::ObIArray<ObTenantBackupTaskInfo>& tenant_backup_tasks)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupTaskInfo> tmp_backup_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_backup_tasks(tmp_backup_info))) {
    LOG_WARN("failed to get all tenant backup tasks", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_backup_info.count(); ++i) {
      const ObTenantBackupTaskInfo& backup_info = tmp_backup_info.at(i);
      if (backup_info.tenant_id_ == tenant_id && backup_info.backup_set_id_ == backup_set_id) {
        if (OB_FAIL(tenant_backup_tasks.push_back(backup_info))) {
          LOG_WARN("failed to push back backup info", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRootValidate::get_tenant_validate_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, share::ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskUpdater updater;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || job_id < 0 || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant validate task get invalid argument",
        K(ret),
        K(job_id),
        K(tenant_id),
        K(incarnation),
        K(backup_set_id));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(updater.get_task(job_id, tenant_id, incarnation, backup_set_id, task_info))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootValidate::get_tenant_validate_tasks(
    const int64_t job_id, common::ObIArray<share::ObTenantValidateTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskUpdater updater;

  if (OB_FAIL(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant validate tasks get invalid argument", K(job_id));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(updater.get_tasks(job_id, OB_SYS_TENANT_ID, task_infos))) {
    LOG_WARN("failed to get tenant validate task", K(ret), K(job_id));
  }
  return ret;
}

int ObRootValidate::insert_validate_task(const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(task_updater_.insert_task(task_info))) {
    LOG_WARN("failed to insert backup validate task info", K(ret));
  }
  return ret;
}

int ObRootValidate::move_validate_task_to_his(
    const common::ObIArray<ObBackupValidateTenant>& validate_tenants, const share::ObBackupValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObBackupValidateTaskUpdater backup_validate_updater;
  ObBackupValidateHistoryUpdater validate_history_updater;
  ObTenantValidateTaskUpdater tenant_updater;
  ObTenantValidateHistoryUpdater tenant_history_updater;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(backup_validate_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup validate updater", K(ret));
  } else if (OB_FAIL(validate_history_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init validate history updater", K(ret));
  } else if (OB_FAIL(tenant_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(tenant_history_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init tenant history updater", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < validate_tenants.count(); ++i) {
      const ObBackupValidateTenant& validate_tenant = validate_tenants.at(i);
      const uint64_t tenant_id = validate_tenant.tenant_id_;
      const bool is_dropped_tenant = validate_tenant.is_dropped_;
      ObTenantValidateTaskInfo tenant_task_info;
      if (FALSE_IT(tenant_updater.set_dropped_tenant(is_dropped_tenant))) {
        // set dropped tenant
      } else if (OB_FAIL(tenant_updater.get_finished_task(task_info.job_id_, tenant_id, tenant_task_info))) {
        LOG_WARN("failed to get finished tenant task", K(ret), K(task_info.job_id_), K(tenant_id));
      } else if (OB_FAIL(tenant_history_updater.insert_task(tenant_task_info))) {
        LOG_WARN("failed to insert tenant task info to history", K(ret), K(tenant_task_info));
      } else if (OB_FAIL(tenant_updater.remove_task(task_info.job_id_, tenant_id, OB_START_INCARNATION))) {
        LOG_WARN("failed to remove tenant validate task", K(ret), K(task_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(validate_history_updater.insert_task(task_info))) {
        LOG_WARN("failed to insert history task", K(ret), K(task_info));
      } else if (OB_FAIL(backup_validate_updater.remove_task(task_info.job_id_))) {
        LOG_WARN("failed to remove backup validate task", K(ret), K(task_info));
      }
    }
  }
  return ret;
}

int ObRootValidate::do_insert_validate_task_his(const share::ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantValidateHistoryUpdater updater;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (updater.insert_task(task_info)) {
    LOG_WARN("failed to insert tenant validate task history", K(ret));
  }
  return ret;
}

int ObRootValidate::update_validate_task(
    const share::ObBackupValidateTaskInfo& src_info, const share::ObBackupValidateTaskInfo& dst_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (OB_FAIL(task_updater_.update_task(src_info, dst_info))) {
    LOG_WARN("failed to insert tenant validate task", K(ret));
  }
  return ret;
}

void ObRootValidate::update_prepare_flag(const bool is_prepare)
{
  UNUSED(is_prepare);
}

bool ObRootValidate::get_prepare_flag() const
{
  return is_prepare_flag_;
}

ObTenantValidate::ObTenantValidate()
    : is_inited_(false),
      is_dropped_(false),
      job_id_(-1),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(-1),
      root_validate_(NULL),
      root_balancer_(NULL),
      rebalance_mgr_(NULL),
      server_mgr_(NULL),
      rpc_proxy_(NULL),
      sql_proxy_(NULL),
      backup_lease_service_(NULL)
{}

ObTenantValidate::~ObTenantValidate()
{}

int ObTenantValidate::init(const bool is_dropped, const int64_t job_id, const uint64_t tenant_id,
    const int64_t backup_set_id, ObRootValidate& root_validate, ObRootBalancer& root_balancer,
    ObRebalanceTaskMgr& rebalance_mgr, ObServerManager& server_mgr, common::ObISQLClient& trans,
    obrpc::ObSrvRpcProxy& rpc_proxy, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant validate init twice", K(ret));
  } else if (OB_FAIL(tenant_task_updater_.init(trans, is_dropped))) {
    LOG_WARN("failed to init tenant task updater", K(ret));
  } else if (OB_FAIL(pg_task_updater_.init(trans, is_dropped))) {
    LOG_WARN("failed to init pg task updater", K(ret));
  } else {
    is_dropped_ = is_dropped;
    job_id_ = job_id;
    tenant_id_ = tenant_id;
    backup_set_id_ = backup_set_id;
    root_validate_ = &root_validate;
    root_balancer_ = &root_balancer;
    rebalance_mgr_ = &rebalance_mgr;
    server_mgr_ = &server_mgr;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &trans;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantValidate::do_scheduler()
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskInfo task_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (OB_FAIL(get_tenant_validate_task_info(task_info))) {
    LOG_WARN("failed to get tenant validate task info", K(ret));
  } else {
    const ObTenantValidateTaskInfo::ValidateStatus& status = task_info.status_;
    switch (status) {
      case ObTenantValidateTaskInfo::SCHEDULE: {
        if (OB_FAIL(do_generate(task_info))) {
          LOG_WARN("failed to do generate", K(ret), K(task_info));
        } else {
          LOG_INFO("tenant validate do generate success", K(ret), K(task_info));
        }
        break;
      }
      case ObTenantValidateTaskInfo::DOING: {
        if (OB_FAIL(do_validate(task_info))) {
          LOG_WARN("failed to do validate", K(ret), K(task_info));
        } else {
          LOG_INFO("tenant validate do validate success", K(ret), K(task_info));
        }
        break;
      }
      case ObTenantValidateTaskInfo::FINISHED: {
        if (OB_FAIL(do_finish(task_info))) {
          LOG_WARN("failed to do finish", K(ret), K(task_info));
        } else {
          LOG_INFO("tenant validate do finish success", K(ret), K(task_info));
        }
        break;
      }
      case ObTenantValidateTaskInfo::CANCEL: {
        if (OB_FAIL(do_cancel(task_info))) {
          LOG_WARN("failed to do cancel", K(ret), K(task_info));
        } else {
          LOG_INFO("tenant validate do cancel success", K(ret), K(task_info));
        }
        break;
      }
      case ObTenantValidateTaskInfo::MAX:
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("err unexpected", K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObTenantValidate::do_generate(const share::ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  int64_t total_pg_count = 0;
  ObArray<ObPartitionKey> pkeys;
  ObClusterBackupDest backup_dest;
  ObArray<ObTenantBackupTaskInfo> backup_task_infos;
  const int64_t full_backup_set_id = 1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (OB_FAIL(
                 backup_dest.set(task_info.backup_dest_, GCONF.cluster, GCONF.cluster_id, task_info.incarnation_))) {
    LOG_WARN("failed to init cluster backup dest", K(ret), K(task_info));
  } else if (OB_FAIL(get_backup_infos_from_his(task_info, backup_task_infos))) {
    LOG_WARN("failed to get backup set infos from history", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_task_infos.count(); ++i) {
      const ObTenantBackupTaskInfo& tenant_backup_info = backup_task_infos.at(i);
      const int64_t backup_set_id = tenant_backup_info.backup_set_id_;
      int64_t log_archive_round = 0;
      ObArray<ObPGValidateTaskInfo> pg_task_infos;
      if (OB_FAIL(root_validate_->get_log_archive_round_of_snapshot_version(
              tenant_backup_info.tenant_id_, tenant_backup_info.snapshot_version_, log_archive_round))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (OB_ALL_BACKUP_SET_ID == task_info.backup_set_id_) {
            // no need to generate task
            // if in interrupted log archive round
            ret = OB_SUCCESS;
            continue;
          } else {
            ObTenantValidateTaskInfo update_task_info;
            update_task_info = task_info;
            update_task_info.result_ = OB_ISOLATED_BACKUP_SET;
            update_task_info.status_ = ObTenantValidateTaskInfo::FINISHED;
            if (OB_FAIL(update_tenant_validate_task(task_info, update_task_info))) {
              LOG_WARN("failed to update tenant validate task info");
            } else {
              root_validate_->wakeup();
            }
            break;
          }
        }
        LOG_WARN("failed to get log archive round of snapshot version", K(tenant_backup_info));
      } else if (OB_FAIL(fetch_all_pg_list(
                     backup_dest, log_archive_round, full_backup_set_id, backup_set_id, task_info, pkeys))) {
        LOG_WARN("failed to fetch all pg list", K(ret), K(task_info), K(pkeys));
      } else if (FALSE_IT(total_pg_count += pkeys.count())) {
      } else if (OB_FAIL(
                     build_pg_validate_task_infos(log_archive_round, backup_set_id, task_info, pkeys, pg_task_infos))) {
        LOG_WARN("failed to build pg validate task infos");
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(pg_task_updater_.batch_report_pg_task(pg_task_infos))) {
          LOG_WARN("failed to batch report pg task", K(ret), K(tenant_backup_info), K(pg_task_infos));
        } else {
          LOG_INFO("batch report pg task success", K(ret), K(tenant_backup_info), K(pg_task_infos));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTenantValidateTaskInfo update_task_info;
    update_task_info = task_info;
    update_task_info.status_ = ObTenantValidateTaskInfo::DOING;
    update_task_info.total_pg_count_ = total_pg_count;
    ObBackupValidateTenant validate_tenant;
    validate_tenant.tenant_id_ = task_info.tenant_id_;
    validate_tenant.is_dropped_ = is_dropped_;
    if (OB_FAIL(update_tenant_validate_task(task_info, update_task_info))) {
      LOG_WARN("failed to update tenant validate task info");
    } else {
      root_balancer_->wakeup();
    }
  }
  return ret;
}

int ObTenantValidate::do_validate(const ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantValidateTaskInfo update_task_info;
  ObArray<ObPGValidateTaskInfo> pg_task_infos;
  bool all_finished = false;
  bool need_report = false;
  int64_t total_macro_block_count = 0;
  int64_t finish_macro_block_count = 0;
  int64_t total_log_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (OB_FAIL(get_finished_pg_validate_task(task_info, pg_task_infos, all_finished, need_report))) {
    LOG_WARN("failed to get finished validate task", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
      const ObPGValidateTaskInfo& pg_task_info = pg_task_infos.at(i);
      total_macro_block_count += pg_task_info.total_macro_block_count_;
      finish_macro_block_count += pg_task_info.finish_macro_block_count_;
      total_log_size += pg_task_info.log_size_;
    }
    update_task_info = task_info;
    update_task_info.end_time_ = ObTimeUtility::current_time();
    update_task_info.status_ = all_finished ? ObTenantValidateTaskInfo::FINISHED : ObTenantValidateTaskInfo::DOING;
    update_task_info.finish_pg_count_ = pg_task_infos.count();
    update_task_info.result_ = OB_SUCCESS;
    update_task_info.total_macro_block_count_ = total_macro_block_count;
    update_task_info.finish_macro_block_count_ = finish_macro_block_count;
    update_task_info.log_size_ = total_log_size;
    if (OB_SUCC(ret) && need_report) {
      if (OB_FAIL(tenant_task_updater_.update_task(task_info, update_task_info))) {
        LOG_WARN("failed to update tenant validate task", K(ret));
      } else {
        root_validate_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantValidate::do_finish(const share::ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantValidateTaskInfo::FINISHED != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do finish get invalid argument", K(ret));
  } else {
    LOG_INFO("tenant validate finished", K(ret));
  }
  return ret;
}

int ObTenantValidate::do_cancel(const share::ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do cancel get invalid argument", K(ret), K(task_info));
  } else {
    ObTenantValidateTaskInfo update_task_info;
    update_task_info = task_info;
    update_task_info.status_ = ObTenantValidateTaskInfo::FINISHED;
    update_task_info.result_ = OB_CANCELED;
    update_task_info.end_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(cancel_doing_pg_tasks(task_info))) {
      LOG_WARN("failed to cancel doing pg tasks", K(ret));
    } else if (OB_FAIL(update_tenant_validate_task(task_info, update_task_info))) {
      LOG_WARN("failed to update tenant validate task", K(ret), K(task_info), K(update_task_info));
    } else {
      root_validate_->wakeup();
    }
  }
  return ret;
}

int ObTenantValidate::get_backup_infos_from_his(const share::ObTenantValidateTaskInfo& task_info,
    common::ObIArray<share::ObTenantBackupTaskInfo>& backup_task_infos)
{
  int ret = OB_SUCCESS;
  backup_task_infos.reset();
  ObArray<ObTenantBackupTaskInfo> all_backup_task_infos;
  const int64_t backup_set_id = task_info.backup_set_id_;
  ObBackupTaskHistoryUpdater history_updater;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup set infos get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(history_updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_FAIL(history_updater.get_tenant_backup_tasks(task_info.tenant_id_, all_backup_task_infos))) {
    LOG_WARN("failed to get tenant backup tasks", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_backup_task_infos.count(); ++i) {
      const ObTenantBackupTaskInfo& tenant_backup_info = all_backup_task_infos.at(i);
      if (OB_ALL_BACKUP_SET_ID == backup_set_id) {
        if (OB_FAIL(backup_task_infos.push_back(tenant_backup_info))) {
          LOG_WARN("failed to push back tenant backup info", K(ret));
        }
      } else {
        if (backup_set_id == tenant_backup_info.backup_set_id_) {
          if (OB_FAIL(backup_task_infos.push_back(tenant_backup_info))) {
            LOG_WARN("failed to push back tenant backup info", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantValidate::build_pg_validate_task_infos(const int64_t archive_round, const int64_t backup_set_id,
    const share::ObTenantValidateTaskInfo& task_info, const common::ObIArray<common::ObPartitionKey>& pkeys,
    common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  ObPGValidateTaskInfo pg_task_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
    pg_task_info.reset();
    pg_task_info.job_id_ = task_info.job_id_;
    pg_task_info.task_id_ = task_info.task_id_;
    pg_task_info.tenant_id_ = task_info.tenant_id_;
    pg_task_info.table_id_ = pkeys.at(i).get_table_id();
    pg_task_info.partition_id_ = pkeys.at(i).get_partition_id();
    pg_task_info.incarnation_ = OB_START_INCARNATION;
    pg_task_info.backup_set_id_ = backup_set_id;
    pg_task_info.archive_round_ = archive_round;
    pg_task_info.total_partition_count_ = 0;
    pg_task_info.finish_partition_count_ = 0;
    pg_task_info.total_macro_block_count_ = 0;
    pg_task_info.finish_macro_block_count_ = 0;
    pg_task_info.log_size_ = 0;
    pg_task_info.status_ = ObPGValidateTaskInfo::PENDING;
    if (OB_FAIL(pg_task_infos.push_back(pg_task_info))) {
      LOG_WARN("failed to push back pg task info", K(ret));
    }
  }
  return ret;
}

int ObTenantValidate::get_tenant_validate_task_info(share::ObTenantValidateTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (OB_FAIL(
                 tenant_task_updater_.get_task(job_id_, tenant_id_, OB_START_INCARNATION, backup_set_id_, task_info))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info should be valid", K(ret), K(task_info));
  }
  return ret;
}

int ObTenantValidate::update_tenant_validate_task(
    const share::ObTenantValidateTaskInfo& src_info, const share::ObTenantValidateTaskInfo& dst_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root validate do not init", K(ret));
  } else if (!src_info.is_valid() || !dst_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant validate task get invalid argument", K(ret));
  } else if (tenant_task_updater_.update_task(src_info, dst_info)) {
    LOG_WARN("failed to update tenant validate task", K(ret));
  }
  return ret;
}

int ObTenantValidate::get_finished_pg_validate_task(const share::ObTenantValidateTaskInfo& tenant_task_info,
    common::ObIArray<share::ObPGValidateTaskInfo>& pg_task_infos, bool& all_finished, bool& need_report)
{
  int ret = OB_SUCCESS;
  all_finished = false;
  need_report = false;
  pg_task_infos.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (!tenant_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task info is invalid", K(ret));
  } else if (OB_FAIL(pg_task_updater_.get_finished_pg_tasks(tenant_task_info.job_id_,
                 tenant_task_info.tenant_id_,
                 tenant_task_info.incarnation_,
                 pg_task_infos))) {
    LOG_WARN("failed to get finished pg validate tasks", K(ret), K(tenant_task_info));
  } else {
    if (tenant_task_info.total_pg_count_ == pg_task_infos.count()) {
      all_finished = true;
    }
    if (tenant_task_info.finish_pg_count_ != pg_task_infos.count()) {
      need_report = true;
    }
  }
  return ret;
}

int ObTenantValidate::check_doing_task_finished(const share::ObPGValidateTaskInfo& pg_task_info, bool& finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  ObPartitionKey pkey;
  ObPGValidateTaskInfo tmp_pg_task_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (OB_FAIL(pkey.init(pg_task_info.table_id_, pg_task_info.partition_id_, 0))) {
    LOG_WARN("failed to init pkey", K(ret), K(pg_task_info));
  } else if (OB_FAIL(pg_task_updater_.get_pg_validate_task(pg_task_info.job_id_,
                 pg_task_info.tenant_id_,
                 pg_task_info.incarnation_,
                 pg_task_info.backup_set_id_,
                 pkey,
                 tmp_pg_task_info))) {
    LOG_WARN("failed to get pg validate task", K(ret));
  } else if (ObPGValidateTaskInfo::FINISHED == tmp_pg_task_info.status_) {
    finished = true;
  }
  return ret;
}

int ObTenantValidate::cancel_doing_pg_tasks(const share::ObTenantValidateTaskInfo& tenant_task_info)
{
  int ret = OB_SUCCESS;
  ObPGValidateTaskInfo pg_task;
  ObPGValidateTaskInfo* pg_task_ptr = NULL;
  const uint64_t tenant_id = is_dropped_ ? OB_SYS_TENANT_ID : tenant_task_info.tenant_id_;
  ObArray<ObPGValidateTaskInfo> pg_task_infos;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (!tenant_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant task info is not valid", K(ret), K(tenant_task_info));
  } else {
    do {
      if (OB_FAIL(pg_task_updater_.get_one_doing_pg_tasks(tenant_id, pg_task_ptr, pg_task_infos))) {
        LOG_WARN("failed to get one doing pg tasks", K(ret), K(tenant_task_info));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
          const ObPGValidateTaskInfo& pg_task_info = pg_task_infos.at(i);
          const ObAddr& server = pg_task_info.server_;
          obrpc::ObCancelTaskArg cancel_arg;
          cancel_arg.task_id_ = pg_task_info.trace_id_;
          if (OB_FAIL(rpc_proxy_->to(server).cancel_sys_task(cancel_arg))) {
            LOG_WARN("failed to cancel sys task", K(ret), K(server), K(cancel_arg));
            // continue canceling the pg task even if one task has failed
            ret = OB_SUCCESS;
          } else {
            LOG_INFO("cancel doing pg tasks success", K(ret), K(server), K(cancel_arg));
          }
        }
      }
      if (OB_SUCC(ret) && pg_task_infos.count() > 0) {
        pg_task = pg_task_infos.at(pg_task_infos.count() - 1);
        pg_task_ptr = &pg_task;
      }
    } while (OB_SUCC(ret) && pg_task_infos.count() > 0);
  }
  return ret;
}

int ObTenantValidate::fetch_all_pg_list(const ObClusterBackupDest& backup_dest, const int64_t log_archive_round,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const share::ObTenantValidateTaskInfo& tenant_task_info, common::ObIArray<common::ObPartitionKey>& pkeys)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUCKET_NUM = 2048;
  pkeys.reset();
  ObArray<ObPartitionKey> normal_pkeys;
  ObArray<ObPartitionKey> sys_pkeys;
  ObArray<ObPartitionKey> clog_pkeys;
  hash::ObHashSet<ObPartitionKey> pkey_set;
  ObExternPGListMgr extern_pg_list_mgr;
  ObBackupListDataMgr list_data_mgr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate do not init", K(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.init(tenant_task_info.tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_dest,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern pg list mgr", K(ret));
  } else if (OB_FAIL(list_data_mgr.init(backup_dest, log_archive_round, tenant_task_info.tenant_id_))) {
    LOG_WARN("failed to init backup list data mgr", K(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.get_normal_pg_list(normal_pkeys))) {
    LOG_WARN("failed to get normal pg list");
  } else if (OB_FAIL(extern_pg_list_mgr.get_sys_pg_list(sys_pkeys))) {
    LOG_WARN("failed to get sys pg list", K(ret));
  } else if (OB_FAIL(append(pkeys, normal_pkeys))) {
    LOG_WARN("failed to add normal pkey array", K(ret));
  } else if (OB_FAIL(append(pkeys, sys_pkeys))) {
    LOG_WARN("failed to add sys pkey array", K(ret));
  } else if (OB_FAIL(list_data_mgr.get_clog_pkey_list(clog_pkeys))) {
    LOG_WARN("failed to get clog pkey list", K(ret));
  } else if (OB_FAIL(pkey_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create pkey set", K(ret));
  } else {
    bool overwrite_key = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      if (OB_FAIL(pkey_set.set_refactored_1(pkey, overwrite_key))) {
        LOG_WARN("failed to set pkey", K(ret), K(pkey));
      }
    }
    if (OB_SUCC(ret)) {
      for (int i = 0; OB_SUCC(ret) && i < clog_pkeys.count(); ++i) {
        const ObPartitionKey& clog_pkey = clog_pkeys.at(i);
        int hash_ret = pkey_set.exist_refactored(clog_pkey);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(pkeys.push_back(clog_pkey))) {
            LOG_WARN("failed to push back pkey", K(ret), K(clog_pkey));
          }
        } else if (OB_HASH_EXIST == hash_ret) {
          // do nothing
        } else {
          ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
          LOG_WARN("failed to check exist from set", K(ret), K(clog_pkey));
        }
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
