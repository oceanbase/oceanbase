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
#include "ob_root_backup.h"
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "ob_root_balancer.h"
#include "ob_rs_event_history_table_operator.h"
#include "share/backup/ob_tenant_backup_clean_info_updater.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver {

int64_t ORootBackupIdling::get_idle_interval_us()
{
  const int64_t backup_check_interval = GCONF._backup_idle_time;
  return backup_check_interval;
}

ObRootBackup::ObRootBackup()
    : is_inited_(false),
      config_(NULL),
      schema_service_(NULL),
      sql_proxy_(NULL),
      idling_(stop_),
      root_balancer_(NULL),
      freeze_info_mgr_(NULL),
      server_mgr_(NULL),
      rebalancer_mgr_(NULL),
      zone_mgr_(NULL),
      rpc_proxy_(NULL),
      lease_time_map_(),
      is_prepare_flag_(false),
      need_switch_tenant_(false),
      is_working_(false),
      inner_error_(OB_SUCCESS),
      extern_device_error_(OB_SUCCESS),
      backup_meta_info_(),
      backup_lease_service_(nullptr)
{}

ObRootBackup::~ObRootBackup()
{}

int ObRootBackup::init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
    ObMySQLProxy& sql_proxy, ObRootBalancer& root_balancer, ObFreezeInfoManager& freeze_info_mgr,
    ObServerManager& server_mgr, ObRebalanceTaskMgr& rebalancer_mgr, ObZoneManager& zone_mgr,
    obrpc::ObSrvRpcProxy& rpc_proxy, share::ObIBackupLeaseService& backup_lease_service,
    ObRestorePointService& restore_point_service)
{
  int ret = OB_SUCCESS;
  const int root_backup_thread_cnt = 1;
  const int64_t MAX_BUCKET = 1000;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "root backup init twice", K(ret));
  } else if (OB_FAIL(create(root_backup_thread_cnt, "RootBackup"))) {
    LOG_WARN("create thread failed", K(ret), K(root_backup_thread_cnt));
  } else if (OB_FAIL(lease_time_map_.create(MAX_BUCKET, ObModIds::BACKUP))) {
    LOG_WARN("failed to create lease time map", K(ret));
  } else {
    config_ = &cfg;
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    root_balancer_ = &root_balancer;
    freeze_info_mgr_ = &freeze_info_mgr;
    server_mgr_ = &server_mgr;
    rebalancer_mgr_ = &rebalancer_mgr;
    zone_mgr_ = &zone_mgr;
    rpc_proxy_ = &rpc_proxy;
    backup_lease_service_ = &backup_lease_service;
    restore_point_service_ = &restore_point_service;
    is_inited_ = true;
    LOG_INFO("root backup init success");
  }
  return ret;
}

int ObRootBackup::idle() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } else {
    LOG_INFO("root backup idle", "idle_time", idling_.get_idle_interval_us());
  }
  return ret;
}

void ObRootBackup::wakeup()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    idling_.wakeup();
  }
}

void ObRootBackup::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    update_prepare_flag(false /*is_prepare_flag*/);
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObRootBackup::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("failed to start", K(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

void ObRootBackup::run3()
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_PREPARED_INFOS_INTERVAL = 10 * 1000 * 1000;  // 10s
  ObArray<uint64_t> tenant_ids;
  ObCurTraceId::init(GCONF.self_addr_);

  FLOG_INFO("start ObRootBackup");
  while (!stop_) {
    ret = OB_SUCCESS;
    tenant_ids.reset();
    inner_error_ = OB_SUCCESS;
    extern_device_error_ = OB_SUCCESS;

    DEBUG_SYNC(BEFROE_DO_ROOT_BACKUP);
    if (OB_FAIL(check_can_backup())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(get_need_backup_tenant_ids(tenant_ids))) {
      LOG_WARN("failed to get need backup tenant ids", K(ret), K(tenant_ids));
    } else if (OB_FAIL(do_root_scheduler(tenant_ids))) {
      LOG_WARN("failed to do root scheduler", K(ret), K(tenant_ids));
    } else if (OB_FAIL(cleanup_stopped_backup_task_infos())) {
      LOG_WARN("failed to cleanup stopped backup task infos", K(ret));
    }

    if (OB_LEASE_NOT_ENOUGH != ret && REACH_TIME_INTERVAL(CHECK_PREPARED_INFOS_INTERVAL)) {
      cleanup_prepared_infos();
    }

    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", K(ret));
    } else {
      continue;
    }
  }

  if (OB_FAIL(lease_time_map_.reuse())) {
    LOG_ERROR("failed to reuse lease time map", K(ret));
  }

  is_working_ = false;
  inner_error_ = OB_SUCCESS;
  reset_tenant_backup_meta_info();
  FLOG_INFO("finish ObRootBackup");
}

int ObRootBackup::get_need_backup_tenant_ids(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObArray<uint64_t> all_tenant_ids;
  ObBackupInfoManager info_manager;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct sys_backup_info;
  const int64_t EXECUTE_TIMEOUT_US = 1L * 100 * 1000;        // 100 ms
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  int64_t calc_stmt_timeout = EXECUTE_TIMEOUT_US;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (all_tenant_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(info_manager.init(all_tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(all_tenant_ids));
  } else if (FALSE_IT(calc_stmt_timeout = (stmt_timeout * all_tenant_ids.count()))) {
  } else if (FALSE_IT(stmt_timeout =
                          (calc_stmt_timeout > MIN_EXECUTE_TIMEOUT_US ? calc_stmt_timeout : MIN_EXECUTE_TIMEOUT_US))) {
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(OB_SYS_TENANT_ID, updater, sys_backup_info))) {
      LOG_WARN("failed to get backup info", K(ret));
    } else if (sys_backup_info.backup_status_.is_prepare_status()) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant_ids.count(); ++i) {
        const uint64_t tenant_id = all_tenant_ids.at(i);
        bool need_add = false;
        if (OB_SYS_TENANT_ID == tenant_id) {
          need_add = true;
        } else if (OB_FAIL(get_need_backup_info(tenant_id, info_manager, need_add))) {
          LOG_WARN("failed to get need backup info", K(ret), K(tenant_id));
        }

        if (OB_FAIL(ret)) {
        } else if (!need_add) {
          // do nothing
        } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
        } else if (OB_FAIL(insert_lease_time(tenant_id))) {
          LOG_WARN("failed to insert lease time", K(ret), K(tenant_id));
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

int ObRootBackup::get_all_tenant_ids(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  tenant_ids.reset();
  ObArray<uint64_t> sys_tenant_id_array;
  ObBackupInfoManager info_manager;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct sys_backup_info;
  ObArray<uint64_t> tmp_tenant_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do init", K(ret));
  } else if (OB_FAIL(sys_tenant_id_array.push_back(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to push back sys tenant id", K(ret));
  } else if (OB_FAIL(info_manager.init(sys_tenant_id_array, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(sys_tenant_id_array));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(OB_SYS_TENANT_ID, updater, sys_backup_info))) {
      LOG_WARN("failed to get backup info", K(ret));
    } else if (0 == sys_backup_info.backup_schema_version_) {
      // do nothing
      // The schema_version of the backup is 0, indicating that the backup has not yet started, so skip
    } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                   OB_SYS_TENANT_ID, *schema_service_, sys_backup_info.backup_schema_version_, guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), K(sys_backup_info));
    } else if (OB_FAIL(guard.get_tenant_ids(tmp_tenant_ids))) {
      LOG_WARN("failed to get tenant ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tmp_tenant_ids.at(i);
        bool is_dropped = false;
        if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
          LOG_WARN("failed to check tenant id dropped", K(ret), K(tenant_id));
        } else if (is_dropped) {
          inner_error_ = OB_SUCCESS == inner_error_ ? OB_TENANT_HAS_BEEN_DROPPED : inner_error_;
          FLOG_INFO("tenant has been dropped, skip clean up", K(is_dropped), K(tenant_id));
        } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
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

int ObRootBackup::get_need_backup_info(const uint64_t tenant_id, ObBackupInfoManager& info_manager, bool& need_add)
{
  int ret = OB_SUCCESS;
  need_add = false;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct info;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else if (info.backup_status_.is_valid()) {
      need_add = true;
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObRootBackup::do_root_scheduler(const ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;

  LOG_INFO("start do root scheduler");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (tenant_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", K(ret), K(tenant_ids));
  } else {
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      need_switch_tenant_ = false;
      while (!need_switch_tenant_) {
        need_switch_tenant_ = true;
        if (OB_SUCCESS != (tmp_ret = do_tenant_scheduler(tenant_ids.at(i), info_manager))) {
          LOG_WARN("failed to do tenant scheduler", K(ret), K(tmp_ret), K(tenant_ids.at(i)));
        }
        if (OB_SUCCESS != tmp_ret) {
          if (OB_LEASE_NOT_ENOUGH == tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("do not own lease", K(ret));
          } else if (ObBackupUtils::is_need_retry_error(tmp_ret)) {
            wakeup();
          } else {
            inner_error_ = OB_SUCCESS == inner_error_ ? tmp_ret : inner_error_;
            FLOG_INFO("set inner error", K(inner_error_));
          }

          if (ObBackupUtils::is_extern_device_error(tmp_ret)) {
            extern_device_error_ = OB_SUCCESS == extern_device_error_ ? tmp_ret : extern_device_error_;
            FLOG_INFO("set extern device error", K(extern_device_error_));
          }
        }
      }
      // need reset backup meta info when change tenant
      reset_tenant_backup_meta_info();
    }
  }
  return ret;
}

int ObRootBackup::do_tenant_scheduler(const uint64_t tenant_id, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("rootservice shutdown", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant scheduler get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(updater))) {
        LOG_WARN("failed to commit trans", K(ret), K(tenant_id), K(info));
      }
    } else {
      int tmp_ret = updater.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_with_status(info_manager, info))) {
      LOG_WARN("failed to do with status", K(ret), K(info));
    }
  }
  return ret;
}

int ObRootBackup::do_with_status(share::ObBackupInfoManager& info_manager, const ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("rootservice shutdown", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(info));
  } else if (OB_FAIL(check_can_backup())) {
    LOG_WARN("failed to check can backup", K(ret), K(info));
  } else {
    const ObBackupInfoStatus& status = info.backup_status_;
    switch (status.status_) {
      case ObBackupInfoStatus::STOP:
        break;
      case ObBackupInfoStatus::PREPARE:
        break;
      case ObBackupInfoStatus::SCHEDULE:
        if (OB_FAIL(do_scheduler(info, info_manager))) {
          LOG_WARN("failed to do scheduler", K(ret), K(info));
        }
        break;
      case ObBackupInfoStatus::DOING:
        if (OB_FAIL(do_backup(info, info_manager))) {
          LOG_WARN("failed to do backup", K(ret), K(info));
        }
        break;
      case ObBackupInfoStatus::CLEANUP:
        if (OB_FAIL(do_cleanup(info, info_manager))) {
          LOG_WARN("failed to do cleanup", K(ret), K(info));
        }
        break;
      case ObBackupInfoStatus::CANCEL:
        if (OB_FAIL(do_cancel(info, info_manager))) {
          LOG_WARN("failed to do cancel", K(ret), K(info));
        }
        break;
      case ObBackupInfoStatus::MAX:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("backup info status is invalid", K(ret), K(info));
        break;
    }
    LOG_INFO("doing backup", K(info), K(ret));
  }
  return ret;
}

int ObRootBackup::do_scheduler(const ObBaseBackupInfoStruct& info, ObBackupInfoManager& info_manager)
{
  // TODO() sys tenant and normal tenant
  LOG_INFO("start do backup scheduler", K(info));
  int ret = OB_SUCCESS;
  const ObExternBackupInfo::ExternBackupInfoStatus status = ObExternBackupInfo::DOING;
  ObExternBackupInfo extern_backup_info;
  const bool force_stop = false;
  share::ObBackupItemTransUpdater updater;
  ObTimeoutCtx timeout_ctx;
  char backup_region[MAX_REGION_LENGTH] = "";

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (stop_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("observer is stopping", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do scheduler get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(update_extern_backup_infos(info, status, force_stop, extern_backup_info))) {
    LOG_WARN("failed to do extern backup infos", K(ret), K(info));
  } else if (OB_FAIL(do_extern_tenant_infos(info, info_manager))) {
    LOG_WARN("failed to do extern tenant infos", K(ret), K(info));
  } else {

#ifdef ERRSIM
    ret = E(EventTable::EN_BACKUP_EXTERN_INFO_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("errsim set extern backup info error", K(ret));
    }
#endif

    ObBaseBackupInfoStruct dest_info = info;
    // insert status beginning task into task_info
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
      LOG_WARN("failed to start trans", K(ret), K(info));
    } else {
      if (OB_FAIL(add_backup_info_lock(info, updater, info_manager))) {
        LOG_WARN("failed to add backup info lock", K(ret), K(info));
      } else if (OB_FAIL(insert_tenant_backup_task(updater.get_trans(), info, extern_backup_info))) {
        LOG_WARN("failed to insert tenant backup task", K(ret), K(info));
      } else if (OB_FAIL(GCONF.backup_region.copy(backup_region, sizeof(backup_region)))) {
        LOG_WARN("failed to copy backup region", K(ret));
      } else if (OB_FAIL(dest_info.detected_backup_region_.assign(backup_region))) {
        LOG_WARN("failed to assign backup region", K(ret), K(dest_info));
      } else {
        if (OB_SYS_TENANT_ID == info.tenant_id_) {
          ROOTSERVICE_EVENT_ADD("backup", "start backup cluster");
        } else {
          ROOTSERVICE_EVENT_ADD("backup", "start backup tenant", "tenant_id", info.tenant_id_);
        }
        DEBUG_SYNC(BACKUP_INFO_BEFOR_DOING);
        dest_info.backup_set_id_ = extern_backup_info.inc_backup_set_id_;
        dest_info.backup_status_.set_backup_status_doing();
        dest_info.backup_type_.type_ = extern_backup_info.backup_type_;
        if (OB_FAIL(update_tenant_backup_info(info, dest_info, info_manager, updater))) {
          LOG_WARN("failed to update tenant backup info", K(ret), K(info), K(dest_info));
        } else {
          wakeup();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(commit_trans(updater))) {
          LOG_WARN("failed to commit trans", K(ret), K(info));
        }
      } else {
        int tmp_ret = updater.end(false /*commit*/);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObRootBackup::do_backup(const ObBaseBackupInfoStruct& info, ObBackupInfoManager& info_manager)
{
  LOG_INFO("start do backup", K(info));
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObTenantBackup tenant_backup;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(check_can_backup())) {
    LOG_WARN("failed to check can backup", K(ret), K(info));
  } else {
    if (OB_SYS_TENANT_ID == info.tenant_id_) {
      if (OB_FAIL(do_sys_tenant_backup(info, info_manager))) {
        LOG_WARN("failed to do sys tenant backup", K(ret), K(info));
      }
    } else {
      if (OB_FAIL(do_tenant_backup(info, info_manager))) {
        LOG_WARN("failed to do tenant backup", K(ret), K(info));
      }
    }
  }
  return ret;
}

int ObRootBackup::do_sys_tenant_backup(
    const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  // TODO() fix it later, use tenant backup task check result
  int ret = OB_SUCCESS;
  share::ObBackupItemTransUpdater updater;
  ObTimeoutCtx timeout_ctx;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || OB_SYS_TENANT_ID != info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
    LOG_WARN("failed to start trans", K(ret), K(info));
  } else {
    if (OB_FAIL(add_backup_info_lock(info, updater, info_manager))) {
      LOG_WARN("failed to add backup info lock", K(ret), K(info));
    } else if (OB_FAIL(check_tenants_backup_task_failed(info, info_manager, updater.get_trans()))) {
      LOG_WARN("failed to check tenants backup task failed", K(ret), K(info));
    } else if (OB_FAIL(do_with_all_finished_info(info, updater, info_manager))) {
      LOG_WARN("failed to do with all finished info", K(ret), K(info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(updater))) {
        LOG_WARN("failed to commit trans", K(ret), K(info));
      }
    } else {
      int tmp_ret = updater.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObRootBackup::do_tenant_backup(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  LOG_INFO("start do backup", K(info));
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObTenantBackup tenant_backup;
  share::ObBackupItemTransUpdater updater;
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || OB_SYS_TENANT_ID == info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(check_tenant_backup_inner_error(info))) {
    LOG_WARN("failed to check tenant backup inner error", K(ret), K(info));
  } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
    LOG_WARN("failed to start trans", K(ret), K(info));
  } else {
    if (OB_FAIL(add_backup_info_lock(info, updater, info_manager))) {
      LOG_WARN("failed to add backup info lock", K(ret), K(info));
    } else if (OB_FAIL(get_tenant_backup_task(updater.get_trans(), info, task_info))) {
      LOG_WARN("failed to get tenant backup task", K(ret), K(info));
    } else if (!task_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant backup task should not be invalid", K(ret), K(task_info));
    } else if (ObTenantBackupTaskInfo::FINISH == task_info.status_) {
      DEBUG_SYNC(BACKUP_INFO_BEFOR_CLEANUP);
      ObBaseBackupInfoStruct dest_info = info;
      dest_info.backup_status_.set_backup_status_cleanup();
      if (OB_FAIL(update_tenant_backup_info(info, dest_info, info_manager, updater))) {
        LOG_WARN("failed to update tenant backup info", K(ret), K(info), K(dest_info));
      } else {
        wakeup();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(updater))) {
        LOG_WARN("failed to commit trans", K(ret), K(info));
      }
    } else {
      int tmp_ret = updater.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (ObTenantBackupTaskInfo::FINISH != task_info.status_) {
        if (OB_FAIL(tenant_backup.init(info,
                *schema_service_,
                *root_balancer_,
                *sql_proxy_,
                *server_mgr_,
                *rebalancer_mgr_,
                *rpc_proxy_,
                *this,
                *backup_lease_service_))) {
          LOG_WARN("failed to init tenant backup", K(ret), K(info));
        } else if (OB_FAIL(tenant_backup.do_backup())) {
          LOG_WARN("failed to do tenant backup", K(ret), K(task_info), K(info));
        }

        if (OB_SUCC(ret)) {
          if (ObTenantBackupTaskInfo::GENERATE == task_info.status_) {
            need_switch_tenant_ = false;

#ifdef ERRSIM
            ret = E(EventTable::EN_ROOT_BACKUP_NEED_SWITCH_TENANT) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              need_switch_tenant_ = true;
              wakeup();
              ret = OB_SUCCESS;
            }
#endif
          }
        }
      }
    }
  }
  LOG_INFO("do root backup wait tenant backup task done", K(task_info.status_), K(ret));
  return ret;
}

int ObRootBackup::do_cleanup(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  LOG_INFO("start do cleanup", K(info));
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo tenant_task_info;
  ObBaseBackupInfoStruct dest_info;
  ObExternBackupInfo extern_backup_info;
  ObExternBackupInfo::ExternBackupInfoStatus status;
  ObExternTenantLocalityInfo extern_tenant_locality_info;
  ObExternBackupSetInfo extern_backup_set_info;
  bool all_pg_task_deleted = false;
  bool is_force_stop = false;
  ObTimeoutCtx timeout_ctx;
  share::ObBackupItemTransUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || !info.backup_status_.is_cleanup_status()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do cleanup get invalid argument", K(ret), K(info));
  } else if (OB_SYS_TENANT_ID == info.tenant_id_) {
    // do nothing
  } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
    LOG_WARN("failed to start trans", K(ret), K(info));
  } else {
    if (OB_FAIL(add_backup_info_lock(info, updater, info_manager))) {
      LOG_WARN("failed to add backup info lock", K(ret), K(info));
    } else if (OB_FAIL(get_tenant_backup_task(updater.get_trans(), info, tenant_task_info))) {
      LOG_WARN("failed to get tenant backup task", K(ret), K(info));
    } else if (!tenant_task_info.is_valid() || ObTenantBackupTaskInfo::FINISH != tenant_task_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant task info is invalid ", K(ret), K(tenant_task_info));
    } else if (OB_FAIL(do_cleanup_pg_backup_tasks(tenant_task_info, all_pg_task_deleted, updater.get_trans()))) {
      LOG_WARN("failed to do cleanup pg backup tasks", K(ret), K(tenant_task_info));
    } else if (!all_pg_task_deleted) {
      // do nothing
      need_switch_tenant_ = false;
    } else if (OB_FAIL(do_insert_tenant_backup_task_his(tenant_task_info, updater.get_trans()))) {
      LOG_WARN("failed to do insert tenant backup task history", K(ret), K(tenant_task_info));
    } else if (FALSE_IT(status = (OB_SUCCESS == tenant_task_info.result_ ? ObExternBackupInfo::SUCCESS
                                                                         : ObExternBackupInfo::FAILED))) {
    } else if (FALSE_IT(
                   is_force_stop = (OB_CANCELED == tenant_task_info.result_ && OB_SUCCESS != extern_device_error_))) {
    } else if (OB_FAIL(update_extern_backup_infos(info, status, is_force_stop, extern_backup_info))) {
      LOG_WARN("failed to do extern backup infos", K(ret), K(info), K(status), K(tenant_task_info));
    } else if (OB_FAIL(do_extern_backup_tenant_locality_infos(
                   info, extern_backup_info, is_force_stop, extern_tenant_locality_info))) {
      LOG_WARN("failed to do extern backup tenant locality infos", K(ret), K(info), K(extern_backup_info));
    } else if (OB_FAIL(do_extern_backup_set_infos(
                   info, tenant_task_info, extern_backup_info, is_force_stop, extern_backup_set_info))) {
      LOG_WARN("failed to do extern backup set infos", K(ret), K(info), K(tenant_task_info), K(extern_backup_info));
    } else if (OB_FAIL(do_extern_diagnose_info(
                   info, extern_backup_info, extern_backup_set_info, extern_tenant_locality_info, is_force_stop))) {
      LOG_WARN("failed to do extern diagnose info", K(ret), K(info));
    } else {
      DEBUG_SYNC(BACKUP_INFO_BEFOR_NORMAL_TENNAT_STOP);
      dest_info = info;
      dest_info.backup_status_.set_backup_status_stop();
      if (OB_FAIL(info_manager.update_backup_info(info.tenant_id_, dest_info, updater))) {
        LOG_WARN("failed to update backup info", K(info), K(dest_info));
      } else {
        wakeup();
      }

      ROOTSERVICE_EVENT_ADD("backup",
          "finish backup tenant",
          "tenant_id",
          info.tenant_id_,
          "result",
          tenant_task_info.result_,
          "total pg count",
          tenant_task_info.pg_count_,
          "finish pg count",
          tenant_task_info.finish_pg_count_,
          "start time",
          tenant_task_info.start_time_,
          "end time",
          tenant_task_info.end_time_);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(updater))) {
        LOG_WARN("failed to ccommit trans", K(ret), K(info));
      }
    } else {
      int tmp_ret = updater.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_CANCELED == tenant_task_info.result_ && ObBackupUtils::is_extern_device_error(ret)) {
      need_switch_tenant_ = false;
    }
  }
  return ret;
}

int ObRootBackup::do_cleanup_pg_backup_tasks(
    const ObTenantBackupTaskInfo& tenant_task_info, bool& all_task_deleted, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObPGBackupTaskUpdater pg_task_updater;
  int64_t affected_rows = 0;
  const int64_t MAX_DELETE_ROWS = 1024;
  all_task_deleted = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!tenant_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup task info is invalid", K(ret), K(tenant_task_info));
  } else if (OB_FAIL(pg_task_updater.init(trans))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(tenant_task_info));
  } else if (OB_FAIL(pg_task_updater.delete_all_pg_tasks(tenant_task_info.tenant_id_,
                 tenant_task_info.incarnation_,
                 tenant_task_info.backup_set_id_,
                 MAX_DELETE_ROWS,
                 affected_rows))) {
    LOG_WARN("failed to delete all pg tasks", K(ret), K(tenant_task_info));
  } else if (affected_rows > MAX_DELETE_ROWS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "delete affected rows should not be bigger than max delete rows", K(ret), K(affected_rows), K(MAX_DELETE_ROWS));
  } else if (affected_rows < MAX_DELETE_ROWS) {
    all_task_deleted = true;
  }
  return ret;
}

int ObRootBackup::do_cleanup_tenant_backup_task(const ObBaseBackupInfoStruct& info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater tenant_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup task info is invalid", K(ret), K(info));
  } else if (OB_FAIL(tenant_task_updater.init(trans))) {
    LOG_WARN("failed to init tenant task updater", K(ret), K(info));
  } else if (OB_FAIL(tenant_task_updater.remove_task(info.tenant_id_, info.incarnation_, info.backup_set_id_))) {
    LOG_WARN("failed to remove tenant backup task", K(ret), K(info));
  }
  return ret;
}

int ObRootBackup::do_insert_tenant_backup_task_his(
    const ObTenantBackupTaskInfo& tenant_task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater backup_task_history_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!tenant_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup task info is invalid", K(ret), K(tenant_task_info));
  } else if (OB_FAIL(backup_task_history_updater.init(trans))) {
    LOG_WARN("failed to init tenant task history updater", K(ret), K(tenant_task_info));
  } else if (OB_FAIL(backup_task_history_updater.insert_tenant_backup_task(tenant_task_info))) {
    LOG_WARN("failed to insert tenant backup task into his", K(ret), K(tenant_task_info));
  }
  return ret;
}

int ObRootBackup::do_cancel(const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || !info.backup_status_.is_cancel_status()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do cancel get invalid argument", K(ret), K(info));
  } else {
    if (OB_SYS_TENANT_ID == info.tenant_id_) {
      // do sys tenant cancel
      if (OB_FAIL(do_sys_tenant_cancel(info, info_manager))) {
        LOG_WARN("failed to do sys tenant cancel", K(ret), K(info));
      }
    } else {
      // do normal tenant cancel
      if (OB_FAIL(do_normal_tenant_cancel(info, info_manager))) {
        LOG_WARN("failed to do normal tenant cancel", K(ret), K(info));
      }
    }
  }
  return ret;
}

int ObRootBackup::get_tenant_backup_task(
    ObMySQLTransaction& trans, const share::ObBaseBackupInfoStruct& info, share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater tenant_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(
                 get_tenant_backup_task(info.tenant_id_, info.backup_set_id_, info.incarnation_, trans, task_info))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(info));
  }
  return ret;
}

int ObRootBackup::get_tenant_backup_task(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t incarnation, common::ObISQLClient& trans, share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater tenant_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation <= 0 || backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(tenant_task_updater.init(trans))) {
    LOG_WARN("failed to init tenant task updater", K(ret), K(tenant_id), K(backup_set_id), K(incarnation));
  } else if (OB_FAIL(tenant_task_updater.get_tenant_backup_task(tenant_id, backup_set_id, incarnation, task_info))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootBackup::insert_tenant_backup_task(
    ObMySQLTransaction& trans, const ObBaseBackupInfoStruct& info, const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater tenant_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || !info.backup_status_.is_scheduler_status()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert tenant backup task get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(tenant_task_updater.init(trans))) {
    LOG_WARN("failed to init tenant backup task updater", K(ret), K(info));
  } else if (OB_FAIL(tenant_task_updater.insert_tenant_backup_task(info, extern_backup_info))) {
    LOG_WARN("failed to insert tenant backup task", K(ret), K(info));
  }
  return ret;
}

int ObRootBackup::update_tenant_backup_info(const share::ObBaseBackupInfoStruct& src_info,
    const share::ObBaseBackupInfoStruct& dest_info, ObBackupInfoManager& info_manager,
    share::ObBackupItemTransUpdater& updater)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!src_info.is_valid() || !dest_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup info get invalid argument", K(ret), K(src_info), K(dest_info));
  } else if (OB_FAIL(info_manager.check_can_update(src_info, dest_info))) {
    LOG_WARN("failed to check can update", K(ret), K(src_info), K(dest_info));
  } else if (OB_FAIL(info_manager.update_backup_info(dest_info.tenant_id_, dest_info, updater))) {
    LOG_WARN("failed to update backup info", K(ret), K(dest_info));
  }
  return ret;
}

int ObRootBackup::get_tenant_total_partition_cnt(const uint64_t tenant_id, int64_t& total_partition_cnt)
{
  // total_partition_cnt contains both normal pg and mark dropped pg
  // here interface just use to get partition count to calc query_time and trx_time
  int ret = OB_SUCCESS;
  total_partition_cnt = 0;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2*> schemas;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("failed to get table schema in tenant", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
      const ObSimpleTableSchemaV2* schema = schemas.at(i);
      // FIXME: doesn't involve the delay delete objects
      total_partition_cnt += schema->get_all_part_num();
    }
  }
  return ret;
}

int ObRootBackup::update_extern_backup_infos(const share::ObBaseBackupInfoStruct& info,
    const ObExternBackupInfo::ExternBackupInfoStatus& status, const bool is_force_stop,
    ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObClusterBackupDest backup_dest;
  const uint64_t tenant_id = info.tenant_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (is_force_stop) {
    FLOG_INFO(
        "backup need force stop, skip update extern backup infos", K(is_force_stop), K(extern_device_error_), K(info));
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr(), info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else if (OB_FAIL(extern_backup_info_mgr.init(tenant_id, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(extern_backup_info_mgr.get_extern_backup_info(info, *freeze_info_mgr_, extern_backup_info))) {
    LOG_WARN("failed to get extern backup info", K(ret), K(info));
  } else if (FALSE_IT(extern_backup_info.status_ = status)) {
  } else if (!info.backup_status_.is_cleanup_status() &&
             OB_FAIL(extern_backup_info_mgr.check_can_backup(extern_backup_info))) {
    LOG_WARN("failed to check can backup", K(ret), K(extern_backup_info));
  } else if (OB_FAIL(extern_backup_info_mgr.upload_backup_info(extern_backup_info))) {
    LOG_WARN("failed to upload backup info", K(ret), K(extern_backup_info));
  }
  return ret;
}

int ObRootBackup::do_extern_backup_set_infos(const ObBaseBackupInfoStruct& info,
    const ObTenantBackupTaskInfo& tenant_task_info, const ObExternBackupInfo& extern_backup_info,
    const bool is_force_stop, ObExternBackupSetInfo& extern_backup_set_info)
{
  int ret = OB_SUCCESS;
  ObExternBackupSetInfoMgr extern_backup_set_info_mgr;
  ObClusterBackupDest backup_dest;
  const uint64_t tenant_id = info.tenant_id_;
  const uint64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  extern_backup_set_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || !tenant_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do extern backup set infos get invalid argument", K(ret), K(info), K(tenant_task_info));
  } else if (is_force_stop) {
    FLOG_INFO("backup is force stop, skip backup set infos", K(is_force_stop), K(extern_device_error_), K(info));
  } else if (!extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do extern backup set infos get invalid argument", K(ret), K(extern_backup_info));
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr(), info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else if (OB_FAIL(
                 extern_backup_set_info_mgr.init(tenant_id, full_backup_set_id, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set info mgr", K(ret), K(tenant_id), K(full_backup_set_id));
  } else {
    extern_backup_set_info.backup_set_id_ = extern_backup_info.inc_backup_set_id_;
    extern_backup_set_info.backup_snapshot_version_ = info.backup_snapshot_version_;
    extern_backup_set_info.cluster_version_ = extern_backup_info.cluster_version_;
    extern_backup_set_info.compatible_ = extern_backup_info.compatible_;
    extern_backup_set_info.compress_type_ = ObCompressorType::NONE_COMPRESSOR;
    extern_backup_set_info.input_bytes_ = tenant_task_info.input_bytes_;
    extern_backup_set_info.output_bytes_ = tenant_task_info.output_bytes_;
    extern_backup_set_info.macro_block_count_ = tenant_task_info.macro_block_count_;
    extern_backup_set_info.pg_count_ = tenant_task_info.pg_count_;
    if (OB_FAIL(extern_backup_set_info_mgr.upload_backup_set_info(extern_backup_set_info))) {
      LOG_WARN("failed to upload backup set info", K(ret), K(extern_backup_set_info));
    }
  }
  return ret;
}

int ObRootBackup::do_extern_backup_tenant_locality_infos(const ObBaseBackupInfoStruct& info,
    const ObExternBackupInfo& extern_backup_info, const bool is_force_stop,
    ObExternTenantLocalityInfo& extern_tenant_locality_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTenantSchema* tenant_info = NULL;
  ObExternTenantLocalityInfoMgr extern_tenant_locality_info_mgr;
  ObClusterBackupDest backup_dest;
  const uint64_t tenant_id = info.tenant_id_;
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  extern_tenant_locality_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (is_force_stop) {
    FLOG_INFO(
        "backup is force stop, skip backup tenant locality infos", K(is_force_stop), K(extern_device_error_), K(info));
  } else if (!extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do extern backup tenant locality infos get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr(), info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id, *schema_service_, info.backup_schema_version_, guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(info));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant info", K(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(extern_tenant_locality_info_mgr.init(
                 tenant_id, full_backup_set_id, inc_backup_set_id, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set info mgr", K(ret), K(tenant_id), K(full_backup_set_id));
  } else if (OB_FAIL(extern_tenant_locality_info.tenant_name_.assign(tenant_info->get_tenant_name()))) {
    LOG_WARN("failed to assign tenant name", K(ret), K(info));
  } else if (OB_FAIL(extern_tenant_locality_info.locality_.assign(tenant_info->get_locality()))) {
    LOG_WARN("failed to assign locality info", K(ret), K(info));
  } else if (OB_FAIL(extern_tenant_locality_info.primary_zone_.assign(tenant_info->get_primary_zone()))) {
    LOG_WARN("failed to assign primary zone", K(ret), K(info));
  } else {
    extern_tenant_locality_info.tenant_id_ = tenant_id;
    extern_tenant_locality_info.backup_set_id_ = extern_backup_info.inc_backup_set_id_;
    extern_tenant_locality_info.backup_snapshot_version_ = info.backup_snapshot_version_;
    extern_tenant_locality_info.compat_mode_ =
        static_cast<lib::Worker::CompatMode>(tenant_info->get_compatibility_mode());

    if (OB_FAIL(check_can_backup())) {
      LOG_WARN("failed ot check can backup", K(ret));
    } else if (OB_FAIL(extern_tenant_locality_info_mgr.upload_tenant_locality_info(extern_tenant_locality_info))) {
      LOG_WARN("failed to upload tenant locality info", K(ret), K(info));
    }
  }
  return ret;
}

int ObRootBackup::do_extern_tenant_infos(
    const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObExternTenantInfoMgr tenant_info_mgr;
  ObClusterBackupDest backup_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != info.tenant_id_) {
    // do nothing
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr(), info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else if (OB_FAIL(tenant_info_mgr.init(backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init tenant info mgr", K(ret), K(backup_dest));
  } else if (OB_FAIL(info_manager.get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObBaseBackupInfoStruct info;
      ObSchemaGetterGuard guard;
      const ObTenantSchema* tenant_schema = NULL;
      ObExternTenantInfo tenant_info;
      if (OB_SYS_TENANT_ID == tenant_id) {
        // do nothing
      } else if (OB_FAIL(info_manager.get_backup_info_without_trans(tenant_id, info))) {
        LOG_WARN("failed to get tenant backup info", K(ret), K(tenant_id), K(info));
      } else if (!info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup info is invalid", K(ret), K(info));
      } else if (0 == info.backup_schema_version_ && info.backup_status_.is_stop_status()) {
        // do nothing
      } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                     tenant_id, *schema_service_, info.backup_schema_version_, guard))) {
        LOG_WARN("failed to get tenant schema guard", K(ret), K(info));
      } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("failed to get tenant schema info", K(ret), K(tenant_id), K(info));
      } else if (OB_FAIL(tenant_info.tenant_name_.assign(tenant_schema->get_tenant_name()))) {
        LOG_WARN("failed to assign tenant name", K(ret), K(tenant_info), K(info));
      } else {
        tenant_info.tenant_id_ = tenant_id;
        tenant_info.delete_timestamp_ = tenant_schema->get_drop_tenant_time();
        // tenant_info.create_timestamp_
        tenant_info.compat_mode_ = static_cast<lib::Worker::CompatMode>(tenant_schema->get_compatibility_mode());
        tenant_info.backup_snapshot_version_ = info.backup_snapshot_version_;
        if (OB_FAIL(tenant_info_mgr.add_tenant_info(tenant_info))) {
          LOG_WARN("failed to add tenant info into tenant info mgr", K(ret), K(tenant_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_info_mgr.upload_tenant_infos())) {
        LOG_WARN("failed to upload tenant infos", K(ret));
      }
    }
  }
  return ret;
}

int ObRootBackup::get_stopped_backup_tenant_task_infos(
    const common::ObIArray<share::ObBaseBackupInfoStruct>& tenant_backup_infos,
    common::ObIArray<share::ObTenantBackupTaskInfo>& tenant_task_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_backup_infos.count(); ++i) {
      const ObBaseBackupInfoStruct& info = tenant_backup_infos.at(i);
      ObTenantBackupTaskUpdater updater;
      ObTenantBackupTaskInfo tenant_task_info;
      if (!info.backup_status_.is_stop_status()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup info backup status is unexpected", K(ret), K(info));
      } else if (OB_FAIL(updater.init(*sql_proxy_))) {
        LOG_WARN("failed to init tenant backup task updater", K(ret), K(info));
      } else if (OB_FAIL(updater.get_tenant_backup_task(
                     info.tenant_id_, info.backup_set_id_, info.incarnation_, tenant_task_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("get not exist task info, skip it", K(info));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tenant backup task info", K(ret), K(info));
        }
      } else if (OB_FAIL(tenant_task_infos.push_back(tenant_task_info))) {
        LOG_WARN("failed to push tenant info into array", K(ret), K(tenant_task_info));
      }
    }
  }
  return ret;
}

int ObRootBackup::get_stopped_backup_tenant_result(
    const common::ObIArray<share::ObBaseBackupInfoStruct>& tenant_backup_infos, int32_t& result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  ObArray<ObTenantBackupTaskInfo> tenant_task_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(get_stopped_backup_tenant_task_infos(tenant_backup_infos, tenant_task_infos))) {
    LOG_WARN("failed to get stopped backup tenant task infos", K(ret), K(tenant_backup_infos));
  } else {
    for (int64_t i = 0; OB_SUCCESS == result && i < tenant_task_infos.count(); ++i) {
      const ObTenantBackupTaskInfo& tenant_task_info = tenant_task_infos.at(i);
      result = tenant_task_info.result_;
    }
  }
  return ret;
}

int ObRootBackup::cleanup_stopped_backup_task_infos()
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct sys_backup_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", K(ret));
  } else if (tenant_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(OB_SYS_TENANT_ID, updater, sys_backup_info))) {
      LOG_WARN("failed to get backup info", K(ret), K(OB_SYS_TENANT_ID));
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }

    if (OB_SUCC(ret)) {
      if (!sys_backup_info.backup_status_.is_stop_status() && !sys_backup_info.backup_status_.is_cleanup_status()) {
        // do nothing
      } else {
        for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          const uint64_t tenant_id = tenant_ids.at(i);
          if (OB_FAIL(cleanup_stopped_tenant_infos(tenant_id, info_manager))) {
            LOG_WARN("failed to cleanup stoppped tenant infos", K(ret), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObRootBackup::cleanup_stopped_tenant_infos(const uint64_t tenant_id, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo sys_tenant_backup_task;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct info;
  ObBaseBackupInfoStruct dest_info;
  bool need_clean = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (stop_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("observer is stopping", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is is invalid", K(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("failed to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else if (OB_SYS_TENANT_ID == info.tenant_id_) {
      need_clean = info.backup_status_.is_cleanup_status() ? true : false;
    } else if (!info.backup_status_.is_stop_status()) {
      // do nothing
      need_clean = false;
    } else if (info.has_cleaned()) {
      // do nothing
      need_clean = false;
    }

    if (OB_SUCC(ret) && need_clean) {
      DEBUG_SYNC(BACKUP_INFO_BEFOR_SYS_TENNAT_STOP);
      ObBaseBackupInfoStruct dest_info = info;
      dest_info.backup_schema_version_ = 0;
      dest_info.backup_snapshot_version_ = 0;
      dest_info.backup_data_version_ = 0;
      dest_info.backup_type_.type_ = ObBackupType::EMPTY;
      dest_info.backup_status_.set_backup_status_stop();

      if (OB_SYS_TENANT_ID == info.tenant_id_) {
        if (OB_FAIL(get_tenant_backup_task(info.tenant_id_,
                info.backup_set_id_,
                info.incarnation_,
                updater.get_trans(),
                sys_tenant_backup_task))) {
          LOG_WARN("failed to get tenant backup task", K(ret), K(info), K(tenant_id));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_SYS_TENANT_ID != info.tenant_id_ &&
                 OB_FAIL(drop_backup_point(info.tenant_id_, info.backup_snapshot_version_))) {
        LOG_WARN("failed to drop backup point", K(ret), K(info));
      } else if (OB_FAIL(do_cleanup_tenant_backup_task(info, updater.get_trans()))) {
        LOG_WARN("failed to do cleanup tenant backup task", K(ret), K(info));
      } else if (OB_FAIL(update_tenant_backup_info(info, dest_info, info_manager, updater))) {
        LOG_WARN("failed to update tenant backup info", K(ret), K(info), K(dest_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(lease_time_map_.erase_refactored(info.tenant_id_))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to erase lease time", K(ret), K(info));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }

    if (OB_SUCC(ret) && need_clean) {
      if (OB_SYS_TENANT_ID == info.tenant_id_) {
        ROOTSERVICE_EVENT_ADD("backup",
            "finish backup cluster",
            "result",
            sys_tenant_backup_task.result_,
            "start time",
            sys_tenant_backup_task.start_time_,
            "end time",
            sys_tenant_backup_task.end_time_,
            "cost time",
            sys_tenant_backup_task.end_time_ - sys_tenant_backup_task.start_time_);
      }
    }
  }
  return ret;
}

int ObRootBackup::get_lease_time(const uint64_t tenant_id, int64_t& lease_time)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get lease time get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(lease_time_map_.get_refactored(tenant_id, lease_time))) {
    LOG_WARN("failed to get lease time", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootBackup::update_lease_time(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int32_t flag = 1;
  const int64_t lease_time = ObTimeUtil::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update lease time get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(lease_time_map_.set_refactored(tenant_id, lease_time, flag))) {
    LOG_WARN("failed to set tenant lease time", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootBackup::insert_lease_time(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert lease time do not init", K(ret));
  } else {
    int64_t tmp_lease_time = 0;
    hash_ret = lease_time_map_.get_refactored(tenant_id, tmp_lease_time);
    if (OB_SUCCESS == hash_ret) {
      // do nothing
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(update_lease_time(tenant_id))) {
        LOG_WARN("failed to update lease time", K(ret), K(tenant_id));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get lease time", K(ret), K(tenant_id));
    }
  }
  return ret;
}

void ObRootBackup::update_prepare_flag(const bool is_prepare_flag)
{
  const bool old_prepare_flag = get_prepare_flag();
  if (old_prepare_flag != is_prepare_flag) {
    FLOG_INFO("update root backup prepare flag", K(old_prepare_flag), K(is_prepare_flag));
    ATOMIC_VCAS(&is_prepare_flag_, old_prepare_flag, is_prepare_flag);
  }
}

bool ObRootBackup::get_prepare_flag() const
{
  const bool prepare_flag = ATOMIC_LOAD(&is_prepare_flag_);
  return prepare_flag;
}

void ObRootBackup::cleanup_prepared_infos()
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;
  ObBaseBackupInfoStruct info;
  bool need_clean = false;
  const int64_t EXECUTE_TIMEOUT_US = 100L * 100 * 1000;      // 100ms
  const int64_t MIN_EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  int64_t calc_stmt_timeout = EXECUTE_TIMEOUT_US;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct sys_backup_info;
  ObBaseBackupInfoStruct dest_sys_backup_info;
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
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (FALSE_IT(calc_stmt_timeout = tenant_ids.count() * EXECUTE_TIMEOUT_US)) {
  } else if (FALSE_IT(stmt_timeout =
                          (calc_stmt_timeout > MIN_EXECUTE_TIMEOUT_US ? calc_stmt_timeout : MIN_EXECUTE_TIMEOUT_US))) {
  } else if (timeout_ctx.set_trx_timeout_us(stmt_timeout)) {
    LOG_WARN("failed to set trx timeout us", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(OB_SYS_TENANT_ID, updater, sys_backup_info))) {
      LOG_WARN("failed to get backup info", K(ret));
    } else if (OB_FAIL(check_need_cleanup_prepared_infos(sys_backup_info, need_clean))) {
      LOG_WARN("failed to check need clean prepared infos", K(ret));
    } else if (!need_clean) {
      // do nothing
    } else {
      LOG_INFO("clean up root backup prepare task", K(tenant_ids));
      // normal tenants, skip sys tenant
      for (int64_t i = 1; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_FAIL(cleanup_tenant_prepared_infos(tenant_id, updater.get_trans(), info_manager))) {
          LOG_WARN("failed to cleanup stoppped tenant infos", K(ret), K(tenant_id));
        }
      }

      if (OB_SUCC(ret)) {
        dest_sys_backup_info = sys_backup_info;
        dest_sys_backup_info.backup_status_.set_backup_status_stop();
        if (OB_FAIL(update_tenant_backup_info(sys_backup_info, dest_sys_backup_info, info_manager, updater))) {
          LOG_WARN("failed to update tenant backup info", K(ret), K(dest_sys_backup_info), K(sys_backup_info));
        }
      }
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
}

int ObRootBackup::check_need_cleanup_prepared_infos(
    const share::ObBaseBackupInfoStruct& sys_backup_info, bool& need_clean)
{
  int ret = OB_SUCCESS;
  need_clean = false;
  bool is_prepare_flag = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (FALSE_IT(is_prepare_flag = get_prepare_flag())) {
  } else if (sys_backup_info.backup_status_.is_prepare_status() && !is_prepare_flag) {
    need_clean = true;
    LOG_INFO("sys tenant is in scheduler status, need cleanup", K(ret), K(sys_backup_info), K(is_prepare_flag));
  } else {
    LOG_INFO("sys tenant is not in schduler status, no need cleanup", K(sys_backup_info), K(is_prepare_flag));
  }
  return ret;
}

int ObRootBackup::cleanup_tenant_prepared_infos(
    const uint64_t tenant_id, ObISQLClient& sys_tenant_trans, ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  const int64_t EXECUTE_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
  ObBackupItemTransUpdater updater;
  ObBaseBackupInfoStruct info;
  ObBaseBackupInfoStruct dest_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (stop_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("observer is stopping", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is is invalid", K(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("failed to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else if (!info.backup_status_.is_scheduler_status() && !info.backup_status_.is_stop_status()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info status is unexpected", K(ret), K(info));
    } else if (info.backup_status_.is_stop_status()) {
      // do nothing
    } else if (OB_FAIL(ObBackupUtil::check_sys_tenant_trans_alive(info_manager, sys_tenant_trans))) {
      LOG_WARN("failed to check sys tenant trans alive", K(ret), K(info));
    } else if (OB_FAIL(drop_backup_point(tenant_id, info.backup_snapshot_version_))) {
      LOG_WARN("failed to drop backup point", K(ret), K(info));
    } else {
      dest_info = info;
      dest_info.backup_status_.set_backup_status_stop();
      if (OB_FAIL(update_tenant_backup_info(info, dest_info, info_manager, updater))) {
        LOG_WARN("failed to update tenant backup info", K(ret), K(info), K(dest_info));
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

int ObRootBackup::check_tenants_backup_task_failed(
    const ObBaseBackupInfoStruct& info, ObBackupInfoManager& info_manager, common::ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  bool has_failed_task = false;
  ObTenantBackupTaskInfo sys_task_info;
  ObTenantBackupTaskInfo sys_dest_task_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || OB_SYS_TENANT_ID != info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tenants backup failed", K(info));
  } else if (OB_FAIL(info_manager.get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret), K(info));
  } else if (OB_FAIL(get_tenant_backup_task(
                 info.tenant_id_, info.backup_set_id_, info.incarnation_, sys_tenant_trans, sys_task_info))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(info));
  } else {
    for (int64_t i = 0; !has_failed_task && OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObTenantBackupTaskInfo task_info;
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_SUCCESS != inner_error_ || !sys_task_info.is_result_succeed()) {
          has_failed_task = true;
          if (!sys_task_info.is_result_succeed()) {
            // do nothing
          } else {
            sys_dest_task_info = sys_task_info;
            sys_dest_task_info.result_ = inner_error_;
            if (OB_FAIL(update_tenant_backup_task(sys_tenant_trans, sys_task_info, sys_dest_task_info))) {
              LOG_WARN("failed to update tenant backup task", K(ret), K(sys_task_info), K(sys_dest_task_info));
            }
          }
        }
      } else if (OB_FAIL(get_tenant_backup_task(
                     tenant_id, info.backup_set_id_, info.incarnation_, *sql_proxy_, task_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("get not exist task info, skip it", K(tenant_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tenant backup task", K(ret), K(info), K(tenant_id));
        }
      } else if (!task_info.is_result_succeed()) {
        has_failed_task = true;
        if (!sys_task_info.is_result_succeed()) {
          // do nothing
        } else {
          sys_dest_task_info = sys_task_info;
          sys_dest_task_info.result_ = task_info.result_;
          if (OB_FAIL(update_tenant_backup_task(sys_tenant_trans, sys_task_info, sys_dest_task_info))) {
            LOG_WARN("failed to update tenant backup task", K(ret), K(sys_task_info), K(sys_dest_task_info));
          }
        }
      }
    }

    if (OB_SUCC(ret) && has_failed_task) {
      // update TenantBackupTaskStatus::CANCEL
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        ObMySQLTransaction trans;
        ObTenantBackupTaskInfo task_info;
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(trans.start(sql_proxy_))) {
          OB_LOG(WARN, "fail to start trans", K(ret));
        } else {
          if (OB_FAIL(get_tenant_backup_task(tenant_id, info.backup_set_id_, info.incarnation_, trans, task_info))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              LOG_INFO("get not exist task info, skip it", K(tenant_id));
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get tenant backup task", K(ret), K(info), K(tenant_id));
            }
          } else if (ObTenantBackupTaskInfo::CANCEL == task_info.status_ ||
                     ObTenantBackupTaskInfo::FINISH == task_info.status_) {
            // do nothing
          } else {
            ObTenantBackupTaskInfo dest_task_info = task_info;
            dest_task_info.status_ = ObTenantBackupTaskInfo::CANCEL;
            dest_task_info.result_ = OB_CANCELED;
            if (OB_FAIL(ObBackupUtil::check_sys_tenant_trans_alive(info_manager, sys_tenant_trans))) {
              LOG_WARN("failed to check sys tenant trans alive", K(ret), K(tenant_id));
            } else if (OB_FAIL(update_tenant_backup_task(trans, task_info, dest_task_info))) {
              LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(dest_task_info));
            }
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
        }
      }
    }
  }
  return ret;
}

int ObRootBackup::update_tenant_backup_task(common::ObISQLClient& trans, const share::ObTenantBackupTaskInfo& src_info,
    const share::ObTenantBackupTaskInfo& dest_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!src_info.is_valid() || !dest_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup task get invalid argument", K(ret), K(src_info), K(dest_info));
  } else if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init tenant backup task updater", K(ret), K(src_info));
  } else if (OB_FAIL(updater.update_tenant_backup_task(src_info, dest_info))) {
    LOG_WARN("failed to updat tenant backup task", K(ret), K(src_info), K(dest_info));
  }
  return ret;
}

int ObRootBackup::do_normal_tenant_cancel(
    const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  LOG_INFO("start do normal tenant cancel", K(info));
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObTenantBackup tenant_backup;
  share::ObBackupItemTransUpdater updater;
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || OB_SYS_TENANT_ID == info.tenant_id_ || !info.backup_status_.is_cancel_status()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
    LOG_WARN("failed to start trans", K(ret), K(info));
  } else {
    if (OB_FAIL(add_backup_info_lock(info, updater, info_manager))) {
      LOG_WARN("failed to add backup info lock", K(ret), K(info));
    } else if (OB_FAIL(get_tenant_backup_task(updater.get_trans(), info, task_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get tenant backup task", K(ret), K(info));
      } else {
        // task info may be invalid cause task info do not insert when backup info in scheduler status
        ret = OB_SUCCESS;
        if (OB_FAIL(insert_tenant_backup_task_failed(updater.get_trans(), info, task_info))) {
          LOG_WARN("failed to insert tenant backup task failed", K(ret), K(info), K(task_info));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ObTenantBackupTaskInfo::FINISH == task_info.status_) {
      // update backup info cleanup
      ObBaseBackupInfoStruct dest_info = info;
      dest_info.backup_status_.set_backup_status_cleanup();
      if (OB_CANCELED == task_info.result_) {
        // do nothing
      } else {
        ObTenantBackupTaskInfo dest_task_info = task_info;
        dest_task_info.result_ = OB_CANCELED;
        if (OB_FAIL(update_tenant_backup_task(updater.get_trans(), task_info, dest_task_info))) {
          LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(dest_task_info));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(update_tenant_backup_info(info, dest_info, info_manager, updater))) {
        LOG_WARN("failed to update tenant backup info", K(ret), K(info), K(dest_info));
      } else {
        wakeup();
      }
    } else if (ObTenantBackupTaskInfo::CANCEL != task_info.status_) {
      ObTenantBackupTaskInfo dest_task_info = task_info;
      dest_task_info.status_ = ObTenantBackupTaskInfo::CANCEL;
      dest_task_info.result_ = OB_CANCELED;
      if (OB_FAIL(update_tenant_backup_task(updater.get_trans(), task_info, dest_task_info))) {
        LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(dest_task_info));
      } else {
        wakeup();
      }
    } else {
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(updater))) {
        LOG_WARN("failed to commit trans", K(ret), K(info));
      }
    } else {
      int tmp_ret = updater.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }

    if (OB_SUCC(ret) && ObTenantBackupTaskInfo::CANCEL == task_info.status_) {
      if (OB_FAIL(tenant_backup.init(info,
              *schema_service_,
              *root_balancer_,
              *sql_proxy_,
              *server_mgr_,
              *rebalancer_mgr_,
              *rpc_proxy_,
              *this,
              *backup_lease_service_))) {
        LOG_WARN("failed to init tenant backup", K(ret), K(info));
      } else if (OB_FAIL(tenant_backup.do_backup())) {
        LOG_WARN("failed to do tenant backup", K(ret), K(info), K(task_info));
      }
    }
  }
  return ret;
}

int ObRootBackup::do_sys_tenant_cancel(
    const share::ObBaseBackupInfoStruct& info, share::ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObTenantBackupTaskInfo sys_backup_task;
  share::ObBackupItemTransUpdater updater;
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || OB_SYS_TENANT_ID != info.tenant_id_ || !info.backup_status_.is_cancel_status()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sys tenant cancel get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(start_trans(timeout_ctx, updater))) {
    LOG_WARN("failed to start trans", K(ret), K(info));
  } else {
    if (OB_FAIL(add_backup_info_lock(info, updater, info_manager))) {
      LOG_WARN("failed to add backup info lock", K(ret), K(info));
    } else if (OB_FAIL(get_tenant_backup_task(updater.get_trans(), info, sys_backup_task))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get tenant backup task", K(ret), K(info));
      } else {
        // overwrite ret
        if (OB_FAIL(insert_tenant_backup_task_failed(updater.get_trans(), info, sys_backup_task))) {
          OB_LOG(WARN, "failed to insert tenant backup task", K(ret), K(info));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(info_manager.get_tenant_ids(tenant_ids))) {
      LOG_WARN("failed to get tenant ids", K(ret), K(info));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(set_normal_tenant_cancel(tenant_id, info_manager, updater.get_trans()))) {
          LOG_WARN("failed to set normal tenant cancel", K(ret), K(tenant_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(do_with_all_finished_info(info, updater, info_manager))) {
          LOG_WARN("failed to do with all finished info", K(ret), K(info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(updater))) {
        LOG_WARN("failed to commit trans", K(ret), K(info));
      }
    } else {
      int tmp_ret = updater.end(false /*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObRootBackup::set_normal_tenant_cancel(
    const uint64_t tenant_id, share::ObBackupInfoManager& sys_info_manager, common::ObISQLClient& sys_tenant_tran)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBackupItemTransUpdater updater;
  ObBackupInfoManager info_manager;
  ObBaseBackupInfoStruct info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set normal tenant cancel get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
    LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else if (info.backup_status_.is_stop_status() || info.backup_status_.is_cancel_status()) {
      // do nothing
    } else if (info.backup_status_.is_cleanup_status()) {
      ObTenantBackupTaskInfo backup_task;
      if (OB_FAIL(get_tenant_backup_task(updater.get_trans(), info, backup_task))) {
        LOG_WARN("failed to get tenant backup task", K(ret), K(info));
      } else if (ObTenantBackupTaskInfo::FINISH != backup_task.status_) {
        // do nothing
      } else if (OB_CANCELED != backup_task.result_) {
        ObTenantBackupTaskInfo dest_backup_task = backup_task;
        dest_backup_task.result_ = OB_CANCELED;
        if (OB_FAIL(update_tenant_backup_task(updater.get_trans(), backup_task, dest_backup_task))) {
          LOG_WARN("failed to update tenant backup task", K(ret), K(backup_task), K(dest_backup_task));
        }
      }
    } else {
      ObBaseBackupInfoStruct dest_backup_info = info;
      dest_backup_info.backup_status_.set_backup_status_cancel();
      if (OB_FAIL(ObBackupUtil::check_sys_tenant_trans_alive(sys_info_manager, sys_tenant_tran))) {
        LOG_WARN("failed to check sys tenant trans alive", K(ret), K(tenant_id));
      } else if (OB_FAIL(info_manager.update_backup_info(dest_backup_info.tenant_id_, dest_backup_info, updater))) {
        LOG_WARN("failed to update backup info", K(ret), K(dest_backup_info));
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

int ObRootBackup::update_sys_tenant_backup_task(
    ObMySQLTransaction& trans, const share::ObBaseBackupInfoStruct& info, const int32_t result)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo src_backup_task;
  ObTenantBackupTaskInfo dest_backup_task;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update sys tenant backup task get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(get_tenant_backup_task(trans, info, src_backup_task))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(info));
  } else if (ObTenantBackupTaskInfo::DOING != src_backup_task.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tenant backup task status is unexpected", K(ret), K(src_backup_task));
  } else {
    dest_backup_task = src_backup_task;
    dest_backup_task.result_ = OB_SUCCESS == src_backup_task.result_ ? result : src_backup_task.result_;
    dest_backup_task.end_time_ = ObTimeUtil::current_time();
    dest_backup_task.status_ = ObTenantBackupTaskInfo::FINISH;
    if (OB_FAIL(update_tenant_backup_task(trans, src_backup_task, dest_backup_task))) {
      LOG_WARN("failed to update tenant backup task", K(ret), K(src_backup_task), K(dest_backup_task));
    } else if (OB_FAIL(do_insert_tenant_backup_task_his(dest_backup_task, trans))) {
      LOG_WARN("failed to do insert tenant backup task his", K(ret), K(dest_backup_task));
    }
  }
  return ret;
}

int ObRootBackup::check_can_backup()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  }
  return ret;
}

int ObRootBackup::do_extern_diagnose_info(const ObBaseBackupInfoStruct& info,
    const ObExternBackupInfo& extern_backup_info, const ObExternBackupSetInfo& extern_backup_set_info,
    const ObExternTenantLocalityInfo& tenant_locality_info, const bool is_force_stop)
{
  int ret = OB_SUCCESS;
  ObExternTenantBackupDiagnoseMgr extern_backup_diagnose_mgr;
  ObClusterBackupDest backup_dest;
  const uint64_t tenant_id = tenant_locality_info.tenant_id_;
  const uint64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  ObExternBackupDiagnoseInfo diagnose_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do extern diagnose info get invalid argument", K(ret), K(info));
  } else if (is_force_stop) {
    FLOG_INFO("backup is force stop, skip backup diagnose info", K(ret), K(info));
  } else if (!extern_backup_info.is_valid() || !extern_backup_set_info.is_valid() || !tenant_locality_info.is_valid()) {
    LOG_WARN("do extern diagnose info get invalid argument",
        K(ret),
        K(extern_backup_info),
        K(extern_backup_set_info),
        K(tenant_locality_info));
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr(), info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else if (OB_FAIL(extern_backup_diagnose_mgr.init(
                 tenant_id, full_backup_set_id, inc_backup_set_id, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup diagnose info mgr", K(ret), K(tenant_id), K(full_backup_set_id));
  } else {
    diagnose_info.extern_backup_info_ = extern_backup_info;
    diagnose_info.backup_set_info_ = extern_backup_set_info;
    diagnose_info.tenant_id_ = tenant_id;
    diagnose_info.tenant_locality_info_ = tenant_locality_info;
    if (OB_FAIL(extern_backup_diagnose_mgr.upload_tenant_backup_diagnose_info(diagnose_info))) {
      LOG_WARN("failed to upload backup set info", K(ret), K(extern_backup_set_info));
    }
  }
  return ret;
}

int ObRootBackup::check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped)
{
  int ret = OB_SUCCESS;
  is_dropped = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tenant exist get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("failed tocheck if tenant has been dropped", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRootBackup::do_with_all_finished_info(const share::ObBaseBackupInfoStruct& info,
    share::ObBackupItemTransUpdater& sys_updater, share::ObBackupInfoManager& info_manager)
{
  // TODO() fix it later, use tenant backup task check result
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBaseBackupInfoStruct tmp_info;
  ObArray<ObBaseBackupInfoStruct> infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid() || OB_SYS_TENANT_ID != info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(info_manager.get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret), K(info));
  } else {
    bool all_tenant_finish = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count() && all_tenant_finish; ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObBackupItemTransUpdater updater;
      if (OB_SYS_TENANT_ID == tenant_id) {
        // do nothing
      } else if (OB_FAIL(updater.start(*sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret), K(tenant_id));
      } else {
        if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, tmp_info))) {
          LOG_WARN("failed to get backup info without trans", K(ret), K(tenant_id));
        } else if (!tmp_info.backup_status_.is_stop_status()) {
          all_tenant_finish = false;
        } else if (OB_FAIL(infos.push_back(tmp_info))) {
          LOG_WARN("failed to push backup info into array", K(ret), K(tmp_info));
        }

        int tmp_ret = updater.end(OB_SUCC(ret));
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (all_tenant_finish) {
        // TODO backup set sys tenant backup stop
        ObBaseBackupInfoStruct dest_info = info;
        dest_info.backup_status_.set_backup_status_cleanup();
        ObExternBackupInfo extern_backup_info;
        ObExternBackupInfo::ExternBackupInfoStatus status;
        int32_t result = OB_SUCCESS;
        bool is_force_stop = false;

        if (OB_FAIL(get_stopped_backup_tenant_result(infos, result))) {
          LOG_WARN("failed to get stopped backup tenant result", K(ret), K(infos));
        } else if (FALSE_IT(
                       status = (OB_SUCCESS == result ? ObExternBackupInfo::SUCCESS : ObExternBackupInfo::FAILED))) {
        } else if (FALSE_IT(is_force_stop = (OB_CANCELED == result))) {
        } else if (OB_FAIL(update_extern_backup_infos(info, status, is_force_stop, extern_backup_info))) {
          LOG_WARN("failed to do extern backup infos", K(ret), K(info), K(status));
        } else if (OB_FAIL(update_sys_tenant_backup_task(sys_updater.get_trans(), info, result))) {
          LOG_WARN("failed to update sys tenant backup task", K(ret), K(info));
        } else if (OB_FAIL(update_tenant_backup_info(info, dest_info, info_manager, sys_updater))) {
          LOG_WARN("failed to update tenant backup info", K(ret), K(info), K(dest_info));
        } else {
          wakeup();
        }
      }
    }
  }
  return ret;
}

int ObRootBackup::update_tenant_backup_meta_info(
    const ObPartitionKey& pkey, const int64_t pg_count, const int64_t partition_count)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (pg_count < 0 || partition_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup meta info get invalid argument", K(ret), K(pg_count), K(partition_count));
  } else {
    backup_meta_info_.reset();
    backup_meta_info_.pg_count_ = pg_count;
    backup_meta_info_.partition_count_ = partition_count;
    backup_meta_info_.break_point_info_.pkey_ = pkey;
  }
  return ret;
}

void ObRootBackup::reset_tenant_backup_meta_info()
{
  backup_meta_info_.reset();
}

int ObRootBackup::get_tenant_backup_meta_info(ObTenantBackupMetaInfo& meta_info)
{
  int ret = OB_SUCCESS;
  meta_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else {
    meta_info = backup_meta_info_;
  }
  return ret;
}

int ObRootBackup::check_tenant_backup_inner_error(const share::ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  int64_t global_broadcase_version = 0;
  const int64_t MAX_ALLOW_BACKUP_DATA_VERSION_GAP = 16;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_SUCCESS != inner_error_) {
    // do nothing
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcase_version))) {
    LOG_WARN("failed to get global broacase version", K(ret), K(info));
  } else {
    if (global_broadcase_version - info.backup_data_version_ >= MAX_ALLOW_BACKUP_DATA_VERSION_GAP) {
      inner_error_ = OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT;
      FLOG_WARN("global data version is larger than backup data gap",
          K(ret),
          K(global_broadcase_version),
          K(info),
          K(MAX_ALLOW_BACKUP_DATA_VERSION_GAP));
    }

#ifdef ERRSIM
    ret = E(EventTable::EN_BACKUP_DATA_VERSION_GAP_OVER_LIMIT) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      inner_error_ = ret;
      ret = OB_SUCCESS;
    }
#endif
  }
  return ret;
}

int ObRootBackup::insert_tenant_backup_task_failed(ObMySQLTransaction& trans, const share::ObBaseBackupInfoStruct& info,
    share::ObTenantBackupTaskInfo& tenant_backup_task)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObTenantBackupTaskUpdater tenant_task_updater;
  tenant_backup_task.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert tenant backup task failed get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(backup_dest.set(info.backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret), K(info));
  } else {
    tenant_backup_task.tenant_id_ = info.tenant_id_;
    tenant_backup_task.backup_set_id_ = info.backup_set_id_;
    tenant_backup_task.incarnation_ = info.incarnation_;
    tenant_backup_task.backup_type_ = info.backup_type_;
    tenant_backup_task.device_type_ = backup_dest.device_type_;
    tenant_backup_task.start_time_ = info.backup_snapshot_version_;
    tenant_backup_task.end_time_ = 0;
    tenant_backup_task.status_ =
        info.tenant_id_ == OB_SYS_TENANT_ID ? ObTenantBackupTaskInfo::DOING : ObTenantBackupTaskInfo::FINISH;
    tenant_backup_task.snapshot_version_ = info.backup_snapshot_version_;
    tenant_backup_task.cluster_id_ = GCONF.cluster_id;
    tenant_backup_task.backup_dest_ = backup_dest;
    tenant_backup_task.backup_schema_version_ = info.backup_schema_version_;
    tenant_backup_task.encryption_mode_ = info.encryption_mode_;
    tenant_backup_task.passwd_ = info.passwd_;
    tenant_backup_task.result_ = OB_CANCELED;
    if (OB_FAIL(tenant_task_updater.init(trans))) {
      LOG_WARN("failed to init tenant backup task updater", K(ret), K(info));
    } else if (OB_FAIL(tenant_task_updater.insert_tenant_backup_task(tenant_backup_task))) {
      LOG_WARN("failed to insert tenant backup task", K(ret), K(info), K(tenant_backup_task));
    }
  }
  return ret;
}

int ObRootBackup::drop_backup_point(const uint64_t tenant_id, const int64_t backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("drop backup point get invalid argument", K(ret), K(tenant_id), K(backup_snapshot_version));
  } else if (OB_FAIL(restore_point_service_->drop_backup_point(tenant_id, backup_snapshot_version))) {
    if (OB_ERR_BACKUP_POINT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to drop backup point", K(ret), K(tenant_id), K(backup_snapshot_version));
    }
  } else {
    FLOG_INFO("succeed drop backup point", K(tenant_id), K(backup_snapshot_version));
  }
  return ret;
}

int ObRootBackup::commit_trans(share::ObBackupItemTransUpdater& updater)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(check_can_backup())) {
    LOG_WARN("failed to check can backup", K(ret));
  }

  tmp_ret = updater.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObRootBackup::add_backup_info_lock(const share::ObBaseBackupInfoStruct& info,
    share::ObBackupItemTransUpdater& updater, share::ObBackupInfoManager& info_manager)
{
  int ret = OB_SUCCESS;
  ObBaseBackupInfoStruct src_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert tenant backup task failed get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(info_manager.get_backup_info(info.tenant_id_, updater, src_info))) {
    LOG_WARN("failed to get backup info", K(ret), K(info));
  } else if (OB_FAIL(info.check_backup_info_match(src_info))) {
    LOG_WARN("backup info has been changed", K(ret), K(info), K(src_info));
  }
  return ret;
}

int ObRootBackup::start_trans(ObTimeoutCtx& timeout_ctx, share::ObBackupItemTransUpdater& updater)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_EXECUTE_TIMEOUT_US = 600L * 1000 * 1000;  // 600s
  int64_t stmt_timeout = MAX_EXECUTE_TIMEOUT_US;
  if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  }
  return ret;
}

ObTenantBackup::ObTenantBackup()
    : is_inited_(false),
      schema_service_(NULL),
      sql_proxy_(NULL),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      incarnation_(0),
      backup_snapshot_version_(0),
      backup_schema_version_(0),
      backup_type_(),
      total_pg_count_(0),
      root_balancer_(NULL),
      server_mgr_(NULL),
      rebalancer_mgr_(NULL),
      rpc_proxy_(NULL),
      root_backup_(NULL),
      backup_dest_(),
      total_partition_count_(0),
      backup_lease_service_(NULL)
{}

int ObTenantBackup::init(const ObBaseBackupInfoStruct& info, share::schema::ObMultiVersionSchemaService& schema_service,
    ObRootBalancer& root_balancer, common::ObMySQLProxy& sql_proxy, ObServerManager& server_mgr,
    ObRebalanceTaskMgr& rebalancer_mgr, obrpc::ObSrvRpcProxy& rpc_proxy, ObRootBackup& root_backup,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup init twice", K(ret));
  } else if (!info.is_valid() || (!info.backup_status_.is_doing_status() && !info.backup_status_.is_cancel_status())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup get invalid argument", K(ret), K(info));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    tenant_id_ = info.tenant_id_;
    backup_set_id_ = info.backup_set_id_;
    incarnation_ = info.incarnation_;
    backup_set_id_ = info.backup_set_id_;
    backup_snapshot_version_ = info.backup_snapshot_version_;
    backup_schema_version_ = info.backup_schema_version_;
    backup_type_ = info.backup_type_;
    backup_dest_ = info.backup_dest_;
    root_balancer_ = &root_balancer;
    server_mgr_ = &server_mgr;
    rebalancer_mgr_ = &rebalancer_mgr;
    rpc_proxy_ = &rpc_proxy;
    root_backup_ = &root_backup;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackup::get_tenant_backup_task_info(ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  share::ObTenantBackupTaskUpdater tenant_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(tenant_task_updater.init(trans))) {
    LOG_WARN("failed to init tenant task updater", K(ret));
  } else if (OB_FAIL(tenant_task_updater.get_tenant_backup_task(tenant_id_, backup_set_id_, incarnation_, task_info))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id_), K(backup_set_id_), K(incarnation_));
  } else if (!task_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info should be valid", K(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackup::do_backup()
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do init", K(ret));
  } else if (OB_FAIL(start_trans(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(get_tenant_backup_task_info(task_info, trans))) {
      LOG_WARN("failed to get tenant backup task info", K(ret), K(tenant_id_), K(backup_set_id_), K(incarnation_));
    } else {
      const ObTenantBackupTaskInfo::BackupStatus& status = task_info.status_;
      switch (status) {
        case ObTenantBackupTaskInfo::GENERATE:
          if (OB_FAIL(do_generate(task_info, trans))) {
            LOG_WARN("failed to do generate", K(ret), K(task_info));
          }
          break;
        case ObTenantBackupTaskInfo::DOING:
          if (OB_FAIL(do_backup(task_info, trans))) {
            LOG_WARN("failed to do backup", K(ret), K(task_info));
          }
          break;
        case ObTenantBackupTaskInfo::FINISH:
          if (OB_FAIL(do_finish(task_info))) {
            LOG_WARN("failed to do finish ", K(ret), K(task_info));
          }
          break;
        case ObTenantBackupTaskInfo::CANCEL:
          if (OB_FAIL(do_cancel(task_info, trans))) {
            LOG_WARN("failed to do cancel", K(ret), K(task_info));
          }
          break;
        case ObTenantBackupTaskInfo::MAX:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant backup task info status is invalid", K(ret), K(task_info));
          break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans(trans))) {
        LOG_WARN("failed to commit trans", K(ret), K(task_info));
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

int ObTenantBackup::do_generate(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  int64_t MAX_BATCH_GENERATE_TASK_NUM = 1024;
  ObArray<share::ObPGBackupTaskInfo> pg_backup_task_infos;
  ObArray<uint64_t> tablegroup_ids;
  ObTenantBackupTaskInfo updater_info;
  ObBreakPointPGInfo point_pg_info;
  bool is_finish = false;
  share::ObPGBackupTaskUpdater pg_task_updater;

#ifdef ERRSIM

  ret = E(EventTable::EN_ROOT_BACKUP_MAX_GENERATE_NUM) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    MAX_BATCH_GENERATE_TASK_NUM = 10;
    ret = OB_SUCCESS;
  }
#endif

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::GENERATE != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do start get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_breakpoint_pg_info(point_pg_info))) {
    LOG_WARN("failed to get break point pg info", K(ret), K(task_info));
  } else if (OB_FAIL(generate_standalone_backup_task(
                 task_info, point_pg_info, MAX_BATCH_GENERATE_TASK_NUM, pg_backup_task_infos))) {
    LOG_WARN("failed to generate standalone backup task", K(ret), K(task_info));
  } else if (OB_FAIL(generate_tablegroup_backup_task(
                 task_info, point_pg_info, MAX_BATCH_GENERATE_TASK_NUM, pg_backup_task_infos, is_finish))) {
    LOG_WARN("failed to generate tablegroup backup task", K(ret), K(task_info));
  }

  if (OB_SUCC(ret)) {
    if (pg_backup_task_infos.empty()) {
      // do noting
    } else if (OB_FAIL(pg_task_updater.init(trans))) {
      LOG_WARN("failed to init pg task updater", K(ret), K(task_info));
    } else if (OB_FAIL(pg_task_updater.batch_report_pg_task(pg_backup_task_infos))) {
      LOG_WARN("failed to batch report pg backup task", K(ret), K(task_info));
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_finish) {
      updater_info = task_info;
      updater_info.pg_count_ = total_pg_count_;
      updater_info.partition_count_ = total_partition_count_;
      updater_info.result_ = OB_SUCCESS;
      if (OB_FAIL(update_tenant_backup_task(task_info, updater_info, trans))) {
        LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(updater_info));
      }
    } else {
      updater_info = task_info;
      updater_info.pg_count_ = total_pg_count_;
      updater_info.partition_count_ = total_partition_count_;
      updater_info.status_ = ObTenantBackupTaskInfo::DOING;
      updater_info.result_ = OB_SUCCESS;
      if (OB_FAIL(upload_pg_list(task_info))) {
        LOG_WARN("failed to upload pg list", K(ret), K(task_info));
      }

      DEBUG_SYNC(BACKUP_TASK_BEFOR_DOING);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(update_tenant_backup_task(task_info, updater_info, trans))) {
        LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(updater_info));
      } else {
        root_backup_->reset_tenant_backup_meta_info();
        root_balancer_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantBackup::generate_tablegroup_backup_task(const share::ObTenantBackupTaskInfo& task_info,
    const ObBreakPointPGInfo& point_pg_info, const int64_t max_batch_generate_task_num,
    ObIArray<share::ObPGBackupTaskInfo>& pg_backup_task_infos, bool& is_finish)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tablegroup_ids;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  is_finish = false;
  int64_t table_id_index = 0;
  ObBreakPointPGInfo point_info = point_pg_info;
  bool curr_pkey_iter_finish = true;
  int64_t table_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::GENERATE != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do start get invalid argument", K(ret), K(task_info));
  } else if (point_info.is_standalone_key()) {
    LOG_INFO("break point pg info is standalone pkey, skip generate tablegroup task", K(point_info));
    point_info.reset();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 task_info.tenant_id_, *schema_service_, backup_schema_version_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(task_info));
  } else if (OB_FAIL(schema_guard.get_tablegroup_ids_in_tenant(task_info.tenant_id_, tablegroup_ids))) {
    LOG_WARN("fail to get tablegroup ids", K(ret));
  } else if (OB_FAIL(find_break_table_id_index(tablegroup_ids, point_info, table_id_index))) {
    LOG_WARN("failed to find break table id index", K(ret), K(tablegroup_ids), K(point_info));
  } else {
    for (; OB_SUCC(ret) && table_id_index < tablegroup_ids.count() &&
           pg_backup_task_infos.count() <= max_batch_generate_task_num;
         ++table_id_index) {
      curr_pkey_iter_finish = false;
      const uint64_t tablegroup_id = tablegroup_ids.at(table_id_index);
      if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(task_info), K(tablegroup_id));
      } else if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema is NULL", K(ret), KP(tablegroup_schema), K(task_info), K(tablegroup_id));
      } else if (!tablegroup_schema->get_binding() || tablegroup_schema->is_dropped_schema()) {
        curr_pkey_iter_finish = true;
        LOG_INFO("tablegroup is not binding or is dropped schema", K(*tablegroup_schema));
      } else if (OB_FAIL(
                     get_table_count_with_partition(task_info.tenant_id_, tablegroup_id, schema_guard, table_count))) {
        LOG_WARN("failed to get table count with partition", K(ret), K(task_info), K(tablegroup_id));
      } else {
        ObPGKey pg_key;
        bool check_dropped_schema = false;
        ObTablegroupPartitionKeyIter pkey_iter(*tablegroup_schema, check_dropped_schema);
        ObPGBackupTaskInfo pg_backup_task_info;
        if (OB_FAIL(find_tg_partition_index(point_info, pkey_iter))) {
          LOG_WARN("failed to find tg partition index", K(ret), K(point_info), K(tablegroup_id));
        } else {
          while (OB_SUCC(ret) && pg_backup_task_infos.count() <= max_batch_generate_task_num &&
                 OB_SUCC(pkey_iter.next_partition_key_v2(pg_key))) {
            if (OB_FAIL(inner_generate_pg_backup_task(task_info, pg_key, pg_backup_task_info))) {
              LOG_WARN("failed to generate pg backup task", K(ret), K(task_info), K(pg_key));
            } else if (OB_FAIL(pg_backup_task_infos.push_back(pg_backup_task_info))) {
              LOG_WARN("failed to add pg backup task info", K(ret), K(pg_backup_task_info), K(pg_key));
            } else {
              ++total_pg_count_;
              total_partition_count_ += table_count;
            }
          }
          if (OB_ITER_END == ret) {
            curr_pkey_iter_finish = true;
            ret = OB_SUCCESS;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(
                    root_backup_->update_tenant_backup_meta_info(pg_key, total_pg_count_, total_partition_count_))) {
              LOG_WARN("failed to update tenant backup meta info", K(ret), K(pg_key), K(task_info));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (table_id_index == tablegroup_ids.count() && curr_pkey_iter_finish) {
        is_finish = true;
      }
    }
  }
  return ret;
}

int ObTenantBackup::generate_standalone_backup_task(const share::ObTenantBackupTaskInfo& task_info,
    const ObBreakPointPGInfo& point_pg_info, const int64_t max_batch_generate_task_num,
    ObIArray<share::ObPGBackupTaskInfo>& pg_backup_task_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> table_ids;
  const ObTableSchema* table_schema = NULL;
  int64_t table_id_index = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::GENERATE != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do start get invalid argument", K(ret), K(task_info));
  } else if (point_pg_info.is_tablegroup_key()) {
    // point info is last tablegroup, just skip it
    LOG_INFO("break point pg info is tablegroup pkey, skip generate standalone task", K(point_pg_info));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 task_info.tenant_id_, *schema_service_, backup_schema_version_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(task_info));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(task_info.tenant_id_, table_ids))) {
    LOG_WARN("fail to get tablegroup ids", K(ret));
  } else if (OB_FAIL(find_break_table_id_index(table_ids, point_pg_info, table_id_index))) {
    LOG_WARN("failed to find break table id index", K(ret), K(table_ids), K(point_pg_info));
  } else {
    for (; OB_SUCC(ret) && table_id_index < table_ids.count() &&
           pg_backup_task_infos.count() <= max_batch_generate_task_num;
         ++table_id_index) {
      const uint64_t table_id = table_ids.at(table_id_index);
      bool need_backup = false;
      if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(task_info), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is NULL", K(ret), KP(table_id), K(task_info), K(table_id));
      } else if (OB_FAIL(check_standalone_table_need_backup(table_schema, need_backup))) {
        LOG_WARN("failed to check standalone table need backup", K(ret), K(table_id));
      } else if (!need_backup) {
        // do nothing
      } else {
        bool check_dropped_schema = false;
        ObTablePartitionKeyIter pkey_iter(*table_schema, check_dropped_schema);
        if (OB_FAIL(find_sd_partition_index(point_pg_info, pkey_iter))) {
          LOG_WARN("failed to find standalone partition index", K(ret), K(point_pg_info), K(table_id));
        } else {
          ObPartitionKey pkey;
          ObPGBackupTaskInfo pg_backup_task_info;
          while (OB_SUCC(ret) && pg_backup_task_infos.count() <= max_batch_generate_task_num &&
                 OB_SUCC(pkey_iter.next_partition_key_v2(pkey))) {
            if (OB_FAIL(inner_generate_pg_backup_task(task_info, pkey, pg_backup_task_info))) {
              LOG_WARN("failed to generate pg backup task", K(ret), K(task_info), K(pkey));
            } else if (OB_FAIL(pg_backup_task_infos.push_back(pg_backup_task_info))) {
              LOG_WARN("failed to add pg backup task info", K(ret), K(pg_backup_task_info), K(pkey));
            } else {
              ++total_pg_count_;
              ++total_partition_count_;
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(root_backup_->update_tenant_backup_meta_info(pkey, total_pg_count_, total_partition_count_))) {
              LOG_WARN("failed to update tenant backup meta info", K(ret), K(pkey), K(task_info));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantBackup::inner_generate_pg_backup_task(const share::ObTenantBackupTaskInfo& task_info, const ObPGKey& pg_key,
    share::ObPGBackupTaskInfo& pg_backup_task_info)
{
  int ret = OB_SUCCESS;
  pg_backup_task_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate pg backup task get invalid argument", K(ret), K(task_info), K(pg_key));
  } else {
    pg_backup_task_info.tenant_id_ = tenant_id_;
    pg_backup_task_info.table_id_ = pg_key.get_table_id();
    pg_backup_task_info.partition_id_ = pg_key.get_partition_id();
    pg_backup_task_info.backup_set_id_ = backup_set_id_;
    pg_backup_task_info.incarnation_ = incarnation_;
    pg_backup_task_info.start_time_ = ObTimeUtil::current_time();
    pg_backup_task_info.status_ = ObPGBackupTaskInfo::PENDING;
    pg_backup_task_info.backup_type_ = backup_type_;
    pg_backup_task_info.snapshot_version_ = backup_snapshot_version_;
    pg_backup_task_info.retry_count_ = 0;
    pg_backup_task_info.result_ = OB_SUCCESS;
  }
  return ret;
}

int ObTenantBackup::do_backup(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo updater;
  ObArray<ObPGBackupTaskInfo> pg_task_infos;
  bool is_all_task_finished = false;
  bool can_report_task = false;

  // check backup_task
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::DOING != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do backup get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_finished_backup_task(task_info, pg_task_infos, trans))) {
    LOG_WARN("failed to get pg backup task result", K(ret), K(task_info));
  } else if (FALSE_IT(is_all_task_finished = (pg_task_infos.count() == task_info.pg_count_))) {
  } else if (!is_all_task_finished && OB_FAIL(check_doing_pg_tasks(task_info, trans))) {
    LOG_WARN("failed to check doing pg task", K(ret), K(task_info));
    // pg backup result is not ready, need wait
  }

  if (OB_SUCC(ret)) {
    if (task_info.is_result_succeed()) {
      if (OB_FAIL(do_tenat_backup_when_succeed(task_info, pg_task_infos, trans, can_report_task))) {
        LOG_WARN("failed to do tenant backup when succeed", K(ret), K(task_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (can_report_task) {
      // TODO () deal with failover
      int32_t task_info_result = OB_SUCCESS;
      int64_t finish_partition_count = 0;
      int64_t macro_block_count = 0;
      int64_t finish_maro_block_count = 0;
      int64_t input_bytes = 0;
      int64_t output_bytes = 0;

      for (int64_t i = 0; i < pg_task_infos.count(); ++i) {
        const ObPGBackupTaskInfo& pg_task_info = pg_task_infos.at(i);
        const int32_t pg_backup_result = pg_task_info.result_;
        if (OB_SUCCESS == task_info_result) {
          task_info_result = pg_backup_result;
        }
        finish_partition_count += pg_task_info.finish_partition_count_;
        macro_block_count += pg_task_info.macro_block_count_;
        finish_maro_block_count += pg_task_info.finish_macro_block_count_;
        input_bytes += pg_task_info.input_bytes_;
        output_bytes += pg_task_info.output_bytes_;
      }
      updater = task_info;
      updater.finish_pg_count_ = pg_task_infos.count();
      updater.end_time_ = ObTimeUtil::current_time();
      updater.result_ = task_info_result;
      updater.finish_partition_count_ = finish_partition_count;
      updater.macro_block_count_ = macro_block_count;
      updater.finish_macro_block_count_ = finish_maro_block_count;
      updater.input_bytes_ = input_bytes;
      updater.output_bytes_ = output_bytes;
      updater.status_ = ObTenantBackupTaskInfo::FINISH;
      DEBUG_SYNC(BACKUP_TASK_BEFOR_FINISH);
      if (OB_FAIL(update_tenant_backup_task(task_info, updater, trans))) {
        LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(updater));
      } else {
        root_backup_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantBackup::update_tenant_backup_task(const share::ObTenantBackupTaskInfo& src_info,
    const share::ObTenantBackupTaskInfo& dest_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  share::ObTenantBackupTaskUpdater tenant_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!src_info.is_valid() || !dest_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup task get invalid argument", K(ret), K(src_info), K(dest_info));
  } else if (OB_FAIL(tenant_task_updater.init(trans))) {
    LOG_WARN("failed to init tenant task updater", K(ret));
  } else if (OB_FAIL(tenant_task_updater.update_tenant_backup_task(src_info, dest_info))) {
    LOG_WARN("failed to update tenant backup task", K(ret), K(src_info), K(dest_info));
  }
  return ret;
}

int ObTenantBackup::get_finished_backup_task(const share::ObTenantBackupTaskInfo& task_info,
    common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  pg_task_infos.reset();
  bool has_not_finished_task = false;
  share::ObPGBackupTaskUpdater pg_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task result get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(pg_task_updater.init(trans))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(task_info));
  } else if (!has_not_finished_task &&
             OB_FAIL(pg_task_updater.get_finished_backup_task(
                 task_info.tenant_id_, task_info.incarnation_, task_info.backup_set_id_, pg_task_infos))) {
    LOG_WARN("failed to get pg backup task result", K(ret), K(task_info));
  } else {
    // TODO() consider pg task infos memory
    LOG_INFO("get_finished backup task", K(pg_task_infos.count()));
  }
  return ret;
}

int ObTenantBackup::get_breakpoint_pg_info(ObBreakPointPGInfo& breakpoint_pg_info)
{
  int ret = OB_SUCCESS;
  ObPGBackupTaskInfo pg_task_info;
  ObTenantBackupMetaInfo backup_meta_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(root_backup_->get_tenant_backup_meta_info(backup_meta_info))) {
    LOG_WARN("failed to get tenant backup meta info", K(ret), K(backup_meta_info));
  } else {
    breakpoint_pg_info = backup_meta_info.break_point_info_;
    total_pg_count_ = backup_meta_info.pg_count_;
    total_partition_count_ = backup_meta_info.partition_count_;
    LOG_INFO("get backup meta info", K(backup_meta_info));
  }
  // TODO() need reconsider break point info restore in all_tenant_backup_task
  return ret;
}

int ObTenantBackup::find_break_table_id_index(
    const common::ObIArray<uint64_t>& table_ids, const ObBreakPointPGInfo& breakpoint_pg_info, int64_t& index)
{
  int ret = OB_SUCCESS;
  index = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!breakpoint_pg_info.is_valid()) {
    // do nothing
  } else {
    bool found = false;
    for (int64_t i = 0; i < table_ids.count() && !found; ++i) {
      if (table_ids.at(i) == breakpoint_pg_info.pkey_.get_table_id()) {
        found = true;
        index = i;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ids do not contain break point pkey", K(ret), K(table_ids), K(breakpoint_pg_info));
    }
  }
  return ret;
}

int ObTenantBackup::find_tg_partition_index(
    const ObBreakPointPGInfo& breakpoint_pg_info, share::schema::ObTablegroupPartitionKeyIter& pkey_iter)
{
  int ret = OB_SUCCESS;
  if (!breakpoint_pg_info.is_valid()) {
    // do nothing
  } else if (breakpoint_pg_info.pkey_.get_tablegroup_id() != pkey_iter.get_tablegroup_id()) {
    // do nothing
  } else {
    ObPGKey pg_key;
    while (OB_SUCC(pkey_iter.next_partition_key_v2(pg_key))) {
      if (pg_key == breakpoint_pg_info.pkey_) {
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find break point table group key", K(ret), K(breakpoint_pg_info));
    }
  }
  return ret;
}

int ObTenantBackup::find_sd_partition_index(
    const ObBreakPointPGInfo& breakpoint_pg_info, share::schema::ObTablePartitionKeyIter& pkey_iter)
{
  int ret = OB_SUCCESS;
  if (!breakpoint_pg_info.is_valid()) {
    // do nothing
  } else if (breakpoint_pg_info.pkey_.get_table_id() != pkey_iter.get_table_id()) {
    // do nothing
  } else {
    ObPartitionKey pkey;
    while (OB_SUCC(pkey_iter.next_partition_key_v2(pkey))) {
      if (pkey == breakpoint_pg_info.pkey_) {
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find break point table group key", K(ret), K(breakpoint_pg_info));
    }
  }
  return ret;
}

int ObTenantBackup::do_finish(const share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::FINISH != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do finish get invalid argument", K(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackup::upload_pg_list(const ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObExternPGListMgr pg_list_mgr;
  const int64_t full_backup_set_id = ObBackupType::FULL_BACKUP == task_info.backup_type_.type_
                                         ? task_info.backup_set_id_
                                         : task_info.prev_full_backup_set_id_;
  const int64_t inc_backup_set_id = task_info.backup_set_id_;
  ObClusterBackupDest dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(dest.set(backup_dest_.ptr(), task_info.incarnation_))) {
    LOG_WARN("failed to set dest", K(ret), K(task_info));
  } else if (OB_FAIL(pg_list_mgr.init(
                 task_info.tenant_id_, full_backup_set_id, inc_backup_set_id, dest, *backup_lease_service_))) {
    LOG_WARN("failed to init pg list mgr", K(ret), K(task_info));
  } else if (OB_FAIL(add_tablegroup_key_to_extern_list(pg_list_mgr))) {
    LOG_WARN("failed to add tablegroup key to extern list", K(ret), K(task_info));
  } else if (OB_FAIL(add_standalone_key_to_extern_list(pg_list_mgr))) {
    LOG_WARN("failed to add standalone key to extern list", K(ret), K(task_info));
  } else if (OB_FAIL(pg_list_mgr.upload_pg_list())) {
    LOG_WARN("failed to upload pg list", K(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackup::add_tablegroup_key_to_extern_list(ObExternPGListMgr& pg_list_mgr)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tablegroup_ids;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id_, *schema_service_, backup_schema_version_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_tablegroup_ids_in_tenant(tenant_id_, tablegroup_ids))) {
    LOG_WARN("fail to get tablegroup ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablegroup_ids.count(); ++i) {
      const uint64_t tablegroup_id = tablegroup_ids.at(i);
      if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(tablegroup_id));
      } else if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema is NULL", K(ret), KP(tablegroup_schema), K(tenant_id_), K(tablegroup_id));
      } else if (!tablegroup_schema->get_binding() || tablegroup_schema->is_dropped_schema()) {
        // do nothing
      } else {
        ObPGKey pg_key;
        bool check_dropped_schema = false;
        ObTablegroupPartitionKeyIter pkey_iter(*tablegroup_schema, check_dropped_schema);
        while (OB_SUCC(ret) && OB_SUCC(pkey_iter.next_partition_key_v2(pg_key))) {
          if (OB_FAIL(pg_list_mgr.add_pg_key(pg_key))) {
            LOG_WARN("failed to push pg key into pg list mgr", K(ret), K(pg_key));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTenantBackup::add_standalone_key_to_extern_list(ObExternPGListMgr& pg_list_mgr)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> table_ids;
  const ObTableSchema* table_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id_, *schema_service_, backup_schema_version_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id_), K(backup_schema_version_));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id_, table_ids))) {
    LOG_WARN("fail to get tablegroup ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const uint64_t table_id = table_ids.at(i);
      bool need_backup = false;
      if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is NULL", K(ret), KP(table_id), K(tenant_id_), K(table_id));
      } else if (OB_FAIL(check_standalone_table_need_backup(table_schema, need_backup))) {
        LOG_WARN("failed to check standalone table need backup", K(ret), K(table_id));
      } else if (!need_backup) {
        // do nothing
      } else {
        bool check_dropped_schema = false;
        ObTablePartitionKeyIter pkey_iter(*table_schema, check_dropped_schema);
        ObPartitionKey pkey;
        while (OB_SUCC(ret) && OB_SUCC(pkey_iter.next_partition_key_v2(pkey))) {
          if (OB_FAIL(pg_list_mgr.add_pg_key(pkey))) {
            LOG_WARN("failed to add pkey into pg list mgr", K(ret), K(pkey));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTenantBackup::check_doing_pg_tasks(const ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObPGBackupTaskInfo> pg_backup_tasks;
  int64_t lease_time = 0;
  share::ObPGBackupTaskUpdater pg_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check doing pg task get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(root_backup_->get_lease_time(task_info.tenant_id_, lease_time))) {
    LOG_WARN("failed to get lease time", K(ret), K(task_info));
  } else if (ObTimeUtil::current_time() - lease_time < MAX_CHECK_INTERVAL) {
    // do nothing
  } else if (OB_FAIL(root_backup_->update_lease_time(task_info.tenant_id_))) {
    LOG_WARN("failed to update lease time", K(ret), K(task_info));
  } else if (OB_FAIL(pg_task_updater.init(trans))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(task_info));
  } else if (OB_FAIL(pg_task_updater.get_one_doing_pg_task(
                 task_info.tenant_id_, task_info.incarnation_, task_info.backup_set_id_, pg_backup_tasks))) {
    LOG_WARN("failed to get doing pg task", K(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_backup_tasks.count(); ++i) {
      const ObPGBackupTaskInfo& pg_task_info = pg_backup_tasks.at(i);
      if (OB_SUCCESS != (tmp_ret = check_doing_pg_task(pg_task_info, trans))) {
        LOG_WARN("failed to check doing pg task", K(tmp_ret), K(pg_task_info));
      }
    }
  }
  return ret;
}

int ObTenantBackup::check_doing_pg_task(const ObPGBackupTaskInfo& pg_backup_task, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;

  bool is_exist = true;
  bool is_finished = true;
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (ObPGBackupTaskInfo::DOING != pg_backup_task.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup pg task status unexpected", K(ret), K(pg_backup_task));
  } else if (OB_FAIL(check_backup_task_on_progress(pg_backup_task, is_exist))) {
    LOG_WARN("failed to check backup task on progress", K(ret), K(pg_backup_task));
  } else if (is_exist) {
    // do nothing
    LOG_INFO("pg backup task on progress", K(pg_backup_task));
  } else if (OB_FAIL(check_task_in_rebalancer_mgr(pg_backup_task, is_exist))) {
    LOG_WARN("failed to check task in rebalancer mgr", K(ret), K(pg_backup_task));
  } else if (is_exist) {
    // do nothing
    LOG_INFO("pg backup task is rebalancer mgr", K(pg_backup_task));
  } else if (OB_FAIL(check_doing_task_finished(pg_backup_task, trans, is_finished))) {
    LOG_WARN("failed to check doing task finished", K(ret), K(pg_backup_task));
  } else if (is_finished) {
    // do nothing
  } else {
    LOG_INFO("pg backup task need reset", K(pg_backup_task));
    if (OB_FAIL(update_lost_task_finished(pg_backup_task, trans))) {
      LOG_WARN("failed to update lost task failed", K(ret), K(pg_backup_task));
    }
  }
  LOG_INFO("check doing pg task", "cost ts", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObTenantBackup::check_backup_task_on_progress(const ObPGBackupTaskInfo& pg_task_info, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = true;
  Bool res = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (ObPGBackupTaskInfo::DOING != pg_task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check backup task on progress get invalid argument", K(ret), K(pg_task_info));
  } else {
    share::ObServerStatus server_status;
    const common::ObAddr& dest = pg_task_info.server_;
    if (OB_FAIL(server_mgr_->is_server_exist(dest, is_exist))) {
      LOG_WARN("fail to check server exist", K(ret));
    } else if (!is_exist) {
      FLOG_INFO("backup dest server is not exist", K(ret), K(dest));
    } else if (OB_FAIL(server_mgr_->get_server_status(dest, server_status))) {
      LOG_WARN("fail to get server status", K(ret));
    } else if (!server_status.is_active() || !server_status.in_service()) {
      is_exist = false;
      FLOG_INFO("server status may not active or in service", K(dest));
    } else if (OB_FAIL(rpc_proxy_->to(dest).check_migrate_task_exist(pg_task_info.trace_id_, res))) {
      LOG_WARN("fail to check task", K(ret), K(pg_task_info));
    } else if (!res) {
      is_exist = false;
    }
    LOG_INFO("check_backup_task_on_progress", K(ret), K(server_status), K(res), K(is_exist));
  }
  return ret;
}

int ObTenantBackup::check_task_in_rebalancer_mgr(const ObPGBackupTaskInfo& pg_task_info, bool& is_exist)
{
  int ret = OB_SUCCESS;
  ObBackupTaskInfo mock_backup_task_info;
  ObPartitionKey pkey;
  ObArenaAllocator allocator;
  ObRebalanceTask* task = NULL;
  is_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret), K(pg_task_info));
  } else if (OB_FAIL(pkey.init(pg_task_info.table_id_, pg_task_info.partition_id_, 0))) {
    LOG_WARN("failed to init pkey", K(ret), K(pg_task_info));
  } else if (OB_FAIL(mock_backup_task_info.init_task_key(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else if (OB_FAIL(
                 rebalancer_mgr_->get_schedule_task(mock_backup_task_info, pg_task_info.server_, allocator, task))) {
    LOG_WARN("failed to get schedule task", K(ret), K(mock_backup_task_info), K(pg_task_info));
  } else if (OB_ISNULL(task)) {
    is_exist = false;
  }
  return ret;
}

int ObTenantBackup::check_doing_task_finished(
    const ObPGBackupTaskInfo& pg_task_info, common::ObISQLClient& trans, bool& is_finished)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;
  ObPGBackupTaskInfo tmp_task_info;
  is_finished = false;
  share::ObPGBackupTaskUpdater pg_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(pkey.init(pg_task_info.table_id_, pg_task_info.partition_id_, 0))) {
    LOG_WARN("failed to init pkey", K(ret), K(pg_task_info));
  } else if (OB_FAIL(pg_task_updater.init(trans))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(pg_task_info));
  } else if (OB_FAIL(pg_task_updater.get_pg_backup_task(pg_task_info.tenant_id_,
                 pg_task_info.incarnation_,
                 pg_task_info.backup_set_id_,
                 pkey,
                 tmp_task_info))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(pg_task_info));
  } else if (ObPGBackupTaskInfo::FINISH == tmp_task_info.status_) {
    is_finished = true;
  }
  return ret;
}

int ObTenantBackup::update_lost_task_finished(const ObPGBackupTaskInfo& pg_task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pkeys;
  ObArray<int32_t> results;
  const ObPGBackupTaskInfo::BackupStatus status = ObPGBackupTaskInfo::FINISH;
  ObArray<ObPGBackupTaskInfo> pg_task_infos;
  share::ObPGBackupTaskUpdater pg_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(pg_task_updater.init(trans))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(pg_task_info));
  } else if (OB_FAIL(pg_task_updater.get_pg_backup_tasks(pg_task_info.tenant_id_,
                 pg_task_info.incarnation_,
                 pg_task_info.backup_set_id_,
                 pg_task_info.task_id_,
                 pg_task_infos))) {
    LOG_WARN("failed to get pg backup tasks", K(ret), K(pg_task_info));
  } else {
    ObPartitionKey pkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
      pkey.reset();
      const ObPGBackupTaskInfo& pg_backup_task = pg_task_infos.at(i);
      if (ObPGBackupTaskInfo::FINISH == pg_backup_task.status_) {
        // do nothing
      } else if (OB_FAIL(pkey.init(pg_backup_task.table_id_, pg_backup_task.partition_id_, 0))) {
        LOG_WARN("failed to init pkey", K(ret), K(pg_backup_task));
      } else if (OB_FAIL(pkeys.push_back(pkey))) {
        LOG_WARN("failed to push pkey into array", K(ret), K(pkey), K(pg_backup_task));
      } else if (OB_FAIL(results.push_back(OB_REBALANCE_TASK_NOT_IN_PROGRESS))) {
        LOG_WARN("failed to push result into array", K(ret), K(pkey), K(pg_backup_task));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg_task_updater.update_status_and_result_without_trans(pkeys, results, status))) {
        LOG_WARN("failed to update status and result", K(ret), K(pkeys));
      }
    }
    LOG_INFO("update_lost_task_finished", K(ret), K(pkeys));
  }
  return ret;
}

int ObTenantBackup::do_with_finished_task(const share::ObTenantBackupTaskInfo& task_info,
    const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans,
    bool& can_report_task)
{
  int ret = OB_SUCCESS;
  can_report_task = false;
  bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do init", K(ret));
  } else if (OB_FAIL(check_finished_task_result(task_info, pg_task_infos, trans, need_retry, can_report_task))) {
    LOG_WARN("failed to check finished task result", K(ret), K(task_info), K(pg_task_infos));
  } else if (can_report_task) {
    // do noting
  } else if (need_retry) {
    if (OB_FAIL(reset_pg_backup_tasks(pg_task_infos, trans))) {
      LOG_WARN("failed to reset pg backup tasks", K(ret), K(pg_task_infos));
    }
  }
  LOG_INFO("do with finish task", K(ret), K(can_report_task), K(need_retry), K(pg_task_infos.count()), K(task_info));
  return ret;
}

int ObTenantBackup::check_finished_task_result(const share::ObTenantBackupTaskInfo& task_info,
    const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans, bool& need_retry,
    bool& can_report_task)
{
  int ret = OB_SUCCESS;
  need_retry = false;
  can_report_task = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to do with finished task", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
      const ObPGBackupTaskInfo& pg_task_info = pg_task_infos.at(i);
      // TODO() add ret_code with not need retry
      const int32_t result = pg_task_info.result_;
      if (OB_SUCCESS == result) {
        // do nothing
      } else if (OB_NOT_INIT != result && OB_INVALID_ARGUMENT != result && OB_ERR_SYS != result &&
                 OB_INIT_TWICE != result && OB_ERR_UNEXPECTED != result && OB_SRC_DO_NOT_ALLOWED_MIGRATE != result &&
                 OB_CANCELED != result && OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT != result &&
                 OB_LOG_ARCHIVE_STAT_NOT_MATCH != result && OB_NOT_SUPPORTED != result &&
                 pg_task_info.retry_count_ < PG_TASK_MAX_RETRY_NUM) {
        need_retry = true;
        can_report_task = false;
        // TODO() retry_count > PG_TASK_MAX_RETRY_NUM need add obtest
      } else {
        LOG_INFO("pg backup task can not retry", K(ret), K(pg_task_info));
        need_retry = false;
        if (OB_FAIL(update_tenant_backup_task_result(task_info, result, trans))) {
          LOG_WARN("failed to update tenant backup task result", K(ret), K(result));
        } else {
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (task_info.pg_count_ == pg_task_infos.count()) {
        if (need_retry) {
          can_report_task = false;
        } else {
          can_report_task = true;
        }
      }
    }
  }
  return ret;
}

int ObTenantBackup::reset_pg_backup_tasks(
    const common::ObIArray<ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObPGBackupTaskInfo> reset_task_infos;
  share::ObPGBackupTaskUpdater pg_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
      const ObPGBackupTaskInfo& pg_task_info = pg_task_infos.at(i);
      if (OB_SUCCESS == pg_task_info.result_) {
        // do nothing
      } else {
        ObPGBackupTaskInfo reset_task_info = pg_task_info;
        reset_task_info.end_time_ = 0;
        reset_task_info.finish_macro_block_count_ = 0;
        reset_task_info.finish_partition_count_ = 0;
        reset_task_info.input_bytes_ = 0;
        reset_task_info.output_bytes_ = 0;
        reset_task_info.macro_block_count_ = 0;
        reset_task_info.partition_count_ = 0;
        reset_task_info.replica_type_ = ObReplicaType::REPLICA_TYPE_MAX;
        reset_task_info.role_ = ObRole::INVALID_ROLE;
        reset_task_info.server_.reset();
        reset_task_info.result_ = 0;
        ++reset_task_info.retry_count_;
        reset_task_info.status_ = ObPGBackupTaskInfo::PENDING;
        if (OB_FAIL(reset_task_infos.push_back(reset_task_info))) {
          LOG_WARN("failed to push reset task info into array", K(ret), K(reset_task_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg_task_updater.init(trans))) {
        LOG_WARN("failed to init pg task updater", K(ret));
      } else if (OB_FAIL(pg_task_updater.batch_report_pg_task(reset_task_infos))) {
        LOG_WARN("failed to batch report pg task", K(ret), K(reset_task_infos), K(pg_task_infos));
      } else {
        root_balancer_->wakeup();
      }
    }
    LOG_INFO("reset task infos", K(ret), K(reset_task_infos));
  }
  return ret;
}

int ObTenantBackup::update_tenant_backup_task_result(
    const ObTenantBackupTaskInfo& task_info, const int32_t result, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo dest_task_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else {
    dest_task_info = task_info;
    dest_task_info.result_ = result;
    if (OB_FAIL(update_tenant_backup_task(task_info, dest_task_info, trans))) {
      LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(dest_task_info));
    } else {
      root_backup_->wakeup();
    }
  }
  return ret;
}

int ObTenantBackup::do_tenat_backup_when_succeed(const share::ObTenantBackupTaskInfo& task_info,
    const ObIArray<ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans, bool& can_report_task)
{
  int ret = OB_SUCCESS;
  can_report_task = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::DOING != task_info.status_ ||
             !task_info.is_result_succeed()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant backup when succeed get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(do_with_finished_task(task_info, pg_task_infos, trans, can_report_task))) {
    LOG_WARN("failed to do with finished task", K(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackup::do_tenant_backup_when_failed(const share::ObTenantBackupTaskInfo& task_info,
    const ObIArray<ObPGBackupTaskInfo>& pg_task_infos, common::ObISQLClient& trans, bool& can_report_task)
{
  int ret = OB_SUCCESS;
  can_report_task = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || task_info.is_result_succeed()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do tenant backup when succeed get invalid argument", K(ret), K(task_info));
  } else if (pg_task_infos.count() == task_info.pg_count_) {
    can_report_task = true;
  } else if (OB_FAIL(cancel_doing_pg_tasks(task_info, trans))) {
    LOG_WARN("failed to cancel doing pg tasks", K(ret), K(task_info));
  } else {
    root_backup_->wakeup();
  }
  return ret;
}

// TODO() need add obtest
int ObTenantBackup::cancel_doing_pg_tasks(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObPGBackupTaskInfo> pg_backup_tasks;
  share::ObPGBackupTaskUpdater pg_task_updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check doing pg task get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(pg_task_updater.init(trans))) {
    LOG_WARN("failed to init pg task updater", K(ret), K(task_info));
  } else if (OB_FAIL(pg_task_updater.get_one_doing_pg_task(
                 task_info.tenant_id_, task_info.incarnation_, task_info.backup_set_id_, pg_backup_tasks))) {
    LOG_WARN("failed to get doing pg task", K(ret), K(task_info));
  } else {
    LOG_INFO("get one doing pg task", K(pg_backup_tasks.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_backup_tasks.count(); ++i) {
      const ObPGBackupTaskInfo& pg_backup_task = pg_backup_tasks.at(i);
      const ObAddr& task_server = pg_backup_task.server_;
      obrpc::ObCancelTaskArg rpc_arg;
      rpc_arg.task_id_ = pg_backup_task.trace_id_;
      if (OB_FAIL(rpc_proxy_->to(task_server).cancel_sys_task(rpc_arg))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("task may not excute on server", K(rpc_arg), K(task_server));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to cancel sys task", K(ret), K(rpc_arg));
        }
      }
    }
  }
  return ret;
}

int ObTenantBackup::clean_pg_backup_task(common::ObISQLClient& trans, const share::ObTenantBackupTaskInfo& task_info)
{
  // TODO backup fix it
  int ret = OB_SUCCESS;
  UNUSED(trans);
  UNUSED(task_info);
  return ret;
}

int ObTenantBackup::do_cancel(const share::ObTenantBackupTaskInfo& task_info, common::ObISQLClient& trans)
{
  LOG_INFO("start do cancel", K(task_info));
  int ret = OB_SUCCESS;
  ObArray<ObPGBackupTaskInfo> pg_task_infos;
  bool is_all_task_finished = true;
  bool can_report_task = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid() || ObTenantBackupTaskInfo::CANCEL != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do cancel get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(get_finished_backup_task(task_info, pg_task_infos, trans))) {
    LOG_WARN("failed to get pg backup task result", K(ret), K(task_info));
  } else if (FALSE_IT(is_all_task_finished = pg_task_infos.count() == task_info.pg_count_)) {
  } else if (!is_all_task_finished && OB_FAIL(check_doing_pg_tasks(task_info, trans))) {
    LOG_WARN("failed to check doing pg task", K(ret), K(task_info));
    // pg backup result is not ready, need wait
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_tenant_backup_when_failed(task_info, pg_task_infos, trans, can_report_task))) {
      LOG_WARN("failed to do tenant backup when failed", K(ret), K(task_info));
    }
  }

  if (OB_SUCC(ret)) {
    if (can_report_task) {
      int32_t task_info_result = OB_CANCELED;
      ObTenantBackupTaskInfo dest_task_info;
      for (int64_t i = 0; i < pg_task_infos.count() && OB_SUCCESS == task_info_result; ++i) {
        const int32_t pg_backup_result = pg_task_infos.at(i).result_;
        if (OB_SUCCESS != pg_backup_result) {
          task_info_result = pg_backup_result;
        }
      }
      dest_task_info = task_info;
      dest_task_info.finish_pg_count_ = pg_task_infos.count();
      dest_task_info.finish_partition_count_ = dest_task_info.partition_count_;
      dest_task_info.end_time_ = ObTimeUtil::current_time();
      dest_task_info.result_ = task_info_result;
      dest_task_info.status_ = ObTenantBackupTaskInfo::FINISH;
      if (OB_FAIL(update_tenant_backup_task(task_info, dest_task_info, trans))) {
        LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(dest_task_info));
      } else {
        root_backup_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantBackup::get_table_count_with_partition(const uint64_t tenant_id, const int64_t tablegroup_id,
    share::schema::ObSchemaGetterGuard& schema_guard, int64_t& table_count)
{
  int ret = OB_SUCCESS;
  table_count = 0;
  ObArray<uint64_t> table_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(tenant_id, tablegroup_id, table_ids))) {
    LOG_WARN("failed to get table ids in tablegroup", K(ret), K(tenant_id), K(tablegroup_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const uint64_t table_id = table_ids.at(i);
      const ObTableSchema* table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be NULL", K(ret), KP(table_schema));
      } else if (table_schema->has_partition()) {
        ++table_count;
      }
    }
  }
  return ret;
}

int ObTenantBackup::check_standalone_table_need_backup(
    const share::schema::ObTableSchema* table_schema, bool& need_backup)
{
  int ret = OB_SUCCESS;
  ObIndexStatus status;
  need_backup = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check standalone table need backup get invalid argument", K(ret), KP(table_schema));
  } else if (table_schema->get_binding()) {
    // do nothing
  } else if (!table_schema->has_self_partition()) {
    // do nothing
  } else if (table_schema->is_dropped_schema()) {
    LOG_INFO("table is dropped schema, skip it", K(*table_schema));
  } else if (FALSE_IT(status = table_schema->get_index_status())) {
  } else if (table_schema->is_global_index_table() && ObIndexStatus::INDEX_STATUS_AVAILABLE != status) {
    LOG_INFO("table is global index but not avaiable, skip it", K(status));
  } else {
    need_backup = true;
  }
  return ret;
}

int ObTenantBackup::commit_trans(ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(root_backup_->check_can_backup())) {
    LOG_WARN("failed to check can backup", K(ret));
  }

  tmp_ret = trans.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObTenantBackup::start_trans(ObTimeoutCtx& timeout_ctx, ObMySQLTransaction& trans)
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

int ObBackupUtil::check_sys_tenant_trans_alive(share::ObBackupInfoManager& info_manager, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObBackupInfoStatus backup_status;

  if (!info_manager.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check sys tenant trans alive get invalid argument", K(ret));
  } else if (OB_FAIL(info_manager.get_backup_status(tenant_id, trans, backup_status))) {
    LOG_WARN("failed to get backup status", K(ret), K(tenant_id));
  } else if (!backup_status.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("backup status is invalid", K(ret), K(backup_status));
  }
  return ret;
}

int ObBackupUtil::check_sys_clean_info_trans_alive(common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTenantBackupCleanInfoUpdater updater;
  ObBackupCleanInfoStatus::STATUS status;

  if (OB_FAIL(updater.init(trans))) {
    LOG_WARN("failed to init backup clean info updater", K(ret));
  } else if (OB_FAIL(updater.get_backup_clean_info_status(tenant_id, trans, status))) {
    LOG_WARN("failed to get backup clean info status", K(ret), K(tenant_id));
  } else if (!ObBackupCleanInfoStatus::is_valid(status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("backup clean info status is invalid", K(ret), K(status));
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
