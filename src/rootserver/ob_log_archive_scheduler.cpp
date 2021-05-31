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
#include "ob_log_archive_scheduler.h"
#include "lib/thread/ob_thread_name.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_backup_manager.h"
#include "ob_leader_coordinator.h"
#include "ob_server_manager.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_rs_event_history_table_operator.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace obrpc;
using namespace rootserver;

ObLogArchiveThreadIdling::ObLogArchiveThreadIdling(volatile bool& stop)
    : ObThreadIdling(stop), idle_time_us_(DEFAULT_IDLE_INTERVAL_US)
{}

int64_t ObLogArchiveThreadIdling::get_idle_interval_us()
{
  return idle_time_us_;
}

void ObLogArchiveThreadIdling::set_log_archive_checkpoint_interval(const int64_t interval_us)
{
  const int64_t idle_us = interval_us / 2;
  int64_t idle_time_us = 0;
  if (idle_us <= 0) {
    idle_time_us = DEFAULT_IDLE_INTERVAL_US;
  } else if (idle_us > MAX_IDLE_INTERVAL_US) {
    idle_time_us = MAX_IDLE_INTERVAL_US;
  } else {
    idle_time_us = idle_us;
  }

  if (idle_time_us != idle_time_us_) {
    FLOG_INFO("[LOG_ARCHIVE] change idle_time_us", K(idle_time_us_), K(idle_time_us));
    idle_time_us_ = idle_time_us;
  }
}

ObLogArchiveScheduler::ObLogArchiveScheduler()
    : is_inited_(false),
      idling_(stop_),
      schema_service_(nullptr),
      server_mgr_(nullptr),
      rpc_proxy_(nullptr),
      sql_proxy_(nullptr),
      is_working_(false),
      tenant_ids_(),
      last_update_tenant_ts_(0),
      tenant_name_mgr_(),
      backup_lease_service_(nullptr)
{}

ObLogArchiveScheduler::~ObLogArchiveScheduler()
{
  destroy();
}

int ObLogArchiveScheduler::init(ObServerManager& server_mgr, share::schema::ObMultiVersionSchemaService* schema_service,
    ObSrvRpcProxy& rpc_proxy, common::ObMySQLProxy& sql_proxy, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(schema_service));
  } else if (OB_FAIL(create(thread_cnt, "LOG_ARCHIVE_SCHEDULER"))) {
    LOG_WARN("failed to create log archive thread", K(ret));
  } else if (OB_FAIL(tenant_name_mgr_.init(sql_proxy, *schema_service))) {
    LOG_WARN("failed to init tenant_name_mgr", K(ret));
  } else {
    server_mgr_ = &server_mgr;
    schema_service_ = schema_service;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    tenant_ids_.reset();
    last_update_tenant_ts_ = 0;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

void ObLogArchiveScheduler::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

void ObLogArchiveScheduler::wakeup()
{
  idling_.wakeup();
}

int ObLogArchiveScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("failed to start", K(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

int ObLogArchiveScheduler::set_enable_log_archive_(const bool is_enable)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("alter system set enable_log_archive=%s", is_enable ? "true" : "false"))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to exec sql", K(sql), K(affected_rows));
  }
  return ret;
}

int ObLogArchiveScheduler::handle_enable_log_archive(const bool is_enable)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (is_enable) {
    if (OB_FAIL(handle_start_log_archive_())) {
      LOG_WARN("failed to handle start log archive", K(ret));
    }
  } else {
    if (OB_FAIL(handle_stop_log_archive_())) {
      LOG_WARN("failed to handle stop log archive", K(ret));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::handle_stop_log_archive_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo sys_info;
  ObLogArchiveBackupInfo extern_sys_info;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, OB_SYS_TENANT_ID, sys_info))) {
      LOG_WARN("failed to get log archive backup sys_info", K(ret));
    } else if (OB_FAIL(sys_info.status_.status_ == ObLogArchiveStatus::STOP ||
                       sys_info.status_.status_ == ObLogArchiveStatus::STOPPING)) {
      ret = OB_ALREADY_NO_LOG_ARCHIVE_BACKUP;
      LOG_WARN("log archive backup is stop or stopping, no need to noarchivelog", K(ret), K(sys_info));
    } else if (OB_FAIL(set_enable_log_archive_(false /*is_enable*/))) {
      LOG_WARN("failed to enable log archive", K(ret));
    } else {
      sys_info.status_.status_ = ObLogArchiveStatus::STOPPING;
      if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, sys_info))) {
        LOG_WARN("Failed to update log archive backup info", K(ret), K(sys_info));
      } else {
        FLOG_INFO("succeed to update log archive backup as stopping status", K(sys_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("[LOG_ARCHIVE] succeed to handle enable log archive", K(sys_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }

    wakeup();
  }

  ROOTSERVICE_EVENT_ADD(
      "log_archive", "handle_stop_log_archive", "status", "STOPPING", "round", sys_info.status_.round_, "result", ret);
  return ret;
}

int ObLogArchiveScheduler::handle_start_log_archive_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo sys_info;
  ObLogArchiveBackupInfo extern_sys_info;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfoMgr info_mgr;
  char backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  const ObClusterType cluster_type = ObClusterInfoGetter::get_cluster_type();
  ObBackupInfoChecker checher;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(wait_backup_dest_(backup_dest, sizeof(backup_dest)))) {
    LOG_WARN("failed to wait backup dest, cannot do schedule log archive", K(ret));
  } else if (PRIMARY_CLUSTER != cluster_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not primary cluster cannot backup now", K(ret), K(cluster_type));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, OB_SYS_TENANT_ID, sys_info))) {
      LOG_WARN("failed to get log archive backup sys_info", K(ret));
    } else if (sys_info.status_.status_ != ObLogArchiveStatus::STOP) {
      ret = OB_CANNOT_START_LOG_ARCHIVE_BACKUP;
      LOG_WARN("cannot start log archive backup in this status", K(ret), K(sys_info));
    } else {
      sys_info.status_.round_++;
      sys_info.status_.status_ = ObLogArchiveStatus::BEGINNING;
      sys_info.status_.incarnation_ = OB_START_INCARNATION;
      sys_info.status_.start_ts_ = ObTimeUtil::current_time();
      sys_info.status_.checkpoint_ts_ = 0;
      sys_info.status_.is_mount_file_created_ = false;
      sys_info.status_.compatible_ = ObTenantLogArchiveStatus::COMPATIBLE::NONE;

      if (OB_FAIL(databuff_printf(sys_info.backup_dest_, sizeof(sys_info.backup_dest_), "%s", backup_dest))) {
        LOG_WARN("failed to copy backup dest", K(ret), K(backup_dest));
      } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, sys_info))) {
        LOG_WARN("Failed to update log archive backup info", K(ret), K(sys_info));
      } else {
        FLOG_INFO("succeed to update log archive backup as beginning status", K(sys_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("[LOG_ARCHIVE] succeed to handle enable log archive", K(sys_info));
        if (OB_FAIL(set_enable_log_archive_(true /*is_enable*/))) {
          LOG_WARN("failed to en table log archive", K(ret));
        }
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }

    wakeup();
  }

  ROOTSERVICE_EVENT_ADD("log_archive",
      "handle_start_log_archive",
      "new_status",
      "BEGINNING",
      "round",
      sys_info.status_.round_,
      "result",
      ret);
  return ret;
}

int ObLogArchiveScheduler::wait_backup_dest_(char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_INTERVAL_US = 1000 * 1000;  // 1s
  const int64_t RETRY_TIMES = 3;                  // config will updated every 2s, wait 3s is enough
  int64_t retry_time = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  }

  while (OB_SUCC(ret)) {
    ++retry_time;
    if (OB_FAIL(check_can_do_work_())) {
      LOG_WARN("failed to check can do work", K(ret));
    } else if (OB_FAIL(GCONF.backup_dest.copy(buf, buf_len))) {
      LOG_WARN("failed to copy backup dest", K(ret));
    } else if (strlen(buf) > 0) {
      FLOG_INFO("[LOG_ARCHIVE] succeed to wait backup dest", K(buf));
      break;
    } else if (retry_time >= RETRY_TIMES) {
      ret = OB_INVALID_BACKUP_DEST;
      FLOG_WARN("[LOG_ARCHIVE] backup dest is empty, cannot start backup", K(ret));
      break;
    } else {
      FLOG_WARN("[LOG_ARCHIVE] backup dest is empty, retry after 1s", K(ret));
      ::usleep(SLEEP_INTERVAL_US);
    }
  }

  return ret;
}

void ObLogArchiveScheduler::run3()
{
  int tmp_ret = OB_SUCCESS;
  int64_t round = 0;
  lib::set_thread_name("LogArchiveScheduler");
  ObCurTraceId::init(GCONF.self_addr_);

  if (!is_inited_) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(tmp_ret));
  } else {
    while (true) {
      ++round;
      FLOG_INFO("start do ObLogArchiveScheduler round", K(round));
      tenant_ids_.reset();
      if (stop_) {
        tmp_ret = OB_IN_STOP_STATE;
        LOG_WARN("exit for stop state", K(tmp_ret));
        break;
      } else if (OB_SUCCESS != (tmp_ret = check_sys_backup_info_())) {
        LOG_WARN("failed to check sys backup info", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = do_schedule_())) {
        LOG_WARN("failed to do schedule", K(tmp_ret));
      }

      const int64_t log_archive_checkpoint_interval = GCONF.log_archive_checkpoint_interval;
      idling_.set_log_archive_checkpoint_interval(log_archive_checkpoint_interval);
      if (OB_SUCCESS != (tmp_ret = idling_.idle())) {
        LOG_WARN("failed to to idling", K(tmp_ret));
      }
    }
    is_working_ = false;
  }
  cleanup_();
}

void ObLogArchiveScheduler::cleanup_()
{
  tenant_name_mgr_.cleanup();
  last_update_tenant_ts_ = 0;
}

int ObLogArchiveScheduler::check_sys_backup_info_()
{
  int ret = OB_SUCCESS;
  ObBackupInfoChecker checker;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(checker.init(sql_proxy_))) {
    LOG_WARN("failed to init checker", K(ret));
  } else if (OB_FAIL(checker.check(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to check sys backup info", K(ret));
  }

  LOG_INFO("finish check sys backup info", K(ret));
  return ret;
}

int ObLogArchiveScheduler::check_backup_info_()
{
  int ret = OB_SUCCESS;
  ObBackupInfoChecker checker;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(checker.init(sql_proxy_))) {
    LOG_WARN("failed to init checker", K(ret));
  } else if (OB_FAIL(checker.check(tenant_ids_, *backup_lease_service_))) {
    LOG_WARN("failed to check backup info", K(ret));
  }

  LOG_INFO("finish check backup info", K(ret), K(tenant_ids_));
  return ret;
}

int ObLogArchiveScheduler::do_schedule_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo sys_info;
  ObLogArchiveBackupInfoMgr info_mgr;
  tenant_ids_.reset();

  DEBUG_SYNC(BEFROE_DO_LOG_ARCHIVE_SCHEDULER);
  const int64_t start_ts = ObTimeUtil::current_time();

  LOG_INFO("start do log archive schedule");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(
                 info_mgr.get_log_archive_backup_info(*sql_proxy_, false /*for_update*/, OB_SYS_TENANT_ID, sys_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (ObLogArchiveStatus::STOP == sys_info.status_.status_) {
    LOG_INFO("log archive status is stop, do nothing");
  } else if (OB_FAIL(tenant_name_mgr_.reload_backup_dest(sys_info.backup_dest_, sys_info.status_.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(sys_info));
  } else if (OB_FAIL(check_mount_file_(sys_info))) {
    LOG_WARN("failed to check mount file", K(ret), K(sys_info));
  } else if (OB_FAIL(tenant_name_mgr_.do_update(false /*is_force*/))) {
    LOG_WARN("failed to update tenant name id mapping", K(ret));
  } else if (OB_FAIL(tenant_name_mgr_.get_tenant_ids(tenant_ids_, last_update_tenant_ts_))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else if (OB_FAIL(check_backup_info_())) {
    LOG_WARN("failed to check_backup_info_", K(ret));
  } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
    LOG_WARN("failed to sync extern log archive backup info", K(ret), K(sys_info));
  }

  if (OB_FAIL(ret)) {
    if ((OB_IO_ERROR == ret || OB_BACKUP_MOUNT_FILE_NOT_VALID == ret) &&
        ObLogArchiveStatus::STOPPING == sys_info.status_.status_) {
      if (OB_FAIL(force_stop_log_archive_backup_(sys_info))) {
        LOG_WARN("failed to do force_stop", K(ret), K(sys_info));
      }
    }
  } else {
    switch (sys_info.status_.status_) {
      case ObLogArchiveStatus::STOP: {
        LOG_DEBUG("log archive status is stop, do nothing");
        break;
      }
      case ObLogArchiveStatus::BEGINNING: {
        DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_BEGINNING);
        if (OB_FAIL(start_log_archive_backup_(sys_info))) {
          if (OB_LOG_ARCHIVE_INTERRUPTED == ret) {
            if (OB_SUCCESS != (tmp_ret = set_log_archive_backup_interrupted_(sys_info))) {
              LOG_WARN("failed to set_log_archive_backup_interrupted", K(ret), K(tmp_ret));
            }
          } else {
            LOG_WARN("failed to start log archive backup", K(ret), K(sys_info));
          }
        }
        break;
      }
      case ObLogArchiveStatus::DOING: {
        DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_DOING);
        if (OB_FAIL(update_log_archive_backup_process_(sys_info))) {
          if (OB_LOG_ARCHIVE_INTERRUPTED == ret) {
            if (OB_SUCCESS != (tmp_ret = set_log_archive_backup_interrupted_(sys_info))) {
              LOG_WARN("failed to set_log_archive_backup_interrupted", K(ret), K(tmp_ret));
            }
          } else {
            LOG_WARN("failed to update log archive backup status", K(ret), K(sys_info));
          }
        }
        break;
      }
      case ObLogArchiveStatus::INTERRUPTED: {
        if (OB_FAIL(set_tenants_log_archive_backup_interrupted_(sys_info))) {
          LOG_ERROR("[LOG_ARCHIVE] failed to set_tenants_log_archive_backup_interrupted_", K(ret), K(sys_info));
        } else {
          LOG_ERROR("[LOG_ARCHIVE] log archive status is interrupted, need manual process", K(sys_info));
        }
        break;
      }
      case ObLogArchiveStatus::STOPPING: {
        DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_STOPPING);
        if (OB_FAIL(stop_log_archive_backup_(sys_info))) {
          LOG_WARN("failed to stop log archive backup", K(ret), K(sys_info));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown log archive status", K(ret), K(sys_info));
      }
    }
  }

  const int64_t cost_ts = ObTimeUtil::current_time() - start_ts;
  FLOG_INFO("finish do log archive schedule", K(ret), K(cost_ts), K(last_update_tenant_ts_), K(sys_info));

  return ret;
}

int ObLogArchiveScheduler::check_mount_file_(share::ObLogArchiveBackupInfo& sys_info)
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  const uint64_t cluster_observer_version = GET_MIN_CLUSTER_VERSION();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (cluster_observer_version < CLUSTER_VERSION_2273) {
    // do nothing
  } else if (OB_FAIL(ObBackupMountFile::need_check_mount_file(sys_info, need_check))) {
    LOG_WARN("failed to need check mount file", K(ret), K(sys_info));
  } else if (!need_check) {
    // no need check
  } else if (sys_info.status_.is_mount_file_created_) {
    if (OB_FAIL(ObBackupMountFile::check_mount_file(sys_info))) {
      LOG_ERROR("failed to check_mount_file", K(ret), K(sys_info));
    }
  } else {  // not created
    share::ObLogArchiveBackupInfo cur_sys_info = sys_info;
    sys_info.status_.is_mount_file_created_ = true;
    if (OB_FAIL(ObBackupMountFile::create_mount_file(sys_info))) {
      LOG_ERROR("failed to create mount file", K(ret));
    } else if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::start_log_archive_backup_(const share::ObLogArchiveBackupInfo& cur_sys_info)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t doing_tenant_count = 0;
  int64_t max_backup_start_ts = cur_sys_info.status_.start_ts_;
  int64_t min_backup_checkpoint_ts = last_update_tenant_ts_;
  hash::ObHashMap<uint64_t, ObTenantLogArchiveStatus*> log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info));
  } else if (OB_FAIL(prepare_tenant_beginning_status_(sys_info, tenant_ids_))) {
    LOG_WARN("failed to prepare_tenant_log_archive_status", K(ret));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(sys_info, allocator, log_archive_status_map))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObTenantLogArchiveStatus* status = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      continue;
    }
    ++user_tenant_count;
    if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
      LOG_WARN("failed to get tenant log archive status", K(ret), K(tenant_id));
    } else if (status->status_ != ObLogArchiveStatus::DOING) {
      LOG_WARN("[LOG_ARCHIVE] log archive status is not doing, waiting for start", K(ret), K(tenant_id), K(*status));
    } else if (status->start_ts_ <= 0) {
      ret = OB_EAGAIN;
      LOG_WARN("invalid start ts, retry later", K(ret), K(tenant_id), K(*status));
    } else if (status->checkpoint_ts_ <= 0) {
      ret = OB_EAGAIN;
      LOG_WARN("invalid checkpoint ts, retry later", K(ret), K(tenant_id), K(*status));
    } else {
      ++doing_tenant_count;
      if (max_backup_start_ts < status->start_ts_) {
        max_backup_start_ts = status->start_ts_;
        FLOG_INFO("update max_backup_start_ts", K(max_backup_start_ts), K(*status));
      }
      if (min_backup_checkpoint_ts > status->checkpoint_ts_) {
        min_backup_checkpoint_ts = status->checkpoint_ts_;
        FLOG_INFO("update min_backup_checkpoint_ts", K(min_backup_checkpoint_ts), K(*status));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (doing_tenant_count != user_tenant_count) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_WARN("[LOG_ARCHIVE] doing tenant count not match user tenant count, waiting for start",
          K(ret),
          K(doing_tenant_count),
          K(user_tenant_count),
          K(max_backup_start_ts));
    } else if (OB_FAIL(tenant_name_mgr_.do_update(true /*is_force*/))) {
      LOG_WARN("failed to update tenant name id mapping", K(ret));
    } else if (0 == user_tenant_count) {
      max_backup_start_ts = ObTimeUtil::current_time();
      min_backup_checkpoint_ts = ObTimeUtil::current_time();
      FLOG_INFO("[LOG_ARCHIVE] no user tenant, use now as log archive start ts",
          K(ret),
          K(user_tenant_count),
          K(max_backup_start_ts));
    } else if (max_backup_start_ts > min_backup_checkpoint_ts) {
      ret = OB_EAGAIN;
      LOG_WARN("start ts is larger than checkpoint ts, waiting start",
          K(ret),
          K(max_backup_start_ts),
          K(min_backup_checkpoint_ts));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
        const uint64_t tenant_id = tenant_ids_.at(i);
        ObTenantLogArchiveStatus* status = nullptr;
        ObLogArchiveBackupInfo tenant_info = sys_info;
        tenant_info.status_.tenant_id_ = tenant_id;

        if (tenant_id < OB_USER_TENANT_ID) {
          continue;
        }

        if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
          LOG_WARN("failed to get tenant log archive status", K(ret));
        } else {
          status->start_ts_ = max_backup_start_ts;
          if (OB_FAIL(tenant_info.status_.update(*status))) {
            LOG_WARN("failed to update tenant info status", K(ret), K(tenant_info), K(status));
          } else if (OB_FAIL(start_tenant_log_archive_backup_info_(tenant_info))) {
            LOG_WARN("failed to start tenant log archive backup", K(ret), K(tenant_info), K(*status));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    sys_info.status_.status_ = ObLogArchiveStatus::DOING;
    sys_info.status_.start_ts_ = max_backup_start_ts;
    sys_info.status_.checkpoint_ts_ = min_backup_checkpoint_ts;

    if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit log archive schedule", K(sys_info), K(last_update_tenant_ts_));
      ROOTSERVICE_EVENT_ADD("log_archive", "change_status", "new_status", "DOING", "round", sys_info.status_.round_);
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::prepare_tenant_beginning_status_(
    const share::ObLogArchiveBackupInfo& sys_info, ObArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  share::ObLogArchiveBackupInfo tenant_info = sys_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (ObLogArchiveStatus::BEGINNING != sys_info.status_.status_ &&
             ObLogArchiveStatus::DOING != sys_info.status_.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("no need to prepare_tenant_beginning_status_ for other status", K(ret), K(sys_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      tenant_info.status_.tenant_id_ = tenant_ids.at(i);
      tenant_info.status_.status_ = ObLogArchiveStatus::BEGINNING;
      if (tenant_info.status_.tenant_id_ < OB_USER_TENANT_ID) {
        // do nothing for sys tenant
      } else if (OB_FAIL(prepare_tenant_beginning_status_(tenant_info))) {
        LOG_WARN("Failed to prepare tenant log archive status", K(ret), K(tenant_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::prepare_tenant_beginning_status_(const share::ObLogArchiveBackupInfo& new_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo inner_info;  // read from inner table
  ObLogArchiveBackupInfoMgr info_mgr;
  bool need_prepared = true;
  DEBUG_SYNC(PREPARE_TENANT_BEGINNING_STATUS);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, new_info.status_.tenant_id_, inner_info))) {
      LOG_WARN("failed to get log archive backup info", K(ret), K(new_info));
    } else if ((ObLogArchiveStatus::BEGINNING == inner_info.status_.status_ ||
                   ObLogArchiveStatus::DOING == inner_info.status_.status_) &&
               new_info.status_.round_ == inner_info.status_.round_) {
      need_prepared = false;
      LOG_DEBUG("tenant is beginning or doing, do nothing", K(ret), K(inner_info), K(new_info));
    } else if (inner_info.status_.status_ != ObLogArchiveStatus::STOP) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_ERROR("tenant log archive not match, cannot begin new log archive round", K(ret), K(inner_info), K(new_info));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, new_info))) {
      LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(new_info));
    } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(new_info, *backup_lease_service_))) {
      LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(new_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }

  if (need_prepared) {
    FLOG_INFO("finish prepare_tenant_beginning_status_", K(ret), K(new_info), K(inner_info));
    DEBUG_SYNC(AFTER_PREPARE_TENANT_BEGINNING_STATUS);
  }
  return ret;
}

int ObLogArchiveScheduler::start_tenant_log_archive_backup_info_(share::ObLogArchiveBackupInfo& new_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo inner_info;  // read from inner table
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, new_info.status_.tenant_id_, inner_info))) {
      LOG_WARN("failed to get log archive backup info", K(ret));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, new_info))) {
      LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(new_info));
    } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(new_info, *backup_lease_service_))) {
      LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(new_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret));
      }
    }
  }

  FLOG_INFO("[LOG_ARCHIVE] finish update_tenant_log_archive_backup_info_", K(ret), K(new_info), K(inner_info));
  return ret;
}

int ObLogArchiveScheduler::update_log_archive_backup_process_(const share::ObLogArchiveBackupInfo& cur_sys_info)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t doing_tenant_count = 0;
  int64_t checkpoint_ts = last_update_tenant_ts_;
  hash::ObHashMap<uint64_t, ObTenantLogArchiveStatus*> log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;

  LOG_INFO("do update_log_archive_backup_process_", K(tenant_ids_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(prepare_tenant_beginning_status_(sys_info, tenant_ids_))) {
    LOG_WARN("failed to prepare_tenant_log_archive_status", K(ret));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(sys_info, allocator, log_archive_status_map))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObTenantLogArchiveStatus* status = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      continue;
    }
    ++user_tenant_count;
    if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
      LOG_WARN("failed to get tenant log archive status", K(ret));
    } else if (sys_info.status_.incarnation_ != status->incarnation_ || sys_info.status_.round_ != status->round_) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_WARN("tenant status not match", K(ret), K(tenant_id), K(sys_info), K(*status));
    } else if (status->status_ == ObLogArchiveStatus::INTERRUPTED) {
      ret = OB_LOG_ARCHIVE_INTERRUPTED;
      LOG_ERROR("[LOG_ARCHIVE] sys log archive backup is doing, but tenant log archive status is interrupted",
          K(ret),
          K(tenant_id),
          K(*status),
          K(sys_info));
    } else if (status->status_ != ObLogArchiveStatus::DOING) {
      LOG_WARN("[LOG_ARCHIVE] sys log archive backup is doing, but tenant log archive status is not doing",
          K(ret),
          K(tenant_id),
          K(*status),
          K(sys_info));
    } else if (status->checkpoint_ts_ < sys_info.status_.checkpoint_ts_) {
      FLOG_INFO("tenant status checkpoint ts is smaller than sys tenant",
          "tenant_status",
          *status,
          "sys_status",
          sys_info.status_);
    } else {
      ++doing_tenant_count;
      if (checkpoint_ts > status->checkpoint_ts_) {
        checkpoint_ts = status->checkpoint_ts_;
        FLOG_INFO("update checkpoint_ts during update_log_archive_backup_process", K(checkpoint_ts), K(*status));
      }
      if (OB_FAIL(update_tenant_log_archive_backup_process_(sys_info, *status))) {
        LOG_WARN("failed to start tenant log archive backup", K(ret), K(sys_info), K(*status));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == user_tenant_count) {
      checkpoint_ts = ObTimeUtil::current_time();
      FLOG_INFO("no tenant exist, just update log archive process", K(ret), K(checkpoint_ts));
    }
  }

  if (OB_SUCC(ret)) {
    if (sys_info.status_.checkpoint_ts_ > checkpoint_ts) {
      ret = OB_ERR_SYS;
      LOG_ERROR("checkpoint ts should not less prev value", K(ret), K(checkpoint_ts), K(sys_info));
    } else if (doing_tenant_count != user_tenant_count) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_WARN("[LOG_ARCHIVE] doing tenant count not match user tenant count, cannot push sys log archive process",
          K(ret),
          K(doing_tenant_count),
          K(user_tenant_count),
          K(checkpoint_ts),
          K(sys_info));
    } else if (FALSE_IT(sys_info.status_.checkpoint_ts_ = checkpoint_ts)) {
    } else if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit update log archive process", K(sys_info), K(last_update_tenant_ts_));
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::update_tenant_log_archive_backup_process_(
    const share::ObLogArchiveBackupInfo& sys_info, const share::ObTenantLogArchiveStatus& tenant_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo info;
  const bool for_update = true;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (tenant_status.tenant_id_ < OB_USER_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("should not use sys tenant in this fun", K(ret), K(tenant_status));
  } else if (!tenant_status.is_valid() || !sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info), K(tenant_status));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_status.tenant_id_, info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(tenant_status));
    }

    if (OB_SUCC(ret)) {
      if (ObLogArchiveStatus::STOP == info.status_.status_) {
        if (0 == info.status_.round_) {
          FLOG_INFO("tenant is new create, use new info", K(ret), K(info), K(sys_info), K(tenant_status));
          info = sys_info;
          info.status_ = tenant_status;
        } else {
          ret = OB_INVALID_LOG_ARCHIVE_STATUS;
          LOG_ERROR("tenant log archive round not match", K(ret), K(info), K(sys_info), K(tenant_status));
        }
      } else if (OB_FAIL(info.status_.update(tenant_status))) {
        LOG_WARN("failed to update inner info", K(ret), K(info), K(tenant_status));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, info))) {
        LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(info));
      } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(info, *backup_lease_service_))) {
        LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {  // commit
        LOG_WARN("failed to commit", K(ret), K(info));
      } else {
        LOG_INFO("succeed to update_tenant_log_archive_backup_info", K(ret), K(info), K(tenant_status));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {  // rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(info), K(tenant_status));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::force_stop_log_archive_backup_(const share::ObLogArchiveBackupInfo& cur_sys_info)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t stop_tenant_count = 0;
  hash::ObHashMap<uint64_t, ObTenantLogArchiveStatus*> log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;

  schema::ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(sys_info, allocator, log_archive_status_map))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    ObTenantLogArchiveStatus* status = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      continue;
    }
    if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
      LOG_WARN("failed to get tenant log archive status", K(ret));
    } else if (status->round_ != sys_info.status_.round_ || status->incarnation_ != sys_info.status_.incarnation_) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_ERROR("[LOG_ARCHIVE] log archive round for incarnation not match", K(ret), K(cur_sys_info), K(*status));
    } else {
      ++user_tenant_count;
      if (status->status_ == ObLogArchiveStatus::STOP) {
        ++stop_tenant_count;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (stop_tenant_count != user_tenant_count) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_WARN("[LOG_ARCHIVE] stop tenant count not match user tenant count, waiting stop",
          K(ret),
          K(stop_tenant_count),
          K(user_tenant_count));
    } else {
      sys_info.status_.status_ = ObLogArchiveStatus::STOP;
      FLOG_INFO("all tenant log archive is stop", K(stop_tenant_count), K(user_tenant_count));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (tenant_id < OB_USER_TENANT_ID) {
        continue;
      } else if (OB_FAIL(check_can_do_work_())) {
        LOG_WARN("failed to check can backup", K(ret));
      } else if (OB_FAIL(stop_tenant_log_archive_backup_(tenant_id, sys_info, true /*force_stop*/))) {
        LOG_WARN("failed to stop_tenant_log_archive_backup_", K(ret), K(tenant_id), K(sys_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(info_mgr.update_log_archive_status_history(*sql_proxy_, sys_info, *backup_lease_service_))) {
      LOG_WARN("failed to update_log_archive_status_history", K(ret), K(sys_info));
    } else if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("Failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit update log archive stop for dead backup dest", K(sys_info));
      ROOTSERVICE_EVENT_ADD("log_archive",
          "change_status",
          "new_status",
          "STOP",
          "round",
          sys_info.status_.round_,
          "comment",
          "force stop for dead backup dest");
    }
  }
  return ret;
}

int ObLogArchiveScheduler::stop_log_archive_backup_(const share::ObLogArchiveBackupInfo& cur_sys_info)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t stop_tenant_count = 0;
  hash::ObHashMap<uint64_t, ObTenantLogArchiveStatus*> log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(sys_info, allocator, log_archive_status_map))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObTenantLogArchiveStatus* status = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      continue;
    }
    if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
      LOG_WARN("failed to get tenant log archive status", K(ret));
    } else if (status->round_ != sys_info.status_.round_ || status->incarnation_ != sys_info.status_.incarnation_) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_ERROR("[LOG_ARCHIVE] log archive round for incarnation not match", K(ret), K(sys_info), K(*status));
    } else {
      ++user_tenant_count;
      if (status->status_ == ObLogArchiveStatus::STOP) {
        ++stop_tenant_count;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (stop_tenant_count != user_tenant_count) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_WARN("[LOG_ARCHIVE] stop tenant count not match user tenant count, waiting stop",
          K(ret),
          K(stop_tenant_count),
          K(user_tenant_count));
    } else {
      sys_info.status_.status_ = ObLogArchiveStatus::STOP;
      FLOG_INFO("all tenant log archive is stop", K(stop_tenant_count), K(user_tenant_count), K(sys_info));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
      const uint64_t tenant_id = tenant_ids_.at(i);
      if (tenant_id < OB_USER_TENANT_ID) {
        continue;
      } else if (OB_FAIL(stop_tenant_log_archive_backup_(tenant_id, sys_info, false /*force stop*/))) {
        LOG_WARN("failed to stop_tenant_log_archive_backup_", K(ret), K(tenant_id), K(sys_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(info_mgr.update_log_archive_status_history(*sql_proxy_, sys_info, *backup_lease_service_))) {
      LOG_WARN("failed to update_log_archive_status_history", K(ret), K(sys_info));
    } else if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit update log archive stop", K(sys_info));
      ROOTSERVICE_EVENT_ADD("log_archive", "change_status", "new_status", "STOP", "round", sys_info.status_.round_);
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::stop_tenant_log_archive_backup_(
    const uint64_t tenant_id, const ObLogArchiveBackupInfo& sys_info, const bool force_stop)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo info;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (tenant_id < OB_USER_TENANT_ID || ObLogArchiveStatus::STOP != sys_info.status_.status_ ||
             !sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(sys_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(sys_info));
    }

    if (OB_SUCC(ret)) {
      if (info.status_.incarnation_ != sys_info.status_.incarnation_) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not support diff incarnation", K(ret), K(info), K(sys_info));
      } else if (ObLogArchiveStatus::STOP != info.status_.status_) {
        info.status_.status_ = ObLogArchiveStatus::STOP;
        if (OB_FAIL(info_mgr.update_log_archive_status_history(*sql_proxy_, info, *backup_lease_service_))) {
          LOG_WARN("failed to update log archive status history", K(ret));
        }
      } else {  //  same STOP
        if (sys_info.status_.round_ == info.status_.round_) {
          if (ObLogArchiveStatus::STOP != sys_info.status_.status_) {
            ret = OB_INVALID_LOG_ARCHIVE_STATUS;
            LOG_ERROR("tenant log archive is stopped, bug new info status not match", K(ret), K(info), K(sys_info));
          } else {
            LOG_INFO("no need to update stop status", K(info), K(sys_info));
          }
        } else if (0 == info.status_.round_ || sys_info.status_.round_ == info.status_.round_ + 1) {
          LOG_INFO("find new stop round, use sys stop status", K(ret), K(info), K(sys_info));
          info = sys_info;
          info.status_.tenant_id_ = tenant_id;
          if (OB_FAIL(info_mgr.update_log_archive_status_history(*sql_proxy_, info, *backup_lease_service_))) {
            LOG_WARN("failed to update log archive status history", K(ret));
          }
        } else {
          ret = OB_INVALID_LOG_ARCHIVE_STATUS;
          LOG_ERROR("tenant log archive round not match", K(ret), K(info), K(sys_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, info))) {
        LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(info));
      } else if (force_stop) {
        LOG_INFO("skip update extern log archive backup info during force_stop", K(info));
      } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(info, *backup_lease_service_))) {
        LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {  // commit
        LOG_WARN("failed to commit", K(ret), K(info));
      } else {
        LOG_INFO("succeed to update_tenant_log_archive_backup_info", K(ret), K(info), K(sys_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {  // rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(info), K(sys_info));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::set_log_archive_backup_interrupted_(const share::ObLogArchiveBackupInfo& cur_sys_info)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else {
    share::ObLogArchiveBackupInfo sys_info = cur_sys_info;
    sys_info.status_.status_ = ObLogArchiveStatus::INTERRUPTED;

    if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit update log archive interrupted", K(sys_info));
      ROOTSERVICE_EVENT_ADD(
          "log_archive", "change_status", "new_status", "INTERRUPTED", "round", sys_info.status_.round_);
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::set_tenants_log_archive_backup_interrupted_(share::ObLogArchiveBackupInfo& sys_info)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObTenantLogArchiveStatus* status = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      continue;
    }
    if (OB_FAIL(set_tenant_log_archive_backup_interrupted_(tenant_id, sys_info))) {
      LOG_WARN("failed to start tenant log archive backup", K(ret), K(sys_info));
    }
  }

  if (OB_SUCC(ret)) {
    const bool enable_log_archive = GCONF.enable_log_archive;
    if (enable_log_archive) {
      if (OB_FAIL(set_enable_log_archive_(false /*is_enable*/))) {
        LOG_WARN("failed to set_enable_log_archive_ false", K(ret));
      } else {
        FLOG_INFO("suceed to set_enable_log_archive_ false");
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::set_tenant_log_archive_backup_interrupted_(
    const uint64_t tenant_id, const ObLogArchiveBackupInfo& sys_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo info;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (tenant_id < OB_USER_TENANT_ID || ObLogArchiveStatus::INTERRUPTED != sys_info.status_.status_ ||
             !sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(sys_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(sys_info));
    }

    if (OB_SUCC(ret)) {
      if (info.status_.incarnation_ != sys_info.status_.incarnation_) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not support diff incarnation", K(ret), K(info), K(sys_info));
      } else if (ObLogArchiveStatus::INTERRUPTED == info.status_.status_) {
        // do nothing
      } else if (ObLogArchiveStatus::DOING == info.status_.status_ ||
                 ObLogArchiveStatus::BEGINNING == info.status_.status_) {
        info.status_.status_ = ObLogArchiveStatus::INTERRUPTED;
        if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, info))) {
          LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(info));
        } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(info, *backup_lease_service_))) {
          LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(info));
        }
      } else {
        LOG_WARN("skip set interrupt for tenant log archive status", K(ret), K(sys_info), K(info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {  // commit
        LOG_WARN("failed to commit", K(ret), K(info));
      } else {
        LOG_INFO("[LOG_ARCHIVE] succeed to update_tenant_log_archive_backup_info", K(ret), K(info), K(sys_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {  // rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(info), K(sys_info));
      }
    }
  }

  return ret;
}
int ObLogArchiveScheduler::fetch_log_archive_backup_status_map_(const share::ObLogArchiveBackupInfo& sys_info,
    ObArenaAllocator& allocator,
    common::hash::ObHashMap<uint64_t, share::ObTenantLogArchiveStatus*>& log_archive_status_map)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObArray<common::ObAddr> active_server_list;
  ObArray<common::ObAddr> inactive_server_list;
  share::ObGetTenantLogArchiveStatusArg arg;
  share::ObTenantLogArchiveStatusWrapper result;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(server_mgr_->get_servers_by_status(active_server_list, inactive_server_list))) {
    LOG_WARN("failed to get servers by status", K(ret));
  } else if (inactive_server_list.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot support log archive when more than one server is inactive", K(ret), K(inactive_server_list));
  } else if (OB_FAIL(log_archive_status_map.create(active_server_list.count(), ObModIds::OB_LOG_ARCHIVE_SCHEDULER))) {
    LOG_WARN("failed to create hashmap", K(ret));
  } else {
    arg.incarnation_ = sys_info.status_.incarnation_;
    arg.round_ = sys_info.status_.round_;
  }

  // TODO(): [backup2] use async rpc
  for (int64_t server_idx = 0; OB_SUCC(ret) && server_idx < active_server_list.count(); ++server_idx) {
    const common::ObAddr& addr = active_server_list.at(server_idx);
    if (OB_FAIL(check_can_do_work_())) {
      LOG_WARN("failed to check can do work", K(ret));
    } else if (OB_FAIL(rpc_proxy_->to(addr).get_tenant_log_archive_status(arg, result))) {
      LOG_WARN("failed to get_tenant_log_archive_status", K(ret), K(addr), K(arg));
    } else if (OB_FAIL(result.result_code_)) {
      if (OB_LOG_ARCHIVE_INTERRUPTED == ret) {
        LOG_ERROR("server log archive status is interrupted", K(ret), K(addr), K(result));
      } else {
        LOG_WARN("Failed to get tenant log archive status", K(ret), K(addr), K(result));
      }
    } else {
      LOG_INFO("succeed to get tenant log archive status", K(addr), "tenant_count", result.status_array_.count());
    }
    const int64_t count = result.status_array_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObTenantLogArchiveStatus& status = result.status_array_.at(i);
      if (!status.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid log archive status", K(ret), K(i), K(addr), K(status));
      } else if (ObLogArchiveStatus::INTERRUPTED == status.status_) {
        ret = OB_LOG_ARCHIVE_INTERRUPTED;
        LOG_ERROR(
            "[LOG_ARCHIVE] log archive status is interrupted, need human intervention", K(ret), K(addr), K(status));
      } else {
        ObTenantLogArchiveStatus* value = nullptr;
        hash_ret = log_archive_status_map.get_refactored(status.tenant_id_, value);
        if (OB_SUCCESS == hash_ret) {
          if (value->status_ != status.status_) {
            ret = OB_EAGAIN;
            LOG_WARN("log archive status not match", K(addr), K(ret), K(status), K(*value));
          }

          if (OB_SUCC(ret)) {
            if (value->checkpoint_ts_ > status.checkpoint_ts_) {
              LOG_INFO("update status checkpoint ts", K(addr), K(*value), K(status));
              value->checkpoint_ts_ = status.checkpoint_ts_;
            }

            if (value->start_ts_ < status.start_ts_) {
              LOG_INFO("update status start ts", K(addr), K(*value), K(status));
              value->start_ts_ = status.start_ts_;
            }
          }
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          void* tmp = allocator.alloc(sizeof(ObTenantLogArchiveStatus));
          if (OB_ISNULL(tmp)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc buf", K(ret));
          } else {
            ObTenantLogArchiveStatus* ptr = new (tmp) ObTenantLogArchiveStatus;
            *ptr = status;
            ptr->is_mount_file_created_ = sys_info.status_.is_mount_file_created_;
            ptr->compatible_ = sys_info.status_.compatible_;
            if (OB_FAIL(log_archive_status_map.set_refactored(status.tenant_id_, ptr))) {
              LOG_WARN("Failed to set log_archive_status_map", K(ret), K(*ptr));
            } else {
              LOG_INFO("add new status", K(addr), K(status));
            }
          }
        } else {
          ret = hash_ret;
          LOG_ERROR("invalid hash ret", K(hash_ret), K(ret), K(status));
        }
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_LOG_ARHIVE_SCHEDULER_INTERRUPT) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_ERROR("fake log archive scheduler interrupt", K(ret));
    }
  }
#endif

  FLOG_INFO("finish fetch_log_archive_backup_status_map", K(ret), K(active_server_list));
  return ret;
}

int ObLogArchiveScheduler::check_can_do_work_()
{
  int ret = OB_SUCCESS;

  if (stop_) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("rootservice is stopping, cannot do log archive scheduler work", K(ret));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  }
  return ret;
}

int ObLogArchiveScheduler::commit_trans_(common::ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(trans.end(true))) {  // commit
    LOG_WARN("failed to commit", K(ret));
  }
  return ret;
}

int ObLogArchiveScheduler::update_sys_backup_info_(
    const share::ObLogArchiveBackupInfo& cur_info, share::ObLogArchiveBackupInfo& new_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo read_info;
  const bool for_update = true;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!cur_info.is_valid() || !new_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_info), K(new_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, new_info.status_.tenant_id_, read_info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(new_info));
    } else if (!cur_info.is_same(read_info)) {
      ret = OB_EAGAIN;
      LOG_WARN("sys info is changed before update", K(ret), K(cur_info), K(read_info), K(new_info));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, new_info))) {
      LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(new_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {  // commit
        LOG_WARN("failed to commit", K(ret), K(new_info));
      } else {
        LOG_INFO("succeed to update_tenant_log_archive_backup_info", K(ret), K(new_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {  // rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(new_info));
      }
    }
  }

  return ret;
}
