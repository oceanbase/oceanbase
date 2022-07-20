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
#include "share/backup/ob_backup_operator.h"
#include "ob_leader_coordinator.h"
#include "ob_server_manager.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_rs_event_history_table_operator.h"
#include "storage/transaction/ob_ts_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace obrpc;
using namespace rootserver;

ObLogArchiveThreadIdling::ObLogArchiveThreadIdling(volatile bool &stop)
    : ObThreadIdling(stop), idle_time_us_(MIN_IDLE_INTERVAL_US)
{}

int64_t ObLogArchiveThreadIdling::get_idle_interval_us()
{
  return idle_time_us_;
}

void ObLogArchiveThreadIdling::set_log_archive_checkpoint_interval(
    const int64_t interval_us, const share::ObLogArchiveStatus::STATUS &status)
{
  const int64_t max_idle_us = interval_us / 2 - RESERVED_FETCH_US;
  int64_t idle_time_us = 0;
  if (interval_us <= 0) {
    idle_time_us = MAX_IDLE_INTERVAL_US;
  } else {
    if (max_idle_us <= MIN_IDLE_INTERVAL_US) {
      idle_time_us = MIN_IDLE_INTERVAL_US;
    } else if (max_idle_us > MAX_IDLE_INTERVAL_US) {
      idle_time_us = MAX_IDLE_INTERVAL_US;
    } else if (ObLogArchiveStatus::BEGINNING == status || ObLogArchiveStatus::STOPPING == status) {
      idle_time_us = FAST_IDLE_INTERVAL_US;
    } else {
      idle_time_us = max_idle_us;
    }
  }

  if (idle_time_us != idle_time_us_) {
    FLOG_INFO("[LOG_ARCHIVE] change idle_time_us", K(idle_time_us_), K(idle_time_us), K(status));
    idle_time_us_ = idle_time_us;
  }
}

ObLogArchiveScheduler::ObLogArchiveScheduler()
    : is_inited_(false),
      idling_(stop_),
      schema_service_(nullptr),
      server_mgr_(nullptr),
      zone_mgr_(nullptr),
      rpc_proxy_(nullptr),
      sql_proxy_(nullptr),
      is_working_(false),
      tenant_ids_(),
      last_update_tenant_ts_(0),
      tenant_name_mgr_(),
      backup_lease_service_(nullptr),
      inner_table_version_(OB_BACKUP_INNER_TABLE_VMAX)
{}

ObLogArchiveScheduler::~ObLogArchiveScheduler()
{
  destroy();
}

int ObLogArchiveScheduler::init(ObServerManager &server_mgr, ObZoneManager &zone_mgr,
    share::schema::ObMultiVersionSchemaService *schema_service, ObSrvRpcProxy &rpc_proxy,
    common::ObMySQLProxy &sql_proxy, share::ObIBackupLeaseService &backup_lease_service)
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
    zone_mgr_ = &zone_mgr;
    schema_service_ = schema_service;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    tenant_ids_.reset();
    last_update_tenant_ts_ = 0;
    backup_lease_service_ = &backup_lease_service;
    inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;
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
  ObBackupInnerTableVersion inner_table_version = OB_BACKUP_INNER_TABLE_VMAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(ObBackupInfoOperator::get_inner_table_version(*sql_proxy_, inner_table_version))) {
    LOG_WARN("failed to get backup inner table version", K(ret));
  } else if (!is_valid_backup_inner_table_version(inner_table_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", K(ret), K(inner_table_version));
  } else if (is_enable) {
    if (inner_table_version < OB_BACKUP_INNER_TABLE_V2) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("start log archive before upgrade backup inner table is not supported", K(ret), K(inner_table_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "start/stop log archive before upgrade backup inner table is");
    } else if (OB_FAIL(handle_start_log_archive_(inner_table_version))) {
      LOG_WARN("failed to handle start log archive", K(ret));
    }
  } else {
    if (OB_FAIL(handle_stop_log_archive_(inner_table_version))) {
      LOG_WARN("failed to handle stop log archive", K(ret));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::handle_stop_log_archive_(const ObBackupInnerTableVersion &inner_table_version)
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
    if (OB_FAIL(
            info_mgr.get_log_archive_backup_info(trans, for_update, OB_SYS_TENANT_ID, inner_table_version, sys_info))) {
      LOG_WARN("failed to get log archive backup sys_info", K(ret));
    } else if (OB_FAIL(sys_info.status_.status_ == ObLogArchiveStatus::STOP ||
                       sys_info.status_.status_ == ObLogArchiveStatus::STOPPING)) {
      ret = OB_ALREADY_NO_LOG_ARCHIVE_BACKUP;
      LOG_WARN("log archive backup is stop or stopping, no need to noarchivelog", K(ret), K(sys_info));
    } else if (OB_FAIL(set_enable_log_archive_(false /*is_enable*/))) {
      LOG_WARN("failed to enable log archive", K(ret));
    } else {
      sys_info.status_.status_ = ObLogArchiveStatus::STOPPING;
      if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version, sys_info))) {
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

  ROOTSERVICE_EVENT_ADD("log_archive",
      "handle_stop_log_archive",
      "status",
      "STOPPING",
      "round_id",
      sys_info.status_.round_,
      "result",
      ret);
  return ret;
}

int ObLogArchiveScheduler::handle_start_log_archive_(const ObBackupInnerTableVersion &inner_table_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo sys_info;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfoMgr info_mgr;
  char backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  const ObClusterType cluster_type = ObClusterInfoGetter::get_cluster_type();
  ObBackupInfoChecker checker;
  ObBackupPieceInfo sys_piece_info;
  int64_t max_piece_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(wait_backup_dest_(backup_dest, sizeof(backup_dest)))) {
    LOG_WARN("failed to wait backup dest, cannot do schedule log archive", K(ret));
  } else if (PRIMARY_CLUSTER != cluster_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not primary cluster cannot backup now", K(ret), K(cluster_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup in not primary cluster is");
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(
            info_mgr.get_log_archive_backup_info(trans, for_update, OB_SYS_TENANT_ID, inner_table_version, sys_info))) {
      LOG_WARN("failed to get log archive backup sys_info", K(ret));
    } else if (sys_info.status_.status_ != ObLogArchiveStatus::STOP) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "start log archive backup when not STOP is");
      LOG_WARN("cannot start log archive backup in this status", K(ret), K(sys_info));
    } else if (OB_FAIL(check_gts_())) {
      LOG_WARN("failed to check gts", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::get_max_piece_id(trans, for_update, max_piece_id))) {
      LOG_WARN("failed to get max piece id", K(ret));
    } else if (OB_FAIL(init_new_sys_info_(backup_dest, max_piece_id, sys_info))) {
      LOG_WARN("failed to init new sys info", K(ret));
    } else if (OB_FAIL(init_new_sys_piece_(sys_info, sys_piece_info))) {
      LOG_WARN("failed to init new piece", K(ret), K(sys_info));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version, sys_info))) {
      LOG_WARN("Failed to update log archive backup info", K(ret), K(inner_table_version), K(sys_info));
    } else if (OB_FAIL(info_mgr.update_backup_piece(trans, sys_piece_info))) {
      LOG_WARN("failed to update log archive piece info", K(ret), K(sys_piece_info));
    } else if (OB_FAIL(ObBackupInfoOperator::set_max_piece_id(trans, sys_info.status_.backup_piece_id_))) {
      LOG_WARN("failed to set max piece id", K(ret), K(sys_piece_info));
    } else if (OB_FAIL(ObBackupInfoOperator::set_max_piece_create_date(trans, sys_piece_info.create_date_))) {
      LOG_WARN("failed to set max piece create date", K(ret), K(sys_piece_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("[LOG_ARCHIVE] succeed to handle enable log archive", K(sys_info), K(sys_piece_info));
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
      "round_id",
      sys_info.status_.round_,
      "piece_id",
      sys_piece_info.key_.backup_piece_id_,
      "result",
      ret);
  return ret;
}

int ObLogArchiveScheduler::check_gts_()
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  bool is_gts = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get tenant its", K(ret));
  } else if (OB_FAIL(ObBackupUtils::check_user_tenant_gts(*schema_service_, tenant_ids, is_gts))) {
    LOG_WARN("fail to get tenant schema guard to determine tenant gts type", KR(ret), K(tenant_ids));
  } else if (!is_gts) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("physical backup without GTS is not supported", K(ret), K(is_gts), K(tenant_ids));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "physical backup without GTS is");
  }
  return ret;
}

int ObLogArchiveScheduler::init_new_sys_info_(
    const char *backup_dest, const int64_t max_piece_id, share::ObLogArchiveBackupInfo &sys_info)
{
  int ret = OB_SUCCESS;
  bool is_backup_backup = false;
  ObBackupDestOpt backup_dest_opt;

  if (OB_FAIL(backup_dest_opt.init(is_backup_backup))) {
    LOG_WARN("failed to get_backup_dest_opt", K(ret));
  } else if (OB_FAIL(databuff_printf(sys_info.backup_dest_, sizeof(sys_info.backup_dest_), "%s", backup_dest))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(backup_dest));
  } else {
    // init info
    sys_info.status_.round_++;
    if (backup_dest_opt.piece_switch_interval_ > 0) {
      if (max_piece_id + 1 <= sys_info.status_.backup_piece_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("max piece id must larger than sys info backup piece id", K(ret), K(max_piece_id), K(sys_info));
      } else {
        sys_info.status_.backup_piece_id_ = max_piece_id + 1;
      }

      sys_info.status_.start_piece_id_ = sys_info.status_.backup_piece_id_;
    } else {
      sys_info.status_.start_piece_id_ = 0;
    }

    sys_info.status_.status_ = ObLogArchiveStatus::BEGINNING;
    sys_info.status_.incarnation_ = OB_START_INCARNATION;
    sys_info.status_.start_ts_ = ObTimeUtil::current_time();
    sys_info.status_.checkpoint_ts_ = 0;
    sys_info.status_.is_mount_file_created_ = false;
    sys_info.status_.compatible_ = ObTenantLogArchiveStatus::COMPATIBLE::COMPATIBLE_VERSION_1;
    FLOG_INFO("[LOG_ARCHIVE] init new sys info", K(ret), K(max_piece_id), K(sys_info), K(backup_dest_opt));
  }
  return ret;
}

int ObLogArchiveScheduler::init_new_sys_piece_(
    const share::ObLogArchiveBackupInfo &sys_info, share::ObBackupPieceInfo &sys_piece)
{
  int ret = OB_SUCCESS;

  if (!sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info));
  } else if (OB_FAIL(sys_info.get_piece_key(sys_piece.key_))) {
    LOG_WARN("Failed to get piece key", K(ret), K(sys_info));
  } else if (OB_FAIL(sys_piece.backup_dest_.assign(sys_info.backup_dest_))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(sys_info));
  } else if (OB_FAIL(ObBackupUtils::get_snapshot_to_time_date(ObTimeUtil::current_time(), sys_piece.create_date_))) {
    LOG_WARN("failed to get create date", K(ret));
  } else {
    sys_piece.start_ts_ = sys_info.status_.start_ts_;  // not decided yet, maybe change
    sys_piece.checkpoint_ts_ = 0;                      // 0 means not decided
    sys_piece.max_ts_ = INT64_MAX;
    sys_piece.status_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE;
    sys_piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
    sys_piece.compatible_ = sys_info.status_.compatible_;
    sys_piece.start_piece_id_ = sys_info.status_.start_piece_id_;
  }
  return ret;
}

int ObLogArchiveScheduler::init_new_sys_piece_for_compat_(
    const share::ObLogArchiveBackupInfo &sys_info, share::ObBackupPieceInfo &sys_piece)
{
  int ret = OB_SUCCESS;

  if (!sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info));
  } else if (OB_FAIL(sys_info.get_piece_key(sys_piece.key_))) {
    LOG_WARN("Failed to get piece key", K(ret), K(sys_info));
  } else if (OB_FAIL(sys_piece.backup_dest_.assign(sys_info.backup_dest_))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(sys_info));
  } else {
    sys_piece.create_date_ = 0;
    sys_piece.start_ts_ = sys_info.status_.start_ts_;
    sys_piece.checkpoint_ts_ = sys_info.status_.checkpoint_ts_;
    sys_piece.max_ts_ = INT64_MAX;
    sys_piece.status_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE;
    sys_piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
    sys_piece.compatible_ = sys_info.status_.compatible_;
    sys_piece.start_piece_id_ = sys_info.status_.start_piece_id_;
  }
  return ret;
}

int ObLogArchiveScheduler::init_next_sys_piece_(const share::ObLogArchiveBackupInfo &sys_info,
    const share::ObBackupPieceInfo &sys_piece, share::ObLogArchiveBackupInfo &new_sys_info,
    share::ObNonFrozenBackupPieceInfo &new_sys_pieces)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t max_piece_id = 0;
  int64_t piece_create_date = 0;
  const bool for_update = true;
  new_sys_pieces.reset();

  if (!sys_info.is_valid() || !sys_piece.is_valid() ||
      sys_info.status_.backup_piece_id_ != sys_piece.key_.backup_piece_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info), K(sys_piece));
  } else if (sys_info.status_.start_piece_id_ <= 0 || sys_piece.key_.backup_piece_id_ <= 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid start piece id or backup piece id", K(ret), K(sys_info), K(sys_piece));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    new_sys_info = sys_info;
    new_sys_pieces.has_prev_piece_info_ = true;
    new_sys_pieces.prev_piece_info_ = sys_piece;
    new_sys_pieces.prev_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FREEZING;
    new_sys_pieces.prev_piece_info_.checkpoint_ts_ = sys_info.status_.checkpoint_ts_;

    if (OB_FAIL(ObBackupInfoOperator::get_max_piece_id(trans, for_update, max_piece_id))) {
      LOG_WARN("failed to get max piece id", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::get_max_piece_create_date(trans, for_update, piece_create_date))) {
      LOG_WARN("failed to get max piece create date", K(ret));
    } else if (max_piece_id == sys_info.status_.backup_piece_id_) {
      ++max_piece_id;
      new_sys_info.status_.backup_piece_id_ = max_piece_id;
      if (OB_FAIL(init_new_sys_piece_(new_sys_info, new_sys_pieces.cur_piece_info_))) {
        LOG_WARN("failed to init new sys piece", K(ret), K(new_sys_info));
      } else {
        LOG_INFO("init next sys piece", K(new_sys_pieces));
      }
    } else if (max_piece_id == sys_info.status_.backup_piece_id_ + 1) {
      new_sys_info.status_.backup_piece_id_ = max_piece_id;
      if (OB_FAIL(init_new_sys_piece_(new_sys_info, new_sys_pieces.cur_piece_info_))) {
        LOG_WARN("failed to init new sys piece", K(ret), K(new_sys_info));
      } else {
        new_sys_pieces.cur_piece_info_.create_date_ = piece_create_date;
        LOG_INFO("init next sys piece with recored piece id and create date",
            K(new_sys_pieces),
            K(max_piece_id),
            K(piece_create_date));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid max piece id", K(ret), K(max_piece_id), K(sys_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObBackupInfoOperator::set_max_piece_id(trans, new_sys_info.status_.backup_piece_id_))) {
        LOG_WARN("failed to set max piece id", K(ret), K(new_sys_pieces));
      } else if (OB_FAIL(ObBackupInfoOperator::set_max_piece_create_date(
                     trans, new_sys_pieces.cur_piece_info_.create_date_))) {
        LOG_WARN("failed to set max piece create date", K(ret), K(new_sys_pieces));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        LOG_WARN("failed to commit trans", K(ret));
      } else {
        FLOG_INFO("succeed to record new piece id",
            K(max_piece_id),
            K(piece_create_date),
            K(new_sys_pieces),
            K(new_sys_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::init_next_user_piece_array_(const share::ObNonFrozenBackupPieceInfo &new_sys_piece,
    const TENANT_ARCHIVE_STATUS_MAP &log_archive_status_map,
    common::ObArray<share::ObNonFrozenBackupPieceInfo> &new_user_piece_array)
{
  int ret = OB_SUCCESS;
  share::ObServerTenantLogArchiveStatus *backup_status;

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObNonFrozenBackupPieceInfo new_user_piece;
    if (OB_FAIL(check_can_do_work_())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, backup_status))) {
      LOG_WARN("failed to get backup status", K(ret));
    } else if (OB_FAIL(
                   init_next_user_piece_(new_sys_piece, tenant_id, backup_status->checkpoint_ts_, new_user_piece))) {
      LOG_WARN("failed to init next user piece_", K(ret), K(tenant_id));
    } else if (OB_FAIL(new_user_piece_array.push_back(new_user_piece))) {
      LOG_WARN("failed to push backup new_user_piece", K(ret));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to init next user piece", K(tenant_id), K(*backup_status), K(new_user_piece));
    }
  }
  return ret;
}

int ObLogArchiveScheduler::init_next_user_piece_(const share::ObNonFrozenBackupPieceInfo &new_sys_piece,
    const uint64_t tenant_id, const int64_t checkpoint_ts, ObNonFrozenBackupPieceInfo &user_piece)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObBackupPieceInfoKey prev_piece_key = new_sys_piece.prev_piece_info_.key_;
  ObBackupPieceInfoKey cur_piece_key = new_sys_piece.cur_piece_info_.key_;
  const bool for_update = false;

  prev_piece_key.tenant_id_ = tenant_id;
  cur_piece_key.tenant_id_ = tenant_id;
  user_piece.reset();

  if (OB_FAIL(info_mgr.get_backup_piece(*sql_proxy_, for_update, prev_piece_key, user_piece.prev_piece_info_))) {
    LOG_WARN("failed to get piece info", K(ret), K(prev_piece_key));
  } else if (OB_FAIL(info_mgr.get_backup_piece(*sql_proxy_, for_update, cur_piece_key, user_piece.cur_piece_info_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get piece info", K(ret), K(cur_piece_key));
    } else if (OB_FAIL(user_piece.cur_piece_info_.init_piece_info(new_sys_piece.cur_piece_info_, tenant_id))) {
      LOG_WARN("failed to init piece", K(ret), K(new_sys_piece));
    }
  }

  if (OB_SUCC(ret)) {
    user_piece.has_prev_piece_info_ = true;
    user_piece.prev_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FREEZING;
    user_piece.prev_piece_info_.checkpoint_ts_ = checkpoint_ts;
    LOG_INFO("init next user piece", K(new_sys_piece), K(tenant_id), K(checkpoint_ts), K(user_piece));
  }
  return ret;
}

int ObLogArchiveScheduler::update_external_user_piece_(
    const common::ObIArray<share::ObNonFrozenBackupPieceInfo> &new_user_piece_array)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;

  for (int64_t i = 0; OB_SUCC(ret) && i < new_user_piece_array.count(); ++i) {
    const ObNonFrozenBackupPieceInfo &user_piece = new_user_piece_array.at(i);
    if (OB_FAIL(info_mgr.update_external_backup_piece(user_piece, *backup_lease_service_))) {
      LOG_WARN("failed to update external user piece", K(ret), K(user_piece));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::update_inner_freeze_piece_info_(const share::ObLogArchiveBackupInfo &cur_sys_info,
    const share::ObLogArchiveBackupInfo &new_sys_info, const share::ObNonFrozenBackupPieceInfo &new_sys_piece,
    const ObArray<share::ObNonFrozenBackupPieceInfo> &new_user_piece_array)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  const int64_t timeout = 60 * 1000 * 1000;  // 60s
  ObTimeoutCtx timeout_ctx;
  ObLogArchiveBackupInfo read_sys_info;
  const bool for_update = true;
  int tmp_ret = OB_SUCCESS;

  if (!cur_sys_info.is_valid() || !new_sys_info.is_valid() || !new_sys_piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_sys_info), K(new_sys_info), K(new_sys_piece));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
    LOG_WARN("failed to set timeout", K(ret), K(timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(
            trans, for_update, new_sys_info.status_.tenant_id_, inner_table_version_, read_sys_info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(new_sys_info));
    } else if (!cur_sys_info.is_same(read_sys_info)) {
      ret = OB_EAGAIN;
      LOG_WARN("sys info is changed before update", K(ret), K(cur_sys_info), K(read_sys_info), K(new_sys_info));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, new_sys_info))) {
      LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(new_sys_info));
    } else if (OB_FAIL(info_mgr.update_backup_piece(trans, new_sys_piece))) {
      LOG_WARN("failed to update sys piece", K(ret), K(new_sys_piece));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < new_user_piece_array.count(); ++i) {
      const ObNonFrozenBackupPieceInfo &user_piece = new_user_piece_array.at(i);
      if (OB_FAIL(info_mgr.update_backup_piece(trans, user_piece))) {
        LOG_WARN("failed to update external user piece", K(ret), K(user_piece));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {  // commit
        LOG_WARN("failed to commit", K(ret), K(new_sys_info), K(new_sys_piece));
      } else {
        FLOG_INFO("[LOG_ARCHIVE] freeze new piece",
            K(ret),
            "round",
            new_sys_piece.cur_piece_info_.key_.round_id_,
            "new_piece_id",
            new_sys_piece.cur_piece_info_.key_.backup_piece_id_,
            K(new_sys_info),
            K(new_sys_piece));
        ROOTSERVICE_EVENT_ADD("log_archive",
            "freeze_piece",
            "round_id",
            new_sys_piece.cur_piece_info_.key_.round_id_,
            "piece_id",
            new_sys_piece.cur_piece_info_.key_.backup_piece_id_);
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {  // rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(new_sys_info), K(new_sys_piece));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::wait_backup_dest_(char *buf, const int64_t buf_len)
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
      LOG_ERROR("[LOG_ARCHIVE] backup dest is empty, cannot start backup", K(ret));
      break;
    } else {
      FLOG_INFO("[LOG_ARCHIVE] backup dest is empty, retry after 1s", K(ret));
      ::usleep(SLEEP_INTERVAL_US);
    }
  }

  return ret;
}

void ObLogArchiveScheduler::run3()
{
  int tmp_ret = OB_SUCCESS;
  int64_t round = 0;
  share::ObLogArchiveStatus::STATUS last_log_archive_status = ObLogArchiveStatus::INVALID;

  lib::set_thread_name("LogArchiveScheduler");
  ObCurTraceId::init(GCONF.self_addr_);
  FLOG_INFO("ObLogArchiveScheduler run start");
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
      } else if (OB_SUCCESS != (tmp_ret = check_backup_inner_table_())) {
        LOG_WARN("failed to update meta table", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = do_schedule_(last_log_archive_status))) {
        LOG_WARN("failed to do schedule", K(tmp_ret));
      }

      inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;  // reset inner_table_version_
      int64_t log_archive_checkpoint_interval = GCONF.log_archive_checkpoint_interval;
      ObBackupDestOpt backup_dest_opt;

      if (OB_SUCCESS != (tmp_ret = backup_dest_opt.init(false /*is_backup_backup*/))) {
        LOG_WARN("failed to get_backup_dest_opt", K(tmp_ret));
      } else {
        log_archive_checkpoint_interval = backup_dest_opt.log_archive_checkpoint_interval_;
      }

      idling_.set_log_archive_checkpoint_interval(log_archive_checkpoint_interval, last_log_archive_status);
      if (OB_SUCCESS != (tmp_ret = idling_.idle())) {
        LOG_WARN("failed to to idling", K(tmp_ret));
      }
    }
    is_working_ = false;
  }
  cleanup_();
  FLOG_INFO("ObLogArchiveScheduler run finish");
}

int ObLogArchiveScheduler::check_backup_inner_table_()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObLogArchiveBackupInfo sys_info;
  inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(ObBackupInfoOperator::get_inner_table_version(*sql_proxy_, inner_table_version_))) {
    LOG_WARN("failed to get backup inner table version", K(ret));
  } else if (!is_valid_backup_inner_table_version(inner_table_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", K(ret), K(inner_table_version_));
  } else if (inner_table_version_ >= OB_BACKUP_INNER_TABLE_V2) {
    // do nothing
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(
                 *sql_proxy_, false /*for_update*/, OB_SYS_TENANT_ID, inner_table_version_, sys_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (ObLogArchiveStatus::STOP != sys_info.status_.status_ &&
             ObLogArchiveStatus::DOING != sys_info.status_.status_) {
    LOG_INFO("log archive status is not stop or doing, cannot upgrade inner table", K(sys_info));
  } else if (OB_FAIL(upgrade_backup_inner_table_())) {
    LOG_WARN("failed to upgrade backup inner table", K(ret), K(inner_table_version_));
  }

  return ret;
}

int ObLogArchiveScheduler::upgrade_backup_inner_table_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  FLOG_INFO("start to upgrade backup inner table", K(ret), K(inner_table_version_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(upgrade_log_archive_status_())) {
    LOG_WARN("failed to upgrade log archive status", K(ret));
  } else if (OB_FAIL(upgrade_frozen_piece_files_())) {
    LOG_WARN("failed to upgrade frozen piece", K(ret));
  } else if (OB_FAIL(upgrade_backupset_files_())) {
    LOG_WARN("failed to upgrade backupset files", K(ret));
  } else if (OB_FAIL(upgrade_backup_info_())) {
    LOG_WARN("failed to upgrade backup info", K(ret));
  } else if (OB_SUCCESS != (tmp_ret = clean_discard_log_archive_status_())) {
    LOG_WARN("failed to clean_discard_log_archive_status", K(tmp_ret));
  }
  FLOG_INFO("finish  to upgrade backup inner table", K(ret), K(inner_table_version_));

  return ret;
}

int ObLogArchiveScheduler::clean_discard_log_archive_status_()
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  ObLogArchiveBackupInfoMgr info_mgr;
  // TODO(yongle.xh): delete backup info log archive status after check info moved to data backup thread

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(get_tenant_ids_from_schema_(tenant_ids))) {
    LOG_WARN("failed to get_tenant_ids_from_schema_", K(ret), K(tenant_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(info_mgr.delete_tenant_log_archive_status_v1(*sql_proxy_, tenant_id))) {
        LOG_WARN("failed to delete_tenant_log_archive_status_v1_", K(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(info_mgr.delete_tenant_log_archive_status_v1(*sql_proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to delete_tenant_log_archive_status_v1_ for sys tenant", K(ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::upgrade_log_archive_status_()
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObLogArchiveBackupInfo sys_info;
  ObBackupPieceInfo sys_piece;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(
                 *sql_proxy_, false /*for_update*/, OB_SYS_TENANT_ID, inner_table_version_, sys_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (ObLogArchiveStatus::STOP == sys_info.status_.status_) {
    LOG_INFO("log archive status is stop, only need update log archive status without piece");
    if (OB_FAIL(info_mgr.update_log_archive_backup_info(*sql_proxy_, OB_BACKUP_INNER_TABLE_V2, sys_info))) {
      LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(sys_info));
    }
  } else if (OB_FAIL(init_new_sys_piece_for_compat_(sys_info, sys_piece))) {
    LOG_WARN("failed to init new sys piece", K(ret), K(sys_info));
  } else if (OB_FAIL(get_tenant_ids_from_schema_(tenant_ids))) {
    LOG_WARN("failed to get tenant ids from schema", K(ret));
  } else if (OB_FAIL(upgrade_log_archive_status_(OB_SYS_TENANT_ID, sys_piece))) {
    LOG_WARN("failed to upgrade sys log archive status", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(upgrade_log_archive_status_(tenant_id, sys_piece))) {
        LOG_WARN("failed to upgrade log archive status", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::upgrade_log_archive_status_(
    const uint64_t tenant_id, const share::ObBackupPieceInfo &sys_piece)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo backup_info;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  share::ObBackupPieceInfo backup_piece;
  ObBackupInnerTableVersion new_version = OB_BACKUP_INNER_TABLE_V2;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!sys_piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_piece));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(backup_piece.init_piece_info(sys_piece, tenant_id))) {
    LOG_WARN("failed to init backup piece", K(ret), K(sys_piece), K(tenant_id));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(
                 *sql_proxy_, false /*for_update*/, tenant_id, inner_table_version_, backup_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tenant backup info not exist", K(ret), K(tenant_id));
    } else {
      LOG_WARN("failed to get log archive backup info", K(ret));
    }
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, new_version, backup_info))) {
      LOG_WARN("failed to set log archive backup info", K(ret), K(backup_info));
    } else if (OB_FAIL(info_mgr.update_backup_piece(trans, backup_piece))) {
      LOG_WARN("failed to update backup piece", K(ret), K(backup_piece));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("succeed to upgrade log archive status", K(backup_info), K(backup_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::upgrade_frozen_piece_files_()
{
  int ret = OB_SUCCESS;
  common::ObArray<ObLogArchiveBackupInfo> infos;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  share::ObBackupPieceInfo piece;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(info_mgr.get_all_log_archive_history_infos(*sql_proxy_, infos))) {
    LOG_WARN("failed to get all log archive history infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      ObLogArchiveBackupInfo &info = infos.at(i);
      piece.reset();
      if (OB_FAIL(check_can_do_work_())) {
        LOG_WARN("failed to check can backup", K(ret));
      } else if (OB_FAIL(info.get_piece_key(piece.key_))) {
        LOG_WARN("failed to get piece key", K(ret), K(info));
      } else if (OB_FAIL(piece.backup_dest_.assign(info.backup_dest_))) {
        LOG_WARN("failed to copy backup dest", K(ret), K(info));
      } else {
        piece.create_date_ = 0;
        piece.start_ts_ = info.status_.start_ts_;
        piece.checkpoint_ts_ = info.status_.checkpoint_ts_;
        piece.max_ts_ = info.status_.checkpoint_ts_;
        piece.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
        if (info.status_.is_mark_deleted_) {
          piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETING;
        } else {
          piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
        }
        piece.compatible_ = ObTenantLogArchiveStatus::NONE;
        piece.start_piece_id_ = 0;  // no piece mode
        if (OB_FAIL(info_mgr.update_backup_piece(*sql_proxy_, piece))) {
          LOG_WARN("failed to update backup piece", K(ret), K(piece));
        }
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::upgrade_backupset_files_()
{
  int ret = OB_SUCCESS;
  common::ObArray<ObTenantBackupTaskItem> infos;
  share::ObBackupSetFileInfo file_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_all_tenant_backup_tasks(*sql_proxy_, infos))) {
    LOG_WARN("failed to get all backup task infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      ObTenantBackupTaskItem &info = infos.at(i);
      file_info.reset();
      if (OB_FAIL(check_can_do_work_())) {
        LOG_WARN("failed to check can backup", K(ret));
      } else if (OB_FAIL(file_info.extract_from_backup_task_info(info))) {
        LOG_WARN("failed to extract_from_backup_task_info", K(ret), K(info));
      } else {
        if (OB_FAIL(ObBackupSetFilesOperator::insert_tenant_backup_set_file_info(file_info, *sql_proxy_))) {
          LOG_WARN("failed to insert_tenant_backup_set_file_info", K(ret), K(file_info));
        }
      }
    }
  }

  return ret;
}

bool ObLogArchiveScheduler::is_force_cancel_() const
{
  return GCONF.backup_dest.get_value_string().empty();
}

int ObLogArchiveScheduler::upgrade_backup_info_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObBackupInfoManager info_manager;
  common::ObAddr scheduler_leader_addr;
  int64_t backup_schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
      LOG_WARN("failed to init info manager", K(ret), K(OB_SYS_TENANT_ID));
    } else if (OB_FAIL(check_can_do_work_())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(
                   ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(trans, backup_schema_version))) {
      LOG_WARN("failed to get tenant name backup schema version", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::set_inner_table_version(trans, OB_BACKUP_INNER_TABLE_V2))) {
      LOG_WARN("failed to set_inner_table_version", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::set_tenant_name_backup_schema_version(trans, backup_schema_version))) {
      LOG_WARN("failed to set tenant name backup schema version", K(ret));
    } else if (OB_FAIL(ObBackupInfoOperator::set_max_piece_id(trans, 0))) {
      LOG_WARN("failed to set max piece id", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        inner_table_version_ = OB_BACKUP_INNER_TABLE_V2;
        FLOG_INFO("finish upgrade_backup_info_", K(ret), K(inner_table_version_));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }

  return ret;
}

void ObLogArchiveScheduler::cleanup_()
{
  tenant_name_mgr_.cleanup();
  last_update_tenant_ts_ = 0;
}

int ObLogArchiveScheduler::check_backup_info_()
{
  int ret = OB_SUCCESS;
  ObBackupInfoChecker checker;  // TODO(muwei): add check before backup when version >= OB_BACKUP_INNER_TABLE_V2

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(checker.init(sql_proxy_, inner_table_version_))) {
    LOG_WARN("failed to init checker", K(ret));
  } else if (OB_FAIL(checker.check(tenant_ids_, *backup_lease_service_))) {
    LOG_WARN("failed to check backup info", K(ret));
  }

  LOG_INFO("finish check backup info", K(ret), K(tenant_ids_));
  return ret;
}

int ObLogArchiveScheduler::do_schedule_(share::ObLogArchiveStatus::STATUS &last_log_archive_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfo sys_info;
  ObNonFrozenBackupPieceInfo sys_non_frozen_piece;
  ObLogArchiveBackupInfoMgr info_mgr;
  tenant_ids_.reset();
  last_log_archive_status = share::ObLogArchiveStatus::MAX;

  DEBUG_SYNC(BEFROE_DO_LOG_ARCHIVE_SCHEDULER);
  const int64_t start_ts = ObTimeUtil::current_time();

  LOG_INFO("start do log archive schedule");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(
                 *sql_proxy_, false /*for_update*/, OB_SYS_TENANT_ID, inner_table_version_, sys_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else if (FALSE_IT(last_log_archive_status = sys_info.status_.status_)) {
  } else if (ObLogArchiveStatus::STOP == sys_info.status_.status_) {
    LOG_INFO("log archive status is stop, do nothing");
  } else if (inner_table_version_ >= OB_BACKUP_INNER_TABLE_V2 &&
             OB_FAIL(info_mgr.get_non_frozen_backup_piece(
                 *sql_proxy_, false /*for_update*/, sys_info, sys_non_frozen_piece))) {
    LOG_WARN("failed to get non frozen backup pieces", K(ret));
  } else if (OB_FAIL(check_mount_file_(sys_info))) {
    LOG_WARN("failed to check mount file", K(ret), K(sys_info));
  } else if (OB_FAIL(tenant_name_mgr_.reload_backup_dest(sys_info.backup_dest_, sys_info.status_.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(sys_info));
  } else if (OB_FAIL(tenant_name_mgr_.do_update(false /*is_force*/))) {
    LOG_WARN("failed to update tenant name id mapping", K(ret));
  } else if (OB_FAIL(tenant_name_mgr_.get_tenant_ids(tenant_ids_, last_update_tenant_ts_))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else if (OB_FAIL(check_backup_info_())) {
    LOG_WARN("failed to check_backup_info_", K(ret));
  } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
    LOG_WARN("failed to sync extern log archive backup info", K(ret), K(sys_info));
  } else if (OB_FAIL(prepare_new_tenant_info_(sys_info, sys_non_frozen_piece, tenant_ids_))) {
    LOG_WARN("failed to prepare_tenant_log_archive_status", K(ret));
  }

  if (OB_FAIL(ret)) {
    if ((is_force_cancel_() || OB_BACKUP_MOUNT_FILE_NOT_VALID == ret || OB_BACKUP_IO_PROHIBITED == ret) &&
        ObLogArchiveStatus::STOPPING == sys_info.status_.status_) {
      if (OB_FAIL(stop_log_archive_backup_(true /*force stop*/, sys_info, sys_non_frozen_piece))) {
        LOG_WARN("failed to do force_stop", K(ret), K(sys_info));
      }
    }
  } else if (OB_FAIL(check_dropped_tenant_(tenant_ids_, sys_non_frozen_piece))) {
    LOG_WARN("failed to check dropped tenant_", K(ret), K(sys_info), K(sys_non_frozen_piece));
  } else {
    switch (sys_info.status_.status_) {
      case ObLogArchiveStatus::STOP: {
        LOG_DEBUG("log archive status is stop, do nothing");
        break;
      }
      case ObLogArchiveStatus::BEGINNING: {
        DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_BEGINNING);
        if (OB_FAIL(start_log_archive_backup_(sys_info, sys_non_frozen_piece))) {
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
        if (OB_FAIL(update_log_archive_backup_process_(sys_info, sys_non_frozen_piece))) {
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
        if (OB_FAIL(stop_log_archive_backup_(false /*force stop*/, sys_info, sys_non_frozen_piece))) {
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

int ObLogArchiveScheduler::check_mount_file_(share::ObLogArchiveBackupInfo &sys_info)
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

int ObLogArchiveScheduler::start_log_archive_backup_(
    share::ObLogArchiveBackupInfo &cur_sys_info, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t doing_tenant_count = 0;
  int64_t max_backup_start_ts = cur_sys_info.status_.start_ts_;
  int64_t min_backup_checkpoint_ts = last_update_tenant_ts_;
  TENANT_ARCHIVE_STATUS_MAP log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;
  int64_t inactive_server_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info));
  } else if (sys_non_frozen_piece.has_prev_piece_info_) {
    ret = OB_ERR_SYS;
    LOG_ERROR(
        "beginning sys piece must not has prev non frozen piece", K(ret), K(cur_sys_info), K(sys_non_frozen_piece));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(
                 sys_info, allocator, log_archive_status_map, inactive_server_count))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObServerTenantLogArchiveStatus *status = nullptr;
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
        ObServerTenantLogArchiveStatus *status = nullptr;
        ObLogArchiveBackupInfo tenant_info = sys_info;
        tenant_info.status_.tenant_id_ = tenant_id;

        if (tenant_id < OB_USER_TENANT_ID) {
          continue;
        }

        if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
          LOG_WARN("failed to get tenant log archive status", K(ret));
        } else {
          tenant_info.status_.status_ = ObLogArchiveStatus::DOING;
          tenant_info.status_.start_ts_ = max_backup_start_ts;
          tenant_info.status_.checkpoint_ts_ = status->checkpoint_ts_;
          ObBackupPieceInfoKey tenant_piece_key = sys_non_frozen_piece.cur_piece_info_.key_;
          tenant_piece_key.tenant_id_ = tenant_id;
          if (OB_FAIL(set_backup_piece_start_ts_(tenant_piece_key, tenant_info.status_.start_ts_))) {
            LOG_WARN("failed to set_backup_piece_start_ts_", K(ret), K(tenant_info));
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
    sys_non_frozen_piece.cur_piece_info_.start_ts_ = sys_info.status_.start_ts_;

    if (OB_FAIL(set_backup_piece_start_ts_(
            sys_non_frozen_piece.cur_piece_info_.key_, sys_non_frozen_piece.cur_piece_info_.start_ts_))) {
      LOG_WARN("failed to set_backup_piece_start_ts_", K(ret), K(sys_non_frozen_piece));
    } else if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit log archive schedule", K(sys_info), K(last_update_tenant_ts_));
      ROOTSERVICE_EVENT_ADD("log_archive", "change_status", "new_status", "DOING", "round_id", sys_info.status_.round_);
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      } else {
        cur_sys_info = sys_info;
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::prepare_new_tenant_info_(const share::ObLogArchiveBackupInfo &sys_info,
    const share::ObNonFrozenBackupPieceInfo &non_frozen_piece, const common::ObArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (ObLogArchiveStatus::STOP == sys_info.status_.status_) {
    // do nothing for stop status
  } else if (ObLogArchiveStatus::STOPPING == sys_info.status_.status_) {
    // do nothing for stopping status
    LOG_INFO("no need to prepare new tenant when sys tenant in STOPPING state",
        K(sys_info),
        K(non_frozen_piece),
        K(tenant_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (tenant_id < OB_USER_TENANT_ID) {
        ret = OB_ERR_SYS;
        LOG_ERROR("sys tenant id should not exist in tenant_ids", K(ret), K(i), K(tenant_id), K(tenant_ids));
      } else if (OB_FAIL(prepare_new_tenant_info_(sys_info, non_frozen_piece, tenant_id))) {
        LOG_WARN("Failed to prepare tenant log archive status", K(ret), K(tenant_id), K(sys_info));
      }
    }
  }
  return ret;
}

// piece info is stored on sys tenant inner table, backup info is stored on user tenant inner table,
// so they cannot be updated in one trans.
// we will check if backup info is not ready first, if it is ready,  1\2\3\4 steps are all done;
// if not, we will repeat 1\2\3\4 steps.
// 1. update external piece info
// 2. update external backup info
// 3. update inner piece info
// 4. update inner backup info
// The only exception is that, piece_id = 0 means use old procedure without piece, we will skip 1\3 steps.
int ObLogArchiveScheduler::prepare_new_tenant_info_(const share::ObLogArchiveBackupInfo &sys_info,
    const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo inner_info;  // read from inner table
  ObLogArchiveBackupInfoMgr info_mgr;
  ObLogArchiveBackupInfo tenant_info;
  ObNonFrozenBackupPieceInfo tenant_non_frozen_piece;
  bool need_prepare = true;
  DEBUG_SYNC(PREPARE_TENANT_BEGINNING_STATUS);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_need_prepare_new_tenant_status(tenant_id, need_prepare))) {
    LOG_WARN("failed to check need prepare new tenant status", K(ret), K(tenant_id));
  } else if (!need_prepare) {
    // do nothing
  } else if (OB_FAIL(prepare_new_tenant_info_(
                 sys_info, sys_non_frozen_piece, tenant_id, tenant_info, tenant_non_frozen_piece))) {
    LOG_WARN("failed to prepare tenant info with piece", K(ret), K(tenant_id), K(sys_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, inner_table_version_, inner_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (inner_info.status_.status_ != ObLogArchiveStatus::STOP) {  // just for inner_table_version_=1
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_ERROR(
          "tenant log archive not match, cannot begin new log archive round", K(ret), K(tenant_id), K(inner_info));
    }

    if (OB_SUCC(ret) && inner_table_version_ >= OB_BACKUP_INNER_TABLE_V2) {
      if (FAILEDx(info_mgr.update_external_backup_piece(tenant_non_frozen_piece, *backup_lease_service_))) {
        LOG_WARN("failed to update external log archive backup piece", K(ret), K(tenant_info));
      } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(tenant_info, *backup_lease_service_))) {
        LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(tenant_info));
      } else if (OB_FAIL(update_tenant_non_frozen_backup_piece_(tenant_non_frozen_piece))) {
        LOG_WARN("failed to update_backup_piece_", K(ret), K(tenant_non_frozen_piece));
      }
    }

    if (FAILEDx(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, tenant_info))) {
      LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(tenant_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO(
            "finish prepare_new_tenant_status", K(ret), K(tenant_info), K(inner_info), K(tenant_non_frozen_piece));

        ROOTSERVICE_EVENT_ADD("log_archive",
            "prepare_new_tenant",
            K(tenant_id),
            "round_id",
            sys_info.status_.round_,
            "piece_id",
            tenant_non_frozen_piece.cur_piece_info_.key_.backup_piece_id_,
            "has_prev_piece",
            tenant_non_frozen_piece.has_prev_piece_info_);
        DEBUG_SYNC(AFTER_PREPARE_TENANT_BEGINNING_STATUS);
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::update_tenant_non_frozen_backup_piece_(
    const share::ObNonFrozenBackupPieceInfo &non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.update_backup_piece(trans, non_frozen_piece))) {
      LOG_WARN("failed to update_backup_piece", K(ret), K(non_frozen_piece));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("update_backup_piece_", K(ret), K(non_frozen_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::check_need_prepare_new_tenant_status(const uint64_t tenant_id, bool &need_prepare)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObLogArchiveBackupInfo inner_info;  // read from inner table
  ObLogArchiveBackupInfoMgr info_mgr;
  need_prepare = false;

  if (OB_FAIL(
          info_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, inner_table_version_, inner_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;
      need_prepare = true;
    }
  } else {
    need_prepare = ObLogArchiveStatus::STOP == inner_info.status_.status_;
  }

  return ret;
}

int ObLogArchiveScheduler::prepare_new_tenant_info_(const share::ObLogArchiveBackupInfo &sys_info,
    const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece, const uint64_t tenant_id,
    share::ObLogArchiveBackupInfo &tenant_info, ObNonFrozenBackupPieceInfo &tenant_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int64_t create_tenant_ts = 0;
  ObLogArchiveBackupInfoMgr info_mgr;

  tenant_info.reset();
  tenant_non_frozen_piece.reset();

  if (!sys_info.is_valid() || OB_INVALID == tenant_id || OB_SYS_TENANT_ID != sys_info.status_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info));
  } else if (OB_FAIL(info_mgr.get_create_tenant_timestamp(*sql_proxy_, tenant_id, create_tenant_ts))) {
    LOG_WARN("failed to fetch tenant create timestamp", K(ret), K(tenant_id));
  } else {
    // init backup info
    tenant_info = sys_info;
    tenant_info.status_.tenant_id_ = tenant_id;
    tenant_info.status_.start_ts_ = create_tenant_ts;
    tenant_info.status_.checkpoint_ts_ = 0;
    if (ObLogArchiveStatus::DOING == tenant_info.status_.status_) {
      tenant_info.status_.status_ = ObLogArchiveStatus::BEGINNING;
    }
  }

  if (OB_SUCC(ret) && inner_table_version_ >= OB_BACKUP_INNER_TABLE_V2) {
    // init backup piece
    tenant_info.status_.start_piece_id_ = sys_non_frozen_piece.cur_piece_info_.key_.backup_piece_id_;
    tenant_non_frozen_piece.has_prev_piece_info_ = sys_non_frozen_piece.has_prev_piece_info_;
    if (tenant_non_frozen_piece.has_prev_piece_info_) {
      tenant_info.status_.start_piece_id_ = sys_non_frozen_piece.prev_piece_info_.key_.backup_piece_id_;
      if (OB_FAIL(tenant_non_frozen_piece.prev_piece_info_.init_piece_info(
              sys_non_frozen_piece.prev_piece_info_, tenant_id))) {
        LOG_WARN("failed to init prev tenant_piece", K(ret), K(sys_non_frozen_piece));
      }
      tenant_non_frozen_piece.prev_piece_info_.start_ts_ = create_tenant_ts;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_non_frozen_piece.cur_piece_info_.init_piece_info(
              sys_non_frozen_piece.cur_piece_info_, tenant_id))) {
        LOG_WARN("failed to init cur tenant_piece", K(ret), K(sys_non_frozen_piece));
      }
      if (tenant_non_frozen_piece.has_prev_piece_info_) {
        tenant_non_frozen_piece.cur_piece_info_.start_ts_ = 0;  // will be filled after frozen
      } else {
        tenant_non_frozen_piece.cur_piece_info_.start_ts_ = create_tenant_ts;
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::set_backup_piece_start_ts_(
    const share::ObBackupPieceInfoKey &piece_key, const int64_t start_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfoMgr info_mgr;
  share::ObBackupPieceInfo piece;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    FLOG_INFO("inner table version is old, skip set piece start ts", K(ret), K(inner_table_version_), K(piece_key));
  } else if (!piece_key.is_valid() || start_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(start_ts), K(piece_key));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_backup_piece(trans, for_update, piece_key, piece))) {
      LOG_WARN("failed to get non frozen backup pieces", K(ret), K(piece_key));
    } else {
      piece.start_ts_ = start_ts;
      if (OB_FAIL(info_mgr.update_external_backup_piece(piece, *backup_lease_service_))) {
        LOG_WARN("failed to update external log archive backup piece", K(ret), K(piece));
      } else if (OB_FAIL(info_mgr.update_backup_piece(trans, piece))) {
        LOG_WARN("failed to update log archive backup piece", K(ret), K(piece));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("finish set_backup_piece_start_ts_", K(ret), K(piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::start_tenant_log_archive_backup_info_(share::ObLogArchiveBackupInfo &new_info)
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
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(
            trans, for_update, new_info.status_.tenant_id_, inner_table_version_, inner_info))) {
      LOG_WARN("failed to get log archive backup info", K(ret));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, new_info))) {
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

int ObLogArchiveScheduler::update_log_archive_backup_process_(
    share::ObLogArchiveBackupInfo &cur_sys_info, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int64_t min_piece_id = INT64_MAX;
  int64_t inactive_server_count = 0;
  TENANT_ARCHIVE_STATUS_MAP log_archive_status_map;
  ObArenaAllocator allocator;

  LOG_INFO("do update_log_archive_backup_process_",
      K(inner_table_version_),
      K(tenant_ids_),
      K(cur_sys_info),
      K(sys_non_frozen_piece));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(try_update_checkpoit_ts_(cur_sys_info,
                 sys_non_frozen_piece,
                 allocator,
                 log_archive_status_map,
                 min_piece_id,
                 inactive_server_count))) {
    LOG_WARN("failed to try update checkpoint ts", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    LOG_INFO("skip update pice for old inner table version", K(ret), K(inner_table_version_));
  } else if (OB_FAIL(try_update_backup_piece_(
                 cur_sys_info, log_archive_status_map, inactive_server_count, sys_non_frozen_piece, min_piece_id))) {
    LOG_WARN("failed to update backup piece", K(ret));
  }

  return ret;
}

int ObLogArchiveScheduler::try_update_checkpoit_ts_(share::ObLogArchiveBackupInfo &cur_sys_info,
    share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece, ObIAllocator &allocator,
    TENANT_ARCHIVE_STATUS_MAP &log_archive_status_map, int64_t &min_piece_id, int64_t &inactive_server_count)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t doing_tenant_count = 0;
  int64_t checkpoint_ts = last_update_tenant_ts_;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;
  inactive_server_count = 0;
  min_piece_id = INT64_MAX;

  if (OB_FAIL(fetch_log_archive_backup_status_map_(
          cur_sys_info, allocator, log_archive_status_map, inactive_server_count))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObServerTenantLogArchiveStatus *status = nullptr;
    ++user_tenant_count;
    if (tenant_id < OB_USER_TENANT_ID) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tenant ids should not has sys tenant", K(ret), K(tenant_id), K(i), K(tenant_ids_));
    } else if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
      LOG_WARN("failed to get tenant log archive status", K(ret));
    } else if (sys_info.status_.incarnation_ != status->incarnation_ || sys_info.status_.round_ != status->round_ ||
               (0 != sys_info.status_.start_piece_id_ &&
                   status->min_backup_piece_id_ < sys_info.status_.backup_piece_id_ - 1)) {
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
    } else if (status->start_ts_ > status->checkpoint_ts_) {
      FLOG_INFO("tenant status start ts is larger than checkpoint ts, wait push up checkpoint ts", K(*status));
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

      if (min_piece_id > status->min_backup_piece_id_) {
        min_piece_id = status->min_backup_piece_id_;
        FLOG_INFO("update backup piece id", K(min_piece_id), K(*status));
      }

      if (OB_FAIL(update_tenant_log_archive_backup_process_(sys_info, sys_non_frozen_piece, *status))) {
        LOG_WARN("failed to start tenant log archive backup", K(ret), K(sys_info), K(*status));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == user_tenant_count) {
      min_piece_id = cur_sys_info.status_.backup_piece_id_;
      checkpoint_ts = ObTimeUtil::current_time();
#ifdef ERRSIM
      const int64_t fake_archive_log_checkpoint_ts = GCONF.fake_archive_log_checkpoint_ts;
      if (0 != fake_archive_log_checkpoint_ts) {
        checkpoint_ts = MIN(checkpoint_ts, fake_archive_log_checkpoint_ts);
      }
#endif
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
    } else if (OB_FAIL(update_sys_log_archive_backup_process_(cur_sys_info, sys_info, sys_non_frozen_piece))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info), K(sys_non_frozen_piece));
    } else {
      cur_sys_info = sys_info;
    }
  }

  return ret;
}

int ObLogArchiveScheduler::try_update_backup_piece_(const share::ObLogArchiveBackupInfo &sys_info,
    const TENANT_ARCHIVE_STATUS_MAP &log_archive_status_map, const int64_t inactive_server_count,
    share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece, int64_t &min_piece_id)
{
  int ret = OB_SUCCESS;
  bool is_backup_backup = false;
  ObBackupDestOpt backup_dest_opt;
  bool need_block_switch_piece_for_test = false;
  int64_t test_max_piece_id = 1;
  ObBackupPieceInfo &sys_piece_info = sys_non_frozen_piece.cur_piece_info_;

  LOG_INFO("start try_update_backup_piece", K(sys_info), K(sys_non_frozen_piece));

#ifdef ERRSIM
  test_max_piece_id = ObServerConfig::get_instance()._max_backup_piece_id;
  need_block_switch_piece_for_test = test_max_piece_id > 0;
#endif
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(backup_dest_opt.init(is_backup_backup))) {
    LOG_WARN("failed to get_backup_dest_opt", K(ret));
  } else if (!sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_info));
  } else if (inactive_server_count > 0) {
    FLOG_WARN("cannot freeze backup piece when server not all alive", K(ret), K(inactive_server_count));
  } else if (sys_non_frozen_piece.has_prev_piece_info_) {
#ifdef ERRSIM
    ret = E(EventTable::EN_BACKUP_RS_BLOCK_FROZEN_PIECE) OB_SUCCESS;
#endif
    if (OB_FAIL(ret)) {
      if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {  // 2s
        LOG_INFO("ERRSIM: RS block frozen", K(ret), K(sys_info), K(sys_non_frozen_piece));
      }
    } else if (min_piece_id < sys_non_frozen_piece.cur_piece_info_.key_.backup_piece_id_) {
      FLOG_INFO("piece is not frozen, skip frozen old piece", K(ret), K(min_piece_id), K(sys_non_frozen_piece));
    } else if (OB_FAIL(frozen_old_piece_(
                   false /*force_stop*/, tenant_ids_, log_archive_status_map, sys_non_frozen_piece))) {
      LOG_WARN("failed to frozen_old_piece", K(ret), K(min_piece_id), K(sys_non_frozen_piece));
    }
  } else {  // sys_info.pieces_.count() is 1
    const int64_t piece_switch_interval = backup_dest_opt.piece_switch_interval_;
    const int64_t piece_start_ts = sys_piece_info.start_ts_;
    const int64_t cur_ts = ObTimeUtil::current_time();
    int64_t max_piece_id = 0;
    if (sys_piece_info.status_ != ObBackupPieceStatus::BACKUP_PIECE_ACTIVE) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid piece info", K(ret), K(sys_info), K(sys_non_frozen_piece));
    } else if (OB_FAIL(ObBackupInfoOperator::get_max_piece_id(*sql_proxy_, false /*for_update*/, max_piece_id))) {
      LOG_WARN("failed to get max piece id", K(ret));
    } else if (max_piece_id < sys_info.status_.backup_piece_id_ ||
               max_piece_id > sys_info.status_.backup_piece_id_ + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected max piece id", K(ret), K(max_piece_id), K(sys_info));
    } else if ((max_piece_id == sys_info.status_.backup_piece_id_ && piece_start_ts + piece_switch_interval > cur_ts) ||
               0 == sys_piece_info.key_.backup_piece_id_ ||
               (need_block_switch_piece_for_test &&
                   sys_non_frozen_piece.cur_piece_info_.key_.backup_piece_id_ >= test_max_piece_id)) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {  // 60s
        LOG_INFO("no need to freeze piece",
            K(cur_ts),
            K(piece_start_ts),
            K(piece_switch_interval),
            K(need_block_switch_piece_for_test),
            K(test_max_piece_id),
            K(max_piece_id),
            K(sys_info));
      }
    } else if (OB_FAIL(trigger_freeze_pieces_(sys_info, sys_piece_info, log_archive_status_map))) {
      LOG_WARN("failed to trigger freeze piece", K(ret), K(piece_switch_interval), K(sys_info));
    }
  }
  LOG_INFO("finish try_update_backup_piece", K(sys_info), K(sys_non_frozen_piece));
  return ret;
}

int ObLogArchiveScheduler::update_sys_active_piece_checkpoint_ts_(
    const int64_t checkpoint_ts, share::ObBackupPieceInfo &sys_piece_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  share::ObBackupPieceInfo cur_piece_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    LOG_INFO("inner table version is old, skip update sys piece checkpoint ts",
        K(ret),
        K(inner_table_version_),
        K(sys_piece_info));
  } else if (!sys_piece_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_piece_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_backup_piece(trans, true /*for update*/, sys_piece_info.key_, cur_piece_info))) {
      LOG_WARN("failed to get backup piece", K(ret), K(sys_piece_info));
    } else if (sys_piece_info != cur_piece_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys piece info is not match", K(ret), K(sys_piece_info), K(cur_piece_info));
    } else {
      sys_piece_info.checkpoint_ts_ = checkpoint_ts;
      if (OB_FAIL(info_mgr.update_backup_piece(trans, sys_piece_info))) {
        LOG_WARN("failed to update backup piece", K(ret), K(sys_piece_info));
      } else if (OB_FAIL(info_mgr.update_external_single_backup_piece_info(sys_piece_info, *backup_lease_service_))) {
        LOG_WARN("failed to update external single backup piece info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        LOG_WARN("failed to commit", K(ret));
      } else {
        LOG_INFO("succeed to update sys active piece checkpoint ts", K(ret), K(sys_piece_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::update_active_piece_checkpoint_ts_(
    const share::ObBackupPieceInfoKey &piece_key, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  share::ObBackupPieceInfo piece_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    LOG_INFO("inner table version is old, skip update active piece checkpoint ts",
        K(ret),
        K(inner_table_version_),
        K(piece_key));
  } else if (!piece_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(piece_key));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_backup_piece(trans, true /*for update*/, piece_key, piece_info))) {
      LOG_WARN("failed to get backup piece", K(ret), K(piece_key));
    } else if (ObBackupPieceStatus::BACKUP_PIECE_ACTIVE != piece_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant piece is not active, cannot update", K(ret), K(piece_info));
    } else {
      piece_info.checkpoint_ts_ = checkpoint_ts;
      if (OB_FAIL(info_mgr.update_backup_piece(trans, piece_info))) {
        LOG_WARN("failed to update backup piece", K(ret), K(piece_info));
      } else if (OB_FAIL(info_mgr.update_external_single_backup_piece_info(piece_info, *backup_lease_service_))) {
        LOG_WARN("failed to update external single backup piece info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        LOG_WARN("failed to commit", K(ret));
      } else {
        LOG_INFO("succeed to update active piece checkpoint ts", K(ret), K(piece_info));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::frozen_old_piece_(const bool force_stop, const common::ObIArray<uint64_t> &tenant_ids,
    const TENANT_ARCHIVE_STATUS_MAP &log_archive_status_map, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int64_t max_gts = 0;
  share::ObServerTenantLogArchiveStatus *status = nullptr;

  DEBUG_SYNC(BACKUP_BEFORE_FROZEN_PIECES);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!sys_non_frozen_piece.is_valid() || !sys_non_frozen_piece.has_prev_piece_info_ ||
             sys_non_frozen_piece.prev_piece_info_.status_ != ObBackupPieceStatus::BACKUP_PIECE_FREEZING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_non_frozen_piece));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_can_do_work_())) {
        LOG_WARN("failed to check can do work", K(ret));
      } else if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
        LOG_WARN("failed to get status", K(ret), K(tenant_id));
      } else if (OB_FAIL(
                     frozen_old_piece_(force_stop, sys_non_frozen_piece, tenant_id, status->max_log_ts_, max_gts))) {
        LOG_WARN("failed to frozen_old_piece_", K(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (tenant_ids.empty()) {
        max_gts = sys_non_frozen_piece.cur_piece_info_.checkpoint_ts_;
        FLOG_INFO("no tenant exists, use sys checkpoint ts", K(max_gts), K(sys_non_frozen_piece));
      }
      if (OB_FAIL(frozen_sys_old_piece_(force_stop, max_gts, sys_non_frozen_piece))) {
        LOG_WARN("failed to frozen_sys_old_piece_", K(ret), K(sys_non_frozen_piece));
      }
    }
  }

  FLOG_INFO("frozen old piece",
      K(ret),
      K(force_stop),
      "has_tenant",
      !tenant_ids.empty(),
      K(tenant_ids),
      K(sys_non_frozen_piece));

  return ret;
}

int ObLogArchiveScheduler::frozen_old_piece_(const bool force_stop,
    const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece, const uint64_t tenant_id,
    const int64_t server_tenant_max_ts, int64_t &max_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t timeout = 10 * 1000 * 1000;  //  10s
  ObLogArchiveBackupInfoMgr info_mgr;
  bool is_external_consistent = true;
  const bool for_update = true;
  ObMySQLTransaction trans;
  int64_t tenant_max_ts = server_tenant_max_ts;
  int64_t gts = 0;

  if (tenant_id <= 0 || !sys_non_frozen_piece.is_valid() || !sys_non_frozen_piece.has_prev_piece_info_ ||
      server_tenant_max_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_non_frozen_piece), K(tenant_id), K(server_tenant_max_ts));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id, timeout, gts, is_external_consistent))) {
    LOG_WARN("failed to get ts sync", K(ret), K(tenant_id), K(timeout));
  } else if (!is_external_consistent) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot backup tenant without gts", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    share::ObNonFrozenBackupPieceInfo tenant_non_frozen_piece;
    share::ObBackupPieceInfoKey cur_key = sys_non_frozen_piece.cur_piece_info_.key_;
    cur_key.tenant_id_ = tenant_id;
    tenant_max_ts = MAX(tenant_max_ts, gts);
    max_ts = MAX(max_ts, gts);
    if (OB_FAIL(info_mgr.get_non_frozen_backup_piece(trans, for_update, cur_key, tenant_non_frozen_piece))) {
      LOG_WARN("failed to get backup piece", K(ret), K(cur_key));
    } else {
      tenant_non_frozen_piece.prev_piece_info_.max_ts_ = gts;
      tenant_non_frozen_piece.cur_piece_info_.start_ts_ = gts;
      tenant_non_frozen_piece.prev_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
      if (!force_stop &&
          OB_FAIL(info_mgr.update_external_backup_piece(tenant_non_frozen_piece, *backup_lease_service_))) {
        LOG_WARN("failed to update external backup piece", K(ret), K(tenant_non_frozen_piece));
      } else if (OB_FAIL(info_mgr.update_backup_piece(trans, tenant_non_frozen_piece))) {
        LOG_WARN("failed to update backup piece", K(ret), K(tenant_non_frozen_piece));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("finish frozen_old_piece_", K(ret), K(tenant_non_frozen_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::frozen_sys_old_piece_(
    const bool force_stop, const int64_t max_gts, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;

  if (max_gts <= 0 || !sys_non_frozen_piece.is_valid() || !sys_non_frozen_piece.has_prev_piece_info_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_non_frozen_piece), K(max_gts));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    sys_non_frozen_piece.prev_piece_info_.max_ts_ = max_gts;
    sys_non_frozen_piece.cur_piece_info_.start_ts_ = max_gts;
    sys_non_frozen_piece.prev_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
    if (!force_stop && OB_FAIL(info_mgr.update_external_backup_piece(sys_non_frozen_piece, *backup_lease_service_))) {
      LOG_WARN("failed to update external backup piece", K(ret), K(sys_non_frozen_piece));
    } else if (OB_FAIL(info_mgr.update_backup_piece(trans, sys_non_frozen_piece))) {
      LOG_WARN("failed to update backup piece", K(ret), K(sys_non_frozen_piece));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("[LOG_ARCHIVE]  finish frozen piece",
            K(ret),
            "round",
            sys_non_frozen_piece.cur_piece_info_.key_.round_id_,
            "frozen_piece_id",
            sys_non_frozen_piece.cur_piece_info_.key_.backup_piece_id_,
            K(sys_non_frozen_piece));
        ROOTSERVICE_EVENT_ADD("log_archive",
            "frozen_piece",
            "round_id",
            sys_non_frozen_piece.cur_piece_info_.key_.round_id_,
            "frozen_piece_id",
            sys_non_frozen_piece.prev_piece_info_.key_.backup_piece_id_,
            "piece_id",
            sys_non_frozen_piece.cur_piece_info_.key_.backup_piece_id_);
        sys_non_frozen_piece.has_prev_piece_info_ = false;
        sys_non_frozen_piece.prev_piece_info_.reset();
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}
// 1. init next sys piece info
// 2. init next user piece info
// 3. update external user piece infos, for old piece, set status and checkpoint_ts_, insert new piece
// 4. update external sys piece info
// 5. update external sys backup info
// 6. update inner table user and sys piece in one transaction
int ObLogArchiveScheduler::trigger_freeze_pieces_(const share::ObLogArchiveBackupInfo &sys_info,
    const share::ObBackupPieceInfo &sys_piece_info, const TENANT_ARCHIVE_STATUS_MAP &log_archive_status_map)
{
  int ret = OB_SUCCESS;
  share::ObLogArchiveBackupInfo new_sys_info;
  share::ObNonFrozenBackupPieceInfo new_sys_piece;
  ObArray<share::ObNonFrozenBackupPieceInfo> new_user_piece_array;
  ObLogArchiveBackupInfoMgr info_mgr;

  DEBUG_SYNC(BACKUP_BEFORE_TRIGGER_FREEZE_PIECES);
  LOG_INFO("trigger_freeze_pieces_", K(sys_info), K(sys_piece_info));
  if (OB_FAIL(new_user_piece_array.reserve(tenant_ids_.count()))) {
    LOG_WARN("failed to reserve new user pieces array", K(ret), "count", tenant_ids_.count());
  } else if (OB_FAIL(init_next_sys_piece_(sys_info, sys_piece_info, new_sys_info, new_sys_piece))) {
    LOG_WARN("failed to init next sys piece info", K(ret));
  } else if (OB_FAIL(init_next_user_piece_array_(new_sys_piece, log_archive_status_map, new_user_piece_array))) {
    LOG_WARN("failed to init next user piece array", K(ret));
  } else if (OB_FAIL(update_external_user_piece_(new_user_piece_array))) {
    LOG_WARN("failed to update external user piece_", K(ret));
  } else if (OB_FAIL(info_mgr.update_external_backup_piece(new_sys_piece, *backup_lease_service_))) {
    LOG_WARN("failed to update external backup piece", K(ret), K(new_sys_piece));
  } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(new_sys_info, *backup_lease_service_))) {
    LOG_WARN("failed to update external sys backup info", K(ret), K(new_sys_info));
  } else if (OB_FAIL(update_inner_freeze_piece_info_(sys_info, new_sys_info, new_sys_piece, new_user_piece_array))) {
    LOG_WARN("failed to update inner freeze info", K(ret));
  }

  return ret;
}

int ObLogArchiveScheduler::update_sys_log_archive_backup_process_(const share::ObLogArchiveBackupInfo &cur_info,
    share::ObLogArchiveBackupInfo &new_info, const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObBackupPieceInfo sys_piece_info = sys_non_frozen_piece.cur_piece_info_;
  if (!cur_info.is_valid() || !new_info.is_valid() || !sys_non_frozen_piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_info), K(new_info), K(sys_non_frozen_piece));
  } else if (OB_FAIL(update_sys_active_piece_checkpoint_ts_(new_info.status_.checkpoint_ts_, sys_piece_info))) {
    LOG_WARN("failed to update checkpoint ts", K(ret), K(cur_info), K(new_info), K(sys_non_frozen_piece));
  } else if (OB_FAIL(update_sys_backup_info_(cur_info, new_info))) {
    LOG_WARN("failed to update sys backup info", K(ret), K(cur_info), K(new_info), K(sys_non_frozen_piece));
  } else {
    FLOG_INFO("[LOG_ARCHIVE] succeed to commit update log archive process",
        K(cur_info),
        K(new_info),
        K(sys_non_frozen_piece));
    if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(new_info, *backup_lease_service_))) {
      LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(new_info));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::update_tenant_log_archive_backup_process_(const share::ObLogArchiveBackupInfo &sys_info,
    const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece,
    const share::ObServerTenantLogArchiveStatus &tenant_status)
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
  } else if (ObLogArchiveStatus::DOING != tenant_status.status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot update tenant log archive process with not doing status", K(ret), K(tenant_status));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(
            trans, for_update, tenant_status.tenant_id_, inner_table_version_, info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(tenant_status));
    } else if (info.status_.checkpoint_ts_ > tenant_status.checkpoint_ts_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("log archive checkpoint ts is rollback", K(ret), K(info), K(tenant_status), K(sys_info));
    } else {
      info.status_.backup_piece_id_ = sys_info.status_.backup_piece_id_;
      info.status_.checkpoint_ts_ = tenant_status.checkpoint_ts_;
    }

    if (OB_SUCC(ret)) {
      if (ObLogArchiveStatus::BEGINNING == info.status_.status_) {
        // for new tenant, start ts maybe changed
        info.status_.status_ = ObLogArchiveStatus::DOING;
        if (info.status_.start_ts_ < tenant_status.start_ts_) {
          info.status_.start_ts_ = tenant_status.start_ts_;
        }
        ObBackupPieceInfoKey tenant_piece_key = sys_non_frozen_piece.cur_piece_info_.key_;
        if (sys_non_frozen_piece.has_prev_piece_info_) {
          tenant_piece_key = sys_non_frozen_piece.prev_piece_info_.key_;
        }
        tenant_piece_key.tenant_id_ = tenant_status.tenant_id_;
        if (OB_FAIL(set_backup_piece_start_ts_(tenant_piece_key, info.status_.start_ts_))) {
          LOG_WARN("failed to set backup piece start ts", K(ret), K(tenant_piece_key), K(info));
        }
      } else if (ObLogArchiveStatus::DOING == info.status_.status_) {
        ObBackupPieceInfoKey active_piece_key = sys_non_frozen_piece.cur_piece_info_.key_;
        active_piece_key.tenant_id_ = tenant_status.tenant_id_;
        if (info.status_.start_ts_ < tenant_status.start_ts_) {
          ret = OB_LOG_ARCHIVE_INTERRUPTED;
          LOG_ERROR("server start ts must not larger than prev one", K(ret), K(info), K(tenant_status));
        } else if (OB_FAIL(update_active_piece_checkpoint_ts_(active_piece_key, tenant_status.checkpoint_ts_))) {
          LOG_WARN("failed to update active piece checkpoint ts", K(ret), K(active_piece_key), K(tenant_status));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected log archive status", K(ret), K(info), K(tenant_status));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, info))) {
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

int ObLogArchiveScheduler::check_dropped_tenant_(
    const common::ObIArray<uint64_t> &tenant_ids, const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  common::ObArray<uint64_t> log_tenant_ids;
  const bool force_stop = false;
  bool is_tenant_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    // no need check dropped tenant
  } else if (OB_FAIL(info_mgr.get_all_active_log_archive_tenants(*sql_proxy_, log_tenant_ids))) {
    LOG_WARN("failed to get all active log archive status", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_tenant_ids.count(); ++i) {
      const uint64_t tenant_id = log_tenant_ids.at(i);
      share::ObBackupPieceInfoKey cur_key = sys_non_frozen_piece.cur_piece_info_.key_;
      int64_t max_ts = 0;
      cur_key.tenant_id_ = tenant_id;
      is_tenant_exist = is_contain(tenant_ids, tenant_id);

      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (is_tenant_exist) {
        // do nothing for exist tenant
      } else if (OB_FAIL(do_stop_tenant_log_archive_backup_v2_(force_stop, !is_tenant_exist, cur_key, max_ts))) {
        LOG_WARN("failed to move dropped tenant status", K(ret));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::do_stop_log_archive_backup_v2_(const bool force_stop,
    const common::ObIArray<uint64_t> &tenant_ids, const share::ObLogArchiveBackupInfo &cur_sys_info,
    const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  common::ObArray<uint64_t> log_tenant_ids;
  bool is_tenant_exist = false;
  int64_t max_ts = 0;
  int64_t user_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (!cur_sys_info.is_valid() || !sys_non_frozen_piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_sys_info), K(sys_non_frozen_piece));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2) {
    ret = OB_ERR_SYS;
    LOG_ERROR("old version should not do_stop_log_archive_backup_v2_", K(ret), K(inner_table_version_));
  } else if (OB_FAIL(info_mgr.get_all_active_log_archive_tenants(*sql_proxy_, log_tenant_ids))) {
    LOG_WARN("failed to get all active log archive status", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_tenant_ids.count(); ++i) {
      const uint64_t tenant_id = log_tenant_ids.at(i);
      share::ObBackupPieceInfoKey cur_key = sys_non_frozen_piece.cur_piece_info_.key_;
      int64_t tenant_max_ts = 0;
      cur_key.tenant_id_ = tenant_id;
      is_tenant_exist = is_contain(tenant_ids, tenant_id);
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(do_stop_tenant_log_archive_backup_v2_(force_stop, !is_tenant_exist, cur_key, tenant_max_ts))) {
        LOG_WARN("failed to do_stop_log_archive_backup_v2_", K(ret), K(tenant_id));
      } else {
        ++user_count;
        max_ts = MAX(max_ts, tenant_max_ts);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == user_count) {
      max_ts = ObTimeUtility::current_time();
      FLOG_INFO("no tenant exist, use current timestamp as sys max ts", K(ret), K(max_ts), K(sys_non_frozen_piece));
    }
    if (OB_FAIL(do_stop_sys_log_archive_backup_v2_(
            force_stop, cur_sys_info, sys_non_frozen_piece.cur_piece_info_.key_, max_ts))) {
      LOG_WARN("failed to do_stop_sys_log_archive_backup_v2_ for sys", K(ret));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::do_stop_tenant_log_archive_backup_v2_(
    const bool force_stop, const bool is_dropped_tenant, const share::ObBackupPieceInfoKey &cur_key, int64_t &max_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo backup_info;
  bool is_external_consistent = true;
  const bool for_update = true;
  const int64_t gts_timeout = 10 * 1000 * 1000;  //  10s for gts
  const int64_t sql_timeout = 60 * 1000 * 1000;  // 60s
  ObTimeoutCtx timeout_ctx;
  share::ObNonFrozenBackupPieceInfo tenant_non_frozen_piece;
  const uint64_t tenant_id = cur_key.tenant_id_;
  max_ts = -1;

  DEBUG_SYNC(BEFROE_DO_STOP_TENANT_ARCHIVE);

#ifdef ERRSIM
  ret = E(EventTable::EN_STOP_TENANT_LOG_ARCHIVE_BACKUP) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (!cur_key.is_valid() || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_key));
  } else if (!is_dropped_tenant &&
             OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id, gts_timeout, max_ts, is_external_consistent))) {
    LOG_WARN("failed to get ts sync", K(ret), K(tenant_id), K(gts_timeout));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(sql_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(sql_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(sql_timeout))) {
    LOG_WARN("failed to set timeout", K(ret), K(sql_timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(
            info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, inner_table_version_, backup_info))) {
      LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id), K(inner_table_version_));
    } else if (inner_table_version_ >= OB_BACKUP_INNER_TABLE_V2) {
      if (is_dropped_tenant) {
        max_ts = backup_info.status_.checkpoint_ts_;  // tenant is dropped, cannot use gts;
      }
      if (OB_FAIL(info_mgr.get_non_frozen_backup_piece(trans, for_update, cur_key, tenant_non_frozen_piece))) {
        LOG_WARN("failed to get backup piece", K(ret), K(cur_key));
      } else if (tenant_non_frozen_piece.has_prev_piece_info_) {
        tenant_non_frozen_piece.prev_piece_info_.max_ts_ = max_ts;
        tenant_non_frozen_piece.cur_piece_info_.start_ts_ = max_ts;
        tenant_non_frozen_piece.prev_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
      }

      if (OB_SUCC(ret)) {
        tenant_non_frozen_piece.cur_piece_info_.checkpoint_ts_ = backup_info.status_.checkpoint_ts_;
        tenant_non_frozen_piece.cur_piece_info_.max_ts_ = max_ts;
        tenant_non_frozen_piece.cur_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
        backup_info.status_.status_ = ObLogArchiveStatus::STOP;
        if (OB_FAIL(info_mgr.update_backup_piece(trans, tenant_non_frozen_piece))) {
          LOG_WARN("failed to update backup piece", K(ret), K(tenant_non_frozen_piece));
        } else if (OB_FAIL(info_mgr.delete_tenant_log_archive_status_v2(trans, tenant_id))) {
          LOG_WARN("failed to delete log archive status", K(ret), K(tenant_id));
        } else if (OB_FAIL(info_mgr.update_log_archive_status_history(
                       trans, backup_info, inner_table_version_, *backup_lease_service_))) {
          LOG_WARN("failed to update log archive status history", K(ret), K(backup_info));
        } else if (force_stop) {
          LOG_INFO("skip update external log archive backup info during force_stop",
              K(backup_info),
              K(tenant_non_frozen_piece));
        } else if (OB_FAIL(info_mgr.update_external_backup_piece(tenant_non_frozen_piece, *backup_lease_service_))) {
          LOG_WARN("failed to update external backup piece", K(ret), K(tenant_non_frozen_piece));
        } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(backup_info, *backup_lease_service_))) {
          LOG_WARN("failed to update_extern_log_archive_backup_info", K(ret), K(backup_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("succeed to do_stop_tenant_log_archive_backup_v2_",
            K(ret),
            K(tenant_id),
            K(is_dropped_tenant),
            K(inner_table_version_),
            K(backup_info),
            K(tenant_non_frozen_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::do_stop_sys_log_archive_backup_v2_(const bool force_stop,
    const share::ObLogArchiveBackupInfo &cur_sys_info, const share::ObBackupPieceInfoKey &cur_key, const int64_t max_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo sys_info;
  const bool for_update = true;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::ObNonFrozenBackupPieceInfo sys_non_frozen_piece;
  const int64_t sql_timeout = 60 * 1000 * 1000;  // 60s
  ObTimeoutCtx timeout_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (max_ts <= 0 || !cur_sys_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(max_ts), K(cur_sys_info));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(sql_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(sql_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(sql_timeout))) {
    LOG_WARN("failed to set timeout", K(ret), K(sql_timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, inner_table_version_, sys_info))) {
      LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id), K(inner_table_version_));
    } else if (!cur_sys_info.is_same(sys_info)) {
      ret = OB_EAGAIN;
      LOG_WARN("sys info is changed before update", K(ret), K(cur_sys_info), K(sys_info));
    } else if (info_mgr.get_non_frozen_backup_piece(trans, for_update, cur_key, sys_non_frozen_piece)) {
      LOG_WARN("failed to get backup piece", K(ret), K(cur_key));
    } else if (sys_non_frozen_piece.has_prev_piece_info_) {
      sys_non_frozen_piece.prev_piece_info_.max_ts_ = max_ts;
      sys_non_frozen_piece.cur_piece_info_.start_ts_ = max_ts;
      sys_non_frozen_piece.prev_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
    }

    if (OB_SUCC(ret)) {
      sys_non_frozen_piece.cur_piece_info_.checkpoint_ts_ = sys_info.status_.checkpoint_ts_;
      sys_non_frozen_piece.cur_piece_info_.max_ts_ = max_ts;
      sys_non_frozen_piece.cur_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
      sys_info.status_.status_ = ObLogArchiveStatus::STOP;
      if (OB_FAIL(info_mgr.update_backup_piece(trans, sys_non_frozen_piece))) {
        LOG_WARN("failed to update backup piece", K(ret), K(sys_non_frozen_piece));
      } else if (OB_FAIL(info_mgr.update_log_archive_status_history(
                     trans, sys_info, inner_table_version_, *backup_lease_service_))) {
        LOG_WARN("failed to update log archive status history", K(ret), K(sys_info));
      } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, sys_info))) {
        LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(sys_info));
      } else if (force_stop) {
        LOG_INFO(
            "skip update external log archive backup info during force_stop", K(sys_info), K(sys_non_frozen_piece));
      } else if (OB_FAIL(info_mgr.update_external_backup_piece(sys_non_frozen_piece, *backup_lease_service_))) {
        LOG_WARN("failed to update external backup piece", K(ret), K(sys_non_frozen_piece));
      } else if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("succeed to do_stop_sys_log_archive_backup_v2_",
            K(ret),
            K(tenant_id),
            K(inner_table_version_),
            K(sys_info),
            K(sys_non_frozen_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }

  return ret;
}
int ObLogArchiveScheduler::stop_log_archive_backup_(const bool force_stop, share::ObLogArchiveBackupInfo &cur_sys_info,
    share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  bool is_all_stop = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!force_stop) {
    if (OB_FAIL(tenant_ids.assign(tenant_ids_))) {
      LOG_WARN("failed to assign tenant ids", K(ret));
    }
  } else if (OB_FAIL(get_tenant_ids_from_schema_(tenant_ids))) {
    LOG_WARN("failed to get tenant ids from schema", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(check_server_stop_backup_(cur_sys_info, tenant_ids, is_all_stop))) {
    LOG_WARN("failed to check_all_server_stop_log_archive_backup", K(ret));
  } else if (!is_all_stop) {
    LOG_INFO("not all server stop backup, waiting", K(cur_sys_info));
  } else if (inner_table_version_ >= OB_BACKUP_INNER_TABLE_V2) {
    if (OB_FAIL(do_stop_log_archive_backup_v2_(force_stop, tenant_ids, cur_sys_info, sys_non_frozen_piece))) {
      LOG_WARN("failed to do_stop_tenant_log_archive_backup_v2_",
          K(ret),
          K(force_stop),
          K(tenant_ids),
          K(cur_sys_info),
          K(sys_non_frozen_piece));
    }
  } else {
    // for compat upgrade from 2.2.76
    if (OB_FAIL(do_stop_log_archive_backup_(force_stop, tenant_ids, cur_sys_info))) {
      LOG_WARN("failed to to stop log archive backup", K(ret), K(cur_sys_info), K(force_stop), K(tenant_ids));
    }
  }
  return ret;
}

int ObLogArchiveScheduler::get_tenant_ids_from_schema_(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t stop_tenant_count = 0;
  hash::ObHashMap<uint64_t, ObTenantLogArchiveStatus *> log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;

  schema::ObSchemaGetterGuard schema_guard;
  common::ObArray<uint64_t> tmp_tenant_ids;
  tenant_ids.reuse();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get_schema_guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tmp_tenant_ids))) {
    LOG_WARN("failed to get_tenant_ids", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tmp_tenant_ids.at(i);
    if (tenant_id < OB_USER_TENANT_ID) {
      // do nothing
    } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("failed to push backup tenant ids", K(ret));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::check_server_stop_backup_(
    share::ObLogArchiveBackupInfo &sys_info, common::ObIArray<uint64_t> &tenant_ids, bool &is_all_stop)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  TENANT_ARCHIVE_STATUS_MAP log_archive_status_map;
  int64_t inactive_server_count = 0;
  int64_t user_tenant_count = 0;
  int64_t stop_tenant_count = 0;
  is_all_stop = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(
                 sys_info, allocator, log_archive_status_map, inactive_server_count))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  } else if (inactive_server_count > 0) {
    is_all_stop = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    ObServerTenantLogArchiveStatus *status = nullptr;

    if (tenant_id < OB_USER_TENANT_ID) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tenant ids should not has sys tenant", K(ret), K(tenant_id), K(i), K(tenant_ids));
    } else if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
      LOG_WARN("failed to get tenant log archive status", K(ret));
    } else if (sys_info.status_.incarnation_ != status->incarnation_ || sys_info.status_.round_ != status->round_) {
      ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
      LOG_WARN("tenant status not match", K(ret), K(tenant_id), K(sys_info), K(*status));
    } else {
      ++user_tenant_count;
      if (status->status_ == ObLogArchiveStatus::STOP) {
        ++stop_tenant_count;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == user_tenant_count) {
      FLOG_INFO("no tenant exist, just treat it as all tenant backup stop", K(ret));
    } else if (stop_tenant_count != user_tenant_count) {
      is_all_stop = false;
      LOG_WARN("[LOG_ARCHIVE] stop tenant count not match user tenant count, waiting stop",
          K(ret),
          K(stop_tenant_count),
          K(user_tenant_count));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::frozen_active_piece_before_stop_(const bool force_stop,
    share::ObLogArchiveBackupInfo &cur_sys_info, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int64_t max_gts = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!sys_non_frozen_piece.is_valid() || sys_non_frozen_piece.has_prev_piece_info_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_non_frozen_piece));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
      const uint64_t tenant_id = tenant_ids_.at(i);
      if (OB_FAIL(check_can_do_work_())) {
        LOG_WARN("failed to check can do work", K(ret));
      } else if (OB_FAIL(frozen_active_piece_before_stop_(force_stop, sys_non_frozen_piece, tenant_id, max_gts))) {
        LOG_WARN("failed to frozen_active_piece_before_stop_", K(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(frozen_sys_active_piece_before_stop_(force_stop, max_gts, cur_sys_info, sys_non_frozen_piece))) {
        LOG_WARN("failed to frozen_sys_old_piece_", K(ret), K(sys_non_frozen_piece));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::frozen_active_piece_before_stop_(const bool force_stop,
    const share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece, const uint64_t tenant_id, int64_t &max_gts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t timeout = 10 * 1000 * 1000;  // 10s
  ObLogArchiveBackupInfoMgr info_mgr;
  bool is_external_consistent = true;
  const bool for_update = true;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo backup_info;
  int64_t gts = 0;

  if (tenant_id <= 0 || !sys_non_frozen_piece.is_valid() || sys_non_frozen_piece.has_prev_piece_info_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_non_frozen_piece), K(tenant_id));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id, timeout, gts, is_external_consistent))) {
    LOG_WARN("failed to get ts sync", K(ret), K(tenant_id), K(timeout));
  } else if (!is_external_consistent) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot backup tenant without gts", K(ret), K(tenant_id));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(
                 *sql_proxy_, false /*for_update*/, tenant_id, inner_table_version_, backup_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    share::ObBackupPieceInfo tenant_piece;
    share::ObBackupPieceInfoKey cur_key = sys_non_frozen_piece.cur_piece_info_.key_;
    cur_key.tenant_id_ = tenant_id;
    max_gts = std::max(max_gts, gts);
    if (OB_FAIL(info_mgr.get_backup_piece(trans, for_update, cur_key, tenant_piece))) {
      LOG_WARN("failed to get backup piece", K(ret), K(cur_key));
    } else {
      tenant_piece.max_ts_ = gts;
      tenant_piece.checkpoint_ts_ = backup_info.status_.checkpoint_ts_;
      tenant_piece.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
      if (!force_stop && OB_FAIL(info_mgr.update_external_backup_piece(tenant_piece, *backup_lease_service_))) {
        LOG_WARN("failed to update external backup piece", K(ret), K(tenant_piece));
      } else if (OB_FAIL(info_mgr.update_backup_piece(trans, tenant_piece))) {
        LOG_WARN("failed to update backup piece", K(ret), K(tenant_piece));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("finish frozen_active_piece_before_stop_", K(ret), K(tenant_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::frozen_sys_active_piece_before_stop_(const bool force_stop, const int64_t max_gts,
    share::ObLogArchiveBackupInfo &cur_sys_info, share::ObNonFrozenBackupPieceInfo &sys_non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo backup_info;

  if (!sys_non_frozen_piece.is_valid() || sys_non_frozen_piece.has_prev_piece_info_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_non_frozen_piece));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    OB_LOG(WARN, "fail to start trans", K(ret));
  } else {
    sys_non_frozen_piece.cur_piece_info_.max_ts_ = max_gts;
    sys_non_frozen_piece.cur_piece_info_.checkpoint_ts_ = cur_sys_info.status_.checkpoint_ts_;
    sys_non_frozen_piece.cur_piece_info_.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
    if (!force_stop &&
        OB_FAIL(info_mgr.update_external_backup_piece(sys_non_frozen_piece.cur_piece_info_, *backup_lease_service_))) {
      LOG_WARN("failed to update external backup piece", K(ret), K(sys_non_frozen_piece));
    } else if (OB_FAIL(info_mgr.update_backup_piece(trans, sys_non_frozen_piece.cur_piece_info_))) {
      LOG_WARN("failed to update backup piece", K(ret), K(sys_non_frozen_piece));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_trans_(trans))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      } else {
        FLOG_INFO("finish frozen_sys_active_piece_before_stop_", K(ret), K(sys_non_frozen_piece));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
        OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::do_stop_log_archive_backup_(const bool force_stop,
    const common::ObIArray<uint64_t> &tenant_ids, const share::ObLogArchiveBackupInfo &cur_sys_info)
{
  int ret = OB_SUCCESS;
  int64_t user_tenant_count = 0;
  int64_t stop_tenant_count = 0;
  TENANT_ARCHIVE_STATUS_MAP log_archive_status_map;
  ObArenaAllocator allocator;
  ObLogArchiveBackupInfoMgr info_mgr;
  share::ObLogArchiveBackupInfo sys_info = cur_sys_info;
  int64_t inactive_server_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(fetch_log_archive_backup_status_map_(
                 sys_info, allocator, log_archive_status_map, inactive_server_count))) {
    LOG_WARN("failed to fetch_log_archive_backup_status_map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    ObServerTenantLogArchiveStatus *status = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid tenatn_id", K(ret), K(tenant_id), K(i), K(tenant_ids));
    } else if (OB_FAIL(log_archive_status_map.get_refactored(tenant_id, status))) {
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
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (tenant_id < OB_USER_TENANT_ID) {
        continue;
      } else if (OB_FAIL(do_stop_tenant_log_archive_backup_(tenant_id, sys_info, force_stop))) {
        LOG_WARN("failed to stop_tenant_log_archive_backup_", K(ret), K(tenant_id), K(sys_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(info_mgr.update_log_archive_status_history(
            *sql_proxy_, sys_info, inner_table_version_, *backup_lease_service_))) {
      LOG_WARN("failed to update_log_archive_status_history", K(ret), K(sys_info));
    } else if (OB_FAIL(update_sys_backup_info_(cur_sys_info, sys_info))) {
      LOG_WARN("failed to update sys backup info", K(ret), K(sys_info), K(cur_sys_info));
    } else {
      FLOG_INFO("[LOG_ARCHIVE] succeed to commit update log archive stop", K(sys_info));
      ROOTSERVICE_EVENT_ADD("log_archive", "change_status", "new_status", "STOP", "round_id", sys_info.status_.round_);
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::do_stop_tenant_log_archive_backup_(
    const uint64_t tenant_id, const ObLogArchiveBackupInfo &sys_info, const bool force_stop)
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
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, inner_table_version_, info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(sys_info));
    }

    if (OB_SUCC(ret)) {
      if (info.status_.incarnation_ != sys_info.status_.incarnation_) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not support diff incarnation", K(ret), K(info), K(sys_info));
      } else if (ObLogArchiveStatus::STOP != info.status_.status_) {
        info.status_.status_ = ObLogArchiveStatus::STOP;
        if (OB_FAIL(info_mgr.update_log_archive_status_history(
                *sql_proxy_, info, inner_table_version_, *backup_lease_service_))) {
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
          if (OB_FAIL(info_mgr.update_log_archive_status_history(
                  *sql_proxy_, info, inner_table_version_, *backup_lease_service_))) {
            LOG_WARN("failed to update log archive status history", K(ret));
          }
        } else {
          ret = OB_INVALID_LOG_ARCHIVE_STATUS;
          LOG_ERROR("tenant log archive round not match", K(ret), K(info), K(sys_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      info.status_.backup_piece_id_ = sys_info.status_.backup_piece_id_;
      info.status_.start_piece_id_ = sys_info.status_.start_piece_id_;

      if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, info))) {
        LOG_WARN("failed to update_log_archive_backup_info", K(ret), K(info));
      } else if (force_stop) {
        LOG_INFO("skip update external log archive backup info during force_stop", K(info));
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

int ObLogArchiveScheduler::set_log_archive_backup_interrupted_(const share::ObLogArchiveBackupInfo &cur_sys_info)
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
          "log_archive", "change_status", "new_status", "INTERRUPTED", "round_id", sys_info.status_.round_);
      if (OB_FAIL(info_mgr.update_extern_log_archive_backup_info(sys_info, *backup_lease_service_))) {
        LOG_WARN("failed to update update_extern_log_archive_backup_info", K(ret), K(sys_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveScheduler::set_tenants_log_archive_backup_interrupted_(share::ObLogArchiveBackupInfo &sys_info)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr info_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    ObTenantLogArchiveStatus *status = nullptr;
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
    const uint64_t tenant_id, const ObLogArchiveBackupInfo &sys_info)
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
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(trans, for_update, tenant_id, inner_table_version_, info))) {
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
        if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, info))) {
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

int ObLogArchiveScheduler::check_server_status_(ObArray<common::ObAddr> &inactive_server_list)
{
  int ret = OB_SUCCESS;
  common::ObZone inactive_zone;
  common::ObZone tmp_zone;
  int64_t zone_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone_count(zone_count))) {
    LOG_WARN("failed to get zone count", K(ret));
  } else if (zone_count <= 2 && !inactive_server_list.empty()) {
    ret = OB_SERVER_NOT_ACTIVE;
    LOG_WARN(
        "cannot update log archive status during server is inactive", K(ret), K(zone_count), K(inactive_server_list));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < inactive_server_list.count(); ++i) {
    const ObAddr &addr = inactive_server_list.at(i);
    if (OB_FAIL(server_mgr_->get_server_zone(addr, tmp_zone))) {
      LOG_WARN("failed to get server zone", K(ret), K(addr));
    } else if (inactive_zone.is_empty()) {
      inactive_zone = tmp_zone;
      LOG_WARN("server is not alive, record inactive zone", K(addr), K(tmp_zone));
    } else if (inactive_zone != tmp_zone) {
      ret = OB_SERVER_NOT_ACTIVE;
      LOG_ERROR("cannot update log archive status when two zones have some servers not alive",
          K(inactive_zone),
          K(tmp_zone),
          K(inactive_server_list));
    } else {
      FLOG_WARN("server of inactive zone is not alive", K(addr), K(tmp_zone));
    }
  }

  return ret;
}

int ObLogArchiveScheduler::fetch_log_archive_backup_status_map_(const share::ObLogArchiveBackupInfo &sys_info,
    ObIAllocator &allocator, TENANT_ARCHIVE_STATUS_MAP &log_archive_status_map, int64_t &inactive_server_count)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObArray<common::ObAddr> active_server_list;
  ObArray<common::ObAddr> inactive_server_list;
  share::ObGetTenantLogArchiveStatusArg arg;
  share::ObServerTenantLogArchiveStatusWrapper result;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(server_mgr_->get_servers_by_status(active_server_list, inactive_server_list))) {
    LOG_WARN("failed to get servers by status", K(ret));
  } else if (active_server_list.empty()) {
    ret = OB_SERVER_NOT_ACTIVE;
    LOG_ERROR("no server is alive, cannot update log archive status", K(ret), K(inactive_server_list));
  } else if (OB_FAIL(check_server_status_(inactive_server_list))) {
    LOG_WARN("failed to check server status", K(ret), K(inactive_server_list));
  } else if (OB_FAIL(log_archive_status_map.create(active_server_list.count(), ObModIds::OB_LOG_ARCHIVE_SCHEDULER))) {
    LOG_WARN("failed to create hashmap", K(ret));
  } else {
    arg.incarnation_ = sys_info.status_.incarnation_;
    arg.round_ = sys_info.status_.round_;
    if (sys_info.status_.start_piece_id_ != 0) {
      arg.backup_piece_id_ = sys_info.status_.backup_piece_id_;
    } else {
      arg.backup_piece_id_ = 0;
    }
    inactive_server_count = inactive_server_list.count();
  }

  // TODO(): [backup2] use async rpc
  for (int64_t server_idx = 0; OB_SUCC(ret) && server_idx < active_server_list.count(); ++server_idx) {
    const common::ObAddr &addr = active_server_list.at(server_idx);
    if (OB_FAIL(check_can_do_work_())) {
      LOG_WARN("failed to check can do work", K(ret));
    } else if (OB_FAIL(fetch_server_tenant_log_archive_status_(addr, arg, result))) {
      LOG_WARN("failed to fetch server tenant log archive status", K(ret), K(addr), K(arg));
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
      ObServerTenantLogArchiveStatus &status = result.status_array_.at(i);
      if (!status.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid log archive status", K(ret), K(i), K(addr), K(status));
      } else if (ObLogArchiveStatus::INTERRUPTED == status.status_) {
        ret = OB_LOG_ARCHIVE_INTERRUPTED;
        LOG_ERROR(
            "[LOG_ARCHIVE] log archive status is interrupted, need human intervention", K(ret), K(addr), K(status));
      } else if (status.incarnation_ != sys_info.status_.incarnation_ || status.round_ != sys_info.status_.round_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server log archive status incarnation or round not match", K(ret), K(sys_info), K(status));
      } else {
#ifdef ERRSIM
        const int64_t fake_report_server_tenant_start_ts = GCONF.fake_report_server_tenant_start_ts;
        if (0 != fake_report_server_tenant_start_ts) {
          status.start_ts_ = MIN(status.start_ts_, fake_report_server_tenant_start_ts);
        }
        const int64_t fake_archive_log_checkpoint_ts = GCONF.fake_archive_log_checkpoint_ts;
        if (0 != fake_archive_log_checkpoint_ts) {
          status.checkpoint_ts_ = MIN(status.checkpoint_ts_, fake_archive_log_checkpoint_ts);
        }
        const int64_t fake_archive_log_status = GCONF.fake_archive_log_status;
        if (0 != fake_archive_log_status) {
          status.status_ = static_cast<ObLogArchiveStatus::STATUS>(fake_archive_log_status);
        }
#endif

        ObServerTenantLogArchiveStatus *value = nullptr;
        hash_ret = log_archive_status_map.get_refactored(status.tenant_id_, value);
        if (OB_SUCCESS == hash_ret) {
          if (value->status_ != status.status_) {
            ret = OB_EAGAIN;
            LOG_WARN("log archive status not match", K(addr), K(ret), K(status), K(*value));
          }

          if (OB_SUCC(ret)) {
            if (value->checkpoint_ts_ > status.checkpoint_ts_) {
              LOG_INFO(
                  "update status checkpoint ts for fetch_log_archive_backup_status_map", K(addr), K(*value), K(status));
              value->checkpoint_ts_ = status.checkpoint_ts_;
            }

            if (value->start_ts_ < status.start_ts_) {
              LOG_INFO("update status start ts for fetch_log_archive_backup_status_map", K(addr), K(*value), K(status));
              value->start_ts_ = status.start_ts_;
            }

            if (value->min_backup_piece_id_ > status.min_backup_piece_id_) {
              LOG_INFO("update status backup_piece_idfor fetch_log_archive_backup_status_map",
                  K(addr),
                  K(*value),
                  K(status));
              value->min_backup_piece_id_ = status.min_backup_piece_id_;
            }
          }
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          void *tmp = allocator.alloc(sizeof(ObServerTenantLogArchiveStatus));
          if (OB_ISNULL(tmp)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc buf", K(ret));
          } else {
            ObServerTenantLogArchiveStatus *ptr = new (tmp) ObServerTenantLogArchiveStatus;
            *ptr = status;
            if (OB_FAIL(log_archive_status_map.set_refactored(status.tenant_id_, ptr))) {
              LOG_WARN("Failed to set log_archive_status_map", K(ret), K(*ptr));
            } else {
              LOG_INFO("add new status for fetch_log_archive_backup_status_map", K(addr), K(*ptr));
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

int ObLogArchiveScheduler::fetch_server_tenant_log_archive_status_(const common::ObAddr &addr,
    const share::ObGetTenantLogArchiveStatusArg &arg, share::ObServerTenantLogArchiveStatusWrapper &result)
{
  int ret = OB_SUCCESS;
  const uint64_t cluster_observer_version = GET_MIN_CLUSTER_VERSION();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_can_do_work_())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else {
    if (OB_FAIL(fetch_server_tenant_log_archive_status_v2_(addr, arg, result))) {
      LOG_WARN("failed to fetch_server_tenant_log_archive_status_v2", K(ret), K(arg));
    }
  }
  FLOG_INFO("succeed to fetch_server_tenant_log_archive_status_",
      K(ret),
      K(cluster_observer_version),
      K(addr),
      K(arg),
      K(result));

  return ret;
}

int ObLogArchiveScheduler::fetch_server_tenant_log_archive_status_v1_(const common::ObAddr &addr,
    const share::ObGetTenantLogArchiveStatusArg &arg, share::ObServerTenantLogArchiveStatusWrapper &result)
{
  int ret = OB_SUCCESS;
  share::ObTenantLogArchiveStatusWrapper tmp_result;
  ObServerTenantLogArchiveStatus status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(addr).get_tenant_log_archive_status(arg, tmp_result))) {
    LOG_WARN("failed to get_tenant_log_archive_status", K(ret), K(addr), K(arg));
  } else {
    result.result_code_ = tmp_result.result_code_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_result.status_array_.count(); ++i) {
      if (OB_FAIL(status.set_status(tmp_result.status_array_.at(i)))) {
        LOG_WARN("failed to get status", K(ret), K(i), "tmp_status", tmp_result.status_array_.at(i));
      } else if (OB_FAIL(result.status_array_.push_back(status))) {
        LOG_WARN("failed to add status", K(ret));
      }
    }
  }

  return ret;
}

int ObLogArchiveScheduler::fetch_server_tenant_log_archive_status_v2_(const common::ObAddr &addr,
    const share::ObGetTenantLogArchiveStatusArg &arg, share::ObServerTenantLogArchiveStatusWrapper &result)
{
  int ret = OB_SUCCESS;
  share::ObTenantLogArchiveStatusWrapper tmp_result;
  ObServerTenantLogArchiveStatus status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(addr).get_tenant_log_archive_status_v2(arg, result))) {
    LOG_WARN("failed to get_tenant_log_archive_status", K(ret), K(addr), K(arg));
  }

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

int ObLogArchiveScheduler::commit_trans_(common::ObMySQLTransaction &trans)
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
    const share::ObLogArchiveBackupInfo &cur_info, share::ObLogArchiveBackupInfo &new_info)
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
    if (OB_FAIL(info_mgr.get_log_archive_backup_info(
            trans, for_update, new_info.status_.tenant_id_, inner_table_version_, read_info))) {
      LOG_WARN("failed to get_log_archive_backup_info", K(ret), K(new_info));
    } else if (!cur_info.is_same(read_info)) {
      ret = OB_EAGAIN;
      LOG_WARN("sys info is changed before update", K(ret), K(cur_info), K(read_info), K(new_info));
    } else if (OB_FAIL(info_mgr.update_log_archive_backup_info(trans, inner_table_version_, new_info))) {
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

int ObLogArchiveScheduler::force_cancel(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tenant_id != OB_SYS_TENANT_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only sys tenant can force cancel archive", K(ret));
  } else if (OB_FAIL(handle_enable_log_archive(false))) {
    LOG_WARN("failed to force cancel archive", K(ret));
  }

  FLOG_WARN("force_cancel archive", K(ret));

  return ret;
}
