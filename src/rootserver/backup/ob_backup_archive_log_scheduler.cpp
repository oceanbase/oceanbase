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
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/backup/ob_backup_archive_log_scheduler.h"
#include "rootserver/backup/ob_backup_data_mgr.h"
#include "share/backup/ob_backup_backuppiece_operator.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_tenant_name_mgr.h"
#include "share/backup/ob_backup_manager.h"
#include "share/backup/ob_backup_path.h"
#include "archive/ob_archive_path.h"
#include "archive/ob_archive_file_utils.h"
#include "lib/restore/ob_storage.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_mutex.h"
#include "observer/ob_server.h"

#include <algorithm>

using namespace oceanbase::archive;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;
using namespace oceanbase::lib;
using namespace oceanbase::common::hash;

namespace oceanbase {
namespace rootserver {

int64_t ObBackupArchiveLogIdling::get_idle_interval_us()
{
  int64_t actual_idle_time = 0;
  const int64_t idle_time = get_local_idle_time();
  if (0 == idle_time) {
    actual_idle_time = 10 * 60 * 1000 * 1000;  // 10 min
  } else {
    actual_idle_time = idle_time;
  }
  return actual_idle_time;
}

void ObBackupArchiveLogIdling::set_idle_interval_us(int64_t idle_time)
{
  idle_time_us_ = idle_time;
}

ObBackupArchiveLogScheduler::ObBackupArchiveLogScheduler()
    : is_inited_(false),
      is_working_(false),
      is_paused_(true),
      schedule_round_(0),
      local_sys_tenant_checkpoint_ts_(0),
      idling_(stop_),
      server_mgr_(NULL),
      rpc_proxy_(NULL),
      sql_proxy_(NULL),
      backup_lease_service_(NULL),
      schema_service_(NULL),
      tenant_queue_map_(),
      server_status_map_(),
      inner_table_version_(OB_BACKUP_INNER_TABLE_VMAX)
{}

ObBackupArchiveLogScheduler::~ObBackupArchiveLogScheduler()
{}

int ObBackupArchiveLogScheduler::init(ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
    common::ObMySQLProxy& sql_proxy, share::ObIBackupLeaseService& backup_lease_service,
    share::schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup archive log scheduler init twice", KR(ret));
  } else if (OB_FAIL(create(thread_cnt, "BACKUP_ARCHIVE_LOG_SCHEDULER"))) {
    LOG_WARN("failed to create backup archive log scheduler thread", KR(ret));
  } else if (OB_FAIL(tenant_queue_map_.create(MAX_BUCKET_NUM, ObModIds::BACKUP))) {
    LOG_WARN("failed to create hash map", KR(ret));
  } else if (OB_FAIL(server_status_map_.create(MAX_BUCKET_NUM, ObModIds::BACKUP))) {
    LOG_WARN("failed to create server status map", KR(ret));
  } else if (OB_FAIL(round_stat_.init())) {
    LOG_WARN("failed to init round stat", KR(ret));
  } else {
    server_mgr_ = &server_mgr;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    backup_lease_service_ = &backup_lease_service;
    schema_service_ = &schema_service;
    inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

void ObBackupArchiveLogScheduler::stop()
{
  if (IS_NOT_INIT) {
    LOG_WARN("not init");
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObBackupArchiveLogScheduler::idle()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t DEFAULT_IDLE_TIME = 10 * 60 * 1000 * 1000;  // 10min
  int64_t actual_idle_time = DEFAULT_IDLE_TIME;
  ObBackupDestOpt opt;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = opt.init(true /*is backup backup*/))) {
      LOG_WARN("failed to init backup opt", K(tmp_ret));
    } else if (opt.log_archive_checkpoint_interval_ > 0) {
      actual_idle_time = opt.log_archive_checkpoint_interval_ / 2;
    }
    if (OB_FAIL(idling_.idle(actual_idle_time))) {
      LOG_WARN("idle failed", KR(ret));
    } else {
      LOG_INFO("backup archive log idling",
          "log_archive_checkpoint_interval_",
          opt.log_archive_checkpoint_interval_,
          K(actual_idle_time));
    }
  }
  return ret;
}

void ObBackupArchiveLogScheduler::wakeup()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("backup archive log scheduler do not init");
  } else {
    idling_.wakeup();
    LOG_INFO("backup archive log scheduler wakeup");
  }
}

void ObBackupArchiveLogScheduler::run3()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  bool is_switch_piece = false;

  while (!stop_) {
    inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;
    if (OB_FAIL(check_can_do_work())) {
      LOG_WARN("failed to check_can_do_work", K(ret));
    } else if (OB_FAIL(check_is_switch_piece(is_switch_piece))) {
      LOG_WARN("failed to check is switch piece", KR(ret));
    } else {
      if (is_switch_piece) {
        if (OB_FAIL(do_backup_backuppiece())) {
          LOG_WARN("failed to do backup backuppiece", KR(ret));
        }
      } else {
        if (OB_FAIL(do_backup_archivelog())) {
          LOG_WARN("failed to do backup archivelog", KR(ret));
        }
      }
    }

    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", K(ret));
    }
  }
  clear_memory_resource_if_stopped();
  round_stat_.reuse();
  LOG_INFO("rs has switch");
}

int ObBackupArchiveLogScheduler::check_is_switch_piece(bool& is_switch_piece)
{
  int ret = OB_SUCCESS;
  bool is_enable = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(check_is_enable_auto_backup(is_enable))) {
    LOG_WARN("failed to do check is enable auto backup", KR(ret));
  } else {
    is_switch_piece = !is_enable;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_is_round_mode(
    const uint64_t tenant_id, const int64_t round_id, bool& is_round_mode)
{
  int ret = OB_SUCCESS;
  is_round_mode = false;
  ObArray<ObLogArchiveBackupInfo> info_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || round_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(round_id));
  } else if (OB_FAIL(get_log_archive_info_list(tenant_id, info_list))) {
    LOG_WARN("failed to get log archive info list", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObLogArchiveBackupInfo& info = info_list.at(i);
      if (info.status_.round_ == round_id) {
        is_round_mode = 0 == info.status_.start_piece_id_;
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_backup_archivelog()
{
  int ret = OB_SUCCESS;
  bool in_history = true;
  bool archivelog_started = false;
  int64_t round = 0;
  ObBackupDest src_backup_dest, dst_backup_dest;
  ObArray<int64_t> b_round_list;
  ObArray<int64_t> bb_round_list;
  ObArray<int64_t> round_list;
  ObArray<ObLogArchiveBackupInfo> info_list;
  if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else if (OB_FAIL(get_log_archive_round_list(OB_SYS_TENANT_ID, info_list, b_round_list))) {
    LOG_WARN("failed to get log archive round list", KR(ret));
  } else if (OB_FAIL(get_bb_sys_his_log_archive_info_list(bb_round_list))) {
    LOG_WARN("failed to get bb sys his log archive info list", KR(ret));
  } else if (OB_FAIL(get_delta_round_list(b_round_list, bb_round_list, round_list))) {
    LOG_WARN("failed to get delta round list", KR(ret));
  } else if (OB_FAIL(check_archivelog_started(info_list, archivelog_started))) {
    LOG_WARN("failed to check archivelog started", KR(ret), K(info_list));
  } else if (!archivelog_started) {
    LOG_DEBUG("alter system archivelog has not executed, can not start backup backup");
  } else if (OB_FAIL(get_next_round(round_list, round))) {
    LOG_WARN("failed to get next round", KR(ret), K(round_list));
  } else if (OB_FAIL(may_do_backup_backup_dest_changed())) {
    LOG_WARN("failed to may do backup backup dest changed", KR(ret));
  } else if (round_list.empty()) {
    LOG_INFO("do nothing if round list is empty");
  } else if (OB_FAIL(check_is_history_round(round, in_history))) {
    LOG_WARN("failed to check is history round", KR(ret), K(round));
  } else if (in_history && OB_FAIL(round_stat_.set_current_round_in_history())) {
    LOG_WARN("failed to set current round in history", KR(ret));
  } else if (OB_FAIL(get_backup_dest_info(OB_SYS_TENANT_ID, round, src_backup_dest, dst_backup_dest))) {
    LOG_WARN("failed to get backup dest info", KR(ret), K(round));
  } else if (OB_FAIL(update_extern_backup_info(src_backup_dest, dst_backup_dest))) {
    LOG_WARN("failed to update extern backup info", KR(ret));
  } else if (OB_FAIL(create_mount_file(dst_backup_dest, round))) {
    LOG_WARN("failed to check mount file", KR(ret), K(dst_backup_dest), K(round));
  } else if (OB_FAIL(do_schedule(round, src_backup_dest, dst_backup_dest))) {
    LOG_WARN("failed to do schedule", KR(ret), K(round));
  } else if (OB_FAIL(do_if_round_finished(round, dst_backup_dest))) {
    LOG_WARN("failed to do if round finished", KR(ret), K(round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_server_busy(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  ServerStatus status;
  status.status_ = ServerStatus::STATUS_BUSY;
  status.lease_time_ = ObTimeUtil::current_time();
  if (OB_FAIL(server_status_map_.set_refactored(addr, status, 1))) {
    LOG_WARN("failed to set server free", KR(ret));
  } else {
    LOG_INFO("set server busy", K(addr));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_server_free(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  ServerStatus status;
  status.status_ = ServerStatus::STATUS_FREE;
  status.lease_time_ = ObTimeUtil::current_time();
  if (OB_FAIL(server_status_map_.set_refactored(addr, status, 1))) {
    LOG_WARN("failed to set server free", KR(ret));
  } else {
    LOG_INFO("set server free", K(addr));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_paused(const bool paused)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else {
    is_paused_ = paused;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_checkpoint_ts(const uint64_t tenant_id, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(checkpoint_ts));
  } else {
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // It may be because of the cut
      ret = hash_ret;
      LOG_WARN("tenant id do not exist", KR(ret));
    } else if (OB_SUCCESS == hash_ret) {
      queue_ptr->checkpoint_ts_ = checkpoint_ts;
    } else {
      ret = hash_ret;
      LOG_WARN("get refactored error", KR(ret), K(hash_ret));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_checkpoint_ts(const uint64_t tenant_id, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  checkpoint_ts = 0;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(checkpoint_ts));
  } else {
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // It may be because of the cut
      ret = hash_ret;
      LOG_WARN("tenant id do not exist", KR(ret));
    } else if (OB_SUCCESS == hash_ret) {
      checkpoint_ts = queue_ptr->checkpoint_ts_;
    } else {
      ret = hash_ret;
      LOG_WARN("get refactored error", KR(ret), K(hash_ret));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_checkpoint_ts_with_lock(const uint64_t tenant_id, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(set_checkpoint_ts(tenant_id, checkpoint_ts))) {
    LOG_WARN("failed to set checkpoint ts", KR(ret), K(tenant_id), K(checkpoint_ts));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_checkpoint_ts_with_lock(const uint64_t tenant_id, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(get_checkpoint_ts(tenant_id, checkpoint_ts))) {
    LOG_WARN("failed to set checkpoint ts", KR(ret), K(tenant_id), K(checkpoint_ts));
  }
  return ret;
}

// current turn finished, 可以推checkpoint ts
// current round finished，可以换round
int ObBackupArchiveLogScheduler::set_pg_finish(const int64_t archive_round, const int64_t checkpoint_ts,
    const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  ObLockGuard<ObMutex> guard(mutex_);
  bool current_turn_in_history = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (archive_round <= 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set pg finish move get invalid argument", KR(ret), K(archive_round), K(tenant_id));
  } else if (pg_list.empty()) {
    LOG_INFO("pg list is empty");
  } else if (OB_FAIL(check_current_turn_in_history(tenant_id, current_turn_in_history))) {
    LOG_WARN("failed to check current turn in history", KR(ret));
  } else if (OB_FAIL(check_response_checkpoint_ts_valid(tenant_id, checkpoint_ts, is_valid))) {
    LOG_WARN("failed to check response checkpoint ts is valid", KR(ret), K(tenant_id), K(checkpoint_ts));
  } else if (!is_valid) {
    ret = OB_RS_STATE_NOT_ALLOW;
    LOG_WARN("checkpoint ts is not valid, may switch rs happened", KR(ret));
  } else if (OB_FAIL(mark_current_turn_pg_list(tenant_id, pg_list, true /*finished*/))) {
    LOG_WARN("failed to mark current turn pg list finished", KR(ret), K(tenant_id));
  } else if (current_turn_in_history && OB_FAIL(round_stat_.mark_pg_list_finished(archive_round, tenant_id, pg_list))) {
    LOG_WARN("failed to mark pg list finished", KR(ret), K(archive_round), K(tenant_id));
  } else {
    LOG_INFO("set pg list finish", K(current_turn_in_history), K(archive_round), K(tenant_id), K(pg_list));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_archive_beginning(const uint64_t tenant_id, bool& is_beginning)
{
  int ret = OB_SUCCESS;
  is_beginning = false;
  const bool for_update = false;
  ObLogArchiveBackupInfo cur_info;
  ObLogArchiveBackupInfoMgr info_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(info_mgr.get_log_archive_backup_info(
                 *sql_proxy_, for_update, tenant_id, inner_table_version_, cur_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get log archive backup info", KR(ret), K(tenant_id));
    }
  } else {
    is_beginning = ObLogArchiveStatus::BEGINNING == cur_info.status_.status_;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_if_pg_task_finished(
    const int64_t round_id, const uint64_t tenant_id, const ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  bool current_turn_finished = false;
  int64_t copy_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else {
    TenantTaskQueue* queue_ptr = NULL;
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // may had switched rs
    } else if (OB_SUCCESS == hash_ret) {
      if (queue_ptr->queue_.is_empty()) {
        int64_t checkpoint_ts = 0;
        bool is_switch_piece = false;
        bool is_stopped = false;
        if (!queue_ptr->pushed_before_) {
          // do nothing
        } else if (OB_FAIL(check_tenant_task_stopped(tenant_id, is_stopped))) {
          LOG_WARN("failed to check tenant task stoped", KR(ret), K(tenant_id));
        } else if (is_stopped) {
          // do nothing if stopped
        } else if (OB_FAIL(check_current_turn_pg_finished(tenant_id, current_turn_finished))) {
          LOG_WARN("failed to check current turn pg finished", KR(ret), K(tenant_id));
        } else if (!current_turn_finished) {
          LOG_INFO("current turn not finished");
        } else if (OB_FAIL(get_checkpoint_ts_with_lock(tenant_id, checkpoint_ts))) {
          LOG_WARN("failed to get tenant checkpoint ts", KR(ret), K(tenant_id));
        } else if (0 == checkpoint_ts) {
          LOG_INFO("do nothing is checkpoint ts is 0", K(round_id), K(tenant_id));
        } else if (OB_FAIL(check_is_switch_piece(is_switch_piece))) {
          LOG_WARN("failed to check is switch piece", KR(ret));
        } else {
          if (is_switch_piece) {
            ObBackupBackupPieceJobInfo job_info;
            ObBackupBackupPieceTaskInfo cur_task;
            ObBackupBackupCopyIdLevel copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
            if (OB_FAIL(get_smallest_job_info(job_info))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get smallest unfinished job info", KR(ret), K(job_info));
              }
            } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
              LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
            } else if (OB_FAIL(get_smallest_doing_backup_piece_task(job_info.job_id_, job_info.tenant_id_, cur_task))) {
              LOG_WARN("failed to get smallest doing backup piece task", KR(ret), K(job_info));
            } else {
              if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level) {
                if (OB_FAIL(check_backup_info(tenant_id, dst))) {
                  LOG_WARN("failed to check backup info", KR(ret), K(tenant_id));
                } else if (OB_FAIL(update_tenant_inner_table_cur_info(tenant_id,
                               round_id,
                               checkpoint_ts,
                               cur_task.copy_id_,
                               cur_task.piece_id_,
                               ObLogArchiveStatus::DOING,
                               dst,
                               *sql_proxy_))) {
                  LOG_WARN("failed to update tenant inner table current info", KR(ret), K(tenant_id), K(round_id));
                } else if (OB_FAIL(update_extern_tenant_clog_backup_info(tenant_id,
                               round_id,
                               cur_task.piece_id_,
                               cur_task.copy_id_,
                               checkpoint_ts,
                               dst,
                               ObLogArchiveStatus::DOING))) {
                  LOG_WARN("failed to update tenant clog backup info",
                      KR(ret),
                      K(tenant_id),
                      K(round_id),
                      K(copy_id),
                      K(dst));
                }
              } else {
                if (OB_FAIL(update_extern_tenant_clog_backup_info(tenant_id,
                        round_id,
                        cur_task.piece_id_,
                        cur_task.copy_id_,
                        checkpoint_ts,
                        dst,
                        ObLogArchiveStatus::DOING))) {
                  LOG_WARN("failed to update tenant clog backup info",
                      KR(ret),
                      K(tenant_id),
                      K(round_id),
                      K(copy_id),
                      K(dst));
                }
              }
            }
          } else {
            if (OB_FAIL(check_backup_info(tenant_id, dst))) {
              LOG_WARN("failed to check backup info", KR(ret), K(tenant_id));
            } else if (OB_FAIL(get_current_round_copy_id(dst, tenant_id, round_id, copy_id))) {
              LOG_WARN("failed to get current round copy id", KR(ret), K(tenant_id));
            } else if (OB_FAIL(update_extern_tenant_clog_backup_info(tenant_id,
                           round_id,
                           0 /*piece_id*/,
                           copy_id,
                           checkpoint_ts,
                           dst,
                           ObLogArchiveStatus::DOING))) {
              LOG_WARN(
                  "failed to update tenant clog backup info", KR(ret), K(tenant_id), K(round_id), K(copy_id), K(dst));
            } else if (OB_FAIL(update_round_piece_info_for_clean(
                           tenant_id, round_id, false /*round_finished*/, dst, *sql_proxy_))) {
              LOG_WARN("failed to update extern round piece info for clean", KR(ret));
            } else if (OB_FAIL(update_tenant_inner_table_cur_info(tenant_id,
                           round_id,
                           checkpoint_ts,
                           copy_id,
                           0 /*piece_id*/,
                           ObLogArchiveStatus::DOING,
                           dst,
                           *sql_proxy_))) {
              LOG_WARN("failed to update tenant inner table current info", KR(ret), K(tenant_id), K(round_id));
            }
          }
        }
        queue_ptr->need_fetch_new_ = true;
      } else {
        LOG_INFO("tenant no need fetch new", K(tenant_id));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("get refactored error", KR(ret));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_sys_tenant_checkpoint_ts()
{
  int ret = OB_SUCCESS;
  int64_t min_checkpoint_ts = INT64_MAX;
  int64_t copy_id = 0;
  const int64_t round = round_stat_.get_current_round();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (0 == local_sys_tenant_checkpoint_ts_) {
    // do nothing
  } else if (OB_FAIL(gc_unused_tenant_resources(round))) {
    LOG_WARN("failed to gc unused tenant resources", KR(ret), K(round));
  } else {
    typedef common::hash::ObHashMap<uint64_t, TenantTaskQueue*>::iterator Iterator;
    Iterator iter = tenant_queue_map_.begin();
    for (; OB_SUCC(ret) && iter != tenant_queue_map_.end(); ++iter) {
      bool cur_finished = false;
      const uint64_t tenant_id = iter->first;
      const int64_t checkpoint_ts = iter->second->checkpoint_ts_;
      bool is_dropped = false;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
        LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
      } else if (OB_FAIL(check_cur_tenant_task_is_finished(round, tenant_id, cur_finished))) {
        LOG_WARN("failed to check cur tenant task is finished", KR(ret), K(round));
      } else if (cur_finished && is_dropped) {
        continue;
      } else if (0 == checkpoint_ts) {
        ret = OB_EAGAIN;
        LOG_WARN("the tenant checkpoint ts is 0", KR(ret), K(tenant_id));
        break;
      } else {
        min_checkpoint_ts = std::min(min_checkpoint_ts, checkpoint_ts);
      }
    }
    if (OB_SUCC(ret)) {
      ObBackupDest dst;
      const int64_t tenant_id = OB_SYS_TENANT_ID;
      const int64_t checkpoint_ts = min_checkpoint_ts;
      char dst_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
      if (INT64_MAX == min_checkpoint_ts) {
        // do nothing
      } else if (OB_FAIL(get_dst_backup_dest(tenant_id, dst_backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to set backup backup dest if needed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(dst.set(dst_backup_dest))) {
        LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest));
      } else if (OB_FAIL(check_backup_info(tenant_id, dst))) {
        LOG_WARN("failed to check backup info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(get_current_round_copy_id(dst, tenant_id, round, copy_id))) {
        LOG_WARN("failed to get current round copy id", KR(ret), K(tenant_id));
      } else if (OB_FAIL(update_extern_tenant_clog_backup_info(
                     tenant_id, round, 0 /*piece_id*/, copy_id, checkpoint_ts, dst, ObLogArchiveStatus::DOING))) {
        LOG_WARN("failed to update tenant clog backup info", KR(ret), K(tenant_id), K(round));
      } else if (OB_FAIL(update_tenant_inner_table_cur_info(tenant_id,
                     round,
                     checkpoint_ts,
                     copy_id,
                     0 /*backup_piece_id*/,
                     ObLogArchiveStatus::DOING,
                     dst,
                     *sql_proxy_))) {
        LOG_WARN("failed to update tenant inner table current info", KR(ret), K(tenant_id), K(round));
      } else {
        LOG_INFO("set tenant need fetch new", K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_cur_tenant_task_is_finished(
    const int64_t round, const uint64_t tenant_id, bool& finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfoMgr bb_mgr;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfo bb_info;
  const int64_t copy_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.get_log_archive_history_info(
                 *sql_proxy_, tenant_id, round, copy_id, false /*for_udpate*/, info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(round));
    }
  } else if (OB_FAIL(bb_mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup");
  } else if (OB_FAIL(
                 bb_mgr.get_log_archive_backup_backup_info(*sql_proxy_, false /*for_update*/, tenant_id, bb_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get log archive backup backup info", KR(ret), K(tenant_id));
    }
  } else {
    finished = info.status_.checkpoint_ts_ == bb_info.status_.checkpoint_ts_;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_response_checkpoint_ts_valid(
    const uint64_t tenant_id, const int64_t checkpoint_ts, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  int64_t cur_checkpoint_ts = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || checkpoint_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check response checkpoint ts valid get invalid argument", KR(ret), K(tenant_id), K(checkpoint_ts));
  } else if (OB_FAIL(get_checkpoint_ts(tenant_id, cur_checkpoint_ts))) {
    LOG_WARN("failed to get current checkpoint ts", KR(ret), K(tenant_id));
  } else if (cur_checkpoint_ts > checkpoint_ts) {
    is_valid = false;
    LOG_INFO("may has switched rs", K(tenant_id), K(cur_checkpoint_ts), K(checkpoint_ts));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_round_interrupted(const int64_t round)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  const int64_t current_round = round_stat_.get_current_round();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (round < current_round) {
    LOG_WARN("the rpc reply is too old", K(round), K(current_round));
  } else if (round > current_round) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current round is not same", K(round), K(current_round));
  } else if (OB_FAIL(round_stat_.set_interrupted())) {
    LOG_WARN("failed to set round interrupted", KR(ret));
  } else if (OB_FAIL(update_inner_table_log_archive_status(ObLogArchiveStatus::INTERRUPTED))) {
    LOG_WARN("failed to update inner table log archive status to interrupted", KR(ret));
  } else {
    // do not sleep if current round is interrupted
    wakeup();
  }
  return ret;
}

int ObBackupArchiveLogScheduler::mark_piece_pg_task_finished(const uint64_t tenant_id, const int64_t piece_id,
    const int64_t job_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  TenantTaskQueue* queue_ptr = NULL;
  const bool finished = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else if (piece_id != queue_ptr->piece_id_ || job_id != queue_ptr->job_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece id do not match", KR(ret), K(piece_id), K(job_id), KP(queue_ptr), K(*queue_ptr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const ObPGKey& pkey = pg_list.at(i);
      const int overwrite_flag = 1;
      CurrentTurnPGStat stat;
      hash_ret = queue_ptr->pg_map_.get_refactored(pkey, stat);
      if (hash_ret == OB_HASH_NOT_EXIST) {
        CurrentTurnPGStat new_stat;
        new_stat.finished_ = finished;
        if (OB_FAIL(queue_ptr->pg_map_.set_refactored(pkey, new_stat, overwrite_flag))) {
          LOG_WARN("failed to set map", KR(ret), K(pkey), K(new_stat));
        }
      } else if (hash_ret == OB_SUCCESS) {
        stat.finished_ = finished;
        if (OB_FAIL(queue_ptr->pg_map_.set_refactored(pkey, stat, overwrite_flag))) {
          LOG_WARN("failed to set map", KR(ret), K(tenant_id), K(pkey));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("some thing unexpected happened", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::redo_failed_pg_tasks(const uint64_t tenant_id, const int64_t piece_id,
    const int64_t job_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (pg_list.empty()) {
    LOG_INFO("do nothing if pg list empty", K(tenant_id));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else if (piece_id != queue_ptr->piece_id_ || job_id != queue_ptr->job_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece id do not match", KR(ret), K(piece_id), K(job_id), KP(queue_ptr), K(*queue_ptr));
  } else if (OB_FAIL(rethrow_pg_back_to_queue(pg_list, queue_ptr))) {
    LOG_WARN("failed to rethrow pg back to queue", KR(ret));
  }
  return ret;
}

void ObBackupArchiveLogScheduler::clear_memory_resource_if_stopped()
{
  int tmp_ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  typedef common::hash::ObHashMap<uint64_t, TenantTaskQueue*>::iterator Iter;
  Iter iter = tenant_queue_map_.begin();
  for (; iter != tenant_queue_map_.end(); ++iter) {
    TenantTaskQueue* queue_ptr = iter->second;
    if (OB_NOT_NULL(queue_ptr)) {
      if (queue_ptr->pg_map_.created()) {
        if (OB_SUCCESS != (tmp_ret = queue_ptr->pg_map_.destroy())) {
          LOG_WARN("failed to reuse pg map", KR(tmp_ret));
        }
      }
      while (!queue_ptr->queue_.is_empty()) {
        ObLink* link = NULL;
        SimplePGWrapper* pg = NULL;
        if (OB_SUCCESS != (tmp_ret = queue_ptr->queue_.pop(link))) {
          LOG_WARN("failed to pop task", KR(tmp_ret), K(link));
        } else if (OB_ISNULL(link)) {
          LOG_WARN("link is null", K(link));
        } else {
          pg = static_cast<SimplePGWrapper*>(link);
          free_pg_task(pg);
        }
      }
      free_tenant_queue(queue_ptr);
    }
  }
  tenant_queue_map_.clear();
  LOG_INFO("[BACKUP BACKUP] clear momory resource if stopped");
}

int ObBackupArchiveLogScheduler::check_can_do_work()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", KR(ret));
  } else if (is_valid_backup_inner_table_version(inner_table_version_) &&
             inner_table_version_ >= OB_BACKUP_INNER_TABLE_V3) {
    // inner table version is new enough
  } else if (OB_FAIL(ObBackupInfoOperator::get_inner_table_version(*sql_proxy_, inner_table_version_))) {
    LOG_WARN("Failed to get inner table version", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V3) {
    ret = OB_EAGAIN;
    LOG_INFO("inner table version is too old, waiting backup inner table upgrade", K(ret), K(inner_table_version_));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::handle_enable_auto_backup(const bool enable_backup)
{
  int ret = OB_SUCCESS;
  bool is_switch_piece = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(check_is_switch_piece(is_switch_piece))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check is switch piece", KR(ret));
    }
  } else if (enable_backup && is_switch_piece) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support auto backup in switch piece mode", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto backup in switch piece mode");
  } else if (!enable_backup) {
    if (OB_FAIL(cancel_all_outgoing_tasks())) {
      LOG_WARN("failed to cancel all outgoing tasks", KR(ret));
    } else if (OB_FAIL(update_inner_table_log_archive_status(ObLogArchiveStatus::PAUSED))) {
      LOG_WARN("failed to update inner table log archive status to paused", KR(ret));
    } else if (OB_FAIL(set_paused(true))) {
      LOG_WARN("failed to set paused", KR(ret));
    } else {
      LOG_INFO("set log archive status to pause");
    }
  } else if (enable_backup) {
    if (is_paused()) {
      if (OB_FAIL(update_inner_table_log_archive_status(ObLogArchiveStatus::DOING))) {
        LOG_WARN("failed to update inner table log archive status to doing", KR(ret));
      } else if (OB_FAIL(set_paused(false))) {
        LOG_WARN("failed to set paused");
      } else {
        LOG_INFO("set log archive status to doing");
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_is_enable_auto_backup(bool& can_do_work)
{
  int ret = OB_SUCCESS;
  bool enable_backup = false;
  ObBackupInfoManager manager;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", KR(ret));
  } else if (OB_FAIL(manager.get_enable_auto_backup_archivelog(OB_SYS_TENANT_ID, *sql_proxy_, enable_backup))) {
    LOG_WARN("failed to update enable auto backup archivelog", KR(ret));
  } else {
    can_do_work = enable_backup;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_inner_table_log_archive_status(const share::ObLogArchiveStatus::STATUS& status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArray<uint64_t> tenant_ids;
  ObLogArchiveBackupInfoMgr mgr;
  const bool for_update = true;
  const int64_t round = round_stat_.get_current_round();
  ;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (status != ObLogArchiveStatus::DOING && status != ObLogArchiveStatus::PAUSED &&
             status != ObLogArchiveStatus::STOP && status != ObLogArchiveStatus::INTERRUPTED) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log archive status should only be doing, paused, stop, interrupted", KR(ret), K(status));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2 ||
             !is_valid_backup_inner_table_version(inner_table_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inner_table_version_", K(ret), K(inner_table_version_));
  } else if (OB_FAIL(get_all_tenant_ids(round, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set copy id", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t status_count = 0;
      ObLogArchiveBackupInfo info;
      if (OB_FAIL(get_status_line_count(tenant_id, status_count))) {
        LOG_WARN("failed to get status line count", KR(ret), K(tenant_id));
      } else if (0 == status_count) {
        // do nothing
      } else if (OB_FAIL(mgr.get_log_archive_backup_backup_info(trans, for_update, tenant_id, info))) {
        LOG_WARN("failed to get log archive backup info", KR(ret), K(for_update), K(tenant_id));
      } else if (FALSE_IT(info.status_.status_ = status)) {
        // set log archive status
      } else if (OB_FAIL(mgr.update_log_archive_backup_info(trans, inner_table_version_, info))) {
        LOG_WARN("failed to update log archive backup info", KR(ret), K(info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_status_line_count(const uint64_t tenant_id, int64_t& status_count)
{
  int ret = OB_SUCCESS;
  status_count = 0;
  ObSqlString sql;
  sqlclient::ObMySQLResult* result = NULL;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id=%lu ",
            OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME,
            tenant_id))) {
      LOG_WARN("failed to init backup info sql", KR(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {  // sys tenant
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", KR(ret), K(sql));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", status_count, int64_t);
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_info(const uint64_t tenant_id, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  int64_t status_line_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_status_line_count(tenant_id, status_line_count))) {
    LOG_WARN("failed to get status line count", KR(ret), K(tenant_id));
  } else if (status_line_count > 0) {
    // do nothing
  } else if (OB_FAIL(insert_backup_archive_info(tenant_id, dst))) {
    LOG_WARN("failed to insert backup archive info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_infos(
    const common::ObArray<uint64_t>& tenant_list, const share::ObBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const uint64_t tenant_id = tenant_list.at(i);
      if (OB_FAIL(check_backup_info(tenant_id, backup_dest))) {
        LOG_WARN("failed to check backup info", KR(ret), K(tenant_id), K(backup_dest));
      }
    }
  }
  DEBUG_SYNC(AFTER_PREPARE_TENANT_BACKUP_BACKUP_BEGINNING);
  return ret;
}

int ObBackupArchiveLogScheduler::insert_backup_archive_info(const uint64_t tenant_id, const share::ObBackupDest& dest)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  int64_t copy_id = 0;
  int64_t round_id = round_stat_.get_current_round();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_current_round_copy_id(dest, tenant_id, round_id, copy_id))) {
    LOG_WARN("failed to get current round copy id", KR(ret), K(dest), K(tenant_id), K(round_id));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s)"
                                    " VALUES (%lu, %ld, %ld, %ld, usec_to_time(0), usec_to_time(0), 'BEGINNING')",
                 OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME,
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_COPY_ID,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_STATUS,
                 tenant_id,
                 OB_START_INCARNATION,
                 round_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(round_id), K(copy_id));
  } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {  // write to sys tenant id
    LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_backup_dest_info(
    const uint64_t tenant_id, const int64_t round, ObBackupDest& src, ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  char dst_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(get_src_backup_dest(tenant_id, round, src))) {
    LOG_WARN("failed to get backup dest", KR(ret), K(tenant_id), K(round));
  } else if (OB_FAIL(get_dst_backup_dest(tenant_id, dst_backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to set backup backup dest if needed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dst.set(dst_backup_dest))) {
    LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest));
  } else if (src.is_root_path_equal(dst)) {
    ret = OB_BACKUP_BACKUP_CAN_NOT_START;
    LOG_WARN("backup dest and backup backup dest is same, can not start backup", KR(ret), K(src), K(dst));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_extern_backup_info(
    const share::ObBackupDest& src, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObClusterBackupDest src_dest, dst_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_dest.set(src, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret));
  } else if (OB_FAIL(dst_dest.set(dst, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret));
  } else if (OB_FAIL(update_tenant_info(src_dest, dst_dest))) {
    LOG_WARN("failed to update tenant info", KR(ret), K(src_dest), K(dst_dest));
  } else if (OB_FAIL(update_tenant_name_info(src_dest, dst_dest))) {
    LOG_WARN("failed to update tenant name info", KR(ret), K(src_dest), K(dst_dest));
  } else if (OB_FAIL(update_clog_info_list(src_dest, dst_dest))) {
    LOG_WARN("failed to update clog info list", KR(ret), K(src_dest), K(dst_dest));
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_time;
  LOG_INFO("update extern backup info cost time", K(cost_ts));
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_info(
    const share::ObClusterBackupDest& src_dest, const share::ObClusterBackupDest& dst_dest)
{
  int ret = OB_SUCCESS;
  ObExternTenantInfoMgr src_mgr, dst_mgr;
  ObArray<ObExternTenantInfo> tenant_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.init(src_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern tenant info mgr", KR(ret), K(src_dest));
  } else if (OB_FAIL(dst_mgr.init(dst_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern tenant info mgr", KR(ret), K(dst_dest));
  } else if (OB_FAIL(src_mgr.get_extern_tenant_infos(tenant_infos))) {
    LOG_WARN("failed to get extern tenant infos", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_infos.count(); ++i) {
      const ObExternTenantInfo& tenant_info = tenant_infos.at(i);
      if (OB_FAIL(dst_mgr.add_tenant_info(tenant_info))) {
        LOG_WARN("failed to add tenant info", KR(ret), K(tenant_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dst_mgr.upload_tenant_infos())) {
        LOG_WARN("failed to upload tenant infos", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_name_info(
    const share::ObClusterBackupDest& src_dest, const share::ObClusterBackupDest& dst_dest)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = 0;
  ObTenantNameSimpleMgr tenant_name_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(tenant_name_mgr.init())) {
    LOG_WARN("failed to init tenant name simple mgr", KR(ret));
  } else if (OB_FAIL(tenant_name_mgr.read_backup_file(src_dest))) {
    LOG_WARN("failed to read backup file", KR(ret), K(src_dest));
  } else if (FALSE_IT(schema_version = tenant_name_mgr.get_schema_version())) {
    // do nothing
  } else if (OB_FAIL(tenant_name_mgr.complete(schema_version))) {
    LOG_WARN("failed to complete schema version", KR(ret), K(schema_version));
  } else if (OB_FAIL(tenant_name_mgr.write_backup_file(dst_dest))) {
    LOG_WARN("failed to write backup file", KR(ret), K(dst_dest));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_clog_info_list(
    const share::ObClusterBackupDest& src_dest, const share::ObClusterBackupDest& dst_dest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObStorageUtil util(true /*need retry*/);
  ObBackupPath src_path, dst_path;
  ObArray<ObString> file_names;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info(src_dest, src_path))) {
    LOG_WARN("failed to get src cluster clog info path", KR(ret), K(src_dest));
  } else if (OB_FAIL(util.list_files(src_path.get_obstr(), src_dest.get_storage_info(), allocator, file_names))) {
    LOG_WARN("failed to list files", KR(ret), K(src_path), K(src_dest));
  } else if (file_names.empty()) {
    LOG_INFO("clog info list is empty", K(file_names));
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info(dst_dest, dst_path))) {
    LOG_WARN("failed to get dst cluster clog info path", KR(ret), K(dst_dest));
  } else if (OB_FAIL(util.mkdir(dst_path.get_obstr(), dst_dest.get_storage_info()))) {
    LOG_WARN("failed to make clog info directory", KR(ret), K(dst_path), K(dst_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
      const ObString& file_name = file_names.at(i);
      ObBackupPath src_path, dst_path;
      int64_t file_length = 0;
      int64_t read_length = 0;
      char* buf = NULL;
      bool is_tmp_file = false;
      if (OB_FAIL(ObBackupUtils::check_is_tmp_file(file_name, is_tmp_file))) {
        LOG_WARN("failed to check is tmp file", KR(ret), K(file_name));
      } else if (is_tmp_file) {
        // do nothing if is tmp file
      } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info_file_path(src_dest, file_name, src_path))) {
        LOG_WARN("failed to get src cluster clog info file path", KR(ret), K(src_dest));
      } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info_file_path(dst_dest, file_name, dst_path))) {
        LOG_WARN("failed to get dst clust4er clog info file path", KR(ret), K(dst_dest));
      } else if (OB_FAIL(util.get_file_length(src_path.get_obstr(), src_dest.get_storage_info(), file_length))) {
        LOG_WARN("failled to get file length", KR(ret), K(src_path), K(src_dest));
      } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(util.read_single_file(
                     src_path.get_obstr(), src_dest.get_storage_info(), buf, file_length, read_length))) {
        LOG_WARN("failed to read single file", KR(ret), K(src_path), K(src_dest));
      } else if (read_length != file_length) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read len not expected", KR(ret), K(read_length), K(file_length));
      } else if (OB_FAIL(util.write_single_file(dst_path.get_obstr(), dst_dest.get_storage_info(), buf, file_length))) {
        LOG_WARN("failed to write single file", KR(ret), K(dst_path), K(dst_dest));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_extern_tenant_clog_backup_info(const uint64_t tenant_id, const int64_t round_id,
    const int64_t piece_id, const int64_t copy_id, const int64_t checkpoint_ts, const share::ObBackupDest& dst,
    const share::ObLogArchiveStatus::STATUS& status)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfo info;
  info.status_.tenant_id_ = tenant_id;
  info.status_.checkpoint_ts_ = checkpoint_ts;
  info.status_.incarnation_ = OB_START_INCARNATION;
  info.status_.round_ = round_id;
  info.status_.copy_id_ = copy_id;
  info.status_.status_ = status;
  info.status_.is_mount_file_created_ = true;
  info.status_.backup_piece_id_ = piece_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant clog backup info get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_round_start_ts(tenant_id, round_id, info.status_.start_ts_))) {
    LOG_WARN("failed to get round start ts", KR(ret), K(tenant_id), K(round_id));
  } else if (OB_FAIL(dst.get_backup_dest_str(info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(dst));
  } else if (OB_FAIL(mgr.update_extern_log_archive_backup_info(info, *backup_lease_service_))) {
    LOG_WARN("failed to update extern log archive backup info", K(ret), K(info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_inner_table_cur_info(const uint64_t tenant_id,
    const int64_t archive_round, const int64_t checkpoint_ts, const int64_t copy_id, const int64_t backup_piece_id,
    const ObLogArchiveStatus::STATUS& status, const share::ObBackupDest& dst, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo backup_info;
  ObLogArchiveBackupInfoMgr dst_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V2 ||
             !is_valid_backup_inner_table_version(inner_table_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inner_table_version_", K(ret), K(inner_table_version_));
  } else if (copy_id > 0 && OB_FAIL(dst_mgr.set_backup_backup())) {
    LOG_WARN("failed to set copy id for dst mgr", KR(ret));
  } else if (OB_FAIL(get_log_archive_round_info(tenant_id, archive_round, backup_info))) {
    LOG_WARN("failed to get log archive round info", KR(ret), K(tenant_id), K(archive_round));
  } else if (archive_round != backup_info.status_.round_) {
    // do nothing
  } else if (FALSE_IT(backup_info.status_.checkpoint_ts_ = checkpoint_ts)) {
    // set checkpoint ts
  } else if (FALSE_IT(backup_info.status_.copy_id_ = copy_id)) {
    // set copy id
  } else if (FALSE_IT(backup_info.status_.status_ = status)) {
    // set doing
  } else if (FALSE_IT(backup_info.status_.backup_piece_id_ = backup_piece_id)) {
    // set backup_piece_id
  } else if (OB_FAIL(dst.get_backup_dest_str(backup_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_info));
  } else if (OB_FAIL(dst_mgr.update_log_archive_backup_info(sql_proxy, inner_table_version_, backup_info))) {
    LOG_WARN("failed to update log archive backup info", KR(ret), K(backup_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_inner_table_his_info(const uint64_t tenant_id,
    const int64_t archive_round, const int64_t copy_id, const share::ObBackupDest& dst, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t original_copy_id = 0;
  const bool for_update = false;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfoMgr src_mgr, dst_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(dst_mgr.set_backup_backup())) {
    LOG_WARN("failed to set copy id for dst mgr", KR(ret));
  } else if (OB_FAIL(src_mgr.get_log_archive_history_info(
                 sql_proxy, tenant_id, archive_round, original_copy_id, for_update, info))) {
    LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(archive_round));
  } else if (FALSE_IT(info.status_.copy_id_ = copy_id)) {
    // do nothing
  } else if (OB_FAIL(dst.get_backup_dest_str(info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(info));
  } else if (OB_FAIL(dst_mgr.update_log_archive_status_history(
                 sql_proxy, info, inner_table_version_, *backup_lease_service_))) {
    LOG_WARN("failed to update log archive status history", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_all_tenant_ids(
    const int64_t log_archive_round, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  bool in_history = false;
  const bool for_update = false;
  ObLogArchiveBackupInfoMgr mgr;
  schema::ObSchemaGetterGuard schema_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (log_archive_round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(log_archive_round));
  } else if (OB_FAIL(check_is_history_round(log_archive_round, in_history))) {
    LOG_WARN("failed to check is history round", KR(ret), K(log_archive_round));
  } else {
    ObArray<ObLogArchiveBackupInfo> info_list;
    if (in_history) {
      if (OB_FAIL(
              mgr.get_same_round_log_archive_history_infos(*sql_proxy_, log_archive_round, for_update, info_list))) {
        LOG_WARN("failed to get same round log archive history infos", KR(ret));
      }
    } else {
      ObArray<ObLogArchiveBackupInfo> cur_info_list;
      ObArray<ObLogArchiveBackupInfo> his_info_list;
      if (OB_FAIL(
              mgr.get_same_round_log_archive_history_infos(*sql_proxy_, log_archive_round, for_update, info_list))) {
        LOG_WARN("failed to get same round log archive history infos", KR(ret));
      } else if (OB_FAIL(
                     mgr.get_all_same_round_in_progress_backup_info(*sql_proxy_, log_archive_round, his_info_list))) {
        LOG_WARN("failed to get_all_same_round_in_progress_backup_info", KR(ret), K(round));
      } else if (OB_FAIL(append(info_list, cur_info_list))) {
        LOG_WARN("failed to add array", KR(ret));
      } else if (OB_FAIL(append(info_list, his_info_list))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObLogArchiveBackupInfo& info = info_list.at(i);
      const uint64_t tenant_id = info.status_.tenant_id_;
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push back tenant id", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::gc_unused_tenant_resources(const int64_t round_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_list;
  ObArray<uint64_t> unused_tenant_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(round_id));
  } else if (OB_FAIL(get_all_tenant_ids(round_id, tenant_list))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round_id));
  } else {
    typedef common::hash::ObHashMap<uint64_t, TenantTaskQueue*>::const_iterator Iterator;
    Iterator iter;
    for (; OB_SUCC(ret) && iter != tenant_queue_map_.end(); ++iter) {
      const uint64_t tenant_id = iter->first;
      bool exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
        if (tenant_id == tenant_list.at(i)) {
          exist = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !exist) {
        if (OB_FAIL(unused_tenant_list.push_back(tenant_id))) {
          LOG_WARN("failed to push back tenant id", KR(ret), K(tenant_id));
        }
      }
    }
    LOG_INFO("gc unused tenant resources", K(round_id), K(unused_tenant_list));
    for (int64_t i = 0; OB_SUCC(ret) && i < unused_tenant_list.count(); ++i) {
      const uint64_t tenant_id = unused_tenant_list.at(i);
      if (OB_FAIL(do_clear_tenant_resource_if_dropped(tenant_id))) {
        LOG_WARN("failed to do clear tenant resource if dropped", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped)
{
  int ret = OB_SUCCESS;
  is_dropped = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("failed to check if tenant has been dropped", KR(ret), K(tenant_id));
  }
  return ret;
}

// 为了防止在出错情况下总是第一个租户被调度，引入rebalance策略，
// 每次数组作一个shift
// 第1轮： 1001，1002，1003
// 第2轮： 1002，1003，1001
// 第3轮： 1003，1002，1001
int ObBackupArchiveLogScheduler::rebalance_all_tenants(const int64_t shift_num, common::ObArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  int64_t shift = shift_num % tenant_ids.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no tenants to rebalance", KR(ret));
  } else if (tenant_ids.count() <= 1) {
    LOG_INFO("no need to rebalance tenants if only one", K(tenant_ids));
  } else {
    std::rotate(tenant_ids.begin(), tenant_ids.begin() + shift, tenant_ids.end());
    std::rotate(tenant_ids.begin() + shift, tenant_ids.end(), tenant_ids.end());
    std::rotate(tenant_ids.begin(), tenant_ids.end(), tenant_ids.end());
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_src_backup_dest(
    const uint64_t tenant_id, const int64_t round, share::ObBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  backup_dest.reset();
  bool for_update = false;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfoMgr archive_mgr;
  const int64_t copy_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round is not valid", KR(ret));
  } else if (OB_FAIL(archive_mgr.get_log_archive_backup_info(
                 *sql_proxy_, for_update, tenant_id, inner_table_version_, info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret), K(for_update), K(tenant_id));
  } else {
    if (info.status_.round_ == round) {
      if (OB_FAIL(backup_dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(info));
      }
    } else {
      if (OB_FAIL(archive_mgr.get_log_archive_history_info(*sql_proxy_, tenant_id, round, copy_id, for_update, info))) {
        LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(round), K(info));
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_BACKUP_BACKUP_CAN_NOT_START;
          LOG_WARN("no such archive round, can not start backup backup", KR(ret), K(tenant_id), K(round));
        }
      } else if (OB_FAIL(backup_dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(info));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_current_log_archive_round_info(
    const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();
  const bool for_update = false;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, inner_table_version_, info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_history_info(
    const uint64_t tenant_id, const int64_t round, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  bool for_update = false;
  ObLogArchiveBackupInfoMgr mgr;
  const int64_t copy_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.get_log_archive_history_info(*sql_proxy_, tenant_id, round, copy_id, for_update, info))) {
    LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::prepare_tenant_if_needed(
    const ObIArray<uint64_t>& tenant_ids, const bool need_fetch_new)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (tenant_ids.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      TenantTaskQueue* queue_ptr = NULL;
      int hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(alloc_tenant_queue(tenant_id, queue_ptr))) {
          LOG_WARN("failed to alloc tenant task queue", KR(ret), K(tenant_id));
        } else {
          queue_ptr->need_fetch_new_ = need_fetch_new;
          if (OB_FAIL(queue_ptr->pg_map_.create(MAX_BUCKET_NUM, "BB_CLOG_TENANT", "BB_CLOG_NODE"))) {
            LOG_WARN("failed to create pg map", KR(ret));
          } else if (OB_FAIL(tenant_queue_map_.set_refactored(tenant_id, queue_ptr))) {
            LOG_WARN("failed to set map", KR(ret), K(tenant_id));
          }
        }
      } else if (OB_SUCCESS == hash_ret) {
        queue_ptr->need_fetch_new_ = need_fetch_new;
        if (queue_ptr->pg_map_.created()) {
          // do nothing
        } else if (OB_FAIL(queue_ptr->pg_map_.create(MAX_BUCKET_NUM, "BB_CLOG_TENANT", "BB_CLOG_NODE"))) {
          LOG_WARN("failed to create pg map", KR(ret));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("get refactored error", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::prepare_server_if_needed(const common::ObIArray<common::ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (servers.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr& addr = servers.at(i);
      ServerStatus status;
      int hash_ret = server_status_map_.get_refactored(addr, status);
      if (OB_SUCCESS == hash_ret) {
        continue;
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        status.status_ = ServerStatus::STATUS_FREE;
        status.lease_time_ = ObTimeUtil::current_time();
        if (OB_FAIL(server_status_map_.set_refactored(addr, status))) {
          LOG_WARN("failed to set server status", KR(ret), K(addr));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get refactored error", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_schedule(
    const int64_t round, const share::ObBackupDest& src, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  bool all_tenant_passed_checkpoint = true;  // 所有租户都推过了某一个checkpoint ts
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("root service is stopped", KR(ret));
  } else if (OB_FAIL(get_all_tenant_ids(round, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round));
  } else if (OB_FAIL(prepare_tenant_if_needed(tenant_ids, true))) {
    LOG_WARN("failed to prepare tenant if needed", KR(ret), K(tenant_ids));
  } else if (OB_FAIL(rebalance_all_tenants(schedule_round_++, tenant_ids))) {
    LOG_WARN("failed to rebalance all tenants", KR(ret), K(tenant_ids));
  } else if (OB_FAIL(check_backup_infos(tenant_ids, dst))) {
    LOG_WARN("failed to check backup infos", KR(ret), K(tenant_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(do_if_pg_task_finished(round, tenant_id, dst))) {
        LOG_WARN("failed to do set tenant need fetch new", KR(ret), K(round), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_round_finished(round, tenant_ids))) {
        LOG_WARN("failed to check round finished", KR(ret), K(round), K(tenant_ids));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t tenant_next_checkpoint_ts = 0;
      bool cur_finished = false;
      bool normal_passed_sys = false;
      bool is_beginning = false;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(backup_lease_service_->check_lease())) {
        LOG_WARN("failed to check can backup", KR(ret));
      } else if (OB_FAIL(check_archive_beginning(tenant_id, is_beginning))) {
        LOG_WARN("failed to check archive beginning", KR(ret), K(tenant_id));
      } else if (is_beginning) {
        all_tenant_passed_checkpoint = false;
        LOG_INFO("origin tenant is still beginning, do later", K(tenant_id));
        continue;
      } else if (OB_FAIL(get_checkpoint_ts_with_lock(tenant_id, tenant_next_checkpoint_ts))) {
        LOG_WARN("failed to get checkpoint ts from inner table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(check_cur_tenant_task_is_finished(round, tenant_id, cur_finished))) {
        LOG_WARN("failed to check cur tenant task is finished", KR(ret), K(round), K(tenant_id));
      } else if (cur_finished) {
        bool is_dropped = false;
        if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
          LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
        } else if (!is_dropped) {
          // do nothing
        } else if (OB_FAIL(update_inner_and_extern_tenant_info_to_stop(dst, tenant_id, round))) {
          LOG_WARN("failed to update inner and extern tenant info", KR(ret));
        } else if (OB_FAIL(update_round_piece_info_for_clean(
                       tenant_id, round, true /*round_finished*/, dst, *sql_proxy_))) {
          LOG_WARN("failed to update round piece info for clean",
              KR(ret),
              K(tenant_id),
              K(round),
              K(tenant_next_checkpoint_ts),
              K(dst));
        } else {
          LOG_INFO("tenant is finished, may be dropped", K(round), K(tenant_id), K(tenant_next_checkpoint_ts));
        }

        continue;
      } else if (0 != tenant_next_checkpoint_ts &&
                 OB_FAIL(check_normal_tenant_passed_sys_tenant(
                     tenant_id, local_sys_tenant_checkpoint_ts_, normal_passed_sys))) {
        LOG_WARN("failed to check normal tenant passed sys tenant", KR(ret), K(tenant_id));
      } else if (normal_passed_sys) {
        LOG_INFO("current tenant checkpoint ts has passed sys checkpoint ts", K(tenant_id));
        continue;
      } else if (OB_FAIL(
                     do_tenant_schedule(tenant_id, round, 0 /*piece_id*/, 0 /*create_date*/, 0 /*job_id*/, src, dst))) {
        if (OB_EAGAIN == ret) {
          LOG_INFO("no server currently available", KR(ret), K(tenant_id));
          all_tenant_passed_checkpoint = false;
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(round_stat_.set_scheduled(round))) {
        LOG_WARN("failed to set scheduled", KR(ret), K(round));
      }
      all_tenant_passed_checkpoint = false;
    }

    if (OB_SUCC(ret)) {
      if (all_tenant_passed_checkpoint && !round_stat_.is_interrupted()) {
        int64_t new_sys_checkpoint_ts = 0;
        ObBackupDest dst;
        char dst_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
        if (OB_FAIL(get_log_archive_round_checkpoint_ts(OB_SYS_TENANT_ID, round, new_sys_checkpoint_ts))) {
          LOG_WARN("failed to get checkpoint ts from inner table");
        } else if (OB_FAIL(update_sys_tenant_checkpoint_ts())) {
          LOG_WARN("failed to update sys tenant checkpoint ts", KR(ret));
        } else if (OB_FAIL(get_dst_backup_dest(OB_SYS_TENANT_ID, dst_backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
          LOG_WARN("failed to set backup backup dest if needed", KR(ret));
        } else if (OB_FAIL(dst.set(dst_backup_dest))) {
          LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest));
        } else if (OB_FAIL(update_round_piece_info_for_clean(
                       OB_SYS_TENANT_ID, round, false /*round_finished*/, dst, *sql_proxy_))) {
          LOG_WARN("failed to update round piece info for clean", KR(ret), K(round));
        }

        if (OB_SUCC(ret)) {
          if (new_sys_checkpoint_ts == local_sys_tenant_checkpoint_ts_) {
            LOG_INFO("need ensure all tenant finished", K(round), K(new_sys_checkpoint_ts));
            if (OB_FAIL(ensure_all_tenant_finish(round, src, dst, tenant_ids))) {
              LOG_WARN("failed to ensure all tenant finish", KR(ret), K(tenant_ids), K(round));
            }
          }
          local_sys_tenant_checkpoint_ts_ = new_sys_checkpoint_ts;
          LOG_INFO("new local sys tenant checkpoint ts is", K(local_sys_tenant_checkpoint_ts_));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_if_round_finished(const int64_t round, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_finished = false;
  bool is_interrupted = false;
  ObArray<uint64_t> tenant_list;
  ObLogArchiveBackupInfoMgr mgr;
  ObMySQLTransaction trans;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup", KR(ret));
  } else if (OB_FAIL(round_stat_.check_round_finished(round, is_finished, is_interrupted))) {
    LOG_WARN("failed to check round finished", KR(ret), K(round));
  } else if (!is_finished && !is_interrupted) {
    LOG_INFO("round is not finished", K(round), K(is_finished), K(is_interrupted));
  } else if (OB_FAIL(get_all_tenant_ids(round, tenant_list))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round));
  } else {
    std::sort(tenant_list.begin(), tenant_list.end());
    for (int64_t i = tenant_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const int64_t tenant_id = tenant_list.at(i);
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(update_round_piece_info_for_clean(tenant_id, round, true /*round_finished*/, dst, trans))) {
          LOG_WARN("failed to update round piece info for clean", KR(ret), K(tenant_id), K(round), K(dst));
        } else if (OB_FAIL(update_inner_and_extern_tenant_info(dst, tenant_id, round, is_interrupted, trans))) {
          LOG_WARN("failed to update inner and extern tenant infos", KR(ret), K(tenant_id), K(dst), K(round));
        } else if (OB_FAIL(mgr.delete_backup_backup_log_archive_info(tenant_id, trans))) {
          LOG_WARN("failed to delete backup backup log archive info", KR(ret), K(tenant_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("failed to commit trans", KR(ret));
          }
        } else {
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      clear_memory_resource_if_stopped();
      LOG_INFO("delete all backup backup log archive info", K(round));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::ensure_all_tenant_finish(const int64_t round, const share::ObBackupDest& src,
    const share::ObBackupDest& dst, const common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (round <= 0 || !src.is_valid() || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(round), K(src), K(dst));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(backup_lease_service_->check_lease())) {
        LOG_WARN("failed to check can backup", KR(ret));
      } else if (OB_FAIL(
                     do_tenant_schedule(tenant_id, round, 0 /*piece_id*/, 0 /*create_date*/, 0 /*job_id*/, src, dst))) {
        if (OB_EAGAIN == ret) {
          LOG_INFO("no server currently available", KR(ret), K(tenant_id));
          ret = OB_SUCCESS;
          break;
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_next_round(const common::ObArray<int64_t>& round_list, int64_t& next_round)
{
  int ret = OB_SUCCESS;
  int64_t old_round = round_stat_.get_current_round();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round_list.empty()) {
    next_round = old_round;
  } else if (OB_FAIL(inner_get_next_round_with_lock(round_list, next_round))) {
    LOG_WARN("failed to get next round", KR(ret), K(round_list));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_round_finished(const int64_t round, const common::ObArray<uint64_t>& tenant_list)
{
  int ret = OB_SUCCESS;
  bool is_history = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(round));
  } else if (OB_FAIL(check_is_history_round(round, is_history))) {
    LOG_WARN("failed to check is history round", KR(ret), K(round));
  } else if (!is_history) {
    // do nothing
  } else {
    bool all_finished = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const uint64_t tenant_id = tenant_list.at(i);
      bool tenant_finished = false;
      if (OB_FAIL(check_cur_tenant_task_is_finished(round, tenant_id, tenant_finished))) {
        LOG_WARN("failed to check cur tenant task is finished", KR(ret), K(round), K(tenant_id));
      } else if (!tenant_finished) {
        all_finished = false;
        LOG_DEBUG("tenant not finished", K(round), K(tenant_id));
        break;
      }
    }
    if (OB_SUCC(ret) && all_finished) {
      if (OB_FAIL(round_stat_.set_mark_finished())) {
        LOG_WARN("failed to set finish", K(round), K(tenant_list));
      } else {
        LOG_INFO("mark current round finished", K(round), K(tenant_list));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_tenant_task_stopped(const uint64_t tenant_id, bool& is_stopped)
{
  int ret = OB_SUCCESS;
  is_stopped = false;
  ObLogArchiveBackupInfo bb_info;
  ObLogArchiveBackupInfoMgr bb_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(bb_mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup");
  } else if (OB_FAIL(
                 bb_mgr.get_log_archive_backup_backup_info(*sql_proxy_, false /*for_update*/, tenant_id, bb_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get log archive backup backup info", KR(ret), K(tenant_id));
    }
  } else {
    is_stopped = ObLogArchiveStatus::STOP == bb_info.status_.status_;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_inner_and_extern_tenant_info_to_stop(
    const share::ObBackupDest& backup_dest, const uint64_t tenant_id, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_rollback = false;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfo bb_info;
  ObLogArchiveBackupInfoMgr bb_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (!backup_dest.is_valid() || OB_INVALID_ID == tenant_id || round_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(bb_mgr.set_backup_backup())) {
      LOG_WARN("failed to set backup backup");
    } else if (OB_FAIL(bb_mgr.get_log_archive_backup_backup_info(trans, true /*for_update*/, tenant_id, bb_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get log archive backup backup info", KR(ret), K(tenant_id));
      }
    } else if (ObLogArchiveStatus::STOP == bb_info.status_.status_) {
      need_rollback = true;
      // already stop, no need update
    } else if (bb_info.status_.round_ != round_id) {
      // do nothing
    } else if (OB_FAIL(update_extern_tenant_clog_backup_info(tenant_id,
                   round_id,
                   0 /*piece_id*/,
                   bb_info.status_.copy_id_,
                   bb_info.status_.checkpoint_ts_,
                   backup_dest,
                   ObLogArchiveStatus::STOP))) {
      LOG_WARN("failed to update tenant clog backup info", KR(ret), K(tenant_id), K(round_id));
    } else if (OB_FAIL(update_tenant_inner_table_cur_info(tenant_id,
                   round_id,
                   bb_info.status_.checkpoint_ts_,
                   bb_info.status_.copy_id_,
                   0 /*backup_piece_id*/,
                   ObLogArchiveStatus::STOP,
                   backup_dest,
                   trans))) {
      LOG_WARN("failed to update tenant inner table current info", KR(ret), K(tenant_id), K(round_id));
    }
    if (OB_SUCC(ret) && !need_rollback) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_inner_and_extern_tenant_info(const share::ObBackupDest& backup_dest,
    const uint64_t tenant_id, const int64_t round_id, const bool is_interrupted, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  int64_t tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  const ObLogArchiveStatus::STATUS status = is_interrupted ? ObLogArchiveStatus::INTERRUPTED : ObLogArchiveStatus::STOP;
  const int64_t origin_copy_id = 0;
  const bool for_update = true;
  ObLogArchiveBackupInfo info;
  int64_t copy_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(round_id));
  } else if (OB_FAIL(update_inner_table_log_archive_status(status))) {
    LOG_WARN("failed to update inner table log archive status", KR(ret), K(status));
  } else if (OB_FAIL(
                 mgr.get_log_archive_history_info(sql_proxy, tenant_id, round_id, origin_copy_id, for_update, info))) {
    LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(round_id));
  } else if (OB_FAIL(get_current_round_copy_id(backup_dest, tenant_id, round_id, copy_id))) {
    LOG_WARN("failed to get current round copy id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(update_extern_tenant_clog_backup_info(
                 tenant_id, round_id, 0 /*piece_id*/, copy_id, info.status_.checkpoint_ts_, backup_dest, status))) {
    LOG_WARN("failed to update extern log archive backup info", KR(ret), K(backup_dest));
  } else if (OB_FAIL(update_tenant_inner_table_his_info(tenant_id, round_id, copy_id, backup_dest, sql_proxy))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to update tenant inner table history info", KR(ret), K(tenant_id), K(round_id));
    }
  }
  return ret;
}

// current_round: 1
// round_list:         5, 6
// first_round: 1, last_round: 4
int ObBackupArchiveLogScheduler::update_inner_and_extern_tenant_infos_if_fast_forwarded(
    const int64_t first_round, const int64_t last_round)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  int64_t copy_id = 0;
  char dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObLogArchiveBackupInfoMgr mgr;
  const bool for_update = true;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLogArchiveStatus::STATUS status = ObLogArchiveStatus::STOP;
  ObArray<ObLogArchiveBackupInfo> infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set copy id", KR(ret));
  } else if (OB_FAIL(get_dst_backup_dest(tenant_id, dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dest.set(dest_str))) {
    LOG_WARN("failed to set backup backup dest", KR(ret));
  } else if (OB_FAIL(mgr.get_backup_backup_log_archive_round_list(*sql_proxy_, first_round, for_update, infos))) {
    LOG_WARN("failed to get backup backup log archive round list", KR(ret));
  } else if (infos.empty()) {
    LOG_INFO("infos is empty", KR(ret), K(infos), K(first_round), K(last_round));
  } else if (OB_FAIL(get_current_round_copy_id(dest, tenant_id, first_round, copy_id))) {
    LOG_WARN("failed to get current round copy id", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      ObLogArchiveBackupInfo& info = infos.at(i);
      info.status_.copy_id_ = copy_id;
      info.status_.status_ = status;
      if (OB_FAIL(
              mgr.update_log_archive_status_history(*sql_proxy_, info, inner_table_version_, *backup_lease_service_))) {
        LOG_WARN("failed to update log archive status history", KR(ret), K(info));
      } else if (OB_FAIL(update_extern_tenant_clog_backup_info(
                     tenant_id, first_round, 0 /*backup_piece_id*/, copy_id, 0 /*checkpoint_ts*/, dest, status))) {
        LOG_WARN("failed to update tenant clog backup info", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_tenant_schedule(const uint64_t tenant_id, const int64_t archive_round,
    const int64_t piece_id, const int64_t create_date, const int64_t job_id, const share::ObBackupDest& src,
    const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest backup_dest;
  ObArray<SimplePGWrapper*> node_list;
  common::ObAddr server;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (stop_) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("root service shutdown", KR(ret));
  } else if (round_stat_.is_interrupted()) {
    LOG_WARN("round is interrupted, do nothing", KR(ret), K(archive_round));
  } else if (OB_FAIL(backup_dest.set(src, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret));
  } else if (OB_FAIL(select_one_server(server))) {
    LOG_INFO("no server available currently", KR(ret), K(tenant_id));
  } else if (OB_FAIL(maybe_reschedule_timeout_pg_task(tenant_id))) {
    LOG_WARN("failed to do maybe reschedule timeout pg task", KR(ret), K(tenant_id));
  } else if (piece_id == 0 && OB_FAIL(get_batch_pg_list(backup_dest, archive_round, tenant_id, node_list))) {
    LOG_WARN("failed to get clog pg list", KR(ret), K(backup_dest), K(archive_round), K(tenant_id));
  } else if (piece_id > 0 && OB_FAIL(get_piece_batch_pg_list(
                                 job_id, archive_round, piece_id, create_date, tenant_id, backup_dest, node_list))) {
    LOG_WARN("failed to get piece batch pg list",
        KR(ret),
        K(archive_round),
        K(piece_id),
        K(create_date),
        K(tenant_id),
        K(backup_dest));
  } else if (node_list.empty() && OB_FAIL(set_server_free(server))) {
    LOG_WARN("failed to set server free", KR(ret), K(server));
  } else if (OB_FAIL(generate_backup_archive_log_task(
                 tenant_id, archive_round, piece_id, create_date, job_id, src, dst, server, node_list))) {
    LOG_WARN("failed to generate backup archive log task",
        KR(ret),
        K(tenant_id),
        K(archive_round),
        K(src),
        K(dst),
        K(server));
  }
  // make sure the pg node is freed when rpc is sent
  // whether failure or not
  free_pg_task_list(node_list);
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_info_list(
    const uint64_t tenant_id, common::ObIArray<share::ObLogArchiveBackupInfo>& round_list)
{
  int ret = OB_SUCCESS;
  round_list.reset();
  bool cur_entry_exist = true;
  const bool for_update = false;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfo cur_info;
  ObArray<ObLogArchiveBackupInfo> his_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(
                 mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, inner_table_version_, cur_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      cur_entry_exist = false;
    } else {
      LOG_WARN("failed to get log archive backup info", KR(ret), K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ObLogArchiveStatus::STATUS::STOP != cur_info.status_.status_ && cur_entry_exist &&
             OB_FAIL(round_list.push_back(cur_info))) {
    LOG_WARN("failed to push back", KR(ret), K(cur_info));
  } else if (OB_FAIL(mgr.get_log_archive_history_infos(*sql_proxy_, tenant_id, for_update, his_infos))) {
    LOG_WARN("failed to get log archive history infos", KR(ret), K(tenant_id));
  } else if (OB_FAIL(append(round_list, his_infos))) {
    LOG_WARN("failed to push back history infos", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_bb_sys_his_log_archive_info_list(common::ObArray<int64_t>& round_list)
{
  int ret = OB_SUCCESS;
  round_list.reset();
  ObLogArchiveBackupInfoMgr mgr;
  const bool for_update = false;
  ObArray<ObLogArchiveBackupInfo> his_infos;
  ObBackupDest dest;
  char dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup", KR(ret));
  } else if (OB_FAIL(mgr.get_backup_log_archive_history_infos(*sql_proxy_, OB_SYS_TENANT_ID, for_update, his_infos))) {
    LOG_WARN("failed to get log archive history infos", KR(ret));
  } else if (OB_FAIL(get_dst_backup_dest(OB_SYS_TENANT_ID, dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest", KR(ret));
  } else if (OB_FAIL(dest.set(dest_str))) {
    LOG_WARN("failed to set backup backup dest", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < his_infos.count(); ++i) {
      const ObLogArchiveBackupInfo& info = his_infos.at(i);
      ObBackupDest bb_dest;
      if (0 != info.status_.start_piece_id_) {
        // do nothing
      } else if (OB_FAIL(info.get_backup_dest(bb_dest))) {
        LOG_WARN("failed to get backup dest", KR(ret), K(info));
      } else if (dest.is_root_path_equal(bb_dest)) {
        if (OB_FAIL(round_list.push_back(info.status_.round_))) {
          LOG_WARN("failed to push back round", KR(ret), K(info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(round_list.begin(), round_list.end());
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_delta_round_list(const common::ObArray<int64_t>& b_round_list,
    const common::ObArray<int64_t>& bb_round_list, common::ObArray<int64_t>& round_list)
{
  int ret = OB_SUCCESS;
  round_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (b_round_list.empty()) {
    ret = OB_EAGAIN;
    LOG_WARN("no archive round now");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < b_round_list.count(); ++i) {
      bool exist_in_bb = false;
      const int64_t b_round = b_round_list.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < bb_round_list.count(); ++j) {
        const int64_t bb_round = bb_round_list.at(j);
        if (bb_round == b_round) {
          exist_in_bb = true;
          break;
        }
      }
      if (!exist_in_bb) {
        if (OB_FAIL(round_list.push_back(b_round))) {
          LOG_WARN("failed to push back", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(round_list.begin(), round_list.end());
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_round_info(
    const uint64_t tenant_id, const int64_t round_id, share::ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObArray<ObLogArchiveBackupInfo> round_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_log_archive_info_list(tenant_id, round_list))) {
    LOG_WARN("failed to get log archive info list", KR(ret), K(round_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < round_list.count(); ++i) {
      if (round_id == round_list.at(i).status_.round_) {
        info = round_list.at(i);
        exist = true;
      }
    }
    if (OB_SUCC(ret) && !exist) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_round_list(
    const uint64_t tenant_id, common::ObArray<ObLogArchiveBackupInfo>& info_list, common::ObArray<int64_t>& round_list)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  round_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_log_archive_info_list(tenant_id, info_list))) {
    LOG_WARN("failed to get log archive info list", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObLogArchiveBackupInfo& info = info_list.at(i);
      ObBackupDest backup_dest;
      bool is_same = true;
      if (0 != info.status_.start_piece_id_) {
        // do not auto backup piece
      } else if (OB_FAIL(info.get_backup_dest(backup_dest))) {
        LOG_WARN("failed to get backup dest", KR(ret), K(info));
      } else if (OB_FAIL(check_backup_dest_same_with_gconf(backup_dest, is_same))) {
        LOG_WARN("failed to check backup dest same with gconf", KR(ret));
      } else if (!is_same) {
        // do nothing if not same with current gconf
      } else if (OB_FAIL(round_list.push_back(info.status_.round_))) {
        LOG_WARN("failed to push back round", KR(ret), K(info));
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(round_list.begin(), round_list.end());
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_archivelog_started(
    const common::ObIArray<share::ObLogArchiveBackupInfo>& info_list, bool& started)
{
  int ret = OB_SUCCESS;
  started = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (info_list.count() > 1) {
    // do nothing
  } else if (info_list.empty()) {
    started = false;
  } else {
    const ObLogArchiveBackupInfo& info = info_list.at(0);
    if (0 == info.status_.round_ && ObLogArchiveStatus::STOP == info.status_.status_) {
      started = false;
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_round_checkpoint_ts(
    const uint64_t tenant_id, const int64_t archive_round, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  checkpoint_ts = 0;
  ObArray<ObLogArchiveBackupInfo> info_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (tenant_id == OB_INVALID_ID || archive_round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get log archive round checkpoint ts get invalid argument", KR(ret), K(tenant_id), K(archive_round));
  } else if (OB_FAIL(get_log_archive_info_list(tenant_id, info_list))) {
    LOG_WARN("failed to get log archive info list", KR(ret), K(tenant_id));
  } else if (info_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the round info list is empty", KR(ret), K(tenant_id), K(archive_round));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObLogArchiveBackupInfo& info = info_list.at(i);
      if (info.status_.round_ == archive_round) {
        checkpoint_ts = info.status_.checkpoint_ts_;
        LOG_INFO("get log archive round checkpoint ts", K(ret), K(tenant_id), K(archive_round), K(checkpoint_ts));
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_batch_pg_list(const share::ObClusterBackupDest& backup_dest, const int64_t round,
    const uint64_t tenant_id, common::ObArray<SimplePGWrapper*>& node_list)
{
  int ret = OB_SUCCESS;
  node_list.reset();
  bool is_history = false;
  bool current_turn_finished = false;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else {
    int64_t checkpoint_ts = 0;
    ObArray<ObPGKey> pg_keys;
    TenantTaskQueue* queue_ptr = NULL;
    int hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_SUCCESS == hash_ret) {
      if (queue_ptr->queue_.is_empty() && queue_ptr->need_fetch_new_) {
        if (OB_FAIL(check_current_turn_pg_finished(tenant_id, current_turn_finished))) {
          LOG_WARN("failed to check current turn pg list finished", KR(ret), K(tenant_id));
        } else if (!current_turn_finished) {
          LOG_INFO("current turn not finished, no need fetch new", K(tenant_id));
        } else if (OB_FAIL(check_is_history_round(round, is_history))) {
          LOG_WARN("failed to check is history round", KR(ret), K(round));
        } else if (OB_FAIL(mark_current_turn_in_history(tenant_id, is_history))) {
          LOG_WARN("failed to mark current turn in history", KR(ret), K(tenant_id), K(is_history));
        } else if (OB_FAIL(
                       get_clog_pg_list_v2(backup_dest, round, 0 /*piece_id*/, 0 /*timestamp*/, tenant_id, pg_keys))) {
          LOG_WARN("failed to get clog pg list", KR(ret), K(tenant_id), K(pg_keys.count()));
        } else if (pg_keys.empty()) {
          LOG_INFO("pg key list is empty", K(pg_keys.count()));
        } else if (OB_FAIL(push_pg_list(queue_ptr->queue_, round, 0 /*piece_id*/, 0 /*create_date*/, pg_keys))) {
          LOG_WARN("failed to push pg list", KR(ret), K(tenant_id));
        } else if (OB_FAIL(mark_current_turn_pg_list(tenant_id, pg_keys, false /*finished*/))) {
          LOG_WARN("failed to mark current turn pg list", KR(ret), K(tenant_id), K(pg_keys));
        } else if (OB_FAIL(round_stat_.add_pg_list(round, tenant_id, pg_keys))) {
          LOG_WARN("failed to add pg list", KR(ret), K(round), K(tenant_id), K(pg_keys));
        } else if (OB_FAIL(get_log_archive_round_checkpoint_ts(tenant_id, round, checkpoint_ts))) {
          LOG_WARN("failed to get log archive round checkpoint ts", KR(ret), K(tenant_id), K(round));
        } else if (0 == checkpoint_ts) {
          ret = OB_BACKUP_BACKUP_CAN_NOT_START;
          LOG_WARN("can not start backup backupset archivelog is checkpoint ts is 0", KR(ret), K(checkpoint_ts));
        } else if (OB_FAIL(set_checkpoint_ts_with_lock(tenant_id, checkpoint_ts))) {
          LOG_WARN("failed to set checkpoint ts", KR(ret));
        } else {
          queue_ptr->need_fetch_new_ = false;
          queue_ptr->pushed_before_ = true;
          LOG_INFO("no need to fetch new pg list until finish",
              K(round),
              K(is_history),
              K(tenant_id),
              K(checkpoint_ts),
              K(pg_keys.count()));
        }

        if (OB_FAIL(ret)) {
          int tmp_ret = rollback_pg_info_if_failed(round, queue_ptr);
          if (tmp_ret != OB_SUCCESS) {
            LOG_WARN("failed to rollback pg info if failed", KR(tmp_ret), K(round), KP(queue_ptr));
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant queue should exist", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pop_pg_list(queue_ptr->queue_, node_list))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("no pg left to move", KR(ret), K(tenant_id));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::rollback_pg_info_if_failed(const int64_t round, TenantTaskQueue* queue_ptr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("queue ptr should not be null", KR(ret), KP(queue_ptr));
  } else {
    if (OB_SUCCESS != (tmp_ret = pop_all_pg_queue(queue_ptr))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("failed to free pg queue", KR(ret), K(queue_ptr));
    }
    if (OB_SUCCESS != (tmp_ret = queue_ptr->pg_map_.clear())) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("failed to free pg queue", KR(ret), K(queue_ptr));
    }
    if (OB_SUCCESS != (tmp_ret = round_stat_.free_pg_map(round, queue_ptr->tenant_id_))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("failed to free pg map", KR(ret), K(round));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::push_pg_list(common::ObSpLinkQueue& queue, const int64_t round_id,
    const int64_t piece_id, const int64_t create_date, const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (pg_keys.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey& pg_key = pg_keys.at(i);
      SimplePGWrapper* pg = NULL;
      if (OB_FAIL(alloc_pg_task(pg, round_id, piece_id, create_date, pg_key))) {
        LOG_WARN("failed to alloc pg task", KR(ret), K(pg), K(round_id), K(piece_id), K(pg_key));
      } else if (OB_FAIL(queue.push(pg))) {
        LOG_WARN("failed to push back pg", KR(ret), K(pg));
        free_pg_task(pg);
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::pop_pg_list(common::ObSpLinkQueue& queue, common::ObArray<SimplePGWrapper*>& node_list)
{
  int ret = OB_SUCCESS;
  node_list.reset();
  int64_t batch_count = MAX_BATCH_SIZE;
#ifdef ERRSIM
  const int64_t tmp_count = GCONF.backup_backup_archive_log_batch_count;
  if (0 != tmp_count) {
    batch_count = tmp_count;
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (queue.is_empty()) {
    ret = OB_ITER_END;
    LOG_INFO("no pg available, wait until next time", KR(ret));
  } else {
    int64_t cnt = 0;
    while (OB_SUCC(ret) && !queue.is_empty()) {
      ObLink* link = NULL;
      SimplePGWrapper* pg = NULL;
      if (OB_FAIL(queue.pop(link))) {
        LOG_WARN("failed to pop task", KR(ret), K(link));
      } else if (OB_ISNULL(link)) {
        LOG_WARN("link is null", KR(ret), K(link));
      } else {
        pg = static_cast<SimplePGWrapper*>(link);
        const ObPGKey& pg_key = pg->pg_key_;
        if (OB_FAIL(node_list.push_back(pg))) {
          LOG_WARN("failed to push back pg node", K(pg_key), KP(pg));
          free_pg_task(pg);
        } else {
          ++cnt;
        }
      }
      if (cnt >= batch_count) {
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::pop_all_pg_queue(TenantTaskQueue* queue_ptr)
{
  int ret = OB_SUCCESS;
  ObArray<SimplePGWrapper*> tmp_pg_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), KP(queue_ptr));
  } else {
    do {
      tmp_pg_list.reset();
      if (OB_FAIL(pop_pg_list(queue_ptr->queue_, tmp_pg_list))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("no pg left to move", KR(ret));
          ret = OB_SUCCESS;
        }
      }
      free_pg_task_list(tmp_pg_list);
    } while (OB_SUCC(ret) && !queue_ptr->queue_.is_empty());
  }
  return ret;
}

int ObBackupArchiveLogScheduler::alloc_tenant_queue(const uint64_t tenant_id, TenantTaskQueue*& queue_ptr)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "BB_CLOG_QUEUE");
  void* buf = ob_malloc(sizeof(TenantTaskQueue), attr);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc tenant task queue", KR(ret));
  } else if (OB_ISNULL(queue_ptr = new (buf) TenantTaskQueue)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("construct tenant task queue failed", KR(ret));
  } else {
    queue_ptr->tenant_id_ = tenant_id;
  }
  return ret;
}

void ObBackupArchiveLogScheduler::free_tenant_queue(TenantTaskQueue*& queue_ptr)
{
  int tmp_ret = OB_SUCCESS;
  // make sure all resource if reclaimed before destructor
  if (OB_SUCCESS != (tmp_ret = pop_all_pg_queue(queue_ptr))) {
    LOG_WARN("failed to free pg queue", KR(tmp_ret));
  }
  if (OB_NOT_NULL(queue_ptr)) {
    queue_ptr->~TenantTaskQueue();
    ob_free(queue_ptr);
    queue_ptr = NULL;
  }
}

int ObBackupArchiveLogScheduler::alloc_pg_task(SimplePGWrapper*& pg, const int64_t round, const int64_t piece_id,
    const int64_t create_date, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "BB_CLOG_PG");
  void* buf = ob_malloc(sizeof(SimplePGWrapper), attr);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc memory for pg", KR(ret), K(sizeof(SimplePGWrapper)));
  } else if (OB_ISNULL(pg = new (buf) SimplePGWrapper)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("construct pg wrapper failed", KR(ret));
  } else {
    pg->archive_round_ = round;
    pg->piece_id_ = piece_id;
    pg->create_date_ = create_date;
    pg->pg_key_ = pg_key;
  }
  return ret;
}

void ObBackupArchiveLogScheduler::free_pg_task(SimplePGWrapper*& pg)
{
  if (OB_NOT_NULL(pg)) {
    pg->~SimplePGWrapper();
    ob_free(pg);
    pg = NULL;
  }
}

void ObBackupArchiveLogScheduler::free_pg_task_list(common::ObArray<SimplePGWrapper*>& node_list)
{
  for (int64_t i = 0; i < node_list.count(); ++i) {
    SimplePGWrapper* node = node_list.at(i);
    if (OB_NOT_NULL(node)) {
      free_pg_task(node);
    }
  }
}

int ObBackupArchiveLogScheduler::get_pg_list_from_node_list(
    const common::ObArray<SimplePGWrapper*>& node_list, common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  pg_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (node_list.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node_list.count(); ++i) {
      const SimplePGWrapper* pg = node_list.at(i);
      if (OB_NOT_NULL(pg)) {
        const ObPGKey& pkey = pg->pg_key_;
        if (OB_FAIL(pg_list.push_back(pkey))) {
          LOG_WARN("failed to push back pg key", KR(ret), K(pkey));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_duplicate_pg(pg_list))) {
        LOG_WARN("failed to remove duplicate pg", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_self_dup_task(pg_list))) {
        LOG_WARN("failed to check self dup task", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::remove_duplicate_pg(common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  static int64_t batch_size = 1024;
  ObArray<ObPGKey> tmp_pg_list;
  common::hash::ObHashSet<ObPGKey> pg_set;
  common::hash::ObHashSet<ObPGKey>::iterator iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (pg_list.empty()) {
    // do nothing
  } else if (OB_FAIL(pg_set.create(batch_size))) {
    LOG_WARN("failed to create pg set", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const ObPGKey& pg_key = pg_list.at(i);
      if (OB_FAIL(pg_set.set_refactored(pg_key))) {
        LOG_WARN("failed to set pg key", KR(ret), K(pg_key));
      }
    }
    if (OB_SUCC(ret)) {
      for (iter = pg_set.begin(); OB_SUCC(ret) && iter != pg_set.end(); iter++) {
        if (OB_FAIL(tmp_pg_list.push_back(iter->first))) {
          LOG_WARN("failed to push pg key", KR(ret), K(iter->first));
        }
      }
    }
    if (OB_SUCC(ret)) {
      pg_list.reset();
      if (OB_FAIL(append(pg_list, tmp_pg_list))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_self_dup_task(const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (pg_list.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < pg_list.count(); ++j) {
        if (pg_list.at(i) == pg_list.at(j)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pg list has dup key", KR(ret), K(i), K(j));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_clog_pg_list_v2(const share::ObClusterBackupDest& backup_dest,
    const int64_t log_archive_round, const int64_t piece_id, const int64_t timestamp, const uint64_t tenant_id,
    common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  ObBackupListDataMgr backup_list_mgr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(backup_list_mgr.init(backup_dest, log_archive_round, tenant_id, piece_id, timestamp))) {
    LOG_WARN("failed to init backup list mgr", K(ret), K(backup_dest));
  } else if (OB_FAIL(backup_list_mgr.get_clog_pkey_list(pg_keys))) {
    LOG_WARN("failed to get clog pkey list", K(ret), K(backup_dest), K(log_archive_round), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::generate_backup_archive_log_task(const uint64_t tenant_id, const int64_t round_id,
    const int64_t piece_id, const int64_t create_date, const int64_t job_id, const share::ObBackupDest& src,
    const share::ObBackupDest& dst, const common::ObAddr& server, common::ObArray<SimplePGWrapper*>& node_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  share::ObTaskId trace_id;
  trace_id.init(MYADDR);
  obrpc::ObBackupArchiveLogBatchArg arg;
  TenantTaskQueue* queue_ptr = NULL;
  bool rpc_failed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (tenant_id == OB_INVALID_ID || round_id < 0 || piece_id < 0 || !src.is_valid() || !dst.is_valid() ||
             !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(round_id), K(piece_id), K(src), K(dst), K(server));
  } else if (node_list.empty()) {
    // do nothing
  } else if (OB_FAIL(build_backup_archive_log_arg(
                 tenant_id, round_id, piece_id, create_date, job_id, src, dst, trace_id, node_list, arg))) {
    LOG_WARN("failed to build backup archive arg", KR(ret), K(round_id), K(tenant_id), K(trace_id));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr should not be null");
  } else if (OB_FAIL(update_pg_stat(server, trace_id, node_list, queue_ptr))) {
    LOG_WARN("failed to update pg stat", KR(ret), K(round_id), K(server), K(trace_id), K(node_list));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BACKUP_ARCHIVELOG_RPC_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        rpc_failed = true;
        LOG_WARN("errsim backup archivelog rpc failed", KR(ret));
      }
    }
#endif
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rpc_proxy_->to(server).backup_archive_log_batch(arg))) {
        rpc_failed = true;
        LOG_WARN("failed to send backup archive log batch rpc",
            KR(ret),
            K(server),
            K(tenant_id),
            K(round_id),
            K(piece_id),
            K(job_id),
            K(arg));
      } else {
        LOG_INFO(
            "send backup archive log rpc success", K(tenant_id), K(round_id), K(piece_id), K(server), K(src), K(dst));
      }
    }
  }
  if (OB_FAIL(ret) && rpc_failed) {
    tmp_ret = do_backup_archive_log_rpc_failed(tenant_id, round_id, piece_id, create_date, node_list, queue_ptr);
    if (OB_FAIL(tmp_ret)) {
      LOG_WARN("failed to do backup archive log rpc failed", KR(ret), K(tenant_id), K(round_id), K(piece_id));
    }
    ROOTSERVICE_EVENT_ADD("backup_archive_log",
        "send_rpc_failed",
        "tenant_id",
        tenant_id,
        "round_id",
        round_id,
        "piece_id",
        piece_id,
        "server",
        server);
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_backup_archive_log_rpc_failed(const uint64_t tenant_id, const int64_t round_id,
    const int64_t piece_id, const int64_t create_date, const common::ObArray<SimplePGWrapper*>& node_list,
    TenantTaskQueue* queue_ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || round_id < 0 || piece_id < 0 || OB_ISNULL(queue_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(round_id), K(piece_id), KP(queue_ptr));
  } else {
    ObArray<ObPGKey> pg_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < node_list.count(); ++i) {
      const SimplePGWrapper* node = node_list.at(i);
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", KR(ret), KP(node));
      } else if (OB_FAIL(pg_list.push_back(node->pg_key_))) {
        LOG_WARN("failed to push back pg key", KR(ret), K(*node));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(push_pg_list(queue_ptr->queue_, round_id, piece_id, create_date, pg_list))) {
      LOG_WARN("failed to push pg list", KR(ret), K(round_id));
    } else if (OB_FAIL(mark_current_turn_pg_list(tenant_id, pg_list, false))) {
      LOG_WARN("failed to mark current turn pg list", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::build_backup_archive_log_arg(const uint64_t tenant_id, const int64_t archive_round,
    const int64_t piece_id, const int64_t create_date, const int64_t job_id, const share::ObBackupDest& src,
    const share::ObBackupDest& dst, const share::ObTaskId& trace_id, const common::ObArray<SimplePGWrapper*>& node_list,
    obrpc::ObBackupArchiveLogBatchArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t checkpoint_ts = 0;
  ObArray<ObPGKey> pg_keys;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || archive_round <= 0 || !src.is_valid() || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build rpc arg get invalid argument", KR(ret), K(tenant_id), K(archive_round), K(src), K(dst));
  } else if (OB_FAIL(get_checkpoint_ts_with_lock(tenant_id, checkpoint_ts))) {
    LOG_WARN("failed to get tenant checkpoint ts", KR(ret), K(tenant_id));
  } else if (0 == piece_id && 0 == checkpoint_ts) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("checkpoint ts is not valid", KR(ret), K(checkpoint_ts));
  } else if (OB_FAIL(get_pg_list_from_node_list(node_list, pg_keys))) {
    LOG_WARN("failed to get pg list from node list", KR(ret), K(node_list));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.archive_round_ = archive_round;
    arg.piece_id_ = piece_id;
    arg.create_date_ = create_date;
    arg.job_id_ = job_id;
    arg.checkpoint_ts_ = checkpoint_ts;
    arg.task_id_ = trace_id;
    if (OB_FAIL(databuff_printf(arg.src_root_path_, OB_MAX_BACKUP_PATH_LENGTH, "%s", src.root_path_))) {
      LOG_WARN("failed to databuff printf", KR(ret), K(src));
    } else if (OB_FAIL(databuff_printf(
                   arg.src_storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", src.storage_info_))) {
      LOG_WARN("failed to databuff printf", KR(ret), K(src));
    } else if (OB_FAIL(databuff_printf(arg.dst_root_path_, OB_MAX_BACKUP_PATH_LENGTH, "%s", dst.root_path_))) {
      LOG_WARN("failed to databuff printf", KR(ret), K(dst));
    } else if (OB_FAIL(databuff_printf(
                   arg.dst_storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", dst.storage_info_))) {
      LOG_WARN("failed to databuff printf", KR(ret), K(dst));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      obrpc::ObPGBackupArchiveLogArg element;
      element.archive_round_ = archive_round;
      element.pg_key_ = pg_keys.at(i);
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("failed to push back element", KR(ret), K(element));
      } else {
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_pg_stat(const common::ObAddr& server, const share::ObTaskId& trace_id,
    common::ObArray<SimplePGWrapper*>& pg_list, TenantTaskQueue* queue_ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!server.is_valid() || trace_id.is_invalid() || OB_ISNULL(queue_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(server), K(trace_id), KP(queue_ptr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      SimplePGWrapper* pg = pg_list.at(i);
      if (OB_ISNULL(pg)) {
      } else {
        CurrentTurnPGStat stat;
        const int flag = 1;
        const ObPGKey& pkey = pg->pg_key_;
        stat.pg_key_ = pkey;
        stat.server_ = server;
        stat.trace_id_ = trace_id;
        stat.finished_ = false;
        if (OB_FAIL(queue_ptr->pg_map_.set_refactored(pkey, stat, flag))) {
          LOG_INFO("failed to set pg key stat", KR(ret), K(pkey), K(stat));
          int tmp_ret = queue_ptr->queue_.push(pg);
          if (OB_SUCCESS != tmp_ret) {
            free_pg_task(pg);
            pg_list[i] = NULL;
            LOG_ERROR("failed to push back pg", KR(ret), KP(pg));
          }
          ret = OB_SUCCESS != tmp_ret ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::select_one_server(common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(server_mgr_->get_all_server_list(server_list))) {
    LOG_WARN("failed to get all server list", KR(ret));
  } else if (OB_UNLIKELY(server_list.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no server exist", KR(ret));
  } else if (OB_FAIL(prepare_server_if_needed(server_list))) {
    LOG_WARN("failed to prepare server", KR(ret), K(server_list));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      const common::ObAddr& addr = server_list.at(i);
      bool is_active = false;
      if (OB_FAIL(server_mgr_->check_server_active(addr, is_active))) {
        LOG_WARN("failed to check server active", KR(ret), K(addr));
      } else if (!is_active) {
        tmp_ret = OB_SERVER_NOT_ACTIVE;
        LOG_WARN("server is not active", KR(tmp_ret), K(addr));
        continue;
      } else {
        ServerStatus status;
        int hash_ret = server_status_map_.get_refactored(addr, status);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server should exist", KR(ret));
        } else if (OB_SUCCESS == hash_ret) {
          if (ServerStatus::STATUS_BUSY == status.status_) {
            if (ObTimeUtil::current_time() - status.lease_time_ < SERVER_LEASE_TIME) {
              continue;
            } else {
              int flag = 1;  // overwrite
              status.lease_time_ = ObTimeUtil::current_time();
              if (OB_FAIL(server_status_map_.set_refactored(addr, status, flag))) {
                LOG_WARN("failed to set server status", KR(ret), K(addr));
              } else {
                server = addr;
                find = true;
                LOG_INFO("select one server to do work", K(addr));
                break;
              }
            }
          } else if (ServerStatus::STATUS_FREE == status.status_) {
            int flag = 1;  // overwrite
            status.status_ = ServerStatus::STATUS_BUSY;
            status.lease_time_ = ObTimeUtil::current_time();
            if (OB_FAIL(server_status_map_.set_refactored(addr, status, flag))) {
              LOG_WARN("failed to set server status", KR(ret), K(addr));
            } else {
              server = addr;
              find = true;
              LOG_INFO("select one server to do work", K(addr));
              break;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get refactored error", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_EAGAIN;
      LOG_INFO("no free server currently");
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_dst_backup_dest(
    const uint64_t tenant_id, char* backup_backup_dest, const int64_t len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObMySQLTransaction trans;
  ObBaseBackupInfoStruct::BackupDest backup_dest;
  ObBaseBackupInfo backup_info;
  ObBackupInfoManager info_manager;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup backup dest get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to push back tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_backup_dest(OB_SYS_TENANT_ID, trans, backup_dest))) {
      LOG_WARN("failed to get backup backup dest", KR(ret));
    } else {
      if (!backup_dest.is_empty()) {
        backup_dest.to_string(backup_backup_dest, len);
      } else {
        if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest, len))) {
          LOG_WARN("failed to copy backup backup dest", KR(ret));
        } else if (OB_FAIL(info_manager.update_backup_backup_dest(OB_SYS_TENANT_ID, trans, backup_backup_dest))) {
          LOG_WARN("failed to update backup info", KR(ret), K(tenant_id), K(backup_info));
        }
      }
    }

    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_is_history_round(const int64_t round, bool& is_his_round)
{
  int ret = OB_SUCCESS;
  is_his_round = false;
  ObLogArchiveBackupInfo cur_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_current_log_archive_round_info(OB_SYS_TENANT_ID, cur_info))) {
    LOG_WARN("failed to get current log archive round", KR(ret));
  } else if (round >= cur_info.status_.round_ && ObLogArchiveStatus::STOP != cur_info.status_.status_) {
    LOG_INFO("archive round is still in progress", K(round), K(cur_info));
  } else {
    is_his_round = true;
    LOG_INFO("archive round is in history", K(round), K(cur_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_current_turn_in_history(const uint64_t tenant_id, bool& in_history)
{
  int ret = OB_SUCCESS;
  in_history = false;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else {
    in_history = queue_ptr->in_history_;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_round_start_ts(const uint64_t tenant_id, const int64_t round, int64_t& start_ts)
{
  int ret = OB_SUCCESS;
  start_ts = 0;
  ObArray<ObLogArchiveBackupInfo> info_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_log_archive_info_list(tenant_id, info_list))) {
    LOG_WARN("failed to get log archive info list", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObLogArchiveBackupInfo& info = info_list.at(i);
      if (round == info.status_.round_) {
        start_ts = info.status_.start_ts_;
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::inner_get_next_round_with_lock(const ObArray<int64_t>& round_list, int64_t& next_round)
{
  int ret = OB_SUCCESS;
  next_round = 0;
  ObLockGuard<ObMutex> guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(round_stat_.get_next_round(round_list, next_round))) {
    LOG_WARN("failed to get next round", KR(ret), K(round_list), K(next_round));
  } else {
    LOG_INFO("inner get next round with lock", K(next_round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_tenant_queue_ptr(const uint64_t tenant_id, TenantTaskQueue*& queue_ptr)
{
  int ret = OB_SUCCESS;
  queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_queue_map_.get_refactored(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::cancel_all_outgoing_tasks()
{
  int ret = OB_SUCCESS;
  ObArray<ServerTraceStat> stat_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_all_server_trace_list(stat_list))) {
    LOG_WARN("failed to get all server trace list", KR(ret));
  } else {
    LOG_INFO("cancel all outing task");
    for (int64_t i = 0; OB_SUCC(ret) && i < stat_list.count(); ++i) {
      const ServerTraceStat& stat = stat_list.at(i);
      obrpc::ObCancelTaskArg rpc_arg;
      const ObAddr& server = stat.server_;
      rpc_arg.task_id_ = stat.trace_id_;
      bool is_alive = false;
      bool in_service = false;
      if (OB_FAIL(server_mgr_->check_server_alive(server, is_alive))) {
        LOG_WARN("failed to check server alive", K(ret), K(server));
      } else if (!is_alive) {
        // do nothing
        LOG_WARN("dst server not alive", K(server));
      } else if (OB_FAIL(server_mgr_->check_in_service(server, in_service))) {
        LOG_WARN("failed to check in servic", K(ret), K(server));
      } else if (!in_service) {
        // do nothing
        LOG_WARN("dst server not in service", K(server));
      } else if (OB_FAIL(rpc_proxy_->to(server).cancel_sys_task(rpc_arg))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("task may not excute on server", K(rpc_arg), K(server));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to cancel sys task", KR(ret), K(rpc_arg));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_all_server_trace_list(common::ObIArray<ServerTraceStat>& stat_list)
{
  int ret = OB_SUCCESS;
  stat_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else {
    typedef common::hash::ObHashMap<uint64_t, TenantTaskQueue*>::iterator Iterator;
    Iterator iter = tenant_queue_map_.begin();
    ObArray<ServerTraceStat> tmp_stat_list;
    for (; OB_SUCC(ret) && iter != tenant_queue_map_.end(); ++iter) {
      tmp_stat_list.reset();
      const TenantTaskQueue* queue_ptr = iter->second;
      if (OB_ISNULL(queue_ptr)) {
        // do nothing
      } else if (OB_FAIL(get_tenant_server_trace_list(queue_ptr, tmp_stat_list))) {
        LOG_WARN("failed to get tenant server trace list", KR(ret), KP(queue_ptr));
      } else if (OB_FAIL(append(stat_list, tmp_stat_list))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_tenant_server_trace_list(
    const TenantTaskQueue* queue_ptr, common::ObIArray<ServerTraceStat>& stat_list)
{
  int ret = OB_SUCCESS;
  stat_list.reset();
  ObHashSet<ServerTraceStat> stat_set;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant queue should not be null", KR(ret), KP(queue_ptr));
  } else if (OB_FAIL(stat_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create server trace stat set", KR(ret));
  } else {
    typedef ObHashMap<ObPGKey, CurrentTurnPGStat>::const_iterator Iterator;
    Iterator iter = queue_ptr->pg_map_.begin();
    for (; OB_SUCC(ret) && iter != queue_ptr->pg_map_.end(); ++iter) {
      const CurrentTurnPGStat& stat = iter->second;
      ServerTraceStat st_stat;
      st_stat.trace_id_ = stat.trace_id_;
      st_stat.server_ = stat.server_;
      if (!stat.finished_) {
        if (!st_stat.is_valid()) {
          // do nothing if stat is not valid
        } else if (OB_FAIL(stat_set.set_refactored(st_stat))) {
          LOG_WARN("failed to set stat set", KR(ret), K(st_stat));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObHashSet<ServerTraceStat>::const_iterator siter;
      for (siter = stat_set.begin(); OB_SUCC(ret) && siter != stat_set.end(); ++siter) {
        const ServerTraceStat& st_stat = siter->first;
        if (OB_FAIL(stat_list.push_back(st_stat))) {
          LOG_WARN("failed to push back server trace stat", KR(ret), K(st_stat));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::maybe_reschedule_timeout_pg_task(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  TenantTaskQueue* queue_ptr = NULL;
  ObArray<ObPGKey> unfinished_list, lost_list;
  bool need_reschedule = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_need_reschedule(queue_ptr, need_reschedule))) {
    LOG_WARN("failed to check need reschedule", KR(ret), K(tenant_id));
  } else if (!need_reschedule) {
    LOG_INFO("no need to reschedule pg tasks", K(tenant_id));
  } else if (OB_FAIL(get_unfinished_pg_list_from_tenant_queue(queue_ptr, unfinished_list))) {
    LOG_WARN("failed to get unfinished pg list from tenant queue", KR(ret));
  } else if (OB_FAIL(get_lost_pg_list(queue_ptr, unfinished_list, lost_list))) {
    LOG_WARN("failed to get lost pg list", KR(ret), K(unfinished_list));
  } else if (OB_FAIL(rethrow_pg_back_to_queue(lost_list, queue_ptr))) {
    LOG_WARN("failed to rethow lost pg backup to queue", KR(ret), K(lost_list));
  } else {
    wakeup();
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_need_reschedule(TenantTaskQueue* queue, bool& need_reschedule)
{
  int ret = OB_SUCCESS;
  need_reschedule = false;
  const int64_t current_timestamp_us = ObTimeUtility::current_time();
  int64_t reschedule_interval = 120 * 1000 * 1000;  // 2min
#ifdef ERRSIM
  int64_t tmp_interval = GCONF.backup_backup_archivelog_retry_interval;
  if (0 != tmp_interval) {
    reschedule_interval = tmp_interval;
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant qeueue should not be null", KR(ret), KP(queue));
  } else if (current_timestamp_us - queue->check_need_reschedule_ts_ < reschedule_interval) {
    // do nothing
  } else {
    need_reschedule = true;
    queue->check_need_reschedule_ts_ = current_timestamp_us;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_unfinished_pg_list_from_tenant_queue(
    const TenantTaskQueue* queue, ObIArray<ObPGKey>& unfinished_pg_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant qeueue should not be null", KR(ret), KP(queue));
  } else {
    typedef ObHashMap<ObPGKey, CurrentTurnPGStat>::const_iterator Iterator;
    Iterator iter = queue->pg_map_.begin();
    for (; OB_SUCC(ret) && iter != queue->pg_map_.end(); ++iter) {
      if (!iter->second.finished_) {
        if (OB_FAIL(unfinished_pg_list.push_back(iter->first))) {
          LOG_WARN("failed to push back pg key", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_lost_pg_list(const TenantTaskQueue* queue,
    const common::ObIArray<common::ObPGKey>& unfinished_list, common::ObIArray<common::ObPGKey>& lost_list)
{
  int ret = OB_SUCCESS;
  lost_list.reset();
  ObHashSet<ServerTraceStat> trace_set;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant qeueue should not be null", KR(ret), KP(queue));
  } else if (OB_FAIL(trace_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create trace set", KR(ret));
  } else if (OB_FAIL(get_stat_set_from_pg_list(queue, unfinished_list, trace_set))) {
    LOG_WARN("failed to get stat set from pg list", KR(ret), K(unfinished_list));
  } else {
    ObHashSet<ServerTraceStat>::iterator iter;
    iter = trace_set.begin();
    for (; OB_SUCC(ret) && iter != trace_set.end(); ++iter) {
      bool in_progress = true;
      const common::ObAddr addr = iter->first.server_;
      const share::ObTaskId trace_id = iter->first.trace_id_;
      ObArray<ObPGKey> tmp_pg_list;
      if (!addr.is_valid() || trace_id.is_invalid()) {
        // do not deal with unsent tasks
        continue;
      } else if (OB_FAIL(check_task_in_progress(addr, trace_id, in_progress))) {
        LOG_WARN("failed to check task in progress", KR(ret), K(addr), K(trace_id));
      } else if (in_progress) {
        // do nothing if in progress
      } else if (OB_FAIL(get_same_trace_id_pg_list(queue, addr, trace_id, tmp_pg_list))) {
        LOG_WARN("failed to get same trace id pg list", KR(ret), K(addr), K(trace_id));
      } else if (OB_FAIL(append(lost_list, tmp_pg_list))) {
        LOG_WARN("failed to add array", KR(ret), K(lost_list));
      }
    }
    LOG_INFO("get lost pg list", KR(ret), "count", lost_list.count(), K(lost_list));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_task_in_progress(
    const common::ObAddr& server, const share::ObTaskId& trace_id, bool& in_progress)
{
  int ret = OB_SUCCESS;
  in_progress = true;
  bool is_exist = true;
  obrpc::Bool res = false;
  share::ObServerStatus server_status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("server manager is null", KR(ret), KP(server_mgr_));
  } else if (OB_FAIL(server_mgr_->is_server_exist(server, is_exist))) {
    LOG_WARN("failed to check server exist", KR(ret), K(server));
  } else if (!is_exist) {
    in_progress = false;
    LOG_INFO("server is not exist, task maybe lost", KR(ret));
  } else if (OB_FAIL(server_mgr_->get_server_status(server, server_status))) {
    LOG_WARN("failed to get server status", KR(ret), K(server));
  } else if (!server_status.is_active() || !server_status.in_service()) {
    in_progress = false;
  } else if (OB_FAIL(rpc_proxy_->to(server).check_migrate_task_exist(trace_id, res))) {
    LOG_WARN("failed to check migrate task exist", KR(ret));
  } else if (!res) {
    in_progress = false;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_stat_set_from_pg_list(const TenantTaskQueue* queue,
    const common::ObIArray<common::ObPGKey>& unfinished_list, common::hash::ObHashSet<ServerTraceStat>& trace_set)
{
  int ret = OB_SUCCESS;
  trace_set.clear();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(queue), K(unfinished_list));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unfinished_list.count(); ++i) {
      const common::ObPGKey& pkey = unfinished_list.at(i);
      CurrentTurnPGStat stat;
      ServerTraceStat new_stat;
      if (OB_FAIL(queue->pg_map_.get_refactored(pkey, stat))) {
        LOG_WARN("failed to get refactored", K(ret));
      } else {
        new_stat.server_ = stat.server_;
        new_stat.trace_id_ = stat.trace_id_;
        if (OB_FAIL(trace_set.set_refactored(new_stat))) {
          LOG_WARN("failed to set refactored", KR(ret), K(new_stat));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::rethrow_pg_back_to_queue(const ObIArray<ObPGKey>& pg_list, TenantTaskQueue*& queue)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("tenant qeueue should not be null", KR(ret), KP(queue));
  } else {
    const int64_t round = round_stat_.get_current_round();
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const ObPGKey& pkey = pg_list.at(i);
      SimplePGWrapper* pg = NULL;
      if (OB_FAIL(alloc_pg_task(pg, round, 0 /*piece_id*/, 0 /*create_date*/, pkey))) {
        LOG_WARN("failed to alloc pg task", KR(ret), K(round), K(pkey));
      } else if (OB_FAIL(queue->queue_.push(pg))) {
        // push failed
        LOG_WARN("failed to push back pg", KR(ret), K(pg));
        free_pg_task(pg);
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::mark_current_turn_pg_list(
    const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list, const bool finished)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const ObPGKey& pkey = pg_list.at(i);
      const int flag = 1;
      CurrentTurnPGStat stat;
      hash_ret = queue_ptr->pg_map_.get_refactored(pkey, stat);
      if (hash_ret == OB_HASH_NOT_EXIST) {
        CurrentTurnPGStat new_stat;
        new_stat.finished_ = finished;
        if (OB_FAIL(queue_ptr->pg_map_.set_refactored(pkey, new_stat, flag))) {
          LOG_WARN("failed to set map", KR(ret), K(pkey), K(new_stat));
        }
      } else if (hash_ret == OB_SUCCESS) {
        stat.finished_ = finished;
        if (OB_FAIL(queue_ptr->pg_map_.set_refactored(pkey, stat, flag))) {
          LOG_WARN("failed to set map", KR(ret), K(tenant_id), K(pkey));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("some thing unexpected happened", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::mark_current_turn_in_history(const uint64_t tenant_id, const bool in_history)
{
  int ret = OB_SUCCESS;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else {
    queue_ptr->in_history_ = in_history;
    LOG_INFO("mark current turn in history", KR(ret), K(tenant_id), K(in_history));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_current_turn_pg_finished(const uint64_t tenant_id, bool& finished)
{
  int ret = OB_SUCCESS;
  finished = true;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_queue_ptr(tenant_id, queue_ptr))) {
    LOG_WARN("failed to get tenant queue ptr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else if (0 == queue_ptr->pg_map_.size()) {
    // is finished if is empty
  } else {
    typedef ObHashMap<ObPGKey, CurrentTurnPGStat>::const_iterator Iterator;
    Iterator iter = queue_ptr->pg_map_.begin();
    for (; OB_SUCC(ret) && iter != queue_ptr->pg_map_.end(); ++iter) {
      if (!iter->second.finished_) {
        finished = false;
        LOG_INFO("current turn pg is not finished", K(iter->first), K(iter->second));
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_same_trace_id_pg_list(const TenantTaskQueue* queue_ptr,
    const common::ObAddr& server, const share::ObTaskId& trace_id, common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  pg_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_ISNULL(queue_ptr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("queue ptr is null", KR(ret), KP(queue_ptr));
  } else {
    typedef ObHashMap<ObPGKey, CurrentTurnPGStat>::const_iterator Iterator;
    Iterator iter = queue_ptr->pg_map_.begin();
    for (; OB_SUCC(ret) && iter != queue_ptr->pg_map_.end(); ++iter) {
      if (iter->second.server_ == server && iter->second.trace_id_.equals(trace_id)) {
        if (OB_FAIL(pg_list.push_back(iter->first))) {
          LOG_WARN("failed to push back pg key", KR(ret), K(server), K(trace_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_clear_tenant_resource_if_dropped(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else {
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(queue_ptr)) {
        // do nothing
      } else {
        if (OB_SUCCESS != (tmp_ret = queue_ptr->pg_map_.destroy())) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("failed to destroy pg map", KR(ret));
        }
        if (OB_SUCCESS != (tmp_ret = pop_all_pg_queue(queue_ptr))) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("failed to free pg queue", KR(ret));
        }
        free_tenant_queue(queue_ptr);
        if (OB_SUCCESS != (tmp_ret = round_stat_.free_tenant_stat(tenant_id))) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("failed to free tenant stat", KR(ret), K(tenant_id));
        }
        if (OB_SUCCESS != (tmp_ret = tenant_queue_map_.erase_refactored(tenant_id))) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("failed to erase pg queue", KR(ret));
        }
      }
      LOG_DEBUG("do clear tenant resource if dropped", K(tenant_id));
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      LOG_INFO("do not clear tenant resource if tenant not exist", K(tenant_id));
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get map element", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

// 换backup backup dest
// backup_dest_1 round_1(copy_1)
//               ---> change backup backup dest
// backup_dest_2 round_1(copy_2) round_2(copy_1)
//               ---> change backup backup dest
// backup_dest_3 round_1(copy_3) round_2(copy_2) round_3(copy_1)
//               ---> change backup backup dest
// backup_dest_4 round_1(copy_4) round_2(copy_3) round_3(copy_2) round_4(copy_3)

int ObBackupArchiveLogScheduler::may_do_backup_backup_dest_changed()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  const int64_t round = round_stat_.get_current_round();
  bool backup_backup_dest_changed = false;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup", KR(ret));
  } else if (OB_FAIL(check_backup_backup_dest_changed(OB_SYS_TENANT_ID, backup_backup_dest_changed))) {
    LOG_WARN("failed to check backup backup dest changed", KR(ret));
  } else if (!backup_backup_dest_changed) {
    LOG_INFO("backup backup dest do not change");
  } else if (OB_FAIL(get_all_tenant_ids(round, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const int64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL((do_backup_backup_dest_changed(tenant_id)))) {
        LOG_WARN("failed to do backup backup dest changed action", KR(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", KR(ret));
      } else {
        if (OB_FAIL(update_new_backup_backup_dest(OB_SYS_TENANT_ID, trans))) {
          LOG_WARN("failed to update new backup backup dest", KR(ret));
        } else if (OB_FAIL(mgr.delete_all_backup_backup_log_archive_info(trans))) {
          LOG_WARN("failed to delete all backup backup log archive info", KR(ret));
        } else {
          LOG_INFO("delete all backup backup log archive info");
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", KR(ret));
          } else {
            round_stat_.reuse();
            LOG_INFO("round stat is reused");
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
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_backup_dest_changed(const uint64_t tenant_id, bool& changed)
{
  int ret = OB_SUCCESS;
  changed = false;
  ObBackupInfoManager mgr;
  ObBaseBackupInfoStruct::BackupDest backup_backup_dest;
  char gconf_backup_backup_dest[OB_MAX_URI_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.get_backup_backup_dest(tenant_id, *sql_proxy_, backup_backup_dest))) {
    LOG_WARN("failed to get backup backup dest", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(gconf_backup_backup_dest, OB_MAX_URI_LENGTH))) {
    LOG_WARN("failed to get gconf backup backup dest", KR(ret));
  } else if (backup_backup_dest.is_empty()) {
    LOG_INFO("backup backup dest is empty, do not change", KR(ret), K(tenant_id));
  } else {
    changed = (0 != STRNCMP(gconf_backup_backup_dest, backup_backup_dest.ptr(), OB_MAX_URI_LENGTH));
    if (changed) {
      LOG_INFO("backup backup dest has changed", K(gconf_backup_backup_dest), K(backup_backup_dest), K(tenant_id));
    } else {
      LOG_DEBUG("backup backup dest do not change", K(gconf_backup_backup_dest), K(backup_backup_dest), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_backup_backup_dest_changed(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObBackupDest old_dest;
  char old_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObLogArchiveBackupInfo current_info;  // current info
  bool is_round_mode = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_current_round_task_info(tenant_id, current_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get current round tasks", KR(ret), K(tenant_id));
    }
  } else if (OB_FAIL(get_dst_backup_dest(OB_SYS_TENANT_ID, old_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get old dst backup dest", KR(ret));
  } else if (OB_FAIL(old_dest.set(old_dest_str))) {
    LOG_WARN("failed to set old dest", KR(ret));
  } else if (OB_FAIL(check_is_round_mode(tenant_id, current_info.status_.round_, is_round_mode))) {
    LOG_WARN("failed to check is round mode", KR(ret), K(tenant_id), K(current_info));
  } else if (is_round_mode &&
             OB_FAIL(update_round_piece_info_for_clean(
                 tenant_id, current_info.status_.round_, true /*round_finished*/, old_dest, *sql_proxy_))) {
    LOG_WARN("failed to update round piece info for clean", KR(ret), K(tenant_id), K(current_info));
  } else if (OB_FAIL(move_current_task_to_history(current_info))) {
    LOG_WARN("failed to move current task to history", KR(ret));
  } else if (OB_FAIL(reset_round_stat_if_dest_changed(tenant_id))) {
    LOG_WARN("failed to do reset round stat if dest changed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_current_round_task_info(
    const uint64_t tenant_id, share::ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  const bool for_update = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("fialed to set copy id", KR(ret));
  } else if (OB_FAIL(mgr.get_log_archive_backup_backup_info(*sql_proxy_, for_update, tenant_id, info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::move_current_task_to_history(const share::ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t copy_id = 0;
  ObBackupDest dest;
  char dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  const uint64_t tenant_id = info.status_.tenant_id_;
  ObLogArchiveBackupInfoMgr mgr;
  share::ObLogArchiveBackupInfo stop_info = info;
  stop_info.status_.status_ = ObLogArchiveStatus::STOP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_dst_backup_dest(tenant_id, dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dest.set(dest_str))) {
    LOG_WARN("failed to set backup backup dest", KR(ret));
  } else if (OB_FAIL(get_current_round_copy_id(dest, info.status_.tenant_id_, info.status_.round_, copy_id))) {
    LOG_WARN("failed to get current round copy id", KR(ret), K(info));
  } else if (copy_id < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy id should be greater than 1", K(ret), K(copy_id));
  } else if (FALSE_IT(stop_info.status_.copy_id_ = copy_id)) {
    // do nothing
  } else if (OB_FAIL(dest.get_backup_dest_str(stop_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("get backup dest str failed", KR(ret), K(dest), K(stop_info));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set copy id for log archive backup info mgr", KR(ret));
  } else if (OB_FAIL(mgr.update_log_archive_status_history(
                 *sql_proxy_, stop_info, inner_table_version_, *backup_lease_service_))) {
    LOG_WARN("failed to update log archive status history", KR(ret), K(stop_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_new_backup_backup_dest(const uint64_t tenant_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager mgr;
  char backup_backup_dest_str[common::OB_MAX_URI_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_str, common::OB_MAX_URI_LENGTH))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else if (OB_FAIL(mgr.update_backup_backup_dest(tenant_id, proxy, backup_backup_dest_str))) {
    LOG_WARN("failed to update backup backup dest", KR(ret), K(tenant_id), K(backup_backup_dest_str));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::reset_round_stat_if_dest_changed(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_clear_tenant_resource_if_dropped(tenant_id))) {
    LOG_WARN("failed to clear tenant resource", KR(ret), K(tenant_id));
  }
  return ret;
}

// normal tenant copy id same as sys tenant copy id
// the input tenant_id is not used, only used for log usage
int ObBackupArchiveLogScheduler::get_current_round_copy_id(
    const share::ObBackupDest& backup_dest, const uint64_t tenant_id, const int64_t round, int64_t& copy_id)
{
  int ret = OB_SUCCESS;
  bool has_same = false;
  copy_id = OB_START_COPY_ID;
  int64_t current_max_copy_id = 0;
  ObLogArchiveBackupInfoMgr mgr;
  ObArray<ObLogArchiveBackupInfo> info_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(round));
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set copy id", KR(ret));
  } else if (OB_FAIL(mgr.get_all_same_round_log_archive_infos(*sql_proxy_, OB_SYS_TENANT_ID, round, info_list))) {
    LOG_WARN("failed to get all same round log archive infos", KR(ret), K(tenant_id), K(round));
  } else if (info_list.empty()) {
    copy_id = OB_START_COPY_ID;
    LOG_INFO("info list is empty copy id set to 1", K(backup_dest), K(tenant_id), K(round));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObLogArchiveBackupInfo& info = info_list.at(i);
      ObBackupDest dest;
      if (OB_FAIL(dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest");
      } else if (dest.is_root_path_equal(backup_dest)) {
        copy_id = info.status_.copy_id_;
        has_same = true;
        break;
      }

      if (info.status_.copy_id_ > current_max_copy_id) {
        current_max_copy_id = info.status_.copy_id_;
      }
    }
    if (OB_SUCC(ret) && !has_same) {
      copy_id = current_max_copy_id + 1;
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_has_same_backup_dest(
    const common::ObIArray<share::ObLogArchiveBackupInfo>& info_list, const share::ObBackupDest& backup_dest,
    bool& has_same)
{
  int ret = OB_SUCCESS;
  has_same = false;
  ObBackupDest his_backup_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check has same backup dest get invalid argument", KR(ret), K(backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      his_backup_dest.reset();
      const share::ObLogArchiveBackupInfo& info = info_list.at(i);
      if (OB_FAIL(his_backup_dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(info));
      } else {
        if (backup_dest == his_backup_dest) {
          has_same = true;
          break;
        } else {
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_dest_same_with_gconf(
    const share::ObBackupDest& backup_dest, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  ObBackupDest gconf_backup_dest;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest", KR(ret));
  } else if (0 == strlen(backup_dest_str)) {
    is_same = false;
  } else if (OB_FAIL(gconf_backup_dest.set(backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
  } else if (gconf_backup_dest.is_root_path_equal(backup_dest)) {
    is_same = true;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_bb_dest_same_with_gconf(
    const share::ObBackupBackupPieceJobInfo& job_info, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  ObBackupDest backup_dest;
  char backup_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else if (0 == strlen(backup_backup_dest_str)) {
    is_same = false;
  } else if (OB_FAIL(backup_dest.set(backup_backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_backup_dest_str));
  } else if (job_info.backup_dest_.is_root_path_equal(backup_dest)) {
    is_same = true;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_job_copy_id_level(
    const share::ObBackupBackupPieceJobInfo& job_info, ObBackupBackupCopyIdLevel& copy_id_level)
{
  int ret = OB_SUCCESS;
  copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  bool is_same_with_gconf = false;
  const bool is_tenant_level = job_info.is_tenant_level();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(check_bb_dest_same_with_gconf(job_info, is_same_with_gconf))) {
    LOG_WARN("failed to check dest same with gconf", KR(ret), K(job_info));
  } else {
    if (!is_tenant_level) {
      if (is_same_with_gconf) {
        copy_id_level = OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST;
      } else {
        copy_id_level = OB_BB_COPY_ID_LEVEL_CLUSTER_USER_DEST;
      }
    } else {
      if (is_same_with_gconf) {
        copy_id_level = OB_BB_COPY_ID_LEVEL_TENANT_GCONF_DEST;
      } else {
        copy_id_level = OB_BB_COPY_ID_LEVEL_TENANT_USER_DEST;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_smallest_job_info(ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  job_info.reset();
  int64_t pos = -1;
  int64_t least_job_id = INT64_MAX;
  ObArray<ObBackupBackupPieceJobInfo> jobs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(ObBackupBackupPieceJobOperator::get_all_job_items(*sql_proxy_, jobs))) {
    LOG_WARN("failed to get all job items", KR(ret));
  } else if (jobs.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("jobs is empty", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
      const ObBackupBackupPieceJobInfo& job = jobs.at(i);
      if (job.job_id_ < least_job_id) {
        least_job_id = job.job_id_;
        pos = i;
      }
    }
    if (OB_SUCC(ret)) {
      job_info = jobs.at(pos);
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_backup_backuppiece()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char dst_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObBackupBackupPieceJobInfo job_info;
  ObBackupBackupCopyIdLevel copy_id_level;
  ObBackupDest backup_dest;
  bool need_check_dest_changed = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check_can_do_work", K(ret));
  } else if (OB_FAIL(get_smallest_job_info(job_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("no backup backuppiece job for now", KR(ret));
    } else {
      LOG_WARN("failed to get smallest unfinished job info", KR(ret), K(job_info));
    }
  } else if (OB_FAIL(get_dst_backup_dest(OB_SYS_TENANT_ID, dst_backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to set backup backup dest if needed", KR(ret));
  } else if (0 == strlen(dst_backup_dest)) {
    need_check_dest_changed = false;
  } else if (OB_FAIL(backup_dest.set(dst_backup_dest))) {
    LOG_WARN("failed to set backup dest", KR(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
  } else if (need_check_dest_changed &&
             OB_FAIL(may_do_backup_backup_dest_changed_in_piece_mode(copy_id_level, backup_dest))) {
    LOG_WARN("failed to check backup dest in piece mode", KR(ret), K(copy_id_level), K(backup_dest));
  } else if (OB_FAIL(backup_piece_do_with_status(job_info))) {
    if (is_fatal_error(ret)) {
      if (OB_SUCCESS != (tmp_ret = on_fatal_error(job_info, ret))) {
        LOG_WARN("failed to do on fatal error", KR(ret), KR(tmp_ret), K(job_info));
      }
    } else {
      LOG_WARN("failed to do with status", KR(ret), K(job_info));
    }
  } else {
    LOG_INFO("backup piece do with status", K(job_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::may_do_backup_backup_dest_changed_in_piece_mode(
    const ObBackupBackupCopyIdLevel& copy_id_level, const share::ObBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObLogArchiveBackupInfoMgr mgr;
  bool backup_backup_dest_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST != copy_id_level) {
    // do nothing
  } else if (OB_FAIL(check_backup_backup_dest_changed(OB_SYS_TENANT_ID, backup_backup_dest_changed))) {
    LOG_WARN("failed to check backup backup dest changed", KR(ret));
  } else if (!backup_backup_dest_changed) {
    LOG_INFO("backup backup dest do not change");
  } else if (OB_FAIL(mgr.set_backup_backup())) {
    LOG_WARN("failed to set backup backup", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    ObArray<ObLogArchiveBackupInfo> infos;
    if (OB_FAIL(mgr.get_all_backup_backup_log_archive_status(trans, true, infos))) {
      LOG_WARN("failed to get all backup backup log archive status", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
        ObLogArchiveBackupInfo& info = infos.at(i);
        info.status_.status_ = ObLogArchiveStatus::STOP;
        if (OB_FAIL(backup_dest.get_backup_dest_str(info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
          LOG_WARN("failed to get backup dest str", KR(ret));
        } else if (OB_FAIL(mgr.update_log_archive_status_history(
                       trans, info, inner_table_version_, *backup_lease_service_))) {
          LOG_WARN("failed to update log archive status history", KR(ret), K(info));
        } else if (OB_FAIL(do_clear_tenant_resource_if_dropped(info.status_.tenant_id_))) {
          LOG_WARN("failed to do clear tenant resource", KR(ret), K(info));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(mgr.delete_all_backup_backup_log_archive_info(trans))) {
          LOG_WARN("failed to delete all backup backup log archive info", KR(ret));
        } else if (OB_FAIL(update_new_backup_backup_dest(OB_SYS_TENANT_ID, trans))) {
          LOG_WARN("failed to update new backup backup dest", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::backup_piece_do_with_status(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else {
    switch (job_info.status_) {
      case ObBackupBackupPieceJobInfo::SCHEDULE: {
        if (OB_FAIL(do_schedule(job_info))) {
          LOG_WARN("failed to do schedule", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupPieceJobInfo::DOING: {
        if (OB_FAIL(do_copy(job_info))) {
          LOG_WARN("failed to do copy", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupPieceJobInfo::CANCEL: {
        if (OB_FAIL(do_cancel(job_info))) {
          LOG_WARN("failed to do cancel", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupPieceJobInfo::FINISH: {
        if (OB_FAIL(do_finish(job_info))) {
          LOG_WARN("failed to do finish", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupPieceJobInfo::MAX: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("job info status is not valid", KR(ret));
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_backup_piece_tenant_list(const share::ObBackupBackupPieceJobInfo& job_info,
    const int64_t piece_id, const int64_t copy_id, common::ObArray<uint64_t>& tenant_list)
{
  int ret = OB_SUCCESS;
  tenant_list.reset();
  ObLogArchiveBackupInfoMgr mgr;
  ObArray<ObBackupPieceInfo> info_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid() || piece_id <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_info), K(piece_id), K(copy_id));
  } else {
    if (OB_SYS_TENANT_ID == job_info.tenant_id_) {
      if (OB_FAIL(mgr.get_backup_piece_tenant_list(*sql_proxy_, piece_id, copy_id, info_list))) {
        LOG_WARN("failed to get backup piece tenant list", KR(ret), K(piece_id), K(copy_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
          const ObBackupPieceInfo& info = info_list.at(i);
          const uint64_t tenant_id = info.key_.tenant_id_;
          if (OB_FAIL(tenant_list.push_back(tenant_id))) {
            LOG_WARN("failed to push back tenant list", KR(ret), K(tenant_id));
          }
        }
      }
    } else {
      if (OB_FAIL(tenant_list.push_back(job_info.tenant_id_))) {
        LOG_WARN("failed to push back tenant list", KR(ret), K(job_info));
      }
    }

    if (OB_SUCC(ret)) {
      std::sort(tenant_list.begin(), tenant_list.end());
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_backup_piece_job(const ObBackupBackupPieceJobInfo& new_job_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!new_job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(new_job_info));
  } else if (OB_FAIL(ObBackupBackupPieceJobOperator::report_job_item(new_job_info, *sql_proxy_))) {
    LOG_WARN("failed to report job item", KR(ret), K(new_job_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_schedule(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_schedule = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_UNLIKELY(0 != tenant_queue_map_.size())) {
    clear_memory_resource_if_stopped();
    LOG_WARN("tenant queue map not empty before do schedule", "map_size", tenant_queue_map_.size());
  } else {
    const int64_t piece_id = job_info.piece_id_;
    if (0 == piece_id) {
      if (OB_FAIL(inner_do_schedule_all(job_info))) {
        LOG_WARN("failed to inner do schedule all", KR(ret), K(job_info));
      }
    } else if (piece_id > 0) {
      if (OB_FAIL(inner_do_schedule_single(job_info))) {
        LOG_WARN("failed to inner do schedule single", KR(ret), K(job_info));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("piece id is not valid", KR(ret));
    }

    DEBUG_SYNC(BACKUP_BACKUPPIECE_AFTER_SCHEDULE);
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BACKUP_BACKUPPIECE_DO_SCHEDULE) OB_SUCCESS;
    }
#endif

    if (is_fatal_error(ret)) {
      if (OB_SUCCESS != (tmp_ret = on_fatal_error(job_info, ret))) {
        LOG_WARN("failed to do on fatal error", KR(ret), KR(tmp_ret), K(job_info));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_schedule = false;
      } else {
        LOG_WARN("failed to inner do schedule single", KR(ret), K(job_info));
      }
    }

    if (OB_SUCC(ret)) {
      ObBackupBackupPieceJobInfo new_job_info = job_info;
      int64_t task_count = 0;
      if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_job_task_count(job_info, *sql_proxy_, task_count))) {
        LOG_WARN("failed to get job task count", KR(ret), K(job_info));
      }
      new_job_info.status_ = 0 == task_count ? ObBackupBackupPieceJobInfo::FINISH : ObBackupBackupPieceJobInfo::DOING;
      if (need_schedule) {
        new_job_info.result_ = 0 == task_count ? OB_BACKUP_BACKUP_CAN_NOT_START : OB_SUCCESS;
        new_job_info.comment_ = 0 == task_count ? OB_BACKUP_BACKUP_CAN_NOT_START__USER_ERROR_MSG : "";
      } else {
        new_job_info.result_ = OB_BACKUP_BACKUP_CAN_NOT_START;
        new_job_info.comment_ = OB_BACKUP_BACKUP_CAN_NOT_START__USER_ERROR_MSG;
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(update_backup_piece_job(new_job_info))) {
        LOG_WARN("failed to update backup piece job status", KR(ret), K(new_job_info));
      } else {
        wakeup();
        ROOTSERVICE_EVENT_ADD("backup_backuppiece",
            "start job",
            "tenant_id",
            new_job_info.tenant_id_,
            "job_id",
            new_job_info.job_id_,
            "backup_piece_id",
            new_job_info.piece_id_,
            "job_status",
            new_job_info.get_status_str());
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_origin_backup_in_current_gconf(
    const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, bool& in_current_gconf)
{
  int ret = OB_SUCCESS;
  in_current_gconf = false;
  ObBackupDest gconf_backup_dest;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObBackupPieceInfoKey piece_key;
  ObLogArchiveBackupInfoMgr mgr;
  piece_key.incarnation_ = OB_START_INCARNATION;
  piece_key.tenant_id_ = tenant_id;
  piece_key.round_id_ = round_id;
  piece_key.backup_piece_id_ = piece_id;
  piece_key.copy_id_ = 0;  // origin
  const bool for_update = false;
  ObBackupPieceInfo piece;
  ObBackupDest piece_backup_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (round_id < 0 || piece_id <= 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(round_id), K(piece_id), K(tenant_id));
  } else if (OB_FAIL(mgr.get_backup_piece(*sql_proxy_, for_update, piece_key, piece))) {
    LOG_WARN("failed to get bakup piece", KR(ret), K(piece_key));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else if (OB_FAIL(gconf_backup_dest.set(backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
  } else if (OB_FAIL(piece.get_backup_dest(piece_backup_dest))) {
    LOG_WARN("failed to get backup dest", KR(ret), K(piece));
  } else if (gconf_backup_dest.is_root_path_equal(piece_backup_dest)) {
    in_current_gconf = true;
  } else {
    LOG_INFO("piece dest not same with gconf dest", K(piece_backup_dest), K(gconf_backup_dest));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::inner_do_schedule_all(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  int64_t min_backup_piece_id = 0;
  int64_t max_backup_piece_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_job_min_backup_piece_id(job_info, min_backup_piece_id))) {
    LOG_WARN("failed to get min frozen backup piece id", KR(ret), K(job_info));
  } else if (OB_FAIL(get_job_max_backup_piece_id(job_info, max_backup_piece_id))) {
    LOG_WARN("failed to get max frozen backup piece id", KR(ret), K(job_info));
  } else {
    for (int64_t i = min_backup_piece_id; OB_SUCC(ret) && i <= max_backup_piece_id; ++i) {
      const int64_t piece_id = i;
      const int64_t origin_copy_id = 0;
      ObArray<uint64_t> piece_tenant_list;
      if (OB_FAIL(get_backup_piece_tenant_list(job_info, piece_id, origin_copy_id, piece_tenant_list))) {
        LOG_WARN("failed to get backup piece tenant list", KR(ret), K(piece_id), K(origin_copy_id));
      } else if (OB_FAIL(inner_do_schedule_tenant_piece_task(job_info, piece_tenant_list, piece_id))) {
        if (OB_BACKUP_BACKUP_REACH_MAX_BACKUP_TIMES == ret) {
          ret = OB_SUCCESS;  // this backup piece is already exist
        } else {
          LOG_WARN("failed to inner schedule tenant piece task", KR(ret), K(job_info), K(piece_tenant_list));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_piece_id_continuous(
    const int64_t src_min_piece_id, const int64_t dst_max_piece_id, bool& is_continuous)
{
  int ret = OB_SUCCESS;
  is_continuous = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (0 == src_min_piece_id || 0 == dst_max_piece_id) {
    // do nothing
  } else if (src_min_piece_id - dst_max_piece_id > 1) {
    is_continuous = false;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_job_min_backup_piece_id(
    const share::ObBackupBackupPieceJobInfo& job_info, int64_t& min_backup_piece_id)
{
  int ret = OB_SUCCESS;
  min_backup_piece_id = 0;
  const uint64_t tenant_id = job_info.tenant_id_;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupBackupCopyIdLevel copy_id_level;
  int64_t current_max_piece_id = 0;
  int64_t min_available_piece_id = 0;
  bool is_continuous = true;
  ObBackupDest gconf_backup_dest;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_info));
  } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
  } else if (OB_FAIL(mgr.get_max_backup_piece_id_in_backup_dest(
                 copy_id_level, job_info.backup_dest_, tenant_id, *sql_proxy_, current_max_piece_id))) {
    LOG_WARN("failed to get max backup piece id in backup dest", KR(ret), K(job_info), K(tenant_id));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else if (OB_FAIL(gconf_backup_dest.set(backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
  } else if (OB_FAIL(mgr.get_min_available_backup_piece_id_in_backup_dest(gconf_backup_dest,
                 OB_START_INCARNATION,
                 tenant_id,
                 0 /*copy_id*/,
                 *sql_proxy_,
                 min_available_piece_id))) {
    LOG_WARN("failed to get min available backup piece id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_backup_piece_id_continuous(min_available_piece_id, current_max_piece_id, is_continuous))) {
    LOG_WARN("failed to check backup piece id continous", KR(ret), K(min_available_piece_id), K(current_max_piece_id));
  } else if (!is_continuous) {
    ret = OB_LOG_ARCHIVE_INTERRUPTED;
    LOG_ERROR("the archive log is interrupted", KR(ret), K(min_available_piece_id), K(current_max_piece_id));
  } else {
    min_backup_piece_id = std::max(current_max_piece_id + 1, min_available_piece_id);
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_job_max_backup_piece_id(
    const share::ObBackupBackupPieceJobInfo& job_info, int64_t& max_backup_piece_id)
{
  int ret = OB_SUCCESS;
  max_backup_piece_id = 0;
  ObLogArchiveBackupInfoMgr mgr;
  share::ObBackupPieceInfo piece_info;
  const uint64_t tenant_id = job_info.tenant_id_;
  const bool with_active_piece = job_info.with_active_piece();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_info));
  } else if (!with_active_piece && OB_FAIL(mgr.get_max_frozen_backup_piece(
                                       *sql_proxy_, OB_START_INCARNATION, tenant_id, 0 /*copy_id*/, piece_info))) {
    LOG_WARN("failed to get max frozen backup piece", KR(ret), K(tenant_id));
  } else if (with_active_piece && OB_FAIL(mgr.get_max_backup_piece(
                                      *sql_proxy_, OB_START_INCARNATION, tenant_id, 0 /*copy_id*/, piece_info))) {
    LOG_WARN("failed to get max frozen backup piece", KR(ret), K(tenant_id));
  } else {
    max_backup_piece_id = piece_info.key_.backup_piece_id_;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::inner_do_schedule_single(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  const int64_t origin_copy_id = 0;
  const int64_t piece_id = job_info.piece_id_;
  ObArray<uint64_t> tenant_list;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup piece job is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_backup_piece_tenant_list(job_info, piece_id, origin_copy_id, tenant_list))) {
    LOG_WARN("failed to get backup piece tenant list", KR(ret), K(piece_id), K(origin_copy_id));
  } else if (tenant_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("do tenant exist for backup piece", KR(ret), K(job_info), K(piece_id));
  } else if (OB_FAIL(inner_do_schedule_tenant_piece_task(job_info, tenant_list, piece_id))) {
    LOG_WARN("failed to inner do schedule tenant piece task", KR(ret), K(job_info), K(tenant_list), K(piece_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_single_piece_is_valid(
    const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id, const int64_t piece_id, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const uint64_t tenant_id = job_info.tenant_id_;
  const bool with_active = job_info.with_active_piece();
  ObBackupPieceInfoKey piece_key;
  piece_key.incarnation_ = OB_START_INCARNATION;
  piece_key.tenant_id_ = tenant_id;
  piece_key.round_id_ = round_id;
  piece_key.backup_piece_id_ = piece_id;
  piece_key.copy_id_ = 0;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPieceInfo piece;
  bool has_incomplete_before = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (0 == job_info.piece_id_) {
    // do nothing
  } else if (!job_info.is_valid() || round_id <= 0 || piece_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup piece job is not valid", KR(ret), K(job_info), K(round_id), K(piece_id));
  } else if (OB_FAIL(mgr.get_backup_piece(*sql_proxy_, false /*for_update*/, piece_key, piece))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_valid = false;
    } else {
      LOG_WARN("failed to get backup piece", KR(ret), K(piece_key));
    }
  } else if (!with_active && ObBackupPieceStatus::BACKUP_PIECE_FROZEN != piece.status_) {
    is_valid = false;
    LOG_WARN("the piece is not frozen", K(piece), K(job_info));
  } else if (OB_FAIL(mgr.check_has_incomplete_file_info_smaller_than_backup_piece_id(OB_START_INCARNATION,
                 tenant_id,
                 piece_id,
                 job_info.backup_dest_,
                 *sql_proxy_,
                 has_incomplete_before))) {
    LOG_WARN("failed to check has incomplete file info smaller than backup piece id", KR(ret), K(job_info));
  } else if (has_incomplete_before) {
    ret = OB_LOG_ARCHIVE_INTERRUPTED;
    LOG_ERROR("has incomplete backup piece before", KR(ret), K(job_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_backup_has_been_backed_up_before(
    const share::ObBackupBackupPieceJobInfo& info, const uint64_t tenant_id, const int64_t round_id,
    const int64_t piece_id, bool& backed_up_before)
{
  int ret = OB_SUCCESS;
  backed_up_before = false;
  ObLogArchiveBackupInfoMgr mgr;
  ObArray<ObBackupPieceInfo> piece_list;
  ObBackupBackupCopyIdLevel copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_job_copy_id_level(info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(info));
  } else if (OB_FAIL(mgr.get_backup_piece_copy_list(
                 OB_START_INCARNATION, tenant_id, round_id, piece_id, copy_id_level, *sql_proxy_, piece_list))) {
    LOG_WARN("failed to get backup piece copy list", KR(ret), K(tenant_id), K(round_id), K(piece_id), K(copy_id_level));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); ++i) {
      ObBackupDest backup_dest;
      const ObBackupPieceInfo& piece = piece_list.at(i);
      if (OB_FAIL(piece.get_backup_dest(backup_dest))) {
        LOG_WARN("failed to get backup dest", KR(ret), K(piece));
      } else if (info.backup_dest_.is_root_path_equal(backup_dest)) {
        switch (piece.file_status_) {
          case ObBackupFileStatus::BACKUP_FILE_COPYING:
          case ObBackupFileStatus::BACKUP_FILE_INCOMPLETE: {
            backed_up_before = false;
            break;
          }
          case ObBackupFileStatus::BACKUP_FILE_AVAILABLE:
          case ObBackupFileStatus::BACKUP_FILE_DELETING:
          case ObBackupFileStatus::BACKUP_FILE_DELETED: {
            backed_up_before = true;
            break;
          }
          case ObBackupFileStatus::BACKUP_FILE_EXPIRED:
          case ObBackupFileStatus::BACKUP_FILE_BROKEN: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no such backup piece info at this time", KR(ret), K(piece));
            break;
          }
          default: {
            ret = OB_ERR_SYS;
            LOG_ERROR("invalid status", K(ret), K(piece));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::inner_do_schedule_tenant_piece_task(const share::ObBackupBackupPieceJobInfo& job_info,
    const common::ObIArray<uint64_t>& tenant_list, const int64_t piece_id)
{
  int ret = OB_SUCCESS;
  const int64_t origin_copy_id = 0;
  int64_t copy_id = -1;
  int64_t round_id = -1;
  ObArray<ObBackupPieceInfo> sys_piece_list;
  bool backed_up_before = false;
  ObBackupBackupCopyIdLevel copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  bool in_current_gconf = false;
  bool single_piece_is_valid = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_piece_info_list(job_info.tenant_id_, origin_copy_id, sys_piece_list))) {
    LOG_WARN("failed to get sys piece info list", KR(ret), K(origin_copy_id));
  } else if (OB_FAIL(get_piece_round_id(sys_piece_list, piece_id, round_id))) {
    LOG_WARN("failed to get piece round id", KR(ret), K(sys_piece_list), K(piece_id));
  } else if (job_info.piece_id_ > 0 &&
             OB_FAIL(check_single_piece_is_valid(job_info, round_id, piece_id, single_piece_is_valid))) {
    LOG_WARN("failed to check single piece is valid", KR(ret), K(job_info), K(round_id), K(piece_id));
  } else if (!single_piece_is_valid) {
    // do nothing
  } else if (OB_FAIL(check_origin_backup_in_current_gconf(OB_SYS_TENANT_ID, round_id, piece_id, in_current_gconf))) {
    LOG_WARN("failed to check origin backup in current gconf", KR(ret), K(round_id), K(piece_id));
  } else if (!in_current_gconf) {
    // do nothing
  } else if (OB_FAIL(create_mount_file(job_info.backup_dest_, round_id))) {
    LOG_WARN("failed to create mount file", KR(ret), K(job_info), K(round_id));
  } else if (OB_FAIL(check_backup_backup_has_been_backed_up_before(
                 job_info, OB_SYS_TENANT_ID, round_id, piece_id, backed_up_before))) {
    LOG_WARN("failed to check backup backup has been backed up before", KR(ret), K(job_info), K(round_id), K(piece_id));
  } else if (backed_up_before) {
    LOG_INFO("piece has been backed up before", K(job_info), K(piece_id), K(tenant_list));
  } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
  } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST != copy_id_level &&
             OB_FAIL(calc_backup_piece_copy_id(job_info, round_id, piece_id, copy_id))) {
    LOG_WARN("failed to get backup piece copy id", KR(ret), K(job_info), K(piece_id));
  } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level &&
             OB_FAIL(get_current_round_copy_id(job_info.backup_dest_, OB_SYS_TENANT_ID, round_id, copy_id))) {
    LOG_WARN("failed to get current round copy id", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const uint64_t tenant_id = tenant_list.at(i);
      ObBackupPieceInfo piece_info;
      ObBackupBackupPieceTaskInfo task_info;
      ObBackupPieceInfo dst_piece_info;
      if (OB_FAIL(get_backup_piece_info(
              OB_START_INCARNATION, tenant_id, round_id, piece_id, origin_copy_id, *sql_proxy_, piece_info))) {
        LOG_WARN("failed to get backup backup piece files info", KR(ret), K(tenant_id), K(piece_id));
      } else if (OB_FAIL(build_backup_piece_task_info(
                     job_info, tenant_id, piece_info.key_.round_id_, piece_id, copy_id, task_info))) {
        LOG_WARN("failed to build backup piece task info", KR(ret), K(job_info), K(tenant_id), K(piece_id), K(copy_id));
      } else if (OB_FAIL(build_backup_piece_info(piece_info,
                     ObBackupFileStatus::BACKUP_FILE_COPYING,
                     job_info.backup_dest_,
                     copy_id,
                     dst_piece_info))) {
        LOG_WARN("failed to build backup piece files info", KR(ret), K(piece_info), K(job_info), K(copy_id));
      } else if (OB_FAIL(insert_backup_backuppiece_task(task_info))) {
        LOG_WARN("failed to insert backup backuppiece task", KR(ret), K(task_info));
      } else if (OB_FAIL(update_backup_piece_info(dst_piece_info, *sql_proxy_))) {
        LOG_WARN("failed to insert backup piece files info", KR(ret), K(dst_piece_info));
      } else if (OB_FAIL(do_extern_backup_piece_info(
                     job_info, ObBackupFileStatus::BACKUP_FILE_COPYING, tenant_id, round_id, piece_id, copy_id))) {
        LOG_WARN("failed to do extern backup piece info",
            KR(ret),
            K(job_info),
            K(tenant_id),
            K(round_id),
            K(piece_id),
            K(copy_id));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::calc_backup_piece_copy_id(
    const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id, const int64_t piece_id, int64_t& copy_id)
{
  int ret = OB_SUCCESS;
  copy_id = -1;
  ObLogArchiveBackupInfoMgr mgr;
  const int64_t max_backup_times = job_info.max_backup_times_;
  ObArray<ObBackupPieceInfo> info_list;
  int64_t max_copy_id = 0;
  ObBackupBackupCopyIdLevel copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  const bool is_tenant_level = job_info.is_tenant_level();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid() || piece_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info), K(piece_id));
  } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
  } else if (OB_FAIL(mgr.get_backup_piece_copy_list(OB_START_INCARNATION,
                 job_info.tenant_id_,
                 round_id,
                 piece_id,
                 copy_id_level,
                 *sql_proxy_,
                 info_list))) {
    LOG_WARN("failed to get backup piece copy list", KR(ret), K(piece_id));
  } else if (info_list.empty()) {
    if (OB_FAIL(get_backup_backup_start_copy_id(copy_id_level, copy_id))) {
      LOG_WARN("failed to get backup backup start copy id", KR(ret), K(copy_id_level));
    } else {
      LOG_INFO("info list is empty", K(is_tenant_level), K(round_id), K(piece_id), K(copy_id));
    }
  } else {
    bool has_same = false;
    int64_t cluster_level_copy_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
      const ObBackupPieceInfo& piece = info_list.at(i);
      ObBackupDest backup_dest;
      if (OB_FAIL(backup_dest.set(piece.backup_dest_.ptr()))) {
        LOG_WARN("failed to set backup dest", KR(ret));
      } else if (backup_dest.is_root_path_equal(job_info.backup_dest_) &&
                 (ObBackupFileStatus::BACKUP_FILE_INCOMPLETE == piece.file_status_ ||
                     ObBackupFileStatus::BACKUP_FILE_COPYING == piece.file_status_)) {
        copy_id = piece.key_.copy_id_;
        has_same = true;
      }
      if (piece.key_.copy_id_ > max_copy_id) {
        max_copy_id = piece.key_.copy_id_;
      }
      if (ObBackupFileStatus::BACKUP_FILE_INCOMPLETE != piece.file_status_ &&
          ObBackupFileStatus::BACKUP_FILE_COPYING != piece.file_status_ && !is_tenant_level) {
        ++cluster_level_copy_count;
      }
    }
    if (OB_SUCC(ret) && !has_same) {
      if (max_backup_times >= 0 && !is_tenant_level && cluster_level_copy_count >= max_backup_times) {
        ret = OB_BACKUP_BACKUP_REACH_MAX_BACKUP_TIMES;
        LOG_WARN("reached max backup backuppiece time limit", KR(ret), K(job_info), K(piece_id));
      } else {
        copy_id = max_copy_id + 1;
        if (!is_tenant_level && copy_id >= OB_TENANT_GCONF_DEST_START_COPY_ID) {
          ret = OB_BACKUP_CAN_NOT_START;
          LOG_ERROR("can not start backup backup", K(round_id), K(piece_id), K(copy_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::build_backup_piece_task_info(const ObBackupBackupPieceJobInfo& job_info,
    const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, const int64_t copy_id,
    ObBackupBackupPieceTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid() || OB_INVALID_ID == tenant_id || round_id < 0 || piece_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info), K(tenant_id), K(piece_id));
  } else {
    task_info.job_id_ = job_info.job_id_;
    task_info.incarnation_ = job_info.incarnation_;
    task_info.tenant_id_ = tenant_id;
    task_info.round_id_ = round_id;
    task_info.piece_id_ = piece_id;
    task_info.copy_id_ = copy_id;
    task_info.task_status_ = ObBackupBackupPieceTaskInfo::DOING;
    task_info.backup_dest_ = job_info.backup_dest_;
    task_info.start_ts_ = ObTimeUtility::current_time();
    task_info.end_ts_ = 0;
    task_info.result_ = 0;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::build_backup_piece_info(const share::ObBackupPieceInfo& src_info,
    const share::ObBackupFileStatus::STATUS file_status, const share::ObBackupDest& backup_dest, const int64_t copy_id,
    share::ObBackupPieceInfo& dst_info)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_BACKUP_DEST_LENGTH] = "";
  dst_info = src_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!src_info.is_valid() || !backup_dest.is_valid() || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(src_info), K(backup_dest), K(copy_id));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(buf, sizeof(buf)))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(dst_info.backup_dest_.assign(buf))) {
    LOG_WARN("failed to copy backup_dest", K(ret), K(buf));
  } else {
    dst_info.key_.copy_id_ = copy_id;
    dst_info.file_status_ = file_status;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_copy(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  bool all_finished = false;
  int result = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  const int64_t job_id = job_info.job_id_;
  const int64_t origin_copy_id = 0;
  ObBackupBackupPieceTaskInfo cur_task;
  ObArray<ObBackupPieceInfo> sys_piece_list;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid() || ObBackupBackupPieceJobInfo::DOING != job_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_smallest_doing_backup_piece_task(job_id, job_info.tenant_id_, cur_task))) {
    LOG_WARN("failed to get smallest doing backup piece task", KR(ret), K(job_info));
  } else if (OB_FAIL(get_backup_piece_tenant_list(job_info, cur_task.piece_id_, origin_copy_id, tenant_ids))) {
    LOG_WARN("failed to get backup piece tenant list", KR(ret), K(cur_task), K(origin_copy_id));
  } else if (OB_FAIL(prepare_tenant_piece_task_if_needed(tenant_ids, cur_task.piece_id_, job_id))) {
    LOG_WARN("failed to prepare tenant piece task if needed", KR(ret), K(tenant_ids), K(cur_task.piece_id_));
  } else if (OB_FAIL(get_piece_info_list(job_info.tenant_id_, origin_copy_id, sys_piece_list))) {
    LOG_WARN("failed to get sys piece info list", KR(ret), K(origin_copy_id));
  } else {
    const int64_t cur_piece_id = cur_task.piece_id_;
    int64_t round_id = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObBackupBackupPieceTaskInfo smallest_task;
      ObBackupPieceInfo src_file_info;
      ObBackupDest src_backup_dest;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(get_piece_round_id(sys_piece_list, cur_piece_id, round_id))) {
        LOG_WARN("failed to get piece round id", KR(ret), K(sys_piece_list), K(cur_piece_id));
      } else if (OB_FAIL(get_backup_piece_info(OB_START_INCARNATION,
                     tenant_id,
                     round_id,
                     cur_piece_id,
                     origin_copy_id,
                     *sql_proxy_,
                     src_file_info))) {
        LOG_WARN("failed to get backup piece files info", KR(ret), K(tenant_id), K(origin_copy_id));
      } else if (ObBackupPieceStatus::BACKUP_PIECE_FREEZING == src_file_info.status_) {
        ret = OB_EAGAIN;
        LOG_WARN("the backup piece is now freezing, do it later", KR(ret), K(job_info), K(src_file_info));
      } else if (OB_FAIL(get_smallest_doing_backup_piece_task(job_id, tenant_id, smallest_task))) {
        LOG_WARN("failed to get smallest doing backup piece task", KR(ret), K(job_id), K(tenant_id));
      } else if (ObBackupBackupPieceTaskInfo::FINISH == smallest_task.task_status_) {
        continue;
      } else if (OB_FAIL(src_backup_dest.set(src_file_info.backup_dest_.ptr()))) {
        LOG_WARN("failed to set src backup dest", KR(ret), K(src_file_info));
      } else {
        const int64_t round_id = src_file_info.key_.round_id_;
        const int64_t create_date = src_file_info.create_date_;
        if (OB_FAIL(do_tenant_schedule(
                tenant_id, round_id, cur_piece_id, create_date, job_id, src_backup_dest, smallest_task.backup_dest_))) {
          if (OB_EAGAIN == ret) {
            LOG_INFO("no server currently available", KR(ret), K(tenant_id));
            ret = OB_SUCCESS;
            break;
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(do_if_pg_task_finished(round_id, tenant_id, job_info.backup_dest_))) {
        LOG_WARN("failed to do set tenant need fetch new", KR(ret), K(round_id), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      bool finished = false;
      bool is_available = true;
      if (OB_FAIL(check_src_data_available(job_info, round_id, cur_piece_id, is_available))) {
        LOG_WARN("failed to check src data available", KR(ret), K(round_id), K(cur_piece_id));
      } else {
        if (is_available) {
          if (OB_FAIL(check_cur_piece_tenant_tasks_finished(cur_piece_id, job_info, tenant_ids, finished))) {
            LOG_WARN("failed to check cur piece tenant tasks finished",
                KR(ret),
                K(cur_piece_id),
                K(job_info),
                K(tenant_ids));
          } else if (!finished) {
            // do nothing
          } else if (OB_FAIL(do_cur_piece_tenant_tasks_finished(round_id, cur_piece_id, job_info, tenant_ids))) {
            LOG_WARN(
                "failed to do cur piece tenant tasks finished", KR(ret), K(cur_piece_id), K(job_info), K(tenant_ids));
          } else {
            wakeup();
          }
        } else {
          result = OB_DATA_SOURCE_NOT_EXIST;
          if (OB_FAIL(update_all_backup_piece_task_finished(job_info, result))) {
            LOG_WARN("failed to update all backup piece task", KR(ret), K(job_info));
          } else {
            ROOTSERVICE_EVENT_ADD("backup_backuppiece",
                "src data is not available",
                "tenant_id",
                job_info.tenant_id_,
                "job_id",
                job_info.job_id_,
                "backup_piece_id",
                job_info.piece_id_,
                "job_status",
                job_info.get_status_str());
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObBackupBackupPieceJobInfo new_job_info = job_info;
    new_job_info.status_ = ObBackupBackupPieceJobInfo::FINISH;
    new_job_info.result_ = result;
    if (OB_FAIL(check_all_backup_piece_task_finished(job_info, all_finished))) {
      LOG_WARN("failed to check all backup piece task finished", KR(ret));
    } else if (!all_finished) {
      // do nothing if not all finished
    } else if (OB_FAIL(update_backup_piece_job(new_job_info))) {
      LOG_WARN("failed to update backup piece job status", KR(ret), K(new_job_info));
    } else {
      wakeup();
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_cancel(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = job_info.job_id_;
  ObArray<ObBackupBackupPieceTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_all_task_items(job_id, *sql_proxy_, task_infos))) {
    LOG_WARN("failed to get all task items", KR(ret), K(job_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObBackupBackupPieceTaskInfo& task_info = task_infos.at(i);
      task_info.task_status_ = ObBackupBackupPieceTaskInfo::FINISH;
      task_info.result_ = OB_CANCELED;
      if (OB_FAIL(ObBackupBackupPieceTaskOperator::report_task_item(task_info, *sql_proxy_))) {
        LOG_WARN("failed to do tenant task", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObBackupBackupPieceJobInfo new_job_info = job_info;
      new_job_info.status_ = ObBackupBackupPieceJobInfo::FINISH;
      new_job_info.result_ = OB_CANCELED;
      if (OB_FAIL(ObBackupBackupPieceJobOperator::report_job_item(new_job_info, *sql_proxy_))) {
        LOG_WARN("failed to update job status", KR(ret), K(job_id), K(new_job_info));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::prepare_tenant_piece_task_if_needed(
    const common::ObIArray<uint64_t>& tenant_ids, const int64_t piece_id, const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObMutex> guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (piece_id <= 0 || job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(piece_id), K(job_id));
  } else if (tenant_ids.empty()) {
    ret = OB_EAGAIN;
    LOG_WARN("no tenant do be prepared", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      TenantTaskQueue* queue_ptr = NULL;
      int hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(alloc_tenant_queue(tenant_id, queue_ptr))) {
          LOG_WARN("failed to alloc tenant task queue", KR(ret), K(tenant_id));
        } else {
          queue_ptr->piece_id_ = piece_id;
          queue_ptr->job_id_ = job_id;
          queue_ptr->pushed_before_ = false;
          if (OB_FAIL(queue_ptr->pg_map_.create(MAX_BUCKET_NUM, "BB_CLOG_TENANT", "BB_CLOG_NODE"))) {
            LOG_WARN("failed to create pg map", KR(ret));
          } else if (OB_FAIL(tenant_queue_map_.set_refactored(tenant_id, queue_ptr))) {
            LOG_WARN("failed to set map", KR(ret), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_cur_piece_tenant_tasks_finished(const int64_t piece_id,
    const share::ObBackupBackupPieceJobInfo& job_info, const common::ObIArray<uint64_t>& tenant_ids, bool& all_finished)
{
  int ret = OB_SUCCESS;
  all_finished = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (piece_id <= 0 || !job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(job_info), K(piece_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      bool finished = false;
      if (tenant_id == OB_SYS_TENANT_ID) {
        // do nothing
      } else if (OB_FAIL(check_piece_task_finished(job_info, tenant_id, piece_id, finished))) {
        LOG_WARN("failed to check piece task finished", KR(ret), K(job_info), K(tenant_id));
      } else if (!finished) {
        all_finished = false;
        LOG_INFO("tenant task is not finished", K(job_info), K(tenant_id), K(piece_id));
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_cur_piece_tenant_tasks_finished(const int64_t round_id, const int64_t piece_id,
    const share::ObBackupBackupPieceJobInfo& job_info, const common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (piece_id <= 0 || !job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(job_info), K(piece_id));
  } else if (OB_FAIL(tenant_list.assign(tenant_ids))) {
    LOG_WARN("failed to assign tenant list", KR(ret));
  } else {
    std::sort(tenant_list.begin(), tenant_list.end());
    for (int64_t i = tenant_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const uint64_t tenant_id = tenant_list.at(i);
      if (OB_FAIL(inner_do_cur_piece_tenant_task_finished(job_info, round_id, piece_id, tenant_id))) {
        LOG_WARN("failed to inner do cur piece tenant task finished",
            KR(ret),
            K(job_info),
            K(round_id),
            K(piece_id),
            K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      clear_memory_resource_if_stopped();
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::inner_do_cur_piece_tenant_task_finished(
    const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id, const int64_t piece_id,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t MAX_EXECUTE_TIMEOUT_US = 60 * 1000 * 1000L;
  int64_t stmt_timeout = MAX_EXECUTE_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObBackupBackupPieceTaskInfo task_info;
  ObBackupPieceInfo piece_info;
  bool is_last_piece = false;
  ObBackupPieceInfo dst_piece_info;
  ObBackupBackupCopyIdLevel copy_id_level;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (round_id <= 0 || piece_id <= 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(round_id), K(piece_id), K(tenant_id));
  } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("failed to set trx timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("failed to set timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level &&
        OB_FAIL(check_backup_info(tenant_id, job_info.backup_dest_))) {
      LOG_WARN("failed to check backup info", KR(ret), K(job_info), K(tenant_id));
    } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_task_item(
                   job_info.job_id_, tenant_id, piece_id, trans, task_info))) {
      LOG_WARN("failed to get task item", KR(ret), K(job_info), K(tenant_id));
    } else if (OB_FAIL(update_backup_piece_task_finish(tenant_id, job_info.job_id_, piece_id, trans))) {
      LOG_WARN("failed to update backup piece task finish", KR(ret), K(tenant_id), K(job_info), K(piece_id));
    } else if (OB_FAIL(do_extern_backup_piece_info(job_info,
                   ObBackupFileStatus::BACKUP_FILE_AVAILABLE,
                   tenant_id,
                   round_id,
                   piece_id,
                   task_info.copy_id_))) {
      LOG_WARN("failed to do extern backup piece info", KR(ret), K(job_info), K(task_info));
    } else if (OB_FAIL(get_backup_piece_info(
                   OB_START_INCARNATION, tenant_id, round_id, piece_id, 0 /*copy_id*/, trans, piece_info))) {
      LOG_WARN("failed to get backup piece info", KR(ret), K(tenant_id), K(round_id));
    } else if (OB_FAIL(build_backup_piece_info(piece_info,
                   ObBackupFileStatus::BACKUP_FILE_AVAILABLE,
                   job_info.backup_dest_,
                   task_info.copy_id_,
                   dst_piece_info))) {
      LOG_WARN("failed to build backup piece files info", KR(ret), K(piece_info), K(job_info), K(task_info));
    } else if (OB_FAIL(update_backup_piece_info(dst_piece_info, trans))) {
      LOG_WARN("failed to insert backup piece files info", KR(ret), K(dst_piece_info));
    } else if (OB_FAIL(check_is_last_piece(tenant_id, round_id, piece_id, is_last_piece))) {
      LOG_WARN("failed to check is last piece in history round", KR(ret), K(tenant_id), K(round_id), K(piece_id));
    } else {
      if (!is_last_piece) {
        if (OB_SYS_TENANT_ID != tenant_id && OB_FAIL(update_extern_tenant_clog_backup_info(tenant_id,
                                                 round_id,
                                                 piece_id,
                                                 task_info.copy_id_,
                                                 piece_info.checkpoint_ts_,
                                                 task_info.backup_dest_,
                                                 ObLogArchiveStatus::DOING))) {
          LOG_WARN("failed to update tenant clog backup info", KR(ret), K(tenant_id));
        } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level &&
                   OB_FAIL(update_tenant_inner_table_cur_info(tenant_id,
                       round_id,
                       piece_info.checkpoint_ts_,
                       task_info.copy_id_,
                       piece_id,
                       ObLogArchiveStatus::DOING,
                       task_info.backup_dest_,
                       trans))) {
          LOG_WARN("failed to update tenant inner table history info", KR(ret), K(tenant_id), K(round_id));
        }
      } else {
        if (OB_SYS_TENANT_ID != tenant_id && OB_FAIL(update_extern_tenant_clog_backup_info(tenant_id,
                                                 round_id,
                                                 piece_id,
                                                 task_info.copy_id_,
                                                 piece_info.checkpoint_ts_,
                                                 task_info.backup_dest_,
                                                 ObLogArchiveStatus::STOP))) {
          LOG_WARN("failed to update tenant clog backup info", KR(ret), K(tenant_id));
        } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level &&
                   OB_FAIL(update_tenant_inner_table_his_info(
                       tenant_id, round_id, task_info.copy_id_, task_info.backup_dest_, trans))) {
          LOG_WARN("failed to update tenant inner table history info", KR(ret), K(tenant_id), K(round_id));
        } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level &&
                   OB_FAIL(update_tenant_inner_table_cur_info(tenant_id,
                       round_id,
                       piece_info.checkpoint_ts_,
                       task_info.copy_id_,
                       piece_id,
                       ObLogArchiveStatus::STOP,
                       task_info.backup_dest_,
                       trans))) {
          LOG_WARN("failed to update tenant inner table history info", KR(ret), K(tenant_id), K(round_id));
        }
      }
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BACKUP_BACKUPPIECE_FINISH_UPDATE_EXTERN_AND_INNER_INFO) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("errsim before backup backup commit", KR(ret));
      }
    }
#endif
    DEBUG_SYNC(BEFORE_BACKUP_BACKUPPIECE_TASK_COMMIT);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_extern_backup_piece_info(const share::ObBackupBackupPieceJobInfo& job_info,
    const share::ObBackupFileStatus::STATUS file_status, const uint64_t tenant_id, const int64_t round_id,
    const int64_t piece_id, const int64_t copy_id)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPieceInfo info;
  ObBackupPieceInfoKey key;
  key.incarnation_ = OB_START_INCARNATION;
  key.tenant_id_ = tenant_id;
  key.round_id_ = round_id;
  key.backup_piece_id_ = piece_id;
  key.copy_id_ = copy_id;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char backup_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObClusterBackupDest cluster_backup_dest;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.get_backup_piece(*sql_proxy_, false /*for_update*/, key, info))) {
    LOG_WARN("failed to get backup piece", KR(ret));
  } else if (OB_FAIL(job_info.backup_dest_.get_backup_dest_str(backup_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret));
  } else if (OB_FAIL(info.backup_dest_.assign(backup_backup_dest_str))) {
    LOG_WARN("failed to assign backup dest", KR(ret));
  } else if (FALSE_IT(info.file_status_ = file_status)) {
    // set file status
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest str", KR(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest_str, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest_str));
  } else if (OB_FAIL(mgr.update_external_single_backup_piece_info(info, *backup_lease_service_))) {
    LOG_WARN("failed to update external single backup piece info", KR(ret), K(info));
  } else if (OB_FAIL(mgr.update_external_backup_backup_piece(cluster_backup_dest, info, *backup_lease_service_))) {
    LOG_WARN("failed to update extern backup backup piece", KR(ret), K(info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_smallest_doing_backup_piece_task(
    const int64_t job_id, const uint64_t tenant_id, ObBackupBackupPieceTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (job_id < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(
                 ObBackupBackupPieceTaskOperator::get_smallest_doing_task(job_id, tenant_id, *sql_proxy_, task_info))) {
    LOG_WARN("failed to get doing task items", KR(ret), K(job_id), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::insert_backup_backuppiece_task(const ObBackupBackupPieceTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(task_info));
  } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::insert_task_item(task_info, *sql_proxy_))) {
    LOG_WARN("failed to insert backup piece task info", KR(ret), K(task_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_backup_piece_info(
    const ObBackupPieceInfo& files_info, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!files_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(files_info));
  } else if (OB_FAIL(mgr.update_backup_piece(sql_proxy, files_info))) {
    LOG_WARN("failed to update backup piece", KR(ret), K(files_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_backup_piece_task_finish(
    const uint64_t tenant_id, const int64_t job_id, const int64_t piece_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || piece_id <= 0 || job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(piece_id), K(job_id));
  } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::update_task_finish(tenant_id, job_id, piece_id, proxy))) {
    LOG_WARN("failed to update task item", KR(ret), K(tenant_id), K(job_id), K(piece_id));
  } else {
    LOG_INFO("update backup piece task finish", K(tenant_id), K(job_id), K(piece_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_finish(const ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t MAX_EXECUTE_TIMEOUT_US = 60 * 1000 * 1000L;
  const int64_t job_id = job_info.job_id_;
  ObArray<ObBackupBackupPieceTaskInfo> job_tasks;
  int64_t stmt_timeout = MAX_EXECUTE_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(update_extern_info_when_finish(job_info))) {
    LOG_WARN("failed to update extern info when finish", KR(ret), K(job_info));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("failed to set trx timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("failed to set timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_all_task_items(job_id, trans, job_tasks))) {
      LOG_WARN("failed to get task items", KR(ret), K(job_id));
    } else if (OB_FAIL(ObBackupBackupPieceJobHistoryOperator::report_job_item(job_info, trans))) {
      LOG_WARN("failed to report job item", KR(ret), K(job_info));
    } else if (OB_FAIL(do_transform_job_tasks_when_finish(job_info, job_tasks))) {
      LOG_WARN("failed to do transform job tasks when finish", KR(ret), K(job_info));
    } else if (OB_FAIL(ObBackupBackupPieceTaskHistoryOperator::report_task_items(job_tasks, trans))) {
      LOG_WARN("failed to report tasks items", KR(ret), K(job_tasks));
    } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::remove_task_items(job_id, trans))) {
      LOG_WARN("failed to remove task items", KR(ret), K(job_id));
    } else if (OB_FAIL(ObBackupBackupPieceJobOperator::remove_job_item(job_id, trans))) {
      LOG_WARN("failed to remove job item", KR(ret), K(job_id));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
    if (OB_SUCC(ret)) {
      clear_memory_resource_if_stopped();
      idling_.wakeup();
    }
    ROOTSERVICE_EVENT_ADD("backup_backuppiece",
        "finish_job",
        "tenant_id",
        job_info.tenant_id_,
        "job_id",
        job_info.job_id_,
        "backup_piece_id",
        job_info.piece_id_,
        "job_status",
        job_info.get_status_str());
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_transform_job_tasks_when_finish(
    const share::ObBackupBackupPieceJobInfo& job_info, common::ObArray<share::ObBackupBackupPieceTaskInfo>& tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      ObBackupBackupPieceTaskInfo& task_info = tasks.at(i);
      task_info.task_status_ = ObBackupBackupPieceTaskInfo::FINISH;
      if (OB_SUCCESS == task_info.result_) {
        task_info.result_ = job_info.result_;
      }
    }
  }
  return ret;
}

// cannot put in one transaction because too much external info update
int ObBackupArchiveLogScheduler::update_extern_info_when_finish(const share::ObBackupBackupPieceJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupBackupPieceTaskInfo> job_tasks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_all_task_items(job_info.job_id_, *sql_proxy_, job_tasks))) {
    LOG_WARN("failed to get task items", KR(ret), K(job_info));
  } else if (OB_FAIL(do_transform_job_tasks_when_finish(job_info, job_tasks))) {
    LOG_WARN("failed to do transform job tasks when finish", KR(ret), K(job_info));
  } else if (OB_FAIL(update_backup_piece_info(job_info, job_tasks))) {
    LOG_WARN("failed to update backup piece info avaiable", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < job_tasks.count(); ++i) {
      const ObBackupBackupPieceTaskInfo& task = job_tasks.at(i);
      const uint64_t tenant_id = task.tenant_id_;
      ObBackupDest src, dst;
      if (OB_SYS_TENANT_ID == tenant_id) {
      } else if (OB_FAIL(get_piece_src_and_dst_backup_dest(task, src, dst))) {
        LOG_WARN("failed to get piece src and dst backup dest", KR(ret), K(task));
      } else if (OB_FAIL(sync_backup_backup_piece_info(tenant_id, src, dst))) {
        LOG_WARN("failed to sync backup backup piece info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(update_extern_backup_info(src, dst))) {
        LOG_WARN("failed to update extern backup info", KR(ret), K(src), K(dst));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_backup_piece_info(
    const share::ObBackupBackupPieceJobInfo& job_info, const common::ObIArray<ObBackupBackupPieceTaskInfo>& task_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list.count(); ++i) {
      const ObBackupBackupPieceTaskInfo& task_info = task_list.at(i);
      ObBackupPieceInfoKey key;
      ObBackupPieceInfo piece_info;
      if (OB_FAIL(task_info.get_backup_piece_key(key))) {
        LOG_WARN("failed to get backup piece key", KR(ret), K(task_info));
      } else if (OB_FAIL(mgr.get_backup_piece(*sql_proxy_, true /*for_update*/, key, piece_info))) {
        LOG_WARN("failed to get backup piece", KR(ret), K(key));
      } else {
        if (OB_SYS_TENANT_ID != job_info.tenant_id_) {
          piece_info.max_ts_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE == piece_info.status_
                                   ? piece_info.checkpoint_ts_
                                   : piece_info.max_ts_;
          piece_info.status_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE == piece_info.status_
                                   ? ObBackupPieceStatus::BACKUP_PIECE_INACTIVE
                                   : piece_info.status_;
        }
        piece_info.file_status_ = 0 == task_info.result_ ? ObBackupFileStatus::BACKUP_FILE_AVAILABLE
                                                         : ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
        if (ObBackupPieceStatus::BACKUP_PIECE_FREEZING == piece_info.status_) {
          if (OB_SUCCESS != task_info.result_) {
            piece_info.status_ = ObBackupPieceStatus::BACKUP_PIECE_INACTIVE;
          } else {
            tmp_ret = OB_ERR_UNEXPECTED;  // unlikely meet this condition, log tmp error for now
            LOG_ERROR("should not be freezing when finish", KR(tmp_ret), K(job_info));
          }
        }
        if (OB_FAIL(mgr.update_backup_piece(*sql_proxy_, piece_info))) {
          LOG_WARN("failed to update backup piece", KR(ret), K(piece_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_piece_batch_pg_list(const int64_t job_id, const int64_t round_id,
    const int64_t piece_id, const int64_t create_date, const uint64_t tenant_id,
    const share::ObClusterBackupDest& backup_dest, common::ObArray<SimplePGWrapper*>& node_list)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  node_list.reset();
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (round_id < 0 || piece_id <= 0 || create_date < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(round_id), K(piece_id));
  } else {
    ObArray<ObPGKey> pg_list;
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_SUCCESS == hash_ret) {
      if (queue_ptr->queue_.is_empty() && !queue_ptr->pushed_before_) {
        ObBackupPieceInfo piece_info;
        if (OB_FAIL(get_clog_pg_list_v2(backup_dest, round_id, piece_id, create_date, tenant_id, pg_list))) {
          LOG_WARN("failed to get clog pg list", KR(ret), K(tenant_id));
        } else if (pg_list.empty()) {
          LOG_INFO("pg list is empty", K(backup_dest), K(tenant_id));
        } else if (OB_FAIL(push_pg_list(queue_ptr->queue_, round_id, piece_id, create_date, pg_list))) {
          LOG_WARN("failed to push pg list", KR(ret), K(round_id));
        } else if (OB_FAIL(mark_current_turn_pg_list(tenant_id, pg_list, false))) {
          LOG_WARN("failed to mark current turn pg list", KR(ret), K(tenant_id));
        } else if (OB_FAIL(get_backup_piece_info(
                       OB_START_INCARNATION, tenant_id, round_id, piece_id, 0 /*copy_id*/, *sql_proxy_, piece_info))) {
          LOG_WARN("failed to get backup piece info", KR(ret), K(tenant_id), K(round_id));
        } else if (OB_FAIL(set_checkpoint_ts_with_lock(tenant_id, piece_info.checkpoint_ts_))) {
          LOG_WARN("failed to set checkpoint ts with lock", KR(ret), K(tenant_id));
        }

        if (OB_SUCC(ret)) {
          queue_ptr->job_id_ = job_id;
          queue_ptr->piece_id_ = piece_id;
          queue_ptr->pushed_before_ = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pop_pg_list(queue_ptr->queue_, node_list))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_piece_info_list(
    const uint64_t tenant_id, const int64_t copy_id, common::ObIArray<share::ObBackupPieceInfo>& info_list)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(copy_id));
  } else if (OB_FAIL(mgr.get_backup_piece_list(*sql_proxy_, tenant_id, copy_id, info_list))) {
    LOG_WARN("failed to get sys backup piece list", KR(ret), K(tenant_id), K(copy_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_piece_round_id(
    common::ObIArray<share::ObBackupPieceInfo>& piece_list, const int64_t piece_id, int64_t& round_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  round_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (piece_list.empty() || piece_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(piece_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); ++i) {
      const ObBackupPieceInfo& piece = piece_list.at(i);
      if (piece.key_.backup_piece_id_ == piece_id) {
        round_id = piece.key_.round_id_;
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("piece id may not exist yet", KR(ret), K(piece_id), K(piece_list));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_backup_piece_info(const int64_t incarnation, const uint64_t tenant_id,
    const int64_t round_id, const int64_t backup_piece_id, const int64_t copy_id, common::ObISQLClient& proxy,
    share::ObBackupPieceInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();
  ObBackupPieceInfoKey key;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_piece_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is not valid", KR(ret), K(tenant_id), K(backup_piece_id), K(copy_id));
  } else {
    key.incarnation_ = incarnation;
    key.tenant_id_ = tenant_id;
    key.round_id_ = round_id;
    key.backup_piece_id_ = backup_piece_id;
    key.copy_id_ = copy_id;
    if (OB_FAIL(mgr.get_backup_piece(proxy, false /*for_update*/, key, info))) {
      LOG_WARN("failed to get backup piece", KR(ret), K(key));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_all_backup_piece_task_finished(
    const share::ObBackupBackupPieceJobInfo& job_info, bool& all_finished)
{
  int ret = OB_SUCCESS;
  all_finished = true;
  const int64_t job_id = job_info.job_id_;
  ObArray<ObBackupBackupPieceTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup piece job is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_all_task_items(job_id, *sql_proxy_, task_infos))) {
    LOG_WARN("failed to get doing task items", KR(ret), K(job_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObBackupBackupPieceTaskInfo& task_info = task_infos.at(i);
      if (ObBackupBackupPieceTaskInfo::FINISH != task_info.task_status_) {
        all_finished = false;
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_src_data_available(const share::ObBackupBackupPieceJobInfo& job_info,
    const int64_t round_id, const int64_t piece_id, bool& is_available)
{
  int ret = OB_SUCCESS;
  is_available = true;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPieceInfoKey piece_key;
  piece_key.incarnation_ = OB_START_INCARNATION;
  piece_key.tenant_id_ = job_info.tenant_id_;
  piece_key.round_id_ = round_id;
  piece_key.backup_piece_id_ = piece_id;
  piece_key.copy_id_ = 0;
  ObBackupPieceInfo piece_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.get_backup_piece(*sql_proxy_, false /*for_update*/, piece_key, piece_info))) {
    LOG_WARN("failed to get backup piece", KR(ret), K(piece_key));
  } else if (ObBackupFileStatus::BACKUP_FILE_AVAILABLE != piece_info.file_status_) {
    is_available = false;
    LOG_WARN("backup piece is not available", K(piece_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_all_backup_piece_task_finished(
    const share::ObBackupBackupPieceJobInfo& job_info, const int result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const int64_t job_id = job_info.job_id_;
  ObArray<ObBackupBackupPieceTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup piece job is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(ObBackupBackupPieceTaskOperator::get_all_task_items(job_id, trans, task_infos))) {
    LOG_WARN("failed to get doing task items", KR(ret), K(job_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObBackupBackupPieceTaskInfo& task_info = task_infos.at(i);
      task_info.task_status_ = ObBackupBackupPieceTaskInfo::FINISH;
      task_info.result_ = result;
      if (OB_FAIL(ObBackupBackupPieceTaskOperator::report_task_item(task_info, trans))) {
        LOG_WARN("failed to report task item", KR(ret), K(task_info));
      }
    }
    if (OB_SUCC(ret)) {
      ObBackupBackupPieceJobInfo new_job_info = job_info;
      new_job_info.result_ = result;
      if (OB_FAIL(ObBackupBackupPieceJobOperator::report_job_item(new_job_info, trans))) {
        LOG_WARN("failed to report job item", KR(ret), K(new_job_info));
      }
    }
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_piece_task_finished(
    const ObBackupBackupPieceJobInfo& job_info, const uint64_t tenant_id, const int64_t piece_id, bool& finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid() || OB_INVALID_ID == tenant_id || piece_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup piece job is not valid", KR(ret), K(job_info), K(tenant_id), K(piece_id));
  } else if (OB_FAIL(check_backup_piece_pg_task_finished(tenant_id, piece_id, finished))) {
    LOG_WARN("failed to check backup piece pg task finished", KR(ret), K(tenant_id), K(piece_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_piece_pg_task_finished(
    const uint64_t tenant_id, const int64_t piece_id, bool& finished)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  finished = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ARGUMENT == tenant_id || piece_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(piece_id));
  } else {
    TenantTaskQueue* queue_ptr = NULL;
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_SUCCESS == hash_ret) {
      if (!queue_ptr->pushed_before_) {
        finished = false;
        LOG_WARN("queue not pushed before", K(tenant_id));
      } else {
        typedef ObHashMap<ObPGKey, CurrentTurnPGStat>::const_iterator Iterator;
        Iterator iter = queue_ptr->pg_map_.begin();
        for (; OB_SUCC(ret) && iter != queue_ptr->pg_map_.end(); ++iter) {
          if (!iter->second.finished_) {
            finished = false;
            LOG_INFO("backup piece pg task not finished", K(iter->first));
            break;
          }
        }
        if (OB_SUCC(ret) && finished) {
          queue_ptr->pushed_before_ = false;
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_piece_src_and_dst_backup_dest(
    const share::ObBackupBackupPieceTaskInfo& piece, share::ObBackupDest& src, share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPieceInfoKey src_key;
  ObBackupPieceInfo src_piece;
  src_key.incarnation_ = piece.incarnation_;
  src_key.tenant_id_ = piece.tenant_id_;
  src_key.round_id_ = piece.round_id_;
  src_key.backup_piece_id_ = piece.piece_id_;
  src_key.copy_id_ = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(!piece.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(piece));
  } else if (OB_FAIL(mgr.get_backup_piece(*sql_proxy_, false /*for_update*/, src_key, src_piece))) {
    LOG_WARN("failed to get backup piece", KR(ret), K(src_key));
  } else if (OB_FAIL(src.set(src_piece.backup_dest_.ptr()))) {
    LOG_WARN("failed to set src backup dest");
  } else {
    dst = piece.backup_dest_;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::sync_backup_backup_piece_info(
    const uint64_t tenant_id, const share::ObBackupDest& src, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObClusterBackupDest src_dest, dst_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_dest.set(src, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src dest", KR(ret));
  } else if (OB_FAIL(dst_dest.set(dst, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src dest", KR(ret));
  } else if (OB_FAIL(mgr.sync_backup_backup_piece_info(tenant_id, src_dest, dst_dest, *backup_lease_service_))) {
    LOG_WARN("failed to sync backup backup piece info", KR(ret), K(tenant_id), K(src_dest), K(dst_dest));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::create_mount_file(const share::ObBackupDest& backup_dest, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfo sys_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!backup_dest.is_valid() || round_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(backup_dest), K(round_id));
  } else if (!backup_dest.is_nfs_storage()) {
    // do nothing
  } else if (OB_FAIL(get_log_archive_round_info(OB_SYS_TENANT_ID, round_id, sys_info))) {
    LOG_WARN("failed to get log archive round info", KR(ret), K(round_id));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(sys_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(sys_info));
  } else if (OB_FAIL(ObBackupMountFile::create_mount_file(sys_info))) {
    LOG_WARN("failed to create mount file", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_is_last_piece(
    const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, bool& is_last_piece)
{
  int ret = OB_SUCCESS;
  is_last_piece = false;
  bool is_history_round = false;
  bool tenant_is_dropped = false;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPieceInfo backup_piece;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(check_is_history_round(round_id, is_history_round))) {
    LOG_WARN("failed to check is history round", KR(ret), K(round_id));
  } else if (OB_FAIL(check_tenant_is_dropped(tenant_id, tenant_is_dropped))) {
    LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
  } else if (!is_history_round && !tenant_is_dropped) {
    // do nothing
  } else if (OB_FAIL(
                 mgr.get_last_piece_in_round(*sql_proxy_, OB_START_INCARNATION, tenant_id, round_id, backup_piece))) {
    LOG_WARN("failed to get last backup piece", KR(ret), K(tenant_id), K(round_id));
  } else {
    is_last_piece = backup_piece.key_.backup_piece_id_ == piece_id;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_round_piece_info_for_clean(const uint64_t tenant_id, const int64_t round_id,
    const bool round_finished, const share::ObBackupDest& backup_dest, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPieceInfo info;
  ObBackupPieceInfoKey key;
  key.incarnation_ = OB_START_INCARNATION;
  key.tenant_id_ = tenant_id;
  key.round_id_ = round_id;
  key.backup_piece_id_ = 0;
  key.copy_id_ = 0;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char backup_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObClusterBackupDest cluster_backup_dest;
  ObLogArchiveBackupInfo cur_info;

  bool is_switch_piece = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(check_is_switch_piece(is_switch_piece))) {
    LOG_WARN("failed to check is switch piece", KR(ret));
  } else if (is_switch_piece) {
    // do nothing
  } else if (OB_FAIL(mgr.get_backup_piece(sql_proxy, false /*for_update*/, key, info))) {
    LOG_WARN("failed to get backup piece", KR(ret), K(key));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(info.backup_dest_.assign(backup_backup_dest_str))) {
    LOG_WARN("failed to assign backup dest", KR(ret));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest str", KR(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest_str, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest_str));
  } else if (OB_FAIL(get_current_round_task_info(tenant_id, cur_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // sys task info may not exist yet
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get current round task info", KR(ret), K(tenant_id));
    }
  } else if (cur_info.status_.round_ != round_id) {
    // do nothing
  } else {
    info.key_.copy_id_ = cur_info.status_.copy_id_;
    if (round_finished) {
      if (info.checkpoint_ts_ > cur_info.status_.checkpoint_ts_) {
        info.status_ = ObBackupPieceStatus::BACKUP_PIECE_INACTIVE;
      } else {
        info.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
      }
    } else {
      info.status_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE;
    }
    info.checkpoint_ts_ = cur_info.status_.checkpoint_ts_;
    if (OB_FAIL(mgr.update_external_single_backup_piece_info(info, *backup_lease_service_))) {
      LOG_WARN("failed to update external single backup piece info", KR(ret), K(info));
    } else if (OB_FAIL(mgr.update_external_backup_backup_piece(cluster_backup_dest, info, *backup_lease_service_))) {
      LOG_WARN("failed to update extern backup backup piece", KR(ret), K(info));
    } else if (OB_FAIL(update_backup_piece_info(info, sql_proxy))) {
      LOG_WARN("failed to insert backup piece info", KR(ret), K(info));
    }
  }
  return ret;
}

bool ObBackupArchiveLogScheduler::is_fatal_error(const int64_t result)
{
  return OB_ERR_SYS == result || OB_ERR_UNEXPECTED == result || OB_INVALID_ARGUMENT == result ||
         OB_LOG_ARCHIVE_INTERRUPTED == result;
}

int ObBackupArchiveLogScheduler::on_fatal_error(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t result)
{
  int ret = OB_SUCCESS;
  share::ObBackupBackupPieceJobInfo new_job_info;
  new_job_info = job_info;
  new_job_info.status_ = ObBackupBackupPieceJobInfo::FINISH;
  new_job_info.result_ = result;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_info));
  } else if (OB_FAIL(update_backup_piece_job(new_job_info))) {
    LOG_WARN("failed to update backup piece job", KR(ret), K(new_job_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_normal_tenant_passed_sys_tenant(
    const uint64_t tenant_id, const int64_t local_sys_checkpoint_ts, bool& passed)
{
  int ret = OB_SUCCESS;
  passed = false;
  ObLogArchiveBackupInfo normal_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_current_round_task_info(tenant_id, normal_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get log archive backup backup info", KR(ret), K(tenant_id));
    }
  } else {
    passed = normal_info.status_.checkpoint_ts_ >= local_sys_checkpoint_ts;
    LOG_INFO("normal tenant has passed sys tenant", K(normal_info));
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
