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

#include "ob_log_membership_task_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "common/ob_trace_profile.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_tenant_mgr.h"
#include "storage/ob_storage_log_type.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/transaction/ob_trans_ctx.h"
#include "ob_i_log_engine.h"
#include "ob_log_block.h"
#include "ob_log_flush_task.h"
#include "ob_log_membership_mgr_V2.h"
#include "ob_log_restore_mgr.h"
#include "ob_log_state_mgr.h"
#include "ob_log_task.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace transaction;
namespace clog {
ObLogMembershipTaskMgr::ObLogMembershipTaskMgr()
    : log_engine_(NULL),
      state_mgr_(NULL),
      mm_(NULL),
      cascading_mgr_(NULL),
      cb_engine_(NULL),
      restore_mgr_(NULL),
      partition_service_(NULL),
      alloc_mgr_(NULL),
      self_(),
      partition_key_(),
      block_meta_len_(0),
      renew_ms_task_lock_(common::ObLatchIds::CLOG_RENEW_MS_TASK_LOCK),
      cur_renew_ms_task_(),
      is_inited_(false)
{}

int ObLogMembershipTaskMgr::init(ObILogEngine* log_engine, ObILogStateMgrForSW* state_mgr, ObILogMembershipMgr* mm,
    ObLogCascadingMgr* cascading_mgr, ObILogCallbackEngine* cb_engine, ObLogRestoreMgr* restore_mgr,
    storage::ObPartitionService* partition_service, common::ObILogAllocator* alloc_mgr, const common::ObAddr& self,
    const common::ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  ObLogBlockMetaV2 block_meta;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(log_engine) || OB_ISNULL(restore_mgr) || OB_ISNULL(cb_engine) || OB_ISNULL(state_mgr) ||
             OB_ISNULL(mm) || OB_ISNULL(cascading_mgr) || OB_ISNULL(partition_service) || OB_ISNULL(alloc_mgr) ||
             !self.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_engine_ = log_engine;
    state_mgr_ = state_mgr;
    mm_ = mm;
    cascading_mgr_ = cascading_mgr;
    cb_engine_ = cb_engine;
    restore_mgr_ = restore_mgr;
    partition_service_ = partition_service;
    alloc_mgr_ = alloc_mgr;
    self_ = self;
    partition_key_ = key;
    block_meta_len_ = block_meta.get_serialize_size();
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogMembershipTaskMgr init success", K_(partition_key));
  }

  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

void ObLogMembershipTaskMgr::destroy()
{
  is_inited_ = false;
  state_mgr_ = NULL;
  log_engine_ = NULL;
  mm_ = NULL;
  cascading_mgr_ = NULL;
  cb_engine_ = NULL;
  partition_service_ = NULL;
  alloc_mgr_ = NULL;
  self_.reset();
  CLOG_LOG(INFO, "ObLogMembershipTaskMgr::destroy finished", K_(partition_key));
}

int ObLogMembershipTaskMgr::reset_renew_ms_log_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    renew_ms_task_lock_.lock();
    (void)cur_renew_ms_task_.reset();
    renew_ms_task_lock_.unlock();
  }
  return ret;
}

int ObLogMembershipTaskMgr::ack_renew_ms_log(const uint64_t log_id, const int64_t submit_ts,
    const common::ObProposalID& proposal_id, const ObAddr& server, bool& majority)
{
  int ret = OB_SUCCESS;
  ObLogTask* log_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == log_id || !server.is_valid() || OB_ISNULL(mm_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    renew_ms_task_lock_.lock();
    log_task = &(cur_renew_ms_task_.log_task_);
    uint64_t cur_renew_log_id = cur_renew_ms_task_.get_log_id();
    int64_t local_submit_ts = log_task->get_submit_timestamp();
    const ObProposalID cur_ms_proposal_id = mm_->get_ms_proposal_id();
    if (log_id != cur_renew_log_id || submit_ts != local_submit_ts || proposal_id != cur_ms_proposal_id) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "renew_ms_log_id not match, unexpected",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(cur_renew_log_id),
          K(submit_ts),
          K(local_submit_ts),
          K(proposal_id),
          K(cur_ms_proposal_id),
          K(*log_task));
    } else if (!log_task->is_submit_log_exist()) {
      ret = OB_ERR_NULL_VALUE;
      CLOG_LOG(WARN, "ack log: get null log", K_(partition_key), K(log_id));
    } else if (mm_->get_curr_member_list().contains(server) && self_ != server) {
      // log_task no need locked again
      if (OB_FAIL(log_task->ack_log(server))) {
        CLOG_LOG(WARN, "log_task->ack_log failed", K_(partition_key), K(ret), K(log_id), K(server));
      } else {
        majority = log_task->try_set_majority_finished();
        cur_renew_ms_task_.ack_mlist_.add_server(server);
      }
    } else {
      // do nothing
    }
    renew_ms_task_lock_.unlock();
    CLOG_LOG(
        INFO, "ack_renew_ms_log finished", K_(partition_key), K(ret), K(log_id), K(server), K(majority), K(*log_task));
  }

  return ret;
}

int ObLogMembershipTaskMgr::need_update_renew_ms_log_task_(
    const ObLogEntryHeader& header, const char* buff, ObLogTask& task, bool& log_need_update, bool& need_send_ack)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(buff)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!task.is_submit_log_exist()) {
    log_need_update = true;
  } else {
    ObProposalID old_pid = task.get_proposal_id();
    ObProposalID new_pid = header.get_proposal_id();
    if (new_pid > old_pid) {
      CLOG_LOG(INFO, "receive newer renew_ms_log, need update log_task", K(ret), K(header), K_(partition_key), K(task));
      log_need_update = true;
    } else if (old_pid == new_pid) {
      const int64_t new_submit_ts = header.get_submit_timestamp();
      const int64_t old_submit_ts = task.get_submit_timestamp();
      if (new_submit_ts > old_submit_ts) {
        CLOG_LOG(
            INFO, "receive newer renew_ms_log, need update log_task", K(ret), K(header), K_(partition_key), K(task));
        log_need_update = true;
      } else if (new_submit_ts == old_submit_ts) {
        if (!task.is_log_confirmed()) {  // avoid confirmed log is written twice
          if (task.is_flush_local_finished()) {
            need_send_ack = true;
          }
        } else {
          // log is already confirmed, need send ack to leader
          if (state_mgr_->get_leader() != self_) {
            need_send_ack = true;
          }
        }
        CLOG_LOG(INFO,
            "receive same renew_ms_log",
            K(ret),
            K(header),
            K_(partition_key),
            K(log_need_update),
            K(need_send_ack),
            K(task));
      } else {
        CLOG_LOG(INFO, "receive older renew_ms_log, ignore", K(ret), K(header), K_(partition_key), K(task));
      }
    } else {
      CLOG_LOG(INFO, "receive older renew_ms_log, ignore", K(ret), K(header), K_(partition_key), K(task));
    }
  }
  return ret;
}

// check renew_ms_log sync state, try to send to all paxos replicas
int ObLogMembershipTaskMgr::check_renew_ms_log_sync_state() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!GCTX.is_standby_cluster()) {
    // skip non standby_cluster
  } else if (STANDBY_LEADER == state_mgr_->get_role()) {
    // standby_leader try to send renew_ms_log to replica that has not reply ack.
    ObMemberList list;
    ObLogEntry log_entry;
    ObLogEntryHeader header;
    char buff[MS_LOG_BUFFER_SIZE];
    renew_ms_task_lock_.lock();
    ObProposalID ms_proposal_id = mm_->get_ms_proposal_id();
    if (ms_proposal_id != cur_renew_ms_task_.log_task_.get_proposal_id()) {
      CLOG_LOG(WARN, "ms_proposal_id not match", K_(partition_key), K(ms_proposal_id), K_(cur_renew_ms_task));
    } else if (cur_renew_ms_task_.ack_mlist_.get_count() >= mm_->get_curr_member_list().get_member_number() - 1) {
      // I already received all followers' ack.
    } else if (OB_FAIL(list.deep_copy(mm_->get_curr_member_list()))) {
      CLOG_LOG(WARN, "deep_copy member_list failed", K(ret), K_(partition_key));
    } else if (OB_FAIL(mm_->get_curr_ms_log_body(header, buff, MS_LOG_BUFFER_SIZE))) {
      CLOG_LOG(WARN, "get_curr_ms_log_buf failed", K(ret), K_(partition_key));
    } else if (OB_FAIL(log_entry.generate_entry(header, buff))) {
      CLOG_LOG(WARN, "generate_entry failed", K_(partition_key), K(ret));
    } else {
      int64_t pos = 0;
      char* serialize_buff = NULL;
      int64_t serialize_size = log_entry.get_serialize_size();
      if (NULL == (serialize_buff = static_cast<char*>(alloc_mgr_->ge_alloc(serialize_size)))) {
        CLOG_LOG(ERROR, "alloc failed", K_(partition_key));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_SUCCESS != (ret = log_entry.serialize(serialize_buff, serialize_size, pos))) {
        CLOG_LOG(WARN, "submit_log serialize failed", K_(partition_key), K(ret));
      } else {
        ObLogNetTask net_task(ms_proposal_id, serialize_buff, serialize_size);
        ObAddr server;
        const int64_t cluster_id = state_mgr_->get_self_cluster_id();
        for (int64_t i = 0; i < list.get_member_number() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(list.get_server_by_index(i, server))) {
            CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K_(partition_key));
          } else if (self_ == server) {
            // skip self
          } else if (cur_renew_ms_task_.ack_mlist_.contains(server)) {
            // already in ack_mlist, skip
          } else if (OB_FAIL(log_engine_->submit_push_ms_log_req(server, cluster_id, partition_key_, &net_task))) {
            CLOG_LOG(WARN, "submit_push_ms_log_req failed", K_(partition_key), K(ret), K(log_entry));
          } else {
            // do nothing
          }
        }
      }
      if (NULL != serialize_buff) {
        alloc_mgr_->ge_free(serialize_buff);
        serialize_buff = NULL;
      }
    }
    renew_ms_task_lock_.unlock();
  } else {
    // do nothing
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_slog_flush_task_(const ObLogType log_type, const uint64_t log_id,
    const ObRenewMembershipLog& renew_ms_log, const ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == log_id || !server.is_valid() || OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(log_id), K(server), K(cluster_id));
  } else if (OB_FAIL(partition_service_->submit_ms_info_task(partition_key_,
                 server,
                 cluster_id,
                 log_type,
                 log_id,
                 renew_ms_log.get_timestamp(),
                 renew_ms_log.get_replica_num(),
                 renew_ms_log.get_prev_member_list(),
                 renew_ms_log.get_member_list(),
                 renew_ms_log.get_ms_proposal_id()))) {
    int err_code = ret;
    if (OB_ENTRY_EXIST == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
      // If the task queue is full, it will return -4018.
      // -4018,-4013 need be converted to -4023.
      ret = OB_EAGAIN;
    }
    CLOG_LOG(WARN, "submit_ms_info_task failed, try again", K_(partition_key), K(ret), K(err_code));
  } else {
    CLOG_LOG(INFO, "submit_slog_flush_task_ success", K(server), K_(partition_key), K(log_id), K(renew_ms_log));
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_log(const ObRenewMembershipLog& renew_ms_log, const ObLogType& log_type,
    const char* buff, const int64_t size, const uint64_t log_id, const int64_t log_timestamp, const bool is_trans_log,
    ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  bool need_replay = false;
  bool send_slave = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (NULL == buff || size > OB_MAX_LOG_ALLOWED_SIZE || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_));
  } else {
    // normal commit
    ObLogEntryHeader header;
    ObAddr dummy_server = self_;
    const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
    ObVersion dummy_version(2, 0);

    // renew_ms_log need contain ms_proposal_id
    ObProposalID proposal_id = mm_->get_ms_proposal_id();

    if (OB_FAIL(header.generate_header(log_type,
            partition_key_,                 // partition_key
            log_id,                         // log_id
            buff,                           // buff
            size,                           // data_len
            ObTimeUtility::current_time(),  // generation_timestamp
            proposal_id.ts_,                // epoch_id
            proposal_id,                    // proposal_id
            log_timestamp,                  // submit_timestamp
            dummy_version,
            is_trans_log))) {
    } else if (OB_FAIL(submit_renew_ms_log_(
                   renew_ms_log, header, buff, cb, need_replay, send_slave, dummy_server, dummy_cluster_id))) {
      CLOG_LOG(WARN, "submit_renew_ms_log_ failed", K_(partition_key), K(ret), K(log_id));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_log(
    const ObRenewMembershipLog& renew_ms_log, const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buff)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool need_replay = (cb == NULL);
    bool send_slave = true;
    ObAddr dummy_server = self_;
    const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_FAIL(submit_renew_ms_log_(
            renew_ms_log, header, buff, cb, need_replay, send_slave, dummy_server, dummy_cluster_id))) {
      CLOG_LOG(WARN,
          "submit_renew_ms_log_ failed",
          K_(partition_key),
          K(ret),
          "log_id",
          header.get_log_id(),
          "log_type",
          header.get_log_type());
    }
  }
  return ret;
}

// Used by the standby leader to submit renew_ms_log or follower to receive renew_ms_log
int ObLogMembershipTaskMgr::submit_renew_ms_log_(const ObRenewMembershipLog& renew_ms_log,
    const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb, const bool need_replay, const bool send_slave,
    const ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogTask* log_task = NULL;
  bool log_task_need_update = false;
  const uint64_t log_id = header.get_log_id();
  const bool need_copy = false;
  bool locked = false;

  char* serialize_buff_extend = NULL;
  char* serialize_buff = NULL;
  ObLogEntry new_log;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(log_engine_) || OB_ISNULL(buff)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(log_engine_), KP(buff), K(ret));
  } else {
    // lock
    renew_ms_task_lock_.lock();
    locked = true;

    log_task = &(cur_renew_ms_task_.log_task_);
    int64_t local_submit_ts = log_task->get_submit_timestamp();
    int64_t arg_submit_ts = header.get_submit_timestamp();
    ObProposalID ms_proposal_id = mm_->get_ms_proposal_id();

    if (local_submit_ts == arg_submit_ts && header.get_proposal_id() == ms_proposal_id) {
      // this log already exists
      if (log_task->is_on_success_cb_called()) {
        if (!log_task->is_checksum_verified(header.get_data_checksum())) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "is_checksum_verified failed", K(ret), K_(partition_key), K(header), K_(cur_renew_ms_task));
        }
      }
    } else {
      // not match, check if it need be updated later
      CLOG_LOG(INFO,
          "local renew_ms_log not match, need check to update",
          K(ret),
          K_(partition_key),
          K(header),
          K_(cur_renew_ms_task));
    }

    if (OB_SUCC(ret)) {
      bool need_send_ack = false;
      const ObCascadMember leader_member = state_mgr_->get_leader_member();
      const ObAddr leader = leader_member.get_server();
      const int64_t leader_cluster_id = leader_member.get_cluster_id();
      if (OB_FAIL(need_update_renew_ms_log_task_(header, buff, *log_task, log_task_need_update, need_send_ack))) {
        CLOG_LOG(WARN, "need_update_renew_ms_log_task_ failed", K(ret), K(log_id), K_(partition_key));
      } else if (need_send_ack && state_mgr_->can_send_renew_ms_log_ack(ms_proposal_id) && server.is_valid() &&
                 server == leader &&
                 OB_FAIL(log_engine_->submit_renew_ms_log_ack(
                     server, leader_cluster_id, partition_key_, log_id, arg_submit_ts, ms_proposal_id))) {
        CLOG_LOG(WARN,
            "submit_renew_ms_log_ack failed",
            K_(partition_key),
            K(ret),
            K(server),
            K(log_id),
            K(arg_submit_ts),
            K(ms_proposal_id));
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret) && log_task_need_update) {
    cur_renew_ms_task_.reset();
    log_task->reset_all_state();  // reset log_task state

    int64_t pos = 0;
    if (OB_FAIL(log_task->init(NULL, mm_->get_replica_num(), need_replay))) {
      CLOG_LOG(WARN, "init log_task failed", K(ret), K(log_id), K_(partition_key));
    } else if (OB_FAIL(new_log.generate_entry(header, buff))) {
      CLOG_LOG(ERROR, "generate_entry failed", K_(partition_key), K(ret));
    } else if (OB_ISNULL(alloc_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (NULL == (serialize_buff_extend = static_cast<char*>(
                            alloc_mgr_->ge_alloc(block_meta_len_ + new_log.get_serialize_size())))) {
      CLOG_LOG(WARN, "alloc memory failed", K_(partition_key), K(new_log));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (NULL == (serialize_buff = serialize_buff_extend + block_meta_len_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "error unexpected", K(ret), K(partition_key_), K(new_log));
    } else if (OB_FAIL(new_log.serialize(serialize_buff, new_log.get_serialize_size(), pos))) {
      CLOG_LOG(WARN, "submit_to_net_and_disk serialize failed", K_(partition_key), K(ret));
    } else {
      if (OB_FAIL(submit_slog_flush_task_(header.get_log_type(), log_id, renew_ms_log, server, cluster_id))) {
        // If the submission fails, set_log will not be executed.
        // standby_leadera will retry,
        // follower will receive log again later.
        CLOG_LOG(WARN, "submit_slog_flush_task_ failed", K(ret), K_(partition_key), K(header));
      } else if (OB_FAIL(log_task->set_log(header, buff, need_copy))) {
        CLOG_LOG(ERROR, "set submit log to log task failed", K(ret), K(header), K_(partition_key));
      } else {
        cur_renew_ms_task_.log_id_ = log_id;
        log_task->reset_log_cursor();
        log_task->set_submit_cb(cb);
        CLOG_LOG(INFO, "cur_renew_ms_task_ set log success", K_(partition_key), K_(cur_renew_ms_task));
      }
    }
  }

  if (locked) {
    // unlock
    renew_ms_task_lock_.unlock();
    locked = false;
  }

  if (OB_SUCC(ret) && (send_slave || cascading_mgr_->has_valid_child()) &&
      (OB_SUCCESS != (tmp_ret = submit_renew_ms_log_to_net_(
                          new_log.get_header(), serialize_buff, new_log.get_serialize_size())))) {
    CLOG_LOG(WARN,
        "submit_renew_ms_log_to_net_ failed",
        K(tmp_ret),
        K_(partition_key),
        K(header),
        K(log_task_need_update),
        K(new_log));
  }

  if (serialize_buff_extend != NULL) {
    alloc_mgr_->ge_free(serialize_buff_extend);
    serialize_buff_extend = NULL;
    serialize_buff = NULL;
  }
  return ret;
}

bool ObLogMembershipTaskMgr::is_renew_ms_log_majority_success() const
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "not inited", K(partition_key_));
  } else if (!state_mgr_->is_can_elect_standby_leader()) {
    CLOG_LOG(WARN, "cluster state not match, unexpected", K(partition_key_));
  } else {
    renew_ms_task_lock_.lock();
    const ObLogTask& log_task = cur_renew_ms_task_.log_task_;

    if (log_task.is_flush_local_finished() && log_task.is_log_confirmed()) {
      bool_ret = true;
    }

    renew_ms_task_lock_.unlock();
  }

  return bool_ret;
}

bool ObLogMembershipTaskMgr::is_renew_ms_log_majority_success(const uint64_t log_id, const int64_t log_ts) const
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "not inited", K(partition_key_));
  } else {
    renew_ms_task_lock_.lock();
    const ObLogTask& log_task = cur_renew_ms_task_.log_task_;

    if (log_ts != log_task.get_submit_timestamp() || log_id != cur_renew_ms_task_.log_id_) {
      CLOG_LOG(WARN, "log info not match", K(partition_key_), K(log_id), K(log_ts), K(cur_renew_ms_task_));
    } else if (log_task.is_flush_local_finished() && log_task.is_log_confirmed()) {
      bool_ret = true;
    } else {
    }

    renew_ms_task_lock_.unlock();
  }

  return bool_ret;
}

int ObLogMembershipTaskMgr::submit_confirmed_info_(const uint64_t log_id, const ObProposalID& ms_proposal_id,
    const ObConfirmedInfo& confirmed_info, const bool is_leader)
{
  UNUSED(is_leader);
  int ret = OB_SUCCESS;
  bool locked = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_can_elect_standby_leader()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    renew_ms_task_lock_.lock();
    locked = true;
    ObLogTask& log_task = (cur_renew_ms_task_.log_task_);
    uint64_t cur_renew_log_id = cur_renew_ms_task_.get_log_id();
    ObProposalID cur_ms_proposal_id = mm_->get_ms_proposal_id();

    if (log_id != cur_renew_log_id || ms_proposal_id != cur_ms_proposal_id ||
        ms_proposal_id != log_task.get_proposal_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "log_id or ms_proposal_id not match with cur_renew_ms_task",
          K_(partition_key),
          K(log_id),
          K(cur_renew_log_id),
          K(ms_proposal_id),
          K(cur_ms_proposal_id),
          K(log_task));
    } else {
      if (log_task.is_on_success_cb_called()) {
        if (!log_task.is_checksum_verified(confirmed_info.get_data_checksum())) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR,
              "is_checksum_verified failed",
              K(ret),
              K(partition_key_),
              K(log_id),
              K(confirmed_info),
              K(log_task));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      if (log_task.is_confirmed_info_exist()) {
      } else {
        if (log_task.is_submit_log_exist()) {
          if ((log_task.get_data_checksum() != confirmed_info.get_data_checksum()) ||
              (log_task.get_epoch_id() != confirmed_info.get_epoch_id())) {
            CLOG_LOG(INFO,
                "log_task and confirmed_info not match, reset",
                K_(partition_key),
                K(log_id),
                K(log_task),
                K(confirmed_info));
            log_task.reset_log();
            log_task.reset_state(false);
            log_task.reset_log_cursor();
          }
        } else {
          CLOG_LOG(TRACE,
              "this partition receive a confirm_info but no submit_log exist",
              K(log_id),
              K(confirmed_info),
              K(log_task),
              K_(partition_key));
        }
        log_task.set_confirmed_info(confirmed_info);
        log_task.set_log_confirmed();
      }
    }

    if (locked) {
      renew_ms_task_lock_.unlock();
      locked = false;
    }

    CLOG_LOG(INFO,
        "submit_confirmed_info_ finished",
        K(ret),
        K_(partition_key),
        K(log_id),
        K(ms_proposal_id),
        K(confirmed_info),
        K(log_task));
  }

  return ret;
}

int ObLogMembershipTaskMgr::submit_renew_ms_confirmed_info_to_net_(
    const uint64_t log_id, const ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mm_) || OB_ISNULL(log_engine_) || OB_ISNULL(cascading_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_SUCCESS != (tmp_ret = submit_renew_ms_confirmed_info_to_mlist_(log_id, ms_proposal_id, confirmed_info))) {
      CLOG_LOG(WARN, "submit_renew_ms_confirmed_info_to_mlist_ failed", K(tmp_ret), K(partition_key_), K(log_id));
    }

    if (OB_SUCCESS != (tmp_ret = submit_renew_ms_confirmed_info_to_children_(log_id, ms_proposal_id, confirmed_info))) {
      CLOG_LOG(WARN, "submit_renew_ms_confirmed_info_to_children_ failed", K(tmp_ret), K(partition_key_), K(log_id));
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_renew_ms_confirmed_info_to_mlist_(
    const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
{
  int ret = OB_SUCCESS;
  if (LEADER == state_mgr_->get_role() || STANDBY_LEADER == state_mgr_->get_role()) {
    ObCascadMemberList list;
    list.reset();

    const int64_t dst_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_FAIL(list.deep_copy(mm_->get_curr_member_list(), dst_cluster_id))) {
      CLOG_LOG(ERROR, "deep_copy failed", K_(partition_key), K(ret));
    } else if (list.is_valid()) {
      if (list.contains(self_) && OB_FAIL(list.remove_server(self_))) {
        CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K(list), K_(self));
      } else if (OB_FAIL(log_engine_->submit_renew_ms_confirmed_info(
                     list, partition_key_, log_id, ms_proposal_id, confirmed_info))) {
        CLOG_LOG(WARN,
            "log_engine submit_renew_ms_confirmed_info failed",
            K_(partition_key),
            K(ret),
            K(list),
            K(log_id),
            K(confirmed_info),
            K(ms_proposal_id));
      } else {
        CLOG_LOG(INFO,
            "log_engine submit_renew_ms_confirmed_info success",
            K_(partition_key),
            K(ret),
            K(list),
            K(log_id),
            K(confirmed_info),
            K(ms_proposal_id));
      }
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_renew_ms_confirmed_info_to_children_(
    const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
{
  int ret = OB_SUCCESS;
  if (cascading_mgr_->has_valid_child()) {
    ObCascadMemberList list;
    list.reset();

    if (OB_FAIL(cascading_mgr_->get_children_list(list))) {
      CLOG_LOG(ERROR, "get_children_list failed", K_(partition_key), K(ret));
    } else if (list.is_valid()) {
      if (list.contains(self_) && OB_FAIL(list.remove_server(self_))) {
        CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K(list), K_(self));
      } else if (OB_FAIL(log_engine_->submit_renew_ms_confirmed_info(
                     list, partition_key_, log_id, ms_proposal_id, confirmed_info))) {
        CLOG_LOG(WARN,
            "log_engine submit_renew_ms_confirmed_info failed",
            K_(partition_key),
            K(ret),
            K(list),
            K(log_id),
            K(confirmed_info),
            K(ms_proposal_id));
      } else {
        CLOG_LOG(INFO,
            "log_engine submit_renew_ms_confirmed_info failed",
            K_(partition_key),
            K(ret),
            K(list),
            K(log_id),
            K(confirmed_info),
            K(ms_proposal_id));
      }
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_renew_ms_log_to_net_(
    const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(log_engine_) || OB_ISNULL(serialize_buff) ||
             OB_ISNULL(cascading_mgr_) || serialize_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (restore_mgr_->is_archive_restoring()) {
  } else {
    if (OB_SUCCESS != (tmp_ret = submit_renew_ms_log_to_mlist_(header, serialize_buff, serialize_size))) {
      CLOG_LOG(WARN, "submit_renew_ms_log_to_mlist_ failed", K(tmp_ret), K(partition_key_), K(header));
    }

    if (OB_SUCCESS != (tmp_ret = submit_renew_ms_log_to_children_(header, serialize_buff, serialize_size))) {
      CLOG_LOG(WARN, "submit_renew_ms_log_to_children_ failed", K(tmp_ret), K(partition_key_), K(header));
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_renew_ms_log_to_mlist_(
    const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size)
{
  int ret = OB_SUCCESS;
  if (STANDBY_LEADER == state_mgr_->get_role()) {
    ObProposalID proposal_id = mm_->get_ms_proposal_id();
    ObLogNetTask net_task(proposal_id, serialize_buff, serialize_size);
    ObCascadMemberList list;
    list.reset();

    const int64_t dst_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_FAIL(list.deep_copy(mm_->get_curr_member_list(), dst_cluster_id))) {
      CLOG_LOG(ERROR, "deep_copy failed", K_(partition_key), K(ret));
    } else if (list.is_valid()) {
      ObAddr server;
      const int64_t cluster_id = state_mgr_->get_self_cluster_id();
      for (int64_t i = 0; i < list.get_member_number() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(list.get_server_by_index(i, server))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K_(partition_key));
        } else if (self_ == server) {
          // skip self
        } else if (OB_FAIL(log_engine_->submit_push_ms_log_req(server, cluster_id, partition_key_, &net_task))) {
          CLOG_LOG(WARN, "submit_push_ms_log_req failed", K_(partition_key), K(ret), K(header));
        } else {
          CLOG_LOG(TRACE, "submit_push_ms_log_req success", K_(partition_key), K(server), K(header));
        }
      }
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::submit_renew_ms_log_to_children_(
    const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size)
{
  int ret = OB_SUCCESS;
  if (cascading_mgr_->has_valid_child()) {
    // renew_ms_log in standby cluster carries ms_proposal_id
    ObProposalID proposal_id = mm_->get_ms_proposal_id();
    ObLogNetTask net_task(proposal_id, serialize_buff, serialize_size);
    ObCascadMemberList list;
    list.reset();

    if (OB_FAIL(cascading_mgr_->get_children_list(list))) {
      CLOG_LOG(ERROR, "get_children_list failed", K_(partition_key), K(ret));
    } else if (list.is_valid()) {
      ObCascadMember member;
      for (int64_t i = 0; i < list.get_member_number() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(list.get_member_by_index(i, member))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K_(partition_key));
        } else if (self_ == member.get_server()) {
          // skip self
        } else if (OB_FAIL(log_engine_->submit_push_ms_log_req(
                       member.get_server(), member.get_cluster_id(), partition_key_, &net_task))) {
          CLOG_LOG(WARN, "submit_push_ms_log_req failed", K_(partition_key), K(ret), K(header));
        } else {
          CLOG_LOG(TRACE, "submit_push_ms_log_req success", K_(partition_key), K(member), K(header));
        }
      }
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::set_renew_ms_log_confirmed(
    const uint64_t log_id, const int64_t submit_timestamp, const common::ObProposalID& proposal_id)
{
  // no need write ilog for renew_ms_log
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_can_elect_standby_leader()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    renew_ms_task_lock_.lock();
    ObLogTask& log_task = (cur_renew_ms_task_.log_task_);
    uint64_t cur_log_id = cur_renew_ms_task_.log_id_;
    int64_t cur_submit_ts = log_task.get_submit_timestamp();
    ObProposalID cur_ms_proposal_id = mm_->get_ms_proposal_id();
    if (log_id != cur_log_id || cur_submit_ts != submit_timestamp || proposal_id != cur_ms_proposal_id) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "log_id or ms_proposal_id not match with cur_renew_ms_task",
          K_(partition_key),
          K(log_id),
          K(cur_log_id),
          K(cur_submit_ts),
          K(submit_timestamp),
          K(proposal_id),
          K(cur_ms_proposal_id),
          K(log_task));
    } else {
      log_task.set_log_confirmed();
    }
    renew_ms_task_lock_.unlock();

    int tmp_ret = OB_SUCCESS;
    int64_t fake_accu_checksum = 0;
    if (OB_SUCCESS != (tmp_ret = standby_leader_submit_confirmed_info_(log_id, log_task, fake_accu_checksum))) {
      CLOG_LOG(WARN,
          "standby_leader_submit_confirmed_info_ failed",
          K(tmp_ret),
          K_(partition_key),
          K(log_id),
          K(cur_log_id));
    }
  }

  CLOG_LOG(INFO, "set_renew_ms_log_confirmed finished", K(ret), K_(partition_key), K(log_id), K(submit_timestamp));
  return ret;
}

int ObLogMembershipTaskMgr::receive_renew_ms_log_confirmed_info(
    const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_can_elect_standby_leader() || ms_proposal_id != mm_->get_ms_proposal_id()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "state not match when receive_renew_ms_log_confirmed_info",
        K_(partition_key),
        K(ret),
        K(log_id),
        K(confirmed_info),
        K(ms_proposal_id));
  } else if (OB_FAIL(submit_confirmed_info_(log_id, ms_proposal_id, confirmed_info, false))) {
    CLOG_LOG(WARN,
        "submit_confirmed_info_ failed",
        K_(partition_key),
        K(ret),
        K(log_id),
        K(confirmed_info),
        K(ms_proposal_id));
  } else if (OB_FAIL(submit_renew_ms_confirmed_info_to_net_(log_id, ms_proposal_id, confirmed_info))) {
    CLOG_LOG(WARN,
        "submit_renew_ms_confirmed_info_to_net_ failed",
        K_(partition_key),
        K(ret),
        K(log_id),
        K(confirmed_info),
        K(ms_proposal_id));
  } else {
  }  // do nothing

  CLOG_LOG(INFO,
      "submit_confirmed_info_ finished",
      K_(partition_key),
      K(ret),
      K(log_id),
      K(confirmed_info),
      K(ms_proposal_id));
  return ret;
}

int ObLogMembershipTaskMgr::standby_leader_submit_confirmed_info_(
    const uint64_t log_id, const ObLogTask& log_task, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_can_elect_standby_leader()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    const ObProposalID ms_proposal_id = mm_->get_ms_proposal_id();
    ObConfirmedInfo confirmed_info;
    log_task.lock();
    const int64_t data_checksum = log_task.get_data_checksum();
    const int64_t epoch_id = log_task.get_epoch_id();
    log_task.unlock();
    if (OB_FAIL(confirmed_info.init(data_checksum, epoch_id, accum_checksum))) {
      CLOG_LOG(ERROR, "confirmed_info init failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(submit_confirmed_info_(log_id, ms_proposal_id, confirmed_info, true))) {
      CLOG_LOG(WARN, "submit_confirmed_info_ failed", K_(partition_key), K(ret), K(log_id), K(confirmed_info));
    } else if (OB_FAIL(submit_renew_ms_confirmed_info_to_net_(log_id, ms_proposal_id, confirmed_info))) {
      CLOG_LOG(WARN,
          "submit_renew_ms_confirmed_info_to_net_ failed",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info));
    } else {
      CLOG_LOG(INFO,
          "standby_leader_submit_confirmed_info_ success",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(log_task));
    }
  }
  return ret;
}

int ObLogMembershipTaskMgr::set_renew_ms_log_flushed_succ(
    const uint64_t log_id, const int64_t submit_timestamp, const ObProposalID proposal_id, bool& majority)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_can_elect_standby_leader()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObProposalID cur_ms_proposal_id = mm_->get_ms_proposal_id();
    renew_ms_task_lock_.lock();
    ObLogTask& log_task = (cur_renew_ms_task_.log_task_);
    const uint64_t cur_log_id = cur_renew_ms_task_.log_id_;
    const int64_t cur_log_ts = log_task.get_submit_timestamp();
    if (log_id != cur_log_id || proposal_id != cur_ms_proposal_id || submit_timestamp != cur_log_ts) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "log_id not match with cur_renew_ms_task",
          K_(partition_key),
          K(log_id),
          K(cur_log_id),
          K(submit_timestamp),
          K(proposal_id),
          K(cur_ms_proposal_id),
          K(log_task));
    } else {
      log_task.lock();
      if (!log_task.is_submit_log_exist()) {
        CLOG_LOG(WARN,
            "cur_renew_ms_task submit_log not exist",
            K_(partition_key),
            K(log_id),
            K(cur_log_id),
            K(proposal_id),
            K(cur_ms_proposal_id),
            K(log_task));
      } else if (proposal_id != log_task.get_proposal_id()) {
        CLOG_LOG(INFO,
            "log is overwritten during disk flushing",
            K_(partition_key),
            "prev proposal id",
            proposal_id,
            "current id",
            log_task.get_proposal_id(),
            K(log_id));
      } else {
        log_task.set_flush_local_finished();
        majority = log_task.try_set_majority_finished();
      }
      log_task.unlock();
    }
    renew_ms_task_lock_.unlock();

    CLOG_LOG(INFO,
        "set_renew_ms_log_flushed_succ finished",
        K_(partition_key),
        K(proposal_id),
        K(log_id),
        K(majority),
        K(log_task));
  }

  return ret;
}

int ObLogMembershipTaskMgr::receive_renew_ms_log(const ObLogEntry& log_entry, const ObRenewMembershipLog& renew_ms_log,
    const ObAddr& server, const int64_t cluster_id, const ReceiveLogType type)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const bool send_slave = false;
  const bool need_replay = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || type <= RL_TYPE_UNKNOWN || type > FETCH_LOG) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(log_entry), K(server), K(type));
  } else if (OB_FAIL(submit_renew_ms_log_(renew_ms_log,
                 log_entry.get_header(),
                 log_entry.get_buf(),
                 NULL,
                 need_replay,
                 send_slave,
                 server,
                 cluster_id))) {
    CLOG_LOG(WARN, "submit_renew_ms_log_ failed", K(ret), K_(partition_key), K(log_id));
  } else {
    CLOG_LOG(INFO, "receive_renew_ms_log success", K(server), K_(partition_key), K(log_id));
  }
  return ret;
}

int ObLogMembershipTaskMgr::renew_ms_log_majority_cb(
    const uint64_t log_id, const int64_t submit_timestamp, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  ObLogTask* log_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_can_elect_standby_leader()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    renew_ms_task_lock_.lock();
    log_task = &(cur_renew_ms_task_.log_task_);
    uint64_t cur_log_id = cur_renew_ms_task_.log_id_;
    int64_t cur_submit_ts = log_task->get_submit_timestamp();
    const ObProposalID cur_ms_proposal_id = mm_->get_ms_proposal_id();
    if (log_id != cur_log_id || cur_submit_ts != submit_timestamp || proposal_id != cur_ms_proposal_id) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "log_id not match with cur_renew_ms_task",
          K_(partition_key),
          K(log_id),
          K(cur_log_id),
          K(cur_submit_ts),
          K(submit_timestamp),
          K(proposal_id),
          K(cur_ms_proposal_id),
          K(*log_task));
    } else if (OB_FAIL(log_task->submit_log_succ_cb(partition_key_, log_id, false, false))) {
      CLOG_LOG(WARN, "submit log majority_cb failed", K_(partition_key), K(log_id), K(ret));
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(INFO,
          "standby_renew_ms_log_majority_cb success",
          K_(partition_key),
          K(log_id),
          K(submit_timestamp),
          K(proposal_id));
    }
    renew_ms_task_lock_.unlock();
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
