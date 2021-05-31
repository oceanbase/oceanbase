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

#include "ob_log_restore_mgr.h"
#include "archive/ob_archive_restore_engine.h"
#include "lib/atomic/ob_atomic.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/partition_table/ob_partition_info.h"
#include "storage/ob_partition_service.h"
#include "archive/ob_archive_restore_engine.h"

namespace oceanbase {
using namespace archive;
using namespace common;
using namespace storage;

namespace clog {

int ObLogRestoreMgr::init(ObPartitionService* partition_service, ObArchiveRestoreEngine* restore_engine,
    ObILogSWForStateMgr* sw, ObILogEngine* log_engine, ObILogMembershipMgr* mm, const ObPartitionKey& pkey,
    const ObAddr& self, const int16_t archive_restore_state)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == partition_service
             //      || NULL == restore_engine
             || NULL == sw || NULL == log_engine || NULL == mm || !pkey.is_valid() || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(partition_service), KP(sw), K(pkey), K(self));
  } else {
    partition_service_ = partition_service;
    archive_restore_engine_ = restore_engine;
    sw_ = sw;
    log_engine_ = log_engine;
    mm_ = mm;
    role_ = FOLLOWER;
    last_takeover_time_ = OB_INVALID_TIMESTAMP;
    partition_key_ = pkey;
    self_ = self;
    archive_restore_state_ = archive_restore_state;
    if (share::REPLICA_RESTORE_LOG < archive_restore_state_ && share::REPLICA_RESTORE_MAX >= archive_restore_state_) {
      // if fetch clog finished, update restore_log_finish_ts
      restore_log_finish_ts_ = ObTimeUtility::current_time();
    }
    last_alive_req_send_ts_ = OB_INVALID_TIMESTAMP;
    last_leader_resp_time_ = OB_INVALID_TIMESTAMP;
    fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_INIT;
    is_inited_ = true;
    CLOG_LOG(
        INFO, "ObLogRestoreMgr init success", K(ret), K(pkey), K_(archive_restore_state), K_(restore_log_finish_ts));
  }
  return ret;
}

void ObLogRestoreMgr::destroy()
{
  ObSpinLockGuard guard(lock_);
  CLOG_LOG(INFO, "ObLogRestoreMgr destroy begin", K_(partition_key), K_(is_inited));
  if (is_inited_) {
    is_inited_ = false;
    role_ = FOLLOWER;
    archive_restore_state_ = 0;
    restore_leader_.reset();
    partition_service_ = NULL;
    fetch_log_result_ = OB_ARCHIVE_FETCH_LOG_INIT;
    sw_ = NULL;
    log_engine_ = NULL;
    mm_ = NULL;
    CLOG_LOG(INFO, "ObLogRestoreMgr destroy finished", K_(partition_key));
  }
}

int ObLogRestoreMgr::leader_takeover_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (RESTORE_LEADER == role_) {
    CLOG_LOG(WARN, "self is already resore leader", K(ret), K_(partition_key), K_(role));
  } else {
    role_ = RESTORE_LEADER;
    restore_leader_ = self_;
    // async update meta table
    if (OB_FAIL(partition_service_->submit_pt_update_task(partition_key_))) {
      CLOG_LOG(WARN, "submit_pt_update_task failed", K(ret), K_(partition_key));
    } else {
      // update check time
      const int64_t now = ObTimeUtility::current_time();
      if (now > last_takeover_time_) {
        last_takeover_time_ = now;
      } else {
        last_takeover_time_ += 1;
      }
      if (OB_FAIL(try_submit_restore_task_())) {
        CLOG_LOG(WARN, "try_submit_restore_task_ failed", K(ret), K_(partition_key));
      }
    }
    if (OB_FAIL(ret)) {
      // revoke when failure
      leader_revoke_();
    }
  }
  CLOG_LOG(INFO, "leader takeover finished", K(ret), K_(partition_key), K_(last_takeover_time));
  return ret;
}

int ObLogRestoreMgr::leader_revoke_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (FOLLOWER == role_) {
    CLOG_LOG(WARN, "self is follower, no need revoke", K(ret), K_(partition_key), K_(role));
  } else {
    role_ = FOLLOWER;
    restore_leader_.reset();
    // reset last_leader_resp_time when revoke
    last_leader_resp_time_ = ObTimeUtility::current_time();
    CLOG_LOG(INFO, "leader revoke", K(ret), K_(partition_key));
  }
  return ret;
}

int ObLogRestoreMgr::leader_check_role_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObAddr loc_leader;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = partition_service_->get_restore_leader_from_loc_cache(partition_key_, loc_leader, false))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "get_leader_from_loc_cache failed", K_(partition_key), K(tmp_ret));
      }
    }
    if (loc_leader.is_valid()) {
      if (self_ == loc_leader) {
        // leader not change, skip
      } else {
        // location leader is valid and not self, need revoke
        (void)leader_revoke_();
        restore_leader_ = loc_leader;
      }
    } else {
      // there is no leader in meta table, need report
      (void)partition_service_->submit_pt_update_task(partition_key_);
    }
  }
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    CLOG_LOG(INFO, "leader_check_role_ finished", K(ret), K_(partition_key), K_(restore_leader));
  }

  return ret;
}

int ObLogRestoreMgr::try_send_alive_req_()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (FOLLOWER == role_ && restore_leader_.is_valid()) {
    if (now - last_alive_req_send_ts_ < RESTORE_ALIVE_REQ_INTERVAL) {
      // no need send alive req
    } else if (OB_FAIL(log_engine_->send_restore_alive_req(restore_leader_, partition_key_))) {
      CLOG_LOG(WARN, "send_restore_alive_req failed", K(ret), K_(partition_key), K_(restore_leader));
    } else {
      last_alive_req_send_ts_ = now;
      CLOG_LOG(
          DEBUG, "send_restore_alive_req success", K_(partition_key), K_(restore_leader), K_(last_alive_req_send_ts));
    }
  }

  return ret;
}

int ObLogRestoreMgr::follower_check_state_()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (OB_INVALID_TIMESTAMP == last_leader_resp_time_) {
    (void)try_renew_location_();
    last_leader_resp_time_ = ObTimeUtility::current_time();
  } else if (now - last_leader_resp_time_ < RENEW_LOCATION_INTERVAL) {
    // no need renew location
  } else {
    ObAddr loc_leader;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = partition_service_->get_restore_leader_from_loc_cache(partition_key_, loc_leader, true))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "get_leader_from_loc_cache failed", K_(partition_key), K(tmp_ret));
      }
    }
    if (!loc_leader.is_valid() || restore_leader_ == loc_leader) {
      if (now - last_leader_resp_time_ >= RESTORE_LEADER_TIMEOUT_THRESHOLD &&
          !ObReplicaTypeCheck::is_log_replica(mm_->get_replica_type()) &&
          (share::REPLICA_RESTORE_DATA <= archive_restore_state_ &&
              archive_restore_state_ <= share::REPLICA_RESTORE_WAIT_ALL_DUMPED)) {
        CLOG_LOG(INFO, "detect leader dead, self try takeover", K_(partition_key), K_(restore_leader));
        (void)leader_takeover_();
      }
    } else if (self_ == loc_leader) {
      (void)leader_takeover_();
    } else {
      restore_leader_ = loc_leader;
    }
  }
  if (REACH_TIME_INTERVAL(100 * 1000)) {
    CLOG_LOG(INFO, "follower_check_state_ finished", K(ret), K_(partition_key), K_(restore_leader));
  }

  return ret;
}

int ObLogRestoreMgr::try_renew_location_()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (now - last_renew_location_time_ < RENEW_LOCATION_INTERVAL) {
    // no reach renew interval, skip
  } else {
    last_renew_location_time_ = now;
    (void)partition_service_->nonblock_renew_loc_cache(partition_key_);
  }

  return ret;
}

int ObLogRestoreMgr::check_state()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (now - last_check_state_time_ < RESTORE_CHECK_STATE_INTERVAL) {
    // not reach interval, skip
  } else if (!is_archive_restoring()) {
  } else {
    ObSpinLockGuard guard(lock_);
    last_check_state_time_ = now;
    if (RESTORE_LEADER == role_) {
      (void)try_renew_location_();
      (void)leader_check_role_();
    } else {
      (void)follower_check_state_();
      (void)try_send_alive_req_();
    }
  }
  return ret;
}

int ObLogRestoreMgr::record_leader_resp_time(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(server));
  } else if (restore_leader_ != server) {
    // not leader, skip
  } else {
    const int64_t now = ObTimeUtility::current_time();
    inc_update(last_leader_resp_time_, now);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "record_leader_resp_time", K(now), K_(partition_key));
    }
  }

  return ret;
}

int ObLogRestoreMgr::leader_takeover()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObSpinLockGuard guard(lock_);
    (void)leader_takeover_();
  }
  return ret;
}

int ObLogRestoreMgr::try_submit_restore_task_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (share::REPLICA_RESTORE_LOG != archive_restore_state_) {
    // state not match, skip
  } else if (RESTORE_LEADER != role_) {
    // self not restore_leader, skip
  } else {
    const uint64_t start_log_id = sw_->get_start_id();
    uint64_t log_id = OB_INVALID_ID;
    int64_t log_ts = OB_INVALID_TIMESTAMP;
    sw_->get_next_replay_log_id_info(log_id, log_ts);
    if (OB_UNLIKELY(start_log_id != log_id)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "invalid log_id", KR(ret), K_(partition_key), K(start_log_id), K(log_id), K(log_ts));
    } else if (OB_FAIL(
                   archive_restore_engine_->submit_restore_task(partition_key_, log_id, log_ts, last_takeover_time_))) {
      CLOG_LOG(WARN, "submit_restore_task failed", K(ret), K_(partition_key), K(log_id), K(log_ts));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogRestoreMgr::set_archive_restore_state(const int16_t new_restore_state)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (new_restore_state == archive_restore_state_) {
    // no nedd update
  } else {
    archive_restore_state_ = new_restore_state;
    SERVER_EVENT_ADD(
        "physical_restore", "switch_state", "partition", partition_key_, "restore_state", new_restore_state);
    if (share::REPLICA_RESTORE_DUMP_MEMTABLE == new_restore_state) {
      restore_log_finish_ts_ = ObTimeUtility::current_time();
    }
    if (RESTORE_LEADER == role_ && share::REPLICA_RESTORE_LOG == archive_restore_state_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_submit_restore_task_())) {
        CLOG_LOG(WARN, "try_submit_restore_task_ failed", K(tmp_ret), K_(partition_key));
        leader_revoke_();
      }
    }
  }
  CLOG_LOG(INFO, "set_archive_restore_state finished", K(ret), K_(partition_key), K(new_restore_state));
  return ret;
}

int ObLogRestoreMgr::set_fetch_log_result(ObArchiveFetchLogResult result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
          OB_ARCHIVE_FETCH_LOG_FINISH_AND_NOT_ENOUGH != result && OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH != result)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(result), K_(partition_key));
  } else {
    fetch_log_result_ = result;
  }
  return ret;
}

void ObLogRestoreMgr::reset_archive_restore_state()
{
  archive_restore_state_ = 0;
}

bool ObLogRestoreMgr::is_archive_restoring_log() const
{
  return (archive_restore_state_ == share::REPLICA_RESTORE_LOG);
}

bool ObLogRestoreMgr::is_archive_restoring_mlist() const
{
  return (archive_restore_state_ == share::REPLICA_RESTORE_MEMBER_LIST);
}

bool ObLogRestoreMgr::is_standby_restore_state() const
{
  return (archive_restore_state_ == share::REPLICA_RESTORE_STANDBY);
}

bool ObLogRestoreMgr::can_update_next_replay_log_ts_in_restore() const
{
  return (archive_restore_state_ == share::REPLICA_RESTORE_DUMP_MEMTABLE ||
          archive_restore_state_ == share::REPLICA_RESTORE_WAIT_ALL_DUMPED ||
          archive_restore_state_ == share::REPLICA_RESTORE_MEMBER_LIST);
}

bool ObLogRestoreMgr::is_restore_log_finished() const
{
  return (archive_restore_state_ == share::REPLICA_NOT_RESTORE ||
          archive_restore_state_ == share::REPLICA_RESTORE_DUMP_MEMTABLE ||
          archive_restore_state_ == share::REPLICA_RESTORE_WAIT_ALL_DUMPED ||
          archive_restore_state_ == share::REPLICA_RESTORE_MEMBER_LIST);
}

bool ObLogRestoreMgr::is_archive_restoring() const
{
  return (archive_restore_state_ >= share::REPLICA_RESTORE_DATA &&
          archive_restore_state_ < share::REPLICA_RESTORE_MEMBER_LIST);
}

int ObLogRestoreMgr::change_restore_leader(const ObAddr& new_leader)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObSpinLockGuard guard(lock_);
    if (RESTORE_LEADER == role_) {
      leader_revoke_();
      if (OB_FAIL(log_engine_->notify_restore_leader_takeover(new_leader, partition_key_))) {
        CLOG_LOG(WARN, "notify_restore_leader_takeover failed", K(ret), K_(partition_key));
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "self is not restore_leader", K(ret), K_(role), K_(partition_key));
    }
  }
  CLOG_LOG(INFO, "change_restore_leader finished", K(ret), K(new_leader), K_(role), K_(partition_key));

  return ret;
}

}  // namespace clog
}  // namespace oceanbase
