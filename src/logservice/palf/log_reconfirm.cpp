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

#define USING_LOG_PREFIX PALF
#include "log_reconfirm.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "log_group_entry.h"
#include "log_meta_info.h"
#include "log_meta_entry.h"
#include "log_meta_entry_header.h"
#include "log_config_mgr.h"
#include "log_mode_mgr.h"
#include "log_sliding_window.h"
#include "log_state_mgr.h"
#include "log_engine.h"
#include "log_io_task_cb_utils.h"
#include "logservice/palf/log_define.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"

namespace oceanbase
{
using namespace common;

namespace palf
{
LogReconfirm::LogReconfirm()
    : state_(INITED),
      palf_id_(),
      self_(),
      new_proposal_id_(INVALID_PROPOSAL_ID),
      prepare_log_ack_list_(),
      curr_paxos_follower_list_(),
      majority_cnt_(0),
      majority_max_log_server_(),
      majority_max_accept_pid_(INVALID_PROPOSAL_ID),
      majority_max_lsn_(),
      saved_end_lsn_(),
      sw_config_version_(),
      sw_(NULL),
      state_mgr_(NULL),
      mm_(NULL),
      mode_mgr_(NULL),
      log_engine_(NULL),
      lock_(ObLatchIds::CLOG_RECONFIRM_LOCK),
      last_submit_prepare_req_time_us_(OB_INVALID_TIMESTAMP),
      last_fetch_log_time_us_(OB_INVALID_TIMESTAMP),
      last_record_sw_start_id_(OB_INVALID_LOG_ID),
      wait_slide_print_time_us_(OB_INVALID_TIMESTAMP),
      wait_majority_time_us_(OB_INVALID_TIMESTAMP),
      last_notify_fetch_time_us_(OB_INVALID_TIMESTAMP),
      last_purge_throttling_time_us_(OB_INVALID_TIMESTAMP),
      is_inited_(false)
{}

int LogReconfirm::init(const int64_t palf_id,
                       const ObAddr &self,
                       LogSlidingWindow *sw,
                       LogStateMgr *state_mgr,
                       LogConfigMgr *mm,
                       LogModeMgr *mode_mgr,
                       LogEngine *log_engine)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(sw) ||
             OB_ISNULL(state_mgr) ||
             OB_ISNULL(mm) ||
             OB_ISNULL(mode_mgr) ||
             OB_ISNULL(log_engine) ||
             !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    palf_id_ = palf_id;
    self_ = self;
    sw_ = sw;
    state_mgr_ = state_mgr;
    mm_ = mm;
    mode_mgr_ = mode_mgr;
    log_engine_ = log_engine;
    is_inited_ = true;
    PALF_LOG(INFO, "LogReconf init success", K(ret), K(palf_id));
  }
  return ret;
}

void LogReconfirm::reset_state()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  if (state_ != INITED) {
    state_ = INITED;
    new_proposal_id_ = INVALID_PROPOSAL_ID;
    prepare_log_ack_list_.reset();
    curr_paxos_follower_list_.reset();
    majority_cnt_ = 0;
    majority_max_log_server_.reset();
    majority_max_accept_pid_ = INVALID_PROPOSAL_ID;
    majority_max_lsn_.reset();
    saved_end_lsn_.reset();
    follower_end_lsn_list_.reset();
    sw_config_version_.reset();
    last_submit_prepare_req_time_us_ = OB_INVALID_TIMESTAMP;
    last_fetch_log_time_us_ = OB_INVALID_TIMESTAMP;
    last_record_sw_start_id_ = OB_INVALID_LOG_ID;
    last_notify_fetch_time_us_ = OB_INVALID_TIMESTAMP;
    last_purge_throttling_time_us_ = OB_INVALID_TIMESTAMP;
  }
}

void LogReconfirm::destroy()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  if (is_inited_) {
    is_inited_ = false;
    last_notify_fetch_time_us_ = OB_INVALID_TIMESTAMP;
    last_purge_throttling_time_us_ = OB_INVALID_TIMESTAMP;
    state_ = INITED;
    new_proposal_id_ = INVALID_PROPOSAL_ID;
    prepare_log_ack_list_.reset();
    curr_paxos_follower_list_.reset();
    majority_cnt_ = 0;
    majority_max_log_server_.reset();
    majority_max_accept_pid_ = INVALID_PROPOSAL_ID;
    majority_max_lsn_.reset();
    saved_end_lsn_.reset();
    follower_end_lsn_list_.destroy();
    sw_config_version_.reset();
    sw_ = NULL;
    state_mgr_ = NULL;
    mm_ = NULL;
    mode_mgr_ = NULL;
    log_engine_ = NULL;
    last_submit_prepare_req_time_us_ = OB_INVALID_TIMESTAMP;
    last_fetch_log_time_us_ = OB_INVALID_TIMESTAMP;
    last_record_sw_start_id_ = OB_INVALID_LOG_ID;
  }
}

bool LogReconfirm::need_wlock()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  // Note: even the wlock is not required in INITED state,
  // we still add it to speedup the reconfirmation
  return INITED == state_ ||
         WAITING_LOG_FLUSHED == state_ ||
         FETCH_MAX_LOG_LSN == state_ ||
         RECONFIRM_MODE_META == state_ ||
         RECONFIRMING == state_ ||
         START_WORKING == state_;
}

int LogReconfirm::init_reconfirm_()
{
  int ret = OB_SUCCESS;
  LSN max_lsn;
  int64_t max_log_proposal_id = INVALID_PROPOSAL_ID;
  ObMemberList member_list;
  int64_t replica_num = 0;
  if (OB_FAIL(mm_->get_alive_member_list_with_arb(member_list, replica_num))) {
    PALF_LOG(WARN, "get_alive_member_list_with_arb failed", K_(palf_id));
  } else if (!member_list.contains(self_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "self is not in curr_member_list", K_(palf_id), K(member_list), K(self_));
  } else if (OB_FAIL(curr_paxos_follower_list_.deep_copy(member_list))) {
    PALF_LOG(ERROR, "deep_copy failed", K_(palf_id));
  } else if (OB_FAIL(curr_paxos_follower_list_.remove_server(self_))) {
    PALF_LOG(ERROR, "remove_server failed", K_(palf_id), K_(curr_paxos_follower_list), K_(self), KP(mm_));
  } else {
    majority_cnt_ = replica_num / 2 + 1;
    PALF_LOG(INFO, "init_reconfirm_ success", K(ret), K_(palf_id), K(member_list), K_(self), K_(majority_cnt), K(replica_num));
  }
  return ret;
}

int LogReconfirm::submit_prepare_log_()
{
  int ret = OB_SUCCESS;
  LSN unused_prev_lsn;
	LSN max_flushed_end_lsn;
	int64_t max_flushed_log_pid = INVALID_PROPOSAL_ID;
  const int64_t now_us = ObTimeUtility::current_time();
  if (OB_FAIL(sw_->get_max_flushed_log_info(unused_prev_lsn, max_flushed_end_lsn, max_flushed_log_pid))) {
    PALF_LOG(WARN, "get_max_flushed_log_info failed", K_(palf_id));
  } else if (OB_EAGAIN != (ret = mode_mgr_->reconfirm_mode_meta())) {
    PALF_LOG(WARN, "reconfirm_mode_meta failed", K_(palf_id));
  } else {
    // clear prepare_log_ack_list_
    prepare_log_ack_list_.reset();
    majority_max_log_server_ = self_;
    majority_max_lsn_ = max_flushed_end_lsn;
    // the init 'majority_max_accept_pid_' should be the maximum of max_flushed_log_proposl_id and
    // proposal_id of config log, otherwise there will are some ghost logs.
    //
    // assume that,
    // first round, replica A is an origin leader, and submit ten logs, but just only the first
    // five log are confirmed, and then;
    // second round replica B change to leader, and just submit START_WORKING to others;
    // third round, replica C change to leader, and it will think the newest server is A,
    // so it will reconfirm the logs generated by A, these logs are ghost.
    //
    // to avoid this problem, A need truncate these logs if it receives the START_WORKING from B.
    // if A does not receive the START_WORKING from B, others should return the proposal_id which
    // is the maxest of max_flushed_log_proposl_id and proposal_id of config log.
    if (INVALID_PROPOSAL_ID != max_flushed_log_pid) {
      majority_max_accept_pid_ = max_flushed_log_pid;
    }
    const int64_t local_accept_proposal_id = mm_->get_accept_proposal_id();
    if (INVALID_PROPOSAL_ID != local_accept_proposal_id
        && INVALID_PROPOSAL_ID != majority_max_accept_pid_
        && local_accept_proposal_id > majority_max_accept_pid_) {
      majority_max_accept_pid_ = local_accept_proposal_id;
    }

    // NB: 'state_mgr_' need guarantee that get_proposal_id will return
    // the maxest value of redo log and prepare meta
    const int64_t old_proposal_id = state_mgr_->get_proposal_id();
    new_proposal_id_ = old_proposal_id + 1;

    if (OB_FAIL(state_mgr_->handle_prepare_request(self_, new_proposal_id_))) {
      PALF_LOG(WARN, "handle_prepare_request failed", K_(palf_id), K_(self), K(new_proposal_id_));
    } else if (curr_paxos_follower_list_.is_valid()
        && OB_FAIL(log_engine_->submit_prepare_meta_req(curr_paxos_follower_list_, new_proposal_id_))) {
			PALF_LOG(WARN, "submit_prepare_meta_req failed", K_(palf_id), K_(self), K_(new_proposal_id));
		} else {
      const int64_t old_prepare_time_us = last_submit_prepare_req_time_us_;
      last_submit_prepare_req_time_us_ = now_us;
			PALF_LOG(INFO, "submit_prepare_meta_req success", K(ret), K_(palf_id), K(curr_paxos_follower_list_), K(prepare_log_ack_list_),
					K(old_proposal_id), K(new_proposal_id_), K(self_), K(majority_max_lsn_), K(majority_max_log_server_),
					K(majority_max_accept_pid_), K(now_us), K(old_prepare_time_us), K(last_submit_prepare_req_time_us_), K(max_flushed_log_pid),
          K(local_accept_proposal_id));
		}
  }
  return ret;
}

int LogReconfirm::wait_all_log_flushed_()
{
  int ret = OB_SUCCESS;
  if (false == sw_->check_all_log_has_flushed()) {
    ret = OB_EAGAIN;
    PALF_LOG(INFO, "There are some logs flushing, need retry", K(ret), K_(palf_id));
  } else {
    PALF_LOG(INFO, "wait_all_log_flushed_ success", K(ret), K_(palf_id));
  }
  return ret;
}

int LogReconfirm::handle_prepare_response(const common::ObAddr &server,
                                          const int64_t &src_proposal_id,
                                          const int64_t &accept_proposal_id,
                                          const LSN &last_lsn,
                                          const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == src_proposal_id
      || INVALID_PROPOSAL_ID == accept_proposal_id || !last_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K_(palf_id), K(server), K(src_proposal_id),
        K(accept_proposal_id), K(last_lsn), K(committed_end_lsn));
    // NB: no need change 'majority_max_log_server_' after FETCH_MAX_LOG_LSN
  } else if (src_proposal_id != new_proposal_id_ || FETCH_MAX_LOG_LSN != state_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "state not match", K_(palf_id), K(server), K(src_proposal_id), K_(new_proposal_id),
        K(state_));
  } else if (false == curr_paxos_follower_list_.contains(server)) {
    PALF_LOG(WARN, "receive prepare req not in follower_list", K_(palf_id), K(server),
        K_(curr_paxos_follower_list));
  } else if (OB_FAIL(prepare_log_ack_list_.add_server(server))) {
    PALF_LOG(WARN, "prepare_log_ack_list_ add_sever failed", K_(palf_id), K(server));
  } else if (INVALID_PROPOSAL_ID != majority_max_accept_pid_
             && accept_proposal_id < majority_max_accept_pid_) {
    // accept_proposal_id is smaller than cur majority value, ignore
    PALF_LOG(INFO, "server's proposal_id is smaller than me, ignore it", K(ret), K_(palf_id), K(server), K(src_proposal_id),
        K(accept_proposal_id), K(last_lsn), K(majority_max_accept_pid_), K(majority_max_lsn_));
  } else {
    // if server's proposal_id is equal with me, compare with lsn.
    if (accept_proposal_id == majority_max_accept_pid_) {
      if (last_lsn > majority_max_lsn_) {
        majority_max_log_server_ = server;
        majority_max_lsn_ = last_lsn;
      }
    } else if (INVALID_PROPOSAL_ID == majority_max_accept_pid_
               || accept_proposal_id > majority_max_accept_pid_) {
      majority_max_lsn_ = last_lsn;
      majority_max_accept_pid_ = accept_proposal_id;
      majority_max_log_server_ = server;
      PALF_LOG(INFO, "the replica's accept_proposal_id is greater than majority_max_accept_pid_", K(ret),
          K_(palf_id), K(server), K(src_proposal_id), K(accept_proposal_id), K(last_lsn));
    } else {
      // do nothing
    }
    // record committed_end_lsn of followers
    // Try to update its match_lsn to committed_end_lsn, because the server's committed_end_lsn
    // is maybe larger than self(advanced by old leader).
    // Suppose A is leader with reconfirm state, B, C are followers:
    // max_flushed_end_lsn: A(200), B(200), C(200)
    // committed_end_lsn:   A(100), B(200), C(200)
    // For this case, followers' match_lsn at A will be 100, and it has no chance to be updated during reconfirm,
    // which will lead to waiting majority sync timeout.
    if (committed_end_lsn.is_valid()) {
      // old version observer's resp may not have this parameter.
      // So it may be invalid.
      (void) update_follower_end_lsn_(server, committed_end_lsn);
    } else {
      PALF_LOG(INFO, "committed_end_lsn in prepare response is invalid, it may be from old version member",
          K(ret), K_(palf_id), K(server), K(accept_proposal_id), K(last_lsn), K(committed_end_lsn));
    }
  }
  PALF_LOG(INFO, "handle_prepare_response finished", K(ret), K_(palf_id), K(server), K(accept_proposal_id), K(last_lsn),
      K(majority_max_accept_pid_), K(majority_max_lsn_), K(prepare_log_ack_list_));
  return ret;
}

int LogReconfirm::update_follower_end_lsn_(const common::ObAddr &server, const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  if (false == server.is_valid() || false == committed_end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 != (index = ack_info_list_get_index(follower_end_lsn_list_, server))) {
    if (follower_end_lsn_list_[index].last_flushed_end_lsn_ < committed_end_lsn) {
      follower_end_lsn_list_[index].last_flushed_end_lsn_ = committed_end_lsn;
    }
  } else {
    LogMemberAckInfo log_info(common::ObMember(server, 1), 0, committed_end_lsn);
    follower_end_lsn_list_.push_back(log_info);
  }
  return ret;
}

int LogReconfirm::ack_log_with_end_lsn_()
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; idx < follower_end_lsn_list_.count(); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObAddr &server = follower_end_lsn_list_.at(idx).member_.get_server();
    const LSN &end_lsn = follower_end_lsn_list_.at(idx).last_flushed_end_lsn_;
    if (OB_SUCCESS != (tmp_ret = sw_->ack_log(server, end_lsn))) {
      PALF_LOG(WARN, "ack_log failed", K(tmp_ret), K(server), K(end_lsn));
    }
  }
  return ret;
}

bool LogReconfirm::is_fetch_log_finished_()
{
  bool bool_ret = false;
  LSN max_flushed_end_lsn;
  (void) sw_->get_max_flushed_end_lsn(max_flushed_end_lsn);
  const LSN max_lsn = sw_->get_max_lsn();
  if (majority_max_lsn_ == max_flushed_end_lsn) {
    bool_ret = true;
  } else {
    //In a scenario with writhing throttling, ensure that the fetched logs are written to disk as soon
    //as possible.
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = purge_throttling_())) {
      PALF_LOG_RET(WARN, tmp_ret, "purge throttling failed", K_(palf_id), K_(majority_max_lsn),
                   K(max_flushed_end_lsn), K(max_lsn));
    }
  }

  PALF_LOG(TRACE, "is_fetch_log_finished_", K_(palf_id), K(bool_ret), K_(majority_max_lsn),
           K(max_flushed_end_lsn), K(max_lsn));
  return bool_ret;
}

int LogReconfirm::try_fetch_log_()
{
  int ret = OB_SUCCESS;
  const int64_t now_us = ObTimeUtility::current_time();
  bool is_fetched = false;
  const int64_t sw_start_id = sw_->get_start_id();
	if (OB_INVALID_TIMESTAMP == last_fetch_log_time_us_
      || OB_INVALID_LOG_ID == last_record_sw_start_id_
      || (sw_start_id == last_record_sw_start_id_
          && now_us - last_fetch_log_time_us_ >= PALF_FETCH_LOG_INTERVAL_US)) {
    if (OB_FAIL(sw_->try_fetch_log_for_reconfirm(majority_max_log_server_, majority_max_lsn_, is_fetched))) {
      PALF_LOG(WARN, "try_fetch_log_for_reconfirm failed", K_(palf_id));
    } else if (is_fetched) {
      // send fetch req success
      last_record_sw_start_id_ = sw_start_id;
      last_fetch_log_time_us_ = now_us;
      PALF_LOG(INFO, "try_fetch_log_ success", K(ret), K_(palf_id), K_(majority_max_lsn),
          K_(majority_max_log_server), K_(self), K_(last_record_sw_start_id));
    }
  } else if (sw_start_id != last_record_sw_start_id_) {
    last_record_sw_start_id_ = sw_start_id;
    last_fetch_log_time_us_ = now_us;
  } else {
    PALF_LOG(INFO, "no need fetch log in current round", K(ret), K_(palf_id), K_(majority_max_log_server),
      K_(majority_max_lsn), K(sw_start_id), K_(last_record_sw_start_id));
  }
  return ret;
}

bool LogReconfirm::can_receive_log() const
{
  // Self can receive log only in RECONFIRM_FETCH_LOG stage.
  // Or it may receive logs with old proposal_id whose lsn is larger than majority_max_lsn.
  // These logs are unexpected, which should be dropped and rewritten.
  return (RECONFIRM_FETCH_LOG == state_);
}

bool LogReconfirm::can_do_degrade() const
{
  // degrade operation can be executed after prepare message reaches majority
  // do not protect with lock_
  // Arb Thread 1: hold lock_ of ConfigMgr -> can_do_degrade() -> lock_
  // LogLoop Thread: hold lock_ -> confirm_start_working_log() -> lock_ of ConfigMgr
  return (state_ > RECONFIRM_FETCH_LOG);
}

bool LogReconfirm::is_majority_catch_up_()
{
  // This step is necessary, because if we directly go to start_working stage:
  // 1) For 2F1A case, the A member can skip prev log check and reply ack faster than F.
  //    This will lead to an unexpected state: the other F replica lacks logs when
  //    reconfirm finishes. If leader crashes later, the data of these logs will lost.
  // 2) For all F replica case, start_working log need prev log check, so all followers
  //    also need wait log sync before process. It's similar with this step.
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  LSN majority_match_lsn;
  if (OB_SUCCESS != (tmp_ret = sw_->get_majority_match_lsn(majority_match_lsn))) {
    PALF_LOG_RET(WARN, tmp_ret, "get_majority_match_lsn failed", K(tmp_ret), K_(palf_id));
  } else if (!majority_match_lsn.is_valid()) {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "majority_match_lsn is invalid, unexpected", K_(palf_id), K(majority_match_lsn));
  } else if (majority_match_lsn < majority_max_lsn_) {
    PALF_LOG_RET(WARN, OB_SUCCESS, "majority_match_lsn is smaller than majority_max_lsn_, need wait", K_(palf_id),
        K_(majority_max_lsn), K(majority_match_lsn));
    if (palf_reach_time_interval(100 * 1000, last_notify_fetch_time_us_)) {
      ObMemberList lagged_list;
      if (OB_SUCCESS != (tmp_ret = sw_->get_lagged_member_list(majority_max_lsn_, lagged_list))) {
        PALF_LOG_RET(WARN, tmp_ret, "get_lagged_member_list_ failed", K(tmp_ret), K_(palf_id));
      } else if (OB_SUCCESS != (tmp_ret = log_engine_->submit_notify_fetch_log_req(lagged_list))) {
        PALF_LOG_RET(WARN, tmp_ret, "submit_notify_fetch_log_req failed", K(tmp_ret), K_(palf_id), K(lagged_list));
      } else {
        PALF_LOG(INFO, "notify_fetch_log success", K(tmp_ret), K_(palf_id), K_(majority_max_lsn), K(lagged_list));
      }
    }
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int LogReconfirm::reconfirm()
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogReconfirm is not init", K_(self), K_(palf_id));
  } else {
    const int64_t now_us = ObTimeUtility::current_time();
    switch (state_) {
      case INITED: {
        if (OB_FAIL(init_reconfirm_())) {
          PALF_LOG(WARN, "init reconfirm failed", K_(palf_id));
        } else {
          state_ = WAITING_LOG_FLUSHED;
          PALF_EVENT("Reconfirm come into WAITING_LOG_FLUSHED state", palf_id_, K_(self), K_(majority_cnt),
          K_(curr_paxos_follower_list));
        }
        if (state_ != WAITING_LOG_FLUSHED) {
          break;
        }
      }
      case WAITING_LOG_FLUSHED: {
        if (OB_FAIL(purge_throttling_())) {
          PALF_LOG(WARN, "purge throttling failed", K_(palf_id));
        } else if (OB_FAIL(wait_all_log_flushed_())) {
          PALF_LOG(WARN, "wait_all_log_flushed_ failed", K_(palf_id));
        } else if (OB_FAIL(submit_prepare_log_())) {
          PALF_LOG(WARN, "submit_prepare_log_ failed", K_(palf_id));
        } else {
          state_ = FETCH_MAX_LOG_LSN;
          //reset last_purge_throttling_time_us_to avoid impacting purging throttling during RECONFIRM_FETCH_LOG
          last_purge_throttling_time_us_ = OB_INVALID_TIMESTAMP;
          PALF_EVENT("Reconfirm come into FETCH_MAX_LOG_LSN state", palf_id_, K_(self), K_(majority_max_accept_pid));
        }
        break;
      }
      case FETCH_MAX_LOG_LSN: {
				const int64_t cost_ts = ObTimeUtility::current_time() - last_submit_prepare_req_time_us_;
        // NB: prepare_log_ack_list_ include self defaultly.
        if (prepare_log_ack_list_.get_count() + 1 < majority_cnt_) {
          // not reach majority in RECONFIRM_PREPARE_RETRY_INTERVAL_US, retry PREPARE phase
          // because we do not record vote_for, so leader need advance proposal_id for each retry.
          if (RECONFIRM_PREPARE_RETRY_INTERVAL_US <= cost_ts) {
            ret = submit_prepare_log_();
            PALF_LOG(WARN, "prepare_log_ack_list_ is not majority in expected interval, submit_prepare_log_ again",
								K(cost_ts), K(prepare_log_ack_list_), K(majority_cnt_), K_(palf_id));
          }
        } else {
          state_ = RECONFIRM_MODE_META;
          PALF_EVENT("Reconfirm come into RECONFIRM_MODE_META state", palf_id_, K(prepare_log_ack_list_),
              K_(majority_max_log_server), K_(majority_max_lsn));
        }
        if (state_ != RECONFIRM_MODE_META) {
          break;
        }
      }
      case RECONFIRM_MODE_META: {
        if (OB_SUCC(mode_mgr_->reconfirm_mode_meta())) {
          if (true == mode_mgr_->need_skip_log_barrier()) {
            state_ = RECONFIRMING;
            PALF_EVENT("Reconfirm come into RECONFIRMING state", palf_id_, K(prepare_log_ack_list_));
          } else {
            state_ = RECONFIRM_FETCH_LOG;
            PALF_EVENT("Reconfirm come into RECONFIRM_FETCH_LOG state", palf_id_, K(prepare_log_ack_list_));
          }
        } else if (OB_EAGAIN != ret) {
          PALF_LOG(WARN, "reconfirm_mode_meta failed", K_(palf_id));
        }
        if (state_ != RECONFIRM_FETCH_LOG) {
          break;
        }
      }
      case RECONFIRM_FETCH_LOG: {
        // Try to update its match_lsn to committed_end_lsn, because the server's committed_end_lsn
        // is maybe larger than self(advanced by old leader).
        // Suppose A is leader with reconfirm state, B, C are followers:
        // max_flushed_end_lsn: A(200), B(200), C(200)
        // committed_end_lsn:   A(100), B(200), C(200)
        // For this case, followers' match_lsn at A will be 100, and it has no chance to be updated during reconfirm,
        // which will lead to waiting majority sync timeout.
        if (OB_FAIL(ack_log_with_end_lsn_())) {
          PALF_LOG(WARN, "ack_log_with_end_lsn_", K_(follower_end_lsn_list));
        } else if (majority_max_log_server_ != self_ && !is_fetch_log_finished_()) {
          if (OB_FAIL(try_fetch_log_())) {
            PALF_LOG(WARN, "try_fetch_log_ failed", K_(palf_id));
          }
        } else {
          state_ = RECONFIRMING;
          PALF_EVENT("Reconfirm come into RECONFIRMING state", palf_id_, K_(self));
        }
        // need break because next state requires wlock
        break;
      }
      case RECONFIRMING: {
        LSN last_lsn;
        LSN last_end_lsn;
        LSN majority_match_lsn;
        int64_t last_log_proposal_id = INVALID_PROPOSAL_ID;
        const bool need_skip_log_barrier = mode_mgr_->need_skip_log_barrier();
        const bool is_cluster_already_4100 = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
        const int64_t leader_epoch = state_mgr_->get_leader_epoch();
        if (false == sw_->check_all_log_has_flushed()) {
          PALF_LOG(WARN, "check_all_log_has_flushed failed, need wait", K_(palf_id));
        // Wait majority match lsn catching up with majority_max_lsn_
        } else if (is_cluster_already_4100  // It needs majority_catch_up_ check only when cluster_version reaches 4100.
            && need_skip_log_barrier == false
            && false == is_majority_catch_up_()) {
          if (palf_reach_time_interval(1 * 1000 * 1000, wait_majority_time_us_)) {
            PALF_LOG(INFO, "is_majority_catch_up_ return false, need wait and retry", K(ret),
                K_(self), K_(palf_id), K_(majority_max_lsn));
          }
        } else if (OB_FAIL(sw_->get_max_flushed_log_info(last_lsn, last_end_lsn, last_log_proposal_id))) {
          PALF_LOG(WARN, "get_max_flushed_log_info failed", K_(self), K_(palf_id));
        } else if (OB_FAIL(mm_->confirm_start_working_log(new_proposal_id_, leader_epoch, sw_config_version_))
            && OB_EAGAIN != ret) {
          PALF_LOG(WARN, "confirm_start_working_log failed", K_(self), K_(palf_id));
        } else {
          // 记录进入start_working阶段时的end_lsn
          saved_end_lsn_ = last_end_lsn;
          state_ = START_WORKING;
          PALF_EVENT("Reconfirm come into START_WORKING state", palf_id_, K(new_proposal_id_),
              K(majority_max_log_server_), K(majority_max_lsn_), K(majority_match_lsn), K(last_lsn),
              K(last_log_proposal_id), K(last_end_lsn));
        }
        if (state_ != START_WORKING) {
          break;
        }
      }
      case START_WORKING: {
        LSN last_slide_lsn, committed_end_lsn;
        int64_t last_slide_log_id = OB_INVALID_LOG_ID;
        const bool need_skip_log_barrier = mode_mgr_->need_skip_log_barrier();
        const int64_t leader_epoch = state_mgr_->get_leader_epoch();
        if (OB_SUCC(mm_->confirm_start_working_log(new_proposal_id_, leader_epoch, sw_config_version_))) {
          // check_ms_log_committed()检查若未达成多数派会尝试重发start_working log
          // 重发时依然用之前取的lsn和proposal_id，因此saved_end_lsn_也无需更新
          const LSN curr_max_lsn = sw_->get_max_lsn();
          if (curr_max_lsn != saved_end_lsn_) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "max_lsn is not equal to saved_end_lsn_, unexpected", K_(palf_id), K_(self),
                K(curr_max_lsn), K_(saved_end_lsn));
          } else if (OB_FAIL(sw_->try_advance_committed_end_lsn(saved_end_lsn_))) {
            // start_working达成多数派后，leader推进committed_end_lsn至之前记录的位点(预期等于当前的max_lsn)
          } else if (!sw_->is_all_committed_log_slided_out(last_slide_lsn, last_slide_log_id, committed_end_lsn)) {
            // There is some log has not slided out, need wait and retry.
            if (palf_reach_time_interval(5 * 1000 * 1000, wait_slide_print_time_us_)) {
              PALF_LOG(INFO, "There is some log has not slided out, need wait and retry", K(ret), K_(palf_id),
                  K_(self), K(last_slide_lsn), K(last_slide_log_id), K(committed_end_lsn));
            }
          } else {
            PALF_EVENT("Reconfirm come into FINISHED state", palf_id_, K_(self), K_(saved_end_lsn));
            state_ = FINISHED;
          }
        }
        if (state_ != FINISHED) {
          PALF_LOG(WARN, "wait start work log majority or log slide", K_(palf_id), K(new_proposal_id_), K(majority_max_log_server_),
              K(majority_max_lsn_), K(majority_max_accept_pid_));
          break;
        }
      }
      case FINISHED: {
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
    if (state_ == FINISHED) {
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
      // election leader may has changed, so ret is maybe OB_NOT_MASTER.
      PALF_LOG(WARN, "reconfirm failed", K_(palf_id), K_(state));
    } else {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(10 * 1000)) {
        PALF_LOG(INFO, "reconfirm waiting retry", K(ret), K_(palf_id), K_(state));
      }
    }
  }
  return ret;
}

int LogReconfirm::purge_throttling_()
{
  int ret = OB_SUCCESS;
  if (palf_reach_time_interval(100 * 1000L, last_purge_throttling_time_us_)) {
    if (OB_FAIL(log_engine_->submit_purge_throttling_task(PurgeThrottlingType::PURGE_BY_RECONFIRM))) {
      PALF_LOG(WARN, "submit_purge_throttling_task", K_(palf_id));
    } else {
      PALF_LOG(INFO, "submit_purge_throttling_task during reconfirming", K_(palf_id));
    }
  }
  return ret;
}

} // namespace palf
} // namespace oceanbase
