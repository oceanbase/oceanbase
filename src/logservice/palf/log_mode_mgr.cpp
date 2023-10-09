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
#include "log_mode_mgr.h"
#include "lib/ob_errno.h"
#include "log_state_mgr.h"
#include "log_engine.h"
#include "log_config_mgr.h"
#include "log_io_task_cb_utils.h"
#include "log_sliding_window.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{

LogModeMgr::LogModeMgr()
    : is_inited_(false),
      palf_id_(-1),
      self_(),
      applied_mode_meta_(),
      accepted_mode_meta_(),
      last_submit_mode_meta_(),
      state_(ModeChangeState::MODE_INIT),
      leader_epoch_(-1),
      new_proposal_id_(INVALID_PROPOSAL_ID),
      ack_list_(),
      follower_list_(),
      majority_cnt_(-1),
      last_submit_req_ts_(OB_INVALID_TIMESTAMP),
      max_majority_accepted_mode_meta_(),
      local_max_lsn_(),
      local_max_log_pid_(INVALID_PROPOSAL_ID),
      max_majority_accepted_pid_(INVALID_PROPOSAL_ID),
      max_majority_lsn_(),
      resend_mode_meta_list_(),
      state_mgr_(NULL),
      log_engine_(NULL),
      config_mgr_(NULL),
      sw_(NULL)
    { }

int LogModeMgr::init(const int64_t palf_id,
                     const common::ObAddr &self,
                     const LogModeMeta &log_mode_meta,
                     LogStateMgr *state_mgr,
                     LogEngine *log_engine,
                     LogConfigMgr *config_mgr,
                     LogSlidingWindow *sw)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "LogModeMgr has inited", K(ret), K(palf_id));
  } else if (false == is_valid_palf_id(palf_id) ||
             false == self.is_valid() ||
             false == log_mode_meta.is_valid() ||
             OB_ISNULL(state_mgr) ||
             OB_ISNULL(log_engine) ||
             OB_ISNULL(config_mgr) ||
             OB_ISNULL(sw)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(palf_id), K(self), K(log_mode_meta),
        KP(state_mgr), KP(log_engine), KP(config_mgr), KP(sw));
  } else {
    palf_id_ = palf_id;
    self_ = self;
    // NB: log_mode_meta_ takes effect immediately on initing;
    //     changing log_mode_meta_ takes effect on committed.
    applied_mode_meta_ = log_mode_meta;
    accepted_mode_meta_ = log_mode_meta;
    last_submit_mode_meta_ = log_mode_meta;
    state_mgr_ = state_mgr;
    log_engine_ = log_engine;
    config_mgr_ = config_mgr;
    sw_ = sw;
    is_inited_ = true;
  }
  return ret;
}

void LogModeMgr::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    palf_id_ = -1;
    self_.reset();
    applied_mode_meta_.reset();
    accepted_mode_meta_.reset();
    last_submit_mode_meta_.reset();
    reset_status_();
    resend_mode_meta_list_.reset();
    state_mgr_ = NULL;
    log_engine_ = NULL;
    config_mgr_ = NULL;
    sw_ = NULL;
  }
}

int LogModeMgr::get_access_mode(AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogModeMgr has inited", K(ret));
  } else {
    access_mode = (AccessMode)ATOMIC_LOAD(reinterpret_cast<const int64_t *>(&applied_mode_meta_.access_mode_));
  }
  return ret;
}

int LogModeMgr::get_mode_version(int64_t &mode_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogModeMgr has inited", K(ret));
  } else {
    mode_version = ATOMIC_LOAD(reinterpret_cast<const int64_t *>(&applied_mode_meta_.mode_version_));
  }
  return ret;
}

int LogModeMgr::get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogModeMgr has inited", K(ret));
  } else {
    mode_version = applied_mode_meta_.mode_version_;
    access_mode = applied_mode_meta_.access_mode_;
  }
  return ret;
}

int LogModeMgr::get_access_mode_ref_scn(int64_t &mode_version,
                                        AccessMode &access_mode,
                                        SCN &ref_scn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogModeMgr has inited", K(ret));
  } else {
    mode_version = applied_mode_meta_.mode_version_;
    access_mode = applied_mode_meta_.access_mode_;
    ref_scn = applied_mode_meta_.ref_scn_;
  }
  return ret;
}

// require rlock of PalfHandleImpl
LogModeMeta LogModeMgr::get_accepted_mode_meta() const
{
  return accepted_mode_meta_;
}

// require rlock of PalfHandleImpl
LogModeMeta LogModeMgr::get_last_submit_mode_meta() const
{
  return last_submit_mode_meta_;
}

// proected by lock_ in PalfHandleImpl
bool LogModeMgr::can_append() const
{
  return applied_mode_meta_.access_mode_ == AccessMode::APPEND && can_do_paxos_accept();
}

// proected by lock_ in PalfHandleImpl
bool LogModeMgr::can_raw_write() const
{
  return applied_mode_meta_.access_mode_ == AccessMode::RAW_WRITE && can_do_paxos_accept();
}

bool LogModeMgr::can_receive_log() const
{
  return applied_mode_meta_.access_mode_ != AccessMode::FLASHBACK;
}

// need lock-free
bool LogModeMgr::is_in_pending_state() const
{
  return MODE_INIT != ATOMIC_LOAD(&state_);
}

// proected by lock_ in PalfHandleImpl
bool LogModeMgr::can_do_paxos_accept() const
{
  return MODE_PREPARE != state_;
}

// proected by lock_ in PalfHandleImpl
bool LogModeMgr::need_skip_log_barrier() const
{
  return applied_mode_meta_.access_mode_ == AccessMode::FLASHBACK && can_do_paxos_accept();
}

int LogModeMgr::can_change_access_mode_(const int64_t mode_version) const
{
  int ret = OB_SUCCESS;
  const bool is_leader_active = state_mgr_->is_leader_active();
  const bool is_epoch_changed = !state_mgr_->check_epoch_is_same_with_election(leader_epoch_);
  const bool see_newer_mode_meta = (max_majority_accepted_mode_meta_.is_valid() &&
    max_majority_accepted_mode_meta_.proposal_id_ > accepted_mode_meta_.proposal_id_);
  const bool see_newer_log =  \
      (max_majority_accepted_pid_ != INVALID_PROPOSAL_ID && max_majority_accepted_pid_ > local_max_log_pid_) ||
      (max_majority_accepted_pid_ == local_max_log_pid_ && max_majority_lsn_ > local_max_lsn_);
  // leader_epoch_ hasn't been updated when state_ is equal to MODE_INIT, so don't check epoch when init
  if ((false == is_leader_active) || (state_ != ModeChangeState::MODE_INIT && is_epoch_changed)) {
    ret = OB_NOT_MASTER;
    PALF_LOG(WARN, "self is not master, can not change_mode", K(ret), K_(palf_id), K_(self),
        K(is_leader_active), K_(state), K(is_epoch_changed));
  } else if (state_ != ModeChangeState::MODE_INIT && (see_newer_mode_meta || see_newer_log)) {
    ret = OB_NOT_MASTER;
    PALF_LOG(WARN, "see newer mode_meta/log, cann't change_acces_mode", K(ret), K_(palf_id), K_(self),
        K(see_newer_mode_meta), K_(max_majority_accepted_mode_meta), K_(accepted_mode_meta),
        K(see_newer_log), K_(max_majority_accepted_pid), K_(local_max_log_pid),
        K_(max_majority_lsn), K_(local_max_lsn));
  } else if (applied_mode_meta_.mode_version_ != mode_version) {
    // mode_version is the proposal_id of PALF when access_mode was applied
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "state don't matches, can not change_mode", K(ret), K_(palf_id), K_(self),
        K(mode_version), "curr_proposal_id", state_mgr_->get_proposal_id(), K_(applied_mode_meta));
  }
  return ret;
}

int LogModeMgr::init_change_mode_()
{
  int ret = OB_SUCCESS;
  int64_t replica_num = 0;
  if (OB_FAIL(config_mgr_->get_alive_member_list_with_arb(follower_list_, replica_num))) {
    PALF_LOG(WARN, "get_alive_member_list_with_arb failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(follower_list_.remove_server(self_))) {
    PALF_LOG(WARN, "remove_server failed", K(ret), K_(palf_id), K_(self), K_(follower_list));
  } else {
    majority_cnt_ = replica_num / 2 + 1;
    leader_epoch_ = state_mgr_->get_leader_epoch();
    local_max_lsn_ = sw_->get_max_lsn();
    local_max_log_pid_ = state_mgr_->get_proposal_id();
    max_majority_accepted_pid_ = local_max_log_pid_;
    max_majority_lsn_ = local_max_lsn_;
  }
  return ret;
}

// State machine for mode changing
//  INIT ◄────┐
//   │        │
//   ▼        │
// PREPARE    │
//   │        │
//   ▼        │
// ACCEPT─────┘
// require caller hold rlock of PalfHandleImpl
bool LogModeMgr::is_state_changed() const
{
  bool bool_ret = false;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    bool_ret = true;
  } else {
    switch (state_) {
      case (ModeChangeState::MODE_INIT):
      {
        bool_ret = true;
        break;
      }
      case (ModeChangeState::MODE_PREPARE):
      case (ModeChangeState::MODE_ACCEPT):
      {
        const bool is_reach_majority = is_reach_majority_();
        const bool is_need_retry = is_need_retry_();
        const bool is_epoch_changed = !state_mgr_->check_epoch_is_same_with_election(leader_epoch_);
        bool_ret = (is_reach_majority || is_need_retry || is_epoch_changed);
        if (true == bool_ret) {
          PALF_LOG(INFO, "is_state_changed", K_(palf_id), K_(self), "state", state2str_(state_),
              K(is_reach_majority), K(is_need_retry), K(is_epoch_changed), K_(follower_list),
              K_(majority_cnt), K_(ack_list), K_(last_submit_req_ts));
        }
        break;
      }
      default:
      {
        PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "Invalid ModeChangeState", K_(palf_id), K_(self), K_(state));
        break;
      }
    }
  }
  return bool_ret;
}

bool LogModeMgr::is_reach_majority_() const
{
  return ack_list_.get_count() >= majority_cnt_;
}

bool LogModeMgr::can_finish_change_mode_() const
{
  bool bool_ret = false;
  LSN last_slide_lsn, committed_end_lsn;
  int64_t unused_id = 0;
  const LSN max_lsn = sw_->get_max_lsn();
  bool_ret = sw_->is_all_committed_log_slided_out(last_slide_lsn, unused_id, committed_end_lsn) &&
      (committed_end_lsn >= max_lsn);
  if (false == bool_ret && palf_reach_time_interval(500 * 1000, wait_committed_log_slide_warn_ts_)) {
    PALF_LOG(INFO, "wait is_all_committed_log_slided_out", K(bool_ret), K(last_slide_lsn),
        K(committed_end_lsn), K(max_lsn));
  }
  return bool_ret;
}

bool LogModeMgr::is_need_retry_() const
{
  return (last_submit_req_ts_ == OB_INVALID_TIMESTAMP ||
      common::ObTimeUtility::current_time() - last_submit_req_ts_ > PREPARE_RETRY_INTERVAL_US);
}

void LogModeMgr::reset_status()
{
  common::ObSpinLockGuard guard(lock_);
  reset_status_();
  resend_mode_meta_list_.reset();
}

void LogModeMgr::reset_status_()
{
  ATOMIC_STORE(&state_, ModeChangeState::MODE_INIT);
  leader_epoch_ = -1;
  new_proposal_id_ = INVALID_PROPOSAL_ID;
  ack_list_.reset();
  follower_list_.reset();
  majority_cnt_ = INT64_MAX;
  last_submit_req_ts_ = OB_INVALID_TIMESTAMP;
  max_majority_accepted_mode_meta_.reset();
  local_max_lsn_.reset();
  local_max_log_pid_ = INVALID_PROPOSAL_ID;
  max_majority_accepted_pid_ = INVALID_PROPOSAL_ID;
  max_majority_lsn_.reset();
}

int LogModeMgr::change_access_mode(
    const int64_t mode_version,
    const AccessMode &access_mode,
    const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == is_valid_access_mode(access_mode) ||
             !ref_scn.is_valid() ||
             INVALID_PROPOSAL_ID == mode_version ||
             mode_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K_(self), K(access_mode),
        K(ref_scn), K(mode_version));
  } else if (OB_FAIL(can_change_access_mode_(mode_version))) {
    PALF_LOG(WARN, "can_change_access_mode failed", K(ret), K_(palf_id), K_(self));
  } else if (applied_mode_meta_.access_mode_ == access_mode) {
    ret = OB_SUCCESS;
    PALF_LOG(INFO, "don't need change access_mode to self", K_(palf_id), K_(self),
        K(access_mode), K_(applied_mode_meta));
  } else if (false == can_switch_access_mode_(applied_mode_meta_.access_mode_, access_mode)) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "can not switch access_mode", K_(palf_id), K_(self),
        K(access_mode), K_(applied_mode_meta));
  } else {
    const bool is_reconfirm = false;
    ret = switch_state_(access_mode, ref_scn, is_reconfirm);
  }
  return ret;
}

int LogModeMgr::reconfirm_mode_meta()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == state_mgr_->is_leader_reconfirm()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "don't call reconfirm_mode_meta when self_ isn't reconfirming leader",
        K(ret), K_(palf_id), K_(self));
  } else {
    const bool is_reconfirm = true;
    SCN invalid_scn;
    ret = switch_state_(AccessMode::INVALID_ACCESS_MODE, invalid_scn, is_reconfirm);
  }
  return ret;
}

int LogModeMgr::switch_state_(const AccessMode &access_mode,
                              const SCN &ref_scn,
                              const bool is_reconfirm)
{
  int ret = OB_SUCCESS;
  bool change_done = false;
  switch (state_) {
    case (ModeChangeState::MODE_INIT):
    {
      const bool need_inc_pid = true;
      const bool need_send_and_handle_prepare = (!is_reconfirm);
      // if access_mode is already changed, just return OB_SUCCESS;
      if (OB_FAIL(init_change_mode_())) {
        PALF_LOG(WARN, "init_change_mode_ failed", K(ret), K_(palf_id), K_(self));
      } else if (OB_FAIL(submit_prepare_req_(need_inc_pid, need_send_and_handle_prepare))) {
        PALF_LOG(WARN, "submit_prepare_req_ failed", K(ret), K_(palf_id), K_(self), K(is_reconfirm));
      } else {
        ATOMIC_STORE(&state_, ModeChangeState::MODE_PREPARE);
      }
      if (state_ != ModeChangeState::MODE_PREPARE) {
        break;
      }
    }
    case (ModeChangeState::MODE_PREPARE):
    {
      if (is_reach_majority_()) {
        ack_list_.reset();
        last_submit_req_ts_ = OB_INVALID_TIMESTAMP;
        ATOMIC_STORE(&state_, ModeChangeState::MODE_ACCEPT);
      } else if (is_reconfirm || is_need_retry_()) {
        // change_access_mode inc update proposal_id only once
        const bool need_inc_pid = is_reconfirm;
        const bool need_send_and_handle_prepare = (!is_reconfirm);
        if (OB_FAIL(submit_prepare_req_(need_inc_pid, need_send_and_handle_prepare))) {
          PALF_LOG(WARN, "submit_prepare_req_ failed", K(ret), K_(palf_id), K_(self));
        }
      }
      if (state_ != ModeChangeState::MODE_ACCEPT) {
        break;
      }
    }
    case (ModeChangeState::MODE_ACCEPT):
    {
      if (is_reach_majority_()) {
        //TODO(yunlong):check conflict

        // not reaches majority until LogModeMeta is been flushed by leader,
        // otherwise next change_access_mode/reconfirm may learn wrong ModeMeta
        if (accepted_mode_meta_.proposal_id_ == new_proposal_id_) {
          // Scenario: There are 3 replicas A, B and C, they are all flashbacked, in this time,
          // user change_access_mode to APPEND.
          // Problem: if A restarts after being flashbacked, A's committed_end_lsn will be smaller
          // than max_flushed_end_lsn even if log in max_flushed_end_lsn has been committed (The
          // committed_end_lsn recorded in last LogEntryHeader is smaller than it's end_lsn).
          // Then A was elected to be leader and change access mode to APPEND, if we don't advance
          // committed_end_lsn of A, some logs which should be readable becomes unreadable in APPEND MODE.
          // Reason: so we advance committed_end_lsn after switching to APPEND to make last logs readable.
          if (false == is_reconfirm &&
              AccessMode::FLASHBACK == applied_mode_meta_.access_mode_ &&
              AccessMode::APPEND == accepted_mode_meta_.access_mode_) {
            LSN max_flushed_end_lsn;
            (void) sw_->get_max_flushed_end_lsn(max_flushed_end_lsn);
            (void) sw_->try_advance_committed_end_lsn(max_flushed_end_lsn);
          }
          // wait all committed log slide out
          change_done = (true == is_reconfirm)? true: can_finish_change_mode_();
          if (change_done) {
            applied_mode_meta_ = accepted_mode_meta_;
            const bool need_inc_ref_scn = applied_mode_meta_.ref_scn_.is_valid() &&
                AccessMode::APPEND == applied_mode_meta_.access_mode_;
            if (need_inc_ref_scn && OB_FAIL(sw_->inc_update_scn_base(applied_mode_meta_.ref_scn_))) {
              PALF_LOG(ERROR, "inc_update_base_log_ts failed", KR(ret), K_(palf_id), K_(self),
                  K_(applied_mode_meta));
            } else if (need_inc_ref_scn && sw_->get_max_scn() < ref_scn) {
              change_done = false;
              ret = OB_ERR_UNEXPECTED;
              PALF_LOG(ERROR, "inc_update_base_log_ts failed", KR(ret), K_(palf_id), K_(self),
                  K_(applied_mode_meta), KPC(sw_));
            } else if (OB_FAIL(set_resend_mode_meta_list_())) {
              PALF_LOG(WARN, "set_resend_mode_meta_list_ failed", K(ret), K_(palf_id), K_(self));
            } else if (OB_FAIL(resend_applied_mode_meta_())) {
              PALF_LOG(WARN, "resend_applied_mode_meta_ failed", K(ret), K_(palf_id), K_(self));
            } else { }
          }
        } else {
          PALF_LOG(INFO, "mode_meta hasn't been flushed in leader", K(ret), K_(palf_id), K_(self),
              K_(last_submit_mode_meta), K_(accepted_mode_meta), K_(new_proposal_id));
        }
      } else if (is_need_retry_()) {
        const int64_t mode_version = new_proposal_id_;
        LogModeMeta mode_meta = max_majority_accepted_mode_meta_;
        mode_meta.proposal_id_ = new_proposal_id_;
        const bool is_applied_mode_meta = false;
        if (false == is_reconfirm && OB_FAIL(mode_meta.generate(new_proposal_id_, mode_version, access_mode, ref_scn))) {
          PALF_LOG(WARN, "generate mode_meta failed", K(ret), K_(palf_id), K_(self),
              K(access_mode), K(ref_scn), K_(new_proposal_id));
        } else if (OB_FAIL(submit_accept_req_(new_proposal_id_, is_applied_mode_meta, mode_meta))) {
          PALF_LOG(WARN, "submit_accept_req_ failed", K(ret), K_(palf_id), K_(self),
              K(mode_meta), K_(new_proposal_id));
        }
      }
      break;
    }
    default:
    {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "Invalid ModeChangeState", K_(palf_id), K_(self), K_(state));
      break;
    }
  }
  if (true == change_done) {
    ret = OB_SUCCESS;
    reset_status_();
  } else if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
    PALF_LOG(ERROR, "switch_state failed", K(ret), K_(palf_id), "state", state2str_(state_), K_(follower_list),
        K_(majority_cnt), K_(ack_list));
  } else {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000)) {
      PALF_LOG(INFO, "change_access_mode waiting retry", K(ret), K_(palf_id), "state", state2str_(state_),
          K_(follower_list), K_(majority_cnt), K_(ack_list));
    }
  }
  return ret;
}

int LogModeMgr::set_resend_mode_meta_list_()
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  common::GlobalLearnerList learner_list;
  int64_t replica_num;
  resend_mode_meta_list_.reset();
  if (OB_FAIL(config_mgr_->get_alive_member_list_with_arb(member_list, replica_num))) {
    PALF_LOG(WARN, "get_alive_member_list_with_arb failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(config_mgr_->get_global_learner_list(learner_list))) {
    PALF_LOG(WARN, "get_global_learner_list failed", K(ret), K_(palf_id), K_(self));
  } else {
    member_list.remove_server(self_);
    (void) learner_list.deep_copy_to(resend_mode_meta_list_);
    const int64_t member_number = member_list.get_member_number();
    for (int64_t idx = 0; idx < member_number && OB_SUCC(ret); ++idx) {
      common::ObAddr server;
      if (OB_FAIL(member_list.get_server_by_index(idx, server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(ret), K(idx));
      } else if (OB_FAIL(resend_mode_meta_list_.add_learner(ObMember(server, 1)))) {
        PALF_LOG(WARN, "add_learner failed", K(ret), K(server));
      }
    }
  }
  return ret;
}

int LogModeMgr::leader_do_loop_work()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  const bool is_leader = (self_ == state_mgr_->get_leader());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (true == is_leader &&
      ModeChangeState::MODE_INIT == state_ &&
      0 != resend_mode_meta_list_.get_member_number() &&
      is_need_retry_()) {
    ret = resend_applied_mode_meta_();
  }
  return ret;
}

int LogModeMgr::resend_applied_mode_meta_()
{
  int ret = OB_SUCCESS;
  const int64_t proposal_id = state_mgr_->get_proposal_id();
  const bool is_applied_mode_meta = true;
  if (resend_mode_meta_list_.is_valid() &&
      OB_FAIL(log_engine_->submit_change_mode_meta_req(resend_mode_meta_list_, proposal_id,
        is_applied_mode_meta, applied_mode_meta_))) {
    PALF_LOG(WARN, "submit_prepare_meta_req failed", K(ret), K_(palf_id), K_(self),
        K_(resend_mode_meta_list), K(proposal_id), K(is_applied_mode_meta), K_(applied_mode_meta));
  } else {
    last_submit_req_ts_ = common::ObTimeUtility::current_time();
  }
  return ret;
}

// require wlock in PalfHandleImpl
int LogModeMgr::submit_prepare_req_(const bool need_inc_pid, const bool need_send_and_handle_prepare)
{
  int ret = OB_SUCCESS;
  // need reset ack_list when resend prepare msg with bigger proposal_id
  ack_list_.reset();
  if (need_inc_pid) {
    new_proposal_id_ = state_mgr_->get_proposal_id() + 1;
  }
  if (need_send_and_handle_prepare && OB_FAIL(state_mgr_->handle_prepare_request(self_, new_proposal_id_))) {
    PALF_LOG(WARN, "handle_prepare_request failed", K(ret), K_(palf_id), K_(self), K_(new_proposal_id));
  } else if (need_send_and_handle_prepare && follower_list_.is_valid() &&
      OB_FAIL(log_engine_->submit_prepare_meta_req(follower_list_, new_proposal_id_))) {
    PALF_LOG(WARN, "submit_prepare_meta_req failed", K(ret), K_(palf_id), K_(self),
        K_(follower_list), K_(new_proposal_id));
  } else if (OB_FAIL(ack_list_.add_server(self_))) {
    PALF_LOG(WARN, "add_server failed", K(ret), K_(palf_id), K_(self));
  } else {
    last_submit_req_ts_ = common::ObTimeUtility::current_time();
    max_majority_accepted_mode_meta_ = accepted_mode_meta_;
    PALF_LOG(INFO, "submit_prepare_meta_req success", K(ret), K_(palf_id), K_(self),
        K_(follower_list), K_(ack_list), K_(new_proposal_id));
  }
  return ret;
}

int LogModeMgr::submit_accept_req_(const int64_t proposal_id,
                                   const bool is_applied_mode_meta,
                                   const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  const bool has_accepted = (accepted_mode_meta_.proposal_id_ == mode_meta.proposal_id_);
  if (false == has_accepted && OB_FAIL(receive_mode_meta_(self_, proposal_id, is_applied_mode_meta, mode_meta))) {
    PALF_LOG(WARN, "receive_mode_meta failed", K(ret), K_(palf_id), K_(self), K(mode_meta));
  } else if (follower_list_.is_valid() &&
      OB_FAIL(log_engine_->submit_change_mode_meta_req(follower_list_, proposal_id,
          is_applied_mode_meta, mode_meta))) {
    PALF_LOG(WARN, "submit_prepare_meta_req failed", K(ret), K_(palf_id), K_(self),
        K_(follower_list), K(proposal_id), K(is_applied_mode_meta), K(mode_meta));
  } else {
    PALF_LOG(INFO, "submit_change_mode_meta_req success", K(ret), K_(palf_id), K_(self),
        K_(follower_list), K_(ack_list), K(proposal_id), K(is_applied_mode_meta), K(mode_meta));
    last_submit_req_ts_ = common::ObTimeUtility::current_time();
  }
  return ret;
}

int LogModeMgr::handle_prepare_response(const common::ObAddr &server,
                                        const int64_t msg_proposal_id,
                                        const int64_t accept_log_proposal_id,
                                        const LSN &last_lsn,
                                        const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == server.is_valid() ||
      INVALID_PROPOSAL_ID == msg_proposal_id ||
      INVALID_PROPOSAL_ID == accept_log_proposal_id ||
      false == last_lsn.is_valid() ||
      false == mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K_(self), K(server), K(msg_proposal_id),
        K(accept_log_proposal_id), K(last_lsn), K(mode_meta));
  } else if (ModeChangeState::MODE_PREPARE != state_ || new_proposal_id_ != msg_proposal_id) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "handle_prepare_response not in PREPARE state", K(ret), K_(palf_id), K_(self),
        K_(new_proposal_id), K(server), K(msg_proposal_id), K(mode_meta), "state", state2str_(state_));
  } else if (false == follower_list_.contains(server)) {
    PALF_LOG(WARN, "receive prepare req not from follower_list", K(ret), K_(palf_id), K(server),
        K_(follower_list));
  } else if (OB_FAIL(ack_list_.add_server(server))) {
    PALF_LOG(WARN, "add_server failed", K(ret), K_(palf_id), K_(self), K(server));
  } else {
    if (false == max_majority_accepted_mode_meta_.is_valid() ||
        max_majority_accepted_mode_meta_.proposal_id_ < mode_meta.proposal_id_) {
      max_majority_accepted_mode_meta_ = mode_meta;
    }
    max_majority_accepted_pid_ = max_proposal_id(max_majority_accepted_pid_, accept_log_proposal_id);
    max_majority_lsn_ = MAX(max_majority_lsn_, last_lsn);
    PALF_LOG(INFO, "handle_prepare_response finish", K(ret), K_(palf_id), K_(self), K(server), K(msg_proposal_id),
        K(accept_log_proposal_id), K(last_lsn), K(mode_meta), K_(ack_list), K_(follower_list), K_(majority_cnt),
        K_(max_majority_accepted_mode_meta), K_(max_majority_accepted_pid), K_(max_majority_lsn));
  }
  return ret;
}

// require rlock of PalfHandleImpl
bool LogModeMgr::can_receive_mode_meta(const int64_t proposal_id,
                                       const LogModeMeta &mode_meta,
                                       bool &has_accepted)
{
  bool bool_ret = false;
  has_accepted = false;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT ||
      proposal_id == INVALID_PROPOSAL_ID ||
      false == mode_meta.is_valid()) {
  } else if (false == state_mgr_->can_receive_config_log(proposal_id)) {
    // for arbitration replica, is_sync_enabled is false, so check can_receive_mode_meta
    // with LogStateMgr::can_receive_config_log
    PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "can_receive_mode_meta failed", K_(palf_id), K_(self),
        K(proposal_id), K(mode_meta));
  } else if (accepted_mode_meta_.proposal_id_ > mode_meta.proposal_id_) {
    // skip, do not receive mode_meta with smaller proposal_id
  } else if (accepted_mode_meta_.proposal_id_ == mode_meta.proposal_id_) {
    bool_ret = true;
    has_accepted = true;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

// require wlock of PalfHandleImpl
int LogModeMgr::receive_mode_meta(const common::ObAddr &server,
                                  const int64_t proposal_id,
                                  const bool is_applied_mode_meta,
                                  const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = receive_mode_meta_(server, proposal_id, is_applied_mode_meta, mode_meta);
  }
  return ret;
}

int LogModeMgr::receive_mode_meta_(const common::ObAddr &server,
                                   const int64_t proposal_id,
                                   const bool is_applied_mode_meta,
                                   const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  if (proposal_id == INVALID_PROPOSAL_ID ||
      false == mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K_(self), K(server), K(proposal_id), K(mode_meta));
  } else {
    FlushMetaCbCtx flush_ctx;
    flush_ctx.type_ = MetaType::MODE_META;
    flush_ctx.proposal_id_ = proposal_id;
    flush_ctx.log_mode_meta_ = mode_meta;
    flush_ctx.is_applied_mode_meta_ = is_applied_mode_meta;
    // why need to record LogModeMeta in FlushCtx?
    // we need to record 'accepted_mode_meta_' as max flushed LogModeMeta, if a follower
    // receives two LogModeMeta A and B successively.
    // 1. record last_submit_mode_meta_ to A
    // 2. A has been flushed and 'after_flush_mode_meta' is going to be called.
    // 3. record last_submit_mode_meta_ to B
    // 4. if don't record LogModeMeta in FlushCtx, A will call 'after_flush_mode_meta'
    // and records last 'submit_mode_meta_' B to 'accepted_mode_meta_'. But B hasn't been flushed
    if (OB_FAIL(log_engine_->submit_flush_mode_meta_task(flush_ctx, mode_meta))) {
      PALF_LOG(WARN, "submit_flush_mode_meta_task failed", K(ret), K_(palf_id), K_(self), K(mode_meta));
    } else {
      last_submit_mode_meta_ = mode_meta;
    }
  }
  return ret;
}

// require wlock of PalfHandleImpl
int LogModeMgr::after_flush_mode_meta(const bool is_applied_mode_meta, const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    accepted_mode_meta_ = (mode_meta.proposal_id_ > accepted_mode_meta_.proposal_id_)?
        mode_meta: accepted_mode_meta_;
    if (is_applied_mode_meta) {
      applied_mode_meta_ = (mode_meta.proposal_id_ > applied_mode_meta_.proposal_id_)?
          mode_meta: applied_mode_meta_;
    }
    PALF_LOG(INFO, "after_flush_mode_meta success", K(ret), K_(palf_id), K_(self),
        K(is_applied_mode_meta), K(mode_meta), K_(accepted_mode_meta), K_(applied_mode_meta));
  }
  return ret;
}

int LogModeMgr::ack_mode_meta(const common::ObAddr &server, const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K_(self), K(server), K(proposal_id));
  } else if (ModeChangeState::MODE_ACCEPT != state_) {
    PALF_LOG(TRACE, "ack_mode_meta not int ACCEPT state", K(ret), K_(palf_id),
        K_(self), K(server), "state", state2str_(state_));
  } else if (new_proposal_id_ != proposal_id) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "ack_mode_meta failed", K(ret), K_(palf_id), K_(self), K(server),
        K(proposal_id), K_(new_proposal_id), "state", state2str_(state_));
  } else if (OB_FAIL(ack_list_.add_server(server))) {
    PALF_LOG(WARN, "add_server failed", K(ret), K_(palf_id), K_(self), K(server));
  } else {
    PALF_LOG(INFO, "ack_mode_meta finish", K(ret), K_(palf_id), K_(self), K(server),
        K(proposal_id), K_(follower_list), K_(majority_cnt), K_(ack_list), K_(resend_mode_meta_list));
  }
  (void) resend_mode_meta_list_.remove_learner(server);
  return ret;
}

// require rlock in PalfHandleImpl
int LogModeMgr::submit_fetch_mode_meta_resp(const common::ObAddr &server,
                                            const int64_t msg_proposal_id,
                                            const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  common::ObMemberList member_list;
  (void) member_list.add_server(server);
  LogModeMeta mode_meta;
  bool is_applied_mode_meta = false;
  if (applied_mode_meta_.proposal_id_ >= accepted_mode_meta_.proposal_id_) {
    is_applied_mode_meta = true;
    mode_meta = applied_mode_meta_;
  } else {
    is_applied_mode_meta = false;
    mode_meta = accepted_mode_meta_;
  }
  if ((accepted_mode_pid == INVALID_PROPOSAL_ID || mode_meta.proposal_id_ >= accepted_mode_pid) &&
      OB_FAIL(log_engine_->submit_change_mode_meta_req(member_list, msg_proposal_id, is_applied_mode_meta, mode_meta))) {
    PALF_LOG(WARN, "submit_change_mode_meta_req failed", K(ret), K_(palf_id), K(server),
        K(server), K(msg_proposal_id), K(mode_meta));
  } else {
    PALF_LOG(INFO, "submit_change_mode_meta_req success", K(ret), K_(palf_id), K(server), K(mode_meta),
        K(accepted_mode_pid));
  }
  return ret;
}

} // namespace palf
} // namespace oceanbase
