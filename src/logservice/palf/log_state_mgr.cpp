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
#include "log_state_mgr.h"
#include "common/ob_clock_generator.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "palf_callback_wrapper.h"
#include "log_io_task_cb_utils.h"
#include "log_engine.h"
#include "log_meta_info.h"
#include "log_config_mgr.h"
#include "log_reconfirm.h"
#include "log_sliding_window.h"
#include "log_mode_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace palf
{
LogStateMgr::LogStateMgr()
  : self_(),
    palf_id_(),
    prepare_meta_(),
    sw_(NULL),
    reconfirm_(NULL),
    log_engine_(NULL),
    mm_(NULL),
    mode_mgr_(NULL),
    palf_role_change_cb_(NULL),
    election_(NULL),
    plugins_(NULL),
    role_state_val_(0),
    leader_(),
    leader_epoch_(OB_INVALID_TIMESTAMP),
    last_check_start_id_(OB_INVALID_LOG_ID),
    last_check_start_id_time_us_(OB_INVALID_TIMESTAMP),
    reconfirm_start_time_us_(OB_INVALID_TIMESTAMP),
    check_sync_enabled_time_(OB_INVALID_TIMESTAMP),
    check_reconfirm_timeout_time_(OB_INVALID_TIMESTAMP),
    check_follower_pending_warn_time_(OB_INVALID_TIMESTAMP),
    log_sync_timeout_warn_time_(OB_INVALID_TIMESTAMP),
    update_leader_warn_time_(OB_INVALID_TIMESTAMP),
    pending_end_lsn_(),
    scan_disk_log_finished_(false),
    is_sync_enabled_(true),
    allow_vote_(true),
    allow_vote_persisted_(true),
    replica_type_(NORMAL_REPLICA),
    is_changing_config_with_arb_(false),
    last_set_changing_config_with_arb_time_us_(OB_INVALID_TIMESTAMP),
    broadcast_leader_(),
    last_recv_leader_broadcast_time_us_(OB_INVALID_TIMESTAMP),
    is_inited_(false)
{}

int LogStateMgr::init(const int64_t palf_id,
                      const common::ObAddr &self,
                      const LogPrepareMeta &log_prepare_meta,
                      const LogReplicaPropertyMeta &replica_property_meta,
                      election::Election* election,
                      LogSlidingWindow *sw,
                      LogReconfirm *reconfirm,
                      LogEngine *log_engine,
                      LogConfigMgr *mm,
                      LogModeMgr *mode_mgr,
                      palf::PalfRoleChangeCbWrapper *palf_role_change_cb,
                      LogPlugins *plugins)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "init twice", K(ret), K_(palf_id));
  } else if (false == is_valid_palf_id(palf_id) || NULL == sw || NULL == reconfirm || NULL == log_engine
      || NULL == election || NULL == mm || NULL == mode_mgr || NULL == palf_role_change_cb ||
      NULL == plugins || !self.is_valid() || !replica_property_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(palf_id), KP(sw), KP(reconfirm), K(log_prepare_meta),
        KP(election), KP(log_engine), KP(mm), KP(mode_mgr), KP(palf_role_change_cb), KP(plugins),
        K(replica_property_meta), K(self));
  } else {
    prepare_meta_= log_prepare_meta;
    palf_id_ = palf_id;
    election_ = election;
    plugins_ = plugins;
    sw_ = sw;
    reconfirm_ = reconfirm;
    log_engine_ = log_engine;
    mm_ = mm;
    mode_mgr_ = mode_mgr;
    palf_role_change_cb_ = palf_role_change_cb;
    self_ = self;
    leader_.reset();
    role_ = FOLLOWER;
    state_ = INIT;
    scan_disk_log_finished_ = false;
    allow_vote_ = replica_property_meta.allow_vote_;
    allow_vote_persisted_ = replica_property_meta.allow_vote_;
    replica_type_ = replica_property_meta.replica_type_;
    is_sync_enabled_ = !is_arb_replica();
    broadcast_leader_.reset();
    is_inited_ = true;
    PALF_LOG(INFO, "LogStateMgr init success", K(ret), K_(palf_id), K_(self));
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

bool LogStateMgr::is_follower_init_() const
{
  return check_role_and_state_(FOLLOWER, INIT);
}

bool LogStateMgr::is_follower_active_() const
{
  return check_role_and_state_(FOLLOWER, ACTIVE);
}

bool LogStateMgr::is_leader_reconfirm_() const
{
  return check_role_and_state_(LEADER, RECONFIRM);
}

bool LogStateMgr::is_leader_active_() const
{
  return check_role_and_state_(LEADER, ACTIVE);
}

bool LogStateMgr::check_role_and_state_(const common::ObRole &role, const ObReplicaState &state) const
{
  bool bool_ret = false;
  const int64_t tmp_val = ATOMIC_LOAD(&role_state_val_);
  if ((tmp_val & UNION_ROLE_VAL_MASK) != role) {  // check role_
  } else if (((tmp_val >> 32) & UNION_ROLE_VAL_MASK) != state) {  // check state_
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

void LogStateMgr::get_role_and_state(common::ObRole &role, ObReplicaState &state) const
{
  const int64_t tmp_val = ATOMIC_LOAD(&role_state_val_);
  role = (ObRole)(tmp_val & UNION_ROLE_VAL_MASK);
  state = (ObReplicaState)((tmp_val >> 32) & UNION_ROLE_VAL_MASK);
}

void LogStateMgr::update_role_and_state_(const common::ObRole &new_role, const ObReplicaState &new_state)
{
  const int64_t old_val = ATOMIC_LOAD(&role_state_val_);
  const int64_t new_val = (((int32_t)new_state & UNION_ROLE_VAL_MASK) << 32) + \
                          ((int32_t)new_role & UNION_ROLE_VAL_MASK);
  if (ATOMIC_BCAS(&role_state_val_, old_val, new_val)) {
    // update success
  } else {
    // 更新role/state时会加palf_handle的写锁，因此预期不应该失败
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "update_role_and_state_ failed", K_(palf_id), K(old_val), K(new_val));
  }
}

bool LogStateMgr::is_follower_pending_() const
{
  return check_role_and_state_(FOLLOWER, PENDING);
}

bool LogStateMgr::can_append(const int64_t proposal_id, const bool need_check_proposal_id) const
{
  bool bool_ret = false;
  if (is_leader_active_()
      && ((need_check_proposal_id && proposal_id == get_proposal_id())
          || false == need_check_proposal_id)
      && OB_LIKELY(mode_mgr_->can_append())) {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogStateMgr::can_raw_write(const int64_t proposal_id, const bool need_check_proposal_id) const
{
  bool bool_ret = false;
  if (is_leader_active_()
      && ((need_check_proposal_id && proposal_id == get_proposal_id())
          || false == need_check_proposal_id)
      && OB_LIKELY(mode_mgr_->can_raw_write())) {
    bool_ret = true;
  }
  return bool_ret;
}


bool LogStateMgr::can_slide_sw() const
{
  return (is_leader_reconfirm_() || is_leader_active_() || is_follower_active_()
          || is_follower_init_() || is_follower_pending_());
}

bool LogStateMgr::can_revoke(const int64_t proposal_id) const
{
  return true == is_leader_active_() && get_proposal_id() == proposal_id;
}

bool LogStateMgr::is_state_changed()
{
  bool state_changed = false;
  bool is_error = false;
  if (is_follower_init_()) {
    state_changed = follower_init_need_switch_();
  } else if (is_follower_active_()) {
    (void) check_and_try_fetch_log_();
    state_changed = follower_active_need_switch_() || need_reset_broadcast_leader_();
  } else if (is_follower_pending_()) {
    (void) check_and_try_fetch_log_();
    state_changed = follower_pending_need_switch_();
    if (false == state_changed) {
      common::ObAddr new_leader;
      int64_t new_leader_epoch = OB_INVALID_TIMESTAMP;
      state_changed = follower_need_update_role_(new_leader, new_leader_epoch);
    }
  } else if (is_leader_reconfirm_()) {
    state_changed = (leader_reconfirm_need_switch_());
  } else if (is_leader_active_()) {
    state_changed = (leader_active_need_switch_(is_error));
  } else {}
  return state_changed;
}

// State machine for normal replica
// ┌──────────────────────────────────────────────────────────┐
// │                                                          │
// │                  follower init                           │
// │                        │                                 │
// │                        │                                 │
// │                        ▼                                 │
// │                  follower active◄───────────────┐        │
// │                        │                        │        │
// │                        │                        │        │
// │                        ▼                        │        │
// │        ┌─────────leader reconfirm <─────┐       │        │
// │        │                │               │       │        │
// │        │                │               │       │        │
// │        │                ▼               │       │        │
// │        │          leader active         │       │        │
// │        │                │               │       │        │
// │        │                │               │       │        │
// │        │                ▼               │       │        │
// │        └─────────►follower pending──────┘───────┘        │
// │                                                          │
// └──────────────────────────────────────────────────────────┘
//
int LogStateMgr::switch_state()
{
  int ret = OB_SUCCESS;

  common::ObAddr new_leader;
  bool need_next_loop = false;
  do {
    const ObAddr old_leader = leader_;
    const common::ObRole old_role = role_;
    const int32_t old_state = state_;
    int64_t old_leader_epoch = leader_epoch_;
    const int64_t start_ts = ObTimeUtility::current_time();

    need_next_loop = false;
    if (is_follower_init_()) {
      if (follower_init_need_switch_()) {
        ret = init_to_follower_active_();
        need_next_loop = true; // next loop will check new leader
      }
    } else if (is_follower_pending_()) {
      new_leader.reset();
      int64_t new_leader_epoch = OB_INVALID_TIMESTAMP;
      if (follower_pending_need_switch_()) {
        ret = pending_to_follower_active_();
        if (follower_active_need_switch_()) {
          need_next_loop = true;
        }
      } else if (follower_need_update_role_(new_leader, new_leader_epoch)) {
        ret = follower_pending_to_reconfirm_(new_leader_epoch);
        need_next_loop = true; // 1) drive reconfirm
      } else {
        // do nothing
      }
    } else if (is_follower_active_()) {
      if (follower_active_need_switch_()) {
        new_leader.reset();
        int64_t new_leader_epoch = OB_INVALID_TIMESTAMP;
        if (follower_need_update_role_(new_leader, new_leader_epoch)) {
          ret = follower_active_to_reconfirm_(new_leader_epoch);
        } else {
          set_leader_and_epoch_(new_leader, new_leader_epoch);
        }
        need_next_loop = true; // 1) drive reconfirm or 2) fetch log from leader
      } else if (need_reset_broadcast_leader_()) {
        reset_broadcast_leader_();
      } else { }
    } else if (is_leader_reconfirm_()) {
      if (is_reconfirm_timeout_()) {
        ret = reconfirm_to_follower_pending_();
      } else if (need_update_leader_(new_leader)) {
        ret = reconfirm_to_follower_pending_();
      } else if (is_reconfirm_state_changed_(ret)) {
        if (OB_SUCC(ret)) {
          ret = reconfirm_to_leader_active_();
        } else {
          ret = reconfirm_to_follower_pending_();
        }
      } else {
        // do nothing
      }
    } else if (is_leader_active_()) {
      bool is_error = false;
      if (leader_active_need_switch_(is_error)) {
        ret = leader_active_to_follower_pending_();
        need_next_loop = true;
      }
    } else {
      // do nothing
    }

    PALF_LOG(INFO, "switch_state", K(ret), K_(palf_id), K(old_role), K_(role),
              K(old_state), K_(state), K(old_leader), K_(leader),
              K(old_leader_epoch), K_(leader_epoch), K(need_next_loop),
              "diff", ObTimeUtility::current_time() - start_ts);
  } while (need_next_loop && OB_SUCC(ret));

  if (OB_EAGAIN == ret) {
    // rewrite ret code to OB_SUCCESS when it's OB_EAGAIN
    ret = OB_SUCCESS;
  }
  return ret;
}

int LogStateMgr::set_scan_disk_log_finished()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    scan_disk_log_finished_ = true;
    PALF_LOG(INFO, "scan disk log finished", K_(palf_id));
  }
  return ret;
}

int64_t LogStateMgr::get_proposal_id() const
{
  return ATOMIC_LOAD(&(prepare_meta_.log_proposal_id_));
}

bool LogStateMgr::can_handle_prepare_response(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id == get_proposal_id()) {
    bool_ret = is_leader_reconfirm_() || is_leader_active_();
  }
  return bool_ret;
}

bool LogStateMgr::can_handle_prepare_request(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id > get_proposal_id()) {
    bool_ret = (is_leader_active_() || is_leader_reconfirm_() || is_follower_active_() \
        || is_follower_pending_());
  }
  return bool_ret;
}

bool LogStateMgr::can_handle_leader_broadcast(const common::ObAddr &server,
                                              const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id == get_proposal_id()) {
    bool_ret = (is_follower_active_());
  }
  return bool_ret;
}

bool LogStateMgr::can_handle_committed_info(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id == get_proposal_id() && true == is_sync_enabled()) {
    bool_ret = (is_follower_active_() || is_follower_pending_());
  }
  return bool_ret;
}

bool LogStateMgr::can_receive_log(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id == get_proposal_id() && true == is_sync_enabled() && mode_mgr_->can_receive_log()) {
    bool_ret = (is_follower_active_() || is_follower_pending_() || is_reconfirm_can_receive_log_());
  }
  return bool_ret;
}

bool LogStateMgr::is_reconfirm_can_receive_log_() const
{
  return (is_leader_reconfirm_() && reconfirm_->can_receive_log());
}

bool LogStateMgr::can_receive_config_log(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id == get_proposal_id()) {
    // can receive config log even if is_sync_enabled is false
    bool_ret = (is_follower_active_() || is_follower_pending_() || is_leader_reconfirm_());
  }
  return bool_ret;
}

int LogStateMgr::handle_prepare_request(const common::ObAddr &server,
                                        const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (INVALID_PROPOSAL_ID == proposal_id || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (proposal_id < get_proposal_id()) {
    (void) reject_prepare_request_(server, get_proposal_id());
  // NB: 'vote_granted' is not used nowday, so we just only accept prepare request
  //     with greater 'proposal_id'.
  } else if (proposal_id == get_proposal_id()) {
    // proposal_id is match with local, no need reply
    PALF_LOG(INFO, "accpet same proposal prepare request, ignore it", K(ret), K_(palf_id),
        K(server), K(proposal_id), K_(prepare_meta));
  } else {
    // proposal_id is greater than local, means another replica has been in the prepare phase,
    // this replica need roll back to follower active.
    if (server != self_ && LEADER == role_) {
      if (is_leader_reconfirm_()) {
        ret = reconfirm_to_follower_pending_();
        PALF_LOG(INFO, "there is another replica has been leader which used greater prorposl id", K(server),
            K(proposal_id), K(self_), "local proposal_id", get_proposal_id());
      } else if (is_leader_active_()) {
        ret = leader_active_to_follower_pending_();
      } else {}
    }

    if (OB_SUCC(ret)) {
      if (server != leader_) {
        // update leader_
        leader_ = server;
        broadcast_leader_ = server;
        last_recv_leader_broadcast_time_us_ = common::ObTimeUtility::current_time();
      }

      if (OB_FAIL(write_prepare_meta_(proposal_id, server))) {
        PALF_LOG(WARN, "write_prepare_meta_ failed", K(ret), K_(palf_id), K(proposal_id), K(server));
      }
    }
    PALF_LOG(INFO, "LogStateMgr handle_prepare_request success", K(ret), K_(palf_id),
        K(self_), K(server), K(proposal_id), K(leader_), K_(broadcast_leader));
  }
  return ret;
}

int LogStateMgr::handle_leader_broadcast(const common::ObAddr &server,
                                         const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  broadcast_leader_ = server;
  last_recv_leader_broadcast_time_us_ = common::ObTimeUtility::current_time();
  return ret;
}

int LogStateMgr::reject_prepare_request_(const common::ObAddr &server,
                                           const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else {
    const bool vote_granted = false;
    int64_t fake_log_proposal_id = INVALID_PROPOSAL_ID;
    LSN fake_lsn;
    LogModeMeta fake_mode_meta;
    if (OB_FAIL(log_engine_->submit_prepare_meta_resp(server, proposal_id,
            vote_granted, fake_log_proposal_id, fake_lsn, fake_lsn, fake_mode_meta))) {
      PALF_LOG(WARN, "submit_prepare_response failed", K(ret), K_(palf_id));
    } else {
      PALF_LOG(INFO, "reject_prepare_request_ success", K(ret), K_(palf_id), K(server), K(proposal_id), K(vote_granted));
    }
  }
  return ret;
}

void LogStateMgr::destroy()
{
  is_inited_ = false;
  is_sync_enabled_ = true;
  sw_ = NULL;
  reconfirm_ = NULL;
  log_engine_ = NULL;
  mm_ = NULL;
  mode_mgr_ = NULL;
  election_ = NULL;
  plugins_ = NULL;
  scan_disk_log_finished_ = false;
  update_role_and_state_(FOLLOWER, INIT);
  leader_.reset();
  leader_epoch_ = OB_INVALID_TIMESTAMP;
  pending_end_lsn_.reset();
  self_.reset();
  is_changing_config_with_arb_ = false;
}

int LogStateMgr::write_prepare_meta_(const int64_t &proposal_id,
                                     const common::ObAddr &new_leader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogStateMgr is not inited", K(ret), K_(palf_id));
  } else if (INVALID_PROPOSAL_ID == proposal_id || !new_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K(proposal_id), K(new_leader));
  } else {
    LogPrepareMeta prepare_meta;
    FlushMetaCbCtx flush_cb_ctx;
    flush_cb_ctx.type_ = PREPARE_META;
    flush_cb_ctx.proposal_id_ = proposal_id;
    if (OB_FAIL(prepare_meta.generate(LogVotedFor(), proposal_id))) {
      PALF_LOG(WARN, "prepare_meta generate failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(log_engine_->submit_flush_prepare_meta_task(flush_cb_ctx, prepare_meta))) {
      PALF_LOG(WARN, "submit_flush_prepare_meta_task failed", K(ret), K_(palf_id));
    } else {
      // update prepare with 'proposal-id'
      prepare_meta_ = prepare_meta;
      PALF_LOG(INFO, "write_prepare_meta_ success", K(prepare_meta), K(new_leader), K(prepare_meta_));
    }
  }
  return ret;
}

int LogStateMgr::init_to_follower_active_()
{
  int ret = OB_SUCCESS;
  reset_status_();
  update_role_and_state_(FOLLOWER, ACTIVE);
  plugins_->record_role_change_event(palf_id_, FOLLOWER, ObReplicaState::INIT,
      FOLLOWER, ObReplicaState::ACTIVE);
  PALF_EVENT("init_to_follower_active", palf_id_, K(ret), K_(self));
  return ret;
}

int LogStateMgr::to_follower_active_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(palf_role_change_cb_->on_role_change(palf_id_))) {
    PALF_LOG(WARN, "on_leader_revoke failed!", K(ret), K_(palf_id));
  } else {}
  PALF_LOG(INFO, "to_follower_active_ finished!", K(ret), K_(palf_id), K_(self));
  return ret;
}

int LogStateMgr::pending_to_follower_active_()
{
  int ret = OB_SUCCESS;
  update_role_and_state_(FOLLOWER, ACTIVE);
  // 需先更新role/state，再提交role_change_event
  // 否则handle role_change event执行可能先执行导致角色切换失败
  if (OB_FAIL(to_follower_active_())) {
    PALF_LOG(ERROR, "to_follower_active_ failed", K(ret), K_(palf_id));
  }
  PALF_REPORT_INFO_KV(K_(leader), K_(pending_end_lsn));
  plugins_->record_role_change_event(palf_id_, FOLLOWER, ObReplicaState::PENDING,
      FOLLOWER, ObReplicaState::ACTIVE, EXTRA_INFOS);
  PALF_EVENT("follower_pending_to_follower_active", palf_id_, K(ret), K_(self), K_(leader),
      K_(pending_end_lsn));
  return ret;
}

int LogStateMgr::to_reconfirm_(const int64_t new_leader_epoch)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(sw_->to_leader_reconfirm())) {
    PALF_LOG(ERROR, "sw to_leader_reconfirm failed", K(ret), K_(palf_id));
  } else {
    reconfirm_start_time_us_ = ObTimeUtility::current_time();
    reset_status_();
    update_role_and_state_(LEADER, RECONFIRM);
    reconfirm_->reset_state();
    leader_ = self_;
    leader_epoch_ = new_leader_epoch;
    // New leader need clear logs after sw' first empty slot(log hole).
    // If not, later local flush waiting will fail.
    (void) sw_->clean_log();
  }

  return ret;
}

int LogStateMgr::follower_active_to_reconfirm_(const int64_t new_leader_epoch)
{
  int ret = to_reconfirm_(new_leader_epoch);
  PALF_REPORT_INFO_KV(K_(leader), K(new_leader_epoch));
  plugins_->record_role_change_event(palf_id_, FOLLOWER, ObReplicaState::ACTIVE,
      LEADER, ObReplicaState::RECONFIRM, EXTRA_INFOS);
  PALF_EVENT("follower_active_to_reconfirm", palf_id_, K(ret), K_(self), K(new_leader_epoch),
     K(leader_), K_(pending_end_lsn));
  return ret;
}

int LogStateMgr::follower_pending_to_reconfirm_(const int64_t new_leader_epoch)
{
  int ret = to_reconfirm_(new_leader_epoch);
  PALF_REPORT_INFO_KV(K_(leader), K(new_leader_epoch), K_(pending_end_lsn));
  plugins_->record_role_change_event(palf_id_, FOLLOWER, ObReplicaState::PENDING,
      LEADER, ObReplicaState::RECONFIRM, EXTRA_INFOS);
  PALF_EVENT("follower_pending_to_reconfirm", palf_id_, K(ret), K_(self), K(new_leader_epoch),
      K(leader_), K_(pending_end_lsn));
  return ret;
}

int LogStateMgr::reconfirm_to_follower_pending_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to_follower_pending_())) {
    PALF_LOG(WARN, "to_follower_pending_ failed, try again", K(ret), K_(palf_id));
  } else {
    reset_status_();
    update_role_and_state_(FOLLOWER, PENDING);
    PALF_REPORT_INFO_KV(K_(leader));
    plugins_->record_role_change_event(palf_id_, LEADER, ObReplicaState::RECONFIRM,
        FOLLOWER, ObReplicaState::PENDING, EXTRA_INFOS);
    PALF_EVENT("reconfirm_to_follower_pending", palf_id_, K_(self), K_(leader), K(lbt()));
  }
  return ret;
}

int LogStateMgr::reconfirm_to_leader_active_()
{
  int ret = OB_SUCCESS;
  const int64_t reconfirm_stage_cost = ObTimeUtility::current_time() - reconfirm_start_time_us_;
  PALF_EVENT("reconfirm_to_leader_active begin", palf_id_, K_(self), K(reconfirm_stage_cost));
  ObMemberList member_list;
  int64_t replica_num = -1;
  LogConfigVersion config_version;
  if (OB_FAIL(mm_->get_config_version(config_version))) {
    PALF_LOG(WARN, "get_config_version failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(mm_->get_alive_member_list_with_arb(member_list, replica_num))) {
    PALF_LOG(WARN, "get_alive_member_list_with_arb failed", K(ret), K_(palf_id));
  } else if (!member_list.contains(self_)) {
    PALF_LOG(ERROR, "curr_member_list doesn't contain self, revoke", K_(palf_id),
             K(member_list), K_(self));
  } else if (OB_FAIL(sw_->to_leader_active())) {
    PALF_LOG(WARN, "sw leader_active failed", K(ret), K_(palf_id));
  } else {
    update_role_and_state_(LEADER, ACTIVE);
    // 需先更新role/state和committed_end_lsn，再提交role_change_event
    // 否则handle role_change event执行可能先执行导致角色切换失败
    if (OB_FAIL(to_leader_active_())) {
      PALF_LOG(ERROR, "to_leader_active_ failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(mm_->submit_broadcast_leader_info(get_proposal_id()))) {
      PALF_LOG(ERROR, "submit_broadcast_leader_info failed", KR(ret));
    }
    const int64_t reconfirm_to_active_cost = ObTimeUtility::current_time() - reconfirm_start_time_us_;
    PALF_EVENT("reconfirm_to_leader_active end", palf_id_, K(ret), K_(self), K(reconfirm_to_active_cost), K_(role), K_(state));
    PALF_REPORT_INFO_KV(K(reconfirm_stage_cost), K(reconfirm_to_active_cost), K(config_version));
    plugins_->record_role_change_event(palf_id_, LEADER, ObReplicaState::RECONFIRM,
        LEADER, ObReplicaState::ACTIVE, EXTRA_INFOS);
  }

  return ret;
}

int LogStateMgr::leader_active_to_follower_pending_()
{
  int ret = OB_SUCCESS;

  if (LEADER == role_) {
    if (OB_FAIL(to_follower_pending_())) {
      PALF_LOG(WARN, "to_follower_pending_ failed, try again", K(ret), K_(palf_id));
    } else {
      reset_status_();
    }
  } else {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "unexpected role", K(ret), K_(palf_id), K_(role));
  }

  if (OB_SUCC(ret)) {
    update_role_and_state_(FOLLOWER, PENDING);
  }

  PALF_REPORT_INFO_KV(K_(pending_end_lsn));
  plugins_->record_role_change_event(palf_id_, LEADER, ObReplicaState::ACTIVE,
      FOLLOWER, ObReplicaState::PENDING, EXTRA_INFOS);
  PALF_EVENT("leader_active_to_follower_pending_", palf_id_, K(ret), K_(self), K_(role), K_(state), K_(pending_end_lsn));
  return ret;
}

void LogStateMgr::reset_status_()
{
  leader_.reset();
  leader_epoch_ = OB_INVALID_TIMESTAMP;
  last_check_start_id_time_us_ = OB_INVALID_TIMESTAMP;
  ATOMIC_STORE(&is_changing_config_with_arb_, false);
  mm_->reset_status();
  mode_mgr_->reset_status();
  PALF_LOG(INFO, "reset_status_", K_(palf_id), K_(self), K_(leader_epoch), K(leader_));
}

int LogStateMgr::to_leader_active_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(palf_role_change_cb_->on_role_change(palf_id_))) {
    PALF_LOG(WARN, "on_leader_takeover failed!", K(ret), K_(palf_id));
  }
  PALF_LOG(INFO, "to_leader_active_ finished!", K(ret), K_(palf_id));
  return ret;
}

int LogStateMgr::truncate(const LSN &truncate_begin_lsn)
{
  int ret = OB_SUCCESS;
  if (!truncate_begin_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (pending_end_lsn_.is_valid()
             && truncate_begin_lsn < pending_end_lsn_) {
    // If truncate_begin_lsn is less than pending_end_lsn_,
    // need update it to avoid waiting slide fail.
    pending_end_lsn_ = truncate_begin_lsn;
  } else {
    // no need update pending_end_lsn_
  }
  PALF_LOG(INFO, "state_mgr truncate finished", K(ret), K_(palf_id), K_(pending_end_lsn),
      K(truncate_begin_lsn));
  return ret;
}

int LogStateMgr::to_follower_pending_()
{
  // save curr_end_lsn as pending_end_lsn_
  int ret = OB_SUCCESS;
  LSN curr_end_lsn;
  if (OB_FAIL(sw_->to_follower_pending(curr_end_lsn))) {
    PALF_LOG(WARN, "get_curr_end_lsn failed", K(ret), K_(palf_id));
  } else {
    pending_end_lsn_ = curr_end_lsn;
  }
  PALF_LOG(INFO, "to_follower_pending_", K(ret), K_(palf_id), K_(pending_end_lsn));
  return ret;
}

bool LogStateMgr::follower_init_need_switch_()
{
  bool state_changed = false;
  if (scan_disk_log_finished_) {
    state_changed = true;
  }
  return state_changed;
}

bool LogStateMgr::is_pending_log_clear_() const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  LSN last_slide_end_lsn;
  if (!pending_end_lsn_.is_valid()) {
    PALF_LOG_RET(ERROR, OB_INVALID_ERROR, "pending_end_lsn_ is invalid", K_(pending_end_lsn), K_(palf_id));
  } else if (OB_SUCCESS != (tmp_ret = sw_->get_last_slide_end_lsn(last_slide_end_lsn))) {
    PALF_LOG_RET(WARN, tmp_ret, "get_last_slide_end_lsn failed", K(tmp_ret), K_(palf_id));
  } else if (last_slide_end_lsn >= pending_end_lsn_) {
    bool_ret = true;
  } else {
    bool_ret = false;
    if (palf_reach_time_interval(1000 * 1000, check_follower_pending_warn_time_)) {
      PALF_LOG(INFO, "follower pending log not clear, need wait", K(bool_ret), K_(palf_id), K(last_slide_end_lsn), K_(pending_end_lsn));
    }
  }
  return bool_ret;
}

bool LogStateMgr::follower_pending_need_switch_()
{
  bool state_changed = false;
  int tmp_ret = OB_SUCCESS;
  LSN last_slide_end_lsn;
  if (is_pending_log_clear_()) {
    state_changed = true;
    PALF_LOG(INFO, "follower_pending_need_switch_", K(state_changed), K_(palf_id));
  }
  return state_changed;
}

bool LogStateMgr::follower_active_need_switch_()
{
  bool state_changed = false;
  common::ObAddr new_leader;
  if (need_update_leader_(new_leader)) {
    state_changed = true;
  } else if (new_leader.is_valid()
             && self_ == new_leader) {
    if (is_arb_replica()) {
      PALF_LOG(INFO, "arb replica is leader, skip", K_(palf_id), K_(self), K(new_leader));
    } else {
      state_changed = true;
    }
  } else {}
  return state_changed;
}

bool LogStateMgr::leader_reconfirm_need_switch_()
{
  int state_changed = false;
  common::ObAddr new_leader;
  int ret = OB_SUCCESS;
  if (is_reconfirm_need_wlock_()) {
    state_changed = true;
  } else if (is_reconfirm_timeout_()) {
    state_changed = true;
  } else if (is_reconfirm_state_changed_(ret)) {
    state_changed = true;
  } else if (need_update_leader_(new_leader)) {
    state_changed = true;
  } else {}
  return state_changed;
}

bool LogStateMgr::is_reconfirm_timeout_()
{
  bool bool_ret = false;
  const int64_t now_us = ObTimeUtility::current_time();

  const int64_t start_id = sw_->get_start_id();
  if (OB_INVALID_TIMESTAMP == last_check_start_id_time_us_
      || last_check_start_id_ != start_id) {
    last_check_start_id_ = start_id;
    last_check_start_id_time_us_ = now_us;
  } else {
    bool is_sw_timeout = false;

    if (now_us - last_check_start_id_time_us_ > PALF_LEADER_RECONFIRM_SYNC_TIMEOUT_US) {
      // start log of sw is timeout
      is_sw_timeout = true;
    }
    bool_ret = is_sw_timeout;
    if (bool_ret) {
      if (palf_reach_time_interval(1 * 1000 * 1000, check_reconfirm_timeout_time_)) {
        PALF_LOG_RET(WARN, OB_TIMEOUT, "leader reconfirm timeout", K_(palf_id), K(start_id), K(is_sw_timeout), K_(reconfirm));
        LOG_DBA_WARN_V2(OB_LOG_LEADER_RECONFIRM_TIMEOUT, OB_TIMEOUT, "leader reconfirm timeout");
        (void) sw_->report_log_task_trace(start_id);
      }
    } else if (palf_reach_time_interval(100 * 1000, check_reconfirm_timeout_time_)) {
      PALF_LOG(INFO, "leader reconfirm need wait", K_(palf_id), K(start_id), K(is_sw_timeout),
          K_(self), K_(reconfirm));
    } else {
      // do nothing
    }
  }
  return bool_ret;
}

bool LogStateMgr::is_reconfirm_need_wlock_()
{
  return reconfirm_->need_wlock();
}

bool LogStateMgr::is_reconfirm_state_changed_(int &ret)
{
  bool bool_ret = false;
  ret = reconfirm_->reconfirm();
  if (OB_EAGAIN == ret) {
  } else if (OB_SUCC(ret)) {
    PALF_LOG(TRACE, "is_reconfirm_state_changed_", K(bool_ret));
    bool_ret = true;
  } else {
    // election leader may has changed, so ret is maybe OB_NOT_MASTER.
    PALF_LOG(WARN, "reconfirm failed", K_(palf_id));
    bool_ret = true;
  }
  return bool_ret;
}

bool LogStateMgr::leader_active_need_switch_(bool &is_error)
{
  bool state_changed = false;
  common::ObAddr new_leader;
  is_error = false;
  if (need_update_leader_(new_leader)) {
    state_changed = true;
  } else if (LEADER == role_) {
    state_changed = check_leader_log_sync_state_();
    if (state_changed) {
      is_error = true;
    }
  } else {
    // do nothing
  }
  return state_changed;
}

bool LogStateMgr::check_leader_log_sync_state_()
{
  // This function is called only in <LEADER, ACTICE> state.
  bool state_changed = false;
  const int64_t start_id = sw_->get_start_id();
  const int64_t now_us = ObTimeUtility::current_time();
  if (OB_INVALID_TIMESTAMP == last_check_start_id_time_us_
      || last_check_start_id_ != start_id) {
    last_check_start_id_ = start_id;
    last_check_start_id_time_us_ = now_us;
  } else if (sw_->is_empty()) {
    // sw is empty
    last_check_start_id_time_us_ = now_us;
  } else {
    // sw is not empty, check log sync state
    if (now_us - last_check_start_id_time_us_ > PALF_LEADER_ACTIVE_SYNC_TIMEOUT_US) {
      if (palf_reach_time_interval(10 * 1000 * 1000, log_sync_timeout_warn_time_)) {
        PALF_LOG_RET(WARN, OB_TIMEOUT, "log sync timeout on leader", K_(palf_id), K_(self), K(now_us), K(last_check_start_id_time_us_), K(start_id));
        LOG_DBA_WARN_V2(OB_LOG_LEADER_LOG_SYNC_TIMEOUT, OB_TIMEOUT, "log sync timeout on leader");
        (void) sw_->report_log_task_trace(start_id);
      }
    }
  }
  return state_changed;
}

bool LogStateMgr::need_update_leader_(common::ObAddr &new_leader)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  int64_t new_leader_epoch = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(get_elect_leader_(new_leader, new_leader_epoch))) {
    if (OB_ELECTION_WARN_INVALID_LEADER != ret) {
      PALF_LOG(WARN, "get_elect_leader_ failed", K(ret), K_(palf_id));
    }
    if (leader_.is_valid()) {
      bool_ret = true;
      PALF_LOG(WARN, "get_elect_leader_ failed, leader_ is valid, need update", K(ret),
          K_(palf_id), K_(self), K(leader_), K(bool_ret));
    }
  } else {
    if (new_leader != leader_ || new_leader_epoch != leader_epoch_) {
      bool_ret = true;
      if (palf_reach_time_interval(1 * 1000 * 1000, update_leader_warn_time_)) {
        PALF_LOG(WARN, "leader or epoch has changed, need update", K(bool_ret), K(new_leader),
            K_(palf_id), K(new_leader_epoch), K(leader_), K(leader_epoch_), K_(self));
      }
    }
  }

  return bool_ret;
}

bool LogStateMgr::need_reset_broadcast_leader_() const
{
  const int64_t curr_time_us = common::ObTimeUtility::current_time();
  return broadcast_leader_.is_valid() &&
      (curr_time_us - last_recv_leader_broadcast_time_us_ > \
      2 * PALF_BROADCAST_LEADER_INFO_INTERVAL_US);
}

bool LogStateMgr::follower_need_update_role_(common::ObAddr &new_leader,
                                             int64_t &new_leader_epoch)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_elect_leader_(new_leader, new_leader_epoch))) {
    if (OB_ELECTION_WARN_INVALID_LEADER != ret) {
      PALF_LOG(WARN, "get_elect_leader_ failed", K(ret), K_(palf_id));
    }
    new_leader.reset();
    new_leader_epoch = OB_INVALID_TIMESTAMP;
  } else {
    bool_ret = (self_ == new_leader);
    if (bool_ret && is_arb_replica()) {
      PALF_LOG(INFO, "arb replica is leader, skip", K_(palf_id), K_(self), K(new_leader), K(new_leader_epoch));
      bool_ret = false;
    }
    PALF_LOG(TRACE, "follower_need_update_role_", K(new_leader), K(new_leader_epoch));
  }
  return bool_ret;
}

void LogStateMgr::set_leader_and_epoch_(const common::ObAddr &new_leader, const int64_t new_leader_epoch)
{
  leader_ = new_leader;
  leader_epoch_ = new_leader_epoch;
  PALF_LOG(INFO, "set_leader_and_epoch_", K_(palf_id), K_(self), K(leader_), K(leader_epoch_), K(new_leader));
}

int LogStateMgr::get_elect_leader_(common::ObAddr &leader,
                                   int64_t &leader_epoch) const
{
  int ret = OB_SUCCESS;
  ObAddr tmp_addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCC(election_->get_current_leader_likely(tmp_addr, leader_epoch))) {
    leader = tmp_addr;
  } else {
//    if (palf_reach_time_interval(1000 * 1000, last_get_leader_fail_time_)) {
      PALF_LOG(WARN, "get_current_leader_likely failed", K(ret), K_(palf_id));
//    }
  }

  return ret;
}

int LogStateMgr::check_role_leader_and_state() const
{
  int ret = OB_SUCCESS;
  if (!check_role_and_state_(LEADER, ACTIVE)) {
    ret = OB_NOT_MASTER;
  }
  return ret;
}

bool LogStateMgr::check_epoch_is_same_with_election(const int64_t expected_epoch) const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  common::ObAddr ununsed_leader;
  ObRole curr_election_role = ObRole::INVALID_ROLE;
  int64_t curr_election_epoch = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(election_->get_role(curr_election_role, curr_election_epoch))) {
    PALF_LOG(WARN, "get_elect_leader_ failed", K(ret));
  } else if (OB_UNLIKELY(curr_election_role != ObRole::LEADER)) {
    PALF_LOG(WARN, "election is not leader now", K(ret), K(curr_election_role), K(curr_election_epoch));
  } else if (OB_UNLIKELY(curr_election_epoch != expected_epoch)) {
    PALF_LOG(WARN, "election epoch has changed", K(ret), K(curr_election_epoch), K(expected_epoch));
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogStateMgr::can_send_log_ack(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != get_proposal_id()) {
    // proposal_id has changed during disk flushing
  } else {
    bool_ret = (is_follower_active_() || is_follower_pending_());
  }
  return bool_ret;
}

bool LogStateMgr::can_receive_log_ack(const int64_t &proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id == get_proposal_id()) {
    bool_ret = (LEADER == role_ && mode_mgr_->can_receive_log());
  }
  return bool_ret;
}

bool LogStateMgr::can_truncate_log() const
{
  bool bool_ret = (is_follower_active_() || is_follower_pending_());
  return bool_ret;
}

bool LogStateMgr::need_freeze_group_buffer() const
{
  // Replica need freeze group buffer during <LEADER, ACTIVE> state.
  // PENDING state is not considered here because all logs have been freezed before replica
  // switches into PENDING.
  return is_leader_active_();
}

int LogStateMgr::check_and_try_fetch_log_()
{
  int ret = OB_SUCCESS;
  const int64_t now_us = ObTimeUtility::current_time();
  const uint64_t start_id = sw_->get_start_id();
  if (false == ATOMIC_LOAD(&is_sync_enabled_)) {
    if (palf_reach_time_interval(5 * 1000 * 1000, check_sync_enabled_time_)) {
      PALF_LOG(INFO, "sync is disabled, cannot fetch log", K_(palf_id));
    }
  } else if (OB_INVALID_ID == last_check_start_id_
      || OB_INVALID_TIMESTAMP == last_check_start_id_time_us_
      || (last_check_start_id_ == start_id
          && now_us - last_check_start_id_time_us_ > PALF_FETCH_LOG_INTERVAL_US)) {
    if (OB_FAIL(sw_->try_fetch_log(FetchTriggerType::LOG_LOOP_TH))) {
      PALF_LOG(WARN, "sw try_fetch_log failed", K(ret), K_(palf_id));
    } else {
      last_check_start_id_ = start_id;
      last_check_start_id_time_us_ = now_us;
      PALF_LOG(TRACE, "sw try_fetch_log success", K(ret), K_(palf_id), K(start_id));
    }
  } else if (last_check_start_id_ != start_id) {
    last_check_start_id_ = start_id;
    last_check_start_id_time_us_ = now_us;
  } else {}
  return ret;
}

bool LogStateMgr::is_sync_enabled() const
{
  return ATOMIC_LOAD(&is_sync_enabled_);
}

int LogStateMgr::enable_sync()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_arb_replica()) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(INFO, "can not enable_sync in arb_replica", K(ret), K_(palf_id), K(self_),
        "replica_type", replica_type_2_str(replica_type_));
  } else {
    ATOMIC_STORE(&is_sync_enabled_, true);
    PALF_LOG(INFO, "enable_sync success", K(ret), K_(palf_id), K(self_));
  }
  return ret;
}

int LogStateMgr::disable_sync()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
//  } else if (true == is_follower_pending_()) {
//    ret = OB_STATE_NOT_MATCH;
//    PALF_LOG(WARN, "can not disable sycn in follower pending", K(ret), KPC(this));
  } else {
    ATOMIC_STORE(&is_sync_enabled_, false);
    PALF_LOG(INFO, "disable_sync success", K(ret), K_(palf_id), K(self_));
  }
  return ret;
}

bool LogStateMgr::is_allow_vote() const
{
  return ATOMIC_LOAD(&allow_vote_);
}

bool LogStateMgr::is_allow_vote_persisted() const
{

  return ATOMIC_LOAD(&allow_vote_persisted_);
}

int LogStateMgr::disable_vote_in_mem()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ATOMIC_STORE(&allow_vote_, false);
    PALF_LOG(INFO, "disable_vote_in_mem success", K(ret), K_(palf_id), K_(self));
  }
  return ret;
}

int LogStateMgr::disable_vote()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ATOMIC_STORE(&allow_vote_, false);
    ATOMIC_STORE(&allow_vote_persisted_, false);
    PALF_LOG(INFO, "disable_vote success", K(ret), K_(palf_id), K_(self));
  }
  return ret;
}

int LogStateMgr::enable_vote()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ATOMIC_STORE(&allow_vote_, true);
    ATOMIC_STORE(&allow_vote_persisted_, true);
    PALF_LOG(INFO, "enable_vote success", K(ret), K_(palf_id), K_(self));
  }
  return ret;
}

LogReplicaType LogStateMgr::get_replica_type() const
{
  return ATOMIC_LOAD(&replica_type_);
}

bool LogStateMgr::is_arb_replica() const
{
  return get_replica_type() == LogReplicaType::ARBITRATION_REPLICA;
}

int LogStateMgr::get_election_role(common::ObRole &role, int64_t &epoch) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(election_->get_role(role, epoch))) {
    PALF_LOG(WARN, "get elect role failed", K(ret), K_(self), K_(palf_id));
  } else {
    // do nothing
  }
  return ret;
}

// protected by wlock in PalfHandleImpl
int LogStateMgr::set_changing_config_with_arb()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (true == ATOMIC_BCAS(&is_changing_config_with_arb_, false, true)) {
    last_set_changing_config_with_arb_time_us_ = common::ObTimeUtility::current_time();
    PALF_EVENT("set_changing_config_with_arb to true", palf_id_, K(ret), K_(self));
  }
  return ret;
}

// protected by wlock in PalfHandleImpl
int LogStateMgr::reset_changing_config_with_arb()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (true == ATOMIC_BCAS(&is_changing_config_with_arb_, true, false)) {
    const int64_t cost_time_us = (common::ObTimeUtility::current_time()
      - last_set_changing_config_with_arb_time_us_);
    PALF_EVENT("set_changing_config_with_arb to false", palf_id_, K(ret), K_(self), K(cost_time_us));
  }
  return ret;
}

// protected by rlock in PalfHandleImpl
bool LogStateMgr::is_changing_config_with_arb() const
{
  return ATOMIC_LOAD(&is_changing_config_with_arb_);
}

void LogStateMgr::reset_broadcast_leader_()
{
  if (need_reset_broadcast_leader_()) {
    PALF_LOG_RET(INFO, OB_SUCCESS, "reset_broadcast_leader_", K_(self), K_(palf_id),
        K_(last_recv_leader_broadcast_time_us), K_(broadcast_leader));
    broadcast_leader_.reset();
  }
}

} // namespace palf
} // namespace oceanbase
