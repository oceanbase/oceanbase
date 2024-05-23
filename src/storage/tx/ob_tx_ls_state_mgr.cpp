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

#include "storage/tx/ob_tx_ls_state_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

#define LOAD_CUR_STATE_CONTAINER         \
  TxLSStateContainer ls_state_container; \
  ls_state_container.state_container_ = ATOMIC_LOAD(&cur_state_.state_container_);

namespace oceanbase
{

namespace transaction
{

int ObTxLSStateMgr::TxLSStateHashFunc::operator()(const TxStateActionPair &key,
                                                  TxLSStateContainer &res) const
{
  int ret = OB_SUCCESS;

  res = key.base_state_;
  switch (key.exec_action_) {
  case TxLSAction::START: {
    if (key.base_state_.state_val_.state_ == TxLSState::INIT) {
      clear_all_flag_(res);
      res.state_val_.state_ = TxLSState::F_WORKING;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::STOP: {

    if (key.base_state_.state_val_.state_ == TxLSState::INIT) {
      // do nothing
    } else {
      clear_all_flag_(res);
      res.state_val_.state_ = TxLSState::STOPPED;
    }
    break;
  }
  case TxLSAction::ONLINE: {
    res.state_val_.offline_flag_ = false;
    res.state_val_.block_start_tx_flag_ = false;
    res.state_val_.block_start_readonly_flag_ = false;
    break;
  }
  case TxLSAction::OFFLINE: {
    res.state_val_.offline_flag_ = true;
    break;
  }
  case TxLSAction::LEADER_REVOKE_GRACEFULLY: {
    if (key.base_state_.state_val_.state_ == TxLSState::L_WORKING
        || key.base_state_.state_val_.state_ == TxLSState::T_SYNC_FAILED
        || key.base_state_.state_val_.state_ == TxLSState::R_SYNC_FAILED) {
      res.state_val_.state_ = TxLSState::F_WORKING;
      res.state_val_.block_start_normal_tx_flag_ = false;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::LEADER_REVOKE_FORCEDLLY: {
    if (key.base_state_.state_val_.state_ == TxLSState::L_WORKING
        || key.base_state_.state_val_.state_ == TxLSState::T_SYNC_FAILED
        || key.base_state_.state_val_.state_ == TxLSState::R_SYNC_FAILED) {
      res.state_val_.state_ = TxLSState::F_WORKING;
      res.state_val_.block_start_normal_tx_flag_ = false;
    } else if (key.base_state_.state_val_.state_ == TxLSState::T_APPLY_PENDING
               || key.base_state_.state_val_.state_ == TxLSState::R_APPLY_PENDING) {
      res.state_val_.state_ = TxLSState::F_SWL_PENDING;
      res.state_val_.block_start_normal_tx_flag_ = false;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::SWL_CB_SUCC: {
    if (key.base_state_.state_val_.state_ == TxLSState::T_SYNC_PENDING) {
      res.state_val_.state_ = TxLSState::T_APPLY_PENDING;
    } else if (key.base_state_.state_val_.state_ == TxLSState::R_SYNC_PENDING) {
      res.state_val_.state_ = TxLSState::R_APPLY_PENDING;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::SWL_CB_FAIL: {

    if (key.base_state_.state_val_.state_ == TxLSState::T_SYNC_PENDING) {
      res.state_val_.state_ = TxLSState::T_SYNC_FAILED;
    } else if (key.base_state_.state_val_.state_ == TxLSState::R_SYNC_PENDING) {
      res.state_val_.state_ = TxLSState::R_SYNC_FAILED;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::APPLY_SWL_SUCC: {
    if (key.base_state_.state_val_.state_ == TxLSState::T_APPLY_PENDING
        || key.base_state_.state_val_.state_ == TxLSState::R_APPLY_PENDING) {
      res.state_val_.state_ = TxLSState::L_WORKING;
    } else if (key.base_state_.state_val_.state_ == TxLSState::F_SWL_PENDING) {
      res.state_val_.state_ = TxLSState::F_WORKING;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::APPLY_SWL_FAIL: {
    if (key.base_state_.state_val_.state_ == TxLSState::T_APPLY_PENDING) {
      res.state_val_.state_ = TxLSState::T_APPLY_PENDING;
    } else if (key.base_state_.state_val_.state_ == TxLSState::R_APPLY_PENDING) {
      res.state_val_.state_ = TxLSState::R_APPLY_PENDING;
    } else if (key.base_state_.state_val_.state_ == TxLSState::F_SWL_PENDING) {
      res.state_val_.state_ = TxLSState::F_SWL_PENDING;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::LEADER_TAKEOVER: {
    if (key.base_state_.state_val_.state_ == TxLSState::F_WORKING) {
      res.state_val_.state_ = TxLSState::T_SYNC_PENDING;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::RESUME_LEADER: {
    if (key.base_state_.state_val_.state_ == TxLSState::F_WORKING) {
      res.state_val_.state_ = TxLSState::R_SYNC_PENDING;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::BLOCK_START_TX: {
    res.state_val_.block_start_tx_flag_ = true;
    break;
  }
  case TxLSAction::BLOCK_START_READONLY: {
    res.state_val_.block_start_readonly_flag_ = true;
    break;
  }
  case TxLSAction::BLOCK_START_NORMAL_TX: {
    if (key.base_state_.state_val_.state_ == TxLSState::L_WORKING) {
      res.state_val_.block_start_normal_tx_flag_ = true;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::UNBLOCK_NORMAL_TX: {
    if (key.base_state_.state_val_.state_ == TxLSState::L_WORKING) {
      res.state_val_.block_start_normal_tx_flag_ = false;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    break;
  }
  case TxLSAction::BLOCK_START_WR: {
    res.state_val_.block_start_tx_flag_ = true;
    res.state_val_.block_start_readonly_flag_ = true;
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unkonwn action for ls_tx_state", K(ret), K(key), K(res.state_val_));
  }
  }

  return ret;
}

int ObTxLSStateMgr::switch_tx_ls_state(const TxLSAction action, const share::SCN start_working_scn)
{
  int ret = OB_SUCCESS;
  if (TxLSAction::SWL_CB_SUCC == action) {
    share::SCN tmp_max_applied_swl_scn = max_applied_start_working_ts_.atomic_load();
    if (!max_applying_start_working_ts_.atomic_bcas(tmp_max_applied_swl_scn,
                                                    tmp_max_applied_swl_scn)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "prev start working log has not been applied", K(ret), "action",
                action_str(action), K(start_working_scn), KPC(this));
    } else {
      max_applying_start_working_ts_.inc_update(start_working_scn);
    }
  } else if (TxLSAction::APPLY_SWL_SUCC == action) {
    if (!max_applying_start_working_ts_.atomic_bcas(start_working_scn, start_working_scn)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Try to applied a unexpected start working scn", K(ret), "action",
                action_str(action), K(start_working_scn), KPC(this));
    } else {
      max_applied_start_working_ts_.inc_update(start_working_scn);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(execute_tx_ls_action_(action))) {
    TRANS_LOG(WARN, "execute tx ls action failed", K(ret), K(ret), "action", action_str(action),
              K(start_working_scn), KPC(this));
  }
  return ret;
}

void ObTxLSStateMgr::restore_tx_ls_state()
{
  int ret = OB_SUCCESS;
  TxLSStateContainer restore_state_container;
  restore_state_container.state_container_ = 0;
  restore_state_container.state_container_ = ATOMIC_LOAD(&prev_state_.state_container_);
  ATOMIC_STORE(&cur_state_.state_container_, restore_state_container.state_container_);
  TRANS_LOG(INFO, "[Tx LS State] restore ls tx state successfully", K(ret),
            K(restore_state_container.state_val_));
}

bool ObTxLSStateMgr::need_retry_apply_SWL(share::SCN &applying_swl_scn)
{
  bool need_retry = false;
  LOAD_CUR_STATE_CONTAINER
  if (ls_state_container.state_val_.state_ == TxLSState::R_APPLY_PENDING
      || ls_state_container.state_val_.state_ == TxLSState::T_APPLY_PENDING
      || ls_state_container.state_val_.state_ == TxLSState::F_SWL_PENDING) {

    share::SCN max_applied_swl_scn = max_applied_start_working_ts_.atomic_load();

    if (max_applying_start_working_ts_.atomic_bcas(max_applied_swl_scn, max_applied_swl_scn)) {
      need_retry = false;
    } else {
      applying_swl_scn = max_applying_start_working_ts_.atomic_load();
      need_retry = true;
    }
  }

  return need_retry;
}

bool ObTxLSStateMgr::waiting_SWL_cb()
{
  bool is_waiting = false;

  LOAD_CUR_STATE_CONTAINER

  if (ls_state_container.state_val_.state_ == TxLSState::R_SYNC_PENDING
      || ls_state_container.state_val_.state_ == TxLSState::T_SYNC_PENDING) {
    is_waiting = true;
  }

  return is_waiting;
}

void ObTxLSStateMgr::replay_SWL_succ(const share::SCN &swl_scn)
{
  int ret = OB_SUCCESS;
  share::SCN tmp_max_applied_swl_scn = max_applied_start_working_ts_.atomic_load();
  if (!max_applying_start_working_ts_.atomic_bcas(tmp_max_applied_swl_scn,
                                                  tmp_max_applied_swl_scn)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "prev start working log has not been applied", K(ret), KPC(this));
  }
  max_applying_start_working_ts_.inc_update(swl_scn);
  max_applied_start_working_ts_.inc_update(swl_scn);
}

const char *ObTxLSStateMgr::iter_ctx_mgr_stat_info(uint64_t &state_container,
                                                   bool &is_master,
                                                   bool &is_stopped) const
{
  LOAD_CUR_STATE_CONTAINER
  is_master = (ls_state_container.state_val_.state_ == TxLSState::L_WORKING);
  is_stopped = (ls_state_container.state_val_.state_ == TxLSState::STOPPED);
  state_container = ls_state_container.state_container_;
  return to_cstring(ls_state_container.state_val_);
}

bool ObTxLSStateMgr::is_master() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.state_ == TxLSState::L_WORKING;
}

bool ObTxLSStateMgr::is_follower() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.state_ == TxLSState::F_WORKING
         || ls_state_container.state_val_.state_ == TxLSState::F_SWL_PENDING;
}

bool ObTxLSStateMgr::is_block_start_tx() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.block_start_tx_flag_;
}

bool ObTxLSStateMgr::is_block_start_readonly() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.block_start_readonly_flag_;
}

bool ObTxLSStateMgr::is_block_start_normal_tx() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.block_start_normal_tx_flag_;
}

bool ObTxLSStateMgr::is_block_WR() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.block_start_tx_flag_
         || ls_state_container.state_val_.block_start_readonly_flag_;
}

bool ObTxLSStateMgr::is_leader_takeover_pending() const
{
  LOAD_CUR_STATE_CONTAINER
  return is_switch_leader_pending_(ls_state_container)
         || is_resume_leader_pending_(ls_state_container);
}

bool ObTxLSStateMgr::is_start_working_apply_pending() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.state_ == TxLSState::T_APPLY_PENDING
         || ls_state_container.state_val_.state_ == TxLSState::R_APPLY_PENDING;
}

bool ObTxLSStateMgr::is_switch_leader_pending() const
{
  LOAD_CUR_STATE_CONTAINER
  return is_switch_leader_pending_(ls_state_container);
}

bool ObTxLSStateMgr::is_resume_leader_pending() const
{
  LOAD_CUR_STATE_CONTAINER
  return is_resume_leader_pending_(ls_state_container);
}

bool ObTxLSStateMgr::is_stopped() const
{
  LOAD_CUR_STATE_CONTAINER
  return ls_state_container.state_val_.state_ == TxLSState::STOPPED;
}

int ObTxLSStateMgr::execute_tx_ls_action_(const TxLSAction action)
{
  int ret = OB_SUCCESS;

  TxLSStateContainer res_state;
  res_state.state_container_ = 0;
  TxStateActionPair pair;
  pair.base_state_ = cur_state_;
  pair.exec_action_ = action;

  TxLSStateHashFunc state_func;

  ret = state_func(pair, res_state);

  if (OB_SUCCESS != ret || UINT64_MAX == res_state.state_container_) {
    TRANS_LOG(WARN, "[Tx LS State] switch ls tx state failed", K(ret), K(pair),
              K(res_state.state_val_), KPC(this));
  } else {
    ATOMIC_STORE(&prev_state_.state_container_, pair.base_state_.state_container_);
    ATOMIC_STORE(&cur_state_.state_container_, res_state.state_container_);
    TRANS_LOG(INFO, "[Tx LS State] switch ls tx state successfully", K(ret), K(pair),
              K(res_state.state_val_), KPC(this));
  }

  return ret;
}

void ObTxLSStateMgr::clear_all_flag_(TxLSStateContainer &cur_state)
{
  cur_state.state_val_.offline_flag_ = false;
  cur_state.state_val_.block_start_tx_flag_ = false;
  cur_state.state_val_.block_start_readonly_flag_ = false;
  cur_state.state_val_.block_start_normal_tx_flag_ = false;
}

bool ObTxLSStateMgr::is_switch_leader_pending_(const TxLSStateContainer &ls_state_container)
{
  return ls_state_container.state_val_.state_ == TxLSState::T_SYNC_PENDING
         || ls_state_container.state_val_.state_ == TxLSState::T_SYNC_FAILED
         || ls_state_container.state_val_.state_ == TxLSState::T_APPLY_PENDING;
}

bool ObTxLSStateMgr::is_resume_leader_pending_(const TxLSStateContainer &ls_state_container)
{

  return ls_state_container.state_val_.state_ == TxLSState::R_SYNC_PENDING
         || ls_state_container.state_val_.state_ == TxLSState::R_SYNC_FAILED
         || ls_state_container.state_val_.state_ == TxLSState::R_APPLY_PENDING;
}

} // namespace transaction

} // namespace oceanbase
