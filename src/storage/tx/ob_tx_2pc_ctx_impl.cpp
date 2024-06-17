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

#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_committer_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace transaction
{

// get_2pc_role is engaged with the current state, so it may become from a leaf
// to a internal at later. So we can only decide its state under lock at one time.
Ob2PCRole ObPartTransCtx::get_2pc_role() const
{
  Ob2PCRole role = Ob2PCRole::UNKNOWN;

  if (exec_info_.upstream_.is_valid()) {
    if (exec_info_.upstream_ == ls_id_) {
      role = Ob2PCRole::ROOT;
    } else if (exec_info_.participants_.empty()) {
      // not root & downstream is empty
      // root must not be leaf, because the distributed txn must be composed by
      // more than one participants.
      role = Ob2PCRole::LEAF;
    } else {
      role = Ob2PCRole::INTERNAL;
    }
  }

  return role;
}

int64_t ObPartTransCtx::get_downstream_size() const
{
  return exec_info_.participants_.count();
}

int64_t ObPartTransCtx::get_self_id()
{
  int ret = OB_SUCCESS;
  if (self_id_ == -1) {
    if (OB_FAIL(find_participant_id_(ls_id_, self_id_))) {
      if (is_root()) {
        TRANS_LOG(ERROR, "find participant id failed", K(ret), K(*this));
      } else {
        self_id_ = -1;
      }
    }
  }
  return self_id_;
}

int ObPartTransCtx::restart_2pc_trans_timer_()
{
  int ret = OB_SUCCESS;

  trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
  (void)unregister_timeout_task_();
  if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
    TRANS_LOG(WARN, "register timeout handler error", KR(ret), "context", *this);
  }

  return ret;
}

/*
 * If no_need_submit_log is true, it will not submit prepare log after do_prepare.
 * XA and dup_table will submit commit info log in do_prepare and drive to submit prepare log after the conditions are met.
 * When prepare log on_succ, 2PC will enter into next phase.
 * */
int ObPartTransCtx::do_prepare(bool &no_need_submit_log)
{
  int ret = OB_SUCCESS;
  no_need_submit_log = false;

  if (OB_SUCC(ret)) {
    if (sub_state_.is_force_abort()) {
      if (OB_FAIL(prepare_mul_data_source_tx_end_(false))) {
        TRANS_LOG(WARN, "notify mds tx end fail", K(ret), K(trans_id_), K(ls_id_));
      } else if (OB_FAIL(compensate_abort_log_())) {
        TRANS_LOG(WARN, "compensate abort log failed", K(ret), K(ls_id_), K(trans_id_),
                  K(get_downstream_state()), K(get_upstream_state()), K(sub_state_));
      } else {
        ret = OB_TRANS_KILLED;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(search_unsubmitted_dup_table_redo_())) {
      TRANS_LOG(WARN, "search unsubmitted dup table redo", K(ret), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    if (exec_info_.is_dup_tx_ && !is_dup_table_redo_sync_completed_()) {
      no_need_submit_log = true;
      if (OB_FAIL(dup_table_tx_redo_sync_())) {
        TRANS_LOG(WARN, "dup table tx  redo sync failed", K(ret));
      }
    }
  }

  if (exec_info_.is_dup_tx_) {
    TRANS_LOG(INFO, "do prepare for dup table", K(ret), K(dup_table_follower_max_read_version_),
              K(is_dup_table_redo_sync_completed_()), KPC(this));
  }

  if (OB_SUCC(ret) && OB_FAIL(prepare_mul_data_source_tx_end_(true))) {
    TRANS_LOG(WARN, "notify mds tx_end fail", K(ret), K(trans_id_), K(ls_id_));
  }

  if (OB_SUCC(ret) && OB_FAIL(generate_prepare_version_())) {
    TRANS_LOG(WARN, "generate prepare version failed", K(ret), K(*this));
  }

  if (OB_SUCC(ret) && OB_FAIL(restart_2pc_trans_timer_())) {
    TRANS_LOG(WARN, "restart_2pc_trans_timer_ error", KR(ret), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::on_prepare()
{
  int ret = OB_SUCCESS;

  // currently do nothing

  return ret;
}


/* Why do we need to execute wait_gts_elapse_commit_version_ successfully before post
 * the pre-commit message;
 *
 * The pre-commit request does not write the palf log; then when the leader replica is switched,
 * the information of the received pre-commit message will be lost, which is equivalent to not
 * executing the pre-commit;
 * Exec wait_gts_elapse_commit_version_ before the pre-commit message is sent, which is equivalent
 * to adding a limit, the max_commit_ts timestamp of pre-commit promotion will not exceed gts;
 * the leader replica switch is completed and then max_commit_ts is updated to the gts value;
 * equivalent to executing pre-commit ;
 *
 * Scenarios that trigger the problem:
 *
 * 1. LS A as coordinator, LS B as participant;
 * 2. LS A sends a pre-commit message to LS B, the pre-commit message (version = 105) will push the
 *    max_commit_ts of each node higher than gts (version = 100);
 *
 * 3. The leader replica of LS B responded to the pre-commit message successfully;
 * 4. The part_ctx on LS A runs to the commit state and executes wait_gts_elapse_commit_version_;
 * 5. The leader replica switch of LS B occurs. After the leader replica switch is completed,
 *    the max_commit_ts of the leader replica of LS B is updated to the gts value (version = 100);
 * 6. LS A wait_gts_elapse_commit_version_ is executed successfully and returns success to the
 *    client, let the client know that value (version=105) has been successfully committed;
      The OB_MSG_TX_COMMIT_REQ message sent by LS A to LS B is still in the network;
 * 7. After the client receives the successful commit, it sends a read request req_1 to the LS B;
 *    the thread thread_1 that processes the req_1 obtains the max_commit_ts of the LS B leader
 *    replica as snapshot ( version = 100 ); then this thread_1 is suspended for scheduling reasons;
 * 8. Then LS B receives the OB_MSG_TX_COMMIT_REQ sent by LS A, updates max_commit_ts, and unlocks
 *    the row;
 * 9. The thread_1 continues to execute, and will read the value of version = 100;
 */
int ObPartTransCtx::do_pre_commit(bool &need_wait)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  need_wait = false;
  if (is_root()) {
    if (OB_FAIL(ctx_tx_data_.set_commit_version(exec_info_.prepare_version_))) {
      TRANS_LOG(ERROR, "set tx data commit version", K(ret), K(ctx_tx_data_));
    } else if (OB_FAIL(wait_gts_elapse_commit_version_(need_wait))) {
      TRANS_LOG(ERROR, "wait gts elapse commit version failed", KR(ret), KPC(this));
    } else {
    }
  }

  // try to post ts sync request at first
  if (exec_info_.is_dup_tx_ && (OB_TMP_FAIL(dup_table_tx_pre_commit_()))) {
    if (OB_EAGAIN == tmp_ret) {
      TRANS_LOG(INFO, "dup table pre_commit is not finish", K(tmp_ret));
    } else {
      TRANS_LOG(WARN, "dup table pre_commit error", K(tmp_ret));
    }
  }

  if (!need_wait && OB_FAIL(update_local_max_commit_version_(ctx_tx_data_.get_commit_version()))) {
    TRANS_LOG(ERROR, "update publish version failed", KR(ret), KPC(this));
  }
  if (OB_SUCC(ret) && OB_FAIL(restart_2pc_trans_timer_())) {
    TRANS_LOG(ERROR, "restart_2pc_trans_timer_ error", KR(ret), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::do_commit()
{
  int ret = OB_SUCCESS;
  SCN gts, local_max_read_version;

  if (is_local_tx_()) {
    if (OB_FAIL(get_gts_(gts))) {
      if (OB_EAGAIN == ret) {
        TRANS_LOG(INFO, "get gts eagain", KR(ret), KPC(this));
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(ERROR, "get gts failed", KR(ret), K(*this));
      }
    } else {
      // To order around the read-write conflict(anti dependency), we need push
      // the txn version upper than all previous read version. So we record all
      // read version each access begins and get the max read version to handle
      // the dependency conflict
      mt_ctx_.before_prepare(gts);
      if (OB_FAIL(get_local_max_read_version_(local_max_read_version))) {
        TRANS_LOG(ERROR, "get local max read version failed", KR(ret), K(*this));
      } else if (OB_FAIL(ctx_tx_data_.set_commit_version(SCN::max(gts, local_max_read_version)))) {
        TRANS_LOG(ERROR, "set tx data commit version failed", K(ret));
      }
    }
  } else {
    // Because Oceanbase two phase commit delay is optimized through distributed
    // prepare state, we can guarantee commit state even commit log hasnot been
    // synchronized.
    //
    // We cannot release row lock until the commit has been submitted(Note the
    // difference between 'synchronized' and 'submitted'), because the other txn
    // that touches the same row and commits after the txn may submit its own log
    // about the row or even the commit info before this txn's.
    //
    // TODO(handora.qc): add ls info arr to prepare_log_info_arr
    // ObLSLogInfoArray ls_info_arr;
    //
    // We already update the local max commit version in precommit phase,
    // while we optimize the phase and skip some participants' request,
    // so we need always update local max commit version
    if (OB_FAIL(update_local_max_commit_version_(ctx_tx_data_.get_commit_version()))) {
      TRANS_LOG(ERROR, "update local max commit version failed", K(ret), KPC(this));
    } else if (is_root() && OB_FAIL(coord_prepare_info_arr_.assign(exec_info_.prepare_log_info_arr_))) {
      TRANS_LOG(WARN, "assign coord_prepare_info_arr_ for root failed", K(ret));
    } else if (is_root()) {
      check_and_response_scheduler_(ObTxState::COMMIT, OB_SUCCESS);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(restart_2pc_trans_timer_())) {
    TRANS_LOG(ERROR, "restart_2pc_trans_timer_ error", KR(ret), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::check_and_response_scheduler_(ObTxState next_phase, int result)
{
  int ret = OB_SUCCESS;
  // when error inject, response scheduler delayed to CLEAR state
  int inject_err = OB_E(EventTable::EN_EARLY_RESPONSE_SCHEDULER) OB_SUCCESS;
  if (!is_sub2pc()
      && inject_err != OB_SUCCESS
      && next_phase != ObTxState::CLEAR
      && !callback_scheduler_on_clear_) {
    callback_scheduler_on_clear_ = true;
    return OB_SUCCESS;
  }

  if (callback_scheduler_on_clear_ && ObTxState::CLEAR != next_phase) {
    // delayed, skip other state
    return OB_SUCCESS;
  }

  if (is_sub2pc()) {
    // TODO, according to part trans action
    if (ObTxState::COMMIT == next_phase) {
      if (OB_FAIL(reply_to_scheduler_for_sub2pc(SUBCOMMIT_RESP))) {
        TRANS_LOG(ERROR, "fail to reply sub commit", K(ret), K(*this));
      }
    } else if (ObTxState::ABORT == next_phase || ObTxState::ABORT == get_downstream_state()) {
      if (OB_FAIL(reply_to_scheduler_for_sub2pc(SUBROLLBACK_RESP))) {
        TRANS_LOG(ERROR, "fail to reply sub rollback", K(ret), K(*this));
      }
    }
  } else {
    if (OB_FAIL(post_tx_commit_resp_(result))) {
      TRANS_LOG(WARN, "post commit response failed", KR(ret), K(*this));
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "response scheduler in 2pc", K(ret), K(result), KPC(this));
  }
  return ret;
}


int ObPartTransCtx::update_local_max_commit_version_(const SCN &commit_version)
{
  int ret = OB_SUCCESS;
  trans_service_->get_tx_version_mgr().update_max_commit_ts(commit_version, false);
  return ret;
}

int ObPartTransCtx::on_commit()
{
  int ret = OB_SUCCESS;

  if (is_local_tx_()) {
    // TODO: fill it for sp commit
  } else {
    if (OB_FAIL(on_dist_end_(true /*commit*/))) {
      TRANS_LOG(WARN, "transaciton end error", KR(ret), "context", *this);
    }
  }

  return ret;
}

int ObPartTransCtx::do_abort()
{
  int ret = OB_SUCCESS;

  if (is_sub2pc()) {
    // do nothing
  } else {
    if (is_root()) {
      check_and_response_scheduler_(ObTxState::ABORT, OB_TRANS_KILLED);
    }
  }

  return ret;
}

int ObPartTransCtx::on_abort()
{
  int ret = OB_SUCCESS;

  if (is_sub2pc() && is_root()) {
    check_and_response_scheduler_(ObTxState::ABORT, OB_TRANS_KILLED);
  }
  if (OB_FAIL(on_dist_end_(false /*commit*/))) {
    TRANS_LOG(WARN, "transaciton end error", KR(ret), "context", *this);
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "transaciton clear error", KR(ret), "context", *this);
  }

  return ret;
}

int ObPartTransCtx::do_clear()
{
  int ret = OB_SUCCESS;

  if (is_root()) {
    // response scheduler after all participant commit log sycned
    const int result = ctx_tx_data_.get_state() == ObTxCommitData::COMMIT ? OB_SUCCESS : OB_TRANS_ROLLBACKED;
    check_and_response_scheduler_(ObTxState::CLEAR, result);
  }
  // currently do nothing

  return ret;
}

int ObPartTransCtx::on_clear()
{
  int ret = OB_SUCCESS;

  (void)unregister_timeout_task_();
  (void)trans_clear_();
  (void)set_exiting_();

  return ret;
}

int ObPartTransCtx::reply_to_scheduler_for_sub2pc(int64_t msg_type)
{
  int ret = OB_SUCCESS;

  if (is_local_tx_()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected trans type", KR(ret), K(*this));
  } else if (!is_root()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "not root, unexpected", KR(ret), K(*this));
  } else {
    if (SUBPREPARE_RESP == msg_type) {
      if (OB_FAIL(post_tx_sub_prepare_resp_(OB_SUCCESS /*commit*/))) {
        TRANS_LOG(WARN, "fail to post sub prepare response", KR(ret), K(*this));
        ret = OB_SUCCESS;
      }
      TRANS_LOG(INFO, "reply to scheduler for sub prepare", KR(ret), K(*this));
    } else if (SUBCOMMIT_RESP == msg_type) {
      if (OB_FAIL(post_tx_sub_commit_resp_(OB_SUCCESS))) {
        TRANS_LOG(WARN, "fail to post sub commit response", KR(ret), K(*this));
        ret = OB_SUCCESS;
      }
      TRANS_LOG(INFO, "reply to scheduler for sub commit", KR(ret), K(*this));
    } else if (SUBROLLBACK_RESP == msg_type) {
      if (OB_FAIL(post_tx_sub_rollback_resp_(OB_SUCCESS))) {
        TRANS_LOG(WARN, "fail to post sub rollback response", KR(ret), K(*this));
        ret = OB_SUCCESS;
      }
      TRANS_LOG(INFO, "reply to scheduler for sub rollback", KR(ret), K(*this));
    }
  }

  return ret;
}

// When to merge the intermediate_participants into the participants in a two
// phase commit needs careful consideration. One of the most critical factors is
// how to deal with concurrency with the transfer out logs.
//
// The primary rule we need to follow is that "the two phase commit state before
// the transfer out log will be relocated to the dest, and the two phase commit
// state after this log will participant into the src participants, progressing
// through a tree-style two phase commit." Therefore, before committing the
// transfer out log, we will first block the advancement of the two phase commit
// protocol for all txns which is required by the transfer (by blocking the
// advancement of the in-memory state through "drive_self_2pc_phase" and the
// advancement of the persistent state machine through "submit_log_if_allow"),
// ensuring the integrity of the two phase commit state. Simultaneously, we will
// add intermediate_participants for these blocked txns.
//
// The second rule is that we need to adhere to the principle that "when a
// participant of a txn enters a certain two phase commit state with a log, all
// transfer out logs before this log need to be included in the participants."
// Therefore, we must ensure that the transfer out logs before the writing of
// this two phase commit log will definitely be included in the temporary
// participants(because the transfer out logs are barrier logs), while the
// transfer out logs after that will not be included in the intermediate
// participants(because the transfer out logs block the advancement of the txn's
// state machine before being written to paxos, including both of the in-memory
// state and the persistent state, as explained above).
//
// Hence, with the protection of the blocking capability of the state machine in
// the in-memory state advancement("drive_self_2pc_phase") and the advancement
// of the persistent state machine("submit_log_if_allow"), we can safely proceed
// with the action of merging the intermediate_participant into the participants.
int ObPartTransCtx::merge_intermediate_participants()
{
  int ret = OB_SUCCESS;
  bool exist = false;

  const int64_t participants_size = exec_info_.participants_.count();
  const int64_t increase_size = exec_info_.intermediate_participants_.count();

  if (increase_size > 0) {
    if (participants_size != exec_info_.commit_parts_.count()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "part size not match", KR(ret), KPC(this));
    } else if (OB_FAIL(exec_info_.participants_.reserve(participants_size + increase_size))) {
      TRANS_LOG(WARN, "part reserve failed", KR(ret), KPC(this));
    } else if (OB_FAIL(exec_info_.commit_parts_.reserve(participants_size + increase_size))) {
      TRANS_LOG(WARN, "part reserve failed", KR(ret), KPC(this));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < increase_size; i++) {
      exist = false;
      for (int64_t j = 0; OB_SUCC(ret) && !exist && j < participants_size; j++) {
        if (exec_info_.participants_[j] == exec_info_.intermediate_participants_[i].ls_id_) {
          if (exec_info_.commit_parts_.at(j).ls_id_ != exec_info_.participants_[j]) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "commit part ls_id not match", KR(ret), KPC(this));
          } else if (exec_info_.commit_parts_.at(j).transfer_epoch_ > 0) {
            // do nothing
            // use first transfer_epoch to drive
          } else {
            exec_info_.commit_parts_.at(j).transfer_epoch_ = exec_info_.intermediate_participants_[i].transfer_epoch_;
          }
          exist = true;
        }
      }

      if (OB_SUCC(ret) && !exist) {
        if (OB_FAIL(exec_info_.participants_.push_back(exec_info_.intermediate_participants_[i].ls_id_))) {
          TRANS_LOG(WARN, "fail to push back incremental participants", KR(ret), KPC(this));
        } else if (OB_FAIL(exec_info_.commit_parts_.push_back(exec_info_.intermediate_participants_[i]))) {
          TRANS_LOG(WARN, "fail to push back incremental participants", KR(ret), KPC(this));
        }
      }
    }

    TRANS_LOG(INFO, "merge participant", KR(ret),
                                         K(trans_id_),
                                         K(ls_id_),
                                         KP(this),
                                         K(exec_info_.participants_),
                                         K(exec_info_.intermediate_participants_));
    if (OB_SUCC(ret)) {
      (void)exec_info_.intermediate_participants_.reuse();
    }
  }

  return ret;
}

bool ObPartTransCtx::is_real_upstream_(const ObLSID upstream)
{
  return upstream == exec_info_.upstream_;
}

bool ObPartTransCtx::is_real_upstream()
{
  bool bret = false;

  if (OB_ISNULL(msg_2pc_cache_)) {
    // If msg_2pc_cache is empty, it is called by handle_timeout, and we only
    // need to send to real upstream during handle_timeout.
    bret = true;
  } else {
    bret = is_real_upstream_(msg_2pc_cache_->sender_);
  }

  return bret;
}

int ObPartTransCtx::add_intermediate_participants(const share::ObLSID ls_id, int64_t transfer_epoch)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && !exist && i < exec_info_.intermediate_participants_.count(); i++) {
    if (ls_id == exec_info_.intermediate_participants_[i].ls_id_) {
      exist = true;
    }
  }
  if (OB_SUCC(ret) && !exist) {
    if (OB_FAIL(exec_info_.intermediate_participants_.push_back(ObTxExecPart(ls_id, -1, transfer_epoch)))) {
      TRANS_LOG(WARN, "fail to push back participant into intermediate participants", KR(ret), KPC(this));
    }
  }

  return ret;
}

} // end namespace transaction
} // end namespace oceanbase
