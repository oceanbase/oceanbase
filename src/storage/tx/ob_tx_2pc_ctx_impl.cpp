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

namespace oceanbase
{
using namespace common;
namespace transaction
{

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

  // common operation
  if (exec_info_.is_dup_tx_ || OB_SUCC(search_unsubmitted_dup_table_redo_())) {
    no_need_submit_log = true;
    if (OB_FAIL(dup_table_tx_redo_sync_())) {
      TRANS_LOG(WARN, "dup table tx  redo sync failed", K(ret));
    }
  } else if (OB_FAIL(generate_prepare_version_())) {
    TRANS_LOG(WARN, "generate prepare version failed", K(ret), K(*this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_mul_data_source_tx_end_(true))) {
      TRANS_LOG(WARN, "trans commit need retry", K(ret), K(trans_id_), K(ls_id_));
    }
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
  int64_t gts = 0;
  int64_t local_max_read_version = 0;

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
      } else if (OB_FAIL(ctx_tx_data_.set_commit_version(std::max(gts, local_max_read_version)))) {
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
      check_and_response_scheduler_(OB_SUCCESS);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(restart_2pc_trans_timer_())) {
    TRANS_LOG(ERROR, "restart_2pc_trans_timer_ error", KR(ret), KPC(this));
  }
  return ret;
}

int ObPartTransCtx::check_and_response_scheduler_(int result)
{
  int ret = OB_SUCCESS;
  ret = E(EventTable::EN_EARLY_RESPONSE_SCHEDULER) OB_SUCCESS;
  if (!is_sub2pc() && OB_FAIL(ret)) {
    // when error inject, response scheduler delayed to CLEAR state
    if (ObTxState::CLEAR == get_upstream_state()) {
      TRANS_LOG(INFO, "response scheduler in clear state", K(ret), K(*this));
      ret = OB_SUCCESS;
    } else {
      return OB_SUCCESS;
    }
  } else {
    // general path, won't response scheduler in CLEAR state
    if (ObTxState::CLEAR == get_upstream_state()) {
      return OB_SUCCESS;
    }
  }

  if (is_sub2pc()) {
    // TODO, according to part trans action
    if (ObTxState::COMMIT == get_upstream_state()) {
      if (OB_FAIL(reply_to_scheduler_for_sub2pc(SUBCOMMIT_RESP))) {
        TRANS_LOG(ERROR, "fail to reply sub commit", K(ret), K(*this));
      }
    } else if (ObTxState::ABORT == get_upstream_state() || ObTxState::ABORT == get_downstream_state()) {
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
  return ret;
}


int ObPartTransCtx::update_local_max_commit_version_(const int64_t &commit_version)
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
      check_and_response_scheduler_(OB_TRANS_KILLED);
    }
  }

  return ret;
}

int ObPartTransCtx::on_abort()
{
  int ret = OB_SUCCESS;

  if (is_sub2pc() && is_root()) {
    check_and_response_scheduler_(OB_TRANS_KILLED);
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
    check_and_response_scheduler_(OB_SUCCESS);
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

bool ObPartTransCtx::need_request_gts_()
{
  bool bret = false;

  if (!is_leaf()) {
    if (0 != exec_info_.participants_.count()
        && ls_id_ == exec_info_.participants_[0]) {
      bret = true;
    } else {
      bret = false;
    }
  }
  return bret;
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

} // end namespace transaction
} // end namespace oceanbase
