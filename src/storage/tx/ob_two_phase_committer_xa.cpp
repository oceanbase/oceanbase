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

#include "storage/tx/ob_two_phase_committer.h"
// TODO, remove when request type is added
#include "storage/tx/ob_tx_msg.h"

namespace oceanbase
{
using namespace common;
namespace transaction
{
// for participant
// currently, this interface is equal to handle_2pc_prepare_request
int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_redo_request()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (!is_sub2pc()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected operation", K(ret), K(*this));
  } else {
    switch (state) {
      case ObTxState::INIT: {
        if (OB_FAIL(handle_2pc_prepare_redo_request_impl_())) {
          TRANS_LOG(WARN, "handle 2pc prepare request failed", K(ret), K(*this));
        }
        break;
      }
      case ObTxState::REDO_COMPLETE: {
        if (ObTxState::REDO_COMPLETE == get_upstream_state() && all_downstream_collected_()) {
          // no need retransmit downstream msg
        } else {
          if (OB_TMP_FAIL(retransmit_downstream_msg_())) {
            TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
          }
        }
        // if enter into prepare version and not finish, do not response
        if (ObTxState::PREPARE == get_upstream_state()) {
          TRANS_LOG(INFO, "prepare version is not finished", KR(ret));
        } else {
          if (OB_TMP_FAIL(retransmit_upstream_msg_(ObTxState::REDO_COMPLETE))) {
            TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
          }
        }
        break;
      }
      case ObTxState::PREPARE: {
        if (OB_TMP_FAIL(retransmit_downstream_msg_())) {
          TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
        }
        if (OB_TMP_FAIL(retransmit_upstream_msg_(ObTxState::PREPARE))) {
          TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
        } else {
          TRANS_LOG(INFO, "post prepare version response");
        }
        break;
      }
      // TODO, adjust abort
      case ObTxState::ABORT: {
        // Txn may go abort itself, so we need reply the response based on the state
        // to advance the two phase commit protocol as soon as possible
        if (OB_TMP_FAIL(retransmit_downstream_msg_())) {
          TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
        }
        // rewrite ret code
        if (OB_TMP_FAIL(retransmit_upstream_msg_(ObTxState::ABORT))) {
          TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
        }
        break;
      }
      case ObTxState::PRE_COMMIT:
      case ObTxState::COMMIT:
      case ObTxState::CLEAR:
      {
        TRANS_LOG(WARN, "handle orphan request, ignore it", K(ret));
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state");
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_redo_request_impl_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_sub2pc()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected operation", K(ret), K(*this));
  } else if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ))) {
    TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
  } else if (OB_FAIL(drive_self_2pc_phase(ObTxState::REDO_COMPLETE))) {
    TRANS_LOG(WARN, "drive 2pc phase", K(ret), K(*this));
  } else if (is_internal()) {
    if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ))) {
      TRANS_LOG(WARN, "post prepare redo msg failed", KR(tmp_ret), KPC(this));
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_redo_response(const int64_t participant)
{
  int ret = OB_SUCCESS;

  if (!is_sub2pc()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected operation", K(ret), K(*this));
  } else {
    switch (get_upstream_state()) {
      case ObTxState::INIT:
        set_upstream_state(ObTxState::REDO_COMPLETE);
        collected_.reset();
        TRANS_LOG(INFO, "recv prepare redo resp when coord state is init",
                  KR(ret), K(participant), K(*this));
        // go through
      case ObTxState::REDO_COMPLETE: {
        if (OB_FAIL(handle_2pc_prepare_redo_response_impl_(participant))) {
          TRANS_LOG(WARN, "handle 2pc prepare redo response failed", K(ret), K(*this));
        }
        break;
      }
      case ObTxState::PREPARE:
      case ObTxState::PRE_COMMIT:
      case ObTxState::COMMIT:
      case ObTxState::ABORT: {
      // Downstream may lost the request, so we need reply the request based on
      // the state to advance the two phase commit protocol as soon as possible
        if (OB_FAIL(retransmit_downstream_msg_(participant))) {
          TRANS_LOG(WARN, "retransmit msg failed", K(ret), K(*this));
        }
        break;
      }
      case ObTxState::CLEAR: {
        TRANS_LOG(WARN, "handle orphan prepare response, ignore it", K(ret), K(*this), K(participant));
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", K(*this));
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_redo_response_impl_(const int64_t participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (!is_sub2pc()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected operation", K(ret), K(*this));
  } else if (OB_FAIL(collect_downstream_(participant))) {
    TRANS_LOG(ERROR, "add participant to collected list failed", K(participant));
  } else if (is_2pc_logging()) {
    // skip if during logging
  } else if ((ObTxState::REDO_COMPLETE == state)
             && all_downstream_collected_()) {
    if (is_root()) {
      if (OB_TMP_FAIL(reply_to_scheduler_for_sub2pc(SUBPREPARE_RESP))) {
        TRANS_LOG(ERROR, "fail to do sub prepare", K(ret), K(*this));
      }
    } else {
      if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP,
                               OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post prepare response failed", KR(tmp_ret), K(*this));
      }
    }
  }
  TRANS_LOG(INFO, "handle 2pc prepare redo response", K(ret), K(*this));

  return ret;
}

// this interface is only used for sub2pc trans (xa trans)
int ObTxCycleTwoPhaseCommitter::apply_commit_info_log()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (!is_sub2pc()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected operation", K(ret), K(*this));
  } else if (ObTxState::ABORT == state) {
    // We need handle the corner case that receive abort response before prepare
    // succesfully synchronized. So we should skip the prepare state transition
    // and use abort to continue the state machine
    TRANS_LOG(INFO, "receive abort response during paxosing prepare", K(ret), K(*this), K(state));
  // TODO,
  //} else if (ObTxState::INIT != state) {
  //  TRANS_LOG(ERROR, "apply invalid log", K(ret), K(*this), K(state));
  //} else if (OB_FAIL(set_2pc_state(ObTxState::REDO_COMPLETE))) {
  //  TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else {
    if ((is_internal() || is_leaf())
        && all_downstream_collected_()) {
      if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP,
                               OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post prepare redo response failed", K(tmp_ret), K(*this));
      }
    }
    if (is_root()
        && all_downstream_collected_()) {
      if (OB_TMP_FAIL(reply_to_scheduler_for_sub2pc(SUBPREPARE_RESP))) {
        TRANS_LOG(ERROR, "fail to response scheduler for sub prepare", K(tmp_ret), K(*this));
      }
    }
    TRANS_LOG(INFO, "apply commit info log for sub trans", K(ret));
  }

  return OB_SUCCESS;
}

// for coordinator
// only persist redo and commit info
// this interface is used to handle sub prepare (xa prepare)
int ObTxCycleTwoPhaseCommitter::prepare_redo()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::INIT != get_upstream_state()) {
    TRANS_LOG(INFO, "already enter two phase commit", K(ret), K(*this));
  } else if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else {
    set_upstream_state(ObTxState::REDO_COMPLETE);
    collected_.reset();
    if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ))) {
      TRANS_LOG(WARN, "post prepare request failed", K(tmp_ret), K(*this));
    }

    // TODO, only submit commit info
    if (OB_TMP_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT_INFO))) {
      if (OB_BLOCK_FROZEN == tmp_ret) {
        // memtable is freezing, can not submit log right now.
      } else {
        TRANS_LOG(WARN, "submit prepare log failed", K(tmp_ret), K(*this));
      }
    }
  }
  TRANS_LOG(INFO, "prepare redo", K(ret), K(*this));

  return ret;
}

// for coordinator
// this interface is used for sub commit/rollback (two phase xa commit/rollback)
// if already in second phase, return success
int ObTxCycleTwoPhaseCommitter::continue_execution(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();
  if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (ObTxState::REDO_COMPLETE < get_upstream_state()
      || ObTxState::REDO_COMPLETE < get_downstream_state()) {
    TRANS_LOG(INFO, "already in second phase", K(ret), K(*this));
  } else {
    if (is_rollback) {
      if (OB_FAIL(drive_self_2pc_phase(ObTxState::ABORT))) {
        TRANS_LOG(WARN, "do abort failed", K(ret));
      } else if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ))) {
        TRANS_LOG(WARN, "post abort request failed", K(tmp_ret), KPC(this));
      }
    } else {
      if (OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
        TRANS_LOG(WARN, "do prepare failed", K(ret));
      } else if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ))) {
        TRANS_LOG(WARN, "post prepare request failed", K(tmp_ret), KPC(this));
      }
    }
  }

  return ret;
}

} // end namespace transaction
} // end namespace oceanbase
