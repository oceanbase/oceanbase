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

namespace oceanbase
{
using namespace common;
namespace transaction
{

int ObTxCycleTwoPhaseCommitter::handle_2pc_req(const ObTwoPhaseCommitMsgType msg_type)
{
  int ret = OB_SUCCESS;

  switch (msg_type) {
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ:
    ret = handle_2pc_prepare_redo_request();
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ:
    ret = handle_2pc_prepare_request();
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ:
    ret = handle_2pc_pre_commit_request();
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ:
    ret = handle_2pc_commit_request();
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ:
    ret = handle_2pc_abort_request();
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ:
    ret = handle_2pc_clear_request();
    break;
  default:
    TRANS_LOG(ERROR, "invalid msg type", K(msg_type));
    ret = OB_TRANS_INVALID_STATE;
    break;
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_req(const ObTwoPhaseCommitMsgType recv_msg_type,
                                                      ObTwoPhaseCommitMsgType& send_msg_type,
                                                      bool& need_ack)
{
  int ret = OB_SUCCESS;

  switch (recv_msg_type) {
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ:
    ret = handle_orphan_2pc_prepare_redo_request(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ:
    ret = handle_orphan_2pc_prepare_request(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ:
    ret = handle_orphan_2pc_pre_commit_request(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ:
    ret = handle_orphan_2pc_commit_request(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ:
    ret = handle_orphan_2pc_abort_request(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ:
    ret = handle_orphan_2pc_clear_request();
    need_ack = false;
    break;
  default:
    TRANS_LOG(ERROR, "invalid msg type", K(recv_msg_type));
    ret = OB_TRANS_INVALID_STATE;
    break;
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::apply_log(const ObTwoPhaseCommitLogType log_type)
{
  int ret = OB_SUCCESS;

  switch (log_type) {
  case ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT_INFO:
    // NOTE that this case is only used for xa trans
    ret = apply_commit_info_log();
    break;
  case ObTwoPhaseCommitLogType::OB_LOG_TX_PREPARE:
    ret = apply_prepare_log();
    break;
  case ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT:
    ret = apply_commit_log();
    break;
  case ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT:
    ret = apply_abort_log();
    break;
  case ObTwoPhaseCommitLogType::OB_LOG_TX_CLEAR:
    ret = apply_clear_log();
    break;
  default:
    TRANS_LOG(ERROR, "invalid log type", K(log_type));
    ret = OB_TRANS_INVALID_STATE;
    break;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "apply log failed", K(ret), K(*this), K(log_type));
    ob_abort();
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::replay_log(const ObTwoPhaseCommitLogType log_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(merge_intermediate_participants())) {
    TRANS_LOG(WARN, "fail to merge incremental participants", KPC(this));
  } else {
    switch (log_type) {
    case ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT_INFO:
      ret = replay_commit_info_log();
      break;
    case ObTwoPhaseCommitLogType::OB_LOG_TX_PREPARE:
      ret = replay_prepare_log();
      break;
    case ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT:
      ret = replay_commit_log();
      break;
    case ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT:
      ret = replay_abort_log();
      break;
    case ObTwoPhaseCommitLogType::OB_LOG_TX_CLEAR:
      ret = replay_clear_log();
      break;
    default:
      TRANS_LOG(ERROR, "invalid log type", K(log_type));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay log failed", K(ret), KPC(this), K(log_type));
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_reboot()
{
  return OB_NOT_SUPPORTED;
}

int ObTxCycleTwoPhaseCommitter::leader_takeover()
{
  return OB_NOT_SUPPORTED;
}

int ObTxCycleTwoPhaseCommitter::leader_revoke()
{
  return OB_NOT_SUPPORTED;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_request()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  switch (state) {
  case ObTxState::INIT:
  case ObTxState::REDO_COMPLETE: {
    if (OB_FAIL(handle_2pc_prepare_request_impl_())) {
      TRANS_LOG(WARN, "handle 2pc prepare request failed", K(ret), K(*this));
    }
    break;
  }
  case ObTxState::PREPARE: {
    if (OB_TMP_FAIL(retransmit_downstream_msg_())) {
      TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(retransmit_upstream_msg_(ObTxState::PREPARE))) {
      TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
    }
    break;
  }
  case ObTxState::ABORT: {
    // NB: we must apply msg cache here because the retransmit needs the
    // upstream_ from it to remind itself that it is during the 2pc.
    if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ))) {
      TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
    } else {
      // Txn may go abort itself, so we need reply the response based on the state
      // to advance the two phase commit protocol as soon as possible
      if (OB_TMP_FAIL(retransmit_downstream_msg_())) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
      }
      if (OB_TMP_FAIL(retransmit_upstream_msg_(ObTxState::ABORT))) {
        TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
      }
    }
    break;
  }
  case ObTxState::PRE_COMMIT:
  case ObTxState::COMMIT: {
    if (OB_TMP_FAIL(retransmit_downstream_msg_())) {
      TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(retransmit_upstream_msg_(ObTxState::PREPARE))) {
      TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
    }
    break;
  }
  case ObTxState::CLEAR: {
    TRANS_LOG(WARN, "handle orphan request, ignore it", K(ret));
    break;
  }
  default: {
    TRANS_LOG(WARN, "invalid 2pc state");
    ret = OB_TRANS_INVALID_STATE;
    break;
  }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::retransmit_upstream_msg_(const ObTxState state)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTwoPhaseCommitMsgType msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_UNKNOWN;
  bool need_respond = false;

  if (get_downstream_state() > get_upstream_state()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Invalid downstream_state", K(ret), KPC(this));
  } else {
    switch (get_2pc_role()) {
      // root do not respond
    case Ob2PCRole::ROOT: {
      if (!is_real_upstream()) {
        // It may be the case that the ROOT is the downstream of the fake
        // upstream and need to respond with the fake upstream
        need_respond = true;
      } else {
        need_respond = false;
      }
      break;
    }
    case Ob2PCRole::INTERNAL: {
      // need respond if all downstreams has responded and submit log succesfully
      need_respond = ((all_downstream_collected_()
                       // need respond if it is not the real upstream and we
                       // should response just after the downstream state has
                       // been synced
                       || !is_real_upstream())
                      && get_downstream_state() == state)
                     // dowstream_state <= upstream_state
                     // => state < downstream_state && state < upstream_state
                     // => post response for last phase
                     || (get_downstream_state() > state);
      break;
    }
    case Ob2PCRole::LEAF: {
      // leaf need respond
      need_respond = get_downstream_state() >= state;
      break;
    }
    default: {
      ret = OB_TRANS_INVALID_STATE;
      TRANS_LOG(WARN, "invalid coord state", KR(ret), K(get_upstream_state()));
      break;
    }
    }

    if (need_respond == true) {
      switch (state) {
      case ObTxState::INIT: {
        // It may happen when participant failed to submit the log
        need_respond = false;
        break;
      }
      case ObTxState::REDO_COMPLETE: {
        if (is_sub2pc()) {
          // if xa trans, prepare redo response is required
          msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP;
        } else {
          need_respond = false;
        }
        break;
      }
      case ObTxState::PREPARE: {
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP;
        break;
      }
      case ObTxState::PRE_COMMIT: {
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP;
        break;
      }
      case ObTxState::COMMIT: {
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP;
        break;
      }
      case ObTxState::ABORT: {
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
        break;
      }
      case ObTxState::CLEAR: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", K(*this));
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
      }
    }
  }

  if (OB_SUCC(ret) && need_respond) {
    if (OB_TMP_FAIL(post_msg(msg_type, OB_C2PC_SENDER_ID))) {
      TRANS_LOG(WARN, "post msg failed", K(tmp_ret), K(msg_type), K(*this));
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_request_impl_() {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ))) {
    TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
  } else if (OB_FAIL(drive_self_2pc_phase(ObTxState::PREPARE))) {
    TRANS_LOG(WARN, "do prepare failed", K(ret), K(*this));
  } else {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "The root should not receive prepare request", K(ret), KPC(this));
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ))) {
        TRANS_LOG(WARN, "post prepare msg failed", KR(ret));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      // do nothing
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected 2pc role", K(ret), K(get_2pc_role()), KPC(this));
      break;
    }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_commit_request()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  switch (state) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc commit request find protocol error", KPC(this));
      break;
    }
    case ObTxState::PREPARE:
    case ObTxState::PRE_COMMIT: {
      if (OB_FAIL(handle_2pc_commit_request_impl_())) {
        TRANS_LOG(WARN, "handle 2pc commit request failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::COMMIT: {
      if (OB_FAIL(retransmit_downstream_msg_())) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret));
      }
      if (OB_FAIL(retransmit_upstream_msg_(ObTxState::COMMIT))) {
        TRANS_LOG(WARN, "retransmit upstream msg failed", KR(ret));
      }
      break;
    }
    case ObTxState::ABORT: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc commit request find protocol error", K(*this));
    }
    case ObTxState::CLEAR: {
      TRANS_LOG(WARN, "handle orphan request failed, ignore it", K(ret), K(*this));
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state");
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_commit_request_impl_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ))) {
    TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
  } else if (OB_FAIL(drive_self_2pc_phase(ObTxState::COMMIT))) {
    TRANS_LOG(WARN, "enter commit phase failed", K(ret));
  } else {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "The root should not receive commit request", K(ret), KPC(this));
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ))) {
        TRANS_LOG(WARN, "post downstream msg failed", K(tmp_ret));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      // do nothing
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected 2pc role", K(ret), K(get_2pc_role()), KPC(this));
    }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_abort_request()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  switch (state) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE:
    case ObTxState::PREPARE: {
      if (OB_FAIL(handle_2pc_abort_request_impl_())) {
        TRANS_LOG(WARN, "handle 2pc abort request failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::PRE_COMMIT: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc abort request find protocol error", K(*this));
      break;
    }
    case ObTxState::COMMIT: {
      // It may be the case, the coordinator has finished the 2pc and exits.
      // While one of the participants donot receive clear request and is still
      // in commit state. If at this time, the coordinator receives a duplicated
      // prepare response, it will send abort according to PRESUME ABORT. And
      // the participant will soon receive abort request during commit state. We
      // can simply skip the request, and it will not cause any problem.
      TRANS_LOG(WARN, "handle 2pc abort request during commit may be possible", K(*this));
      break;
    }
    case ObTxState::ABORT: {
      // NB: we must apply msg cache here because the retransmit needs the
      // upstream_ from it to remind itself that it is during the 2pc.
      if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ))) {
        TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
      } else {
        // Downstream may lost the response, so we need reply the response based on
        // the state to advance the two phase commit protocol as soon as possible
        if (OB_FAIL(retransmit_downstream_msg_())) {
          TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret));
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(retransmit_upstream_msg_(ObTxState::ABORT))) {
          TRANS_LOG(WARN, "retransmit upstream msg failed", KR(ret));
        }
      }
      break;
    }
    case ObTxState::CLEAR: {
      TRANS_LOG(WARN, "handle orphan request failed, ignore it", K(ret), K(*this));
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state");
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_abort_request_impl_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ))) {
    TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
  } else if (OB_FAIL(drive_self_2pc_phase(ObTxState::ABORT))) {
    TRANS_LOG(WARN, "enter abort phase failed", K(ret), K(*this));
  } else {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "The root should not receive 2pc abort request", K(ret), KPC(this));
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ))) {
        TRANS_LOG(WARN, "post abort msg failed", KR(ret));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      // do nothing
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected 2pc role", K(ret), K(get_2pc_role()), KPC(this));
      break;
    }
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_pre_commit_request()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  switch (state) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc pre commit request find protocol error", K(*this));
      break;
    }
    case ObTxState::PREPARE: {
      if (OB_FAIL(handle_2pc_pre_commit_request_impl_())) {
        TRANS_LOG(WARN, "handle 2pc pre commit request failed", K(ret), K(*this));
      } else {
        // do nothing
      }
      break;
    }
    case ObTxState::PRE_COMMIT:
    case ObTxState::COMMIT: {
      if (OB_FAIL(retransmit_downstream_msg_())) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret));
      }
      if (OB_FAIL(retransmit_upstream_msg_(ObTxState::PRE_COMMIT))) {
        TRANS_LOG(WARN, "retransmit upstream msg failed", KR(ret));
      }
      break;
    }
    case ObTxState::ABORT: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc pre commit request find protocol error", K(*this));
      break;
    }
    case ObTxState::CLEAR: {
      TRANS_LOG(WARN, "handle orphan request failed, ignore it", K(ret), K(*this));
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state", K(*this));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_pre_commit_request_impl_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ))) {
    TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
  } else if (OB_FAIL(try_enter_pre_commit_state())) {
    TRANS_LOG(WARN, "try_enter_pre_commit_state failed", K(ret), KPC(this));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_clear_request()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  switch (state) {
    case ObTxState::INIT: {
      // There may be the case of transfer that you have already stay in the
      // init phase and fail to pass the epoch check in the transfer. So you
      // will send the abort response back to the upstream with init state and
      // the upstream will post the abort request w/o youself and then move to
      // the clear and post the clear request to you
      break;
    }
    case ObTxState::PREPARE:
    case ObTxState::PRE_COMMIT: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc clear request find protocol error", K(*this));
      break;
    }
    case ObTxState::COMMIT:
    case ObTxState::ABORT: {
      if (OB_FAIL(handle_2pc_clear_request_impl_())) {
        TRANS_LOG(WARN, "handle 2pc clear request failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::CLEAR: {
      TRANS_LOG(WARN, "handle orphan request failed, ignore it", K(ret), K(*this));
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state");
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_clear_request_impl_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (OB_FAIL(apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ))) {
    TRANS_LOG(WARN, "apply msg failed", K(ret), KPC(this));
  } else if (OB_FAIL(drive_self_2pc_phase(ObTxState::CLEAR))) {
    TRANS_LOG(WARN, "enter clear phase failed", K(ret), K(*this));
  } else {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "The root should not receive clear request", K(ret), KPC(this));
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ))) {
        TRANS_LOG(WARN, "post clear msg failed", KR(ret));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      // do nothing
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected 2pc role", K(ret), K(get_2pc_role()), KPC(this));
      break;
    }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_prepare_redo_request(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the downstream lose the memory, we response with the abort according
  // to presume abort. Even the downstream commits the txn and forgets the
  // memory, the orphan prepare must be old msg and the response will be rejected by the upstream,
  // otherwise the upstream must not send the prepare request(coordinator must
  // enter commit state before downstream forgets its memory)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_prepare_request(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the downstream lose the memory, we response with the abort according
  // to presume abort. Even the downstream commits the txn and forgets the
  // memory, the orphan prepare must be old msg and the response will be rejected by the upstream,
  // otherwise the upstream must not send the prepare request(coordinator must
  // enter commit state before downstream forgets its memory)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_pre_commit_request(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the downstream lose the memory, we response with the abort according
  // to presume abort. Even the downstream commits the txn and forgets the
  // memory, the orphan commit may be old msg or ack request and the response
  // will be reasonably adopted by the upstream (participants may enter commit
  // state and forget all before upstream forgets its memory. And after upstream
  // reboots, it will restart to collect the commit response)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_commit_request(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the downstream lose the memory, we response with the abort according
  // to presume abort. Even the downstream commits the txn and forgets the
  // memory, the orphan commit may be old msg or ack request and the response
  // will be reasonably adopted by the upstream (participants may enter commit
  // state and forget all before upstream forgets its memory. And after upstream
  // reboots, it will restart to collect the commit response)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_abort_request(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // It may be the case that participants may enter abort state and forget all
  // before upstream forgets its memory. And after upstream reboots, it will
  // restart to collect the abort response.
  //
  // TODO(handora.qc): abort process can be optimized by getting rid of the
  // clear phase
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_clear_request()
{
  return OB_SUCCESS;
}

int ObTxCycleTwoPhaseCommitter::apply_prepare_log()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();
  const ObTxState upstream_state = get_upstream_state();

  if (ObTxState::ABORT == upstream_state) {
    // We need handle the corner case that receive abort response before prepare
    // succesfully synchronized. So we should skip the prepare state transition
    // and use abort to continue the state machine
    TRANS_LOG(INFO, "receive abort response during paxosing prepare", K(ret), K(*this));
  } else if (ObTxState::INIT != state && ObTxState::REDO_COMPLETE != state) {
    ret  = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply invalid log", K(ret), K(*this), K(state));
  } else if (ObTxState::PREPARE != upstream_state) {
    ret  = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply invalid log", K(ret), K(*this), K(upstream_state));
  } else if (OB_FAIL(on_prepare())) {
    TRANS_LOG(ERROR, "on prepare failed", K(ret), K(*this), K(state));
  } else if (OB_FAIL(set_downstream_state(ObTxState::PREPARE))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (all_downstream_collected_()) {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      if (OB_FAIL(try_enter_pre_commit_state())) {
        TRANS_LOG(WARN, "try enter pre_commit state failed", K(ret), KPC(this));
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          // retry by gts callback or handle_timout
        }
      }
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post prepare response failed", K(ret), K(*this));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post prepare response failed", K(ret), K(*this));
      }
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

int ObTxCycleTwoPhaseCommitter::apply_commit_log()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();
  const ObTxState upstream_state = get_upstream_state();

  if (ObTxState::PREPARE != state && ObTxState::PRE_COMMIT != state && ObTxState::COMMIT != state) {
    // We will never apply commit under abort and clear state
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply commit with wrong state", K(state), K(*this));
  } else if (ObTxState::COMMIT != upstream_state) {
    ret  = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply invalid log", K(ret), K(*this), K(upstream_state));
  } else if (OB_FAIL(on_commit())) {
    TRANS_LOG(ERROR, "on commit failed", K(ret), K(*this), K(state));
  } else if (OB_FAIL(set_downstream_state(ObTxState::COMMIT))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (all_downstream_collected_()) {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::CLEAR))) {
        TRANS_LOG(WARN, "enter into clear phase failed", K(ret), KPC(this));
      } else if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ))) {
        TRANS_LOG(WARN, "post downstream msg failed", K(tmp_ret));
      }
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post commit response failed", K(ret), K(*this));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post commit response failed", K(ret), K(*this));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected 2pc role", K(ret), K(get_2pc_role()), KPC(this));
      break;
    }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::apply_abort_log()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();
  const ObTxState upstream_state = get_upstream_state();

  if (ObTxState::ABORT != upstream_state) {
    TRANS_LOG(WARN, "meet tx whose upstrean state is not abort", K(ret), KPC(this));
  }

  if (ObTxState::INIT != state
      && ObTxState::REDO_COMPLETE != state
      && ObTxState::PREPARE != state) {
    // We will never apply abort under commit and clear state
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply abort with wrong state", K(state));
  // } else if (ObTxState::ABORT != upstream_state) {
  //   ret  = OB_TRANS_INVALID_STATE;
  //   TRANS_LOG(ERROR, "apply invalid log", K(ret), K(*this), K(upstream_state));
  } else if (OB_FAIL(on_abort())) {
    TRANS_LOG(ERROR, "on abort failed", K(ret), K(*this), K(state));
  } else if (OB_FAIL(set_downstream_state(ObTxState::ABORT))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (all_downstream_collected_()) {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::CLEAR))) {
        TRANS_LOG(WARN, "enter into clear phase failed", K(ret), KPC(this));
      } else if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ))) {
        TRANS_LOG(WARN, "post clear request failed", K(tmp_ret), K(*this));
      }
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post abort response failed", K(tmp_ret), K(*this));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post abort response failed", K(tmp_ret), K(*this));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected 2pc role", K(ret), K(get_2pc_role()), KPC(this));
      break;
    }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::apply_clear_log()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();
  const ObTxState upstream_state = get_upstream_state();

  if (ObTxState::COMMIT != state
      && ObTxState::ABORT != state) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply clear with wrong state", K(state));
  } else if (ObTxState::CLEAR != upstream_state) {
    ret  = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "apply invalid log", K(ret), K(*this), K(upstream_state));
  } else if (OB_FAIL(on_clear())) {
    TRANS_LOG(ERROR, "on clear failed", K(ret), K(*this), K(state));
  } else if (OB_FAIL(set_downstream_state(ObTxState::CLEAR))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::replay_prepare_log()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::PREPARE == state) {
    ret = OB_SUCCESS;
    set_upstream_state(ObTxState::PREPARE);
    collected_.reset();
    TRANS_LOG(INFO, "already in prepare state", K(ret), KPC(this), K(state));
  } else if (ObTxState::INIT != state
             && ObTxState::REDO_COMPLETE != state
             && ObTxState::PRE_COMMIT != state) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "replay invalid log", K(ret), K(*this), K(state));
  } else if (FALSE_IT(set_upstream_state(ObTxState::PREPARE))) {
  } else if (OB_FAIL(set_downstream_state(ObTxState::PREPARE))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (FALSE_IT(collected_.reset())) {
  } else {
    TRANS_LOG(DEBUG, "replay prepare log success", K(ret), KPC(this), K(state));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::replay_commit_info_log()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::REDO_COMPLETE == state) {
    ret = OB_SUCCESS;
    set_upstream_state(ObTxState::REDO_COMPLETE);
    collected_.reset();
    TRANS_LOG(DEBUG, "already in redo complete state", K(ret), KPC(this), K(state));
  } else if (ObTxState::INIT != state
             && ObTxState::PREPARE != state
             && ObTxState::PRE_COMMIT != state) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "replay invalid log", K(ret), K(*this), K(state));
  } else if (FALSE_IT(set_upstream_state(ObTxState::REDO_COMPLETE))) {
  } else if (OB_FAIL(set_downstream_state(ObTxState::REDO_COMPLETE))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (FALSE_IT(collected_.reset())) {
  } else {
    TRANS_LOG(DEBUG, "replay commit info log success", K(ret), KPC(this), K(state));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::replay_commit_log()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::COMMIT == state) {
    ret = OB_SUCCESS;
    set_upstream_state(ObTxState::COMMIT);
    collected_.reset();
    TRANS_LOG(INFO, "already in commit state", K(ret), KPC(this), K(state));
  } else if (ObTxState::PREPARE != state
      && ObTxState::PRE_COMMIT != state) {
    // We will never replay commit under abort and clear state
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "replay commit with wrong state", K(state), K(*this));
  } else if (FALSE_IT(set_upstream_state(ObTxState::COMMIT))) {
  } else if (OB_FAIL(set_downstream_state(ObTxState::COMMIT))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (FALSE_IT(collected_.reset())) {
    TRANS_LOG(ERROR, "add member failed", K(ret), K(*this), K(state));
  } else {
    TRANS_LOG(DEBUG, "replay commit log success", K(ret), KPC(this), K(state));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::replay_abort_log()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::ABORT == state) {
    ret = OB_SUCCESS;
    set_upstream_state(ObTxState::ABORT);
    collected_.reset();
    TRANS_LOG(INFO, "already in abort state", K(ret), KPC(this), K(state));
  } else if (ObTxState::INIT != state
      && ObTxState::REDO_COMPLETE != state
      && ObTxState::PREPARE != state) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "replay abort with wrong state", K(state));
  } else if (FALSE_IT(set_upstream_state(ObTxState::ABORT))) {
  } else if (OB_FAIL(set_downstream_state(ObTxState::ABORT))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  } else if (FALSE_IT(collected_.reset())) {
    TRANS_LOG(ERROR, "add member failed", K(ret), K(*this), K(state));
  } else {
    TRANS_LOG(DEBUG, "replay abort log success", K(ret), KPC(this), K(state));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::replay_clear_log()
{
  int ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::CLEAR == state) {
    ret = OB_SUCCESS;
    set_upstream_state(ObTxState::CLEAR);
    TRANS_LOG(INFO, "already in clear state", K(ret), KPC(this), K(state));
  } else if (ObTxState::COMMIT != state
      && ObTxState::ABORT != state) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(ERROR, "replay clear with wrong state", K(state));
  } else if (FALSE_IT(set_upstream_state(ObTxState::CLEAR))) {
  } else if (OB_FAIL(set_downstream_state(ObTxState::CLEAR))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(ret), K(*this), K(state));
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::recover_from_tx_table()
{
  int ret = OB_SUCCESS;

  const ObTxState state = get_downstream_state();
  set_upstream_state(state);
  collected_.reset();

  return ret;
}

int ObTxCycleTwoPhaseCommitter::try_enter_pre_commit_state()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (is_2pc_logging()) {
    ret = OB_EAGAIN;
    TRANS_LOG(INFO, "committer is 2pc logging", KPC(this));
  } else if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::PRE_COMMIT))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "drive self 2pc pre_commit phase failed", K(ret), KPC(this));
    }
  } else if (OB_FAIL(on_pre_commit())) {
    TRANS_LOG(WARN, "enter_pre_commit_state failed", K(ret), KPC(this));
  } else {
    TRANS_LOG(DEBUG, "enter_pre_commit_state succ", K(ret), KPC(this));
  }
  TRANS_LOG(TRACE, "try enter pre commit state", K(ret), KPC(this));
  return ret;
}

int ObTxCycleTwoPhaseCommitter::on_pre_commit()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(set_downstream_state(ObTxState::PRE_COMMIT))) {
    TRANS_LOG(ERROR, "set 2pc state failed", K(*this));
  } else {
    switch (get_2pc_role()) {
    case Ob2PCRole::ROOT: {
      const int64_t SINGLE_COUNT = 1;
      if (SINGLE_COUNT == get_downstream_size()) {
        TRANS_LOG(INFO, "only one participant, skip pre commit", KPC(this));
        // TODO, currently, if a trans only has one participant,
        // the state can not be drived from pre commit to commit.
        // Therefore, enter commit state directly.
        if (OB_TMP_FAIL(drive_self_2pc_phase(ObTxState::COMMIT))) {
          TRANS_LOG(WARN, "do commit in memory failed", K(ret), KPC(this));
        }
        // not need post downstream msg
      } else {
        if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ))) {
          TRANS_LOG(WARN, "post pre commit msg failed", KR(ret));
        }
      }
      break;
    }
    case Ob2PCRole::INTERNAL: {
      if (OB_TMP_FAIL(post_downstream_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ))) {
        TRANS_LOG(WARN, "post pre commit msg failed", KR(ret));
      }
      break;
    }
    case Ob2PCRole::LEAF: {
      if (OB_TMP_FAIL(
              post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post pre commit response failed", K(ret), K(*this));
      }
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

int ObTxCycleTwoPhaseCommitter::submit_2pc_log_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_submit = false;
  ObTwoPhaseCommitLogType log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_INIT;

  if (OB_FAIL(decide_2pc_log_type_(need_submit, log_type))) {
    TRANS_LOG(WARN, "decide downstream msg type fail", K(ret), KPC(this));
  } else if (need_submit && OB_TMP_FAIL(submit_log(log_type))) {
    TRANS_LOG(WARN, "submit log failed", KR(tmp_ret), K(log_type));
  } else if (need_submit) {
    // TRANS_LOG(INFO, "resubmit 2pc log succeed", KR(tmp_ret), K(log_type), KPC(this));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::decide_2pc_log_type_(bool &need_submit,
                                                            ObTwoPhaseCommitLogType &log_type)
{
  int ret = OB_SUCCESS;
  need_submit = false;
  const ObTxState upstream_state = get_upstream_state();
  const ObTxState downstream_state = get_downstream_state();

  if (is_2pc_logging()) {
    // Case 1: Committeer is during logging, so we donot need submit again
    need_submit = false;
  } else if (upstream_state > downstream_state) {
    switch (upstream_state) {
    case ObTxState::INIT: {
      need_submit = false;
      break;
    }
    case ObTxState::REDO_COMPLETE: {
      need_submit = true;
      log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT_INFO;
      break;
    }
    case ObTxState::PREPARE: {
      //TODO dup_table can not submit prepare log before redo sync finished
      need_submit = true;
      log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_PREPARE;
      break;
    }
    case ObTxState::PRE_COMMIT: {
      need_submit = false;
      break;
    }
    case ObTxState::COMMIT: {
      if (ObTxState::PREPARE == downstream_state
          || ObTxState::PRE_COMMIT == downstream_state) {
        need_submit = true;
        log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT;
      } else {
        ret = OB_TRANS_INVALID_STATE;
        TRANS_LOG(ERROR, "invalid 2pc state", KR(ret), KPC(this));
      }
      break;
    }
    case ObTxState::ABORT: {
      if (ObTxState::COMMIT != downstream_state) {
        need_submit = true;
        log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT;
      } else {
        ret = OB_TRANS_INVALID_STATE;
        TRANS_LOG(ERROR, "invalid 2pc state", KR(ret), KPC(this));
      }
      break;
    }
    case ObTxState::CLEAR: {
      if (ObTxState::COMMIT == downstream_state
          || ObTxState::ABORT == downstream_state) {
        need_submit = true;
        log_type = ObTwoPhaseCommitLogType::OB_LOG_TX_CLEAR;
      } else {
        ret = OB_TRANS_INVALID_STATE;
        TRANS_LOG(ERROR, "invalid 2pc state", KR(ret), KPC(this));
      }
      break;
    }
    default:
      ret = OB_TRANS_INVALID_STATE;
      TRANS_LOG(WARN, "invalid coord state", KR(ret), K(get_upstream_state()));
      break;
    }
  }

  return ret;
}

bool is_2pc_request_msg(const ObTwoPhaseCommitMsgType msg_type)
{
  bool bret = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ == msg_type;
  return bret;
}

} // end namespace transaction
} // end namespace oceanbase
