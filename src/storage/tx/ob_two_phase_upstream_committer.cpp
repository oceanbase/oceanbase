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

/*
 * two_phase_commit - start two phase commit
 *
 * Failure handle:
 * - post msg fail
 * - submit log fail
 * these two type of failure will ignored as success
 * and retry by handle_timeout.
 */
int ObTxCycleTwoPhaseCommitter::two_phase_commit()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();
  bool no_need_submit_log = false;

  //start 2pc from root
  if (ObTxState::PREPARE <= get_upstream_state()) {
    TRANS_LOG(INFO, "already enter two phase commit", K(ret), K(*this));
  } else if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (OB_FAIL(do_prepare(no_need_submit_log))) {
    TRANS_LOG(WARN, "do prepare failed", K(ret), K(*this));
  } else {
    set_upstream_state(ObTxState::PREPARE);
    collected_.reset();
    if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ))) {
      TRANS_LOG(WARN, "post prepare request failed, will retry later", K(tmp_ret), KPC(this));
    }

    if (no_need_submit_log) {
    } else if (OB_TMP_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_PREPARE))) {
      /* submit prepare log fail is harmless, the 2pc will retry */
      TRANS_LOG(WARN, "submit prepare log failed, will retry by prepare message", K(tmp_ret), KPC(this));
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::two_phase_abort()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::INIT != state) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "abort when in 2pc", K(ret), K(*this));
  } else if (is_2pc_logging()) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "abort when logging in 2pc", K(ret), K(*this));
  } else if (OB_FAIL(do_abort())) {
    TRANS_LOG(WARN, "do commit failed", K(ret), K(*this));
  } else if (FALSE_IT(collected_.reset())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "reset colloected failed", K(ret), K(*this));
  } else {
    if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ))) {
      TRANS_LOG(WARN, "post abort request failed", K(tmp_ret), K(*this));
    }

    if (OB_TMP_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT))) {
      TRANS_LOG(WARN, "submit abort log failed", K(tmp_ret), K(*this));
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_resp(const ObTwoPhaseCommitMsgType msg_type,
                                                const uint8_t participant_id)
{
  int ret = OB_SUCCESS;

  switch (msg_type) {
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP:
    ret = handle_2pc_prepare_redo_response(participant_id);
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP:
    ret = handle_2pc_prepare_response(participant_id);
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP:
    ret = handle_2pc_pre_commit_response(participant_id);
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP:
    ret = handle_2pc_commit_response(participant_id);
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP:
    ret = handle_2pc_abort_response(participant_id);
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP:
    ret = handle_2pc_clear_response(participant_id);
    break;
  default:
    TRANS_LOG(ERROR, "invalid msg type", K(msg_type));
    ret = OB_TRANS_INVALID_STATE;
    break;
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_resp(const ObTwoPhaseCommitMsgType recv_msg_type,
                                                       ObTwoPhaseCommitMsgType& send_msg_type,
                                                       bool& need_ack)
{
  int ret = OB_SUCCESS;

  switch (recv_msg_type) {
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP:
    ret = handle_orphan_2pc_prepare_redo_response(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP:
    ret = handle_orphan_2pc_prepare_response(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP:
    ret = handle_orphan_2pc_pre_commit_response(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP:
    ret = handle_orphan_2pc_commit_response(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP:
    ret = handle_orphan_2pc_abort_response(send_msg_type);
    need_ack = true;
    break;
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP:
    ret = handle_orphan_2pc_clear_response();
    need_ack = false;
    break;
  default:
    TRANS_LOG(ERROR, "invalid msg type", K(recv_msg_type));
    ret = OB_TRANS_INVALID_STATE;
    break;
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_timeout()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_TMP_FAIL(resubmit_downstream_log_())) {
    TRANS_LOG(WARN, "resubmit downstream log failed", KR(tmp_ret));
  }

  if (!is_leaf() && OB_TMP_FAIL(retransmit_downstream_msg_())) {
    TRANS_LOG(WARN, "retransmit downstream msg failed", KR(tmp_ret));
  }

  if (!is_root() && OB_TMP_FAIL(retransmit_upstream_msg_(get_downstream_state()))) {
    TRANS_LOG(WARN, "retransmit upstream msg failed", KR(tmp_ret));
  }

  return ret;
}

// retransmit msg to all unresponded downstreams
int ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTwoPhaseCommitMsgType msg_type;
  bool need_submit = true;

  if (!is_leaf()) {
    int this_part_id = get_participant_id();
    if (OB_FAIL(decide_downstream_msg_type_(need_submit, msg_type))) {
      TRANS_LOG(WARN, "deecide downstream msg_type fail", K(ret), KPC(this));
    } else if (need_submit) {
      for (int64_t i = 0; i < get_participants_size(); ++i) {
        if (!collected_.has_member(i) && this_part_id != i) {
          TRANS_LOG(INFO, "unresponded participant", K(i), K(*this));
          if (OB_TMP_FAIL(post_msg(msg_type, i))) {
            TRANS_LOG(WARN, "retransmit dowstream msg failed", KR(tmp_ret), KPC(this));
          }
        }
      }
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::decide_downstream_msg_type_(bool &need_submit,
                                                            ObTwoPhaseCommitMsgType &msg_type)
{
  int ret = OB_SUCCESS;
  msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_UNKNOWN;
  need_submit = true;
  switch (get_upstream_state())
  {
  case ObTxState::REDO_COMPLETE: {
    if (is_sub2pc()) {
      need_submit = true;
      msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ;
    } else {
      need_submit = false;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(WARN, "handle timeout when redo complete", KR(ret), KPC(this));
      }
    }
    break;
  }
  case ObTxState::PREPARE: {
    need_submit = true;
    msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ;
    break;
  }
  case ObTxState::PRE_COMMIT: {
    need_submit = true;
    msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ;
    break;
  }
  case ObTxState::COMMIT: {
    need_submit = true;
    msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ;
    break;
  }
  case ObTxState::ABORT: {
    need_submit = true;
    msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ;
    break;
  }
  case ObTxState::CLEAR: {
    need_submit = true;
    msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ;
    break;
  }
  default:
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(WARN, "invalid coord state", KR(ret), K(get_upstream_state()));
    break;
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::retransmit_downstream_msg_(const uint8_t participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_submit = true;
  ObTwoPhaseCommitMsgType msg_type;
  if (is_leaf()) {
  } else if (OB_FAIL(decide_downstream_msg_type_(need_submit, msg_type))) {
    TRANS_LOG(WARN, "decide downstream msg type fail", K(ret), KPC(this));
  } else if (need_submit && OB_TMP_FAIL(post_msg(msg_type, participant))) {
    TRANS_LOG(WARN, "post prepare msg failed", KR(tmp_ret), KPC(this));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_response(const uint8_t participant)
{
  int ret = OB_SUCCESS;
  bool no_need_submit_log = false;

  switch (get_upstream_state()) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE: {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(INFO, "recv prepare resp when coord state is init or redo complete",
                  KR(ret), K(participant), K(*this));
      }
      break;
    }
    // Because we want to reduce the latency at all costs, we need to handle
    // prepare response before prepare log successfully synchronized.
    case ObTxState::PREPARE: {
      if (OB_FAIL(handle_2pc_prepare_response_impl_(participant))) {
        TRANS_LOG(WARN, "handle 2pc prepare response failed", K(ret), K(*this));
      }
      break;
    }
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

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_prepare_response_impl_(const uint8_t participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (OB_FAIL(collect_downstream_(participant))) {
    TRANS_LOG(ERROR, "add participant to collected list failed", K(participant));
  } else if (is_2pc_logging()) {
    // skip if during logging
  } else if ((ObTxState::PREPARE == state)
             && all_downstream_collected_()) {
    if (is_root()) {
      if (OB_FAIL(try_enter_pre_commit_state())) {
        TRANS_LOG(WARN, "try_enter_pre_commit_state failed", K(ret), KPC(this));
      }
    } else {
      if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP,
                               OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post prepare response failed", KR(tmp_ret), KPC(this));
      }
    }
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_commit_response(const uint8_t participant)
{
  int ret = OB_SUCCESS;

  switch (get_upstream_state()) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc commit response find protocol error", K(get_upstream_state()), K(*this));
      break;
    }
    case ObTxState::PREPARE:
    case ObTxState::PRE_COMMIT: {
      // maybe this senario:
      // root has collected all prepare responses, advanced coord state to COMMIT
      // and post commit req to all participants. after that root crashed, and
      // coord_state_ is set to PREPARE(the same as state_), and participants'
      // commit responses arrive.
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(INFO, "recv commit resp when upstream state is prepare or pre_commit",
                  KR(ret), K(participant), K(*this));
      }
      break;
    }
    case ObTxState::COMMIT: {
      if (OB_FAIL(handle_2pc_ack_response_impl_(participant))) {
        TRANS_LOG(WARN, "retransmit msg failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::ABORT: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc commit response find protocol error", K(*this));
      break;
    }
    case ObTxState::CLEAR: {
      if (OB_FAIL(retransmit_downstream_msg_(participant))) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret), K(participant));
      }
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

int ObTxCycleTwoPhaseCommitter::handle_2pc_abort_response(const uint8_t participant)
{
  int ret = OB_SUCCESS;

  switch (get_upstream_state()) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE:
    // Downstream may abort itself and response the abort before two phase commit.
    case ObTxState::PREPARE: {
      if (OB_FAIL(handle_2pc_abort_response_impl_(participant))) {
        TRANS_LOG(WARN, "retransmit msg failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::COMMIT:
    // There may be the case that all participants has forgotten its state and
    // the coordinator has not, and according to the presume abort, we may
    // receive the abort response even we commit successfully.
    //
    // If you are interested in the similar problem, take a look at
    // https://yuque.antfin-inc.com/ob/transaction/gqif91
    case ObTxState::ABORT: {
      if (OB_FAIL(handle_2pc_ack_response_impl_(participant))) {
        TRANS_LOG(WARN, "retransmit msg failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::CLEAR: {
      // Upstream may lost the request, so we need reply the request based on
      // the state to advance the two phase commit protocol as soon as possible
      if (OB_FAIL(retransmit_downstream_msg_(participant))) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret));
      }
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

int ObTxCycleTwoPhaseCommitter::handle_2pc_ack_response_impl_(const uint8_t participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (OB_FAIL(collect_downstream_(participant))) {
    TRANS_LOG(ERROR, "add participant to collected list failed", K(participant));
  } else if (is_2pc_logging()) {
    // skip if during logging
  } else if ((ObTxState::COMMIT == state
              || ObTxState::ABORT == state)
             && all_downstream_collected_()) {
    if (is_root()) {
      set_upstream_state(ObTxState::CLEAR);
      collected_.reset();
      // TODO, drive it and submit_log via msg
      if (OB_FAIL(do_clear())) {
        TRANS_LOG(WARN, "do clear failed", KR(ret));
      } else {
        if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ))) {
          TRANS_LOG(WARN, "post clear request failed", K(tmp_ret), KPC(this));
        }
        if (OB_TMP_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_CLEAR))) {
          TRANS_LOG(WARN, "submit clear log failed", K(tmp_ret), KPC(this));
        }
      }
    } else {
      ObTwoPhaseCommitMsgType msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_UNKNOWN;
      switch (get_upstream_state()) {
      case ObTxState::PRE_COMMIT:
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP;
        break;
      case ObTxState::COMMIT:
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP;
        break;
      default:
        msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
        break;
      }
      if (OB_TMP_FAIL(post_msg(msg_type, OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post commit response failed", KR(tmp_ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_abort_response_impl_(const uint8_t participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTxState state = get_downstream_state();

  if (ObTxState::INIT != state
      && ObTxState::PREPARE != state) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(WARN, "invalid state when handle abort response", K(participant), KPC(this));
  } else if (is_2pc_logging()) {
    TRANS_LOG(INFO, "committer is under logging", K(ret), K(*this));
  } else if (ObTxState::INIT == get_upstream_state()
             || ObTxState::PREPARE == get_upstream_state()) {
    // Abandoned: We should not skip the msg during log synchronization, for example,
    // one of the participants aborts and response with the abort, and the
    // upstream is logging the prepare. We can jump to the abort state(and also
    // ignore the callback of the prepare log) and advance the two phase commit
    // state machine to abort process.
    //
    // Adopted: For safety, we donot allow the above optimization
    set_upstream_state(ObTxState::ABORT);
    collected_.reset();
    if (OB_FAIL(do_abort())) {
      TRANS_LOG(WARN, "do abort fail", K(ret), KPC(this));
    } else if (OB_FAIL(collect_downstream_(participant))) {
      TRANS_LOG(ERROR, "add participant to collected list failed", K(participant));
    } else {
      if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ))) {
        TRANS_LOG(WARN, "post commit request failed", K(tmp_ret), K(*this));
      }

      if (OB_TMP_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT))) {
        TRANS_LOG(WARN, "submit commit log failed", K(tmp_ret), K(*this));
      }
    }
  } else {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(WARN, "handle 2pc abort response find a bug", K(ret), K(*this));
  }

  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_pre_commit_response(const uint8_t participant)
{
  int ret = OB_SUCCESS;

  switch (get_upstream_state()) {
    case ObTxState::INIT:
    case ObTxState::REDO_COMPLETE: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc commit response find protocol error", K(get_upstream_state()), K(*this));
      break;
    }
    case ObTxState::PREPARE: {
      // maybe this senario:
      // root has collected all prepare responses, advanced coord state to PRE_COMMIT
      // and post pre commit req to all participants. after that root crashed, and
      // coord_state_ is set to PREPARE(the same as state_), and participants'
      // pre_commit responses
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(INFO, "recv pre_commit resp when upstream state is prepare",
                  KR(ret), K(participant), K(*this));
      }
      break;
    }
    case ObTxState::PRE_COMMIT: {
      if (OB_FAIL(handle_2pc_pre_commit_response_impl_(participant))) {
        TRANS_LOG(WARN, "retransmit msg failed", K(ret), K(*this));
      }
      break;
    }
    case ObTxState::COMMIT: {
      if (OB_FAIL(retransmit_downstream_msg_(participant))) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret), K(participant));
      }
      break;
    }
    case ObTxState::ABORT: {
      ret = OB_TRANS_PROTOCOL_ERROR;
      TRANS_LOG(ERROR, "handle 2pc commit response find protocol error", K(*this));
      break;
    }
    case ObTxState::CLEAR: {
      if (OB_FAIL(retransmit_downstream_msg_(participant))) {
        TRANS_LOG(WARN, "retransmit downstream msg failed", KR(ret), K(participant));
      }
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

int ObTxCycleTwoPhaseCommitter::handle_2pc_pre_commit_response_impl_(const uint8_t participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(collect_downstream_(participant))) {
    TRANS_LOG(ERROR, "add participant to collected list failed", K(participant));
  } else if (all_downstream_collected_()) {
    if (is_root()) {
      set_upstream_state(ObTxState::COMMIT);
      collected_.reset();
      // TODO, drive it and submit log via msg
      if (OB_FAIL(do_commit())) {
        TRANS_LOG(WARN, "do commit failed", K(ret), K(*this));
      } else {
        if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ))) {
          TRANS_LOG(WARN, "post commit request failed", K(tmp_ret), K(*this));
        }
        if (OB_TMP_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_COMMIT))) {
          TRANS_LOG(WARN, "submit commit log failed", K(tmp_ret), K(*this));
        }
      }
      // TODO, refine in 4.1
      if (is_sub2pc()) {
        TRANS_LOG(INFO, "handle pre commit response for sub trans", K(ret));
      }
    } else {
      if (OB_TMP_FAIL(post_msg(ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP,
                               OB_C2PC_UPSTREAM_ID))) {
        TRANS_LOG(WARN, "post prepare response failed", KR(tmp_ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_2pc_clear_response(const uint8_t participant)
{
  UNUSED(participant);
  return OB_SUCCESS;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_prepare_redo_response(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the upstream lose the memory, we response with the abort according to
  // presume abort. Even the upstream commits the txn and forgets the memory,
  // the orphan prepare must be old msg and the response will be rejected by the participant,
  // otherwise the downstream must not send the prepare request(participants must
  // enter commit state before upstream forgets its memory)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_prepare_response(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the upstream lose the memory, we response with the abort according to
  // presume abort. Even the upstream commits the txn and forgets the memory,
  // the orphan prepare must be old msg and the response will be rejected by the participant,
  // otherwise the downstream must not send the prepare request(participants must
  // enter commit state before upstream forgets its memory)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_pre_commit_response(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // When the downstream lose the memory, we response with the abort according
  // to presume abort. Even the downstream commits the txn and forgets the
  // memory, the orphan commit may be old msg or ack request and the response
  // will be reasonably adopted by the upstream (participants may enter commit
  // state and forget all before upstream forgets its memory. And after upstream
  // reboots, it will restart to collect the commit response)
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_commit_response(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // It may be the case that the coordinator log the clear and forgets itself
  // while the participant lose the clear request, we need drive the state
  // machine through clear request.
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_abort_response(ObTwoPhaseCommitMsgType& send_msg_type)
{
  int ret = OB_SUCCESS;

  // It may be the case that the coordinator log the clear and forgets itself
  // while the participant lose the clear request, we need drive the state
  // machine through clear request.
  //
  // TODO(handora.qc): abort process can be optimized by getting rid of the
  // clear phase
  send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ;
  return ret;
}

int ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_clear_response()
{
  return OB_SUCCESS;
}

bool ObTxCycleTwoPhaseCommitter::all_downstream_collected_()
{
  if (!is_leaf()) {
    return collected_.num_members() == get_participants_size() - 1;
  } else {
    return true;
  }
}

int ObTxCycleTwoPhaseCommitter::collect_downstream_(const uint8_t participant)
{
  int ret = OB_SUCCESS;

  if (!is_leaf()) {
    // TODO(handora.qc): remove the logic after donot need send vote to myself
    if (participant != get_participant_id()) {
      ret = collected_.add_member(participant);
    }
  }

  return ret;
}

bool is_2pc_response_msg(const ObTwoPhaseCommitMsgType msg_type)
{
  return ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP == msg_type
    || ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP == msg_type;
}

} // end namespace transaction
} // end namespace oceanbase
