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

#include "ob_mock_2pc_ctx.h"

namespace oceanbase
{
using namespace common;
namespace transaction
{

int64_t MockObParticipants::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;

  if (nullptr != buffer && size > 0) {
    databuff_printf(buffer, size, pos, "{vec(%zu): [", this->size());
    for (auto it = this->begin(); it != this->end(); ++it) {
      databuff_printf(buffer, size, pos, "(%ld), ", *it);
    }

    databuff_printf(buffer, size, pos, "]}");
  }

  return pos;
}

int64_t MockObLogQueue::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;

  if (nullptr != buffer && size > 0) {
    databuff_printf(buffer, size, pos, "{Queue(%zu): [", this->size());
    for (auto it = this->begin(); it != this->end(); ++it) {
      databuff_printf(buffer, size, pos, "(%hhu), ", static_cast<uint8_t>(*it));
    }

    databuff_printf(buffer, size, pos, "]}");
  }

  return pos;
}

int MockOb2pcCtx::init(ObMailBoxMgr<ObTwoPhaseCommitMsgType> *mgr)
{
  int ret = OB_SUCCESS;

  upstream_state_ = ObTxState::INIT;
  downstream_state_ = ObTxState::INIT;
  tx_state_ = ObTxState::INIT;
  log_queue_.clear();
  participants_.clear();
  intermediate_participants_.clear();
  coordinator_ = -1;
  sender_ = -1;
  mailbox_mgr_ = mgr;
  if (OB_FAIL(mailbox_mgr_->register_mailbox(addr_, mailbox_, this))) {
    TRANS_LOG(ERROR, "mock ctx register mailbox failed");
  }

  return ret;
}

int MockOb2pcCtx::commit(const MockObParticipants& participants)
{
  ObLockGuard<ObSpinLock> lock_guard(latch_);
  participants_.assign(participants.begin(), participants.end());
  coordinator_ = addr_;
  return two_phase_commit();
}

int MockOb2pcCtx::do_prepare(bool &no_need_submit_log)
{
  no_need_submit_log = false;
  TRANS_LOG(INFO, "mock ctx do prepare successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::on_prepare()
{
  TRANS_LOG(INFO, "mock ctx on prepare successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::do_pre_commit(bool& need_wait)
{
  return OB_SUCCESS;
}

int MockOb2pcCtx::do_commit()
{
  TRANS_LOG(INFO, "mock ctx do commit successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::on_commit()
{
  TRANS_LOG(INFO, "mock ctx on commit successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::do_abort()
{
  TRANS_LOG(INFO, "mock ctx do abort successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::on_abort()
{
  TRANS_LOG(INFO, "mock ctx on abort successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::do_clear()
{
  TRANS_LOG(INFO, "mock ctx do clear successful", K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::on_clear()
{
  TRANS_LOG(INFO, "mock ctx on clear successful", K(*this));
  return OB_SUCCESS;
}

Ob2PCRole MockOb2pcCtx::get_2pc_role() const
{
  Ob2PCRole role;

  if (coordinator_ == -1) {
    role = Ob2PCRole::UNKNOWN;
  } else if (addr_ == coordinator_) {
    role = Ob2PCRole::ROOT;
  } else if (0 == participants_.size()) {
    // not root & downstream is empty
    role = Ob2PCRole::LEAF;
  } else {
    role = Ob2PCRole::INTERNAL;
  }

  return role;
}

int64_t MockOb2pcCtx::get_downstream_size() const
{
  return participants_.size();
}

int64_t MockOb2pcCtx::get_self_id()
{
  int participant_id = 0;

  if ((participant_id = find_participant_id(addr_)) == -1) {
// TRANS_LOG(ERROR, "cannot find self", K(addr_), K(participants_));
  }

  return participant_id;
}

int MockOb2pcCtx::submit_log(const ObTwoPhaseCommitLogType& log_type)
{
  log_queue_.push_back(log_type);
  TRANS_LOG(INFO, "submit log success", K(log_type), K(*this));
  return OB_SUCCESS;
}

int MockOb2pcCtx::post_msg(const ObTwoPhaseCommitMsgType& msg_type,
                          const int64_t participant)
{
  int ret = OB_SUCCESS;
  int64_t to = 0;
  int64_t from = 0;

  ObMail<ObTwoPhaseCommitMsgType> mail;
  from = mailbox_.addr_;
  if (participant == OB_C2PC_UPSTREAM_ID) {
    to = coordinator_;
  } else if (participant == OB_C2PC_SENDER_ID) {
    if (-1 != sender_) {
      to = sender_;
    } else if (-1 != coordinator_) {
      to = coordinator_;
    } else {
      to = -1;
    }
  } else {
    to = participants_[participant];
  }
  mail.init(from, to, sizeof(ObTwoPhaseCommitMsgType), msg_type);

  if (-1 == to
      && participant == OB_C2PC_UPSTREAM_ID
      && ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP == msg_type) {
    TRANS_LOG(INFO, "self decide abort", K(ret), K(msg_type), K(participant),
              K(mail));
  } else if (-1 == to
             && participant == OB_C2PC_UPSTREAM_ID) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid dst", K(ret), K(msg_type), K(participant),
              K(mail));
  } else if (-1 == to
             && participant == OB_C2PC_SENDER_ID) {
    TRANS_LOG(INFO, "new transfer without sender", K(ret), K(msg_type), K(participant),
              K(mail), K(to));
  } else if (mail.from_ != mail.to_) {
    if (OB_FAIL(mailbox_mgr_->send(mail, mail.to_))) {
      TRANS_LOG(WARN, "send mailbox failed", K(ret), K(msg_type), K(participant));
    }
  } else {
    TRANS_LOG(INFO, "send to self", K(ret), K(msg_type), K(participant),
              K(mail));
  }

  return ret;
}

int MockOb2pcCtx::apply_2pc_msg_(const ObTwoPhaseCommitMsgType msg_type)
{
  int ret = OB_SUCCESS;

  UNUSED(msg_type);

  return ret;
}

int64_t MockOb2pcCtx::find_participant_id(int64_t participant_key)
{
  for (int i = 0; i < participants_.size(); i++) {
    if (participants_[i] == participant_key) {
      return i;
    }
  }

  return -1;
}

int MockOb2pcCtx::handle(const ObMail<ObTwoPhaseCommitMsgType> &mail)
{
  int ret = OB_SUCCESS;
  int64_t participant_id = 0;
  ObTwoPhaseCommitMsgType type = *mail.mail_;
  ObLockGuard<ObSpinLock> lock_guard(latch_);

  sender_ = mail.from_;

  if ((participant_id = find_participant_id(mail.from_)) == -1
      && is_2pc_response_msg(type)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "2pc request with wrong participant id", K(ret));
  } else if (is_2pc_request_msg(type)
             && -1 == coordinator_
             && FALSE_IT(coordinator_ = mail.from_)) {
  } else if (is_2pc_request_msg(type)
             && OB_FAIL(handle_2pc_req(type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(mail), K(*this));
  } else if (is_2pc_response_msg(type)
             && OB_FAIL(handle_2pc_resp(type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(mail), K(*this));
  } else {
    TRANS_LOG(INFO, "handle msg success", K(addr_), K(mail), K(*this));
  }

  sender_ = -1;

  return ret;
}

int MockOb2pcCtx::handle(const bool must_have)
{
  return mailbox_.handle(must_have);
}

int MockOb2pcCtx::handle_all()
{
  return mailbox_.handle_all();
}

ObTxState MockOb2pcCtx::get_upstream_state() const
{
  return upstream_state_;
}

int MockOb2pcCtx::set_upstream_state(const ObTxState state)
{
  upstream_state_ = state;
  return OB_SUCCESS;
}

ObTxState MockOb2pcCtx::get_downstream_state() const
{
  return downstream_state_;
}

int MockOb2pcCtx::set_downstream_state(const ObTxState new_state)
{
  switch (new_state) {
  case ObTxState::INIT: {
    TRANS_LOG_RET(ERROR, OB_ERROR, "tx switch to init failed", K(*this), K(new_state));
    ob_abort();
    break;
  }
  case ObTxState::PREPARE: {
    if (ObTxState::INIT != downstream_state_
        && ObTxState::PREPARE != downstream_state_) {
      TRANS_LOG_RET(ERROR, OB_ERROR, "tx switch to prepare failed", K(*this), K(new_state));
      ob_abort();
    }
    break;
  }
  case ObTxState::PRE_COMMIT: {
    if (ObTxState::PREPARE != downstream_state_
        && ObTxState::PRE_COMMIT != downstream_state_) {
      TRANS_LOG_RET(ERROR, OB_ERROR, "tx switch to pre commit failed", KPC(this), K(new_state));
      ob_abort();
    }
    break;
  }
  case ObTxState::COMMIT: {
    if (ObTxState::PRE_COMMIT != downstream_state_
        && ObTxState::COMMIT != downstream_state_) {
      TRANS_LOG_RET(ERROR, OB_ERROR, "tx switch to commit failed", K(*this), K(new_state));
      ob_abort();
    } else {
      tx_state_ = new_state;
    }
    break;
  }
  case ObTxState::ABORT: {
    if (ObTxState::INIT != downstream_state_
        && ObTxState::PREPARE != downstream_state_
        && ObTxState::ABORT != downstream_state_) {
      TRANS_LOG_RET(ERROR, OB_ERROR, "tx switch to abort failed", K(*this), K(new_state));
      ob_abort();
    } else {
      tx_state_ = new_state;
    }
    break;
  }
  case ObTxState::CLEAR: {
    if (ObTxState::COMMIT != downstream_state_
        && ObTxState::ABORT != downstream_state_) {
      TRANS_LOG_RET(ERROR, OB_ERROR, "tx switch to clear failed", K(*this), K(new_state));
      ob_abort();
    }
    break;
  }
  default: {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid 2pc state", KPC(this));
    ob_abort();
    break;
  }
  }

  downstream_state_ = new_state;
  return OB_SUCCESS;
}

int MockOb2pcCtx::apply()
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(latch_);

  if (log_queue_.empty()) {
    TRANS_LOG(ERROR, "log_queue is empty", K(*this));
    ob_abort();
  } else {
    ObTwoPhaseCommitLogType log_type = log_queue_.front();
    log_queue_.pop_front();
    ret = ObTxCycleTwoPhaseCommitter::apply_log(log_type);
  }

  return ret;
}

int MockOb2pcCtx::abort()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(set_upstream_state(ObTxState::ABORT))) {
    TRANS_LOG(WARN, "set upstream state failed", K(ret));
  } else if (OB_FAIL(do_abort())) {
    TRANS_LOG(WARN, "do commit failed", K(ret));
  } else if (OB_FAIL(submit_log(ObTwoPhaseCommitLogType::OB_LOG_TX_ABORT))) {
    TRANS_LOG(WARN, "ctx go to abort failed", K(ret));
  }

  return ret;
}

bool MockOb2pcCtx::is_2pc_logging() const
{
  return !log_queue_.empty();
}

bool MockOb2pcCtx::check_status_valid(const bool should_commit)
{
  bool bret = true;

  // check commit or abort
  if (bret) {
    if (should_commit) {
      bret = ObTxState::COMMIT == tx_state_;
    } else {
      bret = ObTxState::ABORT == tx_state_;
    }
  }

  // check clear
  if (bret) {
    bret = ObTxState::CLEAR == downstream_state_;
  }

  if (!bret) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "state is not match", K(*this), K(should_commit));
  }

  return bret;
}

int MockOb2pcCtx::reply_to_scheduler_for_sub2pc(int64_t msg_type)
{
  TRANS_LOG(INFO, "mock ctx do sub prepare successful", K(*this));
  return OB_SUCCESS;
}

bool MockOb2pcCtx::is_sub2pc() const
{
  return false;
}

bool MockOb2pcCtx::is_dup_tx() const
{
  return false;
}

int MockOb2pcCtx::merge_intermediate_participants()
{
  int ret = OB_SUCCESS;
  bool exist = false;

  for (int64_t i = 0; i < intermediate_participants_.size(); i++) {
    exist = false;
    for (int64_t j = 0; !exist && j < participants_.size(); j++) {
      if (participants_[j] == intermediate_participants_[i]) {
        exist = true;
      }
    }

    if (!exist) {
      participants_.push_back(intermediate_participants_[i]);
    }
  }

  intermediate_participants_.clear();

  return ret;
}

void MockOb2pcCtx::add_intermediate_participants(const int64_t ls_id)
{
  bool exist = false;

  for (int64_t i = 0; !exist && i < intermediate_participants_.size(); i++) {
    if (intermediate_participants_[i] == ls_id) {
      exist = true;
    }
  }

  if (!exist) {
    intermediate_participants_.push_back(ls_id);
  }
}

void MockOb2pcCtx::print_downstream()
{
  TRANS_LOG(INFO, "[TREE_COMMIT_PRINT]", K(addr_));
  for (int64_t i = 0; i < participants_.size(); i++) {
    TRANS_LOG(INFO, "[TREE_COMMIT_PRINT]    ", K(participants_[i]));
  }
}

bool MockOb2pcCtx::is_real_upstream()
{
  bool bret = false;

  if (-1 == sender_) {
    bret = true;
  } else {
    bret = sender_ == coordinator_;
  }

  return bret;
}

bool MockOb2pcCtx::need_to_advance()
{
  Ob2PCRole role = get_2pc_role();
  if (role == Ob2PCRole::ROOT) {
    if (!all_downstream_collected_()) {
      return true;
    } else {
      return false;
    }
  } else if (role == Ob2PCRole::INTERNAL) {
    if (!all_downstream_collected_()) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }

  return false;
}

// bool MockOb2pcCtx::is_downstream_of(const int64_t ls_id)
// {
//   for (int64_t i = 0; i < participants_.size(); i++) {
//     if (participants_[i] == ls_id) {
//       return true;
//     }
//   }

//   for (int64_t i = 0; i < incremental_participants_.size(); i++) {
//     if (intermediate_participants_[i] == ls_id) {
//       return true;
//     }
//   }

//   return false;
// }

} // end namespace transaction
} // end namespace oceanbase
