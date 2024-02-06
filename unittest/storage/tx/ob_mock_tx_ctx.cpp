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

#include "ob_mock_tx_ctx.h"

namespace oceanbase
{

using namespace common;
using namespace share;
namespace transaction
{

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

int MockObTxCtx::init(const ObLSID &ls_id,
                      const ObTransID &trans_id,
                      ObLSTxCtxMgr *ctx_mgr,
                      ObTxData *tx_data,
                      ObMailBoxMgr<ObTxMsg> *mgr)
{
  int ret = OB_SUCCESS;
  int64_t mock_tenant_id = 1001;

  CtxLockGuard guard(lock_);

  default_init_();

  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtx inited twice");
    ret = OB_INIT_TWICE;
// ==================== mock dist trans ctx begin ====================
// ===================== mock dist trans ctx end =====================
// ====================== mock trans ctx begin =======================
  } else if (OB_FAIL(lock_.init(this))) {
    TRANS_LOG(WARN, "init lock error", KR(ret));
// ======================= mock trans ctx end ========================
  } else if (OB_FAIL(init_log_cbs_(ls_id, trans_id))) {
    TRANS_LOG(WARN, "init log cbs failed", KR(ret), K(trans_id), K(ls_id));
  }

  if (OB_SUCC(ret)) {
    // tx_data_ = tx_data;
    ctx_tx_data_.test_init(*tx_data, ctx_mgr);
    // mock dist trans ctx begin
    // we mock away the rpc_ and location_adapter
    rpc_ = NULL;
    // mock dist trans ctx end

    // mock trans ctx begin
    // we mock away the trans_service_ and trans_param_ and ctx_mgr_
    trans_service_ = NULL;
    trans_expired_time_ = UINT64_MAX;
    trans_id_ = trans_id;
    tenant_id_ = mock_tenant_id;
    // mock trans ctx end
    cluster_version_ = CLUSTER_VERSION_4_0_0_0;

    // TODO(handora.qc): add timer
    timer_ = NULL;
    // trans part ctx
    ls_id_ = ls_id;
    mt_ctx_.trans_begin();
    mt_ctx_.set_trans_ctx(this);
    mt_ctx_.set_for_replay(false /*for_replay*/);
    pending_write_ = 0;

    // self begin
    mailbox_mgr_ = mgr;
    log_queue_.clear();
    if (mailbox_mgr_ != NULL
        && OB_FAIL(mailbox_mgr_->register_mailbox(addr_, mailbox_, this))) {
      TRANS_LOG(ERROR, "mock ctx register mailbox failed");
    }
    // self end
    ObPartTransCtx::addr_.parse_from_cstring("127.0.0.1:3001");
    ObPartTransCtx::addr_.set_port(3001 + (int32_t)ls_id.id());
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "mock tx ctx init failed");
    ob_abort();
  } else {
    TRANS_LOG(INFO, "mock tx ctx init success", KPC(this));
  }

  return ret;
}

void MockObTxCtx::destroy()
{
  is_inited_ = false;
}

int MockObTxCtx::submit_log(const ObTwoPhaseCommitLogType& log_type)
{
  log_queue_.push_back(log_type);
  TRANS_LOG(INFO, "submit log success", K(log_type), KPC(this));
  return OB_SUCCESS;
}

int ObPartTransCtx::search_unsubmitted_dup_table_redo_()
{
  int ret = OB_SUCCESS;

  return ret;
}

int MockObTxCtx::register_timeout_task_(const int64_t interval_us)
{
  return OB_SUCCESS;
}
int MockObTxCtx::unregister_timeout_task_()
{
  return OB_SUCCESS;
}
int MockObTxCtx::post_msg_(const ObLSID &receiver,
                           ObTxMsg &msg)
{
  int ret = OB_SUCCESS;
  int64_t to = 0;
  int64_t from = 0;
  uint64_t size = 0;

  ObMail<ObTxMsg> mail;
  from = mailbox_.addr_;
  to = addr_memo_[receiver];

  switch (msg.get_msg_type()) {
  case TX_2PC_PREPARE_REQ: {
    size = sizeof(Ob2pcPrepareReqMsg);
    Ob2pcPrepareReqMsg &prepare_msg = dynamic_cast<Ob2pcPrepareReqMsg&>(msg);
    mail.init(from, to, size, prepare_msg);
    break;
  }
  case TX_2PC_PREPARE_RESP: {
    size = sizeof(Ob2pcPrepareRespMsg);
    Ob2pcPrepareRespMsg &prepare_resp = dynamic_cast<Ob2pcPrepareRespMsg&>(msg);
    mail.init(from, to, size, prepare_resp);
    break;
  }
  case TX_2PC_PRE_COMMIT_REQ: {
    size = sizeof(Ob2pcPreCommitReqMsg);
    Ob2pcPreCommitReqMsg &pre_commit_req = dynamic_cast<Ob2pcPreCommitReqMsg&>(msg);
    mail.init(from, to, size, pre_commit_req);
    break;
  }
  case TX_2PC_PRE_COMMIT_RESP: {
    size = sizeof(Ob2pcPreCommitRespMsg);
    Ob2pcPreCommitRespMsg &pre_commit_resp = dynamic_cast<Ob2pcPreCommitRespMsg&>(msg);
    mail.init(from, to, size, pre_commit_resp);
    break;
  }
  case TX_2PC_COMMIT_REQ: {
    size = sizeof(Ob2pcCommitReqMsg);
    Ob2pcCommitReqMsg &commit_msg = dynamic_cast<Ob2pcCommitReqMsg&>(msg);
    mail.init(from, to, size, commit_msg);
    break;
  }
  case TX_2PC_COMMIT_RESP: {
    size = sizeof(Ob2pcCommitRespMsg);
    Ob2pcCommitRespMsg &commit_resp = dynamic_cast<Ob2pcCommitRespMsg&>(msg);
    mail.init(from, to, size, commit_resp);
    break;
  }
  case TX_2PC_ABORT_REQ: {
    size = sizeof(Ob2pcAbortReqMsg);
    Ob2pcAbortReqMsg &abort_msg = dynamic_cast<Ob2pcAbortReqMsg&>(msg);
    mail.init(from, to, size, abort_msg);
    break;
  }
  case TX_2PC_ABORT_RESP: {
    size = sizeof(Ob2pcAbortRespMsg);
    Ob2pcAbortRespMsg &abort_resp = dynamic_cast<Ob2pcAbortRespMsg&>(msg);
    mail.init(from, to, size, abort_resp);
    break;
  }
  case TX_2PC_CLEAR_REQ: {
    size = sizeof(Ob2pcClearReqMsg);
    Ob2pcClearReqMsg &clear_msg = dynamic_cast<Ob2pcClearReqMsg&>(msg);
    mail.init(from, to, size, clear_msg);
    break;
  }
  case TX_2PC_CLEAR_RESP: {
    size = sizeof(Ob2pcClearRespMsg);
    Ob2pcClearRespMsg &clear_resp = dynamic_cast<Ob2pcClearRespMsg&>(msg);
    mail.init(from, to, size, clear_resp);
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid msg type", K(msg));
  }

  if (OB_FAIL(mailbox_mgr_->send(mail, mail.to_))) {
    TRANS_LOG(WARN, "send mailbox failed", K(ret), K(msg), KPC(this));
  } else {
    TRANS_LOG(INFO, "send mailbox success", K(ret), K(mail), K(msg));
  }

  return ret;
}

int MockObTxCtx::post_msg_(const ObAddr &,
                           ObTxMsg &msg)
{
  int ret = OB_SUCCESS;

  if (TX_COMMIT_RESP != msg.get_msg_type()) {
    TRANS_LOG(ERROR, "encounter invalid msg");
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObMail<ObTxMsg> mail;
    int64_t from = mailbox_.addr_;
    int64_t to = scheduler_addr_;
    ObTxCommitRespMsg &tx_commit_resp = dynamic_cast<ObTxCommitRespMsg&>(msg);
    mail.init(from, to, sizeof(ObTxCommitRespMsg), tx_commit_resp);

    if (OB_FAIL(mailbox_mgr_->send(mail, mail.to_))) {
      TRANS_LOG(WARN, "send mailbox failed", K(ret), K(msg), KPC(this));
    } else {
      TRANS_LOG(INFO, "send mailbox to scheduler success", K(ret), K(mail), K(msg));
    }
  }

  return ret;
}

int MockObTxCtx::apply()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (log_queue_.empty()) {
    TRANS_LOG(ERROR, "log_queue is empty", KPC(this));
    ob_abort();
  } else {
    ObTwoPhaseCommitLogType log_type = log_queue_.front();
    log_queue_.pop_front();
    ret = ObTxCycleTwoPhaseCommitter::apply_log(log_type);
    if (OB_FAIL(ret)) {
      TRANS_LOG(ERROR, "apply log success", K(ret), K(log_type), KPC(this));
      ob_abort();
    }
  }

  return ret;
}

int MockObTxCtx::handle(const ObMail<ObTxMsg>& mail)
{
  int ret = OB_SUCCESS;

  // TODO(handora.qc): remove it?
  addr_memo_[mail.mail_->get_sender()] = mail.from_;
  ls_memo_[mail.from_] = mail.mail_->get_sender();
  TRANS_LOG(INFO, "add addr memo", K(mail.mail_->get_sender()), K(mail.from_));
  if (TX_COMMIT == mail.mail_->get_msg_type()) {
    // TODO(handora.qc): improve it
    const ObTxCommitMsg *tx_commit_msg = dynamic_cast<const ObTxCommitMsg*>(mail.mail_);
    for (int64_t i = 0; i < tx_commit_msg->parts_.count(); i++) {
      ObLSID ls_id = tx_commit_msg->parts_[i];
      for (auto const& x : mailbox_mgr_->mgr_) {
        ObMailBox<ObTxMsg> *mailbox = x.second;
        ObPartTransCtx *ctx = dynamic_cast<ObPartTransCtx *>(mailbox->ctx_);
        if (NULL != ctx /* for scheduler */
            && ls_id == ctx->get_ls_id()) {
          addr_memo_[ls_id] = mailbox->addr_;
          ls_memo_[mailbox->addr_] = ls_id;
          TRANS_LOG(INFO, "add addr memo", K(ls_id), K(mailbox->addr_));
        }
      }
    }
  }

  switch (mail.mail_->get_msg_type()) {
  case TX_COMMIT: {
    const ObTxCommitMsg *msg = dynamic_cast<const ObTxCommitMsg*>(mail.mail_);
    scheduler_addr_ = mail.from_;
    ret = commit(msg->parts_,
                 MonotonicTs::current_time(),
                 msg->expire_ts_,
                 msg->app_trace_info_,
                 msg->request_id_);
    break;
  }
  case TX_2PC_PREPARE_REQ: {
    const Ob2pcPrepareReqMsg *prepare_msg = dynamic_cast<const Ob2pcPrepareReqMsg*>(mail.mail_);
    ret = handle_tx_2pc_prepare_req(*prepare_msg);
    break;
  }
  case TX_2PC_PREPARE_RESP: {
    const Ob2pcPrepareRespMsg *prepare_resp = dynamic_cast<const Ob2pcPrepareRespMsg*>(mail.mail_);
    ret = handle_tx_2pc_prepare_resp(*prepare_resp);
    break;
  }
  case TX_2PC_PRE_COMMIT_REQ: {
    const Ob2pcPreCommitReqMsg *pre_commit_req = dynamic_cast<const Ob2pcPreCommitReqMsg*>(mail.mail_);
    ret = handle_tx_2pc_pre_commit_req(*pre_commit_req);
    break;
  }
  case TX_2PC_PRE_COMMIT_RESP: {
    const Ob2pcPreCommitRespMsg *pre_commit_resp = dynamic_cast<const Ob2pcPreCommitRespMsg*>(mail.mail_);
    ret = handle_tx_2pc_pre_commit_resp(*pre_commit_resp);
    break;
  }
  case TX_2PC_COMMIT_REQ: {
    const Ob2pcCommitReqMsg *commit_msg = dynamic_cast<const Ob2pcCommitReqMsg*>(mail.mail_);
    ret = handle_tx_2pc_commit_req(*commit_msg);
    break;
  }
  case TX_2PC_COMMIT_RESP: {
    const Ob2pcCommitRespMsg *commit_resp = dynamic_cast<const Ob2pcCommitRespMsg*>(mail.mail_);
    ret = handle_tx_2pc_commit_resp(*commit_resp);
    break;
  }
  case TX_2PC_ABORT_REQ: {
    const Ob2pcAbortReqMsg *abort_msg = dynamic_cast<const Ob2pcAbortReqMsg*>(mail.mail_);
    ret = handle_tx_2pc_abort_req(*abort_msg);
    break;
  }
  case TX_2PC_ABORT_RESP: {
    const Ob2pcAbortRespMsg *abort_resp = dynamic_cast<const Ob2pcAbortRespMsg*>(mail.mail_);
    ret = handle_tx_2pc_abort_resp(*abort_resp);
    break;
  }
  case TX_2PC_CLEAR_REQ: {
    const Ob2pcClearReqMsg *clear_msg = dynamic_cast<const Ob2pcClearReqMsg*>(mail.mail_);
    ret = handle_tx_2pc_clear_req(*clear_msg);
    break;
  }
  case TX_2PC_CLEAR_RESP: {
    const Ob2pcClearRespMsg *clear_resp = dynamic_cast<const Ob2pcClearRespMsg*>(mail.mail_);
    ret = handle_tx_2pc_clear_resp(*clear_resp);
    break;
  }
  default:
    TRANS_LOG(ERROR, "invalid msg type", K(mail));
    ret = OB_TRANS_INVALID_STATE;
    break;
  }

  TRANS_LOG(INFO, "handle msg success", K(mail), KPC(this));

  return ret;
}

int MockObTxCtx::mock_tx_commit_msg(const ObTransID &trans_id,
                                    const ObLSID &ls_id,
                                    const std::vector<ObLSID> &participants,
                                    ObTxCommitMsg &msg)
{
  int ret = OB_SUCCESS;
  int64_t mock_cluster_version = 1;
  int64_t mock_cluster_id = 1;
  uint64_t mock_tenant_id = 1001;

  msg.type_ = TX_COMMIT;
  msg.cluster_version_ = mock_cluster_version;
  msg.tenant_id_ = mock_tenant_id;
  msg.tx_id_ = trans_id;
  msg.receiver_ = ls_id;
  msg.cluster_id_ = mock_cluster_id;
  msg.parts_.reset();
  msg.expire_ts_ = ObTimeUtility::current_time() + 5 * 1000 * 1000;
  for (int64_t i = 0; i < participants.size(); i++) {
    msg.parts_.push_back(participants[i]);
  }

  return ret;
}

int MockObTxCtx::handle(const bool must_have)
{
  return mailbox_.handle(must_have);
}

int MockObTxCtx::handle_all()
{
  return mailbox_.handle_all();
}

int MockObTxCtx::get_gts_(share::SCN &gts)
{
  // TODO(handora.qc): get gts failed
  gts.convert_for_gts(ObTimeUtility::current_time_ns());
  return OB_SUCCESS;
}

int MockObTxCtx::wait_gts_elapse_commit_version_(bool &need_wait)
{
  need_wait = false;
  return OB_SUCCESS;
}

int MockObTxCtx::get_local_max_read_version_(share::SCN &local_max_read_version)
{
  local_max_read_version.convert_for_gts(ObTimeUtility::current_time());
  return OB_SUCCESS;
}

int MockObTxCtx::update_local_max_commit_version_(const share::SCN &)
{
  return OB_SUCCESS;
}

void MockObTxCtx::set_exiting_()
{
  TRANS_LOG(INFO, "exiting!!", KPC(this));
  is_exiting_ = true;
}

} // end namespace transaction

} // end namespace oceanbase
