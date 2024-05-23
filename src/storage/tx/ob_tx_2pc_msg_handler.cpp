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

#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace transaction
{

int ObPartTransCtx::post_msg_(const ObTwoPhaseCommitMsgType& msg_type,
                              const ObLSID &receiver)
{
  int ret = OB_SUCCESS;

  switch (msg_type) {
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ: {
    Ob2pcPrepareRedoReqMsg prepare_redo_req;
    build_tx_common_msg_(receiver, prepare_redo_req);
    prepare_redo_req.upstream_ = ls_id_;
    prepare_redo_req.xid_ = exec_info_.xid_;
    prepare_redo_req.app_trace_info_ = trace_info_.get_app_trace_info();
    if (OB_FAIL(post_msg_(receiver, prepare_redo_req))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    TRANS_LOG(INFO, "post prepare redo request msg", K(ret), K(prepare_redo_req));
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP: {
    Ob2pcPrepareRedoRespMsg prepare_redo_resp;
    build_tx_common_msg_(receiver, prepare_redo_resp);
    if (OB_FAIL(post_msg_(receiver, prepare_redo_resp))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ: {
    if (is_sub2pc()) {
      // for xa trans, if prepare request, convert it to prepare version request
      Ob2pcPrepareVersionReqMsg prepare_version_req;
      build_tx_common_msg_(receiver, prepare_version_req);
      prepare_version_req.upstream_ = ls_id_;
      if (OB_FAIL(post_msg_(receiver, prepare_version_req))) {
        TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
      }
    } else {
      Ob2pcPrepareReqMsg prepare_req;
      build_tx_common_msg_(receiver, prepare_req);
      prepare_req.upstream_ = ls_id_;
      prepare_req.app_trace_info_ = trace_info_.get_app_trace_info();
      if (OB_FAIL(post_msg_(receiver, prepare_req))) {
        TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
      }
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP: {
    if (is_sub2pc()) {
      // for xa trans, if prepare response, convert it to prepare version response
      Ob2pcPrepareVersionRespMsg prepare_version_resp;
      build_tx_common_msg_(receiver, prepare_version_resp);
      prepare_version_resp.prepare_version_ = exec_info_.prepare_version_;
      if (OB_FAIL(prepare_version_resp.prepare_info_array_.assign(exec_info_.prepare_log_info_arr_))) {
        TRANS_LOG(WARN, "assign prepare_log_info_arr_ failed", K(ret));
      } else if (OB_FAIL(post_msg_(receiver, prepare_version_resp))) {
        TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
      }
    } else {
      Ob2pcPrepareRespMsg prepare_resp;
      build_tx_common_msg_(receiver, prepare_resp);
      prepare_resp.prepare_version_ = exec_info_.prepare_version_;
      if (OB_FAIL(prepare_resp.prepare_info_array_.assign(exec_info_.prepare_log_info_arr_))) {
        TRANS_LOG(WARN, "assign prepare_log_info_arr_ failed", K(ret));
      } else if (OB_FAIL(post_msg_(receiver, prepare_resp))) {
        TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
      }
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ: {
    Ob2pcPreCommitReqMsg pre_commit_req;
    build_tx_common_msg_(receiver, pre_commit_req);
    pre_commit_req.commit_version_ = ctx_tx_data_.get_commit_version();
    if (OB_FAIL(post_msg_(receiver, pre_commit_req))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP: {
    Ob2pcPreCommitRespMsg pre_commit_resp;
    build_tx_common_msg_(receiver, pre_commit_resp);
    pre_commit_resp.commit_version_ = ctx_tx_data_.get_commit_version();
    if (exec_info_.is_dup_tx_ && (OB_FAIL(dup_table_tx_pre_commit_()))) {
      if (OB_EAGAIN == ret) {
        TRANS_LOG(INFO, "dup table pre_commit is not finish", K(ret));
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "dup table pre_commit error", K(ret));
      }
    } else if (OB_FAIL(post_msg_(receiver, pre_commit_resp))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ: {
    Ob2pcCommitReqMsg commit_req;
    build_tx_common_msg_(receiver, commit_req);
    commit_req.commit_version_ = ctx_tx_data_.get_commit_version();

    if (is_root()) {
      if (false/*exec_info_.prepare_log_info_arr_.count() != exec_info_.participants_.count()*/) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "incomplete prepare_log_info_arr_ in exec_info_", K(ret), K(trans_id_),
                  K(ls_id_), K(exec_info_.prepare_log_info_arr_), K(exec_info_.participants_));
      } else if (coord_prepare_info_arr_.count() <= 0) {
        TRANS_LOG(INFO,
                  "empty coord_prepare_info_arr_ before posting commit requesti, may be recover from "
                  "the commit state",
                  K(ret), KPC(this));
        if (OB_FAIL(coord_prepare_info_arr_.assign(exec_info_.prepare_log_info_arr_))) {
          TRANS_LOG(WARN, "copy prepare_log_info_arr_ from exec_info_ for root failed", K(ret),
                    KPC(this));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(commit_req.prepare_info_array_.assign(coord_prepare_info_arr_))) {
      TRANS_LOG(WARN, "assign prepare_log_info_arr_ failed", K(ret));
    } else if (OB_FAIL(post_msg_(receiver, commit_req))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP: {
    Ob2pcCommitRespMsg commit_resp;
    build_tx_common_msg_(receiver, commit_resp);
    commit_resp.commit_version_ = ctx_tx_data_.get_commit_version();
    if (max_2pc_commit_scn_.is_valid()) {
      commit_resp.commit_log_scn_ =
          share::SCN::max(max_2pc_commit_scn_, ctx_tx_data_.get_end_log_ts());
    } else {
      commit_resp.commit_log_scn_ = ctx_tx_data_.get_end_log_ts();
    }
    if (OB_FAIL(post_msg_(receiver, commit_resp))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ: {
    Ob2pcAbortReqMsg abort_req;
    build_tx_common_msg_(receiver, abort_req);
    abort_req.upstream_ = ls_id_;
    if (OB_FAIL(post_msg_(receiver, abort_req))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP: {
    Ob2pcAbortRespMsg abort_resp;
    build_tx_common_msg_(receiver, abort_resp);
    if (OB_FAIL(post_msg_(receiver, abort_resp))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ: {
    Ob2pcClearReqMsg clear_req;
    build_tx_common_msg_(receiver, clear_req);
    clear_req.max_commit_log_scn_ = share::SCN::max(max_2pc_commit_scn_, ctx_tx_data_.get_end_log_ts());
    if (OB_FAIL(post_msg_(receiver, clear_req))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP: {
    Ob2pcClearRespMsg clear_resp;
    build_tx_common_msg_(receiver, clear_resp);
    if (OB_FAIL(post_msg_(receiver, clear_resp))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(ret), K(*this), K(receiver), K(msg_type));
    }
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid msg type", K(msg_type), K(*this));
  }

  return ret;
}

int ObPartTransCtx::post_msg_(const ObLSID &receiver,
                              ObTxMsg &msg)
{
  TRANS_LOG(DEBUG, "post msg", K(receiver), K(msg));
  return rpc_->post_msg(receiver, msg);
}

int ObPartTransCtx::post_msg_(const ObAddr &receiver,
                              ObTxMsg &msg)
{
  return rpc_->post_msg(receiver, msg);
}

void ObPartTransCtx::build_tx_common_msg_(const ObLSID &receiver,
                                          ObTxMsg &msg)
{
  build_tx_common_msg_(receiver,
                       cluster_version_,
                       tenant_id_,
                       trans_id_,
                       addr_,
                       ls_id_,
                       cluster_id_,
                       msg);
  // fill exec_epoch && transfer_epoch
  for (int64_t idx = 0; idx < exec_info_.commit_parts_.count(); idx++) {
    if (exec_info_.commit_parts_.at(idx).ls_id_ == receiver) {
      msg.epoch_ = exec_info_.commit_parts_.at(idx).exec_epoch_;
      msg.transfer_epoch_ = exec_info_.commit_parts_.at(idx).transfer_epoch_;
    }
  }
}

void ObPartTransCtx::build_tx_common_msg_(const ObTxMsg &recv_msg,
                                          const common::ObAddr &self_addr,
                                          ObTxMsg &msg)
{
    build_tx_common_msg_(recv_msg.get_sender(),
                         recv_msg.get_cluster_version(),
                         recv_msg.get_tenant_id(),
                         recv_msg.get_trans_id(),
                         self_addr,
                         recv_msg.get_receiver(),
                         recv_msg.get_cluster_id(),
                         msg);
}

void ObPartTransCtx::build_tx_common_msg_(const ObLSID &receiver,
                                          const int64_t cluster_version,
                                          const int64_t tenant_id,
                                          const int64_t tx_id,
                                          const common::ObAddr& self_addr,
                                          const ObLSID &self_ls_id,
                                          const int64_t cluster_id,
                                          ObTxMsg &msg)
{
  msg.receiver_ = receiver;
  msg.cluster_version_ = cluster_version;
  msg.tenant_id_ = tenant_id;
  msg.tx_id_ = tx_id;
  msg.sender_addr_ = self_addr;
  msg.sender_ = self_ls_id;
  msg.request_id_ = ObTimeUtility::current_time();
  msg.cluster_id_ = cluster_id;
}

int ObPartTransCtx::post_orphan_msg_(const ObTwoPhaseCommitMsgType &msg_type,
                                     const ObTxMsg &recv_msg,
                                     const common::ObAddr &self_addr,
                                     ObITransRpc* rpc,
                                     const bool ls_deleted)
{
  int ret = OB_SUCCESS;

  switch (msg_type) {
  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ: {
    Ob2pcAbortReqMsg abort_req;
    build_tx_common_msg_(recv_msg,
                         self_addr,
                         abort_req);
    ret = rpc->post_msg(recv_msg.get_sender_addr(), abort_req);
    break;
  }

  case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP: {
    Ob2pcAbortRespMsg abort_resp;
    build_tx_common_msg_(recv_msg,
                         self_addr,
                         abort_resp);
    ret = rpc->post_msg(recv_msg.get_sender_addr(), abort_resp);
    break;
  }

  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ: {
    Ob2pcClearReqMsg clear_req;
    build_tx_common_msg_(recv_msg,
                         self_addr,
                         clear_req);
    ObTransService *trans_service = MTL(ObTransService *);
    if (OB_ISNULL(trans_service)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans service is null", K(ret));
    } else if (ls_deleted) {
      bool unused = false;
      SCN scn;
      ObITsMgr *ts_mgr = trans_service->get_ts_mgr();
      if (OB_ISNULL(ts_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts mgr is null", K(ret));
      } else if (OB_FAIL(ts_mgr->get_ts_sync(MTL_ID(), 1000000, scn, unused))) {
        TRANS_LOG(WARN, "get gts sync failed", K(ret));
      } else {
        clear_req.max_commit_log_scn_ = scn;
      }
      TRANS_LOG(INFO, "ls is deleted, use gts to build clear request", K(ret), K(clear_req));
    } else {
      if (OB_FAIL(trans_service->get_max_decided_scn(clear_req.sender_, clear_req.max_commit_log_scn_))) {
        TRANS_LOG(WARN, "get max get_max_decided_scn failed", K(ret), K(clear_req));
      }
    }
    if (OB_SUCC(ret)) {
      ret = rpc->post_msg(recv_msg.get_sender_addr(), clear_req);
    }
    break;
  }

  case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP: {
    Ob2pcClearRespMsg clear_resp;
    build_tx_common_msg_(recv_msg,
                         self_addr,
                         clear_resp);
    ret = rpc->post_msg(recv_msg.get_sender_addr(), clear_resp);
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid msg type", K(msg_type), K(recv_msg));
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "rpc post msg failed", K(ret),
        K(recv_msg), K(recv_msg.get_sender_addr()), K(msg_type));
  }
  return ret;
}

int ObPartTransCtx::post_msg(const ObTwoPhaseCommitMsgType& msg_type,
                             const int64_t participant_id)
{
  int ret = OB_SUCCESS;
  bool need_post = true;
  ObLSID receiver;

  if (participant_id >= exec_info_.participants_.count()
      && OB_C2PC_UPSTREAM_ID != participant_id
      && OB_C2PC_SENDER_ID != participant_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(participant_id), K(*this));
  } else if (OB_C2PC_UPSTREAM_ID == participant_id) {
    // We should send to real upstream
    receiver = exec_info_.upstream_;
    need_post = true;
  } else if (OB_C2PC_SENDER_ID == participant_id) {
    if (msg_2pc_cache_ != NULL) {
      // We should send to the sender(just the sender of the msg)
      receiver = msg_2pc_cache_->sender_;
      need_post = true;
    } else if (exec_info_.upstream_.is_valid()) {
      // We should retransmit the msg to the real upstream
      receiver = exec_info_.upstream_;
      need_post = true;
    } else {
      // there may be intermediate participant retransmits to the upstream which
      // disturbs the participants in this turn.
      need_post = false;
    }
  } else {
    receiver = exec_info_.participants_[participant_id];
    need_post = true;
  }

  if (OB_SUCC(ret)
      && need_post
      && OB_FAIL(post_msg_(msg_type, receiver))) {
    TRANS_LOG(WARN, "post msg failed", KR(ret), K(*this));
  }

  return ret;
}

int ObPartTransCtx::set_2pc_upstream_(const ObLSID &upstream)
{
  int ret = OB_SUCCESS;

  if (!exec_info_.upstream_.is_valid()) {
    // upstream should be fixed during each state in 2pc in order to prevent
    // the deadlock in the cycle based tree phase commit.
    exec_info_.upstream_ = upstream;
  }

  return ret;
}

int ObPartTransCtx::set_2pc_incremental_participants_(
  const ObLSArray &participants)
{
  int ret = OB_SUCCESS;

  exec_info_.incremental_participants_ = participants;

  return ret;
}

int ObPartTransCtx::set_2pc_participants_(const ObTxCommitParts& participants)
{
  int ret = OB_SUCCESS;
  if (exec_info_.participants_.count() > 0) {
    TRANS_LOG(WARN, "participants has set before", KPC(this));
  } else {
    CONVERT_COMMIT_PARTS_TO_PARTS(participants, exec_info_.participants_);
    if (FAILEDx(assign_commit_parts(exec_info_.participants_,
                                    participants))) {
      TRANS_LOG(WARN, "set participants error", K(ret), K(participants), KPC(this));
    }
  }
  return ret;
}

int ObPartTransCtx::set_2pc_request_id_(const int64_t request_id)
{
  int ret = OB_SUCCESS;

  request_id_ = request_id;

  return ret;
}

int ObPartTransCtx::merge_prepare_log_info_(const ObLSLogInfoArray &info_array)
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  for (int64_t i = 0; i < info_array.count() && OB_SUCC(ret); i++) {
    is_contain = false;
    for (int64_t j = 0; j < exec_info_.prepare_log_info_arr_.count(); j++) {
      if (exec_info_.prepare_log_info_arr_[j] == info_array[i]) {
        is_contain = true;
      } else if (exec_info_.prepare_log_info_arr_[j].get_ls_id() == info_array[i].get_ls_id()) {
        TRANS_LOG(WARN, "unexpected prepare log info from the same ls", K(j), K(i),
                  K(exec_info_.prepare_log_info_arr_[j]), K(info_array[i]));
      }
    }
    if (!is_contain) {
      if (OB_FAIL(exec_info_.prepare_log_info_arr_.push_back(info_array[i]))) {
        TRANS_LOG(WARN, "push back prepare_log_info_arr_ failed", K(ret));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::merge_prepare_log_info_(const ObLSLogInfo &prepare_info)
{
  int ret = OB_SUCCESS;

  bool is_contain = false;

  for (int64_t j = 0; j < exec_info_.prepare_log_info_arr_.count(); j++) {
    if (exec_info_.prepare_log_info_arr_[j] == prepare_info) {
      is_contain = true;
    } else if (exec_info_.prepare_log_info_arr_[j].get_ls_id() == prepare_info.get_ls_id()) {
      TRANS_LOG(WARN, "unexpected prepare log info from the same ls", K(j),
                K(exec_info_.prepare_log_info_arr_[j]), K(prepare_info));
    }
  }
  if (!is_contain) {
    if (OB_FAIL(exec_info_.prepare_log_info_arr_.push_back(prepare_info))) {
      TRANS_LOG(WARN, "push back prepare_log_info_arr_ failed", K(ret));
    }
  }

  return ret;
}

int ObPartTransCtx::update_2pc_prepare_version_(const SCN &prepare_version)
{
  int ret = OB_SUCCESS;

  exec_info_.prepare_version_ = SCN::max(prepare_version, exec_info_.prepare_version_);

  return ret;
}

int ObPartTransCtx::set_2pc_commit_version_(const SCN &commit_version)
{
  int ret = OB_SUCCESS;

  if (ctx_tx_data_.get_commit_version() >= commit_version) {
    // do nothing
  } else if (OB_FAIL(ctx_tx_data_.set_commit_version(commit_version))) {
    TRANS_LOG(WARN, "set tx data commit version failed", K(ret));
  }

  return ret;
}

int ObPartTransCtx::apply_2pc_msg_(const ObTwoPhaseCommitMsgType msg_type)
{
  int ret = OB_SUCCESS;
  ObTwoPhaseCommitMsgType cache_msg_type = switch_msg_type_(msg_2pc_cache_->type_);

  if (OB_ISNULL(msg_2pc_cache_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "empty 2pc msg", K(ret));
  } else if (cache_msg_type != msg_type
             && (ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP != msg_type
                 || ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP != cache_msg_type)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "unexpected 2pc msg type", K(ret), K(msg_type), KPC(msg_2pc_cache_));
  } else {
    switch (cache_msg_type) {
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ: {
      const Ob2pcPrepareRedoReqMsg &msg = *(static_cast<const Ob2pcPrepareRedoReqMsg *>(msg_2pc_cache_));
      if (FALSE_IT(set_trans_type_(TransType::DIST_TRANS))) {
      } else if (OB_FAIL(set_2pc_upstream_(msg.upstream_))) {
        TRANS_LOG(WARN, "set coordinator failed", KR(ret), K(msg), K(*this));
      } else if (OB_FAIL(set_app_trace_info_(msg.app_trace_info_))) {
        TRANS_LOG(WARN, "set app trace info failed", KR(ret), K(msg), K(*this));
      } else {
        exec_info_.xid_ = msg.xid_;
        exec_info_.is_sub2pc_ = true;
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ: {
      if (!is_sub2pc() && TX_MSG_TYPE::TX_2PC_PREPARE_VERSION_REQ == msg_2pc_cache_->type_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpect tx flag", KR(ret), KPC(this));
      } else if (is_sub2pc()) {
        // prepare version for xa trans
        const Ob2pcPrepareVersionReqMsg &msg = *(static_cast<const Ob2pcPrepareVersionReqMsg *>(msg_2pc_cache_));
        if (OB_FAIL(set_2pc_upstream_(msg.upstream_))) {
          TRANS_LOG(WARN, "set coordinator failed", KR(ret), K(msg), K(*this));
        }
        // other actions has been done in entrance function handle_tx_2pc_prepare_version_req
      } else {
        const Ob2pcPrepareReqMsg &msg = *(static_cast<const Ob2pcPrepareReqMsg *>(msg_2pc_cache_));

        if (FALSE_IT(set_trans_type_(TransType::DIST_TRANS))) {
        } else if (OB_FAIL(set_2pc_upstream_(msg.upstream_))) {
          TRANS_LOG(WARN, "set coordinator failed", KR(ret), K(msg), K(*this));
        } else if (OB_FAIL(set_app_trace_info_(msg.app_trace_info_))) {
          TRANS_LOG(WARN, "set app trace info failed", KR(ret), K(msg), K(*this));
        }
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP: {
      if (!is_sub2pc() && TX_MSG_TYPE::TX_2PC_PREPARE_VERSION_RESP == msg_2pc_cache_->type_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpect tx flag", KR(ret), KPC(this));
      } else if (is_sub2pc()) {
        // prepare version for xa trans
        // these actions has been done in entrance function handle_tx_2pc_prepare_version_resp
      } else {
        // if modify logic here, please check codes in handle_tx_2pc_prepare_redo (version)
        const Ob2pcPrepareRespMsg &msg = *(static_cast<const Ob2pcPrepareRespMsg *>(msg_2pc_cache_));

        if (OB_FAIL(update_2pc_prepare_version_(msg.prepare_version_))) {
          TRANS_LOG(WARN, "update prepare version failed", KR(ret), K(msg), K(*this));
        } else if (OB_FAIL(merge_prepare_log_info_(msg.prepare_info_array_))) {
          TRANS_LOG(WARN, "merge prepare log info failed", K(ret));
        }
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ: {

      const Ob2pcPreCommitReqMsg &msg =
          *(static_cast<const Ob2pcPreCommitReqMsg *>(msg_2pc_cache_));

      if (OB_FAIL(set_2pc_upstream_(msg.sender_))) {
        TRANS_LOG(WARN, "set coordinator failed", KR(ret), K(msg), K(*this));
      } else if (OB_FAIL(set_2pc_commit_version_(msg.commit_version_))) {
        TRANS_LOG(WARN, "set commit version failed", KR(ret), K(msg), KPC(this));
      }

      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP: {

      const Ob2pcPreCommitRespMsg &msg =
          *(static_cast<const Ob2pcPreCommitRespMsg *>(msg_2pc_cache_));

      if (OB_FAIL(update_2pc_prepare_version_(msg.commit_version_))) {
        TRANS_LOG(WARN, "update prepare version failed", KR(ret), K(msg), K(*this));
      } else if (OB_FAIL(set_2pc_commit_version_(msg.commit_version_))) {
        TRANS_LOG(WARN, "set commit version failed", KR(ret), K(msg), KPC(this));
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ: {

      const Ob2pcCommitReqMsg &msg = *(static_cast<const Ob2pcCommitReqMsg *>(msg_2pc_cache_));

      if (OB_FAIL(set_2pc_upstream_(msg.sender_))) {
        TRANS_LOG(WARN, "set coordinator failed", KR(ret), K(msg), K(*this));
      } else if (OB_FAIL(set_2pc_commit_version_(msg.commit_version_))) {
        TRANS_LOG(WARN, "set commit version failed", KR(ret), K(msg), K(*this));
      } else if (OB_FAIL(coord_prepare_info_arr_.assign(msg.prepare_info_array_))) {
        TRANS_LOG(WARN, "assign prepare_log_info_arr_ failed", K(ret));
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP: {

      const Ob2pcCommitRespMsg &msg = *(static_cast<const Ob2pcCommitRespMsg *>(msg_2pc_cache_));
      max_2pc_commit_scn_ = share::SCN::max(msg.commit_log_scn_, max_2pc_commit_scn_);
      if (OB_FAIL(set_2pc_commit_version_(msg.commit_version_))) {
        TRANS_LOG(WARN, "set commit version failed", KR(ret), K(msg), K(*this));
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ: {

      const Ob2pcAbortReqMsg &msg = *(static_cast<const Ob2pcAbortReqMsg *>(msg_2pc_cache_));

      if (msg.upstream_.is_valid() && // upstream may be invalid for orphan msg
          OB_FAIL(set_2pc_upstream_(msg.upstream_))) {
        TRANS_LOG(WARN, "set upstream failed", KR(ret), K(msg), K(*this));
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP: {
      const Ob2pcAbortRespMsg &msg = *(static_cast<const Ob2pcAbortRespMsg *>(msg_2pc_cache_));

      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ: {
      const Ob2pcClearReqMsg &msg = *(static_cast<const Ob2pcClearReqMsg *>(msg_2pc_cache_));
      if (CLUSTER_VERSION_4_1_0_0 <= GET_MIN_CLUSTER_VERSION()
          && (msg.max_commit_log_scn_ < max_2pc_commit_scn_
              || msg.max_commit_log_scn_ < ctx_tx_data_.get_end_log_ts())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected max commit log scn in clear request", K(ret), KPC(this));
      } else if (OB_FAIL(set_2pc_upstream_(msg.sender_))) {
        TRANS_LOG(WARN, "set coordinator failed", KR(ret), K(msg), K(*this));
      } else {
        max_2pc_commit_scn_ = share::SCN::max(msg.max_commit_log_scn_, max_2pc_commit_scn_);
      }
      break;
    }
    case ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP: {
      const Ob2pcClearRespMsg &msg = *(static_cast<const Ob2pcClearRespMsg *>(msg_2pc_cache_));

      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unkown 2pc msg", K(ret), K(msg_type), KPC(msg_2pc_cache_), KPC(this));
      break;
    }
    }
  }
  return ret;
}

int ObPartTransCtx::handle_tx_2pc_prepare_req(const Ob2pcPrepareReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  exec_info_.trans_type_ = TransType::DIST_TRANS;

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(set_app_trace_info_(msg.app_trace_info_))) {
    TRANS_LOG(WARN, "set app trace info failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_req(msg_type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  if (OB_SUCC(ret)) {
    part_trans_action_ = ObPartTransAction::COMMIT;
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DELAY_TX_COMMIT);

int ObPartTransCtx::handle_tx_2pc_prepare_resp(const Ob2pcPrepareRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_NOT_INIT == ERRSIM_DELAY_TX_COMMIT) {
    return ret;
  } else if (ERRSIM_DELAY_TX_COMMIT < int(-9999)) {
    // reject trans_id and tenant id to contorl trans sate
    ret = process_errsim_for_standby_read_(int(ERRSIM_DELAY_TX_COMMIT),
                                           OB_NOT_INIT);
    if (OB_FAIL(ret)) {
      return ret;
      TRANS_LOG(INFO, "reject delay tx commit",
                K(ret), K(ERRSIM_DELAY_TX_COMMIT));
    }
  }

  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    if (0 == exec_info_.participants_.count()) {
      // It may be possible that when the coordinator switches to the new
      // leader, compensates the abort log while it may have already broadcasted
      // the prepare requests by the old leader. And during the paxos of the
      // abort log, it may receive the prepare response and has no participants
      // list to handle the response, so we need tolerate it here.
      ret = OB_SUCCESS;
      TRANS_LOG(INFO, "find participant failed", KR(ret), K(msg), K(*this));
    } else {
      TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), K(*this));
    }
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

// for participant
// when coordinator receives sub prepare (xa prepare) from original scheduler,
// it sends prepare redo request to all participants.
// when participant receives prepare redo request, it only persists redo and commit info.
// if succeed, return prepare redo response.
int ObPartTransCtx::handle_tx_2pc_prepare_redo_req(const Ob2pcPrepareRedoReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());

  if (!is_2pc_logging()) {
    exec_info_.xid_ = msg.xid_;
    exec_info_.is_sub2pc_ = true;
    msg_2pc_cache_ = &msg;
    if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
      TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
    } else if (sub_state_.is_force_abort()) {
      if (OB_FAIL(compensate_abort_log_())) {
        TRANS_LOG(WARN, "compensate abort log failed", K(ret), K(ls_id_), K(trans_id_),
                  K(get_downstream_state()), K(get_upstream_state()), K(sub_state_));
      } else {
        ret = OB_TRANS_KILLED;
      }
    } else if (OB_FAIL(handle_2pc_req(msg_type))) {
      TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), K(*this));
    }

    if (OB_SUCC(ret)) {
      part_trans_action_ = ObPartTransAction::COMMIT;
    }
    msg_2pc_cache_ = nullptr;
  }

  TRANS_LOG(INFO, "handle prepare redo request", KR(ret), K(msg));
  return ret;
}

// for coordinator
// when coordinator recieves prepare redo response, check whether it receives responses
// from all participants.
// if yes, it returns sub prepare (xa prepare) response to original scheduler.
int ObPartTransCtx::handle_tx_2pc_prepare_redo_resp(const Ob2pcPrepareRedoRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;

  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    if (0 == exec_info_.participants_.count()) {
      // It may be possible that when the coordinator switches to the new
      // leader, compensates the abort log while it may have already broadcasted
      // the prepare redo requests by the old leader. And during the paxos of
      // the abort log, it may receive the prepare redo response and has no
      // participants list to handle the response, so we need tolerate it here.
      ret = OB_SUCCESS;
      TRANS_LOG(INFO, "find participant failed", KR(ret), K(msg), K(*this));
    } else {
      TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), K(*this));
    }
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), K(*this));
  }

  return ret;
}

// for participant
// when coordinator receives sub commit/rollback (xa commit/rollback) from tmp scheduler,
// it sends prepare version request to all participants.
// when participant receives prepare version request, it continues 2pc prepare.
int ObPartTransCtx::handle_tx_2pc_prepare_version_req(const Ob2pcPrepareVersionReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_req(msg_type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), K(*this));
  }

  if (OB_SUCC(ret)) {
    part_trans_action_ = ObPartTransAction::COMMIT;
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

// for coordinator
// when coordinator recieves prepare version response, check whether it receives responses
// from all participants.
// if yes, it returns sub commit/rollback (xa commit/rollback) response to tmp scheduler.
int ObPartTransCtx::handle_tx_2pc_prepare_version_resp(const Ob2pcPrepareVersionRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;
  msg_2pc_cache_ = &msg;

  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(update_2pc_prepare_version_(msg.prepare_version_))) {
    TRANS_LOG(WARN, "update prepare version failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(merge_prepare_log_info_(msg.prepare_info_array_))){
    TRANS_LOG(WARN, "merge prepare log info failed",K(ret));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_pre_commit_req(const Ob2pcPreCommitReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_NOT_SUPPORTED == ERRSIM_DELAY_TX_COMMIT) {
    return ret;
  } else if (ERRSIM_DELAY_TX_COMMIT < int(-9999)) {
    // reject trans_id and tenant id to contorl trans sate
    ret = process_errsim_for_standby_read_(int(ERRSIM_DELAY_TX_COMMIT),
                                           OB_NOT_SUPPORTED);
    if (OB_FAIL(ret)) {
      return ret;
      TRANS_LOG(INFO, "reject delay tx commit when pre commit",
                K(ret), K(ERRSIM_DELAY_TX_COMMIT));
    }
  }

  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), KPC(this));
  } else if (OB_FAIL(handle_2pc_req(msg_type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), KPC(this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_pre_commit_resp(const Ob2pcPreCommitRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), KPC(this));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), KPC(this));
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), KPC(this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_commit_req(const Ob2pcCommitReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_req(msg_type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_commit_resp(const Ob2pcCommitRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_abort_req(const Ob2pcAbortReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_req(msg_type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_abort_resp(const Ob2pcAbortRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    if (0 == exec_info_.participants_.count()) {
      // It may be possible that when the coordinator switches to the new
      // leader, compensates the abort log while it may have already broadcasted
      // the abort requests by the old leader. And during the paxos of the
      // abort log, it may receive the abort response and has no participants
      // list to handle the response, so we need tolerate it here.
      ret = OB_SUCCESS;
      TRANS_LOG(INFO, "find participant failed", KR(ret), K(msg), K(*this));
    } else {
      TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), K(*this));
    }
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_clear_req(const Ob2pcClearReqMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());

  msg_2pc_cache_ = &msg;
  if (is_exiting()) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(WARN, "trans ctx is exiting", K(ret), K(msg), KPC(this));
  } else if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_req(msg_type))) {
    TRANS_LOG(WARN, "handle 2pc request failed", KR(ret), K(msg), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_2pc_clear_resp(const Ob2pcClearRespMsg &msg)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTwoPhaseCommitMsgType msg_type = switch_msg_type_(msg.get_msg_type());
  int64_t participant_id = INT64_MAX;

  msg_2pc_cache_ = &msg;
  if (OB_FAIL(set_2pc_request_id_(msg.request_id_))) {
    TRANS_LOG(WARN, "set request id failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(find_participant_id_(msg.sender_, participant_id))) {
    TRANS_LOG(ERROR, "find participant failed", KR(ret), K(msg), K(*this));
  } else if (OB_FAIL(handle_2pc_resp(msg_type, participant_id))) {
    TRANS_LOG(WARN, "handle 2pc response failed", KR(ret), K(msg), K(participant_id), K(*this));
  }
  msg_2pc_cache_ = nullptr;

  return ret;
}

int ObPartTransCtx::handle_tx_orphan_2pc_msg(const ObTxMsg &recv_msg,
                                             const common::ObAddr& self_addr,
                                             ObITransRpc* rpc,
                                             const bool ls_deleted)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "handle_tx_orphan_2pc_msg", K(recv_msg));

  ObTwoPhaseCommitMsgType recv_msg_type = switch_msg_type_(recv_msg.get_msg_type());
  ObTwoPhaseCommitMsgType send_msg_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_UNKNOWN;

  bool need_ack = true;
  if (is_2pc_request_msg(recv_msg_type)) {
    if (OB_FAIL(ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_req(recv_msg_type,
                                                                  send_msg_type,
                                                                  need_ack))) {
      TRANS_LOG(WARN, "handle orphan 2pc request failed", KR(ret), K(recv_msg));
    }
  } else if (is_2pc_response_msg(recv_msg_type)) {
    if (OB_FAIL(ObTxCycleTwoPhaseCommitter::handle_orphan_2pc_resp(recv_msg_type,
                                                                   send_msg_type,
                                                                   need_ack))) {
      TRANS_LOG(WARN, "handle orphan 2pc response failed", KR(ret), K(recv_msg));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_FAIL(ret)) {
  } else if (need_ack && OB_FAIL(post_orphan_msg_(send_msg_type,
                                                  recv_msg,
                                                  self_addr,
                                                  rpc,
                                                  ls_deleted))) {
    TRANS_LOG(WARN, "post_orphan_msg_ failed", KR(ret), K(recv_msg), K(send_msg_type));
  }
  return ret;
}

ObTwoPhaseCommitMsgType ObPartTransCtx::switch_msg_type_(const int16_t msg_type)
{
  ObTwoPhaseCommitMsgType ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_UNKNOWN;

  switch (msg_type) {
  case TX_2PC_PREPARE_REDO_REQ:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ;
    break;
  case TX_2PC_PREPARE_REDO_RESP:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_RESP;
    break;
  case TX_2PC_PREPARE_VERSION_REQ:
  case TX_2PC_PREPARE_REQ:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ;
    break;
  case TX_2PC_PREPARE_VERSION_RESP:
  case TX_2PC_PREPARE_RESP:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP;
    break;
  case TX_2PC_PRE_COMMIT_REQ:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_REQ;
    break;
  case TX_2PC_PRE_COMMIT_RESP:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_PRE_COMMIT_RESP;
    break;
  case TX_2PC_COMMIT_REQ:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ;
    break;
  case TX_2PC_COMMIT_RESP:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP;
    break;
  case TX_2PC_ABORT_REQ:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_REQ;
    break;
  case TX_2PC_ABORT_RESP:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP;
    break;
  case TX_2PC_CLEAR_REQ:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ;
    break;
  case TX_2PC_CLEAR_RESP:
    ret_type = ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_RESP;
    break;
  default:
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid msg type", K(msg_type));
  }

  return ret_type;
}

int ObPartTransCtx::post_tx_commit_resp_(const int status)
{
  int ret = OB_SUCCESS;
  bool has_skip = false, use_rpc = true;
  const auto commit_version = ctx_tx_data_.get_commit_version();
  // scheduler on this server, direct call
  if (exec_info_.scheduler_ == addr_) {
    use_rpc = false;
    if (!has_callback_scheduler_()) {
      if (OB_FAIL(defer_callback_scheduler_(status, commit_version))) {
        TRANS_LOG(WARN, "report tx commit result fail", K(ret), K(status), KPC(this));
      } else {
#ifndef NDEBUG
        TRANS_LOG(INFO, "report tx commit result to local scheduler succeed", K(status), KP(this));
#endif
      }
    } else if (commit_cb_.get_cb_ret() == status) {
      has_skip = true;
    } else {
      // maybe has been callbacked due to switch to follower
      // the callback status is not final commit status:
      //   either OB_NOT_MASTER or OB_SWITCH_TO_FOLLOWER_GRACEFULLY
      // in these case should callback the scheduler with final status again
      use_rpc = true;
    }
  }
  if (use_rpc) {
    ObTxCommitRespMsg msg;
    build_tx_common_msg_(SCHEDULER_LS, msg);
    msg.commit_version_ = commit_version;
    msg.ret_ = status;
    if (OB_FAIL(post_msg_(exec_info_.scheduler_, msg))) {
      TRANS_LOG(WARN, "rpc post msg failed", K(*this), K(msg));
    } else {
#ifndef NDEBUG
      TRANS_LOG(INFO, "post tx commit resp successfully", K(status),
                K(msg), K(exec_info_.scheduler_), K(*this));
#endif
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, response_scheduler,
                      OB_ID(ret), ret,
                      OB_ID(tag1), has_skip,
                      OB_ID(tag2), use_rpc,
                      OB_ID(status), status,
                      OB_ID(commit_version), commit_version);
  return ret;
}

int ObPartTransCtx::post_tx_sub_prepare_resp_(const int status)
{
  int ret = OB_SUCCESS;
  ObTxSubPrepareRespMsg msg;
  build_tx_common_msg_(ls_id_, msg);
  msg.ret_ = status;
  if (OB_FAIL(post_msg_(exec_info_.scheduler_, msg))) {
    TRANS_LOG(WARN, "rpc post msg failed", K(*this), K(msg));
  } else {
    TRANS_LOG(INFO, "post sub prepare resp successfully", K(status),
              K(msg), K(exec_info_.scheduler_), K(*this));
  }
  return ret;
}

int ObPartTransCtx::post_tx_sub_commit_resp_(const int status)
{
  int ret = OB_SUCCESS;
  ObTxSubCommitRespMsg msg;
  build_tx_common_msg_(ls_id_, msg);
  msg.ret_ = status;
  if (OB_FAIL(post_msg_(tmp_scheduler_, msg))) {
    TRANS_LOG(WARN, "rpc post msg failed", K(*this), K(msg));
  } else {
    TRANS_LOG(INFO, "post sub commit resp successfully", K(status),
              K(msg), K_(tmp_scheduler), K(*this));
  }
  return ret;
}

int ObPartTransCtx::post_tx_sub_rollback_resp_(const int status)
{
  int ret = OB_SUCCESS;
  ObTxSubRollbackRespMsg msg;
  build_tx_common_msg_(ls_id_, msg);
  msg.ret_ = status;
  if (OB_FAIL(post_msg_(tmp_scheduler_, msg))) {
    TRANS_LOG(WARN, "rpc post msg failed", K(*this), K(msg));
  } else {
    TRANS_LOG(INFO, "post sub rollback resp successfully", K(status),
              K(msg), K_(tmp_scheduler), K(*this));
  }
  return ret;
}
} // transaction
} // oceanbase
