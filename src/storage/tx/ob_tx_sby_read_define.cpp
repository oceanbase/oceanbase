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
#include "storage/tx/ob_tx_sby_read_define.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace transaction
{

bool ObTxSbyStateInfo::is_valid() const { return part_id_.is_valid(); }

bool ObTxSbyStateInfo::is_empty() const { return is_valid() && !src_addr_.is_valid(); }

bool ObTxSbyStateInfo::is_useful() const
{
  return is_confirmed() || (tx_state_ == ObTxState::INIT && ls_readable_scn_.is_valid_and_not_min())
         || (tx_state_ == ObTxState::UNKNOWN && ls_readable_scn_.is_valid_and_not_min())
         || (tx_state_ == ObTxState::PREPARE && trans_version_.is_valid_and_not_min());
}

bool ObTxSbyStateInfo::is_confirmed() const
{
  return is_valid() && src_addr_.is_valid()
         && ((tx_state_ == ObTxState::COMMIT && trans_version_.is_valid_and_not_min())
             || tx_state_ == ObTxState::ABORT || flag_.flag_bit_.ls_not_exist_);
}

void ObTxSbyStateInfo::fill(const share::ObLSID &ls_id,
                            const ObTxState &tx_state,
                            const share::SCN &trans_version,
                            const share::SCN ls_readable_scn,
                            const share::SCN &snapshot,
                            const bool is_transfer_prepare)
{
  if (!ls_id.is_valid() || ls_id != part_id_) {
    // do nothing
  } else {
    tx_state_ = tx_state;
    trans_version_ = trans_version;
    ls_readable_scn_ = ls_readable_scn;
    max_applied_snapshot_ = snapshot;
    src_addr_ = GCTX.self_addr();
    flag_.flag_bit_.infer_by_transfer_prepare_ = is_transfer_prepare;
  }
}

void ObTxSbyStateInfo::set_ls_not_exist(const share::ObLSID &ls_id)
{
  if (!ls_id.is_valid() || ls_id != part_id_) {
    // do nothing
  } else {
    reset();
    part_id_ = ls_id;
    flag_.flag_bit_.ls_not_exist_ = true;
  }
}

int ObTxSbyStateInfo::get_confirmed_state(ObTxCommitData::TxDataState &infer_trx_state,
                                          share::SCN &infer_trx_version) const
{
  int ret = OB_SUCCESS;

  if (!is_confirmed()) {
    ret = OB_EAGAIN;
  } else if (flag_.flag_bit_.ls_not_exist_) {
    ret = OB_SNAPSHOT_DISCARDED;
    TRANS_LOG(WARN, "ls not exist", K(ret), KPC(this));
  } else if (tx_state_ == ObTxState::COMMIT) {
    infer_trx_state = ObTxCommitData::TxDataState::COMMIT;
    infer_trx_version = trans_version_;
  } else if (tx_state_ == ObTxState::ABORT) {
    infer_trx_state = ObTxCommitData::TxDataState::ABORT;
    infer_trx_version.reset();
  }

  return ret;
}

int ObTxSbyStateInfo::infer_by_userful_info(const share::SCN read_snapshot,
                                            const bool filter_unreadable_prepare_trx,
                                            ObTxCommitData::TxDataState &infer_trx_state,
                                            share::SCN &infer_trx_version) const
{
  int ret = OB_SUCCESS;

  if (!is_useful()) {
    ret = OB_EAGAIN;
  } else if (flag_.flag_bit_.ls_not_exist_) {
    ret = OB_SNAPSHOT_DISCARDED;
    TRANS_LOG(WARN, "ls not exist", K(ret), KPC(this));
  } else if ((tx_state_ == ObTxState::INIT
              && read_snapshot <= ls_readable_scn_) // < prepare_version
             || (tx_state_ == ObTxState::PREPARE && read_snapshot < trans_version_)) {
    infer_trx_state = ObTxCommitData::TxDataState::RUNNING;
    infer_trx_version = share::SCN::max_scn();
  } else if (tx_state_ == ObTxState::UNKNOWN) {
    ret = OB_SNAPSHOT_DISCARDED;
    TRANS_LOG(WARN, "unknown sby trx state", K(ret), KPC(this));
  } else {
    ret = OB_EAGAIN;
  }

  // if (OB_FAIL(ret)) {
  //   TRANS_LOG(INFO, "get useful info from state info failed", K(ret), K(read_snapshot),
  //             K(infer_trx_state), K(infer_trx_version), KPC(this));
  // }

  return ret;
}

int ObTxSbyStateInfo::try_to_update(const ObTxSbyStateInfo &other,
                                    const bool allow_update_by_other_ls)
{
  int ret = OB_SUCCESS;

  share::ObLSID tmp_ls_id = part_id_;
  // TRANS_LOG(INFO, "Before update", K(ret), KPC(this), K(other), K(allow_update_by_other_ls));

  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "unexpected other state_info", K(ret), KPC(this), K(other));
  } else if (!is_valid()) {
    *this = other;
  } else if (!allow_update_by_other_ls && part_id_ != other.part_id_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "update by the other ls", K(ret), KPC(this), K(other));
  } else if (is_confirmed()) {
    // do nothing
  } else if (other.is_confirmed()) {
    *this = other;
    part_id_ = tmp_ls_id;
  } else if ((!is_useful()) && other.is_useful()) {
    *this = other;
    part_id_ = tmp_ls_id;
  } else if (other.max_applied_snapshot_ > max_applied_snapshot_) {
    *this = other;
    part_id_ = tmp_ls_id;
  } else if (other.max_applied_snapshot_ == max_applied_snapshot_
             && (other.flag_.flag_bit_.infer_by_root_
                 || ls_readable_scn_ < other.ls_readable_scn_)) {
    *this = other;
    part_id_ = tmp_ls_id;
  }

  // TRANS_LOG(INFO, "after update", K(ret), KPC(this), K(other), K(allow_update_by_other_ls));

  return ret;
}

void ObTxSbyStateInfo::reset()
{
  part_id_.reset();
  tx_state_ = ObTxState::UNKNOWN;
  trans_version_ = share::SCN::invalid_scn();
  ls_readable_scn_ = share::SCN::invalid_scn();
  max_applied_snapshot_ = share::SCN::invalid_scn();
  src_addr_.reset();
  flag_.flag_bit_.reset();
}

OB_SERIALIZE_MEMBER(ObTxSbyStateInfo,
                    part_id_,
                    tx_state_,
                    trans_version_,
                    ls_readable_scn_,
                    max_applied_snapshot_,
                    src_addr_,
                    flag_.flag_val_)

bool ObTxSbyBaseMsg::is_valid() const
{
  return is_valid_sby_msg_type(msg_type_) && is_valid_tenant_id(tenant_id_) && cluster_version_ > 0
         && tx_id_.is_valid() && msg_snapshot_.is_valid_and_not_min() && src_ls_id_.is_valid()
         && src_addr_.is_valid() && dst_ls_id_.is_valid() && dst_addr_.is_valid();
}

bool ObTxSbyAskUpstreamReq::is_valid() const
{
  return ObTxSbyBaseMsg::is_valid() && ori_ls_id_.is_valid() && ori_addr_.is_valid();
}

bool ObTxSbyAskDownstreamReq::is_valid() const
{
  return ObTxSbyBaseMsg::is_valid() && root_ls_id_.is_valid() && root_addr_.is_valid();
}

// bool ObTxSbyAskReplicaReq::is_valid() const { return ObTxSbyBaseMsg::is_valid(); }

bool ObTxSbyStateResultMsg::is_valid() const
{
  return ObTxSbyBaseMsg::is_valid() && src_state_info_.is_valid();
}

OB_SERIALIZE_MEMBER(ObTxSbyBaseMsg,
                    msg_type_,
                    tenant_id_,
                    cluster_version_,
                    tx_id_,
                    msg_snapshot_,
                    src_ls_id_,
                    src_addr_,
                    dst_ls_id_,
                    dst_addr_)

OB_SERIALIZE_MEMBER_INHERIT(ObTxSbyAskUpstreamReq, ObTxSbyBaseMsg, ori_ls_id_, ori_addr_)
OB_SERIALIZE_MEMBER_INHERIT(ObTxSbyAskDownstreamReq,
                            ObTxSbyBaseMsg,
                            root_ls_id_,
                            root_addr_,
                            exec_epoch_,
                            transfer_epoch_)
// OB_SERIALIZE_MEMBER_INHERIT(ObTxSbyAskReplicaReq, ObTxSbyBaseMsg)
OB_SERIALIZE_MEMBER_INHERIT(ObTxSbyStateResultMsg,
                            ObTxSbyBaseMsg,
                            src_state_info_,
                            downstream_parts_,
                            from_root_)

} // namespace transaction

namespace obrpc
{

#define TX_SBY_PROCESS_DEFINE(name)                                                  \
  int Ob##name##P::process()                                                         \
  {                                                                                  \
    int ret = OB_SUCCESS;                                                            \
    if (!arg_.is_valid()) {                                                          \
      ret = OB_INVALID_ARGUMENT;                                                     \
      TRANS_LOG(WARN, "invalid msg", K(ret), K(arg_));                               \
    } else if (OB_FAIL(MTL(transaction::ObTransService *)->handle_sby_msg(&arg_))) { \
      TRANS_LOG(WARN, "handle sby msg failed", K(ret), K(arg_));                     \
    }                                                                                \
    return ret;                                                                      \
  }

TX_SBY_PROCESS_DEFINE(TxSbyAskUpstreamReq)
TX_SBY_PROCESS_DEFINE(TxSbyAskDownstreamReq)
// TX_SBY_PROCESS_DEFINE(TxSbyAskReplicaReq)
TX_SBY_PROCESS_DEFINE(TxSbyStateResult)

int ObTxSbyRpc::init(rpc::frame::ObReqTransport *req_transport,
                     const oceanbase::common::ObAddr &addr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(proxy_.init(req_transport, addr))) {
    TRANS_LOG(WARN, "init sby rpc proxy failed", K(ret));
  }

  return ret;
}
} // namespace obrpc

namespace transaction
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(ERRSIM_STANDBY_CTX_NOT_EXIST)
#endif
int ObTransService::handle_sby_msg(const ObTxSbyBaseMsg *sby_msg)
{
  int ret = OB_SUCCESS;
  bool need_check_tx_data = false;
  share::ObLSID err_resp_ls_id;
  ObAddr err_resp_addr;
  if (OB_ISNULL(sby_msg) || !sby_msg->is_valid() || sby_msg->tenant_id_ != tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id_), KPC(sby_msg));
  } else {
    const ObTxSbyMsgType msg_type = sby_msg->msg_type_;
    share::SCN ls_replica_readable_scn;
    ls_replica_readable_scn.set_invalid();
    ObLSHandle handle;

    ObPartTransCtx *tx_ctx = nullptr;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(sby_msg->dst_ls_id_, handle, ObLSGetMod::TRANS_MOD))) {
      TRANS_LOG(WARN, "get log stream failed", K(ret), KPC(sby_msg));
    } else if (OB_ISNULL(handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "get ls failed", K(ret));
    } else if (OB_FAIL(handle.get_ls()->get_ls_replica_readable_scn(ls_replica_readable_scn))) {
      TRANS_LOG(WARN, "get ls replica readable scn failed", K(ret), KPC(sby_msg));
#ifdef ERRSIM
    } else if (ERRSIM_STANDBY_CTX_NOT_EXIST == OB_TRANS_CTX_NOT_EXIST) {
      ret = OB_TRANS_CTX_NOT_EXIST;
      TRANS_LOG(INFO, "[ERRSIM] standby ctx not exist", K(ret), KPC(sby_msg));
#endif
    } else if (OB_FAIL(get_tx_ctx_for_standby_(sby_msg->dst_ls_id_, sby_msg->tx_id_, tx_ctx))) {
      TRANS_LOG(WARN, "get tx_ctx for standby failed", K(ret), KPC(sby_msg));
    }

    // When the ctx is not existed, do not query other replicas, as it is inconvenient to confirm
    // that all replicas are in an unknown state, which could easily disrupt the overall process.

    switch (msg_type) {
    case ObTxSbyMsgType::ASK_UPSTREAM: {
      const ObTxSbyAskUpstreamReq *msg = static_cast<const ObTxSbyAskUpstreamReq *>(sby_msg);
      if (OB_FAIL(ret)) {
        need_check_tx_data = true;
        err_resp_ls_id = msg->ori_ls_id_;
        err_resp_addr = msg->ori_addr_;
      } else if (OB_FAIL(tx_ctx->handle_sby_ask_upstream(*msg, ls_replica_readable_scn))) {
        TRANS_LOG(WARN, "handle ask upstream request failed", K(ret), KPC(msg),
                  K(ls_replica_readable_scn), KPC(tx_ctx));
      }
      break;
    }
    case ObTxSbyMsgType::ASK_DOWNSTREAM: {
      const ObTxSbyAskDownstreamReq *msg = static_cast<const ObTxSbyAskDownstreamReq *>(sby_msg);
      if (OB_FAIL(ret)) {
        need_check_tx_data = true;
        err_resp_ls_id = msg->root_ls_id_;
        err_resp_addr = msg->root_addr_;
      } else if (OB_FAIL(tx_ctx->handle_sby_ask_downstream(*msg, ls_replica_readable_scn))) {
        TRANS_LOG(WARN, "handle ask downstream request failed", K(ret), KPC(msg),
                  K(ls_replica_readable_scn), KPC(tx_ctx));
      }
      break;
    }
    case ObTxSbyMsgType::ASK_REPLICA: {
      // const ObTxSbyAskReplicaReq *msg = static_cast<const ObTxSbyAskReplicaReq *>(sby_msg);
      // if (OB_FAIL(ret)) {
      //   need_check_tx_data = true;
      //   err_resp_ls_id = msg->src_ls_id_;
      //   err_resp_addr = msg->src_addr_;
      // } else {
      // }
      ret = OB_NOT_SUPPORTED;
      break;
    }
    case ObTxSbyMsgType::STATE_RESULT: {
      const ObTxSbyStateResultMsg *msg = static_cast<const ObTxSbyStateResultMsg *>(sby_msg);
      if (OB_FAIL(ret)) {
        need_check_tx_data = false;
        err_resp_ls_id = msg->src_ls_id_;
        err_resp_addr = msg->src_addr_;
      } else if (OB_FAIL(tx_ctx->handle_sby_state_result(*msg))) {
        TRANS_LOG(WARN, "handle state_result msg failed", K(ret), KPC(msg),
                  K(ls_replica_readable_scn), KPC(tx_ctx));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Invalid Msg Type", K(ret), K(msg_type), KPC(sby_msg));
      break;
    }
    }
    if (OB_NOT_NULL(tx_ctx)) {
      revert_tx_ctx_(tx_ctx);
    }

    {
      int tmp_ret = OB_SUCCESS;
      ObTxSbyStateResultMsg orphan_resp;
      orphan_resp.src_state_info_.reset();
      if (OB_TRANS_CTX_NOT_EXIST == ret && need_check_tx_data) {
        if (OB_TMP_FAIL(get_sby_tx_state_from_tx_table(
                sby_msg->dst_ls_id_, sby_msg->tx_id_, ls_replica_readable_scn,
                sby_msg->msg_snapshot_, orphan_resp.src_state_info_))) {
          TRANS_LOG(WARN, "get standby tx state from tx table failed", K(ret), K(tmp_ret),
                    KPC(sby_msg), K(orphan_resp));
        } else if (orphan_resp.src_state_info_.is_valid()
                   && orphan_resp.src_state_info_.tx_state_ == ObTxState::UNKNOWN
                   && (!orphan_resp.src_state_info_.ls_readable_scn_.is_valid_and_not_min()
                       || !orphan_resp.src_state_info_.max_applied_snapshot_.is_valid_and_not_min()
                       || orphan_resp.src_state_info_.ls_readable_scn_ < sby_msg->msg_snapshot_)) {
          orphan_resp.src_state_info_.reset();

          tmp_ret = ask_sby_state_info_from_replicas(sby_msg);
          TRANS_LOG(INFO, "unreadable replica -- ask the sby state info from replicas", K(ret),
                    K(tmp_ret), KPC(sby_msg));
        }
      } else if (OB_LS_NOT_EXIST == ret || OB_PARTITION_NOT_EXIST == ret) {
        orphan_resp.src_state_info_.init(sby_msg->dst_ls_id_);
        orphan_resp.src_state_info_.set_ls_not_exist(sby_msg->dst_ls_id_);
      }

      if (orphan_resp.src_state_info_.is_valid()) {
        orphan_resp.tenant_id_ = sby_msg->tenant_id_;
        orphan_resp.cluster_version_ = GET_MIN_CLUSTER_VERSION();
        orphan_resp.tx_id_ = sby_msg->tx_id_;
        orphan_resp.msg_snapshot_ = sby_msg->msg_snapshot_;
        orphan_resp.src_ls_id_ = sby_msg->dst_ls_id_;
        orphan_resp.src_addr_ = sby_msg->dst_addr_;
        orphan_resp.dst_ls_id_ = err_resp_ls_id;
        orphan_resp.dst_addr_ = err_resp_addr;
        orphan_resp.downstream_parts_.reset();
        orphan_resp.from_root_ = false;
        if (OB_TMP_FAIL(sby_rpc_impl_.post_msg(orphan_resp.dst_addr_, orphan_resp))) {
          TRANS_LOG(WARN, "post standby orphan msg failed", K(ret), K(tmp_ret), K(orphan_resp));
        }
      }
    }
  }

  return ret;
}

int ObTransService::get_sby_tx_state_from_tx_table(const ObLSID &ls_id,
                                                   const ObTransID &tx_id,
                                                   const share::SCN &ls_readable_scn,
                                                   const share::SCN &msg_snapshot,
                                                   ObTxSbyStateInfo &sby_state_info)
{
  int ret = OB_SUCCESS;

  int64_t tx_data_state = ObTxData::RUNNING;
  SCN trans_version = share::SCN::invalid_scn();
  sby_state_info.reset();
  sby_state_info.init(ls_id);
  if (!sby_state_info.is_valid() || !tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(sby_state_info), K(tx_id));
  } else if (OB_FAIL(get_tx_state_from_tx_table_(ls_id, tx_id, tx_data_state, trans_version))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      sby_state_info.fill(ls_id, ObTxState::UNKNOWN, trans_version, ls_readable_scn, msg_snapshot);
      ret = OB_SUCCESS;
    }
    TRANS_LOG(INFO, "get tx state from tx table fail", K(ret), K(tx_data_state), K(msg_snapshot),
              K(trans_version), K(ls_readable_scn), K(sby_state_info), K(tx_id));
#ifdef ERRSIM
  } else if (ERRSIM_STANDBY_CTX_NOT_EXIST == OB_TRANS_CTX_NOT_EXIST) {
    ret = OB_TRANS_CTX_NOT_EXIST;
    TRANS_LOG(INFO, "[ERRSIM] standby ctx not exist", K(ret), K(ls_id), K(tx_id),
              K(ls_readable_scn), K(msg_snapshot));
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      sby_state_info.fill(ls_id, ObTxState::UNKNOWN, trans_version, ls_readable_scn, msg_snapshot);
      ret = OB_SUCCESS;
    }
    TRANS_LOG(INFO, "get tx state from tx table fail", K(ret), K(tx_data_state), K(msg_snapshot),
              K(trans_version), K(ls_readable_scn), K(sby_state_info), K(tx_id));
#endif
  } else {
    switch (tx_data_state) {
    case ObTxData::COMMIT:
      sby_state_info.tx_state_ = ObTxState::COMMIT;
      if (!trans_version.is_valid_and_not_min()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "invalid trans_version with the commited state", K(ret), K(tx_data_state),
                  K(msg_snapshot), K(trans_version), K(ls_readable_scn), K(sby_state_info),
                  K(tx_id));
      } else {
        sby_state_info.fill(ls_id, ObTxState::COMMIT, trans_version, ls_readable_scn, msg_snapshot);
      }
      break;
    case ObTxData::ABORT:
      sby_state_info.fill(ls_id, ObTxState::ABORT, trans_version, ls_readable_scn, msg_snapshot);
      break;
    case ObTxData::RUNNING:
    default:
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx in-progress but ctx miss", K(ret), K(tx_data_state), K(tx_id),
                K(sby_state_info));
    }
    // state_info.version_ = version;
  }

  if (OB_FAIL(ret)) {
    sby_state_info.reset();
  }

  return ret;
}

#define POST_SBY_MSG_TO_REPLICA(MSG_TYPE)                                                     \
  MSG_TYPE msg = *(static_cast<const MSG_TYPE *>(sby_msg));                                   \
  msg.src_ls_id_ = msg.dst_ls_id_;                                                            \
  msg.src_addr_ = GCONF.self_addr_;                                                           \
  const common::ObIArray<ObLSReplicaLocation> &location_array =                               \
      replica_location.get_replica_locations();                                               \
  for (int i = 0; i < location_array.count(); i++) {                                          \
    msg.dst_addr_ = location_array.at(i).get_server();                                        \
    if (OB_TMP_FAIL(sby_rpc_impl_.post_msg(msg.dst_addr_, msg))) {                            \
      TRANS_LOG(WARN, "post msg failed", K(ret), K(tmp_ret), K(msg), K(location_array.at(i)), \
                K(i));                                                                        \
    }                                                                                         \
  }

int ObTransService::ask_sby_state_info_from_replicas(const ObTxSbyBaseMsg *sby_msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSLocation replica_location;
  if (OB_ISNULL(sby_msg) || !sby_msg->is_valid() || sby_msg->tenant_id_ != tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id_), KPC(sby_msg));
  } else if (sby_msg->dst_ls_id_ == sby_msg->src_ls_id_
             && sby_msg->dst_addr_ != sby_msg->src_addr_) {
    // do nothing
    TRANS_LOG(INFO, "receive a msg from other replicas", K(ret), KPC(sby_msg));
  } else if (OB_FAIL(location_adapter_def_.nonblock_get(GCONF.cluster_id, sby_msg->tenant_id_,
                                                        sby_msg->dst_ls_id_, replica_location))) {
    TRANS_LOG(WARN, "nonblock get locations failed", K(ret), K(tmp_ret), KPC(sby_msg),
              K(replica_location));
    location_adapter_def_.nonblock_renew(GCONF.cluster_id, sby_msg->tenant_id_,
                                         sby_msg->dst_ls_id_);
  } else if (sby_msg->msg_type_ == ObTxSbyMsgType::ASK_DOWNSTREAM) {
    POST_SBY_MSG_TO_REPLICA(ObTxSbyAskDownstreamReq);
  } else if (sby_msg->msg_type_ == ObTxSbyMsgType::ASK_UPSTREAM) {
    POST_SBY_MSG_TO_REPLICA(ObTxSbyAskUpstreamReq);
  } else {
    TRANS_LOG(INFO, "no need to ask other replicas", K(ret), K(tmp_ret), KPC(sby_msg));
  }

  return ret;
}

} // namespace transaction

} // namespace oceanbase
