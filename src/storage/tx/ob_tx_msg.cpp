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

#include "ob_tx_msg.h"

namespace oceanbase
{
namespace transaction
{
OB_SERIALIZE_MEMBER(ObTxMsg,
                    type_,
                    cluster_version_,
                    tenant_id_,
                    tx_id_,
                    receiver_,
                    sender_addr_,
                    sender_,
                    request_id_,
                    timestamp_,
                    epoch_,
                    cluster_id_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxSubPrepareMsg, ObTxMsg, expire_ts_, xid_, parts_, app_trace_info_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxSubPrepareRespMsg, ObTxMsg, ret_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxSubCommitMsg, ObTxMsg, xid_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxSubCommitRespMsg, ObTxMsg, ret_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxSubRollbackMsg, ObTxMsg, xid_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxSubRollbackRespMsg, ObTxMsg, ret_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxCommitMsg, ObTxMsg, expire_ts_, parts_, app_trace_info_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxCommitRespMsg, ObTxMsg, ret_, commit_version_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxAbortMsg, ObTxMsg, reason_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxKeepaliveMsg, ObTxMsg, status_);
OB_SERIALIZE_MEMBER_INHERIT(ObTxKeepaliveRespMsg, ObTxMsg, status_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPrepareReqMsg, ObTxMsg, upstream_, app_trace_info_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPrepareRespMsg, ObTxMsg, prepare_version_, prepare_info_array_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPreCommitReqMsg, ObTxMsg, commit_version_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPreCommitRespMsg, ObTxMsg, commit_version_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcCommitReqMsg, ObTxMsg, commit_version_, prepare_info_array_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcCommitRespMsg, ObTxMsg, commit_version_, commit_log_scn_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcAbortReqMsg, ObTxMsg, upstream_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcAbortRespMsg, ObTxMsg);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcClearReqMsg, ObTxMsg, max_commit_log_scn_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcClearRespMsg, ObTxMsg);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPrepareRedoReqMsg, ObTxMsg, xid_, upstream_, app_trace_info_);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPrepareRedoRespMsg, ObTxMsg);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPrepareVersionReqMsg, ObTxMsg);
OB_SERIALIZE_MEMBER_INHERIT(Ob2pcPrepareVersionRespMsg, ObTxMsg, prepare_version_, prepare_info_array_);
OB_SERIALIZE_MEMBER_INHERIT(ObAskStateMsg, ObTxMsg, snapshot_);
OB_SERIALIZE_MEMBER_INHERIT(ObAskStateRespMsg, ObTxMsg, state_info_array_);
OB_SERIALIZE_MEMBER_INHERIT(ObCollectStateMsg, ObTxMsg, snapshot_);
OB_SERIALIZE_MEMBER_INHERIT(ObCollectStateRespMsg, ObTxMsg, state_info_);

OB_DEF_SERIALIZE_SIZE(ObTxRollbackSPMsg)
{
  int len = 0;
  len += ObTxMsg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, savepoint_, op_sn_, branch_id_);
  if (OB_NOT_NULL(tx_ptr_)) {
    OB_UNIS_ADD_LEN(true);
    OB_UNIS_ADD_LEN(*tx_ptr_);
  } else {
    OB_UNIS_ADD_LEN(false);
  }
  return len;
}

OB_DEF_SERIALIZE(ObTxRollbackSPMsg)
{
  int ret = ObTxMsg::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, savepoint_, op_sn_, branch_id_);
    if (OB_NOT_NULL(tx_ptr_)) {
      OB_UNIS_ENCODE(true);
      OB_UNIS_ENCODE(*tx_ptr_);
    } else {
      OB_UNIS_ENCODE(false);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTxRollbackSPMsg)
{
  int ret = ObTxMsg::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, savepoint_, op_sn_, branch_id_);
    bool has_tx_ptr = false;
    OB_UNIS_DECODE(has_tx_ptr);
    if (has_tx_ptr) {
      void *buffer = ob_malloc(sizeof(ObTxDesc));
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObTxDesc *tmp = new(buffer)ObTxDesc();
        OB_UNIS_DECODE(*tmp);
        tx_ptr_ = tmp;
      }
    }
  }
  return ret;
}

bool ObTxMsg::is_valid() const
{
  bool ret = false;
  if (type_ != TX_UNKNOWN && cluster_version_ > 0 && is_valid_tenant_id(tenant_id_)
      && tx_id_.is_valid() && timestamp_ > 0 && request_id_ > -1
      && sender_.is_valid() && receiver_.is_valid() && sender_addr_.is_valid()
      && cluster_id_ > OB_INVALID_CLUSTER_ID) {
    ret = true;
  }
  return ret;
}

bool ObTxSubPrepareMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == SUBPREPARE && expire_ts_ > OB_INVALID_TIMESTAMP
      && xid_.is_valid() && !xid_.empty() && parts_.count() > 0) {
    ret = true;
  }
  return ret;
}

bool ObTxSubPrepareRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == SUBPREPARE_RESP) {
    ret = true;
  }
  return ret;
}

bool ObTxSubCommitMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == SUBCOMMIT
      && xid_.is_valid() && !xid_.empty()) {
    ret = true;
  }
  return ret;
}

bool ObTxSubCommitRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == SUBCOMMIT_RESP) {
    ret = true;
  }
  return ret;
}

bool ObTxSubRollbackMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid()&& type_ == SUBROLLBACK
      && xid_.is_valid() && !xid_.empty()) {
    ret = true;
  }
  return ret;
}

bool ObTxSubRollbackRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == SUBROLLBACK_RESP) {
    ret = true;
  }
  return ret;
}

bool ObTxCommitMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_COMMIT
     && expire_ts_ > OB_INVALID_TIMESTAMP
     && parts_.count() > 0) {
    ret = true;
  }
  return ret;
}

bool ObTxCommitRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_COMMIT_RESP
     && ((OB_SUCCESS == ret_ && commit_version_.is_valid())
     || (OB_SUCCESS != ret_ && !commit_version_.is_valid()))) {
    ret = true;
  }
  return ret;
}

bool ObTxAbortMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_ABORT) {
    ret = true;
  }
  return ret;
}

bool ObTxRollbackSPMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == ROLLBACK_SAVEPOINT
      && savepoint_ > -1 && op_sn_ > -1) {
    ret = true;
  }
  return ret;
}

bool ObTxKeepaliveMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == KEEPALIVE) {
    ret = true;
  }
  return ret;
}

bool ObTxKeepaliveRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == KEEPALIVE_RESP) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPrepareReqMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PREPARE_REQ
      && upstream_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPrepareRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PREPARE_RESP
      && prepare_version_.is_valid()
      && prepare_info_array_.count() > 0) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPreCommitReqMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PRE_COMMIT_REQ
      && commit_version_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPreCommitRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PRE_COMMIT_RESP
      && commit_version_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool Ob2pcCommitReqMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_COMMIT_REQ
      && commit_version_.is_valid()
      && prepare_info_array_.count() > 0) {
    ret = true;
  }
  return ret;
}

bool Ob2pcCommitRespMsg::is_valid() const {
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_COMMIT_RESP &&
      commit_version_.is_valid()) {
    ret = true;
  }
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0 &&
      !commit_log_scn_.is_valid()) {
    ret = false;
  }
  return ret;
}

bool Ob2pcAbortReqMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_ABORT_REQ) {
    ret = true;
  }
  return ret;
}

bool Ob2pcAbortRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_ABORT_RESP) {
    ret = true;
  }
  return ret;
}

bool Ob2pcClearReqMsg::is_valid() const {
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_CLEAR_REQ) {
    ret = true;
  }
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0 &&
      !max_commit_log_scn_.is_valid()) {
    ret = false;
  }
  return ret;
}

bool Ob2pcClearRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_CLEAR_RESP) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPrepareRedoReqMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PREPARE_REDO_REQ
     && xid_.is_valid() && !xid_.empty() && upstream_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPrepareRedoRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PREPARE_REDO_RESP) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPrepareVersionReqMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PREPARE_VERSION_REQ) {
    ret = true;
  }
  return ret;
}

bool Ob2pcPrepareVersionRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == TX_2PC_PREPARE_VERSION_RESP
      && prepare_version_.is_valid()
      && prepare_info_array_.count() > 0) {
    ret = true;
  }
  return ret;
}

bool ObAskStateMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == ASK_STATE
      && snapshot_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool ObAskStateRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == ASK_STATE_RESP
      && state_info_array_.count() > 0) {
    ret = true;
  }
  return ret;
}

bool ObCollectStateMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == COLLECT_STATE
      && snapshot_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool ObCollectStateRespMsg::is_valid() const
{
  bool ret = false;
  if (ObTxMsg::is_valid() && type_ == COLLECT_STATE_RESP
      && state_info_.is_valid()) {
    ret = true;
  }
  return ret;
}

} // transaction
} // oceanbase
