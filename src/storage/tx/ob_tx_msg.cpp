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
// NOTICE: DO NOT MODIFY FOLLOING MACRO DEFINES, IT IS RESERVED FOR COMPATIBLE WITH OLD <= 4.1.2
#define ObTxSubPrepareMsg_V1_MEMBERS expire_ts_, xid_, parts_, app_trace_info_
#define ObTxSubPrepareRespMsg_V1_MEMBERS ret_
#define ObTxSubCommitMsg_V1_MEMBERS xid_
#define ObTxSubCommitRespMsg_V1_MEMBERS ret_
#define ObTxSubRollbackMsg_V1_MEMBERS xid_
#define ObTxSubRollbackRespMsg_V1_MEMBERS ret_
#define ObTxCommitMsg_V1_MEMBERS expire_ts_, parts_, app_trace_info_
#define ObTxCommitRespMsg_V1_MEMBERS ret_, commit_version_
#define ObTxAbortMsg_V1_MEMBERS reason_
#define ObTxKeepaliveMsg_V1_MEMBERS status_
#define ObTxKeepaliveRespMsg_V1_MEMBERS status_
#define Ob2pcPrepareReqMsg_V1_MEMBERS upstream_, app_trace_info_
#define Ob2pcPrepareRespMsg_V1_MEMBERS prepare_version_, prepare_info_array_
#define Ob2pcPreCommitReqMsg_V1_MEMBERS commit_version_
#define Ob2pcPreCommitRespMsg_V1_MEMBERS commit_version_
#define Ob2pcCommitReqMsg_V1_MEMBERS commit_version_, prepare_info_array_
#define Ob2pcCommitRespMsg_V1_MEMBERS commit_version_, commit_log_scn_
#define Ob2pcAbortReqMsg_V1_MEMBERS upstream_
#define Ob2pcAbortRespMsg_V1_MEMBERS
#define Ob2pcClearReqMsg_V1_MEMBERS max_commit_log_scn_
#define Ob2pcClearRespMsg_V1_MEMBERS
#define Ob2pcPrepareRedoReqMsg_V1_MEMBERS xid_, upstream_, app_trace_info_
#define Ob2pcPrepareRedoRespMsg_V1_MEMBERS
#define Ob2pcPrepareVersionReqMsg_V1_MEMBERS
#define Ob2pcPrepareVersionRespMsg_V1_MEMBERS prepare_version_, prepare_info_array_
#define ObAskStateMsg_V1_MEMBERS snapshot_
#define ObAskStateRespMsg_V1_MEMBERS state_info_array_
#define ObCollectStateMsg_V1_MEMBERS snapshot_
#define ObCollectStateRespMsg_V1_MEMBERS state_info_

#define CONCAT_REF(b) it_.b
#define APPLY_FUNC_(f, ...) f(__VA_ARGS__)
#define OB_TX_MSG_SERDE(CLZ, P_CLZ, ...)                                \
  struct CLZ##_box { CLZ##_box(CLZ &x) : it_(x) {} CLZ &it_; OB_UNIS_VERSION(1); }; \
  APPLY_FUNC_(OB_SERIALIZE_MEMBER, CLZ##_box, LST_DO(CONCAT_REF, (,), ##__VA_ARGS__)); \
  int CLZ::serialize(SERIAL_PARAMS) const                               \
  {                                                                     \
    int ret = P_CLZ::serialize(buf, buf_len, pos);                      \
    if (OB_SUCC(ret) && cluster_version_ <= CLUSTER_VERSION_4_1_0_1) {  \
      LST_DO_CODE(OB_UNIS_ENCODE, CLZ ## _V1_MEMBERS);                  \
    } else if (OB_SUCC(ret)) {                                          \
      CLZ##_box x(const_cast<CLZ&>(*this));                             \
      ret = x.serialize(buf, buf_len, pos);                             \
    }                                                                   \
    return ret;                                                         \
  }                                                                     \
  int CLZ::deserialize(DESERIAL_PARAMS) {                               \
    int ret = OB_SUCCESS;                                               \
    ret = P_CLZ::deserialize(buf, data_len, pos);                       \
    if (OB_SUCC(ret) && cluster_version_ <= CLUSTER_VERSION_4_1_0_1) {  \
      LST_DO_CODE(OB_UNIS_DECODE, CLZ ## _V1_MEMBERS);                  \
    } else if (OB_SUCC(ret)) {                                          \
      CLZ##_box x(*this);                                               \
      ret = x.deserialize(buf, data_len, pos);                          \
    }                                                                   \
    return ret;                                                         \
  }                                                                     \
  int64_t CLZ::get_serialize_size() const {                             \
    int64_t len = P_CLZ::get_serialize_size();                          \
    if (cluster_version_ <= CLUSTER_VERSION_4_1_0_1) {                  \
      LST_DO_CODE(OB_UNIS_ADD_LEN, CLZ ## _V1_MEMBERS);                 \
    } else {                                                            \
      CLZ##_box x(const_cast<CLZ&>(*this));                             \
      len += x.get_serialize_size();                                    \
    }                                                                   \
    return len;                                                         \
  }

OB_TX_MSG_SERDE(ObTxSubPrepareMsg, ObTxMsg, expire_ts_, xid_, parts_, app_trace_info_);
OB_TX_MSG_SERDE(ObTxSubPrepareRespMsg, ObTxMsg, ret_);
OB_TX_MSG_SERDE(ObTxSubCommitMsg, ObTxMsg, xid_);
OB_TX_MSG_SERDE(ObTxSubCommitRespMsg, ObTxMsg, ret_);
OB_TX_MSG_SERDE(ObTxSubRollbackMsg, ObTxMsg, xid_);
OB_TX_MSG_SERDE(ObTxSubRollbackRespMsg, ObTxMsg, ret_);
OB_TX_MSG_SERDE(ObTxCommitMsg, ObTxMsg, expire_ts_, parts_, app_trace_info_, commit_start_scn_);
OB_TX_MSG_SERDE(ObTxCommitRespMsg, ObTxMsg, ret_, commit_version_);
OB_TX_MSG_SERDE(ObTxAbortMsg, ObTxMsg, reason_);
OB_TX_MSG_SERDE(ObTxKeepaliveMsg, ObTxMsg, status_);
OB_TX_MSG_SERDE(ObTxKeepaliveRespMsg, ObTxMsg, status_);
OB_TX_MSG_SERDE(Ob2pcPrepareReqMsg, ObTxMsg, upstream_, app_trace_info_);
OB_TX_MSG_SERDE(Ob2pcPrepareRespMsg, ObTxMsg, prepare_version_, prepare_info_array_);
OB_TX_MSG_SERDE(Ob2pcPreCommitReqMsg, ObTxMsg, commit_version_);
OB_TX_MSG_SERDE(Ob2pcPreCommitRespMsg, ObTxMsg, commit_version_);
OB_TX_MSG_SERDE(Ob2pcCommitReqMsg, ObTxMsg, commit_version_, prepare_info_array_);
OB_TX_MSG_SERDE(Ob2pcCommitRespMsg, ObTxMsg, commit_version_, commit_log_scn_);
OB_TX_MSG_SERDE(Ob2pcAbortReqMsg, ObTxMsg, upstream_);
OB_TX_MSG_SERDE(Ob2pcAbortRespMsg, ObTxMsg);
OB_TX_MSG_SERDE(Ob2pcClearReqMsg, ObTxMsg, max_commit_log_scn_);
OB_TX_MSG_SERDE(Ob2pcClearRespMsg, ObTxMsg);
OB_TX_MSG_SERDE(Ob2pcPrepareRedoReqMsg, ObTxMsg, xid_, upstream_, app_trace_info_);
OB_TX_MSG_SERDE(Ob2pcPrepareRedoRespMsg, ObTxMsg);
OB_TX_MSG_SERDE(Ob2pcPrepareVersionReqMsg, ObTxMsg);
OB_TX_MSG_SERDE(Ob2pcPrepareVersionRespMsg, ObTxMsg, prepare_version_, prepare_info_array_);
OB_TX_MSG_SERDE(ObAskStateMsg, ObTxMsg, snapshot_);
OB_TX_MSG_SERDE(ObAskStateRespMsg, ObTxMsg, state_info_array_);
OB_TX_MSG_SERDE(ObCollectStateMsg, ObTxMsg, snapshot_);
OB_TX_MSG_SERDE(ObCollectStateRespMsg, ObTxMsg, state_info_);
OB_SERIALIZE_MEMBER((ObTxRollbackSPRespMsg, ObTxMsg), ret_, orig_epoch_);

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
  OB_UNIS_ADD_LEN(flag_);
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
    OB_UNIS_ENCODE(flag_);
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
      void *buffer = ob_malloc(sizeof(ObTxDesc), "TxDesc");
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObTxDesc *tmp = new(buffer)ObTxDesc();
        OB_UNIS_DECODE(*tmp);
        tx_ptr_ = tmp;
      }
    }
    OB_UNIS_DECODE(flag_);
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
      && savepoint_.is_valid() && op_sn_ > -1) {
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

OB_SERIALIZE_MEMBER(ObAskTxStateFor4377Msg, tx_id_, ls_id_);
OB_SERIALIZE_MEMBER(ObAskTxStateFor4377RespMsg, is_alive_, ret_);

bool ObAskTxStateFor4377Msg::is_valid() const
{
  bool ret = false;
  if (tx_id_.is_valid()
      && ls_id_.is_valid()) {
    ret = true;
  }
  return ret;
}

bool ObAskTxStateFor4377RespMsg::is_valid() const
{
  return true;
}

} // transaction
} // oceanbase
