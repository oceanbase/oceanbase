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

#include "ob_log_req.h"
#include "lib/utility/ob_serialization_helper.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace clog {

void ObPushLogReq::reset()
{
  proposal_id_.reset();
  buf_ = NULL;
  len_ = 0;
  push_mode_ = PUSH_LOG_ASYNC;
}

DEF_TO_STRING(ObPushLogReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_BUFFER, (const char*)buf_, N_LENGTH, len_, "push_mode", push_mode_);
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObPushLogReq)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == buf_ || len_ <= 0) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = proposal_id_.serialize(buf, buf_len, new_pos))) {
    CLOG_LOG(WARN, "encode proposal_id fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, len_))) {
    CLOG_LOG(WARN, "encode len fail:", K(len_), K(ret));
  } else if (new_pos + len_ > buf_len) {
    ret = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + new_pos, buf_, len_);
    new_pos += len_;

    if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, new_pos, push_mode_))) {
      CLOG_LOG(WARN, "encode push_mode_ failed", K(push_mode_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPushLogReq)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = proposal_id_.deserialize(buf, data_len, new_pos))) {
    CLOG_LOG(WARN, "decode len fail:", K(len_), K(ret));
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &len_))) {
    CLOG_LOG(WARN, "decode len fail:", K(len_), K(ret));
  } else if (new_pos + len_ > data_len) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    buf_ = const_cast<char*>(buf + new_pos);
    new_pos += len_;

    if (new_pos + serialization::encoded_length_i32(push_mode_) <= data_len) {
      if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &push_mode_))) {
        CLOG_LOG(WARN, "decode push_mode failed", K(push_mode_), K(ret));
      }
    } else {
      // old version ObPushLogReq, no push_mode
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushLogReq)
{
  return proposal_id_.get_serialize_size() + serialization::encoded_length_i64(len_) + len_ +
         serialization::encoded_length_i32(push_mode_);
}

void ObPushMsLogReq::reset()
{
  proposal_id_.reset();
  buf_ = NULL;
  len_ = 0;
}

DEF_TO_STRING(ObPushMsLogReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_PROPOSE_ID, proposal_id_, N_BUFFER, (const char*)buf_, N_LENGTH, len_);
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObPushMsLogReq)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == buf_ || len_ <= 0) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = proposal_id_.serialize(buf, buf_len, new_pos))) {
    CLOG_LOG(WARN, "encode proposal_id fail", K(ret));
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, len_))) {
    CLOG_LOG(WARN, "encode len fail:", K(len_), K(ret));
  } else if (new_pos + len_ > buf_len) {
    ret = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + new_pos, buf_, len_);
    new_pos += len_;
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPushMsLogReq)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = proposal_id_.deserialize(buf, data_len, new_pos))) {
    CLOG_LOG(WARN, "decode len fail:", K(len_), K(ret));
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &len_))) {
    CLOG_LOG(WARN, "decode len fail:", K(len_), K(ret));
  } else if (new_pos + len_ > data_len) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    buf_ = const_cast<char*>(buf + new_pos);
    new_pos += len_;
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushMsLogReq)
{
  return proposal_id_.get_serialize_size() + serialization::encoded_length_i64(len_) + len_;
}

void ObFetchLogReqV2::reset()
{
  start_id_ = OB_INVALID_ID;
  end_limit_ = OB_INVALID_ID;
  fetch_type_ = OB_FETCH_LOG_UNKNOWN;
  proposal_id_.reset();
  replica_type_ = REPLICA_TYPE_MAX;
  network_limit_ = 0;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  max_confirmed_log_id_ = common::OB_INVALID_ID;
}

DEF_TO_STRING(ObFetchLogReqV2)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_START_LSN,
      start_id_,
      N_LIMIT,
      end_limit_,
      "fetch_log_type",
      fetch_type_,
      N_PROPOSE_ID,
      proposal_id_,
      N_REPLICA_TYPE,
      replica_type_,
      "network_limit",
      network_limit_,
      N_CLUSTER_ID,
      cluster_id_,
      "max_confirmed_log_id",
      max_confirmed_log_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObFetchLogReqV2, start_id_, end_limit_, fetch_type_, proposal_id_, replica_type_, network_limit_,
    cluster_id_, max_confirmed_log_id_);

void ObFakeAckReq::reset()
{
  log_id_ = OB_INVALID_ID;
  proposal_id_.reset();
}

DEF_TO_STRING(ObFakeAckReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_LSN, log_id_, N_PROPOSE_ID, proposal_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObFakeAckReq, log_id_, proposal_id_);

void ObFakePushLogReq::reset()
{
  log_id_ = OB_INVALID_ID;
  proposal_id_.reset();
}

DEF_TO_STRING(ObFakePushLogReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_LSN, log_id_, N_PROPOSE_ID, proposal_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObFakePushLogReq, log_id_, proposal_id_);

void ObAckLogReqV2::reset()
{
  log_id_ = OB_INVALID_ID;
  proposal_id_.reset();
}

DEF_TO_STRING(ObAckLogReqV2)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_LSN, log_id_, N_PROPOSE_ID, proposal_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObAckLogReqV2, log_id_, proposal_id_);

void ObStandbyAckLogReq::reset()
{
  log_id_ = OB_INVALID_ID;
  proposal_id_.reset();
}

DEF_TO_STRING(ObStandbyAckLogReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_LSN, log_id_, N_PROPOSE_ID, proposal_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObStandbyAckLogReq, log_id_, proposal_id_);

void ObStandbyQuerySyncStartIdReq::reset()
{
  send_ts_ = OB_INVALID_TIMESTAMP;
}

DEF_TO_STRING(ObStandbyQuerySyncStartIdReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_TIMESTAMP, send_ts_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObStandbyQuerySyncStartIdReq, send_ts_);

void ObStandbyQuerySyncStartIdResp::reset()
{
  original_send_ts_ = OB_INVALID_TIMESTAMP;
  sync_start_id_ = OB_INVALID_ID;
}

DEF_TO_STRING(ObStandbyQuerySyncStartIdResp)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_TIMESTAMP, original_send_ts_, "sync_start_id", sync_start_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObStandbyQuerySyncStartIdResp, original_send_ts_, sync_start_id_);

void ObRenewMsLogAckReq::reset()
{
  log_id_ = OB_INVALID_ID;
  submit_timestamp_ = OB_INVALID_TIMESTAMP;
  proposal_id_.reset();
}

DEF_TO_STRING(ObRenewMsLogAckReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_LSN, log_id_, "submit_timestamp", submit_timestamp_, N_PROPOSE_ID, proposal_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObRenewMsLogAckReq, log_id_, submit_timestamp_, proposal_id_);

void ObPrepareReq::reset()
{
  proposal_id_.reset();
}

DEF_TO_STRING(ObPrepareReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_PROPOSE_ID, proposal_id_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObPrepareReq, proposal_id_);

void ObStandbyPrepareReq::reset()
{
  ms_proposal_id_.reset();
}

DEF_TO_STRING(ObStandbyPrepareReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_PROPOSE_ID, ms_proposal_id_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObStandbyPrepareReq, ms_proposal_id_);

DEF_TO_STRING(ObStandbyPrepareResp)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_PROPOSE_ID,
      ms_proposal_id_,
      "ms_log_id",
      ms_log_id_,
      "membership_version",
      membership_version_,
      "member_list",
      member_list_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObStandbyPrepareResp, ms_proposal_id_, ms_log_id_, membership_version_, member_list_);

int ObStandbyPrepareResp::set_member_list(const common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  member_list_.reset();
  if (OB_FAIL(member_list_.deep_copy(member_list))) {
    CLOG_LOG(WARN, "member_list_.deep_copy failed", K(ret), K(member_list));
  }
  return ret;
}

DEF_TO_STRING(ObPrepareRespV2)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_PROPOSE_ID, proposal_id_, N_LOG_ID, max_log_id_, N_MAX_LOG_TS, max_log_ts_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObPrepareRespV2, proposal_id_, max_log_id_, max_log_ts_);

void ObFetchRegisterServerRespV2::reset()
{
  is_assign_parent_succeed_ = false;
  msg_type_ = 0;
  candidate_list_.reset();
}

int ObFetchRegisterServerRespV2::set_candidate_list(const share::ObCascadMemberList& candidate_list)
{
  int ret = OB_SUCCESS;
  candidate_list_.reset();
  // candidate_list can be empty
  if (OB_FAIL(candidate_list_.deep_copy(candidate_list))) {
    CLOG_LOG(WARN, "candidate_list_.deep_copy failed", K(ret), K(candidate_list));
  }
  return ret;
}

DEF_TO_STRING(ObFetchRegisterServerRespV2)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_IS_ASSIGN_PARENT_SUCCEED,
      is_assign_parent_succeed_,
      N_REG_RESP_MSG_TYPE,
      msg_type_,
      N_CANDIDATE_MEMBER_LIST,
      candidate_list_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObFetchRegisterServerRespV2, is_assign_parent_succeed_, msg_type_, candidate_list_);

void ObFetchRegisterServerReqV2::reset()
{
  replica_type_ = REPLICA_TYPE_MAX;
  next_replay_log_ts_ = OB_INVALID_TIMESTAMP;
  is_request_leader_ = false;
  is_need_force_register_ = false;
  region_ = DEFAULT_REGION_NAME;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  idc_.reset();
}

DEF_TO_STRING(ObFetchRegisterServerReqV2)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_REPLICA_TYPE,
      replica_type_,
      N_NEXT_REPLAY_LOG_TS,
      next_replay_log_ts_,
      N_IS_REQUEST_LEADER,
      is_request_leader_,
      N_IS_NEED_FORCE_REGISTER,
      is_need_force_register_,
      N_REGION,
      region_,
      N_CLUSTER_ID,
      cluster_id_,
      N_IDC,
      idc_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObFetchRegisterServerReqV2, replica_type_, next_replay_log_ts_, is_request_leader_,
    is_need_force_register_, region_, cluster_id_, idc_);

void ObRejectMsgReq::reset()
{
  msg_type_ = OB_REPLICA_MSG_TYPE_UNKNOWN;
  timestamp_ = OB_INVALID_TIMESTAMP;
}

DEF_TO_STRING(ObRejectMsgReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_REPLICA_MSG_TYPE, msg_type_, N_TIMESTAMP, timestamp_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObRejectMsgReq, msg_type_, timestamp_);

void ObRestoreAliveMsg::reset()
{
  start_log_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObRestoreAliveMsg, start_log_id_);

void ObRestoreAliveReq::reset()
{
  send_ts_ = OB_INVALID_TIMESTAMP;
}

OB_SERIALIZE_MEMBER(ObRestoreAliveReq, send_ts_);

void ObRestoreAliveResp::reset()
{
  send_ts_ = OB_INVALID_TIMESTAMP;
}

OB_SERIALIZE_MEMBER(ObRestoreAliveResp, send_ts_);

void ObRestoreLogFinishMsg::reset()
{
  log_id_ = OB_INVALID_ID;
}

DEF_TO_STRING(ObRestoreLogFinishMsg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_LOG_ID, log_id_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObRestoreLogFinishMsg, log_id_);

void ObReregisterMsg::reset()
{
  send_ts_ = OB_INVALID_TIMESTAMP;
  new_leader_.reset();
}

DEF_TO_STRING(ObReregisterMsg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_TIMESTAMP, send_ts_, N_LEADER, new_leader_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObReregisterMsg, send_ts_, new_leader_);

OB_SERIALIZE_MEMBER(ObConfirmedInfoReq, log_id_, confirmed_info_, batch_committed_);
OB_SERIALIZE_MEMBER(ObLogKeepaliveMsg, next_log_id_, next_log_ts_lb_, deliver_cnt_);
OB_SERIALIZE_MEMBER(ObRestoreTakeoverMsg, send_ts_);
OB_SERIALIZE_MEMBER(ObNotifyLogMissingReq, start_log_id_, is_in_member_list_, msg_type_);
OB_SERIALIZE_MEMBER(ObLeaderMaxLogMsg, switchover_epoch_, leader_max_log_id_, leader_next_log_ts_);
OB_SERIALIZE_MEMBER(ObSyncLogArchiveProgressMsg, status_);
OB_SERIALIZE_MEMBER(ObRenewMsLogConfirmedInfoReq, log_id_, ms_proposal_id_, confirmed_info_);

void ObBroadcastInfoReq::reset()
{
  replica_type_ = REPLICA_TYPE_MAX;
  max_confirmed_log_id_ = OB_INVALID_ID;
}

DEF_TO_STRING(ObBroadcastInfoReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_REPLICA_TYPE, replica_type_, N_LSN, max_confirmed_log_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObBroadcastInfoReq, replica_type_, max_confirmed_log_id_);

int ObBatchPushLogReq::init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& log_info_array)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid() || 0 == partition_array.count() || partition_array.count() != log_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(partition_array_.assign(partition_array))) {
    CLOG_LOG(WARN, "partition_array_ assign failed", K(ret), K(partition_array));
  } else if (OB_FAIL(log_info_array_.assign(log_info_array))) {
    CLOG_LOG(WARN, "log_info_array_ assign failed", K(ret), K(log_info_array));
  } else {
    trans_id_ = trans_id;
  }
  return ret;
}

void ObBatchPushLogReq::reset()
{
  trans_id_.reset();
  partition_array_.reset();
  log_info_array_.reset();
}

OB_SERIALIZE_MEMBER(ObBatchPushLogReq, trans_id_, partition_array_, log_info_array_);

int ObBatchAckLogReq::init(const transaction::ObTransID& trans_id, const ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid() || 0 == batch_ack_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(trans_id), K(batch_ack_array));
  } else if (OB_FAIL(batch_ack_array_.assign(batch_ack_array))) {
    CLOG_LOG(WARN, "batch_ack_array_ assign failed", K(ret), K(batch_ack_array), K(trans_id));
  } else {
    trans_id_ = trans_id;
  }
  return ret;
}

void ObBatchAckLogReq::reset()
{
  trans_id_.reset();
  batch_ack_array_.reset();
}

OB_SERIALIZE_MEMBER(ObBatchAckLogReq, trans_id_, batch_ack_array_);

void ObReplaceSickChildReq::reset()
{
  sick_child_.reset();
  cluster_id_ = common::OB_INVALID_CLUSTER_ID;
}

DEF_TO_STRING(ObReplaceSickChildReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_SICK_CHILD, sick_child_, N_CLUSTER_ID, cluster_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObReplaceSickChildReq, sick_child_, cluster_id_);

void ObCheckRebuildReq::reset()
{
  start_id_ = OB_INVALID_ID;
}

DEF_TO_STRING(ObCheckRebuildReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_START_LSN, start_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObCheckRebuildReq, start_id_);

};  // end namespace clog
};  // end namespace oceanbase
