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

#include "common/ob_clock_generator.h"
#include "ob_trans_msg2.h"
#include "ob_trans_msg_type2.h"

namespace oceanbase {
using namespace common;

namespace transaction {

OB_SERIALIZE_MEMBER(ObTrxMsgBase, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_, receiver_,
    trans_param_, sender_addr_);
/*
OB_SERIALIZE_MEMBER(ObTrxErrMsg, tenant_id_, trans_id_, err_msg_type_, sender_,
  sender_addr_, status_, sql_no_, request_id_);
OB_SERIALIZE_MEMBER(ObTrxStartStmtRequest, tenant_id_, trans_id_, trans_time_, sender_,
  receiver_, scheduler_, trans_param_, sender_addr_, sql_no_, cluster_id_, request_id_,
  stmt_expired_time_, need_create_ctx_, cluster_version_, snapshot_gene_type_, session_id_,
  proxy_session_id_, app_trace_id_str_, trans_location_cache_, can_elr_, is_not_create_ctx_participant_);
OB_SERIALIZE_MEMBER(ObTrxStartStmtResponse, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  trans_param_, sender_addr_, sql_no_, status_, request_id_);
OB_SERIALIZE_MEMBER(ObTrxStmtRollbackResponse, tenant_id, trans_id_, trans_time_, sender_, receiver_,
  trans_param_, sender_addr_, sql_no_, status_, request_id_);
OB_SERIALIZE_MEMBER(ObTrxStmtRollbackRequest, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  trans_param_, sender_addr_, sql_no_, request_id_, trans_location_cache_);
OB_SERIALIZE_MEMBER(ObTrxCommitRequest, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  scheduler_, coordinator_, participants_, trans_param_, sender_addr_, trans_location_cache_,
  commit_times_, stc_, is_dup_table_trans_, stmt_rollback_info_);
OB_SERIALIZE_MEMBER(ObTrxCommitResponse, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  trans_param_, sender_addr_, status_, commit_times_, need_wait_interval_us_);
OB_SERIALIZE_MEMBER(ObTrx2PCPrepareRequest, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  scheduler_, coordinator_, participants_, trans_param_, sender_addr_, status_, request_id_,
  stc_, partition_log_info_arr_, stmt_rollback_info_);
OB_SERIALIZE_MEMBER(ObTrx2PCPrePrepareRequest, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  scheduler_, coordinator_, participants_, trans_param_, sender_addr_, status_, request_id_, stc_);
OB_SERIALIZE_MEMBER(ObTrx2PCPrePrepareResponse, tenant_id_, trans_id_, trans_time_, sender_,
  receiver_, scheduler_, coordinator_, participants_, trans_param_, prepare_log_id_, prepare_log_timestamp_,
  sender_addr_, status_, trans_version_, request_id_);
OB_SERIALIZE_MEMBER(ObTrx2PCLogIDRequest, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  trans_param_, sender_addr_, request_id_,);
OB_SERIALIZE_MEMBER(ObTrx2PCLogIDResponse, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  trans_param_, prepare_log_id_, prepare_log_timestamp_, sender_addr_, status_, request_id_,
  partition_log_info_arr_);
*/
OB_SERIALIZE_MEMBER(ObTrx2PCCommitRequest, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_,
    receiver_, trans_param_, sender_addr_, scheduler_, coordinator_, participants_, trans_version_, request_id_,
    partition_log_info_arr_, status_, batch_same_leader_partitions_);
OB_SERIALIZE_MEMBER(ObTrx2PCCommitResponse, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_,
    receiver_, trans_param_, sender_addr_, scheduler_, coordinator_, participants_, trans_version_, request_id_,
    partition_log_info_arr_, status_, batch_same_leader_partitions_, commit_log_ts_);
OB_SERIALIZE_MEMBER(ObTrx2PCAbortRequest, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_, receiver_,
    trans_param_, sender_addr_, scheduler_, coordinator_, participants_, request_id_, status_,
    batch_same_leader_partitions_);
OB_SERIALIZE_MEMBER(ObTrx2PCPrepareRequest, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_,
    receiver_, trans_param_, sender_addr_, scheduler_, coordinator_, participants_, status_, request_id_, stc_.mts_,
    partition_log_info_arr_, stmt_rollback_info_, app_trace_info_, batch_same_leader_partitions_, xid_, is_xa_prepare_,
    split_info_);
OB_SERIALIZE_MEMBER(ObTrx2PCPrepareResponse, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_,
    receiver_, trans_param_, sender_addr_, scheduler_, coordinator_, participants_, prepare_log_id_,
    prepare_log_timestamp_, status_, state_, trans_version_, request_id_, partition_log_info_arr_,
    need_wait_interval_us_, app_trace_info_, publish_version_, xid_, is_xa_prepare_, split_info_);
OB_SERIALIZE_MEMBER((ObTrxListenerCommitRequest, ObTrx2PCCommitRequest), split_info_);
OB_SERIALIZE_MEMBER((ObTrxListenerAbortRequest, ObTrx2PCAbortRequest), split_info_);
OB_SERIALIZE_MEMBER(ObTrx2PCClearRequest, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_, receiver_,
    trans_param_, sender_addr_, scheduler_, coordinator_, participants_, request_id_, status_,
    batch_same_leader_partitions_, clear_log_base_ts_);
/*
OB_SERIALIZE_MEMBER(ObTrxClearRequest, tenant_id_, trans_id_, trans_time_, sender_, receiver_,
  scheduler_, participants_, trans_param_, sender_addr_, request_id_, status_);
OB_SERIALIZE_MEMBER(ObTrxAskSchedulerRequest, tenant_id_, trans_id_, trans_time_, sender_,
  receiver_, trans_param_, sender_addr_, scheduler_, status_);
*/

// ObTrxMsgBase
int ObTrxMsgBase::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
      !receiver.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid()) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
    trans_id_ = trans_id;
    msg_type_ = msg_type;
    timestamp_ = ObTimeUtility::current_time();
    trans_time_ = trans_time;
    sender_ = sender;
    receiver_ = receiver;
    trans_param_ = trans_param;
    sender_addr_ = sender_addr;
  }

  return ret;
}

bool ObTrxMsgBase::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && trans_id_.is_valid() && timestamp_ > 0 && 0 < trans_time_ &&
         sender_.is_valid() && receiver_.is_valid() && trans_param_.is_valid() && sender_addr_.is_valid();
}

/*
int ObTrxErrMsg::init(const int64_t msg_type, const int64_t err_msg_type, const uint64_t tenant_id,
  const ObTransID &trans_id, const common::ObPartitionKey &sender, const common::ObAddr &sender_addr,
  const int32_t status, const int64_t sql_no, const int64_t request_id)
{
int ret = OB_SUCCESS;

if ((OB_TRX_ERROR_MSG != msg_type) || !is_valid_tenant_id(tenant_id) || !sender_addr.is_valid()
  || !trans_id.is_valid() || !sender.is_valid() || !ObTransMsgTypeChecker::is_valid_msg_type(err_msg_type)) {
  TRANS_LOG(WARN, "invalid argument", K(msg_type), K(err_msg_type), K(tenant_id), K(trans_id),
      K(sender), K(status));
  ret = OB_INVALID_ARGUMENT;
} else {
  tenant_id_ = tenant_id;
  trans_id_ = trans_id;
  msg_type_ = msg_type;
  err_msg_type_ = err_msg_type;
  sender_ = sender;
  sender_addr_ = sender_addr;
  status_ = status;
  sql_no_ = sql_no;
  request_id_ = request_id;
}

return ret;
}

int ObTrxStartStmtRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
  const ObStartTransParam &trans_param, const common::ObAddr &sender_addr,
  const int64_t sql_no, const uint64_t cluster_id, const int64_t request_id,
  const int64_t stmt_expired_time, const bool need_create_ctx,
  const uint64_t cluster_version, const int32_t snapshot_gene_type, const uint32_t session_id,
  const uint64_t proxy_session_id, const common::ObString &app_trace_id_str,
  const ObTransLocationCache &trans_location_cache, const bool can_elr,
  const bool is_not_create_ctx_participant)
{
int ret = OB_SUCCESS;

if ((OB_TRX_START_STMT_REQUEST != msg_type && OB_TRX_STMT_ROLLBACK_REQUEST != msg_type)
    || !is_valid_tenant_id(tenant_id)
    || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() || !receiver.is_valid()
    || !scheduler.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid() || sql_no <= 0
    || request_id <= 0 || stmt_expired_time < 0 || cluster_version <= 0 || UINT32_MAX == session_id
    || UINT64_MAX == proxy_session_id) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(scheduler), K(trans_param), K(sender_addr), K(sql_no), K(cluster_id),
      K(request_id), K(stmt_expired_time), K(need_create_ctx), K(cluster_version),
      K(session_id), K(proxy_session_id));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else {
  (void)app_trace_id_str_.assign_ptr(app_trace_id_str.ptr(), app_trace_id_str.length());
  sql_no_ = sql_no;
  request_id_ = request_id;
  cluster_id_ = cluster_id;
  stmt_expired_time_ = stmt_expired_time;
  scheduler_ = scheduler;
  need_create_ctx_ = need_create_ctx;
  active_memstore_version_ = ObVersion(2);
  cluster_version_ = cluster_version;
  snapshot_gene_type_ = snapshot_gene_type;
  session_id_ = session_id;
  proxy_session_id_ = proxy_session_id;
  trans_location_cache_ = trans_location_cache;
  can_elr_ = can_elr;
  is_not_create_ctx_participant_ = is_not_create_ctx_participant;
}

return ret;
}

int ObTrxStartStmtResponse::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
  const common::ObAddr &sender_addr, const int64_t sql_no, const int64_t snapshot_version,
  const int32_t status, const int64_t request_id)
{
int ret = OB_SUCCESS;

if (OB_TRX_START_STMT_RESPONSE != msg_type || !is_valid_tenant_id(tenant_id)
    || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() || !receiver.is_valid()
    || !trans_param.is_valid() || !sender_addr.is_valid() || sql_no <= 0 || request_id <= 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time), K(sender),
      K(receiver), K(trans_param), K(sender_addr), K(sql_no), K(snapshot_version), K(status),
      K(request_id));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_FAIL(ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else {
  sql_no_ = sql_no;
  snapshot_version_ = snapshot_version;
  status_ = status;
  request_id_ = request_id;
}

return ret;
}

int ObTrxStmtRollbackResponse::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
  const common::ObAddr &sender_addr, const int64_t sql_no, const int32_t status,
  const int64_t request_id)
{
int ret = OB_SUCCESS;

if ((OB_TRX_STMT_ROLLBACK_RESPONSE != msg_type && OB_TRX_SAVEPOINT_ROLLBACK_RESPONSE != msg_type)
    || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid()
    || !receiver.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid() || sql_no < 0
    || request_id < 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time), K(sender),
      K(receiver), K(trans_param), K(sender_addr), K(sql_no), K(status), K(request_id));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else {
  sql_no_ = sql_no;
  status_ = status;
  request_id_ = request_id;
}

return ret;
}

int ObTrxStmtRollbackRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
  const common::ObAddr &sender_addr, const int64_t sql_no, const int64_t request_id,
  const ObTransLocationCache &trans_location_cache)
{
int ret = OB_SUCCESS;

if ((OB_TRX_STMT_ROLLBACK_REQUEST != msg_type && OB_TRX_SAVEPOINT_ROLLBACK_REQUEST != msg_type)
    || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid()
    || !receiver.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid() || sql_no < 0
    || request_id < 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(trans_param), K(sender_addr), K(sql_no), K(request_id));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else if (OB_FAIL(trans_location_cache_.assign(trans_location_cache))) {
  TRANS_LOG(WARN, "assign transaction location cache error", K(ret), K(trans_location_cache));
} else {
  sql_no_ = sql_no;
  request_id_ = request_id;
}

return ret;
}

int ObTrxCommitRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
  const common::ObPartitionKey &coordinator, const common::ObPartitionArray &participants,
  const ObStartTransParam &trans_param, const common::ObAddr &sender_addr,
  const ObTransLocationCache &trans_location_cache, const int64_t commit_times,
  const int64_t stc, const bool is_dup_table_trans, const ObStmtRollbackInfo &stmt_rollback_info)
{
int ret = OB_SUCCESS;

if ((OB_TRX_COMMIT_REQUEST != msg_type && OB_TRX_ABORT_REQUEST != msg_type)
    || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0
    || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid()
    || 0 >= participants.count() || !trans_param.is_valid() || !sender_addr.is_valid()) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(scheduler), K(coordinator), K(participants), K(trans_param),
      K(sender_addr), "trans_location_cache", cache);
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
  TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
} else if (OB_SUCCESS != (ret = trans_location_cache_.assign(cache))) {
  TRANS_LOG(WARN, "assign transaction location cache error", K(ret), "trans_location_cache", cache);
} else if (OB_FAIL(stmt_rollback_info_.assign(stmt_rollback_info))) {
  TRANS_LOG(WARN, "stmt rollback info assign error", K(ret), K(stmt_rollback_info));
} else {
  scheduler_ = scheduler;
  coordinator_ = coordinator;
  commit_times_ = commit_times;
  stc_ = stc;
  is_dup_table_trans_ = is_dup_table_trans;
}

return ret;
}

int ObTrxCommitResponse::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
  const common::ObAddr &sender_addr, const int32_t status,
  const int64_t commit_times, const int64_t need_wait_interval_us)
{
int ret = OB_SUCCESS;

if ((OB_TRX_COMMIT_RESPONSE != msg_type && OB_TRX_ABORT_RESPONSE != msg_type)
    || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid()
    || !receiver.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid()) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time), K(sender),
      K(receiver), K(trans_param), K(sender_addr), K(status));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else {
  status_ = status;
  commit_times_ = commit_times;
  need_wait_interval_us_ = need_wait_interval_us;
}

return ret;
}

int ObTrx2PCPrePrepareRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
  const common::ObPartitionKey &coordinator, const common::ObPartitionArray &participants,
  const ObStartTransParam &trans_param, const common::ObAddr &sender_addr, const int32_t status,
  const int64_t request_id, const int64_t stc)
{
int ret = OB_SUCCESS;

if ((OB_TRX_2PC_PRE_PREPARE_REQUEST != msg_type && OB_TRX_2PC_CLEAR_RESPONSE != msg_type
    && OB_TRX_2PC_COMMIT_CLEAR_RESPONSE != msg_type)
    || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0
    || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid()
    || participants.count() <= 0 || !trans_param.is_valid() || !sender_addr.is_valid() || request_id < 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(scheduler), K(coordinator), K(participants), K(trans_param), K(sender_addr),
      K(status), K(request_id));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
  TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
} else {
  scheduler_ = scheduler;
  coordinator_ = coordinator;
  status_ = status;
  request_id_ = request_id;
  stc_ = stc;
}

return ret;
}

int ObTrx2PCPrePrepareResponse::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
  const common::ObPartitionKey &coordinator, const common::ObPartitionArray &participants,
  const ObStartTransParam &trans_param, const int64_t redo_prepare_logid,
  const int64_t redo_prepare_log_timestamp, const common::ObAddr &sender_addr,
  const int32_t status, const int64_t trans_version, const int64_t request_id)
{
int ret = OB_SUCCESS;

//log_id and log_timestamp maybe alloc failed and will be invalid
if ((OB_TRX_2PC_PRE_PREPARE_RESPONSE != msg_type) || !is_valid_tenant_id(tenant_id)
    || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() || !receiver.is_valid()
    || !scheduler.is_valid() || !coordinator.is_valid() || participants.count() <= 0
    || !trans_param.is_valid() || !sender_addr.is_valid() || request_id < 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(scheduler), K(coordinator), K(participants), K(trans_param), K(sender_addr),
      K(status), K(request_id), K(redo_prepare_logid), K(redo_prepare_log_timestamp));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
  TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
} else {
  scheduler_ = scheduler;
  coordinator_ = coordinator;
  status_ = status;
  request_id_ = request_id;
  prepare_log_id_ = redo_prepare_logid;
  prepare_log_timestamp_ = redo_prepare_log_timestamp;
  trans_version_ = trans_version;
}

return ret;
}

int ObTrx2PCLogIDRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
  const common::ObAddr &sender_addr, const int64_t request_id)
{
int ret = OB_SUCCESS;

if (OB_TRX_2PC_LOG_ID_REQUEST != msg_type
    || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0
    || !sender.is_valid() || !receiver.is_valid() || !trans_param.is_valid()
    || !sender_addr.is_valid() || request_id < 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(trans_param), K(sender_addr), K(request_id));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else {
  request_id_ = request_id;
}

return ret;
}

int ObTrx2PCLogIDResponse::init(const uint64_t tenant_id, const ObTransID &trans_id,
  const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
  const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
  const int64_t redo_prepare_logid, const int64_t redo_prepare_log_timestamp,
  const common::ObAddr &sender_addr, const int32_t status, const int64_t request_id,
  const PartitionLogInfoArray &arr)
{
int ret = OB_SUCCESS;
//log_id and log_timestamp maybe alloc failed and will be invalid
if ((OB_TRX_2PC_LOG_ID_RESPONSE != msg_type) || !is_valid_tenant_id(tenant_id)
    || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() || !receiver.is_valid()
    || !trans_param.is_valid() || !sender_addr.is_valid() || request_id < 0) {
  TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
      K(sender), K(receiver), K(trans_param), K(sender_addr), K(status), K(request_id),
      K(redo_prepare_logid), K(redo_prepare_log_timestamp));
  ret = OB_INVALID_ARGUMENT;
} else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
    sender, receiver, trans_param, sender_addr))) {
  TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
      K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
} else if (OB_FAIL(partition_log_info_arr_.assign(arr))) {
  TRANS_LOG(WARN, "partition log info array assign error", K(ret), K(arr));
} else {
  scheduler_ = scheduler;
  coordinator_ = coordinator;
  trans_version_ = trans_version;
  request_id_ = request_id;
  status_ = status;
}

return ret;
}
*/

int ObTrx2PCPrepareRequest::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const common::ObPartitionKey& coordinator,
    const common::ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const common::ObAddr& sender_addr, const int32_t status, const int64_t request_id, const MonotonicTs stc,
    const PartitionLogInfoArray& arr, const ObStmtRollbackInfo& stmt_rollback_info,
    const common::ObString& app_trace_info, const ObXATransID& xid, const bool is_xa_prepare)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_2PC_PREPARE_REQUEST != msg_type) || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
      trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
      !coordinator.is_valid() || participants.count() <= 0 || !trans_param.is_valid() || !sender_addr.is_valid() ||
      request_id < 0 || arr.count() < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(coordinator),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(status),
        K(request_id),
        K(arr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTrxMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTrxMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else if (OB_FAIL(partition_log_info_arr_.assign(arr))) {
    TRANS_LOG(WARN, "partition log info array assign error", K(ret), K(arr));
  } else if (OB_FAIL(stmt_rollback_info_.assign(stmt_rollback_info))) {
    TRANS_LOG(WARN, "stmt rollback info assign error", K(ret), K(stmt_rollback_info));
  } else {
    app_trace_info_ = app_trace_info;
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    status_ = status;
    request_id_ = request_id;
    stc_ = stc;
    xid_ = xid;
    is_xa_prepare_ = is_xa_prepare;
  }

  return ret;
}

int ObTrx2PCPrepareResponse::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const common::ObPartitionKey& coordinator,
    const common::ObPartitionArray& participants, const ObStartTransParam& trans_param, const int64_t prepare_logid,
    const int64_t prepare_log_timestamp, const common::ObAddr& sender_addr, const int32_t status, const int64_t state,
    const int64_t trans_version, const int64_t request_id, const PartitionLogInfoArray& arr,
    const int64_t need_wait_interval_us, const common::ObString& app_trace_info, const int64_t publish_version,
    const ObXATransID& xid, const bool is_xa_prepare)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_2PC_PREPARE_RESPONSE != msg_type) || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
      trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
      !coordinator.is_valid() || participants.count() <= 0 || !trans_param.is_valid() || !sender_addr.is_valid() ||
      !Ob2PCState::is_valid(state) || 0 >= trans_version || request_id < 0 ||
      (!is_xa_prepare && (prepare_logid <= 0 || prepare_log_timestamp < 0))) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(coordinator),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(status),
        K(state),
        K(trans_version),
        K(request_id),
        K(prepare_logid),
        K(prepare_log_timestamp),
        K(arr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTrxMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTrxMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else if (OB_FAIL(partition_log_info_arr_.assign(arr))) {
    TRANS_LOG(WARN, "partition log info array assign error", K(ret), K(arr));
  } else {
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    status_ = status;
    state_ = state;
    trans_version_ = trans_version;
    request_id_ = request_id;
    prepare_log_id_ = prepare_logid;
    prepare_log_timestamp_ = prepare_log_timestamp;
    need_wait_interval_us_ = need_wait_interval_us;
    app_trace_info_ = app_trace_info;
    publish_version_ = publish_version;
    xid_ = xid;
    is_xa_prepare_ = is_xa_prepare;
  }

  return ret;
}

int ObTrx2PCCommitRequest::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const common::ObPartitionKey& coordinator,
    const common::ObPartitionArray& participants, const ObStartTransParam& trans_param, const int64_t trans_version,
    const common::ObAddr& sender_addr, const int64_t request_id, const PartitionLogInfoArray& partition_log_info_arr,
    const int32_t status)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_2PC_COMMIT_REQUEST != msg_type && OB_TRX_2PC_PRE_COMMIT_REQUEST != msg_type &&
          OB_TRX_2PC_PRE_COMMIT_RESPONSE != msg_type) ||
      !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
      !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid() || participants.count() <= 0 ||
      !trans_param.is_valid() || !sender_addr.is_valid() || request_id < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(coordinator),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(request_id),
        K(status));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTrxMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTrxMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_FAIL(participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
    TRANS_LOG(WARN, "assign partition log info error", K(ret), K(partition_log_info_arr));
  } else {
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    trans_version_ = trans_version;
    request_id_ = request_id;
    status_ = status;
  }

  return ret;
}

int ObTrx2PCCommitResponse::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const common::ObPartitionKey& coordinator,
    const common::ObPartitionArray& participants, const ObStartTransParam& trans_param, const int64_t trans_version,
    const common::ObAddr& sender_addr, const int64_t request_id, const PartitionLogInfoArray& partition_log_info_arr,
    const int32_t status, const int64_t commit_log_ts)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_2PC_COMMIT_RESPONSE != msg_type) || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
      trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
      !coordinator.is_valid() || participants.count() <= 0 || !trans_param.is_valid() || !sender_addr.is_valid() ||
      request_id < 0 || commit_log_ts <= 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(coordinator),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(request_id),
        K(status),
        K(commit_log_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTrxMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTrxMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_FAIL(participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
    TRANS_LOG(WARN, "assign partition log info error", K(ret), K(partition_log_info_arr));
  } else {
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    trans_version_ = trans_version;
    request_id_ = request_id;
    status_ = status;
    commit_log_ts_ = commit_log_ts;
  }

  return ret;
}

int ObTrx2PCAbortRequest::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const common::ObPartitionKey& coordinator,
    const common::ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const common::ObAddr& sender_addr, const int64_t request_id, const int32_t status)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_2PC_ABORT_REQUEST != msg_type && OB_TRX_2PC_ABORT_RESPONSE != msg_type) ||
      !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
      !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid() || participants.count() <= 0 ||
      !trans_param.is_valid() || !sender_addr.is_valid() || request_id < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(coordinator),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(request_id),
        K(status));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTrxMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTrxMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else {
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    request_id_ = request_id;
    status_ = status;
  }

  return ret;
}

int ObTrx2PCClearRequest::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const common::ObPartitionKey& coordinator,
    const common::ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const common::ObAddr& sender_addr, const int64_t request_id, const int32_t status, int64_t clear_log_base_ts)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_2PC_CLEAR_REQUEST != msg_type && OB_TRX_2PC_CLEAR_RESPONSE != msg_type) ||
      !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
      !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid() || participants.count() <= 0 ||
      !trans_param.is_valid() || !sender_addr.is_valid() || request_id < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(coordinator),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(request_id),
        K(status));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTrxMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTrxMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else {
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    request_id_ = request_id;
    status_ = status;
    clear_log_base_ts_ = clear_log_base_ts;
  }

  return ret;
}
/*
int ObTrxClearRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
    const common::ObPartitionArray &participants, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const int64_t request_id, const int32_t status)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_CLEAR_REQUEST != msg_type && OB_TRX_CLEAR_RESPONSE != msg_type
      && OB_TRX_DISCARD_REQUEST != msg_type)
      || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0
      || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid()
      || !trans_param.is_valid() || !sender_addr.is_valid() || request_id <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
        K(sender), K(receiver), K(scheduler), K(participants), K(trans_param),
        K(sender_addr), K(status), K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = ObTrxMsgBase::init(tenant_id, trans_id, msg_type, trans_time,
      sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
        K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", K(ret), K(participants));
  } else {
    scheduler_ = scheduler;
    status_ = status;
    request_id_ = request_id;
  }

  return ret;
}

int ObTrxAskSchedulerRequest::init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const common::ObAddr &scheduler,
    const int32_t status)
{
  int ret = OB_SUCCESS;

  if ((OB_TRX_ASK_SCHEDULER_STATUS_REQUEST != msg_type &&
        OB_TRX_ASK_SCHEDULER_STATUS_RESPONSE != msg_type) ||
      !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 ||
      !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() || !sender_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(trans_id), K(msg_type), K(trans_time),
              K(sender), K(receiver), K(scheduler), K(sender_addr), K(status));
  } else if (OB_FAIL(ObTrxMsgBase::init(tenant_id,
                                          trans_id,
                                          msg_type,
                                          trans_time,
                                          sender,
                                          receiver,
                                          trans_param,
                                          sender_addr))) {
    TRANS_LOG(WARN, "ObTrxMsgBase init error", K(ret), K(tenant_id), K(trans_id), K(msg_type),
              K(trans_time), K(sender), K(receiver), K(trans_param), K(sender_addr));
  } else {
    scheduler_ = scheduler;
    status_ = status;
  }

  return ret;
}
*/

ObTransMsgUnion::ObTransMsgUnion(const int64_t msg_type)
{
  if (ObTransMsgType2Checker::is_valid_msg_type(msg_type)) {
    if (OB_TRX_2PC_CLEAR_REQUEST == msg_type || OB_TRX_2PC_CLEAR_RESPONSE == msg_type) {
      new (&data_) ObTrx2PCClearRequest();
    } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type || OB_TRX_2PC_ABORT_RESPONSE == msg_type) {
      new (&data_) ObTrx2PCAbortRequest();
    } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type ||
               OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type) {
      new (&data_) ObTrx2PCCommitRequest();
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type) {
      new (&data_) ObTrx2PCCommitResponse();
    } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type) {
      new (&data_) ObTrx2PCPrepareRequest();
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type) {
      new (&data_) ObTrx2PCPrepareResponse();
    } else {
      int ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected msg type", K(ret), K(msg_type));
    }
  } else {
    new (&data_) ObTransMsg();
  }
  msg_type_ = msg_type;
}

ObTransMsgUnion::~ObTransMsgUnion()
{
  if (ObTransMsgType2Checker::is_valid_msg_type(msg_type_)) {
    if (OB_TRX_2PC_CLEAR_REQUEST == msg_type_ || OB_TRX_2PC_CLEAR_RESPONSE == msg_type_) {
      data_.trx_2pc_clear_req.~ObTrx2PCClearRequest();
    } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type_ || OB_TRX_2PC_ABORT_RESPONSE == msg_type_) {
      data_.trx_2pc_abort_req.~ObTrx2PCAbortRequest();
    } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type_ || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type_ ||
               OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_req.~ObTrx2PCCommitRequest();
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_res.~ObTrx2PCCommitResponse();
    } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type_) {
      data_.trx_2pc_prepare_req.~ObTrx2PCPrepareRequest();
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type_) {
      data_.trx_2pc_prepare_res.~ObTrx2PCPrepareResponse();
    } else {
      int ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected msg type", K(ret), K_(msg_type));
    }
  } else {
    data_.trans_msg.~ObTransMsg();
  }
}

void ObTransMsgUnion::set_receiver(const common::ObPartitionKey& receiver)
{
  if (ObTransMsgType2Checker::is_valid_msg_type(msg_type_)) {
    if (OB_TRX_2PC_CLEAR_REQUEST == msg_type_ || OB_TRX_2PC_CLEAR_RESPONSE == msg_type_) {
      data_.trx_2pc_clear_req.receiver_ = receiver;
    } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type_ || OB_TRX_2PC_ABORT_RESPONSE == msg_type_) {
      data_.trx_2pc_abort_req.receiver_ = receiver;
    } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type_ || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type_ ||
               OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_req.receiver_ = receiver;
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_res.receiver_ = receiver;
    } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type_) {
      data_.trx_2pc_prepare_req.receiver_ = receiver;
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type_) {
      data_.trx_2pc_prepare_res.receiver_ = receiver;
    } else {
      int ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected msg type", K(ret), K_(msg_type));
    }
  } else {
    data_.trans_msg.set_receiver(receiver);
  }
}

int ObTransMsgUnion::set_batch_same_leader_partitions(const common::ObPartitionArray& partitions, const int64_t base_ts)
{
  int ret = OB_SUCCESS;

  if (ObTransMsgType2Checker::is_valid_msg_type(msg_type_)) {
    if (OB_TRX_2PC_CLEAR_REQUEST == msg_type_ || OB_TRX_2PC_CLEAR_RESPONSE == msg_type_) {
      if (OB_FAIL(data_.trx_2pc_clear_req.batch_same_leader_partitions_.assign(partitions))) {}
    } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type_ || OB_TRX_2PC_ABORT_RESPONSE == msg_type_) {
      if (OB_FAIL(data_.trx_2pc_abort_req.batch_same_leader_partitions_.assign(partitions))) {}
    } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type_ || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type_ ||
               OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type_) {
      if (OB_FAIL(data_.trx_2pc_commit_req.batch_same_leader_partitions_.assign(partitions))) {}
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type_) {
      if (base_ts <= 0) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(WARN, "invalid argument", K(ret), K_(msg_type), K(base_ts));
      } else if (OB_FAIL(data_.trx_2pc_commit_res.batch_same_leader_partitions_.assign(partitions))) {
      } else {
        data_.trx_2pc_commit_res.commit_log_ts_ = base_ts;
      }
    } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type_) {
      if (OB_FAIL(data_.trx_2pc_prepare_req.batch_same_leader_partitions_.assign(partitions))) {}
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type_) {
      if (OB_FAIL(data_.trx_2pc_prepare_res.batch_same_leader_partitions_.assign(partitions))) {}
    } else {
      int ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected msg type", K(ret), K_(msg_type));
    }
  } else {
    if (OB_FAIL(data_.trans_msg.set_batch_same_leader_partitions(partitions))) {}
  }

  return ret;
}

void ObTransMsgUnion::reset_batch_same_leader_partitions()
{
  if (ObTransMsgType2Checker::is_valid_msg_type(msg_type_)) {
    if (OB_TRX_2PC_CLEAR_REQUEST == msg_type_ || OB_TRX_2PC_CLEAR_RESPONSE == msg_type_) {
      data_.trx_2pc_clear_req.batch_same_leader_partitions_.reset();
    } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type_ || OB_TRX_2PC_ABORT_RESPONSE == msg_type_) {
      data_.trx_2pc_abort_req.batch_same_leader_partitions_.reset();
    } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type_ || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type_ ||
               OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_req.batch_same_leader_partitions_.reset();
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_res.batch_same_leader_partitions_.reset();
    } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type_) {
      data_.trx_2pc_prepare_req.batch_same_leader_partitions_.reset();
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type_) {
      data_.trx_2pc_prepare_res.batch_same_leader_partitions_.reset();
    } else {
      int ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected msg type", K(ret), K_(msg_type));
    }
  } else {
    data_.trans_msg.reset_batch_same_leader_partitions();
  }
}

ObTransMsgUnion& ObTransMsgUnion::operator=(const ObTrxMsgBase& msg)
{
  const int64_t msg_type = msg.msg_type_;
  ObTrxMsgBase* message = const_cast<ObTrxMsgBase*>(&msg);
  if (ObTransMsgType2Checker::is_valid_msg_type(msg_type_)) {
    if (OB_TRX_2PC_CLEAR_REQUEST == msg_type_ || OB_TRX_2PC_CLEAR_RESPONSE == msg_type_) {
      data_.trx_2pc_clear_req = *static_cast<ObTrx2PCClearRequest*>(message);
    } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type_ || OB_TRX_2PC_ABORT_RESPONSE == msg_type_) {
      data_.trx_2pc_abort_req = *static_cast<ObTrx2PCAbortRequest*>(message);
    } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type_ || OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type_ ||
               OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_req = *static_cast<ObTrx2PCCommitRequest*>(message);
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type_) {
      data_.trx_2pc_commit_res = *static_cast<ObTrx2PCCommitResponse*>(message);
    } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type_) {
      data_.trx_2pc_prepare_req = *static_cast<ObTrx2PCPrepareRequest*>(message);
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type_) {
      data_.trx_2pc_prepare_res = *static_cast<ObTrx2PCPrepareResponse*>(message);
    } else {
      int ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected msg type", K(ret), K_(msg_type));
    }
  } else {
    int ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected msg type", K(ret), K_(msg_type));
  }
  return *this;
}

}  // namespace transaction
}  // namespace oceanbase
