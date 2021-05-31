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
#include "ob_trans_msg.h"
#include "ob_trans_msg_type.h"

namespace oceanbase {
using namespace common;

namespace transaction {
// ObTansMsgBase
OB_SERIALIZE_MEMBER(ObTransMsgBase, tenant_id_, trans_id_, msg_type_, timestamp_, trans_time_, sender_, receiver_,
    trans_param_, sender_addr_);
// ObTransMsg
OB_SERIALIZE_MEMBER((ObTransMsg, ObTransMsgBase), scheduler_, coordinator_, participants_, prepare_log_id_,
    prepare_log_timestamp_, partition_log_info_arr_, sql_no_, status_, state_, trans_version_, request_id_,
    err_msg_type_, trans_location_cache_, commit_times_, cluster_id_, snapshot_version_, stmt_expired_time_,
    need_create_ctx_, active_memstore_version_, cluster_version_, snapshot_gene_type_, session_id_, proxy_session_id_,
    app_trace_id_str_, stc_.mts_, can_elr_, is_dup_table_trans_, is_not_create_ctx_participant_, stmt_rollback_info_,
    batch_same_leader_partitions_, app_trace_info_, xid_, is_xa_prepare_, additional_sql_no_,
    stmt_rollback_participants_, msg_timeout_);

// ObTransMsgBase
int ObTransMsgBase::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
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

void ObTransMsgBase::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  trans_id_.reset();
  msg_type_ = OB_TRANS_MSG_UNKNOWN;
  timestamp_ = 0;
  trans_time_ = 0;
  sender_.reset();
  receiver_.reset();
  trans_param_.reset();
  sender_addr_.reset();
}

bool ObTransMsgBase::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && trans_id_.is_valid() && timestamp_ > 0 && 0 < trans_time_ &&
         sender_.is_valid() && receiver_.is_valid() && trans_param_.is_valid() && sender_addr_.is_valid();
}

int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const ObAddr& sender_addr, const int64_t sql_no, const int32_t status, const int64_t state,
    const int64_t trans_version, const int64_t request_id, const MonotonicTs stc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if (!is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
             !receiver.is_valid() || participants.count() < 0 || !trans_param.is_valid() || !sender_addr.is_valid() ||
             sql_no < 0) {
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
        K(sql_no),
        K(status),
        K(state),
        K(trans_version),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    sql_no_ = sql_no;
    status_ = status;
    state_ = state;
    trans_version_ = trans_version;
    request_id_ = request_id;
    stc_ = stc;
  }

  return ret;
}

void ObTransMsg::reset()
{
  is_inited_ = false;
  scheduler_.reset();
  coordinator_.reset();
  participants_.reset();
  sql_no_ = 0;
  status_ = -1;
  state_ = Ob2PCState::UNKNOWN;
  trans_version_ = -1;
  prepare_log_id_ = 0;
  prepare_log_timestamp_ = -1;
  partition_log_info_arr_.reset();
  request_id_ = common::OB_INVALID_TIMESTAMP;
  stmt_expired_time_ = 0;
  err_msg_type_ = OB_TRANS_MSG_UNKNOWN;
  trans_location_cache_.reset();
  commit_times_ = 0;
  cluster_id_ = 0;
  snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  cluster_version_ = 0;
  ObTransMsgBase::reset();
  snapshot_gene_type_ = ObTransSnapshotGeneType::UNKNOWN;
  session_id_ = 0;
  proxy_session_id_ = 0;
  app_trace_id_str_.reset();
  stc_.reset();
  need_create_ctx_ = false;
  can_elr_ = false;
  active_memstore_version_.reset();
  is_dup_table_trans_ = false;
  is_not_create_ctx_participant_ = false;
  need_wait_interval_us_ = 0;
  stmt_rollback_info_.reset();
  batch_same_leader_partitions_.reset();
  app_trace_info_.reset();
  xid_.reset();
  is_xa_prepare_ = false;
  additional_sql_no_ = 0;
  stmt_rollback_participants_.reset();
  msg_timeout_ = INT64_MAX / 2;
}

// init for exception situation
int ObTransMsg::init(const int64_t msg_type, const int64_t err_msg_type, const uint64_t tenant_id,
    const ObTransID& trans_id, const ObPartitionKey& sender, const ObAddr& sender_addr, const int32_t status,
    const int64_t sql_no, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
    // don't check sql_no
  } else if ((OB_TRANS_ERROR_MSG != msg_type) || !is_valid_tenant_id(tenant_id) || !sender_addr.is_valid() ||
             !trans_id.is_valid() || !sender.is_valid() || !ObTransMsgTypeChecker::is_valid_msg_type(err_msg_type)) {
    TRANS_LOG(WARN, "invalid argument", K(msg_type), K(err_msg_type), K(tenant_id), K(trans_id), K(sender), K(status));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
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

// for message type OB_TRANS_START_STMT_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr, const int64_t sql_no, const uint64_t cluster_id,
    const int64_t request_id, const int64_t stmt_expired_time, const bool need_create_ctx,
    const uint64_t cluster_version, const int32_t snapshot_gene_type, const uint32_t session_id,
    const uint64_t proxy_session_id, const common::ObString& app_trace_id_str,
    const ObTransLocationCache& trans_location_cache, const bool can_elr, const bool is_not_create_ctx_participant)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_START_STMT_REQUEST != msg_type) || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
             !trans_param.is_valid() || !sender_addr.is_valid() || sql_no <= 0 || request_id <= 0 ||
             stmt_expired_time < 0 || cluster_version <= 0 || UINT32_MAX == session_id ||
             UINT64_MAX == proxy_session_id) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(trans_param),
        K(sender_addr),
        K(sql_no),
        K(cluster_id),
        K(request_id),
        K(stmt_expired_time),
        K(need_create_ctx),
        K(cluster_version),
        K(session_id),
        K(proxy_session_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else {
    (void)app_trace_id_str_.assign_ptr(app_trace_id_str.ptr(), app_trace_id_str.length());
    is_inited_ = true;
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

// for message type OB_TRANS_STMT_ROLLBACK_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const common::ObAddr& scheduler, const ObStartTransParam& trans_param, const common::ObAddr& sender_addr,
    const int64_t sql_no, const int64_t stmt_min_sql_no, const uint64_t cluster_id, const int64_t request_id,
    const int64_t stmt_expired_time, const bool need_create_ctx, const uint64_t cluster_version,
    const int32_t snapshot_gene_type, const uint32_t session_id, const uint64_t proxy_session_id,
    const common::ObString& app_trace_id_str, const ObTransLocationCache& trans_location_cache, const bool can_elr,
    const bool is_not_create_ctx_participant, const ObPartitionArray& stmt_rollback_participants)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_STMT_ROLLBACK_REQUEST != msg_type) || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
             !sender_addr.is_valid() || !trans_param.is_valid() || sql_no <= 0 || stmt_min_sql_no < 0 ||
             request_id <= 0 || stmt_expired_time < 0 || cluster_version <= 0 || UINT32_MAX == session_id ||
             UINT64_MAX == proxy_session_id) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(trans_param),
        K(sender_addr),
        K(sql_no),
        K(stmt_min_sql_no),
        K(cluster_id),
        K(request_id),
        K(stmt_expired_time),
        K(need_create_ctx),
        K(cluster_version),
        K(session_id),
        K(proxy_session_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        K(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = stmt_rollback_participants_.assign(stmt_rollback_participants))) {
  } else {
    (void)app_trace_id_str_.assign_ptr(app_trace_id_str.ptr(), app_trace_id_str.length());
    is_inited_ = true;
    sql_no_ = sql_no;
    additional_sql_no_ = stmt_min_sql_no;
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

// for message type OB_TRANS_START_STMT_RESPONSE, @TODO skip validate `snapshot_version`
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr, const int64_t sql_no,
    const int64_t snapshot_version, const int32_t status, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_TRANS_START_STMT_RESPONSE != msg_type || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !trans_param.is_valid() ||
             !sender_addr.is_valid() || sql_no <= 0 || request_id <= 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr),
        K(sql_no),
        K(snapshot_version),
        K(status),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransMsgBase::init(
                 tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else {
    is_inited_ = true;
    sql_no_ = sql_no;
    snapshot_version_ = snapshot_version;
    status_ = status;
    request_id_ = request_id;
  }

  return ret;
}

// for message type OB_TRANS_STMT_ROLLBACK_RESPONSE/OB_TRANS_SAVEPOINT_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr, const int64_t sql_no, const int32_t status,
    const int64_t request_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_STMT_ROLLBACK_RESPONSE != msg_type && OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE != msg_type) ||
             !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
             !receiver.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid() || sql_no < 0 ||
             request_id < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr),
        K(sql_no),
        K(status),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else {
    is_inited_ = true;
    sql_no_ = sql_no;
    status_ = status;
    request_id_ = request_id;
  }

  return ret;
}

// for message type OB_TRANS_SAVEPOINT_REQUEST, don't check location_cache count
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr, const int64_t sql_no, const int64_t cur_sql_no,
    const int64_t request_id, const ObTransLocationCache& trans_location_cache, const uint64_t cluster_version)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST != msg_type) || !is_valid_tenant_id(tenant_id) ||
             !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() ||
             !trans_param.is_valid() || !sender_addr.is_valid() || sql_no < 0 || cur_sql_no <= 0 || request_id < 0 ||
             cluster_version <= 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr),
        K(sql_no),
        K(cur_sql_no),
        K(request_id),
        K(cluster_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_FAIL(trans_location_cache_.assign(trans_location_cache))) {
    TRANS_LOG(WARN, "assign transaction location cache error", KR(ret), K(trans_location_cache));
  } else {
    is_inited_ = true;
    sql_no_ = sql_no;
    additional_sql_no_ = cur_sql_no;
    request_id_ = request_id;
    cluster_version_ = cluster_version;
  }

  return ret;
}

// for OB_TRANS_COMMIT_REQUEST / OB_TRANS_ABORT_REQUEST / OB_TRANS_XA_COMMIT_REQUEST / OB_TRANS_XA_ROLLBACK_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const ObAddr& sender_addr, const ObTransLocationCache& cache, const int64_t commit_times, const MonotonicTs stc,
    const bool is_dup_table_trans, const ObStmtRollbackInfo& stmt_rollback_info, const common::ObString& app_trace_info,
    const ObXATransID& xid)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_COMMIT_REQUEST != msg_type && OB_TRANS_ABORT_REQUEST != msg_type &&
                 OB_TRANS_XA_COMMIT_REQUEST != msg_type && OB_TRANS_XA_ROLLBACK_REQUEST != msg_type &&
                 OB_TRANS_XA_PREPARE_REQUEST != msg_type) ||
             !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
             !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid() ||
             (OB_TRANS_XA_COMMIT_REQUEST != msg_type && OB_TRANS_XA_ROLLBACK_REQUEST != msg_type &&
                 0 >= participants.count()) ||
             !trans_param.is_valid() || !sender_addr.is_valid()) {
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
        "trans_location_cache",
        cache);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else if (OB_SUCCESS != (ret = trans_location_cache_.assign(cache))) {
    TRANS_LOG(WARN, "assign transaction location cache error", K(ret), "trans_location_cache", cache);
  } else if (OB_FAIL(stmt_rollback_info_.assign(stmt_rollback_info))) {
    TRANS_LOG(WARN, "stmt rollback info assign error", K(ret), K(stmt_rollback_info));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    commit_times_ = commit_times;
    stc_ = stc;
    is_dup_table_trans_ = is_dup_table_trans;
    app_trace_info_ = app_trace_info;
    xid_ = xid;
  }

  return ret;
}

// OB_TRANS_COMMIT_RESPONSE, OB_TRANS_ABORT_RESPONSE, OB_TRANS_XA_PREPARE_RESPONSE, OB_TRANS_XA_ROLLBACK_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr, const int32_t status, const int64_t commit_times,
    const int64_t need_wait_interval_us)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_COMMIT_RESPONSE != msg_type && OB_TRANS_ABORT_RESPONSE != msg_type &&
                 OB_TRANS_XA_PREPARE_RESPONSE != msg_type && OB_TRANS_XA_ROLLBACK_RESPONSE != msg_type) ||
             !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
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
        K(sender_addr),
        K(status));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else {
    is_inited_ = true;
    status_ = status;
    commit_times_ = commit_times;
    need_wait_interval_us_ = need_wait_interval_us;
  }

  return ret;
}

// for OB TRANS_2PC_PREPARE_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const ObAddr& sender_addr, const int32_t status, const int64_t request_id, const MonotonicTs stc,
    const PartitionLogInfoArray& arr, const ObStmtRollbackInfo& stmt_rollback_info,
    const common::ObString& app_trace_info, const ObXATransID& xid, const bool is_xa_prepare)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_TRANS_2PC_PREPARE_REQUEST != msg_type || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
             !coordinator.is_valid() || participants.count() <= 0 || !trans_param.is_valid() ||
             !sender_addr.is_valid() || request_id < 0 || arr.count() < 0) {
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
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else if (OB_FAIL(partition_log_info_arr_.assign(arr))) {
    TRANS_LOG(WARN, "partition log info array assign error", K(ret), K(arr));
  } else if (OB_FAIL(stmt_rollback_info_.assign(stmt_rollback_info))) {
    TRANS_LOG(WARN, "stmt rollback info assign error", K(ret), K(stmt_rollback_info));
  } else {
    app_trace_info_ = app_trace_info;
    is_inited_ = true;
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

// for OB_TRANS_2PC_PRE_PREPARE_REQUEST, OB_TRANS_2PC_CLEAR_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const ObAddr& sender_addr, const int32_t status, const int64_t request_id, const MonotonicTs stc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_2PC_PRE_PREPARE_REQUEST != msg_type && OB_TRANS_2PC_CLEAR_RESPONSE != msg_type &&
                 OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE != msg_type) ||
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
        K(status),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    status_ = status;
    request_id_ = request_id;
    stc_ = stc;
  }

  return ret;
}

// for OB TRANS_2PC_PREPARE_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const int64_t prepare_logid, const int64_t prepare_log_timestamp, const ObAddr& sender_addr, const int32_t status,
    const int64_t state, const int64_t trans_version, const int64_t request_id, const PartitionLogInfoArray& arr,
    const int64_t need_wait_interval_us, const common::ObString& app_trace_info, const ObXATransID& xid,
    const bool is_xa_prepare)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_TRANS_2PC_PREPARE_RESPONSE != msg_type || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
             !coordinator.is_valid() || participants.count() <= 0 || !trans_param.is_valid() ||
             !sender_addr.is_valid() || !Ob2PCState::is_valid(state) ||
             (!is_xa_prepare && 0 >= trans_version) /* xa prepare don't generate trans_version */
             || request_id < 0 || prepare_logid <= 0 || prepare_log_timestamp < 0) {
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
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else if (OB_FAIL(partition_log_info_arr_.assign(arr))) {
    TRANS_LOG(WARN, "partition log info array assign error", KR(ret), K(arr));
  } else {
    is_inited_ = true;
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
    xid_ = xid;
    is_xa_prepare_ = is_xa_prepare;
  }

  return ret;
}

// for OB_TRANS_2PC_LOG_ID_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const ObAddr& sender_addr, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_TRANS_2PC_LOG_ID_REQUEST != msg_type || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !trans_param.is_valid() ||
             !sender_addr.is_valid() || request_id < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else {
    is_inited_ = true;
    request_id_ = request_id;
  }

  return ret;
}

// for OB TRANS_2PC_LOG_ID_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const int64_t redo_prepare_logid, const int64_t redo_prepare_log_timestamp,
    const ObAddr& sender_addr, const int32_t status, const int64_t request_id, const PartitionLogInfoArray& arr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
    // logid and timestamp maybe alloc failed
  } else if ((OB_TRANS_2PC_LOG_ID_RESPONSE != msg_type) || !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() ||
             trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() || !trans_param.is_valid() ||
             !sender_addr.is_valid() || request_id < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr),
        K(status),
        K(request_id),
        K(redo_prepare_logid),
        K(redo_prepare_log_timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_FAIL(partition_log_info_arr_.assign(arr))) {
    TRANS_LOG(WARN, "partition log info array assign error", KR(ret), K(arr));
  } else {
    is_inited_ = true;
    status_ = status;
    request_id_ = request_id;
    prepare_log_id_ = redo_prepare_logid;
    prepare_log_timestamp_ = redo_prepare_log_timestamp;
  }

  return ret;
}

// for OB TRANS_2PC_PRE_PREPARE_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const int64_t redo_prepare_logid, const int64_t redo_prepare_log_timestamp, const ObAddr& sender_addr,
    const int32_t status, const int64_t trans_version, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
    // logid and timestamp maybe alloced fail
  } else if ((OB_TRANS_2PC_PRE_PREPARE_RESPONSE != msg_type) || !is_valid_tenant_id(tenant_id) ||
             !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() || !receiver.is_valid() ||
             !scheduler.is_valid() || !coordinator.is_valid() || participants.count() <= 0 || !trans_param.is_valid() ||
             !sender_addr.is_valid() || request_id < 0) {
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
        K(redo_prepare_logid),
        K(redo_prepare_log_timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else {
    is_inited_ = true;
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

// for OB_TRANS_2PC_COMMIT_REQUEST, OB_TRANS_2PC_COMMIT_RESPONSE,
// OB_TRANS_2PC_PRE_COMMIT_REQUEST, OB_TRANS_PRE_COMMIT_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const int64_t trans_version, const ObAddr& sender_addr, const int64_t request_id,
    const PartitionLogInfoArray& partition_log_info_arr, const int32_t status)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_2PC_COMMIT_REQUEST != msg_type && OB_TRANS_2PC_COMMIT_RESPONSE != msg_type &&
                 OB_TRANS_2PC_COMMIT_CLEAR_REQUEST != msg_type && OB_TRANS_2PC_PRE_COMMIT_REQUEST != msg_type &&
                 OB_TRANS_2PC_PRE_COMMIT_RESPONSE != msg_type) ||
             !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
             !receiver.is_valid() || !scheduler.is_valid() || !coordinator.is_valid() || participants.count() <= 0 ||
             !trans_param.is_valid() || 0 >= trans_version || !sender_addr.is_valid() || request_id < 0 ||
             OB_SUCCESS != status) {
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
        K(trans_version),
        K(request_id),
        K(partition_log_info_arr),
        K(status));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else if (OB_SUCCESS != (ret = partition_log_info_arr_.assign(partition_log_info_arr))) {
    TRANS_LOG(WARN, "assign partition logid array error", KR(ret), K(partition_log_info_arr));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    trans_version_ = trans_version;
    request_id_ = request_id;
    status_ = status;
  }

  return ret;
}

// for OB_TRANS_2PC_ABORT_REQUEST, OB_TRANS_2PC_ABORT_RESPONSE, OB_TRANS_2PC_CLEAR_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const ObStartTransParam& trans_param,
    const ObAddr& sender_addr, const int64_t request_id, const int32_t status)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_2PC_ABORT_REQUEST != msg_type && OB_TRANS_2PC_ABORT_RESPONSE != msg_type &&
                 OB_TRANS_2PC_CLEAR_REQUEST != msg_type) ||
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
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    coordinator_ = coordinator;
    request_id_ = request_id;
    status_ = status;
  }

  return ret;
}

// for OB_TRANS_CLEAR_REQUEST, OB_TRANS_CLEAR_RESPONSE, OB_TRANS_DISCARD_REQUEST
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const ObPartitionKey& sender, const ObPartitionKey& receiver, const ObAddr& scheduler,
    const ObPartitionArray& participants, const ObStartTransParam& trans_param, const ObAddr& sender_addr,
    const int64_t request_id, const int32_t status)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_TRANS_CLEAR_REQUEST != msg_type && OB_TRANS_CLEAR_RESPONSE != msg_type &&
                 OB_TRANS_DISCARD_REQUEST != msg_type) ||
             !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 || !sender.is_valid() ||
             !receiver.is_valid() || !scheduler.is_valid() || !trans_param.is_valid() || !sender_addr.is_valid() ||
             request_id <= 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(participants),
        K(trans_param),
        K(sender_addr),
        K(status),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = ObTransMsgBase::init(
                  tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign participants error", KR(ret), K(participants));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    status_ = status;
    request_id_ = request_id;
  }

  return ret;
}

// for OB_TRANS_ASK_SCHEDULER_REQUEST, OB_TRANS_ASK_SCHEDULER_RESPONSE
int ObTransMsg::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t trans_time, const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
    const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const common::ObAddr& scheduler,
    const int32_t status)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTransMsg init twice");
  } else if (OB_UNLIKELY((OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST != msg_type &&
                             OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE != msg_type) ||
                         !is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || trans_time <= 0 ||
                         !sender.is_valid() || !receiver.is_valid() || !scheduler.is_valid() ||
                         !sender_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(scheduler),
        K(sender_addr),
        K(status));
  } else if (OB_FAIL(ObTransMsgBase::init(
                 tenant_id, trans_id, msg_type, trans_time, sender, receiver, trans_param, sender_addr))) {
    TRANS_LOG(WARN,
        "ObTransMsgBase init error",
        KR(ret),
        K(tenant_id),
        K(trans_id),
        K(msg_type),
        K(trans_time),
        K(sender),
        K(receiver),
        K(trans_param),
        K(sender_addr));
  } else {
    is_inited_ = true;
    scheduler_ = scheduler;
    status_ = status;
  }

  return ret;
}

bool ObTransMsg::is_valid() const
{
  bool ret = ObTransMsgBase::is_valid();

  // valid each type of message (16 total)
  if (OB_TRANS_ERROR_MSG == msg_type_) {
    ret = (is_valid_tenant_id(tenant_id_) && trans_id_.is_valid() && sender_.is_valid() &&
           ObTransMsgTypeChecker::is_valid_msg_type(err_msg_type_));
  } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type_ || OB_TRANS_STMT_ROLLBACK_RESPONSE == msg_type_) {
    ret = (ret && (sql_no_ >= 0) && (request_id_ > 0));
  } else if (OB_TRANS_COMMIT_REQUEST == msg_type_ || OB_TRANS_ABORT_REQUEST == msg_type_ ||
             OB_TRANS_XA_COMMIT_REQUEST == msg_type_ || OB_TRANS_XA_ROLLBACK_REQUEST == msg_type_) {
    ret = (ret && (scheduler_.is_valid()) && (coordinator_.is_valid()));
  } else if (OB_TRANS_COMMIT_RESPONSE == msg_type_ || OB_TRANS_ABORT_RESPONSE == msg_type_ ||
             OB_TRANS_XA_ROLLBACK_RESPONSE == msg_type_ || OB_TRANS_XA_PREPARE_RESPONSE == msg_type_ ||
             OB_TRANS_XA_PREPARE_REQUEST == msg_type_) {
    // do nothing
  } else if (OB_TRANS_2PC_PREPARE_REQUEST == msg_type_ || OB_TRANS_2PC_CLEAR_REQUEST == msg_type_ ||
             OB_TRANS_2PC_CLEAR_RESPONSE == msg_type_) {
    ret = (ret && (scheduler_.is_valid()) && (coordinator_.is_valid()) && (participants_.count() > 0) &&
           (request_id_ > 0));
  } else if (OB_TRANS_2PC_PREPARE_RESPONSE == msg_type_) {
    ret = (ret && (scheduler_.is_valid()) && (coordinator_.is_valid()) && (participants_.count() > 0) &&
           (trans_version_ > 0) && (request_id_ > 0));
  } else if (OB_TRANS_2PC_COMMIT_REQUEST == msg_type_ || OB_TRANS_2PC_COMMIT_RESPONSE == msg_type_) {
    if (1 == participants_.count()) {
      // single partition transaction, don't need check partition_log_info_arr
      ret = (ret && (scheduler_.is_valid()) && (coordinator_.is_valid()) && (participants_.count() > 0) &&
             (trans_version_ > 0) && (request_id_ > 0) && OB_SUCCESS == status_);
    } else {
      // multi-partition transaction, 0 < participants_logid_arr.count()
      ret = (ret && (scheduler_.is_valid()) && (coordinator_.is_valid()) && (participants_.count() > 0) &&
             (trans_version_ > 0) && (request_id_ > 0) && (partition_log_info_arr_.count() > 0) &&
             OB_SUCCESS == status_);
    }
  } else if (OB_TRANS_2PC_ABORT_REQUEST == msg_type_ || OB_TRANS_2PC_ABORT_RESPONSE == msg_type_) {
    ret = (ret && (scheduler_.is_valid()) && (coordinator_.is_valid()) && (participants_.count() > 0) &&
           (request_id_ > 0));
  } else if (OB_TRANS_CLEAR_REQUEST == msg_type_ || OB_TRANS_CLEAR_RESPONSE == msg_type_) {
    ret = (ret && (scheduler_.is_valid()) && (participants_.count() > 0));
  } else if (OB_TRANS_DISCARD_REQUEST == msg_type_) {
    ret = (ret && scheduler_.is_valid());
  } else if (OB_TRANS_START_STMT_REQUEST == msg_type_) {
    ret = (ret && request_id_ > 0);
  } else if (OB_TRANS_START_STMT_RESPONSE == msg_type_) {
    //@TODO skip valid `snapshot_version`
    ret = (ret && request_id_ > 0);
  } else if (OB_TRANS_2PC_PRE_PREPARE_REQUEST == msg_type_ || OB_TRANS_2PC_PRE_PREPARE_RESPONSE == msg_type_ ||
             OB_TRANS_2PC_PRE_COMMIT_REQUEST == msg_type_ || OB_TRANS_2PC_PRE_COMMIT_RESPONSE == msg_type_ ||
             OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg_type_ || OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE == msg_type_ ||
             OB_TRANS_2PC_LOG_ID_REQUEST == msg_type_ || OB_TRANS_2PC_LOG_ID_RESPONSE == msg_type_ ||
             OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST == msg_type_ ||
             OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE == msg_type_ || OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type_ ||
             OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE == msg_type_) {
    // do nothing
  } else {
    ret = false;
  }

  return ret;
}

int ObTransMsg::set_prepare_log_id(const int64_t prepare_log_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg not init");
    ret = OB_NOT_INIT;
  } else if (prepare_log_id <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(prepare_log_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    prepare_log_id_ = prepare_log_id;
    TRANS_LOG(DEBUG, "set prepare log info successfully");
  }
  return ret;
}

int ObTransMsg::set_msg_timeout(const int64_t msg_timeout)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg not init");
    ret = OB_NOT_INIT;
  } else if (msg_timeout <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(msg_timeout));
    ret = OB_INVALID_ARGUMENT;
  } else {
    msg_timeout_ = msg_timeout;
    TRANS_LOG(DEBUG, "set msg timeout timestamp successfully");
  }
  return ret;
}

int ObTransMsg::set_partition_log_info_arr(const PartitionLogInfoArray& array)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransMsg not init");
    ret = OB_NOT_INIT;
  } else if (array.count() < 0) {
    TRANS_LOG(WARN, "invalid argument", K(array));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = partition_log_info_arr_.assign(array))) {
    TRANS_LOG(WARN, "PartitionLogInfoArray assign error", KR(ret));
  } else {
    TRANS_LOG(DEBUG, "set partition log info array successfully");
  }
  return ret;
}

int ObTransMsg::set_batch_same_leader_partitions(const common::ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;

  batch_same_leader_partitions_.reset();
  if (OB_FAIL(batch_same_leader_partitions_.assign(partitions))) {
    TRANS_LOG(WARN, "assign batch same leader partitions", K(ret));
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
