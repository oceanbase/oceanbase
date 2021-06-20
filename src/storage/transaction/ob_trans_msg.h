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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_MSG_
#define OCEANBASE_TRANSACTION_OB_TRANS_MSG_

#include "share/ob_define.h"
#include "ob_trans_define.h"
#include "share/rpc/ob_batch_proxy.h"

namespace oceanbase {
namespace transaction {
/*
 * Transaction message 16 types, use overried init function to initalize different message
 * and each message should contains following properties:
 *    uint64_t tenant_id_;
 *    ObTransID trans_id_;
 *    int64_t msg_type_;
 *    common::ObPartitionKey sender_;
 *    common::ObPartitionKey receiver_;
 *    ObStartParam trans_param_;
 *    int64_t time_stamp_;             // message born timestamp
 *    int64_t trans_time_;             // transaction timeout absolute time in us
 *    ObAddr sender_addr_;             // addr of the server where message born
 */
class ObTransMsgBase : public obrpc::ObIFill {
  OB_UNIS_VERSION(1);

public:
  ObTransMsgBase()
  {
    reset();
  }
  ~ObTransMsgBase()
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr);
  void reset();
  bool is_valid() const;

public:
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int64_t get_msg_type() const
  {
    return msg_type_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  int64_t get_trans_time() const
  {
    return trans_time_;
  }
  const common::ObPartitionKey& get_sender() const
  {
    return sender_;
  }
  const common::ObPartitionKey& get_receiver() const
  {
    return receiver_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  const common::ObAddr& get_sender_addr() const
  {
    return sender_addr_;
  }

protected:
  uint64_t tenant_id_;
  ObTransID trans_id_;
  int64_t msg_type_;
  int64_t trans_time_;
  common::ObPartitionKey sender_;
  common::ObPartitionKey receiver_;
  ObStartTransParam trans_param_;
  common::ObAddr sender_addr_;
  int64_t timestamp_;
};

class ObTransMsg : public ObTransMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTransMsg()
      : participants_(common::ObModIds::OB_TRANS_PARTITION_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        partition_log_info_arr_(
            common::ObModIds::OB_TRANS_PARTITION_LOG_INFO_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        stmt_rollback_participants_(common::ObModIds::OB_TRANS_PARTITION_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE)
  {
    reset();
  }
  ~ObTransMsg()
  {}
  void reset();

  // with all parameters
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t sql_no,
      const int32_t status, const int64_t state, const int64_t trans_version, const int64_t request_id,
      const MonotonicTs stc);

  // for error msg
  int init(const int64_t msg_type, const int64_t err_msg_type, const uint64_t tenant_id, const ObTransID& trans_id,
      const common::ObPartitionKey& sender, const common::ObAddr& sender_addr, const int32_t status,
      const int64_t sql_no, const int64_t request_id);

  // 1.message type: OB_TRANS_START_STMT_REQUEST
  // 2.extra param: sql_no_, request_id_, stmt_expired_time_
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t sql_no,
      const uint64_t cluster_id, const int64_t request_id, const int64_t stmt_expired_time, const bool need_create_ctx,
      const uint64_t cluster_version, const int32_t snapshot_gene_type, const uint32_t session_id,
      const uint64_t proxy_session_id, const common::ObString& app_trace_id_str,
      const ObTransLocationCache& trans_location_cache, const bool can_elr, const bool is_not_create_ctx_participant);

  // 1.message type: OB_TRANS_STMT_ROLLBACK_REQUEST
  // 2.extra param: sql_no_, additional_sql_no_, request_id_, stmt_expired_time_
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t sql_no,
      const int64_t stmt_min_sql_no, const uint64_t cluster_id, const int64_t request_id,
      const int64_t stmt_expired_time, const bool need_create_ctx, const uint64_t cluster_version,
      const int32_t snapshot_gene_type, const uint32_t session_id, const uint64_t proxy_session_id,
      const common::ObString& app_trace_id_str, const ObTransLocationCache& trans_location_cache, const bool can_elr,
      const bool is_not_create_ctx_participant, const common::ObPartitionArray& stmt_rollback_participants);

  // 1.message type: OB_TRANS_START_STMT_RESPONSE
  // 2.extra param: sql_no_, snapshot_version, status
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t sql_no,
      const int64_t snapshot_version, const int32_t status, const int64_t request_id);

  // 1.message type: OB_TRANS_STMT_ROLLBACK_RESPONSE, OB_TRANS_SAVEPOINT_RESPONSE
  // 2.extra param: sql_no_, status_
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t sql_no,
      const int32_t status, const int64_t request_id);

  // 1.message type: OB_TRANS_STMT_SAVEPOINT_REQUEST
  // 2.extra param: sql_no_, additional_sql_no_
  // 3.note:
  //   (1) scheudler_: not required, ctx maybe non-exist when participant recieve this message, don't reply.
  //   (2) coordinator_ and participants_: not required
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t sql_no,
      const int64_t cur_sql_no, const int64_t request_id, const ObTransLocationCache& trans_location_cache,
      const uint64_t cluster_version);

  // 1.message type: OB_TRANS_COMMIT_REQUEST, OB_TRANS_ABORT_REQUEST
  // 2.extra param: scheduler_, coordinator_, participants_, xid_
  // 3.note:
  //   (1) scheduler_: required, used to send response message to
  //   (2) coordinator_: required, currently unused, but should pass for future extention.
  //   (3) participant_: required, coordinator need known participants
  //   (4) xid: required for XA, and identified XA transaction
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr,
      const ObTransLocationCache& trans_location_cache, const int64_t commit_times, const MonotonicTs stc,
      const bool is_dup_table_trans, const ObStmtRollbackInfo& stmt_rollback_info,
      const common::ObString& app_trace_info, const ObXATransID& xid);

  // 1.message type: OB_TRANS_COMMIT_RESPONSE, OB_TRANS_ABORT_RESPONSE
  // 2.extra param: status_
  // 3.note:
  //   (1) status_: required, indicate commit result
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int32_t status,
      const int64_t commit_times, const int64_t need_wait_interval_us);

  // 1.message type: OB_TRANS_2PC_PREPARE_REQUEST, OB_TRANS_2PC_CLEAR_RESPONSE
  // 2.extra param: scheduler_, coordinator_, participats_, status_, request_id_
  // 3.note:
  //   (1) scheduler_,coordinator_, participants_: required, use to recovery after coordinator failure
  //   (2) status_: required, used to identify participant's commit/abort state
  //   (3) request_id_: required, used to reject steal message
  //   (4) xid: required for XA
  //   (5) is_xa_prepare: required for XA
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int32_t status,
      const int64_t request_id, const MonotonicTs stc, const PartitionLogInfoArray& arr,
      const ObStmtRollbackInfo& stmt_rollback_info, const common::ObString& app_trace_info, const ObXATransID& xid,
      const bool is_xa_prepare);

  // 1.message type: OB_TRANS_2PC_PREPARE_RESPONSE
  // 2.extra param: scheduler_, coordinator_, participats_, status_, state_, trans_version_, request_id_,
  //             xid_, is_xa_prepare_
  // 3.note:
  //   (1) scheduler_,coordinator_, participants_: required for recovery
  //   (2) status_: the result of 2pc_prepare
  //   (3) state_: should be in PREPARE, COMMIT, ABORT or CLEAR
  //   (4) trans_version_: participant's local prepare version
  //   (5) request_id_: used to reject steal message
  //   (6) xid: required for XA
  //   (7) is_xa_prepare: required for XA
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const int64_t prepare_logid, const int64_t prepare_log_timestamp,
      const common::ObAddr& sender_addr, const int32_t status, const int64_t state, const int64_t trans_version,
      const int64_t request_id, const PartitionLogInfoArray& arr, const int64_t need_wait_interval_us,
      const common::ObString& app_trace_info, const ObXATransID& xid, const bool is_xa_prepare);

  // 1.message type: OB_TRANS_2PC_PRE_PREPARE_REQUEST
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int32_t status,
      const int64_t request_id, const MonotonicTs stc);

  // 1.message type: OB_TRANS_2PC_PRE_PREPARE_RESPONSE
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const int64_t redo_prepare_logid, const int64_t redo_prepare_log_timestamp,
      const common::ObAddr& sender_addr, const int32_t status, const int64_t trans_version, const int64_t request_id);

  // 1.message type: OB_TRANS_2PC_LOG_ID_REQUEST
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t request_id);

  // 1.message type: OB_TRANS_2PC_LOG_ID_RESPONSE
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const int64_t redo_prepare_logid, const int64_t redo_prepare_log_timestamp,
      const common::ObAddr& sender_addr, const int32_t status, const int64_t request_id,
      const PartitionLogInfoArray& arr);

  // 1.message type: OB_TRANS_2PC_COMMIT_REQUEST, OB_TRANS_2PC_COMMIT_RESPONSE
  // 2.extra param: scheduler_, coordinator_, participats_, trans_version_, request_id_
  // 3.note:
  //   (1) scheduler_,coordinator_, participants_: required for revoery after coordinator failover
  //   (2) status_: non-required, transaction already decided
  //   (3) trans_version_: required, it also need for response of commit, because of the coordinator
  //                      maybe failure and new coordinator need to known trans_version to continue
  //   (4) request_id_: required to reject steal message
  //   (5) status: required and should be OB_SUCCESS for COMMIT_RESPONSE, which used for re-create coordinator
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const int64_t trans_version, const common::ObAddr& sender_addr,
      const int64_t request_id, const PartitionLogInfoArray& partition_log_info_arr, const int32_t status);

  // 1.message type: OB_TRANS_2PC_ABORT_REQUEST, OB_TRANS_2PC_ABORT_RESPONSE, OB_TRANS_2PC_CLEAR_REQUEST
  // 2.extra param: scheduler_, coordinator_, participats_, request_id_, status_
  // 3.note:
  //   (1) scheduler_, coordinator_, participants_: required for revoery after coordinator failover
  //   (2) status_: required and persistent on participant
  //   (3) request_id_: used to reject steal message
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t request_id,
      const int32_t status);

  // 1.message type: OB_TRANS_CLEAR_REQUEST, OB_TRANS_CLEAR_RESPONSE, OB_TRANS_DISCARD_REQUEST
  // 2.extra param: scheduler_, participats_, status_
  // 3.note: used to let participant forget transaction
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionArray& participants, const ObStartTransParam& trans_param,
      const common::ObAddr& sender_addr, const int64_t request_id, const int32_t status);

  // 1.message type: OB_TRANS_ASK_SCHEDULER_REQUEST, OB_TRANS_ASK_SCHEDULER_RESPONSE
  // 2.extra param: scheduler, status
  // 3.note: use to test whether scheduler is alive and transaction is active
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const common::ObAddr& scheduler,
      const int32_t status);

public:
  const common::ObAddr& get_scheduler() const
  {
    return scheduler_;
  }
  const common::ObPartitionKey& get_coordinator() const
  {
    return coordinator_;
  }
  const common::ObPartitionArray& get_participants() const
  {
    return participants_;
  }
  int64_t get_sql_no() const
  {
    return sql_no_;
  }
  int64_t get_additional_sql_no() const
  {
    return cluster_version_before_2271_() ? sql_no_ : additional_sql_no_;
  }
  int32_t get_status() const
  {
    return status_;
  }
  int64_t get_state() const
  {
    return state_;
  }
  // for prepare log id
  int set_prepare_log_id(const int64_t prepare_logid);
  int64_t get_prepare_log_id() const
  {
    return prepare_log_id_;
  }
  int64_t get_prepare_log_timestamp() const
  {
    return prepare_log_timestamp_;
  }
  // for prepare log ids
  int set_partition_log_info_arr(const PartitionLogInfoArray& array);
  const PartitionLogInfoArray& get_partition_log_info_arr() const
  {
    return partition_log_info_arr_;
  }
  int64_t get_trans_version() const
  {
    return trans_version_;
  }
  int64_t get_request_id() const
  {
    return request_id_;
  }
  int64_t get_err_msg_type() const
  {
    return err_msg_type_;
  }
  const ObTransLocationCache& get_trans_location_cache() const
  {
    return trans_location_cache_;
  }
  int64_t get_commit_times() const
  {
    return commit_times_;
  }
  uint64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  int64_t get_stmt_expired_time() const
  {
    return stmt_expired_time_;
  }
  bool need_create_ctx() const
  {
    return need_create_ctx_;
  }
  uint64_t get_cluster_version() const
  {
    return cluster_version_;
  }
  int set_snapshot_gene_type(const int type)
  {
    snapshot_gene_type_ = type;
    return common::OB_SUCCESS;
  }
  int32_t get_snapshot_gene_type() const
  {
    return snapshot_gene_type_;
  }
  int set_session_id(const uint32_t session_id)
  {
    session_id_ = session_id;
    return common::OB_SUCCESS;
  }
  uint32_t get_session_id() const
  {
    return session_id_;
  }
  int set_proxy_session_id(const uint64_t proxy_session_id)
  {
    proxy_session_id_ = proxy_session_id;
    return common::OB_SUCCESS;
  }
  uint64_t get_proxy_session_id() const
  {
    return proxy_session_id_;
  }
  const common::ObString& get_app_trace_id_str() const
  {
    return app_trace_id_str_;
  }
  MonotonicTs get_stc() const
  {
    return stc_;
  }
  // int64_t get_stc() const { return stc_; }
  const ObXATransID& get_xid() const
  {
    return xid_;
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  bool is_xa_prepare() const
  {
    return is_xa_prepare_;
  }
  bool is_dup_table_trans() const
  {
    return is_dup_table_trans_;
  }
  bool is_not_create_ctx_participant() const
  {
    return is_not_create_ctx_participant_;
  }
  int64_t get_need_wait_interval_us() const
  {
    return need_wait_interval_us_;
  }
  const ObStmtRollbackInfo& get_stmt_rollback_info() const
  {
    return stmt_rollback_info_;
  }
  void set_receiver(const common::ObPartitionKey& pkey)
  {
    receiver_ = pkey;
  }
  int set_batch_same_leader_partitions(const common::ObPartitionArray& partitions);
  const common::ObPartitionArray& get_batch_same_leader_partitions() const
  {
    return batch_same_leader_partitions_;
  }
  void reset_batch_same_leader_partitions()
  {
    batch_same_leader_partitions_.reset();
  }
  const common::ObString& get_app_trace_info() const
  {
    return app_trace_info_;
  }
  int set_msg_timeout(const int64_t msg_timeout);
  int64_t get_msg_timeout() const
  {
    return msg_timeout_;
  }

  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }

  TO_STRING_KV(K_(tenant_id), K_(trans_id), K_(msg_type), K_(err_msg_type), K_(sender), K_(receiver), K_(scheduler),
      K_(coordinator), K_(participants), K_(trans_param), K_(sender_addr), K_(prepare_log_id),
      K_(prepare_log_timestamp), K_(partition_log_info_arr), K_(sql_no), K_(additional_sql_no), K_(status), K_(state),
      K_(trans_version), K_(request_id), K_(trans_location_cache), K_(commit_times), K_(cluster_id),
      K_(snapshot_version), K_(stmt_expired_time), K_(need_create_ctx), K_(cluster_version), K_(snapshot_gene_type),
      K_(can_elr), K_(is_dup_table_trans), K_(is_not_create_ctx_participant), K_(batch_same_leader_partitions),
      K_(app_trace_info), K_(xid), K_(is_xa_prepare), K_(stmt_rollback_participants), K_(msg_timeout));

public:
  bool is_valid() const;

private:
  bool cluster_version_before_2271_() const
  {
    return cluster_version_ < CLUSTER_VERSION_2271;
  }

private:
  bool is_inited_;
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t prepare_log_id_;
  int64_t prepare_log_timestamp_;
  PartitionLogInfoArray partition_log_info_arr_;
  int64_t sql_no_;
  int32_t status_;
  int64_t state_;
  int64_t trans_version_;
  int64_t request_id_;
  int64_t err_msg_type_;
  ObTransLocationCache trans_location_cache_;
  int64_t commit_times_;
  uint64_t cluster_id_;
  int64_t snapshot_version_;
  int64_t stmt_expired_time_;
  bool need_create_ctx_;
  common::ObVersion active_memstore_version_;
  uint64_t cluster_version_;
  int snapshot_gene_type_;
  uint32_t session_id_;
  uint64_t proxy_session_id_;
  common::ObString app_trace_id_str_;
  MonotonicTs stc_;
  bool can_elr_;
  bool is_dup_table_trans_;
  bool is_not_create_ctx_participant_;
  int64_t need_wait_interval_us_;
  ObStmtRollbackInfo stmt_rollback_info_;
  common::ObPartitionArray batch_same_leader_partitions_;
  common::ObString app_trace_info_;
  ObXATransID xid_;
  bool is_xa_prepare_;
  int64_t additional_sql_no_;
  common::ObPartitionArray stmt_rollback_participants_;
  int64_t msg_timeout_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_MSG_
