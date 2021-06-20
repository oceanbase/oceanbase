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

#ifndef OCEANBASE_CLOG_OB_LOG_REQ_
#define OCEANBASE_CLOG_OB_LOG_REQ_

#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_log_type.h"
#include "common/ob_idc.h"
#include "share/ob_cascad_member_list.h"
#include "common/ob_region.h"
#include "share/rpc/ob_batch_proxy.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace clog {
enum ObLogReqType {
  OB_INVALID_REQ = 0,
  OB_PUSH_LOG = 1,
  OB_FETCH_LOG = 2,
  OB_ACK_LOG = 3,
  OB_PREPARE_REQ = 4,
  OB_PREPARE_RESP = 5,
  OB_FETCH_LOG_RESP = 6,
  OB_CONFIRMED_INFO_REQ = 7,
  OB_LEADER_SW_INFO_RESP = 8,
  OB_NOTIFY_LOG_MISSING = 9,
  OB_FETCH_REGISTER_SERVER_REQ = 10,
  OB_FETCH_REGISTER_SERVER_RESP = 11,
  OB_REJECT_MSG = 12,
  OB_PARENT_NEXT_ILOG_INFO_RESP = 13,
  OB_BROADCAST_INFO_REQ = 14,
  OB_BATCH_PUSH_LOG = 15,
  OB_BATCH_ACK_LOG = 16,
  OB_LEADER_SW_INFO_RESP_V2 = 17,
  OB_REPLACE_SICK_CHILD_REQ = 18,
  OB_FETCH_LOG_V2 = 19,
  OB_ACK_LOG_V2 = 20,
  OB_PREPARE_RESP_V2 = 21,
  OB_FETCH_REGISTER_SERVER_REQ_V2 = 22,
  OB_KEEPALIVE_MSG = 23,
  OB_LEADER_MAX_LOG_MSG = 24,
  OB_FETCH_REGISTER_SERVER_RESP_V2 = 25,
  OB_REREGISTER_MSG = 26,
  OB_CHECK_REBUILD_REQ = 27,
  // newly added in 2.1.1, for big trans
  OB_FAKE_ACK_LOG = 30,
  OB_FAKE_PUSH_LOG = 31,
  // used for log archive & restore
  OB_SYNC_LOG_ARCHIVE_PROGRESS = 32,
  OB_RESTORE_LEADER_TAKEOVER_MSG = 33,
  OB_RESTORE_LOG_FINISH_MSG = 34,
  OB_RESTORE_ALIVE_MSG = 35,
  OB_RESTORE_ALIVE_REQ = 36,
  OB_RESTORE_ALIVE_RESP = 37,
  // used for standby cluster
  OB_STANDBY_PREPARE_REQ = 38,
  OB_STANDBY_PREPARE_RESP = 39,
  OB_RENEW_MS_LOG_ACK = 40,
  OB_RENEW_MS_CONFIRMED_INFO_REQ = 41,
  OB_PUSH_RENEW_MS_LOG = 42,
  OB_STANDBY_ACK_LOG = 43,
  OB_STANDBY_QUERY_SYNC_START_ID = 44,
  OB_STANDBY_SYNC_START_ID_RESP = 45,
  // sentry req
  OB_LOG_MAX_REQ_TYPE_ID = 46,
};

inline bool is_batch_submit_msg(const ObLogReqType type)
{
  return OB_BATCH_PUSH_LOG == type || OB_BATCH_ACK_LOG == type;
}

struct ObLogReqContext {
  ObLogReqContext()
      : type_(OB_INVALID_REQ),
        server_(),
        pkey_(),
        req_id_(0),
        data_(NULL),
        len_(0),
        cluster_id_(common::OB_INVALID_CLUSTER_ID)
  {}
  ~ObLogReqContext()
  {}
  bool is_valid() const
  {
    return server_.is_valid() && pkey_.is_valid() && NULL != data_ && len_ >= 0;
  }
  TO_STRING_KV(
      N_TYPE, type_, N_SERVER, server_, N_PKEY, pkey_, N_REQ_ID, req_id_, N_SIZE, len_, N_CLUSTER_ID, cluster_id_);
  ObLogReqType type_;
  common::ObAddr server_;
  common::ObPartitionKey pkey_;
  int64_t req_id_;
  const char* data_;
  int64_t len_;
  int64_t cluster_id_;
};

class ObINetReq : public obrpc::ObIFill {
public:
  ObINetReq()
  {}
  virtual ~ObINetReq()
  {}
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0;
  virtual int64_t get_serialize_size(void) const = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
};

enum ObPushLogMode {
  PUSH_LOG_ASYNC = 0,
  PUSH_LOG_SYNC = 1,
};

struct ObPushLogReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObPushLogReq(common::ObProposalID proposal_id, const char* buf, int64_t len, int32_t push_mode)
      : proposal_id_(proposal_id), buf_(buf), len_(len), push_mode_(push_mode)
  {}
  ObPushLogReq() : proposal_id_(), buf_(NULL), len_(0), push_mode_(PUSH_LOG_ASYNC)
  {}
  ~ObPushLogReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObProposalID proposal_id_;
  const char* buf_;
  int64_t len_;
  int32_t push_mode_;
};

struct ObPushMsLogReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObPushMsLogReq(common::ObProposalID proposal_id, const char* buf, int64_t len)
      : proposal_id_(proposal_id), buf_(buf), len_(len)
  {}
  ObPushMsLogReq() : buf_(NULL), len_(0)
  {}
  ~ObPushMsLogReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObProposalID proposal_id_;
  const char* buf_;
  int64_t len_;
};

struct ObFetchLogReqV2 : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObFetchLogReqV2()
      : start_id_(0),
        end_limit_(0),
        fetch_type_(OB_FETCH_LOG_UNKNOWN),
        proposal_id_(),
        replica_type_(common::REPLICA_TYPE_MAX),
        network_limit_(0),
        cluster_id_(common::OB_INVALID_CLUSTER_ID),
        max_confirmed_log_id_(common::OB_INVALID_ID)
  {}
  ObFetchLogReqV2(int64_t start_id, int64_t end_limit, ObFetchLogType fetch_type, common::ObProposalID proposal_id,
      common::ObReplicaType replica_type, int64_t network_limit, uint64_t max_confirmed_log_id)
      : start_id_(start_id),
        end_limit_(end_limit),
        fetch_type_(fetch_type),
        proposal_id_(proposal_id),
        replica_type_(replica_type),
        network_limit_(network_limit),
        cluster_id_(common::OB_INVALID_CLUSTER_ID),
        max_confirmed_log_id_(max_confirmed_log_id)
  {}
  ~ObFetchLogReqV2()
  {}

  void reset();
  DECLARE_TO_STRING;
  int64_t start_id_;
  int64_t end_limit_;
  ObFetchLogType fetch_type_;
  common::ObProposalID proposal_id_;
  common::ObReplicaType replica_type_;
  int64_t network_limit_;
  int64_t cluster_id_;
  uint64_t max_confirmed_log_id_;
};

struct ObAckLogReqV2 : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObAckLogReqV2(uint64_t log_id, common::ObProposalID proposal_id) : log_id_(log_id), proposal_id_(proposal_id)
  {}
  ObAckLogReqV2() : log_id_(0), proposal_id_()
  {}
  ~ObAckLogReqV2()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t log_id_;
  common::ObProposalID proposal_id_;
};

struct ObStandbyAckLogReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObStandbyAckLogReq() : log_id_(common::OB_INVALID_ID), proposal_id_()
  {}
  ObStandbyAckLogReq(uint64_t log_id, common::ObProposalID proposal_id) : log_id_(log_id), proposal_id_(proposal_id)
  {}
  ~ObStandbyAckLogReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t log_id_;
  common::ObProposalID proposal_id_;
};

struct ObStandbyQuerySyncStartIdReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObStandbyQuerySyncStartIdReq() : send_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  explicit ObStandbyQuerySyncStartIdReq(const int64_t timestamp) : send_ts_(timestamp)
  {}
  ~ObStandbyQuerySyncStartIdReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t send_ts_;
};

struct ObStandbyQuerySyncStartIdResp : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObStandbyQuerySyncStartIdResp()
      : original_send_ts_(common::OB_INVALID_TIMESTAMP), sync_start_id_(common::OB_INVALID_ID)
  {}
  explicit ObStandbyQuerySyncStartIdResp(const int64_t send_ts, const uint64_t sync_start_id)
      : original_send_ts_(send_ts), sync_start_id_(sync_start_id)
  {}
  ~ObStandbyQuerySyncStartIdResp()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t original_send_ts_;
  uint64_t sync_start_id_;
};

struct ObRenewMsLogAckReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRenewMsLogAckReq(uint64_t log_id, int64_t submit_timestamp, common::ObProposalID proposal_id)
      : log_id_(log_id), submit_timestamp_(submit_timestamp), proposal_id_(proposal_id)
  {}
  ObRenewMsLogAckReq() : log_id_(0), submit_timestamp_(common::OB_INVALID_TIMESTAMP), proposal_id_()
  {}
  ~ObRenewMsLogAckReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  uint64_t log_id_;
  int64_t submit_timestamp_;
  common::ObProposalID proposal_id_;
};

struct ObFakeAckReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObFakeAckReq(uint64_t log_id, common::ObProposalID proposal_id) : log_id_(log_id), proposal_id_(proposal_id)
  {}
  ObFakeAckReq() : log_id_(0), proposal_id_()
  {}
  ~ObFakeAckReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t log_id_;
  common::ObProposalID proposal_id_;
};

struct ObFakePushLogReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObFakePushLogReq(uint64_t log_id, common::ObProposalID proposal_id) : log_id_(log_id), proposal_id_(proposal_id)
  {}
  ObFakePushLogReq() : log_id_(0), proposal_id_()
  {}
  ~ObFakePushLogReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t log_id_;
  common::ObProposalID proposal_id_;
};

struct ObPrepareReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  explicit ObPrepareReq(common::ObProposalID proposal_id) : proposal_id_(proposal_id)
  {}
  ObPrepareReq() : proposal_id_()
  {}
  ~ObPrepareReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObProposalID proposal_id_;
};

struct ObPrepareRespV2 : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObPrepareRespV2(common::ObProposalID proposal_id, int64_t max_log_id, int64_t max_log_ts)
      : proposal_id_(proposal_id), max_log_id_(max_log_id), max_log_ts_(max_log_ts)
  {}
  ObPrepareRespV2() : max_log_id_(0), max_log_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObPrepareRespV2()
  {}
  common::ObProposalID proposal_id_;
  int64_t max_log_id_;
  int64_t max_log_ts_;
  DECLARE_TO_STRING;
};

struct ObStandbyPrepareReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  explicit ObStandbyPrepareReq(common::ObProposalID proposal_id) : ms_proposal_id_(proposal_id)
  {}
  ObStandbyPrepareReq() : ms_proposal_id_()
  {}
  ~ObStandbyPrepareReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObProposalID ms_proposal_id_;
};

struct ObStandbyPrepareResp : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  explicit ObStandbyPrepareResp(common::ObProposalID proposal_id, uint64_t ms_log_id, int64_t membership_version)
      : ms_proposal_id_(proposal_id), ms_log_id_(ms_log_id), membership_version_(membership_version)
  {}
  ObStandbyPrepareResp()
      : ms_proposal_id_(),
        ms_log_id_(common::OB_INVALID_ID),
        membership_version_(common::OB_INVALID_TIMESTAMP),
        member_list_()
  {}
  ~ObStandbyPrepareResp()
  {}
  int set_member_list(const common::ObMemberList& member_list);
  DECLARE_TO_STRING;
  common::ObProposalID ms_proposal_id_;
  uint64_t ms_log_id_;
  int64_t membership_version_;
  common::ObMemberList member_list_;
};

struct ObConfirmedInfoReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObConfirmedInfoReq(const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
  {
    log_id_ = log_id;
    confirmed_info_.deep_copy(confirmed_info);
    batch_committed_ = batch_committed;
  }
  ObConfirmedInfoReq() : log_id_(0), confirmed_info_(), batch_committed_(false)
  {}
  ~ObConfirmedInfoReq()
  {}
  uint64_t log_id_;
  ObConfirmedInfo confirmed_info_;
  bool batch_committed_;
  TO_STRING_KV(K_(log_id), K_(confirmed_info), K_(batch_committed));
};

struct ObRenewMsLogConfirmedInfoReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRenewMsLogConfirmedInfoReq(
      const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
  {
    log_id_ = log_id;
    ms_proposal_id_ = ms_proposal_id;
    confirmed_info_.deep_copy(confirmed_info);
  }
  ObRenewMsLogConfirmedInfoReq() : log_id_(0), ms_proposal_id_(), confirmed_info_()
  {}
  ~ObRenewMsLogConfirmedInfoReq()
  {}
  uint64_t log_id_;
  common::ObProposalID ms_proposal_id_;
  ObConfirmedInfo confirmed_info_;
  TO_STRING_KV(K_(log_id), K_(ms_proposal_id), K_(confirmed_info));
};

struct ObLogKeepaliveMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObLogKeepaliveMsg(const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt)
      : next_log_id_(next_log_id), next_log_ts_lb_(next_log_ts_lb), deliver_cnt_(deliver_cnt)
  {}
  ObLogKeepaliveMsg() : next_log_id_(0), next_log_ts_lb_(0), deliver_cnt_(0)
  {}
  ~ObLogKeepaliveMsg()
  {}
  uint64_t next_log_id_;
  int64_t next_log_ts_lb_;
  uint64_t deliver_cnt_;
  TO_STRING_KV(K_(next_log_id), K_(next_log_ts_lb), K_(deliver_cnt));
};

struct ObRestoreAliveMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRestoreAliveMsg(const uint64_t start_log_id) : start_log_id_(start_log_id)
  {}
  ObRestoreAliveMsg() : start_log_id_(0)
  {}
  ~ObRestoreAliveMsg()
  {}
  void reset();
  TO_STRING_KV(K_(start_log_id));
  uint64_t start_log_id_;
};

struct ObRestoreAliveReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRestoreAliveReq(const int64_t send_ts) : send_ts_(send_ts)
  {}
  ObRestoreAliveReq() : send_ts_(0)
  {}
  ~ObRestoreAliveReq()
  {}
  void reset();
  TO_STRING_KV(K_(send_ts));
  int64_t send_ts_;
};

struct ObRestoreAliveResp : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRestoreAliveResp(const int64_t send_ts) : send_ts_(send_ts)
  {}
  ObRestoreAliveResp() : send_ts_(0)
  {}
  ~ObRestoreAliveResp()
  {}
  void reset();
  TO_STRING_KV(K_(send_ts));
  int64_t send_ts_;
};

struct ObRestoreTakeoverMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRestoreTakeoverMsg() : send_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ObRestoreTakeoverMsg(const int64_t send_ts) : send_ts_(send_ts)
  {}
  ~ObRestoreTakeoverMsg()
  {}
  TO_STRING_KV(K_(send_ts));
  int64_t send_ts_;
};

enum ObLogMissingMsgType {
  OB_INVALID_MSG_TYPE = 0,
  OB_REBUILD_MSG = 1,
  OB_STANDBY_RESTORE_MSG = 2,
};

struct ObNotifyLogMissingReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObNotifyLogMissingReq(const uint64_t start_log_id, const bool is_in_member_list, const int32_t msg_type)
      : start_log_id_(start_log_id), is_in_member_list_(is_in_member_list), msg_type_(msg_type)
  {}
  ObNotifyLogMissingReq() : start_log_id_(0), is_in_member_list_(false), msg_type_(OB_REBUILD_MSG)
  {}
  ~ObNotifyLogMissingReq()
  {}
  uint64_t start_log_id_;
  bool is_in_member_list_;
  int32_t msg_type_;
  TO_STRING_KV(K_(start_log_id), K_(is_in_member_list), K_(msg_type));
};

struct ObFetchRegisterServerRespV2 : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObFetchRegisterServerRespV2() : is_assign_parent_succeed_(false), candidate_list_(), msg_type_(0)
  {}
  explicit ObFetchRegisterServerRespV2(bool is_assign_parent_succeed, const int32_t msg_type)
      : is_assign_parent_succeed_(is_assign_parent_succeed), msg_type_(msg_type)
  {}
  ~ObFetchRegisterServerRespV2()
  {}
  int set_candidate_list(const share::ObCascadMemberList& candidate_list);
  void reset();
  DECLARE_TO_STRING;
  bool is_assign_parent_succeed_;
  share::ObCascadMemberList candidate_list_;
  int32_t msg_type_;
};

struct ObFetchRegisterServerReqV2 : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObFetchRegisterServerReqV2()
      : replica_type_(common::REPLICA_TYPE_MAX),
        next_replay_log_ts_(common::OB_INVALID_TIMESTAMP),
        is_request_leader_(false),
        is_need_force_register_(false),
        region_(common::DEFAULT_REGION_NAME),
        cluster_id_(common::OB_INVALID_CLUSTER_ID),
        idc_()
  {}
  explicit ObFetchRegisterServerReqV2(common::ObReplicaType replica_type, int64_t next_replay_log_ts,
      bool is_request_leader, bool is_need_force_register, common::ObRegion region, common::ObIDC idc)
      : replica_type_(replica_type),
        next_replay_log_ts_(next_replay_log_ts),
        is_request_leader_(is_request_leader),
        is_need_force_register_(is_need_force_register),
        region_(region),
        cluster_id_(common::OB_INVALID_CLUSTER_ID),
        idc_(idc)
  {}
  explicit ObFetchRegisterServerReqV2(common::ObReplicaType replica_type, int64_t next_replay_log_ts,
      bool is_request_leader, bool is_need_force_register, common::ObRegion region, int64_t cluster_id,
      common::ObIDC idc)
      : replica_type_(replica_type),
        next_replay_log_ts_(next_replay_log_ts),
        is_request_leader_(is_request_leader),
        is_need_force_register_(is_need_force_register),
        region_(region),
        cluster_id_(cluster_id),
        idc_(idc)
  {}
  ~ObFetchRegisterServerReqV2()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObReplicaType replica_type_;
  int64_t next_replay_log_ts_;
  bool is_request_leader_;
  bool is_need_force_register_;
  common::ObRegion region_;
  int64_t cluster_id_;
  common::ObIDC idc_;
};

struct ObRejectMsgReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRejectMsgReq() : msg_type_(0), timestamp_(common::OB_INVALID_TIMESTAMP)
  {}
  explicit ObRejectMsgReq(const int32_t msg_type, const int64_t timestamp) : msg_type_(msg_type), timestamp_(timestamp)
  {}
  ~ObRejectMsgReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  int32_t msg_type_;
  int64_t timestamp_;
};

struct ObRestoreLogFinishMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObRestoreLogFinishMsg() : log_id_(common::OB_INVALID_ID)
  {}
  explicit ObRestoreLogFinishMsg(const uint64_t log_id) : log_id_(log_id)
  {}
  ~ObRestoreLogFinishMsg()
  {}
  void reset();
  DECLARE_TO_STRING;
  uint64_t log_id_;
};

struct ObReregisterMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObReregisterMsg() : send_ts_(common::OB_INVALID_TIMESTAMP), new_leader_()
  {}
  explicit ObReregisterMsg(const int64_t timestamp, const share::ObCascadMember& new_leader)
      : send_ts_(timestamp), new_leader_(new_leader)
  {}
  ~ObReregisterMsg()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t send_ts_;
  share::ObCascadMember new_leader_;
};

struct ObBroadcastInfoReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObBroadcastInfoReq(const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
      : replica_type_(replica_type), max_confirmed_log_id_(max_confirmed_log_id)
  {}
  ObBroadcastInfoReq() : replica_type_(common::REPLICA_TYPE_MAX), max_confirmed_log_id_(0)
  {}
  ~ObBroadcastInfoReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObReplicaType replica_type_;
  uint64_t max_confirmed_log_id_;
};

class ObBatchPushLogReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObBatchPushLogReq()
  {
    reset();
  }
  virtual ~ObBatchPushLogReq()
  {
    reset();
  }
  int init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array);
  void reset();
  TO_STRING_KV(K(trans_id_), K(partition_array_), K(log_info_array_));

public:
  transaction::ObTransID trans_id_;
  common::ObPartitionArray partition_array_;
  ObLogInfoArray log_info_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchPushLogReq);
};

class ObBatchAckLogReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObBatchAckLogReq()
  {
    reset();
  }
  virtual ~ObBatchAckLogReq()
  {
    reset();
  }
  int init(const transaction::ObTransID& trans_id, const ObBatchAckArray& batch_ack_array);
  void reset();
  TO_STRING_KV(K(trans_id_), K(batch_ack_array_));

public:
  transaction::ObTransID trans_id_;
  ObBatchAckArray batch_ack_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchAckLogReq);
};

struct ObReplaceSickChildReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObReplaceSickChildReq() : sick_child_(), cluster_id_(common::OB_INVALID_CLUSTER_ID)
  {}
  explicit ObReplaceSickChildReq(const common::ObAddr& sick_child)
      : sick_child_(sick_child), cluster_id_(common::OB_INVALID_CLUSTER_ID)
  {}
  explicit ObReplaceSickChildReq(const common::ObAddr& sick_child, const int64_t cluster_id)
      : sick_child_(sick_child), cluster_id_(cluster_id)
  {}
  ~ObReplaceSickChildReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  common::ObAddr sick_child_;
  int64_t cluster_id_;
};

struct ObLeaderMaxLogMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObLeaderMaxLogMsg() : switchover_epoch_(-1), leader_max_log_id_(0), leader_next_log_ts_(-1)
  {}
  ObLeaderMaxLogMsg(const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t leader_next_log_ts)
      : switchover_epoch_(switchover_epoch),
        leader_max_log_id_(leader_max_log_id),
        leader_next_log_ts_(leader_next_log_ts)
  {}
  ~ObLeaderMaxLogMsg()
  {}
  int64_t switchover_epoch_;
  uint64_t leader_max_log_id_;
  int64_t leader_next_log_ts_;
  TO_STRING_KV(K_(switchover_epoch), K_(leader_max_log_id), K_(leader_next_log_ts));
};

struct ObCheckRebuildReq : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObCheckRebuildReq() : start_id_(0)
  {}

  ObCheckRebuildReq(int64_t start_id) : start_id_(start_id)
  {}
  ~ObCheckRebuildReq()
  {}
  void reset();
  DECLARE_TO_STRING;
  int64_t start_id_;
};

struct ObSyncLogArchiveProgressMsg : public ObINetReq {
  OB_UNIS_VERSION(1);

public:
  ObSyncLogArchiveProgressMsg(const ObPGLogArchiveStatus& status) : status_(status)
  {}
  ObSyncLogArchiveProgressMsg() : status_()
  {}
  ~ObSyncLogArchiveProgressMsg()
  {}
  ObPGLogArchiveStatus status_;
  TO_STRING_KV(K_(status));
};

};  // end namespace clog
};  // end namespace oceanbase

#endif /* OCEANBASE_CLOG_OB_LOG_REQ_H_ */
