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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_MSG2_
#define OCEANBASE_TRANSACTION_OB_TRANS_MSG2_

#include "share/ob_define.h"
#include "ob_trans_define.h"
#include "share/rpc/ob_batch_proxy.h"
#include "ob_trans_msg.h"
#include "ob_trans_split_adapter.h"
#include "ob_trans_msg_type2.h"

namespace oceanbase {
namespace transaction {

struct ObTrxMsgBase : public obrpc::ObIFill {
  OB_UNIS_VERSION(1);

public:
  ObTrxMsgBase()
      : tenant_id_(common::OB_INVALID_TENANT_ID), msg_type_(OB_TRX_MSG_UNKNOWN), trans_time_(0), timestamp_(0)
  {}
  ~ObTrxMsgBase()
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr);
  bool is_valid() const;
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(K_(tenant_id), K_(trans_id), K_(msg_type), K_(trans_time), K_(sender), K_(receiver), K_(trans_param),
      K_(sender_addr), K_(timestamp));

public:
  uint64_t tenant_id_;
  ObTransID trans_id_;
  int64_t msg_type_;
  int64_t trans_time_;
  common::ObPartitionKey sender_;
  common::ObPartitionKey receiver_;
  ObStartTransParam trans_param_;
  common::ObAddr sender_addr_;
  int64_t timestamp_;
  common::ObPartitionArray batch_same_leader_partitions_;
};

/*
struct ObTrxErrMsg :  public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const int64_t msg_type, const int64_t err_msg_type, const uint64_t tenant_id,
    const ObTransID &trans_id, const common::ObPartitionKey &sender, const common::ObAddr &sender_addr,
    const int32_t status, const int64_t sql_no, const int64_t request_id);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
int64_t err_msg_type_;
int status_;
int64_t sql_no_;
int64_t request_id_;
};

struct ObTrxStartStmtRequest : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
    const ObStartTransParam &trans_param, const common::ObAddr &sender_addr,
    const int64_t sql_no, const uint64_t cluster_id, const int64_t request_id,
    const int64_t stmt_expired_time, const bool need_create_ctx,
    const uint64_t cluster_version, const int32_t snapshot_gene_type, const uint32_t session_id,
    const uint64_t proxy_session_id, const common::ObString &app_trace_id_str,
    const ObTransLocationCache &trans_location_cache, const bool can_elr,
    const bool is_not_create_ctx_participant);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
common::ObAddr scheduler_;
int64_t sql_no_;
uint64_t cluster_id_;
int64_t request_id_;
int64_t stmt_expired_time_;
bool need_create_ctx_;
uint64_t cluster_version_;
int snapshot_gene_type_;
uint32_t session_id_;
uint64_t proxy_session_id_;
common::ObString app_trace_id_str_;
ObTransLocationCache trans_location_cache_;
bool can_elr_;
bool is_not_create_ctx_participant_;
};

struct ObTrxStartStmtResponse : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const int64_t sql_no, const int64_t snapshot_version,
    const int32_t status, const int64_t request_id);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
int64_t sql_no_;
int32_t status_;
int64_t request_id_;
};

struct ObTrxStmtRollbackResponse : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const int64_t sql_no, const int32_t status,
    const int64_t request_id);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
int64_t sql_no_;
int status_;
int64_t request_id_;
};

typedef ObTrxStmtRollbackResponse ObTrxSavepointResponse;

struct ObTrxStmtRollbackRequest : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const int64_t sql_no, const int64_t request_id,
    const ObTransLocationCache &trans_location_cache);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
int64_t sql_no_;
int64_t request_id_;
ObTransLocationCache trans_location_cache_;
};

typedef ObTrxStmtRollbackRequest ObTrxSavepointRequest;

struct ObTrxCommitRequest : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
    const common::ObPartitionKey &coordinator, const common::ObPartitionArray &participants,
    const ObStartTransParam &trans_param, const common::ObAddr &sender_addr,
    const ObTransLocationCache &trans_location_cache, const int64_t commit_times,
    const int64_t stc, const bool is_dup_table_trans, const ObStmtRollbackInfo &stmt_rollback_info);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
common::ObAddr scheduler_;
common::ObPartitionKey coordinator_;
common::ObPartitionArray participants_;
ObTransLocationCache trans_location_cache_;
int64_t commit_times_;
int64_t stc_;
bool is_dup_table_trans_;
ObStmtRollbackInfo stmt_rollback_info_;
};

typedef ObTrxCommitRequest ObTrxAbortRequest;

struct ObTrxCommitResponse : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const int32_t status,
    const int64_t commit_times, const int64_t need_wait_interval_us);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
int status_;
int64_t commit_times_;
bool need_wait_interval_us_;
};

typedef ObTrxCommitResponse ObTrxAbortResponse;

struct ObTrx2PCPrePrepareRequest : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
    const common::ObPartitionKey &coordinator, const common::ObPartitionArray &participants,
    const ObStartTransParam &trans_param, const common::ObAddr &sender_addr, const int32_t status,
    const int64_t request_id, const int64_t stc);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
common::ObAddr scheduler_;
common::ObPartitionKey coordinator_;
common::ObPartitionArray participants_;
int status_;
int64_t request_id_;
int64_t stc_;
};

struct ObTrx2PCPrePrepareResponse : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
    const common::ObPartitionKey &coordinator, const common::ObPartitionArray &participants,
    const ObStartTransParam &trans_param, const int64_t redo_prepare_logid,
    const int64_t redo_prepare_log_timestamp, const common::ObAddr &sender_addr,
    const int32_t status, const int64_t trans_version, const int64_t request_id);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
common::ObAddr scheduler_;
common::ObPartitionKey coordinator_;
common::ObPartitionArray participants_;
int64_t prepare_log_id_;
int64_t prepare_log_timestamp_;
int status_;
int64_t trans_version_;
int64_t request_id_;
};

struct ObTrx2PCLogIDRequest : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const common::ObAddr &sender_addr, const int64_t request_id);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
common::ObAddr scheduler_;
common::ObPartitionKey coordinator_;
int status_;
int64_t trans_version_;
int64_t request_id_;
};

struct ObTrx2PCLogIDResponse : public ObTrxMsgBase
{
OB_UNIS_VERSION(1);
public:
int init(const uint64_t tenant_id, const ObTransID &trans_id,
    const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
    const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
    const int64_t redo_prepare_logid, const int64_t redo_prepare_log_timestamp,
    const common::ObAddr &sender_addr, const int32_t status, const int64_t request_id,
    const PartitionLogInfoArray &arr);
virtual int fill_buffer(char* buf, int64_t size) const
{
  int64_t pos = 0;
  return serialize(buf, size, pos);
}
virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
int64_t prepare_log_id_;
int64_t prepare_log_timestamp_;
int status_;
int64_t request_id_;
PartitionLogInfoArray partition_log_info_arr_;
};
*/

struct ObTrx2PCPrepareRequest : public ObTrxMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTrx2PCPrepareRequest() : status_(common::OB_SUCCESS), request_id_(0), stc_(0), is_xa_prepare_(false)
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int32_t status,
      const int64_t request_id, const MonotonicTs stc, const PartitionLogInfoArray& arr,
      const ObStmtRollbackInfo& stmt_rollback_info, const common::ObString& app_trace_info, const ObXATransID& xid,
      const bool is_xa_prepare);
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  int set_split_info(const ObTransSplitInfo& split_info)
  {
    return split_info_.assign(split_info);
  }
  TO_STRING_KV(K_(scheduler), K_(coordinator), K_(participants), K_(request_id), K_(status), K_(stc),
      K_(partition_log_info_arr), K_(batch_same_leader_partitions), K_(app_trace_info), K_(xid), K_(is_xa_prepare));

public:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int status_;
  int64_t request_id_;
  MonotonicTs stc_;
  PartitionLogInfoArray partition_log_info_arr_;
  ObStmtRollbackInfo stmt_rollback_info_;
  common::ObString app_trace_info_;
  ObTransSplitInfo split_info_;
  ObXATransID xid_;
  bool is_xa_prepare_;
};

struct ObTrx2PCPrepareResponse : public ObTrxMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTrx2PCPrepareResponse()
      : prepare_log_id_(0),
        prepare_log_timestamp_(0),
        status_(common::OB_SUCCESS),
        state_(Ob2PCState::UNKNOWN),
        trans_version_(0),
        request_id_(0),
        need_wait_interval_us_(0),
        publish_version_(0),
        is_xa_prepare_(false)
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const int64_t prepare_logid, const int64_t prepare_log_timestamp,
      const common::ObAddr& sender_addr, const int32_t status, const int64_t state, const int64_t trans_version,
      const int64_t request_id, const PartitionLogInfoArray& arr, const int64_t need_wait_interval_us,
      const common::ObString& app_trace_info, const int64_t publish_version, const ObXATransID& xid_,
      const bool is_xa_prepare);
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  int set_split_info(const ObTransSplitInfo& split_info)
  {
    return split_info_.assign(split_info);
  }
  TO_STRING_KV(K_(scheduler), K_(coordinator), K_(participants), K_(request_id), K_(status), K_(state),
      K_(partition_log_info_arr), K_(trans_version), K_(prepare_log_id), K_(prepare_log_timestamp),
      K_(need_wait_interval_us), K_(app_trace_info), K_(publish_version), K_(xid), K_(is_xa_prepare));

public:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t prepare_log_id_;
  int64_t prepare_log_timestamp_;
  int status_;
  int64_t state_;
  int64_t trans_version_;
  int64_t request_id_;
  PartitionLogInfoArray partition_log_info_arr_;
  int64_t need_wait_interval_us_;
  common::ObString app_trace_info_;
  int64_t publish_version_;
  ObTransSplitInfo split_info_;
  ObXATransID xid_;
  bool is_xa_prepare_;
};

struct ObTrx2PCCommitRequest : public ObTrxMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTrx2PCCommitRequest() : trans_version_(0), request_id_(0), status_(common::OB_SUCCESS)
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const int64_t trans_version, const common::ObAddr& sender_addr,
      const int64_t request_id, const PartitionLogInfoArray& partition_log_info_arr, const int32_t status);
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(
      K_(scheduler), K_(coordinator), K_(participants), K_(request_id), K_(status), K_(batch_same_leader_partitions));

public:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t trans_version_;
  int64_t request_id_;
  PartitionLogInfoArray partition_log_info_arr_;
  int status_;
};

typedef ObTrx2PCCommitRequest ObTrx2PCPreCommitRequest;
typedef ObTrx2PCPreCommitRequest ObTrx2PCPreCommitResponse;

struct ObTrxListenerCommitRequest : public ObTrx2PCCommitRequest {
  OB_UNIS_VERSION(1);

public:
  INHERIT_TO_STRING_KV("ObTrx2PCCommitRequest", ObTrx2PCCommitRequest, K_(split_info));

public:
  ObTransSplitInfo split_info_;
};

typedef ObTrxListenerCommitRequest ObTrxListenerCommitResponse;

struct ObTrx2PCCommitResponse : public ObTrxMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTrx2PCCommitResponse() : trans_version_(0), request_id_(0), status_(common::OB_SUCCESS), commit_log_ts_(0)
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const int64_t trans_version, const common::ObAddr& sender_addr,
      const int64_t request_id, const PartitionLogInfoArray& partition_log_info_arr, const int32_t status,
      const int64_t commit_log_ts);
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(K_(scheduler), K_(coordinator), K_(participants), K_(request_id), K_(status),
      K_(batch_same_leader_partitions), K_(commit_log_ts));

public:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t trans_version_;
  int64_t request_id_;
  PartitionLogInfoArray partition_log_info_arr_;
  int status_;
  int64_t commit_log_ts_;
};

struct ObTrx2PCAbortRequest : public ObTrxMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTrx2PCAbortRequest() : request_id_(0), status_(common::OB_SUCCESS)
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t request_id,
      const int32_t status);
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(
      K_(scheduler), K_(coordinator), K_(participants), K_(request_id), K_(status), K_(batch_same_leader_partitions));

public:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t request_id_;
  int status_;
  // common::ObPartitionArray batch_same_leader_partitions_;
};

typedef ObTrx2PCAbortRequest ObTrx2PCAbortResponse;

struct ObTrx2PCClearRequest : public ObTrxMsgBase {
  OB_UNIS_VERSION(1);

public:
  ObTrx2PCClearRequest() : request_id_(0), status_(common::OB_SUCCESS), clear_log_base_ts_(0)
  {}
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t msg_type, const int64_t trans_time,
      const common::ObPartitionKey& sender, const common::ObPartitionKey& receiver, const common::ObAddr& scheduler,
      const common::ObPartitionKey& coordinator, const common::ObPartitionArray& participants,
      const ObStartTransParam& trans_param, const common::ObAddr& sender_addr, const int64_t request_id,
      const int32_t status, const int64_t clear_log_base_ts);
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(K_(scheduler), K_(coordinator), K_(participants), K_(request_id), K_(status),
      K_(batch_same_leader_partitions), K_(clear_log_base_ts));

public:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t request_id_;
  int status_;
  int64_t clear_log_base_ts_;
};

typedef ObTrx2PCClearRequest ObTrx2PCClearResponse;

struct ObTrxListenerAbortRequest : public ObTrx2PCAbortRequest {
  OB_UNIS_VERSION(1);

public:
  INHERIT_TO_STRING_KV("ObTrx2PCAbortRequest", ObTrx2PCAbortRequest, K_(split_info));

public:
  ObTransSplitInfo split_info_;
};

typedef ObTrxListenerAbortRequest ObTrxListenerAbortResponse;

typedef ObTrx2PCClearRequest ObTrxListenerClearRequest;
typedef ObTrxListenerClearRequest ObTrxListenerClearResponse;

/*
struct ObTrxClearRequest : public ObTrxMsgBase
{
  OB_UNIS_VERSION(1);
public:
  int init(const uint64_t tenant_id, const ObTransID &trans_id,
      const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
      const common::ObPartitionKey &receiver, const common::ObAddr &scheduler,
      const common::ObPartitionArray &participants, const ObStartTransParam &trans_param,
      const common::ObAddr &sender_addr, const int64_t request_id, const int32_t status);
  virtual int fill_buffer(char* buf, int64_t size) const
  {
    int64_t pos = 0;
    return serialize(buf, size, pos);
  }
  virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
  common::ObAddr scheduler_;
  common::ObPartitionArray participants_;
  int64_t request_id_;
  int status_;
};

typedef ObTrxClearRequest ObTrxClearResponse;
typedef ObTrxClearRequest ObTrxDiscardRequest;

struct ObTrxAskSchedulerRequest : public ObTrxMsgBase
{
  OB_UNIS_VERSION(1);
public:
  int init(const uint64_t tenant_id, const ObTransID &trans_id,
      const int64_t msg_type, const int64_t trans_time, const common::ObPartitionKey &sender,
      const common::ObPartitionKey &receiver, const ObStartTransParam &trans_param,
      const common::ObAddr &sender_addr, const common::ObAddr &scheduler,
      const int32_t status);
  virtual int fill_buffer(char* buf, int64_t size) const
  {
    int64_t pos = 0;
    return serialize(buf, size, pos);
  }
  virtual int64_t get_req_size() const { return get_serialize_size(); }
public:
  common::ObAddr scheduler_;
  int status_;
};

typedef ObTrxAskSchedulerRequest ObTrxAskSchedulerResponse;
*/

union ObTransMsgUnionData {
  ObTransMsg trans_msg;
  ObTrxMsgBase trx_msg_base;
  ObTrx2PCClearRequest trx_2pc_clear_req;
  ObTrx2PCCommitRequest trx_2pc_commit_req;
  ObTrx2PCAbortRequest trx_2pc_abort_req;
  ObTrx2PCPrepareRequest trx_2pc_prepare_req;
  ObTrx2PCPreCommitRequest trx_2pc_pre_commit_req;
  ObTrx2PCClearResponse trx_2pc_clear_res;
  ObTrx2PCCommitResponse trx_2pc_commit_res;
  ObTrx2PCAbortResponse trx_2pc_abort_res;
  ObTrx2PCPrepareResponse trx_2pc_prepare_res;
  ObTrx2PCPreCommitResponse trx_2pc_pre_commit_res;
  ObTransMsgUnionData()
  {}
  ~ObTransMsgUnionData()
  {}
};

class ObTransMsgUnion {
public:
  ObTransMsgUnion(const int64_t msg_type);
  ~ObTransMsgUnion();
  void set_receiver(const common::ObPartitionKey& receiver);
  int set_batch_same_leader_partitions(const common::ObPartitionArray& partitions, const int64_t base_ts);
  void reset_batch_same_leader_partitions();
  ObTransMsgUnion& operator=(const ObTrxMsgBase& msg);
  ObTransMsgUnionData data_;
  int64_t msg_type_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_MSG2_
