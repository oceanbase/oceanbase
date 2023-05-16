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

#ifndef OCEANBASE_DUP_TABLE_RPC_H_
#define OCEANBASE_DUP_TABLE_RPC_H_

#include "common/ob_queue_thread.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"


namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace transaction
{
class ObDupTablePartitionMgr;
class ObTransService;

class ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObDupTableMsgHeader() { reset(); }
  virtual ~ObDupTableMsgHeader() {}
  void reset();
  int set_header(const ObAddr &src, const ObAddr &dst, const ObAddr &proxy);
  const ObAddr &get_src() const { return src_; }
  const ObAddr &get_dst() const { return dst_; }
  const ObAddr &get_proxy() const { return proxy_; }
  TO_STRING_KV(K_(src), K_(dst), K_(proxy));
protected:
  ObAddr src_;
  ObAddr dst_;
  ObAddr proxy_;
};

class ObDupTableLeaseRequestMsg : public ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObDupTableLeaseRequestMsg() { reset(); }
  ~ObDupTableLeaseRequestMsg() {}
  int init(const int64_t request_ts,
           const common::ObAddr &addr,
           const uint64_t last_log_id,
           const int64_t request_lease_interval_us,
           const int64_t gts);
  int64_t get_request_ts() const { return request_ts_; }
  // const common::ObPartitionKey &get_partition() const { return partition_; }
  const common::ObAddr &get_addr() const { return addr_; }
  uint64_t get_last_log_id() const { return last_log_id_; }
  int64_t get_request_lease_interval_us() const { return request_lease_interval_us_; }
  int64_t get_gts() const { return gts_; }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(request_ts), K_(addr), K_(last_log_id), K_(request_lease_interval_us), K_(gts));
private:
  // the send timestamp of a copy replica applying for lease
  int64_t request_ts_;
  // common::ObPartitionKey partition_;
  common::ObAddr addr_;
  uint64_t last_log_id_;
  // the interval in us of the requested lease
  int64_t request_lease_interval_us_;
  // the gts cache value on the dup replica
  int64_t gts_;
};

class ObDupTableLeaseResponseMsg : public ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObDupTableLeaseResponseMsg() { reset(); }
  ~ObDupTableLeaseResponseMsg() {}
  enum class ObDupTableLeaseStatus : int
  {
    OB_DUP_TABLE_LEASE_UNKNOWN,
    OB_DUP_TABLE_LEASE_SUCC,
    OB_DUP_TABLE_LEASE_EXPIRED,
    OB_DUP_TABLE_LEASE_LOG_TOO_OLD,
    OB_DUP_TABLE_LEASE_NOT_MASTER
  };
  int init(const int64_t request_ts,
           const uint64_t cur_log_id,
           const ObDupTableLeaseStatus status,
           const int64_t lease_interval_us,
           const int64_t gts);
  int64_t get_request_ts() const { return request_ts_; }
  uint64_t get_cur_log_id() const { return cur_log_id_; }
  ObDupTableLeaseStatus get_status() const { return status_; }
  int64_t get_lease_interval_us() const { return lease_interval_us_; }
  int64_t get_gts() const { return gts_; }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(request_ts), K_(cur_log_id), K_(status), K_(lease_interval_us), K_(gts));
private:
  // the send timestamp of a copy replica applying for lease
  int64_t request_ts_;
  uint64_t cur_log_id_;
  enum ObDupTableLeaseStatus status_;
  // the interval in us of the lease
  int64_t lease_interval_us_;
  // gts cache value on the leader
  int64_t gts_;
};

class ObRedoLogSyncRequestMsg : public ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObRedoLogSyncRequestMsg() { reset(); }
  ~ObRedoLogSyncRequestMsg() {}
  int init(const uint64_t log_id,
           const int64_t log_ts,
           const int64_t log_type,
           const ObTransID &trans_id);
  uint64_t get_log_id() const { return log_id_; }
  int64_t get_log_ts() const { return log_ts_; }
  const ObTransID &get_trans_id() const { return trans_id_; }
  int64_t get_log_type() const { return log_type_; }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(log_id),
               K_(log_ts), K_(trans_id), K_(log_type));
private:
  uint64_t log_id_;
  int64_t log_ts_;
  int64_t log_type_;
  ObTransID trans_id_;
};

class ObRedoLogSyncResponseMsg : public ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObRedoLogSyncResponseMsg() { reset(); }
  ~ObRedoLogSyncResponseMsg() {}
  enum class ObRedoLogSyncResponseStatus : int
  {
    OB_REDO_LOG_SYNC_UNKNOWN,
    OB_REDO_LOG_SYNC_SUCC,
    OB_REDO_LOG_SYNC_LEASE_EXPIRED,
    OB_REDO_LOG_SYNC_NOT_SYNC
  };
  int init(
           const uint64_t log_id,
           const ObTransID &trans_id,
           const common::ObAddr &addr,
           const ObRedoLogSyncResponseStatus status);
  uint64_t get_log_id() const { return log_id_; }
  const ObTransID &get_trans_id() const { return trans_id_; }
  const common::ObAddr &get_addr() const { return addr_; }
  ObRedoLogSyncResponseStatus get_status() const { return status_; }
  int64_t get_gts() const { return gts_; }
  void set_gts(const int64_t gts) { gts_ = gts; }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(log_id), K_(trans_id), K_(addr), K_(status), K_(gts));
private:
  uint64_t log_id_;
  ObTransID trans_id_;
  common::ObAddr addr_;
  enum ObRedoLogSyncResponseStatus status_;
  int64_t gts_;
};

class ObPreCommitRequestMsg : public ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObPreCommitRequestMsg() : tenant_id_(OB_INVALID_TENANT_ID), commit_version_(-1) {}
  ~ObPreCommitRequestMsg() {}
  int init(const uint64_t tenant_id,
           const ObTransID &trans_id,
           const int64_t commit_version)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) ||
                    !trans_id.is_valid() ||
                    commit_version <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id),
                K(trans_id), K(commit_version));
    } else {
      commit_version_ = commit_version;
      tenant_id_ = tenant_id;
      trans_id_ = trans_id;
    }
    return ret;
  }
  bool is_valid() const
  {
    return commit_version_ > 0 &&
           is_valid_tenant_id(tenant_id_) &&
           trans_id_.is_valid();
  }
  void reset()
  {
    commit_version_ = -1;
    tenant_id_ = OB_INVALID_TENANT_ID;
    trans_id_.reset();
  }
  int64_t get_commit_version() const { return commit_version_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObTransID& get_trans_id() const { return trans_id_; }
  TO_STRING_KV(K_(tenant_id), K_(trans_id), K_(commit_version));
private:
  uint64_t tenant_id_;
  ObTransID trans_id_;
  int64_t commit_version_;
};

class ObPreCommitResponseMsg : public ObDupTableMsgHeader
{
  OB_UNIS_VERSION(1);
public:
  ObPreCommitResponseMsg() : result_(-1) {}
  ~ObPreCommitResponseMsg() {}
  int init(
           const ObTransID &trans_id,
           const common::ObAddr &addr,
           const int64_t result)
  {
    int ret = OB_SUCCESS;
    if (
        OB_UNLIKELY(!trans_id.is_valid()) ||
        OB_UNLIKELY(!addr.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(addr));
    } else {
      trans_id_ = trans_id;
      addr_ = addr;
      result_ = result;
    }
    return ret;
  }
  void reset()
  {
    trans_id_.reset();
    addr_.reset();
    result_ = -1;
  }
  bool is_valid() const { return trans_id_.is_valid() && addr_.is_valid(); }
  int64_t get_result() const { return result_; }
  const ObTransID& get_trans_id() const { return trans_id_; }
  const common::ObAddr& get_addr() const { return addr_; }
  TO_STRING_KV(K_(trans_id), K_(addr), K_(result));
private:
  ObTransID trans_id_;
  common::ObAddr addr_;
  int64_t result_;
};

}//transaction

namespace obrpc
{
class ObDupTableRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObDupTableRpcProxy);

  // RPC_AP(PRZ post_dup_table_lease_request, OB_DUP_TABLE_LEASE_REQUEST,
  //        (transaction::ObDupTableLeaseRequestMsg));
  // RPC_AP(PRZ post_dup_table_lease_response, OB_DUP_TABLE_LEASE_RESPONSE,
  //        (transaction::ObDupTableLeaseResponseMsg));
  // RPC_AP(PR3 post_redo_log_sync_request, OB_DUP_TABLE_LEASE_RESPONSE,
  //        (transaction::ObRedoLogSyncRequestMsg));
  // RPC_AP(PR3 post_redo_log_sync_response, OB_DUP_TABLE_LEASE_RESPONSE,
  //        (transaction::ObRedoLogSyncResponseMsg));
  // RPC_AP(PR3 post_pre_commit_request, OB_DUP_TABLE_PRE_COMMIT_REQ,
  //        (transaction::ObPreCommitRequestMsg));
  // RPC_AP(PR3 post_pre_commit_response, OB_DUP_TABLE_PRE_COMMIT_RESP,
  //        (transaction::ObPreCommitResponseMsg));
};

// class ObDupTableLeaseRequestMsgP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_DUP_TABLE_LEASE_REQUEST>>
// {
// public:
//   explicit ObDupTableLeaseRequestMsgP(const observer::ObGlobalContext &global_ctx) : global_ctx_(global_ctx) {}
// protected:
//   int process();
//
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseRequestMsgP);
// private:
//   const observer::ObGlobalContext &global_ctx_;
// };
//
// class ObDupTableLeaseResponseMsgP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_DUP_TABLE_LEASE_RESPONSE>>
// {
// public:
//   explicit ObDupTableLeaseResponseMsgP(const observer::ObGlobalContext &global_ctx) : global_ctx_(global_ctx) {}
// protected:
//   int process();
//
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseResponseMsgP);
// private:
//   const observer::ObGlobalContext &global_ctx_;
// };

// class ObRedoLogSyncRequestP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_REDO_LOG_SYNC_REQUEST>>
// {
// public:
//   explicit ObRedoLogSyncRequestP(const observer::ObGlobalContext &global_ctx) : global_ctx_(global_ctx) {}
// protected:
//   int process();
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObRedoLogSyncRequestP);
// private:
//   const observer::ObGlobalContext &global_ctx_;
// };
//
// class ObRedoLogSyncResponseP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_REDO_LOG_SYNC_RESPONSE>>
// {
// public:
//   explicit ObRedoLogSyncResponseP(const observer::ObGlobalContext &global_ctx) : global_ctx_(global_ctx) {}
// protected:
//   int process();
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObRedoLogSyncResponseP);
// private:
//   const observer::ObGlobalContext &global_ctx_;
// };
//
// class ObPreCommitRequestP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_DUP_TABLE_PRE_COMMIT_REQ>>
// {
// public:
//   explicit ObPreCommitRequestP(const observer::ObGlobalContext &global_ctx) : global_ctx_(global_ctx) {}
// protected:
//   int process();
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObPreCommitRequestP);
// private:
//   const observer::ObGlobalContext &global_ctx_;
// };
//
// class ObPreCommitResponseP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_DUP_TABLE_PRE_COMMIT_RESP>>
// {
// public:
//   explicit ObPreCommitResponseP(const observer::ObGlobalContext &global_ctx) : global_ctx_(global_ctx) {}
// protected:
//   int process();
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObPreCommitResponseP);
// private:
//   const observer::ObGlobalContext &global_ctx_;
// };
//
}//obrpc

namespace transaction
{
class ObIDupTableRpc
{
public:
  ObIDupTableRpc() {}
  virtual ~ObIDupTableRpc() {}
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;
public:
  virtual int post_dup_table_lease_request(const uint64_t tenant_id,
                                           const common::ObAddr &server,
                                           const ObDupTableLeaseRequestMsg &msg) = 0;
  virtual int post_dup_table_lease_response(const uint64_t tenant_id,
                                            const common::ObAddr &server,
                                            const ObDupTableLeaseResponseMsg &msg) = 0;
  virtual int post_redo_log_sync_request(const uint64_t tenant_id,
                                         const common::ObAddr &server,
                                         const ObRedoLogSyncRequestMsg &msg) = 0;
  virtual int post_redo_log_sync_response(const uint64_t tenant_id,
                                          const common::ObAddr &server,
                                          const ObRedoLogSyncResponseMsg &msg) = 0;
  virtual int post_pre_commit_request(const uint64_t tenant_id,
                                      const common::ObAddr &server,
                                      const ObPreCommitRequestMsg &msg) = 0;
  virtual int post_pre_commit_response(const uint64_t tenant_id,
                                       const common::ObAddr &addr,
                                       const ObPreCommitResponseMsg &msg) = 0;
};

class ObDupTableRpc_old : public ObIDupTableRpc
{
public:
  ObDupTableRpc_old() : is_inited_(false), is_running_(false),
                    trans_service_(NULL), rpc_proxy_(NULL) {}
  ~ObDupTableRpc_old() { destroy(); }
  int init(ObTransService *trans_service, rpc::frame::ObReqTransport *transport, const ObAddr &addr);
  int start();
  int stop();
  int wait();
  void destroy();
  virtual int post_dup_table_lease_request(const uint64_t tenant_id,
                                           const common::ObAddr &server,
                                           const ObDupTableLeaseRequestMsg &msg);
  virtual int post_dup_table_lease_response(const uint64_t tenant_id,
                                            const common::ObAddr &server,
                                            const ObDupTableLeaseResponseMsg &msg);
  virtual int post_redo_log_sync_request(const uint64_t tenant_id,
                                         const common::ObAddr &server,
                                         const ObRedoLogSyncRequestMsg &msg);
  virtual int post_redo_log_sync_response(const uint64_t tenant_id,
                                          const common::ObAddr &server,
                                          const ObRedoLogSyncResponseMsg &msg);
  virtual int post_pre_commit_request(const uint64_t tenant_id,
                                      const common::ObAddr &server,
                                      const ObPreCommitRequestMsg &msg);
  virtual int post_pre_commit_response(const uint64_t tenant_id,
                                       const common::ObAddr &addr,
                                       const ObPreCommitResponseMsg &msg);
private:
  bool is_inited_;
  bool is_running_;
  ObTransService *trans_service_;
  obrpc::ObDupTableRpcProxy rpc_proxy_;
};

}//transaction

}//oceanbase

#endif
