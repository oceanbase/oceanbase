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
#include "storage/transaction/ob_trans_define.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace observer {
class ObGlobalContext;
}
namespace transaction {
class ObDupTablePartitionMgr;
class ObTransService;

class ObDupTableMsgHeader {
  OB_UNIS_VERSION(1);

public:
  ObDupTableMsgHeader()
  {
    reset();
  }
  virtual ~ObDupTableMsgHeader()
  {}
  void reset();
  int set_header(const ObAddr& src, const ObAddr& dst, const ObAddr& proxy);
  const ObAddr& get_src() const
  {
    return src_;
  }
  const ObAddr& get_dst() const
  {
    return dst_;
  }
  const ObAddr& get_proxy() const
  {
    return proxy_;
  }
  TO_STRING_KV(K_(src), K_(dst), K_(proxy));

protected:
  ObAddr src_;
  ObAddr dst_;
  ObAddr proxy_;
};

class ObDupTableLeaseRequestMsg : public ObDupTableMsgHeader {
  OB_UNIS_VERSION(1);

public:
  ObDupTableLeaseRequestMsg()
  {
    reset();
  }
  ~ObDupTableLeaseRequestMsg()
  {}
  int init(const int64_t request_ts, const common::ObPartitionKey& partition, const common::ObAddr& addr,
      const uint64_t last_log_id, const int64_t request_lease_interval_us);
  int64_t get_request_ts() const
  {
    return request_ts_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  uint64_t get_last_log_id() const
  {
    return last_log_id_;
  }
  int64_t get_request_lease_interval_us() const
  {
    return request_lease_interval_us_;
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(request_ts), K_(partition), K_(addr), K_(last_log_id), K_(request_lease_interval_us));

private:
  // the send timestamp of a copy replica applying for lease
  int64_t request_ts_;
  common::ObPartitionKey partition_;
  common::ObAddr addr_;
  uint64_t last_log_id_;
  // the interval in us of the requested lease
  int64_t request_lease_interval_us_;
};

class ObDupTableLeaseResponseMsg : public ObDupTableMsgHeader {
  OB_UNIS_VERSION(1);

public:
  ObDupTableLeaseResponseMsg()
  {
    reset();
  }
  ~ObDupTableLeaseResponseMsg()
  {}
  enum class ObDupTableLeaseStatus : int {
    OB_DUP_TABLE_LEASE_UNKNOWN,
    OB_DUP_TABLE_LEASE_SUCC,
    OB_DUP_TABLE_LEASE_EXPIRED,
    OB_DUP_TABLE_LEASE_LOG_TOO_OLD
  };
  int init(const int64_t request_ts, const common::ObPartitionKey& partition, const uint64_t cur_log_id,
      const ObDupTableLeaseStatus status, const int64_t lease_interval_us);
  int64_t get_request_ts() const
  {
    return request_ts_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  uint64_t get_cur_log_id() const
  {
    return cur_log_id_;
  }
  ObDupTableLeaseStatus get_status() const
  {
    return status_;
  }
  int64_t get_lease_interval_us() const
  {
    return lease_interval_us_;
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(request_ts), K_(partition), K_(cur_log_id), K_(status), K_(lease_interval_us));

private:
  // the send timestamp of a copy replica applying for lease
  int64_t request_ts_;
  common::ObPartitionKey partition_;
  uint64_t cur_log_id_;
  enum ObDupTableLeaseStatus status_;
  // the interval in us of the lease
  int64_t lease_interval_us_;
};

class ObRedoLogSyncRequestMsg : public ObDupTableMsgHeader {
  OB_UNIS_VERSION(1);

public:
  ObRedoLogSyncRequestMsg()
  {
    reset();
  }
  ~ObRedoLogSyncRequestMsg()
  {}
  int init(const common::ObPartitionKey& partition, const uint64_t log_id, const int64_t log_ts, const int64_t log_type,
      const ObTransID& trans_id);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  int64_t get_log_ts() const
  {
    return log_ts_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int64_t get_log_type() const
  {
    return log_type_;
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(partition), K_(log_id), K_(log_ts), K_(trans_id), K_(log_type));

private:
  common::ObPartitionKey partition_;
  uint64_t log_id_;
  int64_t log_ts_;
  int64_t log_type_;
  ObTransID trans_id_;
};

class ObRedoLogSyncResponseMsg : public ObDupTableMsgHeader {
  OB_UNIS_VERSION(1);

public:
  ObRedoLogSyncResponseMsg()
  {
    reset();
  }
  ~ObRedoLogSyncResponseMsg()
  {}
  enum class ObRedoLogSyncResponseStatus : int {
    OB_REDO_LOG_SYNC_UNKNOWN,
    OB_REDO_LOG_SYNC_SUCC,
    OB_REDO_LOG_SYNC_LEASE_EXPIRED,
    OB_REDO_LOG_SYNC_NOT_SYNC
  };
  int init(const common::ObPartitionKey& partition, const uint64_t log_id, const ObTransID& trans_id,
      const common::ObAddr& addr, const ObRedoLogSyncResponseStatus status);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  ObRedoLogSyncResponseStatus get_status() const
  {
    return status_;
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(partition), K_(log_id), K_(trans_id), K_(addr), K_(status));

private:
  common::ObPartitionKey partition_;
  uint64_t log_id_;
  ObTransID trans_id_;
  common::ObAddr addr_;
  enum ObRedoLogSyncResponseStatus status_;
};

}  // namespace transaction

namespace obrpc {
class ObDupTableRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObDupTableRpcProxy);

  RPC_AP(PRZ post_dup_table_lease_request, OB_DUP_TABLE_LEASE_REQUEST, (transaction::ObDupTableLeaseRequestMsg));
  RPC_AP(PRZ post_dup_table_lease_response, OB_DUP_TABLE_LEASE_RESPONSE, (transaction::ObDupTableLeaseResponseMsg));
  RPC_AP(PR3 post_redo_log_sync_request, OB_REDO_LOG_SYNC_REQUEST, (transaction::ObRedoLogSyncRequestMsg));
  RPC_AP(PR3 post_redo_log_sync_response, OB_REDO_LOG_SYNC_RESPONSE, (transaction::ObRedoLogSyncResponseMsg));
};

class ObDupTableLeaseRequestMsgP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_DUP_TABLE_LEASE_REQUEST>> {
public:
  explicit ObDupTableLeaseRequestMsgP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseRequestMsgP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObDupTableLeaseResponseMsgP
    : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_DUP_TABLE_LEASE_RESPONSE>> {
public:
  explicit ObDupTableLeaseResponseMsgP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseResponseMsgP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObRedoLogSyncRequestP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_REDO_LOG_SYNC_REQUEST>> {
public:
  explicit ObRedoLogSyncRequestP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedoLogSyncRequestP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObRedoLogSyncResponseP : public ObRpcProcessor<obrpc::ObDupTableRpcProxy::ObRpc<OB_REDO_LOG_SYNC_RESPONSE>> {
public:
  explicit ObRedoLogSyncResponseP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedoLogSyncResponseP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

}  // namespace obrpc

namespace transaction {
class ObIDupTableRpc {
public:
  ObIDupTableRpc()
  {}
  virtual ~ObIDupTableRpc()
  {}
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;

public:
  virtual int post_dup_table_lease_request(
      const uint64_t tenant_id, const common::ObAddr& server, const ObDupTableLeaseRequestMsg& msg) = 0;
  virtual int post_dup_table_lease_response(
      const uint64_t tenant_id, const common::ObAddr& server, const ObDupTableLeaseResponseMsg& msg) = 0;
  virtual int post_redo_log_sync_request(
      const uint64_t tenant_id, const common::ObAddr& server, const ObRedoLogSyncRequestMsg& msg) = 0;
  virtual int post_redo_log_sync_response(
      const uint64_t tenant_id, const common::ObAddr& server, const ObRedoLogSyncResponseMsg& msg) = 0;
};

class ObDupTableRpc : public ObIDupTableRpc {
public:
  ObDupTableRpc() : is_inited_(false), is_running_(false), trans_service_(NULL), rpc_proxy_(NULL)
  {}
  ~ObDupTableRpc()
  {
    destroy();
  }
  int init(ObTransService* trans_service, obrpc::ObDupTableRpcProxy* rpc_proxy);
  int start();
  int stop();
  int wait();
  void destroy();
  virtual int post_dup_table_lease_request(
      const uint64_t tenant_id, const common::ObAddr& server, const ObDupTableLeaseRequestMsg& msg);
  virtual int post_dup_table_lease_response(
      const uint64_t tenant_id, const common::ObAddr& server, const ObDupTableLeaseResponseMsg& msg);
  virtual int post_redo_log_sync_request(
      const uint64_t tenant_id, const common::ObAddr& server, const ObRedoLogSyncRequestMsg& msg);
  virtual int post_redo_log_sync_response(
      const uint64_t tenant_id, const common::ObAddr& server, const ObRedoLogSyncResponseMsg& msg);

private:
  bool is_inited_;
  bool is_running_;
  ObTransService* trans_service_;
  obrpc::ObDupTableRpcProxy* rpc_proxy_;
};

}  // namespace transaction

}  // namespace oceanbase

#endif
