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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_RPC_
#define OCEANBASE_TRANSACTION_OB_TRANS_RPC_

#include "common/ob_partition_key.h"
#include "common/ob_queue_thread.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "share/rpc/ob_batch_rpc.h"
#include "ob_trans_msg.h"
#include "ob_trans_define.h"
#include "ob_trans_msg_type.h"
#include "ob_trans_factory.h"
#include "ob_trans_msg_type2.h"
#include "ob_trans_msg2.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {

namespace observer {
class ObGlobalContext;
}

namespace storage {
class ObPartitionService;
}

namespace transaction {
class ObTransService;
class ObTransMsg;

int refresh_location_cache(
    ObTransService* trans_service, const common::ObPartitionKey& partition, const bool need_clear_cache);
int handle_trans_msg_callback(ObTransService* trans_service, const ObPartitionKey& partition, const ObTransID& trans_id,
    const int64_t msg_type, const int status, const int64_t task_type, const ObAddr& addr, const int64_t sql_no,
    const int64_t request_id);
}  // namespace transaction

namespace obrpc {
class ObTransRpcResult {
  OB_UNIS_VERSION(1);

public:
  ObTransRpcResult()
  {
    reset();
  }
  virtual ~ObTransRpcResult()
  {}

  void init(const int status, const int64_t timestamp);
  int get_status() const
  {
    return status_;
  }
  int64_t get_timestamp() const
  {
    return send_timestamp_;
  }
  void reset();
  TO_STRING_KV(K_(status), K_(send_timestamp));

public:
  static const int64_t OB_TRANS_RPC_TIMEOUT_THRESHOLD = 3 * 1000 * 1000;

private:
  int status_;
  int64_t send_timestamp_;
};

class ObTransRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObTransRpcProxy);

  RPC_AP(PR3 post_trans_msg, OB_TRANS, (transaction::ObTransMsg), ObTransRpcResult);
  RPC_AP(PR3 post_trans_resp_msg, OB_TRANS_RESP, (transaction::ObTransMsg));
};

class ObTransP : public ObRpcProcessor<obrpc::ObTransRpcProxy::ObRpc<OB_TRANS> > {
public:
  ObTransP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransP);

private:
  storage::ObPartitionService* partition_service_;
};

class ObTransRespP : public ObRpcProcessor<obrpc::ObTransRpcProxy::ObRpc<OB_TRANS_RESP> > {
public:
  ObTransRespP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransRespP);

private:
  storage::ObPartitionService* partition_service_;
};

template <ObRpcPacketCode PC>
class ObTransRPCCB : public ObTransRpcProxy::AsyncCB<PC> {
public:
  ObTransRPCCB()
      : is_inited_(false),
        msg_type_(transaction::OB_TRANS_MSG_UNKNOWN),
        trans_service_(NULL),
        sql_no_(-1),
        request_id_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObTransRPCCB()
  {}
  void set_args(const typename ObTransRpcProxy::AsyncCB<PC>::Request& args)
  {
    pkey_ = args.get_receiver();
    trans_id_ = args.get_trans_id();
    msg_type_ = args.get_msg_type();
    sql_no_ = args.get_sql_no();
    request_id_ = args.get_request_id();
  }
  int init(transaction::ObTransService* trans_service);
  oceanbase::rpc::frame::ObReqTransport::AsyncCB* clone(const oceanbase::rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObTransRPCCB<PC>* newcb = new (buf) ObTransRPCCB<PC>();
    if (newcb) {
      newcb->is_inited_ = is_inited_;
      newcb->trans_service_ = trans_service_;
      newcb->pkey_ = pkey_;
      newcb->trans_id_ = trans_id_;
      newcb->msg_type_ = msg_type_;
      newcb->sql_no_ = sql_no_;
      newcb->request_id_ = request_id_;
    }
    return newcb;
  }

public:
  int process();
  void on_timeout();

private:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  transaction::ObTransID trans_id_;
  int64_t msg_type_;
  transaction::ObTransService* trans_service_;
  int64_t sql_no_;
  int64_t request_id_;
};

template <ObRpcPacketCode PC>
int ObTransRPCCB<PC>::init(transaction::ObTransService* trans_service)
{
  int ret = common::OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransRPCCB inited twice");
    ret = common::OB_INIT_TWICE;
  } else if (OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN, "invalid argument", KP(trans_service));
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    trans_service_ = trans_service;
    is_inited_ = true;
  }

  return ret;
}

template <ObRpcPacketCode PC>
int ObTransRPCCB<PC>::process()
{
  int ret = common::OB_SUCCESS;
  const ObTransRpcResult& result = ObTransRpcProxy::AsyncCB<PC>::result_;
  const ObAddr& dst = ObTransRpcProxy::AsyncCB<PC>::dst_;
  ObRpcResultCode& rcode = ObTransRpcProxy::AsyncCB<PC>::rcode_;
  const int64_t LOG_INTERVAL_US = 1 * 1000 * 1000;
  bool need_refresh = false;
  int status = common::OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransRPCCB not inited");
    ret = common::OB_NOT_INIT;
  } else if (OB_ISNULL(trans_service_)) {
    TRANS_LOG(WARN, "trans service is NULL");
    ret = common::OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!pkey_.is_valid())) {
    TRANS_LOG(WARN, "receiver is invalid", K_(pkey), K_(trans_id), K(dst));
    ret = common::OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(common::OB_SUCCESS != rcode.rcode_)) {
    status = rcode.rcode_;
    TRANS_LOG(WARN, "transaction rpc error", K(rcode), K_(pkey), K_(trans_id), K(dst), K_(msg_type));
    if (common::OB_TENANT_NOT_IN_SERVER == status) {
      need_refresh = true;
      if (OB_FAIL(handle_trans_msg_callback(trans_service_,
              pkey_,
              trans_id_,
              msg_type_,
              status,
              transaction::ObTransRetryTaskType::CALLBACK_TASK,
              dst,
              sql_no_,
              request_id_))) {
        TRANS_LOG(WARN,
            "handle transaction msg callback error",
            K(ret),
            KP(trans_service_),
            K_(trans_id),
            K_(msg_type),
            "rcode",
            status,
            K(dst));
      }
    } else {
      ret = rcode.rcode_;
    }
  } else {
    status = result.get_status();
    if (common::OB_SUCCESS != status) {
      if (common::OB_NOT_MASTER == status || common::OB_PARTITION_NOT_EXIST == status) {
        need_refresh = true;
      } else {
        TRANS_LOG(WARN, "transaction rpc error", K(result), K_(pkey), K_(trans_id), K(dst), K_(msg_type));
      }
    }
  }
  // ignore ret
  if (need_refresh) {
    const bool need_clear_cache = true;
    if (OB_FAIL(transaction::refresh_location_cache(trans_service_, pkey_, need_clear_cache))) {
      TRANS_LOG(WARN,
          "refresh location cache error",
          KR(ret),
          K_(trans_id),
          "partition",
          pkey_,
          K(result),
          K(dst),
          K(status),
          K_(msg_type));
    } else if (REACH_TIME_INTERVAL(LOG_INTERVAL_US)) {
      TRANS_LOG(INFO, "refresh location cache", K_(trans_id), K_(pkey), K(dst), K_(msg_type), K(status));
    } else {
      TRANS_LOG(DEBUG, "refresh location cache", K_(trans_id), K_(pkey), K(dst), K_(msg_type), K(status));
    }
  }

  return ret;
}

template <ObRpcPacketCode PC>
void ObTransRPCCB<PC>::on_timeout()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObAddr& dst = ObTransRpcProxy::AsyncCB<PC>::dst_;
  int status = common::OB_TRANS_RPC_TIMEOUT;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransRPCCB not inited");
  } else if (OB_ISNULL(trans_service_)) {
    TRANS_LOG(WARN, "trans service is NULL");
  } else if (transaction::ObTransMsgTypeChecker::is_2pc_msg_type(msg_type_)) {
    // do nothing
  } else {
    if (pkey_.is_valid()) {
      const bool need_clear_cache = false;
      if (OB_FAIL(transaction::refresh_location_cache(trans_service_, pkey_, need_clear_cache))) {
        TRANS_LOG(WARN, "refresh location cache error", KR(ret), K_(trans_id), "partition", pkey_, K(dst));
      } else {
        TRANS_LOG(DEBUG, "refresh location cache", K_(trans_id), "partition", pkey_, K(dst));
      }
      // 1. in start_stmt phase, if remote server crash stop, weak read request sql thread
      //    will be waken up and retry the stmt
      // 2. for read-only transaction, if remote server crash stop when tranaction commit,
      //    scheduler no need to retry the request
      // 3. for read-write transaction, if need to rollback, location cache will be refreshed after timeout reached
      //    clean cache of current transaction and resend request to new leader
      // 4. for other type of messsage, don't handle but register asynchronouse task
      if (OB_SUCCESS != (tmp_ret = transaction::handle_trans_msg_callback(trans_service_,
                             pkey_,
                             trans_id_,
                             msg_type_,
                             status,
                             transaction::ObTransRetryTaskType::NOTIFY_TASK,
                             dst,
                             sql_no_,
                             request_id_))) {
        TRANS_LOG(WARN, "notify start stmt", "ret", tmp_ret, K_(trans_id), K_(msg_type), "receiver", pkey_, K(dst));
        ret = tmp_ret;
      }
    } else {
      TRANS_LOG(WARN, "receiver is invalid", K_(trans_id), "partition", pkey_, K(dst));
    }
    TRANS_LOG(WARN,
        "transaction rpc timeout",
        K(tmp_ret),
        K_(pkey),
        K_(trans_id),
        K_(msg_type),
        K(dst),
        K(sql_no_),
        "fd",
        get_fd(ObTransRpcProxy::AsyncCB<PC>::req_),
        K(status));
  }
}

}  // namespace obrpc

namespace transaction {
class TransRpcTask : public ObTransTask {
public:
  TransRpcTask()
  {
    reset();
  }
  ~TransRpcTask()
  {
    reset();
  }
  void reset();
  int init(const ObTransMsg& msg, const int64_t task_type);
  int init(const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t msg_type, const int status,
      const int64_t task_type, const ObAddr& addr, const int64_t sql_no, const int64_t request_id);
  int init(const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t msg_type, const int64_t request_id,
      const int64_t task_type, const int64_t sql_no, const int64_t task_timeout);
  const ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int64_t get_msg_type() const
  {
    return msg_type_;
  }
  int get_status() const
  {
    return status_;
  }
  const ObTransMsg& get_msg() const
  {
    return msg_;
  }
  int64_t get_task_type() const
  {
    return task_type_;
  }
  const ObAddr& get_addr() const
  {
    return addr_;
  }
  int64_t get_request_id() const
  {
    return request_id_;
  }
  int64_t get_sql_no() const
  {
    return sql_no_;
  }
  int64_t get_task_timeout() const
  {
    return task_timeout_;
  }
  TO_STRING_KV(K_(partition), K_(trans_id), K_(msg_type), K_(status), K_(msg), K_(task_type), K_(addr), K_(request_id),
      K_(sql_no), K_(task_timeout));

public:
  ObPartitionKey partition_;
  ObTransID trans_id_;
  int64_t msg_type_;
  int status_;
  ObTransMsg msg_;
  ObAddr addr_;
  int64_t request_id_;
  int64_t sql_no_;
  int64_t task_timeout_;
};

class ObITransRpc {
public:
  ObITransRpc()
  {}
  virtual ~ObITransRpc()
  {}
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;

public:
  virtual int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type) = 0;
  virtual int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type) = 0;
  virtual int post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg) = 0;
};

class ObTransRpc : public ObITransRpc {
public:
  ObTransRpc()
      : is_inited_(false),
        is_running_(false),
        rpc_proxy_(NULL),
        trans_service_(NULL),
        total_trans_msg_count_(0),
        total_trans_resp_msg_count_(0),
        last_stat_ts_(0)
  {}
  ~ObTransRpc()
  {
    destroy();
  }
  int init(obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const common::ObAddr& self,
      obrpc::ObBatchRpc* rpc);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type);
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type);
  int post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg);

private:
  void statistics_();

private:
  static const int64_t STAT_INTERVAL = 1 * 1000 * 1000;

private:
  bool is_inited_;
  bool is_running_;
  obrpc::ObTransRpcProxy* rpc_proxy_;
  obrpc::ObTransRPCCB<obrpc::OB_TRANS> trans_cb_;
  ObTransService* trans_service_;
  common::ObAddr self_;
  int64_t total_trans_msg_count_;
  int64_t total_trans_resp_msg_count_;
  int64_t last_stat_ts_;
};

class ObTransBatchRpc : public ObITransRpc {
public:
  ObTransBatchRpc() : trx_rpc_(), batch_rpc_(NULL)
  {}
  ~ObTransBatchRpc()
  {
    destroy();
  }
  int init(obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const common::ObAddr& self,
      obrpc::ObBatchRpc* rpc);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type);
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type);
  int post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg);

private:
  ObTransRpc trx_rpc_;
  obrpc::ObBatchRpc* batch_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObTransBatchRpc);
};

}  // namespace transaction

}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_RPC_
