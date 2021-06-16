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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_RPC_
#define OCEANBASE_ELECTION_OB_ELECTION_RPC_

#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/utility/ob_unify_serialize.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "ob_election_msg.h"
#include "ob_election_base.h"
#include "ob_election_async_log.h"
#include "share/rpc/ob_batch_rpc.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace observer {
class ObGlobalContext;
}

namespace election {
class ObIElectionMgr;
class ObElectionMsgBuffer;
class ObElectionMsg;
class ObElectionGroupId;
class ObPartArrayBuffer;
}  // namespace election

namespace storage {
class ObPartitionService;
}

namespace obrpc {
class ObElectionRpcResult {
  OB_UNIS_VERSION(1);

public:
  ObElectionRpcResult() : status_(common::OB_SUCCESS), send_timestamp_(0L)
  {}
  ~ObElectionRpcResult()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  void set_status(const int status)
  {
    status_ = status;
  }
  int get_status() const
  {
    return status_;
  }
  void set_timestamp(const int64_t timestamp)
  {
    send_timestamp_ = timestamp;
  }
  int64_t get_timestamp() const
  {
    return send_timestamp_;
  }

  TO_STRING_KV(K_(status), K_(send_timestamp));

private:
  int status_;
  int64_t send_timestamp_;
  DISALLOW_COPY_AND_ASSIGN(ObElectionRpcResult);
};

class ObElectionRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObElectionRpcProxy);
  RPC_AP(PR1 post_election_msg, OB_ELECTION, (election::ObElectionMsgBuffer), ObElectionRpcResult);
};

class ObElectionP : public ObRpcProcessor<obrpc::ObElectionRpcProxy::ObRpc<OB_ELECTION> > {
public:
  // ObElectionP(election::ObIElectionMgr *election_mgr) : election_mgr_(election_mgr) {}
  ObElectionP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObElectionP()
  {}

public:
  static void statistics(const int64_t fly_us, const int64_t wait_us, const int64_t handle_us);

protected:
  int process();

private:
  static const int64_t STATISTICS_INTERVAL = 5 * 1000 * 1000;
  static int64_t total_fly_us_;
  static int64_t total_wait_us_;
  static int64_t total_handle_us_;
  static int64_t total_process_;

private:
  // election::ObIElectionMgr *election_mgr_;
  storage::ObPartitionService* partition_service_;
  DISALLOW_COPY_AND_ASSIGN(ObElectionP);
};

template <ObRpcPacketCode PC>
class ObElectionRPCCB : public ObElectionRpcProxy::AsyncCB<PC> {
public:
  ObElectionRPCCB() : is_inited_(false), election_mgr_(NULL)
  {}
  virtual ~ObElectionRPCCB()
  {}
  int init(election::ObIElectionMgr* election_mgr);
  void set_args(const typename ObElectionRpcProxy::AsyncCB<PC>::Request& args)
  {
    UNUSED(args);
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB* clone(const oceanbase::rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObElectionRPCCB<PC>* newcb = new (buf) ObElectionRPCCB<PC>();
    if (newcb) {
      newcb->is_inited_ = is_inited_;
      newcb->election_mgr_ = election_mgr_;
    }
    return newcb;
  }

public:
  int process();
  void on_timeout();

private:
  bool is_inited_;
  election::ObIElectionMgr* election_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObElectionRPCCB);
};

template <ObRpcPacketCode PC>
int ObElectionRPCCB<PC>::init(election::ObIElectionMgr* election_mgr)
{
  int ret = common::OB_SUCCESS;

  if (is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionRPCCB inited twice");
    ret = common::OB_INIT_TWICE;
  } else if (OB_ISNULL(election_mgr)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", KP(election_mgr));
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    election_mgr_ = election_mgr;
    is_inited_ = true;
  }

  return ret;
}

template <ObRpcPacketCode PC>
int ObElectionRPCCB<PC>::process()
{
  int ret = common::OB_SUCCESS;
  const ObElectionRpcResult& result = ObElectionRpcProxy::AsyncCB<PC>::result_;
  ObRpcResultCode& rcode = ObElectionRpcProxy::AsyncCB<PC>::rcode_;
  const common::ObAddr& dst = ObElectionRpcProxy::AsyncCB<PC>::dst_;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionRPCCB not inited");
    ret = common::OB_NOT_INIT;
  } else if (OB_ISNULL(election_mgr_)) {
    ELECT_ASYNC_LOG(WARN, "election manager is null");
    ret = common::OB_ERR_UNEXPECTED;
  } else if (common::OB_SUCCESS != rcode.rcode_) {
    ELECT_ASYNC_LOG(WARN, "election rpc error", K(rcode), K(dst));
    ret = rcode.rcode_;
  } else if (common::OB_SUCCESS != result.get_status()) {
    ELECT_ASYNC_LOG(
        WARN, "election rpc error", "status", result.get_status(), "timestamp", result.get_timestamp(), K(dst));
  } else {
    // do nothing
  }

  return ret;
}

template <ObRpcPacketCode PC>
void ObElectionRPCCB<PC>::on_timeout()
{
  static const int64_t LOG_INTERVAL = 1000 * 1000;
  const common::ObAddr& dst = ObElectionRpcProxy::AsyncCB<PC>::dst_;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionRPCCB not inited");
  } else if (OB_ISNULL(election_mgr_)) {
    ELECT_ASYNC_LOG(WARN, "election manager is null");
  } else if (REACH_TIME_INTERVAL(LOG_INTERVAL)) {
    ELECT_ASYNC_LOG(WARN, "election rpc timeout", K(dst));
  } else {
    // do nothing
  }
}
}  // namespace obrpc

namespace election {

class ObIElectionRpc {
public:
  ObIElectionRpc()
  {}
  virtual ~ObIElectionRpc()
  {}
  virtual int init(obrpc::ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const common::ObAddr& self) = 0;
  virtual void destroy() = 0;

public:
  virtual int post_election_msg(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition, const ObElectionMsg& msg) = 0;
  virtual int post_election_group_msg(const common::ObAddr& server, const int64_t cluster_id,
      const ObElectionGroupId& eg_id, const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg) = 0;
};

class ObElectionRpc : public ObIElectionRpc {
public:
  ObElectionRpc() : is_inited_(false), rpc_proxy_(NULL), election_mgr_(NULL)
  {}
  ~ObElectionRpc()
  {
    destroy();
  }
  int init(obrpc::ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const common::ObAddr& self);
  void destroy();

public:
  int post_election_msg(const common::ObAddr& server, const int64_t cluster_id, const common::ObPartitionKey& partition,
      const ObElectionMsg& msg);
  int post_election_group_msg(const common::ObAddr& server, const int64_t cluster_id, const ObElectionGroupId& eg_id,
      const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg);

private:
  static const int64_t MAX_PROCESS_HANDLER_TIME = 10 * 1000;

private:
  bool is_inited_;
  obrpc::ObElectionRpcProxy* rpc_proxy_;
  obrpc::ObElectionRPCCB<obrpc::OB_ELECTION> election_cb_;
  ObIElectionMgr* election_mgr_;
  common::ObAddr self_;
  DISALLOW_COPY_AND_ASSIGN(ObElectionRpc);
};

class ObElectionBatchRpc : public ObIElectionRpc {
public:
  ObElectionBatchRpc() : rpc_(NULL)
  {}
  ~ObElectionBatchRpc()
  {
    destroy();
  }
  int init(obrpc::ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const common::ObAddr& self)
  {
    UNUSED(rpc_proxy);
    UNUSED(election_mgr);
    UNUSED(self);
    return 0;
  }
  int init(obrpc::ObBatchRpc* rpc)
  {
    int ret = common::OB_SUCCESS;
    rpc_ = rpc;
    return ret;
  }
  void destroy()
  {}

public:
  int post_election_msg(const common::ObAddr& server, const int64_t cluster_id, const common::ObPartitionKey& partition,
      const ObElectionMsg& msg);
  int post_election_group_msg(const common::ObAddr& server, const int64_t cluster_id, const ObElectionGroupId& eg_id,
      const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg);

private:
  obrpc::ObBatchRpc* rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObElectionBatchRpc);
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_RPC_
