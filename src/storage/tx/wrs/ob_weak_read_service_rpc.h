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

#ifndef OCEANBASE_TRANSACTION_OB_WEAK_READ_SERVICE_RPC_H_
#define OCEANBASE_TRANSACTION_OB_WEAK_READ_SERVICE_RPC_H_

#include "ob_i_weak_read_service.h"             // ObIWeakReadService

#include "ob_weak_read_service_rpc_define.h"    // ObWrsRpcProxy

namespace oceanbase
{
namespace obrpc
{
struct ObRpcResultCode;
}

namespace transaction
{

class ObIWrsRpc
{
public:
  virtual ~ObIWrsRpc()  {}

public:
  virtual int get_cluster_version(const common::ObAddr &server,
      const uint64_t tenant_id,
      const obrpc::ObWrsGetClusterVersionRequest &req,
      obrpc::ObWrsGetClusterVersionResponse &res) = 0;

  virtual int post_cluster_heartbeat(const common::ObAddr &server,
      const uint64_t tenant_id,
      const obrpc::ObWrsClusterHeartbeatRequest &req) = 0;
};


class ObWrsRpc : public ObIWrsRpc
{
  static const int64_t MAX_RPC_PROCESS_HANDLER_TIME = 100 * 1000L;  // report warn threshold
public:
  ObWrsRpc();
  virtual ~ObWrsRpc() {}

  int init(const rpc::frame::ObReqTransport *transport, ObIWeakReadService &wrs);

  virtual int get_cluster_version(const common::ObAddr &server,
      const uint64_t tenant_id,
      const obrpc::ObWrsGetClusterVersionRequest &req,
      obrpc::ObWrsGetClusterVersionResponse &res);

  virtual int post_cluster_heartbeat(const common::ObAddr &server,
      const uint64_t tenant_id,
      const obrpc::ObWrsClusterHeartbeatRequest &req);

public:
  class ClusterHeartbeatCB : public obrpc::ObWrsRpcProxy::AsyncCB<obrpc::OB_WRS_CLUSTER_HEARTBEAT>
  {
  public:
    ClusterHeartbeatCB() : wrs_(NULL) {}
    virtual ~ClusterHeartbeatCB() {}

  public:
    void init(ObIWeakReadService *wrs) { wrs_ = wrs; }
    void set_args(const Request &args) { UNUSED(args); }
    rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const;
    int process();
    void on_timeout();
    void on_invalid();
  private:
    int do_process_(const obrpc::ObRpcResultCode &rcode);
  private:
    ObIWeakReadService *wrs_;
  };

private:
  bool                  inited_;
  obrpc::ObWrsRpcProxy  proxy_;
  ClusterHeartbeatCB    cluster_heartbeat_cb_;
};
} // transaction

} // oceanbase

#endif
