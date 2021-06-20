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

#ifndef OCEANBASE_TRANSACTION_OB_XA_RPC_
#define OCEANBASE_TRANSACTION_OB_XA_RPC_

#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "lib/utility/ob_unify_serialize.h"
#include "ob_trans_define.h"
#include "ob_trans_result.h"

namespace oceanbase {

namespace observer {
class ObGlobalContext;
}

namespace obrpc {
static const int64_t OB_XA_RPC_TIMEOUT = 5000000;

class ObXAPrepareRPCRequest {
  OB_UNIS_VERSION(1);

public:
  ObXAPrepareRPCRequest()
  {}
  ~ObXAPrepareRPCRequest()
  {}
  int init(const transaction::ObTransID& trans_id, const transaction::ObXATransID& xid, const int64_t stmt_timeout);
  const transaction::ObXATransID& get_xid() const
  {
    return xid_;
  }
  transaction::ObTransID get_trans_id() const
  {
    return trans_id_;
  }
  bool is_valid() const
  {
    return trans_id_.is_valid();
  }
  int64_t get_stmt_timeout() const
  {
    return stmt_timeout_;
  }
  TO_STRING_KV(K_(trans_id), K_(xid), K_(stmt_timeout));

private:
  transaction::ObTransID trans_id_;
  transaction::ObXATransID xid_;
  int64_t stmt_timeout_;
};

class ObXAEndTransRPCRequest {
  OB_UNIS_VERSION(1);

public:
  ObXAEndTransRPCRequest()
  {}
  ~ObXAEndTransRPCRequest()
  {}
  int init(const transaction::ObTransID& trans_id, const transaction::ObXATransID& xid, const bool is_rollback,
      const bool is_terminated = false);
  bool is_valid() const
  {
    return trans_id_.is_valid();
  }
  transaction::ObTransID get_trans_id() const
  {
    return trans_id_;
  }
  const transaction::ObXATransID& get_xid() const
  {
    return xid_;
  }
  bool get_is_rollback() const
  {
    return is_rollback_;
  }
  void set_terminated()
  {
    is_terminated_ = true;
  }
  bool is_terminated() const
  {
    return is_terminated_;
  }
  TO_STRING_KV(K_(trans_id), K_(xid), K_(is_rollback), K_(is_terminated));

private:
  transaction::ObTransID trans_id_;
  transaction::ObXATransID xid_;
  bool is_rollback_;
  // false by default
  bool is_terminated_;
};

class ObXASyncStatusRPCRequest {
  OB_UNIS_VERSION(1);

public:
  ObXASyncStatusRPCRequest()
  {}
  ~ObXASyncStatusRPCRequest()
  {}
  int init(const transaction::ObTransID& trans_id, const transaction::ObXATransID& xid, const common::ObAddr& sender,
      const bool is_new_branch, const bool is_stmt_pull, const bool is_tightly_coupled, const int64_t timeout_seconds);
  const transaction::ObXATransID& get_xid() const
  {
    return xid_;
  }
  transaction::ObTransID get_trans_id() const
  {
    return trans_id_;
  }
  const common::ObAddr& get_sender() const
  {
    return sender_;
  }
  bool get_is_new_branch() const
  {
    return is_new_branch_;
  }
  bool get_is_stmt_pull() const
  {
    return is_stmt_pull_;
  }
  bool get_is_tightly_coupled() const
  {
    return is_tightly_coupled_;
  }
  bool get_pull_trans_desc() const
  {
    return pull_trans_desc_;
  }
  void set_pull_trans_desc(const bool pull_trans_desc)
  {
    pull_trans_desc_ = pull_trans_desc;
  }
  int64_t get_timeout_seconds() const
  {
    return timeout_seconds_;
  }
  bool is_valid() const
  {
    return trans_id_.is_valid();
  }
  TO_STRING_KV(K_(trans_id), K_(xid), K_(sender), K_(is_new_branch), K_(is_stmt_pull), K_(is_tightly_coupled),
      K_(pull_trans_desc), K_(timeout_seconds));

private:
  transaction::ObTransID trans_id_;
  transaction::ObXATransID xid_;
  common::ObAddr sender_;
  bool is_new_branch_;
  bool is_stmt_pull_;
  // for tightly-coupled xa branches
  bool is_tightly_coupled_;
  bool pull_trans_desc_;
  int64_t timeout_seconds_;
};

class ObXASyncStatusRPCResponse {
  OB_UNIS_VERSION(1);

public:
  ObXASyncStatusRPCResponse()
  {}
  ~ObXASyncStatusRPCResponse()
  {}
  int init(const transaction::ObTransDesc& trans_desc, const bool is_stmt_pull);
  const transaction::ObTransDesc& get_trans_desc() const
  {
    return trans_desc_;
  }
  bool get_is_stmt_pull() const
  {
    return is_stmt_pull_;
  }
  bool is_valid() const
  {
    return trans_desc_.is_valid();
  }
  TO_STRING_KV(K_(trans_desc), K_(is_stmt_pull));

private:
  transaction::ObTransDesc trans_desc_;
  bool is_stmt_pull_;
};

class ObXAMergeStatusRPCRequest {
  OB_UNIS_VERSION(1);

public:
  ObXAMergeStatusRPCRequest()
  {}
  ~ObXAMergeStatusRPCRequest()
  {}
  int init(const transaction::ObTransDesc& trans_desc, const bool is_stmt_push, const bool is_tightly_coupled,
      const transaction::ObXATransID& xid, const int64_t seq_no);
  const transaction::ObTransDesc& get_trans_desc() const
  {
    return trans_desc_;
  }
  bool get_is_stmt_push() const
  {
    return is_stmt_push_;
  }
  bool get_is_tightly_coupled() const
  {
    return is_tightly_coupled_;
  }
  const transaction::ObXATransID& get_xid() const
  {
    return xid_;
  }
  int64_t get_seq_no() const
  {
    return seq_no_;
  }
  bool is_valid() const
  {
    return trans_desc_.is_valid() && xid_.is_valid();
  }
  TO_STRING_KV(K_(trans_desc), K_(is_stmt_push), K_(is_tightly_coupled), K_(xid), K_(seq_no));

private:
  transaction::ObTransDesc trans_desc_;
  bool is_stmt_push_;
  bool is_tightly_coupled_;
  transaction::ObXATransID xid_;
  int64_t seq_no_;
};

class ObXAHbRequest {
  OB_UNIS_VERSION(1);

public:
  ObXAHbRequest()
  {}
  ~ObXAHbRequest()
  {}
  int init(const transaction::ObTransID& trans_id, const transaction::ObXATransID& xid, const common::ObAddr& sender);
  const transaction::ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  const transaction::ObXATransID& get_xid() const
  {
    return xid_;
  }
  const ObAddr& get_sender() const
  {
    return sender_;
  }
  bool is_valid() const
  {
    return trans_id_.is_valid() && xid_.is_valid() && sender_.is_valid();
  }
  TO_STRING_KV(K_(trans_id), K_(xid), K_(sender));

private:
  transaction::ObTransID trans_id_;
  transaction::ObXATransID xid_;
  common::ObAddr sender_;
};

typedef ObXAHbRequest ObXAHbResponse;

/*class ObXAHbResponse
{
  OB_UNIS_VERSION(1);
public:
  ObXAHbResponse() {}
  ~ObXAHbResponse() {}
  int init(const transaction::ObTransID &trans_id, const transaction::ObXATransID &xid);
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  bool is_valid() const { return trans_id_.is_valid() && xid_.is_valid(); }
  TO_STRING_KV(K_(trans_id), K_(xid));
private:
  transaction::ObTransID trans_id_;
  transaction::ObXATransID xid_;
};*/

class ObXARpcProxy : public ObRpcProxy {
public:
  DEFINE_TO(ObXARpcProxy);
  RPC_AP(PR1 xa_prepare, OB_XA_PREPARE, (ObXAPrepareRPCRequest), Int64);
  RPC_AP(PR1 xa_end_trans, OB_XA_END_TRANS, (ObXAEndTransRPCRequest), Int64);
  RPC_AP(PR1 xa_sync_status, OB_XA_SYNC_STATUS, (ObXASyncStatusRPCRequest), Int64);
  RPC_AP(PR1 xa_sync_status_response, OB_XA_SYNC_STATUS_RESPONSE, (ObXASyncStatusRPCResponse), Int64);
  RPC_AP(PR1 xa_merge_status, OB_XA_MERGE_STATUS, (ObXAMergeStatusRPCRequest), Int64);
  RPC_AP(PR1 xa_hb_req, OB_XA_HB_REQ, (ObXAHbRequest), Int64);
  RPC_AP(PR1 xa_hb_resp, OB_XA_HB_RESP, (ObXAHbResponse), Int64);
};

class ObXAPrepareP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_PREPARE>> {
public:
  explicit ObXAPrepareP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXAPrepareP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObXAEndTransP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_END_TRANS>> {
public:
  explicit ObXAEndTransP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXAEndTransP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObXASyncStatusP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_SYNC_STATUS>> {
public:
  explicit ObXASyncStatusP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXASyncStatusP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObXASyncStatusResponseP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_SYNC_STATUS_RESPONSE>> {
public:
  explicit ObXASyncStatusResponseP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXASyncStatusResponseP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObXAMergeStatusP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_MERGE_STATUS>> {
public:
  explicit ObXAMergeStatusP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXAMergeStatusP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObXAHbReqP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_HB_REQ>> {
public:
  explicit ObXAHbReqP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXAHbReqP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

class ObXAHbRespP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_HB_RESP>> {
public:
  explicit ObXAHbRespP(const observer::ObGlobalContext& global_ctx) : global_ctx_(global_ctx)
  {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObXAHbRespP);

private:
  const observer::ObGlobalContext& global_ctx_;
};

template <ObRpcPacketCode PC>
class ObXARPCCB : public ObXARpcProxy::AsyncCB<PC> {
public:
  ObXARPCCB() : is_inited_(false), cond_(NULL)
  {}
  ~ObXARPCCB()
  {}
  int init(transaction::ObTransCond* cond)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObXARPCCB init twice", KR(ret));
    } else {
      cond_ = cond;
      is_inited_ = true;
    }
    return ret;
  }
  void reset()
  {
    cond_ = NULL;
    is_inited_ = false;
  }
  void set_args(const typename ObXARpcProxy::AsyncCB<PC>::Request& args)
  {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObXARPCCB<PC>* newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObXARPCCB<PC>();
      newcb->cond_ = cond_;
      newcb->is_inited_ = true;
    }
    return newcb;
  }
  int process()
  {
    const common::ObAddr& dst = ObXARpcProxy::AsyncCB<PC>::dst_;
    ObRpcResultCode& rcode = ObXARpcProxy::AsyncCB<PC>::rcode_;
    const Int64 result = ObXARpcProxy::AsyncCB<PC>::result_;

    if (OB_SUCCESS != rcode.rcode_) {
      TRANS_LOG(WARN, "xa rpc returns error code", K(rcode), K(dst));
      // TODO, check ret code
      if (OB_NOT_NULL(cond_)) {
        cond_->notify(rcode.rcode_);
      }
    } else {
      if (OB_NOT_NULL(cond_)) {
        cond_->notify(result);
      }
    }
    return OB_SUCCESS;
  }
  void on_timeout()
  {
    const common::ObAddr& dst = ObXARpcProxy::AsyncCB<PC>::dst_;
    const int real_ret = this->get_error();
    const int ret =
        ((EASY_OK == real_ret || EASY_TIMEOUT == real_ret) ? common::OB_TIMEOUT : common::OB_RPC_CONNECT_ERROR);
    TRANS_LOG(WARN, "XA rpc timeout", K(ret), K(real_ret), K(dst), "lbt", lbt());
    if (OB_NOT_NULL(cond_)) {
      cond_->notify(ret);
    }
  }

private:
  bool is_inited_;
  transaction::ObTransCond* cond_;
};

}  // namespace obrpc

using namespace obrpc;

namespace transaction {

class ObIXARpc {
public:
  ObIXARpc()
  {}
  virtual ~ObIXARpc()
  {}
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;

public:
  virtual int xa_prepare(const uint64_t tenant_id, const common::ObAddr& server, const ObXAPrepareRPCRequest& req,
      ObXARPCCB<OB_XA_PREPARE>& cb) = 0;
  virtual int xa_end_trans(const uint64_t tenant_id, const common::ObAddr& server, const ObXAEndTransRPCRequest& req,
      ObXARPCCB<OB_XA_END_TRANS>& cb) = 0;
  virtual int xa_sync_status(const uint64_t tenant_id, const common::ObAddr& server,
      const ObXASyncStatusRPCRequest& req, ObXARPCCB<OB_XA_SYNC_STATUS>* cb) = 0;
  virtual int xa_sync_status_response(const uint64_t tenant_id, const common::ObAddr& server,
      const ObXASyncStatusRPCResponse& resp, ObXARPCCB<OB_XA_SYNC_STATUS_RESPONSE>* cb) = 0;
  virtual int xa_merge_status(const uint64_t tenant_id, const common::ObAddr& server,
      const ObXAMergeStatusRPCRequest& req, ObXARPCCB<OB_XA_MERGE_STATUS>* cb) = 0;
  virtual int xa_hb_req(const uint64_t tenant_id, const common::ObAddr& server, const ObXAHbRequest& req,
      ObXARPCCB<OB_XA_HB_REQ>* cb) = 0;
  virtual int xa_hb_resp(const uint64_t tenant_id, const common::ObAddr& server, const ObXAHbResponse& resp,
      ObXARPCCB<OB_XA_HB_RESP>* cb) = 0;
};

class ObXARpc : public ObIXARpc {
public:
  ObXARpc() : is_inited_(false), is_running_(false), rpc_proxy_(NULL)
  {}
  ~ObXARpc()
  {
    destroy();
  }
  int init(ObXARpcProxy* proxy, const common::ObAddr& self);
  int start();
  int stop();
  int wait();
  void destroy();

public:
  int xa_prepare(const uint64_t tenant_id, const common::ObAddr& server, const ObXAPrepareRPCRequest& req,
      ObXARPCCB<OB_XA_PREPARE>& cb);
  int xa_end_trans(const uint64_t tenant_id, const common::ObAddr& server, const ObXAEndTransRPCRequest& req,
      ObXARPCCB<OB_XA_END_TRANS>& cb);
  int xa_sync_status(const uint64_t tenant_id, const common::ObAddr& server, const ObXASyncStatusRPCRequest& req,
      ObXARPCCB<OB_XA_SYNC_STATUS>* cb);
  int xa_sync_status_response(const uint64_t tenant_id, const common::ObAddr& server,
      const ObXASyncStatusRPCResponse& resp, ObXARPCCB<OB_XA_SYNC_STATUS_RESPONSE>* cb);
  int xa_merge_status(const uint64_t tenant_id, const common::ObAddr& server, const ObXAMergeStatusRPCRequest& req,
      ObXARPCCB<OB_XA_MERGE_STATUS>* cb);
  int xa_hb_req(
      const uint64_t tenant_id, const common::ObAddr& server, const ObXAHbRequest& req, ObXARPCCB<OB_XA_HB_REQ>* cb);
  int xa_hb_resp(
      const uint64_t tenant_id, const common::ObAddr& server, const ObXAHbResponse& resp, ObXARPCCB<OB_XA_HB_RESP>* cb);

private:
  bool is_inited_;
  bool is_running_;
  ObXARpcProxy* rpc_proxy_;
  common::ObAddr self_;
};

}  // namespace transaction

}  // namespace oceanbase

#endif
