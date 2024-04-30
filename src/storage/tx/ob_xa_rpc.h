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

namespace oceanbase
{

namespace observer
{
struct ObGlobalContext;
}

namespace obrpc
{
static const int64_t OB_XA_RPC_TIMEOUT = 5000000;

class ObXAPrepareRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXAPrepareRPCRequest() {}
  ~ObXAPrepareRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           const transaction::ObXATransID &xid,
           const int64_t stmt_timeout);
  const transaction::ObXATransID &get_xid() const { return xid_; }
  transaction::ObTransID get_tx_id() const { return tx_id_; }
  bool is_valid() const { return tx_id_.is_valid(); }
  int64_t get_timeout_us() const { return timeout_us_; }
  TO_STRING_KV(K_(tx_id), K_(xid), K_(timeout_us));
private:
  transaction::ObTransID tx_id_;
  transaction::ObXATransID xid_;
  int64_t timeout_us_;
};

class ObXAStartRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXAStartRPCRequest() {}
  ~ObXAStartRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           const transaction::ObXATransID &xid,
           const common::ObAddr &sender,
           const bool is_new_branch,
           const bool is_tightly_coupled,
           const int64_t timeout_seconds,
           const int64_t flags,
           const bool is_first_branch);
  const transaction::ObXATransID &get_xid() const { return xid_; }
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const common::ObAddr &get_sender() const { return sender_; }
  bool is_new_branch() const { return is_new_branch_; }
  bool is_tightly_coupled() const { return is_tightly_coupled_; }
  bool is_first_branch() const { return is_first_branch_; }
  int64_t get_timeout_seconds() const { return timeout_seconds_; }
  int64_t get_flags() const { return flags_; }
  bool is_valid() const;
  TO_STRING_KV(K_(tx_id), K_(xid), K_(sender), K_(is_new_branch), K_(is_tightly_coupled),
               K_(is_first_branch), K_(timeout_seconds), K_(flags));
private:
  transaction::ObTransID tx_id_;
  transaction::ObXATransID xid_;
  common::ObAddr sender_;
  bool is_new_branch_;
  // for tightly-coupled xa branches
  bool is_tightly_coupled_;
  bool is_first_branch_;
  int64_t timeout_seconds_;
  int64_t flags_;
};

class ObXAStartRPCResponse
{
  OB_UNIS_VERSION(1);
public:
  ObXAStartRPCResponse() {}
  ~ObXAStartRPCResponse() {}
  int init(const transaction::ObTransID &tx_id,
           transaction::ObTxDesc &tx_desc,
           const bool is_first_branch);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObTxInfo &get_tx_info() const { return tx_info_; }
  bool is_first_branch() const { return is_first_branch_; }
  bool is_valid() const { return tx_id_.is_valid() && tx_info_.is_valid(); }
  TO_STRING_KV(K_(tx_id), K_(tx_info), K_(is_first_branch));
private:
  transaction::ObTransID tx_id_;
  transaction::ObTxInfo tx_info_;
  bool is_first_branch_;
};

class ObXAEndRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXAEndRPCRequest() {}
  ~ObXAEndRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           transaction::ObTxDesc &tx_desc,
           const transaction::ObXATransID &xid,
           const bool is_tightly_coupled,
           const int64_t seq_no,
           const int64_t end_flag);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObTxStmtInfo &get_stmt_info() const { return stmt_info_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  bool is_tightly_coupled() const { return is_tightly_coupled_; }
  int64_t get_seq_no() const { return seq_no_; }
  int64_t get_end_flag() const { return end_flag_; }
  bool is_valid() const;
  TO_STRING_KV(K_(tx_id), K_(stmt_info), K_(xid), K_(is_tightly_coupled), K_(seq_no),
               K_(end_flag));
private:
  transaction::ObTransID tx_id_;
  transaction::ObTxStmtInfo stmt_info_;
  transaction::ObXATransID xid_;
  bool is_tightly_coupled_;
  int64_t seq_no_;
  int64_t end_flag_;
};

class ObXAStartStmtRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXAStartStmtRPCRequest() {}
  ~ObXAStartStmtRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           const transaction::ObXATransID &xid,
           const common::ObAddr &sender,
           const int64_t request_id);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  const common::ObAddr &get_sender() const { return sender_; }
  int64_t get_id() const { return request_id_; }
  bool is_valid() const;
  TO_STRING_KV(K_(tx_id), K_(xid), K_(sender), K_(request_id));
private:
  transaction::ObTransID tx_id_;
  transaction::ObXATransID xid_;
  common::ObAddr sender_;
  int64_t request_id_;
};

class ObXAStartStmtRPCResponse
{
  OB_UNIS_VERSION(1);
public:
  ObXAStartStmtRPCResponse() {}
  ~ObXAStartStmtRPCResponse() {}
  int init(const transaction::ObTransID &tx_id,
           transaction::ObTxDesc &tx_desc,
           const int64_t response_id);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObTxStmtInfo &get_stmt_info() const { return stmt_info_; }
  int64_t get_id() const { return response_id_; }
  bool is_valid() const { return tx_id_.is_valid() && stmt_info_.is_valid(); }
  TO_STRING_KV(K_(tx_id), K_(stmt_info), K_(response_id));
private:
  transaction::ObTransID tx_id_;
  transaction::ObTxStmtInfo stmt_info_;
  int64_t response_id_;
};

class ObXAEndStmtRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXAEndStmtRPCRequest() {}
  ~ObXAEndStmtRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           transaction::ObTxDesc &tx_desc,
           const transaction::ObXATransID &xid,
           const int64_t seq_no);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObTxStmtInfo &get_stmt_info() const { return stmt_info_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  int64_t get_seq_no() const { return seq_no_; }
  bool is_valid() const;
  TO_STRING_KV(K_(tx_id), K_(stmt_info), K_(xid), K_(seq_no));
private:
  transaction::ObTransID tx_id_;
  transaction::ObTxStmtInfo stmt_info_;
  transaction::ObXATransID xid_;
  int64_t seq_no_;
};

class ObXACommitRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXACommitRPCRequest() {}
  ~ObXACommitRPCRequest() {}
  int init(const transaction::ObTransID &trans_id,
           const transaction::ObXATransID &xid,
           const int64_t timeout_us,
           const int64_t request_id);
  bool is_valid() const { return trans_id_.is_valid(); }
  transaction::ObTransID get_trans_id() const { return trans_id_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  int64_t get_timeout_us() const { return timeout_us_; }
  int64_t get_id() const { return request_id_; }
  TO_STRING_KV(K_(trans_id), K_(xid), K_(timeout_us), K_(request_id));
private:
  transaction::ObTransID trans_id_;
  transaction::ObXATransID xid_;
  int64_t timeout_us_;
  int64_t request_id_;
};

class ObXAHbRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXAHbRequest() {}
  ~ObXAHbRequest() {}
  int init(const transaction::ObTransID &tx_id,
           const transaction::ObXATransID &xid,
           const common::ObAddr &sender);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  const ObAddr &get_sender() const { return sender_; }
  bool is_valid() const { return tx_id_.is_valid() && xid_.is_valid() && sender_.is_valid(); }
  TO_STRING_KV(K_(tx_id), K_(xid), K_(sender));
private:
  transaction::ObTransID tx_id_;
  transaction::ObXATransID xid_;
  common::ObAddr sender_;
};

class ObXARollbackRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXARollbackRPCRequest() : tx_id_(), xid_(), timeout_us_(-1), request_id_(-1)
  {}
  ~ObXARollbackRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           const transaction::ObXATransID &xid,
           const int64_t timeout_us,
           const int64_t request_id);
  const transaction::ObTransID &get_tx_id() const { return tx_id_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  int64_t get_timeout_us() const { return timeout_us_; }
  int64_t get_id() const { return request_id_; }
  bool is_valid() const { return tx_id_.is_valid() && xid_.is_valid() && 0 <= timeout_us_; }
  TO_STRING_KV(K_(tx_id), K_(xid), K_(timeout_us), K_(request_id));
private:
  transaction::ObTransID tx_id_;
  transaction::ObXATransID xid_;
  int64_t timeout_us_;
  int64_t request_id_;
};

class ObXATerminateRPCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObXATerminateRPCRequest() : tx_id_(), xid_(), timeout_us_(-1) {}
  ~ObXATerminateRPCRequest() {}
  int init(const transaction::ObTransID &tx_id,
           const transaction::ObXATransID &xid,
           const int64_t timeout_us);
  bool is_valid() const { return tx_id_.is_valid(); }
  transaction::ObTransID get_tx_id() const { return tx_id_; }
  const transaction::ObXATransID &get_xid() const { return xid_; }
  int64_t get_timeout_us() const { return timeout_us_; }
  TO_STRING_KV(K_(tx_id), K_(xid), K_(timeout_us));
private:
  transaction::ObTransID tx_id_;
  transaction::ObXATransID xid_;
  int64_t timeout_us_;
};

typedef ObXAHbRequest ObXAHbResponse;

class ObXACommitRpcResult
{
  OB_UNIS_VERSION(1);
public:
  ObXACommitRpcResult() { reset(); }
  virtual ~ObXACommitRpcResult() {}

  void init(const int status, const bool has_tx_level_temp_table)
  {
    status_ = status;
    has_tx_level_temp_table_ = has_tx_level_temp_table;
  }
  void reset()
  {
    status_ = OB_SUCCESS;
    has_tx_level_temp_table_ = false;
  }
  TO_STRING_KV(K_(status), K_(has_tx_level_temp_table));
public:
  int status_;
  bool has_tx_level_temp_table_;
};

class ObXARpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObXARpcProxy);
  RPC_AP(PR1 xa_prepare, OB_XA_PREPARE, (ObXAPrepareRPCRequest), Int64);
  RPC_AP(PR1 xa_start_req, OB_XA_START_REQ, (ObXAStartRPCRequest), Int64);
  RPC_AP(PR1 xa_start_resp, OB_XA_START_RESP, (ObXAStartRPCResponse), Int64);
  RPC_AP(PR1 xa_end_req, OB_XA_END_REQ, (ObXAEndRPCRequest), Int64);
  RPC_AP(PR1 xa_start_stmt_req, OB_XA_START_STMT_REQ, (ObXAStartStmtRPCRequest), Int64);
  RPC_AP(PR1 xa_start_stmt_resp, OB_XA_START_STMT_RESP, (ObXAStartStmtRPCResponse), Int64);
  RPC_AP(PR1 xa_end_stmt_req, OB_XA_END_STMT_REQ, (ObXAEndStmtRPCRequest), Int64);
  RPC_AP(PR1 xa_commit, OB_XA_COMMIT, (ObXACommitRPCRequest), ObXACommitRpcResult);
  RPC_AP(PR1 xa_hb_req, OB_XA_HB_REQ, (ObXAHbRequest), Int64);
  RPC_AP(PR1 xa_hb_resp, OB_XA_HB_RESP, (ObXAHbResponse), Int64);
  RPC_AP(PR1 xa_rollback, OB_XA_ROLLBACK, (ObXARollbackRPCRequest), Int64);
  RPC_AP(PR1 xa_terminate, OB_XA_TERMINATE, (ObXATerminateRPCRequest), Int64);
};

class ObXAPrepareP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_PREPARE>>
{
public:
  explicit ObXAPrepareP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAPrepareP);
};

class ObXAStartP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_START_REQ>>
{
public:
  explicit ObXAStartP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAStartP);
};

class ObXAStartResponseP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_START_RESP>>
{
public:
  explicit ObXAStartResponseP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAStartResponseP);
};

class ObXAEndP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_END_REQ>>
{
public:
  explicit ObXAEndP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAEndP);
};

class ObXACommitP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_COMMIT>>
{
public:
  explicit ObXACommitP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXACommitP);
};

class ObXAStartStmtP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_START_STMT_REQ>>
{
public:
  explicit ObXAStartStmtP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAStartStmtP);
};

class ObXAStartStmtResponseP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_START_STMT_RESP>>
{
public:
  explicit ObXAStartStmtResponseP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAStartStmtResponseP);
};

class ObXAEndStmtP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_END_STMT_REQ>>
{
public:
  explicit ObXAEndStmtP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAEndStmtP);
};

class ObXAHbReqP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_HB_REQ>>
{
public:
  explicit ObXAHbReqP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAHbReqP);
};

class ObXAHbRespP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_HB_RESP>>
{
public:
  explicit ObXAHbRespP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAHbRespP);
};

class ObXARollbackP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_ROLLBACK>>
{
public:
  explicit ObXARollbackP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXARollbackP);
};

class ObXATerminateP : public ObRpcProcessor<ObXARpcProxy::ObRpc<OB_XA_TERMINATE>>
{
public:
  explicit ObXATerminateP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObXATerminateP);
};

template <ObRpcPacketCode PC>
class ObXARPCCB : public ObXARpcProxy::AsyncCB<PC>
{
public:
  ObXARPCCB() : is_inited_(false), cond_(NULL), start_ts_(-1), tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObXARPCCB() {}
  int init(transaction::ObTransCond *cond)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObXARPCCB init twice", KR(ret));
    } else {
      cond_ = cond;
      start_ts_ = ObTimeUtility::current_time();
      tenant_id_ = MTL_ID();
      is_inited_ = true;
    }
    return ret;
  }
  void reset()
  {
    cond_ = NULL;
    start_ts_ = -1;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_inited_ = false;
  }
  void set_args(const typename ObXARpcProxy::AsyncCB<PC>::Request &args)
  {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof (*this));
    ObXARPCCB<PC> *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObXARPCCB<PC>();
      newcb->cond_ = cond_;
      newcb->start_ts_ = start_ts_;
      newcb->tenant_id_ = tenant_id_;
      newcb->is_inited_ = true;
    }
    return newcb;
  }
  int process()
  {
    const common::ObAddr &dst = ObXARpcProxy::AsyncCB<PC>::dst_;
    ObRpcResultCode &rcode = ObXARpcProxy::AsyncCB<PC>::rcode_;
    const Int64 result = ObXARpcProxy::AsyncCB<PC>::result_;
 
    if (OB_SUCCESS != rcode.rcode_) {
      TRANS_LOG_RET(WARN, rcode.rcode_, "xa rpc returns error code", K(rcode), K(dst));
      // TODO, check ret code
      if (OB_NOT_NULL(cond_)) {
        cond_->notify(rcode.rcode_);
      }
    } else {
      if (OB_NOT_NULL(cond_)) {
        cond_->notify(result);
      }
    }
    statistics();
    return OB_SUCCESS;
  }
  void on_timeout()
  {
    const common::ObAddr &dst = ObXARpcProxy::AsyncCB<PC>::dst_;
    const int real_ret = this->get_error();
    const int ret = ((EASY_OK == real_ret || EASY_TIMEOUT == real_ret)
                     ? common::OB_TIMEOUT : common::OB_RPC_CONNECT_ERROR);
    TRANS_LOG(WARN, "XA rpc timeout", K(ret), K(real_ret), K(dst), "lbt", lbt());
    if (OB_NOT_NULL(cond_)) {
      cond_->notify(ret);
    }
    statistics();
  }
  void statistics();
private:
  bool is_inited_;
  transaction::ObTransCond *cond_;
  int64_t start_ts_;
  uint64_t tenant_id_;
};

class ObXACommitRPCCB : public ObXARpcProxy::AsyncCB<OB_XA_COMMIT>
{
public:
  ObXACommitRPCCB() : is_inited_(false), cond_(NULL), has_tx_level_temp_table_(NULL),
                      start_ts_(-1), tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObXACommitRPCCB() {}
  int init(transaction::ObTransCond *cond, bool *has_tx_level_temp_table)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObXARPCCB init twice", KR(ret));
    } else {
      has_tx_level_temp_table_ = has_tx_level_temp_table;
      cond_ = cond;
      start_ts_ = ObTimeUtility::current_time();
      tenant_id_ = MTL_ID();
      is_inited_ = true;
    }
    return ret;
  }
  void reset()
  {
    has_tx_level_temp_table_ = NULL;
    cond_ = NULL;
    start_ts_ = -1;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_inited_ = false;
  }
  void set_args(const typename ObXARpcProxy::AsyncCB<OB_XA_COMMIT>::Request &args)
  {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof (*this));
    ObXACommitRPCCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObXACommitRPCCB();
      newcb->has_tx_level_temp_table_ = has_tx_level_temp_table_;
      newcb->cond_ = cond_;
      newcb->start_ts_ = start_ts_;
      newcb->tenant_id_ = tenant_id_;
      newcb->is_inited_ = true;
    }
    return newcb;
  }
  int process()
  {
    const common::ObAddr &dst = ObXARpcProxy::AsyncCB<OB_XA_COMMIT>::dst_;
    ObRpcResultCode &rcode = ObXARpcProxy::AsyncCB<OB_XA_COMMIT>::rcode_;
    const ObXACommitRpcResult result = ObXARpcProxy::AsyncCB<OB_XA_COMMIT>::result_;
 
    if (OB_NOT_NULL(has_tx_level_temp_table_)) {
      *has_tx_level_temp_table_ = result.has_tx_level_temp_table_;
    }
    if (OB_SUCCESS != rcode.rcode_) {
      TRANS_LOG_RET(WARN, rcode.rcode_, "xa rpc returns error code", K(rcode), K(dst));
      // TODO, check ret code
      if (OB_NOT_NULL(cond_)) {
        cond_->notify(rcode.rcode_);
      }
    } else {
      if (OB_NOT_NULL(cond_)) {
        cond_->notify(result.status_);
      }
    }
    statistics();
    return OB_SUCCESS;
  }
  void on_timeout()
  {
    const common::ObAddr &dst = ObXARpcProxy::AsyncCB<OB_XA_COMMIT>::dst_;
    const int real_ret = this->get_error();
    const int ret = ((EASY_OK == real_ret || EASY_TIMEOUT == real_ret)
                     ? common::OB_TIMEOUT : common::OB_RPC_CONNECT_ERROR);
    TRANS_LOG(WARN, "XA rpc timeout", K(ret), K(real_ret), K(dst), "lbt", lbt());
    if (OB_NOT_NULL(cond_)) {
      cond_->notify(ret);
    }
    statistics();
  }
  void statistics();
private:
  bool is_inited_;
  transaction::ObTransCond *cond_;
  bool *has_tx_level_temp_table_;
  int64_t start_ts_;
  uint64_t tenant_id_;
};

}//obrpc

using namespace obrpc;

namespace transaction
{

class ObIXARpc
{
public:
  ObIXARpc() {}
  virtual ~ObIXARpc() {}
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;
public:
  virtual int xa_prepare(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAPrepareRPCRequest &req, ObXARPCCB<OB_XA_PREPARE> &cb) = 0;
  virtual int xa_start(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAStartRPCRequest &req, ObXARPCCB<OB_XA_START_REQ> *cb) = 0;
  virtual int xa_start_response(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAStartRPCResponse &req, ObXARPCCB<OB_XA_START_RESP> *cb) = 0;
  virtual int xa_end(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAEndRPCRequest &req, ObXARPCCB<OB_XA_END_REQ> *cb) = 0;
  virtual int xa_start_stmt(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAStartStmtRPCRequest &req, ObXARPCCB<OB_XA_START_STMT_REQ> *cb) = 0;
  virtual int xa_start_stmt_response(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAStartStmtRPCResponse &req, ObXARPCCB<OB_XA_START_STMT_RESP> *cb) = 0;
  virtual int xa_end_stmt(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAEndStmtRPCRequest &req, ObXARPCCB<OB_XA_END_STMT_REQ> *cb) = 0;
  virtual int xa_commit(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXACommitRPCRequest &req, ObXACommitRPCCB &cb) = 0;
  virtual int xa_hb_req(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAHbRequest &req, ObXARPCCB<OB_XA_HB_REQ> *cb) = 0;
  virtual int xa_hb_resp(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAHbResponse &resp, ObXARPCCB<OB_XA_HB_RESP> *cb) = 0;
  virtual int xa_rollback(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXARollbackRPCRequest &req, ObXARPCCB<OB_XA_ROLLBACK> *cb) = 0;
  virtual int xa_terminate(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXATerminateRPCRequest &req, ObXARPCCB<OB_XA_TERMINATE> *cb) = 0;
};

class ObXARpc : public ObIXARpc
{
public:
  ObXARpc() : is_inited_(false), is_running_(false), rpc_proxy_(NULL) {}
  ~ObXARpc() { destroy(); }
  int init(ObXARpcProxy *proxy, const common::ObAddr &self);
  int start();
  int stop();
  int wait();
  void destroy();
public:
  int xa_prepare(const uint64_t tenant_id, const common::ObAddr &server,
                 const ObXAPrepareRPCRequest &req, ObXARPCCB<OB_XA_PREPARE> &cb);
  int xa_start(const uint64_t tenant_id, const common::ObAddr &server,
               const ObXAStartRPCRequest &req, ObXARPCCB<OB_XA_START_REQ> *cb);
  int xa_start_response(const uint64_t tenant_id, const common::ObAddr &server,
                        const ObXAStartRPCResponse &req, ObXARPCCB<OB_XA_START_RESP> *cb);
  int xa_end(const uint64_t tenant_id, const common::ObAddr &server,
             const ObXAEndRPCRequest &req, ObXARPCCB<OB_XA_END_REQ> *cb);
  int xa_start_stmt(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAStartStmtRPCRequest &req, ObXARPCCB<OB_XA_START_STMT_REQ> *cb);
  int xa_start_stmt_response(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAStartStmtRPCResponse &req, ObXARPCCB<OB_XA_START_STMT_RESP> *cb);
  int xa_end_stmt(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAEndStmtRPCRequest &req, ObXARPCCB<OB_XA_END_STMT_REQ> *cb);
  int xa_commit(const uint64_t tenant_id, const common::ObAddr &server,
                   const ObXACommitRPCRequest &req, ObXACommitRPCCB &cb);
  int xa_hb_req(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAHbRequest &req, ObXARPCCB<OB_XA_HB_REQ> *cb);
  int xa_hb_resp(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXAHbResponse &resp, ObXARPCCB<OB_XA_HB_RESP> *cb);
  int xa_rollback(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXARollbackRPCRequest &req, ObXARPCCB<OB_XA_ROLLBACK> *cb);
  int xa_terminate(const uint64_t tenant_id, const common::ObAddr &server,
                  const ObXATerminateRPCRequest &req, ObXARPCCB<OB_XA_TERMINATE> *cb);
private:
  bool is_inited_;
  bool is_running_;
  ObXARpcProxy *rpc_proxy_;
  common::ObAddr self_;
};

}//transaction

}//oceanbase


#endif
