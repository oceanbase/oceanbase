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
#include "ob_trans_define.h"
#include "ob_trans_factory.h"
#include "ob_tx_msg.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "ob_tx_free_route_msg.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}

namespace transaction
{
class ObTransService;
class ObTxMsg;
}

namespace obrpc
{
class ObTransRpcResult
{
  OB_UNIS_VERSION(1);
public:
  ObTransRpcResult()
  { reset(); }
  virtual ~ObTransRpcResult() {}

  void init(const int status, const int64_t timestamp);
  int get_status() const { return status_; }
  int64_t get_timestamp() const { return send_timestamp_; }
  void reset();
  TO_STRING_KV(K_(status), K_(send_timestamp), K_(private_data));
private:
  int status_;
  int64_t send_timestamp_;
public:
  // for ObTxCommitReqMsg, it is commit version
  share::SCN private_data_;
};

struct ObTxRpcRollbackSPResult
{
  OB_UNIS_VERSION(1);
public:
  ObTxRpcRollbackSPResult(): ignore_(false) {}
  int status_;
  int64_t send_timestamp_;
  int64_t born_epoch_;
  ObAddr addr_;
  // rollback response has changed to use ObTxRollbackSPRespMsg
  // use this field to indicate handler ignore handle by this msg
  bool ignore_;
  ObSEArray<transaction::ObTxLSEpochPair, 1> downstream_parts_;
public:
  int get_status() const { return status_; }
  TO_STRING_KV(K_(status), K_(send_timestamp), K_(born_epoch), K_(addr), K_(ignore), K_(downstream_parts));
};

class ObTransRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObTransRpcProxy);

  RPC_AP(PR3 post_commit_msg, OB_TX_COMMIT, (transaction::ObTxCommitMsg), ObTransRpcResult);
  RPC_AP(PR3 post_commit_resp_msg, OB_TX_COMMIT_RESP, (transaction::ObTxCommitRespMsg), ObTransRpcResult);
  RPC_AP(PR3 post_abort_msg, OB_TX_ABORT, (transaction::ObTxAbortMsg), ObTransRpcResult);
  RPC_AP(PR3 post_rollback_sp_msg, OB_TX_ROLLBACK_SAVEPOINT, (transaction::ObTxRollbackSPMsg), ObTxRpcRollbackSPResult);
  RPC_AP(PR3 post_rollback_sp_resp_msg, OB_TX_ROLLBACK_SAVEPOINT_RESP, (transaction::ObTxRollbackSPRespMsg), ObTransRpcResult);
  RPC_AP(PR3 post_keep_alive_msg, OB_TX_KEEPALIVE, (transaction::ObTxKeepaliveMsg), ObTransRpcResult);
  RPC_AP(PR3 post_keep_alive_resp_msg, OB_TX_KEEPALIVE_RESP, (transaction::ObTxKeepaliveRespMsg), ObTransRpcResult);
  // for standby
  RPC_AP(PR3 post_ask_state_msg, OB_TX_ASK_STATE, (transaction::ObAskStateMsg), ObTransRpcResult);
  RPC_AP(PR3 post_ask_state_resp_msg, OB_TX_ASK_STATE_RESP, (transaction::ObAskStateRespMsg), ObTransRpcResult);
  RPC_AP(PR3 post_collect_state_msg, OB_TX_COLLECT_STATE, (transaction::ObCollectStateMsg), ObTransRpcResult);
  RPC_AP(PR3 post_collect_state_resp_msg, OB_TX_COLLECT_STATE_RESP, (transaction::ObCollectStateRespMsg), ObTransRpcResult);
  // xa
  RPC_AP(PR3 post_sub_prepare_msg, OB_TX_SUB_PREPARE, (transaction::ObTxSubPrepareMsg), ObTransRpcResult);
  RPC_AP(PR3 post_sub_prepare_resp_msg, OB_TX_SUB_PREPARE_RESP, (transaction::ObTxSubPrepareRespMsg), ObTransRpcResult);
  RPC_AP(PR3 post_sub_commit_msg, OB_TX_SUB_COMMIT, (transaction::ObTxSubCommitMsg), ObTransRpcResult);
  RPC_AP(PR3 post_sub_commit_resp_msg, OB_TX_SUB_COMMIT_RESP, (transaction::ObTxSubCommitRespMsg), ObTransRpcResult);
  RPC_AP(PR3 post_sub_rollback_msg, OB_TX_SUB_ROLLBACK, (transaction::ObTxSubRollbackMsg), ObTransRpcResult);
  RPC_AP(PR3 post_sub_rollback_resp_msg, OB_TX_SUB_ROLLBACK_RESP, (transaction::ObTxSubRollbackRespMsg), ObTransRpcResult);
  // txn free route
  RPC_AP(PR3 post_msg, OB_TX_FREE_ROUTE_CHECK_ALIVE, (transaction::ObTxFreeRouteCheckAliveMsg), ObTransRpcResult);
  RPC_AP(PR3 post_msg, OB_TX_FREE_ROUTE_CHECK_ALIVE_RESP, (transaction::ObTxFreeRouteCheckAliveRespMsg), ObTransRpcResult);
  RPC_S(@PR3 sync_access, OB_TX_FREE_ROUTE_PUSH_STATE, (transaction::ObTxFreeRoutePushState), transaction::ObTxFreeRoutePushStateResp);
  // state check for 4377
  RPC_S(@PR3 ask_tx_state_for_4377, OB_ASK_TX_STATE_FOR_4377, (transaction::ObAskTxStateFor4377Msg), transaction::ObAskTxStateFor4377RespMsg);
};

#define TX_P_(name, pcode)                                              \
class ObTx##name##P : public ObRpcProcessor< ObTransRpcProxy::ObRpc<pcode> > \
{ \
public: \
  ObTx##name##P() {} \
protected: \
  int process(); \
private: \
  DISALLOW_COPY_AND_ASSIGN(ObTx##name##P); \
}

TX_P_(Commit, OB_TX_COMMIT);
TX_P_(CommitResp, OB_TX_COMMIT_RESP);
TX_P_(Abort, OB_TX_ABORT);
TX_P_(RollbackSP, OB_TX_ROLLBACK_SAVEPOINT);
TX_P_(RollbackSPResp, OB_TX_ROLLBACK_SAVEPOINT_RESP);
TX_P_(Keepalive, OB_TX_KEEPALIVE);
TX_P_(KeepaliveResp, OB_TX_KEEPALIVE_RESP);
//for standby
TX_P_(AskState, OB_TX_ASK_STATE);
TX_P_(AskStateResp, OB_TX_ASK_STATE_RESP);
TX_P_(CollectState, OB_TX_COLLECT_STATE);
TX_P_(CollectStateResp, OB_TX_COLLECT_STATE_RESP);
// for xa
TX_P_(SubPrepare, OB_TX_SUB_PREPARE);
TX_P_(SubPrepareResp, OB_TX_SUB_PREPARE_RESP);
TX_P_(SubCommit, OB_TX_SUB_COMMIT);
TX_P_(SubCommitResp, OB_TX_SUB_COMMIT_RESP);
TX_P_(SubRollback, OB_TX_SUB_ROLLBACK);
TX_P_(SubRollbackResp, OB_TX_SUB_ROLLBACK_RESP);

template<ObRpcPacketCode PC>
class ObTxRPCCB : public ObTransRpcProxy::AsyncCB<PC>
{
public:
  ObTxRPCCB() : is_inited_(false),
                msg_type_(transaction::TX_UNKNOWN),
                tenant_id_(0),
                request_id_(common::OB_INVALID_TIMESTAMP) {}
  ~ObTxRPCCB() {}
  void set_args(const typename ObTransRpcProxy::AsyncCB<PC>::Request &args)
  {
    sender_ls_id_ = args.get_sender();
    receiver_ls_id_ = args.get_receiver();
    epoch_ = args.get_epoch();
    trans_id_ = args.get_trans_id();
    msg_type_ = args.get_msg_type();
    tenant_id_ = args.get_tenant_id();
    request_id_ = args.get_request_id();
  }
  int init();
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const {
    ObTxRPCCB<PC> *newcb = NULL;
    void *buf = alloc(sizeof (*this));
    if (NULL != buf) {
      newcb = new (buf) ObTxRPCCB<PC>();
      if (newcb) {
        newcb->is_inited_ = is_inited_;
        newcb->sender_ls_id_ = sender_ls_id_;
        newcb->receiver_ls_id_ = receiver_ls_id_;
        newcb->epoch_ = epoch_;
        newcb->trans_id_ = trans_id_;
        newcb->msg_type_ = msg_type_;
        newcb->tenant_id_ = tenant_id_;
        newcb->request_id_ = request_id_;
      }
    }
    return newcb;
  }

public:
  int process();
  void on_timeout();
private:
  int handle_tx_msg_cb_(const int status, const ObAddr &dst);
private:
  bool is_inited_;
  share::ObLSID sender_ls_id_;
  share::ObLSID receiver_ls_id_;
  int64_t epoch_;
  transaction::ObTransID trans_id_;
  int16_t msg_type_;
  uint64_t tenant_id_;
  int64_t request_id_;
};

template<ObRpcPacketCode PC>
int ObTxRPCCB<PC>::init()
{
  int ret = common::OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTxRPCCB inited twice");
    ret = common::OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }

  return ret;
}

// publich method
bool need_refresh_location_cache_(const int status);
int refresh_location_cache(const share::ObLSID ls);
int handle_trans_msg_callback(const share::ObLSID &sender_ls_id,
                              const share::ObLSID &receiver_ls_id,
                              const transaction::ObTransID &tx_id,
                              const int16_t msg_type,
                              const int status,
                              const ObAddr &receiver_addr,
                              const int64_t request_id,
                              const share::SCN &private_data);

int handle_sp_rollback_resp(const share::ObLSID &receiver_ls_id,
                            const int64_t epoch,
                            const transaction::ObTransID &tx_id,
                            const int status,
                            const int64_t request_id,
                            const ObTxRpcRollbackSPResult &result);
template<ObRpcPacketCode PC>
int ObTxRPCCB<PC>::process()
{
  int ret = common::OB_SUCCESS;
  const typename ObTransRpcProxy::AsyncCB<PC>::Response &result =
    ObTransRpcProxy::AsyncCB<PC>::result_;
  const ObAddr &dst = ObTransRpcProxy::AsyncCB<PC>::dst_;
  ObRpcResultCode &rcode = ObTransRpcProxy::AsyncCB<PC>::rcode_;
  const int64_t LOG_INTERVAL_US = 1 * 1000 * 1000;
  bool need_refresh = false;
  int status = common::OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTxRPCCB not inited");
    ret = common::OB_NOT_INIT;
  } else if (!is_valid_tenant_id(tenant_id_)) {
      TRANS_LOG(WARN, "tenant_id is invalid", K_(tenant_id), K(dst));
      ret = OB_ERR_UNEXPECTED;
  } else {
    MTL_SWITCH(tenant_id_) {
      if (OB_SUCCESS != rcode.rcode_) {
        status = rcode.rcode_;
        TRANS_LOG(WARN, "transaction rpc error", K(rcode),
                  K_(sender_ls_id), K_(receiver_ls_id), K_(trans_id), K(dst), K_(msg_type));
      } else {
        status = result.get_status();
      }
      if (status != OB_SUCCESS
          && !receiver_ls_id_.is_scheduler_ls()
          && receiver_ls_id_.is_valid()
          && need_refresh_location_cache_(status)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(refresh_location_cache(receiver_ls_id_))) {
          TRANS_LOG(WARN, "refresh location cache error", KR(ret),
                    K_(trans_id), "ls", receiver_ls_id_, K(result), K(dst), K(status), K_(msg_type));
        } else if (REACH_TIME_INTERVAL(LOG_INTERVAL_US)) {
          TRANS_LOG(INFO, "refresh location cache", K_(trans_id),
                    K_(sender_ls_id), K_(receiver_ls_id), K(dst), K_(msg_type), K(status));
        } else {
          TRANS_LOG(DEBUG, "refresh location cache", K_(trans_id),
                    K_(sender_ls_id), K_(receiver_ls_id), K(dst), K_(msg_type), K(status));
        }
      }
      if (OB_FAIL(handle_tx_msg_cb_(status, dst))) {
        TRANS_LOG(WARN, "handle transaction msg callback error", K(ret),
                  K_(trans_id), K_(sender_ls_id), K_(receiver_ls_id),
                  K_(msg_type), "rcode", status, K(dst));
      }
    }
  }
  if (OB_SUCCESS != ret || (OB_SUCCESS != status && status != -1 && status != OB_NEED_RETRY && status != OB_EAGAIN)) {
    TRANS_LOG(WARN, "trx rpc callback", K(ret), K(status), K(dst), K(result));
  }
  return ret;
}

template<ObRpcPacketCode PC>
inline int ObTxRPCCB<PC>::handle_tx_msg_cb_(const int status, const ObAddr &dst)
{
  const typename ObTransRpcProxy::AsyncCB<PC>::Response &result =
    ObTransRpcProxy::AsyncCB<PC>::result_;
  return handle_trans_msg_callback(sender_ls_id_,
                                   receiver_ls_id_,
                                   trans_id_,
                                   msg_type_,
                                   status,
                                   dst,
                                   request_id_,
                                   result.private_data_);
}

int handle_sp_rollback_resp(const share::ObLSID &receiver_ls_id,
                            const int64_t epoch,
                            const transaction::ObTransID &tx_id,
                            const int status,
                            const int64_t request_id,
                            const ObTxRpcRollbackSPResult &result);
template<>
inline int ObTxRPCCB<OB_TX_ROLLBACK_SAVEPOINT>::handle_tx_msg_cb_(const int status, const ObAddr &dst)
{
  return handle_sp_rollback_resp(receiver_ls_id_,
                                 epoch_,
                                 trans_id_,
                                 status,
                                 request_id_,
                                 result_);
}

template<ObRpcPacketCode PC>
void ObTxRPCCB<PC>::on_timeout()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObAddr &dst = ObTransRpcProxy::AsyncCB<PC>::dst_;
  int status = common::OB_TRANS_RPC_TIMEOUT;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTxRPCCB not inited");
  } else if (!is_valid_tenant_id(tenant_id_)) {
    TRANS_LOG(WARN, "tenant_id is invalid", K_(tenant_id), K(dst));
    ret = OB_ERR_UNEXPECTED;
  } else {
    MTL_SWITCH(tenant_id_) {
      if (transaction::ObTxMsgTypeChecker::is_2pc_msg_type(msg_type_)) {
        // do nothing
      } else {
        if (receiver_ls_id_.is_scheduler_ls()) {
        } else if (receiver_ls_id_.is_valid()) {
          if (OB_FAIL(refresh_location_cache(receiver_ls_id_))) {
            TRANS_LOG(WARN, "refresh location cache error", KR(ret), K_(trans_id), K_(receiver_ls_id), K(dst), K_(tenant_id));
          } else {
            TRANS_LOG(DEBUG, "refresh location cache", K_(trans_id), K_(receiver_ls_id), K(dst));
          }
          if (OB_SUCCESS != (tmp_ret = handle_tx_msg_cb_(status, dst))) {
            TRANS_LOG(WARN, "notify start stmt", "ret", tmp_ret, K_(trans_id), K_(msg_type), K_(receiver_ls_id), K(dst));
            ret = tmp_ret;
          }
        } else {
          TRANS_LOG(WARN, "receiver is invalid", K_(trans_id), K_(receiver_ls_id), K(dst));
        }
        TRANS_LOG(WARN, "transaction rpc timeout", K(tmp_ret),
                  K_(sender_ls_id), K_(receiver_ls_id),
                  K_(trans_id), K_(msg_type), K(dst), K(status));
      }
    }
  }
}
} // obrpc

#include "ob_tx_free_route_rpc.h"

namespace transaction
{

class ObITransRpc
{
public:
  ObITransRpc() {}
  virtual ~ObITransRpc() {}
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
public:
  virtual int post_msg(const ObAddr &server, ObTxMsg &msg) = 0;
  virtual int post_msg(const share::ObLSID &p, ObTxMsg &msg) = 0;
  virtual int post_msg(const ObAddr &server, const ObTxFreeRouteMsg &m) = 0;
  virtual int sync_access(const ObAddr &server, const ObTxFreeRoutePushState &m, ObTxFreeRoutePushStateResp &result) = 0;
  virtual int ask_tx_state_for_4377(const ObAskTxStateFor4377Msg &msg,
                                    ObAskTxStateFor4377RespMsg &resp) = 0;

};

/*
 * adaptive batching msg rpc class
 *
 * for performance reason, this class will try batching msg
 * it accomplish this by send msg to a system provoided `batch_rpc_` instance
 *
 * not all message will send by batch, for instantly reason, some msg will be
 * sent out immediately via ObTransRpc instance: `trx_rpc_`
 */
class ObTransRpc : public ObITransRpc
{
public:
  ObTransRpc() : is_inited_(false),
                 is_running_(false),
                 trans_service_(NULL),
                 rpc_proxy_(),
                 batch_rpc_(NULL),
                 total_trans_msg_count_(0),
                 total_batch_msg_count_(0),
                 last_stat_ts_(0) {}
  ~ObTransRpc() { destroy(); }
  int init(ObTransService *trans_service,
           rpc::frame::ObReqTransport *req_transport,
           const common::ObAddr &self,
           obrpc::ObBatchRpc *batch_rpc);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  int post_msg(const ObAddr &server, ObTxMsg &msg);
  int post_msg(const share::ObLSID &p, ObTxMsg &msg);
  int post_msg(const ObAddr &server, const ObTxFreeRouteMsg &m);
  int sync_access(const ObAddr &server, const ObTxFreeRoutePushState &m, ObTxFreeRoutePushStateResp &result);
  int ask_tx_state_for_4377(const ObAskTxStateFor4377Msg &msg,
                            ObAskTxStateFor4377RespMsg &resp);
  private:
  int post_(const ObAddr &server, ObTxMsg &msg);
  int post_commit_msg_(const ObAddr &server, ObTxMsg &msg);
  int post_sub_request_msg_(const ObAddr &server, ObTxMsg &msg);
  int post_sub_response_msg_(const ObAddr &server, ObTxMsg &msg);
  int post_standby_msg_(const ObAddr &server, ObTxMsg &msg);
  void statistics_();
private:
  static const int64_t STAT_INTERVAL = 1 * 1000 * 1000;
  bool is_inited_;
  bool is_running_;
  // common info
  int64_t tenant_id_;
  ObTransService *trans_service_;
  obrpc::ObTransRpcProxy rpc_proxy_;
  obrpc::ObBatchRpc* batch_rpc_; // used to send msg by batch
  // callback info
  obrpc::ObTxRPCCB<obrpc::OB_TX_COMMIT> tx_commit_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_COMMIT_RESP> tx_commit_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_ABORT> tx_abort_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_ROLLBACK_SAVEPOINT> tx_rollback_sp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_ROLLBACK_SAVEPOINT_RESP> tx_rollback_sp_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_KEEPALIVE> tx_keepalive_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_KEEPALIVE_RESP> tx_keepalive_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_ASK_STATE> tx_ask_state_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_ASK_STATE_RESP> tx_ask_state_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_COLLECT_STATE> tx_collect_state_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_COLLECT_STATE_RESP> tx_collect_state_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_SUB_PREPARE> tx_sub_prepare_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_SUB_PREPARE_RESP> tx_sub_prepare_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_SUB_COMMIT> tx_sub_commit_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_SUB_COMMIT_RESP> tx_sub_commit_resp_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_SUB_ROLLBACK> tx_sub_rollback_cb_;
  obrpc::ObTxRPCCB<obrpc::OB_TX_SUB_ROLLBACK_RESP> tx_sub_rollback_resp_cb_;
  obrpc::ObTxFreeRouteRPCCB<obrpc::OB_TX_FREE_ROUTE_CHECK_ALIVE> tx_free_route_ck_alive_cb_;
  obrpc::ObTxFreeRouteRPCCB<obrpc::OB_TX_FREE_ROUTE_CHECK_ALIVE_RESP> tx_free_route_ck_alive_resp_cb_;
  // statistic info
  int64_t total_trans_msg_count_ CACHE_ALIGNED;
  int64_t total_batch_msg_count_ CACHE_ALIGNED;
  int64_t last_stat_ts_ CACHE_ALIGNED;
};

class ObAskTxStateFor4377P : public obrpc::ObRpcProcessor< obrpc::ObTransRpcProxy::ObRpc<obrpc::OB_ASK_TX_STATE_FOR_4377> >
{
protected:
  int process();
};

} // transaction

} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_RPC_
