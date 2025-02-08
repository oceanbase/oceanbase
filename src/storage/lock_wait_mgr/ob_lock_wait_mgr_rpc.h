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

#ifndef OCEANBASE_LOCKWAITMGR_OB_LOCK_WAIT_MGR_RPC_
#define OCEANBASE_LOCKWAITMGR_OB_LOCK_WAIT_MGR_RPC_

#include "lib/utility/ob_unify_serialize.h"     // OB_UNIS_VERSION
#include "lib/net/ob_addr.h"                    // ObAddr
#include "rpc/obrpc/ob_rpc_proxy.h"             // ObRpcProxy
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "share/rpc/ob_batch_proxy.h"
#include "share/rpc/ob_batch_rpc.h"
#include "ob_lock_wait_mgr_msg.h"

namespace oceanbase {

namespace obrpc
{
class ObLockWaitMgrRpcResult
{
  OB_UNIS_VERSION(1);
public:
  virtual ~ObLockWaitMgrRpcResult() {}
  void init(int status) {
    status_ = status;
  }
  int get_status() const { return status_; }
  TO_STRING_KV(K_(status));
private:
  int status_;
};

class ObLockWaitMgrRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObLockWaitMgrRpcProxy);

  RPC_AP(PR3 inform_dst_enqueue, OB_LOCK_WAIT_MGR_DST_ENQUEUE, (lockwaitmgr::ObLockWaitMgrDstEnqueueMsg), ObLockWaitMgrRpcResult);
  RPC_AP(PR3 dst_enqueue_resp, OB_LOCK_WAIT_MGR_DST_ENQUEUE_RESP, (lockwaitmgr::ObLockWaitMgrDstEnqueueRespMsg), ObLockWaitMgrRpcResult);
  RPC_AP(PR3 inform_lock_release, OB_LOCK_WAIT_MGR_LOCK_RELEASE, (lockwaitmgr::ObLockWaitMgrLockReleaseMsg), ObLockWaitMgrRpcResult);
  RPC_AP(PR3 wake_up_remote, OB_LOCK_WAIT_MGR_WAKE_UP_REMOTE, (lockwaitmgr::ObLockWaitMgrWakeUpRemoteMsg), ObLockWaitMgrRpcResult);
};

#define LOCK_WAIT_MGR_P_(name, pcode)                                              \
class ObLockWaitMgr##name##P : public ObRpcProcessor< ObLockWaitMgrRpcProxy::ObRpc<pcode> > \
{ \
public: \
  ObLockWaitMgr##name##P() {} \
protected: \
  int process(); \
private: \
  DISALLOW_COPY_AND_ASSIGN(ObLockWaitMgr##name##P); \
}

LOCK_WAIT_MGR_P_(DstEnqueue, OB_LOCK_WAIT_MGR_DST_ENQUEUE);
LOCK_WAIT_MGR_P_(DstEnqueueResp, OB_LOCK_WAIT_MGR_DST_ENQUEUE_RESP);
LOCK_WAIT_MGR_P_(LockRelease, OB_LOCK_WAIT_MGR_LOCK_RELEASE);
LOCK_WAIT_MGR_P_(WakeUp, OB_LOCK_WAIT_MGR_WAKE_UP_REMOTE);

template<ObRpcPacketCode PC>
class ObLockWaitMgrRpcCB : public ObLockWaitMgrRpcProxy::AsyncCB<PC>
{
public:
  void set_args(const typename ObLockWaitMgrRpcProxy::AsyncCB<PC>::Request &args)
  {
    msg_type_ = args.get_msg_type();
    tenant_id_ = args.get_tenant_id();
    hash_ = args.get_hash();
    node_id_ = args.get_node_id();
    sender_addr_ = args.get_sender_addr();
  }
  int init() {}
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(const oceanbase::rpc::frame::SPAlloc &alloc) const {
    ObLockWaitMgrRpcCB<PC> *newcb = NULL;
    void *buf = alloc(sizeof (*this));
    if (NULL != buf) {
      newcb = new (buf) ObLockWaitMgrRpcCB<PC>();
      if (newcb) {
        newcb->msg_type_ = msg_type_;
        newcb->tenant_id_ = tenant_id_;
        newcb->hash_ = hash_;
        newcb->node_id_ = node_id_;
        newcb->sender_addr_ = sender_addr_;
      }
    }
    return newcb;
  }
  int process();
  void on_timeout();
private:
  int handle_lock_wait_mgr_msg_cb_(const int status, const ObAddr &dst);
private:
  int16_t msg_type_;
  uint64_t tenant_id_;
  uint64_t hash_;
  rpc::NodeID node_id_;
  common::ObAddr sender_addr_;
};

// public method
int handle_lock_wait_mgr_msg_cb(int status,
                                int16_t msg_type,
                                uint64_t hash,
                                rpc::NodeID node_id,
                                const common::ObAddr &receiver_addr);

template<ObRpcPacketCode PC>
inline int ObLockWaitMgrRpcCB<PC>::handle_lock_wait_mgr_msg_cb_(const int status, const ObAddr &dst)
{
  return handle_lock_wait_mgr_msg_cb(status,
                                     msg_type_,
                                     hash_,
                                     node_id_,
                                     dst);
}

ERRSIM_POINT_DEF(ERRSIM_LOCK_WAIT_MGR_DST_ENQUEUE_RPC_TIMEOUT);
ERRSIM_POINT_DEF(ERRSIM_LOCK_WAIT_MGR_LOCK_RELEASE_RPC_TIMEOUT);

template<ObRpcPacketCode PC>
int ObLockWaitMgrRpcCB<PC>::process()
{
  #define PRINT_WRAPPER K(rcode), K(dst), K_(msg_type), K_(tenant_id), K_(hash), K_(node_id), K_(sender_addr)
  int ret = OB_SUCCESS;
  const typename ObLockWaitMgrRpcProxy::AsyncCB<PC>::Response &result =
    ObLockWaitMgrRpcProxy::AsyncCB<PC>::result_;
  const ObAddr &dst = ObLockWaitMgrRpcProxy::AsyncCB<PC>::dst_;
  ObRpcResultCode &rcode = ObLockWaitMgrRpcProxy::AsyncCB<PC>::rcode_;
  int status = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if ((OB_TMP_FAIL(ERRSIM_LOCK_WAIT_MGR_DST_ENQUEUE_RPC_TIMEOUT) && msg_type_ == lockwaitmgr::LWM_DST_ENQUEUE)
   || (OB_TMP_FAIL(ERRSIM_LOCK_WAIT_MGR_LOCK_RELEASE_RPC_TIMEOUT) && msg_type_ == lockwaitmgr::LWM_LOCK_RELEASE)) {
    status = OB_TIMEOUT;
    on_timeout();
  } else if (!is_valid_tenant_id(tenant_id_)) {
      TRANS_LOG(WARN, "tenant_id is invalid", PRINT_WRAPPER);
      ret = OB_ERR_UNEXPECTED;
  } else {
    MTL_SWITCH(tenant_id_) {
      if (OB_SUCCESS != rcode.rcode_) {
        status = rcode.rcode_;
        TRANS_LOG(WARN, "transaction rpc error", PRINT_WRAPPER);
      } else {
        status = result.get_status();
      }
      if (OB_FAIL(handle_lock_wait_mgr_msg_cb_(status, dst))) {
        TRANS_LOG(WARN, "handle transaction msg callback error", PRINT_WRAPPER);
      }
    }
  }
  TRANS_LOG(TRACE, "lock wait mgr rpc callback", PRINT_WRAPPER);
  return OB_SUCCESS;
  #undef PRINT_WRAPPER
}

template<ObRpcPacketCode PC>
void ObLockWaitMgrRpcCB<PC>::on_timeout()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObAddr &dst = ObLockWaitMgrRpcProxy::AsyncCB<PC>::dst_;
  int status = OB_TIMEOUT;
  if (!is_valid_tenant_id(tenant_id_)) {
    TRANS_LOG(WARN, "tenant_id is invalid", K_(tenant_id), K(dst));
    ret = OB_ERR_UNEXPECTED;
  } else {
    MTL_SWITCH(tenant_id_) {
      if (OB_SUCCESS != (tmp_ret = handle_lock_wait_mgr_msg_cb_(status, dst))) {
        TRANS_LOG(WARN, "transaction rpc error",
                  K_(msg_type), K_(tenant_id), K_(hash), K_(node_id), K_(sender_addr));
        ret = tmp_ret;
      }
    }
  }
  TRANS_LOG_RET(WARN, OB_TIMEOUT, "lock wait mgr rpc timeout",
                K_(msg_type), K_(tenant_id), K_(hash), K_(node_id), K_(sender_addr));
}

} // obrpc

namespace lockwaitmgr
{
class ObILockWaitMgrRpc
{
public:
  ObILockWaitMgrRpc() {}
  virtual ~ObILockWaitMgrRpc() {}
  virtual int post_msg(const ObAddr &server, ObLockWaitMgrMsg &msg) = 0;
  virtual int init(const ObLockWaitMgr *lock_wait_mgr) { return OB_SUCCESS; }
};

class ObLockWaitMgrRpc : public ObILockWaitMgrRpc
{
public:
  ObLockWaitMgrRpc() : is_inited_(false),
                       lock_wait_mgr_(NULL),
                       rpc_proxy_(),
                       batch_rpc_(NULL) {}
  virtual ~ObLockWaitMgrRpc() {}
  int init(const ObLockWaitMgr *lock_wait_mgr);
  virtual int post_msg(const ObAddr &server, ObLockWaitMgrMsg &msg);
private:
  virtual int post_(const ObAddr &server, ObLockWaitMgrMsg &msg);
  bool is_inited_;
  ObLockWaitMgr *lock_wait_mgr_;
  obrpc::ObLockWaitMgrRpcProxy rpc_proxy_;
  obrpc::ObBatchRpc* batch_rpc_; // used to send msg by batch
  obrpc::ObLockWaitMgrRpcCB<obrpc::OB_LOCK_WAIT_MGR_DST_ENQUEUE> lock_wait_mgr_dst_enqueue_cb_;
  obrpc::ObLockWaitMgrRpcCB<obrpc::OB_LOCK_WAIT_MGR_DST_ENQUEUE_RESP> lock_wait_mgr_enqueue_resp_cb_;
  obrpc::ObLockWaitMgrRpcCB<obrpc::OB_LOCK_WAIT_MGR_LOCK_RELEASE> lock_wait_mgr_lock_release_cb_;
  obrpc::ObLockWaitMgrRpcCB<obrpc::OB_LOCK_WAIT_MGR_WAKE_UP_REMOTE> lock_wait_mgr_wake_up_cb_;
};


} // lockwaitmgr
} // oceanbase

#endif // OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_RPC_