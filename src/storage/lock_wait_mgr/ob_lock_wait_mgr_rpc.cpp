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

#include "ob_lock_wait_mgr_rpc.h"
#include "ob_lock_wait_mgr.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_errno.h"
#include "share/ob_errno.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "observer/ob_srv_network_frame.h"

#define USING_LOG_PREFIX TRANS

namespace oceanbase
{

using namespace obrpc;

namespace lockwaitmgr
{

int ObLockWaitMgrRpc::init(const ObLockWaitMgr *lock_wait_mgr)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  obrpc::ObBatchRpc *batch_rpc = GCTX.batch_rpc_;
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  rpc::frame::ObReqTransport *req_transport = net_frame->get_req_transport();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObLockWaitMgrRpc already inited", K(ret), K(is_inited_));
  } else if (OB_ISNULL(lock_wait_mgr)
          || OB_ISNULL(req_transport)
          || !self.is_valid()
          || OB_ISNULL(batch_rpc)) {
    TRANS_LOG(WARN, "invalid argument",
      KP(lock_wait_mgr), KP(req_transport), K(self), KP(batch_rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(rpc_proxy_.init(req_transport, self))) {
    TRANS_LOG(WARN, "init ObLockWaitMgrRpc rpc_proxy fail", KR(ret));
  } else {
    batch_rpc_ = batch_rpc;
    is_inited_ = true;
    TRANS_LOG(INFO, "ObLockWaitMgrRPC inited success");
  }
  return ret;
}

int ObLockWaitMgrRpc::post_msg(const ObAddr &server, ObLockWaitMgrMsg &msg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = msg.get_tenant_id();

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObLockWaitMgrRpc not inited");
    ret = OB_NOT_INIT;
  } else if (!is_valid_tenant_id(tenant_id) ||
             !server.is_valid() || !msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(server), K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(post_(server, msg))) {
    TRANS_LOG(WARN, "post msg error", K(ret), K(server), K(msg));
  }
  return ret;
}

int ObLockWaitMgrRpc::post_(const ObAddr &server, ObLockWaitMgrMsg &msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();
  const uint64_t tenant_id = msg.get_tenant_id();
  switch (msg_type)
  {
    case LWM_DST_ENQUEUE: {
      ret = rpc_proxy_.to(server).by(tenant_id).timeout(GCONF.rpc_timeout).
              inform_dst_enqueue(static_cast<ObLockWaitMgrDstEnqueueMsg&>(msg), &lock_wait_mgr_dst_enqueue_cb_);
      break;
    }
    case LWM_DST_ENQUEUE_RESP: {
      ret = rpc_proxy_.to(server).by(tenant_id).timeout(GCONF.rpc_timeout).
              dst_enqueue_resp(static_cast<ObLockWaitMgrDstEnqueueRespMsg&>(msg), &lock_wait_mgr_enqueue_resp_cb_);
      break;
    }
    case LWM_LOCK_RELEASE: {
      ret = rpc_proxy_.to(server).by(tenant_id).timeout(GCONF.rpc_timeout).
              inform_lock_release(static_cast<ObLockWaitMgrLockReleaseMsg&>(msg), &lock_wait_mgr_lock_release_cb_);
      break;
    }
    case LWM_WAKE_UP: {
      ret = rpc_proxy_.to(server).by(tenant_id).timeout(GCONF.rpc_timeout).
              wake_up_remote(static_cast<ObLockWaitMgrWakeUpRemoteMsg&>(msg), &lock_wait_mgr_wake_up_cb_);
      break;
    }
    case LWM_CHECK_NODE_STATE:
    case LWM_CHECK_NODE_STATE_RESP: {
      if (OB_FAIL(batch_rpc_->post(tenant_id,
                                   server,
                                   obrpc::ObRpcNetHandler::CLUSTER_ID,
                                   obrpc::LOCK_WAIT_MGR_REQ,
                                   msg.get_msg_type(),
                                   msg))) {
        TRANS_LOG(WARN, "post check node state msg failed", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "rpc proxy not supported", K(tenant_id), K(server), K(msg));
      break;
    }
  }
  TRANS_LOG(TRACE, "LockWaitMgr send msg finish", K(ret), K(server), K(msg));
  return ret;
}

} // lockwaitmgr

namespace obrpc
{
OB_SERIALIZE_MEMBER(ObLockWaitMgrRpcResult, status_);

#define LOCK_WAIT_MGR_PROCESS(name, handle_func)                                \
int ObLockWaitMgr##name##P::process()                                           \
{                                                                               \
  int ret = OB_SUCCESS;                                                         \
  lockwaitmgr::ObLockWaitMgr *lwm = NULL;                                          \
  uint64_t tenant_id = rpc_pkt_->get_tenant_id();                               \
  if (tenant_id != MTL_ID()) {                                                  \
    ret = OB_ERR_UNEXPECTED;                                                    \
    TRANS_LOG(WARN, "tenant is not match", K(ret), K(tenant_id));               \
  } else if (OB_ISNULL(lwm = MTL(lockwaitmgr::ObLockWaitMgr*))) {                  \
    ret = OB_ERR_UNEXPECTED;                                                    \
    TRANS_LOG(WARN, "get lock wait mgr fail", K(ret), K(tenant_id));            \
  } else if (!arg_.is_valid()) {                                                \
    ret = OB_INVALID_ARGUMENT;                                                  \
    TRANS_LOG(WARN, "msg is invalid", K(ret), K_(arg));                         \
  } else if ((*lwm).handle_func(arg_, result_)){                                \
    TRANS_LOG(WARN, "handle lock wait mgr msg fail", K(ret), "msg", arg_);      \
  }                                                                             \
  return ret;                                                                   \
}

LOCK_WAIT_MGR_PROCESS(DstEnqueue, handle_inform_dst_enqueue_req);
LOCK_WAIT_MGR_PROCESS(DstEnqueueResp, handle_dst_enqueue_resp);
LOCK_WAIT_MGR_PROCESS(LockRelease, handle_lock_release_req);
LOCK_WAIT_MGR_PROCESS(WakeUp, handle_wake_up_req);


int handle_lock_wait_mgr_msg_cb(int status,
                                int16_t msg_type,
                                uint64_t hash,
                                rpc::NodeID node_id,
                                const common::ObAddr &receiver_addr)
{
  return MTL(lockwaitmgr::ObLockWaitMgr *)->handle_msg_cb(status,
                                                       msg_type,
                                                       hash,
                                                       node_id,
                                                       receiver_addr);
}

} // obrpc
} // oceanabse