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

#include "ob_gti_rpc.h"
#include "ob_trans_id_service.h"
#include "ob_trans_service.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

namespace oceanbase
{
using namespace share;
using namespace obrpc;
namespace transaction
{

OB_SERIALIZE_MEMBER(ObGtiRequest, tenant_id_, range_);

int ObGtiRequest::init(const uint64_t tenant_id, const int64_t range)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || 0 >= range) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(range));
  } else {
    tenant_id_ = tenant_id;
    range_ = range;
  }
  return ret;
}

bool ObGtiRequest::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && range_ > 0;
}

int ObGtiRequestRpc::init(ObGtiRpcProxy *rpc_proxy, const ObAddr &self, ObGtiSource *gti_source)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "gti request rpc inited twice", KR(ret));
  } else if (OB_ISNULL(rpc_proxy) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(rpc_proxy), K(self));
  } else if (OB_SUCCESS != (ret = gti_request_cb_.init(gti_source))) {
    TRANS_LOG(WARN, "gti request callback inited failed", KR(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_ = self;
    is_inited_ = true;
    TRANS_LOG(INFO, "gti request rpc inited success", KP(this), K(self));
  }
  return ret;
}

int ObGtiRequestRpc::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gti request rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gti request rpc already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "gti request rpc start success");
  }
  return ret;
}

int ObGtiRequestRpc::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gti request rpc not inited", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "gti request rpc stop success");
  }
  return ret;
}

int ObGtiRequestRpc::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gti request rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gti request rpc is running", KR(ret));
  } else {
    TRANS_LOG(INFO, "gti request rpc wait success");
  }
  return ret;
}

void ObGtiRequestRpc::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gti request rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gti request rpc wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    rpc_proxy_ = NULL;
    self_.reset();
    TRANS_LOG(INFO, "gti request rpc destroy");
  }
}

int ObGtiRequestRpc::post(const ObGtiRequest &msg)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gti request rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "gti request rpc not running", KR(ret));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(msg));
  } else if (OB_FAIL(MTL(transaction::ObTransService*)->get_location_adapter()->nonblock_get_leader(GCONF.cluster_id, msg.get_tenant_id(), GTI_LS, server))) {
    TRANS_LOG(WARN, "get leader failed", KR(ret), K(msg), K(GTI_LS));
  } else if (server == self_) {
   ObGtiRpcResult gti_rpc_result;
   if (OB_FAIL(MTL(ObTransIDService*)->handle_request(msg, gti_rpc_result))) {
     TRANS_LOG(WARN, "post local gti request failed", KR(ret), K(server), K(msg));
   } else if (!gti_rpc_result.is_valid()) {
     ret = OB_ERR_UNEXPECTED;
     TRANS_LOG(ERROR, "post local gti request and gti_rpc_result is invalid", KR(ret), K(server),
               K(msg), K(gti_rpc_result));
   } else {
     ObRpcResultCode rcode;
     // Local call, rpc succeeds by default
     rcode.rcode_ = OB_SUCCESS;
     gti_request_cb_.set_tenant_id(msg.get_tenant_id());
     if (OB_FAIL(gti_request_cb_.process(gti_rpc_result, server, rcode))) {
       TRANS_LOG(WARN, "post local gti request failed", KR(ret), K(server), K(msg));
     } else {
       TRANS_LOG(DEBUG, "post local gti request success", KR(ret), K(server), K(msg));
     }
   }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(msg.get_tenant_id())
                                           .timeout(ObGtiRpcResult::OB_GTI_RPC_TIMEOUT)
                                           .group_id(OBCG_ID_SERVICE)
                                           .post(msg, &gti_request_cb_))) {
    TRANS_LOG(WARN, "post gti request failed", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post gti request success", K(server), K(msg));
  }
  return ret;
}

} //transaction

namespace obrpc
{

OB_SERIALIZE_MEMBER(ObGtiRpcResult, tenant_id_, status_, start_id_, end_id_);

int ObGtiRpcResult::init(const uint64_t tenant_id, const int status, const int64_t start_id,
    const int64_t end_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) ||
      (OB_SUCCESS == status && (0 >= start_id || 0 >= end_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id),
        K(status), K(start_id), K(end_id));
  } else {
    tenant_id_ = tenant_id;
    status_ = status;
    start_id_ = start_id;
    end_id_ = end_id;
    TRANS_LOG(INFO, "ObGtiRpcResult init", KR(ret), K(tenant_id),
        K(status), K(start_id), K(end_id));
  }
  return ret;
}

void ObGtiRpcResult::reset()
{
  tenant_id_ = 0;
  status_ = OB_SUCCESS;
  start_id_ = 0;
  end_id_ = 0;
}

bool ObGtiRpcResult::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) &&
    (OB_SUCCESS != status_ || (start_id_ > 0 && end_id_ > 0));
}

int ObGtiP::process()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("gti_request", 100000);
  if (arg_.get_tenant_id() != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tenant is not match", K(ret), K(arg_));
  }
  if (OB_SUCC(ret)) {
    transaction::ObTransIDService *trans_id_service = MTL(transaction::ObTransIDService *);
    if (OB_ISNULL(trans_id_service)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "timestamp service is null", KR(ret), KP(trans_id_service));
    } else if (OB_FAIL(trans_id_service->handle_request(arg_, result_))) {
      TRANS_LOG(WARN, "handle request failed", KR(ret), K(arg_));
    } else {
      // do nothing
    }
  }
  return ret;
}


} // obrpc

} // oceanbase
