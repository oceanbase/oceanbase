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

#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_task.h"
#include "observer/omt/ob_tenant.h"
#include "ob_gts_rpc.h"
#include "ob_ts_worker.h"
#include "ob_ts_mgr.h"
#include "ob_timestamp_access.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

namespace oceanbase
{

using namespace common;
using namespace transaction;
using namespace obrpc;
using namespace storage;
using namespace omt;
using namespace observer;
using namespace share;

namespace obrpc
{

OB_SERIALIZE_MEMBER(ObGtsRpcResult, tenant_id_, status_, srr_.mts_, gts_start_, gts_end_);

int ObGtsRpcResult::init(const uint64_t tenant_id, const int status,
    const MonotonicTs srr, const int64_t gts_start, const int64_t gts_end)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) ||
      (OB_SUCCESS == status && (!srr.is_valid() || 0 >= gts_start || 0 >= gts_end))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id),
        K(status), K(srr), K(gts_start), K(gts_end));
  } else {
    tenant_id_ = tenant_id;
    status_ = status;
    srr_ = srr;
    gts_start_ = gts_start;
    gts_end_ = gts_end;
  }
  return ret;
}

void ObGtsRpcResult::reset()
{
  tenant_id_ = 0;
  status_ = OB_SUCCESS;
  srr_.reset();
  gts_start_ = 0;
  gts_end_ = 0;
}

bool ObGtsRpcResult::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) &&
    (OB_SUCCESS != status_ || (srr_.is_valid() && gts_start_ > 0 && gts_end_ > 0));
}

int ObGtsP::process()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("gts_request", 100000);
  if (arg_.get_tenant_id() != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tenant is not match", K(ret), K(arg_));
  }
  if (OB_SUCC(ret)) {
    ObTimestampAccess *timestamp_access = MTL(ObTimestampAccess *);
    if (OB_ISNULL(timestamp_access)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "timestamp access is null", KR(ret), KP(timestamp_access));
    } else if (OB_FAIL(timestamp_access->handle_request(arg_, result_))) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        TRANS_LOG(WARN, "handle request failed", KR(ret), K(arg_));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObGtsErrRespP::process()
{
  int ret = OB_SUCCESS;
  ObTsMgr *ts_mgr = &OB_TS_MGR;
  ObTimeGuard timeguard("gts_err_response", 100000);

  if (NULL == ts_mgr) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gts cache mgr is null, unexpected error", KR(ret), KP(ts_mgr));
  } else if (OB_FAIL(ts_mgr->handle_gts_err_response(arg_))) {
    TRANS_LOG(WARN, "handle gts err response error", KR(ret), K_(arg));
  } else {
    // do nothing
  }

  return ret;
}

} // obrpc

namespace transaction
{
int ObGtsRequestRpc::init(ObGtsRpcProxy *rpc_proxy, const ObAddr &self,
                          ObTsMgr *ts_mgr, ObTsWorker *ts_worker)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "gts request rpc inited twice", KR(ret));
  } else if (OB_ISNULL(rpc_proxy) || !self.is_valid() || OB_ISNULL(ts_mgr) || OB_ISNULL(ts_worker)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(rpc_proxy), K(self), KP(ts_mgr), KP(ts_worker));
  } else if (OB_SUCCESS != (ret = gts_request_cb_.init(ts_mgr, ts_worker))) {
    TRANS_LOG(WARN, "gts request callback inited failed", KR(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_ = self;
    is_inited_ = true;
    ts_mgr_ = ts_mgr;
    TRANS_LOG(INFO, "gts request rpc inited success", KP(this), K(self), KP(ts_mgr));
  }
  return ret;
}

int ObGtsRequestRpc::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts request rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gts request rpc already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "gts request rpc start success");
  }
  return ret;
}

int ObGtsRequestRpc::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts request rpc not inited", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "gts request rpc stop success");
  }
  return ret;
}

int ObGtsRequestRpc::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts request rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gts request rpc is running", KR(ret));
  } else {
    TRANS_LOG(INFO, "gts request rpc wait success");
  }
  return ret;
}

void ObGtsRequestRpc::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gts request rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gts request rpc wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    rpc_proxy_ = NULL;
    self_.reset();
    TRANS_LOG(INFO, "gts request rpc destroy");
  }
}

int ObGtsRequestRpc::post(const uint64_t tenant_id, const ObAddr &server,
    const ObGtsRequest &msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts request rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "gts request rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  } else if (server == self_) {
    // Use local calls instead of rpc
    ObGtsRpcResult gts_rpc_result;
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(MTL(ObTimestampAccess*)->handle_request(msg, gts_rpc_result))) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "post local gts request failed", KR(ret), K(server), K(msg));
        }
      } else if (!gts_rpc_result.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "post local gts request and gts_rpc_result is invalid", KR(ret), K(server),
                  K(msg), K(gts_rpc_result));
      } else {
        ObRpcResultCode rcode;
        // Local call, rpc succeeds by default
        rcode.rcode_ = OB_SUCCESS;
        gts_request_cb_.set_tenant_id(tenant_id);
        if (OB_FAIL(gts_request_cb_.process(gts_rpc_result, server, rcode))) {
          TRANS_LOG(WARN, "post local gts request failed", KR(ret), K(server), K(msg));
        } else {
          TRANS_LOG(DEBUG, "post local gts request success", KR(ret), K(server), K(msg));
        }
      }
    }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id)
                                           .timeout(ObGtsRpcResult::OB_GTS_RPC_TIMEOUT)
                                           .group_id(OBCG_ID_SERVICE)
                                           .post(msg, &gts_request_cb_))) {
    TRANS_LOG(WARN, "post gts request failed", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post gts request success", K(server), K(msg));
  }
  return ret;
}

int ObGtsResponseRpc::init(oceanbase::rpc::frame::ObReqTransport *req_transport, const ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "gts response rpc inited twice", KR(ret));
  } else if (OB_ISNULL(req_transport) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(req_transport), K(self));
  } else if (OB_FAIL(rpc_proxy_.init(req_transport, self))) {
    TRANS_LOG(WARN, "rpc proxy init failed", KR(ret), K(self));
  } else {
    self_ = self;
    is_inited_ = true;
    TRANS_LOG(INFO, "gts response rpc inited success", K(self), KP(this));
  }
  return ret;
}

int ObGtsResponseRpc::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts response rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gts response rpc already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "gts response rpc start success");
  }
  return ret;
}

int ObGtsResponseRpc::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts response rpc not inited", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "gts response rpc stop success");
  }
  return ret;
}

int ObGtsResponseRpc::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts response rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "gts response rpc already running", KR(ret));
  } else {
    TRANS_LOG(INFO, "gts response rpc wait success");
  }
  return ret;
}

void ObGtsResponseRpc::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gts response rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "gts response rpc wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    self_.reset();
    TRANS_LOG(INFO, "gts response rpc destroyed");
  }
}

int ObGtsResponseRpc::post(const uint64_t tenant_id, const ObAddr &server,
    const ObGtsErrResponse &msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "gts response rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "gts response rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  } else if (OB_FAIL(rpc_proxy_.to(server).by(tenant_id).post(msg, NULL))) {
    TRANS_LOG(WARN, "post gts err response message error", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post gts err response message success", K(server), K(msg));
  }
  return ret;
}

} // transaction

} // oceanbase
