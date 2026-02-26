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

#define USING_LOG_PREFIX SERVER
#include "ob_table_query_session_id_rpc.h"
#include "ob_table_query_session_id_service.h"
#include "share/location_cache/ob_location_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace observer
{
OB_SERIALIZE_MEMBER(ObTableSessIDRequest, tenant_id_, range_);

int ObTableSessIDRequest::init(const uint64_t tenant_id, const int64_t range)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || 0 >= range) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(range));
  } else {
    tenant_id_ = tenant_id;
    range_ = range;
  }
  return ret;
}

bool ObTableSessIDRequest::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && range_ > 0;
}

int ObTableSessIDRequestRpc::init(obrpc::ObTableSessIDRpcProxy *rpc_proxy,
                                       const common::ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("das id request rpc inited twice", KR(ret));
  } else if (OB_ISNULL(rpc_proxy) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(rpc_proxy), K(self));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_ = self;
    is_inited_ = true;
  }
  return ret;
}

void ObTableSessIDRequestRpc::destroy()
{
  if (is_inited_) {
    rpc_proxy_ = NULL;
    self_.reset();
    is_inited_ = false;
  }
}

int ObTableSessIDRequestRpc::fetch_new_range(const ObTableSessIDRequest &req,
                                             ObTableSessIDRpcResult &res)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  ObLSID ls_id = SYS_LS; // use sys_ls to get ls_leader
  uint64_t tenant_id = req.get_tenant_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("das id request rpc not inited", KR(ret));
  } else if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid request", KR(ret), K(req));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id,
                                                        tenant_id,
                                                        ls_id,
                                                        false, // non-block get
                                                        server))) {
    LOG_WARN("get leader failed", KR(ret), K(req), K(GTI_LS));
  } else if (server == self_) {
    // local call
    if (OB_FAIL(MTL(ObTableSessIDService*)->handle_request(req, res))) {
      LOG_WARN("fail to handle local sess id request", K(ret), K(server), K(req), K(tenant_id));
    } else if (!res.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sess id result is invalid", K(ret), K(res));
    }
  } else if (OB_FAIL(rpc_proxy_
                     ->to(server)
                     .by(tenant_id)
                     .timeout(ObTableSessIDRpcResult::SESS_ID_RPC_TIMEOUT)
                     .group_id(OBCG_ID_SERVICE)
                     .sync_fetch_sess_id(req, res))) {
      LOG_WARN("fetch new range failed", K(ret), K(server), K(req));
  }
  LOG_DEBUG("fetch new table session id range finish", K(ret), K(req), K(res));
  return ret;
}
} // namespace observer

namespace obrpc
{
OB_SERIALIZE_MEMBER(ObTableSessIDRpcResult, tenant_id_, status_, start_id_, end_id_);

int ObTableSessIDRpcResult::init(const uint64_t tenant_id, const int status, const int64_t start_id, const int64_t end_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) ||
      (OB_SUCCESS == status && (0 >= start_id || 0 >= end_id))) {
    // when status is OB_SUCCESS, RPC should have succeeded with valid start id and end id
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(start_id), K(end_id));
  } else {
    tenant_id_ = tenant_id;
    status_ = status;
    start_id_ = start_id;
    end_id_ = end_id;
  }
  return ret;
}

bool ObTableSessIDRpcResult::is_valid() const
{
  // when status is not OB_SUCCESS,
  // RPC has failed due to some error on the server side, e.g. NOT_MASTER
  // otherwise, RPC should have succeeded with valid start id and end id
  return is_valid_tenant_id(tenant_id_) &&
         (OB_SUCCESS != status_ || (start_id_ > 0 && end_id_ > 0));
}

int ObTableSessIDP::process()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("gti_request", 100000);
  if (arg_.get_tenant_id() != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is not match", K(ret), K(arg_));
  }
  if (OB_SUCC(ret)) {
    observer::ObTableSessIDService *sess_id_service = MTL(observer::ObTableSessIDService *);
    if (OB_ISNULL(sess_id_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("timestamp service is null", KR(ret), KP(sess_id_service));
    } else if (OB_FAIL(sess_id_service->handle_request(arg_, result_))) {
      LOG_WARN("handle request failed", KR(ret), K(arg_));
    } else {
      // do nothing
    }
  }
  return ret;
}
} // namespace obrpc
} // namespace oceanbase