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

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_id_rpc.h"
#include "ob_das_id_service.h"
#include "share/location_cache/ob_location_service.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER(ObDASIDRequest, tenant_id_, range_);

int ObDASIDRequest::init(const uint64_t tenant_id, const int64_t range)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || 0 >= range) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(range));
  } else {
    tenant_id_ = tenant_id;
    range_ = range;
  }
  return ret;
}

bool ObDASIDRequest::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && range_ > 0;
}

ObDASIDRequestRpc::ObDASIDRequestRpc()
  : is_inited_(false),
    is_running_(false),
    rpc_proxy_(NULL),
    self_()
{
}

int ObDASIDRequestRpc::init(obrpc::ObDASIDRpcProxy *rpc_proxy,
                            const common::ObAddr &self,
                            ObDASIDCache *id_cache)
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

void ObDASIDRequestRpc::destroy()
{
  if (is_inited_) {
    rpc_proxy_ = NULL;
    self_.reset();
    is_inited_ = false;
  }
}

int ObDASIDRequestRpc::fetch_new_range(const ObDASIDRequest &msg,
                                       obrpc::ObDASIDRpcResult &res,
                                       const int64_t timeout,
                                       const bool force_renew)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  uint64_t tenant_id = msg.get_tenant_id();
  if (is_user_tenant(tenant_id)) {
    tenant_id = gen_meta_tenant_id(tenant_id);
  }
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("das id request rpc not inited", KR(ret));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid request", KR(ret), K(msg));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id,
                                                        tenant_id,
                                                        DAS_ID_LS,
                                                        force_renew,
                                                        server))) {
    TRANS_LOG(WARN, "get leader failed", KR(ret), K(msg), K(GTI_LS));
  } else if (OB_FAIL(rpc_proxy_
                     ->to(server)
                     .by(tenant_id)
                     .timeout(timeout)
                     .group_id(OBCG_ID_SERVICE)
                     .sync_fetch_das_id(msg, res))) {
      LOG_WARN("fetch new range failed", KR(ret), K(server), K(msg));
  }
  LOG_INFO("fetch new DAS ID range finish", KR(ret), K(msg), K(res));
  return ret;
}
} // namespace sql

namespace obrpc
{

OB_SERIALIZE_MEMBER(ObDASIDRpcResult, tenant_id_, status_, start_id_, end_id_);

int ObDASIDRpcResult::init(const uint64_t tenant_id,
                           const int status,
                           const int64_t start_id,
                           const int64_t end_id)
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

bool ObDASIDRpcResult::is_valid() const
{
  // when status is not OB_SUCCESS,
  // RPC has failed due to some error on the server side, e.g. NOT_MASTER
  // otherwise, RPC should have succeeded with valid start id and end id
  return is_valid_tenant_id(tenant_id_) &&
         (OB_SUCCESS != status_ || (start_id_ > 0 && end_id_ > 0));
}

int ObDASIDP::process()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("das_id_request", 100000);
  sql::ObDASIDService *das_id_service = MTL(sql::ObDASIDService *);
  if (OB_ISNULL(das_id_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das id service is null", K(ret), KP(das_id_service));
  } else if (OB_FAIL(das_id_service->handle_request(arg_, result_))) {
    LOG_WARN("handle request failed", K(ret), K(arg_));
  }
  return ret;
}
} // namespace obrpc
} // namespace oceanbase
