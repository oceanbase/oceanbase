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

#define USING_LOG_PREFIX STORAGE

#include "ob_common_id_utils.h"
#include "share/ob_share_util.h"
#include "storage/tx/ob_unique_id_service.h" // ObUniqueIDService

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{
int ObCommonIDUtils::gen_unique_id(const uint64_t tenant_id, ObCommonID &id)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_TIMEOUT = GCONF.rpc_timeout;
  int64_t unique_id = ObCommonID::INVALID_ID;

  id.reset();

  if (OB_UNLIKELY(MTL_ID() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaild tenant id", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("set default timeout ctx fail", KR(ret), K(DEFAULT_TIMEOUT));
  } else if (OB_FAIL(MTL(transaction::ObUniqueIDService*)->gen_unique_id(unique_id,
      ctx.get_timeout()))) {
    LOG_WARN("gen_unique_id failed", KR(ret), K(tenant_id), K(ctx));
  } else {
    id = ObCommonID(unique_id);
  }

  return ret;
}

int ObCommonIDUtils::gen_unique_id_by_rpc(const uint64_t tenant_id, ObCommonID &id)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  share::ObLocationService *location_service = nullptr;
  ObAddr leader_addr;
  if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)
      || OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service or location_cache is null", KR(ret), KP(srv_rpc_proxy), KP(location_service));
  } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                  tenant_id,
                                                  SYS_LS,
                                                  false,/*force_renew*/
                                                  leader_addr))) {
    LOG_WARN("get leader failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).by(tenant_id).gen_unique_id(tenant_id, id))) {
    LOG_WARN("fail to send gen unique id rpc", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObCommonIDUtils::gen_monotonic_id(const uint64_t tenant_id,
    const ObMaxIdType id_type,
    const int32_t group_id,
    common::ObMySQLProxy &proxy,
    share::ObCommonID &id)
{
  int ret = OB_SUCCESS;
  uint64_t ret_id = OB_INVALID_ID;
  ObMaxIdFetcher id_fetcher(proxy, group_id);

  id.reset();

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(id_fetcher.fetch_new_max_id(tenant_id, id_type, ret_id, 1L/*start id*/))) {
    LOG_WARN("fetch new max id failed", KR(ret), K(tenant_id), K(id_type));
  } else {
    id = ObCommonID(ret_id);
  }

  return ret;
}

} // end namespace storage
} // end namespace oceanbase
