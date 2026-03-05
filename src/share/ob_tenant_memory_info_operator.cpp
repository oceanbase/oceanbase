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

#define USING_LOG_PREFIX SHARE

#include "lib/ob_define.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_tenant_memory_info_operator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;
using namespace oceanbase::rootserver;

namespace oceanbase
{
namespace share
{

int ObTenantMemoryInfoOperator::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode_))) {
    LOG_WARN("fail to check oracle mode", K(ret), K(tenant_id_));
  }
  return ret;
}

int ObTenantMemoryInfoOperator::get(const ObIArray<ObAddr> &servers,
                                    ObIArray<TenantServerMemoryInfo> &mem_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  mem_infos.reset();
  if (servers.count() == 0) {
    // do nothing
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {
    ObGetTenantMemoryInfoProxy proxy(rpc_proxy_, &obrpc::ObSrvRpcProxy::get_tenant_memory_info);
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &server = servers.at(i);
      obrpc::ObGetTenantMemoryInfoArg arg;
      bool is_alive = false;
      if (OB_UNLIKELY(!server.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid server", KR(ret), K(server));
      } else if (OB_TMP_FAIL(SVR_TRACER.check_server_alive(server, is_alive))) {
        LOG_WARN("check_server_alive failed, skip server", KR(tmp_ret), K(server));
        // skip this server, do not fail shrink
      } else if (!is_alive) {
        // server down, skip
      } else if (OB_FAIL(arg.init(server))) {
        LOG_WARN("fail to init arg", KR(ret), K(server));
      } else {
        arg.set_tenant_id(tenant_id_);
        const int64_t time_out = ctx.get_timeout();
        if (OB_FAIL(proxy.call(server, time_out, GCONF.cluster_id, tenant_id_, arg))) {
          LOG_WARN("fail to send get_tenant_memory_info rpc", KR(ret), K(server), K(time_out), K(tenant_id_));
        }
      }
    }
    if (OB_TMP_FAIL(proxy.wait())) {
      LOG_WARN("fail to wait all batch result", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
        const obrpc::ObGetTenantMemoryInfoResult *rpc_result = proxy.get_results().at(idx);
        if (OB_ISNULL(rpc_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rpc_result is null", KR(ret), KP(rpc_result));
        } else if (OB_UNLIKELY(!rpc_result->is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rpc_result is invalid", KR(ret), KPC(rpc_result));
        } else {
          TenantServerMemoryInfo mem_info;
          mem_info.tenant_id_ = rpc_result->get_tenant_id();
          mem_info.server_ = rpc_result->get_svr_addr();
          mem_info.menstore_info_ = rpc_result->get_menstore_info();
          mem_info.vector_mem_info_ = rpc_result->get_vector_mem_info();
          if (OB_FAIL(mem_infos.push_back(mem_info))) {
            LOG_WARN("fail to push back mem_info", KR(ret), K(mem_info));
          }
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTenantMemoryInfoOperator::TenantMenstoreInfo,
                    total_memstore_used_,
                    memstore_limit_);

OB_SERIALIZE_MEMBER(ObTenantMemoryInfoOperator::TenantVectorMemInfo,
                    raw_malloc_size_,
                    index_metadata_size_,
                    vector_mem_hold_,
                    vector_mem_used_,
                    vector_mem_limit_);

}//end namespace share
}//end namespace oceanbase
