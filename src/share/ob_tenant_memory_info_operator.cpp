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

int ObTenantMemoryInfoOperator::get(const common::ObIArray<common::ObAddr> &servers, common::ObIArray<TenantServerMemoryInfo> &mem_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  mem_infos.reset();
  if (servers.count() == 0) {
    // do nothing
  } else if (OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_2_1)) {
    if (OB_FAIL(get_by_sql(servers, mem_infos))) {
      LOG_WARN("fail to get from sql", KR(ret), K(servers));
    }
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
          if (OB_FAIL(mem_info.set_by_rpc(rpc_result->get_tenant_id(),
                                            rpc_result->get_svr_addr(),
                                            rpc_result->get_memstore_info(),
                                            rpc_result->get_vector_mem_info()))) {
            LOG_WARN("fail to init mem_info from rpc", KR(ret), KPC(rpc_result));
          } else if (OB_FAIL(mem_infos.push_back(mem_info))) {
            LOG_WARN("fail to push back mem_info", KR(ret), K(mem_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantMemoryInfoOperator::get_by_sql(const common::ObIArray<common::ObAddr> &servers, common::ObIArray<TenantServerMemoryInfo> &mem_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString unit_servers_str;
  for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
    char svr_ip_str[common::MAX_IP_ADDR_LENGTH] = "";
    const common::ObAddr &unit_server = servers.at(i);
    if (!unit_server.ip_to_string(svr_ip_str, static_cast<int32_t>(MAX_IP_ADDR_LENGTH))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert ip to string", K(ret));
    } else if (OB_FAIL(unit_servers_str.append_fmt("%s(svr_ip='%s' and svr_port=%d)",
        (0 != i) ? " or " : "",
        svr_ip_str,
        unit_server.get_port()))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else {} // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, svr_ip, svr_port, memstore_used, memstore_limit FROM %s WHERE tenant_id = %ld and (%s)",
      OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TNAME, tenant_id_, unit_servers_str.ptr()))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(mysql_proxy_.read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else {
        TenantServerMemoryInfo mem_info;
        int64_t tmp_real_str_len = 0; // It is only used to fill out the parameters and does not work. It is necessary to ensure that there is no'\0' character in the corresponding string
        char svr_ip[OB_IP_STR_BUFF] = "";
        int64_t svr_port = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            uint64_t tenant_id = OB_INVALID_TENANT_ID;
            TenantMemstoreInfo memstore_info;
            common::ObAddr server_addr;
            EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
            EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
            (void) tmp_real_str_len; // make compiler happy
            EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "memstore_used", memstore_info.total_memstore_used_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "memstore_limit", memstore_info.memstore_limit_, int64_t);
            if (OB_FAIL(ret)) {
            } else if (!server_addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid svr_ip or invalid svr_port", K(svr_ip), K(svr_port), K(ret));
            } else if (OB_FAIL(mem_info.set_by_sql(tenant_id, server_addr, memstore_info))) {
              LOG_WARN("fail to init mem_info from sql", K(ret), K(tenant_id), K(server_addr), K(memstore_info));
            } else if (OB_FAIL(mem_infos.push_back(mem_info))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantMemoryInfoOperator::TenantServerMemoryInfo::set_by_rpc(uint64_t tenant_id,
                                                                     const common::ObAddr &server,
                                                                     const TenantMemstoreInfo &memstore_info,
                                                                     const TenantVectorMemInfo &vector_mem_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(server));
  } else {
    tenant_id_ = tenant_id;
    server_ = server;
    memstore_info_ = memstore_info;
    vector_mem_info_ = vector_mem_info;
  }
  return ret;
}

int ObTenantMemoryInfoOperator::TenantServerMemoryInfo::set_by_sql(uint64_t tenant_id,
                                                                     const common::ObAddr &server,
                                                                     const TenantMemstoreInfo &memstore_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(server));
  } else {
    tenant_id_ = tenant_id;
    server_ = server;
    memstore_info_ = memstore_info;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(TenantMemstoreInfo,
                    total_memstore_used_,
                    memstore_limit_);

OB_SERIALIZE_MEMBER(TenantVectorMemInfo,
                    raw_malloc_size_,
                    index_metadata_size_,
                    vector_mem_hold_,
                    vector_mem_used_,
                    vector_mem_limit_);

}//end namespace share
}//end namespace oceanbase
