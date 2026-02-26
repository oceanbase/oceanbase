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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_trigger_storage_cache_executor.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/cmd/ob_trigger_storage_cache_stmt.h"


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
class ObExecContext;
int ObTriggerStorageCacheExecutor::execute(ObExecContext &ctx, ObTriggerStorageCacheStmt &stmt)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObCommonRpcProxy *common_proxy = NULL;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("trigger storage cache is not supported if version is less than 4.3.5.2", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor failed", K(ret));
  } else if (OB_ISNULL(common_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ObArray<ObAddr> server_list;
    ObArray<ObUnit> tenant_units;
    ObUnitTableOperator unit_op;
    ObTriggerStorageCacheArg &arg = stmt.get_rpc_arg();
    uint64_t tenant_id = arg.get_tenant_id();
    if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init unit op", KR(ret));
    } else if (OB_FAIL(unit_op.get_units_by_tenant(tenant_id, tenant_units))) {
      LOG_WARN("failed to get tenant units", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(0 == tenant_units.count())) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
    } else {
      FOREACH_X(unit, tenant_units, OB_SUCC(ret)) {
        bool is_alive = false;
        if (OB_FAIL(SVR_TRACER.check_server_alive(unit->server_, is_alive))) {
          LOG_WARN("check_server_alive failed", KR(ret), K(unit->server_));
        } else if (is_alive) {
          if (has_exist_in_array(server_list, unit->server_)) {
            // server exist
          } else if (OB_FAIL(server_list.push_back(unit->server_))) {
            LOG_WARN("push_back failed", KR(ret), K(unit->server_));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t rpc_timeout_us = GCONF.rpc_timeout;
      obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
      if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
        ret = OB_ERR_SYS;
        LOG_WARN("srv rpc proxy is null", KR(ret), KP(srv_rpc_proxy));
      } else {
        int tmp_ret = OB_SUCCESS;
        FOREACH_X(server_addr, server_list, OB_SUCC(tmp_ret)) {
          if (ObTriggerStorageCacheArg::TRIGGER == arg.get_op()) {
            if (OB_TMP_FAIL(srv_rpc_proxy->to(*server_addr).timeout(rpc_timeout_us).trigger_storage_cache(arg))) {
              // If the server is timeout, just skip it
              if (ret == OB_SUCCESS) {
                ret = tmp_ret;
              }
              LOG_WARN("fail to send trigger storage cache rpc for server", KR(ret), KR(tmp_ret), K(arg), KPC(server_addr));
              tmp_ret = OB_SUCCESS;
            }
          }
        }
      }
    }
  }
#endif
  return ret;
}
} // namespace sql
} // namespace oceanbase