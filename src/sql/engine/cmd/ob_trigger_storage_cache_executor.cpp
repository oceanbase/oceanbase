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
#include "observer/ob_server_event_history_table_operator.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"


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
  obrpc::ObTriggerStorageCacheArg &arg = stmt.get_rpc_arg();
  LOG_INFO("[SCP]executor: start execute storage cache command",
      "op", arg.get_op(),
      "tenant_id", arg.get_tenant_id(),
      "tablet_id", arg.get_tablet_id(),
      "policy_status", arg.get_policy_status());
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("trigger storage cache is not supported if version is less than 4.3.5.2", K(ret));
  } else if (ObTriggerStorageCacheArg::SET_STATUS == arg.get_op() &&
             GET_MIN_CLUSTER_VERSION() < DATA_VERSION_4_4_1_0) {
    // Note: 4.4.1 is special - all releases are hotfixes with the same version number,
    // so this check cannot actually block older 4.4.1 builds. However, it won't cause
    // any adverse effects since the feature simply won't work on unsupported builds.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set status storage cache policy is not supported if version is less than 4.4.1.0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set status storage cache policy in version less than 4.4.1.0");
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
    uint64_t tenant_id = arg.get_tenant_id();
    // For meta tenant, use its corresponding user tenant's units
    // Meta tenant shares units with user tenant
    uint64_t unit_tenant_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;
    if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init unit op", KR(ret));
    } else if (OB_FAIL(unit_op.get_units_by_tenant(unit_tenant_id, tenant_units))) {
      LOG_WARN("failed to get tenant units", KR(ret), K(tenant_id), K(unit_tenant_id));
    } else if (OB_UNLIKELY(0 == tenant_units.count())) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(tenant_id), K(unit_tenant_id));
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
        LOG_INFO("[SCP]executor: about to send RPC to servers",
            "server_count", server_list.count(),
            K(arg));
        FOREACH_X(server_addr, server_list, OB_SUCC(tmp_ret)) {
          if (ObTriggerStorageCacheArg::TRIGGER == arg.get_op() ||
              ObTriggerStorageCacheArg::SET_STATUS == arg.get_op()) {
            LOG_INFO("[SCP]executor: sending RPC to server",
                KPC(server_addr),
                "op", arg.get_op(),
                "tenant_id", arg.get_tenant_id(),
                "tablet_id", arg.get_tablet_id(),
                "policy_status", arg.get_policy_status());
            if (OB_TMP_FAIL(srv_rpc_proxy->to(*server_addr).timeout(rpc_timeout_us).trigger_storage_cache(arg))) {
              // If the server is timeout, just skip it
              if (ret == OB_SUCCESS) {
                ret = tmp_ret;
              }
              LOG_WARN("[SCP]executor: fail to send trigger storage cache rpc for server", KR(ret), KR(tmp_ret), K(arg), KPC(server_addr));
              tmp_ret = OB_SUCCESS;
            } else {
              LOG_INFO("[SCP]executor: RPC sent successfully to server", KPC(server_addr), K(arg));
            }
          }
        }
      }
    }
    // record event to __all_server_event_history
    if (ObTriggerStorageCacheArg::TRIGGER == arg.get_op()) {
      SERVER_EVENT_ADD("storage_cache_policy", "trigger_executor",
          "tenant_id", arg.get_tenant_id(),
          "ret", ret,
          "trace_id", *ObCurTraceId::get_trace_id());
    } else if (ObTriggerStorageCacheArg::SET_STATUS == arg.get_op()) {
      const char *policy_status_str = "UNKNOWN";
      if (storage::PolicyStatus::HOT == arg.get_policy_status()) {
        policy_status_str = "HOT";
      } else if (storage::PolicyStatus::AUTO == arg.get_policy_status()) {
        policy_status_str = "AUTO";
      } else if (storage::PolicyStatus::COLD == arg.get_policy_status()) {
        policy_status_str = "COLD";
      }
      SERVER_EVENT_ADD("storage_cache_policy", "set_status",
          "tenant_id", arg.get_tenant_id(),
          "ret", ret,
          "trace_id", *ObCurTraceId::get_trace_id(),
          "tablet_id", arg.get_tablet_id(),
          "policy_status", policy_status_str);
    }
  }
#endif
  return ret;
}
} // namespace sql
} // namespace oceanbase