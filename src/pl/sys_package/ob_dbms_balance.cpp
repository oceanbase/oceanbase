/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_balance.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server_struct.h" // GCTX
#include "common/ob_timeout_ctx.h" // ObTimeoutCtx

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace observer;

namespace pl
{
int ObDBMSBalance::trigger_partition_balance(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  int64_t job_id = OB_INVALID_ID;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  int32_t balance_timeout_s = 0;
  int64_t balance_timeout_us = 0;
  ObAddr leader;
  ObTimeoutCtx timeout_ctx;
  ObTriggerPartitionBalanceArg arg;
  UNUSED(result);
  uint64_t data_version = 0;
  if (OB_ISNULL(GCTX.srv_rpc_proxy_)
      || OB_ISNULL(GCTX.location_service_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.srv_rpc_proxy_),
        K(GCTX.location_service_), K(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else if (data_version < DATA_VERSION_4_2_4_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to trigger partition balance", KR(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "trigger partition balance is");
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, trigger_partition_balance is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not user tenant, trigger partition balance is");
  } else if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", KR(ret), K(params));
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_int32(balance_timeout_s))) {
    LOG_WARN("get int failed", KR(ret), K(params.at(0)));
  } else if (FALSE_IT(balance_timeout_us = balance_timeout_s * USECS_PER_SEC)) {
  } else if (OB_FAIL(arg.init(tenant_id, balance_timeout_us))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id), K(balance_timeout_us), K(params.at(0)));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.balancer_task_timeout))) {
    LOG_WARN("set default timeout ctx failed", KR(ret), K(ctx));
  } else {
    const int64_t RETRY_CNT_LIMIT = 3;
    int64_t retry_cnt = 0;
    do {
      if (OB_NOT_MASTER == ret) {
        ret = OB_SUCCESS;
        ob_usleep(1_s);
      }
      if (FAILEDx(GCTX.location_service_->get_leader_with_retry_until_timeout(
          GCONF.cluster_id,
          tenant_id,
          SYS_LS,
          leader))) { // default 1s timeout
        LOG_WARN("get leader failed", KR(ret), "cluster_id", GCONF.cluster_id.get_value(),
            K(tenant_id), K(leader));
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader)
                                              .by(tenant_id)
                                              .timeout(timeout_ctx.get_abs_timeout())
                                              .trigger_partition_balance(arg))) {
        LOG_WARN("trigger partition balance failed", KR(ret), K(arg));
      }
    } while (OB_NOT_MASTER == ret && ++retry_cnt <= RETRY_CNT_LIMIT);
  }
  return ret;
}

} // end of pl
} // end oceanbase
