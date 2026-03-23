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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_schema.h"
#include "pl/ob_pl.h"
#include "observer/ob_server_struct.h" // GCTX
#include "common/ob_timeout_ctx.h" // ObTimeoutCtx
#include "share/ob_common_rpc_proxy.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_history_recycle_service.h"
#include "share/ob_inspection_service.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{

int ObDBMSSchema::recycle_schema_history(
  ObPLExecCtx &pl_ctx,
  sql::ParamStore &params,
  common::ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(params.count() != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count", KR(ret), K(params.count()));
  } else if (OB_ISNULL(pl_ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is null", KR(ret));
  } else if (OB_ISNULL(pl_ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else {
    const uint64_t tenant_id = pl_ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    MTL_SWITCH(tenant_id) {
      share::schema::ObSchemaHistoryRecycleService *svc = MTL(share::schema::ObSchemaHistoryRecycleService*);
      if (OB_ISNULL(svc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObSchemaHistoryRecycleService is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(svc->run_once())) {
        LOG_WARN("fail to run schema history recycle service", KR(ret), K(tenant_id));
      }
    } else {
      LOG_WARN("switch tenant failed", KR(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObDBMSSchema::run_inspection(
  ObPLExecCtx &pl_ctx,
  sql::ParamStore &params,
  common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  common::ObAddr leader;
  common::ObTimeoutCtx timeout_ctx;
  obrpc::ObRunInspectionArg arg;
  if (OB_UNLIKELY(0 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count", KR(ret), K(params.count()));
  } else if (OB_ISNULL(pl_ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is null", KR(ret));
  } else if (OB_ISNULL(pl_ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.srv_rpc_proxy_), K(GCTX.location_service_));
  } else {
    const uint64_t tenant_id = pl_ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    if (OB_FAIL(arg.init(tenant_id))) {
      LOG_WARN("init failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF._ob_ddl_timeout))) {
      LOG_WARN("set default timeout ctx failed", KR(ret), K(tenant_id));
    } else {
      // always execute on leader
      const int64_t RETRY_CNT_LIMIT = 3;
      int64_t retry_cnt = 0;
      do {
        if (OB_NOT_MASTER == ret) {
          ret = OB_SUCCESS;
          ob_usleep(1000 * 1000); // 1s
        }
        if (FAILEDx(GCTX.location_service_->get_leader_with_retry_until_timeout(
            GCONF.cluster_id,
            tenant_id,
            SYS_LS,
            leader))) {
          LOG_WARN("get leader failed", KR(ret), "cluster_id", GCONF.cluster_id.get_value(),
              K(tenant_id), K(leader));
        } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader)
                                                .by(tenant_id)
                                                .timeout(timeout_ctx.get_timeout())
                                                .run_inspection(arg))) {
          LOG_WARN("run inspection failed", KR(ret), K(tenant_id), K(leader), K(arg));
        }
      } while (OB_NOT_MASTER == ret && ++retry_cnt <= RETRY_CNT_LIMIT);
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
