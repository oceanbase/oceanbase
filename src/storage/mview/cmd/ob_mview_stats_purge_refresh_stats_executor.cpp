/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_stats_purge_refresh_stats_executor.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mview_refresh_stats_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;

ObMViewStatsPurgeRefreshStatsExecutor::ObMViewStatsPurgeRefreshStatsExecutor()
  : ctx_(nullptr),
    session_info_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    retention_period_(INT64_MAX)
{
}

ObMViewStatsPurgeRefreshStatsExecutor::~ObMViewStatsPurgeRefreshStatsExecutor() {}

int ObMViewStatsPurgeRefreshStatsExecutor::execute(ObExecContext &ctx,
                                                   int64_t retention_period)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  retention_period_ = retention_period;
  CK(OB_NOT_NULL(session_info_ = ctx.get_my_session()));
  CK(OB_NOT_NULL(ctx.get_sql_proxy()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObMViewExecutorUtil::check_min_data_version(
    tenant_id_, DATA_VERSION_4_3_0_0,
    "tenant's data version is below 4.3.0.0, purge refreh stats is"));

  if (OB_SUCC(ret)) {
    int64_t effective_retention = retention_period_;
    if (INT64_MAX == effective_retention) {
      ObMViewRefreshStatsParams sys_params;
      if (OB_FAIL(ObMViewRefreshStatsParams::fetch_sys_defaults(*ctx.get_sql_proxy(),
                                                                tenant_id_,
                                                                sys_params))) {
        LOG_WARN("fail to fetch sys defaults", KR(ret), K(tenant_id_));
      } else {
        effective_retention = sys_params.get_retention_period();
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      int64_t affected_rows = 0;
      if (OB_FAIL(ObMViewRefreshStatsUtils::purge_refresh_stats(ctx.get_sql_proxy(), tenant_id_, effective_retention, affected_rows))) {
        LOG_WARN("fail to purge refresh stats", KR(ret), K(effective_retention));
      }
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
