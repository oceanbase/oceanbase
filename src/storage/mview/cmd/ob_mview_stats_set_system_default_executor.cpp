/**
 * Copyright (c) 2023 OceanBase
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

#include "storage/mview/cmd/ob_mview_stats_set_system_default_executor.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace sql;

ObMViewStatsSetSystemDefaultExecutor::ObMViewStatsSetSystemDefaultExecutor()
  : ctx_(nullptr),
    session_info_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    op_type_(OpType::MAX),
    collection_level_(ObMVRefreshStatsCollectionLevel::MAX),
    retention_period_(0)
{
}

ObMViewStatsSetSystemDefaultExecutor::~ObMViewStatsSetSystemDefaultExecutor() {}

int ObMViewStatsSetSystemDefaultExecutor::execute(ObExecContext &ctx,
                                                  const ObMViewStatsSetSystemDefaultArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  CK(OB_NOT_NULL(session_info_ = ctx.get_my_session()));
  CK(OB_NOT_NULL(ctx.get_sql_proxy()));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObMViewExecutorUtil::check_min_data_version(
    tenant_id_, DATA_VERSION_4_3_0_0,
    "tenant's data version is below 4.3.0.0, set system default is"));
  OZ(resolve_arg(arg));

  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    ObMViewRefreshStatsParams stats_params;
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), tenant_id_))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(ObMViewRefreshStatsParams::fetch_sys_defaults(
                 trans, tenant_id_, stats_params, true /*for_update*/))) {
      LOG_WARN("fail to fetch sys defaults", KR(ret), K(tenant_id_));
    } else if (OpType::SET_COLLECTION_LEVEL == op_type_ &&
               FALSE_IT(stats_params.set_collection_level(collection_level_))) {
    } else if (OpType::SET_RETENTION_PERIOD == op_type_ &&
               FALSE_IT(stats_params.set_retention_period(retention_period_))) {
    } else if (OB_FAIL(
                 ObMViewRefreshStatsParams::set_sys_defaults(trans, tenant_id_, stats_params))) {
      LOG_WARN("fail to set sys defaults", KR(ret), K(tenant_id_), K(stats_params));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }

  return ret;
}

int ObMViewStatsSetSystemDefaultExecutor::resolve_arg(const ObMViewStatsSetSystemDefaultArg &arg)
{
  int ret = OB_SUCCESS;
  if (0 == arg.parameter_name_.case_compare("COLLECTION_LEVEL")) {
    op_type_ = OpType::SET_COLLECTION_LEVEL;
    if (arg.collection_level_.empty()) {
      collection_level_ = ObMViewRefreshStatsParams::DEFAULT_COLLECTION_LEVEL;
    } else {
      if (OB_FAIL(
            ObMViewExecutorUtil::to_collection_level(arg.collection_level_, collection_level_))) {
        LOG_WARN("fail to cast collection level", KR(ret), K(arg));
      }
    }
  } else if (0 == arg.parameter_name_.case_compare("RETENTION_PERIOD")) {
    op_type_ = OpType::SET_RETENTION_PERIOD;
    if (INT64_MAX == arg.retention_period_) {
      retention_period_ = ObMViewRefreshStatsParams::DEFAULT_RETENTION_PERIOD;
    } else {
      if (OB_UNLIKELY(
            !ObMViewRefreshStatsParams::is_retention_period_valid(arg.retention_period_))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid retention period", KR(ret), K(arg));
      } else {
        retention_period_ = arg.retention_period_;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parameter name", KR(ret), K(arg));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
