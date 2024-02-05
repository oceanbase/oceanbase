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

#include "storage/mview/cmd/ob_mview_stats_purge_refresh_stats_executor.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mview_refresh_stats_purge.h"

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
    op_type_(OpType::MAX),
    retention_period_(INT64_MAX)
{
}

ObMViewStatsPurgeRefreshStatsExecutor::~ObMViewStatsPurgeRefreshStatsExecutor() {}

int ObMViewStatsPurgeRefreshStatsExecutor::execute(ObExecContext &ctx,
                                                   const ObMViewStatsPurgeRefreshStatsArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  CK(OB_NOT_NULL(session_info_ = ctx.get_my_session()));
  CK(OB_NOT_NULL(ctx.get_sql_proxy()));
  CK(OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*ctx.get_sql_ctx()->schema_guard_, session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObMViewExecutorUtil::check_min_data_version(
    tenant_id_, DATA_VERSION_4_3_0_0,
    "tenant's data version is below 4.3.0.0, purge refreh stats is"));
  OZ(resolve_arg(arg));

  if (OB_SUCC(ret)) {
    if (OpType::PURGE_ALL_REFRESH_STATS == op_type_) {
      ObMViewRefreshStats::FilterParam filter_param;
      if (INT64_MAX == retention_period_) {
        ObMViewRefreshStatsParams stats_params;
        if (OB_FAIL(ObMViewRefreshStatsParams::fetch_sys_defaults(*ctx.get_sql_proxy(), tenant_id_,
                                                                  stats_params))) {
          LOG_WARN("fail to fetch sys defaults", KR(ret), K(tenant_id_));
        } else {
          filter_param.set_retention_period(stats_params.get_retention_period());
        }
      } else if (retention_period_ > 0) {
        filter_param.set_retention_period(retention_period_);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(purge_refresh_stats(filter_param))) {
          LOG_WARN("fail to purge refresh stats", KR(ret), K(filter_param));
        }
      }
    } else if (OpType::PURGE_SPECIFY_REFRESH_STATS == op_type_) {
      if (mview_ids_.count() > 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported purge multiple mviews refresh stats", KR(ret), K(arg));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "purge multiple mviews refresh stats is");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < mview_ids_.count(); ++i) {
        const uint64_t mview_id = mview_ids_.at(i);
        ObMViewRefreshStats::FilterParam filter_param;
        filter_param.set_mview_id(mview_id);
        if (INT64_MAX == retention_period_) {
          ObMViewRefreshStatsParams stats_params;
          if (OB_FAIL(ObMViewRefreshStatsParams::fetch_mview_refresh_stats_params(
                *ctx.get_sql_proxy(), tenant_id_, mview_id, stats_params, true))) {
            if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
              LOG_WARN("fail to fetch mview refresh stats params", KR(ret), K_(tenant_id), K(mview_id));
            } else {
              ret = OB_ERR_MVIEW_NOT_EXIST;
              LOG_WARN("mview not exist", KR(ret), K_(tenant_id), K(mview_id));
            }
          } else {
            filter_param.set_retention_period(stats_params.get_retention_period());
          }
        } else if (retention_period_ > 0) {
          filter_param.set_retention_period(retention_period_);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(purge_refresh_stats(filter_param))) {
            LOG_WARN("fail to purge refresh stats", KR(ret), K(filter_param));
          }
        }
      }
    }
  }

  return ret;
}

int ObMViewStatsPurgeRefreshStatsExecutor::resolve_arg(const ObMViewStatsPurgeRefreshStatsArg &arg)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  }
  // resolve mv_list
  if (OB_SUCC(ret)) {
    ObArray<ObString> mview_names;
    op_type_ =
      arg.mv_list_.empty() ? OpType::PURGE_ALL_REFRESH_STATS : OpType::PURGE_SPECIFY_REFRESH_STATS;
    if (OB_FAIL(ObMViewExecutorUtil::split_table_list(arg.mv_list_, mview_names))) {
      LOG_WARN("fail to split table list", KR(ret), K(arg.mv_list_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mview_names.count(); ++i) {
      const ObString &mview_name = mview_names.at(i);
      ObString database_name, table_name;
      bool has_synonym = false;
      ObString new_db_name, new_tbl_name;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(cs_type, case_mode, lib::is_oracle_mode(),
                                                          mview_name, database_name, table_name))) {
        LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(mview_name));
        LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(mview_name.length()),
                       mview_name.ptr());
      } else if (database_name.empty() &&
                 FALSE_IT(database_name = session_info_->get_database_name())) {
      } else if (OB_UNLIKELY(database_name.empty())) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("No database selected", KR(ret));
      } else if (OB_FAIL(schema_checker_.get_table_schema_with_synonym(
                   tenant_id_, database_name, table_name, false /*is_index_table*/, has_synonym,
                   new_db_name, new_tbl_name, table_schema))) {
        LOG_WARN("fail to get table schema with synonym", KR(ret), K(database_name), K(table_name));
      } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!table_schema->is_materialized_view())) {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        LOG_WARN("mview not exist", KR(ret), K(database_name), K(table_name), KPC(table_schema));
      } else if (OB_FAIL(mview_ids_.push_back(table_schema->get_table_id()))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  // resolve retention_period
  if (OB_SUCC(ret)) {
    retention_period_ = arg.retention_period_;
  }
  return ret;
}

int ObMViewStatsPurgeRefreshStatsExecutor::purge_refresh_stats(
  const ObMViewRefreshStats::FilterParam &filter_param)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  do {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(ctx_->get_sql_proxy(), tenant_id_))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(ObMViewRefreshStatsPurgeUtil::purge_refresh_stats(
                 trans, tenant_id_, filter_param, affected_rows, PURGE_BATCH_COUNT))) {
      LOG_WARN("fail to purge refresh stats", KR(ret), K(tenant_id_), K(filter_param));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  } while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status()) && affected_rows > 0);
  return ret;
}

} // namespace storage
} // namespace oceanbase
