/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_refresh_report_executor.h"

#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/cmd/ob_mview_refresh_report.h"
#include "storage/mview/cmd/ob_mview_refresh_report_fetcher.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace sql;
using namespace share::schema;

ObMViewRefreshReportExecutor::ObMViewRefreshReportExecutor() {}

ObMViewRefreshReportExecutor::~ObMViewRefreshReportExecutor() {}

int ObMViewRefreshReportExecutor::execute(ObExecContext &ctx,
                                          const ObMViewRefreshReportArg &arg,
                                          ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  int64_t refresh_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(resolve_refresh_id_(ctx, arg, refresh_id))) {
    LOG_WARN("fail to resolve refresh id", KR(ret), K(arg));
  } else {
    ObArenaAllocator allocator("MvRefReport", OB_MALLOC_NORMAL_BLOCK_SIZE, arg.tenant_id_);
    MViewReportData data(allocator);
    MViewReportContext context;
    context.allocator_ = &allocator;
    context.data_ = &data;
    ObSQLSessionInfo *session_info = NULL;
    if (OB_ISNULL(session_info = ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", KR(ret));
    } else {
      uint64_t conn_tenant_id = session_info->get_effective_tenant_id();
      if (OB_FAIL(ObMViewRefreshReportFetcher::fetch_all(ctx, conn_tenant_id, arg.tenant_id_, refresh_id, context))) {
        LOG_WARN("fail to fetch report data", KR(ret), K(refresh_id));
      } else if (OB_FAIL(resolve_names(ctx, arg.tenant_id_, context))) {
        LOG_WARN("fail to resolve names", KR(ret), K(refresh_id));
      } else {
        MViewRefreshReport *report = NULL;
        void *mem = allocator.alloc(sizeof(MViewRefreshReport));
        if (OB_ISNULL(mem)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc report", KR(ret));
        } else {
          report = new (mem) MViewRefreshReport(allocator);
          if (OB_FAIL(report->build(context))) {
            LOG_WARN("fail to build report", KR(ret), K(refresh_id));
          } else {
            const ObTimeZoneInfo *sys_tz_info = session_info->get_timezone_info();
            if (0 == arg.format_.case_compare("JSON")) {
              if (OB_FAIL(report->append_json(report_text))) {
                LOG_WARN("fail to format json report", KR(ret), K(refresh_id));
              }
            } else if (OB_FAIL(report->append_text(report_text, sys_tz_info))) {
              LOG_WARN("fail to format text report", KR(ret), K(refresh_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewRefreshReportExecutor::resolve_mv_with_guard_(ObExecContext &ctx,
                                                         uint64_t target_tenant_id,
                                                         const ObString &mv_name,
                                                         ObSchemaGetterGuard &local_guard,
                                                         const ObTableSchema *&mv_schema,
                                                         ObString &out_db_name,
                                                         ObString &out_mv_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSchemaGetterGuard *guard = NULL;
  mv_schema = NULL;
  out_db_name.reset();
  out_mv_name.reset();
  if (OB_ISNULL(ctx.get_sql_proxy()) || OB_ISNULL(session_info = ctx.get_my_session())
      || OB_ISNULL(sql_ctx = ctx.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy / session / sql_ctx / schema_guard is null", KR(ret));
  } else if (session_info->get_effective_tenant_id() == target_tenant_id) {
    guard = sql_ctx->schema_guard_;
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(target_tenant_id, local_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(target_tenant_id));
  } else {
    guard = &local_guard;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(resolve_mv_name_to_schema(*session_info, *guard, target_tenant_id,
                                                mv_name, mv_schema, out_db_name, out_mv_name))) {
    LOG_WARN("fail to resolve mv_name to schema", KR(ret), K(target_tenant_id), K(mv_name));
  }
  return ret;
}

int ObMViewRefreshReportExecutor::resolve_refresh_id_(ObExecContext &ctx,
                                                      const ObMViewRefreshReportArg &arg,
                                                      int64_t &refresh_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard tenant_guard;
  if (arg.has_refresh_id_) {
    refresh_id = arg.refresh_id_;
  } else {
    const ObTableSchema *mv_schema = NULL;
    ObString db_name;
    ObString mv_name;
    if (OB_FAIL(resolve_mv_with_guard_(ctx, arg.tenant_id_, arg.mv_name_,
                                       tenant_guard, mv_schema, db_name, mv_name))) {
      LOG_WARN("fail to resolve mv with guard", KR(ret), K(arg));
    } else {
      const uint64_t mview_id = mv_schema->get_table_id();
      ObSQLSessionInfo *session_info = ctx.get_my_session();
      const uint64_t conn_tenant_id = session_info->get_effective_tenant_id();
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("SELECT refresh_id FROM %s "
                                 "WHERE tenant_id = %lu AND mview_id = %lu AND elapsed_time > 0 "
                                 "ORDER BY end_time DESC LIMIT 1",
                                 OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TNAME,
                                 arg.tenant_id_,
                                 mview_id))) {
        LOG_WARN("fail to build sql", KR(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res)
        {
          sqlclient::ObMySQLResult *sql_result = NULL;
          if (OB_FAIL(ctx.get_sql_proxy()->read(res, conn_tenant_id, sql.ptr()))) {
            LOG_WARN("fail to execute sql", KR(ret), K(sql));
          } else if (OB_ISNULL(sql_result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null sql result", KR(ret));
          } else if (OB_FAIL(sql_result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("no refresh record found for mv_name", KR(ret), K(arg.mv_name_));
              ObCStringHelper helper;
              LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(db_name), helper.convert(mv_name));
            } else {
              LOG_WARN("fail to get next", KR(ret));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*sql_result, "refresh_id", refresh_id, int64_t);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && arg.has_refresh_id_ && !arg.mv_name_.empty()) {
    ObSchemaGetterGuard validate_tenant_guard;
    const ObTableSchema *mv_schema = NULL;
    ObString db_name;
    ObString mv_name;
    if (OB_FAIL(resolve_mv_with_guard_(ctx, arg.tenant_id_, arg.mv_name_,
                                       validate_tenant_guard, mv_schema, db_name, mv_name))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("mv_name does not refer to an existing materialized view", KR(ret), K(arg.mv_name_));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "mv_name does not match the given refresh_id");
      }
    } else {
      const uint64_t mview_id_by_name = mv_schema->get_table_id();
      ObSQLSessionInfo *session_info = ctx.get_my_session();
      const uint64_t conn_tenant_id = session_info->get_effective_tenant_id();
      ObSqlString sql;
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *sql_result = NULL;
        if (OB_FAIL(sql.assign_fmt("SELECT mview_id FROM %s WHERE tenant_id = %lu AND refresh_id = %ld",
                                   OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TNAME,
                                   arg.tenant_id_,
                                   arg.refresh_id_))) {
          LOG_WARN("fail to build sql", KR(ret));
        } else if (OB_FAIL(ctx.get_sql_proxy()->read(res, conn_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", KR(ret), K(sql));
        } else if (OB_ISNULL(sql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null sql result", KR(ret));
        } else if (OB_FAIL(sql_result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("given refresh_id not found", KR(ret), K(arg.refresh_id_));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "refresh_id and mv_name do not match");
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else {
          uint64_t mview_id_by_refresh = OB_INVALID_ID;
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "mview_id", mview_id_by_refresh, uint64_t);
          if (OB_SUCC(ret) && mview_id_by_name != mview_id_by_refresh) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("mv_name does not match refresh_id", KR(ret), K(mview_id_by_name),
                     K(mview_id_by_refresh), K(arg.refresh_id_), K(arg.mv_name_));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "refresh_id and mv_name do not match");
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewRefreshReportExecutor::resolve_names(ObExecContext &ctx, uint64_t tenant_id, MViewReportContext &context)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObIAllocator *allocator = NULL;
  MViewReportData *data = NULL;
  ObSchemaGetterGuard tenant_guard;
  if (OB_ISNULL(sql_ctx = ctx.get_sql_ctx())
      || OB_ISNULL(session_info = ctx.get_my_session())
      || OB_ISNULL(allocator = context.allocator_) || OB_ISNULL(data = context.data_) || OB_ISNULL(data->run_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_ctx, session, allocator, data or run_data is null", KR(ret));
  } else if (OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", KR(ret));
  } else if (session_info->get_effective_tenant_id() == tenant_id) {
    schema_guard = sql_ctx->schema_guard_;
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, tenant_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else {
    schema_guard = &tenant_guard;
  }
  if (OB_FAIL(ret)) {
  } else if (0 == data->run_data_->mview_id_) {
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, data->run_data_->mview_id_, table_schema))) {
    LOG_WARN("fail to get target mv schema", KR(ret), K(tenant_id), K(data->run_data_->mview_id_));
  } else if (OB_ISNULL(table_schema)) {
  } else if (OB_FAIL(ob_write_string(*allocator, table_schema->get_table_name(), data->run_data_->mview_name_))) {
    LOG_WARN("fail to copy target mv name", KR(ret));
  }

  // Resolve run owner name via schema, analogous to table name resolution above.
  if (OB_FAIL(ret)) {
  } else if (0 != data->run_data_->run_user_id_) {
    const ObUserInfo *user_info = schema_guard->get_user_info(tenant_id,
                                                              static_cast<uint64_t>(data->run_data_->run_user_id_));
    if (OB_ISNULL(user_info)) {
    } else if (OB_FAIL(ob_write_string(*allocator, user_info->get_user_name(), data->run_data_->run_owner_))) {
      LOG_WARN("fail to copy user name", KR(ret), K(data->run_data_->run_user_id_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (data->mv_array_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data->mv_array_.count(); ++i) {
      MViewReportMVData &mv = data->mv_array_.at(i);
      const ObTableSchema *mv_table_schema = NULL;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id, mv.mview_id_, mv_table_schema))) {
        LOG_WARN("fail to get mv schema", KR(ret), K(tenant_id), K(mv.mview_id_));
      } else if (OB_ISNULL(mv_table_schema)) {
      } else if (OB_FAIL(ob_write_string(*allocator, mv_table_schema->get_table_name(), mv.mv_name_))) {
        LOG_WARN("fail to copy mv name", KR(ret));
      } else {
        const ObSimpleDatabaseSchema *db_schema = NULL;
        uint64_t db_id = mv_table_schema->get_database_id();
        if (OB_FAIL(schema_guard->get_database_schema(tenant_id, db_id, db_schema))) {
          LOG_WARN("fail to get db schema", KR(ret), K(tenant_id), K(db_id));
        } else if (OB_ISNULL(db_schema)) {
        } else if (OB_FAIL(ob_write_string(*allocator, db_schema->get_database_name_str(), mv.mv_owner_))) {
          LOG_WARN("fail to copy db name", KR(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (data->change_array_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data->change_array_.count(); ++i) {
      MViewReportChangeData &ch = data->change_array_.at(i);
      const ObTableSchema *ch_table_schema = NULL;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id, ch.detail_table_id_, ch_table_schema))) {
        LOG_WARN("fail to get change table schema", KR(ret), K(tenant_id), K(ch.detail_table_id_));
      } else if (OB_ISNULL(ch_table_schema)) {
      } else if (OB_FAIL(ob_write_string(*allocator, ch_table_schema->get_table_name(), ch.tbl_name_))) {
        LOG_WARN("fail to copy change tbl name", KR(ret));
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(ch_table_schema)) {
        const ObSimpleDatabaseSchema *db_schema = NULL;
        uint64_t db_id = ch_table_schema->get_database_id();
        if (OB_FAIL(schema_guard->get_database_schema(tenant_id, db_id, db_schema))) {
          LOG_WARN("fail to get db schema", KR(ret), K(tenant_id), K(db_id));
        } else if (OB_ISNULL(db_schema)) {
        } else if (OB_FAIL(ob_write_string(*allocator, db_schema->get_database_name_str(), ch.tbl_owner_))) {
          LOG_WARN("fail to copy change db name", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObMViewRefreshReportExecutor::resolve_mv_name_to_schema(
    ObSQLSessionInfo &session,
    ObSchemaGetterGuard &schema_guard,
    uint64_t tenant_id,
    const ObString &mv_name_input,
    const ObTableSchema *&mv_schema,
    ObString &out_db_name,
    ObString &out_mv_name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  mv_schema = NULL;
  out_db_name.reset();
  out_mv_name.reset();
  if (OB_FAIL(session.get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session.get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation connection", KR(ret));
  } else if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(cs_type,
                                                              case_mode,
                                                              lib::is_oracle_mode(),
                                                              mv_name_input,
                                                              out_db_name,
                                                              out_mv_name))) {
    LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(mv_name_input));
    LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(mv_name_input.length()), mv_name_input.ptr());
  } else if (out_db_name.empty() && FALSE_IT(out_db_name = session.get_database_name())) {
  } else if (OB_UNLIKELY(out_db_name.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("no database selected", KR(ret));
    LOG_USER_ERROR(OB_ERR_NO_DB_SELECTED);
  } else if (OB_UNLIKELY(out_mv_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mv_name is empty after splitting db", KR(ret), K(mv_name_input));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   out_db_name,
                                                   out_mv_name,
                                                   false,
                                                   mv_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(out_db_name), K(out_mv_name));
  } else if (OB_ISNULL(mv_schema) || !mv_schema->is_materialized_view()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("materialized view not found", KR(ret), K(tenant_id), K(out_db_name), K(out_mv_name));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
