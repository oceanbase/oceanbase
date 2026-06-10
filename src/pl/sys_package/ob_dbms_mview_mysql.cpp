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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_mview_mysql.h"
#include "lib/worker.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_rpc_struct.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/cmd/ob_mview_purge_log_executor.h"
#include "storage/mview/cmd/ob_mview_refresh_executor.h"
#include "storage/mview/cmd/ob_mview_refresh_report_executor.h"
#include "storage/mview/cmd/ob_mview_explain_refresh_executor.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "share/ob_lob_access_utils.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
using namespace omt;
using namespace sql;
using namespace share::schema;
using namespace storage;

/*
PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 0);
*/
int ObDBMSMViewMysql::purge_log(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  if (2 != params.count()
      || !params.at(0).is_varchar()
      || !params.at(1).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for mlog purge", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObMViewPurgeLogArg purge_params;
    ObMViewPurgeLogExecutor purge_executor;
    // fill params
    purge_params.master_ = params.at(0).get_varchar();
    purge_params.purge_log_parallel_ = params.at(1).get_int();
    if (OB_FAIL(purge_executor.execute(ctx, purge_params))) {
      LOG_WARN("fail to execute mlog purge", KR(ret), K(purge_params));
    }
  }
  return ret;
}

/*
PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DERAULT FALSE); -- 4.3.5.3
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL); -- 4.3.5.3
*/
int ObDBMSMViewMysql::refresh(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;
  bool async = false;
  bool force = false;
  if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_sql_ctx())
      || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx.get_my_session()), K(ctx.get_sql_ctx()));
  } else if (OB_FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version below 4.3.0.0, not support mview", K(ret), K(data_version));
  } else {
    const int64_t p_count = params.count();
    const int64_t OLD_MAX_PARAM = ObDBMSMViewRefreshParam::NESTED_REFRESH_MODE + 1;
    if (p_count < OLD_MAX_PARAM) {
      if (p_count < ObDBMSMViewRefreshParam::NESTED
          || (data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0)
          || data_version >= DATA_VERSION_4_4_2_0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected params count", K(ret), K(p_count), K(data_version));
      } else {
        LOG_WARN("use lowere version of admin pkg", K(p_count));
      }
    } else if (p_count >= OLD_MAX_PARAM && p_count <= ObDBMSMViewRefreshParam::MAX_PARAM) {
      if (OB_UNLIKELY(!params.at(ObDBMSMViewRefreshParam::NESTED).is_tinyint())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for materialized view refresh", K(ret));
      }
      if (OB_SUCC(ret) && p_count > ObDBMSMViewRefreshParam::ASYNC) {
        if (!params.at(ObDBMSMViewRefreshParam::ASYNC).is_tinyint()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid async param type", K(ret));
        } else {
          async = params.at(ObDBMSMViewRefreshParam::ASYNC).get_bool();
        }
      }
      if (OB_SUCC(ret) && p_count > ObDBMSMViewRefreshParam::FORCE) {
        if (!params.at(ObDBMSMViewRefreshParam::FORCE).is_tinyint()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid force param type", K(ret));
        } else {
          force = params.at(ObDBMSMViewRefreshParam::FORCE).get_bool();
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params count", K(ret), K(p_count));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!params.at(ObDBMSMViewRefreshParam::MV_LIST).is_varchar() ||
             (!params.at(ObDBMSMViewRefreshParam::METHOD).is_null() &&
              !params.at(ObDBMSMViewRefreshParam::METHOD).is_varchar()) ||
             !params.at(ObDBMSMViewRefreshParam::REFRESH_PARALLEL).is_int32()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for materialized view refresh", K(ret));
  }
  // tenant_config must be read after tenant_id is populated (TENANT_CONF(OB_INVALID_TENANT_ID) always returns invalid).
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  const bool refresh_queuing_enabled = (data_version >= DATA_VERSION_4_4_2_2)
                              && tenant_config.is_valid()
                              && tenant_config->_enable_mview_refresh_queuing;
  // DBMS_SCHEDULER-driven refreshes must not block the scheduler worker — force async.
  if (OB_SUCC(ret) && nullptr != ctx.get_my_session()->get_job_info()) {
    async = true;
  }
  if (OB_FAIL(ret)) {
  } else if (refresh_queuing_enabled) {
    share::schema::ObMVRefreshMethod refresh_method = share::schema::ObMVRefreshMethod::MAX;
    rootserver::ObMViewMaintenanceService *mview_maintenance_service = MTL(rootserver::ObMViewMaintenanceService*);
    obrpc::ObScheduleMViewRefreshArg schedule_arg;
    obrpc::ObScheduleMViewRefreshResult schedule_result;
    schedule_arg.tenant_id_ = tenant_id;
    schedule_arg.run_user_id_ = ctx.get_my_session()->get_priv_user_id();
    schedule_arg.is_nested_ = params.count() > ObDBMSMViewRefreshParam::NESTED
                              && params.at(ObDBMSMViewRefreshParam::NESTED).get_bool();
    schedule_arg.refresh_parallel_ = params.at(ObDBMSMViewRefreshParam::REFRESH_PARALLEL).get_int();
    schedule_arg.force_ = force;
    schedule_arg.expire_ts_ = THIS_WORKER.get_timeout_ts();
    if (OB_ISNULL(mview_maintenance_service) ||
          OB_ISNULL(mview_maintenance_service->get_pending_task_manager())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mview maintenance service is null", KR(ret));
      } else if (OB_FAIL(ObMViewExecutorUtil::resolve_mview_list_and_method(
                          ctx.get_sql_ctx()->schema_guard_,
                          ctx.get_my_session(),
                          params.at(ObDBMSMViewRefreshParam::MV_LIST).get_varchar(),
                          params.at(ObDBMSMViewRefreshParam::METHOD).get_varchar(),
                          schedule_arg.mview_id_,
                          schedule_arg.refresh_method_))) {
        LOG_WARN("fail to resolve mview list and method", KR(ret));
      } else if (OB_INVALID_ID == schedule_arg.mview_id_) {
        // empty list: do nothing, return success
      } else if (OB_FAIL(mview_maintenance_service->get_pending_task_manager()->schedule_mview_refresh(
                            schedule_arg, schedule_result))) {
        LOG_WARN("fail to schedule mview refresh", KR(ret), K(schedule_arg));
      } else if (!async
                 && OB_FAIL(ObMViewExecutorUtil::wait_mview_refresh(ctx, tenant_id, schedule_result.refresh_id_, schedule_arg.mview_id_))) {
        LOG_WARN("fail to wait mview refresh", KR(ret), K(schedule_result.refresh_id_), K(schedule_arg.mview_id_));
      }
      LOG_TRACE("schedule mview refresh", KR(ret), K(schedule_arg), K(schedule_result));
  } else {
    ObMViewRefreshArg refresh_params;
    ObMViewRefreshExecutor refresh_executor;
    // fill params
    refresh_params.list_ =
            params.at(ObDBMSMViewRefreshParam::MV_LIST).get_varchar();
    refresh_params.method_ =
            params.at(ObDBMSMViewRefreshParam::METHOD).is_varchar() ?
            params.at(ObDBMSMViewRefreshParam::METHOD).get_varchar() : NULL;
    refresh_params.refresh_parallel_ =
            params.at(ObDBMSMViewRefreshParam::REFRESH_PARALLEL).get_int();
    refresh_params.nested_ = params.count() > ObDBMSMViewRefreshParam::NESTED
                             && params.at(ObDBMSMViewRefreshParam::NESTED).get_bool();
    if (OB_FAIL(refresh_executor.execute(ctx, refresh_params))) {
      LOG_WARN("fail to execute mview refresh", KR(ret), K(refresh_params));
    }
  }
  return ret;
}

/*
FUNCTION refresh_report(
    IN     refresh_id          INT            DEFAULT NULL,
    IN     mv_name             VARCHAR(65535) DEFAULT NULL,
    IN     tenant_id           INT            DEFAULT NULL,
    IN     format              VARCHAR(65535) DEFAULT 'TEXT')
RETURN TEXT;
*/
int ObDBMSMViewMysql::refresh_report(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t refresh_id = OB_INVALID_ID;
  ObString mv_name;
  ObString format;
  bool has_refresh_id = false;
  bool has_mv_name = false;
  if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret));
  } else if (ObDBMSMViewRefreshReportParam::REPORT_MAX_PARAM != params.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count", KR(ret), K(params.count()));
  }
  // parse refresh_id
  if (OB_FAIL(ret)) {
  } else if (!params.at(ObDBMSMViewRefreshReportParam::REPORT_REFRESH_ID).is_null()) {
    if (!params.at(ObDBMSMViewRefreshReportParam::REPORT_REFRESH_ID).is_int32()
        && !params.at(ObDBMSMViewRefreshReportParam::REPORT_REFRESH_ID).is_int()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("refresh_id must be int", KR(ret));
    } else {
      refresh_id = params.at(ObDBMSMViewRefreshReportParam::REPORT_REFRESH_ID).get_int();
      has_refresh_id = true;
    }
  }
  // parse mv_name
  if (OB_FAIL(ret)) {
  } else if (!params.at(ObDBMSMViewRefreshReportParam::REPORT_MV_NAME).is_null()) {
    if (!params.at(ObDBMSMViewRefreshReportParam::REPORT_MV_NAME).is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mv_name must be varchar", KR(ret));
    } else {
      mv_name = params.at(ObDBMSMViewRefreshReportParam::REPORT_MV_NAME).get_varchar();
      has_mv_name = true;
    }
  }
  // at least one of refresh_id / mv_name must be provided
  if (OB_FAIL(ret)) {
  } else if (!has_refresh_id && !has_mv_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("at least one of refresh_id or mv_name must be provided", KR(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "refresh_id and mv_name: at least one must be provided");
  }
  // parse tenant_id
  if (OB_FAIL(ret)) {
  } else if (params.at(ObDBMSMViewRefreshReportParam::REPORT_TENANT_ID).is_null()) {
    tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  } else if (!params.at(ObDBMSMViewRefreshReportParam::REPORT_TENANT_ID).is_int32()
             && !params.at(ObDBMSMViewRefreshReportParam::REPORT_TENANT_ID).is_int()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id must be int", KR(ret));
  } else {
    int64_t tid = params.at(ObDBMSMViewRefreshReportParam::REPORT_TENANT_ID).get_int();
    if (OB_UNLIKELY(tid <= 0 || !is_valid_tenant_id(static_cast<uint64_t>(tid)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_id", KR(ret), K(tid));
    } else {
      tenant_id = static_cast<uint64_t>(tid);
      if (tenant_id != ctx.get_my_session()->get_effective_tenant_id()
          && !is_sys_tenant(ctx.get_my_session()->get_effective_tenant_id())) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("only sys tenant can specify tenant_id", KR(ret), K(tenant_id));
      }
    }
  }
  // parse format
  if (OB_FAIL(ret)) {
  } else if (params.at(ObDBMSMViewRefreshReportParam::REPORT_FORMAT).is_null()) {
    format = ObString("TEXT");
  } else if (!params.at(ObDBMSMViewRefreshReportParam::REPORT_FORMAT).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("format must be varchar", KR(ret));
  } else {
    format = params.at(ObDBMSMViewRefreshReportParam::REPORT_FORMAT).get_varchar();
    if (0 != format.case_compare("TEXT") && 0 != format.case_compare("JSON")) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("format must be TEXT or JSON", KR(ret), K(format));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "format: must be 'TEXT' or 'JSON'");
    }
  }
  // execute report generation
  if (OB_SUCC(ret)) {
    ObMViewRefreshReportArg report_arg;
    ObMViewRefreshReportExecutor report_executor;
    ObSqlString report_text;
    report_arg.refresh_id_ = refresh_id;
    report_arg.mv_name_ = mv_name;
    report_arg.tenant_id_ = tenant_id;
    report_arg.format_ = format;
    report_arg.has_refresh_id_ = has_refresh_id;
    if (OB_FAIL(report_executor.execute(ctx, report_arg, report_text))) {
      LOG_WARN("fail to execute refresh report", KR(ret), K(report_arg));
    } else {
      ObTextStringResult text_res(ObTextType, true, &ctx.get_allocator());
      if (OB_FAIL(text_res.init(report_text.length()))) {
        LOG_WARN("fail to init text result", KR(ret), K(report_text.length()));
      } else if (OB_FAIL(text_res.append(report_text.ptr(), report_text.length()))) {
        LOG_WARN("fail to append report text", KR(ret));
      } else {
        ObString lob_str;
        text_res.get_result_buffer(lob_str);
        result.set_lob_value(ObTextType, lob_str.ptr(), lob_str.length());
        result.set_has_lob_header();
      }
    }
  }
  return ret;
}

/*
PROCEDURE kill(IN refresh_id BIGINT);
*/
int ObDBMSMViewMysql::kill(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;
  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_4_2_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("kill not supported in this version", KR(ret), K(data_version));
  } else if (1 != params.count() || !params.at(0).is_int()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for kill", KR(ret), K(params.count()));
  } else if (OB_FAIL(storage::ObMViewExecutorUtil::check_kill_refresh_privilege(ctx))) {
    LOG_WARN("fail to check kill privilege", KR(ret));
  } else {
    const int64_t refresh_id = params.at(0).get_int();
    rootserver::ObMViewMaintenanceService *service =
        MTL(rootserver::ObMViewMaintenanceService *);
    if (OB_ISNULL(service) || OB_ISNULL(service->get_pending_task_manager())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mview maintenance service or pending task manager is null", KR(ret));
    } else {
      obrpc::ObKillMViewRefreshArg arg;
      arg.tenant_id_ = tenant_id;
      arg.refresh_id_ = refresh_id;
      if (OB_FAIL(service->get_pending_task_manager()->kill_refresh(arg))) {
        LOG_WARN("fail to kill mview refresh", KR(ret), K(tenant_id), K(refresh_id));
      }
    }
  }
  return ret;
}

/*
PROCEDURE set_refresh_params(
    IN     mv_name                VARCHAR(65535),
    IN     parameter_name         VARCHAR(65535),
    IN     parameter_value        VARCHAR(65535));
*/
int ObDBMSMViewMysql::set_refresh_params(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  const ObTableSchema *table_schema = nullptr;
  ObSchemaChecker schema_checker;
  ObString mv_name;
  ObString database_name;
  ObString table_name;
  bool has_synonym = false;
  ObString new_db_name;
  ObString new_tbl_name;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_UNLIKELY(3 != params.count()
                  || !params.at(0).is_varchar()
                  || !params.at(1).is_varchar()
                  || !params.at(2).is_varchar())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for set_refresh_params", KR(ret));
  } else if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_sql_ctx())
             || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or schema guard is null", KR(ret));
  } else if (OB_FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_checker.init(*ctx.get_sql_ctx()->schema_guard_,
                                          ctx.get_my_session()->get_server_sid()))) {
    LOG_WARN("fail to init schema checker", KR(ret));
  } else if (OB_FALSE_IT(mv_name = params.at(0).get_varchar())) {
  } else if (OB_FAIL(ctx.get_my_session()->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(ctx.get_my_session()->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation connection", KR(ret));
  } else if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(
                 cs_type, case_mode, lib::is_oracle_mode(),
                 mv_name, database_name, table_name))) {
    LOG_WARN("fail to resolve table name", KR(ret), K(mv_name));
    LOG_USER_ERROR(OB_WRONG_TABLE_NAME,
                   static_cast<int>(mv_name.length()), mv_name.ptr());
  } else if (database_name.empty() &&
             FALSE_IT(database_name = ctx.get_my_session()->get_database_name())) {
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("No database selected", KR(ret));
  } else if (OB_FAIL(schema_checker.get_table_schema_with_synonym(
                 tenant_id, database_name, table_name, false /*is_index_table*/,
                 has_synonym, new_db_name, new_tbl_name, table_schema))) {
    LOG_WARN("fail to get table schema with synonym", KR(ret),
             K(database_name), K(table_name));
  } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!table_schema->is_materialized_view())) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("mview not exist", KR(ret), K(database_name), K(table_name));
  } else if (OB_FAIL(storage::ObMViewSchedJobUtils::set_mview_refresh_params(
                 tenant_id, table_schema->get_table_id(),
                 params.at(1).get_varchar(), params.at(2).get_varchar()))) {
    LOG_WARN("fail to set mview refresh params", KR(ret));
  }
  return ret;
}
/*
FUNCTION explain_refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     tenant_id              INT            DEFAULT 0
) RETURN TEXT;
*/
int ObDBMSMViewMysql::explain_refresh(ObPLExecCtx &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = ctx.get_exec_ctx();
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx or allocator is null", KR(ret));
  } else if (OB_UNLIKELY(4 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count for explain_refresh", KR(ret), K(params.count()));
  } else if (!params.at(0).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mv_name param", KR(ret));
  } else if (!params.at(1).is_null() && !params.at(1).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid method param", KR(ret));
  } else if (!params.at(2).is_tinyint()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested param", KR(ret));
  } else if (!params.at(3).is_int32() && !params.at(3).is_int()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id param", KR(ret));
  } else if (OB_UNLIKELY(params.at(3).get_int() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id param", KR(ret), K(params.at(3).get_int()));
  } else {
    ObMViewExplainRefreshArg explain_arg;
    ObMViewExplainRefreshExecutor executor;
    ObString result_str;
    explain_arg.list_ = params.at(0).get_varchar();
    explain_arg.method_ = params.at(1).is_varchar() ? params.at(1).get_varchar() : ObString();
    explain_arg.nested_ = params.at(2).get_bool();
    explain_arg.tenant_id_ = static_cast<uint64_t>(params.at(3).get_int());
    ObIAllocator &alloc = *ctx.allocator_;
    ObString lob_str;
    ObTextStringResult text_res(ObLongTextType, true, &alloc);
    if (OB_FAIL(executor.execute(*exec_ctx, explain_arg, alloc, result_str))) {
      LOG_WARN("fail to execute explain_refresh", KR(ret), K(explain_arg));
    } else if (OB_FAIL(text_res.init(result_str.length()))) {
      LOG_WARN("fail to init text result", KR(ret));
    } else if (OB_FAIL(text_res.append(result_str.ptr(), result_str.length()))) {
      LOG_WARN("fail to append text result", KR(ret));
    } else {
      text_res.get_result_buffer(lob_str);
      result.set_lob_value(ObLongTextType, lob_str.ptr(), lob_str.length());
      result.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      result.set_has_lob_header();
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
