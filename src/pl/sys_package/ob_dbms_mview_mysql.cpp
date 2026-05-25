/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_mview_mysql.h"
#include "storage/mview/cmd/ob_mview_purge_log_executor.h"
#include "storage/mview/cmd/ob_mview_refresh_executor.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
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
    IN     refresh_parallel       INT            DEFAULT 1,
    IN     nested                 BOOLEAN        DERAULT FALSE); -- 4.3.5.3
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL); -- 4.3.5.3
*/
int ObDBMSMViewMysql::refresh(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  uint64_t data_version;
  common::ObObj nested(false);
  ObString nested_refresh_mode;
  bool nested_consistent_refresh = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version below 4.3.0.0, not support mview", K(ret), K(data_version));
  } else {
    // TODO: MAX is temporarily changed to ASYNC for placeholder. Further handling is required when merging the feature.
    if (params.count() < ObDBMSMViewRefreshParam::ASYNC) {
      if (params.count() < ObDBMSMViewRefreshParam::NESTED
          || (data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0)
          || (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0)
          || data_version >= DATA_VERSION_4_5_1_0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected params count", K(ret), K(params.count()), K(data_version));
      } else {
        LOG_WARN("use lowere version of admin pkg", K(params.count()));
      }
      // TODO: MAX is temporarily changed to ASYNC for placeholder. Further handling is required when merging the feature.
    } else if (params.count() == ObDBMSMViewRefreshParam::ASYNC) {
      if (!params.at(ObDBMSMViewRefreshParam::NESTED).is_tinyint() ||
          (!params.at(ObDBMSMViewRefreshParam::NESTED_REFRESH_MODE).is_null() &&
           !params.at(ObDBMSMViewRefreshParam::NESTED_REFRESH_MODE).is_varchar())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for materialized view refresh", K(ret));
      } else {
        nested.reset();
        // copy params to obj
        params.at(ObDBMSMViewRefreshParam::NESTED).copy_value_or_obj(nested, true);
        if (!params.at(ObDBMSMViewRefreshParam::NESTED_REFRESH_MODE).is_null()) {
          if ((data_version < MOCK_DATA_VERSION_4_3_5_3
               || (data_version >= DATA_VERSION_4_4_0_0 && data_version < MOCK_DATA_VERSION_4_4_2_0)
               || (data_version >= DATA_VERSION_4_5_0_0 && data_version < DATA_VERSION_4_5_1_0))) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("data version below 4.3.5.3 or between 4.4.0.0 and 4.4.2.0 or between 4.5.0.0 and 4.5.1.0, not support nested refresh mode",
                     K(ret), K(data_version));
          } else if (!nested.get_bool()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("nested is false, invalid argument", K(ret));
          } else {
            nested_refresh_mode = params.at(ObDBMSMViewRefreshParam::NESTED_REFRESH_MODE).get_varchar();
            if (0 == nested_refresh_mode.case_compare(ObString("CONSISTENT"))) {
              nested_consistent_refresh = true;
            } else if (0 == nested_refresh_mode.case_compare(ObString("INCONSISTENT"))) {
              nested_consistent_refresh = false;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret), K(nested_refresh_mode));
            }
            LOG_INFO("get consistent param", KR(ret), K(nested_refresh_mode), K(nested_consistent_refresh));
          }
        } else {
          nested_consistent_refresh = false;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params count", K(ret), K(params.count()));
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
  if (OB_SUCC(ret)) {
    ObMViewRefreshArg refresh_params;
    ObMViewRefreshExecutor refresh_executor;
    refresh_params.list_ =
            params.at(ObDBMSMViewRefreshParam::MV_LIST).get_varchar();
    refresh_params.method_ =
            params.at(ObDBMSMViewRefreshParam::METHOD).is_varchar() ?
            params.at(ObDBMSMViewRefreshParam::METHOD).get_varchar() : NULL;
    refresh_params.refresh_parallel_ =
            params.at(ObDBMSMViewRefreshParam::REFRESH_PARALLEL).get_int();
    refresh_params.nested_ = nested.get_bool();
    refresh_params.nested_consistent_refresh_ = nested_consistent_refresh;
    if (OB_FAIL(refresh_executor.execute(ctx, refresh_params))) {
      LOG_WARN("fail to execute mview refresh", KR(ret), K(refresh_params));
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
} // namespace pl
} // namespace oceanbase
