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

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_trim_mlog_task.h"
#include "rootserver/mview/ob_mview_utils.h"
#include "observer/ob_server_struct.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_share_util.h"
#include "share/schema/ob_mlog_info.h"
#include "share/schema/ob_mview_info.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "share/schema/ob_dependency_info.h"

namespace oceanbase {
namespace rootserver {

using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;

ObMViewTrimMLogTask::ObMViewTrimMLogTask()
  : is_inited_(false),
    in_sched_(false),
    is_stop_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    last_trim_time_(0)
{
}

ObMViewTrimMLogTask::~ObMViewTrimMLogTask() {}

int ObMViewTrimMLogTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewTrimMLogTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }
  return ret;
}

int ObMViewTrimMLogTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewTrimMLogTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MVIEW_TRIM_MLOG_INTERVAL, true /*repeat*/))) {
      LOG_WARN("fail to schedule ObMViewTrimMLogTask", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewTrimMLogTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewTrimMLogTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObMViewTrimMLogTask::wait() { wait_task(); }

void ObMViewTrimMLogTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  ObSchemaGetterGuard schema_guard;
  ObSEArray<uint64_t, 16> mlog_ids;
  bool has_build_mview_task = false;
  sql::ObSQLSessionInfo *session = nullptr;
  sql::ObFreeSessionCtx free_session_ctx;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  const int64_t cur_timestamp = ObTimeUtil::current_time();
  const ObSysVariableSchema *sys_variable_schema = nullptr;
  bool is_oracle_mode = false;
  lib::Worker::CompatMode save_compat_mode = THIS_WORKER.get_compatibility_mode();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewTrimMLogTask not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop_)) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_4) {
  } else if (!tenant_config.is_valid() || !tenant_config->enable_mlog_auto_maintenance) {
  } else if (cur_timestamp - last_trim_time_ < tenant_config->mlog_trim_interval) {
  } else if (FALSE_IT(last_trim_time_ = cur_timestamp)) {
  } else if (OB_UNLIKELY(OB_ISNULL(sql_proxy))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy or mgr is null", KR(ret), K(sql_proxy));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(check_has_build_mview_task(has_build_mview_task))) {
    LOG_WARN("failed to check has build mview task", KR(ret), K(tenant_id_));
  } else if (has_build_mview_task) {
    LOG_INFO("mview is in construction, skip trim mlog");
  } else if (OB_FAIL(ObMLogInfo::batch_fetch_mlog_ids(*sql_proxy, tenant_id_, OB_INVALID_ID, mlog_ids))) {
    LOG_WARN("failed to fetch mlog ids", KR(ret));
  } else if (mlog_ids.empty()) {
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id_));
  } else if (NULL == sys_variable_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is NULL", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("get oracle mode failed", K(ret));
  } else {
    THIS_WORKER.set_compatibility_mode(is_oracle_mode ? Worker::CompatMode::ORACLE
                                                      : Worker::CompatMode::MYSQL);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMViewUtils::create_inner_session(tenant_id_, schema_guard, free_session_ctx, session))) {
    LOG_WARN("failed to create inner session", KR(ret));
  } else {
    const int64_t mlog_count = mlog_ids.count();
    for (int64_t i = 0; i < mlog_count; ++i) {
      const uint64_t mlog_id = mlog_ids.at(i);
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trim_mlog_impl(mlog_id, schema_guard, sql_proxy, session))) {
        LOG_WARN("failed to trim mlog", KR(tmp_ret), K(mlog_id));
      }
    }
  }
  THIS_WORKER.set_compatibility_mode(save_compat_mode);
  ObMViewUtils::release_inner_session(free_session_ctx, session);
}

int ObMViewTrimMLogTask::trim_mlog_impl(const uint64_t mlog_id,
                                        ObSchemaGetterGuard &schema_guard,
                                        common::ObISQLClient *sql_proxy,
                                        sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *mlog_schema = nullptr;
  const ObTableSchema *base_table_schema = nullptr;
  const ObDatabaseSchema *db_schema = nullptr;
  ObSEArray<uint64_t, 4> relevent_mviews;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, mlog_id, mlog_schema))) {
    LOG_WARN("failed to get mlog schema", KR(ret), K(mlog_id));
  } else if (OB_ISNULL(mlog_schema)) {
    // bypass
    LOG_WARN("mlog is dropped", KR(ret), K(mlog_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, mlog_schema->get_data_table_id(),
                                                   base_table_schema))) {
    LOG_WARN("failed to get base table schema", KR(ret), K(mlog_id));
  } else if (OB_ISNULL(base_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base table schema is null", KR(ret), K(mlog_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, mlog_schema->get_database_id(),
                                                      db_schema))) {
    LOG_WARN("failed to get db schema", KR(ret), K(tenant_id_), K(mlog_schema->get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database not exist", KR(ret), K(tenant_id_), K(mlog_schema->get_database_id()));
  } else if (OB_FAIL(get_relevent_mviews(sql_proxy, schema_guard, base_table_schema->get_table_id(), relevent_mviews))) {
    LOG_WARN("failed to get relevent mviews", KR(ret), K(tenant_id_), KPC(base_table_schema));
  } else if (relevent_mviews.empty()) {
    if (OB_FAIL(drop_mlog(mlog_schema, base_table_schema, db_schema))) {
      LOG_WARN("failed to drop mlog", KR(ret), K(mlog_id));
    }
  } else {
    if (OB_FAIL(replace_mlog(relevent_mviews, schema_guard, base_table_schema, mlog_schema, session))) {
      LOG_WARN("failed to replace mlog", KR(ret), K(mlog_id));
    }
  }

  LOG_INFO("trim mlog finished", KR(ret), K(mlog_id), K(relevent_mviews));

  return ret;
}

int ObMViewTrimMLogTask::drop_mlog(const ObTableSchema *mlog_schema,
                                   const ObTableSchema *base_table_schema,
                                   const ObDatabaseSchema *db_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(mlog_schema) || OB_ISNULL(base_table_schema) || OB_ISNULL(db_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mlog_schema), K(base_table_schema), K(db_schema));
  } else {
    HEAP_VARS_2((ObDropIndexArg, drop_index_arg), (ObDropIndexRes, drop_index_res))
    {
      ObAddr rs_addr;
      const int64_t DEFAULT_TIMEOUT_US = GCONF.internal_sql_execute_timeout;
      ObTimeoutCtx timeout_ctx;
      ObBasedSchemaObjectInfo based_info;
      drop_index_arg.tenant_id_ = tenant_id_;
      drop_index_arg.exec_tenant_id_ = tenant_id_;
      drop_index_arg.table_id_ = base_table_schema->get_table_id();
      drop_index_arg.index_table_id_ = mlog_schema->get_table_id();
      drop_index_arg.index_name_ = mlog_schema->get_table_name();
      drop_index_arg.session_id_ = base_table_schema->get_session_id();
      drop_index_arg.table_name_ = base_table_schema->get_table_name();
      drop_index_arg.database_name_ = db_schema->get_database_name_str();
      drop_index_arg.is_add_to_scheduler_ = true;
      drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_MLOG;
      based_info.schema_id_ = base_table_schema->get_table_id();
      based_info.schema_type_ = ObSchemaType::TABLE_SCHEMA;
      based_info.schema_version_ = base_table_schema->get_schema_version();
      based_info.schema_tenant_id_ = tenant_id_;
      if (OB_FAIL(drop_index_arg.based_schema_object_infos_.push_back(based_info))) {
        LOG_WARN("fail to push back base info", KR(ret));
      } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT_US))) {
        LOG_WARN("fail to set default timeout ctx", KR(ret));
      } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
        LOG_WARN("fail to rootservice address", KR(ret));
      } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr)
                             .timeout(timeout_ctx.get_timeout())
                             .drop_index(drop_index_arg, drop_index_res))) {
        LOG_WARN("failed to drop mlog", KR(ret), K(drop_index_arg));
      }
    }
  }

  return ret;
}

int ObMViewTrimMLogTask::replace_mlog(const ObIArray<uint64_t> &relevent_mviews,
                                      ObSchemaGetterGuard &schema_guard,
                                      const ObTableSchema *base_table_schema,
                                      const ObTableSchema *mlog_schema,
                                      sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> referenced_column_set;
  ObSEArray<uint64_t, 16> orig_column_ids;
  bool need_replace_mlog = false;

  if (OB_ISNULL(base_table_schema) || OB_ISNULL(mlog_schema) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(base_table_schema), K(mlog_schema), K(session));
  } else if (OB_FAIL(referenced_column_set.create(16, lib::ObMemAttr(tenant_id_, "TrimMLogTask")))) {
    LOG_WARN("failed to create hash set", KR(ret));
  }
  // retrieve referenced columns from all relevent mv tables
  for (int64_t i = 0; OB_SUCC(ret) && i < relevent_mviews.count(); ++i) {
    const uint64_t mview_id = relevent_mviews.at(i);
    sql::ObMVProvider mv_provider(tenant_id_, mview_id);
    if (OB_FAIL(mv_provider.get_columns_referenced_by_mv(tenant_id_, mview_id, base_table_schema->get_table_id(),
                                                         session, &schema_guard, referenced_column_set))) {
      LOG_WARN("failed to get referenced columns", KR(ret), K(tenant_id_), K(mview_id));
    }
  }
  // iterate through all columns in mlog, if any column is not referenced by any mv, then replace
  // the mlog to trim unused columns
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mlog_schema->get_column_ids(orig_column_ids))) {
      LOG_WARN("failed to get column ids", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !need_replace_mlog && i < orig_column_ids.count(); ++i) {
      uint64_t column_id = orig_column_ids.at(i);
      if (column_id < OB_MLOG_SEQ_NO_COLUMN_ID) {
        if (OB_FAIL(referenced_column_set.exist_refactored(column_id))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            need_replace_mlog = true;
          } else {
            LOG_WARN("failed to check exist", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_replace_mlog) {
    ObSEArray<ObString, 16> final_columns_list;
    FOREACH(it, referenced_column_set) {
      const uint64_t column_id = it->first;
      const ObColumnSchemaV2 *column_schema = base_table_schema->get_column_schema(column_id);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", KR(ret), K(column_id));
      } else if (column_schema->is_rowkey_column()) {
        // rowkey columns will be added automatically, ignore
      } else if (column_schema->is_generated_column()) {
        // mlog can not be built for generated columns, ignore
      } else if (OB_FAIL(final_columns_list.push_back(column_schema->get_column_name()))) {
        LOG_WARN("failed to add missing column", KR(ret), K(column_id));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t task_id = OB_INVALID_ID;
      if (OB_FAIL(ObMViewUtils::submit_build_mlog_task(tenant_id_, final_columns_list,
                                                       schema_guard, base_table_schema, task_id))) {
        LOG_WARN("failed to submit replace mlog task", KR(ret), K(final_columns_list));
      }
    }
  }

  return ret;
}

int ObMViewTrimMLogTask::check_has_build_mview_task(bool &has_build_mview_task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_string.assign_fmt(
            "SELECT EXISTS(SELECT 1 FROM %s WHERE ddl_type = %d) as has",
            OB_ALL_DDL_TASK_STATUS_TNAME, ObDDLType::DDL_CREATE_MVIEW))) {
      LOG_WARN("assign sql string failed", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id_, sql_string.ptr()))) {
      LOG_WARN("query ddl task record failed", K(ret), K(sql_string));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret), KP(result));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("result next failed", K(ret));
    } else {
      EXTRACT_BOOL_FIELD_MYSQL(*result, "has", has_build_mview_task);
    }
  }

  return ret;
}

int ObMViewTrimMLogTask::get_relevent_mviews(common::ObISQLClient *sql_proxy,
                                             ObSchemaGetterGuard &schema_guard,
                                             const uint64_t base_table_id,
                                             ObIArray<uint64_t> &relevent_mviews)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDependencyInfo, 16> dependency_infos;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObDependencyInfo::collect_dep_infos(tenant_id_, base_table_id, *sql_proxy, dependency_infos))) {
    LOG_WARN("failed to get dependency infos", KR(ret), K(tenant_id_), K(base_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dependency_infos.count(); ++i) {
      const ObDependencyInfo &dependency_info = dependency_infos.at(i);
      const uint64_t dep_obj_id = dependency_info.get_dep_obj_id();
      if (ObObjectType::VIEW == dependency_info.get_dep_obj_type()) {
        const ObTableSchema *mview_schema = nullptr;
        ObMViewInfo mview_info;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dep_obj_id, mview_schema))) {
          LOG_WARN("failed to get mview schema", KR(ret), K(tenant_id_), K(dep_obj_id));
        } else if (OB_ISNULL(mview_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mview schema is null", KR(ret), K(tenant_id_), K(dep_obj_id));
        } else if (!mview_schema->is_materialized_view()) {
          // ignore non-mview tables
        } else if (OB_FAIL(ObMViewInfo::fetch_mview_info(*sql_proxy, tenant_id_, dep_obj_id, mview_info))) {
          LOG_WARN("failed to fetch mview info", KR(ret), K(tenant_id_), K(dep_obj_id));
        } else if (!(ObMVRefreshMethod::FAST == mview_info.get_refresh_method() ||
                     ObMVRefreshMethod::FORCE == mview_info.get_refresh_method() ||
                     mview_schema->mv_on_query_computation())) {
          // only cares about fast/force refresh mviews and mviews on query computation
        } else if (OB_FAIL(relevent_mviews.push_back(dep_obj_id))) {
          LOG_WARN("failed to push back mview id", KR(ret), K(dep_obj_id));
        }
      }
    }
  }

  return ret;
}

} // namespace rootserver
} // namespace oceanbase
