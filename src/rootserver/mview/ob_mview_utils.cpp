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
#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_utils.h"
#include "share/schema/ob_mview_info.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/ob_sql_utils.h"
#include "src/share/ob_ddl_common.h"
#include "src/share/ob_get_compat_mode.h"
#include "deps/oblib/src/lib/utility/utility.h"
#include "deps/oblib/src/common/object/ob_object.h"
#include "rootserver/ob_tenant_event_def.h"

namespace oceanbase
{
namespace rootserver
{

using namespace share::schema;
using namespace obrpc;
using namespace tenant_event;

int ObMViewUtils::create_inner_session(const uint64_t tenant_id,
                                       ObSchemaGetterGuard &schema_guard,
                                       sql::ObFreeSessionCtx &free_session_ctx,
                                       sql::ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session = nullptr;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  const ObTenantSchema *tenant_info = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("Failed to create sess id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(tenant_id, sid, proxy_sid,
                                                       ObTimeUtility::current_time(), session))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session = nullptr;
    LOG_WARN("Failed to create session", K(ret), K(sid), K(tenant_id));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null tenant schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_sys_variable(false, false))) {
    LOG_WARN("Failed to load default sys variable", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_configs_in_pc())) {
    LOG_WARN("Failed to load default configs in pc", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
    LOG_WARN("Failed to init tenant in session", K(ret), K(tenant_id));
  } else {
    LOG_INFO("is oracle mode", K(is_oracle_mode));
    session->set_inner_session();
    session->set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE
                                                   : ObCompatibilityMode::MYSQL_MODE);
    session->set_query_start_time(ObTimeUtil::current_time());
  }
  if (OB_FAIL(ret)) {
    release_inner_session(free_session_ctx, session);
  }

  return ret;
}

void ObMViewUtils::release_inner_session(sql::ObFreeSessionCtx &free_session_ctx,
                                         sql::ObSQLSessionInfo *&session)
{
  if (nullptr != session) {
    session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
    session = nullptr;
  }
}

ERRSIM_POINT_DEF(ERRSIM_CREATE_MLOG_RPC_ERROR);
int ObMViewUtils::submit_build_mlog_task(const uint64_t tenant_id,
                                        ObIArray<ObString> &mlog_columns,
                                        ObSchemaGetterGuard &schema_guard,
                                        const ObTableSchema *base_table_schema,
                                        int64_t &task_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(base_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(base_table_schema));
  } else {
    HEAP_VARS_2((ObCreateMLogArg, create_mlog_arg), (ObCreateMLogRes, create_mlog_res))
    {
      const ObDatabaseSchema *db_schema = nullptr;
      common::ObArenaAllocator allocator("ObMViewUtils");
      ObString base_table_name;
      ObString mlog_table_name;
      bool is_table_with_logic_pk = false;
      const ObSysVariableSchema *sys_variable_schema = nullptr;
      if (OB_FAIL(get_base_table_name_for_print(tenant_id, base_table_schema, schema_guard, base_table_name))) {
        LOG_WARN("failed to get real base table name", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_database_schema(
                     tenant_id, base_table_schema->get_database_id(), db_schema))) {
        LOG_WARN("failed to get db schema", KR(ret), K(base_table_schema->get_database_id()));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database does not exist", KR(ret), K(base_table_schema->get_database_id()));
      } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
        LOG_WARN("failed to get sys variable schema", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(sys_variable_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sys variable schema", KR(ret), KP(sys_variable_schema));
      } else if (OB_FAIL(ObTableSchema::build_mlog_table_name(
                     allocator, base_table_name, mlog_table_name, lib::is_oracle_mode(),
                     base_table_schema->has_mlog_table()))) {
        LOG_WARN("failed to build mlog table name", KR(ret), K(base_table_name));
      } else if (OB_FAIL(base_table_schema->is_table_with_logic_pk(schema_guard,
                                                                   is_table_with_logic_pk))) {
        LOG_WARN("failed to check if table has logic pk", KR(ret));
      } else {
        char buf[OB_MAX_PROC_ENV_LENGTH];
        int64_t pos = 0;
        const int64_t current_time =
            ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
        const int64_t mlog_dop = max(int64_t(MTL_CPU_COUNT() * 0.3), 1);
        create_mlog_arg.replace_if_exists_ = true;
        create_mlog_arg.database_name_ = db_schema->get_database_name();
        create_mlog_arg.table_name_ = base_table_name;
        create_mlog_arg.mlog_name_ = mlog_table_name;
        create_mlog_arg.tenant_id_ = tenant_id;
        create_mlog_arg.exec_tenant_id_ = tenant_id;
        create_mlog_arg.base_table_id_ = base_table_schema->get_table_id();
        create_mlog_arg.include_new_values_ = true;
        create_mlog_arg.with_sequence_ = true;
        create_mlog_arg.purge_options_.purge_mode_ = ObMLogPurgeMode::DEFERRED;
        create_mlog_arg.purge_options_.start_datetime_expr_.set_timestamp(current_time);
        create_mlog_arg.purge_options_.next_datetime_expr_ = "sysdate() + interval 60 second";
        create_mlog_arg.mlog_schema_.set_dop(mlog_dop);
        if (!is_table_with_logic_pk) {
          create_mlog_arg.with_primary_key_ = false;
          create_mlog_arg.with_rowid_ = true;
        } else {
          create_mlog_arg.with_primary_key_ = true;
          create_mlog_arg.with_rowid_ = false;
        }
        OZ(sql::ObExecEnv::gen_exec_env(*sys_variable_schema, buf, OB_MAX_PROC_ENV_LENGTH, pos));
        OX(create_mlog_arg.purge_options_.exec_env_.assign(buf, pos));
        for (int64_t i = 0; OB_SUCC(ret) && i < mlog_columns.count(); ++i) {
          if (OB_FAIL(create_mlog_arg.store_columns_.push_back(mlog_columns.at(i)))) {
            LOG_WARN("failed to push back column", KR(ret), K(i));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          // RS will check if the schema version of the base table is changed, if so, it means
          // a concurrent ddl has already altered the base table, then we need to retry the task
          // to ensure we generate the correct column list for the mlog
          ObBasedSchemaObjectInfo based_info;
          based_info.schema_id_ = base_table_schema->get_table_id();
          based_info.schema_type_ = ObSchemaType::TABLE_SCHEMA;
          based_info.schema_version_ = base_table_schema->get_schema_version();
          based_info.schema_tenant_id_ = tenant_id;
          if (OB_FAIL(create_mlog_arg.based_schema_object_infos_.push_back(based_info))) {
            LOG_WARN("fail to push back base info", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          int64_t ddl_rpc_timeout = 0;
          if (OB_FAIL(share::ObDDLUtil::get_ddl_rpc_timeout(
                  tenant_id, base_table_schema->get_table_id(), ddl_rpc_timeout))) {
            LOG_WARN("failed to get ddl rpc timeout", KR(ret));
          } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", KR(ret), KP(GCTX.rs_rpc_proxy_));
          } else if (OB_UNLIKELY(ERRSIM_CREATE_MLOG_RPC_ERROR)) {
            ret = OB_ERR_PARALLEL_DDL_CONFLICT;
            LOG_WARN("errsim ERRSIM_CREATE_MLOG_RPC_ERROR", KR(ret), K(create_mlog_arg));
          } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(GCTX.self_addr())
                                 .timeout(ddl_rpc_timeout)
                                 .create_mlog(create_mlog_arg, create_mlog_res))) {
            LOG_WARN("failed to create mlog", KR(ret), K(create_mlog_arg));
          } else if (OB_UNLIKELY(OB_INVALID_ID == create_mlog_res.mlog_table_id_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid mlog table id", KR(ret), K(create_mlog_res));
          } else if (OB_INVALID_VERSION == create_mlog_res.schema_version_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected schema version", KR(ret), K(create_mlog_res));
          } else {
            task_id = create_mlog_res.task_id_;
          }
          LOG_DEBUG("submit build mlog task", KR(ret), K(create_mlog_arg), K(create_mlog_res));
        }
      }
    }
  }

  return ret;
}

int ObMViewUtils::generate_mview_complete_refresh_sql(
                  const uint64_t tenant_id,
                  const int64_t mview_table_id,
                  const int64_t container_table_id,
                  ObSchemaGetterGuard &schema_guard,
                  const int64_t snapshot_version,
                  const uint64_t mview_target_data_sync_scn,
                  const int64_t execution_id,
                  const int64_t task_id,
                  const int64_t parallelism,
                  const bool use_schema_version_hint_for_src_table,
                  const ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos,
                  const ObString &mview_select_sql,
                  ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  using namespace oceanbase::share;
  const ObTableSchema *mview_table_schema = nullptr;
  const ObTableSchema *container_table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  ObSqlString src_table_schema_version_hint;
  src_table_schema_version_hint.reset();
  ObSqlString insert_hint_str;
  insert_hint_str.reset();
  ObArenaAllocator allocator("ObDDLMviewTmp");
  ObString database_name;
  ObString container_table_name;
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == mview_table_id ||
                  OB_INVALID_ID == container_table_id || snapshot_version <= 0 ||
                  execution_id < 0 || task_id <= 0 || based_schema_object_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(mview_table_id), K(container_table_id),
             K(snapshot_version), K(execution_id), K(task_id), K(based_schema_object_infos));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_table_id, mview_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mview_table_id));
  } else if (OB_ISNULL(mview_table_schema)) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("fail to get mview table schema", KR(ret), K(mview_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, container_table_id,
                     container_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(container_table_id));
  } else if (OB_ISNULL(container_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", KR(ret), K(container_table_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                     tenant_id, mview_table_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret), K(tenant_id),
             K(mview_table_schema->get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, database schema must not be nullptr", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                     tenant_id, mview_table_id, is_oracle_mode))) {
    LOG_WARN("check if oracle mode failed", KR(ret), K(mview_table_id));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     allocator, database_schema->get_database_name_str(),
                     database_name, is_oracle_mode))) {
    LOG_WARN("fail to generate new name with escape character", KR(ret),
             K(database_schema->get_database_name_str()), K(is_oracle_mode));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     allocator, container_table_schema->get_table_name_str(),
                     container_table_name, is_oracle_mode))) {
    LOG_WARN("fail to generate new name with escape character", KR(ret),
      K(container_table_schema->get_table_name_str()), K(is_oracle_mode));
  } else if (use_schema_version_hint_for_src_table &&
             OB_FAIL(ObMViewUtils::check_schema_version_and_generate_ddl_schema_hint(
                     tenant_id, mview_table_id, schema_guard, based_schema_object_infos,
                     is_oracle_mode, src_table_schema_version_hint))) {
    LOG_WARN("failed to generated mview ddl schema hint", KR(ret));
  } else {
    // generate mview complete refresh sql
    const bool nested_consistent_refresh = mview_target_data_sync_scn == OB_INVALID_SCN_VAL ? false : true;
    const int64_t real_parallelism = ObDDLUtil::get_real_parallelism(parallelism, true/*is mv refresh*/);
    const ObString &select_sql_string = mview_table_schema->get_view_schema().get_expand_view_definition_for_mv_str().empty() ?
      mview_table_schema->get_view_schema().get_view_definition_str() :
      mview_table_schema->get_view_schema().get_expand_view_definition_for_mv_str();
    std::string select_sql(mview_select_sql.ptr());
    std::string real_sql;
    ObSqlString insert_columns;
    if (nested_consistent_refresh) {
      if (mview_select_sql.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nested sync refresh with empty sql string", K(mview_select_sql), K(mview_table_id));
      } else if (OB_FAIL(ObMViewRefreshHelper::replace_all_snapshot_zero(
                         select_sql, snapshot_version, real_sql, is_oracle_mode))) {
        LOG_WARN("fail to replace snapshot", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_mview_insert_hint(
                       insert_hint_str, real_parallelism, execution_id, task_id,
                       true/*load_data_hint*/, true/*use_pdml_hint*/))) {
      LOG_WARN("failed to generate mview insert hint", KR(ret));
    } else if (OB_FAIL(extract_columns_from_schema(*container_table_schema, is_oracle_mode, insert_columns, allocator))) {
      LOG_WARN("failed to extract columns from container table schema", KR(ret));
    } else if (OB_FAIL(sql_string.assign("INSERT "))) {
      LOG_WARN("failed to assign sql string", KR(ret));
    } else if (OB_FAIL(sql_string.append(insert_hint_str.string()))) {
      LOG_WARN("failed to append insert hint", KR(ret));
    } else if (is_oracle_mode &&
               OB_FAIL(sql_string.append_fmt(" INTO \"%.*s\".\"%.*s\" ",
                       static_cast<int>(database_name.length()), database_name.ptr(),
                       static_cast<int>(container_table_name.length()), container_table_name.ptr()))) {
      LOG_WARN("failed to append insert hint", KR(ret), K(database_name), K(container_table_name));
    } else if (!is_oracle_mode &&
               OB_FAIL(sql_string.append_fmt(" INTO `%.*s`.`%.*s` ",
                       static_cast<int>(database_name.length()), database_name.ptr(),
                       static_cast<int>(container_table_name.length()), container_table_name.ptr()))) {
      LOG_WARN("failed to append insert hint", KR(ret), K(database_name), K(container_table_name));
    } else if (OB_FAIL(sql_string.append_fmt(" %.*s ",
                       static_cast<int>(insert_columns.length()), insert_columns.ptr()))) {
      LOG_WARN("failed to append select sql string", KR(ret));
    } else if (OB_FAIL(sql_string.append_fmt(" SELECT /*+ %.*s */ * FROM ",
                       static_cast<int>(src_table_schema_version_hint.length()), src_table_schema_version_hint.ptr()))) {
      LOG_WARN("failed to append select sql string", KR(ret));
    } else if (!nested_consistent_refresh &&
                OB_FAIL(sql_string.append_fmt(" (%.*s) ",
                        static_cast<int>(select_sql_string.length()), select_sql_string.ptr()))) {
      LOG_WARN("failed to append select sql string", KR(ret));
    } else if (nested_consistent_refresh &&
               OB_FAIL(sql_string.append_fmt(" (%.*s) ",
                       static_cast<int>(real_sql.length()), real_sql.c_str()))) {
      LOG_WARN("failed to append select sql string", KR(ret));
    } else if (nested_consistent_refresh) {
      // nested consistent refresh, no need to append snapshot version
    } else if (is_oracle_mode && OB_FAIL(sql_string.append_fmt(" as of scn %ld ;", snapshot_version))) {
      LOG_WARN("failed to append snapshot version", KR(ret));
    } else if (!is_oracle_mode && OB_FAIL(sql_string.append_fmt(" as of snapshot %ld ;", snapshot_version))) {
      LOG_WARN("failed to append snapshot version", KR(ret));
    }
    LOG_INFO("prepare mview complete refresh sql", K(sql_string));
  }
  return ret;
}

int ObMViewUtils::check_schema_version_and_generate_ddl_schema_hint(
                  const uint64_t tenant_id,
                  const uint64_t mview_table_id,
                  share::schema::ObSchemaGetterGuard &schema_guard,
                  const ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos,
                  const bool is_oracle_mode,
                  ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ObDDLMviewTmp");
  ObString database_name;
  ObString table_name;
  int64_t based_schema_version = OB_INVALID_VERSION;
  const ObSchema *schema_obj = nullptr;
  int64_t based_obj_schema_version = OB_INVALID_VERSION;
  ObSchemaType based_obj_schema_type = OB_MAX_SCHEMA;
  ARRAY_FOREACH(based_schema_object_infos, i) {
    const ObBasedSchemaObjectInfo &based_info = based_schema_object_infos.at(i);
    const ObObjectType ref_obj_type = ObMViewUtils::get_object_type_for_mview(based_info.schema_type_);
    if (OB_FAIL(ObMViewUtils::get_schema_object_from_dependency(tenant_id, schema_guard, based_info.schema_id_,
                ref_obj_type, schema_obj, based_obj_schema_version, based_obj_schema_type))) {
      LOG_WARN("fail to get schema object from dependency", KR(ret), K(based_info));
    } else if (OB_ISNULL(schema_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema object is nullptr", KR(ret), K(based_info));
    } else if (OB_UNLIKELY(based_obj_schema_version != based_info.schema_version_)) {
      ret = OB_OLD_SCHEMA_VERSION;
      LOG_WARN("based schema version is changed", KR(ret), K(based_info), KP(schema_obj));
    }
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH(based_schema_object_infos, i) {
      const ObBasedSchemaObjectInfo &based_info = based_schema_object_infos.at(i);
      const ObTableSchema *table_schema = nullptr;
      const ObDatabaseSchema *database_schema = nullptr;
      database_name.reset();
      table_name.reset();
      allocator.reuse();
      if (based_info.schema_type_ == ObSchemaType::TABLE_SCHEMA ||
          based_info.schema_type_ == ObSchemaType::VIEW_SCHEMA) {
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, based_info.schema_id_, table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(based_info));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", KR(ret), K(tenant_id), K(based_info));
        } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(),
                                                            database_schema))) {
          LOG_WARN("fail to get database schema", KR(ret), K(tenant_id),
                  K(table_schema->get_database_id()));
        } else if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, database schema must not be nullptr", KR(ret));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                          allocator, database_schema->get_database_name_str(), database_name,
                          is_oracle_mode))) {
          LOG_WARN("fail to generate new name with escape character", KR(ret),
                  K(database_schema->get_database_name_str()), K(is_oracle_mode));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                          allocator, table_schema->get_table_name_str(), table_name, is_oracle_mode))) {
          LOG_WARN("fail to generate new name with escape character", KR(ret),
                  K(table_schema->get_table_name_str()), K(is_oracle_mode));
        } else if (is_oracle_mode) {
          if (OB_FAIL(sql_string.append_fmt("ob_ddl_schema_version(\"%.*s\".\"%.*s\", %ld) ",
                                            static_cast<int>(database_name.length()), database_name.ptr(),
                                            static_cast<int>(table_name.length()), table_name.ptr(),
                                            based_info.schema_version_))) {
            LOG_WARN("append sql string failed", KR(ret), K(database_name),
                    K(table_name), K(based_info.schema_version_));
          }
        } else {
          if (OB_FAIL(sql_string.append_fmt("ob_ddl_schema_version(`%.*s`.`%.*s`, %ld) ",
                                            static_cast<int>(database_name.length()), database_name.ptr(),
                                            static_cast<int>(table_name.length()), table_name.ptr(),
                                            based_info.schema_version_))) {
            LOG_WARN("append sql string failed", KR(ret), K(database_name),
                    K(table_name), K(based_info.schema_version_));
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewUtils::get_base_table_name_for_print(const uint64_t tenant_id,
                                                const ObTableSchema *base_table_schema,
                                                ObSchemaGetterGuard &schema_guard,
                                                ObString &base_table_name)
{
  int ret = OB_SUCCESS;
  base_table_name.reset();

  if (OB_ISNULL(base_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(base_table_schema));
  } else if (base_table_schema->mv_container_table()) {
    uint64_t mview_id = OB_INVALID_ID;
    if (OB_FAIL(ObMViewInfo::get_mview_id_from_container_id(*GCTX.sql_proxy_, tenant_id, base_table_schema->get_table_id(), mview_id))) {
      LOG_WARN("failed to get mview id", KR(ret), K(tenant_id));
    } else {
      const ObTableSchema *mview_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_id, mview_schema))) {
        LOG_WARN("failed to get table schema", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(mview_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null mview schema", KR(ret), KP(mview_schema));
      } else {
        base_table_name = mview_schema->get_table_name();
      }
    }
  } else {
    base_table_name = base_table_schema->get_table_name();
  }

  return ret;
}

int ObMViewUtils::get_mlog_column_list_str(const ObIArray<ObString> &mlog_columns,
                                           ObSqlString &column_list_str)
{
  int ret = OB_SUCCESS;
  column_list_str.reset();

  if (OB_FAIL(column_list_str.assign_fmt("["))) {
    LOG_WARN("failed to assign column list str", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mlog_columns.count(); ++i) {
      if (0 != i) {
        if (OB_FAIL(column_list_str.append_fmt(", "))) {
          LOG_WARN("failed to append column name", KR(ret), K(mlog_columns.at(i)));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_list_str.append_fmt("%s", mlog_columns.at(i).ptr()))) {
        LOG_WARN("failed to append column name", KR(ret), K(mlog_columns.at(i)));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(column_list_str.append_fmt("]"))) {
      LOG_WARN("failed to append column list str", KR(ret));
    }
  }

  return ret;
}

// Helper function to check if a column is a hidden column
bool ObMViewUtils::is_hidden_column(const ObString &column_name)
{
  // Check for __MV_DEP_COL_0$$ pattern
  bool is_hidden = false;
  if (column_name.prefix_match("__MV_DEP_COL_") && column_name.suffix_match("$$")) {
    is_hidden = true;
  }
  // Check for OB_HIDDEN_PK_INCREMENT_COLUMN_NAME
  if (0 == column_name.case_compare(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME)) {
    is_hidden = true;
  }

  return is_hidden;
}

int ObMViewUtils::extract_columns_from_schema(const ObTableSchema &container_table_schema,
                                              const bool is_oracle_mode,
                                              ObSqlString &column_list_str,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  column_list_str.reset();

  if (!container_table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: invalid container table schema", KR(ret));
  } else {
    // Get all column IDs from the container table schema
    ObArray<uint64_t> column_ids;
    if (OB_FAIL(container_table_schema.get_column_ids(column_ids))) {
      LOG_WARN("failed to get column ids from container table schema", KR(ret));
    } else {
      // Format the result
      if (OB_FAIL(column_list_str.assign("("))) {
        LOG_WARN("failed to assign column list start", KR(ret));
      } else if (column_ids.empty()) {
        LOG_WARN("no columns found in container table schema", KR(ret));
      } else {
        int64_t added_column_count = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
          const ObColumnSchemaV2 *column_schema = container_table_schema.get_column_schema(column_ids.at(i));
          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", KR(ret), K(column_ids.at(i)));
          } else if (column_ids.at(i) < OB_APP_MIN_COLUMN_ID) {
            // not user defined column, skip it
            // LOG_INFO("extract columns from schema - skipping system column", K(column_ids.at(i)), K(OB_APP_MIN_COLUMN_ID));
          } else {
            if (added_column_count > 0) {
              if (OB_FAIL(column_list_str.append(", "))) {
                LOG_WARN("failed to append comma", KR(ret));
              }
            }
            if (OB_SUCC(ret)) {
              const ObString &column_name = column_schema->get_column_name_str();
              ObString escaped_column_name;
              // 使用ObSQLUtils::generate_new_name_with_escape_character处理转义
              if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                      allocator,
                      column_name,
                      escaped_column_name,
                      is_oracle_mode))) {
                LOG_WARN("fail to generate escape character", KR(ret), K(column_name), K(is_oracle_mode));
              } else if (is_oracle_mode) {
                // Oracle 模式：使用双引号包围列名
                if (OB_FAIL(column_list_str.append("\""))) {
                  LOG_WARN("failed to append oracle quote start", KR(ret));
                } else if (OB_FAIL(column_list_str.append(escaped_column_name))) {
                  LOG_WARN("failed to append oracle column name", KR(ret), K(escaped_column_name));
                } else if (OB_FAIL(column_list_str.append("\""))) {
                  LOG_WARN("failed to append oracle quote end", KR(ret));
                }
              } else {
                // MySQL 模式：使用反引号包围列名
                if (OB_FAIL(column_list_str.append("`"))) {
                  LOG_WARN("failed to append mysql quote start", KR(ret));
                } else if (OB_FAIL(column_list_str.append(escaped_column_name))) {
                  LOG_WARN("failed to append mysql column name", KR(ret), K(escaped_column_name));
                } else if (OB_FAIL(column_list_str.append("`"))) {
                  LOG_WARN("failed to append mysql quote end", KR(ret));
                }
              }
              if (OB_SUCC(ret)) {
                added_column_count++;
                // LOG_INFO("extract columns from schema", K(column_name), K(column_ids.at(i)));
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(column_list_str.append(")"))) {
          LOG_WARN("failed to append column list end", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObMViewAutoMlogEventInfo::set_base_table_name(const ObString &base_table_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, base_table_name, base_table_name_, true))) {
    LOG_WARN("failed to write string", KR(ret), K(base_table_name));
  }
  return ret;
}

int ObMViewAutoMlogEventInfo::set_related_create_mview_ddl(const ObString &related_create_mview_ddl)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, related_create_mview_ddl, related_create_mview_ddl_, true))) {
    LOG_WARN("failed to write string", KR(ret), K(related_create_mview_ddl));
  }
  return ret;
}

int ObMViewUtils::generate_mview_insert_hint(
                  ObSqlString &sql_string,
                  const int64_t parallelism,
                  const int64_t execution_id,
                  const int64_t task_id,
                  const bool load_data_hint,
                  const bool use_pdml_hint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_string.assign_fmt("/*+ monitor "))) {
    LOG_WARN("failed to assign sql string", KR(ret));
  } else if (load_data_hint &&
             OB_FAIL(sql_string.append_fmt(" append "))) {
    LOG_WARN("failed to append sql string", KR(ret));
  } else if (use_pdml_hint &&
             OB_FAIL(sql_string.append_fmt(" enable_parallel_dml parallel(%ld) use_px ",
                     parallelism))) {
    LOG_WARN("failed to append sql string", KR(ret));
  } else if (OB_FAIL(sql_string.append_fmt(" opt_param('ddl_execution_id', %ld) "
                     " opt_param('ddl_task_id', %ld) ",
                     execution_id, task_id))) {
    LOG_WARN("failed to append ddl opt param hint", KR(ret));
  } else if (OB_FAIL(sql_string.append_fmt(" */"))) {
    LOG_WARN("failed to append sql string", KR(ret));
  }
  return ret;
}

int ObMViewAutoMlogEventInfo::set_mlog_columns(const ObString &mlog_columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, mlog_columns, mlog_columns_, true))) {
    LOG_WARN("failed to write string", KR(ret), K(mlog_columns));
  }
  return ret;
}

void ObMViewAutoMlogEventInfo::submit_event()
{
  const int64_t cost_time = ObTimeUtility::current_time() - start_ts_;
  TENANT_EVENT(tenant_id_, MVIEW, AUTO_BUILD_MLOG, start_ts_, ret_, cost_time,
               base_table_id_, base_table_name_, old_mlog_id_, new_mlog_id_,
               mlog_columns_, related_create_mview_ddl_);
}

int ObMViewUtils::get_schema_object_from_dependency(
    const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t schema_id,
    const ObObjectType ref_obj_type,
    const share::schema::ObSchema *&schema_obj,
    int64_t &schema_version,
    share::schema::ObSchemaType &schema_type)
{
  int ret = OB_SUCCESS;
  // Initialize output parameters
  schema_obj = nullptr;
  schema_version = OB_INVALID_VERSION;
  schema_type = OB_MAX_SCHEMA;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (ObObjectType::VIEW == ref_obj_type && data_version < DATA_VERSION_4_3_5_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("referring views in materialized view definition is not supported before 4.3.5.0");
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "referring views in materialized view definition is");
  } else if ((ObObjectType::FUNCTION == ref_obj_type || ObObjectType::TYPE == ref_obj_type)
              && ((data_version < MOCK_DATA_VERSION_4_3_5_5) || (data_version >= DATA_VERSION_4_4_0_0 && data_version < DATA_VERSION_4_4_2_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("referring functions/types in materialized view definition is not supported before 4.3.5.5");
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "referring functions/types in materialized view definition is");
  } else {
    switch (ref_obj_type) {
      case ObObjectType::TABLE:
      case ObObjectType::VIEW: {
        const ObTableSchema *table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                  schema_id, table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("based table not exist", KR(ret), K(tenant_id), KP(table_schema));
        } else {
          schema_obj = table_schema;
          schema_version = table_schema->get_schema_version();
          schema_type = ref_obj_type == ObObjectType::TABLE ? ObSchemaType::TABLE_SCHEMA : ObSchemaType::VIEW_SCHEMA;
        }
        break;
      }
      case ObObjectType::FUNCTION: {
        const ObRoutineInfo *routine_info = nullptr;
        if (OB_FAIL(schema_guard.get_routine_info(tenant_id, schema_id, routine_info))) {
          LOG_WARN("fail to get udf info", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(routine_info)) {
          ret = OB_OBJECT_NAME_NOT_EXIST;
          LOG_WARN("udf info is nullptr", KR(ret), K(tenant_id), KP(routine_info));
        } else {
          schema_obj = routine_info;
          schema_version = routine_info->get_schema_version();
          schema_type = ObSchemaType::ROUTINE_SCHEMA;
        }
        break;
      }
      case ObObjectType::TYPE: {
        const ObUDTTypeInfo *udt_info = nullptr;
        if (OB_FAIL(schema_guard.get_udt_info(tenant_id, schema_id, udt_info))) {
          LOG_WARN("fail to get udf info", K(ret));
        } else if (OB_ISNULL(udt_info)) {
          ret = OB_OBJECT_NAME_NOT_EXIST;
          LOG_WARN("udt info is nullptr", KR(ret), K(tenant_id), KP(udt_info));
        } else {
          schema_obj = udt_info;
          schema_version = udt_info->get_schema_version();
          schema_type = ObSchemaType::UDT_SCHEMA;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid obj type", KR(ret), K(tenant_id), K(ref_obj_type));
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
