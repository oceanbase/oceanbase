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
} // namespace rootserver
} // namespace oceanbase
