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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/resolver/ob_cmd.h"
#include "sql/executor/ob_cmd_executor.h"
#include "lib/ob_name_def.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_drop_tenant_stmt.h"
#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "sql/resolver/ddl/ob_lock_tenant_stmt.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_drop_index_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/ddl/ob_drop_index_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_alter_database_stmt.h"
#include "sql/resolver/ddl/ob_drop_database_stmt.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_create_outline_stmt.h"
#include "sql/resolver/ddl/ob_alter_outline_stmt.h"
#include "sql/resolver/ddl/ob_drop_outline_stmt.h"
#include "sql/resolver/ddl/ob_create_routine_stmt.h"
#include "sql/resolver/ddl/ob_drop_routine_stmt.h"
#include "sql/resolver/ddl/ob_alter_routine_stmt.h"
#include "sql/resolver/ddl/ob_create_udt_stmt.h"
#include "sql/resolver/ddl/ob_drop_udt_stmt.h"
#include "sql/resolver/ddl/ob_create_package_stmt.h"
#include "sql/resolver/ddl/ob_alter_package_stmt.h"
#include "sql/resolver/ddl/ob_drop_package_stmt.h"
#include "sql/resolver/ddl/ob_trigger_stmt.h"
#include "sql/resolver/ddl/ob_rename_table_stmt.h"
#include "sql/resolver/ddl/ob_truncate_table_stmt.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ddl/ob_lock_table_stmt.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "sql/resolver/dcl/ob_drop_user_stmt.h"
#include "sql/resolver/dcl/ob_rename_user_stmt.h"
#include "sql/resolver/dcl/ob_lock_user_stmt.h"
#include "sql/resolver/dcl/ob_set_password_stmt.h"
#include "sql/resolver/dcl/ob_grant_stmt.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/resolver/dcl/ob_create_role_stmt.h"
#include "sql/resolver/dcl/ob_drop_role_stmt.h"
#include "sql/resolver/dcl/ob_alter_user_profile_stmt.h"
#include "sql/resolver/dcl/ob_alter_user_primary_zone_stmt.h"
#include "sql/resolver/tcl/ob_start_trans_stmt.h"
#include "sql/resolver/tcl/ob_end_trans_stmt.h"
#include "sql/resolver/tcl/ob_savepoint_stmt.h"
#include "sql/resolver/cmd/ob_bootstrap_stmt.h"
#include "sql/resolver/cmd/ob_kill_stmt.h"
#include "sql/resolver/cmd/ob_empty_query_stmt.h"
#include "sql/resolver/cmd/ob_resource_stmt.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/cmd/ob_get_diagnostics_stmt.h"
#include "sql/resolver/cmd/ob_clear_balance_task_stmt.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/resolver/cmd/ob_anonymous_block_stmt.h"
#include "sql/resolver/prepare/ob_prepare_stmt.h"
#include "sql/resolver/prepare/ob_execute_stmt.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
#include "sql/resolver/ddl/ob_rename_table_stmt.h"
#include "sql/resolver/ddl/ob_truncate_table_stmt.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ddl/ob_create_synonym_stmt.h"
#include "sql/resolver/ddl/ob_drop_synonym_stmt.h"
#include "sql/resolver/ddl/ob_analyze_stmt.h"
#include "sql/resolver/ddl/ob_create_func_stmt.h"
#include "sql/resolver/ddl/ob_drop_func_stmt.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/resolver/xa/ob_xa_stmt.h"
#include "sql/resolver/ddl/ob_optimize_stmt.h"
#include "sql/resolver/ddl/ob_create_profile_stmt.h"
#include "sql/resolver/ddl/ob_create_dblink_stmt.h"
#include "sql/resolver/ddl/ob_drop_dblink_stmt.h"
#include "sql/resolver/cmd/ob_create_restore_point_stmt.h"
#include "sql/resolver/cmd/ob_drop_restore_point_stmt.h"
#include "sql/resolver/ddl/ob_create_directory_stmt.h"
#include "sql/resolver/ddl/ob_drop_directory_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_empty_query_executor.h"
#include "sql/engine/cmd/ob_dcl_executor.h"
#include "sql/engine/cmd/ob_tcl_executor.h"
#include "sql/engine/cmd/ob_tenant_executor.h"
#include "sql/engine/cmd/ob_set_names_executor.h"
#include "sql/engine/cmd/ob_alter_system_executor.h"
#include "sql/engine/cmd/ob_set_password_executor.h"
#include "sql/engine/cmd/ob_tablegroup_executor.h"
#include "sql/engine/cmd/ob_database_executor.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "sql/engine/cmd/ob_table_executor.h"
#include "sql/engine/cmd/ob_index_executor.h"
#include "sql/engine/cmd/ob_resource_executor.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/engine/cmd/ob_user_cmd_executor.h"
#include "sql/engine/cmd/ob_outline_executor.h"
#include "sql/engine/cmd/ob_restore_executor.h"
#include "sql/engine/cmd/ob_routine_executor.h"
#include "sql/engine/cmd/ob_udt_executor.h"
#include "sql/engine/cmd/ob_synonym_executor.h"
#include "sql/engine/cmd/ob_package_executor.h"
#include "sql/engine/cmd/ob_trigger_executor.h"
#include "sql/engine/cmd/ob_analyze_executor.h"
#include "sql/engine/cmd/ob_udf_executor.h"
#include "sql/engine/cmd/ob_dblink_executor.h"
#include "sql/engine/cmd/ob_load_data_executor.h"
#include "sql/engine/cmd/ob_sequence_executor.h"
#include "sql/engine/cmd/ob_role_cmd_executor.h"
#include "sql/engine/cmd/ob_xa_executor.h"
#include "sql/engine/cmd/ob_profile_cmd_executor.h"
#include "sql/engine/cmd/ob_get_diagnostics_executor.h"
#include "sql/engine/cmd/ob_lock_table_executor.h"
#include "sql/engine/prepare/ob_prepare_executor.h"
#include "sql/engine/prepare/ob_execute_executor.h"
#include "sql/engine/prepare/ob_deallocate_executor.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "sql/engine/cmd/ob_directory_executor.h"
#include "sql/resolver/dcl/ob_alter_role_stmt.h"
#include "sql/resolver/ddl/ob_create_context_resolver.h"
#include "sql/resolver/ddl/ob_drop_context_resolver.h"
#include "sql/engine/cmd/ob_context_executor.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "sql/resolver/ddl/ob_create_keystore_stmt.h"
#include "sql/resolver/ddl/ob_alter_keystore_stmt.h"
#include "sql/resolver/ddl/ob_create_tablespace_stmt.h"
#include "sql/resolver/ddl/ob_alter_tablespace_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablespace_stmt.h"
#include "sql/engine/cmd/ob_keystore_cmd_executor.h"
#include "sql/engine/cmd/ob_tablespace_cmd_executor.h"
#endif
#ifdef OB_BUILD_AUDIT_SECURITY
#include "sql/resolver/ddl/ob_audit_stmt.h"
#include "sql/engine/cmd/ob_audit_executor.h"
#endif

namespace oceanbase
{
using namespace common;
namespace sql
{

// Do not inline the executor call to make sure ObCmdExecutor::execute() stack usage under 8K.
#define DEFINE_EXECUTE_CMD(Statement, Executor) \
        [&]() __attribute__((noinline)) { \
        Statement &stmt = *(static_cast<Statement*>(&cmd)); \
        Executor executor; \
        sql_text = (stmt.get_query_ctx() == NULL) ? "" : stmt.get_query_ctx()->get_sql_stmt();         \
        ret = executor.execute(ctx, stmt); \
        }()

int ObCmdExecutor::execute(ObExecContext &ctx, ObICmd &cmd)
{
  int ret = OB_SUCCESS;
  ObString sql_text;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  bool is_ddl_or_dcl_stmt = false;
  int64_t ori_query_timeout;
  int64_t ori_trx_timeout;

  if (ObStmt::is_ddl_stmt(static_cast<stmt::StmtType>(cmd.get_cmd_type()), true)
      || ObStmt::is_dcl_stmt(static_cast<stmt::StmtType>(cmd.get_cmd_type()))) {
    if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (stmt::T_VARIABLE_SET == static_cast<stmt::StmtType>(cmd.get_cmd_type())
        && !static_cast<ObVariableSetStmt*>(&cmd)->has_global_variable()) {
      // 只有 set global variable 才是 DDL 操作，session 级别的 variable 变更不是 DDL
      // do nothing
    } else {
      my_session->get_query_timeout(ori_query_timeout);
      my_session->get_tx_timeout(ori_trx_timeout);
      ObObj val;
      val.set_int(GCONF._ob_ddl_timeout);
      is_ddl_or_dcl_stmt = true;
      if (cmd.get_cmd_type() == stmt::T_TRUNCATE_TABLE
          && static_cast<ObTruncateTableStmt*>(&cmd)->is_truncate_oracle_temp_table()) {
        //truncate oracle temp table by delete inner sql, which is not a ddl
        //do not need adjust _ob_ddl_timeout
      } else if (OB_FAIL(my_session->update_sys_variable(
                         share::SYS_VAR_OB_QUERY_TIMEOUT, val))) {
        LOG_WARN("set sys variable failed", K(ret), K(val.get_int()));
      } else if (OB_FAIL(my_session->update_sys_variable(
                         share::SYS_VAR_OB_TRX_TIMEOUT, val))) {
        LOG_WARN("set sys variable failed", K(ret), K(val.get_int()));
      } else {
        ctx.get_physical_plan_ctx()->set_timeout_timestamp(
            my_session->get_query_start_time() + GCONF._ob_ddl_timeout);
        THIS_WORKER.set_timeout_ts(
            my_session->get_query_start_time() + GCONF._ob_ddl_timeout);
      }
      if (OB_SUCC(ret)) {
        // DDL 在向 RS 发 rpc 之前释放所持有的特定版本的 schema_mgr
        // 避免在排队的 DDL 始终占用槽位导致 RS 正在处理的 DDL 没有新槽位可用而相互死锁的问题
        if (stmt::T_CREATE_OUTLINE == static_cast<stmt::StmtType>(cmd.get_cmd_type())
            || stmt::T_ALTER_OUTLINE == static_cast<stmt::StmtType>(cmd.get_cmd_type())
          // create outline 和 alter outline 会在 execute 的时候继续使用 schema guard 生成逻辑计划
          // reset 延后到 ObCreateOutlineExecutor::execute 和 ObAlterOutlineExecutor::execute 里进行
            || (stmt::T_CREATE_TABLE == static_cast<stmt::StmtType>(cmd.get_cmd_type()))
          // ctas 需要在 execute_ctas 中使用 ObCreateTableStmt 中的 ObSelectStmt 拼出类似于 insert into select 的语句
          // 拼 SQL 过程中，ObSelectStmt 中的成员需要继续依赖从特定版本 schema guard 中获取的 schema
          // reset 延后到 ObCreateTableExecutor::execute 和 ObCreateTableExecutor::execute_cta 里进行
        ) {
        } else if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->reset())){
          LOG_WARN("schema_guard reset failed", K(ret));
        }
      }
    }
  }
  
  if (OB_SUCC(ret)) {
    switch (cmd.get_cmd_type()) {
      case stmt::T_CREATE_RESOURCE_POOL: {
        DEFINE_EXECUTE_CMD(ObCreateResourcePoolStmt, ObCreateResourcePoolExecutor);
        break;
      }
      case stmt::T_DROP_RESOURCE_POOL: {
        DEFINE_EXECUTE_CMD(ObDropResourcePoolStmt, ObDropResourcePoolExecutor);
        break;
      }
      case stmt::T_SPLIT_RESOURCE_POOL: {
        DEFINE_EXECUTE_CMD(ObSplitResourcePoolStmt, ObSplitResourcePoolExecutor);
        break;
      }
      case stmt::T_MERGE_RESOURCE_POOL: {
        DEFINE_EXECUTE_CMD(ObMergeResourcePoolStmt, ObMergeResourcePoolExecutor);
        break;
      }
      case stmt::T_ALTER_RESOURCE_POOL: {
        DEFINE_EXECUTE_CMD(ObAlterResourcePoolStmt, ObAlterResourcePoolExecutor);
        break;
      }
      case stmt::T_ALTER_RESOURCE_TENANT: {
        DEFINE_EXECUTE_CMD(ObAlterResourceTenantStmt, ObAlterResourceTenantExecutor);
        break;
      }
      case stmt::T_CREATE_RESOURCE_UNIT: {
        DEFINE_EXECUTE_CMD(ObCreateResourceUnitStmt, ObCreateResourceUnitExecutor);
        break;
      }
      case stmt::T_ALTER_RESOURCE_UNIT: {
        DEFINE_EXECUTE_CMD(ObAlterResourceUnitStmt, ObAlterResourceUnitExecutor);
        break;
      }
      case stmt::T_DROP_RESOURCE_UNIT: {
        DEFINE_EXECUTE_CMD(ObDropResourceUnitStmt, ObDropResourceUnitExecutor);
        break;
      }
      case stmt::T_CREATE_TENANT: {
        DEFINE_EXECUTE_CMD(ObCreateTenantStmt, ObCreateTenantExecutor);
        break;
      }
      case stmt::T_CREATE_STANDBY_TENANT: {
        DEFINE_EXECUTE_CMD(ObCreateTenantStmt, ObCreateStandbyTenantExecutor);
        break;
      }
#ifdef OB_BUILD_AUDIT_SECURITY
      case stmt::T_AUDIT: {
        DEFINE_EXECUTE_CMD(ObAuditStmt, ObAuditExecutor);
        break;
      }
#endif
      case stmt::T_DROP_TENANT: {
        DEFINE_EXECUTE_CMD(ObDropTenantStmt, ObDropTenantExecutor);
        break;
      }
      case stmt::T_MODIFY_TENANT: {
        DEFINE_EXECUTE_CMD(ObModifyTenantStmt, ObModifyTenantExecutor);
        break;
      }
      case stmt::T_LOCK_TENANT: {
        DEFINE_EXECUTE_CMD(ObLockTenantStmt, ObLockTenantExecutor);
        break;
      }
      case stmt::T_CREATE_VIEW: // fall through
      case stmt::T_CREATE_TABLE: {
        DEFINE_EXECUTE_CMD(ObCreateTableStmt, ObCreateTableExecutor);
        break;
      }
      case stmt::T_ALTER_TABLE: {
        DEFINE_EXECUTE_CMD(ObAlterTableStmt, ObAlterTableExecutor);
        break;
      }
      case stmt::T_START_TRANS: {
        DEFINE_EXECUTE_CMD(ObStartTransStmt, ObStartTransExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_END_TRANS: {
        DEFINE_EXECUTE_CMD(ObEndTransStmt, ObEndTransExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_CREATE_SAVEPOINT: {
        DEFINE_EXECUTE_CMD(ObCreateSavePointStmt, ObCreateSavePointExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_ROLLBACK_SAVEPOINT: {
        DEFINE_EXECUTE_CMD(ObRollbackSavePointStmt, ObRollbackSavePointExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_RELEASE_SAVEPOINT: {
        DEFINE_EXECUTE_CMD(ObReleaseSavePointStmt, ObReleaseSavePointExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_DROP_VIEW: // fall through
      case stmt::T_DROP_TABLE: {
        DEFINE_EXECUTE_CMD(ObDropTableStmt, ObDropTableExecutor);
        break;
      }
      case stmt::T_RENAME_TABLE: {
        DEFINE_EXECUTE_CMD(ObRenameTableStmt, ObRenameTableExecutor);
        break;
      }
      case stmt::T_TRUNCATE_TABLE: {
        DEFINE_EXECUTE_CMD(ObTruncateTableStmt, ObTruncateTableExecutor);
        break;
      }
      case stmt::T_VARIABLE_SET: {
        DEFINE_EXECUTE_CMD(ObVariableSetStmt, ObVariableSetExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_DIAGNOSTICS: {
        DEFINE_EXECUTE_CMD(ObGetDiagnosticsStmt, ObGetDiagnosticsExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_CREATE_DATABASE: {
        DEFINE_EXECUTE_CMD(ObCreateDatabaseStmt, ObCreateDatabaseExecutor);
        break;
      }
      case stmt::T_USE_DATABASE: {
        DEFINE_EXECUTE_CMD(ObUseDatabaseStmt, ObUseDatabaseExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_ALTER_DATABASE: {
        DEFINE_EXECUTE_CMD(ObAlterDatabaseStmt, ObAlterDatabaseExecutor);
        break;
      }
      case stmt::T_DROP_DATABASE: {
        DEFINE_EXECUTE_CMD(ObDropDatabaseStmt, ObDropDatabaseExecutor);
        break;
      }
      case stmt::T_CREATE_TABLEGROUP: {
        DEFINE_EXECUTE_CMD(ObCreateTablegroupStmt, ObCreateTablegroupExecutor);
        break;
      }
      case stmt::T_ALTER_TABLEGROUP: {
        DEFINE_EXECUTE_CMD(ObAlterTablegroupStmt, ObAlterTablegroupExecutor);
        break;
      }
      case stmt::T_DROP_TABLEGROUP: {
        DEFINE_EXECUTE_CMD(ObDropTablegroupStmt, ObDropTablegroupExecutor);
        break;
      }
      case stmt::T_CREATE_INDEX: {
        DEFINE_EXECUTE_CMD(ObCreateIndexStmt, ObCreateIndexExecutor);
        break;
      }
      case stmt::T_DROP_INDEX: {
        DEFINE_EXECUTE_CMD(ObDropIndexStmt, ObDropIndexExecutor);
        break;
      }
      case stmt::T_ALTER_VIEW: {
        break;
      }
      case stmt::T_CREATE_TABLE_LIKE: {
        DEFINE_EXECUTE_CMD(ObCreateTableLikeStmt, ObCreateTableLikeExecutor);
        break;
      }
      case stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN: {
        DEFINE_EXECUTE_CMD(ObFlashBackTableFromRecyclebinStmt, ObFlashBackTableFromRecyclebinExecutor);
        break;
      }
      case stmt::T_FLASHBACK_TABLE_TO_SCN: {
        DEFINE_EXECUTE_CMD(ObFlashBackTableToScnStmt, ObFlashBackTableToScnExecutor);
        break;
      }
      case stmt::T_FLASHBACK_INDEX: {
        DEFINE_EXECUTE_CMD(ObFlashBackIndexStmt, ObFlashBackIndexExecutor);
        break;
      }
      case stmt::T_FLASHBACK_DATABASE: {
        DEFINE_EXECUTE_CMD(ObFlashBackDatabaseStmt, ObFlashBackDatabaseExecutor);
        break;
      }
      case stmt::T_FLASHBACK_TENANT: {
        DEFINE_EXECUTE_CMD(ObFlashBackTenantStmt, ObFlashBackTenantExecutor);
        break;
      }
      case stmt::T_PURGE_TABLE: {
        DEFINE_EXECUTE_CMD(ObPurgeTableStmt, ObPurgeTableExecutor);
        break;
      }
      case stmt::T_PURGE_INDEX: {
        DEFINE_EXECUTE_CMD(ObPurgeIndexStmt, ObPurgeIndexExecutor);
        break;
      }
      case stmt::T_PURGE_DATABASE: {
        DEFINE_EXECUTE_CMD(ObPurgeDatabaseStmt, ObPurgeDatabaseExecutor);
        break;
      }
      case stmt::T_PURGE_TENANT: {
        DEFINE_EXECUTE_CMD(ObPurgeTenantStmt, ObPurgeTenantExecutor);
        break;
      }
      case stmt::T_PURGE_RECYCLEBIN: {
        DEFINE_EXECUTE_CMD(ObPurgeRecycleBinStmt, ObPurgeRecycleBinExecutor);
        break;
      }
      case stmt::T_OPTIMIZE_TABLE: {
        DEFINE_EXECUTE_CMD(ObOptimizeTableStmt, ObOptimizeTableExecutor);
        break;
      }
      case stmt::T_OPTIMIZE_TENANT: {
        DEFINE_EXECUTE_CMD(ObOptimizeTenantStmt, ObOptimizeTenantExecutor);
        break;
      }
      case stmt::T_OPTIMIZE_ALL: {
        DEFINE_EXECUTE_CMD(ObOptimizeAllStmt, ObOptimizeAllExecutor);
        break;
      }

      case stmt::T_HELP:
      case stmt::T_CREATE_USER: {
        DEFINE_EXECUTE_CMD(ObCreateUserStmt, ObCreateUserExecutor);
        break;
      }
      case stmt::T_ALTER_USER_PROFILE:
      case stmt::T_ALTER_USER: {
        DEFINE_EXECUTE_CMD(ObAlterUserProfileStmt, ObAlterUserProfileExecutor);
        break;
      }
      case stmt::T_ALTER_USER_PRIMARY_ZONE: {
        DEFINE_EXECUTE_CMD(ObAlterUserPrimaryZoneStmt, ObAlterUserPrimaryZoneExecutor);
        break;
      }

      case stmt::T_DROP_USER: {
        DEFINE_EXECUTE_CMD(ObDropUserStmt, ObDropUserExecutor);
        break;
      }
      case stmt::T_RENAME_USER: {
        DEFINE_EXECUTE_CMD(ObRenameUserStmt, ObRenameUserExecutor);
        break;
      }
      case stmt::T_SET_PASSWORD: {
        DEFINE_EXECUTE_CMD(ObSetPasswordStmt, ObSetPasswordExecutor);
        break;
      }
      case stmt::T_LOCK_USER: {
        DEFINE_EXECUTE_CMD(ObLockUserStmt, ObLockUserExecutor);
        break;
      }
      case stmt::T_SYSTEM_GRANT:
      case stmt::T_GRANT_ROLE:
      case stmt::T_GRANT: {
        DEFINE_EXECUTE_CMD(ObGrantStmt, ObGrantExecutor);
        break;
      }
      case stmt::T_SYSTEM_REVOKE:
      case stmt::T_REVOKE_ROLE:
      case stmt::T_REVOKE: {
        DEFINE_EXECUTE_CMD(ObRevokeStmt, ObRevokeExecutor);
        break;
      }
      case stmt::T_PREPARE: {
        DEFINE_EXECUTE_CMD(ObPrepareStmt, ObPrepareExecutor);
        break;
      }
      case stmt::T_EXECUTE: {
        // ps文本模式应该要返回结果，不能定义为cmd
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ps text shoudle be handled as normal query, not cmd", K(ret));
        // DEFINE_EXECUTE_CMD(ObExecuteStmt, ObExecuteExecutor);
        break;
      }
      case stmt::T_DEALLOCATE: {
        DEFINE_EXECUTE_CMD(ObDeallocateStmt, ObDeallocateExecutor);
        break;
      }
      case stmt::T_CHANGE_OBI:
      case stmt::T_SWITCH_MASTER:
      case stmt::T_SERVER_ACTION: {
        break;
      }
      case stmt::T_BOOTSTRAP: {
        DEFINE_EXECUTE_CMD(ObBootstrapStmt, ObBootstrapExecutor);
        break;
      }
      case stmt::T_ADMIN_SERVER: {
        DEFINE_EXECUTE_CMD(ObAdminServerStmt, ObAdminServerExecutor);
        break;
      }
      case stmt::T_ADMIN_ZONE: {
        DEFINE_EXECUTE_CMD(ObAdminZoneStmt, ObAdminZoneExecutor);
        break;
      }
      case stmt::T_FREEZE: {
        DEFINE_EXECUTE_CMD(ObFreezeStmt, ObFreezeExecutor);
        break;
      }
      case stmt::T_FLUSH_CACHE: {
        DEFINE_EXECUTE_CMD(ObFlushCacheStmt, ObFlushCacheExecutor);
        break;
      }
      case stmt::T_FLUSH_KVCACHE: {
        DEFINE_EXECUTE_CMD(ObFlushKVCacheStmt, ObFlushKVCacheExecutor);
        break;
      }
      case stmt::T_FLUSH_ILOGCACHE: {
        DEFINE_EXECUTE_CMD(ObFlushIlogCacheStmt, ObFlushIlogCacheExecutor);
        break;
      }
      case stmt::T_FLUSH_DAG_WARNINGS: {
        DEFINE_EXECUTE_CMD(ObFlushDagWarningsStmt, ObFlushDagWarningsExecutor);
        break;
      }
      case stmt::T_SWITCH_REPLICA_ROLE: {
        DEFINE_EXECUTE_CMD(ObSwitchReplicaRoleStmt, ObSwitchReplicaRoleExecutor);
        break;
      }
      case stmt::T_SWITCH_RS_ROLE: {
        DEFINE_EXECUTE_CMD(ObSwitchRSRoleStmt, ObSwitchRSRoleExecutor);
        break;
      }
      case stmt::T_REPORT_REPLICA: {
        DEFINE_EXECUTE_CMD(ObReportReplicaStmt, ObReportReplicaExecutor);
        break;
      }
      case stmt::T_RECYCLE_REPLICA: {
        DEFINE_EXECUTE_CMD(ObRecycleReplicaStmt, ObRecycleReplicaExecutor);
        break;
      }
      case stmt::T_ADMIN_MERGE: {
        DEFINE_EXECUTE_CMD(ObAdminMergeStmt, ObAdminMergeExecutor);
        break;
      }
      case stmt::T_ADMIN_RECOVERY: {
        DEFINE_EXECUTE_CMD(ObAdminRecoveryStmt, ObAdminRecoveryExecutor);
        break;
      }
      case stmt::T_CLEAR_ROOT_TABLE: {
        DEFINE_EXECUTE_CMD(ObClearRoottableStmt, ObClearRoottableExecutor);
        break;
      }
      case stmt::T_REFRESH_SCHEMA: {
        DEFINE_EXECUTE_CMD(ObRefreshSchemaStmt, ObRefreshSchemaExecutor);
        break;
      }
      case stmt::T_REFRESH_MEMORY_STAT: {
        DEFINE_EXECUTE_CMD(ObRefreshMemStatStmt, ObRefreshMemStatExecutor);
        break;
      }
      case stmt::T_WASH_MEMORY_FRAGMENTATION: {
        DEFINE_EXECUTE_CMD(ObWashMemFragmentationStmt, ObWashMemFragmentationExecutor);
        break;
      }
      case stmt::T_REFRESH_IO_CALIBRATION: {
        DEFINE_EXECUTE_CMD(ObRefreshIOCalibraitonStmt, ObRefreshIOCalibraitonExecutor);
        break;
      }
      case stmt::T_ALTER_SYSTEM_SET_PARAMETER: {
        DEFINE_EXECUTE_CMD(ObSetConfigStmt, ObSetConfigExecutor);
        break;
      }
      case stmt::T_ALTER_SYSTEM_SETTP: {
        DEFINE_EXECUTE_CMD(ObSetTPStmt, ObSetTPExecutor);
        break;
      }
      case stmt::T_CLEAR_LOCATION_CACHE: {
        DEFINE_EXECUTE_CMD(ObClearLocationCacheStmt, ObClearLocationCacheExecutor);
        break;
      }
      case stmt::T_RELOAD_UNIT: {
        DEFINE_EXECUTE_CMD(ObReloadUnitStmt, ObReloadUnitExecutor);
        break;
      }
      case stmt::T_RELOAD_SERVER: {
        DEFINE_EXECUTE_CMD(ObReloadServerStmt, ObReloadServerExecutor);
        break;
      }
      case stmt::T_RELOAD_ZONE: {
        DEFINE_EXECUTE_CMD(ObReloadZoneStmt, ObReloadZoneExecutor);
        break;
      }
      case stmt::T_CLEAR_MERGE_ERROR: {
        DEFINE_EXECUTE_CMD(ObClearMergeErrorStmt, ObClearMergeErrorExecutor);
        break;
      }
      case stmt::T_MIGRATE_UNIT: {
        DEFINE_EXECUTE_CMD(ObMigrateUnitStmt, ObMigrateUnitExecutor);
        break;
      }
      case stmt::T_ADD_ARBITRATION_SERVICE: {
        DEFINE_EXECUTE_CMD(ObAddArbitrationServiceStmt, ObAddArbitrationServiceExecutor);
        break;
      }
      case stmt::T_REMOVE_ARBITRATION_SERVICE: {
        DEFINE_EXECUTE_CMD(ObRemoveArbitrationServiceStmt, ObRemoveArbitrationServiceExecutor);
        break;
      }
      case stmt::T_REPLACE_ARBITRATION_SERVICE: {
        DEFINE_EXECUTE_CMD(ObReplaceArbitrationServiceStmt, ObReplaceArbitrationServiceExecutor);
        break;
      }
      case stmt::T_UPGRADE_VIRTUAL_SCHEMA: {
        DEFINE_EXECUTE_CMD(ObUpgradeVirtualSchemaStmt, ObUpgradeVirtualSchemaExecutor);
        break;
      }
      case stmt::T_ADMIN_UPGRADE_CMD: {
        DEFINE_EXECUTE_CMD(ObAdminUpgradeCmdStmt, ObAdminUpgradeCmdExecutor);
        break;
      }
      case stmt::T_ADMIN_ROLLING_UPGRADE_CMD: {
        DEFINE_EXECUTE_CMD(ObAdminRollingUpgradeCmdStmt, ObAdminRollingUpgradeCmdExecutor);
        break;
      }
      case stmt::T_RUN_JOB: {
        DEFINE_EXECUTE_CMD(ObRunJobStmt, ObRunJobExecutor);
        break;
      }
      case stmt::T_ADMIN_RUN_UPGRADE_JOB: {
        DEFINE_EXECUTE_CMD(ObRunUpgradeJobStmt, ObRunUpgradeJobExecutor);
        break;
      }
      case stmt::T_ADMIN_STOP_UPGRADE_JOB: {
        DEFINE_EXECUTE_CMD(ObStopUpgradeJobStmt, ObStopUpgradeJobExecutor);
        break;
      }
      case stmt::T_CANCEL_TASK: {
        DEFINE_EXECUTE_CMD(ObCancelTaskStmt, ObCancelTaskExecutor);
        break;
      }
      case stmt::T_SET_NAMES: {
        DEFINE_EXECUTE_CMD(ObSetNamesStmt, ObSetNamesExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_LOAD_DATA: {
        DEFINE_EXECUTE_CMD(ObLoadDataStmt, ObLoadDataExecutor);
        break;
      }
      case stmt::T_KILL: {
        DEFINE_EXECUTE_CMD(ObKillStmt, ObKillExecutor);
        break;
      }
      case stmt::T_EMPTY_QUERY: {
        DEFINE_EXECUTE_CMD(ObEmptyQueryStmt, ObEmptyQueryExecutor);
        break;
      }
      case stmt::T_LOCK_TABLE: {
        DEFINE_EXECUTE_CMD(ObLockTableStmt, ObLockTableExecutor);
        break;
      }
      case stmt::T_CREATE_OUTLINE: {
        DEFINE_EXECUTE_CMD(ObCreateOutlineStmt, ObCreateOutlineExecutor);
        break;
      }
      case stmt::T_ALTER_OUTLINE: {
        DEFINE_EXECUTE_CMD(ObAlterOutlineStmt, ObAlterOutlineExecutor);
        break;
      }
      case stmt::T_DROP_OUTLINE: {
        DEFINE_EXECUTE_CMD(ObDropOutlineStmt, ObDropOutlineExecutor);
        break;
      }
      case stmt::T_CREATE_ROUTINE: {
        DEFINE_EXECUTE_CMD(ObCreateRoutineStmt, ObCreateRoutineExecutor);
        break;
      }
      case stmt::T_ALTER_ROUTINE: {
        DEFINE_EXECUTE_CMD(ObAlterRoutineStmt, ObAlterRoutineExecutor);
        break;
      }
      case stmt::T_DROP_ROUTINE: {
        DEFINE_EXECUTE_CMD(ObDropRoutineStmt, ObDropRoutineExecutor);
        break;
      }
      case stmt::T_CALL_PROCEDURE: {
        DEFINE_EXECUTE_CMD(ObCallProcedureStmt, ObCallProcedureExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_ANONYMOUS_BLOCK: {
        DEFINE_EXECUTE_CMD(ObAnonymousBlockStmt, ObAnonymousBlockExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
#ifdef OB_BUILD_ORACLE_PL
      case stmt::T_CREATE_TYPE: {
        DEFINE_EXECUTE_CMD(ObCreateUDTStmt, ObCreateUDTExecutor);
        break;
      }
      case stmt::T_DROP_TYPE: {
        DEFINE_EXECUTE_CMD(ObDropUDTStmt, ObDropUDTExecutor);
        break;
      }
#endif
      case stmt::T_CREATE_PACKAGE: {
        DEFINE_EXECUTE_CMD(ObCreatePackageStmt, ObCreatePackageExecutor);
        break;
      }
      case stmt::T_ALTER_PACKAGE: {
        DEFINE_EXECUTE_CMD(ObAlterPackageStmt, ObAlterPackageExecutor);
        break;
      }
      case stmt::T_DROP_PACKAGE: {
        DEFINE_EXECUTE_CMD(ObDropPackageStmt, ObDropPackageExecutor);
        break;
      }
      case stmt::T_CREATE_TRIGGER: {
        DEFINE_EXECUTE_CMD(ObCreateTriggerStmt, ObCreateTriggerExecutor);
        break;
      }
      case stmt::T_DROP_TRIGGER: {
        DEFINE_EXECUTE_CMD(ObDropTriggerStmt, ObDropTriggerExecutor);
        break;
      }
      case stmt::T_ALTER_TRIGGER: {
        DEFINE_EXECUTE_CMD(ObAlterTriggerStmt, ObAlterTriggerExecutor);
        break;
      }
      case stmt::T_REFRESH_TIME_ZONE_INFO: {
        DEFINE_EXECUTE_CMD(ObRefreshTimeZoneInfoStmt, ObRefreshTimeZoneInfoExecutor);
        break;
      }
      case stmt::T_SET_DISK_VALID: {
        DEFINE_EXECUTE_CMD(ObSetDiskValidStmt, ObSetDiskValidExecutor);
        break;
      }
      case stmt::T_CREATE_SYNONYM: {
        DEFINE_EXECUTE_CMD(ObCreateSynonymStmt, ObCreateSynonymExecutor);
        break;
      }
      case stmt::T_DROP_SYNONYM: {
        DEFINE_EXECUTE_CMD(ObDropSynonymStmt, ObDropSynonymExecutor);
        break;
      }
      case stmt::T_CLEAR_BALANCE_TASK: {
        DEFINE_EXECUTE_CMD(ObClearBalanceTaskStmt, ObClearBalanceTaskExecutor);
        break;
      }
      case stmt::T_ANALYZE: {
        DEFINE_EXECUTE_CMD(ObAnalyzeStmt, ObAnalyzeExecutor);
        break;
      }
      case stmt::T_PHYSICAL_RESTORE_TENANT: {
        DEFINE_EXECUTE_CMD(ObPhysicalRestoreTenantStmt, ObPhysicalRestoreTenantExecutor);
        break;
      }
      case stmt::T_CHANGE_TENANT: {
        DEFINE_EXECUTE_CMD(ObChangeTenantStmt, ObChangeTenantExecutor);
        sql_text = ObString::make_empty_string();  // do not record
        break;
      }
      case stmt::T_CREATE_FUNC: {
        DEFINE_EXECUTE_CMD(ObCreateFuncStmt, ObCreateFuncExecutor);
        break;
      }
      case stmt::T_DROP_FUNC: {
        DEFINE_EXECUTE_CMD(ObDropFuncStmt, ObDropFuncExecutor);
        break;
      }
      case stmt::T_CREATE_SEQUENCE: {
        DEFINE_EXECUTE_CMD(ObCreateSequenceStmt, ObCreateSequenceExecutor);
        break;
      }
      case stmt::T_DROP_SEQUENCE: {
        DEFINE_EXECUTE_CMD(ObDropSequenceStmt, ObDropSequenceExecutor);
        if (OB_SUCC(ret)) {
          ObDropSequenceStmt &stmt = *(static_cast<ObDropSequenceStmt*>(&cmd));
          const uint64_t sequence_id = stmt.get_arg().get_sequence_id();
          if (OB_FAIL(my_session->drop_sequence_value_if_exists(sequence_id))) {
            LOG_WARN("failed to drop sequence value from session", K(ret));
          }
        }
        break;
      }
      case stmt::T_ALTER_SEQUENCE: {
        DEFINE_EXECUTE_CMD(ObAlterSequenceStmt, ObAlterSequenceExecutor);
        break;
      }
      case stmt::T_SWITCHOVER: {
        DEFINE_EXECUTE_CMD(ObSwitchTenantStmt, ObSwitchTenantExecutor);
        break;
      }
      case stmt::T_RECOVER: {
        DEFINE_EXECUTE_CMD(ObRecoverTenantStmt, ObRecoverTenantExecutor);
        break;
      }
      case stmt::T_SET_TABLE_COMMENT:
      case stmt::T_SET_COLUMN_COMMENT: {
        DEFINE_EXECUTE_CMD(ObAlterTableStmt, ObAlterTableExecutor);
        break;
      }
      case stmt::T_XA_START: {
        DEFINE_EXECUTE_CMD(ObXaStartStmt, ObXaStartExecutor);
        break;
      }
      case stmt::T_XA_END: {
        DEFINE_EXECUTE_CMD(ObXaEndStmt, ObXaEndExecutor);
        break;
      }
      case stmt::T_XA_PREPARE: {
        DEFINE_EXECUTE_CMD(ObXaPrepareStmt, ObXaPrepareExecutor);
        break;
      }
      case stmt::T_XA_COMMIT: {
        DEFINE_EXECUTE_CMD(ObXaCommitStmt, ObXaEndTransExecutor);
        break;
      }
      case stmt::T_XA_ROLLBACK: {
        DEFINE_EXECUTE_CMD(ObXaRollBackStmt, ObXaEndTransExecutor);
        break;
      }
      case stmt::T_ALTER_DISKGROUP_ADD_DISK: {
        DEFINE_EXECUTE_CMD(ObAddDiskStmt , ObAddDiskExecutor);
        break;
      }
      case stmt::T_ALTER_DISKGROUP_DROP_DISK: {
        DEFINE_EXECUTE_CMD(ObDropDiskStmt , ObDropDiskExecutor);
        break;
      }
      case stmt::T_CREATE_ROLE: {
        DEFINE_EXECUTE_CMD(ObCreateRoleStmt, ObCreateRoleExecutor);
        break;
      }
      case stmt::T_DROP_ROLE: {
        DEFINE_EXECUTE_CMD(ObDropRoleStmt, ObDropRoleExecutor);
        break;
      }
      case stmt::T_ALTER_ROLE: {
        DEFINE_EXECUTE_CMD(ObAlterRoleStmt, ObAlterRoleExecutor);
        break;
      }
      /*case stmt::T_SET_ROLE: {
        DEFINE_EXECUTE_CMD(ObSetRoutineStmt, ObSetRoleExecutor);
        break;
      }*/
#ifdef OB_BUILD_TDE_SECURITY
      case stmt::T_CREATE_KEYSTORE: {
        DEFINE_EXECUTE_CMD(ObCreateKeystoreStmt, ObCreateKeystoreExecutor);
        break;
      }
      case stmt::T_ALTER_KEYSTORE: {
        DEFINE_EXECUTE_CMD(ObAlterKeystoreStmt, ObAlterKeystoreExecutor);
        break;
      }
      case stmt::T_CREATE_TABLESPACE: {
        DEFINE_EXECUTE_CMD(ObCreateTablespaceStmt, ObCreateTablespaceExecutor);
        break;
      }
      case stmt::T_ALTER_TABLESPACE: {
        DEFINE_EXECUTE_CMD(ObAlterTablespaceStmt, ObAlterTablespaceExecutor);
        break;
      }
      case stmt::T_DROP_TABLESPACE: {
        DEFINE_EXECUTE_CMD(ObDropTablespaceStmt, ObDropTablespaceExecutor);
        break;
      }
#endif
      case stmt::T_CREATE_PROFILE:
      case stmt::T_ALTER_PROFILE:
      case stmt::T_DROP_PROFILE: {
        DEFINE_EXECUTE_CMD(ObUserProfileStmt , ObProfileDDLExecutor);
        break;
      }
      case stmt::T_ARCHIVE_LOG: {
        DEFINE_EXECUTE_CMD(ObArchiveLogStmt, ObArchiveLogExecutor);
        break;
      }
      case stmt::T_BACKUP_DATABASE: {
        DEFINE_EXECUTE_CMD(ObBackupDatabaseStmt, ObBackupDatabaseExecutor);
        break;
      }
      case stmt::T_CANCEL_RESTORE: {
        DEFINE_EXECUTE_CMD(ObCancelRestoreStmt, ObCancelRestoreExecutor);
        break;
      }
      case stmt::T_RECOVER_TABLE: {
        DEFINE_EXECUTE_CMD(ObRecoverTableStmt, ObRecoverTableExecutor);
        break;
      }
      case stmt::T_BACKUP_MANAGE: {
        DEFINE_EXECUTE_CMD(ObBackupManageStmt, ObBackupManageExecutor);
        break;
      }
      case stmt::T_BACKUP_CLEAN: {
        DEFINE_EXECUTE_CMD(ObBackupCleanStmt, ObBackupCleanExecutor);
        break;
      }
      case stmt::T_DELETE_POLICY: {
        DEFINE_EXECUTE_CMD(ObDeletePolicyStmt, ObDeletePolicyExecutor);
        break;
      }
      case stmt::T_BACKUP_KEY: {
        DEFINE_EXECUTE_CMD(ObBackupKeyStmt, ObBackupKeyExecutor);
        break;
      }
      case stmt::T_CREATE_DBLINK: {
        DEFINE_EXECUTE_CMD(ObCreateDbLinkStmt, ObCreateDbLinkExecutor);
        break;
      }
      case stmt::T_DROP_DBLINK: {
        DEFINE_EXECUTE_CMD(ObDropDbLinkStmt, ObDropDbLinkExecutor);
        break;
      }
      case stmt::T_BACKUP_BACKUPSET: {
        DEFINE_EXECUTE_CMD(ObBackupBackupsetStmt, ObBackupBackupsetExecutor);
        break;
      }
      case stmt::T_BACKUP_ARCHIVELOG: {
        DEFINE_EXECUTE_CMD(ObBackupArchiveLogStmt, ObBackupArchiveLogExecutor);
        break;
      }
      case stmt::T_BACKUP_SET_ENCRYPTION: {
        DEFINE_EXECUTE_CMD(ObBackupSetEncryptionStmt, ObBackupSetEncryptionExecutor);
        break;
      }
      case stmt::T_SET_REGION_NETWORK_BANDWIDTH: {
        DEFINE_EXECUTE_CMD(ObSetRegionBandwidthStmt, ObSetRegionBandwidthExecutor);
        break;
      }
      case stmt::T_BACKUP_SET_DECRYPTION: {
        DEFINE_EXECUTE_CMD(ObBackupSetDecryptionStmt, ObBackupSetDecryptionExecutor);
        break;
      }
      case stmt::T_ENABLE_SQL_THROTTLE: {
        DEFINE_EXECUTE_CMD(ObEnableSqlThrottleStmt, ObEnableSqlThrottleExecutor);
        break;
      }
      case stmt::T_DISABLE_SQL_THROTTLE: {
        DEFINE_EXECUTE_CMD(ObDisableSqlThrottleStmt, ObDisableSqlThrottleExecutor);
        break;
      }
      case stmt::T_CREATE_RESTORE_POINT: {
        DEFINE_EXECUTE_CMD(ObCreateRestorePointStmt, ObCreateRestorePointExecutor);
        break;
      }
      case stmt::T_DROP_RESTORE_POINT: {
        DEFINE_EXECUTE_CMD(ObDropRestorePointStmt, ObDropRestorePointExecutor);
        break;
      }
      case stmt::T_CREATE_DIRECTORY: {
        DEFINE_EXECUTE_CMD(ObCreateDirectoryStmt, ObCreateDirectoryExecutor);
        break;
      }
      case stmt::T_DROP_DIRECTORY: {
        DEFINE_EXECUTE_CMD(ObDropDirectoryStmt, ObDropDirectoryExecutor);
        break;
      }
      case stmt::T_BACKUP_BACKUPPIECE: {
        DEFINE_EXECUTE_CMD(ObBackupBackupPieceStmt, ObBackupBackupPieceExecutor);
        break;
      }
      case stmt::T_ADD_RESTORE_SOURCE: {
        DEFINE_EXECUTE_CMD(ObAddRestoreSourceStmt, ObAddRestoreSourceExecutor);
        break;
      }
      case stmt::T_CLEAR_RESTORE_SOURCE: {
        DEFINE_EXECUTE_CMD(ObClearRestoreSourceStmt, ObClearRestoreSourceExecutor);
        break;
      }
      case stmt::T_CREATE_CONTEXT: {
        DEFINE_EXECUTE_CMD(ObCreateContextStmt, ObCreateContextExecutor);
        break;
      }
      case stmt::T_DROP_CONTEXT: {
        DEFINE_EXECUTE_CMD(ObDropContextStmt, ObDropContextExecutor);
        break;
      }
      case stmt::T_CHECKPOINT_SLOG: {
        DEFINE_EXECUTE_CMD(ObCheckpointSlogStmt, ObCheckpointSlogExecutor);
        break;
      }
      case stmt::T_TABLE_TTL: {
        DEFINE_EXECUTE_CMD(ObTableTTLStmt, ObTableTTLExecutor);
        break;
      }
      case stmt::T_CS_DISKMAINTAIN:
      case stmt::T_TABLET_CMD:
      case stmt::T_SWITCH_ROOTSERVER:
      case stmt::T_SWITCH_UPDATESERVER:
      case stmt::T_CLUSTER_MANAGER:
      case stmt::T_DROP_MEMTABLE:
      case stmt::T_CLEAR_MEMTABLE:
      case stmt::T_ADD_UPDATESERVER:
      case stmt::T_DELETE_UPDATESERVER:
      case stmt::T_CHECK_ROOT_TABLE:
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknow cmd type", "cmd_type", cmd.get_cmd_type(), "T_MAX", T_MAX);
        break;
      }
    }
  }
  if (!sql_text.empty()) {
    SERVER_EVENT_ADD("sql", "execute_cmd",
                     "cmd_type", cmd.get_cmd_type(),
                     "sql_text", ObHexEscapeSqlStr(ctx.get_sql_ctx()->is_sensitive_ ?
                                                   ObString(OB_MASKED_STR) : sql_text),
                     "return_code", ret);
  }

  if (is_ddl_or_dcl_stmt) {
    // ddl/dcl 执行过程中修改了 session 的 query_timeout 和 trx_timeout，执行完需要还原回去
    int tmp_ret = ret;
    ObObj ori_query_timeout_obj;
    ObObj ori_trx_timeout_obj;
    ori_query_timeout_obj.set_int(ori_query_timeout);
    ori_trx_timeout_obj.set_int(ori_trx_timeout);
    if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (OB_ISNULL(ctx.get_task_exec_ctx().schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service_ is null", K(ret));
    } else if (OB_FAIL(my_session->update_sys_variable(
                       share::SYS_VAR_OB_QUERY_TIMEOUT,
                       ori_query_timeout_obj))) {
      LOG_WARN("set sys variable failed", K(ret),
                                          K(ori_query_timeout_obj.get_int()));
    } else if (OB_FAIL(my_session->update_sys_variable(
                       share::SYS_VAR_OB_TRX_TIMEOUT,
                       ori_trx_timeout_obj))) {
      LOG_WARN("set sys variable failed", K(ret),
                                          K(ori_trx_timeout_obj.get_int()));
    } else if (OB_FAIL(ctx.get_task_exec_ctx().schema_service_->get_tenant_schema_guard(
                       my_session->get_effective_tenant_id(),
                       *(ctx.get_sql_ctx()->schema_guard_)))) {
      LOG_WARN("failed to get schema guard", K(ret));
    }
    if (OB_FAIL(tmp_ret)) {
      ret = tmp_ret;
    }
  }

  return ret;
}

#undef DEFINE_EXECUTE_CMD

}/* ns sql*/
}/* ns oceanbase */
