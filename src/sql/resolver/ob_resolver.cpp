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

#define USING_LOG_PREFIX SQL_RESV
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_merge_resolver.h"
#include "sql/resolver/dml/ob_update_resolver.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/tcl/ob_start_trans_resolver.h"
#include "sql/resolver/tcl/ob_end_trans_resolver.h"
#include "sql/resolver/prepare/ob_prepare_resolver.h"
#include "sql/resolver/prepare/ob_execute_resolver.h"
#include "sql/resolver/prepare/ob_deallocate_resolver.h"
#include "sql/resolver/cmd/ob_bootstrap_resolver.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include "sql/resolver/ddl/ob_create_func_resolver.h"
#include "sql/resolver/ddl/ob_drop_func_resolver.h"
#include "sql/resolver/ddl/ob_rename_table_resolver.h"
#include "sql/resolver/ddl/ob_truncate_table_resolver.h"
#include "sql/resolver/ddl/ob_create_table_like_resolver.h"
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "sql/resolver/ddl/ob_drop_table_resolver.h"
#include "sql/resolver/ddl/ob_create_index_resolver.h"
#include "sql/resolver/ddl/ob_create_synonym_resolver.h"
#include "sql/resolver/ddl/ob_drop_synonym_resolver.h"
#include "sql/resolver/ddl/ob_drop_index_resolver.h"
#include "sql/resolver/ddl/ob_create_database_resolver.h"
#include "sql/resolver/ddl/ob_alter_database_resolver.h"
#include "sql/resolver/ddl/ob_use_database_resolver.h"
#include "sql/resolver/ddl/ob_drop_database_resolver.h"
#include "sql/resolver/ddl/ob_create_tenant_resolver.h"
#include "sql/resolver/ddl/ob_lock_tenant_resolver.h"
#include "sql/resolver/ddl/ob_modify_tenant_resolver.h"
#include "sql/resolver/ddl/ob_drop_tenant_resolver.h"
#include "sql/resolver/ddl/ob_create_tablegroup_resolver.h"
#include "sql/resolver/ddl/ob_alter_tablegroup_resolver.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_resolver.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ddl/ob_create_outline_resolver.h"
#include "sql/resolver/ddl/ob_alter_outline_resolver.h"
#include "sql/resolver/ddl/ob_drop_outline_resolver.h"
#include "sql/resolver/ddl/ob_create_routine_resolver.h"
#include "sql/resolver/ddl/ob_drop_routine_resolver.h"
#include "sql/resolver/ddl/ob_trigger_resolver.h"
#include "sql/resolver/ddl/ob_optimize_resolver.h"
#include "sql/resolver/ddl/ob_create_standby_tenant_resolver.h"
#include "ddl/ob_create_routine_resolver.h"
#include "ddl/ob_drop_routine_resolver.h"
#include "ddl/ob_alter_routine_resolver.h"
#include "sql/resolver/ddl/ob_create_package_resolver.h"
#include "sql/resolver/ddl/ob_alter_package_resolver.h"
#include "sql/resolver/ddl/ob_drop_package_resolver.h"
#include "sql/resolver/ddl/ob_flashback_resolver.h"
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "sql/resolver/ddl/ob_analyze_stmt_resolver.h"
#include "sql/resolver/ddl/ob_flashback_resolver.h"
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "sql/resolver/ddl/ob_create_sequence_resolver.h"
#include "sql/resolver/ddl/ob_alter_sequence_resolver.h"
#include "sql/resolver/ddl/ob_drop_sequence_resolver.h"
#include "sql/resolver/ddl/ob_set_comment_resolver.h"
#include "sql/resolver/ddl/ob_create_profile_resolver.h"
#include "sql/resolver/ddl/ob_lock_table_resolver.h"
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_update_resolver.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/dcl/ob_create_user_resolver.h"
#include "sql/resolver/dcl/ob_drop_user_resolver.h"
#include "sql/resolver/dcl/ob_rename_user_resolver.h"
#include "sql/resolver/dcl/ob_set_password_resolver.h"
#include "sql/resolver/dcl/ob_lock_user_resolver.h"
#include "sql/resolver/dcl/ob_grant_resolver.h"
#include "sql/resolver/dcl/ob_revoke_resolver.h"
#include "sql/resolver/dcl/ob_create_role_resolver.h"
#include "sql/resolver/dcl/ob_drop_role_resolver.h"
#include "sql/resolver/dcl/ob_alter_user_profile_resolver.h"
#include "sql/resolver/dcl/ob_alter_user_primary_zone_resolver.h"
#include "sql/resolver/tcl/ob_start_trans_resolver.h"
#include "sql/resolver/tcl/ob_end_trans_resolver.h"
#include "tcl/ob_savepoint_resolver.h"
#include "sql/resolver/cmd/ob_resource_resolver.h"
#include "sql/resolver/cmd/ob_variable_set_resolver.h"
#include "sql/resolver/cmd/ob_show_resolver.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "sql/resolver/cmd/ob_help_resolver.h"
#include "sql/resolver/cmd/ob_kill_resolver.h"
#include "sql/resolver/cmd/ob_set_names_resolver.h"
#include "sql/resolver/cmd/ob_set_transaction_resolver.h"
#include "sql/resolver/cmd/ob_bootstrap_resolver.h"
#include "sql/resolver/cmd/ob_empty_query_resolver.h"
#include "sql/resolver/cmd/ob_anonymous_block_resolver.h"
#include "sql/resolver/cmd/ob_call_procedure_resolver.h"
#include "sql/resolver/cmd/ob_load_data_resolver.h"
#include "sql/resolver/prepare/ob_execute_resolver.h"
#include "sql/resolver/prepare/ob_deallocate_resolver.h"
#include "sql/resolver/ddl/ob_flashback_resolver.h"
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "sql/resolver/ddl/ob_create_sequence_resolver.h"
#include "sql/resolver/ddl/ob_alter_sequence_resolver.h"
#include "sql/resolver/ddl/ob_drop_sequence_resolver.h"
#include "sql/resolver/ddl/ob_set_comment_resolver.h"
#include "sql/resolver/ddl/ob_create_dblink_resolver.h"
#include "sql/resolver/ddl/ob_drop_dblink_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_wrap_enum_set.h"
#include "sql/resolver/xa/ob_xa_start_resolver.h"
#include "sql/resolver/xa/ob_xa_end_resolver.h"
#include "sql/resolver/xa/ob_xa_prepare_resolver.h"
#include "sql/resolver/xa/ob_xa_commit_resolver.h"
#include "sql/resolver/xa/ob_xa_rollback_resolver.h"
#include "sql/resolver/cmd/ob_create_restore_point_resolver.h"
#include "sql/resolver/cmd/ob_drop_restore_point_resolver.h"
#include "sql/resolver/cmd/ob_get_diagnostics_resolver.h"
#include "sql/resolver/cmd/ob_switch_tenant_resolver.h"
#include "sql/resolver/dcl/ob_alter_role_resolver.h"
#include "sql/resolver/dml/ob_multi_table_insert_resolver.h"
#include "sql/resolver/ddl/ob_create_directory_resolver.h"
#include "sql/resolver/ddl/ob_drop_directory_resolver.h"
#include "pl/ob_pl_package.h"
#include "sql/resolver/ddl/ob_create_context_resolver.h"
#include "sql/resolver/ddl/ob_drop_context_resolver.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "sql/resolver/ddl/ob_create_tablespace_resolver.h"
#include "sql/resolver/ddl/ob_alter_tablespace_resolver.h"
#include "sql/resolver/ddl/ob_drop_tablespace_resolver.h"
#include "sql/resolver/ddl/ob_create_keystore_resolver.h"
#include "sql/resolver/ddl/ob_alter_keystore_resolver.h"
#endif
#ifdef OB_BUILD_ORACLE_PL
#include "sql/resolver/ddl/ob_create_udt_resolver.h"
#include "sql/resolver/ddl/ob_drop_udt_resolver.h"
#include "sql/resolver/ddl/ob_audit_resolver.h"
#endif
namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObResolver::ObResolver(ObResolverParams &params)
    : params_(params)
{

}

ObResolver::~ObResolver()
{
}

template <typename ResolverType>
int ObResolver::stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ResolverType, stmt_resolver, params) {
    if (OB_FAIL(stmt_resolver.resolve(parse_tree))) {
      LOG_WARN("execute stmt_resolver failed", K(ret), K(parse_tree.type_));
    }
    stmt = stmt_resolver.get_basic_stmt();
  }
  return ret;
}

template <typename SelectResolverType>
int ObResolver::select_stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(SelectResolverType, stmt_resolver, params) {
    stmt_resolver.set_calc_found_rows(true);
    stmt_resolver.set_has_top_limit(true);
    if (OB_FAIL(stmt_resolver.resolve(parse_tree))) {
      LOG_WARN("execute stmt_resolver failed", K(ret), K(parse_tree.type_));
    }
    stmt = stmt_resolver.get_basic_stmt();
  }
  return ret;
}

int ObResolver::resolve(IsPrepared if_prepared, const ParseNode &parse_tree, ObStmt *&stmt)
{
#define REGISTER_STMT_RESOLVER(name)                                   \
  do {                                                                 \
    ret = stmt_resolver_func<Ob##name##Resolver>(params_, *real_parse_tree, stmt); \
  } while (0)

#define REGISTER_SELECT_STMT_RESOLVER(name)                                   \
  do {                                                                        \
    ret = select_stmt_resolver_func<Ob##name##Resolver>(params_, parse_tree, stmt); \
  } while (0)

  int ret = OB_SUCCESS;
  const ParseNode *real_parse_tree = NULL;
  UNUSED(if_prepared);

  if (OB_ISNULL(params_.allocator_)
      || OB_ISNULL(params_.schema_checker_)
      || OB_ISNULL(params_.session_info_)
      || OB_ISNULL(params_.expr_factory_)
      || OB_ISNULL(params_.query_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator or schema checker or session_info or query_ctx is NULL",
             K(params_.allocator_), K(params_.schema_checker_),
             K(params_.session_info_), K(params_.query_ctx_), KP(params_.expr_factory_));
  } else if (T_SP_PRE_STMTS == parse_tree.type_) {
    pl::ObPLPackageGuard package_guard(params_.session_info_->get_effective_tenant_id());
    pl::ObPLResolver resolver(*(params_.allocator_),
                              *(params_.session_info_),
                              *(params_.schema_checker_->get_schema_guard()),
                              package_guard,
                              *(params_.sql_proxy_),
                              *(params_.expr_factory_),
                              NULL,
                              params_.is_prepare_protocol_);
    OZ (resolver.resolve_condition_compile(&parse_tree, real_parse_tree, params_.query_ctx_->question_marks_count_));
  } else {
    real_parse_tree = &parse_tree;
  }

  if (OB_SUCC(ret)) {
    params_.resolver_scope_stmt_type_ = parse_tree.type_;
    params_.session_info_->set_stmt_type(
      ObResolverUtils::get_stmt_type_by_item_type(parse_tree.type_));
    ObExecContext *exec_ctx = params_.session_info_->get_cur_exec_ctx();
    if (exec_ctx != nullptr && exec_ctx->get_sql_ctx() != nullptr) {
      //save the current stmt type on my context,
      //some other modules may need to get the current SQL stmt type through its context
      ObSqlCtx *sql_ctx = exec_ctx->get_sql_ctx();
      sql_ctx->stmt_type_ = ObResolverUtils::get_stmt_type_by_item_type(parse_tree.type_);
    }

    switch (real_parse_tree->type_) {
      case T_CREATE_RESOURCE_UNIT: {
        REGISTER_STMT_RESOLVER(CreateResourceUnit);
        break;
      }
      case T_ALTER_RESOURCE_UNIT: {
        REGISTER_STMT_RESOLVER(AlterResourceUnit);
        break;
      }
      case T_DROP_RESOURCE_UNIT: {
        REGISTER_STMT_RESOLVER(DropResourceUnit);
        break;
      }
      case T_CREATE_RESOURCE_POOL: {
        REGISTER_STMT_RESOLVER(CreateResourcePool);
        break;
      }
      case T_DROP_RESOURCE_POOL: {
        REGISTER_STMT_RESOLVER(DropResourcePool);
        break;
      }
      case T_ALTER_RESOURCE_POOL: {
        REGISTER_STMT_RESOLVER(AlterResourcePool);
        break;
      }
      case T_SPLIT_RESOURCE_POOL: {
        REGISTER_STMT_RESOLVER(SplitResourcePool);
        break;
      }
      case T_MERGE_RESOURCE_POOL: {
        REGISTER_STMT_RESOLVER(MergeResourcePool);
        break;
      }
      case T_ALTER_RESOURCE_TENANT: {
        REGISTER_STMT_RESOLVER(AlterResourceTenant);
        break;
      }
      case T_CREATE_TENANT: {
        REGISTER_STMT_RESOLVER(CreateTenant);
        break;
      }
      case T_CREATE_STANDBY_TENANT: {
        REGISTER_STMT_RESOLVER(CreateStandbyTenant);
        break;
      }
      case T_DROP_TENANT: {
        REGISTER_STMT_RESOLVER(DropTenant);
        break;
      }
      case T_MODIFY_TENANT: {
        REGISTER_STMT_RESOLVER(ModifyTenant);
        break;
      }
      case T_LOCK_TENANT: {
        REGISTER_STMT_RESOLVER(LockTenant);
        break;
      }
      case T_CREATE_TABLE: {
        REGISTER_STMT_RESOLVER(CreateTable);
        break;
      }
#ifdef OB_BUILD_AUDIT_SECURITY
      case T_AUDIT: {
        REGISTER_STMT_RESOLVER(Audit);
        break;
      }
#endif
      case T_CREATE_FUNC: {
        REGISTER_STMT_RESOLVER(CreateFunc);
        break;
      }
      case T_ALTER_TABLE: {
        REGISTER_STMT_RESOLVER(AlterTable);
        break;
      }
      case T_CREATE_INDEX: {
        REGISTER_STMT_RESOLVER(CreateIndex);
        break;
      }
      case T_CREATE_VIEW: {
        REGISTER_STMT_RESOLVER(CreateView);
        break;
      }
      case T_ALTER_VIEW: {
#if 0
        ObAlterViewResolver stmt_resolver(params_);
        if (OB_FAIL(stmt_resolver.resolve(*node))) {
          LOG_WARN("execute alter view resolver failed", K(ret));
        }

        stmt = stmt_resolver.get_basic_stmt();
#endif
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support to alter view");
	      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter view");
        break;
      }
      case T_DROP_VIEW:
      case T_DROP_TABLE: {
        REGISTER_STMT_RESOLVER(DropTable);
        break;
      }
      case T_DROP_INDEX: {
        REGISTER_STMT_RESOLVER(DropIndex);
        break;
      }
      case T_CREATE_TABLE_LIKE: {
        REGISTER_STMT_RESOLVER(CreateTableLike);
        break;
      }
      case T_SELECT: {
        REGISTER_SELECT_STMT_RESOLVER(Select);
        break;
      }
      case T_INSERT: {
        REGISTER_STMT_RESOLVER(Insert);
        break;
      }
      case T_MULTI_INSERT: {
        REGISTER_STMT_RESOLVER(MultiTableInsert);
        break;
      }
      case T_MERGE: {
        REGISTER_STMT_RESOLVER(Merge);
        break;
      }
      case T_UPDATE: {
        REGISTER_STMT_RESOLVER(Update);
        break;
      }
      case T_DELETE: {
        REGISTER_STMT_RESOLVER(Delete);
        break;
      }
      case T_BEGIN: {
        REGISTER_STMT_RESOLVER(StartTrans);
        break;
      }
      case T_ROLLBACK:
        //fall through
      case T_COMMIT: {
        REGISTER_STMT_RESOLVER(EndTrans);
        break;
      }
      case T_BOOTSTRAP: {
        REGISTER_STMT_RESOLVER(Bootstrap);
        break;
      }
      case T_FREEZE: {
        REGISTER_STMT_RESOLVER(Freeze);
        break;
      }
      case T_FLUSH_CACHE: {
        REGISTER_STMT_RESOLVER(FlushCache);
        break;
      }
      case T_FLUSH_KVCACHE: {
        REGISTER_STMT_RESOLVER(FlushKVCache);
        break;
      }
      case T_FLUSH_ILOGCACHE: {
        REGISTER_STMT_RESOLVER(FlushIlogCache);
        break;
      }
      case T_FLUSH_DAG_WARNINGS: {
        REGISTER_STMT_RESOLVER(FlushDagWarnings);
        break;
      }
      case T_SWITCH_REPLICA_ROLE: {
        REGISTER_STMT_RESOLVER(SwitchReplicaRole);
        break;
      }
      case T_SWITCH_RS_ROLE: {
        REGISTER_STMT_RESOLVER(SwitchRSRole);
        break;
      }
      case T_SWITCHOVER: {
        REGISTER_STMT_RESOLVER(SwitchTenant);
        break;
      }
      case T_RECOVER: {
        REGISTER_STMT_RESOLVER(RecoverTenant);
        break;
      }
      case T_REPORT_REPLICA: {
        REGISTER_STMT_RESOLVER(ReportReplica);
        break;
      }
      case T_RECYCLE_REPLICA: {
        REGISTER_STMT_RESOLVER(RecycleReplica);
        break;
      }
      case T_MERGE_CONTROL: {
        REGISTER_STMT_RESOLVER(AdminMerge);
        break;
      }
      case T_RECOVERY_CONTROL: {
        REGISTER_STMT_RESOLVER(AdminRecovery);
        break;
      }
      case T_UPGRADE_VIRTUAL_SCHEMA: {
        REGISTER_STMT_RESOLVER(UpgradeVirtualSchema);
        break;
      }
      case T_ADMIN_UPGRADE_CMD: {
        REGISTER_STMT_RESOLVER(AdminUpgradeCmd);
        break;
      }
      case T_ADMIN_ROLLING_UPGRADE_CMD: {
        REGISTER_STMT_RESOLVER(AdminRollingUpgradeCmd);
        break;
      }
      case T_PHYSICAL_RESTORE_TENANT: {
        REGISTER_STMT_RESOLVER(PhysicalRestoreTenant);
        break;
      }
      case T_CLEAR_ROOT_TABLE: {
        REGISTER_STMT_RESOLVER(ClearRootTable);
        break;
      }
      case T_CANCEL_TASK: {
        REGISTER_STMT_RESOLVER(CancelTask);
        break;
      }
      case T_REFRESH_SCHEMA: {
        REGISTER_STMT_RESOLVER(RefreshSchema);
        break;
      }
      case T_REFRESH_MEMORY_STAT: {
        REGISTER_STMT_RESOLVER(RefreshMemStat);
        break;
      }
      case T_WASH_MEMORY_FRAGMENTATION: {
        REGISTER_STMT_RESOLVER(WashMemFragmentation);
        break;
      }
      case T_REFRESH_IO_CALIBRATION: {
        REGISTER_STMT_RESOLVER(RefreshIOCalibration);
        break;
      }
      case T_ALTER_SYSTEM_SET_PARAMETER: {
        REGISTER_STMT_RESOLVER(SetConfig);
        break;
      }
      case T_ALTER_SYSTEM_SETTP: {
        REGISTER_STMT_RESOLVER(SetTP);
        break;
      }
      case T_CLEAR_LOCATION_CACHE: {
        REGISTER_STMT_RESOLVER(ClearLocationCache);
        break;
      }
      case T_RELOAD_GTS: {
        REGISTER_STMT_RESOLVER(ReloadGts);
        break;
      }
      case T_RELOAD_UNIT: {
        REGISTER_STMT_RESOLVER(ReloadUnit);
        break;
      }
      case T_RELOAD_SERVER: {
        REGISTER_STMT_RESOLVER(ReloadServer);
        break;
      }
      case T_RELOAD_ZONE: {
        REGISTER_STMT_RESOLVER(ReloadZone);
        break;
      }
      case T_CLEAR_MERGE_ERROR: {
        REGISTER_STMT_RESOLVER(ClearMergeError);
        break;
      }
      case T_MIGRATE_UNIT: {
        REGISTER_STMT_RESOLVER(MigrateUnit);
        break;
      }
      case T_ADD_ARBITRATION_SERVICE: {
        REGISTER_STMT_RESOLVER(AddArbitrationService);
        break;
      }
      case T_REMOVE_ARBITRATION_SERVICE: {
        REGISTER_STMT_RESOLVER(RemoveArbitrationService);
        break;
      }
      case T_REPLACE_ARBITRATION_SERVICE: {
        REGISTER_STMT_RESOLVER(ReplaceArbitrationService);
        break;
      }
      case T_RUN_JOB: {
        REGISTER_STMT_RESOLVER(RunJob);
        break;
      }
      case T_ADMIN_RUN_UPGRADE_JOB: {
        REGISTER_STMT_RESOLVER(RunUpgradeJob);
        break;
      }
      case T_ADMIN_STOP_UPGRADE_JOB: {
        REGISTER_STMT_RESOLVER(StopUpgradeJob);
        break;
      }
      case T_CREATE_DATABASE: {
        REGISTER_STMT_RESOLVER(CreateDatabase);
        break;
      }
      case T_USE_DATABASE: {
        REGISTER_STMT_RESOLVER(UseDatabase);
        break;
      }
      case T_ALTER_DATABASE: {
        REGISTER_STMT_RESOLVER(AlterDatabase);
        break;
      }
      case T_DROP_DATABASE: {
        REGISTER_STMT_RESOLVER(DropDatabase);
        break;
      }
      case T_CREATE_TABLEGROUP: {
        REGISTER_STMT_RESOLVER(CreateTablegroup);
        break;
      }
      case T_DROP_TABLEGROUP: {
        REGISTER_STMT_RESOLVER(DropTablegroup);
        break;
      }
      case T_ALTER_TABLEGROUP: {
        REGISTER_STMT_RESOLVER(AlterTablegroup);
        break;
      }
      case T_RENAME_TABLE: {
        REGISTER_STMT_RESOLVER(RenameTable);
        break;
      }
      case T_TRUNCATE_TABLE: {
        REGISTER_STMT_RESOLVER(TruncateTable);
        break;
      }
      case T_FLASHBACK_TABLE_FROM_RECYCLEBIN: {
        REGISTER_STMT_RESOLVER(FlashBackTableFromRecyclebin);
        break;
      }
      case T_FLASHBACK_TABLE_TO_TIMESTAMP:
      case T_FLASHBACK_TABLE_TO_SCN: {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "flashback table");
        //REGISTER_STMT_RESOLVER(FlashBackTableToScn);
        break;
      }
      case T_FLASHBACK_INDEX: {
        REGISTER_STMT_RESOLVER(FlashBackIndex);
        break;
      }
      case T_FLASHBACK_DATABASE: {
        REGISTER_STMT_RESOLVER(FlashBackDatabase);
        break;
      }
      case T_FLASHBACK_TENANT: {
        REGISTER_STMT_RESOLVER(FlashBackTenant);
        break;
      }
      case T_PURGE_TABLE: {
        REGISTER_STMT_RESOLVER(PurgeTable);
        break;
      }
      case T_PURGE_INDEX: {
        REGISTER_STMT_RESOLVER(PurgeIndex);
        break;
      }
      case T_PURGE_DATABASE: {
        REGISTER_STMT_RESOLVER(PurgeDatabase);
        break;
      }
      case T_PURGE_TENANT: {
        REGISTER_STMT_RESOLVER(PurgeTenant);
        break;
      }
      case T_PURGE_RECYCLEBIN: {
        REGISTER_STMT_RESOLVER(PurgeRecycleBin);
        break;
      }
      case T_OPTIMIZE_TABLE: {
        REGISTER_STMT_RESOLVER(OptimizeTable);
        break;
      }
      case T_OPTIMIZE_TENANT: {
        REGISTER_STMT_RESOLVER(OptimizeTenant);
        break;
      }
      case T_OPTIMIZE_ALL: {
        REGISTER_STMT_RESOLVER(OptimizeAll);
        break;
      }
      case T_PREPARE: {
        if (params_.is_prepare_protocol_) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("parse error", K(ret));
        } else {
          REGISTER_STMT_RESOLVER(Prepare);
        }
        break;
      }
      case T_EXECUTE: {
        if (params_.is_prepare_protocol_) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("parse error", K(ret));
        } else {
          REGISTER_STMT_RESOLVER(Execute);
        }
        break;
      }
      case T_DEALLOCATE: {
        if (params_.is_prepare_protocol_) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("parse error", K(ret));
        } else {
          REGISTER_STMT_RESOLVER(Deallocate);
        }
        break;
      }
      case T_VARIABLE_SET: {
        REGISTER_STMT_RESOLVER(VariableSet);
        break;
      }
      case T_TRANSACTION: {
        REGISTER_STMT_RESOLVER(SetTransaction);
        break;
      }
      case T_SHOW_TABLES:
      case T_SHOW_DATABASES:
      case T_SHOW_VARIABLES:
      case T_SHOW_COLUMNS:
      case T_SHOW_SCHEMA:
      case T_SHOW_CREATE_DATABASE:
      case T_SHOW_CREATE_TABLE:
      case T_SHOW_CREATE_VIEW:
      case T_SHOW_CREATE_PROCEDURE:
      case T_SHOW_CREATE_FUNCTION:
      case T_SHOW_TABLE_STATUS:
      case T_SHOW_PARAMETERS:
      case T_SHOW_INDEXES:
      case T_SHOW_PROCESSLIST:
      case T_SHOW_SERVER_STATUS:
      case T_SHOW_WARNINGS:
      case T_SHOW_ERRORS:
      case T_SHOW_TRACE:
      case T_SHOW_ENGINES:
      case T_SHOW_PRIVILEGES:
      case T_SHOW_CHARSET:
      case T_SHOW_COLLATION:
      case T_SHOW_GRANTS:
      case T_SHOW_TABLEGROUPS:
      case T_SHOW_TENANT:
      case T_SHOW_CREATE_TENANT:
      case T_SHOW_RECYCLEBIN:
      case T_SHOW_PROCEDURE_STATUS:
      case T_SHOW_FUNCTION_STATUS:
      case T_SHOW_TRIGGERS:
      case T_SHOW_CREATE_TABLEGROUP:
      case T_SHOW_RESTORE_PREVIEW:
      case T_SHOW_QUERY_RESPONSE_TIME:
      case T_SHOW_STATUS:
      case T_SHOW_CREATE_TRIGGER:
      case T_SHOW_SEQUENCES: {
        REGISTER_STMT_RESOLVER(Show);
        break;
      }
      case T_CREATE_USER: {
        REGISTER_STMT_RESOLVER(CreateUser);
        break;
      }
      case T_DROP_USER: {
        REGISTER_STMT_RESOLVER(DropUser);
        break;
      }
      case T_RENAME_USER: {
        REGISTER_STMT_RESOLVER(RenameUser);
        break;
      }
      case T_SET_PASSWORD: {
        REGISTER_STMT_RESOLVER(SetPassword);
        break;
      }
      case T_LOCK_USER: {
        REGISTER_STMT_RESOLVER(LockUser);
        break;
      }
      case T_ALTER_USER_PROFILE:
      case T_ALTER_USER_DEFAULT_ROLE:
      case T_SET_ROLE: {
        REGISTER_STMT_RESOLVER(AlterUserProfile);
        break;
      }
      case T_ALTER_USER_PRIMARY_ZONE: {
        REGISTER_STMT_RESOLVER(AlterUserPrimaryZone);
        break;
      }

      case T_SYSTEM_GRANT:
      case T_GRANT: {
        REGISTER_STMT_RESOLVER(Grant);
        break;
      }
      case T_SYSTEM_REVOKE:
      case T_REVOKE:
      case T_REVOKE_ALL: {
        REGISTER_STMT_RESOLVER(Revoke);
        break;
      }
      case T_EXPLAIN: {
        REGISTER_STMT_RESOLVER(Explain);
        break;
      }
      case T_ADMIN_SERVER: {
        REGISTER_STMT_RESOLVER(AdminServer);
        break;
      }
      case T_ADMIN_ZONE: {
        REGISTER_STMT_RESOLVER(AdminZone);
        break;
      }
      case T_ALTER_SYSTEM_SET: {
        REGISTER_STMT_RESOLVER(AlterSystemSet);
        break;
      }
      case T_ALTER_SYSTEM_KILL: {
        REGISTER_STMT_RESOLVER(Kill);
        break;
      }
      case T_ALTER_SESSION_SET: {
        REGISTER_STMT_RESOLVER(AlterSessionSet);
        break;
      }
      case T_SET_NAMES:
        // fall through
      case T_SET_CHARSET: {
        REGISTER_STMT_RESOLVER(SetNames);
        break;
      }
      case T_HELP: {
        REGISTER_STMT_RESOLVER(Help);
        break;
      }
      case T_KILL: {
        REGISTER_STMT_RESOLVER(Kill);
        break;
      }
      case T_EMPTY_QUERY: {
        REGISTER_STMT_RESOLVER(EmptyQuery);
        break;
      }
      case T_LOCK_TABLE: {
        REGISTER_STMT_RESOLVER(LockTable);
        break;
      }
      case T_CREATE_OUTLINE: {
        REGISTER_STMT_RESOLVER(CreateOutline);
        break;
      }
      case T_ALTER_OUTLINE: {
        REGISTER_STMT_RESOLVER(AlterOutline);
        break;
      }
      case T_DROP_OUTLINE: {
        REGISTER_STMT_RESOLVER(DropOutline);
        break;
      }
      case T_SP_CREATE: {
        REGISTER_STMT_RESOLVER(CreateProcedure);
        break;
      }
      case T_SP_ALTER: {
        REGISTER_STMT_RESOLVER(AlterProcedure);
        break;
      }
      case T_SP_CALL_STMT: {
        REGISTER_STMT_RESOLVER(CallProcedure);
        break;
      }
      case T_SF_CREATE: {
        REGISTER_STMT_RESOLVER(CreateFunction);
        break;
      }
      case T_SF_ALTER: {
        REGISTER_STMT_RESOLVER(AlterFunction);
        break;
      }
      case T_SP_DROP: {
        REGISTER_STMT_RESOLVER(DropProcedure);
        break;
      }
      case T_SF_DROP: {
        REGISTER_STMT_RESOLVER(DropFunction);
        break;
      }
      case T_SP_ANONYMOUS_BLOCK: {
        REGISTER_STMT_RESOLVER(AnonymousBlock);
        break;
      }
#ifdef OB_BUILD_ORACLE_PL
      case T_SP_CREATE_TYPE: {
        REGISTER_STMT_RESOLVER(CreateUDT);
        break;
      }
      case T_SP_CREATE_TYPE_BODY: {
        REGISTER_STMT_RESOLVER(CreateUDTBody);
        break;
      }
      case T_SP_DROP_TYPE: {
        REGISTER_STMT_RESOLVER(DropUDT);
        break;
      }
#endif
      case T_PACKAGE_CREATE: {
        REGISTER_STMT_RESOLVER(CreatePackage);
        break;
      }
      case T_PACKAGE_CREATE_BODY: {
        REGISTER_STMT_RESOLVER(CreatePackageBody);
        break;
      }
      case T_PACKAGE_ALTER: {
        REGISTER_STMT_RESOLVER(AlterPackage);
        break;
      }
      case T_PACKAGE_DROP: {
        REGISTER_STMT_RESOLVER(DropPackage);
        break;
      }
      case T_REFRESH_TIME_ZONE_INFO: {
        REGISTER_STMT_RESOLVER(RefreshTimeZoneInfo);
        break;
      }
      case T_ENABLE_SQL_THROTTLE: {
        REGISTER_STMT_RESOLVER(EnableSqlThrottle);
        break;
      }
      case T_DISABLE_SQL_THROTTLE: {
        REGISTER_STMT_RESOLVER(DisableSqlThrottle);
        break;
      }
      case T_SET_DISK_VALID: {
        REGISTER_STMT_RESOLVER(SetDiskValid);
        break;
      }
      case T_CREATE_SYNONYM: {
        REGISTER_STMT_RESOLVER(CreateSynonym);
        break;
      }
      case T_DROP_SYNONYM: {
        REGISTER_STMT_RESOLVER(DropSynonym);
        break;
      }
      case T_CLEAR_BALANCE_TASK: {
        REGISTER_STMT_RESOLVER(ClearBalanceTask);
        break;
      }
      case T_ANALYZE:
      case T_MYSQL_UPDATE_HISTOGRAM:
      case T_MYSQL_DROP_HISTOGRAM: {
        REGISTER_STMT_RESOLVER(AnalyzeStmt);
        break;
      }
      case T_DROP_FUNC: {
        REGISTER_STMT_RESOLVER(DropFunc);
        if (OB_FAIL(ret)) {
          if (ret == OB_ERR_FUNCTION_UNKNOWN) {
            ret = OB_SUCCESS;
            REGISTER_STMT_RESOLVER(DropFunction);
          } else {
            LOG_WARN("execute ObDropFuncResolver failed", K(ret), K_(parse_tree.type));
          }
        }
        break;
      }
      case T_LOAD_DATA: {
        REGISTER_STMT_RESOLVER(LoadData);
        break;
      }
      case T_CHANGE_TENANT: {
        REGISTER_STMT_RESOLVER(ChangeTenant);
        break;
      }
      case T_ALTER_SYSTEM_DROP_TEMP_TABLE: {
        REGISTER_STMT_RESOLVER(DropTempTable);
        break;
      }
      case T_ALTER_SYSTEM_REFRESH_TEMP_TABLE: {
        REGISTER_STMT_RESOLVER(RefreshTempTable);
        break;
      }
      case T_CREATE_SEQUENCE: {
        REGISTER_STMT_RESOLVER(CreateSequence);
        break;
      }
      case T_ALTER_SEQUENCE: {
        REGISTER_STMT_RESOLVER(AlterSequence);
        break;
      }
      case T_DROP_SEQUENCE: {
        REGISTER_STMT_RESOLVER(DropSequence);
        break;
      }
      case T_SET_TABLE_COMMENT:
      case T_SET_COLUMN_COMMENT: {
        REGISTER_STMT_RESOLVER(SetComment);
        break;
      }
      case T_XA_START: {
        REGISTER_STMT_RESOLVER(XaStart);
        break;
      }
      case T_XA_END: {
        REGISTER_STMT_RESOLVER(XaEnd);
        break;
      }
      case T_XA_PREPARE: {
        REGISTER_STMT_RESOLVER(XaPrepare);
        break;
      }
      case T_XA_COMMIT: {
        REGISTER_STMT_RESOLVER(XaCommit);
        break;
      }
      case T_XA_ROLLBACK: {
        REGISTER_STMT_RESOLVER(XaRollBack);
        break;
      }
      case T_ALTER_DISKGROUP_ADD_DISK: {
        REGISTER_STMT_RESOLVER(AlterDiskgroupAddDisk);
        break;
      }
      case T_ALTER_DISKGROUP_DROP_DISK: {
        REGISTER_STMT_RESOLVER(AlterDiskgroupDropDisk);
        break;
      }
      case T_CREATE_ROLE: {
        REGISTER_STMT_RESOLVER(CreateRole);
        break;
      }
      case T_DROP_ROLE: {
        REGISTER_STMT_RESOLVER(DropRole);
        break;
      }
      case T_ALTER_ROLE: {
        REGISTER_STMT_RESOLVER(AlterRole);
        break;
      }
      /*case T_SET_ROLE: {
        REGISTER_STMT_RESOLVER(SetRole);
        break;
      }*/
#ifdef OB_BUILD_TDE_SECURITY
      case T_CREATE_KEYSTORE: {
        REGISTER_STMT_RESOLVER(CreateKeystore);
        break;
      }
      case T_ALTER_KEYSTORE_OPEN:
      case T_ALTER_KEYSTORE_CLOSE:
      case T_ALTER_KEYSTORE_SET_KEY:
      case T_ALTER_KEYSTORE_PASSWORD: {
        REGISTER_STMT_RESOLVER(AlterKeystore);
        break;
      }
      case T_CREATE_TABLESPACE: {
        REGISTER_STMT_RESOLVER(CreateTablespace);
        break;
      }
      case T_ALTER_TABLESPACE: {
        REGISTER_STMT_RESOLVER(AlterTablespace);
        break;
      }
      case T_DROP_TABLESPACE: {
        REGISTER_STMT_RESOLVER(DropTablespace);
        break;
      }
#endif
      case T_CREATE_PROFILE: {
        REGISTER_STMT_RESOLVER(UserProfile);
        break;
      }
      case T_ALTER_PROFILE: {
        REGISTER_STMT_RESOLVER(UserProfile);
        break;
      }
      case T_DROP_PROFILE: {
        REGISTER_STMT_RESOLVER(UserProfile);
        break;
      }
      case T_CREATE_SAVEPOINT:
      case T_ROLLBACK_SAVEPOINT:
      case T_RELEASE_SAVEPOINT: {
        REGISTER_STMT_RESOLVER(SavePoint);
        break;
      }
      case T_TG_CREATE: {
        REGISTER_STMT_RESOLVER(Trigger);
        break;
      }
      case T_TG_DROP: {
        REGISTER_STMT_RESOLVER(Trigger);
        break;
      }
      case T_TG_ALTER: {
        REGISTER_STMT_RESOLVER(Trigger);
        break;
      }
      case T_ARCHIVE_LOG: {
        REGISTER_STMT_RESOLVER(ArchiveLog);
        break;
      }
      case T_BACKUP_DATABASE: {
        REGISTER_STMT_RESOLVER(BackupDatabase);
        break;
      }
      case T_CANCEL_RESTORE: {
        REGISTER_STMT_RESOLVER(CancelRestore);
        break;
      }
      case T_CANCEL_RECOVER_TABLE: {
        REGISTER_STMT_RESOLVER(CancelRecoverTable);
        break;
      }
      case T_BACKUP_KEY: {
        REGISTER_STMT_RESOLVER(BackupKey);
        break;
      }
      case T_RECOVER_TABLE: {
        REGISTER_STMT_RESOLVER(RecoverTable);
        break;
      }
      case T_BACKUP_MANAGE: {
        REGISTER_STMT_RESOLVER(BackupManage);
        break;
      }
      case T_BACKUP_CLEAN: {
        REGISTER_STMT_RESOLVER(BackupClean);
        break;
      }
      case T_DELETE_POLICY: {
        REGISTER_STMT_RESOLVER(DeletePolicy);
        break;
      }
      case T_CREATE_DBLINK: {
        REGISTER_STMT_RESOLVER(CreateDbLink);
        break;
      }
      case T_DROP_DBLINK: {
        REGISTER_STMT_RESOLVER(DropDbLink);
        break;
      }
      case T_BACKUP_ARCHIVELOG: {
        REGISTER_STMT_RESOLVER(BackupArchiveLog);
        break;
      }
      case T_BACKUP_SET_ENCRYPTION: {
        REGISTER_STMT_RESOLVER(BackupSetEncryption);
        break;
      }
      case T_BACKUP_SET_DECRYPTION: {
        REGISTER_STMT_RESOLVER(BackupSetDecryption);
        break;
      }
      case T_ADD_RESTORE_SOURCE: {
        REGISTER_STMT_RESOLVER(AddRestoreSource);
        break;
      }
      case T_CLEAR_RESTORE_SOURCE: {
        REGISTER_STMT_RESOLVER(ClearRestoreSource);
        break;
      }
      case T_CREATE_RESTORE_POINT: {
        REGISTER_STMT_RESOLVER(CreateRestorePoint);
        break;
      }
      case T_DROP_RESTORE_POINT: {
        REGISTER_STMT_RESOLVER(DropRestorePoint);
        break;
      }
      case T_SET_REGION_NETWORK_BANDWIDTH: {
        REGISTER_STMT_RESOLVER(SetRegionBandwidth);
        break;
      }
      case T_CREATE_DIRECTORY: {
        REGISTER_STMT_RESOLVER(CreateDirectory);
        break;
      }
      case T_DROP_DIRECTORY: {
        REGISTER_STMT_RESOLVER(DropDirectory);
        break;
      }
      case T_DIAGNOSTICS: {
        REGISTER_STMT_RESOLVER(GetDiagnostics);
        break;
      }
      case T_CREATE_CONTEXT: {
        REGISTER_STMT_RESOLVER(CreateContext);
        break;
      }
      case T_DROP_CONTEXT: {
        REGISTER_STMT_RESOLVER(DropContext);
        break;
      }
      case T_CHECKPOINT_SLOG: {
        REGISTER_STMT_RESOLVER(CheckpointSlog);
        break;
      }
      case T_TABLE_TTL: {
        REGISTER_STMT_RESOLVER(TableTTL);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        const char *type_name = get_type_name(parse_tree.type_);
        LOG_WARN("Statement not supported now", K(ret), K(type_name));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "statement type");
        break;
      }
    }  // end switch

    if (OB_SUCC(ret) && stmt->is_dml_stmt()) {
      OZ( (static_cast<ObDMLStmt*>(stmt)->disable_writing_external_table()) );
    }

    if (OB_SUCC(ret)) {
      if (ObStmt::is_write_stmt(stmt->get_stmt_type(), stmt->has_global_variable())
          && !MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
        ret = OB_STANDBY_READ_ONLY;
        TRANS_LOG(WARN, "standby tenant support read only", K(ret), K(stmt));
      }
    }

    if (OB_SUCC(ret) && stmt->is_dml_write_stmt()) {
      // todo yanli:检查主备库
    }
    
    if (OB_SUCC(ret) && stmt->is_dml_stmt()) {
      ObDMLStmt *dml_stmt = static_cast<ObDMLStmt*>(stmt);
      ObRawExprWrapEnumSet enum_set_wrapper(*params_.expr_factory_, params_.session_info_);
      if (OB_FAIL(enum_set_wrapper.wrap_enum_set(*dml_stmt))) {
        LOG_WARN("failed to wrap_enum_set", K(ret));
      }
    }

    if (OB_SUCC(ret) && stmt->is_dml_stmt() && !stmt->is_explain_stmt()) {
      if (OB_FAIL(params_.query_ctx_->query_hint_.init_query_hint(params_.allocator_,
                                                                  params_.session_info_,
                                                                  static_cast<ObDMLStmt*>(stmt)))) {
        LOG_WARN("failed to init query hint.", K(ret));
      } else if (OB_FAIL(params_.query_ctx_->query_hint_.check_and_set_params_from_hint(params_,
                                                         *static_cast<ObDMLStmt*>(stmt)))) {
        LOG_WARN("failed to check and set params from hint", K(ret));
      }
    }

    if (OB_SUCC(ret) && stmt->is_dml_stmt() && !stmt->is_explain_stmt()) {
      bool is_contain_inner_table = false;
      bool is_contain_select_for_update = false;
      ObDMLStmt *dml_stmt = static_cast<ObDMLStmt*>(stmt);
      if (OB_FAIL(dml_stmt->check_if_contain_inner_table(is_contain_inner_table))) {
        LOG_WARN("fail to check if contain inner table", K(ret));
      } else if (OB_FAIL(dml_stmt->check_if_contain_select_for_update(
                  is_contain_select_for_update))) {
        LOG_WARN("fail to check if contain select for update", K(ret));
      } else {
        params_.query_ctx_->is_contain_inner_table_ = is_contain_inner_table;
        params_.query_ctx_->is_contain_select_for_update_ = is_contain_select_for_update;
        params_.query_ctx_->has_dml_write_stmt_ = dml_stmt->is_dml_write_stmt();
      }
    }
    if (OB_SUCC(ret)) {
      stmt::StmtType stmt_type = stmt->get_stmt_type();
      if (ObStmt::is_ddl_stmt(stmt_type, stmt->has_global_variable()) || ObStmt::is_dcl_stmt(stmt_type)) {
        ObDDLStmt *ddl_stmt = static_cast<ObDDLStmt*>(stmt);
        obrpc::ObDDLArg &ddl_arg = ddl_stmt->get_ddl_arg();
        ddl_arg.exec_tenant_id_ = params_.session_info_->get_effective_tenant_id();
        if (OB_ISNULL(params_.query_ctx_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query ctx is null", K(ret));
        } else {
          ddl_arg.ddl_stmt_str_ = params_.query_ctx_->get_sql_stmt();
        }
        if (OB_FAIL(ObResolverUtils::set_sync_ddl_id_str(params_.session_info_, ddl_arg.ddl_id_str_))) {
          LOG_WARN("Failed to set_sync_ddl_id_str", K(ret));
        } else { } // do-nothing
      }
    }
  }  // end if
  return ret;
}

const ObTimeZoneInfo *ObResolver::get_timezone_info()
{
  return TZ_INFO(params_.session_info_);
}
}  // namespace sql
}  // namespace oceanbase
