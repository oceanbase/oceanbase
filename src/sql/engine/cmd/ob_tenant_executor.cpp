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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_tenant_executor.h"

#include "lib/container/ob_se_array_iterator.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_unit_getter.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_get_compat_mode.h"
#include "share/ls/ob_ls_operator.h"
#include "share/ob_leader_election_waiter.h"
#include "share/ls/ob_ls_status_operator.h"       //ObLSStatusInfo, ObLSStatusOperator
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_drop_tenant_stmt.h"
#include "sql/resolver/ddl/ob_lock_tenant_stmt.h"
#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/resolver/cmd/ob_create_restore_point_stmt.h"
#include "sql/resolver/cmd/ob_drop_restore_point_stmt.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/ls/ob_ls_status_operator.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
int check_sys_var_options(ObExecContext &ctx,
                          const common::ObIArray<ObVariableSetStmt::VariableSetNode> &sys_var_nodes,
                          share::schema::ObTenantSchema &tenant_schema,
                          common::ObIArray<obrpc::ObSysVarIdValue> &sys_var_list);

int ObCreateTenantExecutor::execute(ObExecContext &ctx, ObCreateTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 tenant_id;
  const obrpc::ObCreateTenantArg &create_tenant_arg = stmt.get_create_tenant_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObCreateTenantArg&>(create_tenant_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (create_tenant_arg.tenant_schema_.get_arbitration_service_status() != ObArbitrationServiceStatus(ObArbitrationServiceStatus::DISABLED)) {
    bool is_compatible = false;
    if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(OB_SYS_TENANT_ID, is_compatible))) {
      LOG_WARN("fail to check sys tenant compat version", KR(ret));
    } else if (!is_compatible) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("sys tenant data version is below 4.1, tenant with arbitration service not allow", KR(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "sys tenant data version is below 4.1, with arbitration service");
    }
  }
  if (OB_FAIL(ret)){
  } else if (OB_FAIL(check_sys_var_options(ctx,
                                    stmt.get_sys_var_nodes(),
                                    stmt.get_create_tenant_arg().tenant_schema_,
                                    stmt.get_create_tenant_arg().sys_var_list_))) {
    LOG_WARN("check_sys_var_options failed", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->create_tenant(create_tenant_arg, tenant_id))) {
    LOG_WARN("rpc proxy create tenant failed", K(ret));
  } else if (!create_tenant_arg.if_not_exist_ && OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("if_not_exist not set and tenant_id invalid tenant_id", K(create_tenant_arg), K(tenant_id), K(ret));
  } else if (OB_INVALID_ID != tenant_id) {
    int tmp_ret = OB_SUCCESS; // try refresh schema and wait ls valid
    if (OB_TMP_FAIL(wait_schema_refreshed_(tenant_id))) {
      LOG_WARN("fail to wait schema refreshed", KR(tmp_ret), K(tenant_id));
    } else if (OB_TMP_FAIL(wait_user_ls_valid_(tenant_id))) {
      LOG_WARN("failed to wait user ls valid, but ignore", KR(tmp_ret), K(tenant_id));
    }
  }
  LOG_INFO("[CREATE TENANT] create tenant", KR(ret), K(create_tenant_arg),
           "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObCreateTenantExecutor::wait_schema_refreshed_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  bool is_dropped = false;
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t user_schema_version = OB_INVALID_VERSION;
  int64_t meta_schema_version = OB_INVALID_VERSION;
  while (OB_SUCC(ret)) {
    if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("failed to wait user ls valid", KR(ret));
    } else if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(meta_tenant_id, is_dropped))) {
      LOG_WARN("meta tenant has been dropped", KR(ret), K(meta_tenant_id));
    } else if (is_dropped) {
      ret = OB_TENANT_HAS_BEEN_DROPPED;
      LOG_WARN("meta tenant has been dropped", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(user_tenant_id, is_dropped))) {
      LOG_WARN("user tenant has been dropped", KR(ret), K(user_tenant_id));
    } else if (is_dropped) {
      ret = OB_TENANT_HAS_BEEN_DROPPED;
      LOG_WARN("user tenant has been dropped", KR(ret), K(user_tenant_id));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(
          meta_tenant_id, meta_schema_version))) {
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("get refreshed schema version failed", KR(ret), K(meta_tenant_id));
        }
      } else if (OB_TMP_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(
          user_tenant_id, user_schema_version))) {
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("get refreshed schema version failed", KR(ret), K(user_tenant_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (ObSchemaService::is_formal_version(meta_schema_version)
                 && ObSchemaService::is_formal_version(user_schema_version)) {
        break;
      } else {
        LOG_INFO("wait schema refreshed", K(tenant_id), K(meta_schema_version), K(user_schema_version));
        ob_usleep(500 * 1000L); // 500ms
      }
    }
  }
  LOG_INFO("[CREATE TENANT] wait schema refreshed", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObCreateTenantExecutor::wait_user_ls_valid_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else {
    bool user_ls_valid = false;
    ObLSStatusOperator status_op;
    ObLSStatusInfoArray ls_array;
    ObLSID ls_id;
    //wait user ls create success
    while (OB_SUCC(ret) && !user_ls_valid) {
      ls_array.reset();
      if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("failed to wait user ls valid", KR(ret));
      } else if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array, *GCTX.sql_proxy_))) {
        LOG_WARN("failed to get ls status", KR(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count() && !user_ls_valid; ++i) {
          const ObLSStatusInfo &ls_status = ls_array.at(i);
          if (!ls_status.ls_id_.is_sys_ls() && ls_status.ls_is_normal()) {
            user_ls_valid = true;
            ls_id = ls_status.ls_id_;
          }
        }//end for
      }
      if (OB_FAIL(ret)) {
      } else if (user_ls_valid) {
      } else {
        const int64_t INTERVAL = 500 * 1000L; // 500ms
        LOG_INFO("wait user ls valid", KR(ret), K(tenant_id));
        ob_usleep(INTERVAL);
      }
    }// end while
    LOG_INFO("[CREATE TENANT] wait user ls created", KR(ret), K(tenant_id),
             "cost", ObTimeUtility::current_time() - start_ts);

    if (OB_SUCC(ret)) {
      start_ts = ObTimeUtility::current_time();
      //wait user ls election
      if (OB_ISNULL(GCTX.lst_operator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls operator is null", KR(ret));
      } else {
        volatile bool stopped = false;
        share::ObLSLeaderElectionWaiter ls_leader_waiter(*GCTX.lst_operator_, stopped);
        const int64_t timeout = THIS_WORKER.get_timeout_remain();
        if (OB_FAIL(ls_leader_waiter.wait(tenant_id, ls_id, timeout))) {
          LOG_WARN("fail to wait election leader", KR(ret), K(tenant_id), K(ls_id), K(timeout));
        }
      }
      LOG_INFO("[CREATE TENANT] wait user ls election result", KR(ret), K(tenant_id),
               "cost", ObTimeUtility::current_time() - start_ts);
    }
  }
  return ret;
}

int ObCreateStandbyTenantExecutor::execute(ObExecContext &ctx, ObCreateTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 tenant_id;
  obrpc::ObCreateTenantArg &create_tenant_arg = stmt.get_create_tenant_arg();
  ObString first_stmt;
  ObCompatibilityMode compat_mode = ObCompatibilityMode::OCEANBASE_MODE;
  uint64_t compat_ver = 0;

  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObCreateTenantArg&>(create_tenant_arg).ddl_stmt_str_ = first_stmt;
  }

  if (OB_FAIL(ret)){
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.check_can_create_standby_tenant(
                         create_tenant_arg.log_restore_source_, compat_mode))) {
    LOG_WARN("check can create standby_tenant failed", KR(ret), K(create_tenant_arg));
  } else {
    create_tenant_arg.tenant_schema_.set_compatibility_mode(compat_mode);
  }

  if (OB_FAIL(ret)){
  } else if (OB_FAIL(common_rpc_proxy->create_tenant(create_tenant_arg, tenant_id))) {
    LOG_WARN("rpc proxy create tenant failed", K(ret));
  } else if (!create_tenant_arg.if_not_exist_ && OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("if_not_exist not set and tenant_id invalid tenant_id", KR(ret), K(create_tenant_arg), K(tenant_id));
  } else if (OB_INVALID_ID != tenant_id) {
    if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.wait_create_standby_tenant_end(tenant_id))) {
      LOG_WARN("failed to wait user create end", KR(ret), K(tenant_id));
    }
  }
  LOG_INFO("[CREATE STANDBY TENANT] create standby tenant", KR(ret), K(create_tenant_arg),
           "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int check_sys_var_options(ObExecContext &ctx,
                          const common::ObIArray<ObVariableSetStmt::VariableSetNode> &sys_var_nodes,
                          share::schema::ObTenantSchema &tenant_schema,
                          common::ObIArray<obrpc::ObSysVarIdValue> &sys_var_list)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(session = ctx.get_my_session()) || OB_ISNULL(sql_proxy = ctx.get_sql_proxy())
      || OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or sql proxy or plan_ctx is NULL", K(session), K(sql_proxy), K(plan_ctx), K(ret));
  } else {
    //construct expr_ctx
    SMART_VARS_3((ObPhysicalPlan, phy_plan), (ObPhysicalPlanCtx, phy_plan_ctx, ctx.get_allocator()),
                 (ObExecContext, exec_ctx, ctx.get_allocator())) {
      ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC,
                                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                                 session->get_effective_tenant_id());
      ObExprCtx expr_ctx;
      expr_ctx.phy_plan_ctx_ = &phy_plan_ctx;
      expr_ctx.my_session_ = session;
      expr_ctx.exec_ctx_ = &exec_ctx;
      expr_ctx.calc_buf_ = &allocator;
      expr_ctx.exec_ctx_->set_sql_proxy(sql_proxy);
      phy_plan_ctx.set_phy_plan(&phy_plan);
      const int64_t cur_time = plan_ctx->has_cur_time() ?
          plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
      phy_plan_ctx.set_cur_time(cur_time, *session);

      ObVariableSetStmt::VariableSetNode tmp_node;//just for init node
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_var_nodes.count(); ++i) {
        ObVariableSetStmt::VariableSetNode &cur_node = tmp_node;
        ObBasicSysVar *sys_var = NULL;
        if (OB_FAIL(sys_var_nodes.at(i, cur_node))) {
          LOG_WARN("failed to access node from array", K(ret));
        } else if (!cur_node.is_system_variable_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create tenant can only set system variables", K(cur_node), K(ret));
        } else if (OB_FAIL(session->get_sys_variable_by_name(cur_node.variable_name_, sys_var))) {
          LOG_WARN("fail to get_sys_variable_by_name", K(ret));
        } else if (OB_ISNULL(sys_var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("got sys var is NULL", K(ret));
        } else if (cur_node.is_set_default_) { // set default, then do nothing
        } else {
          ObObj value_obj;
          //first:calculate value of expression
          ObNewRow tmp_row;
          RowDesc row_desc;
          ObTempExpr *temp_expr = NULL;
          CK(OB_NOT_NULL(ctx.get_sql_ctx()));
          OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(cur_node.value_expr_,
             row_desc, ctx.get_allocator(), session,
             ctx.get_sql_ctx()->schema_guard_, temp_expr));
          CK(OB_NOT_NULL(temp_expr));
          OZ(temp_expr->eval(ctx, tmp_row, value_obj));

          //second:convert value to dest type
          uint64_t fake_tenant_id = OB_INVALID_ID;
          ObSetVar set_var(cur_node.variable_name_, cur_node.set_scope_, cur_node.is_set_default_,
                           fake_tenant_id, *expr_ctx.calc_buf_, *sql_proxy);
          if (OB_SUCC(ret)) {
            ObObj out_obj;
            const bool is_set_stmt = false;
            if (OB_FAIL(ObVariableSetExecutor::check_and_convert_sys_var(ctx, set_var, *sys_var, value_obj, out_obj, is_set_stmt))) {
              LOG_WARN("fail to check_and_convert_sys_var", K(cur_node), K(*sys_var), K(value_obj), K(ret));
            } else if (FALSE_IT(value_obj = out_obj)) {
            } else if (OB_FAIL(ObVariableSetExecutor::cast_value(ctx, cur_node, fake_tenant_id, ctx.get_allocator(),
                                          *sys_var, value_obj, out_obj))) {
              LOG_WARN("fail to cast value", K(cur_node), K(*sys_var), K(value_obj), K(ret));
            } else if (FALSE_IT(value_obj = out_obj)) {
            } else {/*do nothing*/}
          }
          //add variable value into ObCreateTenantArg
          if (OB_SUCC(ret)) {
            if (set_var.var_name_ == OB_SV_COLLATION_SERVER
                || set_var.var_name_ == OB_SV_COLLATION_DATABASE
                || set_var.var_name_ == OB_SV_COLLATION_CONNECTION
                || set_var.var_name_ == OB_SV_CHARACTER_SET_SERVER
                || set_var.var_name_ == OB_SV_CHARACTER_SET_DATABASE
                || set_var.var_name_ == OB_SV_CHARACTER_SET_CONNECTION) {
              //TODO(yaoying.yyy):千拂将会重构字符集系统变量一块儿，为避免无用的重复逻辑，暂时不支持 set
              //字符集相关的系统变量
              //等千拂重构后，这段就可以打开了，
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("collation or charset can not be modify temporarily", K(set_var), K(ret));
            } else {
              //read only should also modify tenant_schema
              if (set_var.var_name_ == OB_SV_READ_ONLY) {
                if (session->get_in_transaction()) {
                  ret = OB_ERR_LOCK_OR_ACTIVE_TRANSACTION;

                  LOG_WARN("Can't execute the given command because "
                           "you have active locked tables or an active transaction", K(ret));
                } else {
                  tenant_schema.set_read_only(value_obj.get_bool());
                }
              }
              ObSysVarClassType sys_id = sys_var->get_type();
              ObString val_str;
              expr_ctx.calc_buf_ = &ctx.get_allocator();//make sure use this allocator to keep ObString is valid
              EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
              EXPR_GET_VARCHAR_V2(value_obj, val_str);
              if (OB_SUCC(ret)) {
                int64_t sys_var_val_length = OB_MAX_SYS_VAR_VAL_LENGTH;
                if (set_var.var_name_ == OB_SV_TCP_INVITED_NODES) {
                  uint64_t data_version = 0;
                  if (OB_FAIL(GET_MIN_DATA_VERSION(session->get_effective_tenant_id(), data_version))) {
                    LOG_WARN("fail to get tenant data version", KR(ret));
                  } else if (data_version >= DATA_VERSION_4_2_1_1) {
                    sys_var_val_length = OB_MAX_TCP_INVITED_NODES_LENGTH;
                  }
                }
                if (OB_SUCC(ret) && OB_UNLIKELY(val_str.length() > sys_var_val_length)) {
                  ret = OB_SIZE_OVERFLOW;
                  LOG_WARN("set sysvar value is overflow", "max length", sys_var_val_length, "value length", val_str.length(), K(sys_id), K(val_str));
                } else if (OB_FAIL(sys_var_list.push_back(obrpc::ObSysVarIdValue(sys_id, val_str)))) {
                  LOG_WARN("failed to push back", K(sys_id), K(val_str), K(ret));
                }
              }
            }
          }
        }
      }//end of for
    }
  }
  return ret;
}

int ObLockTenantExecutor::execute(ObExecContext &ctx, ObLockTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObLockTenantArg &lock_tenant_arg = stmt.get_lock_tenant_arg();

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->lock_tenant(lock_tenant_arg))) {
    LOG_WARN("rpc proxy lock tenant failed", K(ret));
  }
  return ret;
}

int modify_progressive_merge_num_for_tenant(ObExecContext &ctx,
    const int64_t tenant_id, const int64_t progressive_merge_num)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  ObSchemaGetterGuard schema_guard;
  common::ObCommonSqlProxy *user_sql_proxy;
  common::ObOracleSqlProxy oracle_sql_proxy;
  if (OB_SYS_TENANT_ID == tenant_id && !GCONF.enable_sys_table_ddl) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sys_table_ddl option");
    LOG_WARN("sys_table_ddl option should be enabled", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (nullptr == sql_proxy) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, sql proxy must not be null", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    observer::ObInnerSQLConnectionPool *pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool());
    if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, pool must not be null", K(ret));
    } else if (OB_FAIL(oracle_sql_proxy.init(pool))) {
      LOG_WARN("fail to init oracle sql proxy", K(ret));
    } else {
      ObArray<const ObDatabaseSchema *> dbs;
      if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, dbs))) {
        LOG_WARN("fail to get db schemas", K(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; i < dbs.count() && OB_SUCC(ret); i++) {
          const ObDatabaseSchema *db = dbs.at(i);
          const uint64_t database_id = db->get_database_id();
          ObArray<const ObTableSchema *> tables;
          if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id,
                  database_id,
                  tables))) {
            LOG_WARN("fail to get table schemas", K(ret), K(tenant_id), K(database_id));
          }
          for (int64_t j = 0; j < tables.count() && OB_SUCC(ret); j++) {
            if (OB_FAIL(ctx.check_status())) {
              LOG_WARN("should stopped", K(ret));
            } else {
              const ObTableSchema *table = tables.at(j);
              bool do_alter = true;
              if (!table->has_partition()) {
                do_alter = false;
              }
              if (table->is_index_table()) {
                do_alter = false;
              }
              if (table->is_in_recyclebin()) {
                do_alter = false;
              }
              if (table->is_tmp_table()) {
                do_alter = false;
              }
              if (table->get_table_id() == OB_ALL_CORE_TABLE_TID) {
                do_alter = false;
              }
              if (table->get_progressive_merge_num() == progressive_merge_num) {
                do_alter = false;
              }
              if (OB_SYS_TENANT_ID != tenant_id && table->is_sys_table()) {
                do_alter = false;
              }
              if (table->is_tmp_table()) {
                do_alter = false;
              }
              if (table->is_external_table()) {
                do_alter = false;
              }
              if (do_alter) {
                ObSqlString sql;
                int64_t affected_rows = 0;
                lib::Worker::CompatMode mode;
                if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, mode))) {
                  LOG_WARN("fail to get tenant mode", K(ret), K(tenant_id));
                } else if (lib::Worker::CompatMode::MYSQL == mode) {
                  if (OB_FAIL(sql.assign_fmt("ALTER TABLE `%.*s`.`%.*s` SET PROGRESSIVE_MERGE_NUM = %ld",
                          db->get_database_name_str().length(),
                          db->get_database_name_str().ptr(),
                          table->get_table_name_str().length(),
                          table->get_table_name_str().ptr(),
                          progressive_merge_num))) {
                    LOG_WARN("sql assign_fmt failed", K(ret));
                  } else {
                    user_sql_proxy = sql_proxy;
                  }
                } else if (lib::Worker::CompatMode::ORACLE == mode) {
                  if (OB_FAIL(sql.assign_fmt("ALTER TABLE \"%.*s\".\"%.*s\" SET PROGRESSIVE_MERGE_NUM = %ld",
                          db->get_database_name_str().length(),
                          db->get_database_name_str().ptr(),
                          table->get_table_name_str().length(),
                          table->get_table_name_str().ptr(),
                          progressive_merge_num))) {
                    LOG_WARN("sql assign_fmt failed", K(ret));
                  } else {
                    user_sql_proxy = &oracle_sql_proxy;
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("error unexpected, invalid tenant mode", K(ret), K(mode));
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(user_sql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
                    if (OB_ERR_OPERATION_ON_RECYCLE_OBJECT != ret) {
                      LOG_WARN("execute sql failed", K(ret));
                    } else {
                      ret = OB_SUCCESS;
                    }
                  }
                }
              }
            }
          }
        } // table loop end
      } // database loop end
    }
  }
  return ret;
}

int modify_progressive_merge_num_for_all_tenants(ObExecContext &ctx, const int64_t progressive_merge_num)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSEArray<uint64_t, 32> tenant_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const int64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(modify_progressive_merge_num_for_tenant(ctx, tenant_id, progressive_merge_num))) {
        LOG_WARN("fail to modify progressive merge num for tenant", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int enable_extended_rowid_for_tenant_tables(ObExecContext &ctx, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  ObSchemaGetterGuard schema_guard;
  common::ObCommonSqlProxy *user_sql_proxy;
  common::ObOracleSqlProxy oracle_sql_proxy;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id));
  } else if (nullptr == sql_proxy) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, sql proxy must not be null", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    observer::ObInnerSQLConnectionPool *pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool());
    if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, pool must not be null", K(ret));
    } else if (OB_FAIL(oracle_sql_proxy.init(pool))) {
      LOG_WARN("fail to init oracle sql proxy", K(ret));
    } else {
      ObArray<const ObDatabaseSchema *> dbs;
      if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, dbs))) {
        LOG_WARN("fail to get db schemas", K(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; i < dbs.count() && OB_SUCC(ret); i++) {
          const ObDatabaseSchema *db = dbs.at(i);
          const uint64_t database_id = db->get_database_id();
          ObArray<const ObTableSchema *> tables;
          if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id,
                  database_id,
                  tables))) {
            LOG_WARN("fail to get table schemas", K(ret), K(tenant_id), K(database_id));
          }
          for (int64_t j = 0; j < tables.count() && OB_SUCC(ret); j++) {
            if (OB_FAIL(ctx.check_status())) {
              LOG_WARN("should stopped", K(ret));
            } else {
              const ObTableSchema *table = tables.at(j);
              if (table->is_in_recyclebin()) {
                // do nothing
              } else if (OB_ALL_CORE_TABLE_TID == table->get_table_id()) {
                // do nothing
              } else if (!table->has_rowid()) {
                // do nothing
              } else if (table->is_extended_rowid_mode()) {
                // do nothing
              } else {
                ObSqlString sql;
                int64_t affected_rows = 0;
                lib::Worker::CompatMode mode;
                if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, mode))) {
                  LOG_WARN("fail to get tenant mode", K(ret), K(tenant_id));
                } else if (lib::Worker::CompatMode::MYSQL == mode) {
                  if (OB_FAIL(sql.assign_fmt("ALTER TABLE `%.*s`.`%.*s` SET ENABLE_EXTENDED_ROWID = true",
                          db->get_database_name_str().length(),
                          db->get_database_name_str().ptr(),
                          table->get_table_name_str().length(),
                          table->get_table_name_str().ptr()))) {
                    LOG_WARN("sql assign_fmt failed", K(ret));
                  } else {
                    user_sql_proxy = sql_proxy;
                  }
                } else if (lib::Worker::CompatMode::ORACLE == mode) {
                  if (OB_FAIL(sql.assign_fmt("ALTER TABLE \"%.*s\".\"%.*s\" SET ENABLE_EXTENDED_ROWID = true",
                          db->get_database_name_str().length(),
                          db->get_database_name_str().ptr(),
                          table->get_table_name_str().length(),
                          table->get_table_name_str().ptr()))) {
                    LOG_WARN("sql assign_fmt failed", K(ret));
                  } else {
                    user_sql_proxy = &oracle_sql_proxy;
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("error unexpected, invalid tenant mode", K(ret), K(mode));
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(user_sql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
                    if (OB_ERR_OPERATION_ON_RECYCLE_OBJECT != ret) {
                      LOG_WARN("execute sql failed", K(ret));
                    } else {
                      ret = OB_SUCCESS;
                    }
                  }
                }
              }
            }
          }
        } // table loop end
      } // database loop end
    }
  }
  return ret;
}

int ObModifyTenantExecutor::execute(ObExecContext &ctx, ObModifyTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObModifyTenantArg &modify_tenant_arg = stmt.get_modify_tenant_arg();
  ObString first_stmt;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObModifyTenantArg&>(modify_tenant_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(
                         modify_tenant_arg.tenant_schema_.get_tenant_name_str(), tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (-1 != stmt.get_progressive_merge_num()) {
    if (modify_tenant_arg.tenant_schema_.get_tenant_name_str().case_compare("all") == 0) {
      ObSQLSessionInfo *session = NULL;
      if (OB_ISNULL(session = ctx.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (session->get_tenant_name().case_compare(ObString::make_string(OB_SYS_TENANT_NAME)) != 0) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("no privilege", K(ret));
      } else if (OB_FAIL(modify_progressive_merge_num_for_all_tenants(ctx, stmt.get_progressive_merge_num()))) {
        LOG_WARN("modify_progressive_merge_num_for_tables failed", K(ret));
      }
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, tenant schema must not be NULL", KR(ret));
    } else if (OB_FAIL(modify_progressive_merge_num_for_tenant(ctx, tenant_schema->get_tenant_id(), stmt.get_progressive_merge_num()))) {
      LOG_WARN("fail to modify progressive merge num for tenant", K(ret), "tenant_id", tenant_schema->get_tenant_id());
    }
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exists", KR(ret), K(modify_tenant_arg));
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, modify_tenant_arg.tenant_schema_.get_tenant_name_str().length(),
                   modify_tenant_arg.tenant_schema_.get_tenant_name_str().ptr());
  } else if (stmt.get_modify_tenant_arg().alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::ENABLE_EXTENDED_ROWID)) {
    if (OB_FAIL(enable_extended_rowid_for_tenant_tables(ctx, tenant_schema->get_tenant_id()))) {
      LOG_WARN("fail to enable extended rowid for tenant tables", K(ret), "tenant_id", tenant_schema->get_tenant_id());
    }
  } else {
    if (stmt.get_modify_tenant_arg().alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::ENABLE_ARBITRATION_SERVICE)) {
      bool is_compatible = false;
      if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(tenant_schema->get_tenant_id(), is_compatible))) {
        LOG_WARN("fail to check sys tenant compat version", KR(ret), KPC(tenant_schema));
      } else if (!is_compatible) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("user tenant data version is below 4.1, tenant with arbitration service not allow", KR(ret), KPC(tenant_schema));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "user tenant data version is below 4.1, with arbitration service");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_sys_var_options(ctx,
            stmt.get_sys_var_nodes(),
            stmt.get_modify_tenant_arg().tenant_schema_,
            stmt.get_modify_tenant_arg().sys_var_list_))) {
      LOG_WARN("check_sys_var_options failed", K(ret));
    } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed");
    } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed");
    } else if (OB_FAIL(common_rpc_proxy->modify_tenant(modify_tenant_arg))) {
      LOG_WARN("rpc proxy modify tenant failed", K(ret));
    }
  }
  return ret;
}

int ObDropTenantExecutor::execute(ObExecContext &ctx, ObDropTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropTenantArg &drop_tenant_arg = stmt.get_drop_tenant_arg();
  ObString first_stmt;
  ObSchemaGetterGuard guard;
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObDropTenantArg&>(drop_tenant_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(guard.get_tenant_info(
             drop_tenant_arg.tenant_name_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(drop_tenant_arg));
  } else if (OB_ISNULL(tenant_schema)) {
    if (drop_tenant_arg.if_exist_) {
      LOG_USER_NOTE(OB_TENANT_NOT_EXIST, drop_tenant_arg.tenant_name_.length(), drop_tenant_arg.tenant_name_.ptr());
      LOG_INFO("tenant not exist, no need to delete it", K(drop_tenant_arg));
    } else {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, drop_tenant_arg.tenant_name_.length(), drop_tenant_arg.tenant_name_.ptr());
      LOG_WARN("tenant not exist", KR(ret), K(drop_tenant_arg));
    }
  } else {
    DEBUG_SYNC(BEFORE_DROP_TENANT);
    if (OB_FAIL(common_rpc_proxy->drop_tenant(drop_tenant_arg))) {
      LOG_WARN("rpc proxy drop tenant failed", K(ret));
    } else if (OB_FAIL(check_tenant_has_been_dropped_(
              ctx, stmt, tenant_schema->get_tenant_id()))) {
      LOG_WARN("fail to check tenant has been dropped", KR(ret), KPC(tenant_schema));
    }
  }
  return ret;
}

int ObDropTenantExecutor::check_tenant_has_been_dropped_(
    ObExecContext &ctx,
    ObDropTenantStmt &stmt,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const obrpc::ObDropTenantArg &arg = stmt.get_drop_tenant_arg();
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  const bool delay_to_drop = (arg.delay_to_drop_ && !arg.open_recyclebin_);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (delay_to_drop) {
    while (OB_SUCC(ret)) {
      bool is_timeout = false;
      bool is_dropped = false;
      if (OB_FAIL(my_session->is_timeout(is_timeout))) {
        LOG_WARN("fail to check timeout", KR(ret));
      } else if (is_timeout) {
        ret = OB_TIMEOUT;
        LOG_WARN("session is timeout", KR(ret));
      } else if (OB_FAIL(my_session->check_session_status())) {
        LOG_WARN("fail to check session status", KR(ret));
      } else if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(
                 tenant_id, is_dropped))) {
        LOG_WARN("fail to check tenant has been dropped", KR(ret), K(tenant_id));
      } else if (is_dropped) {
        LOG_INFO("tenant has been dropped", KR(ret), K(tenant_id));
        break;
      } else {
        const int64_t INTERVAL = 1000 * 1000L; // 1s
        LOG_INFO("tenant has not be dropped yet", KR(ret), K(tenant_id));
        ob_usleep(INTERVAL);
      }
    }
  }
  return ret;
}

int ObFlashBackTenantExecutor::execute(ObExecContext &ctx, ObFlashBackTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObFlashBackTenantArg &flashback_tenant_arg = stmt.get_flashback_tenant_arg();
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObFlashBackTenantArg&>(flashback_tenant_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->flashback_tenant(flashback_tenant_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy flashback tenant failed", K(ret));
  }
  return ret;
}

int ObPurgeTenantExecutor::execute(ObExecContext &ctx, ObPurgeTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObPurgeTenantArg &purge_tenant_arg = stmt.get_purge_tenant_arg();
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObPurgeTenantArg&>(purge_tenant_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->purge_tenant(purge_tenant_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy purge tenant failed", K(ret));
  }
  return ret;
}

int ObPurgeRecycleBinExecutor::execute(ObExecContext &ctx, ObPurgeRecycleBinStmt &stmt)
{
  int ret = OB_SUCCESS;
  //use to test purge recyclebin objects
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObPurgeRecycleBinArg &purge_recyclebin_arg = stmt.get_purge_recyclebin_arg();

//  int64_t current_time = ObTimeUtility::current_time();
//  obrpc::Int64 expire_time = current_time - GCONF.schema_history_expire_time;
  obrpc::Int64 affected_rows = 0;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObPurgeRecycleBinArg&>(purge_recyclebin_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else {
    bool is_tenant_finish = false;
    int64_t total_purge_count = 0;
    uint64_t tenant_id = purge_recyclebin_arg.tenant_id_;
    while (OB_SUCC(ret) && !is_tenant_finish) {
      //一个租户只purge 10个回收站的对象，防止卡住RS的ddl线程
      //每次返回purge的行数，只有purge数目少于affected_rows
      int64_t cal_timeout = 0;
      int64_t start_time = ObTimeUtility::current_time();
      if (OB_FAIL(GSCHEMASERVICE.cal_purge_need_timeout(purge_recyclebin_arg, cal_timeout))) {
        LOG_WARN("fail to cal purge time out", KR(ret), K(tenant_id));
      } else if (0 == cal_timeout) {
        is_tenant_finish = true;
      } else if (OB_FAIL(common_rpc_proxy->timeout(cal_timeout).purge_expire_recycle_objects(purge_recyclebin_arg, affected_rows))) {
        LOG_WARN("purge reyclebin objects failed", K(ret), K(affected_rows), K(purge_recyclebin_arg));
        //如果失败情况下，不需要继续
        is_tenant_finish = false;
      } else {
        is_tenant_finish = obrpc::ObPurgeRecycleBinArg::DEFAULT_PURGE_EACH_TIME == affected_rows ? false : true;
        total_purge_count += affected_rows;
      }
      int64_t cost_time = ObTimeUtility::current_time() - start_time;
      LOG_INFO("purge recycle objects", KR(ret), K(cost_time), K(cal_timeout),
               K(total_purge_count), K(purge_recyclebin_arg), K(affected_rows), K(is_tenant_finish));
    }
    LOG_INFO("purge recyclebin success", KR(ret), K(purge_recyclebin_arg), K(total_purge_count));
  }
  return ret;
}

int ObCreateRestorePointExecutor::execute(ObExecContext &ctx, ObCreateRestorePointStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  stmt.set_tenant_id(tenant_id);
  const obrpc::ObCreateRestorePointArg &create_restore_point_arg = stmt.get_create_restore_point_arg();
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->create_restore_point(create_restore_point_arg))) {
    LOG_WARN("rpc proxy create restore point failed", K(ret));
  }
  return ret;
}
int ObDropRestorePointExecutor::execute(ObExecContext &ctx, ObDropRestorePointStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  stmt.set_tenant_id(tenant_id);

  const obrpc::ObDropRestorePointArg &drop_restore_point_arg = stmt.get_drop_restore_point_arg();

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->drop_restore_point(drop_restore_point_arg))) {
    LOG_WARN("rpc proxy drop restore point failed", K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
