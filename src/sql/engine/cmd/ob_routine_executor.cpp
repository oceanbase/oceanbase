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
#include "ob_routine_executor.h"
#include "sql/resolver/ddl/ob_create_routine_stmt.h"
#include "sql/resolver/ddl/ob_drop_routine_stmt.h"
#include "sql/resolver/ddl/ob_alter_routine_stmt.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/resolver/cmd/ob_anonymous_block_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_expr_info_flag.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_stmt.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "share/config/ob_config_helper.h"

namespace oceanbase
{
namespace sql
{

int ObCompileRoutineInf::compile_routine(ObExecContext &ctx,
                                          uint64_t tenant_id,
                                          uint64_t database_id,
                                          ObString &routine_name,
                                          ObRoutineType routine_type,
                                          int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObCacheObjGuard cacheobj_guard(PL_ROUTINE_HANDLE);
  const ObRoutineInfo *routine_info = nullptr;
  pl::ObPLFunction* routine = nullptr;
  uint64_t db_id = OB_INVALID_ID;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OZ (ctx.get_task_exec_ctx().schema_service_->
    get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), *ctx.get_sql_ctx()->schema_guard_));
  if (ROUTINE_PROCEDURE_TYPE == routine_type) {
    OZ (ctx.get_sql_ctx()->schema_guard_->get_standalone_procedure_info(tenant_id,
                                                                        database_id,
                                                                        routine_name,
                                                                        routine_info));
  } else {
    OZ (ctx.get_sql_ctx()->schema_guard_->get_standalone_function_info(tenant_id,
                                                                      database_id,
                                                                      routine_name,
                                                                      routine_info));
  }
  OZ (ctx.get_my_session()->get_database_id(db_id));
  if (OB_SUCC(ret) && OB_NOT_NULL(routine_info) && schema_version == routine_info->get_schema_version()) {
    pl::ObPLCacheCtx pc_ctx;
    pc_ctx.session_info_ = ctx.get_my_session();
    pc_ctx.schema_guard_ = ctx.get_sql_ctx()->schema_guard_;

    pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_PRCR;
    pc_ctx.key_.db_id_ = db_id;
    pc_ctx.key_.key_id_ = routine_info->get_routine_id();
    pc_ctx.key_.sessid_ = ctx.get_my_session()->is_pl_debug_on() ? ctx.get_my_session()->get_sessid() : 0;
    CK (OB_NOT_NULL(ctx.get_pl_engine()));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pl::ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
      LOG_TRACE("get pl function from ol cache failed", K(ret), K(pc_ctx.key_));
      ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
    } else {
      routine = static_cast<pl::ObPLFunction*>(cacheobj_guard.get_cache_obj());
    }
    if (OB_SUCC(ret) && OB_ISNULL(routine)) {
      OZ (ctx.get_pl_engine()->generate_pl_function(ctx, routine_info->get_routine_id(), cacheobj_guard));
      OX (routine = static_cast<pl::ObPLFunction*>(cacheobj_guard.get_cache_obj()));
      CK (OB_NOT_NULL(routine));
      OZ (ctx.get_my_session()->get_database_id(db_id));
      if (OB_SUCC(ret)
          && routine->get_can_cached()) {
        routine->get_stat_for_update().name_ = routine->get_function_name();
        routine->get_stat_for_update().type_ = pl::ObPLCacheObjectType::STANDALONE_ROUTINE_TYPE;
        OZ (ctx.get_pl_engine()->add_pl_lib_cache(routine, pc_ctx));
      }
    }
  }

  return ret;
}

int ObCreateRoutineExecutor::execute(ObExecContext &ctx, ObCreateRoutineStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 table_id;
  obrpc::ObCreateRoutineArg &crt_routine_arg = stmt.get_routine_arg();
  ObString first_stmt;
  uint64_t tenant_id = crt_routine_arg.routine_info_.get_tenant_id();
  uint64_t database_id = crt_routine_arg.routine_info_.get_database_id();
  ObString db_name = crt_routine_arg.db_name_;
  ObString routine_name = crt_routine_arg.routine_info_.get_routine_name();
  ObRoutineType type = crt_routine_arg.routine_info_.get_routine_type();
  obrpc::ObRoutineDDLRes res;
  bool with_res = (GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
                   && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
                  || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0;
  bool has_error = ERROR_STATUS_HAS_ERROR == crt_routine_arg.error_info_.get_error_status();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_my_session()->get_effective_tenant_id()));
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    crt_routine_arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (!with_res && OB_FAIL(common_rpc_proxy->create_routine(crt_routine_arg))) {
    LOG_WARN("rpc proxy create procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
  } else if (with_res && OB_FAIL(common_rpc_proxy->create_routine_with_res(crt_routine_arg, res))) {
    LOG_WARN("rpc proxy create procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  if (OB_SUCC(ret) && !has_error && with_res &&
      tenant_config.is_valid() &&
      tenant_config->plsql_v2_compatibility) {
    CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
    OZ (ObSPIService::force_refresh_schema(tenant_id, res.store_routine_schema_version_));
    OZ (compile_routine(ctx, tenant_id, database_id, routine_name, type,
                        res.store_routine_schema_version_));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to persistent routine", K(ret));
      ret = OB_SUCCESS;
      if (NULL != ctx.get_my_session()) {
        ctx.get_my_session()->reset_warnings_buf();
      }
    }
  }
  if(crt_routine_arg.with_if_not_exist_ && ret == OB_ERR_SP_ALREADY_EXISTS) {
    LOG_USER_WARN(OB_ERR_SP_ALREADY_EXISTS, "ROUTINE",  crt_routine_arg.routine_info_.get_routine_name().length(), crt_routine_arg.routine_info_.get_routine_name().ptr());
    ret = OB_SUCCESS;
  }
  return ret;
}


int ObCallProcedureExecutor::execute(ObExecContext &ctx, ObCallProcedureStmt &stmt)
{
  int ret = OB_SUCCESS;
  uint64_t package_id = OB_INVALID_ID;
  uint64_t routine_id = OB_INVALID_ID;
  ObCallProcedureInfo *call_proc_info = NULL;
  LOG_DEBUG("call procedure execute", K(stmt));
  if (OB_ISNULL(ctx.get_pl_engine()) || OB_ISNULL(ctx.get_output_row())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl engine is NULL", K(ctx.get_pl_engine()), K(ret));
  } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(ctx.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql context is null", K(ret));
  } else if (OB_ISNULL(ctx.get_stmt_factory()) ||
             OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else if (OB_FAIL(ob_write_string(ctx.get_allocator(),
                                     ctx.get_my_session()->get_current_query_string(),
                                     ctx.get_stmt_factory()->get_query_ctx()->get_sql_stmt()))) {
    LOG_WARN("fail to set query string");
  } else if (OB_ISNULL(call_proc_info = stmt.get_call_proc_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("call procedure info is null", K(ret));
  } else {
    ParamStore params( (ObWrapperAllocator(ctx.get_allocator())) );
    const share::schema::ObRoutineInfo *routine_info = NULL;

    if (!call_proc_info->can_direct_use_param()) {
      ObObjParam param;
      const ParamStore &origin_params = ctx.get_physical_plan_ctx()->get_param_store_for_update();
      pl::ExecCtxBak exec_ctx_bak;
      sql::ObPhysicalPlanCtx phy_plan_ctx(ctx.get_allocator());
      phy_plan_ctx.set_timeout_timestamp(ctx.get_physical_plan_ctx()->get_timeout_timestamp());
      exec_ctx_bak.backup(ctx);
      ctx.set_physical_plan_ctx(&phy_plan_ctx);
      if (call_proc_info->get_expr_op_size() > 0)  {
        OZ (ctx.init_expr_op(call_proc_info->get_expr_op_size()));
      }
      OZ (call_proc_info->get_frame_info().pre_alloc_exec_memory(ctx));

      for (int64_t i = 0; OB_SUCC(ret) && i < origin_params.count(); ++i) {
        OZ (phy_plan_ctx.get_param_store_for_update().push_back(origin_params.at(i)));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < call_proc_info->get_expressions().count(); ++i) {
        const ObSqlExpression *expr = call_proc_info->get_expressions().at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr NULL", K(i), K(ret));
        } else {
          param.reset();
          param.ObObj::reset();
          if (OB_FAIL(ObSQLUtils::calc_sql_expression_without_row(ctx, *expr, param))) {
            LOG_WARN("failed to calc exec param expr", K(i), K(*expr), K(ret));
          } else {
            if (expr->get_is_pl_mock_default_expr()) {
              param.set_is_pl_mock_default_param(true);
            }
            if (param.is_pl_extend() && !IS_CONST_TYPE(expr->get_expr_items().at(0).get_item_type())) {
              const ObExprOperator *op = expr->get_expr_items().at(0).get_expr_operator();
              if (OB_ISNULL(op)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr operator", K(ret));
              } else {
                param.set_udt_id(op->get_result_type().get_expr_udt_id());
              }
            } else if (ob_is_xml_sql_type(param.get_type(), param.get_udt_subschema_id())) {
              // convert call procedure input sql udt types to pl extend (only xmltype supported currently)
              bool is_strict = is_strict_mode(ctx.get_my_session()->get_sql_mode());
              const ObDataTypeCastParams dtc_params =
                ObBasicSessionInfo::create_dtc_params(ctx.get_my_session());
              ObCastMode cast_mode = CM_NONE;
              ObExprResType result_type;
              OZ (ObSQLUtils::get_default_cast_mode(stmt::T_NONE, ctx.get_my_session(), cast_mode));
              ObCastCtx cast_ctx(&ctx.get_allocator(), &dtc_params, cast_mode, param.get_collation_type());
              result_type.reset();
              // if is xml type
              result_type.set_ext();
              result_type.set_extend_type(pl::PL_OPAQUE_TYPE);
              result_type.set_udt_id(T_OBJ_XML);
              ObObj tmp;
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(ObExprColumnConv::convert_with_null_check(tmp, param, result_type,
                                                                           is_strict, cast_ctx, NULL))) {
                LOG_WARN("Cast sql udt to pl extend failed",
                        K(ret), K(param), K(result_type), K(is_strict), K(i));
              } else {
                param = tmp;
                param.set_udt_id(T_OBJ_XML);
                param.set_param_meta();
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(params.push_back(param))) {
              LOG_WARN("push back error", K(i), K(*expr), K(ret));
            } else {
              params.at(params.count() - 1).set_param_meta();
            }
          }
        }
      } // for end

      if (call_proc_info->get_expr_op_size() > 0) {
        ctx.reset_expr_op();
        ctx.get_allocator().free(ctx.get_expr_op_ctx_store());
      }
      exec_ctx_bak.restore(ctx);
    } else {
      LOG_DEBUG("direct use params", K(ret), K(stmt));
      int64_t param_cnt = ctx.get_physical_plan_ctx()->get_param_store().count();
      if (call_proc_info->get_param_cnt() != param_cnt) {
        ret = OB_ERR_SP_WRONG_ARG_NUM;
        LOG_WARN("argument number not equal", K(call_proc_info->get_param_cnt()), K(param_cnt), K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
        LOG_DEBUG("params", "param", ctx.get_physical_plan_ctx()->get_param_store().at(i), K(i));
        if (OB_FAIL(params.push_back(ctx.get_physical_plan_ctx()->get_param_store().at(i)))) {
          LOG_WARN("push back error", K(i), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      package_id = call_proc_info->get_package_id();
      routine_id = call_proc_info->get_routine_id();
    }
    if (OB_SUCC(ret)) {
      ObArray<int64_t> path;
      ObArray<int64_t> nocopy_params;
      ObObj result;
      int64_t pkg_id = call_proc_info->is_udt_routine()
               ? share::schema::ObUDTObjectType::mask_object_id(package_id) : package_id;
      if (OB_ISNULL(stmt.get_dblink_routine_info())) {
        if (OB_FAIL(ctx.get_pl_engine()->execute(ctx,
                                                ctx.get_allocator(),
                                                pkg_id,
                                                routine_id,
                                                path,
                                                params,
                                                nocopy_params,
                                                result))) {
          LOG_WARN("failed to execute pl", K(package_id), K(routine_id), K(ret), K(pkg_id));
        }
#ifdef OB_BUILD_ORACLE_PL
      } else if (OB_FAIL(ObSPIService::spi_execute_dblink(ctx,
                                                          ctx.get_allocator(),
                                                          NULL,
                                                          stmt.get_dblink_routine_info(),
                                                          params,
                                                          NULL))) {
        LOG_WARN("failed to execute dblink pl", K(ret), KP(stmt.get_dblink_routine_info()));
#endif
      }
      if (OB_READ_NOTHING == ret
          && lib::is_oracle_mode()
          && !ObTriggerInfo::is_trigger_package_id(package_id)) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (call_proc_info->get_output_count() > 0) {
        ctx.get_output_row()->count_ = call_proc_info->get_output_count();
        if (OB_ISNULL(ctx.get_output_row()->cells_ = static_cast<ObObj *>(
                      ctx.get_allocator().alloc(sizeof(ObObj) * call_proc_info->get_output_count())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc obj array", K(call_proc_info->get_output_count()), K(ret));
        } else {
          int64_t idx = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
            if (call_proc_info->is_out_param(i)) {
              if (ob_is_enum_or_set_type(params.at(i).get_type())) {
                OZ (ObSPIService::cast_enum_set_to_string(
                  ctx,
                  call_proc_info->get_out_type().at(idx).get_type_info(),
                  params.at(i),
                  ctx.get_output_row()->cells_[idx]));
                OX (idx++);
              } else {
                ctx.get_output_row()->cells_[idx] = params.at(i);
                idx++;
              }

              if (OB_FAIL(ret)) {
              } else if (!call_proc_info->can_direct_use_param()) {
                const ObSqlExpression *expr = call_proc_info->get_expressions().at(i);
                ObItemType expr_type = expr->get_expr_items().at(0).get_item_type();
                if (OB_LIKELY(IS_CONST_TYPE(expr_type))) {
                  const ObObj &value = expr->get_expr_items().at(0).get_obj();
                  if (T_QUESTIONMARK == expr_type) {
                    int64_t idx = value.get_unknown();
                    ctx.get_physical_plan_ctx()->get_param_store_for_update().at(idx) = params.at(i);
                  } else {
                    /* do nothing */
                  }
                } else if (T_OP_GET_USER_VAR == expr_type) { //这里只有可能出现用户变量
                  ObExprCtx expr_ctx;
                  if (expr->get_expr_items().count() < 2 || T_VARCHAR != expr->get_expr_items().at(1).get_item_type()) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("Unexpected result expr", K(*expr), K(ret));
                  } else if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt.get_stmt_type(), ctx, ctx.get_allocator(), expr_ctx))) {
                    LOG_WARN("Failed to wrap expr ctx", K(ret));
                  } else {
                    const ObString var_name = expr->get_expr_items().at(1).get_obj().get_string();
                    if (OB_FAIL(ObVariableSetExecutor::set_user_variable(params.at(i), var_name, expr_ctx))) {
                      LOG_WARN("set user variable failed", K(ret));
                    }
                  }
                } else {
                  ret = OB_ERR_OUT_PARAM_NOT_BIND_VAR;
                  LOG_WARN("output parameter not a bind variable", K(ret));
                }
              } else {
                ctx.get_physical_plan_ctx()->get_param_store_for_update().at(i) = params.at(i);
              }
            }
          } // for end
        }
      } else { /*do nothing*/ }
    }
    ctx.get_sql_ctx()->cur_stmt_ = &stmt;
  }
  return ret;
}

int ObDropRoutineExecutor::execute(ObExecContext &ctx, ObDropRoutineStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::UInt64 table_id;
  obrpc::ObDropRoutineArg &drop_routine_arg = stmt.get_routine_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    drop_routine_arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->drop_routine(drop_routine_arg))) {
    LOG_WARN("rpc proxy drop procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
  }
  return ret;
}

int ObAlterRoutineExecutor::execute(ObExecContext &ctx, ObAlterRoutineStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObCreateRoutineArg &alter_routine_arg = stmt.get_routine_arg();
  uint64_t tenant_id = alter_routine_arg.routine_info_.get_tenant_id();
  uint64_t database_id = alter_routine_arg.routine_info_.get_database_id();
  ObString db_name = alter_routine_arg.db_name_;
  ObString routine_name = alter_routine_arg.routine_info_.get_routine_name();
  ObRoutineType type = alter_routine_arg.routine_info_.get_routine_type();
  bool has_error = ERROR_STATUS_HAS_ERROR == alter_routine_arg.error_info_.get_error_status();
  bool need_create_routine = (lib::is_oracle_mode() && alter_routine_arg.is_or_replace_) ||
                            (lib::is_mysql_mode() && alter_routine_arg.is_need_alter_);
  ObString first_stmt;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_my_session()->get_effective_tenant_id()));
  if (need_create_routine) {
    obrpc::ObRoutineDDLRes res;
    bool with_res = (GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
                     && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
                    || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0;
    if (OB_ISNULL(ctx.get_pl_engine())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl engine is null", K(ret));
    } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
      LOG_WARN("fail to get first stmt" , K(ret));
    } else {
      alter_routine_arg.ddl_stmt_str_ = first_stmt;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_NOT_INIT;
      LOG_WARN("get task executor context failed", K(ret));
    } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
      LOG_WARN("get common rpc proxy failed", K(ret));
    } else if (OB_ISNULL(common_rpc_proxy)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy should not be null", K(ret));
    } else if (!with_res && OB_FAIL(common_rpc_proxy->alter_routine(alter_routine_arg))) {
      LOG_WARN("rpc proxy alter procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
    } else if (with_res && OB_FAIL(common_rpc_proxy->alter_routine_with_res(alter_routine_arg, res))) {
      LOG_WARN("rpc proxy alter procedure failed", K(ret), "dst", common_rpc_proxy->get_server());
    }
    if (OB_SUCC(ret) && !has_error && with_res &&
        tenant_config.is_valid() &&
        tenant_config->plsql_v2_compatibility) {
      CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
      OZ (ObSPIService::force_refresh_schema(tenant_id, res.store_routine_schema_version_));
      OZ (compile_routine(ctx, tenant_id, database_id, routine_name, type,
                          res.store_routine_schema_version_));
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to persistent routine", K(ret));
        common::ob_reset_tsi_warning_buffer();
        ret = OB_SUCCESS;
      }
    }
  } else {
    ObMySQLTransaction trans;
    const ObRoutineInfo *routine_info = nullptr;
    ObSchemaGetterGuard schema_guard;
    OZ (ctx.get_task_exec_ctx().schema_service_->
        get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), schema_guard));
    OZ(schema_guard.get_routine_info(tenant_id, alter_routine_arg.routine_info_.get_routine_id(), routine_info));
    CK (OB_NOT_NULL(routine_info));
    OZ (trans.start(GCTX.sql_proxy_, tenant_id, true));
    OZ (alter_routine_arg.error_info_.handle_error_info(trans, routine_info));
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
        LOG_WARN("trans end failed", K(ret), K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret) && !has_error
        && ((GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_3_0
             && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
            || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0)
        && tenant_config.is_valid() && tenant_config->plsql_v2_compatibility) {
      OZ (compile_routine(ctx, tenant_id, database_id, routine_name, type,
                          routine_info->get_schema_version()));
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to persistent routine", K(ret));
        common::ob_reset_tsi_warning_buffer();
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObAnonymousBlockExecutor::execute(ObExecContext &ctx, ObAnonymousBlockStmt &stmt)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx.get_pl_engine()));
  CK (OB_NOT_NULL(stmt.get_params()));
  CK (OB_NOT_NULL(ctx.get_my_session()));

  if (OB_FAIL(ret)) {
  } else if (stmt.is_prepare_protocol()) {
    ObBitSet<OB_DEFAULT_BITSET_SIZE> out_args;
    OZ (ctx.get_pl_engine()->execute(
      ctx, *stmt.get_params(), stmt.get_stmt_id(), stmt.get_sql(), out_args),
      K(stmt), KPC(stmt.get_params()));

    // 处理匿名块出参的场景, 如果是通过execute immediate执行的匿名块, 需要将匿名块中参数的修改覆盖到父调用的param_store中
    if (OB_SUCC(ret)
        && stmt.get_params()->count() > 0
        && OB_NOT_NULL(ctx.get_my_session()->get_pl_context())) {
      CK (OB_NOT_NULL(ctx.get_physical_plan_ctx()));
      CK (ctx.get_physical_plan_ctx()->get_param_store().count() == stmt.get_params()->count());
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_params()->count(); ++i) {
        ctx.get_physical_plan_ctx()->get_param_store_for_update().at(i) = stmt.get_params()->at(i);
      }
    }
    // 处理顶层匿名块的出参, 顶层匿名块的出参需要返回给客户端
    if (OB_SUCC(ret)
        && stmt.get_params()->count() > 0
        && OB_ISNULL(ctx.get_my_session()->get_pl_context())
        && !out_args.is_empty()) {
      bool need_push = false;
      ctx.get_output_row()->count_ = out_args.num_members();
      if (OB_ISNULL(ctx.get_output_row()->cells_ =
        static_cast<ObObj *>(ctx.get_allocator().alloc(sizeof(ObObj) * out_args.num_members())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc obj array", K(ret), K(stmt.get_params()->count()));
      }
      CK (OB_NOT_NULL(ctx.get_field_columns()));
      OV (ctx.get_field_columns()->count() == out_args.num_members()
        || 0 == ctx.get_field_columns()->count(),
        OB_ERR_UNEXPECTED, ctx.get_field_columns()->count(), out_args.num_members());
      if (OB_SUCC(ret) && 0 == ctx.get_field_columns()->count()) {
        OZ (ctx.get_field_columns()->reserve(out_args.num_members()));
        OX (need_push = true);
      }
      int64_t out_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_params()->count(); ++i) {
        if (out_args.has_member(i)) {
          ObField field;
          ObObjParam &value = stmt.get_params()->at(i);
          ctx.get_output_row()->cells_[out_idx] = stmt.get_params()->at(i);
          field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI;
          field.type_.set_type(value.get_type());
          field.accuracy_ = value.get_accuracy();
          if (value.get_type() != ObExtendType) { // 基础数据类型
            if (ObVarcharType == value.get_type()
                || ObCharType == value.get_type()
                || ob_is_nstring_type(value.get_type())) {
              if (-1 == field.accuracy_.get_length()) {
                field.length_ = ObCharType == value.get_type()
                  ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : OB_MAX_ORACLE_VARCHAR_LENGTH;
              } else {
                field.length_ = field.accuracy_.get_length();
              }
            } else {
              OZ (common::ObField::get_field_mb_length(
                field.type_.get_type(), field.accuracy_, common::CS_TYPE_INVALID, field.length_));
            }
            ObCollationType collation = CS_TYPE_INVALID;
            OZ (ObCharset::get_default_collation(value.get_collation_type(), collation));
            OX (field.charsetnr_ = collation);
          } else { // 复杂数据类型
            field.length_ = field.accuracy_.get_length();
            if (value.is_ref_cursor_type()) {
              OZ (ob_write_string(ctx.get_allocator(), ObString("SYS_REFCURSOR"), field.type_name_));
            } else if (value.get_udt_id() != OB_INVALID_ID) {
              OZ (fill_field_with_udt_id(ctx, value.get_udt_id(), field));
            } else if (value.is_pl_extend()
                       && pl::PL_NESTED_TABLE_TYPE == value.get_meta().get_extend_type()) {
              // anonymous collection, reuse default value to record element type
              pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection *>(value.get_ext());
              CK (OB_NOT_NULL(coll));
              OX (field.default_value_.set_type(coll->get_element_type().get_obj_type()));
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("anonymous out parameter type is not anonymous collection",
                       K(ret), K(value));
              LOG_USER_ERROR(OB_NOT_SUPPORTED,
                             "anonymous out parameter type is not anonymous collection");
            }
          }
          if (need_push) {
            OZ (ctx.get_field_columns()->push_back(field));
          } else {
            OX (ctx.get_field_columns()->at(out_idx) = field);
          }
          OX (out_idx ++);
        }
      }
    }
  } else {
    CK (OB_NOT_NULL(stmt.get_params()));
    OZ (ctx.get_pl_engine()->execute(ctx, *stmt.get_params(), stmt.get_body()));
  }
  return ret;
}

int ObAnonymousBlockExecutor::fill_field_with_udt_id(
    ObExecContext &ctx, uint64_t udt_id, common::ObField &field)
{
  int ret = OB_SUCCESS;
  const share::schema::ObUDTTypeInfo *udt_info = NULL;
  const share::schema::ObDatabaseSchema *db_schema = NULL;
  const uint64_t tenant_id = pl::get_tenant_id_by_object_id(udt_id);
  ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
  CK (OB_NOT_NULL(sql_ctx));
  CK (OB_NOT_NULL(sql_ctx->schema_guard_));
  CK (udt_id != OB_INVALID_ID);
  OZ (sql_ctx->schema_guard_->get_udt_info(tenant_id, udt_id, udt_info));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(udt_info)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support udt type", K(ret), K(udt_id));
  } else {
    OZ (sql_ctx->schema_guard_->get_database_schema(udt_info->get_tenant_id(),
        udt_info->get_database_id(), db_schema));
    CK (OB_NOT_NULL(db_schema));
    OZ (ob_write_string(ctx.get_allocator(),
                        OB_SYS_TENANT_ID == db_schema->get_tenant_id()
                          ? ObString("SYS") : db_schema->get_database_name(),
                        field.type_owner_));
    OZ (ob_write_string(ctx.get_allocator(), udt_info->get_type_name(), field.type_name_));
  }

  return ret;
}

}
}
