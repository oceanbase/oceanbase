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

#define USING_LOG_PREFIX PL

#include "pl/ob_pl.h"
#include "lib/container/ob_fast_array.h"
#include "lib/string/ob_sql_string.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/ob_smart_call.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_exception_handling.h"
#include "pl/ob_pl_compile.h"
#include "pl/ob_pl_code_generator.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_interface_pragma.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_spi.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/ob_sql.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "share/ob_truncated_string.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/session/ob_basic_session_info.h"
#include "observer/ob_req_time_service.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/engine/expr/ob_expr_pl_integer_checker.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_udt_object_manager.h"
#include "pl/debug/ob_pl_debugger.h"
#include "pl/debug/ob_pl_debugger_manager.h"
#include "pl/opaque/ob_pl_json_type.h"
#include "pl/ob_pl_profiler.h"
#endif
#include "pl/pl_cache/ob_pl_cache_mgr.h"
#include "sql/engine/dml/ob_trigger_handler.h"
#include "pl/ob_pl_allocator.h"
#include "sql/dblink/ob_tm_service.h"
namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share;
using namespace share::schema;
using namespace transaction;
namespace sql
{
extern int sys_pkg_need_priv_check(uint64_t pkg_id, ObSchemaGetterGuard *schema_guard,
                                   bool &need_check, uint64_t &pkg_spec_id,
                                   bool &need_only_obj_check);
}
namespace pl
{
int ObPL::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  jit::ObLLVMHelper::initialize();

  jit::ObLLVMHelper::add_symbol(ObString("spi_calc_expr_at_idx"),
                                (void*)(sql::ObSPIService::spi_calc_expr_at_idx));
  jit::ObLLVMHelper::add_symbol(ObString("spi_calc_package_expr"),
                                (void*)(sql::ObSPIService::spi_calc_package_expr));
  jit::ObLLVMHelper::add_symbol(ObString("spi_set_variable_to_expr"),
                                (void*)(sql::ObSPIService::spi_set_variable_to_expr));
  jit::ObLLVMHelper::add_symbol(ObString("spi_query_into_expr_idx"),
                                (void*)(sql::ObSPIService::spi_query_into_expr_idx));
  jit::ObLLVMHelper::add_symbol(ObString("spi_check_autonomous_trans"),
                                (void*)(sql::ObSPIService::spi_check_autonomous_trans));
  jit::ObLLVMHelper::add_symbol(ObString("spi_execute_with_expr_idx"),
                                (void*)(sql::ObSPIService::spi_execute_with_expr_idx));
  jit::ObLLVMHelper::add_symbol(ObString("spi_execute_immediate"),
                                (void*)(sql::ObSPIService::spi_execute_immediate));
  jit::ObLLVMHelper::add_symbol(ObString("spi_cursor_init"),
                                (void*)(sql::ObSPIService::spi_cursor_init));
  jit::ObLLVMHelper::add_symbol(ObString("spi_cursor_open_with_param_idx"),
                                (void*)(sql::ObSPIService::spi_cursor_open_with_param_idx));
  jit::ObLLVMHelper::add_symbol(ObString("spi_dynamic_open"),
                                (void*)(sql::ObSPIService::spi_dynamic_open));
  jit::ObLLVMHelper::add_symbol(ObString("spi_cursor_fetch"),
                                (void*)(sql::ObSPIService::spi_cursor_fetch));
  jit::ObLLVMHelper::add_symbol(ObString("spi_cursor_close"),
                                (void*)(sql::ObSPIService::spi_cursor_close));
  jit::ObLLVMHelper::add_symbol(ObString("spi_extend_collection"),
                                (void*)(sql::ObSPIService::spi_extend_collection));
  jit::ObLLVMHelper::add_symbol(ObString("spi_delete_collection"),
                                (void*)(sql::ObSPIService::spi_delete_collection));
  jit::ObLLVMHelper::add_symbol(ObString("spi_trim_collection"),
                                (void*)(sql::ObSPIService::spi_trim_collection));
  jit::ObLLVMHelper::add_symbol(ObString("spi_raise_application_error"),
                                (void*)(sql::ObSPIService::spi_raise_application_error));
  jit::ObLLVMHelper::add_symbol(ObString("spi_process_resignal"),
                                (void*)(sql::ObSPIService::spi_process_resignal));
  jit::ObLLVMHelper::add_symbol(ObString("spi_destruct_collection"),
                                (void*)(sql::ObSPIService::spi_destruct_collection));
  jit::ObLLVMHelper::add_symbol(ObString("spi_reset_collection"),
                                (void*)(sql::ObSPIService::spi_reset_collection));
  jit::ObLLVMHelper::add_symbol(ObString("spi_copy_datum"),
                                (void*)(sql::ObSPIService::spi_copy_datum));
  jit::ObLLVMHelper::add_symbol(ObString("spi_destruct_obj"),
                                (void*)(sql::ObSPIService::spi_destruct_obj));
  jit::ObLLVMHelper::add_symbol(ObString("spi_sub_nestedtable"),
                                (void*)(sql::ObSPIService::spi_sub_nestedtable));
  jit::ObLLVMHelper::add_symbol(ObString("spi_alloc_complex_var"),
                                (void*)(sql::ObSPIService::spi_alloc_complex_var));
  jit::ObLLVMHelper::add_symbol(ObString("spi_construct_collection"),
                                (void*)(sql::ObSPIService::spi_construct_collection));
  jit::ObLLVMHelper::add_symbol(ObString("spi_clear_diagnostic_area"),
                                (void*)(sql::ObSPIService::spi_clear_diagnostic_area));
  jit::ObLLVMHelper::add_symbol(ObString("spi_end_trans"),
                                (void*)(sql::ObSPIService::spi_end_trans));
  jit::ObLLVMHelper::add_symbol(ObString("spi_set_pl_exception_code"),
                                (void*)(sql::ObSPIService::spi_set_pl_exception_code));
  jit::ObLLVMHelper::add_symbol(ObString("spi_get_pl_exception_code"),
                                (void*)(sql::ObSPIService::spi_get_pl_exception_code));
  jit::ObLLVMHelper::add_symbol(ObString("spi_check_early_exit"),
                                (void*)(sql::ObSPIService::spi_check_early_exit));
  jit::ObLLVMHelper::add_symbol(ObString("spi_convert_objparam"),
                                (void*)(sql::ObSPIService::spi_convert_objparam));
  jit::ObLLVMHelper::add_symbol(ObString("spi_pipe_row_to_result"),
                                (void*)(sql::ObSPIService::spi_pipe_row_to_result));
  jit::ObLLVMHelper::add_symbol(ObString("spi_check_exception_handler_legal"),
                               (void*)(sql::ObSPIService::spi_check_exception_handler_legal));
  jit::ObLLVMHelper::add_symbol(ObString("spi_interface_impl"),
                                (void*)(sql::ObSPIService::spi_interface_impl));
  jit::ObLLVMHelper::add_symbol(ObString("spi_process_nocopy_params"),
                                (void*)(sql::ObSPIService::spi_process_nocopy_params));
  jit::ObLLVMHelper::add_symbol(ObString("spi_update_package_change_info"),
                                (void*)(sql::ObSPIService::spi_update_package_change_info));
  jit::ObLLVMHelper::add_symbol(ObString("spi_check_composite_not_null"),
                                (void*)(sql::ObSPIService::spi_check_composite_not_null));
  jit::ObLLVMHelper::add_symbol(ObString("pl_execute"),
                                (void*)(ObPL::execute_proc));
  jit::ObLLVMHelper::add_symbol(ObString("set_user_type_var"),
                                (void*)(ObPL::set_user_type_var));
  jit::ObLLVMHelper::add_symbol(ObString("set_implicit_cursor_in_forall"),
                                (void*)(ObPL::set_implicit_cursor_in_forall));
  jit::ObLLVMHelper::add_symbol(ObString("unset_implicit_cursor_in_forall"),
                                (void*)(ObPL::unset_implicit_cursor_in_forall));

  jit::ObLLVMHelper::add_symbol(ObString("eh_create_exception"),
                                (void*)(ObPLEH::eh_create_exception));
  jit::ObLLVMHelper::add_symbol(ObString("_Unwind_RaiseException"),
                                (void*)(_Unwind_RaiseException));
  jit::ObLLVMHelper::add_symbol(ObString("_Unwind_Resume"),
                                (void*)(_Unwind_Resume));
  jit::ObLLVMHelper::add_symbol(ObString("eh_personality"),
                                (void*)(ObPLEH::eh_personality));
  jit::ObLLVMHelper::add_symbol(ObString("eh_convert_exception"),
                                (void*)(ObPLEH::eh_convert_exception));
  jit::ObLLVMHelper::add_symbol(ObString("eh_classify_exception"),
                                (void*)(ObPLEH::eh_classify_exception));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_int64"),
                                (void*)(ObPLEH::eh_debug_int64));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_int64ptr"),
                                (void*)(ObPLEH::eh_debug_int64ptr));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_int32"),
                                (void*)(ObPLEH::eh_debug_int32));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_int32ptr"),
                                  (void*)(ObPLEH::eh_debug_int32ptr));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_int8"),
                                  (void*)(ObPLEH::eh_debug_int8));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_int8ptr"),
                                (void*)(ObPLEH::eh_debug_int8ptr));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_obj"),
                                (void*)(ObPLEH::eh_debug_obj));
  jit::ObLLVMHelper::add_symbol(ObString("eh_debug_objparam"),
                                (void*)(ObPLEH::eh_debug_objparam));
  jit::ObLLVMHelper::add_symbol(ObString("spi_add_ref_cursor_refcount"),
                                (void*)(sql::ObSPIService::spi_add_ref_cursor_refcount));
  jit::ObLLVMHelper::add_symbol(ObString("spi_handle_ref_cursor_refcount"),
                                (void*)(sql::ObSPIService::spi_handle_ref_cursor_refcount));
  jit::ObLLVMHelper::add_symbol(ObString("spi_opaque_assign_null"),
                                (void*)(sql::ObSPIService::spi_opaque_assign_null));
  jit::ObLLVMHelper::add_symbol(ObString("spi_pl_profiler_before_record"),
                                (void*)(sql::ObSPIService::spi_pl_profiler_before_record));
  jit::ObLLVMHelper::add_symbol(ObString("spi_pl_profiler_after_record"),
                                (void*)(sql::ObSPIService::spi_pl_profiler_after_record));

  sql_proxy_ = &sql_proxy;
  OZ (codegen_lock_.init(1024));
  OZ (jit_lock_.first.init(1024));
  OZ (jit_lock_.second.init(1024));
  OZ (interface_service_.init());
  OX (serialize_composite_callback = ObUserDefinedType::serialize_obj);
  OX (deserialize_composite_callback = ObUserDefinedType::deserialize_obj);
  OX (composite_serialize_size_callback = ObUserDefinedType::get_serialize_obj_size);
  return ret;
}

void ObPLCtx::reset_obj()
{
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < objects_.count(); ++i) {
    if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(objects_.at(i)))) {
      LOG_WARN_RET(tmp_ret, "failed to destruct pl object", K(i), K(tmp_ret));
    }
  }
  objects_.reset();
}

void ObPLCtx::reset_obj_range_to_end(int64_t index)
{
  int tmp_ret = OB_SUCCESS;
  if (index < objects_.count() && index >= 0) {
    for (int64_t i = objects_.count() - 1; i >= index; i--) {
      if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(objects_.at(i)))) {
        LOG_WARN_RET(tmp_ret, "failed to destruct pl object", K(i), K(tmp_ret));
      }
      objects_.pop_back();
    }
  }
}

ObPLCtx::~ObPLCtx()
{
  reset_obj();
}

void ObPL::destory()
{
  codegen_lock_.destroy();
}

int ObPL::execute_proc(ObPLExecCtx &ctx,
                       uint64_t package_id,
                       uint64_t proc_id,
                       int64_t *subprogram_path,
                       int64_t path_length,
                       uint64_t loc,
                       int64_t argc,
                       common::ObObjParam **argv,
                       int64_t *nocopy_argv,
                       uint64_t dblink_id)
{
  int ret = OB_SUCCESS;
  lib::MemoryContext mem_context;
  if (OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(ctx.exec_ctx_)
      || OB_ISNULL(ctx.exec_ctx_->get_sql_ctx())
      || OB_ISNULL(ctx.result_)
      || OB_ISNULL(ctx.status_)
      || OB_ISNULL(ctx.allocator_)
      || (NULL == subprogram_path && path_length > 0)
      || (NULL != subprogram_path && 0 == path_length)
      || (NULL == nocopy_argv && argc > 0)) {
    ret = OB_ERR_UNEXPECTED;
  LOG_WARN("argument is NULL",
             K(GCTX.schema_service_),
             K(ctx.exec_ctx_),
             K(ctx.result_),
             K(ctx.status_),
             K(ctx.allocator_),
             K(subprogram_path),
             K(path_length),
             K(nocopy_argv),
             K(ret));
  } else if (OB_FAIL(ObSPIService::spi_check_early_exit(&ctx))) {
    LOG_WARN("failed to check early exit", K(ret));
  } else {
    lib::ContextParam param;
    OX (param.set_mem_attr(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(),
                            ObModIds::OB_PL_TEMP,
                            ObCtxIds::DEFAULT_CTX_ID));
    OZ (CURRENT_CONTEXT->CREATE_CONTEXT(mem_context, param));
    CK (OB_NOT_NULL(mem_context));


    ObSEArray<int64_t, 8> path_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < path_length; ++i) {
      if (OB_INVALID_INDEX == subprogram_path[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("path is invalid", K(i), K(subprogram_path[i]), K(path_length), K(ret));
      } else if (OB_FAIL(path_array.push_back(subprogram_path[i]))) {
        LOG_WARN("push back error", K(i), K(subprogram_path[i]), K(ret));
      } else { /*do nothing*/ }
    }

    ParamStore proc_params((ObWrapperAllocator(mem_context->get_arena_allocator())));
    ObSEArray<int64_t, 8> nocopy_params;
    if (OB_SUCC(ret) && NULL != argv && argc > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < argc; ++i) {
        if (OB_ISNULL(argv[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arg is NULL", K(i), K(proc_id), K(argc), K(ret));
        } else if (OB_FAIL(proc_params.push_back(*argv[i]))) {
          LOG_WARN("push back error", K(i), K(argv[i]), K(ret));
        } else {
          OZ (nocopy_params.push_back(nocopy_argv[i]));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      share::schema::ObSchemaGetterGuard schema_guard;
      const uint64_t tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else {
        ObPL pl;
        share::schema::ObSchemaGetterGuard *old_schema_guard = ctx.exec_ctx_->get_sql_ctx()->schema_guard_;
        ctx.exec_ctx_->get_sql_ctx()->schema_guard_ = &schema_guard;
        try {
          if (OB_FAIL(pl.execute(*ctx.exec_ctx_,
                                 ctx.exec_ctx_->get_allocator(),
                                 package_id,
                                 proc_id,
                                 path_array,
                                 proc_params,
                                 nocopy_params,
                                 *ctx.result_,
                                 ctx.status_,
                                 true,
                                 ctx.in_function_,
                                 loc,
                                 false,
                                 dblink_id))) {
            LOG_WARN("failed to execute pl", K(ret), K(package_id), K(proc_id), K(ctx.in_function_));
          }
        } catch (...) {
          ctx.exec_ctx_->get_sql_ctx()->schema_guard_ = old_schema_guard;
          throw;
        }
        if (OB_SUCC(ret)) {
          if (NULL != argv && argc > 0) {
            for (int64_t i = 0; OB_SUCC(ret) && i < argc; ++i) {
              *argv[i] = proc_params.at(i);
            }
          }
        }
        ctx.exec_ctx_->get_sql_ctx()->schema_guard_ = old_schema_guard; //这里其实没有必要置回来，但是不置的话schema_guard的生命周期结束后会变成野指针太危险了
        // support `SHOW WARNINGS` in mysql PL
        if (OB_FAIL(ret)) {
          ctx.exec_ctx_->get_my_session()->set_show_warnings_buf(ret);
        }
      }
    }
  }
  if (NULL != mem_context) {
    DESTROY_CONTEXT(mem_context);
    mem_context = NULL;
  }
  if (OB_ISNULL(ctx.status_)) {
    ret = OB_SUCCESS == ret ? OB_INVALID_ARGUMENT : ret;
    LOG_WARN("status in is NULL", K(ctx.status_), K(ret));
  } else {
    *ctx.status_ = ret;
  }
  return ret;
}

int ObPL::set_user_type_var(ObPLExecCtx *ctx, int64_t var_index, int64_t var_addr, int64_t init_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null");
  } else if (OB_ISNULL(ctx->params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_ is null");
  } else if (OB_UNLIKELY(var_index < 0) || OB_UNLIKELY(var_index > ctx->params_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_index is invalid", K(var_index), K(var_index > ctx->params_->count()));
  } else {
    ObObjParam &obj_param = ctx->params_->at(var_index);
    obj_param.set_extend(var_addr,
                         obj_param.get_meta().get_extend_type(),
                         (0 == obj_param.get_val_len()) ? init_size : obj_param.get_val_len());
    obj_param.set_param_meta();
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ctx->status_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("status in is NULL", K(ctx->status_), K(ret));
    } else {
      *ctx->status_ = ret;
    }
  }
  return ret;
}

int ObPL::set_implicit_cursor_in_forall(ObPLExecCtx *ctx, bool save_exception)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(ctx),
     OB_NOT_NULL(ctx->exec_ctx_),
     OB_NOT_NULL(ctx->exec_ctx_->get_my_session()),
     OB_NOT_NULL(ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor()));
  if (OB_SUCC(ret)) {
    pl::ObPLCursorInfo *cursor = ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor();
    cursor->set_in_forall(save_exception);
  }
  return ret;
}

int ObPL::unset_implicit_cursor_in_forall(ObPLExecCtx *ctx)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(ctx),
     OB_NOT_NULL(ctx->exec_ctx_),
     OB_NOT_NULL(ctx->exec_ctx_->get_my_session()),
     OB_NOT_NULL(ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor()));
  if (OB_SUCC(ret)) {
    pl::ObPLCursorInfo *cursor = ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor();
    cursor->unset_in_forall();
    if (cursor->get_bulk_exception_count() > 0) {
      ret = OB_ERR_IN_ARRAY_DML;
      LOG_USER_ERROR(OB_ERR_IN_ARRAY_DML);
    }
  }
  return ret;
}

int ObPLContext::check_debug_priv(ObSchemaGetterGuard *guard,
                                  sql::ObSQLSessionInfo *sess_info,
                                  ObPLFunction *func)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(guard, sess_info, func);
#else
  if (OB_ISNULL(guard) || OB_ISNULL(sess_info) || OB_ISNULL(func)) {
    // do thing
  } else if (func->has_debug_priv()) {
    // do nothing
    // according to oracle, if a routine has debug priv, before debug stop, it will hold it
    // even if it's debug priv is revoked during the debug process.
  } else {
    uint64_t package_id = func->get_package_id();
    uint64_t routine_id = func->get_routine_id();
    uint64_t obj_owner_id = func->get_owner();
    uint64_t db_id = func->get_database_id();
    uint64_t obj_id = OB_INVALID_ID;
    ObObjectType obj_type = ObObjectType::INVALID;
    const share::schema::ObDatabaseSchema *db_schema = NULL;
    const uint64_t tenant_id = func->get_tenant_id();
    OZ (guard->get_database_schema(tenant_id, db_id, db_schema), db_id);
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID != package_id) {
        if (ObUDTObjectType::is_object_id_masked(package_id)) {
          obj_type = ObObjectType::TYPE;
        } else {
          obj_type = ObObjectType::PACKAGE;
        }
        obj_id = package_id;
      } else if (OB_INVALID_ID != routine_id) {
        if (func->is_function()) {
          obj_type = ObObjectType::FUNCTION;
        } else {
          obj_type = ObObjectType::PROCEDURE;
        }
        obj_id = routine_id;
      }
      if (OB_INVALID_ID != obj_id) {
        uint64_t tenant_id = OB_INVALID_ID;
        uint64_t user_id = OB_INVALID_ID;
        CK (OB_NOT_NULL(sess_info));
        OX (tenant_id = sess_info->get_effective_tenant_id());
        OX (user_id = sess_info->get_user_id());
        // first check debug any procedure
        OZ (ObDBMSDebug::check_debug_sys_priv_impl(*guard, *sess_info, PRIV_ID_DEBUG_ANY_PROC));
        if (OB_ERR_NO_PRIVILEGE == ret || OB_ERR_NO_SYS_PRIVILEGE == ret) {
          ret = OB_SUCCESS;
          OZ (sql::ObOraSysChecker::check_ora_obj_priv(*guard,
                          tenant_id,
                          user_id,
                          db_schema->get_database_name_str(),
                          obj_id,
                          OBJ_LEVEL_FOR_TAB_PRIV,
                          static_cast<int64_t>(obj_type),
                          OBJ_PRIV_ID_DEBUG,
                          CHECK_FLAG_NORMAL,
                          obj_owner_id,
                          sess_info->get_enable_role_array()),
                          user_id, obj_id, obj_type, obj_owner_id);
        }
        if (OB_ERR_NO_PRIVILEGE == ret || OB_ERR_NO_SYS_PRIVILEGE == ret) {
          func->clean_debug_priv();
        } else if (OB_SUCC(ret)) {
          func->set_debug_priv();
        } else {
          // do nothing
        }
      }
    }
  }
#endif
  return ret;
}

int ObPLContext::debug_start(ObSQLSessionInfo *sql_session)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSED(sql_session);
#else
  pl::debugger::ObPLDebugger *pl_debugger = NULL;
  if (sql_session->is_pl_debug_on()) {
    CK (OB_NOT_NULL(pl_debugger = sql_session->get_pl_debugger()));
    if (OB_SUCC(ret) && !pl_debugger->is_debug_thread_running()) {
      OZ (pl_debugger->debug_start());
    }
  }
#endif
  return ret;
}

int ObPLContext::debug_stop(ObSQLSessionInfo *sql_session)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSED(sql_session);
#else
  pl::debugger::ObPLDebugger *pl_debugger = NULL;
  OX (pl_debugger = sql_session->get_pl_debugger());
  if (OB_SUCC(ret)
      && sql_session->is_pl_debug_on()
      && NULL == sql_session->get_pl_context() // we only stop debugger on top pl/sql
      && pl_debugger->is_debug_thread_running()) {
    OZ (pl_debugger->debug_stop());
    if (OB_SUCC(ret) && pl_debugger->need_debug_off()) {
      OZ (sql_session->free_pl_debugger());
    }
  }
#endif
  return ret;
}

int ObPLContext::notify(ObSQLSessionInfo *sql_session)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSED(sql_session);
#else
  pl::debugger::ObPLDebugger *pl_debugger = NULL;
  OX (pl_debugger = sql_session->get_pl_debugger());
  if (OB_SUCC(ret)
      && sql_session->is_pl_debug_on()
      && pl_debugger->is_debug_thread_running()) {
    OZ (pl_debugger->notify());
  } else if (pl_debugger != NULL) {
    LOG_INFO("[TARGET THREAD] No Need To Notify!",
             K(ret),
             K(sql_session->is_pl_debug_on()),
             K(pl_debugger->is_debug_thread_running()));
  }
#endif
  return ret;
}

void ObPLContext::record_tx_id_before_begin_autonomous_session_for_deadlock_(ObSQLSessionInfo &session_info,
                                                                             ObTransID &last_trans_id)
{
  last_trans_id = session_info.get_tx_id();
}

void ObPLContext::register_after_begin_autonomous_session_for_deadlock_(ObSQLSessionInfo &session_info,
                                                                        const ObTransID last_trans_id)
{
  ObTransID now_trans_id = session_info.get_tx_id();
  // 上一个事务等自治事务结束，若自治事务要加上一个事务已经持有的锁，则会死锁
  // 为检测死锁，需要注册上一个事务到自治事务的等待关系
  if (last_trans_id != now_trans_id &&
      last_trans_id.is_valid() &&
      now_trans_id.is_valid()) {
    int ret = OB_SUCCESS;
    int64_t query_timeout = 0;
    if (OB_FAIL(session_info.get_query_timeout(query_timeout))) {
      DETECT_LOG(WARN, "get query timeout failed", K(last_trans_id), K(now_trans_id), KR(ret));
    } else {
      if (OB_FAIL(ObTransDeadlockDetectorAdapter::
                  autonomous_register_to_deadlock(last_trans_id,
                                                  now_trans_id,
                                                  query_timeout))) {
        DETECT_LOG(WARN, "autonomous register to deadlock failed",
                         K(last_trans_id), K(now_trans_id), KR(ret));
      }
    }
  } else {
    DETECT_LOG_RET(WARN, OB_ERR_UNEXPECTED, "not register to deadlock", K(last_trans_id), K(now_trans_id));
  }
}

int ObPLContext::init(ObSQLSessionInfo &session_info,
                       ObExecContext &ctx,
                       ObPLFunction *routine,
                       bool is_function_or_trigger,
                       ObIAllocator *allocator,
                       const bool is_dblink)
{
  int ret = OB_SUCCESS;
  int64_t pl_block_timeout = 0;
  int64_t query_start_time = session_info.get_query_start_time();
  if (!is_dblink) {
    CK (OB_NOT_NULL(routine));
    OX (is_function_or_trigger |= routine->is_function());
    OX (is_autonomous_ = routine->is_autonomous());
    OX (is_function_or_trigger_ = is_function_or_trigger);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ObCurTraceId::get_trace_id())) {
    trace_id_.set(*ObCurTraceId::get_trace_id());
  }

  OZ (session_info.get_pl_block_timeout(pl_block_timeout));
  OX (pl_block_timeout = std::min(pl_block_timeout, OB_MAX_USER_SPECIFIED_TIMEOUT));
  if (is_function_or_trigger
      && (ObTimeUtility::current_time() + pl_block_timeout) < THIS_WORKER.get_timeout_ts()) {
    OX (old_worker_timeout_ts_ = THIS_WORKER.get_timeout_ts());
    OX (THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + pl_block_timeout));
    if (OB_SUCC(ret) && OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
      old_phy_plan_timeout_ts_ = ctx.get_physical_plan_ctx()->get_timeout_timestamp();
      ctx.get_physical_plan_ctx()->set_timeout_timestamp(ObTimeUtility::current_time() + pl_block_timeout);
    }
  }
  if (OB_ISNULL(session_info.get_pl_context())) {
    if (lib::is_mysql_mode()) {
      OX (session_info.set_show_warnings_buf(OB_SUCCESS));
    }
    OZ (session_info.shrink_package_info());
    OX (cursor_info_.reset());
    OX (cursor_info_.set_implicit());
    OX (sqlcode_info_.reset());
    OX (is_top_stack_ = true);

    if (!is_function_or_trigger) {
      OX (THIS_WORKER.set_timeout_ts(query_start_time + pl_block_timeout));
      if (OB_SUCC(ret) && OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
        ctx.get_physical_plan_ctx()->set_timeout_timestamp(query_start_time + pl_block_timeout);
      }
    }
    if (lib::is_oracle_mode()) {
      OZ (ObPLContext::debug_start(&session_info));
      if (OB_SUCC(ret) && !in_nested_sql_ctrl()) {
        /*!
         * 如果已经开始了STMT, 说明在嵌套语句中, 此时不需要设置SAVEPOINT,
         * 因为目前嵌套语句的实现保证了不需要再嵌套语句内部回滚PL的执行;
         * 主要是两种情况: 1. FUNCTION, TRIGGER中执行DML 2. FUNCTION,TRIGGER 中执行SELECT
         * 情况2没有回滚的需求
         * 情况1目前的实现会限制FUNCTION,TRIGGER中不能有异常捕获语句, 因此也没有回滚的需求
         */
        // 如果已经在事务中，则需要创建一个回滚点，用于在PL中的语句失败时回滚到PL的开始点；
        // 如果没有在事务中，则不需要创建回滚点，在PL失败时直接回滚整个事务就可以了；
        if (OB_NOT_NULL(session_info.get_tx_desc()) && session_info.get_tx_desc()->in_tx_or_has_extra_state()) {
          OZ (ObSqlTransControl::create_savepoint(ctx, PL_IMPLICIT_SAVEPOINT));
          OX (has_implicit_savepoint_ = true);
          LOG_DEBUG("create pl implicit savepoint for oracle", K(ret), K(PL_IMPLICIT_SAVEPOINT));
        }
      }
      if (OB_SUCC(ret) && session_info.get_local_autocommit()) {
        reset_autocommit_ = true;
        OZ (session_info.set_autocommit(false));
      }
    } else { // MySQL Mode
      // PL/SQL in MySQL mode may need to retry on LOCK_ON_CONFLICT error.
      // for retry PL/SQL, we create a savepoint here,
      // if failed, rollback to this savepoint, and PL/SQL caller will retry.
      if (OB_NOT_NULL(session_info.get_tx_desc()) &&
          session_info.get_tx_desc()->in_tx_or_has_extra_state() && !in_nested_sql_ctrl()) {
        OZ (ObSqlTransControl::create_savepoint(ctx, PL_IMPLICIT_SAVEPOINT));
        OX (has_implicit_savepoint_ = true);
        LOG_DEBUG("create pl implicit savepoint for mysql", K(ret), K(PL_IMPLICIT_SAVEPOINT));
      }
      if (is_function_or_trigger && session_info.get_local_autocommit()) {
        OX (reset_autocommit_ = true);
        OZ (session_info.set_autocommit(false));
      }
    }

    OZ (ob_write_string(allocator != NULL ? *allocator
                        : ctx.get_allocator(), session_info.get_current_query_string(), cur_query_));

    OZ (recursion_ctx_.init(session_info));
    OX (session_info.set_pl_stack_ctx(this));
    OX (session_info.set_pl_can_retry(true));
  } else if (is_function_or_trigger && lib::is_mysql_mode()) {
    //mysql模式, 内层function或者trigger不需要创建隐式savepoint, 只需要重置ac
    //如果是procedure调udf场景:
    // ac = 0时, udf内部的dml操作需要回滚, 需要创建隐式savepoint
    // ac = 1时, procedure内部的dml语句执行完立即提交了, 所以只考虑执行udf语句本身:
    // 1. dml触发udf, 语句本身开事务, udf执行报错时, 内部无需回滚, 跟随外面的dml语句回滚即可
    // 2. 不开事务的dml语句, 没有事务, 无需创建隐式回滚点, 内部无需回滚, 跟随外面的dml语句回滚即可
    // 3. pl内部表达式中调用udf, 没有事务, 无需创建隐式回滚点, 内层udf内部如果开事务了, destory阶段整体回滚或提交即可
    //如果是udf调udf场景, 外层udf已经重置ac, 且内层udf跟随外层udf在destory阶段的回滚或提交
    if (OB_NOT_NULL(session_info.get_tx_desc()) &&
        session_info.get_tx_desc()->in_tx_or_has_extra_state() && !in_nested_sql_ctrl()) {
      OZ (ObSqlTransControl::create_savepoint(ctx, PL_IMPLICIT_SAVEPOINT));
      OX (has_implicit_savepoint_ = true);
      LOG_DEBUG("create pl implicit savepoint for mysql", K(ret), K(PL_IMPLICIT_SAVEPOINT));
    }
    if (is_function_or_trigger && session_info.get_local_autocommit()) {
      OX (reset_autocommit_ = true);
      OZ (session_info.set_autocommit(false));
    }
  }

  if (OB_SUCC(ret) && is_function_or_trigger && lib::is_mysql_mode() &&
      routine->get_has_parallel_affect_factor()) {
    // 并行场景下不能创建stash savepoint, 只有当udf/trigger内部有tcl语句时, stash savepoint才有意义
    // udf内部有tcl语句时，该标记为true
    const ObString stash_savepoint_name("PL stash savepoint");
    OZ (ObSqlTransControl::create_stash_savepoint(ctx, stash_savepoint_name));
    OX (has_stash_savepoint_ = true);
  }
  if (OB_SUCC(ret) && is_function_or_trigger && lib::is_mysql_mode()) {
    last_insert_id_ = session_info.get_local_last_insert_id();
  }
  if (OB_SUCC(ret) && is_autonomous_) {
    has_inner_dml_write_ = session_info.has_exec_inner_dml();
    session_info.set_has_exec_inner_dml(false);

    ObTransID last_trans_id;
    (void) record_tx_id_before_begin_autonomous_session_for_deadlock_(session_info, last_trans_id);
    OZ (session_info.begin_autonomous_session(saved_session_));
    OX (saved_has_implicit_savepoint_ = session_info.has_pl_implicit_savepoint());
    OX (session_info.clear_pl_implicit_savepoint());
    OZ (ObSqlTransControl::explicit_start_trans(ctx, false));
    (void) register_after_begin_autonomous_session_for_deadlock_(session_info, last_trans_id);
  }
  OX (session_info_ = &session_info);
  return ret;
}

int ObPLContext::implicit_end_trans(
  ObSQLSessionInfo &session_info, ObExecContext &ctx, bool is_rollback, bool can_async)
{
  int ret = OB_SUCCESS;
  bool is_async = false;
  if (session_info.is_in_transaction()) {
    is_async = !is_rollback && ctx.is_end_trans_async() && can_async;
    if (!is_async) {
      if (OB_FAIL(ObSqlTransControl::implicit_end_trans(ctx, is_rollback))) {
        LOG_WARN("failed to implicit end trans with sync callback", K(ret));
      }
    } else {
      ObEndTransAsyncCallback &callback = session_info.get_end_trans_cb();
      if (OB_FAIL(ObSqlTransControl::implicit_end_trans(ctx, is_rollback, &callback))) {
        LOG_WARN("failed implicit end trans with async callback", K(ret));
      }
      ctx.get_trans_state().set_end_trans_executed(OB_SUCCESS == ret);
    }
  } else {
    ObSqlTransControl::reset_session_tx_state(&session_info, true);
    ctx.set_need_disconnect(false);
  }
  LOG_TRACE("pl.implicit_end_trans", K(is_async), K(session_info), K(can_async), K(is_rollback));
  return ret;
}

void ObPLContext::destory(
  ObSQLSessionInfo &session_info, ObExecContext &ctx, int &ret)
{
  int trans_state_ret = OB_SUCCESS;
  if (is_autonomous_ && session_info.is_in_transaction() && session_info.has_exec_inner_dml()) {
    trans_state_ret =  OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK;
    LOG_WARN("active autonomous transaction detected", K(trans_state_ret));
    ret = COVER_SUCC(trans_state_ret);
  }

  if (old_worker_timeout_ts_ != 0) {
    THIS_WORKER.set_timeout_ts(old_worker_timeout_ts_);
    if (OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
      ctx.get_physical_plan_ctx()->set_timeout_timestamp(old_phy_plan_timeout_ts_);
    }
  }

  if (lib::is_mysql_mode()
      && OB_NOT_NULL(ctx.get_physical_plan_ctx())
      && !is_function_or_trigger_) {
    ctx.get_physical_plan_ctx()->set_affected_rows(get_cursor_info().get_rowcount());
  }

  if (lib::is_mysql_mode() && is_function_or_trigger_) {
    uint64_t cur_last_insert_id = session_info.get_local_last_insert_id();
    if (cur_last_insert_id != last_insert_id_) {
      ObObj last_insert_id;
      int update_ret = OB_SUCCESS;
      last_insert_id.set_uint64(last_insert_id_);
      update_ret = session_info.update_sys_variable(SYS_VAR_LAST_INSERT_ID, last_insert_id);
      if (OB_SUCCESS == update_ret &&
          OB_SUCCESS != (update_ret = session_info.update_sys_variable(SYS_VAR_IDENTITY, last_insert_id))) {
        LOG_WARN("succ update last_insert_id, but fail to update identity", K(update_ret));
      }
      ret = OB_SUCCESS == ret ? update_ret : ret;
    }
    if (has_stash_savepoint_) {
      const ObString stash_savepoint_name("PL stash savepoint");
      int pop_ret = ObSqlTransControl::release_stash_savepoint(ctx, stash_savepoint_name);
      if (OB_SUCCESS != pop_ret) {
        LOG_WARN("fail to release stash savepoint", K(pop_ret));
        ret = OB_SUCCESS == ret ? pop_ret : ret;
      }
    }
  }

  if (is_top_stack_) {
    if (OB_ISNULL(session_info_)
        || session_info_ != &session_info
        || session_info_->get_pl_context() != this) {
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      LOG_ERROR("current stack ctx is top, but session info is not", K(ret));
    } else {
#define IS_DBLINK_TRANS \
  transaction::ObTxDesc *tx_desc = session_info.get_tx_desc();  \
  const transaction::ObXATransID xid = session_info.get_xid();  \
  const transaction::ObGlobalTxType global_tx_type = tx_desc->get_global_tx_type(xid);  \
  bool is_dblink = (transaction::ObGlobalTxType::DBLINK_TRANS == global_tx_type);

      if (!in_nested_sql_ctrl() &&
          lib::is_mysql_mode() && is_function_or_trigger_ &&
          OB_SUCCESS == ret &&
          reset_autocommit_ &&
          session_info.is_in_transaction()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported cmd execute udf which has dml stmt", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "use cmd stmt execute udf which has dml stmt");
      }
      if (OB_SUCCESS != ret && session_info.is_in_transaction()) { // PL执行失败, 需要回滚
        int tmp_ret = OB_SUCCESS;
        if (has_implicit_savepoint_) {
          // ORACLE: alreay rollback to PL/SQL start.
          // MYSQL : rollback only if OB_TRY_LOCK_ROW_CONFLICT==ret and PL/SQL can retry.
          if (lib::is_oracle_mode() ||
              (lib::is_mysql_mode() &&
              ((OB_TRY_LOCK_ROW_CONFLICT == ret && session_info.get_pl_can_retry()) ||
              is_function_or_trigger_))) {
            if (OB_SUCCESS !=
                  (tmp_ret = ObSqlTransControl::rollback_savepoint(ctx, PL_IMPLICIT_SAVEPOINT))) {
              LOG_WARN("failed to rollback current pl to implicit savepoint", K(ret), K(tmp_ret));
            }
            LOG_DEBUG("rollback pl to implicit savepoint", K(ret), K(tmp_ret));
          } else if (lib::is_mysql_mode()) {
            session_info.set_pl_can_retry(false);
          }
#ifdef OB_BUILD_ORACLE_PL
        } else if (session_info.associated_xa()) {
          IS_DBLINK_TRANS;
          if (is_dblink) {
            if (in_nested_sql_ctrl()) {
              // rollback or commit by the top sql.
            } else {
              transaction::ObTransID tx_id;
              const bool force_disconnect = false;
              int rl_ret = OB_SUCCESS;
              if (OB_SUCCESS != (rl_ret = ObTMService::tm_rollback(ctx, tx_id))) {
                LOG_WARN("failed to rollback for dblink trans", K(ret), K(rl_ret), K(tx_id), K(xid), K(global_tx_type));
              } else if (OB_SUCCESS != (rl_ret =
                                        session_info.get_dblink_context().clean_dblink_conn(force_disconnect))) {
                LOG_WARN("dblink trans failed to release dblink connections", K(ret), K(rl_ret), K(tx_id), K(xid));
              }
              ret = OB_SUCCESS == ret ? rl_ret : ret;
            }
          } else if (OB_TRANS_XA_BRANCH_FAIL != ret) {
            tmp_ret = ObDbmsXA::xa_rollback_savepoint(ctx);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("xa trans roll back to save point failed",
                      K(tmp_ret), KPC(session_info.get_tx_desc()));
            }
          }
#endif
        } else if (!in_nested_sql_ctrl() && session_info.get_in_transaction()) {
          // 如果没有隐式的检查点且不再嵌套事务中, 说明当前事务中仅包含该PL, 直接回滚事务
          // 嵌套语句中的PL会随着顶层的语句一起回滚, 不需要单独回滚
          // ORACLE: alreay rollback to PL/SQL start.
          // MYSQL : rollback only if OB_TRY_LOCK_ROW_CONFLICT==ret and PL/SQL can retry.
          if (lib::is_oracle_mode() ||
             (lib::is_mysql_mode() &&
             ((OB_TRY_LOCK_ROW_CONFLICT == ret && session_info.get_pl_can_retry()) ||
             is_function_or_trigger_))) {
            tmp_ret = implicit_end_trans(session_info, ctx, true);
          } else if (lib::is_mysql_mode()) {
            session_info.set_pl_can_retry(false);
          }
        }
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      } else if (!is_autonomous_ && reset_autocommit_ && !in_nested_sql_ctrl() &&
                (lib::is_oracle_mode() || (lib::is_mysql_mode() && is_function_or_trigger_))) {
                /* 非dml出发点的udf, 如set @a= f1(), 需要在udf内部提交 */
        if (has_implicit_savepoint_) {
          // reset_autocommit_ && has_implicit_savepoint_ equal to scene of ac=1 && explict transaction.
          // no need to commit
        } else if (!session_info.associated_xa()) {
          //如果当前事务是xa事务,则不提交当前事务,只设置ac=true.否则提交当前事务
          // 先COMMIT, 然后再修改AutoCommit
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS == ret
              //异步提交无法带给proxy未hit信息(ObPartitionHitInfo默认值是Hit),如果未hit走同步提交
              && session_info_->partition_hit().get_bool()
              // 如果顶层调用有出参也不走异步提交, 因为要向客户端回数据
              && !has_output_arguments()) {
            if (OB_SUCCESS !=
                (tmp_ret = implicit_end_trans(session_info, ctx, false, true))) {
              // 不覆盖原来的错误码
              LOG_WARN("failed to explicit end trans", K(ret), K(tmp_ret));
            } else {
              LOG_DEBUG("explicit end trans success!", K(ret));
            }
          } else { // 不确定上层是否会扔回队列重试,因此失败了一定要走同步提交
            // always call commit/rollback txn in order to reset txn because of autocommit
            tmp_ret = implicit_end_trans(session_info, ctx, ret != OB_SUCCESS);
          }
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        } else {
          // in XA trans, check whether the trans is dblink trans.
          int cm_ret = OB_SUCCESS;
          IS_DBLINK_TRANS;
          if (is_dblink) {
            transaction::ObTransID tx_id;
            const bool force_disconnect = false;
            if (OB_SUCCESS !=(cm_ret = ObTMService::tm_commit(ctx, tx_id))) {
              LOG_WARN("failed to commit dblink trans", K(ret), K(tx_id), K(xid), K(global_tx_type));
            } else if (OB_SUCCESS != (cm_ret = session_info.get_dblink_context().clean_dblink_conn(force_disconnect))) {
              LOG_WARN("dblink trans failed to release dblink connections", K(ret), K(cm_ret), K(tx_id), K(xid));
            }
            ret = OB_SUCCESS == ret ? cm_ret : ret;
          }
          ctx.set_need_disconnect(false);
        }
      }

      // 清理serially package
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS !=
        (tmp_ret = session_info.reset_all_serially_package_state())) {
        LOG_WARN("failed to reset all serially package state", K(ret), K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    if (!cur_query_.empty()) {
      int tmp_ret = session_info.store_query_string(cur_query_);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to restore query string", K(ret), K(cur_query_));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    // 无论如何恢复session上的状态
    session_info.set_pl_stack_ctx(NULL);
    session_info_ = NULL;

    IGNORE_RETURN ObPLContext::debug_stop(&session_info);
#undef IS_DBLINK_TRANS
  } else if (lib::is_mysql_mode() && is_function_or_trigger_) {
    // 非嵌套场景: 内层udf一定是在表达式里面, 提交由spi_calc_expr处来保证
    // 嵌套场景: 内层udf被dml语句触发, 回滚或提交由外层dml语句保证
    if (OB_SUCCESS != ret && session_info.is_in_transaction()) { // PL执行失败, 需要回滚
      int tmp_ret = OB_SUCCESS;
      if (has_implicit_savepoint_) {
        if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::rollback_savepoint(ctx, PL_IMPLICIT_SAVEPOINT))) {
          LOG_WARN("failed to rollback current pl to implicit savepoint", K(ret), K(tmp_ret));
        }
#ifdef OB_BUILD_ORACLE_PL
      } else if (session_info.associated_xa()) {
        if (OB_TRANS_XA_BRANCH_FAIL != ret) {
          tmp_ret = ObDbmsXA::xa_rollback_savepoint(ctx);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("xa trans roll back to save point failed",
                    K(tmp_ret), KPC(session_info.get_tx_desc()));
          }
        }
#endif
      } else if (!in_nested_sql_ctrl() && session_info.get_in_transaction()) {
        tmp_ret = implicit_end_trans(session_info, ctx, true);
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    // 无论如何都还原autocommit值
    if (reset_autocommit_) {
      int tmp_ret = OB_SUCCESS;
      tmp_ret = session_info.set_autocommit(true);
      if (tmp_ret != OB_SUCCESS) {
        LOG_ERROR("restore autocommit value failed", K(tmp_ret), K(ret));
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  if (is_autonomous_) {
    int end_trans_ret = end_autonomous(ctx, session_info);
    ret = OB_SUCCESS == ret ? end_trans_ret : ret;
  }
  if (is_top_stack_) {
    // 无论如何都还原autocommit值
    if (reset_autocommit_) {
      int tmp_ret = OB_SUCCESS;
      tmp_ret = session_info.set_autocommit(true);
      if (tmp_ret != OB_SUCCESS) {
        LOG_ERROR("restore autocommit value failed", K(tmp_ret), K(ret));
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
}

int ObPLContext::end_autonomous(ObExecContext &ctx, sql::ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;

  int end_trans_ret =
      session_info.is_in_transaction() ? implicit_end_trans(session_info, ctx, true) : OB_SUCCESS;
  int switch_trans_ret = session_info.end_autonomous_session(saved_session_);
  if (OB_SUCCESS != end_trans_ret) {
    LOG_WARN("failed to rollback trans", K(end_trans_ret));
    ret = end_trans_ret;
  }
  if (OB_SUCCESS != switch_trans_ret) {
    LOG_WARN("failed to switch trans", K(switch_trans_ret));
    ret = switch_trans_ret;
  }
  session_info.set_has_pl_implicit_savepoint(saved_has_implicit_savepoint_);
  session_info.set_has_exec_inner_dml(has_inner_dml_write_);
  return ret;
}

bool ObPLContext::in_autonomous() const
{
  bool bret = false;
  const ObPLContext *cur_stack = this;
  //traverse all pl stack context inside the same exec ctx
  while (!bret && cur_stack != nullptr && cur_stack->my_exec_ctx_ == my_exec_ctx_) {
    bret = cur_stack->is_autonomous();
    cur_stack = cur_stack->parent_stack_ctx_;
  }
  return bret;
}

ObPLContext* ObPLContext::get_stack_pl_ctx()
{
  int ret = OB_SUCCESS;
  ObPLContext *ctx = NULL;
  CK (!is_top_stack_);
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(ctx = session_info_->get_pl_context()));
  CK (ctx->is_top_stack());
  return ctx;
}

int ObPLContext::inc_and_check_depth(int64_t package_id,
                                      int64_t routine_id,
                                      bool is_function)
{
  int ret = OB_SUCCESS;
  if (is_top_stack_) {
    OZ (recursion_ctx_.inc_and_check_depth(package_id, routine_id, is_function));
  } else {
    ObPLContext *ctx = NULL;
    CK (OB_NOT_NULL(ctx = get_stack_pl_ctx()));
    OZ (ctx->inc_and_check_depth(package_id, routine_id, is_function));
  }
  if (OB_SUCC(ret)) {
    inc_recursion_depth_ = true;
  }
  return ret;
}

void ObPLContext::dec_and_check_depth(int64_t package_id,
                                       int64_t routine_id,
                                       int &ret,
                                       bool inner_call = false)
{
  if (is_top_stack_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = recursion_ctx_.dec_and_check_depth(package_id, routine_id))) {
      LOG_WARN("failed to dec and check depth",
               K(ret), K(tmp_ret), K(package_id), K(routine_id));
      ret = OB_SUCCESS == ret && inner_call ? tmp_ret : ret;
    }
  } else if (inc_recursion_depth_) {
    ObPLContext *ctx = NULL;
    if (OB_ISNULL(ctx = get_stack_pl_ctx())) {
      ret = OB_SUCCESS != ret ? ret : OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get top stack pl ctx in session", K(ret), K(ctx));
    } else {
      ctx->dec_and_check_depth(package_id, routine_id, ret, true);
    }
    inc_recursion_depth_ = false;
  }
}

int ObPLContext::check_stack_overflow()
{
  int ret = OB_SUCCESS;
  bool overflow = false;
  OZ (common::check_stack_overflow(overflow));
  if (OB_SUCC(ret) && overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret), K(lbt()));
  }
  return ret;
}

int ObPLContext::check_routine_legal(ObPLFunction &routine, bool in_function, bool in_tg)
{
  int ret = OB_SUCCESS;
  // 检查routine中语句的合法性
  if (in_function || in_tg) {
    if (routine.get_contain_dynamic_sql() && lib::is_mysql_mode()) {
      ret = OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG;
      LOG_WARN("Dynamic SQL is not allowed in stored function", K(ret));
      LOG_USER_ERROR(OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG, "Dynamic SQL");
    } else if (routine.get_multi_results()) {
      ret = OB_ER_SP_NO_RETSET;
      LOG_WARN("Not allowed to return a result set in pl function", K(ret));
      if (in_tg) {
        LOG_USER_ERROR(OB_ER_SP_NO_RETSET, "trigger");
      } else {
        LOG_USER_ERROR(OB_ER_SP_NO_RETSET, "function");
      }
    } else if (routine.get_has_commit_or_rollback()) {
      ret = OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG;
      LOG_WARN("DDL SQL is not allowed in stored function", K(ret));
      LOG_USER_ERROR(OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG);
    } else if (routine.get_has_set_autocommit_stmt()) {
      ret = OB_ER_SP_CANT_SET_AUTOCOMMIT;
      LOG_USER_ERROR(OB_ER_SP_CANT_SET_AUTOCOMMIT);
      LOG_WARN("Not allowed to set autocommit from a stored function", K(ret));
    }
  }
  return ret;
}

int ObPLContext::set_exec_env(ObPLFunction &routine)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(my_exec_ctx_));
  OZ (exec_env_.load(*session_info_, &my_exec_ctx_->get_allocator()));
  if (OB_SUCC(ret) && exec_env_ != routine.get_exec_env()) {
    OZ (routine.get_exec_env().store(*session_info_));
    OX (need_reset_exec_env_ = true);
  }

  // always restore sql_mode in mysql mode,
  // because sql_mode may be change inside PL.
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    OX(need_reset_exec_env_ = true);
  }

  return ret;
}

void ObPLContext::reset_exec_env(int &ret)
{
  int tmp_ret = OB_SUCCESS;
  if (need_reset_exec_env_) {
    if (OB_ISNULL(session_info_)) {
      ret = OB_SUCCESS != ret ? ret : OB_ERR_UNEXPECTED;
      LOG_ERROR("current session is null", K(ret), K(session_info_));
    } else if (OB_SUCCESS != (tmp_ret = exec_env_.store(*session_info_))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret; // 不覆盖错误码
      LOG_WARN("failed to set exec_env", K(ret), K(tmp_ret), K(exec_env_));
    }
  }
}

int ObPLContext::set_role_id_array(ObPLFunction &routine,
                                   share::schema::ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  /* All roles are disabled in any named PL/SQL block (stored procedure, function, or trigger)
     that executes with definer's rights. Roles are not used for privilege checking
     and you cannot set roles within a definer's rights procedure. */

  if (ObSchemaChecker::is_ora_priv_check() && !routine.is_invoker_right()
      && routine.get_proc_type() != STANDALONE_ANONYMOUS) {
    bool is_special_ir = false;
    if (OB_FAIL(routine.is_special_pkg_invoke_right(guard, is_special_ir))){
      LOG_WARN("failed to check special pkg invoke right", K(ret));
    } else if (is_special_ir){
      need_reset_role_id_array_ = false;
    } else {
      uint64_t priv_user_id = OB_INVALID_ID;
      CK (OB_NOT_NULL(session_info_));
      /* 1. save in definer, just for more information */
      OX (old_in_definer_ = session_info_->get_in_definer_named_proc());
      OX (session_info_->set_in_definer_named_proc(true));
      if (OB_SUCC(ret)) {
        if (0 == session_info_->get_database_name().case_compare(OB_SYS_DATABASE_NAME)) {
          OX (priv_user_id = OB_ORA_SYS_USER_ID);
        } else {
          OZ (guard.get_user_id(session_info_->get_effective_tenant_id(),
                                session_info_->get_database_name(),
                                ObString(OB_DEFAULT_HOST_NAME),
                                priv_user_id,
                                false));
        }
      }
      if (OB_SUCC(ret) && OB_INVALID_ID == priv_user_id) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("fail to get procedure owner id",
                K(session_info_->get_effective_tenant_id()),
                K(session_info_->get_database_name()));
      }
      /* 2. save priv user id, and set new priv user id, change grantee_id, for priv check */
      OX (old_priv_user_id_ = session_info_->get_priv_user_id());
      OX (session_info_->set_priv_user_id(priv_user_id));
      /* 3. save role id array , remove role id array for priv check */
      OZ (old_role_id_array_.assign(session_info_->get_enable_role_array()));
      OX (session_info_->get_enable_role_array().reset());
      OZ (session_info_->get_enable_role_array().push_back(OB_ORA_PUBLIC_ROLE_ID));
      OX (need_reset_role_id_array_ = true);
    }
  } else if (lib::is_mysql_mode() && !routine.is_invoker_right() &&
             0 != routine.get_priv_user().length()
             /* 兼容存量存储过程，存量存储过程的priv_user为空。mysql存储过程默认为definer行为，
              当前ob mysql模式做成了默认invoker行为，支持definer后，ob mysql模式也默认为definer行为 */) {
    ObString priv_user = routine.get_priv_user();
    ObString user_name = priv_user.split_on('@');
    ObString host_name = priv_user;
    uint64_t priv_user_id = OB_INVALID_ID;
    const ObUserInfo *user_info = NULL;

    OZ (guard.get_user_info(session_info_->get_effective_tenant_id(),
                            user_name, host_name, user_info));
    if (OB_SUCC(ret) && OB_ISNULL(user_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("fail to get priv user id", K(session_info_->get_effective_tenant_id()),
                                           K(user_name), K(host_name), K(routine.get_priv_user()));
    }
    OX (priv_user_id = user_info->get_user_id());
    /* save priv user id, and set new priv user id, change grantee_id, for priv check */
    if (OB_SUCC(ret) && priv_user_id != session_info_->get_priv_user_id()) {
      //backup old session values firstly
      OX (old_priv_user_id_ = session_info_->get_priv_user_id());
      OZ (old_role_id_array_.assign(session_info_->get_enable_role_array()));
      OX (old_user_priv_set_ = session_info_->get_user_priv_set());
      OX (old_db_priv_set_ = session_info_->get_db_priv_set());

      OX (need_reset_role_id_array_ = true);

      //assign new session values
      //1. priv user_id
      OX (session_info_->set_priv_user_id(priv_user_id));
      //2. roles
      bool activate_all_role = false;
      OZ (session_info_->get_activate_all_role_on_login(activate_all_role));
      OX (session_info_->get_enable_role_array().reuse());
      for (int i = 0; OB_SUCC(ret) && i < user_info->get_role_id_array().count(); ++i) {
        if (activate_all_role
            || user_info->get_disable_option(user_info->get_role_id_option_array().at(i)) == 0) {
          OZ (session_info_->get_enable_role_array().push_back(user_info->get_role_id_array().at(i)));
        }
      }
      //3. user priv set
      OX (session_info_->set_user_priv_set(user_info->get_priv_set()));
      //4. db priv set
      ObPrivSet db_priv_set;
      OZ (guard.get_db_priv_set(session_info_->get_effective_tenant_id(),
                                priv_user_id, session_info_->get_database_name(), db_priv_set));
      OX (session_info_->set_db_priv_set(db_priv_set));
    }
  }
  return ret;
}

void ObPLContext::reset_role_id_array(int &ret)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_) && need_reset_role_id_array_) {
    // in_definer_named_proc and priv_user_id should always be reset even if ret != OB_SUCCESS
    if (lib::is_oracle_mode()) {
      session_info_->set_in_definer_named_proc(old_in_definer_);
      session_info_->set_priv_user_id(old_priv_user_id_);
      tmp_ret = session_info_->set_enable_role_array(old_role_id_array_);
      need_reset_role_id_array_ = false;
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else {
      //priv user
      session_info_->set_priv_user_id(old_priv_user_id_);
      //role array
      tmp_ret = session_info_->set_enable_role_array(old_role_id_array_);
      //user priv set
      session_info_->set_user_priv_set(old_user_priv_set_);
      //db priv set
      session_info_->set_db_priv_set(old_db_priv_set_);
      need_reset_role_id_array_ = false;
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
}

int ObPLContext::set_default_database(ObPLFunction &routine,
                                      share::schema::ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  bool is_special_ir = false;
  const uint64_t tenant_id = routine.get_tenant_id();
  bool need_set_db = true;

  // in mysql mode, only system packages with invoker's right do not need set db
  // in oracle mode, set db by if the routine is invoker's right
  if (lib::is_oracle_mode()
      || get_tenant_id_by_object_id(routine.get_package_id()) == OB_SYS_TENANT_ID) {
    need_set_db = !routine.is_invoker_right();
  }

  OZ (routine.is_special_pkg_invoke_right(guard, is_special_ir));
  if (need_set_db
      && !is_special_ir
      && routine.get_proc_type() != NESTED_FUNCTION
      && routine.get_proc_type() != NESTED_PROCEDURE
      && routine.get_proc_type() != STANDALONE_ANONYMOUS) {
    const share::schema::ObDatabaseSchema *database_schema = NULL;
    CK (OB_NOT_NULL(session_info_));
    OZ (guard.get_database_schema(tenant_id, routine.get_database_id(), database_schema));
    if (OB_SUCC(ret) && OB_ISNULL(database_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("fail to get database schema",
               K(ret), K(routine.get_database_id()), K(database_schema));
    }
    if (OB_SUCC(ret)
        && database_schema->get_database_name_str() != session_info_->get_database_name()) {
      OZ (database_name_.append(session_info_->get_database_name()));
      OX (database_id_ = session_info_->get_database_id());
      OZ (session_info_->set_default_database(database_schema->get_database_name_str()));
      OX (session_info_->set_database_id(routine.get_database_id()));
      OX (need_reset_default_database_ = true);
    }
  }
  return ret;
}

void ObPLContext::reset_default_database(int &ret)
{
  if (need_reset_default_database_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(session_info_)) {
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      LOG_ERROR("current session info is null", K(ret), K(session_info_));
    } else if (OB_SUCCESS !=
        (tmp_ret = session_info_->set_default_database(database_name_.string()))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret; // 不覆盖错误码
      LOG_ERROR("failed to reset default database", K(ret), K(tmp_ret), K(database_name_));
    } else {
      session_info_->set_database_id(database_id_);
    }
  }
}

int ObPLContext::valid_execute_context(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  CK (OB_NOT_NULL(ctx.get_package_guard()));
  return ret;
}

int ObPLContext::get_exec_state_from_local(
  ObSQLSessionInfo &session_info,
  int64_t package_id, int64_t routine_id,
  ObPLExecState *&plstate)
{
  int ret = OB_SUCCESS;
  ObPLContext  *top = session_info.get_pl_context();
  if (OB_NOT_NULL(top)) {
    ObIArray<ObPLExecState*> &exec_stack = top->get_exec_stack();
    plstate = NULL;
    for (int64_t i = exec_stack.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObPLExecState *state = exec_stack.at(i);
      CK (OB_NOT_NULL(state));
      if (OB_SUCC(ret)
          && state->get_function().get_package_id() == package_id
          && state->get_function().get_routine_id() == routine_id) {
        plstate = state;
        break;
      }
    }
  }
  return ret;
}

int ObPLContext::get_param_store_from_local(
  ObSQLSessionInfo &session_info,
  int64_t package_id, int64_t routine_id,
  ParamStore *&params)
{
  int ret = OB_SUCCESS;
  params = NULL;
  ObPLExecState  *state = NULL;
  OZ (get_exec_state_from_local(session_info, package_id, routine_id, state));
  CK (OB_NOT_NULL(state));
  OX (params = state->get_exec_ctx().params_);
  return ret;
}

int ObPLContext::get_routine_from_local(
  ObSQLSessionInfo &session_info,
  int64_t package_id, int64_t routine_id,
  ObPLFunction *&routine)
{
  int ret = OB_SUCCESS;
  ObPLExecState *state = NULL;
  OZ (get_exec_state_from_local(session_info, package_id, routine_id, state));
  if (OB_SUCC(ret) && OB_NOT_NULL(state)) {
    routine = &(state->get_function());
  }
  return ret;
}

int ObPLContext::get_subprogram_var_from_local(
  ObSQLSessionInfo &session_info,
  int64_t package_id, int64_t routine_id,
  int64_t var_idx, ObObjParam &result)
{
  int ret = OB_SUCCESS;
  ObPLExecState *state = NULL;
  OZ (get_exec_state_from_local(session_info, package_id, routine_id, state));
  OV (OB_NOT_NULL(state), OB_ERR_UNEXPECTED, package_id, routine_id, var_idx);
  OZ (state->get_var(var_idx, result), package_id, routine_id, var_idx);
  return ret;
}

int ObPLContext::set_subprogram_var_from_local(
  ObSQLSessionInfo &session_info,
  int64_t package_id, int64_t routine_id,
  int64_t var_idx, const ObObjParam &value)
{
  int ret = OB_SUCCESS;
  ObPLExecState *state = NULL;
  OZ (get_exec_state_from_local(session_info, package_id, routine_id, state));
  CK (OB_NOT_NULL(state));
  OZ (state->set_var(var_idx, value));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLContext::get_exact_error_msg(ObIArray<DbmsUtilityHelper::BtInfo*> &error_trace,
                                       ObIArray<DbmsUtilityHelper::BtInfo*> &call_stack,
                                       common::ObSqlString &err_msg)
{
  int ret = OB_SUCCESS;
  err_msg.reuse();
  if (0 == call_stack.count() || 0 == error_trace.count()) {
    LOG_INFO("error message is incomplete. ", K(call_stack.count()), K(error_trace.count()));
  } else {
    int64_t i = call_stack.count() - 1;
    for (; OB_SUCC(ret) && i>=0 && OB_NOT_NULL(call_stack.at(i)); i--) {
      int64_t line = call_stack.at(i)->get_line_number();
      int64_t col  = call_stack.at(i)->get_column_number();
      for (int64_t j = 0 ; j < error_trace.count(); j++) {
        if (OB_NOT_NULL(error_trace.at(j))
             && call_stack.at(i)->handler == error_trace.at(j)->handler) {
          line = error_trace.at(j)->get_line_number();
          col  = error_trace.at(j)->get_column_number();
          break;
        }
      }
      if (OB_FAIL(err_msg.append_fmt("\nat "))) {
        LOG_WARN("fail to get call stack name.", K(ret));
      } else if (OB_FAIL(err_msg.append_fmt("%.*s , line : %ld, col : %ld",
                                      call_stack.at(i)->object_name.length(),
                                      call_stack.at(i)->object_name.ptr(), line, col))) {
        LOG_WARN("fail to get call stack name.", K(ret),
                    K(call_stack.at(i)->object_name), K(line), K(col));
      } else {
        LOG_DEBUG("exact error msg: ", K(call_stack.at(i)->object_name),
                  K(line), K(col));
      }
    }
  }
  return ret;
}
#endif

// for common execute routine.
int ObPL::execute(ObExecContext &ctx,
                  ObIAllocator &allocator,
                  ObPLPackageGuard &package_guard,
                  ObPLFunction &routine,
                  ParamStore *params,
                  const ObIArray<int64_t> *nocopy_params,
                  ObObj *result,
                  int *status,
                  bool is_top_stack,
                  bool is_inner_call,
                  bool is_in_function,
                  bool is_anonymous,
                  uint64_t loc,
                  bool is_called_from_sql)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  int64_t execute_start = ObTimeUtility::current_time();
  ObObj local_result(ObMaxType);
  int local_status = OB_SUCCESS;

  ObPLExecState pl(allocator,
                   ctx,
                   package_guard,
                   routine,
                   local_result, // 这里不直接传result而是用local_result，是因为调用内部要检查OB_ER_SP_NORETURNEND错误
                   NULL != status ? *status : local_status,
                   is_top_stack,
                   is_inner_call,
                   is_in_function,
                   nocopy_params,
                   loc,
                   is_called_from_sql);
  OZ (pl.init(params, is_anonymous));
  OZ (pl.execute());
  OZ (pl.deep_copy_result_if_need());
  pl.final(ret);

  // process out arguments
  for (int64_t i = 0; OB_SUCC(ret) && i < routine.get_arg_count(); ++i) {
    if (routine.get_out_args().has_member(i)) {
      OX (params->at(i) = pl.get_params().at(i));
    }
  }
  // process anonymous out arguments
  if (OB_SUCC(ret) && is_anonymous && is_inner_call && OB_NOT_NULL(params)) {
    CK (params->count() <= pl.get_params().count());
    for (int i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
      OX (params->at(i) = pl.get_params().at(i));
    }
  }
  if (OB_SUCC(ret) && routine.get_ret_type().is_ref_cursor_type()) {
    ObPLCursorInfo *ref_cursor = reinterpret_cast<ObPLCursorInfo *>(local_result.get_ext());
    if (OB_NOT_NULL(ref_cursor)) {
      CK (1 <= ref_cursor->get_ref_count());
      if (OB_SUCC(ret)) {
        ref_cursor->set_is_returning(true);
        ref_cursor->dec_ref_count();
        LOG_DEBUG("ref cursor dec ref count in function return",K(*ref_cursor),
                                                                K(ref_cursor->get_ref_count()));
      }
    } else {
      // do nothing, 有可能return一个null出来
    }
  }
  // process function return value
  if (OB_SUCC(ret) && local_result.is_valid_type()) {
    CK (OB_NOT_NULL(result));
    OX (*result = local_result);
  }

  //当前层 pl 执行时间
  int64_t execute_end = ObTimeUtility::current_time();
  pl.add_pl_exec_time(execute_end - execute_start - pl.get_pure_sql_exec_time(), is_called_from_sql);

#ifndef NDEBUG
    LOG_INFO(">>>>>>>>>Execute Time: ", K(ret),
      K(routine.get_package_id()), K(routine.get_routine_id()), K(routine.get_package_name()), K(routine.get_function_name()), K(execute_end - execute_start),
        K(is_top_stack), K(execute_end - execute_start - pl.get_pure_sql_exec_time()), K(pl.get_pure_sql_exec_time()),
        K(pl.get_plsql_exec_time()), K(pl.get_sub_plsql_exec_time()));
#else
    LOG_DEBUG(">>>>>>>>Execute Time: ", K(ret),
      K(routine.get_package_id()), K(routine.get_routine_id()), K(routine.get_package_name()), K(routine.get_function_name()), K(execute_end - execute_start),
      K(is_top_stack), K(execute_end - execute_start - pl.get_pure_sql_exec_time()), K(pl.get_pure_sql_exec_time()),  K(pl.get_plsql_exec_time()),
      K(pl.get_sub_plsql_exec_time()));
#endif
  OX (routine.update_execute_time(execute_end - execute_start));

  return ret;
}

int ObPL::trans_sql(PlTransformTreeCtx &trans_ctx, ParseNode *root, ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  int64_t param_unit_idx = 0;
  int64_t param_num = trans_ctx.raw_param_num_;
  ObPlanCacheCtx pc_ctx(trans_ctx.raw_sql_or_expr_,
                      PC_PL_MODE, // PL_MODE
                      ctx.get_allocator(),
                      *(ctx.get_sql_ctx()),
                      ctx,
                      ctx.get_my_session()->get_effective_tenant_id());
  pc_ctx.fp_result_.pc_key_.namespace_ = ObLibCacheNameSpace::NS_ANON;
  pc_ctx.fp_result_.pc_key_.name_ = trans_ctx.no_param_sql_;
  ObSEArray<ObPCParam *, OB_PC_SPECIAL_PARAM_COUNT> special_params;
  ParamStore params((ObWrapperAllocator(trans_ctx.allocator_)));
  ObMaxConcurrentParam::FixParamStore fix_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                  ObWrapperAllocator(trans_ctx.allocator_));
  if (param_num > 0) {
    SqlInfo sql_info;
    sql_info.need_check_fp_ = false;
    ObPCParam *pc_param = NULL;
    char *ptr = (char *)trans_ctx.allocator_->alloc(param_num * sizeof(ObPCParam));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for pc param", K(ret), K(ptr));
    }
    for (int64_t i = 0;
        OB_SUCC(ret) && i < param_num && NULL != trans_ctx.p_list_;//p_list包含所有表达式和sql的raw param
        ++i) {
      pc_param = new(ptr)ObPCParam();
      ptr += sizeof(ObPCParam);
      pc_param->node_ = trans_ctx.p_list_->node_;
      if (OB_FAIL(pc_ctx.fp_result_.raw_params_.push_back(pc_param))) {
        LOG_WARN("fail to push into params", K(ret));
      } else {
        trans_ctx.p_list_ = trans_ctx.p_list_->next_;
      }
    } // for end
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(*trans_ctx.allocator_,
                                                                *ctx.get_my_session(),
                                                                &pc_ctx.fp_result_.raw_params_,
                                                                root,
                                                                sql_info,
                                                                params,
                                                                NULL,
                                                                fix_param_store,
                                                                false,
                                                                INVALID_MODE,
                                                                NULL,
                                                                true))) {
      LOG_WARN("fail to exec transform_syntax_tree", K(ret));
    } else if (OB_FAIL(ObSqlParameterization::check_and_generate_param_info(pc_ctx.fp_result_.raw_params_,
                                              sql_info,
                                              special_params))) {
      LOG_WARN("fail to check and generate param info", K(ret));
    }
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      int32_t pos = 0;
      buf = (char *)trans_ctx.allocator_->alloc(pc_ctx.raw_sql_.length());
      if (NULL == buf) {
        LOG_WARN("fail to alloc buf", K(pc_ctx.raw_sql_.length()));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(ObSqlParameterization::construct_sql_for_pl(pc_ctx.fp_result_.pc_key_.name_, special_params, buf, pc_ctx.raw_sql_.length(), pos))) {
        LOG_WARN("fail to construct_sql", K(ret));
      } else {
        if (trans_ctx.buf_size_ < trans_ctx.buf_len_ + pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error about trans_ctx.buf", K(ret));
        } else {
          MEMCPY(trans_ctx.buf_ + trans_ctx.buf_len_, buf, pos);
          trans_ctx.buf_len_ += pos;
        }
        CK (OB_NOT_NULL(trans_ctx.params_));
        for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
          ObObjParam obj = params.at(i);
          OZ (deep_copy_obj(ctx.get_allocator(), params.at(i), obj));
          OZ (trans_ctx.params_->push_back(obj));
        }
      }
    }
  } else {
    if (trans_ctx.buf_size_ < trans_ctx.buf_len_ + trans_ctx.no_param_sql_.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error about trans_ctx.buf", K(ret));
    } else {
      MEMCPY(trans_ctx.buf_ + trans_ctx.buf_len_, trans_ctx.no_param_sql_.ptr(), trans_ctx.no_param_sql_.length());
      trans_ctx.buf_len_ += trans_ctx.no_param_sql_.length();
    }
  }
  return ret;
}

int ObPL::transform_tree(PlTransformTreeCtx &trans_ctx, ParseNode *root, ParseNode *no_param_root, ObExecContext &ctx, ParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(root) && OB_NOT_NULL(no_param_root)) {
    if (T_EXPR == no_param_root->type_) {
      ParseNode *expr_node = NULL;
      /* 调整语法树结构, 避免语法树分析阶段因为结构不同报错 */
      if (OB_ISNULL(expr_node = new_non_terminal_node(trans_ctx.allocator_, T_EXPR, 1, root))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        /* 语法树分析时会修改node上部分属性, 这里提前记录 */
        int64_t raw_pos = expr_node->children_[0]->pos_ - trans_ctx.raw_anonymous_off_;
        int64_t raw_str_off = expr_node->children_[0]->pl_str_off_;
        trans_ctx.raw_sql_or_expr_.assign_ptr(expr_node->children_[0]->raw_text_, expr_node->children_[0]->text_len_);
        trans_ctx.raw_param_num_ = no_param_root->children_[0]->param_num_;
        trans_ctx.no_param_sql_.assign_ptr(parse_result.no_param_sql_ + no_param_root->children_[0]->pos_, no_param_root->children_[0]->str_len_);
        if (trans_ctx.buf_size_ < trans_ctx.buf_len_ + raw_pos - trans_ctx.copied_idx_ ||
            raw_pos < trans_ctx.copied_idx_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error about trans_ctx.buf", K(ret), K(raw_pos), K(trans_ctx.copied_idx_));
        } else {
          MEMCPY(trans_ctx.buf_ + trans_ctx.buf_len_, trans_ctx.raw_sql_.ptr() + trans_ctx.copied_idx_, raw_pos - trans_ctx.copied_idx_);
          trans_ctx.buf_len_ += raw_pos - trans_ctx.copied_idx_;
          OZ (trans_sql(trans_ctx, expr_node, ctx));
          trans_ctx.copied_idx_ = raw_pos + raw_str_off;
        }
      }
    } else if (T_SQL_STMT == no_param_root->type_) {
      int64_t raw_pos = root->children_[0]->pos_ - trans_ctx.raw_anonymous_off_;
      int64_t raw_str_off = root->children_[0]->pl_str_off_;
      trans_ctx.raw_sql_or_expr_.assign_ptr(root->children_[0]->raw_text_, root->children_[0]->text_len_);
      trans_ctx.raw_param_num_ = no_param_root->children_[0]->param_num_;
      trans_ctx.no_param_sql_.assign_ptr(parse_result.no_param_sql_ + no_param_root->children_[0]->pos_, no_param_root->children_[0]->str_len_);
      if (trans_ctx.buf_size_ < trans_ctx.buf_len_ + raw_pos - trans_ctx.copied_idx_ ||
          raw_pos < trans_ctx.copied_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error about trans_ctx.buf", K(ret));
      } else {
        MEMCPY(trans_ctx.buf_ + trans_ctx.buf_len_, trans_ctx.raw_sql_.ptr() + trans_ctx.copied_idx_, raw_pos - trans_ctx.copied_idx_);
        trans_ctx.buf_len_ += raw_pos - trans_ctx.copied_idx_;
        OZ (trans_sql(trans_ctx, root, ctx));
        trans_ctx.copied_idx_ = raw_pos + raw_str_off;
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < root->num_child_ && i < no_param_root->num_child_; ++i) {
        OZ (SMART_CALL(transform_tree(trans_ctx, root->children_[i], no_param_root->children_[i], ctx, parse_result)));
      }
    }
  }
  return ret;
}

int ObPL::parameter_anonymous_block(ObExecContext &ctx,
                              const ObStmtNodeTree *block,
                              ParamStore &params,
                              ObIAllocator &allocator,
                              ObCacheObjGuard &cacheobj_guard)
{
  int ret = OB_SUCCESS;
  ObString pc_key;
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(block));
  if (OB_SUCC(ret)) {
    ObArenaAllocator paramerter_alloc("AnonyParam", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString sql(static_cast<int64_t>(block->str_len_), block->str_value_);
    ParseResult parse_result;
    ObPLParser pl_parser(paramerter_alloc,
                      ctx.get_my_session()->get_charsets4parser(),
                      ctx.get_my_session()->get_sql_mode());
    OZ (pl_parser.fast_parse(sql, parse_result));
    if (OB_SUCC(ret)) {
      PlTransformTreeCtx trans_ctx;
      ParseNode *block_node = NULL;
      memset(&trans_ctx, 0, sizeof(PlTransformTreeCtx));
      trans_ctx.allocator_ = &paramerter_alloc;
      trans_ctx.raw_sql_ = sql;
      trans_ctx.raw_anonymous_off_ = block->pl_str_off_;
      trans_ctx.params_ = &params;
      trans_ctx.buf_ = (char *)trans_ctx.allocator_->alloc(sql.length());
      if (OB_ISNULL(trans_ctx.buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(sql), K(ret));
      } else {
        trans_ctx.buf_size_ = sql.length();
        trans_ctx.p_list_ = parse_result.param_nodes_;
        CK (OB_NOT_NULL(parse_result.result_tree_));
        CK (T_STMT_LIST == parse_result.result_tree_->type_ && 1 == parse_result.result_tree_->num_child_);
        CK (OB_NOT_NULL(block_node = parse_result.result_tree_->children_[0]));
        CK (T_SP_ANONYMOUS_BLOCK == block_node->type_);
        CK (OB_NOT_NULL(block_node = block_node->children_[0]));
        CK (T_SP_BLOCK_CONTENT == block_node->type_ || T_SP_LABELED_BLOCK == block_node->type_);
        OZ (transform_tree(trans_ctx, const_cast<ParseNode *>(block), block_node, ctx, parse_result));
        if (OB_SUCC(ret)) {
          if (trans_ctx.buf_size_ < trans_ctx.buf_len_ + trans_ctx.raw_sql_.length() - trans_ctx.copied_idx_ ||
              trans_ctx.raw_sql_.length() < trans_ctx.copied_idx_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error about trans_ctx.buf", K(ret));
          } else {
            MEMCPY(trans_ctx.buf_ + trans_ctx.buf_len_,
                    trans_ctx.raw_sql_.ptr() + trans_ctx.copied_idx_,
                    trans_ctx.raw_sql_.length() - trans_ctx.copied_idx_);
            trans_ctx.buf_len_ += trans_ctx.raw_sql_.length() - trans_ctx.copied_idx_;
          }
        }
        //pc_key.assign_ptr(trans_ctx.buf_, trans_ctx.buf_len_);
        OZ (ob_write_string(ctx.get_allocator(), ObString(trans_ctx.buf_len_, trans_ctx.buf_), pc_key));
      }
    }
  }
  // generate sql_id using paramiterized sql, and overwrite privious sql_id
  OZ (ObSQLUtils::md5(pc_key, ctx.get_sql_ctx()->sql_id_,
                      (int32_t)sizeof(ctx.get_sql_ctx()->sql_id_)));
  OX (ctx.get_my_session()->set_cur_sql_id(ctx.get_sql_ctx()->sql_id_));
  OZ (get_pl_function(ctx, params, OB_INVALID_ID, pc_key, cacheobj_guard));
  return ret;
}

struct ObPLExecTraceIdGuard {
  ObPLExecTraceIdGuard(const ObCurTraceId::TraceId &trace_id,
                       uint64_t package_id, uint64_t routine_id)
      : package_id_(package_id), routine_id_(routine_id) {
    int ret = OB_SUCCESS;
    if (trace_id.is_valid()
          && OB_NOT_NULL(ObCurTraceId::get_trace_id())
          && !(trace_id == *ObCurTraceId::get_trace_id())) {
      origin_trace_id_.set(*ObCurTraceId::get_trace_id());

      // log with SQL trace_id
      LOG_TRACE("executing pl, restore trace_id to pl trace_id",
               K(package_id_), K(routine_id_),
               "from", origin_trace_id_, "to", trace_id);

      ObCurTraceId::get_trace_id()->set(trace_id);
    }
  }

  ~ObPLExecTraceIdGuard() {
    int ret = OB_SUCCESS;
    if (origin_trace_id_.is_valid() && OB_NOT_NULL(ObCurTraceId::get_trace_id())) {
      ObCurTraceId::TraceId curr_trace_id = *ObCurTraceId::get_trace_id();
      ObCurTraceId::get_trace_id()->set(origin_trace_id_);
      LOG_TRACE("pl execution finished, trace id restored from pl trace_id to sql trace_id",
               K(package_id_), K(routine_id_),
               "from", curr_trace_id, "to", origin_trace_id_);
    }
  }
  ObCurTraceId::TraceId origin_trace_id_;
  uint64_t package_id_;
  uint64_t routine_id_;
};

// for execute anonymous
int ObPL::execute(ObExecContext &ctx, ParamStore &params, const ObStmtNodeTree *block)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  FLTSpanGuard(pl_entry);
  lib::MemoryContext mem_context = NULL;
  lib::ContextParam param;
  ObPLFunction *routine = NULL;
  ObCacheObjGuard cacheobj_guard(PL_ANON_HANDLE);
  bool is_forbid_anony_parameter =
         block->is_forbid_anony_parameter_
          || (params.count() > 0)
          || lib::is_mysql_mode();

  int64_t old_worker_timeout_ts = 0;
  /* !!!
   * PL，req_timeinfo_guard一定要在执行前定义
   * !!!
   */
  observer::ObReqTimeGuard req_timeinfo_guard;
  CHECK_COMPATIBILITY_MODE(ctx.get_my_session());

  OZ (ObPLContext::valid_execute_context(ctx));

  OX (is_forbid_anony_parameter = is_forbid_anony_parameter
                                    || ctx.get_my_session()->is_pl_debug_on()
                                    || ctx.get_my_session()->get_pl_profiler() != nullptr);

  OX (param.set_mem_attr(ctx.get_my_session()->get_effective_tenant_id(),
                         ObModIds::OB_PL_TEMP,
                         ObCtxIds::DEFAULT_CTX_ID));
  OZ (CURRENT_CONTEXT->CREATE_CONTEXT(mem_context, param));
  CK (OB_NOT_NULL(mem_context));

  if (OB_SUCC(ret)) {
    ParamStore exec_params((ObWrapperAllocator(ctx.get_allocator())));
    if (OB_ISNULL(ctx.get_my_session()->get_pl_context())) {
      // set work timeout for compile it only top level store routine
      int64_t pl_block_timeout = 0;
      int64_t query_start_time = ctx.get_my_session()->get_query_start_time();
      old_worker_timeout_ts = THIS_WORKER.get_timeout_ts();
      OZ (ctx.get_my_session()->get_pl_block_timeout(pl_block_timeout));
      if (OB_SUCC(ret) && pl_block_timeout > OB_MAX_USER_SPECIFIED_TIMEOUT) {
        pl_block_timeout = OB_MAX_USER_SPECIFIED_TIMEOUT;
      }
      OX (THIS_WORKER.set_timeout_ts(query_start_time + pl_block_timeout));
    }

    if (OB_FAIL(ret)) {
    } else if (!is_forbid_anony_parameter) {
      OZ (parameter_anonymous_block(ctx, block, exec_params, ctx.get_allocator(), cacheobj_guard));
      OX (routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()));
      CK (OB_NOT_NULL(routine));
      OX (routine->set_debug_priv());
    } else {
      // compile it.
      if (OB_SUCC(ret) && NULL == routine) {
        ObPLCompiler compiler(mem_context->get_arena_allocator(),
                              *(ctx.get_my_session()),
                              *(ctx.get_sql_ctx()->schema_guard_),
                              *(ctx.get_package_guard()),
                              *(ctx.get_sql_proxy()));
        if (OB_ISNULL(routine = static_cast<ObPLFunction*>(
              mem_context->get_arena_allocator().alloc(sizeof(ObPLFunction))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for anonymous pl function",
                  K(ret), K(sizeof(ObPLFunction)));
        }
        OX (routine = new(routine)ObPLFunction(mem_context));

        // stmt_id is OB_INVALID_ID for anonymous block from text protocol
        OZ (compiler.compile(block, OB_INVALID_ID, *routine, &params, false));
        OX (routine->set_debug_priv());
        if (OB_SUCC(ret) && params.count() != routine->get_params_info().count()) {
          ret = OB_ERR_BIND_VARIABLE_NOT_EXIST;
          LOG_WARN("text anonymous can not contain bind variable", K(ret));
        }
      }
    }
    // restore work timeout
    if (old_worker_timeout_ts != 0) {
      THIS_WORKER.set_timeout_ts(old_worker_timeout_ts);
    }

    // prepare it.
    if (OB_SUCC(ret)) {
      SMART_VAR(ObPLContext, stack_ctx) {
        ObCurTraceId::TraceId parent_trace_id;
        if (nullptr != ctx.get_my_session()->get_pl_context()) {
          ObPLContext *curr = ctx.get_my_session()->get_pl_context()->get_top_stack_ctx();
          parent_trace_id.set(curr->get_trace_id());
        }
        ObPLExecTraceIdGuard trace_guard(parent_trace_id, OB_INVALID_ID, OB_INVALID_ID);

        LinkPLStackGuard link_stack_guard(ctx, stack_ctx);
        OZ (stack_ctx.init(*(ctx.get_my_session()), ctx, routine, false));

        try {
          // execute it.
          OZ (execute(ctx,
                      ctx.get_allocator(),
                      *(ctx.get_package_guard()),
                      *routine,
                      is_forbid_anony_parameter ? &params : &exec_params, // params
                      NULL, // nocopy params
                      NULL, // result
                      NULL, // status
                      stack_ctx.is_top_stack(),
                      false, // inner call
                      false,
                      true));

          // unprepare it.
          if (stack_ctx.is_inited()) {
            stack_ctx.destory(*(ctx.get_my_session()), ctx, ret);
          }
        } catch (...) {
          // unprepare it.
          if (stack_ctx.is_inited()) {
            stack_ctx.destory(*(ctx.get_my_session()), ctx, ret);
          }
          if (is_forbid_anony_parameter && NULL != routine) {
            routine->~ObPLFunction();
          }
          if (NULL != mem_context) {
            DESTROY_CONTEXT(mem_context);
            mem_context = NULL;
          }
          throw;
        }
      }
    }
  }


#ifndef NDEBUG
  if(OB_SUCC(ret)) {
    ctx.get_my_session()->print_all_cursor();
  }
#endif

  if (is_forbid_anony_parameter && NULL != routine) {
    routine->~ObPLFunction();
  }
  if (NULL != mem_context) {
    DESTROY_CONTEXT(mem_context);
    mem_context = NULL;
  }
  return ret;
}

//execute anonymous interface for ps
int ObPL::execute(ObExecContext &ctx,
                  ParamStore &params,
                  uint64_t stmt_id,
                  const ObString &sql,
                  ObBitSet<OB_DEFAULT_BITSET_SIZE> &out_args)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  FLTSpanGuard(pl_entry);
  ObPLFunction *routine = NULL;
  ObCacheObjGuard cacheobj_guard(PL_ANON_HANDLE);
  int64_t old_worker_timeout_ts = 0;

  /* !!!
   * PL，req_timeinfo_guard一定要在执行前定义
   * !!!
   */
  observer::ObReqTimeGuard req_timeinfo_guard;
  CHECK_COMPATIBILITY_MODE(ctx.get_my_session());
  if (OB_ISNULL(ctx.get_my_session()->get_pl_context())) {
    // set work timeout for compile it only top level store routine
    int64_t pl_block_timeout = 0;
    int64_t query_start_time = ctx.get_my_session()->get_query_start_time();
    old_worker_timeout_ts = THIS_WORKER.get_timeout_ts();
    OZ (ctx.get_my_session()->get_pl_block_timeout(pl_block_timeout));
    if (OB_SUCC(ret) && pl_block_timeout > OB_MAX_USER_SPECIFIED_TIMEOUT) {
      pl_block_timeout = OB_MAX_USER_SPECIFIED_TIMEOUT;
    }
    OX (THIS_WORKER.set_timeout_ts(query_start_time + pl_block_timeout));
  }

  // get from cache or compile it ...
  OZ (get_pl_function(ctx, params, stmt_id, sql, cacheobj_guard));
  OX (routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()));
  CK (OB_NOT_NULL(routine));
  OX (routine->set_debug_priv());
  OX (out_args = routine->get_out_args());
  CK (OB_NOT_NULL(ctx.get_package_guard()));

  // restore work timeout
  if (old_worker_timeout_ts != 0) {
    THIS_WORKER.set_timeout_ts(old_worker_timeout_ts);
  }

  // prepare it...
  if (OB_SUCC(ret)) {
    SMART_VAR(ObPLContext, stack_ctx) {
      ObCurTraceId::TraceId parent_trace_id;
      if (nullptr != ctx.get_my_session()->get_pl_context()) {
        ObPLContext *curr = ctx.get_my_session()->get_pl_context()->get_top_stack_ctx();
        parent_trace_id.set(curr->get_trace_id());
      }
      ObPLExecTraceIdGuard trace_guard(parent_trace_id, OB_INVALID_ID, stmt_id);

      LinkPLStackGuard link_stack_guard(ctx, stack_ctx);
      OZ (stack_ctx.init(*(ctx.get_my_session()), ctx, routine, false));

      try {
        // execute it...
        OZ (execute(ctx,
                    ctx.get_allocator(),
                    *(ctx.get_package_guard()),
                    *routine,
                    &params,
                    NULL, // nocopy params
                    NULL, // result
                    NULL, // status
                    stack_ctx.is_top_stack(),
                    false, // inner call
                    false, // in function
                    true)); // anonymous
        // unprepare it ...
        if (stack_ctx.is_inited()) {
          stack_ctx.destory(*(ctx.get_my_session()), ctx, ret);
        }
      } catch (...) {
        // unprepare it ...
        if (stack_ctx.is_inited()) {
          stack_ctx.destory(*(ctx.get_my_session()), ctx, ret);
        }
        throw;
      }
    }
  }

  return ret;
}

// for normal routine
int ObPL::execute(ObExecContext &ctx,
                  ObIAllocator &allocator,
                  uint64_t package_id,
                  uint64_t routine_id,
                  const ObIArray<int64_t> &subprogram_path,
                  ParamStore &params,
                  const ObIArray<int64_t> &nocopy_params,
                  ObObj &result,
                  int *status,
                  bool inner_call,
                  bool in_function,
                  uint64_t loc,
                  bool is_called_from_sql,
                  uint64_t dblink_id)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  FLTSpanGuard(pl_entry);
  bool debug_mode = false;
  ObPLFunction *routine = NULL;
  ObPLFunction *local_routine = NULL;
  ObCacheObjGuard cacheobj_guard(PL_ROUTINE_HANDLE);
  int64_t old_worker_timeout_ts = 0;
  ObCurTraceId::TraceId parent_trace_id;
  /* !!!
  * PL，req_timeinfo_guard一定要在执行前定义
  * !!!
  */
  SMART_VAR(ObPLContext, stack_ctx) {
    if (OB_ISNULL(ctx.get_my_session()->get_pl_context())) {
      // set work timeout for compile it only top level store routine
      int64_t pl_block_timeout = 0;
      int64_t query_start_time = ctx.get_my_session()->get_query_start_time();
      old_worker_timeout_ts = THIS_WORKER.get_timeout_ts();
      OZ (ctx.get_my_session()->get_pl_block_timeout(pl_block_timeout));
      if (OB_SUCC(ret) && pl_block_timeout > OB_MAX_USER_SPECIFIED_TIMEOUT) {
        pl_block_timeout = OB_MAX_USER_SPECIFIED_TIMEOUT;
      }
      OX (THIS_WORKER.set_timeout_ts(query_start_time + pl_block_timeout));
    } else {
      ObPLContext *curr = ctx.get_my_session()->get_pl_context()->get_top_stack_ctx();
      parent_trace_id.set(curr->get_trace_id());
    }
    ObPLExecTraceIdGuard trace_guard(parent_trace_id, package_id, routine_id);

    observer::ObReqTimeGuard req_timeinfo_guard;

    LinkPLStackGuard link_stack_guard(ctx, stack_ctx);
    CHECK_COMPATIBILITY_MODE(ctx.get_my_session());

    CK (!inner_call || (inner_call && OB_NOT_NULL(status)));

    OZ (ObPLContext::valid_execute_context(ctx));

    // NOTE: need save current stmt type avoid PL-Compile corrupt session.stmt_type
    auto saved_stmt_type = ctx.get_my_session()->get_stmt_type();
    if (OB_FAIL(ret)) {
    } else if (!is_valid_id(dblink_id)) {
      OZ (get_pl_function(ctx,
                          *ctx.get_package_guard(),
                          package_id,
                          routine_id,
                          subprogram_path,
                          cacheobj_guard,
                          local_routine),
            K(routine_id), K(subprogram_path));
      // restore work timeout
      if (old_worker_timeout_ts != 0) {
        THIS_WORKER.set_timeout_ts(old_worker_timeout_ts);
      }
      // if the routine comes from local, guard needn't to manage it.
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(local_routine)) {
        routine = local_routine;
      } else {
        routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj());
      }
      CK (OB_NOT_NULL(routine));
      CK (OB_NOT_NULL(ctx.get_my_session()));
      OZ (ObPLContext::check_routine_legal(*routine, in_function,
                                          ctx.get_my_session()->is_for_trigger_package()));
      OZ (check_trigger_arg(params, *routine));
      if (OB_SUCC(ret) && ctx.get_my_session()->is_pl_debug_on()) {
        int tmp_ret = OB_SUCCESS;
        bool need_check = true;
        ObPLContext *pl_ctx = ctx.get_my_session()->get_pl_context();
        if (OB_NOT_NULL(pl_ctx)) {
          ObIArray<pl::ObPLExecState *> &stack = pl_ctx->get_exec_stack();
          if (stack.count() > 0) {
            pl::ObPLExecState *frame = stack.at(stack.count() - 1);
            // look into caller, if caller hasn't debug priv, the callee also has not
            if (OB_NOT_NULL(frame) && !(frame->get_function().has_debug_priv())) {
              need_check = false;
            }
          }
        } else {
        }
        bool is_nested_routine = OB_NOT_NULL(local_routine) && (!subprogram_path.empty());
        // routine default has not debug priv, if a routine is not a nested routine, we check it to see
        // if it has debug priv, and set or clear debug flag.
        if (need_check) {
          if (!is_nested_routine) {
            tmp_ret = ObPLContext::check_debug_priv(ctx.get_sql_ctx()->schema_guard_,
                                      ctx.get_my_session(), routine);
          } else {
            // a nested routine debug priv same as caller, because a nested routine cann't be called
            // from outside of this routine, to be here, we can see that the caller has debug priv
            // or the need_check flag is not true;
            routine->set_debug_priv();
          }
        }
      }
      const ObDatabaseSchema *db_schema = NULL;
      ObString db_name = "";
      ObSchemaGetterGuard *guard = ctx.get_sql_ctx()->schema_guard_;
      CK (OB_NOT_NULL(guard));
      if (OB_SUCC(ret) && lib::is_mysql_mode()) {
        OZ (guard->get_database_schema(ctx.get_my_session()->get_effective_tenant_id(),
                                      routine->get_database_id(),
                                      db_schema));
        if (OB_SUCC(ret)) {
          db_name = db_schema->get_database_name_str();
        }
      }
      if (OB_SUCC(ret) && !ObUDTObjectType::is_object_id(package_id)) {
        OZ (check_exec_priv(ctx, db_name, routine));
      }
    }
    // prepare it ...
    OZ (stack_ctx.init(*(ctx.get_my_session()), ctx,
                      routine,
                      in_function
                      || (package_id != OB_INVALID_ID
                          && ObTriggerInfo::is_trigger_package_id(package_id)),
                      &allocator,
                      is_valid_id(dblink_id)));
    if (OB_SUCC(ret) && !is_valid_id(dblink_id)) {
      OZ (stack_ctx.inc_and_check_depth(package_id, routine_id, routine->is_function()));
      OZ (stack_ctx.set_exec_env(*routine));
      OZ (stack_ctx.set_default_database(*routine, *(ctx.get_sql_ctx()->schema_guard_)));
      OZ (stack_ctx.set_role_id_array(*routine, *(ctx.get_sql_ctx()->schema_guard_)));
    }

#define UNPREPARE() \
    if (stack_ctx.is_inited()) { \
      if (!is_valid_id(dblink_id)) { \
        stack_ctx.reset_exec_env(ret); \
        stack_ctx.reset_default_database(ret); \
        stack_ctx.reset_role_id_array(ret); \
        stack_ctx.dec_and_check_depth(package_id, routine_id, ret); \
      } \
      stack_ctx.destory(*ctx.get_my_session(), ctx, ret); \
    } \
    if (NULL != routine) routine->clean_debug_priv(); \
    if (OB_INVALID_ID == package_id \
        && subprogram_path.empty() \
        && routine != NULL) { \
      routine = NULL; \
    }

    //check mysql definer has execute priv
    if (OB_SUCC(ret) && lib::is_mysql_mode() && !ObUDTObjectType::is_object_id(package_id)) {
      const ObDatabaseSchema *db_schema = NULL;
      ObString db_name = "";
      ObSchemaGetterGuard *guard = ctx.get_sql_ctx()->schema_guard_;
      CK (OB_NOT_NULL(guard));
      OZ (guard->get_database_schema(ctx.get_my_session()->get_effective_tenant_id(),
                                      routine->get_database_id(),
                                      db_schema));
      CK (OB_NOT_NULL(db_schema));
      if (OB_SUCC(ret)) {
        db_name = db_schema->get_database_name_str();
      }
      OZ (check_exec_priv(ctx, db_schema->get_database_name_str(), routine));
    }

    // NOTE: restore stmt type saved before get_pl_function before start execution
    ctx.get_my_session()->set_stmt_type(saved_stmt_type);
    try {
      // execute it ...
      if (OB_FAIL(ret)) {
#ifdef OB_BUILD_ORACLE_PL
      } else if (is_valid_id(dblink_id)) {
        OZ (ObSPIService::spi_execute_dblink(ctx, allocator, dblink_id, package_id, routine_id, params, &result));
#endif
      } else {
        OZ (execute(ctx,
                    allocator,
                    *(ctx.get_package_guard()),
                    *routine,
                    &params,
                    ((0 == nocopy_params.count()) ? NULL : &nocopy_params),
                    &result,
                    status,
                    stack_ctx.is_top_stack(),
                    inner_call,
                    routine->is_function() || in_function,
                    false,
                    loc,
                    is_called_from_sql));
      }
    } catch (...) {
      LOG_WARN("failed to execute it", K(ret), K(package_id), K(routine_id), K(subprogram_path));
      UNPREPARE();
      throw;
    }
    UNPREPARE();

#undef UNPREPARE
  }

  return ret;
}

// get anonymous routine from plan cache or compile it.
int ObPL::get_pl_function(ObExecContext &ctx,
                          ParamStore &params,
                          uint64_t stmt_id,
                          const ObString &sql,
                          ObCacheObjGuard& cacheobj_guard)
{
  int ret = OB_SUCCESS;
  ObPLFunction* routine = NULL;
  OZ (ObPLContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    uint64_t database_id = OB_INVALID_ID;
    ctx.get_my_session()->get_database_id(database_id);

    ObPLCacheCtx pc_ctx;
    pc_ctx.session_info_ = ctx.get_my_session();
    pc_ctx.schema_guard_ = ctx.get_sql_ctx()->schema_guard_;
    pc_ctx.cache_params_ = &params;
    pc_ctx.raw_sql_ = sql;
    if (ctx.get_sql_ctx()->sql_id_[0] != '\0') {
      MEMCPY(pc_ctx.sql_id_, ctx.get_sql_ctx()->sql_id_, (int32_t)sizeof(ctx.get_sql_ctx()->sql_id_));
    } else {
      CK (!pc_ctx.raw_sql_.empty());
      OX ((void)ObSQLUtils::md5(pc_ctx.raw_sql_,
                            pc_ctx.sql_id_,
                            (int32_t)sizeof(pc_ctx.sql_id_)));
    }

    pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_ANON;
    pc_ctx.key_.db_id_ = database_id;
    pc_ctx.key_.sessid_ = ctx.get_my_session()->is_pl_debug_on() ? ctx.get_my_session()->get_sessid() : 0;
    pc_ctx.key_.mode_ = ctx.get_my_session()->get_pl_profiler() != nullptr
                          ? ObPLObjectKey::ObjectMode::PROFILE : ObPLObjectKey::ObjectMode::NORMAL;

    if (OB_SUCC(ret)) {
      if (stmt_id != OB_INVALID_ID) {
        // use stmt id as key
        pc_ctx.key_.key_id_ = stmt_id;
        if (OB_FAIL(ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
          LOG_INFO("get pl function from plan cache failed",
                    K(ret), K(pc_ctx.key_), K(stmt_id), K(sql), K(params));
          ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
        } else if (FALSE_IT(routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()))) {
          // do nothing
        } else if (OB_NOT_NULL(routine)) {
          LOG_DEBUG("get pl function from plan cache success", KPC(routine));
        }
      }
    }

    // use sql as key
    if (OB_SUCC(ret) && OB_ISNULL(routine)) {
      pc_ctx.key_.key_id_ = OB_INVALID_ID;
      pc_ctx.key_.name_ = sql;
      LOG_DEBUG("find plan by stmt_id failed, start to find plan by sql",
                 K(ret), K(sql), K(stmt_id), K(pc_ctx.key_));
      if (OB_FAIL(ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
        LOG_INFO("get pl function by sql failed, will ignore this error",
                 K(ret), K(pc_ctx.key_), K(stmt_id), K(sql), K(params));
        ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
      } else if (FALSE_IT(routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()))) {
        // do nothing
      } else if (OB_NOT_NULL(routine) && stmt_id != OB_INVALID_ID) {
        //pc_ctx.fp_result_.pc_key_.key_id_ = stmt_id;
        //pc_ctx.fp_result_.pc_key_.name_ = sql;
        //OZ (plan_cache->add_exists_cache_obj_by_stmt_id(pc_ctx, routine));
        //LOG_DEBUG("plan added succeed",
        //          K(ret), K(pc_ctx.fp_result_.pc_key_), K(stmt_id), K(sql), K(params));
      }
      // reset pc_ctx
      if (stmt_id != OB_INVALID_ID) {
        pc_ctx.key_.key_id_ = stmt_id;
        pc_ctx.key_.name_.reset();
      }
    }

    LOG_DEBUG("get anonymous from cache by sql", K(ret),
                                                 K(stmt_id),
                                                 KPC(routine),
                                                 K(pc_ctx.key_),
                                                 K(ctx.get_sql_ctx()->sql_id_),
                                                 K(sql));

    // not in cache, compile it and add to cache
    if (OB_SUCC(ret) && OB_ISNULL(routine)) {
      ParseNode root_node;
      uint64_t lock_idx = stmt_id != OB_INVALID_ID ? stmt_id : murmurhash(sql.ptr(), sql.length(), 0);
      ObBucketHashWLockGuard guard(codegen_lock_, lock_idx);
      // check session status after get lock
      if (OB_FAIL(check_session_alive(*ctx.get_my_session()))) {
        LOG_WARN("query or session is killed after get PL codegen lock", K(ret));

        // check cache again after get lock
      } else if (OB_FAIL(ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
        LOG_INFO("get pl function by sql failed, will ignore this error",
                 K(ret), K(pc_ctx.key_), K(stmt_id), K(sql), K(params));
        ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
      }
      OX (routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()));
      if (OB_SUCC(ret) && OB_ISNULL(routine)) {
        OZ (generate_pl_function(ctx, sql, params, root_node, cacheobj_guard, stmt_id));
        OX (routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()));
        CK (OB_NOT_NULL(routine));
        if (OB_SUCC(ret) && routine->get_can_cached()) {
          routine->get_stat_for_update().name_ = pc_ctx.raw_sql_;
          routine->get_stat_for_update().type_ = ObPLCacheObjectType::ANONYMOUS_BLOCK_TYPE;
          OZ (add_pl_lib_cache(routine, pc_ctx));
          // add sql key to plan cache
          //OX (pc_ctx.key_.name_ = sql);
          //OZ (plan_cache->add_exists_cache_obj_by_sql(pc_ctx, routine));
          //OX (pc_ctx.key_.name_.reset());
        }
        LOG_DEBUG("add anonymous to cache",
                  K(ret), K(stmt_id), KPC(routine),
                  K(pc_ctx.key_), K(ctx.get_sql_ctx()->sql_id_), K(sql));
      }
    }
  }
  return ret;
}

// @Param ObPLFunction: routine that comes from local
// @Param cacheobj_guard: routine that comes from plan cache or generated.
// get schema routine from plan cache or compile it.
int ObPL::get_pl_function(ObExecContext &ctx,
                          //to keep routine package referenced when execute
                          ObPLPackageGuard &package_guard,
                          int64_t package_id,
                          int64_t routine_id,
                          const ObIArray<int64_t> &subprogram_path,
                          ObCacheObjGuard& cacheobj_guard,
                          ObPLFunction *&local_routine)
{
  int ret = OB_SUCCESS;
  ObPLFunction* routine = NULL;
  OZ (ObPLContext::valid_execute_context(ctx));
  if (OB_SUCC(ret) && !subprogram_path.empty()) {
    OZ (ObPLContext::get_routine_from_local(
        *(ctx.get_my_session()), package_id, routine_id, local_routine));
    CK (OB_NOT_NULL(local_routine));
    OZ (local_routine->get_subprogram(subprogram_path, local_routine));
    CK (OB_NOT_NULL(local_routine));
  }
  if (OB_FAIL(ret) || OB_NOT_NULL(routine) || !subprogram_path.empty()) {
    // do nothing ...
  } else if (OB_INVALID_ID != package_id) { // package or object routine
    ObPLResolveCtx pl_ctx(ctx.get_allocator(),
                          *ctx.get_my_session(),
                          *ctx.get_sql_ctx()->schema_guard_,
                          package_guard,
                          *ctx.get_sql_proxy(),
                          false /*PS MODE*/);
#ifdef OB_BUILD_ORACLE_PL
   if (ObUDTObjectType::is_object_id_masked(package_id)) {
      OX (package_id = ObUDTObjectType::clear_object_id_mask(package_id));
      OZ (ObPLUDTObjectManager::get_udt_function(pl_ctx,
                                                 ctx,
                                                 package_id,
                                                 routine_id,
                                                 local_routine));
    } else {
#endif
      OZ (package_manager_.get_package_routine(pl_ctx,
                                               ctx,
                                               package_id,
                                               routine_id,
                                               local_routine));
#ifdef OB_BUILD_ORACLE_PL
    }
#endif
    CK (OB_NOT_NULL(local_routine));
  } else { // standalone routine
    static const ObString PLSQL = ObString("PL/SQL");

    uint64_t database_id = OB_INVALID_ID;
    ctx.get_my_session()->get_database_id(database_id);

    ObPLCacheCtx pc_ctx;
    pc_ctx.session_info_ = ctx.get_my_session();
    pc_ctx.schema_guard_ = ctx.get_sql_ctx()->schema_guard_;
    pc_ctx.raw_sql_ = PLSQL;
    MEMCPY(pc_ctx.sql_id_, ctx.get_sql_ctx()->sql_id_, (int32_t)sizeof(ctx.get_sql_ctx()->sql_id_));

    pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_PRCR;
    pc_ctx.key_.db_id_ = database_id;
    pc_ctx.key_.key_id_ = routine_id;
    pc_ctx.key_.sessid_ = ctx.get_my_session()->is_pl_debug_on() ? ctx.get_my_session()->get_sessid() : 0;
    pc_ctx.key_.mode_ =  ctx.get_my_session()->get_pl_profiler() != nullptr
                           ? ObPLObjectKey::ObjectMode::PROFILE : ObPLObjectKey::ObjectMode::NORMAL;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
      LOG_INFO("get pl function from plan cache failed",
               K(ret), K(pc_ctx.key_), K(package_id), K(routine_id));
      ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
    } else if (FALSE_IT(routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()))) {
      // do nothing
    } else if (OB_NOT_NULL(routine)) {
      LOG_DEBUG("get pl function from plan cache success", KPC(routine));
    }
    if (OB_SUCC(ret) && OB_ISNULL(routine)) {  // not in cache, compile it...
      bool need_update_schema = false;
      {
        ObBucketHashWLockGuard guard(codegen_lock_, routine_id);
        // check session status after get lock
        if (OB_FAIL(check_session_alive(*ctx.get_my_session()))){
          LOG_WARN("query or session is killed after get PL codegen lock", K(ret));

          // check pl cache again after get lock.
        } else if (OB_FAIL(ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
          LOG_INFO("get pl function from plan cache failed",
                   K(ret), K(pc_ctx.key_), K(package_id), K(routine_id));
          ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
        }
        OX (routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()));

        if (OB_SUCC(ret) && OB_ISNULL(routine)) {
          OZ (generate_pl_function(ctx, routine_id, cacheobj_guard), K(routine_id));
          OX (routine = static_cast<ObPLFunction*>(cacheobj_guard.get_cache_obj()));
          CK (OB_NOT_NULL(routine));
          if (OB_SUCC(ret)
              && routine->get_can_cached()) {
            routine->get_stat_for_update().name_ = routine->get_function_name();
            routine->get_stat_for_update().type_ = ObPLCacheObjectType::STANDALONE_ROUTINE_TYPE;
            OZ (add_pl_lib_cache(routine, pc_ctx));
          }
          OX (need_update_schema = true);
          LOG_DEBUG("get func by compile",
                     K(package_id), K(routine_id), KPC(routine));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(routine)) {
        const ObRoutineInfo *routine_info = NULL;
        ObErrorInfo error_info;
        const uint64_t tenant_id = routine->get_tenant_id();
        OZ (ctx.get_sql_ctx()->schema_guard_->get_routine_info(tenant_id, routine_id, routine_info));
        CK (OB_NOT_NULL(routine_info));
        OZ (error_info.delete_error(routine_info));
        if (need_update_schema) {
          OZ (ObPLCompiler::update_schema_object_dep_info(routine->get_dependency_table(),
                                                          routine->get_tenant_id(),
                                                          routine->get_owner(),
                                                          routine_id,
                                                          routine_info->get_schema_version(),
                                                          routine_info->get_object_type()));
        }
      }
    }
  }
  return ret;
}

int ObPL::add_pl_lib_cache(ObPLFunction *pl_func, ObPLCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObPlanCache *plan_cache = NULL;
  ObSQLSessionInfo *session = pc_ctx.session_info_;
  if (OB_ISNULL(session) || OB_ISNULL(pl_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(session), K(pl_func));
  } else if (OB_ISNULL(plan_cache = session->get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan cache is null");
  } else if (OB_FAIL(ObPLCacheMgr::add_pl_cache(plan_cache, pl_func, pc_ctx))) {
    if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("this plan has been added by others, need not add again", KPC(pl_func));
    } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
      if (REACH_TIME_INTERVAL(1000000)) { //1s, 当内存达到上限时, 该日志打印会比较频繁, 所以以1s为间隔打印
        LOG_DEBUG("can't add plan to plan cache",
                 K(ret), K(pl_func->get_mem_size()), K(pc_ctx.key_),
                 K(plan_cache->get_mem_used()));
      }
      ret = OB_SUCCESS;
    } else if (is_not_supported_err(ret)) {
      ret = OB_SUCCESS;
      LOG_DEBUG("plan cache don't support add this kind of plan now",  KPC(pl_func));
    } else {
      if (OB_REACH_MAX_CONCURRENT_NUM != ret) { //如果是达到限流上限, 则将错误码抛出去
        ret = OB_SUCCESS; //add plan出错, 覆盖错误码, 确保因plan cache失败不影响正常执行路径
        LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
      }
    }
  } else {
    LOG_DEBUG("add pl function to plan cache success", K(pc_ctx.key_));
  }
  return ret;
}

//anonymous of ps mode
int ObPL::generate_pl_function(ObExecContext &ctx,
                               const ObString &anonymouse_sql,
                               ParamStore &params,
                               ParseNode &parse_node,
                               ObCacheObjGuard& cacheobj_guard,
                               const uint64_t stmt_id,
                               bool is_anonymous_text)
{
  int ret = OB_SUCCESS;
  ParseNode *block_node = NULL;
  ObPLFunction *routine = NULL;
  ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());

  int64_t compile_start = ObTimeUtility::current_time();

  OZ (ObPLContext::valid_execute_context(ctx));

  // Anonymous block will be come a ObPLFunction object
  OZ (ObCacheObjectFactory::alloc(cacheobj_guard,
                                  ObLibCacheNameSpace::NS_PRCR,
                                  ctx.get_my_session()->get_effective_tenant_id()));
  OX (routine = static_cast<ObPLFunction *>(cacheobj_guard.get_cache_obj()));
  CK (OB_NOT_NULL(routine));

  // do parser
  if (OB_SUCC(ret)) {
    ObParser parser(ctx.get_allocator(),
                    ctx.get_my_session()->get_sql_mode(),
                    ctx.get_my_session()->get_charsets4parser());
    ParseResult parse_result;
    ParseMode parse_mode = ctx.get_sql_ctx()->is_dynamic_sql_ ? DYNAMIC_SQL_MODE
        : (ctx.get_my_session()->is_for_trigger_package() ? TRIGGER_MODE : STD_MODE);
    const ParseNode *parse_tree = NULL;
    OZ (parser.parse(anonymouse_sql, parse_result, parse_mode));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(parse_result.result_tree_)
              || OB_ISNULL(parse_result.result_tree_->children_)
              || OB_ISNULL(parse_tree = parse_result.result_tree_->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args",
               KP(parse_result.result_tree_),
               KP(parse_result.result_tree_->children_),
               KP(parse_result.result_tree_->children_[0]));
    } else if (T_SP_PRE_STMTS == parse_tree->type_) {
      OZ (ObPLResolver::resolve_condition_compile(
          ctx.get_allocator(),
          ctx.get_my_session(),
          ctx.get_sql_ctx()->schema_guard_,
          &(package_guard),
          ctx.get_sql_proxy(),
          NULL,
          parse_tree,
          parse_tree,
          false, /*inner_parse*/
          ctx.get_my_session()->is_for_trigger_package(),
          ctx.get_sql_ctx()->is_dynamic_sql_));
      CK (OB_NOT_NULL(parse_tree));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(T_SP_ANONYMOUS_BLOCK != parse_tree->type_
               || OB_ISNULL(block_node = parse_tree->children_[0]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the children of parse tree is NULL", K(ret), K(parse_tree->type_));
    } else if (T_SP_BLOCK_CONTENT != block_node->type_
               && T_SP_LABELED_BLOCK != block_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid procedure name node", K(block_node->type_), K(ret));
    } else {
      parse_node = *(parse_result.result_tree_);
    }
  }

  // do compile
  if (OB_SUCC(ret)) {
    ObPLCompiler compiler(ctx.get_allocator(),
                          *(ctx.get_my_session()),
                          *(ctx.get_sql_ctx()->schema_guard_),
                          *(ctx.get_package_guard()),
                          *(ctx.get_sql_proxy()));

    OZ (compiler.compile(
      block_node, stmt_id, *routine, &params, ctx.get_sql_ctx()->is_prepare_protocol_));
    OZ (routine->set_params_info(params, true));
  }

  int64_t compile_end = ObTimeUtility::current_time();
  LOG_INFO(">>>>>>>>>>Compile Anonymous Time: ",
           K(ret), K(params), K(anonymouse_sql), K(compile_end - compile_start), KPC(routine));
  return ret;
}

int ObPL::generate_pl_function(
  ObExecContext &ctx, uint64_t proc_id, ObCacheObjGuard& cacheobj_guard)
{
  int ret = OB_SUCCESS;
  ObPLFunction *routine = NULL;

  int64_t compile_start = ObTimeUtility::current_time();
  OZ (ObPLContext::valid_execute_context(ctx));
  OZ (ObCacheObjectFactory::alloc(cacheobj_guard,
                                  ObLibCacheNameSpace::NS_PRCR,
                                  ctx.get_my_session()->get_effective_tenant_id()));
  OX (routine = static_cast<ObPLFunction *>(cacheobj_guard.get_cache_obj()));
  CK (OB_NOT_NULL(routine));
  if (OB_SUCC(ret)) {
    ObPLCompiler compiler(ctx.get_allocator(),
                          *(ctx.get_my_session()),
                          *(ctx.get_sql_ctx()->schema_guard_),
                          *(ctx.get_package_guard()),
                          *(ctx.get_sql_proxy()));
    OZ (compiler.compile(proc_id, *routine), proc_id);
  }
  int64_t compile_end = ObTimeUtility::current_time();
  LOG_INFO(">>>>>>>>>>Compile Routine Time: ",
            K(ret), K(proc_id), K(compile_end - compile_start), KPC(routine));
  return ret;
}

int ObPL::insert_error_msg(int errcode)
{
  int ret = OB_SUCCESS;
  ObString err_txt(common::ob_get_tsi_err_msg(errcode));
  if (err_txt.empty()) {
    ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
    if (OB_NOT_NULL(wb)) {
      wb->set_error(common::ob_oracle_strerror(errcode), errcode);
    }
  }
  return ret;
}

int ObPL::check_trigger_arg(const ParamStore &params, const ObPLFunction &func)
{
  int ret = OB_SUCCESS;
  if (TriggerHandle::is_trigger_body_routine(func.get_package_id(), func.get_routine_id(), func.get_proc_type())) {
    const int64_t param_cnt = TriggerHandle::get_routine_param_count(func.get_routine_id());
    OV (params.count() == param_cnt, OB_ERR_UNEXPECTED, K(params.count()), K(param_cnt));
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; i++) {
      const ObPLDataType &data_type = func.get_variables().at(i);
      CK (data_type.is_record_type());
      CK (params.at(i).is_ext());
      if (OB_SUCC(ret)) {
        uint64_t udt_id = data_type.get_user_type_id();
        const ObUserDefinedType *udt = NULL;
        OV (OB_INVALID_ID != udt_id);
        for (int64_t j = 0; OB_SUCC(ret) && OB_ISNULL(udt) && j < func.get_type_table().count(); j++) {
          OV (OB_NOT_NULL(func.get_type_table().at(j)));
          if (OB_SUCC(ret) && func.get_type_table().at(j)->get_user_type_id() == udt_id) {
            udt = func.get_type_table().at(j);
          }
        }
        OV (OB_NOT_NULL(udt));
        OV (udt->is_record_type());
        if (OB_SUCC(ret)) {
          ObPLRecord *record = reinterpret_cast<ObPLRecord *>(params.at(i).get_ext());
          CK (OB_NOT_NULL(record));
          CK (record->get_count() == (static_cast<const ObRecordType *>(udt))->get_member_count());
        }
      }
    }
    LOG_DEBUG("check trigger routine arg end", K(ret), K(func), K(params));
  }
  return ret;
}

ObPLExecState::~ObPLExecState()
{
#ifdef OB_BUILD_ORACLE_PL
  if (dwarf_helper_ != NULL) {
    dwarf_helper_->~ObDWARFHelper();
  }
#endif
}

int ObPLExecState::get_var(int64_t var_idx, ObObjParam& result)
{
  int ret = OB_SUCCESS;
  ParamStore *params = ctx_.params_;
  CK (OB_NOT_NULL(params));
  CK (var_idx >= 0 && var_idx < params->count());
  OX (result = params->at(var_idx));
  return ret;
}

int ObPLExecState::set_var(int64_t var_idx, const ObObjParam& value)
{
  int ret = OB_SUCCESS;
  ParamStore *params = ctx_.params_;
  ObObjParam copy_value;

  CK (OB_NOT_NULL(params));
  CK (OB_NOT_NULL(get_allocator()));
  CK (var_idx >= 0 && var_idx < params->count());

  if (OB_SUCC(ret)
      && params->at(var_idx).is_pl_extend()
      && params->at(var_idx).get_ext() != 0
      && params->at(var_idx).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
    OZ (ObUserDefinedType::destruct_obj(params->at(var_idx), ctx_.exec_ctx_->get_my_session()));
  }

  if (OB_FAIL(ret)) {
  } else if (value.is_pl_extend()
              && value.get_ext() != 0
              && value.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
    OZ (ObUserDefinedType::deep_copy_obj(*get_allocator(), value, copy_value));
  } else {
    OZ (deep_copy_obj(*get_allocator(), value, copy_value));
  }

  if (OB_FAIL(ret)) {
  } else if (copy_value.is_null() && params->at(var_idx).is_pl_extend()) {
    if (params->at(var_idx).get_ext() == 0) {
      const ObUserDefinedType *user_type = NULL;
      uint64_t udt_id = params->at(var_idx).get_udt_id();
      OZ (ctx_.get_user_type(udt_id, user_type), K(udt_id));
      CK (OB_NOT_NULL(user_type));
      OZ (init_complex_obj(*get_allocator(), *user_type, params->at(var_idx)));
    }
  } else if (!copy_value.is_ext()) {
    bool is_ref_cursor = params->at(var_idx).is_ref_cursor_type();
    copy_value.ObObj::set_scale(params->at(var_idx).get_meta().get_scale());
    copy_value.set_accuracy(params->at(var_idx).get_accuracy());
    params->at(var_idx) = copy_value;
    params->at(var_idx).set_is_ref_cursor_type(is_ref_cursor);
    params->at(var_idx).set_param_meta();
  } else if (!params->at(var_idx).is_ref_cursor_type()) {
    int64_t udt_id = params->at(var_idx).get_udt_id();
    params->at(var_idx) = copy_value;
    params->at(var_idx).set_udt_id(udt_id);
  } else {
    params->at(var_idx) = copy_value;
  }
  OX (params->at(var_idx).set_param_meta());
  return ret;
}

int ObPLExecState::deep_copy_result_if_need()
{
  int ret = OB_SUCCESS;
  //all composite need to be deep copied and recorded.
  ObObj new_obj;
  ObIAllocator *allocator = ctx_.allocator_;
  if (func_.get_ret_type().is_composite_type() && result_.is_ext()) {
    CK (OB_NOT_NULL(allocator));
    CK (OB_NOT_NULL(ctx_.exec_ctx_));
    CK (OB_NOT_NULL(ctx_.exec_ctx_->get_pl_ctx()));
    OZ (ObUserDefinedType::deep_copy_obj(*allocator, result_, new_obj));
    if (OB_SUCC(ret) && func_.is_pipelined()) {
      CK (OB_NOT_NULL(ctx_.exec_ctx_));
      CK (OB_NOT_NULL(ctx_.exec_ctx_->get_my_session()));
      OZ (ObUserDefinedType::destruct_obj(result_, ctx_.exec_ctx_->get_my_session()));
    }
    OZ (ctx_.exec_ctx_->get_pl_ctx()->add(new_obj));
    OX (result_ = new_obj);
  } else if (func_.get_ret_type().is_obj_type() && result_.need_deep_copy()) {
    CK (OB_NOT_NULL(allocator));
    OZ (deep_copy_obj(*allocator, result_, new_obj));
    OX (result_ = new_obj);
  }
  return ret;
}

int ObPLExecState::add_pl_exec_time(int64_t pl_exec_time, bool is_called_from_sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("top context is null. ", K(ret));
  } else if (top_context_->get_exec_stack().count() > 0) {
    // get_exec_stack is already pop in final(ret)
    ObPLExecState *parent = top_context_->get_exec_stack().at(top_context_->get_exec_stack().count() - 1);
    CK (OB_NOT_NULL(parent));
    CK (parent != this);
    OX (parent->add_sub_plsql_exec_time(pl_exec_time));
    if (!is_called_from_sql) {
      OX (parent->add_pure_sql_exec_time(get_pure_sql_exec_time()));
    }
  } else {
    CK (OB_NOT_NULL(ctx_.exec_ctx_));
    CK (OB_NOT_NULL(ctx_.exec_ctx_->get_my_session()));
    // top pl exec time , need add to session
    OX ((ctx_.exec_ctx_->get_my_session()->add_plsql_exec_time(pl_exec_time)));
  }
  return ret;
}

bool ObPLExecCtx::valid()
{
  return OB_NOT_NULL(allocator_)
         && OB_NOT_NULL(exec_ctx_)
         // 通过interface机制映射进来的接口无法提供、也不会使用func_指针。
//       && OB_NOT_NULL(func_)
         && OB_NOT_NULL(exec_ctx_->get_sql_ctx())
         && OB_NOT_NULL(exec_ctx_->get_my_session());
}

int ObPLExecCtx::get_user_type(uint64_t type_id,
                                const ObUserDefinedType *&user_type,
                                ObIAllocator *allocator) const
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  user_type = NULL;
  CK (OB_NOT_NULL(func_));
  for (int64_t i = 0;
      OB_SUCC(ret) && NULL == user_type && i < func_->get_type_table().count();
      ++i) {
    CK (OB_NOT_NULL(func_->get_type_table().at(i)));
    if (OB_SUCC(ret) && type_id == func_->get_type_table().at(i)->get_user_type_id()) {
      user_type = func_->get_type_table().at(i);
    }
  }
  if (OB_SUCC(ret)
      && OB_ISNULL(user_type)
      && OB_NOT_NULL(allocator_)
      && OB_NOT_NULL(exec_ctx_)
      && OB_NOT_NULL(exec_ctx_->get_my_session())
      && OB_NOT_NULL(exec_ctx_->get_sql_ctx())
      && OB_NOT_NULL(exec_ctx_->get_sql_ctx()->schema_guard_)
      && OB_NOT_NULL(exec_ctx_->get_sql_proxy())
      && OB_NOT_NULL(guard_)) {
    pl::ObPLResolveCtx resolve_ctx(*allocator_,
                                    *(exec_ctx_->get_my_session()),
                                    *(exec_ctx_->get_sql_ctx()->schema_guard_),
                                    *(guard_),
                                    *(exec_ctx_->get_sql_proxy()),
                                    false);
    OZ (resolve_ctx.get_user_type(type_id, user_type));
  }
  return ret;
}

int ObPLExecCtx::calc_expr(uint64_t package_id, int64_t expr_idx, ObObjParam &result)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == package_id) {
    OZ (ObSPIService::spi_calc_expr_at_idx(this, expr_idx, OB_INVALID_INDEX, &result));
  } else {
    OZ (ObSPIService::spi_calc_package_expr(this, package_id, expr_idx, &result));
  }
  return ret;
}

int ObPLExecState::final(int ret)
{
  int tmp_ret = OB_SUCCESS;
  // PL执行失败, 需要将出参的数组也释放, 避免内存泄漏;
  // 如果是调用栈的顶层, 将入参的数组也释放掉;(PS协议,数组通过序列化协议构造)
  for (int64_t i = 0; top_call_ && i < func_.get_arg_count(); ++i) {
    if (func_.get_variables().at(i).is_composite_type()
        && i < get_params().count() && get_params().at(i).is_ext()) {
      // 纯IN参数直接释放
      // 如果是sql触发的存储过程执行, udf入参处理时不会做深拷, 此处不负责释放入参内存,
      // 由对应expr将obj挂到pl ctx上, 解决复杂内存的释放问题
      if (func_.get_in_args().has_member(i) && !func_.get_out_args().has_member(i) && is_called_from_sql_) {
        // do nothing
      } else if (func_.get_in_args().has_member(i) && !func_.get_out_args().has_member(i)) {
        // do nothing
      } else if (OB_SUCCESS != ret) { // OUT参数在失败的情况下在这里释放
        if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(get_params().at(i),
            ctx_.exec_ctx_->get_my_session()))) {
          LOG_WARN("failed to destruct pl object", K(i), K(tmp_ret));
        }
      }
    } else if (func_.get_variables().at(i).is_cursor_type()
      && func_.get_out_args().has_member(i) && !func_.get_in_args().has_member(i)) {
      // the session cursor, be used as a out param should be closed from session when error happen
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        ObPLCursorInfo *cursor = NULL;
        ObObjParam param;
        ObSPIService::ObCusorDeclareLoc loc;
        tmp_ret = ObSPIService::spi_get_cursor_info(&ctx_, func_.get_package_id(),
                                          func_.get_routine_id(),
                                          i, cursor, param, loc);
        int64_t cursor_id = NULL == cursor ? -1 : cursor->get_id();
        if (OB_SUCCESS == tmp_ret && NULL != cursor && cursor->is_session_cursor()
            && NULL != ctx_.exec_ctx_->get_my_session()) {
          // when execute fail. should use session close cursor
          ObSQLSessionInfo *session = ctx_.exec_ctx_->get_my_session();
          tmp_ret = session->close_cursor(cursor_id);
        }
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("faild close cursor. ", K(tmp_ret), K(cursor_id));
        }
      }
    }
  }
  // 1. inner call inout参数会深拷一份, 执行异常时需要释放
  // 2. inner call 纯out属性复杂数据类型参数, 会生成一个新的obj, 执行失败时会抛出异常, 不会走到geneate_out_param里面的释放内存逻辑
  // 需要提前释放内存
  for (int64_t i = 0; OB_SUCCESS != ret && inner_call_ && !func_.is_function() && i < func_.get_arg_count(); ++i) {
    if (OB_NOT_NULL(ctx_.nocopy_params_) &&
        ctx_.nocopy_params_->count() > i &&
        OB_INVALID_INDEX == ctx_.nocopy_params_->at(i) &&
        func_.get_variables().at(i).is_composite_type() &&
        i < get_params().count() && get_params().at(i).is_ext()) {
      if (func_.get_out_args().has_member(i)) {
        if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(get_params().at(i),
            ctx_.exec_ctx_->get_my_session()))) {
          LOG_WARN("failed to destruct pl object", K(i), K(tmp_ret));
        }
      }
    }
  }
  for (int64_t i = func_.get_arg_count(); i < func_.get_variables().count(); ++i) {
    if (func_.get_variables().at(i).is_composite_type()
        && i < get_params().count() && get_params().at(i).is_ext()) {
      if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(get_params().at(i),
          ctx_.exec_ctx_->get_my_session()))) {
        LOG_WARN("failed to destruct pl object", K(i), K(tmp_ret));
      }
    } else if (func_.get_variables().at(i).is_cursor_type()) {
      int tmp_ret = OB_SUCCESS;
      ObPLCursorInfo *cursor = NULL;
      ObObjParam param;
      ObSPIService::ObCusorDeclareLoc loc;
      ObSQLSessionInfo *session = ctx_.exec_ctx_->get_my_session();
      tmp_ret = ObSPIService::spi_get_cursor_info(&ctx_, func_.get_package_id(),
                                        func_.get_routine_id(),
                                        i, cursor, param, loc);
      if (OB_SUCCESS == tmp_ret && NULL != cursor) {
        if (0 == cursor->get_ref_count() && (cursor->is_session_cursor() || cursor->is_ref_by_refcursor())) {
          // when refcount is 0. should use session close cursor
          ObSQLSessionInfo *session = ctx_.exec_ctx_->get_my_session();
          tmp_ret = session->close_cursor(cursor->get_id());
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_INFO("close session cursor after pl exec.", K(ret), K(tmp_ret), K(cursor->get_id()));
        } else if (OB_FAIL(ret)) {
          // 函数结束这儿还需要close cursor，因为如果有异常，block结束除的close cursor就走不到，这儿还需要关闭
          // 这儿为啥可能为null
          /*
          *
          * create or replace procedure pp(a number) is
            b number;
            begin
            for c1 in (select * from t) loop
              dbms_output.put_line('c1 ' || c1.a || '  ' || c1.b);
            end loop;
            null;
            raise_application_error(-20002, 'test error');
            null;
            for c2 in (select * from tt) loop
              dbms_output.put_line('c2 ' || c2.a || ' ' || c2.b);
            end loop;
            exception
            when others then
              dbms_output.put_line('catch exception');
            end;
          *
          * 上例中c1 调用了cursor init，但是c2没有调用，因为被execption打断，这个时候在final函数里面调用cursor close
          * 函数，这个obj就是null，因为c2没有调用cursor init。 另外goto也可能导致执行流变动，没有open就去close
          */
          if (OB_SUCCESS != ObSPIService::spi_cursor_close(&ctx_, func_.get_package_id(),
                                                  func_.get_routine_id(), i, true)) {
            LOG_WARN("failed to get cursor info", K(tmp_ret),
              K(func_.get_package_id()), K(func_.get_routine_id()), K(i));
          }
        } else {
          // local cursor must be closed.
          if (!cursor->is_session_cursor() && !cursor->is_ref_by_refcursor()) {
            if (OB_SUCCESS != ObSPIService::spi_cursor_close(&ctx_, func_.get_package_id(),
                                                    func_.get_routine_id(), i, true)) {
              LOG_WARN("failed to close cursor info", K(tmp_ret),
              K(func_.get_package_id()), K(func_.get_routine_id()), K(i));
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && func_.get_ret_type().is_composite_type() && result_.is_ext() && func_.is_pipelined()) {
    tmp_ret = ObUserDefinedType::destruct_obj(result_, ctx_.exec_ctx_->get_my_session());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to destruct pl object", K(tmp_ret));
    }
  }

#ifdef OB_BUILD_ORACLE_PL
  // save profiler time stack data
  if (OB_NOT_NULL(profiler_time_stack_)) {
    ObPLProfiler *profiler = nullptr;

    if (OB_NOT_NULL(ctx_.exec_ctx_)
          && OB_NOT_NULL(ctx_.exec_ctx_->get_my_session())
          && OB_NOT_NULL(profiler = ctx_.exec_ctx_->get_my_session()->get_pl_profiler())) {
      int ret = OB_SUCCESS;

      if (OB_FAIL(profiler_time_stack_->pop_all(*profiler))) {
        LOG_WARN("[DBMS_PROFILER] failed to flush pl profiler time stack", K(ret), K(lbt()));
      }

      if (is_top_call()) {
        if (OB_FAIL(profiler->flush_data())) {
          LOG_WARN("[DBMS_PROFILER] failed to flush pl profiler data", K(ret), K(lbt()));
        }
      }
    }

    profiler_time_stack_->~ObPLProfilerTimeStack();
    if (OB_NOT_NULL(get_allocator())) {
      get_allocator()->free(profiler_time_stack_);
    }
    profiler_time_stack_ = nullptr;
  }
#endif // OB_BUILD_ORACLE_PL

  if (OB_NOT_NULL(top_context_)
      && top_context_->get_exec_stack().count() > 0
      && top_context_->get_exec_stack().at(
        top_context_->get_exec_stack().count() - 1
      ) == this) {
    top_context_->get_exec_stack().pop_back();
  }

  // reset physical plan context
  if (need_reset_physical_plan_) {
    if (func_.get_expr_op_size() > 0) {
      //Memory leak
      //Must be reset before free expr_op_ctx!
      ctx_.exec_ctx_->reset_expr_op();
      ctx_.exec_ctx_->get_allocator().free(ctx_.exec_ctx_->get_expr_op_ctx_store());
    }
    exec_ctx_bak_.restore(*ctx_.exec_ctx_);
  }

  if (OB_NOT_NULL(ctx_.exec_ctx_->get_my_session())) {
  #ifdef OB_BUILD_ORACLE_PL
    ObSQLSessionInfo *session = ctx_.exec_ctx_->get_my_session();
    ObPlJsonTypeManager::release_useless_resource(session->get_json_pl_mngr());
  #endif
  }

  return OB_SUCCESS;
}

int ObPLExecState::init_complex_obj(ObIAllocator &allocator,
                                    const ObPLDataType &pl_type,
                                    common::ObObjParam &obj,
                                    bool set_null)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  common::ObMySQLProxy *sql_proxy = NULL;
  ObPLPackageGuard *package_guard = NULL;
  const ObPLDataType *real_pl_type = &pl_type;
  OX (set_null = pl_type.is_record_type() ? true : set_null);
  CK (OB_NOT_NULL(session = ctx_.exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(schema_guard = ctx_.exec_ctx_->get_sql_ctx()->schema_guard_));
  CK (OB_NOT_NULL(sql_proxy = ctx_.exec_ctx_->get_sql_proxy()));
  CK (OB_NOT_NULL(package_guard = ctx_.exec_ctx_->get_package_guard()));

  if (OB_FAIL(ret)) {
  } else if (pl_type.is_generic_type()) {
    ObPLComposite *composite = NULL;
    const ObUserDefinedType* user_type = NULL;
    if (!obj.is_pl_extend()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "generic paramter has a non composite input value");
    } else if (OB_ISNULL(composite = reinterpret_cast<ObPLComposite*>(obj.get_ext()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "generic parameter has null value pointer");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(session->get_pl_context())
          && OB_NOT_NULL(session->get_pl_context()->get_current_ctx())) {
      pl::ObPLINS *ns = session->get_pl_context()->get_current_ctx();
      OZ (ns->get_user_type(composite->get_id(), user_type));
      CK (OB_NOT_NULL(user_type));
    } else {
      ObPLResolveCtx ns(allocator,
                      *session,
                      *schema_guard,
                      *package_guard,
                      *sql_proxy,
                      false);
      OZ (ns.get_user_type(composite->get_id(), user_type));
      CK (OB_NOT_NULL(user_type));
    }
    OX (real_pl_type = user_type);
  }

  if (OB_FAIL(ret)) {
  } else if (real_pl_type->is_ref_cursor_type() || real_pl_type->is_sys_refcursor_type()) {
    OX (obj.set_is_ref_cursor_type(true));
  } else if (real_pl_type->is_udt_type()) {
    ObPLUDTNS ns(*schema_guard);
    OZ (ns.init_complex_obj(allocator, *real_pl_type, obj, false, set_null));
  } else if (OB_NOT_NULL(session->get_pl_context())
      && OB_NOT_NULL(session->get_pl_context()->get_current_ctx())) {
    pl::ObPLINS *ns = session->get_pl_context()->get_current_ctx();
    CK (OB_NOT_NULL(ns));
    OZ (ns->init_complex_obj(allocator, *real_pl_type, obj, false, set_null));
  } else {
    ObPLResolveCtx ns(allocator,
                      *session,
                      *schema_guard,
                      *package_guard,
                      *sql_proxy,
                      false);
    OZ (ns.init_complex_obj(allocator, *real_pl_type, obj, false, set_null));
  }
  OX (obj.set_udt_id(real_pl_type->get_user_type_id()));
  return ret;
}


int ObPLExecState::check_routine_param_legal(ParamStore *params)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  common::ObMySQLProxy *sql_proxy = NULL;
  ObPLPackageGuard *package_guard = NULL;
  CK (OB_NOT_NULL(session = ctx_.exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(schema_guard = ctx_.exec_ctx_->get_sql_ctx()->schema_guard_));
  CK (OB_NOT_NULL(sql_proxy = ctx_.exec_ctx_->get_sql_proxy()));
  CK (OB_NOT_NULL(package_guard = ctx_.exec_ctx_->get_package_guard()));

  int64_t arg_num = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < func_.get_variables().count(); ++i) {
    if (func_.get_in_args().has_member(i) ||
        func_.get_out_args().has_member(i)) {
      ++arg_num;
    }
  }

  if ((NULL == params && arg_num != 0) ||
      (NULL != params && params->count() != arg_num)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("routine parameters is not match", K(ret), K(arg_num));
  }
  if (OB_SUCC(ret) && NULL != params) {
    for (int64_t i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
      const ObPLDataType &dest_type = func_.get_variables().at(i);
      if (params->at(i).is_null() ||
          params->at(i).is_pl_mock_default_param()) {
        // need not check
      } else if (!params->at(i).is_ext()) { // basic type
        if (!dest_type.is_obj_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("incorrect argument type, expected complex, but get basic type", K(ret));
        }
      } else if (0 == params->at(i).get_ext()
                || ((PL_REF_CURSOR_TYPE == params->at(i).get_meta().get_extend_type()
                     || PL_CURSOR_TYPE == params->at(i).get_meta().get_extend_type())
                      && dest_type.is_cursor_type())) {
        // do nothing
      } else {
        const pl::ObPLComposite *src_composite = NULL;
        uint64_t udt_id = params->at(i).get_udt_id();
        CK (OB_NOT_NULL(src_composite = reinterpret_cast<const ObPLComposite *>(params->at(i).get_ext())));
        OV (params->at(i).is_pl_extend(), OB_ERR_UNEXPECTED, K(params->at(i)), K(i));
        if (OB_FAIL(ret)) {
        } else if (!dest_type.is_composite_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("incorrect argument type", K(ret));
        } else if (OB_INVALID_ID == udt_id) {
          if (PL_RECORD_TYPE == params->at(i).get_meta().get_extend_type()
              || PL_NESTED_TABLE_TYPE == params->at(i).get_meta().get_extend_type()
              || PL_ASSOCIATIVE_ARRAY_TYPE == params->at(i).get_meta().get_extend_type()
              || PL_VARRAY_TYPE == params->at(i).get_meta().get_extend_type()) {
            const ObPLComposite *composite = reinterpret_cast<const ObPLComposite *>(params->at(i).get_ext());
            CK (OB_NOT_NULL(composite));
            OV (udt_id = composite->get_id(), OB_ERR_UNEXPECTED, KPC(composite), K(params->at(i)), K(i));
          } else if (PL_OPAQUE_TYPE == params->at(i).get_meta().get_extend_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("opaque type without udtid is unexpected", K(ret), K(params->at(i)), K(i));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_INVALID_ID == udt_id) { // 匿名数组
          const pl::ObPLCollection *src_coll = NULL;
          ObPLResolveCtx resolve_ctx(*get_allocator(),
                                      *session,
                                      *schema_guard,
                                      *package_guard,
                                      *sql_proxy,
                                      false);
          const pl::ObUserDefinedType *pl_user_type = NULL;
          const pl::ObCollectionType *coll_type = NULL;
          OZ (resolve_ctx.get_user_type(dest_type.get_user_type_id(), pl_user_type));
          CK (OB_NOT_NULL(coll_type = static_cast<const ObCollectionType *>(pl_user_type)));
          CK (OB_NOT_NULL(src_coll = static_cast<const ObPLCollection *>(src_composite)));
          if (OB_FAIL(ret)) {
          } else if (coll_type->get_element_type().is_obj_type() ^
                     src_coll->get_element_desc().is_obj_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("incorrect argument type, diff type",
                          K(ret), K(coll_type->get_element_type()), K(src_coll->get_element_desc()));
          } else if (coll_type->get_element_type().is_obj_type()) { // basic data type
            const ObDataType *src_data_type = &src_coll->get_element_desc();
            const ObDataType *dst_data_type = coll_type->get_element_type().get_data_type();
            if (dst_data_type->get_obj_type() == src_data_type->get_obj_type()
                || (cast_supported(src_data_type->get_obj_type(),
                                      src_data_type->get_collation_type(),
                                      dst_data_type->get_obj_type(),
                                      dst_data_type->get_collation_type()))) {
              // do nothing ...
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("incorrect argument type, diff type", K(ret));
            }
          } else {
            // element is composite type
            uint64_t element_type_id = src_coll->get_element_desc().get_udt_id();
            bool is_compatible = element_type_id == coll_type->get_element_type().get_user_type_id();
            if (!is_compatible) {
              OZ (ObPLResolver::check_composite_compatible(
                ctx_, element_type_id, dest_type.get_user_type_id(), is_compatible));
            }
            if (OB_SUCC(ret) && !is_compatible) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("incorrect argument type", K(ret));
            }
          }
          // do cast to udt
          if (OB_SUCC(ret)) {
            int64_t dst_size = 0;
            ObObjParam &param = params->at(i);
            ObObj dst;
            ObObj *dst_ptr = &dst;
            ObObj *src_ptr = &param;
            OZ (pl_user_type->init_obj(*(schema_guard), *get_allocator(), dst, dst_size));
            OZ (pl_user_type->convert(resolve_ctx, src_ptr, dst_ptr));
            if (OB_SUCC(ret)) {
              ObPLCollection *collection = reinterpret_cast<ObPLCollection*>(param.get_ext());
              if (OB_NOT_NULL(collection)
                  && OB_NOT_NULL(dynamic_cast<ObPLCollAllocator *>(collection->get_allocator()))) {
                collection->get_allocator()->reset();
                collection->set_data(NULL);
                collection->set_count(0);
                collection->set_first(OB_INVALID_INDEX);
                collection->set_last(OB_INVALID_INDEX);
              }
            }
            CK (OB_NOT_NULL(ctx_.exec_ctx_->get_pl_ctx()));
            if (OB_FAIL(ret)) {
              ObUserDefinedType::destruct_obj(dst, session);
            }
            OZ (ctx_.exec_ctx_->get_pl_ctx()->add(dst));
            OX (param = dst);
            OX (param.set_param_meta());
            OX (param.set_udt_id(pl_user_type->get_user_type_id()));
          }
        } else { //非匿名数组复杂类型
          uint64_t left_type_id = udt_id;
          uint64_t right_type_id = dest_type.get_user_type_id();
          bool is_compatible = false;
          if (left_type_id == right_type_id) {
            is_compatible = true;
          } else {
            OZ (ObPLResolver::check_composite_compatible(ctx_, left_type_id, right_type_id, is_compatible), K(left_type_id), K(right_type_id));
          }
          if (OB_SUCC(ret) && !is_compatible) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("incorrect argument type", K(ret));
          }
        }
        
      }
    }
  }
  return ret;
}

int ObPLExecState::init_params(const ParamStore *params, bool is_anonymous)
{
  int ret = OB_SUCCESS;
  int param_cnt = OB_NOT_NULL(params) ? params->count() : 0;

  CK (OB_NOT_NULL(ctx_.exec_ctx_));
  if (OB_SUCC(ret) && OB_ISNULL(ctx_.exec_ctx_->get_pl_ctx())) {
    OZ (ctx_.exec_ctx_->init_pl_ctx());
    CK (OB_NOT_NULL(ctx_.exec_ctx_->get_pl_ctx()));
  }
  if (OB_SUCC(ret) && top_call_ && ctx_.exec_ctx_->get_sql_ctx()->is_execute_call_stmt_) {
    OZ (check_routine_param_legal(const_cast<ParamStore *>(params)));
  }
  OZ (get_params().reserve(func_.get_variables().count()));
  ObObjParam param;
  for (int64_t i = 0; OB_SUCC(ret) && i < func_.get_variables().count(); ++i) {
    param.reset();
    param.ObObj::reset();
    if (func_.get_variables().at(i).is_obj_type()) {
      CK (OB_NOT_NULL(func_.get_variables().at(i).get_data_type()));
      OX (param.set_meta_type(func_.get_variables().at(i).get_data_type()->get_meta_type()));
      OX (param.set_param_meta());
      OX (param.set_accuracy(func_.get_variables().at(i).get_data_type()->get_accuracy()));
      if (OB_SUCC(ret) && i >= param_cnt) {
        ObObjMeta null_meta = param.get_meta();
        param.set_null();
        param.set_null_meta(null_meta);
      }
    } else if (func_.get_variables().at(i).is_ref_cursor_type()) {
      OX (param.set_is_ref_cursor_type(true));
      // CURSOR初始化为NULL
    } else if (func_.get_variables().at(i).is_cursor_type()) {
      // leave obj as null type, spi_init wil init it.
    } else if (func_.get_variables().at(i).is_subtype()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("found subtype in variables symbol table is unexpected", K(ret), K(i));
    } else {
      param.set_type(ObExtendType);
      param.set_extend(0, func_.get_variables().at(i).get_type());
      param.set_param_meta();
      param.set_udt_id(func_.get_variables().at(i).get_user_type_id());
    }
    OZ (get_params().push_back(param));
  }

  if (OB_SUCC(ret)
      && !is_anonymous
      && OB_ISNULL(params)
      && func_.get_arg_count() != 0) {
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    LOG_WARN("param count is not equal to pl defination", K(func_.get_arg_count()), K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(params)) {
    if (get_params().count() < params->count()) {
      ret = OB_ERR_SP_WRONG_ARG_NUM;
      LOG_WARN("param count is larger than symbol table",
               K(func_.get_variables().count()), K(params->count()), K(ret));
    } else if (!is_anonymous && func_.get_arg_count() != params->count()) {
      ret = OB_ERR_SP_WRONG_ARG_NUM;
      LOG_WARN("param count is not equal to pl defination",
               K(func_.get_arg_count()), K(params->count()), K(ret));
    }
  }

#define CHECK_NOT_NULL_VIOLATED(i, param)                             \
do {                                                                  \
  const ObPLDataType &pl_type = func_.get_variables().at(i);          \
  if (OB_FAIL(ret)) {                                                 \
  } else if (pl_type.is_not_null()) {                                 \
    if (func_.get_out_args().has_member(i)                            \
        && (ob_is_string_or_lob_type(pl_type.get_obj_type())          \
           || ob_is_raw_tc(pl_type.get_obj_type()))                   \
        && lib::is_oracle_mode()) {                                 \
      /* In Oracle Mode: StringTC, LobTC, RawTC as Out parameter, */  \
      /* Ignore not null violated! */                                 \
    } else if (param.is_null() || param.is_null_oracle()) {           \
      ret = OB_NULL_CHECK_ERROR;                                      \
      LOG_WARN("not null check violated!", K(ret), K(i), K(param));   \
    }                                                                 \
  }                                                                   \
} while (0)

  if (OB_SUCC(ret) && NULL != params) {

    bool is_strict = is_strict_mode(ctx_.exec_ctx_->get_my_session()->get_sql_mode());
    ObCastMode cast_mode = CM_NONE;
    ObExprResType result_type;
    OZ (ObSQLUtils::get_default_cast_mode(
        stmt::T_NONE, ctx_.exec_ctx_->get_my_session(), cast_mode));

    for (int64_t i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
      /*
       * In Args of Normal Routine, Check all arguments if need to convert.
       * In Args of Anonymous, If need_to_check_type_ is true then check to convert.
       *   else It will directly used by static sql, do not need to check.
       */
      if (func_.get_in_args().has_member(i)) {
        const ObPLDataType &pl_type = func_.get_variables().at(i);
        if (is_anonymous && !func_.get_params_info().at(i).flag_.need_to_check_type_) {
          OX (get_params().at(i) = params->at(i));
        } else if (params->at(i).is_pl_mock_default_param()) { // 使用参数默认值
          ObObjParam result;
          sql::ObSqlExpression *default_expr = func_.get_default_expr(i);
          OV (OB_NOT_NULL(default_expr), OB_ERR_UNEXPECTED, K(i), K(func_.get_default_idxs()));
          OZ (ObSPIService::spi_calc_expr(&ctx_, default_expr, OB_INVALID_INDEX, &result));
          if (OB_SUCC(ret) && result.is_null()) {
            ObObjMeta null_meta = get_params().at(i).get_meta();
            get_params().at(i).set_null();
            get_params().at(i).set_null_meta(null_meta);
          } else {
            OX (get_params().at(i) = result);
          }
          if (pl_type.is_composite_type() && result.is_null()) {
            OZ (init_complex_obj(
              (*get_allocator()), func_.get_variables().at(i), get_params().at(i)));
          }
        } else if (pl_type.is_composite_type()
                   && (params->at(i).is_null()
                        || (params->at(i).is_ext() && params->at(i).get_ext() == 0))) {
          // 如果复杂类型传入了null则构造一个初始化的值
          OZ (init_complex_obj((*get_allocator()),
                               func_.get_variables().at(i),
                               get_params().at(i)));
        } else if (pl_type.is_obj_type() // 复杂类型不需要做类型转换(不支持复杂类型的转换), 直接赋值
                   && (params->at(i).get_meta() != get_params().at(i).get_meta()
                      || params->at(i).get_accuracy() != get_params().at(i).get_accuracy())) {
          if (params->at(i).is_null()
              || (lib::is_oracle_mode() && params->at(i).is_null_oracle())) {
            ObObjMeta null_meta = get_params().at(i).get_meta();
            get_params().at(i) = params->at(i); // 空值不做cast
            params->at(i).is_null() ? get_params().at(i).set_null_meta(null_meta) : (void)NULL;
          } else if (lib::is_oracle_mode()
                       && params->at(i).get_meta().get_type() == get_params().at(i).get_meta().get_type()
                       && params->at(i).get_meta().is_numeric_type()) {
            ObObj tmp;
            if (pl_type.is_pl_integer_type()) {
              OZ (ObExprPLIntegerChecker::calc(tmp,
                                               params->at(i),
                                               pl_type.get_pl_integer_type(),
                                               pl_type.get_range(),
                                               *get_allocator()));
              OX (get_params().at(i) = tmp);
            } else {
              get_params().at(i) = params->at(i);
            }
          } else if (params->at(i).get_meta().is_ext()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported complex type cast to basic type", K(ret), K(pl_type));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "complex type cast to basic type");
          } else if (get_params().at(i).get_meta().is_null()
                     && is_anonymous
                     && func_.get_is_all_sql_stmt()) {
            // 匿名块中全部是SQL语句, 需要向Null转的情况, 这里直接跳过
            get_params().at(i) = params->at(i);
          } else {
            LOG_DEBUG("column convert",
                      K(i), K(params->at(i).get_meta()), K(get_params().at(i).get_meta()),
                      K(params->at(i).get_accuracy()), K(get_params().at(i).get_accuracy()));
            const ObDataTypeCastParams dtc_params =
              ObBasicSessionInfo::create_dtc_params(ctx_.exec_ctx_->get_my_session());
            ObCastCtx cast_ctx(get_allocator(),
                               &dtc_params,
                               cast_mode,
                               get_params().at(i).get_collation_type());
            result_type.reset();
            result_type.set_meta(func_.get_variables().at(i).get_data_type()->get_meta_type());
            result_type.set_accuracy(func_.get_variables().at(i).get_data_type()->get_accuracy());
            ObObj tmp;
            // CHARSET_ANY代表接受任何字符集,因此不能改变入参的字符集
            if (CS_TYPE_ANY == result_type.get_collation_type()) {
              if (params->at(i).get_meta().is_string_or_lob_locator_type()) {
                result_type.set_collation_type(params->at(i).get_meta().get_collation_type());
              } else {
                result_type.set_collation_type(ctx_.exec_ctx_->get_my_session()->get_nls_collation());
              }
            }
            if ((result_type.is_blob() || result_type.is_blob_locator()
                 || params->at(i).is_blob() || params->at(i).is_blob_locator())
                && lib::is_oracle_mode()) {
              cast_ctx.cast_mode_ |= CM_ENABLE_BLOB_CAST;
            }
            if (OB_FAIL(ObExprColumnConv::convert_with_null_check(
                  tmp, params->at(i), result_type, is_strict, cast_ctx,
                  &(func_.get_variables().at(i).get_type_info())))) {
              LOG_WARN("Cast result type failed",
                        K(ret), K(params->at(i)), K(result_type), K(is_strict), K(i),
                        K(params->count()), K(func_.get_is_all_sql_stmt()),
                        K(func_.get_variables()));
            } else if (pl_type.is_pl_integer_type()
                       && OB_FAIL(ObExprPLIntegerChecker::calc(
                         tmp, tmp, pl_type.get_pl_integer_type(), pl_type.get_range(),
                         *get_allocator()))) {
              LOG_WARN("failed to check pls integer value", K(ret));
            } else if (OB_FAIL(get_params().at(i).apply(tmp))) {
              LOG_WARN("failed to apply tmp to params",
                       K(ret), K(tmp), K(i), K(params->count()));
            } else {
              //
              // 由存储层传入的数据未设置collation_level, 这里设置下
              if (get_params().at(i).is_string_type()) {
                get_params().at(i).set_collation_level(result_type.get_collation_level());
              }
              get_params().at(i).set_param_meta();
            }
          }
        } else {
          if (pl_type.is_pl_integer_type()) {
            ObObj tmp;
            OZ (ObExprPLIntegerChecker::calc(tmp,
                                             params->at(i),
                                             pl_type.get_pl_integer_type(),
                                             pl_type.get_range(),
                                             *get_allocator()));
            OX (get_params().at(i) = tmp);
          } else {
            // same type, we already check this on resolve stage, here directly assign value to symbol.
            if (get_params().at(i).is_ref_cursor_type()) {
              get_params().at(i) = params->at(i);
              get_params().at(i).set_is_ref_cursor_type(true);
            } else if (pl_type.is_collection_type() && OB_INVALID_ID == params->at(i).get_udt_id()) {
              ObPLComposite *composite = NULL;
              get_params().at(i) = params->at(i);
              get_params().at(i).set_udt_id(pl_type.get_user_type_id());
              composite = reinterpret_cast<ObPLComposite *>(params->at(i).get_ext());
              if (OB_NOT_NULL(composite) && composite->is_collection() && OB_INVALID_ID == composite->get_id()) {
                composite->set_id(pl_type.get_user_type_id());
              }
            } else {
              get_params().at(i) = params->at(i);
            }
          }
        }
        CHECK_NOT_NULL_VIOLATED(i, get_params().at(i));
      } else {
        CHECK_NOT_NULL_VIOLATED(i, params->at(i));
        if (OB_FAIL(ret)) {
        } else if (func_.get_variables().at(i).is_obj_type()) {
          // 纯OUT参数, 对于基础类型直接传入NULL的ObObj
          ObObj obj;  // 基础类型apply一个空的OBJECT
          ObObjMeta null_meta = get_params().at(i).get_meta();
          OZ (get_params().at(i).apply(obj));
          OX (get_params().at(i).set_null_meta(null_meta));
        } else if (is_anonymous
                   && (func_.get_variables().at(i).is_nested_table_type()
                        || func_.get_variables().at(i).is_varray_type())
                   && params->at(i).is_ext()) {
#ifndef OB_BUILD_ORACLE_PL
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support nested table type", K(ret));
#else
          // anonymous out collection only in ps mode.
          ObPLNestedTable *table = NULL;
          CK (OB_NOT_NULL(table = reinterpret_cast<ObPLNestedTable *>(params->at(i).get_ext())));
          for (int64_t i = 0; OB_SUCC(ret) && i < table->get_count(); ++i) {
            OZ (table->delete_collection_elem(i), K(i), KPC(table));
          }
          OX (get_params().at(i) = params->at(i));
#endif
        } else {
          // 纯OUT参数, 对于复杂类型需要重新初始化值; 如果传入的复杂类型值为NULL(PS协议), 则初始化一个新的复杂类型
          // 这里先copy入参的值, 由init_complex_obj函数判断是否重新分配内存
          OX (get_params().at(i) = params->at(i));
          OZ (init_complex_obj(*(get_allocator()),
                               func_.get_variables().at(i),
                               get_params().at(i),
                               !(top_call_ && ctx_.exec_ctx_->get_sql_ctx()->is_execute_call_stmt_)));
        }
      }
    }
  }

#undef CHECK_NOT_NULL_VIOLATED

  return ret;
}

void ExecCtxBak::backup(sql::ObExecContext &ctx)
{
#define DO_EXEC_CTX_ATTR_BACKUP(x) x = ctx.x;ctx.x = 0
  LST_DO_CODE(DO_EXEC_CTX_ATTR_BACKUP, EXPAND(PL_EXEC_CTX_BAK_ATTRS));
#undef DO_EXEC_CTX_ATTR_BACKUP
}

void ExecCtxBak::restore(sql::ObExecContext &ctx)
{
#define DO_EXEC_CTX_ATTR_RESTORE(x) ctx.x = x
  LST_DO_CODE(DO_EXEC_CTX_ATTR_RESTORE, EXPAND(PL_EXEC_CTX_BAK_ATTRS));
#undef DO_EXEC_CTX_ATTR_RESTORE
}

int ObPLExecState::init(const ParamStore *params, bool is_anonymous)
{
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  int64_t total_time = 0;

  CK (OB_NOT_NULL(ctx_.exec_ctx_));
  OZ (ObPLContext::valid_execute_context(*ctx_.exec_ctx_));

  OZ (ctx_.exec_ctx_->get_my_session()->get_query_timeout(query_timeout));
  OX (total_time =
    (ctx_.exec_ctx_->get_physical_plan_ctx() != NULL
     && ctx_.exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp() > 0) ?
      ctx_.exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp()
      : ctx_.exec_ctx_->get_my_session()->get_query_start_time() + query_timeout);
  OX (phy_plan_ctx_.set_timeout_timestamp(total_time));

  OX (exec_ctx_bak_.backup(*ctx_.exec_ctx_));
  OX (ctx_.exec_ctx_->set_physical_plan_ctx(&get_physical_plan_ctx()));
  OX (ctx_.exec_ctx_->get_physical_plan_ctx()->set_cur_time(ObTimeUtility::current_time(), *ctx_.exec_ctx_->get_my_session()));
  OX (need_reset_physical_plan_ = true);
  if (OB_SUCC(ret) && func_.get_expr_op_size() > 0)  {
    OZ (ctx_.exec_ctx_->init_expr_op(func_.get_expr_op_size(), ctx_.allocator_));
  }

  if (OB_SUCC(ret)) {
    // TODO bin.lb: how about the memory?
    //
    OZ(func_.get_frame_info().pre_alloc_exec_memory(*ctx_.exec_ctx_, ctx_.allocator_));
  }

  // init params may use exec stack, need append to pl context first
  CK (OB_NOT_NULL(top_context_ = ctx_.exec_ctx_->get_my_session()->get_pl_context()));
  OZ (top_context_->get_exec_stack().push_back(this));

  OZ (init_params(params, is_anonymous));

  OX (top_context_->set_has_output_arguments(!func_.get_out_args().is_empty()));

  if (OB_SUCC(ret)) {
    if (func_.need_register_debug_info()) {
      OZ (ObPLContext::notify(ctx_.exec_ctx_->get_my_session()));
    }
  }
  return ret;
}

int ObPLExecRecursionCtx::init(sql::ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObObj max_recursion_value;
  if (OB_FAIL(session_info.get_sys_variable(
      SYS_VAR_MAX_SP_RECURSION_DEPTH, max_recursion_value))) {
    LOG_WARN("fail to get system variable value", K(ret), K(SYS_VAR_MAX_SP_RECURSION_DEPTH));
  } else {
    // Oracle兼容: 不限制递归的层次
    max_recursion_depth_ = lib::is_oracle_mode() ? INT64_MAX : max_recursion_value.get_int();
    init_ = true;
  }
  return ret;
}

int ObPLExecRecursionCtx::inc_and_check_depth(uint64_t package_id, uint64_t proc_id, bool is_function)
{
  int ret = OB_SUCCESS;
  int64_t recursion_depth = 0;
  int64_t *depth_store = NULL;
  // 兼容mysql, function不允许嵌套
  int64_t max_recursion_depth = is_function && lib::is_mysql_mode() ? 0 : max_recursion_depth_;
  if (!init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("recursion context not init", K(ret), K(init_));
  } else {
    if (OB_LIKELY(!recursion_depth_map_.created())) {
      FOREACH_CNT(it, recursion_depth_array_) {
        if (it->first.first == package_id
            && it->first.second == proc_id) {
          recursion_depth = it->second;
          depth_store = &it->second;
          break;
        }
      }
    } else {
      if (OB_FAIL(recursion_depth_map_.get_refactored(std::make_pair(package_id, proc_id), recursion_depth))) {
        if (OB_HASH_NOT_EXIST == ret) {
          recursion_depth = 0;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to search recursion depth hash map", K(ret), K(proc_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (recursion_depth > max_recursion_depth) {
      if (is_function) {
        ret = OB_ER_SP_NO_RECURSION;
        LOG_USER_ERROR(OB_ER_SP_NO_RECURSION);
      } else {
        ret = OB_ER_SP_RECURSION_LIMIT;
        LOG_USER_ERROR(OB_ER_SP_RECURSION_LIMIT, max_recursion_depth);
      }
      LOG_WARN("too deep recursive", K(ret), K(proc_id),
               K(recursion_depth), K(max_recursion_depth), K(is_function));
    } else {
      recursion_depth++;
      if (NULL != depth_store) {
        *depth_store = recursion_depth;
      } else if (recursion_depth_array_.count() < RECURSION_ARRAY_SIZE) {
        if (OB_FAIL(recursion_depth_array_.push_back(
                    std::make_pair(std::make_pair(package_id, proc_id), recursion_depth)))) {
          LOG_WARN("array push back failed", K(ret));
        }
      } else {
        if (!recursion_depth_map_.created()) {
          // create hash map && copy all items form array to hash map.
          if (OB_FAIL(recursion_depth_map_.create(RECURSION_MAP_SIZE, ObModIds::OB_PL_TEMP))) {
            LOG_WARN("fail to init recursion depth map", K(ret));
          } else {
            FOREACH_CNT_X(it, recursion_depth_array_, OB_SUCC(ret)) {
              if (OB_FAIL(recursion_depth_map_.set_refactored(it->first, it->second))) {
                LOG_WARN("hash map set failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(recursion_depth_map_.set_refactored(std::make_pair(package_id, proc_id), recursion_depth, true))) {
            LOG_WARN("fail to inc recursion depth", K(ret), K(proc_id), K(recursion_depth));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLExecRecursionCtx::dec_and_check_depth(uint64_t package_id, uint64_t proc_id)
{
  int ret = OB_SUCCESS;
  if (!init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("recursion context not init", K(ret), K(init_));
  } else if (OB_LIKELY(!recursion_depth_map_.created())) {
    int64_t *depth = NULL;
    FOREACH_CNT(it, recursion_depth_array_) {
      if (it->first.first == package_id
          && it->first.second == proc_id) {
        depth = &it->second;
        break;
      }
    }
    if (NULL == depth || *depth <= 0 ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("proc not found or unexpected recursion depth", K(ret), K(depth));
    } else {
      *depth -= 1;
    }
  } else {
    int64_t recursion_depth = 0;
    if (OB_FAIL(recursion_depth_map_.get_refactored(std::make_pair(package_id, proc_id), recursion_depth))) {
      LOG_WARN("fail to search recursion depth hash map", K(ret), K(package_id), K(proc_id));
    } else if (recursion_depth <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("recursion depth is unexpected", K(ret), K(recursion_depth));
    } else if (OB_FAIL(recursion_depth_map_.set_refactored(std::make_pair(package_id, proc_id), --recursion_depth, true))) {
      LOG_WARN("fail to dec recursion depth", K(ret), K(package_id), K(proc_id), K(recursion_depth));
    }
  }
  return ret;
}

/* check用户是否有调用存储过程的权限 */
int ObPL::check_exec_priv(
    ObExecContext &exec_ctx,
    const ObString &database_name,
    ObPLFunction *routine)
{
  int ret = OB_SUCCESS;
  uint64_t pkg_id = OB_INVALID_ID;
  uint64_t func_id = OB_INVALID_ID;
  uint64_t db_id = OB_INVALID_ID;
  uint64_t tenant_id = OB_INVALID_ID;

  CK (OB_NOT_NULL(routine));
  OX (pkg_id = routine->get_package_id());
  OX (func_id = routine->get_routine_id());
  OX (db_id = routine->get_database_id());

  ObSchemaGetterGuard *guard = exec_ctx.get_sql_ctx()->schema_guard_;
  CK (OB_NOT_NULL(guard));
  OX (tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id());
  if (OB_SUCC(ret)
     && OB_INVALID_ID != pkg_id && !ObTriggerInfo::is_trigger_package_id(pkg_id)
     && OB_INVALID_ID != db_id && OB_INVALID_ID != func_id) {

    CK (exec_ctx.get_my_session() != NULL);

    if (OB_SYS_TENANT_ID == routine->get_tenant_id()) {
      bool need_check = false;
      bool need_only_obj_check = false;
      uint64_t spec_id = OB_INVALID_ID;
      OZ (sys_pkg_need_priv_check(pkg_id, guard, need_check, spec_id, need_only_obj_check),
                                  pkg_id, spec_id);
      if (need_check) {
        OZ (ObOraSysChecker::check_ora_obj_priv(*guard,
                          tenant_id,
                          exec_ctx.get_my_session()->get_user_id(),
                          database_name,
                          spec_id,
                          OBJ_LEVEL_FOR_TAB_PRIV,
                          need_only_obj_check ?
                                  static_cast<uint64_t>(ObObjectType::SYS_PACKAGE_ONLY_OBJ_PRIV)
                                : static_cast<uint64_t>(ObObjectType::SYS_PACKAGE),
                          OBJ_PRIV_ID_EXECUTE,
                          CHECK_FLAG_NORMAL,
                          OB_SYS_USER_ID,
                          exec_ctx.get_my_session()->get_enable_role_array()),
                          pkg_id, db_id, func_id, spec_id);
      }
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode() && pkg_id == OB_INVALID_ID) {
    if (ObSchemaChecker::enable_mysql_pl_priv_check(tenant_id, *guard)) {
      share::schema::ObSessionPrivInfo session_priv;
      if (OB_FAIL(guard->get_session_priv_info(
                                      exec_ctx.get_my_session()->get_priv_tenant_id(),
                                      exec_ctx.get_my_session()->get_priv_user_id(),
                                      exec_ctx.get_my_session()->get_database_name(),
                                      session_priv))) {
          LOG_WARN("fail to get_session_priv_info", K(ret));
      } else if (OB_FAIL(exec_ctx.get_my_session()->get_security_version(
                                                          session_priv.security_version_))) {
        LOG_WARN("fail to get security version", K(ret));
      } else if (OB_UNLIKELY(!session_priv.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Session priv is invalid", "tenant_id", session_priv.tenant_id_,
                  "user_id", session_priv.user_id_, K(ret));
      } else {
        ObNeedPriv need_priv;
        need_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
        need_priv.db_ = database_name;
        need_priv.table_ = routine->get_function_name();
        need_priv.priv_set_ = OB_PRIV_EXECUTE;
        const ObRoutineInfo *routine_info = NULL;
        OZ (guard->get_routine_info(tenant_id, routine->get_routine_id(), routine_info));
        CK (OB_NOT_NULL(routine_info));
        OX (need_priv.obj_type_ = routine_info->is_procedure() ? ObObjectType::PROCEDURE : ObObjectType::FUNCTION);
        OZ (guard->check_routine_priv(session_priv, need_priv));
      }
    }
  }
  return ret;
}

int ObPLSqlInfo::generate(
  const ObPLSqlStmt &sql_stmt, ObIArray<sql::ObSqlExpression *> &exprs)
{
  int ret = OB_SUCCESS;

  OX (loc_ = sql_stmt.get_location());

  // copy ObPLSQL
  OX (forall_sql_ = sql_stmt.is_forall_sql());
  OX (for_update_ = sql_stmt.is_for_update());
  OX (has_hidden_rowid_ = sql_stmt.has_hidden_rowid());
  CK (OB_NOT_NULL(allocator_));
  OZ (ob_write_string(*allocator_, sql_stmt.get_sql(), sql_, true));
  OX (params_.set_capacity(sql_stmt.get_params().count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_stmt.get_params().count(); ++i) {
    OZ (params_.push_back(exprs.at(sql_stmt.get_params().at(i))));
  }
  OX (array_binding_params_.set_capacity(sql_stmt.get_array_binding_params().count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_stmt.get_array_binding_params().count(); ++i) {
    OZ (array_binding_params_.push_back(exprs.at(sql_stmt.get_array_binding_params().at(i))));
  }

  OZ (ob_write_string(*allocator_, sql_stmt.get_ps_sql(), ps_sql_, true));
  OX (stmt_type_ = sql_stmt.get_stmt_type());
  OX (rowid_table_id_ = sql_stmt.get_rowid_table_id());

  // copy ObPLInto
  OX (into_.set_capacity(sql_stmt.get_into().count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_stmt.get_into().count(); ++i) {
    OZ (into_.push_back(exprs.at(sql_stmt.get_into().at(i))));
  }
  OZ (not_null_flags_.assign(sql_stmt.get_not_null_flags()));
  OZ (pl_integer_ranges_.assign(sql_stmt.get_pl_integer_ranges()));
  OZ (data_type_.assign(sql_stmt.get_data_type()));
  OX (bulk_ = sql_stmt.is_bulk());

  return ret;
}

int ObPL::simple_execute(ObPLExecCtx *ctx, int64_t argc, int64_t *argv)
{
  int ret = OB_SUCCESS;
  ObPLFunction *func = NULL;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(func = ctx->func_));
  CK (func->get_is_all_sql_stmt());
  if (OB_SUCC(ret)) {
    ObIArray<ObPLSqlInfo> &sql_infos = func->get_sql_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_infos.count(); ++i) {
      ObPLSqlInfo &sql_info = sql_infos.at(i);
      if (sql_info.params_.empty()) {
        OZ (ObSPIService::spi_query(
          ctx, sql_infos.at(i).sql_.ptr(), sql_infos.at(i).stmt_type_,
          sql_info.into_.get_data(), sql_info.into_.count(),
          sql_info.data_type_.get_data(), sql_info.data_type_.count(),
          sql_info.not_null_flags_.get_data(),
          sql_info.pl_integer_ranges_.get_data(),
          sql_info.bulk_));
      } else {
        OZ (ObSPIService::spi_execute(
          ctx, sql_info.ps_sql_.ptr(), sql_info.stmt_type_,
          sql_info.forall_sql_ ? sql_info.array_binding_params_.get_data() : sql_info.params_.get_data(),
          sql_info.forall_sql_ ? sql_info.array_binding_params_.count() : sql_info.params_.count(),
          sql_info.into_.get_data(), sql_info.into_.count(),
          sql_info.data_type_.get_data(), sql_info.data_type_.count(),
          sql_info.not_null_flags_.get_data(),
          sql_info.pl_integer_ranges_.get_data(),
          sql_info.bulk_,
          sql_info.forall_sql_));
      }
      if (OB_READ_NOTHING == ret && lib::is_mysql_mode()) {
        //ignore OB_READ_NOTHING error in mysql mode
        ret = OB_SUCCESS;
      }

 #ifdef OB_BUILD_ORACLE_PL
      ObPLEH::eh_adjust_call_stack(func, ctx->pl_ctx_, sql_infos.at(i).loc_, ret);
 #endif

    }
  }
  return ret;
}

int ObPLExecState::check_pl_execute_priv(ObSchemaGetterGuard &guard,
                                          const uint64_t tenant_id,
                                          const uint64_t user_id,
                                          const ObSchemaObjVersion &schema_obj,
                                          const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = 0;
  const ObUDTTypeInfo *udt_info = NULL;
  const ObRoutineInfo *routine_info = NULL;
  const ObPackageInfo *package_info = NULL;
  const ObUserInfo *user_info = NULL;
  uint64_t obj_tenant_id = tenant_id;
  const uint64_t fetch_tenant_id = get_tenant_id_by_object_id(schema_obj.get_object_id());
  ObSchemaType schema_type = schema_obj.get_schema_type();
  ObObjectType object_type = ObObjectType::INVALID;
  int64_t obj_id = schema_obj.get_object_id();

  if (UDT_SCHEMA == schema_type) {
    OZ (guard.get_udt_info(fetch_tenant_id, schema_obj.get_object_id(), udt_info));
    OX (object_type = ObObjectType::TYPE);
    if (OB_NOT_NULL(udt_info)) {
      OX (db_id = udt_info->get_database_id());
      OX (obj_tenant_id = udt_info->get_tenant_id());
    }
  } else if (ROUTINE_SCHEMA == schema_type) {
    OZ (guard.get_routine_info(fetch_tenant_id, schema_obj.get_object_id(), routine_info));
    OX (object_type =
         DEPENDENCY_PROCEDURE == schema_obj.object_type_ ?
         ObObjectType::PROCEDURE : ObObjectType::FUNCTION);
    if (OB_NOT_NULL(routine_info)) {
      OX (db_id = routine_info->get_database_id());
      OX (obj_tenant_id = routine_info->get_tenant_id());
      if (ROUTINE_UDT_TYPE == routine_info->get_routine_type()) {
        // for UDT routines, privilege of the UDT should be checked
        OZ (guard.get_udt_info(fetch_tenant_id, routine_info->get_package_id(), udt_info));
        if (OB_SUCC(ret) && OB_NOT_NULL(udt_info)) {
          object_type = ObObjectType::TYPE;
          obj_id = routine_info->get_package_id();
          db_id = udt_info->get_database_id();
          obj_tenant_id = udt_info->get_tenant_id();
        }
      } else if (ROUTINE_PACKAGE_TYPE == routine_info->get_routine_type()) {
        // for package routines, privilege of the package should be checked
        OZ (guard.get_package_info(fetch_tenant_id, routine_info->get_package_id(), package_info));
        if (OB_SUCC(ret) && OB_NOT_NULL(package_info)) {
          object_type = ObObjectType::PACKAGE;
          obj_id = routine_info->get_package_id();
          db_id = package_info->get_database_id();
          obj_tenant_id = package_info->get_tenant_id();
        }
      }
    }
  }

  if (OB_SUCC(ret) && (OB_NOT_NULL(udt_info) || OB_NOT_NULL(routine_info))) {
    const ObDatabaseSchema *db_schema = NULL;
    common::ObString database_name = OB_SYS_DATABASE_NAME;
    uint64_t obj_owner_id = OB_SYS_USER_ID;
    if (OB_SYS_TENANT_ID != obj_tenant_id) {
      OZ (guard.get_database_schema(tenant_id, db_id, db_schema));
      CK (db_schema != NULL);
      OX(database_name = db_schema->get_database_name());
      OZ (guard.get_user_info(tenant_id,
                              database_name,
                              OB_DEFAULT_HOST_NAME,
                              user_info));
      CK (user_info != NULL);
      OX (obj_owner_id = user_info->get_user_id());
      OZ (ObOraSysChecker::check_ora_obj_priv(guard,
                          obj_tenant_id,
                          user_id,
                          database_name,
                          obj_id,
                          OBJ_LEVEL_FOR_TAB_PRIV,
                          static_cast<uint64_t>(object_type),
                          OBJ_PRIV_ID_EXECUTE,
                          CHECK_FLAG_NORMAL,
                          obj_owner_id,
                          role_id_array),
                          K(obj_tenant_id), K(user_id), K(database_name), K(obj_id), K(obj_owner_id), K(role_id_array));
    }
    if (ROUTINE_SCHEMA == schema_type && ret == OB_TABLE_NOT_EXIST) {
      ret = OB_WRONG_COLUMN_NAME;
    }
  }
  return ret;
}

int ObPLExecState::check_pl_priv(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t user_id,
    const DependenyTableStore &dep_obj)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < dep_obj.count() && OB_SUCC(ret); i++) {
    const ObSchemaObjVersion &schema_obj = dep_obj.at(i);
    OZ(check_pl_execute_priv(guard, tenant_id, user_id, schema_obj,
                             ctx_.exec_ctx_->get_my_session()->get_enable_role_array()));
  }
    // check func self priv
  return ret;
}

int ObPLExecState::execute()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_ISNULL(reinterpret_cast<void*>(func_.get_action()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("action is NULL", K(ret));
  } else if (OB_ISNULL(ctx_.exec_ctx_) || OB_ISNULL(ctx_.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute context is null", K(ret));
  } else {
    int (*fp)(ObPLExecCtx*, int64_t, int64_t*) = (int(*)(ObPLExecCtx*, int64_t, int64_t*))(func_.get_action());
    int64_t *argv = NULL;
    if (ctx_.exec_ctx_ != NULL && ctx_.exec_ctx_->get_my_session() != NULL &&
        ctx_.exec_ctx_->get_sql_ctx() != NULL &&
        ctx_.exec_ctx_->get_sql_ctx()->schema_guard_ != NULL) {
      uint64_t user_id = ctx_.exec_ctx_->get_my_session()->get_priv_user_id();
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (check_pl_priv(*ctx_.exec_ctx_->get_sql_ctx()->schema_guard_,
                            ctx_.exec_ctx_->get_my_session()->get_effective_tenant_id(),
                            user_id,
                            func_.get_dependency_table()));
      }
    }
    if (OB_SUCC(ret) && func_.get_arg_count() > 0) {
      argv = static_cast<int64_t*>(get_allocator()->alloc(sizeof(int64_t) * func_.get_arg_count()));
      if (OB_ISNULL(argv)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(sizeof(int64_t) * func_.get_arg_count()), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < func_.get_arg_count(); ++i) {
          argv[i] = reinterpret_cast<int64_t>(&get_params().at(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (inner_call_) {
        _Unwind_Exception *eptr = nullptr;
        ret = SMART_CALL([&]() {
                           int ret = OB_SUCCESS;
                           try {
                             ret = fp(&ctx_, func_.get_arg_count(), argv);
                           } catch(...) {
                             eptr = tl_eptr;
                           }
                           return ret;
                         }());
        if (eptr != nullptr) {
          ret = OB_SUCCESS == ret ? (NULL != ctx_.status_ ? *ctx_.status_ : OB_ERR_UNEXPECTED)
              : ret;
          final(ret); // 避免当前执行的pl内数组内存泄漏, 捕获到异常后先执行final, 然后将异常继续向上抛
          _Unwind_RaiseException(eptr);
        }
      } else {
        bool has_exception = false;
        ret = SMART_CALL([&]() {
                           int ret = OB_SUCCESS;
                           try {
                             ret = fp(&ctx_, func_.get_arg_count(), argv);
                           } catch(...) {
                             has_exception = true;
                           }
                           return ret;
                         }());
        if (has_exception) {
          if (OB_ISNULL(ctx_.status_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("status is NULL", K(ret));
          } else {
            ret = OB_SUCCESS == ret ? *ctx_.status_ : ret;
            if (lib::is_oracle_mode()) {
              ret = ret < OB_SUCCESS && ret > -OB_MAX_ERROR_CODE ? ret : OB_ERR_SP_UNHANDLED_EXCEPTION;
            } else {
              ret = ret > 0 ? OB_SP_RAISE_APPLICATION_ERROR : ret;
            }
            LOG_WARN("Unhandled exception has occurred in PL", K(*ctx_.status_), K(ret));
            if (OB_ERR_SP_UNHANDLED_EXCEPTION == ret) {
              LOG_USER_ERROR(OB_ERR_SP_UNHANDLED_EXCEPTION);
            }
          }
        }
      }
      if (OB_ISNULL(ctx_.result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is NULL", K(ret));
      } else if (func_.is_function()
                 && OB_SUCC(ret)
                 && 0 == *ctx_.status_) {
        if (ctx_.result_->is_invalid_type()) {
          if (!func_.is_pipelined()) {
            ret = OB_ER_SP_NORETURNEND;
            LOG_WARN("FUNCTION ended without RETURN",K(func_), K(ret));
          }
        } else {
          // Check function return value not null violated!
          if (func_.get_ret_type().is_not_null()
              && (ctx_.result_->is_null() || ctx_.result_->is_null_oracle())) {
            ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
            LOG_WARN("not null check violated!",
                     K(ret), K(func_.get_ret_type()), KPC(ctx_.result_));
          }
        }
      } else { /*do nothing*/ }
    }

    if (top_call_
        && ctx_.exec_ctx_->get_my_session()->is_track_session_info()
        && ctx_.exec_ctx_->get_my_session()->is_package_state_changed()) {
      LOG_DEBUG("++++++++ add changed package info to session! +++++++++++");
      int tmp_ret = ctx_.exec_ctx_->get_my_session()->add_changed_package_info(*ctx_.exec_ctx_);
      if (tmp_ret != OB_SUCCESS) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("failed to add changed package info", K(ret));
      } else {
        ctx_.exec_ctx_->get_my_session()->reset_all_package_changed_info();
      }
    }

    if (OB_SUCC(ret)) {
      ObSQLSessionInfo *session_info = ctx_.exec_ctx_->get_my_session();
#ifdef OB_BUILD_ORACLE_PL
      int64_t call_cnt = top_context_->get_call_stack().count();
      int64_t err_cnt = top_context_->get_error_trace().count();
      if (OB_SUCC(ret) && err_cnt > 0) {
        DbmsUtilityHelper::BtInfo* top_error = top_context_->get_error_trace().at(err_cnt - 1);
        if (func_.get_action() == top_error->handler) {
          OX (top_context_->get_error_trace().pop_back());
        }
      }
      if (OB_SUCC(ret) && call_cnt > 0) {
        DbmsUtilityHelper::BtInfo* top_call = top_context_->get_call_stack().at(call_cnt - 1);
        if (func_.get_action() == top_call->handler) {
          OX (top_context_->get_call_stack().pop_back());
        }
      }
    } else if (!inner_call_) {
      ObPLContext::get_exact_error_msg(top_context_->get_error_trace(),
                                       top_context_->get_call_stack(),
                                       ctx_.exec_ctx_->get_my_session()->get_pl_exact_err_msg());
#endif
    }
  }
  return ret;
}

ObPLCompileUnit::~ObPLCompileUnit()
{
  for (int64_t i = 0; i < routine_table_.count(); ++i) {
    if (OB_NOT_NULL(routine_table_.at(i))) {
      routine_table_.at(i)->~ObPLFunction();
    }
  }
}

int ObPLCompileUnit::add_routine(ObPLFunction *routine)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(routine_table_.push_back(routine))) {
    LOG_WARN("routine push back failed", K(ret));
  }
  return ret;
}

int ObPLCompileUnit::get_routine(int64_t routine_idx, ObPLFunction *&routine) const
{
  int ret = OB_SUCCESS;
  routine = NULL;
  if (routine_idx < 0 || routine_idx >= routine_table_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine index invalid", K(routine_idx), K(ret));
  } else {
    routine = routine_table_.at(routine_idx);
  }
  return ret;
}

void ObPLCompileUnit::reset()
{
  ObPLCacheObject::reset();
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  dependency_tables_.reset();
}


int ObPLCompileUnit::check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add)
{
  int ret = OB_SUCCESS;

  ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  need_real_add = pc_ctx.need_add_obj_stat_;

  return ret;
}

void ObPLCompileUnit::dump_deleted_log_info(const bool is_debug_log /* = true */) const
{
  ObString raw_sql;
  if (is_anon()) {
    const pl::ObPLFunction *pl_func = dynamic_cast<const pl::ObPLFunction *>(this);
    if (OB_ISNULL(pl_func)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "the plan is null", K(object_id_),
                                    K(tenant_id_),
                                    K(added_to_lc_),
                                    K(get_ref_count()),
                                    K(log_del_time_),
                                    K(routine_table_),
                                    K(expr_op_size_),
                                    K(can_cached_));
    } else {
      raw_sql = ObTruncatedString(pl_func->get_stat().raw_sql_, OB_MAX_SQL_LENGTH).string();
    }
  } else {
    // do nothing
  }
  if (is_debug_log) {
    LOG_DEBUG("Dumping Cache Deleted Info",
               K(object_id_),
               K(tenant_id_),
               K(added_to_lc_),
               K(ns_),
               K(get_ref_count()),
               K(log_del_time_),
               K(raw_sql),
               K(routine_table_),
               K(expr_op_size_),
               K(can_cached_));
  } else {
    LOG_INFO("Dumping Cache Deleted Info",
               K(object_id_),
               K(tenant_id_),
               K(added_to_lc_),
               K(ns_),
               K(get_ref_count()),
               K(log_del_time_),
               K(raw_sql),
               K(routine_table_),
               K(expr_op_size_),
               K(can_cached_));
  }
}

ObPLCompileUnit::ObPLCompileUnit(sql::ObLibCacheNameSpace ns,
                                 lib::MemoryContext &mem_context)
    : ObPLCacheObject(ns, mem_context), routine_table_(allocator_),
      type_table_(), helper_(allocator_), di_helper_(allocator_),
      can_cached_(true),
      exec_env_(),
      profiler_unit_info_(std::make_pair(OB_INVALID_ID, INVALID_PROC_TYPE))
{
#ifndef USE_MCJIT
  int ret = OB_SUCCESS;
  if (OB_FAIL(helper_.init())) {
    LOG_WARN("failed to init llvm helper", K(ret), K(helper_.get_jc()));
  }
#endif // USE_MCJIT
}

ObPLFunction::~ObPLFunction()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_buf_)) {
    allocator_.free(di_buf_);
    di_buf_ = NULL;
  }
}


int ObPLFunction::set_variables(const ObPLSymbolTable &symbol_table)
{
  int ret = OB_SUCCESS;
  variables_.set_capacity(static_cast<uint32_t>(symbol_table.get_count()));
  default_idxs_.set_capacity(static_cast<uint32_t>(symbol_table.get_count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < symbol_table.get_count(); ++i) {
    ObPLDataType type;
    if (OB_ISNULL(symbol_table.get_symbol(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("symbol var is NULL", K(i), K(symbol_table.get_symbol(i)), K(ret));
    } else if (OB_FAIL(type.deep_copy(allocator_, symbol_table.get_symbol(i)->get_type()))) {
      LOG_WARN("fail to deep copy pl data type", K(symbol_table.get_symbol(i)->get_type()), K(ret));
    } else {
      if (type.get_meta_type() != NULL && type.get_meta_type()->is_lob_storage()) {
        type.get_data_type()->meta_.set_has_lob_header();
      }
      if (OB_FAIL(variables_.push_back(type))) {
        LOG_WARN("push back error", K(i), K(type), K(symbol_table.get_symbol(i)), K(variables_), K(ret));
      } else if (OB_FAIL(default_idxs_.push_back(symbol_table.get_symbol(i)->get_default()))) {
        LOG_WARN("push back error", K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObPLFunction::set_variables_debuginfo(const ObPLSymbolDebugInfoTable &symbol_debug_info_table)
{
  int ret = OB_SUCCESS;
  variables_debuginfo_.set_capacity(static_cast<uint32_t>(symbol_debug_info_table.get_count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < symbol_debug_info_table.get_count(); ++i) {
    ObPLVarDebugInfo* var_debuginfo = NULL;
    if (OB_ISNULL(var_debuginfo
      = reinterpret_cast<ObPLVarDebugInfo*>(allocator_.alloc(sizeof(ObPLVarDebugInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for ObPLVarDebugInfo!", K(ret));
    }
    OX (var_debuginfo = new(var_debuginfo)ObPLVarDebugInfo());
    CK (OB_NOT_NULL(symbol_debug_info_table.get_symbol(i)));
    OZ (var_debuginfo->deep_copy(allocator_, *symbol_debug_info_table.get_symbol(i)));
    OZ (variables_debuginfo_.push_back(var_debuginfo));
  }
  LOG_INFO("set variable debuginfo", K(ret), K(variables_debuginfo_), K(symbol_debug_info_table));
  return ret;
}

int ObPLFunction::set_name_debuginfo(const ObPLFunctionAST &ast)
{
  int ret = OB_SUCCESS;
  OZ (ob_write_string(allocator_, ast.get_db_name(), name_debuginfo_.owner_name_));
  OZ (ob_write_string(allocator_, ast.get_package_name(), name_debuginfo_.package_name_));
  OZ (ob_write_string(allocator_, ast.get_name(), name_debuginfo_.routine_name_));
  return ret;
}

int ObPLFunction::set_types(const ObPLUserTypeTable &type_table)
{
  int ret = OB_SUCCESS;
  ret = ObPLCompiler::compile_type_table(type_table, *this);
  return ret;
}

int ObPLFunction::get_subprogram(const ObIArray<int64_t> &path, ObPLFunction *&routine) const
{
  int ret = OB_SUCCESS;
  routine = NULL;
  ObPLFunction *parent = const_cast<ObPLFunction*>(this);
  for (int64_t i = 0; OB_SUCC(ret) && i < path.count(); ++i) {
    if (OB_ISNULL(parent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("routine is NULL", K(i), K(routine_table_), K(ret));
    } else if (OB_FAIL(parent->get_routine(path.at(i), parent))) {
      LOG_WARN("failed to get routine", K(i), K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    routine = parent;
  }
  return ret;
}

bool ObPLFunction::should_init_as_session_cursor()
{
  bool b_ret = false;
  /* three case:
   * 1. this is a function and return type is sys_refcursor.
   * 2. this is a routine and has out sys_refcursor param
   * 3. a subprogram can direct access parent var. if a cursor var can't find in local variables,
   * we have to alloc the cursors on session memory
   * for example:
   * CREATE OR REPLACE FUNCTION fun4(cur out sys_refcursor)
     return sys_refcursor
     as
     kk sys_refcursor;
     begin
        open kk for select * from table where 1=2;
        cur := kk;
       return kk;
    end;
   *
   * #subprogram
   * create or replace procedure(a number) is
   * cur sys_refcursor
   * procedure subp is
   * cur1 sys_refcursor
   * begin
   *   open cur1 for select a from tbl;
   *   cur = cur1;
   * end;
   * begin
   * close cur;
   * end;
   *
   * kk is a local cursor, which mean it will be freed after the function is returned.
   * but the return value and the out param requires that this cursor should be used out
   * of this function in somewhere. so instead of alloc local cursorinfo, we should
   * alloc a session cursor.
   *
   * TODO: we just simply replace all local cursors inside the function with session cursors.
   * actually, not all local cursors have to be session cursors except it is returned or assigned
   * to a out cursor param. to achieve this, we have to recoginze it while resolving the stmt.
   * but we cann't handle some case which has branch stmt such as:
   * if xxx then
   *  cur := local_cursor1;
   * else
   *  cur := local_cursor2;
   * end if;
   * in this case, we have to change those two local cursor into two session cursor
  */

  if (is_function() && get_ret_type().is_ref_cursor_type()) {
    b_ret = true;
  } else if(has_open_external_ref_cursor()) {
    b_ret = true;
  } else {
    for (int64_t i = 0; i < get_variables().count(); ++i) {
      if (get_out_args().has_member(i) && get_variables().at(i).is_ref_cursor_type()) {
        b_ret = true;
      }
    }
  }
  LOG_DEBUG("check external session cursor", K(b_ret));

  return b_ret;
}

int ObPLFunction::is_special_pkg_invoke_right(ObSchemaGetterGuard &guard, bool &flag)
{
  typedef const char *(*name_pair_ptr)[2];
  static const char *name_pair[] = { "dbms_utility", "name_resolve" };
  static const char *name_pair1[] = { "dbms_utility", "ICD_NAME_RES" };
  static const char *name_pair2[] = { "dbms_utility", "old_current_schema" };
  static const char *name_pair3[] = { "dbms_utility", "exec_ddl_statement" };
  static const char *name_pair4[] = { "dbms_describe", "describe_procedure" };
  static name_pair_ptr name_arr[] = {
    &name_pair,
    &name_pair1,
    &name_pair2,
    &name_pair3,
    &name_pair4
    // { "dbms_utility", "name_resolve" }
  };
  int ret = OB_SUCCESS;
  uint64_t pkg_id = get_package_id();
  uint64_t db_id = get_database_id();
  uint64_t func_id = get_routine_id();
  if (OB_SYS_TENANT_ID == get_tenant_id()
     && OB_INVALID_ID != pkg_id
     && !ObTriggerInfo::is_trigger_package_id(pkg_id)
     && OB_INVALID_ID != func_id) {
    const ObSimplePackageSchema *pkg_schema = NULL;
    if (OB_FAIL(guard.get_simple_package_info(get_tenant_id(), pkg_id, pkg_schema))) {
      LOG_WARN("failed to get pkg schema", K(ret), K(get_tenant_id()), K(pkg_id));
    } else if (OB_ISNULL(pkg_schema)) {
      // TODO: udt routine may through here, must not be dbms_utility, go through.
    } else {
      for (int i = 0; OB_SUCC(ret) && i < sizeof(name_arr) / sizeof(name_pair_ptr); ++i) {
        name_pair_ptr np = name_arr[i];
        if (ObCharset::case_insensitive_equal(pkg_schema->get_package_name(), ObString((*np)[0]))
        && ObCharset::case_insensitive_equal(get_function_name(), ObString((*np)[1]))) {
          flag = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObPLINS::calc_expr(uint64_t package_id, int64_t expr_idx, ObObjParam &result)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_WARN(OB_NOT_SUPPORTED, "call expr on base class ObIPLNS");
  return ret;
}

int ObPLINS::init_complex_obj(ObIAllocator &allocator,
                              const ObPLDataType &pl_type,
                              common::ObObjParam &obj,
                              bool set_allocator,
                              bool set_null)
{
  int ret = OB_SUCCESS;
  int64_t init_size = 0;
  void *ptr = NULL;
  CK (pl_type.is_composite_type());
  OZ (get_size(PL_TYPE_INIT_SIZE, pl_type, init_size, &allocator));
  // 如果原来已经有值，则不重新分配, 直接在此基础上修改
  if (obj.is_ext() && obj.get_ext() != 0) {
    ObObj out_param = obj;
    OZ (ObUserDefinedType::destruct_obj(out_param));
    CK (OB_NOT_NULL(ptr = reinterpret_cast<void*>(obj.get_ext())));
  } else { // 如果原来没有值, 重新分配内存, PS协议的情况, 前端发过来的纯OUT参数是NULL
    if (OB_SUCC(ret) && OB_ISNULL(ptr = allocator.alloc(init_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(init_size));
    }
    OX (MEMSET(ptr, 0, init_size));
  }

  const ObUserDefinedType *user_type = NULL;
  OZ (get_user_type(pl_type.get_user_type_id(), user_type, &allocator));
  CK (OB_NOT_NULL(user_type));

  if (OB_SUCC(ret) && user_type->is_record_type()) {
    ObPLRecord *record = NULL;
    ObObj *member = NULL;
    const ObRecordType* record_type = static_cast<const ObRecordType*>(user_type);
    OX (new(ptr)ObPLRecord(user_type->get_user_type_id(), record_type->get_member_count()));
    OX (record = reinterpret_cast<ObPLRecord*>(ptr));
    for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_member_count(); ++i) {
      CK (OB_NOT_NULL(record_type->get_member(i)));
      CK (OB_NOT_NULL(record_type->get_record_member(i)));
      OZ (record->get_element(i, member));
      CK (OB_NOT_NULL(member));
      OX (new (member) ObObj(ObNullType));
      if (OB_FAIL(ret)) {
      } else if (record_type->get_record_member(i)->get_default() != OB_INVALID_INDEX) {
        ObObjParam default_v;
        if (record_type->is_package_type()) {
          OZ (calc_expr(extract_package_id(pl_type.get_user_type_id()),
                        record_type->get_record_member(i)->get_default(),
                        default_v));
        } else {
          OZ (calc_expr(OB_INVALID_ID,
                        record_type->get_record_member(i)->get_default(),
                        default_v));
        }
        if (OB_FAIL(ret)) {
        } else if (record_type->get_member(i)->is_obj_type()) {
          OZ (deep_copy_obj(allocator, default_v, *member));
        } else {
          OZ (ObUserDefinedType::deep_copy_obj(allocator, default_v, *member));
        }
      } else {
        if (!record_type->get_member(i)->is_obj_type()) {
          int64_t init_size = OB_INVALID_SIZE;
          int64_t member_ptr = 0;
          OZ (record_type->get_member(i)->get_size(PL_TYPE_INIT_SIZE, init_size));
          OZ (record_type->get_member(i)->newx(allocator, this, member_ptr));
          OX (member->set_extend(member_ptr, record_type->get_member(i)->get_type(), init_size));
        }
      }
    }
    // f(self object_type, p1 out object_type), p1 will be init here, we have to set it null
    // but self can't be set to null.
    if (OB_SUCC(ret) && user_type->is_object_type() && set_null) {
      OX (record->set_is_null(true));
    }
  }

#ifdef OB_BUILD_ORACLE_PL
  if (OB_SUCC(ret) && user_type->is_opaque_type()) {
    const ObOpaqueType *opaque_type = static_cast<const ObOpaqueType*>(user_type);
    CK (OB_NOT_NULL(opaque_type));
    OX (new(ptr)ObPLOpaque());
  }

  if (OB_SUCC(ret) && user_type->is_nested_table_type()) {
    const ObNestedTableType* nested_type = static_cast<const ObNestedTableType*>(user_type);
    CK (OB_NOT_NULL(nested_type));
    OX (new(ptr)ObPLNestedTable(user_type->get_user_type_id()));
  }
  if (OB_SUCC(ret) && user_type->is_varray_type()) {
    const ObVArrayType* varray_type = static_cast<const ObVArrayType*>(user_type);
    ObPLVArray *varray = reinterpret_cast<ObPLVArray*>(ptr);
    CK (OB_NOT_NULL(varray_type));
    CK (OB_NOT_NULL(varray));
    OX (new(varray)ObPLVArray(varray_type->get_user_type_id()));
    OX (varray->set_capacity(varray_type->get_capacity()));
  }
  if (OB_SUCC(ret) && user_type->is_associative_array_type()) {
    const ObAssocArrayType *assoc_type = static_cast<const ObAssocArrayType*>(user_type);
    CK (OB_NOT_NULL(assoc_type));
    CK (OB_NOT_NULL(ptr));
    OX (new(ptr)ObPLAssocArray(assoc_type->get_user_type_id()));
  }
  if (OB_SUCC(ret) && user_type->is_collection_type()) {
    int64_t field_cnt = OB_INVALID_COUNT;
    ObPLCollection *coll = reinterpret_cast<ObPLCollection *>(ptr);
    ObElemDesc elem_desc;
    bool not_null;
    const ObCollectionType *coll_type = static_cast<const ObCollectionType*>(user_type);
    CK (OB_NOT_NULL(coll));
    OX (set_allocator ? coll->set_allocator(&allocator) : coll->set_allocator(NULL));
    if (OB_FAIL(ret)) {
    } else if (user_type->is_associative_array_type()) {
      OX (coll->set_inited());
    } else {
      OX ((obj.is_ext() && obj.get_ext() != 0) ? (void)NULL : (set_null ? (void)NULL : coll->set_inited()));
    }
    OX (coll->set_type(pl_type.get_type()));
    OZ (get_element_data_type(pl_type, elem_desc, &allocator));
    OZ (get_not_null(pl_type, not_null, &allocator));
    OZ (coll_type->get_element_type().get_field_count(*this, field_cnt));
    OX (elem_desc.set_pl_type(coll_type->get_element_type().get_type()));
    OX (elem_desc.set_field_count(field_cnt));
    OX (elem_desc.set_not_null(not_null));
    OX (elem_desc.set_udt_id(coll_type->get_element_type().get_user_type_id()));
    OX (coll->set_element_desc(elem_desc));
  }
#endif
  OX (obj.set_extend(reinterpret_cast<int64_t>(ptr), user_type->get_type(), init_size));
  OX (obj.set_param_meta());
  OX (obj.set_udt_id(pl_type.get_user_type_id()));
  return ret;
}

int ObPLINS::get_size(ObPLTypeSize type,
                      const ObPLDataType& pl_type,
                      int64_t &size,
                      ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
  const ObUserDefinedType *user_type = NULL;
  CK (pl_type.is_composite_type());
  OZ (get_user_type(pl_type.get_user_type_id(), user_type, allocator));
  OV (OB_NOT_NULL(user_type), OB_ERR_UNEXPECTED, K(pl_type));
  OZ (user_type->get_size(type, size));
  return ret;
}

int ObPLINS::get_element_data_type(const ObPLDataType &pl_type,
                                   ObDataType &elem_type,
                                   ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(pl_type, elem_type, allocator);
#else
  const ObUserDefinedType *user_type = NULL;
  const ObCollectionType *collection_type = NULL;
  CK (pl_type.is_composite_type());
  CK (pl_type.is_collection_type());
  OZ (get_user_type(pl_type.get_user_type_id(), user_type, allocator));
  CK (OB_NOT_NULL(user_type));
  CK (user_type->is_collection_type());
  CK (OB_NOT_NULL(collection_type = static_cast<const ObCollectionType*>(user_type)));
  if (OB_SUCC(ret)) {
    if (collection_type->get_element_type().is_obj_type()) {
      CK (OB_NOT_NULL(collection_type->get_element_type().get_data_type()));
      if (OB_SUCC(ret)) {
        elem_type = *(collection_type->get_element_type().get_data_type());
      }
    } else {
      elem_type.set_obj_type(common::ObExtendType);
    }
  }
#endif
  return ret;
}

int ObPLINS::get_not_null(const ObPLDataType &pl_type,
                          bool &not_null,
                          ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(pl_type, not_null, allocator);
#else
  const ObUserDefinedType *user_type = NULL;
  const ObCollectionType *collection_type = NULL;
  CK (pl_type.is_composite_type());
  CK (pl_type.is_collection_type());
  OZ (get_user_type(pl_type.get_user_type_id(), user_type, allocator));
  CK (OB_NOT_NULL(user_type));
  CK (user_type->is_collection_type());
  CK (OB_NOT_NULL(collection_type = static_cast<const ObCollectionType*>(user_type)));
  if (OB_SUCC(ret)) {
    not_null = collection_type->get_element_type().get_not_null();
  }
#endif
  return ret;
}

int ObPL::check_session_alive(const ObBasicSessionInfo &session) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(session.is_query_killed())) {
    ret = OB_ERR_QUERY_INTERRUPTED;
    LOG_WARN("query is killed", K(ret));
  } else if (OB_UNLIKELY(session.is_zombie())) {
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session is killed", K(ret));
  }
  return ret;
}

int ObPLFunction::gen_action_from_precompiled(const ObString &name, size_t length,
                                       const char *ptr) {
  int ret = OB_SUCCESS;
  OZ (helper_.add_compiled_object(length, ptr));
  OX (set_action(helper_.get_function_address(name)));
  return ret;
}

} // namespace pl
}
