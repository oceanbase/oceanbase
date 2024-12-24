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

#define USING_LOG_PREFIX SQL
#include "ob_spi.h"
#include "ob_sql.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "ob_sql_utils.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_result.h"
#include "observer/ob_server_struct.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/obmp_stmt_execute.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_pl_integer_checker.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_allocator.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/ob_result_set.h"
#include "sql/parser/parse_node.h"
#include "pl/ob_pl_exception_handling.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "storage/tx/ob_trans_service.h"
#include "pl/sys_package/ob_dbms_sql.h"
#include "pl/ob_pl.h"
#include "sql/dblink/ob_tm_service.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/dblink/ob_pl_dblink_util.h"
#include "pl/ob_pl_profiler.h"
#include "pl/ob_pl_call_stack_trace.h"
#endif
#include "pl/ob_pl_allocator.h"
#include "pl/diagnosis/ob_pl_sql_audit_guard.h"
namespace oceanbase
{
using namespace sqlclient;
using namespace common;
using namespace share;
using namespace observer;
using namespace pl;
using namespace share::schema;
namespace sql
{
#define SET_SPI_STATUS \
  do { \
    if (OB_ISNULL(ctx->status_)) { \
      ret = OB_SUCCESS == ret ? OB_INVALID_ARGUMENT : ret; \
      LOG_WARN("status in is NULL", K(ctx->status_), K(ret)); \
    } else { \
      *ctx->status_ = ret; \
    } \
  } while (0)

#define SET_FORALL_BULK_EXCEPTION \
  do { \
    if (OB_FAIL(ret)) { \
      ObPLCursorInfo* cursor = NULL; \
      if (OB_ISNULL(ctx) \
          || OB_ISNULL(ctx->exec_ctx_) \
          || OB_ISNULL(ctx->exec_ctx_->get_my_session()) \
          || OB_ISNULL(cursor = ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor())) { \
        /*don`t change error code, so do nothing*/ \
      } else if (OB_BATCHED_MULTI_STMT_ROLLBACK == ret) { \
        /*forall optimal rollback, forall will rollback to single sql mode*/ \
        cursor->set_forall_rollback(); \
      } else if (!cursor->get_in_forall() \
                 || !cursor->get_save_exception() \
                 /*|| !cursor->is_forall_rollback()*/) { \
        /*not in forall or in forall rollback, do nothing*/ \
        /*we just handle bulk exception in single sql mode*/ \
      } else if (OB_SUCCESS != cursor->set_bulk_exception(ret)) { \
        LOG_WARN("failed to set bulk exception"); \
      } else { \
        ret = OB_SUCCESS; \
      } \
    } \
  } while (0)

#define MAKE_EXPR_BUFFER(allocator, expr_idx, expr_count, result)              \
  do {                                                                         \
    CK (OB_NOT_NULL(ctx));                                                     \
    CK (OB_NOT_NULL(ctx->func_));                                              \
    if (OB_SUCC(ret) && expr_idx != nullptr && expr_count > 0) {               \
      int64_t alloc_size = expr_count * sizeof(decltype(*result));             \
      result = static_cast<decltype(result)>(allocator.alloc(alloc_size));     \
      if (OB_ISNULL(result)) {                                                 \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                       \
        LOG_WARN("failed to alloc memory for sql expr buffer", K(expr_count)); \
      } else {                                                                 \
        MEMSET(result, 0, alloc_size);                                         \
        for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; ++i) {             \
          if (OB_UNLIKELY(expr_idx[i] == OB_INVALID_ID)) {                     \
            ret = OB_ERR_UNEXPECTED;                                           \
            LOG_WARN("invalid expr idx", K(expr_idx), K(expr_count), K(i));    \
          } else {                                                             \
            CK (OB_LIKELY(0 <= expr_idx[i]                                     \
                 && expr_idx[i] < ctx->func_->get_expressions().count()));     \
            OX (result[i] = ctx->func_->get_expressions().at(expr_idx[i]);)    \
          }                                                                    \
        }                                                                      \
      }                                                                        \
    }                                                                          \
  } while (0)

#define GET_NULLABLE_EXPR_BY_IDX(ctx, expr_idx, result)                        \
  do {                                                                         \
    CK (OB_NOT_NULL(ctx));                                                     \
    CK (OB_NOT_NULL(ctx->func_));                                              \
    if (OB_FAIL(ret)) {                                                        \
    } else if (expr_idx == OB_INVALID_ID) {                                    \
      result = nullptr;                                                        \
    } else {                                                                   \
      CK (OB_LIKELY(0 <= expr_idx &&                                           \
                   expr_idx < ctx->func_->get_expressions().count()));         \
      OX (result = ctx->func_->get_expressions().at(expr_idx);)                \
    }                                                                          \
  } while (0)

int ObSPIService::PLPrepareResult::init(sql::ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(session_info.get_effective_tenant_id(),
                    ObModIds::OB_PL_TEMP,
                    ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create memory entity failed", K(ret));
  } else {
    result_set_ = new (buf_) ObResultSet(session_info, mem_context_->get_arena_allocator());
  }
  return ret;
}

int ObSPIResultSet::init(sql::ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(session_info.get_effective_tenant_id(),
                    ObModIds::OB_RESULT_SET,
                    ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create memory entity failed", K(ret));
  } else {
    result_set_ = new (buf_) ObResultSet(session_info, mem_context_->get_arena_allocator());
    is_inited_ = true;
    result_set_->get_exec_context().get_task_exec_ctx().set_min_cluster_version(session_info.get_exec_min_cluster_version());
  }
  return ret;
}

int ObSPIResultSet::close_result_set()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    WITH_CONTEXT(mem_context_) {
      ret = result_set_->close();
    }
  } else {
    LOG_INFO("result set is not init", K(ret));
  }
  return ret;
}

int ObSPIResultSet::destruct_exec_params(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (NULL != get_result_set()
      && NULL != get_result_set()->get_exec_context().get_physical_plan_ctx()) {
    ParamStore &params = get_result_set()->get_exec_context().get_physical_plan_ctx()->get_param_store_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      OZ (ObUserDefinedType::destruct_obj(params.at(i), &session));
    }
  }
  return ret;
}

int ObSPIResultSet::store_session(ObSQLSessionInfo *session,
                    sql::ObSQLSessionInfo::StmtSavedValue *&value,
                    int64_t &nested_count)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session));
  OZ (alloc_saved_value(value));
  CK (OB_NOT_NULL(value));
  OZ (session->save_session(*value));
  OX (nested_count = session->get_nested_count());
  OX (session->set_query_start_time(ObTimeUtility::current_time()));
  return ret;
}

int ObSPIResultSet::restore_session(ObSQLSessionInfo *session,
                      sql::ObSQLSessionInfo::StmtSavedValue *value,
                      int64_t nested_count)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(value));
  OZ (session->restore_session(*value));
  OX (session->set_nested_count(nested_count));
  return ret;
}

int ObSPIResultSet::store_orign_session(ObSQLSessionInfo *session)
{
  return store_session(session, orign_session_value_, orign_nested_count_);
}

int ObSPIResultSet::store_cursor_session(ObSQLSessionInfo *session)
{
  return store_session(session, cursor_session_value_, cursor_nested_count_);
}

int ObSPIResultSet::restore_orign_session(ObSQLSessionInfo *session)
{
  return ObSPIResultSet::restore_session(session, orign_session_value_, orign_nested_count_);
}

int ObSPIResultSet::restore_cursor_session(ObSQLSessionInfo *session)
{
  return restore_session(session, cursor_session_value_, cursor_nested_count_);
}

int ObSPIResultSet::begin_nested_session(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  OZ (alloc_saved_value(nested_session_value_));
  OV (OB_NOT_NULL(nested_session_value_));
  OZ (session.begin_nested_session(*nested_session_value_));
  return ret;
}

int ObSPIResultSet::end_nested_session(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(nested_session_value_));
  OZ (session.end_nested_session(*nested_session_value_));
  return ret;
}

int ObSPIResultSet::alloc_saved_value(sql::ObSQLSessionInfo::StmtSavedValue *&session_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_value)) {
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator_.alloc(sizeof(sql::ObSQLSessionInfo::StmtSavedValue)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for saved session value", K(ret));
    }
    CK (OB_NOT_NULL(session_value = new(ptr)sql::ObSQLSessionInfo::StmtSavedValue()));
  }
  return ret;
}

int ObSPIResultSet::is_set_global_var(ObSQLSessionInfo &session, const ObString &sql, bool &has_global_variable)
{
  int ret = OB_SUCCESS;
  has_global_variable = false;
  ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_SET_VAR), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ParseResult parse_result;
  ParseMode parse_mode = STD_MODE;
  ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
  if (sql.empty()) {
  } else if (OB_FAIL(parser.parse(sql,
                            parse_result,
                            parse_mode,
                            false/*is_batched_multi_stmt_split_on*/,
                            false/*no_throw_parser_error*/,
                            true))) {
    LOG_WARN("generate syntax tree failed", K(sql), K(ret));
  } else if (OB_NOT_NULL(parse_result.result_tree_) &&
              parse_result.result_tree_->num_child_ > 0 &&
              OB_NOT_NULL(parse_result.result_tree_->children_[0])) {
    ParseNode *set_node = NULL;
    ParseNode *parse_tree = parse_result.result_tree_->children_[0];
    for (int64_t i = 0; OB_SUCC(ret) && !has_global_variable && i < parse_tree->num_child_; ++i) {
      if (OB_ISNULL(set_node = parse_tree->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("set node is NULL", K(ret));
      } else if (T_VAR_VAL == set_node->type_ &&
                 1 == set_node->value_) { // global var
        has_global_variable = true;
      } else if (set_node->num_child_ > 0 && OB_NOT_NULL(set_node->children_[0])) {
        ParseNode *var = set_node->children_[0];
        ObString name;
        if (T_OBJ_ACCESS_REF == var->type_) { //Oracle mode
          const ParseNode *name_node = NULL;
          if (OB_ISNULL(name_node = var->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else {
            name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
          }
        } else if (T_SYSTEM_VARIABLE == var->type_) {
          name.assign_ptr(var->str_value_, static_cast<int32_t>(var->str_len_));
        }
        if (OB_SUCC(ret) && 0 == name.case_compare("ob_query_timeout")) {
          has_global_variable = true;
        }
      }
    }
  }
  return ret;
}

int ObSPIResultSet::check_nested_stmt_legal(ObExecContext &exec_ctx, const ObString &sql, stmt::StmtType stmt_type, bool for_update)
{
  int ret = OB_SUCCESS;
  bool has_global_variable = false;
  stmt::StmtType parent_stmt_type = stmt::T_NONE;
  ObSqlCtx *sql_ctx = exec_ctx.get_sql_ctx();
  ObPLContext *pl_ctx = exec_ctx.get_pl_stack_ctx();
  if (OB_ISNULL(sql_ctx) || OB_ISNULL(pl_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_ctx or pl_ctx", K(ret), K(sql_ctx), K(pl_ctx));
  } else {
    parent_stmt_type = sql_ctx->stmt_type_;
    LOG_DEBUG("check nested stmt legal", K(parent_stmt_type), K(pl_ctx->in_autonomous()), K(stmt_type));
  }
  if (stmt::T_VARIABLE_SET == stmt_type && OB_NOT_NULL(exec_ctx.get_my_session())) {
    OZ (is_set_global_var(*exec_ctx.get_my_session(), sql, has_global_variable));
  }
  if (OB_SUCC(ret) && !pl_ctx->in_autonomous() && ObStmt::is_dml_stmt(parent_stmt_type)) {
    //only when parent stmt is dml or select, it can trigger a nested sql
    if (parent_stmt_type == stmt::T_SELECT && ObStmt::is_dml_write_stmt(stmt_type)) {
      /**
       * CREATE FUNCTION func() RETURNS VARCHAR(128)
       *   BEGIN
       *     INSERT INTO t1 VALUES (4);
       *     return '隐式提交';
       * END
       *
       * select func() from dual;
       * this function is allowed in mysql, but not allowed in oracle
       */
      if (!(lib::is_mysql_mode())) {
        ret = OB_ERR_CANNOT_PERFORM_DML_INSIDE_QUERY;
        LOG_WARN("OBE-14551: cannot perform a DML operation inside a query",
                 K(ret), K(stmt_type), K(exec_ctx.get_sql_ctx()),
                 K(&exec_ctx), K(exec_ctx.get_my_session()->get_cur_exec_ctx()));
      }
    } else if (stmt::T_SELECT == parent_stmt_type && stmt::T_SELECT == stmt_type && for_update && lib::is_oracle_mode()) {
      ret = OB_ERR_CANNOT_PERFORM_DML_INSIDE_QUERY;
      LOG_WARN("OBE-14551: cannot perform a DML operation inside a query",
                 K(ret), K(stmt_type), K(exec_ctx.get_sql_ctx()),
                 K(&exec_ctx), K(exec_ctx.get_my_session()->get_cur_exec_ctx()));
    } else if (ObStmt::is_ddl_stmt(stmt_type, has_global_variable) || ObStmt::is_tcl_stmt(stmt_type)) {
      ret = lib::is_oracle_mode() ? OB_NOT_SUPPORTED : OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG;
      LOG_WARN("OBE-14552: Cannot Perform a DDL Commit or Rollback Inside a Query or DML tips",
               K(ret), K(stmt_type), K(lbt()));
      if (OB_NOT_SUPPORTED == ret) {
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "OBE-14552: Cannot Perform a DDL Commit or Rollback Inside a Query or DML tips");
      }
    } else if (lib::is_oracle_mode() &&
              (stmt::T_CREATE_SAVEPOINT == stmt_type
               || stmt::T_ROLLBACK_SAVEPOINT == stmt_type
               || stmt::T_RELEASE_SAVEPOINT == stmt_type)) {
      ret = OB_ERR_CANNOT_PERFORM_DDL_COMMIT_OR_ROLLBACK_INSIDE_QUERY_OR_DML_TIPS;
      LOG_WARN("OBE-14552: Cannot Perform a DDL Commit or Rollback Inside a Query or DML tips",
               K(ret), K(stmt_type), K(lbt()));
    } else if (exec_ctx.get_my_session()->is_in_user_scope() && ObStmt::is_dml_write_stmt(stmt_type)) {
      ret = OB_ERR_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT;
      LOG_WARN("Can't update table while ctas is being created.", K(ret));
    }
  }
  return ret;
}

int ObSPIResultSet::start_trans(ObExecContext &ctx)
{
  // cursor执行之前如果还没有事务, 则开始一个新的事务
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  CK (OB_NOT_NULL(plan_ctx), OB_NOT_NULL(my_session));
  CK (OB_LIKELY(task_exec_ctx.min_cluster_version_is_valid()));
  OZ (ObSqlTransControl::explicit_start_trans(ctx, false));
  return ret;
}

int ObSPIResultSet::set_cursor_env(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  OZ (store_orign_session(&session));
  OZ (restore_cursor_session(&session));
  OX (need_end_nested_stmt_ = EST_RESTORE_SESSION);
  return ret;
}

int ObSPIResultSet::reset_cursor_env(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  OZ (store_cursor_session(&session));
  OZ (restore_orign_session(&session));
  OX (need_end_nested_stmt_ = EST_NEED_NOT);
  return ret;
}

int ObSPIResultSet::start_cursor_stmt(
  ObPLExecCtx *pl_ctx, stmt::StmtType stmt_type, bool is_for_update)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(pl_ctx));
  CK (OB_NOT_NULL(pl_ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = pl_ctx->exec_ctx_->get_my_session()));
  if (OB_SUCC(ret)) {
    OZ (store_orign_session(session));
    OX (need_end_nested_stmt_ = EST_RESTORE_SESSION);
  }
  LOG_TRACE("call spi start cursor stmt",
            K(ret), K(stmt_type),
            K(is_for_update),
            K(need_end_nested_stmt_),
            K(session->get_stmt_type()));
  return ret;
}

void ObSPIResultSet::end_cursor_stmt(ObPLExecCtx *pl_ctx, int &result)
{
  int ret = OB_SUCCESS;
  if (need_end_nested_stmt_ != EST_NEED_NOT) {
    ObSQLSessionInfo *session = NULL;
    CK (OB_NOT_NULL(pl_ctx));
    CK (OB_NOT_NULL(pl_ctx->exec_ctx_));
    CK (OB_NOT_NULL(session = pl_ctx->exec_ctx_->get_my_session()));
    OZ (reset_cursor_env(*session));
    if (OB_SUCC(ret)
        && OB_NOT_NULL(get_result_set())
        && get_out_params().has_out_param()) {
      OZ (ObSPIService::process_function_out_result(
        pl_ctx, *get_result_set(), get_out_params().get_out_params()));
    }
    if (OB_FAIL(ret)) {
      result = OB_SUCCESS == result ? ret : result;
      LOG_WARN("failed to end cursor stmt", K(ret), K(session), K(need_end_nested_stmt_));
    }
    LOG_TRACE("call spi end cursor stmt", K(ret));
  }
  return;
}

int ObSPIResultSet::start_nested_stmt_if_need(ObPLExecCtx *pl_ctx, const ObString &sql, stmt::StmtType stmt_type, bool for_update)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(pl_ctx));
  CK (OB_NOT_NULL(pl_ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = pl_ctx->exec_ctx_->get_my_session()));
  // 如果未发生嵌套, 这里不限制执行语句的类型, 只有在嵌套的情况下才做判断
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (OB_NOT_NULL(pl_ctx->exec_ctx_->get_pl_stack_ctx())
             && pl_ctx->exec_ctx_->get_pl_stack_ctx()->in_nested_sql_ctrl()) {
    // 嵌套的顶层语句一定是一个DML语句, 并且开启了事务, 此时走fast_select流程
    OZ (check_nested_stmt_legal(*pl_ctx->exec_ctx_, sql, stmt_type, for_update));
    OZ (begin_nested_session(*session));
    OX (session->set_query_start_time(ObTimeUtility::current_time()));
    OX (need_end_nested_stmt_ = EST_END_NESTED_SESSION);
    LOG_TRACE("call start nested stmt if need", K(ret),
              K(stmt_type),
              K(need_end_nested_stmt_),
              K(session->get_stmt_type()));
  }
  return ret;
}

void ObSPIResultSet::end_nested_stmt_if_need(ObPLExecCtx *pl_ctx, int &result)
{
  int ret = OB_SUCCESS;
  if (need_end_nested_stmt_ > EST_NEED_NOT) {
    ObSQLSessionInfo *session = NULL;
    CK (need_end_nested_stmt_ == EST_RESTORE_SESSION ||
          need_end_nested_stmt_ == EST_END_NESTED_SESSION);
    CK (OB_NOT_NULL(pl_ctx));
    CK (OB_NOT_NULL(pl_ctx->exec_ctx_));
    CK (OB_NOT_NULL(session = pl_ctx->exec_ctx_->get_my_session()));
    CK (OB_NOT_NULL(pl_ctx->exec_ctx_->get_my_session()->get_pl_context()));
    OX (pl_ctx->exec_ctx_->get_my_session()->get_pl_context()->set_exception_handler_illegal());
    switch (need_end_nested_stmt_) {
    case EST_RESTORE_SESSION:
      OZ (restore_orign_session(session));
      break;
    case EST_END_NESTED_SESSION:
      OZ (end_nested_session(*session));
      break;
    default:
      break;
    }
    OX (need_end_nested_stmt_ = EST_NEED_NOT);
    if (OB_FAIL(ret)) {
      result = OB_SUCCESS == result ? ret : result;
      LOG_WARN("failed to end nested stmt", K(ret));
    }
    LOG_TRACE("call end nested stmt if need", K(ret));
  }
  return;
}


bool ObSPIService::can_obj_access_expr_fast_calc(const ObSqlExpression &expr,
                                                 const ObExprObjAccess *&obj_access)
{
  bool ret = false;
  if (is_obj_access_expression(expr)
      && 1 == expr.get_expr_items().count()
      && OB_NOT_NULL(get_first_expr_item(expr).get_expr_operator())
      && T_OBJ_ACCESS_REF == get_first_expr_item(expr).get_expr_operator()->get_type()) {
    const ObExprOperator* op = get_first_expr_item(expr).get_expr_operator();
    obj_access = static_cast<const ObExprObjAccess*>(op);
    if (OB_NOT_NULL(obj_access)) {
      ret = true;
    }
  } else {
    obj_access = NULL;
  }
  return ret;
}

int ObSPIService::calc_obj_access_expr(ObPLExecCtx *ctx,
                                       const ObSqlExpression &expr,
                                       ObObjParam &result)
{
  int ret = OB_SUCCESS;
  ObIAllocator *expr_alloc = nullptr;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->params_));
  OX (expr_alloc = ctx->get_top_expr_allocator());
  CK (OB_NOT_NULL(expr_alloc));
  if (OB_SUCC(ret)) {
    const ObExprObjAccess *obj_access = NULL;
    ObEvalCtx eval_ctx(*ctx->exec_ctx_, expr_alloc);
    if (1 == expr.get_expr_items().count()) { // 没有入参, 直接计算
      CK (OB_NOT_NULL(obj_access =
          static_cast<const ObExprObjAccess *>(get_first_expr_item(expr).get_expr_operator())));
      OZ(obj_access->calc_result(result, *expr_alloc, NULL, 0, *(ctx->params_), &eval_ctx));
    } else if (2 == expr.get_expr_items().count()
               && T_OBJ_ACCESS_REF == expr.get_expr_items().at(1).get_item_type()) { // 有一个入参, 且入参是ObjAccessExpr
      ObObj first_result;
      CK (OB_NOT_NULL(obj_access =
          static_cast<const ObExprObjAccess *>(expr.get_expr_items().at(1).get_expr_operator())));
      OZ(obj_access->calc_result(first_result, *expr_alloc, NULL, 0, *(ctx->params_), &eval_ctx));
      CK (OB_NOT_NULL(obj_access =
          static_cast<const ObExprObjAccess *>(get_first_expr_item(expr).get_expr_operator())));
      OZ(obj_access->calc_result(result, *expr_alloc, &first_result, 1, *(ctx->params_), &eval_ctx));
    } else {  // 其他情况
      LOG_DEBUG("calc_obj_access_expr without row", K(expr));
      OZ (ObSQLUtils::calc_sql_expression_without_row(*ctx->exec_ctx_, expr, result, expr_alloc));
    }
    ObExprResType type;
    OZ (get_result_type(*ctx, expr, type));
    OX (result.set_param_meta(type));
  }
  return ret;
}

int ObSPIService::spi_pad_char_or_varchar(ObSQLSessionInfo *session_info,
                                          const ObSqlExpression *expr,
                                          ObIAllocator *allocator,
                                          ObObj *result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session_info), OB_NOT_NULL(expr), OB_NOT_NULL(allocator), OB_NOT_NULL(result));
  if (OB_SUCC(ret)
      && result->is_character_type()
      && is_pad_char_to_full_length(session_info->get_sql_mode())
      && T_FUN_COLUMN_CONV == get_expression_type(*expr)) { //TODO:@ryan.ly padding
    const ObSqlFixedArray<ObInfixExprItem>& items = expr->get_expr_items();
    CK(T_INT == items.at(3).get_item_type()); // type
    CK(T_INT32 == items.at(1).get_item_type()); // accuracy
    if (OB_SUCC(ret)) {
      const ObObjType type = static_cast<ObObjType>(items.at(1).get_obj().get_int());
      ObAccuracy accuracy;
      accuracy.set_accuracy(items.at(3).get_obj().get_int());
      OZ (spi_pad_char_or_varchar(session_info, type, accuracy, allocator, result));
    }
  }
  return ret;
}

int ObSPIService::spi_pad_char_or_varchar(ObSQLSessionInfo *session_info,
                                          const ObRawExpr *expr,
                                          ObIAllocator *allocator,
                                          ObObj *result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session_info), OB_NOT_NULL(expr), OB_NOT_NULL(allocator), OB_NOT_NULL(result));
  if (OB_SUCC(ret)
      && result->is_character_type()
      && is_pad_char_to_full_length(session_info->get_sql_mode())
      && T_FUN_COLUMN_CONV == expr->get_expr_type()) { //TODO:@ryan.ly padding
    CK (expr->get_param_count() >= ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO);
    if (OB_SUCC(ret)) {
      ObAccuracy accuracy;
      int32_t data_type = 0;
      int64_t accuracy_type = 0;
      const ObConstRawExpr *type_expr =
        static_cast<const ObConstRawExpr*>(expr->get_param_expr(0));
      const ObConstRawExpr *accuracy_expr =
        static_cast<const ObConstRawExpr*>(expr->get_param_expr(2));
      CK (OB_NOT_NULL(type_expr) && OB_NOT_NULL(accuracy_expr));
      OX (type_expr->get_value().get_int32(data_type));
      OX (accuracy_expr->get_value().get_int(accuracy_type));
      OX (accuracy.set_accuracy(accuracy_type));
      OZ (spi_pad_char_or_varchar(session_info, static_cast<ObObjType>(data_type),
          accuracy, allocator, result));
    }
  }
  return ret;
}

int ObSPIService::spi_pad_char_or_varchar(ObSQLSessionInfo *session_info,
                                          const ObObjType &type,
                                          const ObAccuracy &accuracy,
                                          ObIAllocator *allocator,
                                          ObObj *result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session_info), OB_NOT_NULL(allocator), OB_NOT_NULL(result));
  if (OB_SUCC(ret)
      && result->is_character_type()
      && is_pad_char_to_full_length(session_info->get_sql_mode())) {
    if (OB_SUCC(ret)) {
      if (ObCharType == type || ObNCharType == type) {
        int32_t length = accuracy.get_length(); // byte or char length
        int32_t cell_strlen = 0; // byte or char length
        int32_t pad_whitespace_length = 0; // pad whitespace length
        if (OB_FAIL(result->get_char_length(accuracy, cell_strlen, lib::is_oracle_mode()))) {
          LOG_WARN("Fail to get char length, ", K(ret));
        } else {
          if (cell_strlen < length) {
            pad_whitespace_length = length - cell_strlen;
          }
          if (pad_whitespace_length > 0) {
            ObString res_string;
            if (OB_FAIL(ObCharset::whitespace_padding(*allocator,
                                                      result->get_collation_type(),
                                                      result->get_string(),
                                                      pad_whitespace_length,
                                                      res_string))) {
              LOG_WARN("whitespace_padding failed", K(ret), K(pad_whitespace_length));
            } else {
              // watch out !!! in order to deep copy an ObObj instance whose type is char or varchar,
              // set_collation_type() should be revoked. But here no need to set collation type
              result->set_string(result->get_type(), res_string);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSPIService::spi_pad_binary(ObSQLSessionInfo *session_info,
                                const ObAccuracy &accuracy,
                                ObIAllocator *allocator,
                                ObObj *result)
{
  int ret = OB_SUCCESS;
  const ObObjType &type = result->get_type();
  CK (OB_NOT_NULL(session_info), OB_NOT_NULL(allocator), OB_NOT_NULL(result));
  if (OB_SUCC(ret)
      && result->is_binary()
      && (ObCharType == type || ObNCharType == type)) {
    int32_t obj_max_length = accuracy.get_length(); 
    int32_t cell_strlen = result->get_val_len(); 
    int32_t pad_zero_length = 0; // pad '\0' length
    if (cell_strlen < obj_max_length) {
      pad_zero_length = obj_max_length - cell_strlen;
      ObString res_string;
      const ObCollationType coll_type = result->get_collation_type();
      const ObString &input = result->get_string();
      char *buf = NULL;
      int32_t buf_len = input.length() + pad_zero_length;
      if (OB_UNLIKELY(pad_zero_length <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid len", K(ret), K(pad_zero_length));
      } else if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory", K(ret), K(buf_len));
      } else {
        MEMMOVE(buf, input.ptr(), input.length());
        MEMSET(buf + input.length(), '\0', pad_zero_length);
        res_string = ObString(buf_len, buf_len, buf);
      }
      result->set_string(result->get_type(), res_string);
      }
  }
  return ret;
}

int ObSPIService::cast_enum_set_to_string(ObExecContext &ctx,
                                          const ObIArray<ObString> &enum_set_values,
                                          ObObjParam &src,
                                          ObObj &result)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = ctx.get_expr_factory();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObConstRawExpr *c_expr = NULL;
  ObSysFunRawExpr *out_expr = NULL;
  ObExprResType result_type;
  CK (OB_NOT_NULL(expr_factory));
  CK (OB_NOT_NULL(session_info));
  OZ (expr_factory->create_raw_expr(static_cast<ObItemType>(src.get_type()), c_expr));
  CK (OB_NOT_NULL(c_expr));
  OZ (c_expr->set_enum_set_values(enum_set_values));
  OX (c_expr->set_value(src));
  OX (result_type.set_meta(src.get_meta()));
  OX (result_type.set_accuracy(src.get_accuracy()));
  OX (c_expr->set_result_type(result_type));
  OX (c_expr->mark_enum_set_skip_build_subschema());
  OZ (ObRawExprUtils::create_type_to_str_expr(*expr_factory, c_expr, out_expr, session_info, true));
  CK (OB_NOT_NULL(out_expr));
  OZ (ObSPIService::spi_calc_raw_expr(session_info, &(ctx.get_allocator()), out_expr, &result));
  return ret;
}

int ObSPIService::spi_calc_raw_expr(ObSQLSessionInfo *session,
                                    ObIAllocator *allocator,
                                    const ObRawExpr *expr,
                                    ObObj *result)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ParamStore param_store((ObWrapperAllocator(*allocator)));
  CK (OB_NOT_NULL(result));
  OZ (ObSQLUtils::se_calc_const_expr(session, expr, param_store, *allocator, exec_ctx, *result));
  OX (result->set_collation_level(expr->get_result_type().get_collation_level()));
  OZ (spi_pad_char_or_varchar(session, expr, allocator, result));
  return ret;
}

int ObSPIService::spi_convert(ObSQLSessionInfo &session,
                              ObIAllocator &allocator,
                              ObObj &src,
                              const ObExprResType &dst_type,
                              ObObj &dst,
                              bool ignore_fail,
                              const ObIArray<ObString> *type_info)
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = CM_NONE;
  OZ (ObSQLUtils::get_default_cast_mode(stmt::T_NONE, &session, cast_mode));
  if (OB_SUCC(ret)) {
    if (ignore_fail) {
      cast_mode |= CM_WARN_ON_FAIL;
    }
    bool is_strict = is_strict_mode(session.get_sql_mode());
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session);
    ObCastCtx cast_ctx(&allocator, &dtc_params, cast_mode, dst_type.get_collation_type());
    if ((dst_type.is_blob() || dst_type.is_blob_locator() || src.is_blob() || src.is_blob_locator()) && lib::is_oracle_mode()) {
      cast_ctx.cast_mode_ |= CM_ENABLE_BLOB_CAST;
    }
    if (dst_type.is_null() || dst_type.is_unknown() || dst_type.is_ext()) {
      OX (dst = src);
    } else if (ob_is_enum_or_set_type(src.get_type()) && ob_is_enum_or_set_type(dst_type.get_type())) {
      OX (dst = src);
    } else {
      OZ (ObExprColumnConv::convert_with_null_check(dst, src, dst_type, is_strict, cast_ctx, type_info));
    }
  }
  return ret;
}

int ObSPIService::spi_convert(ObSQLSessionInfo *session,
                              ObIAllocator *allocator,
                              ObObjParam &src,
                              const ObExprResType &result_type,
                              ObObjParam &result)
{
  int ret = OB_SUCCESS;
  ObObj dst;
  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(allocator));
  OZ (spi_convert(*session, *allocator, src, result_type, dst));
  OX (src.set_param_meta());
  OX (result.set_accuracy(result_type.get_accuracy()));
  OX (result.set_meta_type(result_type.get_obj_meta()));
  OX (result = dst);
  OX (result.set_param_meta());
  return ret;
}

int ObSPIService::spi_convert_objparam(ObPLExecCtx *ctx,
                                       ObObjParam *src,
                                       const int64_t result_idx,
                                       ObObjParam *result,
                                       bool need_set)
{
  int ret = OB_SUCCESS;
  const ObPLDataType *expected_type;
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->func_));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->params_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(ctx->allocator_));
  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(result));
  CK (ctx->params_->count() == ctx->func_->get_variables().count());
  CK (result_idx < ctx->params_->count() && result_idx >= 0);
  CK (OB_NOT_NULL(expected_type = &(ctx->func_->get_variables().at(result_idx))));
  CK (OB_NOT_NULL(expected_type->get_data_type()));
  if (OB_SUCC(ret)) {
    ObExprResType result_type;
    ObObjParam result_value;
    result_type.reset();
    result_type.set_meta(expected_type->get_data_type()->get_meta_type());
    result_type.set_accuracy(expected_type->get_data_type()->get_accuracy());
    if (result_type.is_null()) {
      ObObjParam value;
      CK (OB_LIKELY(STANDALONE_ANONYMOUS == ctx->func_->get_proc_type()));
      OX (value = *src);
      OX (value.set_int(0));
      OZ (deep_copy_obj(*ctx->allocator_, *src, value));
      OX (result_value = value);
      if (OB_SUCC(ret) && need_set) {
        void *ptr = ctx->params_->at(result_idx).get_deep_copy_obj_ptr();
        if (nullptr != ptr) {
          ctx->allocator_->free(ptr);
        }
        OX (ctx->params_->at(result_idx) = result_value);
      }
    } else {
      ObObjParam value;
      OZ (spi_convert(ctx->exec_ctx_->get_my_session(), &tmp_alloc, *src, result_type, value));
      OZ (deep_copy_obj(*ctx->allocator_, value, result_value));
      if (OB_SUCC(ret) && need_set) {
        void *ptr = ctx->params_->at(result_idx).get_deep_copy_obj_ptr();
        if (nullptr != ptr) {
          ctx->allocator_->free(ptr);
        }
        OX (ctx->params_->at(result_idx).apply(result_value));
        OX (ctx->params_->at(result_idx).set_param_meta());
      }
    }
    if (OB_NOT_NULL(result)) {
      OX (*result = result_value);
    }
  }
  return ret;
}

int ObSPIService::spi_calc_expr_at_idx(pl::ObPLExecCtx *ctx,
                                       const int64_t expr_idx,
                                       const int64_t result_idx,
                                       ObObjParam *result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->func_));
  CK (expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= expr_idx && expr_idx < ctx->func_->get_expressions().count()));
  const ObSqlExpression *expr = nullptr;
  OX (expr = ctx->func_->get_expressions().at(expr_idx));
  OZ (spi_calc_expr(ctx, expr, result_idx, result));
  return ret;
}

int ObSPIService::spi_calc_expr(ObPLExecCtx *ctx,
                                const ObSqlExpression *expr,
                                const int64_t result_idx,
                                ObObjParam *result)
{
  int ret = OB_SUCCESS;
  ObIAllocator *expr_allocator = nullptr;
  CK (OB_NOT_NULL(ctx), OB_NOT_NULL(expr), OB_NOT_NULL(result));
  CK (OB_NOT_NULL(ctx->exec_ctx_), OB_NOT_NULL(ctx->params_), OB_NOT_NULL(ctx->allocator_), OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  OX (expr_allocator = ctx->get_top_expr_allocator());
  CK (OB_NOT_NULL(expr_allocator));
  if (OB_SUCC(ret)) {
    ObItemType expr_type = expr->get_expr_items().at(0).get_item_type();
    if (IS_CONST_TYPE(expr_type)) {
      bool need_check = false;
      const ObObj &value = expr->get_expr_items().at(0).get_obj();
      if (T_QUESTIONMARK == expr_type) {
        OZ (ObSQLUtils::get_param_value<ObObjParam>(value, *ctx->params_, *result, need_check));
      } else {
        OZ (deep_copy_obj(*expr_allocator, value, *result));
        result->set_param_meta();
      }
    } else if (T_OBJ_ACCESS_REF == expr_type) {
      OZ (calc_obj_access_expr(ctx, *expr, *result));
    } else {
      LOG_DEBUG("spi_calc_expr without row", K(*expr));
      ObExprResType result_type;
      bool has_implicit_savepoint = false;
      bool explicit_trans = ctx->exec_ctx_->get_my_session()->has_explicit_start_trans();
      ObPLContext *pl_ctx = ctx->exec_ctx_->get_pl_stack_ctx();
      CK (OB_NOT_NULL(pl_ctx));
      if (OB_SUCC(ret) && lib::is_mysql_mode() && !pl_ctx->is_function_or_trigger()) {
        if (ctx->exec_ctx_->get_my_session()->is_in_transaction()) {
          const ObString expr_savepoint_name("PL expr savepoint");
          OZ (ObSqlTransControl::create_savepoint(*ctx->exec_ctx_, expr_savepoint_name));
          OX (has_implicit_savepoint = true);
        }
      }
      OZ (ObSQLUtils::calc_sql_expression_without_row(*ctx->exec_ctx_, *expr, *result, expr_allocator),
        KPC(expr), K(result_idx));
      OX(result->set_param_meta());

      if ((OB_DATA_OUT_OF_RANGE == ret || OB_ERR_DATA_TOO_LONG == ret
          || OB_ERR_VALUE_LARGER_THAN_ALLOWED == ret) && lib::is_oracle_mode()) {
        LOG_WARN("change error code to value error", K(ret));
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      }
      OZ (get_result_type(*ctx, *expr, result_type));
      OX (result->set_param_meta(result_type));
      /* 如果本层是udf, 本次计算的表达式中含有udf;
         如果内层udf失败，由udf内部来回滚, 如果内层udf成功, 发生了强转失败等问题, 此处不回滚,
         由本层udf的destory接口来保证回滚, 兼容mysql */
      if (lib::is_mysql_mode() && !pl_ctx->is_function_or_trigger()) {
        if (OB_SUCCESS != ret && ctx->exec_ctx_->get_my_session()->is_in_transaction()) {
          int tmp_ret = OB_SUCCESS;
          if (has_implicit_savepoint) {
            const ObString expr_savepoint_name("PL expr savepoint");
            if (OB_SUCCESS !=
                (tmp_ret = ObSqlTransControl::rollback_savepoint(*ctx->exec_ctx_, expr_savepoint_name))) {
              LOG_WARN("failed to rollback current pl to implicit savepoint", K(ret), K(tmp_ret));
            }
#ifdef OB_BUILD_ORACLE_PL
          } else if (ctx->exec_ctx_->get_my_session()->associated_xa()) {
            if (OB_TRANS_XA_BRANCH_FAIL != ret) {
              tmp_ret = ObDbmsXA::xa_rollback_savepoint(*ctx->exec_ctx_);
              if (OB_SUCCESS != tmp_ret) {
                LOG_WARN("xa trans roll back to save point failed",
                        K(tmp_ret), KPC(ctx->exec_ctx_->get_my_session()->get_tx_desc()));
              }
            }
#endif
          } else if (ctx->exec_ctx_->get_my_session()->get_in_transaction()) {
            tmp_ret = ObPLContext::implicit_end_trans(*ctx->exec_ctx_->get_my_session(), *ctx->exec_ctx_, true);
          }
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        } else if (ctx->exec_ctx_->get_my_session()->get_local_autocommit() && !explicit_trans) {
          if (!ctx->exec_ctx_->get_my_session()->associated_xa()) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS == ret) {
              if (OB_SUCCESS != (tmp_ret = ObPLContext::implicit_end_trans(*ctx->exec_ctx_->get_my_session(), *ctx->exec_ctx_, false, true))) {
                // 不覆盖原来的错误码
                LOG_WARN("failed to explicit end trans", K(ret), K(tmp_ret));
              }
            }
            ret = OB_SUCCESS == ret ? tmp_ret : ret;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)
      && result->is_binary()
      && result->get_val_len() < result->get_accuracy().get_length()) {
      OZ (spi_pad_binary(ctx->exec_ctx_->get_my_session(), 
        result->get_accuracy(), expr_allocator, result));
  }

  if (OB_SUCC(ret)
      && result->is_character_type()) {
    ObObjType type = result->get_type();
    if (lib::is_mysql_mode() && T_QUESTIONMARK == get_expression_type(*expr) &&
        (ObCharType == type || ObNCharType == type)) {
      if (is_pad_char_to_full_length(ctx->exec_ctx_->get_my_session()->get_sql_mode())) {
        OZ (spi_pad_char_or_varchar(
          ctx->exec_ctx_->get_my_session(), type, result->get_accuracy(), expr_allocator, result));
      } else {
        ObString res = result->get_string();
        OZ (ObCharsetUtils::remove_char_endspace(  // this function only adjust res.data_length_
            res, ObCharset::get_charset(result->get_collation_type())));
        OX (result->val_len_ = res.length());
      }
    } else {
      OZ (spi_pad_char_or_varchar(
        ctx->exec_ctx_->get_my_session(), expr, expr_allocator, result));
    }
  }

  if (OB_SUCC(ret)) {
    if (result->is_null()) {
      result->v_.int64_ = 0;
    }
    if (OB_INVALID_INDEX != result_idx) {
      // local baisc var 需要用执行栈allocator深拷一次
      ObObjParam &param = ctx->params_->at(result_idx);
      bool is_ref_cursor = param.is_ref_cursor_type();
      ObAccuracy invalid_accuracy;
      if (!result->is_ext()) {
        ObObjParam tmp;
        OZ (deep_copy_obj(*ctx->allocator_, *result, tmp));
        OX (*result = tmp);
        if (OB_SUCC(ret)) {
          bool has_lob_header = result->ObObj::has_lob_header();
          if (param.get_meta().get_scale() != SCALE_UNKNOWN_YET) {
            result->ObObj::set_scale(param.get_meta().get_scale());
            result->set_accuracy(ctx->params_->at(result_idx).get_accuracy());
          } else if (result->get_accuracy() == invalid_accuracy) {
            result->set_accuracy(ctx->params_->at(result_idx).get_accuracy());
          }
          if (result->is_null()) {
            result->set_param_meta(param.get_param_meta());
          }
          if (has_lob_header) {
            result->ObObj::set_has_lob_header();
          }
          if (is_ref_cursor &&
              param.get_ext() != 0 && result->is_null()) {
            OZ (spi_add_ref_cursor_refcount(ctx, &param, -1));
          }
          if (OB_SUCC(ret)) {
            void *ptr = param.get_deep_copy_obj_ptr();
            if (nullptr != ptr) {
              ctx->allocator_->free(ptr);
            }
            param = *result;
            param.set_is_ref_cursor_type(is_ref_cursor);
            param.set_param_meta();
          }
        }
      } else if (!is_ref_cursor) {
        int64_t orig_udt_id = ctx->params_->at(result_idx).get_udt_id();
        ObObj tmp;
        if (result->get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
          OZ (ObUserDefinedType::deep_copy_obj(*ctx->allocator_, *result, tmp));
        } else {
          tmp = *result;
        }
        if (0 != ctx->params_->at(result_idx).get_ext()) {
          OZ (ObUserDefinedType::destruct_objparam(*ctx->allocator_,
                                                  ctx->params_->at(result_idx),
                                                  ctx->exec_ctx_->get_my_session()));
        }
        OX (ctx->params_->at(result_idx) = tmp);
        OX (ctx->params_->at(result_idx).set_udt_id(orig_udt_id));
        OX (ctx->params_->at(result_idx).set_param_meta());
      }
    }
    result->set_param_meta();
  }
  if (OB_SUCC(ret)) {
    result->set_is_pl_mock_default_param(expr->get_is_pl_mock_default_expr());
  } else if (lib::is_mysql_mode()) {
    ctx->exec_ctx_->get_my_session()->set_show_warnings_buf(ret);
  }
  SET_SPI_STATUS;
  return ret;
}

int ObSPIService::spi_calc_subprogram_expr(ObPLExecCtx *ctx,
                                           uint64_t package_id,
                                           uint64_t routine_id,
                                           int64_t expr_idx,
                                           ObObjParam *result)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObPLExecState *state = NULL;
  ObSqlExpression *expr = NULL;
  CK (OB_NOT_NULL(exec_ctx = ctx->exec_ctx_));
  CK (OB_NOT_NULL(session_info = exec_ctx->get_my_session()));
  OZ (ObPLContext::get_exec_state_from_local(*session_info, package_id, routine_id, state));
  CK (OB_NOT_NULL(state));
  CK (OB_NOT_NULL(exec_ctx = state->get_exec_ctx().exec_ctx_));
  CK (expr_idx >= 0 && expr_idx < state->get_function().get_expressions().count());
  CK (OB_NOT_NULL(expr = state->get_function().get_expressions().at(expr_idx)));
  if (OB_SUCC(ret)) {
    ExecCtxBak exec_ctx_bak;
    OX (exec_ctx_bak.backup(*exec_ctx));
    OX (exec_ctx->set_physical_plan_ctx(&(state->get_physical_plan_ctx())));
    if (OB_SUCC(ret) && state->get_function().get_expr_op_size() > 0)  {
      OZ (exec_ctx->init_expr_op(state->get_function().get_expr_op_size()));
    }
    OZ (state->get_function().get_frame_info().pre_alloc_exec_memory(*exec_ctx));
    OZ (spi_calc_expr(&(state->get_exec_ctx()), expr, OB_INVALID_ID, result), KPC(expr));
    if (state->get_function().get_expr_op_size() > 0) {
      exec_ctx->reset_expr_op();
      exec_ctx->get_allocator().free(exec_ctx->get_expr_op_ctx_store());
    }
    exec_ctx_bak.restore(*exec_ctx);
  }
  return ret;
}

int ObSPIService::spi_calc_package_expr_v1(const pl::ObPLResolveCtx &resolve_ctx,
                                           sql::ObExecContext &exec_ctx,
                                           ObIAllocator &allocator,
                                           uint64_t package_id,
                                           int64_t expr_idx,
                                           ObObjParam *result)
{
  int ret = OB_SUCCESS;
  ObSqlExpression *sql_expr = NULL;
  ObPLPackageManager &pl_manager = resolve_ctx.session_info_.get_pl_engine()->get_package_manager();
  ObCacheObjGuard *cache_obj_guard = NULL;
  ObPLPackage *package = NULL;
   ObPLPackageGuard &guard = resolve_ctx.package_guard_;
  OZ (pl_manager.get_package_expr(resolve_ctx, package_id, expr_idx, sql_expr));
  CK (OB_NOT_NULL(sql_expr));
  OZ (guard.get(package_id, cache_obj_guard));
  CK (OB_NOT_NULL(cache_obj_guard));
  CK (OB_NOT_NULL(package = static_cast<ObPLPackage*>(cache_obj_guard->get_cache_obj())));
  if (OB_SUCC(ret)) {
    ExecCtxBak exec_ctx_bak;
    sql::ObPhysicalPlanCtx phy_plan_ctx(exec_ctx.get_allocator());
    OX (exec_ctx_bak.backup(exec_ctx));
    OX (exec_ctx.set_physical_plan_ctx(&phy_plan_ctx));
    if (OB_SUCC(ret) && package->get_expr_op_size() > 0)  {
      OZ (exec_ctx.init_expr_op(package->get_expr_op_size()));
    }
    OZ (package->get_frame_info().pre_alloc_exec_memory(exec_ctx));
    OZ (ObSQLUtils::calc_sql_expression_without_row(exec_ctx, *sql_expr, *result, &allocator));
    if (package->get_expr_op_size() > 0) {
      exec_ctx.reset_expr_op();
      exec_ctx.get_allocator().free(exec_ctx.get_expr_op_ctx_store());
    }
    exec_ctx_bak.restore(exec_ctx);
  }
  return ret;
}

int ObSPIService::spi_calc_package_expr(ObPLExecCtx *ctx,
                                        uint64_t package_id, 
                                        int64_t expr_idx,
                                        ObObjParam *result)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  ObPL *pl_engine = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(GCTX.schema_service_));
  CK (OB_NOT_NULL(exec_ctx = ctx->exec_ctx_));
  CK (OB_NOT_NULL(session_info = exec_ctx->get_my_session()));
  CK (OB_NOT_NULL(sql_proxy = exec_ctx->get_sql_proxy()));
  CK (OB_NOT_NULL(pl_engine = exec_ctx->get_my_session()->get_pl_engine()));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(
                            session_info->get_effective_tenant_id(),
                            schema_guard));
  if (OB_SUCC(ret)) {
    ObPLPackageGuard package_guard(session_info->get_effective_tenant_id());
    ObSqlExpression *sql_expr = NULL;
    ObPLPackageManager &pl_manager = pl_engine->get_package_manager();
    ObPLPackageGuard &guard = ctx->guard_ != NULL ? (*ctx->guard_) : package_guard;
    ObCacheObjGuard *cache_obj_guard = NULL;
    ObPLPackage *package = NULL;
    ObPLResolveCtx resolve_ctx(exec_ctx->get_allocator(),
                               *session_info,
                               schema_guard,
                               guard,
                               *sql_proxy,
                               false);
    OZ (package_guard.init());
    OZ (pl_manager.get_package_expr(resolve_ctx, package_id, expr_idx, sql_expr));
    CK (OB_NOT_NULL(sql_expr));
    OZ (guard.get(package_id, cache_obj_guard));
    CK (OB_NOT_NULL(cache_obj_guard));
    CK (OB_NOT_NULL(package = static_cast<ObPLPackage*>(cache_obj_guard->get_cache_obj())));
    if (OB_SUCC(ret)) {
      ExecCtxBak exec_ctx_bak;
      sql::ObPhysicalPlanCtx phy_plan_ctx(exec_ctx->get_allocator());
      OX (exec_ctx_bak.backup(*exec_ctx));
      OX (exec_ctx->set_physical_plan_ctx(&phy_plan_ctx));
      if (OB_SUCC(ret) && package->get_expr_op_size() > 0)  {
        OZ (exec_ctx->init_expr_op(package->get_expr_op_size()));
      }
      OZ (package->get_frame_info().pre_alloc_exec_memory(*exec_ctx));
      OZ (spi_calc_expr(ctx, sql_expr, OB_INVALID_ID, result));
      if (package->get_expr_op_size() > 0) {
        exec_ctx->reset_expr_op();
        exec_ctx->get_allocator().free(exec_ctx->get_expr_op_ctx_store());
      }
      exec_ctx_bak.restore(*exec_ctx);
    }
  }
  return ret;
}

int ObSPIService::check_and_deep_copy_result(ObIAllocator &alloc,
                                             const ObObj &src,
                                             ObObj &dst)
{
  int ret = OB_SUCCESS;

  if (dst.is_pl_extend()) {
    if (!src.is_pl_extend()) {
      ret =OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("expr is wrong type", K(ret));
    } else if (PL_CURSOR_TYPE == src.get_meta().get_extend_type() ||
               PL_REF_CURSOR_TYPE == src.get_meta().get_extend_type() ||
               PL_OPAQUE_TYPE == src.get_meta().get_extend_type()) {
      OZ (ObUserDefinedType::deep_copy_obj(alloc, src, dst, true));
    } else {
      ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(dst.get_ext());
      if (NULL == composite) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected composite to store datum", KPC(composite), K(ret));
      } else {
        if (OB_SUCC(ret)) {
          ObPLComposite *src_composite = reinterpret_cast<ObPLComposite*>(src.get_ext());
          if (NULL == src_composite || src_composite->get_type() != composite->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected composite to store datum", KPC(src_composite), K(ret));
          } else if (OB_INVALID_ID == src_composite->get_id() || OB_INVALID_ID ==  composite->get_id()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected composite to store datum", K(src_composite->get_id()), K(composite->get_id()), K(ret));
          } else if (src_composite->get_id() != composite->get_id()) {
            ret = OB_ERR_EXPRESSION_WRONG_TYPE;
            LOG_WARN("src id is not same as dst id", K(src_composite->get_id()), K(composite->get_id()), K(ret));
          } else {
            OZ (pl::ObUserDefinedType::deep_copy_obj(alloc, src, dst));
          }
        }
      }
    }
  } else if (src.is_pl_extend()) {
    ret =OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("expr is wrong type", K(ret));
  } else {
    OZ (deep_copy_obj(alloc, src, dst));
  }

  return ret;
}

int ObSPIService::spi_set_package_variable(
  ObExecContext *exec_ctx,
  ObPLPackageGuard *guard,
  uint64_t package_id, int64_t var_idx, const ObObj &value,
  ObIAllocator *allocator, bool need_deep_copy)
{
  int ret = OB_SUCCESS;
  ObPL *pl_engine = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  ObSQLSessionInfo *session_info = NULL;
  CK (OB_NOT_NULL(GCTX.schema_service_));
  CK (OB_NOT_NULL(exec_ctx));
  CK (OB_NOT_NULL(session_info = exec_ctx->get_my_session()));
  CK (OB_NOT_NULL(sql_proxy = exec_ctx->get_sql_proxy()));
  CK (OB_NOT_NULL(pl_engine = session_info->get_pl_engine()));
  if (OB_SUCC(ret)) {
    ObObj result = *const_cast<ObObj *>(&value);
    ObPLPackageManager &pl_manager = pl_engine->get_package_manager();
    share::schema::ObSchemaGetterGuard schema_guard;
    ObPLPackageGuard package_guard(session_info->get_effective_tenant_id());
    ObPLResolveCtx resolve_ctx(exec_ctx->get_allocator(),
                               *session_info,
                               schema_guard,
                               guard != NULL ? *(guard) : package_guard,
                               *sql_proxy,
                               false); // is_prepare_protocol
    OZ (GCTX.schema_service_->get_tenant_schema_guard(
        session_info->get_effective_tenant_id(), schema_guard));
    OZ (package_guard.init());
    OZ (pl_manager.set_package_var_val(
          resolve_ctx, *exec_ctx, package_id, var_idx, result),
          K(package_id), K(var_idx));
  }
  return ret;
}

int ObSPIService::spi_set_package_variable(
  ObPLExecCtx *ctx, uint64_t package_id, int64_t var_idx, const ObObj &value,
  bool need_deep_copy)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = NULL;
  if (need_deep_copy) {
    OZ (spi_get_package_allocator(ctx, package_id, allocator));
  }
  OZ (spi_set_package_variable(
    ctx->exec_ctx_, ctx->guard_, package_id, var_idx, value, allocator, need_deep_copy));
  return ret;
}

int ObSPIService::spi_set_variable_to_expr(ObPLExecCtx *ctx,
                                          const int64_t expr_idx,
                                           const ObObjParam *value,
                                          bool is_default,
                                          bool need_copy)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->func_));
  CK (expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= expr_idx && expr_idx < ctx->func_->get_expressions().count()));
  const ObSqlExpression *expr = nullptr;
  OX (expr = ctx->func_->get_expressions().at(expr_idx));
  OZ (spi_set_variable(ctx, expr, value, is_default, need_copy));
  return ret;
}

int ObSPIService::spi_set_variable(ObPLExecCtx *ctx,
                                   const ObSqlExpression* expr,
                                   const ObObjParam *value,
                                   bool is_default,
                                   bool need_copy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(expr) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(expr), K(value), K(ret));
  } else {
    ObItemType expr_type = expr->get_expr_items().at(0).get_item_type();
    if (T_OP_GET_SYS_VAR == expr_type || T_OP_GET_USER_VAR == expr_type) {
      if (expr->get_expr_items().count() < 2 || T_VARCHAR != expr->get_expr_items().at(1).get_item_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected result expr", K(*expr), K(ret));
      } else {
        const ObString &name = expr->get_expr_items().at(1).get_obj().get_string();
        ObSetVar::SetScopeType scope = ObSetVar::SET_SCOPE_NEXT_TRANS;
        if (T_OP_GET_SYS_VAR == expr_type) {
          if (expr->get_expr_items().count() < 3 || T_INT != expr->get_expr_items().at(2).get_item_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected result expr", K(*expr), K(ret));
          } else {
            scope = static_cast<ObSetVar::SetScopeType>(expr->get_expr_items().at(2).get_obj().get_int());
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(set_variable(ctx, scope, name, *value, is_default))) {
          LOG_WARN("Failed to set variable", K(name), K(value), K(ret));
        }
      }
    } else if (T_OP_GET_PACKAGE_VAR == expr_type) {
      OV (5 <= expr->get_expr_items().count(), OB_ERR_UNEXPECTED, expr->get_expr_items().count());
      CK (T_UINT64 == expr->get_expr_items().at(1).get_item_type());
      CK (T_INT == expr->get_expr_items().at(2).get_item_type());
      OZ (spi_set_package_variable(
          ctx,
          expr->get_expr_items().at(1).get_obj().get_uint64(), // pkg id
          expr->get_expr_items().at(2).get_obj().get_int(), // var idx
          *value, // value
          need_copy));
    } else if (T_OP_GET_SUBPROGRAM_VAR == expr_type) {
      ObSQLSessionInfo *session_info = NULL;
      uint64_t package_id = OB_INVALID_ID;
      uint64_t routine_id = OB_INVALID_ID;
      int64_t var_idx = OB_INVALID_INDEX;
      ObPLExecState *state = NULL;
      ObObjParam result = *const_cast<ObObjParam *>(value);
      CK (OB_NOT_NULL(ctx->exec_ctx_));
      CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
      OX (package_id = expr->get_expr_items().at(1).get_obj().get_uint64());
      OX (routine_id = expr->get_expr_items().at(2).get_obj().get_uint64());
      OX (var_idx = expr->get_expr_items().at(3).get_obj().get_int());
      OZ (ObPLContext::get_exec_state_from_local(*session_info, package_id, routine_id, state));
      OZ (state->set_var(var_idx, result));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid sys func", K(expr_type), K(ret));
    }
  }

  SET_SPI_STATUS;
  return ret;
}

int ObSPIService::set_variable(ObPLExecCtx *ctx,
                               const ObSetVar::SetScopeType scope,
                               const ObString &name,
                               const ObObjParam &value,
                               bool is_default)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->exec_ctx_)|| name.empty() || value.is_invalid_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(name), K(value), K(ret));
  } else {
    ObSQLSessionInfo *session = ctx->exec_ctx_->get_my_session();
    ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_SET_VAR), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Argument in pl context is NULL", K(session), K(ret));
    } else {
      char *sql = NULL;
      char *new_buf = NULL;
      char buf[OB_SHORT_SQL_LENGTH];
      MEMSET(buf, 0, OB_SHORT_SQL_LENGTH);
      int64_t pos = 0;
      snprintf(buf + pos, OB_SHORT_SQL_LENGTH - pos, "SET ");
      pos += 4;
      if (ObSetVar::SET_SCOPE_SESSION == scope) {
        snprintf(buf + pos, OB_SHORT_SQL_LENGTH - pos, "@@SESSION.");
        pos += 10;
      } else if (ObSetVar::SET_SCOPE_GLOBAL == scope) {
        snprintf(buf + pos, OB_SHORT_SQL_LENGTH - pos, "@@GLOBAL.");
        pos += 9;
      } else {
        snprintf(buf + pos, OB_SHORT_SQL_LENGTH - pos, "@");
        pos += 1;
      }

      if (OB_SUCC(ret)) {
        if (name.length() >= OB_SHORT_SQL_LENGTH - pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Argument in pl context is NULL",
                   K(OB_SHORT_SQL_LENGTH - pos), K(name), K(ret));
        } else {
          snprintf(buf + pos, name.length() + 1, "%s", name.ptr());
          pos += name.length();
        }
      }

      if (OB_SUCC(ret)) {
        snprintf(buf + pos, OB_SHORT_SQL_LENGTH - pos, "=");
        pos += 1;
      }

      if (OB_SUCC(ret)) {
        if (is_default) {
          snprintf(buf + pos, OB_SHORT_SQL_LENGTH - pos, "DEFAULT");
          sql = buf;
        } else {
          ObObjPrintParams print_params(session->get_timezone_info(),
                                        session->get_dtc_params().connection_collation_);
          int64_t tmp_pos = 0;
          if (OB_FAIL(value.print_sql_literal(buf + pos,
                                              OB_SHORT_SQL_LENGTH - pos,
                                              tmp_pos,
                                              print_params))) {
            if (OB_SIZE_OVERFLOW == ret) {
              int64_t alloc_len = OB_MAX_SQL_LENGTH;
              while (OB_SIZE_OVERFLOW == ret) {
                ret = OB_SUCCESS;
                if (OB_ISNULL(new_buf)) {
                  new_buf = static_cast<char*>(allocator.alloc(alloc_len));
                } else {
                  if (alloc_len < (1L << 20)) {
                    alloc_len *= 2;
                    allocator.free(new_buf);
                    new_buf = NULL;
                    new_buf = static_cast<char*>(allocator.alloc(alloc_len));
                  } else {
                    ret = OB_SIZE_OVERFLOW;
                    LOG_WARN("failed to print_plain_str_literal",
                              K(buf), K(new_buf), K(pos), K(tmp_pos), K(ret));
                    break;
                  }
                }
                if (OB_ISNULL(new_buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed to alloc memory for set sql", K(ret), K(OB_MAX_SQL_LENGTH));
                } else {
                  tmp_pos = 0;
                  MEMCPY(new_buf, buf, pos);
                  if (OB_FAIL(value.print_sql_literal(new_buf + pos,
                                                      alloc_len - pos,
                                                      tmp_pos,
                                                      print_params))) {
                    if (OB_SIZE_OVERFLOW != ret) {
                      LOG_WARN("failed to print_plain_str_literal",
                            K(buf), K(new_buf), K(pos), K(tmp_pos), K(ret));
                    }
                  } else {
                    sql = new_buf;
                  }
                }
              }
            } else {
              LOG_WARN("failed to print_plain_str_literal",
                       K(buf), K(new_buf), K(pos), K(tmp_pos), K(ret));
            }
          } else {
            sql = buf;
          }
        }
      }

      if(OB_SUCC(ret)) {
        if (OB_FAIL(spi_query(ctx, sql, stmt::T_VARIABLE_SET))) {
          LOG_WARN("Failed to spi_query", K(sql), K(ret));
        }
      }
    }
  }
  return ret;
}

void ObSPIService::adjust_pl_status_for_xa(sql::ObExecContext &ctx, int &result)
{
  if (OB_NOT_NULL(ctx.get_my_session())
      && ctx.get_my_session()->get_pl_context()) {
    ctx.get_my_session()->set_pl_can_retry(false);
    recreate_implicit_savapoint_if_need(ctx, result);
  }
  return;
}

int ObSPIService::recreate_implicit_savapoint_if_need(pl::ObPLExecCtx *ctx, int &result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()->get_pl_context()));
  OZ (recreate_implicit_savapoint_if_need(*(ctx->exec_ctx_), result));
  return ret;
}

int ObSPIService::recreate_implicit_savapoint_if_need(sql::ObExecContext &ctx, int &result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx.get_my_session()));
  if (OB_SUCC(ret) // 存在隐式的savepoint才做检查
      && OB_NOT_NULL(ctx.get_my_session()->get_pl_context())
      && ctx.get_my_session()->has_pl_implicit_savepoint()) {
    ObSQLSessionInfo *session_info = ctx.get_my_session();
    if (!session_info->is_in_transaction()) { // 事务已经结束, 清除隐式savepoint标记
      OX (session_info->clear_pl_implicit_savepoint());
    } else if (!session_info->get_tx_desc()
               ->contain_savepoint(PL_IMPLICIT_SAVEPOINT)) {
      // PL内部的rollback to savepoint语句回滚到了外层的检查点，将PL的隐式检查点冲掉了，此时需要重建检查点
      OZ (ObSqlTransControl::create_savepoint(ctx, PL_IMPLICIT_SAVEPOINT));
    }
  }
  result = OB_SUCCESS == result ? ret : result;
  return ret;
}

int ObSPIService::spi_end_trans(ObPLExecCtx *ctx, const char *sql, bool is_rollback)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  if (OB_SUCC(ret)) {
    ObSQLSessionInfo *my_session = ctx->exec_ctx_->get_my_session();
    if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("session ptr is null", K(ret));
    } else {
      ObString sqlstr(sql);
      OZ (ObSPIResultSet::check_nested_stmt_legal(*(ctx->exec_ctx_), sqlstr, stmt::T_END_TRANS));
      int64_t saved_query_start_time = my_session->get_query_start_time();
      my_session->set_query_start_time(ObTimeUtility::current_time());
      if (OB_SUCC(ret)) {
        if (my_session->is_in_transaction() &&
          my_session->get_tx_desc()->is_xa_trans()) {
#ifndef OB_BUILD_ORACLE_PL
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not support ObDbmsXA", K(ret));
#else
          transaction::ObXATransID xid = my_session->get_xid();
          transaction::ObGlobalTxType global_tx_type = my_session->get_tx_desc()->get_global_tx_type(xid);
          bool is_dblink = (transaction::ObGlobalTxType::DBLINK_TRANS == global_tx_type);
          transaction::ObTransID tx_id;
          if (is_rollback) {
            //Rollback can be executed in the xa transaction,
            //the role is to roll back all modifications,
            //but does not end the xa transaction
            if (is_dblink) {
              if (OB_FAIL(ObTMService::tm_rollback(*ctx->exec_ctx_, tx_id))) {
                LOG_WARN("fail to do rollback for dblink trans", K(ret), K(tx_id), K(xid), K(global_tx_type));
              }
            } else if (OB_FAIL(pl::ObDbmsXA::xa_rollback_origin_savepoint(*(ctx->exec_ctx_)))) {
              LOG_WARN("rollback xa changes failed", K(ret), K(xid));
            }
          } else {
            if (is_dblink) {
              if (OB_FAIL(ObTMService::tm_commit(*ctx->exec_ctx_, tx_id))) {
                LOG_WARN("fail to do commit for dblink trans", K(ret), K(tx_id), K(xid), K(global_tx_type));
              }
            } else {
              //Commit is prohibited in xa transaction
              ret = OB_TRANS_XA_ERR_COMMIT;
              LOG_WARN("COMMIT is not allowed in a xa trans", K(ret), K(xid));
            }
          }
          if (is_dblink) {
            const bool force_disconnect = false;
            int tmp_ret = OB_SUCCESS;
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret =
                my_session->get_dblink_context().clean_dblink_conn(force_disconnect)))) {
              LOG_WARN("dblink transaction failed to release dblink connections", K(tmp_ret), K(tx_id), K(xid));
            }
          }
          ctx->exec_ctx_->set_need_disconnect(false);
#endif
        } else {
          // PL内部的提交使用同步提交
          OZ (sql::ObSqlTransControl::end_trans(*ctx->exec_ctx_, is_rollback, true));
          // 如果发生过提交禁止PL整体重试
          if (!is_rollback) {
            OX (ctx->exec_ctx_->get_my_session()->set_pl_can_retry(false));
          }
        }
      }
      // restore query_start_time
      my_session->set_query_start_time(saved_query_start_time);
    }
  }
  if (OB_SUCC(ret)
      && OB_NOT_NULL(ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor())) {
    ctx->exec_ctx_->get_my_session()->get_pl_implicit_cursor()->set_rowcount(0);
  }
  recreate_implicit_savapoint_if_need(ctx, ret);
  LOG_DEBUG("spi end trans", K(ret), K(sql), K(is_rollback));

  return ret;
}

ObPLSPITraceIdGuard::ObPLSPITraceIdGuard(const ObString &sql,
                    const ObString &ps_sql,
                    ObSQLSessionInfo &session,
                    int &ret,
                    ObCurTraceId::TraceId *reused_trace_id)
    : sql_(sql), ps_sql_(ps_sql), session_(session), ret_(ret)
{
  if (OB_NOT_NULL(ObCurTraceId::get_trace_id())) {
    origin_trace_id_.set(*ObCurTraceId::get_trace_id());

    if (reused_trace_id != nullptr && reused_trace_id->is_valid()) {
      // do not log when fetching from cursor
      ObCurTraceId::get_trace_id()->set(*reused_trace_id);
    } else {
      ObCurTraceId::TraceId new_trace_id;
      new_trace_id.init(origin_trace_id_.get_addr());

      // log with PL trace_id
      LOG_TRACE("executing sql, trace id changed",
              K(sql_), K(ps_sql_),
              "from", origin_trace_id_,
              "to", new_trace_id);

      ObCurTraceId::get_trace_id()->set(new_trace_id);
    }
  }
}

ObPLSPITraceIdGuard::~ObPLSPITraceIdGuard()
{
  int &ret = ret_;
  ObCurTraceId::TraceId curr_trace_id;
  if (OB_NOT_NULL(ObCurTraceId::get_trace_id())) {
    curr_trace_id.set(*ObCurTraceId::get_trace_id());
    session_.set_last_trace_id(ObCurTraceId::get_trace_id());
    ObCurTraceId::get_trace_id()->set(origin_trace_id_);

    if (OB_FAIL(ret) && ret != OB_READ_NOTHING) {
      LOG_WARN("sql execution finished, trace id restored",
            K(ret), K(sql_), K(ps_sql_),
            "from", curr_trace_id,
            "to", origin_trace_id_);
    } else {
      LOG_TRACE("sql execution finished, trace id restored",
            K(sql_), K(ps_sql_),
            "from", curr_trace_id,
            "to", origin_trace_id_);
    }
  }
}

// common sql execute interface (static-sql & dynamic-sql & dbms-sql-execute)
int ObSPIService::spi_inner_execute(ObPLExecCtx *ctx,
                                    ObIAllocator &out_param_alloc,
                                    const char *sql,
                                    const char *ps_sql,
                                    int64_t type,
                                    void *params,
                                    int64_t param_count,
                                    const ObSqlExpression **into_exprs,
                                    int64_t into_count,
                                    const ObDataType *column_types,
                                    int64_t type_count,
                                    const bool *exprs_not_null_flag,
                                    const int64_t *pl_integer_ranges,
                                    int64_t is_bulk,
                                    bool is_forall,
                                    bool is_type_record,
                                    bool for_update,
                                    bool is_dynamic_sql,
                                    ObIArray<ObObjParam*> *using_out_params,
                                    bool is_dbms_sql)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->allocator_));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()->get_pl_sqlcode_info()));

  if (OB_SUCC(ret)) {
    HEAP_VAR(ObSPIResultSet, spi_result) {

      ObSQLSessionInfo *session = ctx->exec_ctx_->get_my_session();
      stmt::StmtType stmt_type = static_cast<stmt::StmtType>(type);
      ObString sqlstr(sql);

      if (is_forall && !session->is_enable_batched_multi_statement()) {
        /* forall need rollback to for loop */
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_TRACE("cannot batch execute", K(ret), K(sql), K(type));
      }

      OZ (spi_result.init(*session));
      OZ (spi_result.start_nested_stmt_if_need(ctx, sqlstr, stmt_type, for_update));
      OX (spi_result.get_sql_ctx().is_dbms_sql_ = is_dbms_sql);

      if (OB_SUCC(ret)) {

        ObPLSqlAuditRecord audit_record(sql::PLSql);
        ObQueryRetryCtrl retry_ctrl;
        ObSPIExecEnvGuard env_guard(*session, spi_result);
        int save_sqlcode = session->get_pl_sqlcode_info()->get_sqlcode();
        ObString save_sqlmsg = session->get_pl_sqlcode_info()->get_sqlmsg();

        do {

          bool can_retry = true;
          int64_t row_count = 0;
          ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_STATIC_SQL_EXEC), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          {
            ObPLSPITraceIdGuard trace_id_guard(sql, ps_sql, *session, ret);
            ObPLSubPLSqlTimeGuard guard(ctx);
            ObPLSqlAuditGuard audit_guard(
              *(ctx->exec_ctx_), *(session), spi_result, audit_record, ret, (sql != NULL ? sql : ps_sql), retry_ctrl, trace_id_guard, static_cast<stmt::StmtType>(type));
            ObSPIRetryCtrlGuard retry_guard(retry_ctrl, spi_result, *session, ret);

            OX (session->get_pl_sqlcode_info()->set_sqlcode(OB_SUCCESS));

            if (OB_SUCC(ret) && !ObStmt::is_diagnostic_stmt(stmt_type) && lib::is_mysql_mode()) {
              ob_reset_tsi_warning_buffer();
            }

            OZ (inner_open(ctx,
                           allocator,
                           sql,
                           ps_sql,
                           type,
                           params,
                           param_count,
                           into_exprs,
                           into_count,
                           spi_result,
                           spi_result.get_out_params(),
                           is_forall,
                           is_dynamic_sql,
                           is_dbms_sql), K(sql), K(ps_sql));
            OZ (inner_fetch(ctx,
                            can_retry,
                            spi_result,
                            into_exprs,
                            into_count,
                            column_types,
                            type_count,
                            exprs_not_null_flag,
                            pl_integer_ranges,
                            OB_NOT_NULL(using_out_params) && !using_out_params->empty() ? using_out_params : NULL,
                            row_count,
                            is_bulk,
                            is_forall,
                            is_dynamic_sql,
                            NULL,
                            false,
                            false,
                            INT64_MAX,
                            NULL,
                            0,
                            is_type_record), K(ps_sql), K(sql), K(type));

            if (OB_SUCC(ret) && (is_dynamic_sql || is_dbms_sql) && !is_bulk) {
              // if it is bulk into, not allow using out param, so no need deep copy
              OZ (dynamic_out_params(
                out_param_alloc, spi_result.get_result_set(), params, param_count, is_dbms_sql));
            }
            if (spi_result.get_out_params().has_out_param()) {
              OZ (process_function_out_result(ctx, *spi_result.get_result_set(), spi_result.get_out_params().get_out_params()));
            }

            ObPLSqlCodeInfo *sqlcode_info = session->get_pl_sqlcode_info();
            if (sqlcode_info->get_sqlcode() == OB_SUCCESS) {
              sqlcode_info->set_sqlcode(save_sqlcode, save_sqlmsg);
            }

            if (can_retry) {
              retry_guard.test();
            }
            int close_ret = spi_result.close_result_set();
            if (OB_SUCCESS != close_ret) {
              LOG_WARN("close spi result failed", K(ret), K(close_ret));
            }
            ret = OB_SUCCESS == ret ? close_ret : ret;
          }
          if (!is_dbms_sql) {
            spi_result.destruct_exec_params(*session);
          }

        } while (RETRY_TYPE_NONE != retry_ctrl.get_retry_type()); //SPI只做LOCAL重试
      }

      if (OB_SUCC(ret)
          && (ObStmt::is_ddl_stmt(stmt_type, true)
              || ObStmt::is_tcl_stmt(stmt_type)
              || (lib::is_mysql_mode() && session->get_local_autocommit()))) {
        OX (session->set_pl_can_retry(false));
      }

      if (OB_SUCC(ret) && 0 == param_count) {
        if (ObStmt::is_ddl_stmt(stmt_type, true)
            || ObStmt::is_tcl_stmt(stmt_type)
            || ObStmt::is_savepoint_stmt(stmt_type)) {
          if (ObStmt::is_ddl_stmt(stmt_type, true)) {
            OZ (force_refresh_schema(session->get_effective_tenant_id()), sql);
          }
          recreate_implicit_savapoint_if_need(ctx, ret);
        }
      }

      // 记录第一条SQL执行PartitionHit信息, 并对PartitionHit进行Freeze, 防止后续的SQL冲掉
      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(spi_result.get_result_set()->get_physical_plan())) {
          session->partition_hit().freeze();
        }
      }

      if (OB_FAIL(ret) && !ObStmt::is_diagnostic_stmt(stmt_type)) {
        // support `SHOW WARNINGS` in mysql PL
        session->set_show_warnings_buf(ret);
      }

      spi_result.end_nested_stmt_if_need(ctx, ret);

      SET_FORALL_BULK_EXCEPTION;
      SET_SPI_STATUS;
    }
  }
  return ret;
}

int ObSPIService::dbms_cursor_execute(ObPLExecCtx *ctx,
                                      const ObString ps_sql,
                                      stmt::StmtType stmt_type,
                                      ObDbmsCursorInfo &cursor,
                                      bool is_dbms_sql)
{
  int ret = OB_SUCCESS;
  OZ (spi_inner_execute(ctx,
                        cursor.get_area_allocator(),
                        cursor.get_sql_stmt().ptr(),
                        ps_sql.ptr(),
                        stmt_type,
                        &cursor.get_exec_params(),
                        cursor.get_exec_params().count(),
                        nullptr, /*into_exprs*/
                        0, /*into_count*/
                        nullptr, /*column_types*/
                        0,/*type_count*/
                        nullptr, /*exprs_not_null_flag*/
                        nullptr, /*pl_integer_ranges*/
                        false, /*is_bulk*/
                        false, /*is_forall*/
                        false, /*is_type_record*/
                        false, /*for_update*/
                        false, /*is_dynamic_sql*/
                        nullptr, /*using_out_params*/
                        true/*is_dbms_sql*/));
  return ret;
}

int ObSPIService::spi_check_autonomous_trans(pl::ObPLExecCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  pl::ObPLContext *pl_context = NULL;
  OV (OB_NOT_NULL(ctx));
  OV (OB_NOT_NULL(ctx->allocator_));
  OV (OB_NOT_NULL(ctx->exec_ctx_));
  OV (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(pl_context = ctx->exec_ctx_->get_pl_stack_ctx()));

  if (OB_SUCC(ret) && pl_context->is_autonomous()) {
    if (session_info->is_in_transaction() && session_info->has_exec_inner_dml()) {
      ret = OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK;
      LOG_WARN("active autonomous transaction detected", K(ret));
    }
    if (OB_SUCC(ret) || OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK == ret) {
      int end_trans_ret = pl_context->end_autonomous(*ctx->exec_ctx_, *session_info);
      ret = OB_SUCCESS == ret ? end_trans_ret : ret;
      pl_context->clear_autonomous();
    }
  }
  return ret;
}


int ObSPIService::spi_get_current_expr_allocator(pl::ObPLExecCtx *ctx, int64_t *addr)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *alloc = nullptr;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(addr));
  OX (*addr = 0);
  OX (alloc = ctx->get_top_expr_allocator());
  CK (OB_NOT_NULL(alloc));
  OX (*addr = reinterpret_cast<int64_t>(alloc));
  return ret;
}

int ObSPIService::spi_init_composite(ObIAllocator *current_allcator, int64_t addr, bool is_record, bool need_allocator)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(current_allcator));
  if (OB_SUCC(ret)) {
    if (is_record) {
      ObPLRecord *record = reinterpret_cast<ObPLRecord *>(addr);
      CK(OB_NOT_NULL(record));
      OZ (record->init_data(*current_allcator, need_allocator));
    } else {
      ObPLCollection *collection = reinterpret_cast<ObPLCollection *>(addr);
      CK(OB_NOT_NULL(collection));
      OZ (collection->init_allocator(*current_allcator, need_allocator));
    }
  }
  return ret;
}

int ObSPIService::spi_get_parent_allocator(ObIAllocator *current_allcator,
                                           int64_t *parent_allocator_addr)
{
  int ret = OB_SUCCESS;
  ObPLAllocator1 *pl_allocator = nullptr;
  *parent_allocator_addr = 0;
  CK (OB_NOT_NULL(current_allcator));
  CK (OB_NOT_NULL(parent_allocator_addr));
  OX (pl_allocator = dynamic_cast<ObPLAllocator1 *>(current_allcator));
  CK (OB_NOT_NULL(pl_allocator));
  if (OB_SUCC(ret) && OB_NOT_NULL(pl_allocator->get_parent_allocator())) {
    *parent_allocator_addr = reinterpret_cast<int64_t>(pl_allocator->get_parent_allocator());
  }

  return ret;
}

int ObSPIService::spi_query_into_expr_idx(ObPLExecCtx *ctx,
                                          const char *sql,
                                          int64_t type,
                                          const int64_t *into_exprs_idx,
                                          int64_t into_count,
                                          const ObDataType *column_types,
                                          int64_t type_count,
                                          const bool *exprs_not_null_flag,
                                          const int64_t *pl_integer_ranges,
                                          bool is_bulk,
                                          bool is_type_record,
                                          bool for_update)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator alloc("SpiTemp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const ObSqlExpression **into_exprs = nullptr;

  MAKE_EXPR_BUFFER(alloc, into_exprs_idx, into_count, into_exprs);

  OZ (spi_query(ctx, sql, type, into_exprs, into_count, column_types,
                type_count, exprs_not_null_flag, pl_integer_ranges, is_bulk,
                is_type_record, for_update));

  return ret;
}

int ObSPIService::spi_query(ObPLExecCtx *ctx,
                            const char *sql,
                            int64_t type,
                            const ObSqlExpression **into_exprs,
                            int64_t into_count,
                            const ObDataType *column_types,
                            int64_t type_count,
                            const bool *exprs_not_null_flag,
                            const int64_t *pl_integer_ranges,
                            bool is_bulk,
                            bool is_type_record,
                            bool for_update)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pl_spi_query);
  OZ (SMART_CALL(spi_inner_execute(ctx, ctx->expr_alloc_, sql, "", type, NULL, 0,
                        into_exprs, into_count,
                        column_types, type_count,
                        exprs_not_null_flag,
                        pl_integer_ranges, is_bulk, false, is_type_record, for_update)),
                        sql, type);
  return ret;
}

int ObSPIService::spi_execute_with_expr_idx(ObPLExecCtx *ctx,
                              const char *ps_sql,
                              int64_t type,
                              const int64_t *param_exprs_idx,
                              int64_t param_count,
                              const int64_t *into_exprs_idx,
                              int64_t into_count,
                              const ObDataType *column_types,
                              int64_t type_count,
                              const bool *exprs_not_null_flag,
                              const int64_t *pl_integer_ranges,
                              bool is_bulk,
                              bool is_forall,
                              bool is_type_record,
                              bool for_update)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator alloc("SpiTemp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const ObSqlExpression **param_exprs = nullptr;
  const ObSqlExpression **into_exprs = nullptr;

  MAKE_EXPR_BUFFER(alloc, param_exprs_idx, param_count, param_exprs);
  MAKE_EXPR_BUFFER(alloc, into_exprs_idx, into_count, into_exprs);

  OZ (spi_execute(ctx, ps_sql, type, param_exprs, param_count, into_exprs,
                  into_count, column_types, type_count, exprs_not_null_flag,
                  pl_integer_ranges, is_bulk, is_forall, is_type_record,
                  for_update));

  return ret;
}
int ObSPIService::spi_execute(ObPLExecCtx *ctx,
                              const char *ps_sql,
                              int64_t type,
                              const ObSqlExpression **param_exprs,
                              int64_t param_count,
                              const ObSqlExpression **into_exprs,
                              int64_t into_count,
                              const ObDataType *column_types,
                              int64_t type_count,
                              const bool *exprs_not_null_flag,
                              const int64_t *pl_integer_ranges,
                              bool is_bulk,
                              bool is_forall,
                              bool is_type_record,
                              bool for_update)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pl_spi_execute);
  OZ (SMART_CALL(spi_inner_execute(ctx, ctx->expr_alloc_, NULL, ps_sql, type, param_exprs, param_count,
                        into_exprs, into_count, column_types, type_count,
                        exprs_not_null_flag, pl_integer_ranges, is_bulk,
                        is_forall, is_type_record, for_update)));
  return ret;
}

int ObSPIService::spi_prepare(common::ObIAllocator &allocator,
                              ObSQLSessionInfo &session,
                              ObMySQLProxy &sql_proxy,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              sql::ObRawExprFactory &expr_factory,
                              const ObString &sql,
                              bool is_cursor,
                              pl::ObPLBlockNS *secondary_namespace,
                              ObSPIPrepareResult &prepare_result)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pl_spi_prepare);
  CHECK_COMPATIBILITY_MODE(&session);
  ret = is_oracle_mode() ?
      spi_resolve_prepare(allocator,
                          session,
                          sql_proxy,
                          schema_guard,
                          expr_factory,
                          sql,
                          is_cursor,
                          secondary_namespace,
                          prepare_result)
      : spi_parse_prepare(allocator,
                          session,
                          sql_proxy,
                          schema_guard,
                          expr_factory,
                          sql,
                          secondary_namespace,
                          prepare_result);
  return ret;
}

int ObSPIService::spi_parse_prepare(common::ObIAllocator &allocator,
                                    ObSQLSessionInfo &session,
                                    ObMySQLProxy &sql_proxy,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    sql::ObRawExprFactory &expr_factory,
                                    const ObString &sql,
                                    pl::ObPLBlockNS *secondary_namespace,
                                    ObSPIPrepareResult &prepare_result)
{
  int ret = OB_SUCCESS;
  if (sql.empty() || OB_ISNULL(secondary_namespace)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(sql), K(secondary_namespace), K(ret));
  } else {
    ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
    ParseResult parse_result;
    if (OB_FAIL(parser.prepare_parse(sql, static_cast<void*>(secondary_namespace), parse_result))) {
      LOG_WARN("Generate syntax tree failed", K(sql), K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, ObString(parse_result.no_param_sql_len_, parse_result.no_param_sql_), prepare_result.route_sql_))) {
      LOG_WARN("failed to write string", K(sql), K(ret));
    } else {
      PLPrepareCtx pl_prepare_ctx(session, NULL, false, false, false);
      SMART_VAR(PLPrepareResult, pl_prepare_result) {
        CK (OB_NOT_NULL(GCTX.sql_engine_));
        OZ (pl_prepare_result.init(session));
        CK (OB_NOT_NULL(pl_prepare_result.result_set_));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(GCTX.sql_engine_->handle_pl_prepare(
                prepare_result.route_sql_, pl_prepare_ctx, pl_prepare_result))) {
            LOG_WARN("query failed", K(ret), K(sql));
          }
        }
        LOG_TRACE("execute sql", K(sql), K(ret));

        if (OB_SUCC(ret)) {
          OZ (ob_write_string(allocator, pl_prepare_result.result_set_->get_stmt_ps_sql(), prepare_result.ps_sql_));
          prepare_result.type_ = pl_prepare_result.result_set_->get_stmt_type();
          prepare_result.for_update_ = pl_prepare_result.result_set_->get_is_select_for_update();
          prepare_result.has_hidden_rowid_ = false;
          prepare_result.is_bulk_ = false;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(resolve_exec_params(parse_result,
                                          session,
                                          schema_guard,
                                          expr_factory,
                                          *secondary_namespace,
                                          prepare_result,
                                          allocator))) { //resolve PL exec变量
            LOG_WARN("failed to resolve_exec_params", K(ret));
          } else if (OB_FAIL(resolve_into_params(parse_result,
                                                session,
                                                schema_guard,
                                                expr_factory,
                                                *secondary_namespace,
                                                prepare_result))) { //resolve PL into变量
            LOG_WARN("failed to resolve_into_params", K(ret));
          } else if (OB_FAIL(resolve_ref_objects(parse_result,
                                                session,
                                                schema_guard,
                                                prepare_result))) { //resolve ref object
            LOG_WARN("failed to resolve_ref_objects", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSPIService::spi_build_record_type(common::ObIAllocator &allocator,
                                        ObSQLSessionInfo &session,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        const sql::ObResultSet &result_set,
                                        int64_t hidden_column_count,
                                        pl::ObRecordType *&record_type,
                                        uint64_t &rowid_table_id,
                                        pl::ObPLBlockNS *secondary_namespace,
                                        bool &has_dup_column_name)
{
  int ret = OB_SUCCESS;
  const common::ColumnsFieldIArray *columns = result_set.get_field_columns();
  has_dup_column_name = false;
  if (OB_ISNULL(columns) || OB_ISNULL(record_type) || 0 == columns->count() || OB_ISNULL(secondary_namespace)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(columns), K(record_type), K(ret));
  } else {
    int dup_idx = 0;
    OZ (record_type->record_members_init(&allocator, columns->count() - hidden_column_count));
    for (int64_t i = 0; OB_SUCC(ret) && i < columns->count() - hidden_column_count; ++i) {
      ObPLDataType pl_type;
      if (columns->at(i).type_.is_ext() && columns->at(i).accuracy_.accuracy_ != OB_INVALID_ID) {
        uint64_t udt_id = columns->at(i).accuracy_.accuracy_;
        const ObUserDefinedType *user_type = NULL;
        OZ (secondary_namespace->get_pl_data_type_by_id(udt_id, user_type));
        CK (OB_NOT_NULL(user_type));
        OX (pl_type.set_user_type_id(user_type->get_type(), udt_id));
        OX (pl_type.set_type_from(user_type->get_type_from()));
      } else if (columns->at(i).type_.is_user_defined_sql_type()) {
        if (columns->at(i).type_.is_xml_sql_type()) {
          // dynamic cast from sql type to pl type in store_result
          uint64_t udt_id = T_OBJ_XML;
          const ObUserDefinedType *user_type = NULL;
          OZ (secondary_namespace->get_pl_data_type_by_id(udt_id, user_type));
          CK (OB_NOT_NULL(user_type));
          OX (pl_type.set_user_type_id(user_type->get_type(), udt_id));
          OX (pl_type.set_type_from(user_type->get_type_from()));
        } else {
          uint16_t subschema_id;
          uint64_t udt_id = columns->at(i).accuracy_.accuracy_;
          const ObUserDefinedType *user_type = NULL;
          OZ (secondary_namespace->get_pl_data_type_by_id(udt_id, user_type));
          CK (OB_NOT_NULL(user_type));
          if (OB_FAIL(ret)) {
          } else if (!ObObjUDTUtil::ob_is_supported_sql_udt(udt_id)) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("inconsistent datatypes", K(ret), K(udt_id));
          } else if (OB_FAIL((const_cast<ObExecContext &>(result_set.get_exec_context()).get_subschema_id_by_udt_id(udt_id, subschema_id)))) {
            LOG_WARN("Failed to get subshcema_meta_info", K(ret), K(udt_id));
          } else if (subschema_id != columns->at(i).type_.get_udt_subschema_id()) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("inconsistent datatypes", K(ret), K(subschema_id), K(udt_id), K(columns->at(i).type_.get_udt_subschema_id()));
          } else {
            OX (pl_type.set_user_type_id(user_type->get_type(), udt_id));
            OX (pl_type.set_type_from(user_type->get_type_from()));
          }
        }
      } else if (columns->at(i).type_.is_geometry()) {
        // cast geometry type to pl extend
        uint64_t udt_id = T_OBJ_SDO_GEOMETRY;
        const ObUserDefinedType *user_type = NULL;
        OZ (secondary_namespace->get_pl_data_type_by_id(udt_id, user_type));
        CK (OB_NOT_NULL(user_type));
        OX (pl_type.set_user_type_id(user_type->get_type(), udt_id));
        OX (pl_type.set_type_from(user_type->get_type_from()));
      } else {
        ObDataType data_type;
        data_type.set_meta_type(columns->at(i).type_.get_meta());
        data_type.set_accuracy(columns->at(i).accuracy_);
        pl_type.set_data_type(data_type);
      }
      if (OB_SUCC(ret)) {
        char* name_buf = NULL;
        if (OB_ISNULL(name_buf = static_cast<char*>(allocator.alloc(columns->at(i).cname_.length() + 10)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc column name buf", K(ret), K(columns->at(i).cname_));
        } else {
          bool duplicate = false;
          for (int64_t j = 0; OB_SUCC(ret) && j < columns->count() - hidden_column_count; ++j) {
            if (i != j && columns->at(j).cname_ == columns->at(i).cname_) {
              duplicate = true;
              has_dup_column_name = true;
              break;
            }
          }
          if (duplicate) {
            sprintf(name_buf, "%.*s&%d",
                    columns->at(i).cname_.length(), columns->at(i).cname_.ptr(), dup_idx);
            dup_idx++;
          } else {
            sprintf(name_buf, "%.*s", columns->at(i).cname_.length(), columns->at(i).cname_.ptr());
          }
          ObString deep_copy_name(name_buf);
          if (OB_FAIL(record_type->add_record_member(deep_copy_name, pl_type))) {
            LOG_WARN("add record member failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && 1 == hidden_column_count) {
        const common::ObField &field = columns->at(columns->count() - 1);
        uint64_t table_id = OB_INVALID_ID;
        OZ (schema_guard.get_table_id(session.get_effective_tenant_id(), field.dname_, field.org_tname_,
                                      false, ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id));
        OX (rowid_table_id = table_id);
    } else {
      rowid_table_id = OB_INVALID_ID;
    }
  }
  return ret;
}

int ObSPIService::spi_resolve_prepare(common::ObIAllocator &allocator,
                                      ObSQLSessionInfo &session,
                                      ObMySQLProxy &sql_proxy,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      sql::ObRawExprFactory &expr_factory,
                                      const ObString &sql,
                                      bool is_cursor,
                                      pl::ObPLBlockNS *secondary_namespace,
                                      ObSPIPrepareResult &prepare_result)
{
  int ret = OB_SUCCESS;
  if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(sql), K(ret));
  } else {
    PLPrepareCtx pl_prepare_ctx(session, secondary_namespace, false, false, is_cursor);
    const int64_t start = ::oceanbase::common::ObTimeUtility::current_time();

    SMART_VAR(PLPrepareResult, pl_prepare_result) {
      CK (OB_NOT_NULL(GCTX.sql_engine_));
      OZ (pl_prepare_result.init(session));
      CK (OB_NOT_NULL(pl_prepare_result.result_set_));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(GCTX.sql_engine_->handle_pl_prepare(sql, pl_prepare_ctx, pl_prepare_result))) {
          LOG_WARN("query failed", K(ret), K(start), K(sql));
        }
      }
      LOG_TRACE("execute sql", K(sql), K(ret));

      if (OB_SUCC(ret)) {
        OZ (ob_write_string(allocator, pl_prepare_result.result_set_->get_stmt_ps_sql(), prepare_result.ps_sql_));
        prepare_result.type_ = pl_prepare_result.result_set_->get_stmt_type();
        prepare_result.for_update_ = pl_prepare_result.result_set_->get_is_select_for_update();
        prepare_result.has_hidden_rowid_ = pl_prepare_result.result_set_->has_hidden_rowid();
        prepare_result.is_bulk_ = pl_prepare_result.result_set_->is_bulk();
        prepare_result.has_link_table_ = pl_prepare_result.result_set_->is_link_table();
        prepare_result.is_skip_locked_ = pl_prepare_result.result_set_->is_skip_locked();
        if (OB_FAIL(ret)) {
        } else if (OB_NOT_NULL(prepare_result.record_type_)) {
          if (stmt::T_SELECT != prepare_result.type_) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("cursor only supported select stmt", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-select stmt in cursor");
          } else if (OB_NOT_NULL(pl_prepare_result.result_set_->get_field_columns()) &&
                    pl_prepare_result.result_set_->get_field_columns()->count() > 0) {
            OZ (spi_build_record_type(allocator,
                                      session,
                                      schema_guard,
                                      *pl_prepare_result.result_set_,
                                      prepare_result.has_hidden_rowid_ ? 1 : 0,
                                      prepare_result.record_type_,
                                      prepare_result.rowid_table_id_,
                                      secondary_namespace,
                                      prepare_result.has_dup_column_name_));
          } else {
            PLPrepareCtx tmp_pl_prepare_ctx(session, secondary_namespace, false, false, false);
            const ObString &route_sql = pl_prepare_result.result_set_->get_stmt_ps_sql();
            ParamStore *params = NULL;
            ObRawExpr *expr = NULL;
            ObObjParam exec_param;
            ObExprResType result_type;
            if (OB_ISNULL(params = reinterpret_cast<ParamStore *>(allocator.alloc(sizeof(ParamStore))))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc memory for exec params", K(ret));
            }
            OX (new (params) ParamStore( (ObWrapperAllocator(allocator)) ) );
            OZ (params->reserve(pl_prepare_result.result_set_->get_external_params().count()));
            for (int64_t i = 0; OB_SUCC(ret) &&
                  i < pl_prepare_result.result_set_->get_external_params().count(); ++i) {
              OX (expr = pl_prepare_result.result_set_->get_external_params().at(i));
              if (OB_ISNULL(expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to copy expr, expr is NULL", K(expr), K(ret));
              } else {
                OX (exec_param.reset());
                OX (exec_param.ObObj::reset());
                OX (exec_param.set_type(ObNullType));
                OX (result_type.reset());
                OX (exec_param.set_param_meta(expr->get_result_meta()));
                OX (result_type = expr->get_result_type());
                OX (exec_param.set_accuracy(result_type.get_accuracy()));
                if (OB_SUCC(ret) && result_type.is_ext()) {
                  if (ob_is_xml_pl_type(result_type.get_type(), result_type.get_udt_id())) {
                    const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(&session);
                    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, ObCharset::get_system_collation());
                    if (OB_FAIL(ObObjCaster::to_type(ObUserDefinedSQLType, cast_ctx, exec_param, exec_param))) {
                      LOG_WARN("failed to cast type of xml type", K(ret), K(exec_param));
                    }
                  }
                }
                if (OB_SUCC(ret) && OB_FAIL(params->push_back(exec_param))) {
                  LOG_WARN("failed to push back param", K(ret));
                }
              }
            }
            if (OB_SUCC(ret)) {
              SMART_VAR(PLPrepareResult, tmp_pl_prepare_result) {
              CK (OB_NOT_NULL(GCTX.sql_engine_));
              OZ (tmp_pl_prepare_result.init(session));
              CK (OB_NOT_NULL(tmp_pl_prepare_result.result_set_));
              // 如果当前语句含有INTO, 则resultset中没有输出列, 我们使用reconstruct的route_sql来构造recordtype
              CK (!pl_prepare_result.result_set_->get_route_sql().empty());
              OZ(GCTX.sql_engine_->handle_pl_prepare(route_sql, tmp_pl_prepare_ctx, tmp_pl_prepare_result, params));
              CK (OB_NOT_NULL(tmp_pl_prepare_result.result_set_->get_field_columns()));
              OZ (spi_build_record_type(allocator,
                                        session,
                                        schema_guard,
                                        *tmp_pl_prepare_result.result_set_,
                                        prepare_result.has_hidden_rowid_ ? 1 : 0,
                                        prepare_result.record_type_,
                                        prepare_result.rowid_table_id_,
                                        secondary_namespace,
                                        prepare_result.has_dup_column_name_));
              }
            }
          }
        }
        OZ (append(prepare_result.ref_objects_, pl_prepare_result.result_set_->get_ref_objects()));
        if (OB_SUCC(ret)) {
          ObRawExpr *expr = NULL;
          for (int64_t i = 0; OB_SUCC(ret) && i < pl_prepare_result.result_set_->get_external_params().count(); ++i) {
            expr = NULL;
            if (OB_FAIL(ObPLExprCopier::copy_expr(expr_factory,
                                                  pl_prepare_result.result_set_->get_external_params().at(i),
                                                  expr))) {
              LOG_WARN("failed to copy expr",
                        K(i), K(pl_prepare_result.result_set_->get_external_params().at(i)), K(ret));
            } else if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to copy expr, expr is NULL", K(expr), K(ret));
            } else { /*do nothing*/ }
            if (OB_SUCC(ret) && OB_FAIL(prepare_result.exec_params_.push_back(expr))) {
              LOG_WARN("push_back error", K(ret));
            }
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < pl_prepare_result.result_set_->get_into_exprs().count(); ++i) {
            expr = NULL;
            if (OB_FAIL(ObPLExprCopier::copy_expr(expr_factory,
                                                  pl_prepare_result.result_set_->get_into_exprs().at(i),
                                                  expr))) {
              LOG_WARN("failed to copy expr", K(i), K(pl_prepare_result.result_set_->get_into_exprs().at(i)), K(ret));
            } else if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to copy expr", K(expr), K(ret));
            } else if (OB_FAIL(prepare_result.into_exprs_.push_back(expr))) {
              LOG_WARN("push_back error", K(ret));
            }
          }
          // add debug info, for convenience of sql reconstruct debug
          LOG_DEBUG("spi prepare, source sql and prepared reconstruct sql", K(sql),
                                              K(pl_prepare_result.result_set_->get_route_sql()));
          if (OB_SUCC(ret)) {
            if (pl_prepare_result.result_set_->get_route_sql().empty()) {
              prepare_result.route_sql_ = sql;
            } else {
              if (OB_FAIL(ob_write_string(allocator, pl_prepare_result.result_set_->get_route_sql(), prepare_result.route_sql_))) {
                LOG_WARN("failed to write string", K(sql), K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSPIService::calc_dynamic_sqlstr(
  ObPLExecCtx *ctx, const ObSqlExpression *sql, ObSqlString &sql_str)
{
  int ret = OB_SUCCESS;
  ObObjParam result;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(sql));
  OZ (spi_calc_expr(ctx, sql, OB_INVALID_INDEX, &result));
  if (OB_FAIL(ret)) {
  } else if (result.is_null_oracle()) {
    ret = OB_ERR_STATEMENT_STRING_IN_EXECUTE_IMMEDIATE_IS_NULL_OR_ZERO_LENGTH;
    LOG_WARN(
      "OBE-06535: statement string in EXECUTE IMMEDIATE is NULL or 0 length", K(ret), K(result));
  } else if (!result.is_string_type() && !result.is_clob_locator()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Dynamic sql is not a string", K(ret), K(result), K(sql_str));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Dynamic sql is not a string");
  } else {
    ObArenaAllocator temp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_DYNAMIC_SQL_EXEC), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString tmp_sql;
    ObString user_sql;
    ObCharsetType client_cs_type = CHARSET_INVALID;

    OZ (result.get_string(tmp_sql));
    if (OB_SUCC(ret) && tmp_sql.length() > 1 && ';' == tmp_sql[tmp_sql.length() - 1]) {
      tmp_sql.assign_ptr(tmp_sql.ptr(), tmp_sql.length() - 1);
    }
    OZ (ctx->exec_ctx_->get_my_session()->get_character_set_client(client_cs_type));
    OZ (ObCharset::charset_convert(temp_allocator, tmp_sql,
                                   result.get_collation_type(),
                                   ObCharset::get_default_collation(client_cs_type),
                                   user_sql));
    OZ (sql_str.append(user_sql));
    LOG_DEBUG("Dynamic sql", K(ret), K(result), K(sql_str));
  }
  return ret;
}

int ObSPIService::prepare_dynamic(ObPLExecCtx *ctx,
                                  const ObSqlExpression *sql_expr,
                                  ObIAllocator &allocator,
                                  bool is_returning,
                                  int64_t param_cnt,
                                  ObSqlString &sql_str,
                                  ObString &ps_sql,
                                  stmt::StmtType &stmt_type,
                                  bool &for_update,
                                  bool &hidden_rowid,
                                  int64_t &into_cnt,
                                  bool &skip_locked)
{
  int ret = OB_SUCCESS;
  OZ (calc_dynamic_sqlstr(ctx, sql_expr, sql_str));
  OZ (prepare_dynamic(ctx, allocator, is_returning, false, param_cnt, sql_str,
                      ps_sql, stmt_type, for_update, hidden_rowid, into_cnt, skip_locked));
  return ret;
}

int ObSPIService::prepare_dynamic(ObPLExecCtx *ctx,
                                  ObIAllocator &allocator,
                                  bool is_returning,
                                  bool is_dbms_sql,
                                  int64_t param_cnt,
                                  ObSqlString &sql_str,
                                  ObString &ps_sql,
                                  stmt::StmtType &stmt_type,
                                  bool &for_update,
                                  bool &hidden_rowid,
                                  int64_t &into_cnt,
                                  bool &skip_locked,
                                  common::ColumnsFieldArray *field_list)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(ctx->allocator_));
  CK (OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(GCTX.sql_engine_));
  stmt_type = stmt::T_NONE;
  OV ((is_dbms_sql && NULL != field_list)
      || (!is_dbms_sql && NULL == field_list),
      OB_ERR_UNEXPECTED, is_dbms_sql, field_list);
  if (OB_SUCC(ret)) {
    PLPrepareCtx pl_prepare_ctx(*session, NULL, true, is_dbms_sql, false);
    SMART_VAR(PLPrepareResult, pl_prepare_result) {
      OZ (pl_prepare_result.init(*session));
      CK (OB_NOT_NULL(pl_prepare_result.result_set_));
      OZ(GCTX.sql_engine_->handle_pl_prepare(sql_str.string(), pl_prepare_ctx, pl_prepare_result));

      OX (stmt_type = static_cast<stmt::StmtType>(pl_prepare_result.result_set_->get_stmt_type()));
      OZ (ob_write_string(allocator, pl_prepare_result.result_set_->get_stmt_ps_sql(), ps_sql, true));
      OX (for_update = pl_prepare_result.result_set_->get_is_select_for_update());
      OX (hidden_rowid = pl_prepare_result.result_set_->has_hidden_rowid());
      OX (into_cnt = pl_prepare_result.result_set_->get_into_exprs().count());
      OX (skip_locked = pl_prepare_result.result_set_->is_skip_locked());

      if (OB_SUCC(ret) && NULL != field_list) {
        CK (OB_NOT_NULL(pl_prepare_result.result_set_->get_field_columns()));
        if (OB_SUCC(ret) && pl_prepare_result.result_set_->get_field_columns()->count() > 0) {
          OZ (ObDbmsInfo::deep_copy_field_columns(
            *(field_list->get_allocator()),
            pl_prepare_result.result_set_->get_field_columns(),
            *field_list));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t exec_param_cnt = ObStmt::is_dml_stmt(stmt_type)
          ? pl_prepare_result.result_set_->get_external_params().count()
            : pl_prepare_result.result_set_->get_param_fields()->count();
        if (pl_prepare_result.result_set_->is_returning() && 0 == into_cnt) {
            ret = OB_ERR_MISSING_INTO_KEYWORD;
            LOG_WARN("OBE-00925: missing INTO keyword", K(ret),
                    K(pl_prepare_result.result_set_->is_returning()), K(into_cnt));
        } else {
          /*!
            * 1、select语句的INTO子句在动态语句里直接丢掉，所以select语句参数个数按传进来的入参个数检查
            * 2、dml语句如果有RETURNING INTO子句，需要去掉动态语句里RETURNING INTO的参数，
            * 但是如果EXECUTE IMMEDIATE本身有RETURNING子句的话就不用去了
            */
          int64_t need_exec_param_cnt = exec_param_cnt;
          if (ObStmt::is_dml_write_stmt(stmt_type)) {
            need_exec_param_cnt = need_exec_param_cnt + (is_returning ? 0 : into_cnt);
          }
          if (param_cnt != need_exec_param_cnt) {
            if (lib::is_mysql_mode()) {
              ret = OB_ERR_WRONG_DYNAMIC_PARAM;
              LOG_USER_ERROR(OB_ERR_WRONG_DYNAMIC_PARAM, exec_param_cnt, param_cnt);
            } else if (param_cnt < need_exec_param_cnt) {
              ret = OB_ERR_NOT_ALL_VARIABLE_BIND;
              LOG_WARN("OBE-01008: not all variables bound",
                        K(ret), K(param_cnt),
                        K(need_exec_param_cnt), K(into_cnt), K(is_returning), K(stmt_type));
            } else {
              ret = OB_ERR_BIND_VARIABLE_NOT_EXIST;
              LOG_WARN("OBE-01006: bind variable does not exist",
                        K(ret), K(param_cnt),
                        K(need_exec_param_cnt), K(into_cnt), K(is_returning), K(stmt_type));
            }
          }
        }
      }

        if (OB_SUCC(ret)) {
          bool remove_into = false;
          if (is_returning) {
            if (stmt_type != stmt::T_INSERT
                && stmt_type != stmt::T_DELETE
                && stmt_type != stmt::T_UPDATE) {
              ret = OB_ERR_CLAUSE_RETURN_ILLEGAL;
              LOG_WARN("OBE-06547: RETURNING clause must be used with "
                      "INSERT, UPDATE, or DELETE statements", K(ret), K(stmt_type));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "RETURNING clause used with not "
                      "INSERT, UPDATE, or DELETE statements");
          } else if (pl_prepare_result.result_set_->get_into_exprs().empty()) {
            ret = OB_ERR_MISSING_INTO_KEYWORD;
            LOG_WARN("OBE-00925: missing INTO keyword", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "missing INTO keyword");
          } else {
            remove_into = true;
          }
        } else if (stmt::T_SELECT == stmt_type || pl_prepare_result.result_set_->is_returning()) {
          /*
            * 动态语句如果是select into，INTO子句会被忽略掉，INTO子句里面占位符的也不需要绑定实参
            * 例如：
            * SQL> DECLARE
            * x int;
            * y int :=1;
            * c SYS_REFCURSOR;
            * BEGIN
            * execute immediate 'select * into :a from t where a1 >1' using IN y;
            * dbms_output.put_line(x);
            * END;
            * /  2    3    4    5    6    7    8    9
            * DECLARE
            * *
            * ERROR at line 1:
            * OBE-01006: bind variable does not exist
            * OBE-06512: at line 6
            * */
          remove_into = !pl_prepare_result.result_set_->get_into_exprs().empty();
        } else { /*do nothing*/ }

        if (OB_SUCC(ret) && remove_into) {
          CK (!pl_prepare_result.result_set_->get_route_sql().empty());
          CK ((pl_prepare_result.result_set_->get_route_sql().length() + 1) < OB_MAX_SQL_LENGTH);
          OX (sql_str.reset());
          OZ (sql_str.append(pl_prepare_result.result_set_->get_route_sql()));
        }
      }
    }
  }

  return ret;
}

int ObSPIService::dynamic_out_params(
  common::ObIAllocator &allocator, ObResultSet *result_set, void* params, int64_t param_count, bool is_dbms_sql)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  const ParamStore *param_store = NULL;
  if (param_count > 0) {
    CK (OB_NOT_NULL(params));
    CK (OB_NOT_NULL(result_set));
    CK (OB_NOT_NULL(
      plan_ctx = result_set->get_exec_context().get_physical_plan_ctx()));
    CK (OB_NOT_NULL(param_store = &(plan_ctx->get_param_store())));
    OV (param_store->count() >= param_count,
        OB_ERR_UNEXPECTED, param_store->count(), param_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      // memory of params_store from result set will released by result close.
      // so we need to deep copy it.
      ObObjParam &obj = is_dbms_sql ? reinterpret_cast<ParamStore *>(params)->at(i)
                                    : *(reinterpret_cast<ObObjParam**>(params)[i]);
      if (param_store->at(i).is_pl_extend()
          && param_store->at(i).get_meta().get_extend_type() != PL_CURSOR_TYPE
          && param_store->at(i).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
        OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, param_store->at(i), obj, true));
      } else {
        OZ (deep_copy_objparam(allocator, param_store->at(i), obj));
      }
    }
  }
  return ret;
}


int ObSPIService::check_dynamic_sql_legal(ObPLExecCtx *ctx,
                                          ObIAllocator &alloc,
                                          ObSqlString &sql_str,
                                          stmt::StmtType stmt_type,
                                          int64_t into_count,
                                          int64_t inner_into_cnt,
                                          common::ObObjParam **params,
                                          int64_t param_count,
                                          const int64_t *params_mode,
                                          bool is_returning,
                                          bool for_update,
                                          int64_t &exec_param_cnt,
                                          ObIArray<ObObjParam*> &out_using_params)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (ObStmt::is_ddl_stmt(stmt_type, false)
        && (into_count > 0 || param_count > 0 || is_returning)) {
      ret = OB_ERR_DDL_IN_ILLEGAL_CONTEXT;
      LOG_WARN("DDL statement is executed in an illegal context",
                K(ret), K(into_count), K(param_count), K(is_returning));
    } else if (ObStmt::is_dml_write_stmt(stmt_type) && inner_into_cnt > 0 && 0 == into_count) {
      /*
      * 处理
      * 仅当dml语句含returning变量，并且外部没有INTO变量时才允许使用USING OUT接收参数
      */
      CK (param_count >= inner_into_cnt);
      OX (exec_param_cnt = param_count - inner_into_cnt);
      for (int64_t i = exec_param_cnt; OB_SUCC(ret) && i < param_count; ++i) {
        ObPLRoutineParamMode pm = static_cast<ObPLRoutineParamMode>(params_mode[i]);
        if (PL_PARAM_INOUT == pm || PL_PARAM_OUT == pm) {
          OZ (out_using_params.push_back(params[i]));
        } else {
          ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
          LOG_WARN("OBE-06536: IN bind variable bound to an OUT position", K(ret));
        }
      }
    } else if (ObStmt::is_dml_write_stmt(stmt_type) && inner_into_cnt > 0 && into_count > 0 && !is_returning) {
      CK (param_count >= inner_into_cnt);
      for (int64_t i = param_count - inner_into_cnt; OB_SUCC(ret) && i < param_count; ++i) {
        ObPLRoutineParamMode pm = static_cast<ObPLRoutineParamMode>(params_mode[i]);
        if (PL_PARAM_IN == pm) {
          ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
          LOG_WARN("OBE-06536: IN bind variable bound to an OUT position", K(ret));
        }
      }
    } else { /*do nothing*/ }
  }

  for (int i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    ObPLRoutineParamMode pm = static_cast<ObPLRoutineParamMode>(params_mode[i]);
    if (ObStmt::is_select_stmt(stmt_type) && (PL_PARAM_INOUT == pm || PL_PARAM_OUT == pm)) {
      ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
      LOG_WARN("select stmt with using out/inout param mode is not allowed", K(ret));
    }
    if (ObStmt::is_dml_write_stmt(stmt_type)
        && i < param_count - inner_into_cnt
        && PL_PARAM_OUT == pm) {
      ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
      LOG_WARN("using out/inout param mode is not allowed", K(ret));
    } else if (ObStmt::is_dml_write_stmt(stmt_type) &&
              into_count > 0 && !is_returning &&
              (PL_PARAM_INOUT == pm || PL_PARAM_OUT == pm)) {
      ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
      LOG_WARN("using out/inout param mode is not allowed", K(ret));
    }
    if (OB_SUCC(ret) && ObStmt::is_dml_stmt(stmt_type)) {
      if (PL_PARAM_IN == pm &&
          NULL != params[i] &&
          params[i]->is_pl_extend() &&
          PL_RECORD_TYPE == params[i]->get_meta().get_extend_type()) {
        const ObUserDefinedType *user_type = NULL;
        ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(params[i]->get_ext());
        CK (OB_NOT_NULL(composite));
        OZ (ctx->get_user_type(composite->get_id(), user_type));
        CK (OB_NOT_NULL(user_type));
        if (OB_SUCC(ret) && user_type->is_type_record()) {
          ret = OB_ERR_EXPR_SQL_TYPE;
          LOG_WARN("expressions have to be of SQL types", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObParser parser(alloc, STD_MODE);
    ObMPParseStat parse_stat;
    ObSEArray<ObString, 1> queries;
    OZ (parser.split_multiple_stmt(sql_str.string(), queries, parse_stat));
    if (OB_SUCC(ret) && queries.count() > 1) {
      ret = OB_ERR_CMD_NOT_PROPERLY_ENDED;
      LOG_WARN("execute immdeidate only support one stmt", K(ret));
    }
  }
  return ret;
}

int ObSPIService::prepare_dynamic_sql_params(ObPLExecCtx *ctx,
                                             ObSPIResultSet &spi_result,
                                             ObIAllocator &allocator,
                                             int64_t exec_param_cnt,
                                             ObObjParam **params,
                                             ParamStore *&exec_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_params = reinterpret_cast<ParamStore*>(allocator.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for exec params", K(ret));
  } else {
    new (exec_params) ParamStore((ObWrapperAllocator(allocator)));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_param_cnt; ++i) {
    CK (OB_NOT_NULL(params[i]));
    if (OB_SUCC(ret)) {
      ObObjParam new_param = *params[i];
      if (params[i]->is_pl_extend()) {
        if (params[i]->get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
          new_param.set_int_value(0);
          if (!ob_is_xml_pl_type(params[i]->get_type(), params[i]->get_udt_id())) {
            OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, *params[i], new_param, true));
          } else {
            uint64_t udt_id = params[i]->get_udt_id();
            const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(ctx->exec_ctx_->get_my_session());
            ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, ObCharset::get_system_collation());
            cast_ctx.exec_ctx_ = ctx->exec_ctx_;
            uint16_t subschema_id = ObInvalidSqlType;
            if (OB_FAIL(cast_ctx.exec_ctx_->get_subschema_id_by_udt_id(udt_id,
                                                                       subschema_id,
                                                                       &spi_result.get_scheme_guard()))) {
              LOG_WARN("failed to get subschema id by udt_id", K(ret), K(params[i]));
            } else if (FALSE_IT(new_param.set_subschema_id(subschema_id))) {
            } else if (OB_FAIL(ObObjCaster::to_type(ObUserDefinedSQLType, cast_ctx, *params[i], new_param))) {
              LOG_WARN("failed to_type", K(ret), K(new_param));
            } else { // sql udt params need original udt id for plan choose
              new_param.set_udt_id(udt_id);
              new_param.set_param_meta(); // param meta also changed
            }
          }
        }
      } else {
        OZ (deep_copy_obj(allocator, *params[i], new_param));
      }
      OX (new_param.set_need_to_check_type(true));
      OZ (exec_params->push_back(new_param), K(new_param));
    }
  }
  OZ (store_params_string(ctx, spi_result, exec_params));
  return ret;
}

int ObSPIService::spi_execute_immediate(ObPLExecCtx *ctx,
                                        const int64_t sql_idx,
                                        common::ObObjParam **params,
                                        const int64_t *params_mode,
                                        int64_t param_count,
                                        const int64_t *into_exprs_idx,
                                        int64_t into_count,
                                        const ObDataType *column_types,
                                        int64_t type_count,
                                        const bool *exprs_not_null_flag,
                                        const int64_t *pl_integer_ranges,
                                        bool is_bulk,
                                        bool is_returning,
                                        bool is_type_record)
{
  int ret = OB_SUCCESS;


  ObSQLSessionInfo *session = NULL;
  ObSqlString sql_str;
  const ObSqlExpression *sql = nullptr;
  const ObSqlExpression **into_exprs = nullptr;

  ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_DYNAMIC_SQL_EXEC), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  stmt::StmtType stmt_type = stmt::T_NONE;
  ObString ps_sql;
  bool for_update = false;
  bool hidden_rowid = false;
  bool skip_locked = false;
  int64_t inner_into_cnt = 0; //动态语句里into子句的变量个数
  ObArray<ObObjParam*> out_using_params;
  int64_t exec_param_cnt = param_count;

  stmt::StmtType saved_stmt_type = stmt::T_NONE;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(ctx->func_));
  CK ((OB_NOT_NULL(params) && param_count > 0) || (OB_ISNULL(params) && 0 == param_count));
  CK (OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()));
  CK (sql_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= sql_idx && sql_idx < ctx->func_->get_expressions().count()));
  OX (sql = ctx->func_->get_expressions().at(sql_idx));
  CK (OB_NOT_NULL(sql));
  CK (OB_NOT_NULL(GCTX.sql_engine_));
  OX (saved_stmt_type = session->get_stmt_type());

  MAKE_EXPR_BUFFER(allocator, into_exprs_idx, into_count, into_exprs);

  // Step: Prepare dynamic SQL! Only prepare once!
  OZ (prepare_dynamic(ctx,
                      sql,
                      allocator,
                      is_returning,
                      param_count,
                      sql_str,
                      ps_sql,
                      stmt_type,
                      for_update,
                      hidden_rowid,
                      inner_into_cnt,
                      skip_locked));

  OZ (check_dynamic_sql_legal(ctx,
                      allocator,
                      sql_str,
                      stmt_type,
                      into_count,
                      inner_into_cnt,
                      params,
                      param_count,
                      params_mode,
                      is_returning,
                      for_update,
                      exec_param_cnt,
                      out_using_params));

  OX (session->set_stmt_type(saved_stmt_type));

  // Step: execute dynamic SQL now!
  if (OB_FAIL(ret)) {
  } else if (ObStmt::is_select_stmt(stmt_type) && !for_update && into_count <= 0) {
    /*!
     * If dynamic_sql_statement is a SELECT statement, and you omit both
     * into_clause and bulk_collect_into_clause, then
     * execute_immediate_statement never executes.
     * For example, this statement never increments the sequence:
     * EXECUTE IMMEDIATE 'SELECT S.NEXTVAL FROM DUAL'
     */
    ObPLCursorInfo *implicit_cursor = session->get_pl_implicit_cursor();
    CK (OB_NOT_NULL(implicit_cursor));
    OX (implicit_cursor->set_rowcount(0));
  } else {
    OZ (SMART_CALL(spi_inner_execute(ctx,
                                     ctx->expr_alloc_,
                                     sql_str.ptr(),
                                     ps_sql.ptr(),
                                     stmt_type,
                                     params,
                                     exec_param_cnt,
                                     into_exprs,
                                     into_count,
                                     column_types,
                                     type_count,
                                     exprs_not_null_flag,
                                     pl_integer_ranges,
                                     is_bulk,
                                     false, /*is_forall*/
                                     is_type_record,
                                     for_update,
                                     true, /*is_dynamic_sql*/
                                     &out_using_params)));
  }
  return ret;
}

int ObSPIService::spi_cursor_alloc(ObIAllocator &allocator, ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor = NULL;
  if (OB_ISNULL(cursor =
        static_cast<ObPLCursorInfo*>(allocator.alloc(sizeof(ObPLCursorInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mysqlresult", K(ret));
  } else {
    new(cursor) ObPLCursorInfo(&allocator);
    LOG_DEBUG("cursor alloc, local cursor", K(cursor));
    cursor->reset();
    cursor->set_ref_count(1);
    obj.set_extend(reinterpret_cast<int64_t>(cursor), PL_CURSOR_TYPE);
  }
  return ret;
}

int ObSPIService::spi_cursor_init(ObPLExecCtx *ctx, int64_t cursor_index)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(ctx));
  CK(OB_NOT_NULL(ctx->allocator_));
  CK (OB_NOT_NULL(ctx->params_));
  CK (cursor_index >= 0 && cursor_index < ctx->params_->count());
  if (OB_SUCC(ret)) {
    ObObjParam &obj = ctx->params_->at(cursor_index);
    LOG_DEBUG("spi cursor init", K(cursor_index), K(obj), K(obj.is_ref_cursor_type()), K(obj.is_null()));
    // ref cursor is pointer to cursor, we don't have to alloc here
    // we should alloc it in open stmt
    if (obj.is_ref_cursor_type()) {
      if (!obj.is_null()) {
        ObPLCursorInfo *cursor_info = NULL;
        CK (obj.is_pl_extend());
        CK (PL_REF_CURSOR_TYPE == obj.get_meta().get_extend_type());
        if (OB_SUCC(ret)
            && obj.get_ext() != 0
            && OB_NOT_NULL(cursor_info = reinterpret_cast<ObPLCursorInfo *>(obj.get_ext()))) {
          CK (!cursor_info->isopen());
          CK (0 == cursor_info->get_ref_count());
        }
      } else {
        // init as null
        obj.set_extend(static_cast<int64_t>(0), PL_REF_CURSOR_TYPE);
        obj.set_param_meta();
      }
    } else {
      if (obj.is_null()) {
        //OZ (spi_cursor_alloc(*ctx->allocator_, obj));
        OZ (spi_cursor_alloc(ctx->exec_ctx_->get_allocator(), obj));
      }
    }
  }
  return ret;
}

// we have to check when cursor is opened.
// we have two case here need to be handled.
/*
* 1. cursor is local cursor, which mean it is alloc ObPLCursorInfo when in spi_cursor_init
* in this case, param 'cursor' is not null, wo don't have to do anything
* 2. cursor is a ref cursor, in this case, ObPLCursorInfo is not alloc during cursor init
* so we have to alloc it. may local or session according to how this cursor is using. it may
* also reopened, see the case below
*/
int ObSPIService::cursor_open_check(ObPLExecCtx *ctx,
                                        int64_t package_id,
                                        int64_t routine_id,
                                        int64_t cursor_index,
                                        ObPLCursorInfo *&cursor,
                                        ObObjParam &obj,
                                        ObCusorDeclareLoc loc)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(exec_ctx = ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = exec_ctx->get_my_session()));
  LOG_DEBUG("spi cursor open check", K(cursor_index), K(package_id), K(obj.is_ref_cursor_type()),
                                     K(routine_id), K(cursor), K(obj), K(loc));
  // ObObjParam &obj = ctx->params_->at(cursor_index);
  // reopen branch,
  if (OB_NOT_NULL(cursor)) {
    if (obj.is_ref_cursor_type()) {
      // reopen a ref cursor, reinit the cursor info.
      /* for example:
      * a sys_refcursor;
      * a := func(); // a is open as a return value
      * open a for select * from table; // a is reopened
      * cursor is closed before open, so we remake a cursor
      * note it will reuse cursor memory if it is not null
      */
      int64_t ref_cnt = cursor->get_ref_count();
      if (cursor->isopen()) {
        CK (0 < ref_cnt);
      } else {
        // 这儿为啥这么干， 见下面的例子
        /*
        * declare
          c1 sys_refcursor;
          c2 sys_refcursor;
          c3 sys_refcursor;
          c4 sys_refcursor;
          c5 sys_refcursor;
          a number;
          begin
          open c1 for select 6 from dual;
          c2 := c1;
          c3 := c1;
          c4 := c1;  -- c1, c2, c3, c4的ref count是4
          fetch c1 into a;
          dbms_output.put_line(a);
          a := 0;
          fetch c1 into a;
          dbms_output.put_line(a);
          a := 0;
          close c2; -- 虽然close，但是对于的cursor info的ref count还是4
          open c2 for select 8 from dual;
          fetch c4 into a;
          dbms_output.put_line(a);
          a := 0;
          open c5 for select 9 from dual; --c5 ref count 1
          fetch c5 into a;
          dbms_output.put_line(a);
          a := 0;
          c4 := c5;  -- c4的ref count是4， 所以赋值之后，c4原来指向的cursor ref count为3,
          fetch c4 into a; --c4, c5 的ref count是2
          dbms_output.put_line(a);
          a := 0;
          fetch c2 into a; -- ref count是3， 分别是c1, c2, c3
          dbms_output.put_line(a);
          a := 0;
          close c4;
          end;
          * 所以只要ref count不是1， 关闭的时候要保持这个ref count，重新open，这个ref count需要恢复
        */
        ref_cnt = (ref_cnt == 0) ? 1 : ref_cnt;
      }
      if (OB_SUCC(ret)) {
        // 理论上reopen的时候， cg openfor 的时候，已经close了，保险起见，这儿再close一次
        // OZ (cursor_close_impl(ctx, cursor, true, OB_INVALID_ID, OB_INVALID_ID, true));
        /* OB cursor close 逻辑
         *  1. 释放数据部分（spi_result/spi_cursor）， 其余部分做 reset
         *  2. 尽管 cursor 做了 close， obj.get_ext 的结果仍然是原来 cursor 的地址
         * OB cursor reopen 逻辑 （不包含 ps cursor）
         *  1. spi_open_cursor 时，会判断一下 obj.get_ext 的结果是不是空
         *    a. 为空，说明第一次 open , 直接分配内存即可
         *    b. 不为空，说明是 reopen，一般不需要重新分配内存
         *  2. reopen 时需要重新设置 session_cursor， session_cursor 内存分配位置有别于 local_cursor, 所以这个标记需要设置
         *    a. 此时不需要重新 make_cursor , 内存已经分配过了， cursor_close_impl 里的 close 并没有把 cursor 从 session 上摘掉
         *    b. 这个值以前没有设置成功为什么也没有出问题？ 因为 spi 中 server cursor 的判断都是使用 cursor_id 做判断
         *       server cursor 的 close 全部都使用 reuse， cursor_id 不会被 清空， 所以暂时没有出现问题
        */
        if (cursor->is_session_cursor()) {
          //OZ (session->make_cursor(cursor));
          OX (cursor->set_is_session_cursor());
        } else {
          // local ref cursor, just reset
          cursor->reuse();
        }
        OX (cursor->set_ref_by_refcursor());
        OX (cursor->set_ref_count(ref_cnt));
      }
    } else {
      // normal cursor, do nothing
    }
  } else {
    if (obj.is_ref_cursor_type()) {
      if (ctx->func_->should_init_as_session_cursor()) {
        OZ (session->make_cursor(cursor));
        CK (OB_NOT_NULL(cursor));
        OX (cursor->set_is_session_cursor());
        OX (obj.set_extend(reinterpret_cast<int64_t>(cursor), PL_REF_CURSOR_TYPE));
        OX (obj.set_param_meta());
      } else {
        CK (OB_NOT_NULL(ctx->allocator_));
        //OZ (spi_cursor_alloc(*ctx->allocator_, obj));
        OZ (spi_cursor_alloc(exec_ctx->get_allocator(), obj));
        OX (obj.set_extend(obj.get_ext(), PL_REF_CURSOR_TYPE));
        OX (cursor = reinterpret_cast<ObPLCursorInfo*>(obj.get_ext()));
      }
      OX (cursor->set_ref_by_refcursor());
      OX (cursor->set_ref_count(1));
      // update subprogram local cursor var
      // 这个的更新是因为subprogram的cursor变量保存在它自己的栈中，这儿需要去更新这个值，使他指向新开的cursor内存
      // local变量，直接拷贝一份新的内存值。
      // package变量不需要，因为package中不会定义ref cursor。
      if (DECL_SUBPROG == loc) {
        OZ (spi_set_subprogram_cursor_var(ctx, package_id, routine_id, cursor_index, obj));
      } else if (DECL_LOCAL == loc) {
        OX (obj.copy_value_or_obj(ctx->params_->at(cursor_index), true));
        OX (ctx->params_->at(cursor_index).set_param_meta());
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cursor not inited, but it is not ref cursor", K(ret), K(lbt()));
    }
  }
  return ret;
}

int ObSPIService::spi_get_package_cursor_info(ObPLExecCtx *ctx,
                                              uint64_t package_id,
                                              uint64_t routine_id,
                                              int64_t index,
                                              ObPLCursorInfo *&cursor,
                                              ObObjParam &param)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  ObPL *pl_engine = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  UNUSED(routine_id);
  cursor = NULL;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(GCTX.schema_service_));
  CK (OB_NOT_NULL(exec_ctx = ctx->exec_ctx_));
  CK (OB_NOT_NULL(session_info = exec_ctx->get_my_session()));
  CK (OB_NOT_NULL(sql_proxy = exec_ctx->get_sql_proxy()));
  CK (OB_NOT_NULL(pl_engine = exec_ctx->get_my_session()->get_pl_engine()));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(
                            session_info->get_effective_tenant_id(),
                            schema_guard));
  ObPLPackageGuard package_guard(session_info->get_effective_tenant_id());
  OZ (package_guard.init());
  if (OB_SUCC(ret)) {
    ObObj value;
    ObPLPackageManager &pl_manager = pl_engine->get_package_manager();
    ObPLResolveCtx resolve_ctx(exec_ctx->get_allocator(),
                               *session_info,
                               schema_guard,
                               ctx->guard_ != NULL ? *(ctx->guard_) : package_guard,
                               *sql_proxy,
                               false);
    OZ (pl_manager.get_package_var_val(
      resolve_ctx, *exec_ctx, package_id, OB_INVALID_VERSION, OB_INVALID_VERSION, index, value));
    CK (value.is_ext() || value.is_null());
    OX (value.copy_value_or_obj(param,true));
    OX (cursor = value.is_ext() ? reinterpret_cast<ObPLCursorInfo*>(value.get_ext())
                                : reinterpret_cast<ObPLCursorInfo *>(NULL));
    if (OB_SUCC(ret) && OB_NOT_NULL(cursor) && cursor->is_sync_cursor()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("use package cursor in remote server is not supported,"
               "because we can not sync cursor state to other server for now!",
               K(ret), K(package_id), K(routine_id), K(index), KPC(cursor));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "use package cursor in remote server");
    }
    if (param.is_ref_cursor_type()) {
      // do nothing, cursor may null;
    } else {
      // 存在null的可能，主要是exception导致的执行流变化，在exception中fetch或者close就会取到null
      // CK (OB_NOT_NULL(cursor));
    }
  }
  return ret;
}

int ObSPIService::spi_get_subprogram_cursor_info(ObPLExecCtx *ctx,
                                                 uint64_t package_id,
                                                 uint64_t routine_id,
                                                 int64_t index,
                                                 ObPLCursorInfo *&cursor,
                                                 ObObjParam &param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObPLContext *context = NULL;
  ObObjParam result;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(context = session_info->get_pl_context()));
  OZ (context->get_subprogram_var_from_local(
    *session_info, package_id, routine_id, index, result));
  CK (result.is_ext() || result.is_null());
  // OX (param.copy_value_or_obj(result, true));
  OX (param = result);
  OX (cursor = result.is_ext() ? reinterpret_cast<ObPLCursorInfo *>(result.get_ext())
                                : reinterpret_cast<ObPLCursorInfo*>(NULL));
  if (param.is_ref_cursor_type()) {
    // may be null if is ref cursor
    // do nothing
  } else {
    // 存在null的可能，主要是exception导致的执行流变化，在exception中fetch或者close就会取到null
    // CK (OB_NOT_NULL(cursor));
  }
  return ret;
}

int ObSPIService::spi_set_subprogram_cursor_var(ObPLExecCtx *ctx,
                                                uint64_t package_id,
                                                uint64_t routine_id,
                                                int64_t index,
                                                ObObjParam &param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObPLContext *context = NULL;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(context = session_info->get_pl_context()));
  OZ (context->set_subprogram_var_from_local(*session_info, package_id, routine_id, index, param));
  return ret;
}

int ObSPIService::spi_get_cursor_info(ObPLExecCtx *ctx,
                                      uint64_t package_id,
                                      uint64_t routine_id,
                                      int64_t index,
                                      ObPLCursorInfo *&cursor,
                                      ObObjParam &param,
                                      ObSPIService::ObCusorDeclareLoc &location)
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  CK (OB_NOT_NULL(ctx) && ctx->valid());
  if (OB_FAIL(ret)) {
  } else if (package_id != OB_INVALID_ID && OB_INVALID_ID == routine_id) {
    OZ (spi_get_package_cursor_info(ctx, package_id, routine_id, index, cursor, param));
    OX (location = DECL_PKG);
  } else {
    if (ctx->func_->get_routine_id() == routine_id && ctx->func_->get_package_id() == package_id) {
      OZ (spi_get_cursor_info(ctx, index, cursor, param));
      OX (location = DECL_LOCAL);
    } else {
      OZ (spi_get_subprogram_cursor_info(ctx, package_id, routine_id, index, cursor, param),
        routine_id, package_id, index);
      OX (location = DECL_SUBPROG);
    }
  }
  return ret;
}

int ObSPIService::spi_get_cursor_info(ObPLExecCtx *ctx, int64_t index,
                                      ObPLCursorInfo *& cursor,
                                      ObObjParam &param)
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  ObObjParam *obj;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->params_));
  if (OB_SUCC(ret) && (OB_UNLIKELY(index < 0 || index >= ctx->params_->count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cursor index", K(ret), K(index), K(ctx->params_->count()));
  }
  OX (obj = &(ctx->params_->at(index)));
  // 如果cursor没有init，它就是null type, 例如exception导致执行流的转变，或者goto导致的执行流变化
  // 导致没有open，直接去fetch或者close
  CK (obj->is_ext() || obj->is_null());
  OX (param = *obj);
  OX (cursor = obj->is_ext() ? reinterpret_cast<ObPLCursorInfo *>(obj->get_ext())
                         : reinterpret_cast<ObPLCursorInfo *>(NULL));
  if (param.is_ref_cursor_type()) {
    // do nothing, ref cursor may null;
  } else {
    // CK (OB_NOT_NULL(cursor));
  }
  return ret;
}

int ObSPIService::spi_dynamic_open(ObPLExecCtx *ctx,
                                  const int64_t sql_idx,
                                  const int64_t *sql_param_exprs_idx,
                                  int64_t sql_param_count,
                                  uint64_t package_id,
                                  uint64_t routine_id,
                                  int64_t cursor_index)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  stmt::StmtType stmt_type = stmt::T_NONE;
  ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_DYNAMIC_SQL_EXEC), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObString ps_sql;
  bool for_update = false;
  bool hidden_rowid = false;
  int64_t inner_into_cnt = 0;
  bool skip_locked = false;

  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->func_));
  CK (sql_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= sql_idx && sql_idx < ctx->func_->get_expressions().count()));

  OZ (prepare_dynamic(ctx,
                      ctx->func_->get_expressions().at(sql_idx),
                      allocator,
                      false/*not returning*/,
                      sql_param_count,
                      sql_str,
                      ps_sql,
                      stmt_type,
                      for_update,
                      hidden_rowid,
                      inner_into_cnt,
                      skip_locked));

  if (OB_SUCC(ret)) {
    const ObSqlExpression **sql_param_exprs = nullptr;
    ObArenaAllocator alloc("SpiTemp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

    MAKE_EXPR_BUFFER(alloc, sql_param_exprs_idx, sql_param_count, sql_param_exprs);

    OZ (spi_cursor_open(ctx,
                        sql_param_count > 0 ? NULL : sql_str.ptr(),
                        ps_sql.ptr(),//trans to c-stype
                        stmt_type,
                        for_update,
                        hidden_rowid,
                        sql_param_exprs,
                        sql_param_count,
                        package_id,
                        routine_id,
                        cursor_index,
                        NULL/*formal_param_idxs*/,
                        NULL/*actual_param_exprs*/,
                        0/*cursor_param_count*/,
                        skip_locked));
  }

  return ret;
}

int ObSPIService::dbms_dynamic_open(ObPLExecCtx *pl_ctx,
                                    ObDbmsCursorInfo &cursor,
                                    bool is_dbms_sql,
                                    int64_t orc_max_ret_rows)
{
  int ret = OB_SUCCESS;

  stmt::StmtType stmt_type = cursor.get_stmt_type();
  const ObString ps_sql = cursor.get_ps_sql();
  bool for_update = cursor.is_for_update();
  bool hidden_rowid = cursor.has_hidden_rowid();

  OV (OB_NOT_NULL(pl_ctx->exec_ctx_->get_my_session()));
  if (ObStmt::is_select_stmt(stmt_type)
      || cursor.get_into_names().count() > 0) { // NOTICE: DML Returning also use cursor impl.
    OZ (dbms_cursor_open(
            pl_ctx, cursor, ps_sql, stmt_type, for_update, hidden_rowid, orc_max_ret_rows),
        cursor);
  } else {
    OZ (dbms_cursor_execute(pl_ctx, ps_sql, stmt_type, cursor, is_dbms_sql), cursor);
    if (OB_SUCC(ret) && cursor.get_exec_params().count() > 0) {
      ObIAllocator &alloc = cursor.get_dbms_entity()->get_arena_allocator();
      for (int64_t i = 0; i < cursor.get_exec_params().count(); ++i) {
        ObObjParam &obj = cursor.get_exec_params().at(i);
        if (obj.is_pl_extend()
            && obj.get_meta().get_extend_type() != PL_CURSOR_TYPE
            && obj.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(alloc, obj, obj, true));
        } else {
          OZ (deep_copy_obj(alloc, obj, obj));
        }
      }
    }
    OX (cursor.set_affected_rows(pl_ctx->exec_ctx_->get_my_session()->get_affected_rows()));
  }
  return ret;
}

int ObSPIService::prepare_cursor_parameters(ObPLExecCtx *ctx,
                                            ObSQLSessionInfo &session_info,
                                            uint64_t package_id,
                                            uint64_t routine_id,
                                            ObCusorDeclareLoc loc,
                                            const int64_t *formal_param_idxs,
                                            const ObSqlExpression **actual_param_exprs,
                                            int64_t cursor_param_count)
{
  int ret = OB_SUCCESS;

  ObObjParam dummy_result;

  for (int64_t i = 0; OB_SUCC(ret) && i < cursor_param_count; ++i) {

    CK (OB_NOT_NULL(actual_param_exprs[i]));
    OX (dummy_result.reset());
    OX (dummy_result.ObObj::reset());
    OZ (spi_calc_expr(ctx, actual_param_exprs[i], OB_INVALID_INDEX, &dummy_result),
                K(i), K(cursor_param_count), KPC(actual_param_exprs[i]), K(dummy_result));

    if (OB_SUCC(ret) && dummy_result.is_pl_mock_default_param()) {
      int64_t idx = dummy_result.get_int();
      ObSqlExpression *actual_param_expr = NULL;
      dummy_result.reset();
      dummy_result.ObObj::reset();
      if (DECL_PKG == loc) {
        OZ (spi_calc_package_expr(ctx, package_id, idx, &dummy_result));
      } else {
        OZ (spi_calc_subprogram_expr(ctx, package_id, routine_id, idx, &dummy_result));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (DECL_PKG == loc) {
      OZ (spi_set_package_variable(ctx, package_id, formal_param_idxs[i], dummy_result));
    } else {
      OZ (ObPLContext::set_subprogram_var_from_local(
        session_info, package_id, routine_id, formal_param_idxs[i], dummy_result));
    }
  }

  return ret;
}

int ObSPIService::spi_cursor_open_with_param_idx(ObPLExecCtx *ctx,
                                  const char *sql,
                                  const char *ps_sql,
                                  int64_t type,
                                  bool for_update,
                                  bool has_hidden_rowid,
                                  const int64_t *sql_param_idx,
                                  int64_t sql_param_count,
                                  uint64_t package_id,
                                  uint64_t routine_id,
                                  int64_t cursor_index,
                                  const int64_t *formal_param_idxs,
                                  const int64_t *actual_param_idx,
                                  int64_t cursor_param_count,
                                  bool skip_locked)
{
  int ret = OB_SUCCESS;

  const ObSqlExpression **sql_param_exprs = nullptr;
  const ObSqlExpression **actual_param_exprs = nullptr;
  ObArenaAllocator alloc("SpiTemp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  MAKE_EXPR_BUFFER(alloc, sql_param_idx, sql_param_count, sql_param_exprs);
  MAKE_EXPR_BUFFER(alloc, actual_param_idx, cursor_param_count,
                    actual_param_exprs);

  OZ(spi_cursor_open(ctx, sql, ps_sql, type, for_update, has_hidden_rowid,
                      sql_param_exprs, sql_param_count, package_id, routine_id,
                      cursor_index, formal_param_idxs, actual_param_exprs,
                      cursor_param_count, skip_locked));

  return ret;
}

int ObSPIService::streaming_cursor_open(ObPLExecCtx *ctx,
                                        ObPLCursorInfo &cursor,
                                        ObSQLSessionInfo &session_info,
                                        const ObString &sql,
                                        const ObString &ps_sql,
                                        int64_t type,
                                        void *params,
                                        int64_t sql_param_count,
                                        bool is_server_cursor,
                                        bool for_update,
                                        bool has_hidden_rowid,
                                        bool is_dbms_cursor)
{
  int ret = OB_SUCCESS;

  cursor.set_streaming();
  ObSPIResultSet *spi_result = NULL;
  ObString ps_sql_copy;

  OZ (cursor.prepare_spi_result(ctx, spi_result));
  CK (OB_NOT_NULL(spi_result));
  CK (OB_NOT_NULL(spi_result->get_memory_ctx()));
  OZ (spi_result->start_cursor_stmt(ctx, static_cast<stmt::StmtType>(type), for_update));

  // in a streaming cursor, lifetime of ps_sql may be shorter than the cursor itself,
  // so we need to open it with a deep copy.
  OZ (ob_write_string(spi_result->get_allocator(), ps_sql, ps_sql_copy));

  if (OB_SUCC(ret)) {

    ObPLSqlAuditRecord audit_record(sql::PLSql);
    ObQueryRetryCtrl retry_ctrl;
    ObSPIExecEnvGuard env_guard(session_info, *spi_result, cursor.is_ps_cursor());

    do {
      {
        ObPLSubPLSqlTimeGuard guard(ctx);
        ObPLSPITraceIdGuard trace_id_guard(sql, ps_sql, session_info, ret);
        ObPLSqlAuditGuard audit_guard(
          *(ctx->exec_ctx_), session_info, *spi_result, audit_record, ret, (sql != NULL ? sql : ps_sql), retry_ctrl, trace_id_guard, static_cast<stmt::StmtType>(type));
        ObSPIRetryCtrlGuard retry_guard(retry_ctrl, *spi_result, session_info, ret);

        if (OB_FAIL(ret)) {
        } else if (is_server_cursor) {
          WITH_CONTEXT(cursor.get_cursor_entity()) {
            lib::ContextTLOptGuard guard(false);
            OZ (inner_open(ctx,
                           spi_result->get_memory_ctx()->get_arena_allocator(),
                           sql,
                           ps_sql_copy,
                           type,
                           params,
                           sql_param_count,
                           nullptr,
                           0,
                           *spi_result,
                           spi_result->get_out_params(),
                           false, /*is_forall*/
                           false, /*is_dynamic_sql*/
                           is_dbms_cursor /*is_dbms_sql*/));
          }
        } else {
          OZ (inner_open(ctx,
                         spi_result->get_memory_ctx()->get_arena_allocator(),
                         sql,
                         ps_sql_copy,
                         type,
                         params,
                         sql_param_count,
                         nullptr,
                         0,
                         *spi_result,
                         spi_result->get_out_params(),
                         false, /*is_forall*/
                         false, /*is_dynamic_sql*/
                         is_dbms_cursor /*is_dbms_sql*/));
        }
        CK (OB_NOT_NULL(spi_result->get_result_set()->get_field_columns()));
        if (OB_SUCC(ret) && is_dbms_cursor) {
          ObDbmsCursorInfo &dbms_cursor = static_cast<ObDbmsCursorInfo&>(cursor);
          if (dbms_cursor.get_field_columns().empty()) {
            const common::ColumnsFieldArray* field_column =
              dynamic_cast<const common::ColumnsFieldArray *>
                (spi_result->get_result_set()->get_field_columns());
            OX (dbms_cursor.get_field_columns().set_allocator(&dbms_cursor.get_dbms_entity()->get_arena_allocator()));
            OZ (dbms_cursor.get_field_columns().assign(*field_column));
          }
        }
        if (OB_SUCC(ret) && OB_INVALID_ID != cursor.get_id()) {
          //如果是客户端游标，设置结果集为二进制模式
          OX (spi_result->get_result_set()->set_ps_protocol());
        }
        OX (cursor.open(spi_result));
        OX (for_update ? cursor.set_for_update() : (void)NULL);
        OX (for_update ? cursor.set_trans_id(session_info.get_tx_id()) : (void)NULL);
        OX (has_hidden_rowid ? cursor.set_hidden_rowid() : (void)NULL);
        OZ (setup_cursor_snapshot_verify_(&cursor, spi_result));
        if (!cursor.is_ps_cursor()) {
          retry_guard.test();
        }
        if (OB_FAIL(ret)) {
          int close_ret = spi_result->close_result_set();
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("close mysql result set failed", K(ret), K(close_ret));
          }
        }
      }
    } while (RETRY_TYPE_NONE != retry_ctrl.get_retry_type());
    spi_result->end_cursor_stmt(ctx, ret);
    cursor.set_last_execute_time(ObTimeUtility::current_time());
  }
  // if cursor already open, spi_result will released by cursor close.
  // there is not a situation like '!cursor.is_open && spi_result not close'.
  // so do not need call spi_result.close_result_set.
  if (OB_FAIL(ret) && OB_NOT_NULL(spi_result) && !cursor.isopen()) {
    spi_result->~ObSPIResultSet();
  }
  return ret;
}

int ObSPIService::unstreaming_cursor_open(ObPLExecCtx *ctx,
                                          ObPLCursorInfo &cursor,
                                          ObSQLSessionInfo &session_info,
                                          const ObString &sql,
                                          const ObString &ps_sql,
                                          int64_t type,
                                          void *params,
                                          int64_t sql_param_count,
                                          bool is_server_cursor,
                                          bool for_update,
                                          bool has_hidden_rowid,
                                          bool is_dbms_cursor,
                                          int64_t orc_max_ret_rows)
{
  int ret = OB_SUCCESS;

  HEAP_VAR(ObSPIResultSet, spi_result) {
    ObSPICursor* spi_cursor = NULL;
    OZ (spi_result.init(session_info));
    OZ (spi_result.start_nested_stmt_if_need(ctx, sql, static_cast<stmt::StmtType>(type), for_update));

    if (OB_SUCC(ret)) {

      ObPLSqlAuditRecord audit_record(sql::PLSql);
      ObQueryRetryCtrl retry_ctrl;
      ObSPIExecEnvGuard env_guard(session_info, spi_result, cursor.is_ps_cursor());

      do {
        ret = OB_SUCCESS;
        uint64_t size = 0;
        {
          ObPLSubPLSqlTimeGuard guard(ctx);
          ObPLSPITraceIdGuard trace_id_guard(sql, ps_sql, session_info, ret);
          ObPLSqlAuditGuard audit_guard(
            *(ctx->exec_ctx_), session_info, spi_result, audit_record, ret, (sql != NULL ? sql : ps_sql), retry_ctrl, trace_id_guard, static_cast<stmt::StmtType>(type));
          ObSPIRetryCtrlGuard retry_guard(retry_ctrl, spi_result, session_info, ret);

          CK (OB_NOT_NULL(spi_result.get_memory_ctx()));
          OZ (inner_open(ctx,
                         spi_result.get_memory_ctx()->get_arena_allocator(),
                         sql,
                         ps_sql,
                         type,
                         params,
                         sql_param_count,
                         NULL,
                         0,
                         spi_result,
                         spi_result.get_out_params(),
                         false, /*is_forall*/
                         false, /*is_dynamic_sql*/
                         is_dbms_cursor /*is_dbms_sql*/));
          OZ (session_info.get_tmp_table_size(size));
          OZ (cursor.prepare_spi_cursor(spi_cursor,
                                        session_info.get_effective_tenant_id(),
                                        size,
                                        (for_update && !is_server_cursor && !is_dbms_cursor),
                                        &session_info), K(size));
          CK (OB_NOT_NULL(spi_result.get_result_set()));
          OZ (fill_cursor(*spi_result.get_result_set(), spi_cursor, ObTimeUtility::current_time(), orc_max_ret_rows));
          OZ (spi_cursor->row_store_.finish_add_row());
          if (OB_FAIL(ret)) {
          } else if (is_dbms_cursor) {
            ObDbmsCursorInfo &dbms_cursor = static_cast<ObDbmsCursorInfo&>(cursor);
            OZ (ObDbmsInfo::deep_copy_field_columns(
              dbms_cursor.get_dbms_entity()->get_arena_allocator(),
              spi_result.get_result_set()->get_field_columns(),
              dbms_cursor.get_field_columns()));
          } else {
            CK (OB_NOT_NULL(cursor.get_allocator()));
            OZ (ObDbmsInfo::deep_copy_field_columns(
              *cursor.get_allocator(), spi_result.get_result_set()->get_field_columns(), spi_cursor->fields_));
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(spi_result.get_result_set()) && OB_NOT_NULL(spi_result.get_result_set()->get_physical_plan())) {
            cursor.set_packed(spi_result.get_result_set()->get_physical_plan()->is_packed());
          }
          OX (cursor.open(spi_cursor));
          OX (for_update ? cursor.set_for_update() : (void)NULL);
          OX (for_update ? cursor.set_trans_id(session_info.get_tx_id()) : (void)NULL);
          OX (has_hidden_rowid ? cursor.set_hidden_rowid() : (void)NULL);
          if (OB_SUCC(ret) && lib::is_oracle_mode()) {
            OZ (setup_cursor_snapshot_verify_(&cursor, &spi_result));
          }
          if (!cursor.is_ps_cursor()) {
            retry_guard.test();
          }
          int close_ret = spi_result.close_result_set();
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("close mysql result failed", K(ret), K(close_ret));
          }
          ret = (OB_SUCCESS == ret ? close_ret : ret);
        }
        if (!is_dbms_cursor) {
          spi_result.destruct_exec_params(session_info);
        }

      } while (RETRY_TYPE_NONE != retry_ctrl.get_retry_type());
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(spi_cursor)) {
      spi_cursor->~ObSPICursor();
    }
    spi_result.end_nested_stmt_if_need(ctx, ret);
  }
  return ret;
}

int ObSPIService::spi_cursor_open(ObPLExecCtx *ctx,
                                  const char *sql,
                                  const char *ps_sql,
                                  int64_t type,
                                  bool for_update,
                                  bool has_hidden_rowid,
                                  const ObSqlExpression **sql_param_exprs,
                                  int64_t sql_param_count,
                                  uint64_t package_id,
                                  uint64_t routine_id,
                                  int64_t cursor_index,
                                  const int64_t *formal_param_idxs,
                                  const ObSqlExpression **actual_param_exprs,
                                  int64_t cursor_param_count,
                                  bool skip_locked)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObPLCursorInfo *cursor = NULL;
  ObObjParam cursor_var;
  ObCusorDeclareLoc loc;
  if (OB_ISNULL(ctx)
     || OB_ISNULL(ctx->exec_ctx_)
     || OB_ISNULL(session_info = ctx->exec_ctx_->get_my_session())
     || (sql_param_count > 0 && NULL == sql_param_exprs)
     || (NULL != sql && sql_param_count > 0)
     || (NULL == sql && 0 == sql_param_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(sql), K(ps_sql), K(type), K(sql_param_exprs), K(sql_param_count), K(ret));
  } else if (OB_FAIL(spi_get_cursor_info(ctx, package_id, routine_id, cursor_index, cursor, cursor_var, loc))) {
    LOG_WARN("failed to get cursor info", K(ret), K(cursor_index));
  } else if (OB_FAIL(cursor_open_check(ctx, package_id, routine_id, cursor_index, cursor, cursor_var, loc))) {
    LOG_WARN("cursor info not init", K(ret), K(cursor));
  } else if (cursor->isopen()) {
    ret = OB_ER_SP_CURSOR_ALREADY_OPEN;
    LOG_USER_ERROR(OB_ER_SP_CURSOR_ALREADY_OPEN);
    LOG_WARN("Cursor is already open",
      K(ret), KPC(cursor), K(package_id), K(routine_id), K(cursor_index), K(cursor_var), K(loc));
  } else if (stmt::T_SELECT != static_cast<stmt::StmtType>(type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only supported select stmt in cursor", K(ret), K(type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-select stmt in cursor");
  } else {
    ParamStore current_params(ObWrapperAllocator(ctx->allocator_));
    bool need_restore_params = false;
    ObPLSubPLSqlTimeGuard guard(ctx);
    if (cursor_param_count > 0 && (NULL == formal_param_idxs || NULL == actual_param_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cursor params in not valid",
               K(cursor_param_count), K(formal_param_idxs), K(actual_param_exprs), K(ret));
    } else {
      OZ (prepare_cursor_parameters(
        ctx, *session_info, package_id,
        routine_id, loc, formal_param_idxs, actual_param_exprs, cursor_param_count));
    }

    if (OB_SUCC(ret) && DECL_SUBPROG == loc) {
      ParamStore *subprog_params = NULL;
      OZ (current_params.assign(*ctx->params_));
      OZ (ObPLContext::get_param_store_from_local(*session_info, package_id, routine_id, subprog_params));
      CK (OB_NOT_NULL(subprog_params));
      OZ (ctx->params_->assign(*subprog_params));
      OX (need_restore_params = true);
    }

    bool is_server_cursor = false;
    bool use_stream = false;
    if (OB_SUCC(ret)) {
      is_server_cursor = OB_INVALID_ID != cursor->get_id()
        || (package_id != OB_INVALID_ID && OB_INVALID_ID == routine_id);
      if (is_server_cursor) {
        OZ (ObPLCursorInfo::prepare_entity(*session_info, cursor->get_cursor_entity()));
        OX (cursor->set_spi_cursor(NULL));
      }
    }
    OZ (session_info->ps_use_stream_result_set(use_stream));

    if (OB_FAIL(ret)) {
    } else if (lib::is_oracle_mode() // streaming cursor
               && ((is_server_cursor && use_stream) || !is_server_cursor)
               && (!for_update || (for_update && skip_locked))) {
      OZ (streaming_cursor_open(ctx,
                                *cursor,
                                *session_info,
                                sql,
                                ps_sql,
                                type,
                                sql_param_exprs,
                                sql_param_count,
                                is_server_cursor,
                                for_update,
                                has_hidden_rowid));
    } else { //MySQL Cursor/Updated Cursor/Server Cursor(REF_CURSOR, PACKAGE CURSOR), unstreaming cursor
      OZ (unstreaming_cursor_open(ctx,
                                  *cursor,
                                  *session_info,
                                  sql,
                                  ps_sql,
                                  type,
                                  sql_param_exprs,
                                  sql_param_count,
                                  is_server_cursor,
                                  for_update,
                                  has_hidden_rowid));
    }
    if (need_restore_params) {
      int ret = OB_SUCCESS;
      OZ (ctx->params_->assign(current_params));
    }
    if (OB_SUCC(ret) && DECL_PKG == loc) {
      OZ (spi_update_package_change_info(ctx, package_id, cursor_index));
    }
  }
  if (OB_FAIL(ret) && lib::is_mysql_mode()) {
    ctx->exec_ctx_->get_my_session()->set_show_warnings_buf(ret);
  }
  SET_SPI_STATUS;
  return ret;
}

int ObSPIService::dbms_cursor_open(ObPLExecCtx *ctx,
                                   ObDbmsCursorInfo &cursor,
                                   const ObString &ps_sql,
                                   int64_t stmt_type,
                                   bool for_update,
                                   bool hidden_rowid,
                                   int64_t orc_max_ret_rows)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ParamStore &exec_params = cursor.get_exec_params();
  ObString sql_str = (exec_params.count() > 0 || cursor.get_into_names().count() > 0) ? ObString() : cursor.get_sql_stmt();
  bool use_stream = false;
  ObPLSubPLSqlTimeGuard guard(ctx);

  OV (OB_NOT_NULL(ctx) &&
      OB_NOT_NULL(ctx->exec_ctx_) &&
      OB_NOT_NULL(ctx->allocator_) &&
      OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()),
      OB_INVALID_ARGUMENT, KP(ctx), K(ps_sql), K(stmt_type));

  OZ (session->ps_use_stream_result_set(use_stream));

  if (OB_SUCC(ret) && cursor.isopen()) {
    OZ (dbms_cursor_close(*ctx->exec_ctx_, cursor), K(cursor.get_id()));
    OZ (ObPLCursorInfo::prepare_entity(*session, cursor.get_cursor_entity()));
    OX (cursor.set_spi_cursor(NULL));
  }

  if (OB_FAIL(ret)) {
  } else if (!for_update && use_stream) { // streaming branch
    OZ (streaming_cursor_open(ctx,
                              cursor,
                              *session,
                              sql_str,
                              ps_sql,
                              stmt_type,
                              &exec_params,
                              exec_params.count(),
                              true, /*is_server_cursor*/
                              for_update,
                              hidden_rowid,
                              true /*is_dbms_cursor*/));
  } else { // unstreaming branch
    OZ (unstreaming_cursor_open(ctx,
                                cursor,
                                *session,
                                sql_str,
                                ps_sql,
                                stmt_type,
                                &exec_params,
                                exec_params.count(),
                                true, /*is_server_cursor*/
                                for_update,
                                hidden_rowid,
                                true, /*is_dbms_cursor*/
                                orc_max_ret_rows));
  }
  SET_SPI_STATUS;
  return ret;
}

int ObSPIService::do_cursor_fetch(ObPLExecCtx *ctx,
                                  ObPLCursorInfo *cursor,
                                  bool is_server_cursor,
                                  const ObSqlExpression **into_exprs,
                                  int64_t into_count,
                                  const ObDataType *column_types,
                                  int64_t type_count,
                                  const bool *exprs_not_null_flag,
                                  const int64_t *pl_integer_ranges,
                                  bool is_bulk,
                                  int64_t limit,
                                  const ObDataType *return_types,
                                  int64_t return_type_count,
                                  bool is_type_record)
{
  int ret = OB_SUCCESS;

  ObSPIResultSet *spi_result = NULL;
  ObSQLSessionInfo *session = NULL;

  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(cursor));

  // check cursor is valid for fetch
  if (OB_SUCC(ret)
      // not fetched or has fetched rows
      && (!cursor->get_fetched() || cursor->get_fetched_with_row())
      && cursor->is_need_check_snapshot()
      && lib::is_oracle_mode()) {
    /* case: select * from dual, snapshot do not initilize, so it's invalid */
    /*
      a) if snapshot has been invalidated, can not fetch
      b) for update cursor:
      can't fetch if transaction committed, because can not cross transaction boundary
      c) streaming cursor:
      can't fetch if transaction committed, because snapshot data has been removed
    */
    if (!cursor->get_snapshot().is_valid()) {
        ret = OB_ERR_FETCH_OUT_SEQUENCE;
        LOG_WARN("snapshot is invalid", K(cursor->get_snapshot()), K(ret));
    } else if (cursor->is_for_update()) {
      if (cursor->get_snapshot().is_committed()) {
          ret = OB_ERR_FETCH_OUT_SEQUENCE;
          LOG_WARN("transaction has been committed, for update cursor can not fetch",
                  K(cursor->get_snapshot()), K(ret));
      }
    } else if (cursor->is_streaming()) {
      if(cursor->get_snapshot().is_committed()) {
        ret = OB_ERR_FETCH_OUT_SEQUENCE;
        LOG_WARN("transaction has been committed, streaming cursor can not fetch",
                 K(cursor->get_snapshot()), K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_bulk && INT64_MAX != limit) { //limit子句必须和Bulk Collect合用
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Limit must be used with Bulk Collect Into Clause", K(is_bulk), K(limit), K(ret));
  } else if (limit != INT64_MAX && limit <= 0) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("OBE-06502: PL/SQL: numeric or value error", K(ret), K(limit));
  } else if (!cursor->isopen()) {
    ret = OB_ER_SP_CURSOR_NOT_OPEN;
    LOG_USER_ERROR(OB_ER_SP_CURSOR_NOT_OPEN);
    LOG_WARN("Cursor is not open", K(cursor), K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (cursor->is_streaming()) {
    CK (OB_NOT_NULL(spi_result = cursor->get_cursor_handler()));
    OZ (spi_result->set_cursor_env(*ctx->exec_ctx_->get_my_session()));
    OZ (adjust_out_params(ctx, into_exprs, into_count, spi_result->get_out_params()));
  } else if (OB_NOT_NULL(cursor->get_spi_cursor())
              && cursor->get_spi_cursor()->row_store_.get_row_cnt() > 0
              && cursor->get_current_row().is_invalid()) { //有数据才需要做，避免重复做
    //仅缓存在ObRowStore的Cursor需要初始化ObNewRow结构
    CK (OB_NOT_NULL(cursor->get_spi_cursor()));
    int64_t column_count = cursor->get_spi_cursor()->row_desc_.count();
    ObIAllocator *spi_allocator = NULL;
    OX (spi_allocator = NULL == cursor->get_cursor_entity()
                                  ? cursor->get_allocator()
                                  : &cursor->get_cursor_entity()->get_arena_allocator());
    CK (OB_NOT_NULL(spi_allocator));
    if (OB_SUCC(ret)) {
      void *ptr = spi_allocator->alloc(column_count * sizeof(ObObj));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory for row failed", "size", column_count * sizeof(ObObj));
      } else {
        cursor->get_current_row().cells_ = new(ptr) common::ObObj[column_count];
        cursor->get_current_row().count_ = column_count;
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t row_count = 0;

#define GET_RESULT                                                                    \
  do {                                                                                \
    if (OB_FAIL(ret)) {                                                               \
    } else if (!cursor->is_streaming()) {                                             \
      bool can_retry = true;                                                          \
      ret = get_result(ctx,                                                           \
                   static_cast<void*>((cursor)->get_spi_cursor()),                    \
                   false,                                                             \
                   into_exprs,                                                        \
                   into_count,                                                        \
                   column_types,                                                      \
                   type_count,                                                        \
                   exprs_not_null_flag,                                               \
                   pl_integer_ranges,                                                 \
                   NULL,                                                              \
                   row_count,                                                         \
                   cursor->get_current_row(),                                         \
                   can_retry,                                                         \
                   cursor->has_hidden_rowid(),                                        \
                   is_bulk,                                                           \
                   false/*is_dynamic_sql*/,                                           \
                   true/*for_cursor*/,                                                \
                   false,                                                             \
                   limit,                                                             \
                   return_types,                                                      \
                   return_type_count,                                                 \
                   is_type_record);                                                   \
    } else {                                                                          \
      ObString sql;                                                                   \
      ObString ps_sql;                                                                \
      if (spi_result->get_sql_ctx().is_prepare_protocol_) {                           \
        ps_sql = spi_result->get_sql_ctx().cur_sql_;                                  \
      } else {                                                                        \
        sql = spi_result->get_sql_ctx().cur_sql_;                                     \
      }                                                                               \
      ObPLSqlAuditRecord audit_record(sql::CursorFetch);                              \
      ObQueryRetryCtrl retry_ctrl;                                                    \
      ObPLSPITraceIdGuard trace_id_guard(sql, ps_sql, *session, ret, cursor->get_sql_trace_id()); \
      ObPLSubPLSqlTimeGuard guard(ctx);                                               \
      ObPLSqlAuditGuard audit_guard(                                                  \
        *(ctx->exec_ctx_), *session, *spi_result, audit_record, ret, ps_sql, retry_ctrl, trace_id_guard, spi_result->get_sql_ctx().stmt_type_); \
      if (cursor->get_sql_trace_id()->is_invalid()                                    \
            && OB_NOT_NULL(ObCurTraceId::get_trace_id())) {                           \
        cursor->get_sql_trace_id()->set(*ObCurTraceId::get_trace_id());               \
      }                                                                               \
      ret = inner_fetch_with_retry(ctx,                                               \
                   *cursor->get_cursor_handler(),                                     \
                   into_exprs,                                                        \
                   into_count,                                                        \
                   column_types,                                                      \
                   type_count,                                                        \
                   exprs_not_null_flag,                                               \
                   pl_integer_ranges,                                                 \
                   row_count,                                                         \
                   cursor->get_current_row(),                                         \
                   cursor->has_hidden_rowid(),                                        \
                   is_bulk,                                                           \
                   true,                                                              \
                   limit,                                                             \
                   cursor->get_last_execute_time(),                                   \
                   return_types,                                                      \
                   return_type_count,                                                 \
                   is_type_record);                                                   \
    }                                                                                 \
  } while(0)

    if (is_server_cursor) {
      lib::ContextTLOptGuard guard(false);
      GET_RESULT;
    } else {
      GET_RESULT;
    }

#undef GET_RESULT

    if (cursor->is_streaming()) {
      spi_result->end_cursor_stmt(ctx, ret);
      cursor->set_last_execute_time(ObTimeUtility::current_time());
    }

    // Oracle模式的Cursor会吞掉READ_NOTHING的错误, 为了避免无效日志过多, 只在Mysql模式下打印WARN
    if (OB_SUCC(ret)
       || (OB_READ_NOTHING == ret && lib::is_oracle_mode())) {
      cursor->set_fetched();
      cursor->set_fetched_with_row(ret != OB_READ_NOTHING);
      cursor->set_rowcount(cursor->get_rowcount() + row_count);
    } else {
      LOG_WARN("failed to spi cursor fetch", K(ret));
    }
  }

  return ret;
}

int ObSPIService::dbms_cursor_fetch(
  ObPLExecCtx *ctx, ObDbmsCursorInfo &cursor, bool is_server_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_cursor_fetch(ctx,
                              &cursor,
                              is_server_cursor,/*ps is server cursor*/
                              NULL,
                              0,
                              NULL,
                              0,
                              NULL,
                              NULL,
                              false,
                              INT64_MAX))) {
    if (ret != OB_READ_NOTHING) {
      LOG_WARN("failed to do cursor fetch", K(ret), K(is_server_cursor), K(cursor));
    }
  }
  return ret;
}

int ObSPIService::spi_cursor_fetch(ObPLExecCtx *ctx,
                                   uint64_t package_id,
                                   uint64_t routine_id,
                                   int64_t cursor_index,
                                   const int64_t *into_exprs_idx,
                                   int64_t into_count,
                                   const ObDataType *column_types,
                                   int64_t type_count,
                                   const bool *exprs_not_null_flag,
                                   const int64_t *pl_integer_ranges,
                                   bool is_bulk,
                                   int64_t limit,
                                   const ObDataType *return_types,
                                   int64_t return_type_count,
                                   bool is_type_record)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor = NULL;
  ObObjParam cur_var;
  ObCusorDeclareLoc loc;
  ObArenaAllocator alloc("SpiTemp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const ObSqlExpression **into_exprs = nullptr;
  MAKE_EXPR_BUFFER(alloc, into_exprs_idx, into_count, into_exprs);
  OZ (spi_get_cursor_info(ctx, package_id, routine_id, cursor_index, cursor, cur_var, loc));
  if (OB_SUCC(ret) && OB_ISNULL(cursor)) {
      ret = OB_ERR_INVALID_CURSOR;
      LOG_WARN("ref cursor is null", K(ret));
  }
  OV (!cursor->is_invalid_cursor(), OB_ERR_INVALID_CURSOR);
  OZ (do_cursor_fetch(ctx,
                      cursor,
                      OB_INVALID_ID != cursor->get_id()
                        || (package_id != OB_INVALID_ID && OB_INVALID_ID == routine_id),
                      into_exprs,
                      into_count,
                      column_types,
                      type_count,
                      exprs_not_null_flag,
                      pl_integer_ranges,
                      is_bulk,
                      limit,
                      return_types,
                      return_type_count,
                      is_type_record));

  if (OB_FAIL(ret) && lib::is_mysql_mode()) {
    ctx->exec_ctx_->get_my_session()->set_show_warnings_buf(ret);
  }

  if (lib::is_mysql_mode() || OB_READ_NOTHING != ret) {
    //Oracle模式的cursor发生NOT FOUND错误的时候不对外报错，而是把错误信息记录在CURSOR上，PL的CG会吞掉这个错误
    SET_SPI_STATUS;
  }
  return ret;
}

int ObSPIService::cursor_close_impl(ObPLExecCtx *ctx,
                                        ObPLCursorInfo *cursor,
                                        bool is_refcursor,
                                        uint64_t package_id,
                                        uint64_t routine_id,
                                        bool ignore)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("cursor close", K(cursor), KPC(cursor));
  if (OB_ISNULL(cursor)) {
    if (is_refcursor) {
      // do nothing
    } else {
      ret = OB_ERR_INVALID_CURSOR;
      LOG_WARN("close null cursor", K(package_id), K(routine_id), K(ret));
    }
  } else {
    bool is_server_cursor = OB_INVALID_ID != cursor->get_id()
      || (package_id != OB_INVALID_ID && OB_INVALID_ID == routine_id)
      || cursor->is_session_cursor();
    if (!cursor->isopen()) {
      if (!ignore) {
        ret = OB_ER_SP_CURSOR_NOT_OPEN;
        LOG_USER_ERROR(OB_ER_SP_CURSOR_NOT_OPEN);
        LOG_WARN("Cursor is not open", K(cursor), K(ret));
      }
      is_server_cursor ? cursor->reuse() : cursor->reset();
    } else {
      OZ (cursor->close(*ctx->exec_ctx_->get_my_session(), is_server_cursor));
    }
  }
  return ret;
}

int ObSPIService::spi_cursor_close(ObPLExecCtx *ctx,
                                   uint64_t package_id,
                                   uint64_t routine_id,
                                   int64_t cursor_index,
                                   bool ignore)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor = nullptr;
  ObObjParam cur_var;
  ObCusorDeclareLoc loc;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(ctx->params_));
  OZ (spi_get_cursor_info(ctx, package_id, routine_id, cursor_index, cursor, cur_var, loc),
      package_id, routine_id, cursor_index, cur_var);
  OV (ignore || OB_ISNULL(cursor) || !cursor->is_invalid_cursor(), OB_ERR_INVALID_CURSOR, ignore, cur_var);

  OZ (cursor_close_impl(ctx, cursor, cur_var.is_ref_cursor_type(), package_id, routine_id, ignore),
      package_id, routine_id, cursor_index, cur_var);
  if (OB_SUCC(ret) && DECL_PKG == loc) {
    OZ (spi_update_package_change_info(ctx, package_id, cursor_index));
  }
  return ret;
}

int ObSPIService::dbms_cursor_close(ObExecContext &exec_ctx, ObPLCursorInfo &cursor)
{
  int ret = OB_SUCCESS;
  // dbms cursor与pl cursor不同：
  // 1. 执行过dbms_sql.open_cursor接口后，就可以执行dbms_sql.close_cursor接口。
  // 2. 但只有执行过dbms_sql.execute接口后，cursor.is_open()才返回true。
  if (cursor.isopen()) {
    OV (OB_NOT_NULL(exec_ctx.get_my_session()));
    OZ (cursor.close(*exec_ctx.get_my_session(),
                     OB_NOT_NULL(cursor.get_cursor_entity()) ? true : false));
  }
  return ret;
}

int ObSPIService::spi_set_pl_exception_code(
  pl::ObPLExecCtx *ctx, int64_t code, bool is_pop_warning_buf, int level)
{
  int ret = OB_SUCCESS;
  ObPLContext *pl_ctx = NULL;
  ObPLSqlCodeInfo *sqlcode_info = NULL;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(pl_ctx = ctx->exec_ctx_->get_my_session()->get_pl_context()));
  CK (OB_NOT_NULL(sqlcode_info = ctx->exec_ctx_->get_my_session()->get_pl_sqlcode_info()));
  if (OB_SUCC(ret) && code != sqlcode_info->get_sqlcode()) {
    if (lib::is_oracle_mode()) {
      OX (sqlcode_info->set_sqlcode(code > 0 ? 1 : code));
    } else {
      OX (sqlcode_info->set_sqlcode(code));
    }
    if (code >= OB_MIN_RAISE_APPLICATION_ERROR && code <= OB_MAX_RAISE_APPLICATION_ERROR) {
      LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, code, 0, "");
    }
  }
  if (is_pop_warning_buf && lib::is_mysql_mode() && sqlcode_info->get_stack_warning_buf().count() > 0) {
    int64_t idx = sqlcode_info->get_stack_warning_buf().count() - 1;
    OX (sqlcode_info->get_stack_warning_buf().at(idx).~ObWarningBuffer());
    OX (sqlcode_info->get_stack_warning_buf().pop_back());
    OX (ctx->exec_ctx_->get_my_session()->set_show_warnings_buf(OB_SUCCESS));
  }

#ifdef OB_BUILD_ORACLE_PL
  if (OB_FAIL(ret) || lib::is_mysql_mode() || OB_ISNULL(pl_ctx->get_call_stack_trace())) {
  } else if (!is_pop_warning_buf) {
    OZ (pl_ctx->get_call_stack_trace()->format_error_trace(pl_ctx->get_exec_stack(), level));
  } else {
    OZ (pl_ctx->get_call_stack_trace()->format_error_trace(pl_ctx->get_exec_stack().count(), level));
  }
#endif

  return ret;
}

int ObSPIService::spi_get_pl_exception_code(pl::ObPLExecCtx *ctx, int64_t *code)
{
  int ret = OB_SUCCESS;
  ObPLSqlCodeInfo *sqlcode_info = NULL;
  ObWarningBuffer *wb = NULL;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(sqlcode_info = ctx->exec_ctx_->get_my_session()->get_pl_sqlcode_info()));
  CK (OB_NOT_NULL(code));
  if (lib::is_mysql_mode()
      && OB_NOT_NULL(wb = common::ob_get_tsi_warning_buffer())) {
    OX (wb->reset_warning());
  }
  OX (*code = sqlcode_info->get_sqlcode());
  if (lib::is_mysql_mode()
      && OB_NOT_NULL(wb = common::ob_get_tsi_warning_buffer())) {
    OZ (sqlcode_info->get_stack_warning_buf().push_back(*wb));
  }
  return ret;
}

int ObSPIService::spi_check_exception_handler_legal(pl::ObPLExecCtx *ctx, int64_t code)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()->get_pl_context()));
  if (OB_SUCC(ret)
      && (ctx->exec_ctx_->get_my_session()->get_pl_context()->is_exception_handler_illegal()
          || OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL == code)) {
    ret = OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL;
    LOG_WARN("implementation restriction: exception handler in nested transaction is illegal",
             K(ret), K(ctx->exec_ctx_->get_my_session()->get_nested_count()),
             K(ctx->exec_ctx_->get_my_session()->get_pl_context()->is_exception_handler_illegal()));
    LOG_USER_ERROR(OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL);
  }
  return ret;
}

int ObSPIService::spi_check_early_exit(pl::ObPLExecCtx *ctx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  OZ (spi_check_timeout(*ctx->exec_ctx_));
  return ret;
}

int ObSPIService::spi_check_timeout(sql::ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(exec_ctx.get_my_session()));

  if (OB_SUCC(ret)) {
    ObSQLSessionInfo *session_info = exec_ctx.get_my_session();
    if (OB_FAIL(session_info->check_session_status())) {
      LOG_WARN("spi check session not healthy", K(ret));
    } else if (session_info->is_in_transaction()
               && !session_info->get_tx_desc()->is_tx_end()
               && session_info->get_tx_desc()->is_tx_timeout()) {
      ret = OB_TRANS_TIMEOUT;
      LOG_WARN("spi check early exit, transaction timeout", K(ret));
    } else if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("spi check early exit, pl block timeout", K(ret));
    }
  }

  bool is_stack_overflow = false;
  OZ (check_stack_overflow(is_stack_overflow));
  if (OB_SUCC(ret) && is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow in loop", K(is_stack_overflow), K(ret));
  }
  return ret;
}

int ObSPIService::spi_pipe_row_to_result(pl::ObPLExecCtx *ctx,
                                         ObObjParam *single_row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, single_row);
#else
  ObPLNestedTable *coll = NULL;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->result_));
  CK (OB_NOT_NULL(ctx->allocator_));
  CK (OB_NOT_NULL(single_row));
  CK (ctx->result_->is_ext());
  CK (OB_NOT_NULL(coll = reinterpret_cast<ObPLNestedTable *>(ctx->result_->get_ext())));
  OX (coll->is_inited() ? (void)NULL : coll->set_inited());
  OZ (spi_set_collection(ctx->exec_ctx_->get_my_session()->get_effective_tenant_id(),
                           ctx, *(ctx->allocator_), *coll, 1, true));
  CK (coll->get_count() >= 1);
  CK (coll->get_column_count() > 0);
  if (OB_SUCC(ret)) {
    ObObj *copied = coll->get_data() + coll->get_count() - 1;
    OZ (spi_copy_datum(ctx, coll->get_allocator(), single_row, copied,
                       const_cast<ObDataType *>(&coll->get_element_type()),
                       OB_INVALID_ID /* package_id */));
    //OZ (coll->set_row(copied, coll->get_count() - 1));
  }

  if (OB_SUCC(ret) && NULL != coll->get_allocator()) {
    // use _pipelined_table_function_memory_limit to limit pipe row
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_LIKELY(tenant_config.is_valid())) {
      if ((static_cast<ObPLAllocator1*>(coll->get_allocator()))->get_used()
            > tenant_config->_pipelined_table_function_memory_limit) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("pipe row use too much memory", K(ret));
      }
    }
  }

#endif
  return ret;
}

int ObSPIService::spi_alloc_complex_var(pl::ObPLExecCtx *ctx,
                                        int64_t type,
                                        int64_t id,
                                        int64_t var_idx,
                                        int64_t init_size,
                                        int64_t *addr,
                                        ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, type, id, var_idx, init_size, addr);
#else
  void *ptr = NULL;
  bool need_alloc = true;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->allocator_));
  CK (OB_NOT_NULL(alloc));
  CK (OB_NOT_NULL(ctx->params_));
  CK (OB_NOT_NULL(ctx->result_));
  CK ((var_idx >= 0 && var_idx < ctx->params_->count())
      || OB_INVALID_INDEX == var_idx); // OB_INVALID_ID代表初始化函数返回值

  if (OB_SUCC(ret)) {
    if (var_idx >= 0 && var_idx < ctx->params_->count()) {
      if (ctx->params_->at(var_idx).is_pl_extend() &&
          0 != ctx->params_->at(var_idx).get_ext() &&
          PL_OPAQUE_TYPE != ctx->params_->at(var_idx).get_meta().get_extend_type()) {
        //need_alloc = false;
        int tmp = OB_SUCCESS;
        if (OB_SUCCESS != (tmp = ObUserDefinedType::destruct_objparam(*ctx->allocator_,
                                                                      ctx->params_->at(var_idx),
                                                                      ctx->exec_ctx_->get_my_session()))) {
          LOG_WARN("fail to destruct obj", K(tmp));
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_alloc) {
    ptr = alloc->alloc(init_size);
    LOG_DEBUG("debug for spi alloc complex var",
              K(var_idx), K(init_size),
              K(sizeof(ObPLCollection)), K(sizeof(ObPLAssocArray)),
              K(sizeof(ObPLNestedTable)), K(sizeof(ObPLVArray)));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for complex var", K(ret));
    } else {
      MEMSET(ptr, 0, init_size);
      if (NULL != addr) {
        *addr = reinterpret_cast<int64_t>(ptr);
      }

#define CONSTRUCT_COLLECTION(type, class) \
  case type: { \
    new(ptr)class(id); \
    break; \
  }
      ObPLType complex_type = (ObPLType)type;
      switch (complex_type) {
        CONSTRUCT_COLLECTION(PL_NESTED_TABLE_TYPE, ObPLNestedTable);
        CONSTRUCT_COLLECTION(PL_ASSOCIATIVE_ARRAY_TYPE, ObPLAssocArray);
        CONSTRUCT_COLLECTION(PL_VARRAY_TYPE, ObPLVArray);
        case PL_RECORD_TYPE: {
          new (ptr) ObPLRecord(id, 0);
        } break;
        case PL_OPAQUE_TYPE: {
          new (ptr) ObPLOpaque();
        } break;
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected complex type", K(ret), K(complex_type));
        }
      }
#undef CONSTRUCT_COLLECTION

      if (OB_FAIL(ret)) {
      } else if (OB_INVALID_INDEX != var_idx) {
        ctx->params_->at(var_idx).set_extend(reinterpret_cast<int64_t>(ptr), type, init_size);
        ctx->params_->at(var_idx).set_param_meta();
      } else if (OB_INVALID_INDEX == var_idx && NULL == addr) {
        ctx->result_->set_extend(reinterpret_cast<int64_t>(ptr), type, init_size);
      } else { /*do nothing*/ }
    }
  }
#endif
  return ret;
}

int ObSPIService::spi_clear_diagnostic_area(pl::ObPLExecCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx->exec_ctx_->get_my_session();

  CK (OB_NOT_NULL(session_info));
  OX (session_info->set_show_warnings_buf(OB_SUCCESS));

  return ret;
}

int ObSPIService::spi_construct_collection(
  pl::ObPLExecCtx *ctx, uint64_t package_id, ObObjParam *result)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = NULL;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(allocator = ctx->allocator_));
  CK (OB_NOT_NULL(result));
  if (OB_SUCC(ret) && package_id != OB_INVALID_ID) {
    OZ (spi_get_package_allocator(ctx, package_id, allocator));
    CK (OB_NOT_NULL(allocator));
  }
  if (OB_SUCC(ret)) {
    CK (result->is_ext());
    if (OB_SUCC(ret)) {
      void *ptr = NULL;
      ObPLCollection* coll = NULL;
      CK (OB_NOT_NULL(ptr = reinterpret_cast<void *>(result->get_ext())));
      CK (OB_NOT_NULL(coll = reinterpret_cast<ObPLCollection *>(ptr)));
      OZ (spi_set_collection(ctx->exec_ctx_->get_my_session()->get_effective_tenant_id(),
                             ctx,
                             *allocator,
                             *coll,
                             0,
                             false));
    }
  }
  return ret;
}

#define GET_INTEGER_FROM_OBJ(result, r)               \
CK (result.is_integer_type() || result.is_number());  \
if (OB_SUCC(ret)) {                                   \
  if (result.is_integer_type()) {                     \
    r = result.get_int();                             \
  } else if (result.is_number()) {                    \
    if (!result.get_number().is_valid_int64(r)) {     \
      number::ObNumber num = result.get_number();     \
      OZ (num.round(0));                              \
      if (OB_SUCC(ret) && !num.is_valid_int64(r)) {   \
        ret = OB_ARRAY_OUT_OF_RANGE;                  \
        LOG_WARN("wrong array index", K(ret));        \
      }                                               \
    }                                                 \
  }                                                   \
}

int ObSPIService::spi_extend_collection(pl::ObPLExecCtx *ctx,
                                        const int64_t collection_expr_idx,
                                        int64_t column_count,
                                        const int64_t n_expr_idx,
                                        const int64_t i_expr_idx,
                                        uint64_t package_id)
{
  int ret = OB_SUCCESS;
  const ObSqlExpression *collection_expr = nullptr;
  const ObSqlExpression *n_expr = nullptr;
  const ObSqlExpression *i_expr = nullptr;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->func_));
  CK (collection_expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= collection_expr_idx && collection_expr_idx < ctx->func_->get_expressions().count()));
  CK (n_expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= n_expr_idx && n_expr_idx < ctx->func_->get_expressions().count()));
  OX (collection_expr = ctx->func_->get_expressions().at(collection_expr_idx));
  OX (n_expr = ctx->func_->get_expressions().at(n_expr_idx));
  OX (GET_NULLABLE_EXPR_BY_IDX(ctx, i_expr_idx, i_expr));

  if (OB_FAIL(ret)) {
    // do nothing
  }else if (OB_ISNULL(ctx)
      || OB_ISNULL(ctx->exec_ctx_)
      || OB_ISNULL(ctx->exec_ctx_->get_my_session())
      || OB_ISNULL(collection_expr)
      || column_count <= 0
      || OB_ISNULL(n_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(collection_expr), K(column_count), K(n_expr), K(i_expr), K(ret));
  } else {
    ObObjParam result;
    ObObjParam n_result;
    ObObjParam i_result;
    bool do_nothing = false;
    ObPLCollection *table = NULL;
    int64_t n = OB_INVALID_SIZE;
    int64_t i = OB_INVALID_INDEX;
    int64_t org_elem_cnt = OB_INVALID_SIZE;
    if (OB_FAIL(spi_calc_expr(ctx, collection_expr, OB_INVALID_INDEX, &result))) {
      LOG_WARN("failed to calc expr", K(ctx), K(collection_expr), K(result), K(ret));
    } else if (result.get_type() != ObExtendType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extend collection not a extend type obj", K(ret), K(result));
    } else if (0 == result.get_ext()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a invalid table", K(result.get_ext()), K(ret));
    } else {
      table = reinterpret_cast<ObPLCollection*>(result.get_ext());
      if (!table->is_inited()) {
        ret = OB_NOT_INIT;
        LOG_WARN("OBE-06531: Reference to uninitialized collection", K(ret));
      } else if (0 == table->get_column_count()) {
        table->set_column_count(column_count);
      }
      OX (org_elem_cnt = table->get_count());
    }

    if (OB_SUCC(ret)) {
      OZ (spi_calc_expr(ctx, n_expr, OB_INVALID_INDEX, &n_result));
      OX (do_nothing = n_result.is_null());
    }

    if (OB_SUCC(ret) && NULL != i_expr) {
      OZ (spi_calc_expr(ctx, i_expr, OB_INVALID_INDEX, &i_result));
      OX (do_nothing = do_nothing || i_result.is_null());
    }

    bool is_deleted = false;
    if (OB_FAIL(ret) || do_nothing) {
    } else {
      GET_INTEGER_FROM_OBJ(n_result, n);
      if (OB_SUCC(ret) && n < 0) {
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
        LOG_WARN("get a invalid table", K(result), K(ret));
      }
      if (NULL != i_expr) {
        GET_INTEGER_FROM_OBJ(i_result, i);
        if (OB_SUCC(ret) && (i <= 0 || i > table->get_count())) {
          ret = OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT;
          LOG_WARN("get a invalid table", K(result), K(ret));
        }
      }
      if (OB_SUCC(ret) && i != OB_INVALID_INDEX) {
        OZ (table->is_elem_deleted(i - 1, is_deleted));
      }
    }

    if (OB_FAIL(ret) || do_nothing) {
    } else if (is_deleted) {
      ret = OB_READ_NOTHING;
      LOG_WARN("element already deleted!", K(ret), K(i), K(is_deleted));
    } else if (n > 0) {
      ObIAllocator *allocator = NULL;
      if (OB_INVALID_ID != package_id) {
        OZ (spi_get_package_allocator(ctx, package_id, allocator));
      } else {
        allocator = ctx->allocator_;
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(allocator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("allocator is NULL", K(ctx), K(column_count), K(n), K(i), K(package_id), K(ret));
        } else if (OB_FAIL(spi_set_collection(ctx->exec_ctx_->get_my_session()->get_effective_tenant_id(),
                                                ctx, *allocator, *table, n, true))) {
          LOG_WARN("failed to spi_set_collection_data", K(ctx), K(column_count), K(n), K(i), K(package_id), K(ret));
        } else if (OB_SUCC(ret) && OB_NOT_NULL(i_expr)){
          // fill i'th elem into the extended pos;
          // EXTEND operates on the internal size of a collection. That is, if DELETE deletes an
          // element but keeps a placeholder for it, then EXTEND considers the element to exist.
          int64_t coll_cnt = table->get_count();
          ObObj *data = reinterpret_cast<ObObj*>(table->get_data());
          if (i > 0 && i <= org_elem_cnt) {
            ObObj &ith_elem = data[i - 1];
            CK (OB_NOT_NULL(table->get_allocator()));
            for (int64_t j = coll_cnt - n; OB_SUCC(ret) && j < coll_cnt; ++j) {
              if (ith_elem.is_pl_extend()) {
                OZ (ObUserDefinedType::destruct_obj(data[j], ctx->exec_ctx_->get_my_session(), true));
                OZ (ObUserDefinedType::deep_copy_obj(*(table->get_allocator()), ith_elem, data[j]));
              } else {
                void *ptr = data[j].get_deep_copy_obj_ptr();
                if (nullptr != ptr) {
                  table->get_allocator()->free(ptr);
                }
                OZ (deep_copy_obj(*(table->get_allocator()), ith_elem, data[j]));
              }
            }
          } else {
            ret = OB_ERR_SUBSCRIPT_BEYOND_COUNT;
            LOG_WARN("collection index is out of range", K(i), K(org_elem_cnt), K(coll_cnt));
          }
        } else {
          // do nothing
        }
      }
    }
  }
  SET_SPI_STATUS;
  return ret;
}

int ObSPIService::spi_raise_application_error(pl::ObPLExecCtx *ctx,
                                              const int64_t errcode_expr_idx,
                                              const int64_t errmsg_expr_idx)
{
  int ret = OB_SUCCESS;
  ObObjParam errcode_result;
  ObObjParam errmsg_result;
  ObPLSqlCodeInfo *sqlcode_info = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObSqlExpression *errcode_expr = nullptr;
  const ObSqlExpression *errmsg_expr = nullptr;
  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(ctx->func_));
  CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(sqlcode_info = ctx->exec_ctx_->get_my_session()->get_pl_sqlcode_info()));
  CK (errcode_expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= errcode_expr_idx && errcode_expr_idx < ctx->func_->get_expressions().count()));
  CK (errmsg_expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= errmsg_expr_idx && errmsg_expr_idx < ctx->func_->get_expressions().count()));
  OX (errcode_expr = ctx->func_->get_expressions().at(errcode_expr_idx));
  OX (errmsg_expr = ctx->func_->get_expressions().at(errmsg_expr_idx));
  CK (OB_NOT_NULL(ctx->get_top_expr_allocator()));

#define CALC(expr, type, result) \
  do { \
    ObObjParam tmp; \
    ObExprResType expected_type; \
    OZ (spi_calc_expr(ctx, expr, OB_INVALID_INDEX, &tmp)); \
    OX (expected_type.set_##type()); \
    OX (expected_type.set_collation_type(tmp.get_collation_type())); \
    OZ (spi_convert(ctx->exec_ctx_->get_my_session(), ctx->get_top_expr_allocator(), tmp, expected_type, result)); \
  } while(0)

   CALC(errcode_expr, int32, errcode_result);
   CALC(errmsg_expr, varchar, errmsg_result);
   OX (sqlcode_info->set_sqlcode(errcode_result.get_int32()));
   if (OB_SUCC(ret)) {
    ObPLContext *pl_ctx = NULL;
    ObPLExecState *frame = NULL;
    ObIAllocator *expr_allocator = NULL;
    ObString deep_sqlmsg;
    CK (OB_NOT_NULL(pl_ctx = ctx->exec_ctx_->get_my_session()->get_pl_context()));
    CK (pl_ctx->get_exec_stack().count() > 0);
    CK (OB_NOT_NULL(frame = pl_ctx->get_exec_stack().at(0)));
    CK (frame->is_top_call());
    CK (OB_NOT_NULL(expr_allocator = frame->get_exec_ctx().get_top_expr_allocator()));
    OZ (ob_write_string(*expr_allocator, errmsg_result.get_string(), deep_sqlmsg));
    OX (sqlcode_info->set_sqlmsg(deep_sqlmsg));
  }
  
  if (OB_SUCC(ret)) {
    if (sqlcode_info->get_sqlcode() <= OB_MAX_RAISE_APPLICATION_ERROR
        && sqlcode_info->get_sqlcode() >= OB_MIN_RAISE_APPLICATION_ERROR) {
      ObString convert_sqlmsg;
      OZ (ObCharset::charset_convert(*ctx->get_top_expr_allocator(),
                                     errmsg_result.get_string(),
                                     errmsg_result.get_collation_type(),
                                     session_info->get_local_collation_connection(),
                                     convert_sqlmsg));
      if (OB_SUCC(ret)) {
        LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR,
                       static_cast<int64_t>(sqlcode_info->get_sqlcode()),
                       convert_sqlmsg.length(),
                       convert_sqlmsg.ptr());
      }
    } else {
      ret = OB_SP_RAISE_APPLICATION_ERROR_NUM;
      LOG_WARN("error number argument to raise_application_error of stringstring is out of range",
               K(ret), K(sqlcode_info->get_sqlcode()), K(sqlcode_info->get_sqlmsg()));
      LOG_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR_NUM, sqlcode_info->get_sqlcode());
      sqlcode_info->set_sqlcode(OB_SP_RAISE_APPLICATION_ERROR_NUM);
    }
  }
  OX (ret = sqlcode_info->get_sqlcode());
  return ret;
}

int ObSPIService::spi_process_resignal(pl::ObPLExecCtx *ctx,
                                       const int64_t errcode_expr_idx,
                                       const int64_t errmsg_expr_idx,
                                       const char *sql_state,
                                       int *error_code,
                                       const char *resignal_sql_state,
                                       bool is_signal)
{
  int ret = OB_SUCCESS;
  ObObjParam result;
  ObPLSqlCodeInfo *sqlcode_info = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObWarningBuffer *wb = NULL;
  ObPLConditionType type = ObPLConditionType::INVALID_TYPE;
  int cur_err_code = OB_SUCCESS;
  static const uint32_t STR_LEN = 128;
  char err_msg[STR_LEN] = {0};
  const ObSqlExpression *errcode_expr = nullptr;
  const ObSqlExpression *errmsg_expr = nullptr;

  CK (OB_NOT_NULL(ctx), ctx->valid());
  CK (OB_NOT_NULL(ctx->func_));
  CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(sqlcode_info = ctx->exec_ctx_->get_my_session()->get_pl_sqlcode_info()));
  OX (wb = common::ob_get_tsi_warning_buffer());
  CK (OB_NOT_NULL(error_code));
  OX (GET_NULLABLE_EXPR_BY_IDX(ctx, errcode_expr_idx, errcode_expr));
  OX (GET_NULLABLE_EXPR_BY_IDX(ctx, errmsg_expr_idx, errmsg_expr));
  CK (OB_NOT_NULL(ctx->get_top_expr_allocator()));

#define CALC(expr, type, result) \
  do { \
    ObObjParam tmp; \
    ObExprResType expected_type; \
    OZ (spi_calc_expr(ctx, expr, OB_INVALID_INDEX, &tmp)); \
    OX (expected_type.set_##type()); \
    OX (expected_type.set_collation_type(tmp.get_collation_type())); \
    OZ (spi_convert(ctx->exec_ctx_->get_my_session(), ctx->get_top_expr_allocator(), tmp, expected_type, result)); \
  } while(0)

  if (OB_SUCC(ret)) {
    type = ObPLEH::eh_classify_exception(sql_state);
    if (OB_NOT_NULL(wb)) {
      cur_err_code = wb->get_err_code();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(errcode_expr)) {
    if (!is_signal && *error_code != -1 && NULL == sql_state) {
      sqlcode_info->set_sqlcode(*error_code);
    } else {
      if (type == ObPLConditionType::NOT_FOUND) {
        sqlcode_info->set_sqlcode(ER_SIGNAL_NOT_FOUND);
      } else {
        sqlcode_info->set_sqlcode(ER_SIGNAL_EXCEPTION);
      }
    }
  } else {
    CALC(errcode_expr, int32, result);
    if (OB_SUCC(ret)) {
      if (result.is_null()) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_WARN("error code is NULL", K(ret));
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, static_cast<int32_t>(STRLEN("MYSQL_ERRNO")), "MYSQL_ERRNO",
                       static_cast<int32_t>(STRLEN("NULL")), "NULL");
      } else if (result.get_int32() <= 0 || result.get_int32() > 65535) {
        char mysql_errno[20] = {0};
        sprintf(mysql_errno, "%d", result.get_int32());
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_WARN("error code is over 65535", K(ret), K(result.get_int32()));
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, static_cast<int32_t>(STRLEN("MYSQL_ERRNO")), "MYSQL_ERRNO",
                       static_cast<int32_t>(STRLEN(mysql_errno)), mysql_errno);
      }
    }
    OX (sqlcode_info->set_sqlcode(result.get_int32()));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(errmsg_expr)) {
      if (is_signal) {
        if (type == ObPLConditionType::NOT_FOUND) {
          sqlcode_info->set_sqlmsg(ObString("Unhandled user-defined not found condition"));
        } else {
          sqlcode_info->set_sqlmsg(ObString("Unhandled user-defined exception condition"));
        }
      } else {
        int64_t idx;
        CK (sqlcode_info->get_stack_warning_buf().count() > 0);
        OX (idx = sqlcode_info->get_stack_warning_buf().count() - 1);
        OX (sqlcode_info->set_sqlmsg(sqlcode_info->get_stack_warning_buf().at(idx).get_err_msg()));
      }
    } else {
      CALC(errmsg_expr, varchar, result);
      if (OB_SUCC(ret)) {
        if (result.is_null()) {
          ret = OB_ERR_WRONG_VALUE_FOR_VAR;
          LOG_WARN("error code is NULL", K(ret));
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, static_cast<int32_t>(STRLEN("MESSAGE_TEXT")), "MESSAGE_TEXT",
                        static_cast<int32_t>(STRLEN("NULL")), "NULL");
        }
      }
      OX (sqlcode_info->set_sqlmsg(result.get_string()));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    snprintf(err_msg, STR_LEN, "%.*s", sqlcode_info->get_sqlmsg().length(), sqlcode_info->get_sqlmsg().ptr());
    if (ObPLConditionType::SQL_WARNING == type) {
      if (OB_ISNULL(errcode_expr)) {
        if (OB_NOT_NULL(wb)) {
          wb->append_warning(err_msg, OB_ERR_SIGNAL_WARN);
        }
      } else {
        if (OB_NOT_NULL(wb)) {
          wb->append_warning(err_msg, sqlcode_info->get_sqlcode(), sql_state);
        }
      }
      if (is_signal) {
        sqlcode_info->set_sqlcode(OB_SUCCESS);
        if (OB_NOT_NULL(wb)) {
          wb->reset_err();
        }
      }
    } else {
      if (is_signal) {
        LOG_MYSQL_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR,
                            MIN(sqlcode_info->get_sqlmsg().length(), STR_LEN),
                            sqlcode_info->get_sqlmsg().ptr());
        if (OB_NOT_NULL(wb)) {
          wb->set_error_code(sqlcode_info->get_sqlcode());
          wb->set_sql_state(sql_state);
        }
      } else {
        if (OB_NOT_NULL(wb)) {
          wb->set_error(err_msg, sqlcode_info->get_sqlcode());
          wb->set_sql_state(sql_state != NULL
                             ? sql_state
                             : (resignal_sql_state != NULL) ? resignal_sql_state : ob_sqlstate(cur_err_code));
        }
      }
      *error_code = sqlcode_info->get_sqlcode();
    }
  }
  return ret;
}

int ObSPIService::spi_trim_collection(pl::ObPLExecCtx *ctx,
                                   const int64_t collection_expr_idx,
                                   int64_t row_size,
                                   const int64_t n_expr_idx)
{
  int ret = OB_SUCCESS;
  const ObSqlExpression *collection_expr = nullptr;
  const ObSqlExpression *n_expr = nullptr;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, collection_expr_idx, row_size, n_expr_idx);
#else
  UNUSED(row_size);
  CK (OB_NOT_NULL(ctx),
      OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->func_));
  CK (collection_expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= collection_expr_idx && collection_expr_idx < ctx->func_->get_expressions().count()));
  OX (collection_expr = ctx->func_->get_expressions().at(collection_expr_idx));
  OX (GET_NULLABLE_EXPR_BY_IDX(ctx, n_expr_idx, n_expr));
  if (OB_SUCC(ret)) {
    ObObjParam result;
    ObPLCollection *table = NULL;
    int64_t n = OB_INVALID_INDEX;

    CK (OB_NOT_NULL(collection_expr));
    OZ (spi_calc_expr(ctx, collection_expr, OB_INVALID_INDEX, &result));
    CK (OB_LIKELY(ObExtendType == result.get_type()));
    CK (OB_LIKELY(result.get_ext() != 0));
    CK (OB_NOT_NULL(table = reinterpret_cast<ObPLCollection*>(result.get_ext())));
    CK (OB_LIKELY(table->get_column_count() != 0));

    if (OB_SUCC(ret) && !table->is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("OBE-06531: Reference to uninitialized collection", K(ret));
    }
    if (OB_SUCC(ret) && table->is_associative_array()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("associative array is not support trim operation", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "associative array trim operation");
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(n_expr)) {
        OZ (spi_calc_expr(ctx, n_expr, OB_INVALID_INDEX, &result));
        if (OB_FAIL(ret)) {
        } else if (result.is_null()) {
          n = 0;
        } else {
          GET_INTEGER_FROM_OBJ(result, n);
        }
      } else {
        // coll.trim/ coll.trim() 无参，删除最后一个
        n = 1;
      }
      if (OB_SUCC(ret)) {
        if (0 > n) {
          ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
        } else if (0 == n) {
          // do nothing ...
        } else if (n > table->get_count()) {
          // raise exception
          ret = OB_ERR_SUBSCRIPT_BEYOND_COUNT;
        } else {
          OZ (table->trim_collection_elem(n));
        }
      }
    }
  }
#endif
  return ret;
}

int ObSPIService::spi_delete_collection(pl::ObPLExecCtx *ctx,
                                        const int64_t collection_expr_idx,
                                        int64_t row_size,
                                        const int64_t m_expr_idx,
                                        const int64_t n_expr_idx)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, collection_expr_idx, row_size, m_expr_idx, n_expr_idx);
#else
  UNUSED(row_size);
  const ObSqlExpression *collection_expr = nullptr;
  const ObSqlExpression *m_expr = nullptr;
  const ObSqlExpression *n_expr = nullptr;
  CK (OB_NOT_NULL(ctx),
      OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->func_));
  CK (collection_expr_idx != OB_INVALID_ID);
  CK (OB_LIKELY(0 <= collection_expr_idx && collection_expr_idx < ctx->func_->get_expressions().count()));

  if (OB_SUCC(ret)) {
    ObObjParam result;
    ObPLCollection *table = NULL;
    int64_t m = OB_INVALID_INDEX;
    int64_t n = OB_INVALID_INDEX;

    collection_expr = ctx->func_->get_expressions().at(collection_expr_idx);
    OX (GET_NULLABLE_EXPR_BY_IDX(ctx, m_expr_idx, m_expr));
    OX (GET_NULLABLE_EXPR_BY_IDX(ctx, n_expr_idx, n_expr));

    OZ (spi_calc_expr(ctx, collection_expr, OB_INVALID_INDEX, &result));
    CK (OB_LIKELY(result.get_type() == ObExtendType));
    CK (OB_LIKELY(result.get_ext() != 0));
    CK (OB_NOT_NULL(table = reinterpret_cast<ObPLCollection*>(result.get_ext())));
    CK (OB_LIKELY(table->get_column_count() != 0));

    CK (!(OB_ISNULL(m_expr) && OB_NOT_NULL(n_expr)));
    if (OB_SUCC(ret) && !table->is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("OBE-06531: Reference to uninitialized collection", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(m_expr) && OB_ISNULL(n_expr)) {
        // delete all data
        if (table->get_count() != 0) {
          ObObj tmp;
          tmp.set_extend(reinterpret_cast<int64_t>(table), table->get_type());
          OZ (ObUserDefinedType::destruct_obj(tmp, nullptr, true));
          OX (table->set_count(0));
        }
      } else if (OB_NOT_NULL(m_expr) && OB_ISNULL(n_expr)) {
        // delete table(m)
        OZ (spi_calc_expr(ctx, m_expr, OB_INVALID_INDEX, &result));
        if (OB_SUCC(ret) && 0 < table->get_count() && !result.is_null()) {
          if (table->is_nested_table()) {
            GET_INTEGER_FROM_OBJ(result, m);
            if (OB_SUCC(ret)) {
              if (m > table->get_count() || m <= 0) {
                /*
                 * From an associative array or nested table (but not a varray):
                 * – DELETE(n) deletes the element whose index is n, if that element exists;
                 * otherwise, it does nothing.
                 * – DELETE(m,n) deletes all elements whose indexes are in the range m..n, if
                 * both m and n exist and m <= n; otherwise, it does nothing.
                 * */
              } else {
                OX (table->delete_collection_elem(m - 1));
              }
            }
          } else if (table->is_varray()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("varray type is not able to delete with index", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "delete varray type with index");
          } else if (table->is_associative_array()) {
            ObObj *key1 = static_cast<ObObj *>(&result);
            ObPLAssocArray *atable = static_cast<ObPLAssocArray *>(table);
            ObObj *key = atable->get_key();
            // 到这儿count必然大于0，可以直接拿来比较
            OV (key!=NULL ? key[0].can_compare(*key1) : key1->is_integer_type(),
                  OB_ERR_UNEXPECTED, K(key[0]), KPC(key1));
            // key 不可能重复，如果有重复是一个bug
            for (int64_t i = 0; OB_SUCC(ret) && i < atable->get_count(); ++i) {
              if (key!=NULL ? key[i]==*key1 : (result.get_int32() == i)) {
                OX (atable->delete_collection_elem(i));
                if (atable->get_first() - 1 == i) {
                  atable->update_first();
                }
                if (atable->get_last() - 1 == i) {
                  atable->update_last();
                }
                break;
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("illegal table type detected.", K(*table));
          }
        }
      } else if (OB_NOT_NULL(m_expr) && OB_NOT_NULL(n_expr)) {
        // delete table(m,n)
        ObObjParam result1;
        OZ (spi_calc_expr(ctx, m_expr, OB_INVALID_INDEX, &result));
        OZ (spi_calc_expr(ctx, n_expr, OB_INVALID_INDEX, &result1));
        if (OB_SUCC(ret) && 0 < table->get_count() && !result.is_null() && !result1.is_null()) {
          if (table->is_nested_table()) {
            GET_INTEGER_FROM_OBJ(result, m);
            GET_INTEGER_FROM_OBJ(result1, n);
            if (OB_SUCC(ret)) {
              m = m < 0 ? 0 : m > table->get_count() ? table->get_count() + 1 : m;
              n = n < 0 ? 0 : n > table->get_count() ? table->get_count() + 1 : n;
              if (m < n || ((m == n && m != 0 && m != table->get_count()))) {
                m = 0 == m ? 1 : m;
                n = (table->get_count() + 1) == n ? table->get_count() : n;
                for (int64_t i = m - 1; OB_SUCC(ret) && i <= n - 1; ++i) {
                  OZ (table->delete_collection_elem(i));
                }
              }
            }
          } else if (table->is_varray()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("varray type is not able to delete with index", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "delete varray type with index");
          } else if (table->is_associative_array()) {

            ObObj *key1 = static_cast<ObObj *>(&result);
            ObObj *key2 = static_cast<ObObj *>(&result1);
            ObPLAssocArray *atable = static_cast<ObPLAssocArray *>(table);
            ObObj *key = atable->get_key();
            CK (OB_NOT_NULL(key1));
            CK (OB_NOT_NULL(key2));
            CK (key!=NULL ? key1->can_compare(*key2) && key[0].can_compare(*key1)
                          : (key1->is_integer_type() && key2->is_integer_type()));
            if (OB_SUCC(ret)) {
              if (*key1 <= *key2) {
                for (int64_t i = 0; OB_SUCC(ret) && i < atable->get_count(); ++i) {
                  bool flag = key!=NULL ? (key[i] >= *key1 && key[i] <= *key2)
                                        : (i+1 >= key1->get_int32() && i+1 <= key2->get_int32());
                  if (flag) {
                    OZ (atable->delete_collection_elem(i));
                    if (atable->get_first() - 1 == i) {
                      atable->update_first();
                    }
                    if (atable->get_last() - 1 == i) {
                      atable->update_last();
                    }
                  }
                }
              } else {
                // do nothing;
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("illegal table type detected.", K(*table));
          }
        }
      }
    }
  }
  SET_SPI_STATUS;
#endif
  return ret;
}
#undef GET_INTEGER_FROM_OBJ
int ObSPIService::spi_destruct_collection(ObPLExecCtx *ctx, int64_t idx)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, idx);
#else
  if (OB_ISNULL(ctx)
      || OB_ISNULL(ctx->exec_ctx_)
      || OB_ISNULL(ctx->exec_ctx_->get_my_session())
      || OB_INVALID_INDEX == idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(idx), K(ret));
  } else {
    ParamStore *params = ctx->params_;
    if (OB_ISNULL(params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Argument in pl context is NULL", K(params), K(ret));
    } else if (OB_FAIL(ObUserDefinedType::destruct_objparam(*ctx->allocator_, params->at(idx), ctx->exec_ctx_->get_my_session()))) {
      LOG_WARN("collection is NULL", K(idx), K(params->at(idx).get_int()), K(ret));
    } else { /*do nothing*/ }
  }
  SET_SPI_STATUS;
#endif
  return ret;
}

int ObSPIService::spi_sub_nestedtable(ObPLExecCtx *ctx, int64_t src_idx, int64_t dst_idx, int32_t lower, int32_t upper)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, src_idx, dst_idx, lower, upper);
#else
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->params_));
  CK (OB_LIKELY(src_idx >= 0 && src_idx < ctx->params_->count()));
  CK (OB_LIKELY(dst_idx >= 0 && dst_idx < ctx->params_->count()));
  CK (OB_LIKELY(lower <= upper));
  if (OB_SUCC(ret)) {
    ParamStore *params = ctx->params_;
    ObObjParam& src_obj = params->at(src_idx);
    ObObjParam& dst_obj = params->at(dst_idx);
    ObPLCollection *src_coll = reinterpret_cast<ObPLCollection*>(src_obj.get_ext());
    CK (OB_LIKELY(src_obj.is_ext()));
    CK (OB_LIKELY(dst_obj.is_ext() || dst_obj.is_null()));
    CK (OB_NOT_NULL(src_coll));
    CK (OB_NOT_NULL(ctx->allocator_));
    if (OB_SUCC(ret)) {
      ObPLCollection *dst_coll = NULL;
      ObObj *data_ptr = src_coll->get_data();
      ObPLAllocator1 *coll_alloc = nullptr;
      CK (OB_NOT_NULL(data_ptr));
      if (lower <= 0 // 检查是否越界, lower,upper从1开始
          || lower > src_coll->get_count()) {
        ret = OB_ELEMENT_AT_GIVEN_INDEX_NOT_EXIST;
        LOG_USER_ERROR(OB_ELEMENT_AT_GIVEN_INDEX_NOT_EXIST, static_cast<int64_t>(lower));
        LOG_WARN("ORA-22160: element at index does not exist",
                 K(lower), K(upper), K(src_coll->get_count()), K(ret));
      } else if (upper > src_coll->get_count()) {
        ret = OB_ELEMENT_AT_GIVEN_INDEX_NOT_EXIST;
        LOG_USER_ERROR(OB_ELEMENT_AT_GIVEN_INDEX_NOT_EXIST, src_coll->get_count() + 1);
        LOG_WARN("OBE-22160: element at index does not exist",
                 K(lower), K(upper), K(src_coll->get_count()), K(ret));
      } else if (OB_ISNULL(dst_coll = static_cast<ObPLCollection*>(ctx->allocator_->alloc(sizeof(ObPLCollection))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc dst collection memory", K(ret));
      } else if (FALSE_IT(dst_coll = new(dst_coll)ObPLNestedTable(src_coll->get_id()))) {
      } else if (OB_FAIL(dst_coll->init_allocator(*ctx->allocator_, true))) {
        LOG_WARN("failed to init allocator", K(ret));
        ctx->allocator_->free(dst_coll);
      } else if (OB_ISNULL(coll_alloc = dynamic_cast<ObPLAllocator1 *>(dst_coll->get_allocator()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collection allocator", K(ret));
      } else {
        ObObj *dst_data = nullptr;
        int64_t count = upper - lower + 1;
        data_ptr = data_ptr + (lower - 1);
        if (OB_ISNULL(dst_data = reinterpret_cast<ObObj *>(coll_alloc->alloc(count * sizeof(ObObj))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for collection", K(ret), K(count));
        }
        int64_t i = 0;
        for (; OB_SUCC(ret) && i < count; ++i) {
          OX (dst_data[i].reset());
          OZ (deep_copy_obj(*coll_alloc, data_ptr[i], dst_data[i]));
        }
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(dst_data)) {
            for (int j = 0; j < i - 1; ++j) {
              ObUserDefinedType::destruct_objparam(*coll_alloc,
                                                  dst_data[j],
                                                  ctx->exec_ctx_->get_my_session());
            }
            coll_alloc->free(dst_data);
          }
          coll_alloc->~ObPLAllocator1();
          ctx->allocator_->free(coll_alloc);
          ctx->allocator_->free(dst_coll);
        } else {
          dst_coll->set_data(dst_data);
          dst_coll->set_type(PL_NESTED_TABLE_TYPE);
          dst_coll->set_element_type(src_coll->get_element_type());
          dst_coll->set_column_count(src_coll->get_column_count());
          dst_coll->set_first(OB_INVALID_INDEX);
          dst_coll->set_last(OB_INVALID_INDEX);
          dst_coll->set_count(count);
          dst_coll->set_element_type(src_coll->get_element_type());
          if (dst_obj.get_ext() != 0) {
            ObUserDefinedType::destruct_objparam(*ctx->allocator_,
                                                  dst_obj,
                                                  ctx->exec_ctx_->get_my_session());
          }
          dst_obj.set_extend(reinterpret_cast<int64_t>(dst_coll), dst_coll->get_type());
          dst_obj.set_param_meta();
        }
      }
    }
  }
#endif
  return ret;
}

int ObSPIService::spi_init_collection(ObPLExecCtx *ctx, ObPLCollection *src, ObPLCollection *dest, int64_t row_size, uint64_t package_id)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, src, dest, row_size, package_id);
#else
  if (OB_ISNULL(ctx)
      || OB_ISNULL(ctx->exec_ctx_)
      || OB_ISNULL(ctx->exec_ctx_->get_my_session())
      || OB_ISNULL(src)
      || OB_ISNULL(dest)
      || row_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(src), K(dest), K(row_size), K(ret));
  } else if (!src->is_inited()) { // 如果源collection未初始化, 将目标也设置为未初始化
    dest->set_count(OB_INVALID_COUNT);
    dest->set_first(OB_INVALID_INDEX);
    dest->set_last(OB_INVALID_INDEX);
    dest->set_data(NULL);
  } else {
    ObIAllocator *allocator = NULL;
    if (package_id != OB_INVALID_ID) {
      OZ (spi_get_package_allocator(ctx, package_id, allocator));
    } else {
      allocator = ctx->allocator_;
    }
    CK (OB_NOT_NULL(allocator));
    OZ (spi_set_collection(ctx->exec_ctx_->get_my_session()->get_effective_tenant_id(),
                             ctx, *allocator, *dest, src->get_count()), src->get_count());
    CK (OB_NOT_NULL(dest->get_allocator()));
    if (OB_SUCC(ret)) {
      switch (src->get_type()) {
      case PL_NESTED_TABLE_TYPE: {
        // do nothing
      }
      break;
      case PL_ASSOCIATIVE_ARRAY_TYPE: {
        ObObj *key = NULL;
        int64_t *sort = NULL;
        ObPLAssocArray *src_aa = static_cast<ObPLAssocArray*>(src);
        ObPLAssocArray *dest_aa = static_cast<ObPLAssocArray*>(dest);
        CK (OB_NOT_NULL(src_aa));
        CK (OB_NOT_NULL(dest_aa));
        if (OB_SUCC(ret) && src->get_count() > 0) {
          if (NULL != src_aa->get_sort() && NULL != src_aa->get_key()) {
            key = static_cast<ObObj*>(dest->get_allocator()->alloc(src->get_count() * sizeof(ObObj)));
            sort = static_cast<int64_t*>(dest->get_allocator()->alloc(src->get_count() * sizeof(int64_t)));
            CK (OB_NOT_NULL(key));
            CK (OB_NOT_NULL(sort));
            for (int64_t i = 0; OB_SUCC(ret) && i < src->get_count(); ++i) {
              OZ (deep_copy_obj(*dest->get_allocator(), *src_aa->get_key(i), key[i]));
              OX (sort[i] = src_aa->get_sort(i));
            }
          } else if (NULL == src_aa->get_sort() && NULL == src_aa->get_key()) {
            //Associative array的优化
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected associative array", K(*src), K(*dest), K(row_size), K(package_id), K(ret));
          }
        }
        OX (dest_aa->set_key(key));
        OX (dest_aa->set_sort(sort));
      }
      break;
      case PL_VARRAY_TYPE: {
        ObPLVArray *src_va = static_cast<ObPLVArray*>(src);
        ObPLVArray *dest_va = static_cast<ObPLVArray*>(dest);
        CK (OB_NOT_NULL(src_va));
        CK (OB_NOT_NULL(dest_va));
        OX (dest_va->set_capacity(src_va->get_capacity()));
      }
      break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src is not a collection", K(ctx), K(*src), K(*dest), K(row_size), K(package_id), K(ret));
      }
      break;
      }
    }
  }
  SET_SPI_STATUS;
#endif
  return ret;
}

int ObSPIService::spi_set_collection(int64_t tenant_id,
                                       const ObPLINS *ns,
                                       ObIAllocator &allocator,
                                       ObPLCollection &coll,
                                       int64_t n,
                                       bool extend_mode)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(tenant_id, ns, allocator, coll, n, extend_mode);
#else
#define CHECK_VARRAY_CAPACITY \
  do { \
    if (OB_SUCC(ret)) { \
      if (coll.is_varray()) { \
        ObPLVArray &va = static_cast<ObPLVArray&>(coll); \
        if (va.get_capacity() < va.get_count() + n) { \
          ret = OB_SIZE_OVERFLOW; \
          LOG_WARN("varray size overflow, OBE-06532: Subscript outside of limit", K(va), K(va.get_capacity()), K(n), K(coll.get_count()), K(ret)); \
        } \
      } \
    } \
  } while (0)

#define CHECK_NOT_NULL \
  do { \
    if (OB_SUCC(ret)) { \
      if (coll.is_not_null()) { \
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR; \
        LOG_WARN("not null check violated", K(coll), K(n), K(extend_mode), K(coll.is_not_null()), K(coll.get_count()), K(ret)); \
      } \
    } \
  } while (0)

#define SET_COLLECTION_INFO \
  do { \
    if (OB_SUCC(ret)) { \
      coll.set_data(reinterpret_cast<ObObj*>(data)); \
      coll.set_count(coll.get_count() + n); \
      if (coll.is_associative_array()) { \
        /*associative array的first和last不在这里设置，需要根据数据计算*/ \
      } else { \
        OB_INVALID_INDEX == coll.get_first() ? coll.set_first(1) : (void)NULL;  /*first和last从1开始*/ \
        coll.set_last(coll.get_count()); \
      } \
    } \
  } while (0)

  UNUSED(tenant_id);
  ObPLAllocator1 *collection_allocator = NULL;
  void *data = NULL;
  bool set_data = false;
  if (extend_mode && !coll.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("OBE-06531: Reference to uninitialized collection", K(ret));
  } else if (NULL == coll.get_allocator()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must init allcator first", K(ret));
  } else if (!coll.is_inited()) {
    coll.set_count(0);
    coll.set_first(OB_INVALID_INDEX);
    coll.set_last(OB_INVALID_INDEX);
    coll.set_data(NULL);
  }
  if (OB_SUCC(ret)) {
    if (extend_mode && n > 0) {

      CHECK_VARRAY_CAPACITY;

      // CHECK_NOT_NULL;

      if (OB_SUCC(ret)) {
        data = coll.get_allocator()->alloc(sizeof(ObObj) * (coll.get_count() + n));
        if (OB_ISNULL(data)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed",
                   K(sizeof(ObObj) * (coll.get_count() + n)),
                   K(n),
                   K(coll.get_count()),
                   K(ret));
        } else {
          ObObj *obj_data = reinterpret_cast<ObObj*>(data);
          for (int64_t i = coll.get_count(); OB_SUCC(ret) && i < coll.get_count() + n; ++i) {
            new (obj_data + i) ObObj();
          }
        }
        LOG_DEBUG("PL/SQL realloc collection memory",
                 K(coll.get_data()),
                 K(coll.get_count()),
                 K(n),
                 K(coll.get_allocator()->total()),
                 K(coll.get_allocator()->used()));

        CK (coll.get_column_count() > 0);
        int64_t i = 0;
        for (; OB_SUCC(ret) && i < n; ++i) {
          /*
           * extend模式需要把下层结构递归分配出来，因为在下面这个case里extend之后下面就可以直接访问内存了
           * declare
           *  TYPE rec_type IS RECORD(a int, b varchar(10));
           *  TYPE tab_type IS TABLE OF rec_type;
           *  v_tab1 tab_type := tab_type();
           *  begin
           *      v_tab1.extend;
           *      v_tab1(1).a := 1;
           *      v_tab1(1).b := 'a';
           *      end;
           */
          ObObj* row = &(reinterpret_cast<ObObj*>(data)[(coll.get_count() + i)]);
          if (!coll.get_element_desc().is_composite_type() || NULL == ns) {
            new (row) ObObj(ObNullType);
          } else {
            const ObUserDefinedType *type = NULL;
            const ObCollectionType *collection_type = NULL;
            int64_t ptr = 0;
            int64_t init_size = OB_INVALID_SIZE;
            CK (OB_NOT_NULL(ns));
            OZ (ns->get_user_type(coll.get_id(), type));
            CK (OB_NOT_NULL(type));
            CK (type->is_collection_type());
            CK (OB_NOT_NULL(collection_type = static_cast<const ObCollectionType*>(type)));
            OZ (collection_type->get_element_type().newx(*coll.get_allocator(), ns, ptr));
            if (OB_FAIL(ret)) {
            } else if (collection_type->get_element_type().is_collection_type()) {
              ObPLCollection *collection = NULL;
              CK (OB_NOT_NULL(collection = reinterpret_cast<ObPLCollection*>(ptr)));
              OX (collection->set_count(0));
            } else if (collection_type->get_element_type().is_record_type()) {
              ObPLRecord *record = NULL;
              CK (OB_NOT_NULL(record = reinterpret_cast<ObPLRecord*>(ptr)));
              OX (record->set_null());
            }
            OZ (collection_type->get_element_type().get_size(PL_TYPE_INIT_SIZE, init_size));
            OX (row->set_extend(ptr, collection_type->get_element_type().get_type(), init_size));
          }
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(data)) {
          for (int j = 0; j < i; ++j) {
            ObObj* row = &(reinterpret_cast<ObObj*>(data)[(coll.get_count() + j)]);
            ObUserDefinedType::destruct_objparam(*coll.get_allocator(), *row, nullptr);
          }
          coll.get_allocator()->free(data);
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(coll.get_data())) {
          MEMCPY(data, coll.get_data(), sizeof(ObObj) * coll.get_count());
          coll.get_allocator()->free(coll.get_data());
        }
        SET_COLLECTION_INFO;
      }
    } else {
      ObObj tmp;
      tmp.set_extend(reinterpret_cast<int64_t>(&coll), coll.get_type());
      OZ (ObUserDefinedType::destruct_obj(tmp, nullptr, true));
      OX (coll.set_count(0));
      OX (set_data = true);
    }
  }

  if (OB_SUCC(ret) && set_data && n > 0) {
    CHECK_VARRAY_CAPACITY;

    // CHECK_NOT_NULL;

    if (OB_SUCC(ret)) {
      data = coll.get_allocator()->alloc(sizeof(ObObj) * n);
      if (OB_ISNULL(data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(sizeof(ObObj) * n), K(n), K(coll), K(ret));
      } else {
        CK (coll.get_column_count() > 0);
        //非extend模式仅扩展出n个Obj结构并初始化为NULL，下层结构不分配
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          new (&reinterpret_cast<ObObj*>(data)[i])ObObj(ObNullType);
        }
        if (OB_SUCC(ret)) {
          SET_COLLECTION_INFO;
        } else {
          coll.get_allocator()->free(data);
        }
      }
    }
  }
#endif
  return ret;
}

int ObSPIService::spi_reset_composite(ObPLComposite *composite,
                                      bool is_record,
                                      int32_t member_idx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(composite));
  if (OB_SUCC(ret)) {
    if (is_record) {
      ObObj obj;
      ObIAllocator *pl_allocator = nullptr;
      ObPLRecord *record = static_cast<ObPLRecord *>(composite);
      CK (member_idx >= 0 && member_idx < record->get_count());
      CK (OB_NOT_NULL(pl_allocator = record->get_allocator()));
      OZ (record->get_element(member_idx, obj));
      OZ (ObUserDefinedType::destruct_objparam(*pl_allocator, obj, nullptr));
    } else {
      /*
      * Oracle support assign NULL to a Collection, it means making collection uninitialized.
      *
      * Example:
      * CREATE OR REPLACE TYPE ARRYTYPE is table of Varchar2(10);
      * Type created.
      *
      * SQL>
      * declare TYPE ARRYTYPE is table of Varchar2(10);
      * arr ARRYTYPE := ARRYTYPE('a','b','c');
      * begin
      * dbms_output.put_line(arr.count || arr(3));
      * arr := NULL;
      * dbms_output.put_line(arr.count || arr(3));
      * end;
      * /
      *
      * declare TYPE ARRYTYPE is table of Varchar2(10);
      * ERROR at line 1:
      * ORA-06531: Reference to uninitialized collection
      * ORA-06512: at line 6
      * */
      ObPLCollection *coll = static_cast<ObPLCollection *>(composite);
      if (OB_NOT_NULL(coll->get_allocator())) {
        OZ (spi_set_collection(MTL_ID(), NULL, *(coll->get_allocator()), *coll, 0, false));
        OX (coll->set_count(OB_INVALID_COUNT));
      } else {
        CK (coll->get_count() == 0 || coll->get_count() == OB_INVALID_COUNT);
        OX (coll->set_count(OB_INVALID_COUNT));
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObSPIService::spi_extend_assoc_array(int64_t tenant_id,
                                         const ObPLINS *ns,
                                         ObIAllocator &allocator,
                                         ObPLAssocArray &assoc_array,
                                         int64_t n)
{
  int ret = OB_SUCCESS;
  assoc_array.is_inited() ? (void)NULL : assoc_array.set_inited();
  if (OB_FAIL(spi_set_collection(tenant_id, ns, allocator, assoc_array, n, true))) {
    LOG_WARN("failed to spi_reset_composite", K(assoc_array), K(ret));
  } else if (OB_ISNULL(assoc_array.get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(assoc_array), K(n), K(ret));
  } else if (NULL == dynamic_cast<ObPLAllocator1*>(assoc_array.get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected allocator used in Collection", K(assoc_array), K(n), K(ret));
  } else if (NULL == assoc_array.get_key()) {
    if (NULL != assoc_array.get_sort()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key is NULL, but sort is not NULL", K(assoc_array), K(n), K(ret));
    }

#define ALLOC_ASSOC_ARRAY(TYPE, PROPERTY) \
  do { \
    if (OB_SUCC(ret)) { \
      TYPE *addr = NULL; \
      ObPLAllocator1 *allocator = static_cast<ObPLAllocator1*>(assoc_array.get_allocator()); \
      if (OB_ISNULL(allocator)) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN("failed to static cast pl allocator", \
                 K(ret), K(allocator), K(assoc_array.get_allocator())); \
      } else if (OB_ISNULL(allocator->get_allocator())) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN("allocator is null", K(ret), K(allocator), K(assoc_array.get_allocator())); \
      } else if (OB_ISNULL(addr = static_cast<TYPE *>(allocator->get_allocator()->alloc(sizeof(TYPE) * n)))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("alloc failed", K(assoc_array), K(n), K(sizeof(TYPE)), K(ret)); \
      } else { \
        assoc_array.set_##PROPERTY(addr); \
      } \
    } \
  } while(0)

    ALLOC_ASSOC_ARRAY(ObObj, key);

    ALLOC_ASSOC_ARRAY(int64_t, sort);
    if (nullptr != assoc_array.get_key() && nullptr == assoc_array.get_sort()) {
      if (OB_NOT_NULL(assoc_array.get_allocator())) {
        assoc_array.get_allocator()->free(assoc_array.get_key());
      }
      assoc_array.set_key(nullptr);
    }

    // NOTE: init extend key avoid to deep copy core!!!
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < assoc_array.get_count(); ++i) {
        ObObj *key = assoc_array.get_key(i);
        key = new(key)ObObj();
      }
    }

  } else {

#define REALLOC_ASSOC_ARRAY(TYPE, PROPERTY) \
  do { \
    if (OB_SUCC(ret)) { \
      TYPE *addr = NULL; \
      ObPLAllocator1 *allocator = static_cast<ObPLAllocator1*>(assoc_array.get_allocator()); \
      if (OB_ISNULL(allocator)) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN("failed to static cast pl allocator", \
                 K(ret), K(allocator), K(assoc_array.get_allocator())); \
      } else if (OB_ISNULL(allocator->get_allocator())) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN("allocator is null", K(ret), K(allocator), K(assoc_array.get_allocator())); \
      } else if (OB_ISNULL(addr = reinterpret_cast<TYPE*>(allocator->get_allocator()->alloc(sizeof(TYPE) * assoc_array.get_count())))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("alloc failed", K(ret), K(assoc_array), K(n), K(sizeof(TYPE) * (assoc_array.get_count() - n)), K(sizeof(TYPE) * assoc_array.get_count())); \
      } \
      OX (OB_NOT_NULL(assoc_array.get_##PROPERTY()) ? MEMCPY(addr, assoc_array.get_##PROPERTY(), sizeof(TYPE) * (assoc_array.get_count() - n)) \
                                                    : (void*)(NULL)); \
      OX (OB_NOT_NULL(assoc_array.get_##PROPERTY()) ? assoc_array.get_allocator()->free(assoc_array.get_##PROPERTY()) : void(NULL)); \
      OX (assoc_array.set_##PROPERTY(addr)); \
    } \
  } while(0)

    REALLOC_ASSOC_ARRAY(ObObj, key);

    REALLOC_ASSOC_ARRAY(int64_t, sort);
    if (OB_FAIL(ret)) {
      for (int64_t i = assoc_array.get_count() - 1; i >= assoc_array.get_count() - n; --i) {
        ObObj &obj = assoc_array.get_data()[i];
        if (obj.is_pl_extend()) {
          int tmp = ObUserDefinedType::destruct_objparam(*assoc_array.get_allocator(), obj, nullptr);
          if (OB_SUCCESS != tmp) {
            LOG_WARN("fail to destruct obj", K(tmp), K(ret));
          }
        } else {
          void * ptr = obj.get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            assoc_array.get_allocator()->free(ptr);
          }
        }
      }
      assoc_array.set_count(assoc_array.get_count() - n);
    }

    // NOTE: init extend key avoid to deep copy core!!!
    if (OB_SUCC(ret)) {
      for (int64_t i = assoc_array.get_count() - 1; i >= assoc_array.get_count() - n; --i) {
        ObObj *key = assoc_array.get_key(i);
        key = new(key)ObObj();
      }
    }

    LOG_DEBUG("spi extend assoc array", K(ret), K(assoc_array));
  }
  return ret;
}
#endif

int ObSPIService::spi_get_package_allocator(
  ObPLExecCtx *ctx, uint64_t package_id, ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  ObPL *pl_engine = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(GCTX.schema_service_));
  CK (OB_NOT_NULL(exec_ctx = ctx->exec_ctx_));
  CK (OB_NOT_NULL(session_info = exec_ctx->get_my_session()));
  CK (OB_NOT_NULL(sql_proxy = exec_ctx->get_sql_proxy()));
  CK (OB_NOT_NULL(pl_engine = exec_ctx->get_my_session()->get_pl_engine()));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(
                            session_info->get_effective_tenant_id(),
                            schema_guard));
  ObPLPackageGuard package_guard(session_info->get_effective_tenant_id());
  OZ (package_guard.init());
  OX (allocator = NULL);
  if (OB_SUCC(ret)) {
    ObPLPackageState *package_state = NULL;
    ObPLPackageManager &pl_manager = pl_engine->get_package_manager();
    ObPLResolveCtx resolve_ctx(exec_ctx->get_allocator(),
                               *session_info,
                               schema_guard,
                               ctx->guard_ != NULL ? *(ctx->guard_) : package_guard,
                               *sql_proxy,
                               false);
    OZ (pl_manager.get_package_state(
      resolve_ctx, *exec_ctx, package_id, package_state), package_id);
    CK (OB_NOT_NULL(package_state));
    OX (allocator = &package_state->get_pkg_allocator());
  }
  return ret;
}

int ObSPIService::spi_add_ref_cursor_refcount(ObPLExecCtx *ctx, ObObj *cursor, int64_t addend)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor_info = NULL;
  CK (OB_NOT_NULL(cursor));
  OX (cursor_info = reinterpret_cast<ObPLCursorInfo *>(cursor->get_ext()));
  if (OB_NOT_NULL(cursor_info)) {
    CK (1 == addend || -1 == addend);
    if (OB_SUCC(ret)) {
      if (cursor_info->isopen()) {
        (1 == addend) ? cursor_info->inc_ref_count() : cursor_info->dec_ref_count();
        if (0 == cursor_info->get_ref_count()) {
          OZ (cursor_close_impl(ctx, cursor_info, true, OB_INVALID_ID, OB_INVALID_ID, true));
        }
      } else {
        // cursor maybe closed already
        if (-1 == addend) {
          OX (cursor_info->dec_ref_count());
        }
        LOG_DEBUG("spi process return ref cursor, cursor not open", K(cursor_info->get_ref_count()));
      }
    }
  } else {
    // do nothing; 例如，这种case，在return之前，会对cur进行操作，但cur是null
    /*
    * create or replace function return_null return sys_refcursor is
      cur sys_refcursor;
      begin
      return cur;
      end;
      /
      declare
      cc sys_refcursor;
      begin
      cc := return_null();
      if cc%isopen then
      dbms_output.put_line('cc is open');
      else
      dbms_output.put_line('cc is not open');
      end if;
      end;
    * */
  }
  return ret;
}

int ObSPIService::spi_handle_ref_cursor_refcount(ObPLExecCtx *ctx,
                                      uint64_t package_id,
                                      uint64_t routine_id,
                                      int64_t index,
                                      int64_t addend)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, package_id, routine_id, index, addend);
#else
  ObPLCursorInfo *cursor_info = NULL;
  ObObjParam cursor_var;
  ObCusorDeclareLoc odc;
  LOG_DEBUG("spi handle ref cursor", K(package_id), K(routine_id), K(index), K(addend));
  OZ (spi_get_cursor_info(ctx, package_id, routine_id, index, cursor_info, cursor_var, odc));
  if (OB_NOT_NULL(cursor_info)) {
    OZ (spi_add_ref_cursor_refcount(ctx, &cursor_var, addend));
  } else {
    // do nothing
    // for example:
    /*
    * declare
      cur1 SYS_REFCURSOR;
      begin
      end;
      cur1 is not open, so it is null;

      declare
      cursor cur is select 1 from dual;
      begin
      null;
      end;
    */
  }
#endif
  return ret;
}

int ObSPIService::spi_copy_ref_cursor(ObPLExecCtx *ctx,
                                 ObIAllocator *allocator,
                                 ObObj *src,
                                 ObObj *dest,
                                 ObDataType *dest_type,
                                 uint64_t package_id)
{
  int ret = OB_SUCCESS;  
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, allocator, src, dest, dest_type, package_id);
#else
  UNUSEDx(allocator, dest_type);
  CK (OB_INVALID_ID == package_id); // ref cursor can't define inside package
  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(dest));
  if (OB_SUCC(ret)) {
    bool need_inc_ref_cnt = false;
    // see if null cursor
    // case 1: dest is not null, add dest cursor ref count
    // case 2: src is not null, dec src cursor ref count, if equals to 0, close cursor,
    ObPLCursorInfo *src_cursor = reinterpret_cast<ObPLCursorInfo *>(src->get_ext());
    ObPLCursorInfo *dest_cursor = reinterpret_cast<ObPLCursorInfo *>(dest->get_ext());
    if (OB_NOT_NULL(src_cursor) && src_cursor->isopen()) {
      LOG_DEBUG("copy ref cursor, src ref count: ", K(src_cursor->get_ref_count()));
      need_inc_ref_cnt = (0 == src_cursor->get_ref_count() && src_cursor->get_is_returning());
      OV (0 < src_cursor->get_ref_count() || need_inc_ref_cnt, OB_ERR_UNEXPECTED, KPC(src_cursor), K(need_inc_ref_cnt));
      // 到了这里先把returning状态重置，ref count先不加，等赋值成功再加
      OX (src_cursor->set_is_returning(false));
    }
    if (src_cursor == dest_cursor) {
      // 说明指向同一个内存，或者都为null， 这个时候应该不干，除了一个特殊的场景，即return场景，需要将count+1
      // 例如: c1 := c2; c1 := c2; c2 may null or not null;
      if (need_inc_ref_cnt) {
        OX (src_cursor->inc_ref_count());
      }
    } else if (!src_cursor->isopen() && 0 == src_cursor->get_ref_count() && NULL == dest_cursor) {
      // src cursor is already closed and do not has any ref
      // dest cursor is null do not need copy
    } else {
      if (OB_NOT_NULL(dest_cursor) && dest_cursor->isopen()) {
        LOG_DEBUG("copy ref cursor, dest ref count: ",K(*dest_cursor),
                                                      K(dest_cursor->get_ref_count()));
        CK (0 < dest_cursor->get_ref_count());
        OX (dest_cursor->dec_ref_count());
        if (0 == dest_cursor->get_ref_count()) {
          OZ (cursor_close_impl(ctx, dest_cursor, true, OB_INVALID_ID, OB_INVALID_ID, true));
        }
      }
      OX (*dest = *src);
      if (OB_NOT_NULL(src_cursor)) {
        OX (src_cursor->inc_ref_count());
        LOG_DEBUG("copy ref cursor, src ref count: ",K(*src_cursor),
                                                      K(src_cursor->get_ref_count()));
      }
    }
  }
#endif
  return ret;
}

/*
 * 因为有可能调用者调用本函数是为了copy一个collection里的一项，此时传入的必须是collection的allocator，所以这个参数必要。
 * 如果能保证此函数不会用来copy普通数据类型那么可以不要。
 * */
int ObSPIService::spi_copy_datum(ObPLExecCtx *ctx,
                                 ObIAllocator *allocator,
                                 ObObj *src,
                                 ObObj *dest,
                                 ObDataType *dest_type,
                                 uint64_t package_id)
{
  int ret = OB_SUCCESS;
  ObExprResType result_type;
  ObIAllocator *copy_allocator = NULL;
  ObObjParam result;
  ObObjParam src_tmp;
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(dest));
  if (OB_SUCC(ret)) {
    if (NULL == allocator && OB_INVALID_ID != package_id) {
      OZ (spi_get_package_allocator(ctx, package_id, copy_allocator));
    } else {
      copy_allocator = NULL == allocator ? ctx->allocator_ : allocator;
    }
    CK (OB_NOT_NULL(copy_allocator));
    if (OB_FAIL(ret)) {
    } else if (src->is_null() || ObMaxType == src->get_type()) {
      //ObMaxTC means deleted element in Collection, no need to copy
      uint8 type = dest->get_meta().get_extend_type();
      if ((PL_CURSOR_TYPE == type || PL_REF_CURSOR_TYPE == type) && dest->get_ext() != 0) {
        OZ (spi_add_ref_cursor_refcount(ctx, dest, -1));
      } else {
        if (!dest->is_pl_extend()) {
          OZ (ObUserDefinedType::destruct_objparam(*copy_allocator, *dest));
        }
        OX (*dest = *src);
      }
    } else if (PL_CURSOR_TYPE == src->get_meta().get_extend_type()
        || PL_REF_CURSOR_TYPE == src->get_meta().get_extend_type()) {
      OZ (spi_copy_ref_cursor(ctx, allocator, src, dest, dest_type, package_id));
    } else if (src->is_ext() && OB_NOT_NULL(dest_type) && dest_type->get_meta_type().is_ext()) {
      OZ (ObPLComposite::copy_element(*src, *dest, *copy_allocator, ctx, ctx->exec_ctx_->get_my_session()));
    } else if (OB_NOT_NULL(dest_type) && dest_type->get_meta_type().is_ext()) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("src type and dest type is not match", K(ret));
    } else {
      CK (OB_NOT_NULL(ctx));
      CK (ctx->valid());
      CK (OB_NOT_NULL(dest_type));
      OX (result_type.set_meta(dest_type->get_meta_type()));
      OX (result_type.set_accuracy(dest_type->get_accuracy()));
      if (OB_SUCC(ret)) {
        if ((ObEnumType == src->get_type() && ObEnumType == result_type.get_type())
            || (ObSetType == src->get_type() && ObSetType == result_type.get_type())) {
          result = *src;
        } else {
          OX (src_tmp = *src);
          OZ (spi_convert(
            ctx->exec_ctx_->get_my_session(), &tmp_alloc, src_tmp, result_type, result),
            K(src_tmp), K(result_type),KPC(src));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (NULL != copy_allocator) {
        if (result.get_deep_copy_obj_ptr() != dest->get_deep_copy_obj_ptr()) {
          OZ (ObUserDefinedType::destruct_objparam(*copy_allocator, *dest));
          OZ (deep_copy_obj(*copy_allocator, result, *dest));
        } else {
          *dest = result;
        }
      }
    }
  }
  SET_SPI_STATUS;
  return ret;
}

int ObSPIService::spi_destruct_obj(ObPLExecCtx *ctx,
                                   ObObj *obj)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(obj));
  if (OB_SUCC(ret) &&
      obj->is_pl_extend() &&
      obj->get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
    OZ (ObUserDefinedType::destruct_obj(*obj, ctx->exec_ctx_->get_my_session()));
    OX (obj->set_null());
  }

  return ret;
}

int ObSPIService::spi_interface_impl(pl::ObPLExecCtx *ctx, const char *interface_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == interface_name || nullptr == ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(interface_name), K(ret));
  } else if (OB_ISNULL(ctx->exec_ctx_)
      || OB_ISNULL(ctx->params_)
      || OB_ISNULL(ctx->result_)
      || OB_ISNULL(ctx->exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid context", K(ctx->exec_ctx_), K(ctx->params_), K(ctx->result_));
  } else {
    ctx->exec_ctx_->get_my_session()->init_use_rich_format();
    ObString name(interface_name);
    PL_C_INTERFACE_t fp = GCTX.pl_engine_->get_interface_service().get_entry(name);
    if (nullptr != fp) {
      ParamStore exec_params( (ObWrapperAllocator(ctx->expr_alloc_)) );
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx->params_->count(); ++i) {
        ObObjParam new_param = ctx->params_->at(i);
        if (ctx->params_->at(i).is_pl_extend()) {
          // do nothing
        } else if (ctx->params_->at(i).need_deep_copy()) {
          OZ (deep_copy_obj(ctx->expr_alloc_, ctx->params_->at(i), new_param));
        }
        OZ (exec_params.push_back(new_param));
      }
      if (OB_SUCC(ret)) {
        ObIAllocator *old = ctx->allocator_;
        ctx->allocator_ = &ctx->expr_alloc_;
        ObObj tmp_result(ObMaxType);
        ret = fp(*ctx, exec_params, tmp_result);
        ctx->allocator_ = old;
        if (OB_FAIL(ret)) {
          if (tmp_result.is_pl_extend() &&
            pl::PL_REF_CURSOR_TYPE != tmp_result.get_meta().get_extend_type()) {
            ObUserDefinedType::destruct_obj(tmp_result, ctx->exec_ctx_->get_my_session());
          }
        } else {
          if (ctx->func_->get_ret_type().is_composite_type() ||
              ctx->func_->get_ret_type().is_obj_type()) {
            if (tmp_result.is_pl_extend() &&
                pl::PL_REF_CURSOR_TYPE != tmp_result.get_meta().get_extend_type()) {
              ObIAllocator *alloc = ctx->allocator_;
              OZ (ObUserDefinedType::deep_copy_obj(*alloc, tmp_result, *ctx->result_));
              ObUserDefinedType::destruct_obj(tmp_result, ctx->exec_ctx_->get_my_session());
            } else if (!tmp_result.is_ext() && tmp_result.need_deep_copy()) {
              OZ (deep_copy_obj(*ctx->allocator_, tmp_result, *ctx->result_));
            } else {
              *ctx->result_ = tmp_result;
            }
          }

          for (int64_t i = 0; OB_SUCC(ret) && i < ctx->func_->get_arg_count(); ++i) {
            if (ctx->func_->get_out_args().has_member(i)) {
              if (exec_params.at(i).is_pl_extend() &&
                pl::PL_REF_CURSOR_TYPE != exec_params.at(i).get_meta().get_extend_type()) {
                ObIAllocator *alloc = ctx->allocator_;
                //这里复杂数据类型的壳子一定是执行interface之前就有(init param), 不会是在interface c接口中构造的
                OZ (ObUserDefinedType::deep_copy_obj(*alloc, exec_params.at(i), ctx->params_->at(i)));
                if (exec_params.at(i).get_ext() != ctx->params_->at(i).get_ext()) {
                  ObUserDefinedType::destruct_obj(exec_params.at(i), ctx->exec_ctx_->get_my_session());
                }
              } else if (!exec_params.at(i).is_ext()) {
                void *ptr = ctx->params_->at(i).get_deep_copy_obj_ptr();
                if (nullptr != ptr) {
                  ctx->allocator_->free(ptr);
                }
                ctx->params_->at(i).set_null();
                OZ (deep_copy_obj(*ctx->allocator_, exec_params.at(i), ctx->params_->at(i)));
              }
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Calling C interface which doesn't exist", K(interface_name), K(name));
    }
  }
  return ret;
}

OB_INLINE int ObSPIService::acquire_spi_conn(ObMySQLProxy &sql_proxy,
                                             ObSQLSessionInfo &session_info,
                                             ObInnerSQLConnection *&spi_conn)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnectionPool *pool = static_cast<ObInnerSQLConnectionPool*>(sql_proxy.get_pool());
#ifndef NDEBUG
  CK(sql_proxy.is_inited());
  CK(OB_NOT_NULL(pool));
#endif
  OZ(pool->acquire_spi_conn(&session_info, spi_conn));
  return ret;
}

int ObSPIService::adjust_out_params(
  ObResultSet &result_set, ObSPIOutParams &out_params)
{
  int ret = OB_SUCCESS;
  /*observer::ObInnerSQLResult* inner_result =
    reinterpret_cast<observer::ObInnerSQLResult*>(mysql_result.get_result());
  sql::ObPhysicalPlan *plan = NULL;
  if (OB_NOT_NULL(inner_result)
      && OB_NOT_NULL(plan = inner_result->result_set().get_physical_plan())) {
    // TODO:
    // 检查下依赖对象中是否有Function或者Package, 这两种依赖对象的调用可能会有Out参数;
    // 暂时不检查是否确实有Out参数, 暂时统一认为存在Out参数;
    // 后续再做的更细些;
    bool has_out_param = false;
    const ObIArray<ObSchemaObjVersion> &ref_objects = plan->get_dependency_table();
    for (int64_t i = 0; i < ref_objects.count(); ++i) {
      if (DEPENDENCY_PACKAGE == ref_objects.at(i).object_type_
          || DEPENDENCY_FUNCTION == ref_objects.at(i).object_type_) {
        has_out_param =true;
      }
    }
    out_params.set_has_out_param(has_out_param);
    LOG_DEBUG("debug for adjust_out_params", K(ret), K(out_params), K(ref_objects));
  }*/
  // In Oracle: function with out parameter can not use in sql.
  UNUSED(result_set);
  out_params.set_has_out_param(false);
  return ret;
}

int ObSPIService::adjust_out_params(ObPLExecCtx *ctx,
                                    const ObSqlExpression **into_exprs,
                                    int64_t into_count,
                                    ObSPIOutParams &out_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObObj> &params = out_params.get_out_params();
  CK (OB_NOT_NULL(ctx));
  CK (into_count >= 0);
  for (int64_t i = 0;
      OB_SUCC(ret) && out_params.has_out_param() && i < params.count(); ++i) {
    if (params.at(i).is_ext() || params.at(i).is_unknown()) {
      bool exist = false;
      OZ (check_exist_in_into_exprs(ctx, into_exprs, into_count, params.at(i), exist));
      if (OB_SUCC(ret) && exist) {
        params.at(i) = ObObj(ObNullType);
      }
    }
  }
  return ret;
}

int ObSPIService::check_exist_in_into_exprs(ObPLExecCtx *ctx,
                                            const ObSqlExpression **into_exprs,
                                            int64_t into_count,
                                            const ObObj &result,
                                            bool &exist)
{
  int ret = OB_SUCCESS;
  ObPlCompiteWrite *composite_write = nullptr;
  CK (OB_NOT_NULL(ctx));
  CK (into_count >= 0);
  CK (result.is_ext() || result.is_unknown());
  OX (exist = false);
  for (int64_t i = 0; OB_SUCC(ret) && i < into_count; ++i) {
    CK (OB_NOT_NULL(into_exprs[i]));
    if (result.is_ext()) {
      if (is_obj_access_expression(*(into_exprs[i]))) {
        ObObjParam into;
        OZ (spi_calc_expr(ctx, into_exprs[i], OB_INVALID_ID, &into));
        CK (into.is_ext());
        OX (composite_write = reinterpret_cast<ObPlCompiteWrite *>(into.get_ext()));
        CK (OB_NOT_NULL(composite_write));
        if (OB_SUCC(ret) && composite_write->value_addr_ == result.get_ext()) {
          exist = true;
        }
      }
    } else if (is_question_mark_expression(*(into_exprs[i]))) {
      const ObObj &into = get_const_value(*(into_exprs[i]));
      CK (into.is_unknown());
      if (OB_SUCC(ret) && into.get_unknown() == result.get_unknown()) {
        exist = true;
      }
    }
  }
  return ret;
}

// out_params用于标记可能的通过function计算的输出参数
// ObExtendType代表地址, ObIntType代表在paramstore中的位置, ObNullType代表该参数不需要处理
int ObSPIService::construct_exec_params(ObPLExecCtx *ctx,
                                        ObIAllocator &param_allocator, //用于拷贝执行期参数
                                        const ObSqlExpression **param_exprs,
                                        int64_t param_count,
                                        const ObSqlExpression **into_exprs,
                                        int64_t into_count,
                                        ParamStore &exec_params,
                                        ObSPIOutParams &out_params,
                                        bool is_forall)
{
  int ret = OB_SUCCESS;
  ObObjParam result;
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  CK (OB_NOT_NULL(ctx));
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    const ObSqlExpression *expr = static_cast<const ObSqlExpression*>(param_exprs[i]);
    ObObj null_obj(ObNullType);
    result.reset();
    result.ObObj::reset();
    CK (OB_NOT_NULL(expr));
    OZ (spi_calc_expr(
      ctx, expr, OB_INVALID_INDEX, &result), i, param_count, *expr, result);
    OX (result.set_param_meta());
    if OB_SUCC (ret) {
      ObExprResType result_type;
      OZ (get_result_type(*ctx, *expr, result_type));
      if (OB_FAIL(ret)) {
      } else if (result.is_ext()) {
        if (result_type.is_ext()) {
          OX (result.set_udt_id(result_type.get_udt_id()));
          // xml pl type add cast
          if (ob_is_xml_pl_type(result_type.get_type(), result_type.get_udt_id())) {
            const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(ctx->exec_ctx_->get_my_session());
            ObCastCtx cast_ctx(&tmp_alloc, &dtc_params, CM_NONE, ObCharset::get_system_collation());
            if (OB_FAIL(ObObjCaster::to_type(ObUserDefinedSQLType, cast_ctx, result, result))) {
              LOG_WARN("failed to_type", K(ret), K(result));
            }
          }
          if (OB_SUCC(ret)
              && result_type.get_extend_type() > 0
              && result_type.get_extend_type() < T_EXT_SQL_ARRAY
              && !result.is_pl_extend()) {
            OX (const_cast<ObObjMeta&>(result.get_meta()).set_extend_type(result_type.get_extend_type()));
          }
          OZ (out_params.push_back(null_obj));
        } else {
          bool exist = false;
          ObObj* obj = reinterpret_cast<ObObj*>(result.get_ext());
          OZ (check_exist_in_into_exprs(ctx, into_exprs, into_count, result, exist));
          if (OB_SUCC(ret)) {
            if (!exist) {
              OZ (out_params.push_back(result), result);
            } else {
              OZ (out_params.push_back(null_obj));
            }
          }
          CK (OB_NOT_NULL(obj));
          OX (result = *obj);
          result.set_param_meta();
          OX (result.set_accuracy(result_type.get_accuracy()));
        }
      } else {
        result.set_accuracy(result_type.get_accuracy());
        result.set_param_meta(result_type);
        if (is_question_mark_expression(*expr)) {
          bool exist = false;
          OZ (check_exist_in_into_exprs(ctx, into_exprs, into_count, get_const_value(*expr), exist));
          if (OB_SUCC(ret)) {
            if (!exist) {
              OZ (out_params.push_back(get_const_value(*expr)), get_const_value(*expr));
            } else {
              OZ (out_params.push_back(null_obj));
            }
          }
        } else {
          OZ (out_params.push_back(null_obj));
        }
      }
      if (OB_SUCC(ret)) { //执行期参数必须从PL的内存空间拷贝到SQL自己的内存空间，以防止在SQL执行过程中参数被改掉
        ObObjParam new_param = result;
        if (!is_forall) { // forall场景, 不做拷贝, 上层代码transform_pl_ext_type会进行paramstore整体拷贝
          if (result.is_pl_extend()) {
            if (result.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
              new_param.set_int_value(0);
              OZ (pl::ObUserDefinedType::deep_copy_obj(param_allocator, result, new_param, true));
            }
          } else {
            OZ (deep_copy_obj(param_allocator, result, new_param));
          }
        }
        OX (new_param.set_accuracy(result.get_accuracy()));
        OX (new_param.set_need_to_check_type(true));
        if (OB_SUCC(ret)) {
          OZ (exec_params.push_back(new_param));
          if (OB_FAIL(ret) && !is_forall) {
            int ret = OB_SUCCESS;
            OZ (ObUserDefinedType::destruct_obj(new_param, ctx->exec_ctx_->get_my_session()));
          }
        }
      }
    }
  }
  LOG_DEBUG("debug for construct_exec_params", K(ret), K(out_params));
  return ret;
}

int ObSPIService::process_function_out_result(ObPLExecCtx *ctx,
                                              ObResultSet &result_set,
                                              ObIArray<ObObj> &out_params)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObPhysicalPlanCtx *pctx = NULL;
  ParamStore *exec_params = NULL;
  CK (OB_NOT_NULL(exec_ctx = &(result_set.get_exec_context())));
  CK (OB_NOT_NULL(pctx = exec_ctx->get_physical_plan_ctx()));
  CK (OB_NOT_NULL(exec_params = &(pctx->get_param_store_for_update())));

  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->allocator_));

  for (int64_t i = 0; OB_SUCC(ret) && i < out_params.count(); ++i) {
    if (out_params.at(i).is_ext()) {
      ObObj* obj = reinterpret_cast<ObObj*>(out_params.at(i).get_ext());
      CK (OB_NOT_NULL(obj));
      OZ (deep_copy_obj(*(ctx->allocator_), exec_params->at(i), *obj));
    } else if (out_params.at(i).is_unknown()) {
      ObObjParam* obj = NULL;
      int64_t idx = out_params.at(i).get_unknown();
      CK (idx >= 0 && idx < ctx->params_->count());
      OX (obj = &(ctx->params_->at(idx)));
      if (OB_SUCC(ret) && *obj != exec_params->at(i)) {
        ObObjParam result;
        OZ (spi_convert_objparam(ctx, &(exec_params->at(i)), idx, &result, false));
        OZ (deep_copy_obj(*(ctx->allocator_), result, *obj));
        obj->set_param_meta();
      }
    } else {
      CK (out_params.at(i).is_null());
    }
  }
  return ret;
}

int ObSPIService::store_params_string(
  ObPLExecCtx *ctx, ObSPIResultSet &spi_result, ParamStore *exec_params)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)
      && OB_NOT_NULL(ctx)
      && OB_NOT_NULL(ctx->exec_ctx_)
      && OB_NOT_NULL(ctx->exec_ctx_->get_my_session())) {
    char *tmp_ptr = NULL;
    int64_t tmp_len = 0;
    OZ (ObMPStmtExecute::store_params_value_to_str(spi_result.get_memory_ctx()->get_arena_allocator(),
                                                   *ctx->exec_ctx_->get_my_session(),
                                                   exec_params,
                                                   tmp_ptr,
                                                   tmp_len));
    OX (spi_result.get_exec_params_str_ptr()->assign(tmp_ptr, tmp_len));
  }
  return ret;
}

int ObSPIService::prepare_static_sql_params(ObPLExecCtx *ctx,
                                            ObIAllocator &param_allocator, //用于拷贝执行期参数
                                            const ObString &sql,
                                            const ObString &ps_sql,
                                            int64_t type,
                                            const ObSqlExpression **params,
                                            int64_t param_count,
                                            const ObSqlExpression **into_exprs,
                                            int64_t into_count,
                                            ObSPIResultSet &spi_result,
                                            ObSPIOutParams &out_params,
                                            bool is_forall,
                                            ParamStore *&curr_params)
{
  int ret = OB_SUCCESS;

  int64_t array_binding_count = 0;
  ParamStore *exec_params = NULL;
  ParamStore *batch_params = NULL;
  const ObSqlExpression **param_exprs = reinterpret_cast<const ObSqlExpression **>(params);

  curr_params = NULL;

  if (OB_ISNULL(exec_params = reinterpret_cast<ParamStore *>(param_allocator.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for exec params", K(ret));
  }
  OX (new (exec_params) ParamStore( (ObWrapperAllocator(param_allocator)) ) );
  OX (curr_params = exec_params);

  if (NULL == sql) {
    OZ (construct_exec_params(ctx, param_allocator, param_exprs, param_count,
                              into_exprs, into_count, *exec_params, out_params, is_forall),
      K(sql), K(ps_sql), K(type), K(param_count), K(out_params), KPC(exec_params));

    OZ (store_params_string(ctx, spi_result, exec_params));
  }

  if (OB_SUCC(ret) && is_forall && OB_NOT_NULL(exec_params)) {

    for (int64_t i = 0; OB_SUCC(ret) && i < exec_params->count(); ++i) {
      if (exec_params->at(i).is_ext()) {
        pl::ObPLCollection *coll = NULL;
        CK (OB_NOT_NULL(coll = reinterpret_cast<pl::ObPLCollection*>(exec_params->at(i).get_ext())));
        if (OB_SUCC(ret)) {
          array_binding_count = coll->get_actual_count();
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (array_binding_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array_binding_count is wrong", K(array_binding_count), K(ret));
      } else if (OB_FAIL(ObSQLUtils::transform_pl_ext_type(
          *exec_params, array_binding_count, param_allocator, batch_params, true))) {
        LOG_WARN("transform failed", K(ret));
      } else if (OB_ISNULL(batch_params)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        curr_params = batch_params;
        spi_result.get_sql_ctx().multi_stmt_item_.set_ps_mode(true);
        spi_result.get_sql_ctx().multi_stmt_item_.set_ab_cnt(array_binding_count);
      }
    }
  }

  return ret;
}

int ObSPIService::convert_ext_null_params(ParamStore &params, ObSQLSessionInfo *session)
{
  int  ret = OB_SUCCESS;
  bool is_ext_null = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (params.at(i).is_pl_extend()) {
      if (OB_FAIL(ObPLDataType::obj_is_null(params.at(i), is_ext_null))) {
        LOG_WARN("check obj_is_null failed", K(ret));
      } else if (is_ext_null) {
        ObObjMeta param_meta = params.at(i).get_meta();
        CK (OB_NOT_NULL(session));
        OZ (ObUserDefinedType::destruct_obj(params.at(i), session));
        OX (params.at(i).set_null());
        OX (params.at(i).set_param_meta(param_meta));
      }
    }
  }
  return ret;
}

int ObSPIService::inner_open(ObPLExecCtx *ctx,
                             ObIAllocator &param_allocator,
                             const ObString &sql,
                             const ObString &ps_sql,
                             int64_t type,
                             void *params,
                             int64_t param_count,
                             const ObSqlExpression **into_exprs,
                             int64_t into_count,
                             ObSPIResultSet &spi_result,
                             ObSPIOutParams &out_params,
                             bool is_forall,
                             bool is_dynamic_sql,
                             bool is_dbms_sql)
{
  int ret = OB_SUCCESS;

  ParamStore *curr_params = NULL;

  if (is_dynamic_sql) {
    ObObjParam **dynamic_params = reinterpret_cast<ObObjParam **>(params);
    OZ (prepare_dynamic_sql_params(ctx, spi_result, param_allocator, param_count, dynamic_params, curr_params));
  } else if (is_dbms_sql) {
    OX (curr_params = reinterpret_cast<ParamStore *>(params));
    OZ (store_params_string(ctx, spi_result, curr_params));
  } else {
    const ObSqlExpression **static_params = reinterpret_cast<const ObSqlExpression **>(params);
    OZ (prepare_static_sql_params(ctx,
                                  param_allocator,
                                  sql,
                                  ps_sql,
                                  type,
                                  static_params,
                                  param_count,
                                  into_exprs,
                                  into_count,
                                  spi_result,
                                  out_params,
                                  is_forall,
                                  curr_params));
  }

  CK (OB_NOT_NULL(curr_params));
  OZ (convert_ext_null_params(*curr_params, ctx->exec_ctx_->get_my_session()));
  OZ (inner_open(ctx, sql, ps_sql, type, *curr_params, spi_result, out_params, is_dynamic_sql));

  // if failed, we need release complex parameter memory in here
  if (OB_FAIL(ret) && OB_NOT_NULL(curr_params) && !is_dbms_sql) {
    int ret = OB_SUCCESS; // ignore destruct obj error
    for (int64_t i = 0; i < curr_params->count(); ++i) {
      OZ (ObUserDefinedType::destruct_obj(curr_params->at(i), ctx->exec_ctx_->get_my_session()));
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSPIService::inner_open(ObPLExecCtx *ctx,
                             const ObString &sql,
                             const ObString &ps_sql,
                             int64_t type,
                             ParamStore &exec_params,
                             ObSPIResultSet &spi_result,
                             ObSPIOutParams &out_params,
                             bool is_dynamic_sql)
{
  int ret = OB_SUCCESS;

  OZ (adjust_out_params(*spi_result.get_result_set(), out_params));

  if (OB_ISNULL(ctx)
      || OB_ISNULL(ctx->exec_ctx_)
      || (NULL == ctx->allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(ctx), K(sql), K(ret));
  } else {
#ifndef NDEBUG
    LOG_INFO("spi_execute using", K(sql), K(ps_sql), K(exec_params));
#else
    LOG_TRACE("spi_execute using", K(sql), K(ps_sql), K(exec_params));
#endif
    ObSQLSessionInfo *session = ctx->exec_ctx_->get_my_session();
    if (OB_ISNULL(session) || OB_ISNULL(GCTX.sql_engine_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Argument in pl context is NULL", K(session), K(ret));
    } else {
      ObPLASHGuard guard(ObPLASHGuard::ObPLASHStatus::IS_SQL_EXECUTION);
      bool old_client_return_rowid = session->is_client_return_rowid();
      bool is_inner_session = session->is_inner();
      ObSQLSessionInfo::SessionType old_session_type = session->get_session_type();
      ObInnerSQLConnection *spi_conn = NULL;
      !is_inner_session ? session->set_inner_session() : (void)NULL;
      session->set_session_type(ObSQLSessionInfo::USER_SESSION);
      if (NULL != ctx->pl_ctx_) {
        session->set_client_return_rowid(false);
      }
      if (OB_SUCC(ret)) {
        WITH_CONTEXT(spi_result.get_memory_ctx()) {
          if (exec_params.count() <= 0 && !sql.empty()) {
            spi_result.get_result_set()->set_user_sql(true);
            OZ (GCTX.sql_engine_->handle_pl_execute(sql,
                                                    *session,
                                                    exec_params,
                                                    *spi_result.get_result_set(),
                                                    spi_result.get_sql_ctx(),
                                                    false /* is_prepare_protocol */,
                                                    false /* is_dynamic_sql*/), K(sql), K(ps_sql), K(exec_params));
          } else {
            spi_result.get_result_set()->set_stmt_type(static_cast<stmt::StmtType>(type));
            OZ (GCTX.sql_engine_->handle_pl_execute(ps_sql,
                                                    *session,
                                                    exec_params,
                                                    *spi_result.get_result_set(),
                                                    spi_result.get_sql_ctx(),
                                                    true /* is_prepare_protocol */,
                                                    is_dynamic_sql /* is_dynamic_sql */), K(sql), K(ps_sql), K(exec_params));
            OZ (adjust_out_params(*spi_result.get_result_set(), out_params));
          }
        }
      }

      !is_inner_session ? session->set_user_session() : (void)NULL;
      session->set_session_type(old_session_type);
      session->set_client_return_rowid(old_client_return_rowid);
    }
  }
  return ret;
}

int ObSPIService::inner_fetch(ObPLExecCtx *ctx,
                              bool &can_retry,
                              ObSPIResultSet &spi_result,
                              const ObSqlExpression **into_exprs,
                              int64_t into_count,
                              const ObDataType *column_types,
                              int64_t type_count,
                              const bool *exprs_not_null_flag,
                              const int64_t *pl_integer_ranges,
                              ObIArray<ObObjParam*> *out_using_params,
                              int64_t &row_count,
                              bool is_bulk,
                              bool is_forall,
                              bool is_dynamic_sql,
                              ObNewRow *current_row,
                              bool has_hidden_rowid,
                              bool for_cursor,
                              int64_t limit,
                              const ObDataType *return_types,
                              int64_t return_type_count,
                              bool is_type_record)
{
  int ret = OB_SUCCESS;
  ObResultSet *result_set = spi_result.get_result_set();
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_sql_ctx()));
  CK (OB_NOT_NULL(result_set));
  if (OB_SUCC(ret)) {
    ObNewRow dummy_row;
    current_row = NULL != current_row ? current_row : &dummy_row;
    WITH_CONTEXT(spi_result.get_memory_ctx()) {
      if (OB_FAIL((get_result(ctx,
                              result_set,
                              true/*is streaming*/,
                              into_exprs,
                              into_count,
                              column_types,
                              type_count,
                              exprs_not_null_flag,
                              pl_integer_ranges,
                              out_using_params,
                              row_count,
                              *current_row,
                              can_retry,
                              has_hidden_rowid,
                              is_bulk,
                              is_dynamic_sql,
                              for_cursor,
                              is_forall,
                              limit,
                              return_types,
                              return_type_count,
                              is_type_record)))) {
        if (!for_cursor || (for_cursor && ret != OB_READ_NOTHING)) {
          LOG_WARN("failed to get_result, check if need retry", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSPIService::inner_fetch_with_retry(ObPLExecCtx *ctx,
                                         ObSPIResultSet &spi_result,
                                         const ObSqlExpression **into_exprs,
                                         int64_t into_count,
                                         const ObDataType *column_types,
                                         int64_t type_count,
                                         const bool *exprs_not_null_flag,
                                         const int64_t *pl_integer_ranges,
                                         int64_t &row_count,
                                         ObNewRow &current_row,
                                         bool has_hidden_rowid,
                                         bool is_bulk,
                                         bool for_cursor,
                                         int64_t limit,
                                         int64_t last_exec_time,
                                         const ObDataType *return_types,
                                         int64_t return_type_count,
                                         bool is_type_record)
{
  int ret = OB_SUCCESS;
  ObQueryRetryCtrl retry_ctrl;
  ObSQLSessionInfo *session = NULL;
  ObResultSet *result_set = spi_result.get_result_set();
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()));
  CK (OB_NOT_NULL(result_set));
  if (OB_SUCC(ret)) {
    int64_t time_gap = ObTimeUtility::current_time() - last_exec_time;
    int64_t old_query_start_time = session->get_query_start_time();
    int64_t old_timeout_ts = THIS_WORKER.get_timeout_ts();
    int64_t min_timeout_ts = old_timeout_ts;

    CK (result_set->get_exec_context().get_physical_plan_ctx() != NULL);
    OX (min_timeout_ts = MIN(old_timeout_ts,
        result_set->get_exec_context().get_physical_plan_ctx()->get_timeout_timestamp()));
    OX (session->set_query_start_time(ObTimeUtility::current_time()));
    OX (THIS_WORKER.set_timeout_ts(min_timeout_ts + time_gap));
    OX (result_set->get_exec_context().get_physical_plan_ctx()->
              set_timeout_timestamp(min_timeout_ts + time_gap));
    if (OB_SUCC(ret)) {
      do {
        ret = OB_SUCCESS;
        bool can_retry = true;
        ObSPIRetryCtrlGuard retry_guard(retry_ctrl, spi_result, *session, ret, true);
        if (FAILEDx(inner_fetch(ctx,
                                can_retry,
                                spi_result,
                                into_exprs,
                                into_count,
                                column_types,
                                type_count,
                                exprs_not_null_flag,
                                pl_integer_ranges,
                                NULL, /*out_using_params*/
                                row_count,
                                is_bulk,
                                false,
                                false, /*is_dynamic_sql*/
                                &current_row,
                                has_hidden_rowid,
                                for_cursor,
                                limit,
                                return_types,
                                return_type_count,
                                is_type_record))) {
          if (!for_cursor || (for_cursor && ret != OB_READ_NOTHING)) {
            LOG_WARN("failed to get_result, check if need retry", K(ret));
          }
        }
        if (can_retry) {
          retry_guard.test();
        }
      // NOTE: cursor fetch failed can not retry, we only use this to refresh location cache.
      // actully, this function only called by cursor, so `for_cursor` just in case.
      } while (RETRY_TYPE_NONE != retry_ctrl.get_retry_type() && !for_cursor);
      session->get_retry_info_for_update().clear();
      session->set_query_start_time(old_query_start_time);
      THIS_WORKER.set_timeout_ts(old_timeout_ts);
    }
  }
  return ret;
}

int ObSPIService::store_into_result(ObPLExecCtx *ctx,
                                    ObCastCtx &cast_ctx,
                                    ObNewRow &cur_row,
                                    const ObSqlExpression **into_exprs,
                                    const ObDataType *column_types,
                                    int64_t type_count,
                                    int64_t into_count,
                                    const bool *exprs_not_null,
                                    const int64_t *pl_integer_ranges,
                                    const ObDataType *return_types,
                                    int64_t return_type_count,
                                    int64_t actual_column_count,
                                    ObIArray<ObDataType> &row_desc,
                                    bool is_type_record)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = ctx->exec_ctx_;
  bool is_strict = false;
  bool is_null_row = 0 == cur_row.get_count();
  CK (OB_NOT_NULL(exec_ctx), OB_NOT_NULL(exec_ctx->get_my_session()));
  CK (is_null_row ? true : (actual_column_count <= cur_row.get_count()));
  OX (is_strict = is_strict_mode(exec_ctx->get_my_session()->get_sql_mode()));

  /* 如果into variable只有一个:
      1.是基础类型变量
      2.create or replace type定义的udt变量
      3.type定义的record
      2和3都是record类型
     如果into variable有多个，为上述1和2的组合。
     对于record变量，目前的行为是展开后获取内部的元素值，然后依次赋值而非深拷后整体赋值
     如果record内部元素是object, object的赋值动作需要深拷吗
     对于顶层record，基础类型属性，需要考虑强转,
     复杂数据类型属性在resolve阶段严格限制，执行期不做强转逻辑，只考虑赋值
     按照这个逻辑，复杂数据类型没有expand的必要，也无需传递到执行期做类型转换
  */
  if (into_count > 1) {
    ObSEArray<ObObj, 1> tmp_result;
    ObSEArray<ObDataType, 1> tmp_desc;
    CK (into_count == actual_column_count);
    CK(return_types != nullptr ? return_type_count == into_count : true);
    for (int64_t i = 0; OB_SUCC(ret) && i < actual_column_count; ++i) {
      //循环处理多个into变量的赋值，每个变量赋值需要检测是否需要强转，然后用强转后的obj赋值
      tmp_result.reuse();
      tmp_desc.reuse();
      if (is_null_row) {
        OZ (tmp_result.push_back(ObObj(ObNullType)));
      } else {
        OZ (tmp_result.push_back(cur_row.get_cell(i)));
      }
      OZ (tmp_desc.push_back(row_desc.at(i)));
      OZ (store_result(ctx, into_exprs[i], &column_types[i],
                      1, exprs_not_null + i, pl_integer_ranges + i,
                      tmp_desc, is_strict,
                      cast_ctx, tmp_result,
                      return_types != nullptr ? &return_types[i] : nullptr,
                      return_types != nullptr ? 1 : 0));
    }
  } else if (is_type_record) {
    /* into后面的是type record(作为一个整体)，对应into前面的多个column value
        多个obj存到record中，实现上需要依次为对应属性赋值，需要考虑cast
      into record是展开一层的，因此column_types == return_type_count == actual_column_count
       将所有column value收集起来，传递到store result中进行cast校验&赋值
    */
    ObSEArray<ObObj, OB_DEFAULT_SE_ARRAY_COUNT> tmp_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < actual_column_count; ++i) {
      if (is_null_row) {
        OZ (tmp_result.push_back(ObObj(ObNullType)));
      } else {
        OZ (tmp_result.push_back(cur_row.get_cell(i)));
      }
    }
    OZ (store_result(ctx, into_exprs[0], column_types,
                     type_count, exprs_not_null, pl_integer_ranges,
                     row_desc, is_strict,
                     cast_ctx, tmp_result, return_types, return_type_count,
                     true));
  } else {
    /*
      into后面是udt record, 视为单个variable个体，对应into前面单个column value
        本身是一个obj存到record中，当前的实现时，将record对应的obj拆开成多个obj，然后存到record中,
        代码逻辑上可以复用场景1. 理论上应该可以直接找到into record的地址，深拷赋值，不需要强转
    */
    CK (1 == actual_column_count);
    if (OB_SUCC(ret)) {
      ObSEArray<ObObj, 1> tmp_result;
      ObSEArray<ObDataType, 1> tmp_desc;
      if (is_null_row) {
        OZ (tmp_result.push_back(ObObj(ObNullType)));
      } else {
        OZ (tmp_result.push_back(cur_row.get_cell(0)));
      }
      OZ (tmp_desc.push_back(row_desc.at(0)));
      OZ (store_result(ctx, into_exprs[0], column_types,
                      1, exprs_not_null, pl_integer_ranges,
                      tmp_desc, is_strict,
                      cast_ctx, tmp_result,
                      return_types,
                      return_type_count));
    }
  }
  return ret;
}


int ObSPIService::get_package_var_info_by_expr(const ObSqlExpression *expr,
                                              uint64_t &package_id,
                                              uint64_t &var_idx)
{
  int ret = OB_SUCCESS;
  // package var need add package change to sync var to remote
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (T_OP_GET_PACKAGE_VAR == get_expression_type(*expr)) {
    OV (5 <= expr->get_expr_items().count(), OB_ERR_UNEXPECTED, expr->get_expr_items().count());
    CK (T_UINT64 == expr->get_expr_items().at(1).get_item_type());
    CK (T_INT == expr->get_expr_items().at(2).get_item_type());
    OX (package_id = expr->get_expr_items().at(1).get_obj().get_uint64());// pkg id
    OX (var_idx = expr->get_expr_items().at(2).get_obj().get_int());// var idx
  } else if (is_obj_access_expression(*expr)
              && expr->get_expr_items().count() > 1
              && T_OP_GET_PACKAGE_VAR == expr->get_expr_items().at(1).get_item_type()) {
    uint16_t param_pos = expr->get_expr_items().at(1).get_param_idx();
    OX (package_id = expr->get_expr_items().at(param_pos).get_obj().get_uint64());
    OX (var_idx = expr->get_expr_items().at(param_pos+1).get_obj().get_int());
  }
  LOG_DEBUG("get_package_var_info_by_expr ", K(package_id), K(var_idx));
  return ret;
}

/***************************************************************************************/
/* 注意：以下是内存排列有关的代码，修改这里一定要十分理解各种数据类型在LLVM端和SQL端的内存排列和生命周期。
 * SQL端的隐式赋值（Into/Bulk Collect Into）相对于显式赋值（Assign）较为简单，因为需要存储的源数据一定是从查询语句获得的
 * 基础数据类型，不存在源端是ADT的情况。
 * 如有问题请联系如颠ryan.ly
 ***************************************************************************************/
int ObSPIService::get_result(ObPLExecCtx *ctx,
                             void *result_set, // 存储结果集的结构, 可能是ObSPICursor和ObInnerSQLResult
                             bool is_streaming, //false表示结果是ObSPICursor，否则是ObInnerSQLResult
                             const ObSqlExpression **into_exprs, //ObSqlExpression*数组，指示了该result存放的位置：如果是Question Mark，表示是在params里的下标，如果是ObjAccess，其表达式结果表示是一个内存地址
                             int64_t into_count, // 代表into_exprs的数量
                             const ObDataType *column_types,
                             int64_t type_count,
                             const bool *exprs_not_null,
                             const int64_t *pl_integer_ranges,
                             ObIArray<ObObjParam*> *out_using_params, //动态DML的returing通过USING OUT传参
                             int64_t &row_count,
                             ObNewRow &current_row, //返回最后一行
                             bool &can_retry,
                             bool has_hidden_rowid, //如果最后一列是隐藏的rowid，需要跳过
                             bool is_bulk,
                             bool is_dynamic_sql,
                             bool for_cursor, //是否检查单行和notfound
                             bool is_forall,
                             int64_t limit, //INT64_MAX:无limit
                             const ObDataType *return_types,
                             int64_t return_type_count,
                             bool is_type_record)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_BULK_INTO), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObIAllocator *allocator = ctx->allocator_;
  ObExecContext *exec_ctx = ctx->exec_ctx_;
  ObPLCursorInfo *implicit_cursor = NULL;
  int64_t hidden_column_count = has_hidden_rowid ? 1 : 0;
  bool for_dbms_sql = for_cursor && 0 == into_count; //DBMS_SQL包允许没有into子句的情况下获取数据
  ObIAllocator *current_expr_allocator = nullptr;
  OX (current_expr_allocator = ctx->get_top_expr_allocator());
  CK (OB_NOT_NULL(allocator), OB_NOT_NULL(exec_ctx), OB_NOT_NULL(exec_ctx->get_my_session()));
  if (OB_SUCC(ret) && !for_cursor) {
    CK (OB_NOT_NULL(implicit_cursor = exec_ctx->get_my_session()->get_pl_implicit_cursor()));
  }
  if (OB_FAIL(ret)) {
  } else if (into_count > 0 || NULL != out_using_params || for_dbms_sql) {
    // INTO, BULK COLLECT INTO, FETCH INTO, FETCH BULK COLLECT INTO以及动态DML语句USING OUT
    if (OB_ISNULL(result_set)
        || ((NULL == column_types || type_count <= 0) && into_count > 0)
        || (NULL == into_exprs && into_count > 0)
        || (into_count > 0 && NULL != out_using_params) //如果有USING OUT一定不能同时出现INTO子句
        || (!for_cursor && INT64_MAX != limit) //limit子句仅可能出现在Fetch语句
        || (!is_bulk && INT64_MAX != limit) //limit子句必须和Bulk Collect合用
        || (!is_streaming && !for_cursor)) { //如果不是流式，那么一定是cursor
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Argument passed in is NULL",
               K(result_set), K(into_exprs), K(into_count), K(column_types),
               K(type_count), K(for_cursor), K(is_bulk), K(limit), K(out_using_params), K(ret));
    } else if (!is_streaming
               && 0 == static_cast<ObSPICursor*>(result_set)->row_store_.get_row_cnt()) {
      //Oracle里找不到数据的话，BULK不报错、Returning不报错、Cursor(Fetch)不报错；只有SELECT INTO一种报错
      if (is_bulk) { // BULK模式如果没有数据也需要初始化数组
        const ObSqlExpression *result_expr = NULL;
        ObPlCompiteWrite *composite_write = nullptr;
        ObObjParam result_address;
        for (int64_t i = 0; OB_SUCC(ret) && i < into_count; ++i) {
          ObPLCollection *table = NULL;
          CK (OB_NOT_NULL(result_expr = into_exprs[i]));
          CK (is_obj_access_expression(*result_expr));
          OZ (spi_calc_expr(ctx, result_expr, OB_INVALID_INDEX, &result_address));
          OX (composite_write = reinterpret_cast<ObPlCompiteWrite *>(result_address.get_ext()));
          CK (OB_NOT_NULL(composite_write));
          CK (OB_NOT_NULL(table = reinterpret_cast<ObPLCollection*>(composite_write->value_addr_)));
          CK (OB_NOT_NULL(table));
          CK (OB_NOT_NULL(table->get_allocator()));//need fix
          if (OB_SUCC(ret) &&
                (!table->is_inited()
                  || for_cursor
                  || (NULL != implicit_cursor && !implicit_cursor->get_in_forall())
                  || (NULL != implicit_cursor && 0 == implicit_cursor->get_bulk_rowcount_count()))) {
            //FORALL的BULK是追加模式，仅在非追加模式或追加模式的第一次需要spi_reset_collection
            OZ (spi_set_collection(ctx->exec_ctx_->get_my_session()->get_effective_tenant_id(),
                                     ctx, *allocator, *table, 0));
          }
        }
      }
      ret = OB_READ_NOTHING;
    } else {
      bool not_found = false;
      int64_t column_count = 0;
      int64_t actual_column_count = 0;
      // Step1: 获取结果集行描述
      ObArray<ObDataType> row_desc;
      if (OB_FAIL(row_desc.reserve(OB_DEFAULT_SE_ARRAY_COUNT))) {
        LOG_WARN("fail to reserve row_desc", K(ret));
      } else if (!is_streaming) { //Mysql模式的Cursor或者可更新游标或者游标表达式会缓存数据
        column_count = static_cast<ObSPICursor*>(result_set)->row_desc_.count();
        actual_column_count = column_count - hidden_column_count;
        // TODO: 原来的 column_count 是通过 row_store_ 拿到的， OB_RA_ROW_STORE 不提供 row_col_cnt
        // 这个判断暂时没有意义，先注掉
        // OV (column_count == static_cast<ObSPICursor*>(result_set)->row_desc_.count(),
        //     OB_INVALID_ARGUMENT,
        //     K(actual_column_count),
        //     K(static_cast<ObSPICursor*>(result_set)->row_desc_.count()));
        for (int64_t i = 0; OB_SUCC(ret) && i < actual_column_count; ++i) {
          OZ (row_desc.push_back(static_cast<ObSPICursor*>(result_set)->row_desc_.at(i)));
        }
      } else {
        const common::ColumnsFieldIArray *fields = static_cast<ObResultSet*>(result_set)->get_field_columns();
        if (OB_ISNULL(fields)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else {
          column_count = fields->count();
          actual_column_count = column_count - hidden_column_count;
        }
        bool need_subschema_ctx = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < actual_column_count; ++i) {
          ObDataType type;
          type.set_meta_type(fields->at(i).type_.get_meta());
          type.set_accuracy(fields->at(i).accuracy_);
          if (type.get_meta_type().is_user_defined_sql_type()
              || type.get_meta_type().is_collection_sql_type()) {
            // need subschema ctx to convert sql udt to pl types in convert obj
            need_subschema_ctx = true;
          }
          if (OB_FAIL(row_desc.push_back(type))) {
            LOG_WARN("push back error", K(i), K(fields->at(i).type_), K(fields->at(i).accuracy_),
                     K(ret));
          }
        }
        if (OB_SUCC(ret) && need_subschema_ctx) {
          CK (OB_NOT_NULL(exec_ctx->get_physical_plan_ctx()));
          if (OB_SUCC(ret)) {
            ObSubSchemaCtx & subschema_ctx = exec_ctx->get_physical_plan_ctx()->get_subschema_ctx();
            if (OB_FAIL(subschema_ctx.assgin(
                  static_cast<ObResultSet*>(result_set)->get_physical_plan()->get_subschema_ctx()))) {
              LOG_WARN("fail to assign subschema ctx", K(ret));
            }
          }
        }
      }
      // Step2: 检查类型匹配
      if (OB_SUCC(ret)) {
        if (for_dbms_sql) {
          //DBMS_SQL包的FETCH不需要检查
        } else if (NULL == out_using_params) {
          if (actual_column_count != type_count
              && actual_column_count != into_count) {
            ret = OB_ERR_SP_INVALID_FETCH_ARG;
            LOG_WARN("type count is not equal to column count", K(column_count),
                     K(actual_column_count), K(type_count), K(ret));
          }
          if (actual_column_count == type_count) {
            for (int64_t i = 0; OB_SUCC(ret) && i < actual_column_count; ++i) {
              if (!cast_supported(row_desc.at(i).get_obj_type(), row_desc.at(i).get_collation_type(),
                                  column_types[i].get_obj_type(), column_types[i].get_collation_type())) {
                ret = OB_ERR_INVALID_TYPE_FOR_OP;
                LOG_WARN("cast parameter to expected type not supported",
                         K(ret), K(i), K(row_desc.at(i)), K(column_types[i]));
              }
            }
          }
        } else {
          if (actual_column_count != out_using_params->count()) {
            ret = OB_ERR_SP_INVALID_FETCH_ARG;
            LOG_WARN("param count is not equal to column count", K(column_count),
                     K(actual_column_count), K(*out_using_params), K(ret));
          }
        }
      }

      // Step3: 获取collationtype和castmode
      ObCollationType cast_coll_type = CS_TYPE_INVALID;
      ObCastMode cast_mode = CM_NONE;
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(ctx->exec_ctx_->get_my_session());
      bool is_strict = false;
      if (OB_SUCC(ret)) {
        OZ (exec_ctx->get_my_session()->get_collation_connection(cast_coll_type));
        OX (is_strict = is_strict_mode(exec_ctx->get_my_session()->get_sql_mode()));
        OZ (ObSQLUtils::get_default_cast_mode(stmt::T_NONE, exec_ctx->get_my_session(), cast_mode));
      }
      // Step4: 获取结果并存储到变量
      ObSEArray<std::pair<uint64_t, uint64_t>, OB_DEFAULT_SE_ARRAY_COUNT> package_vars_info;
      lib::ObMemAttr attr(MTL_ID());
      OX (package_vars_info.set_attr(attr));
      if (OB_SUCC(ret) && !is_bulk) { // [FETCH] INTO x, y, z OR [FETCH] INTO record
        /*
         * 若非多个variables，便是单个record。不可能是多个record或者record和variables混在一起。（详见oracle语法）
         * 1、如果是多个into，into的数目必须和select item数目一致（一定是多个variables的情况）；
         * 2、如果into的数目比select item少，那么into的数目必须是1（一定是单个record的情况）；
         * 3、如果into的数目和select item一样多，并且是1，那么是单个record还是单个变量不确定，但是同样是合法的；
         * 还有一种特殊情况，是动态DML语句带RETURNING通过USING OUT传递参数的情况，详见：
         *
         *  这种情况仅支持SQL基础类型。
         *  另外，DBMS_SQL没有输出参数，只需要返回current_row即可
         */
        if (!((into_count < actual_column_count && 1 == into_count)
            || into_count == actual_column_count
            || (NULL != out_using_params && out_using_params->count() == actual_column_count )
            || (for_cursor && NULL == out_using_params && 0 == into_count))) {
          ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
          LOG_WARN("into clause not match select items", K(into_count), K(column_count),
                   K(actual_column_count), K(out_using_params), K(ret));
        } else if (OB_FAIL(fetch_row(result_set, is_streaming, row_count, current_row))) {
          if (OB_ITER_END == ret) {
            not_found = true;
            if (!for_cursor) {
              implicit_cursor->set_rowcount(0);
            }
            if (is_streaming) {
              current_row.reset();
            }
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("read result error", K(ret), K(row_count), K(for_cursor));
          }
        } else {
          if (OB_SUCC(ret) && !for_cursor) { //如果不是cursor，into只能返回一行，需要检查返回多行报错
            ObNewRow tmp_row;
            int64_t cnt = row_count;
            if (OB_FAIL(ob_write_row(tmp_allocator, current_row, tmp_row))) {
              LOG_WARN("copy current row fail.", K(ret));
            } else {
              ObNewRow tmp_row2;
              current_row = tmp_row;
              if (OB_FAIL(fetch_row(result_set, is_streaming, cnt, tmp_row2))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("read result error", K(ret));
                }
              } else {
                ret = OB_ERR_TOO_MANY_ROWS;
              }
            }
            // If a SELECT INTO statement without a BULK COLLECT clause returns multiple rows,
            // PL/SQL raises the predefined exception TOO_MANY_ROWS and SQL%ROWCOUNT returns 1,
            // not the actual number of rows that satisfy the query.
            implicit_cursor->set_rowcount(1);
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (for_dbms_sql) {
            //DBMS_SQL包的FETCH不需要检查
          } else if (NULL != out_using_params) {
            ObCastCtx cast_ctx(current_expr_allocator, &dtc_params, cast_mode, cast_coll_type);
            CK (actual_column_count == row_desc.count());
            for (int64_t i = 0; OB_SUCC(ret) && i < actual_column_count; ++i) {
              ObObj &obj = current_row.get_cell(i);
              if (obj.is_pl_extend()) {
                ObIAllocator *alloc = current_expr_allocator;
                if (PL_CURSOR_TYPE == obj.get_meta().get_extend_type() ||
                    PL_REF_CURSOR_TYPE == obj.get_meta().get_extend_type()) {
                  alloc = &ctx->exec_ctx_->get_allocator();
                }
                if (out_using_params->at(i)->is_null()) {
                  if (out_using_params->at(i)->get_param_meta().is_pl_extend_type()) {
                    OZ (pl::ObUserDefinedType::deep_copy_obj(*alloc, obj, *out_using_params->at(i)));
                  } else if (obj.is_pl_extend()) {
                    ret =OB_ERR_EXPRESSION_WRONG_TYPE;
                    LOG_WARN("expr is wrong type", K(ret), K(obj), KPC(out_using_params->at(i)));
                  } else {
                    OZ (deep_copy_obj(*alloc, obj, *out_using_params->at(i)));
                  }
                } else {
                  OZ (check_and_deep_copy_result(*alloc, obj, *out_using_params->at(i)));
                }
              } else {
                ObSEArray<ObObj, 1> cur_result;
                ObSEArray<ObObj, 1> conv_result;
                ObSEArray<ObDataType, 1> cur_type;
                OZ (cur_result.push_back(obj));
                OZ (cur_type.push_back(row_desc.at(i)));
                OZ (convert_obj(ctx, cast_ctx, is_strict, NULL, cur_type, cur_result, &column_types[i], 1, conv_result));
                CK (1 == conv_result.count());
                OX (*out_using_params->at(i) = conv_result.at(0));
                OX (out_using_params->at(i)->set_param_meta());
              }
            }
          } else {
            ObCastCtx cast_ctx(&tmp_allocator, &dtc_params, cast_mode, cast_coll_type);
            OZ (store_into_result(ctx, cast_ctx, current_row, into_exprs, column_types, type_count,
                             into_count, exprs_not_null, pl_integer_ranges, return_types, return_type_count,
                             actual_column_count, row_desc, is_type_record));
            for (int64_t i = 0; OB_SUCC(ret) && i < into_count; ++i) {
              if (is_obj_access_expression(*into_exprs[i])) {
                std::pair<uint64_t, uint64_t> package_var_info = std::pair<uint64_t, uint64_t>(OB_INVALID_ID, OB_INVALID_ID);
                OZ (get_package_var_info_by_expr(into_exprs[i], package_var_info.first, package_var_info.second));
                if (OB_INVALID_ID != package_var_info.first && OB_INVALID_ID != package_var_info.second) {
                  OZ (package_vars_info.push_back(package_var_info));
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret) && is_bulk) { // BULK COLLECT INTO
        const ObSqlExpression *result_expr = NULL;
        ObPlCompiteWrite *composite_write = nullptr;
        ObObjParam result_address;
        ObArray<ObPLCollection*> bulk_tables;
        ObArray<ObCastCtx> cast_ctxs;
        OZ (bulk_tables.reserve(OB_DEFAULT_SE_ARRAY_COUNT));
        OZ (cast_ctxs.reserve(OB_DEFAULT_SE_ARRAY_COUNT));
        for (int64_t i = 0; OB_SUCC(ret) && i < into_count; ++i) {
          ObPLCollection *table = NULL;
          ObPLAllocator1 *pl_allocator = nullptr;
          std::pair<uint64_t, uint64_t> package_var_info = std::pair<uint64_t, uint64_t>(OB_INVALID_ID, OB_INVALID_ID);
          // ObIAllocator *collection_allocator = NULL;
          CK (OB_NOT_NULL(result_expr = into_exprs[i]));
          CK (is_obj_access_expression(*result_expr));
          OZ (spi_calc_expr(ctx, result_expr, OB_INVALID_INDEX, &result_address));
          OX (composite_write = reinterpret_cast<ObPlCompiteWrite *>(result_address.get_ext()));
          CK (OB_NOT_NULL(composite_write));
          CK (OB_NOT_NULL(table = reinterpret_cast<ObPLCollection*>(composite_write->value_addr_)));
          CK (OB_NOT_NULL(table));
          CK (OB_NOT_NULL(table->get_allocator()));
          OX (pl_allocator = static_cast<ObPLAllocator1 *>(table->get_allocator()));
          CK (composite_write->allocator_ == reinterpret_cast<int64_t>(pl_allocator->get_parent_allocator()));
          OZ (bulk_tables.push_back(table));
          OX (table->print());
          if (OB_SUCC(ret) &&
              (!table->is_inited()
                || for_cursor
                || (NULL != implicit_cursor && !implicit_cursor->get_in_forall())
                || (NULL != implicit_cursor && 0 == implicit_cursor->get_bulk_rowcount_count()))) {
            //FORALL的BULK是追加模式，仅在非追加模式或追加模式的第一次需要spi_reset_collection
            OZ (spi_set_collection(ctx->exec_ctx_->get_my_session()->get_effective_tenant_id(),
                                     ctx, *allocator, *table, 0));
          }
          OZ (get_package_var_info_by_expr(result_expr, package_var_info.first, package_var_info.second));
          if (OB_INVALID_ID != package_var_info.first && OB_INVALID_ID != package_var_info.second) {
            OZ (package_vars_info.push_back(package_var_info));
          }
          // collection may modified by sql fetch, which can be reset and allocator will change, such like stmt a:=b in trigger
          // so allocator of collection can not be used by collect_cells.
          // CK (OB_NOT_NULL(collection_allocator = table->get_allocator()));
          if (is_type_record) {
            for (int64_t j = 0; OB_SUCC(ret) && j < table->get_column_count(); ++j) {
              OZ (cast_ctxs.push_back(ObCastCtx(&tmp_allocator,
                                                &dtc_params,
                                                cast_mode,
                                                cast_coll_type)));
            }
          } else {
            // 对于collection内部是非record场景而言, 每个table只需要一个castctx
            OZ (cast_ctxs.push_back(ObCastCtx(&tmp_allocator,
                                                &dtc_params,
                                                cast_mode,
                                                cast_coll_type)));
          }
        }
        ObArray<ObObj> tmp_result;
        OZ (tmp_result.reserve(OB_DEFAULT_SE_ARRAY_COUNT));
        while (OB_SUCC(ret) && (limit < 0 || row_count < limit)) {
          if (OB_FAIL(fetch_row(result_set, is_streaming, row_count, current_row))) {
            break;
          }
          OZ (collect_cells(*ctx, current_row, column_types, type_count,
                            row_desc, is_strict, cast_ctxs, hidden_column_count, tmp_result,
                            is_streaming ? static_cast<ObResultSet*>(result_set) : nullptr));
        }
        if (OB_FAIL(ret)) {
          if (OB_ITER_END == ret) {
            // CURSOR BULK COLLECT INTO的NOTFOUND行为与SQL不同, 只要遇到ITER_END就设置为TRUE
            not_found = 0 == row_count || for_cursor ? true : false;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("read result error", K(ret));
            for (int64_t i = 0; i < tmp_result.count(); ++i) {
              int tmp_ret = OB_SUCCESS;
              if ((tmp_ret = ObUserDefinedType::destruct_obj(tmp_result.at(i), ctx->exec_ctx_->get_my_session())) != OB_SUCCESS) {
                LOG_WARN("failed to destruct obj, memory may leak", K(ret), K(tmp_ret), K(i));
              }
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < bulk_tables.count(); ++i) {
          ObPLCollection *table = bulk_tables.at(i);
          CK (OB_NOT_NULL(table));
#ifdef OB_BUILD_ORACLE_PL
          if (OB_SUCC(ret) && table->is_varray()) {
            ObPLVArray *varray = static_cast<ObPLVArray*>(table);
            bool append_mode = (NULL == implicit_cursor ? false : implicit_cursor->get_in_forall());
            int64_t new_count = append_mode ? table->get_count() + row_count : row_count;
            CK (OB_NOT_NULL(varray));
            if (OB_SUCC(ret) && new_count > varray->get_capacity()) {
              ret = OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT;
              LOG_WARN("Subscript outside of limit", K(ret), K(append_mode), K(new_count), KPC(varray));
            }
          }
#endif
        }
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < bulk_tables.count(); ++i) {
            ObPLCollection *table = bulk_tables.at(i);
            CK (OB_NOT_NULL(table));
            if (OB_SUCC(ret) && table->get_count() > 0) {
              if (implicit_cursor == NULL || !implicit_cursor->get_in_forall()) {
                // only clear table data, do not reset collection allocator
                int tmp_ret = OB_SUCCESS;
                ObObj tmp;
                tmp.set_extend(reinterpret_cast<int64_t>(table), table->get_type());
                if ((tmp_ret = ObUserDefinedType::destruct_obj(tmp, ctx->exec_ctx_->get_my_session(), true)) != OB_SUCCESS) {
                  LOG_WARN("failed to destruct obj, memory may leak", K(ret), K(tmp_ret), K(i));
                }
                table->set_count(0);
#ifdef OB_BUILD_ORACLE_PL
              } else if (table->is_associative_array()) {
                ObPLAssocArray* assoc = static_cast<ObPLAssocArray*>(table);
                CK (OB_NOT_NULL(assoc));
                if (OB_SUCC(ret) && assoc->get_key() != NULL) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED,
                                 "Implementation restriction, bulk collect with associative array has key values");
                }
              }
#else
              }
#endif
            }
          }
        }
        if (OB_SUCC(ret) && row_count > 0) { // 累积存储在pl table里
          OZ (store_result(ctx, bulk_tables, row_count, type_count, tmp_result,
                          NULL == implicit_cursor ? false : implicit_cursor->get_in_forall(), is_type_record));
        }
        if (tmp_result.count() > 0) {
          int tmp = OB_SUCCESS;
          for (int i = 0; i < tmp_result.count(); ++i) {
            tmp = ObUserDefinedType::destruct_obj(tmp_result.at(i), nullptr);
          }
          ret = OB_SUCCESS == ret ? tmp : ret;
        }
        if (!for_cursor && OB_NOT_NULL(implicit_cursor)) {
          OX (implicit_cursor->set_rowcount(row_count)); // 设置隐式游标
        }
      }
      // update package info
      for (int64_t i = 0; OB_SUCC(ret) && i < package_vars_info.count(); i++) {
        OZ (spi_update_package_change_info(ctx, package_vars_info.at(i).first, package_vars_info.at(i).second));
      }

      if (OB_SUCC(ret) && not_found) {
        /*
         * Oracle模式：
         * 如果找不到数据，Select Into会抛出异常；BULK不报错；Returning不报错，结果undefined；cursor不会抛出异常，但是会把错误记录在CURSOR里
         * Mysql模式：
         * 无论静态SQL还是cursor都会抛出异常，但是静态SQL的NOT FOUND异常如果最后没有被捕捉会吞掉该异常，并报一个WARNING信息出来；
         * 如果是cursor的NOT FOUND异常如果最后没有被捕捉则会抛出这个错误。
         */
        if (lib::is_oracle_mode()) {
          // for not found, fetch into and bulk into and returning into not report error.
          if (!for_cursor && !is_bulk) {
            if (ObStmt::is_dml_write_stmt(static_cast<ObResultSet*>
                                            (result_set)->get_stmt_type())) {
              // dml returning with dynamic sql need set into value to null
              if (is_dynamic_sql && into_count > 0) {
                ObCastCtx cast_ctx(&tmp_allocator, &dtc_params, cast_mode, cast_coll_type);
                ObNewRow null_row;
                OZ (store_into_result(ctx, cast_ctx, null_row, into_exprs, column_types, type_count,
                             into_count, exprs_not_null, pl_integer_ranges, return_types, return_type_count,
                             actual_column_count, row_desc, is_type_record));
              }
            } else {
              // not fetch into, not returning into, must be select into
              ret = OB_READ_NOTHING;
            }
          } else if (for_cursor) {
            //cursor不会抛出异常，但是我们这里需要把错误抛给上层的spi_cursor_fetch来设置属性，PL在CG Fetch的时候再把错误码吞掉
            ret = OB_READ_NOTHING;
          }
        } else {
          ret = OB_READ_NOTHING;
        }
      }
    }
  } else if (!for_cursor) { //虽然不需要存储结果，但是也需要把get_next调一遍
    ObResultSet *ob_result_set = static_cast<ObResultSet*>(result_set);
    if (ob_result_set->is_with_rows()) { // SELECT或DML RETURNING
      ObPLASHGuard guard(ObPLASHGuard::ObPLASHStatus::IS_SQL_EXECUTION);
      if (lib::is_oracle_mode()) { // ORACLE Mode: only iterate to end
        const ObNewRow *row = NULL;
        while (OB_SUCC(ob_result_set->get_next_row(row))) {
          row_count++;
        }
        implicit_cursor->set_rowcount(row_count);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("read result error", K(ret), K(row_count));
        }
      } else { // MYSQL Mode: send query result to client.
        ObSQLSessionInfo *session_info = NULL;
        ObSyncCmdDriver  *query_sender = NULL;
        CK (OB_NOT_NULL(session_info = exec_ctx->get_my_session()));
        CK (OB_NOT_NULL(
          query_sender = static_cast<ObSyncCmdDriver *>(session_info->get_pl_query_sender())));
        OZ (query_sender->response_query_result(*ob_result_set,
                                                session_info->is_ps_protocol(),
                                                true,
                                                can_retry));
        OZ (query_sender->send_eof_packet(true));

        OX(implicit_cursor->set_rowcount(into_count > 0 ? 1 : 0));
      }
    } else if (stmt::T_ANONYMOUS_BLOCK != ob_result_set->get_stmt_type()) {
      // 不带Returing的INSERT，DELETE，UPDATE
      if (stmt::T_UPDATE == ob_result_set->get_stmt_type()) {
        ObPhysicalPlanCtx *phy_ctx
          = GET_PHY_PLAN_CTX(ob_result_set->get_exec_context());
        CK (OB_NOT_NULL(phy_ctx));
        OX (implicit_cursor->set_rowcount(phy_ctx->get_row_matched_count()));
      } else {
        implicit_cursor->set_rowcount(ob_result_set->get_affected_rows());
      }
    }
  } else { /*do nothing*/ }

  if (OB_SUCC(ret) && !for_cursor) {
    ObResultSet *ob_result_set = static_cast<ObResultSet*>(result_set);
    ObPhysicalPlan *physical_plan = ob_result_set->get_physical_plan();
    ObPhysicalPlanCtx *plan_ctx = nullptr;
    if (OB_NOT_NULL(physical_plan)) {
      if (OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ob_result_set->get_exec_context()))) {
        physical_plan->update_cache_access_stat(plan_ctx->get_table_scan_stat());
        plan_ctx->get_table_scan_stat().reset_cache_stat();
        if (is_forall) {
          const ImplicitCursorInfoArray &cursor_array = plan_ctx->get_implicit_cursor_infos();
          implicit_cursor->reset_bulk_rowcount();
          implicit_cursor->clear_row_count();
          for (int64_t i = 0; OB_SUCC(ret) && i < cursor_array.count(); ++i) {
            implicit_cursor->set_rowcount(cursor_array.at(i).affected_rows_);
          }
        }
      }
    }
  }
  return ret;
}


int ObSPIService::collect_cells(pl::ObPLExecCtx &ctx,
                                ObNewRow &row,
                                const ObDataType *result_types,
                                int64_t type_count,
                                const ObIArray<ObDataType> &row_desc,
                                bool is_strict,
                                ObIArray<ObCastCtx> &cast_ctxs,
                                int64_t hidden_column_count,
                                ObIArray<ObObj> &result,
                                ObResultSet *ob_result_set)
{
  int ret = OB_SUCCESS;
  ObObj tmp_obj;
  if (OB_ISNULL(result_types)
      || type_count + hidden_column_count != row.get_count()
      || type_count != row_desc.count()
      || type_count != cast_ctxs.count() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is Invalid",
      K(result_types), K(type_count), K(row_desc.count()), K(cast_ctxs.count()),
      K(row.get_count()), K(hidden_column_count), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count() - hidden_column_count; ++i) {
      tmp_obj.reset();
      ObObj &obj = row.get_cell(i);
      if (!obj.is_user_defined_sql_type()) {
        obj.set_collation_level(result_types[i].get_collation_level());
      }
      if (row_desc.at(i).get_udt_id() == T_OBJ_XML) {
        obj.set_subschema_id(ObXMLSqlType);
      }
      if (obj.is_pl_extend()) {
        // need deep copy immediately at bulk into scenes, because when fetch next row, current row will be free
        ObIAllocator *alloc = cast_ctxs.at(i).allocator_v2_;
        if (PL_CURSOR_TYPE == obj.get_meta().get_extend_type() ||
            PL_REF_CURSOR_TYPE == obj.get_meta().get_extend_type()) {
          alloc = &ctx.exec_ctx_->get_allocator();
        }
        OZ (ObUserDefinedType::deep_copy_obj(*alloc, obj, tmp_obj, true));
      } else if (obj.get_meta() == result_types[i].get_meta_type()
          && (row_desc.at(i).get_accuracy() == result_types[i].get_accuracy()
              || (result_types[i].get_meta_type().is_number() // NUMBER目标类型精度未知直接做赋值
                  && PRECISION_UNKNOWN_YET == result_types[i].get_accuracy().get_precision()
                  && ORA_NUMBER_SCALE_UNKNOWN_YET == result_types[i].get_accuracy().get_scale())
              || (result_types[i].get_meta_type().is_character_type() // CHAR/VARCHAR长度未知直接赋值
                  && (-1) == result_types[i].get_accuracy().get_length()))) {
        if (OB_FAIL(deep_copy_obj(*cast_ctxs.at(i).allocator_v2_, obj, tmp_obj))) {
          LOG_WARN("deep copy error", K(obj), K(ret));
        } else {
          LOG_DEBUG("same type deep copy directly", K(obj), K(tmp_obj), K(result_types[i]), K(i));
        }
      } else {
        LOG_DEBUG("column convert", K(i), K(obj.get_meta()), K(result_types[i].get_meta_type()),
                                    K(row_desc.at(i)), K(result_types[i].get_accuracy()));
        ObExprResType result_type;
        result_type.set_meta(result_types[i].get_meta_type());
        result_type.set_accuracy(result_types[i].get_accuracy());
        if ((result_type.is_blob() || result_type.is_blob_locator() || obj.is_blob() || obj.is_blob_locator()) && lib::is_oracle_mode()) {
          cast_ctxs.at(i).cast_mode_ |= CM_ENABLE_BLOB_CAST;
        }
        if (OB_ISNULL(cast_ctxs.at(i).exec_ctx_)) {
          cast_ctxs.at(i).exec_ctx_ = ctx.exec_ctx_;
        }
        OZ (ObExprColumnConv::convert_with_null_check(tmp_obj, obj, result_type, is_strict, cast_ctxs.at(i)));
        if (OB_SUCC(ret) && tmp_obj.need_deep_copy() && obj.get_string_ptr() == tmp_obj.get_string_ptr()) {
          // obj may not deep copied in ObExprColumnConv::convert(), do deep copy it if needed.
          ObObj tmp_obj2 = tmp_obj;
          CK (OB_NOT_NULL(cast_ctxs.at(i).allocator_v2_));
          OZ (deep_copy_obj(*cast_ctxs.at(i).allocator_v2_, tmp_obj2, tmp_obj));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(result.push_back(tmp_obj))) {
          LOG_WARN("push back error", K(obj), K(tmp_obj), K(ret));
        }
      }
    }
  }
  LOG_DEBUG("spi get result", K(result), K(ret));
  return ret;
}

int ObSPIService::check_package_dest_and_deep_copy(
  ObPLExecCtx &ctx,
  const ObSqlExpression &expr, ObIArray<ObObj> &src_array, ObIArray<ObObj> &dst_array)
{
  int ret = OB_SUCCESS;
  if (is_obj_access_expression(expr)) {
    if (expr.get_expr_items().count() > 1
        && T_OP_GET_PACKAGE_VAR == expr.get_expr_items().at(1).get_item_type()
        && OB_NOT_NULL(expr.get_expr_items().at(1).get_expr_operator())
        && expr.get_expr_items().at(1).get_expr_operator()->get_result_type().is_ext()) {
      uint16_t param_pos = expr.get_expr_items().at(1).get_param_idx();
      uint64_t package_id = OB_INVALID_ID;
      ObIAllocator *allocator = NULL;
      OX (package_id = expr.get_expr_items().at(param_pos).get_obj().get_uint64());
      OZ (spi_get_package_allocator(&ctx, package_id, allocator));
      CK (OB_NOT_NULL(allocator));
      for (int64_t i = 0; OB_SUCC(ret) && i < src_array.count(); ++i) {
        ObObj tmp;
        if (src_array.at(i).is_pl_extend()) {
          pl::ObUserDefinedType::deep_copy_obj(*allocator, src_array.at(i), tmp);
        } else {
          OZ (ob_write_obj(*allocator, src_array.at(i), tmp));
        }
        OZ (dst_array.push_back(tmp));
      }
    } else {
      OZ (dst_array.assign(src_array));
    }
  } else {
    OZ (dst_array.assign(src_array));
  }
  return ret;
}

int ObSPIService::convert_obj(ObPLExecCtx *ctx,
                              ObCastCtx &cast_ctx,
                              bool is_strict,
                              const ObSqlExpression *result_expr,
                              const ObIArray<ObDataType> &current_type,
                              ObIArray<ObObj> &obj_array,
                              const ObDataType *result_types,
                              int64_t type_count,
                              ObIArray<ObObj> &calc_array)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(ctx));
  CK(OB_NOT_NULL(ctx->func_));
  CK(OB_NOT_NULL(ctx->params_));
  CK(OB_NOT_NULL(ctx->exec_ctx_));
  CK(OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK(OB_NOT_NULL(result_types));
  CK(current_type.count() == type_count);
  CK(obj_array.count() == type_count);

  ObObj tmp_obj;
  for (int i = 0; OB_SUCC(ret) && i < obj_array.count(); ++i) {
    ObObj &obj = obj_array.at(i);
    tmp_obj.reset();
    if (!obj.is_user_defined_sql_type()) {
      // collation/collation level is used as subschema id for sql udt
      // but on field collation type, and collation level is set to other values for output
      // so, ignore cs_type, cs_level setting here
      obj.set_collation_level(result_types[i].get_collation_level());
    }
    LOG_DEBUG("column convert", K(obj.get_meta()), K(result_types[i].get_meta_type()),
              K(current_type.at(i)), K(result_types[i].get_accuracy()));
    if (obj.is_pl_extend()/* && pl::PL_RECORD_TYPE == obj.get_meta().get_extend_type()*/
        && result_types[i].get_meta_type().is_ext()) {
      //record嵌object场景，object属性在resolver阶段要求强一致，无需强转
      OZ (calc_array.push_back(obj));
    } else if (obj.get_meta() == result_types[i].get_meta_type()
        && (current_type.at(i).get_accuracy() == result_types[i].get_accuracy()
            || (result_types[i].get_meta_type().is_number() // NUMBER目标类型精度未知直接做赋值
                && PRECISION_UNKNOWN_YET == result_types[i].get_accuracy().get_precision()
                && ORA_NUMBER_SCALE_UNKNOWN_YET == result_types[i].get_accuracy().get_scale())
            || (result_types[i].get_meta_type().is_character_type() // CHAR/VARCHAR长度未知,直接赋值
                && -1 == result_types[i].get_accuracy().get_length()))) {
      ObObj tmp_obj;
      if (obj.is_pl_extend()) {
        ObIAllocator *alloc = cast_ctx.allocator_v2_;
        if (PL_CURSOR_TYPE == obj.get_meta().get_extend_type() ||
            PL_REF_CURSOR_TYPE == obj.get_meta().get_extend_type()) {
          alloc = &ctx->exec_ctx_->get_allocator();
        }
        OZ (ObUserDefinedType::deep_copy_obj(*alloc, obj, tmp_obj, true));
        if (OB_SUCC(ret) && obj.get_meta().get_extend_type() != PL_CURSOR_TYPE) {
          OZ (ObUserDefinedType::destruct_obj(obj, ctx->exec_ctx_->get_my_session()));
        }
      } else {
        OZ (deep_copy_obj(*cast_ctx.allocator_v2_, obj, tmp_obj));
      }
      if (OB_SUCC(ret) && tmp_obj.get_meta().is_bit()) {
        tmp_obj.set_scale(result_types[i].get_meta_type().get_scale());
      }
      OZ (calc_array.push_back(tmp_obj));
      if (OB_SUCC(ret)) {
        LOG_DEBUG("same type directyly copy", K(obj), K(tmp_obj), K(result_types[i]), K(i));
      }
    } else if (obj.is_null()
               && result_types[i].get_meta_type().is_ext()
               && (current_type.at(i).get_meta_type().is_user_defined_sql_type() && !current_type.at(i).get_meta_type().is_xml_sql_type())) {
      // only support xml null cast to xmltype, others will report error.
      ret = OB_ERR_INTO_EXPR_ILLEGAL;
      LOG_WARN("PLS-00597: expression 'string' in the INTO list is of wrong type", K(ret), K(obj), K(i), K(current_type.at(i)), K(result_types[i]));
    } else if (!obj.is_pl_extend()
               && !obj.is_user_defined_sql_type()
               && !obj.is_geometry()
               && !obj.is_null()
               && result_types[i].get_meta_type().is_ext()
               && !ob_is_xml_pl_type(result_types[i].get_obj_type(), result_types[i].get_udt_id())) {
      // sql udt or oracle gis can cast to pl extend, null from sql udt type can cast to pl extend(xmltype)
      // support: select extract(xmlparse(document '<a>a</a>'), '/b') into xml_data from dual;
      ret = OB_ERR_INTO_EXPR_ILLEGAL;
      LOG_WARN("PLS-00597: expression 'string' in the INTO list is of wrong type", K(ret), K(obj), K(i), K(current_type.at(i)), K(result_types[i]));
    } else {
      LOG_DEBUG("column convert", K(i), K(obj.get_meta()), K(result_types[i].get_meta_type()),
                                  K(current_type.at(i)), K(result_types[i].get_accuracy()));
      ObIArray<ObString> *type_info = NULL;
      // only mysql mode will run this logic
      if (ob_is_enum_or_set_type(result_types[i].get_obj_type())) {
        if (OB_ISNULL(result_expr)) {
          // do nothing
        } else if (!is_question_mark_expression(*result_expr)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only can store to local enum set variables", K(ret));
        } else {
          int64_t param_idx = get_const_value(*result_expr).get_unknown();
          if (param_idx >= ctx->func_->get_variables().count() || param_idx < 0) {
            ret = OB_ARRAY_OUT_OF_RANGE;
            LOG_WARN("param idx out of range", K(ret), K(param_idx));
          } else {
            OZ(ctx->func_->get_variables().at(param_idx).get_type_info(type_info));
          }
        }
      }
      ObObj &cur_time = ctx->exec_ctx_->get_physical_plan_ctx()->get_cur_time();
      cast_ctx.cur_time_ = cur_time.get_timestamp();
      ObExprResType result_type;
      OX (result_type.set_meta(result_types[i].get_meta_type()));
      if (result_types[i].get_meta_type().is_number()
          && result_types[i].get_precision() == -1
          && result_types[i].get_scale() == -1
          && current_type.at(i).get_meta_type().is_number()) {
        OX (result_type.set_accuracy(current_type.at(i).get_accuracy()));
      } else {
        OX (result_type.set_accuracy(result_types[i].get_accuracy()));
        if (OB_SUCC(ret) && result_type.is_enum_set_with_subschema()) {
          ObObjMeta org_obj_meta;
          if (OB_FAIL(ObRawExprUtils::extract_enum_set_collation(result_type,
                                                                ctx->exec_ctx_->get_my_session(),
                                                                org_obj_meta))) {
            LOG_WARN("fail to extrac enum set meta", K(ret));
          } else {
            result_type.set_collation(org_obj_meta);
          }
        }
      }
      if (OB_SUCC(ret) && (result_type.is_blob() || result_type.is_blob_locator() || obj.is_blob() || obj.is_blob_locator())
          && lib::is_oracle_mode()) {
        cast_ctx.cast_mode_ |= CM_ENABLE_BLOB_CAST;
      }
      if (OB_SUCC(ret) && ((result_type.is_ext() && (obj.is_user_defined_sql_type() || obj.is_geometry())))) {
        cast_ctx.exec_ctx_ = ctx->exec_ctx_;
      }
      if (OB_FAIL(ret)) {
      } else if (result_type.is_null() || result_type.is_unknown()) {
        tmp_obj = obj;
      } else {
        if (((obj.get_meta().is_ext())  // xmltype can not convert with other type in pl
              && !(result_type.get_type() == ObExtendType
                    || ob_is_xml_sql_type(result_type.get_type(), result_type.get_subschema_id())))
            || (obj.get_meta().get_type() == ObUserDefinedSQLType
                && !(result_type.get_type() == ObExtendType
                    || ob_is_xml_sql_type(result_type.get_type(), result_type.get_subschema_id())
                    || ob_is_string_tc(result_type.get_type())))
            || (!((obj.get_meta().is_ext())
                    || obj.get_meta().get_type() == ObUserDefinedSQLType
                    || obj.get_meta().is_geometry()
                    || obj.is_null())
               && (result_type.get_type() == ObExtendType
                    || ob_is_xml_sql_type(result_type.get_type(), result_type.get_subschema_id())))
            || (obj.get_meta().is_geometry() && lib::is_oracle_mode() && result_type.get_type() != ObExtendType)
            || (obj.is_null() && current_type.at(i).get_meta_type().is_xml_sql_type()
                && !ob_is_xml_pl_type(result_type.get_type(), result_type.get_accuracy().get_accuracy())
                && !ob_is_string_tc(result_type.get_type()))) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("xml type can not convert other type in pl", K(ret), K(obj), K(current_type.at(i)), K(i), K(result_type));
        } else if (result_type.is_ext()
                    && (result_type.get_accuracy().get_accuracy() == 300004 ||
                        result_type.get_accuracy().get_accuracy() == 300005)
                    && obj.is_xml_sql_type()) {
          // Disable sql_udt to anytype conversion
          ret = OB_ERR_INVALID_XML_DATATYPE;
          LOG_WARN("sql udt type can not convert extend any type", K(ret));
        } else if (OB_FAIL(ObExprColumnConv::convert_with_null_check(tmp_obj, obj, result_type, is_strict, cast_ctx, type_info))) {
          LOG_WARN("fail to convert with null check", K(ret));
        } else if (tmp_obj.is_null()) {
          if (!ob_is_string_tc(result_type.get_type()) &&
              (current_type.at(i).get_meta_type().is_xml_sql_type()
              || (current_type.at(i).get_meta_type().is_ext() && current_type.at(i).get_accuracy().get_accuracy() == T_OBJ_XML))) {
  #ifdef OB_BUILD_ORACLE_PL
            ObPLOpaque *opaque = reinterpret_cast<ObPLOpaque*>(cast_ctx.allocator_v2_->alloc(sizeof(ObPLOpaque)));;
            if (OB_ISNULL(opaque)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloca memory for xml null value", K(ret));
            } else {
              new (opaque) ObPLOpaque();
              tmp_obj.set_extend(reinterpret_cast<int64_t>(opaque), pl::PL_OPAQUE_TYPE);
            }
  #endif
          } else if (result_types[i].get_meta_type().is_ext() &&
                     (PL_RECORD_TYPE == result_types[i].get_meta_type().get_extend_type() ||
                      PL_NESTED_TABLE_TYPE == result_types[i].get_meta_type().get_extend_type() ||
                      PL_VARRAY_TYPE == result_types[i].get_meta_type().get_extend_type())) {
            int64_t udt_id = result_types[i].get_udt_id();
            const ObUserDefinedType *type = nullptr;
            int64_t ptr = 0;
            int64_t init_size = OB_INVALID_SIZE;
            OZ (ctx->get_user_type(udt_id, type));
            OZ (type->newx(*cast_ctx.allocator_v2_, ctx, ptr));
            OZ (type->get_size(PL_TYPE_INIT_SIZE, init_size));
            OX (tmp_obj.set_extend(ptr, type->get_type(), init_size));
          }
        }
        if (OB_ERR_DATA_TOO_LONG == ret && lib::is_oracle_mode()) {
          LOG_WARN("change error code to value error", K(ret));
          ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
        }
      }
      if (OB_SUCC(ret) && tmp_obj.need_deep_copy() && obj.get_string_ptr() == tmp_obj.get_string_ptr()) {
        // obj may not deep copied in ObExprColumnConv::convert(), do deep copy it if needed.
        ObObj tmp_obj2 = tmp_obj;
        CK (OB_NOT_NULL(cast_ctx.allocator_v2_));
        OZ (deep_copy_obj(*cast_ctx.allocator_v2_, tmp_obj2, tmp_obj));
      }
      OZ (calc_array.push_back(tmp_obj));
      if (OB_SUCC(ret) && tmp_obj.get_meta().is_bit()) {
        calc_array.at(i).set_scale(result_types[i].get_meta_type().get_scale());
      }
    }
  }

  return ret;
}

bool ObSPIService::is_sql_type_into_pl(ObObj &dest_addr, ObIArray<ObObj> &obj_array)
{
  bool bret = false;
  if (1 == obj_array.count()) {
    // query result is oracle gis, will convert to pl extend type
    if (dest_addr.is_pl_extend() && obj_array.at(0).is_pl_extend()) {
      ObPLComposite *left = reinterpret_cast<ObPLComposite*>(dest_addr.get_ext());
      ObPLComposite *right = reinterpret_cast<ObPLComposite*>(obj_array.at(0).get_ext());
      if (OB_NOT_NULL(left) && OB_NOT_NULL(right)
          && left->get_id() == right->get_id()
          && ObObjUDTUtil::ob_is_supported_sql_udt(left->get_id())) {
        bret = true;
      }
    }
  }
  return bret;
}

int ObSPIService::store_result(ObPLExecCtx *ctx,
                               const ObSqlExpression *result_expr,
                               const ObDataType *result_types,
                               int64_t type_count,
                               const bool *not_null_flags,
                               const int64_t *pl_integer_ranges,
                               const ObIArray<ObDataType> &row_desc,
                               bool is_strict,
                               ObCastCtx &cast_ctx,
                               ObIArray<ObObj> &obj_array,
                               const ObDataType *return_types,
                               int64_t return_type_count,
                               bool is_type_record)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  bool need_convert_type = true;
  ObSEArray<ObObj, OB_DEFAULT_SE_ARRAY_COUNT> tmp_obj_array;
  ObSEArray<ObObj, OB_DEFAULT_SE_ARRAY_COUNT> tmp_obj_array1;
  ObIArray<ObObj> *calc_array = &obj_array;
  CK(OB_NOT_NULL(ctx));
  CK(OB_NOT_NULL(ctx->func_));
  CK(OB_NOT_NULL(ctx->params_));
  CK(OB_NOT_NULL(ctx->exec_ctx_));
  CK(OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  CK(OB_NOT_NULL(result_expr));
  CK(OB_NOT_NULL(result_types));
  CK(OB_NOT_NULL(not_null_flags));
  CK(OB_NOT_NULL(pl_integer_ranges));
  CK(!obj_array.empty());
  bool is_schema_object = (!is_type_record &&
                           1 == type_count &&
                           1 == obj_array.count() &&
                           ((obj_array.at(0).is_pl_extend() &&
                           obj_array.at(0).get_meta().get_extend_type() != PL_CURSOR_TYPE &&
                           obj_array.at(0).get_meta().get_extend_type() != PL_OPAQUE_TYPE) || // xmltypes may need to do cast
                           (obj_array.at(0).is_null() &&
                           row_desc.at(0).get_meta_type().is_ext() &&
                           row_desc.at(0).get_meta_type().get_extend_type() > 0 &&
                           row_desc.at(0).get_meta_type().get_extend_type() < T_EXT_SQL_ARRAY &&
                           row_desc.at(0).get_meta_type().get_extend_type() != PL_CURSOR_TYPE &&
                           row_desc.at(0).get_meta_type().get_extend_type() != PL_OPAQUE_TYPE))); // xmltypes may need to do cast
  if (!is_schema_object) {
    if (OB_SUCC(ret) && type_count != obj_array.count()) {
      ret = OB_ERR_SP_INVALID_FETCH_ARG;
      LOG_WARN("type count is not equal to column count",
                K(obj_array.count()), K(type_count), K(ret));
    }
    if (OB_SUCC(ret)
        && (is_get_var_func_expression(*result_expr)
            || row_desc.count() != obj_array.count())) {
      need_convert_type = false;
    }
    //  检查not null修饰符是否生效
    for (int64_t i = 0; OB_SUCC(ret) && i < type_count; ++i) {
      if (not_null_flags[i] && obj_array.at(i).is_null()) {
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
        LOG_WARN("not null check violated",
                K(type_count),
                K(obj_array.at(i).is_null()),
                K(not_null_flags[i]),
                K(i),
                K(ret));
      }
    }

    // 做类型转换
    if (OB_FAIL(ret)) {
    } else if (return_types != nullptr && row_desc.count() == obj_array.count()) {
      OZ(convert_obj(ctx, cast_ctx, is_strict, result_expr, row_desc,
                    obj_array, return_types, return_type_count, tmp_obj_array));
      OX(calc_array = &tmp_obj_array);

      bool is_same = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_same && i < type_count; ++i) {
        if (!(return_types[i] == result_types[i])) {
          is_same = false;
        }
      }

      if (OB_SUCC(ret) && !is_same && need_convert_type) {
        ObSEArray<ObDataType, OB_DEFAULT_SE_ARRAY_COUNT> current_type;
        for (int i = 0; OB_SUCC(ret) && i < return_type_count; ++i) {
          OZ(current_type.push_back(return_types[i]));
        }

        OZ(convert_obj(ctx, cast_ctx, is_strict, result_expr, current_type,
                      tmp_obj_array, result_types, type_count, tmp_obj_array1));
        OX(calc_array = &tmp_obj_array1);
      }
    } else if (need_convert_type) {
      OZ(convert_obj(ctx, cast_ctx, is_strict, result_expr, row_desc,
                    obj_array, result_types, type_count, tmp_obj_array));
      OX(calc_array = &tmp_obj_array);
    }
    // check range
    for (int64_t i = 0; OB_SUCC(ret) && i < calc_array->count(); ++i) {
      OZ (sql::ObExprPLIntegerChecker::check_range(
        calc_array->at(i), calc_array->at(i).get_type(), pl_integer_ranges[i]));
    }
  } else if (obj_array.at(0).is_null()) { //null into extend variable，generate extend null obj
    if (row_desc.at(0).get_meta_type().is_ext() &&
      (PL_RECORD_TYPE == row_desc.at(0).get_meta_type().get_extend_type() ||
       PL_NESTED_TABLE_TYPE == row_desc.at(0).get_meta_type().get_extend_type() ||
       PL_VARRAY_TYPE == row_desc.at(0).get_meta_type().get_extend_type())) {
      int64_t udt_id = row_desc.at(0).get_udt_id();
      const ObUserDefinedType *type = nullptr;
      int64_t ptr = 0;
      int64_t init_size = OB_INVALID_SIZE;
      ObObj   tmp_obj;
      OX (tmp_obj.reset());
      OZ (ctx->get_user_type(udt_id, type));
      OZ (type->newx(*cast_ctx.allocator_v2_, ctx, ptr));
      OZ (type->get_size(PL_TYPE_INIT_SIZE, init_size));
      OX (tmp_obj.set_extend(ptr, type->get_type(), init_size));
      OZ (tmp_obj_array.push_back(tmp_obj));
      OX (calc_array = &tmp_obj_array);
    }
  }
  // 向变量赋值
  if (OB_SUCC(ret)) {
    ParamStore *params = ctx->params_;
    ObObjParam result_address;
    if (is_obj_access_expression(*result_expr)) { //通过ObjAccess访问得到的基础变量或record
      ObIAllocator *pkg_allocator = NULL;
      ObIAllocator *composite_allocator = nullptr;
      ObPlCompiteWrite *composite_write = nullptr;
      OZ (spi_calc_expr(ctx, result_expr, OB_INVALID_INDEX, &result_address));
      OX (composite_write = reinterpret_cast<ObPlCompiteWrite *>(result_address.get_ext()));
      CK (OB_NOT_NULL(composite_write));
      OX (result_address.set_extend(composite_write->value_addr_, result_address.get_meta().get_extend_type(), result_address.get_val_len()));
      OX (composite_allocator = reinterpret_cast<ObIAllocator *>(composite_write->allocator_));
      if (OB_SUCC(ret) &&
          !is_schema_object &&
          !is_sql_type_into_pl(result_address, *calc_array) &&
          PL_RECORD_TYPE == result_address.get_meta().get_extend_type()) {
        ObPLRecord *record = reinterpret_cast<ObPLRecord *>(result_address.get_ext());
        CK (OB_NOT_NULL(record));
        OX (composite_allocator = record->get_allocator());
      }
      if (OB_SUCC(ret)) {
        if (result_expr->get_expr_items().count() > 1
            && T_OP_GET_PACKAGE_VAR == result_expr->get_expr_items().at(1).get_item_type()
            && OB_NOT_NULL(result_expr->get_expr_items().at(1).get_expr_operator())
            && result_expr->get_expr_items().at(1).get_expr_operator()->get_result_type().is_ext()) {
          uint16_t param_pos = result_expr->get_expr_items().at(1).get_param_idx();
          uint64_t package_id = OB_INVALID_ID;
          OX (package_id = result_expr->get_expr_items().at(param_pos).get_obj().get_uint64());
          OZ (spi_get_package_allocator(ctx, package_id, pkg_allocator));
          CK (OB_NOT_NULL(pkg_allocator));
        }
        ObIAllocator *alloc = nullptr;
        if (OB_SUCC(ret)) {
          if (nullptr != composite_allocator) {
            alloc = composite_allocator;
          } else if (nullptr != pkg_allocator) {
            alloc = pkg_allocator;
          } else {
            alloc = ctx->allocator_;
          }
        }

        CK (OB_NOT_NULL(alloc));
        // udt会在store datums深拷
        if (OB_SUCC(ret) && !is_schema_object && !is_sql_type_into_pl(result_address, *calc_array)) {
          int64_t i = 0;
          for (; OB_SUCC(ret) && i < calc_array->count(); ++i) {
            ObObj tmp;
            if (calc_array->at(i).is_pl_extend()) {
              ObIAllocator *tmp_alloc = alloc;
              if (PL_CURSOR_TYPE == calc_array->at(i).get_meta().get_extend_type() ||
                  PL_REF_CURSOR_TYPE == calc_array->at(i).get_meta().get_extend_type()) {
                tmp_alloc = &ctx->exec_ctx_->get_allocator();
              }
              OZ (pl::ObUserDefinedType::deep_copy_obj(*tmp_alloc, calc_array->at(i), tmp));
            } else {
              OZ (ob_write_obj(*alloc, calc_array->at(i), tmp));
            }
            OX (calc_array->at(i) = tmp);
          }
          if (OB_FAIL(ret)) {
            for (int64_t j = 0; j < i - 1; ++j) {
              int ret = OB_SUCCESS;
              OZ (ObUserDefinedType::destruct_objparam(*alloc, calc_array->at(j), ctx->exec_ctx_->get_my_session()));
            }
          }
        }
        OZ (store_datums(result_address, *calc_array, alloc, ctx->exec_ctx_->get_my_session(), is_schema_object));
      }
    } else if (is_question_mark_expression(*result_expr)) { //通过question mark访问得到的基础变量
      int64_t param_idx = get_const_value(*result_expr).get_unknown();
      ObAccuracy accuracy;
      accuracy.set_accuracy(result_types[0].accuracy_);
      if (param_idx >= params->count() || param_idx < 0) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("param idx out of range", K(ret), K(param_idx), K(params->count()));
      } else {
        if (params->at(param_idx).is_pl_extend()
            && params->at(param_idx).get_ext() != 0
            && params->at(param_idx).get_ext() != calc_array->at(0).get_ext()) {
          OZ (ObUserDefinedType::destruct_objparam(*ctx->allocator_, params->at(param_idx), ctx->exec_ctx_->get_my_session()));
        }
        ObObj result;
        const ObPLDataType &var_type = ctx->func_->get_variables().at(param_idx);
        if (OB_FAIL(ret)) {
        } else if (!var_type.is_obj_type() ||
                  (var_type.get_data_type() != NULL && var_type.get_data_type()->get_meta_type().is_ext())) {
          uint64_t dst_id = OB_INVALID_INDEX;
          if (!calc_array->at(0).is_pl_extend()) {
            ret =OB_ERR_EXPRESSION_WRONG_TYPE;
            LOG_WARN("expr is wrong type", K(ret));
          } else if (PL_CURSOR_TYPE == calc_array->at(0).get_meta().get_extend_type() ||
                    PL_REF_CURSOR_TYPE == calc_array->at(0).get_meta().get_extend_type() ||
                    PL_OPAQUE_TYPE == calc_array->at(0).get_meta().get_extend_type()) {
            ObIAllocator *tmp_alloc = PL_OPAQUE_TYPE == calc_array->at(0).get_meta().get_extend_type() ? ctx->allocator_
                                                                                                       : &ctx->exec_ctx_->get_allocator();
            OZ (ObUserDefinedType::deep_copy_obj(*tmp_alloc, calc_array->at(0), result, true));
          } else {
            dst_id = var_type.get_user_type_id();
            ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(calc_array->at(0).get_ext());
            if (NULL == composite) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected composite to store datum", KPC(composite), K(ret));
            } else if (OB_INVALID_ID == dst_id || OB_INVALID_ID == composite->get_id()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected composite to store datum", K(dst_id), K(composite->get_id()), K(ret));
            } else if (dst_id != composite->get_id()) {
              ret =OB_ERR_EXPRESSION_WRONG_TYPE;
              LOG_WARN("expr is wrong type", K(dst_id), K(composite->get_id()), K(ret));
            } else {
              OZ (pl::ObUserDefinedType::deep_copy_obj(*ctx->allocator_, calc_array->at(0), result));
            }
          }
        } else if (var_type.is_obj_type() && var_type.get_data_type() != NULL &&
                   var_type.get_data_type()->get_meta_type().is_null() &&
                   calc_array->at(0).is_pl_extend()) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(*ctx->allocator_, calc_array->at(0), result));
        } else if (is_schema_object || calc_array->at(0).is_pl_extend()) {
          ret =OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("expr is wrong type", K(ret));
        } else {
          OZ (spi_pad_char_or_varchar(ctx->exec_ctx_->get_my_session(), result_types[0].get_obj_type(),
                                    accuracy, &tmp_alloc, &calc_array->at(0)));
          OZ (deep_copy_obj(*ctx->allocator_, calc_array->at(0), result));
        }
        if (OB_SUCC(ret)) {
          bool is_ref_cursor = false;
          is_ref_cursor = params->at(param_idx).is_ref_cursor_type();
          if (params->at(param_idx).is_pl_extend()) {
            if (params->at(param_idx).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
              ObUserDefinedType::destruct_objparam(*ctx->allocator_, params->at(param_idx), ctx->exec_ctx_->get_my_session());
            }
          } else {
            void *ptr = params->at(param_idx).get_deep_copy_obj_ptr();
            if (nullptr != ptr) {
              ctx->allocator_->free(ptr);
            }
            params->at(param_idx).set_null();
          }
          OX (params->at(param_idx) = result);
          OX (params->at(param_idx).set_is_ref_cursor_type(is_ref_cursor));
          OX (params->at(param_idx).set_param_meta());
        }
        OZ (spi_process_nocopy_params(ctx, param_idx));
        OX (params->at(param_idx).set_accuracy(accuracy));
      }
    } else if (is_get_var_func_expression(*result_expr)
              || is_get_package_or_subprogram_var_expression(*result_expr)) {
      //通过系统函数访问的基础变量(user var/sys var) 或 访问的package/subprogram 变量
      ObObjParam value;
      OX (value = calc_array->at(0));

      // outrow lob can not be assigned to user var, so convert outrow to inrow lob
      if (OB_SUCC(ret) && value.is_lob_storage()) {
        if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(value, value, nullptr, cast_ctx.allocator_v2_, true/*allow_persist_inrow*/))) {
          LOG_WARN("convert outrow to inrow lob fail", K(ret), K(value));
        }
      }

      CK (!value.is_pl_extend());
      OX (value.set_param_meta());
      if (OB_SUCC(ret)) {
        // save session value
        SMART_VAR(sql::ObSQLSessionInfo::StmtSavedValue, session_value) {
          int64_t nested_count = 0;
          OX (nested_count = ctx->exec_ctx_->get_my_session()->get_nested_count());
          OZ (ctx->exec_ctx_->get_my_session()->save_session(session_value));
          OZ (spi_set_variable(ctx, static_cast<const ObSqlExpression*>(result_expr), &value, false, true));
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = ctx->exec_ctx_->get_my_session()->restore_session(session_value))) {
            LOG_WARN("failed to restore session", K(tmp_ret));
            ret = COVER_SUCC(tmp_ret);
          }
          ctx->exec_ctx_->get_my_session()->set_nested_count(nested_count);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid result address",
               K(*calc_array), K(*result_expr), K(result_address), K(ret));
    }
  }
  return ret;
}

int ObSPIService::check_and_copy_composite(
  ObObj &result, ObObj &src, ObIAllocator &allocator, ObPLType type, uint64_t dst_udt_id)
{
  int ret = OB_SUCCESS;
  if (PL_OBJ_TYPE == type || PL_INTEGER_TYPE == type) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("expr is wrong type", K(ret));
#ifdef OB_BUILD_ORACLE_PL
  } else if (PL_OPAQUE_TYPE == type) {
    if (src.get_meta().get_extend_type() != PL_OPAQUE_TYPE) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("expr is wrong type", K(ret), K(src), K(type), K(dst_udt_id));
    } else {
      ObPLOpaque* opaque = reinterpret_cast<ObPLOpaque *>(src.get_ext());
      if (opaque->get_type() != ObPLOpaque::get_type(dst_udt_id)) {
        ret = OB_ERR_EXPRESSION_WRONG_TYPE;
        LOG_WARN("expr is wrong type", K(ret));
      }
    }
#endif
  } else {
    ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(src.get_ext());
    if (NULL == composite) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected composite to store datum", KPC(composite), K(ret));
    } else if (OB_INVALID_ID == dst_udt_id || OB_INVALID_ID == composite->get_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected composite to store datum", K(dst_udt_id), K(composite->get_id()), K(ret));
    } else if (dst_udt_id != composite->get_id()) {
      ret =OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("expr is wrong type", K(dst_udt_id), K(composite->get_id()), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // in collect_cells interface, it has been deep copy using tmp allocator,
    // now deep copy to table allocator and destruct obj_array
    // @hr351303: consider in collect_cells use table allocator directly to avoid twice deep copy
    OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, src, result));
  }
  return ret;
}

int ObSPIService::store_result(ObPLExecCtx *ctx,
                               ObIArray<ObPLCollection*> &bulk_tables,
                               int64_t row_count,
                               int64_t column_count,
                               ObIArray<ObObj> &obj_array,
                               bool append_mode,
                               bool is_type_record)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || bulk_tables.empty() || obj_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(bulk_tables), K(obj_array), K(ret));
  } else if (is_type_record && bulk_tables.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(bulk_tables), K(obj_array), K(ret));
  } else {
    int64_t start_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < bulk_tables.count(); ++i) {
      ObPLCollection *table = bulk_tables.at(i);
      bool need_ignore = false;
      for (int64_t j = i + 1; OB_SUCC(ret) && j < bulk_tables.count(); ++j) {
        if (table == bulk_tables.at(j)) {
          need_ignore = true;
          break;
        }
      }
      if (OB_ISNULL(table) || OB_ISNULL(table->get_allocator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table or allocator is NULL", K(table), K(ret));
      } else if (is_type_record) {
        // collection内部是record场景, bulk_tables.count() 只可能是1
        start_idx = 0;
      } else {
        //对于object而言, get_column_count是record的列数, 这里会整体拷贝object, 因此不能用get_column_count作为跳数
        start_idx += 0 == i ? 0 : 1;
      }
      if (OB_FAIL(ret)) {
      } else if (!need_ignore) {
        int64_t old_count = table->get_count();
        void *old_data = table->get_data();
        ObIAllocator *allocator = table->get_allocator();
        void *bulk_addr = append_mode ?
            allocator->alloc((row_count + old_count) * sizeof(ObObj))
            : allocator->alloc(row_count * sizeof(ObObj));
        if (OB_ISNULL(bulk_addr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("get a invalid address", K(bulk_addr), K(row_count), K(old_count), K(table->get_column_count()), K(table->get_data()), K(ret));
        } else {
          table->set_count(append_mode ? old_count + row_count : row_count);
          table->set_data(reinterpret_cast<ObObj*>(bulk_addr));
        }

        //初始化所有的ObObj
        if (OB_SUCC(ret) && append_mode && old_count > 0) {
          MEMCPY(bulk_addr, old_data, old_count * sizeof(ObObj));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < row_count; ++j) {
          OX (new (&reinterpret_cast<ObObj*>(table->get_data())[append_mode ? old_count + j : j])ObObj(ObNullType));
        }

        if (OB_SUCC(ret)) {
          int64_t j = 0;
          if (is_type_record) {
            // collection element is type record
            ObArray<ObObj> row;
            uint64_t record_id = table->get_element_desc().get_udt_id();
            const ObUserDefinedType *into_user_type = NULL;
            const ObRecordType *into_record_type = NULL;
            OZ (ctx->get_user_type(record_id, into_user_type));
            CK (OB_NOT_NULL(into_user_type));
            CK (into_user_type->is_record_type());
            OX (into_record_type = static_cast<const ObRecordType*>(into_user_type));
            for (; OB_SUCC(ret) && j < row_count; ++j) {
              OX (row.reset());
              CK (table->get_column_count() == into_record_type->get_record_member_count());
              for (int64_t k = 0; OB_SUCC(ret) && k < table->get_column_count(); ++k) {
                int64_t idx = j * column_count + start_idx + k;
                CK (OB_NOT_NULL(table->get_allocator()));
                if (OB_SUCC(ret)) {
                  if (obj_array.at(idx).is_null() && into_record_type->get_record_member_type(k)->is_composite_type()) {
                    const ObUserDefinedType *type = NULL;
                    int64_t ptr = 0;
                    int64_t init_size = OB_INVALID_SIZE;
                    OZ (ctx->get_user_type(into_record_type->get_record_member_type(k)->get_user_type_id(), type));
                    CK (OB_NOT_NULL(type));
                    OZ (type->newx(ctx->expr_alloc_, ctx, ptr));
                    OZ (type->get_size(PL_TYPE_INIT_SIZE, init_size));
                    OX (obj_array.at(idx).set_extend(ptr, type->get_type(), init_size));
                  }
                  OZ (row.push_back(obj_array.at(idx)));
                }
              }
              OZ (table->set_row(row, append_mode ? old_count + j : j));
            }
          } else { //如果table里是非record数据，直接按顺序存储数据即可
            int64_t current_datum = append_mode ?
                reinterpret_cast<int64_t>(bulk_addr) + old_count * sizeof(ObObj)
                : reinterpret_cast<int64_t>(bulk_addr);
            for (; OB_SUCC(ret) && j < row_count; ++j) {
              ObObj &current_obj = obj_array.at(j * column_count + start_idx);
              if (OB_UNLIKELY(table->is_not_null())) {
                if (current_obj.is_null()) {
                  ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
                  LOG_WARN("not null check violated", K(current_obj), K(i), K(j), K(*table), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                ObObj tmp;
                CK (OB_NOT_NULL(table->get_allocator()));
                if (OB_FAIL(ret)) {
                } else if (current_obj.is_pl_extend()) {
                  OZ (check_and_copy_composite(tmp,
                                               current_obj,
                                               *(table->get_allocator()),
                                               table->get_element_desc().get_pl_type(),
                                               table->get_element_desc().get_udt_id()));
                } else if (current_obj.is_null() && table->get_element_desc().is_composite_type()) {
                  const ObUserDefinedType *type = NULL;
                  int64_t ptr = 0;
                  int64_t init_size = OB_INVALID_SIZE;
                  OZ (ctx->get_user_type(table->get_element_desc().get_udt_id(), type), K(table->get_element_desc().get_udt_id()));
                  CK (OB_NOT_NULL(type));
                  OZ (type->newx(*(table->get_allocator()), ctx, ptr));
                  OZ (type->get_size(PL_TYPE_INIT_SIZE, init_size));
                  OX (tmp.set_extend(ptr, type->get_type(), init_size));
                } else {
                  OZ (deep_copy_obj(*table->get_allocator(), current_obj, tmp));
                }
                OZ (store_datum(current_datum, tmp, ctx->exec_ctx_->get_my_session(), table->get_allocator()));
              }
            }
          }
          if (OB_SUCC(ret)) {
            table->set_first(1);
            table->set_last(table->get_count());
            allocator->free(old_data);
          } else if (OB_NOT_NULL(bulk_addr)) {
            int ret = OB_SUCCESS;
            ObObj *new_data = reinterpret_cast<ObObj*>(table->get_data());
            new_data = append_mode ? (new_data + old_count) : new_data;
            for (int64_t i = 0; i < j; ++i) {
              if (OB_FAIL(ObUserDefinedType::destruct_objparam(*allocator, *new_data, ctx->exec_ctx_->get_my_session()))) {
                LOG_WARN("failed to destruct dirty table", K(ret), K(i), KPC(new_data));
              }
              new_data++;
            }
            table->set_data(reinterpret_cast<ObObj*>(old_data));
            table->set_count(old_count);
            allocator->free(bulk_addr);
          }
        }
      }
    }
  }
  return ret;
}

int ObSPIService::store_datums(ObObj &dest_addr, ObIArray<ObObj> &obj_array,
                               ObIAllocator *alloc, ObSQLSessionInfo *session_info, bool is_schema_object)
{
  int ret = OB_SUCCESS;
  ObIAllocator *datum_allocator = alloc;
  if (obj_array.empty() || OB_ISNULL(alloc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(dest_addr), K(obj_array), K(ret));
  } else if (is_schema_object || is_sql_type_into_pl(dest_addr, obj_array)) {
    ObObj src;
    CK (dest_addr.is_pl_extend());
    /* schema record只能作为单独的into variable存在
          拷贝时，要求src id和dest id必须一致. */
    CK (1 == obj_array.count());
    OX (src = obj_array.at(0));
    OV (src.is_pl_extend(), OB_ERR_EXPRESSION_WRONG_TYPE);
    OZ (check_and_deep_copy_result(*alloc, src, dest_addr));
  } else {
    int64_t current_datum = 0;
    bool is_opaque = false;
    if (dest_addr.is_pl_extend()) {
      if (PL_OPAQUE_TYPE == dest_addr.get_meta().get_extend_type()) {
        is_opaque = true;
        CK (1 == obj_array.count());
        OX (current_datum = reinterpret_cast<int64_t>(&dest_addr));
#ifdef OB_BUILD_ORACLE_PL
        if (OB_SUCC(ret) && obj_array.at(0).is_pl_extend()) {
          ObPLOpaque *src = reinterpret_cast<ObPLOpaque *>(obj_array.at(0).get_ext());
          ObPLOpaque *dst = reinterpret_cast<ObPLOpaque *>(dest_addr.get_ext());
          if (OB_FAIL(ObSPIService::spi_copy_opaque(NULL, NULL, *src, dst))) { // dst buffer has enough size?
            LOG_WARN("opaque data store datum failed ", KPC(src), K(dest_addr), K(obj_array), K(ret));
          } else if (OB_NOT_NULL(src)) {
            src->~ObPLOpaque();
            alloc->free(src);
          }
        }
#endif
      } else {
        ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(dest_addr.get_ext());
        ObPLRecord *record = NULL;
        if (NULL == composite) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected composite to store datum", KPC(composite), K(dest_addr), K(obj_array), K(ret));
        } else if (!composite->is_record()) {
          // user_defined_sql_type can be cast into pl_extend, so it cannot be blocked in the front,
          // but it is not allowed to be written into varray.
          // Inserting user_defined_sql_type into the PL_VARRAY_TYPE type will take this part of the logic.
          ret = OB_ERR_INTO_EXPR_ILLEGAL;
          LOG_WARN("PLS-00597: expression 'string' in the INTO list is of wrong type", K(ret));
        } else if (OB_ISNULL(record = static_cast<ObPLRecord*>(composite))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected record to store datum", KPC(record), KPC(composite), K(ret));
        } else if (record->get_count() != obj_array.count() && record->get_id() != T_OBJ_SDO_GEOMETRY) {
          //Example: for idx (select obj(1,2) from dual) loop null; end loop;
          //which is not supported yet!!! Will fixed it later in OB 4.2 version.
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "record`s element count not equal to select item count");
          LOG_WARN("record`s element count not equal to select item count", K(ret), KPC(record), K(obj_array));
        } else {
          ObPLAllocator1 *pl_allocator = dynamic_cast<ObPLAllocator1 *>(composite->get_allocator());
          CK (OB_NOT_NULL(pl_allocator));
          CK (alloc == pl_allocator);
          if (OB_SUCC(ret)) {
            ObPLRecord *record = static_cast<ObPLRecord*>(composite);
            current_datum = reinterpret_cast<int64_t>(record->get_element());
            record->set_is_null(false);
            datum_allocator = composite->get_allocator();
          }
        }
      }
    } else { //must be a single Obj
      CK (1 == obj_array.count());
      OX (current_datum = reinterpret_cast<int64_t>(dest_addr.get_ext()));
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < obj_array.count(); ++i) {
        int tmp_ret = OB_SUCCESS;
        if ((tmp_ret = ObUserDefinedType::destruct_objparam(*alloc, obj_array.at(i), session_info)) != OB_SUCCESS) {
          LOG_WARN("failed to destruct obj, memory may leak", K(ret), K(tmp_ret), K(i), K(obj_array));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && !is_opaque && i < obj_array.count(); ++i) {
      if (OB_FAIL(store_datum(current_datum, obj_array.at(i), session_info, datum_allocator))) {
        LOG_WARN("failed to arrange store", K(dest_addr), K(i), K(obj_array.at(i)), K(obj_array), K(ret));
      }
    }
  }
  return ret;
}

int ObSPIService::store_datum(int64_t &current_addr, const ObObj &obj, ObSQLSessionInfo *session_info, ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == current_addr) || obj.is_invalid_type() || OB_ISNULL(alloc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(current_addr), K(obj), K(ret));
  } else {
    ObObj *cur_obj = reinterpret_cast<ObObj*>(current_addr);
    OZ (ObUserDefinedType::destruct_objparam(*alloc, *cur_obj, session_info));
    if (OB_SUCC(ret)) {
      new (cur_obj)ObObj(obj);
      current_addr += sizeof(ObObj);
    }
  }
  return ret;
}

int ObSPIService::fill_cursor(ObResultSet &result_set,
                              ObSPICursor *cursor,
                              int64_t new_query_start_time,
                              int64_t orc_max_ret_rows) {
  int ret = OB_SUCCESS;
  int64_t old_time_out_ts = THIS_WORKER.get_timeout_ts();
  if (OB_ISNULL(cursor) || OB_ISNULL(cursor->allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(cursor), K(ret));
  } else {
    const common::ObNewRow *row = NULL;
    const common::ColumnsFieldIArray *fields = result_set.get_field_columns();
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    }
    if (OB_NOT_NULL(result_set.get_physical_plan()) && new_query_start_time > 0) {
      // fill_dbms_cursor do not need check hint , so only new_query_start_time > 0 will check hint timeout
      ObPhysicalPlan *plan = result_set.get_physical_plan();
      if (plan->get_phy_plan_hint().query_timeout_ > 0) {
        old_time_out_ts = THIS_WORKER.get_timeout_ts();
        THIS_WORKER.set_timeout_ts(new_query_start_time + plan->get_phy_plan_hint().query_timeout_);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fields->count(); ++i) {
      ObDataType type;
      type.set_meta_type(fields->at(i).type_.get_meta());
      type.set_accuracy(fields->at(i).accuracy_);
      if (OB_FAIL(cursor->row_desc_.push_back(type))) {
        LOG_WARN("push back error", K(i), K(fields->at(i).type_), K(fields->at(i).accuracy_),
                 K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orc_max_ret_rows; i++) {
      if (OB_FAIL(result_set.get_next_row(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("read result error", K(ret));
        }
        break;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get a invalud row", K(ret));
      } else {
        ObNewRow tmp_row = *row;
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_row.get_count(); ++i) {
          ObObj& obj = tmp_row.get_cell(i);
          ObObj tmp;
          if (obj.is_pl_extend()) {
            if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(*(cursor->allocator_), obj, tmp))) {
              LOG_WARN("failed to copy pl extend", K(ret));
            } else {
              obj = tmp;
              cursor->complex_objs_.push_back(tmp);
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(cursor->row_store_.add_row(tmp_row))) {
          LOG_WARN("failed to add row to row store", K(ret));
        }
      }
    }
  }
  THIS_WORKER.set_timeout_ts(old_time_out_ts);
  return ret;
}

int ObSPIService::fetch_row(void *result_set,
                            bool is_streaming,
                            int64_t &row_count,
                            ObNewRow &cur_row)
{
  int ret = OB_SUCCESS;
  ObPLASHGuard guard(ObPLASHGuard::ObPLASHStatus::IS_SQL_EXECUTION);
  if (OB_ISNULL(result_set)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(result_set), K(ret));
  } else if (!is_streaming) {
    ObSPICursor *cursor = static_cast<ObSPICursor*>(result_set);
    if (OB_FAIL(cursor->row_store_.get_row(cursor->cur_, cur_row))) {
      if (OB_INDEX_OUT_OF_RANGE == ret
          && (0 > cursor->cur_ || cursor->cur_ >= cursor->row_store_.get_row_cnt())) {
        ret = OB_ITER_END;
      } else {
        LOG_WARN("read result error", K(cursor->cur_), K(ret));
      }
    } else {
      ++cursor->cur_;
      ++row_count;
    }
  } else {
    ObResultSet *ob_result_set = static_cast<ObResultSet*>(result_set);
    const ObNewRow *row = NULL;
    if (OB_FAIL(ob_result_set->get_next_row(row))) {
      //上层判断返回值，这里不打印信息
    } else {
      cur_row = *row;
      ++row_count;
    }
  }
  LOG_DEBUG("spi fetch row", K(cur_row), K(row_count), K(ret));
  return ret;
}

int ObSPIService::get_result_type(ObPLExecCtx &ctx, const ObSqlExpression &expr, ObExprResType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Argument is NULL", K(ret));
  } else {
    if (is_const_expression(expr)) {
      if (is_question_mark_expression(expr)) {
        int64_t idx = get_const_value(expr).get_int();
        if (idx >= ctx.params_->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx is invlid", K(ret));
        } else {
          ObObjParam &obj_param = ctx.params_->at(idx);
          if (idx < ctx.func_->get_variables().count() &&
              ctx.func_->get_variables().at(idx).is_obj_type() &&
              OB_NOT_NULL(ctx.func_->get_variables().at(idx).get_data_type())) {
            type.set_meta(ctx.func_->get_variables().at(idx).get_data_type()->get_meta_type());
            type.set_accuracy(ctx.func_->get_variables().at(idx).get_data_type()->get_accuracy());
          } else {
            type.set_meta(obj_param.get_meta());
            type.set_accuracy(obj_param.get_accuracy());
          }
        }
      } else {
        ObObjType obj_type = static_cast<ObObjType>(get_expression_type(expr));
        type.set_type(obj_type);
        type.set_accuracy(get_first_expr_item(expr).get_accuracy());
      }
    } else {
      const ObExprOperator *op = get_first_expr_item(expr).get_expr_operator();
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr operator is NULL", K(ret));
      } else {
        type = op->get_result_type();
      }
    }
  }
  return ret;
}

const ObPostExprItem &ObSPIService::get_last_expr_item(const ObSqlExpression &expr)
{
  return expr.get_expr_items().at(expr.get_expr_items().count() - 1);
}

const ObInfixExprItem &ObSPIService::get_first_expr_item(const ObSqlExpression &expr)
{
  return expr.get_expr_items().at(0);
}

ObItemType ObSPIService::get_expression_type(const ObSqlExpression &expr)
{
  return get_first_expr_item(expr).get_item_type();
}

const ObObj &ObSPIService::get_const_value(const ObSqlExpression &expr)
{
  return get_first_expr_item(expr).get_obj();
}

bool ObSPIService::is_question_mark_expression(const ObSqlExpression &expr)
{
  return T_QUESTIONMARK == get_expression_type(expr);
}

bool ObSPIService::is_const_expression(const ObSqlExpression &expr)
{
  return IS_CONST_TYPE(get_expression_type(expr));
}

bool ObSPIService::is_obj_access_expression(const ObSqlExpression &expr)
{
  return T_OBJ_ACCESS_REF == get_expression_type(expr);
}

bool ObSPIService::is_get_var_func_expression(const ObSqlExpression &expr)
{
  return T_OP_GET_USER_VAR == get_expression_type(expr)
         || T_OP_GET_SYS_VAR == get_expression_type(expr);
}

bool ObSPIService::is_get_package_or_subprogram_var_expression(const ObSqlExpression &expr)
{
  return T_OP_GET_PACKAGE_VAR == get_expression_type(expr) || T_OP_GET_SUBPROGRAM_VAR == get_expression_type(expr);
}

int ObSPIService::force_refresh_schema(uint64_t tenant_id, int64_t refresh_version)
{
  int ret = OB_SUCCESS;
  int64_t local_version = OB_INVALID_VERSION;
  int64_t global_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is NULL", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_refreshed_schema_version(
                     tenant_id, local_version))) {
    LOG_WARN("fail to get local version", K(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                     tenant_id, global_version))) {
    LOG_WARN("fail to get global version", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    int64_t need_refresh_version = OB_INVALID_VERSION == refresh_version ? global_version : refresh_version;
    if (local_version >= need_refresh_version) {
      // do nothing
    } else if (OB_FAIL(GCTX.schema_service_->async_refresh_schema(tenant_id, need_refresh_version))) {
      LOG_WARN("failed to refresh schema",
              K(ret), K(tenant_id), K(local_version), K(global_version), K(refresh_version));
    }
  }
  return ret;
}

int ObSPIService::resolve_exec_params(const ParseResult &parse_result,
                                      ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      sql::ObRawExprFactory &expr_factory,
                                      pl::ObPLBlockNS &secondary_namespace,
                                      ObSPIPrepareResult &prepare_result,
                                      common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  for (ParamList *current_param = parse_result.param_nodes_;
       OB_SUCC(ret) && NULL != current_param;
       current_param = current_param->next_) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(current_param->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param node is NULL", K(ret));
    } else {
      if (session.is_for_trigger_package()
          && ObRawExprUtils::is_new_old_column_ref(current_param->node_)) {
        ParseNode *obj_access_node = NULL;
        OZ (ObRawExprUtils::mock_obj_access_ref_node(allocator, obj_access_node,
                                                     current_param->node_,
                                                     prepare_result.tg_timing_event_));
        CK (OB_NOT_NULL(obj_access_node));
        OZ (ObPLResolver::resolve_raw_expr(*obj_access_node,
                                           allocator,
                                           expr_factory,
                                           secondary_namespace,
                                           false, /*is_prepare_protocol*/
                                           expr));
      } else {
        OZ (ObPLResolver::resolve_local_var(*current_param->node_,
                                            secondary_namespace,
                                            expr_factory,
                                            &session,
                                            &schema_guard,
                                            expr));
      }
    }
    if (OB_FAIL(ret)) {
      // do noting
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    // prepare协议不支持执行参数是ENUM SET的情况，这里将ENUM SET转换为VARCHAR
    } else if (ob_is_enum_or_set_type(expr->get_result_type().get_type())) {
      ObSysFunRawExpr *out_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory,
                                                          expr,
                                                          out_expr,
                                                          &session,
                                                          true))) {
        LOG_WARN("failed to create type to str expr", K(ret));
      } else if (OB_ISNULL(out_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null raw expr", K(ret));
      } else {
        expr = out_expr;
      }
    }
    if (OB_SUCC(ret)) {
      bool find  = false;
      for (int64_t i = 0; OB_SUCC(ret) && !find && i < prepare_result.exec_params_.count(); ++i) {
        CK (OB_NOT_NULL(prepare_result.exec_params_.at(i)));
        if (OB_SUCC(ret)) {
          if (prepare_result.exec_params_.at(i)->same_as(*expr)) {
            find = true;
          }
        }
      }
      if (OB_SUCC(ret) && !find) {
        OZ (prepare_result.exec_params_.push_back(expr));
      }
    }
  }
  return ret;
}

int ObSPIService::resolve_into_params(const ParseResult &parse_result,
                                      ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      sql::ObRawExprFactory &expr_factory,
                                      pl::ObPLBlockNS &secondary_namespace,
                                      ObSPIPrepareResult &prepare_result)
{
  int ret = OB_SUCCESS;
  ParseNode *into_node = NULL;
  if (OB_ISNULL(parse_result.result_tree_)
      || OB_UNLIKELY(T_STMT_LIST != parse_result.result_tree_->type_)
      || OB_ISNULL(parse_result.result_tree_->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result tree is NULL or invalid result tree", K(ret));
  } else if (T_CREATE_VIEW == parse_result.result_tree_->children_[0]->type_) {
    if (parse_result.result_tree_->children_[0]->num_child_ <= 4
        || OB_ISNULL(parse_result.result_tree_->children_[0]->children_[4])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result tree is NULL or invalid result tree", K(ret));
    } else if (OB_FAIL(ObResolverUtils::get_select_into_node(*parse_result.result_tree_->children_[0]->children_[4],
                                                             into_node,
                                                             true))) {
      LOG_WARN("wrong usage of into clause", K(ret));
    } else if (OB_NOT_NULL(into_node)) {
      ret = OB_ERR_VIEW_SELECT_CONTAIN_INTO;
      LOG_WARN("View's SELECT contains a 'INTO' clause.", K(ret));
    }
  } else if (OB_FAIL(ObResolverUtils::get_select_into_node(*parse_result.result_tree_->children_[0],
                                                           into_node,
                                                           true))) {
      LOG_WARN("wrong usage of into clause", K(ret));
  } else if (T_SELECT == parse_result.result_tree_->children_[0]->type_
      && NULL != into_node
      && T_INTO_VARIABLES == into_node->type_) {
    if (OB_ISNULL(into_node->children_[0])
        || OB_UNLIKELY(T_INTO_VARS_LIST != into_node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid into vars", K(ret));
    } else {
      ParseNode *into_list = into_node->children_[0];
      for (int64_t i = 0; OB_SUCC(ret) && i < into_list->num_child_; ++i) {
        ObRawExpr* expr = NULL;
        if (OB_ISNULL(into_list->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param node is NULL", K(ret));
        } else if (T_IDENT == into_list->children_[i]->type_) {
          if (OB_FAIL(ObPLResolver::resolve_local_var(*into_list->children_[i],
                                                      secondary_namespace,
                                                      expr_factory,
                                                      &session,
                                                      &schema_guard,
                                                      expr))) {
            LOG_WARN("failed to resolve_local_var", K(ret));
          }
        } else if (T_USER_VARIABLE_IDENTIFIER == into_list->children_[i]->type_ ) {
          if (OB_FAIL(ObRawExprUtils::build_get_user_var(expr_factory,
                                                         ObString(into_list->children_[i]->str_len_,
                                                                  into_list->children_[i]->str_value_),
                                                         expr,
                                                         &session))) {
            LOG_WARN("Failed to build get user var", K(into_list->children_[i]->str_value_), K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid into node", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(prepare_result.into_exprs_.push_back(expr))) {
            LOG_WARN("push_back error", K(ret));
          }
        }
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObSPIService::resolve_ref_objects(const ParseResult &parse_result,
                                      ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      ObSPIPrepareResult &prepare_result)
{
  int ret = OB_SUCCESS;
  for (RefObjList *current_obj = parse_result.pl_parse_info_.ref_object_nodes_;
       OB_SUCC(ret) && NULL != current_obj;
       current_obj = current_obj->next_) {
    if (OB_ISNULL(current_obj->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param node is NULL", K(ret));
    } else {
      switch (current_obj->type_) {
      case REF_REL: {
        ObString db_name;
        ObString rel_name;
        const ObTableSchema *table_schema = NULL;
        if (OB_FAIL(ObStmtResolver::resolve_ref_factor(current_obj->node_, &session, rel_name, db_name))) {
          LOG_WARN("failed to resolve ref factor", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(session.get_effective_tenant_id(),
                                                         db_name,
                                                         rel_name,
                                                         false,
                                                         table_schema))) {
          if (OB_TABLE_NOT_EXIST == ret) {
            //找不到对象是正常的，do nothing
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("relation in pl not exists", K(db_name), K(rel_name), K(ret));
          }
        } else if (NULL ==table_schema) {
          //找不到对象是正常的，do nothing
        } else {
          ObSchemaObjVersion obj_version(table_schema->get_table_id(), table_schema->get_schema_version(), DEPENDENCY_TABLE);
          if (OB_FAIL(prepare_result.ref_objects_.push_back(obj_version))) {
            LOG_WARN("push_back error", K(ret));
          }
        }
      }
        break;
      case REF_PROC: {
        //procedure 不会出现在DML语句中
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should never be here", K(ret));
      }
        break;
      case REF_FUNC: {
        ObUDFInfo udf_info;
        ParseNode *node = current_obj->node_;
        ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
        bool need_check_udf = true;
        if (OB_FAIL(session.get_name_case_mode(case_mode))) {
          LOG_WARN("fail to get name case mode", K(ret));
        } else if (T_FUN_SYS == node->type_) {
          ObString func_name(static_cast<int32_t>(node->children_[0]->str_len_), node->children_[0]->str_value_);
          if (IS_FUN_SYS_TYPE(ObExprOperatorFactory::get_type_by_name(func_name))) {
            need_check_udf = false;
          }
        } else if (IS_FUN_SYS_TYPE(node->type_) || IS_AGGR_FUN(node->type_)) {
          need_check_udf = false;
        } else {}
        if (OB_SUCC(ret) && need_check_udf) {
          ParseNode *udf_node = node;
          if (T_FUN_SYS == node->type_) {
            ObString empty_db_name;
            ObString empty_pkg_name;
            if (OB_FAIL(ObResolverUtils::transform_func_sys_to_udf(reinterpret_cast<ObIAllocator *>(parse_result.malloc_pool_),
                node, empty_db_name, empty_pkg_name, udf_node))) {
              LOG_WARN("transform func sys to udf node failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObResolverUtils::resolve_udf_name_by_parse_node(udf_node, case_mode, udf_info))) {
              LOG_WARN("fail to resolve udf name", K(ret));
            } else if (udf_info.udf_database_.empty()) {
              udf_info.udf_database_ = session.get_database_name();
              if (lib::is_oracle_mode() && !udf_info.udf_database_.empty()) {
                size_t size = ObCharset::caseup(CS_TYPE_UTF8MB4_GENERAL_CI,
                                udf_info.udf_database_.ptr(), udf_info.udf_database_.length(),
                                udf_info.udf_database_.ptr(), udf_info.udf_database_.length());
                udf_info.udf_database_.set_length(static_cast<int32_t>(size));
              }
            }
            if (OB_SUCC(ret)) {
              ObSchemaChecker schema_checker;
              const ObRoutineInfo *func_info = NULL;
              if (OB_FAIL(schema_checker.init(schema_guard, session.get_sessid()))) {
                LOG_WARN("fail to init schema checker", K(ret));
              } else if (OB_FAIL(schema_checker.get_standalone_function_info(
                                                  session.get_effective_tenant_id(),
                                                  udf_info.udf_database_,
                                                  udf_info.udf_name_,
                                                  func_info))) {
                if (ret == OB_ERR_SP_DOES_NOT_EXIST) {
                  //找不到对象是正常的，do nothing
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("fail to get standalone function info",
                            K(ret), K(udf_info));
                }
              } else if (OB_ISNULL(func_info)) {
                //找不到对象是正常的，do nothing
              } else {
                ObSchemaObjVersion obj_version(func_info->get_routine_id(),
                                                 func_info->get_schema_version(),
                                                 DEPENDENCY_FUNCTION);
                if (OB_FAIL(prepare_result.ref_objects_.push_back(obj_version))) {
                  LOG_WARN("push_back error", K(ret));
                }
              }
            }
          }
        }
      }
        break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted ref object type", K(current_obj->type_), K(ret));
      }
      }
    }
  }
  return ret;
}

int ObSPIService::spi_process_nocopy_params(pl::ObPLExecCtx *ctx, int64_t local_idx, bool need_free)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (local_idx != OB_INVALID_INDEX);
  CK (ctx->params_->count() > local_idx);
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(ctx->nocopy_params_)
             && ctx->nocopy_params_->count() > local_idx) {
    const common::ObIArray<int64_t> *nocopy_params = ctx->nocopy_params_;
    ObObjParam &local_result = ctx->params_->at(local_idx);
    int64_t nocopy_idx = OB_INVALID_INDEX;
    CK (nocopy_params->count() > local_idx);
    OX (nocopy_idx = nocopy_params->at(local_idx));
    if (OB_FAIL(ret)) {
    } else if (nocopy_idx != OB_INVALID_INDEX) {
      for (int64_t i = 0; OB_SUCC(ret) && i < nocopy_params->count(); ++i) {
        if (nocopy_params->at(i) == nocopy_idx && i != local_idx) {
          void *ptr = ctx->params_->at(i).get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            ctx->allocator_->free(ptr);
          }
          OZ (deep_copy_obj(*ctx->allocator_, local_result, ctx->params_->at(i)));
        }
      }
    }
  }
  return ret;
}

int ObSPIService::spi_update_package_change_info(
  pl::ObPLExecCtx *ctx, uint64_t package_id, uint64_t var_idx)
{
  /*! 调用该函数前一定已经将PackageState存储在Session上, 此处直接在Session上获取PackageState */
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObPLPackageState *package_state = NULL;
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(session_info = ctx->exec_ctx_->get_my_session()));
  OZ (session_info->get_package_state(package_id, package_state));
  CK (OB_NOT_NULL(package_state));
  OZ (package_state->update_changed_vars(var_idx));
  OX (session_info->set_pl_can_retry(false));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObSPIService::spi_copy_opaque(
  pl::ObPLExecCtx *ctx, ObIAllocator *allocator,
  ObPLOpaque &src, ObPLOpaque *&dest, uint64_t package_id)
{
  int ret = OB_SUCCESS;
  bool is_new_opaque = false;
  UNUSEDx(ctx, package_id);
  if (NULL == dest) {
    CK (OB_NOT_NULL(allocator));
    if (OB_SUCC(ret) && OB_ISNULL(dest = reinterpret_cast<ObPLOpaque*>(allocator->alloc(src.get_init_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for dest opaque", K(ret), K(src.get_init_size()));
    }
    OX (new (dest)ObPLOpaque());
    OX (is_new_opaque = true);
  }
  if (OB_SUCC(ret)) {
    switch (src.get_type()) {
      case ObPLOpaqueType::PL_ANY_TYPE: {
        ObPLAnyType &anytype = static_cast<ObPLAnyType&>(src);
        OZ (anytype.deep_copy(dest));
      } break;
      case ObPLOpaqueType::PL_ANY_DATA: {
        ObPLAnyData &anydata = static_cast<ObPLAnyData&>(src);
        OZ (anydata.deep_copy(dest));
      } break;
      case ObPLOpaqueType::PL_XML_TYPE: {
        ObPLXmlType &xml = static_cast<ObPLXmlType&>(src);
        OZ (xml.deep_copy(dest));
      } break;
      case ObPLOpaqueType::PL_JSON_TYPE: {
        ObPLJsonBaseType &jsn = static_cast<ObPLJsonBaseType&>(src);
        OZ (jsn.deep_copy(dest));
      } break;
      default: {
        OZ (src.deep_copy(dest));
      } break;
    }
    if (OB_FAIL(ret) && is_new_opaque) {
      dest->~ObPLOpaque();
      allocator->free(dest);
      dest = NULL;
    }
  }
  return ret;
}
#endif

int ObSPIService::spi_check_composite_not_null(ObObjParam *v)
{
  int ret = OB_SUCCESS;
  ObPLComposite * composite = NULL;
  if (OB_NOT_NULL(v)
      && v->is_pl_extend()
      && OB_NOT_NULL(composite = reinterpret_cast<ObPLComposite *>(v->get_ext()))
      && composite->is_collection()) {
    ObPLCollection *coll = static_cast<ObPLCollection *>(composite);
    CK (OB_NOT_NULL(coll));
    if (OB_SUCC(ret) && coll->is_collection_null()) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("not null check violated", K(coll),
                                          K(coll->is_not_null()),
                                          K(coll->get_count()),
                                          K(ret));
    }
  }
  return ret;
}

int ObSPIService::spi_update_location(pl::ObPLExecCtx *ctx, uint64_t location)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx)
      && OB_NOT_NULL(ctx->pl_ctx_)
      && ctx->pl_ctx_->get_exec_stack().count() > 0) {
    ObIArray<ObPLExecState *> &stack = ctx->pl_ctx_->get_exec_stack();
    ObPLExecState *state = stack.at(stack.count() - 1);
    state->set_loc(location);
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObSPIService::spi_execute_dblink(ObExecContext &exec_ctx,
                                     ObIAllocator &allocator,
                                     uint64_t dblink_id,
                                     uint64_t package_id,
                                     uint64_t proc_id,
                                     ParamStore &params,
                                     ObObj *result,
                                     const ObRoutineInfo *dblink_routine_info)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  const pl::ObPLDbLinkInfo *dblink_info = NULL;
  pl::ObPLPackageGuard *package_guard = NULL;
  if (NULL != dblink_routine_info) {
    routine_info = dblink_routine_info;
  } else {
    OZ (exec_ctx.get_package_guard(package_guard));
    CK (OB_NOT_NULL(package_guard));
    OZ (package_guard->dblink_guard_.get_dblink_routine_info(dblink_id, package_id,
                                                             proc_id, routine_info),
                                                             dblink_id, package_id, proc_id);
    CK (OB_NOT_NULL(routine_info));
    OZ (package_guard->dblink_guard_.get_dblink_info(dblink_id, dblink_info));
    CK (OB_NOT_NULL(dblink_info));
  }
  OZ (spi_execute_dblink(exec_ctx, allocator, dblink_info, routine_info, params, result));
  return ret;
}

int ObSPIService::spi_execute_dblink(ObExecContext &exec_ctx,
                                     ObIAllocator &allocator,
                                     const pl::ObPLDbLinkInfo *dblink_info,
                                     const ObRoutineInfo *routine_info,
                                     ParamStore &params,
                                     ObObj *result)
{
  int ret = OB_SUCCESS;
  sql::DblinkGetConnType conn_type = sql::DblinkGetConnType::DBLINK_POOL;
  ObSQLSessionInfo *session = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  common::ObDbLinkProxy *dblink_proxy = NULL;
  common::sqlclient::ObISQLConnection *dblink_conn = NULL;
  ObString call_stmt;
  typedef common::sqlclient::DblinkDriverProto DbLinkType;
  DbLinkType link_type = DBLINK_UNKNOWN;
  int64_t affected_rows;
  transaction::ObTransID tx_id;
  bool is_native_oracle = false;
  CK (OB_NOT_NULL(session = exec_ctx.get_my_session()));
  CK (OB_NOT_NULL(routine_info));
  CK (OB_NOT_NULL(dblink_proxy = GCTX.dblink_proxy_));
  OX (tenant_id = session->get_effective_tenant_id());
  OZ (ObPLDblinkUtil::init_dblink(dblink_proxy, dblink_conn, routine_info->get_dblink_id(), *session, link_type, true));
  CK (OB_NOT_NULL(dblink_conn));
  OX (is_native_oracle = (DblinkDriverProto::DBLINK_DRV_OCI == dblink_conn->get_dblink_driver_proto()));
  if (OB_SUCC(ret)) {
    const int64_t out_param_cnt = routine_info->get_out_param_count();
    int64_t out_param_idx[out_param_cnt];
    for (int64_t i = 0; i < out_param_cnt; i++) {
      out_param_idx[i] = 0;
    }
    common::ObSEArray<const pl::ObUserDefinedType *, 1> udts;
    ParamStore exec_params((ObWrapperAllocator(allocator)));
    ObObj tmp_result;
    bool is_print_sql = false;
    if (routine_info->is_function()) {
      ObExecContext *top_ctx = &exec_ctx;
      while (OB_SUCC(ret) && !is_print_sql && NULL != top_ctx) {
        CK (OB_NOT_NULL(top_ctx->get_sql_ctx()));
        OX (is_print_sql = (stmt::T_SELECT == top_ctx->get_sql_ctx()->stmt_type_));
        OX (top_ctx = top_ctx->get_parent_ctx());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
      ObObjParam param_value;
      if (params.at(i).is_pl_extend()) {
        OZ (ObUserDefinedType::deep_copy_obj(allocator, params.at(i), param_value));
      } else {
        OZ (deep_copy_obj(allocator, params.at(i), param_value));
      }
      OX (param_value.set_is_pl_mock_default_param(params.at(i).is_pl_mock_default_param()));
      OX (param_value.set_param_meta());
      OX (param_value.set_accuracy(params.at(i).get_accuracy()));
      OZ (exec_params.push_back(param_value));
    }
    OZ (ObPLDblinkUtil::print_dblink_ps_call_stmt(allocator, dblink_info,
                                                  call_stmt, params, routine_info,
                                                  udts, out_param_idx, out_param_cnt,
                                                  is_print_sql, is_native_oracle));
    OZ (ObTMService::tm_rm_start(exec_ctx, link_type, dblink_conn, tx_id));
    OZ (dblink_proxy->dblink_execute_proc(OB_INVALID_TENANT_ID, dblink_conn, allocator,
                                          exec_params, call_stmt, *routine_info, udts,
                                          session->get_timezone_info(), &tmp_result, is_print_sql), call_stmt);
    OZ (spi_after_execute_dblink(exec_ctx, routine_info, allocator, params, exec_params, result, tmp_result));
    for (int64_t i = 0; i < exec_params.count(); i++) {
      if (exec_params.at(i).is_pl_extend()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(exec_params.at(i)))) {
          LOG_WARN("destruct obj failed", K(ret), K(exec_params.at(i)));
        }
      }
    }
    if (routine_info->is_function()
        && ob_is_extend(routine_info->get_ret_type()->get_obj_type())) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ObUserDefinedType::destruct_obj(tmp_result))) {
        LOG_WARN("destruct obj failed", K(ret), K(tmp_result));
      }
    }
    if (OB_SUCC(ret) && NULL != result && !result->is_null() && result->is_ext()) {
      CK (OB_NOT_NULL(exec_ctx.get_pl_ctx()));
      OZ (exec_ctx.get_pl_ctx()->add(*result));
    }
  }
  if (OB_NOT_NULL(dblink_conn)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObDblinkCtxInSession::revert_dblink_conn(dblink_conn))) {
      ret = (ret == OB_SUCCESS) ? tmp_ret : ret;
      LOG_WARN("failed to revert dblink conn", K(ret), K(tmp_ret), KP(dblink_conn));
    }
  }
  OZ (spi_check_timeout(exec_ctx));
  return ret;
}

int ObSPIService::spi_after_execute_dblink(ObExecContext &exec_ctx,
                                           const ObRoutineInfo *routine_info,
                                           ObIAllocator &allocator,
                                           ParamStore &params,
                                           ParamStore &exec_params,
                                           ObObj *result,
                                           ObObj &tmp_result)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(routine_info));
  CK (OB_NOT_NULL(session = exec_ctx.get_my_session()));
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    ObRoutineParam *param = routine_info->get_routine_params().at(i + routine_info->get_param_start_idx());
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is NULL", K(ret), K(i));
    } else if (param->is_out_sp_param() || param->is_inout_sp_param()) {
      if (ob_is_string_or_lob_type(exec_params.at(i).get_type())
          && (0 == exec_params.at(i).get_varchar().case_compare(""))) {
        params.at(i).set_null();
      } else {
        if (ob_is_extend(param->get_param_type().get_obj_type())) {
          OZ (ObUserDefinedType::deep_copy_obj(allocator, exec_params.at(i), params.at(i), true));
        } else if (param->get_param_type().get_obj_type() != exec_params.at(i).get_param_meta().get_type()) {
          const ObDataType &datatype = param->get_param_type();
          ObObjParam result_value;
          ObExprResType result_type;
          result_type.reset();
          result_type.set_meta(datatype.get_meta_type());
          result_type.set_accuracy(datatype.get_accuracy());
          OZ (spi_convert(session, &allocator, exec_params.at(i), result_type, result_value));
          OZ (deep_copy_obj(allocator, result_value, params.at(i)));
          OX (params.at(i).set_param_meta());
        } else {
          OZ (deep_copy_obj(allocator, exec_params.at(i), params.at(i)));
          OX (params.at(i).set_param_meta());
        }
      }
    }
  }
  if (OB_SUCC(ret) && routine_info->is_function()) {
    if (OB_ISNULL(routine_info->get_ret_type()) || OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return type is NULL", K(ret), KPC(routine_info), K(result));
    } else if (ob_is_string_or_lob_type(routine_info->get_ret_type()->get_obj_type())
               && (0 == tmp_result.get_varchar().case_compare(""))) {
      result->set_null();
    } else if (ob_is_extend(routine_info->get_ret_type()->get_obj_type())) {
      OZ (ObUserDefinedType::deep_copy_obj(exec_ctx.get_allocator(), tmp_result, *result, true));
    } else if (tmp_result.get_type() != routine_info->get_ret_type()->get_obj_type()) {
      const ObDataType *datatype = routine_info->get_ret_type();
      ObObj convert_obj;
      ObExprResType convert_type;
      CK (OB_NOT_NULL(datatype));
      OX (convert_type.reset());
      OX (convert_type.set_meta(datatype->get_meta_type()));
      OX (convert_type.set_accuracy(datatype->get_accuracy()));
      OZ (spi_convert(*session, allocator, tmp_result, convert_type, convert_obj));
      OZ (deep_copy_obj(exec_ctx.get_allocator(), convert_obj, *result));
    } else {
      OZ (deep_copy_obj(exec_ctx.get_allocator(), tmp_result, *result));
    }
  }
  return ret;
}

#endif

int ObSPIService::spi_opaque_assign_null(int64_t opaque_ptr)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  ObPLOpaque *opaque = reinterpret_cast<ObPLOpaque *>(opaque_ptr);
  CK (OB_NOT_NULL(opaque));
  OX (opaque->~ObPLOpaque()); // release orig resource
  OX (new (opaque) ObPLOpaque()); // reinit a new opaque which is invalid.
#endif // OB_BUILD_ORACLE_PL
  return ret;
}

int ObSPIService::spi_pl_profiler_before_record(pl::ObPLExecCtx *ctx, int64_t line, int64_t level)
{
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_ORACLE_PL

  ObSQLSessionInfo *session = nullptr;
  ObPLExecState *curr_state = nullptr;
  ObPLProfiler *profiler = nullptr;

  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()));

  CK (ctx->pl_ctx_);
  CK (curr_state = ctx->pl_ctx_->get_current_state());

  CK (OB_LIKELY(line > 0));
  CK (OB_LIKELY(level >= 0));

  if (OB_SUCC(ret) && OB_NOT_NULL(profiler = session->get_pl_profiler())) {
    ObPLProfilerTimeStack *time_stack = nullptr;

    if (OB_ISNULL(time_stack = curr_state->get_profiler_time_stack())) {
      CK (curr_state->get_allocator());

      if (OB_SUCC(ret)) {
        time_stack = OB_NEWx(ObPLProfilerTimeStack, curr_state->get_allocator(), *curr_state);

        if (OB_ISNULL(time_stack)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("[DBMS_PROFILER] failed to allocate memory for pl profiler time stack",
                   K(ret), KPC(profiler));
        } else {
          curr_state->set_profiler_time_stack(time_stack);
        }
      }
    }

    while (OB_SUCC(ret) && time_stack->get_last_level() >= level) {
      if (OB_FAIL(time_stack->pop(*profiler))) {
        LOG_WARN("[DBMS_PROFILER] failed to pop pl profiler time stack",
                K(ret), KPC(profiler), K(line), K(level));
      }
    }

    CK (OB_NOT_NULL(ctx->func_));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(
                   time_stack->push(ctx->func_->get_profiler_unit_info().first,
                                    ctx->func_->get_profiler_unit_info().second,
                                    line,
                                    level))) {
      LOG_WARN("[DBMS_PROFILER] failed to push into time stack",
               K(ret), KPC(profiler), K(line), K(level));
    }
  }

#endif // OB_BUILD_ORACLE_PL

  return ret;
}

int ObSPIService::spi_pl_profiler_after_record(pl::ObPLExecCtx *ctx, int64_t line, int64_t level)
{
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_ORACLE_PL

  ObSQLSessionInfo *session = nullptr;
  ObPLExecState *curr_state = nullptr;
  ObPLProfiler *profiler = nullptr;
  ObPLProfilerTimeStack *time_stack = nullptr;

  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(session = ctx->exec_ctx_->get_my_session()));

  CK (ctx->pl_ctx_);
  CK (curr_state = ctx->pl_ctx_->get_current_state());

  CK (OB_LIKELY(line > 0));
  CK (OB_LIKELY(level >= 0));

  if (OB_SUCC(ret)
        && OB_NOT_NULL(profiler = session->get_pl_profiler())
        && OB_NOT_NULL(time_stack = curr_state->get_profiler_time_stack())
        && time_stack->get_stack_count() > 0) {
    CK (OB_LIKELY(time_stack->get_last_level() != OB_INVALID_INDEX));

    while (OB_SUCC(ret) && time_stack->get_last_level() >= level) {
      if (OB_FAIL(time_stack->pop(*profiler))) {
        LOG_WARN("[DBMS_PROFILER] failed to pop pl profiler time stack",
                K(ret), KPC(profiler), K(line), K(level));
      }
    }
  }

#endif // OB_BUILD_ORACLE_PL

  return ret;
}

int ObSPIService::setup_cursor_snapshot_verify_(ObPLCursorInfo *cursor, ObSPIResultSet *spi_result)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = spi_result->get_result_set()->get_exec_context();
  transaction::ObTxReadSnapshot &snapshot = exec_ctx.get_das_ctx().get_snapshot();
  transaction::ObTxDesc *tx = exec_ctx.get_my_session()->get_tx_desc();
  bool need_register_snapshot = false;
  if (!snapshot.is_valid()) {
    need_register_snapshot = false;
  } else if (cursor->is_for_update()) {
    if (!tx || !tx->get_tx_id().is_valid() || !snapshot.tx_id().is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("for update cursor opened but not trans id invalid", K(ret), KPC(tx), K(snapshot));
    }
    need_register_snapshot = true;
  } else if (cursor->is_streaming() && tx && tx->is_in_tx()) {
    if (exec_ctx.get_my_session()->enable_enhanced_cursor_validation()) {
      need_register_snapshot = false;
      LOG_TRACE("enable cursor open check read uncommitted");
      const DependenyTableStore &tables = spi_result->get_result_set()->get_physical_plan()->get_dependency_table();
      ARRAY_FOREACH(tables, i) {
        if (tables.at(i).is_base_table()) {
          if (tx->has_modify_table((uint64_t)tables.at(i).get_object_id())) {
            LOG_TRACE("streaming cursor read uncommitted, need register snapshot",
                      K(tables.at(i).get_object_id()));
            need_register_snapshot = true;
            break;
          }
        }
      }
    } else {
      need_register_snapshot = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_register_snapshot) {
    OZ (cursor->set_and_register_snapshot(snapshot));
  } else {
    LOG_TRACE("convert to out of transaction snapshot", K(snapshot));
    snapshot.convert_to_out_tx();
    cursor->set_snapshot(snapshot);
  }
  LOG_TRACE("cursor setup snapshot", K(cursor->is_for_update()), K(cursor->is_streaming()),
            K(snapshot), KPC(cursor), K(need_register_snapshot));
  return ret;
}

ObPLSubPLSqlTimeGuard::ObPLSubPLSqlTimeGuard(pl::ObPLExecCtx *ctx) :
  old_sub_plsql_exec_time_(-1),
  execute_start_(ObTimeUtility::current_time()),
  state_(NULL)
{
  if (OB_NOT_NULL(ctx)
      && OB_NOT_NULL(ctx->pl_ctx_)
      && ctx->pl_ctx_->get_exec_stack().count() > 0) {
    // hold old time and reset sub time
    ObIArray<ObPLExecState *> &stack = ctx->pl_ctx_->get_exec_stack();
    state_ = stack.at(stack.count()-1);
    old_sub_plsql_exec_time_ = state_->get_sub_plsql_exec_time();
    old_pure_sql_exec_time_ = state_->get_pure_sql_exec_time();
    state_->reset_sub_plsql_exec_time();
    state_->reset_pure_sql_exec_time();
  }
}

ObPLSubPLSqlTimeGuard::~ObPLSubPLSqlTimeGuard()
{
  int ret = OB_SUCCESS;
  int64_t sql_exec_time = ObTimeUtility::current_time() - execute_start_;
  if (OB_NOT_NULL(state_)) {
    if (state_->get_sub_plsql_exec_time() > 0) {
      //此时纯SQL时间已经在 add_pl_exec_time 被加到了上层
      LOG_DEBUG("<<< sql exec time ", K(sql_exec_time), K(state_->get_sub_plsql_exec_time()),
        K(sql_exec_time-state_->get_sub_plsql_exec_time()), K(state_->get_pure_sql_exec_time()),
        K(old_sub_plsql_exec_time_), K(old_pure_sql_exec_time_));
    }
    // update sub plsql exec time
    if (-1 != old_sub_plsql_exec_time_) {
      state_->add_sub_plsql_exec_time(old_sub_plsql_exec_time_);
    }
    if (-1 != old_pure_sql_exec_time_) {
      state_->add_pure_sql_exec_time(old_pure_sql_exec_time_);
    }
  }
}

ObSPIRetryCtrlGuard::ObSPIRetryCtrlGuard(
  ObQueryRetryCtrl &retry_ctrl, ObSPIResultSet &spi_result, ObSQLSessionInfo &session_info, int &ret, bool for_fetch)
  : retry_ctrl_(retry_ctrl), spi_result_(spi_result), session_info_(session_info), ret_(ret), init_(false)
{
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  uint64_t eff_tenant_id = session_info_.get_effective_tenant_id();
  if (!for_fetch) {
    spi_result_.get_out_params().reset();
    spi_result_.reset_member_for_retry(session_info_);
  }
  retry_ctrl_.clear_state_before_each_retry(session_info_.get_retry_info_for_update());
  if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("already timeout!", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(eff_tenant_id, spi_result_.get_scheme_guard()))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(spi_result_.get_scheme_guard().get_schema_version(eff_tenant_id, tenant_version))) {
    LOG_WARN("fail get schema version", K(ret));
  } else if (OB_FAIL(spi_result_.get_scheme_guard().get_schema_version(OB_SYS_TENANT_ID, sys_version))) {
    LOG_WARN("fail get sys schema version", K(ret));
  } else {
    retry_ctrl_.set_tenant_local_schema_version(tenant_version);
    retry_ctrl_.set_sys_local_schema_version(sys_version);
    spi_result_.get_sql_ctx().schema_guard_ = &spi_result.get_scheme_guard();
    init_ = true;
  }
}

ObSPIRetryCtrlGuard::~ObSPIRetryCtrlGuard()
{
}

void ObSPIRetryCtrlGuard::test()
{
  int &ret = ret_;
  if (init_ && OB_FAIL(ret) && spi_result_.get_result_set() != NULL) {
    int cli_ret = OB_SUCCESS;
    retry_ctrl_.test_and_save_retry_state(GCTX,
                                          spi_result_.get_sql_ctx(),
                                          *(spi_result_.get_result_set()),
                                          ret,
                                          cli_ret,
                                          true,   /*force_local_retry in SPI, we can only do local retry*/
                                          false,  /*is_inner_sql*/
                                          true);  /*is_part_of_pl_sql*/
    ret = cli_ret;
    spi_result_.get_sql_ctx().clear();
    session_info_.set_session_in_retry(retry_ctrl_.need_retry());
  }
}

ObSPIExecEnvGuard::ObSPIExecEnvGuard(
  ObSQLSessionInfo &session_info, ObSPIResultSet &spi_result, bool is_ps_cursor)
  : session_info_(session_info), spi_result_(spi_result), is_ps_cursor_(is_ps_cursor)
{
  if (!is_ps_cursor_) {
    query_start_time_bk_ = session_info.get_query_start_time();
    session_info.set_query_start_time(ObTimeUtility::current_time());
  }
}

ObSPIExecEnvGuard::~ObSPIExecEnvGuard()
{
  session_info_.get_retry_info_for_update().clear();
  if (!is_ps_cursor_) {
    session_info_.set_query_start_time(query_start_time_bk_);
  }
}

}
}
