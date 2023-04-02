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
#include "sql/engine/cmd/ob_outline_executor.h"

#include "lib/string/ob_string.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/ob_server_struct.h" //能获取mysql_proxy
#include "sql/ob_sql.h"
#include "sql/resolver/ddl/ob_create_outline_stmt.h"
#include "sql/resolver/ddl/ob_alter_outline_stmt.h"
#include "sql/resolver/ddl/ob_drop_outline_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/monitor/ob_sql_plan.h"
namespace oceanbase
{

using namespace common;
using namespace share::schema;
using namespace obrpc;

namespace sql
{


int ObOutlineExecutor::generate_outline_info2(ObExecContext &ctx,
                                             ObCreateOutlineStmt *create_outline_stmt,
                                             ObOutlineInfo &outline_info)
{
  int ret = OB_SUCCESS;
  outline_info.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
  outline_info.set_outline_content(create_outline_stmt->get_hint());
  outline_info.set_sql_id(create_outline_stmt->get_sql_id());

  if (create_outline_stmt->get_max_concurrent() >= 0) {
    ObMaxConcurrentParam concurrent_param(&ctx.get_allocator());
    concurrent_param.concurrent_num_ = create_outline_stmt->get_max_concurrent();
    if (OB_FAIL(outline_info.add_param(concurrent_param))) {
     LOG_WARN("fail to add param", K(ret));
    }
  }
  return ret;
}

int ObOutlineExecutor::generate_outline_info(ObExecContext &ctx,
                                             ObCreateOutlineStmt *outline_stmt,
                                             ObOutlineInfo &outline_info)
{
  int ret = OB_SUCCESS;
  ObCreateOutlineStmt *create_outline_stmt = reinterpret_cast<ObCreateOutlineStmt *>(outline_stmt);
  if (create_outline_stmt->get_outline_stmt() == NULL) {
    ret = generate_outline_info2(ctx, create_outline_stmt, outline_info);
  } else {
    ObDMLStmt *outline_stmt = static_cast<ObDMLStmt *>(create_outline_stmt->get_outline_stmt());
    ret = generate_outline_info1(ctx, outline_stmt, outline_info);
  }
  return ret;
}


int ObOutlineExecutor::generate_outline_info1(ObExecContext &ctx,
                                             ObDMLStmt *outline_stmt,
                                             ObOutlineInfo &outline_info)
{
  int ret = OB_SUCCESS;
  bool has_questionmark_in_outline_sql = false;
  ObString outline;
  ObString outline_key;
  ObString &outline_sql = outline_info.get_sql_text_str();
  int64_t max_concurrent = ObGlobalHint::UNSET_MAX_CONCURRENT;
  const ObQueryHint *query_hint = NULL;
  ObMaxConcurrentParam concurrent_param(&ctx.get_allocator());
  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", K(ret));
  } else if (OB_ISNULL(outline_stmt) || OB_ISNULL(outline_stmt->get_query_ctx())
             || OB_ISNULL(query_hint = &outline_stmt->get_query_ctx()->get_query_hint())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid outline stmt is NULL", K(ret), K(outline_stmt), K(query_hint));
  } else if (OB_FAIL(ObSQLUtils::get_outline_key(ctx.get_allocator(), ctx.get_my_session(),
                                                 outline_sql, outline_key,
                                                 concurrent_param.fixed_param_store_,
                                                 FP_PARAMERIZE_AND_FILTER_HINT_MODE,
                                                 has_questionmark_in_outline_sql))) {
    LOG_WARN("fail to get outline key", "outline_sql", outline_sql, K(ret));
  } else if (FALSE_IT(max_concurrent = query_hint->get_global_hint().max_concurrent_)) {
  } else if (OB_UNLIKELY(has_questionmark_in_outline_sql && max_concurrent < 0)) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "sql text should have no ? when there is no concurrent limit");
    LOG_WARN("outline should have no ? when there is no concurrent limit",
             K(outline_sql), K(ret));
  } else if (OB_UNLIKELY(max_concurrent > ObGlobalHint::UNSET_MAX_CONCURRENT
                         && query_hint->has_hint_exclude_concurrent())) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "outline and sql concurrent limit can not be mixed");
    LOG_WARN("outline and sql concurrent limit can not be mixed",
             "outline_sql_text", outline_info.get_sql_text_str(), K(ret));
  } else if (ObGlobalHint::UNSET_MAX_CONCURRENT == max_concurrent
             && OB_FAIL(get_outline(ctx, outline_stmt, outline))) {
    LOG_WARN("fail to get outline", K(ret));
  } else {
    //to check whether ok
    outline_info.set_outline_content(outline);
    outline_info.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    outline_info.set_signature(outline_key);
    ObString &target_sql = outline_info.get_outline_target_str();
    if (!target_sql.empty()) {
      ObString target_key;
      ObString target_key_with_hint;
      ObMaxConcurrentParam target_param(&ctx.get_allocator());
      ObMaxConcurrentParam target_param_with_hint(&ctx.get_allocator());
      bool has_questionmark_in_target_sql = false;
      bool is_same_param = true;
      //get signature derived from to_clause, then check if equal with signature derived from
      //on_clause
      if (OB_FAIL(ObSQLUtils::get_outline_key(ctx.get_allocator(), ctx.get_my_session(),
                                              target_sql, target_key,
                                              target_param.fixed_param_store_,
                                              FP_PARAMERIZE_AND_FILTER_HINT_MODE,
                                              has_questionmark_in_target_sql))) {
        LOG_WARN("fail to get outline key", K(target_sql), K(ret));

      } else if (target_key != outline_key || has_questionmark_in_target_sql != has_questionmark_in_outline_sql) {
        ret = OB_INVALID_OUTLINE;
        LOG_USER_ERROR(OB_INVALID_OUTLINE,
                       "signature derived from on_clause is not same as signature derived from to_clause");
        LOG_WARN("outline key is not same with target key", K(outline_sql), K(target_sql),
                 K(has_questionmark_in_target_sql), K(has_questionmark_in_outline_sql), K(ret));
      } else if (max_concurrent >= 0
                 && (OB_FAIL(concurrent_param.same_param_as(target_param, is_same_param)) || !is_same_param)) {
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to check if param is same", K(outline_sql), K(target_sql), K(ret));
        } else {
          ret = OB_INVALID_OUTLINE;
          LOG_USER_ERROR(OB_INVALID_OUTLINE,
                         "fixed_param  derived from on_clause is not same as fixed_param derived from to_clause");
          LOG_WARN("outline fixed_param is not same with target fixed_param", K(outline_sql), K(target_sql), K(ret));
        }
      } else if (OB_FAIL(ObSQLUtils::get_outline_key(ctx.get_allocator(), ctx.get_my_session(),
                                                     target_sql, target_key_with_hint,
                                                     target_param_with_hint.fixed_param_store_,
                                                     FP_MODE,
                                                     has_questionmark_in_target_sql))) {
        LOG_WARN("fail to get outline key", K(target_sql), K(ret));
      } else {
        //replace outline_key with target_key derived from to_clause with index not filtered
        outline_info.set_signature(target_key_with_hint);
      }
    }
    if (OB_SUCC(ret)) {
      //set concurrent limit info to ObOutlineInfo
      if (max_concurrent < 0) {
        //if concurrent num is negative, you should reset the max concurrent param store
      } else {
        concurrent_param.concurrent_num_ = max_concurrent;
        concurrent_param.sql_text_ = outline_info.get_sql_text_str();
        if (OB_FAIL(outline_info.add_param(concurrent_param))) {
          LOG_WARN("fail to add param", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOutlineExecutor::generate_logical_plan(ObExecContext &ctx,
                                             ObOptimizerContext &opt_ctx,
                                             ObDMLStmt *outline_stmt,
                                             ObLogPlan *&logical_plan)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObPhysicalPlan *phy_plan = NULL;
  ObOptimizer optimizer(opt_ctx);
  ObCacheObjGuard guard(OUTLINE_EXEC_HANDLE);
  if (OB_ISNULL(session_info) || OB_ISNULL(outline_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(session_info), K(outline_stmt));
  } else if (OB_FAIL(ObCacheObjectFactory::alloc(
                  guard, ObLibCacheNameSpace::NS_CRSR, session_info->get_effective_tenant_id()))) {
    LOG_WARN("fail to alloc phy_plan", K(ret));
  } else if (FALSE_IT(phy_plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj()))) {
    // do nothing
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to alloc physical plan from tc factory", K(ret));
  } else if (OB_FAIL(ObSql::calc_pre_calculable_exprs(outline_stmt->get_calculable_exprs(),
                                                      ctx,
                                                      *outline_stmt,
                                                      *phy_plan))) {
    LOG_WARN("fail to calc pre calculable expr", K(ret));
  } else if (OB_FAIL(ObSql::transform_stmt(opt_ctx.get_sql_schema_guard(),
                                           opt_ctx.get_opt_stat_manager(),
                                           &opt_ctx.get_local_server_addr(),
                                           phy_plan,
                                           ctx,
                                           outline_stmt))) {
    LOG_WARN("fail to transform outline stmt", K(ret));
  } else if (FALSE_IT(opt_ctx.set_root_stmt(outline_stmt))) {
    /*do nothing*/
  } else if (OB_FAIL(ObSql::optimize_stmt(optimizer, *session_info, *outline_stmt, logical_plan))) {
    LOG_WARN("fail to optimize stmt", K(ret));
  } else {/*do nothing*/}

  return ret;
}

bool ObOutlineExecutor::is_valid_outline_stmt_type(stmt::StmtType type)
{
  return  type == stmt::T_SELECT
      || type == stmt::T_INSERT
      || type == stmt::T_UPDATE
      || type == stmt::T_REPLACE
      || type == stmt::T_DELETE;
}

int ObOutlineExecutor::print_outline(ObExecContext &ctx, ObLogPlan *log_plan, ObString &outline)
{
  void *tmp_ptr = NULL;
  char *buf = NULL;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get log plan", K(ret), K(log_plan));
  } else if (OB_UNLIKELY(NULL == (tmp_ptr = ctx.get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {//the same as __all_outline column outline_content
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(ret));
  } else if (FALSE_IT(buf = static_cast<char *>(tmp_ptr))) {
  } else {
    PlanText plan_text;
    plan_text.buf_ = buf;
    plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
    if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, log_plan))) {
      LOG_WARN("failed to get plan outline info", K(ret));
    } else {
      outline.assign_ptr(buf, static_cast<ObString::obstr_size_t>(plan_text.pos_));
    }
  }
  return ret;
}

int ObOutlineExecutor::get_outline(ObExecContext &ctx, ObDMLStmt *outline_stmt, ObString &outline)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObPhysicalPlanCtx *pctx = ctx.get_physical_plan_ctx();
  ObLogPlan *log_plan = NULL;

  if (OB_ISNULL(session_info)
      || OB_ISNULL(pctx)
      || OB_ISNULL(outline_stmt)
      || OB_ISNULL(outline_stmt->get_query_ctx())
      || OB_ISNULL(ctx.get_sql_ctx())
      || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)
      || OB_ISNULL(ctx.get_stmt_factory())
      || OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(session_info), K(pctx), K(outline_stmt));
  } else if (!is_valid_outline_stmt_type(outline_stmt->get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline stmt type", K(outline_stmt->get_stmt_type()), K(ret));
  } else if (OB_ISNULL(ctx.get_expr_factory())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx.get_expr_factory()));
  } else {
    const ObGlobalHint &global_hint = outline_stmt->get_query_ctx()->get_global_hint();
    ObOptimizerContext optctx(session_info,
                              &ctx,
                              &outline_stmt->get_query_ctx()->sql_schema_guard_,
                              &ObOptStatManager::get_instance(),
                              ctx.get_allocator(),
                              &pctx->get_param_store(),
                              GCTX.self_addr(),
                              GCTX.srv_rpc_proxy_,
                              global_hint,
                              *ctx.get_expr_factory(),
                              outline_stmt,
                              false,
                              ctx.get_stmt_factory()->get_query_ctx());
    if (OB_FAIL(generate_logical_plan(ctx, optctx, outline_stmt, log_plan))) {
      LOG_WARN("fail to generate logical plan", K(ret));
    } else if (OB_FAIL(print_outline(ctx, log_plan, outline))) {
      LOG_WARN("fail to print outline", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObCreateOutlineExecutor::execute(ObExecContext &ctx, ObCreateOutlineStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString outline_key;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObCreateOutlineArg &arg = stmt.get_create_outline_arg();
  ObOutlineInfo &outline_info = arg.outline_info_;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_outline_info(ctx, &stmt, outline_info))) {
    LOG_WARN("generate_outline_info failed", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->reset())){
    LOG_WARN("schema_guard reset failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->create_outline(arg))) {
    LOG_WARN("rpc proxy create outline failed", "dst", common_rpc_proxy->get_server(), K(ret));
  } else {/*do nothing*/ }
  return ret;
}

int ObAlterOutlineExecutor::execute(ObExecContext &ctx, ObAlterOutlineStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString outline_key;
  ObString outline;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObAlterOutlineArg &arg = stmt.get_alter_outline_arg();
  ObOutlineInfo &outline_info = arg.alter_outline_info_;
  ObDMLStmt *outline_stmt = static_cast<ObDMLStmt *>(stmt.get_outline_stmt());
  ObString &outline_sql = stmt.get_outline_sql();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(outline_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outline stmt is empty", K(ret));
  } else if (OB_FAIL(generate_outline_info1(ctx, outline_stmt, outline_info))) {
    LOG_WARN("generate_outline_info failed", K(outline_sql), K(ret));
  } else {
    ObAlterOutlineInfo &alter_outline_info = arg.alter_outline_info_;
    int64_t index = OB_INVALID_INDEX;
    bool has_limit_param = false;
    if (OB_FAIL(alter_outline_info.has_concurrent_limit_param(has_limit_param))) {
      LOG_WARN("fail to judge whether outline_info has concurrent_limit_param", K(ret));
    } else if (has_limit_param) {
      index = ObAlterOutlineArg::ADD_CONCURRENT_LIMIT;
    } else if (!alter_outline_info.get_outline_content_str().empty()) {
      index = ObAlterOutlineArg::ADD_OUTLINE_CONTENT;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alter outline info", K(alter_outline_info), K(ret));
    }

    if (OB_SUCC(ret) && OB_FAIL(alter_outline_info.get_alter_option_bitset().add_member(index))) {
      LOG_WARN("failed to add member to alter_option_bitset", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->reset())){
    LOG_WARN("schema_guard reset failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->alter_outline(arg))) {
    LOG_WARN("rpc proxy alter outline failed", "dst", common_rpc_proxy->get_server(), K(ret));
  } else {/*do nothing*/ }
  return ret;
}

int ObDropOutlineExecutor::execute(ObExecContext &ctx, ObDropOutlineStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObDropOutlineArg arg = stmt.get_drop_outline_arg();
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->drop_outline(arg))) {
    LOG_WARN("rpc proxy drop outline failed", K(ret),
             "dst", common_rpc_proxy->get_server());
  } else {/*do nothing*/ }

  return ret;
}
}//namespace sql
}//namespace oceanbase
