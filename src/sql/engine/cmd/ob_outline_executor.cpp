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

#include <regex.h>  // For regex validation (regcomp, regerror, regfree)
#include "sql/ob_sql.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/ddl/ob_create_outline_stmt.h"
#include "sql/resolver/ddl/ob_alter_outline_stmt.h"
#include "sql/resolver/ddl/ob_drop_outline_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/monitor/ob_sql_plan.h"
#include "sql/optimizer/ob_log_plan.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/schema/ob_schema_struct.h"
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
  outline_info.set_format_sql_id(create_outline_stmt->get_format_sql_id());

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
  ObString &outline_sql = outline_info.is_format() ?
            outline_info.get_format_sql_text_str() : outline_info.get_sql_text_str();
  int64_t max_concurrent = ObGlobalHint::UNSET_MAX_CONCURRENT;
  const ObQueryHint *query_hint = NULL;
  char* buf = NULL;
  int32_t len = 0;
  int32_t pos = 0;
  ObMaxConcurrentParam concurrent_param(&ctx.get_allocator());
  bool has_in_expr = false;
  int64_t in_expr_pos = 0;
  buf = (char *)ctx.get_allocator().alloc(outline_sql.length());
  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", K(ret));
  } else if (NULL == buf) {
    SQL_PC_LOG(WARN, "fail to alloc buf", K(outline_sql.length()));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(outline_stmt) || OB_ISNULL(outline_stmt->get_query_ctx())
             || OB_ISNULL(query_hint = &outline_stmt->get_query_ctx()->get_query_hint())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid outline stmt is NULL", K(ret), K(outline_stmt), K(query_hint));
  } else if (OB_FAIL(ObSQLUtils::get_outline_key(ctx.get_allocator(), ctx.get_my_session(),
                                                 outline_sql, outline_key,
                                                 concurrent_param.fixed_param_store_,
                                                 FP_PARAMERIZE_AND_FILTER_HINT_MODE,
                                                 has_questionmark_in_outline_sql,
                                                 outline_info.is_format()))) {
    LOG_WARN("fail to get outline key", "outline_sql", outline_sql, K(ret));
  } else if (OB_FAIL(ObSqlParameterization::search_in_expr_pos(outline_info.get_format_sql_text_str().ptr(),
                                                               outline_info.get_format_sql_text_str().length(),
                                                               in_expr_pos, has_in_expr))) {
    LOG_WARN("failed to search in expr", K(ret), K(outline_info.get_format_sql_text_str()));
  } else if (FALSE_IT(max_concurrent = query_hint->get_global_hint().max_concurrent_)) {
  } else if (OB_UNLIKELY(has_questionmark_in_outline_sql && query_hint->has_hint_exclude_concurrent())) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "sql text should have no ? when there is no concurrent limit");
    LOG_WARN("outline should have no ? when there is no concurrent limit",
             K(outline_sql), K(ret));
  } else if (max_concurrent > ObGlobalHint::UNSET_MAX_CONCURRENT
            && query_hint->has_hint_exclude_concurrent()) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "outline and sql concurrent limit can not be mixed");
    LOG_WARN("outline and sql concurrent limit can not be mixed",
    "outline_sql_text", outline_info.get_sql_text_str(), K(ret));
  } else if (OB_UNLIKELY(max_concurrent > ObGlobalHint::UNSET_MAX_CONCURRENT && has_in_expr
                         && concurrent_param.fixed_param_store_.count() > 0 && outline_info.is_format())) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "format outline with in expr not support concurrent limit, recommend to use normal outline");
    LOG_WARN("format outline with in expr can not have const param",
             "outline_format_sql_text", outline_info.get_format_sql_text_str(), K(ret));
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
                                              has_questionmark_in_target_sql,
                                              outline_info.is_format()))) {
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
                                                     has_questionmark_in_target_sql,
                                                     outline_info.is_format()))) {
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
  } else if (stmt.has_binding_rule()
             && (stmt.get_binding_rule().is_tenant_scope()
                 || stmt.get_binding_rule().get_map_item_count() > 0)
             && OB_FAIL(generate_binding_rule_info(ctx, &stmt, outline_info))) {
    LOG_WARN("generate_binding_rule_info failed", K(ret));
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
  }
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
  // SCOPE=TENANT outlines are stored with database_id = OB_PUBLIC_SCHEMA_ID.
  // The rootserver drop_outline() already handles fallback lookup from current db
  // to OB_PUBLIC_SCHEMA_ID when outline is not found.
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
// Helper: check if character is part of an identifier [a-zA-Z0-9_]
static inline bool is_identifier_char(char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') || c == '_';
}

// ==================== BINDING_RULE helper: case-insensitive find with word boundary ====================
static int64_t ob_find_str_ci(const char *haystack, int64_t hay_len,
                              const char *needle, int64_t needle_len)
{
  if (NULL == haystack || NULL == needle || needle_len <= 0 || needle_len > hay_len) {
    return -1;
  }
  for (int64_t i = 0; i <= hay_len - needle_len; ++i) {
    if (0 == strncasecmp(haystack + i, needle, needle_len)) {
      // Word boundary check: avoid matching inside longer identifiers
      bool left_ok = (i == 0) || !is_identifier_char(haystack[i - 1]);
      bool right_ok = (i + needle_len >= hay_len) || !is_identifier_char(haystack[i + needle_len]);
      if (left_ok && right_ok) {
        return i;
      }
      // else continue searching
    }
  }
  return -1;
}


// Helper: replace table name with placeholder for a single ObTableInHint
static int replace_single_table_in_hint(ObTableInHint *table_in_hint,
                                        ObOutlineBindingRule &binding_rule,
                                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  bool matched = false;
  for (int64_t m = 0; OB_SUCC(ret) && !matched && m < binding_rule.get_map_item_count(); ++m) {
    const ObOutlineRuleMapping &mapping = binding_rule.get_map_item(m);
    if (mapping.needs_placeholder()) {
      bool table_match = (0 == table_in_hint->table_name_.case_compare(
                                   mapping.get_original_table_name()));
      bool db_match = table_in_hint->db_name_.empty() ||
                      mapping.get_original_db_name().empty() ||
                      (0 == table_in_hint->db_name_.case_compare(
                                mapping.get_original_db_name()));
      if (table_match && db_match) {
        matched = true;
        int64_t position = mapping.get_ast_position();
        uint64_t tb_obj_id = mapping.get_tb_obj_id();
        ObSqlString tb_ph;
        if (OB_INVALID_ID == tb_obj_id) {
          const ObString &tbl = mapping.get_original_table_name();
          if (OB_FAIL(tb_ph.assign_fmt("TB_%.*s$%ld", tbl.length(), tbl.ptr(), position))) {
            LOG_WARN("fail to build tb placeholder", K(ret));
          }
        } else if (OB_FAIL(tb_ph.assign_fmt("TB_%lu$%ld", tb_obj_id, position))) {
          LOG_WARN("fail to build tb placeholder", K(ret));
        }
        if (OB_SUCC(ret)) {
          ObString deep_copy_tb;
          if (OB_FAIL(ob_write_string(allocator, tb_ph.string(), deep_copy_tb))) {
            LOG_WARN("fail to deep copy tb placeholder", K(ret));
          } else {
            table_in_hint->table_name_ = deep_copy_tb;
          }
        }
        if (OB_SUCC(ret) && mapping.has_db_prefix() && !table_in_hint->db_name_.empty()) {
          uint64_t db_obj_id = mapping.get_db_obj_id();
          ObSqlString db_ph;
          if (OB_INVALID_ID == db_obj_id) {
            const ObString &db = mapping.get_original_db_name();
            if (OB_FAIL(db_ph.assign_fmt("DB_%.*s$%ld", db.length(), db.ptr(), position))) {
              LOG_WARN("fail to build db placeholder", K(ret));
            }
          } else if (OB_FAIL(db_ph.assign_fmt("DB_%lu$%ld", db_obj_id, position))) {
            LOG_WARN("fail to build db placeholder", K(ret));
          }
          if (OB_SUCC(ret)) {
            ObString deep_copy_db;
            if (OB_FAIL(ob_write_string(allocator, db_ph.string(), deep_copy_db))) {
              LOG_WARN("fail to deep copy db placeholder", K(ret));
            } else {
              table_in_hint->db_name_ = deep_copy_db;
            }
          }
        }
      }
    }
  }
  return ret;
}

// Helper: replace table names with placeholders in a single ObHints array
static int replace_table_in_hints_array(ObIArray<ObHints> &hints_array,
                                        ObOutlineBindingRule &binding_rule,
                                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableInHint*, 16> all_tables;
  for (int64_t h = 0; OB_SUCC(ret) && h < hints_array.count(); ++h) {
    ObHints &hints = hints_array.at(h);
    for (int64_t i = 0; OB_SUCC(ret) && i < hints.hints_.count(); ++i) {
      ObHint *hint = hints.hints_.at(i);
      all_tables.reuse();
      if (OB_ISNULL(hint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hint is null", K(ret), K(h), K(i));
      } else if (OB_FAIL(hint->get_all_table_in_hint(all_tables))) {
        LOG_WARN("failed to get all table in hint", K(ret));
      }
      for (int64_t t = 0; OB_SUCC(ret) && t < all_tables.count(); ++t) {
        ObTableInHint *table_in_hint = all_tables.at(t);
        if (OB_ISNULL(table_in_hint)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_in_hint is null", K(ret), K(h), K(i), K(t));
        } else if (!table_in_hint->table_name_.empty()) {
          if (OB_FAIL(replace_single_table_in_hint(table_in_hint, binding_rule, allocator))) {
            LOG_WARN("failed to replace table in hint", K(ret), K(t));
          }
        }
      }
    }
  }
  return ret;
}

// ==================== BINDING_RULE: replace_table_with_placeholder_by_hint ====================
// Use hint AST to precisely replace table names with placeholders.
// This is more robust than string replacement because it only touches actual table references
// in hints (ObTableInHint), avoiding false matches on index names or substrings.
int ObOutlineExecutor::replace_table_with_placeholder_by_hint(
    ObDMLStmt *outline_stmt,
    ObOutlineBindingRule &binding_rule,
    ObIAllocator &allocator,
    ObSqlString &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(outline_stmt) || OB_ISNULL(outline_stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outline_stmt or query_ctx is NULL", K(ret));
  } else {
    ObQueryHint &query_hint = outline_stmt->get_query_ctx()->get_query_hint_for_update();

    // Step 1: Replace table names with placeholders in all hint arrays
    if (OB_FAIL(replace_table_in_hints_array(query_hint.qb_hints_, binding_rule, allocator))) {
      LOG_WARN("failed to replace tables in qb_hints", K(ret));
    } else if (OB_FAIL(replace_table_in_hints_array(query_hint.stmt_id_hints_, binding_rule, allocator))) {
      LOG_WARN("failed to replace tables in stmt_id_hints", K(ret));
    }

    // Step 2: Re-print all hints into outline_content string via print_outline_data
    if (OB_SUCC(ret)) {
      const int64_t buf_size = OB_MAX_SQL_LENGTH;  // 64KB
      char *buf = static_cast<char*>(allocator.alloc(buf_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buffer for hint printing", K(ret), K(buf_size));
      } else {
        PlanText plan_text;
        plan_text.buf_ = buf;
        plan_text.buf_len_ = buf_size;
        plan_text.pos_ = 0;
        plan_text.is_oneline_ = true;
        plan_text.is_outline_data_ = true;

        BUF_PRINT_CONST_STR("/*+", plan_text);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(query_hint.print_outline_data(plan_text))) {
          LOG_WARN("failed to print outline data", K(ret));
        } else {
          BUF_PRINT_CONST_STR("*/", plan_text);
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(result.assign(ObString(plan_text.pos_, buf)))) {
            LOG_WARN("fail to build final outline content", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// ==================== BINDING_RULE: generate_binding_rule_info ====================
int ObOutlineExecutor::generate_binding_rule_info(ObExecContext &ctx,
                                                  ObCreateOutlineStmt *stmt,
                                                  ObOutlineInfo &outline_info)
{
  int ret = OB_SUCCESS;
  ObOutlineBindingRule &binding_rule = stmt->get_binding_rule();  // Need non-const for auto-generating map_items
  ObIAllocator &allocator = ctx.get_allocator();
  ObSQLSessionInfo *session = ctx.get_my_session();

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    // is_template is derived from pattern_rules being non-empty (no explicit field)

    // Handle SCOPE=TENANT: set database_id to OB_PUBLIC_SCHEMA_ID
    if (binding_rule.is_tenant_scope()) {
      outline_info.set_database_id(OB_PUBLIC_SCHEMA_ID);
    }

    // 1. Template Signature: produced by the resolver from the pristine outline AST
    //    (before transform_stmt() rewrites it). Executor is purely a carrier here —
    //    if the resolver did not produce one, treat it as an internal error rather
    //    than re-deriving from the now-possibly-rewritten outline_stmt.
    if (OB_SUCC(ret)) {
      const ObString &ast_sig = stmt->get_template_signature();
      if (ast_sig.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("template signature missing from stmt; resolver should have produced it",
                 K(ret),
                 "outline_name", stmt->get_create_outline_arg().outline_info_.get_name_str());
      } else if (OB_FAIL(outline_info.set_signature(ast_sig))) {
        LOG_WARN("fail to set template signature on outline_info", K(ret));
      } else {
        LOG_DEBUG("[OUTLINE] create template outline signature",
                  "outline_name", stmt->get_create_outline_arg().outline_info_.get_name_str(),
                  K(ast_sig));
      }
    }

    // 2. Replace table references with DB_[objid]$N / TB_[objid]$N placeholders in outline_content
    // Use hint AST approach: modify ObTableInHint objects directly and re-print.
    // This is more robust than string replacement (avoids false matches on index names).
    if (OB_SUCC(ret) && binding_rule.get_map_item_count() > 0) {
      ObDMLStmt *outline_stmt = static_cast<ObDMLStmt *>(stmt->get_outline_stmt());
      if (OB_NOT_NULL(outline_stmt)) {
        ObSqlString new_content;
        if (OB_FAIL(replace_table_with_placeholder_by_hint(outline_stmt, binding_rule,
                                                           allocator, new_content))) {
          LOG_WARN("fail to replace table with placeholder by hint AST", K(ret));
        } else if (OB_FAIL(outline_info.set_outline_content(new_content.string()))) {
          LOG_WARN("fail to set outline content with placeholders", K(ret));
        }
      }
    }

    // 2a. Store generated placeholder strings back into map items for JSON serialization
    // Must happen BEFORE to_json_string so pattern_rules contains tb_placeholder/db_placeholder
    // CRITICAL: Must deep copy strings using allocator, as ObSqlString tb_ph/db_ph are destroyed
    // after each loop iteration. Shallow assignment causes tb_placeholder_ to point to freed memory.
    if (OB_SUCC(ret) && binding_rule.get_map_item_count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < binding_rule.get_map_item_count(); ++i) {
        ObOutlineRuleMapping &item = binding_rule.get_map_item(i);
        if (!item.needs_placeholder()) {
          continue;
        }
        int64_t placeholder_n = item.get_ast_position();
        uint64_t tb_obj_id = item.get_tb_obj_id();
        uint64_t db_obj_id = item.get_db_obj_id();
        ObSqlString tb_ph;
        ObString tb_ph_str;
        if (OB_INVALID_ID == tb_obj_id) {
          const ObString &tbl = item.get_original_table_name();
          if (OB_FAIL(tb_ph.assign_fmt("TB_%.*s$%ld", tbl.length(), tbl.ptr(), placeholder_n))) {
            LOG_WARN("fail to build tb placeholder string", K(ret));
          }
        } else {
          if (OB_FAIL(tb_ph.assign_fmt("TB_%lu$%ld", tb_obj_id, placeholder_n))) {
            LOG_WARN("fail to build tb placeholder string", K(ret));
          }
        }
        // Deep copy into allocator so string persists after tb_ph is destroyed
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(allocator, tb_ph.string(), tb_ph_str))) {
            LOG_WARN("fail to deep copy tb placeholder string", K(ret), K(i));
          } else if (OB_FAIL(item.set_tb_placeholder(tb_ph_str))) {
            LOG_WARN("fail to set tb_placeholder on mapping", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret) && item.has_db_prefix()) {
          ObSqlString db_ph;
          ObString db_ph_str;
          if (OB_INVALID_ID == db_obj_id) {
            const ObString &db = item.get_original_db_name();
            if (OB_FAIL(db_ph.assign_fmt("DB_%.*s$%ld", db.length(), db.ptr(), placeholder_n))) {
              LOG_WARN("fail to build db placeholder string", K(ret));
            }
          } else {
            if (OB_FAIL(db_ph.assign_fmt("DB_%lu$%ld", db_obj_id, placeholder_n))) {
              LOG_WARN("fail to build db placeholder string", K(ret));
            }
          }
          // Deep copy into allocator so string persists after db_ph is destroyed
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ob_write_string(allocator, db_ph.string(), db_ph_str))) {
              LOG_WARN("fail to deep copy db placeholder string", K(ret), K(i));
            } else if (OB_FAIL(item.set_db_placeholder(db_ph_str))) {
              LOG_WARN("fail to set db_placeholder on mapping", K(ret), K(i));
            }
          }
        }
      }
    }

    // 2b. Strip database name from outline_content for SCOPE=TENANT
    if (OB_SUCC(ret) && binding_rule.is_tenant_scope()) {
      ObString oc = outline_info.get_outline_content_str();
      if (!oc.empty()) {
        ObSqlString working;
        ObSqlString temp;
        if (OB_FAIL(working.assign(oc))) {
          LOG_WARN("fail to copy outline content for db strip", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < binding_rule.get_map_item_count(); ++i) {
          const ObOutlineRuleMapping &item = binding_rule.get_map_item(i);
          ObString db_name = item.get_original_db_name();
          if (db_name.empty()) {
            db_name = session->get_database_name();
          }
          if (!db_name.empty()) {
            // Build pattern: "db_name".  (quoted db name followed by dot)
            ObSqlString db_prefix;
            if (OB_FAIL(db_prefix.append("\"")) ||
                OB_FAIL(db_prefix.append(db_name)) ||
                OB_FAIL(db_prefix.append("\"."))) {
              LOG_WARN("fail to build db prefix pattern", K(ret), K(db_name));
            } else {
              bool found = true;
              while (OB_SUCC(ret) && found) {
                int64_t pos = ob_find_str_ci(working.ptr(), working.length(),
                                             db_prefix.ptr(), db_prefix.length());
                if (pos < 0) {
                  found = false;
                } else {
                  temp.reset();
                  if (OB_FAIL(temp.append(working.ptr(), pos))) {
                    LOG_WARN("fail to append prefix", K(ret));
                  } else if (OB_FAIL(temp.append(working.ptr() + pos + db_prefix.length(),
                                                 working.length() - pos - db_prefix.length()))) {
                    LOG_WARN("fail to append suffix", K(ret));
                  } else {
                    working.reset();
                    if (OB_FAIL(working.assign(temp.string()))) {
                      LOG_WARN("fail to assign temp", K(ret));
                    }
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(outline_info.set_outline_content(working.string()))) {
            LOG_WARN("fail to set outline content without db name", K(ret));
          } else {
            LOG_DEBUG("stripped db name from outline_content for SCOPE=TENANT",
                     "new_content", working.string());
          }
        }
      }
    }

    // 3. Validate regex patterns (fail-fast before persistence)
    // Invalid regex should be caught at CREATE time, not MATCH time
    // Prevents invalid regex like "[a-z" (missing ]) from being persisted
    static const int64_t REGEX_MAX_LEN = 128;  // Same as OB_PATTERN_MAX_REGEX_LEN in ob_pattern_matcher.h
    if (OB_SUCC(ret) && binding_rule.get_map_item_count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < binding_rule.get_map_item_count(); ++i) {
        const ObOutlineRuleMapping &item = binding_rule.get_map_item(i);
        // Validate db_var_regex if present
        const ObString &db_regex = item.get_db_var_regex();
        if (!db_regex.empty()) {
          regex_t test_regex;
          char regex_buf[REGEX_MAX_LEN + 8];
          if (db_regex.length() > REGEX_MAX_LEN) {
            ret = OB_ERR_REGEXP_ERROR;
            LOG_WARN("db_var_regex exceeds max length, cannot create outline",
                     K(ret), K(i), "length", db_regex.length(), K(REGEX_MAX_LEN));
          } else {
            MEMCPY(regex_buf, db_regex.ptr(), db_regex.length());
            regex_buf[db_regex.length()] = '\0';
            int reg_ret = regcomp(&test_regex, regex_buf, REG_EXTENDED | REG_NOSUB);
            if (0 != reg_ret) {
              ret = OB_ERR_REGEXP_ERROR;
              LOG_WARN("invalid db_var_regex, cannot create outline with invalid regex pattern",
                       K(ret), K(i), K(db_regex));
            } else {
              regfree(&test_regex);
            }
          }
        }
        // Validate table_var_regex if present
        if (OB_SUCC(ret)) {
          const ObString &tbl_regex = item.get_table_var_regex();
          if (!tbl_regex.empty()) {
            regex_t test_regex;
            char regex_buf[REGEX_MAX_LEN + 8];
            if (tbl_regex.length() > REGEX_MAX_LEN) {
              ret = OB_ERR_REGEXP_ERROR;
              LOG_WARN("table_var_regex exceeds max length, cannot create outline",
                       K(ret), K(i), "length", tbl_regex.length(), K(REGEX_MAX_LEN));
            } else {
              MEMCPY(regex_buf, tbl_regex.ptr(), tbl_regex.length());
              regex_buf[tbl_regex.length()] = '\0';
              int reg_ret = regcomp(&test_regex, regex_buf, REG_EXTENDED | REG_NOSUB);
              if (0 != reg_ret) {
                ret = OB_ERR_REGEXP_ERROR;
                LOG_WARN("invalid table_var_regex, cannot create outline with invalid regex pattern",
                         K(ret), K(i), K(tbl_regex));
              } else {
                regfree(&test_regex);
              }
            }
          }
        }
      }
    }

    // 4. Serialize binding_rule to JSON for pattern_rules
    if (OB_SUCC(ret)) {
      const int64_t json_buf_len = OB_MAX_SQL_LENGTH;
      char *json_buf = static_cast<char *>(allocator.alloc(json_buf_len));
      if (OB_ISNULL(json_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc json buffer", K(ret));
      } else {
        int64_t json_pos = 0;
        if (OB_FAIL(binding_rule.to_json_string(json_buf, json_buf_len, json_pos))) {
          LOG_WARN("fail to serialize binding rule to json", K(ret));
        } else {
          ObString json_str(json_pos, json_buf);
          if (OB_FAIL(outline_info.set_pattern_rules(json_str))) {
            LOG_WARN("fail to set pattern_rules", K(ret));
          }
        }
      }
    }

  }
  return ret;
}

}//namespace sql
}//namespace oceanbase
