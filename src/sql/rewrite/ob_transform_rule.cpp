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

#define USING_LOG_PREFIX SQL_REWRITE
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/stat/ob_stat_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_optimizer_partition_location_cache.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "observer/ob_server_struct.h"
#include "lib/json/ob_json_print_utils.h"

namespace oceanbase {
namespace sql {

bool ObTransformerCtx::is_valid()
{
  return NULL != allocator_ && NULL != session_info_ && NULL != schema_checker_ && NULL != exec_ctx_ &&
         NULL != expr_factory_ && NULL != stmt_factory_ && NULL != partition_location_cache_ && NULL != stat_mgr_ &&
         NULL != partition_service_ && NULL != sql_schema_guard_ && NULL != sql_schema_guard_->get_schema_guard() &&
         NULL != self_addr_;
}

int ObTransformRule::transform(ObDMLStmt*& stmt, uint64_t& transform_types)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObParentDMLStmt, 4> parent_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(transform_stmt_recursively(parent_stmts, 0, stmt))) {
    LOG_WARN("failed to transform stmt recursively", K(ret));
  } else if (OB_FAIL(adjust_transform_types(transform_types))) {
    LOG_WARN("failed to adjust transform types", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformRule::adjust_transform_types(uint64_t& transform_types)
{
  int ret = OB_SUCCESS;
  UNUSED(transform_types);
  return ret;
}

int ObTransformRule::transform_stmt_recursively(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(current_level), K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(current_level), K(is_stack_overflow), K(ret));
  } else if (is_large_stmt(*stmt) && !(transformer_type_ == PRE_PROCESS || transformer_type_ == POST_PROCESS)) {
    // skip transformation for large stmt
    // mainly for union stmt, select union select union select union ... union select
  } else if (TransMethod::POST_ORDER == transform_method_) {
    if (OB_FAIL(transform_post_order(parent_stmts, current_level, stmt))) {
      LOG_WARN("failed to do top down transformation", K(*stmt), K(current_level), K(ret));
    } else { /*do nothing*/
    }
  } else if (TransMethod::PRE_ORDER == transform_method_) {
    if (OB_FAIL(transform_pre_order(parent_stmts, current_level, stmt))) {
      LOG_WARN("failed to do top down transformation", K(*stmt), K(current_level), K(ret));
    } else { /*do nothing*/
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported transformation method", K(ret));
  }
  return ret;
}

// pre-order transformation
int ObTransformRule::transform_pre_order(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(current_level), K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(current_level), K(is_stack_overflow), K(ret));
  } else if (OB_FAIL(transform_self(parent_stmts, stmt))) {
    LOG_WARN("failed to transform self statement", K(ret));
  } else if (OB_FAIL(transform_children(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform children stmt", K(ret));
  }
  return ret;
}

// post-order transformation
int ObTransformRule::transform_post_order(
    ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(stmt), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(current_level), K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(current_level), K(is_stack_overflow), K(ret));
  } else if (OB_FAIL(transform_children(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform children stmt", K(ret));
  } else if (OB_FAIL(transform_self(parent_stmts, stmt))) {
    LOG_WARN("failed to transform self statement", K(ret));
  }
  return ret;
}

/**
 * @brief ObCostBasedRewriteRule::accept_transform
 * @param stmts: origin stmt
 * @param trans_stmts: transformed stmt
 * @param trans_happened
 * check whether the transformed stmt has less cost than the origin stmt
 * @return
 */
int ObTransformRule::accept_transform(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, ObDMLStmt* trans_stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  double trans_stmt_cost = 0.0;
  trans_happened = false;
  ObDMLStmt* top_stmt = parent_stmts.empty() ? stmt : parent_stmts.at(0).stmt_;
  cost_based_trans_tried_ = true;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(trans_stmt) || OB_ISNULL(top_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context is null", K(ret), K(ctx_), K(stmt), K(trans_stmt), K(top_stmt));
  } else if (OB_FAIL(evaluate_cost(parent_stmts, trans_stmt, trans_stmt_cost))) {
    LOG_WARN("failed to evaluate cost for the transformed stmt", K(ret));
  } else if (stmt_cost_ >= 0) {
    // do nothing if origin stmt cost has been evaluted
  } else if (OB_FAIL(evaluate_cost(parent_stmts, stmt, stmt_cost_))) {
    LOG_WARN("failed to evaluate cost for the origin stmt", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (trans_stmt_cost < stmt_cost_ * COST_BASE_TRANSFORM_THRESHOLD) {
      LOG_TRACE("accept transform because the cost is decreased", K_(stmt_cost), K(trans_stmt_cost));
      stmt = trans_stmt;
      stmt_cost_ = trans_stmt_cost;
      trans_happened = true;
    } else {
      LOG_TRACE("reject transform because the cost is increased", K_(stmt_cost), K(trans_stmt_cost));
    }
  }
  return ret;
}

int ObTransformRule::evaluate_cost(common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, double& plan_cost)
{
  int ret = OB_SUCCESS;
  bool aggr_pushdown_allowed = true;
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObTransformerImpl trans(ctx_);
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(stmt_factory = ctx_->stmt_factory_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
             OB_ISNULL(session_info = ctx_->session_info_) ||
             OB_ISNULL(schema_guard = ctx_->schema_checker_->get_sql_schema_guard()) ||
             OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt factory", K(ret), K(stmt_factory), K(expr_factory), K(session_info), K(schema_guard));
  } else if (OB_FAIL(ctx_->session_info_->if_aggr_pushdown_allowed(aggr_pushdown_allowed))) {
    LOG_WARN("fail to get aggr_pushdown_allowed", K(ret));
  } else {
    ObQueryHint query_hint;
    ObOptimizerPartitionLocationCache optimizer_location_cache(*ctx_->allocator_, ctx_->partition_location_cache_);
    ObDMLStmt* optimizer_stmt = NULL;
    ObDMLStmt* temp_stmt = NULL;
    lib::MemoryContext* mem_context = nullptr;
    lib::ContextParam param;
    param.set_mem_attr(session_info->get_effective_tenant_id(), "CostBasedRewrit", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(ObTransformRule::adjust_transformed_stmt(parent_stmts, stmt, optimizer_stmt))) {
      LOG_WARN("failed to adjust transformed stmt", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*stmt_factory, *expr_factory, optimizer_stmt, temp_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (OB_FAIL(temp_stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(trans.transform_heuristic_rule(reinterpret_cast<ObDMLStmt*&>(temp_stmt)))) {
      LOG_WARN("failed to transform heuristic rule", K(ret));
    } else if (OB_FAIL(temp_stmt->check_and_convert_hint(*session_info))) {
      LOG_WARN("failed to check and convert hint", K(ret));
    } else if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context, param))) {
      LOG_WARN("failed to create memory entity", K(ret));
    } else if (OB_ISNULL(mem_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create memory entity", K(ret));
    } else {
      int64_t question_mark = stmt->get_question_marks_count();
      query_hint = optimizer_stmt->get_stmt_hint().get_query_hint();

      ObOptimizerContext optctx(ctx_->session_info_,
          ctx_->exec_ctx_,
          ctx_->sql_schema_guard_,
          ctx_->stat_mgr_,
          ctx_->opt_stat_mgr_,
          ctx_->partition_service_,
          mem_context->get_arena_allocator(),
          &optimizer_location_cache,
          &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
          *ctx_->self_addr_,
          GCTX.srv_rpc_proxy_,
          ctx_->merged_version_,
          query_hint,
          *ctx_->expr_factory_,
          temp_stmt);
      ObOptimizer optimizer(optctx);
      if (OB_FAIL(optimizer.get_optimization_cost(*temp_stmt, plan_cost))) {
        LOG_WARN("failed to get optimization cost", K(ret));
      } else { /*do nothing*/
      }
      // reset table location
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(temp_stmt->get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null query ctx", K(ret));
        } else {
          temp_stmt->get_query_ctx()->table_partition_infos_.reset();
          temp_stmt->get_query_ctx()->question_marks_count_ = question_mark;
          temp_stmt->get_exec_param_ref_exprs().reset();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransformUtils::free_stmt(*stmt_factory, temp_stmt))) {
        LOG_WARN("failed to free stmt", K(ret));
      } else {
        DESTROY_CONTEXT(mem_context);
      }
    }
  }
  return ret;
}

int ObTransformRule::adjust_transformed_stmt(
    ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt* stmt, ObDMLStmt*& transformed_stmt)
{
  int ret = OB_SUCCESS;
  transformed_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else if (0 == parent_stmts.count()) {
    transformed_stmt = stmt;
  } else {
    ObParentDMLStmt& parent = parent_stmts.at(parent_stmts.count() - 1);
    ObDMLStmt* parent_stmt = parent.stmt_;
    if (OB_ISNULL(parent_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null stmt", K(ret));
    } else if (OB_FAIL(parent_stmt->set_child_stmt(parent.pos_, static_cast<ObSelectStmt*>(stmt)))) {
      LOG_WARN("failed to set child stmt", K(ret));
    } else {
      transformed_stmt = parent_stmts.at(0).stmt_;
    }
  }
  return ret;
}

bool ObTransformRule::need_rewrite(const common::ObIArray<ObParentDMLStmt>& parent_stmts, const ObDMLStmt& stmt)
{
  UNUSED(parent_stmts);
  return !stmt.get_stmt_hint().no_rewrite_;
}

int ObTransformRule::transform_self(common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  bool trans_happened = false;
  stmt_cost_ = -1;
  LOG_TRACE("before transfrom self stmt", KPC(stmt), K(trans_happened_));

  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point error", K(ret), K(stmt), K(ctx_));
  } else if (PRE_PROCESS != transformer_type_ && POST_PROCESS != transformer_type_ &&
             (!need_rewrite(parent_stmts, *stmt) || stmt->is_hierarchical_query() ||
                 (stmt->is_insert_stmt() && static_cast<ObInsertStmt*>(stmt)->is_multi_insert_stmt()))) {

  } else if (OB_FAIL(transform_one_stmt(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to transform own stmt", K(ret));
  } else if (!trans_happened) {
    // do nothing
  } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if (!stmt->is_delete_stmt() && !stmt->is_update_stmt()) {
    // do nothing
  } else if (OB_FAIL(static_cast<ObDelUpdStmt*>(stmt)->update_base_tid_cid())) {
    LOG_WARN("failed to update base tid and cid", K(ret));
  }
  trans_happened_ = (trans_happened_ || trans_happened);
  LOG_TRACE("succeed to transfrom self stmt", KPC(stmt), K(trans_happened_), K(lbt()));
  return ret;
}

// transform non_set children statements
int ObTransformRule::transform_children(
    ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmts failed", K(ret));
  } else { /*do nothing*/
  }
  ObParentDMLStmt parent_stmt;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObDMLStmt* child_stmt = child_stmts.at(i);
    parent_stmt.pos_ = i;
    parent_stmt.stmt_ = stmt;
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child stmt", K(ret));
    } else if (child_stmt->is_eliminated()) {
      /*do nothing*/
    } else if (OB_FAIL(parent_stmts.push_back(parent_stmt))) {
      LOG_WARN("failed to push back parent stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(transform_stmt_recursively(parent_stmts, current_level + 1, child_stmt)))) {
      LOG_WARN("failed to transform stmt recursively", K(ret));
    } else if (OB_FAIL(stmt->set_child_stmt(i, static_cast<ObSelectStmt*>(child_stmt)))) {
      LOG_WARN("failed to set child stmt", K(ret));
    } else {
      parent_stmts.pop_back();
    }
  }
  return ret;
}

bool ObTransformRule::is_view_stmt(const ObIArray<ObParentDMLStmt>& parents, const ObDMLStmt& stmt)
{
  bool bret = false;
  if (parents.empty()) {
    // do nothing
  } else if (OB_ISNULL(parents.at(parents.count() - 1).stmt_)) {
    // do nothing
  } else {
    bret = stmt.get_current_level() == parents.at(parents.count() - 1).stmt_->get_current_level();
  }
  return bret;
}

bool ObTransformRule::is_large_stmt(const ObDMLStmt& stmt)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  int64_t size = 0;
  if (stmt.is_select_stmt()) {
    if (OB_FAIL(static_cast<const ObSelectStmt&>(stmt).get_set_stmt_size(size))) {
      LOG_WARN("failed to get set stm size", K(ret));
    } else if (size > common::OB_MAX_SET_STMT_SIZE) {
      bret = true;
    }
  }
  return bret;
}

} /* namespace sql */
} /* namespace oceanbase */
