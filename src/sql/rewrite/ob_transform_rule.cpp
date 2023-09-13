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
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "observer/ob_server_struct.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_select_stmt_printer.h"
namespace oceanbase
{
namespace sql
{

const ObString ObTransformerCtx::SRC_STR_OR_EXPANSION_WHERE  = "or_expand_where_cond";
const ObString ObTransformerCtx::SRC_STR_OR_EXPANSION_INNER_JOIN  = "or_expand_inner_join";
const ObString ObTransformerCtx::SRC_STR_OR_EXPANSION_OUTER_JOIN  = "or_expand_outer_join";
const ObString ObTransformerCtx::SRC_STR_OR_EXPANSION_SEMI  = "or_expand_semi_anti_join";
const ObString ObTransformerCtx::SRC_STR_CREATE_SIMPLE_VIEW  = "create_simple_view";

bool ObTransformerCtx::is_valid()
{
  return NULL != allocator_ &&
         NULL != session_info_ &&
         NULL != schema_checker_ &&
         NULL != exec_ctx_ &&
         NULL != expr_factory_ &&
         NULL != stmt_factory_ &&
         NULL != sql_schema_guard_ &&
         NULL != sql_schema_guard_->get_schema_guard() &&
         NULL != self_addr_;
}

void ObTransformerCtx::reset()
{
  expr_constraints_.reset();
  happened_cost_based_trans_ = 0;
  equal_sets_.reset();
  ignore_semi_infos_.reset();
  temp_table_ignore_stmts_.reset();
  trans_list_loc_ = 0;
  src_qb_name_.reset();
  src_hash_val_.reset();
  outline_trans_hints_.reset();
  used_trans_hints_.reset();
  groupby_pushdown_stmts_.reset();
  is_spm_outline_ = false;
}

int ObTransformerCtx::add_src_hash_val(const ObString &src_str)
{
  int ret = OB_SUCCESS;
  uint32_t hash_val = src_hash_val_.empty() ? 0 : src_hash_val_.at(src_hash_val_.count() - 1);
  hash_val = fnv_hash2(src_str.ptr(), src_str.length(), hash_val);
  if (OB_FAIL(src_hash_val_.push_back(hash_val))) {
    LOG_WARN("failed to push_back hash val", K(ret));
  }
  return ret;
}

int ObTransformerCtx::add_src_hash_val(uint64_t trans_type)
{
  int ret = OB_SUCCESS;
  const char *str = NULL;
  if (OB_ISNULL(str = get_trans_type_string(trans_type))) {
    LOG_WARN("failed to convert trans type to src value", K(ret));
  } else {
    uint32_t hash_val = src_hash_val_.empty() ? 0 : src_hash_val_.at(src_hash_val_.count() - 1);
    hash_val = fnv_hash2(str, strlen(str), hash_val);
    if (OB_FAIL(src_hash_val_.push_back(hash_val))) {
      LOG_WARN("failed to push_back hash val", K(ret));
    }
  }
  return ret;
}

const char* ObTransformerCtx::get_trans_type_string(uint64_t trans_type)
{
  #define TRANS_TYPE_TO_STR(trans_type) case trans_type: return #trans_type;
  switch(trans_type) {
    TRANS_TYPE_TO_STR(PRE_PROCESS)
    TRANS_TYPE_TO_STR(POST_PROCESS)
    TRANS_TYPE_TO_STR(SIMPLIFY_SUBQUERY)
    TRANS_TYPE_TO_STR(FASTMINMAX)
    TRANS_TYPE_TO_STR(ELIMINATE_OJ)
    TRANS_TYPE_TO_STR(VIEW_MERGE)
    TRANS_TYPE_TO_STR(WHERE_SQ_PULL_UP)
    TRANS_TYPE_TO_STR(QUERY_PUSH_DOWN)
    TRANS_TYPE_TO_STR(AGGR_SUBQUERY)
    TRANS_TYPE_TO_STR(SIMPLIFY_SET)
    TRANS_TYPE_TO_STR(PROJECTION_PRUNING)
    TRANS_TYPE_TO_STR(OR_EXPANSION)
    TRANS_TYPE_TO_STR(WIN_MAGIC)
    TRANS_TYPE_TO_STR(JOIN_ELIMINATION)
    TRANS_TYPE_TO_STR(GROUPBY_PUSHDOWN)
    TRANS_TYPE_TO_STR(GROUPBY_PULLUP)
    TRANS_TYPE_TO_STR(SUBQUERY_COALESCE)
    TRANS_TYPE_TO_STR(PREDICATE_MOVE_AROUND)
    TRANS_TYPE_TO_STR(NL_FULL_OUTER_JOIN)
    TRANS_TYPE_TO_STR(SEMI_TO_INNER)
    TRANS_TYPE_TO_STR(JOIN_LIMIT_PUSHDOWN)
    TRANS_TYPE_TO_STR(TEMP_TABLE_OPTIMIZATION)
    TRANS_TYPE_TO_STR(CONST_PROPAGATE)
    TRANS_TYPE_TO_STR(LEFT_JOIN_TO_ANTI)
    TRANS_TYPE_TO_STR(COUNT_TO_EXISTS)
    TRANS_TYPE_TO_STR(SIMPLIFY_DISTINCT)
    TRANS_TYPE_TO_STR(SIMPLIFY_EXPR)
    TRANS_TYPE_TO_STR(SIMPLIFY_GROUPBY)
    TRANS_TYPE_TO_STR(SIMPLIFY_LIMIT)
    TRANS_TYPE_TO_STR(SIMPLIFY_ORDERBY)
    TRANS_TYPE_TO_STR(SIMPLIFY_WINFUNC)
    TRANS_TYPE_TO_STR(SELECT_EXPR_PULLUP)
    TRANS_TYPE_TO_STR(PROCESS_DBLINK)
    default:  return NULL;
  }
}

int ObTransformRule::transform(ObDMLStmt *&stmt,
                               uint64_t &transform_types)
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
  } else { /*do nothing*/ }
  return ret;
}

int ObTransformRule::adjust_transform_types(uint64_t &transform_types)
{
  int ret = OB_SUCCESS;
  UNUSED(transform_types);
  return ret;
}

int ObTransformRule::transform_stmt_recursively(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                const int64_t current_level,
                                                ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  int64_t size = 0;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(current_level), K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(current_level), K(is_stack_overflow), K(ret));
  } else if (stmt->is_select_stmt() &&
        OB_FAIL(static_cast<const ObSelectStmt *>(stmt)->get_set_stmt_size(size))) {
    LOG_WARN("failed to get set stm size", K(ret));
  } else if (size > common::OB_MAX_SET_STMT_SIZE &&
            !(transformer_type_ == PRE_PROCESS ||
            transformer_type_ == POST_PROCESS)) {
    // skip transformation for large stmt
    // mainly for union stmt, select union select union select union ... union select
  } else if (TransMethod::POST_ORDER == transform_method_) {
    if (OB_FAIL(transform_post_order(parent_stmts, current_level, stmt))) {
      LOG_WARN("failed to do top down transformation", K(*stmt), K(current_level), K(ret));
    } else { /*do nothing*/ }
  } else if (TransMethod::PRE_ORDER == transform_method_) {
    if (OB_FAIL(transform_pre_order(parent_stmts, current_level, stmt))) {
      LOG_WARN("failed to do top down transformation", K(*stmt), K(current_level), K(ret));
    } else { /*do nothing*/ }
  } else if (TransMethod::ROOT_ONLY == transform_method_) {
    if (OB_FAIL(transform_root_only(parent_stmts, current_level, stmt))) {
      LOG_WARN("failed to do root only transformation", K(*stmt), K(current_level), K(ret));
    } else { /*do nothing*/ }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported transformation method", K(ret));
  }
  return ret;
}

// pre-order transformation
int ObTransformRule::transform_pre_order(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         ObDMLStmt *&stmt)
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
  } else if (OB_FAIL(transform_self(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform self statement", K(ret));
  } else if (OB_FAIL(transform_children(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform children stmt", K(ret));
  }
  return ret;
}

// post-order transformation
int ObTransformRule::transform_post_order(ObIArray<ObParentDMLStmt> &parent_stmts,
                                          const int64_t current_level,
                                          ObDMLStmt *&stmt)
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
  } else if (OB_FAIL(transform_self(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform self statement", K(ret));
  }
  return ret;
}

// root-only transformation
int ObTransformRule::transform_root_only(ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         ObDMLStmt *&stmt)
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
  } else if (OB_FAIL(transform_self(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform self statement", K(ret));
  } else if (OB_FAIL(transform_temp_tables(parent_stmts, current_level, stmt))) {
    LOG_WARN("failed to transform children stmt", K(ret));
  }
  return ret;
}

/**
 * @brief ObCostBasedRewriteRule::accept_transform
 * @param stmts: origin stmt
 * @param trans_stmts: transformed stmt
 * @param trans_happened
 * check whether accept transformed stmt:
 *  1. force accept due to hint/rule;
 *  2. the transformed stmt has less cost than the origin stmt.
 * @return
 */
int ObTransformRule::accept_transform(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                      ObDMLStmt *&stmt,
                                      ObDMLStmt *trans_stmt,
                                      bool force_accept,
                                      bool check_original_plan,
                                      bool &trans_happened,
                                      void *check_ctx /* = NULL*/)
{
  int ret = OB_SUCCESS;
  double trans_stmt_cost = 0.0;
  trans_happened = false;
  ObDMLStmt *top_stmt = parent_stmts.empty() ? stmt : parent_stmts.at(0).stmt_;
  bool is_expected = false;
  bool is_original_expected = false;
  ObDMLStmt *tmp1 = NULL;
  ObDMLStmt *tmp2 = NULL;
  cost_based_trans_tried_ = true;
  STOP_OPT_TRACE;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(trans_stmt) || OB_ISNULL(top_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context is null", K(ret), K(ctx_), K(stmt), K(trans_stmt), K(top_stmt));
  } else if (force_accept) {
    trans_happened = true;
  } else if (ctx_->is_set_stmt_oversize_) {
    LOG_TRACE("not accept transform because large set stmt", K(ctx_->is_set_stmt_oversize_));
  } else if (OB_FAIL(evaluate_cost(parent_stmts, trans_stmt, true,
                                   trans_stmt_cost, is_expected, check_ctx))) {
    LOG_WARN("failed to evaluate cost for the transformed stmt", K(ret));
  } else if ((!check_original_plan && stmt_cost_ >= 0) || !is_expected) {
    trans_happened = is_expected && trans_stmt_cost < stmt_cost_;
  } else if (OB_FAIL(evaluate_cost(parent_stmts, stmt, false,
                                   stmt_cost_, is_original_expected,
                                   check_original_plan ? check_ctx : NULL))) {
    LOG_WARN("failed to evaluate cost for the origin stmt", K(ret));
  } else if (!is_original_expected) {
    trans_happened = is_original_expected;
  } else {
    trans_happened = trans_stmt_cost < stmt_cost_;
  }
  RESUME_OPT_TRACE;

  if (OB_FAIL(ret)) {
  } else if (!trans_happened) {
    OPT_TRACE("reject transform because the cost is increased or the query plan is unexpected.");
    OPT_TRACE("before transform cost:", stmt_cost_);
    OPT_TRACE("after transform cost:", trans_stmt_cost);
    OPT_TRACE("is expected plan:", is_expected);
    OPT_TRACE("is expected original plan:", is_original_expected);
    LOG_TRACE("reject transform because the cost is increased or the query plan is unexpected",
                     K_(ctx_->is_set_stmt_oversize), K_(stmt_cost), K(trans_stmt_cost), K(is_expected));
  } else if (OB_FAIL(adjust_transformed_stmt(parent_stmts, trans_stmt, tmp1, tmp2))) {
    LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (force_accept) {
    LOG_TRACE("succeed force accept transform because hint/rule");
    stmt = trans_stmt;
    OPT_TRACE("hint or rule force cost based transform apply.");
  } else {
    OPT_TRACE("accept transform because the cost is decreased.");
    OPT_TRACE("before transform cost:", stmt_cost_);
    OPT_TRACE("after transform cost:", trans_stmt_cost);
    LOG_TRACE("accept transform because the cost is decreased",
              K_(stmt_cost), K(trans_stmt_cost));
    stmt = trans_stmt;
    stmt_cost_ = trans_stmt_cost;
  }
  return ret;
}

int ObTransformRule::is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid)
{
  UNUSED(plan);
  UNUSED(check_ctx);
  UNUSED(is_trans_plan);
  is_valid = false;
  return OB_SUCCESS;
}

int ObTransformRule::evaluate_cost(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                   ObDMLStmt *&stmt,
                                   bool is_trans_stmt,
                                   double &plan_cost,
                                   bool &is_expected,
                                   void *check_ctx /* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObEvalCostHelper eval_cost_helper;
  is_expected = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid())
      || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())
      || OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory())
      || OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(eval_cost_helper.fill_helper(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                  *stmt->get_query_ctx(), *ctx_))) {
    LOG_WARN("failed to fill eval cost helper", K(ret));
  } else {
    ctx_->eval_cost_ = true;
    ParamStore &param_store = ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store_for_update();
    ObDMLStmt *root_stmt = NULL;
    lib::ContextParam param;
    ObTransformerImpl trans(ctx_);
    param.set_mem_attr(ctx_->session_info_->get_effective_tenant_id(),
                       "CostBasedRewrit",
                       ObCtxIds::DEFAULT_CTX_ID)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL)
       .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(prepare_eval_cost_stmt(parent_stmts, *stmt, root_stmt, is_trans_stmt))) {
      LOG_WARN("failed to prepare eval cost stmt", K(ret));
    } else if (OB_FAIL(trans.transform_heuristic_rule(reinterpret_cast<ObDMLStmt*&>(root_stmt)))) {
      LOG_WARN("failed to transform heuristic rule", K(ret));
    } else {
      LOG_DEBUG("get transformed heuristic rule stmt when evaluate_cost", K(*root_stmt));
      CREATE_WITH_TEMP_CONTEXT(param) {
        ObRawExprFactory tmp_expr_factory(CURRENT_CONTEXT->get_arena_allocator());
        HEAP_VAR(ObOptimizerContext, optctx,
                ctx_->session_info_,
                ctx_->exec_ctx_,
                ctx_->sql_schema_guard_,
                ctx_->opt_stat_mgr_,
                CURRENT_CONTEXT->get_arena_allocator(),
                &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                *ctx_->self_addr_,
                GCTX.srv_rpc_proxy_,
                stmt->get_query_ctx()->get_global_hint(),
                tmp_expr_factory,
                root_stmt,
                false,
                ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx()) {
          //optctx.set_only_ds_basic_stat(true);
          ObOptimizer optimizer(optctx);
          ObLogPlan *plan = NULL;
          if (OB_FAIL(optimizer.get_optimization_cost(*root_stmt, plan, plan_cost))) {
            LOG_WARN("failed to get optimization cost", K(ret));
          } else if (NULL == check_ctx) {
            // do nothing
          } else if (OB_FAIL(is_expected_plan(plan, check_ctx, is_trans_stmt, is_expected))) {
            LOG_WARN("failed to check transformed plan", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(eval_cost_helper.recover_context(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                      *ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx(),
                                                      *ctx_))) {
            LOG_WARN("failed to recover context", K(ret));
          } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, root_stmt))) {
            LOG_WARN("failed to free stmt", K(ret));
          }
        }
      }
    }
  }
  return ret;
}


int ObTransformRule::prepare_root_stmt_with_temp_table_filter(ObDMLStmt &root_stmt, ObDMLStmt *&root_stmt_with_filter)
{
  int ret = OB_SUCCESS;
  ObSqlTempTableInfo temp_table_info;
  ObRawExpr *temp_table_filter = NULL;
  ObSelectStmt *sel_root_stmt = static_cast<ObSelectStmt *>(&root_stmt);
  bool have_temp_table_filter = false;
  if (NULL == current_temp_table_) {
    root_stmt_with_filter = &root_stmt;
  } else if (OB_UNLIKELY(!root_stmt.is_select_stmt()) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
            OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(root_stmt));
  } else if (OB_FAIL(ObSqlTempTableInfo::collect_specified_temp_table(*ctx_->allocator_,
                                                                       current_temp_table_->temp_table_query_,
                                                                       current_temp_table_->upper_stmts_,
                                                                       current_temp_table_->table_items_,
                                                                       temp_table_info,
                                                                       have_temp_table_filter))) {
    LOG_WARN("failed to collect_specified_temp_table", K(ret));
  } else if (have_temp_table_filter) {
    TableItem *new_table_item = NULL;
    ObSEArray<ObRawExpr *, 8> column_exprs;
    root_stmt_with_filter = NULL;
    ObSelectStmt *sel_stmt_with_filter = NULL;
    if (OB_FAIL(ctx_->stmt_factory_->create_stmt(sel_stmt_with_filter))) {
      LOG_WARN("failed to create stmt", K(ret));
    } else if (OB_ISNULL(sel_stmt_with_filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FALSE_IT(sel_stmt_with_filter->set_query_ctx(sel_root_stmt->get_query_ctx()))) {
    } else if (OB_FAIL(sel_stmt_with_filter->adjust_statement_id(ctx_->allocator_,
                                                                 ctx_->src_qb_name_,
                                                                 ctx_->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, sel_stmt_with_filter, sel_root_stmt, new_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(new_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(sel_stmt_with_filter->add_from_item(new_table_item->table_id_, false))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(sel_stmt_with_filter->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuid table hash", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                          *new_table_item,
                                                          sel_stmt_with_filter,
                                                          column_exprs))) {
      LOG_WARN("failed to create column items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            column_exprs,
                                                            sel_stmt_with_filter))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(sel_stmt_with_filter->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::push_down_temp_table_filter(*ctx_->expr_factory_,
                                                                    ctx_->session_info_,
                                                                    temp_table_info,
                                                                    temp_table_filter,
                                                                    sel_stmt_with_filter))) {
      LOG_WARN("failed to push down filter for temp table", K(ret));
    } else if (OB_ISNULL(temp_table_filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter is null", K(temp_table_info));
    } else if (OB_FAIL(sel_stmt_with_filter->add_condition_expr(temp_table_filter))) {
      LOG_WARN("failed to add condition expr", K(ret));
    } else {
      root_stmt_with_filter = sel_stmt_with_filter;
    }
  } else {
    root_stmt_with_filter = &root_stmt;
  }
  return ret;
}

int ObTransformRule::prepare_eval_cost_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                            ObDMLStmt &stmt,
                                            ObDMLStmt *&copied_stmt,
                                            bool is_trans_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 2> old_temp_table_stmts;
  ObSEArray<ObSelectStmt*, 2> new_temp_table_stmts;
  ObDMLStmt *root_stmt = NULL;
  ObDMLStmt *root_stmt_with_filter = NULL;
  ObDMLStmt *copied_trans_stmt = NULL;
  ObString cur_qb_name;
  ObDMLStmt *orig_stmt = NULL;
  ObDMLStmt *temp = NULL;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(stmt.get_query_ctx()));
  } else if (OB_FAIL(adjust_transformed_stmt(parent_stmts, &stmt, orig_stmt, root_stmt))) {
    LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (OB_FAIL(prepare_root_stmt_with_temp_table_filter(*root_stmt, root_stmt_with_filter))) {
    LOG_WARN("failed to prepare root stmt with temp table filter", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      root_stmt_with_filter, copied_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(deep_copy_temp_table(*copied_stmt,
                                          *ctx_->stmt_factory_,
                                          *ctx_->expr_factory_,
                                          old_temp_table_stmts,
                                          new_temp_table_stmts))) {
    LOG_WARN("failed to deep copy temp table", K(ret));
  } else if (NULL != orig_stmt && &stmt != orig_stmt // reset stmt
             && OB_FAIL(adjust_transformed_stmt(parent_stmts, orig_stmt, temp, root_stmt))) {
      LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (!is_trans_stmt) {
    /* do nothing */
  } else if (OB_FAIL(stmt.get_qb_name(cur_qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (OB_FAIL(ObTransformRule::construct_transform_hint(stmt, NULL))) {
    // To get happended transform rule by outline_trans_hints_ during evaluating cost,
    // here construct and add hint for cost based transform rule.
    // Added hint only filled qb name parameter.
    LOG_WARN("failed to construct transform hint", K(ret), K(stmt.get_stmt_id()),
                                                  K(ctx_->src_qb_name_), K(get_transformer_type()));
  } else if (skip_adjust_qb_name() || cur_qb_name != ctx_->src_qb_name_) {
    ctx_->src_qb_name_ = cur_qb_name;
  } else if (OB_FAIL(copied_stmt->get_stmt_by_stmt_id(stmt.get_stmt_id(), copied_trans_stmt))) {
    LOG_WARN("failed to get stmt by stmt id", K(ret), K(stmt.get_stmt_id()), K(*copied_stmt));
  } else if(OB_ISNULL(copied_trans_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret), K(stmt.get_stmt_id()), K(*copied_stmt));
  } else if (OB_FAIL(copied_trans_stmt->adjust_qb_name(ctx_->allocator_,
                                                       ctx_->src_qb_name_,
                                                       ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copied_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(copied_stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

// deep copy and replace temp table in stmt
// only used for deep copy temp table stmt before evaluate cost
int ObTransformRule::deep_copy_temp_table(ObDMLStmt &stmt,
                                          ObStmtFactory &stmt_factory,
                                          ObRawExprFactory &expr_factory,
                                          ObIArray<ObSelectStmt*> &old_temp_table_stmts,
                                          ObIArray<ObSelectStmt*> &new_temp_table_stmts)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *temp_query = NULL;
  ObIArray<TableItem*> &tables = stmt.get_table_items();
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
    if (OB_ISNULL(tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!tables.at(i)->is_temp_table()) {
      /* do nothing */
    } else if (OB_ISNULL(tables.at(i)->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(old_temp_table_stmts,
                                               tables.at(i)->ref_query_, &idx))) {
      LOG_WARN("failed to get temp table plan", K(ret));
    } else if (old_temp_table_stmts.count() == new_temp_table_stmts.count()) {
      // temp table stmt has added
      if (OB_UNLIKELY(idx < 0 || idx >= new_temp_table_stmts.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected idx", K(ret), K(idx), K(new_temp_table_stmts.count()));
      } else {
        tables.at(i)->ref_query_ = new_temp_table_stmts.at(idx);
      }
    } else if (OB_FAIL(stmt_factory.create_stmt(temp_query))) {
      LOG_WARN("failed to create select stmt", K(ret));
    } else if(OB_ISNULL(temp_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(temp_query->deep_copy(stmt_factory, expr_factory,
                                             *tables.at(i)->ref_query_))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (OB_FAIL(new_temp_table_stmts.push_back(temp_query))) {
      LOG_WARN("failed to push back stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(deep_copy_temp_table(*temp_query,
                                                       stmt_factory,
                                                       expr_factory,
                                                       old_temp_table_stmts,
                                                       new_temp_table_stmts)))) {
      LOG_WARN("failed to deep copy temp table", K(ret));
    } else {
      tables.at(i)->ref_query_ = temp_query;
    }
  }
  if (OB_SUCC(ret)) { // deep copy and replace in child stmt
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(deep_copy_temp_table(*child_stmts.at(i),
                                                         stmt_factory,
                                                         expr_factory,
                                                         old_temp_table_stmts,
                                                         new_temp_table_stmts)))) {
        LOG_WARN("failed to deep copy temp table", K(ret));
      }
    }
  }
  return ret;
}

// replace orgin_stmt by stmt, get root_stmt
int ObTransformRule::adjust_transformed_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *stmt,
                                             ObDMLStmt *&orgin_stmt,
                                             ObDMLStmt *&root_stmt)
{
  int ret = OB_SUCCESS;
  root_stmt = NULL;
  orgin_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else if (0 == parent_stmts.count()) {
    root_stmt = stmt;
  } else {
    ObParentDMLStmt &parent = parent_stmts.at(parent_stmts.count() - 1);
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    ObDMLStmt *parent_stmt = NULL;
    if (OB_ISNULL(parent_stmt = parent.stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null stmt", K(ret));
    } else if (OB_FAIL(parent_stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    } else if (OB_UNLIKELY(child_stmts.count() <= parent.pos_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos", K(ret), K(child_stmts.count()), K(parent.pos_));
    } else if (OB_FAIL(parent_stmt->set_child_stmt(parent.pos_, static_cast<ObSelectStmt*>(stmt)))) {
      LOG_WARN("failed to set child stmt", K(ret));
    } else {
      orgin_stmt = child_stmts.at(parent.pos_);
      root_stmt = parent_stmts.at(0).stmt_;
    }
  }
  return ret;
}

bool ObTransformRule::is_normal_disabled_transform(const ObDMLStmt &stmt)
{
  return (stmt.is_hierarchical_query() && transform_method_ != TransMethod::ROOT_ONLY) ||
         stmt.is_insert_all_stmt() ||
         stmt.is_values_table_query();
}

int ObTransformRule::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                    const int64_t current_level,
                                    const ObDMLStmt &stmt,
                                    bool &need_trans)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  UNUSED(current_level);
  need_trans = false;
  if (is_normal_disabled_transform(stmt)) {
    need_trans = false;
    OPT_TRACE("hierarchical query or insert query can not transform");
  } else if (stmt.has_instead_of_trigger()) {
    need_trans = false;
    OPT_TRACE("stmt with instead of trigger can not transform");
  } else if (OB_FAIL(check_hint_status(stmt, need_trans))) {
    LOG_WARN("failed to check hint status", K(ret));
  }
  return ret;
}

int ObTransformRule::transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                     ObDMLStmt *&stmt,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(transform_one_stmt(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to transform one stmt for or expansion", K(ret));
  } else if (trans_happened && !skip_move_trans_loc()) {
    ++ctx_->trans_list_loc_;
  }
  return ret;
}

int ObTransformRule::transform_self(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                    const int64_t current_level,
                                    ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  bool trans_happened = false;
  stmt_cost_ = -1;
  int64_t src_hash_val_cnt = -1;
  bool need_trans = false;
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = stmt->get_stmt_hint().query_hint_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point error", K(ret), K(stmt), K(ctx_), K(query_hint));
  } else if (stmt->is_select_stmt() &&
             static_cast<ObSelectStmt*>(stmt)->get_select_item_size() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find select stmt with 0 select item", K(ret), KPC(stmt));
  } else if (OB_FAIL(stmt->get_qb_name(ctx_->src_qb_name_))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else {
    OPT_TRACE("transform query block:", ctx_->src_qb_name_);
    OPT_TRACE_TRANSFORM_SQL(stmt);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(need_transform(parent_stmts, current_level, *stmt, need_trans))) {
    LOG_WARN("failed to check need rewrite", K(ret));
  } else if (!need_trans) {
    // do nothing
  } else if (OB_FALSE_IT(src_hash_val_cnt = ctx_->src_hash_val_.count())) {
  } else if (OB_FAIL(ctx_->add_src_hash_val(transformer_type_))) {
    LOG_WARN("failed to add trans rule src hash val", K(ret));
  } else if (query_hint->has_outline_data()) {
    if (OB_FAIL(transform_one_stmt_with_outline(parent_stmts, stmt, trans_happened))) {
      LOG_WARN("failed to transform one stmt with outline", K(ret));
    }
  } else if (OB_FAIL(transform_one_stmt(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to transform own stmt", K(ret));
  }

  if (OB_FAIL(ret) || !need_trans) {
  } else if (OB_FALSE_IT(ctx_->src_hash_val_.pop_back())) {
  } else if (OB_UNLIKELY(ctx_->src_hash_val_.count() != src_hash_val_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected src hash val count after transform", K(ret));
  } else if (!trans_happened) {
    // do nothing
  } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if ((!stmt->is_delete_stmt() && !stmt->is_update_stmt())
              || stmt->has_instead_of_trigger()) {
    // do nothing
  } else if (OB_FAIL(static_cast<ObDelUpdStmt *>(stmt)->update_base_tid_cid())) {
    LOG_WARN("failed to update base tid and cid", K(ret));
  }
  trans_happened_ = (trans_happened_ || trans_happened);
  OPT_TRACE("transform happened:", trans_happened);
  if (trans_happened) {
    OPT_TRACE(stmt);
  }
  // the following code print too much logs. Adjust the log level as debug
  LOG_DEBUG("succeed to transfrom self stmt",
            "trans_type", ctx_->get_trans_type_string(get_transformer_type()),
            KPC(stmt), K(trans_happened_));
  return ret;
}

// transform non_set children statements
int ObTransformRule::transform_children(ObIArray<ObParentDMLStmt> &parent_stmts,
                                        const int64_t current_level,
                                        ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  ObParentDMLStmt parent_stmt;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmts failed", K(ret));
  } else { /*do nothing*/ }
  //transform temp table for root stmt
  //disable temp table transform for temp table
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_temp_tables(parent_stmts, current_level, stmt))) {
      LOG_WARN("failed to transform temp tables", K(ret));
    }
  }
  if (!child_stmts.empty()) {
    OPT_TRACE_BEGIN_SECTION;
  }
  //transform child stmt
  for(int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObDMLStmt *child_stmt = child_stmts.at(i);
    parent_stmt.pos_ = i;
    parent_stmt.stmt_ = stmt;
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child stmt", K(ret));
    } else if (OB_FAIL(parent_stmts.push_back(parent_stmt))) {
      LOG_WARN("failed to push back parent stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(transform_stmt_recursively(parent_stmts,
                                                             current_level + 1,
                                                             child_stmt)))) {
      LOG_WARN("failed to transform stmt recursively", K(ret));
    } else if (OB_FAIL(stmt->set_child_stmt(i, static_cast<ObSelectStmt*>(child_stmt)))) {
      LOG_WARN("failed to set child stmt", K(ret));
    } else {
      parent_stmts.pop_back();
    }
  }
  if (!child_stmts.empty()) {
    OPT_TRACE_END_SECTION;
  }
  return ret;
}

int ObTransformRule::transform_temp_tables(ObIArray<ObParentDMLStmt> &parent_stmts,
                                           const int64_t current_level,
                                           ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::TempTableInfo, 8> temp_table_infos;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (0 == current_level) {
    if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
      LOG_WARN("failed to collect temp table infos", K(ret));
    }
    if (!temp_table_infos.empty()) {
      OPT_TRACE("start transform temp table stmt");
    }
    for(int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); ++i) {
      ObDMLStmt *child_stmt = temp_table_infos.at(i).temp_table_query_;
      current_temp_table_ = &temp_table_infos.at(i);
      if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(transform_stmt_recursively(parent_stmts,
                                                              current_level + 1,
                                                              child_stmt)))) {
        LOG_WARN("failed to transform stmt recursively", K(ret));
      }
      current_temp_table_ = NULL;
      //reset ref query
      for (int64_t j = 0; OB_SUCC(ret) && j < temp_table_infos.at(i).table_items_.count(); ++j) {
        TableItem *temp_table = temp_table_infos.at(i).table_items_.at(j);
        if (OB_ISNULL(temp_table) || !temp_table->is_temp_table()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect temp table item", KPC(temp_table), K(ret));
        } else {
          temp_table->ref_query_ = static_cast<ObSelectStmt*>(child_stmt);
        }
      }
    }
    if (!temp_table_infos.empty()) {
      OPT_TRACE("end transform temp table stmt");
    }
  }
  return ret;
}


int ObTryTransHelper::fill_helper(const ObQueryCtx *query_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query context", K(ret), K(query_ctx));
  } else if (OB_FAIL(query_ctx->query_hint_.get_qb_name_counts(query_ctx->stmt_count_, qb_name_counts_))) {
    LOG_WARN("failed to get qb name counts", K(ret));
  } else {
    available_tb_id_ = query_ctx->available_tb_id_;
    subquery_count_ = query_ctx->subquery_count_;
    temp_table_count_ = query_ctx->temp_table_count_;
  }
  return ret;
}

int ObTryTransHelper::recover(ObQueryCtx *query_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query context", K(ret), K(query_ctx));
  } else if (OB_FAIL(query_ctx->query_hint_.recover_qb_names(qb_name_counts_, query_ctx->stmt_count_))) {
    LOG_WARN("failed to revover qb names", K(ret));
  } else {
    query_ctx->available_tb_id_ = available_tb_id_;
    query_ctx->subquery_count_ = subquery_count_;
    query_ctx->temp_table_count_ = temp_table_count_;
  }
  return ret;
}

int ObEvalCostHelper::fill_helper(const ObPhysicalPlanCtx &phy_plan_ctx,
                                  const ObQueryCtx &query_ctx,
                                  const ObTransformerCtx &trans_ctx)
{
  int ret = OB_SUCCESS;

  // query context
  question_marks_count_ = query_ctx.question_marks_count_;
  calculable_items_count_ = query_ctx.calculable_items_.count();
  if (OB_FAIL(try_trans_helper_.fill_helper(&query_ctx))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  }

  // transform context
  if (OB_SUCC(ret)) {
    eval_cost_ = trans_ctx.eval_cost_;
    expr_constraints_count_ = trans_ctx.expr_constraints_.count();
    plan_const_param_constraints_count_ = trans_ctx.plan_const_param_constraints_.count();
    equal_param_constraints_count_ = trans_ctx.equal_param_constraints_.count();
    src_qb_name_ = trans_ctx.src_qb_name_;
    outline_trans_hints_count_ = trans_ctx.outline_trans_hints_.count();
    used_trans_hints_count_ = trans_ctx.used_trans_hints_.count();
  }
  return ret;
}

int ObEvalCostHelper::recover_context(ObPhysicalPlanCtx &phy_plan_ctx,
                                      ObQueryCtx &query_ctx,
                                      ObTransformerCtx &trans_ctx)
{
  int ret = OB_SUCCESS;
  
  // query context
  query_ctx.question_marks_count_ = phy_plan_ctx.get_param_store().count();
  if (OB_FAIL(try_trans_helper_.recover(&query_ctx))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  }

  // transform context
  if (OB_SUCC(ret)) {
    trans_ctx.eval_cost_ = eval_cost_;
    trans_ctx.src_qb_name_ = src_qb_name_;
    ObOptimizerUtil::revert_items(trans_ctx.expr_constraints_, expr_constraints_count_);
    ObOptimizerUtil::revert_items(trans_ctx.plan_const_param_constraints_,
                                  plan_const_param_constraints_count_);
    ObOptimizerUtil::revert_items(trans_ctx.equal_param_constraints_, equal_param_constraints_count_);
    ObOptimizerUtil::revert_items(trans_ctx.outline_trans_hints_, outline_trans_hints_count_);
    ObOptimizerUtil::revert_items(trans_ctx.used_trans_hints_, used_trans_hints_count_);
  }
  return ret;
}

int ObTransformRule::add_transform_hint(ObDMLStmt &trans_stmt, void *trans_params /* = NULL*/)
{
  int ret = OB_SUCCESS;
  int64_t src_hash_val_cnt = -1;
  ObString qb_name;
  if (OB_ISNULL(ctx_) || OB_ISNULL(trans_stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_stmt.get_query_ctx()));
  } else if (OB_FALSE_IT(src_hash_val_cnt = ctx_->src_hash_val_.count())) {
  } else if (OB_FAIL(construct_transform_hint(trans_stmt, trans_params))) {
    LOG_WARN("failed to construct transform hint", K(ret), K(trans_stmt.get_stmt_id()),
                                                  K(ctx_->src_qb_name_), K(get_transformer_type()));
  } else if (OB_FAIL(trans_stmt.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (skip_adjust_qb_name() || qb_name != ctx_->src_qb_name_) {
    ctx_->src_qb_name_ = qb_name;
  } else if (OB_FAIL(trans_stmt.adjust_qb_name(ctx_->allocator_,
                                               ctx_->src_qb_name_,
                                               ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else if (OB_FAIL(trans_stmt.get_qb_name(ctx_->src_qb_name_))) {
    LOG_WARN("failed to get qb name", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObOptimizerUtil::revert_items(ctx_->src_hash_val_, src_hash_val_cnt);
  }
  return ret;
}

int ObTransformRule::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObTransHint *hint = NULL;
  ObString qb_name;
  UNUSED(trans_params);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used hint", K(ret));
  } else if (get_hint_type() == T_INVALID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hint type", K(ret), K(stmt.get_stmt_id()), K(ctx_->src_qb_name_), K(get_transformer_type()));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, get_hint_type(), hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
  }
  return ret;
}

int ObTransformRule::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = NULL;
  const ObHint *myhint = get_hint(stmt.get_stmt_hint());
  bool is_enable = (NULL != myhint && myhint->is_enable_hint());
  bool is_disable = (NULL != myhint && myhint->is_disable_hint());
  need_trans = false;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    const ObHint *no_rewrite_hint = stmt.get_stmt_hint().get_no_rewrite_hint();
    if (is_enable) {
      need_trans = true;
    } else if (NULL != no_rewrite_hint || is_disable) {
      if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite_hint))) {
        LOG_WARN("failed to add used transform hint", K(ret));
      } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used transform hint", K(ret));
      }
      OPT_TRACE("hint reject current transform");
    } else if ((ALL_COST_BASED_RULES & (1L << get_transformer_type())) &&
               query_hint->global_hint_.disable_cost_based_transform()) {
      /* disable transform by NO_COST_BASED_QUERY_TRANSFORMATION hint */
    } else {
      need_trans = true;
    }
  } else if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint)) {
    need_trans = true;
  } else {
    OPT_TRACE("outline reject current transform");
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
