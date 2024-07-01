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

#include "sql/rewrite/ob_transformer_impl.h"
#include "common/ob_common_utility.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_view_merge.h"
#include "sql/rewrite/ob_transform_min_max.h"
#include "sql/rewrite/ob_transform_where_subquery_pullup.h"
#include "sql/rewrite/ob_transform_eliminate_outer_join.h"
#include "sql/rewrite/ob_transform_simplify_distinct.h"
#include "sql/rewrite/ob_transform_simplify_expr.h"
#include "sql/rewrite/ob_transform_simplify_groupby.h"
#include "sql/rewrite/ob_transform_simplify_subquery.h"
#include "sql/rewrite/ob_transform_simplify_winfunc.h"
#include "sql/rewrite/ob_transform_simplify_orderby.h"
#include "sql/rewrite/ob_transform_simplify_limit.h"
#include "sql/rewrite/ob_transform_query_push_down.h"
#include "sql/rewrite/ob_transform_aggr_subquery.h"
#include "sql/rewrite/ob_transform_simplify_set.h"
#include "sql/rewrite/ob_transform_project_pruning.h"
#include "sql/rewrite/ob_transform_or_expansion.h"
#include "sql/rewrite/ob_transform_join_elimination.h"
#include "sql/rewrite/ob_transform_win_magic.h"
#include "sql/rewrite/ob_transform_groupby_pullup.h"
#include "sql/rewrite/ob_transform_groupby_pushdown.h"
#include "sql/rewrite/ob_transform_subquery_coalesce.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/rewrite/ob_transform_predicate_move_around.h"
#include "sql/rewrite/ob_transform_semi_to_inner.h"
#include "sql/rewrite/ob_transform_join_limit_pushdown.h"
#include "sql/rewrite/ob_transform_temp_table.h"
#include "sql/rewrite/ob_transform_const_propagate.h"
#include "sql/rewrite/ob_transform_left_join_to_anti.h"
#include "sql/rewrite/ob_transform_count_to_exists.h"
#include "sql/rewrite/ob_transform_expr_pullup.h"
#include "sql/rewrite/ob_transform_dblink.h"
#include "sql/rewrite/ob_transform_conditional_aggr_coalesce.h"
#include "sql/rewrite/ob_transform_mv_rewrite.h"
#include "sql/rewrite/ob_transform_decorrelate.h"
#include "common/ob_smart_call.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{
int ObTransformerImpl::transform(ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  bool trans_happended = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(do_transform_dblink_write(stmt, trans_happended))) {
    LOG_WARN("failed to do transform dblink write", K(ret));
  } else if (trans_happended) {
    //dml write query will be executed in remote, do not need transform
  } else if (OB_FAIL(SMART_CALL(get_stmt_trans_info(stmt, true)))) {
    LOG_WARN("get_stmt_trans_info failed", K(ret));
  } else if (OB_FAIL(do_transform_pre_precessing(stmt))) {
    LOG_WARN("failed to do transform pre_precessing", K(ret));
  } else if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
    LOG_WARN("failed to formalize query ref exprs");
  } else if (OB_FAIL(stmt->formalize_stmt_expr_reference(ctx_->expr_factory_, ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if (OB_FAIL(do_transform(stmt))) {
    LOG_WARN("failed to do transform", K(ret));
  } else if (OB_FAIL(do_transform_dblink_read(stmt))) {
    LOG_WARN("failed to do transform dblink read", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt_expr_reference(ctx_->expr_factory_, ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if (OB_FAIL(do_after_transform(stmt))) {
    LOG_WARN("failed deal after transform", K(ret));
  } else {
    print_trans_stat();
  }
  return ret;
}

int ObTransformerImpl::get_stmt_trans_info(ObDMLStmt *stmt, bool is_root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_contain_oversize_set_stmt(stmt, ctx_->is_set_stmt_oversize_))) {
    LOG_WARN("failed to check set stmt oversize");
  }
  if (OB_SUCC(ret) && !ctx_->is_set_stmt_oversize_ && is_root) {
    ObArray<ObDMLStmt::TempTableInfo> temp_table_infos;
    if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
      LOG_WARN("failed to collect temp table infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !ctx_->is_set_stmt_oversize_ && i < temp_table_infos.count(); i++) {
      if (OB_FAIL(SMART_CALL(ObTransformUtils::check_stmt_contain_oversize_set_stmt(temp_table_infos.at(i).temp_table_query_,
                                                                                    ctx_->is_set_stmt_oversize_)))) {
        LOG_WARN("check_contain_oversize_set_stmt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformerImpl::get_random_order_array(uint64_t need_types,
                                              ObQueryCtx *query_ctx,
                                              ObArray<uint64_t> &need_types_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(query_ctx));
  } else {
    common::ObArray<int> index_array;
    for (int64_t i = POST_PROCESS + 1; i < TRANSFORM_TYPE_COUNT_PLUS_ONE && OB_SUCC(ret); ++i) {
      if ((need_types & (1 << i)) != 0) {
        if (OB_FAIL(index_array.push_back(i))) {
          LOG_WARN("failed to push back index", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t idx = 1; idx < index_array.count(); ++idx) {
        int64_t rand_pos = query_ctx->rand_gen_.get(0, idx);
        if (rand_pos != idx) {
          std::swap(index_array.at(rand_pos), index_array.at(idx));
        }
      }

      LOG_TRACE("the random order will be ",K(index_array));
      for (int64_t i = 0; i < index_array.count() && OB_SUCC(ret); ++i) {
        int need_types_pos = index_array.at(i);
        uint64_t need_types_local = 1u << need_types_pos;
        if (OB_FAIL(need_types_array.push_back(need_types_local))) {
          LOG_WARN("failed to push back need types array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformerImpl::transform_random_order(ObDMLStmt *&stmt, ObQueryCtx *query_ctx, uint64_t need_types, int iter_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(query_ctx));
  } else {
    ObArray<uint64_t> need_types_array;
    if (OB_FAIL(get_random_order_array(need_types, query_ctx, need_types_array))) {
      LOG_WARN("failed to get random order array", K(ret));
    } else {
      bool any_trans_happened = true;
      for (int64_t i = 0; i < iter_count && OB_SUCC(ret) && any_trans_happened; ++i) {
        any_trans_happened = false;
        for (int64_t j = 0; j < need_types_array.count() && OB_SUCC(ret); ++j) {
          bool trans_happened = false;
          if (OB_FAIL(transform_rule_set(stmt, need_types_array.at(j), 1, trans_happened))) {
            LOG_WARN("failed to transform one rule set", K(ret));
          } else {
            any_trans_happened = any_trans_happened || trans_happened;
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformerImpl::do_transform(ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  uint64_t need_types = ObTransformRule::ALL_TRANSFORM_RULES;
  bool transformation_enabled = true;
  ObQueryCtx *query_ctx = NULL;
  int iter_count = max_iteration_count_;
  bool is_outline = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(query_ctx = stmt->get_query_ctx())
      || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(query_ctx), K(ctx_),
                                    K(ctx_->session_info_), K(ret));
  } else if (OB_FAIL(ctx_->session_info_->is_transformation_enabled(transformation_enabled))) {
    LOG_WARN("fail to get transformation_enabled", K(ret));
  } else if (!transformation_enabled || query_ctx->get_global_hint().disable_query_transform()) {
    /*do nothing*/
  } else if (OB_FAIL(choose_rewrite_rules(stmt, need_types))) {
    LOG_WARN("failed choose rewrite rules for stmt", K(ret));
  } else if (need_types == 0) {
    // do nothing
  } else {
    if (!query_ctx->get_injected_random_status()) {
      is_outline = query_ctx->get_query_hint().has_outline_data();
      if (is_outline) {
        iter_count = max(iter_count, query_ctx->get_query_hint().trans_list_.count() + 1);
      }
      bool trans_happened = false;
      if (OB_FAIL(transform_rule_set(stmt, need_types, iter_count, trans_happened))) {
        LOG_WARN("failed to transform one rule set", K(ret));
      }
    } else {
      //unlikely need_types 特殊处理, 把所有位置取出来重新排序。
      if (OB_FAIL(transform_random_order(stmt, query_ctx, need_types, iter_count))) {
        LOG_WARN("failed to transform random order", K(ret));
      }
    }
  }
  return ret;
}

// do somthing after transform postprocess:
//    1. add pre calc constraints.
//    2. add trans happened hints
int ObTransformerImpl::do_after_transform(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObExecContext *exec_ctx = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(query_ctx = stmt->get_query_ctx())
      || OB_ISNULL(ctx_) || OB_ISNULL(exec_ctx = ctx_->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(query_ctx), K(ctx_), K(exec_ctx));
  } else if (OB_FAIL(finalize_exec_params(stmt))) {
    LOG_WARN("failed to finalize exec param", K(ret));
  } else if (OB_FAIL(add_trans_happended_hints(*query_ctx, *ctx_))) {
    LOG_WARN("failed to add trans happended hints", K(ret));
  } else if (OB_FAIL(add_param_and_expr_constraints(*exec_ctx, *ctx_, *stmt))) {
    LOG_WARN("failed to add pre calc constraints", K(ret));
  } else if (OB_FAIL(adjust_global_dependency_tables(stmt))) {
    LOG_WARN("failed to adjust global depency", K(ret));
  } else if (OB_FAIL(verify_all_stmt_exprs(stmt))) {
    LOG_WARN("failed to verify all stmt exprs", K(ret));
  }
  return ret;
}

int ObTransformerImpl::get_all_stmts(ObDMLStmt *stmt,
                                     ObIArray<ObDMLStmt*> &all_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(all_stmts.push_back(stmt))) {
    LOG_WARN("failed to push back stmt", K(ret));
  } else {
    ObIArray<TableItem*> &tables = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      if (OB_ISNULL(tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!tables.at(i)->is_temp_table()) {
        /* do nothing */
      } else if (has_exist_in_array(all_stmts, static_cast<ObDMLStmt*>(tables.at(i)->ref_query_))) {
        //do nothing
      } else if (OB_FAIL(SMART_CALL(get_all_stmts(tables.at(i)->ref_query_, all_stmts)))) {
        LOG_WARN("failed to get all stmts", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObSelectStmt*, 4> child_stmts;
      if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
        LOG_WARN("failed to get child stmts", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
        if (OB_FAIL(SMART_CALL(get_all_stmts(child_stmts.at(i), all_stmts)))) {
          LOG_WARN("failed to get all stmts", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformerImpl::do_transform_pre_precessing(ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    ObTransformPreProcess trans(ctx_);
    trans.set_transformer_type(PRE_PROCESS);
    uint64_t dummy_value = 0;
    OPT_TRACE_TITLE("start pre process transform");
    if (OB_FAIL(trans.transform(stmt, dummy_value))) {
      LOG_WARN("failed to do transform pre processing", K(ret));
    } else {
      LOG_TRACE("succeed to do transform pre processing");
    }
  }
  return ret;
}

int ObTransformerImpl::do_transform_dblink_write(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    OPT_TRACE_TITLE("start transform dblink write");
    ObTransformDBlink trans(ctx_);
    trans.set_transformer_type(PROCESS_DBLINK);
    trans.set_transform_for_write(true);
    uint64_t dummy_value = 0;
    if (stmt->is_dml_write_stmt()) {
      ObSEArray<ObParentDMLStmt, 4> parent_stmts;
      if (OB_FAIL(trans.transform_self(parent_stmts, 0, stmt))) {
        LOG_WARN("failed to transform self", K(ret));
      } else {
        trans_happened = trans.get_trans_happened();
        LOG_TRACE("succeed to do transform dml dblink write");
      }
    }
  }
  return ret;
}

int ObTransformerImpl::do_transform_dblink_read(ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    OPT_TRACE_TITLE("start transform dblink read");
    ObTransformDBlink trans(ctx_);
    trans.set_transformer_type(PROCESS_DBLINK);
    trans.set_transform_for_write(false);
    uint64_t dummy_value = 0;
    if (OB_FAIL(trans.transform(stmt, dummy_value))) {
      LOG_WARN("failed to do transform dblink read", K(ret));
    } else {
      LOG_TRACE("succeed to do transform dblink read");
    }
  }
  return ret;
}

int ObTransformerImpl::transform_heuristic_rule(ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  bool trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(transform_rule_set(stmt,
                                        ObTransformRule::ALL_HEURISTICS_RULES,
                                        max_iteration_count_,
                                        trans_happened))) {
    LOG_WARN("failed to transform one rule set", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObTransformerImpl::transform_rule_set(ObDMLStmt *&stmt,
                                          uint64_t needed_types,
                                          int64_t iteration_count,
                                          bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (0 != (needed_types & needed_transform_types_)) {
    bool need_next_iteration = true;
    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && need_next_iteration && i < iteration_count; ++i) {
      bool trans_happened_in_iteration = false;
      ctx_->iteration_level_ = i;
      LOG_TRACE("start to transform one iteration", K(i));
      OPT_TRACE("-- begin ", i, " iteration");
      if (OB_FAIL(transform_rule_set_in_one_iteration(stmt,
                                                      needed_types,
                                                      trans_happened_in_iteration))) {
        LOG_WARN("failed to do transformation one iteration", K(i), K(ret));
      } else if (!trans_happened_in_iteration) {
        need_next_iteration = false;
      } else if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
        LOG_WARN("failed to formalize subquery exprs", K(ret));
      } else if (OB_FAIL(stmt->formalize_stmt_expr_reference(ctx_->expr_factory_, ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt expr", K(ret));
      } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else {
        need_next_iteration = true;
        trans_happened = true;
      }
      LOG_TRACE("succeed to transform one iteration", K(i), K(need_next_iteration), K(ret));
      OPT_TRACE("-- end ", i, " iteration");
    }
    if (OB_SUCC(ret) && i == max_iteration_count_) {
      LOG_INFO("transformer ends without convergence", K(max_iteration_count_));
    }
  }
  return ret;
}

int ObTransformerImpl::transform_rule_set_in_one_iteration(ObDMLStmt *&stmt,
                                                           uint64_t needed_types,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    /*
     * The ordering to apply the following rules is important,
     * think carefully when new rules are added
     */
    APPLY_RULE_IF_NEEDED(MV_REWRITE, ObTransformMVRewrite);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_EXPR, ObTransformSimplifyExpr);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_DISTINCT, ObTransformSimplifyDistinct);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_GROUPBY, ObTransformSimplifyGroupby);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_WINFUNC, ObTransformSimplifyWinfunc);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_ORDERBY, ObTransformSimplifyOrderby);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_LIMIT, ObTransformSimplifyLimit);
    APPLY_RULE_IF_NEEDED(TEMP_TABLE_OPTIMIZATION, ObTransformTempTable);
    APPLY_RULE_IF_NEEDED(PROJECTION_PRUNING, ObTransformProjectPruning);
    APPLY_RULE_IF_NEEDED(CONST_PROPAGATE, ObTransformConstPropagate);
    APPLY_RULE_IF_NEEDED(SUBQUERY_COALESCE, ObTransformSubqueryCoalesce);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_SET, ObTransformSimplifySet);
    APPLY_RULE_IF_NEEDED(VIEW_MERGE, ObTransformViewMerge);
    APPLY_RULE_IF_NEEDED(COUNT_TO_EXISTS, ObTransformCountToExists);
    APPLY_RULE_IF_NEEDED(WHERE_SQ_PULL_UP, ObWhereSubQueryPullup);
    APPLY_RULE_IF_NEEDED(SIMPLIFY_SUBQUERY, ObTransformSimplifySubquery);
    APPLY_RULE_IF_NEEDED(SEMI_TO_INNER, ObTransformSemiToInner);
    APPLY_RULE_IF_NEEDED(QUERY_PUSH_DOWN, ObTransformQueryPushDown);
    APPLY_RULE_IF_NEEDED(SELECT_EXPR_PULLUP, ObTransformExprPullup);
    APPLY_RULE_IF_NEEDED(DECORRELATE, ObTransformDecorrelate);
    APPLY_RULE_IF_NEEDED(ELIMINATE_OJ, ObTransformEliminateOuterJoin);
    APPLY_RULE_IF_NEEDED(JOIN_ELIMINATION, ObTransformJoinElimination);
    APPLY_RULE_IF_NEEDED(JOIN_LIMIT_PUSHDOWN, ObTransformJoinLimitPushDown);
    APPLY_RULE_IF_NEEDED(LEFT_JOIN_TO_ANTI, ObTransformLeftJoinToAnti);
    APPLY_RULE_IF_NEEDED(AGGR_SUBQUERY, ObTransformAggrSubquery);
    APPLY_RULE_IF_NEEDED(WIN_MAGIC, ObTransformWinMagic);
    APPLY_RULE_IF_NEEDED(GROUPBY_PUSHDOWN, ObTransformGroupByPushdown);
    APPLY_RULE_IF_NEEDED(GROUPBY_PULLUP, ObTransformGroupByPullup);
    APPLY_RULE_IF_NEEDED(CONDITIONAL_AGGR_COALESCE, ObTransformConditionalAggrCoalesce);
    APPLY_RULE_IF_NEEDED(FASTMINMAX, ObTransformMinMax);
    APPLY_RULE_IF_NEEDED(PREDICATE_MOVE_AROUND, ObTransformPredicateMoveAround);
    APPLY_RULE_IF_NEEDED(OR_EXPANSION, ObTransformOrExpansion);
  }
  return ret;
}

int ObTransformerImpl::get_cost_based_trans_happened(TRANSFORM_TYPE type, bool &trans_happened) const
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx_));
  } else {
    trans_happened = is_type_needed(ctx_->happened_cost_based_trans_, type);
  }
  return ret;
}

int ObTransformerImpl::collect_trans_stat(const ObTransformRule &rule)
{
  int ret = OB_SUCCESS;
  if (rule.get_trans_happened()) {
    int64_t idx = rule.get_transformer_type();
    if (idx >= 1 && idx < MAX_RULE_COUNT) {
      ++ trans_count_[idx];
    }
  }
  return ret;
}

void ObTransformerImpl::print_trans_stat()
{
  for (int64_t i = 1; i < TRANSFORM_TYPE_COUNT_PLUS_ONE; ++i) {
    LOG_TRACE("Transform Stat ", "Rule", i, "Happened", trans_count_[i]);
  }
}

int ObTransformerImpl::choose_rewrite_rules(ObDMLStmt *stmt, uint64_t &need_types)
{
  int ret = OB_SUCCESS;
  uint64_t disable_list = 0;
  StmtFunc func;
  ObSqlCtx *sql_ctx = NULL;
  if (OB_ISNULL(stmt)
      || OB_ISNULL(ctx_->exec_ctx_)
      || OB_ISNULL(sql_ctx = ctx_->exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (sql_ctx->is_batch_params_execute()) {
    need_types = 0; //如果是batch优化暂时不做改写
  } else if (OB_FAIL(check_stmt_functions(stmt, func))) {
    LOG_WARN("failed to check stmt functions", K(ret));
  } else if (OB_FAIL(check_temp_table_functions(stmt, func))) {
    LOG_WARN("failed to check stmt functions", K(ret));
  } else {
    //TODO::unpivot open @xifeng
    if (func.contain_unpivot_query_ || func.contain_enum_set_values_ || func.contain_geometry_values_ ||
        func.contain_fulltext_search_ || func.contain_dml_with_doc_id_) {
       disable_list = ObTransformRule::ALL_TRANSFORM_RULES;
    }
    if (func.contain_dml_with_doc_id_) {
      uint64_t dml_with_doc_id_enable_list = 0;
      ObTransformRule::add_trans_type(dml_with_doc_id_enable_list, PREDICATE_MOVE_AROUND);
      disable_list &= (~dml_with_doc_id_enable_list);
    }
    if (func.contain_sequence_) {
      ObTransformRule::add_trans_type(disable_list, WIN_MAGIC);
    }
    if (func.contain_for_update_) {
      ObTransformRule::add_trans_type(disable_list, JOIN_ELIMINATION);
      ObTransformRule::add_trans_type(disable_list, WIN_MAGIC);
      ObTransformRule::add_trans_type(disable_list, NL_FULL_OUTER_JOIN);
      ObTransformRule::add_trans_type(disable_list, OR_EXPANSION);
      ObTransformRule::add_trans_type(disable_list, GROUPBY_PUSHDOWN);
      ObTransformRule::add_trans_type(disable_list, GROUPBY_PULLUP);
    }
    if (func.update_global_index_) {
      ObTransformRule::add_trans_type(disable_list, WIN_MAGIC);
    }
    if (func.contain_link_table_) {
      // some rules might generate filter which contains constant values which has implicit types,
      // which can not be printed in the link sql.
      // example：
      // create table t (c1 varchar(10), c2 char(10))
      // select * from t where c1 = 'a' and c2 = c1;
      // => select * from t where c1 = 'a' and c2 = implicit cast('a' as varchar);
      uint64_t dblink_enable_list = 0;
      ObTransformRule::add_trans_type(dblink_enable_list, SIMPLIFY_DISTINCT);
      ObTransformRule::add_trans_type(dblink_enable_list, SIMPLIFY_ORDERBY);
      ObTransformRule::add_trans_type(dblink_enable_list, SIMPLIFY_LIMIT);
      ObTransformRule::add_trans_type(dblink_enable_list, PROJECTION_PRUNING);
      ObTransformRule::add_trans_type(dblink_enable_list, VIEW_MERGE);
      ObTransformRule::add_trans_type(dblink_enable_list, COUNT_TO_EXISTS);
      ObTransformRule::add_trans_type(dblink_enable_list, WHERE_SQ_PULL_UP);
      ObTransformRule::add_trans_type(dblink_enable_list, SIMPLIFY_SUBQUERY);
      ObTransformRule::add_trans_type(dblink_enable_list, QUERY_PUSH_DOWN);
      ObTransformRule::add_trans_type(dblink_enable_list, ELIMINATE_OJ);
      ObTransformRule::add_trans_type(dblink_enable_list, JOIN_ELIMINATION);
      ObTransformRule::add_trans_type(dblink_enable_list, JOIN_LIMIT_PUSHDOWN);
      ObTransformRule::add_trans_type(dblink_enable_list, LEFT_JOIN_TO_ANTI);
      ObTransformRule::add_trans_type(dblink_enable_list, AGGR_SUBQUERY);
      ObTransformRule::add_trans_type(dblink_enable_list, FASTMINMAX);
      disable_list |= (~dblink_enable_list);
    }
    if (func.contain_json_table_) {
      // json table ban group by pushdown && join limit pushdown && left join pushdown
      ObTransformRule::add_trans_type(disable_list, GROUPBY_PUSHDOWN);
      ObTransformRule::add_trans_type(disable_list, JOIN_LIMIT_PUSHDOWN);
      ObTransformRule::add_trans_type(disable_list, LEFT_JOIN_TO_ANTI);
    }
    //dblink trace point
    if ((OB_E(EventTable::EN_GENERATE_PLAN_WITH_RECONSTRUCT_SQL) OB_SUCCESS) != OB_SUCCESS) {
      ObTransformRule::add_trans_type(disable_list, SELECT_EXPR_PULLUP);
    }
    need_types = ObTransformRule::ALL_TRANSFORM_RULES & (~disable_list);
  }
  return ret;
}

int ObTransformerImpl::check_temp_table_functions(ObDMLStmt *stmt, StmtFunc &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::TempTableInfo, 8> temp_table_infos;
  if (func.all_found()) {
    // do nothing
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  }
  for(int64_t i = 0; OB_SUCC(ret) && !func.all_found() && i < temp_table_infos.count(); ++i) {
    ObDMLStmt *child_stmt = temp_table_infos.at(i).temp_table_query_;
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child stmt", K(ret));
    } else if (OB_FAIL(check_stmt_functions(child_stmt, func))) {
      LOG_WARN("failed to check stmt functions", K(ret));
    }
  }
  return ret;
}

int ObTransformerImpl::check_stmt_functions(const ObDMLStmt *stmt, StmtFunc &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else {
    func.contain_hie_query_ = func.contain_hie_query_ || stmt->is_hierarchical_query();
    func.contain_sequence_ = func.contain_sequence_ || stmt->has_sequence();
    func.contain_for_update_ = func.contain_for_update_ || stmt->has_for_update();
    func.contain_unpivot_query_ = func.contain_unpivot_query_ || stmt->is_unpivot_select();
    func.contain_fulltext_search_ = func.contain_fulltext_search_ || (stmt->get_match_exprs().count() != 0);
  }
  for (int64_t i = 0; OB_SUCC(ret)
                      && (!func.contain_enum_set_values_ || !func.contain_geometry_values_)
                      && i < stmt->get_column_items().count(); ++i) {
    const ColumnItem &col = stmt->get_column_items().at(i);
    if (OB_ISNULL(col.get_expr())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      func.contain_enum_set_values_ |= ob_is_enumset_tc(col.get_expr()->get_data_type());
      func.contain_geometry_values_ |= ob_is_geometry_tc(col.get_expr()->get_data_type());
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !func.contain_link_table_ &&
                      i < stmt->get_table_items().count(); ++i) {
    const TableItem *table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (table->is_link_table()) {
      func.contain_link_table_ = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !func.contain_json_table_ &&
                      i < stmt->get_table_items().count(); ++i) {
    const TableItem *table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_json_table()) { // do nothing
    } else if (OB_ISNULL(table->json_table_def_)
               || OB_ISNULL(table->json_table_def_->doc_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!table->json_table_def_->doc_expr_->get_relation_ids().is_empty()) {
      func.contain_json_table_ = true;
    }
  }
  if (OB_SUCC(ret) && (stmt->is_delete_stmt() ||
                       stmt->is_update_stmt() ||
                       stmt->is_merge_stmt() ||
                       stmt->is_insert_stmt())) {
    const ObDelUpdStmt *del_upd_stmt = static_cast<const ObDelUpdStmt *>(stmt);
    func.update_global_index_ = func.update_global_index_ || del_upd_stmt->has_global_index();
  }
  if (OB_SUCC(ret) && (stmt->is_update_stmt() || stmt->is_delete_stmt())) {
    ObSqlSchemaGuard &schema_guard = stmt->query_ctx_->sql_schema_guard_;
    for (int64_t i = 0; OB_SUCC(ret) && !func.contain_dml_with_doc_id_ &&
                        i < stmt->get_table_items().count(); ++i) {
      const ObTableSchema *table_schema;
      if (OB_ISNULL(stmt->get_table_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!stmt->get_table_items().at(i)->get_table_name().suffix_match("rowkey_doc")) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(stmt->get_table_items().at(i)->ref_id_, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table schema", K(table_schema));
      } else if (table_schema->is_rowkey_doc_id()) {
        func.contain_dml_with_doc_id_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && !func.all_found()) {
    ObSEArray<ObSelectStmt*, 8> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      ObSelectStmt *sub_stmt = child_stmts.at(i);
      if (OB_ISNULL(sub_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub stmt is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_stmt_functions(sub_stmt, func)))) {
        LOG_WARN("failed to check stmt functions", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformerImpl::finalize_exec_params(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    ObArray<ObDMLStmt::TempTableInfo> temp_table_infos;
    if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
      LOG_WARN("failed to collect temp table infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); ++i) {
      if (OB_FAIL(child_stmts.push_back(temp_table_infos.at(i).temp_table_query_))) {
        LOG_WARN("failed to push back temp table query", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
    TableItem *table_item = NULL;
    if (OB_ISNULL(table_item = stmt->get_table_items().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(finalize_exec_params(stmt, table_item->exec_params_))) {
      LOG_WARN("failed to finalize exec params", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
    ObQueryRefRawExpr *query_ref = NULL;
    if (OB_ISNULL(query_ref = stmt->get_subquery_exprs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref expr is null", K(ret));
    } else if (OB_FAIL(finalize_exec_params(stmt, query_ref->get_exec_params()))) {
      LOG_WARN("failed to finalize exec params", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(finalize_exec_params(child_stmts.at(i))))) {
      LOG_WARN("failed to finalize exec params", K(ret));
    }
  }
  return ret;
}

int ObTransformerImpl::finalize_exec_params(ObDMLStmt *stmt, ObIArray<ObExecParamRawExpr*> & exec_params)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < exec_params.count(); ++j) {
    ObExecParamRawExpr *exec_param = NULL;
    if (OB_ISNULL(exec_param = exec_params.at(j))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret));
    } else if (exec_param->get_param_index() >= 0) {
      // do nothing
    } else {
      exec_param->set_param_index(stmt->get_question_marks_count());
      stmt->increase_question_marks_count();
    }
  }
  return ret;
}

// TODO @yibo perhaps we can't remove dependency table.
// for example, t1 left join t2 on t1.c1 = t2.c1 eliminate t2 relay on t2.c1 is unique,
// if schema version of t2 change, t2.c1 may not uniuqe.
int ObTransformerImpl::adjust_global_dependency_tables(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObIArray<share::schema::ObSchemaObjVersion> *global_tables = stmt->get_global_dependency_table();
    if (OB_ISNULL(global_tables)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else { /*do nothing.*/ }
    for (int64_t i = 0; OB_SUCC(ret) && i < global_tables->count(); ++i) {
      bool is_existed = false;
      if (global_tables->at(i).is_base_table()) {
        if (OB_FAIL(stmt->check_if_table_exists(global_tables->at(i).object_id_,
                                                is_existed))) {
          LOG_WARN("failed to check if exists in stmt.", K(ret));
        } else if (!is_existed) {
          global_tables->at(i).is_existed_ = false;
        } else { /* do nothing. */ }
      } else { /* do nothing. */ }
    }
  }
  return ret;
}

int ObTransformerImpl::verify_all_stmt_exprs(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(verify_stmt_exprs(stmt))) {
    LOG_WARN("failed to verify stmt exprs", K(ret));
  } else {
    ObArray<ObDMLStmt::TempTableInfo> temp_table_infos;
    if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
      LOG_WARN("failed to collect temp table infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); ++i) {
      if (OB_FAIL(verify_stmt_exprs(temp_table_infos.at(i).temp_table_query_))) {
        LOG_WARN("failed to verify temp table query exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformerImpl::verify_stmt_exprs(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    ObStmtExprChecker checker;
    checker.set_relation_scope();
    if (OB_FAIL(stmt->iterate_stmt_expr(checker))) {
      LOG_WARN("failed to check stmt expr", K(ret), KPC(stmt));
    }
  }
  return ret;
}

int ObTransformerImpl::add_param_and_expr_constraints(ObExecContext &exec_ctx,
                                                      ObTransformerCtx &trans_ctx,
                                                      ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObHiddenColumnItem, 8> pre_calc_error_exprs;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(query_ctx));
  } else if (OB_FAIL(append(query_ctx->all_plan_const_param_constraints_,
                            trans_ctx.plan_const_param_constraints_))) {
    LOG_WARN("fail to append const param constraints. ", K(ret));
  } else if (OB_FAIL(append(query_ctx->all_equal_param_constraints_,
                            trans_ctx.equal_param_constraints_))) {
    LOG_WARN("fail to append equal param constraints. ", K(ret));
  } else if (OB_FAIL(append(query_ctx->all_expr_constraints_,
                            trans_ctx.expr_constraints_))) {
    LOG_WARN("fail to append expr constraints", K(ret));
  }
  return ret;
}

int ObTransformerImpl::add_all_rowkey_columns_to_stmt(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(ctx_));
  } else {
    ObIArray<TableItem*> &tables = stmt->get_table_items();
    TableItem *table_item = NULL;
    const ObTableSchema *table_schema = NULL;
    ObSEArray<ColumnItem, 16> column_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(table_item = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null", K(ret), K(table_item));
      } else if (!table_item->is_basic_table() && !table_item->is_link_table()) {
        /* do nothing */
      } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(),
                                                                 table_item->ref_id_,
                                                                 table_schema,
                                                                 table_item->is_link_table()))) {
        LOG_WARN("table schema not found", K(table_schema));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table schema", K(table_schema));
      } else if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table_schema, *table_item,
                                                        *ctx_->expr_factory_,
                                                        *stmt,
                                                        column_items))) {
        LOG_WARN("add all rowkey exprs failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && !column_items.empty()) {
      ObIArray<ColumnItem> &orign_column_items = stmt->get_column_items();
      for (int i = 0; OB_SUCC(ret) && i < orign_column_items.count(); ++i) {
        if (OB_ISNULL(orign_column_items.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(orign_column_items.at(i).expr_));
        } else if (!orign_column_items.at(i).expr_->is_rowkey_column() &&
                   OB_FAIL(column_items.push_back(orign_column_items.at(i)))) {
          LOG_WARN("failed to push back column item", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(stmt->get_column_items().assign(column_items))) {
        LOG_WARN("failed to assign column items", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformerImpl::add_all_rowkey_columns_to_stmt(const ObTableSchema &table_schema,
                                                      const TableItem &table_item,
                                                      ObRawExprFactory &expr_factory,
                                                      ObDMLStmt &stmt,
                                                      ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  const ObColumnSchemaV2 *column_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;
  ColumnItem *exists_col_item = NULL;
  ObColumnRefRawExpr *rowkey = NULL;
  for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
    if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (NULL != (exists_col_item = stmt.get_column_item_by_id(table_item.table_id_,
                                                                     column_id))) {
      if (OB_FAIL(column_items.push_back(*exists_col_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      }
    } else if (OB_MOCK_LINK_TABLE_PK_COLUMN_ID == column_id && table_item.is_link_table()) {		
      continue;
    } else if (OB_ISNULL(column_schema = (table_schema.get_column_schema(column_id)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(column_id), K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *column_schema, rowkey))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(rowkey)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create raw expr for dummy output", K(ret));
    } else {
      ColumnItem column_item;
      rowkey->set_ref_id(table_item.table_id_, column_schema->get_column_id());
      rowkey->set_column_attr(table_item.get_table_name(), column_schema->get_column_name_str());
      rowkey->set_database_name(table_item.database_name_);
      if (!table_item.alias_name_.empty()) {
        rowkey->set_table_alias_name();
      }
      column_item.table_id_ = rowkey->get_table_id();
      column_item.column_id_ = rowkey->get_column_id();
      column_item.base_tid_ = table_item.ref_id_;
      column_item.base_cid_ = rowkey->get_column_id();
      column_item.column_name_ = rowkey->get_column_name();
      column_item.set_default_value(column_schema->get_cur_default_value());
      column_item.expr_ = rowkey;
      if (OB_FAIL(stmt.add_column_item(column_item))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else if (OB_FAIL(column_items.push_back(column_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      } else if (FALSE_IT(rowkey->clear_explicited_referece())) {
      } else if (OB_FAIL(rowkey->formalize(NULL))) {
        LOG_WARN("formalize rowkey failed", K(ret));
      } else if (OB_FAIL(rowkey->pull_relation_id())) {
        LOG_WARN("failed to pullup relation ids", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformerImpl::add_trans_happended_hints(ObQueryCtx &query_ctx,
                                                 ObTransformerCtx &trans_ctx)
{
  int ret = OB_SUCCESS;
  ObQueryHint &query_hint = query_ctx.get_query_hint_for_update();
#ifndef OB_BUILD_SPM
  if (OB_FAIL(query_hint.outline_trans_hints_.assign(trans_ctx.outline_trans_hints_))) {
#else
  if (OB_UNLIKELY(query_ctx.is_spm_evolution_
                  && query_hint.has_outline_data()
                  && query_hint.trans_list_.count() != trans_ctx.outline_trans_hints_.count())) {
    ret = OB_OUTLINE_NOT_REPRODUCIBLE;
    LOG_WARN("failed to do transform for spm evolution plan", K(ret));
  } else if (OB_FAIL(query_hint.outline_trans_hints_.assign(trans_ctx.outline_trans_hints_))) {
#endif
    LOG_WARN("failed to assign trans hints", K(ret));
  } else if (OB_FAIL(query_hint.used_trans_hints_.assign(trans_ctx.used_trans_hints_))) {
    LOG_WARN("failed to assign trans hints", K(ret));
  }
  return ret;
}

}
}
