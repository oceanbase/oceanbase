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
#include "sql/rewrite/ob_transform_view_merge.h"
#include "sql/rewrite/ob_transform_aggregate.h"
#include "sql/rewrite/ob_transform_where_subquery_pullup.h"
#include "sql/rewrite/ob_transform_eliminate_outer_join.h"
#include "sql/rewrite/ob_transform_simplify.h"
#include "sql/rewrite/ob_transform_any_all.h"
#include "sql/rewrite/ob_transform_query_push_down.h"
#include "sql/rewrite/ob_transform_aggr_subquery.h"
#include "sql/rewrite/ob_transform_set_op.h"
#include "sql/rewrite/ob_transform_project_pruning.h"
#include "sql/rewrite/ob_transform_or_expansion.h"
#include "sql/rewrite/ob_transform_join_elimination.h"
#include "sql/rewrite/ob_transform_win_magic.h"
#include "sql/rewrite/ob_transform_groupby_placement.h"
#include "sql/rewrite/ob_transform_subquery_coalesce.h"
#include "sql/rewrite/ob_transform_win_groupby.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/rewrite/ob_transform_post_process.h"
#include "sql/rewrite/ob_transform_full_outer_join.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/rewrite/ob_transform_predicate_move_around.h"
#include "sql/rewrite/ob_transform_semi_to_inner.h"
#include "sql/rewrite/ob_transform_outerjoin_limit_pushdown.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace sql {

int ObTransformerImpl::transform(ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(do_transform_pre_precessing(stmt))) {
    LOG_WARN("failed to do transform pre_precessing", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if (OB_FAIL(do_transform(stmt))) {
    LOG_WARN("failed to do transform", K(ret));
  } else if (OB_FAIL(do_transform_post_precessing(stmt))) {
    LOG_WARN("failed to do transform post precessing", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else {
    print_trans_stat();
  }
  return ret;
}

int ObTransformerImpl::do_transform(ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  uint64_t need_types = ObTransformRule::ALL_TRANSFORM_RULES;
  bool transformation_enabled = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ctx_->session_info_), K(ret));
  } else if (OB_FAIL(ctx_->session_info_->is_transformation_enabled(transformation_enabled))) {
    LOG_WARN("fail to get transformation_enabled", K(ret));
  } else if (!transformation_enabled) {
    /*do nothing*/
  } else if (OB_FAIL(choose_rewrite_rules(stmt, need_types))) {
    LOG_WARN("failed choose rewrite rules for stmt", K(ret));
  } else if (need_types == 0) {
    // do nothing
  } else if (OB_FAIL(transform_rule_set(stmt, need_types, max_iteration_count_))) {
    LOG_WARN("failed to transform one rule set", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}
int ObTransformerImpl::do_transform_pre_precessing(ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    ObTransformPreProcess trans(ctx_);
    trans.set_transformer_type(PRE_PROCESS);
    uint64_t dummy_value = 0;
    if (OB_FAIL(trans.transform(stmt, dummy_value))) {
      LOG_WARN("failed to do transform pre processing", K(ret));
    } else {
      LOG_TRACE("succeed to do transform pre processing");
    }
  }
  return ret;
}

int ObTransformerImpl::do_transform_post_precessing(ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  ObQueryCtx* sql_ctx = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(sql_ctx = stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(sql_ctx));
  } else {
    ObTransformPostProcess trans(ctx_);
    trans.set_transformer_type(POST_PROCESS);
    uint64_t dummy_value = 0;
    if (OB_FAIL(trans.transform(stmt, dummy_value))) {
      LOG_WARN("failed to do transform post processing", K(ret));
    } else if (OB_FAIL(adjust_global_dependency_tables(stmt))) {
      LOG_WARN("failed to adjust global dependency tables.", K(ret));
    } else if (sql_ctx->temp_table_infos_.count() > 0) {
      // For table query in temp table may have hierarchical view of created in transform pre phase.
      // So, we need transform hierarchical view to hierarchical query in tranformer post phase.
      for (int64_t i = 0; OB_SUCC(ret) && i < sql_ctx->temp_table_infos_.count(); ++i) {
        ObSqlTempTableInfo* temp_table_info = NULL;
        if (OB_ISNULL(temp_table_info = sql_ctx->temp_table_infos_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          ObDMLStmt* query_stmt = static_cast<ObDMLStmt*>(temp_table_info->table_query_);
          if (OB_FAIL(trans.transform(query_stmt, dummy_value))) {
            LOG_WARN("failed to do transform post processing", K(ret));
          } else if (OB_FAIL(adjust_global_dependency_tables(query_stmt))) {
            LOG_WARN("failed to adjust global dependency tables.", K(ret));
          } else {
            LOG_TRACE("succeed to do transform post processing", K(*query_stmt));
          }
        }
      }
    } else {
      LOG_TRACE("succeed to do transform post processing");
    }
  }
  return ret;
}

int ObTransformerImpl::transform_heuristic_rule(ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(transform_rule_set(stmt, ObTransformRule::ALL_HEURISTICS_RULES, max_iteration_count_))) {
    LOG_WARN("failed to transform one rule set", K(ret));
  } else if (OB_FAIL(do_transform_post_precessing(stmt))) {
    LOG_WARN("failed to transform post processing", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformerImpl::transform_rule_set(ObDMLStmt*& stmt, uint64_t needed_types, int64_t iteration_count)
{
  int ret = OB_SUCCESS;
  if (0 != (needed_types & needed_transform_types_)) {
    bool need_next_iteration = true;
    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && need_next_iteration && i < iteration_count; ++i) {
      bool trans_happened_in_iteration = false;
      LOG_TRACE("start to transform one iteration", K(i));
      if (OB_FAIL(transform_rule_set_in_one_iteration(stmt, needed_types, trans_happened_in_iteration))) {
        LOG_WARN("failed to do transformation one iteration", K(i), K(ret));
      } else {
        need_next_iteration &= trans_happened_in_iteration;
        LOG_TRACE("succeed to transform one iteration", K(i));
      }
    }
    if (OB_SUCC(ret) && i == max_iteration_count_) {
      LOG_INFO("transformer ends without convergence", K(max_iteration_count_));
    }
  }
  return ret;
}

int ObTransformerImpl::transform_rule_set_in_one_iteration(
    ObDMLStmt*& stmt, uint64_t needed_types, bool& trans_happened)
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
    uint64_t trans_happened_route = 0;

    APPLY_RULE_IF_NEEDED(SIMPLIFY, ObTransformSimplify);
    APPLY_RULE_IF_NEEDED(SUBQUERY_COALESCE, ObTransformSubqueryCoalesce);
    APPLY_RULE_IF_NEEDED(ANYALL, ObTransformAnyAll);
    APPLY_RULE_IF_NEEDED(SET_OP, ObTransformSetOp);
    APPLY_RULE_IF_NEEDED(VIEW_MERGE, ObTransformViewMerge);
    APPLY_RULE_IF_NEEDED(WHERE_SQ_PULL_UP, ObWhereSubQueryPullup);
    APPLY_RULE_IF_NEEDED(SEMI_TO_INNER, ObTransformSemiToInner);
    APPLY_RULE_IF_NEEDED(QUERY_PUSH_DOWN, ObTransformQueryPushDown);
    APPLY_RULE_IF_NEEDED(ELIMINATE_OJ, ObTransformEliminateOuterJoin);
    APPLY_RULE_IF_NEEDED(JOIN_ELIMINATION, ObTransformJoinElimination);
    APPLY_RULE_IF_NEEDED(OUTERJOIN_LIMIT_PUSHDOWN, ObTransformOuterJoinLimitPushDown);
    APPLY_RULE_IF_NEEDED(NL_FULL_OUTER_JOIN, ObTransformFullOuterJoin);
    APPLY_RULE_IF_NEEDED(OR_EXPANSION, ObTransformOrExpansion);
    APPLY_RULE_IF_NEEDED(WIN_MAGIC, ObTransformWinMagic);
    APPLY_RULE_IF_NEEDED(WIN_GROUPBY, ObTransformWinGroupBy);
    APPLY_RULE_IF_NEEDED(JOIN_AGGREGATION, ObTransformAggrSubquery);
    APPLY_RULE_IF_NEEDED(GROUPBY_PLACEMENT, ObTransformGroupByPlacement);
    APPLY_RULE_IF_NEEDED(AGGR, ObTransformAggregate);
    // project pruning should be done after window function transformation rules
    APPLY_RULE_IF_NEEDED(PROJECTION_PRUNING, ObTransformProjectPruning);
    APPLY_RULE_IF_NEEDED(PREDICATE_MOVE_AROUND, ObTransformPredicateMoveAround);

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ctx_)) {
        LOG_WARN("transformer ctx is null", KP(ctx_), K(ret));
      } else if (OB_FAIL(ctx_->trans_happened_route_.push_back(trans_happened_route))) {
        LOG_WARN("fail to push trans route", K(ret), K(trans_happened_route));
      }
    }
  }
  return ret;
}

int ObTransformerImpl::adjust_global_dependency_tables(ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObIArray<schema::ObSchemaObjVersion>* global_tables = stmt->get_global_dependency_table();
    if (OB_ISNULL(global_tables)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else { /*do nothing.*/
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < global_tables->count(); i++) {
      bool is_existed = false;
      if (global_tables->at(i).is_base_table()) {
        if (OB_FAIL(stmt->check_if_table_exists(global_tables->at(i).object_id_, is_existed))) {
          LOG_WARN("failed to check if exists in stmt.", K(ret));
        } else if (!is_existed) {
          global_tables->at(i).is_existed_ = false;
        } else { /* do nothing. */
        }
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObTransformerImpl::get_cost_based_trans_happened(TRANSFORM_TYPE type, bool& trans_happened) const
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx_));
  } else {
    trans_happened = ((ctx_->happened_cost_based_trans_ & type) != 0);
  }
  return ret;
}

int ObTransformerImpl::collect_trans_stat(const ObTransformRule& rule)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (rule.get_trans_happened()) {
    idx = std::log(rule.get_transformer_type());
    if (idx > 1 && idx < MAX_RULE_COUNT) {
      ++trans_count_[idx];
    }
  }
  return ret;
}

void ObTransformerImpl::print_trans_stat()
{
  int64_t idx = 1;
  for (int64_t i = 1; i < TRANSFORM_TYPE_COUNT_PLUS_ONE; i = i * 2) {
    LOG_TRACE("Transform Stat ", "Rule", idx, "Happened", trans_count_[idx]);
    ++idx;
  }
}

int ObTransformerImpl::choose_rewrite_rules(ObDMLStmt* stmt, uint64_t& need_types)
{
  int ret = OB_SUCCESS;
  uint64_t disable_list = 0;
  StmtFunc func;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(check_stmt_functions(stmt, func))) {
    LOG_WARN("failed to check stmt functions", K(ret));
  } else {
    // TODO::unpivot open
    if (func.contain_unpivot_query_) {
      disable_list = ObTransformRule::ALL_TRANSFORM_RULES;
    }
    if (func.contain_sequence_) {
      disable_list = disable_list | (WIN_MAGIC | WIN_GROUPBY);
    }
    if (func.contain_for_update_) {
      disable_list = disable_list | (JOIN_ELIMINATION | WIN_MAGIC | NL_FULL_OUTER_JOIN | OR_EXPANSION | WIN_GROUPBY);
    }
    if (func.update_global_index_) {
      disable_list = disable_list | (OR_EXPANSION | WIN_MAGIC);
    }
    need_types = ObTransformRule::ALL_TRANSFORM_RULES & (~disable_list);
  }
  return ret;
}

int ObTransformerImpl::check_stmt_functions(ObDMLStmt* stmt, StmtFunc& func)
{
  int ret = OB_SUCCESS;
  bool has_for_update = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (!func.contain_for_update_ && OB_FAIL(stmt->has_for_update(has_for_update))) {
    LOG_WARN("failed to check has for update", K(ret));
  } else {
    func.contain_hie_query_ = func.contain_hie_query_ || stmt->is_hierarchical_query();
    func.contain_sequence_ = func.contain_sequence_ || stmt->has_sequence();
    func.contain_for_update_ = func.contain_for_update_ || has_for_update;
    func.contain_unpivot_query_ = func.contain_unpivot_query_ || stmt->is_unpivot_select();
  }
  if (OB_SUCC(ret) &&
      (stmt->is_delete_stmt() || stmt->is_update_stmt() || stmt->is_merge_stmt() || stmt->is_insert_stmt())) {
    ObDelUpdStmt* del_upd_stmt = static_cast<ObDelUpdStmt*>(stmt);
    func.update_global_index_ = func.update_global_index_ || del_upd_stmt->has_global_index();
  }
  if (OB_SUCC(ret) && !func.all_found()) {
    ObSEArray<ObSelectStmt*, 8> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      ObSelectStmt* sub_stmt = child_stmts.at(i);
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

}  // namespace sql
}  // namespace oceanbase
