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

#include "ob_stmt_comparer.h"
#include "ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"

using namespace oceanbase::sql;

int ObStmtCompareContext::init(const ObQueryCtx* context)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    context_ = context;
  }
  return ret;
}

int ObStmtCompareContext::init(const ObDMLStmt* inner, const ObDMLStmt* outer, const ObIArray<int64_t>& table_map)
{
  int ret = OB_SUCCESS;
  TableItem* inner_table = NULL;
  TableItem* outer_table = NULL;
  if (OB_ISNULL(inner) || OB_ISNULL(outer) || OB_ISNULL(inner->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret), K(inner), K(outer));
  } else if (OB_UNLIKELY(inner->get_table_size() != table_map.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table map size", K(ret));
  } else {
    context_ = inner->get_query_ctx();
  }
  for (int64_t inner_idx = 0; OB_SUCC(ret) && inner_idx < table_map.count(); ++inner_idx) {
    int64_t outer_idx = table_map.at(inner_idx);
    if (outer_idx < 0 || outer_idx >= outer->get_table_size()) {
      // do nothing
    } else if (OB_ISNULL(inner_table = inner->get_table_items().at(inner_idx)) ||
               OB_ISNULL(outer_table = outer->get_table_items().at(outer_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table items are null", K(ret));
    } else {
      std::pair<int64_t, int64_t> id_pair(inner_table->table_id_, outer_table->table_id_);
      if (OB_FAIL(table_id_pairs_.push_back(id_pair))) {
        LOG_WARN("failed to push back table id pairs", K(ret));
      }
    }
  }
  return ret;
}

bool ObStmtCompareContext::compare_column(const ObColumnRefRawExpr& inner, const ObColumnRefRawExpr& outer)
{
  bool bret = false;
  if (inner.get_column_id() == outer.get_column_id()) {
    bret = inner.get_table_id() == outer.get_table_id();
    for (int64_t i = 0; !bret && i < table_id_pairs_.count(); ++i) {
      bret =
          inner.get_table_id() == table_id_pairs_.at(i).first && outer.get_table_id() == table_id_pairs_.at(i).second;
    }
  }
  return bret;
}

bool ObStmtCompareContext::compare_const(const ObConstRawExpr& left, const ObConstRawExpr& right)
{
  int& ret = err_code_;
  bool bret = false;
  if (OB_SUCC(ret) && left.get_result_type() == right.get_result_type()) {
    if (&left == &right) {
      bret = true;
    } else if (left.has_flag(IS_PARAM) && right.has_flag(IS_PARAM)) {
      bool is_left_calc_item = false;
      bool is_right_calc_item = false;
      if (OB_FAIL(is_pre_calc_item(left, is_left_calc_item))) {
        LOG_WARN("failed to is pre calc item", K(ret));
      } else if (OB_FAIL(is_pre_calc_item(right, is_right_calc_item))) {
        LOG_WARN("failed to is pre calc item", K(ret));
      } else if (is_left_calc_item && is_right_calc_item) {
        const ObRawExpr* left_param = NULL;
        const ObRawExpr* right_param = NULL;
        if (OB_FAIL(get_calc_expr(left.get_value().get_unknown(), left_param))) {
          LOG_WARN("faield to get calculable expr", K(ret));
        } else if (OB_FAIL(get_calc_expr(right.get_value().get_unknown(), right_param))) {
          LOG_WARN("failed to get calculable expr", K(ret));
        } else if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param exprs are null", K(ret), K(left_param), K(right_param));
        } else {
          bret = left_param->same_as(*right_param, this);
        }
      } else if (is_left_calc_item || is_right_calc_item) {
        bret = false;
      } else if (left.get_result_type().get_param().is_equal(right.get_result_type().get_param(), CS_TYPE_BINARY)) {
        ObPCParamEqualInfo info;
        info.first_param_idx_ = left.get_value().get_unknown();
        info.second_param_idx_ = right.get_value().get_unknown();
        bret = true;
        if (info.first_param_idx_ != info.second_param_idx_ && OB_FAIL(equal_param_info_.push_back(info))) {
          LOG_WARN("failed to push back equal param info", K(ret));
        }
      }
    } else if (left.has_flag(IS_PARAM) || right.has_flag(IS_PARAM)) {
      bret = ObExprEqualCheckContext::compare_const(left, right);
    } else {
      bret = left.get_value().is_equal(right.get_value(), CS_TYPE_BINARY);
    }
  }
  return bret;
}

int ObStmtCompareContext::is_pre_calc_item(const ObConstRawExpr& const_expr, bool& is_calc)
{
  int ret = OB_SUCCESS;
  int64_t calc_count = 0;
  is_calc = false;
  if (OB_ISNULL(context_) || OB_UNLIKELY((calc_count = context_->calculable_items_.count()) < 0 ||
                                         const_expr.get_expr_type() != T_QUESTIONMARK)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(context_), K(const_expr.get_expr_type()), K(calc_count));
  } else if (const_expr.has_flag(IS_EXEC_PARAM)) {
    is_calc = true;
  } else if (calc_count > 0) {
    int64_t q_idx = const_expr.get_value().get_unknown();
    int64_t min_calc_index = context_->calculable_items_.at(0).hidden_idx_;
    if (OB_UNLIKELY(q_idx < 0 || min_calc_index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid argument", K(q_idx), K(min_calc_index));
    } else if (q_idx - min_calc_index >= 0 && q_idx - min_calc_index < calc_count) {
      is_calc = true;
    } else { /*do nothing*/
    }
  }
  return ret;
}

bool ObStmtCompareContext::compare_query(const ObQueryRefRawExpr& first, const ObQueryRefRawExpr& second)
{
  bool bret = false;
  if (&first == &second) {
    bret = true;
  }
  return bret;
}

int ObStmtCompareContext::get_calc_expr(const int64_t param_idx, const ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context_) || context_->calculable_items_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query context is null", K(ret));
  } else {
    int64_t offset = param_idx - context_->calculable_items_.at(0).hidden_idx_;
    if (offset < 0 || offset >= context_->calculable_items_.count() ||
        param_idx != context_->calculable_items_.at(offset).hidden_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param index", K(ret), K(param_idx), K(offset));
    } else {
      expr = context_->calculable_items_.at(offset).expr_;
    }
  }
  return ret;
}

int ObStmtComparer::compute_stmt_overlap(ObDMLStmt* first, ObDMLStmt* second, ObStmtMapInfo& map_info)
{
  int ret = OB_SUCCESS;
  int64_t match_count = 0;
  map_info.reset();
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts are null", K(ret), K(first), K(second));
  } else if (OB_FAIL(compute_from_items_map(first, second, map_info, match_count))) {
    LOG_WARN("failed to compute from items map", K(ret));
  } else if (OB_FAIL(map_info.cond_map_.prepare_allocate(first->get_condition_size()))) {
    LOG_WARN("failed to pre-allocate condition map", K(ret));
  } else if (OB_FAIL(compute_conditions_map(first,
                 second,
                 first->get_condition_exprs(),
                 second->get_condition_exprs(),
                 map_info,
                 map_info.cond_map_,
                 match_count))) {
    LOG_WARN("failed to compute condition map", K(ret));
  } else {
    LOG_TRACE("stmt map info", K(map_info));
  }
  return ret;
}

/**
 * @brief ObTransformUtils::is_same_from
 *  compare whether two from items are same
 * @return
 */
int ObStmtComparer::is_same_from(
    ObDMLStmt* first, const FromItem& first_from, ObDMLStmt* second, const FromItem& second_from, bool& is_same)
{
  int ret = OB_SUCCESS;
  TableItem* first_table = NULL;
  TableItem* second_table = NULL;
  bool is_semi_table = false;
  is_same = false;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts have null", K(ret), K(first), K(second));
  } else if (first_from.is_joined_ || second_from.is_joined_) {
    // do nothing
  } else if (OB_ISNULL(first_table = first->get_table_item_by_id(first_from.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first table item is null", K(ret), K(first));
  } else if (OB_ISNULL(second_table = second->get_table_item_by_id(second_from.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("second table item is null", K(ret), K(second));
  } else if (OB_FAIL(ObTransformUtils::is_semi_join_right_table(*first, first_table->table_id_, is_semi_table))) {
    LOG_WARN("failed to check is semi join right table", K(ret));
  } else if (is_semi_table) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::is_semi_join_right_table(*second, second_table->table_id_, is_semi_table))) {
    LOG_WARN("failed to check is semi join right table", K(ret));
  } else if (is_semi_table) {
    // do nothing
  } else if (first_table->is_basic_table() && second_table->is_basic_table()) {
    QueryRelation relation = QueryRelation::UNCOMPARABLE;
    // be careful for partition hint
    if (OB_FAIL(compare_basic_table_item(first, first_table, second, second_table, relation))) {
      LOG_WARN("compare table part failed", K(ret), K(first_table), K(second_table));
    } else if (QueryRelation::EQUAL == relation) {
      is_same = true;
    } else {
      /*do noting*/
    }
  } else if (first_table->is_generated_table() && first_table->is_view_table_ && second_table->is_generated_table() &&
             second_table->is_view_table_) {
    if (OB_ISNULL(first_table->ref_query_) || OB_ISNULL(second_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is null", K(ret));
    } else {
      is_same = (first_table->ref_query_->get_view_ref_id() == second_table->ref_query_->get_view_ref_id()) &&
                first_table->ref_query_->get_view_ref_id() != OB_INVALID_ID;
    }
  }
  return ret;
}

int ObStmtComparer::check_stmt_containment(
    ObDMLStmt* first, ObDMLStmt* second, ObStmtMapInfo& map_info, QueryRelation& relation)
{
  int ret = OB_SUCCESS;
  int64_t first_count = 0;
  int64_t second_count = 0;
  int64_t match_count = 0;
  ObSelectStmt* first_sel = NULL;
  ObSelectStmt* second_sel = NULL;
  relation = QueryRelation::UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (!first->is_select_stmt() || !second->is_select_stmt()) {
    /*do nothing*/
  } else if (FALSE_IT(first_sel = static_cast<ObSelectStmt*>(first))) {
    /*do nothing*/
  } else if (FALSE_IT(second_sel = static_cast<ObSelectStmt*>(second))) {
    /*do nothing*/
  } else if (first_sel->is_set_stmt() || second_sel->is_set_stmt() || first_sel->has_recusive_cte() ||
             second_sel->has_recusive_cte() || first_sel->has_hierarchical_query() ||
             second_sel->has_hierarchical_query() || first_sel->is_contains_assignment() ||
             second_sel->is_contains_assignment()) {
    /*do nothing*/
  } else if (first_sel->get_from_item_size() != second_sel->get_from_item_size()) {
    /*do nothing*/
  } else {
    // check from items
    if (OB_FAIL(compute_from_items_map(first_sel, second_sel, map_info, match_count))) {
      LOG_WARN("failed to compute from items map", K(ret));
    } else if (match_count != first_sel->get_from_item_size()) {
      relation = QueryRelation::UNCOMPARABLE;
      LOG_TRACE("succeed to check from item map", K(relation), K(map_info));
    } else {
      relation = QueryRelation::EQUAL;
      LOG_TRACE("succeed to check from item map", K(relation), K(map_info));
    }

    // check condition exprs
    if (OB_SUCC(ret) && QueryRelation::EQUAL == relation) {
      first_count = first_sel->get_condition_size();
      second_count = second_sel->get_condition_size();
      if (OB_FAIL(compute_conditions_map(first_sel,
              second_sel,
              first_sel->get_condition_exprs(),
              second_sel->get_condition_exprs(),
              map_info,
              map_info.cond_map_,
              match_count))) {
        LOG_WARN("failed to compute conditions map", K(ret));
      } else if (match_count == first_count && match_count == second_count) {
        relation = QueryRelation::EQUAL;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      } else if (match_count == first_count && match_count < second_count) {
        relation = QueryRelation::RIGHT_SUBSET;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      } else if (match_count < first_count && match_count == second_count) {
        relation = QueryRelation::LEFT_SUBSET;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      }
    }

    // check group by exprs
    if (OB_SUCC(ret) && QueryRelation::UNCOMPARABLE != relation) {
      bool is_consistent = false;
      common::ObSEArray<ObRawExpr*, 4> first_groupby_rollup_exprs;
      common::ObSEArray<ObRawExpr*, 4> second_groupby_rollup_exprs;
      if (OB_FAIL(append(first_groupby_rollup_exprs, first_sel->get_group_exprs()))) {
        LOG_WARN("failed to append group by rollup exprs.", K(ret));
      } else if (OB_FAIL(append(first_groupby_rollup_exprs, first_sel->get_rollup_exprs()))) {
        LOG_WARN("failed to append group by rollup exprs.", K(ret));
      } else if (OB_FAIL(append(second_groupby_rollup_exprs, second_sel->get_group_exprs()))) {
        LOG_WARN("failed to append group by rollup exprs.", K(ret));
      } else if (OB_FAIL(append(second_groupby_rollup_exprs, second_sel->get_rollup_exprs()))) {
        LOG_WARN("failed to append group by rollup exprs.", K(ret));
      } else { /*do nothing.*/
      }
      first_count = first_groupby_rollup_exprs.count();
      second_count = second_groupby_rollup_exprs.count();
      if (OB_FAIL(ret)) {
      } else if ((first_sel->get_aggr_item_size() > 0 || second_sel->get_aggr_item_size() > 0) &&
                 relation != QueryRelation::EQUAL) {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      } else if (0 == first_count && 0 == second_count) {
        /*do nothing*/
      } else if (OB_FAIL(ObTransformUtils::check_group_by_consistent(first_sel, is_consistent))) {
        LOG_WARN("failed to check whether group by is consistent", K(ret));
      } else if (!is_consistent) {
        relation = QueryRelation::UNCOMPARABLE;
      } else if (OB_FAIL(ObTransformUtils::check_group_by_consistent(second_sel, is_consistent))) {
        LOG_WARN("failed to check whether group by is consistent", K(ret));
      } else if (!is_consistent) {
        relation = QueryRelation::UNCOMPARABLE;
      } else if (OB_FAIL(compute_conditions_map(first_sel,
                     second_sel,
                     first_groupby_rollup_exprs,
                     second_groupby_rollup_exprs,
                     map_info,
                     map_info.group_map_,
                     match_count))) {
        LOG_WARN("failed to compute group by expr map", K(ret));
      } else if (match_count != first_count || match_count != second_count) {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      } else {
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      }
    }

    // check having exprs
    if (OB_SUCC(ret) && QueryRelation::UNCOMPARABLE != relation) {
      first_count = first_sel->get_having_expr_size();
      second_count = second_sel->get_having_expr_size();
      if (0 == first_count && 0 == second_count) {
        /*do nothing*/
      } else if (OB_FAIL(compute_conditions_map(first_sel,
                     second_sel,
                     first_sel->get_having_exprs(),
                     second_sel->get_having_exprs(),
                     map_info,
                     map_info.having_map_,
                     match_count))) {
        LOG_WARN("failed to compute having expr map", K(ret));
      } else if (match_count == first_count && match_count == second_count) {
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      } else if (match_count == first_count && match_count < second_count &&
                 (relation == QueryRelation::RIGHT_SUBSET || relation == QueryRelation::EQUAL)) {
        relation = QueryRelation::RIGHT_SUBSET;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      } else if (match_count == second_count && match_count < first_count &&
                 (relation == QueryRelation::LEFT_SUBSET || relation == QueryRelation::EQUAL)) {
        relation = QueryRelation::LEFT_SUBSET;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      }
    }

    // check window function exprs
    if (OB_SUCC(ret) && QueryRelation::UNCOMPARABLE != relation) {
      first_count = first_sel->get_window_func_count();
      second_count = second_sel->get_window_func_count();
      if (0 == first_count && 0 == second_count) {
        /*do nothing*/
      } else if (relation != QueryRelation::EQUAL) {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check window function map", K(relation), K(map_info));
      } else {
        LOG_TRACE("succeed to check window function map", K(relation), K(map_info));
      }
    }

    // check distinct exprs
    if (OB_SUCC(ret) && QueryRelation::UNCOMPARABLE != relation) {
      if ((!first_sel->has_distinct() && !second_sel->has_distinct()) ||
          (first_sel->has_distinct() && second_sel->has_distinct())) {
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      } else if (first_sel->has_distinct() && !second_sel->has_distinct() &&
                 (relation == QueryRelation::LEFT_SUBSET || relation == QueryRelation::EQUAL)) {
        relation = QueryRelation::LEFT_SUBSET;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      } else if (!first_sel->has_distinct() && second_sel->has_distinct() &&
                 (relation == QueryRelation::RIGHT_SUBSET || relation == QueryRelation::EQUAL)) {
        relation = QueryRelation::RIGHT_SUBSET;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      }
    }

    // check limit exprs
    if (OB_SUCC(ret) && QueryRelation::UNCOMPARABLE != relation) {
      if (!first_sel->has_limit() && !second_sel->has_limit()) {
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else if (first_sel->has_limit() && !second_sel->has_limit() &&
                 (relation == QueryRelation::LEFT_SUBSET || relation == QueryRelation::EQUAL)) {
        relation = QueryRelation::LEFT_SUBSET;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else if (!first_sel->has_limit() && second_sel->has_limit() &&
                 (relation == QueryRelation::RIGHT_SUBSET || relation == QueryRelation::EQUAL)) {
        relation = QueryRelation::RIGHT_SUBSET;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else {
        relation = QueryRelation::UNCOMPARABLE;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      }
    }

    // compute map for select items output
    if (OB_SUCC(ret) && QueryRelation::UNCOMPARABLE != relation) {
      ObSEArray<ObRawExpr*, 16> first_exprs;
      ObSEArray<ObRawExpr*, 16> second_exprs;
      if (OB_FAIL(first_sel->get_select_exprs(first_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(second_sel->get_select_exprs(second_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(compute_conditions_map(first_sel,
                     second_sel,
                     first_exprs,
                     second_exprs,
                     map_info,
                     map_info.select_item_map_,
                     match_count))) {
        LOG_WARN("failed to compute output expr map", K(ret));
      } else {
        LOG_TRACE("succeed to check stmt containment", K(relation), K(map_info));
      }
    }
  }
  return ret;
}

int ObStmtComparer::compute_from_items_map(
    ObDMLStmt* first, ObDMLStmt* second, ObStmtMapInfo& map_info, int64_t& match_count)
{
  int ret = OB_SUCCESS;
  match_count = 0;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (OB_FAIL(map_info.table_map_.prepare_allocate(first->get_table_size()))) {
    LOG_WARN("failed to pre-allocate table map", K(ret));
  } else if (OB_FAIL(map_info.from_map_.prepare_allocate(first->get_from_item_size()))) {
    LOG_WARN("failed to pre-allocate from map", K(ret));
  } else {
    ObSqlBitSet<> matched_items;
    for (int64_t i = 0; i < map_info.table_map_.count(); ++i) {
      map_info.table_map_.at(i) = OB_INVALID_ID;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < first->get_from_item_size(); ++i) {
      const FromItem& first_from = first->get_from_item(i);
      bool is_match = false;
      map_info.from_map_.at(i) = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && !is_match && j < second->get_from_item_size(); ++j) {
        const FromItem& second_from = second->get_from_item(j);
        if (matched_items.has_member(j)) {
          // do nothing
        } else if (OB_FAIL(is_same_from(first, first_from, second, second_from, is_match))) {
          LOG_WARN("failed to check the from item same", K(ret));
        } else if (!is_match) {
          // do nothing
        } else if (OB_FAIL(matched_items.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else {
          match_count++;
          map_info.from_map_.at(i) = j;
          const int32_t first_table_index = first->get_table_bit_index(first_from.table_id_);
          const int32_t second_table_index = second->get_table_bit_index(second_from.table_id_);
          if (OB_INVALID_ID == first_table_index || OB_INVALID_ID == second_table_index) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table bit index is invalid", K(ret), K(first_table_index), K(second_table_index));
          } else {
            map_info.table_map_.at(first_table_index - 1) = second_table_index - 1;
          }
        }
      }
    }
  }
  return ret;
}

int ObStmtComparer::compute_conditions_map(ObDMLStmt* first, ObDMLStmt* second, const ObIArray<ObRawExpr*>& first_exprs,
    const ObIArray<ObRawExpr*>& second_exprs, ObStmtMapInfo& map_info, ObIArray<int64_t>& condition_map,
    int64_t& match_count)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> matched_items;
  ObStmtCompareContext context;
  match_count = 0;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (OB_FAIL(context.init(first, second, map_info.table_map_))) {
    LOG_WARN("failed to set table map", K(ret));
  } else if (OB_FAIL(condition_map.prepare_allocate(first_exprs.count()))) {
    LOG_WARN("failed to preallocate array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < first_exprs.count(); ++i) {
      bool is_match = false;
      condition_map.at(i) = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && !is_match && j < second_exprs.count(); ++j) {
        if (matched_items.has_member(j)) {
          // do nothing
        } else if (OB_FAIL(is_same_condition(first_exprs.at(i), second_exprs.at(j), context, is_match))) {
          LOG_WARN("failed to check is condition equal", K(ret));
        } else if (!is_match) {
          // do nothing
        } else if (OB_FAIL(append(map_info.equal_param_map_, context.equal_param_info_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(matched_items.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else {
          match_count++;
          condition_map.at(i) = j;
        }
      }
    }
  }
  return ret;
}

int ObStmtComparer::is_same_condition(ObRawExpr* left, ObRawExpr* right, ObStmtCompareContext& context, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  context.equal_param_info_.reset();
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!(is_same = left->same_as(*right, &context))) {
    context.equal_param_info_.reset();
    if (!IS_COMMON_COMPARISON_OP(left->get_expr_type()) ||
        get_opposite_compare_type(left->get_expr_type()) != right->get_expr_type()) {
      // do nothing
    } else if (OB_ISNULL(left->get_param_expr(0)) || OB_ISNULL(left->get_param_expr(1)) ||
               OB_ISNULL(right->get_param_expr(0)) || OB_ISNULL(right->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param exprs are null", K(ret));
    } else if (!left->get_param_expr(0)->same_as(*right->get_param_expr(1), &context)) {
      // do nothing
    } else if (!left->get_param_expr(1)->same_as(*right->get_param_expr(0), &context)) {
      // do nothing
    } else {
      is_same = true;
    }
  }
  return ret;
}

int ObStmtComparer::compare_basic_table_item(ObDMLStmt* first, const TableItem* first_table, ObDMLStmt* second,
    const TableItem* second_table, QueryRelation& relation)
{
  int ret = OB_SUCCESS;
  relation = QueryRelation::UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(first_table) || OB_ISNULL(second) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(first), K(first_table), K(second), K(second_table));
  } else if (first_table->is_basic_table() && second_table->is_basic_table() &&
             first_table->ref_id_ == second_table->ref_id_) {
    const ObPartHint* first_table_hint = first->get_stmt_hint().get_part_hint(first_table->table_id_);
    const ObPartHint* second_table_hint = second->get_stmt_hint().get_part_hint(second_table->table_id_);
    if (OB_ISNULL(first_table_hint) && OB_ISNULL(second_table_hint)) {
      relation = QueryRelation::EQUAL;
    } else if (OB_ISNULL(first_table_hint)) {
      relation = QueryRelation::RIGHT_SUBSET;
    } else if (OB_ISNULL(second_table_hint)) {
      relation = QueryRelation::LEFT_SUBSET;
    } else {
      ObSqlBitSet<> first_set, second_set;
      for (int64_t i = 0; i < first_table_hint->part_ids_.count(); ++i) {
        first_set.add_member(first_table_hint->part_ids_.at(i));
      }
      for (int64_t i = 0; i < second_table_hint->part_ids_.count(); ++i) {
        second_set.add_member(second_table_hint->part_ids_.at(i));
      }
      if (first_set.equal(second_set)) {
        relation = QueryRelation::EQUAL;
      } else if (first_set.is_subset(second_set)) {
        relation = QueryRelation::LEFT_SUBSET;
      } else if (first_set.is_superset(second_set)) {
        relation = QueryRelation::RIGHT_SUBSET;
      } else {
        relation = QueryRelation::UNCOMPARABLE;
      }
    }
  } else if (first_table->is_generated_table() && second_table->is_generated_table() &&
             first_table->ref_query_ == second_table->ref_query_) {
    relation = QueryRelation::EQUAL;
  } else {
    /*do nothing*/
  }
  return ret;
}
