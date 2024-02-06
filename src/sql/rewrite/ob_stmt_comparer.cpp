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
#include "common/ob_smart_call.h"


using namespace oceanbase::sql;

void ObStmtMapInfo::reset()
{
  table_map_.reset();
  is_table_equal_ = false;
  from_map_.reset();
  is_from_equal_ = false;
  semi_info_map_.reset();
  is_semi_info_equal_ = false;
  cond_map_.reset();
  is_cond_equal_ = false;
  group_map_.reset();
  is_group_equal_ = false;
  having_map_.reset();
  is_having_equal_ = false;
  is_order_equal_ = false;
  select_item_map_.reset();
  is_select_item_equal_ = false;
  is_distinct_equal_ = false;
  equal_param_map_.reset();
  view_select_item_map_.reset();
}

int ObStmtMapInfo::assign(const ObStmtMapInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_map_.assign(other.table_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(from_map_.assign(other.from_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(semi_info_map_.assign(other.semi_info_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(cond_map_.assign(other.cond_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(group_map_.assign(other.group_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(having_map_.assign(other.having_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(select_item_map_.assign(other.select_item_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(equal_param_map_.assign(other.equal_param_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else if (OB_FAIL(view_select_item_map_.assign(other.view_select_item_map_))) {
    LOG_WARN("failed to assign table map", K(ret));
  } else {
    is_table_equal_ = other.is_table_equal_;
    is_from_equal_ = other.is_from_equal_;
    is_semi_info_equal_ = other.is_semi_info_equal_;
    is_cond_equal_ = other.is_cond_equal_;
    is_group_equal_ = other.is_group_equal_;
    is_having_equal_ = other.is_having_equal_;
    is_order_equal_ = other.is_order_equal_;
    is_select_item_equal_ = other.is_select_item_equal_;
    is_distinct_equal_ = other.is_distinct_equal_;
  }
  return ret;
}

int StmtCompareHelper::alloc_compare_helper(ObIAllocator &allocator, StmtCompareHelper* &helper)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(StmtCompareHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret));
  } else {
    helper = new(buf)StmtCompareHelper();
  }
  return ret;
}

void ObStmtCompareContext::init(const ObIArray<ObHiddenColumnItem> *calculable_items)
{
  calculable_items_ = calculable_items;
}

void ObStmtCompareContext::init(const ObDMLStmt *inner,
                                const ObDMLStmt *outer,
                                const ObStmtMapInfo &map_info,
                                const ObIArray<ObHiddenColumnItem> *calculable_items)
{
  inner_ = inner;
  outer_ = outer;
  map_info_ = map_info;
  calculable_items_ = calculable_items;
}

int ObStmtCompareContext::get_table_map_idx(uint64_t l_table_id, uint64_t r_table_id)
{
  int ret = OB_SUCCESS;
  int64_t ret_idx = OB_INVALID_ID;
  if (OB_ISNULL(inner_) || OB_ISNULL(outer_)) {
    ret = OB_ERR_UNEXPECTED;
  }
  TableItem *inner_table = NULL;
  TableItem *outer_table = NULL;
  for (int64_t inner_idx = 0; OB_SUCC(ret) && ret_idx == OB_INVALID_ID 
                && inner_idx < map_info_.table_map_.count(); ++inner_idx) {
    int64_t outer_idx = map_info_.table_map_.at(inner_idx);
    if (outer_idx < 0 || outer_idx >= outer_->get_table_size()
        || inner_idx >= inner_->get_table_size()) {
      // do nothing
    } else if (OB_ISNULL(inner_table = inner_->get_table_items().at(inner_idx)) ||
               OB_ISNULL(outer_table = outer_->get_table_items().at(outer_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table items are null", K(ret));
    } else if ((inner_table->table_id_ == l_table_id && outer_table->table_id_ == r_table_id) ||
              (inner_table->table_id_ == r_table_id && outer_table->table_id_ == l_table_id)) {
      ret_idx = inner_idx;          
    }
  }
  return ret_idx;
}

bool ObStmtCompareContext::compare_column(const ObColumnRefRawExpr &inner,
                                          const ObColumnRefRawExpr &outer)
{
  bool bret = false;
  int idx = get_table_map_idx(inner.get_table_id(), outer.get_table_id());
  if (inner.get_table_id() == outer.get_table_id()) {
    bret = inner.get_column_id() == outer.get_column_id();
  } else if (idx == OB_INVALID_ID) {
    //do nothing
  } else if (idx >= map_info_.view_select_item_map_.count()) {
    //do nothing
  } else if (OB_ISNULL(inner_)) {
  } else if (!inner_->get_table_item_by_id(inner.get_table_id())->is_generated_table()) {
    bret = inner.get_column_id() == outer.get_column_id();
  } else {
    int64_t inner_pos = inner.get_column_id() - OB_APP_MIN_COLUMN_ID;
    int64_t outer_pos = outer.get_column_id() - OB_APP_MIN_COLUMN_ID;
    const ObIArray<int64_t> &select_item_map = map_info_.view_select_item_map_.at(idx);
    if (OB_UNLIKELY(inner_pos < 0 || inner_pos >= select_item_map.count()) ||
        OB_INVALID_ID == select_item_map.at(inner_pos)) {
      //do nothing
    } else {
      bret = select_item_map.at(inner_pos) == outer_pos;
    }
  }
  return bret;
}

bool ObStmtCompareContext::compare_const(const ObConstRawExpr &left, const ObConstRawExpr &right)
{
  int &ret = err_code_;
  bool bret = false;
  if (OB_SUCC(ret) && left.get_result_type() == right.get_result_type()) {
    if (&left == &right) {
      bret = true;
    } else if (left.is_param_expr() && right.is_param_expr()) {
      bool is_left_calc_item = false;
      bool is_right_calc_item = false;
      if (OB_FAIL(is_pre_calc_item(left, is_left_calc_item))) {
        LOG_WARN("failed to is pre calc item", K(ret));
      } else if (OB_FAIL(is_pre_calc_item(right, is_right_calc_item))) {
        LOG_WARN("failed to is pre calc item", K(ret));
      } else if (is_left_calc_item && is_right_calc_item) {
        const ObRawExpr *left_param = NULL;
        const ObRawExpr *right_param = NULL;
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
      } else if (ignore_param_) {
        bret = ObExprEqualCheckContext::compare_const(left, right);
      } else if (left.get_result_type().get_param().is_equal(
                   right.get_result_type().get_param(), CS_TYPE_BINARY)) {
        ObPCParamEqualInfo info;
        info.first_param_idx_ = left.get_value().get_unknown();
        info.second_param_idx_ = right.get_value().get_unknown();
        bret = true;
        if (info.first_param_idx_ != info.second_param_idx_ &&
            OB_FAIL(equal_param_info_.push_back(info))) {
          LOG_WARN("failed to push back equal param info", K(ret));
        }
      }
    } else if (left.is_param_expr() || right.is_param_expr()) {
      bret = ObExprEqualCheckContext::compare_const(left, right);
    } else {
      bret = left.get_value().is_equal(right.get_value(), CS_TYPE_BINARY);
    }
  }
  return bret;
}

int ObStmtCompareContext::is_pre_calc_item(const ObConstRawExpr &const_expr, bool &is_calc)
{
  int ret = OB_SUCCESS;
  int64_t calc_count = 0;
  is_calc = false;
  if (OB_ISNULL(calculable_items_) || OB_UNLIKELY((calc_count = calculable_items_->count()) < 0
      || const_expr.get_expr_type() != T_QUESTIONMARK)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(calculable_items_), K(const_expr.get_expr_type()),
                                     K(calc_count));
  } else if (const_expr.has_flag(IS_DYNAMIC_PARAM)) {
    is_calc = true;
  } else if (calc_count > 0) {
    int64_t q_idx = const_expr.get_value().get_unknown();
    int64_t min_calc_index = calculable_items_->at(0).hidden_idx_;
    if (OB_UNLIKELY(q_idx < 0 || min_calc_index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid argument", K(q_idx), K(min_calc_index));
    } else if (q_idx - min_calc_index >= 0 && q_idx - min_calc_index < calc_count) {
      is_calc = true;
    } else {/*do nothing*/}
  }
  return ret;
}

bool ObStmtCompareContext::compare_query(const ObQueryRefRawExpr &first,
                                         const ObQueryRefRawExpr &second)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  ObStmtMapInfo stmt_map_info;
  QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (&first == &second) {
    bret = true;
  } else if (first.is_set() != second.is_set() || first.is_multiset() != second.is_multiset()) {
    bret = false;
  } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(first.get_ref_stmt(),
                                                            second.get_ref_stmt(),
                                                            stmt_map_info,
                                                            relation))) {
    LOG_WARN("failed to compute stmt relationship", K(ret));
    err_code_ = ret;
  } else if (stmt_map_info.is_select_item_equal_ && QueryRelation::QUERY_EQUAL == relation) {
    bret = true;
    if (OB_FAIL(append(equal_param_info_, stmt_map_info.equal_param_map_))) {
      LOG_WARN("failed to append equal param", K(ret));
      err_code_ = ret;
    }
  }
  return bret;
}

int ObStmtCompareContext::get_calc_expr(const int64_t param_idx, const ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calculable_items_) || calculable_items_->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query context is null", K(ret));
  } else {
    int64_t offset = param_idx - calculable_items_->at(0).hidden_idx_;
    if (offset < 0 || offset >= calculable_items_->count() ||
        param_idx != calculable_items_->at(offset).hidden_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param index", K(ret), K(param_idx), K(offset));
    } else {
      expr = calculable_items_->at(offset).expr_;
    }
  }
  return ret;
}

bool ObStmtCompareContext::compare_set_op_expr(const ObSetOpRawExpr& left, const ObSetOpRawExpr& right)
{
  bool bret = false;
  int64_t pos = left.get_idx();
  if (OB_UNLIKELY(pos < 0 || pos >= map_info_.select_item_map_.count()) ||
      OB_INVALID_ID == map_info_.select_item_map_.at(pos)) {
    //do nothing
  } else {
    bret = map_info_.select_item_map_.at(pos) == right.get_idx();
  }
  return bret;
}

int ObStmtComparer::compute_stmt_overlap(const ObDMLStmt *first,
                                         const ObDMLStmt *second,
                                         ObStmtMapInfo &map_info)
{
  int ret = OB_SUCCESS;
  int64_t match_count = 0;
  map_info.reset();
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts are null", K(ret), K(first), K(second));
  } else if (OB_FAIL(compute_from_items_map(first,
                                            second,
                                            map_info,
                                            match_count))) {
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
 *  比较两个 From 项是否相同
 * @return
 */
int ObStmtComparer::is_same_from(const ObDMLStmt *first,
                                 const FromItem &first_from,
                                 const ObDMLStmt *second,
                                 const FromItem &second_from,
                                 ObStmtMapInfo &map_info,
                                 bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts have null", K(ret), K(first), K(second));
  } else if (first_from.is_joined_ && second_from.is_joined_) {
    QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
    if (OB_FAIL(compare_joined_table_item(first,
                                          first->get_joined_table(first_from.table_id_),
                                          second,
                                          second->get_joined_table(second_from.table_id_),
                                          map_info,
                                          relation))) {
      LOG_WARN(" compare joined table item failed", K(ret));
    } else if (QueryRelation::QUERY_EQUAL == relation) {
      is_same = true;
    } else {
      /*do noting*/
    }
  } else if (!first_from.is_joined_ && !second_from.is_joined_) {
    QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
    if (OB_FAIL(compare_table_item(first,
                                  first->get_table_item_by_id(first_from.table_id_),
                                  second,
                                  second->get_table_item_by_id(second_from.table_id_),
                                  map_info,
                                  relation))) {
      LOG_WARN(" compare table item failed", K(ret));
    } else if (QueryRelation::QUERY_EQUAL == relation) {
      is_same = true;
    } else {
      /*do noting*/
    }
  }
  return ret;
}

int ObStmtComparer::check_stmt_containment(const ObDMLStmt *first,
                                           const ObDMLStmt *second,
                                           ObStmtMapInfo &map_info,
                                           QueryRelation &relation)
{
  int ret = OB_SUCCESS;
  int64_t first_count = 0;
  int64_t second_count = 0;
  int64_t match_count = 0;
  ObSelectStmt *first_sel = NULL;
  ObSelectStmt *second_sel = NULL;
  relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (!first->is_select_stmt() || !second->is_select_stmt()) {
    /*do nothing*/
  } else if (FALSE_IT(first_sel = const_cast<ObSelectStmt*>(static_cast<const ObSelectStmt*>(first)))) {
    /*do nothing*/
  } else if (FALSE_IT(second_sel = const_cast<ObSelectStmt*>(static_cast<const ObSelectStmt*>(second)))) {
    /*do nothing*/
  } else if (first_sel->has_recursive_cte() || second_sel->has_recursive_cte() ||
             first_sel->has_hierarchical_query() || second_sel->has_hierarchical_query() ||
             first_sel->is_contains_assignment() || second_sel->is_contains_assignment()) {
    /*do nothing*/
  } else if (first_sel->is_set_stmt() && second_sel->is_set_stmt()) {
    if (OB_FAIL(compare_set_stmt(first_sel, second_sel, map_info, relation))) {
      LOG_WARN("failed to compare set stmt", K(ret));
    }
  } else if (first_sel->is_set_stmt() || second_sel->is_set_stmt()) {
    /*do nothing*/
  } else if (first_sel->get_from_item_size() != second_sel->get_from_item_size()) {
    /*do nothing*/
  } else {
    // check from items
    if (OB_FAIL(compute_from_items_map(first_sel,
                                       second_sel,
                                       map_info,
                                       match_count))) {
      LOG_WARN("failed to compute from items map", K(ret));
    } else if (match_count != first_sel->get_from_item_size()) {
      relation = QueryRelation::QUERY_UNCOMPARABLE;
      LOG_TRACE("succeed to check from item map", K(relation), K(map_info));
    } else {
      relation = QueryRelation::QUERY_EQUAL;
      map_info.is_table_equal_ = true;
      map_info.is_from_equal_ = true;
      LOG_TRACE("succeed to check from item map", K(relation), K(map_info));
    }

    //check semi infos
    if (OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == relation) {
      first_count = first_sel->get_semi_info_size();
      second_count = second_sel->get_semi_info_size();
      if (OB_FAIL(compute_semi_infos_map(first_sel,
                                        second_sel,
                                        map_info,
                                        match_count))) {
        LOG_WARN("failed to compute semi info map", K(ret));
      } else if (match_count == first_count && match_count == second_count) {
        relation = QueryRelation::QUERY_EQUAL;
        map_info.is_semi_info_equal_ = true;
        LOG_TRACE("succeed to check semi info map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check semi info map", K(relation), K(map_info));
      }
    }

    // check condition exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == relation) {
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
        relation = QueryRelation::QUERY_EQUAL;
        map_info.is_cond_equal_ = true;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      } else if (match_count == first_count && match_count < second_count) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      } else if (match_count < first_count && match_count == second_count) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check conditions map", K(relation), K(map_info));
      }
    }

    // check group by exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      bool is_consistent = false;
      first_count = first_sel->get_group_exprs().count();
      second_count = second_sel->get_group_exprs().count();
      int64_t first_rollup_count = first_sel->get_rollup_exprs().count();
      int64_t second_rollup_count = second_sel->get_rollup_exprs().count();
      if ((first_sel->get_aggr_item_size() > 0 ||
           second_sel->get_aggr_item_size() > 0)
           && relation != QueryRelation::QUERY_EQUAL) {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      } else if (first_rollup_count != second_rollup_count ||
                 first_count != second_count) {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      } else if (0 == first_count && 0 == second_count &&
                 0 == first_rollup_count && 0 == second_rollup_count) {
        map_info.is_group_equal_ = true;
      } else if (OB_FAIL(ObTransformUtils::check_group_by_consistent(first_sel,
                                                                     is_consistent))) {
        LOG_WARN("failed to check whether group by is consistent", K(ret));
      } else if (!is_consistent) {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
      } else if (OB_FAIL(ObTransformUtils::check_group_by_consistent(second_sel,
                                                                     is_consistent))) {
        LOG_WARN("failed to check whether group by is consistent", K(ret));
      } else if (!is_consistent) {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
      } else if (OB_FAIL(map_info.group_map_.prepare_allocate(first_count))) {
        LOG_WARN("failed to allcoate group by map", K(ret));
      } else if (OB_FAIL(compute_conditions_map(first_sel,
                                                second_sel,
                                                first_sel->get_group_exprs(),
                                                second_sel->get_group_exprs(),
                                                map_info,
                                                map_info.group_map_,
                                                match_count))) {
        LOG_WARN("failed to compute group by expr map", K(ret));
      } else if (match_count != first_count || match_count != second_count) {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      } else if (first_rollup_count != 0) {
        ObStmtCompareContext context(first_sel, second_sel, map_info, &first_sel->get_query_ctx()->calculable_items_);
        bool rollup_match = true;
        for (int64_t i = 0; OB_SUCC(ret) && rollup_match && i < first_rollup_count; i++) {
          if (OB_FAIL(is_same_condition(first_sel->get_rollup_exprs().at(i),
                                        second_sel->get_rollup_exprs().at(i),
                                        context,
                                        rollup_match))) {
            LOG_WARN("failed to check is condition equal", K(ret));
          }
        }
        map_info.is_group_equal_ = rollup_match;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      } else {
        // todo @guoping.wgp we can do better to check containment relationship for group by clause
        map_info.is_group_equal_ = true;
        LOG_TRACE("succeed to check group by map", K(relation), K(map_info));
      }
    }

    // check having exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      first_count = first_sel->get_having_expr_size();
      second_count = second_sel->get_having_expr_size();
      if (0 == first_count && 0 == second_count) {
        map_info.is_having_equal_ = true;
      } else if (OB_FAIL(compute_conditions_map(first_sel,
                                                second_sel,
                                                first_sel->get_having_exprs(),
                                                second_sel->get_having_exprs(),
                                                map_info,
                                                map_info.having_map_,
                                                match_count))) {
        LOG_WARN("failed to compute having expr map", K(ret));
      } else if (match_count == first_count && match_count == second_count) {
        map_info.is_having_equal_ = true;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      } else if (match_count == first_count && match_count < second_count &&
                 (relation == QueryRelation::QUERY_RIGHT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      } else if (match_count == second_count && match_count < first_count &&
                 (relation == QueryRelation::QUERY_LEFT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check having map", K(relation), K(map_info));
      }
    }

    // check window function exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      first_count = first_sel->get_window_func_count();
      second_count = second_sel->get_window_func_count();
      if (0 == first_count && 0 == second_count) {
        /*do nothing*/
      } else if (relation != QueryRelation::QUERY_EQUAL) {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check window function map", K(relation), K(map_info));
      } else {
       LOG_TRACE("succeed to check window function map", K(relation), K(map_info));
      }
    }

    // check distinct exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      if ((!first_sel->has_distinct() && !second_sel->has_distinct()) ||
          (first_sel->has_distinct() && second_sel->has_distinct())) {
            map_info.is_distinct_equal_ = true;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      } else if (first_sel->has_distinct() && !second_sel->has_distinct() &&
                 (relation == QueryRelation::QUERY_LEFT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      } else if (!first_sel->has_distinct() && second_sel->has_distinct() &&
                 (relation == QueryRelation::QUERY_RIGHT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check distinct expr map", K(relation), K(map_info));
      }
    }

    // check order by exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      first_count = first->get_order_item_size();
      second_count = second->get_order_item_size();
      if (0 == first_count && 0 == second_count) {
        map_info.is_order_equal_ = true;
      } else if (first_count != second_count) {
        // do nothing
      } else if (OB_FAIL(compute_orderby_map(first_sel,
                                             second_sel,
                                             first_sel->get_order_items(),
                                             second_sel->get_order_items(),
                                             map_info,
                                             match_count))) {
        LOG_WARN("failed to compute order item map", K(ret));
      } else if (match_count == first_count && match_count == second_count) {
        map_info.is_order_equal_ = true;
        LOG_TRACE("succeed to check order item map", K(relation), K(map_info));
      } else {
        // The Order Item relation does not afftect the query relation
        LOG_TRACE("succeed to check order item map", K(relation), K(map_info));
      }
    }

    // check limit exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      if (!first_sel->has_limit() && !second_sel->has_limit()) {
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else if (first_sel->has_limit() && !second_sel->has_limit() &&
                 (relation == QueryRelation::QUERY_LEFT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else if (!first_sel->has_limit() && second_sel->has_limit() &&
                 (relation == QueryRelation::QUERY_RIGHT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      }
    }

    // compute map for select items output
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
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
      } else if (match_count == first_exprs.count() && match_count == second_exprs.count()) {
        map_info.is_select_item_equal_ = true;
        LOG_TRACE("succeed to check select item map", K(relation), K(map_info));
      } else {
        LOG_TRACE("succeed to check stmt containment", K(relation), K(map_info));
      }
    }
  }
  return ret;
}

int ObStmtComparer::compute_from_items_map(const ObDMLStmt *first,
                                           const ObDMLStmt *second,
                                           ObStmtMapInfo &map_info,
                                           int64_t &match_count)
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
  } else if (OB_FAIL(map_info.view_select_item_map_.prepare_allocate(first->get_table_size()))) {
    LOG_WARN("failed to pre-allocate generated table map", K(ret));
  } else {
    ObSqlBitSet<> matched_items;
    for (int64_t i = 0; i < map_info.table_map_.count(); ++i) {
      map_info.table_map_.at(i) = OB_INVALID_ID;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < first->get_from_item_size(); ++i) {
      const FromItem &first_from = first->get_from_item(i);
      bool is_match = false;
      map_info.from_map_.at(i) = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && !is_match && j < second->get_from_item_size(); ++j) {
        const FromItem &second_from = second->get_from_item(j);
        if (matched_items.has_member(j)) {
          // do nothing
        } else if (OB_FAIL(is_same_from(first,
                                        first_from,
                                        second,
                                        second_from,
                                        map_info,
                                        is_match))) {
          LOG_WARN("failed to check the from item same", K(ret));
        } else if (!is_match) {
          // do nothing
        } else if (OB_FAIL(matched_items.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else {
          match_count++;
          map_info.from_map_.at(i) = j;
        }
      }
    }
  }
  return ret;
}

int ObStmtComparer::compute_conditions_map(const ObDMLStmt *first,
                                           const ObDMLStmt *second,
                                           const ObIArray<ObRawExpr*> &first_exprs,
                                           const ObIArray<ObRawExpr*> &second_exprs,
                                           ObStmtMapInfo &map_info,
                                           ObIArray<int64_t> &condition_map,
                                           int64_t &match_count)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> matched_items;
  ObStmtCompareContext context(first, second, map_info, &first->get_query_ctx()->calculable_items_);
  match_count = 0;
  if (OB_ISNULL(first) || OB_ISNULL(second) || OB_ISNULL(first->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (OB_FAIL(condition_map.prepare_allocate(first_exprs.count()))) {
    LOG_WARN("failed to preallocate array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < first_exprs.count(); ++i) {
      bool is_match = false;
      condition_map.at(i) = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && !is_match && j < second_exprs.count(); ++j) {
        if (matched_items.has_member(j)) {
          // do nothing
        } else if (OB_FAIL(is_same_condition(first_exprs.at(i),
                                             second_exprs.at(j),
                                             context,
                                             is_match))) {
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

int ObStmtComparer::compute_orderby_map(const ObDMLStmt *first,
                                        const ObDMLStmt *second,
                                        const ObIArray<OrderItem> &first_orders,
                                        const ObIArray<OrderItem> &second_orders,
                                        ObStmtMapInfo &map_info,
                                        int64_t &match_count)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext context(first, second, map_info, &first->get_query_ctx()->calculable_items_);
  match_count = 0;
  bool first_match_all = true;
  if (OB_ISNULL(first) || OB_ISNULL(second) || OB_ISNULL(first->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && first_match_all && i < first_orders.count() && i < second_orders.count(); ++i) {
      bool is_match = false;
      if (first_orders.at(i).order_type_ != second_orders.at(i).order_type_) {
        first_match_all = false;
      } else if (OB_FAIL(is_same_condition(first_orders.at(i).expr_,
                                           second_orders.at(i).expr_,
                                           context,
                                           is_match))) {
        LOG_WARN("failed to check is condition equal", K(ret));
      } else if (!is_match) {
        first_match_all = false;
      } else if (OB_FAIL(append(map_info.equal_param_map_, context.equal_param_info_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else {
        match_count++;
      }
    }
  }
  return ret;
}

int ObStmtComparer::is_same_condition(const ObRawExpr *left,
                                      const ObRawExpr *right,
                                      ObStmtCompareContext &context,
                                      bool &is_same)
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
    } else if (OB_ISNULL(left->get_param_expr(0)) ||
               OB_ISNULL(left->get_param_expr(1)) ||
               OB_ISNULL(right->get_param_expr(0)) ||
               OB_ISNULL(right->get_param_expr(1))) {
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

int ObStmtComparer::compute_semi_infos_map(const ObDMLStmt *first,
                                          const ObDMLStmt *second,
                                          ObStmtMapInfo &map_info,
                                          int64_t &match_count)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> matched_items;
  match_count = 0;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (OB_FAIL(map_info.semi_info_map_.prepare_allocate(first->get_semi_info_size()))) {
    LOG_WARN("failed to pre-allocate generated table map", K(ret));
  } else {
    const ObIArray<SemiInfo*> &first_semi_infos = first->get_semi_infos();
    const ObIArray<SemiInfo*> &second_semi_infos = second->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < first_semi_infos.count(); ++i) {
      bool is_match = false;
      map_info.semi_info_map_.at(i) = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && !is_match && j < second_semi_infos.count(); ++j) {
        if (matched_items.has_member(j)) {
          // do nothing
        } else if (OB_FAIL(is_same_semi_info(first,
                                             first_semi_infos.at(i),
                                             second,
                                             second_semi_infos.at(j),
                                             map_info,
                                             is_match))) {
          LOG_WARN("failed to check is condition equal", K(ret));
        } else if (!is_match) {
          // do nothing
        } else if (OB_FAIL(matched_items.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else {
          match_count++;
          map_info.semi_info_map_.at(i) = j;
        }
      }
    }
  }
  return ret;
}

int ObStmtComparer::is_same_semi_info(const ObDMLStmt *first,
                                      const SemiInfo *first_semi_info,
                                      const ObDMLStmt *second,
                                      const SemiInfo *second_semi_info,
                                      ObStmtMapInfo &map_info,
                                      bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (OB_ISNULL(first) || OB_ISNULL(first_semi_info) ||
      OB_ISNULL(second) || OB_ISNULL(second_semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (first_semi_info->join_type_ != second_semi_info->join_type_) {
    //do nothing
  } else {
    int64_t match_count = 0;
    //check left tables
    ObSEArray<int64_t, 4> table_map;
    if (OB_FAIL(compute_tables_map(first,
                                   second,
                                   first_semi_info->left_table_ids_,
                                   second_semi_info->left_table_ids_,
                                   map_info,
                                   table_map,
                                   match_count))) {
      LOG_WARN("failed to compute tables map", K(ret));
    } else if (match_count == first_semi_info->left_table_ids_.count() && 
               match_count == second_semi_info->left_table_ids_.count()) {
      is_same = true;
    } else {
      is_same = false;
    }
    //check right table
    if (OB_SUCC(ret) && is_same) {
      const TableItem *first_table = first->get_table_item_by_id(first_semi_info->right_table_id_);
      const TableItem *second_table = second->get_table_item_by_id(second_semi_info->right_table_id_);
      QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
      if (OB_FAIL(compare_table_item(first,
                                     first_table,
                                     second,
                                     second_table,
                                     map_info,
                                     relation))) {
        LOG_WARN("failed to compare table item", K(ret));
      } else if (QueryRelation::QUERY_EQUAL == relation) {
        is_same = true;
      } else {
        is_same = false;
      }
    }
    //check semi condition
    if (OB_SUCC(ret) && is_same) {
      ObSEArray<int64_t, 4> condition_map;
      if (OB_FAIL(compute_conditions_map(first,
                                         second,
                                         first_semi_info->semi_conditions_,
                                         second_semi_info->semi_conditions_,
                                         map_info,
                                         condition_map,
                                         match_count))) {
        LOG_WARN("failed to compute conditions map", K(ret));
      } else if (match_count == first_semi_info->semi_conditions_.count() && 
                 match_count == second_semi_info->semi_conditions_.count()) {
        is_same = true;
      } else {
        is_same = false;
      }
    }
  }
  return ret;
}

int ObStmtComparer::compute_tables_map(const ObDMLStmt *first,
                                      const ObDMLStmt *second,
                                      const ObIArray<uint64_t> &first_table_ids,
                                      const ObIArray<uint64_t> &second_table_ids,
                                      ObStmtMapInfo &map_info,
                                      ObIArray<int64_t> &table_map,
                                      int64_t &match_count)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> matched_items;
  ObStmtCompareContext context(first, second, map_info, &first->get_query_ctx()->calculable_items_);
  match_count = 0;
  if (OB_ISNULL(first) || OB_ISNULL(second) || OB_ISNULL(first->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first), K(second), K(ret));
  } else if (OB_FAIL(table_map.prepare_allocate(first_table_ids.count()))) {
    LOG_WARN("failed to preallocate array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < first_table_ids.count(); ++i) {
      bool is_match = false;
      table_map.at(i) = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && !is_match && j < second_table_ids.count(); ++j) {
        if (matched_items.has_member(j)) {
          // do nothing
        } else if (context.get_table_map_idx(first_table_ids.at(i), second_table_ids.at(j)) == OB_INVALID_ID) {
          //do nothing
        } else if (OB_FAIL(matched_items.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else {
          match_count++;
          table_map.at(i) = j;
        }
      }
    }
  }
  return ret;
}


int ObStmtComparer::compare_basic_table_item(const ObDMLStmt *first,
                                            const TableItem *first_table,
                                            const ObDMLStmt *second,
                                            const TableItem *second_table,
                                            QueryRelation &relation)
{
  int ret = OB_SUCCESS;
  relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(first_table)
     || OB_ISNULL(second) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(first), K(first_table), K(second), K(second_table));
  } else if ((first_table->is_basic_table() || first_table->is_link_table()) &&
            (second_table->is_basic_table() || second_table->is_link_table()) &&
            first_table->ref_id_ == second_table->ref_id_ && 
            first_table->flashback_query_type_ == second_table->flashback_query_type_ &&
            (first_table->flashback_query_expr_ == second_table->flashback_query_expr_ ||
             first_table->flashback_query_expr_->same_as(*second_table->flashback_query_expr_))) {
    if (OB_LIKELY(first_table->access_all_part() && second_table->access_all_part())) {
      relation = QueryRelation::QUERY_EQUAL;
    } else if (first_table->access_all_part()) {
      relation = QueryRelation::QUERY_RIGHT_SUBSET;
    } else if (second_table->access_all_part()) {
      relation = QueryRelation::QUERY_LEFT_SUBSET;
    } else {
      // part ids for subpartition is a large int64_t number, here can not use bit set.
      bool left_subset = ObOptimizerUtil::is_subset(first_table->part_ids_,
                                                    second_table->part_ids_);
      bool right_subset = ObOptimizerUtil::is_subset(second_table->part_ids_,
                                                     first_table->part_ids_);
      if (left_subset && right_subset) {
        relation = QueryRelation::QUERY_EQUAL;
      } else if (left_subset) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
      } else if (right_subset) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
      }
    }
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObStmtComparer::compare_joined_table_item (const ObDMLStmt *first,
                                              const TableItem *first_table,
                                              const ObDMLStmt *second,
                                              const TableItem *second_table,
                                              ObStmtMapInfo &map_info,
                                              QueryRelation &relation)
{
  int ret = OB_SUCCESS;
  relation = QueryRelation::QUERY_UNCOMPARABLE;
  QueryRelation left_relation = QueryRelation::QUERY_UNCOMPARABLE;
  QueryRelation right_relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(first_table)
     || OB_ISNULL(second) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(first), K(first_table), K(second), K(second_table));
  } else if (first_table->is_joined_table() && second_table->is_joined_table()) {
    const JoinedTable *first_joined_table = static_cast<const JoinedTable *>(first_table);
    const JoinedTable *second_joined_table = static_cast<const JoinedTable *>(second_table);
    int64_t first_count = first_joined_table->join_conditions_.count();
    int64_t second_count = second_joined_table->join_conditions_.count();
    ObSEArray<int64_t, 4> condition_map;
    int64_t match_count = 0;
    if (first_joined_table->joined_type_ != second_joined_table->joined_type_) {
      /*do nothing*/
    } else if (OB_FAIL(SMART_CALL(compare_joined_table_item(first,
                                                            first_joined_table->left_table_,
                                                            second,
                                                            second_joined_table->left_table_,
                                                            map_info,
                                                            left_relation)))) {
      LOG_WARN("compare joined table failed", K(ret));                                                   
    } else if (OB_FAIL(SMART_CALL(compare_joined_table_item(first,
                                                            first_joined_table->right_table_,
                                                            second,
                                                            second_joined_table->right_table_,
                                                            map_info,
                                                            right_relation)))) {
      LOG_WARN("compare joined table failed", K(ret));                                                   
    } else if (left_relation != QueryRelation::QUERY_EQUAL ||
              right_relation != QueryRelation::QUERY_EQUAL) {
      if (first_joined_table->joined_type_ != INNER_JOIN && 
          first_joined_table->joined_type_ != FULL_OUTER_JOIN) {
        //inner join、full outer join需要交换左右表检查          
      } else if (OB_FAIL(SMART_CALL(compare_joined_table_item(first,
                                                              first_joined_table->left_table_,
                                                              second,
                                                              second_joined_table->right_table_,
                                                              map_info,
                                                              left_relation)))) {
        LOG_WARN("compare joined table failed", K(ret));                                                   
      } else if (OB_FAIL(SMART_CALL(compare_joined_table_item(first,
                                                              first_joined_table->right_table_,
                                                              second,
                                                              second_joined_table->left_table_,
                                                              map_info,
                                                              right_relation)))) {
        LOG_WARN("compare joined table failed", K(ret));                                                   
      }
    } else {/*do nothing*/}
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (left_relation != QueryRelation::QUERY_EQUAL ||
              right_relation != QueryRelation::QUERY_EQUAL) {
      //do nothing
    } else if (OB_FAIL(compute_conditions_map(first,
                                              second,
                                              first_joined_table->join_conditions_,
                                              second_joined_table->join_conditions_,
                                              map_info,
                                              condition_map,
                                              match_count))) {
      LOG_WARN("failed to compute conditions map", K(ret));
    } else if (match_count != first_count || match_count != second_count) {
      //on condition不相同
    } else {
      relation = QueryRelation::QUERY_EQUAL;
    }
  } else if (OB_FAIL(compare_table_item(first,
                                        first_table,
                                        second,
                                        second_table,
                                        map_info,
                                        relation))) {
    LOG_WARN("failed to compare table item", K(ret));
  }
  return ret;
}

int ObStmtComparer::compare_table_item(const ObDMLStmt *first,
                                        const TableItem *first_table,
                                        const ObDMLStmt *second,
                                        const TableItem *second_table,
                                        ObStmtMapInfo &map_info,
                                        QueryRelation &relation)
{
  int ret = OB_SUCCESS;
  relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(second) ||
      OB_ISNULL(first_table) || OB_ISNULL(second_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts have null", K(ret), K(first), K(second));
  } else if (map_info.table_map_.count() < first->get_table_size() &&
             OB_FAIL(map_info.table_map_.prepare_allocate(first->get_table_size()))) {
    LOG_WARN("failed to pre-allocate table map", K(ret));
  } else if (map_info.view_select_item_map_.count() < first->get_table_size() &&
             OB_FAIL(map_info.view_select_item_map_.prepare_allocate(first->get_table_size()))) {
    LOG_WARN("failed to pre-allocate generated table map", K(ret));
  } else if (first_table->is_temp_table() && second_table->is_temp_table()) {
    if (first_table->ref_query_ == second_table->ref_query_) {
      relation = QueryRelation::QUERY_EQUAL;
      const int32_t first_table_index = first->get_table_bit_index(first_table->table_id_);
      const int32_t second_table_index = second->get_table_bit_index(second_table->table_id_);
      if (first_table_index < 1 || first_table_index > first->get_table_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect table bit index", K(ret));
      } else {
        map_info.table_map_.at(first_table_index - 1) = second_table_index - 1;
      }
    } else {
      relation = QueryRelation::QUERY_UNCOMPARABLE;
    }
  } else if ((first_table->is_basic_table() || first_table->is_link_table()) &&
            (second_table->is_basic_table() || second_table->is_link_table())) {
    if (OB_FAIL(compare_basic_table_item(first, 
                                        first_table, 
                                        second, 
                                        second_table, 
                                        relation))) {
      LOG_WARN("compare table part failed",K(ret), K(first_table), K(second_table));
    } else if (QueryRelation::QUERY_UNCOMPARABLE != relation) {
      const int32_t first_table_index = first->get_table_bit_index(first_table->table_id_);
      const int32_t second_table_index = second->get_table_bit_index(second_table->table_id_);
      if (first_table_index < 1 || first_table_index > first->get_table_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect table bit index", K(ret));
      } else {
        map_info.table_map_.at(first_table_index - 1) = second_table_index - 1;
      }
    }
  //TODO:jiangxiu.wt 后续打开flashback query针对view和generated table的支持，这里需要处理
  } else if (first_table->is_generated_table() &&
             second_table->is_generated_table()) {
    ObStmtMapInfo ref_query_map_info;
    const int32_t first_table_index = first->get_table_bit_index(first_table->table_id_);
    const int32_t second_table_index = second->get_table_bit_index(second_table->table_id_);
    if (first_table_index < 1 || first_table_index > first->get_table_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect table bit index", K(ret));
    } else if (OB_FAIL(SMART_CALL(check_stmt_containment(first_table->ref_query_,
                                                         second_table->ref_query_,
                                                         ref_query_map_info,
                                                         relation)))) {
      LOG_WARN("check stmt containment failed", K(ret));
    } else if (OB_FAIL(map_info.view_select_item_map_.at(first_table_index - 1).assign(ref_query_map_info.select_item_map_))) {
      LOG_WARN("failed to assign select item map", K(ret));
    } else if (QueryRelation::QUERY_UNCOMPARABLE != relation) {
      if (ref_query_map_info.is_select_item_equal_ && QueryRelation::QUERY_EQUAL == relation) {
        map_info.table_map_.at(first_table_index - 1) = second_table_index - 1;
        if (OB_FAIL(append(map_info.equal_param_map_, ref_query_map_info.equal_param_map_))) {
          LOG_WARN("failed to append equal param", K(ret));
        }
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
      }
    }
  } else if (first_table->is_joined_table() &&
             second_table->is_joined_table()) {
    if (OB_FAIL(compare_joined_table_item(first,
                                          first_table,
                                          second,
                                          second_table,
                                          map_info,
                                          relation))) {
      LOG_WARN("failed to compare joined table item", K(ret));
    }
  } else if (first_table->is_values_table() && second_table->is_values_table()) {
    if (OB_FAIL(compare_values_table_item(first,
                                          first_table,
                                          second,
                                          second_table,
                                          map_info,
                                          relation))) {
      LOG_WARN("compare values table failed",K(ret), K(first_table), K(second_table));
    } else if (QueryRelation::QUERY_UNCOMPARABLE != relation) {
      const int32_t first_table_index = first->get_table_bit_index(first_table->table_id_);
      const int32_t second_table_index = second->get_table_bit_index(second_table->table_id_);
      if (first_table_index < 1 || first_table_index > first->get_table_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect table bit index", K(ret), K(first_table_index), K(first->get_table_size()));
      } else {
        map_info.table_map_.at(first_table_index - 1) = second_table_index - 1;
      }
    }
  }
  return ret;
}

int ObStmtComparer::compare_set_stmt(const ObSelectStmt *first,
                                    const ObSelectStmt *second,
                                    ObStmtMapInfo &map_info,
                                    QueryRelation &relation)
{
  int ret = OB_SUCCESS;
  relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmts have null", K(ret), K(first), K(second));
  } else if (!first->is_set_stmt() || !second->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect set stmt", KPC(first), KPC(second), K(ret));
  } else if (first->is_recursive_union() || second->is_recursive_union()) {
    //do nothing
  } else if (first->get_set_query().count() != second->get_set_query().count()) {
    //do nothing
  } else {
    QueryRelation set_query_relation = QueryRelation::QUERY_EQUAL;
    for (int64_t i = 0; OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == set_query_relation &&
          i < first->get_set_query().count(); ++i) {
      ObStmtMapInfo ref_query_map_info;
      if (OB_FAIL(SMART_CALL(check_stmt_containment(first->get_set_query(i),
                                                    second->get_set_query(i),
                                                    ref_query_map_info,
                                                    set_query_relation)))) {
        LOG_WARN("check stmt containment failed", K(ret));
      } else if (QueryRelation::QUERY_EQUAL == set_query_relation && ref_query_map_info.is_select_item_equal_) {
        if (OB_FAIL(map_info.view_select_item_map_.push_back(ref_query_map_info.select_item_map_))) {
          LOG_WARN("failed to push back map info", K(ret));
        } else if (OB_FAIL(append(map_info.equal_param_map_, ref_query_map_info.equal_param_map_))) {
          LOG_WARN("failed to append equal param", K(ret));
        }
      } else {
        set_query_relation = QueryRelation::QUERY_UNCOMPARABLE;
      }
    }
    if (OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == set_query_relation) {
      map_info.is_select_item_equal_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == set_query_relation 
          && i < map_info.view_select_item_map_.count(); ++i) {
        const ObIArray<int64_t> &select_item_map = map_info.view_select_item_map_.at(i);
        if (0 == i) {
          if (OB_FAIL(map_info.select_item_map_.assign(select_item_map))) {
            LOG_WARN("failed to assign select item map info", K(ret));
          }
        } else if (map_info.select_item_map_.count() != select_item_map.count()) {
          set_query_relation = QueryRelation::QUERY_UNCOMPARABLE;
          map_info.is_select_item_equal_ = false;
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == set_query_relation && 
                j < map_info.select_item_map_.count(); ++j) {
            if (map_info.select_item_map_.at(j) != select_item_map.at(j) || 
                OB_INVALID_ID == map_info.select_item_map_.at(j)) {
              set_query_relation = QueryRelation::QUERY_UNCOMPARABLE;
              map_info.is_select_item_equal_ = false;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && QueryRelation::QUERY_EQUAL == set_query_relation) {
      if (first->get_set_op() == second->get_set_op() && 
          ((first->is_set_distinct() && second->is_set_distinct()) || 
          (!first->is_set_distinct() && !second->is_set_distinct()))) {
        relation = QueryRelation::QUERY_EQUAL;
      } else if (ObSelectStmt::UNION == first->get_set_op() && !first->is_set_distinct()) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
      } else if (ObSelectStmt::UNION == second->get_set_op() && !second->is_set_distinct()) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
      } else if (ObSelectStmt::UNION == first->get_set_op()) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
      } else if (ObSelectStmt::UNION == second->get_set_op()) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
      }
    }

    // check order by exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      int64_t first_count = first->get_order_item_size();
      int64_t second_count = second->get_order_item_size();
      int64_t match_count = 0;
      if (0 == first_count && 0 == second_count) {
        map_info.is_order_equal_ = true;
      } else if (first_count != second_count) {
        // do nothing
      } else if (OB_FAIL(compute_orderby_map(first,
                                             second,
                                             first->get_order_items(),
                                             second->get_order_items(),
                                             map_info,
                                             match_count))) {
        LOG_WARN("failed to compute order item map", K(ret));
      } else if (match_count == first_count && match_count == second_count) {
        map_info.is_order_equal_ = true;
        LOG_TRACE("succeed to check order item map", K(relation), K(map_info));
      } else {
        // The Order Item relation does not afftect the query relation
        LOG_TRACE("succeed to check order item map", K(relation), K(map_info));
      }
    }

    // check limit exprs
    if (OB_SUCC(ret) && QueryRelation::QUERY_UNCOMPARABLE != relation) {
      if (!first->has_limit() && !second->has_limit()) {
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else if (first->has_limit() && !second->has_limit() &&
                 (relation == QueryRelation::QUERY_LEFT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_LEFT_SUBSET;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else if (!first->has_limit() && second->has_limit() &&
                 (relation == QueryRelation::QUERY_RIGHT_SUBSET ||
                  relation == QueryRelation::QUERY_EQUAL)) {
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      } else {
        relation = QueryRelation::QUERY_UNCOMPARABLE;
        LOG_TRACE("succeed to check limit expr", K(relation), K(map_info));
      }
    }
  }
  return ret;
}

int ObStmtComparer::compare_values_table_item(const ObDMLStmt *first,
                                              const TableItem *first_table,
                                              const ObDMLStmt *second,
                                              const TableItem *second_table,
                                              ObStmtMapInfo &map_info,
                                              QueryRelation &relation)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext context(first, second, map_info, &first->get_query_ctx()->calculable_items_);
  relation = QueryRelation::QUERY_UNCOMPARABLE;
  if (OB_ISNULL(first) || OB_ISNULL(first_table)
     || OB_ISNULL(second) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(first), K(first_table), K(second), K(second_table));
  } else if (first_table->is_values_table() &&
             second_table->is_values_table() &&
             first->get_column_size(first_table->table_id_) == second->get_column_size(second_table->table_id_) &&
             first_table->table_values_.count() % first->get_column_size(first_table->table_id_) == 0 &&
             second_table->table_values_.count() % second->get_column_size(second_table->table_id_) == 0) {
    //Perhaps in the future, the comparison of different row orders can be considered
    int64_t match_count = 0;
    bool is_match = true;
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < first_table->table_values_.count(); ++i) {
      bool is_match = false;
      if (i >= second_table->table_values_.count()) {
        break;
      } else if (OB_FAIL(is_same_condition(first_table->table_values_.at(i),
                                           second_table->table_values_.at(i),
                                           context,
                                           is_match))) {
        LOG_WARN("failed to check is condition equal", K(ret));
      } else if (!is_match) {
        // do nothing
      } else if (OB_FAIL(append(map_info.equal_param_map_, context.equal_param_info_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else {
        ++match_count;
      }
    }
    if (OB_SUCC(ret) && is_match) {
      if (match_count == first_table->table_values_.count() &&
          match_count == second_table->table_values_.count()) {//first table is equal second table
        relation = QueryRelation::QUERY_EQUAL;
      } else if (match_count == first_table->table_values_.count()) {//first table is subset second table
        relation = QueryRelation::QUERY_LEFT_SUBSET;
      } else if (match_count == second_table->table_values_.count()) {////second table is subset first table
        relation = QueryRelation::QUERY_RIGHT_SUBSET;
      }
    }
  } else {
    /*do nothing*/
  }
  return ret;
}
