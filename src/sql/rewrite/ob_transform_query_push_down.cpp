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
#include "sql/rewrite/ob_transform_query_push_down.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
namespace oceanbase {
using namespace common;
namespace sql {

/**
 * @brief ObTransformQueryPushDown::transform_one_stmt
 * 1.where condition
 * select * from (select * from t1 order by c3) where c2 > 2;
 * ==>
 * select * from t1 where c2 > 2 order by c3;
 *
 * select c2, c3 from (select c2, c3 from t1 group by c2, c3) where c2 > 2;
 * ==>
 * select c2, c3 from t1 group by c2, c3 having c2 > 2;
 *
 * 2.group by(having)
 * select c2 from (select c2 from t1) group by c2;
 * ==>
 * select c2 from t1 group by c2;
 * select c3 from (select c3 from t1) group by c3 having c3 > 5;
 * ==>
 * select c3 from t1 group by c3 having c3 > 5;
 *
 * 3.window function
 * select row_number() OVER() from (select * from t1);
 * ==>
 * select row_number() OVER() from t1;
 *
 * 4.aggr
 * select sum(c3) from (select * from t1) group by c3;
 * ==>
 * select sum(c3) from t1 group by c3;
 *
 * 5.distinct
 * select distinct * from (select * from t1 order by c2);
 * ==>
 * select distinct * from t1 order by c2;
 *
 * 6.order by
 * select * from (select * from t1 order by c2) order by c3;
 * ==>
 * select * from t1 order by c3;
 *
 * 7.limit
 * select * from (select t1.a, sum(b) from t1 group by t1.a) limit 5
 * ==>
 * select t1.a, sum(b) from t1 group by t1.a limit 5
 *
 * 8.rownum
 * select * from (select * from t1 where rownum = 1)
 * ==>
 * select * from t1 where rownum = 1
 *
 */
int ObTransformQueryPushDown::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  ObSelectStmt* select_stmt = NULL;     // outer select stmt
  ObSelectStmt* view_stmt = NULL;       // inner select stmt
  bool can_transform = false;           // can rewrite
  bool need_distinct = false;           // need distinct
  bool transform_having = false;        // can push where condition to having condition
  ObSEArray<int64_t, 4> select_offset;  // record select item offset postition, used to adjust set-op stmt output
                                        // postition
  ObSEArray<SelectItem, 4> const_select_items;  // record const select item, used to adjust set-op stmt output postition
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    // do nothing
  } else if (1 == select_stmt->get_from_item_size() &&  // only one table reference
             !select_stmt->get_from_item(0).is_joined_ && select_stmt->get_semi_infos().empty()) {
    const FromItem& cur_from = select_stmt->get_from_item(0);
    const TableItem* view_table_item = NULL;
    if (OB_ISNULL(view_table_item = select_stmt->get_table_item_by_id(cur_from.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table item", K(ret), K(cur_from));
    } else if (!view_table_item->is_generated_table()) {
      // do nothing
    } else if (OB_ISNULL(view_stmt = view_table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view stmt is null", K(ret));
    } else if (OB_FAIL(check_transform_validity(select_stmt,
                   view_stmt,
                   can_transform,
                   need_distinct,
                   transform_having,
                   select_offset,
                   const_select_items))) {
      LOG_WARN("gather transform information failed", K(ret));
    } else if (!can_transform) {
      /*do nothing*/
    } else if (OB_FAIL(do_transform(select_stmt,
                   view_stmt,
                   need_distinct,
                   transform_having,
                   view_table_item,
                   select_offset,
                   const_select_items))) {
      LOG_WARN("do transform failed", K(ret), K(stmt));
    } else if (OB_ISNULL(stmt = view_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view stmt is null", K(ret), K(view_stmt));
    } else {
      trans_happened = true;
    }
  } else {
    /*do nothing*/
  }
  LOG_TRACE("succeed to push query down", K(trans_happened));
  return ret;
}

int ObTransformQueryPushDown::check_transform_validity(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt,
    bool& can_transform, bool& need_distinct, bool& transform_having, ObIArray<int64_t>& select_offset,
    ObIArray<SelectItem>& const_select_items)
{
  int ret = OB_SUCCESS;
  bool check_status = false;
  can_transform = false;
  need_distinct = false;
  transform_having = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (view_stmt->get_stmt_hint().enable_no_view_merge() || select_stmt->is_recursive_union() ||
             view_stmt->is_hierarchical_query() || (view_stmt->is_recursive_union() && !select_stmt->is_spj()) ||
             select_stmt->has_set_op() || (select_stmt->has_sequence() && !view_stmt->is_spj()) ||
             select_stmt->need_temp_table_trans()) {
    can_transform = false;
  } else if (OB_FAIL(check_rownum_push_down(select_stmt, view_stmt, check_status))) {
    LOG_WARN("check rownum push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
  } else if (OB_FAIL(check_select_item_push_down(
                 select_stmt, view_stmt, select_offset, const_select_items, check_status))) {
    LOG_WARN("check select item push down failed");
  } else if (!check_status) {
    can_transform = false;
  } else if (select_stmt->get_condition_size() > 0 &&
             OB_FAIL(check_where_condition_push_down(select_stmt, view_stmt, transform_having, check_status))) {
    LOG_WARN("check where condition push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
  } else if ((select_stmt->has_group_by() || select_stmt->has_rollup()) && !view_stmt->is_spj()) {
    can_transform = false;
  } else if (select_stmt->has_window_function() && OB_FAIL(check_window_function_push_down(view_stmt, check_status))) {
    LOG_WARN("check window function push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
  } else if (select_stmt->has_distinct() && OB_FAIL(check_distinct_push_down(view_stmt, need_distinct, check_status))) {
    LOG_WARN("check distinct push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
  } else if (select_stmt->has_order_by() && view_stmt->has_limit()) {
    can_transform = false;
  } else if (select_stmt->has_limit()) {
    can_transform = (!select_stmt->is_calc_found_rows() && !view_stmt->is_contains_assignment() &&
                     can_limit_merge(*select_stmt, *view_stmt));
  } else {
    can_transform = true;
  }
  return ret;
}

//  select ... from (select ... limit a1 offset b1) limit a2 offset b2;
//  select ... from ... limit min(a1 - b2, a2) offset b1 + b2;
bool ObTransformQueryPushDown::can_limit_merge(ObSelectStmt& upper_stmt, ObSelectStmt& view_stmt)
{
  bool can_merge = false;
  if (!view_stmt.has_limit()) {
    can_merge = true;
  } else if (NULL == upper_stmt.get_limit_percent_expr() && NULL == view_stmt.get_limit_percent_expr() &&
             !upper_stmt.is_fetch_with_ties() && !view_stmt.is_fetch_with_ties()) {
    can_merge = true;
  } else {
    can_merge = false;
  }
  return can_merge;
}

int ObTransformQueryPushDown::do_limit_merge(ObSelectStmt& upper_stmt, ObSelectStmt& view_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr* limit_expr = NULL;
  ObRawExpr* offset_expr = NULL;
  if (!upper_stmt.has_limit()) {
    /*do nothing*/
  } else if (!can_limit_merge(upper_stmt, view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt input", K(ret), K(upper_stmt), K(view_stmt));
  } else if (!view_stmt.has_limit()) {
    view_stmt.set_limit_offset(upper_stmt.get_limit_expr(), upper_stmt.get_offset_expr());
    view_stmt.set_limit_percent_expr(upper_stmt.get_limit_percent_expr());
    view_stmt.set_fetch_with_ties(upper_stmt.is_fetch_with_ties());
    view_stmt.set_has_fetch(upper_stmt.has_fetch());
  } else if (OB_FAIL(ObTransformUtils::merge_limit_offset(ctx_,
                 view_stmt.get_limit_expr(),
                 upper_stmt.get_limit_expr(),
                 view_stmt.get_offset_expr(),
                 upper_stmt.get_offset_expr(),
                 limit_expr,
                 offset_expr))) {
    LOG_WARN("failed to merge limit offset", K(ret));
  } else {
    view_stmt.set_limit_offset(limit_expr, offset_expr);
  }
  return ret;
}

int ObTransformQueryPushDown::is_select_item_same(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt, bool& is_same,
    ObIArray<int64_t>& select_offset, ObIArray<SelectItem>& const_select_items)
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (select_stmt->get_select_item_size() < view_stmt->get_select_item_size()) {
    is_same = false;
  } else {
    is_same = true;
    ObBitSet<> column_id;
    bool is_same_exactly = true;
    int64_t column_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < select_stmt->get_select_item_size(); ++i) {
      const ObRawExpr* sel_expr = NULL;
      const ObColumnRefRawExpr* column_expr = NULL;
      if (OB_ISNULL(sel_expr = select_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret), K(sel_expr));
      } else if (sel_expr->has_const_or_const_expr_flag()) {
        if (OB_FAIL(const_select_items.push_back(select_stmt->get_select_item(i)))) {
          LOG_WARN("failed to push back select item");
        } else if (OB_FAIL(select_offset.push_back(-1))) {  //-1 meanings const expr
          LOG_WARN("failed to push back select offset");
        } else {
          is_same_exactly = false;
        }
      } else if (!sel_expr->is_column_ref_expr() || sel_expr->get_expr_level() != select_stmt->get_current_level()) {
        is_same = false;
      } else if (OB_FAIL(select_offset.push_back(
                     static_cast<const ObColumnRefRawExpr*>(sel_expr)->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("push back location offset failed", K(ret));
      } else {
        ++column_count;
        column_expr = static_cast<const ObColumnRefRawExpr*>(sel_expr);
        if (column_id.has_member(column_expr->get_column_id())) {
          is_same = false;
          is_same_exactly = false;
        } else {
          column_id.add_member(column_expr->get_column_id());
        }
        is_same_exactly &=
            (static_cast<const ObColumnRefRawExpr*>(sel_expr)->get_column_id() == i + OB_APP_MIN_COLUMN_ID);
      }
    }
    if (OB_SUCC(ret) && is_same) {
      if (column_count == view_stmt->get_select_item_size()) {
        /*do nothing*/
        // if outer output is const expr, then inner output must be const expr, eg:select 1 from (select 2 from t1)
      } else if (const_select_items.count() == select_stmt->get_select_item_size()) {
        for (int64_t i = 0; OB_SUCC(ret) && is_same && i < view_stmt->get_select_item_size(); ++i) {
          ObRawExpr* select_expr = NULL;
          if (OB_ISNULL(select_expr = view_stmt->get_select_item(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("select expr is null", K(ret), K(select_expr));
          } else if (select_expr->is_set_op_expr()) {
            if (OB_FAIL(check_set_op_is_const_expr(view_stmt, select_expr, is_same))) {
              LOG_WARN("failed to check set op is const expr", K(ret));
            }
          } else {
            is_same = select_expr->has_const_or_const_expr_flag();
          }
        }
      } else {
        is_same = false;
      }
      if (OB_SUCC(ret) && is_same && is_same_exactly) {
        select_offset.reset();
        const_select_items.reset();
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_rownum_push_down(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool select_has_rownum = false;
  bool view_has_rownum = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (OB_FAIL(select_stmt->has_rownum(select_has_rownum))) {
    LOG_WARN("check stmt has rownum failed", K(select_stmt), K(ret));
  } else if (OB_FAIL(view_stmt->has_rownum(view_has_rownum))) {
    LOG_WARN("check stmt has rownum failed", K(view_stmt), K(ret));
  } else if (select_has_rownum) {
    can_be = false;
  } else if (view_has_rownum &&
             (select_stmt->get_condition_size() > 0 || select_stmt->has_group_by() || select_stmt->has_rollup() ||
                 select_stmt->has_window_function() || select_stmt->has_set_op() || select_stmt->has_order_by())) {
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformQueryPushDown::check_select_item_push_down(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt,
    ObIArray<int64_t>& select_offset, ObIArray<SelectItem>& const_select_items, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool check_status = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (OB_FAIL(check_select_item_subquery(*select_stmt, *view_stmt, check_status))) {
    LOG_WARN("failed to check select item has subquery", K(ret));
  } else if (!check_status) {
    can_be = false;
  } else if (view_stmt->is_contains_assignment() || select_stmt->is_contains_assignment()) {
    can_be = false;
  } else if (OB_FAIL(is_select_item_same(select_stmt, view_stmt, check_status, select_offset, const_select_items))) {
    LOG_WARN("check select item same failed", K(ret));
  } else if (check_status && !view_stmt->is_recursive_union()) {
    can_be = true;
  } else if (view_stmt->is_scala_group_by() || view_stmt->has_distinct() ||
             (view_stmt->is_recursive_union() && (!check_status || !select_offset.empty())) ||
             (view_stmt->has_set_op() && !view_stmt->is_recursive_union())) {
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformQueryPushDown::check_select_item_subquery(ObSelectStmt &select_stmt,
                                                         ObSelectStmt &view,
                                                         bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = true;
  ObSEArray<ObRawExpr*, 4> column_exprs_from_subquery;
  ObRawExpr *expr = NULL;
  TableItem *table = NULL;
  ObSqlBitSet<> table_set;
  if (OB_UNLIKELY(1 != select_stmt.get_table_items().count())
      || OB_ISNULL(table = select_stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect select stmt", K(ret), K(select_stmt.get_from_item_size()), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < view.get_select_item_size(); ++i) {
    if (OB_ISNULL(expr = view.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->has_flag(CNT_SUB_QUERY)) {
      /* do nothing */
    } else if (OB_ISNULL(expr = select_stmt.get_column_expr_by_id(table->table_id_,
                                                                  i + OB_APP_MIN_COLUMN_ID))) {
      /* do nothing */
    } else if (OB_FAIL(column_exprs_from_subquery.push_back(expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (column_exprs_from_subquery.empty()) {
    /* do nothing */
  } else if (OB_FAIL(select_stmt.get_table_rel_ids(*table, table_set))) {
    LOG_WARN("failed to get rel ids", K(ret));
  } else {
    ObIArray<ObQueryRefRawExpr*> &subquery_exprs = select_stmt.get_subquery_exprs();
    ObSEArray<ObDMLStmt *, 4> ignore_stmts;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < subquery_exprs.count(); ++i) {
      column_exprs.reuse();
      if(OB_FAIL(ObTransformUtils::extract_column_exprs(subquery_exprs.at(i),
                                                        select_stmt.get_current_level(),
                                                        table_set, ignore_stmts,
                                                        column_exprs))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (ObOptimizerUtil::overlap(column_exprs, column_exprs_from_subquery)) {
        can_be = false;
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_where_condition_push_down(
    ObSelectStmt* select_stmt, ObSelectStmt* view_stmt, bool& transform_having, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  transform_having = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(view_stmt), K(ret));
  } else if (view_stmt->has_set_op() || view_stmt->has_limit() || view_stmt->has_window_function() ||
             view_stmt->is_contains_assignment()) {
    can_be = false;
  } else if (view_stmt->has_group_by() || view_stmt->has_rollup()) {
    bool is_invalid = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_invalid && i < select_stmt->get_condition_size(); ++i) {
      const ObRawExpr* cond_expr = NULL;
      if (OB_ISNULL(cond_expr = select_stmt->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret));
      } else if (cond_expr->has_flag(CNT_SUB_QUERY)) {
        is_invalid = true;
        can_be = false;
      } else {
        /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !is_invalid) {
      transform_having = true;
      can_be = true;
    }
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformQueryPushDown::check_window_function_push_down(ObSelectStmt* view_stmt, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  if (OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(view_stmt), K(ret));
  } else if (view_stmt->has_window_function() || view_stmt->has_distinct() || view_stmt->has_set_op() ||
             view_stmt->has_order_by() || view_stmt->has_limit() || view_stmt->is_contains_assignment()) {
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformQueryPushDown::check_distinct_push_down(ObSelectStmt* view_stmt, bool& need_distinct, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  if (OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(view_stmt), K(ret));
  } else if (view_stmt->has_limit()) {
    can_be = false;
  } else if (view_stmt->has_set_op()) {
    need_distinct = true;
    can_be = true;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformQueryPushDown::do_transform(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt, bool need_distinct,
    bool transform_having, const TableItem* view_table_item, ObIArray<int64_t>& select_offset,
    ObIArray<SelectItem>& const_select_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt) || OB_ISNULL(view_table_item) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(select_stmt), K(view_stmt), K(view_table_item), K(ctx_), K(ret));
  } else if (OB_FAIL(replace_stmt_exprs(select_stmt, view_stmt, view_table_item->table_id_))) {
    LOG_WARN("failed to replace stmt exprs", K(ret));
  } else if (OB_FAIL(push_down_stmt_exprs(
                 select_stmt, view_stmt, need_distinct, transform_having, select_offset, const_select_items))) {
    LOG_WARN("push down stmt exprs failed", K(ret));
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformQueryPushDown::push_down_stmt_exprs(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt,
    bool need_distinct, bool transform_having, ObIArray<int64_t>& select_offset,
    ObIArray<SelectItem>& const_select_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(select_stmt), K(view_stmt), K(ret));
  } else if (!transform_having &&
             OB_FAIL(append(view_stmt->get_condition_exprs(), select_stmt->get_condition_exprs()))) {
    LOG_WARN("append select_stmt condition exprs to view stmt failed", K(ret));
  } else if (transform_having && OB_FAIL(append(view_stmt->get_having_exprs(), select_stmt->get_condition_exprs()))) {
    LOG_WARN("append select_stmt condition exprs to view stmt having expr failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_group_exprs(), select_stmt->get_group_exprs()))) {
    LOG_WARN("append select_stmt window func exprs to view stmt window func exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_rollup_exprs(), select_stmt->get_rollup_exprs()))) {
    LOG_WARN("append select_stmt rollup exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_rollup_dirs(), select_stmt->get_rollup_dirs()))) {
    LOG_WARN("append select_stmt rollup directions failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_aggr_items(), select_stmt->get_aggr_items()))) {
    LOG_WARN("append aggr items failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_having_exprs(), select_stmt->get_having_exprs()))) {
    LOG_WARN("append select_stmt having exprs to view stmt having exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_window_func_exprs(), select_stmt->get_window_func_exprs()))) {
    LOG_WARN("append select_stmt window func exprs to view stmt window func exprs failed", K(ret));
  } else {
    if (select_stmt->has_rollup()) {
      view_stmt->assign_rollup();
    }
    if (!view_stmt->is_from_pivot()) {
      view_stmt->set_from_pivot(select_stmt->is_from_pivot());
    }
    if (need_distinct && !view_stmt->is_set_distinct()) {
      view_stmt->assign_set_distinct();
    } else if (select_stmt->has_distinct() && !view_stmt->has_distinct()) {
      view_stmt->assign_distinct();
    }
    if (select_stmt->has_select_into()) {
      view_stmt->set_select_into(select_stmt->get_select_into());
    }
    if (select_stmt->has_order_by()) {
      view_stmt->get_order_items().reset();
      if (OB_FAIL(append(view_stmt->get_order_items(), select_stmt->get_order_items()))) {
        LOG_WARN("append select_stmt order items to view stmt order items failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (select_stmt->has_limit() && OB_FAIL(do_limit_merge(*select_stmt, *view_stmt))) {
      LOG_WARN("failed to merge limit", K(ret));
    } else if (select_stmt->has_sequence() &&
               OB_FAIL(append(view_stmt->get_nextval_sequence_ids(), select_stmt->get_nextval_sequence_ids()))) {
      LOG_WARN("failed to append nextval sequence ids", K(ret));
    } else if (select_stmt->has_sequence() &&
               OB_FAIL(append(view_stmt->get_currval_sequence_ids(), select_stmt->get_currval_sequence_ids()))) {
      LOG_WARN("failed to append currval sequence ids", K(ret));
    } else if (view_stmt->has_set_op()) {
      if (select_offset.empty()) {
        /*do nothing*/
      } else if (OB_FAIL(recursive_adjust_select_item(view_stmt, select_offset, const_select_items))) {
        LOG_WARN("recursive adjust select item location failed", K(ret));
      } else {
        /*do nothing*/
      }
    } else {
      view_stmt->get_select_items().reset();
      if (OB_FAIL(append(view_stmt->get_select_items(), select_stmt->get_select_items()))) {
        LOG_WARN("view stmt replace select items failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(view_stmt->get_subquery_exprs(), select_stmt->get_subquery_exprs()))) {
      LOG_WARN("view stmt append subquery failed", K(ret));
    } else if (OB_FAIL(adjust_stmt_hints(select_stmt, view_stmt))) {
      LOG_WARN("failed to adjust hints", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_stmt_parent(select_stmt, view_stmt))) {
      LOG_WARN("failed to adjust subquery stmt parent", K(ret));
    } else {
      view_stmt->set_current_level(select_stmt->get_current_level());
      view_stmt->set_parent_namespace_stmt(select_stmt->get_parent_namespace_stmt());
      // add all the view store info
      const ObDMLStmt::ObViewTableIds& view_ids = select_stmt->get_view_table_id_store();
      for (int64_t i = 0; OB_SUCC(ret) && i < view_ids.count(); i++) {
        if (OB_FAIL(view_stmt->add_view_table_id(view_ids.at(i)))) {
          LOG_WARN("fail to add view table id", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_from_show_stmt = select_stmt->is_from_show_stmt();
        stmt::StmtType literal_stmt_type = select_stmt->get_literal_stmt_type();
        view_stmt->set_literal_stmt_type(literal_stmt_type);
        view_stmt->set_is_from_show_stmt(is_from_show_stmt);
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::adjust_stmt_hints(ObSelectStmt* select_stmt, ObSelectStmt* view_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(select_stmt), K(view_stmt), K(ret));
  } else if (select_stmt->has_group_by()) {
    view_stmt->get_stmt_hint().aggregate_ = select_stmt->get_stmt_hint().aggregate_;
    view_stmt->get_stmt_hint().use_place_groupby_ = select_stmt->get_stmt_hint().use_place_groupby_;
    if (OB_FAIL(view_stmt->get_stmt_hint().place_groupby_.assign(select_stmt->get_stmt_hint().place_groupby_))) {
      LOG_WARN("place groupby expand array assign failed.", K(ret));
    } else if (OB_FAIL(view_stmt->get_stmt_hint().no_place_groupby_.assign(
                   select_stmt->get_stmt_hint().no_place_groupby_))) {
      LOG_WARN("no place groupby expand array assign failed.", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret) && select_stmt->get_condition_size() > 0) {
    view_stmt->get_stmt_hint().use_expand_ = select_stmt->get_stmt_hint().use_expand_;
    if (OB_FAIL(view_stmt->get_stmt_hint().no_expand_.assign(select_stmt->get_stmt_hint().no_expand_))) {
      LOG_WARN("no expand array assign failed.", K(ret));
    } else if (OB_FAIL(view_stmt->get_stmt_hint().use_concat_.assign(select_stmt->get_stmt_hint().use_concat_))) {
      LOG_WARN("use concat array assign failed.", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    view_stmt->get_stmt_hint().use_unnest_ = select_stmt->get_stmt_hint().use_unnest_;
    if (OB_FAIL(view_stmt->get_stmt_hint().no_unnest_.assign(select_stmt->get_stmt_hint().no_unnest_))) {
      LOG_WARN("no unnest array assign failed.", K(ret));
    } else if (OB_FAIL(view_stmt->get_stmt_hint().unnest_.assign(select_stmt->get_stmt_hint().unnest_))) {
      LOG_WARN("unnest array assign failed.", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    view_stmt->get_stmt_hint().use_view_merge_ = select_stmt->get_stmt_hint().use_view_merge_;
    if (OB_FAIL(view_stmt->get_stmt_hint().no_v_merge_.assign(select_stmt->get_stmt_hint().no_v_merge_))) {
      LOG_WARN("no unnest array assign failed.", K(ret));
    } else if (OB_FAIL(view_stmt->get_stmt_hint().v_merge_.assign(select_stmt->get_stmt_hint().v_merge_))) {
      LOG_WARN("unnest array assign failed.", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}
int ObTransformQueryPushDown::replace_stmt_exprs(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSEArray<ObColumnRefRawExpr*, 16> temp_exprs;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->get_column_exprs(table_id, temp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(old_column_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                 old_column_exprs, *child_stmt, new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else {
    ret = parent_stmt->replace_inner_stmt_expr(old_column_exprs, new_column_exprs);
  }
  return ret;
}

int ObTransformQueryPushDown::recursive_adjust_select_item(
    ObSelectStmt* select_stmt, ObIArray<int64_t>& select_offset, ObIArray<SelectItem>& const_select_items)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(ctx_), K(ctx_->expr_factory_), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(is_stack_overflow), K(ret));
  } else if (select_stmt->has_set_op()) {
    ObIArray<ObSelectStmt*>& child_stmts = static_cast<ObSelectStmt*>(select_stmt)->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      ret = SMART_CALL(recursive_adjust_select_item(child_stmts.at(i), select_offset, const_select_items));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(reset_set_stmt_select_list(select_stmt, select_offset))) {
        LOG_WARN("failed to reset set stmt select list", K(ret));
      }
    }
  } else {
    ObSEArray<SelectItem, 4> new_select_item;
    ObSEArray<SelectItem, 4> old_select_item;
    ObSEArray<SelectItem, 4> new_const_select_items;
    if (OB_FAIL(old_select_item.assign(select_stmt->get_select_items()))) {
      LOG_WARN("failed to assign a new select item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_select_items(
                   *ctx_->expr_factory_, const_select_items, new_const_select_items, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy select items failed", K(ret));
    } else {
      int64_t k = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_offset.count(); ++i) {
        if (select_offset.at(i) == -1) {  //-1 meanings upper stmt has const select item
          if (OB_UNLIKELY(k >= new_const_select_items.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(k), K(new_const_select_items.count()));
          } else if (OB_FAIL(new_select_item.push_back(new_const_select_items.at(k)))) {
            LOG_WARN("push back select item error", K(ret));
          } else {
            ++k;
          }
        } else if (OB_UNLIKELY(select_offset.at(i) < 0 || select_offset.at(i) >= old_select_item.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(select_offset.at(i)), K(old_select_item.count()), K(ret));
        } else if (OB_FAIL(new_select_item.push_back(old_select_item.at(select_offset.at(i))))) {
          LOG_WARN("push back select item error", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret)) {
        select_stmt->get_select_items().reset();
        if (OB_FAIL(append(select_stmt->get_select_items(), new_select_item))) {
          LOG_WARN("view stmt replace select items failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::reset_set_stmt_select_list(ObSelectStmt* select_stmt, ObIArray<int64_t>& select_offset)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> old_select_exprs;
  ObSEArray<ObRawExpr*, 4> adjust_old_select_exprs;
  ObSEArray<ObRawExpr*, 4> new_select_exprs;
  ObSEArray<ObRawExpr*, 4> adjust_new_select_exprs;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(ctx_), K(ret));
  } else if (!select_stmt->is_set_stmt()) {
    /*do nothing*/
  } else if (OB_FAIL(select_stmt->get_select_exprs(old_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else {
    select_stmt->get_select_items().reset();
    if (OB_FAIL(ObOptimizerUtil::gen_set_target_list(
            ctx_->allocator_, ctx_->session_info_, ctx_->expr_factory_, select_stmt))) {
      LOG_WARN("failed to create select list for union", K(ret));
    } else if (OB_FAIL(select_stmt->get_select_exprs(new_select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_offset.count(); ++i) {
        if (select_offset.at(i) == -1) {  //-1 meanings upper stmt has const select item
          /*do nothing */
        } else if (OB_UNLIKELY(select_offset.at(i) < 0 || select_offset.at(i) >= old_select_exprs.count() ||
                               i >= new_select_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error",
              K(select_offset.at(i)),
              K(old_select_exprs.count()),
              K(new_select_exprs.count()),
              K(i),
              K(ret));
        } else if (OB_FAIL(adjust_old_select_exprs.push_back(old_select_exprs.at(select_offset.at(i)))) ||
                   OB_FAIL(adjust_new_select_exprs.push_back(new_select_exprs.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret) && adjust_old_select_exprs.count() > 0) {
        if (OB_FAIL(select_stmt->replace_inner_stmt_expr(adjust_old_select_exprs, adjust_new_select_exprs))) {
          LOG_WARN("failed to replace inner stmt expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_set_op_is_const_expr(ObSelectStmt* select_stmt, ObRawExpr* expr, bool& is_const)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt) ||
      OB_UNLIKELY(!expr->is_set_op_expr() || !select_stmt->is_set_stmt() || select_stmt->get_set_query().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(select_stmt));
  } else if (!is_const) {
    /*do nothing*/
  } else if (select_stmt->is_set_distinct()) {
    is_const = false;
  } else {
    int64_t idx = static_cast<ObSetOpRawExpr*>(expr)->get_idx();
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < select_stmt->get_set_query().count(); ++i) {
      ObRawExpr* select_expr = NULL;
      if (OB_ISNULL(select_stmt->get_set_query(i)) ||
          OB_UNLIKELY(idx < 0 || idx >= select_stmt->get_set_query(i)->get_select_item_size()) ||
          OB_ISNULL(select_expr = select_stmt->get_set_query(i)->get_select_item(idx).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(i), K(select_stmt->get_set_query(i)), K(idx), K(select_expr), K(ret));
      } else if (select_expr->is_set_op_expr()) {
        if (OB_FAIL(SMART_CALL(check_set_op_is_const_expr(select_stmt->get_set_query(i), select_expr, is_const)))) {
          LOG_WARN("failed to check set op is const expr", K(ret));
        }
      } else {
        is_const &= select_expr->has_const_or_const_expr_flag();
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
