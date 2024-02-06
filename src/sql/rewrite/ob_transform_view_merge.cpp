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

#include "sql/rewrite/ob_transform_view_merge.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObTransformViewMerge::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *&stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_from_item_happened = false;
  bool is_semi_info_happened = false;
  ObSEArray<ObSelectStmt*, 4> merged_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (OB_FAIL(transform_in_from_item(stmt, merged_stmts, is_from_item_happened))) {
    LOG_WARN("failed to do view merge in from item", K(ret));
  } else if (OB_FAIL(transform_in_semi_info(stmt, merged_stmts, is_semi_info_happened))) {
    LOG_WARN("failed to do view merge in semi info", K(ret));
  } else if (!is_from_item_happened && !is_semi_info_happened) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
    LOG_WARN("failed to fromalize query ref exprs", K(ret));
  } else if (OB_FAIL(add_transform_hint(*stmt, &merged_stmts))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("succeed to do view merge", K(is_from_item_happened), K(is_semi_info_happened));
  }
  return ret;
}

int ObTransformViewMerge::transform_one_stmt_with_outline(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                          ObDMLStmt *&stmt,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_happened = false;
  ObSEArray<ObSelectStmt*, 4> merged_stmts;
  do {
    is_happened = false;
    if (OB_FAIL(transform_in_from_item(stmt, merged_stmts, is_happened))) {
      LOG_WARN("failed to do view merge in from item", K(ret));
    } else if (!is_happened && OB_FAIL(transform_in_semi_info(stmt, merged_stmts, is_happened))) {
      LOG_WARN("failed to do view merge in semi info", K(ret));
    } else if (!is_happened) {
      LOG_TRACE("can not do view merge with outline", K(ctx_->src_qb_name_));
    } else {
      ++ctx_->trans_list_loc_;
      trans_happened = true;
      LOG_TRACE("succeed to do view merge with outline", K(ctx_->src_qb_name_));
    }
  } while (OB_SUCC(ret) && is_happened);
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
      LOG_WARN("failed to fromalize query ref exprs", K(ret));
    } else if (OB_FAIL(add_transform_hint(*stmt, &merged_stmts))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }

  return ret;
}

int ObTransformViewMerge::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         const ObDMLStmt &stmt,
                                         bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  UNUSED(parent_stmts);
  UNUSED(current_level);
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  if (!stmt.is_sel_del_upd() || stmt.has_instead_of_trigger() || stmt.is_hierarchical_query()) {
    need_trans = false;
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    need_trans = true;
  } else if (NULL == (trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))
             || !trans_hint->is_view_merge_hint()
             || !static_cast<const ObViewMergeHint*>(trans_hint)->enable_view_merge(ctx_->src_qb_name_)) {
    /*do nothing*/
  } else {
    const TableItem *table = NULL;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
      if (OB_ISNULL(table = stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (!table->is_generated_table()) {
        /*do nothing*/
      } else if (OB_ISNULL(table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(*table));
      } else {
        need_trans = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                  get_hint(table->ref_query_->get_stmt_hint()));
      }
    }
    if (!need_trans) {
      OPT_TRACE("outline reject transform");
    }
  }
  return ret;
}

int ObTransformViewMerge::check_hint_allowed_merge(ObDMLStmt &stmt,
                                                   ObSelectStmt &ref_query,
                                                   bool &force_merge,
                                                   bool &force_no_merge)
{
  int ret = OB_SUCCESS;
  force_merge = false;
  force_no_merge = false;
  bool contain_inner_table = false;
  const ObQueryHint *query_hint = NULL;
  const ObViewMergeHint *myhint = static_cast<const ObViewMergeHint*>(get_hint(ref_query.get_stmt_hint()));
  bool is_disable = (NULL != myhint && myhint->enable_no_view_merge());
  const ObHint *no_rewrite1 = stmt.get_stmt_hint().get_no_rewrite_hint();
  const ObHint *no_rewrite2 = ref_query.get_stmt_hint().get_no_rewrite_hint();
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (query_hint->has_outline_data()) {
    // outline data allowed merge
    if (myhint != NULL &&
        query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint) &&
        myhint->enable_view_merge(ctx_->src_qb_name_)) {
      force_merge = true;
    } else {
      force_no_merge = true;
    }
  } else if (NULL != myhint && myhint->enable_view_merge(ctx_->src_qb_name_)) {
    // enable transform hint added after transform in construct_transform_hint()
    force_merge = true;
  } else if (is_disable || NULL != no_rewrite1 || NULL != no_rewrite2) {
    // add disable transform hint here
    force_no_merge = true;
    if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite1))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite2))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    }
  } else if (OB_FAIL(check_contain_inner_table(ref_query, contain_inner_table))) {
    LOG_WARN("failed to check contain inner table", K(ret));
  } else if (contain_inner_table) {
    force_no_merge = true;
    OPT_TRACE("stmt contain inner table, can not merge");
  }
  return ret;
}

int ObTransformViewMerge::transform_in_from_item(ObDMLStmt *stmt,
                                                 ObIArray<ObSelectStmt*> &merged_stmts,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    int64_t i = 0;
    while (OB_SUCC(ret) && i < stmt->get_from_item_size()) {
      bool is_happened = false;
      TableItem *table_item = stmt->get_table_item(stmt->get_from_item(i));
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (table_item->is_joined_table()) {
        if (OB_FAIL(transform_joined_table(stmt,
                                           static_cast<JoinedTable*>(table_item),
                                           true,
                                           merged_stmts,
                                           is_happened))) {
          LOG_WARN("failed to transform joined table", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to do view merge for joined table", K(is_happened),
                    K(table_item->table_id_));
        }
      } else if (table_item->is_generated_table()) {
        if (OB_FAIL(transform_generated_table(stmt, table_item, merged_stmts, is_happened))) {
          LOG_WARN("failed to transform basic table", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to do view merge for basic table", K(is_happened),
                    K(table_item->table_id_));
        }
      }
      if (OB_SUCC(ret) && !is_happened) {
        i++;
      }
    }
  }
  return ret;
}

// do view merge for semi right table
int ObTransformViewMerge::transform_in_semi_info(ObDMLStmt *stmt,
                                                 ObIArray<ObSelectStmt*> &merged_stmts,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
    bool can_be = false;
    SemiInfo *semi_info = NULL;
    ObSelectStmt *child_stmt = NULL;
    TableItem *right_table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(stmt) || OB_ISNULL(semi_info = semi_infos.at(i)) ||
          OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!right_table->is_generated_table()) {
        /*do nothing*/
      } else if (OB_ISNULL(child_stmt = right_table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(check_semi_right_table_can_be_merged(stmt, child_stmt, can_be))) {
        LOG_WARN("failed to check semi right table can be unnested", K(ret));
      } else if (!can_be) {
        /*do nothing*/
      } else if (OB_FAIL(do_view_merge_for_semi_right_table(stmt, child_stmt, semi_info))) {
        LOG_WARN("failed to do view merge", K(ret));
      } else if (OB_FAIL(merged_stmts.push_back(child_stmt))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformViewMerge::do_view_merge_for_semi_right_table(ObDMLStmt *parent_stmt,
                                                             ObSelectStmt *child_stmt,
                                                             SemiInfo *semi_info)
{
  int ret = OB_SUCCESS;
  TableItem *right_table = NULL;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  // TODO link.zt I can refine here now
  // updatable view have some unexpected design, as a result we use a sepcial replace here
  // updatable view's part expr should be replaced while it belongs to table.
  // In normal case, we expect the part expr should be removed.
  ObStmtExprReplacer replacer;
  replacer.set_recursive(false);
  if (OB_ISNULL(ctx_) || OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt) || OB_ISNULL(semi_info) ||
      OB_ISNULL(right_table = parent_stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!child_stmt->is_single_table_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected semi info for view merge", K(ret));
  } else if (OB_FAIL(parent_stmt->get_stmt_hint().merge_stmt_hint(child_stmt->get_stmt_hint(),
                                                                  LEFT_HINT_DOMINATED))) {
    LOG_WARN("failed to merge view stmt hint", K(ret));
  } else if (OB_FAIL(parent_stmt->get_stmt_hint().replace_name_for_single_table_view(ctx_->allocator_,
                                                                                     *parent_stmt,
                                                                                     *right_table))) {
    LOG_WARN("failed to replace name for single table view", K(ret));
  } else if (OB_FALSE_IT(semi_info->right_table_id_ =
                          child_stmt->get_from_items().at(0).table_id_)) {
  } else if (OB_FAIL(parent_stmt->get_column_exprs(right_table->table_id_, old_column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(old_column_exprs,
                                                                          *child_stmt,
                                                                          new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else if (OB_FAIL(parent_stmt->remove_table_info(right_table))) {
    LOG_WARN("failed to remove right table info", K(ret));
  } else if (OB_FAIL(replacer.add_replace_exprs(old_column_exprs, new_column_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
  } else if (OB_FAIL(parent_stmt->iterate_stmt_expr(replacer))) {
    LOG_WARN("failed to replace stmt expr", K(ret));
  } else if (OB_FAIL(adjust_updatable_view(parent_stmt, right_table))) {
    LOG_WARN("failed to adjust updatable view", K(ret));
  } else if (OB_FAIL(append(parent_stmt->get_table_items(), child_stmt->get_table_items()))) {
    LOG_WARN("failed to append table items", K(ret));
  } else if (OB_FAIL(append(parent_stmt->get_column_items(), child_stmt->get_column_items()))) {
    LOG_WARN("failed to append column items", K(ret));
  } else if (OB_FAIL(append(parent_stmt->get_part_exprs(), child_stmt->get_part_exprs()))) {
    LOG_WARN("failed to append part exprs", K(ret));
  } else if (OB_FAIL(append(parent_stmt->get_check_constraint_items(),
                            child_stmt->get_check_constraint_items()))) {
    LOG_WARN("failed to append check constraint items", K(ret));
  } else if (parent_stmt->is_select_stmt() &&
              OB_FAIL(append(static_cast<ObSelectStmt *>(parent_stmt)->get_sample_infos(),
                             child_stmt->get_sample_infos()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*parent_stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(parent_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(parent_stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  }
  return ret;
}

// check semi right table can merge
int ObTransformViewMerge::check_semi_right_table_can_be_merged(ObDMLStmt *stmt,
                                                               ObSelectStmt *ref_query,
                                                               bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool has_rownum = false;
  bool force_merge = false;
  bool force_no_merge = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ref_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ref_query));
  } else if (OB_FAIL(check_hint_allowed_merge(*stmt, 
                                              *ref_query, 
                                              force_merge, 
                                              force_no_merge))) {
    LOG_WARN("failed to check hint allowed merge", K(ret));
  } else if (force_no_merge) {
    can_be = false;
  } else if (!ref_query->is_single_table_stmt()
             || !ref_query->get_condition_exprs().empty()
             || !ref_query->get_subquery_exprs().empty()
             || ref_query->has_distinct()
             || ref_query->has_group_by()
             || ref_query->has_rollup()
             || ref_query->has_window_function()
             || ref_query->has_limit()
             || ref_query->is_contains_assignment()
             || ref_query->has_sequence()
             || ref_query->is_hierarchical_query()
             || ref_query->has_ora_rowscn()
             || (lib::is_mysql_mode() && ref_query->has_for_update())) {
    can_be = false;
  } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum expr", K(ret));
  } else if (has_rownum) {
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

/*@brief transform_basic_table处理普通generated table
 */
int ObTransformViewMerge::transform_generated_table(ObDMLStmt *parent_stmt,
                                                    TableItem *table_item,
                                                    ObIArray<ObSelectStmt*> &merged_stmts,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_be = false;
  ObSelectStmt *child_stmt = NULL;
  ViewMergeHelper helper;
  trans_happened = false;
  OPT_TRACE("try to merge view:", table_item);
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), K(parent_stmt), K(table_item));
  } else if (!table_item->is_generated_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(child_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_can_be_merged(parent_stmt,
                                         child_stmt,
                                         helper,
                                         parent_stmt->is_hierarchical_query(),
                                         false,
                                         can_be))) {
    LOG_WARN("failed to check can be unnested", K(ret));
  } else if (!can_be) {
    /*do nothing*/
    OPT_TRACE("view can no be merged");
  } else if (OB_FAIL(do_view_merge(parent_stmt, child_stmt, table_item, helper))) {
    LOG_WARN("failed to do view merge", K(ret));
  } else if (OB_FAIL(merged_stmts.push_back(child_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

/*@brief transform_basic_table处理joined table中的generated table
 */
int ObTransformViewMerge::transform_generated_table(ObDMLStmt *parent_stmt,
                                                    JoinedTable *parent_table,
                                                    TableItem *table_item,
                                                    bool need_check_where_condi,
                                                    bool can_push_where,
                                                    ObIArray<ObSelectStmt*> &merged_stmts,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_be = false;
  ObSelectStmt *child_stmt = NULL;
  ViewMergeHelper helper;
  helper.parent_table = parent_table;
  helper.trans_table = table_item;
  helper.can_push_where = can_push_where;
  bool is_left_join_right_table = false;
  trans_happened = false;
  OPT_TRACE("try to merge view:", table_item);
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(parent_table) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), K(parent_stmt), K(parent_table), K(table_item));
  } else if (!table_item->is_generated_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(child_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (parent_table->joined_type_ == FULL_OUTER_JOIN) {
    //如果是full outer join，如果视图有null reject输出表达式，需要视图有非空列
    helper.need_check_null_propagate = true;
  } else if (parent_table->joined_type_ == LEFT_OUTER_JOIN) {
    if (table_item == parent_table->right_table_) {
      //如果是left join的右表，如果视图有null reject输出表达式，需要视图有非空列
      helper.need_check_null_propagate = true;
      is_left_join_right_table = true;
    }
  } else if (parent_table->joined_type_ == RIGHT_OUTER_JOIN) {
    if (table_item == parent_table->left_table_) {
      //如果是right join的左表，如果视图有null reject输出表达式，需要视图有非空列
      helper.need_check_null_propagate = true;
      is_left_join_right_table = true;
    }
  } else {/*do nothing*/}
  if (OB_FAIL(ret) || !table_item->is_generated_table()) {
    /*do nothing*/
  } else if (need_check_where_condi && child_stmt->get_condition_size() > 0) {
    /*do nothing*/
    OPT_TRACE("view has conditions, can not merge view");
  } else if (OB_FAIL(check_can_be_merged(parent_stmt,
                                         child_stmt,
                                         helper,
                                         !can_push_where,
                                         is_left_join_right_table,
                                         can_be))) {
    LOG_WARN("failed to check can be unnested", K(ret));
  } else if (!can_be) {
    /*do nothing*/
  } else if (OB_FAIL(do_view_merge(parent_stmt,
                                   child_stmt,
                                   table_item,
                                   helper))) {
    LOG_WARN("failed to do view merge in joined table", K(ret));
  } else if (OB_FAIL(merged_stmts.push_back(child_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformViewMerge::check_basic_validity(ObDMLStmt *parent_stmt,
                                               ObSelectStmt *child_stmt,
                                               bool &can_be) {
  int ret = OB_SUCCESS;
  can_be = false;
  bool has_assignment = false;
  bool has_rownum_expr = false;
  bool has_ref_assign_user_var = false;
  bool force_merge = false;
  bool force_no_merge = false;
  bool is_select_expr_valid = false;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_hint_allowed_merge(*parent_stmt, 
                                              *child_stmt, 
                                              force_merge, 
                                              force_no_merge))) {
    LOG_WARN("failed to check hint allowed merge", K(ret));
  } else if (force_no_merge) {
    can_be = false;
    OPT_TRACE("hint reject transform");
  } else if (child_stmt->is_hierarchical_query()
             || child_stmt->has_distinct()
             || child_stmt->has_group_by()
             || child_stmt->is_set_stmt()
             || child_stmt->has_rollup()
             || child_stmt->has_order_by()
             || child_stmt->has_limit()
             || child_stmt->get_aggr_item_size() != 0
             || child_stmt->has_window_function()
             || child_stmt->has_sequence()
             || child_stmt->has_ora_rowscn()
             || child_stmt->is_values_table_query()) {
    can_be = false;
    OPT_TRACE("not a valid view");
  } else if (OB_FAIL(ObTransformUtils::check_has_assignment(*child_stmt, has_assignment))) {
    LOG_WARN("check has assignment failed", K(ret));
  } else if (has_assignment) {
    can_be = false;
  } else if (OB_FAIL(child_stmt->has_rownum(has_rownum_expr))) {
    LOG_WARN("failed to check has rownum expr", K(ret));
  } else if (has_rownum_expr) {
    can_be = false;
    OPT_TRACE("view has rownum");
  } else if (OB_FAIL(child_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_expr_valid_for_stmt_merge(select_exprs,
                                                                       is_select_expr_valid))) {
    LOG_WARN("failed to check select expr valid", K(ret));
  } else if (!is_select_expr_valid) {
    can_be = false;
  } else if (0 == child_stmt->get_from_item_size()) {
    //当 view 为 select ... from dual, 若上层非层次查询, 允许视图合并
    can_be = parent_stmt->is_single_table_stmt()
             && !parent_stmt->is_hierarchical_query();
  } else if (OB_FAIL(child_stmt->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_ref_assign_user_var) {
    can_be = false;
    OPT_TRACE("view has user var");
  } else if (parent_stmt->get_condition_size() > 0) {
    can_be = true;
    ObSEArray<ObRawExpr*, 8> select_exprs;
    if (OB_FAIL(child_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < select_exprs.count(); ++i) {
      ObRawExpr *expr = select_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->has_flag(CNT_SYS_CONNECT_BY_PATH)) {
        can_be = false;
        OPT_TRACE("view`s select expr contain connect by path func");
      }
    }
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformViewMerge::check_contain_inner_table(const ObSelectStmt &stmt,
                                                    bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  const ObIArray<TableItem*> &table_items = stmt.get_table_items();
  for (int64_t i = 0; OB_SUCC(ret) && !contain && i < table_items.count(); ++i) {
    const TableItem *table = table_items.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_basic_table()) {
      //do nothing
    } else {
      contain = is_virtual_table(table->ref_id_)
                || is_inner_table(table->ref_id_);
    }
  }
  return ret;
}

int ObTransformViewMerge::check_can_be_merged(ObDMLStmt *parent_stmt,
                                              ObSelectStmt *child_stmt,
                                              ViewMergeHelper &helper,
                                              bool need_check_subquery,
                                              bool is_left_join_right_table,
                                              bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = true;
  bool has_rollup = false;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_basic_validity(parent_stmt, child_stmt, can_be))) {
    LOG_WARN("failed to check", K(ret));
  } else if (!can_be) {
  } else {
    has_rollup = parent_stmt->is_select_stmt() &&
                 static_cast<ObSelectStmt *>(parent_stmt)->has_rollup();
    //select expr不能包含subquery
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < child_stmt->get_select_item_size(); i++) {
      ObRawExpr *expr = child_stmt->get_select_item(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr", K(ret));
      } else if (expr->has_flag(CNT_SUB_QUERY)) {
        can_be = false;
        OPT_TRACE("view`s select expr contain subquery, can not merge");
      } else if (expr->is_const_expr() && has_rollup) {
        can_be = false;
        OPT_TRACE("const expr can not be merged into rollup stmt");
      }
    }
    //stmt不能包含rand函数
    if (OB_SUCC(ret) && can_be) {
      bool has_rand = false;
      if (OB_FAIL(child_stmt->has_rand(has_rand))) {
        LOG_WARN("failed to get rand flag", K(ret));
      } else if (has_rand) {
        can_be = false;
        OPT_TRACE("view has random expr, can not merge");
      }
    }
  }
  //检查where condition是否存在子查询
  if (OB_FAIL(ret) || !can_be) {
    /*do nothing*/
  } else if (need_check_subquery){
    if (child_stmt->get_semi_infos().count() > 0) {
      can_be =false;
      OPT_TRACE("view has semi info, can not merge");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && can_be && i < child_stmt->get_condition_size(); i++) {
        const ObRawExpr *expr = child_stmt->get_condition_expr(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr", K(ret));
        } else if (expr->has_flag(CNT_SUB_QUERY)) {
          can_be = false;
          OPT_TRACE("view`s condition has subquery, can not merge");
        } else { /*do nothing*/ }
      }
    }
  } else {/*do nothing*/}
  //Check if the left join right view expansion will increase the plan space. 
  if (OB_FAIL(ret) || !can_be || !is_left_join_right_table) {
    /*do nothing*/
  } else if (OB_ISNULL(helper.parent_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(check_left_join_right_view_need_merge(parent_stmt,
                                                           child_stmt, 
                                                           helper.trans_table,
                                                           helper.parent_table,
                                                           can_be))) {
    LOG_WARN("failed to check left join right view need merge", K(ret));
  }
  //检查视图是否有空值拒绝表达式
  if (OB_FAIL(ret) || !can_be) {
    /*do nothing*/
  } else if (helper.need_check_null_propagate){
    ObSEArray<ObRawExpr *, 4> columns;
    ObSqlBitSet<> from_tables;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    if (OB_FAIL(child_stmt->get_from_tables(from_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(child_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*child_stmt, columns, from_tables, column_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else {
      //检查是否有空值拒绝表达式
      bool find = false;
      for (int64_t i = 0; OB_SUCC(ret) && !find && i < child_stmt->get_select_item_size(); i++) {
        ObRawExpr *expr = child_stmt->get_select_item(i).expr_;
        bool is_null_propagate = true;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, column_exprs, is_null_propagate))) {
          LOG_WARN("failed to is null propagate expr", K(ret));
        } else if (!is_null_propagate) {
          find = true;
        } else {/*do nothing*/}
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (!find) {
        /*do nothing*/
      } else if (OB_FAIL(find_not_null_column(*parent_stmt, *child_stmt, helper, column_exprs, can_be))){
        LOG_WARN("failed to find not null column", K(ret));
      } else if (!can_be) {
        OPT_TRACE("view has null propagate expr, but not found not null column");
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObTransformViewMerge::check_left_join_right_view_need_merge(ObDMLStmt *parent_stmt,
                                                                ObSelectStmt* child_stmt,
                                                                TableItem *view_table,
                                                                JoinedTable *joined_table,
                                                                bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  bool force_merge = false;
  bool force_no_merge = false;
  if (OB_ISNULL(child_stmt) || OB_ISNULL(parent_stmt) || 
      OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (2 > child_stmt->get_table_items().count()) {
    need_merge = true;
  } else if (OB_FAIL(check_hint_allowed_merge(*parent_stmt, 
                                              *child_stmt, 
                                              force_merge, 
                                              force_no_merge))) {
    LOG_WARN("failed to check hint allowed merge", K(ret));
  } else if (force_merge) {
    need_merge = true;
  } else if (ObTransformUtils::check_joined_table_combinable(parent_stmt, 
                                                             joined_table, 
                                                             view_table, 
                                                             true,
                                                             need_merge)) {
    LOG_WARN("failed to check joined table combinable", K(ret));
  } else if (!need_merge) {
    OPT_TRACE("right tables not combinable, no need merge view");
  }
  return ret;
}

int ObTransformViewMerge::find_not_null_column(ObDMLStmt &parent_stmt,
                                              ObSelectStmt &child_stmt,
                                              ViewMergeHelper &helper,
                                              ObIArray<ObRawExpr *> &column_exprs,
                                              bool &can_be)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (OB_FAIL(find_not_null_column_with_condition(parent_stmt,
                                                  child_stmt,
                                                  helper,
                                                  column_exprs))) {
    LOG_WARN("failed to find not null column with join condition", K(ret));
  } else if (OB_NOT_NULL(helper.not_null_column)) {
    //find not null column, do nothing
  } else if (OB_FAIL(ObTransformUtils::find_not_null_expr(child_stmt,
                                                          helper.not_null_column,
                                                          is_valid,
                                                          ctx_))) {
    LOG_WARN("failed to find not null expr", K(ret));
  }  else if (is_valid) {
    //find not null column, do nothing
  } else {
    can_be = false;
  }
  return ret;
}

int ObTransformViewMerge::find_not_null_column_with_condition(
                                        ObDMLStmt &parent_stmt,
                                        ObSelectStmt &child_stmt,
                                        ViewMergeHelper &helper,
                                        ObIArray<ObRawExpr *> &column_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSEArray<ObColumnRefRawExpr *, 16> temp_exprs;
  if (OB_ISNULL(helper.parent_table) || OB_ISNULL(helper.trans_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(join_conditions.assign(helper.parent_table->join_conditions_))) {
    LOG_WARN("failed to assign join conditions");
  } else if (OB_FAIL(parent_stmt.get_column_exprs(helper.trans_table->table_id_, temp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(old_column_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(old_column_exprs,
                                                                          child_stmt,
                                                                          new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < old_column_exprs.count(); ++i) {
      bool has_null_reject = false;
      //首先找到null reject的select expr
      if (OB_FAIL(ObTransformUtils::has_null_reject_condition(join_conditions,
                                                              old_column_exprs.at(i),
                                                              has_null_reject))) {
        LOG_WARN("failed to check has null reject condition", K(ret));
      } else if (!has_null_reject) {
        //do nothing
      } else if (OB_FAIL(find_null_propagate_column(new_column_exprs.at(i),
                                                    column_exprs,
                                                    helper.not_null_column,
                                                    find))) {
        LOG_WARN("failed to find null propagate column", K(ret));
      } else {
        //在对应的select expr中找到了null propagate的column expr
        //do nothing
      }
    }
  }
  return ret;
}

int ObTransformViewMerge::find_null_propagate_column(ObRawExpr *condition,
                                                    ObIArray<ObRawExpr*> &columns,
                                                    ObRawExpr *&null_propagate_column,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  null_propagate_column = NULL;
  bool is_null_propagate = false;
  ObSEArray<const ObRawExpr *, 4> dummy_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < columns.count(); ++i) {
    dummy_exprs.reuse();
    if (OB_FAIL(dummy_exprs.push_back(columns.at(i)))) {
      LOG_WARN("failed to push back column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(condition,
                                                                dummy_exprs,
                                                                is_null_propagate))) {
      LOG_WARN("failed to check null propagate expr", K(ret));                                                    
    } else if (!is_null_propagate) {
      //do nothing
    } else {
      null_propagate_column = columns.at(i);
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformViewMerge::transform_joined_table(ObDMLStmt *stmt,
                                                 JoinedTable *joined_table,
                                                 bool parent_can_push_where,
                                                 ObIArray<ObSelectStmt*> &merged_stmts,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) ||
      OB_ISNULL(left_table = joined_table->left_table_) ||
      OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(stmt), K(joined_table), K(left_table), K(right_table), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (joined_table->joined_type_ == CONNECT_BY_JOIN) {
    // do nothing
  } else {
    //处理左表
    bool can_push_where = true;
    bool need_check_where_condi = false;
    if (!parent_can_push_where ||
        joined_table->joined_type_ == FULL_OUTER_JOIN ||
        joined_table->joined_type_ == RIGHT_OUTER_JOIN) {
      can_push_where = false;
      if (joined_table->joined_type_ == LEFT_OUTER_JOIN ||
          joined_table->joined_type_ == INNER_JOIN ||
          joined_table->joined_type_ == FULL_OUTER_JOIN) {
        need_check_where_condi = true;
      }
    }
    bool is_happened = false;
    if (left_table->is_joined_table()) {
      JoinedTable *j_table = static_cast<JoinedTable*>(left_table);
      if (OB_FAIL(SMART_CALL(transform_joined_table(stmt,
                                                    j_table,
                                                    can_push_where,
                                                    merged_stmts,
                                                    is_happened)))) {
        LOG_WARN("failed to transform joined table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    } else if (OB_FAIL(transform_generated_table(stmt,
                                                 joined_table,
                                                 left_table,
                                                 need_check_where_condi,
                                                 can_push_where,
                                                 merged_stmts,
                                                 is_happened))) {
      LOG_WARN("failed to transform basic table", K(ret));
    } else {
      trans_happened |= is_happened;
    }
    //处理右表
    if (OB_SUCC(ret)) {
      can_push_where = true;
      need_check_where_condi = false;
      if (!parent_can_push_where ||
          joined_table->joined_type_ == FULL_OUTER_JOIN ||
          joined_table->joined_type_ == LEFT_OUTER_JOIN) {
        can_push_where = false;
        if (joined_table->joined_type_ == RIGHT_OUTER_JOIN ||
            joined_table->joined_type_ == INNER_JOIN ||
            joined_table->joined_type_ == FULL_OUTER_JOIN) {
          need_check_where_condi = true;
        }
      }
      if (right_table->is_joined_table()) {
        JoinedTable *j_table = static_cast<JoinedTable*>(right_table);
        if (OB_FAIL(SMART_CALL(transform_joined_table(stmt,
                                                      j_table,
                                                      can_push_where,
                                                      merged_stmts,
                                                      is_happened)))) {
          LOG_WARN("failed to transform joined table", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      } else if (OB_FAIL(transform_generated_table(stmt,
                                                   joined_table,
                                                   right_table,
                                                   need_check_where_condi,
                                                   can_push_where,
                                                   merged_stmts,
                                                   is_happened))) {
        LOG_WARN("failed to transform basic table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    //处理joined table single table ids
    if (OB_SUCC(ret)) {
      joined_table->single_table_ids_.reset();
      if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*joined_table,
                                                                      *left_table))) {
        LOG_WARN("failed to add joined table single table ids",K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*joined_table,
                                                                             *right_table))) {
        LOG_WARN("failed to add joined table single table ids",K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObTransformViewMerge::create_joined_table_for_view(ObSelectStmt *child_stmt,
                                                       TableItem *&new_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(child_stmt), K(ret));
  } else {
    new_table = NULL;
    TableItem *right_table = NULL;
    const ObSEArray<ObRawExpr *, 4> joined_conds;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_from_item_size(); ++i) {
      FromItem &cur_from = child_stmt->get_from_item(i);
      if (cur_from.is_joined_) {
        right_table = child_stmt->get_joined_table(cur_from.table_id_);
      } else {
        right_table = child_stmt->get_table_item_by_id(cur_from.table_id_);
      }
      if (i == 0) {
        new_table = right_table;
      } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                                                                *child_stmt,
                                                                INNER_JOIN,
                                                                new_table,
                                                                right_table,
                                                                joined_conds,
                                                                new_table,
                                                                false))) {
        LOG_WARN("failed to add new joined table", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformViewMerge::do_view_merge(ObDMLStmt *parent_stmt,
                                                       ObSelectStmt *child_stmt,
                                                       TableItem *table_item,
                                                       ViewMergeHelper &helper)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = helper.parent_table;
  if (OB_ISNULL(ctx_) || OB_ISNULL(parent_stmt)
      || OB_ISNULL(child_stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ctx_), K(parent_stmt), K(child_stmt), K(table_item), K(ret));
    // adjsut hint, replace name after merge stmt hint.
  } else if (OB_FAIL(parent_stmt->get_stmt_hint().merge_stmt_hint(child_stmt->get_stmt_hint(),
                                                                  LEFT_HINT_DOMINATED))) {
    LOG_WARN("failed to merge view stmt hint", K(ret));
  } else if (OB_FAIL(parent_stmt->get_stmt_hint().replace_name_for_single_table_view(ctx_->allocator_,
                                                                                     *parent_stmt,
                                                                                     *table_item))) {
    LOG_WARN("failed to replace name for single table view", K(ret));
  } else if (OB_NOT_NULL(joined_table)) {//调整joined table中generated table需要单独处理的部分
    if (OB_FAIL(ObOptimizerUtil::remove_item(joined_table->single_table_ids_,
                                            table_item->table_id_))) {
      LOG_WARN("failed to remove table id", K(ret));
    } else if (joined_table->left_table_ == table_item &&
               OB_FAIL(create_joined_table_for_view(child_stmt, joined_table->left_table_))) {
      LOG_WARN("failed to create joined table for view", K(ret));
    } else if (joined_table->right_table_ == table_item &&
               OB_FAIL(create_joined_table_for_view(child_stmt, joined_table->right_table_))) {
      LOG_WARN("failed to create joined table for view", K(ret));
    } else {/*do nothing*/}
  } else {//调整普通generated table需要单独处理的部分
    if (OB_FAIL(append(parent_stmt->get_from_items(), child_stmt->get_from_items()))) {
      LOG_WARN("failed to append from items", K(ret));
    } else if (OB_FAIL(append(parent_stmt->get_joined_tables(), child_stmt->get_joined_tables()))) {
      LOG_WARN("failed to append joined tables", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (parent_stmt->is_select_stmt() && 
        child_stmt->is_select_stmt() &&
        child_stmt->is_hierarchical_query()) {
      ObSelectStmt *parent_sel_stmt = static_cast<ObSelectStmt *>(parent_stmt);
      ObSelectStmt *child_sel_stmt = static_cast<ObSelectStmt *>(child_stmt);
      parent_sel_stmt->set_order_siblings(child_sel_stmt->is_order_siblings());
      parent_sel_stmt->set_has_prior(child_sel_stmt->has_prior());
      parent_sel_stmt->set_hierarchical_query(true);
      parent_sel_stmt->set_nocycle(child_sel_stmt->is_nocycle());
      if (OB_FAIL(append(parent_sel_stmt->get_order_items(), child_sel_stmt->get_order_items()))) {
        LOG_WARN("failed to append order items", K(ret));
      } else if (OB_FAIL(append(parent_sel_stmt->get_connect_by_prior_exprs(),
                                child_sel_stmt->get_connect_by_prior_exprs()))) {
        LOG_WARN("failed to append connect by prior exprs", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {//调整公共部分
    if (OB_FAIL(replace_stmt_exprs(parent_stmt,
                                   child_stmt,
                                   table_item,
                                   helper,
                                   helper.need_check_null_propagate))) {
      LOG_WARN("failed to replace stmt exprs", K(ret));
    } else if (OB_FAIL(adjust_updatable_view(parent_stmt, table_item))) {
      LOG_WARN("failed to adjust updatable view", K(ret));
    } else if (OB_FAIL(append(parent_stmt->get_table_items(), child_stmt->get_table_items()))) {
      LOG_WARN("failed to append table items", K(ret));
    } else if (OB_FAIL(parent_stmt->remove_table_info(table_item))) {
      LOG_WARN("failed to remove table item", K(ret));
    } else if (OB_FAIL(append(parent_stmt->get_column_items(), child_stmt->get_column_items()))) {
      LOG_WARN("failed to append column items", K(ret));
    } else if (OB_FAIL(parent_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(parent_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(adjust_stmt_semi_infos(parent_stmt, child_stmt, table_item->table_id_))) {
      LOG_WARN("failed to adjust stmt semi infos", K(ret));
    } else if (OB_FAIL(append(parent_stmt->get_semi_infos(), child_stmt->get_semi_infos()))) {
      LOG_WARN("failed to append semi infos", K(ret));
    } else if (helper.can_push_where && OB_FAIL(append(parent_stmt->get_condition_exprs(),
                                                child_stmt->get_condition_exprs()))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    //joined table中的generated table的where不能直接下压到外层where里面的，只能下压到连接条件（on）里面
    } else if (!helper.can_push_where && OB_FAIL(append(joined_table->get_join_conditions(),
                                                 child_stmt->get_condition_exprs()))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    } else if (OB_FAIL(append(parent_stmt->get_part_exprs(), child_stmt->get_part_exprs()))) {
      LOG_WARN("failed to append part exprs", K(ret));
    } else if (OB_FAIL(append(parent_stmt->get_check_constraint_items(),
                              child_stmt->get_check_constraint_items()))) {
      LOG_WARN("failed to append check constraint items", K(ret));
    } else if (parent_stmt->is_select_stmt() &&
              OB_FAIL(append(static_cast<ObSelectStmt *>(parent_stmt)->get_sample_infos(),
                             child_stmt->get_sample_infos()))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::pull_up_subquery(parent_stmt, child_stmt))) {
      LOG_WARN("failed to pull up subquery", K(ret));
    } else if (OB_FAIL(parent_stmt->remove_from_item(table_item->table_id_))) {
      LOG_WARN("failed to remove from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*parent_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(parent_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(parent_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformViewMerge::adjust_updatable_view(ObDMLStmt *parent_stmt, TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_stmt), K(table_item), K(ret));
  } else if (OB_UNLIKELY(!table_item->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(table_item->type_), K(ret));
  } else if (parent_stmt->is_update_stmt() &&
             OB_FAIL(static_cast<ObUpdateStmt*>(parent_stmt)->remove_invalid_assignment())) {
    LOG_WARN("failed to remove invalid assignment", K(ret));
  } else if (parent_stmt->is_delete_stmt() || parent_stmt->is_update_stmt()) {
    ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt *>(parent_stmt);
    ObSEArray<ObDmlTableInfo*, 2> table_infos;
    if (OB_FAIL(del_upd_stmt->get_dml_table_infos(table_infos))) {
      LOG_WARN("failed to get dml table infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObDmlTableInfo* table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      } else if (table_info->table_id_ == table_item->table_id_) {
        if (OB_UNLIKELY(table_info->column_exprs_.empty()) ||
            OB_ISNULL(table_info->column_exprs_.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index dml info is invalid", K(ret), KPC(table_info));
        } else {
          table_info->table_id_ = table_info->column_exprs_.at(0)->get_table_id();
        }
      }
    }
  }
  return ret;
}

int ObTransformViewMerge::replace_stmt_exprs(ObDMLStmt *parent_stmt,
                                             ObSelectStmt *child_stmt,
                                             TableItem *table_item,
                                             ViewMergeHelper &helper,
                                             bool need_wrap_case_when)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSEArray<ObColumnRefRawExpr *, 16> temp_exprs;
  ObStmtExprReplacer replacer;
  replacer.set_recursive(false);
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->get_column_exprs(table_item->table_id_, temp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(old_column_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(old_column_exprs,
                                                                          *child_stmt,
                                                                          new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else if (need_wrap_case_when &&
             OB_FAIL(wrap_case_when_if_necessary(*child_stmt, helper, new_column_exprs))) {
    LOG_WARN("failed to wrap case when is necessary", K(ret));
  } else if (OB_FAIL(parent_stmt->remove_table_info(table_item))) {
    LOG_WARN("failed to remove table item", K(ret));
  } else if (OB_FAIL(replacer.add_replace_exprs(old_column_exprs, new_column_exprs))) {
    LOG_WARN("failed to add replace exprs", K(ret));
  } else if (OB_FAIL(parent_stmt->iterate_stmt_expr(replacer))) {
    LOG_WARN("failed to replace stmt expr", K(ret));
  }
  return ret;
}

int ObTransformViewMerge::wrap_case_when_if_necessary(ObSelectStmt &child_stmt,
                                                      ViewMergeHelper &helper,
                                                      ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSqlBitSet<> from_tables;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_FAIL(child_stmt.get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else if (OB_FAIL(child_stmt.get_column_exprs(columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(child_stmt,
                                                          columns,
                                                          from_tables,
                                                          column_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      bool is_null_propagate = false;
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(exprs.at(i),
                                                                  column_exprs,
                                                                  is_null_propagate))) {
        LOG_WARN("failed to is null propagate expr", K(ret));
      } else if (is_null_propagate) {
        //do nothing
      } else if (OB_FAIL(wrap_case_when(child_stmt, helper.not_null_column, exprs.at(i)))) {
        LOG_WARN("failed to wrap case when", K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}


int ObTransformViewMerge::wrap_case_when(ObSelectStmt &child_stmt,
                                        ObRawExpr *not_null_column,
                                        ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ctx_), K(ret));
  } else if (OB_ISNULL(not_null_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null column expr", K(ret));
  } else {
    ObRawExpr *null_expr = NULL;
    ObRawExpr *cast_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObRawExprFactory *factory = ctx_->expr_factory_;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(*factory, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_ISNULL(null_expr) || OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                              ctx_->expr_factory_,
                              ctx_->session_info_,
                              *null_expr,
                              expr->get_result_type(),
                              cast_expr))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(child_stmt,
                                                              not_null_column,
                                                              expr,
                                                              cast_expr,
                                                              case_when_expr,
                                                              ctx_))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_ISNULL(case_when_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("case expr is null", K(ret), K(case_when_expr));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *case_when_expr,
                                                               expr->get_result_type(),
                                                               expr))) {
      LOG_WARN("failed to add cast expr above", K(ret));
    }
  }
  return ret;
}

int ObTransformViewMerge::adjust_stmt_semi_infos(ObDMLStmt *parent_stmt,
                                                 ObSelectStmt *child_stmt,
                                                 uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_stmt),
        K(child_stmt), K(ret));
  } else {
    ObSEArray<uint64_t, 4> table_ids;
    bool removed = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_from_item_size(); ++i) {
      FromItem &from = child_stmt->get_from_item(i);
      if (from.is_joined_) {
        JoinedTable *table = child_stmt->get_joined_table(from.table_id_);
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get joined table", K(ret));
        } else if (OB_FAIL(append(table_ids, table->single_table_ids_))) {
          LOG_WARN("failed to get joined table", K(ret));
        }
      } else if (OB_FAIL(table_ids.push_back(from.table_id_))) {
        LOG_WARN("failed to push back table id", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_stmt->get_semi_info_size(); i++) {
      SemiInfo *semi_info = parent_stmt->get_semi_infos().at(i);
      if (OB_ISNULL(semi_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_info->left_table_ids_, table_id,
                                                      &removed))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (!removed) {
        // do nothing
      } else if (OB_FAIL(append(semi_info->left_table_ids_, table_ids))) {
        LOG_WARN("failed append table id", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformViewMerge::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*> *merged_stmts = NULL;
  UNUSED(stmt);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params)
      || OB_ISNULL(merged_stmts = static_cast<ObIArray<ObSelectStmt*>*>(trans_params))
      || OB_UNLIKELY(merged_stmts->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params), K(merged_stmts));
  } else {
    ObViewMergeHint *hint = NULL;
    ObDMLStmt *child_stmt = NULL;
    ObString child_qb_name;
    const ObViewMergeHint *myhint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < merged_stmts->count(); ++i) {
      if (OB_ISNULL(child_stmt = merged_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child_stmt));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_MERGE_HINT, hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(child_stmt->get_qb_name(child_qb_name))) {
        LOG_WARN("failed to get qb name", K(ret), K(child_stmt->get_stmt_id()));
      } else if (OB_FAIL(ctx_->add_src_hash_val(child_qb_name))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (NULL != (myhint = static_cast<const ObViewMergeHint*>(get_hint(child_stmt->get_stmt_hint())))
                 && myhint->enable_view_merge(ctx_->src_qb_name_)
                 && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else {
        hint->set_qb_name(child_qb_name);
        hint->set_parent_qb_name(ctx_->src_qb_name_);
      }
    }
  }
  return ret;
}

}//namespace sql
}//namespqce oceanbase
