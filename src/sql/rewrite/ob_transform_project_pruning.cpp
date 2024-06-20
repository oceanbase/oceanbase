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
#include "sql/rewrite/ob_transform_project_pruning.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObTransformProjectPruning::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                  ObDMLStmt *&stmt,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(transform_table_items(stmt, false, trans_happened))) {
    LOG_WARN("failed to transform table items", K(ret));
  }
  return ret;
}

int ObTransformProjectPruning::transform_one_stmt_with_outline(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                              ObDMLStmt *&stmt,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(transform_table_items(stmt, true, trans_happened))) {
    LOG_WARN("failed to transform table items", K(ret));
  } else if (!trans_happened) {
    //do nothing
  } else {
    trans_happened = true;
    LOG_TRACE("succeed to do project prune with outline", K(ctx_->src_qb_name_));
  }
  return ret;
}

int ObTransformProjectPruning::transform_table_items(ObDMLStmt *&stmt,
                                                    bool with_outline,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_insert_stmt()) {
    //do nothing
    OPT_TRACE("insert stmt can not transform");
  } else {
    //traverse table items(all table items are in from items)
    ObIArray<TableItem*> &table_items = stmt->get_table_items();
    TableItem *table_item = NULL;
    ObSEArray<ObSelectStmt*, 4> transed_stmts;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_items.count(); ++idx) {
      bool is_valid = false;
      if (OB_ISNULL(table_item = table_items.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Table item is NULL in table items", K(ret));
      } else if (table_item->is_generated_table() ||
                 table_item->is_lateral_table()) {
        ObSelectStmt *ref_query = NULL;
        OPT_TRACE("try to prune preject for view:", table_item);
        if (OB_ISNULL(ref_query = table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Ref query of generate_table is NULL", K(ret));
        } else if (OB_FAIL(check_hint_allowed_prune(*ref_query, is_valid))) {
          LOG_WARN("failed to check hint allowed prune", K(ret));
        } else if (!is_valid) {
          //do nothing
          OPT_TRACE("hint reject transform");
        } else if (OB_FAIL(ObTransformUtils::check_project_pruning_validity(*ref_query, is_valid))) {
          LOG_WARN("failed to check transform valid", K(ret));
        } else if (!is_valid) {
          //do nothing
        } else if (OB_FAIL(project_pruning(table_item->table_id_,
                                           *ref_query,
                                           *stmt,
                                           is_happened))) {
          LOG_WARN("Failed to project pruning generated table", K(ret));
        } else if (!is_happened) {
          //do nothing
        } else if (OB_FAIL(transed_stmts.push_back(ref_query))) {
          LOG_WARN("failed to push back transed stmts", K(ret));
        } else {
          trans_happened = true;
          if (with_outline) {
            ++ctx_->trans_list_loc_;
          }
        }
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && !transed_stmts.empty()
        && OB_FAIL(add_transform_hint(*stmt, &transed_stmts))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformProjectPruning::is_const_expr(ObRawExpr* expr, bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->is_const_expr()) {
    is_const = true;
  } else if (expr->is_set_op_expr()) {
    is_const = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_const_expr(expr->get_param_expr(i), is_const)))) {
        LOG_WARN("failed to check is const expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformProjectPruning::check_transform_validity(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool is_const = false;
  bool has_assign = true;
  if (stmt.has_distinct() || stmt.is_scala_group_by() ||
      stmt.is_recursive_union() || (stmt.is_set_stmt() && stmt.is_set_distinct()) ||
      stmt.is_hierarchical_query()) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::check_has_assignment(stmt, has_assign))) {
    LOG_WARN("check has assign failed", K(ret));
  } else if (has_assign) {
    //do nothing
  } else if (stmt.get_select_item_size() == 1
             && OB_FAIL(is_const_expr(stmt.get_select_item(0).expr_, is_const))) {
      LOG_WARN("failed to check is const expr", K(ret));
  } else if (is_const) {
    // do nothing, only with a dummy output
  } else if (stmt.is_set_stmt()) {
    is_valid = true;
    const ObIArray<ObSelectStmt*> &child_stmts = stmt.get_set_query();
    ObRawExpr *order_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child query is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_transform_validity(*child_stmts.at(i), is_valid)))) {
        LOG_WARN("failed to check transform validity", K(ret));
      }
    }
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformProjectPruning::project_pruning(const uint64_t table_id,
                                               ObSelectStmt &child_stmt,
                                               ObDMLStmt &upper_stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> removed_idx;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt.get_select_item_size(); i++) {
    bool need_remove = false;
    if (OB_ISNULL(upper_stmt.get_column_item_by_id(table_id, i + OB_APP_MIN_COLUMN_ID))
        && OB_FAIL(ObTransformUtils::check_select_item_need_remove(&child_stmt, i, need_remove))) {
      LOG_WARN("fail to check column in set ordrt by", K(ret));
    } else if (need_remove) {
      ret = removed_idx.add_member(i);
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret) && !removed_idx.is_empty()) {
    if (OB_FAIL(ObTransformUtils::remove_select_items(ctx_,
                                                      table_id,
                                                      child_stmt,
                                                      upper_stmt,
                                                      removed_idx))) {
      LOG_WARN("failed to remove select items", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformProjectPruning::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    need_trans = true;
  } else if (NULL == (trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))
             || !trans_hint->is_project_prune_hint()) {
    /*do nothing*/
  } else {
    const TableItem *table = NULL;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
      if (OB_ISNULL(table = stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (!table->is_generated_table() &&
                 !table->is_lateral_table()) {
        /*do nothing*/
      } else if (OB_ISNULL(table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(*table));
      } else {
        need_trans = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                  get_hint(table->ref_query_->get_stmt_hint()));
      }
    }
  }
  return ret;
}

int ObTransformProjectPruning::check_hint_allowed_prune(ObSelectStmt &ref_query,
                                                        bool &allowed)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = NULL;
  const ObHint *myhint = get_hint(ref_query.get_stmt_hint());
  bool is_enable = (NULL != myhint && myhint->is_enable_hint());
  bool is_disable = (NULL != myhint && myhint->is_disable_hint());
  allowed = false;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = ref_query.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    const ObHint *no_rewrite_hint = ref_query.get_stmt_hint().get_no_rewrite_hint();
    if (is_enable) {
      allowed = true;
    } else if (NULL != no_rewrite_hint || is_disable) {
      if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite_hint))) {
        LOG_WARN("failed to add used transform hint", K(ret));
      } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used transform hint", K(ret));
      }
    } else {
      allowed = true;
    }
  } else if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint)) {
    allowed = true;
  }
  return ret;
}

int ObTransformProjectPruning::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*> *transed_stmts = static_cast<ObIArray<ObSelectStmt*>*>(trans_params);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(transed_stmts)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->add_src_hash_val(ctx_->src_qb_name_))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else {
    ObTransHint *hint = NULL;
    ObString qb_name;
    ObSelectStmt *cur_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < transed_stmts->count(); i++) {
      if (OB_ISNULL(cur_stmt = transed_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transed_stmt is null", K(ret), K(i));
      } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(cur_stmt->get_stmt_hint())))) {
        LOG_WARN("failed to add used hint", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, get_hint_type(), hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (OB_FAIL(cur_stmt->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(cur_stmt->adjust_qb_name(ctx_->allocator_,
                                                  qb_name,
                                                  ctx_->src_hash_val_))) {
        LOG_WARN("adjust stmt id failed", K(ret));
      } else {
        hint->set_qb_name(qb_name);
      }
    }
  }
  return ret;
}

}
}
