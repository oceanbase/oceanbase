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
#include "sql/rewrite/ob_transform_post_process.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_view_merge.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObTransformPostProcess::transform_one_stmt(
    ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool is_happened = false;
  trans_happened = false;
  if (OB_FAIL(extract_calculable_expr(stmt, is_happened))) {
    LOG_WARN("failed to extract calculable expr", K(ret));
  } else {
    trans_happened |= is_happened;
    LOG_TRACE("succeed to extract calculable expr", K(is_happened));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_for_hierarchical_query(stmt, is_happened))) {
      LOG_WARN("failed to transform hierarchical query", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to transform hierarchical query in post", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pullup_hierarchical_query(stmt, is_happened))) {
      LOG_WARN("failed to pullup hierarchical query", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to pullup hierarchical query", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extract_calculable_expr(stmt, is_happened))) {
      LOG_WARN("failed to extract calculable expr", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to extract calculable expr", K(is_happened));
    }
  }
  return ret;
}

int ObTransformPostProcess::extract_calculable_expr(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> exprs;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
    LOG_WARN("failed to get all relation exprs", K(ret));
  } else {
    int64_t old_count = stmt->get_calculable_exprs().count();
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      if (OB_FAIL(ObSQLUtils::convert_calculable_expr_to_question_mark(*stmt, exprs.at(i), *ctx_))) {
        LOG_WARN("Failed to convert calculable expr to question mark", K(ret));
      }
    }
    if (OB_SUCC(ret) && old_count != stmt->get_calculable_exprs().count()) {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPostProcess::transform_for_hierarchical_query(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_select_stmt() || !stmt->is_hierarchical_query())) {
    // do nothing
  } else if (OB_UNLIKELY(stmt->get_from_item_size() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect from item size", K(stmt->get_from_item_size()), K(ret));
  } else {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    TableItem* left_table = NULL;
    TableItem* right_table = NULL;
    FromItem left_item = sel_stmt->get_from_item(0);
    FromItem right_item = sel_stmt->get_from_item(1);
    if (OB_UNLIKELY(left_item.is_joined_ || right_item.is_joined_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect joined table", K(ret));
    } else if (OB_ISNULL(left_table = sel_stmt->get_table_item(left_item))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_ISNULL(right_table = sel_stmt->get_table_item(right_item))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_UNLIKELY(!left_table->is_generated_table() || !right_table->is_generated_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect basic table item", K(ret));
    } else if (OB_FAIL(transform_prior_exprs(*sel_stmt, *left_table, *right_table))) {
      LOG_WARN("failed to transform prior exprs", K(ret));
    } else if (OB_FAIL(make_connect_by_joined_table(*sel_stmt, *left_table, *right_table))) {
      LOG_WARN("failed to make connect by joined table", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPostProcess::pullup_hierarchical_query(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  ObSelectStmt* subquery = NULL;
  ObSEArray<ObParentDMLStmt, 2> dummy_parent_stmts;
  ObTransformViewMerge view_merge(ctx_);
  view_merge.set_for_post_process(true);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(stmt->get_from_item_size() != 1)) {
    // do nothing
  } else if (OB_ISNULL(table = stmt->get_table_item(stmt->get_from_item(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_UNLIKELY(!table->is_generated_table())) {
    // do nothing
  } else if (OB_ISNULL(subquery = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null query ref", K(ret));
  } else if (OB_UNLIKELY(!subquery->is_hierarchical_query()) || subquery->get_from_item_size() != 1 ||
             stmt->get_semi_info_size() > 0) {
  } else if (OB_FAIL(view_merge.transform_one_stmt(dummy_parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to merge view for hierarchical query", K(ret));
  } else {
    LOG_TRACE("success to merge view for hierarchical query", K(trans_happened));
  }
  return ret;
}

int ObTransformPostProcess::transform_prior_exprs(ObSelectStmt& stmt, TableItem& left_table, TableItem& right_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> connect_by_prior_exprs;
  ObSEArray<ObRawExpr*, 4> prior_exprs;
  ObSEArray<ObRawExpr*, 4> converted_exprs;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  bool use_static_engine = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret));
  } else if (OB_FAIL(get_prior_exprs(stmt.get_connect_by_exprs(), connect_by_prior_exprs))) {
    LOG_WARN("failed to get prior exprs", K(ret));
  } else if (FALSE_IT(use_static_engine = ctx_->session_info_->use_static_typing_engine())) {
  } else if (use_static_engine) {
    for (int64_t i = 0; i < connect_by_prior_exprs.count() && OB_SUCC(ret); i++) {
      ObRawExpr* raw_expr = connect_by_prior_exprs.at(i);
      ObRawExpr* copy_raw_expr = NULL;
      if (OB_ISNULL(raw_expr) || OB_UNLIKELY(1 != raw_expr->get_param_count()) ||
          OB_ISNULL(raw_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                     *ctx_->expr_factory_, raw_expr->get_param_expr(0), copy_raw_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("copy expr failed", K(ret));
      } else if (OB_FAIL(stmt.add_connect_by_prior_expr(copy_raw_expr))) {
        LOG_WARN("failed to add connect by prior expr", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("get stmt relation exprs fail", K(ret));
  } else if (OB_FAIL(get_prior_exprs(relation_exprs, prior_exprs))) {
    LOG_WARN("failed to get prior exprs", K(ret));
  } else if (OB_FAIL(modify_prior_exprs(stmt, left_table, right_table, prior_exprs, converted_exprs))) {
    LOG_WARN("failed to modify prior exprs");
  } else if (OB_FAIL(stmt.replace_inner_stmt_expr(prior_exprs, converted_exprs))) {
    LOG_WARN("failed to replace stmt expr", K(ret));
  } else if (!use_static_engine) {
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_prior_exprs.count(); ++i) {
      int64_t idx = -1;
      if (!ObOptimizerUtil::find_item(prior_exprs, connect_by_prior_exprs.at(i), &idx) ||
          OB_UNLIKELY(idx < 0 || idx >= prior_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("does not find joined prior expr", K(ret), K(idx), K(prior_exprs.count()));
      } else if (prior_exprs.at(idx) == converted_exprs.at(idx)) {
        // do nothing
      } else if (OB_FAIL(stmt.add_connect_by_prior_expr(converted_exprs.at(idx)))) {
        LOG_WARN("failed to add connect by prior expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPostProcess::get_prior_exprs(ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& prior_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(get_prior_exprs(exprs.at(i), prior_exprs))) {
      LOG_WARN("failed to get prior exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPostProcess::get_prior_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& prior_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_OP_PRIOR == expr->get_expr_type()) {
    if (OB_FAIL(add_var_to_array_no_dup(prior_exprs, expr))) {
      LOG_WARN("failed to add prior exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_prior_exprs(expr->get_param_expr(i), prior_exprs)))) {
        LOG_WARN("failed to get prior exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPostProcess::modify_prior_exprs(ObSelectStmt& stmt, TableItem& left_table, TableItem& right_table,
    ObIArray<ObRawExpr*>& prior_exprs, ObIArray<ObRawExpr*>& convert_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> left_columns;
  ObSEArray<ObRawExpr*, 8> right_columns;
  ObSelectStmt* left_stmt = NULL;
  ObSelectStmt* right_stmt = NULL;
  if (OB_UNLIKELY(!left_table.is_generated_table() || !right_table.is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect genereated table", K(left_table), K(right_table), K(ret));
  } else if (OB_ISNULL(left_stmt = left_table.ref_query_) || OB_ISNULL(right_stmt = right_table.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(left_stmt), K(right_stmt), K(ret));
  } else if (OB_UNLIKELY(left_stmt->get_select_item_size() != right_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select item size is incorrect", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_stmt->get_select_item_size(); ++i) {
      uint64_t column_id = i + OB_APP_MIN_COLUMN_ID;
      ObColumnRefRawExpr* left_column = NULL;
      ObColumnRefRawExpr* right_column = NULL;
      if (OB_ISNULL(left_column = stmt.get_column_expr_by_id(left_table.table_id_, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not find left table`s column", K(column_id), K(ret));
      } else if (OB_ISNULL(right_column = stmt.get_column_expr_by_id(right_table.table_id_, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not find right table`s column", K(column_id), K(ret));
      } else if (OB_FAIL(left_columns.push_back(left_column))) {
        LOG_WARN("failed to push back column expr");
      } else if (OB_FAIL(right_columns.push_back(right_column))) {
        LOG_WARN("failed to push back column expr");
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(right_columns, left_columns, prior_exprs))) {
    LOG_WARN("failed to replace column expr", K(ret));
  } else {
    for (int64_t i = 0; i < prior_exprs.count() && OB_SUCC(ret); i++) {
      ObRawExpr* raw_expr = prior_exprs.at(i);
      if (OB_ISNULL(raw_expr) || OB_UNLIKELY(1 != raw_expr->get_param_count()) ||
          OB_ISNULL(raw_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (!raw_expr->get_expr_levels().has_member(stmt.get_current_level())) {
        // do nothing
      } else {
        raw_expr = raw_expr->get_param_expr(0);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(convert_exprs.push_back(raw_expr))) {
        LOG_WARN("failed to push back prior param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPostProcess::make_connect_by_joined_table(
    ObSelectStmt& stmt, TableItem& left_table, TableItem& right_table)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  JoinedTable* joined_table = NULL;
  ObQueryCtx* query_ctx = stmt.get_query_ctx();
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_ctx) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter has NULL", K(query_ctx), K(ctx_), K(ret));
  } else if (OB_UNLIKELY(NULL == (ptr = ctx_->allocator_->alloc(sizeof(JoinedTable))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for JoinedTable failed", "size", sizeof(JoinedTable), K(ret));
  } else if (OB_ISNULL(joined_table = new (ptr) JoinedTable())) {
    LOG_WARN("unexpect null joined table", K(ret));
  } else {
    joined_table->table_id_ = query_ctx->available_tb_id_--;
    joined_table->type_ = TableItem::JOINED_TABLE;
    joined_table->joined_type_ = CONNECT_BY_JOIN;
    joined_table->left_table_ = &left_table;
    joined_table->right_table_ = &right_table;
    if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(joined_table))) {
      LOG_WARN("failed to adjust single table ids", K(ret));
    } else if (OB_FAIL(joined_table->join_conditions_.assign(stmt.get_connect_by_exprs()))) {
      LOG_WARN("fail to assign connect by exprs", K(ret));
    } else {
      stmt.clear_connect_by_exprs();
      stmt.clear_from_items();
      stmt.get_joined_tables().reset();
      if (OB_FAIL(stmt.add_joined_table(joined_table))) {
        LOG_WARN("fail to add joined table", KPC(joined_table), K(ret));
      } else if (OB_FAIL(stmt.add_from_item(joined_table->table_id_, true))) {
        LOG_WARN("add from item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPostProcess::merge_mock_view(ObDMLStmt* stmt, TableItem& table)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* subquery = table.ref_query_;
  bool has_dup_expr = false;
  bool is_all_column_expr = false;
  bool trans_happened = false;
  ObSEArray<ObParentDMLStmt, 2> dummy_parent_stmts;
  ObTransformViewMerge view_merge(ctx_);
  view_merge.set_for_post_process(true);
  if (OB_ISNULL(subquery) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null query ref", K(ret));
  } else if (OB_FAIL(check_select_item(*subquery, has_dup_expr, is_all_column_expr))) {
  } else if (has_dup_expr || !is_all_column_expr) {
    // do nothing
  } else if (OB_FAIL(view_merge.transform_one_stmt(dummy_parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to merge mock view", K(ret));
  } else {
    LOG_TRACE("success to view merge for hierarchical query", K(trans_happened));
  }
  return ret;
}

int ObTransformPostProcess::check_select_item(ObSelectStmt& stmt, bool& has_dup_expr, bool& is_all_column_expr)
{
  int ret = OB_SUCCESS;
  has_dup_expr = false;
  is_all_column_expr = true;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr* expr = stmt.get_select_item(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->is_column_ref_expr()) {
      is_all_column_expr = false;
    } else if (ObOptimizerUtil::find_item(select_exprs, expr)) {
      has_dup_expr = true;
    } else if (OB_FAIL(add_var_to_array_no_dup(select_exprs, expr))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
