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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_view_merge.h"
#include "share/schema/ob_schema_struct.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObTransformPostProcess::transform_one_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                               ObDMLStmt *&stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool is_happened = false;
  trans_happened = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_for_hierarchical_query(stmt, is_happened))) {
      LOG_WARN("failed to transform hierarchical query", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to transform hierarchical query in post",  K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pullup_hierarchical_query(stmt, is_happened))) {
      LOG_WARN("failed to pullup hierarchical query", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to pullup hierarchical query",  K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_merge_into_subquery(stmt, is_happened))) {
      LOG_WARN("failed to transform merge into subquery", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to transform merge into subquery", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pullup_exec_exprs(stmt, is_happened))) {
      LOG_WARN("failed to pullup exec exprs", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to pullup exec exprs", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_onetime_subquery(stmt, is_happened))) {
      LOG_WARN("failed to extract onetime subquery", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to extract onetime subquery", K(ret), K(is_happened));
    }
  }
  return ret;
}

int ObTransformPostProcess::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                           const int64_t current_level,
                                           const ObDMLStmt &stmt,
                                           bool &need_trans)
{
  UNUSED(parent_stmts);
  UNUSED(current_level);
  UNUSED(stmt);
  need_trans = true;
  return OB_SUCCESS;
}

int ObTransformPostProcess::transform_for_hierarchical_query(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_select_stmt() || !stmt->is_hierarchical_query())) {
    //do nothing
  } else if (OB_UNLIKELY(stmt->get_from_item_size() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect from item size",K(stmt->get_from_item_size()), K(ret));
  } else {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt*>(stmt);
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
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

int ObTransformPostProcess::pullup_hierarchical_query(ObDMLStmt *stmt, bool &trans_happened)
{
  //需要检查子查询是否已经完成层次查询的后续改写
  int ret = OB_SUCCESS;
  TableItem *table = NULL;
  ObSelectStmt *subquery = NULL;
  ObSEArray<ObParentDMLStmt, 2> dummy_parent_stmts;
  ObTransformViewMerge view_merge(ctx_);
  view_merge.set_transformer_type(VIEW_MERGE);
  view_merge.set_for_post_process(true);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(stmt->get_from_item_size() != 1)) {
    //do nothing
  } else if (OB_ISNULL(table = stmt->get_table_item(stmt->get_from_item(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_UNLIKELY(!table->is_generated_table())) {
    //do nothing
  } else if (OB_ISNULL(subquery = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null query ref", K(ret));
  } else if (OB_UNLIKELY(!subquery->is_hierarchical_query()) || 
             subquery->get_from_item_size() != 1 ||
             stmt->get_semi_info_size() > 0) {
    //层次查询没有处理完成，do nothing
  } else if (OB_FAIL(view_merge.transform_self(dummy_parent_stmts, 0, stmt))) {
    LOG_WARN("failed to merge view for hierarchical query", K(ret));
  } else {
    trans_happened = view_merge.get_trans_happened();
    LOG_TRACE("success to merge view for hierarchical query", K(trans_happened));
  }
  return ret;
}

int ObTransformPostProcess::transform_prior_exprs(ObSelectStmt &stmt,
                                                  TableItem &left_table,
                                                  TableItem &right_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> connect_by_prior_exprs;
  ObSEArray<ObRawExpr *, 4> prior_exprs;
  ObSEArray<ObRawExpr *, 4> converted_exprs;
  ObSEArray<ObRawExpr *, 16> relation_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(ctx_));
  } else if (OB_FAIL(get_prior_exprs(stmt.get_connect_by_exprs(), connect_by_prior_exprs))) {
    LOG_WARN("failed to get prior exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_prior_exprs.count(); ++i) {
    ObRawExpr *raw_expr = connect_by_prior_exprs.at(i);
    ObRawExpr *param_expr = NULL;
    if (OB_ISNULL(raw_expr) ||
        OB_UNLIKELY(1 != raw_expr->get_param_count()) ||
        OB_ISNULL(param_expr = raw_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(stmt.add_connect_by_prior_expr(param_expr))) {
      LOG_WARN("failed to add connect by prior expr", K(ret));
    }
  }
  //创建column映射关系
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("get stmt relation exprs fail", K(ret));
  } else if (OB_FAIL(get_prior_exprs(relation_exprs, prior_exprs))) {
    LOG_WARN("failed to get prior exprs", K(ret));
  } else if (OB_FAIL(modify_prior_exprs(*ctx_->expr_factory_,
                                        stmt,
                                        left_table,
                                        right_table,
                                        prior_exprs,
                                        converted_exprs))) {
    LOG_WARN("failed to modify prior exprs");
  } else if (OB_FAIL(stmt.replace_inner_stmt_expr(prior_exprs, converted_exprs))) {
    LOG_WARN("failed to replace stmt expr", K(ret));
  }
  return ret;
}

int ObTransformPostProcess::get_prior_exprs(ObIArray<ObRawExpr *> &exprs,
                                            ObIArray<ObRawExpr *> &prior_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(get_prior_exprs(exprs.at(i), prior_exprs))) {
      LOG_WARN("failed to get prior exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPostProcess::get_prior_exprs(ObRawExpr *expr,
                                            ObIArray<ObRawExpr *> &prior_exprs)
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

int ObTransformPostProcess::modify_prior_exprs(ObRawExprFactory &expr_factory,
                                               ObSelectStmt &stmt,
                                               TableItem &left_table,
                                               TableItem &right_table,
                                               ObIArray<ObRawExpr *> &prior_exprs,
                                               ObIArray<ObRawExpr *> &convert_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *left_stmt = NULL;
  ObSelectStmt *right_stmt = NULL;
  ObRawExprCopier copier(expr_factory);
  if (OB_UNLIKELY(!left_table.is_generated_table() || !right_table.is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect genereated table", K(left_table), K(right_table), K(ret));
  } else if (OB_ISNULL(left_stmt = left_table.ref_query_) || 
             OB_ISNULL(right_stmt = right_table.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query",K(left_stmt), K(right_stmt), K(ret));
  } else if (OB_UNLIKELY(left_stmt->get_select_item_size() != right_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select item size is incorrect", K(ret));
  } else {
    //找到左右表一一对应的column expr
    for (int64_t i = 0; OB_SUCC(ret) && i < left_stmt->get_select_item_size(); ++i) {
      uint64_t column_id = i + OB_APP_MIN_COLUMN_ID;
      ObColumnRefRawExpr *left_column = NULL;
      ObColumnRefRawExpr *right_column = NULL;
      if (OB_ISNULL(left_column = stmt.get_column_expr_by_id(left_table.table_id_, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not find left table`s column", K(column_id), K(ret));
      } else if (OB_ISNULL(right_column = stmt.get_column_expr_by_id(right_table.table_id_, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not find right table`s column", K(column_id), K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(right_column, left_column))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
  }
  //更新prior表达式引用左表的列
  //取出prior表达式的参数
  for (int64_t i = 0; i < prior_exprs.count() && OB_SUCC(ret); i++) {
    ObRawExpr *raw_expr = prior_exprs.at(i);
    ObRawExpr *new_expr = NULL;
    if (OB_ISNULL(raw_expr) || OB_UNLIKELY(1 != raw_expr->get_param_count())
        || OB_ISNULL(raw_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (!raw_expr->get_expr_levels().has_member(stmt.get_current_level())) {
      // do nothing
    } else if (OB_FAIL(copier.copy_on_replace(raw_expr->get_param_expr(0), new_expr))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    } else {
      raw_expr = new_expr;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(convert_exprs.push_back(raw_expr))) {
      LOG_WARN("failed to push back prior param expr", K(ret));
    }
  }
  return ret;
}

int ObTransformPostProcess::make_connect_by_joined_table(ObSelectStmt &stmt,
                                                        TableItem &left_table,
                                                        TableItem &right_table)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  JoinedTable *joined_table = NULL;
  ObQueryCtx *query_ctx = stmt.get_query_ctx();
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_ctx) ||
      OB_ISNULL(ctx_->allocator_)) {
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

int ObTransformPostProcess::transform_merge_into_subquery(ObDMLStmt *stmt,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObRawExpr *matched_expr = NULL;
  ObRawExpr *not_matched_expr = NULL;
  bool update_has_subquery = false;
  bool insert_has_subquery = false;
  ObSEArray<ObRawExpr*, 8> condition_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> target_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> delete_subquery_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (!stmt->is_merge_stmt() || !stmt->has_subquery()) {
    // do nothing
  } else if (FALSE_IT(merge_stmt = static_cast<ObMergeStmt *>(stmt))) {
    // do nothing
  } else if (OB_FAIL(create_matched_expr(*merge_stmt, matched_expr, not_matched_expr))) {
    LOG_WARN("failed to build matched expr", K(ret));
  } else if (OB_FAIL(get_update_insert_condition_subquery(merge_stmt,
                                                          matched_expr,
                                                          not_matched_expr,
                                                          update_has_subquery,
                                                          insert_has_subquery,
                                                          condition_subquery_exprs))) {
    LOG_WARN("failed to allocate update insert condition subquery", K(ret));
  } else if (OB_FAIL(get_update_insert_target_subquery(merge_stmt,
                                                       matched_expr,
                                                       not_matched_expr,
                                                       update_has_subquery,
                                                       insert_has_subquery,
                                                       target_subquery_exprs))) {
    LOG_WARN("failed to allocate update insert target subquery", K(ret));
  } else if (OB_FAIL(get_delete_condition_subquery(merge_stmt,
                                                   matched_expr,
                                                   update_has_subquery,
                                                   delete_subquery_exprs))) {
    LOG_WARN("failed to allocate delete condition subquery", K(ret));
  } else if (OB_FAIL(merge_stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt expr reference", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformPostProcess::create_matched_expr(ObMergeStmt &stmt,
                                                ObRawExpr *&matched_flag,
                                                ObRawExpr *&not_matched_flag)
{
  int ret = OB_SUCCESS;
  ObMergeTableInfo& table_info = stmt.get_merge_table_info();
  ObRawExpr *not_null_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); ++i) {
    ObColumnRefRawExpr *column = NULL;
    if (OB_ISNULL(column = table_info.column_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (column->is_not_null_for_read()) {
      not_null_expr = column;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("context is invalid", K(ret), K(ctx_));
    } else if (OB_ISNULL(not_null_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find not null expr from merge into target table", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(
                         *ctx_->expr_factory_,
                         not_null_expr,
                         true,
                         matched_flag))) {
      LOG_WARN("failed to build is not null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(
                         *ctx_->expr_factory_,
                         not_null_expr,
                         false,
                         not_matched_flag))) {
      LOG_WARN("failed to build is null expr", K(ret));
    } else if (OB_FAIL(matched_flag->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize matched expr", K(ret));
    } else if (OB_FAIL(not_matched_flag->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize not matched flag", K(ret));
    }
  }
  return ret;
}

int ObTransformPostProcess::get_update_insert_condition_subquery(ObMergeStmt *merge_stmt,
                                                                 ObRawExpr *matched_expr,
                                                                 ObRawExpr *not_matched_expr,
                                                                 bool &update_has_subquery,
                                                                 bool &insert_has_subquery,
                                                                 ObIArray<ObRawExpr*> &new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  update_has_subquery = false;
  insert_has_subquery = false;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(merge_stmt), K(ctx_));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_update_condition_exprs(),
                                                                  update_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_insert_condition_exprs(),
                                                                  insert_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (update_has_subquery &&
             OB_FAIL(generate_merge_conditions_subquery(matched_expr,
                                                        merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (insert_has_subquery &&
             OB_FAIL(generate_merge_conditions_subquery(not_matched_expr,
                                                        merge_stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (update_has_subquery &&
             OB_FAIL(append(new_subquery_exprs, merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (insert_has_subquery &&
             OB_FAIL(append(new_subquery_exprs, merge_stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize case expr", K(ret));
      } else if (OB_FAIL(raw_expr->pull_relation_id_and_levels(merge_stmt->get_current_level()))) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformPostProcess::generate_merge_conditions_subquery(ObRawExpr *matched_expr,
                                                               ObIArray<ObRawExpr*> &condition_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *and_expr = NULL;
  ObSEArray<ObRawExpr *, 4> param_conditions;
  ObSEArray<ObRawExpr*, 8> subquery_exprs;
  ObSEArray<ObRawExpr*, 8> non_subquery_exprs;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(matched_expr) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(condition_exprs,
                                                              subquery_exprs,
                                                              non_subquery_exprs))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(param_conditions.push_back(matched_expr)) ||
             OB_FAIL(append(param_conditions, condition_exprs))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory, param_conditions, and_expr))) {
    LOG_WARN("failed to build matched expr", K(ret));
  } else if (OB_ISNULL(and_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    condition_exprs.reuse();
    if (OB_FAIL(condition_exprs.push_back(and_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformPostProcess::get_update_insert_target_subquery(ObMergeStmt *merge_stmt,
                                                              ObRawExpr *matched_expr,
                                                              ObRawExpr *not_matched_expr,
                                                              bool update_has_subquery,
                                                              bool insert_has_subquery,
                                                              ObIArray<ObRawExpr*> &new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *null_expr = NULL;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  ObSEArray<ObRawExpr*, 8> assign_exprs;
  ObSEArray<ObRawExpr*, 8> update_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> insert_values_subquery_exprs;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(merge_stmt->get_assignments_exprs(assign_exprs))) {
    LOG_WARN("failed to get table assignment exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs,
                                                         update_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_values_vector(),
                                                         insert_values_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*expr_factory, null_expr))) {
    LOG_WARN("failed to build null expr", K(ret));
  } else if (OB_ISNULL(null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(null_expr), K(ret));
  } else {
    if (!update_subquery_exprs.empty()) {
      ObRawExpr *update_matched_expr = NULL;
      if (merge_stmt->get_update_condition_exprs().empty()) {
        update_matched_expr = matched_expr;
      } else if (update_has_subquery) {
        if (OB_UNLIKELY(1 != merge_stmt->get_update_condition_exprs().count()) ||
            OB_ISNULL(merge_stmt->get_update_condition_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else {
          update_matched_expr = merge_stmt->get_update_condition_exprs().at(0);
        }
      } else if (OB_FAIL(temp_exprs.push_back(matched_expr)) ||
                 OB_FAIL(append(temp_exprs, merge_stmt->get_update_condition_exprs()))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                        temp_exprs,
                                                        update_matched_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(update_matched_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/ }

      for (int64_t i = 0; OB_SUCC(ret) && i < update_subquery_exprs.count(); i++) {
        ObRawExpr *raw_expr = NULL;
        ObRawExpr *cast_expr = NULL;
        ObRawExpr *case_when_expr = NULL;
        if (OB_ISNULL(raw_expr = update_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                              ctx_->expr_factory_,
                              ctx_->session_info_,
                              *null_expr,
                              raw_expr->get_result_type(),
                              cast_expr))) {
          LOG_WARN("try add cast expr above failed", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(*expr_factory,
                                                                update_matched_expr,
                                                                raw_expr,
                                                                cast_expr,
                                                                case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_ISNULL(case_when_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_subquery_exprs.push_back(case_when_expr))) {
          LOG_WARN("failed to push back case when expr", K(ret));
        } else { /*do nothing*/ }
      }
    }
    if (OB_SUCC(ret) && !insert_values_subquery_exprs.empty()) {
      ObRawExpr *insert_matched_expr = NULL;
      if (merge_stmt->get_insert_condition_exprs().empty()) {
        insert_matched_expr = not_matched_expr;
      } else if (insert_has_subquery) {
        if (OB_UNLIKELY(1 != merge_stmt->get_insert_condition_exprs().count()) ||
            OB_ISNULL(merge_stmt->get_insert_condition_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          insert_matched_expr = merge_stmt->get_insert_condition_exprs().at(0);
        }
      } else if (OB_FAIL(temp_exprs.push_back(not_matched_expr)) ||
                 OB_FAIL(temp_exprs.assign(merge_stmt->get_insert_condition_exprs()))) {
        LOG_WARN("failed to fill and child exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory, temp_exprs,
                                                        insert_matched_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(insert_matched_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/ }

      for (int64_t i = 0; OB_SUCC(ret) && i < insert_values_subquery_exprs.count(); i++) {
        ObRawExpr *raw_expr = NULL;
        ObRawExpr *case_when_expr = NULL;
        if (OB_ISNULL(raw_expr = insert_values_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(*expr_factory,
                                                                insert_matched_expr,
                                                                raw_expr,
                                                                null_expr,
                                                                case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_ISNULL(case_when_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_subquery_exprs.push_back(case_when_expr))) {
          LOG_WARN("failed to push back subquery exprs", K(ret));
        } else { /*do nothing*/ }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize case expr", K(ret));
      } else if (OB_FAIL(raw_expr->pull_relation_id_and_levels(merge_stmt->get_current_level()))) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && !new_subquery_exprs.empty()) {
      ObSEArray<ObRawExpr*, 8> old_subquery_exprs;
      if (OB_FAIL(append(old_subquery_exprs, update_subquery_exprs)) ||
          OB_FAIL(append(old_subquery_exprs, insert_values_subquery_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_UNLIKELY(old_subquery_exprs.count() != new_subquery_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array count", K(old_subquery_exprs.count()),
            K(new_subquery_exprs.count()), K(ret));
      } else if (OB_FAIL(merge_stmt->replace_inner_stmt_expr(old_subquery_exprs, new_subquery_exprs))) {
        LOG_WARN("failed to replace merge stmt", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformPostProcess::get_delete_condition_subquery(ObMergeStmt *merge_stmt,
                                                          ObRawExpr *matched_expr,
                                                          bool update_has_subquery,
                                                          ObIArray<ObRawExpr*> &new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  bool delete_has_subquery = false;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_delete_condition_exprs(),
                                                                  delete_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (!delete_has_subquery) {
    /*do nothing*/
  } else {
    ObSqlBitSet<> check_table_set;
    ObSEArray<ObRawExpr*, 8> temp_exprs;
    ObRawExpr *delete_matched_expr = NULL;
    ObSEArray<ObRawExpr*, 8> all_column_exprs;
    ObSEArray<ObRawExpr*, 8> delete_column_exprs;
    if (OB_FAIL(check_table_set.add_member(merge_stmt->get_table_bit_index(
                                           merge_stmt->get_target_table_id())))) {
      LOG_WARN("failed to add table set", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(merge_stmt->get_delete_condition_exprs(),
                                                            all_column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*merge_stmt,
                                                             all_column_exprs,
                                                             check_table_set,
                                                             delete_column_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (delete_column_exprs.empty()) {
      /*do nothing*/
    } else {
      ObSEArray<ObRawExpr*, 8> old_exprs;
      ObSEArray<ObRawExpr*, 8> new_exprs;
      ObAssignments &table_assigns  = merge_stmt->get_merge_table_info().assignments_;
      for (int64_t j = 0; OB_SUCC(ret) && j < table_assigns.count(); j++) {
        if (OB_ISNULL(table_assigns.at(j).column_expr_) ||
            OB_ISNULL(table_assigns.at(j).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!ObOptimizerUtil::find_item(delete_column_exprs,
                                               table_assigns.at(j).column_expr_)) {
          /*do nothing*/
        } else if (table_assigns.at(j).expr_->has_flag(CNT_SUB_QUERY) ||
                   table_assigns.at(j).expr_->has_flag(CNT_ONETIME)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not support replace column expr with subquery expr", K(ret));
        } else if (OB_FAIL(old_exprs.push_back(table_assigns.at(j).column_expr_)) ||
                   OB_FAIL(new_exprs.push_back(table_assigns.at(j).expr_))) {
          LOG_WARN("failed to push back exprs", K(ret));
        }  else { /*do nothing*/ }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTransformUtils::replace_exprs(old_exprs,
                                                    new_exprs,
                                                    merge_stmt->get_delete_condition_exprs()))) {
          LOG_WARN("failed to replace exprs", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (merge_stmt->get_update_condition_exprs().empty()) {
      delete_matched_expr = matched_expr;
    } else if (update_has_subquery) {
      if (OB_UNLIKELY(1 != merge_stmt->get_update_condition_exprs().count()) ||
          OB_ISNULL(merge_stmt->get_update_condition_exprs().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else {
        delete_matched_expr = merge_stmt->get_update_condition_exprs().at(0);
      }
    } else if (OB_FAIL(temp_exprs.push_back(matched_expr)) ||
               OB_FAIL(append(temp_exprs, merge_stmt->get_update_condition_exprs()))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                      temp_exprs,
                                                      delete_matched_expr))) {
      LOG_WARN("failed to build matched expr", K(ret));
    } else if (OB_ISNULL(delete_matched_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else { /*do nothing*/}

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(generate_merge_conditions_subquery(delete_matched_expr,
                                                          merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("failed to generate merge conditions", K(ret));
    } else if (OB_FAIL(append(new_subquery_exprs, merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("failed to append new exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
        ObRawExpr *raw_expr = NULL;
        if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(raw_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize case expr", K(ret));
        } else if (OB_FAIL(raw_expr->pull_relation_id_and_levels(merge_stmt->get_current_level()))) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObTransformPostProcess::pullup_exec_exprs(ObDMLStmt *stmt,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
    ObQueryRefRawExpr *query_ref = stmt->get_subquery_exprs().at(i);
    ObSEArray<ObRawExpr *, 4> candi_exprs;
    if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref is null", K(ret), K(query_ref));
    } else if (query_ref->get_exec_params().empty()) {
      // do nothing
    } else if (OB_FAIL(extract_exec_exprs(query_ref->get_ref_stmt(),
                                          query_ref->get_expr_level(),
                                          candi_exprs))) {
      LOG_WARN("failed to get exec exprs", K(ret));
    } else if (candi_exprs.empty()) {
      // do nothing
    } else if (OB_FAIL(adjust_exec_param_exprs(query_ref,
                                               candi_exprs,
                                               stmt->get_current_level()))) {
      LOG_WARN("failed to build exec param exprs", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPostProcess::adjust_exec_param_exprs(ObQueryRefRawExpr *query_ref,
                                                    ObIArray<ObRawExpr *> &candi_exprs,
                                                    const int64_t stmt_level)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
    ObRawExpr *old_expr = NULL;
    ObRawExpr *new_expr = NULL;
    ObExecParamRawExpr *exec_param = NULL;
    if (OB_ISNULL(old_expr = candi_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("candi expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::decorrelate(old_expr, stmt_level))) {
      LOG_WARN("failed to decorrelate expr", K(ret));
    } else if (old_expr->formalize(ctx_->session_info_)) {
      LOG_WARN("failed to formalize", K(ret));
    } else if (old_expr->is_const_expr()) {
      new_expr = old_expr;
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_QUESTIONMARK, exec_param))) {
      LOG_WARN("failed to create exec param expr", K(ret));
    } else if (OB_ISNULL(exec_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new expr is null", K(ret), K(exec_param));
    } else if (OB_FAIL(exec_param->add_flag(IS_CONST))) {
      LOG_WARN("failed to add flag", K(ret));
    } else if (OB_FAIL(exec_param->add_flag(IS_DYNAMIC_PARAM))) {
      LOG_WARN("failed to add flag", K(ret));
    } else {
      exec_param->set_ref_expr(old_expr);
      exec_param->set_result_type(old_expr->get_result_type());
      exec_param->set_param_index(-1);
      exec_param->set_expr_level(stmt_level);
      new_expr = exec_param;
    }
    if (OB_SUCC(ret) && OB_FAIL(new_exprs.push_back(new_expr))) {
      LOG_WARN("failed to push back new expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(query_ref->get_ref_stmt()->replace_inner_stmt_expr(candi_exprs,
                                                                   new_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_exprs.count(); ++i) {
    if (new_exprs.at(i)->is_exec_param_expr()) {
      ObExecParamRawExpr *exec = static_cast<ObExecParamRawExpr *>(new_exprs.at(i));
      if (OB_FAIL(ObTransformUtils::decorrelate(exec->get_ref_expr(), stmt_level))) {
        LOG_WARN("failed to decorrelate ref expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(query_ref->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize", K(ret));
    } else if (OB_FAIL(query_ref->pull_relation_id_and_levels(stmt_level))) {
      LOG_WARN("failed to pull relation id", K(ret));
    }
  }
  return ret;
}

int ObTransformPostProcess::extract_exec_exprs(ObDMLStmt *stmt,
                                               const int32_t expr_level,
                                               ObIArray<ObRawExpr *> &candi_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> exprs;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(SMART_CALL(extract_exec_exprs(exprs.at(i),
                                              expr_level,
                                              candi_exprs)))) {
      LOG_WARN("failed to extract exec exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(extract_exec_exprs(child_stmts.at(i),
                                              expr_level,
                                              candi_exprs)))) {
      LOG_WARN("failed to extract exec exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPostProcess::extract_exec_exprs(ObRawExpr *expr,
                                               const int32_t expr_level,
                                               ObIArray<ObRawExpr *> &candi_exprs)
{
  int ret = OB_SUCCESS;
  if (!expr->has_flag(CNT_DYNAMIC_PARAM)) {
    // do nothing
  } else if (expr->is_exec_param_expr()) {
    ObExecParamRawExpr *exec = static_cast<ObExecParamRawExpr *>(expr);
    if (exec->is_onetime()) {
      if (OB_FAIL(SMART_CALL(extract_exec_exprs(exec->get_ref_expr(),
                                                expr_level,
                                                candi_exprs)))) {
        LOG_WARN("failed to extract exec exprs", K(ret));
      }
    }
  } else if (!expr->is_const_expr() ||
             expr->has_flag(CNT_AGG) ||
             expr->has_flag(CNT_WINDOW_FUNC) ||
             expr->get_expr_type() == T_OP_ROW ||
             get_expr_level(*expr) != expr_level) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_exec_exprs(expr->get_param_expr(i),
                                                expr_level,
                                                candi_exprs)))) {
        LOG_WARN("failed to pullup exec exprs", K(ret));
      }
    }
  } else if (OB_FAIL(candi_exprs.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int64_t ObTransformPostProcess::get_expr_level(const ObRawExpr &expr)
{
  int64_t expr_level = -1;
  int64_t max_size = expr.get_expr_levels().bit_count();
  for (int64_t i = max_size - 1; i >= 0; --i) {
    if (expr.get_expr_levels().has_member(i)) {
      expr_level = i;
      break;
    }
  }
  return expr_level;
}

int ObTransformPostProcess::transform_onetime_subquery(ObDMLStmt *stmt,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> old_exprs;
  ObSEArray<ObRawExpr *, 4> new_exprs;
  ObArray<ObRawExpr *> func_table_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->get_subquery_expr_size() > 0) {
    ObSEArray<ObRawExprPointer, 4> expr_ptrs;
    if (OB_FAIL(stmt->get_relation_exprs(expr_ptrs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else if (OB_FAIL(stmt->get_table_function_exprs(func_table_exprs))) {
      LOG_WARN("failed to get table function exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_ptrs.count(); ++i) {
      bool dummy = false;
      bool is_happend = false;
      ObRawExpr *expr = NULL;
      ObSEArray<ObRawExpr *, 4> onetime_list;
      if (OB_FAIL(expr_ptrs.at(i).get(expr))) {
        LOG_WARN("failed to get expr", K(ret));
      } else if (ObOptimizerUtil::find_item(func_table_exprs, expr)) {
        // do nothing
      } else if (OB_FAIL(extract_onetime_subquery(expr, onetime_list, dummy))) {
        LOG_WARN("failed to extract onetime subquery", K(ret));
      } else if (onetime_list.empty()) {
        // do nothing
      } else if (OB_FAIL(create_onetime_param(stmt, expr, onetime_list,
                                              old_exprs, new_exprs, is_happend))) {
        LOG_WARN("failed to create onetime param", K(ret));
      } else if (!is_happend) {
        // do nothing
      } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize condition", K(ret));
      } else if (OB_FAIL(expr_ptrs.at(i).set(expr))) {
        LOG_WARN("failed to set expr", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformPostProcess::extract_onetime_subquery
 * @param expr
 * @param onetime_list
 * @param is_valid: if a expr is invalid,
 *                  its parent is also invalid while its children can be valid
 * @return
 */
int ObTransformPostProcess::extract_onetime_subquery(ObRawExpr *&expr,
                                                     ObIArray<ObRawExpr *> &onetime_list,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_valid_non_correlated_exists = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    // if a expr contain psedu column, hierachical expr, any column
    is_valid = (expr != NULL) &&
        !ObOptimizerUtil::has_psedu_column(*expr) &&
        !ObOptimizerUtil::has_hierarchical_expr(*expr) &&
        !expr->has_flag(CNT_COLUMN) &&
        !expr->has_flag(CNT_AGG) &&
        !expr->has_flag(CNT_WINDOW_FUNC) &&
        !expr->has_flag(CNT_ONETIME) &&
        !expr->has_flag(CNT_ALIAS) &&
        !expr->has_flag(CNT_SET_OP);

    if (is_valid) {
      int64_t ref_count = 0;
      if (OB_FAIL(is_non_correlated_exists_for_onetime(expr, 
                    is_valid_non_correlated_exists,
                    ref_count))) {
        LOG_WARN("failed to check non correlated exist for one time", K(ret));
      } else if (!is_valid_non_correlated_exists) {
        // do nothing
      } else if (OB_FAIL(onetime_list.push_back(expr))) {
        LOG_WARN("failed to push back non-correlated exists", K(ret));
      } else if (ref_count > 1) {
        is_valid = false;
      }
    }
  }

  if (OB_SUCC(ret) && expr->is_query_ref_expr()) {
    bool has_ref_assign_user_var = false;
    if (!is_valid) {
      // do nothing
    } else if (expr->get_param_count() > 0) {
      is_valid = false;
    } else if (OB_FAIL(ObOptimizerUtil::check_subquery_has_ref_assign_user_var(
                         expr, has_ref_assign_user_var))) {
      LOG_WARN("failed to check subquery has ref assign user var", K(ret));
    } else if (has_ref_assign_user_var) {
      is_valid = false;
    } else if (expr->get_output_column() == 1 &&
               !static_cast<ObQueryRefRawExpr *>(expr)->is_set()) {
      if (OB_FAIL(onetime_list.push_back(expr))) {
        LOG_WARN("failed to push back candi onetime expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && expr->has_flag(CNT_SUB_QUERY) && !is_valid_non_correlated_exists) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      bool is_param_valid = false;
      if (OB_FAIL(extract_onetime_subquery(expr->get_param_expr(i),
                                           onetime_list,
                                           is_param_valid))) {
        LOG_WARN("failed to extract onetime subquery", K(ret));
      } else if (!is_param_valid) {
        is_valid = false;
      }
    }
    if (OB_SUCC(ret) && is_valid && !expr->is_query_ref_expr() &&
        expr->get_expr_type() != T_FUN_COLUMN_CONV &&
        expr->get_expr_type() != T_OP_ROW) {
      if (OB_FAIL(onetime_list.push_back(expr))) {
        LOG_WARN("failed to push back candi onetime exprs", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_valid && !is_valid_non_correlated_exists) {
    is_valid = (expr->get_ref_count() <= 1);
  }
  return ret;
}

int ObTransformPostProcess::create_onetime_param(ObDMLStmt *stmt,
                                                 ObRawExpr *&expr,
                                                 const ObIArray<ObRawExpr *> &onetime_list,
                                                 ObIArray<ObRawExpr *> &old_exprs,
                                                 ObIArray<ObRawExpr *> &new_exprs,
                                                 bool &is_happened)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (ObOptimizerUtil::find_item(old_exprs, expr, &idx)) {
    if (OB_UNLIKELY(idx < 0 || idx >= new_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), K(new_exprs.count()));
    } else {
      expr = new_exprs.at(idx);
      is_happened = true;
    }
  } else if (ObOptimizerUtil::find_item(onetime_list, expr)) {
    ObExecParamRawExpr *new_expr = NULL;
    if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_QUESTIONMARK, new_expr))) {
      LOG_WARN("faield to create exec param expr", K(ret));
    } else if (OB_ISNULL(new_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new expr is null", K(ret), K(new_expr));
    } else {
      new_expr->set_ref_expr(expr, true);
      new_expr->set_result_type(expr->get_result_type());
      new_expr->set_expr_level(stmt->get_current_level());
      if (OB_FAIL(new_expr->add_flag(IS_CONST))) {
        LOG_WARN("failed to add flag", K(ret));
      } else if (OB_FAIL(new_expr->add_flag(IS_DYNAMIC_PARAM))) {
        LOG_WARN("failed to add flag", K(ret));
      } else if (OB_FAIL(new_expr->add_flag(IS_ONETIME))) {
        LOG_WARN("failed to add flag", K(ret));
      } else if (OB_FAIL(stmt->add_onetime_expr(new_expr))) {
        LOG_WARN("failed to add onetime expr", K(ret));
      } else if (OB_FAIL(old_exprs.push_back(expr))) {
        LOG_WARN("failed to add old expr", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push back new expr", K(ret));
      } else {
        expr = new_expr;
        is_happened = true;
      }
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(create_onetime_param(stmt,
                                       expr->get_param_expr(i),
                                       onetime_list,
                                       old_exprs,
                                       new_exprs,
                                       is_happened))) {
        LOG_WARN("failed to create onetime param", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPostProcess::is_non_correlated_exists_for_onetime(ObRawExpr *expr,
                                                                 bool &is_non_correlated_exists_for_onetime,
                                                                 int64_t &ref_count)
{
  int ret = OB_SUCCESS;
  is_non_correlated_exists_for_onetime = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr", K(ret));
  } else if (T_OP_EXISTS == expr->get_expr_type() ||
             T_OP_NOT_EXISTS == expr->get_expr_type()) {
    bool has_ref_assign_user_var = false;
    ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(0));
    if (OB_FAIL(ObOptimizerUtil::check_subquery_has_ref_assign_user_var(
                                 expr, has_ref_assign_user_var))) {
      LOG_WARN("failed to check subquery has ref assign user var", K(ret));
    } else if (!has_ref_assign_user_var && query_ref_expr->get_param_count() == 0) {
      is_non_correlated_exists_for_onetime = true;
      ref_count = query_ref_expr->get_ref_count();
    }
  }
  return ret;
}

}
}

