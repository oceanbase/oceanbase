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

#define USING_LOG_PREFIX SQL
#include "ob_merge_stmt_printer.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

void ObMergeStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObMergeStmt *stmt)
{
  ObDMLStmtPrinter::init(buf, buf_len, pos, stmt);
}

int ObMergeStmtPrinter::do_print()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt should not be NULL", K(ret));
  } else {
    expr_printer_.init(buf_,
                       buf_len_,
                       pos_,
                       schema_guard_,
                       print_params_,
                       param_store_);
    if (OB_FAIL(print())) {
      LOG_WARN("fail to print stmt", K(ret));
    } 
  }
  return ret;
}
// merge into target using source on match_condition update clause insert_clause
int ObMergeStmtPrinter::print()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_) ||
      OB_UNLIKELY(!stmt_->is_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret),
             K(stmt_), K(buf_), K(pos_));
  } else {
    const ObMergeStmt *merge_stmt = static_cast<const ObMergeStmt *>(stmt_);
    const TableItem *target_table = NULL;
    const TableItem *source_table = NULL;
    const ObIArray<ObRawExpr *> &match_conds = merge_stmt->get_match_condition_exprs();
    if (OB_ISNULL(target_table = merge_stmt->get_table_item_by_id(
                    merge_stmt->get_target_table_id())) ||
        OB_ISNULL(source_table = merge_stmt->get_table_item_by_id(
                    merge_stmt->get_source_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target table or source table is null",
               K(ret), K(target_table), K(source_table));
    }
    if (OB_SUCC(ret) && OB_FAIL(print_temp_table_as_cte())) {
      LOG_WARN("failed to print cte", K(ret));
    }
    DATA_PRINTF("merge ");
    if (OB_SUCC(ret) && print_hint()) {
      LOG_WARN("failed to print hint", K(ret));
    }
    DATA_PRINTF("into ");
    if (OB_SUCC(ret) && print_table(target_table)) {
      LOG_WARN("failed to print target table", K(ret));
    }
    DATA_PRINTF(" using ");
    if (OB_SUCC(ret) && print_table(source_table)) {
      LOG_WARN("failed to print source table", K(ret));
    }
    DATA_PRINTF(" on ( ");
    if (OB_SUCC(ret) && print_conds(match_conds)) {
      LOG_WARN("failed to print match conditions", K(ret));
    }
    DATA_PRINTF(" )");
    if (OB_SUCC(ret) && merge_stmt->has_update_clause()) {
      if (OB_FAIL(print_update_clause(*merge_stmt))) {
        LOG_WARN("failed to print update clause", K(ret));
      }
    }
    if (OB_SUCC(ret) && merge_stmt->has_insert_clause()) {
      if (OB_FAIL(print_insert_clause(*merge_stmt))) {
        LOG_WARN("failed to print insert clause", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeStmtPrinter::print_conds(const ObIArray<ObRawExpr *> &conds)
{
  int ret = OB_SUCCESS;
  if (conds.empty()) {
    DATA_PRINTF("1=1");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    if (OB_FAIL(expr_printer_.do_print(conds.at(i), T_NONE_SCOPE))) {
      LOG_WARN("failed to print match condition", K(ret));
    } else if (i < conds.count() - 1) {
      DATA_PRINTF(" and ");
    }
  }
  return ret;
}

int ObMergeStmtPrinter::print_update_clause(const ObMergeStmt &merge_stmt)
{
  int ret = OB_SUCCESS;
  bool first_assign = true;
  const ObIArray<ObRawExpr *> &update_conds = merge_stmt.get_update_condition_exprs();
  const ObIArray<ObRawExpr *> &delete_conds = merge_stmt.get_delete_condition_exprs();
  const common::ObIArray<ObAssignment> &assignments = merge_stmt.get_table_assignments();
  if (OB_UNLIKELY(assignments.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update clause does not have assignments", K(ret));
  }
  DATA_PRINTF(" when matched then");
  if (OB_SUCC(ret)) {
    // has update assign
    DATA_PRINTF(" update set ");
    for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
      const ObAssignment &assign = assignments.at(i);
      ObColumnRefRawExpr *column = assign.column_expr_;
      ObRawExpr *value = assign.expr_;
      ObRawExpr *real_value = NULL;
      if (OB_ISNULL(column) || OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column assign", K(ret));
      } else if (!assign.is_implicit_) {
        if (!first_assign) {
          DATA_PRINTF(", ");
        } else {
          first_assign = false;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(expr_printer_.do_print(column, T_UPDATE_SCOPE))) {
            LOG_WARN("failed to print assign target", K(ret));
          } else {
            DATA_PRINTF(" = ");
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObRawExprUtils::erase_inner_added_exprs(value, real_value))) {
            LOG_WARN("failed to erase inner case expr", K(ret));
          } else if (OB_FAIL(expr_printer_.do_print(real_value, T_UPDATE_SCOPE))) {
            LOG_WARN("failed to print assign value", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !update_conds.empty()) {
    DATA_PRINTF(" where ");
    if (OB_SUCC(ret) && print_conds(update_conds)) {
      LOG_WARN("failed to print update conditions", K(ret));
    }
  }
  if (OB_SUCC(ret) && !delete_conds.empty()) {
    DATA_PRINTF(" delete where ");
    if (OB_SUCC(ret) && print_conds(delete_conds)) {
      LOG_WARN("failed to print conditions", K(ret));
    }
  }
  return ret;
}

int ObMergeStmtPrinter::print_insert_clause(const ObMergeStmt &merge_stmt)
{
  int ret = OB_SUCCESS;
  int64_t column_count = merge_stmt.get_values_desc().count();
  int64_t value_count = merge_stmt.get_values_vector().count();
  ObArenaAllocator allocator("PrintMergeStmt");
  const ObIArray<ObRawExpr *> &insert_conds = merge_stmt.get_insert_condition_exprs();
  if (OB_UNLIKELY(column_count != value_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count does not equal with value count",
             K(ret), K(column_count), K(value_count));
  }
  DATA_PRINTF(" when not matched then insert (");
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    const ObColumnRefRawExpr* column = merge_stmt.get_values_desc().at(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is NULL", K(ret));
    } else {
      ObString column_name = column->get_column_name();
      CONVERT_CHARSET_FOR_RPINT(allocator, column_name);
      DATA_PRINTF(" %.*s %c", LEN_AND_PTR(column_name),
                             (i < column_count - 1 ? ',' : ')'));
    }
  }
  DATA_PRINTF(" values (");
  for (int64_t i = 0; OB_SUCC(ret) && i < value_count; ++i) {
    DATA_PRINTF(" ");
    if (OB_FAIL(expr_printer_.do_print(merge_stmt.get_values_vector().at(i), T_INSERT_SCOPE))) {
      LOG_WARN("fail to print value expr", K(ret));
    }
    DATA_PRINTF(" %c", i < value_count - 1 ? ',' : ')');
  }
  if (OB_SUCC(ret) && !insert_conds.empty()) {
    DATA_PRINTF(" where ");
    if (OB_SUCC(ret) && print_conds(insert_conds)) {
      LOG_WARN("failed to print conditions", K(ret));
    }
  }
  return ret;
}
