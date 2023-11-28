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
#include "sql/ob_insert_all_stmt_printer.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_select_stmt_printer.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

void ObInsertAllStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObInsertAllStmt *stmt)
{
  ObDMLStmtPrinter::init(buf, buf_len, pos, stmt);
}

int ObInsertAllStmtPrinter::do_print()
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


int ObInsertAllStmtPrinter::print()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_UNLIKELY(!stmt_->is_insert_all_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid insert stmt", K(stmt_->get_stmt_type()),K(ret));
  } else if (OB_FAIL(print_multi_insert_stmt())) {
    LOG_WARN("fail to print multi insert stmt", K(ret), K(*stmt_));
  } else { /*do nothing*/ }

  return ret;
}

int ObInsertAllStmtPrinter::print_multi_insert_stmt()
{
  int ret = OB_SUCCESS;
  const ObInsertAllStmt *insert_stmt = static_cast<const ObInsertAllStmt*>(stmt_);
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else if (OB_UNLIKELY(!stmt_->is_insert_all_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid insert stmt", K(stmt_->get_stmt_type()), K(ret));
  } else if (OB_FAIL(print_temp_table_as_cte())) {
    LOG_WARN("failed to print cte", K(ret));
  } else if (OB_FAIL(print_multi_insert(insert_stmt))) {
    LOG_WARN("fail to print select", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_multi_value(insert_stmt))) {
    LOG_WARN("fail to print into", K(ret), K(*stmt_));
  } else if (insert_stmt->get_table_size() > 0) {
    const TableItem *table_item = insert_stmt->get_table_item(insert_stmt->get_table_size() - 1);
    const ObSelectStmt* sub_select_stmt = NULL;
    if (OB_ISNULL(table_item) || OB_UNLIKELY(!table_item->is_generated_table()) ||
        OB_ISNULL(sub_select_stmt = table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub select stmt is null", K(ret), K(table_item), K(sub_select_stmt));
    } else if (OB_FAIL(print_subquery(sub_select_stmt, PRINT_CTE | FORCE_COL_ALIAS))) {
      LOG_WARN("failed to print subquery");
    } else {
      LOG_DEBUG("print multi insert stmt complete");
    }
  }
  return ret;
}

int ObInsertAllStmtPrinter::print_multi_insert(const ObInsertAllStmt *insert_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert_stmt is NULL", K(ret), K(insert_stmt));
  } else {
    DATA_PRINTF("insert ");
    if (OB_FAIL(print_hint())) { // hint
      LOG_WARN("fail to print hint", K(ret), K(*stmt_));
    } else if (insert_stmt->get_is_multi_insert_first()) {
      DATA_PRINTF("first");
    } else {
      DATA_PRINTF("all");
    }
  }
  return ret;
}

int ObInsertAllStmtPrinter::print_multi_value(const ObInsertAllStmt *insert_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert_stmt is NULL", K(ret), K(insert_stmt));
  } else if (insert_stmt->get_is_multi_condition_insert()) {
    if (OB_FAIL(print_multi_conditions_insert(insert_stmt))) {
      LOG_WARN("failed to print multi conditions insert", K(ret));
    }
  } else if (OB_FAIL(print_basic_multi_insert(insert_stmt))) {
    LOG_WARN("failed to print basic multi insert", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObInsertAllStmtPrinter::print_basic_multi_insert(const ObInsertAllStmt *insert_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_stmt) || OB_UNLIKELY(insert_stmt->get_is_multi_condition_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(insert_stmt));
  } else {
    const ObIArray<ObInsertAllTableInfo*> &table_infos = insert_stmt->get_insert_all_table_info();
    for (int64_t i = 0;OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObInsertAllTableInfo* table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      } else if (OB_FAIL(print_into_table_values(insert_stmt, *table_info))) {
        LOG_WARN("failed to print into table values", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertAllStmtPrinter::print_multi_conditions_insert(const ObInsertAllStmt *insert_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_stmt) || OB_UNLIKELY(!insert_stmt->get_is_multi_condition_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(insert_stmt));
  } else {
    int64_t pre_idx = -1;
    const ObIArray<ObInsertAllTableInfo*> &table_infos = insert_stmt->get_insert_all_table_info();
    for (int64_t i = 0;OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObInsertAllTableInfo* table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      } else if (table_info->when_cond_idx_ == pre_idx) {
        if (OB_FAIL(print_into_table_values(insert_stmt, *table_info))) {
          LOG_WARN("failed to print into table values", K(ret));
        }
      } else if (table_info->when_cond_idx_ != -1) { //when expr
        DATA_PRINTF(" when ");
        for (int64_t j = 0; OB_SUCC(ret) && j < table_info->when_cond_exprs_.count(); ++j) {
          if (OB_FAIL(expr_printer_.do_print(table_info->when_cond_exprs_.at(j),
                                             T_NONE_SCOPE))) {
            LOG_WARN("fail to print condition expr", K(ret));
          } else {
            DATA_PRINTF(" and ");
          }
        }
        if (OB_SUCC(ret)) {
          *pos_ -= 5; // strlen(" and ")
          DATA_PRINTF(" then");
          if (OB_FAIL(print_into_table_values(insert_stmt, *table_info))) {
            LOG_WARN("failed to print into table values", K(ret));
          } else {
            pre_idx = table_info->when_cond_idx_;
          }
        }
      } else { //else expr
        DATA_PRINTF(" else ");
        if (OB_FAIL(print_into_table_values(insert_stmt, *table_info))) {
          LOG_WARN("failed to print into table values", K(ret));
        } else {
          pre_idx = table_info->when_cond_idx_;
        }
      }
    }
  }
  return ret;
}

int ObInsertAllStmtPrinter::print_into_table_values(const ObInsertAllStmt *insert_stmt,
                                                    const ObInsertAllTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  DATA_PRINTF(" into ");
  const TableItem *table_item = NULL;
  if (OB_ISNULL(insert_stmt) ||
      OB_ISNULL(table_item = insert_stmt->get_table_item_by_id(table_info.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid table item", K(table_info), K(ret));
  } else if (OB_FAIL(print_table(table_item, true))) {
    LOG_WARN("failed to print table", K(*table_item), K(ret));
  } else {
    //print columns
    DATA_PRINTF("(");
    for (int64_t j = 0; OB_SUCC(ret) && j < table_info.values_desc_.count(); ++j) {
      const ObColumnRefRawExpr* column = table_info.values_desc_.at(j);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is NULL", K(ret));
      } else {
        PRINT_IDENT_WITH_QUOT(column->get_column_name());
        DATA_PRINTF(",");
      }
    }
    if (OB_SUCC(ret)) {
      --*pos_;
      DATA_PRINTF(") ");
      //print values
      int64_t col_count = table_info.values_desc_.count();
      int64_t row_count = table_info.values_vector_.count() / col_count;
      DATA_PRINTF("values");
      for (int64_t k = 0; OB_SUCC(ret) && k < row_count; ++k) {
        DATA_PRINTF("(");
        for (int64_t p = 0; OB_SUCC(ret) && p < col_count; ++p) {
          if (OB_FAIL(expr_printer_.do_print(table_info.values_vector_.at(k * col_count + p),
                                             T_INSERT_SCOPE,
                                             true))) {
            LOG_WARN("fail to print where expr", K(ret));
          } else {
            DATA_PRINTF(",");
          }
        }
        if (OB_SUCC(ret)) {
          --*pos_;
          DATA_PRINTF("),");
        }
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase


