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
#include "sql/ob_update_stmt_printer.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

void ObUpdateStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObUpdateStmt *stmt)
{
  ObDMLStmtPrinter::init(buf, buf_len, pos, stmt);
}

int ObUpdateStmtPrinter::do_print()
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


int ObUpdateStmtPrinter::print()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_FAIL(print_basic_stmt())) {
    LOG_WARN("fail to print basic stmt", K(ret), K(*stmt_));
  } else { /*do nothing*/ }

  return ret;
}

int ObUpdateStmtPrinter::print_basic_stmt()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_FAIL(print_with())) {
    LOG_WARN("failed to print with", K(ret));
  } else if (OB_FAIL(print_temp_table_as_cte())) {
    LOG_WARN("failed to print cte", K(ret));
  } else if (OB_FAIL(print_update())) {
    LOG_WARN("fail to print select", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_from(false/*not need from, only print table name*/))) {
    LOG_WARN("fail to print from", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_set())) {
    LOG_WARN("fail to print from", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_where())) {
    LOG_WARN("fail to print where", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_order_by())) {
    LOG_WARN("fail to print order by", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_limit())) {
    LOG_WARN("fail to print limit", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_returning())) {
    LOG_WARN("fail to print_returning", K(ret), K(*stmt_));
  } else {
    // do-nothing
  }

  return ret;
}

int ObUpdateStmtPrinter::print_update()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_update_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid update stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    DATA_PRINTF("update ");
    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_hint())) { // hint
        LOG_WARN("fail to print hint", K(ret), K(*stmt_));
      }
    }
  }
  return ret;
}

int ObUpdateStmtPrinter::print_set()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_update_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid update stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    DATA_PRINTF(" set ");
    if (OB_SUCC(ret)) {
      const ObUpdateStmt *update_stmt = static_cast<const ObUpdateStmt*>(stmt_);

      for (int64_t i = 0; OB_SUCC(ret) && i < update_stmt->get_update_table_info().count(); ++i) {
        ObUpdateTableInfo* table_info = update_stmt->get_update_table_info().at(i);
        if (OB_ISNULL(table_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null table info", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
          const ObAssignment &assign = table_info->assignments_.at(j);
          // (c1, c2) = select x1, x2 from ... is represented as
          // c1 = alias(subquery, 0), c2 = alias(subquery, 1)
          ObAliasRefRawExpr *alias = NULL;
          if (OB_ISNULL(assign.column_expr_) || OB_ISNULL(assign.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is null", K(ret), K(assign.column_expr_), K(assign.expr_));
          } else if (assign.is_implicit_) {
            continue;
          } else if (OB_FAIL(ObRawExprUtils::find_alias_expr(assign.expr_, alias))) {
            LOG_WARN("failed to find alias expr", K(ret));
          } else if (alias == NULL) {
            if (OB_FAIL(print_simple_assign(assign))) {
              LOG_WARN("failed to print simple assign expr", K(ret));
            } else {
              DATA_PRINTF(",");
            }
          } else if (alias->get_project_index() == 0) {
            if (OB_FAIL(print_vector_assign(table_info->assignments_, alias->get_param_expr(0)))) {
              LOG_WARN("failed to print vector assign", K(ret));
            } else if (OB_SUCC(ret)) {
              DATA_PRINTF(",");
            }
          }
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
      }
    }
  }
  return ret;
}

int ObUpdateStmtPrinter::print_simple_assign(const ObAssignment &assign)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(assign.expr_) || OB_ISNULL(assign.column_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assign is invalid", K(ret), K(assign.expr_), K(assign.column_expr_));
  } else if (OB_FAIL(expr_printer_.do_print(assign.column_expr_, T_FIELD_LIST_SCOPE))) {
    LOG_WARN("fail to print target column", K(ret));
  } else {
    DATA_PRINTF(" = ");
  }
  if (OB_SUCC(ret)) {
    ObRawExpr *tmp_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::erase_inner_added_exprs(assign.expr_, tmp_expr))) {
      LOG_WARN("erase inner cast expr failed", K(ret));
    } else if (OB_ISNULL(tmp_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null");
    } else if (OB_FAIL(expr_printer_.do_print(tmp_expr, T_FIELD_LIST_SCOPE))) {
      LOG_WARN("fail to print select expr", K(ret));
    }
  }
  return ret;
}

int ObUpdateStmtPrinter::print_vector_assign(const ObAssignments &assignments,
                                             ObRawExpr *query_ref_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *stmt = NULL;
  ObSEArray<ObRawExpr *, 4> left_columns;
  if (OB_ISNULL(query_ref_expr) ||
      OB_UNLIKELY(!query_ref_expr->is_query_ref_expr()) ||
      OB_ISNULL(stmt = static_cast<ObQueryRefRawExpr *>(query_ref_expr)->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref expr is null", K(ret), K(query_ref_expr), K(stmt));
  } else if (OB_FAIL(left_columns.prepare_allocate(stmt->get_select_item_size()))) {
    LOG_WARN("failed to prepare allocate select item size", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
    ObColumnRefRawExpr *col = NULL;
    ObRawExpr *value_expr = NULL;
    ObAliasRefRawExpr *alias = NULL;
    if (OB_ISNULL(value_expr = assignments.at(i).expr_) ||
        OB_ISNULL(col = assignments.at(i).column_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value expr is null", K(ret), K(col), K(value_expr));
    } else if (col->is_generated_column()) {
      // skip
    } else if (OB_FAIL(ObRawExprUtils::find_alias_expr(value_expr, alias))) {
      LOG_WARN("failed to find alias expr", K(ret));
    } else if (alias == NULL) {
      // do nothing
    } else if (alias->get_param_expr(0) == query_ref_expr) {
      int64_t idx = alias->get_project_index();
      if (OB_UNLIKELY(idx < 0 || idx >= left_columns.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index is invalid", K(ret), K(idx));
      } else {
        left_columns.at(idx) = col;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < left_columns.count(); ++i) {
    DATA_PRINTF("%c", (i == 0 ? '(' : ' '));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr_printer_.do_print(left_columns.at(i), T_FIELD_LIST_SCOPE))) {
        LOG_WARN("fail to print target column", K(ret));
      } else {
        DATA_PRINTF("%c", (i == left_columns.count() - 1 ? ')' : ','));
      }
    }
  }
  if (OB_SUCC(ret)) {
    DATA_PRINTF(" = ");
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr_printer_.do_print(query_ref_expr, T_FIELD_LIST_SCOPE))) {
      LOG_WARN("failed to print query ref expr", K(ret));
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase




