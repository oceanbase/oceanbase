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

#include "sql/rewrite/ob_constraint_process.h"

#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/resolver/dml/ob_select_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace sql {

int ObConstraintProcess::resolve_constraint_column_expr(ObResolverParams& params, uint64_t table_id, ObDMLStmt*& stmt,
    const ObString& expr_str, ObRawExpr*& expr, bool& is_success)
{
  int ret = OB_SUCCESS;
  const ParseNode* expr_node = NULL;
  if (OB_ISNULL(params.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null");
  } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(expr_str, *params.allocator_, expr_node))) {
    LOG_WARN("parse expr node from str failed", K(ret), K(expr_str));
  } else if (OB_FAIL(resolve_constraint_column_expr(params, table_id, stmt, expr_node, expr, is_success))) {
    LOG_WARN("resolve generated column expr failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObConstraintProcess::resolve_constraint_column_expr(ObResolverParams& params, uint64_t table_id, ObDMLStmt*& stmt,
    const ParseNode* node, ObRawExpr*& expr, bool& is_success)
{
  int ret = OB_SUCCESS;
  ColumnItem* col_item = NULL;
  ObArray<ObQualifiedName> columns;
  ObSQLSessionInfo* session_info = params.session_info_;
  ObRawExprFactory* expr_factory = params.expr_factory_;
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(*expr_factory, *session_info, *node, expr, columns))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }

  uint64_t tid = OB_INVALID_ID;
  is_success = false;
  const common::ObIArray<TableItem*>& table_items = stmt->get_table_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); i++) {
    const TableItem* table_item = table_items.at(i);
    if (table_item->ref_id_ == table_id) {
      tid = table_item->table_id_;
      is_success = true;
      break;
    }
  }

  for (int64_t i = 0; is_success && OB_SUCC(ret) && i < columns.count(); ++i) {
    ObQualifiedName& q_name = columns.at(i);
    if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "generated column function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else if (NULL == (col_item = stmt->get_column_item(tid, q_name.col_name_))) {
      is_success = false;
      break;
    } else {
      if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, col_item->expr_))) {
        LOG_WARN("replace column ref expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && is_success && OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("formalize expr failed", K(ret));
  }
  return ret;
}

int ObConstraintProcess::resolve_related_part_exprs(const ObRawExpr* expr, ObDMLStmt*& stmt,
    const share::schema::ObTableSchema& table_schema, ObIArray<ObRawExpr*>& related_part_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_factory_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(expr_factory_), K(session_info_));
  }
  for (ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
       OB_SUCC(ret) && iter != table_schema.constraint_end();
       iter++) {
    sql::ObResolverParams params;
    ObRawExpr* check_expr = NULL;
    params.expr_factory_ = expr_factory_;
    params.allocator_ = &allocator_;
    params.session_info_ = session_info_;
    bool is_success = false;
    if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
      continue;
    } else if (share::is_oracle_mode() && !((*iter)->get_enable_flag()) && !((*iter)->get_rely_flag())) {
      continue;
    } else if (OB_FAIL(resolve_constraint_column_expr(
                   params, table_schema.get_table_id(), stmt, (*iter)->get_check_expr_str(), check_expr, is_success))) {
      LOG_WARN("fail to resolve_constraint_column_expr", K(ret));
    } else if (!is_success) {
      // do nothing
    } else if (T_OP_EQ == check_expr->get_expr_type()) {
      ObOpRawExpr* op_check_expr = static_cast<ObOpRawExpr*>(check_expr);
      if (2 != op_check_expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param count", K(op_check_expr->get_param_count()));
      } else {
        ObRawExpr* left = op_check_expr->get_param_expr(0);
        ObRawExpr* right = op_check_expr->get_param_expr(1);

        if (ObRawExprUtils::is_same_raw_expr(left, expr)) {
          if (expr->is_column_ref_expr() && ObRawExprUtils::need_column_conv(expr->get_result_type(), *right)) {
            if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*expr_factory_,
                    allocator_,
                    static_cast<const ObColumnRefRawExpr&>(*expr),
                    right,
                    params.session_info_))) {
              LOG_WARN("create cast expr failed", K(ret));
            } else {
              LOG_TRACE("add cast for check constraint", K(*right));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(related_part_exprs.push_back(right))) {
            LOG_WARN("fail to push back related_part_exprs", K(ret));
          }
        } else if (ObRawExprUtils::is_same_raw_expr(right, expr)) {
          if (expr->is_column_ref_expr() && ObRawExprUtils::need_column_conv(expr->get_result_type(), *left)) {
            if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*expr_factory_,
                    allocator_,
                    static_cast<const ObColumnRefRawExpr&>(*expr),
                    left,
                    params.session_info_))) {
              LOG_WARN("create cast expr failed", K(ret));
            } else {
              LOG_TRACE("add cast for check constraint", K(*left));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(related_part_exprs.push_back(left))) {
            LOG_WARN("fail to push back related_part_exprs", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObConstraintProcess::after_transform(ObDMLStmt*& stmt, share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  common::ObIArray<ObDMLStmt::PartExprItem>& part_expr_items = stmt->get_part_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < part_expr_items.count(); i++) {
    ObDMLStmt::PartExprItem& expr_item = part_expr_items.at(i);

    ObDMLStmt::PartExprArray expr_array;
    expr_array.table_id_ = expr_item.table_id_;
    expr_array.index_tid_ = expr_item.index_tid_;

    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(expr_item.index_tid_, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(expr_item));
    } else if (table_schema != NULL) {
      if (OB_SUCC(ret) && expr_item.part_expr_ != NULL) {
        if (OB_FAIL(
                resolve_related_part_exprs(expr_item.part_expr_, stmt, *table_schema, expr_array.part_expr_array_))) {
          LOG_WARN("fail to resolve_related_part_exprs", K(ret));
        }
      }
      if (OB_SUCC(ret) && expr_item.subpart_expr_ != NULL) {
        if (OB_FAIL(resolve_related_part_exprs(
                expr_item.subpart_expr_, stmt, *table_schema, expr_array.subpart_expr_array_))) {
          LOG_WARN("fail to resolve_related_part_exprs", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (expr_array.part_expr_array_.count() > 0 || expr_array.subpart_expr_array_.count() > 0) {
          if (OB_FAIL(stmt->get_related_part_expr_arrays().push_back(expr_array))) {
            LOG_WARN("fail to push back expr_array", K(ret));
          }
        }
      }
    }
  }

  common::ObIArray<TableItem*>& table_items = stmt->get_table_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); i++) {
    TableItem* table_item = table_items.at(i);
    if (table_item->is_generated_table()) {
      if (OB_ISNULL(table_item->ref_query_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("genreated table item ref query is NULL,", K(ret));
      } else if (stmt::T_SELECT != table_item->ref_query_->get_stmt_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subquery should be select stmt, "
                 "input type is",
            K(table_item->ref_query_->get_stmt_type()));
      }

      if (OB_SUCC(ret)) {
        ObDMLStmt* sub_query = static_cast<ObDMLStmt*>(table_item->ref_query_);
        if (OB_FAIL(after_transform(sub_query, schema_guard))) {
          LOG_WARN("fail to transform sub_query", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
