/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_oracle_update_join_checker.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
namespace
{
int unsupported(const char *reason)
{
  LOG_USER_ERROR(OB_NOT_SUPPORTED, reason);
  return OB_NOT_SUPPORTED;
}

bool literal_or_bind(const ObRawExpr *expr, int depth = 0)
{
  bool allowed = false;
  if (expr != nullptr && depth <= 16) {
    if (expr->is_const_raw_expr()) {
      allowed = true;
    } else if (expr->get_expr_type() == T_FUN_SYS_CAST && expr->get_param_count() == 2 &&
               (expr->has_flag(IS_INNER_ADDED_EXPR) || expr->has_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
      // A resolver-inserted cast of a value is safe. A cast of a key column is never accepted.
      allowed = literal_or_bind(expr->get_param_expr(0), depth + 1);
    }
  }
  return allowed;
}

bool column_of(const ObRawExpr *expr, uint64_t table_id, uint64_t column_id = OB_INVALID_ID)
{
  bool match = false;
  if (expr != nullptr && expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(expr);
    match = col->get_table_id() == table_id &&
        (column_id == OB_INVALID_ID || col->get_column_id() == column_id);
  }
  return match;
}

// Preserve point-lookup semantics: never allow a VARCHAR key to be compared numerically.
// Unknown bind types are accepted at prepare; execution resolves the actual bind types again.
bool key_value_compatible(const ObRawExpr *key, const ObRawExpr *value)
{
  bool compatible = false;
  if (key != nullptr && value != nullptr && literal_or_bind(value)) {
    const ObObjType key_type = key->get_data_type();
    const ObObjType value_type = value->get_data_type();
    compatible = value_type == ObNullType ||
        (value_type == ObUnknownType && value->is_param_expr()) ||
        (ob_is_numeric_type(key_type) && ob_is_numeric_type(value_type)) ||
        (key_type == ObVarcharType && (value_type == ObVarcharType || value_type == ObCharType) &&
         key->get_collation_type() == value->get_collation_type());
  }
  return compatible;
}

// Only literals/binds, own-table columns, addition/subtraction and SYSTIMESTAMP.
bool assignment_value(const ObRawExpr *expr, uint64_t table_id, int depth = 0)
{
  bool allowed = false;
  if (expr == nullptr || depth > 16) {
  } else if (expr->get_expr_type() == T_FUN_COLUMN_CONV && expr->get_param_count() >= 5) {
    // Resolver-inserted target-column precision/nullability conversion; inspect its input.
    allowed = assignment_value(expr->get_param_expr(4), table_id, depth + 1);
  } else if (expr->has_flag(IS_INNER_ADDED_EXPR) &&
             ((expr->get_expr_type() == T_FUN_SYS_CAST && expr->get_param_count() == 2) ||
              (expr->get_expr_type() == T_FUN_SYS_WRAPPER_INNER && expr->get_param_count() == 1))) {
    allowed = assignment_value(expr->get_param_expr(0), table_id, depth + 1);
  } else if (literal_or_bind(expr) || column_of(expr, table_id)) {
    allowed = true;
  } else if (expr->get_expr_type() == T_FUN_SYS_SYSTIMESTAMP && expr->get_param_count() == 0) {
    allowed = true;
  } else if ((expr->get_expr_type() == T_OP_ADD || expr->get_expr_type() == T_OP_MINUS) &&
             expr->get_param_count() == 2) {
    allowed = assignment_value(expr->get_param_expr(0), table_id, depth + 1) &&
              assignment_value(expr->get_param_expr(1), table_id, depth + 1);
  }
  return allowed;
}

// Operators (including OR) may filter the two point-qualified rows. Scalar functions
// and subqueries remain outside this extension. binds_key() separately requires
// unconditional AND-connected key equalities, never equalities beneath OR/NOT.
bool predicate(const ObRawExpr *expr, uint64_t left_id, uint64_t right_id, int depth = 0)
{
  bool allowed = false;
  if (expr == nullptr || depth > 16) {
  } else if (literal_or_bind(expr) || column_of(expr, left_id) || column_of(expr, right_id)) {
    allowed = true;
  } else if (expr->get_expr_type() == T_FUN_SYS_CAST && expr->get_param_count() == 2 &&
             (expr->has_flag(IS_INNER_ADDED_EXPR) || expr->has_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
    allowed = predicate(expr->get_param_expr(0), left_id, right_id, depth + 1);
  } else if (expr->is_op_expr()) {
    allowed = true;
    for (int64_t i = 0; allowed && i < expr->get_param_count(); ++i) {
      allowed = predicate(expr->get_param_expr(i), left_id, right_id, depth + 1);
    }
  }
  return allowed;
}

bool binds_key(const ObRawExpr *expr, uint64_t table_id, uint64_t column_id, int depth = 0)
{
  bool found = false;
  if (expr == nullptr || depth > 16) {
  } else if (expr->get_expr_type() == T_OP_AND) {
    for (int64_t i = 0; !found && i < expr->get_param_count(); ++i) {
      found = binds_key(expr->get_param_expr(i), table_id, column_id, depth + 1);
    }
  } else if (expr->get_expr_type() == T_OP_EQ && expr->get_param_count() == 2) {
    found = (column_of(expr->get_param_expr(0), table_id, column_id) && key_value_compatible(expr->get_param_expr(0), expr->get_param_expr(1))) ||
            (column_of(expr->get_param_expr(1), table_id, column_id) && key_value_compatible(expr->get_param_expr(1), expr->get_param_expr(0)));
  }
  return found;
}
}

bool ObOracleUpdateJoinChecker::is_update_join(const ObDMLStmt &stmt)
{
  // Call only after resolver validation. Oracle's direct JOIN extension is the
  // only supported two-target UPDATE. Unlike FROM items, targets survive the
  // optimizer's JOIN flattening and predicate rewrites.
  return lib::is_oracle_mode() && stmt.is_update_stmt() &&
      static_cast<const ObUpdateStmt &>(stmt).get_update_table_info().count() == 2;
}

int ObOracleUpdateJoinChecker::check(const ObUpdateStmt &stmt, const ObSchemaChecker &schema_checker,
                                    const ObSQLSessionInfo &session, bool is_batch_stmt)
{
  int ret = OB_SUCCESS;
  if (stmt.get_from_item_size() != 1 || !stmt.get_from_item(0).is_joined_) {
    // Existing single-table UPDATEs and updates through a view never enter this extension.
  } else {
    const TableItem *root = stmt.get_table_item(stmt.get_from_item(0));
    const JoinedTable *join = static_cast<const JoinedTable *>(root);
    if (root == nullptr || !root->is_joined_table() || join->joined_type_ != INNER_JOIN ||
        join->left_table_ == nullptr || join->right_table_ == nullptr ||
        !join->left_table_->is_basic_table() || !join->right_table_->is_basic_table() ||
        stmt.get_table_items().count() != 2 || stmt.get_update_table_info().count() != 2 ||
        join->left_table_->ref_id_ == join->right_table_->ref_id_) {
      ret = unsupported("Oracle update join requires two distinct base tables and two update targets");
    } else if (is_batch_stmt || stmt.is_returning() || stmt.is_error_logging() ||
               stmt.has_limit() || stmt.has_order_by() || stmt.has_sequence()) {
      ret = unsupported("Oracle update join with batch, returning, error logging, limit or sequence");
    }

    if (OB_SUCC(ret)) {
      const TableItem *tables[] = {join->left_table_, join->right_table_};
      if (join->join_conditions_.count() != 1) {
        // Only a single column equality joins the two independently point-qualified rows.
        ret = unsupported("Oracle update join requires one equijoin condition");
      } else {
        const ObRawExpr *on = join->join_conditions_.at(0);
        if (on == nullptr || on->get_expr_type() != T_OP_EQ || on->get_param_count() != 2 ||
            !((column_of(on->get_param_expr(0), tables[0]->table_id_) && column_of(on->get_param_expr(1), tables[1]->table_id_)) ||
              (column_of(on->get_param_expr(1), tables[0]->table_id_) && column_of(on->get_param_expr(0), tables[1]->table_id_)))) {
          ret = unsupported("Oracle update join requires a column equality between its two tables");
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_exprs().count(); ++i) {
        if (!predicate(stmt.get_condition_exprs().at(i), tables[0]->table_id_, tables[1]->table_id_)) {
          ret = unsupported("Oracle update join predicates support only columns, literals, binds and operators");
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
        const TableItem &table = *tables[i];
        const ObTableSchema *schema = nullptr;
        if (OB_FAIL(schema_checker.get_table_schema(session.get_effective_tenant_id(), table.ref_id_, schema))) {
          LOG_WARN("get Oracle update join table schema failed", K(ret));
        } else if (schema == nullptr) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!schema->is_user_table() || schema->is_tmp_table() || schema->is_partitioned_table() ||
                   schema->is_heap_table() || schema->has_generated_column() ||
                   !schema->get_trigger_list().empty() ||
                   !schema->get_foreign_key_infos().empty() || table.is_link_table() ||
                   !table.part_names_.empty()) {
          ret = unsupported("Oracle update join requires nonpartitioned primary-key tables without triggers, foreign keys or generated columns");
        } else {
          const auto &indexes = schema->get_simple_index_infos();
          for (int64_t k = 0; OB_SUCC(ret) && k < indexes.count(); ++k) {
            const ObTableSchema *index_schema = nullptr;
            if (OB_FAIL(schema_checker.get_table_schema(session.get_effective_tenant_id(),
                                                        indexes.at(k).table_id_, index_schema))) {
              LOG_WARN("get Oracle update join index schema failed", K(ret));
            } else if (OB_ISNULL(index_schema)) {
              ret = OB_ERR_UNEXPECTED;
            } else if (!index_schema->is_normal_index() && !index_schema->is_unique_index()) {
              ret = unsupported("Oracle update join supports only normal or unique indexes");
            }
          }
          for (ObTableSchema::const_constraint_iterator c = schema->constraint_begin();
               OB_SUCC(ret) && c != schema->constraint_end(); ++c) {
            if ((*c)->get_constraint_type() == CONSTRAINT_TYPE_CHECK) {
              ret = unsupported("Oracle update join with check constraints");
            }
          }
          const ObRowkeyInfo &keys = schema->get_rowkey_info();
          for (int64_t k = 0; OB_SUCC(ret) && k < keys.get_size(); ++k) {
            uint64_t key_id = OB_INVALID_ID;
            bool found = false;
            if (OB_FAIL(keys.get_column_id(k, key_id))) {
              LOG_WARN("get primary key column failed", K(ret));
            }
            for (int64_t p = 0; !found && OB_SUCC(ret) && p < stmt.get_condition_exprs().count(); ++p) {
              found = binds_key(stmt.get_condition_exprs().at(p), table.table_id_, key_id);
            }
            if (OB_SUCC(ret) && !found) {
              ret = unsupported("Oracle update join requires every primary key column explicitly equal to a literal or bind in WHERE");
            }
          }
          for (int64_t t = 0; OB_SUCC(ret) && t < stmt.get_update_table_info().count(); ++t) {
            const ObUpdateTableInfo &info = *stmt.get_update_table_info().at(t);
            if (info.table_id_ != table.table_id_) continue;
            for (int64_t a = 0; OB_SUCC(ret) && a < info.assignments_.count(); ++a) {
              const ObAssignment &assign = info.assignments_.at(a);
              const ObColumnSchemaV2 *col = schema->get_column_schema(assign.column_expr_->get_column_id());
              if (col == nullptr || col->is_rowkey_column() || assign.is_implicit_ ||
                  !assignment_value(assign.expr_, table.table_id_)) {
                LOG_WARN("Oracle update join rejected assignment", K(assign));
                ret = unsupported("Oracle update join supports only nonkey assignments using own-table values, binds and SYSTIMESTAMP");
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
