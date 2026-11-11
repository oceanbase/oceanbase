/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/ob_errno.h"
#include "sql/resolver/mv/ob_mv_checker.h"

#define USING_LOG_PREFIX SQL_RESV

#include "lib/utility/ob_template_utils.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/mv/ob_mv_checker_utils.h"
#include "sql/resolver/mv/ob_mv_major_refresh_checker.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace sql
{

int ObMVMajorRefreshChecker::check(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool has_outer_join = false;
  uint64_t data_version = 0;
  ObMVMajorRefreshChecker checker(stmt, params, result);
  if (OB_FAIL(checker.check_mv_stmt_refresh_type_basic(stmt, is_valid))) {
    LOG_WARN("failed to check mv refresh type basic", K(ret));
  } else if (!is_valid || stmt.is_set_stmt() || stmt.has_group_by()) {
    // not a major refresh MJV candidate
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(params.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_4_0) {
    // major refresh is not supported before this data version
  } else if (OB_FAIL(checker.check_join_shape_valid(stmt, is_valid, has_outer_join))) {
    LOG_WARN("failed to check join shape", K(ret));
  } else if (!is_valid || has_outer_join) {
    // not a major refresh inner join MJV candidate
  } else if (OB_FAIL(checker.check_match_major_refresh_mv(stmt, is_valid))) {
    LOG_WARN("failed to check major refresh mv", K(ret));
  } else if (is_valid && OB_FAIL(checker.set_final_refresh_type(OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV))) {
    LOG_WARN("failed to set major refresh type", K(ret));
  }
  return ret;
}

int ObMVMajorRefreshChecker::check_match_major_refresh_mv(const ObSelectStmt &stmt, bool &is_match)
{
  int ret = OB_SUCCESS;
  const ObIArray<JoinedTable *> &joined_tables = stmt.get_joined_tables();
  const TableItem *left_table = NULL;
  const TableItem *right_table = NULL;
  const ObTableSchema *left_table_schema = NULL;
  const ObTableSchema *right_table_schema = NULL;
  ObSqlSchemaGuard *sql_schema_guard = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSEArray<uint64_t, 4> missing_column_ids;
  ObSEArray<ObRawExpr *, 8> conditions;
  is_match = true;
  if (stmt.get_table_size() != 2 || joined_tables.count() > 1) {
    is_match = false;
    // clang-format off
    LOG_INFO("[MAJ_REF_MV] join table size not valid", KR(ret), "table_size", stmt.get_table_size(),
             "joined_table_count", joined_tables.count());
    // clang-format on
  } else if (joined_tables.empty()) {
    left_table = stmt.get_table_item(0);
    right_table = stmt.get_table_item(1);
  } else if (OB_ISNULL(joined_tables.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined table is null", KR(ret), K(joined_tables));
  } else {
    left_table = joined_tables.at(0)->left_table_;
    right_table = joined_tables.at(0)->right_table_;
    if (OB_FAIL(append(conditions, joined_tables.at(0)->get_join_conditions()))) {
      LOG_WARN("failed to append join conditions", KR(ret));
    }
  }
  if (OB_FAIL(ret) || !is_match) {
  } else if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", KR(ret), KP(left_table), KP(right_table));
  } else if (OB_FAIL(append(conditions, stmt.get_condition_exprs()))) {
    LOG_WARN("failed to append where conditions", KR(ret));
  } else if (OB_ISNULL(stmt.get_query_ctx())
             || OB_ISNULL(sql_schema_guard = &stmt.get_query_ctx()->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql schema guard is null", KR(ret), K(stmt.get_query_ctx()), K(sql_schema_guard));
  } else if (bool cnt_proctime_table = false; OB_FAIL(stmt.is_contains_mv_proctime_table(cnt_proctime_table))) {
    LOG_WARN("failed to check contains mv proctime table", KR(ret));
  } else if (cnt_proctime_table) {
    is_match = false;
    LOG_INFO("[MAJ_REF_MV] mv proctime table is not supported",
             KPC(left_table),
             KPC(right_table));
  } else if (OB_FAIL(sql_schema_guard->get_table_schema(left_table->ref_id_, left_table_schema))) {
    LOG_WARN("left table schema not found", KR(ret), KPC(left_table));
  } else if (OB_FAIL(sql_schema_guard->get_table_schema(right_table->ref_id_, right_table_schema))) {
    LOG_WARN("right table schema not found", KR(ret), KPC(right_table));
  } else if (OB_ISNULL(left_table_schema) || OB_ISNULL(right_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret), K(left_table_schema), K(right_table_schema));
  } else if (OB_ISNULL(schema_guard = sql_schema_guard->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", KR(ret), K(sql_schema_guard));
  } else if (OB_FAIL(ObMVCheckerUtils::get_missing_table_key_column_ids(stmt,
                                                                        *left_table,
                                                                        left_table_schema,
                                                                        schema_guard,
                                                                        missing_column_ids))) {
    LOG_WARN("failed to get missing left table key columns", KR(ret));
  } else if (!missing_column_ids.empty()) {
    is_match = false;
    LOG_INFO("[MAJ_REF_MV] left table key columns are missing from select", K(missing_column_ids));
  } else if (OB_FAIL(ObMVCheckerUtils::get_missing_table_key_column_ids(stmt,
                                                                        *right_table,
                                                                        right_table_schema,
                                                                        schema_guard,
                                                                        missing_column_ids))) {
    LOG_WARN("failed to get missing right table key columns", KR(ret));
  } else if (!missing_column_ids.empty()) {
    is_match = false;
    LOG_INFO("[MAJ_REF_MV] right table key columns are missing from select", K(missing_column_ids));
  } else if (OB_FAIL(check_right_table_join_key_valid(stmt,
                                                     *left_table,
                                                     *right_table,
                                                     conditions,
                                                     *right_table_schema,
                                                     is_match))) {
    LOG_WARN("failed to check join key valid", KR(ret));
  } else if (is_match
             && OB_FAIL(check_left_table_partition_rule_valid(stmt,
                                                              *left_table,
                                                              *left_table_schema,
                                                              is_match))) {
    LOG_WARN("failed to check partition rule valid", KR(ret));
  } else if (is_match
             && OB_FAIL(check_select_item_valid(stmt,
                                                conditions,
                                                left_table->table_id_,
                                                is_match))) {
    LOG_WARN("failed to check select item valid", KR(ret));
  } else if (is_match && OB_FAIL(check_left_table_rowkey_valid(*left_table_schema, is_match))) {
    LOG_WARN("failed to check left table rowkey valid", KR(ret));
  } else if (is_match && OB_FAIL(check_broadcast_table_valid(*right_table_schema, is_match))) {
    LOG_WARN("failed to check broadcast table valid", KR(ret));
  } else if (is_match && OB_FAIL(check_column_store_valid(*left_table_schema, *right_table_schema, is_match))) {
    LOG_WARN("failed to check column store valid", KR(ret));
  } else if (is_match && FALSE_IT(is_match = !GCTX.is_shared_storage_mode())) {
  }

  LOG_INFO("[MAJ_REF_MV] check match major refresh mv", K(is_match));
  return ret;
}

/**
 * Valid when cross-table equalities directly constrain every right-table rowkey column,
 * ensuring at most one right row matches each left row.
 */
int ObMVMajorRefreshChecker::check_right_table_join_key_valid(const ObSelectStmt &stmt,
                                                              const TableItem &left_table,
                                                              const TableItem &right_table,
                                                              const ObIArray<ObRawExpr *> &conditions,
                                                              const ObTableSchema &right_table_schema,
                                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> join_source_ids;  // dummy argument
  ObSqlBitSet<> join_target_ids;  // dummy argument
  ObSEArray<ObRawExpr *, 4> right_join_exprs;
  ObSEArray<uint64_t, 4> right_table_keys;
  ObSEArray<uint64_t, 4> right_table_join_keys;
  is_valid = false;

  if (OB_FAIL(ObTransformUtils::get_table_joined_exprs(stmt,
                                                       left_table,
                                                       right_table,
                                                       conditions,
                                                       right_join_exprs,
                                                       join_source_ids,
                                                       join_target_ids))) {
    LOG_WARN("failed to get right table equal join exprs", KR(ret));
  } else if (OB_FAIL(right_table_schema.get_rowkey_info().get_column_ids(right_table_keys))) {
    LOG_WARN("failed to get table keys", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_join_exprs.count(); ++i) {
    const ObRawExpr *expr = right_join_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", KR(ret));
    } else if (expr->is_column_ref_expr()) {
      const uint64_t column_id = static_cast<const ObColumnRefRawExpr *>(expr)->get_column_id();
      if (OB_FAIL(add_var_to_array_no_dup(right_table_join_keys, column_id))) {
        LOG_WARN("failed to add right table join key", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // Direct equalities covering the right rowkey ensure at most one match per left row.
    is_valid = !right_table_keys.empty() && ObOptimizerUtil::is_subset(right_table_keys, right_table_join_keys);
    if (!is_valid) {
      LOG_INFO("[MAJ_REF_MV] right table join key is not valid", K(right_table_keys), K(right_table_join_keys));
    }
  }

  return ret;
}

int ObMVMajorRefreshChecker::check_left_table_partition_rule_valid(const ObSelectStmt &stmt,
                                                                   const TableItem &left_table,
                                                                   const ObTableSchema &left_table_schema,
                                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRelIds left_table_set;
  is_valid = true;
  if (OB_FAIL(stmt.get_table_rel_ids(left_table, left_table_set))) {
    LOG_WARN("failed to get table rel ids", KR(ret));
  } else {
    const schema::ObPartitionOption &left_partop = left_table_schema.get_part_option();
    const schema::ObPartitionOption &mv_partop = params_.mv_container_table_schema_.get_part_option();
    // TODO for now only works for collect_mv, we could relax the rule later: partition type can
    // be other type, partition func expr should consider about column name alias, etc.
    if (!left_partop.is_hash_part() || !mv_partop.is_hash_part()) {
      is_valid = false;
      LOG_INFO("[MAJ_REF_MV] is not hash partition", K(left_partop), K(mv_partop));
    } else if (left_partop.get_part_num() != mv_partop.get_part_num()) {
      is_valid = false;
      LOG_INFO("[MAJ_REF_MV] part num doesn't match", K(left_partop), K(mv_partop));
    } else {
      ObString format_left_part_str = left_partop.get_part_func_expr_str();
      ObString format_mv_part_str = mv_partop.get_part_func_expr_str();
      bool found_item = false;
      // remove ` or " from part_func_expr_str
      if (format_mv_part_str.length() > 2
          && ((format_mv_part_str[format_mv_part_str.length() - 1] == '`' && format_mv_part_str[0] == '`')
              || (format_mv_part_str[format_mv_part_str.length() - 1] == '"' && format_mv_part_str[0] == '"'))) {
        format_mv_part_str.assign_ptr(format_mv_part_str.ptr() + 1, format_mv_part_str.length() - 2);
      }
      if (format_left_part_str.length() > 2
          && ((format_left_part_str[format_left_part_str.length() - 1] == '`' && format_left_part_str[0] == '`')
              || (format_left_part_str[format_left_part_str.length() - 1] == '"' && format_left_part_str[0] == '"'))) {
        format_left_part_str.assign_ptr(format_left_part_str.ptr() + 1, format_left_part_str.length() - 2);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_items().count(); ++i) {
        const ObRawExpr *expr = NULL;
        if (OB_ISNULL(expr = stmt.get_select_items().at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret), K(i), K(stmt.get_select_items()));
        } else if (!expr->is_column_ref_expr() || !expr->get_relation_ids().is_subset(left_table_set)) {
        } else {
          const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(expr);
          if (!col_expr->get_alias_column_name().empty()
              && format_mv_part_str.case_compare(col_expr->get_alias_column_name()) != 0) {
          } else if (col_expr->get_alias_column_name().empty()
                     && format_mv_part_str.case_compare(col_expr->get_column_name()) != 0) {
          } else if (format_left_part_str.case_compare(col_expr->get_column_name()) != 0) {
          } else {
            found_item = true;
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!found_item) {
        is_valid = false;
        LOG_INFO("[MAJ_REF_MV] hash expr doesn't match", K(left_partop), K(mv_partop));
      }
    }
  }

  return ret;
}

int ObMVMajorRefreshChecker::check_select_item_valid(const ObSelectStmt &stmt,
                                                     const ObIArray<ObRawExpr *> &conditions,
                                                     const uint64_t left_table_id,
                                                     bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  ObSEArray<ObRawExpr *, 8> exprs;
  const ObColumnRefRawExpr *col_expr = NULL;
  const ObIArray<SelectItem> &select_items = stmt.get_select_items();
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(conditions, exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret), K(conditions));
  }
  for (int64_t i = 0; is_match && OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (OB_ISNULL(select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(select_items));
    } else {
      is_match = select_items.at(i).expr_->is_column_ref_expr();
    }
  }
  for (int64_t i = 0; is_match && OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(col_expr = dynamic_cast<ObColumnRefRawExpr *>(exprs.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(exprs));
    } else if (col_expr->get_table_id() != left_table_id) {
      /* do nothing */
    } else {
      is_match = stmt.check_is_select_item_expr(col_expr);
    }
  }
  return ret;
}

int ObMVMajorRefreshChecker::check_left_table_rowkey_valid(const ObTableSchema &left_table_schema, bool &is_valid)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> left_table_rowkey_column_ids;
  common::ObArray<uint64_t> mv_table_rowkey_column_ids;

  if (OB_FAIL(left_table_schema.get_rowkey_info().get_column_ids(left_table_rowkey_column_ids))) {
    LOG_WARN("failed to get table keys", KR(ret));
  } else if (OB_FAIL(params_.mv_container_table_schema_.get_rowkey_info().get_column_ids(mv_table_rowkey_column_ids))) {
    LOG_WARN("failed to get table keys", KR(ret));
  } else {
    lib::ob_sort(left_table_rowkey_column_ids.begin(), left_table_rowkey_column_ids.end());
    lib::ob_sort(mv_table_rowkey_column_ids.begin(), mv_table_rowkey_column_ids.end());
    if (!is_array_equal(left_table_rowkey_column_ids, mv_table_rowkey_column_ids)) {
      is_valid = false;
      LOG_INFO("[MAJ_REF_MV] rowkey doesn't match", K(left_table_rowkey_column_ids), K(mv_table_rowkey_column_ids));
    }
  }

  return ret;
}

int ObMVMajorRefreshChecker::check_broadcast_table_valid(const ObTableSchema &right_table_schema, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;

  if (!right_table_schema.is_broadcast_table()) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] right table is not broadcast table", K(right_table_schema));
  }

  return ret;
}

int ObMVMajorRefreshChecker::check_column_store_valid(const ObTableSchema &left_table_schema,
                                                      const ObTableSchema &right_table_schema,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_column_store = false;
  is_valid = true;

  if (params_.mv_container_table_schema_.is_column_store_supported()) {
    is_valid = false;
    // clang-format off
    LOG_INFO("[MAJ_REF_MV] mv container table is column store table (contains WITH COLUMN GROUP clause in CREATE MATERIALIZED VIEW statement)",
             K(params_.mv_container_table_schema_.get_table_name()));
    // clang-format on
  } else if (OB_FAIL(left_table_schema.get_is_column_store(is_column_store))) {
    LOG_WARN("failed to get is column store", KR(ret));
  } else if (is_column_store) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] left table is column store table", K(left_table_schema.get_table_name()));
  } else if (OB_FAIL(right_table_schema.get_is_column_store(is_column_store))) {
    LOG_WARN("failed to get is column store", KR(ret));
  } else if (is_column_store) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] right table is column store table", K(right_table_schema.get_table_name()));
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
