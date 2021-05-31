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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"

/**
 * DELETE syntax from MySQL 5.7
 *
 * Single-Table Syntax:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
 *   [PARTITION (partition_name,...)]
 *   [WHERE where_condition]
 *   [ORDER BY ...]
 *   [LIMIT row_count]
 *
 * Multiple-Table Syntax
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   tbl_name[.*] [, tbl_name[.*]] ...
 *   FROM table_references
 *   [WHERE where_condition]
 *  Or:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   FROM tbl_name[.*] [, tbl_name[.*]] ...
 *   USING table_references
 *   [WHERE where_condition]
 */
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {
ObDeleteResolver::ObDeleteResolver(ObResolverParams& params) : ObDMLResolver(params)
{
  params.contain_dml_ = true;
}

ObDeleteResolver::~ObDeleteResolver()
{}

int ObDeleteResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* table_node = parse_tree.children_[TABLE];

  // create the delete stmt
  ObDeleteStmt* delete_stmt = NULL;
  bool is_multi_table_delete = false;
  if (NULL == (delete_stmt = create_stmt<ObDeleteStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create delete stmt failed");
  } else if (OB_UNLIKELY(parse_tree.type_ != T_DELETE) || OB_UNLIKELY(parse_tree.num_child_ < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree type is invalid", K_(parse_tree.type), K_(parse_tree.num_child));
  } else if (OB_ISNULL(table_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_node is null");
  } else {
    stmt_ = delete_stmt;
    if (OB_FAIL(resolve_table_list(*table_node, is_multi_table_delete))) {
      LOG_WARN("resolve table failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < delete_tables_.count(); ++i) {
    const TableItem* table_item = delete_tables_.at(i);
    CK(OB_NOT_NULL(table_item));
    OZ(add_related_columns_to_stmt(*table_item));
    OZ(resolve_global_delete_index_info(*table_item));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_where_clause(parse_tree.children_[WHERE]))) {
      LOG_WARN("resolve delete where clause failed", K(ret));
    } else if (OB_FAIL(resolve_order_clause(parse_tree.children_[ORDER_BY]))) {
      LOG_WARN("resolve delete order clause failed", K(ret));
    } else if (OB_FAIL(resolve_limit_clause(parse_tree.children_[LIMIT]))) {
      LOG_WARN("resolve delete limit clause failed", K(ret));
    } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT]))) {
      LOG_WARN("resolve hints failed", K(ret));
    } else if (OB_FAIL(resolve_returning(parse_tree.children_[RETURNING]))) {
      LOG_WARN("resolve returning failed", K(ret));
    } else if (OB_FAIL(delete_stmt->formalize_stmt(session_info_))) {
      LOG_WARN("pull stmt all expr relation ids failed", K(ret));
    } else if (is_oracle_mode() && OB_FAIL(resolve_check_constraints(delete_stmt->get_table_item(0)))) {
      LOG_WARN("resolve check constraint failed", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < delete_tables_.count(); i++) {
    const TableItem* table_item = delete_tables_.at(i);
    if (OB_FAIL(try_add_rowid_column_to_stmt(*table_item))) {
      LOG_WARN("failed to try adding rowid column", K(ret));
    }
  }  // for end

  if (OB_SUCC(ret)) {
    TableItem* delete_table = NULL;
    if (OB_FAIL(view_pullup_part_exprs())) {
      LOG_WARN("view pull up part exprs failed", K(ret));
    } else if (OB_FAIL(check_view_deletable())) {
      LOG_TRACE("view not deletable", K(ret));
    } else if (delete_stmt->get_from_item_size() == 1 &&
               NULL != (delete_table = delete_stmt->get_table_item(delete_stmt->get_from_item(0))) &&
               delete_table->is_basic_table()) {
      // do nothing
    } else {
      delete_stmt->set_dml_source_from_join(true);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_stmt->check_dml_need_filter_null())) {
      LOG_WARN("failed to check dml need filter null", K(ret));
    } else if (OB_FAIL(check_safe_update_mode(delete_stmt, is_multi_table_delete))) {
      LOG_WARN("failed to check safe update mode", K(ret));
    } else {
      /*do nothing */
    }
  }
  return ret;
}

int ObDeleteResolver::check_safe_update_mode(ObDeleteStmt* delete_stmt, bool is_multi_table_delete)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(delete_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), K(params_.session_info_), K(delete_stmt));
  } else if (OB_FAIL(params_.session_info_->get_sql_safe_updates(is_sql_safe_updates))) {
    LOG_WARN("failed to get is safe update mode", K(ret));
  } else if (is_sql_safe_updates) {
    /* Update table values in mysql safe mode, you need to meet:
     * Prerequisite: Single table deletion, not multi-table deletion, must contain where conditions;
     * At least one of the following conditions must be met:
     * 1. Contains limit, there are related columns in the where condition;
     * 2. The query range can be extracted in the where condition ==> Since the query range can only be extracted in the
     * optimizer stage, the condition is Check at the optimizer stage
     */
    if (is_multi_table_delete || delete_stmt->get_condition_exprs().empty()) {
      ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
      LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
    } else if (delete_stmt->has_limit()) {
      bool is_const_expr = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_const_expr && i < delete_stmt->get_condition_exprs().count(); ++i) {
        const ObRawExpr* expr = delete_stmt->get_condition_expr(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(expr));
        } else {
          is_const_expr = expr->has_const_or_const_expr_flag();
        }
      }
      if (OB_SUCC(ret) && is_const_expr) {
        ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
        LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObDeleteResolver::resolve_table_list(const ParseNode& table_list, bool& is_multi_table_delete)
{
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  is_multi_table_delete = false;
  const ParseNode* delete_list = NULL;
  const ParseNode* from_list = NULL;
  ObDeleteStmt* delete_stmt = get_delete_stmt();

  CK(T_DELETE_TABLE_NODE == table_list.type_);
  CK(2 == table_list.num_child_);
  CK(OB_NOT_NULL(delete_stmt));

  if (OB_SUCC(ret)) {
    delete_list = table_list.children_[0];
    from_list = table_list.children_[1];
  }

  CK(OB_NOT_NULL(from_list));
  CK(T_TABLE_REFERENCES == from_list->type_);
  CK(OB_NOT_NULL(from_list->children_));

  for (int64_t i = 0; OB_SUCC(ret) && i < from_list->num_child_; ++i) {
    const ParseNode* table_node = from_list->children_[i];
    CK(OB_NOT_NULL(table_node));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
        LOG_WARN("failed to resolve table", K(ret));
      } else if (table_item->is_function_table()) {
        ret = OB_WRONG_TABLE_NAME;
        LOG_WARN("invalid table name", K(ret));
      } else if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
        LOG_WARN("add reference table to namespace checker failed", K(ret));
      } else if (OB_FAIL(delete_stmt->add_from_item(table_item->table_id_, table_item->is_joined_table()))) {
        LOG_WARN("failed to add from item", K(ret));
      } else {
        /*
          In order to share the same logic with 'select' to generate access path costly, we
          add the table in the udpate stmt in the from_item list as well.
         */
        LOG_DEBUG("succ to add from item", KPC(table_item));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == delete_list) {
      // single table delete, delete list is same with from list
      CK(delete_stmt->get_table_size() == 1);
      OZ(delete_tables_.push_back(delete_stmt->get_table_item(0)));
    } else {
      // multi table delete
      is_multi_table_delete = true;
      ObString table_name;
      ObString db_name;
      CK(T_TABLE_REFERENCES == delete_list->type_);
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_list->num_child_; ++i) {
        table_name.reset();
        db_name.reset();
        table_item = NULL;
        const ParseNode* table_node = delete_list->children_[i];
        OZ(resolve_table_relation_node(table_node, table_name, db_name, true));
        OZ(find_delete_table_with_mysql_rule(db_name, table_name, table_item));
        OZ(delete_tables_.push_back(table_item));
      }
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(table_item, delete_tables_, OB_SUCC(ret))
    {
      if (NULL != *table_item && (*table_item)->is_generated_table()) {
        if (OB_FAIL(set_base_table_for_view(*const_cast<TableItem*>(*table_item)))) {
          LOG_WARN("set base table for delete view failed", K(ret));
        } else if (OB_FAIL(add_all_column_to_updatable_view(*delete_stmt, **table_item))) {
          LOG_WARN("add all column to updatable view failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDeleteResolver::find_delete_table_with_mysql_rule(
    const ObString& db_name, const ObString& table_name, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  CK(OB_NOT_NULL(stmt));
  CK(OB_NOT_NULL(session_info_));
  ObString final_db_name;
  table_item = NULL;
  ObSEArray<const TableItem*, 2> table_items;
  OZ(stmt->get_all_table_item_by_tname(session_info_, db_name, table_name, table_items));
  if (OB_SUCC(ret)) {
    if (table_items.count() == 1) {
      table_item = const_cast<TableItem*>(table_items.at(0));
    } else if (db_name.empty()) {
      // If there are multiple table_items in table_items, it means that the delete clause does not specify the
      // database_name of the table And all tables in the from clause are base tables, such as delete t1.* from db1.t1,
      // db2.t1; In this case, we think that the database_name in the delete clause is consistent with the session
      ObString final_db_name = session_info_->get_database_name();
      if (final_db_name.empty()) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no database selected");
      }
      for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_item) && i < table_items.count(); ++i) {
        if (ObCharset::case_insensitive_equal(final_db_name, table_items.at(i)->database_name_) &&
            ObCharset::case_insensitive_equal(table_name, table_items.at(i)->table_name_)) {
          table_item = const_cast<TableItem*>(table_items.at(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNKNOWN_TABLE;
      ObString print_table_name = concat_table_name(db_name, table_name);
      ObString scope_name = ObString::make_string("MULTI DELETE");
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE,
          print_table_name.length(),
          print_table_name.ptr(),
          scope_name.length(),
          scope_name.ptr());
    } else if (!table_item->is_basic_table() && !table_item->is_generated_table()) {
      ret = OB_ERR_NON_UPDATABLE_TABLE;
      ObString print_table_name = concat_table_name(db_name, table_name);
      ObString scope_name = ObString::make_string("DELETE");
      LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
          print_table_name.length(),
          print_table_name.ptr(),
          scope_name.length(),
          scope_name.ptr());
    }
  }
  return ret;
}

int ObDeleteResolver::add_related_columns_to_stmt(const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt* delete_stmt = get_delete_stmt();
  if (NULL == schema_checker_ || NULL == delete_stmt) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "schema_checker_ or delete_stmt is null");
  }
  const ObTableSchema* table_schema = NULL;
  // Firstly, add primary index(data table) rowkey columns
  IndexDMLInfo* primary_dml_info = NULL;
  if (OB_SUCC(ret)) {
    if (NULL == (primary_dml_info = delete_stmt->get_or_add_table_columns(table_item.table_id_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to get table columns", K(table_item));
    } else if (OB_FAIL(add_all_rowkey_columns_to_stmt(table_item, primary_dml_info->column_exprs_))) {
      LOG_WARN("add all rowkey columns to stmt failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t binlog_row_image = ObBinlogRowImage::FULL;
    if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
      LOG_WARN("fail to get binlog row image", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(table_item.get_base_table_item().ref_id_, table_schema))) {
      SQL_RESV_LOG(WARN, "fail to get table schema", K(ret), K(table_item));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(ret), K(table_item.get_base_table_item().ref_id_));
    } else if (need_all_columns(*table_schema, binlog_row_image)) {
      if (OB_FAIL(add_all_columns_to_stmt(table_item, primary_dml_info->column_exprs_))) {
        LOG_WARN("fail to add all column to stmt", K(ret), K(table_item));
      }
    } else if (OB_FAIL(add_index_related_column_to_stmt(table_item, primary_dml_info->column_exprs_))) {
      LOG_WARN("fail to add relate column to stmt", K(ret), K(table_item));
    }
    if (OB_SUCC(ret)) {
      primary_dml_info->table_id_ = table_item.table_id_;
      primary_dml_info->loc_table_id_ = table_item.get_base_table_item().table_id_;
      primary_dml_info->index_tid_ = table_item.get_base_table_item().ref_id_;
      primary_dml_info->rowkey_cnt_ = table_schema->get_rowkey_column_num();
      primary_dml_info->part_cnt_ = table_schema->get_partition_cnt();
      primary_dml_info->index_name_ = table_schema->get_table_name_str();
    }
  }
  return ret;
}

int ObDeleteResolver::add_index_related_column_to_stmt(
    const TableItem& table_item, ObIArray<ObColumnRefRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* index_schema = NULL;
  uint64_t idx_tids[OB_MAX_INDEX_PER_TABLE];
  int64_t idx_count = OB_MAX_INDEX_PER_TABLE;
  ObDeleteStmt* delete_stmt = get_delete_stmt();
  CK(OB_NOT_NULL(schema_checker_));
  CK(OB_NOT_NULL(delete_stmt));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_checker_->get_can_write_index_array(
            table_item.get_base_table_item().ref_id_, idx_tids, idx_count))) {
      SQL_RESV_LOG(WARN, "get can write index array failed", K(ret));
    }
  }
  // Secondly, for each index, all all its rowkey
  ObArray<ObColumnRefRawExpr*> column_items;
  for (int64_t i = 0; OB_SUCC(ret) && i < idx_count; ++i) {
    column_items.reset();
    if (OB_FAIL(schema_checker_->get_table_schema(idx_tids[i], index_schema))) {
      SQL_RESV_LOG(WARN, "get index schema failed", "index_id", idx_tids[i]);
    } else if (OB_FAIL(add_all_index_rowkey_to_stmt(table_item, index_schema, column_exprs))) {
      LOG_WARN("add all index rowkey column to stmt failed", K(ret));
    }
  }
  return ret;
}

int ObDeleteResolver::resolve_global_delete_index_info(const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  int64_t binlog_row_image = 0;
  IndexDMLInfo index_dml_info;
  ObAssignments empty_assignments;
  ObDeleteStmt* delete_stmt = get_delete_stmt();
  uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
  CK(OB_NOT_NULL(delete_stmt));
  CK(OB_NOT_NULL(params_.session_info_));
  OC((params_.session_info_->get_binlog_row_image)(binlog_row_image));
  OC((schema_checker_->get_can_write_index_array)(
      table_item.get_base_table_item().ref_id_, index_tid, index_cnt, true));

  if (OB_SUCC(ret)) {
    delete_stmt->set_has_global_index(index_cnt > 0);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    index_dml_info.reset();
    const ObTableSchema* index_schema = NULL;
    if (OB_FAIL(schema_checker_->get_table_schema(index_tid[i], index_schema))) {
      LOG_WARN("get index table schema failed", K(ret), K(index_tid[i]));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is null");
    } else if (OB_FAIL(resolve_index_related_column_exprs(
                   table_item.table_id_, *index_schema, empty_assignments, index_dml_info.column_exprs_))) {
      LOG_WARN("resolve index related column exprs failed", K(ret));
    } else {
      index_dml_info.table_id_ = table_item.table_id_;
      index_dml_info.loc_table_id_ = table_item.get_base_table_item().table_id_;
      index_dml_info.index_tid_ = index_tid[i];
      index_dml_info.rowkey_cnt_ = index_schema->get_rowkey_column_num();
      index_dml_info.part_cnt_ = index_schema->get_partition_cnt();
      if (OB_FAIL(index_schema->get_index_name(index_dml_info.index_name_))) {
        LOG_WARN("get index name from index schema failed", K(ret));
      } else if (OB_FAIL(delete_stmt->add_multi_table_dml_info(index_dml_info))) {
        LOG_WARN("add index dml info to update stmt failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && !with_clause_without_record_) {
      ObSchemaObjVersion table_version;
      table_version.object_id_ = index_schema->get_table_id();
      table_version.object_type_ = DEPENDENCY_TABLE;
      table_version.version_ = index_schema->get_schema_version();
      if (OB_FAIL(delete_stmt->add_global_dependency_table(table_version))) {
        LOG_WARN("add global dependency table failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDeleteResolver::check_view_deletable()
{
  int ret = OB_SUCCESS;
  // uv_check_basic && mysql join already checked in set_base_table_for_view()
  FOREACH_CNT_X(it, delete_tables_, OB_SUCC(ret))
  {
    const TableItem* table = *it;
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL table table ", K(ret));
    } else if (!table->is_generated_table()) {
      continue;
    } else if (OB_ISNULL(table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    }

    if (OB_SUCC(ret) && is_mysql_mode()) {
      if (OB_SUCC(ret)) {
        bool has_subquery = false;
        bool has_dependent_subquery = false;
        bool ref_update_table = false;
        if (OB_FAIL(ObResolverUtils::uv_check_select_item_subquery(
                *table, has_subquery, has_dependent_subquery, ref_update_table))) {
          LOG_WARN("updatable view check select table failed", K(ret));
        } else {
          LOG_DEBUG("update view check", K(has_subquery), K(has_dependent_subquery), K(ref_update_table));
          ret = (has_dependent_subquery || ref_update_table) ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
        }
      }

      if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        ObString str = "DELETE";
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table->get_table_name().length(),
            table->get_table_name().ptr(),
            str.length(),
            str.ptr());
      }
    }

    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      if (OB_SUCC(ret)) {
        bool has_distinct = false;
        if (OB_FAIL(
                ObResolverUtils::uv_check_oracle_distinct(*table, *session_info_, *schema_checker_, has_distinct))) {
          LOG_WARN("check updatable view distinct failed", K(ret));
        } else {
          LOG_DEBUG("check has distinct", K(ret), K(has_distinct));
          ret = has_distinct ? OB_ERR_ILLEGAL_VIEW_UPDATE : ret;
        }
      }

      // check key preserved table
      if (OB_SUCC(ret)) {
        bool key_preserved = 0;
        if (OB_FAIL(uv_check_key_preserved(*table, key_preserved))) {
          LOG_WARN("check key preserved failed", K(ret));
        } else {
          LOG_DEBUG("check key preserved", K(key_preserved));
          ret = !key_preserved ? OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED : ret;
        }
      }
    }
  }

  return ret;
}

int ObDeleteResolver::try_add_rowid_column_to_stmt(const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt* delete_stmt = get_delete_stmt();
  IndexDMLInfo* primary_dml_info = NULL;
  ObArray<ObColumnRefRawExpr*> column_exprs;
  if (OB_ISNULL(delete_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "delete stmt is null", K(ret));
  } else if (OB_ISNULL(primary_dml_info = delete_stmt->get_or_add_table_columns(table_item.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "unexpecteed null primary dml info", K(ret));
  } else if (OB_FAIL(delete_stmt->get_column_exprs(column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    int rowid_col_idx = -1;
    for (int i = 0; - 1 == rowid_col_idx && OB_SUCC(ret) && i < column_exprs.count(); i++) {
      if (OB_ISNULL(column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "unexpected null column expr", K(ret));
      } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_exprs.at(i)->get_column_id() &&
                 table_item.table_id_ == column_exprs.at(i)->get_table_id()) {
        rowid_col_idx = i;
      }
    }  // for end

    if (OB_FAIL(ret) || -1 == rowid_col_idx) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(primary_dml_info->column_exprs_, column_exprs.at(rowid_col_idx)))) {
      SQL_RESV_LOG(WARN, "failed to add element to array", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
