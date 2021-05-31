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
#include "sql/resolver/dml/ob_update_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {
ObUpdateResolver::ObUpdateResolver(ObResolverParams& param)
    : ObDMLResolver(param), has_add_all_rowkey_(false), has_add_all_columns_(false), update_column_ids_()
{
  param.contain_dml_ = true;
}

ObUpdateResolver::~ObUpdateResolver()
{}

int ObUpdateResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = NULL;

  if (T_UPDATE != parse_tree.type_ || 3 > parse_tree.num_child_ || OB_ISNULL(parse_tree.children_) ||
      OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree for update",
        K(parse_tree.type_),
        K(parse_tree.num_child_),
        K(parse_tree.children_),
        K((session_info_)));
  } else if (OB_ISNULL(update_stmt = create_stmt<ObUpdateStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create update stmt failed");
  } else {
    stmt_ = update_stmt;
    update_stmt->set_ignore(false);
    if (NULL != parse_tree.children_[IGNORE]) {
      update_stmt->set_ignore(true);
      session_info_->set_ignore_stmt(true);
    }
  }

  // 1. resolve table items
  if (OB_SUCC(ret)) {
    ParseNode* table_node = parse_tree.children_[TABLE];
    if (OB_FAIL(resolve_table_list(*table_node))) {
      LOG_WARN("resolve table failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    current_scope_ = T_UPDATE_SCOPE;
    // resolve assignments
    ParseNode* assign_list = parse_tree.children_[UPDATE_LIST];
    if (OB_ISNULL(assign_list) || OB_UNLIKELY(T_ASSIGN_LIST != assign_list->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid assign list node", K(assign_list));
    } else if (OB_FAIL(resolve_assignments(*assign_list, update_stmt->get_tables_assignments(), current_scope_))) {
      LOG_WARN("fail to resolve assignment", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(it, update_stmt->get_table_items(), OB_SUCC(ret))
    {
      if (NULL != *it && (*it)->is_generated_table()) {
        if (NULL != (*it)->view_base_item_ && OB_FAIL(add_all_column_to_updatable_view(*update_stmt, *(*it)))) {
          LOG_WARN("add all column for updatable view failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(view_pullup_generated_column_exprs())) {
        LOG_WARN("view pullup generated column exprs failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // Resolve cascaded updated columns
    if (OB_FAIL(resolve_additional_assignments(update_stmt->get_tables_assignments(), T_UPDATE_SCOPE))) {
      LOG_WARN("fail to resolve_additional_assignments", K(ret));
    }
  }
  if (OB_SUCC(ret) && update_stmt->get_table_size() > 1) {
    // check multi table update conflict
    if (OB_FAIL(check_multi_update_key_conflict())) {
      LOG_WARN("check multi update key conflict failed", K(ret));
    }
  }

  // add column for table scan, keep rowkey column in the head
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_related_columns_to_stmt())) {
      LOG_WARN("fail to add column to stmt", K(ret));
    }
  }

  // 3. resolve other clauses
  if (OB_SUCC(ret)) {
    if (share::is_oracle_mode()) {
      ObTablesAssignments& tas = update_stmt->get_tables_assignments();
      if (1 != tas.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("oracle mode don't support multi table update grammar", K(ret));
      } else {
        ObTableAssignment& ta = tas.at(0);
        if (OB_INVALID_ID == ta.table_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid table assignment", K(ta.table_id_));
        } else if (OB_FAIL(resolve_check_constraints(update_stmt->get_table_item(0)))) {
          LOG_WARN("resolve check constraint failed", K(ret));
        } else if (session_info_->use_static_typing_engine()) {
          //
          // TODO : support trigger in new engine.
          // Replace constraint with assigned values is removed in 1d840f573269 commit to
          // support trigger. Because the trigger may alter the assigned value, constraint
          // check must base on the altered values. A magic hack is done in code generator
          // to make the constraint find the new values.
          //
          // We can not do this magic in new engine because we can not evaluate expression
          // with different input rows. We must make sure the constraint base on the assigned
          // values here. Trigger is not supported in new engine right now, because PL is not
          // supported. We believe we can add a trigger generated expr here and make
          // constraint base on that expressions to solve the problem in future.
          for (uint64_t i = 0; OB_SUCC(ret) && i < update_stmt->get_check_constraint_exprs_size(); ++i) {
            if (OB_FAIL(ObTableAssignment::expand_expr(
                    tas.at(0).assignments_, update_stmt->get_check_constraint_exprs().at(i)))) {
              LOG_WARN("expand generated column expr failed", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_where_clause(parse_tree.children_[WHERE]))) {
        LOG_WARN("resolve where clause failed", K(ret));
      } else if (OB_FAIL(resolve_order_clause(parse_tree.children_[ORDER_BY]))) {
        LOG_WARN("resolve order clause failed", K(ret));
      } else if (OB_FAIL(resolve_limit_clause(parse_tree.children_[LIMIT]))) {
        LOG_WARN("resolve limit clause failed", K(ret));
      } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT]))) {
        LOG_WARN("resolve hints failed", K(ret));
      } else if (OB_FAIL(resolve_returning(parse_tree.children_[RETURNING]))) {
        LOG_WARN("resolve returning failed", K(ret));
      } else {
        if (session_info_->use_static_typing_engine() && !update_stmt->get_returning_exprs().empty()) {
          // The old engine pass the updated row to the returning expression to
          // get the updated value. We can not do this in static engine, we need to
          // replace the column with the assigned value.
          ObTableAssignment& ta = update_stmt->get_tables_assignments().at(0);
          FOREACH_CNT_X(e, update_stmt->get_returning_exprs(), OB_SUCC(ret))
          {
            OZ(ObTableAssignment::expand_expr(ta.assignments_, *e));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(update_stmt->formalize_stmt(session_info_))) {
          LOG_WARN("pull update stmt all expr relation ids failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && is_mysql_mode() && !update_stmt->is_ignore()) {
      bool is_multi_update = false;
      if (OB_FAIL(is_multi_table_update(update_stmt, is_multi_update))) {
        LOG_WARN("failed to check is multi table udpate", K(ret));
      } else if (is_multi_update && update_stmt->has_order_by()) {
        // Incorrect usage of UPDATE and ORDER BY
        ret = OB_ERR_UPDATE_ORDER_BY;
      } else if (is_multi_update && update_stmt->has_limit()) {
        // Incorrect usage of UPDATE and LIMIT
        ret = OB_ERR_UPDATE_LIMIT;
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < update_stmt->get_tables_assignments().count(); i++) {
      const ObTableAssignment& tas = update_stmt->get_tables_assignments().at(i);
      if (OB_FAIL(try_add_rowid_column_to_stmt(tas))) {
        LOG_WARN("failed to try adding rowid column", K(ret));
      }
    }  // for end
  }
  if (OB_SUCC(ret)) {
    // Resolve which global indexes need to be updated in cascade
    int64_t N = update_stmt->get_tables_assignments().count();
    ObSEArray<uint64_t, 4> global_indexs;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      global_indexs.reset();
      const ObTableAssignment& ta = update_stmt->get_tables_assignments().at(i);
      if (OB_FAIL(resolve_cascade_updated_global_index(ta, global_indexs))) {
        LOG_WARN("resolve cascade updated global index failed", K(ret));
      } else if (OB_FAIL(resolve_multi_table_dml_info(ta, global_indexs))) {
        LOG_WARN("resolve global update index info failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    TableItem* update_table = NULL;
    if (OB_FAIL(view_pullup_part_exprs())) {
      LOG_WARN("view pull up part exprs failed", K(ret));
    } else if (OB_FAIL(check_view_updatable())) {
      LOG_TRACE("view not updatable", K(ret));
    } else if (update_stmt->get_from_item_size() == 1 &&
               NULL != (update_table = update_stmt->get_table_item(update_stmt->get_from_item(0))) &&
               update_table->is_basic_table()) {
      // do nothing
    } else {
      update_stmt->set_dml_source_from_join(true);
    }
  }

  if (OB_SUCC(ret)) {
    // Distribute the centralized assignment information to each index, and do assignment updates for each index
    if (OB_FAIL(update_stmt->refill_index_assignment_info())) {
      LOG_WARN("init index assignment info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObIArray<TableColumns>& all_table_columns = update_stmt->get_all_table_columns();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns.count(); ++i) {
      ObIArray<IndexDMLInfo>& index_infos = all_table_columns.at(i).index_dml_infos_;
      for (int64_t j = 0; OB_SUCC(ret) && j < index_infos.count(); ++j) {
        IndexDMLInfo& index_info = index_infos.at(j);
        if (OB_FAIL(try_add_remove_const_expr(index_info))) {
          LOG_WARN("add value expr failed", K(ret));
        } else if (OB_FAIL(add_rowkey_ids(index_info.index_tid_, index_info.primary_key_ids_))) {
          LOG_WARN("fail init index key ids", K(ret));
        } else if (OB_FAIL(get_part_key_ids(index_info.index_tid_, index_info.part_key_ids_))) {
          LOG_WARN("fail init part key ids", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_stmt->check_dml_need_filter_null())) {
      LOG_WARN("failed to check dml need filter null", K(ret));
    } else if (share::is_mysql_mode() && OB_FAIL(check_safe_update_mode(update_stmt))) {
      LOG_WARN("failed to check fullfill safe update mode", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (update_stmt->is_ignore() && update_stmt->has_global_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ignore with global index");
    } else { /*do nothing*/
    }
  }

  return ret;
}

int ObUpdateResolver::try_add_remove_const_expr(IndexDMLInfo& index_info)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(session_info_) && OB_NOT_NULL(schema_checker_));
  if (OB_SUCC(ret) && session_info_->use_static_typing_engine()) {
    const ObTableSchema* table_schema = NULL;
    OZ(schema_checker_->get_table_schema(index_info.index_tid_, table_schema));
    CK(OB_NOT_NULL(table_schema));
    if (OB_SUCC(ret) && table_schema->is_user_table()) {
      const ObIArray<ObForeignKeyInfo>& fk_infos = table_schema->get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < index_info.assignments_.count(); ++i) {
        ObAssignment& assign = index_info.assignments_.at(i);
        CK(OB_NOT_NULL(assign.expr_) && OB_NOT_NULL(assign.column_expr_));
        if (OB_FAIL(ret)) {
        } else if (assign.expr_->has_const_or_const_expr_flag() &&
                   is_parent_col_self_ref_fk(assign.column_expr_->get_column_id(), fk_infos)) {
          ObRawExpr* new_expr = NULL;
          CK(OB_NOT_NULL(params_.expr_factory_));
          OZ(ObRawExprUtils::build_remove_const_expr(*params_.expr_factory_, *session_info_, assign.expr_, new_expr));
          CK(OB_NOT_NULL(new_expr));
          OX(assign.expr_ = new_expr);
        }
      }
    }
  }
  return ret;
}

// check if %parent_col_id is parent column of a self reference foreign key
bool ObUpdateResolver::is_parent_col_self_ref_fk(uint64_t parent_col_id, const ObIArray<ObForeignKeyInfo>& fk_infos)
{
  bool bret = false;
  for (int64_t i = 0; i < fk_infos.count(); i++) {
    const ObForeignKeyInfo& fk_info = fk_infos.at(i);
    if (fk_info.parent_table_id_ == fk_info.child_table_id_) {
      if (has_exist_in_array(fk_info.parent_column_ids_, parent_col_id, NULL)) {
        bret = true;
        break;
      }
    }
  }
  return bret;
}

int ObUpdateResolver::check_safe_update_mode(ObUpdateStmt* update_stmt)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), K(params_.session_info_), K(update_stmt));
  } else if (OB_FAIL(params_.session_info_->get_sql_safe_updates(is_sql_safe_updates))) {
    LOG_WARN("failed to get is safe update mode", K(ret));
  } else if (is_sql_safe_updates) {
    if (!update_stmt->has_limit() && update_stmt->get_condition_exprs().empty()) {
      ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
      LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObUpdateResolver::check_multi_update_key_conflict()
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt* update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("update stmt is null");
  } else {
    const ObTablesAssignments& tas = update_stmt->get_tables_assignments();
    for (int64_t i = 0; OB_SUCC(ret) && i < tas.count() - 1; ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < tas.count(); ++j) {
        const ObTableAssignment& ta1 = tas.at(i);
        const ObTableAssignment& ta2 = tas.at(j);
        const TableItem* table1 = update_stmt->get_table_item_by_id(ta1.table_id_);
        const TableItem* table2 = update_stmt->get_table_item_by_id(ta2.table_id_);
        if (OB_ISNULL(table1) || OB_ISNULL(table2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tables are null", K(table1), K(table2));
        } else if (ta1.table_id_ != ta2.table_id_ && table1->ref_id_ == table2->ref_id_) {
          uint64_t base_table_id =
              table1->is_generated_table() ? table1->get_base_table_item().ref_id_ : table1->ref_id_;
          for (int64_t k = 0; OB_SUCC(ret) && k < ta1.assignments_.count(); ++k) {
            bool is_key = false;
            uint64_t column_id = ta1.assignments_.at(k).column_expr_->get_column_id();
            if (OB_FAIL(schema_checker_->column_is_key(base_table_id, column_id, is_key))) {
              LOG_WARN("check column is key failed", K(ret), K(base_table_id), K(column_id));
            } else if (is_key) {
              // set error code
              ret = OB_ERR_MULTI_UPDATE_KEY_CONFLICT;
              const ObString concat_name1 = concat_table_name(table1->database_name_, table1->get_table_name());
              const ObString concat_name2 = concat_table_name(table2->database_name_, table2->get_table_name());
              LOG_USER_ERROR(OB_ERR_MULTI_UPDATE_KEY_CONFLICT,
                  concat_name1.length(),
                  concat_name1.ptr(),
                  concat_name2.length(),
                  concat_name2.ptr());
            }
          }
          for (int64_t k = 0; OB_SUCC(ret) && k < ta2.assignments_.count(); ++k) {
            bool is_key = false;
            uint64_t column_id = ta2.assignments_.at(k).column_expr_->get_column_id();
            if (OB_FAIL(schema_checker_->column_is_key(base_table_id, column_id, is_key))) {
              LOG_WARN("check column is key failed", K(ret), K(base_table_id), K(column_id));
            } else if (is_key) {
              // set error code
              ret = OB_ERR_MULTI_UPDATE_KEY_CONFLICT;
              const ObString concat_name1 = concat_table_name(table1->database_name_, table1->get_table_name());
              const ObString concat_name2 = concat_table_name(table2->database_name_, table2->get_table_name());
              LOG_USER_ERROR(OB_ERR_MULTI_UPDATE_KEY_CONFLICT,
                  concat_name1.length(),
                  concat_name1.ptr(),
                  concat_name2.length(),
                  concat_name2.ptr());
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::resolve_table_list(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = get_update_stmt();
  TableItem* table_item = NULL;
  ObSelectStmt* ref_stmt = NULL;
  if (OB_UNLIKELY(T_TABLE_REFERENCES != parse_tree.type_) || OB_UNLIKELY(parse_tree.num_child_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid update stmt", K(update_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
    const ParseNode* table_node = parse_tree.children_[i];
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table node is null");
    } else if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
      LOG_WARN("failed to resolve table", K(ret));
      // This is to be compatible with oracle
    } else if (is_oracle_mode() && table_node->num_child_ == 2) {
      if (OB_ISNULL(table_item) || !table_item->is_generated_table() || OB_ISNULL(ref_stmt = table_item->ref_query_)) {
        int ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(table_item), K(ref_stmt), K(ret));
      } else if (OB_UNLIKELY(ref_stmt->get_from_items().count() != 1)) {
        ret = OB_ERR_ILLEGAL_VIEW_UPDATE;
        LOG_WARN("not updatable", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
        LOG_WARN("add reference table to namespace checker failed", K(ret));
      } else if (OB_FAIL(update_stmt->add_from_item(table_item->table_id_, table_item->is_joined_table()))) {
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
  return ret;
}

int ObUpdateResolver::add_related_columns_to_stmt()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt) || OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid update stmt", K(update_stmt), K(params_.session_info_));
  } else {
    ObTablesAssignments& tas = update_stmt->get_tables_assignments();
    int64_t N = tas.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObTableAssignment& ta = tas.at(i);
      const TableItem* table = update_stmt->get_table_item_by_id(ta.table_id_);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid table assignment", K(table));
      } else {
        uint64_t table_id = ta.table_id_;
        IndexDMLInfo* primary_dml_info = NULL;
        const ObTableSchema* table_schema = NULL;
        const uint64_t base_table_id = table->get_base_table_item().ref_id_;
        if (OB_ISNULL(primary_dml_info = update_stmt->get_or_add_table_columns(table_id))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to get table columns", K(table_id));
        } else if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table, primary_dml_info->column_exprs_))) {
          LOG_WARN("add all rowkey columns to stmt failed", K(ret));
        } else if (OB_FAIL(schema_checker_->get_table_schema(base_table_id, table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), K(base_table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid table schema", K(table_schema));
        } else {
          primary_dml_info->table_id_ = table->table_id_;
          primary_dml_info->loc_table_id_ = table->get_base_table_item().table_id_;
          primary_dml_info->index_tid_ = base_table_id;
          primary_dml_info->rowkey_cnt_ = table_schema->get_rowkey_column_num();
          primary_dml_info->part_cnt_ = table_schema->get_partition_cnt();
          primary_dml_info->all_part_num_ = table_schema->get_all_part_num();
          primary_dml_info->index_name_ = table_schema->get_table_name_str();

          int64_t binlog_row_image = ObBinlogRowImage::FULL;
          if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
            LOG_WARN("fail to get binlog row image", K(ret));
          } else if (need_all_columns(*table_schema, binlog_row_image)) {
            if (OB_FAIL(add_all_columns_to_stmt(*table, primary_dml_info->column_exprs_))) {
              LOG_WARN("fail to add all column to stmt", K(ret), K(table_id));
            }
          } else {
            int64_t M = ta.assignments_.count();
            for (int64_t j = 0; OB_SUCC(ret) && j < M; ++j) {
              ObAssignment& assign = ta.assignments_.at(j);
              if (OB_FAIL(add_var_to_array_no_dup(primary_dml_info->column_exprs_, assign.column_expr_))) {
                LOG_WARN("failed to add all table columns", K(ret));
              } else if (OB_FAIL(add_index_related_columns_to_stmt(
                             *table, assign.column_expr_->get_column_id(), primary_dml_info->column_exprs_))) {
                LOG_WARN("failed to add index columns", K(ret));
              }
            }
          }
        }
      }
    }  // end for
  }
  return ret;
}

int ObUpdateResolver::resolve_cascade_updated_global_index(
    const ObTableAssignment& ta, ObIArray<uint64_t>& cascade_global_index)
{
  int ret = OB_SUCCESS;
  const TableItem* table = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params are invalid", K(ret), K(get_stmt()), K_(schema_checker));
  } else if (OB_ISNULL(table = get_stmt()->get_table_item_by_id(ta.table_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table item is invalid", K(ret), K(ta.table_id_));
  } else {
    uint64_t ref_table_id = table->get_base_table_item().ref_id_;
    uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
    int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
    if (OB_FAIL(schema_checker_->get_can_write_index_array(
            ref_table_id, index_tid, index_cnt, true /*only fetch global index*/))) {
      LOG_WARN("get can write index array failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
      bool is_exist = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_exist && j < ta.assignments_.count(); ++j) {
        uint64_t column_id = OB_INVALID_ID;
        ColumnItem* col_item = NULL;
        const ObAssignment& assign = ta.assignments_.at(j);
        if (OB_ISNULL(assign.column_expr_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("assign column expr is null");
        } else if (OB_ISNULL(col_item = get_update_stmt()->get_column_item_by_id(
                                 table->table_id_, assign.column_expr_->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column item not found", K(ret), K(table->table_id_), K(assign.column_expr_->get_column_id()));
        } else {
          column_id = table->is_generated_table() ? col_item->base_cid_ : col_item->column_id_;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(schema_checker_->check_column_exists(index_tid[i], column_id, is_exist))) {
          LOG_WARN("check column exists failed", K(ret));
        } else if (is_exist) {
          if (OB_FAIL(cascade_global_index.push_back(index_tid[i]))) {
            LOG_WARN("add index tid to cascade global index failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::resolve_multi_table_dml_info(const ObTableAssignment& ta, ObIArray<uint64_t>& global_indexs)
{
  int ret = OB_SUCCESS;
  int64_t binlog_row_image = 0;

  ObUpdateStmt* update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt) || OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update stmt is null", K(params_.session_info_), K(update_stmt));
  } else if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    update_stmt->set_has_global_index(!global_indexs.empty());
    IndexDMLInfo index_dml_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < global_indexs.count(); ++i) {
      index_dml_info.reset();
      const ObTableSchema* index_schema = NULL;
      const ObAssignments& assignments = ta.assignments_;
      const TableItem* table = NULL;
      uint64_t index_tid = global_indexs.at(i);
      if (OB_FAIL(schema_checker_->get_table_schema(index_tid, index_schema))) {
        LOG_WARN("get index table schema failed", K(ret), K(index_tid));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null");
      } else if (OB_FAIL(resolve_index_related_column_exprs(
                     ta.table_id_, *index_schema, assignments, index_dml_info.column_exprs_))) {
        LOG_WARN("resolve index related column exprs failed", K(ret));
      } else if (OB_ISNULL(table = update_stmt->get_table_item_by_id(ta.table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table item", K(ret), K(ta.table_id_));
      } else {
        index_dml_info.table_id_ = ta.table_id_;
        index_dml_info.loc_table_id_ = table->get_base_table_item().table_id_;
        index_dml_info.index_tid_ = index_tid;
        index_dml_info.part_cnt_ = index_schema->get_partition_cnt();
        index_dml_info.all_part_num_ = index_schema->get_all_part_num();
        index_dml_info.rowkey_cnt_ = index_schema->get_rowkey_column_num();
        if (OB_FAIL(index_schema->get_index_name(index_dml_info.index_name_))) {
          LOG_WARN("get index name from index schema failed", K(ret));
        } else if (OB_FAIL(update_stmt->add_multi_table_dml_info(index_dml_info))) {
          LOG_WARN("add index dml info to update stmt failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !with_clause_without_record_) {
        ObSchemaObjVersion table_version;
        table_version.object_id_ = index_schema->get_table_id();
        table_version.object_type_ = DEPENDENCY_TABLE;
        table_version.version_ = index_schema->get_schema_version();
        if (OB_FAIL(update_stmt->add_global_dependency_table(table_version))) {
          LOG_WARN("add global dependency table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::check_view_updatable()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = get_update_stmt();
  if (NULL == update_stmt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update stmt is NULL", K(ret));
  }
  // uv_check_basic already checked
  if (OB_SUCC(ret) && is_mysql_mode()) {
    FOREACH_CNT_X(assign, update_stmt->get_tables_assignments(), OB_SUCC(ret))
    {
      const TableItem* table = update_stmt->get_table_item_by_id(assign->table_id_);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(assign->table_id_));
      } else if (!table->is_generated_table()) {
        continue;
      }

      // check select item subquery
      if (OB_SUCC(ret)) {
        bool has_subquery = false;
        bool has_dependent_subquery = false;
        bool ref_update_table = false;
        if (OB_FAIL(ObResolverUtils::uv_check_select_item_subquery(
                *table, has_subquery, has_dependent_subquery, ref_update_table))) {
          LOG_WARN("updatable view check select item failed", K(ret));
        } else {
          LOG_DEBUG("update view check", K(has_subquery), K(has_dependent_subquery), K(ref_update_table));
          ret = has_dependent_subquery ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret)) {
        bool ref_update_table = false;
        if (OB_FAIL(ObResolverUtils::uv_check_where_subquery(*table, ref_update_table))) {
          LOG_WARN("update view check where condition failed", K(ret));
        } else {
          LOG_DEBUG("update view check", K(ref_update_table));
          ret = ref_update_table ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret)) {
        bool has_non_inner_join = false;
        if (OB_FAIL(ObResolverUtils::uv_check_has_non_inner_join(*table, has_non_inner_join))) {
          LOG_WARN("check has non inner join failed", K(ret));
        } else {
          LOG_DEBUG("update view check", K(has_non_inner_join));
          ret = has_non_inner_join ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
        }
      }

      if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        ObString upd_str = "UPDATE";
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table->get_table_name().length(),
            table->get_table_name().ptr(),
            upd_str.length(),
            upd_str.ptr());
      }
    }
  }

  if (OB_SUCC(ret) && lib::is_oracle_mode()) {
    FOREACH_CNT_X(assign, update_stmt->get_tables_assignments(), OB_SUCC(ret))
    {
      const TableItem* table = update_stmt->get_table_item_by_id(assign->table_id_);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(assign->table_id_));
      } else if (!table->is_generated_table()) {
        continue;
      }

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
          ret = !key_preserved ? OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED : ret;
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::try_add_rowid_column_to_stmt(const ObTableAssignment& tas)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = get_update_stmt();
  IndexDMLInfo* primary_dml_info = NULL;
  ObArray<ObColumnRefRawExpr*> column_exprs;
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "delete stmt is null", K(ret));
  } else if (OB_ISNULL(primary_dml_info = update_stmt->get_or_add_table_columns(tas.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "unexpecteed null primary dml info", K(ret));
  } else if (OB_FAIL(update_stmt->get_column_exprs(column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    int rowid_col_idx = -1;
    for (int i = 0; - 1 == rowid_col_idx && OB_SUCC(ret) && i < column_exprs.count(); i++) {
      if (OB_ISNULL(column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "unexpected null column expr", K(ret));
      } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_exprs.at(i)->get_column_id() &&
                 tas.table_id_ == column_exprs.at(i)->get_table_id()) {
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

int ObUpdateResolver::is_multi_table_update(const ObDMLStmt* stmt, bool& is_multi_table)
{
  int ret = OB_SUCCESS;
  is_multi_table = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (stmt->get_from_item_size() > 1) {
    is_multi_table = true;
  } else {
    const TableItem* table_item = stmt->get_table_item(stmt->get_from_item(0));
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table item", K(ret));
    } else if (table_item->is_joined_table()) {
      is_multi_table = true;
    } else if (table_item->is_generated_table() &&
               OB_FAIL(is_multi_table_update(table_item->ref_query_, is_multi_table))) {
      LOG_WARN("failed to check is multi table update", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
