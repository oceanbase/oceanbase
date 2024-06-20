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

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
ObUpdateResolver::ObUpdateResolver(ObResolverParams &param)
  : ObDelUpdResolver(param)
{
  param.contain_dml_ = true;
}

ObUpdateResolver::~ObUpdateResolver()
{
}

int ObUpdateResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = NULL;
  ObSEArray<ObTableAssignment, 2> tables_assign;
  bool has_tg = false;
  bool disable_limit_offset = false;
  if (T_UPDATE != parse_tree.type_
      || 3 > parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_)
      || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree for update", K(parse_tree.type_),
             K(parse_tree.num_child_), K(parse_tree.children_), K((session_info_)));
  } else if (OB_ISNULL(update_stmt = create_stmt<ObUpdateStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create update stmt failed");
  } else if (OB_FAIL(session_info_->check_feature_enable(ObCompatFeatureType::UPD_LIMIT_OFFSET,
                                                         disable_limit_offset))) {
    LOG_WARN("failed to check feature enable", K(ret));
  } else {
    stmt_ = update_stmt;
    update_stmt->set_ignore(false);
    if (NULL != parse_tree.children_[IGNORE]) {
      update_stmt->set_ignore(true);
      session_info_->set_ignore_stmt(true);
    }
    if (lib::is_oracle_mode() && NULL != parse_tree.children_[ERRORLOGGING]) {
      update_stmt->set_is_error_logging(true);
    }
  }

  // resolve outline data hints first
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_outline_data_hints())) {
      LOG_WARN("resolve outline data hints failed", K(ret));
    }
  }

  // resolve with clause before resolve table items
  if (OB_SUCC(ret) && is_mysql_mode()) {
    if (OB_FAIL(resolve_with_clause(parse_tree.children_[WITH_MYSQL]))) {
      LOG_WARN("resolve outline data hints failed", K(ret));
    }
  }

  // 1. resolve table items
  if (OB_SUCC(ret)) {
    ParseNode *table_node = parse_tree.children_[TABLE];
    if (OB_FAIL(resolve_table_list(*table_node))) {
      LOG_WARN("resolve table failed", K(ret));
    } else {
      has_tg = update_stmt->has_instead_of_trigger();
    }
  }

  if (OB_SUCC(ret)) {
    current_scope_ = T_UPDATE_SCOPE;
    // resolve assignments
    ParseNode *assign_list = parse_tree.children_[UPDATE_LIST];
    if (OB_ISNULL(assign_list) || OB_UNLIKELY(T_ASSIGN_LIST != assign_list->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid assign list node", K(assign_list));
    } else if (OB_FAIL(resolve_assignments(*assign_list,
                                           tables_assign,
                                           current_scope_))) {
      LOG_WARN("fail to resolve assignment", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(it, update_stmt->get_table_items(), OB_SUCC(ret)) {
      if (NULL != *it && ((*it)->is_generated_table() || (*it)->is_temp_table())) {
        if ((NULL != (*it)->view_base_item_ || has_tg) &&
            OB_FAIL(add_all_column_to_updatable_view(*update_stmt, *(*it), has_tg))) {
          LOG_WARN("add all column for updatable view failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(view_pullup_special_column_exprs())) {
        LOG_WARN("view pullup generated column exprs failed",K(ret));
      } else if (!has_tg && OB_FAIL(view_pullup_part_exprs())) {
        LOG_WARN("view pull up part exprs failed", K(ret));
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret) && !has_tg) {
    // 解析级联更新的列
    if (OB_FAIL(resolve_additional_assignments(tables_assign,
                                               T_UPDATE_SCOPE))) {
      LOG_WARN("fail to resolve_additional_assignments", K(ret));
    }
  }

  //add column for table scan, keep rowkey column in the head
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_assign.count(); i++) {
      if (OB_FAIL(generate_update_table_info(tables_assign.at(i)))) {
        LOG_WARN("failed to generate update table info", K(ret));
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_multi_update_table_conflict())) {
      LOG_WARN("failed to check multi-update table conflict", K(ret));
    } else if (update_stmt->is_ignore() && update_stmt->has_global_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ignore with global index");
    } else { /*do nothing*/ }
  }

  // 3. resolve other clauses
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_add_remove_const_expr_for_assignments())) {
      LOG_WARN("failed to add remove const expr", K(ret));
    } else if (OB_FAIL(resolve_update_constraints())) {
      LOG_WARN("failed to resolve check exprs", K(ret));
    } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT]))) {
      LOG_WARN("resolve hints failed", K(ret));
    } else if (OB_FAIL(resolve_where_clause(parse_tree.children_[WHERE]))) {
      LOG_WARN("resolve where clause failed", K(ret));
    } else if (params_.is_batch_stmt_ && OB_FAIL(generate_batched_stmt_info())) {
      LOG_WARN("failed to generate batched stmt info", K(ret));
    } else if (OB_FAIL(resolve_order_clause(parse_tree.children_[ORDER_BY]))) {
      LOG_WARN("resolve order clause failed", K(ret));
    } else if (OB_FAIL(resolve_limit_clause(parse_tree.children_[LIMIT], disable_limit_offset))) {
      LOG_WARN("resolve limit clause failed", K(ret));
    } else if (OB_FAIL(resolve_returning(parse_tree.children_[RETURNING]))) {
      LOG_WARN("resolve returning failed", K(ret));
    } else if (OB_FAIL(try_expand_returning_exprs())) {
      LOG_WARN("failed to try expand returning exprs", K(ret));
    } else if (lib::is_oracle_mode() && OB_NOT_NULL(parse_tree.children_[ERRORLOGGING]) &&
               OB_FAIL(resolve_error_logging(parse_tree.children_[ERRORLOGGING]))) {
      LOG_WARN("failed to resolve error logging", K(ret));
    } else if (is_mysql_mode() && !update_stmt->is_ignore() &&
               OB_FAIL(check_join_update_conflict())) {
      LOG_WARN("failed to check join update conflict", K(ret));
    } else if (OB_FAIL(update_stmt->formalize_stmt(session_info_))) {
      LOG_WARN("pull update stmt all expr relation ids failed", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_view_updatable())) {
      LOG_TRACE("view not updatable", K(ret));
    } else if (OB_FAIL(update_stmt->check_dml_need_filter_null())) {
      LOG_WARN("failed to check dml need filter null", K(ret));
    } else if (OB_FAIL(update_stmt->check_dml_source_from_join())) {
      LOG_WARN("failed to check dml source from join", K(ret));
    } else if (lib::is_mysql_mode() && OB_FAIL(check_safe_update_mode(update_stmt))) {
      LOG_WARN("failed to check fulfill safe update mode", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObUpdateResolver::try_expand_returning_exprs()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = NULL;
  // we do not need expand returing expr in prepare stage because we resolve
  // it twice, first in prepare stage, second in actual execution. We can only
  // do it in second stage
  // Otherwise if we expand in prepare stage, which will pollute our spell SQL
  // then got a wrong result
  bool need_expand = !is_prepare_stage_;
  if (OB_ISNULL(update_stmt = get_update_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!update_stmt->get_returning_exprs().empty() && need_expand) {
    // The old engine pass the updated row to the returning expression to
    // get the updated value. We can not do this in static engine, we need to
    // replace the column with the assigned value.
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    if (OB_UNLIKELY(1 != tables_info.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table info count", K(ret));
    } else {
      ObIArray<ObAssignment> &assignments = tables_info.at(0)->assignments_;
      ObRawExprCopier copier(*params_.expr_factory_);
      for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
        if (assignments.at(i).column_expr_->is_xml_column()) {
          // skip and will rewite in transform stage
        } else if (OB_FAIL(copier.add_replaced_expr(assignments.at(i).column_expr_,
                                             assignments.at(i).expr_))) {
          LOG_WARN("failed to add replaced expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(copier.add_skipped_expr(update_stmt->get_returning_aggr_items(), false))) {
          LOG_WARN("failed to add uncopy exprs", K(ret));
        } else if (OB_FAIL(copier.copy_on_replace(update_stmt->get_returning_exprs(),
                                                  update_stmt->get_returning_exprs()))) {
          LOG_WARN("failed to copy on replace returning exprs", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_oracle_mode()) {
    if (OB_FAIL(check_update_assign_duplicated(update_stmt))) {
      LOG_WARN("update has duplicate columns", K(ret));
    }
  }

  return ret;
}

int ObUpdateResolver::check_update_assign_duplicated(const ObUpdateStmt *update_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    // check duplicate assignments
    // update t1 set c1 = 1, c1 = 2;
    const ObIArray<ObUpdateTableInfo*> &table_infos = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObUpdateTableInfo* table_info = table_infos.at(i);
      if (OB_ISNULL(table_info))  {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
        const ObAssignment &assign_item = table_info->assignments_.at(j);
        if (assign_item.is_duplicated_) {
          ret = OB_ERR_FIELD_SPECIFIED_TWICE;
          LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(assign_item.column_expr_->get_column_name()));
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::try_add_remove_const_expr_for_assignments()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_update_stmt()) ||OB_ISNULL(session_info_) ||
      OB_ISNULL(schema_checker_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(session_info_), K(schema_checker_),
        K(params_.expr_factory_), K(ret));
  } else if (stmt->has_instead_of_trigger()) {
    /*do nothing*/
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      const ObTableSchema *table_schema = NULL;
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                           tables_info.at(i)->ref_table_id_,
                                                           table_schema,
                                                           tables_info.at(i)->is_link_table_))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(tables_info.at(i)->ref_table_id_));
      } else if (!table_schema->is_user_table()) {
        /*do nothing*/
      } else {
        const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema->get_foreign_key_infos();
        ObIArray<ObAssignment> &assignments = tables_info.at(i)->assignments_;
        for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
          ObAssignment &assign = assignments.at(i);
          if (OB_ISNULL(assign.expr_) || OB_ISNULL(assign.column_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(assign.expr_), K(assign.column_expr_), K(ret));
          } else if (assign.expr_->is_const_expr() &&
                     is_parent_col_self_ref_fk(assign.column_expr_->get_column_id(), fk_infos)) {
            ObRawExpr *new_expr = NULL;
            if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(*params_.expr_factory_,
                                                                *session_info_,
                                                                 assign.expr_,
                                                                 new_expr))) {
              LOG_WARN("failed to build expr", K(ret));
            } else if (OB_ISNULL(new_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else {
              assign.expr_ = new_expr;
            }
          }
        }
      }
    }
  }
  return ret;
}

// check if %parent_col_id is parent column of a self reference foreign key
bool ObUpdateResolver::is_parent_col_self_ref_fk(uint64_t parent_col_id,
                                                const ObIArray<ObForeignKeyInfo> &fk_infos)
{
  bool bret = false;
  for (int64_t i = 0; i < fk_infos.count(); i++) {
    const ObForeignKeyInfo &fk_info = fk_infos.at(i);
    if (fk_info.parent_table_id_ == fk_info.child_table_id_) {
      if (has_exist_in_array(fk_info.parent_column_ids_, parent_col_id, NULL)) {
        bret = true;
        break;
      }
    }
  }
  return bret;
}

int ObUpdateResolver::check_safe_update_mode(ObUpdateStmt *update_stmt)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), K(params_.session_info_), K(update_stmt));
  } else if (OB_FAIL(params_.session_info_->get_sql_safe_updates(is_sql_safe_updates))) {
     LOG_WARN("failed to get is safe update mode", K(ret));
  } else if (is_sql_safe_updates) {
    /*mysql安全模式下更新表值，需要满足下面两个条件中的其中一个:
    * 1.含有limit；
    * 2.含有where条件，其能够抽取query range ==> 由于抽取query range只能在optimizer阶段才能够抽取,因此这里只
    *   检查是否存在where条件；
    */
    if (!update_stmt->has_limit() && update_stmt->get_condition_exprs().empty()) {
      ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
      LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
    }
  } else {/*do nothing*/}
  return ret;
}

int ObUpdateResolver::check_multi_update_table_conflict()
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt *update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (update_stmt->get_update_table_info().count() <= 1) {
    /*do nothing*/
  } else {
    const ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count() - 1; ++i) {
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < tables_info.count(); ++j) {
          if (OB_ISNULL(tables_info.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (tables_info.at(i)->table_id_ != tables_info.at(j)->table_id_ &&
                     tables_info.at(i)->ref_table_id_ == tables_info.at(j)->ref_table_id_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple aliases to same table");
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::check_join_update_conflict()
{
  int ret = OB_SUCCESS;
  bool is_join_update = false;
  ObUpdateStmt *update_stmt = NULL;
  if (OB_ISNULL(update_stmt = get_update_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_join_table_update(update_stmt, is_join_update))) {
    LOG_WARN("failed to check is join table update", K(ret));
  } else if (is_join_update && update_stmt->has_order_by()) {
    // Incorrect usage of UPDATE and ORDER BY
    ret = OB_ERR_UPDATE_ORDER_BY;
  } else if (is_join_update && update_stmt->has_limit()) {
    // Incorrect usage of UPDATE and LIMIT
    ret = OB_ERR_UPDATE_LIMIT;
  }
  return ret;
}

int ObUpdateResolver::resolve_table_list(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  TableItem *table_item = NULL;
  ObSelectStmt *ref_stmt = NULL;
  if (OB_UNLIKELY(T_TABLE_REFERENCES != parse_tree.type_)
      || OB_UNLIKELY(parse_tree.num_child_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid update stmt", K(update_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
    const ParseNode *table_node = parse_tree.children_[i];
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table node is null");
    } else if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
      LOG_WARN("failed to resolve table", K(ret));
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
        LOG_WARN("add reference table to namespace checker failed", K(ret));
      } else if (OB_FAIL(update_stmt->add_from_item(table_item->table_id_, table_item->is_joined_table()))) {
        LOG_WARN("failed to add from item", K(ret));
      } else if (OB_FAIL(check_need_fired_trigger(table_item))) {
        LOG_WARN("failed to check need fired trigger", K(ret));
      } else {
      /*
        In order to share the same logic with 'select' to generate access path costly, we
        add the table in the udpate stmt in the from_item list as well.
       */
        LOG_DEBUG("succ to add from item", KPC(table_item));
      }
    }
  }
  if (OB_SUCC(ret) && is_mysql_mode() && 1 == update_stmt->get_from_item_size()) {
    const TableItem *table_item = update_stmt->get_table_item(update_stmt->get_from_item(0));
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (table_item->cte_type_ != TableItem::NOT_CTE) {
      ret = OB_ERR_NON_UPDATABLE_TABLE;
      const ObString &table_name = table_item->alias_name_.empty() ? table_item->table_name_ : table_item->alias_name_;
      ObString scope_name = "UPDATE";
      LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
                      table_name.length(), table_name.ptr(),
                      scope_name.length(), scope_name.ptr());
      LOG_WARN("table is not updatable", K(ret));
    }
  }
  return ret;
}

int ObUpdateResolver::generate_update_table_info(ObTableAssignment &table_assign)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObUpdateStmt *update_stmt = get_update_stmt();
  const ObTableSchema *table_schema = NULL;
  const TableItem *table_item = NULL;
  ObUpdateTableInfo *table_info = NULL;
  uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
  int64_t gindex_cnt = OB_MAX_INDEX_PER_TABLE;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(params_.session_info_) ||
      OB_ISNULL(allocator_) || OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(schema_checker_), K(params_.session_info_),
        K(allocator_), K(update_stmt), K(ret));
  } else if (OB_ISNULL(table_item = update_stmt->get_table_item_by_id(table_assign.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_item), K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                       table_item->get_base_table_item().ref_id_,
                                                       table_schema,
                                                       table_item->get_base_table_item().is_link_table()))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_can_write_index_array(params_.session_info_->get_effective_tenant_id(),
                                                                table_item->get_base_table_item().ref_id_,
                                                                index_tid, gindex_cnt, true))) {
    LOG_WARN("failed to get global index", K(ret));
  } else if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else if (NULL == (ptr = allocator_->alloc(sizeof(ObUpdateTableInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table info", K(ret));
  } else {
    table_info = new(ptr) ObUpdateTableInfo();
    if (OB_FAIL(table_info->assignments_.assign(table_assign.assignments_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (OB_FAIL(table_info->part_ids_.assign(table_item->get_base_table_item().part_ids_))) {
      LOG_WARN("failed to assign part ids", K(ret));
    } else if (!update_stmt->has_instead_of_trigger()) {
      if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table_item, table_info->column_exprs_))) {
        LOG_WARN("add all rowkey columns to stmt failed", K(ret));
      } else if (need_all_columns(*table_schema, binlog_row_image) ||
                 update_stmt->is_error_logging()) {
        if (OB_FAIL(add_all_columns_to_stmt(*table_item, table_info->column_exprs_))) {
          LOG_WARN("fail to add all column to stmt", K(ret), K(*table_item));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_assign.assignments_.count(); ++i) {
          ObAssignment &assign = table_assign.assignments_.at(i);
          if (OB_FAIL(add_var_to_array_no_dup(table_info->column_exprs_, assign.column_expr_))) {
            LOG_WARN("failed to add all table columns", K(ret));
          } else if (OB_FAIL(add_index_related_columns_to_stmt(*table_item,
                             assign.column_expr_->get_column_id(), table_info->column_exprs_))) {
            LOG_WARN("failed to add index columns", K(ret));
          } else { /*do nothing*/ }
        }
      }
      if (OB_SUCC(ret)) {
        table_info->table_id_ = table_item->table_id_;
        table_info->loc_table_id_ = table_item->get_base_table_item().table_id_;
        table_info->ref_table_id_ = table_item->get_base_table_item().ref_id_;
        table_info->table_name_ = table_schema->get_table_name_str();
        table_info->is_link_table_ = table_item->get_base_table_item().is_link_table();
      }
    } else {
      // view has `instead of trigger`
      // `update (select * from t1) t set t.c1 = 1` is legal in Oracle
      uint64_t view_id = OB_INVALID_ID;
      if (OB_FAIL(add_all_columns_to_stmt_for_trigger(*table_item, table_info->column_exprs_))) {
        LOG_WARN("failed to add all columns to stmt", K(ret));
      } else if (OB_FAIL(get_view_id_for_trigger(*table_item, view_id))) {
        LOG_WARN("get view id failed", K(table_item), K(ret));
      } else {
        table_info->table_id_ = table_item->table_id_;
        table_info->loc_table_id_ = table_item->table_id_;
        table_info->ref_table_id_ = view_id;
        table_info->table_name_ = table_item->table_name_;
      }
    }
    if (OB_SUCC(ret)) {
      TableItem *rowkey_doc = NULL;
      if (OB_FAIL(try_add_join_table_for_fts(table_item, rowkey_doc))) {
        LOG_WARN("fail to try add join table for fts", K(ret), KPC(table_item));
      } else if (OB_NOT_NULL(rowkey_doc) && OB_FAIL(try_update_column_expr_for_fts(
                                                                      *table_item,
                                                                      rowkey_doc,
                                                                      table_info->column_exprs_))) {
        LOG_WARN("fail to try update column expr for fts", K(ret), KPC(table_item));
      } else if (OB_FAIL(update_stmt->get_update_table_info().push_back(table_info))) {
        LOG_WARN("failed to push back table info", K(ret));
      } else if (gindex_cnt > 0) {
        update_stmt->set_has_global_index(true);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObUpdateResolver::check_view_updatable()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  if (NULL == update_stmt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update stmt is NULL", K(ret));
  } else if (is_mysql_mode()) {
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      const TableItem *table_item = NULL;
      if (OB_ISNULL(tables_info.at(i)) ||
          OB_ISNULL(table_item = update_stmt->get_table_item_by_id(tables_info.at(i)->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(table_item), K(ret));
      } else if (!table_item->is_generated_table() && !table_item->is_temp_table()) {
        continue;
      } else {
        // check select item subquery
        if (OB_SUCC(ret)) {
          bool has_subquery = false;
          bool has_dependent_subquery = false;
          bool ref_update_table = false;
          if (OB_FAIL(ObResolverUtils::uv_check_select_item_subquery(
              *table_item, has_subquery, has_dependent_subquery, ref_update_table))) {
            LOG_WARN("updatable view check select item failed", K(ret));
          } else {
            LOG_DEBUG("update view check",
                K(has_subquery), K(has_dependent_subquery), K(ref_update_table));
            ret = has_dependent_subquery ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          bool ref_update_table = false;
          if (OB_FAIL(ObResolverUtils::uv_check_where_subquery(*table_item, ref_update_table))) {
            LOG_WARN("update view check where condition failed", K(ret));
          } else {
            LOG_DEBUG("update view check", K(ref_update_table));
            ret = ref_update_table ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret)) {
          bool has_non_inner_join = false;
          if (OB_FAIL(ObResolverUtils::uv_check_has_non_inner_join(*table_item, has_non_inner_join))) {
            LOG_WARN("check has non inner join failed", K(ret));
          } else {
            LOG_DEBUG("update view check", K(has_non_inner_join));
            ret = has_non_inner_join ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
          }
        }

        if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
          ObString upd_str = "UPDATE";
          LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
              table_item->get_table_name().length(), table_item->get_table_name().ptr(),
              upd_str.length(), upd_str.ptr());
        }
      }
    }
  } else if (lib::is_oracle_mode() && !update_stmt->has_instead_of_trigger()) {
    // 兼容oracle,如果包含instead trigger,不做下面的检查,因为不会真正执行dml语句
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      const TableItem *table_item = NULL;
      if (OB_ISNULL(tables_info.at(i)) ||
          OB_ISNULL(table_item = update_stmt->get_table_item_by_id(tables_info.at(i)->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(table_item));
      } else if (!table_item->is_generated_table() && !table_item->is_temp_table()) {
        continue;
      } else {
        if (OB_SUCC(ret)) {
          bool has_distinct = false;
          if (OB_FAIL(ObResolverUtils::uv_check_oracle_distinct(
              *table_item, *session_info_, *schema_checker_, has_distinct))) {
            LOG_WARN("check updatable view distinct failed", K(ret));
          } else {
            LOG_DEBUG("check has distinct", K(ret), K(has_distinct));
            ret = has_distinct ? OB_ERR_ILLEGAL_VIEW_UPDATE : ret;
          }
        }

        // check key preserved table_item
        if (OB_SUCC(ret)) {
          bool key_preserved = 0;
          if (OB_FAIL(uv_check_key_preserved(*table_item, key_preserved))) {
            LOG_WARN("check key preserved failed", K(ret));
          } else {
            LOG_DEBUG("check key preserved", K(key_preserved));
            ret = !key_preserved ? OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED: ret;
          }
        }
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObUpdateResolver::is_join_table_update(const ObDMLStmt *stmt, bool &is_multi_table)
{
  int ret = OB_SUCCESS;
  is_multi_table = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (stmt->get_from_item_size() > 1) {
    is_multi_table = true;
  } else {
    const TableItem *table_item = stmt->get_table_item(stmt->get_from_item(0));
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table item", K(ret));
    } else if (table_item->is_joined_table()) {
      is_multi_table = true;
    } else if ((table_item->is_generated_table() || table_item->is_temp_table()) &&
               OB_FAIL(is_join_table_update(table_item->ref_query_, is_multi_table))) {
      LOG_WARN("failed to check is multi table update", K(ret));
    }
  }
  return ret;
}

int ObUpdateResolver::generate_batched_stmt_info()
{
  int ret = OB_SUCCESS;
  //extract all predicate column from condition exprs
  //see the issue:
  ObSEArray<ObRawExpr*, 4> predicate_columns;
  ObUpdateStmt *update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(update_stmt->get_condition_exprs(),
                                                          predicate_columns))) {
    LOG_WARN("extract column exprs failed", K(ret));
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); ++i) {
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        ObIArray<ObAssignment> &assignments = tables_info.at(i)->assignments_;
        for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
          ObAssignment &assignment = assignments.at(j);
          ObRawExpr *column_expr = assignment.column_expr_;
          bool contain_case_when = false;
          if (has_exist_in_array(predicate_columns, column_expr)) {
            assignment.is_predicate_column_ = true;
          } else if (OB_FAIL(ObRawExprUtils::check_contain_case_when_exprs(assignment.expr_,
                                                                           contain_case_when))) {
            LOG_WARN("fail to check contain case when", K(ret), K(assignment));
          } else if (contain_case_when) {
            ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
            LOG_TRACE("batched multi stmt contain case when expr", K(ret));
          }
        }

      }
    }
  }
  return ret;
}

int ObUpdateResolver::resolve_update_constraints()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    // resolve view-check exprs
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      TableItem *table_item = NULL;
      ObUpdateTableInfo* table_info = tables_info.at(i);
      if (OB_ISNULL(table_info) ||
          OB_ISNULL(table_item = update_stmt->get_table_item_by_id(table_info->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(table_item), K(ret));
      } else if (!update_stmt->has_instead_of_trigger() &&
                 OB_FAIL(resolve_view_check_exprs(table_item->table_id_, table_item, false, table_info->view_check_exprs_))) {
        LOG_WARN("failed to resolve view check exprs", K(ret));
      } else if (OB_FAIL(resolve_check_constraints(table_item, table_info->check_constraint_exprs_))) {
        LOG_WARN("failed to resolve view check exprs", K(ret));
      } else if (OB_FAIL(ObResolverUtils::prune_check_constraints(table_info->assignments_,
                                                                  table_info->check_constraint_exprs_))) {
        LOG_WARN("failed to prune check constraints", K(ret));
      } else {
        // TODO @yibo remove view check exprs in log_del_upd
        for (uint64_t j = 0; OB_SUCC(ret) && j < table_info->view_check_exprs_.count(); ++j) {
          if (OB_FAIL(ObTableAssignment::expand_expr(*params_.expr_factory_,
                                                     table_info->assignments_,
                                                     table_info->view_check_exprs_.at(j)))) {
            LOG_WARN("expand generated column expr failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
