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
#include "lib/string/ob_sql_string.h"

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
  uint64_t compat_version = 0;
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
  } else if (OB_FAIL(session_info_->get_compatibility_version(compat_version))) {
    LOG_WARN("failed to get compatibility version", K(ret));
  } else if (OB_FAIL(ObCompatControl::check_feature_enable(compat_version,
                                      ObCompatFeatureType::UPD_LIMIT_OFFSET, disable_limit_offset))) {
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

#ifdef OB_HOTSPOT_GROUP_COMMIT
  // Resolve primary key parameter positions if group_commit hint is enabled
  // Only do this in PS protocol prepare stage
  if (OB_SUCC(ret) && update_stmt->get_query_ctx()->get_global_hint().group_commit_enabled()) {
    if (params_.is_prepare_protocol_) {
      if (params_.is_prepare_stage_ &&OB_FAIL(resolve_group_commit_key_param_infos())) {
        LOG_WARN("failed to resolve pk param infos for group commit", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "group commit only supported in PS, Explain/Text/PL is not supported");
      LOG_WARN("group commit is not supported in non-prepare stage", K(ret));
    }
  }
#endif

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
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE,
              helper.convert(assign_item.column_expr_->get_column_name()));
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
    if (OB_ISNULL(table_item) || (session_info_->is_inner() && OB_ISNULL(session_info_->get_job_info()))) {
    } else if (OB_UNLIKELY(table_item->is_system_table_ && table_item->table_name_.case_compare(OB_ALL_LICENSE_TNAME) == 0)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("modify license table is not allowed", KR(ret), K(table_item->table_name_), K(table_item->is_system_table_));
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
      if (OB_FAIL(update_stmt->get_update_table_info().push_back(table_info))) {
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
        ObRawExprReplacer replacer;
        for (int64_t i = 0; OB_SUCC(ret) && i < table_info->assignments_.count(); ++i) {
          if (OB_FAIL(replacer.add_replace_expr(table_info->assignments_.at(i).column_expr_,
                                                 table_info->assignments_.at(i).expr_))) {
            LOG_WARN("failed to add replace expr", K(ret));
          }
        }
        for (uint64_t j = 0; OB_SUCC(ret) && j < table_info->view_check_exprs_.count(); ++j) {
          if (OB_FAIL(replacer.replace(table_info->view_check_exprs_.at(j)))) {
            LOG_WARN("expand generated column expr failed", K(ret));
          }
        }
        if (!table_info->view_check_exprs_.empty()) {
          ObRawExpr *child = NULL;
          if (table_info->view_check_exprs_.at(0)->is_op_expr()) {
            child = static_cast<ObOpRawExpr *>(table_info->view_check_exprs_.at(0))->get_param_expr(0);
          }
        }
      }
    }
  }
  return ret;
}

#ifdef OB_HOTSPOT_GROUP_COMMIT
// Collect primary key and unique key column sets from table schema
int ObUpdateResolver::collect_key_sets(const ObTableSchema *table_schema,
                                       ObSEArray<ObSEArray<uint64_t, 4>, 4> &key_sets,
                                       int &primary_key_index_in_key_sets)
{
  int ret = OB_SUCCESS;
  primary_key_index_in_key_sets = -1;

  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null or schema checker or session info is null", K(ret));
  }

  // Collect primary key columns
  if (OB_SUCC(ret) &&!(table_schema->is_heap_table())) {
    const ObRowkeyInfo &pk_info = table_schema->get_rowkey_info();
    ObSEArray<uint64_t, 4> pk_cols;
    for (int64_t i = 0; OB_SUCC(ret) && i < pk_info.get_size(); ++i) {
      uint64_t col_id = OB_INVALID_ID;
      if (OB_FAIL(pk_info.get_column_id(i, col_id))) {
        LOG_WARN("get pk column id fail", K(ret));
      } else if (OB_FAIL(pk_cols.push_back(col_id))) {
        LOG_WARN("push back fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && pk_cols.count() > 0 && OB_FAIL(key_sets.push_back(pk_cols))) {
      LOG_WARN("push back pk fail", K(ret));
    }
    primary_key_index_in_key_sets = key_sets.count() - 1;
  }

  // Collect unique key columns
  if (OB_SUCC(ret)) {
    ObSEArray<ObAuxTableMetaInfo, 4> index_infos;
    if (OB_FAIL(table_schema->get_simple_index_infos(index_infos))) {
      LOG_WARN("get index infos fail", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
      const ObTableSchema *idx_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(
              session_info_->get_effective_tenant_id(), index_infos.at(i).table_id_, idx_schema))) {
        ret = OB_SUCCESS;  // ignore missing index
      } else if (OB_NOT_NULL(idx_schema) && idx_schema->is_unique_index()
          && is_available_index_status(idx_schema->get_index_status())) {
        const ObIndexInfo &idx_info = idx_schema->get_index_info();
        ObSEArray<uint64_t, 4> uk_cols;
        for (int64_t j = 0; OB_SUCC(ret) && j < idx_info.get_size(); ++j) {
          uint64_t col_id = OB_INVALID_ID;
          if (OB_FAIL(idx_info.get_column_id(j, col_id))) {
            LOG_WARN("get uk column id fail", K(ret));
          } else if (OB_FAIL(uk_cols.push_back(col_id))) {
            LOG_WARN("push back fail", K(ret));
          }
        }
        if (OB_SUCC(ret) && uk_cols.count() > 0 && OB_FAIL(key_sets.push_back(uk_cols))) {
          LOG_WARN("push back uk fail", K(ret));
        }
      }
    }
  }
  return ret;
}

// Build column_id -> param_idx map from WHERE "col = ?" conditions and equal conditions
int ObUpdateResolver::build_col_to_param_map(ObUpdateStmt *update_stmt,
                                             TableItem *target_table_item,
                                             hash::ObHashMap<uint64_t, int64_t> &questionmark_col_to_param,
                                             hash::ObHashMap<uint64_t, int64_t> &equal_col_to_param)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(update_stmt) || OB_ISNULL(target_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(update_stmt), KP(target_table_item));
  } else if (!questionmark_col_to_param.created() || !equal_col_to_param.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("questionmark_col_to_param or equal_col_to_param map not created", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &conds = update_stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
      ObRawExpr *expr = conds.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret), K(i));
        break;
      }
      ObColumnRefRawExpr *col = NULL;
      ObRawExpr *param = NULL;

      const ObItemType expr_type = expr->get_expr_type();
      bool is_questionmark_condition = false;
      if (T_OP_EQ == expr_type || T_OP_NSEQ == expr_type) {
        ObOpRawExpr *op = static_cast<ObOpRawExpr*>(expr);
        if (OB_UNLIKELY(op->get_param_count() < 2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op expr param count unexpected", K(ret), K(op->get_param_count()), K(expr_type));
          break;
        }
        ObRawExpr *left = op->get_param_expr(0);
        ObRawExpr *right = op->get_param_expr(1);
        if (OB_ISNULL(left) || OB_ISNULL(right)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op expr param is null", K(ret), K(expr_type));
          break;
        }

        if (ObRawExprUtils::is_column_ref_skip_implicit_cast(left)) {
          // col = xx
          if (ObRawExprUtils::unwrap_implicit_cast_and_check_expr_type(T_QUESTIONMARK, right)) {
            // col = ?
            is_questionmark_condition = true;
          }
          col = static_cast<ObColumnRefRawExpr*>(left);
          param = right;
        } else if (ObRawExprUtils::is_column_ref_skip_implicit_cast(right)) {
          // xx = col
          if (ObRawExprUtils::unwrap_implicit_cast_and_check_expr_type(T_QUESTIONMARK, left)) {
            // ? = col
            is_questionmark_condition = true;
          }
          col = static_cast<ObColumnRefRawExpr*>(right);
          param = left;
        }
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(param) && OB_NOT_NULL(col) && col->get_table_id() == target_table_item->table_id_) {
        int64_t idx = static_cast<ObConstRawExpr*>(param)->get_value().get_unknown();
        const uint64_t col_id = col->get_column_id();
        if (OB_FAIL(equal_col_to_param.set_refactored(col_id, idx, 1))) {
          LOG_WARN("set equal_col_to_param map fail", K(ret));
        }
        if (is_questionmark_condition) {
          if (OB_FAIL(questionmark_col_to_param.set_refactored(col_id, idx, 1))) {
            LOG_WARN("set questionmark_col_to_param map fail", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

// Check if any matched key columns are being updated in SET clause and report error if found e.g. update id = 1 where id = ?
int ObUpdateResolver::check_key_columns_updated(ObUpdateStmt *update_stmt,
                                                const share::schema::ObTableSchema *table_schema,
                                                TableItem *target_table_item,
                                                const common::hash::ObHashMap<uint64_t, int64_t> &col_to_param,
                                                const common::ObSEArray<int64_t, 4> &matched_param_idx_result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(update_stmt) || OB_ISNULL(table_schema) || OB_ISNULL(target_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(update_stmt), KP(table_schema), KP(target_table_item));
  } else if (matched_param_idx_result.count() == 0 || !col_to_param.created()) {
    // No matched key to check
  } else {
    // Get matched key column IDs from matched_param_idx_result
    ObSEArray<uint64_t, 4> matched_key_col_ids;
    for (hash::ObHashMap<uint64_t, int64_t>::const_iterator it = col_to_param.begin();
         OB_SUCC(ret) && it != col_to_param.end(); ++it) {
      int64_t param_idx = it->second;
      if (has_exist_in_array(matched_param_idx_result, param_idx)) {
        uint64_t col_id = it->first;
        if (OB_FAIL(matched_key_col_ids.push_back(col_id))) {
          LOG_WARN("push back matched key col id failed", K(ret), K(col_id));
        }
      }
    }

    // Check if any matched key columns are being updated
    if (OB_SUCC(ret) && matched_key_col_ids.count() > 0) {
      const ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();

      for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); ++i) {
        const ObUpdateTableInfo *table_info = tables_info.at(i);
        if (OB_ISNULL(table_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table info is null", K(ret), K(i));
        } else if (table_info->table_id_ == target_table_item->table_id_) {
          const ObIArray<ObAssignment> &assignments = table_info->assignments_;
          for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
            const ObAssignment &assign = assignments.at(j);
            if (OB_ISNULL(assign.column_expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column expr is null", K(ret), K(j));
            } else if (!assign.is_implicit_) {
              uint64_t col_id = assign.column_expr_->get_column_id();
              if (has_exist_in_array(matched_key_col_ids, col_id)) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "updating key columns in group commit");
                LOG_WARN("group commit does not support updating key columns", K(ret), K(col_id));
                break;
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

// Find complete key match from col_to_param map, prefer primary key
int ObUpdateResolver::find_matched_key(const hash::ObHashMap<uint64_t, int64_t> &questionmark_col_to_param,
                                       const hash::ObHashMap<uint64_t, int64_t> &equal_col_to_param,
                                       const ObSEArray<ObSEArray<uint64_t, 4>, 4> &key_sets,
                                       int primary_key_index_in_key_sets,
                                       ObSEArray<int64_t, 4> &matched_param_idx_result)
{
  int ret = OB_SUCCESS;
  matched_param_idx_result.reset();

  if (!questionmark_col_to_param.created() || !equal_col_to_param.created() || key_sets.count() == 0) {
    // No keys or map to check
  } else {
    for (int64_t k = 0; OB_SUCC(ret) && k < key_sets.count(); ++k) {
      const ObIArray<uint64_t> &cols = key_sets.at(k);
      ObSEArray<int64_t, 4> temp_matched_param_idx_result;
      for (int64_t c = 0; OB_SUCC(ret) && c < cols.count(); ++c) {
        int64_t temp_matched_param_idx = -1;
        if (OB_SUCCESS == questionmark_col_to_param.get_refactored(cols.at(c), temp_matched_param_idx)) {
          if (OB_FAIL(temp_matched_param_idx_result.push_back(temp_matched_param_idx))) {
            LOG_WARN("push back failed", K(ret));
          }
        } else {
          break;
        }
      }
      if (temp_matched_param_idx_result.count() == cols.count()) {
        if (k == primary_key_index_in_key_sets) {
          // found primary key, assign and break
          if (OB_FAIL(matched_param_idx_result.assign(temp_matched_param_idx_result))) {
            LOG_WARN("assign failed", K(ret));
          }
          break;
        }
        if (matched_param_idx_result.empty()) {
          // found first unique key, assign
            if (OB_FAIL(matched_param_idx_result.assign(temp_matched_param_idx_result))) {
              LOG_WARN("assign failed", K(ret));
              break;
            }
        } else {
          // found multiple unique key, error
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple unique key matched in where condition");
          LOG_WARN("multiple unique key matched, error");
          break;
        }
      }
    }

    if (OB_SUCC(ret) && matched_param_idx_result.count() == 0) {
      // we can not find any pk/uk in where condition
      // check if any pk/uk is in where condition but with value
      int value_matched_param_count = 0;
      for (int64_t k = 0; OB_SUCC(ret) && k < key_sets.count(); ++k) {
        const ObIArray<uint64_t> &cols = key_sets.at(k);
        for (int64_t c = 0; OB_SUCC(ret) && c < cols.count(); ++c) {
          if (OB_NOT_NULL(equal_col_to_param.get(cols.at(c)))) {
            value_matched_param_count++;
          } else {
            value_matched_param_count = 0;
            break;
          }
        }
        if (value_matched_param_count == cols.count()) {
          // find pk/uk in where condition but with value
          // e.g. prepare stmt from 'update t1 = ? where id = 1'
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "key matched in where condition but with value");
          LOG_WARN("key matched in where condition but with value, error");
          break;
        }
      }
    }

    if (OB_SUCC(ret) && matched_param_idx_result.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "no key matched in where condition");
      LOG_WARN("no key matched, skip");
    }
  }

  return ret;
}

// Resolve primary key parameter positions for group commit
int ObUpdateResolver::resolve_group_commit_key_param_infos()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  const ObTableSchema *table_schema = NULL;
  TableItem *target_table_item = NULL;
  hash::ObHashMap<uint64_t, int64_t> questionmark_col_to_param; // col = ?
  hash::ObHashMap<uint64_t, int64_t> equal_col_to_param; // WHERE col = rhs (? or literal)
  ObSEArray<ObSEArray<uint64_t, 4>, 4> key_sets;
  ObSEArray<int64_t, 4> matched_param_idx_result;
  int primary_key_index_in_key_sets = -1;

  uint64_t tenant_id = OB_NOT_NULL(session_info_)
                       ? session_info_->get_effective_tenant_id()
                       : OB_SERVER_TENANT_ID;
  key_sets.set_attr(ObMemAttr(tenant_id, "GrpCommitSets"));
  matched_param_idx_result.set_attr(ObMemAttr(tenant_id, "GrpCommitParams"));
  if (OB_FAIL(questionmark_col_to_param.create(16, "GrpCommitMap", ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("create questionmark_col_to_param map fail", K(ret));
  }
  if (OB_FAIL(equal_col_to_param.create(16, "GrpCommitVMap", ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("create equal_col_to_param map fail", K(ret));
  }

  if (OB_ISNULL(update_stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(update_stmt), KP(schema_checker_), KP(session_info_));
  }

  // Get target table item
  if (OB_SUCC(ret)) {
    const ObIArray<TableItem*> &tables = update_stmt->get_table_items();
    if (1 == tables.count()) {
      target_table_item = tables.at(0);
      if (OB_FAIL(schema_checker_->get_table_schema(
        session_info_->get_effective_tenant_id(), target_table_item->ref_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple table update or join update");
      LOG_WARN("does not support multiple table update or join update", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(collect_key_sets(table_schema, key_sets, primary_key_index_in_key_sets))) {
      LOG_WARN("collect key sets failed", K(ret),K(key_sets), K(primary_key_index_in_key_sets));
    } else if (OB_FAIL(build_col_to_param_map(update_stmt, target_table_item, questionmark_col_to_param, equal_col_to_param))) {
      LOG_WARN("build col to param map failed", K(ret), K(questionmark_col_to_param.created()));
    } else if (OB_FAIL(find_matched_key(questionmark_col_to_param, equal_col_to_param, key_sets, primary_key_index_in_key_sets, matched_param_idx_result))) {
      LOG_WARN("find matched key failed", K(ret), K(key_sets), K(questionmark_col_to_param.created()), K(primary_key_index_in_key_sets), K(matched_param_idx_result));
    } else if (OB_FAIL(check_key_columns_updated(update_stmt, table_schema, target_table_item, questionmark_col_to_param, matched_param_idx_result))) {
      LOG_WARN("check key columns updated failed", K(ret), K(questionmark_col_to_param.created()), K(matched_param_idx_result));
    } else if (OB_FAIL(update_stmt->set_group_commit_param_idx(matched_param_idx_result))) {
      LOG_WARN("set group commit param indices failed", K(ret), K(matched_param_idx_result));
    } else if (questionmark_col_to_param.created()) {
      questionmark_col_to_param.destroy();
    } else if (equal_col_to_param.created()) {
      equal_col_to_param.destroy();
    }
  }
  if (ret == OB_NOT_SUPPORTED) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (!tenant_config.is_valid() || !tenant_config->_enable_hotspot_group_commit) {
      LOG_INFO("group commit is not supported and tenant config is not enabled, still success", K(ret));
      ret = OB_SUCCESS;
    } else {
      if (tenant_config->_inlist_rewrite_threshold == 1) {
        LOG_WARN("group commit is not supported maybe because inlist_rewrite_threshold is 1", K(ret));
      }
    }
  }
  LOG_INFO("resolve group commit key params infos", K(ret), K(update_stmt->get_group_commit_param_idx()));
  return ret;
}
#endif

}  // namespace sql
}  // namespace oceanbase