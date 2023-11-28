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
#include "sql/optimizer/ob_optimizer_util.h"

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
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
ObDeleteResolver::ObDeleteResolver(ObResolverParams &params)
  : ObDelUpdResolver(params)
{
  params.contain_dml_ = true;
}

ObDeleteResolver::~ObDeleteResolver()
{
}

int ObDeleteResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  // create the delete stmt
  ObDeleteStmt *delete_stmt = NULL;
  bool is_multi_table_delete = false;
  if (NULL == (delete_stmt = create_stmt<ObDeleteStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create delete stmt failed", K(ret));
  } else if (OB_UNLIKELY(parse_tree.type_ != T_DELETE) || OB_UNLIKELY(parse_tree.num_child_ < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree type is invalid", K_(parse_tree.type), K_(parse_tree.num_child));
  } else if (OB_ISNULL(parse_tree.children_[TABLE])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_node is null", K(ret));
  } else {
    stmt_ = delete_stmt;
    if (OB_FAIL(resolve_outline_data_hints())) {
      LOG_WARN("resolve outline data hints failed", K(ret));
    } else if (is_mysql_mode() && OB_FAIL(resolve_with_clause(parse_tree.children_[WITH_MYSQL]))) {
      LOG_WARN("resolve with clause failed", K(ret));
    } else if (OB_FAIL(resolve_table_list(*parse_tree.children_[TABLE], is_multi_table_delete))) {
      LOG_WARN("resolve table failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_tables_.count(); ++i) {
        const TableItem *table_item = delete_tables_.at(i);
        if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(generate_delete_table_info(*table_item))) {
          LOG_WARN("failed to generate delete table info", K(ret));
        } else if (is_oracle_mode() && !delete_stmt->has_instead_of_trigger()) {
          // resolve check constraint for disable and validate check
          ObSEArray<ObRawExpr*, 4> ck_exprs;
          if (OB_FAIL(resolve_check_constraints(table_item, ck_exprs))) {
            LOG_WARN("resolve check constraint failed", K(ret));
          }
        } else { /*do nothing*/ }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_multi_delete_table_conflict())) {
        LOG_WARN("failed to check multi-delete table conflict", K(ret));
      }
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
      } else if (is_oracle_mode() && NULL != parse_tree.children_[ERRORLOGGING] &&
                 OB_FAIL(resolve_error_logging(parse_tree.children_[ERRORLOGGING]))) {
        LOG_WARN("failed to resolve error logging", K(ret));
      } else if (OB_FAIL(delete_stmt->formalize_stmt(session_info_))) {
        LOG_WARN("pull stmt all expr relation ids failed", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret)) {
      if (!delete_stmt->has_instead_of_trigger() && OB_FAIL(view_pullup_part_exprs())) {
        // instead of trigger的delete语句不需要 pull up
        LOG_WARN("view pull up part exprs failed", K(ret));
      } else if (OB_FAIL(check_view_deletable())) {
        LOG_WARN("failed to check view deletable", K(ret));
      } else if (OB_FAIL(delete_stmt->check_dml_need_filter_null())) {
        LOG_WARN("failed to check dml need filter null", K(ret));
      } else if (OB_FAIL(delete_stmt->check_dml_source_from_join())) {
        LOG_WARN("failed to check dml source from join");
      } else if (OB_FAIL(check_safe_update_mode(delete_stmt, is_multi_table_delete))) {
        LOG_WARN("failed to check safe update mode", K(ret));
      } else { /*do nothing */ }
    }
  }
  return ret;
}

int ObDeleteResolver::check_safe_update_mode(ObDeleteStmt *delete_stmt, bool is_multi_table_delete)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(delete_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), K(params_.session_info_), K(delete_stmt));
  } else if (OB_FAIL(params_.session_info_->get_sql_safe_updates(is_sql_safe_updates))) {
    LOG_WARN("failed to get is safe update mode", K(ret));
  } else if (is_sql_safe_updates) {
    /*mysql安全模式下更新表值，需要满足：
    * 前提条件：单表删除，不能是多表删除，必须含有where条件；
    * 下面条件二者至少满足其中一个：
    * 1.含有limit，where条件中有相关列存在；
    * 2.有where条件中能够抽取query range ==> 由于抽取query range只能在optimizer阶段才能够抽取，因此该条件在
    *   optimizer阶段去检查
    */
    if (is_multi_table_delete || delete_stmt->get_condition_exprs().empty()) {//前提条件
      ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
      LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
    } else if (delete_stmt->has_limit()) {//条件1
      bool is_const_expr = true;
      for (int64_t i = 0;
            OB_SUCC(ret) && is_const_expr && i < delete_stmt->get_condition_exprs().count();
            ++i) {
        const ObRawExpr *expr = delete_stmt->get_condition_expr(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(expr));
        } else {
          is_const_expr = expr->is_const_expr();
        }
      }
      if (OB_SUCC(ret) && is_const_expr) {
        ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
        LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObDeleteResolver::check_multi_delete_table_conflict()
{
  int ret = OB_SUCCESS;
  const ObDeleteStmt *delete_stmt = get_delete_stmt();
  if (OB_ISNULL(delete_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (delete_stmt->get_delete_table_info().count() <= 1) {
    /*do nothing*/
  } else {
    const ObIArray<ObDeleteTableInfo*> &tables_info = delete_stmt->get_delete_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count() - 1; ++i) {
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < tables_info.count(); ++j) {
          if (OB_ISNULL(tables_info.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (tables_info.at(i)->ref_table_id_ == tables_info.at(j)->ref_table_id_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple aliases to same table");
          }
        }
      }
    }
  }
  return ret;
}

int ObDeleteResolver::resolve_table_list(const ParseNode &table_list, bool &is_multi_table_delete)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  is_multi_table_delete = false;
  const ParseNode *delete_list = NULL;
  const ParseNode *from_list = NULL;
  ObDeleteStmt *delete_stmt = get_delete_stmt();

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
    const ParseNode *table_node = from_list->children_[i];
    CK(OB_NOT_NULL(table_node));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
        LOG_WARN("failed to resolve table", K(ret));
      } else if (table_item->is_function_table() || table_item->is_json_table()) {//兼容oracle行为
        ret = OB_WRONG_TABLE_NAME;
        LOG_WARN("invalid table name", K(ret));
      } else if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
        LOG_WARN("add reference table to namespace checker failed", K(ret));
      } else if (OB_FAIL(delete_stmt->add_from_item(table_item->table_id_,
                                                    table_item->is_joined_table()))) {
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
      bool has_tg = false;
      //single table delete, delete list is same with from list
      CK(delete_stmt->get_table_size() == 1);
      OZ(delete_tables_.push_back(delete_stmt->get_table_item(0)));
      if (OB_SUCC(ret) && delete_stmt->get_table_item(0)->is_view_table_ && is_oracle_mode()) {
        OZ(has_need_fired_trigger_on_view(delete_stmt->get_table_item(0), has_tg));
      }
      OX(delete_stmt->set_has_instead_of_trigger(has_tg));
    } else {
      //multi table delete
      is_multi_table_delete = true;
      ObString table_name;
      ObString db_name;
      CK(T_TABLE_REFERENCES == delete_list->type_);
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_list->num_child_; ++i) {
        table_name.reset();
        db_name.reset();
        table_item = NULL;
        const ParseNode *table_node = delete_list->children_[i];
        if (OB_FAIL(resolve_table_relation_node(table_node, table_name, db_name, true))) {
          LOG_WARN("failed to resolve table relation node", K(ret));
        } else if (OB_FAIL(find_delete_table_with_mysql_rule(db_name, table_name, table_item))) {
          LOG_WARN("failed to find delete table with mysql rule", K(ret));
        } else if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null table item", K(ret));
        } else if (OB_UNLIKELY(ObOptimizerUtil::find_item(delete_tables_, table_item))) {
          ret = OB_ERR_NONUNIQ_TABLE;
          LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, table_item->table_name_.length(),
                      table_item->table_name_.ptr());
        } else if (OB_FAIL(delete_tables_.push_back(table_item))) {
          LOG_WARN("failed to push back table item", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(table_item, delete_tables_, OB_SUCC(ret)) {
      if (NULL != *table_item) {
        if ((*table_item)->cte_type_ != TableItem::NOT_CTE) {
          ret = OB_ERR_NON_UPDATABLE_TABLE;
          const ObString &table_name = (*table_item)->alias_name_.empty() ? (*table_item)->table_name_ : (*table_item)->alias_name_;
          ObString scope_name = "DELETE";
          LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
                          table_name.length(), table_name.ptr(),
                          scope_name.length(), scope_name.ptr());
          LOG_WARN("table is not updatable", K(ret));
        } else if ((*table_item)->is_generated_table() || (*table_item)->is_temp_table()) {
          if (!delete_stmt->has_instead_of_trigger()
              && OB_FAIL(set_base_table_for_view(**table_item))) {
            LOG_WARN("set base table for delete view failed", K(ret));
          } else if (OB_FAIL(add_all_column_to_updatable_view(*delete_stmt, **table_item,
                            delete_stmt->has_instead_of_trigger()))) {
            LOG_WARN("add all column to updatable view failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDeleteResolver::find_delete_table_with_mysql_rule(const ObString &db_name,
                                                        const ObString &table_name,
                                                        TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  CK(OB_NOT_NULL(stmt));
  CK(OB_NOT_NULL(session_info_));
  ObString final_db_name;
  table_item = NULL;
  ObSEArray<const TableItem*, 2> table_items;
  OZ(stmt->get_all_table_item_by_tname(session_info_, db_name, table_name, table_items));
  if (OB_SUCC(ret)) {
    if (table_items.count() == 1) {
      table_item = const_cast<TableItem*>(table_items.at(0));
      if (!db_name.empty() && !table_item->alias_name_.empty()) {
       table_item = NULL;
      }
    } else if (db_name.empty()) {
      //如果table_items中有多个table_item，说明delete子句没有指定table的database_name
      //而from子句中所有表都是基表，例如delete t1.* from db1.t1, db2.t1;
      //这种情况下，我们认为delete子句中的database_name和session上的保持一致
      ObString final_db_name = session_info_->get_database_name();
      if (final_db_name.empty()) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no database selected");
      }
      for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_item) && i < table_items.count(); ++i) {
        if (ObCharset::case_insensitive_equal(final_db_name, table_items.at(i)->database_name_)
            && ObCharset::case_insensitive_equal(table_name, table_items.at(i)->table_name_)) {
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
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, print_table_name.length(), print_table_name.ptr(),
                     scope_name.length(), scope_name.ptr());
    } else if (!table_item->is_basic_table() && !table_item->is_generated_table() && !table_item->is_temp_table()) {
      ret = OB_ERR_NON_UPDATABLE_TABLE;
      ObString print_table_name = concat_table_name(db_name, table_name);
      ObString scope_name = ObString::make_string("DELETE");
      LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE, print_table_name.length(), print_table_name.ptr(),
                     scope_name.length(), scope_name.ptr());
    }
  }
  return ret;
}

int ObDeleteResolver::generate_delete_table_info(const TableItem &table_item)
{
  int ret = OB_SUCCESS;
  const TableItem &base_table_item = table_item.get_base_table_item();
  void *ptr = NULL;
  const ObTableSchema *table_schema = NULL;
  ObDeleteStmt *delete_stmt = get_delete_stmt();
  ObDeleteTableInfo *table_info = NULL;
  uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
  int64_t gindex_cnt = OB_MAX_INDEX_PER_TABLE;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(params_.session_info_) ||
      OB_ISNULL(allocator_) || OB_ISNULL(delete_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(schema_checker_), K(params_.session_info_),
        K(allocator_), K(delete_stmt), K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                       base_table_item.ref_id_,
                                                       table_schema,
                                                       base_table_item.is_link_table()))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_can_write_index_array(params_.session_info_->get_effective_tenant_id(),
                                                                base_table_item.ref_id_,
                                                                index_tid, gindex_cnt, true))) {
    LOG_WARN("failed to get global index", K(ret));
  } else if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else if (NULL == (ptr = allocator_->alloc(sizeof(ObDeleteTableInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table info", K(ret));
  } else {
    table_info = new(ptr) ObDeleteTableInfo();
    if (OB_FAIL(table_info->part_ids_.assign(base_table_item.part_ids_))) {
      LOG_WARN("failed to assign part ids", K(ret));
    } else if (!delete_stmt->has_instead_of_trigger()) {
      // todo @zimiao error logging also need all columns ?
      if (OB_FAIL(add_all_rowkey_columns_to_stmt(table_item, table_info->column_exprs_))) {
        LOG_WARN("add all rowkey columns to stmt failed", K(ret));
      } else if (need_all_columns(*table_schema, binlog_row_image)) {
        if (OB_FAIL(add_all_columns_to_stmt(table_item, table_info->column_exprs_))) {
          LOG_WARN("fail to add all column to stmt", K(ret), K(table_item));
        }
      } else if (OB_FAIL(add_all_index_rowkey_to_stmt(table_item,
                                                      table_info->column_exprs_))) {
        LOG_WARN("fail to add relate column to stmt", K(ret), K(table_item));
      } else if (OB_FAIL(add_all_lob_columns_to_stmt(table_item, table_info->column_exprs_))) {
        LOG_WARN("fail to add lob column to stmt", K(ret), K(table_item));
      }
      if (OB_SUCC(ret)) {
        table_info->table_id_ = table_item.table_id_;
        table_info->loc_table_id_ = base_table_item.table_id_;
        table_info->ref_table_id_ = base_table_item.ref_id_;
        table_info->table_name_ = table_schema->get_table_name_str();
        table_info->is_link_table_ = base_table_item.is_link_table();
      }
    } else {
      uint64_t view_id = OB_INVALID_ID;
      if (OB_FAIL(add_all_columns_to_stmt_for_trigger(table_item, table_info->column_exprs_))) {
        LOG_WARN("failed to add all columns to stmt", K(ret));
      } else if (OB_FAIL(get_view_id_for_trigger(table_item, view_id))) {
        LOG_WARN("get view id failed", K(table_item), K(ret));
      } else {
        table_info->table_id_ = table_item.table_id_;
        table_info->loc_table_id_ = table_item.table_id_;
        table_info->ref_table_id_ = view_id;
        table_info->table_name_ = table_item.table_name_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(delete_stmt->get_delete_table_info().push_back(table_info))) {
        LOG_WARN("failed to push back table info", K(ret));
      } else if (gindex_cnt > 0) {
        delete_stmt->set_has_global_index(true);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObDeleteResolver::check_view_deletable()
{
  int ret = OB_SUCCESS;
  // uv_check_basic && mysql join already checked in set_base_table_for_view()
  FOREACH_CNT_X(it, delete_tables_, OB_SUCC(ret)) {
    const TableItem *table = *it;
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL table table ", K(ret));
    } else if (!table->is_generated_table() && !table->is_temp_table()) {
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
          LOG_DEBUG("update view check",
              K(has_subquery), K(has_dependent_subquery), K(ref_update_table));
          ret = (has_dependent_subquery || ref_update_table) ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
        }
      }

      if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        ObString str = "DELETE";
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table->get_table_name().length(), table->get_table_name().ptr(),
            str.length(), str.ptr());
      }
    }

    if (OB_SUCC(ret) && lib::is_oracle_mode() && !get_delete_stmt()->has_instead_of_trigger()) {
      if (OB_SUCC(ret)) {
        bool has_distinct = false;
        if (OB_FAIL(ObResolverUtils::uv_check_oracle_distinct(
            *table, *session_info_, *schema_checker_, has_distinct))) {
          LOG_WARN("check updatable view distinct failed", K(ret));
        } else {
          LOG_DEBUG("check has distinct", K(ret), K(has_distinct));
          ret = has_distinct ? OB_ERR_ILLEGAL_VIEW_UPDATE : ret;
        }
      }
      if (OB_SUCC(ret)) {
        // check key preserved table
        bool key_preserved = 0;
        if (OB_FAIL(uv_check_key_preserved(*table, key_preserved))) {
          LOG_WARN("check key preserved failed", K(ret));
        } else {
          LOG_DEBUG("check key preserved", K(key_preserved));
          ret = !key_preserved ? OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED: ret;
        }
      }
    }
  }

  return ret;
}

}  // namespace sql
}
