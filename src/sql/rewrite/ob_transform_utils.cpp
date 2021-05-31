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

#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "common/ob_common_utility.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/rewrite/ob_equal_analysis.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

int ObTransformUtils::is_correlated_expr(const ObRawExpr* expr, int32_t correlated_level, bool& is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < expr->get_expr_levels().bit_count(); ++i) {
      if (!expr->get_expr_levels().has_member(i)) {
        // do nothing, just continue
      } else {
        if (i <= correlated_level) {
          is_correlated = true;
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_direct_correlated_expr(
    const ObRawExpr* expr, int32_t correlated_level, bool& is_direct_correlated)
{
  int ret = OB_SUCCESS;
  is_direct_correlated = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_direct_correlated && i < expr->get_expr_levels().bit_count(); ++i) {
      if (!expr->get_expr_levels().has_member(i)) {
        // do nothing, just continue
      } else if (i == correlated_level) {
        is_direct_correlated = true;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::has_current_level_column(const ObRawExpr* expr, int32_t curlevel, bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(ret));
  } else {
    has = expr->get_expr_levels().has_member(curlevel);
  }
  return ret;
}

int ObTransformUtils::is_column_unique(
    const ObRawExpr* expr, uint64_t table_id, ObSchemaChecker* schema_checker, bool& is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr*, 1> expr_array;
  if (OB_FAIL(expr_array.push_back(const_cast<ObRawExpr*>(expr)))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(is_columns_unique(expr_array, table_id, schema_checker, is_unique))) {
    LOG_WARN("failed to check is columns unique", K(ret));
  }
  return ret;
}

int ObTransformUtils::is_columns_unique(
    const ObIArray<ObRawExpr*>& exprs, uint64_t table_id, ObSchemaChecker* schema_checker, bool& is_unique)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  is_unique = false;
  if (OB_ISNULL(schema_checker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema checker is null", K(ret));
  } else if (OB_FAIL(schema_checker->get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_id));
  } else {
    if (table_schema->get_rowkey_column_num() > 0 &&
        OB_FAIL(exprs_has_unique_subset(exprs, table_schema->get_rowkey_info(), is_unique))) {
      LOG_WARN("failed to check rowkey", K(ret));
    } else if (!is_unique) {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos, false))) {
        LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < simple_index_infos.count(); ++i) {
        const ObTableSchema* index_schema = NULL;
        if (OB_FAIL(schema_checker->get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
          LOG_WARN("failed to get table schema", K(ret), "index_id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_schema)) {
          LOG_WARN("index schema should not be null", K(ret));
        } else if (index_schema->is_unique_index() && index_schema->get_index_column_num() > 0) {
          const ObIndexInfo& index_info = index_schema->get_index_info();
          if (OB_FAIL(exprs_has_unique_subset(exprs, index_info, is_unique))) {
            LOG_WARN("failed to check subset", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::exprs_has_unique_subset(
    const common::ObIArray<ObRawExpr*>& full, const common::ObRowkeyInfo& sub, bool& is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = true;
  // be careful about that sub maybe empty set
  for (int64_t i = 0; OB_SUCC(ret) && i < sub.get_size(); i++) {
    uint64_t column_id_sub = OB_INVALID_ID;
    if (OB_FAIL(sub.get_column_id(i, column_id_sub))) {
      LOG_WARN("failed to get column id", K(ret));
    } else if (OB_INVALID_ID != column_id_sub) {
      bool is_find = false;
      for (int64_t j = 0; !is_find && OB_SUCC(ret) && j < full.count(); j++) {
        if (OB_ISNULL(full.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL expr", K(ret));
        } else if (full.at(j)->has_flag(IS_COLUMN) &&
                   static_cast<ObColumnRefRawExpr*>(full.at(j))->get_column_id() == column_id_sub) {
          is_find = true;
        }
      }
      if (OB_SUCC(ret) && !is_find) {
        is_subset = false;
      }
    }
  }
  return ret;
}

int ObTransformUtils::add_new_table_item(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSelectStmt* subquery, TableItem*& new_table_item)
{
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) ||
      OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add new table_item because some value is null", K(ret));
  } else if (OB_ISNULL(table_item = stmt->create_table_item(*ctx->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else if (OB_FAIL(stmt->generate_view_name(*ctx->allocator_, table_item->table_name_))) {
    LOG_WARN("failed to generate view name", K(ret));
  } else {
    table_item->table_id_ = stmt->get_query_ctx()->available_tb_id_--;
    table_item->type_ = TableItem::GENERATED_TABLE;
    table_item->ref_id_ = OB_INVALID_ID;
    table_item->database_name_ = ObString::make_string("");
    table_item->alias_name_ = table_item->table_name_;
    table_item->ref_query_ = subquery;
    if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
      LOG_WARN("fail to add table_id to hash table", K(ret), K(table_item));
    } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
      LOG_WARN("add table item failed", K(ret));
    } else {
      new_table_item = table_item;
    }
  }
  return ret;
}

int ObTransformUtils::add_new_joined_table(ObTransformerCtx* ctx, ObDMLStmt& stmt, const ObJoinType join_type,
    TableItem* left_table, TableItem* right_table, const ObIArray<ObRawExpr*>& joined_conds, TableItem*& new_join_table,
    bool add_table /* = true */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) || OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret), K(ctx), K(stmt), K(left_table), K(right_table));
  } else {
    JoinedTable* joined_table = static_cast<JoinedTable*>(ctx->allocator_->alloc(sizeof(JoinedTable)));
    joined_table = new (joined_table) JoinedTable();
    joined_table->type_ = TableItem::JOINED_TABLE;
    joined_table->table_id_ = stmt.get_query_ctx()->available_tb_id_--;
    joined_table->joined_type_ = join_type;
    joined_table->left_table_ = left_table;
    joined_table->right_table_ = right_table;
    if (OB_FAIL(joined_table->join_conditions_.assign(joined_conds))) {
      LOG_WARN("failed to push back join conditions", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*joined_table, *left_table))) {
      LOG_WARN("failed to add left table ids", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*joined_table, *right_table))) {
      LOG_WARN("failed to add right table ids", K(ret));
    } else if (add_table && OB_FAIL(stmt.add_joined_table(joined_table))) {
      LOG_WARN("failed to add joined table into stmt", K(ret));
    } else {
      new_join_table = joined_table;
    }
  }
  return ret;
}

int ObTransformUtils::create_new_column_expr(ObTransformerCtx* ctx, const TableItem& table_item,
    const int64_t column_id, const SelectItem& select_item, ObDMLStmt* stmt, ObColumnRefRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* new_column_ref = NULL;
  bool is_nullable = false;
  uint64_t base_table_id = OB_INVALID_ID;
  uint64_t base_column_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_REF_COLUMN, new_column_ref))) {
    LOG_WARN("failed to create a new column ref expr", K(ret));
  } else if (OB_ISNULL(new_column_ref)) {
    LOG_WARN("new_column_ref should not be null", K(ret));
  } else if (OB_FAIL(check_stmt_output_nullable(table_item.ref_query_, select_item.expr_, is_nullable))) {
    LOG_WARN("failed to check stmt output nullable", K(ret));
  } else {
    ObRawExpr* select_expr = select_item.expr_;
    new_column_ref->set_table_name(table_item.alias_name_);
    new_column_ref->set_column_name(select_item.alias_name_);
    new_column_ref->set_ref_id(table_item.table_id_, column_id);  // only one column
    new_column_ref->add_relation_id(stmt->get_table_bit_index(table_item.table_id_));
    new_column_ref->set_collation_type(select_expr->get_collation_type());
    new_column_ref->set_collation_level(select_expr->get_collation_level());
    new_column_ref->set_result_type(select_expr->get_result_type());
    new_column_ref->set_expr_level(stmt->get_current_level());
    if (is_nullable) {
      new_column_ref->unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    } else {
      new_column_ref->set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    }
    if (select_expr->is_column_ref_expr()) {
      const ObColumnRefRawExpr* old_col = static_cast<const ObColumnRefRawExpr*>(select_expr);
      const ColumnItem* old_col_item = NULL;
      if (OB_ISNULL(table_item.ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is invalid", K(ret));
      } else if (OB_ISNULL(old_col_item = table_item.ref_query_->get_column_item_by_id(
                               old_col->get_table_id(), old_col->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column item", K(ret));
      } else {
        base_table_id = old_col_item->base_tid_;
        base_column_id = old_col_item->base_cid_;
      }
    }
    if (OB_SUCC(ret)) {
      ColumnItem column_item;
      column_item.column_name_ = select_item.alias_name_;
      column_item.expr_ = new_column_ref;
      column_item.table_id_ = table_item.table_id_;
      column_item.column_id_ = column_id;
      column_item.base_tid_ = base_table_id;
      column_item.base_cid_ = base_column_id;
      if (OB_FAIL(stmt->add_column_item(column_item))) {
        LOG_WARN("failed to add column item", K(column_item), K(ret));
      } else if (OB_FAIL(new_column_ref->get_expr_levels().add_member(stmt->get_current_level()))) {
        LOG_WARN("add expr levels failed", K(ret));
      } else if (OB_FAIL(new_column_ref->formalize(ctx->session_info_))) {
        LOG_WARN("failed to formalize a new expr", K(ret));
      } else {
        new_expr = new_column_ref;
      }
    }
  }
  return ret;
}

int ObTransformUtils::merge_from_items_as_inner_join(ObTransformerCtx* ctx_, ObDMLStmt& stmt, TableItem*& ret_table)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> dummy_conds;
  ObRelIds rel_ids;
  ret_table = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    const FromItem& item = stmt.get_from_item(i);
    TableItem* table_item = NULL;
    rel_ids.reuse();
    if (item.is_joined_) {
      JoinedTable* joined_table = stmt.get_joined_table(item.table_id_);
      table_item = joined_table;
      if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_joined_tables(), joined_table))) {
        LOG_WARN("failed to remove joined table", K(ret));
      }
    } else {
      table_item = stmt.get_table_item_by_id(item.table_id_);
    }
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_ISNULL(ret_table)) {
      ret_table = table_item;
    } else if (OB_FAIL(add_new_joined_table(
                   ctx_, stmt, INNER_JOIN, ret_table, table_item, dummy_conds, ret_table, false /* add_table */))) {
      LOG_WARN("failed to add new joined table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ret_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no associated table item is found", K(ret));
    } else {
      stmt.get_from_items().reset();
    }
  }
  return ret;
}

int ObTransformUtils::create_columns_for_view(
    ObTransformerCtx* ctx, TableItem& view_table_item, ObDMLStmt* stmt, ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  uint64_t candi_column_id = OB_APP_MIN_COLUMN_ID;
  ObSelectStmt* subquery = NULL;
  if (OB_UNLIKELY(!view_table_item.is_generated_table() && !view_table_item.is_temp_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(view_table_item.type_), K(ret));
  } else if (OB_ISNULL(subquery = view_table_item.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
    if (OB_FAIL(stmt->get_column_exprs(view_table_item.table_id_, table_columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
        const SelectItem& select_item = subquery->get_select_item(i);
        ObColumnRefRawExpr* output_column = NULL;
        bool is_have = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_have && j < table_columns.count(); ++j) {
          output_column = table_columns.at(j);
          if (OB_ISNULL(output_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected table column null ptr", K(output_column), K(ret));
          } else if (output_column->get_column_id() == candi_column_id) {
            is_have = true;
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          if (is_have) {
            if (OB_FAIL(column_exprs.push_back(output_column))) {
              LOG_WARN("failed to push back column expr", K(ret));
            } else {
              candi_column_id++;
            }
          } else if (OB_FAIL(ObTransformUtils::create_new_column_expr(
                         ctx, view_table_item, candi_column_id++, select_item, stmt, output_column))) {
            LOG_WARN("failed to create column expr for view", K(ret));
          } else if (OB_FAIL(column_exprs.push_back(output_column))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::create_columns_for_view
 * add new select exprs into a view stmt, and
 * create columns output for the generated table
 * @return
 */
int ObTransformUtils::create_columns_for_view(ObTransformerCtx* ctx, TableItem& view_table_item, ObDMLStmt* stmt,
    ObIArray<ObRawExpr*>& new_select_list, ObIArray<ObRawExpr*>& new_column_list)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* view_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(view_stmt = view_table_item.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx), K(view_table_item.ref_query_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_select_list.count(); ++i) {
    ObRawExpr* expr = NULL;
    ObColumnRefRawExpr* col = NULL;
    int64_t idx = -1;
    if (OB_ISNULL(expr = new_select_list.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    }
    for (idx = 0;
         OB_SUCC(ret) && idx < view_stmt->get_select_item_size() && expr != view_stmt->get_select_item(idx).expr_;
         ++idx)
      ;

    if (OB_SUCC(ret)) {
      uint64_t column_id = OB_APP_MIN_COLUMN_ID + idx;
      if (idx >= 0 && idx < view_stmt->get_select_item_size()) {
        if (OB_NOT_NULL(col = stmt->get_column_expr_by_id(view_table_item.table_id_, column_id))) {
          // do nothing
        } else if (OB_FAIL(create_new_column_expr(
                       ctx, view_table_item, column_id, view_stmt->get_select_item(idx), stmt, col))) {
          LOG_WARN("failed to create new column expr", K(ret));
        }
      } else {
        if (OB_FAIL(create_select_item(*ctx->allocator_, expr, view_stmt))) {
          LOG_WARN("failed to create select item", K(ret));
        } else if (OB_FAIL(create_new_column_expr(
                       ctx, view_table_item, column_id, view_stmt->get_select_item(idx), stmt, col))) {
          LOG_WARN("failed to create new column expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(new_column_list.push_back(col))) {
        LOG_WARN("failed to push back column expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::create_select_item(
    ObIAllocator& allocator, ObIArray<ObRawExpr*>& select_exprs, ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(allocator, select_exprs.at(i), select_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::create_select_item(
    ObIAllocator& allocator, ObIArray<ColumnItem>& column_items, ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); i++) {
    if (OB_ISNULL(column_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(allocator, column_items.at(i).expr_, select_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::create_select_item(ObIAllocator& allocator, ObRawExpr* select_expr, ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(select_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or expr is null", K(ret), K(select_stmt), K(select_expr));
  } else {
    ObString alias_name = select_expr->get_expr_name();
    char name_buf[64];
    int64_t pos = 0;
    if (!alias_name.empty()) {
      // do nothing
    } else if (OB_FAIL(select_expr->get_name(name_buf, 64, pos))) {
      ret = OB_SUCCESS;
      pos = sprintf(name_buf, "SEL_%ld", select_stmt->get_select_item_size() + 1);
      pos = (pos < 0 || pos >= 64) ? 0 : pos;
    }
    alias_name.assign(name_buf, static_cast<int32_t>(pos));
    if (OB_FAIL(ob_write_string(allocator, alias_name, alias_name))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      SelectItem select_item;
      select_item.expr_ = select_expr;
      select_item.expr_name_ = select_expr->get_expr_name();
      select_item.alias_name_ = alias_name;
      if (OB_FAIL(select_stmt->add_select_item(select_item))) {
        LOG_WARN("failed to add select item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::copy_stmt(ObStmtFactory& stmt_factory, const ObDMLStmt* stmt, ObDMLStmt*& dml_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* select_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(select_stmt))) {
      LOG_WARN("failed to create select stmt", K(ret));
    } else if (OB_FAIL(select_stmt->assign(static_cast<const ObSelectStmt&>(*stmt)))) {
      LOG_WARN("failed to assign select stmt", K(ret));
    } else {
      dml_stmt = select_stmt;
    }
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt* update_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(update_stmt))) {
      LOG_WARN("failed to create update stmt", K(ret));
    } else if (OB_FAIL(update_stmt->assign(static_cast<const ObUpdateStmt&>(*stmt)))) {
      LOG_WARN("failed to assign update stmt", K(ret));
    } else {
      dml_stmt = update_stmt;
    }
  } else if (stmt->is_delete_stmt()) {
    ObDeleteStmt* delete_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(delete_stmt))) {
      LOG_WARN("failed to create delete stmt", K(ret));
    } else if (OB_FAIL(delete_stmt->assign(static_cast<const ObDeleteStmt&>(*stmt)))) {
      LOG_WARN("failed to assign delete stmt", K(ret));
    } else {
      dml_stmt = delete_stmt;
    }
  } else if (stmt->is_insert_stmt()) {
    ObInsertStmt* insert_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(insert_stmt))) {
      LOG_WARN("failed to create insert stmt", K(ret));
    } else if (OB_FAIL(insert_stmt->assign(static_cast<const ObInsertStmt&>(*stmt)))) {
      LOG_WARN("failed to assign insert stmt", K(ret));
    } else {
      dml_stmt = insert_stmt;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObTransformUtils::deep_copy_stmt(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt* stmt, ObDMLStmt*& dml_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* select_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(select_stmt))) {
      LOG_WARN("failed to create select stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(select_stmt->deep_copy(stmt_factory, expr_factory, *stmt)))) {
      LOG_WARN("failed to deep copy select stmt", K(ret));
    } else {
      dml_stmt = select_stmt;
    }
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt* update_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(update_stmt))) {
      LOG_WARN("failed to create update stmt", K(ret));
    } else if (OB_FAIL(update_stmt->deep_copy(stmt_factory, expr_factory, *stmt))) {
      LOG_WARN("failed to deep copy update stmt", K(ret));
    } else {
      dml_stmt = update_stmt;
    }
  } else if (stmt->is_delete_stmt()) {
    ObDeleteStmt* delete_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(delete_stmt))) {
      LOG_WARN("failed to create delete stmt", K(ret));
    } else if (OB_FAIL(delete_stmt->deep_copy(stmt_factory, expr_factory, *stmt))) {
      LOG_WARN("failed to deep copy delete stmt", K(ret));
    } else {
      dml_stmt = delete_stmt;
    }
  } else if (stmt->is_insert_stmt()) {
    ObInsertStmt* insert_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(insert_stmt))) {
      LOG_WARN("failed to create insert stmt", K(ret));
    } else {
      insert_stmt->set_replace(static_cast<const ObInsertStmt*>(stmt)->is_replace());
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(insert_stmt->deep_copy(stmt_factory, expr_factory, *stmt))) {
      LOG_WARN("failed to deep copy insert stmt", K(ret));
    } else {
      dml_stmt = insert_stmt;
    }
  } else if (stmt->is_merge_stmt()) {
    ObMergeStmt* merge_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(merge_stmt))) {
      LOG_WARN("failed to create insert stmt", K(ret));
    } else if (OB_FAIL(merge_stmt->deep_copy(stmt_factory, expr_factory, *stmt))) {
      LOG_WARN("failed to deep copy insert stmt", K(ret));
    } else {
      dml_stmt = merge_stmt;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("stmt type is not valid for deep copy", K(ret), K(stmt->get_stmt_type()));
  }
  return ret;
}

int ObTransformUtils::add_joined_table_single_table_ids(JoinedTable& joined_table, TableItem& child_table)
{
  int ret = OB_SUCCESS;
  if (child_table.is_joined_table()) {
    JoinedTable& child_joined_table = static_cast<JoinedTable&>(child_table);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_joined_table.single_table_ids_.count(); ++i) {
      if (OB_FAIL(joined_table.single_table_ids_.push_back(child_joined_table.single_table_ids_.at(i)))) {
        LOG_WARN("push back single table id failed", K(ret));
      }
    }
  } else if (OB_FAIL(joined_table.single_table_ids_.push_back(child_table.table_id_))) {
    LOG_WARN("push back single table id failed", K(ret));
  }
  return ret;
}

int ObTransformUtils::deep_copy_order_items(ObRawExprFactory& expr_factory, const ObIArray<OrderItem>& input_items,
    ObIArray<OrderItem>& output_items, const uint64_t copy_types, bool use_new_allocator)
{
  int ret = OB_SUCCESS;
  OrderItem order_item;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_items.count(); i++) {
    order_item.reset();
    if (OB_FAIL(order_item.deep_copy(expr_factory, input_items.at(i), copy_types, use_new_allocator))) {
      LOG_WARN("failed to deep copy order item", K(ret));
    } else if (OB_FAIL(output_items.push_back(order_item))) {
      LOG_WARN("failed to push back order item", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_semi_infos(ObRawExprFactory& expr_factory, const ObIArray<SemiInfo*>& input_semi_infos,
    ObIArray<SemiInfo*>& output_semi_infos, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_semi_infos.count(); i++) {
    if (OB_ISNULL(input_semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null semi info", K(ret));
    } else {
      SemiInfo* temp_semi_info = NULL;
      void* ptr = expr_factory.get_allocator().alloc(sizeof(SemiInfo));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate semi info", K(ret));
      } else {
        temp_semi_info = new (ptr) SemiInfo();
        if (OB_FAIL(temp_semi_info->deep_copy(expr_factory, *input_semi_infos.at(i), copy_types))) {
          LOG_WARN("failed to deep copy semi info", K(ret));
        } else if (OB_FAIL(output_semi_infos.push_back(temp_semi_info))) {
          LOG_WARN("failed to push back semi info", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_column_items(ObRawExprFactory& expr_factory,
    const ObIArray<ColumnItem>& input_column_items, ObIArray<ColumnItem>& output_column_items)
{
  int ret = OB_SUCCESS;
  ColumnItem column_item;
  ObRawExpr* copy_expr = NULL;
  for (int i = 0; OB_SUCC(ret) && i < input_column_items.count(); i++) {
    column_item.reset();
    if (OB_FAIL(column_item.deep_copy(expr_factory, input_column_items.at(i)))) {
      LOG_WARN("failed to deep copy column item", K(ret));
    } else if (OB_FAIL(output_column_items.push_back(column_item))) {
      LOG_WARN("failed to push back column item", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_related_part_expr_arrays(ObRawExprFactory& expr_factory,
    const ObIArray<ObDMLStmt::PartExprArray>& input_related_part_expr_arrays,
    ObIArray<ObDMLStmt::PartExprArray>& output_related_part_expr_arrays, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  ObDMLStmt::PartExprArray tmp_ralated_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_related_part_expr_arrays.count(); ++i) {
    tmp_ralated_array.reset();
    if (OB_FAIL(tmp_ralated_array.deep_copy(expr_factory, input_related_part_expr_arrays.at(i), copy_types))) {
      LOG_WARN("failed to deep copy part expr array", K(ret));
    } else if (OB_FAIL(output_related_part_expr_arrays.push_back(tmp_ralated_array))) {
      LOG_WARN("failed to push back part expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_part_expr_items(ObRawExprFactory& expr_factory,
    const ObIArray<ObDMLStmt::PartExprItem>& input_part_expr_items,
    ObIArray<ObDMLStmt::PartExprItem>& output_part_expr_items, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  ObDMLStmt::PartExprItem temp_part_expr;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_part_expr_items.count(); i++) {
    temp_part_expr.reset();
    if (OB_FAIL(temp_part_expr.deep_copy(expr_factory, input_part_expr_items.at(i), copy_types))) {
      LOG_WARN("failed to deep copy part expr", K(ret));
    } else if (OB_FAIL(output_part_expr_items.push_back(temp_part_expr))) {
      LOG_WARN("failed to push back part expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_table_items(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory,
    const ObIArray<TableItem*>& input_table_items, ObIArray<TableItem*>& output_table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_table_items.count(); i++) {
    if (OB_ISNULL(input_table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret));
    } else {
      TableItem* temp_table_item = NULL;
      void* ptr = expr_factory.get_allocator().alloc(sizeof(TableItem));
      if (NULL == ptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate table item", K(ret));
      } else {
        temp_table_item = new (ptr) TableItem();
        if (OB_FAIL(temp_table_item->deep_copy(stmt_factory, expr_factory, *input_table_items.at(i)))) {
          LOG_WARN("failed to deep copy table item", K(ret));
        } else if (OB_FAIL(output_table_items.push_back(temp_table_item))) {
          LOG_WARN("failed to push back table item", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_join_tables(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory,
    const ObIArray<JoinedTable*>& input_joined_tables, ObIArray<JoinedTable*>& output_joined_tables,
    const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_joined_tables.count(); i++) {
    JoinedTable* temp_joined_table = NULL;
    if (OB_ISNULL(input_joined_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null joined table", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_join_table(
                   stmt_factory, expr_factory, *input_joined_tables.at(i), temp_joined_table, copy_types))) {
      LOG_WARN("failed to deep copy joined table", K(ret));
    } else if (OB_FAIL(output_joined_tables.push_back(temp_joined_table))) {
      LOG_WARN("failed to push back joined table", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_join_table(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory,
    const JoinedTable& input_joined_table, JoinedTable*& output_joined_table, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  output_joined_table = NULL;
  JoinedTable* temp_joined_table = NULL;
  void* ptr = stmt_factory.get_allocator().alloc(sizeof(JoinedTable));
  if (NULL == ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate joined table", K(ret));
  } else {
    temp_joined_table = new (ptr) JoinedTable();
    if (OB_FAIL(temp_joined_table->deep_copy(stmt_factory, expr_factory, input_joined_table, copy_types))) {
      LOG_WARN("failed to deep copy joined table", K(ret));
    } else {
      output_joined_table = temp_joined_table;
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_select_items(ObRawExprFactory& expr_factory,
    const ObIArray<SelectItem>& input_select_items, ObIArray<SelectItem>& output_select_items,
    const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  SelectItem temp_select_item;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_select_items.count(); i++) {
    temp_select_item.reset();
    if (OB_FAIL(temp_select_item.deep_copy(expr_factory, input_select_items.at(i), copy_types))) {
      LOG_WARN("failed to deep copy select item", K(ret));
    } else if (OB_FAIL(output_select_items.push_back(temp_select_item))) {
      LOG_WARN("failed to push back select item", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}
int ObTransformUtils::replace_equal_expr(ObRawExpr* old_expr, ObRawExpr* new_expr, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> old_exprs;
  ObSEArray<ObRawExpr*, 1> new_exprs;
  if (OB_ISNULL(old_expr) || OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(old_exprs.push_back(old_expr)) || OB_FAIL(new_exprs.push_back(new_expr))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_equal_expr(old_exprs, new_exprs, expr))) {
    LOG_WARN("failed to replace general expr", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObTransformUtils::replace_equal_expr(const common::ObIArray<ObRawExpr*>& other_exprs,
    const common::ObIArray<ObRawExpr*>& current_exprs, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  int64_t pos = OB_INVALID_ID;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(other_exprs.count() != current_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(other_exprs.count()), K(current_exprs.count()), K(ret));
  } else if (ObOptimizerUtil::find_equal_expr(other_exprs, expr, pos)) {
    if (OB_UNLIKELY(pos < 0 || pos >= current_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected array pos", K(pos), K(current_exprs.count()), K(ret));
    } else {
      expr = current_exprs.at(pos);
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(ObTransformUtils::replace_equal_expr(other_exprs, current_exprs, expr->get_param_expr(i)))) {
        LOG_WARN("failed to replace general expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_expr(ObRawExpr* old_expr, ObRawExpr* new_expr, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> old_exprs;
  ObSEArray<ObRawExpr*, 1> new_exprs;
  if (OB_FAIL(old_exprs.push_back(old_expr)) || OB_FAIL(new_exprs.push_back(new_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(replace_expr(old_exprs, new_exprs, expr))) {
    LOG_WARN("failed to replace expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::replace_expr(ObRawExpr* old_expr, ObRawExpr* new_expr, ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> old_exprs;
  ObSEArray<ObRawExpr*, 1> new_exprs;
  if (OB_ISNULL(old_expr) || OB_ISNULL(new_expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (OB_FAIL(old_exprs.push_back(old_expr)) || OB_FAIL(new_exprs.push_back(new_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::replace_expr(
    const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr)) {
    ObRawExpr* temp_expr = NULL;
    int64_t idx = -1;
    if (!ObOptimizerUtil::find_item(other_exprs, expr, &idx)) {
      /*do nothing*/
    } else if (OB_UNLIKELY(idx < 0 || idx >= new_exprs.count()) || OB_ISNULL(new_exprs.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), K(new_exprs.count()), K(new_exprs));
    } else {
      expr = new_exprs.at(idx);
      temp_expr = other_exprs.at(idx);
      const_cast<ObIArray<ObRawExpr*>&>(other_exprs).at(idx) = NULL;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr->replace_expr(other_exprs, new_exprs))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else if (NULL != temp_expr) {
        const_cast<ObIArray<ObRawExpr*>&>(other_exprs).at(idx) = temp_expr;
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_expr_for_order_item(
    const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs, ObIArray<OrderItem>& order_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    if (OB_ISNULL(order_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, order_items.at(i).expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_from_item(const ObIArray<FromItem>& other_from_items,
    const uint64_t old_table_id, const uint64_t new_table_id, common::ObIArray<FromItem>& from_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_from_items.count() != from_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal from items count", K(from_items.count()), K(other_from_items.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_from_items.count(); i++) {
    if (other_from_items.at(i).table_id_ == old_table_id) {
      from_items.at(i).table_id_ = new_table_id;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_joined_tables(const ObIArray<JoinedTable*>& other_joined_tables,
    const uint64_t old_table_id, const uint64_t new_table_id, ObIArray<JoinedTable*>& joined_tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_joined_tables.count() != joined_tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal joined tables count", K(other_joined_tables.count()), K(joined_tables.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < other_joined_tables.count(); i++) {
      if (OB_ISNULL(other_joined_tables.at(i)) || OB_ISNULL(joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null joined table", K(other_joined_tables.at(i)), K(joined_tables.at(i)), K(ret));
      } else if (OB_FAIL((update_table_id_for_joined_table(
                     *other_joined_tables.at(i), old_table_id, new_table_id, *joined_tables.at(i))))) {
        LOG_WARN("failed to update table id for joined table", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_joined_table(
    const JoinedTable& other, const uint64_t old_table_id, const uint64_t new_table_id, JoinedTable& current)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.left_table_) || OB_ISNULL(other.right_table_) || OB_ISNULL(current.left_table_) ||
      OB_ISNULL(current.right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null table item",
        K(ret),
        K(other.left_table_),
        K(other.right_table_),
        K(current.left_table_),
        K(current.right_table_));
  } else if (OB_FAIL(update_table_id(other.single_table_ids_, old_table_id, new_table_id, current.single_table_ids_))) {
    LOG_WARN("failed to update table id array", K(ret));
  } else if (other.table_id_ == old_table_id) {
    current.table_id_ = new_table_id;
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret) && other.left_table_->is_joined_table() && current.left_table_->is_joined_table()) {
    if (OB_FAIL(SMART_CALL(update_table_id_for_joined_table(static_cast<JoinedTable&>(*other.left_table_),
            old_table_id,
            new_table_id,
            static_cast<JoinedTable&>(*current.left_table_))))) {
      LOG_WARN("failed to update table if for joined table", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret) && other.right_table_->is_joined_table() && current.right_table_->is_joined_table()) {
    if (OB_FAIL(SMART_CALL(update_table_id_for_joined_table(static_cast<JoinedTable&>(*other.right_table_),
            old_table_id,
            new_table_id,
            static_cast<JoinedTable&>(*current.right_table_))))) {
      LOG_WARN("failed to update table if for joined table", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_part_item(const ObIArray<ObDMLStmt::PartExprItem>& other_part_expr_items,
    const uint64_t old_table_id, const uint64_t new_table_id, ObIArray<ObDMLStmt::PartExprItem>& part_expr_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_part_expr_items.count() != part_expr_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal part expr items count",
        K(other_part_expr_items.count()),
        K(part_expr_items.count()),
        K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_part_expr_items.count(); i++) {
    if (other_part_expr_items.at(i).table_id_ == old_table_id) {
      part_expr_items.at(i).table_id_ = new_table_id;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_part_array(const ObIArray<ObDMLStmt::PartExprArray>& other_part_expr_arrays,
    const uint64_t old_table_id, const uint64_t new_table_id, ObIArray<ObDMLStmt::PartExprArray>& part_expr_arrays)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_part_expr_arrays.count() != part_expr_arrays.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal part expr arrays count",
        K(other_part_expr_arrays.count()),
        K(part_expr_arrays.count()),
        K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_part_expr_arrays.count(); i++) {
    if (other_part_expr_arrays.at(i).table_id_ == old_table_id) {
      part_expr_arrays.at(i).table_id_ = new_table_id;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_semi_info(const ObIArray<SemiInfo*>& other_semi_infos,
    const uint64_t old_table_id, const uint64_t new_table_id, ObIArray<SemiInfo*>& semi_infos)
{
  int ret = OB_SUCCESS;
  SemiInfo* other = NULL;
  SemiInfo* current = NULL;
  if (OB_UNLIKELY(other_semi_infos.count() != semi_infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal semi infos count", K(other_semi_infos.count()), K(semi_infos.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_semi_infos.count(); i++) {
    if (OB_ISNULL(other = other_semi_infos.at(i)) || OB_ISNULL(current = semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null semi info", K(other), K(current), K(ret));
    } else if (OB_FAIL(update_table_id(other->left_table_ids_, old_table_id, new_table_id, current->left_table_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (other->right_table_id_ == old_table_id) {
      current->right_table_id_ = new_table_id;
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_view_table_id(const common::ObIArray<uint64_t>& other_view_table_id,
    const uint64_t old_table_id, const uint64_t new_table_id, common::ObIArray<uint64_t>& view_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_view_table_id.count() != view_table_id.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal view table id count", K(other_view_table_id.count()), K(view_table_id.count()), K(ret));
  } else if (OB_FAIL(update_table_id(other_view_table_id, old_table_id, new_table_id, view_table_id))) {
    LOG_WARN("failed to update table id array", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_column_item(const ObIArray<ColumnItem>& other_column_items,
    const uint64_t old_table_id, const uint64_t new_table_id, const int32_t old_bit_id, const int32_t new_bit_id,
    ObIArray<ColumnItem>& column_items)
{
  int ret = OB_SUCCESS;
  bool has_bit_index = false;
  if (OB_INVALID_ID != old_bit_id && OB_INVALID_ID != new_bit_id) {
    has_bit_index = true;
  }
  if (OB_UNLIKELY(other_column_items.count() != column_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal column item count", K(other_column_items.count()), K(column_items.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_column_items.count(); i++) {
    if (OB_ISNULL(other_column_items.at(i).expr_) || OB_ISNULL(column_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column expr", K(other_column_items.at(i)), K(column_items.at(i)), K(ret));
    } else {
      if (other_column_items.at(i).table_id_ == old_table_id) {
        column_items.at(i).table_id_ = new_table_id;
      } else { /*do nothing*/
      }
      if (other_column_items.at(i).expr_->get_table_id() == old_table_id) {
        column_items.at(i).expr_->set_table_id(new_table_id);
      } else { /*do nothing*/
      }
      if (has_bit_index && OB_FAIL(update_table_id_index(other_column_items.at(i).expr_->get_relation_ids(),
                               old_bit_id,
                               new_bit_id,
                               column_items.at(i).expr_->get_relation_ids()))) {
        LOG_WARN("failed to add table id index", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_stmt_hint(const ObStmtHint& other_stmt_hint, const uint64_t old_table_id,
    const uint64_t new_table_id, const int32_t old_bit_id, const int32_t new_bit_id, ObStmtHint& stmt_hint)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_stmt_hint.part_hints_.count() != stmt_hint.part_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "should have equal count", K(other_stmt_hint.part_hints_.count()), K(stmt_hint.part_hints_.count()), K(ret));
  } else if (OB_UNLIKELY(other_stmt_hint.indexes_.count() != stmt_hint.indexes_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal count", K(other_stmt_hint.indexes_.count()), K(stmt_hint.indexes_.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_stmt_hint.part_hints_.count(); i++) {
    if (other_stmt_hint.part_hints_.at(i).table_id_ == old_table_id) {
      stmt_hint.part_hints_.at(i).table_id_ = new_table_id;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_stmt_hint.indexes_.count(); i++) {
    if (other_stmt_hint.indexes_.at(i).table_id_ == old_table_id) {
      stmt_hint.indexes_.at(i).table_id_ = new_table_id;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            update_table_id(other_stmt_hint.join_order_ids_, old_table_id, new_table_id, stmt_hint.join_order_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.use_merge_ids_, old_table_id, new_table_id, stmt_hint.use_merge_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.no_use_merge_ids_, old_table_id, new_table_id, stmt_hint.no_use_merge_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.use_hash_ids_, old_table_id, new_table_id, stmt_hint.use_hash_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.no_use_hash_ids_, old_table_id, new_table_id, stmt_hint.no_use_hash_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(
                   update_table_id(other_stmt_hint.use_nl_ids_, old_table_id, new_table_id, stmt_hint.use_nl_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.no_use_nl_ids_, old_table_id, new_table_id, stmt_hint.no_use_nl_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(
                   update_table_id(other_stmt_hint.use_bnl_ids_, old_table_id, new_table_id, stmt_hint.use_bnl_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.no_use_bnl_ids_, old_table_id, new_table_id, stmt_hint.no_use_bnl_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(other_stmt_hint.use_nl_materialization_ids_,
                   old_table_id,
                   new_table_id,
                   stmt_hint.use_nl_materialization_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(other_stmt_hint.no_use_nl_materialization_ids_,
                   old_table_id,
                   new_table_id,
                   stmt_hint.no_use_nl_materialization_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(
                   other_stmt_hint.px_join_filter_ids_, old_table_id, new_table_id, stmt_hint.px_join_filter_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id(other_stmt_hint.no_px_join_filter_ids_,
                   old_table_id,
                   new_table_id,
                   stmt_hint.no_px_join_filter_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_INVALID_ID == old_bit_id || OB_INVALID_ID == new_bit_id) {
      /* do nothing */
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.use_merge_idxs_, old_bit_id, new_bit_id, stmt_hint.use_merge_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.no_use_merge_idxs_, old_bit_id, new_bit_id, stmt_hint.no_use_merge_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.use_hash_idxs_, old_bit_id, new_bit_id, stmt_hint.use_hash_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.no_use_hash_idxs_, old_bit_id, new_bit_id, stmt_hint.no_use_hash_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.use_nl_idxs_, old_bit_id, new_bit_id, stmt_hint.use_nl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.no_use_nl_idxs_, old_bit_id, new_bit_id, stmt_hint.no_use_nl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.use_bnl_idxs_, old_bit_id, new_bit_id, stmt_hint.use_bnl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.no_use_bnl_idxs_, old_bit_id, new_bit_id, stmt_hint.no_use_bnl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.pq_distributes_idxs_, old_bit_id, new_bit_id, stmt_hint.pq_distributes_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.px_join_filter_idxs_, old_bit_id, new_bit_id, stmt_hint.px_join_filter_idxs_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.no_px_join_filter_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.no_px_join_filter_idxs_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_no_px_join_filter_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_no_px_join_filter_idxs_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.valid_use_merge_idxs_, old_bit_id, new_bit_id, stmt_hint.valid_use_merge_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_no_use_merge_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_no_use_merge_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.valid_use_hash_idxs_, old_bit_id, new_bit_id, stmt_hint.valid_use_hash_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_no_use_hash_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_no_use_hash_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.valid_use_nl_idxs_, old_bit_id, new_bit_id, stmt_hint.valid_use_nl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.valid_no_use_nl_idxs_, old_bit_id, new_bit_id, stmt_hint.valid_no_use_nl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.valid_use_bnl_idxs_, old_bit_id, new_bit_id, stmt_hint.valid_use_bnl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(
                   other_stmt_hint.valid_no_use_bnl_idxs_, old_bit_id, new_bit_id, stmt_hint.valid_no_use_bnl_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_use_nl_materialization_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_use_nl_materialization_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_no_use_nl_materialization_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_no_use_nl_materialization_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_pq_distributes_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_pq_distributes_idxs_))) {
      LOG_WARN("failed to update table id index", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_px_join_filter_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_px_join_filter_idxs_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (OB_FAIL(update_table_id_index(other_stmt_hint.valid_no_px_join_filter_idxs_,
                   old_bit_id,
                   new_bit_id,
                   stmt_hint.valid_no_px_join_filter_idxs_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id(const common::ObIArray<ObTablesIndex>& old_ids, const uint64_t old_table_id,
    const uint64_t new_table_id, common::ObIArray<ObTablesIndex>& new_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_ids.count(); i++) {
    ObTablesIndex tables_index;
    if (OB_FAIL(update_table_id(old_ids.at(i).indexes_, old_table_id, new_table_id, tables_index.indexes_))) {
      LOG_WARN("failed to update table id", K(ret));
    } else {
      new_ids.push_back(tables_index);
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id(const ObIArray<uint64_t>& old_ids, const uint64_t old_table_id,
    const uint64_t new_table_id, ObIArray<uint64_t>& new_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> temp_ids;
  if (OB_FAIL(temp_ids.assign(new_ids))) {
    LOG_WARN("failed to assign table id", K(ret));
  } else {
    new_ids.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_ids.count(); i++) {
    if (old_ids.at(i) == old_table_id) {
      if (OB_FAIL(new_ids.push_back(new_table_id))) {
        LOG_WARN("failed to push back id", K(ret));
      } else { /*do nothing*/
      }
    } else {
      if (OB_FAIL(new_ids.push_back(temp_ids.at(i)))) {
        LOG_WARN("failed to push back id", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_index(const common::ObIArray<ObPQDistributeIndex>& old_ids,
    const int32_t old_bit_id, const int32_t new_bit_id, common::ObIArray<ObPQDistributeIndex>& new_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_ids.count(); ++i) {
    ObPQDistributeIndex* distribute_ids = NULL;
    if (OB_ISNULL(distribute_ids = new_ids.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc the memory for distribute hint error.", K(ret));
    } else if (OB_FAIL(
                   update_table_id_index(old_ids.at(i).rel_ids_, old_bit_id, new_bit_id, distribute_ids->rel_ids_))) {
      LOG_WARN("failed to add new id.", K(ret));
    } else {
      *(ObPQDistributeHintMethod*)distribute_ids = old_ids.at(i);
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_index(const common::ObIArray<ObRelIds>& old_ids, const int32_t old_bit_id,
    const int32_t new_bit_id, common::ObIArray<ObRelIds>& new_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_ids.count(); ++i) {
    ObRelIds rel_ids;
    if (OB_FAIL(update_table_id_index(old_ids.at(i), old_bit_id, new_bit_id, rel_ids))) {
      LOG_WARN("failed to add new id.", K(ret));
    } else {
      new_ids.push_back(rel_ids);
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_index(
    const ObRelIds& old_ids, const int32_t old_bit_id, const int32_t new_bit_id, ObRelIds& new_ids)
{
  int ret = OB_SUCCESS;
  if (old_ids.has_member(old_bit_id)) {
    if (OB_FAIL(new_ids.add_member(new_bit_id))) {
      LOG_WARN("failed to add new id", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

bool ObTransformUtils::is_valid_type(ObItemType expr_type)
{
  return T_OP_EQ == expr_type || T_OP_LE == expr_type || T_OP_LT == expr_type || T_OP_GE == expr_type ||
         T_OP_GT == expr_type || T_OP_NE == expr_type;
}

int ObTransformUtils::is_expr_query(const ObSelectStmt* stmt, bool& is_expr_type)
{
  int ret = OB_SUCCESS;
  is_expr_type = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (0 == stmt->get_from_item_size() && 1 == stmt->get_select_item_size() && !stmt->is_contains_assignment() &&
             !stmt->has_subquery() && 0 == stmt->get_aggr_item_size() && 0 == stmt->get_window_func_count() &&
             0 == stmt->get_condition_size() && 0 == stmt->get_having_expr_size() && !stmt->has_limit() &&
             !stmt->is_hierarchical_query() && !stmt->has_set_op()) {
    is_expr_type = true;
  }
  return ret;
}

int ObTransformUtils::is_aggr_query(const ObSelectStmt* stmt, bool& is_aggr_type)
{
  int ret = OB_SUCCESS;
  is_aggr_type = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_set_stmt() && 1 == stmt->get_aggr_item_size() && 1 == stmt->get_select_item_size() &&
             0 == stmt->get_group_expr_size() && 0 == stmt->get_rollup_expr_size() &&
             0 == stmt->get_having_expr_size() && !stmt->has_limit()) {
    bool ref_outer_block_relation = false;
    if (OB_FAIL(is_ref_outer_block_relation(stmt, stmt->get_current_level(), ref_outer_block_relation))) {
      LOG_WARN("failed to check is_ref_outer_block_relation", K(ret));
    } else if (!ref_outer_block_relation) {
      is_aggr_type = true;
    }
  }
  return ret;
}

int ObTransformUtils::is_ref_outer_block_relation(
    const ObSelectStmt* stmt, const int32_t level, bool& ref_outer_block_relation)
{
  int ret = OB_SUCCESS;
  // check where and having condition
  ref_outer_block_relation = false;
  ObSEArray<int64_t, 8> expr_levels;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && !ref_outer_block_relation && i < relation_exprs.count(); ++i) {
    const ObRawExpr* raw_expr = relation_exprs.at(i);
    if (OB_FAIL(raw_expr->get_expr_levels().to_array(expr_levels))) {
      LOG_WARN("expr_level to array failed", K(ret));
    } else {
      for (int64_t j = 0; !ref_outer_block_relation && j < expr_levels.count(); ++j) {
        ref_outer_block_relation = (level > expr_levels[j]);
      }
    }
  }

  // check child stmts
  if (OB_SUCC(ret) && !ref_outer_block_relation) {
    ObSEArray<ObSelectStmt*, 8> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !ref_outer_block_relation && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_ref_outer_block_relation(child_stmts.at(i), level, ref_outer_block_relation)))) {
        LOG_WARN("failed to check child stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::add_is_not_null(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, ObRawExpr* child_expr, ObOpRawExpr*& is_not_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* null_expr = NULL;
  ObConstRawExpr* flag_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  is_not_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(ctx));
  } else if (OB_ISNULL(ctx->session_info_) || OB_ISNULL(expr_factory = ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx fields have null", K(ret), K(ctx->session_info_), K(expr_factory));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_OP_IS_NOT, is_not_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_NULL, null_expr))) {
    LOG_WARN("failed to create const null expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_BOOL, flag_expr))) {
    LOG_WARN("failed to create flag bool expr", K(ret));
  } else if (OB_ISNULL(is_not_expr) || OB_ISNULL(null_expr) || OB_ISNULL(flag_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create not null expr", K(ret));
  } else {
    ObObjParam null_val;
    null_val.set_null();
    null_val.set_param_meta();
    null_expr->set_param(null_val);
    null_expr->set_value(null_val);
    ObObjParam flag_val;
    flag_val.set_bool(false);
    flag_val.set_param_meta();
    flag_expr->set_param(flag_val);
    flag_expr->set_value(flag_val);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_not_expr->set_param_exprs(child_expr, null_expr, flag_expr))) {
    LOG_WARN("failed to set param for not_null op", K(ret), K(*is_not_expr));
  } else if (OB_FAIL(is_not_expr->formalize(ctx->session_info_))) {
    LOG_WARN("failed to formalize a new expr", K(ret));
  } else if (OB_FAIL(is_not_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
    LOG_WARN("pull expr relation ids failed", K(ret));
  }
  return ret;
}

int ObTransformUtils::is_column_nullable(
    const ObDMLStmt* stmt, ObSchemaChecker* schema_checker, const ObColumnRefRawExpr* col_expr, bool& is_nullable)
{
  int ret = OB_SUCCESS;
  const TableItem* table_item = NULL;
  is_nullable = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(schema_checker), K(col_expr));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table item", K(ret), K(col_expr->get_table_id()));
  } else if (table_item->is_basic_table()) {
    const ObColumnSchemaV2* col_schema = NULL;
    if (OB_FAIL(schema_checker->get_column_schema(table_item->ref_id_, col_expr->get_column_id(), col_schema, true))) {
      LOG_WARN("failed to get_column_schema", K(ret));
    } else {
      is_nullable = col_schema->is_nullable();
    }
  }
  return ret;
}

int ObTransformUtils::flatten_expr(common::ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_exprs;
  if (OB_FAIL(temp_exprs.assign(exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    exprs.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_exprs.count(); i++) {
      if (OB_ISNULL(temp_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(flatten_expr(temp_exprs.at(i), exprs))) {
        LOG_WARN("failed to flatten exprs", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::flatten_expr(ObRawExpr* expr, common::ObIArray<ObRawExpr*>& flattened_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(flatten_expr(expr->get_param_expr(i), flattened_exprs)))) {
        LOG_WARN("failed to flatten expr", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    if (OB_FAIL(flattened_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::check_stmt_output_nullable(const ObSelectStmt* stmt, const ObRawExpr* expr, bool& is_nullable)
{
  int ret = OB_SUCCESS;
  is_nullable = true;
  bool is_not_null = false;
  ObSEArray<ObRawExpr*, 4> conds;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (expr->get_expr_type() == T_FUN_COUNT) {
    is_nullable = false;
  } else if (stmt->is_single_set_query()) {
    // do nothing
  } else if (OB_FAIL(check_is_not_null_column(stmt, expr, is_not_null))) {
    LOG_WARN("failed to check column not null", K(ret));
  } else if (is_not_null) {
    is_nullable = false;
  } else if (expr->has_flag(CNT_AGG)) {
    // does not use where condition
  } else if (OB_FAIL(conds.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign conds", K(ret));
  }
  if (OB_SUCC(ret) && is_nullable) {
    if (OB_FAIL(append(conds, stmt->get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    } else if (OB_FAIL(has_null_reject_condition(conds, expr, is_not_null))) {
      LOG_WARN("failed to check has null reject condition", K(ret));
    } else if (is_not_null) {
      is_nullable = false;
    }
  }
  return ret;
}

int ObTransformUtils::find_not_null_expr(ObDMLStmt& stmt, ObRawExpr*& not_null_expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObRelIds from_tables;
  if (OB_FAIL(get_from_tables(stmt, from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
    ObColumnRefRawExpr* col = NULL;
    bool is_nullable = false;
    if (OB_ISNULL(col = stmt.get_column_item(i)->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (!from_tables.is_superset(col->get_relation_ids())) {
      // do nothing
    } else if (OB_FAIL(check_expr_nullable(&stmt, col, is_nullable))) {
      LOG_WARN("failed to check expr nullable", K(ret));
    } else if (!is_nullable) {
      not_null_expr = col;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    is_valid = (not_null_expr != NULL);
  }
  return ret;
}

// 1. check is there null rejecting condition
// 2. check is nullable column, and is not the right table of outer join
int ObTransformUtils::check_expr_nullable(ObDMLStmt* stmt, ObRawExpr* expr, bool& is_nullable)
{
  int ret = OB_SUCCESS;
  is_nullable = true;
  bool has_null_reject = false;
  bool not_null_col = false;
  ObSEArray<ObRawExpr*, 4> valid_conds;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_is_not_null_column(stmt, expr, not_null_col))) {
    LOG_WARN("failed to check is not null column", K(ret));
  } else if (not_null_col) {
    is_nullable = false;
  } else if (OB_FAIL(stmt->get_equal_set_conditions(valid_conds, true, SCOPE_WHERE))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (OB_FAIL(has_null_reject_condition(valid_conds, expr, has_null_reject))) {
    LOG_WARN("failed to check has null reject condition", K(ret));
  } else if (has_null_reject) {
    is_nullable = false;
  }
  return ret;
}

int ObTransformUtils::check_is_not_null_column(const ObDMLStmt* stmt, const ObRawExpr* expr, bool& col_not_null)
{
  int ret = OB_SUCCESS;
  bool is_on_null_side = false;
  const ObColumnRefRawExpr* col_expr = NULL;
  const ColumnItem* col_item = NULL;
  col_not_null = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (!expr->is_column_ref_expr()) {
    // not column expr, do nothing
  } else if (FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr*>(expr))) {
  } else if (OB_ISNULL(col_item = stmt->get_column_item_by_id(col_expr->get_table_id(), col_expr->get_column_id()))) {
    if (col_expr->get_expr_level() == stmt->get_current_level()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col item is not found", K(ret), K(expr));
    }
  } else if (!col_item->is_not_null()) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt, col_expr->get_table_id(), is_on_null_side))) {
    LOG_WARN("failed to check is table on null side", K(ret));
  } else if (!is_on_null_side) {
    col_not_null = true;
  }
  return ret;
}

int ObTransformUtils::find_null_reject_exprs(
    const ObIArray<ObRawExpr*>& conditions, const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& null_reject_exprs)
{
  int ret = OB_SUCCESS;
  bool has_null_reject = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(has_null_reject_condition(conditions, exprs.at(i), has_null_reject))) {
      LOG_WARN("failed to check null reject condition", K(ret));
    } else if (!has_null_reject) {
      // do nothing
    } else if (OB_FAIL(null_reject_exprs.push_back(exprs.at(i)))) {
      LOG_WARN("failed to push back null reject expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::has_null_reject_condition(
    const ObIArray<ObRawExpr*>& conditions, const ObRawExpr* expr, bool& has_null_reject)
{
  int ret = OB_SUCCESS;
  has_null_reject = false;
  bool is_null_reject = false;
  ObSEArray<const ObRawExpr*, 1> dummy_expr_lists;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr));
  } else if (OB_FAIL(dummy_expr_lists.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_null_reject && i < conditions.count(); ++i) {
    if (OB_FAIL(is_null_reject_condition(conditions.at(i), dummy_expr_lists, is_null_reject))) {
      LOG_WARN("failed to check is null reject condition", K(ret));
    } else if (is_null_reject) {
      has_null_reject = true;
    }
  }
  return ret;
}

int ObTransformUtils::is_null_reject_conditions(
    const ObIArray<ObRawExpr*>& conditions, const ObRelIds& target_table, bool& is_null_reject)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  is_null_reject = false;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(conditions, column_exprs))) {
    LOG_WARN("failed to extrace column exprs", K(ret));
  }
  int64_t N = column_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && !is_null_reject && i < N; ++i) {
    ObRawExpr* expr = column_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->get_relation_ids().is_subset(target_table)) {
      // do nothing
    } else if (OB_FAIL(has_null_reject_condition(conditions, expr, is_null_reject))) {
      LOG_WARN("failed to check null reject", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::is_null_reject_condition(
    const ObRawExpr* condition, const ObIArray<const ObRawExpr*>& targets, bool& is_null_reject)
{
  int ret = OB_SUCCESS;
  is_null_reject = false;
  if (OB_ISNULL(condition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(condition));
  } else if (T_OP_AND == condition->get_expr_type()) {
    // and, one of param expr is null reject
    for (int64_t i = 0; OB_SUCC(ret) && !is_null_reject && i < condition->get_param_count(); ++i) {
      if (OB_FAIL(is_null_reject_condition(condition->get_param_expr(i), targets, is_null_reject))) {
        LOG_WARN("failed to check whether param is null reject", K(ret));
      }
    }
  } else if (T_OP_OR == condition->get_expr_type()) {
    // or  all of param exprs are null reject
    is_null_reject = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_null_reject && i < condition->get_param_count(); ++i) {
      if (OB_FAIL(is_null_reject_condition(condition->get_param_expr(i), targets, is_null_reject))) {
        LOG_WARN("failed to check whether param is null reject", K(ret));
      }
    }
  } else if (OB_FAIL(is_simple_null_reject(condition, targets, is_null_reject))) {
    LOG_WARN("failed to check is simple null reject", K(ret));
  }
  if (OB_SUCC(ret) && is_null_reject) {
    LOG_TRACE("find null reject condition", K(*condition));
  }
  return ret;
}

int ObTransformUtils::is_simple_null_reject(
    const ObRawExpr* condition, const ObIArray<const ObRawExpr*>& targets, bool& is_null_reject)
{
  bool ret = OB_SUCCESS;
  is_null_reject = false;
  const ObRawExpr* expr = NULL;
  bool check_expr_is_null_reject = false;
  bool check_expr_is_null_propagate = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(condition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(condition));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_IS_NOT == condition->get_expr_type()) {
    if (OB_ISNULL(condition->get_param_expr(1)) || OB_ISNULL(condition->get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition param is null", K(ret));
    } else if (T_NULL == condition->get_param_expr(1)->get_expr_type() &&
               T_BOOL == condition->get_param_expr(2)->get_expr_type() &&
               !(static_cast<const ObConstRawExpr*>(condition->get_param_expr(2)))->get_value().get_bool()) {
      check_expr_is_null_propagate = true;
      expr = condition->get_param_expr(0);
    }
  } else if (T_OP_IS == condition->get_expr_type()) {
    if (OB_ISNULL(condition->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is not correct", K(ret));
    } else if (T_BOOL == condition->get_param_expr(1)->get_expr_type()) {
      const ObConstRawExpr* b_expr = static_cast<const ObConstRawExpr*>(condition->get_param_expr(1));
      if (b_expr->get_value().is_true()) {
        check_expr_is_null_reject = true;
      } else {
        check_expr_is_null_propagate = true;
      }
      expr = condition->get_param_expr(0);
    }
  } else if (IS_SUBQUERY_COMPARISON_OP(condition->get_expr_type())) {
    if (condition->has_flag(IS_WITH_ANY)) {
      check_expr_is_null_propagate = true;
      expr = condition->get_param_expr(0);
    }
  } else {
    check_expr_is_null_propagate = true;
    expr = condition;
  }
  if (OB_FAIL(ret)) {
  } else if (check_expr_is_null_propagate) {
    if (OB_FAIL(is_null_propagate_expr(expr, targets, is_null_reject))) {
      LOG_WARN("failed to check expr is null propagate", K(ret));
    }
  } else if (check_expr_is_null_reject) {
    if (OB_FAIL(is_null_reject_condition(expr, targets, is_null_reject))) {
      LOG_WARN("failed to check expr is null reject", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::is_null_propagate_expr(const ObRawExpr* expr, const ObIArray<ObRawExpr*>& targets, bool& bret)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 4> tmp;
  if (OB_FAIL(append(tmp, targets))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(is_null_propagate_expr(expr, tmp, bret))) {
    LOG_WARN("failed to check is null propagate expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::is_null_propagate_expr(
    const ObRawExpr* expr, const ObIArray<const ObRawExpr*>& targets, bool& bret)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  bret = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(find_expr(targets, expr, bret))) {
    LOG_WARN("failed to find expr", K(ret));
  } else if (bret) {
    // expr exists in targets
  } else if (is_null_propagate_type(expr->get_expr_type())) {
    for (int64_t i = 0; OB_SUCC(ret) && !bret && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(i), targets, bret)))) {
        LOG_WARN("failed to check param can propagate null", K(ret));
      }
    }
  } else if (T_OP_LIKE == expr->get_expr_type() || T_OP_NOT_LIKE == expr->get_expr_type()) {
    // c1 like 'xxx' escape NULL is not null propagate. escape NULL = escape '\\'
    for (int64_t i = 0; OB_SUCC(ret) && !bret && i < expr->get_param_count() - 1; ++i) {
      if (OB_FAIL(is_null_propagate_expr(expr->get_param_expr(i), targets, bret))) {
        LOG_WARN("failed to check param can propagate null", K(ret));
      }
    }
  } else if (T_OP_AND == expr->get_expr_type() || T_OP_OR == expr->get_expr_type()) {
    bret = true;
    for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(i), targets, bret)))) {
        LOG_WARN("failed to check param can propagate null", K(ret));
      }
    }
  } else if (T_OP_BTW == expr->get_expr_type() || T_OP_NOT_BTW == expr->get_expr_type()) {
    if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(0), targets, bret)))) {
      LOG_WARN("failed to check param can propagate null", K(ret));
    }
  }
  if (OB_SUCC(ret) && bret) {
    LOG_TRACE("find null propagate expr", K(*expr));
  }
  return ret;
}

inline bool is_valid_arith_expr(const ObItemType type)
{
  return (T_OP_NEG <= type && type <= T_OP_MOD) || (T_OP_AGG_ADD <= type && type <= T_OP_AGG_DIV) || T_OP_POW == type ||
         T_OP_SIGN == type || T_OP_XOR == type;
}

/**
 * @brief is_valid_bool_expr
 * NSEQ is null safe comparison
 * NULL IS [NOT] NULL/TRUE/FALSE => TRUE/FALSE,
 * NULL AND FALSE                => FALSE
 * NULL OR TRUE                  => TRUE
 * NULL >  all (empty set)       => TRUE
 * NULL >  any (empty set)       => FALSE
 */
inline bool is_valid_bool_expr(const ObItemType type)
{
  return ((T_OP_EQ <= type && type <= T_OP_NE) || (T_OP_REGEXP <= type && type <= T_OP_NOT_IN) || T_OP_BOOL == type) &&
         T_OP_NSEQ != type && T_OP_AND != type && T_OP_OR != type;
}

inline bool is_valid_bit_expr(const ObItemType type)
{
  return T_OP_BIT_AND <= type && type <= T_OP_BIT_RIGHT_SHIFT;
}

/**
 * @brief is_valid_sys_func
 * more sys func can be included here
 */
inline bool is_valid_sys_func(const ObItemType type)
{
  bool ret = false;
  const static ObItemType WHITE_LIST[] = {
      T_FUN_SYS_SQRT,
      T_FUN_SYS_LOG_TEN,
      T_FUN_SYS_LOG_TWO,
      T_FUN_SYS_FLOOR,
      T_FUN_SYS_CEIL,
      T_FUN_SYS_LEAST,
      T_FUN_SYS_GREATEST,
      T_FUN_SYS_LEAST_INNER,
      T_FUN_SYS_GREATEST_INNER,
  };
  for (int64_t i = 0; !ret && i < sizeof(WHITE_LIST) / sizeof(ObItemType); ++i) {
    ret = (type == WHITE_LIST[i]);
  }
  return ret;
}

inline bool is_valid_aggr_func(const ObItemType type)
{
  bool ret = false;
  const static ObItemType WHITE_LIST[] = {T_FUN_MIN, T_FUN_MAX, T_FUN_SUM};
  for (int64_t i = 0; !ret && i < sizeof(WHITE_LIST) / sizeof(ObItemType); ++i) {
    ret = (type == WHITE_LIST[i]);
  }
  return ret;
}

/**
 * @brief ObTransformUtils::is_null_propagate_type
 * 1. arithmetic operators or functions
 * 2. boolean operators, except for T_OP_NESQ, T_OP_IS, T_OP_IS_NOT,
 * T_OP_AND, T_OP_OR,  T_OP_NOT_IN and T_OP_SQ_*
 * 3. bit operator
 * 4. some sys function
 */
bool ObTransformUtils::is_null_propagate_type(const ObItemType type)
{
  return is_valid_arith_expr(type) || is_valid_bool_expr(type) || is_valid_bit_expr(type) || is_valid_sys_func(type) ||
         is_valid_aggr_func(type);
}

int ObTransformUtils::find_expr(const ObIArray<const ObRawExpr*>& source, const ObRawExpr* target, bool& bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest expr is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !bret && i < source.count(); ++i) {
    if (OB_ISNULL(source.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in source is null", K(ret));
    } else if (source.at(i) == target || source.at(i)->same_as(*target)) {
      bret = true;
    }
  }
  return ret;
}

/**
 * column <=> ? or ? <=> column
 * column is ? or column != ?
 * column like ?
 * column not/between ? and ?
 * column in (?,?,?)
 */
int ObTransformUtils::get_simple_filter_column(
    ObDMLStmt* stmt, ObRawExpr* expr, int64_t table_id, ObIArray<ObColumnRefRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else {
    switch (expr->get_expr_type()) {
      case T_OP_EQ:
      case T_OP_NSEQ:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE: {
        ObRawExpr* left = NULL;
        ObRawExpr* right = NULL;
        bool left_is_lossless = false;
        bool right_is_lossless = false;
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr must has 2 arguments", K(ret));
        } else if (OB_ISNULL(left = expr->get_param_expr(0)) || OB_ISNULL(right = expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcept null param expr", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(left, left_is_lossless)) ||
                   OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(right, right_is_lossless))) {
          LOG_WARN("failed to check lossless cast", K(ret));
        } else if (left_is_lossless && FALSE_IT(left = left->get_param_expr(0))) {
        } else if (right_is_lossless && FALSE_IT(right = right->get_param_expr(0))) {
        } else if (left->is_column_ref_expr() && table_id == static_cast<ObColumnRefRawExpr*>(left)->get_table_id()) {
          if (right->get_relation_ids().has_member(stmt->get_table_bit_index(table_id))) {
            // do nothing
          } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, static_cast<ObColumnRefRawExpr*>(left)))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        } else if (right->is_column_ref_expr() && table_id == static_cast<ObColumnRefRawExpr*>(right)->get_table_id()) {
          if (left->get_relation_ids().has_member(stmt->get_table_bit_index(table_id))) {
            // do nothing
          } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, static_cast<ObColumnRefRawExpr*>(right)))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        }
        break;
      }
      case T_OP_IS:
      case T_OP_LIKE:
      case T_OP_BTW:
      case T_OP_NOT_BTW:
      case T_OP_IN: {
        ObRawExpr* left = NULL;
        ObRawExpr* right = NULL;
        if (OB_ISNULL(left = expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcept null param expr", K(ret));
        } else if (left->is_column_ref_expr() && table_id == static_cast<ObColumnRefRawExpr*>(left)->get_table_id()) {
          bool is_simple = true;
          for (int64_t i = 1; OB_SUCC(ret) && is_simple && i < expr->get_param_count(); ++i) {
            if (OB_ISNULL(right = expr->get_param_expr(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexcept null param expr", K(ret));
            } else if (right->get_relation_ids().has_member(stmt->get_table_bit_index(table_id))) {
              is_simple = false;
            }
          }
          if (OB_SUCC(ret) && is_simple) {
            if (OB_FAIL(add_var_to_array_no_dup(col_exprs, static_cast<ObColumnRefRawExpr*>(left)))) {
              LOG_WARN("failed to push back column expr", K(ret));
            }
          }
        }
        break;
      }
      case T_OP_AND:
      case T_OP_OR: {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          if (OB_FAIL(SMART_CALL(get_simple_filter_column(stmt, expr->get_param_expr(i), table_id, col_exprs)))) {
            LOG_WARN("failed to get simple filter column", K(ret));
          }
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObTransformUtils::get_parent_stmt(
    ObDMLStmt* root_stmt, ObDMLStmt* stmt, ObDMLStmt*& parent_stmt, int64_t& table_id, bool& is_valid)
{
  int ret = OB_SUCCESS;
  parent_stmt = NULL;
  table_id = OB_INVALID;
  is_valid = false;
  if (OB_ISNULL(root_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (root_stmt == stmt) {
    // do nothing
  } else {
    if (root_stmt->is_set_stmt()) {
      ObIArray<ObSelectStmt*>& child_query = static_cast<ObSelectStmt*>(root_stmt)->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_query.count(); ++i) {
        if (child_query.at(i) == stmt) {
          is_valid = true;
          parent_stmt = root_stmt;
        }
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && !is_valid && j < root_stmt->get_table_size(); ++j) {
      TableItem* table = root_stmt->get_table_item(j);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!table->is_generated_table()) {
        // do nothing
      } else if (stmt == table->ref_query_) {
        is_valid = true;
        table_id = table->table_id_;
        parent_stmt = root_stmt;
      }
    }
    if (OB_FAIL(ret) || is_valid) {
      // do nothing
    } else {
      ObSEArray<ObSelectStmt*, 8> child_stmts;
      if (OB_FAIL(root_stmt->get_child_stmts(child_stmts))) {
        LOG_WARN("failed to get child stmts", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < child_stmts.count(); ++i) {
        if (OB_FAIL(SMART_CALL(get_parent_stmt(child_stmts.at(i), stmt, parent_stmt, table_id, is_valid)))) {
          LOG_WARN("failed to get parent stmt", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_simple_filter_column_in_parent_stmt(ObDMLStmt* root_stmt, ObDMLStmt* stmt,
    ObDMLStmt* view_stmt, int64_t table_id, ObIArray<ObColumnRefRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* parent_stmt = NULL;
  bool is_valid = false;
  int64_t view_table_id = OB_INVALID;
  ObSEArray<ObColumnRefRawExpr*, 8> parent_col_exprs;
  const ObSelectStmt* sel_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!view_stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<const ObSelectStmt*>(view_stmt))) {
  } else if (OB_FAIL(get_parent_stmt(root_stmt, stmt, parent_stmt, view_table_id, is_valid))) {
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (parent_stmt->is_set_stmt()) {
    if OB_FAIL (SMART_CALL(
                    get_simple_filter_column_in_parent_stmt(root_stmt, parent_stmt, view_stmt, table_id, col_exprs))) {
      LOG_WARN("failed to get filter column in parent stmt", K(ret));
    }
  } else if (OB_FAIL(SMART_CALL(get_filter_columns(root_stmt, parent_stmt, view_table_id, parent_col_exprs)))) {
    LOG_WARN("failed to get filter columns", K(ret));
  } else if (!parent_col_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_col_exprs.count(); ++i) {
      ObColumnRefRawExpr* col = parent_col_exprs.at(i);
      int64_t sel_idx = -1;
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      } else if (OB_FALSE_IT(sel_idx = col->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      } else if (sel_idx < 0 || sel_idx >= sel_stmt->get_select_item_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item index is incorrect", K(sel_idx), K(ret));
      } else {
        ObRawExpr* sel_expr = sel_stmt->get_select_item(sel_idx).expr_;
        ObColumnRefRawExpr* col_expr = NULL;
        if (OB_ISNULL(sel_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (!sel_expr->is_column_ref_expr()) {
          // do nothing
        } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
        } else if (table_id != col_expr->get_table_id()) {
          // do nothing
        } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, col_expr))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_filter_columns(
    ObDMLStmt* root_stmt, ObDMLStmt* stmt, int64_t table_id, ObIArray<ObColumnRefRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& conditions = stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      if (OB_FAIL(get_simple_filter_column(stmt, conditions.at(i), table_id, col_exprs))) {
        LOG_WARN("failed to get simple filter column", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_simple_filter_column_in_parent_stmt(root_stmt, stmt, stmt, table_id, col_exprs))) {
      LOG_WARN("failed to get simple filter column in parent stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_column_match_index(ObDMLStmt* root_stmt, ObDMLStmt* stmt, ObSqlSchemaGuard* schema_guard,
    const ObColumnRefRawExpr* col_expr, bool& is_match)
{
  int ret = OB_SUCCESS;
  const TableItem* table_item = NULL;
  is_match = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col_expr), K(schema_guard));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    LOG_TRACE("table item not exists in this stmt", K(ret), K(*col_expr), K(table_item));
  } else if (table_item->is_basic_table()) {
    ObSEArray<ObColumnRefRawExpr*, 8> col_exprs;
    if (OB_FAIL(get_filter_columns(root_stmt, stmt, table_item->table_id_, col_exprs))) {
      LOG_WARN("failed to get filter columns", K(ret));
    } else if (OB_FAIL(is_match_index(schema_guard, stmt, col_expr, is_match, NULL, &col_exprs))) {
      LOG_WARN("failed to check is match index", K(ret));
    }
  } else if (table_item->is_generated_table()) {
    int64_t sel_idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
    ObSelectStmt* subquery = table_item->ref_query_;
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(ret));
    } else if (OB_FAIL(check_select_item_match_index(root_stmt, subquery, schema_guard, sel_idx, is_match))) {
      LOG_WARN("failed to check select item match index", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_select_item_match_index(
    ObDMLStmt* root_stmt, ObSelectStmt* stmt, ObSqlSchemaGuard* schema_guard, int64_t sel_index, bool& is_match)
{
  int ret = OB_SUCCESS;
  const TableItem* table_item = NULL;
  is_match = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(schema_guard));
  } else if (stmt->is_set_stmt()) {
    ObIArray<ObSelectStmt*>& child_query = stmt->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < child_query.count(); ++i) {
      ret = SMART_CALL(
          check_select_item_match_index(root_stmt, stmt->get_set_query(i), schema_guard, sel_index, is_match));
    }
  } else {
    if (sel_index < 0 || sel_index >= stmt->get_select_item_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item index is incorrect", K(sel_index), K(ret));
    } else {
      ObRawExpr* sel_expr = stmt->get_select_item(sel_index).expr_;
      ObColumnRefRawExpr* col = NULL;
      if (OB_ISNULL(sel_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!sel_expr->is_column_ref_expr()) {
        // do nothing
      } else if (OB_FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
      } else if (OB_FAIL(SMART_CALL(check_column_match_index(root_stmt, stmt, schema_guard, col, is_match)))) {
        LOG_WARN("failed to check column match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_match_index(ObSqlSchemaGuard* schema_guard, const ObDMLStmt* stmt,
    const ObColumnRefRawExpr* col_expr, bool& is_match, EqualSets* equal_sets, ObIArray<ObColumnRefRawExpr*>* col_exprs)
{
  int ret = OB_SUCCESS;
  uint64_t table_ref_id = OB_INVALID_ID;
  const TableItem* table_item = NULL;
  const ObTableSchema* table_schema = NULL;
  is_match = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col_expr), K(schema_guard));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get_table_item", K(ret), K(table_item));
  } else if (table_item->is_basic_table()) {
    table_ref_id = table_item->ref_id_;
    ObSEArray<uint64_t, 8> index_cols;
    ObArenaAllocator alloc;
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(schema_guard->get_table_schema(table_ref_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_ref_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is NULL", K(ret), K(table_schema));
    } else if (NULL != equal_sets && OB_FAIL(const_cast<ObDMLStmt*>(stmt)->get_stmt_equal_sets(
                                         *equal_sets, alloc, true, EQUAL_SET_SCOPE::SCOPE_WHERE))) {
      LOG_WARN("failed to get stmt equal sets", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos, false))) {
        LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
      }
      for (int64_t i = -1; OB_SUCC(ret) && !is_match && i < simple_index_infos.count(); ++i) {
        const ObTableSchema* index_schema = NULL;
        uint64_t tid = (i == -1 ? table_ref_id : simple_index_infos.at(i).table_id_);
        index_cols.reuse();
        if (OB_FAIL(schema_guard->get_table_schema(tid, index_schema))) {
          LOG_WARN("fail to get index schema", K(ret), K(tid));
        } else if (!index_schema->get_rowkey_info().is_valid()) {
          // do nothing
        } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(index_cols))) {
          LOG_WARN("failed to get index cols", K(ret));
        } else if (OB_FAIL(is_match_index(stmt, index_cols, col_expr, is_match, equal_sets, col_exprs))) {
          LOG_WARN("failed to check is column match index", K(ret));
        }
      }
    }
    if (NULL != equal_sets) {
      equal_sets->reuse();
    }
  }
  return ret;
}

int ObTransformUtils::is_match_index(const ObDMLStmt* stmt, const ObIArray<uint64_t>& index_cols,
    const ObColumnRefRawExpr* col_expr, bool& is_match, EqualSets* equal_sets, ObIArray<ObColumnRefRawExpr*>* col_exprs)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* index_expr = NULL;
  bool is_const = true;
  is_match = false;
  ObSEArray<ObRawExpr*, 4> const_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(col_expr));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(stmt->get_condition_exprs(), const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && is_const && i < index_cols.count(); ++i) {
    if (col_expr->get_column_id() == index_cols.at(i)) {
      is_match = true;
    } else if (OB_ISNULL(index_expr = stmt->get_column_expr_by_id(col_expr->get_table_id(), index_cols.at(i)))) {
      is_const = false;
    } else if (NULL != col_exprs && ObOptimizerUtil::find_item(*col_exprs, index_expr)) {
      // continue
    } else if (NULL != equal_sets &&
               OB_FAIL(ObOptimizerUtil::is_const_expr(index_expr, *equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (NULL == equal_sets) {
      is_const = false;
    }
  }
  return ret;
}

int ObTransformUtils::extract_query_ref_expr(ObIArray<ObRawExpr*>& exprs, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_query_ref_expr(exprs.at(i), subqueries))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_query_ref_expr(ObRawExpr* expr, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_query_ref_expr()) {
    ret = add_var_to_array_no_dup(subqueries, static_cast<ObQueryRefRawExpr*>(expr));
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_query_ref_expr(expr->get_param_expr(i), subqueries)))) {
        LOG_WARN("failed to extract query ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_aggr_expr(
    int32_t expr_level, ObIArray<ObRawExpr*>& exprs, ObIArray<ObAggFunRawExpr*>& aggrs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_aggr_expr(expr_level, exprs.at(i), aggrs))) {
      LOG_WARN("failed to extract agg exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_aggr_expr(int32_t expr_level, ObRawExpr* expr, ObIArray<ObAggFunRawExpr*>& aggrs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_aggr_expr()) {
    if (expr_level == expr->get_expr_level()) {
      ret = add_var_to_array_no_dup(aggrs, static_cast<ObAggFunRawExpr*>(expr));
    }
  } else if (expr->is_query_ref_expr()) {
    ObSelectStmt* ref_stmt = static_cast<ObQueryRefRawExpr*>(expr)->get_ref_stmt();
    ObSEArray<ObRawExpr*, 16> relation_exprs;
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(ref_stmt));
    } else if (OB_FAIL(ref_stmt->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
        if (OB_FAIL(SMART_CALL(extract_aggr_expr(expr_level, relation_exprs.at(i), aggrs)))) {
          LOG_WARN("failed to extract aggr expr", K(ret));
        }
      }
    }
    /* aggregate expr may exists in subqueries,eg:
     * select max(c1), sum(max(c1)) from t2 group by c1 having exists (select * from t3 where t3.c1 > max(t2.c1)
     */
  } else if (expr->has_flag(CNT_AGG) || expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_aggr_expr(expr_level, expr->get_param_expr(i), aggrs)))) {
        LOG_WARN("failed to extract aggr expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_winfun_expr(ObIArray<ObRawExpr*>& exprs, ObIArray<ObWinFunRawExpr*>& win_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_winfun_expr(exprs.at(i), win_exprs))) {
      LOG_WARN("failed to extract winfun exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_winfun_expr(ObRawExpr* expr, ObIArray<ObWinFunRawExpr*>& win_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_win_func_expr()) {
    ret = add_var_to_array_no_dup(win_exprs, static_cast<ObWinFunRawExpr*>(expr));
  } else if (expr->has_flag(CNT_WINDOW_FUNC)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_winfun_expr(expr->get_param_expr(i), win_exprs)))) {
        LOG_WARN("failed to extract aggr expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_foreign_primary_join(const TableItem* first_table, const TableItem* second_table,
    const ObIArray<const ObRawExpr*>& first_exprs, const ObIArray<const ObRawExpr*>& second_exprs,
    ObSchemaChecker* schema_checker, bool& is_foreign_primary_join, bool& is_first_table_parent,
    ObForeignKeyInfo*& foreign_key_info)
{
  int ret = OB_SUCCESS;
  is_foreign_primary_join = false;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(schema_checker) || OB_ISNULL(first_table) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameters have null", K(ret), K(schema_checker), K(first_table), K(second_table));
  } else if (OB_FAIL(schema_checker->get_table_schema(first_table->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_schema));
  } else {
    // get foreign key infos from first table's schema
    const ObIArray<ObForeignKeyInfo>& first_infos = table_schema->get_foreign_key_infos();
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < first_infos.count(); ++i) {
      const ObForeignKeyInfo& cur_info = first_infos.at(i);
      const uint64_t parent_id = cur_info.parent_table_id_;
      const uint64_t child_id = cur_info.child_table_id_;
      if (child_id == first_table->ref_id_ && parent_id == second_table->ref_id_) {
        // first table is child table
        is_first_table_parent = false;
        if (OB_FAIL(is_all_foreign_key_involved(first_exprs, second_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      } else if (child_id == second_table->ref_id_ && parent_id == first_table->ref_id_) {
        // second table is child table
        is_first_table_parent = true;
        if (OB_FAIL(is_all_foreign_key_involved(second_exprs, first_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      }
      if (OB_SUCC(ret) && find) {
        foreign_key_info = &(const_cast<ObForeignKeyInfo&>(cur_info));
        is_foreign_primary_join = true;
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_foreign_primary_join(const TableItem* first_table, const TableItem* second_table,
    const ObIArray<ObRawExpr*>& first_exprs, const ObIArray<ObRawExpr*>& second_exprs, ObSchemaChecker* schema_checker,
    bool& is_foreign_primary_join, bool& is_first_table_parent, ObForeignKeyInfo*& foreign_key_info)
{
  int ret = OB_SUCCESS;
  is_foreign_primary_join = false;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(schema_checker) || OB_ISNULL(first_table) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameters have null", K(ret), K(schema_checker), K(first_table), K(second_table));
  } else if (OB_FAIL(schema_checker->get_table_schema(first_table->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_schema));
  } else {
    // get foreign key infos from first table's schema
    const ObIArray<ObForeignKeyInfo>& first_infos = table_schema->get_foreign_key_infos();
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < first_infos.count(); ++i) {
      const ObForeignKeyInfo& cur_info = first_infos.at(i);
      const uint64_t parent_id = cur_info.parent_table_id_;
      const uint64_t child_id = cur_info.child_table_id_;
      if (child_id == first_table->ref_id_ && parent_id == second_table->ref_id_) {
        // first table is child table
        is_first_table_parent = false;
        if (OB_FAIL(is_all_foreign_key_involved(first_exprs, second_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      } else if (child_id == second_table->ref_id_ && parent_id == first_table->ref_id_) {
        // second table is child table
        is_first_table_parent = true;
        if (OB_FAIL(is_all_foreign_key_involved(second_exprs, first_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      }
      if (OB_SUCC(ret) && find) {
        foreign_key_info = &(const_cast<ObForeignKeyInfo&>(cur_info));
        is_foreign_primary_join = true;
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_all_foreign_key_involved(const ObIArray<const ObRawExpr*>& child_exprs,
    const ObIArray<const ObRawExpr*>& parent_exprs, const ObForeignKeyInfo& info, bool& is_all_involved)
{
  int ret = OB_SUCCESS;
  is_all_involved = false;
  const int64_t N = info.child_column_ids_.count();
  if (OB_UNLIKELY(child_exprs.count() != parent_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "child exprs and parent exprs should have equal size", K(ret), K(child_exprs.count()), K(parent_exprs.count()));
  } else if (N == child_exprs.count()) {
    int64_t match = 0;
    for (int64_t i = 0; i < N; ++i) {
      bool find = false;
      const ObRawExpr* child_expr = child_exprs.at(i);
      const ObRawExpr* parent_expr = parent_exprs.at(i);
      if (OB_ISNULL(child_expr) || OB_ISNULL(parent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child expr or parent expr is null", K(ret), K(child_expr), K(parent_expr));
      } else if (OB_UNLIKELY(!child_expr->has_flag(IS_COLUMN) || !parent_expr->has_flag(IS_COLUMN))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not column expr", K(ret));
      } else {
        const ObColumnRefRawExpr* child_col = static_cast<const ObColumnRefRawExpr*>(child_exprs.at(i));
        const ObColumnRefRawExpr* parent_col = static_cast<const ObColumnRefRawExpr*>(parent_exprs.at(i));
        for (int64_t j = 0; !find && j < N; ++j) {
          if (parent_col->get_column_id() == info.parent_column_ids_.at(j) &&
              child_col->get_column_id() == info.child_column_ids_.at(j)) {
            ++match;
            find = true;
          }
        }
      }
    }
    if (N == match) {
      is_all_involved = true;
    }
  }
  return ret;
}

int ObTransformUtils::is_all_foreign_key_involved(const ObIArray<ObRawExpr*>& child_exprs,
    const ObIArray<ObRawExpr*>& parent_exprs, const ObForeignKeyInfo& info, bool& is_all_involved)
{
  int ret = OB_SUCCESS;
  is_all_involved = false;
  const int64_t N = info.child_column_ids_.count();
  if (OB_UNLIKELY(child_exprs.count() != parent_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "child exprs and parent exprs should have equal size", K(ret), K(child_exprs.count()), K(parent_exprs.count()));
  } else if (N == child_exprs.count()) {
    int64_t match = 0;
    for (int64_t i = 0; i < N; ++i) {
      bool find = false;
      ObRawExpr* child_expr = child_exprs.at(i);
      ObRawExpr* parent_expr = parent_exprs.at(i);
      if (OB_ISNULL(child_expr) || OB_ISNULL(parent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child expr or parent expr is null", K(ret), K(child_expr), K(parent_expr));
      } else if (OB_UNLIKELY(!child_expr->has_flag(IS_COLUMN) || !parent_expr->has_flag(IS_COLUMN))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not column expr", K(ret));
      } else {
        ObColumnRefRawExpr* child_col = static_cast<ObColumnRefRawExpr*>(child_exprs.at(i));
        ObColumnRefRawExpr* parent_col = static_cast<ObColumnRefRawExpr*>(parent_exprs.at(i));
        for (int64_t j = 0; !find && j < N; ++j) {
          if (parent_col->get_column_id() == info.parent_column_ids_.at(j) &&
              child_col->get_column_id() == info.child_column_ids_.at(j)) {
            ++match;
            find = true;
          }
        }
      }
    }
    if (N == match) {
      is_all_involved = true;
    }
  }
  return ret;
}

int ObTransformUtils::is_foreign_key_rely(
    ObSQLSessionInfo* session_info, const share::schema::ObForeignKeyInfo* foreign_key_info, bool& is_rely)
{
  int ret = OB_SUCCESS;
  int64_t foreign_key_checks = 0;
  is_rely = false;
  if (OB_ISNULL(session_info) || OB_ISNULL(foreign_key_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(ret), K(session_info), K(foreign_key_info));
  } else if (SMO_IS_ORACLE_MODE(session_info->get_sql_mode())) {
    if (foreign_key_info->rely_flag_) {
      is_rely = true;
    } else if (foreign_key_info->validate_flag_) {
      is_rely = true;
    } else {
      is_rely = false;
    }
  } else if (OB_FAIL(session_info->get_foreign_key_checks(foreign_key_checks))) {
    LOG_WARN("get var foreign_key_checks failed", K(ret));
  } else if (foreign_key_checks) {
    is_rely = true;
  }
  return ret;
}

int ObTransformUtils::check_exprs_unique(ObDMLStmt& stmt, TableItem* table, const ObIArray<ObRawExpr*>& exprs,
    const ObIArray<ObRawExpr*>& conditions, ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker,
    bool& is_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem*, 1> table_items;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(table_items.push_back(table))) {
    LOG_WARN("failed to push back tableitem", K(ret));
  } else if (OB_FAIL(check_exprs_unique_on_table_items(
                 &stmt, session_info, schema_checker, table_items, exprs, conditions, true, is_unique))) {
    LOG_WARN("failed to check exprs unique on table items", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_exprs_unique(ObDMLStmt& stmt, TableItem* table, const ObIArray<ObRawExpr*>& exprs,
    ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker, bool& is_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem*, 1> table_items;
  ObSEArray<ObRawExpr*, 1> dummy_condition;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(table_items.push_back(table))) {
    LOG_WARN("failed to push back tableitem", K(ret));
  } else if (OB_FAIL(check_exprs_unique_on_table_items(
                 &stmt, session_info, schema_checker, table_items, exprs, dummy_condition, false, is_unique))) {
    LOG_WARN("failed to check exprs unique on table items", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_exprs_unique_on_table_items(ObDMLStmt* stmt, ObSQLSessionInfo* session_info,
    ObSchemaChecker* schema_checker, const ObIArray<TableItem*>& table_items, const ObIArray<ObRawExpr*>& exprs,
    const ObIArray<ObRawExpr*>& conditions, bool is_strict, bool& is_unique)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    MemoryContext* mem_entity = NULL;
    lib::ContextParam param;
    param.set_mem_attr(ObMemAttr(session_info->get_effective_tenant_id(), "CheckUnique"));
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL);
    param.set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_entity, param))) {
      LOG_WARN("failed to create memory entity", K(ret));
    } else if (OB_ISNULL(mem_entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create memory entity", K(ret));
    } else {
      ObArenaAllocator& alloc = mem_entity->get_arena_allocator();
      ObFdItemFactory fd_item_factory(alloc);
      ObRawExprFactory expr_factory(alloc);
      UniqueCheckHelper check_helper;
      check_helper.alloc_ = &alloc;
      check_helper.fd_factory_ = &fd_item_factory;
      check_helper.expr_factory_ = &expr_factory;
      check_helper.schema_checker_ = schema_checker;
      check_helper.session_info_ = session_info;
      UniqueCheckInfo res_info;
      ObRelIds all_tables;
      if (OB_FAIL(compute_tables_property(stmt, check_helper, table_items, conditions, res_info))) {
        LOG_WARN("failed to compute tables property", K(ret));
      } else if (OB_FAIL(get_rel_ids_from_tables(stmt, table_items, all_tables))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (!is_strict && OB_FAIL(append(res_info.fd_sets_, res_info.candi_fd_sets_))) {
        // is strict, use fd_item_set & candi_fd_set check unique
        LOG_WARN("failed to append fd item sets", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(
                     exprs, all_tables, res_info.fd_sets_, res_info.equal_sets_, res_info.const_exprs_, is_unique))) {
        LOG_WARN("failed to check is exprs unique", K(ret));
      } else {
        LOG_TRACE("get is unique result", K(exprs), K(table_items), K(is_unique));
      }
    }
    if (OB_NOT_NULL(mem_entity)) {
      DESTROY_CONTEXT(mem_entity);
    }
  }
  return ret;
}

int ObTransformUtils::check_stmt_unique(ObSelectStmt* stmt, ObSQLSessionInfo* session_info,
    ObSchemaChecker* schema_checker, const bool is_strict, bool& is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr*, 16> select_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(SMART_CALL(
                 check_stmt_unique(stmt, session_info, schema_checker, select_exprs, is_strict, is_unique)))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_stmt_unique(ObSelectStmt* stmt, ObSQLSessionInfo* session_info,
    ObSchemaChecker* schema_checker, const ObIArray<ObRawExpr*>& exprs, const bool is_strict, bool& is_unique,
    const uint64_t extra_flags)  // default value FLAGS_DEFAULT(0)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> select_exprs;
  bool ignore_distinct = (extra_flags & FLAGS_IGNORE_DISTINCT) == FLAGS_IGNORE_DISTINCT;
  bool ignore_group = (extra_flags & FLAGS_IGNORE_GROUP) == FLAGS_IGNORE_GROUP;
  is_unique = false;
  bool need_check = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_hierarchical_query()) {
    is_unique = false;
    need_check = false;
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (stmt->is_set_stmt()) {
    if (stmt->is_set_distinct() && ObOptimizerUtil::subset_exprs(select_exprs, exprs)) {
      is_unique = true;
    } else if (ObSelectStmt::INTERSECT != stmt->get_set_op() && ObSelectStmt::EXCEPT != stmt->get_set_op()) {
      need_check = false;
    } else {
      need_check = true;
    }
  } else if (0 == stmt->get_from_item_size()  // select expr from dual
             && ObOptimizerUtil::overlap_exprs(select_exprs, exprs)) {
    is_unique = true;
  } else if (!ignore_distinct && stmt->is_distinct()  // distinct
             && ObOptimizerUtil::subset_exprs(select_exprs, exprs)) {
    is_unique = true;
  } else if (!ignore_group && stmt->has_rollup()) {
    is_unique = false;
    need_check = false;
  } else if (!ignore_group && stmt->get_group_expr_size() > 0  // group by
             && ObOptimizerUtil::subset_exprs(stmt->get_group_exprs(), exprs)) {
    is_unique = true;
  } else if (!ignore_group && stmt->is_scala_group_by()  // scalar group by
             && ObOptimizerUtil::subset_exprs(select_exprs, exprs)) {
    is_unique = true;
  }

  if (OB_FAIL(ret) || is_unique || !need_check) {
    /*do nothing*/
  } else {
    MemoryContext* mem_entity = NULL;
    lib::ContextParam param;
    param.set_mem_attr(ObMemAttr(session_info->get_effective_tenant_id(), "CheckUnique"));
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL);
    param.set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_entity, param))) {
      LOG_WARN("failed to create memory entity", K(ret));
    } else if (OB_ISNULL(mem_entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create memory entity", K(ret));
    } else {
      ObArenaAllocator& alloc = mem_entity->get_arena_allocator();
      ObFdItemFactory fd_item_factory(alloc);
      ObRawExprFactory expr_factory(alloc);
      UniqueCheckHelper check_helper;
      check_helper.alloc_ = &alloc;
      check_helper.fd_factory_ = &fd_item_factory;
      check_helper.expr_factory_ = &expr_factory;
      check_helper.schema_checker_ = schema_checker;
      check_helper.session_info_ = session_info;
      UniqueCheckInfo res_info;
      ObRelIds all_tables;
      if (OB_FAIL(compute_stmt_property(stmt, check_helper, res_info, extra_flags))) {
        LOG_WARN("failed to compute stmt property", K(ret));
      } else if (OB_FAIL(get_from_tables(*stmt, all_tables))) {
        LOG_WARN("failed to get from tables", K(ret));
      } else if (!is_strict && OB_FAIL(append(res_info.fd_sets_, res_info.candi_fd_sets_))) {
        // is strict, use fd_item_set & candi_fd_set check unique
        LOG_WARN("failed to append fd item sets", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(
                     exprs, all_tables, res_info.fd_sets_, res_info.equal_sets_, res_info.const_exprs_, is_unique))) {
        LOG_WARN("failed to check is exprs unique", K(ret));
      } else {
        LOG_TRACE("get is unique result", K(ret), K(is_unique));
      }
    }
    if (OB_NOT_NULL(mem_entity)) {
      DESTROY_CONTEXT(mem_entity);
    }
  }
  return ret;
}

int ObTransformUtils::compute_stmt_property(ObSelectStmt* stmt, UniqueCheckHelper& check_helper,
    UniqueCheckInfo& res_info,
    const uint64_t extra_flags)  // default value FLAGS_DEFAULT(0)
{
  int ret = OB_SUCCESS;
  bool ignore_distinct = (extra_flags & FLAGS_IGNORE_DISTINCT) == FLAGS_IGNORE_DISTINCT;
  bool ignore_group = (extra_flags & FLAGS_IGNORE_GROUP) == FLAGS_IGNORE_GROUP;
  ObFdItemFactory* fd_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(fd_factory = check_helper.fd_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!ignore_group && stmt->has_rollup()) {
    // do nothing
  } else if (stmt->is_set_stmt()) {
    ret = compute_set_stmt_property(stmt, check_helper, res_info, extra_flags);
  } else if (!stmt->is_hierarchical_query() && OB_FAIL(compute_path_property(stmt, check_helper, res_info))) {
    LOG_WARN("failed to compute path property", K(ret));
  } else {
    // add const exprs / unique fd
    ObTableFdItem* select_unique_fd = NULL;
    ObTableFdItem* group_unique_fd = NULL;
    ObRelIds table_set;
    ObSEArray<ObRawExpr*, 4> select_exprs;
    if (OB_FAIL(get_from_tables(*stmt, table_set))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (0 == stmt->get_from_item_size() || (!ignore_group && stmt->is_scala_group_by())) {
      // from dual / scalar group by: add const exprs & unique fd
      if (OB_FAIL(fd_factory->create_table_fd_item(
              select_unique_fd, true, select_exprs, stmt->get_current_level(), table_set))) {
        LOG_WARN("failed to create table fd item", K(ret));
      } else if (OB_FAIL(append_array_no_dup(res_info.const_exprs_, select_exprs))) {
        LOG_WARN("failed to append const exprs", K(ret));
      }
    } else if (!ignore_distinct && stmt->is_distinct()) {  // distinct: add unique fd item
      ret =
          fd_factory->create_table_fd_item(select_unique_fd, true, select_exprs, stmt->get_current_level(), table_set);
    }

    if (OB_SUCC(ret) && !ignore_group && stmt->get_group_expr_size() > 0) {
      // group by: add unique fd item
      ret = fd_factory->create_table_fd_item(
          group_unique_fd, true, stmt->get_group_exprs(), stmt->get_current_level(), table_set);
    }

    if (OB_FAIL(ret)) {
    } else if (NULL != select_unique_fd && OB_FAIL(res_info.fd_sets_.push_back(select_unique_fd))) {
      LOG_WARN("failed to push back fd item set", K(ret));
    } else if (NULL != group_unique_fd && OB_FAIL(res_info.fd_sets_.push_back(group_unique_fd))) {
      LOG_WARN("failed to push back fd item set", K(ret));
    } else if (OB_FAIL(fd_factory->deduce_fd_item_set(
                   res_info.equal_sets_, select_exprs, res_info.const_exprs_, res_info.fd_sets_))) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("get stmt in compute stmt property", K(*stmt));
    LOG_TRACE("get stmt property",
        K(res_info.const_exprs_),
        K(res_info.equal_sets_),
        K(res_info.fd_sets_),
        K(res_info.candi_fd_sets_));
  }
  return ret;
}

int ObTransformUtils::compute_set_stmt_property(ObSelectStmt* stmt, UniqueCheckHelper& check_helper,
    UniqueCheckInfo& res_info,
    const uint64_t extra_flags)  // default value FLAGS_DEFAULT(0)
{
  int ret = OB_SUCCESS;
  bool ignore_distinct = (extra_flags & FLAGS_IGNORE_DISTINCT) == FLAGS_IGNORE_DISTINCT;
  ObExprFdItem* unique_fd = NULL;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> set_exprs;
  UniqueCheckInfo right_info;
  ObSEArray<ObRawExpr*, 4> equal_conds;
  EqualSets tmp_equal_sets;
  const ObSelectStmt::SetOperator set_type = stmt->get_set_op();
  ObFdItemFactory* fd_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(fd_factory = check_helper.fd_factory_) || OB_ISNULL(check_helper.expr_factory_) ||
      OB_ISNULL(check_helper.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!stmt->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (stmt->is_recursive_union()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if ((ObSelectStmt::EXCEPT == set_type || ObSelectStmt::INTERSECT == set_type) &&
             OB_FAIL(SMART_CALL(compute_stmt_property(stmt->get_set_query(0), check_helper, res_info)))) {
    LOG_WARN("failed to compute left stmt property", K(ret));
  } else if (ObSelectStmt::INTERSECT == set_type &&
             OB_FAIL(SMART_CALL(compute_stmt_property(stmt->get_set_query(1), check_helper, right_info)))) {
    LOG_WARN("failed to compute right stmt property", K(ret));
  } else if (!ignore_distinct && stmt->is_set_distinct() &&
             OB_FAIL(fd_factory->create_expr_fd_item(
                 unique_fd, true, select_exprs, stmt->get_current_level(), select_exprs))) {
    LOG_WARN("failed to create table fd item", K(ret));
  } else if (OB_FAIL(append(res_info.const_exprs_, right_info.const_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(tmp_equal_sets, res_info.equal_sets_))) {
    LOG_WARN("failed to append fd equal set", K(ret));
  } else if (OB_FAIL(append(tmp_equal_sets, right_info.equal_sets_))) {
    LOG_WARN("failed to append fd equal set", K(ret));
  } else if (OB_FAIL(get_expr_in_cast(select_exprs, set_exprs))) {
    LOG_WARN("failed to get first set op exprs", K(ret));
  } else if (OB_FAIL(get_equal_set_conditions(
                 *check_helper.expr_factory_, check_helper.session_info_, stmt, set_exprs, equal_conds))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                 check_helper.alloc_, equal_conds, tmp_equal_sets, res_info.equal_sets_))) {
    LOG_WARN("failed to compute compute equal set", K(ret));
  } else if (OB_FAIL(append(res_info.fd_sets_, right_info.fd_sets_))) {
    LOG_WARN("failed to append fd item", K(ret));
  } else if (NULL != unique_fd && OB_FAIL(res_info.fd_sets_.push_back(unique_fd))) {
    LOG_WARN("failed to push back fd item set", K(ret));
  } else if (OB_FAIL(append(res_info.candi_fd_sets_, right_info.candi_fd_sets_))) {
    LOG_WARN("failed to append fd item", K(ret));
  } else if (OB_FAIL(append(res_info.not_null_, right_info.not_null_))) {
    LOG_WARN("failed to append fd item", K(ret));
  } else if (OB_FAIL(fd_factory->deduce_fd_item_set(
                 res_info.equal_sets_, select_exprs, res_info.const_exprs_, res_info.fd_sets_))) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_equal_set_conditions(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    ObSelectStmt* stmt, ObIArray<ObRawExpr*>& set_exprs, ObIArray<ObRawExpr*>& equal_conds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<ObSelectStmt*, 2> child_stmts;
    if (ObSelectStmt::EXCEPT == stmt->get_set_op()) {
      // keep left equal expr for except stmts
      ret = child_stmts.push_back(stmt->get_set_query(0));
    } else if (ObSelectStmt::INTERSECT == stmt->get_set_op()) {
      // keep right equal expr for intersect stmts
      ret = child_stmts.assign(stmt->get_set_query());
    }
    ObSelectStmt* child_stmt = NULL;
    ObRawExpr* set_expr = NULL;
    ObRawExpr* child_expr = NULL;
    ObRawExpr* equal_expr = NULL;
    int64_t idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        const int64_t select_size = child_stmt->get_select_item_size();
        for (int64_t j = 0; OB_SUCC(ret) && j < set_exprs.count(); ++j) {
          if (OB_ISNULL(set_expr = set_exprs.at(j)) || OB_UNLIKELY(!set_expr->is_set_op_expr()) ||
              OB_UNLIKELY((idx = static_cast<ObSetOpRawExpr*>(set_expr)->get_idx()) < 0 || idx >= select_size) ||
              OB_ISNULL(child_expr = child_stmt->get_select_item(idx).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(
                         expr_factory, session_info, child_expr, set_expr, equal_expr))) {
            LOG_WARN("failed to create equal expr", K(ret));
          } else if (OB_FAIL(equal_conds.push_back(equal_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_expr_in_cast(ObIArray<ObRawExpr*>& input_exprs, ObIArray<ObRawExpr*>& output_exprs)
{
  int ret = OB_SUCCESS;
  UNUSED(input_exprs);
  UNUSED(output_exprs);
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); i++) {
    ObRawExpr* expr = input_exprs.at(i);
    int64_t cast_level = 0;
    while (OB_NOT_NULL(expr) && T_FUN_SYS_CAST == expr->get_expr_type()) {
      expr = ++cast_level <= OB_MAX_SET_STMT_SIZE ? expr->get_param_expr(0) : NULL;
    }
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(output_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

ObRawExpr* ObTransformUtils::get_expr_in_cast(ObRawExpr* expr)
{
  int64_t cast_level = 0;
  while (OB_NOT_NULL(expr) && T_FUN_SYS_CAST == expr->get_expr_type()) {
    expr = ++cast_level <= OB_MAX_SET_STMT_SIZE ? expr->get_param_expr(0) : NULL;
  }
  return expr;
}

int ObTransformUtils::UniqueCheckInfo::assign(const UniqueCheckInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    table_set_.clear_all();
    if (OB_FAIL(table_set_.add_members(other.table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(const_exprs_.assign(other.const_exprs_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (OB_FAIL(equal_sets_.assign(other.equal_sets_))) {
      LOG_WARN("failed to assign equal sets", K(ret));
    } else if (OB_FAIL(fd_sets_.assign(other.fd_sets_))) {
      LOG_WARN("failed to assign fd sets", K(ret));
    } else if (OB_FAIL(candi_fd_sets_.assign(other.candi_fd_sets_))) {
      LOG_WARN("failed to assign fd sets", K(ret));
    } else if (OB_FAIL(not_null_.assign(other.not_null_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

void ObTransformUtils::UniqueCheckInfo::reset()
{
  table_set_.clear_all();
  const_exprs_.reuse();
  equal_sets_.reuse();
  fd_sets_.reuse();
  candi_fd_sets_.reuse();
  not_null_.reuse();
}

int ObTransformUtils::compute_path_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> cond_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(cond_exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {

    // 1. compute inner join from table
    ObSEArray<ObRawExpr*, 1> dummy_inner_join_conds;
    UniqueCheckInfo cur_info;
    bool first_table = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
      const FromItem& from_item = stmt->get_from_item(i);
      TableItem* table = NULL;
      ObSqlBitSet<> rel_ids;
      UniqueCheckInfo left_info;
      UniqueCheckInfo right_info;
      if (from_item.is_joined_) {
        table = static_cast<TableItem*>(stmt->get_joined_table(from_item.table_id_));
      } else {
        table = stmt->get_table_item_by_id(from_item.table_id_);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected from item", K(ret), K(from_item));
      } else if (OB_FAIL(get_table_rel_ids(*stmt, *table, rel_ids))) {
        LOG_WARN("failed to get table relids", K(ret));
      } else if (OB_FAIL(compute_table_property(stmt, check_helper, table, cond_exprs, right_info))) {
        LOG_WARN("failed to compute table property", K(ret));
      } else if (first_table) {
        first_table = false;
        ret = cur_info.assign(right_info);
      } else if (OB_FAIL(left_info.assign(cur_info))) {
        LOG_WARN("failed to assign check info", K(ret));
      } else {
        cur_info.reset();
        ret = compute_inner_join_property(
            stmt, check_helper, left_info, right_info, dummy_inner_join_conds, cond_exprs, cur_info);
      }
    }

    // 2. enhance fd using semi on condition
    ObIArray<SemiInfo*>& semi_infos = stmt->get_semi_infos();
    ObSEArray<ObRawExpr*, 8> semi_conditions;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (semi_infos.at(i)->is_anti_join()) {
        /* do nothing */
      } else if (OB_FAIL(append(semi_conditions, semi_infos.at(i)->semi_conditions_))) {
        LOG_WARN("failed to append conditions", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(res_info.table_set_.add_members(cur_info.table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(res_info.const_exprs_.assign(cur_info.const_exprs_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(semi_conditions, res_info.const_exprs_))) {
      LOG_WARN("failed to compute const equivalent exprs", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                   check_helper.alloc_, semi_conditions, cur_info.equal_sets_, res_info.equal_sets_))) {
      LOG_WARN("failed to compute equal set", K(ret));
    } else if (OB_FAIL(res_info.fd_sets_.assign(cur_info.fd_sets_)) ||
               OB_FAIL(res_info.candi_fd_sets_.assign(cur_info.candi_fd_sets_))) {
      LOG_WARN("failed to assign fd item set", K(ret));
    } else if (OB_FAIL(res_info.not_null_.assign(cur_info.not_null_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (!semi_conditions.empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                   semi_conditions, res_info.candi_fd_sets_, res_info.fd_sets_, res_info.not_null_))) {
      LOG_WARN("failed to enhance fd item set", K(ret));
    } else {
      LOG_TRACE("get info in compute path property",
          K(res_info.const_exprs_),
          K(res_info.equal_sets_),
          K(res_info.fd_sets_),
          K(res_info.candi_fd_sets_));
    }
  }
  return ret;
}

int ObTransformUtils::compute_tables_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper,
    const ObIArray<TableItem*>& table_items, const ObIArray<ObRawExpr*>& conditions, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> cond_exprs;
  ObSEArray<ObRawExpr*, 1> dummy_inner_join_conds;
  TableItem* table = NULL;
  bool first_table = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(cond_exprs.assign(conditions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    UniqueCheckInfo left_info;
    UniqueCheckInfo right_info;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(table = table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected from item", K(ret));
    } else if (OB_FAIL(compute_table_property(stmt, check_helper, table, cond_exprs, right_info))) {
      LOG_WARN("failed to compute table property", K(ret));
    } else if (first_table) {
      first_table = false;
      ret = res_info.assign(right_info);
    } else if (OB_FAIL(left_info.assign(res_info))) {
      LOG_WARN("failed to assign check info", K(ret));
    } else {
      res_info.reset();
      ret = compute_inner_join_property(
          stmt, check_helper, left_info, right_info, dummy_inner_join_conds, cond_exprs, res_info);
    }
  }

  if (OB_SUCC(ret) && !cond_exprs.empty() &&
      OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
          cond_exprs, res_info.candi_fd_sets_, res_info.fd_sets_, res_info.not_null_))) {
    LOG_WARN("failed to enhance fd item set", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("get info in compute tables property",
        K(res_info.const_exprs_),
        K(res_info.equal_sets_),
        K(res_info.fd_sets_),
        K(res_info.candi_fd_sets_));
  }
  return ret;
}

int ObTransformUtils::compute_table_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, TableItem* table,
    ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  JoinedTable* joined_table = NULL;
  ObJoinType joined_type = UNKNOWN_JOIN;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (table->is_basic_table() &&
             OB_FAIL(compute_basic_table_property(stmt, check_helper, table, cond_exprs, res_info))) {
    LOG_WARN("failed to compute basic table property", K(ret));
  } else if ((table->is_generated_table() || table->is_temp_table()) &&
             OB_FAIL(SMART_CALL(compute_generate_table_property(stmt, check_helper, table, cond_exprs, res_info)))) {
    LOG_WARN("failed to compute generate table property", K(ret));
  } else if (!table->is_joined_table()) {
    /*do nothing*/
  } else if (FALSE_IT(joined_table = static_cast<JoinedTable*>(table)) ||
             FALSE_IT(joined_type = joined_table->joined_type_)) {
  } else if (INNER_JOIN == joined_type) {
    ret = SMART_CALL(compute_inner_join_property(stmt, check_helper, joined_table, cond_exprs, res_info));
  } else if (IS_OUTER_JOIN(joined_type)) {
    ret = SMART_CALL(compute_outer_join_property(stmt, check_helper, joined_table, cond_exprs, res_info));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined type", K(ret), K(*table));
  }
  return ret;
}

int ObTransformUtils::compute_basic_table_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, TableItem* table,
    ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> cur_cond_exprs;
  const ObTableSchema* table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> index_infos;
  ObSchemaChecker* schema_checker = NULL;
  ObSqlBitSet<> table_set;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(check_helper.alloc_) || OB_ISNULL(check_helper.fd_factory_) ||
      OB_ISNULL(schema_checker = check_helper.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!table->is_basic_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table", K(ret), K(*table));
  } else if (OB_FAIL(get_table_rel_ids(*stmt, *table, table_set)) ||
             OB_FAIL(res_info.table_set_.add_members(table_set))) {
    LOG_WARN("failed to get table relids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, *table, cur_cond_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
    LOG_WARN("failed to remove cur cond exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, cur_cond_exprs, res_info.equal_sets_))) {
    LOG_WARN("failed to compute compute equal set", K(ret));
  } else if (OB_FAIL(schema_checker->get_table_schema(table->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table->ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(ret), K(table));
  } else if (OB_FAIL(ObOptimizerUtil::try_add_fd_item(stmt,
                 *check_helper.fd_factory_,
                 table->table_id_,
                 res_info.table_set_,
                 table_schema,
                 cur_cond_exprs,
                 res_info.not_null_,
                 res_info.fd_sets_,
                 res_info.candi_fd_sets_))) {
    LOG_WARN("failed to try add fd item", K(ret));
  } else if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(index_infos, false))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
    const ObTableSchema* index_schema = NULL;
    if (OB_FAIL(schema_checker->get_table_schema(index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null index schema", K(ret));
    } else if (!index_schema->is_unique_index()) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::try_add_fd_item(stmt,
                   *check_helper.fd_factory_,
                   table->table_id_,
                   res_info.table_set_,
                   index_schema,
                   cur_cond_exprs,
                   res_info.not_null_,
                   res_info.fd_sets_,
                   res_info.candi_fd_sets_))) {
      LOG_WARN("failed to try add fd item", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::compute_generate_table_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper,
    TableItem* table, ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  UniqueCheckInfo child_info;
  EqualSets tmp_equal_set;
  ObSelectStmt* ref_query = NULL;
  ObSEArray<ObRawExpr*, 4> cur_cond_exprs;
  ObSqlBitSet<> table_set;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(check_helper.alloc_) || OB_ISNULL(check_helper.fd_factory_) ||
      OB_ISNULL(check_helper.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if ((!table->is_generated_table() && !table->is_temp_table()) || OB_ISNULL(ref_query = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table", K(ret), K(*table));
  } else if (OB_FAIL(SMART_CALL(compute_stmt_property(ref_query, check_helper, child_info)))) {
    LOG_WARN("failed to compute stmt property", K(ret));
  } else if (OB_FAIL(get_table_rel_ids(*stmt, *table, table_set)) ||
             OB_FAIL(res_info.table_set_.add_members(table_set))) {
    LOG_WARN("failed to get table relids", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(*check_helper.expr_factory_,
                 child_info.equal_sets_,
                 table->table_id_,
                 *stmt,
                 *ref_query,
                 true,
                 child_info.const_exprs_,
                 res_info.const_exprs_))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(*check_helper.expr_factory_,
                 child_info.equal_sets_,
                 table->table_id_,
                 *stmt,
                 *ref_query,
                 true,
                 child_info.not_null_,
                 res_info.not_null_))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, *table, cur_cond_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
    LOG_WARN("failed to remove cur cond exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_equal_sets(check_helper.alloc_,
                 *check_helper.expr_factory_,
                 table->table_id_,
                 *stmt,
                 *ref_query,
                 child_info.equal_sets_,
                 tmp_equal_set))) {
    LOG_WARN("failed to convert subplan scan equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                 check_helper.alloc_, cur_cond_exprs, tmp_equal_set, res_info.equal_sets_))) {
    LOG_WARN("failed to compute equal set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_fd_item_sets(*check_helper.fd_factory_,
                 *check_helper.expr_factory_,
                 child_info.equal_sets_,
                 table->table_id_,
                 *stmt,
                 *ref_query,
                 child_info.fd_sets_,
                 res_info.fd_sets_))) {
    LOG_WARN("failed to convert subplan scan fd item sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_fd_item_sets(*check_helper.fd_factory_,
                 *check_helper.expr_factory_,
                 child_info.equal_sets_,
                 table->table_id_,
                 *stmt,
                 *ref_query,
                 child_info.candi_fd_sets_,
                 res_info.candi_fd_sets_))) {
    LOG_WARN("failed to convert subplan scan fd item sets", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 cur_cond_exprs, res_info.candi_fd_sets_, res_info.fd_sets_, res_info.not_null_))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  }
  return ret;
}

int ObTransformUtils::compute_inner_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, JoinedTable* table,
    ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  TableItem* left_table = NULL;
  TableItem* right_table = NULL;
  UniqueCheckInfo left_info;
  UniqueCheckInfo right_info;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!table->is_inner_join() || OB_ISNULL(left_table = table->left_table_) ||
             OB_ISNULL(right_table = table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inner join table", K(ret), K(*table));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, left_table, cond_exprs, left_info))) {
    LOG_WARN("failed to compute inner join left table property", K(ret));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, right_table, cond_exprs, right_info))) {
    LOG_WARN("failed to compute inner join right table property", K(ret));
  } else if (OB_FAIL(compute_inner_join_property(
                 stmt, check_helper, left_info, right_info, table->get_join_conditions(), cond_exprs, res_info))) {
    LOG_WARN("failed to compute inner join table property", K(ret));
  }
  return ret;
}

int ObTransformUtils::compute_inner_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper,
    UniqueCheckInfo& left_info, UniqueCheckInfo& right_info, ObIArray<ObRawExpr*>& inner_join_cond_exprs,
    ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info)
{
  ObSEArray<ObRawExpr*, 4> left_join_exprs;
  ObSEArray<ObRawExpr*, 4> right_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_right_join_exprs;
  ObSEArray<ObRawExpr*, 4> cur_cond_exprs;
  EqualSets tmp_equal_sets;
  ObSqlBitSet<> table_set;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(check_helper.alloc_) || OB_ISNULL(check_helper.fd_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(table_set.add_members(left_info.table_set_)) ||
             OB_FAIL(table_set.add_members(right_info.table_set_)) ||
             OB_FAIL(res_info.table_set_.add_members(table_set))) {
    LOG_WARN("failed to get table relids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, table_set, cur_cond_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
    LOG_WARN("failed to remove cur cond exprs", K(ret));
  } else if (OB_FAIL(append(cur_cond_exprs, inner_join_cond_exprs))) {
    LOG_WARN("failed to append cur cond exprs", K(ret));
  } else if (OB_FAIL(append(res_info.const_exprs_, left_info.const_exprs_)) ||
             OB_FAIL(append(res_info.const_exprs_, right_info.const_exprs_))) {
    LOG_WARN("failed to append const exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(append(tmp_equal_sets, left_info.equal_sets_)) ||
             OB_FAIL(append(tmp_equal_sets, right_info.equal_sets_))) {
    LOG_WARN("failed to append const exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                 check_helper.alloc_, cur_cond_exprs, tmp_equal_sets, res_info.equal_sets_))) {
    LOG_WARN("failed to compute compute equal set", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 cur_cond_exprs, left_info.candi_fd_sets_, left_info.fd_sets_, left_info.not_null_))) {
    LOG_WARN("failed to enhance left fd item set", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 cur_cond_exprs, right_info.candi_fd_sets_, right_info.fd_sets_, right_info.not_null_))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  } else if (OB_FAIL(append(res_info.not_null_, left_info.not_null_)) ||
             OB_FAIL(append(res_info.not_null_, right_info.not_null_))) {
    LOG_WARN("failed to append not null columns", K(ret));
  } else if (!cur_cond_exprs.empty() && OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(cur_cond_exprs,
                                            left_info.table_set_,
                                            right_info.table_set_,
                                            left_join_exprs,
                                            right_join_exprs,
                                            all_left_join_exprs,
                                            all_right_join_exprs))) {
    LOG_WARN("failed to get type safe join exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(*check_helper.fd_factory_,
                 right_info.table_set_,
                 right_join_exprs,
                 right_info.const_exprs_,
                 right_info.equal_sets_,
                 right_info.fd_sets_,
                 all_left_join_exprs,
                 left_info.equal_sets_,
                 left_info.fd_sets_,
                 left_info.candi_fd_sets_,
                 res_info.fd_sets_,
                 res_info.candi_fd_sets_))) {
    LOG_WARN("failed to add left fd_item_set for inner join", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(*check_helper.fd_factory_,
                 left_info.table_set_,
                 left_join_exprs,
                 left_info.const_exprs_,
                 left_info.equal_sets_,
                 left_info.fd_sets_,
                 all_right_join_exprs,
                 right_info.equal_sets_,
                 right_info.fd_sets_,
                 right_info.candi_fd_sets_,
                 res_info.fd_sets_,
                 res_info.candi_fd_sets_))) {
    LOG_WARN("failed to add right fd_item_set for inner join", K(ret));
  }
  return ret;
}

int ObTransformUtils::compute_outer_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, JoinedTable* table,
    ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info)
{
  int ret = OB_SUCCESS;
  TableItem* left_table = NULL;
  UniqueCheckInfo left_info;
  TableItem* right_table = NULL;
  UniqueCheckInfo right_info;
  ObSEArray<ObRawExpr*, 1> dummy_conds;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(check_helper.alloc_) || OB_ISNULL(check_helper.fd_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!table->is_full_join() && !table->is_left_join() && !table->is_right_join()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined table", K(ret), K(*table));
  } else if (table->is_full_join()) {
    /*do nothing*/
  } else if (table->is_left_join() &&
             (OB_ISNULL(left_table = table->left_table_) || OB_ISNULL(right_table = table->right_table_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inner join table", K(ret), K(*table));
  } else if (table->is_right_join() &&
             (OB_ISNULL(left_table = table->right_table_) || OB_ISNULL(right_table = table->left_table_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inner join table", K(ret), K(*table));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, left_table, dummy_conds, left_info))) {
    LOG_WARN("failed to compute inner join left table property", K(ret));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, right_table, dummy_conds, right_info))) {
    LOG_WARN("failed to compute inner join right table property", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> left_join_exprs;
    ObSEArray<ObRawExpr*, 4> right_join_exprs;
    ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
    ObSEArray<ObRawExpr*, 4> all_right_join_exprs;
    ObSEArray<ObRawExpr*, 4> cur_cond_exprs;
    EqualSets tmp_equal_sets;
    ObSqlBitSet<> table_set;
    if (OB_FAIL(table_set.add_members(left_info.table_set_)) || OB_FAIL(table_set.add_members(right_info.table_set_)) ||
        OB_FAIL(res_info.table_set_.add_members(table_set))) {
      LOG_WARN("failed to get table relids", K(ret));
    } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, table_set, cur_cond_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
      LOG_WARN("failed to remove cur cond exprs", K(ret));
    } else if (OB_FAIL(append(res_info.const_exprs_, left_info.const_exprs_))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else if (OB_FAIL(append(tmp_equal_sets, left_info.equal_sets_))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                   check_helper.alloc_, cur_cond_exprs, tmp_equal_sets, res_info.equal_sets_))) {
      LOG_WARN("failed to compute compute equal set", K(ret));
    } else if (!cur_cond_exprs.empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                   cur_cond_exprs, left_info.candi_fd_sets_, left_info.fd_sets_, left_info.not_null_))) {
      LOG_WARN("failed to enhance left fd item set", K(ret));
    } else if (!cur_cond_exprs.empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                   cur_cond_exprs, right_info.candi_fd_sets_, right_info.fd_sets_, right_info.not_null_))) {
      LOG_WARN("failed to enhance right fd item set", K(ret));
    } else if (!table->get_join_conditions().empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(table->get_join_conditions(),
                   right_info.candi_fd_sets_,
                   right_info.fd_sets_,
                   right_info.not_null_))) {
      LOG_WARN("failed to enhance right fd item set", K(ret));
    } else if (OB_FAIL(append(res_info.not_null_, left_info.not_null_))) {
      LOG_WARN("failed to append not null columns", K(ret));
    } else if (!table->get_join_conditions().empty() &&
               OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(table->get_join_conditions(),
                   left_info.table_set_,
                   right_info.table_set_,
                   left_join_exprs,
                   right_join_exprs,
                   all_left_join_exprs,
                   all_right_join_exprs))) {
      LOG_WARN("failed to get type safe join exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(*check_helper.fd_factory_,
                   right_info.table_set_,
                   right_join_exprs,
                   right_info.const_exprs_,
                   right_info.equal_sets_,
                   right_info.fd_sets_,
                   all_left_join_exprs,
                   left_info.equal_sets_,
                   left_info.fd_sets_,
                   left_info.candi_fd_sets_,
                   res_info.fd_sets_,
                   res_info.candi_fd_sets_))) {
      LOG_WARN("failed to add left fd_item_set for inner join", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_table_exprs(const ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& source_exprs,
    const TableItem& target, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  if (OB_FAIL(get_table_rel_ids(stmt, target, table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(stmt, source_exprs, table_set, exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_table_exprs(const ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& source_exprs,
    const ObIArray<TableItem*>& tables, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  if (OB_FAIL(get_table_rel_ids(stmt, tables, table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(stmt, source_exprs, table_set, exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_table_exprs(const ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& source_exprs,
    const ObSqlBitSet<>& table_set, ObIArray<ObRawExpr*>& table_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < source_exprs.count(); ++i) {
    ObRawExpr* expr = source_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source expr shoud not be null", K(ret), K(source_exprs), K(i));
    } else if (!expr->get_expr_levels().has_member(stmt.get_current_level())) {
      // do nothing
    } else if (!table_set.is_superset2(expr->get_relation_ids())) {
      /* do nothing */
    } else if (OB_FAIL(table_exprs.push_back(expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_table_joined_exprs(const ObDMLStmt& stmt, const TableItem& source, const TableItem& target,
    const ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& target_exprs, ObSqlBitSet<>& join_source_ids,
    ObSqlBitSet<>& join_target_ids)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> source_table_ids;
  ObSqlBitSet<> target_table_ids;
  if (OB_FAIL(get_table_rel_ids(stmt, source, source_table_ids))) {
    LOG_WARN("failed to get source table rel ids", K(ret));
  } else if (OB_FAIL(get_table_rel_ids(stmt, target, target_table_ids))) {
    LOG_WARN("failed to get target table rel ids", K(ret));
  } else if (OB_FAIL(get_table_joined_exprs(
                 source_table_ids, target_table_ids, conditions, target_exprs, join_source_ids, join_target_ids))) {
    LOG_WARN("failed to get table joined exprs by table rel ids", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_table_joined_exprs(const ObDMLStmt& stmt, const ObIArray<TableItem*>& sources,
    const TableItem& target, const ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& target_exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> source_table_ids;
  ObSqlBitSet<> target_table_ids;
  ObSqlBitSet<> join_source_ids;
  ObSqlBitSet<> join_target_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < sources.count(); ++i) {
    const TableItem* table = sources.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(get_table_rel_ids(stmt, *table, source_table_ids))) {
      LOG_WARN("failed to get source table rel ids", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(get_table_rel_ids(stmt, target, target_table_ids))) {
    LOG_WARN("failed to get target table rel ids", K(ret));
  } else if (OB_FAIL(get_table_joined_exprs(
                 source_table_ids, target_table_ids, conditions, target_exprs, join_source_ids, join_target_ids))) {
    LOG_WARN("failed to get table joined exprs by table rel ids", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

// Given conditions, find all equal conditions
// equal conditions looks like: source related expr = target related expr,
// e.g. target table is t2
// join condition can be: t1.a = t2.b, t1.a = t2.b + 1, t1.a = t2.a + t2.b
//            can not be: t2.a = t2.b or t2.a = 1
int ObTransformUtils::get_table_joined_exprs(const ObSqlBitSet<>& source_ids, const ObSqlBitSet<>& target_ids,
    const ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& target_exprs, ObSqlBitSet<>& join_source_ids,
    ObSqlBitSet<>& join_target_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
    ObRawExpr* condition = conditions.at(i);
    bool is_valid = false;
    if (OB_ISNULL(condition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition should not be null", K(ret));
    } else if (T_OP_EQ == condition->get_expr_type()) {
      ObOpRawExpr* op = static_cast<ObOpRawExpr*>(condition);
      ObRawExpr* child1 = op->get_param_expr(0);
      ObRawExpr* child2 = op->get_param_expr(1);
      if (OB_ISNULL(child1) || OB_ISNULL(child2)) {
        LOG_WARN("parameter of EQ expr should not be null", K(ret), K(child1), K(child2));
      } else if (!child1->has_flag(CNT_COLUMN) || !child2->has_flag(CNT_COLUMN)) {
        /* do nothing */
      } else if (source_ids.is_superset2(child1->get_relation_ids()) &&
                 target_ids.is_superset2(child2->get_relation_ids())) {
        // child2 contain target exprs
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                child1->get_result_type(), child2->get_result_type(), child2->get_result_type(), is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child1 expr type as the (child1, child2) compare type");
        } else if (OB_FAIL(target_exprs.push_back(child2))) {
          LOG_WARN("failed to push target expr", K(ret));
        } else if (OB_FAIL(join_source_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        } else if (OB_FAIL(join_target_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        }
      } else if (target_ids.is_superset2(child1->get_relation_ids()) &&
                 source_ids.is_superset2(child2->get_relation_ids())) {
        // child1 contain target column
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                child2->get_result_type(), child1->get_result_type(), child1->get_result_type(), is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child2 expr type as the (child2, child1) compare type");
        } else if (OB_FAIL(target_exprs.push_back(child1))) {
          LOG_WARN("failed to push target expr", K(ret));
        } else if (OB_FAIL(join_source_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        } else if (OB_FAIL(join_target_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        }
      } else if (target_ids.is_superset2(child1->get_relation_ids()) &&
                 target_ids.is_superset2(child2->get_relation_ids())) {
        // child1 contain target column and child2 contain target column too.
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                child1->get_result_type(), child2->get_result_type(), child2->get_result_type(), is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child2 expr type as the (child2, child1) compare type");
        } else if (OB_FAIL(join_target_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        } else if (OB_FAIL(join_target_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        }
      } else if (source_ids.is_superset2(child1->get_relation_ids()) &&
                 source_ids.is_superset2(child2->get_relation_ids())) {
        // child1 contain source column and child2 contain source column too.
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                child2->get_result_type(), child1->get_result_type(), child1->get_result_type(), is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child2 expr type as the (child2, child1) compare type");
        } else if (OB_FAIL(join_source_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        } else if (OB_FAIL(join_source_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::relids_to_table_items(ObDMLStmt* stmt, const ObRelIds& rel_ids, ObIArray<TableItem*>& rel_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    TableItem* table = NULL;
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
      if (OB_ISNULL(table = stmt->get_table_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_UNLIKELY((idx = stmt->get_table_bit_index(table->table_id_)) == OB_INVALID_INDEX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table item invalid idx", K(idx), K(table->table_id_));
      } else if (rel_ids.has_member(idx)) {
        ret = rel_array.push_back(table);
      }
    }
  }
  return ret;
}

int ObTransformUtils::relids_to_table_ids(
    ObDMLStmt* stmt, const ObSqlBitSet<>& table_set, ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    TableItem* table = NULL;
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
      if (OB_ISNULL(table = stmt->get_table_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_UNLIKELY((idx = stmt->get_table_bit_index(table->table_id_)) == OB_INVALID_INDEX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table item invalid idx", K(idx), K(table->table_id_));
      } else if (table_set.has_member(idx)) {
        ret = table_ids.push_back(table->table_id_);
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_table_rel_ids(const ObDMLStmt& stmt, const TableItem& target, ObSqlBitSet<>& table_set)
{
  int ret = OB_SUCCESS;
  if (target.is_joined_table()) {
    const JoinedTable& joined_table = static_cast<const JoinedTable&>(target);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.single_table_ids_.count(); ++i) {
      if (OB_FAIL(table_set.add_member(stmt.get_table_bit_index(joined_table.single_table_ids_.at(i))))) {
        LOG_WARN("failed to add member", K(ret), K(joined_table.single_table_ids_.at(i)));
      }
    }
  } else if (OB_FAIL(table_set.add_member(stmt.get_table_bit_index(target.table_id_)))) {
    LOG_WARN("failed to add member", K(ret), K(target.table_id_));
  }
  return ret;
}

int ObTransformUtils::get_table_rel_ids(
    const ObDMLStmt& stmt, const ObIArray<uint64_t>& table_ids, ObSqlBitSet<>& table_set)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    idx = stmt.get_table_bit_index(table_ids.at(i));
    if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect idx", K(ret));
    } else if (OB_FAIL(table_set.add_member(idx))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_table_rel_ids(const ObDMLStmt& stmt, const uint64_t table_id, ObSqlBitSet<>& table_set)
{
  int ret = OB_SUCCESS;
  int32_t idx = stmt.get_table_bit_index(table_id);
  if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect idx", K(ret));
  } else if (OB_FAIL(table_set.add_member(idx))) {
    LOG_WARN("failed to add members", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_table_rel_ids(
    const ObDMLStmt& stmt, const ObIArray<TableItem*>& tables, ObSqlBitSet<>& table_set)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_ISNULL(tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null", K(ret));
    } else if (OB_FAIL(get_table_rel_ids(stmt, *tables.at(i), table_set))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_from_item(ObDMLStmt* stmt, TableItem* table, FromItem& from)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or table is null", K(ret), K(stmt), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < stmt->get_from_item_size(); ++i) {
    from = stmt->get_from_item(i);
    if (from.table_id_ == table->table_id_) {
      found = true;
    } else if (!from.is_joined_) {
      // do nothing
    } else if (OB_FAIL(
                   ObOptimizerUtil::find_table_item(stmt->get_joined_table(from.table_id_), table->table_id_, found))) {
      LOG_WARN("failed to find table item", K(ret));
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find from item", K(*table));
  }
  return ret;
}

int ObTransformUtils::is_equal_correlation(
    ObRawExpr* cond, const int64_t stmt_level, bool& is_valid, ObRawExpr** outer_param, ObRawExpr** inner_param)
{
  int ret = OB_SUCCESS;
  ObRawExpr* left = NULL;
  ObRawExpr* right = NULL;
  bool left_is_correlated = false;
  bool right_is_correlated = false;
  is_valid = false;
  if (OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition expr is null", K(ret), K(cond));
  } else if (cond->get_expr_type() != T_OP_EQ) {
  } else if (OB_ISNULL(left = cond->get_param_expr(0)) || OB_ISNULL(right = cond->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal operator is invalid", K(ret), K(left), K(right));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(left, stmt_level - 1, left_is_correlated))) {
    LOG_WARN("failed to check left child is correlated", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(right, stmt_level - 1, right_is_correlated))) {
    LOG_WARN("failed to check right child is correlated", K(ret));
  } else if (left_is_correlated == right_is_correlated) {
    // both correlated or both not
    LOG_TRACE("both condition are [not] correlated", K(is_valid), K(left_is_correlated), K(right_is_correlated));
  } else if ((left_is_correlated && left->get_expr_levels().has_member(stmt_level)) ||
             (right_is_correlated && right->get_expr_levels().has_member(stmt_level))) {
    LOG_TRACE("expr used both inner and outer block columns", K(is_valid));
  } else {
    is_valid = true;
    // left is the expr from the outer stmt
    if (right_is_correlated) {
      ObRawExpr* tmp = left;
      left = right;
      right = tmp;
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
            left->get_result_type(), right->get_result_type(), right->get_result_type(), is_valid))) {
      LOG_WARN("failed to check is expr result equivalent", K(ret));
    } else if (!is_valid) {
      // left = right <=> group by right (right = right)
      // equal meta should be same for (left, right) comparison and (right, right) comparision
      // be careful about two cases
      // 1. varchar = varchar (but collation is not the same)
      // 2. varchar = int (cast is required)
      LOG_TRACE("cast is required to transform equal as group/partition by", K(is_valid));
    } else {
      if (OB_NOT_NULL(outer_param)) {
        *outer_param = left;
      }
      if (OB_NOT_NULL(inner_param)) {
        *inner_param = right;
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_common_comparsion_correlation(
    ObRawExpr* cond, const int64_t stmt_level, bool& is_valid, ObRawExpr** outer_param, ObRawExpr** inner_param)
{
  int ret = OB_SUCCESS;
  ObRawExpr* left = NULL;
  ObRawExpr* right = NULL;
  bool left_is_correlated = false;
  bool right_is_correlated = false;
  is_valid = false;
  if (OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition expr is null", K(ret), K(cond));
  } else if (!IS_COMMON_COMPARISON_OP(cond->get_expr_type())) {
  } else if (OB_ISNULL(left = cond->get_param_expr(0)) || OB_ISNULL(right = cond->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common comparsion operator is invalid", K(ret), K(left), K(right));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(left, stmt_level - 1, left_is_correlated))) {
    LOG_WARN("failed to check left child is correlated", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(right, stmt_level - 1, right_is_correlated))) {
    LOG_WARN("failed to check right child is correlated", K(ret));
  } else if (left_is_correlated == right_is_correlated) {
    // both correlated or both not
    LOG_TRACE("both condition are [not] correlated", K(is_valid), K(left_is_correlated), K(right_is_correlated));
  } else if ((left_is_correlated && left->get_expr_levels().has_member(stmt_level)) ||
             (right_is_correlated && right->get_expr_levels().has_member(stmt_level))) {
    LOG_TRACE("expr used both inner and outer block columns", K(is_valid));
  } else {
    is_valid = true;
    // left is the expr from the outer stmt
    if (right_is_correlated) {
      ObRawExpr* tmp = left;
      left = right;
      right = tmp;
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    // for join first, no need to check is_equivalent(left, right, right)
    if (NULL != outer_param) {
      *outer_param = left;
    }
    if (NULL != inner_param) {
      *inner_param = right;
    }
  }
  return ret;
}

int ObTransformUtils::is_semi_join_right_table(const ObDMLStmt& stmt, const uint64_t table_id, bool& is_semi_table)
{
  int ret = OB_SUCCESS;
  const ObIArray<SemiInfo*>& semi_infos = stmt.get_semi_infos();
  is_semi_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_semi_table && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret));
    } else if (table_id == semi_infos.at(i)->right_table_id_) {
      is_semi_table = true;
    }
  }
  return ret;
}

int ObTransformUtils::merge_table_items(
    ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table, const ObIArray<int64_t>* output_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> from_col_exprs;
  ObSEArray<ObRawExpr*, 16> to_col_exprs;
  ObSEArray<ColumnItem, 16> target_column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(stmt), K(source_table), K(target_table));
  } else if (OB_FAIL(stmt->get_column_items(target_table->table_id_, target_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    int64_t miss_output_count = 0;
    uint64_t source_table_id = source_table->table_id_;
    uint64_t target_table_id = target_table->table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_column_items.count(); ++i) {
      ColumnItem* target_col = NULL;
      ColumnItem* source_col = NULL;
      uint64_t column_id = OB_INVALID_ID;
      if (OB_ISNULL(target_col = stmt->get_column_item_by_id(target_table_id, target_column_items.at(i).column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL == output_map) {
        column_id = target_col->column_id_;
        source_col = stmt->get_column_item_by_id(source_table_id, column_id);
      } else {
        // generated table with output map
        int64_t output_id = target_col->column_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_UNLIKELY(output_id < 0 || output_id >= output_map->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected array count", K(output_id), K(output_map->count()), K(ret));
        } else if (OB_INVALID_ID != output_map->at(output_id)) {
          column_id = output_map->at(output_id) + OB_APP_MIN_COLUMN_ID;
          source_col = stmt->get_column_item_by_id(source_table_id, column_id);
        } else if (OB_ISNULL(source_table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          column_id = source_table->ref_query_->get_select_item_size() + OB_APP_MIN_COLUMN_ID + miss_output_count;
          ++miss_output_count;
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_ISNULL(source_col)) {
        // distinct column, to be saved
        target_col->table_id_ = source_table_id;
        target_col->column_id_ = column_id;
        target_col->get_expr()->set_ref_id(source_table_id, column_id);
        target_col->get_expr()->set_table_name(source_table->get_table_name());
        if (OB_FAIL(target_col->get_expr()->pull_relation_id_and_levels(stmt->get_current_level()))) {
          LOG_WARN("failed to pull relation id and levels");
        }
      } else {
        // duplicate column item, to be replaced and remove
        if (OB_FAIL(from_col_exprs.push_back(target_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(to_col_exprs.push_back(source_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(stmt->remove_column_item(target_table_id, target_column_items.at(i).column_id_))) {
          LOG_WARN("failed to remove column item", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret) && !from_col_exprs.empty()) {
      if (OB_FAIL(stmt->replace_inner_stmt_expr(from_col_exprs, to_col_exprs))) {
        LOG_WARN("failed to replace col exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::merge_table_items(ObSelectStmt* source_stmt, ObSelectStmt* target_stmt,
    const TableItem* source_table, const TableItem* target_table, ObIArray<ObRawExpr*>& old_exprs,
    ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> target_column_items;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(source_stmt), K(target_stmt), K(source_table), K(target_table));
  } else if (OB_FAIL(target_stmt->get_column_items(target_table->table_id_, target_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    uint64_t source_table_id = source_table->table_id_;
    uint64_t target_table_id = target_table->table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_column_items.count(); ++i) {
      uint64_t column_id = target_column_items.at(i).column_id_;
      ColumnItem* target_col = target_stmt->get_column_item_by_id(target_table_id, column_id);
      ColumnItem* source_col = source_stmt->get_column_item_by_id(source_table_id, column_id);
      if (OB_ISNULL(target_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_ISNULL(source_col)) {
        // distinct column, to be saved
        target_col->table_id_ = source_table_id;
        target_col->column_id_ = column_id;
        target_col->get_expr()->set_ref_id(source_table_id, column_id);
        target_col->get_expr()->set_table_name(source_table->get_table_name());
        if (OB_FAIL(target_col->get_expr()->pull_relation_id_and_levels(source_stmt->get_current_level()))) {
          LOG_WARN("failed to pull relation id and levels");
        } else if (OB_FAIL(source_stmt->add_column_item(*target_col))) {
          LOG_WARN("failed to add column item", K(ret));
        } else { /*do nothing*/
        }
      } else {
        // duplicate column item, to be replaced and remove
        if (OB_FAIL(old_exprs.push_back(target_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(source_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::find_parent_expr(ObDMLStmt* stmt, ObRawExpr* target, ObRawExpr*& root, ObRawExpr*& parent)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> relation_exprs;
  parent = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(target));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(parent) && i < relation_exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = relation_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (OB_FAIL(find_parent_expr(expr, target, parent))) {
      LOG_WARN("failed to find parent expr", K(ret));
    } else if (NULL != parent) {
      root = expr;
    }
  }
  return ret;
}

int ObTransformUtils::find_parent_expr(ObRawExpr* expr, ObRawExpr* target, ObRawExpr*& parent)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr), K(target));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr == target) {
    parent = expr;
  } else if (target->is_query_ref_expr() && !expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (target->is_column_ref_expr() && !expr->has_flag(CNT_COLUMN)) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(parent) && i < expr->get_param_count(); ++i) {
      ObRawExpr* param_expr = NULL;
      if (OB_ISNULL(param_expr = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (target == param_expr) {
        parent = expr;
      } else if (OB_FAIL(SMART_CALL(find_parent_expr(param_expr, target, parent)))) {
        LOG_WARN("failed to find parent expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::find_relation_expr(
    ObDMLStmt* stmt, ObIArray<ObRawExpr*>& targets, ObIArray<ObRawExprPointer>& roots)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 4> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    ObRawExpr* parent = NULL;
    if (OB_FAIL(relation_exprs.at(i).get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && OB_ISNULL(parent) && j < targets.count(); ++j) {
      if (OB_FAIL(find_parent_expr(expr, targets.at(j), parent))) {
        LOG_WARN("failed to find parent expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(parent)) {
      if (OB_FAIL(roots.push_back(relation_exprs.at(i)))) {
        LOG_WARN("failed to push back relation expr pointer", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::generate_not_null_column(ObTransformerCtx* ctx, ObDMLStmt* stmt, ObRawExpr*& not_null_column)
{
  int ret = OB_SUCCESS;
  not_null_column = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx), K(stmt));
  } else {
    bool find = false;
    bool is_semi_table = false;
    bool is_on_null_side = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < stmt->get_table_size(); ++i) {
      TableItem* table = stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (!table->is_basic_table()) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_semi_join_right_table(*stmt, table->table_id_, is_semi_table))) {
        LOG_WARN("failed to check semi right table", K(ret));
      } else if (is_semi_table) {
        // do nothing
      } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt, table->table_id_, is_on_null_side))) {
        LOG_WARN("failed to check table on null side", K(ret));
      } else if (is_on_null_side) {
        // do nothing
      } else if (OB_FAIL(generate_not_null_column(ctx, stmt, table, not_null_column))) {
        LOG_WARN("failed to genereated not null column", K(ret));
      } else if (OB_NOT_NULL(not_null_column)) {
        not_null_column->set_explicited_reference();
        find = true;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObTransformUtils::generate_not_null_column(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, TableItem* item, ObRawExpr*& not_null_column)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  not_null_column = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->schema_checker_) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx), K(stmt));
  } else if (!item->is_basic_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is not basic table", K(*item));
  } else if (OB_FAIL(ctx->schema_checker_->get_table_schema(item->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  } else if (table_schema->is_no_pk_table()) {
    if (OB_FAIL(generate_row_key_column(stmt,
            *ctx->expr_factory_,
            *table_schema,
            item->table_id_,
            OB_HIDDEN_PK_INCREMENT_COLUMN_ID,
            not_null_column))) {
      LOG_WARN("failed to generate row key column");
    }
  } else {
    if (OB_FAIL(generate_row_key_column(
            stmt, *ctx->expr_factory_, *table_schema, item->table_id_, OB_NOT_EXIST_COLUMN_ID, not_null_column))) {
      LOG_WARN("failed to generate row key column");
    }
  }
  return ret;
}

int ObTransformUtils::generate_row_key_column(ObDMLStmt* stmt, ObRawExprFactory& expr_factory,
    const ObTableSchema& table_schema, uint64_t table_id, uint64_t row_key_id, ObRawExpr*& row_key_expr)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;
  ColumnItem* col_item = NULL;
  ObColumnRefRawExpr* col_expr = NULL;
  row_key_expr = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (row_key_id != OB_NOT_EXIST_COLUMN_ID) {
    if (NULL != (col_item = stmt->get_column_item_by_id(table_id, row_key_id))) {
      // do nothing
    } else if (OB_ISNULL(column_schema = (table_schema.get_column_schema(row_key_id)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(row_key_id), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_expr(
                   stmt, expr_factory, table_id, *column_schema, col_expr, NULL))) {
      LOG_WARN("failed to get row key expr", K(ret));
    } else {
      row_key_expr = col_expr;
    }
  } else {
    bool find = false;
    // get all the index keys
    const ObRowkeyInfo* rowkey_info = NULL;
    uint64_t column_id = OB_INVALID_ID;
    if (table_schema.is_index_table() && is_virtual_table(table_schema.get_data_table_id()) &&
        !table_schema.is_ordered()) {
      // for virtual table and its hash index
      rowkey_info = &table_schema.get_index_info();
    } else {
      rowkey_info = &table_schema.get_rowkey_info();
    }
    for (int col_idx = 0; !find && OB_SUCC(ret) && col_idx < rowkey_info->get_size(); ++col_idx) {
      if (OB_FAIL(rowkey_info->get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column id", K(ret));
      } else if (column_id == OB_HIDDEN_PK_PARTITION_COLUMN_ID || column_id == OB_HIDDEN_PK_CLUSTER_COLUMN_ID) {
        // do nothing
      } else if (NULL != (col_item = stmt->get_column_item_by_id(table_id, column_id))) {
        // do nothing
      } else if (OB_ISNULL(column_schema = (table_schema.get_column_schema(column_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_expr(
                     stmt, expr_factory, table_id, *column_schema, col_expr, NULL))) {
        LOG_WARN("failed to get row key expr", K(ret));
      } else {
        find = true;
        row_key_expr = col_expr;
      }
    }
  }
  return ret;
}

int ObTransformUtils::generate_unique_key(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, TableItem* item, ObIArray<ObRawExpr*>& unique_keys)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObSEArray<ColumnItem, 4> rowkey_cols;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->schema_checker_) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(item) ||
      OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx), K(item));
  } else if (!item->is_basic_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is not basic table", K(*item));
  } else if (OB_FAIL(ctx->schema_checker_->get_table_schema(item->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_column_items(
                 stmt, *ctx->expr_factory_, item->table_id_, *table_schema, rowkey_cols))) {
    LOG_WARN("failed to generate rowkey column items", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_cols.count(); ++j) {
    if (OB_ISNULL(rowkey_cols.at(j).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (rowkey_cols.at(j).expr_->get_column_id() == OB_HIDDEN_PK_PARTITION_COLUMN_ID ||
               rowkey_cols.at(j).expr_->get_column_id() == OB_HIDDEN_PK_CLUSTER_COLUMN_ID) {
      // do nothing
    } else if (FALSE_IT(rowkey_cols.at(j).expr_->set_explicited_reference())) {
    } else if (OB_FAIL(unique_keys.push_back(rowkey_cols.at(j).expr_))) {
      LOG_WARN("failed to push back partition expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::generate_unique_key(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSqlBitSet<>& ignore_tables, ObIArray<ObRawExpr*>& unique_keys)
{
  int ret = OB_SUCCESS;
  ObRelIds from_rel_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(stmt), K(ctx), K(ret));
  } else if (OB_FAIL(get_from_tables(*stmt, from_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else {
    ObIArray<TableItem*>& table_items = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      TableItem* table = table_items.at(i);
      int32_t idx = OB_INVALID_INDEX;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(table), K(ret));
      } else if (OB_FALSE_IT(idx = stmt->get_table_bit_index(table->table_id_))) {
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table bit index", K(table->table_id_), K(idx), K(ret));
      } else if (!from_rel_ids.has_member(idx)) {
        // semi join tables
      } else if (ignore_tables.has_member(idx)) {
        // do nothing
      } else if (table->is_basic_table()) {
        if (OB_FAIL(generate_unique_key(ctx, stmt, table, unique_keys))) {
          LOG_WARN("failed to generate unique key", K(ret));
        }
      } else if (table->is_generated_table()) {
        ObSelectStmt* view_stmt = NULL;
        ObSEArray<ObRawExpr*, 4> stmt_unique_keys;
        ObSEArray<ObRawExpr*, 4> column_exprs;
        if (OB_ISNULL(view_stmt = table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view_stmt = stmt is null", K(view_stmt), K(ret));
        } else if (OB_FAIL(recursive_set_stmt_unique(view_stmt, ctx, false, &stmt_unique_keys))) {
          LOG_WARN("recursive set stmt unique failed", K(ret));
        } else if (OB_FAIL(create_columns_for_view(ctx, *table, stmt, column_exprs))) {
          LOG_WARN("failed to create columns for view", K(ret));
        } else if (OB_FAIL(convert_select_expr_to_column_expr(
                       stmt_unique_keys, *view_stmt, *stmt, table->table_id_, unique_keys))) {
          LOG_WARN("failed to get stmt unique keys columns expr", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_column_exprs(ObDMLStmt* stmt, const int64_t stmt_level, const ObSqlBitSet<>& table_set,
    const ObIArray<ObDMLStmt*>& ignore_stmts, common::ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (ObOptimizerUtil::find_item(ignore_stmts, stmt)) {
    // do nothing
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(SMART_CALL(extract_column_exprs(relation_exprs, stmt_level, table_set, ignore_stmts, columns)))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformUtils::extract_column_exprs(const ObIArray<ObRawExpr*>& exprs, const int64_t stmt_level,
    const ObSqlBitSet<>& table_set, const ObIArray<ObDMLStmt*>& ignore_stmts, common::ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(SMART_CALL(extract_column_exprs(exprs.at(i), stmt_level, table_set, ignore_stmts, columns)))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::extract_column_exprs(ObRawExpr* expr, const int64_t stmt_level, const ObSqlBitSet<>& table_set,
    const ObIArray<ObDMLStmt*>& ignore_stmts, common::ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (!expr->get_expr_levels().has_member(stmt_level)) {
    // do nothing
  } else if (expr->is_column_ref_expr()) {
    if (!expr->get_relation_ids().is_subset2(table_set)) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(columns, expr))) {
      LOG_WARN("failed to add var to array", K(ret));
    }
  } else if (expr->is_query_ref_expr()) {
    if (OB_FAIL(SMART_CALL(extract_column_exprs(
            static_cast<ObQueryRefRawExpr*>(expr)->get_ref_stmt(), stmt_level, table_set, ignore_stmts, columns)))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(
              extract_column_exprs(expr->get_param_expr(i), stmt_level, table_set, ignore_stmts, columns)))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      }
    }
  }
  return ret;
}

bool ObTransformUtils::is_subarray(const ObRelIds& table_ids, const common::ObIArray<ObRelIds>& other)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < other.count(); i++) {
    if (table_ids.equal(other.at(i))) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

int ObTransformUtils::check_loseless_join(ObDMLStmt* stmt, ObTransformerCtx* ctx, TableItem* source_table,
    TableItem* target_table, ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker,
    ObStmtMapInfo& stmt_map_info, bool& is_loseless,
    EqualSets* input_equal_sets)  // default value NULL
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  bool source_unique = false;
  bool target_unique = false;
  ObSEArray<ObRawExpr*, 16> source_exprs;
  ObSEArray<ObRawExpr*, 16> target_exprs;
  is_loseless = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table) || OB_ISNULL(schema_checker)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table), K(target_table), K(schema_checker), K(ret));
  } else if (OB_FAIL(check_table_item_containment(stmt, source_table, target_table, stmt_map_info, is_contain))) {
    LOG_WARN("failed to check table item containment", K(ret));
  } else if (!is_contain) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::extract_lossless_join_columns(stmt,
                 ctx,
                 source_table,
                 target_table,
                 stmt_map_info.select_item_map_,
                 source_exprs,
                 target_exprs,
                 input_equal_sets))) {
    LOG_WARN("failed to extract lossless join columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(
                 *stmt, source_table, source_exprs, session_info, schema_checker, source_unique))) {
    LOG_WARN("failed to check exprs unique", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(
                 *stmt, target_table, target_exprs, session_info, schema_checker, target_unique))) {
    LOG_WARN("failed to check exprs unique", K(ret));
  } else if (source_unique && target_unique) {
    is_loseless = true;
    LOG_TRACE("succeed to check lossless join", K(source_unique), K(target_unique), K(is_loseless));
  } else {
    is_loseless = false;
    LOG_TRACE("succeed to check lossless join", K(source_unique), K(target_unique), K(is_loseless));
  }
  return ret;
}

int ObTransformUtils::check_relations_containment(ObDMLStmt* stmt, const common::ObIArray<TableItem*>& source_rels,
    const common::ObIArray<TableItem*>& target_rels, common::ObIArray<ObStmtMapInfo>& stmt_map_infos,
    common::ObIArray<int64_t>& rel_map_info, bool& is_contain)
{
  int ret = OB_SUCCESS;
  ObRelIds matched_rels;
  is_contain = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (source_rels.count() != target_rels.count()) {
    is_contain = false;
  } else {
    is_contain = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_contain && i < source_rels.count(); ++i) {
      bool is_matched = false;
      TableItem* source_table = source_rels.at(i);
      ObStmtMapInfo stmt_map_info;
      if (OB_ISNULL(source_table)) {
        LOG_WARN("can not find table item", K(source_rels.at(i)));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_matched && j < target_rels.count(); ++j) {
          if (matched_rels.has_member(j)) {
            /*do nothing*/
          } else {
            TableItem* target_table = target_rels.at(j);
            if (OB_ISNULL(target_table)) {
              LOG_WARN("can not find table item", K(target_rels.at(j)));
            } else if (OB_FAIL(
                           check_table_item_containment(stmt, source_table, target_table, stmt_map_info, is_matched))) {
              LOG_WARN("check table item containment failed", K(ret));
            } else if (is_matched && OB_SUCC(matched_rels.add_member(j)) && OB_FAIL(rel_map_info.push_back(j))) {
              LOG_WARN("push back table id failed", K(ret));
            } else {
              /*do nothing*/
            }
          }
        }
        if (OB_SUCC(ret) && is_matched) {
          if (OB_FAIL(stmt_map_infos.push_back(stmt_map_info))) {
            LOG_WARN("push back stmt map info failed", K(ret));
          } else {
            is_contain = true;
          }
        } else {
          is_contain = false;
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_table_item_containment(ObDMLStmt* stmt, const TableItem* source_table,
    const TableItem* target_table, ObStmtMapInfo& stmt_map_info, bool& is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table), K(target_table), K(ret));
  } else if (source_table->is_basic_table() && target_table->is_basic_table()) {
    QueryRelation relation = QueryRelation::UNCOMPARABLE;
    if (OB_FAIL(ObStmtComparer::compare_basic_table_item(stmt, source_table, stmt, target_table, relation))) {
      LOG_WARN("compare table part failed", K(ret), K(source_table), K(target_table));
    } else if (QueryRelation::LEFT_SUBSET == relation || QueryRelation::EQUAL == relation) {
      is_contain = true;
      LOG_TRACE("succeed to check table item containment", K(is_contain));
    } else {
      /*do nothing*/
    }
  } else if (source_table->is_generated_table() && target_table->is_generated_table()) {
    QueryRelation relation = QueryRelation::UNCOMPARABLE;
    if (OB_ISNULL(source_table->ref_query_) || OB_ISNULL(target_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(source_table->ref_query_), K(target_table->ref_query_), K(ret));
    } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(
                   source_table->ref_query_, target_table->ref_query_, stmt_map_info, relation))) {
      LOG_WARN("failed to compute stmt relationship", K(ret));
    } else if (QueryRelation::LEFT_SUBSET == relation || QueryRelation::EQUAL == relation) {
      is_contain = true;
      LOG_TRACE("succeed to check table item containment", K(is_contain));
    } else {
      is_contain = false;
      LOG_TRACE("succeed to check table item containment", K(is_contain));
    }
  } else {
    /*
     * todo in future, we should check containment relations for the following two more cases
     * case 1: source_table is a basic table and target_table is a generated table
     * case 2: source_table is a generated table and target_table is a basic table
     */
    is_contain = false;
    LOG_TRACE("succeed to check table item containment", K(is_contain));
  }
  return ret;
}

int ObTransformUtils::extract_lossless_join_columns(ObDMLStmt* stmt, ObTransformerCtx* ctx,
    const TableItem* source_table, const TableItem* target_table, const ObIArray<int64_t>& output_map,
    ObIArray<ObRawExpr*>& source_exprs, ObIArray<ObRawExpr*>& target_exprs,
    EqualSets* input_equal_sets)  // default value NULL
{
  int ret = OB_SUCCESS;
  EqualSets* equal_sets = input_equal_sets;
  ObArenaAllocator allocator;
  ObSEArray<ObRawExpr*, 16> candi_source_exprs;
  ObSEArray<ObRawExpr*, 16> candi_target_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table), K(target_table), K(ret));
  } else if (OB_FAIL(extract_lossless_mapping_columns(
                 stmt, source_table, target_table, output_map, candi_source_exprs, candi_target_exprs))) {
    LOG_WARN("failed to extract lossless mapping columns", K(ret));
  } else if (OB_UNLIKELY(candi_source_exprs.count() != candi_target_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(candi_source_exprs.count()), K(candi_target_exprs.count()), K(ret));
  } else if (NULL == equal_sets) {
    equal_sets = &ctx->equal_sets_;
    ret = stmt->get_stmt_equal_sets(*equal_sets, allocator, true, SCOPE_WHERE);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_source_exprs.count(); i++) {
    ObRawExpr* source = NULL;
    ObRawExpr* target = NULL;
    if (OB_ISNULL(source = candi_source_exprs.at(i)) || OB_ISNULL(target = candi_target_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(source), K(target), K(ret));
    } else if (!ObOptimizerUtil::is_expr_equivalent(source, target, *equal_sets)) {
      /*do nothing*/
    } else if (OB_FAIL(source_exprs.push_back(source))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(target_exprs.push_back(target))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to extract lossless columns", K(source_exprs), K(target_exprs));
  }
  if (NULL == input_equal_sets) {
    equal_sets->reuse();
  }
  return ret;
}

int ObTransformUtils::extract_lossless_mapping_columns(ObDMLStmt* stmt, const TableItem* source_table,
    const TableItem* target_table, const ObIArray<int64_t>& output_map, ObIArray<ObRawExpr*>& candi_source_exprs,
    ObIArray<ObRawExpr*>& candi_target_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> source_column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table), K(target_table), K(ret));
  } else if (OB_FAIL(stmt->get_column_items(source_table->table_id_, source_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < source_column_items.count(); i++) {
      ColumnItem& source_col = source_column_items.at(i);
      ColumnItem* target_col = NULL;
      if (target_table->is_basic_table()) {
        target_col = stmt->get_column_item_by_id(target_table->table_id_, source_col.column_id_);

      } else if (target_table->is_generated_table()) {
        int64_t pos = source_col.column_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_UNLIKELY(pos < 0 || pos >= output_map.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(pos), K(output_map.count()), K(ret));
        } else if (OB_INVALID_ID == output_map.at(pos)) {
          /*do nothing*/
        } else {
          target_col = stmt->get_column_item_by_id(target_table->table_id_, output_map.at(pos) + OB_APP_MIN_COLUMN_ID);
        }
      } else { /*do nothing*/
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(target_col)) {
        if (OB_ISNULL(source_col.expr_) || OB_ISNULL(target_col->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(source_col.expr_), K(target_col->expr_), K(ret));
        } else if (OB_FAIL(candi_source_exprs.push_back(source_col.expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(candi_target_exprs.push_back(target_col->expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to extract lossless mapping column", K(candi_source_exprs), K(candi_target_exprs));
    }
  }
  return ret;
}

int ObTransformUtils::adjust_agg_and_win_expr(ObSelectStmt* source_stmt, ObRawExpr*& source_expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(source_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt), K(source_expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (!source_expr->has_flag(CNT_AGG) && !source_expr->has_flag(CNT_WINDOW_FUNC)) {
    /*do nothing*/
  } else if (source_expr->is_aggr_expr()) {
    ObAggFunRawExpr* same_aggr_expr = NULL;
    if (OB_FAIL(source_stmt->check_and_get_same_aggr_item(source_expr, same_aggr_expr))) {
      LOG_WARN("failed to check and get same aggr item.", K(ret));
    } else if (same_aggr_expr != NULL) {
      source_expr = same_aggr_expr;
    } else if (OB_FAIL(source_stmt->add_agg_item(static_cast<ObAggFunRawExpr&>(*source_expr)))) {
      LOG_WARN("failed to add agg item", K(ret));
    } else { /*do nothing*/
    }
  } else if (source_expr->is_win_func_expr()) {
    if (OB_FAIL(source_stmt->add_window_func_expr(static_cast<ObWinFunRawExpr*>(source_expr)))) {
      LOG_WARN("failed to add window function expr", K(ret));
    } else { /*do nothing*/
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < source_expr->get_param_count(); i++) {
      if (OB_ISNULL(source_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(adjust_agg_and_win_expr(source_stmt, source_expr->get_param_expr(i))))) {
        LOG_WARN("failed to remove duplicated agg expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_group_by_consistent(ObSelectStmt* sel_stmt, bool& is_consistent)
{
  int ret = OB_SUCCESS;
  is_consistent = false;
  if (OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (sel_stmt->get_group_exprs().empty() && sel_stmt->get_rollup_exprs().empty()) {
    is_consistent = true;
  } else {
    is_consistent = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_consistent && i < sel_stmt->get_select_item_size(); i++) {
      ObRawExpr* expr = NULL;
      if (OB_ISNULL(expr = sel_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else if (expr->has_flag(CNT_AGG) || ObOptimizerUtil::find_equal_expr(sel_stmt->get_group_exprs(), expr) ||
                 ObOptimizerUtil::find_equal_expr(sel_stmt->get_rollup_exprs(), expr)) {
        /*do nothing*/
      } else {
        is_consistent = false;
      }
    }
  }
  return ret;
}

int ObTransformUtils::contain_select_ref(ObRawExpr* expr, bool& has)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    has = expr->has_flag(IS_SELECT_REF);
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(contain_select_ref(expr->get_param_expr(i), has))) {
      LOG_WARN("failed to check contain select ref expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::remove_select_items(ObTransformerCtx* ctx, const uint64_t table_id, ObSelectStmt& child_stmt,
    ObDMLStmt& upper_stmt, ObIArray<ObRawExpr*>& removed_select_exprs)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem>& select_items = child_stmt.get_select_items();
  ObSqlBitSet<> removed_idxs;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (ObOptimizerUtil::find_item(removed_select_exprs, select_items.at(i).expr_)) {
      ret = removed_idxs.add_member(i);
    }
  }
  if (OB_SUCC(ret) && remove_select_items(ctx, table_id, child_stmt, upper_stmt, removed_idxs)) {
    LOG_WARN("failed to remove select items", K(ret));
  }
  return ret;
}

int ObTransformUtils::remove_select_items(ObTransformerCtx* ctx, const uint64_t table_id, ObSelectStmt& child_stmt,
    ObDMLStmt& upper_stmt, ObSqlBitSet<>& removed_idxs)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  typedef ObSEArray<SelectItem, 8> SelectItems;
  HEAP_VAR(SelectItems, new_select_items)
  {
    ObSEArray<ColumnItem, 16> new_column_items;
    ObSEArray<ObRawExpr*, 16> removed_select_exprs;
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("argument invalid", K(ctx), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt.get_select_item_size(); i++) {
        if (removed_idxs.has_member(i)) {
          if (OB_FAIL(removed_select_exprs.push_back(child_stmt.get_select_item(i).expr_))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /*do nothing*/
          }
        } else {
          ColumnItem* column_item = NULL;
          if (OB_FAIL(new_select_items.push_back(child_stmt.get_select_item(i)))) {
            LOG_WARN("failed to push back select item", K(ret));
          } else if (OB_ISNULL(column_item = upper_stmt.get_column_item_by_id(table_id, i + OB_APP_MIN_COLUMN_ID))) {
            // if a select item is an assign expr (@a := 1), even if it is not used by upper stmt
            // we can not remove it.
            count++;
            LOG_WARN("fail to find column_item in upper_stmt");
          } else if (OB_FAIL(new_column_items.push_back(*column_item))) {
            LOG_WARN("failed to push back column items", K(ret));
          } else {
            new_column_items.at(new_column_items.count() - 1).set_ref_id(table_id, count + OB_APP_MIN_COLUMN_ID);
            count++;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(upper_stmt.remove_column_item(table_id))) {
          LOG_WARN("failed to remove column item", K(ret));
        } else if (OB_FAIL(upper_stmt.add_column_item(new_column_items))) {
          LOG_WARN("failed to add column item", K(ret));
        } else if (child_stmt.is_set_stmt()) {
          ret = SMART_CALL(remove_select_items(ctx, child_stmt, removed_idxs));
        } else if (OB_FAIL(child_stmt.get_select_items().assign(new_select_items))) {
          LOG_WARN("failed to assign select item", K(ret));
        } else if (child_stmt.get_select_items().empty() && OB_FAIL(create_dummy_select_item(child_stmt, ctx))) {
          LOG_WARN("failed to create dummy select item", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::remove_select_items(ObTransformerCtx* ctx, ObSelectStmt& union_stmt, ObSqlBitSet<>& removed_idxs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_UNLIKELY(!union_stmt.is_set_stmt()) || union_stmt.is_recursive_union()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument invalid", K(ret), K(ctx), K(union_stmt));
  } else {
    const int64_t select_count = union_stmt.get_select_item_size();
    int64_t new_select_count = -1;
    ObSelectStmt* child_stmt = NULL;
    typedef ObSEArray<SelectItem, 8> SelectItems;
    HEAP_VAR(SelectItems, new_select_items)
    {
      for (int64_t i = 0; OB_SUCC(ret) && i < union_stmt.get_set_query().count(); i++) {
        new_select_items.reuse();
        if (OB_ISNULL(child_stmt = union_stmt.get_set_query().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(child_stmt));
        } else if (OB_UNLIKELY(select_count != child_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected select item count", K(ret), K(select_count), K(child_stmt->get_select_item_size()));
        } else if (child_stmt->is_set_stmt()) {
          ret = SMART_CALL(remove_select_items(ctx, *child_stmt, removed_idxs));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < select_count; ++j) {
            if (removed_idxs.has_member(j)) {
              /*do nothing*/
            } else if (OB_FAIL(new_select_items.push_back(child_stmt->get_select_item(j)))) {
              LOG_WARN("failed to push back select items", K(ret));
            } else { /*do nothing*/
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(child_stmt->get_select_items().assign(new_select_items))) {
            LOG_WARN("failed to assign select item", K(ret));
          } else if (!child_stmt->get_select_items().empty()) {
            /*do nothing*/
          } else if (OB_FAIL(create_dummy_select_item(*child_stmt, ctx))) {
            LOG_WARN("failed to create dummy select item", K(ret));
          } else { /*do nothing*/
          }
        }

        if (OB_FAIL(ret)) {
        } else if (-1 == new_select_count) {
          new_select_count = child_stmt->get_select_item_size();
        } else if (OB_UNLIKELY(new_select_count != child_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "set-stmt select items no equal", K(ret), K(new_select_count), K(child_stmt->get_select_item_size()));
        }
      }
      new_select_items.reuse();
      int64_t idx = 0;
      ObRawExpr* set_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_count; i++) {
        if (removed_idxs.has_member(i)) {
          /*do nothing*/
        } else if (OB_FAIL(new_select_items.push_back(union_stmt.get_select_item(i)))) {
          LOG_WARN("failed to push back select items", K(ret));
        } else if (OB_ISNULL(set_expr = get_expr_in_cast(new_select_items.at(idx).expr_)) ||
                   OB_UNLIKELY(!set_expr->is_set_op_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected set expr", K(ret), K(set_expr));
        } else {
          static_cast<ObSetOpRawExpr*>(set_expr)->set_idx(idx++);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!new_select_items.empty()) {
        union_stmt.get_select_items().assign(new_select_items);
      } else if (OB_ISNULL(ctx->expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(ctx->expr_factory_));
      } else {
        union_stmt.get_select_items().reuse();
        ret = union_stmt.create_select_list_for_set_stmt(*ctx->expr_factory_);
      }
    }
  }
  return ret;
}

// not support set stmt
int ObTransformUtils::create_dummy_select_item(ObSelectStmt& stmt, ObTransformerCtx* ctx)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObConstRawExpr* const_expr = NULL;
  ObRawExpr* dummy_expr = NULL;
  int64_t const_value = 1;
  if (OB_ISNULL(ctx) || OB_ISNULL(expr_factory = ctx->expr_factory_) || OB_ISNULL(session_info = ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(expr_factory), K(session_info), K(ret));
  } else if (stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not create dummy select for set stmt", K(ret), K(stmt.get_set_op()));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, const_value, const_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_ISNULL(dummy_expr = static_cast<ObRawExpr*>(const_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(dummy_expr->formalize(session_info))) {
    LOG_WARN("Failed to formalize a new expr", K(ret));
  } else {
    SelectItem select_item;
    select_item.alias_name_ = "1";
    select_item.expr_name_ = "1";
    select_item.expr_ = dummy_expr;
    if (OB_FAIL(stmt.add_select_item(select_item))) {
      LOG_WARN("Failed to add dummy expr", K(ret));
    }
  }
  return ret;
}

// select exprs at the same pos in left_stmt/right_stmt have same result type
int ObTransformUtils::create_union_stmt(ObTransformerCtx* ctx, const bool is_distinct, ObSelectStmt* left_stmt,
    ObSelectStmt* right_stmt, ObSelectStmt*& union_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 2> child_stmts;
  if (OB_ISNULL(left_stmt) || OB_ISNULL(right_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(left_stmt), K(right_stmt), K(ret));
  } else if (OB_FAIL(child_stmts.push_back(left_stmt)) || OB_FAIL(child_stmts.push_back(right_stmt))) {
    LOG_WARN("failed to push back stmt", K(ret));
  } else if (OB_FAIL(create_union_stmt(ctx, is_distinct, child_stmts, union_stmt))) {
    LOG_WARN("failed to create union stmt", K(ret));
  }
  return ret;
}

// select exprs at the same pos in child_stmts have same result type
int ObTransformUtils::create_union_stmt(
    ObTransformerCtx* ctx, const bool is_distinct, ObIArray<ObSelectStmt*>& child_stmts, ObSelectStmt*& union_stmt)
{
  int ret = OB_SUCCESS;
  union_stmt = NULL;
  ObSelectStmt* temp_stmt = NULL;
  ObSelectStmt* child_stmt = NULL;
  const int64_t child_num = child_stmts.count();
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->expr_factory_) || OB_UNLIKELY(child_num < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(ret), K(ctx), K(child_stmts));
  } else if (child_num > 2 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100) {
    ObSEArray<ObSelectStmt*, 2> temp_child_stmts;
    temp_stmt = child_stmts.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < child_num; ++i) {
      temp_child_stmts.reuse();
      if (OB_ISNULL(temp_stmt) || OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null point", K(ret), K(temp_stmt), K(child_stmts.at(i)));
      } else if (OB_FAIL(temp_child_stmts.push_back(temp_stmt)) ||
                 OB_FAIL(temp_child_stmts.push_back(child_stmts.at(i)))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(create_union_stmt(ctx, is_distinct, temp_child_stmts, temp_stmt))) {
        LOG_WARN("failed to create union stmt", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      union_stmt = temp_stmt;
    }
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(temp_stmt))) {
    LOG_WARN("failed to create union stmt", K(temp_stmt), K(ret));
  } else if (OB_ISNULL(temp_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(temp_stmt));
  } else if (OB_FAIL(temp_stmt->get_set_query().assign(child_stmts))) {
    LOG_WARN("failed to assign child query", K(ret));
  } else if (OB_ISNULL(child_stmt = child_stmts.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(ret), K(child_stmt));
  } else {
    temp_stmt->assign_set_all();
    temp_stmt->assign_set_op(ObSelectStmt::UNION);
    temp_stmt->set_current_level(child_stmt->get_current_level());
    temp_stmt->set_parent_namespace_stmt(child_stmt->get_parent_namespace_stmt());
    temp_stmt->set_query_ctx(child_stmt->get_query_ctx());
    temp_stmt->set_sql_stmt(child_stmt->get_sql_stmt());
    temp_stmt->set_calc_found_rows(child_stmt->is_calc_found_rows());
    temp_stmt->set_stmt_id();
    if (is_distinct && OB_FALSE_IT(temp_stmt->assign_set_distinct())) {
    } else if (OB_FAIL(temp_stmt->create_select_list_for_set_stmt(*ctx->expr_factory_))) {
      LOG_WARN("failed to create select list for union", K(ret));
    } else if (OB_FAIL(ObStmtHint::add_whole_hint(temp_stmt))) {
      LOG_WARN("Add whole hint error", K(ret));
    } else {
      union_stmt = temp_stmt;
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::create_simple_view
 * split a stmt into two parts:
 * the first part processes table scan, join and filter, which is a spj stmt.
 * the second part processes distinct, group-by, order-by, window function, which is a non-spj stmt
 *
 * push_subquery: whether process some subqueries in the first part, maybe we can do ja-rewrite on the first part
 */
int ObTransformUtils::create_simple_view(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSelectStmt*& view_stmt, bool push_subquery)
{
  int ret = OB_SUCCESS;
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObSEArray<ObRawExpr*, 4> select_list;
  ObSEArray<ObRawExpr*, 4> post_join_exprs;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(session_info = ctx->session_info_) ||
      OB_ISNULL(stmt_factory = ctx->stmt_factory_) || OB_ISNULL(expr_factory = ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(expr_factory), K(stmt));
  } else if (stmt->is_set_stmt() || stmt->is_hierarchical_query() || !stmt->is_sel_del_upd()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not create spj stmt",
        K(ret),
        K(stmt->is_set_stmt()),
        K(stmt->is_hierarchical_query()),
        K(stmt->is_sel_del_upd()));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(view_stmt->ObDMLStmt::assign(*stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else {
    view_stmt->set_stmt_type(stmt::T_SELECT);
    // 1. handle table, columns, from
    // dml_stmt: from table, semi table, joined table
    stmt->reset_table_items();
    stmt->reset_CTE_table_items();
    stmt->get_joined_tables().reuse();
    stmt->get_semi_infos().reuse();
    stmt->get_column_items().reuse();
    stmt->clear_from_items();
    stmt->get_part_exprs().reset();
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash", K(ret));
    }
  }
  // 2. handle where conditions
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 8> norm_conds;
    ObSEArray<ObRawExpr*, 8> rownum_conds;
    if (OB_FAIL(classify_rownum_conds(*stmt, norm_conds, rownum_conds))) {
      LOG_WARN("failed to classify rownum conditions", K(ret));
    } else if (OB_FAIL(stmt->get_condition_exprs().assign(rownum_conds))) {
      LOG_WARN("failed to assign rownum conditions", K(ret));
    } else if (OB_FAIL(view_stmt->get_condition_exprs().assign(norm_conds))) {
      LOG_WARN("failed to assign normal conditions", K(ret));
    }
  }
  // 3. handle clauses processed by the upper_stmt
  if (OB_SUCC(ret)) {
    // consider following parts:
    // select: group-by, rollup, select subquery, window function, distinct, sequence,
    //         order by, limit, select into
    // update: order-by, limit, sequence, assignments, returning
    // delete: order-by, limit, returning
    view_stmt->get_order_items().reset();
    view_stmt->set_limit_offset(NULL, NULL);
    view_stmt->set_limit_percent_expr(NULL);
    view_stmt->set_fetch_with_ties(false);
    view_stmt->set_has_fetch(false);
    view_stmt->clear_sequence();
    view_stmt->set_select_into(NULL);
    view_stmt->get_pseudo_column_like_exprs().reset();
  }
  // 4. let the view_stmt process some subqueries
  if (OB_SUCC(ret) && push_subquery) {
    // TODO, subqueries in order by, assignment can also be processed by view
    if (OB_FAIL(ObTransformUtils::get_post_join_exprs(stmt, post_join_exprs))) {
      LOG_WARN("failed to get additional push down exprs", K(ret));
    }
  }
  // 5. finish creating the child stmts
  if (OB_SUCC(ret)) {
    // create select list
    ObSEArray<ObRawExpr*, 4> columns;
    ObSEArray<ObQueryRefRawExpr*, 4> query_refs;
    ObRelIds rel_ids;
    ObSqlBitSet<> from_tables;
    ObSEArray<ObRawExpr*, 16> shared_exprs;
    if (OB_FAIL(get_from_tables(*view_stmt, rel_ids))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(from_tables.add_members2(rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(view_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(extract_table_exprs(*view_stmt, columns, from_tables, select_list))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(extract_query_ref_expr(post_join_exprs, query_refs))) {
      LOG_WARN("failed to extract query refs", K(ret));
    } else if (OB_FAIL(append(select_list, query_refs))) {
      LOG_WARN("failed to append query ref exprs", K(ret));
    } else if (OB_FAIL(extract_shared_expr(stmt, view_stmt, shared_exprs))) {
      LOG_WARN("failed to extract shared expr", K(ret));
    } else if (OB_FAIL(append(select_list, shared_exprs))) {
      LOG_WARN("failed to append shared exprs", K(ret));
    } else if (OB_FAIL(create_select_item(*(ctx->allocator_), select_list, view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_stmt_parent(stmt, view_stmt))) {
      LOG_WARN("failed to adjust sbuquery stmt parent", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_statement_id())) {
      LOG_WARN("failed to adjust stmt id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (view_stmt->get_select_item_size() != 0) {
      // do nothing
    } else if (OB_FAIL(create_dummy_select_item(*view_stmt, ctx))) {
      LOG_WARN("failed to create dummy select item", K(ret));
    }
  }
  // 5. link upper stmt and view stmt
  TableItem* view_table_item = NULL;
  ObSEArray<ObRawExpr*, 4> new_cols;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), view_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to remove subqueries", K(ret));
    } else if (OB_FAIL(add_new_table_item(ctx, stmt, view_stmt, view_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(view_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt->add_from_item(view_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(create_columns_for_view(ctx, *view_table_item, stmt, new_cols))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (!stmt->is_delete_stmt() && !stmt->is_update_stmt()) {
      // do nothing
    } else if (OB_FAIL(adjust_updatable_view(*expr_factory, static_cast<ObDelUpdStmt*>(stmt), *view_table_item))) {
      LOG_WARN("failed to adjust updatable view", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (view_stmt->has_for_update() && lib::is_oracle_mode()) {
      view_table_item->for_update_ = true;
    }
  }

  // 6. do replace and formalize
  if (OB_SUCC(ret)) {
    stmt->get_table_items().pop_back();
    if (OB_FAIL(stmt->replace_inner_stmt_expr(select_list, new_cols))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(stmt->get_table_items().push_back(view_table_item))) {
      LOG_WARN("failed to push back view table item", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::adjust_updatable_view
 * create part exprs for updatable view
   keep and rename part exprs for update/delete stmt
   because they are required by global index update
 * @param expr_factory
 * @param del_upd_stmt
 * @param view_table_item
 * @return
 */
int ObTransformUtils::adjust_updatable_view(
    ObRawExprFactory& expr_factory, ObDelUpdStmt* del_upd_stmt, TableItem& view_table_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::PartExprItem, 4> part_exprs;
  ObSelectStmt* view_stmt = NULL;
  if (OB_ISNULL(view_stmt = view_table_item.ref_query_) || OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(view_table_item), K(del_upd_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt->get_all_table_columns().count(); ++i) {
    TableColumns& tab_cols = del_upd_stmt->get_all_table_columns().at(i);
    uint64_t loc_table_id = tab_cols.index_dml_infos_.at(0).loc_table_id_;

    // update index dml infos table id
    for (int64_t j = 0; OB_SUCC(ret) && j < tab_cols.index_dml_infos_.count(); ++j) {
      tab_cols.index_dml_infos_.at(j).table_id_ = view_table_item.table_id_;
    }
    // create partition exprs for index dml infos
    if (OB_FAIL(view_stmt->get_part_expr_items(loc_table_id, part_exprs))) {
      LOG_WARN("failed to get part expr items", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_exprs.count(); ++i) {
      ObDMLStmt::PartExprItem part_item;
      if (OB_FAIL(part_item.deep_copy(expr_factory, part_exprs.at(i), COPY_REF_DEFAULT))) {
        LOG_WARN("failed to deep copy part expr item", K(ret));
      } else {
        part_exprs.at(i) = part_item;
      }
    }
    if (OB_SUCC(ret) && !part_exprs.empty()) {
      if (OB_FAIL(del_upd_stmt->set_part_expr_items(part_exprs))) {
        LOG_WARN("failed to set part expr items", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::push_down_groupby
 * SELECT ... FROM (SELECT ... FROM ... WHERE ...) GROUP BY ... HAVING ...
 * SELECT ... FROM (SELECT ..., aggs, FROM ... WHERE ... GROUP BY ...) WHERE ...
 * @return
 */
int ObTransformUtils::push_down_groupby(ObTransformerCtx* ctx, ObSelectStmt* stmt, TableItem* view_table)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* view_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> view_select_list;
  ObSEArray<ObRawExpr*, 4> view_column_list;
  ObSEArray<ObRawExpr*, 4> view_aggr_list;
  ObSEArray<ObRawExpr*, 4> view_aggr_cols;
  if (OB_ISNULL(ctx) || OB_ISNULL(view_table) || OB_ISNULL(view_stmt = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view stmt is invalid", K(ret), K(view_table), K(view_stmt), K(ctx));
  } else if (OB_FAIL(view_stmt->get_group_exprs().assign(stmt->get_group_exprs()))) {
    LOG_WARN("failed to push down group exprs", K(ret));
  } else if (OB_FAIL(view_stmt->get_aggr_items().assign(stmt->get_aggr_items()))) {
    LOG_WARN("failed to push down aggregation exprs", K(ret));
  } else if (OB_FAIL(append(stmt->get_condition_exprs(), stmt->get_having_exprs()))) {
    LOG_WARN("failed to push back having exprs into where", K(ret));
  } else if (OB_FAIL(stmt->get_view_output(*view_table, view_select_list, view_column_list))) {
    LOG_WARN("failed to get view output", K(ret));
  } else if (OB_FAIL(replace_exprs(view_column_list, view_select_list, view_stmt->get_group_exprs()))) {
    LOG_WARN("failed to replace group exprs", K(ret));
  } else if (OB_FAIL(replace_exprs(view_column_list, view_select_list, view_stmt->get_aggr_items()))) {
    LOG_WARN("failed to replace aggregation exprs", K(ret));
  } else if (OB_FAIL(append(view_aggr_list, view_stmt->get_group_exprs()))) {
    LOG_WARN("failed to append aggr list", K(ret));
  } else if (OB_FAIL(append(view_aggr_list, view_stmt->get_aggr_items()))) {
    LOG_WARN("failed to append aggr list", K(ret));
  } else if (OB_FAIL(create_columns_for_view(ctx, *view_table, stmt, view_aggr_list, view_aggr_cols))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else {
    stmt->get_group_exprs().reset();
    stmt->get_aggr_items().reset();
    stmt->get_having_exprs().reset();
    // tricky: skip the view stmt when replacing exprs
    view_table->type_ = TableItem::BASE_TABLE;
    if (OB_FAIL(stmt->replace_inner_stmt_expr(view_aggr_list, view_aggr_cols))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    }
    view_table->type_ = TableItem::GENERATED_TABLE;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::push_down_vector_assign(
    ObTransformerCtx* ctx, ObUpdateStmt* stmt, ObAliasRefRawExpr* root_expr, TableItem* view_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> alias_exprs;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(root_expr) || OB_ISNULL(view_table) ||
      OB_UNLIKELY(!root_expr->is_ref_query_output()) || OB_ISNULL(root_expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(root_expr), K(view_table));
  } else if (OB_FAIL(stmt->get_vector_assign_values(
                 static_cast<ObQueryRefRawExpr*>(root_expr->get_param_expr(0)), alias_exprs))) {
    LOG_WARN("failed to get vector assign values", K(ret));
  } else if (OB_FAIL(create_columns_for_view(ctx, *view_table, stmt, alias_exprs, column_exprs))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else {
    view_table->type_ = TableItem::BASE_TABLE;
    if (OB_FAIL(stmt->replace_inner_stmt_expr(alias_exprs, column_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    }
    view_table->type_ = TableItem::GENERATED_TABLE;
  }
  if (OB_SUCC(ret)) {
    // clear update_set flag
    if (OB_FAIL(stmt->check_assign())) {
      LOG_WARN("failed to check vector assign", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::create_stmt_with_generated_table(
    ObTransformerCtx* ctx, ObSelectStmt* child_stmt, ObSelectStmt*& parent_stmt)
{
  int ret = OB_SUCCESS;
  TableItem* new_table_item = NULL;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  parent_stmt = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) || OB_ISNULL(child_stmt) || OB_ISNULL(ctx->stmt_factory_) ||
      OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child_stmt), K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(parent_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(parent_stmt->set_query_ctx(child_stmt->get_query_ctx()))) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx, parent_stmt, child_stmt, new_table_item))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_ISNULL(new_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->add_from_item(new_table_item->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(parent_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    parent_stmt->set_current_level(child_stmt->get_current_level());
    if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx, *new_table_item, parent_stmt, column_exprs))) {
      LOG_WARN("failed to create column items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_, column_exprs, parent_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else {
      parent_stmt->set_select_into(child_stmt->get_select_into());
      child_stmt->set_select_into(NULL);  // move `child_stmt select into` into parent_stmt
      parent_stmt->set_parent_namespace_stmt(child_stmt->get_parent_namespace_stmt());
      if (OB_FAIL(parent_stmt->formalize_stmt(ctx->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else if (OB_FAIL(ObDMLStmt::copy_query_hint(child_stmt, parent_stmt))) {
        LOG_WARN("faield to copy query hint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::create_stmt_with_joined_table(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, JoinedTable* joined_table, ObSelectStmt*& simple_stmt)
{
  int ret = OB_SUCCESS;
  simple_stmt = NULL;
  ObSEArray<TableItem*, 8> table_items;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(ctx->allocator_) ||
      OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(simple_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(simple_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(simple_stmt->set_query_ctx(stmt->get_query_ctx()))) {
    /*do nothing*/
  } else if (OB_FAIL(extract_table_items(joined_table, table_items))) {
    LOG_WARN("failed to extract table items", K(ret));
  } else if (OB_FAIL(add_table_item(simple_stmt, table_items))) {
    LOG_WARN("failed to add table item", K(ret));
  } else if (OB_FAIL(simple_stmt->add_joined_table(joined_table))) {
    LOG_WARN("failed to add join table", K(ret));
  } else if (OB_FAIL(simple_stmt->add_from_item(joined_table->table_id_, true))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(simple_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(simple_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    simple_stmt->set_current_level(stmt->get_current_level());
    simple_stmt->set_parent_namespace_stmt(stmt->get_parent_namespace_stmt());
    ObSEArray<ObDMLStmt::PartExprItem, 8> part_items;
    ObSEArray<ColumnItem, 8> column_items;
    if (OB_FAIL(stmt->get_part_expr_items(joined_table->single_table_ids_, part_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(simple_stmt->set_part_expr_items(part_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_column_items(joined_table->single_table_ids_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(simple_stmt->add_column_item(column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(simple_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_, column_items, simple_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(simple_stmt->get_stmt_hint().assign(stmt->get_stmt_hint()))) {
      LOG_WARN("failed to assign hint", K(ret));
    } else if (OB_FAIL(simple_stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::create_stmt_with_basic_table(
    ObTransformerCtx* ctx, ObDMLStmt* stmt, TableItem* table, ObSelectStmt*& simple_stmt)
{
  int ret = OB_SUCCESS;
  simple_stmt = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(ctx->allocator_) ||
      OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!table->is_basic_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(ret), K(table->type_));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(simple_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(simple_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(simple_stmt->set_query_ctx(stmt->get_query_ctx()))) {
    /*do nothing*/
  } else if (OB_FAIL(add_table_item(simple_stmt, table))) {
    LOG_WARN("failed to add table item", K(ret));
  } else if (OB_FAIL(simple_stmt->add_from_item(table->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(simple_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(simple_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    simple_stmt->set_current_level(stmt->get_current_level());
    simple_stmt->set_parent_namespace_stmt(stmt->get_parent_namespace_stmt());
    ObSEArray<ObDMLStmt::PartExprItem, 8> part_items;
    ObSEArray<ColumnItem, 8> column_items;
    if (OB_FAIL(stmt->get_part_expr_items(table->table_id_, part_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(simple_stmt->set_part_expr_items(part_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_column_items(table->table_id_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(simple_stmt->add_column_item(column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(simple_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_, column_items, simple_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(simple_stmt->get_stmt_hint().assign(stmt->get_stmt_hint()))) {
      LOG_WARN("failed to assign hint", K(ret));
    } else if (OB_FAIL(simple_stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::create_view_with_table(
    ObDMLStmt* stmt, ObTransformerCtx* ctx, TableItem* table, TableItem*& view_table)
{
  int ret = OB_SUCCESS;
  view_table = NULL;
  TableItem* new_table = NULL;
  ObSEArray<ObRawExpr*, 8> old_column_exprs;
  ObSEArray<ObRawExpr*, 8> new_column_exprs;
  ObSelectStmt* view_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx), K(table));
  } else if (table->is_generated_table()) {
    ret = create_stmt_with_generated_table(ctx, table->ref_query_, view_stmt);
  } else if (table->is_basic_table()) {
    ret = create_stmt_with_basic_table(ctx, stmt, table, view_stmt);
  } else if (table->is_joined_table()) {
    ret = create_stmt_with_joined_table(ctx, stmt, static_cast<JoinedTable*>(table), view_stmt);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type", K(ret), K(table->type_));
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 8> tmp_column_exprs;
    ObSEArray<ObRawExpr*, 8> tmp_select_exprs;
    if (OB_FAIL(add_new_table_item(ctx, stmt, view_stmt, new_table))) {
      LOG_WARN("failed to add table items", K(ret));
    } else if (OB_FAIL(create_columns_for_view(ctx, *new_table, stmt, tmp_column_exprs))) {
      LOG_WARN("failed to create column items", K(ret));
    } else if (OB_FAIL(convert_column_expr_to_select_expr(tmp_column_exprs, *view_stmt, tmp_select_exprs))) {
      LOG_WARN("failed to convert column expr to select expr", K(ret));
    } else {
      ObColumnRefRawExpr* col_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_select_exprs.count(); ++i) {
        if (OB_NOT_NULL(tmp_select_exprs.at(i)) && tmp_select_exprs.at(i)->is_column_ref_expr()) {
          col_expr = static_cast<ObColumnRefRawExpr*>(tmp_select_exprs.at(i));
          uint64_t table_id = table->is_generated_table() ? table->table_id_ : col_expr->get_table_id();
          if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(table_id, col_expr->get_column_id()))) {
          } else if (OB_FAIL(old_column_exprs.push_back(col_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(new_column_exprs.push_back(tmp_column_exprs.at(i)))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected view stmt select expr", K(ret), K(*view_stmt));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(replace_table_in_stmt(stmt, new_table, table))) {
      LOG_WARN("failed to replace table in stmt", K(ret));
    } else if (table->is_basic_table() && OB_FAIL(stmt->remove_part_expr_items(table->table_id_))) {
      LOG_WARN("failed to remove part expr items", K(ret));
    } else if (table->is_joined_table() &&
               OB_FAIL(stmt->remove_part_expr_items(static_cast<JoinedTable*>(table)->single_table_ids_))) {
      LOG_WARN("failed to remove part expr items", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_stmt_parent(stmt, view_stmt))) {
      LOG_WARN("failed to adjust subquery stmt parent", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(new_table))) {
      LOG_WARN("failed to remove table item", K(ret));
    } else if (OB_FAIL(stmt->replace_inner_stmt_expr(old_column_exprs, new_column_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(stmt->get_table_items().push_back(new_table))) {
      LOG_WARN("failed to push back table item", K(ret));
    } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      view_table = new_table;
    }
  }
  return ret;
}

int ObTransformUtils::can_push_down_filter_to_table(TableItem& table, bool& can_push)
{
  int ret = OB_SUCCESS;
  can_push = false;
  ObSelectStmt* ref_query = NULL;
  bool has_rownum = false;
  if (!table.is_generated_table()) {
    can_push = false;
  } else if (OB_ISNULL(ref_query = table.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ref_query));
  } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (!has_rownum) {
    can_push = !(ref_query->is_set_stmt() || ref_query->is_hierarchical_query() || ref_query->has_limit() ||
                 ref_query->has_sequence() || ref_query->is_contains_assignment() || ref_query->has_window_function() ||
                 ref_query->has_group_by() || ref_query->has_rollup());
  }
  return ret;
}

int ObTransformUtils::pushdown_semi_info_right_filter(ObDMLStmt* stmt, ObTransformerCtx* ctx, SemiInfo* semi_info)
{
  int ret = OB_SUCCESS;
  TableItem* right_table = NULL;
  ObSEArray<ObRawExpr*, 4> right_filters;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(ctx), K(semi_info));
  } else if (OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(right_table));
  } else if (OB_FAIL(extract_table_exprs(*stmt, semi_info->semi_conditions_, *right_table, right_filters))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (right_filters.empty()) {
    /* do nothing */
  } else if (OB_FAIL(pushdown_semi_info_right_filter(stmt, ctx, semi_info, right_filters))) {
    LOG_WARN("failed to pushdown semi info right filter", K(ret));
  }
  return ret;
}

// pushdown right table filter in semi condition:
// 1. if right table is a basic table, create a generate table.
// 2. pushdown the right table filters into the generate table.
int ObTransformUtils::pushdown_semi_info_right_filter(
    ObDMLStmt* stmt, ObTransformerCtx* ctx, SemiInfo* semi_info, ObIArray<ObRawExpr*>& right_filters)
{
  int ret = OB_SUCCESS;
  TableItem* right_table = NULL;
  ObSelectStmt* child_stmt = NULL;
  bool can_push = false;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(ctx), K(semi_info));
  } else if (right_filters.empty()) {
    /* do nothing */
  } else if (OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(right_table));
  } else if (OB_FAIL(can_push_down_filter_to_table(*right_table, can_push))) {
    LOG_WARN("failed to check can push down", K(ret), K(*right_table));
  } else if (!can_push && OB_FAIL(create_view_with_table(stmt, ctx, right_table, right_table))) {
    LOG_WARN("failed to create view with table", K(ret));
  } else if (OB_ISNULL(right_table) || OB_UNLIKELY(!right_table->is_generated_table()) ||
             OB_ISNULL(child_stmt = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected right table", K(ret), K(right_table), K(child_stmt));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_info->semi_conditions_, right_filters))) {
    LOG_WARN("failed to remove item", K(ret));
  } else if (OB_FAIL(child_stmt->add_condition_exprs(right_filters))) {
    LOG_WARN("failed to add condotion exprs", K(ret));
  } else if (OB_FAIL(extract_query_ref_expr(right_filters, child_stmt->get_subquery_exprs()))) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(stmt->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(child_stmt->adjust_subquery_stmt_parent(stmt, child_stmt))) {
    LOG_WARN("failed to adjust subquery stmt parent", K(ret));
  } else if (OB_FAIL(stmt->get_column_exprs(semi_info->right_table_id_, old_column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(convert_column_expr_to_select_expr(old_column_exprs, *child_stmt, new_column_exprs))) {
    LOG_WARN("failed to conver column exprs", K(ret));
  } else if (OB_FAIL(child_stmt->replace_inner_stmt_expr(old_column_exprs, new_column_exprs))) {
    LOG_WARN("failed to replace stmt exprs", K(ret));
  } else if (OB_FAIL(child_stmt->formalize_stmt(ctx->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformUtils::replace_table_in_stmt(ObDMLStmt* stmt, TableItem* other_table, TableItem* current_table)
{
  int ret = OB_SUCCESS;
  ObRelIds rel_ids;
  ObSEArray<uint64_t, 4> table_ids;
  int64_t from_item_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!other_table->is_basic_table() && !other_table->is_generated_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(ret), K(*other_table));
  } else if (OB_FAIL(remove_tables_from_stmt(stmt, current_table, table_ids))) {
    LOG_WARN("failed to remove tables from stmt", K(ret));
  } else if (OB_FAIL(replace_table_in_semi_infos(stmt, other_table, current_table))) {
    LOG_WARN("failed to replace table in semi infos", K(ret));
  } else if (OB_FAIL(replace_table_in_joined_tables(stmt, other_table, current_table))) {
    LOG_WARN("failed to replace table in joined tables", K(ret));
  } else if (FALSE_IT(from_item_idx = stmt->get_from_item_idx(current_table->table_id_))) {
  } else if (from_item_idx < 0 || from_item_idx > stmt->get_from_item_size()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->remove_from_item(current_table->table_id_))) {
    LOG_WARN("failed to remove from item", K(ret));
  } else if (OB_FAIL(stmt->add_from_item(other_table->table_id_, other_table->is_joined_table()))) {
    LOG_WARN("failed to add from item", K(ret));
  }
  return ret;
}

int ObTransformUtils::remove_tables_from_stmt(ObDMLStmt* stmt, TableItem* table_item, ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(stmt), K(table_item), K(ret));
  } else if (table_item->type_ == TableItem::JOINED_TABLE) {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(SMART_CALL(remove_tables_from_stmt(stmt, joined_table->left_table_, table_ids)))) {
      LOG_WARN("failed to remove tables from stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(remove_tables_from_stmt(stmt, joined_table->right_table_, table_ids)))) {
      LOG_WARN("failed to remove tables from stmt", K(ret));
    } else { /*do nothing.*/
    }
  } else if (OB_FAIL(stmt->remove_table_item(table_item))) {
    LOG_WARN("failed to remove table items.", K(ret));
  } else if (OB_FAIL(stmt->remove_column_item(table_item->table_id_))) {
    LOG_WARN("failed to remove column items.", K(ret));
  } else if (OB_FAIL(table_ids.push_back(table_item->table_id_))) {
    LOG_WARN("failed to push back table id", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

// replace current_table with other_table, other_table is a basic/generate table.
int ObTransformUtils::replace_table_in_semi_infos(
    ObDMLStmt* stmt, const TableItem* other_table, const TableItem* current_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(other_table->is_basic_table() && other_table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected other table item", K(ret), K(other_table->type_));
  } else if (current_table->is_joined_table()) {
    ret = table_ids.assign(static_cast<const JoinedTable*>(current_table)->single_table_ids_);
  } else {
    ret = table_ids.push_back(current_table->table_id_);
  }
  if (OB_SUCC(ret)) {
    ObIArray<SemiInfo*>& semi_infos = stmt->get_semi_infos();
    bool happend = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (current_table->table_id_ == semi_infos.at(i)->right_table_id_) {
        semi_infos.at(i)->right_table_id_ = other_table->table_id_;
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_infos.at(i)->left_table_ids_, table_ids, &happend))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (!happend) {
        /* do nothing */
      } else if (OB_FAIL(add_var_to_array_no_dup(semi_infos.at(i)->left_table_ids_, other_table->table_id_))) {
        LOG_WARN("failed to add table id", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_table_in_joined_tables(ObDMLStmt* stmt, TableItem* other_table, TableItem* current_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<JoinedTable*, 4> new_joined_tables;
    ObIArray<JoinedTable*>& joined_tables = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables.count(); ++i) {
      if (OB_ISNULL(joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (current_table == joined_tables.at(i)) {
        if (other_table->is_joined_table() &&
            OB_FAIL(new_joined_tables.push_back(static_cast<JoinedTable*>(other_table)))) {
          LOG_WARN("failed to push back table", K(ret));
        } else { /*do nothing*/
        }
      } else if (OB_FAIL(replace_table_in_joined_tables(joined_tables.at(i), other_table, current_table))) {
        LOG_WARN("failed to replace table in joined tables", K(ret));
      } else if (OB_FAIL(new_joined_tables.push_back(joined_tables.at(i)))) {
        LOG_WARN("failed to push back table", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(stmt->get_joined_tables().assign(new_joined_tables))) {
      LOG_WARN("failed to assign joined tables", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::replace_table_in_joined_tables(TableItem* table, TableItem* other_table, TableItem* current_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table);
    TableItem* left_table = NULL;
    TableItem* right_table = NULL;
    joined_table->single_table_ids_.reset();
    if (OB_ISNULL(left_table = joined_table->left_table_) || OB_ISNULL(right_table = joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (current_table == left_table) {
      joined_table->left_table_ = other_table;
    } else if (OB_FAIL(SMART_CALL(replace_table_in_joined_tables(left_table, other_table, current_table)))) {
      LOG_WARN("failed to replace table in joined tables", K(ret));
    } else if (current_table == right_table) {
      joined_table->right_table_ = other_table;
    } else if (OB_FAIL(SMART_CALL(replace_table_in_joined_tables(right_table, other_table, current_table)))) {
      LOG_WARN("failed to replace table in joined tables", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_joined_table_single_table_ids(*joined_table, *joined_table->right_table_))) {
      LOG_WARN("add joined table single table ids failed", K(ret));
    } else if (OB_FAIL(add_joined_table_single_table_ids(*joined_table, *joined_table->left_table_))) {
      LOG_WARN("add joined table single table ids failed", K(ret));
    }
  }
  return ret;
}

/**
 * @brief get_output_rel_ids
 * get ObRelIds for all tables in from items
 */
int ObTransformUtils::get_from_tables(const ObDMLStmt& stmt, ObRelIds& output_rel_ids)
{
  int ret = OB_SUCCESS;
  int32_t bit_id = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    const FromItem& from = stmt.get_from_item(i);
    if (from.is_joined_) {
      const JoinedTable* table = stmt.get_joined_table(from.table_id_);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get joined table", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < table->single_table_ids_.count(); ++j) {
        uint64_t table_id = table->single_table_ids_.at(j);
        if (OB_UNLIKELY(OB_INVALID_INDEX == (bit_id = stmt.get_table_bit_index(table_id)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid table bit index", K(ret), K(table_id), K(bit_id));
        } else if (OB_FAIL(output_rel_ids.add_member(bit_id))) {
          LOG_WARN("failed to add member", K(ret));
        }
      }
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == (bit_id = stmt.get_table_bit_index(from.table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table bit index", K(ret), K(from.table_id_), K(bit_id));
    } else if (OB_FAIL(output_rel_ids.add_member(bit_id))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::classify_rownum_conds(
    ObDMLStmt& stmt, ObIArray<ObRawExpr*>& spj_conds, ObIArray<ObRawExpr*>& other_conds)
{
  int ret = OB_SUCCESS;
  if (stmt.is_select_stmt()) {
    ObSelectStmt& sel_stmt = static_cast<ObSelectStmt&>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_select_item_size(); ++i) {
      ObRawExpr* sel_expr = NULL;
      if (OB_ISNULL(sel_expr = sel_stmt.get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret));
      } else if (!sel_expr->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(sel_expr->add_flag(IS_SELECT_REF))) {
        LOG_WARN("failed to add flag", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    ObRawExpr* cond_expr = NULL;
    bool has = false;
    if (OB_ISNULL(cond_expr = stmt.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond_expr->has_flag(CNT_ROWNUM)) {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into upper stmt", K(ret));
      }
    } else if (OB_FAIL(ObTransformUtils::contain_select_ref(cond_expr, has))) {
      LOG_WARN("failed to contain select ref", K(ret));
    } else if (has) {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to push back condition expr", K(ret));
      }
    } else if (OB_FAIL(spj_conds.push_back(cond_expr))) {
      LOG_WARN("failed to add condition expr into spj stmt", K(ret));
    }
  }
  if (stmt.is_select_stmt()) {
    ObSelectStmt& sel_stmt = static_cast<ObSelectStmt&>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_select_item_size(); ++i) {
      ObRawExpr* sel_expr = NULL;
      if (OB_ISNULL(sel_expr = sel_stmt.get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret));
      } else if (!sel_expr->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(sel_expr->clear_flag(IS_SELECT_REF))) {
        LOG_WARN("failed to add flag", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief rebuild_select_items
 * rebuild select items with all column item exprs whose relation_ids is in
 * input parameter output_rel_ids
 */
int ObTransformUtils::rebuild_select_items(ObSelectStmt& stmt, ObRelIds& output_rel_ids)
{
  int ret = OB_SUCCESS;
  SelectItem sel_item;
  stmt.get_select_items().reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
    ColumnItem* column_item = stmt.get_column_item(i);
    if (OB_ISNULL(column_item) || OB_ISNULL(column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret), K(column_item));
    } else if (column_item->expr_->is_explicited_reference() &&
               column_item->expr_->get_relation_ids().is_subset(output_rel_ids)) {
      sel_item.expr_name_ = column_item->column_name_;
      sel_item.alias_name_ = column_item->column_name_;
      sel_item.expr_ = column_item->expr_;
      if (OB_FAIL(stmt.add_select_item(sel_item))) {
        LOG_WARN("failed to add select item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::create_select_item_for_subquery(
    ObSelectStmt& stmt, ObSelectStmt*& child_stmt, ObIAllocator& alloc, ObIArray<ObRawExpr*>& query_ref_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr*, 4> tmp_query_ref_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr* cur_expr = stmt.get_select_item(i).expr_;
    if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(cur_expr, tmp_query_ref_exprs))) {
      LOG_WARN("failed to extract query ref expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append(query_ref_exprs, tmp_query_ref_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_ref_exprs.count(); ++i) {
    if (OB_FAIL(create_select_item(alloc, query_ref_exprs.at(i), child_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::right_join_to_left(ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else {
    int64_t N = stmt->get_joined_tables().count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      JoinedTable* cur_joined = stmt->get_joined_tables().at(i);
      if (OB_FAIL(ObTransformUtils::change_join_type(cur_joined))) {
        LOG_WARN("failed to change right outer join to left", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::change_join_type(TableItem* joined_table)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (NULL == joined_table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (joined_table->is_joined_table()) {
    JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table);
    if (RIGHT_OUTER_JOIN == cur_joined->joined_type_) {
      TableItem* l_child = cur_joined->left_table_;
      cur_joined->left_table_ = cur_joined->right_table_;
      cur_joined->right_table_ = l_child;
      cur_joined->joined_type_ = LEFT_OUTER_JOIN;
    }
    if (OB_FAIL(SMART_CALL(change_join_type(cur_joined->left_table_)))) {
      LOG_WARN("failed to change left child type", K(ret));
    } else if (OB_FAIL(SMART_CALL(change_join_type(cur_joined->right_table_)))) {
      LOG_WARN("failed to change right child type", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::build_const_expr_for_count(
    ObRawExprFactory& expr_factory, const int64_t value, ObConstRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (1 != value && 0 != value) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected const value for count", K(ret), K(value));
  } else if (!lib::is_oracle_mode()) {
    ret = ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, value, expr);
  } else if (1 == value) {
    ret =
        ObRawExprUtils::build_const_number_expr(expr_factory, ObNumberType, number::ObNumber::get_positive_one(), expr);
  } else if (0 == value) {
    ret = ObRawExprUtils::build_const_number_expr(expr_factory, ObNumberType, number::ObNumber::get_zero(), expr);
  }
  return ret;
}

// case expr is not null then then_value else default_expr end
int ObTransformUtils::build_case_when_expr(ObDMLStmt& stmt, ObRawExpr* expr, ObRawExpr* then_expr,
    ObRawExpr* default_expr, ObRawExpr*& out_expr, ObTransformerCtx* ctx)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr* case_expr = NULL;
  ObOpRawExpr* is_not_expr = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), K(ctx));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_OP_CASE, case_expr))) {
    LOG_WARN("failed to create case expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx, &stmt, expr, is_not_expr))) {
    LOG_WARN("failed to build is not null expr", K(ret));
  } else if (OB_FAIL(case_expr->add_when_param_expr(is_not_expr))) {
    LOG_WARN("failed to add when param expr", K(ret));
  } else if (OB_FAIL(case_expr->add_then_param_expr(then_expr))) {
    LOG_WARN("failed to add then expr", K(ret));
  } else if (FALSE_IT(case_expr->set_default_param_expr(default_expr))) {
    // do nothing
  } else if (OB_FAIL(case_expr->formalize(ctx->session_info_))) {
    LOG_WARN("failed to formalize case expr", K(ret));
  } else if (OB_FAIL(case_expr->pull_relation_id_and_levels(stmt.get_current_level()))) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  } else {
    out_expr = case_expr;
  }
  return ret;
}

//  select ... from (select ... limit a1 offset b1) limit a2 offset b2;
//  select ... from ... limit min(a1 - b2, a2) offset b1 + b2;
int ObTransformUtils::merge_limit_offset(ObTransformerCtx* ctx, ObRawExpr* view_limit, ObRawExpr* upper_limit,
    ObRawExpr* view_offset, ObRawExpr* upper_offset, ObRawExpr*& limit_expr, ObRawExpr*& offset_expr)
{
  int ret = OB_SUCCESS;
  offset_expr = NULL;
  limit_expr = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (NULL == view_offset) {
    offset_expr = upper_offset;
  } else if (NULL == upper_offset) {
    offset_expr = view_offset;
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                 *ctx->expr_factory_, ctx->session_info_, T_OP_ADD, offset_expr, view_offset, upper_offset))) {
    LOG_WARN("failed to create double op expr", K(ret));
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {  // merge limit
    ObRawExpr* minus_expr = NULL;
    ObRawExpr* nonneg_minus_expr = NULL;
    if (NULL == view_limit) {
      limit_expr = upper_limit;
    } else if (NULL == upper_limit && NULL == upper_offset) {
      limit_expr = view_limit;
    } else if (NULL == upper_offset) {
      nonneg_minus_expr = view_limit;
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                   *ctx->expr_factory_, ctx->session_info_, T_OP_MINUS, minus_expr, view_limit, upper_offset))) {
      LOG_WARN("failed to create double op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr_for_limit(
                   *ctx->expr_factory_, minus_expr, nonneg_minus_expr))) {
      LOG_WARN("failed to build case when expr for limit", K(ret));
    } else if (NULL == upper_limit) {
      limit_expr = nonneg_minus_expr;
    }

    ObRawExpr* is_not_null_expr = NULL;
    ObRawExpr* cmp_expr = NULL;
    ObRawExpr* case_when_expr = NULL;
    if (OB_FAIL(ret)) {
    } else if (NULL != limit_expr || (NULL == upper_limit && NULL == view_limit)) {
      /*do nothing*/
    } else if (OB_FAIL(
                   ObRawExprUtils::build_is_not_null_expr(*ctx->expr_factory_, nonneg_minus_expr, is_not_null_expr))) {
      LOG_WARN("failed to build is not null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                   *ctx->expr_factory_, T_OP_LT, nonneg_minus_expr, upper_limit, cmp_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(
                   *ctx->expr_factory_, cmp_expr, nonneg_minus_expr, upper_limit, case_when_expr))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(
                   *ctx->expr_factory_, is_not_null_expr, case_when_expr, nonneg_minus_expr, limit_expr))) {
      LOG_WARN("failed to build case when expr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObExprResType dst_type;
    dst_type.set_int();
    ObSysFunRawExpr* cast_expr = NULL;
    if (NULL != limit_expr) {
      OZ(ObRawExprUtils::create_cast_expr(*ctx->expr_factory_, limit_expr, dst_type, cast_expr, ctx->session_info_));
      CK(NULL != cast_expr);
      if (OB_SUCC(ret)) {
        limit_expr = cast_expr;
      }
    }
    if (OB_SUCC(ret) && NULL != offset_expr) {
      OZ(ObRawExprUtils::create_cast_expr(*ctx->expr_factory_, offset_expr, dst_type, cast_expr, ctx->session_info_));
      CK(NULL != cast_expr);
      if (OB_SUCC(ret)) {
        offset_expr = cast_expr;
      }
    }
  }
  return ret;
}

/**
 * @brief get_stmt_limit_value
 * get value of stmt limit expr
 * limit[out]: value of limit expr, -1 represent stmt not has limit,
 *             -2 represent limit value is not a interger
 *             -3 represent has offset, no limit
 *             -4 represent limit value is percent
 */
int ObTransformUtils::get_stmt_limit_value(const ObDMLStmt& stmt, int64_t& limit)
{
  int ret = OB_SUCCESS;
  limit = -1;
  const ObRawExpr* limit_expr = NULL;
  const ObRawExpr* percent_expr = NULL;
  if (!stmt.has_limit()) {
    // do nothing
  } else if (OB_NOT_NULL(percent_expr = stmt.get_limit_percent_expr())) {
    limit = -4;
  } else if (OB_ISNULL(limit_expr = stmt.get_limit_expr())) {
    limit = -3;
  } else if (limit_expr->is_const_expr()) {
    const ObObj& limit_value = static_cast<const ObConstRawExpr*>(limit_expr)->get_value();
    if (limit_value.is_integer_type()) {
      limit = limit_value.get_int();
    } else {
      limit = -2;
    }
  } else {
    limit = -2;
  }
  return ret;
}

int ObTransformUtils::check_limit_value(const ObDMLStmt& stmt, const ParamStore& param_store,
    ObSQLSessionInfo* session_info, ObIAllocator* allocator, int64_t limit, bool& is_equal, bool add_param_constraint,
    ObQueryCtx* query_ctx)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* limit_expr = NULL;
  ObObj target_value;
  target_value.set_int(ObIntType, limit);
  is_equal = false;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null allocator", K(ret));
  } else if (!stmt.has_limit()) {
    // do nothing
  } else if (OB_NOT_NULL(stmt.get_limit_percent_expr())) {
    // do nothing
  } else if (OB_ISNULL(limit_expr = stmt.get_limit_expr())) {
    // do nothing
  } else if (limit_expr->has_flag(IS_CALCULABLE_EXPR)) {
    ObPCConstParamInfo const_param_info;
    ObSEArray<ObRawExpr*, 4> params;
    ObRawExpr* param_expr = NULL;
    ObConstRawExpr* const_expr = NULL;
    ObObj result;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
            stmt.get_stmt_type(), session_info, limit_expr, result, &param_store, *allocator))) {
      LOG_WARN("Failed to calc const or calculable expr", K(ret));
    } else if (target_value.is_invalid_type() || result.is_invalid_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected invalid type", K(ret));
    } else if (!target_value.can_compare(result) || 0 != target_value.compare(result)) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::extract_params(const_cast<ObRawExpr*>(limit_expr), params))) {
      LOG_WARN("failed to extract params", K(ret));
    } else if (params.empty()) {
      is_equal = true;
    } else if (!add_param_constraint) {
      is_equal = true;
    } else if (OB_ISNULL(query_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null query ctx", K(ret));
    } else if (1 != params.count()) {
      // if limit expr is ? + ?, we can not generate plan constraint,
      // return false in sake of hitting inproper plan.
    } else if (OB_ISNULL(param_expr = params.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (T_QUESTIONMARK != param_expr->get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect questionmark expr", K(*param_expr), K(ret));
    } else if (OB_FALSE_IT(const_expr = static_cast<ObConstRawExpr*>(param_expr))) {
    } else if (OB_FAIL(const_param_info.const_idx_.push_back(const_expr->get_value().get_unknown()))) {
      LOG_WARN("failed to push back param idx", K(ret));
    } else if (OB_FAIL(const_param_info.const_params_.push_back(target_value))) {
      LOG_WARN("failed to push back value", K(ret));
    } else if (OB_FAIL(query_ctx->all_plan_const_param_constraints_.push_back(const_param_info))) {
      LOG_WARN("failed to push back const param info", K(ret));
    } else {
      is_equal = true;
    }
  } else if (limit_expr->is_const_expr()) {
    const ObObj& limit_value = static_cast<const ObConstRawExpr*>(limit_expr)->get_value();
    if (limit_value.is_invalid_type() || target_value.is_invalid_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected invalid type", K(ret));
    } else if (!limit_value.can_compare(target_value) || 0 != limit_value.compare(target_value)) {
    } else {
      is_equal = true;
    }
  }
  return ret;
}

int ObTransformUtils::convert_column_expr_to_select_expr(const common::ObIArray<ObRawExpr*>& column_exprs,
    const ObSelectStmt& inner_stmt, common::ObIArray<ObRawExpr*>& select_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr* outer_expr = NULL;
  ObRawExpr* inner_expr = NULL;
  ObColumnRefRawExpr* column_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    int64_t pos = OB_INVALID_ID;
    if (OB_ISNULL(outer_expr = column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_UNLIKELY(!outer_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr type", K(ret));
    } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(outer_expr))) {
      /*do nothing*/
    } else if (FALSE_IT(pos = column_expr->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      /*do nothing*/
    } else if (OB_UNLIKELY(pos < 0 || pos >= inner_stmt.get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array pos", K(pos), K(inner_stmt.get_select_item_size()), K(ret));
    } else if (OB_ISNULL(inner_expr = inner_stmt.get_select_item(pos).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(inner_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::convert_select_expr_to_column_expr(const common::ObIArray<ObRawExpr*>& select_exprs,
    const ObSelectStmt& inner_stmt, ObDMLStmt& outer_stmt, uint64_t table_id,
    common::ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    ObColumnRefRawExpr* col = NULL;
    int64_t idx = -1;
    bool find = false;
    if (OB_ISNULL(expr = select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < inner_stmt.get_select_item_size(); ++j) {
      if (expr == inner_stmt.get_select_item(j).expr_) {
        find = true;
        idx = j;
      }
    }
    uint64_t column_id = idx + OB_APP_MIN_COLUMN_ID;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find select expr inner stmt", K(ret));
    } else if (OB_ISNULL(col = outer_stmt.get_column_expr_by_id(table_id, column_id))) {
      LOG_WARN("failed to get column expr by id", K(ret));
    } else if (OB_FAIL(column_exprs.push_back(col))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::pull_up_subquery(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_stmt), K(child_stmt), K(ret));
  } else {
    const ObIArray<ObQueryRefRawExpr*>& subquery_exprs = child_stmt->get_subquery_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
      ObQueryRefRawExpr* query_ref = subquery_exprs.at(i);
      if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(query_ref), K(ret));
      } else if (OB_FAIL(parent_stmt->add_subquery_ref(query_ref))) {
        LOG_WARN("failed to add subquery ref", K(ret));
      } else if (OB_FAIL(query_ref->get_ref_stmt()->adjust_view_parent_namespace_stmt(parent_stmt))) {
        LOG_WARN("failed to adjust subquery parent namespace", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_table_size(); ++i) {
      TableItem* table_item = child_stmt->get_table_item(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (table_item->is_generated_table()) {
        ObSelectStmt* view_stmt = NULL;
        if (OB_ISNULL(view_stmt = table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view_stmt is null", K(ret));
        } else if (OB_FAIL(view_stmt->adjust_view_parent_namespace_stmt(parent_stmt->get_parent_namespace_stmt()))) {
          LOG_WARN("adjust view parent namespace stmt failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_subquery_expr_from_joined_table(ObDMLStmt* stmt, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> on_conditions;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_joined_tables().count(); ++i) {
    JoinedTable* joined_table = stmt->get_joined_tables().at(i);
    if (OB_FAIL(get_on_condition(joined_table, on_conditions))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extract_query_ref_expr(on_conditions, subqueries))) {
      LOG_WARN("failed to extract query ref expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_on_conditions(ObDMLStmt& stmt, ObIArray<ObRawExpr*>& conditions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    FromItem& from_item = stmt.get_from_item(i);
    JoinedTable* joined_table = NULL;
    if (!from_item.is_joined_) {
      // do nothing
    } else if (OB_ISNULL(joined_table = stmt.get_joined_table(from_item.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(get_on_condition(joined_table, conditions))) {
      LOG_WARN("failed to get on condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_on_condition(TableItem* table_item, ObIArray<ObRawExpr*>& conditions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    // do nothing
  } else {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(append(conditions, joined_table->get_join_conditions()))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(get_on_condition(joined_table->left_table_, conditions))) {
      LOG_WARN("failed to get on condition", K(ret));
    } else if (OB_FAIL(get_on_condition(joined_table->right_table_, conditions))) {
      LOG_WARN("failed to get on condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_semi_conditions(ObIArray<SemiInfo*>& semi_infos, ObIArray<ObRawExpr*>& conditions)
{
  int ret = OB_SUCCESS;
  SemiInfo* semi_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_info = semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(append(conditions, semi_info->semi_conditions_))) {
      LOG_WARN("failed to append semi condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::set_limit_expr(ObDMLStmt* stmt, ObTransformerCtx* ctx)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* new_limit_count_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ctx), K(ctx->expr_factory_));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx->expr_factory_, ObIntType, 1, new_limit_count_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else {
    stmt->set_limit_offset(new_limit_count_expr, stmt->get_offset_expr());
  }
  return ret;
}

int ObTransformUtils::make_pushdown_limit_count(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
    ObRawExpr* limit_count, ObRawExpr* limit_offset, ObRawExpr*& pushdown_limit_count)
{
  int ret = OB_SUCCESS;
  if (NULL == limit_offset) {
    pushdown_limit_count = limit_count;
  } else {
    OZ(ObRawExprUtils::create_double_op_expr(
        expr_factory, &session, T_OP_ADD, pushdown_limit_count, limit_count, limit_offset));
    CK(NULL != pushdown_limit_count);
    OZ(pushdown_limit_count->formalize(&session));
    if (OB_SUCC(ret) && session.use_static_typing_engine() &&
        !pushdown_limit_count->get_result_type().is_integer_type()) {
      // Cast to integer in static typing engine if needed.
      ObExprResType dst_type;
      dst_type.set_int();
      ObSysFunRawExpr* cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(expr_factory, pushdown_limit_count, dst_type, cast_expr, &session));
      CK(NULL != cast_expr);
      OZ(cast_expr->formalize(&session));
      if (OB_SUCC(ret)) {
        pushdown_limit_count = cast_expr;
      }
    }
  }
  return ret;
}

int ObTransformUtils::recursive_set_stmt_unique(ObSelectStmt* select_stmt, ObTransformerCtx* ctx,
    bool ignore_check_unique, /*default false */
    ObIArray<ObRawExpr*>* unique_keys)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  bool is_stack_overflow = false;
  ObRelIds origin_output_rel_ids;
  ObSEArray<ObRawExpr*, 4> pkeys;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->session_info_) || OB_ISNULL(ctx->schema_checker_) ||
      OB_ISNULL(ctx->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(select_stmt), K(ctx));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (!ignore_check_unique &&
             (OB_FAIL(check_stmt_unique(
                 select_stmt, ctx->session_info_, ctx->schema_checker_, true /* strict */, is_unique)))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (is_unique) {
    if (OB_ISNULL(unique_keys)) {
      // do nothing
    } else if (OB_FAIL(select_stmt->get_select_exprs(*unique_keys))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  } else if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(get_from_tables(*select_stmt, origin_output_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else {
    ObIArray<TableItem*>& table_items = select_stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      TableItem* cur_table = table_items.at(i);
      int32_t bit_id = OB_INVALID_INDEX;
      if (OB_ISNULL(cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(cur_table));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == (bit_id = select_stmt->get_table_bit_index(cur_table->table_id_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table bit index", K(ret), K(cur_table->table_id_), K(bit_id));
      } else if (!origin_output_rel_ids.has_member(bit_id)) {
        // do nothing
      } else if (cur_table->is_generated_table()) {
        ObSelectStmt* view_stmt = NULL;
        ObSEArray<ObRawExpr*, 4> stmt_unique_keys;
        ObSEArray<ObRawExpr*, 4> column_exprs;
        if (OB_ISNULL(view_stmt = cur_table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view_stmt = stmt is null", K(ret), K(view_stmt));
        } else if (OB_FAIL(SMART_CALL(recursive_set_stmt_unique(view_stmt, ctx, false, &stmt_unique_keys)))) {
          LOG_WARN("recursive set stmt unique failed", K(ret));
        } else if (OB_FAIL(create_columns_for_view(ctx, *cur_table, select_stmt, column_exprs))) {
          LOG_WARN("failed to create columns for view", K(ret));
        } else if (OB_FAIL(convert_select_expr_to_column_expr(
                       stmt_unique_keys, *view_stmt, *select_stmt, cur_table->table_id_, pkeys))) {
          LOG_WARN("failed to get stmt unique keys columns expr", K(ret));
        }
      } else if (!cur_table->is_basic_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect table item type", K(*cur_table), K(ret));
      } else if (OB_FAIL(generate_unique_key(ctx, select_stmt, cur_table, pkeys))) {
        LOG_WARN("failed to generate unique key", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_NOT_NULL(unique_keys) && OB_FAIL(append(*unique_keys, pkeys))) {
      LOG_WARN("failed to append unique keys", K(ret));
    } else if (OB_FAIL(add_non_duplicated_select_expr(pkeys, select_exprs))) {
      LOG_WARN("failed to add non-duplicated select expr", K(ret));
    } else if (OB_FAIL(create_select_item(*ctx->allocator_, pkeys, select_stmt))) {
      LOG_WARN("failed to get tables primary keys", K(ret));
    } else if (OB_FAIL(select_stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::add_non_duplicated_select_expr(
    ObIArray<ObRawExpr*>& add_select_exprs, ObIArray<ObRawExpr*>& org_select_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_select_exprs;
  if (OB_FAIL(ObOptimizerUtil::except_exprs(add_select_exprs, org_select_exprs, new_select_exprs))) {
    LOG_WARN("failed cal except exprs", K(ret));
  } else if (OB_FAIL(add_select_exprs.assign(new_select_exprs))) {
    LOG_WARN("failed to assign select exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_can_set_stmt_unique(ObDMLStmt* stmt, bool& can_set_unique)
{
  int ret = OB_SUCCESS;
  can_set_unique = false;
  ObRelIds output_rel_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(stmt), K(ret));
  } else if (OB_FAIL(get_from_tables(*stmt, output_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else {
    ObSelectStmt* view_stmt = NULL;
    can_set_unique = true;
    ObIArray<TableItem*>& table_items = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && can_set_unique && i < table_items.count(); ++i) {
      const TableItem* table_item = table_items.at(i);
      int32_t bit_id = OB_INVALID_INDEX;
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(table_item), K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == (bit_id = stmt->get_table_bit_index(table_item->table_id_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table bit index", K(ret), K(table_item->table_id_), K(bit_id));
      } else if (!output_rel_ids.has_member(bit_id)) {
        // do nothing
      } else if (table_item->is_generated_table()) {
        if (OB_ISNULL(view_stmt = table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null ptr", K(view_stmt), K(ret));
        } else if (view_stmt->is_set_stmt() && !view_stmt->is_set_distinct()) {
          can_set_unique = false;
        } else if (view_stmt->is_set_stmt() && view_stmt->is_set_distinct()) {
          // do nothing
        } else if (view_stmt->is_hierarchical_query()) {
          can_set_unique = false;
        } else if (OB_FAIL(SMART_CALL(check_can_set_stmt_unique(view_stmt, can_set_unique)))) {
          LOG_WARN("failed to check can set stmt unique", K(ret));
        } else { /*do nothing */
        }
      } else if (table_item->is_basic_table()) {
        // do nothing
      } else {
        can_set_unique = false;
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_rel_ids_from_tables(
    ObDMLStmt* stmt, const ObIArray<TableItem*>& table_items, ObRelIds& rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  }
  TableItem* table = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    if (OB_ISNULL(table = table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null.", K(ret));
    } else if (table->is_joined_table()) {
      ret = get_rel_ids_from_join_table(stmt, static_cast<JoinedTable*>(table), rel_ids);
    } else if (OB_FAIL(rel_ids.add_member(stmt->get_table_bit_index(table_items.at(i)->table_id_)))) {
      LOG_WARN("failed to add member", K(ret), K(table_items.at(i)->table_id_));
    }
  }
  return ret;
}

int ObTransformUtils::get_rel_ids_from_join_table(ObDMLStmt* stmt, const JoinedTable* joined_table, ObRelIds& rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->single_table_ids_.count(); ++i) {
      if (OB_FAIL(rel_ids.add_member(stmt->get_table_bit_index(joined_table->single_table_ids_.at(i))))) {
        LOG_WARN("failed to add member", K(ret), K(joined_table->single_table_ids_.at(i)));
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObTransformUtils::adjust_single_table_ids(JoinedTable* joined_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else {
    joined_table->single_table_ids_.reset();
    TableItem* left_table = joined_table->left_table_;
    TableItem* right_table = joined_table->right_table_;
    if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left or right table is null", K(ret));
    } else if (left_table->type_ == TableItem::JOINED_TABLE) {
      JoinedTable* left_join_table = static_cast<JoinedTable*>(left_table);
      if (OB_FAIL(append_array_no_dup(joined_table->single_table_ids_, left_join_table->single_table_ids_))) {
        LOG_WARN("failed to append to single table ids.", K(ret));
      } else { /* do nothing. */
      }
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(joined_table->single_table_ids_, left_table->table_id_))) {
        LOG_WARN("failed to add var to array no dup.", K(ret));
      } else { /* do nothing. */
      }
    }
    if (OB_FAIL(ret)) {
    } else if (right_table->type_ == TableItem::JOINED_TABLE) {
      JoinedTable* right_join_table = static_cast<JoinedTable*>(right_table);
      if (OB_FAIL(append_array_no_dup(joined_table->single_table_ids_, right_join_table->single_table_ids_))) {
        LOG_WARN("failed to append to single table ids.", K(ret));
      } else { /* do nothing. */
      }
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(joined_table->single_table_ids_, right_table->table_id_))) {
        LOG_WARN("failed to add var to array no dup.", K(ret));
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_table_items(TableItem* table_item, ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (table_item->is_joined_table()) {
    TableItem* left_table = static_cast<JoinedTable*>(table_item)->left_table_;
    TableItem* right_table = static_cast<JoinedTable*>(table_item)->right_table_;
    if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null error.", K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_table_items(left_table, table_items)))) {
      LOG_WARN("failed to extaract table items.", K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_table_items(right_table, table_items)))) {
      LOG_WARN("failed to extaract table items.", K(ret));
    } else {
    }
  } else if (OB_FAIL(table_items.push_back(table_item))) {
    LOG_WARN("failed to push back into table items.", K(ret));
  } else {
  }
  return ret;
}

int ObTransformUtils::get_base_column(ObDMLStmt* stmt, ObColumnRefRawExpr*& col)
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  ObRawExpr* sel_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col));
  } else if (OB_ISNULL(table = stmt->get_table_item_by_id(col->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(table));
  } else if (table->is_basic_table()) {
    /*do nothing*/
  } else if (!table->is_generated_table() || OB_ISNULL(table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated table", K(ret), K(table));
  } else {
    uint64_t sid = col->get_column_id() - OB_APP_MIN_COLUMN_ID;
    ObSelectStmt* view = table->ref_query_;
    if (OB_ISNULL(view) || OB_ISNULL(view = view->get_real_stmt()) ||
        OB_UNLIKELY(sid < 0 || sid >= view->get_select_item_size()) ||
        OB_UNLIKELY(sid >= view->get_select_item_size()) || OB_ISNULL(sel_expr = view->get_select_item(sid).expr_) ||
        OB_UNLIKELY(!sel_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid generated table column", K(ret), K(view), K(sid), K(sel_expr));
    } else if (FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
      /*do nothing*/
    } else if (OB_FAIL(SMART_CALL(get_base_column(view, col)))) {
      LOG_WARN("failed to get update table", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::get_post_join_exprs
 * the term post-join exprs refers to exprs processed upon the results of join
 * @param stmt
 * @param exprs
 * @param with_vector_assign:
 *        update tc set (c1, c2) = (select sum(d1), sum(d2) from td);
 * @return
 */
int ObTransformUtils::get_post_join_exprs(
    ObDMLStmt* stmt, ObIArray<ObRawExpr*>& exprs, bool with_vector_assgin /*= false*/)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    // do nothing
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (sel_stmt->has_rollup()) {
      // do nothing
    } else if (sel_stmt->has_group_by()) {
      if (OB_FAIL(append(exprs, sel_stmt->get_group_exprs()))) {
        LOG_WARN("failed to append group by exprs", K(ret));
      } else if (OB_FAIL(append(exprs, sel_stmt->get_aggr_items()))) {
        LOG_WARN("failed to append aggr items", K(ret));
      }
    } else if (OB_FAIL(sel_stmt->get_select_exprs(exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt* upd_stmt = static_cast<ObUpdateStmt*>(stmt);
    if (upd_stmt->has_limit() || upd_stmt->has_order_by()) {
      // do nothing
    } else if (upd_stmt->is_update_set() && !with_vector_assgin) {
      // do nothing
    } else if (OB_FAIL(upd_stmt->get_assign_values(exprs))) {
      LOG_WARN("failed to get assign value exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::free_stmt(ObStmtFactory& stmt_factory, ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(free_stmt(stmt_factory, child_stmts.at(i)))) {
      LOG_WARN("failed to free stmt", K(ret));
    }
  }
  if (OB_SUCC(ret) && stmt->is_select_stmt()) {
    if (OB_FAIL(SMART_CALL(stmt_factory.free_stmt(static_cast<ObSelectStmt*>(stmt))))) {
      LOG_WARN("failed to free stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_shared_expr(
    ObDMLStmt* upper_stmt, ObDMLStmt* child_stmt, ObIArray<ObRawExpr*>& shared_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> upper_contain_column_exprs;
  ObSEArray<ObRawExpr*, 16> child_contain_column_exprs;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(upper_stmt), K(child_stmt));
  } else if (OB_FAIL(extract_stmt_column_contained_expr(
                 upper_stmt, upper_stmt->get_current_level(), upper_contain_column_exprs))) {
    LOG_WARN("failed to extract stmt column contained expr", K(ret));
  } else if (OB_FAIL(extract_stmt_column_contained_expr(
                 child_stmt, child_stmt->get_current_level(), child_contain_column_exprs))) {
    LOG_WARN("failed to extract stmt column contained expr", K(ret));
  } else if (OB_FAIL(
                 ObOptimizerUtil::intersect(child_contain_column_exprs, upper_contain_column_exprs, shared_exprs))) {
    LOG_WARN("failed to intersect", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformUtils::extract_stmt_column_contained_expr(
    ObDMLStmt* stmt, int32_t stmt_level, ObIArray<ObRawExpr*>& contain_column_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
      if (OB_FAIL(extract_column_contained_expr(relation_exprs.at(i), stmt_level, contain_column_exprs))) {
        LOG_WARN("failed to extract column contained expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_column_contained_expr(
    ObRawExpr* expr, int32_t stmt_level, ObIArray<ObRawExpr*>& contain_column_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (!expr->get_expr_levels().has_member(stmt_level)) {
    /*do nothing */
  } else if (!expr->is_query_ref_expr() && !expr->get_relation_ids().is_empty()) {
    if (!expr->is_column_ref_expr() && !expr->is_aggr_expr() && !expr->is_win_func_expr() &&
        expr->get_expr_levels().num_members() == 1 && !ObRawExprUtils::find_expr(contain_column_exprs, expr) &&
        OB_FAIL(contain_column_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(
                SMART_CALL(extract_column_contained_expr(expr->get_param_expr(i), stmt_level, contain_column_exprs)))) {
          LOG_WARN("failed to extract column contained expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  } else if (expr->is_query_ref_expr()) {
    if (OB_FAIL(extract_stmt_column_contained_expr(
            static_cast<ObQueryRefRawExpr*>(expr)->get_ref_stmt(), stmt_level, contain_column_exprs))) {
      LOG_WARN("failed to extract stmt column contained expr", K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformUtils::is_question_mark_pre_param(const ObDMLStmt& stmt, const int64_t param_idx, bool& is_pre_param)
{
  int ret = OB_SUCCESS;
  int64_t pre_param_count = 0;
  const ObQueryCtx* query_ctx = NULL;
  is_pre_param = false;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (query_ctx->calculable_items_.count() > 0) {
    pre_param_count = -1;
    for (int64_t i = 0; i < query_ctx->calculable_items_.count(); ++i) {
      const ObHiddenColumnItem& cur_item = query_ctx->calculable_items_.at(i);
      if (-1 == pre_param_count) {
        pre_param_count = cur_item.hidden_idx_;
      } else if (cur_item.hidden_idx_ < pre_param_count) {
        pre_param_count = cur_item.hidden_idx_;
      }
    }
  } else {
    pre_param_count = query_ctx->question_marks_count_;
  }
  if (OB_SUCC(ret)) {
    is_pre_param = (param_idx >= 0 && param_idx < pre_param_count);
  }
  return ret;
}

int ObTransformUtils::extract_pseudo_column_like_expr(
    ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& pseudo_column_like_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_pseudo_column_like_expr(exprs.at(i), pseudo_column_like_exprs))) {
      LOG_WARN("failed to extract pseudo column like exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_pseudo_column_like_expr(ObRawExpr* expr, ObIArray<ObRawExpr*>& pseudo_column_like_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (ObRawExprUtils::is_pseudo_column_like_expr(*expr)) {
    ret = add_var_to_array_no_dup(pseudo_column_like_exprs, expr);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_pseudo_column_like_expr(expr->get_param_expr(i), pseudo_column_like_exprs)))) {
        LOG_WARN("failed to extract pseudo column like expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::adjust_pseudo_column_like_exprs(ObDMLStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> relation_exprs;
  ObSEArray<ObRawExpr*, 8> pseudo_column_like_exprs;
  if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(extract_pseudo_column_like_expr(relation_exprs, pseudo_column_like_exprs))) {
    LOG_WARN("failed to extract pseudo column like expr", K(ret));
  } else if (OB_FAIL(stmt.get_pseudo_column_like_exprs().assign(pseudo_column_like_exprs))) {
    LOG_WARN("failed to assign pseudo column like exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_has_rownum(const ObIArray<ObRawExpr*>& exprs, bool& has_rownum)
{
  int ret = OB_SUCCESS;
  has_rownum = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_rownum && i < exprs.count(); ++i) {
    ObRawExpr* expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_ROWNUM)) {
      has_rownum = true;
    }
  }
  return ret;
}

int ObTransformUtils::create_view_with_from_items(ObDMLStmt* stmt, ObTransformerCtx* ctx, TableItem* table_item,
    const ObIArray<ObRawExpr*>& new_select_exprs, const ObIArray<ObRawExpr*>& new_conds, TableItem*& view_table)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* view = NULL;
  // 1. create stmt
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(table_item) || OB_ISNULL(ctx->allocator_) ||
      OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(view))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(view)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  // 2. set baisc info
  if (OB_SUCC(ret)) {
    view->set_current_level(stmt->get_current_level());
    view->set_query_ctx(stmt->get_query_ctx());
    view->set_parent_namespace_stmt(stmt->get_parent_namespace_stmt());
    view->set_stmt_id();
    if (OB_ISNULL(view->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(
                   view->get_query_ctx()->add_stmt_id_name(view->get_stmt_id(), ObString::make_empty_string(), view))) {
      LOG_WARN("failed to add_stmt_id_name", "stmt_id", view->get_stmt_id(), K(ret));
    }
  }
  // 3. add table_item, column items, part expr items
  if (OB_SUCC(ret)) {
    ObSEArray<ObDMLStmt::PartExprItem, 8> part_items;
    ObSEArray<ColumnItem, 8> column_items;
    if (OB_FAIL(add_table_item(view, table_item))) {
      LOG_WARN("failed to add table item", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(table_item))) {
      LOG_WARN("failed to add table item", K(ret));
    } else if (OB_FAIL(stmt->remove_from_item(table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(stmt->get_part_expr_items(table_item->table_id_, part_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(view->set_part_expr_items(part_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(stmt->remove_part_expr_items(table_item->table_id_))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_column_items(table_item->table_id_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(view->add_column_item(column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(stmt->remove_column_item(table_item->table_id_))) {
      LOG_WARN("failed to get column items", K(ret));
    }
  }
  // 4. formalize view
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(view->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(view->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  } else if (OB_FAIL(view->formalize_stmt(ctx->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  // 5. create new table item for view
  ObSEArray<ObRawExpr*, 8> select_exprs;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append(view->get_condition_exprs(), new_conds))) {
    LOG_WARN("failed to append conditions", K(ret));
  } else if (OB_FAIL(select_exprs.assign(new_select_exprs))) {
    LOG_WARN("failed to assgin exprs", K(ret));
  } else if (new_select_exprs.empty() && OB_FAIL(view->get_column_exprs(select_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(add_new_table_item(ctx, stmt, view, view_table))) {
    LOG_WARN("failed to add table items", K(ret));
  } else if (OB_FAIL(create_columns_for_view(ctx, *view_table, stmt, select_exprs, column_exprs))) {
    LOG_WARN("failed to create column items", K(ret));
  } else if (OB_FAIL(view->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(view->adjust_subquery_stmt_parent(stmt, view))) {
    LOG_WARN("failed to adjust sbuquery stmt parent", K(ret));
  } else if (OB_FAIL(adjust_pseudo_column_like_exprs(*view))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(stmt->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(stmt->remove_table_item(view_table))) {
    LOG_WARN("failed to remove table item", K(ret));
  } else if (OB_FAIL(stmt->replace_inner_stmt_expr(select_exprs, column_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(stmt->get_table_items().push_back(view_table))) {
    LOG_WARN("failed to push back table item", K(ret));
  }
  // 6. final format
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(adjust_pseudo_column_like_exprs(*stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt(ctx->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformUtils::add_table_item(ObDMLStmt* stmt, TableItem* table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(table_item), K(ret));
  } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
    LOG_WARN("failed to set table bit index", K(ret));
  }
  return ret;
}

int ObTransformUtils::add_table_item(ObDMLStmt* stmt, ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ret = add_table_item(stmt, table_items.at(i));
  }
  return ret;
}

int ObTransformUtils::replace_having_expr(ObSelectStmt* stmt, ObRawExpr* from, ObRawExpr* to)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_having_expr_size(); i++) {
      if (OB_FAIL(ObRawExprUtils::replace_ref_column(stmt->get_having_exprs().at(i), from, to))) {
        LOG_WARN("replace reference column failed", K(ret));
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_subquery_match_index(ObTransformerCtx* ctx, ObSelectStmt* subquery, bool& is_match)
{
  int ret = OB_SUCCESS;
  int64_t stmt_level = 0;
  ObRawExpr* cond = NULL;
  ObRawExpr* left = NULL;
  ObRawExpr* right = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    stmt_level = subquery->get_current_level();
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < subquery->get_condition_size(); ++i) {
    bool left_is_correlated = false;
    bool right_is_correlated = false;
    if (OB_ISNULL(cond = subquery->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (IS_COMMON_COMPARISON_OP(cond->get_expr_type()) && T_OP_NSEQ != cond->get_expr_type() &&
               cond->get_expr_levels().has_member(stmt_level - 1) && cond->get_expr_levels().has_member(stmt_level) &&
               !cond->has_flag(CNT_SUB_QUERY)) {
      if (OB_ISNULL(left = cond->get_param_expr(0)) || OB_ISNULL(right = cond->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(left), K(right));
      } else if (OB_FAIL(is_correlated_expr(left, stmt_level - 1, left_is_correlated))) {
        LOG_WARN("failed to check left child is correlated", K(ret));
      } else if (OB_FAIL(is_correlated_expr(right, stmt_level - 1, right_is_correlated))) {
        LOG_WARN("failed to check right child is correlated", K(ret));
      } else if (left_is_correlated == right_is_correlated) {
        // both correlated or both not
        LOG_TRACE("both condition are [not] correlated", K(left_is_correlated), K(right_is_correlated));
      } else if ((left_is_correlated && left->get_expr_levels().has_member(stmt_level)) ||
                 (right_is_correlated && right->get_expr_levels().has_member(stmt_level))) {
        LOG_TRACE("expr used both inner and outer block columns");
      } else {
        ObRawExpr* inner_expr = left_is_correlated ? right : left;
        if (inner_expr->is_column_ref_expr()) {
          if (OB_FAIL(is_match_index(ctx->sql_schema_guard_,
                  subquery,
                  static_cast<ObColumnRefRawExpr*>(inner_expr),
                  is_match,
                  &ctx->equal_sets_))) {
            LOG_WARN("failed to check is match index", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_limit_value(const ObRawExpr* limit_expr, const ObDMLStmt* stmt, const ParamStore* param_store,
    ObSQLSessionInfo* session_info, ObIAllocator* allocator, int64_t& limit_value, bool& is_null_value)
{
  int ret = OB_SUCCESS;
  limit_value = 0;
  is_null_value = false;
  if (NULL == limit_expr) {
    /*do nothing*/
  } else if (OB_ISNULL(stmt) || OB_ISNULL(param_store) || OB_ISNULL(session_info) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(limit_expr), K(param_store), K(session_info), K(allocator));
  } else if (T_INT == limit_expr->get_expr_type()) {
    limit_value = static_cast<const ObConstRawExpr*>(limit_expr)->get_value().get_int();
  } else if (T_NULL == limit_expr->get_expr_type()) {
    is_null_value = true;
  } else {
    ObObj value;
    if (T_QUESTIONMARK == limit_expr->get_expr_type()) {
      int64_t idx = static_cast<const ObConstRawExpr*>(limit_expr)->get_value().get_unknown();
      if (idx < 0 || idx >= param_store->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Idx is invalid", K(idx), K(param_store->count()), K(ret));
      } else {
        value = param_store->at(idx);
      }
    } else if ((limit_expr->has_flag(IS_CALCULABLE_EXPR) || limit_expr->is_const_expr()) &&
               (limit_expr->get_result_type().is_integer_type() || limit_expr->get_result_type().is_number())) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
              stmt->get_stmt_type(), session_info, limit_expr, value, param_store, *allocator))) {
        LOG_WARN("Failed to get const or calculable expr value", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected limit expr type", K(limit_expr->get_expr_type()), K(ret));
    }
    if (OB_SUCC(ret)) {
      number::ObNumber number;
      if (value.is_integer_type()) {
        limit_value = value.get_int();
      } else if (value.is_null()) {
        is_null_value = true;
      } else if (OB_FAIL(value.get_number(number))) {
        LOG_WARN("unexpected value type", K(ret), K(value));
      } else if (OB_UNLIKELY(!number.is_valid_int64(limit_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("number is not valid int64", K(ret), K(value), K(number));
      }
    }
  }
  if (OB_SUCC(ret) && limit_value < 0) {
    limit_value = 0;
  }
  return ret;
}

int ObTransformUtils::add_const_param_constraints(ObRawExpr* expr, ObQueryCtx* query_ctx, ObTransformerCtx* ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSEArray<ObRawExpr*, 4> params;
  if (OB_ISNULL(expr) || OB_ISNULL(query_ctx) || OB_ISNULL(ctx) || OB_ISNULL(ctx->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(query_ctx), K(ctx), K(ctx->exec_ctx_), K(plan_ctx));
  } else if (OB_FAIL(ObOptimizerUtil::extract_params(expr, params))) {
    LOG_WARN("failed to extract params", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      if (OB_ISNULL(params.at(i)) || OB_UNLIKELY(params.at(i)->get_expr_type() != T_QUESTIONMARK)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(params), K(i));
      } else {
        ObPCConstParamInfo param_info;
        ObObj target_obj;
        ObConstRawExpr* const_expr = static_cast<ObConstRawExpr*>(params.at(0));
        int64_t param_idx = const_expr->get_value().get_unknown();
        if (OB_UNLIKELY(param_idx < 0 || param_idx >= plan_ctx->get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(param_idx), K(plan_ctx->get_param_store().count()));
        } else if (OB_FAIL(param_info.const_idx_.push_back(param_idx))) {
          LOG_WARN("failed to push back param idx", K(ret));
        } else if (OB_FAIL(param_info.const_params_.push_back(plan_ctx->get_param_store().at(param_idx)))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (OB_FAIL(query_ctx->all_plan_const_param_constraints_.push_back(param_info))) {
          LOG_WARN("failed to push back param info", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_grouping_sets_items(ObRawExprFactory& expr_factory,
    const common::ObIArray<ObGroupingSetsItem>& input_items, common::ObIArray<ObGroupingSetsItem>& output_items,
    const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_items.count(); i++) {
    ObGroupingSetsItem grouping_sets_item;
    if (OB_FAIL(grouping_sets_item.deep_copy(expr_factory, input_items.at(i), copy_types))) {
      LOG_WARN("failed to deep copy grouping sets items", K(ret));
    } else if (OB_FAIL(output_items.push_back(grouping_sets_item))) {
      LOG_WARN("failed to push back grouping sets items", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::deep_copy_multi_rollup_items(ObRawExprFactory& expr_factory,
    const common::ObIArray<ObMultiRollupItem>& input_items, common::ObIArray<ObMultiRollupItem>& output_items,
    const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_items.count(); i++) {
    ObMultiRollupItem multi_rollup_item;
    if (OB_FAIL(multi_rollup_item.deep_copy(expr_factory, input_items.at(i), copy_types))) {
      LOG_WARN("failed to deep copy grouping sets items", K(ret));
    } else if (OB_FAIL(output_items.push_back(multi_rollup_item))) {
      LOG_WARN("failed to push back grouping sets items", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::replace_stmt_expr_with_groupby_exprs(ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    common::ObIArray<SelectItem>& select_items = select_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); i++) {
      if (OB_FAIL(replace_with_groupby_exprs(select_stmt, select_items.at(i).expr_))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/
      }
    }
    common::ObIArray<ObRawExpr*>& having_exprs = select_stmt->get_having_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs.count(); i++) {
      if (OB_FAIL(replace_with_groupby_exprs(select_stmt, having_exprs.at(i)))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/
      }
    }
    common::ObIArray<OrderItem>& order_items = select_stmt->get_order_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
      if (OB_FAIL(replace_with_groupby_exprs(select_stmt, order_items.at(i).expr_))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_with_groupby_exprs(ObSelectStmt* select_stmt, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext check_context;
  check_context.reset();
  check_context.override_const_compare_ = true;
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got an unexpected null", K(ret));
  } else if (OB_FAIL(check_context.init(select_stmt->get_query_ctx()))) {
    LOG_WARN("failed to init check context.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(replace_with_groupby_exprs(select_stmt, expr->get_param_expr(i))))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/
      }
    }
    bool is_existed = false;
    ObIArray<ObRawExpr*>& groupby_exprs = select_stmt->get_group_exprs();
    ObIArray<ObRawExpr*>& rollup_exprs = select_stmt->get_rollup_exprs();
    ObIArray<ObGroupingSetsItem>& grouping_sets_items = select_stmt->get_grouping_sets_items();
    ObIArray<ObMultiRollupItem>& multi_rollup_items = select_stmt->get_multi_rollup_items();
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < groupby_exprs.count(); i++) {
      if (OB_ISNULL(groupby_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null", K(ret));
      } else if (groupby_exprs.at(i)->same_as(*expr, &check_context)) {
        expr = groupby_exprs.at(i);
        is_existed = true;
      } else { /*do nothing.*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < rollup_exprs.count(); i++) {
      if (OB_ISNULL(rollup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null", K(ret));
      } else if (rollup_exprs.at(i)->same_as(*expr, &check_context)) {
        expr = rollup_exprs.at(i);
        is_existed = true;
      } else { /*do nothing.*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < grouping_sets_items.count(); i++) {
      ObIArray<ObGroupbyExpr>& grouping_sets_exprs = grouping_sets_items.at(i).grouping_sets_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < grouping_sets_exprs.count(); j++) {
        ObIArray<ObRawExpr*>& groupby_exprs = grouping_sets_exprs.at(j).groupby_exprs_;
        for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < groupby_exprs.count(); k++) {
          if (OB_ISNULL(groupby_exprs.at(k))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got an unexpected null", K(ret));
          } else if (groupby_exprs.at(k)->same_as(*expr, &check_context)) {
            expr = groupby_exprs.at(k);
            is_existed = true;
          } else { /*do nothing.*/
          }
        }
      }
      if (OB_SUCC(ret) && !is_existed) {
        ObIArray<ObMultiRollupItem>& rollup_items = grouping_sets_items.at(i).multi_rollup_items_;
        for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < rollup_items.count(); j++) {
          ObIArray<ObGroupbyExpr>& rollup_list_exprs = rollup_items.at(j).rollup_list_exprs_;
          for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < rollup_list_exprs.count(); k++) {
            ObIArray<ObRawExpr*>& groupby_exprs = rollup_list_exprs.at(k).groupby_exprs_;
            for (int64_t m = 0; OB_SUCC(ret) && !is_existed && m < groupby_exprs.count(); m++) {
              if (OB_ISNULL(groupby_exprs.at(m))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("got an unexpected null", K(ret));
              } else if (groupby_exprs.at(m)->same_as(*expr, &check_context)) {
                expr = groupby_exprs.at(m);
                is_existed = true;
              } else { /*do nothing.*/
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_existed) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < multi_rollup_items.count(); i++) {
        ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(i).rollup_list_exprs_;
        for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < rollup_list_exprs.count(); j++) {
          ObIArray<ObRawExpr*>& groupby_exprs = rollup_list_exprs.at(j).groupby_exprs_;
          for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < groupby_exprs.count(); k++) {
            if (OB_ISNULL(groupby_exprs.at(k))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("got an unexpected null", K(ret));
            } else if (groupby_exprs.at(k)->same_as(*expr, &check_context)) {
              expr = groupby_exprs.at(k);
              is_existed = true;
            } else { /*do nothing.*/
            }
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
