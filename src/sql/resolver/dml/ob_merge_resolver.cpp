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
#include "sql/resolver/dml/ob_merge_resolver.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/ob_autoincrement_param.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_default_value_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
ObMergeResolver::ObMergeResolver(ObResolverParams &params)
    : ObDelUpdResolver(params),
      join_column_exprs_(),
      resolve_clause_(NONE_CLAUSE),
      insert_select_items_count_(0)
{
  column_namespace_checker_.disable_check_unique();
}

ObMergeResolver::~ObMergeResolver()
{
}

int ObMergeResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  int64_t insert_idx = INSERT_CLAUSE_NODE;
  int64_t update_idx = UPDATE_CLAUSE_NODE;

  if (OB_UNLIKELY(T_MERGE != parse_tree.type_)
      || OB_UNLIKELY(MERGE_FILED_COUNT != parse_tree.num_child_)
      || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.type_), K(parse_tree.num_child_), K(parse_tree.children_));
  } else if (OB_ISNULL(merge_stmt = create_stmt<ObMergeStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create insert stmt failed", K(merge_stmt));
  } else if (OB_FAIL(resolve_outline_data_hints())) {
    LOG_WARN("resolve outline data hints failed", K(ret));
  } else {
    if (OB_NOT_NULL(parse_tree.children_[insert_idx]) &&
          parse_tree.children_[insert_idx]->type_ != T_INSERT) {
      insert_idx = UPDATE_CLAUSE_NODE;
      update_idx = INSERT_CLAUSE_NODE;
    } else {
      insert_idx = INSERT_CLAUSE_NODE;
      update_idx = UPDATE_CLAUSE_NODE;
    }
  }

  if (OB_FAIL(ret)){

  } else if (OB_FAIL(resolve_target_relation(parse_tree.children_[TARGET_NODE]))) {
    LOG_WARN("fail to resolve target relation", K(ret));
  } else if (OB_FAIL(resolve_source_relation(parse_tree.children_[SOURCE_NODE]))) {
    LOG_WARN("fail to resolve target relation", K(ret));
  } else if (OB_FAIL(resolve_match_condition(parse_tree.children_[MATCH_COND_NODE]))) {
    LOG_WARN("fail to resolve match condition", K(ret));
  } else if (OB_FAIL(resolve_insert_clause(parse_tree.children_[insert_idx]))) {
    LOG_WARN("fail to resolve insert clause", K(ret));
  } else if (OB_FAIL(resolve_update_clause(parse_tree.children_[update_idx]))) {
    LOG_WARN("fail to resolve update clause", K(ret));
  } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT_NODE]))) {
    LOG_WARN("resolve hints failed", K(ret));
  } else if (OB_FAIL(generate_autoinc_params(merge_stmt->get_merge_table_info()))) {
    LOG_WARN("fail to save autoinc params", K(ret));
  } else if (OB_FAIL(merge_stmt->formalize_stmt(session_info_))) {
    LOG_WARN("fail to formalize stmt", K(ret));
  } else if (OB_FAIL(check_stmt_validity())) {
    LOG_WARN("failed to check subquery validity", K(ret));
  } else {
    LOG_DEBUG("check merge table info", K(merge_stmt->get_merge_table_info()));
  }

  return ret;
}

int ObMergeResolver::resolve_merge_constraint()
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  TableItem *table_item = NULL;
  if (OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(merge_stmt), K(ret));
  } else if (OB_ISNULL(table_item = merge_stmt->get_table_item_by_id(
                                    merge_stmt->get_target_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObMergeTableInfo &table_info = merge_stmt->get_merge_table_info();
    if (OB_FAIL(resolve_check_constraints(table_item, table_info.check_constraint_exprs_))) {
      LOG_WARN("failed to resolve check constraints", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}
/**
 * @brief ObMergeResolver::check_stmt_validity
 * 1. merge stmt should not have rownum
 * 2. merge into does not support trigger when the delete clause contains subquery
 * @return
 */
int ObMergeResolver::check_stmt_validity()
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool trigger_exists = false;
  bool delete_has_subquery = false;
  const ObTableSchema *table_schema = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(params_.schema_checker_) ||
      OB_ISNULL(schema_guard = params_.schema_checker_->get_schema_guard()) ||
      OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(merge_stmt), K(schema_guard), K(session_info_));
  } else if (OB_FAIL(merge_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the behavior of rownum is undefined in merge stmt", K(ret), K(merge_stmt));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the behavior of rownum in merge stmt");
  } else if (merge_stmt->get_subquery_exprs().empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_delete_condition_exprs(),
                                                                  delete_has_subquery))) {
    LOG_WARN("failed to check if expr contain subquery", K(ret));
  } else if (!delete_has_subquery) {
    /*do nothing*/
  } else if (OB_FAIL(schema_guard->get_table_schema(
             session_info_->get_effective_tenant_id(),
             merge_stmt->get_merge_table_info().ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(table_schema), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema", K(ret), K(merge_stmt->get_merge_table_info().ref_table_id_));
  } else if (OB_FAIL(table_schema->has_before_update_row_trigger(*schema_guard,
                                                                 trigger_exists))) {
    LOG_WARN("failed to check if has before update row trigger", K(ret));
  } else if (trigger_exists) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support delete subquery when before update row trigger exists", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "delete subquery when before update row trigger exists");
  } else { /*do nothing*/ }
  return ret;
}

int ObMergeResolver::get_equal_columns(ObIArray<ObString> &equal_cols)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  ObRawExpr *expr = NULL;
  if (OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(ret));
  } else {
    ObIArray<ObRawExpr *> &conds = merge_stmt->get_match_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
      ObRawExpr *left = NULL;
      ObRawExpr *right = NULL;
      if (OB_ISNULL(expr = conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr->get_expr_type() != T_OP_EQ) {
        // do nothing
      } else if (OB_ISNULL(left = expr->get_param_expr(0)) ||
                 OB_ISNULL(right = expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params are invalid", K(ret), K(left), K(right));
      } else if (left->is_column_ref_expr() &&
                 right->is_column_ref_expr() &&
                 left->get_result_type() == right->get_result_type()) {
        ObColumnRefRawExpr *lcol = static_cast<ObColumnRefRawExpr *>(left);
        ObColumnRefRawExpr *rcol = static_cast<ObColumnRefRawExpr *>(right);
        if (0 == lcol->get_column_name().case_compare(rcol->get_column_name())) {
          if (lcol->get_table_id() == merge_stmt->get_target_table_id() &&
              rcol->get_table_id() == merge_stmt->get_source_table_id()) {
            if (OB_FAIL(equal_cols.push_back(lcol->get_column_name()))) {
              LOG_WARN("failed to push back column expr", K(ret));
            }
          } else if (lcol->get_table_id() == merge_stmt->get_source_table_id() &&
                     rcol->get_table_id() == merge_stmt->get_target_table_id()) {
            if (OB_FAIL(equal_cols.push_back(rcol->get_column_name()))) {
              LOG_WARN("failed to push back column expr", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMergeResolver::resolve_target_relation(const ParseNode *target_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  TableItem *table_item = NULL;
  if (OB_ISNULL(target_node) || OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(target_node), K(merge_stmt), K(ret));
  } else if (OB_FAIL(resolve_table(*target_node, table_item))) {
    LOG_WARN("fail to resolve table", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item is NULL", K(ret));
  } else if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
    LOG_WARN("add reference table to column namespace checker failed", K(ret));
  } else if (OB_FAIL(check_need_fired_trigger(table_item))) {
    LOG_WARN("check has need fired trigger failed");
  } else if (table_item->is_generated_table()) {
    ObSelectStmt *ref_stmt = NULL;
    if (OB_ISNULL(ref_stmt = table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret));
    } else if (OB_FAIL(check_target_generated_table(*ref_stmt))) {
      LOG_WARN("failed to check target generated table", K(ret));
    } else if (OB_FALSE_IT(insert_select_items_count_ = ref_stmt->get_select_item_size())) {
    } else if (OB_FAIL(set_base_table_for_view(*table_item))) {
      LOG_WARN("set base table for insert view failed", K(ret));
    } else if (OB_FAIL(add_all_column_to_updatable_view(*merge_stmt, *table_item, false))) {
      LOG_WARN("failed add all column to updatable view", K(ret));
    } else if (is_oracle_mode() && OB_FAIL(add_default_sequence_id_to_stmt(table_item->table_id_))) {
      LOG_WARN("add default sequence id to stmt failed", K(ret));
    } else if (OB_FAIL(view_pullup_special_column_exprs())) {
      LOG_WARN("failed pullup special column exprs", K(ret));
    } else if (OB_FAIL(view_pullup_part_exprs())) {
      LOG_WARN("pullup part exprs for view failed", K(ret));
    } else {
      merge_stmt->set_target_table_id(table_item->table_id_);
    }
  } else if (table_item->is_basic_table() || table_item->is_link_table()) {
    merge_stmt->set_target_table_id(table_item->table_id_);
    const TableItem &base_table_item = table_item->get_base_table_item();
    merge_stmt->get_merge_table_info().is_link_table_ = base_table_item.is_link_table();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table item type", K(table_item->type_), K(ret));
  }
  const ObTableSchema *table_schema = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                  table_item->get_base_table_item().ref_id_,
                                                  table_schema,
                                                  table_item->is_link_table()))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (table_schema->is_oracle_tmp_table() && !params_.is_prepare_stage_) {
        session_info_->set_has_temp_table_flag();
        set_is_oracle_tmp_table(true);
        set_oracle_tmp_table_type(table_schema->is_oracle_sess_tmp_table() ? 0 : 1);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_insert_table_info(*table_item, merge_stmt->get_merge_table_info()))) {
      LOG_WARN("failed to generate insert table info", K(ret));
    } else if (OB_FAIL(resolve_merge_constraint())) {
      LOG_WARN("failed to resolve merge constraint", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObMergeResolver::check_target_generated_table(const ObSelectStmt &ref_stmt)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  if (OB_UNLIKELY(!ref_stmt.is_single_table_stmt())) {
    // ORA-38106: MERGE not supported on join view or view with INSTEAD OF trigger.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("merge target table is joined table, not support now", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "merge join view");
  } else if (OB_ISNULL(table_item = ref_stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!table_item->is_generated_table()) {
    // do nothing
  } else if (OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (OB_FAIL(SMART_CALL(check_target_generated_table(*table_item->ref_query_)))) {
    LOG_WARN("failed to check target generated table", K(ret));
  }
  return ret;
}

int ObMergeResolver::resolve_source_relation(const ParseNode *source_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  TableItem *table_item = NULL;
  if (OB_ISNULL(source_node)
      || OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K(source_node), K(merge_stmt), K(ret));
  } else if (OB_FAIL(resolve_table(*source_node, table_item))) {
    LOG_WARN("fail to resolve table", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item is NULL", K(ret));
  } else if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
    LOG_WARN("add reference table to column namespace checker failed", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is NULL", K(ret));
  } else {
    merge_stmt->set_source_table_id(table_item->table_id_);
  }
  return ret;
}

int ObMergeResolver::resolve_table(const ParseNode &parse_tree, TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  const ParseNode *table_node = NULL;
  const ParseNode *alias_node = NULL;
  if (OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != 4)
      || OB_UNLIKELY(T_ALIAS != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse tree",
             K(parse_tree.children_), K(parse_tree.num_child_), K(parse_tree.type_), K(ret));
  } else if (OB_ISNULL(table_node = parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table node", K(ret));
  } else {
    switch (table_node->type_) {
      case T_RELATION_FACTOR: {
        if (OB_FAIL(ObDMLResolver::resolve_basic_table(parse_tree, table_item))) {
          LOG_WARN("fail to resolve base or alias table factor", K(ret));
        }
        break;
      }
      case T_SELECT: {
        if (OB_FAIL(resolve_generate_table(*table_node, parse_tree.children_[1], table_item))) {
          LOG_WARN("fail to resolve generate taable", K(ret));
        }
        break;
      }
      case T_TABLE_COLLECTION_EXPRESSION: {
        if (OB_ISNULL(session_info_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        }
        ObStmtScope scope_backup = current_scope_;
        current_scope_ = T_FROM_SCOPE;
        DEFER(current_scope_ = scope_backup);
        OZ (resolve_function_table_item(*table_node, table_item));
        break;
      }
      case T_JSON_TABLE_EXPRESSION:
      case T_XML_TABLE_EXPRESSION: {
        if (OB_ISNULL(session_info_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        }
        OZ (resolve_json_table_item(*table_node, table_item));
        break;
      }
      default: {
        /* won't be here */
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("Unknown table type", "node_type", table_node->type_);
      }
    }
  }
  return ret;
}

int ObMergeResolver::resolve_column_ref_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_table_column_expr(q_name, real_ref_expr))) {
    LOG_WARN("resolve single table column item failed", K(ret));
  } else if (OB_ISNULL(real_ref_expr) || !real_ref_expr->is_column_ref_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(check_column_validity(static_cast<ObColumnRefRawExpr*>(real_ref_expr)))) {
    LOG_WARN("check column validity failed", K(ret));
  }
  return ret;
}

int ObMergeResolver::resolve_column_ref_for_subquery(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdResolver::resolve_column_ref_for_subquery(q_name, real_ref_expr))) {
    LOG_WARN("failed to resolve column ref for subquery", K(ret));
  } else if (OB_ISNULL(real_ref_expr) || OB_UNLIKELY(!real_ref_expr->is_column_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not valid column expr", K(ret), K(real_ref_expr));
  } else if (OB_FAIL(check_column_validity(static_cast<ObColumnRefRawExpr *>(real_ref_expr)))) {
    LOG_WARN("failed to check column validity", K(ret));
  }
  return ret;
}

int ObMergeResolver::check_column_validity(ObColumnRefRawExpr *col_expr)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  if (OB_ISNULL(col_expr) || OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col is null", K(ret));
  } else if (resolve_clause_ == MATCH_CLAUSE) {
    if (col_expr->get_table_id() != merge_stmt->get_target_table_id()) {
      // do nothing
    } else if (OB_FAIL(join_column_exprs_.push_back(col_expr))) {
      LOG_WARN("failed to push back join column exprs", K(ret));
    }
  } else if (resolve_clause_ == INSERT_VALUE_CLAUSE ||
             resolve_clause_ == INSERT_WHEN_CLAUSE) {
    if (OB_UNLIKELY(col_expr->get_table_id() == merge_stmt->get_target_table_id())) {
      ret = OB_ERR_INVALID_INSERT_COLUMN;
      LOG_WARN("cannot insert value", K(ret));
      LOG_USER_ERROR(OB_ERR_INVALID_INSERT_COLUMN,
                     col_expr->get_table_name().length(),
                     col_expr->get_table_name().ptr(),
                     col_expr->get_column_name().length(),
                     col_expr->get_column_name().ptr());
    }
  }
  return ret;
}

int ObMergeResolver::resolve_generate_table(const ParseNode &table_node,
                                            const ParseNode *alias_node,
                                            TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *child_stmt = NULL;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  TableItem *item = NULL;
  ObSelectResolver select_resolver(params_);
  select_resolver.set_current_level(current_level_);
  select_resolver.set_is_sub_stmt(true);
  select_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  select_resolver.set_current_view_level(current_view_level_);
  ObString alias_name;
  if (alias_node != NULL) {
    alias_name.assign_ptr((char *)(alias_node->str_value_), static_cast<int32_t>(alias_node->str_len_));
  }
  if (OB_ISNULL(merge_stmt)
      || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(merge_stmt), K(allocator_), K(ret));
  } else if (OB_FAIL(select_resolver.resolve_child_stmt(table_node))) {
    LOG_WARN("fail to resolve child stmt", K(ret));
  } else if (OB_ISNULL(child_stmt= select_resolver.get_child_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is NULL", K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_duplicated_column(*child_stmt))) {
    LOG_WARN("fail to check duplicate column", K(ret));
  } else if (OB_UNLIKELY(NULL == (item = merge_stmt->create_table_item(*allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed", K(ret));
  } else if (alias_name.empty() && OB_FAIL(merge_stmt->generate_anonymous_view_name(*allocator_, alias_name))) {
    LOG_WARN("failed to generate view name", K(ret));
  } else {
    item->ref_query_ = child_stmt;
    item->table_id_ = generate_table_id();
    item->table_name_ = alias_name;
    item->alias_name_ = alias_name;
    item->type_ = TableItem::GENERATED_TABLE;
    if (OB_FAIL(merge_stmt->add_table_item(session_info_, item))) {
      LOG_WARN("add table item failed", K(ret));
    } else {
      table_item = item;
    }
  }
  return ret;
}

int ObMergeResolver::resolve_match_condition(const ParseNode *condition_node)
{
  int ret = OB_SUCCESS;
  bool has_outer_join_symbol = false;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  column_namespace_checker_.enable_check_unique();
  resolve_clause_ = MATCH_CLAUSE;
  ObStmtScope old_scope = current_scope_;
  current_scope_ = T_ON_SCOPE;
  if (OB_ISNULL(condition_node) || OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K(condition_node), K(merge_stmt), K(ret));
  } else if (OB_FAIL(resolve_and_split_sql_expr(*condition_node,
                                                merge_stmt->get_match_condition_exprs()))) {
    LOG_WARN("fail to resolve match condition expr", K(ret));
  }
  current_scope_ = old_scope;
  resolve_clause_ = NONE_CLAUSE;
  column_namespace_checker_.disable_check_unique();
  return ret;
}

int ObMergeResolver::resolve_insert_clause(const ParseNode *insert_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  ParseNode *columns_node = NULL;
  ParseNode *value_node = NULL;
  ParseNode *where_node = NULL;
  ObArray<uint64_t> label_se_columns;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is NULL", K(ret));
  } else if (NULL == insert_node) {//doesn't has insert_clause
    /*do nothing*/
  } else if (OB_UNLIKELY(insert_node->num_child_ != 3)
             || OB_ISNULL(insert_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K(insert_node->num_child_), K(insert_node->children_), K(merge_stmt), K(ret));
  } else {
    columns_node = insert_node->children_[0];
    value_node = insert_node->children_[1];
    where_node = insert_node->children_[2];
    current_scope_ = T_INSERT_SCOPE;
    if (OB_FAIL(resolve_insert_columns(columns_node, merge_stmt->get_merge_table_info()))) {
      LOG_WARN("fail to resolve insert columns", K(ret));
    } else if (OB_FAIL(check_insert_clause())) {
      LOG_WARN("failed to check insert columns", K(ret));
    } else if (FALSE_IT(resolve_clause_ = INSERT_VALUE_CLAUSE)) {
    } else if (OB_FAIL(get_label_se_columns(merge_stmt->get_merge_table_info(), label_se_columns))) {
      LOG_WARN("failed to get label se columns", K(ret));
    } else if (OB_FAIL(resolve_insert_values(value_node, merge_stmt->get_merge_table_info(), label_se_columns))) {
      LOG_WARN("fail to resolve values", K(ret));
    } else if (OB_FAIL(add_column_for_oracle_temp_table(merge_stmt->get_merge_table_info().ref_table_id_,
                                                        merge_stmt->get_merge_table_info().table_id_,
                                                        merge_stmt))) {
      LOG_WARN("failed to add column for oracle temp table", K(ret));
    } else if (OB_FAIL(generate_column_conv_function(merge_stmt->get_merge_table_info()))) {
      LOG_WARN("failed to generate column conv function", K(ret));
    } else if (OB_FAIL(replace_gen_col_dependent_col(merge_stmt->get_merge_table_info()))) {
      LOG_WARN("failed to replace gen col dependent col", K(ret));
    } else if (FALSE_IT(resolve_clause_ = INSERT_WHEN_CLAUSE)) {
    } else if (OB_FAIL(resolve_where_conditon(
                         where_node, merge_stmt->get_insert_condition_exprs()))) {
      LOG_WARN("fail to resolve insert condition exprs", K(ret));
    }
    resolve_clause_ = NONE_CLAUSE;
    current_scope_ = T_NONE_SCOPE;
  }
  return ret;
}

int ObMergeResolver::resolve_update_clause(const ParseNode *update_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  ObSEArray<ObString, 4> equal_names;
  ParseNode *assign_node = NULL;
  ParseNode *update_condition_node = NULL;
  ParseNode *delete_condition_node = NULL;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is NULL", K(ret));
  } else if (NULL == update_node) {//doesn't has update clause
    /*do nothing*/
  } else if (OB_UNLIKELY(update_node->num_child_ != 3)
      || OB_ISNULL(update_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(merge_stmt), K(update_node->num_child_), K(update_node->children_), K(ret));
  } else if (OB_FAIL(get_equal_columns(equal_names))) {
    LOG_WARN("failed to get equal columns", K(ret));
  } else if (OB_FAIL(column_namespace_checker_.set_equal_columns(equal_names))) {
    LOG_WARN("failed to set equal columns", K(ret));
  } else {
    column_namespace_checker_.enable_check_unique();
    assign_node = update_node->children_[0];
    update_condition_node = update_node->children_[1];
    delete_condition_node = update_node->children_[2];
    current_scope_ = T_UPDATE_SCOPE;
    if (OB_FAIL(resolve_insert_update_assignment(assign_node, merge_stmt->get_merge_table_info()))) {
      LOG_WARN("fail to resolve insert update assignment", K(ret));
    } else if (OB_FAIL(resolve_where_conditon(
                         update_condition_node, merge_stmt->get_update_condition_exprs()))) {
      LOG_WARN("fail to resolve update condition exprs", K(ret));
    } else if (OB_FAIL(resolve_where_conditon(
                         delete_condition_node, merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("fail to resolve delete condition exprs", K(ret));
    } else {
      // 新引擎下，全部替换为update后的值，这样就不需要在计算delete condition时候，强制将expr的值弄成新值
      ObMergeTableInfo& merge_info = merge_stmt->get_merge_table_info();
      ObRawExprCopier copier(*params_.expr_factory_);
      if (OB_INVALID_ID == merge_info.table_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid table assignment", K(merge_info.table_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < merge_info.assignments_.count(); ++i) {
          if (OB_FAIL(copier.add_replaced_expr(merge_info.assignments_.at(i).column_expr_,
                                               merge_info.assignments_.at(i).expr_))) {
            LOG_WARN("failed to add replaced expr", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(copier.add_skipped_expr(merge_stmt->get_subquery_exprs(), false))) {
            LOG_WARN("failed to add uncopy exprs", K(ret));
          } else if (OB_FAIL(copier.copy_on_replace(merge_stmt->get_delete_condition_exprs(),
                                                    merge_stmt->get_delete_condition_exprs()))) {
            LOG_WARN("failed to copy on replace delete conditions", K(ret));
          }
        }
      }
    }
    current_scope_ = T_NONE_SCOPE;
    column_namespace_checker_.disable_check_unique();
    column_namespace_checker_.clear_equal_columns();
  }
  return ret;
}

int ObMergeResolver::resolve_where_conditon(const ParseNode *condition_node,
                                            ObIArray<ObRawExpr*> &condition_exprs)
{
  int ret = OB_SUCCESS;
  const ParseNode *exprs_node = NULL;
  current_scope_ = T_WHERE_SCOPE;
  if (NULL == condition_node) {
    //do nothing
  } else if (OB_UNLIKELY(condition_node->num_child_ != 2)
             || OB_ISNULL(condition_node->children_)
             || OB_ISNULL(exprs_node = condition_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition parser tree", K(condition_node->num_child_), K(condition_node->children_), K(ret));
  } else if (OB_FAIL(resolve_and_split_sql_expr(*exprs_node, condition_exprs))) {
    LOG_WARN("fail to resolve and splict sql expr", K(ret));
  }
  current_scope_ = T_NONE_SCOPE;
  return ret;
}

int ObMergeResolver::find_value_desc(ObInsertTableInfo &table_info,
                                     uint64_t column_id,
                                     ObRawExpr *&column_ref)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObIArray<ObColumnRefRawExpr*> &value_desc = table_info.values_desc_;
  int64_t value_vector_cnt = table_info.values_vector_.count();
  ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < value_desc.count() && OB_ENTRY_NOT_EXIST == ret; i++) {
    ObColumnRefRawExpr *expr = value_desc.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get values expr", K(i), K(value_desc), K(ret));
    } else if (column_id == expr->get_column_id()) {
      ret = OB_SUCCESS;
      idx = i;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(idx > value_vector_cnt - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(idx), K(value_vector_cnt), K(value_desc.count()), K(ret));
  } else {
    column_ref = table_info.values_vector_.at(idx);
    if (T_QUESTIONMARK == column_ref->get_expr_type()) {
      OZ (column_ref->add_flag(IS_TABLE_ASSIGN));
      OX (column_ref->set_result_type(value_desc.at(idx)->get_result_type()));
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMergeResolver::check_insert_clause()
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  if (OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_stmt->get_values_desc().count(); ++i) {
    ObColumnRefRawExpr *col_expr = NULL;
    if (OB_ISNULL(col_expr = merge_stmt->get_values_desc().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (OB_UNLIKELY(col_expr->get_table_id() != merge_stmt->get_target_table_id())) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("cannot insert column", K(ret),
               K(merge_stmt->get_table_items()),
               K(merge_stmt->get_values_desc()));
      ObString column_name = concat_qualified_name("",
                                                   col_expr->get_table_name(),
                                                   col_expr->get_column_name());
      ObString scope_name = ObString::make_string(get_scope_name(T_INSERT_SCOPE));
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                     column_name.length(),
                     column_name.ptr(),
                     scope_name.length(),
                     scope_name.ptr());
    }
  }
  return ret;
}

int ObMergeResolver::add_assignment(ObIArray<ObTableAssignment> &assigns,
                                    const TableItem *table_item,
                                    const ColumnItem *col_item,
                                    ObAssignment &assign)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *column_expr = NULL;
  ObMergeStmt *merge_stmt = NULL;
  // check validity of assign at first
  if (OB_ISNULL(column_expr = assign.column_expr_) ||
      OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column expr is null", K(ret));
  } else if (assign.is_implicit_) {
    // skip
  } else if (OB_UNLIKELY(assign.column_expr_->get_table_id() !=
                         merge_stmt->get_target_table_id())) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("invalid column expr in the update clause", K(ret));
    ObString column_name = concat_qualified_name("",
                                                 column_expr->get_table_name(),
                                                 column_expr->get_column_name());
    ObString scope_name = ObString::make_string(get_scope_name(T_UPDATE_SCOPE));
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                   column_name.length(),
                   column_name.ptr(),
                   scope_name.length(),
                   scope_name.ptr());
  } else if (ObRawExprUtils::find_expr(join_column_exprs_, column_expr)) {
    ret = OB_ERR_UPDATE_ON_EXPR;
    LOG_WARN("cannot update column in match condition", K(assign.column_expr_));
    LOG_USER_ERROR(OB_ERR_UPDATE_ON_EXPR,
                   column_expr->get_table_name().length(),
                   column_expr->get_table_name().ptr(),
                   column_expr->get_column_name().length(),
                   column_expr->get_column_name().ptr());
  } else if (!assigns.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.at(0).assignments_.count(); ++i) {
      const ObAssignment &other = assigns.at(0).assignments_.at(i);
      if (other.is_implicit_) {
        // skip
      } else if (other.column_expr_ == assign.column_expr_) {
        ret = OB_ERR_FIELD_SPECIFIED_TWICE;
        LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE,
                       to_cstring(assign.column_expr_->get_column_name()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDelUpdResolver::add_assignment(assigns,
                                                 table_item,
                                                 col_item,
                                                 assign))) {
      LOG_WARN("failed to add assignment", K(ret));
    }
  }
  return ret;
}

int ObMergeResolver::mock_values_column_ref(const ObColumnRefRawExpr *column_ref)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = get_merge_stmt();
  ObColumnRefRawExpr *value_desc = NULL;
  if (OB_ISNULL(column_ref) || OB_ISNULL(merge_stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_ref), K(merge_stmt), KP_(params_.expr_factory));
  } else {
    bool found_column = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_stmt->get_values_desc().count(); ++i) {
      if (OB_ISNULL(value_desc = merge_stmt->get_values_desc().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value desc is null");
      } else if (column_ref->get_column_id() == value_desc->get_column_id()) {
        found_column = true;
        break;
      }
    }
    if (found_column) {
      //ignore generating new column
    } else if (OB_FAIL(merge_stmt->get_values_desc().push_back(const_cast<ObColumnRefRawExpr*>(column_ref)))) {
      LOG_WARN("failed to push back values desc", K(ret), K(*value_desc));
    }
  }
  return ret;
}

int ObMergeResolver::resolve_merge_generated_table_columns(ObDMLStmt *stmt,
                                                           TableItem *table,
                                                           ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *ref_stmt = NULL;
  if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_generated_table())
      || OB_ISNULL(ref_stmt = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(ref_stmt));
  } else if (OB_UNLIKELY(insert_select_items_count_ <= 0 ||
                         insert_select_items_count_ > ref_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected select items count", K(ret), K(insert_select_items_count_),
                                              K(ref_stmt->get_select_item_size()));
  } else {
    ObString col_name; // not used
    ColumnItem *col_item = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_select_items_count_; i++) {
      if (OB_FAIL(resolve_generated_table_column_item(*table, col_name, col_item,
                                                      stmt, OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("resolve generate table item failed", K(ret));
      } else if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is NULL", K(ret));
      } else if (OB_FAIL(column_items.push_back(*col_item))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  return ret;
}


}//sql
}//oceanbase
