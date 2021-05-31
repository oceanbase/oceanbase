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
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {
ObMergeResolver::ObMergeResolver(ObResolverParams& params)
    : ObInsertResolver(params), join_column_exprs_(), resolve_clause_(NONE_CLAUSE)
{
  column_namespace_checker_.disable_check_unique();
}

ObMergeResolver::~ObMergeResolver()
{}

int ObMergeResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = NULL;
  if (OB_UNLIKELY(T_MERGE != parse_tree.type_) || OB_UNLIKELY(MERGE_FILED_COUNT != parse_tree.num_child_) ||
      OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.type_), K(parse_tree.num_child_), K(parse_tree.children_));
  } else if (OB_ISNULL(merge_stmt = create_stmt<ObMergeStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create insert stmt failed", K(merge_stmt));
  } else if (OB_FAIL(resolve_target_relation(parse_tree.children_[TARGET_NODE]))) {
    LOG_WARN("fail to resolve target relation", K(ret));
  } else if (OB_FAIL(set_table_columns())) {
    LOG_WARN("fail to set table columns", K(ret));
  } else if (OB_FAIL(resolve_check_constraints(merge_stmt->get_table_item_by_id(merge_stmt->get_insert_table_id())))) {
    LOG_WARN("failed to resolve check constraints", K(ret));
  } else if (OB_FAIL(resolve_source_relation(parse_tree.children_[SOURCE_NODE]))) {
    LOG_WARN("fail to resolve target relation", K(ret));
  } else if (OB_FAIL(resolve_match_condition(parse_tree.children_[MATCH_COND_NODE]))) {
    LOG_WARN("fail to resolve match condition", K(ret));
  } else if (OB_FAIL(resolve_insert_clause(parse_tree.children_[INSERT_CLAUSE_NODE]))) {
    LOG_WARN("fail to resolve insert clause", K(ret));
  } else if (OB_FAIL(resolve_update_clause(parse_tree.children_[UPDATE_CLAUSE_NODE]))) {
    LOG_WARN("fail to resolve update clause", K(ret));
  } else if (OB_FAIL(add_delete_refered_column_ids())) {
    LOG_WARN("fail to add delete refered column ids", K(ret));
  } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT_NODE]))) {
    LOG_WARN("resolve hints failed", K(ret));
  } else if (OB_FAIL(replace_col_ref_for_check_cst())) {
    LOG_WARN("fail to replace column reference expr for check constraint", K(ret));
  } else if (OB_FAIL(add_rowkey_columns())) {
    LOG_WARN("fail to add rowkey column", K(ret));
  } else if (OB_FAIL(generate_rowkeys_exprs())) {
    LOG_WARN("fail to generate rowkey exprs", K(ret));
  } else if (OB_FAIL(save_autoinc_params())) {
    LOG_WARN("fail to save autoinc params", K(ret));
  } else if (OB_FAIL(resolve_multi_table_merge_info())) {
    LOG_WARN("failed to resolve multi table merge into", K(ret));
  } else if (OB_FAIL(merge_stmt->formalize_stmt(session_info_))) {
    LOG_WARN("fail to formalize stmt", K(ret));
  } else if (OB_FAIL(check_stmt_validity())) {
    LOG_WARN("failed to check subquery validity", K(ret));
  }

  // Replace column in shadow key with conv expr
  // The column in the shadow key is replaced with conv expr
  // because of each index_table in the multi part insert
  // The access_exprs_ corresponding to the sub-plan will directly
  // depend on the output of multi_part_insert (conv_exprs)
  // In order to calculate the shadow key,
  // the value of conv_exprs can be used directly, so it needs to be replaced here;
  //
  // For replace/insert_up, access_exprs_ depends on table_columns, so there is no need to replace
  if (OB_SUCC(ret) && session_info_->use_static_typing_engine() && merge_stmt->has_insert_clause()) {
    CK(merge_stmt->get_all_table_columns().count() == 1);
    ObIArray<IndexDMLInfo>& index_infos = merge_stmt->get_all_table_columns().at(0).index_dml_infos_;
    common::ObIArray<ObRawExpr*>& conv_columns = merge_stmt->get_column_conv_functions();
    const common::ObIArray<ObColumnRefRawExpr*>* table_columns = merge_stmt->get_table_columns();
    CK(OB_NOT_NULL(table_columns));
    CK(conv_columns.count() == table_columns->count());
    for (int64_t i = 0; i < index_infos.count() && OB_SUCC(ret); i++) {
      IndexDMLInfo& index_info = index_infos.at(i);
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < index_info.column_exprs_.count(); col_idx++) {
        ObColumnRefRawExpr* col_expr = index_info.column_exprs_.at(col_idx);
        CK(OB_NOT_NULL(col_expr));
        if (is_shadow_column(col_expr->get_column_id())) {
          ObRawExpr* spk_expr = col_expr->get_dependant_expr();
          for (int64_t k = 0; OB_SUCC(ret) && k < table_columns->count(); k++) {
            OZ(ObRawExprUtils::replace_ref_column(spk_expr, table_columns->at(k), conv_columns.at(k)));
          }  // for assignment end
        }
      }
    }
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
  bool delete_has_subquery = false;
  const ObTableSchema* table_schema = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(params_.schema_checker_) ||
      OB_ISNULL(schema_guard = params_.schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(merge_stmt), K(schema_guard));
  } else if (OB_FAIL(merge_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the behavior of rownum is undefined in merge stmt", K(ret), K(merge_stmt));
  } else if (merge_stmt->get_subquery_exprs().empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(
                 merge_stmt->get_delete_condition_exprs(), NULL, delete_has_subquery))) {
    LOG_WARN("failed to check if expr contain subquery", K(ret));
  } else if (!delete_has_subquery) {
    /*do nothing*/
  } else if (OB_FAIL(schema_guard->get_table_schema(merge_stmt->get_ref_table_id(), table_schema)) ||
             OB_ISNULL(table_schema)) {
    LOG_WARN("failed to get table schema", K(table_schema), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObMergeResolver::get_equal_columns(ObIArray<ObString>& equal_cols)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = NULL;
  ObRawExpr* expr = NULL;
  if (OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(ret));
  } else {
    ObIArray<ObRawExpr*>& conds = merge_stmt->get_match_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
      ObRawExpr* left = NULL;
      ObRawExpr* right = NULL;
      if (OB_ISNULL(expr = conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr->get_expr_type() != T_OP_EQ) {
        // do nothing
      } else if (OB_ISNULL(left = expr->get_param_expr(0)) || OB_ISNULL(right = expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params are invalid", K(ret), K(left), K(right));
      } else if (left->is_column_ref_expr() && right->is_column_ref_expr() &&
                 left->get_result_type() == right->get_result_type()) {
        ObColumnRefRawExpr* lcol = static_cast<ObColumnRefRawExpr*>(left);
        ObColumnRefRawExpr* rcol = static_cast<ObColumnRefRawExpr*>(right);
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

int ObMergeResolver::resolve_target_relation(const ParseNode* target_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  TableItem* table_item = NULL;
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
  } else if (table_item->is_view_table_ || table_item->is_generated_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("merge target table is view, not support now", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "merge view");
  } else {
    merge_stmt->set_target_table(table_item->table_id_);
  }
  return ret;
}

int ObMergeResolver::resolve_source_relation(const ParseNode* source_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  TableItem* table_item = NULL;
  if (OB_ISNULL(source_node) || OB_ISNULL(merge_stmt)) {
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
    merge_stmt->set_source_table(table_item->table_id_);
  }
  return ret;
}

int ObMergeResolver::resolve_table(const ParseNode& parse_tree, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  const ParseNode* table_node = NULL;
  const ParseNode* alias_node = NULL;
  if (OB_ISNULL(parse_tree.children_) || OB_UNLIKELY(parse_tree.num_child_ != 4) ||
      OB_UNLIKELY(T_ALIAS != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse tree", K(parse_tree.children_), K(parse_tree.num_child_), K(parse_tree.type_), K(ret));
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
        alias_node = parse_tree.children_[1];
        if (OB_FAIL(resolve_generate_table(*table_node, alias_node, table_item))) {
          LOG_WARN("fail to resolve generate taable", K(ret));
        }
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

int ObMergeResolver::resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  const TableItem* table_item = NULL;
  ColumnItem* col_item = NULL;
  const bool include_hidden = false;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge_stmt is null", K(merge_stmt), K(session_info_), K(ret));
  } else if (OB_FAIL(column_namespace_checker_.check_table_column_namespace(q_name, table_item))) {
    LOG_WARN("check table column namespace failed", K(ret), K(q_name));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is NULL", K(ret));
  } else if (table_item->is_basic_table()) {
    if (OB_FAIL(resolve_basic_column_item(*table_item, q_name.col_name_, include_hidden, col_item))) {
      LOG_WARN("resolve basic column item failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (table_item->is_generated_table()) {
    if (OB_FAIL(resolve_generated_table_column_item(*table_item, q_name.col_name_, col_item))) {
      LOG_WARN("resolve generated table column failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_column_validity(col_item->expr_))) {
      LOG_WARN("check column validity failed", K(ret));
    } else {
      real_ref_expr = col_item->expr_;
    }
  }
  return ret;
}

int ObMergeResolver::resolve_column_ref_for_subquery(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertResolver::resolve_column_ref_for_subquery(q_name, real_ref_expr))) {
    LOG_WARN("failed to resolve column ref for subquery", K(ret));
  } else if (OB_ISNULL(real_ref_expr) || OB_UNLIKELY(!real_ref_expr->is_column_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not valid column expr", K(ret), K(real_ref_expr));
  } else if (OB_FAIL(check_column_validity(static_cast<ObColumnRefRawExpr*>(real_ref_expr)))) {
    LOG_WARN("failed to check column validity", K(ret));
  }
  return ret;
}

int ObMergeResolver::check_column_validity(ObColumnRefRawExpr* col_expr)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = NULL;
  if (OB_ISNULL(col_expr) || OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col is null", K(ret));
  } else if (resolve_clause_ == MATCH_CLAUSE) {
    if (col_expr->get_table_id() != merge_stmt->get_target_table_id()) {
      // do nothing
    } else if (OB_FAIL(join_column_exprs_.push_back(col_expr))) {
      LOG_WARN("failed to push back join column exprs", K(ret));
    }
  } else if (resolve_clause_ == INSERT_VALUE_CLAUSE || resolve_clause_ == INSERT_WHEN_CLAUSE) {
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

int ObMergeResolver::resolve_generate_table(
    const ParseNode& table_node, const ParseNode* alias_node, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* child_stmt = NULL;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  TableItem* item = NULL;
  ObSelectResolver select_resolver(params_);
  ObString alias_name;
  select_resolver.set_current_level(current_level_);
  select_resolver.set_in_subquery(true);
  select_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(merge_stmt), K(allocator_), K(ret));
  } else if (OB_FAIL(select_resolver.resolve_child_stmt(table_node))) {
    LOG_WARN("fail to resolve child stmt", K(ret));
  } else if (OB_ISNULL(child_stmt = select_resolver.get_child_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is NULL", K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_duplicated_column(*child_stmt))) {
    LOG_WARN("fail to check duplicate column", K(ret));
  } else if (OB_UNLIKELY(NULL == (item = merge_stmt->create_table_item(*allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed", K(ret));
  } else {
    if (alias_node != NULL) {
      alias_name.assign_ptr((char*)(alias_node->str_value_), static_cast<int32_t>(alias_node->str_len_));
    }
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

int ObMergeResolver::resolve_match_condition(const ParseNode* condition_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  column_namespace_checker_.enable_check_unique();
  resolve_clause_ = MATCH_CLAUSE;
  if (OB_ISNULL(condition_node) || OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K(condition_node), K(merge_stmt), K(ret));
  } else if (OB_FAIL(resolve_and_split_sql_expr(*condition_node, merge_stmt->get_match_condition_exprs()))) {
    LOG_WARN("fail to resolve match condition expr", K(ret));
  }
  resolve_clause_ = NONE_CLAUSE;
  column_namespace_checker_.disable_check_unique();
  return ret;
}

int ObMergeResolver::resolve_insert_clause(const ParseNode* insert_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  ParseNode* columns_node = NULL;
  ParseNode* value_node = NULL;
  ParseNode* where_node = NULL;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is NULL", K(ret));
  } else if (NULL == insert_node) {  // doesn't has insert_clause
    merge_stmt->set_insert_clause(false);
  } else if (OB_UNLIKELY(insert_node->num_child_ != 3) || OB_ISNULL(insert_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K(insert_node->num_child_), K(insert_node->children_), K(merge_stmt), K(ret));
  } else {
    merge_stmt->set_insert_clause(true);
    columns_node = insert_node->children_[0];
    value_node = insert_node->children_[1];
    where_node = insert_node->children_[2];
    current_scope_ = T_INSERT_SCOPE;
    if (OB_FAIL(resolve_insert_columns(columns_node))) {
      LOG_WARN("fail to resolve insert columns", K(ret));
    } else if (OB_FAIL(check_insert_clause())) {
      LOG_WARN("failed to check insert columns", K(ret));
    } else if (FALSE_IT(resolve_clause_ = INSERT_VALUE_CLAUSE)) {
    } else if (OB_FAIL(resolve_insert_values(value_node))) {
      LOG_WARN("fail to resolve values", K(ret));
    } else if (OB_FAIL(add_column_conv_function())) {
      LOG_WARN("fail to add column conv function", K(ret));
    } else if (OB_FAIL(expand_insert_value_exprs())) {
      LOG_WARN("failed to expand generated column", K(ret));
    } else if (FALSE_IT(resolve_clause_ = INSERT_WHEN_CLAUSE)) {
    } else if (OB_FAIL(resolve_where_conditon(where_node, merge_stmt->get_insert_condition_exprs()))) {
      LOG_WARN("fail to resolve insert condition exprs", K(ret));
    }
    resolve_clause_ = NONE_CLAUSE;
    current_scope_ = T_NONE_SCOPE;
  }
  return ret;
}

int ObMergeResolver::expand_insert_value_exprs()
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = NULL;
  if (OB_ISNULL(merge_stmt = get_merge_stmt()) || OB_ISNULL(merge_stmt->get_table_columns()) ||
      OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(ret), K(merge_stmt), K(merge_stmt->get_table_columns()), K(session_info_));
  } else {
    const ObIArray<ObColumnRefRawExpr*>& columns = *(merge_stmt->get_table_columns());
    ObIArray<ObRawExpr*>& values = merge_stmt->get_column_conv_functions();
    if (OB_UNLIKELY(columns.count() != values.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column count does not match values count", K(ret), K(columns.count()), K(values.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      if (OB_ISNULL(columns.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (!columns.at(i)->is_generated_column()) {
        // do nothing
      } else if (OB_FAIL(expand_value_expr(values.at(i), columns, values))) {
        LOG_WARN("failed to expand value expr", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeResolver::expand_value_expr(
    ObRawExpr*& expr, const ObIArray<ObColumnRefRawExpr*>& columns, const ObIArray<ObRawExpr*>& values)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (!expr->is_column_ref_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(expand_value_expr(expr->get_param_expr(i), columns, values))) {
        LOG_WARN("failed to expand value expr", K(ret));
      }
    }
  } else {
    bool found = false;
    ObColumnRefRawExpr* col = static_cast<ObColumnRefRawExpr*>(expr);
    if (col->is_generated_column()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("a generated column should not be created from another generated column", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < columns.count(); ++i) {
      if (columns.at(i) == expr) {
        expr = values.at(i);
        found = true;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find new value for column", K(ret), K(expr), K(*expr), K(*get_stmt()));
    }
  }
  return ret;
}

int ObMergeResolver::resolve_update_clause(const ParseNode* update_node)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  ObSEArray<ObString, 4> equal_names;
  ParseNode* assign_node = NULL;
  ParseNode* update_condition_node = NULL;
  ParseNode* delete_condition_node = NULL;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is NULL", K(ret));
  } else if (NULL == update_node) {  // doesn't has update clause
    merge_stmt->set_update_clause(false);
  } else if (OB_UNLIKELY(update_node->num_child_ != 3) || OB_ISNULL(update_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(merge_stmt), K(update_node->num_child_), K(update_node->children_), K(ret));
  } else if (OB_FAIL(get_equal_columns(equal_names))) {
    LOG_WARN("failed to get equal columns", K(ret));
  } else if (OB_FAIL(column_namespace_checker_.set_equal_columns(equal_names))) {
    LOG_WARN("failed to set equal columns", K(ret));
  } else {
    column_namespace_checker_.enable_check_unique();
    merge_stmt->set_update_clause(true);
    assign_node = update_node->children_[0];
    update_condition_node = update_node->children_[1];
    delete_condition_node = update_node->children_[2];
    current_scope_ = T_UPDATE_SCOPE;
    if (OB_FAIL(resolve_insert_update_assignment(assign_node))) {
      LOG_WARN("fail to resolve insert update assignment", K(ret));
    } else if (OB_FAIL(resolve_where_conditon(update_condition_node, merge_stmt->get_update_condition_exprs()))) {
      LOG_WARN("fail to resolve update condition exprs", K(ret));
    } else if (OB_FAIL(resolve_where_conditon(delete_condition_node, merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("fail to resolve delete condition exprs", K(ret));
    } else if (session_info_->use_static_typing_engine()) {
      // Under the new engine, all are replaced with the updated value,
      // so there is no need to force the value of expr to be a new value
      // when calculating the delete condition
      ObTablesAssignments& tas = merge_stmt->get_table_assignments();
      if (1 != tas.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("oracle mode don't support multi table update grammar", K(ret));
      } else {
        ObTableAssignment& ta = tas.at(0);
        if (OB_INVALID_ID == ta.table_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid table assignment", K(ta.table_id_));
        } else {
          for (uint64_t i = 0; OB_SUCC(ret) && i < merge_stmt->get_delete_condition_exprs().count(); ++i) {
            if (OB_FAIL(ObTableAssignment::expand_expr(
                    tas.at(0).assignments_, merge_stmt->get_delete_condition_exprs().at(i)))) {
              LOG_WARN("expand generated column expr failed", K(ret));
            }
          }
        }
      }
    }
    current_scope_ = T_NONE_SCOPE;
    column_namespace_checker_.disable_check_unique();
    column_namespace_checker_.clear_equal_columns();
    // due to the bad design of the merge operator, assign value expr should not be shared
    // consider t(c1, c2 generated as c1 + 1)
    // c1 = column_conv(c1) (expr1), c2 = column_conv(c1) + 1 (expr2)
    // the computation of expr2 should not rely on the result of expr1
    if (OB_SUCC(ret)) {
      ObTableAssignment& assigns = merge_stmt->get_table_assignments().at(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < assigns.assignments_.count(); ++i) {
        ObAssignment& assign = assigns.assignments_.at(i);
        if (OB_ISNULL(assign.expr_) || OB_ISNULL(assign.column_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("assign is invalid", K(ret));
        } else if (!assign.column_expr_->is_generated_column()) {
          // do nothing
        } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                       *params_.expr_factory_, assign.expr_, assign.expr_, COPY_REF_DEFAULT))) {
          LOG_WARN("failed to copy expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMergeResolver::resolve_where_conditon(const ParseNode* condition_node, ObIArray<ObRawExpr*>& condition_exprs)
{
  int ret = OB_SUCCESS;
  const ParseNode* exprs_node = NULL;
  current_scope_ = T_WHERE_SCOPE;
  if (NULL == condition_node) {
    // do nothing
  } else if (OB_UNLIKELY(condition_node->num_child_ != 2) || OB_ISNULL(condition_node->children_) ||
             OB_ISNULL(exprs_node = condition_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition parser tree", K(condition_node->num_child_), K(condition_node->children_), K(ret));
  } else if (OB_FAIL(resolve_and_split_sql_expr(*exprs_node, condition_exprs))) {
    LOG_WARN("fail to resolve and splict sql expr", K(ret));
  }
  current_scope_ = T_NONE_SCOPE;
  return ret;
}

int ObMergeResolver::find_value_desc(uint64_t column_id, ObRawExpr*& output_expr, uint64_t index /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  int64_t idx = 0;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(merge_stmt));
  } else {
    ObIArray<ObColumnRefRawExpr*>& value_desc = merge_stmt->get_values_desc();
    int64_t value_vector_cnt = merge_stmt->get_value_vectors().count();
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; i < value_desc.count() && OB_ENTRY_NOT_EXIST == ret; i++) {
      ObColumnRefRawExpr* expr = value_desc.at(i);
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
      output_expr = merge_stmt->get_value_vectors().at(idx);
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  UNUSED(index);
  return ret;
}

int ObMergeResolver::replace_column_ref(ObArray<ObRawExpr*>* value_row, ObRawExpr*& expr)
{
  UNUSED(expr);
  UNUSED(value_row);
  return OB_SUCCESS;
}

int ObMergeResolver::add_rowkey_columns()
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is NULL", K(ret));
  } else if (merge_stmt->has_update_clause()) {
    // Because add rowkey will be executed when parsing assignment expr,
    // so there is no need to call it separately
  } else if (OB_FAIL(ObInsertResolver::add_rowkey_columns_for_insert_up())) {
    LOG_WARN("fail to add rowkey columns", K(ret));
  }
  return ret;
}

int ObMergeResolver::generate_rowkeys_exprs()
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(merge_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument with NULL pointer", K(merge_stmt), K(schema_checker_), K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(merge_stmt->get_insert_base_tid(), table_schema))) {
    LOG_WARN("fail to get table schema", K(table_schema), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_schema), K(ret));
  } else {
    const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
    const ObColumnSchemaV2* column_schema = NULL;
    ObColumnRefRawExpr* column_expr = NULL;
    ObRawExpr* raw_expr = NULL;
    for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
      uint64_t column_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column_id from rowkey_info", K(ret));
      } else if (NULL != (raw_expr = merge_stmt->get_column_expr_by_id(merge_stmt->get_insert_table_id(), column_id))) {
        column_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
      } else if (OB_ISNULL(column_schema = (table_schema->get_column_schema(column_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else if (OB_FAIL(build_rowkey_expr(merge_stmt->get_insert_table_id(), *column_schema, column_expr))) {
        LOG_WARN("fail to build rowkey expr", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_stmt->add_rowkey_column_expr(column_expr))) {
        LOG_WARN("fail to add rowkey column expr", KPC(column_expr), K(ret));
      }
    }
  }
  return ret;
}

int ObMergeResolver::build_rowkey_expr(
    uint64_t table_id, const ObColumnSchemaV2& column_schema, ObColumnRefRawExpr*& column_expr)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  const TableItem* table_item = NULL;
  ColumnItem dummy_col_item;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(merge_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K(params_.expr_factory_), K(merge_stmt), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*(params_.expr_factory_), column_schema, column_expr))) {
    LOG_WARN("fail to build column expr", K(ret));
  } else if (OB_ISNULL(column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column expr is NULL", K(ret));
  } else if (OB_ISNULL(table_item = merge_stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(table_id), K(ret));
  } else {
    column_expr->set_ref_id(table_id, column_schema.get_column_id());
    column_expr->set_column_attr(table_item->get_table_name(), column_schema.get_column_name_str());
    dummy_col_item.table_id_ = column_expr->get_table_id();
    dummy_col_item.column_id_ = column_expr->get_column_id();
    dummy_col_item.column_name_ = column_expr->get_column_name();
    dummy_col_item.set_default_value(column_schema.get_cur_default_value());
    dummy_col_item.expr_ = column_expr;
    LOG_DEBUG("fill dummy_col_item", K(dummy_col_item), K(column_schema.is_default_expr_v2_column()), K(lbt()));
    // The column processed in this function must not be in column_item,
    // so there is no need to judge duplicates here, just add directly
    if (OB_FAIL(merge_stmt->add_column_item(dummy_col_item))) {
      LOG_WARN("add column item to stmt failed", K(ret));
    } else if (OB_FAIL(column_expr->formalize(session_info_))) {
      LOG_WARN("fail to formalize rowkey", KPC(column_expr), K(ret));
    }
  }
  return ret;
}

int ObMergeResolver::check_insert_clause()
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = NULL;
  if (OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_stmt->get_values_desc().count(); ++i) {
    ObColumnRefRawExpr* col_expr = NULL;
    if (OB_ISNULL(col_expr = merge_stmt->get_values_desc().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (OB_UNLIKELY(col_expr->get_table_id() != merge_stmt->get_target_table_id())) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("cannot insert column", K(ret), K(merge_stmt->get_table_items()), K(merge_stmt->get_values_desc()));
      ObString column_name = concat_qualified_name("", col_expr->get_table_name(), col_expr->get_column_name());
      ObString scope_name = ObString::make_string(get_scope_name(T_INSERT_SCOPE));
      LOG_USER_ERROR(
          OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
    }
  }
  return ret;
}

int ObMergeResolver::add_delete_refered_column_ids()
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge stmt", K(ret));
  } else if (merge_stmt->get_delete_condition_exprs().count() == 0) {
    // there is no delete condition, do nothing
  } else if (OB_FAIL(add_delete_refered_column_exprs(merge_stmt->get_insert_table_id()))) {
    LOG_WARN("fail to add delete refered column expr", K(ret));
  } else {
    const ObIArray<TableColumns>& all_table_columns = merge_stmt->get_all_table_columns();
    CK(all_table_columns.count() >= 1);
    CK(all_table_columns.at(0).index_dml_infos_.count() >= 1);
  }
  return ret;
}
int ObMergeResolver::add_delete_refered_column_exprs(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  if (NULL == schema_checker_ || NULL == merge_stmt) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "schema_checker_ or merged_stmt is null");
  }
  const ObTableSchema* schema = NULL;
  // add rowkey columns
  IndexDMLInfo* primary_dml_info = NULL;
  const TableItem* table_item = NULL;
  CK(OB_NOT_NULL(merge_stmt));
  CK(OB_NOT_NULL(table_item = merge_stmt->get_table_item_by_id(table_id)));
  if (OB_SUCC(ret)) {
    if (NULL == (primary_dml_info = merge_stmt->get_or_add_table_columns(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table columns", K(table_id));
    } else if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table_item, primary_dml_info->column_exprs_))) {
      LOG_WARN("add all rowkey columns to stmt failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t binlog_row_image = ObBinlogRowImage::FULL;
    if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
      LOG_WARN("fail to get binlog row image", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(table_item->ref_id_, schema))) {
      SQL_RESV_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
    } else if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(ret), K(table_item->ref_id_));
    } else if (need_all_columns(*schema, binlog_row_image)) {
      if (OB_FAIL(add_all_columns_to_stmt(*table_item, primary_dml_info->column_exprs_))) {
        LOG_WARN("fail to add all column to stmt", K(ret), K(table_id));
      }
    } else if (OB_FAIL(add_delete_related_column_to_stmt(table_id, primary_dml_info->column_exprs_))) {
      LOG_WARN("fail to add relate column to stmt", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObMergeResolver::add_delete_related_column_to_stmt(uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& column_items)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* index_schema = NULL;
  uint64_t idx_tids[OB_MAX_INDEX_PER_TABLE];
  int64_t idx_count = sizeof(idx_tids);
  ObMergeStmt* merge_stmt = get_merge_stmt();
  IndexDMLInfo* primary_dml_info = NULL;
  const TableItem* table_item = NULL;
  CK(OB_NOT_NULL(merge_stmt));
  CK(OB_NOT_NULL(table_item = merge_stmt->get_table_item_by_id(table_id)));
  if (NULL == schema_checker_ || NULL == merge_stmt) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "schema_checker_ or merge_stmt is null");
  } else if (NULL == (primary_dml_info = merge_stmt->get_or_add_table_columns(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table columns", K(table_id));
  }
  if (OB_SUCC(ret)) {
    uint64_t base_table_id = table_item->get_base_table_item().ref_id_;
    if (OB_FAIL(schema_checker_->get_can_write_index_array(base_table_id, idx_tids, idx_count))) {
      SQL_RESV_LOG(WARN, "get can write index array failed", K(ret));
    }
  }
  // for each index, all all its rowkey
  for (int64_t i = 0; OB_SUCC(ret) && i < idx_count; ++i) {
    column_items.reset();
    if (OB_FAIL(schema_checker_->get_table_schema(idx_tids[i], index_schema))) {
      SQL_RESV_LOG(WARN, "get index schema failed", "index_id", idx_tids[i]);
    } else if (OB_FAIL(add_all_index_rowkey_to_stmt(*table_item, index_schema, column_items))) {
      LOG_WARN("add all index rowkey column to stmt failed", K(ret));
    }
  }
  return ret;
}

int ObMergeResolver::resolve_multi_table_merge_info()
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(merge_stmt), K_(schema_checker));
  } else {
    uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
    int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
    IndexDMLInfo index_dml_info;
    OZ(schema_checker_->get_can_write_index_array(merge_stmt->get_insert_base_tid(), index_tid, index_cnt, true));
    if (OB_SUCC(ret)) {
      merge_stmt->set_has_global_index(index_cnt > 0);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; i++) {
      const ObTableSchema* index_schema = NULL;
      index_dml_info.reset();
      if (OB_FAIL(schema_checker_->get_table_schema(index_tid[i], index_schema))) {
        LOG_WARN("get index table schema failed", K(ret), K(index_tid));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null", K(index_tid));
      } else if (OB_FAIL(resolve_index_all_column_exprs(
                     merge_stmt->get_insert_table_id(), *index_schema, index_dml_info.column_exprs_))) {
        LOG_WARN("resolve index column exprs failed", K(ret));
      } else {
        index_dml_info.table_id_ = merge_stmt->get_insert_table_id();
        index_dml_info.loc_table_id_ =
            (merge_stmt->get_table_items().count() > 0 && merge_stmt->get_table_items().at(0) != NULL)
                ? merge_stmt->get_table_items().at(0)->get_base_table_item().table_id_
                : OB_INVALID_ID;
        index_dml_info.index_tid_ = index_schema->get_table_id();
        index_dml_info.rowkey_cnt_ = index_schema->get_rowkey_column_num();
        index_dml_info.part_cnt_ = index_schema->get_partition_cnt();
        if (OB_FAIL(index_schema->get_index_name(index_dml_info.index_name_))) {
          LOG_WARN("get index name from index schema failed", K(ret));
        } else if (OB_FAIL(merge_stmt->add_multi_table_dml_info(index_dml_info))) {
          LOG_WARN("add index dml info to stmt failed", K(ret), K(index_dml_info));
        }
      }

      if (OB_SUCC(ret) && !with_clause_without_record_) {
        ObSchemaObjVersion table_version;
        table_version.object_id_ = index_schema->get_table_id();
        table_version.object_type_ = DEPENDENCY_TABLE;
        table_version.version_ = index_schema->get_schema_version();
        if (OB_FAIL(merge_stmt->add_global_dependency_table(table_version))) {
          LOG_WARN("add global dependency table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMergeResolver::add_assignment(
    ObTablesAssignments& assigns, const TableItem* table_item, const ColumnItem* col_item, ObAssignment& assign)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* column_expr = NULL;
  ObMergeStmt* merge_stmt = NULL;
  // check validity of assign at first
  if (OB_ISNULL(column_expr = assign.column_expr_) || OB_ISNULL(merge_stmt = get_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column expr is null", K(ret));
  } else if (assign.is_implicit_) {
    // skip
  } else if (OB_UNLIKELY(assign.column_expr_->get_table_id() != merge_stmt->get_target_table_id())) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("invalid column expr in the update clause", K(ret));
    ObString column_name = concat_qualified_name("", column_expr->get_table_name(), column_expr->get_column_name());
    ObString scope_name = ObString::make_string(get_scope_name(T_UPDATE_SCOPE));
    LOG_USER_ERROR(
        OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
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
      const ObAssignment& other = assigns.at(0).assignments_.at(i);
      if (other.is_implicit_) {
        // skip
      } else if (other.column_expr_ == assign.column_expr_) {
        ret = OB_ERR_FIELD_SPECIFIED_TWICE;
        LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(assign.column_expr_->get_column_name()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObInsertResolver::add_assignment(assigns, table_item, col_item, assign))) {
      LOG_WARN("failed to add assignment", K(ret));
    }
  }
  return ret;
}

int ObMergeResolver::replace_col_ref_for_check_cst()
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = get_merge_stmt();
  CK(OB_NOT_NULL(merge_stmt));
  CK(OB_NOT_NULL(session_info_));
  if (OB_SUCC(ret) && session_info_->use_static_typing_engine() && 0 < merge_stmt->get_check_constraint_exprs_size()) {
    ObIArray<ObRawExpr*>& update_cst_exprs = merge_stmt->get_check_constraint_exprs();
    ObSEArray<ObRawExpr*, OB_PREALLOCATED_NUM> insert_cst_exprs;
    OZ(ObRawExprUtils::copy_exprs(*params_.expr_factory_, update_cst_exprs, insert_cst_exprs, COPY_REF_DEFAULT));
    CK(insert_cst_exprs.count() == update_cst_exprs.count());

    // replace column ref for update clause
    if (merge_stmt->has_update_clause()) {
      ObTablesAssignments& tas = merge_stmt->get_table_assignments();
      CK(1 == tas.count());
      if (OB_SUCC(ret)) {
        ObTableAssignment& ta = tas.at(0);
        CK(OB_INVALID_ID != ta.table_id_);
        for (uint64_t i = 0; OB_SUCC(ret) && i < update_cst_exprs.count(); ++i) {
          if (OB_FAIL(ObTableAssignment::expand_expr(tas.at(0).assignments_, update_cst_exprs.at(i)))) {
            LOG_WARN("expand generated column expr failed", K(ret));
          }
        }
      }
    }

    // replace column ref for insert clause
    if (merge_stmt->has_insert_clause()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < insert_cst_exprs.count(); ++i) {
        OZ(ObInsertResolver::replace_column_ref_for_check_constraint(insert_cst_exprs.at(i)));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < insert_cst_exprs.count(); ++i) {
      OZ(update_cst_exprs.push_back(insert_cst_exprs.at(i)));
    }
    CK(merge_stmt->get_check_constraint_exprs_size() == insert_cst_exprs.count() * 2);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
