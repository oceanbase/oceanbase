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
#include "sql/resolver/dml/ob_multi_table_insert_resolver.h"
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
#include "sql/parser/parse_malloc.h"
#include "ob_default_value_utils.h"
#include "observer/ob_server.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {
ObMultiTableInsertResolver::ObMultiTableInsertResolver(ObResolverParams& params) : ObInsertResolver(params)
{}

ObMultiTableInsertResolver::~ObMultiTableInsertResolver()
{}

int ObMultiTableInsertResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  /*Use insert stmt directly, in order not to affect the resolver of insert stmt, it is also parsed
    separately for multi table insert situations*/
  ObInsertStmt* insert_stmt = NULL;
  if (OB_UNLIKELY(T_MULTI_INSERT != parse_tree.type_) || OB_UNLIKELY(4 > parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(insert_stmt = create_stmt<ObInsertStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create insert stmt failed", K(insert_stmt));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.children_[0]));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session info", K(session_info_));
  } else if (session_info_->use_static_typing_engine()) {  // The new engine has not been implemented. FIXME TODO
    ret = STATIC_ENG_NOT_IMPLEMENT;
    LOG_INFO("not implemented in sql static typing engine, "
             "will retry the old engine automatically",
        K(ret));
  } else if (OB_FAIL(resolve_multi_table_insert(*parse_tree.children_[0]))) {
    LOG_WARN("resolve single table insert failed", K(ret));
    // resolve hints and inner cast
  } else if (OB_FAIL(resolve_hints(parse_tree.children_[2]))) {
    LOG_WARN("failed to resolve hints", K(ret));
  } else if (OB_FAIL(insert_stmt->formalize_stmt(session_info_))) {
    LOG_WARN("pull stmt all expr relation ids failed", K(ret));
  } else {
    insert_stmt->get_check_constraint_exprs().reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_table_items().count() - 1; ++i) {
      if (OB_FAIL(resolve_multi_table_dml_info(i))) {
        LOG_WARN("failed to resolve multi table dml info", K(ret));
      } else if (OB_FAIL(resolve_duplicate_key_checker(i))) {
        LOG_WARN("resolve duplicate key checker failed", K(ret));
      } else if (OB_FAIL(resolve_check_constraints(insert_stmt->get_table_item(i)))) {
        LOG_WARN("resolve check constraint failed", K(ret));
      } else {
        RawExprArray temp_check_cst_exprs;
        for (int64_t j = 0; OB_SUCC(ret) && j < insert_stmt->get_check_constraint_exprs().count(); ++j) {
          ObSEArray<ObRawExpr*, 4> old_column_exprs;
          ObSEArray<ObRawExpr*, 4> new_column_exprs;
          ObRawExpr* check_cst_exprs = insert_stmt->get_check_constraint_exprs().at(j);
          // Since the same stmt may involve multiple insertions of the same table, it will be
          // resolved by the resolve_check_constraints function. The table id of the column in
          // check_constraint_exprs is the table id of the origin table, which will cause subsequent
          // errors in the cg phase, so need to reset the real table id of the column in
          // check_constraint_exprs. eg:
          //  CREATE TABLE test2(id int CHECK(id BETWEEN 2 AND 5));
          //  INSERT ALL into test2 values(3) into test2 values(4) select 1 from dual;
          if (OB_FAIL(ObRawExprUtils::extract_column_exprs(check_cst_exprs, old_column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          } else if (OB_FAIL(get_new_columns_exprs(
                         insert_stmt, insert_stmt->get_table_item(i), old_column_exprs, new_column_exprs))) {
            LOG_WARN("failed to get new columns exprs", K(ret));
          } else if (new_column_exprs.count() != 0 &&
                     OB_FAIL(check_cst_exprs->replace_expr(old_column_exprs, new_column_exprs))) {
            LOG_WARN("failed to replace expr", K(ret));
          } else if (OB_FAIL(temp_check_cst_exprs.push_back(check_cst_exprs))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(append(
                    insert_stmt->get_multi_insert_table_info().at(i).check_constraint_exprs_, temp_check_cst_exprs))) {
              LOG_WARN("failed to append expr", K(ret));
            } else {
              insert_stmt->get_check_constraint_exprs().reset();
            }
          }
        }
      }
    }
    // When opening the new engine, you need to replace the column in the shadow key with conv expr:
    // if (OB_SUCC(ret) && session_info_->use_static_typing_engine()) {
    //   for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_all_table_columns().count(); ++i) {
    //     ObIArray<IndexDMLInfo> &index_infos =
    //                                   insert_stmt->get_all_table_columns().at(i).index_dml_infos_;
    //     common::ObIArray<ObRawExpr*> &conv_columns = insert_stmt->get_multi_insert_col_conv_funcs().at(i);
    //     const common::ObIArray<ObColumnRefRawExpr *> *table_columns = insert_stmt->get_table_columns(i);
    //     CK(OB_NOT_NULL(table_columns));
    //     CK(conv_columns.count() == table_columns->count());
    //     for (int64_t j = 0; j < index_infos.count() && OB_SUCC(ret); ++j) {
    //       IndexDMLInfo &index_info = index_infos.at(j);
    //       for (int64_t col_idx = 0;
    //            OB_SUCC(ret) && col_idx < index_info.column_exprs_.count();
    //            ++col_idx) {
    //         ObColumnRefRawExpr *col_expr = index_info.column_exprs_.at(col_idx);
    //         CK(OB_NOT_NULL(col_expr));
    //         if (is_shadow_column(col_expr->get_column_id())) {
    //           ObRawExpr *spk_expr = col_expr->get_dependant_expr();
    //           for (int64_t k = 0; OB_SUCC(ret) && k < table_columns->count(); ++k) {
    //             OZ(ObRawExprUtils::replace_ref_column(spk_expr,
    //                                                   table_columns->at(k),
    //                                                   conv_columns.at(k)));
    //           } // for assignment end
    //         }
    //       }
    //     }
    //   }
    // }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(view_pullup_part_exprs())) {
        LOG_WARN("pullup part exprs for view failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::get_new_columns_exprs(ObInsertStmt* insert_stmt, TableItem* table_item,
    ObIArray<ObRawExpr*>& old_column_exprs, ObIArray<ObRawExpr*>& new_column_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_stmt), K(table_item), K(ret));
  } else if (old_column_exprs.count() == 0) {
    /*do nothing*/
  } else if (OB_FAIL(insert_stmt->get_column_exprs(table_item->table_id_, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    bool is_continued = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_continued && i < old_column_exprs.count(); ++i) {
      ObColumnRefRawExpr* old_col_expr = static_cast<ObColumnRefRawExpr*>(old_column_exprs.at(i));
      if (OB_ISNULL(old_col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(old_col_expr), K(ret));
      } else {
        bool is_found = false;
        for (int64_t j = 0; OB_SUCC(ret) && is_continued && !is_found && j < column_exprs.count(); ++j) {
          ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(j));
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(col_expr), K(ret));
          } else if (col_expr->get_table_id() == old_col_expr->get_table_id()) {
            is_continued = false;
          } else if (col_expr->get_column_id() == old_col_expr->get_column_id()) {
            if (OB_FAIL(new_column_exprs.push_back(column_exprs.at(j)))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else {
              is_found = true;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_continued && OB_UNLIKELY(old_column_exprs.count() != new_column_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(old_column_exprs), K(new_column_exprs), K(ret));
      }
    }
  }
  return ret;
}
/*
 * @brief resolve_multi_table_insert used to resolve multi table insert stmt
 *  1. Firstly resolve all inserted table nodes;
 *  2. Secondly resolve the subquery;
 *  3. Then parse the insert conditions, because the condition exprs comes from the subquery,
 *     so it must be parsed after the subquery, and related checks need to be performed;
 *  4. Finally, the inserted value node is parsed. Because all values come from the subquery,
 *     it must be parsed after the subquery, and related checks are required.
 */
int ObMultiTableInsertResolver::resolve_multi_table_insert(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  const ParseNode* subquery_node = NULL;
  const ParseNode* insert_node = NULL;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  insert_stmt->set_is_multi_insert_stmt(true);
  // For all the values node nodes inserted into the table, save them temporarily, and then resolve
  // them after the subquery is parsed
  ObSEArray<InsertValueNode, 4> multi_insert_values_node;
  // For all insert conditions, save it first, and then resolve it after the subquery is resolved
  ObSEArray<InsertConditionNode, 4> multi_insert_cond_node;
  if (OB_ISNULL(session_info_) || OB_ISNULL(insert_stmt) || OB_ISNULL(insert_node = node.children_[0]) ||
      OB_ISNULL(subquery_node = node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info_), K(insert_stmt), K(subquery_node), K(insert_node));
    // 1.resolve all inserted table nodes
  } else if (insert_node->type_ == T_MULTI_CONDITION_INSERT) {
    if (OB_FAIL(
            resolve_multi_conditions_insert_clause(*insert_node, multi_insert_values_node, multi_insert_cond_node))) {
      LOG_WARN("failed to resolve multi conditions insert clause", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(resolve_multi_insert_clause(*insert_node, multi_insert_values_node))) {
    LOG_WARN("failed to resolve multi insert clause", K(ret));
  }
  if (OB_FAIL(ret)) {
    // 2.resolve the subquery
  } else if (OB_FAIL(resolve_multi_insert_subquey(*subquery_node))) {
    LOG_WARN("failed to resolve multi insert subbquery");
  } else {
    // 3.resolve insert conditions
    // For columns, it is only necessary to check whether there are corresponding columns in the
    // generated table corresponding to the subquery, so set the relevant flags in advance
    params_.is_multi_table_insert_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_cond_node.count(); ++i) {
      RawExprArray insert_condition_exprs;
      if (OB_UNLIKELY(multi_insert_cond_node.at(i).table_cnt_ <= 0) ||
          OB_ISNULL(multi_insert_cond_node.at(i).insert_cond_node_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error",
            K(multi_insert_cond_node.at(i).table_cnt_),
            K(multi_insert_cond_node.at(i).insert_cond_node_),
            K(ret));
      } else if (OB_FAIL(resolve_and_split_sql_expr(
                     *multi_insert_cond_node.at(i).insert_cond_node_, insert_condition_exprs))) {
        LOG_WARN("failed to resolve and split sql expr", K(ret));
      } else if (OB_FAIL(check_exprs_sequence(insert_condition_exprs))) {
        LOG_WARN("failed to check exprs sequence", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < insert_stmt->get_multi_insert_table_info().count(); ++j) {
          if (insert_stmt->get_multi_insert_table_info().at(j).when_conds_idx_ == i) {
            if (OB_FAIL(append(
                    insert_stmt->get_multi_insert_table_info().at(j).when_conds_expr_, insert_condition_exprs))) {
              LOG_WARN("failed to append expr", K(ret));
            }
          } else { /*do nothing*/
          }
        }
      }
    }
    // 4.resolve insert values
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_values_node.count(); ++i) {
      set_is_oracle_tmp_table(get_is_oracle_tmp_table_array().at(i));
      if (OB_UNLIKELY(i != multi_insert_values_node.at(i).table_idx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(i), K(multi_insert_values_node.at(i).table_idx_));
      } else if (OB_FAIL(resolve_insert_values_node(multi_insert_values_node.at(i).insert_value_node_, i))) {
        LOG_WARN("failed to resolve insert values node", K(ret));
        // Insert oracle temporary table needs to add session_id
      } else if (OB_FAIL(add_new_column_for_oracle_temp_table(insert_stmt->get_insert_base_tid(i)))) {
        LOG_WARN("failed to resolve insert values node", K(ret));
      }
      set_is_oracle_tmp_table(false);
    }
  }

  if (OB_SUCC(ret)) {
    OZ(set_base_info_for_multi_table_insert());
  }
  return ret;
}

int ObMultiTableInsertResolver::check_exprs_sequence(const ObIArray<ObRawExpr*>& condition_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    if (OB_ISNULL(condition_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(condition_exprs.at(i)));
    } else if (condition_exprs.at(i)->has_flag(CNT_SEQ_EXPR)) {
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
      LOG_WARN("sequence number not allowed here", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_multi_insert_subquey(const ParseNode& subquery_node)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  void* select_buffer = NULL;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stat", K(insert_stmt), K_(allocator), K(ret));
  } else if (OB_ISNULL(select_buffer = allocator_->alloc(sizeof(ObSelectResolver)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate select buffer failed", K(ret), "size", sizeof(ObSelectResolver));
  } else {
    ObSelectStmt* select_stmt = NULL;
    ObSelectResolver* sub_select_resolver = get_sub_select_resolver();
    params_.is_multi_table_insert_ = false;
    params_.have_same_table_name_ = false;
    sub_select_resolver = new (select_buffer) ObSelectResolver(params_);
    sub_select_resolver->set_current_level(current_level_);
    sub_select_resolver->set_parent_namespace_resolver(NULL);
    TableItem* sub_select_table = NULL;
    ObString view_name;
    ObSEArray<ColumnItem, 4> column_items;
    if (OB_UNLIKELY(T_SELECT != subquery_node.type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid value node", K(subquery_node.type_));
    } else if (OB_FAIL(sub_select_resolver->resolve(subquery_node))) {
      LOG_WARN("failed to resolve select stmt in INSERT stmt", K(ret));
    } else if (OB_ISNULL(select_stmt = sub_select_resolver->get_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid select stmt", K(select_stmt), K(ret));
    } else if (select_stmt->has_sequence()) {
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
      LOG_WARN("sequence number not allowed here", K(ret));
    } else if (is_oracle_tmp_table() && OB_FAIL(add_new_sel_item_for_oracle_temp_table(*select_stmt))) {
      LOG_WARN("add session id value to select item failed", K(ret));
    } else if (OB_FAIL(insert_stmt->generate_view_name(*allocator_, view_name))) {
      LOG_WARN("failed to generate view name", K(ret));
    } else if (OB_FAIL(resolve_generate_table_item(select_stmt, view_name, sub_select_table))) {
      LOG_WARN("failed to resolve generate table item", K(ret));
    } else if (OB_FAIL(resolve_all_generated_table_columns(*sub_select_table, column_items))) {
      LOG_WARN("failed to resolve all generated table columns", K(ret));
    } else if (OB_FAIL(insert_stmt->add_from_item(sub_select_table->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(column_namespace_checker_.add_reference_table(sub_select_table))) {
      LOG_WARN("failed to add from item", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_multi_insert_clause(const ParseNode& insert_list_node,
    ObIArray<InsertValueNode>& multi_insert_values_node, int64_t when_conds_idx /*default -1*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(allocator_) || OB_UNLIKELY(insert_stmt->get_table_items().count() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_stmt), K_(allocator), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_list_node.num_child_; ++i) {
      const ParseNode* insert_table_node = NULL;
      if (OB_ISNULL(insert_list_node.children_[i]) || insert_list_node.children_[i]->type_ != T_SINGLE_INSERT ||
          OB_UNLIKELY(insert_list_node.children_[i]->num_child_ != 2) ||
          OB_ISNULL(insert_table_node = insert_list_node.children_[i]->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(insert_list_node.children_[i]), K(insert_table_node), K(ret));
      } else if (OB_FAIL(resolve_insert_table_node(*insert_table_node, when_conds_idx))) {
        LOG_WARN("failed to resolve insert table node", K(ret));
      } else {
        InsertValueNode insert_value_node;
        insert_value_node.table_idx_ = insert_stmt->get_table_size() - 1;
        insert_value_node.insert_value_node_ = insert_list_node.children_[i]->children_[1];
        if (OB_FAIL(multi_insert_values_node.push_back(insert_value_node))) {
          LOG_WARN("failed to push back node", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_insert_table_node(const ParseNode& insert_table_node, int64_t when_conds_idx)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ParseNode* table_node = NULL;
  TableItem* table_item = NULL;
  bool is_oracle_tmp_table = false;
  ObInsertTableInfo insert_table_info;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(table_node = insert_table_node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(insert_stmt), K(table_node));
  } else if (OB_FAIL(resolve_basic_table(*table_node, table_item))) {
    LOG_WARN("failed to resolve basic table");
  } else if (table_item->is_generated_table()) {
    ret = OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE;
    LOG_WARN("a view is not appropriate here", K(ret));
  } else if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
    LOG_WARN("failed to resolve basic table");
  } else {
    // Set the parsing parameters in advance. For scenarios where the same table appears for
    // multi table insert, you need to set the parameters in advance when checking
    params_.is_multi_table_insert_ = true;
    insert_table_info.table_id_ = table_item->table_id_;
    insert_table_info.when_conds_idx_ = when_conds_idx;
    if (OB_FAIL(insert_stmt->get_multi_insert_table_info().push_back(insert_table_info))) {
      LOG_WARN("failed to push back insert table info");
    } else if (OB_FAIL(resolve_insert_table_columns(insert_table_node.children_[1]))) {
      LOG_WARN("failed to resolve insert columns", K(ret));
    } else {
      current_scope_ = T_INSERT_SCOPE;
      const ObTableSchema* table_schema = NULL;
      if (TableItem::ALIAS_TABLE == table_item->type_) {
        if (OB_FAIL(schema_checker_->get_table_schema(table_item->get_base_table_item().ref_id_, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        }
      } else {
        if (OB_FAIL(schema_checker_->get_table_schema(table_item->get_base_table_item().table_id_, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (table_schema->is_oracle_tmp_table()) {
          session_info_->set_has_temp_table_flag();
          set_is_oracle_tmp_table(true);
          is_oracle_tmp_table = true;
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("insert all not support temp table", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(get_is_oracle_tmp_table_array().push_back(is_oracle_tmp_table))) {
            LOG_WARN("failed to push back value", K(ret));
          } else if (OB_FAIL(remove_dup_dep_cols_for_heap_table(*insert_stmt))) {
            LOG_WARN("failed to remove dup dep cols for heap table", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_insert_table_columns(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  get_insert_column_ids().clear();
  if (!insert_stmt->get_values_desc().empty()) {
    insert_stmt->get_values_desc().reset();
  }
  int64_t table_offset = insert_stmt->get_table_items().count() - 1;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_) ||
      OB_ISNULL(table_item = insert_stmt->get_table_items().at(table_offset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert stmt", K(insert_stmt), K(table_offset), K(table_item), K(ret));
  } else if (NULL != node && T_COLUMN_LIST == node->type_) {
    ParseNode* column_node = NULL;
    if (OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node children", K(node->children_));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      column_node = node->children_[i];
      ObQualifiedName column_ref;
      ObRawExpr* ref_expr = NULL;
      ObColumnRefRawExpr* column_expr = NULL;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      bool is_duplicate = false;
      if (OB_ISNULL(column_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid node children", K(column_node));
      } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(column_node, case_mode, column_ref))) {
        LOG_WARN("failed to resolve column def", K(ret));
      } else if (OB_FAIL(resolve_basic_column_ref(column_ref, ref_expr))) {
        LOG_WARN("resolve basic column reference failed", K(ret));
        report_user_error_msg(ret, ref_expr, column_ref);
      } else if (OB_ISNULL(ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_expr is null");
      } else if (!ref_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref expr is invalid", K(ret), KPC(ref_expr));
      } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(ref_expr))) {
        // do nothing
      } else if (OB_FAIL(check_insert_column_duplicate(column_expr->get_column_id(), is_duplicate))) {
        LOG_WARN("check insert column duplicate failed", K(ret));
      } else if (is_duplicate) {
        ret = OB_ERR_FIELD_SPECIFIED_TWICE;
        LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(column_expr->get_column_name()));
      } else if (OB_HIDDEN_SESSION_ID_COLUMN_ID == column_expr->get_column_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify __session_id value");
      } else if (OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID == column_expr->get_column_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify __sess_create_time value");
      } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_expr->get_column_id()) {
        ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
            column_expr->get_column_name().length(),
            column_expr->get_column_name().ptr(),
            scope_name.length(),
            scope_name.ptr());
      } else if (OB_FAIL(mock_values_column_ref(column_expr))) {
        LOG_WARN("mock values column reference failed", K(ret));
      }
    }  // end for
  } else {
    ObArray<ColumnItem> column_items;
    if (table_item->is_basic_table()) {
      if (OB_FAIL(resolve_all_basic_table_columns(*table_item, false, &column_items))) {
        LOG_WARN("resolve all basic table columns failed", K(ret));
      }
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("Only base table can be inserted in multi table", K(table_item->type_), K(ret));
    }
    if (OB_SUCC(ret)) {
      int64_t N = column_items.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        uint64_t column_id = column_items.at(i).column_id_;
        bool is_duplicate = false;
        if (OB_FAIL(check_insert_column_duplicate(column_id, is_duplicate))) {
          LOG_WARN("check insert column duplicate failed", K(ret));
        } else if (is_duplicate) {
          ret = OB_ERR_FIELD_SPECIFIED_TWICE;
          LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(column_items.at(i).column_name_));
        } else if (OB_FAIL(mock_values_column_ref(column_items.at(i).expr_))) {
          LOG_WARN("mock values column reference failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ColRawExprArray temp_values_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_values_desc().count(); ++i) {
      if (OB_ISNULL(insert_stmt->get_values_desc().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value desc is null", K(ret), K(insert_stmt->get_values_desc().at(i)));
      } else if (insert_stmt->get_values_desc().at(i)->is_generated_column()) {
        ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
        ColumnItem* orig_col_item = insert_stmt->get_column_item_by_id(
            insert_stmt->get_insert_table_id(table_offset), insert_stmt->get_values_desc().at(i)->get_column_id());
        if (NULL != orig_col_item && orig_col_item->expr_ != NULL) {
          const ObString& column_name = orig_col_item->expr_->get_column_name();
          const ObString& table_name = orig_col_item->expr_->get_table_name();
          LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
              column_name.length(),
              column_name.ptr(),
              table_name.length(),
              table_name.ptr());
        }
      } else if (OB_FAIL(temp_values_desc.push_back(insert_stmt->get_values_desc().at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(insert_stmt->get_multi_values_desc().push_back(temp_values_desc))) {
      LOG_WARN("failed to push back values desc", K(ret));
    } else {
      insert_stmt->get_values_desc().reset();
    }
  }

  return ret;
}

int ObMultiTableInsertResolver::mock_values_column_ref(const ObColumnRefRawExpr* column_ref)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* stmt = get_insert_stmt();
  ObColumnRefRawExpr* value_desc = NULL;
  if (OB_ISNULL(column_ref) || OB_ISNULL(stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_ref), K(stmt), KP_(params_.expr_factory));
  } else {
    bool found_column = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_values_desc().count(); ++i) {
      if (OB_ISNULL(value_desc = stmt->get_values_desc().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value desc is null");
      } else if (column_ref->get_column_id() == value_desc->get_column_id()) {
        found_column = true;
        break;
      }
    }
    if (found_column) {
      // ignore generating new column
    } else if (stmt->get_stmt_type() == stmt::T_MERGE) {
      if (OB_FAIL(stmt->get_values_desc().push_back(const_cast<ObColumnRefRawExpr*>(column_ref)))) {
        LOG_WARN("failed to push back values desc", K(ret), K(*value_desc));
      }
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_REF_COLUMN, value_desc))) {
      LOG_WARN("create column ref raw expr failed", K(ret));
    } else if (OB_ISNULL(value_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(("value desc is null"));
    } else {
      value_desc->set_result_type(column_ref->get_result_type());
      value_desc->set_result_flag(column_ref->get_result_flag());
      value_desc->set_column_flags(column_ref->get_column_flags());
      value_desc->set_dependant_expr(const_cast<ObRawExpr*>(column_ref->get_dependant_expr()));
      value_desc->set_ref_id(stmt->get_insert_table_id(), column_ref->get_column_id());
      value_desc->set_expr_level(current_level_);
      value_desc->set_column_attr(ObString::make_string(OB_VALUES), column_ref->get_column_name());
      if (ob_is_enumset_tc(column_ref->get_result_type().get_type()) &&
          OB_FAIL(value_desc->set_enum_set_values(column_ref->get_enum_set_values()))) {
        LOG_WARN("failed to set_enum_set_values", K(*column_ref), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(value_desc->add_flag(IS_COLUMN))) {
          LOG_WARN("failed to add flag IS_COLUMN", K(ret));
        } else if (OB_FAIL(stmt->get_values_desc().push_back(value_desc))) {
          LOG_WARN("failed to push back values desc", K(ret), K(*value_desc));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_insert_values_node(const ParseNode* node, int64_t table_offset)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  RawExprArray value_row;
  TableItem* table_item = NULL;
  if (OB_ISNULL(insert_stmt) ||
      OB_UNLIKELY(table_offset == common::OB_INVALID_ID || table_offset < 0 ||
                  table_offset >= insert_stmt->get_table_items().count()) ||
      OB_ISNULL(table_item = insert_stmt->get_table_item(table_offset)) ||
      OB_UNLIKELY(table_offset < 0 || table_offset >= insert_stmt->get_multi_values_desc().count() ||
                  table_offset >= get_is_oracle_tmp_table_array().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(insert_stmt),
        K(table_offset),
        K(table_item),
        K(insert_stmt->get_table_items().count()),
        K(table_offset),
        K(insert_stmt->get_multi_values_desc().count()),
        K(get_is_oracle_tmp_table_array().count()),
        K(ret));
  } else if (node != NULL) {
    if (T_VALUE_VECTOR != node->type_ || OB_ISNULL(node->children_) || OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arguemnt", K(node->children_), K(node->type_), K(session_info_), K(ret));
    } else if (node->num_child_ != insert_stmt->get_multi_values_desc().at(table_offset).count()) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, 2l);
      LOG_WARN("Column count doesn't match value count",
          "num_child",
          node->num_child_,
          "vector_count",
          insert_stmt->get_multi_values_desc().at(table_offset).count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
        ObRawExpr* expr = NULL;
        ObRawExpr* tmp_expr = NULL;
        const ObColumnRefRawExpr* column_expr = NULL;
        if (OB_ISNULL(node->children_[i]) ||
            OB_UNLIKELY(i >= insert_stmt->get_multi_values_desc().at(table_offset).count()) ||
            OB_ISNULL(column_expr = insert_stmt->get_multi_values_desc().at(table_offset).at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("inalid children node",
              K(i),
              K(column_expr),
              K(ret),
              K(insert_stmt->get_multi_values_desc().at(table_offset).count()));
        } else {
          uint64_t column_id = column_expr->get_column_id();
          ObDefaultValueUtils utils(insert_stmt, &params_, this);
          bool is_generated_column = false;
          if (OB_FAIL(resolve_sql_expr(*(node->children_[i]), expr))) {
            LOG_WARN("resolve sql expr failed", K(ret));
          } else if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fail to resolve sql expr", K(ret), K(expr));
          } else if (T_DEFAULT == expr->get_expr_type()) {
            ColumnItem* column_item = NULL;
            if (column_expr->is_generated_column()) {
              if (OB_FAIL(ObRawExprUtils::copy_expr(
                      *params_.expr_factory_, column_expr->get_dependant_expr(), expr, COPY_REF_DEFAULT))) {
                LOG_WARN("copy expr failed", K(ret));
              }
            } else if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(table_item->table_id_, column_id))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get column item by id failed", K(table_item->table_id_), K(column_id), K(ret));
            } else if (OB_FAIL(utils.resolve_default_expr(*column_item, expr, T_INSERT_SCOPE))) {
              LOG_WARN("fail to resolve default value", K(table_item->table_id_), K(column_id), K(ret));
            }
          } else if (OB_FAIL(check_basic_column_generated(column_expr, insert_stmt, is_generated_column))) {
            LOG_WARN("check column generated failed", K(ret));
          } else if (is_generated_column) {
            ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
            ColumnItem* orig_col_item = NULL;
            if (NULL != (orig_col_item = insert_stmt->get_column_item_by_id(table_item->table_id_, column_id)) &&
                orig_col_item->expr_ != NULL) {
              const ObString& column_name = orig_col_item->expr_->get_column_name();
              const ObString& table_name = orig_col_item->expr_->get_table_name();
              LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                  column_name.length(),
                  column_name.ptr(),
                  table_name.length(),
                  table_name.ptr());
            }
          }
          tmp_expr = expr;
          if (OB_SUCC(ret)) {
            if (OB_FAIL(tmp_expr->formalize(session_info_))) {
              LOG_WARN("formalize value expr failed", K(ret));
            } else if (OB_FAIL(value_row.push_back(tmp_expr))) {
              LOG_WARN("Can not add expr_id to ObArray", K(ret));
            }
          }
        }  // end else
      }    // end for
      if (OB_SUCC(ret)) {
        set_is_oracle_tmp_table(get_is_oracle_tmp_table_array().at(table_offset));
        if (OB_FAIL(add_new_value_for_oracle_temp_table(value_row))) {
          LOG_WARN("failed to add __session_id value", K(ret));
        } else {
          set_is_oracle_tmp_table(false);
        }
      }
    }
  } else {
    ObSelectStmt* ref_stmt = NULL;
    if (OB_UNLIKELY(insert_stmt->get_table_items().count() <= 0) ||
        OB_ISNULL(table_item = insert_stmt->get_table_item(insert_stmt->get_table_size() - 1)) ||
        OB_UNLIKELY(!table_item->is_generated_table()) || OB_ISNULL(ref_stmt = table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(insert_stmt->get_table_items().count()), K(table_item), K(ref_stmt));
    } else if (ref_stmt->get_select_item_size() != insert_stmt->get_multi_values_desc().at(table_offset).count()) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, 1l);
      LOG_WARN("Column count doesn't match value count",
          "num_child",
          ref_stmt->get_select_item_size(),
          "vector_count",
          insert_stmt->get_multi_values_desc().at(table_offset).count());
    } else {
      ObArray<ColumnItem> column_items;
      if (OB_FAIL(resolve_all_generated_table_columns(*table_item, column_items))) {
        LOG_WARN("resolve all basic table columns failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        int64_t N = column_items.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
          if (OB_ISNULL(column_items.at(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(column_items.at(i).expr_));
          } else if (OB_FAIL(value_row.push_back(column_items.at(i).expr_))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(insert_stmt->get_multi_value_vectors().push_back(value_row))) {
      LOG_WARN("failed to push back vector desc", K(ret));
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_multi_conditions_insert_clause(const ParseNode& node,
    ObIArray<InsertValueNode>& multi_insert_values_node, ObIArray<InsertConditionNode>& multi_insert_cond_node)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ParseNode* first_or_all_node = NULL;
  const ParseNode* condition_insert_node = NULL;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(allocator_) ||
      OB_UNLIKELY(node.type_ != T_MULTI_CONDITION_INSERT || node.num_child_ != 3) ||
      OB_ISNULL(first_or_all_node = node.children_[0]) || OB_ISNULL(condition_insert_node = node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stat",
        K(insert_stmt),
        K_(allocator),
        K(node.type_),
        K(node.num_child_),
        K(first_or_all_node),
        K(condition_insert_node),
        K(ret));
  } else {
    insert_stmt->set_is_multi_conditions_insert(true);
    // 1.FIRST/ALL
    if (first_or_all_node->type_ == T_FIRST) {
      insert_stmt->set_is_multi_insert_first(true);
    }
    // 2.WHEN ... THEN ...
    for (int64_t i = 0; OB_SUCC(ret) && i < condition_insert_node->num_child_; ++i) {
      const ParseNode* when_node = NULL;
      const ParseNode* insert_list_node = NULL;
      const ParseNode* condition_node = NULL;
      ObSEArray<ObInsertStmt*, 16> multi_insert_stmt;
      if (OB_ISNULL(condition_node = condition_insert_node->children_[i]) ||
          OB_UNLIKELY(condition_node->type_ != T_CONDITION_INSERT || condition_node->num_child_ != 2) ||
          OB_ISNULL(when_node = condition_node->children_[0]) ||
          OB_ISNULL(insert_list_node = condition_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null",
            K(condition_node),
            K(condition_node->type_),
            K(condition_node->num_child_),
            K(when_node),
            K(insert_list_node),
            K(ret));
      } else {
        InsertConditionNode insert_cond_node;
        insert_cond_node.insert_cond_node_ = when_node;
        int64_t old_table_size = insert_stmt->get_table_size();
        if (OB_FAIL(resolve_multi_insert_clause(*insert_list_node, multi_insert_values_node, i))) {
          LOG_WARN("failed to resolve multi insert clause", K(ret));
        } else {
          insert_cond_node.table_cnt_ = insert_stmt->get_table_size() - old_table_size;
          if (OB_FAIL(multi_insert_cond_node.push_back(insert_cond_node))) {
            LOG_WARN("failed to push back insert cond node", K(ret));
          }
        }
      }
    }
    // 3.ELSE ...
    if (OB_SUCC(ret)) {
      if (node.children_[2] != NULL) {
        if (OB_FAIL(resolve_multi_insert_clause(*node.children_[2], multi_insert_values_node))) {
          LOG_WARN("failed to resolve multi insert clause", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

/* @brief,set_base_info_for_multi_table_insert used to set base info:
 * 1.set_table_columns;
 * 2.set auto params;
 * 3.add column conv functions
 * 4.replace gen col dependent col
 * 5.set primary key ids
 */
int ObMultiTableInsertResolver::set_base_info_for_multi_table_insert()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_) ||
      OB_UNLIKELY(insert_stmt->get_table_items().count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_stmt), K(schema_checker_), K(session_info_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_table_items().count() - 1; ++i) {
      ObSEArray<uint64_t, 16> primary_key_ids;
      if (OB_FAIL(set_table_columns(i))) {
        LOG_WARN("failed to set table columns", K(ret));
      } else if (OB_FAIL(save_autoinc_params(i))) {
        LOG_WARN("failed to save autoinc params", K(ret));
      } else if (OB_FAIL(add_column_conv_function(i))) {
        LOG_WARN("failed to add column conv function", K(ret));
      } else {
        RawExprArray tmp_col_conv_funcs;
        for (int64_t j = 0; OB_SUCC(ret) && j < insert_stmt->get_column_conv_functions().count(); ++j) {
          if (OB_FAIL(tmp_col_conv_funcs.push_back(insert_stmt->get_column_conv_functions().at(j)))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /*do nothing */
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(insert_stmt->get_multi_insert_col_conv_funcs().push_back(tmp_col_conv_funcs))) {
            LOG_WARN("failed to push back column conv functions", K(ret));
            // TODO
            // } else if (session_info_->use_static_typing_engine() &&
            //           OB_FAIL(replace_gen_col_dependent_col(i))) {
            //   LOG_WARN("failed to replace gen col dependent col", K(ret));
          } else if (OB_FAIL(add_rowkey_ids(insert_stmt->get_insert_base_tid(i), primary_key_ids))) {
            LOG_WARN("failed to add rowkey ids", K(ret));
          } else if (OB_FAIL(insert_stmt->get_multi_insert_primary_key_ids().push_back(primary_key_ids))) {
            LOG_WARN("failed to push back ids", K(ret));
          } else {
            insert_stmt->get_column_conv_functions().reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::find_value_desc(
    uint64_t column_id, ObRawExpr*& output_expr, uint64_t index /*default 0 */)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  int64_t idx = 0;
  if (OB_ISNULL(insert_stmt) || OB_UNLIKELY(index >= insert_stmt->get_multi_values_desc().count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(insert_stmt), K(index), K(insert_stmt->get_multi_values_desc().count()));
  } else {
    ObIArray<ObColumnRefRawExpr*>& value_desc = insert_stmt->get_multi_values_desc().at(index);
    ObIArray<ObRawExpr*>& vector_desc = insert_stmt->get_multi_value_vectors().at(index);
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
    } else if (OB_UNLIKELY(idx > vector_desc.count() - 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(idx), K(vector_desc.count()), K(ret));
    } else {
      output_expr = vector_desc.at(idx);
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::replace_gen_col_dependent_col(uint64_t index)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(session_info_) ||
      OB_UNLIKELY(index >= insert_stmt->get_multi_insert_col_conv_funcs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(insert_stmt),
        K(session_info_),
        K(index),
        K(insert_stmt->get_multi_insert_col_conv_funcs().count()),
        K(ret));
  } else {
    const ObIArray<ObColumnRefRawExpr*>& all_cols = *insert_stmt->get_table_columns(index);
    ObIArray<ObRawExpr*>& conv_funcs = insert_stmt->get_multi_insert_col_conv_funcs().at(index);
    for (int64_t i = 0; OB_SUCC(ret) && i < all_cols.count(); i++) {
      if (NULL != all_cols.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(all_cols.at(i)), K(ret));
      } else if (all_cols.at(i)->is_generated_column()) {
        if (i >= conv_funcs.count() || OB_ISNULL(conv_funcs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid null column conv function", K(ret), K(i), K(all_cols.count()));
        } else if (OB_FAIL(replace_col_with_new_value(conv_funcs.at(i), index))) {
          LOG_WARN("failed replace col with new value", K(ret), K(i), K(all_cols.count()));
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::replace_col_with_new_value(ObRawExpr*& expr, uint64_t index)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_UNLIKELY(index >= insert_stmt->get_multi_insert_col_conv_funcs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(insert_stmt),
        K(session_info_),
        K(index),
        K(insert_stmt->get_multi_insert_col_conv_funcs().count()),
        K(ret));
  } else {
    const ObIArray<ObColumnRefRawExpr*>& all_cols = *insert_stmt->get_table_columns(index);
    ObIArray<ObRawExpr*>& conv_funcs = insert_stmt->get_multi_insert_col_conv_funcs().at(index);
    for (int i = 0; OB_SUCC(ret) && i < all_cols.count(); i++) {
      if (i >= conv_funcs.count() || OB_ISNULL(conv_funcs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null column conv function", K(ret), K(i), K(all_cols.count()));
      } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, all_cols.at(i), conv_funcs.at(i)))) {
        LOG_WARN("failed to replace ref column", K(ret));
      }
    }  // for end

    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr->formalize(session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
