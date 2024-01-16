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
#include "sql/parser/parse_malloc.h"
#include "ob_default_value_utils.h"
#include "observer/ob_server.h"
#include "pl/ob_pl_resolver.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
ObMultiTableInsertResolver::ObMultiTableInsertResolver(ObResolverParams &params)
: ObDelUpdResolver(params),
  is_oracle_tmp_table_array_(),
  the_missing_label_se_columns_array_(),
  the_missing_label_se_columns_()
{
}

ObMultiTableInsertResolver::~ObMultiTableInsertResolver()
{
}

int ObMultiTableInsertResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  /*直接使用insert stmt，为了不影响insert stmt的解析，针对multi table insert情形同时也单独解析*/
  ObInsertAllStmt *insert_all_stmt = NULL;
  if (OB_UNLIKELY(T_MULTI_INSERT != parse_tree.type_)
      || OB_UNLIKELY(4 > parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(insert_all_stmt = create_stmt<ObInsertAllStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create insert stmt failed", K(insert_all_stmt));
  } else if (OB_FAIL(resolve_outline_data_hints())) {
    LOG_WARN("resolve outline data hints failed", K(ret));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.children_[0]));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session info", K(session_info_));
  } else if (OB_FAIL(resolve_multi_table_insert(*parse_tree.children_[0]))) {
    LOG_WARN("resolve single table insert failed", K(ret));
  } else if (OB_FAIL(set_base_info_for_multi_table_insert())) {
    LOG_WARN("failed to set base info for multi table insert", K(ret));
  } else if (OB_FAIL(generate_insert_all_constraint())) {
    LOG_WARN("failed to generate insert all constraint", K(ret));
  // resolve hints and inner cast
  } else if (OB_FAIL(resolve_hints(parse_tree.children_[2]))) {
    LOG_WARN("failed to resolve hints", K(ret));
  } else if (OB_FAIL(insert_all_stmt->formalize_stmt(session_info_))) {
    LOG_WARN("pull stmt all expr relation ids failed", K(ret));
  } else if (OB_UNLIKELY(insert_all_stmt->get_table_items().count() - 1 !=
                         insert_all_stmt->get_insert_all_table_info().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(insert_all_stmt->get_table_items().count() - 1),
                                     K(insert_all_stmt->get_insert_all_table_info().count()));
  } else {
    LOG_DEBUG("check insert all table info", KPC(insert_all_stmt));
  }

  return ret;
}

int ObMultiTableInsertResolver::get_new_columns_exprs(const ObInsertAllTableInfo& table_info,
                                                      ObIArray<ObRawExpr*> &old_column_exprs,
                                                      ObIArray<ObRawExpr*> &new_column_exprs)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_ISNULL(insert_all_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_all_stmt), K(ret));
  } else if (old_column_exprs.count() == 0) {
    /*do nothing*/
  } else if (OB_FAIL(insert_all_stmt->get_column_exprs(table_info.table_id_, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    bool is_continued = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_continued && i < old_column_exprs.count(); ++i) {
      ObColumnRefRawExpr *old_col_expr = static_cast<ObColumnRefRawExpr *>(old_column_exprs.at(i));
      if (OB_ISNULL(old_col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(old_col_expr), K(ret));
      } else {
        bool is_found = false;
        for (int64_t j = 0; OB_SUCC(ret) && is_continued && !is_found && j < column_exprs.count(); ++j) {
          ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(j));
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
 * @brief resolve_multi_table_insert用于解析multi table insert stmt
 *  1. 首先解析所有的插入表节点;
 *  2. 其次解析子查询;
 *  3. 然后解析insert conditions，因为condition exprs来自于子查询，所以必须在子查询之后解析，需要进行相关的检查;
 *  4. 最后解析插入的value值节点，因为所有value值来自于子查询，所以必须在子查询之后解析，需要进行相关的检查.
 */
int ObMultiTableInsertResolver::resolve_multi_table_insert(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  const ParseNode *subquery_node = NULL;
  const ParseNode *insert_node = NULL;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  //对于所有插入表的values node节点先暂时保存，解析完子查询之后再解析
  ObSEArray<InsertValueNode, 4> multi_insert_values_node;
  //对于所有insert conditions条件先保存，等解析完子查询之后再解析
  ObSEArray<InsertConditionNode, 4> multi_insert_cond_node;
  if (OB_ISNULL(session_info_) || OB_ISNULL(insert_all_stmt) ||
      OB_ISNULL(insert_node = node.children_[0]) ||
      OB_ISNULL(subquery_node = node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info_), K(insert_all_stmt),
                                    K(subquery_node), K(insert_node));
  //1.解析插入表节点
  } else if (insert_node->type_ == T_MULTI_CONDITION_INSERT) {
    if (OB_FAIL(resolve_multi_conditions_insert_clause(*insert_node,
                                                       multi_insert_values_node,
                                                       multi_insert_cond_node))) {
      LOG_WARN("failed to resolve multi conditions insert clause", K(ret));
    } else {/*do nothing*/ }
  } else if (OB_FAIL(resolve_multi_insert_clause(*insert_node, multi_insert_values_node))) {
    LOG_WARN("failed to resolve multi insert clause", K(ret));
  }
  if (OB_FAIL(ret)) {
  //2.解析子查询
  } else if (OB_FAIL(resolve_multi_insert_subquey(*subquery_node))) {
    LOG_WARN("failed to resolve multi insert subbquery");
  } else {
    //3.解析insert conditions
    //对于列只需要检测在subquery对应的generated table中是否存在对应的列,因此提前设置好相关标记
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_cond_node.count(); ++i) {
      ObSEArray<ObRawExpr*, 8> insert_condition_exprs;
      if (OB_UNLIKELY(multi_insert_cond_node.at(i).table_cnt_ <= 0) ||
          OB_ISNULL(multi_insert_cond_node.at(i).insert_cond_node_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(multi_insert_cond_node.at(i).table_cnt_),
                                         K(multi_insert_cond_node.at(i).insert_cond_node_), K(ret));
      } else if (OB_FAIL(resolve_and_split_sql_expr(*multi_insert_cond_node.at(i).insert_cond_node_,
                                                    insert_condition_exprs))) {
        LOG_WARN("failed to resolve and split sql expr", K(ret));
      } else if (OB_FAIL(check_exprs_sequence(insert_condition_exprs))) {
        LOG_WARN("failed to check exprs sequence", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < insert_all_stmt->get_insert_all_table_info().count(); ++j) {
          ObInsertAllTableInfo *table_info = insert_all_stmt->get_insert_all_table_info().at(j);
          if (OB_ISNULL(table_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (table_info->when_cond_idx_ != i) {
            // do nothing
          } else if (OB_FAIL(table_info->when_cond_exprs_.assign(insert_condition_exprs))) {
            LOG_WARN("failed to append expr", K(ret));

          }
        }
      }
    }
    //4.解析insert values
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_values_node.count(); ++i) {
      ObInsertAllTableInfo *table_info;
      if (OB_UNLIKELY(i >= insert_all_stmt->get_insert_all_table_info().count()
                      || i >= get_is_oracle_tmp_table_array().count()
                      || i >= get_oracle_tmp_table_type_array().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array size does not match", K(ret));
      } else {
        set_is_oracle_tmp_table(get_is_oracle_tmp_table_array().at(i));
        set_oracle_tmp_table_type(get_oracle_tmp_table_type_array().at(i));
        table_info = insert_all_stmt->get_insert_all_table_info().at(i);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(i != multi_insert_values_node.at(i).table_idx_) || OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(i), K(multi_insert_values_node.at(i).table_idx_));
      } else if (OB_FAIL(resolve_insert_values_node(
                                           multi_insert_values_node.at(i).insert_value_node_, i))) {
        LOG_WARN("failed to resolve insert values node", K(ret));
      //插入oracle临时表需要添加session_id
      } else if (OB_FAIL(add_column_for_oracle_temp_table(*table_info, insert_all_stmt))) {
        LOG_WARN("failed to resolve insert values node", K(ret));
      //处理oracle label security
      } else if (get_the_missing_label_se_columns_array().at(i).count() <= 0) {
        // do nothing
      } else if (OB_FAIL(add_new_column_for_oracle_label_security_table(get_the_missing_label_se_columns_array().at(i),
                                                                        table_info->ref_table_id_))) {
        LOG_WARN("failed to add new column for oracle label securitytable", K(ret));
      }
      set_is_oracle_tmp_table(false);
      set_oracle_tmp_table_type(0);
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::check_exprs_sequence(const ObIArray<ObRawExpr*> &condition_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    if (OB_ISNULL(condition_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(condition_exprs.at(i)));
    } else if (condition_exprs.at(i)->has_flag(CNT_SEQ_EXPR)) {//多表插入场景中的条件不能出现sequence
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
      LOG_WARN("sequence number not allowed here", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_multi_insert_subquey(const ParseNode &subquery_node)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  void *select_buffer = NULL;
  if (OB_ISNULL(insert_all_stmt) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stat", K(insert_all_stmt), K_(allocator), K(ret));
  } else if (OB_ISNULL(select_buffer = allocator_->alloc(sizeof(ObSelectResolver)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate select buffer failed", K(ret), "size", sizeof(ObSelectResolver));
  } else {
    ObSelectStmt *select_stmt = NULL;
    ObSelectResolver *sub_select_resolver = nullptr;
    //重置状态，避免状态误判
    params_.have_same_table_name_ = false;
    sub_select_resolver = new(select_buffer) ObSelectResolver(params_);
    sub_select_resolver->set_current_level(current_level_);
    sub_select_resolver->set_parent_namespace_resolver(NULL);
    TableItem *sub_select_table = NULL;
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
    } else if (select_stmt->has_sequence()) {//多表插入不允许子查询中使用sequence
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
      LOG_WARN("sequence number not allowed here", K(ret));
    } else if (OB_FAIL(resolve_generate_table_item(select_stmt, view_name, sub_select_table))) {
      LOG_WARN("failed to resolve generate table item", K(ret));
    } else if (OB_FAIL(resolve_all_generated_table_columns(*sub_select_table, column_items))) {
      LOG_WARN("failed to resolve all generated table columns", K(ret));
    } else if (OB_FAIL(insert_all_stmt->add_from_item(sub_select_table->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(column_namespace_checker_.add_reference_table(sub_select_table))) {
      LOG_WARN("failed to add from item", K(ret));
    } else {/*do nothing*/}
    if (sub_select_resolver != nullptr) {
      sub_select_resolver->~ObSelectResolver();
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_multi_insert_clause(const ParseNode &insert_list_node,
                                                            ObIArray<InsertValueNode> &multi_insert_values_node,
                                                            int64_t when_conds_idx /*default -1*/)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  if (OB_ISNULL(insert_all_stmt) || OB_ISNULL(allocator_) ||
      OB_UNLIKELY(insert_all_stmt->get_table_items().count() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_all_stmt), K_(allocator), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_list_node.num_child_; ++i) {
      const ParseNode *insert_table_node = NULL;
      if (OB_ISNULL(insert_list_node.children_[i]) ||
          insert_list_node.children_[i]->type_ != T_SINGLE_INSERT ||
          OB_UNLIKELY(insert_list_node.children_[i]->num_child_ != 2) ||
          OB_ISNULL(insert_table_node = insert_list_node.children_[i]->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(insert_list_node.children_[i]), K(insert_table_node),
                                        K(ret));
      } else {
        bool has_value_node = (insert_list_node.children_[i]->children_[1] != NULL);
        if (OB_FAIL(resolve_insert_table_node(*insert_table_node, has_value_node, when_conds_idx))) {
          LOG_WARN("failed to resolve insert table node", K(ret));
        } else {
          InsertValueNode insert_value_node;
          insert_value_node.table_idx_ = insert_all_stmt->get_insert_all_table_info().count() - 1;
          insert_value_node.insert_value_node_ = insert_list_node.children_[i]->children_[1];
          if (OB_FAIL(multi_insert_values_node.push_back(insert_value_node))) {
            LOG_WARN("failed to push back node", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_insert_table_node(const ParseNode &insert_table_node,
                                                          bool has_value_node,
                                                          int64_t when_conds_idx)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  const ParseNode *table_node = NULL;
  TableItem *table_item = NULL;
  bool is_oracle_tmp_table = false;
  int oracle_tmp_table_type = 0;
  ObInsertAllTableInfo* table_info = nullptr;
  if (OB_ISNULL(insert_all_stmt) || OB_ISNULL(session_info_) ||
      OB_ISNULL(table_node = insert_table_node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(insert_all_stmt), K(session_info_), K(table_node));
  } else if (OB_FAIL(resolve_basic_table(*table_node, table_item))) {
    LOG_WARN("failed to resolve basic table");
  } else if (table_item->is_link_table()) {
    ret = OB_ERR_DDL_ON_REMOTE_DATABASE; // behavior same as Oracle
    LOG_WARN("dblink dml not support T_INSERT_ALL", K(OB_ERR_DDL_ON_REMOTE_DATABASE));
  } else if (table_item->is_generated_table() || table_item->is_temp_table()) {
    ret = OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE;
    LOG_WARN("a view is not appropriate here", K(ret));
  } else if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
    LOG_WARN("failed to resolve basic table");
  } else if (OB_FAIL(check_need_fired_trigger(table_item))) {
    LOG_WARN("failed to check has need fired trigger", K(ret), K(table_item->ref_id_));
  } else {
    //提前设置好解析参数，对于多表插入会出现相同的表的场景，因此检查时需要提前设置好参数
    if (OB_FAIL(generate_insert_all_table_info(*table_item, when_conds_idx, table_info))) {
      LOG_WARN("failed to generate insert all table info", K(ret));
    } else if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(resolve_insert_table_columns(insert_table_node.children_[1],
                                                    *table_info, has_value_node))) {
      LOG_WARN("failed to resolve insert columns", K(ret));
    } else {
      current_scope_ = T_INSERT_SCOPE;
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                    table_item->get_base_table_item().ref_id_,
                                                    table_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (table_schema->is_oracle_tmp_table() && !params_.is_prepare_stage_) {
          //oracle临时表各session不会创建自己的私有对象只能在数据增加时设置标记
          session_info_->set_has_temp_table_flag();
          set_is_oracle_tmp_table(true);
          is_oracle_tmp_table = true;
          oracle_tmp_table_type = table_schema->is_oracle_sess_tmp_table() ? 0 : 1;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(get_is_oracle_tmp_table_array().push_back(is_oracle_tmp_table))) {
            LOG_WARN("failed to push back value", K(ret));
          } else if (OB_FAIL(get_oracle_tmp_table_type_array().push_back(oracle_tmp_table_type))) {
            LOG_WARN("failed to push back value", K(ret));
          } else if (OB_FAIL(remove_dup_dep_cols_for_heap_table(table_info->part_generated_col_dep_cols_,
                                                                table_info->values_desc_))) {
            LOG_WARN("failed to remove dup dep cols for heap table", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_insert_table_columns(const ParseNode *node,
                                                             ObInsertAllTableInfo& table_info,
                                                             bool has_value_node)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  //由于这里是multi table insert, 因此在对每一张表检查表是否存在重复列时, 都需要先清空之前保存的上张表的
  //insert_column_ids_,避免误判;
  get_insert_column_ids().clear();
  if (OB_ISNULL(insert_all_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_) ||
      OB_ISNULL(table_item = insert_all_stmt->get_table_item_by_id(table_info.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert stmt", K(insert_all_stmt), K(table_info), K(table_item), K(ret));
  } else if (NULL != node && T_COLUMN_LIST == node->type_) {
    ParseNode *column_node = NULL;
    if (OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node children", K(node->children_));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      column_node = node->children_[i];
      ObQualifiedName column_ref;
      ObRawExpr *ref_expr = NULL;
      ObColumnRefRawExpr *column_expr = NULL;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      bool is_duplicate = false;
      if (OB_ISNULL(column_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid node children", K(column_node));
      } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(column_node, case_mode, column_ref))) {
        LOG_WARN("failed to resolve column def", K(ret));
      } else if (ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                                   column_ref.col_name_)) {
        ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
        LOG_WARN("cannot insert rowid pseudo column", K(ret), K(column_ref));
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
        //do nothing
      } else if (OB_FAIL(check_insert_column_duplicate(column_expr->get_column_id(),
                                                       is_duplicate))) {
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
      } else if (OB_FAIL(mock_values_column_ref(column_expr, table_info))) {
        LOG_WARN("mock values column reference failed", K(ret));
      }
    }//end for
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
        } else if (OB_FAIL(mock_values_column_ref(column_items.at(i).expr_, table_info))) {
          LOG_WARN("mock values column reference failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info.values_desc_.count(); ++i) {
      ObColumnRefRawExpr* value_desc = table_info.values_desc_.at(i);
      if (OB_ISNULL(value_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value desc is null", K(ret), K(i), K(table_info.values_desc_));
      } else if (!has_value_node && value_desc->is_generated_column()) {
        ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
        ColumnItem *orig_col_item = insert_all_stmt->get_column_item_by_id(table_info.table_id_,
                                                                           value_desc->get_column_id());
        if (NULL != orig_col_item && orig_col_item->expr_ != NULL) {
          const ObString &column_name = orig_col_item->expr_->get_column_name();
          const ObString &table_name = orig_col_item->expr_->get_table_name();
          LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                        column_name.length(), column_name.ptr(),
                        table_name.length(), table_name.ptr());
        }
      } else if (!has_value_node && value_desc->is_always_identity_column()) {
        ret = OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN;
        LOG_USER_ERROR(OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN);
      }
    }
  }

  //handle label security columns
  //统计哪些安全列没有插入，后面做特殊处理
  if (OB_SUCC(ret)) {
    const ObTableSchema *table_schema = NULL;
    common::ObSEArray<uint64_t, 4> the_miss_label_se_columns;
    if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                  table_info.ref_table_id_, table_schema))) {
      LOG_WARN("not find table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
    } else if (table_schema->has_label_se_column()) {
      const ObIArray<uint64_t> &label_se_column_ids = table_schema->get_label_se_column_ids();
      for (int64_t i = 0; OB_SUCC(ret) && i < label_se_column_ids.count(); ++i) {
        bool label_se_column_already_handled = false;
        uint64_t column_id = label_se_column_ids.at(i);
        if (OB_FAIL(check_insert_column_duplicate(column_id, label_se_column_already_handled))) {
          LOG_WARN("fail to check insert column duplicate", K(ret));
        } else {
          if (!label_se_column_already_handled) {
            if (OB_FAIL(the_miss_label_se_columns.push_back(column_id))) {
              LOG_WARN("push back to array failed", K(ret));
            }
          } else {
            //do nothing
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_the_missing_label_se_columns_array().push_back(the_miss_label_se_columns))) {
        LOG_WARN("failed to push back the missing label se columns", K(ret));
      } else {/*do nothing*/ }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::mock_values_column_ref(const ObColumnRefRawExpr *column_ref,
                                                       ObInsertTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *value_desc = NULL;
  if (OB_ISNULL(column_ref) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_ref), KP_(params_.expr_factory));
  } else {
    bool found_column = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info.values_desc_.count(); ++i) {
      if (OB_ISNULL(value_desc = table_info.values_desc_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value desc is null");
      } else if (column_ref->get_column_id() == value_desc->get_column_id()) {
        found_column = true;
        break;
      }
    }
    if (found_column) {
      //ignore generating new column
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_REF_COLUMN, value_desc))) {
      LOG_WARN("create column ref raw expr failed", K(ret));
    } else if (OB_ISNULL(value_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(("value desc is null"));
    } else {
      value_desc->set_result_type(column_ref->get_result_type());
      value_desc->set_result_flag(column_ref->get_result_flag());
      value_desc->set_column_flags(column_ref->get_column_flags());
      value_desc->set_dependant_expr(const_cast<ObRawExpr *>(column_ref->get_dependant_expr()));
      value_desc->set_ref_id(table_info.table_id_, column_ref->get_column_id());
      value_desc->set_column_attr(ObString::make_string(OB_VALUES), column_ref->get_column_name());
      value_desc->set_udt_set_id(column_ref->get_udt_set_id());
      if (ob_is_enumset_tc(column_ref->get_result_type().get_type ())
          && OB_FAIL(value_desc->set_enum_set_values(column_ref->get_enum_set_values()))) {
        LOG_WARN("failed to set_enum_set_values", K(*column_ref), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(value_desc->add_flag(IS_COLUMN))) {
          LOG_WARN("failed to add flag IS_COLUMN", K(ret));
        } else if (OB_FAIL(table_info.values_desc_.push_back(value_desc))) {
          LOG_WARN("failed to push back values desc", K(ret), K(*value_desc));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_insert_values_node(const ParseNode *node,
                                                           int64_t table_offset)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  common::ObSEArray<ObRawExpr *, 16, common::ModulePageAllocator, true> value_row;
  ObInsertAllTableInfo *table_info = nullptr;
  if (OB_ISNULL(insert_all_stmt) ||
      OB_UNLIKELY(table_offset < 0) ||
      OB_UNLIKELY(table_offset >= insert_all_stmt->get_insert_all_table_info().count() ||
                  table_offset >= get_is_oracle_tmp_table_array().count() ||
                  table_offset >= get_oracle_tmp_table_type_array().count() ||
                  table_offset >= get_the_missing_label_se_columns_array().count()) ||
      OB_ISNULL(table_info = insert_all_stmt->get_insert_all_table_info().at(table_offset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_all_stmt), K(table_offset), K(table_info),
                                    K(get_is_oracle_tmp_table_array().count()),
                                    K(get_oracle_tmp_table_type_array().count()),
                                    K(get_the_missing_label_se_columns_array().count()), K(ret));
  } else if (node != NULL) {
    ObIArray<ObColumnRefRawExpr*>& values_desc = table_info->values_desc_;
    if (T_VALUE_VECTOR != node->type_ || OB_ISNULL(node->children_) || OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arguemnt", K(node->children_), K(node->type_), K(session_info_), K(ret));
    } else if (node->num_child_ != values_desc.count()) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, 2l);
      LOG_WARN("Column count doesn't match value count",
               "num_child", node->num_child_,
               "vector_count", values_desc.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ObRawExpr *expr = NULL;
      ObRawExpr *tmp_expr = NULL;
      const ObColumnRefRawExpr *column_expr = NULL;
      if (OB_ISNULL(node->children_[i]) ||
          OB_ISNULL(column_expr = values_desc.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("inalid children node", K(i), K(column_expr), K(ret));
      } else {
        uint64_t column_id = column_expr->get_column_id();
        ObDefaultValueUtils utils(insert_all_stmt, &params_, this);
        bool is_generated_column = false;
        if (OB_FAIL(resolve_sql_expr(*(node->children_[i]), expr))) {
          LOG_WARN("resolve sql expr failed", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fail to resolve sql expr", K(ret), K(expr));
        } else if (T_DEFAULT == expr->get_expr_type()) {
          ColumnItem *column_item = NULL;
          if (OB_ISNULL(column_item = insert_all_stmt->get_column_item_by_id(
                          table_info->table_id_, column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get column item by id failed",K(table_info->table_id_),
                     K(column_id), K(ret));
          } else if (column_expr->is_generated_column()) {
            //values中对应的生成列出现了default关键字，我们统一将default替换成生成列对应的表达式
            //下面的统一处理逻辑会为values中的column进行替换
            if (OB_FAIL(copy_schema_expr(*params_.expr_factory_,
                                         column_item->expr_->get_dependant_expr(),
                                        expr))) {
              LOG_WARN("copy expr failed", K(ret));
            }
          } else if (OB_FAIL(utils.resolve_default_expr(*column_item, expr, T_INSERT_SCOPE))) {
            LOG_WARN("fail to resolve default value", K(table_info->table_id_), K(column_id),
                                                      K(ret));
          }
        } else if (OB_FAIL(check_basic_column_generated(column_expr, insert_all_stmt,
                                                        is_generated_column))) {
          LOG_WARN("check column generated failed", K(ret));
        } else if (is_generated_column) {
          ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
          ColumnItem *orig_col_item = NULL;
          if (NULL != (orig_col_item = insert_all_stmt->get_column_item_by_id(table_info->table_id_,
                                                                              column_id))
              && orig_col_item->expr_ != NULL) {
            const ObString &column_name = orig_col_item->expr_->get_column_name();
            const ObString &table_name = orig_col_item->expr_->get_table_name();
            LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN, column_name.length(),
                            column_name.ptr(), table_name.length(), table_name.ptr());
          }
        } else if (column_expr->is_always_identity_column()) {
          ret = OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN;
          LOG_USER_ERROR(OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN);
        } else {
          if ((column_expr->is_table_part_key_column()
              || column_expr->is_table_part_key_org_column())
              && expr->has_flag(CNT_SEQ_EXPR)) {
            insert_all_stmt->set_has_part_key_sequence(true);
          }
        }
        if (OB_SUCC(ret) && ObSchemaUtils::is_label_se_column(column_expr->get_column_flags())) {
          ObSysFunRawExpr *label_value_check_expr = NULL;
          if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_LABEL_SE_LABEL_VALUE_CHECK,
                                                              label_value_check_expr))) {
            LOG_WARN("fail to create raw expr", K(ret));
          } else {
            ObString func_name = ObString::make_string(N_OLS_LABEL_VALUE_CHECK);
            label_value_check_expr->set_func_name(func_name);
            if (OB_FAIL(label_value_check_expr->add_param_expr(expr))) {
              LOG_WARN("fail to add parm", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            expr = label_value_check_expr;
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
      } //end else
    } //end for
  } else {
    //未指定value时,默认将subquery输出的所有列做为输出
    ObSelectStmt *ref_stmt = NULL;
    TableItem* table_item = nullptr;
    ObIArray<ObColumnRefRawExpr*>& values_desc = table_info->values_desc_;
    if (OB_UNLIKELY(insert_all_stmt->get_table_items().count() <= 0) ||
        OB_ISNULL(table_item = insert_all_stmt->get_table_item(insert_all_stmt->get_table_size() - 1)) ||
        OB_UNLIKELY(!table_item->is_generated_table() && !table_item->is_temp_table()) ||
        OB_ISNULL(ref_stmt = table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(insert_all_stmt->get_table_items().count()),
                                      K(table_item), K(ref_stmt));
    } else if (ref_stmt->get_select_item_size() != values_desc.count()) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, 1l);
      LOG_WARN("Column count doesn't match value count",
               "num_child", ref_stmt->get_select_item_size(),
               "vector_count", values_desc.count());
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
          } else {/*do nothing*/}
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_new_value_for_oracle_temp_table(value_row))) {
      LOG_WARN("failed to add __session_id value", K(ret));
    } else if (OB_FAIL(add_new_value_for_oracle_label_security_table(*table_info,
                                                                     get_the_missing_label_se_columns_array().at(table_offset),
                                                                     value_row))) {
      LOG_WARN("fail to add new value for oracle label security table", K(ret));
    } else if (OB_FAIL(table_info->values_vector_.assign(value_row))) {
      LOG_WARN("failed to assign vector desc", K(ret));
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::resolve_multi_conditions_insert_clause(
                                              const ParseNode &node,
                                              ObIArray<InsertValueNode> &multi_insert_values_node,
                                              ObIArray<InsertConditionNode> &multi_insert_cond_node)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  const ParseNode *first_or_all_node = NULL;
  const ParseNode *condition_insert_node = NULL;
  if (OB_ISNULL(insert_all_stmt) || OB_ISNULL(allocator_) ||
      OB_UNLIKELY(node.type_ != T_MULTI_CONDITION_INSERT ||
                  node.num_child_ != 3) ||
      OB_ISNULL(first_or_all_node = node.children_[0]) ||
      OB_ISNULL(condition_insert_node = node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stat", K(insert_all_stmt), K_(allocator), K(node.type_),
                             K(node.num_child_), K(first_or_all_node),
                             K(condition_insert_node), K(ret));
  } else {
    insert_all_stmt->set_is_multi_condition_insert(true);
    //1.FIRST/ALL
    insert_all_stmt->set_is_multi_insert_first(first_or_all_node->type_ == T_FIRST);

    //2.WHEN ... THEN ...
    for (int64_t i = 0; OB_SUCC(ret) && i < condition_insert_node->num_child_; ++i) {
      const ParseNode *when_node = NULL;
      const ParseNode *insert_list_node = NULL;
      const ParseNode *condition_node = NULL;
      if (OB_ISNULL(condition_node = condition_insert_node->children_[i]) ||
          OB_UNLIKELY(condition_node->type_ != T_CONDITION_INSERT ||
                      condition_node->num_child_ != 2) ||
          OB_ISNULL(when_node = condition_node->children_[0]) ||
          OB_ISNULL(insert_list_node = condition_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(condition_node), K(condition_node->type_),
                                        K(condition_node->num_child_), K(when_node),
                                        K(insert_list_node), K(ret));
      } else {
        InsertConditionNode insert_cond_node;
        insert_cond_node.insert_cond_node_ = when_node;
        int64_t old_table_size = insert_all_stmt->get_table_size();
        if (OB_FAIL(resolve_multi_insert_clause(*insert_list_node, multi_insert_values_node, i))) {
          LOG_WARN("failed to resolve multi insert clause", K(ret));
        } else {
          insert_cond_node.table_cnt_ = insert_all_stmt->get_table_size() - old_table_size;
          if (OB_FAIL(multi_insert_cond_node.push_back(insert_cond_node))) {
            LOG_WARN("failed to push back insert cond node", K(ret));
          }
        }
      }
    }
    //3.ELSE ...
    if (OB_SUCC(ret)) {
      if (node.children_[2] != NULL) {
        if (OB_FAIL(resolve_multi_insert_clause(*node.children_[2], multi_insert_values_node))) {
          LOG_WARN("failed to resolve multi insert clause", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

/* @brief,set_base_info_for_multi_table_insert设置多表插入的基本信息:
 * 1.set auto params;
 * 2.add column conv functions
 * 3.replace gen col dependent col
 */
int ObMultiTableInsertResolver::set_base_info_for_multi_table_insert()
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *stmt = get_insert_all_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_) ||
      OB_UNLIKELY(stmt->get_table_items().count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(schema_checker_), K(session_info_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_insert_all_table_info().count(); ++i) {
      ObInsertAllTableInfo *table_info = stmt->get_insert_all_table_info().at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(generate_autoinc_params(*table_info))) {
        LOG_WARN("failed to save autoinc params", K(ret));
      } else if (OB_FAIL(generate_column_conv_function(*table_info))) {
        LOG_WARN("failed to generate column conv function", K(ret));
      } else if (OB_FAIL(replace_gen_col_dependent_col(*table_info))) {
        LOG_WARN("failed to replace gen col dependent col", K(ret));
      }
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::generate_insert_all_table_info(const TableItem& table_item,
                                                               const int64_t when_cond_idx,
                                                               ObInsertAllTableInfo*& table_info)
{
  int ret = OB_SUCCESS;
  void* ptr = nullptr;
  table_info = nullptr;
  ObInsertAllStmt* stmt = get_insert_all_stmt();
  if (OB_ISNULL(allocator_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(allocator_), K(stmt));
  } else if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObInsertAllTableInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for insert all table info", K(ret));
  } else {
    table_info = new(ptr) ObInsertAllTableInfo();
    table_info->when_cond_idx_ = when_cond_idx;
    if (OB_FAIL(generate_insert_table_info(table_item, *table_info))) {
      LOG_WARN("failed to generate insert table info", K(ret));
    } else if (OB_FAIL(stmt->get_insert_all_table_info().push_back(table_info))) {
      LOG_WARN("failed to push back table info", K(ret));
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::generate_insert_all_constraint()
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  TableItem* table_item = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_all_stmt->get_insert_all_table_info().count(); ++i) {
    ObInsertAllTableInfo *table_info = insert_all_stmt->get_insert_all_table_info().at(i);
    if (OB_ISNULL(table_info) ||
        OB_ISNULL(table_item = insert_all_stmt->get_table_item_by_id(table_info->table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(table_info), K(table_item));
    } else if (OB_FAIL(resolve_check_constraints(table_item, table_info->check_constraint_exprs_))) {
      LOG_WARN("failed to resolve check constraints", K(ret));
    } else if (OB_FAIL(refine_table_check_constraint(*table_info))) {
      LOG_WARN("failed to refine table check constraint", K(ret));
    }
  }
  return ret;
}

int ObMultiTableInsertResolver::refine_table_check_constraint(ObInsertAllTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info.check_constraint_exprs_.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> old_column_exprs;
    ObSEArray<ObRawExpr*, 4> new_column_exprs;
    ObRawExpr*& check_cst_expr = table_info.check_constraint_exprs_.at(i);
    //由于同一个stmt可能涉及同一张表的多次插入，这样会使得通过resolve_check_constraints函数解析出来的
    //check_constraint_exprs中的列的table id都是origin table的table id，会使得后续在cg阶段报错，因此
    //需要重新设置一下check_constraint_exprs中的列的真实的table id,eg:
    //  CREATE TABLE test2(id int CHECK(id BETWEEN 2 AND 5));
    //  INSERT ALL into test2 values(3) into test2 values(4) select 1 from dual;
    //具体见bug:
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(check_cst_expr, old_column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(get_new_columns_exprs(table_info,
                                             old_column_exprs,
                                             new_column_exprs))) {
      LOG_WARN("failed to get new columns exprs", K(ret));
    } else if (new_column_exprs.count() != 0 &&
               OB_FAIL(check_cst_expr->replace_expr(old_column_exprs, new_column_exprs))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else {
      LOG_TRACE("succeed to get check constraint expr", K(*check_cst_expr));
    }
  }
  return ret;
}
int ObMultiTableInsertResolver::add_new_sel_item_for_oracle_label_security_table(ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  ObInsertAllStmt *insert_all_stmt = get_insert_all_stmt();
  if (OB_ISNULL(insert_all_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(insert_all_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_all_stmt->get_insert_all_table_info().count(); ++i) {
    ObInsertAllTableInfo *table_info = insert_all_stmt->get_insert_all_table_info().at(i);
    ObIArray<uint64_t>& column_array = the_missing_label_se_columns_array_.at(i);
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObDelUpdResolver::add_new_sel_item_for_oracle_label_security_table(*table_info,
                                                                                          column_array,
                                                                                          select_stmt))) {
    LOG_WARN("failed to add new sel item for oracle label security table", K(ret));
    }
  }
  return ret;  
}

int ObMultiTableInsertResolver::find_value_desc(ObInsertTableInfo &table_info,
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
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

}//sql
}//oceanbase
