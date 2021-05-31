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
#include "sql/resolver/dml/ob_insert_resolver.h"
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
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
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
ObInsertResolver::ObInsertResolver(ObResolverParams& params)
    : ObDMLResolver(params),
      insert_column_ids_(),
      row_count_(0),
      sub_select_resolver_(NULL),
      autoinc_col_added_(false),
      is_column_specify_(false),
      is_all_default_(false),
      is_oracle_tmp_table_(false),
      is_oracle_tmp_table_array_()
{
  params.contain_dml_ = true;
}

ObInsertResolver::~ObInsertResolver()
{
  if (NULL != sub_select_resolver_) {
    sub_select_resolver_->~ObSelectResolver();
  }
}

int ObInsertResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = NULL;
  if (OB_UNLIKELY(T_INSERT != parse_tree.type_) || OB_UNLIKELY(4 > parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(insert_stmt = create_stmt<ObInsertStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create insert stmt failed", K(insert_stmt));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(parse_tree.children_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session info", K(session_info_));
  } else if (FALSE_IT(session_info_->set_ignore_stmt(NULL != parse_tree.children_[3] ? true : false))) {
    // do nothing
  } else if (OB_LIKELY(T_SINGLE_TABLE_INSERT == parse_tree.children_[0]->type_)) {
    if (OB_FAIL(resolve_single_table_insert(*parse_tree.children_[0]))) {
      LOG_WARN("resolve single table insert failed", K(ret));
    }
  } else {
    //@TODO: resolve multi table insert(oracle)
  }

  if (OB_SUCC(ret)) {
    // REPLACE flag
    if (OB_ISNULL(parse_tree.children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node for is_replacement", K(parse_tree.children_[1]));
    } else if (parse_tree.children_[1]->type_ == T_REPLACE) {
      insert_stmt->set_replace(true);
      if (OB_FAIL(add_rowkey_ids(insert_stmt->get_insert_base_tid(), insert_stmt->get_primary_key_ids()))) {
        LOG_WARN("fail to add rowkey column for replace", K(ret));
      }
    } else {
      insert_stmt->set_replace(false);
    }
  }

  // resolve hints and inner cast
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_hints(parse_tree.children_[2]))) {
      LOG_WARN("failed to resolve hints", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    insert_stmt->set_ignore(session_info_->is_ignore_stmt());
    if (insert_stmt->is_replace() && insert_stmt->is_ignore()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace statement with ignore");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(insert_stmt->formalize_stmt(session_info_))) {
      LOG_WARN("pull stmt all expr relation ids failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_multi_table_dml_info())) {
      LOG_WARN("resolve index dml info failed", K(ret));
    } else if (OB_FAIL(resolve_duplicate_key_checker())) {
      LOG_WARN("resolve duplicate key checker failed", K(ret));
    }
  }

  // Replace column in shadow key as conv expr
  // The column in the shadow key is replaced with conv expr because of each index in the multi part insert
  // The access_exprs_ corresponding to the sub-plan will directly depend on
  //   the output of multi_part_insert (conv_exprs)
  // In order to calculate the shadow key, the value of conv_exprs can be used directly,
  //   so it needs to be replaced here;
  // For replace/insert_up, access_exprs_ depends on table_columns, so there is no need to replace
  if (OB_SUCC(ret) && session_info_->use_static_typing_engine() && !insert_stmt->is_replace() &&
      !insert_stmt->get_insert_up()) {
    CK(insert_stmt->get_all_table_columns().count() == 1);
    ObIArray<IndexDMLInfo>& index_infos = insert_stmt->get_all_table_columns().at(0).index_dml_infos_;
    common::ObIArray<ObRawExpr*>& conv_columns = insert_stmt->get_column_conv_functions();
    const common::ObIArray<ObColumnRefRawExpr*>* table_columns = insert_stmt->get_table_columns();
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

  // expand returning expr with conv expr in new engine
  if (OB_SUCC(ret) && session_info_->use_static_typing_engine()) {
    common::ObIArray<ObRawExpr*>& conv_columns = insert_stmt->get_column_conv_functions();
    const common::ObIArray<ObColumnRefRawExpr*>* table_columns = insert_stmt->get_table_columns();
    CK(NULL != table_columns);
    CK(conv_columns.count() == table_columns->count());
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_returning_exprs().count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < table_columns->count(); j++) {
        OZ(ObRawExprUtils::replace_ref_column(
            insert_stmt->get_returning_exprs().at(i), table_columns->at(j), conv_columns.at(j)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(view_pullup_part_exprs())) {
      LOG_WARN("pullup part exprs for view failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_view_insertable())) {
      LOG_TRACE("view not insertable", K(ret));
    }
  }

  if (OB_SUCC(ret) && insert_stmt->value_from_select() && session_info_->use_static_typing_engine() &&
      !insert_stmt->get_insert_up()) {
    if (OB_FAIL(fill_index_dml_info_column_conv_exprs())) {
      LOG_WARN("fail to fill index dml info column conv exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (insert_stmt->is_ignore() && insert_stmt->has_global_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ignore with global index");
    }
  }
  return ret;
}

int ObInsertResolver::resolve_single_table_insert(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  // resolve insert table reference
  // the target table must be resolved first,
  // insert_stmt appoint that the first table in table_items_ is the target table
  const ParseNode* insert_into = NULL;
  const ParseNode* values_node = NULL;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  CK(OB_NOT_NULL(session_info_));
  CK(OB_NOT_NULL(insert_stmt));
  CK(OB_NOT_NULL(insert_into = node.children_[0]));
  CK(OB_NOT_NULL(values_node = node.children_[1]));
  CK(OB_NOT_NULL(params_.session_info_));
  OZ(resolve_insert_field(*insert_into));
  OZ(resolve_values(*values_node));

  if (OB_SUCC(ret) && !insert_stmt->get_table_items().empty() && NULL != insert_stmt->get_table_item(0) &&
      insert_stmt->get_table_item(0)->is_generated_table()) {
    OZ(add_all_column_to_updatable_view(*insert_stmt, *insert_stmt->get_table_item(0)));
    OZ(view_pullup_generated_column_exprs());
  }

  OZ(add_new_column_for_oracle_temp_table(insert_stmt->get_insert_base_tid()));
  OZ(set_table_columns());
  OZ(check_has_unique_index());
  OZ(save_autoinc_params());

  OZ(add_column_conv_function());
  // In static engine we need to replace the dependent column of generate column with the
  // new insert value. e.g.:
  //   c3 as c1 + c2
  // after add_column_conv_function() the c3's new value is: column_conv(c1 + c2)
  // should be replaced to: column_conv(column_conv(__values.c1) + column_conv(__values.c2).
  //
  // The old engine no need to do this because it calculate generate column with the
  // new inserted row.
  OZ(replace_gen_col_dependent_col());

  OZ(add_rowkey_ids(insert_stmt->get_insert_base_tid(), insert_stmt->get_primary_key_ids()));
  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode()) {
      // resolve returning node
      ParseNode* returning_node = node.children_[2];
      OZ(resolve_returning(returning_node));
    } else {
      // resolve assignments
      ParseNode* assign_list = node.children_[2];
      if (NULL != assign_list) {
        insert_stmt->set_insert_up(true);
        OZ(resolve_insert_update_assignment(assign_list));
      }
    }
  }

  if (OB_SUCC(ret) && lib::is_oracle_mode()) {
    if (0 == insert_stmt->get_table_items().count() || OB_ISNULL(insert_stmt->get_table_item(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table items count is zero in insert stmt", K(ret));
    } else if (OB_FAIL(resolve_check_constraints(insert_stmt->get_table_item(0)))) {
      LOG_WARN("resolve check constraint failed", K(ret));
    } else if (session_info_->use_static_typing_engine()) {
      for (uint64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_check_constraint_exprs_size(); ++i) {
        if (OB_FAIL(replace_column_ref_for_check_constraint(insert_stmt->get_check_constraint_exprs().at(i)))) {
          LOG_WARN("fail to replace column ref for check constraint", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::fill_index_dml_info_column_conv_exprs()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());

  CK(OB_NOT_NULL(insert_stmt));
  if (OB_SUCC(ret)) {
    ObIArray<IndexDMLInfo>& dml_infos = insert_stmt->get_all_table_columns().at(0).index_dml_infos_;
    IndexDMLInfo& primary_dml_info = dml_infos.at(0);
    if (OB_FAIL(primary_dml_info.column_convert_exprs_.assign(insert_stmt->get_column_conv_functions()))) {
      LOG_WARN("fail to assign array", K(ret));
    }

    for (int64_t i = 1; OB_SUCC(ret) && i < dml_infos.count(); ++i) {
      IndexDMLInfo& index_dml_info = dml_infos.at(i);
      if (OB_FAIL(fill_index_column_convert_exprs(session_info_->use_static_typing_engine(),
              primary_dml_info,
              index_dml_info.column_exprs_,
              index_dml_info.column_convert_exprs_))) {
        LOG_WARN("fail to fill index column convert expr", K(ret));
      }
    }
  }
  return ret;
}

// Generate information related to multi table dml first for the optimizer to choose
// You need to use this information when you need to generate a multi table dml-related plan
// In the multi table insert scenario, you need to specify the table_offset of the inserted table in stmt
int ObInsertResolver::resolve_multi_table_dml_info(uint64_t table_offset /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(schema_checker_) ||
      OB_UNLIKELY(table_offset < 0 || table_offset >= insert_stmt->get_table_items().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null",
        K(insert_stmt),
        K_(schema_checker),
        K(table_offset),
        K(insert_stmt->get_table_items().count()),
        K(ret));
  } else {
    uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
    int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
    IndexDMLInfo index_dml_info;

    OZ(schema_checker_->get_can_write_index_array(
        insert_stmt->get_insert_base_tid(table_offset), index_tid, index_cnt, true));

    if (OB_SUCC(ret)) {
      insert_stmt->set_has_global_index(index_cnt > 0);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
      const ObTableSchema* index_schema = NULL;
      index_dml_info.reset();
      if (OB_FAIL(schema_checker_->get_table_schema(index_tid[i], index_schema))) {
        LOG_WARN("get index table schema failed", K(ret), K(index_tid));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null", K(index_tid));
      } else if (OB_FAIL(resolve_index_all_column_exprs(
                     insert_stmt->get_insert_table_id(table_offset), *index_schema, index_dml_info.column_exprs_))) {
        LOG_WARN("resolve index column exprs failed", K(ret));
      } else if (OB_ISNULL(insert_stmt->get_table_item(table_offset))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table item failed", K(ret));
      } else {
        index_dml_info.table_id_ = insert_stmt->get_insert_table_id(table_offset);
        index_dml_info.loc_table_id_ =
            (insert_stmt->get_table_items().count() > 0 && insert_stmt->get_table_items().at(table_offset) != NULL)
                ? insert_stmt->get_table_items().at(table_offset)->get_base_table_item().table_id_
                : OB_INVALID_ID;
        index_dml_info.index_tid_ = index_schema->get_table_id();
        index_dml_info.rowkey_cnt_ = index_schema->get_rowkey_column_num();
        index_dml_info.part_cnt_ = index_schema->get_partition_cnt();
        if (OB_FAIL(index_schema->get_index_name(index_dml_info.index_name_))) {
          LOG_WARN("get index name from index schema failed", K(ret));
        } else if (OB_FAIL(insert_stmt->add_multi_table_dml_info(index_dml_info))) {
          LOG_WARN("add index dml info to stmt failed", K(ret), K(index_dml_info));
        }
      }
      if (OB_SUCC(ret) && !with_clause_without_record_) {
        ObSchemaObjVersion table_version;
        table_version.object_id_ = index_schema->get_table_id();
        table_version.object_type_ = DEPENDENCY_TABLE;
        table_version.version_ = index_schema->get_schema_version();
        if (OB_FAIL(insert_stmt->add_global_dependency_table(table_version))) {
          LOG_WARN("add global dependency table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::find_value_desc(uint64_t column_id, ObRawExpr*& column_ref, uint64_t index /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  column_ref = NULL;
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(insert_stmt));
  } else if (!insert_stmt->value_from_select()) {
    ObIArray<ObColumnRefRawExpr*>& value_desc = insert_stmt->get_values_desc();
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; i < value_desc.count() && OB_ENTRY_NOT_EXIST == ret; i++) {
      ObColumnRefRawExpr* expr = value_desc.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get values expr", K(i), K(value_desc));
      } else if (column_id == expr->get_column_id()) {
        column_ref = value_desc.at(i);
        ret = OB_SUCCESS;
      }
    }
  } else {
    // select item
    ret = OB_ENTRY_NOT_EXIST;
    uint64_t source_table_id = OB_INVALID_ID;
    if (OB_UNLIKELY(insert_stmt->get_from_item_size() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid from items", K(ret), K(insert_stmt->get_from_items()));
    } else {
      source_table_id = insert_stmt->get_from_item(0).table_id_;
    }
    for (int64_t i = 0; i < insert_stmt->get_values_desc().count() && OB_ENTRY_NOT_EXIST == ret; i++) {
      if (OB_ISNULL(insert_stmt->get_values_desc().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid value desc", K(i), K(insert_stmt->get_values_desc()));
      } else if (column_id != insert_stmt->get_values_desc().at(i)->get_column_id()) {
        // do nothing
      } else if (OB_ISNULL(
                     column_ref = insert_stmt->get_column_expr_by_id(source_table_id, i + OB_APP_MIN_COLUMN_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is not found in the generated table", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  UNUSED(index);
  return ret;
}

int ObInsertResolver::add_column_conv_function(uint64_t index /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(insert_stmt), K_(params_.expr_factory));
  } else {
    CK(OB_NOT_NULL(insert_stmt->get_table_columns(index)));
    uint64_t table_id = insert_stmt->get_insert_table_id(index);
    if (OB_FAIL(insert_stmt->get_column_conv_functions().prepare_allocate(
            insert_stmt->get_table_columns(index)->count()))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else {
      for (int i = 0; i < insert_stmt->get_column_conv_functions().count(); i++) {
        insert_stmt->get_column_conv_functions().at(i) = NULL;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_table_columns(index)->count(); i++) {
      uint64_t column_id = OB_INVALID_ID;
      ColumnItem* column_item = NULL;
      ObRawExpr* column_ref = NULL;
      const ObColumnRefRawExpr* tbl_col = NULL;
      if (OB_ISNULL(tbl_col = insert_stmt->get_table_columns(index)->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table column", K(ret), K(i), K(insert_stmt->get_table_columns(index)));
      } else if (FALSE_IT(column_id = tbl_col->get_column_id())) {
        // nothing to do
      } else if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(table_id, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find column item", K(ret), K(column_id), K(column_item), K(*tbl_col));
      } else if (OB_FAIL(find_value_desc(column_id, column_ref, index))) {
        LOG_WARN("fail to check column is exists", K(ret), K(column_id));
      } else if (OB_ISNULL(column_ref)) {
        if (OB_FAIL(build_column_conv_function_with_default_expr(i, index))) {
          LOG_WARN("build column convert function with default expr failed", K(ret));
        }
      } else if (OB_FAIL(build_column_conv_function_with_value_desc(i, column_ref, index))) {
        LOG_WARN("failed to build column conv with value desc", K(ret));
      }
    }  // end for
  }
  return ret;
}

int ObInsertResolver::replace_gen_col_dependent_col()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  CK(NULL != insert_stmt);
  CK(NULL != session_info_);
  const auto& all_cols = *insert_stmt->get_table_columns();
  auto& conv_funcs = insert_stmt->get_column_conv_functions();
  for (int64_t i = 0; OB_SUCC(ret) && session_info_->use_static_typing_engine() && i < all_cols.count(); i++) {
    CK(NULL != all_cols.at(i));
    if (OB_SUCC(ret) && all_cols.at(i)->is_generated_column()) {
      if (i >= conv_funcs.count() || OB_ISNULL(conv_funcs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null column conv function", K(ret), K(i), K(all_cols.count()));
      } else {
        OZ(replace_col_with_new_value(conv_funcs.at(i)));
      }
    }
  }
  return ret;
}

int ObInsertResolver::process_values_function(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr));
  } else if (OB_UNLIKELY(!expr->has_flag(IS_VALUES)) || OB_UNLIKELY(expr->get_param_count() != 1)) {
    LOG_WARN("invalid expr", K(expr), K(expr->get_param_count()));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN)) {
    LOG_WARN("invalid param expr", "type", expr->get_param_expr(0)->get_expr_type());
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid insert stmt", K(ret), K(insert_stmt));
  } else {
    const ObIArray<ObColumnRefRawExpr*>* insert_columns = insert_stmt->get_table_columns();
    ObColumnRefRawExpr* b_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(0));
    if (OB_ISNULL(b_expr) || OB_ISNULL(insert_columns)) {
      LOG_WARN("invalid expr or insert_columns", K(b_expr), K(insert_columns));
      ret = OB_INVALID_ARGUMENT;
    } else {
      int64_t table_id = insert_stmt->get_insert_table_id();
      uint64_t column_id = b_expr->get_column_id();
      ColumnItem* column_item = NULL;
      if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(table_id, column_id))) {
        LOG_WARN("fail to get column item", K(ret), K(table_id), K(column_id));
      } else {
        const int64_t N = insert_columns->count();
        int64_t index = OB_INVALID_INDEX;
        ret = OB_ENTRY_NOT_EXIST;
        for (int64_t i = 0; i < N && OB_ENTRY_NOT_EXIST == ret; i++) {
          if (OB_ISNULL(insert_columns->at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid insert columns", K(i), K(insert_columns->at(i)));
          } else if (insert_columns->at(i)->get_column_id() == column_id) {
            index = i;
            ret = OB_SUCCESS;
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_UNLIKELY(index == OB_INVALID_INDEX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to find column position", K(column_id));
        } else {
          ObRawExpr* value_expr = get_insert_stmt()->get_column_conv_functions().at(index);
          if (OB_FAIL(add_additional_function_according_to_type(column_item, value_expr, T_INSERT_SCOPE, true))) {
            LOG_WARN("fail to add additional function", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, b_expr, value_expr))) {
            LOG_WARN("fail to replace ref column", K(ret), K(b_expr), K(value_expr));
          } else {
            SQL_RESV_LOG(DEBUG, "replace ref column", K(expr), K(b_expr), K(value_expr), K(column_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::replace_column_ref_for_check_constraint(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();

  if (OB_ISNULL(expr) || OB_ISNULL(insert_stmt) || OB_ISNULL(params_.expr_factory_)) {
    LOG_WARN("invalid argument", K(expr), K(insert_stmt));
    ret = OB_INVALID_ARGUMENT;
  } else if (expr->get_param_count() > 0) {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(replace_column_ref_for_check_constraint(expr->get_param_expr(i))))) {
        LOG_WARN("replace column ref for check constraint", K(ret), KPC(expr->get_param_expr(i)));
      }
    }
  }
  if (OB_SUCC(ret) && expr->is_column_ref_expr()) {
    const ObIArray<ObColumnRefRawExpr*>* insert_columns = insert_stmt->get_table_columns();
    ObColumnRefRawExpr* b_expr = static_cast<ObColumnRefRawExpr*>(expr);
    if (OB_ISNULL(b_expr) || OB_ISNULL(insert_columns)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expr or insert_columns", K(b_expr), K(insert_columns));
    } else {
      const int64_t N = insert_columns->count();
      int64_t index = OB_INVALID_INDEX;
      ret = OB_ENTRY_NOT_EXIST;
      for (int64_t i = 0; i < N && OB_ENTRY_NOT_EXIST == ret; ++i) {
        if (OB_ISNULL(insert_columns->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid insert columns", K(i), K(insert_columns->at(i)));
        } else if (b_expr == insert_columns->at(i)) {
          index = i;
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(index == OB_INVALID_INDEX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to find column position", K(ret));
        } else {
          expr = insert_stmt->get_column_conv_functions().at(index);
        }
      }
    }
  }

  return ret;
}

int ObInsertResolver::replace_column_ref(ObArray<ObRawExpr*>* value_row, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(expr) || OB_ISNULL(value_row) || OB_ISNULL(insert_stmt) || OB_ISNULL(params_.expr_factory_)) {
    LOG_WARN("invalid argument", K(expr), K(value_row), K(insert_stmt));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "fail to get allocator", K(ret), K_(allocator));
  } else if (expr->get_param_count() > 0) {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(replace_column_ref(value_row, expr->get_param_expr(i))))) {
        LOG_WARN("fail to postorder_spread", K(ret), K(expr->get_param_expr(i)));
      }
    }
  }
  if (OB_SUCC(ret) && expr->is_column_ref_expr()) {
    int64_t value_index = -1;
    ObIArray<ObColumnRefRawExpr*>& value_desc = insert_stmt->get_values_desc();
    if (share::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("column not allowed here", KPC(expr));
    } else if (value_desc.count() < value_row->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array size", K(value_row->count()), K(value_desc.count()));
    } else {
      ObColumnRefRawExpr* b_expr = static_cast<ObColumnRefRawExpr*>(expr);
      uint64_t column_id = b_expr->get_column_id();
      ret = OB_ENTRY_NOT_EXIST;
      for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < value_row->count(); i++) {
        if (value_desc.at(i)->get_column_id() == column_id) {
          value_index = i;
          ret = OB_SUCCESS;
        }
      }
      ColumnItem* column_item = NULL;
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(replace_column_to_default(expr))) {
          LOG_WARN("fail to replace column to default", K(ret), K(*expr));
        } else {
          SQL_RESV_LOG(DEBUG, "replace column ref to default", K(*expr));
        }
      } else if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(
                               get_insert_stmt()->get_insert_table_id(), column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find column item", K(ret), K(column_id));
      } else {
        ObRawExpr*& value_expr = value_row->at(value_index);
        if (OB_ISNULL(value_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get value expr", K(value_index), K(value_expr), K(value_row));
        } else if (OB_FAIL(add_additional_function_according_to_type(column_item, value_expr, T_INSERT_SCOPE, true))) {
          LOG_WARN("fail to build column conv expr", K(ret));
        }
        if (OB_SUCC(ret)) {
          expr = insert_stmt->get_values_desc().at(value_index);
          insert_stmt->set_is_all_const_values(false);
          SQL_RESV_LOG(DEBUG, "replace column ref to value", K(*expr), K(value_index));
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::add_select_list_for_set_stmt(ObSelectStmt& select_stmt)
{
  int ret = OB_SUCCESS;
  SelectItem new_select_item;
  ObExprResType res_type;
  ObSelectStmt* child_stmt = NULL;
  if (!select_stmt.is_set_stmt()) {
    // do nothing
  } else if (OB_ISNULL(child_stmt = select_stmt.get_set_query(0)) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret), K(child_stmt));
  } else {
    int64_t num = child_stmt->get_select_item_size();
    for (int64_t i = select_stmt.get_select_item_size(); OB_SUCC(ret) && i < num; i++) {
      SelectItem& select_item = child_stmt->get_select_item(i);
      // unused
      // ObString set_column_name = left_select_item.alias_name_;
      ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + select_stmt.get_set_op());
      res_type.reset();
      new_select_item.alias_name_ = select_item.alias_name_;
      new_select_item.expr_name_ = select_item.expr_name_;
      new_select_item.default_value_ = select_item.default_value_;
      new_select_item.default_value_expr_ = select_item.default_value_expr_;
      new_select_item.is_real_alias_ = true;
      res_type = select_item.expr_->get_result_type();
      if (OB_FAIL(ObRawExprUtils::make_set_op_expr(
              *params_.expr_factory_, i, set_op_type, res_type, session_info_, new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FAIL(select_stmt.add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) || OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      } else {
        new_select_item.expr_->set_expr_level(child_stmt->get_current_level());
      }
    }
  }
  return ret;
}

int ObInsertResolver::add_select_items(ObSelectStmt& select_stmt, const ObIArray<SelectItem>& select_items)
{
  int ret = OB_SUCCESS;
  if (!select_stmt.is_set_stmt()) {
    if (OB_ISNULL(params_.expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr factory", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_select_items(
                   *params_.expr_factory_, select_items, select_stmt.get_select_items(), COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy select items failed", K(ret));
    }
  } else {
    ObIArray<ObSelectStmt*>& child_query = select_stmt.get_set_query();
    const int64_t child_num = child_query.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      if (OB_ISNULL(child_query.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(add_select_items(*child_query.at(i), select_items)))) {
        LOG_WARN("failed to add select items", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(add_select_list_for_set_stmt(select_stmt))) {
      LOG_WARN("failed to create select list", K(ret));
    }
  }
  return ret;
}

int ObInsertResolver::add_new_sel_item_for_oracle_temp_table(ObSelectStmt& select_stmt)
{
  int ret = OB_SUCCESS;
  if (is_oracle_tmp_table_) {
    ObConstRawExpr* session_id_expr = NULL;
    ObConstRawExpr* sess_create_time_expr = NULL;
    ObSEArray<SelectItem, 4> select_items;
    SelectItem select_item;
    if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, session_id_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, sess_create_time_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(session_id_expr) || OB_ISNULL(sess_create_time_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dummy expr is null", K(session_id_expr), K(sess_create_time_expr));
    } else {
      ObObj val;
      val.set_int(session_info_->get_sessid_for_table());
      session_id_expr->set_value(val);
      select_item.is_implicit_added_ = true;
      select_item.expr_ = session_id_expr;
      if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("push subquery select items failed", K(ret));
      } else if (session_info_->is_obproxy_mode() && 0 == session_info_->get_sess_create_time()) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "can't insert into oracle temporary table via obproxy, upgrade obproxy first", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "obproxy version is too old, insert into temporary table");
      } else {
        val.set_int(session_info_->get_sess_create_time());
        sess_create_time_expr->set_value(val);
        select_item.expr_ = sess_create_time_expr;
        if (OB_FAIL(select_items.push_back(select_item))) {
          LOG_WARN("push subquery select items failed", K(ret));
        } else if (OB_FAIL(add_select_items(select_stmt, select_items))) {
          LOG_WARN("failed to add select items", K(ret));
        }
        LOG_DEBUG("add __session_id & __sess_create_time to select item succeed");
      }
    }
  }
  return ret;
}

int ObInsertResolver::add_new_value_for_oracle_temp_table(ObIArray<ObRawExpr*>& value_row)
{
  int ret = OB_SUCCESS;
  if (is_oracle_tmp_table_) {
    ObConstRawExpr* session_id_expr = NULL;
    ObConstRawExpr* sess_create_time_expr = NULL;
    ObInsertStmt* insert_stmt = get_insert_stmt();
    if (OB_ISNULL(session_info_) || OB_ISNULL(insert_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid session_info_ or insert_stmt", K(session_info_), K(insert_stmt));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, session_id_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, sess_create_time_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(session_id_expr) || OB_ISNULL(sess_create_time_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dummy expr is null", K(session_id_expr), K(sess_create_time_expr));
    } else {
      ObObj val;
      val.set_int(session_info_->get_sessid_for_table());
      session_id_expr->set_value(val);
      if (OB_FAIL(value_row.push_back(session_id_expr))) {
        LOG_WARN("push back to output expr failed", K(ret));
      } else if (session_info_->is_obproxy_mode() && 0 == session_info_->get_sess_create_time()) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "can't insert into oracle temporary table via obproxy, upgrade obproxy first", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "obproxy version is too old, insert into temporary table");
      } else {
        val.set_int(session_info_->get_sess_create_time());
        sess_create_time_expr->set_value(val);
        if (OB_FAIL(value_row.push_back(sess_create_time_expr))) {
          LOG_WARN("push back to output expr failed", K(ret));
        }
        LOG_DEBUG("add session id & sess create time value succeed",
            K(session_id_expr),
            K(sess_create_time_expr),
            K(value_row),
            K(lbt()));
      }
    }
  }
  return ret;
}

int ObInsertResolver::add_new_column_for_oracle_temp_table(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (is_oracle_tmp_table_) {
    const schema::ObColumnSchemaV2* column_schema = NULL;
    const schema::ObColumnSchemaV2* column_schema2 = NULL;
    const ObTableSchema* table_schema = NULL;
    ObColumnRefRawExpr* session_id_expr = NULL;
    ObColumnRefRawExpr* sess_create_time_expr = NULL;
    if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid session_info_", K(session_info_));
    } else if (OB_FAIL(schema_checker_->get_table_schema(table_id, table_schema))) {
      LOG_WARN("not find table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
    } else if (OB_ISNULL(column_schema = (table_schema->get_column_schema(OB_HIDDEN_SESSION_ID_COLUMN_ID)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_, *column_schema, session_id_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(session_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session id expr is null");
    } else if (OB_FAIL(session_id_expr->formalize(session_info_))) {
      LOG_WARN("fail to formalize rowkey", KPC(session_id_expr), K(ret));
    } else if (OB_ISNULL(column_schema2 = (table_schema->get_column_schema(OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(ret));
    } else if (OB_FAIL(
                   ObRawExprUtils::build_column_expr(*params_.expr_factory_, *column_schema2, sess_create_time_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(sess_create_time_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session create time expr is null");
    } else if (OB_FAIL(sess_create_time_expr->formalize(session_info_))) {
      LOG_WARN("fail to formalize rowkey", KPC(sess_create_time_expr), K(ret));
    } else {
      session_id_expr->set_ref_id(table_schema->get_table_id(), column_schema->get_column_id());
      session_id_expr->set_column_attr(table_schema->get_table_name(), column_schema->get_column_name_str());
      sess_create_time_expr->set_ref_id(table_schema->get_table_id(), column_schema2->get_column_id());
      sess_create_time_expr->set_column_attr(table_schema->get_table_name(), column_schema2->get_column_name_str());
      LOG_DEBUG(
          "add __session_id & __sess_create_time to target succeed", K(*session_id_expr), K(*sess_create_time_expr));
    }

    if (OB_SUCC(ret) && OB_FAIL(mock_values_column_ref(session_id_expr))) {
      LOG_WARN("mock values column reference failed", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(mock_values_column_ref(sess_create_time_expr))) {
      LOG_WARN("mock values column reference failed", K(ret));
    }
  }
  return ret;
}

int ObInsertResolver::resolve_insert_field(const ParseNode& insert_into)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ParseNode* table_node = NULL;
  TableItem* table_item = NULL;
  ObSelectStmt* ref_stmt = NULL;
  CK(OB_NOT_NULL(insert_stmt));
  CK(OB_NOT_NULL(table_node = insert_into.children_[0]));
  // resolve insert table
  // oracle mode allow to use insert subquery... => eg:insert into (select * from t1)v values(1,2,3);
  // When the node child is 2, it means that the above situation exists, and special treatment is required
  if (is_oracle_mode() && table_node->num_child_ == 2) {
    if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
      LOG_WARN("failed to resolve table", K(ret));
    } else if (OB_ISNULL(table_item) || !table_item->is_generated_table() ||
               OB_ISNULL(ref_stmt = table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(table_item), K(ref_stmt), K(ret));
    } else if (OB_UNLIKELY(ref_stmt->get_from_items().count() != 1)) {
      // This is to be compatible with oracle's error reporting behavior.
      // When inserting data directly into the subquery, if the from item in the subquery is not 1 item,
      // it is illegal to report an error.
      // Other situations are similar to the update view judgment, and the judgment will not be repeated here
      ret = OB_ERR_ILLEGAL_VIEW_UPDATE;
      LOG_WARN("not updatable", K(ret));
    } else if (OB_FAIL(set_base_table_for_view(*table_item))) {
      LOG_WARN("set base table for insert view failed", K(ret));
    } else { /*do nothing*/
    }
  } else {
    OZ(resolve_basic_table(*table_node, table_item));
  }
  OZ(column_namespace_checker_.add_reference_table(table_item));
  if (OB_SUCC(ret)) {
    current_scope_ = T_INSERT_SCOPE;
    const ObTableSchema* table_schema = NULL;
    if (TableItem::ALIAS_TABLE == table_item->type_) {
      OZ(schema_checker_->get_table_schema(table_item->get_base_table_item().ref_id_, table_schema));
    } else {
      OZ(schema_checker_->get_table_schema(table_item->get_base_table_item().table_id_, table_schema));
    }
    if (OB_SUCC(ret)) {
      if (table_schema->is_oracle_tmp_table()) {
        // The sessions of the oracle temporary table will not create their own private objects
        // and can only set the flag when the data increases
        session_info_->set_has_temp_table_flag();
        is_oracle_tmp_table_ = true;
      }
    }
  }
  // resolve insert columns
  if (OB_SUCC(ret) && 2 == insert_into.num_child_) {
    OZ(resolve_insert_columns(insert_into.children_[1]));
  }

  OZ(remove_dup_dep_cols_for_heap_table(*insert_stmt));
  return ret;
}

int ObInsertResolver::resolve_insert_assign(const ParseNode& assign_list)
{
  int ret = OB_SUCCESS;
  // insert into t1 set c1=1, c2=1;
  ObTablesAssignments assigns;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid insert stmt", K(ret), K(insert_stmt));
  } else if (OB_FAIL(resolve_assignments(assign_list, assigns, current_scope_))) {
    LOG_WARN("resolve insert set assignment list failed", K(ret));
  } else {
    ObArray<ObRawExpr*> value_row;
    int64_t table_count = assigns.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_count; i++) {
      ObTableAssignment& ts = assigns.at(i);
      int64_t assign_count = ts.assignments_.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < assign_count; j++) {
        ObAssignment& assign = ts.assignments_.at(j);
        // add column_item
        if (OB_ISNULL(assign.column_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid assginment variable", K(i), K(j), K(assign));
        } else if (assign.is_duplicated_) {
          ret = OB_ERR_FIELD_SPECIFIED_TWICE;
          LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(assign.column_expr_->get_column_name()));
        } else if (OB_FAIL(replace_column_to_default(assign.expr_))) {
          LOG_WARN("replace values column to default failed", K(ret));
        } else if (OB_FAIL(assign.expr_->formalize(session_info_))) {
          LOG_WARN("formalize expr failed", K(ret));
        } else if (OB_FAIL(value_row.push_back(assign.expr_))) {
          LOG_WARN("Can not add expr_id to ObArray", K(ret));
        }
      }
      if (OB_SUCCESS == ret && OB_FAIL(add_new_value_for_oracle_temp_table(value_row))) {
        LOG_WARN("add cur session value failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_value_row(value_row))) {
        LOG_WARN("Add value-row to ObInsertStmt error", K(ret));
      }
    }
  }

  return ret;
}

int ObInsertResolver::resolve_values(const ParseNode& value_node)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  void* select_buffer = NULL;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stat", K(insert_stmt), K_(allocator));
  } else if (T_VALUE_LIST == value_node.type_) {
    // value list
    if (OB_FAIL(resolve_insert_values(&value_node))) {
      LOG_WARN("resolve insert values failed", K(ret));
    }
  } else if (T_ASSIGN_LIST == value_node.type_) {
    if (OB_FAIL(resolve_insert_assign(value_node))) {
      LOG_WARN("resolve insert assign list failed", K(ret));
    }
  } else if (OB_ISNULL(select_buffer = allocator_->alloc(sizeof(ObSelectResolver)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate select buffer failed", K(ret), "size", sizeof(ObSelectResolver));
  } else {
    // value from sub-query(insert into table select ..)
    ObSelectStmt* select_stmt = NULL;
    sub_select_resolver_ = new (select_buffer) ObSelectResolver(params_);
    // insert clause and select clause in insert into select belong to the same namespace level
    // so select resolver current level equal to insert resolver
    sub_select_resolver_->set_current_level(current_level_);
    // The select layer should not see the attributes of the upper insert stmt,
    // so the upper scope stmt should be empty
    sub_select_resolver_->set_parent_namespace_resolver(NULL);
    TableItem* sub_select_table = NULL;
    ObString view_name;
    ObSEArray<ColumnItem, 4> column_items;
    if (OB_UNLIKELY(T_SELECT != value_node.type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid value node", K(value_node.type_));
    } else if (OB_FAIL(sub_select_resolver_->resolve(value_node))) {
      LOG_WARN("failed to resolve select stmt in INSERT stmt", K(ret));
    } else if (OB_ISNULL(select_stmt = sub_select_resolver_->get_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid select stmt", K(select_stmt));
    } else if (OB_FAIL(check_insert_select_field(*insert_stmt, *select_stmt))) {
      LOG_WARN("check insert select field failed", K(ret), KPC(insert_stmt), KPC(select_stmt));
    } else if (OB_FAIL(add_new_sel_item_for_oracle_temp_table(*select_stmt))) {
      LOG_WARN("add session id value to select item failed", K(ret));
    } else if (OB_FAIL(insert_stmt->generate_view_name(*allocator_, view_name))) {
      LOG_WARN("failed to generate view name", K(ret));
    } else if (OB_FAIL(resolve_generate_table_item(select_stmt, view_name, sub_select_table))) {
      LOG_WARN("failed to resolve generate table item", K(ret));
    } else if (OB_FAIL(resolve_all_generated_table_columns(*sub_select_table, column_items))) {
      LOG_WARN("failed to resolve all generated table columns", K(ret));
    } else {
      insert_stmt->add_from_item(sub_select_table->table_id_);
    }
  }
  return ret;
}

int ObInsertResolver::check_insert_select_field(ObInsertStmt& insert_stmt, ObSelectStmt& select_stmt)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnRefRawExpr*>& values_desc = insert_stmt.get_values_desc();
  if (values_desc.count() != select_stmt.get_select_item_size()) {
    ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
    LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, 1l);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < values_desc.count(); ++i) {
    const ObColumnRefRawExpr* value_desc = values_desc.at(i);
    if (OB_ISNULL(value_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value desc is null", K(ret));
    } else if (value_desc->is_generated_column()) {
      ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
      ColumnItem* orig_col_item = NULL;
      if (NULL != (orig_col_item = insert_stmt.get_column_item_by_id(
                       insert_stmt.get_insert_table_id(), value_desc->get_column_id())) &&
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
  }
  return ret;
}

int ObInsertResolver::add_rowkey_columns_for_insert_up()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get insert stmt", K(insert_stmt));
  } else if (OB_FAIL(add_rowkey_ids(insert_stmt->get_insert_base_tid(), insert_stmt->get_primary_key_ids()))) {
    LOG_WARN("fail to add rowkey ids", K(ret));
  }

  return ret;
}

int ObInsertResolver::add_relation_columns()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get insert stmt", K(insert_stmt));
  } else if (insert_stmt->get_table_assignments().count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Table assignments should in insert_stmt only one table", K(ret));
  } else {
    ObTableAssignment& table_assign = insert_stmt->get_table_assignments().at(0);
    if (insert_stmt->get_insert_table_id() != table_assign.table_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Table assignment should be the table of insert_stmt",
          K(table_assign.table_id_),
          K(insert_stmt->get_insert_table_id()),
          K(ret));
    }
    if (OB_SUCC(ret)) {
      // add auto_incrememnt column
      const ObIArray<ObColumnRefRawExpr*>* table_columns = insert_stmt->get_table_columns();
      ObIArray<share::AutoincParam>& auto_params = insert_stmt->get_autoinc_params();
      CK(OB_NOT_NULL(table_columns));
      for (int64_t i = 0; i < auto_params.count() && OB_SUCCESS == ret; i++) {
        if (auto_params.at(i).autoinc_col_id_ != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
          bool is_exist = false;
          int64_t index = -1;
          for (int64_t j = 0; j < table_columns->count() && OB_SUCC(ret); ++j) {
            const ColumnItem* col_item =
                insert_stmt->get_column_item_by_id(table_assign.table_id_, table_columns->at(j)->get_column_id());
            if (OB_ISNULL(col_item)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column item not found", K(ret));
            } else if (auto_params.at(i).autoinc_col_id_ == col_item->base_cid_) {
              is_exist = true;
              index = j;
              break;
            }
          }
          if (is_exist == false) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("auto increment column id not found in table columns", K(ret), K(auto_params.at(i)));
          } else {
            auto_params.at(i).autoinc_old_value_index_ = index;
          }
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::check_insert_column_duplicate(uint64_t column_id, bool& is_duplicate)
{
  int ret = OB_SUCCESS;
  is_duplicate = false;
  if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id) {
    // do nothing
  } else if (OB_HASH_EXIST == (ret = insert_column_ids_.exist_refactored(column_id))) {
    ret = OB_SUCCESS;
    is_duplicate = true;
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("check column id whether exist failed", K(ret), K(column_id));
  } else if (OB_FAIL(insert_column_ids_.set_refactored(column_id))) {
    LOG_WARN("set column id to insert column ids failed", K(ret), K(column_id));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObInsertResolver::resolve_insert_columns(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert stmt", K(insert_stmt), K_(session_info), K_(schema_checker));
  } else if (OB_ISNULL(table_item = insert_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (NULL != node && T_COLUMN_LIST == node->type_) {
    ParseNode* column_node = NULL;
    is_column_specify_ = true;
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
    if (insert_stmt->get_table_size() != 1 && insert_stmt->get_stmt_type() != stmt::T_MERGE) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("Insert statement only support one table", K(insert_stmt->get_stmt_type()), K(ret));
    }
    ObArray<ColumnItem> column_items;
    if (OB_SUCC(ret)) {
      if (table_item->is_basic_table()) {
        if (OB_FAIL(resolve_all_basic_table_columns(*table_item, false, &column_items))) {
          LOG_WARN("resolve all basic table columns failed", K(ret));
        }
      } else if (table_item->is_generated_table()) {
        if (OB_FAIL(resolve_all_generated_table_columns(*table_item, column_items))) {
          LOG_WARN("resolve all generated table columns failed", K(ret));
        }
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("Only base table or view can be inserted", K(table_item->type_), K(ret));
      }
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

  if (OB_SUCC(ret) && table_item->is_generated_table()) {
    FOREACH_CNT_X(desc, insert_stmt->get_values_desc(), OB_SUCC(ret))
    {
      const ColumnItem* column_item =
          insert_stmt->get_column_item_by_id(table_item->table_id_, (*desc)->get_column_id());
      if (OB_ISNULL(column_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item not found", K(ret));
      } else {
        if (NULL == table_item->view_base_item_) {
          if (OB_FAIL(set_base_table_for_updatable_view(*table_item, *column_item->expr_))) {
            LOG_WARN("find base table failed", K(ret));
          } else if (OB_ISNULL(table_item->view_base_item_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("view base table is NULL", K(ret));
          }
        } else {
          if (OB_FAIL(check_same_base_table(*table_item, *column_item->expr_))) {
            LOG_WARN("check insert columns is same base table failed", K(ret), K(*table_item), K(*column_item->expr_));
          }
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::mock_values_column_ref(const ObColumnRefRawExpr* column_ref)
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

int ObInsertResolver::replace_column_to_default(ObRawExpr*& origin)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(origin)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else {
    if (T_REF_COLUMN == origin->get_expr_type()) {
      ObColumnRefRawExpr* b_expr = static_cast<ObColumnRefRawExpr*>(origin);
      ColumnItem* column_item = NULL;
      ObDefaultValueUtils utils(insert_stmt, &params_, static_cast<ObDMLResolver*>(this));
      if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(
                        insert_stmt->get_insert_table_id(), b_expr->get_column_id()))) {
        LOG_WARN("fail to get column item", K(ret));
      } else if (OB_FAIL(utils.resolve_column_ref_in_insert(column_item, origin))) {
        LOG_WARN("fail to resolve column ref in insert", K(ret));
      }
    } else {
      int64_t N = origin->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ObRawExpr*& cur_child = origin->get_param_expr(i);
        if (OB_FAIL(replace_column_to_default(cur_child))) {
          LOG_WARN("failed to replace child column_ref expr to default value", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::check_returning_validity()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret), K(insert_stmt));
  } else if (insert_stmt->value_from_select()) {
    ret = OB_ERR_CMD_NOT_PROPERLY_ENDED;
    LOG_WARN("insert into select does not have returning into clause", K(ret));
  } else if (insert_stmt->get_returning_aggr_item_size() > 0) {
    ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
    LOG_WARN("insert into returning into does not allow group function", K(ret));
  } else if (OB_FAIL(ObDMLResolver::check_returning_validity())) {
    LOG_WARN("check returning validity failed", K(ret));
  }
  return ret;
}

int ObInsertResolver::resolve_insert_values(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  ObArray<ObRawExpr*> value_row;
  uint64_t value_count = OB_INVALID_ID;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(node) || T_VALUE_LIST != node->type_ || OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(insert_stmt), K(node));
  }
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (is_oracle_mode() && 1 < node->num_child_) {
    // values only hold one row in oracle mode
    // ret = OB_ERR_CMD_NOT_PROPERLY_ENDED;

    // LOG_DEBUG("not supported in oracle mode", K(ret));

    /*
     * If the VALUES clause of an INSERT statement contains a record variable, no other
     * variable or value is allowed in the clause.
     */
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode* vector_node = node->children_[i];
    if (OB_ISNULL(vector_node) || OB_ISNULL(vector_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node children", K(i), K(node));
    } else if (vector_node->num_child_ != insert_stmt->get_values_desc().count() &&
               !(1 == vector_node->num_child_ && T_EMPTY == vector_node->children_[0]->type_)) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, i + 1);
      LOG_WARN("Column count doesn't match value count",
          "num_child",
          vector_node->num_child_,
          "vector_count",
          insert_stmt->get_values_desc().count());
    }
    if (OB_SUCC(ret)) {
      for (int32_t j = 0; OB_SUCC(ret) && j < vector_node->num_child_; j++) {
        ObRawExpr* expr = NULL;
        ObRawExpr* tmp_expr = NULL;
        const ObColumnRefRawExpr* column_expr = NULL;
        if (OB_ISNULL(vector_node->children_[j]) || OB_ISNULL(column_expr = insert_stmt->get_values_desc().at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("inalid children node", K(j), K(vector_node));
        } else if (T_EMPTY == vector_node->children_[j]->type_) {
          // nothing todo
        } else {
          uint64_t column_id = column_expr->get_column_id();
          ObDefaultValueUtils utils(insert_stmt, &params_, this);
          bool is_generated_column = false;
          if (OB_FAIL(resolve_sql_expr(*(vector_node->children_[j]), expr))) {
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
            } else if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(
                                     insert_stmt->get_insert_table_id(), column_id))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get column item by id failed", K(insert_stmt->get_insert_table_id()), K(column_id));
            } else if (OB_FAIL(utils.resolve_default_expr(*column_item, expr, T_INSERT_SCOPE))) {
              LOG_WARN("fail to resolve default value",
                  "table_id",
                  insert_stmt->get_insert_table_id(),
                  K(column_id),
                  K(ret));
            }
          } else if (OB_FAIL(check_basic_column_generated(column_expr, insert_stmt, is_generated_column))) {
            LOG_WARN("check column generated failed", K(ret));
          } else if (is_generated_column) {
            ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
            ColumnItem* orig_col_item = NULL;
            if (NULL != (orig_col_item =
                                insert_stmt->get_column_item_by_id(insert_stmt->get_insert_table_id(), column_id)) &&
                orig_col_item->expr_ != NULL) {
              const ObString& column_name = orig_col_item->expr_->get_column_name();
              const ObString& table_name = orig_col_item->expr_->get_table_name();
              LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                  column_name.length(),
                  column_name.ptr(),
                  table_name.length(),
                  table_name.ptr());
            }
          } else if (column_expr->is_table_part_key_column() && expr->has_flag(CNT_SEQ_EXPR)) {
            insert_stmt->set_has_part_key_sequence(true);
          }

          const ObIArray<ObColumnRefRawExpr*>& dep_cols = insert_stmt->get_part_generated_col_dep_cols();
          if (OB_SUCC(ret) && 0 != dep_cols.count()) {
            ColumnItem* col_item = NULL;
            for (int64_t i = 0; OB_SUCC(ret) && i < dep_cols.count(); ++i) {
              const ObColumnRefRawExpr* col_ref = dep_cols.at(i);
              CK(OB_NOT_NULL(col_ref));
              CK(OB_NOT_NULL(
                  col_item = insert_stmt->get_column_item_by_id(col_ref->get_table_id(), col_ref->get_column_id())));
              if (OB_SUCC(ret)) {
                if (NULL == col_item->default_value_expr_) {
                  OZ(utils.resolve_default_expr(*col_item, col_item->default_value_expr_, T_INSERT_SCOPE));
                }
              }
            }
          }

          tmp_expr = expr;
          // mysql support insert into xx values(expr, ...), expr contains column_ref
          // which has default values in column schema, replace column ref to const value here
          // eg: CREATE TABLE t1(f1 VARCHAR(100) DEFAULT 'test',id int primary key);
          //     INSERT INTO t1 VALUES(SUBSTR(f1, 1, 3),1);
          //     select * from t1;
          //      f1  id
          //      tes 1
          if (OB_SUCC(ret)) {
            if (tmp_expr->has_flag(CNT_COLUMN)) {
              if (OB_FAIL(replace_column_ref(&value_row, tmp_expr))) {
                LOG_WARN("fail to replace column ref", K(ret));
              } else {
                SQL_RESV_LOG(DEBUG, "replace column to default", K(ret), K(*tmp_expr));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(tmp_expr->formalize(session_info_))) {
              LOG_WARN("formalize value expr failed", K(ret));
            } else if (OB_FAIL(value_row.push_back(tmp_expr))) {
              LOG_WARN("Can not add expr_id to ObArray", K(ret));
            }
          }
        }  // end else
      }    // end for
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID == value_count) {
        value_count = value_row.count();
      }
      if (OB_FAIL(check_column_value_pair(&value_row, i + 1, value_count))) {
        LOG_WARN("fail to check column value count", K(ret));
      } else if (is_all_default_) {
        // Handle the situation of insert into test values(),()
        if (OB_FAIL(build_row_for_empty_brackets(value_row))) {
          LOG_WARN("fail to build row for empty brackets", K(ret));
        }
      } else {
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_new_value_for_oracle_temp_table(value_row))) {
          LOG_WARN("failed to add __session_id value");
        } else if (OB_FAIL(add_value_row(value_row))) {
          LOG_WARN("Add value-row to ObInsertStmt error", K(ret));
        }
      }
    }
    value_row.reset();
  }
  return ret;
}

int ObInsertResolver::check_column_value_pair(
    ObArray<ObRawExpr*>* value_row, const int64_t row_index, const uint64_t value_count)
{
  int ret = OB_SUCCESS;
  // If the column is actually specified, then values() is not allowed to be empty
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(value_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(value_row));
  } else if (value_row->count() == 0) {
    is_all_default_ = true;
    if (is_column_specify_) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, row_index);
    } else {
    }
  } else {
  }
  if (OB_SUCC(ret)) {
    uint64_t row_value_count = value_row->count();
    if (value_count != row_value_count) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, row_index);
    } else {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(insert_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid insert stmt", K(insert_stmt));
    } else if (value_row->count() != 0 && insert_stmt->get_values_desc().count() != value_row->count()) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, row_index);
    } else {
    }
  }
  return ret;
}

int ObInsertResolver::get_value_row_size(uint64_t& value_row_size) const
{
  int ret = OB_SUCCESS;
  value_row_size = 0;
  ObInsertStmt* insert_stmt = const_cast<ObInsertResolver*>(this)->get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get insert stmt", K(insert_stmt));
  } else {
    if (insert_stmt->value_from_select()) {
      value_row_size = 1;
    } else {
      value_row_size = insert_stmt->get_row_count();
    }
  }
  return ret;
}

int ObInsertResolver::resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_basic_column_ref(q_name, real_ref_expr))) {
    if ((OB_ERR_BAD_FIELD_ERROR == ret || OB_ERR_COLUMN_NOT_FOUND == ret) && sub_select_resolver_ != NULL) {
      // In insert into select stmt, insert clause and select clause belong to the same namespace
      // the on duplicate key update clause will see the column in select clause,
      // so need to add table reference to insert resolve column namespace checker
      int64_t idx = -1;
      ObString dummy_name;
      TableItem* view = NULL;
      ColumnItem* col_item = NULL;
      ObSelectStmt* sel_stmt = sub_select_resolver_->get_select_stmt();
      ObInsertStmt* insert_stmt = get_insert_stmt();
      if (OB_ISNULL(sel_stmt) || OB_ISNULL(insert_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret), K(sel_stmt), K(insert_stmt));
      } else if (OB_UNLIKELY(insert_stmt->get_from_item_size() != 1) ||
                 OB_ISNULL(view = insert_stmt->get_table_item_by_id(insert_stmt->get_from_item(0).table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("does not find generated table item", K(ret), K(insert_stmt->get_from_items()));
      } else if (OB_FAIL(sub_select_resolver_->resolve_column_ref_expr(q_name, real_ref_expr))) {
        LOG_WARN("resolve column ref expr in sub select resolver failed", K(ret), K(q_name));
      } else if (!real_ref_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is expected to be column", K(ret));
      } else {
        for (idx = 0; OB_SUCC(ret) && idx < sel_stmt->get_select_item_size(); ++idx) {
          if (OB_ISNULL(sel_stmt->get_select_item(idx).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("select expr is null", K(ret));
          } else if (sel_stmt->get_select_item(idx).expr_ == real_ref_expr) {
            break;
          }
        }
        if (idx == sel_stmt->get_select_item_size()) {
          SelectItem sel_item;
          sel_item.expr_ = real_ref_expr;
          sel_item.expr_name_ = static_cast<ObColumnRefRawExpr*>(real_ref_expr)->get_column_name();
          sel_item.alias_name_ = sel_item.expr_name_;
          if (OB_FAIL(sel_stmt->add_select_item(sel_item))) {
            LOG_WARN("failed to add select item", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_generated_table_column_item(
                  *view, dummy_name, col_item, insert_stmt, idx + OB_APP_MIN_COLUMN_ID))) {
            LOG_WARN("failed to resolve generated table column item", K(ret));
          } else if (OB_ISNULL(col_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column item is null", K(ret), K(col_item));
          } else {
            real_ref_expr = col_item->expr_;
          }
        }
      }
    } else {
      LOG_WARN("resolve basic column ref failed", K(ret), K(q_name));
    }
  }
  return ret;
}

int ObInsertResolver::resolve_order_item(const ParseNode& sort_node, OrderItem& order_item)
{
  UNUSED(sort_node);
  UNUSED(order_item);
  return OB_NOT_SUPPORTED;
}

int ObInsertResolver::build_row_for_empty_brackets(ObArray<ObRawExpr*>& value_row)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (!is_all_default_) {
    // nothing to do
  } else if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", K(insert_stmt));
  } else {
    ColumnItem* item = NULL;
    ObDefaultValueUtils utils(insert_stmt, &params_, static_cast<ObDMLResolver*>(this));
    for (int64_t i = 0; i < insert_stmt->get_values_desc().count() && OB_SUCCESS == ret; ++i) {
      ObRawExpr* expr = NULL;
      int64_t column_id = insert_stmt->get_values_desc().at(i)->get_column_id();
      if (OB_ISNULL(item = insert_stmt->get_column_item_by_id(insert_stmt->get_insert_table_id(), column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column item", K(column_id));
      } else if (OB_UNLIKELY(item->expr_->is_generated_column())) {
        if (OB_FAIL(ObRawExprUtils::copy_expr(
                *params_.expr_factory_, item->expr_->get_dependant_expr(), expr, COPY_REF_DEFAULT))) {
          LOG_WARN("copy generated column dependant expr failed", K(ret));
        } else if (expr->has_flag(CNT_COLUMN)) {
          if (OB_FAIL(replace_column_ref(&value_row, expr))) {
            LOG_WARN("replace column reference failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(value_row.push_back(expr))) {
          LOG_WARN("fail to push back value expr", K(ret));
        }
      } else if (item->is_auto_increment()) {
        // insert into t (..) values (); the nextval expression cannot be automatically generated,
        // but null should be generated
        if (OB_FAIL(ObRawExprUtils::build_null_expr(*params_.expr_factory_, expr))) {
          LOG_WARN("failed to build next_val expr as null", K(ret));
        } else if (OB_FAIL(value_row.push_back(expr))) {
          LOG_WARN("fail to push back value expr", K(ret));
        }
      } else {
        if (OB_FAIL(utils.generate_insert_value(item, expr))) {
          LOG_WARN("fail to generate insert values", K(ret), K(column_id));
        } else if (OB_FAIL(value_row.push_back(expr))) {
          LOG_WARN("fail to push back value expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInsertResolver::check_has_unique_index()
{
  int ret = OB_SUCCESS;
  const ObTableSchema* index_schema[common::OB_MAX_INDEX_PER_TABLE];
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ObTableSchema* table_schema = NULL;
  int64_t unique_index_cnt = 0;
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid insert stmt", K(insert_stmt));
  } else if (OB_FAIL(schema_checker_->get_table_schema(insert_stmt->get_insert_base_tid(), table_schema))) {
    LOG_WARN("not find table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (!table_schema->is_no_pk_table()) {
      ++unique_index_cnt;
    }
    if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    } else if (simple_index_infos.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
        if (OB_FAIL(schema_checker_->get_table_schema(simple_index_infos.at(i).table_id_, index_schema[i]))) {
          LOG_WARN("fail to get table schema", K(ret), "table_id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_schema[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get table schema", K(ret), K(index_schema[i]));
        } else if (index_schema[i]->is_unique_index() && !index_schema[i]->is_final_invalid_index()) {
          ++unique_index_cnt;
        }
      }  // end for
    } else {
    }
  }
  if (OB_SUCC(ret)) {
    insert_stmt->set_only_one_unique_key(1 == unique_index_cnt);
  }
  return ret;
}

// Specify the order of input
// In the multi table insert scenario, you need to specify the table_offset of the inserted table in stmt
int ObInsertResolver::set_table_columns(uint64_t table_offset /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ObTableSchema* table_schema = NULL;
  ObArray<ObColumnRefRawExpr*> column_items;
  TableItem* table_item = NULL;
  IndexDMLInfo* primary_dml_info = NULL;
  CK(OB_NOT_NULL(insert_stmt));
  CK(insert_stmt->get_table_items().count() >= 1);
  CK(OB_NOT_NULL(schema_checker_));
  if (OB_SUCC(ret)) {
    table_item = insert_stmt->get_table_item(table_offset);
    CK(OB_NOT_NULL(table_item));
    OZ(schema_checker_->get_table_schema(table_item->get_base_table_item().ref_id_, table_schema), KPC(table_item));
  }
  if (OB_SUCC(ret)) {
    primary_dml_info = insert_stmt->get_or_add_table_columns(table_item->table_id_);
    if (OB_ISNULL(primary_dml_info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get or add table columns failed", K(ret));
    } else {
      primary_dml_info->table_id_ = table_item->table_id_;
      primary_dml_info->loc_table_id_ = table_item->get_base_table_item().table_id_;
      primary_dml_info->index_tid_ = table_item->get_base_table_item().ref_id_;
      primary_dml_info->rowkey_cnt_ = table_schema->get_rowkey_column_num();
      primary_dml_info->part_cnt_ = table_schema->get_partition_cnt();
      primary_dml_info->index_name_ = table_schema->get_table_name_str();
    }
    // first add rowkey columns
    OZ(add_all_rowkey_columns_to_stmt(*table_item, primary_dml_info->column_exprs_), KPC(table_item));
    // second add other table columns
    OZ(add_all_columns_to_stmt(*table_item, primary_dml_info->column_exprs_), KPC(table_item));
  }
  return ret;
}

int ObInsertResolver::add_value_row(ObIArray<ObRawExpr*>& value_row)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert stmt", K(insert_stmt));
  }
  for (; OB_SUCC(ret) && i < value_row.count(); ++i) {
    if (OB_FAIL(insert_stmt->get_value_vectors().push_back(value_row.at(i)))) {
      LOG_WARN("push back expr failed", K(ret));
    }
  }
  return ret;
}

// In the multi table insert scenario, you need to specify the table_offset of the inserted table in stmt
int ObInsertResolver::save_autoinc_params(uint64_t table_offset /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert stmt", K(insert_stmt), K(params_.session_info_));
  } else if (OB_FAIL(schema_checker_->get_table_schema(insert_stmt->get_insert_base_tid(table_offset), table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", insert_stmt->get_insert_base_tid(table_offset));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get table schema", K(ret), K(table_schema));
  } else {
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCCESS == ret && iter != table_schema->column_end();
         ++iter) {
      ObColumnSchemaV2* column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", K(column_schema));
      } else {
        uint64_t column_id = column_schema->get_column_id();
        if (column_schema->is_autoincrement()) {
          insert_stmt->set_affected_last_insert_id(true);
          AutoincParam param;
          param.tenant_id_ = params_.session_info_->get_effective_tenant_id();
          param.autoinc_table_id_ = insert_stmt->get_insert_base_tid(table_offset);
          param.autoinc_first_part_num_ = table_schema->get_first_part_num();
          param.autoinc_table_part_num_ = table_schema->get_all_part_num();
          param.autoinc_col_id_ = column_id;
          param.part_level_ = table_schema->get_part_level();
          ObObjType column_type = table_schema->get_column_schema(column_id)->get_data_type();
          param.autoinc_col_type_ = column_type;
          param.autoinc_desired_count_ = 0;
          // hidden pk auto-increment variables' default value is 1
          // auto-increment variables for other columns are set in ob_sql.cpp
          // because physical plan may come from plan cache; it need be reset every time
          if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id) {
            param.autoinc_increment_ = 1;
            param.autoinc_offset_ = 1;
          }
          uint64_t tid = insert_stmt->get_insert_table_id(table_offset);
          const ObIArray<ObColumnRefRawExpr*>* table_columns = NULL;
          // get auto-increment column index in insert columns
          if (OB_ISNULL(table_columns = insert_stmt->get_table_columns(table_offset))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fail to get table schema", K(ret), K(table_columns));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < table_columns->count() && OB_SUCC(ret); ++i) {
              const ColumnItem* col_item =
                  insert_stmt->get_column_item_by_id(tid, table_columns->at(i)->get_column_id());
              if (OB_ISNULL(col_item)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column item not found", KK(ret));
              } else if (column_id == col_item->base_cid_) {
                param.autoinc_col_index_ = i;
                break;
              }
            }
            if (OB_FAIL(get_value_row_size(param.total_value_count_))) {
              LOG_WARN("fail to get value row size", K(ret));
            } else if (OB_FAIL(insert_stmt->get_autoinc_params().push_back(param))) {
              LOG_WARN("failed to push autoinc_param", K(param), K(ret));
            }
          }
        }
      }
    }  // end for
  }
  LOG_DEBUG("generate autoinc_params", "autoin_params", insert_stmt->get_autoinc_params());
  return ret;
}

int ObInsertResolver::resolve_insert_update_assignment(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(node) || T_ASSIGN_LIST != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(insert_stmt), K(node));
  }
  if (OB_SUCC(ret)) {
    if (insert_stmt->is_replace()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("REPLACE statement does not support ON DUPLICATE KEY UPDATE clause");
    } else if (OB_FAIL(add_rowkey_columns_for_insert_up())) {
      LOG_WARN("Add needed columns error", K(ret));
    } else if (OB_FAIL(resolve_assignments(*node, insert_stmt->get_table_assignments(), T_UPDATE_SCOPE))) {
      LOG_WARN("resolve assignment error", K(ret));
    } else if (OB_FAIL(resolve_additional_assignments(insert_stmt->get_table_assignments(), T_INSERT_SCOPE))) {
      LOG_WARN("resolve additional assignment error", K(ret));
    } else if (insert_stmt->get_table_assignments().count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Table assignments in insert_stmt should only one table", K(ret));
    } else if (OB_FAIL(add_relation_columns())) {
      LOG_WARN("Add needed columns error", K(ret));
    }
  }
  return ret;
}

// In the multi table insert scenario, you need to specify the table_offset of the inserted table in stmt
int ObInsertResolver::resolve_duplicate_key_checker(uint64_t table_offset /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert stmt is null");
  } else {
    uint64_t table_id = insert_stmt->get_insert_base_tid(table_offset);
    const ObTableSchema* tbl_schema = NULL;
    const ObTableSchema* index_schema = NULL;
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    bool has_global_unique_index = false;
    ObUniqueConstraintInfo constraint_info;
    ObUniqueConstraintCheckStmt& constraint_check_stmt = insert_stmt->get_constraint_check_stmt();
    // Duplicate check information of the main table
    OZ(schema_checker_->get_table_schema(table_id, tbl_schema), table_id);
    CK(OB_NOT_NULL(tbl_schema));
    OZ(resolve_dupkey_scan_info(*tbl_schema, false, constraint_check_stmt.table_scan_info_, table_offset));
    OZ(resolve_unique_index_constraint_info(*tbl_schema, constraint_info, table_offset));
    OZ(constraint_check_stmt.constraint_infos_.push_back(constraint_info));
    OZ(tbl_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos), table_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      constraint_info.reset();
      ObDupKeyScanInfo dupkey_scan_info;
      OZ(schema_checker_->get_table_schema(simple_index_infos.at(i).table_id_, index_schema),
          simple_index_infos.at(i).table_id_);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get index schema", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_schema->is_final_invalid_index() && index_schema->is_unique_index()) {
        OZ(resolve_unique_index_constraint_info(*index_schema, constraint_info, table_offset));
        OZ(constraint_check_stmt.constraint_infos_.push_back(constraint_info));
        if (OB_SUCC(ret) && index_schema->is_global_index_table()) {
          // resolve duplicate checker info of global unique index
          has_global_unique_index = true;
          OZ(resolve_dupkey_scan_info(*index_schema, false, dupkey_scan_info, table_offset));
          OZ(constraint_check_stmt.gui_scan_infos_.push_back(dupkey_scan_info));
        }
      }
    }
    if (OB_SUCC(ret) && has_global_unique_index) {
      // build global unique index lookup operation
      OZ(resolve_dupkey_scan_info(*tbl_schema, true, constraint_check_stmt.gui_lookup_info_, table_offset));
    }
  }
  return ret;
}

// In the multi table insert scenario, you need to specify the table_offset of the inserted table in stmt
int ObInsertResolver::resolve_unique_index_constraint_info(
    const ObTableSchema& index_schema, ObUniqueConstraintInfo& constraint_info, uint64_t table_offset /*default 0*/)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt* insert_stmt = get_insert_stmt();
  CK(OB_NOT_NULL(insert_stmt));
  if (OB_SUCC(ret)) {
    constraint_info.table_id_ = insert_stmt->get_insert_table_id(table_offset);
    constraint_info.index_tid_ = index_schema.get_table_id();
    if (!index_schema.is_index_table()) {
      constraint_info.constraint_name_ = "PRIMARY";
    } else {
      OZ(index_schema.get_index_name(constraint_info.constraint_name_));
    }
  }
  /*
   * create table t1(pk int, a int, b int, primary key (a), unique key uk_a_b (a, b)) partition by hash(a) partitions 3;
   * OceanBase(root@oceanbase)>explain replace into t1 values(80, 81, 82), (100, 101,102)\G;
   * *************************** 1. row ***************************
   * Query Plan: ============================================
   * |ID|OPERATOR           |NAME|EST. ROWS|COST|
   * --------------------------------------------
   *  |0 |MULTI TABLE REPLACE|    |2        |1   |
   *  |1 | EXPRESSION        |    |2        |1   |
   *  ============================================
   *
   *  Outputs & filters:
   *  -------------------------------------
   *    0 - output([column_conv(INT,PS:(11,0),NOT NULL,__values.a)], [column_conv(INT,PS:(11,0),NULL,__values.pk)],
   * [column_conv(INT,PS:(11,0),NULL,__values.b)]), filter(nil), columns([{t1: ({t1: (t1.a, t1.pk, t1.b)}, {uk_a_b:
   * (t1.a, t1.b, uk_a_b.shadow_pk_0)})}]), partitions(p0, p2) 1 - output([__values.pk], [__values.a], [__values.b]),
   * filter(nil) values({80, 81, 82}, {100, 101, 102})
   *
   *  For the above plan, a, b, shadow_pk_0 will be used as constraint_columns expr,
   *  The column dependent on shadow_pk_0 will be replaced with the expression of column_conv(..., __values.*),
   *  And constraint_columns is used for duplicate_checker, in order to achieve more clarity, all duplicate_checker
   *  The expression calculation involved in the final depends on the table column value,
   *   so the shadow_pk_0 used for duplicate_checker depends on
   *  column does not want to be replaced with column_conv(..., __value.*) expression,
   *   so when parsing index_rowkey_exprs here,
   *  Will not use the shadow_pk_0 in the previously generated index_column_exprs,
   *   but regenerate a shadow_pk_0 expression,
   *  The expression ultimately depends on the table column value;
   * */
  OZ(resolve_index_rowkey_exprs(
      constraint_info.table_id_, index_schema, constraint_info.constraint_columns_, false /*use_shared_spk*/));
  return ret;
}

// In the multi table insert scenario, you need to specify the table_offset of the inserted table in stmt
int ObInsertResolver::resolve_dupkey_scan_info(const ObTableSchema& index_schema, bool is_gui_lookup,
    ObDupKeyScanInfo& dupkey_scan_info, uint64_t table_offset /*default 0*/)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  const ObIArray<ObColumnRefRawExpr*>* table_columns = NULL;
  int64_t primary_rowkey_cnt = 0;
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert stmt is null", K(insert_stmt));
  } else {
    dupkey_scan_info.table_id_ = insert_stmt->get_insert_table_id(table_offset);
    dupkey_scan_info.loc_table_id_ =
        (insert_stmt->get_table_items().count() > 0 && insert_stmt->get_table_items().at(table_offset) != NULL)
            ? insert_stmt->get_table_items().at(table_offset)->get_base_table_item().table_id_
            : OB_INVALID_ID;
    dupkey_scan_info.index_tid_ = index_schema.get_table_id();
    dupkey_scan_info.only_data_table_ = is_gui_lookup;
    ObSEArray<ObColumnRefRawExpr*, 8> index_rowkey_exprs;
    if (!insert_stmt->is_multi_insert_stmt()) {
      primary_rowkey_cnt = insert_stmt->get_primary_key_ids().count();
    } else {
      primary_rowkey_cnt = insert_stmt->get_multi_insert_primary_key_ids().at(table_offset).count();
    }
    if (OB_ISNULL(table_columns = insert_stmt->get_table_columns(table_offset))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(table_columns), K(table_offset));
    } else if (OB_FAIL(resolve_index_rowkey_exprs(
                   insert_stmt->get_insert_table_id(table_offset), index_schema, index_rowkey_exprs))) {
      LOG_WARN("resolve index rowkey exprs failed", K(ret), KPC(insert_stmt), K(index_schema));
    } else if (index_schema.is_global_index_table()) {
      // For global unique index,
      // conflict_exprs is index rowkey exprs
      // output_exprs is data table rowkey exprs
      // access_exprs is index rowkey_exprs+data_table rowke exprs
      // First construct conflict_exprs
      if (OB_FAIL(dupkey_scan_info.conflict_exprs_.assign(index_rowkey_exprs))) {
        LOG_WARN("assign dupkey scan info access exprs failed", K(ret), K(index_rowkey_exprs));
      }
      // Reconstruct access_exprs
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dupkey_scan_info.access_exprs_.assign(dupkey_scan_info.conflict_exprs_))) {
          LOG_WARN("construct access exprs failed", K(ret), K(dupkey_scan_info.conflict_exprs_));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < primary_rowkey_cnt; ++i) {
        if (OB_FAIL(add_var_to_array_no_dup(dupkey_scan_info.access_exprs_, table_columns->at(i)))) {
          LOG_WARN("add var to array no duplicate key failed", K(ret), K(dupkey_scan_info));
        }
      }
      // Spell output_exprs again
      for (int64_t i = 0; OB_SUCC(ret) && i < primary_rowkey_cnt; ++i) {
        if (OB_FAIL(dupkey_scan_info.output_exprs_.push_back(table_columns->at(i)))) {
          LOG_WARN("store table columns to dupkey scan output exprs failed", K(ret), K(table_columns));
        }
      }
    } else {
      // For primary index and local unique index,
      // conflict_exprs is data table rowkey exprs + other local unique index column
      // output_exprs is data table rowkey exprs + other data table column exprs
      // For gui look up, conflict_exprs is data table rowkey exprs
      // output exprs is data table rowkey exprs + other data table column exprs
      // access exprs is data table rowkey exprs + other data table column exprs
      // First construct output exprs
      if (OB_FAIL(append(dupkey_scan_info.output_exprs_, *table_columns))) {
        LOG_WARN("append table columns to output exprs failed", K(table_columns));
      } else if (OB_FAIL(dupkey_scan_info.access_exprs_.assign(*table_columns))) {
        LOG_WARN("construct access exprs failed", K(ret), K(table_columns));
      }
      int64_t conflict_expr_cnt = is_gui_lookup ? primary_rowkey_cnt : table_columns->count();
      for (int64_t i = 0; OB_SUCC(ret) && i < conflict_expr_cnt; ++i) {
        if (OB_FAIL(dupkey_scan_info.conflict_exprs_.push_back(table_columns->at(i)))) {
          LOG_WARN("store table column to dupkey scan access exprs failed", K(ret), K(table_columns));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!index_schema.is_index_table()) {
      dupkey_scan_info.table_name_ = index_schema.get_table_name_str();
    } else if (OB_FAIL(index_schema.get_index_name(dupkey_scan_info.table_name_))) {
      LOG_WARN("get index name from index schema failed", K(ret), K(index_schema));
    }
    LOG_DEBUG("resolve duplicate key checker info", K(ret), K(is_gui_lookup), K(dupkey_scan_info));
  }
  return ret;
}

int ObInsertResolver::check_view_insertable()
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  ObInsertStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_insert_stmt()) || stmt->get_table_items().empty() ||
      OB_ISNULL(table = stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL or table item is NULL", K(ret));
  }
  // uv_check_basic already checked
  if (OB_SUCC(ret) && is_mysql_mode() && table->is_generated_table()) {
    // check duplicate base column and non column reference column.
    if (OB_SUCC(ret)) {
      bool has_dup_col = false;
      bool has_non_col_ref = false;
      if (OB_FAIL(ObResolverUtils::uv_check_dup_base_col(*table, has_dup_col, has_non_col_ref))) {
        LOG_WARN("check update view hash duplicate column failed", K(ret));
      } else {
        LOG_DEBUG("update view check duplicate column", K(has_dup_col), K(has_non_col_ref));
        if (has_dup_col || has_non_col_ref) {
          ret = OB_ERR_NON_INSERTABLE_TABLE;
        }
      }
    }

    // check all column belong to the insert base table, some columns already checked in
    // resolving assignment, but some columns are missing. e.g.:
    //     create table t1 (c1 int primary key, c2 int);
    //     create table t2 (c3 int primary key, c4 int);
    //     create view v as select * from t1, t2 where c1 = c3;
    //     insert into v (c2) values (1) on duplicate update c2 = c3 + c4;
    if (OB_SUCC(ret)) {
      bool log_error = true;
      ObSEArray<ObRawExpr*, 4> col_exprs;
      FOREACH_CNT_X(tas, stmt->get_table_assignments(), OB_SUCC(ret))
      {
        FOREACH_CNT_X(as, tas->assignments_, OB_SUCC(ret))
        {
          col_exprs.reuse();
          if (OB_FAIL(ObRawExprUtils::extract_column_exprs(as->expr_, col_exprs))) {
            LOG_WARN("extract column failed", K(ret));
          } else {
            FOREACH_CNT_X(col, col_exprs, OB_SUCC(ret))
            {
              if (OB_FAIL(check_same_base_table(*table, *static_cast<ObColumnRefRawExpr*>(*col), log_error))) {
                LOG_TRACE("check same base table fail", K(ret));
              }
            }
          }
        }
      }
    }

    // subquery in select list is checked in uv_check_dup_base_col

    // check subquery in where
    if (OB_SUCC(ret)) {
      bool ref_update_table = false;
      if (OB_FAIL(ObResolverUtils::uv_check_where_subquery(*table, ref_update_table))) {
        LOG_WARN("update view check where condition failed", K(ret));
      } else {
        LOG_DEBUG("update view check", K(ref_update_table));
        ret = ref_update_table ? OB_ERR_NON_INSERTABLE_TABLE : OB_SUCCESS;
      }
    }

    // check join
    if (OB_SUCC(ret)) {
      bool insertable = true;
      if (OB_FAIL(
              ObResolverUtils::uv_mysql_insertable_join(*table, table->get_base_table_item().ref_id_, insertable))) {
        LOG_WARN("check insertable join failed", K(ret));
      } else {
        LOG_DEBUG("update view check join", K(insertable));
        ret = insertable ? ret : OB_ERR_NON_INSERTABLE_TABLE;
      }
    }

    if (ret == OB_ERR_NON_INSERTABLE_TABLE) {
      LOG_USER_ERROR(OB_ERR_NON_INSERTABLE_TABLE, table->get_table_name().length(), table->get_table_name().ptr());
    }
  }

  if (OB_SUCC(ret) && is_oracle_mode() && table->is_generated_table()) {
    if (OB_SUCC(ret)) {
      bool has_distinct = false;
      if (OB_FAIL(ObResolverUtils::uv_check_oracle_distinct(*table, *session_info_, *schema_checker_, has_distinct))) {
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

    // check dup base column
    if (OB_SUCC(ret)) {
      bool has_dup_col = false;
      bool has_non_col_ref = false;
      if (OB_FAIL(ObResolverUtils::uv_check_dup_base_col(*table, has_dup_col, has_non_col_ref))) {
        LOG_WARN("check update view hash duplicate column failed", K(ret));
      } else {
        LOG_DEBUG("update view check duplicate column", K(has_dup_col), K(has_non_col_ref));
        if (has_dup_col) {
          ret = OB_ERR_NON_INSERTABLE_TABLE;
        }
      }
    }
  }
  return ret;
}

/**
 *
 * Why do you cast here?
 * The processing in this place is very similar to the processing of set operations (eg. union).
 * The main reason is that the type of select item in insert into select is aligned with the type of columns of insert,
 * mainly to be able to
 * Able to form a pdml PK plan. PKEY cannot be done with different types.
 *
 * What is the impact on the old plan? In the old plan scenario, some performance degradation may occur.
 *
 */
int ObInsertResolver::inner_cast(common::ObIArray<ObColumnRefRawExpr*>& target_columns, ObSelectStmt& select_stmt)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  if (target_columns.count() != select_stmt.get_select_items().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert target column and select items",
        K(ret),
        K(target_columns),
        K(select_stmt.get_select_items()));
  }
  for (int64_t i = 0; i < target_columns.count() && OB_SUCC(ret); ++i) {
    SelectItem& select_item = select_stmt.get_select_item(i);
    res_type = target_columns.at(i)->get_result_type();
    ObSysFunRawExpr* new_expr = NULL;
    if (res_type == select_item.expr_->get_result_type()) {
      // no need to generate cast expr.
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                   *params_.expr_factory_, select_item.expr_, res_type, new_expr, session_info_))) {
      LOG_WARN("create cast expr for stmt failed", K(ret));
    } else {
      OZ(new_expr->add_flag(IS_INNER_ADDED_EXPR));
      OX(select_item.expr_ = new_expr);
    }
    LOG_DEBUG("pdml build a cast expr", KPC(target_columns.at(i)), KPC(new_expr));
  }

  return ret;
}

int ObInsertResolver::build_column_conv_function_with_value_desc(
    const int64_t idx, ObRawExpr* column_ref, uint64_t m_index)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_UNLIKELY(insert_stmt->get_column_conv_functions().count() <= idx) ||
      OB_ISNULL(insert_stmt->get_table_columns(m_index)) ||
      OB_ISNULL(insert_stmt->get_table_columns(m_index)->at(idx)) || OB_ISNULL(column_ref)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    uint64_t column_id = OB_INVALID_ID;
    uint64_t table_id = insert_stmt->get_insert_table_id(m_index);
    ColumnItem* column_item = NULL;
    ObRawExpr* function_expr = NULL;

    const ObColumnRefRawExpr* tbl_col = insert_stmt->get_table_columns(m_index)->at(idx);
    column_id = tbl_col->get_column_id();

    if (OB_ISNULL(column_item = insert_stmt->get_column_item_by_id(table_id, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column item", K(ret));
    } else if (OB_FAIL(add_additional_function_according_to_type(column_item,
                   column_ref,
                   T_INSERT_SCOPE,
                   ObObjMeta::is_binary(column_ref->get_data_type(), column_ref->get_collation_type())))) {
      LOG_WARN("failed to build column conv expr", K(ret));
    } else {
      SQL_RESV_LOG(TRACE, "ad column conv expr", K(*column_ref));
      function_expr = column_ref;

      insert_stmt->get_column_conv_functions().at(idx) = function_expr;
    }
  }
  return ret;
}

int ObInsertResolver::build_column_conv_function_with_default_expr(const int64_t idx, uint64_t m_index)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  if (OB_ISNULL(insert_stmt) || OB_UNLIKELY(insert_stmt->get_column_conv_functions().count() <= idx) ||
      OB_ISNULL(insert_stmt->get_table_columns(m_index)) ||
      OB_ISNULL(insert_stmt->get_table_columns(m_index)->at(idx)) || OB_ISNULL(session_info_) ||
      OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    uint64_t table_id = insert_stmt->get_insert_table_id(m_index);
    const ObColumnRefRawExpr* tbl_col = insert_stmt->get_table_columns(m_index)->at(idx);
    uint64_t column_id = tbl_col->get_column_id();

    ColumnItem* column_item = insert_stmt->get_column_item_by_id(table_id, column_id);
    ObRawExpr* function_expr = NULL;
    ObRawExpr* expr = NULL;
    ObDefaultValueUtils utils(insert_stmt, &params_, this);
    if (OB_ISNULL(column_item) || OB_ISNULL(column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column item", K(ret), K(column_item));
    } else if (OB_FAIL(utils.generate_insert_value(column_item, expr))) {
      LOG_WARN("failed to generate insert value", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be null", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (ob_is_enum_or_set_type(expr->get_data_type())) {
      function_expr = expr;
    } else {
      // For char type, compare and hash ignore space
      // For binary type, compare and hash not ignore '\0', so need to padding
      // '\0' for optimizer calculating partition location. As storage do right
      // trim of '\0', so don't worry extra space usage.
      if (ObObjMeta::is_binary(expr->get_data_type(), expr->get_collation_type())) {
        if (OB_FAIL(build_padding_expr(session_info_, column_item, expr))) {
          LOG_WARN("Build padding expr error", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(
                     *params_.expr_factory_, *params_.allocator_, *column_item->get_expr(), expr, session_info_))) {
        LOG_WARN("fail to build column conv expr", K(ret));
      } else {
        function_expr = expr;
        SQL_RESV_LOG(DEBUG, "add column conv expr", K(*function_expr));
      }
    }

    if (OB_SUCC(ret)) {
      insert_stmt->get_column_conv_functions().at(idx) = function_expr;
    }
  }
  return ret;
}

int ObInsertResolver::replace_col_with_new_value(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = get_insert_stmt();
  CK(NULL != insert_stmt)
  {
    const ObIArray<ObColumnRefRawExpr*>& all_cols = *insert_stmt->get_table_columns();
    const auto& conv_funcs = insert_stmt->get_column_conv_functions();
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

// part_generated_col_dep_cols_.count() != 0 means:
//  in heap table, generated column is partition key
//  we need to get all dependant columns of generated column.
// here we remove all columns which dup with values_desc
// eg: create table t1(c1 int, c2 int, c2 int as(c1+c2));
//     insert into t1(c1) values(1);
//       values_desc: c1
//       dep_cols: c1, c2
//       after remove dup: c2
int ObInsertResolver::remove_dup_dep_cols_for_heap_table(ObInsertStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObIArray<ObColumnRefRawExpr*>& dep_cols = stmt.get_part_generated_col_dep_cols();
  if (0 == dep_cols.count()) {
    // do nothing
  } else {
    const ObIArray<ObColumnRefRawExpr*>& values_desc = stmt.get_values_desc();
    ObSEArray<ObColumnRefRawExpr*, 4> cols_no_dup;
    int64_t j = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_cols.count(); ++i) {
      CK(OB_NOT_NULL(dep_cols.at(i)));
      for (j = 0; OB_SUCC(ret) && j < values_desc.count(); ++j) {
        CK(OB_NOT_NULL(values_desc.at(j)));
        if (OB_SUCC(ret) && values_desc.at(j)->get_column_id() == dep_cols.at(i)->get_column_id()) {
          break;
        }
      }                                                // end for
      if (OB_SUCC(ret) && j >= values_desc.count()) {  // not found
        OZ(cols_no_dup.push_back(dep_cols.at(i)));
      }
    }  // end for
    OX(dep_cols.reset());
    OZ(dep_cols.assign(cols_no_dup));
  }
  LOG_DEBUG("remove dup dep cols done", K(dep_cols));
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
