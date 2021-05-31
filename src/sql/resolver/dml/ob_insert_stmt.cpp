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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
namespace oceanbase {
using namespace common;
namespace sql {

int ObDupKeyScanInfo::deep_copy(ObRawExprFactory& expr_factory, const ObDupKeyScanInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  index_tid_ = other.index_tid_;
  loc_table_id_ = other.loc_table_id_;
  table_name_ = other.table_name_;
  only_data_table_ = other.only_data_table_;
  if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.output_exprs_, output_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(conflict_exprs_.assign(other.conflict_exprs_))) {
    LOG_WARN("failed to assign conflict exprs", K(ret));
  } else if (OB_FAIL(access_exprs_.assign(other.access_exprs_))) {
    LOG_WARN("failed to assign access exprs", K(ret));
  }
  return ret;
}

int ObDupKeyScanInfo::assign(const ObDupKeyScanInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(output_exprs_.assign(other.output_exprs_))) {
    LOG_WARN("failed to assign output exprs", K(ret));
  } else if (OB_FAIL(conflict_exprs_.assign(other.conflict_exprs_))) {
    LOG_WARN("failed to assign conflict exprs", K(ret));
  } else if (OB_FAIL(access_exprs_.assign(other.access_exprs_))) {
    LOG_WARN("failed to assign access exprs", K(ret));
  } else {
    table_id_ = other.table_id_;
    index_tid_ = other.index_tid_;
    loc_table_id_ = other.loc_table_id_;
    table_name_ = other.table_name_;
    only_data_table_ = other.only_data_table_;
  }
  return ret;
}

int ObUniqueConstraintInfo::assign(const ObUniqueConstraintInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  index_tid_ = other.index_tid_;
  constraint_name_ = other.constraint_name_;
  if (OB_FAIL(constraint_columns_.assign(other.constraint_columns_))) {
    LOG_WARN("failed to assign constraint columns", K(ret));
  }
  return ret;
}

int ObUniqueConstraintCheckStmt::deep_copy(ObRawExprFactory& expr_factory, const ObUniqueConstraintCheckStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gui_lookup_info_.deep_copy(expr_factory, other.gui_lookup_info_))) {
    LOG_WARN("failed to deep copy gui lookup infos", K(ret));
  } else if (OB_FAIL(table_scan_info_.deep_copy(expr_factory, other.table_scan_info_))) {
    LOG_WARN("failed to deep copy table scan infos", K(ret));
  } else if (OB_FAIL(gui_scan_infos_.prepare_allocate(other.gui_scan_infos_.count()))) {
    LOG_WARN("failed to prepare allocate gui scan infos", K(ret));
  } else if (OB_FAIL(constraint_infos_.prepare_allocate(other.constraint_infos_.count()))) {
    LOG_WARN("failed to prepare allocate constraint infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < gui_scan_infos_.count(); ++i) {
    if (OB_FAIL(gui_scan_infos_.at(i).deep_copy(expr_factory, other.gui_scan_infos_.at(i)))) {
      LOG_WARN("failed to deep copy gui scan infos", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_.count(); ++i) {
    if (OB_FAIL(constraint_infos_.at(i).assign(other.constraint_infos_.at(i)))) {
      LOG_WARN("failed to deep copy constraint infos", K(ret));
    }
  }
  return ret;
}

int ObUniqueConstraintCheckStmt::assign(const ObUniqueConstraintCheckStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gui_lookup_info_.assign(other.gui_lookup_info_))) {
    LOG_WARN("failed to assign gui lookup info", K(ret));
  } else if (OB_FAIL(table_scan_info_.assign(other.table_scan_info_))) {
    LOG_WARN("failed to assign table scan info", K(ret));
  } else if (OB_FAIL(gui_scan_infos_.assign(other.gui_scan_infos_))) {
    LOG_WARN("failed to assign gui scan infos", K(ret));
  } else if (OB_FAIL(constraint_infos_.assign(other.constraint_infos_))) {
    LOG_WARN("failed to assign constraint infos", K(ret));
  } else { /*do nohthing*/
  }
  return ret;
}

ObInsertStmt::ObInsertStmt()
    : ObDelUpdStmt(stmt::T_INSERT),
      is_replace_(false),
      values_desc_(),
      value_vectors_(),
      low_priority_(false),
      high_priority_(false),
      delayed_(false),
      insert_up_(false),
      only_one_unique_key_(false),
      column_conv_functions_(),
      primary_key_ids_(),
      is_all_const_values_(true),
      part_generated_col_dep_cols_(),
      is_multi_insert_stmt_(false),
      multi_values_desc_(),
      multi_value_vectors_(),
      multi_insert_col_conv_funcs_(),
      multi_insert_primary_key_ids_(),
      is_multi_insert_first_(false),
      is_multi_conditions_insert_(false),
      multi_insert_table_info_()
{}

ObInsertStmt::~ObInsertStmt()
{}

int ObInsertStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& input)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt& other = static_cast<const ObInsertStmt&>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(stmt_factory, expr_factory, input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(values_desc_.assign(other.values_desc_))) {
    LOG_WARN("failed to assign value desc", K(ret));
  } else if (OB_FAIL(
                 ObRawExprUtils::copy_exprs(expr_factory, other.value_vectors_, value_vectors_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.column_conv_functions_, column_conv_functions_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy column convert functions", K(ret));
  } else if (OB_FAIL(primary_key_ids_.assign(other.primary_key_ids_))) {
    LOG_WARN("failed to assign primary key ids", K(ret));
  } else if (OB_FAIL(table_assignments_.prepare_allocate(other.table_assignments_.count()))) {
    LOG_WARN("failed to prepare allocate table assignments", K(ret));
  } else {
    is_replace_ = other.is_replace_;
    low_priority_ = other.low_priority_;
    high_priority_ = other.high_priority_;
    delayed_ = other.delayed_;
    insert_up_ = other.insert_up_;
    only_one_unique_key_ = other.only_one_unique_key_;
    is_all_const_values_ = other.is_all_const_values_;
    is_multi_insert_stmt_ = other.is_multi_insert_stmt_;
    is_multi_insert_first_ = other.is_multi_insert_first_;
    is_multi_conditions_insert_ = other.is_multi_conditions_insert_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.table_assignments_.count(); ++i) {
    if (OB_FAIL(table_assignments_.at(i).deep_copy(expr_factory, other.table_assignments_.at(i)))) {
      LOG_WARN("failed to deep copy table assignment", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(constraint_check_stmt_.deep_copy(expr_factory, other.constraint_check_stmt_))) {
      LOG_WARN("failed to deep copy constraint check stmt", K(ret));
    } else if (OB_FAIL(part_generated_col_dep_cols_.assign(other.part_generated_col_dep_cols_))) {
      LOG_WARN("failed to copy other.part_generated_col_dep_cols_", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_values_desc_.count(); ++i) {
      ColRawExprArray tmp_multi_values_desc;
      if (OB_FAIL(ObRawExprUtils::copy_exprs(
              expr_factory, other.multi_values_desc_.at(i), tmp_multi_values_desc, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy exprs", K(ret));
      } else if (OB_FAIL(multi_values_desc_.push_back(tmp_multi_values_desc))) {
        LOG_WARN("failed to push back", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_value_vectors_.count(); ++i) {
      RawExprArray tmp_multi_value_vector;
      if (OB_FAIL(ObRawExprUtils::copy_exprs(
              expr_factory, other.multi_value_vectors_.at(i), tmp_multi_value_vector, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy exprs", K(ret));
      } else if (OB_FAIL(multi_value_vectors_.push_back(tmp_multi_value_vector))) {
        LOG_WARN("failed to push back", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_insert_col_conv_funcs_.count(); ++i) {
      RawExprArray tmp_multi_insert_col_conv_funcs;
      if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory,
              other.multi_insert_col_conv_funcs_.at(i),
              tmp_multi_insert_col_conv_funcs,
              COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy exprs", K(ret));
      } else if (OB_FAIL(multi_insert_col_conv_funcs_.push_back(tmp_multi_insert_col_conv_funcs))) {
        LOG_WARN("failed to push back", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_insert_primary_key_ids_.count(); ++i) {
      ObSEArray<uint64_t, 16> tmp_insert_primary_key_ids;
      if (OB_FAIL(tmp_insert_primary_key_ids.assign(other.multi_insert_primary_key_ids_.at(i)))) {
        LOG_WARN("failed to assign primary key ids", K(ret));
      } else if (OB_FAIL(multi_insert_primary_key_ids_.push_back(tmp_insert_primary_key_ids))) {
        LOG_WARN("failed to push back", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_insert_table_info_.count(); ++i) {
      ObInsertTableInfo tmp_table_info;
      if (OB_FAIL(tmp_table_info.deep_copy(expr_factory, other.multi_insert_table_info_.at(i)))) {
        LOG_WARN("failed to deep copy table info", K(ret));
      } else if (OB_FAIL(multi_insert_table_info_.push_back(tmp_table_info))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObInsertStmt::assign(const ObInsertStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(values_desc_.assign(other.values_desc_))) {
    LOG_WARN("failed to assign values desc", K(ret));
  } else if (OB_FAIL(value_vectors_.assign(other.value_vectors_))) {
    LOG_WARN("failed to assign value vectors", K(ret));
  } else if (OB_FAIL(column_conv_functions_.assign(other.column_conv_functions_))) {
    LOG_WARN("failed to assign column conv functions", K(ret));
  } else if (OB_FAIL(primary_key_ids_.assign(other.primary_key_ids_))) {
    LOG_WARN("failed to assign primary key ids", K(ret));
  } else if (OB_FAIL(table_assignments_.assign(other.table_assignments_))) {
    LOG_WARN("failed to assign table assignments", K(ret));
  } else if (OB_FAIL(constraint_check_stmt_.assign(other.constraint_check_stmt_))) {
    LOG_WARN("failed to assign constraint check stmt", K(ret));
  } else if (OB_FAIL(part_generated_col_dep_cols_.assign(other.part_generated_col_dep_cols_))) {
    LOG_WARN("failed to copy other.part_generated_col_dep_cols_", K(ret));
  } else if (OB_FAIL(multi_values_desc_.assign(other.multi_values_desc_))) {
    LOG_WARN("failed to copy other.multi_values_desc_", K(ret));
  } else if (OB_FAIL(multi_value_vectors_.assign(other.multi_value_vectors_))) {
    LOG_WARN("failed to copy other.multi_value_vectors_", K(ret));
  } else if (OB_FAIL(multi_insert_col_conv_funcs_.assign(other.multi_insert_col_conv_funcs_))) {
    LOG_WARN("failed to copy other.multi_insert_col_conv_funcs_", K(ret));
  } else if (OB_FAIL(multi_insert_primary_key_ids_.assign(other.multi_insert_primary_key_ids_))) {
    LOG_WARN("failed to copy other.multi_insert_primary_key_ids_", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_insert_table_info_.count(); ++i) {
      ObInsertTableInfo tmp_table_info;
      if (OB_FAIL(tmp_table_info.assign(other.multi_insert_table_info_.at(i)))) {
        LOG_WARN("failed to assign table info", K(ret));
      } else if (OB_FAIL(multi_insert_table_info_.push_back(tmp_table_info))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_replace_ = other.is_replace_;
    low_priority_ = other.low_priority_;
    high_priority_ = other.high_priority_;
    delayed_ = other.delayed_;
    insert_up_ = other.insert_up_;
    only_one_unique_key_ = other.only_one_unique_key_;
    is_all_const_values_ = other.is_all_const_values_;
    is_multi_insert_stmt_ = other.is_multi_insert_stmt_;
    is_multi_insert_first_ = other.is_multi_insert_first_;
    is_multi_conditions_insert_ = other.is_multi_conditions_insert_;
  }
  return ret;
}

int64_t ObInsertStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  ObSEArray<ObSelectStmt*, 1> child_stmts;
  if (get_child_stmts(child_stmts) != OB_SUCCESS) {
    databuff_printf(buf, buf_len, pos, "ERROR get child stmts failed");
  } else {
    J_KV(N_STMT_TYPE,
        ((int)stmt_type_),
        N_TABLE,
        table_items_,
        N_FROM,
        from_items_,
        N_PARTITION_EXPR,
        part_expr_items_,
        K_(all_table_columns),
        N_IS_IGNORE,
        ignore_,
        N_PRIMARY_CID,
        primary_key_ids_,
        N_COLUMN_CONV_FUNCTION,
        column_conv_functions_,
        N_QUERY_HINT,
        stmt_hint_,
        N_QUERY_CTX,
        *query_ctx_,
        N_VALUE,
        value_vectors_,
        "value_desc",
        values_desc_,
        "returning",
        returning_exprs_,
        "part_generated_col_dep_cols",
        part_generated_col_dep_cols_,
        N_CHILD_STMT,
        child_stmts);
    if (insert_up_) {
      J_COMMA();
      J_KV(N_ASSIGN, table_assignments_);
    }
    if (is_multi_insert_stmt_) {
      J_KV(N_IS_MULTI_TABLE_INSERT, is_multi_insert_stmt_);
      J_KV(N_IS_MULTI_INSERT_FIRST, is_multi_insert_first_);
      J_KV(N_IS_MULTI_CONDITIONS_INSERT, is_multi_conditions_insert_);
      J_KV(N_MULTI_VALUES_DESC, multi_values_desc_);
      J_KV(N_MULTI_VALUE_VECTORS, multi_value_vectors_);
      J_KV(N_MULTI_INSERT_COL_CONV_FUNCS, multi_insert_col_conv_funcs_);
    }
  }
  J_OBJ_END();
  return pos;
}

int ObInsertStmt::inner_get_relation_exprs(RelExprCheckerBase& expr_checker)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_assignments_.count(); ++i) {
    ObTableAssignment& table_assign = table_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObAssignment& assign = table_assign.assignments_.at(j);
      ObRawExpr*& column_expr = reinterpret_cast<ObRawExpr*&>(assign.column_expr_);
      if (OB_FAIL(expr_checker.add_expr((table_assignments_.at(i)).assignments_.at(j).expr_))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      } else if (OB_FAIL(expr_checker.add_expr(column_expr))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      }
    }
  }
  // Add the value expr corresponding to the partition key in insert values
  int64_t value_desc_cnt = values_desc_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < value_desc_cnt; ++i) {
    const ObColumnRefRawExpr* col_expr = values_desc_.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (col_expr->is_table_part_key_column()) {
      for (int64_t j = i; OB_SUCC(ret) && j < value_vectors_.count(); j += value_desc_cnt) {
        if (OB_FAIL(expr_checker.add_expr(value_vectors_.at(j)))) {
          LOG_WARN("add expr to expr checker failed", K(ret), K(i), K(j));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !value_from_select() && !subquery_exprs_.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < value_vectors_.count(); i++) {
      if (OB_ISNULL(value_vectors_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (value_vectors_.at(i)->has_flag(CNT_SUB_QUERY) &&
                 OB_FAIL(expr_checker.add_expr(value_vectors_.at(i)))) {
        LOG_WARN("failed to add expr to expr checker", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr_checker.add_exprs(column_conv_functions_))) {
      LOG_WARN("failed to add exprs to expr checker", K(ret));
    } else if (OB_FAIL(ObDelUpdStmt::inner_get_relation_exprs(expr_checker))) {
      LOG_WARN("get delup stmt relation exprs failed", K(ret));
    }
  }
  if (is_multi_insert_stmt()) {  // multi table insert
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_value_vectors_.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < multi_value_vectors_.at(i).count(); ++j) {
        if (OB_FAIL(expr_checker.add_expr(multi_value_vectors_.at(i).at(j)))) {
          LOG_WARN("add expr to expr checker failed", K(ret), K(j), K(j));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_col_conv_funcs_.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < multi_insert_col_conv_funcs_.at(i).count(); ++j) {
        if (OB_FAIL(expr_checker.add_expr(multi_insert_col_conv_funcs_.at(i).at(j)))) {
          LOG_WARN("add expr to expr checker failed", K(ret), K(j), K(j));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_table_info_.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < multi_insert_table_info_.at(i).when_conds_expr_.count(); ++j) {
        if (OB_FAIL(expr_checker.add_expr(multi_insert_table_info_.at(i).when_conds_expr_.at(j)))) {
          LOG_WARN("add expr to expr checker failed", K(ret), K(j), K(j));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < multi_insert_table_info_.at(i).check_constraint_exprs_.count(); ++j) {
        if (OB_FAIL(expr_checker.add_expr(multi_insert_table_info_.at(i).check_constraint_exprs_.at(j)))) {
          LOG_WARN("add expr to expr checker failed", K(ret), K(j), K(j));
        }
      }
    }
  }
  return ret;
}

int ObInsertStmt::replace_inner_stmt_expr(
    const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::replace_inner_stmt_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, values_desc_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, value_vectors_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, column_conv_functions_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, part_generated_col_dep_cols_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_assignments_.count(); i++) {
      if (OB_FAIL(replace_table_assign_exprs(other_exprs, new_exprs, table_assignments_.at(i).assignments_))) {
        LOG_WARN("failed to replace table assign expr", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(replace_dupkey_exprs(other_exprs, new_exprs, constraint_check_stmt_.gui_lookup_info_))) {
      LOG_WARN("failed to replace dupkey exprs", K(ret));
    } else if (OB_FAIL(replace_dupkey_exprs(other_exprs, new_exprs, constraint_check_stmt_.table_scan_info_))) {
      LOG_WARN("failed to replace dupkey exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_check_stmt_.gui_scan_infos_.count(); i++) {
        if (OB_FAIL(replace_dupkey_exprs(other_exprs, new_exprs, constraint_check_stmt_.gui_scan_infos_.at(i)))) {
          LOG_WARN("failed to replace dupkey exprs", K(ret));
        } else { /*do nothing*/
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_check_stmt_.constraint_infos_.count(); i++) {
        if (OB_FAIL(ObTransformUtils::replace_exprs(
                other_exprs, new_exprs, constraint_check_stmt_.constraint_infos_.at(i).constraint_columns_))) {
          LOG_WARN("failed to replace exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_values_desc_.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, multi_values_desc_.at(i)))) {
        LOG_WARN("failed to replace exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_value_vectors_.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, multi_value_vectors_.at(i)))) {
        LOG_WARN("failed to replace exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_col_conv_funcs_.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, multi_insert_col_conv_funcs_.at(i)))) {
        LOG_WARN("failed to replace exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_table_info_.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::replace_exprs(
              other_exprs, new_exprs, multi_insert_table_info_.at(i).when_conds_expr_))) {
        LOG_WARN("failed to replace exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_exprs(
                     other_exprs, new_exprs, multi_insert_table_info_.at(i).check_constraint_exprs_))) {
        LOG_WARN("failed to replace exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertStmt::replace_dupkey_exprs(const common::ObIArray<ObRawExpr*>& other_exprs,
    const common::ObIArray<ObRawExpr*>& new_exprs, ObDupKeyScanInfo& scan_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, scan_info.access_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, scan_info.output_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, scan_info.conflict_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObInsertStmt::inner_get_relation_exprs_for_wrapper(RelExprChecker& expr_checker)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_assignments_.count(); ++i) {
    ObTableAssignment& table_assign = table_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObAssignment& assign = table_assign.assignments_.at(j);
      ObRawExpr*& column_expr = reinterpret_cast<ObRawExpr*&>(assign.column_expr_);
      if (OB_FAIL(expr_checker.add_expr((table_assignments_.at(i)).assignments_.at(j).expr_))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      } else if (OB_FAIL(expr_checker.add_expr(column_expr))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDelUpdStmt::inner_get_relation_exprs_for_wrapper(expr_checker))) {
      LOG_WARN("get delup stmt relation exprs failed", K(ret));
    }
  }
  return ret;
}

int ObInsertStmt::get_tables_assignments_exprs(ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_assignments_.count(); ++i) {
    const ObTableAssignment& table_assignment = table_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assignment.assignments_.count(); ++j) {
      const ObAssignment& assignment = table_assignment.assignments_.at(j);
      if (OB_FAIL(exprs.push_back(assignment.expr_))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObInsertStmt::replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_conv_functions_.count(); ++i) {
    if (column_conv_functions_.at(i) == from) {
      column_conv_functions_.at(i) = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_assignments_.count(); ++i) {
    ObTableAssignment& table_assign = table_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObAssignment& assign = table_assign.assignments_.at(j);
      if (assign.expr_ == from) {
        assign.expr_ = to;
      }
    }
  }
  int64_t value_desc_cnt = values_desc_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < value_desc_cnt; ++i) {
    const ObColumnRefRawExpr* col_expr = values_desc_.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (col_expr->is_table_part_key_column()) {
      for (int64_t j = i; OB_SUCC(ret) && j < value_vectors_.count(); j += value_desc_cnt) {
        if (value_vectors_.at(j) == from) {
          value_vectors_.at(j) = to;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDelUpdStmt::replace_expr_in_stmt(from, to))) {
    LOG_WARN("replace expr in stmt failed", K(ret));
  }
  return ret;
}

int ObInsertStmt::part_key_has_rand_value(bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  // Add the value expr corresponding to the partition key in insert values
  int64_t value_desc_cnt = values_desc_.count();
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < value_desc_cnt; ++i) {
    ObColumnRefRawExpr* col_expr = values_desc_.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (IS_SHADOW_COLUMN(col_expr->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_base_column(this, col_expr))) {
      LOG_WARN("failed to get base column", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (col_expr->is_table_part_key_column()) {
      for (int64_t j = i; OB_SUCC(ret) && !has && j < value_vectors_.count(); j += value_desc_cnt) {
        if (value_vectors_.at(j)->has_flag(CNT_RAND_FUNC) || value_vectors_.at(j)->has_flag(CNT_STATE_FUNC)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

int ObInsertStmt::part_key_has_subquery(bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < values_desc_.count(); i++) {
    ObColumnRefRawExpr* column_expr = NULL;
    if (OB_ISNULL(column_expr = values_desc_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (IS_SHADOW_COLUMN(column_expr->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_base_column(this, column_expr))) {
      LOG_WARN("failed to get base column", K(ret));
    } else if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (column_expr->is_table_part_key_column()) {
      for (int64_t j = i; OB_SUCC(ret) && !has && j < value_vectors_.count(); j += values_desc_.count()) {
        if (value_vectors_.at(j)->has_flag(CNT_SUB_QUERY) || value_vectors_.at(j)->has_flag(CNT_EXEC_PARAM)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

int64_t ObInsertStmt::get_value_index(uint64_t table_id, uint64_t column_id) const
{
  int64_t index = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_INVALID_INDEX == index && i < values_desc_.count(); ++i) {
    const ObColumnRefRawExpr* col_ref = values_desc_.at(i);
    if (col_ref != NULL && col_ref->get_column_id() == column_id && col_ref->get_table_id() == table_id) {
      index = i;
    }
  }
  return index;
}

int ObInsertStmt::get_value_exprs(ObIArray<ObRawExpr*>& value_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_conv_functions_.count(); ++i) {
    ObRawExpr* param = NULL;
    if (OB_ISNULL(column_conv_functions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column conv function is null", K(ret));
    } else if (column_conv_functions_.at(i)->get_expr_type() != T_FUN_COLUMN_CONV) {
      param = column_conv_functions_.at(i);
    } else {
      param = column_conv_functions_.at(i)->get_param_expr(4);
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (OB_FAIL(value_exprs.push_back(param))) {
        LOG_WARN("failed to push back param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertStmt::get_insert_columns(common::ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  const IndexDMLInfo* index_dml_info = NULL;
  if (OB_ISNULL(index_dml_info = get_table_dml_info(get_insert_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index dml info is null", K(ret), K(index_dml_info));
  } else if (OB_FAIL(append(columns, index_dml_info->column_exprs_))) {
    LOG_WARN("failed to append column exprs", K(ret));
  }
  return ret;
}

int ObInsertStmt::get_values_desc_for_heap_table(common::ObIArray<ObColumnRefRawExpr*>& arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_generated_col_dep_cols_.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("use get_values_desc instead", K(ret));
  } else {
    arr.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < values_desc_.count(); ++i) {
      if (OB_FAIL(arr.push_back(values_desc_.at(i)))) {
        LOG_WARN("push back expr failed", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_generated_col_dep_cols_.count(); ++i) {
      if (OB_FAIL(arr.push_back(part_generated_col_dep_cols_.at(i)))) {
        LOG_WARN("push back expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertStmt::get_value_vectors_for_heap_table(common::ObIArray<ObRawExpr*>& arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_generated_col_dep_cols_.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("use get_values_desc instead", K(ret));
  } else {
    arr.reset();
    const int64_t row_num = value_vectors_.count() / values_desc_.count();
    const int64_t col_num = values_desc_.count();
    ObSEArray<ObRawExpr*, 4> def_values;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_generated_col_dep_cols_.count(); ++i) {
      ColumnItem* col_item = NULL;
      ObColumnRefRawExpr* col_ref = part_generated_col_dep_cols_.at(i);
      CK(OB_NOT_NULL(col_ref));
      OX(col_item = get_column_item_by_id(col_ref->get_table_id(), col_ref->get_column_id()));
      CK(OB_NOT_NULL(col_item));
      CK(OB_NOT_NULL(col_item->default_value_expr_));
      OZ(def_values.push_back(col_item->default_value_expr_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < col_num; ++j) {
        OZ(arr.push_back(value_vectors_.at(i * col_num + j)));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < def_values.count(); ++j) {
        OZ(arr.push_back(def_values.at(j)));
      }
    }
  }
  return ret;
}

int ObInsertTableInfo::deep_copy(ObRawExprFactory& expr_factory, const ObInsertTableInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExprUtils::copy_exprs(
          expr_factory, other.check_constraint_exprs_, check_constraint_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.when_conds_expr_, when_conds_expr_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else {
    table_id_ = other.table_id_;
    when_conds_idx_ = other.when_conds_idx_;
  }
  return ret;
}

int ObInsertTableInfo::assign(const ObInsertTableInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_constraint_exprs_.assign(other.check_constraint_exprs_))) {
    LOG_WARN("failed to copy other.check_constraint_exprs_", K(ret));
  } else if (OB_FAIL(when_conds_expr_.assign(other.when_conds_expr_))) {
    LOG_WARN("failed to copy other.when_conds_expr_", K(ret));
  } else {
    table_id_ = other.table_id_;
    when_conds_idx_ = other.when_conds_idx_;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
