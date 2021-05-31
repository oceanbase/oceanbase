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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObMergeStmt::ObMergeStmt()
    : ObInsertStmt(),
      source_table_id_(OB_INVALID_ID),
      target_table_id_(OB_INVALID_ID),
      match_condition_exprs_(),
      insert_condition_exprs_(),
      update_condition_exprs_(),
      delete_condition_exprs_(),
      rowkey_exprs_(),
      has_insert_clause_(false),
      has_update_clause_(false)
{
  stmt_type_ = stmt::T_MERGE;
}

ObMergeStmt::~ObMergeStmt()
{}

int ObMergeStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& input)
{
  int ret = OB_SUCCESS;
  const ObMergeStmt& other = static_cast<const ObMergeStmt&>(input);
  if (OB_UNLIKELY(input.get_stmt_type() != get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObInsertStmt::deep_copy_stmt_struct(stmt_factory, expr_factory, input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.match_condition_exprs_, match_condition_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.insert_condition_exprs_, insert_condition_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.update_condition_exprs_, update_condition_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.delete_condition_exprs_, delete_condition_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.rowkey_exprs_, rowkey_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy rowkey exprs", K(ret));
  } else {
    source_table_id_ = other.source_table_id_;
    target_table_id_ = other.target_table_id_;
    has_insert_clause_ = other.has_insert_clause_;
    has_update_clause_ = other.has_update_clause_;
  }
  return ret;
}

int ObMergeStmt::assign(const ObMergeStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(match_condition_exprs_.assign(other.match_condition_exprs_))) {
    LOG_WARN("failed to assign match condition exprs", K(ret));
  } else if (OB_FAIL(insert_condition_exprs_.assign(other.insert_condition_exprs_))) {
    LOG_WARN("failed to assign insert condition exprs", K(ret));
  } else if (OB_FAIL(update_condition_exprs_.assign(other.update_condition_exprs_))) {
    LOG_WARN("failed to assign update condition exprs", K(ret));
  } else if (OB_FAIL(delete_condition_exprs_.assign(other.delete_condition_exprs_))) {
    LOG_WARN("failed to assign delete condition exprs", K(ret));
  } else if (OB_FAIL(rowkey_exprs_.assign(other.rowkey_exprs_))) {
    LOG_WARN("failed to assign rowkey exprs", K(ret));
  } else {
    source_table_id_ = other.source_table_id_;
    target_table_id_ = other.target_table_id_;
    has_insert_clause_ = other.has_insert_clause_;
    has_update_clause_ = other.has_update_clause_;
  }
  return ret;
}

int64_t ObMergeStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE,
      ((int)stmt_type_),
      N_TABLE,
      table_items_,
      "match condition",
      match_condition_exprs_,
      "update condition",
      update_condition_exprs_,
      "insert condition",
      insert_condition_exprs_,
      "delete condition",
      delete_condition_exprs_,
      "row key",
      rowkey_exprs_,
      N_VALUE,
      value_vectors_,
      N_COLUMN_CONV_FUNCTION,
      column_conv_functions_,
      N_ASSIGN,
      table_assignments_,
      K_(source_table_id),
      K_(target_table_id),
      N_PARTITION_EXPR,
      part_expr_items_,
      K_(all_table_columns),
      N_QUERY_CTX,
      stmt_hint_);
  J_OBJ_END();
  return pos;
}

int ObMergeStmt::inner_get_relation_exprs(RelExprCheckerBase& expr_checker)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_checker.add_exprs(match_condition_exprs_))) {
    LOG_WARN("failed to add exprs to expr checker", K(ret));
  } else if (OB_FAIL(expr_checker.add_exprs(update_condition_exprs_))) {
    LOG_WARN("failed to add exprs to expr checker", K(ret));
  } else if (OB_FAIL(expr_checker.add_exprs(insert_condition_exprs_))) {
    LOG_WARN("failed to add exprs to expr checker", K(ret));
  } else if (OB_FAIL(expr_checker.add_exprs(delete_condition_exprs_))) {
    LOG_WARN("failed to add exprs to expr checker", K(ret));
  } else if (OB_FAIL(ObInsertStmt::inner_get_relation_exprs(expr_checker))) {
    LOG_WARN("get insert stmt relation exprs failed", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObMergeStmt::replace_inner_stmt_expr(
    const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertStmt::replace_inner_stmt_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, match_condition_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, update_condition_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, insert_condition_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, delete_condition_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, rowkey_exprs_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObMergeStmt::add_rowkey_column_expr(ObColumnRefRawExpr* column_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_exprs_.push_back(column_expr))) {
    LOG_WARN("fail to push column expr", KPC(column_expr), K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
