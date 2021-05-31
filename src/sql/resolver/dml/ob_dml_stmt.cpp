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
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "lib/utility/utility.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/parser/parse_malloc.h"
#include "sql/ob_sql_context.h"
#include "sql/rewrite/ob_equal_analysis.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int TransposeItem::InPair::assign(const TransposeItem::InPair& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else if (OB_FAIL(exprs_.assign(other.exprs_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else if (OB_FAIL(column_names_.assign(other.column_names_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else {
    pivot_expr_alias_ = other.pivot_expr_alias_;
  }
  return ret;
}

int TransposeItem::assign(const TransposeItem& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else if (OB_FAIL(for_columns_.assign(other.for_columns_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else if (OB_FAIL(in_pairs_.assign(other.in_pairs_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else if (OB_FAIL(unpivot_columns_.assign(other.unpivot_columns_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else {
    aggr_pairs_.reset();
    old_column_count_ = other.old_column_count_;
    is_unpivot_ = other.is_unpivot_;
    is_incude_null_ = other.is_incude_null_;
    alias_name_ = other.alias_name_;
  }
  return ret;
}

int TransposeItem::deep_copy(const TransposeItem& other, ObRawExprFactory& expr_factory)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(other), K(ret));
  }

  for (int64_t i = 0; i < in_pairs_.count() && OB_SUCC(ret); ++i) {
    InPair& in_pair = in_pairs_.at(i);
    in_pair.exprs_.reuse();
    if (OB_FAIL(
            ObRawExprUtils::copy_exprs(expr_factory, other.in_pairs_.at(i).exprs_, in_pair.exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy expr failed", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObUnpivotInfo, old_column_count_, unpivot_column_count_, for_column_count_, is_include_null_);

int SemiInfo::deep_copy(ObRawExprFactory& expr_factory, const SemiInfo& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_table_ids_.assign(other.left_table_ids_))) {
    LOG_WARN("failed to assign left table ids", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.semi_conditions_, semi_conditions_, copy_types))) {
    LOG_WARN("failed to deep copy exprs", K(ret));
  } else {
    join_type_ = other.join_type_;
    semi_id_ = other.semi_id_;
    right_table_id_ = other.right_table_id_;
  }
  return ret;
}

int FromItem::deep_copy(const FromItem& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  link_table_id_ = other.link_table_id_;
  is_joined_ = other.is_joined_;
  return ret;
}

int ObAssignment::deep_copy(ObRawExprFactory& expr_factory, const ObAssignment& other)
{
  int ret = OB_SUCCESS;
  column_expr_ = other.column_expr_;
  is_duplicated_ = other.is_duplicated_;
  is_implicit_ = other.is_implicit_;
  base_table_id_ = other.base_table_id_;
  base_column_id_ = other.base_column_id_;
  if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, other.expr_, expr_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to deep copy raw expr", K(ret));
  }
  return ret;
}

int ObAssignment::assign(const ObAssignment& other)
{
  int ret = OB_SUCCESS;
  column_expr_ = other.column_expr_;
  expr_ = other.expr_;
  is_duplicated_ = other.is_duplicated_;
  is_implicit_ = other.is_implicit_;
  base_table_id_ = other.base_table_id_;
  base_column_id_ = other.base_column_id_;
  return ret;
}

int ObTableAssignment::deep_copy(ObRawExprFactory& expr_factory, const ObTableAssignment& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  is_update_part_key_ = other.is_update_part_key_;
  if (OB_FAIL(assignments_.prepare_allocate(other.assignments_.count()))) {
    LOG_WARN("failed to prepare allocate array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments_.count(); ++i) {
    if (OB_FAIL(assignments_.at(i).deep_copy(expr_factory, other.assignments_.at(i)))) {
      LOG_WARN("failed to deep copy assignments", K(ret));
    }
  }
  return ret;
}

int ObTableAssignment::assign(const ObTableAssignment& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assignments_.assign(other.assignments_))) {
    LOG_WARN("failed to assign assignments", K(ret));
  } else {
    table_id_ = other.table_id_;
    is_update_part_key_ = other.is_update_part_key_;
  }
  return ret;
}

int ObTableAssignment::expand_expr(ObAssignments& assigns, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (NULL == expr) {
    LOG_WARN("invalid argument", K(expr));
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret && expr->has_flag(CNT_COLUMN)) {
    if (expr->has_flag(IS_VALUES)) {
      // nothing to do
    } else if (expr->has_flag(IS_COLUMN)) {
      if (OB_FAIL(replace_assigment_expr(assigns, expr))) {
        LOG_WARN("fail to replace assigment expr", K(ret), K(expr));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        if (OB_FAIL(SMART_CALL(ObTableAssignment::expand_expr(assigns, expr->get_param_expr(i))))) {
          LOG_WARN("fail to postorder_spread", K(ret), K(expr->get_param_expr(i)));
        }
      }
    }
  }
  return ret;
}

int ObTableAssignment::replace_assigment_expr(ObAssignments& assigns, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (expr == NULL) {
    LOG_WARN("invalid argument", K(expr));
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret)) {
    if (!expr->has_flag(IS_COLUMN)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expr", K(expr));
    } else {
      // eg:update test set c1 = c1+c2, c2=c1+3
      // c1 = c1+c2, c2=c1+c2+3
      for (int64_t i = 0; i < assigns.count(); i++) {
        if (assigns.at(i).column_expr_ == expr) {
          expr = assigns.at(i).expr_;
          break;
        }
      }
    }
  }
  return ret;
}

bool JoinedTable::same_as(const JoinedTable& other) const
{
  bool bret = true;
  if (TableItem::JOINED_TABLE != type_ || TableItem::JOINED_TABLE != other.type_ || NULL == left_table_ ||
      NULL == right_table_ || NULL == other.left_table_ || NULL == other.right_table_) {
    bret = false;
  } else {
    if (table_id_ != other.table_id_ || joined_type_ != other.joined_type_ ||
        join_conditions_.count() != other.join_conditions_.count()) {
      bret = false;
    } else if (left_table_->type_ != other.left_table_->type_ || right_table_->type_ != other.right_table_->type_) {
      bret = false;
    } else if (TableItem::JOINED_TABLE == left_table_->type_) {
      const JoinedTable* left_table = static_cast<JoinedTable*>(left_table_);
      const JoinedTable* other_left_table = static_cast<JoinedTable*>(other.left_table_);
      bret = left_table->same_as(*other_left_table);
    } else {
      if (left_table_->table_id_ != other.left_table_->table_id_) {
        bret = false;
      }
    }
  }
  if (true == bret) {
    if (TableItem::JOINED_TABLE == right_table_->type_) {
      const JoinedTable* right_table = static_cast<JoinedTable*>(right_table_);
      const JoinedTable* other_right_table = static_cast<JoinedTable*>(other.right_table_);
      bret = right_table->same_as(*other_right_table);
      bret = right_table->same_as(*other_right_table);
    } else if (right_table_->table_id_ != other.right_table_->table_id_) {
      bret = false;
    }
  }
  if (true == bret) {
    for (int64_t i = 0; bret && i < join_conditions_.count(); ++i) {
      const ObRawExpr* join_condition = join_conditions_.at(i);
      const ObRawExpr* other_join_condition = other.join_conditions_.at(i);
      if (NULL == join_condition || NULL == other_join_condition) {
        bret = false;
      } else {
        bret = join_condition->same_as(*other_join_condition);
      }
    }
  }
  return bret;
}

int ColumnItem::deep_copy(ObRawExprFactory& expr_factory, const ColumnItem& other)
{
  int ret = OB_SUCCESS;
  column_id_ = other.column_id_;
  table_id_ = other.table_id_;
  column_name_ = other.column_name_;
  default_value_ = other.default_value_;
  auto_filled_timestamp_ = other.auto_filled_timestamp_;
  base_tid_ = other.base_tid_;
  base_cid_ = other.base_cid_;
  ObRawExpr* temp_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, other.expr_, temp_expr, COPY_REF_SHARED))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_ISNULL(temp_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else if (OB_UNLIKELY(ObRawExpr::EXPR_COLUMN_REF != temp_expr->get_expr_class())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr class", K(temp_expr->get_expr_class()), K(ret));
  } else {
    expr_ = static_cast<ObColumnRefRawExpr*>(temp_expr);
  }

  if (OB_SUCC(ret)) {
    temp_expr = NULL;
    if (NULL == other.default_value_expr_) {
      default_value_expr_ = NULL;
    } else if (OB_FAIL(
                   ObRawExprUtils::copy_expr(expr_factory, other.default_value_expr_, temp_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_ISNULL(temp_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else {
      default_value_expr_ = temp_expr;
    }
  }
  return ret;
}

int TableItem::deep_copy(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const TableItem& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  table_name_ = other.table_name_;
  alias_name_ = other.alias_name_;
  synonym_name_ = other.synonym_name_;
  synonym_db_name_ = other.synonym_db_name_;
  type_ = other.type_;
  ref_id_ = other.ref_id_;
  is_system_table_ = other.is_system_table_;
  is_index_table_ = other.is_index_table_;
  is_view_table_ = other.is_view_table_;
  is_recursive_union_fake_table_ = other.is_recursive_union_fake_table_;
  cte_type_ = other.cte_type_;
  is_materialized_view_ = other.is_materialized_view_;
  database_name_ = other.database_name_;
  for_update_ = other.for_update_;
  for_update_wait_us_ = other.for_update_wait_us_;
  mock_id_ = other.mock_id_;
  node_ = other.node_;  // should deep copy ? seems to be unnecessary
  // dblink
  dblink_name_ = other.dblink_name_;
  link_database_name_ = other.link_database_name_;
  if (OB_FAIL(ObRawExprUtils::copy_expr(
          expr_factory, other.function_table_expr_, function_table_expr_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to deep copy raw expr", K(ret));
  } else if (NULL != other.ref_query_) {
    ObSelectStmt* temp_ref_query = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(temp_ref_query))) {
      LOG_WARN("failed to create select stmt", K(ret));
    } else if (OB_ISNULL(temp_ref_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(temp_ref_query->deep_copy(stmt_factory, expr_factory, *other.ref_query_))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else {
      ref_query_ = temp_ref_query;
    }
  }
  return ret;
}

int JoinedTable::deep_copy(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const JoinedTable& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.left_table_) || OB_ISNULL(other.right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null table item", K(other.left_table_), K(other.right_table_), K(ret));
  } else if (OB_FAIL(TableItem::deep_copy(stmt_factory, expr_factory, other))) {
    LOG_WARN("deep copy table item failed", K(ret));
  } else if (OB_FAIL(single_table_ids_.assign(other.single_table_ids_))) {
    LOG_WARN("failed to assign single table ids", K(ret));
  } else if (OB_FAIL(using_columns_.assign(other.using_columns_))) {
    LOG_WARN("failed to assign using columns", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.join_conditions_, join_conditions_, copy_types))) {
    LOG_WARN("failed to deep copy exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.coalesce_expr_, coalesce_expr_, copy_types))) {
    LOG_WARN("failed to deep copy exprs", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      if (other.left_table_->is_joined_table()) {
        JoinedTable* temp_left_table_item = NULL;
        if (OB_FAIL(ObTransformUtils::deep_copy_join_table(stmt_factory,
                expr_factory,
                static_cast<JoinedTable&>(*other.left_table_),
                temp_left_table_item,
                copy_types))) {
          LOG_WARN("failed to deep copy join table", K(ret));
        } else {
          left_table_ = temp_left_table_item;
        }
      } else {
        left_table_ = other.left_table_;
      }
    }
    if (OB_SUCC(ret)) {
      if (other.right_table_->is_joined_table()) {
        JoinedTable* temp_right_table_item = NULL;
        if (OB_FAIL(ObTransformUtils::deep_copy_join_table(stmt_factory,
                expr_factory,
                static_cast<JoinedTable&>(*other.right_table_),
                temp_right_table_item,
                copy_types))) {
          LOG_WARN("failed to deep copy join table", K(ret));
        } else {
          right_table_ = temp_right_table_item;
        }
      } else {
        right_table_ = other.right_table_;
      }
    }
    if (OB_SUCC(ret)) {
      joined_type_ = other.joined_type_;
    }
  }
  return ret;
}

int ObDMLStmt::PartExprItem::deep_copy(
    ObRawExprFactory& expr_factory, const PartExprItem& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  ObRawExpr* temp_part_expr = NULL;
  ObRawExpr* temp_subpart_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, other.part_expr_, temp_part_expr, copy_types))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, other.subpart_expr_, temp_subpart_expr, copy_types))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else {
    table_id_ = other.table_id_;
    index_tid_ = other.index_tid_;
    part_expr_ = temp_part_expr;
    subpart_expr_ = temp_subpart_expr;
  }
  return ret;
}

int ObDMLStmt::PartExprArray::deep_copy(
    ObRawExprFactory& expr_factory, const PartExprArray& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.part_expr_array_, part_expr_array_, copy_types))) {
    LOG_WARN("failed to copy part expr array", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.subpart_expr_array_, subpart_expr_array_, copy_types))) {
    LOG_WARN("failed to copy subpart expr array", K(ret));
  } else {
    table_id_ = other.table_id_;
    index_tid_ = other.index_tid_;
  }
  return ret;
}

ObDMLStmt::ObDMLStmt(stmt::StmtType type)
    : ObStmt(type),
      order_items_(),
      limit_count_expr_(NULL),
      limit_offset_expr_(NULL),
      limit_percent_expr_(NULL),
      has_fetch_(false),
      is_fetch_with_ties_(false),
      from_items_(),
      part_expr_items_(),
      joined_tables_(),
      stmt_hint_(),
      semi_infos_(),
      has_subquery_(false),
      view_table_id_store_(),
      autoinc_params_(),
      is_calc_found_rows_(false),
      has_top_limit_(false),
      is_contains_assignment_(false),
      affected_last_insert_id_(false),
      has_part_key_sequence_(false),
      table_items_(),
      column_items_(),
      condition_exprs_(),
      deduced_exprs_(),
      pseudo_column_like_exprs_(),
      tables_hash_(),
      parent_namespace_stmt_(NULL),
      current_level_(0),
      subquery_exprs_(),
      is_hierarchical_query_(false),
      is_order_siblings_(false),
      has_prior_(false),
      transpose_item_(NULL),
      check_constraint_exprs_(),
      user_var_exprs_(),
      has_is_table_(false),
      eliminated_(false)
{}

ObDMLStmt::~ObDMLStmt()
{}

int ObDMLStmt::remove_from_item(uint64_t tid)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < from_items_.count(); i++) {
    if (tid == from_items_.at(i).table_id_) {
      if (OB_FAIL(from_items_.remove(i))) {
        LOG_WARN("failed to remove from_items", K(ret));
      }
      break;
    }
  }
  return ret;
}

// ref_query in ObStmt only do pointer copy
int ObDMLStmt::assign(const ObDMLStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(table_items_.assign(other.table_items_))) {
    LOG_WARN("assign table items failed", K(ret));
  } else if (OB_FAIL(CTE_table_items_.assign(other.CTE_table_items_))) {
    LOG_WARN("assign cte table items failed", K(ret));
  } else if (OB_FAIL(tables_hash_.assign(other.tables_hash_))) {
    LOG_WARN("assign table hash desc failed", K(ret));
  } else if (OB_FAIL(column_items_.assign(other.column_items_))) {
    LOG_WARN("assign column items failed", K(ret));
  } else if (OB_FAIL(condition_exprs_.assign(other.condition_exprs_))) {
    LOG_WARN("assign condition exprs failed", K(ret));
  } else if (OB_FAIL(deduced_exprs_.assign(other.deduced_exprs_))) {
    LOG_WARN("assign deduced exprs failed", K(ret));
  } else if (OB_FAIL(order_items_.assign(other.order_items_))) {
    LOG_WARN("assign order items failed", K(ret));
  } else if (OB_FAIL(from_items_.assign(other.from_items_))) {
    LOG_WARN("assign from items failed", K(ret));
  } else if (OB_FAIL(part_expr_items_.assign(other.part_expr_items_))) {
    LOG_WARN("assign part expr items failed", K(ret));
  } else if (OB_FAIL(joined_tables_.assign(other.joined_tables_))) {
    LOG_WARN("assign joined tables failed", K(ret));
  } else if (OB_FAIL(semi_infos_.assign(other.semi_infos_))) {
    LOG_WARN("assign semi infos failed", K(ret));
  } else if (OB_FAIL(stmt_hint_.assign(other.stmt_hint_))) {
    LOG_WARN("assign stmt hint failed", K(ret));
  } else if (OB_FAIL(subquery_exprs_.assign(other.subquery_exprs_))) {
    LOG_WARN("assign subquery exprs failed", K(ret));
  } else if (OB_FAIL(pseudo_column_like_exprs_.assign(other.pseudo_column_like_exprs_))) {
    LOG_WARN("assgin pseudo column exprs fail", K(ret));
  } else if (OB_FAIL(view_table_id_store_.assign(other.view_table_id_store_))) {
    LOG_WARN("assign view table id fail", K(ret));
  } else if (OB_FAIL(autoinc_params_.assign(other.autoinc_params_))) {
    LOG_WARN("assign autoinc params fail", K(ret));
  } else if (OB_FAIL(nextval_sequence_ids_.assign(other.nextval_sequence_ids_))) {
    LOG_WARN("failed to assign sequence ids", K(ret));
  } else if (OB_FAIL(currval_sequence_ids_.assign(other.currval_sequence_ids_))) {
    LOG_WARN("failed to assign sequence ids", K(ret));
  } else if (OB_FAIL(user_var_exprs_.assign(other.user_var_exprs_))) {
    LOG_WARN("assign user var exprs fail", K(ret));
  } else {
    parent_namespace_stmt_ = other.parent_namespace_stmt_;
    limit_count_expr_ = other.limit_count_expr_;
    limit_offset_expr_ = other.limit_offset_expr_;
    limit_percent_expr_ = other.limit_percent_expr_;
    has_fetch_ = other.has_fetch_;
    is_fetch_with_ties_ = other.is_fetch_with_ties_;
    has_subquery_ = other.has_subquery_;
    is_hierarchical_query_ = other.is_hierarchical_query_;
    has_prior_ = other.has_prior_;
    is_order_siblings_ = other.is_order_siblings_;
    current_level_ = other.current_level_;
    has_is_table_ = other.has_is_table_;
    eliminated_ = other.eliminated_;
    is_calc_found_rows_ = other.is_calc_found_rows_;
    has_top_limit_ = other.has_top_limit_;
    is_contains_assignment_ = other.is_contains_assignment_;
    affected_last_insert_id_ = other.affected_last_insert_id_;
    has_part_key_sequence_ = other.has_part_key_sequence_;
    transpose_item_ = other.transpose_item_;
  }
  return ret;
}

int ObDMLStmt::deep_copy(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_share_exprs;
  ObSEArray<ObRawExpr*, 4> old_share_exprs;
  if (OB_UNLIKELY(get_stmt_type() != other.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret), K(get_stmt_type()), K(other.get_stmt_type()));
  } else if (OB_FAIL(other.mark_share_exprs())) {
    LOG_WARN("failed to mark share exprs", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_struct(stmt_factory, expr_factory, other))) {
    LOG_WARN("failed to deep copy members", K(ret));
  } else if (OB_FAIL(deep_copy_share_exprs(expr_factory, other, old_share_exprs, new_share_exprs))) {
    LOG_WARN("failed to copy share exprs", K(ret));
  } else if (OB_FAIL(replace_inner_stmt_expr(old_share_exprs, new_share_exprs))) {
    LOG_WARN("failed to replace stmt expr", K(ret));
  } else if (OB_FAIL(adjust_subquery_stmt_parent(&other, this))) {
    LOG_WARN("failed to adjust subquery stmt parent", K(ret));
  } else if (OB_FAIL(adjust_statement_id())) {
    LOG_WARN("failed to adjust statement id", K(ret));
  }
  return ret;
}

int ObDMLStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStmt::deep_copy(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_table_items(
                 stmt_factory, expr_factory, other.table_items_, table_items_))) {
    LOG_WARN("deep copy table items failed", K(ret));
  } else if (OB_FAIL(CTE_table_items_.assign(other.CTE_table_items_))) {
    LOG_WARN("assign cte table items failed", K(ret));
  } else if (OB_FAIL(tables_hash_.assign(other.tables_hash_))) {
    LOG_WARN("assign table hash desc failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_join_tables(
                 stmt_factory, expr_factory, other.joined_tables_, joined_tables_, COPY_REF_DEFAULT))) {
    LOG_WARN("assign joined tables failed", K(ret));
  } else if (OB_FAIL(construct_join_tables(other))) {
    LOG_WARN("construct join table failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_semi_infos(
                 expr_factory, other.semi_infos_, semi_infos_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy semi infos failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_column_items(expr_factory, other.column_items_, column_items_))) {
    LOG_WARN("deep copy column items failed", K(ret));
  } else if (OB_FAIL(
                 ObRawExprUtils::copy_exprs(expr_factory, other.subquery_exprs_, subquery_exprs_, COPY_REF_SHARED))) {
    LOG_WARN("failed to copy subquery exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.pseudo_column_like_exprs_, pseudo_column_like_exprs_, COPY_REF_SHARED))) {
    LOG_WARN("failed to copy pseudo column exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.condition_exprs_, condition_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy condition exprs failed", K(ret));
  } else if (OB_FAIL(
                 ObRawExprUtils::copy_exprs(expr_factory, other.deduced_exprs_, deduced_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy deduced exprs failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_part_expr_items(
                 expr_factory, other.part_expr_items_, part_expr_items_, COPY_REF_DEFAULT))) {
    LOG_WARN("assign part expr items failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_related_part_expr_arrays(
                 expr_factory, other.related_part_expr_arrays_, related_part_expr_arrays_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to deep copy related part expr arrays", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 expr_factory, other.limit_count_expr_, limit_count_expr_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy limit count expr failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 expr_factory, other.limit_offset_expr_, limit_offset_expr_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy limit offset expr failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 expr_factory, other.limit_percent_expr_, limit_percent_expr_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy limit percent expr failed", K(ret));
  } else if (OB_FAIL(from_items_.assign(other.from_items_))) {
    LOG_WARN("assign from items failed", K(ret));
  } else if (OB_FAIL(stmt_hint_.assign(other.stmt_hint_))) {
    LOG_WARN("assign stmt hint failed", K(ret));
  } else if (OB_FAIL(view_table_id_store_.assign(other.view_table_id_store_))) {
    LOG_WARN("assign view table id failed", K(ret));
  } else if (OB_FAIL(autoinc_params_.assign(other.autoinc_params_))) {
    LOG_WARN("assign autoinc params failed", K(ret));
  } else if (OB_FAIL(nextval_sequence_ids_.assign(other.nextval_sequence_ids_))) {
    LOG_WARN("failed to assign sequence ids", K(ret));
  } else if (OB_FAIL(currval_sequence_ids_.assign(other.currval_sequence_ids_))) {
    LOG_WARN("failed to assign sequence ids", K(ret));
  } else {
    if (NULL != other.transpose_item_) {
      void* ptr = NULL;
      TransposeItem* tmp_item = NULL;
      if (OB_ISNULL(ptr = stmt_factory.get_allocator().alloc(sizeof(TransposeItem)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no more memory to create TransposeItem");
      } else {
        tmp_item = new (ptr) TransposeItem();
        if (OB_FAIL(tmp_item->deep_copy(*other.transpose_item_, expr_factory))) {
          LOG_WARN("failed to deep copy transpose_item_", K(ret));
        } else {
          transpose_item_ = tmp_item;
        }
      }
    } else {
      transpose_item_ = NULL;
    }
    parent_namespace_stmt_ = other.parent_namespace_stmt_;
    has_subquery_ = other.has_subquery_;
    is_hierarchical_query_ = other.is_hierarchical_query_;
    has_prior_ = other.has_prior_;
    is_order_siblings_ = other.is_order_siblings_;
    current_level_ = other.current_level_;
    has_is_table_ = other.has_is_table_;
    eliminated_ = other.eliminated_;
    is_calc_found_rows_ = other.is_calc_found_rows_;
    has_top_limit_ = other.has_top_limit_;
    is_contains_assignment_ = other.is_contains_assignment_;
    affected_last_insert_id_ = other.affected_last_insert_id_;
    has_part_key_sequence_ = other.has_part_key_sequence_;
    has_fetch_ = other.has_fetch_;
    is_fetch_with_ties_ = other.is_fetch_with_ties_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); ++i) {
    ObDMLStmt* new_stmt = NULL;
    if (OB_ISNULL(subquery_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(
                   stmt_factory, expr_factory, subquery_exprs_.at(i)->get_ref_stmt(), new_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else {
      subquery_exprs_.at(i)->set_ref_stmt(static_cast<ObSelectStmt*>(new_stmt));
    }
  }
  return ret;
}

int ObDMLStmt::deep_copy_share_exprs(ObRawExprFactory& expr_factory, const ObDMLStmt& other_stmt,
    ObIArray<ObRawExpr*>& old_share_exprs, ObIArray<ObRawExpr*>& new_share_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  if (OB_FAIL(other_stmt.inner_get_share_exprs(old_exprs))) {
    LOG_WARN("failed to get share exprs", K(ret));
  } else if (OB_FAIL(inner_get_share_exprs(new_exprs))) {
    LOG_WARN("failed to get share exprs", K(ret));
  } else if (OB_UNLIKELY(old_exprs.count() != new_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("share exprs numbers are expected to be the same",
        K(ret),
        K(old_exprs.count()),
        K(new_exprs.count()),
        K(old_exprs),
        K(new_exprs));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_exprs.count(); ++i) {
    ObRawExpr* new_share_expr = NULL;
    if (!ObOptimizerUtil::find_item(old_share_exprs, old_exprs.at(i))) {
      // find an old expr, which is not copied yet
      if (new_exprs.at(i) != old_exprs.at(i)) {
        new_share_expr = new_exprs.at(i);
      } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, old_exprs.at(i), new_share_expr, COPY_REF_SHARED))) {
        LOG_WARN("failed to copy shared expr", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(old_share_exprs.push_back(old_exprs.at(i)))) {
          LOG_WARN("failed to push back old expr", K(ret));
        } else if (OB_FAIL(new_share_exprs.push_back(new_share_expr))) {
          LOG_WARN("failed to push back new share exprs", K(ret));
        }
      }
    }
  }
  // clear shared ref flag, after the deep copy is finished
  for (int64_t i = 0; OB_SUCC(ret) && i < old_share_exprs.count(); ++i) {
    if (OB_ISNULL(old_share_exprs.at(i)) || OB_ISNULL(new_share_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs are null", K(ret));
    } else if (OB_FAIL(old_share_exprs.at(i)->clear_flag(IS_SHARED_REF))) {
      LOG_WARN("failed to clear shared flag", K(ret));
    } else if (OB_FAIL(new_share_exprs.at(i)->clear_flag(IS_SHARED_REF))) {
      LOG_WARN("failed to clear shared flag", K(ret));
    }
  }
  /* consider here
    if (OB_UNLIKELY(transpose_item_->in_pairs_.count()
                    != other.transpose_item_->in_pairs_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr numbers do not match", K(ret));
    }
    for (int64_t i = 0; i < transpose_item_->in_pairs_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(append(other_exprs, other.transpose_item_->in_pairs_.at(i).exprs_))
          || OB_FAIL(append(new_exprs, transpose_item_->in_pairs_.at(i).exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    }
    */
  return ret;
}

int ObDMLStmt::get_child_table_id_recurseive(common::ObIArray<share::schema::ObObjectStruct>& object_ids,
    const int64_t object_limit_count /*OB_MAX_TABLE_NUM_PER_STMT*/) const
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  }

  bool is_finish = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_finish && i < get_table_size(); ++i) {
    const sql::TableItem* tmp_table = NULL;
    if (OB_ISNULL(tmp_table = get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (tmp_table->is_basic_table()) {
      ObObjectStruct tmp_struct(share::schema::ObObjectType::TABLE, tmp_table->ref_id_);
      if (OB_FAIL(object_ids.push_back(tmp_struct))) {
        LOG_WARN("failed to push_back tmp_table->ref_id_", KPC(tmp_table), K(ret));
      } else if (OB_UNLIKELY(object_ids.count() >= object_limit_count)) {
        is_finish = true;
        LOG_DEBUG("arrieve limit count", KPC(this), K(object_limit_count));
      } else {
        LOG_DEBUG("succ push object_ids", KPC(tmp_table));
      }
    }
  }

  // try complex table
  if (OB_SUCC(ret) && !is_finish && get_table_size() != object_ids.count()) {
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        const ObSelectStmt* sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Sub-stmt is NULL", K(ret));
        } else if (OB_FAIL(SMART_CALL(sub_stmt->get_child_table_id_recurseive(object_ids, object_limit_count)))) {
          LOG_WARN("failed to get_child_table_id_with_cte_", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_child_table_id_count_recurseive(
    int64_t& object_ids_cnt, const int64_t object_limit_count /*OB_MAX_TABLE_NUM_PER_STMT*/) const
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  }

  bool is_finish = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_finish && i < get_table_size(); ++i) {
    const sql::TableItem* tmp_table = NULL;
    if (OB_ISNULL(tmp_table = get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (tmp_table->is_basic_table()) {
      if (OB_UNLIKELY(++object_ids_cnt >= object_limit_count)) {
        is_finish = true;
        LOG_DEBUG("arrieve limit count", KPC(tmp_table), K(object_limit_count), K(object_ids_cnt));
      }
    }
  }

  // try complex table
  if (OB_SUCC(ret) && !is_finish && get_table_size() != object_ids_cnt) {
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        const ObSelectStmt* sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Sub-stmt is NULL", K(ret));
        } else if (OB_FAIL(
                       SMART_CALL(sub_stmt->get_child_table_id_count_recurseive(object_ids_cnt, object_limit_count)))) {
          LOG_WARN("failed to get_child_table_id_count_recurseive", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::extract_column_expr(
    const ObIArray<ColumnItem>& column_items, ObIArray<ObColumnRefRawExpr*>& column_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); i++) {
    if (OB_ISNULL(column_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column expr", K(ret));
    } else if (OB_FAIL(column_exprs.push_back(column_items.at(i).expr_))) {
      LOG_WARN("failed to push back column expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLStmt::replace_inner_stmt_expr(const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  // replace order items
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); i++) {
    if (OB_ISNULL(order_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, order_items_.at(i).expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else { /*do nothing*/
    }
  }
  // replace table items
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
    if (OB_ISNULL(table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (NULL != table_items_.at(i)->function_table_expr_ &&
               OB_FAIL(
                   ObTransformUtils::replace_expr(other_exprs, new_exprs, table_items_.at(i)->function_table_expr_))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else { /*do nothing*/
    }
  }
  // replace join table items
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables_.count(); i++) {
    if (OB_ISNULL(joined_tables_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null join tables", K(ret));
    } else if (OB_FAIL(replace_expr_for_joined_table(other_exprs, new_exprs, *joined_tables_.at(i)))) {
      LOG_WARN("failed to replace join condition exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  // replace semi info
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos_.count(); i++) {
    if (OB_ISNULL(semi_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null semi info", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, semi_infos_.at(i)->semi_conditions_))) {
      LOG_WARN("failed to replace semi conditions exprs", K(ret));
    }
  }
  // replace other exprs
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, condition_exprs_))) {
      LOG_WARN("failed to replace condition exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, deduced_exprs_))) {
      LOG_WARN("failed to replace deduced exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, limit_count_expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, limit_offset_expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, limit_percent_expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, check_constraint_exprs_))) {
      LOG_WARN("failed to replace check constraint exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  // replace part expr items
  for (int64_t i = 0; OB_SUCC(ret) && i < part_expr_items_.count(); i++) {
    if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, part_expr_items_.at(i).part_expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, part_expr_items_.at(i).subpart_expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else { /*do nothing*/
    }
  }
  // repalce related part expr array
  for (int64_t i = 0; OB_SUCC(ret) && i < related_part_expr_arrays_.count(); ++i) {
    PartExprArray& part_array = related_part_expr_arrays_.at(i);
    if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, part_array.part_expr_array_))) {
      LOG_WARN("failed to repalce part expr array", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, part_array.subpart_expr_array_))) {
      LOG_WARN("failed to repalce part expr array", K(ret));
    }
  }

  // replace dependent expr
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    if (OB_ISNULL(column_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column expr", K(ret));
    } else if (NULL != column_items_.at(i).expr_->get_dependant_expr()) {
      ObRawExpr* temp_expr = column_items_.at(i).expr_->get_dependant_expr();
      if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, temp_expr))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else {
        column_items_.at(i).expr_->set_dependant_expr(temp_expr);
      }
    }

    if (OB_SUCC(ret) && NULL != column_items_.at(i).default_value_expr_) {
      ObRawExpr* temp_expr = column_items_.at(i).default_value_expr_;
      if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, temp_expr))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else {
        column_items_.at(i).default_value_expr_ = temp_expr;
      }
    } else { /*do nothing*/
    }
  }

  if (NULL != transpose_item_) {
    for (int64_t i = 0; i < transpose_item_->in_pairs_.count() && OB_SUCC(ret); ++i) {
      TransposeItem::InPair& in_pair = const_cast<TransposeItem::InPair&>(transpose_item_->in_pairs_.at(i));
      for (int64_t j = 0; j < in_pair.exprs_.count() && OB_SUCC(ret); ++j) {
        if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, in_pair.exprs_.at(j)))) {
          LOG_WARN("failed to replace expr", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObSelectStmt*, 4> subqueries;
    if (OB_FAIL(get_child_stmts(subqueries))) {
      LOG_WARN("failed to get subqueries", K(ret));
    }
    // replace exprs in child stmt
    for (int64_t i = 0; OB_SUCC(ret) && i < subqueries.count(); i++) {
      if (OB_ISNULL(subqueries.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(subqueries.at(i)->replace_inner_stmt_expr(other_exprs, new_exprs)))) {
        LOG_WARN("failed to repalce expr in child stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::replace_expr_for_joined_table(
    const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs, JoinedTable& joined_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(joined_table.left_table_) || OB_ISNULL(joined_table.right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table item", K(ret), K(joined_table.left_table_), K(joined_table.right_table_));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, joined_table.join_conditions_))) {
    LOG_WARN("failed to replace join condition exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, joined_table.coalesce_expr_))) {
    LOG_WARN("failed to replace coalesce exprs", K(ret));
  } else if (joined_table.left_table_->is_joined_table() &&
             OB_FAIL(SMART_CALL(replace_expr_for_joined_table(
                 other_exprs, new_exprs, static_cast<JoinedTable&>(*joined_table.left_table_))))) {
    LOG_WARN("failed to replace expr for joined table", K(ret));
  } else if (joined_table.right_table_->is_joined_table() &&
             OB_FAIL(SMART_CALL(replace_expr_for_joined_table(
                 other_exprs, new_exprs, static_cast<JoinedTable&>(*joined_table.right_table_))))) {
    LOG_WARN("failed to replace expr for joined table", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObDMLStmt::construct_join_tables(const ObDMLStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(joined_tables_.count() != other.joined_tables_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item count should be the same", K(joined_tables_.count()), K(other.joined_tables_.count()));
  } else {
    const ObIArray<JoinedTable*>& other_joined_tables = other.get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < other_joined_tables.count(); ++i) {
      if (OB_ISNULL(other_joined_tables.at(i)) || OB_ISNULL(joined_tables_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("joined table is null", K(ret), K(other_joined_tables.at(i)), K(joined_tables_.at(i)));
      } else if (OB_FAIL(construct_join_table(other, *other_joined_tables.at(i), *joined_tables_.at(i)))) {
        LOG_WARN("failed to replace joined_table item", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObDMLStmt::construct_join_table(const ObDMLStmt& other_stmt, const JoinedTable& other, JoinedTable& current)
{
  int ret = OB_SUCCESS;
  int64_t idx_left = -1;
  int64_t idx_right = -1;
  if (OB_UNLIKELY(table_items_.count() != other_stmt.table_items_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "table item count should be the same", K(table_items_.count()), K(other_stmt.table_items_.count()), K(ret));
  } else if (OB_ISNULL(other.left_table_) || OB_ISNULL(other.right_table_) || OB_ISNULL(current.left_table_) ||
             OB_ISNULL(current.right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null table item",
        K(other.left_table_),
        K(other.right_table_),
        K(current.left_table_),
        K(current.right_table_),
        K(ret));
  } else if (OB_UNLIKELY(other.left_table_->type_ != current.left_table_->type_) ||
             OB_UNLIKELY(other.right_table_->type_ != current.right_table_->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have the same table item type",
        K(other.left_table_->type_),
        K(other.right_table_->type_),
        K(current.left_table_->type_),
        K(current.right_table_->type_),
        K(ret));
  } else { /*do nothing*/
  }
  // replace left table item
  if (OB_SUCC(ret)) {
    if (other.left_table_->is_joined_table()) {
      if (OB_FAIL(SMART_CALL(construct_join_table(other_stmt,
              static_cast<JoinedTable&>(*other.left_table_),
              static_cast<JoinedTable&>(*current.left_table_))))) {
        LOG_WARN("failed to replace joined table item", K(ret));
      } else { /*do nothing*/
      }
    } else {
      if (OB_FAIL(other_stmt.get_table_item_idx(other.left_table_, idx_left)) || (-1 == idx_left)) {
        LOG_WARN("failed to get table item", K(ret), K(idx_left));
      } else {
        current.left_table_ = table_items_.at(idx_left);
      }
    }
  }
  // replace right table item
  if (OB_SUCC(ret)) {
    if (other.right_table_->is_joined_table()) {
      if (OB_FAIL(construct_join_table(other_stmt,
              static_cast<JoinedTable&>(*other.right_table_),
              static_cast<JoinedTable&>(*current.right_table_)))) {
        LOG_WARN("failed to replace joined table", K(ret));
      } else { /*do nothing*/
      }
    } else {
      if (OB_FAIL(other_stmt.get_table_item_idx(other.right_table_, idx_right)) || (-1 == idx_right)) {
        LOG_WARN("failed to get table item", K(idx_right), K(ret));
      } else {
        current.right_table_ = table_items_.at(idx_right);
      }
    }
  }
  return ret;
}

/*
 * for or-expansion transformation
 */
int ObDMLStmt::update_stmt_table_id(const ObDMLStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other.table_items_.count() != table_items_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal table item count", K(table_items_.count()), K(other.table_items_.count()), K(ret));
  } else if (OB_UNLIKELY(other.joined_tables_.count() != joined_tables_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "should have equal joined table count", K(joined_tables_.count()), K(other.joined_tables_.count()), K(ret));
  } else if (OB_UNLIKELY(subquery_exprs_.count() != other.subquery_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal subquery exprs", K(subquery_exprs_.count()), K(other.subquery_exprs_.count()), K(ret));
  } else { /*do nothing*/
  }
  // recursively update table id for child statements
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
    if (OB_ISNULL(table_items_.at(i)) || OB_ISNULL(other.table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(table_items_.at(i)), K(other.table_items_.at(i)), K(ret));
    } else if (table_items_.at(i)->is_generated_table() && other.table_items_.at(i)->is_generated_table() &&
               NULL != table_items_.at(i)->ref_query_ && NULL != other.table_items_.at(i)->ref_query_ &&
               OB_FAIL(table_items_.at(i)->ref_query_->update_stmt_table_id(*other.table_items_.at(i)->ref_query_))) {
      LOG_WARN("failed to update table id for generated table", K(ret));
    } else { /*do nothing*/
    }
  }
  // recursively update table id for subquery exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); i++) {
    if (OB_ISNULL(subquery_exprs_.at(i)) || OB_ISNULL(subquery_exprs_.at(i)->get_ref_stmt()) ||
        OB_ISNULL(other.subquery_exprs_.at(i)) || OB_ISNULL(other.subquery_exprs_.at(i)->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null point error",
          K(subquery_exprs_.at(i)),
          K(other.subquery_exprs_.at(i)),
          K(subquery_exprs_.at(i)->get_ref_stmt()),
          K(other.subquery_exprs_.at(i)->get_ref_stmt()),
          K(ret));
    } else if (OB_FAIL(subquery_exprs_.at(i)->get_ref_stmt()->update_stmt_table_id(
                   *other.subquery_exprs_.at(i)->get_ref_stmt()))) {
      LOG_WARN("failed to update table id for subquery exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  // reset tables hash
  if (OB_SUCC(ret)) {
    tables_hash_.reset();
    if (OB_FAIL(set_table_bit_index(common::OB_INVALID_ID))) {
      LOG_WARN("failed to set table bit index", K(ret));
    } else { /*do nothing*/
    }
  }
  // reset table id from column items
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    if (OB_ISNULL(column_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column expr", K(ret));
    } else {
      column_items_.at(i).expr_->get_relation_ids().reset();
    }
  }
  // reset table id for stmt hint
  if (OB_SUCC(ret)) {
    stmt_hint_.use_merge_idxs_.reset();
    stmt_hint_.no_use_merge_idxs_.reset();
    stmt_hint_.use_hash_idxs_.reset();
    stmt_hint_.no_use_hash_idxs_.reset();
    stmt_hint_.use_nl_idxs_.reset();
    stmt_hint_.no_use_nl_idxs_.reset();
    stmt_hint_.use_bnl_idxs_.reset();
    stmt_hint_.no_use_bnl_idxs_.reset();
    stmt_hint_.pq_distributes_idxs_.reset();
    stmt_hint_.valid_use_merge_idxs_.reset();
    stmt_hint_.valid_no_use_merge_idxs_.reset();
    stmt_hint_.valid_use_hash_idxs_.reset();
    stmt_hint_.valid_no_use_hash_idxs_.reset();
    stmt_hint_.valid_use_nl_idxs_.reset();
    stmt_hint_.valid_no_use_nl_idxs_.reset();
    stmt_hint_.valid_use_bnl_idxs_.reset();
    stmt_hint_.valid_no_use_bnl_idxs_.reset();
    stmt_hint_.valid_use_nl_materialization_idxs_.reset();
    stmt_hint_.valid_no_use_nl_materialization_idxs_.reset();
    stmt_hint_.valid_pq_distributes_idxs_.reset();
  }
  // update table item id
  for (int64_t i = 0; OB_SUCC(ret) && i < other.table_items_.count(); i++) {
    if (OB_ISNULL(table_items_.at(i)) || OB_ISNULL(other.table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(table_items_.at(i)), K(other.table_items_.at(i)), K(ret));
    } else if (OB_FAIL(update_table_item_id(other, *other.table_items_.at(i), true, *table_items_.at(i)))) {
      LOG_WARN("failed to update table id for table item", K(ret));
    } else { /*do nothing*/
    }
  }
  // update joined table id
  for (int64_t i = 0; OB_SUCC(ret) && i < other.joined_tables_.count(); i++) {
    if (OB_ISNULL(other.joined_tables_.at(i)) || OB_ISNULL(joined_tables_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null joined table", K(other.joined_tables_.at(i)), K(joined_tables_.at(i)), K(ret));
    } else if (OB_FAIL(
                   update_table_item_id_for_joined_table(other, *other.joined_tables_.at(i), *joined_tables_.at(i)))) {
      LOG_WARN("failed to update table id for joined table", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLStmt::adjust_statement_id()
{
  int ret = OB_SUCCESS;
  set_stmt_id();
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_ctx is NULL", K(ret));
  } else if (OB_FAIL(query_ctx_->add_stmt_id_name(get_stmt_id(), ObString::make_empty_string(), this))) {
    LOG_WARN("failed to add_stmt_id_name", K(stmt_id_), K(ret));
  } else { /* do nothing */
  }
  return ret;
}

/**
 * @brief ObSelectStmt::adjust_subquery_stmt_parent
 * replace parent_namespace_stmt_ field for all descendants
 */
int ObDMLStmt::adjust_subquery_stmt_parent(const ObDMLStmt* old_parent, ObDMLStmt* new_parent)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> subqueries;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack is overflow", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(get_child_stmts(subqueries))) {
    LOG_WARN("failed to get child stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subqueries.count(); ++i) {
    if (OB_ISNULL(subqueries.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null", K(ret), K(i));
    } else if (subqueries.at(i)->parent_namespace_stmt_ != old_parent) {
      // grand_child_stmt's parent can only be child_stmt or child_stmt's parent
      // since child_stmt's parent is not new_parent
      // grand_child_stmt's parent would never be new_parent
    } else if (OB_FAIL(SMART_CALL(subqueries.at(i)->adjust_subquery_stmt_parent(old_parent, new_parent)))) {
      LOG_WARN("failed to adjust stmt parent", K(ret));
    } else {
      subqueries.at(i)->parent_namespace_stmt_ = new_parent;
    }
  }
  return ret;
}

int ObDMLStmt::update_table_item_id_for_joined_table(
    const ObDMLStmt& other_stmt, const JoinedTable& other, JoinedTable& current)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.left_table_) || OB_ISNULL(other.right_table_) || OB_ISNULL(current.left_table_) ||
      OB_ISNULL(current.right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null table item",
        K(other.left_table_),
        K(other.right_table_),
        K(current.left_table_),
        K(current.right_table_),
        K(ret));
  } else if (OB_FAIL(update_table_item_id(other_stmt, other, false, current))) {
    LOG_WARN("failed to update table id", K(ret));
  } else if (other.left_table_->is_joined_table() && current.left_table_->is_joined_table() &&
             OB_FAIL(update_table_item_id_for_joined_table(other_stmt,
                 static_cast<JoinedTable&>(*other.left_table_),
                 static_cast<JoinedTable&>(*current.left_table_)))) {
    LOG_WARN("failed to update table id", K(ret));
  } else if (other.right_table_->is_joined_table() && current.right_table_->is_joined_table() &&
             OB_FAIL(update_table_item_id_for_joined_table(other_stmt,
                 static_cast<JoinedTable&>(*other.right_table_),
                 static_cast<JoinedTable&>(*current.right_table_)))) {
    LOG_WARN("failed to update table id", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObDMLStmt::update_table_item_id(
    const ObDMLStmt& other, const TableItem& old_item, const bool has_bit_index, TableItem& new_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query ctx", K(ret));
  } else {
    uint64_t old_table_id = old_item.table_id_;
    uint64_t new_table_id = query_ctx_->available_tb_id_--;
    int32_t old_bit_id = OB_INVALID_INDEX;
    int32_t new_bit_id = OB_INVALID_INDEX;
    new_item.table_id_ = new_table_id;
    if (TableItem::TableType::BASE_TABLE == new_item.type_) {
      new_item.type_ = TableItem::TableType::ALIAS_TABLE;
      new_item.alias_name_ = ObString::make_string("");
    }
    if (has_bit_index) {
      if (OB_FAIL(set_table_bit_index(new_table_id))) {
        LOG_WARN("failed to set table bit index", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == (new_bit_id = get_table_bit_index(new_table_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table index id", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == (old_bit_id = other.get_table_bit_index(old_table_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table index id", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransformUtils::update_table_id_for_from_item(
              other.from_items_, old_table_id, new_table_id, from_items_))) {
        LOG_WARN("failed to update table id for from item", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_joined_tables(
                     other.joined_tables_, old_table_id, new_table_id, joined_tables_))) {
        LOG_WARN("failed to update table id for joined tables", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_part_item(
                     other.part_expr_items_, old_table_id, new_table_id, part_expr_items_))) {
        LOG_WARN("failed to update table id for part item", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_part_array(
                     other.related_part_expr_arrays_, old_table_id, new_table_id, related_part_expr_arrays_))) {
        LOG_WARN("failed to update table id for part array", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_semi_info(
                     other.semi_infos_, old_table_id, new_table_id, semi_infos_))) {
        LOG_WARN("failed to update table id for semi_info", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_view_table_id(
                     other.view_table_id_store_, old_table_id, new_table_id, view_table_id_store_))) {
        LOG_WARN("failed to update table id for view table id", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_column_item(
                     other.column_items_, old_table_id, new_table_id, old_bit_id, new_bit_id, column_items_))) {
        LOG_WARN("failed to update table id for view table id", K(ret));
      } else if (OB_FAIL(ObTransformUtils::update_table_id_for_stmt_hint(
                     other.stmt_hint_, old_table_id, new_table_id, old_bit_id, new_bit_id, stmt_hint_))) {
        LOG_WARN("failed to update table id for stmt hint", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObDMLStmt::inner_get_relation_exprs(RelExprCheckerBase& expr_checker)
{
  int ret = OB_SUCCESS;
  // add order exprs
  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::ORDER_SCOPE)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); ++i) {
      OrderItem& order_item = order_items_.at(i);
      if (OB_FAIL(expr_checker.add_expr(order_item.expr_))) {
        LOG_WARN("add relation expr failed", K(ret));
      }
    }
  }
  // add join table expr
  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::JOIN_CONDITION_SCOPE)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables_.count(); ++i) {
      if (OB_ISNULL(joined_tables_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null joined table", K(ret));
      } else if (OB_FAIL(get_join_condition_expr(*joined_tables_.at(i), expr_checker))) {
        LOG_WARN("get join table condition expr failed", K(ret));
      }
    }
  }

  // add semi condition expr
  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::JOIN_CONDITION_SCOPE)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos_.count(); ++i) {
      if (OB_ISNULL(semi_infos_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null semi info", K(ret));
      } else if (OB_FAIL(expr_checker.add_exprs((semi_infos_.at(i)->semi_conditions_)))) {
        LOG_WARN("failed to add semi condition exprs", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::LIMIT_SCOPE)) {
    if (limit_count_expr_ != NULL && OB_FAIL(expr_checker.add_expr(limit_count_expr_))) {
      LOG_WARN("add limit count expr failed", K(ret));
    } else if (limit_offset_expr_ != NULL && OB_FAIL(expr_checker.add_expr(limit_offset_expr_))) {
      LOG_WARN("add limit offset expr failed", K(ret));
    } else if (limit_percent_expr_ != NULL && OB_FAIL(expr_checker.add_expr(limit_percent_expr_))) {
      LOG_WARN("add limit percent expr failed", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::WHERE_SCOPE)) {
    if (OB_FAIL(expr_checker.add_exprs(condition_exprs_))) {
      LOG_WARN("add condition exprs to relation exprs failed", K(ret));
    } else if (OB_FAIL(expr_checker.add_exprs(deduced_exprs_))) {
      LOG_WARN("add deduced exprs to relation exprs failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLStmt::get_join_condition_expr(JoinedTable& join_table, RelExprCheckerBase& expr_checker) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_checker.add_exprs(join_table.join_conditions_))) {
    LOG_WARN("add join condition to relexpr checker failed", K(ret));
  } else if (OB_FAIL(expr_checker.add_exprs(join_table.coalesce_expr_))) {
    LOG_WARN("add join condition to relexpr checker failed", K(ret));
  } else {
    if (NULL != join_table.left_table_ && join_table.left_table_->is_joined_table()) {
      JoinedTable& left_table = static_cast<JoinedTable&>(*join_table.left_table_);
      if (OB_FAIL(SMART_CALL(get_join_condition_expr(left_table, expr_checker)))) {
        LOG_WARN("get left join table condition expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && join_table.right_table_ != NULL && join_table.right_table_->is_joined_table()) {
      JoinedTable& right_table = static_cast<JoinedTable&>(*join_table.right_table_);
      if (OB_FAIL(SMART_CALL(get_join_condition_expr(right_table, expr_checker)))) {
        LOG_WARN("get right join table condition expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); ++i) {
    OrderItem& order_item = order_items_.at(i);
    if (order_item.expr_ == from) {
      order_item.expr_ = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs_.count(); ++i) {
    if (condition_exprs_.at(i) == from) {
      condition_exprs_.at(i) = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables_.count(); ++i) {
    if (OB_FAIL(replace_expr_in_joined_table(*joined_tables_.at(i), from, to))) {
      LOG_WARN("get join table condition expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && limit_count_expr_ == from) {
    limit_count_expr_ = to;
  }
  if (OB_SUCC(ret) && limit_offset_expr_ == from) {
    limit_offset_expr_ = to;
  }
  if (OB_SUCC(ret) && limit_percent_expr_ == from) {
    limit_percent_expr_ = to;
  }
  return ret;
}

int ObDMLStmt::replace_expr_in_joined_table(JoinedTable& joined_table, ObRawExpr* from, ObRawExpr* to)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.join_conditions_.count(); ++i) {
    ObRawExpr*& expr = joined_table.join_conditions_.at(i);
    if (expr == from) {
      expr = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.coalesce_expr_.count(); ++i) {
    ObRawExpr*& expr = joined_table.coalesce_expr_.at(i);
    if (expr == from) {
      expr = to;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    if (NULL != joined_table.left_table_ && joined_table.left_table_->is_joined_table()) {
      JoinedTable& left_table = static_cast<JoinedTable&>(*joined_table.left_table_);
      if (OB_FAIL(SMART_CALL(replace_expr_in_joined_table(left_table, from, to)))) {
        LOG_WARN("replace expr in joined table failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && joined_table.right_table_ != NULL && joined_table.right_table_->is_joined_table()) {
      JoinedTable& right_table = static_cast<JoinedTable&>(*joined_table.right_table_);
      if (OB_FAIL(SMART_CALL(replace_expr_in_joined_table(right_table, from, to)))) {
        LOG_WARN("replace expr in joined table failed", K(ret));
      }
    }
  }
  return ret;
}

void ObDMLStmt::set_subquery_flag(bool has_subquery)
{
  has_subquery_ = has_subquery;
}

bool ObDMLStmt::has_subquery() const
{
  return subquery_exprs_.count() > 0;
}

int ObDMLStmt::remove_part_expr_items(ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    ret = remove_part_expr_items(table_ids.at(i));
  }
  return ret;
}

int ObDMLStmt::remove_part_expr_items(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = part_expr_items_.count() - 1; i >= 0; --i) {
    if (table_id == part_expr_items_.at(i).table_id_) {
      if (OB_FAIL(part_expr_items_.remove(i))) {
        LOG_WARN("fail to remove part expr item", K(table_id), K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_part_expr_items(ObIArray<uint64_t>& table_ids, ObIArray<PartExprItem>& part_items)
{
  int ret = OB_SUCCESS;
  part_items.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < part_expr_items_.count(); ++j) {
      if (table_ids.at(i) != part_expr_items_.at(j).table_id_) {
        // do nothing
      } else if (OB_FAIL(part_items.push_back(part_expr_items_.at(j)))) {
        LOG_WARN("failed to push back part expr item", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_part_expr_items(uint64_t table_id, ObIArray<PartExprItem>& part_items)
{
  int ret = OB_SUCCESS;
  part_items.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < part_expr_items_.count(); ++i) {
    if (table_id != part_expr_items_.at(i).table_id_) {
      // do nothing
    } else if (OB_FAIL(part_items.push_back(part_expr_items_.at(i)))) {
      LOG_WARN("failed to push back part expr item", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::set_part_expr_items(ObIArray<PartExprItem>& part_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_items.count(); ++i) {
    if (OB_FAIL(set_part_expr_item(part_items.at(i)))) {
      if (OB_LIKELY(OB_ENTRY_EXIST == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to set part expr item", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::set_part_expr_item(PartExprItem& part_item)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; !found && i < part_expr_items_.count(); i++) {
    if (part_item.table_id_ == part_expr_items_.at(i).table_id_ &&
        part_item.index_tid_ == part_expr_items_.at(i).index_tid_) {
      found = true;
    }
  }
  if (found) {
    ret = OB_ENTRY_EXIST;
  } else {
    if (OB_FAIL(part_expr_items_.push_back(part_item))) {
      SQL_RESV_LOG(WARN, "push back part expr item failed", K(ret));
    }
  }

  return ret;
}

ObRawExpr* ObDMLStmt::get_related_part_expr(const uint64_t table_id, uint64_t index_tid, int32_t idx) const
{
  ObRawExpr* expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < related_part_expr_arrays_.count(); ++i) {
    if (table_id == related_part_expr_arrays_.at(i).table_id_ &&
        index_tid == related_part_expr_arrays_.at(i).index_tid_) {
      expr = related_part_expr_arrays_.at(i).part_expr_array_.at(idx);
      found = true;
    }
  }
  return expr;
}

ObRawExpr* ObDMLStmt::get_related_subpart_expr(const uint64_t table_id, uint64_t index_tid, int32_t idx) const
{
  ObRawExpr* expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < related_part_expr_arrays_.count(); ++i) {
    if (table_id == related_part_expr_arrays_.at(i).table_id_ &&
        index_tid == related_part_expr_arrays_.at(i).index_tid_) {
      expr = related_part_expr_arrays_.at(i).subpart_expr_array_.at(idx);
      found = true;
    }
  }
  return expr;
}

ObRawExpr* ObDMLStmt::get_part_expr(const uint64_t table_id, uint64_t index_tid) const
{
  ObRawExpr* expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < part_expr_items_.count(); ++i) {
    if (table_id == part_expr_items_.at(i).table_id_ && index_tid == part_expr_items_.at(i).index_tid_) {
      expr = part_expr_items_.at(i).part_expr_;
      found = true;
    }
  }
  return expr;
}

ObRawExpr* ObDMLStmt::get_subpart_expr(const uint64_t table_id, uint64_t index_tid) const
{
  ObRawExpr* expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < part_expr_items_.count(); ++i) {
    if (table_id == part_expr_items_.at(i).table_id_ && index_tid == part_expr_items_.at(i).index_tid_) {
      expr = part_expr_items_.at(i).subpart_expr_;
      found = true;
    }
  }
  return expr;
}

int ObDMLStmt::set_part_expr(uint64_t table_id, uint64_t index_tid, ObRawExpr* part_expr, ObRawExpr* subpart_expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_expr_items_.count(); ++i) {
    if (table_id == part_expr_items_.at(i).table_id_ && index_tid == part_expr_items_.at(i).index_tid_) {
      ret = OB_ERR_TABLE_EXIST;
      SQL_RESV_LOG(WARN, "table part expr exists", K(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    PartExprItem part_item;
    part_item.table_id_ = table_id;
    part_item.index_tid_ = index_tid;
    part_item.part_expr_ = part_expr;
    part_item.subpart_expr_ = subpart_expr;
    if (OB_FAIL(part_expr_items_.push_back(part_item))) {
      SQL_RESV_LOG(WARN, "push back part expr item failed", K(ret));
    }
  }
  return ret;
}

const JoinedTable* ObDMLStmt::get_joined_table(uint64_t table_id) const
{
  const JoinedTable* joined_table = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < joined_tables_.count(); ++i) {
    const JoinedTable* cur_table = joined_tables_.at(i);
    if (NULL != cur_table) {
      if (cur_table->table_id_ == table_id) {
        joined_table = cur_table;
        found = true;
      }
    }
  }
  return joined_table;
}

JoinedTable* ObDMLStmt::get_joined_table(uint64_t table_id)
{
  JoinedTable* joined_table = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < joined_tables_.count(); ++i) {
    JoinedTable* cur_table = joined_tables_.at(i);
    if (NULL != cur_table) {
      if (cur_table->table_id_ == table_id) {
        joined_table = cur_table;
        found = true;
      }
    }
  }
  return joined_table;
}

int ObDMLStmt::pull_all_expr_relation_id_and_levels()
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> relation_exprs;
  ObArray<ObSelectStmt*> view_stmts;
  if (OB_FAIL(get_relation_exprs(relation_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr* expr = relation_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(expr->pull_relation_id_and_levels(get_current_level()))) {
      LOG_WARN("pull expr relation ids failed", K(ret), K(*expr));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_from_subquery_stmts(view_stmts))) {
      LOG_WARN("get from subquery stmts failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < view_stmts.count(); ++i) {
      ObSelectStmt* view_stmt = view_stmts.at(i);
      if (OB_ISNULL(view_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view_stmt is null");
      } else if (OB_FAIL(SMART_CALL(view_stmt->pull_all_expr_relation_id_and_levels()))) {
        LOG_WARN("pull view stmt all expr relation id and levels failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_order_exprs(ObIArray<ObRawExpr*>& order_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); i++) {
    if (OB_ISNULL(order_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(order_exprs.push_back(order_items_.at(i).expr_))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLStmt::formalize_stmt(ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> relation_exprs;
  ObArray<ObSelectStmt*> view_stmts;
  if (OB_FAIL(get_relation_exprs(relation_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr* expr = relation_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(expr->pull_relation_id_and_levels(get_current_level()))) {
      LOG_WARN("pull expr relation ids failed", K(ret), K(*expr));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_from_subquery_stmts(view_stmts))) {
      LOG_WARN("get from subquery stmts failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < view_stmts.count(); ++i) {
      ObSelectStmt* view_stmt = view_stmts.at(i);
      if (OB_ISNULL(view_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view_stmt is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(view_stmt->formalize_stmt(session_info)))) {
        LOG_WARN("formalize view stmt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::formalize_stmt_expr_reference()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 32> stmt_exprs;
  ObSEArray<ObSelectStmt*, 32> child_stmts;
  if (OB_FAIL(clear_sharable_expr_reference())) {
    LOG_WARN("failed to clear sharable expr reference", K(ret));
  } else if (OB_FAIL(get_relation_exprs(stmt_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  } else if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_exprs.count(); i++) {
      if (OB_ISNULL(stmt_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(set_sharable_expr_reference(*stmt_exprs.at(i)))) {
        LOG_WARN("failed to set sharable expr reference", K(ret));
      } else { /*do nothing*/
      }
    }
    // table function column expr should not be removed
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
      TableItem* table_item = NULL;
      ColumnItem& column_item = column_items_.at(i);
      if (OB_ISNULL(column_item.expr_) || OB_ISNULL(table_item = get_table_item_by_id(column_item.table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_item.expr_), K(table_item), K(ret));
      } else if (table_item->is_function_table() || table_item->for_update_ || is_hierarchical_query_) {
        if (OB_FAIL(set_sharable_expr_reference(*column_item.expr_))) {
          LOG_WARN("failed to set sharable exprs reference", K(ret));
        }
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      ObSelectStmt* stmt = child_stmts.at(i);
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(stmt->formalize_stmt_expr_reference()))) {
        LOG_WARN("failed to formalize stmt reference", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_useless_sharable_expr())) {
        LOG_WARN("failed to remove useless sharable expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObDMLStmt::set_sharable_expr_reference(ObRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (expr.is_column_ref_expr() || expr.is_aggr_expr() || expr.is_win_func_expr() || expr.is_query_ref_expr() ||
      ObRawExprUtils::is_pseudo_column_like_expr(expr)) {
    expr.set_explicited_reference();
    if (expr.is_column_ref_expr()) {
      ObColumnRefRawExpr& column_expr = static_cast<ObColumnRefRawExpr&>(expr);
      if (NULL != column_expr.get_dependant_expr() &&
          OB_FAIL(set_sharable_expr_reference(*column_expr.get_dependant_expr()))) {
        LOG_WARN("failed to set sharable expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && (expr.has_flag(CNT_COLUMN) || expr.has_flag(CNT_AGG) || expr.has_flag(CNT_WINDOW_FUNC) ||
                          expr.has_flag(CNT_SUB_QUERY) || expr.has_flag(CNT_ROWNUM) || expr.has_flag(CNT_SEQ_EXPR) ||
                          expr.has_flag(CNT_PSEUDO_COLUMN))) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); i++) {
      if (OB_ISNULL(expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(set_sharable_expr_reference(*expr.get_param_expr(i)))) {
        LOG_WARN("failed to set sharable expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObDMLStmt::remove_useless_sharable_expr()
{
  int ret = OB_SUCCESS;
  for (int64_t i = column_items_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    bool is_referred = false;
    ObColumnRefRawExpr* expr = NULL;
    if (OB_ISNULL(expr = column_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr->is_explicited_reference() || expr->is_fulltext_column()) {
      /*do nothing*/
    } else if (OB_FAIL(is_referred_by_partitioning_expr(expr, is_referred))) {
      LOG_WARN("failed to check whether is referred by partitioning expr", K(ret));
    } else if (is_referred) {
      /*do nothing*/
    } else if (OB_FAIL(column_items_.remove(i))) {
      LOG_WARN("failed to remove column item", K(ret));
    } else {
      LOG_TRACE("succeed to remove column items", K(*expr), K(lbt()));
    }
  }
  for (int64_t i = subquery_exprs_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    ObQueryRefRawExpr* expr = NULL;
    if (OB_ISNULL(expr = subquery_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr->is_explicited_reference() || expr->is_cursor()) {
      /*do nothing*/
    } else if (OB_FAIL(subquery_exprs_.remove(i))) {
      LOG_WARN("failed to remove subquery expr", K(ret));
    } else {
      LOG_TRACE("succeed to remove subquery exprs", K(*expr));
    }
  }
  for (int64_t i = pseudo_column_like_exprs_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = pseudo_column_like_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr->is_explicited_reference()) {
      // do nothing
    } else if (OB_FAIL(pseudo_column_like_exprs_.remove(i))) {
      LOG_WARN("failed to remove pseudo column like exprs", K(ret));
    } else {
      LOG_TRACE("succeed to remove pseudo column like exprs", K(*expr));
    }
  }
  return ret;
}

int ObDMLStmt::is_referred_by_partitioning_expr(const ObRawExpr* expr, bool& is_referred)
{
  int ret = OB_SUCCESS;
  is_referred = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> part_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr_items_.count(); i++) {
      ObRawExpr* part_expr = part_expr_items_.at(i).part_expr_;
      ObRawExpr* subpart_expr = part_expr_items_.at(i).subpart_expr_;
      if (NULL != part_expr && OB_FAIL(part_exprs.push_back(part_expr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else if (NULL != subpart_expr && OB_FAIL(part_exprs.push_back(subpart_expr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < related_part_expr_arrays_.count(); i++) {
      if (OB_FAIL(append(part_exprs, related_part_expr_arrays_.at(i).part_expr_array_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(append(part_exprs, related_part_expr_arrays_.at(i).subpart_expr_array_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr*, 16> column_exprs;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_exprs, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
          ObColumnRefRawExpr* column_expr = NULL;
          if (OB_ISNULL(column_exprs.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_UNLIKELY(!column_exprs.at(i)->is_column_ref_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected expr type", K(ret));
          } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(i)))) {
            /*do nothing*/
          } else if (NULL != column_expr->get_dependant_expr() &&
                     OB_FAIL(part_exprs.push_back(column_expr->get_dependant_expr()))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_referred = ObOptimizerUtil::is_sub_expr(expr, part_exprs);
    }
  }
  return ret;
}

int ObDMLStmt::clear_sharable_expr_reference()
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  // for column items
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    if (OB_ISNULL(expr = column_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      expr->clear_explicited_referece();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); i++) {
    if (OB_ISNULL(subquery_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      subquery_exprs_.at(i)->clear_explicited_referece();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pseudo_column_like_exprs_.count(); i++) {
    if (OB_ISNULL(pseudo_column_like_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      pseudo_column_like_exprs_.at(i)->clear_explicited_referece();
    }
  }
  return ret;
}

int ObDMLStmt::get_from_subquery_stmts(ObIArray<ObSelectStmt*>& child_stmts) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_table_size(); ++i) {
    const TableItem* table_item = get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item is null", K(i));
    } else if (table_item->is_generated_table() || table_item->is_temp_table()) {
      if (OB_FAIL(child_stmts.push_back(table_item->ref_query_))) {
        LOG_WARN("adjust parent namespace stmt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::add_view_table_ids(const ObIArray<uint64_t>& view_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < view_ids.count(); ++i) {
    if (OB_FAIL(add_view_table_id(view_ids.at(i)))) {
      LOG_WARN("fail to add view table id", K(i), K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::add_view_table_id(uint64_t view_table_id)
{
  int ret = OB_SUCCESS;
  // remove duplicate
  bool is_exist = false;
  for (int64_t i = 0; !is_exist && i < view_table_id_store_.count(); i++) {
    if (view_table_id == view_table_id_store_.at(i)) {
      is_exist = true;
    }
  }
  if (false == is_exist) {
    if (OB_FAIL(view_table_id_store_.push_back(view_table_id))) {
      SQL_RESV_LOG(WARN, "fail to push view table id", K(ret));
    }
  }
  return ret;
}

void ObDMLStmt::set_is_table_flag()
{
  has_is_table_ = true;
}

int ObDMLStmt::has_is_table(bool& has_is_table) const
{
  int ret = OB_SUCCESS;
  has_is_table = false;
  ObArray<ObSelectStmt*> child_stmts;
  if (has_is_table_) {
    has_is_table = true;
  } else if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_ERROR("get child stmt failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_is_table && i < child_stmts.count(); ++i) {
      ObSelectStmt* sub_stmt = child_stmts.at(i);
      if (OB_ISNULL(sub_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub stmt is null");
      } else if (OB_FAIL(SMART_CALL(sub_stmt->has_is_table(has_is_table)))) {
        LOG_WARN("check sub stmt whether has is table failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::has_inner_table(bool& has_inner_table) const
{
  int ret = OB_SUCCESS;
  has_inner_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_inner_table && i < table_items_.count(); i++) {
    if (OB_ISNULL(table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret));
    } else if (table_items_.at(i)->is_basic_table()) {
      has_inner_table = is_inner_table(table_items_.at(i)->ref_id_);
    } else { /*do nothing*/
    }
  }
  return ret;
}

///////////functions for sql hint/////////////
int ObDMLStmt::check_and_convert_hint(const ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmt failed", K(ret));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < child_stmts.count(); ++idx) {
    ObDMLStmt* child_stmt = child_stmts.at(idx);
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Child stmt is NULL", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_stmt->check_and_convert_hint(session_info)))) {
      LOG_WARN("Failed to check and convert hint", K(ret));
    } else {
    }  // do nothing
  }

  if (OB_FAIL(ret)) {
  } else if (stmt_hint_.converted_) {
    // been converted, do nothing
  } else if (OB_FAIL(SMART_CALL(check_and_convert_hint(session_info, stmt_hint_)))) {
    LOG_WARN("check and convert stmt hint failed", K(ret));
  } else if (OB_FAIL(stmt_hint_.merge_view_hints())) {
    LOG_WARN("Failed to merge view hints", K(ret));
  } else {
    stmt_hint_.converted_ = true;
  }
  return ret;
}

int ObDMLStmt::check_and_convert_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_and_convert_index_hint(session_info, hint))) {
    LOG_WARN("Failed to check and convert index hint", K(ret));
  } else if (OB_FAIL(check_and_convert_leading_hint(session_info, hint))) {
    LOG_WARN("Failed to check and convert leading hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.use_merge_, hint.use_merge_order_pairs_, hint.use_merge_ids_))) {
    LOG_WARN("Failed to check and convert merge join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.no_use_merge_, hint.no_use_merge_order_pairs_, hint.no_use_merge_ids_))) {
    LOG_WARN("Failed to check and convert no merge join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.use_hash_, hint.use_hash_order_pairs_, hint.use_hash_ids_))) {
    LOG_WARN("Failed to check and convert hash join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.no_use_hash_, hint.no_use_hash_order_pairs_, hint.no_use_hash_ids_))) {
    LOG_WARN("Failed to check and convert no hash join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.use_nl_, hint.use_nl_order_pairs_, hint.use_nl_ids_))) {
    LOG_WARN("Failed to check and convert nl join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.no_use_nl_, hint.no_use_nl_order_pairs_, hint.no_use_nl_ids_))) {
    LOG_WARN("Failed to check and convert no nl join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.use_bnl_, hint.use_bnl_order_pairs_, hint.use_bnl_ids_))) {
    LOG_WARN("Failed to check and convert bnl join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.no_use_bnl_, hint.no_use_bnl_order_pairs_, hint.no_use_bnl_ids_))) {
    LOG_WARN("Failed to check and convert no bnl join hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(session_info,
                 hint.use_nl_materialization_,
                 hint.use_nl_materialization_order_pairs_,
                 hint.use_nl_materialization_ids_))) {
    LOG_WARN("Failed to check and convert use material nl hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(session_info,
                 hint.no_use_nl_materialization_,
                 hint.no_use_nl_materialization_order_pairs_,
                 hint.no_use_nl_materialization_ids_))) {
    LOG_WARN("Failed to check and convert no use material nl hint", K(ret));
  } else if (OB_FAIL(check_and_convert_pq_dist_hint(session_info, hint))) {
    LOG_WARN("Failed to check and convert pq distribute hint", K(ret));
  } else if (OB_FAIL(check_and_convert_pq_map_hint(session_info, hint))) {
    LOG_WARN("Failed to check and convert pq distribute hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(
                 session_info, hint.px_join_filter_, hint.px_join_filter_order_pairs_, hint.px_join_filter_ids_))) {
    LOG_WARN("Failed to check and convert px join filter hint", K(ret));
  } else if (OB_FAIL(check_and_convert_join_method_hint(session_info,
                 hint.no_px_join_filter_,
                 hint.no_px_join_filter_order_pairs_,
                 hint.no_px_join_filter_ids_))) {
    LOG_WARN("Failed to check and convert px join filter hint", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < hint.subquery_hints_.count(); ++i) {
      ObStmtHint* sub_hints = const_cast<ObStmtHint*>(hint.subquery_hints_.at(i));
      if (OB_ISNULL(sub_hints)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subquery hint is null");
      } else if (OB_FAIL(check_and_convert_hint(session_info, *sub_hints))) {
        LOG_WARN("check and convert hint failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::check_and_convert_index_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint)
{
  int ret = OB_SUCCESS;

  if (hint.org_indexes_.count() > 0) {
    uint64_t table_id = OB_INVALID_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < hint.org_indexes_.count(); ++idx) {
      ObOrgIndexHint& index_hint = hint.org_indexes_.at(idx);
      ObTableInHint& table = index_hint.table_;
      if (OB_FAIL(get_table_id(session_info, table.qb_name_, table.db_name_, table.table_name_, table_id))) {
        LOG_WARN("Failed to get table id", K(ret));
      } else if (OB_INVALID_ID == table_id) {  // not find table
        hint.set_table_name_error_flag(false);
      } else if (OB_FAIL(hint.add_index_hint(table_id, index_hint.index_name_))) {
        LOG_WARN("Failed to add index hint", K(ret));
      } else {
      }  // do nothing
    }
  }

  return ret;
}

int ObDMLStmt::check_and_convert_pq_dist_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint)
{
  int ret = OB_SUCCESS;
  FOREACH_X(org, hint.org_pq_distributes_, OB_SUCC(ret))
  {
    uint64_t table_id = OB_INVALID_ID;
    ObPQDistributeHint* pq_distributes = NULL;
    common::ObSEArray<ObTableInHint, 3> table_arr = org->tables_;
    if (OB_ISNULL(pq_distributes = hint.pq_distributes_.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObTablesInHint from array error", K(ret));
    } else {
      *(ObPQDistributeHintMethod*)pq_distributes = *org;
      FOREACH_X(t, table_arr, OB_SUCC(ret))
      {
        if (OB_FAIL(get_table_id(session_info, t->qb_name_, t->db_name_, t->table_name_, table_id))) {
          LOG_WARN("failed to get table id", K(ret));
        } else if (OB_INVALID_ID == table_id) {  // table not found, do nothing.
        } else if (OB_FAIL(pq_distributes->table_ids_.push_back(table_id))) {
          LOG_WARN("fail to push back pq distribute table ids.", K(ret));
        } else {
        }  // do nothing.
      }
    }
  }
  return ret;
}

int ObDMLStmt::check_and_convert_pq_map_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint)
{
  int ret = OB_SUCCESS;
  if (hint.org_pq_maps_.count() > 0) {
    uint64_t table_id = OB_INVALID_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < hint.org_pq_maps_.count(); ++idx) {
      ObOrgPQMapHint& org_pq_map_hint = hint.org_pq_maps_.at(idx);
      ObTableInHint& table = org_pq_map_hint.table_;
      ObPQMapHint pq_map_hint;
      if (OB_FAIL(get_table_id(session_info, table.qb_name_, table.db_name_, table.table_name_, table_id))) {
        LOG_WARN("failed to get table id", K(ret));
      } else if (OB_INVALID_ID == table_id) {
        // table not found, do nothing
      } else {
        pq_map_hint.table_id_ = table_id;
        if (OB_FAIL(hint.pq_maps_.push_back(pq_map_hint))) {
          LOG_WARN("failed to add pq map hint", K(ret));
        }
      }
      LOG_DEBUG("convert pq map hint", K(table));
    }
  }

  return ret;
}

int ObDMLStmt::check_and_convert_leading_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint)
{
  int ret = OB_SUCCESS;
  if (hint.join_order_.count() > 0) {
    ObIArray<uint64_t>& leading_ids = hint.join_order_ids_;
    const ObIArray<ObTableInHint>& tables = hint.join_order_;
    uint64_t table_id = OB_INVALID_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tables.count(); ++idx) {
      const ObTableInHint& table = tables.at(idx);
      if (OB_FAIL(get_table_id(session_info, table.qb_name_, table.db_name_, table.table_name_, table_id))) {
        LOG_WARN("Failed to get table id", K(ret));
      } else if (OB_INVALID_ID == table_id) {
        leading_ids.reset();
        break;
      } else if (has_exist_in_array(leading_ids, table_id)) {
        leading_ids.reset();
        break;
      } else if (OB_FAIL(leading_ids.push_back(table_id))) {
        LOG_WARN("Failed to add table id to join order ids", K(ret));
      } else {
      }  // do nothing
    }    // end of for

    if (OB_SUCC(ret)) {
      if (leading_ids.count() != tables.count()) {
        hint.join_order_pairs_.reset();
      } else {
        for (int64_t idx = 0; OB_SUCC(ret) && idx >= 0 && idx < hint.join_order_pairs_.count(); ++idx) {
          const std::pair<uint8_t, uint8_t>& order_pair = hint.join_order_pairs_.at(idx);
          if (order_pair.first >= order_pair.second || order_pair.second >= leading_ids.count() ||
              order_pair.second >= UINT8_MAX) {
            if (OB_FAIL(hint.join_order_pairs_.remove(idx))) {
              hint.join_order_pairs_.reset();
              LOG_WARN("Failed to remove invalid join order pair", K(idx));
            } else {
              idx--;
            }
          } else { /* do nothing */
          }
        }
      }
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }
  return ret;
}

int ObDMLStmt::check_and_convert_join_method_hint(const ObSQLSessionInfo& session_info,
    const common::ObIArray<ObTableInHint>& hint_tables,
    const common::ObIArray<std::pair<uint8_t, uint8_t>>& join_order_pairs, common::ObIArray<ObTablesIndex>& table_ids)
{
  int ret = OB_SUCCESS;

  if (hint_tables.count() > 0) {
    uint64_t table_id = OB_INVALID_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < join_order_pairs.count(); ++idx) {
      ObTablesIndex tablesIndex;
      std::pair<uint8_t, uint8_t> table_pair = join_order_pairs.at(idx);
      for (int64_t i = table_pair.first; OB_SUCC(ret) && i <= table_pair.second; i++) {
        const ObTableInHint& table = hint_tables.at(i);
        if (OB_FAIL(get_table_id(session_info, table.qb_name_, table.db_name_, table.table_name_, table_id))) {
          LOG_WARN("Failed to get table id", K(ret));
        } else if (OB_INVALID_ID == table_id) {
          // do nothing
        } else if (OB_FAIL(add_var_to_array_no_dup(tablesIndex.indexes_, table_id))) {
          LOG_WARN("Failed to add table id", K(ret));
        } else {
          // do nothing
        }
      }
      if (OB_FAIL(table_ids.push_back(tablesIndex))) {
        LOG_WARN("failed to push back table index.", K(ret));
      } else {
      }  // do nothing.
    }
  }

  return OB_SUCCESS;
}

int ObDMLStmt::get_table_item(const ObSQLSessionInfo* session_info, const ObString& database_name,
    const ObString& object_name, const TableItem*& table_item) const
{
  int ret = OB_SUCCESS;
  int64_t num = table_items_.count();
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
    if (OB_ISNULL(table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table items is null", K(i));
    } else {
      const TableItem& item = *(table_items_.at(i));
      bool is_db_found = true;
      // database_name is not empty, so must compare database name
      // if database_name is empty, means that the column does not specify the database name,
      // so we must find in all table
      if (OB_FAIL(ObResolverUtils::name_case_cmp(
              session_info, database_name, item.database_name_, OB_TABLE_NAME_CLASS, is_db_found))) {
        LOG_WARN("fail to compare db names", K(database_name), K(object_name), K(item), K(ret));
      } else if (is_db_found) {
        const ObString& src_table_name = item.get_object_name();
        if (OB_FAIL(ObResolverUtils::name_case_cmp(
                session_info, object_name, src_table_name, OB_TABLE_NAME_CLASS, is_found))) {
          LOG_WARN("fail to compare names", K(database_name), K(object_name), K(item), K(ret));
        } else if (is_found) {
          table_item = &item;
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ERR_UNKNOWN_TABLE;
  }
  return ret;
}

// get all table in current namespace, maybe table come from different database
int ObDMLStmt::get_all_table_item_by_tname(const ObSQLSessionInfo* session_info, const ObString& db_name,
    const ObString& table_name, ObIArray<const TableItem*>& table_items) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); ++i) {
    const TableItem* item = table_items_.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null");
    } else {
      const ObString& tname = item->alias_name_.length() > 0 ? item->alias_name_ : item->table_name_;
      bool is_equal = true;
      if (!db_name.empty()) {
        if (OB_FAIL(ObResolverUtils::name_case_cmp(
                session_info, db_name, item->database_name_, OB_TABLE_NAME_CLASS, is_equal))) {
          LOG_WARN("fail to compare names", K(table_name), K(item), K(ret));
        }
      }
      if (is_equal &&
          OB_FAIL(ObResolverUtils::name_case_cmp(session_info, table_name, tname, OB_TABLE_NAME_CLASS, is_equal))) {
        LOG_WARN("fail to compare names", K(table_name), K(item), K(ret));
      } else if (is_equal) {
        if (OB_FAIL(table_items.push_back(item))) {
          LOG_WARN("push back table item failed", K(ret));
        }
      }
    }
  }
  return ret;
}

const TableItem* ObDMLStmt::get_table_item_by_id(uint64_t table_id) const
{
  const TableItem* table_item = NULL;
  int64_t num = table_items_.count();
  for (int64_t i = 0; i < num; ++i) {
    if (table_items_.at(i) != NULL && table_items_.at(i)->table_id_ == table_id) {
      table_item = table_items_.at(i);
      break;
    }
  }
  return table_item;
}

TableItem* ObDMLStmt::get_table_item_by_id(uint64_t table_id)
{
  TableItem* table_item = NULL;
  int64_t num = table_items_.count();
  for (int64_t i = 0; i < num; ++i) {
    if (table_items_.at(i) != NULL && table_items_.at(i)->table_id_ == table_id) {
      table_item = table_items_.at(i);
      break;
    }
  }
  return table_item;
}

int ObDMLStmt::get_table_item_by_id(ObIArray<uint64_t>& table_ids, ObIArray<TableItem*>& tables)
{
  int ret = OB_SUCCESS;
  int64_t num = table_items_.count();
  for (int64_t i = 0; i < num; ++i) {
    if (!has_exist_in_array(table_ids, table_items_.at(i)->table_id_)) {
      /*do nothing*/
    } else if (OB_ISNULL(table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(tables.push_back(table_items_.at(i)))) {
      LOG_WARN("failed to push back table", K(ret));
    }
  }
  return ret;
}

TableItem* ObDMLStmt::get_table_item(const FromItem item)
{
  TableItem* ret = NULL;
  if (item.is_joined_) {
    ret = get_joined_table(item.table_id_);
  } else {
    ret = get_table_item_by_id(item.table_id_);
  }
  return ret;
}

const TableItem* ObDMLStmt::get_table_item(const FromItem item) const
{
  const TableItem* ret = NULL;
  if (item.is_joined_) {
    ret = get_joined_table(item.table_id_);
  } else {
    ret = get_table_item_by_id(item.table_id_);
  }
  return ret;
}

TableItem* ObDMLStmt::create_table_item(ObIAllocator& allocator)
{
  TableItem* table_item = NULL;
  void* ptr = NULL;
  if (NULL == (ptr = allocator.alloc(sizeof(TableItem)))) {
    LOG_WARN("alloc table item failed");
  } else {
    table_item = new (ptr) TableItem();
  }
  return table_item;
}

int ObDMLStmt::add_table_item(const ObSQLSessionInfo* session_info, ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ret = add_table_item(session_info, table_items.at(i));
  }
  return ret;
}

int ObDMLStmt::add_table_item(const ObSQLSessionInfo* session_info, TableItem* table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null");
  } else {
    const TableItem* old_item = NULL;
    const ObString& object_name = table_item->get_object_name();
    if (OB_FAIL(get_table_item(session_info, table_item->database_name_, object_name, old_item))) {
      if (OB_ERR_UNKNOWN_TABLE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get table item failed", K(ret), K(object_name), K_(table_item->database_name));
      }
    } else {
      ret = OB_ERR_NONUNIQ_TABLE;
      LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, object_name.length(), object_name.ptr());
      LOG_WARN("table_item existed", K(ret), KPC(table_item), KPC(old_item));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_items_.push_back(table_item))) {
        LOG_WARN("push back table items failed", K(ret));
      } else if (OB_FAIL(set_table_bit_index(table_item->table_id_))) {
        LOG_WARN("set table bit index failed", K(ret), K(*table_item));
      }
    }
  }
  LOG_DEBUG("finish to add table item", K(*table_item), K(tables_hash_), KPC(table_item->ref_query_), K(common::lbt()));
  return ret;
}

int ObDMLStmt::add_table_item(const ObSQLSessionInfo* session_info, TableItem* table_item, bool& have_same_table_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null");
  } else if (OB_UNLIKELY(table_item->alias_name_.length() > OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE && is_oracle_mode())) {
    ret = OB_ERR_TOO_LONG_IDENT;
    LOG_WARN("table alias name too long", K(ret), KPC(table_item));
  } else {
    const TableItem* old_item = NULL;
    const ObString& object_name = table_item->get_object_name();
    if (OB_FAIL(get_table_item(session_info, table_item->database_name_, object_name, old_item))) {
      if (OB_ERR_UNKNOWN_TABLE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get table item failed", K(ret), K(object_name), K_(table_item->database_name));
      }
    } else if (share::is_oracle_mode()) {  // oracle allow have the same name two tables
      have_same_table_name |= table_item->is_basic_table();
    } else {
      ret = OB_ERR_NONUNIQ_TABLE;
      LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, object_name.length(), object_name.ptr());
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_items_.push_back(table_item))) {
        LOG_WARN("push back table items failed", K(ret));
      } else if (OB_FAIL(set_table_bit_index(table_item->table_id_))) {
        LOG_WARN("set table bit index failed", K(ret));
      }
    }
  }
  LOG_DEBUG("finish to add table item", K(*table_item), K(tables_hash_));
  return ret;
}

// ingore the exist table
int ObDMLStmt::add_mock_table_item(TableItem* table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else {
    if (OB_FAIL(table_items_.push_back(table_item))) {
      LOG_WARN("push back table items failed", KPC(table_item), K(ret));
    } else if (OB_FAIL(set_table_bit_index(table_item->table_id_))) {
      LOG_WARN("set table bit index failed", KPC(table_item), K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::add_cte_table_item(TableItem* table_item, bool& dup_name)
{
  int ret = OB_SUCCESS;
  dup_name = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_UNLIKELY(table_item->alias_name_.length() > OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE && is_oracle_mode())) {
    ret = OB_ERR_TOO_LONG_IDENT;
    LOG_WARN("table alias name too long", K(ret), KPC(table_item));
  } else {
    bool name_already_exist = false;
    for (int64_t i = 0; i < CTE_table_items_.count(); i++) {
      if (ObCharset::case_insensitive_equal(table_item->table_name_, CTE_table_items_[i]->table_name_)) {
        name_already_exist = true;
      }
    }
    if (name_already_exist) {
      dup_name = true;
    } else if (OB_FAIL(CTE_table_items_.push_back(table_item))) {
      LOG_WARN("push back table items failed", KPC(table_item), K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::check_CTE_name_exist(const ObString& var_name, bool& exist, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  exist = false;
  for (int64_t i = 0; i < CTE_table_items_.count(); i++) {
    if (ObCharset::case_insensitive_equal(CTE_table_items_[i]->table_name_, var_name)) {
      exist = true;
      table_item = CTE_table_items_[i];
      break;
    }
  }
  return ret;
}

int ObDMLStmt::check_CTE_name_exist(const ObString& var_name, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  for (int64_t i = 0; i < CTE_table_items_.count(); i++) {
    if (ObCharset::case_insensitive_equal(CTE_table_items_[i]->table_name_, var_name)) {
      exist = true;
      break;
    }
  }
  return ret;
}

int ObDMLStmt::generate_view_name(ObIAllocator& allocator, ObString& view_name, bool is_temp)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const uint64_t OB_MAX_SUBQUERY_NAME_LENGTH = 64;
  const char* SUBQUERY_VIEW = is_temp ? "TEMP" : "VIEW";
  char buf[OB_MAX_SUBQUERY_NAME_LENGTH];
  int64_t buf_len = OB_MAX_SUBQUERY_NAME_LENGTH;
  if (OB_FAIL(BUF_PRINTF(SUBQUERY_VIEW))) {
    LOG_WARN("append name to buf error", K(ret));
  } else if (OB_FAIL(append_id_to_view_name(buf, OB_MAX_SUBQUERY_NAME_LENGTH, pos, is_temp))) {
    LOG_WARN("append name to buf error", K(ret));
  } else {
    ObString generate_name(pos, buf);
    if (OB_FAIL(ob_write_string(allocator, generate_name, view_name))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::append_id_to_view_name(char* buf, int64_t buf_len, int64_t& pos, bool is_temp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Buf should not be NULL", K(ret));
  } else if (pos >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buf size not enough", K(ret));
  } else {
    bool find_unique = false;
    int64_t old_pos = pos;
    do {
      pos = old_pos;
      int64_t id = is_temp ? get_query_ctx()->get_temp_table_id() : get_query_ctx()->get_new_subquery_id();
      if (OB_FAIL(BUF_PRINTF("%ld", id))) {
        LOG_WARN("Append idx to stmt_name error", K(ret));
      } else {
        bool find_dup = false;
        for (int64_t i = 0; !find_dup && i < table_items_.count(); ++i) {
          const ObString& tname = table_items_.at(i)->alias_name_.length() > 0 ? table_items_.at(i)->alias_name_
                                                                               : table_items_.at(i)->table_name_;
          if (0 == tname.case_compare(buf)) {
            find_dup = true;
          } else {
          }
        }
        find_unique = !find_dup;
      }
    } while (!find_unique && OB_SUCC(ret));
  }
  return ret;
}

int ObDMLStmt::get_table_item_idx(const TableItem* child_table, int64_t& idx) const
{
  int ret = OB_SUCCESS;
  idx = -1;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < get_table_size(); i++) {
    if (child_table == get_table_item(i)) {
      idx = i;
      found = true;
    }
  }
  return ret;
}

int ObDMLStmt::remove_table_item(const ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); i++) {
    ret = remove_table_item(table_items.at(i));
  }
  return ret;
}

int ObDMLStmt::remove_table_item(const TableItem* ti)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_table_size(); i++) {
    if (ti == get_table_item(i)) {
      if (OB_FAIL(table_items_.remove(i))) {
        LOG_WARN("fail to remove table item", K(ret));
      } else {
        LOG_DEBUG("succ to remove_table_item", KPC(ti), K(lbt()));
        break;
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_joined_item_idx(const TableItem* child_table, int64_t& idx) const
{
  int ret = OB_SUCCESS;
  idx = -1;
  bool is_found = false;
  for (int64_t i = 0; !is_found && i < joined_tables_.count(); i++) {
    if (child_table == joined_tables_.at(i)) {
      idx = i;
      is_found = true;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLStmt::get_column_items(ObIArray<uint64_t>& table_ids, ObIArray<ColumnItem>& column_items) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
    ret = get_column_items(table_ids.at(i), column_items);
  }
  return ret;
}

int ObDMLStmt::get_column_items(uint64_t table_id, ObIArray<ColumnItem>& column_items) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    if (table_id == column_items_.at(i).table_id_) {
      ret = column_items.push_back(column_items_.at(i));
    }
  }
  return ret;
}

int ObDMLStmt::get_column_exprs(ObIArray<ObColumnRefRawExpr*>& column_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); ++i) {
    ret = column_exprs.push_back(column_items_.at(i).expr_);
  }
  return ret;
}

int ObDMLStmt::get_column_exprs(ObIArray<ObRawExpr*>& column_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); ++i) {
    ret = column_exprs.push_back(column_items_.at(i).expr_);
  }
  return ret;
}

int ObDMLStmt::get_view_output(
    const TableItem& table, ObIArray<ObRawExpr*>& select_list, ObIArray<ObRawExpr*>& column_list) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 4> columns;
  if (OB_UNLIKELY(!table.is_generated_table()) || OB_ISNULL(table.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the table is not a generated table", K(ret));
  } else if (OB_FAIL(get_column_exprs(table.table_id_, columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(column_list, columns))) {
    LOG_WARN("failed to append columns", K(ret));
  } else if (OB_FAIL(table.ref_query_->get_select_exprs(select_list))) {
    LOG_WARN("failed to get select list", K(ret));
  } else if (OB_UNLIKELY(select_list.count() != column_list.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the select list does match the column list", K(ret), K(select_list.count()), K(column_list.count()));
  }
  return ret;
}

int32_t ObDMLStmt::get_table_bit_index(uint64_t table_id) const
{
  int64_t idx = tables_hash_.get_idx(table_id, OB_INVALID_ID);
  return static_cast<int32_t>(idx);
}

int ObDMLStmt::set_table_bit_index(uint64_t table_id)
{
  return tables_hash_.add_column_desc(table_id, OB_INVALID_ID);
}

ColumnItem* ObDMLStmt::get_column_item(uint64_t table_id, const ObString& col_name)
{
  ColumnItem* item = NULL;
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  if (share::is_oracle_mode()) {
    cs_type = common::CS_TYPE_UTF8MB4_BIN;
  }
  for (int64_t i = 0; i < column_items_.count(); ++i) {
    if (table_id == column_items_[i].table_id_ &&
        (0 == ObCharset::strcmp(cs_type, col_name, column_items_[i].column_name_))) {
      item = &column_items_.at(i);
      break;
    }
  }
  return item;
}

int ObDMLStmt::add_column_item(ObIArray<ColumnItem>& column_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); i++) {
    if (OB_FAIL(add_column_item(column_items.at(i)))) {
      LOG_WARN("failed to add column item", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLStmt::add_column_item(ColumnItem& column_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_item.expr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("column item isn't init");
  } else {
    column_item.expr_->set_explicited_reference();
    column_item.expr_->set_expr_level(current_level_);
    column_item.expr_->get_relation_ids().reuse();
    if (OB_FAIL(column_item.expr_->add_relation_id(get_table_bit_index(column_item.expr_->get_table_id())))) {
      LOG_WARN("add relation id to expr failed", K(ret));
    } else if (OB_FAIL(column_items_.push_back(column_item))) {
      LOG_WARN("push back column item failed", K(ret));
    } else {
      LOG_DEBUG("add_column_item", K(column_item), KPC(column_item.expr_));
    }
  }
  return ret;
}

int ObDMLStmt::remove_column_item(uint64_t table_id, uint64_t column_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    if (table_id == column_items_.at(i).table_id_ && column_id == column_items_.at(i).column_id_) {
      if (OB_FAIL(column_items_.remove(i))) {
        LOG_WARN("failed to remove column_items", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObDMLStmt::remove_column_item(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = column_items_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (table_id == column_items_.at(i).table_id_) {
      if (OB_FAIL(column_items_.remove(i))) {
        LOG_WARN("failed to remove column item", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::remove_column_item(const ObRawExpr* column_expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    if (column_expr == column_items_.at(i).expr_) {
      if (OB_FAIL(column_items_.remove(i))) {
        LOG_WARN("failed to remove column_items", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObDMLStmt::remove_column_item(const ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    ret = remove_column_item(column_exprs.at(i));
  }
  return ret;
}

int ObDMLStmt::add_subquery_ref(ObQueryRefRawExpr* query_ref)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ref)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null");
  } else if (OB_FAIL(subquery_exprs_.push_back(query_ref))) {
    LOG_WARN("push back subquery ref failed", K(ret));
  } else {
    query_ref->set_explicited_reference();
    query_ref->set_ref_id(subquery_exprs_.count() - 1);
  }
  return ret;
}

int ObDMLStmt::get_child_stmts(ObIArray<ObSelectStmt*>& child_stmts) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_table_size(); ++i) {
    const TableItem* table_item = get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item is null", K(i));
    } else if (table_item->is_generated_table()) {
      if (OB_FAIL(child_stmts.push_back(table_item->ref_query_))) {
        LOG_WARN("store child stmt failed", K(ret));
      }
    }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < get_subquery_expr_size(); ++j) {
    ObQueryRefRawExpr* subquery_ref = subquery_exprs_.at(j);
    if (OB_ISNULL(subquery_ref)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery reference is null", K(subquery_ref));
    } else if (subquery_ref->get_ref_stmt() != NULL) {
      if (OB_FAIL(child_stmts.push_back(subquery_ref->get_ref_stmt()))) {
        LOG_WARN("stored subquery reference stmt failed", K(ret));
      }
    } else if (subquery_ref->get_ref_operator() != NULL) {
      ObDMLStmt* stmt = subquery_ref->get_ref_operator()->get_stmt();
      if (OB_ISNULL(stmt) || OB_UNLIKELY(!stmt->is_select_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is invalid", K(stmt));
      } else if (OB_FAIL(child_stmts.push_back(static_cast<ObSelectStmt*>(stmt)))) {
        LOG_WARN("stored subquery reference stmt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::set_child_stmt(const int64_t child_num, ObSelectStmt* child_stmt)
{
  int ret = OB_SUCCESS;
  int64_t child_size = 0;
  if (OB_ISNULL(child_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_FAIL(get_child_stmt_size(child_size))) {
    LOG_WARN("failed to get child size", K(ret));
  } else if (OB_UNLIKELY(child_num < 0) || OB_UNLIKELY(child_num >= child_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child number", K(child_num), K(child_size), K(ret));
  } else {
    int pos = 0;
    bool is_find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_find && i < get_table_size(); ++i) {
      TableItem* table_item = get_table_item(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is null", K(i));
      } else if (table_item->is_generated_table()) {
        if (child_num == pos) {
          is_find = true;
          table_item->ref_query_ = child_stmt;
        } else {
          pos++;
        }
      } else { /*do nothing*/
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < get_subquery_expr_size(); ++j) {
      ObQueryRefRawExpr* subquery_ref = subquery_exprs_.at(j);
      if (OB_ISNULL(subquery_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subquery reference is null", K(subquery_ref));
      } else if (subquery_ref->get_ref_stmt() != NULL) {
        if (child_num == pos) {
          is_find = true;
          subquery_ref->set_ref_stmt(child_stmt);
        } else {
          pos++;
        }
      } else if (subquery_ref->get_ref_operator() != NULL) {
        if (child_num == pos) {
          is_find = true;
          subquery_ref->get_ref_operator()->set_stmt(child_stmt);
        } else {
          pos++;
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_child_stmt_size(int64_t& child_size) const
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_ERROR("get child stmts failed", K(ret));
  } else {
    child_size = child_stmts.count();
  }
  return ret;
}

bool ObDMLStmt::has_link_table() const
{
  bool bret = false;
  for (int i = 0; !bret && i < table_items_.count(); i++) {
    if (OB_NOT_NULL(table_items_.at(i))) {
      if (table_items_.at(i)->is_generated_table()) {
        if (OB_NOT_NULL(table_items_.at(i)->ref_query_)) {
          bret = table_items_.at(i)->ref_query_->has_link_table();
        }
      } else {
        bret = table_items_.at(i)->is_link_table();
      }
    }
  }
  return bret;
}

int ObDMLStmt::get_relation_exprs(ObIArray<ObRawExpr*>& rel_array, int32_t ignore_scope /* = 0*/) const
{
  int ret = OB_SUCCESS;
  FastRelExprChecker expr_checker(rel_array, ignore_scope);
  if (OB_FAIL(const_cast<ObDMLStmt*>(this)->inner_get_relation_exprs(expr_checker))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }
  return ret;
}

int ObDMLStmt::get_relation_exprs(ObIArray<ObRawExprPointer>& rel_array, int32_t ignore_scope /* = 0*/)
{
  int ret = OB_SUCCESS;
  RelExprPointerChecker expr_checker(rel_array, ignore_scope);
  if (OB_FAIL(expr_checker.init())) {
    LOG_WARN("init relexpr checker failed", K(ret));
  } else if (OB_FAIL(inner_get_relation_exprs(expr_checker))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }
  return ret;
}

int ObDMLStmt::get_relation_exprs_for_enum_set_wrapper(ObIArray<ObRawExpr*>& rel_array)
{
  int ret = OB_SUCCESS;
  RelExprChecker expr_checker(rel_array);
  if (OB_FAIL(expr_checker.init())) {
    LOG_WARN("init expr checker failed", K(ret));
  } else if (OB_FAIL(inner_get_relation_exprs_for_wrapper(expr_checker))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }
  return ret;
}

const TableItem* ObDMLStmt::get_table_item_in_all_namespace(uint64_t table_id) const
{
  const TableItem* table_item = NULL;
  const ObDMLStmt* cur_stmt = this;
  do {
    if (NULL != (table_item = cur_stmt->get_table_item_by_id(table_id))) {
      break;
    }
    cur_stmt = cur_stmt->parent_namespace_stmt_;
  } while (cur_stmt != NULL);

  return table_item;
}

ColumnItem* ObDMLStmt::get_column_item_by_id(uint64_t table_id, uint64_t column_id)
{
  ColumnItem* column_item = NULL;
  int64_t num = column_items_.count();
  for (int64_t i = 0; i < num; i++) {
    if (table_id == column_items_[i].table_id_ && column_id == column_items_[i].column_id_) {
      column_item = &column_items_.at(i);
      break;
    }
  }
  if (NULL == column_item && NULL != parent_namespace_stmt_) {
    column_item = parent_namespace_stmt_->get_column_item_by_id(table_id, column_id);
  }
  return column_item;
}

ObColumnRefRawExpr* ObDMLStmt::get_column_expr_by_id(uint64_t table_id, uint64_t column_id)
{
  ObColumnRefRawExpr* ref_expr = NULL;
  ColumnItem* column_item = get_column_item_by_id(table_id, column_id);
  if (column_item != NULL) {
    ref_expr = column_item->expr_;
  }
  return ref_expr;
}

const ColumnItem* ObDMLStmt::get_column_item_by_id(uint64_t table_id, uint64_t column_id) const
{
  const ColumnItem* column_item = NULL;
  int64_t num = column_items_.count();
  for (int64_t i = 0; i < num; i++) {
    if (table_id == column_items_[i].table_id_ && column_id == column_items_[i].column_id_) {
      column_item = &column_items_[i];
      break;
    }
  }
  if (NULL == column_item && NULL != parent_namespace_stmt_) {
    column_item = parent_namespace_stmt_->get_column_item_by_id(table_id, column_id);
  }
  return column_item;
}

const ObColumnRefRawExpr* ObDMLStmt::get_column_expr_by_id(uint64_t table_id, uint64_t column_id) const
{
  ObColumnRefRawExpr* ref_expr = NULL;
  const ColumnItem* column_item = get_column_item_by_id(table_id, column_id);
  if (column_item != NULL) {
    ref_expr = column_item->expr_;
  }
  return ref_expr;
}

int ObDMLStmt::get_column_exprs(uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& table_cols) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); ++i) {
    if (column_items_.at(i).table_id_ == table_id) {
      if (OB_FAIL(table_cols.push_back(column_items_.at(i).expr_))) {
        LOG_WARN("failed to push back column exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_column_exprs(uint64_t table_id, ObIArray<ObRawExpr*>& table_cols) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); ++i) {
    if (column_items_.at(i).table_id_ == table_id) {
      if (OB_FAIL(table_cols.push_back(column_items_.at(i).expr_))) {
        LOG_WARN("failed to push back column exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_column_exprs(ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& column_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    uint64_t table_id = table_items.at(i)->table_id_;
    for (int64_t j = 0; OB_SUCC(ret) && j < column_items_.count(); ++j) {
      if (column_items_.at(j).table_id_ == table_id) {
        if (OB_FAIL(column_exprs.push_back(column_items_.at(j).expr_))) {
          LOG_WARN("failed to push back column exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::convert_table_ids_to_bitset(
    common::ObIArray<ObTablesIndex>& table_ids, common::ObIArray<ObRelIds>& table_idxs)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
    ObRelIds table_idx;
    ObTablesIndex tableIndex = table_ids.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < tableIndex.indexes_.count(); j++) {
      if (OB_INVALID_INDEX == (idx = get_table_bit_index(tableIndex.indexes_.at(j)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get_table_bit_index", "table_id:", tableIndex.indexes_.at(j), K(ret));
      } else if (OB_FAIL(table_idx.add_member(idx))) {
        LOG_WARN("failed to add member", K(idx), K(ret));
      } else {
      }  // do nothing.
    }
    if (OB_FAIL(table_idxs.push_back(table_idx))) {
      LOG_WARN("failed to push back table idxs.", K(ret));
    } else {
    }  // do nothing.
  }
  return ret;
}

int ObDMLStmt::convert_table_ids_to_bitset(common::ObIArray<ObPQMapHint>& table_ids, ObRelIds& table_idxs)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
    uint64_t table_id = table_ids.at(i).table_id_;
    if (OB_INVALID_INDEX == (idx = get_table_bit_index(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get_table_bit_index", K(table_id), K(ret));
    } else if (OB_FAIL(table_idxs.add_member(idx))) {
      LOG_WARN("failed to add member", K(idx), K(ret));
    } else {
    }  // do nothing.
  }
  return ret;
}

int ObDMLStmt::convert_table_ids_to_bitset(
    common::ObIArray<ObPQDistributeHint>& hints_ids, common::ObIArray<ObPQDistributeIndex>& hint_idxs)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < hints_ids.count(); i++) {
    ObPQDistributeIndex* pq_distribute_index = NULL;
    if (OB_UNLIKELY(NULL == (pq_distribute_index = hint_idxs.alloc_place_holder()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObTablesInHint from array error", K(ret));
    } else {
      *(ObPQDistributeHintMethod*)pq_distribute_index = hints_ids.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < hints_ids.at(i).table_ids_.count(); j++) {
        if (OB_INVALID_INDEX == (idx = get_table_bit_index(hints_ids.at(i).table_ids_.at(j)))) {
          // ret = OB_ERR_UNEXPECTED;
          // LOG_WARN("failed to get_table_bit_index", "table_id:", hints_ids.at(i).table_ids_.at(j), K(ret));
        } else if (OB_FAIL(pq_distribute_index->rel_ids_.add_member(idx))) {
          LOG_WARN("failed to add member", K(idx), K(ret));
        }
      }
    }
  }
  return ret;
}

int64_t ObDMLStmt::get_from_item_idx(uint64_t table_id) const
{
  int64_t idx = -1;
  for (int64_t i = 0; i < from_items_.count(); ++i) {
    if (table_id == from_items_.at(i).table_id_) {
      idx = i;
      break;
    }
  }
  return idx;
}

///////////functions for sql hint/////////////
// Allow database_name is empty.
int ObDMLStmt::find_table_item(const ObSQLSessionInfo& session_info, const ObString& database_name,
    const ObString& table_name, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  table_item = NULL;
  bool finish = false;
  TableItem* item = NULL;
  int64_t num = table_items_.count();
  for (int64_t i = 0; OB_SUCC(ret) && !finish && i < num; i++) {
    bool is_equal = true;
    if (OB_ISNULL(item = table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null");
    } else if (!database_name.empty() && OB_FAIL(session_info.name_case_cmp(
                                             database_name, item->database_name_, OB_TABLE_NAME_CLASS, is_equal))) {
      LOG_WARN("fail to compare db names", K(database_name), K_(item->database_name), K(ret));
    } else if (!is_equal) {
      /* do nothing */
    } else if (OB_FAIL(
                   session_info.name_case_cmp(table_name, item->get_object_name(), OB_TABLE_NAME_CLASS, is_equal))) {
      LOG_WARN("Failed to compare name", K(table_name), KPC(item), K(ret));
    } else if (!is_equal) {
      /* do nothing */
    } else if (!database_name.empty()) {
      table_item = item;
      finish = true;
    } else if (NULL == table_item) {
      table_item = item;
    } else {
      table_item = NULL;
      finish = true;
    }
  }
  return ret;
}

int ObDMLStmt::get_table_id(const ObSQLSessionInfo& session_info, const common::ObString& stmt_name,
    const common::ObString& database_name, const common::ObString& table_name, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt_name);
  table_id = OB_INVALID_ID;
  TableItem* table_item = NULL;
  if (OB_FAIL(find_table_item(session_info, database_name, table_name, table_item))) {
    LOG_WARN("Failed to find table item", K(database_name), K(table_name), K(ret));
  } else if (NULL != table_item) {
    table_id = table_item->table_id_;
  } else { /* do nothing */
  }
  return ret;
}

int ObDMLStmt::check_if_contain_inner_table(bool& is_contain_inner_table) const
{
  int ret = OB_SUCCESS;
  is_contain_inner_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain_inner_table && i < table_items_.count(); ++i) {
    TableItem* table_item = table_items_.at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table item is NULL", K(ret), K(i), K(table_items_.count()));
    } else if (is_inner_table(table_item->ref_id_)) {
      is_contain_inner_table = true;
    }
  }
  if (OB_SUCC(ret) && !is_contain_inner_table) {
    ObSEArray<ObSelectStmt*, 16> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_ERROR("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !is_contain_inner_table && i < child_stmts.count(); ++i) {
        ObSelectStmt* sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sub stmt is null", K(ret));
        } else if (OB_FAIL(SMART_CALL(sub_stmt->check_if_contain_inner_table(is_contain_inner_table)))) {
          LOG_WARN("check sub stmt whether has is table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::has_for_update(bool& has) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < table_items_.count(); ++i) {
    TableItem* table_item = table_items_.at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is NULL", K(ret), K(i), K(table_items_.count()));
    } else if (table_item->for_update_) {
      has = true;
    }
  }
  return ret;
}

int ObDMLStmt::check_if_contain_select_for_update(bool& is_contain_select_for_update) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 16> child_stmts;
  is_contain_select_for_update = false;
  if (OB_FAIL(has_for_update(is_contain_select_for_update))) {
    LOG_WARN("failed to check for update", K(ret));
  } else if (is_contain_select_for_update) {
    // do nothing
  } else if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmts failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain_select_for_update && i < child_stmts.count(); ++i) {
    ObSelectStmt* sub_stmt = child_stmts.at(i);
    if (OB_ISNULL(sub_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub stmt is null", K(ret));
    } else if (OB_FAIL(SMART_CALL(sub_stmt->check_if_contain_select_for_update(is_contain_select_for_update)))) {
      LOG_WARN("check sub stmt whether has select for update failed", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::check_if_table_exists(uint64_t table_id, bool& is_existed) const
{
  int ret = OB_SUCCESS;
  is_existed = false;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < child_stmts.count(); i++) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is null.", K(ret));
      } else if (OB_FAIL(SMART_CALL(child_stmts.at(i)->check_if_table_exists(table_id, is_existed)))) {
        LOG_WARN("failed to check if all virtual tables.", K(ret));
      } else { /* do nothing. */
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < table_items_.count(); i++) {
      if (OB_ISNULL(table_items_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null.", K(ret));
      } else if (table_id == table_items_.at(i)->ref_id_) {
        is_existed = true;
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObDMLStmt::has_special_expr(const ObExprInfoFlag flag, bool& has) const
{
  int ret = OB_SUCCESS;
  has = false;
  // where statement
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < condition_exprs_.count(); i++) {
    const ObRawExpr* expr = condition_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Condition expr is null", K(ret));
    } else if (expr->has_flag(flag)) {
      has = true;
    }
  }
  // order statement
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < order_items_.count(); i++) {
    const ObRawExpr* expr = order_items_.at(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Order item expr is null", K(ret));
    } else if (expr->has_flag(flag)) {
      has = true;
    }
  }
  // joined table
  for (int64_t i = 0; OB_SUCC(ret) && i < get_joined_tables().count(); ++i) {
    const JoinedTable* joined_table = NULL;
    const ObRawExpr* expr = NULL;
    if (OB_ISNULL(joined_table = get_joined_tables().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null joined table", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < joined_table->get_join_conditions().count(); j++) {
        if (OB_ISNULL(expr = joined_table->get_join_conditions().at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Condition expr is null", K(ret));
        } else if (expr->has_flag(flag)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::pull_up_object_ids()
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  const bool error_with_exist = false;
  if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("fail to get child stmts", K(ret));
  }

  // execute child pull object ids
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObSelectStmt* cur_stmt = child_stmts.at(i);
    if (OB_ISNULL(cur_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur stmt is NULL", K(i), K(ret));
    } else if (OB_FAIL(SMART_CALL(cur_stmt->pull_up_object_ids()))) {
      LOG_WARN("fail to pull up object ids", K(i), K(ret));
    }
  }
  // pull child object ids to current stmt
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObSelectStmt* cur_stmt = child_stmts.at(i);
    if (OB_ISNULL(cur_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur stmt is NULL", K(i), K(ret));
    } else if (OB_FAIL(add_view_table_ids(cur_stmt->get_view_table_id_store()))) {
      LOG_WARN("fail to add view table ids", K(i), K(ret));
    } else if (OB_FAIL(add_synonym_ids(cur_stmt->get_synonym_id_store(), error_with_exist))) {
      LOG_WARN("fail to add synonym ids", K(i), K(error_with_exist), K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::push_down_query_hint()
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmt failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObSelectStmt* sub_stmt = child_stmts.at(i);
    if (OB_ISNULL(sub_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Sub-stmt should not be NULL", K(i), K(ret));
    } else if (OB_FAIL(ObDMLStmt::copy_query_hint(this, sub_stmt))) {
      LOG_WARN("failed to copy query hint", K(ret));
    } else if (OB_FAIL(SMART_CALL(sub_stmt->push_down_query_hint()))) {
      LOG_WARN("Failed to reset child hint", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::copy_query_hint(ObDMLStmt* from, ObDMLStmt* to)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(from), K(to));
  } else {
    to->get_stmt_hint().frozen_version_ = from->get_stmt_hint().frozen_version_;
    to->get_stmt_hint().topk_precision_ = from->get_stmt_hint().topk_precision_;
    to->get_stmt_hint().use_jit_policy_ = from->get_stmt_hint().use_jit_policy_;
    to->get_stmt_hint().force_trace_log_ = from->get_stmt_hint().force_trace_log_;
    to->get_stmt_hint().read_consistency_ = from->get_stmt_hint().read_consistency_;
    to->get_stmt_hint().bnl_allowed_ = from->get_stmt_hint().bnl_allowed_;
    to->get_stmt_hint().query_timeout_ = from->get_stmt_hint().query_timeout_;
    to->get_stmt_hint().use_px_ = from->get_stmt_hint().use_px_;
    to->get_stmt_hint().log_level_ = from->get_stmt_hint().log_level_;
    to->get_stmt_hint().plan_cache_policy_ = from->get_stmt_hint().plan_cache_policy_;
    if (from->get_stmt_hint().parallel_ != ObStmtHint::UNSET_PARALLEL) {
      to->get_stmt_hint().parallel_ = from->get_stmt_hint().parallel_;
    }
  }
  return ret;
}

int ObDMLStmt::rebuild_tables_hash()
{
  int ret = OB_SUCCESS;
  TableItem* ti = NULL;
  ObSEArray<uint64_t, 4> table_id_list;
  ObSEArray<int64_t, 4> bit_index_map;
  // dump old table id - rel id map
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_hash_.get_column_num(); ++i) {
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    if (OB_FAIL(tables_hash_.get_tid_cid(i, tid, cid))) {
      LOG_WARN("failed to get tid cid", K(ret));
    } else if (OB_FAIL(table_id_list.push_back(tid))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  }
  tables_hash_.reset();
  if (OB_FAIL(set_table_bit_index(OB_INVALID_ID))) {
    LOG_WARN("fail to add table_id to hash table", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
    if (OB_ISNULL(ti = table_items_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(table_items_), K(ret));
    } else if (OB_FAIL(set_table_bit_index(ti->table_id_))) {
      LOG_WARN("fail to add table_id to hash table", K(ret), K(ti));
    }
  }
  // create old rel id - new rel id map
  for (int64_t i = 0; OB_SUCC(ret) && i < table_id_list.count(); ++i) {
    if (OB_FAIL(bit_index_map.push_back(get_table_bit_index(table_id_list.at(i))))) {
      LOG_WARN("failed to push back new bit index", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::update_rel_ids(ObRelIds& rel_ids, const ObIArray<int64_t>& bit_index_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> old_bit_index;
  if (OB_FAIL(rel_ids.to_array(old_bit_index))) {
    LOG_WARN("failed to convert bit set to bit index", K(ret));
  } else {
    rel_ids.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_bit_index.count(); ++i) {
    int64_t old_index = old_bit_index.at(i);
    int64_t new_index = bit_index_map.at(old_index);
    if (OB_INVALID_INDEX == new_index) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table index is invalid", K(ret), K(old_index));
    } else if (OB_FAIL(rel_ids.add_member(new_index))) {
      LOG_WARN("failed to add new table index", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::update_column_item_rel_id()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); i++) {
    ColumnItem& ci = column_items_.at(i);
    if (OB_ISNULL(ci.expr_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ci));
    } else {
      ci.expr_->get_relation_ids().reuse();
      int64_t rel_id = get_table_bit_index(ci.expr_->get_table_id());
      if (rel_id <= 0 || rel_id > table_items_.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(rel_id), K(table_items_.count()));
      } else if (OB_FAIL(ci.expr_->add_relation_id(rel_id))) {
        LOG_WARN("fail to add relation id", K(rel_id), K(ret));
      }
    }
  }

  return ret;
}

int ObDMLStmt::reset_from_item(const common::ObIArray<FromItem>& from_items)
{
  return from_items_.assign(from_items);
}

int ObDMLStmt::reset_table_item(const common::ObIArray<TableItem*>& table_items)
{
  return table_items_.assign(table_items);
}

void ObDMLStmt::clear_column_items()
{
  column_items_.reset();
}

int ObDMLStmt::disable_px_hint()
{
  int ret = OB_SUCCESS;
  stmt_hint_.use_px_ = ObUsePxHint::DISABLE;
  common::ObArray<ObSelectStmt*> stmts;
  if (OB_FAIL(get_child_stmts(stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); ++i) {
    if (OB_ISNULL(stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(stmts.at(i)->disable_px_hint()))) {
      LOG_WARN("failed to disable px hint for child stmt", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::remove_subquery_expr(const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); i++) {
      if (expr == subquery_exprs_.at(i)) {
        if (OB_FAIL(subquery_exprs_.remove(i))) {
          LOG_WARN("failed to remove expr", K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

///////////end of functions for sql hint/////////////

/*
 * extract all query_ref_expr from stmt's relation exprs, and push them into stmt's query_ref_exprs_
 */
int ObDMLStmt::adjust_subquery_list()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> relation_exprs;
  ObSEArray<ObRawExpr*, 8> nest_sunquery_exprs;
  if (OB_FAIL(get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (FALSE_IT(subquery_exprs_.reset())) {
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(relation_exprs, subquery_exprs_))) {
    LOG_WARN("failed to extract query ref expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::extract_target_level_query_ref_expr(
                 subquery_exprs_, current_level_, subquery_exprs_, nest_sunquery_exprs))) {
    LOG_WARN("failed to extract level query ref expr", K(ret));
  } else {
    ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < nest_sunquery_exprs.count(); ++i) {
      if (OB_ISNULL(expr = nest_sunquery_exprs.at(i)) || !expr->is_query_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret));
      } else if (OB_FAIL(subquery_exprs_.push_back(dynamic_cast<ObQueryRefRawExpr*>(expr)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::contain_hierarchical_query(bool& contain_hie_query) const
{
  int ret = OB_SUCCESS;
  contain_hie_query |= is_hierarchical_query();
  if (!contain_hie_query) {
    ObSEArray<ObSelectStmt*, 16> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_ERROR("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !contain_hie_query && i < child_stmts.count(); ++i) {
        ObSelectStmt* sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sub stmt is null", K(ret));
        } else if (OB_FAIL(SMART_CALL(sub_stmt->contain_hierarchical_query(contain_hie_query)))) {
          LOG_WARN("check sub stmt whether has is table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_stmt_equal_sets(
    EqualSets& equal_sets, ObIAllocator& allocator, const bool is_strict, const int check_scope)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> equal_set_conditions;
  if (OB_FAIL(get_equal_set_conditions(equal_set_conditions, is_strict, check_scope))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (equal_set_conditions.count() > 0) {
    if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&allocator, equal_set_conditions, equal_sets))) {
      LOG_WARN("failed to compute equal set", K(ret));
    }
  } else { /*do nothing*/
  }
  return ret;
}

// equal set conditions:
// 1. where conditions
// 2. join conditions in joined tables
// 3. join conditions in semi infos
int ObDMLStmt::get_equal_set_conditions(ObIArray<ObRawExpr*>& conditions, const bool is_strict, const int check_scope)
{
  int ret = OB_SUCCESS;
  bool check_where = check_scope & SCOPE_WHERE;
  if (!check_where) {
  } else if (OB_FAIL(append(conditions, condition_exprs_))) {
    LOG_WARN("failed to append conditions", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables_.count(); ++i) {
      if (OB_FAIL(extract_equal_condition_from_joined_table(joined_tables_.at(i), conditions, is_strict))) {
        LOG_WARN("failed to extract equal condition from joined table", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos_.count(); ++i) {
      if (OB_ISNULL(semi_infos_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (semi_infos_.at(i)->is_anti_join()) {
        /* do nothing */
      } else if (OB_FAIL(append(conditions, semi_infos_.at(i)->semi_conditions_))) {
        LOG_WARN("failed to append conditions", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::extract_equal_condition_from_joined_table(
    const TableItem* table, ObIArray<ObRawExpr*>& conditions, const bool is_strict)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  bool check_left = false;
  bool check_right = false;
  bool check_this = false;

  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table item", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable* joined_table = static_cast<const JoinedTable*>(table);
    if (joined_table->is_inner_join()) {
      check_left = true;
      check_right = true;
      check_this = true;
    } else if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
      check_left = true;
      check_right = !is_strict;
      check_this = !is_strict;
    } else if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      check_left = !is_strict;
      check_right = true;
      check_this = !is_strict;
    } else {
      check_left = !is_strict;
      check_right = !is_strict;
      check_this = !is_strict;
    }

    if (check_this) {
      for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->get_join_conditions().count(); ++i) {
        if (OB_FAIL(conditions.push_back(joined_table->get_join_conditions().at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (check_left && OB_FAIL(SMART_CALL(extract_equal_condition_from_joined_table(
                                 joined_table->left_table_, conditions, is_strict)))) {
      LOG_WARN("failed to extract equal condition from join table", K(ret));
    } else if (check_right && OB_FAIL(SMART_CALL(extract_equal_condition_from_joined_table(
                                  joined_table->right_table_, conditions, is_strict)))) {
      LOG_WARN("failed to extract equal condition from join table", K(ret));
    }
  } else { /* do nothing */
  }
  return ret;
}

int ObDMLStmt::get_rownum_expr(ObRawExpr*& expr) const
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObRawExpr* cur_expr = NULL;
  bool find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < pseudo_column_like_exprs_.count(); ++i) {
    if (OB_ISNULL(cur_expr = pseudo_column_like_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (T_FUN_SYS_ROWNUM == cur_expr->get_expr_type()) {
      expr = cur_expr;
      find = true;
    }
  }
  return ret;
}

int ObDMLStmt::has_rownum(bool& has_rownum) const
{
  int ret = OB_SUCCESS;
  ObRawExpr* rownum_expr = NULL;
  if (OB_FAIL(get_rownum_expr(rownum_expr))) {
    LOG_WARN("failed to get rownum expr", K(ret));
  } else {
    has_rownum = (rownum_expr != NULL);
  }
  return ret;
}

int ObDMLStmt::get_sequence_expr(ObRawExpr*& expr,
    const ObString seq_name,    // sequence object name
    const ObString seq_action,  // NEXTVAL or CURRVAL
    const uint64_t seq_id) const
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObRawExpr* cur_expr = NULL;
  bool find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < pseudo_column_like_exprs_.count(); ++i) {
    if (OB_ISNULL(cur_expr = pseudo_column_like_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (T_FUN_SYS_SEQ_NEXTVAL == cur_expr->get_expr_type()) {
      ObSequenceRawExpr* seq_expr = static_cast<ObSequenceRawExpr*>(cur_expr);
      if (seq_expr->get_name() == seq_name && seq_expr->get_action() == seq_action &&
          seq_expr->get_sequence_id() == seq_id) {
        expr = cur_expr;
        find = true;
      }
    }
  }
  return ret;
}

int ObDMLStmt::check_rowid_column_exists(const uint64_t table_id, bool& is_contain_rowid)
{
  int ret = OB_SUCCESS;
  is_contain_rowid = false;
  ObArray<ObColumnRefRawExpr*> column_exprs;
  if (OB_FAIL(get_column_exprs(table_id, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && !is_contain_rowid && i < column_exprs.count(); i++) {
      if (OB_ISNULL(column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column expr", K(ret));
      } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_exprs.at(i)->get_column_id()) {
        is_contain_rowid = true;
      }
    }  // for end
  }
  return ret;
}

int ObDMLStmt::mark_share_exprs() const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> share_exprs;
  if (OB_FAIL(inner_get_share_exprs(share_exprs))) {
    LOG_WARN("failed to inner get share exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < share_exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = share_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("share expr is null", K(ret));
    } else if (OB_FAIL(expr->add_flag(IS_SHARED_REF))) {
      LOG_WARN("failed to add flag", K(ret));
    }
  }
  return ret;
}

int ObDMLStmt::inner_get_share_exprs(ObIArray<ObRawExpr*>& candi_share_exprs) const
{
  int ret = OB_SUCCESS;
  /**
   * column, query ref, pseudo_column can be shared
   */
  if (OB_FAIL(get_column_exprs(candi_share_exprs))) {
    LOG_WARN("failed to append column exprs", K(ret));
  } else if (OB_FAIL(append(candi_share_exprs, get_pseudo_column_like_exprs()))) {
    LOG_WARN("failed to append pseduo column like exprs", K(ret));
  } else if (OB_FAIL(append(candi_share_exprs, get_subquery_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  }
  return ret;
}

int ObDMLStmt::has_ref_assign_user_var(bool& has_ref_user_var) const
{
  int ret = OB_SUCCESS;
  has_ref_user_var = false;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (query_ctx_->all_user_variable_.empty()) {
    // do nothing
  } else {
    // quick check
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < query_ctx_->all_user_variable_.count(); ++i) {
      const ObUserVarIdentRawExpr* cur_expr = query_ctx_->all_user_variable_.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (cur_expr->get_is_contain_assign() || cur_expr->get_query_has_udf()) {
        find = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (!find) {
        // no user variable assignment in query
      } else if (OB_FAIL(recursive_check_has_ref_assign_user_var(has_ref_user_var))) {
        LOG_WARN("failed to recursive check has assignment ref user var", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::recursive_check_has_ref_assign_user_var(bool& has_ref_user_var) const
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  has_ref_user_var = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_ref_user_var && i < get_user_var_size(); ++i) {
    const ObUserVarIdentRawExpr* cur_expr = get_user_vars().at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (cur_expr->get_is_contain_assign() || cur_expr->get_query_has_udf()) {
      has_ref_user_var = true;
    }
  }
  if (OB_SUCC(ret) && !has_ref_user_var) {
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !has_ref_user_var && i < child_stmts.count(); ++i) {
        if (OB_ISNULL(child_stmts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (child_stmts.at(i)->recursive_check_has_ref_assign_user_var(has_ref_user_var)) {
          LOG_WARN("failed to recursive check has assignment ref user var", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLStmt::get_temp_table_ids(ObIArray<uint64_t>& temp_table_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 8> child_stmts;
  if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(child_stmts.at(i)->get_temp_table_ids(temp_table_ids)))) {
        LOG_WARN("failed to get temp table ids", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      if (OB_ISNULL(table_items_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!table_items_.at(i)->is_temp_table()) {
        /*do nothing*/
      } else if (OB_FAIL(add_var_to_array_no_dup(temp_table_ids, table_items_.at(i)->ref_id_))) {
        LOG_WARN("failed to add to array without duplicates", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObDMLStmt::reset_statement_id(const ObDMLStmt& other)
{
  int ret = OB_SUCCESS;
  stmt_id_ = other.stmt_id_;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  ObSEArray<ObSelectStmt*, 4> other_child_stmts;
  ObSelectStmt* child_stmt = NULL;
  ObSelectStmt* other_child_stmt = NULL;
  if (OB_FAIL(get_child_stmts(child_stmts)) || OB_FAIL(other.get_child_stmts(other_child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else if (OB_UNLIKELY(child_stmts.count() != other_child_stmts.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected child stmt count", K(ret), K(child_stmts.count()), K(other_child_stmts.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmt = child_stmts.at(i)) || OB_ISNULL(other_child_stmt = other_child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(child_stmts.at(i)->reset_statement_id(*other_child_stmt)))) {
        LOG_WARN("failed to reset statement id", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmt::check_pseudo_column_exist(ObItemType type, ObPseudoColumnRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < pseudo_column_like_exprs_.count(); ++i) {
    ObRawExpr* pseudo_column_expr = pseudo_column_like_exprs_.at(i);
    if (OB_ISNULL(pseudo_column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(i), K(ret));
    } else if (pseudo_column_expr->get_expr_type() == type) {
      if (OB_LIKELY(pseudo_column_expr->is_pseudo_column_expr())) {
        expr = static_cast<ObPseudoColumnRawExpr*>(pseudo_column_expr);
        found = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr should be ObPseudoColumnRawExpr", K(ret), K(*pseudo_column_expr));
      }
    }
  }
  return ret;
}
