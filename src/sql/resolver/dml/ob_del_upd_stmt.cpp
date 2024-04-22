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
#include "ob_del_upd_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObAssignment::deep_copy(ObIRawExprCopier &expr_copier,
                            const ObAssignment &other)
{
  int ret = OB_SUCCESS;
  ObRawExpr *new_col = NULL;
  is_duplicated_ = other.is_duplicated_;
  is_implicit_ = other.is_implicit_;
  is_predicate_column_ = other.is_predicate_column_;
  if (OB_FAIL(expr_copier.copy(other.column_expr_, new_col))) {
    LOG_WARN("failed to copy column expr", K(ret));
  } else if (OB_ISNULL(new_col) || OB_UNLIKELY(!new_col->is_column_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid new column expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.expr_, expr_))) {
    LOG_WARN("failed to copy assign expr", K(ret));
  } else {
    column_expr_ = static_cast<ObColumnRefRawExpr *>(new_col);
  }
  return ret;
}

int ObAssignment::assign(const ObAssignment &other)
{
  int ret = OB_SUCCESS;
  column_expr_ = other.column_expr_;
  expr_ = other.expr_;
  is_duplicated_ = other.is_duplicated_;
  is_implicit_ = other.is_implicit_;
  is_predicate_column_ = other.is_predicate_column_;
  return ret;
}

int ObDmlTableInfo::assign(const ObDmlTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_exprs_.assign(other.column_exprs_))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(check_constraint_exprs_.assign(other.check_constraint_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(view_check_exprs_.assign(other.view_check_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(part_ids_.assign(other.part_ids_))) {
    LOG_WARN("failed to assign part ids", K(ret));
  } else {
    table_id_ = other.table_id_;
    loc_table_id_ = other.loc_table_id_;
    ref_table_id_ = other.ref_table_id_;
    table_name_ = other.table_name_;
    table_type_ = other.table_type_;
    is_link_table_ = other.is_link_table_;
    need_filter_null_ = other.need_filter_null_;
  }
  return ret;
}

int ObDmlTableInfo::deep_copy(ObIRawExprCopier &expr_copier, const ObDmlTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_copier.copy(other.column_exprs_, column_exprs_))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.check_constraint_exprs_, check_constraint_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.view_check_exprs_, view_check_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(part_ids_.assign(other.part_ids_))) {
    LOG_WARN("failed to assign part ids", K(ret));
  } else {
    table_id_ = other.table_id_;
    loc_table_id_ = other.loc_table_id_;
    ref_table_id_ = other.ref_table_id_;
    table_name_ = other.table_name_;
    table_type_ = other.table_type_;
    is_link_table_ = other.is_link_table_;
    need_filter_null_ = other.need_filter_null_;
  }
  return ret;
}

int ObDmlTableInfo::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(visitor.visit(check_constraint_exprs_, SCOPE_DML_CONSTRAINT))) {
    LOG_WARN("failed to visit check constraint exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(view_check_exprs_, SCOPE_DML_CONSTRAINT))) {
    LOG_WARN("failed to visit view check constraint exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(column_exprs_, SCOPE_DML_COLUMN))) {
    LOG_WARN("failed to visit dml column exprs", K(ret));
  }
  return ret;
}

int ObUpdateTableInfo::assign(const ObUpdateTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDmlTableInfo::assign(other))) {
    LOG_WARN("failed to assign base table info", K(ret));
  } else if (OB_FAIL(assignments_.assign(other.assignments_))){
    LOG_WARN("failed to assign assignments", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObUpdateTableInfo::deep_copy(ObIRawExprCopier &expr_copier,
                               const ObUpdateTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDmlTableInfo::deep_copy(expr_copier, other))) {
    LOG_WARN("failed to deep copy base table info", K(ret));
  } else if (OB_FAIL(assignments_.prepare_allocate(other.assignments_.count()))) {
    LOG_WARN("failed to do propare allocate array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assignments_.count(); i++) {
      if (OB_FAIL(assignments_.at(i).deep_copy(expr_copier, other.assignments_.at(i)))) {
        LOG_WARN("failed to deep copy expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObUpdateTableInfo::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDmlTableInfo::iterate_stmt_expr(visitor))) {
    LOG_WARN("failed to iterate stmt expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments_.count(); ++i) {
    ObAssignment &assign = assignments_.at(i);
    if (OB_FAIL(visitor.visit(assign.column_expr_, SCOPE_DML_COLUMN))) {
      LOG_WARN("failed to visit assign column expr", K(ret));
    } else if (OB_FAIL(visitor.visit(assign.expr_, SCOPE_DML_VALUE))) {
      LOG_WARN("failed to visit assign new value", K(ret));
    }
  }
  return ret;
}

int ObInsertTableInfo::assign(const ObInsertTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDmlTableInfo::assign(other))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else if (OB_FAIL(values_desc_.assign(other.values_desc_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(values_vector_.assign(other.values_vector_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(column_conv_exprs_.assign(other.column_conv_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(part_generated_col_dep_cols_.assign(other.part_generated_col_dep_cols_))) {
    LOG_WARN("failed to assign part generated col dep cols", K(ret));
  } else if (OB_FAIL(assignments_.assign(other.assignments_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(column_in_values_vector_.assign(other.column_in_values_vector_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    is_replace_ = other.is_replace_;
  }
  return ret;
}

int ObInsertTableInfo::deep_copy(ObIRawExprCopier &expr_copier,
                                 const ObInsertTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDmlTableInfo::deep_copy(expr_copier, other))) {
    LOG_WARN("failed to deep copy table info", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.values_desc_, values_desc_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.values_vector_, values_vector_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.column_in_values_vector_, column_in_values_vector_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.column_conv_exprs_, column_conv_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.part_generated_col_dep_cols_,
                                      part_generated_col_dep_cols_))) {
    LOG_WARN("failed to copy other.part_generated_col_dep_cols_", K(ret));
  } else if (OB_FAIL(assignments_.prepare_allocate(other.assignments_.count()))) {
    LOG_WARN("failed to do propare allocate array", K(ret));
  } else {
    is_replace_ = other.is_replace_;
    for (int64_t i = 0; OB_SUCC(ret) && i < assignments_.count(); i++) {
      if (OB_FAIL(assignments_.at(i).deep_copy(expr_copier, other.assignments_.at(i)))) {
        LOG_WARN("failed to deep copy expr", K(ret));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObInsertTableInfo::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  int64_t value_desc_cnt = values_desc_.count();
  if (OB_FAIL(ObDmlTableInfo::iterate_stmt_expr(visitor))) {
    LOG_WARN("failed to iterate dml table info", K(ret));
  } else if (OB_FAIL(visitor.visit(values_desc_, SCOPE_INSERT_DESC))) {
    LOG_WARN("failed to iterate valeus desc exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(column_conv_exprs_, SCOPE_DML_VALUE))) {
    LOG_WARN("failed to iterate column conv exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(part_generated_col_dep_cols_, SCOPE_DICT_FIELDS))) {
    LOG_WARN("failed to iterate part generated col dep cols", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value_desc_cnt; ++i) {
    const ObColumnRefRawExpr *col_expr = values_desc_.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (col_expr->is_table_part_key_column()) {
      for (int64_t j = i; OB_SUCC(ret) && j < values_vector_.count(); j += value_desc_cnt) {
        if (OB_FAIL(visitor.visit(values_vector_.at(j), SCOPE_INSERT_VECTOR))) {
          LOG_WARN("add expr to expr checker failed", K(ret), K(i), K(j));
        }
      }
    }
  }
  // !value_from_select() && !subquery_exprs_.empty()
  for (int64_t i = 0; OB_SUCC(ret) && i < values_vector_.count(); ++i) {
    if (OB_ISNULL(values_vector_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if ((values_vector_.at(i)->has_flag(CNT_SUB_QUERY) ||
                values_vector_.at(i)->has_flag(CNT_ONETIME) ||
                values_vector_.at(i)->has_flag(CNT_PL_UDF)) &&
               OB_FAIL(visitor.visit(values_vector_.at(i), SCOPE_INSERT_VECTOR))) {
      LOG_WARN("failed to add expr to expr checker", K(ret));
    } else { /*do nothing*/ }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments_.count(); ++i) {
    ObAssignment &assign = assignments_.at(i);
    if (OB_FAIL(visitor.visit(assign.column_expr_, SCOPE_DML_COLUMN))) {
      LOG_WARN("failed to visit assign column expr", K(ret));
    } else if (OB_FAIL(visitor.visit(assign.expr_, SCOPE_DML_VALUE))) {
      LOG_WARN("failed to visit assign new value", K(ret));
    }
  }
  return ret;
}

int ObMergeTableInfo::assign(const ObMergeTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertTableInfo::assign(other))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else if (OB_FAIL(match_condition_exprs_.assign(other.match_condition_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(insert_condition_exprs_.assign(other.insert_condition_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(update_condition_exprs_.assign(other.update_condition_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(delete_condition_exprs_.assign(other.delete_condition_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    source_table_id_ = other.source_table_id_;
    target_table_id_ = other.target_table_id_;
  }
  return ret;
}

int ObMergeTableInfo::deep_copy(ObIRawExprCopier &expr_copier,
                                const ObMergeTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertTableInfo::deep_copy(expr_copier, other))) {
    LOG_WARN("failed to deep copy table info", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.match_condition_exprs_, match_condition_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.insert_condition_exprs_, insert_condition_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.update_condition_exprs_, update_condition_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.delete_condition_exprs_, delete_condition_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else {
    source_table_id_ = other.source_table_id_;
    target_table_id_ = other.target_table_id_;
  }
  return ret;
}

int ObMergeTableInfo::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertTableInfo::iterate_stmt_expr(visitor))) {
    LOG_WARN("failed to iterate stmt expr", K(ret));
  } else if (OB_FAIL(visitor.visit(match_condition_exprs_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit match condition exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(update_condition_exprs_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit update condition exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(insert_condition_exprs_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit insert condition exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(delete_condition_exprs_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit delete condition exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(values_vector_, SCOPE_INSERT_VECTOR))) {
    LOG_WARN("failed to add expr to expr checker", K(ret));
  }
  return ret;
}

int ObInsertAllTableInfo::assign(const ObInsertAllTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertTableInfo::assign(other))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else if (OB_FAIL(when_cond_exprs_.assign(other.when_cond_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    when_cond_idx_ = other.when_cond_idx_;
  }
  return ret;
}

int ObInsertAllTableInfo::deep_copy(ObIRawExprCopier &expr_copier,
                                    const ObInsertAllTableInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertTableInfo::deep_copy(expr_copier, other))) {
    LOG_WARN("failed to deep copy table info", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.when_cond_exprs_, when_cond_exprs_))) {
    LOG_WARN("failed to copy when condition exprs", K(ret));
  } else {
    when_cond_idx_ = other.when_cond_idx_;
  }
  return ret;
}

int ObInsertAllTableInfo::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertTableInfo::iterate_stmt_expr(visitor))) {
    LOG_WARN("failed to iterate stmt expr", K(ret));
  } else if (OB_FAIL(visitor.visit(when_cond_exprs_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to iterate stmt expr", K(ret));
  }
  return ret;
}

int ObErrLogInfo::assign(const ObErrLogInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(error_log_exprs_.assign(other.error_log_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    is_error_log_ = other.is_error_log_;
    table_id_ = other.table_id_;
    table_name_ = other.table_name_;
    database_name_ = other.database_name_;
    reject_limit_ = other.reject_limit_;
  }
  return ret;
}

int ObErrLogInfo::deep_copy(const ObErrLogInfo &other,
                            ObRawExprCopier &expr_copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_copier.copy(other.error_log_exprs_, error_log_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    is_error_log_ = other.is_error_log_;
    table_id_ = other.table_id_;
    table_name_ = other.table_name_;
    database_name_ = other.database_name_;
    reject_limit_ = other.reject_limit_;
  }
  return ret;
}

int ObDelUpdStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                        ObRawExprCopier &expr_copier,
                                        const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt &other = static_cast<const ObDelUpdStmt &>(input);
  if (OB_UNLIKELY(input.get_stmt_type() != get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret), K(input.get_stmt_type()), K(get_stmt_type()));
  } else if (OB_FAIL(ObDMLStmt::deep_copy_stmt_struct(allocator, expr_copier, other))) {
    LOG_WARN("failed to deep copy stmt structure", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.returning_exprs_,
                                      returning_exprs_))) {
    LOG_WARN("failed to deep copy returning fileds", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.returning_into_exprs_,
                                      returning_into_exprs_))) {
    LOG_WARN("failed to deep copy returning into fields", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.returning_agg_items_,
                                      returning_agg_items_))) {
    LOG_WARN("failed to deep copy returning aggregation exprs", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<OrderItem>(expr_copier,
                                                       other.order_items_,
                                                       order_items_))) {
    LOG_WARN("deep copy order items failed", K(ret));
  } else if (OB_FAIL(returning_strs_.assign(other.returning_strs_))) {
    LOG_WARN("failed to assign returning strings", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.sharding_conditions_, sharding_conditions_))) {
    LOG_WARN("failed to copy sharding conditions", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.ab_stmt_id_expr_, ab_stmt_id_expr_))) {
    LOG_WARN("copy ab_stmt_id_expr_ failed", K(ret));
  } else if (OB_FAIL(error_log_info_.deep_copy(other.error_log_info_, expr_copier))) {
    LOG_WARN("failed to deep copy error log info", K(ret));
  } else {
    ignore_ = other.ignore_;
    has_global_index_ = other.has_global_index_;
    has_instead_of_trigger_ = other.has_instead_of_trigger_;
    pdml_disabled_ = other.pdml_disabled_;
  }
  return ret;
}

int ObDelUpdStmt::assign(const ObDelUpdStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(returning_strs_.assign(other.returning_strs_))) {
    LOG_WARN("failed to assign returning strs", K(ret));
  } else if (OB_FAIL(returning_agg_items_.assign(other.returning_agg_items_))) {
    LOG_WARN("failed to assign returning agg items", K(ret));
  } else if (OB_FAIL(returning_into_exprs_.assign(other.returning_into_exprs_))) {
    LOG_WARN("failed to assign returning into exprs", K(ret));
  } else if (OB_FAIL(error_log_info_.assign(other.error_log_info_))) {
    LOG_WARN("failed to assign error log info", K(ret));
  } else if (OB_FAIL(sharding_conditions_.assign(other.sharding_conditions_))) {
    LOG_WARN("failed to assign sharding conditions", K(ret));
  } else {
    ignore_ = other.ignore_;
    has_global_index_ = other.has_global_index_;
    has_instead_of_trigger_ = other.has_instead_of_trigger_;
    ab_stmt_id_expr_ = other.ab_stmt_id_expr_;
    pdml_disabled_ = other.pdml_disabled_;
  }
  return ret;
}

bool ObDelUpdStmt::is_dml_table_from_join() const
{
  const TableItem *table_item = NULL;
  return get_from_item_size() > 1 ||
         (NULL != (table_item = get_table_item(get_from_item(0))) &&
          !table_item->is_basic_table());
}

int64_t ObDelUpdStmt::get_instead_of_trigger_column_count() const
{
  return 0;
}

int ObDelUpdStmt::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDmlTableInfo *, 4> dml_table_infos;
  if (OB_FAIL(ObDMLStmt::iterate_stmt_expr(visitor))) {
    LOG_WARN("failed to visit DMLStmt expr", K(ret));
  } else if (OB_FAIL(visitor.visit(returning_exprs_, SCOPE_RETURNING))) {
    LOG_WARN("failed to visit returning exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(returning_into_exprs_, SCOPE_RETURNING))) {
    LOG_WARN("failed to visit returning into exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(returning_agg_items_, SCOPE_DICT_FIELDS))) {
    LOG_WARN("failed to visit returning agg items", K(ret));
  } else if (ab_stmt_id_expr_ != NULL &&
             OB_FAIL(visitor.visit(ab_stmt_id_expr_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit ab stmt id expr", K(ret));
  } else if (OB_FAIL(visitor.visit(error_log_info_.error_log_exprs_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit errlog exprs", K(ret));
  } else if (OB_FAIL(visitor.visit(sharding_conditions_, SCOPE_DMLINFOS))) {
    LOG_WARN("failed to visit sharding conditions", K(ret));
  } else if (OB_FAIL(get_dml_table_infos(dml_table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dml_table_infos.count(); ++i) {
    if (OB_ISNULL(dml_table_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml table info is null", K(ret));
    } else if (OB_FAIL(dml_table_infos.at(i)->iterate_stmt_expr(visitor))) {
      LOG_WARN("failed to itearte dml table infos", K(ret));
    } else if (dml_table_infos.at(i)->table_id_ != OB_INVALID_ID &&
               dml_table_infos.at(i)->table_id_ != dml_table_infos.at(i)->loc_table_id_) {
      // handle updatable view in a speical way
      for (int64_t j = 0; OB_SUCC(ret) && j < part_expr_items_.count(); ++j) {
        if (part_expr_items_.at(j).table_id_ == dml_table_infos.at(i)->loc_table_id_) {
          if (OB_FAIL(visitor.visit(part_expr_items_.at(j).part_expr_,
                                    SCOPE_DMLINFOS))) {
            LOG_WARN("failed to visit part expr items", K(ret));
          } else if (OB_FAIL(visitor.visit(part_expr_items_.at(j).subpart_expr_,
                                           SCOPE_DMLINFOS))) {
            LOG_WARN("failed to visit subpart exprs", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdStmt::update_base_tid_cid()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDmlTableInfo*, 2> table_info;
  if (OB_FAIL(get_dml_table_infos(table_info))) {
    LOG_WARN("failed to get dml table info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info.count(); i++) {
      ObDmlTableInfo *dml_table = NULL;
      ObIArray<ObAssignment> *table_assignments = NULL;
      if (OB_ISNULL(dml_table = table_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        uint64_t base_tid = OB_INVALID_ID;
        for (int64_t j = 0; OB_SUCC(ret) && j < dml_table->column_exprs_.count(); j++) {
          ObColumnRefRawExpr *col = dml_table->column_exprs_.at(j);
          ColumnItem *col_item = NULL;
          if (OB_ISNULL(col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is null", K(ret), K(col));
          } else if (IS_SHADOW_COLUMN(col->get_column_id())) {
            // do nothing
          } else if (OB_ISNULL(
              col_item = get_column_item_by_id(col->get_table_id(), col->get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column item is not found", K(ret), K(*col), K(col_item));
          } else if (OB_FAIL(ObTransformUtils::get_base_column(this, col))) {
            LOG_WARN("failed to get column base info", K(ret));
          } else if (OB_ISNULL(col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(col), K(ret));
          } else {
            const bool is_rowkey_doc = col->get_table_name().suffix_match("rowkey_doc");
            col_item->base_tid_ = col->get_table_id();
            col_item->base_cid_ = col->get_column_id();
            if (OB_UNLIKELY(col_item->base_tid_ == OB_INVALID_ID) ||
            OB_UNLIKELY(j != 0 && col_item->base_tid_ != base_tid && !is_rowkey_doc)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("base table id is invalid", K(ret), K(col_item->base_tid_), K(base_tid));
            } else if (j == 0) {
              base_tid = col_item->base_tid_;
            }
          }
        }
        if (OB_FAIL(ret)) {
          /*do nothing*/
        } else if (dml_table->is_update_table()) {
          table_assignments = &static_cast<ObUpdateTableInfo*>(dml_table)->assignments_;
        } else if (dml_table->is_insert_table() || dml_table->is_merge_table()) {
          table_assignments = &static_cast<ObInsertTableInfo*>(dml_table)->assignments_;
        } else { /*do nothing*/ }

        if (OB_SUCC(ret) && dml_table->loc_table_id_ != base_tid) {
          for (int64_t k = 0; OB_SUCC(ret) && k < part_expr_items_.count(); ++k) {
            if (part_expr_items_.at(k).table_id_ == dml_table->loc_table_id_) {
              part_expr_items_.at(k).table_id_ = base_tid;
            }
          }
          dml_table->loc_table_id_ = base_tid;
        }

      }
    }
  }
  return ret;
}

int ObDelUpdStmt::check_part_key_is_updated(const ObIArray<ObAssignment> &assigns,
                                            bool &is_updated) const
{
  int ret = OB_SUCCESS;
  is_updated = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_updated && i < assigns.count(); i++) {
    ObColumnRefRawExpr *col_expr = assigns.at(i).column_expr_;
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (IS_SHADOW_COLUMN(col_expr->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_base_column(this, col_expr))) {
      LOG_WARN("failed to get base column", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (col_expr->is_table_part_key_column() || col_expr->is_table_part_key_org_column()) {
      is_updated = true;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObDelUpdStmt::get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const
{
  UNUSED(exprs);
  return false;
}

int ObDelUpdStmt::get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info)
{
  UNUSED(dml_table_info);
  return OB_SUCCESS;
}

int ObDelUpdStmt::get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const
{
  UNUSED(dml_table_info);
  return OB_SUCCESS;
}

int ObDelUpdStmt::get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const
{
  UNUSED(view_check_exprs);
  return OB_SUCCESS;
}

int ObDelUpdStmt::get_value_exprs(ObIArray<ObRawExpr *> &value_exprs) const
{
  UNUSED(value_exprs);
  return OB_SUCCESS;
}

int ObDelUpdStmt::remove_table_item_dml_info(const TableItem* table)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("can not remove all dml table", K(ret));
  return ret;
}

int ObDelUpdStmt::has_dml_table_info(const uint64_t table_id, bool &has) const
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObDmlTableInfo *, 2> table_infos;
  has = false;
  if (OB_FAIL(get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
    if (OB_ISNULL(table_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table info is null", K(ret));
    } else if (table_infos.at(i)->table_id_ == table_id) {
      has = true;
      break;
    }
  }
  return ret;
}

int ObDelUpdStmt::check_dml_need_filter_null()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_ids;
  ObSEArray<ObDmlTableInfo*, 4> dml_table_infos;
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables_.count(); ++i) {
    if (OB_FAIL(extract_need_filter_null_table(joined_tables_.at(i), table_ids))) {
      LOG_WARN("recursively check filter null failed", K(ret), K(joined_tables_));
    }
  }
  if (OB_SUCC(ret) && !table_ids.empty()) {
    if (OB_FAIL(get_dml_table_infos(dml_table_infos))) {
      LOG_WARN("failed to get dml talbe infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_table_infos.count(); ++i) {
      if (OB_ISNULL(dml_table_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null dml table info", K(ret), K(i), K(dml_table_infos));
      } else if (ObOptimizerUtil::find_item(table_ids, dml_table_infos.at(i)->table_id_)) {
        dml_table_infos.at(i)->need_filter_null_ = true;
      }
    }
  }
  return ret;
}

int ObDelUpdStmt::extract_need_filter_null_table(const JoinedTable *cur_table,
                                                 ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  bool need_check_left = false;
  bool need_check_right = false;
  if (OB_ISNULL(cur_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null joined table", K(ret), K(cur_table));
  } else if (FULL_OUTER_JOIN == cur_table->joined_type_) {
    if (OB_FAIL(append(table_ids, cur_table->single_table_ids_))) {
      LOG_WARN("failed to append table ids", K(ret));
    }
  } else if (LEFT_OUTER_JOIN == cur_table->joined_type_ || 
             RIGHT_OUTER_JOIN == cur_table->joined_type_) {
    const TableItem *child_table = NULL;
    if (LEFT_OUTER_JOIN == cur_table->joined_type_) {
      need_check_left = true;
      child_table = cur_table->right_table_;
    } else {
      need_check_right = true;
      child_table = cur_table->left_table_;
    }
    if (OB_ISNULL(child_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child table is null", K(ret));
    } else if (child_table->is_joined_table()) {
      const JoinedTable *join_table = static_cast<const JoinedTable*>(child_table);
      if (OB_FAIL(append(table_ids, join_table->single_table_ids_))) {
        LOG_WARN("failed to append table ids", K(ret));
      }
    } else if (OB_FAIL(table_ids.push_back(child_table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  } else {
    need_check_left = true;
    need_check_right = true;
  }

  if (OB_SUCC(ret) && need_check_left && OB_NOT_NULL(cur_table->left_table_) &&
      cur_table->left_table_->is_joined_table()) {
    const JoinedTable *left_table = static_cast<const JoinedTable*>(cur_table->left_table_);
    if (OB_FAIL(SMART_CALL(extract_need_filter_null_table(left_table, table_ids)))) {
      LOG_WARN("failed to extract need filter null table", K(ret));
    }
  }
  if (OB_SUCC(ret) && need_check_right && OB_NOT_NULL(cur_table->right_table_) &&
      cur_table->right_table_->is_joined_table()) {
    const JoinedTable *right_table = static_cast<const JoinedTable*>(cur_table->right_table_);
    if (OB_FAIL(SMART_CALL(extract_need_filter_null_table(right_table, table_ids)))) {
      LOG_WARN("failed to extract need filter null table", K(ret));
    }
  }
  return ret;
}

int ObDelUpdStmt::check_dml_source_from_join()
{
  int ret = OB_SUCCESS;
  TableItem *dml_table = nullptr;
  if (get_from_item_size() == 1 &&
      nullptr != (dml_table = get_table_item(get_from_item(0))) &&
      dml_table->is_basic_table()) {
    // do nothing
  } else {
    set_dml_source_from_join(true);
  }
  return ret;
}