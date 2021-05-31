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
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

ObUpdateStmt::ObUpdateStmt()
    : ObDelUpdStmt(stmt::T_UPDATE), tables_assignments_(), low_priority_(false), update_set_(false)
{}

ObUpdateStmt::~ObUpdateStmt()
{}

int ObUpdateStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& input)
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt& other = static_cast<const ObUpdateStmt&>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(stmt_factory, expr_factory, input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(tables_assignments_.prepare_allocate(other.tables_assignments_.count()))) {
    LOG_WARN("failed to prepare allocate table assignment array", K(ret));
  } else {
    low_priority_ = other.low_priority_;
    update_set_ = other.update_set_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
    if (OB_FAIL(tables_assignments_.at(i).deep_copy(expr_factory, other.tables_assignments_.at(i)))) {
      LOG_WARN("failed to deep copy table assignment", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refill_index_assignment_info())) {
      LOG_WARN("fail refill index assignment info", K(ret));
    }
  }
  return ret;
}

int ObUpdateStmt::assign(const ObUpdateStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(tables_assignments_.assign(other.tables_assignments_))) {
    LOG_WARN("failed to assign tables assignments", K(ret));
  } else {
    low_priority_ = other.low_priority_;
    update_set_ = other.update_set_;
  }
  return ret;
}

int ObUpdateStmt::update_base_tid_cid()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::update_base_tid_cid())) {
    LOG_WARN("failed to update base tid and cid", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
    ObTableAssignment& table_assign = tables_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObColumnRefRawExpr* col = table_assign.assignments_.at(j).column_expr_;
      ColumnItem* col_item = NULL;
      if (OB_ISNULL(col_item = get_column_item_by_id(col->get_table_id(), col->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column item", K(ret), K(col_item));
      } else {
        table_assign.assignments_.at(j).base_table_id_ = col_item->base_tid_;
        table_assign.assignments_.at(j).base_column_id_ = col_item->base_cid_;
      }
    }
  }
  return ret;
}

int ObUpdateStmt::replace_inner_stmt_expr(
    const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::replace_inner_stmt_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); i++) {
      if (OB_FAIL(replace_table_assign_exprs(other_exprs, new_exprs, tables_assignments_.at(i).assignments_))) {
        LOG_WARN("failed to replace table assign exprs", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObUpdateStmt::inner_get_relation_exprs(RelExprCheckerBase& expr_checker)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
    ObTableAssignment& table_assign = tables_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObAssignment& assign = table_assign.assignments_.at(j);
      ObRawExpr*& expr = assign.expr_;
      ObRawExpr*& column_expr = reinterpret_cast<ObRawExpr*&>(assign.column_expr_);
      if (OB_FAIL(expr_checker.add_expr(expr))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      } else if (OB_FAIL(expr_checker.add_expr(column_expr))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDelUpdStmt::inner_get_relation_exprs(expr_checker))) {
      LOG_WARN("get delup stmt relation exprs failed", K(ret));
    }
  }
  return ret;
}

int ObUpdateStmt::get_tables_assignments_exprs(ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
    const ObTableAssignment& table_assignment = tables_assignments_.at(i);
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

int ObUpdateStmt::replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
    ObTableAssignment& table_assign = tables_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObAssignment& assign = table_assign.assignments_.at(j);
      if (assign.expr_ == from) {
        assign.expr_ = to;
      }
    }
  }
  // replace expr in IndexDMLInfo
  for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_.count(); i++) {
    ObIArray<IndexDMLInfo>& index_dml_infos = all_table_columns_.at(i).index_dml_infos_;
    for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_infos.count(); j++) {
      IndexDMLInfo& index_dml_info = index_dml_infos.at(j);
      ObAssignments& assignments = index_dml_info.assignments_;
      for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
        ObAssignment& assign = assignments.at(j);
        if (assign.expr_ == from) {
          assign.expr_ = to;
        }
      }
      auto& column_convert_exprs = index_dml_info.column_convert_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && j < column_convert_exprs.count(); ++j) {
        if (column_convert_exprs.at(j) == from) {
          column_convert_exprs.at(j) = to;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDelUpdStmt::replace_expr_in_stmt(from, to))) {
    LOG_WARN("replace expr in stmt failed", K(ret));
  }
  return ret;
}

int ObUpdateStmt::has_special_expr(const ObExprInfoFlag flag, bool& has) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDMLStmt::has_special_expr(flag, has))) {
    LOG_WARN("failed to check rownum in stmt", K(ret));
  } else if (!has) {
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < get_tables_assignments().count(); ++i) {
      const ObTableAssignment& table_assignment = get_tables_assignments().at(i);
      for (int64_t j = 0; OB_SUCC(ret) && !has && j < table_assignment.assignments_.count(); ++j) {
        ObRawExpr* expr = table_assignment.assignments_.at(j).expr_;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Update assignment expr is null", K(ret));
        } else if (expr->has_flag(flag)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

int ObUpdateStmt::get_assign_values(ObIArray<ObRawExpr*>& exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
    const ObTableAssignment& table_assign = tables_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      const ObAssignment& assign = table_assign.assignments_.at(j);
      if (OB_ISNULL(assign.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assgin expr is null", K(ret));
      } else if (OB_FAIL(exprs.push_back(assign.expr_))) {
        LOG_WARN("failed to push back assign value expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateStmt::get_vector_assign_values(ObQueryRefRawExpr* query_ref, ObIArray<ObRawExpr*>& assign_values) const
{
  int ret = OB_SUCCESS;
  int64_t vector_size = 0;
  if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref expr is null", K(ret), K(query_ref));
  } else {
    vector_size = query_ref->get_ref_stmt()->get_select_item_size();
  }
  if (is_update_set() && vector_size > 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_.count(); ++i) {
      const ObTableAssignment& table_assign = tables_assignments_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
        const ObAssignment& assign = table_assign.assignments_.at(j);
        ObAliasRefRawExpr* alias = NULL;
        if (OB_FAIL(ObRawExprUtils::find_alias_expr(assign.expr_, alias))) {
          LOG_WARN("failed to find alias expr", K(ret));
        } else if (alias == NULL) {
          // do nothing
        } else if (alias->get_param_expr(0) == query_ref) {
          int64_t project_index = alias->get_project_index();
          if (project_index < 0 || project_index >= vector_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("project index is invalid", K(ret));
          } else if (assign_values.empty() && OB_FAIL(assign_values.prepare_allocate(vector_size))) {
            LOG_WARN("failed to prepare allocate vector array", K(ret));
          } else {
            assign_values.at(project_index) = alias;
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateStmt::check_assign()
{
  int ret = OB_SUCCESS;
  bool has_vector_assign = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_vector_assign && i < tables_assignments_.count(); ++i) {
    const ObTableAssignment& table_assign = tables_assignments_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && !has_vector_assign && j < table_assign.assignments_.count(); ++j) {
      const ObAssignment& assign = table_assign.assignments_.at(j);
      ObAliasRefRawExpr* alias = NULL;
      if (OB_FAIL(ObRawExprUtils::find_alias_expr(assign.expr_, alias))) {
        LOG_WARN("failed to find alias expr", K(ret));
      } else {
        has_vector_assign = (alias != NULL);
      }
    }
  }
  if (OB_SUCC(ret) && !has_vector_assign) {
    set_update_set(false);
  }
  return ret;
}

const ObTablesAssignments* ObUpdateStmt::get_slice_from_all_table_assignments(
    ObIAllocator& allocator, int64_t table_idx, int64_t index_idx) const
{
  int ret = OB_SUCCESS;
  ObTablesAssignments* tables_assignments = nullptr;  // Multi-table
  ObTableAssignment* table_assignments = nullptr;     // for one table multi assignment
  void* buf1 = nullptr;
  void* buf2 = nullptr;
  if (table_idx < 0 || table_idx >= tables_assignments_.count()) {
    // no need set ret
    LOG_WARN("invalid table idx", K(table_idx), K(index_idx));
  } else if (index_idx < 0 || index_idx >= all_table_columns_.at(table_idx).index_dml_infos_.count()) {
    // no need set ret
    LOG_WARN("invalid index idx", K(table_idx), K(index_idx));
  } else if (nullptr == (buf1 = allocator.alloc(sizeof(ObTablesAssignments)))) {
    // no need set ret
    LOG_WARN("no memory", KP(buf1));
  } else if (nullptr == (buf2 = allocator.alloc(sizeof(ObTableAssignment)))) {
    // no need set ret
    LOG_WARN("no memory", KP(buf2));
  } else {
    tables_assignments = new (buf1) ObTablesAssignments();
    table_assignments = new (buf2) ObTableAssignment();
    const ObTableAssignment& assignments = tables_assignments_.at(table_idx);
    const IndexDMLInfo& dml_index_info = all_table_columns_.at(table_idx).index_dml_infos_.at(index_idx);
    if (OB_FAIL(table_assignments->assignments_.assign(dml_index_info.assignments_))) {
      LOG_WARN("fail copy array", K(ret));
    } else {
      bool is_update_part_key = false;
      if (dml_index_info.all_part_num_ > 1) {  // Only partition tables have part_key
        for (int i = 0; i < dml_index_info.assignments_.count(); ++i) {
          uint64_t col_id = dml_index_info.assignments_.at(i).column_expr_->get_column_id();
          for (int j = 0; j < dml_index_info.part_key_ids_.count(); ++j) {
            if (dml_index_info.part_key_ids_.at(j) == col_id) {
              is_update_part_key = true;
              break;
            }
          }
        }
      }
      table_assignments->is_update_part_key_ = is_update_part_key;
      table_assignments->table_id_ = dml_index_info.table_id_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tables_assignments->push_back(*table_assignments))) {
        LOG_WARN("fail save table_assignments to array", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    tables_assignments = nullptr;
  }
  return tables_assignments;
}

int ObUpdateStmt::refill_index_assignment_info()
{
  int ret = OB_SUCCESS;
  const ObTablesAssignments& table_assign = get_tables_assignments();
  ObIArray<TableColumns>& all_table_columns = get_all_table_columns();
  CK(table_assign.count() == all_table_columns.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < table_assign.count(); ++i) {
    const ObAssignments& assignments = table_assign.at(i).assignments_;
    ObIArray<IndexDMLInfo>& index_infos = all_table_columns.at(i).index_dml_infos_;
    for (int64_t j = 0; OB_SUCC(ret) && j < index_infos.count(); ++j) {
      IndexDMLInfo& index_info = index_infos.at(j);
      if (OB_FAIL(index_info.init_assignment_info(assignments))) {
        LOG_WARN("init index assignment info failed", K(i), K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
