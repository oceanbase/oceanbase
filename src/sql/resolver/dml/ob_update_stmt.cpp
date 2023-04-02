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
namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

ObUpdateStmt::ObUpdateStmt()
  : ObDelUpdStmt(stmt::T_UPDATE),
    table_info_()
{
}

ObUpdateStmt::~ObUpdateStmt()
{
}

int ObUpdateStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                        ObRawExprCopier &expr_copier,
                                        const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt &other = static_cast<const ObUpdateStmt &>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(allocator,
                                                         expr_copier,
                                                         input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObUpdateTableInfo>(allocator,
                                                               expr_copier,
                                                               other.table_info_,
                                                               table_info_))) {
    LOG_WARN("failed do deep copy table info", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObUpdateStmt::assign(const ObUpdateStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(table_info_.assign(other.table_info_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObUpdateStmt::check_table_be_modified(uint64_t ref_table_id, bool& found) const
{
  found = false;
  for (int64_t i = 0; !found && i < table_info_.count(); i++) {
    if (NULL != table_info_.at(i) && table_info_.at(i)->ref_table_id_ == ref_table_id) {
      found = true;
    }
  }
  return OB_SUCCESS;
}

int ObUpdateStmt::get_assign_values(ObIArray<ObRawExpr *> &exprs,
                                    bool with_vector_assgin) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    if (OB_ISNULL(table_info_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < table_info_.at(i)->assignments_.count(); ++j) {
        const ObAssignment &assign = table_info_.at(i)->assignments_.at(j);
        if (OB_ISNULL(assign.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("assgin expr is null", K(ret));
        } else if (assign.expr_->has_flag(CNT_ALIAS) && !with_vector_assgin) {
          /* do nothing */
        } else if (OB_FAIL(exprs.push_back(assign.expr_))) {
          LOG_WARN("failed to push back assign value expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUpdateStmt::get_vector_assign_values(ObQueryRefRawExpr *query_ref,
                                           ObIArray<ObRawExpr *> &assign_values) const
{
  int ret = OB_SUCCESS;
  int64_t vector_size = 0;
  if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref expr is null", K(ret), K(query_ref));
  } else {
    vector_size = query_ref->get_ref_stmt()->get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
      if (OB_ISNULL(table_info_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < table_info_.at(i)->assignments_.count(); ++j) {
          const ObAssignment &assign = table_info_.at(i)->assignments_.at(j);
          ObAliasRefRawExpr *alias = NULL;
          if (OB_FAIL(ObRawExprUtils::find_alias_expr(assign.expr_, alias))) {
            LOG_WARN("failed to find alias expr", K(ret));
          } else if (alias == NULL) {
            // do nothing
          } else if (alias->get_param_expr(0) == query_ref) {
            int64_t project_index = alias->get_project_index();
            if (project_index < 0 || project_index >= vector_size) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("project index is invalid", K(ret));
            } else if (assign_values.empty() &&
                       OB_FAIL(assign_values.prepare_allocate(vector_size))) {
              LOG_WARN("failed to prepare allocate vector array", K(ret));
            } else {
              assign_values.at(project_index) = alias;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateStmt::part_key_is_updated(bool &is_updated) const
{
  int ret = OB_SUCCESS;
  is_updated = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); i++) {
    if (OB_ISNULL(table_info_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!table_info_.at(i)->is_link_table_ &&
               OB_FAIL(check_part_key_is_updated(table_info_.at(i)->assignments_,
                                                 is_updated))) {
      LOG_WARN("failed to check partition key is updated", K(ret));
    } else { /*do nothing*/ }
  }
  return  ret;
}

int ObUpdateStmt::get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObUpdateTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
      if (OB_FAIL(exprs.push_back(table_info->assignments_.at(j).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateStmt::get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(dml_table_info, table_info_))) {
    LOG_WARN("failed to append table info", K(ret));
  }
  return ret;
}

int ObUpdateStmt::get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(dml_table_info, table_info_))) {
    LOG_WARN("failed to append table info", K(ret));
  }
  return ret;
}

int ObUpdateStmt::get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObUpdateTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (OB_FAIL(append(view_check_exprs, table_info->view_check_exprs_))) {
      LOG_WARN("failed to append view check exprs", K(ret));
    }
  }
  return ret;
}

int64_t ObUpdateStmt::get_instead_of_trigger_column_count() const
{
  const TableItem *table_item = NULL;
  int64_t column_count = 0;
  if (1 == table_info_.count() &&
      NULL != table_info_.at(0) &&
      NULL != (table_item = get_table_item_by_id(table_info_.at(0)->table_id_)) &&
      table_item->is_view_table_ &&
      NULL != table_item->ref_query_) {
    column_count = table_item->ref_query_->get_select_item_size();
  }
  return column_count;
}

int ObUpdateStmt::remove_table_item_dml_info(const TableItem* table)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (; idx < table_info_.count(); ++idx) {
      if (OB_ISNULL(table_info_.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (table_info_.at(idx)->table_id_ == table->table_id_) {
        break;
      }
    }
    if (idx >= table_info_.count()) {
      // not find, do nothing
    } else if (table_info_.count() == 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not remove all dml table", K(ret));
    } else if (OB_FAIL(table_info_.remove(idx))) {
      LOG_WARN("failed to remove dml table info", K(ret));
    }
  }
  return ret;
}

int ObUpdateStmt::remove_invalid_assignment()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObUpdateTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      for (int64_t j = table_info->assignments_.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
        ObAssignment& assign = table_info->assignments_.at(j);
        if (OB_ISNULL(assign.column_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!assign.column_expr_->is_const_expr()) {
          // do nothing
        } else if (OB_FAIL(table_info->assignments_.remove(j))) {
          LOG_WARN("failed to remove assignment", K(ret));
        }
      }
    }
  }

  return ret;
}

} //namespace sql
} //namespace oceanbase
