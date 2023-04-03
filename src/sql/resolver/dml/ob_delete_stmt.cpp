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
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include <stdio.h>

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

ObDeleteStmt::ObDeleteStmt()
  : ObDelUpdStmt(stmt::T_DELETE),
    table_info_()
{
}

ObDeleteStmt::~ObDeleteStmt()
{
}

int ObDeleteStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                        ObRawExprCopier &expr_copier,
                                        const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObDeleteStmt &other = static_cast<const ObDeleteStmt &>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(allocator,
                                                         expr_copier,
                                                         other))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObDeleteTableInfo>(allocator,
                                                             expr_copier,
                                                             other.table_info_,
                                                             table_info_))) {
    LOG_WARN("failed do deep copy table info", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObDeleteStmt::assign(const ObDeleteStmt &other) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(table_info_.assign(other.table_info_))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObDeleteStmt::check_table_be_modified(uint64_t ref_table_id, bool& found) const
{
  int ret = OB_SUCCESS;
  found = false;
  for (int64_t i = 0; !found && i < table_info_.count(); i++) {
    if (NULL != table_info_.at(i) && table_info_.at(i)->ref_table_id_ == ref_table_id) {
      found = true;
    }
  }
  return ret;
}

int ObDeleteStmt::remove_delete_table_info(int64_t table_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = table_info_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (OB_ISNULL(table_info_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (table_id == table_info_.at(i)->table_id_) {
      if (OB_FAIL(table_info_.remove(i))) {
        LOG_WARN("failed to remove table info", K(ret));
      } else {
        break;
      }
    }
  }
  return ret;
}

int64_t ObDeleteStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_ISNULL(query_ctx_)) {
    databuff_printf(buf, buf_len, pos, "ERROR query context is null");
  } else if (get_child_stmts(child_stmts) != OB_SUCCESS) {
    databuff_printf(buf, buf_len, pos, "ERROR get child stmts failed");
  } else {
    J_KV(
          N_STMT_TYPE, stmt_type_,
          N_TABLE, table_items_,
          N_JOINED_TABLE, joined_tables_,
          N_SEMI_INFO, semi_infos_,
          N_PARTITION_EXPR, part_expr_items_,
          N_COLUMN, column_items_,
          N_FROM, from_items_,
          N_WHERE, condition_exprs_,
          N_ORDER_BY, order_items_,
          N_LIMIT, limit_count_expr_,
          N_OFFSET, limit_offset_expr_,
          N_STMT_HINT, stmt_hint_,
          N_QUERY_CTX, *query_ctx_,
          K(child_stmts)
          );
  }
  if (is_error_logging()) {
    J_KV("is_err_log", error_log_info_.is_error_log_,
         "err_log_table_name", error_log_info_.table_name_,
         "err_log_database_name", error_log_info_.database_name_,
         "err_log_table_id", error_log_info_.table_id_,
         "err_log_reject_limit", error_log_info_.reject_limit_,
         "err_log_exprs", error_log_info_.error_log_exprs_);
  }
  J_OBJ_END();
  return pos;
}

int ObDeleteStmt::get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(dml_table_info, table_info_))) {
    LOG_WARN("failed to append table info", K(ret));
  }
  return ret;
}

int ObDeleteStmt::get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(dml_table_info, table_info_))) {
    LOG_WARN("failed to append table info", K(ret));
  }
  return ret;
}

int ObDeleteStmt::get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObDeleteTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (OB_FAIL(append(view_check_exprs, table_info->view_check_exprs_))) {
      LOG_WARN("failed to append view check exprs", K(ret));
    }
  }
  return ret;
}

int64_t ObDeleteStmt::get_instead_of_trigger_column_count() const
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

int ObDeleteStmt::remove_table_item_dml_info(const TableItem* table)
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

}
}
