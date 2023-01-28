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
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObInsertAllStmt::ObInsertAllStmt()
  : ObDelUpdStmt(stmt::T_INSERT_ALL),
    is_multi_insert_first_(false),
    is_multi_condition_insert_(false),
    table_info_()
{
  // TODO Auto-generated constructor stub

}

ObInsertAllStmt::~ObInsertAllStmt()
{
  // TODO Auto-generated destructor stub
}

int ObInsertAllStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                        ObRawExprCopier &expr_copier,
                                        const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObInsertAllStmt &other = static_cast<const ObInsertAllStmt &>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(allocator,
                                                         expr_copier,
                                                         input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObInsertAllTableInfo>(allocator,
                                                                  expr_copier,
                                                                  other.table_info_,
                                                                  table_info_))) {
    LOG_WARN("failed do deep copy table info", K(ret));
  } else {
    is_multi_insert_first_ = other.is_multi_insert_first_;
    is_multi_condition_insert_ = other.is_multi_condition_insert_;
  }
  return ret;
}

int ObInsertAllStmt::assign(const ObInsertAllStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(table_info_.assign(other.table_info_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    is_multi_insert_first_ = other.is_multi_insert_first_;
    is_multi_condition_insert_ = other.is_multi_condition_insert_;
  }
  return ret;
}

int ObInsertAllStmt::check_table_be_modified(uint64_t ref_table_id, bool& found) const
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

int ObInsertAllStmt::get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(dml_table_info, table_info_))) {
    LOG_WARN("failed to append table info", K(ret));
  }
  return ret;
}

int ObInsertAllStmt::get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(dml_table_info, table_info_))) {
    LOG_WARN("failed to append table info", K(ret));
  }
  return ret;
}

int ObInsertAllStmt::get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObInsertAllTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (OB_FAIL(append(view_check_exprs, table_info->view_check_exprs_))) {
      LOG_WARN("failed to append view check exprs", K(ret));
    }
  }
  return ret;
}

int ObInsertAllStmt::get_all_values_vector(ObIArray<ObRawExpr*> &all_values_vector) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObInsertAllTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (OB_FAIL(append(all_values_vector, table_info->values_vector_))) {
      LOG_WARN("failed to append values vector", K(ret));
    }
  }
  return ret;
}

int ObInsertAllStmt::get_all_when_cond_exprs(ObIArray<ObRawExpr*> &all_when_cond_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.count(); ++i) {
    ObInsertAllTableInfo* table_info = table_info_.at(i);
    if (OB_ISNULL(table_info))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (OB_FAIL(append(all_when_cond_exprs, table_info->when_cond_exprs_))) {
      LOG_WARN("failed to append when cond exprs", K(ret));
    }
  }
  return ret;
}

int64_t ObInsertAllStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  J_OBJ_START();
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_FAIL(ObDMLStmt::get_child_stmts(child_stmts))) {
    databuff_printf(buf, buf_len, pos, "ERROR get child stmts failed");
  } else if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    databuff_printf(buf, buf_len, pos, "ERROR query context is null");
  } else {
    J_KV(N_STMT_TYPE, stmt_type_,
         N_TABLE, table_items_,
         N_PARTITION_EXPR, part_expr_items_,
         N_COLUMN, column_items_,
         "insert_table_info", table_info_,
         K(child_stmts));
  }
  J_OBJ_END();
  return pos;
}

} /* namespace sql */
} /* namespace oceanbase */
