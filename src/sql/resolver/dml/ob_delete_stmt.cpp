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

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

ObDeleteStmt::ObDeleteStmt() : ObDelUpdStmt(stmt::T_DELETE), low_priority_(false), quick_(false), ignore_(false)
{}

ObDeleteStmt::~ObDeleteStmt()
{}

int ObDeleteStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& input)
{
  int ret = OB_SUCCESS;
  const ObDeleteStmt& other = static_cast<const ObDeleteStmt&>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(stmt_factory, expr_factory, other))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else {
    low_priority_ = other.low_priority_;
    quick_ = other.quick_;
    ignore_ = other.ignore_;
  }
  return ret;
}

int ObDeleteStmt::assign(const ObDeleteStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else {
    low_priority_ = other.low_priority_;
    quick_ = other.quick_;
    ignore_ = other.ignore_;
  }
  return ret;
}

int64_t ObDeleteStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_ISNULL(query_ctx_)) {
    databuff_printf(buf, buf_len, pos, "ERROR query context is null");
  } else if (get_child_stmts(child_stmts) != OB_SUCCESS) {
    databuff_printf(buf, buf_len, pos, "ERROR get child stmts failed");
  } else {
    J_KV(N_STMT_TYPE,
        stmt_type_,
        N_TABLE,
        table_items_,
        N_JOINED_TABLE,
        joined_tables_,
        N_SEMI_INFO,
        semi_infos_,
        N_PARTITION_EXPR,
        part_expr_items_,
        N_COLUMN,
        column_items_,
        N_FROM,
        from_items_,
        N_WHERE,
        condition_exprs_,
        N_ORDER_BY,
        order_items_,
        N_LIMIT,
        limit_count_expr_,
        N_OFFSET,
        limit_offset_expr_,
        N_QUERY_HINT,
        stmt_hint_,
        N_QUERY_CTX,
        *query_ctx_,
        K(child_stmts));
  }
  J_OBJ_END();
  return pos;
}

}  // namespace sql
}  // namespace oceanbase
