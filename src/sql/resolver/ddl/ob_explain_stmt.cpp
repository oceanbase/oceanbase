/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/ddl/ob_explain_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExplainStmt::ObExplainStmt(ObIAllocator &allocator) : ObDMLStmt(stmt::T_EXPLAIN, allocator),
                                 format_(EXPLAIN_UNINITIALIZED),
                                 explain_stmt_(NULL)
{
}

ObExplainStmt::~ObExplainStmt()
{
}

bool ObExplainStmt::is_select_explain() const
{
  bool bool_ret = false;
  ObDMLStmt *query_stmt = get_explain_query_stmt();
  if (NULL != query_stmt) {
    bool_ret = query_stmt->is_select_stmt();
  }
  return bool_ret;
}

bool ObExplainStmt::is_dml_explain() const
{
  bool bool_ret = false;
  ObDMLStmt *query_stmt = get_explain_query_stmt();
  if (NULL != query_stmt) {
    bool_ret = query_stmt->is_dml_stmt();
  }
  return bool_ret;
}

int64_t ObExplainStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (OB_ISNULL(explain_stmt_)) {
    databuff_printf(buf, buf_len, pos, "explain stmt is null");
  } else if (is_ddl_explain()) {
    J_KV(N_EXPLAIN_STMT, explain_stmt_);
  } else {
    J_KV(K_(into_table), K_(statement_id), N_EXPLAIN_STMT, explain_stmt_);
  }
  J_OBJ_END();
  return pos;
}

}//end of sql
}//end of oceanbase

//void ObExplainStmt::print(FILE *fp, int32_t level, int32_t index)
//{
//  UNUSED(index);
//  print_indentation(fp, level);
//  fprintf(fp, "ObExplainStmt %d Begin\n", index);
//  //if (verbose_) {
//  //  print_indentation(fp, level + 1);
//  //  fprintf(fp, "VERBOSE\n");
//  //}
//  print_indentation(fp, level + 1);
//  fprintf(fp, "Explain Query ::= <%p>\n", explain_stmt_);
//  print_indentation(fp, level);
//  fprintf(fp, "ObExplainStmt %d End\n", index);
//}
