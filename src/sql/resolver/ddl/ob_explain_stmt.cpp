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

#include "sql/resolver/ddl/ob_explain_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExplainStmt::ObExplainStmt() : ObDMLStmt(stmt::T_EXPLAIN),
                                 format_(EXPLAIN_UNINITIALIZED),
                                 explain_query_stmt_(NULL)
{
}

ObExplainStmt::~ObExplainStmt()
{
}

bool ObExplainStmt::is_select_explain() const
{
  bool bool_ret = false;
  if (NULL != explain_query_stmt_) {
    bool_ret = explain_query_stmt_->is_select_stmt();
  }
  return bool_ret;
}

bool ObExplainStmt::is_dml_explain() const
{
  bool bool_ret = false;
  if (NULL != explain_query_stmt_) {
    bool_ret = explain_query_stmt_->is_dml_stmt();
  }
  return bool_ret;
}

int64_t ObExplainStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (OB_ISNULL(explain_query_stmt_)) {
    databuff_printf(buf, buf_len, pos, "explain query stmt is null");
  } else {
    J_KV(K_(into_table), K_(statement_id), N_EXPLAIN_STMT, explain_query_stmt_);
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
//  fprintf(fp, "Explain Query ::= <%p>\n", explain_query_stmt_);
//  print_indentation(fp, level);
//  fprintf(fp, "ObExplainStmt %d End\n", index);
//}
