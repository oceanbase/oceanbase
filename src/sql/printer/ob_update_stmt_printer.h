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

#ifndef OCEANBASE_SRC_SQL_OB_UPDATE_STMT_PRINTER_H_
#define OCEANBASE_SRC_SQL_OB_UPDATE_STMT_PRINTER_H_

#include "ob_dml_stmt_printer.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/printer/ob_raw_expr_printer.h"

namespace oceanbase
{
namespace sql
{
class ObUpdateStmtPrinter : public ObDMLStmtPrinter {

public:
  ObUpdateStmtPrinter()=delete;
  ObUpdateStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObUpdateStmt *stmt,
                      ObSchemaGetterGuard *schema_guard,
                      common::ObObjPrintParams print_params,
                      const ParamStore *param_store = NULL,
                      const ObSQLSessionInfo *session = NULL) :
    ObDMLStmtPrinter(buf, buf_len, pos, stmt, schema_guard, print_params, param_store, session) {}
  virtual ~ObUpdateStmtPrinter() {}

  void init(char *buf, int64_t buf_len, int64_t *pos, ObUpdateStmt *stmt);
  virtual int do_print();

private:
  int print();
  int print_basic_stmt();

  int print_update();
  int print_set();

  int print_simple_assign(const ObAssignment &assign);

  int print_vector_assign(const ObAssignments &assignments,
                          ObRawExpr *query_ref_expr);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUpdateStmtPrinter);
private:
  // data members

};

}
}




#endif /* OCEANBASE_SRC_SQL_OB_UPDATE_STMT_PRINTER_H_ */
