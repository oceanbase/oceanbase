/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
