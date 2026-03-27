/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_OB_INSERT_ALL_STMT_PRINTER_H_
#define OCEANBASE_SRC_SQL_OB_INSERT_ALL_STMT_PRINTER_H_

#include "ob_dml_stmt_printer.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/printer/ob_raw_expr_printer.h"

namespace oceanbase
{
namespace sql
{
class ObInsertAllStmtPrinter : public ObDMLStmtPrinter {

public:
  ObInsertAllStmtPrinter() = delete;
  ObInsertAllStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObInsertAllStmt *stmt,
                         ObSchemaGetterGuard *schema_guard,
                         common::ObObjPrintParams print_params,
                         const ParamStore *param_store = NULL,
                         const ObSQLSessionInfo *session = NULL) :
    ObDMLStmtPrinter(buf, buf_len, pos, stmt, schema_guard, print_params, param_store, session) {}
  virtual ~ObInsertAllStmtPrinter() {}

  void init(char *buf, int64_t buf_len, int64_t *pos, ObInsertAllStmt *stmt);
  virtual int do_print()override;

private:
  int print();
  int print_multi_insert_stmt();
  int print_multi_insert(const ObInsertAllStmt *insert_stmt);
  int print_multi_value(const ObInsertAllStmt *insert_stmt);
  int print_basic_multi_insert(const ObInsertAllStmt *insert_stmt);
  int print_multi_conditions_insert(const ObInsertAllStmt *insert_stmt);
  int print_into_table_values(const ObInsertAllStmt *insert_stmt,
                              const ObInsertAllTableInfo& table_info);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObInsertAllStmtPrinter);
private:
  // data members

};

}
}


#endif /* OCEANBASE_SRC_SQL_OB_INSERT_ALL_STMT_PRINTER_H_ */
