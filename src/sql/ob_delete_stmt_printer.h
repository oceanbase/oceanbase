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

#ifndef OCEANBASE_SRC_SQL_OB_DELETE_STMT_PRINTER_H_
#define OCEANBASE_SRC_SQL_OB_DELETE_STMT_PRINTER_H_

#include "ob_dml_stmt_printer.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"

namespace oceanbase
{
namespace sql
{
class ObDeleteStmtPrinter : public ObDMLStmtPrinter {

public:
  ObDeleteStmtPrinter()=delete;
  ObDeleteStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObDeleteStmt *stmt,
                      ObSchemaGetterGuard *schema_guard,
                      common::ObObjPrintParams print_params,
                      const ParamStore *param_store = NULL) :
    ObDMLStmtPrinter(buf, buf_len, pos, stmt, schema_guard, print_params, param_store) {}
  virtual ~ObDeleteStmtPrinter() {}

  void init(char *buf, int64_t buf_len, int64_t *pos, ObDeleteStmt *stmt);
  virtual int do_print();

private:
  int print();
  int print_basic_stmt();

  int print_delete();

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDeleteStmtPrinter);
private:
  // data members

};

}
}



#endif /* OCEANBASE_SRC_SQL_OB_DELETE_STMT_PRINTER_H_ */
