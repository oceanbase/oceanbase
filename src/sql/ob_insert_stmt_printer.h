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

#ifndef OCEANBASE_SRC_SQL_OB_INSERT_STMT_PRINTER_H_
#define OCEANBASE_SRC_SQL_OB_INSERT_STMT_PRINTER_H_

#include "ob_dml_stmt_printer.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"

namespace oceanbase {
namespace sql {
class ObInsertStmtPrinter : public ObDMLStmtPrinter {

public:
  ObInsertStmtPrinter()
  {}
  ObInsertStmtPrinter(
      char* buf, int64_t buf_len, int64_t* pos, const ObInsertStmt* stmt, common::ObObjPrintParams print_params)
      : ObDMLStmtPrinter(buf, buf_len, pos, stmt, print_params)
  {}
  virtual ~ObInsertStmtPrinter()
  {}

  void init(char* buf, int64_t buf_len, int64_t* pos, ObInsertStmt* stmt);
  virtual int do_print();

private:
  int print();
  int print_basic_stmt();

  int print_insert();
  int print_into();
  int print_values();

  int print_multi_insert_stmt();
  int print_multi_insert(const ObInsertStmt* insert_stmt);
  int print_multi_value(const ObInsertStmt* insert_stmt);
  int print_basic_multi_insert(const ObInsertStmt* insert_stmt);
  int print_multi_conditions_insert(const ObInsertStmt* insert_stmt);
  int print_subquery(const ObInsertStmt* insert_stmt);
  int print_into_table_values(const ObInsertStmt* insert_stmt, int64_t index);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObInsertStmtPrinter);

private:
  // data members
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_OB_INSERT_STMT_PRINTER_H_ */
