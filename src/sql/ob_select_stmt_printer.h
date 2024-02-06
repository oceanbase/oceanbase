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

#ifndef OCEANBASE_SQL_OB_SELECT_STMT_PRINTER_H_
#define OCEANBASE_SQL_OB_SELECT_STMT_PRINTER_H_

#include "ob_dml_stmt_printer.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"

namespace oceanbase
{
namespace sql
{
class ObSelectStmtPrinter : public ObDMLStmtPrinter {

public:
  ObSelectStmtPrinter()=delete;

  ObSelectStmtPrinter(char *buf,
                      int64_t buf_len,
                      int64_t *pos,
                      const ObSelectStmt *stmt,
                      ObSchemaGetterGuard *schema_guard,
                      common::ObObjPrintParams print_params,
                      const bool force_col_alias = false)
  : ObDMLStmtPrinter(buf, buf_len, pos, stmt, schema_guard, print_params),
    column_list_(NULL),
    force_col_alias_(force_col_alias) {}
  
  ObSelectStmtPrinter(char *buf,
                      int64_t buf_len,
                      int64_t *pos,
                      const ObSelectStmt *stmt,
                      ObSchemaGetterGuard *schema_guard,
                      common::ObObjPrintParams print_params,
                      const ParamStore *param_store,
                      const bool force_col_alias = false)
  : ObDMLStmtPrinter(buf, buf_len, pos, stmt, schema_guard, print_params, param_store),
    column_list_(NULL),
    force_col_alias_(force_col_alias) {}
  virtual ~ObSelectStmtPrinter() {}

  void init(char *buf,
            int64_t buf_len,
            int64_t *pos,
            ObSelectStmt *stmt,
            common::ObIArray<common::ObString> *column_list);

  virtual int do_print();
  static int remove_double_quotation_for_string(ObString &alias_string, ObIAllocator &allocator);
  void set_column_list(common::ObIArray<common::ObString> *column_list) { column_list_ = column_list; }
  bool need_print_alias() const { return force_col_alias_ || (is_root_stmt() && print_params_.print_origin_stmt_); }

private:
  int print();
  int print_unpivot();
  int print_set_op_stmt();
  int print_recursive_union_stmt();
  int print_basic_stmt();

  int print_select();
  int print_start_with();
  int print_connect_by();
  int print_group_by();
  int print_having();
  int print_order_by();
  int print_for_update();

  ///////cte related functions
  int print_rollup_items(const common::ObIArray<ObRollupItem> &rollup_items);
  int print_cube_items(const common::ObIArray<ObCubeItem> &cube_items);
  int print_with_check_option();

  int find_recursive_cte_table(const ObSelectStmt* stmt, TableItem* &table);

  ///////end of functions
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSelectStmtPrinter);
private:
  // data members
  // create view v(column_list) as...
  common::ObIArray<common::ObString> *column_list_;
  // tell printer whether current stmt is a set left/right subquery
  bool force_col_alias_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_SELECT_STMT_PRINTER_H_
