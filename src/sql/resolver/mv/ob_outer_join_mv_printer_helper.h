/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_OUTER_JOIN_MV_PRINTER_HELPER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_OUTER_JOIN_MV_PRINTER_HELPER_H_
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObMVPrinter;

class ObOuterJoinMVPrinterHelper
{
public:
  explicit ObOuterJoinMVPrinterHelper(const ObMVPrinter &printer)
    : printer_(printer) {}
  ~ObOuterJoinMVPrinterHelper() {}

protected:
  int init_outer_join_mv_printer_helper();
  int gen_refresh_dmls_for_table(const TableItem *table,
                                 const JoinedTable *upper_table,
                                 ObIArray<ObDMLStmt*> &dml_stmts);

private:
  virtual int gen_refresh_dmls_for_inner_join(const TableItem *delta_table,
                                              const int64_t delta_table_idx,
                                              ObIArray<ObDMLStmt*> &dml_stmts) = 0;
  virtual int gen_refresh_dmls_for_left_join(const TableItem *delta_table,
                                             const int64_t delta_table_idx,
                                             const JoinedTable *upper_table,
                                             ObIArray<ObDMLStmt*> &dml_stmts) = 0;
  int update_table_idx_array(const int64_t delta_table_idx,
                             const JoinedTable *upper_table);

protected:
  ObSEArray<ObSqlBitSet<>, 8> right_table_idxs_;
  ObSqlBitSet<> refreshed_table_idxs_;

private:
  const ObMVPrinter &printer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOuterJoinMVPrinterHelper);
};

}
}

#endif