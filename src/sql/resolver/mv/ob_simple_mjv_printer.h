/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SQL_RESOLVER_MV_OB_SIMPLE_MJV_PRINTER_H_
 #define OCEANBASE_SQL_RESOLVER_MV_OB_SIMPLE_MJV_PRINTER_H_
 #include "sql/resolver/mv/ob_mv_printer.h"

 namespace oceanbase
 {
 namespace sql
 {

 class ObSimpleMJVPrinter : public ObMVPrinter
 {
 public:
   explicit ObSimpleMJVPrinter(ObMVPrinterCtx &ctx,
                               const share::schema::ObTableSchema &mv_schema,
                               const share::schema::ObTableSchema &mv_container_schema,
                               const ObSelectStmt &mv_def_stmt,
                               const MlogSchemaPairIArray &mlog_tables)
    : ObMVPrinter(ctx, mv_schema, mv_container_schema, mv_def_stmt, &mlog_tables)
     {}

   ~ObSimpleMJVPrinter() {}

private:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  int gen_delete_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_insert_into_select_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_access_mv_data_for_simple_mjv(ObSelectStmt *&sel_stmt);
  int gen_access_delta_data_for_simple_mjv(ObIArray<ObSelectStmt*> &access_delta_stmts);
  int gen_one_access_delta_data_for_simple_mjv(const int64_t delta_table_idx,
                                               ObIArray<ObRawExpr*> &anti_filters,
                                               ObSelectStmt *&delta_stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleMJVPrinter);
};

}
}

#endif
