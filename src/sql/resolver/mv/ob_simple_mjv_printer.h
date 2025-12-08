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
  int prepare_gen_access_delta_data_for_simple_mjv(ObSelectStmt *&base_delta_stmt,
                                                   ObIArray<ObRawExpr*> &semi_filters,
                                                   ObIArray<ObRawExpr*> &anti_filters);
  int gen_one_access_delta_data_for_simple_mjv(const ObSelectStmt &base_delta_stmt,
                                               const int64_t table_idx,
                                               const ObIArray<ObRawExpr*> &semi_filters,
                                               const ObIArray<ObRawExpr*> &anti_filters,
                                               ObSelectStmt *&sel_stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleMJVPrinter);
};

}
}

#endif
