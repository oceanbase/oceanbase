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

 #ifndef OCEANBASE_SQL_RESOLVER_MV_OB_UNION_ALL_MV_PRINTER_H_
 #define OCEANBASE_SQL_RESOLVER_MV_OB_UNION_ALL_MV_PRINTER_H_
 #include "sql/resolver/mv/ob_mv_printer.h"
 
 namespace oceanbase
 {
 namespace sql
 {
 
 class ObUnionAllMVPrinter : public ObMVPrinter
 {
 public:
   explicit ObUnionAllMVPrinter(ObMVPrinterCtx &ctx,
                               const share::schema::ObTableSchema &mv_schema,
                               const share::schema::ObTableSchema &mv_container_schema,
                               const ObSelectStmt &mv_def_stmt,
                               const int64_t marker_idx,
                               const ObIArray<ObMVRefreshableType> &child_refresh_types,
                               const MlogSchemaPairIArray &mlog_tables,
                               const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs)
    : ObMVPrinter(ctx, mv_schema, mv_container_schema, mv_def_stmt, &mlog_tables),
      marker_idx_(marker_idx),
      child_refresh_types_(child_refresh_types),
      expand_aggrs_(expand_aggrs)
     {}

   ~ObUnionAllMVPrinter() {}
private:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  int gen_child_refresh_dmls(const ObMVRefreshableType refresh_type,
                             const ObSelectStmt &child_sel_stmt,
                             ObIArray<ObDMLStmt*> &dml_stmts);
  int get_child_expand_aggrs(const ObSelectStmt &child_sel_stmt,
                             ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &child_expand_aggrs);
private:
  int64_t marker_idx_;
  const ObIArray<ObMVRefreshableType> &child_refresh_types_;
  const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs_;
  DISALLOW_COPY_AND_ASSIGN(ObUnionAllMVPrinter);
};

}
}

#endif
