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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_SIMPLE_JOIN_MAV_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_SIMPLE_JOIN_MAV_PRINTER_H_
#include "sql/resolver/mv/ob_simple_mav_printer.h"

namespace oceanbase
{
namespace sql
{

class ObSimpleJoinMAVPrinter : public ObSimpleMAVPrinter
{
public:
  explicit ObSimpleJoinMAVPrinter(ObMVPrinterCtx &ctx,
                                  const share::schema::ObTableSchema &mv_schema,
                                  const ObSelectStmt &mv_def_stmt,
                                  const MlogSchemaPairIArray &mlog_tables,
                                  const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs)
    : ObSimpleMAVPrinter(ctx, mv_schema, mv_def_stmt, mlog_tables, expand_aggrs)
    {}

  ~ObSimpleJoinMAVPrinter() {}

private:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs) override;
  int gen_update_insert_delete_for_simple_join_mav(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_merge_for_simple_join_mav(ObIArray<ObDMLStmt *> &dml_stmts);
  int gen_inner_delta_mav_for_simple_join_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs);
  int gen_inner_delta_mav_for_simple_join_mav(const int64_t inner_delta_no,
                                              const ObIArray<ObSelectStmt*> &all_delta_datas,
                                              const ObIArray<ObSelectStmt*> &all_pre_datas,
                                              ObSelectStmt *&inner_delta_mav);
  int construct_table_items_for_simple_join_mav_delta_data(const int64_t inner_delta_no,
                                                           const ObIArray<ObSelectStmt*> &all_delta_datas,
                                                           const ObIArray<ObSelectStmt*> &all_pre_datas,
                                                           ObSelectStmt *&stmt);
  int gen_delta_data_access_stmt(const TableItem &source_table, ObSelectStmt *&access_sel);
  int gen_pre_data_access_stmt(const TableItem &source_table, ObSelectStmt *&access_sel);
  int gen_unchanged_data_access_stmt(const TableItem &source_table, ObSelectStmt *&access_sel);
  int gen_deleted_data_access_stmt(const TableItem &source_table, ObSelectStmt *&access_sel);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleJoinMAVPrinter);
};

}
}

#endif
