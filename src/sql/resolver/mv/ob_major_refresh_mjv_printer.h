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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MAJOR_REFRESH_MJV_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MAJOR_REFRESH_MJV_PRINTER_H_
#include "sql/resolver/mv/ob_mv_printer.h"

namespace oceanbase
{
namespace sql
{

class ObMajorRefreshMJVPrinter : public ObMVPrinter
{
public:
  explicit ObMajorRefreshMJVPrinter(ObMVPrinterCtx &ctx,
                                    const share::schema::ObTableSchema &mv_schema,
                                    const share::schema::ObTableSchema &mv_container_schema,
                                    const ObSelectStmt &mv_def_stmt)
    : ObMVPrinter(ctx, mv_schema, mv_container_schema, mv_def_stmt, NULL)
    {}

  ~ObMajorRefreshMJVPrinter() {}

  static int set_refresh_table_scan_flag_for_mr_mv(ObSelectStmt &refresh_stmt);
  static int set_real_time_table_scan_flag_for_mr_mv(ObSelectStmt &rt_mv_stmt);
  static const uint64_t MR_MV_RT_QUERY_LEADING_TABLE_FLAG = 0x1 << 2;

private:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  int get_rowkey_pos_in_select(ObIArray<int64_t> &rowkey_sel_pos);
  int fill_table_partition_name(const TableItem &src_table,
                                TableItem &table);
  int append_rowkey_range_filter(const ObIArray<SelectItem> &select_items,
                                 uint64_t rowkey_count,
                                 ObIArray<ObRawExpr*> &conds);
  int gen_mr_rt_mv_access_mv_data_stmt(ObSelectStmt *&sel_stmt);
  int create_mr_rt_mv_delta_stmt(const TableItem &orig_table, ObSelectStmt *&sel_stmt);
  int create_mr_rt_mv_access_mv_from_table(ObSelectStmt &sel_stmt,
                                           const TableItem &mv_table,
                                           const TableItem &delta_left_table,
                                           const TableItem &delta_right_table);
  int gen_mr_rt_mv_access_mv_data_select_list(ObSelectStmt &sel_stmt,
                                              const TableItem &mv_table,
                                              const TableItem &delta_left_table,
                                              const TableItem &delta_right_table);
  int gen_mr_rt_mv_left_delta_data_stmt(ObSelectStmt *&stmt);
  int gen_exists_cond_for_mview(const TableItem &source_table,
                                const TableItem &outer_table,
                                ObRawExpr *&exists_expr);
  int gen_one_refresh_select_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                   const bool is_delta_left,
                                                   ObSelectStmt *&delta_stmt);
  int gen_refresh_validation_select_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                          ObSelectStmt *&delta_stmt);
  int gen_refresh_select_hint_for_major_refresh_mjv(const TableItem &left_table,
                                                    const TableItem &right_table,
                                                    ObStmtHint &stmt_hint);
  int prepare_gen_access_delta_data_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                          ObSelectStmt &base_delta_stmt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorRefreshMJVPrinter);
};

}
}

#endif
