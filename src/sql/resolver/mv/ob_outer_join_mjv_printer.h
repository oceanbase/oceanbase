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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_OUTER_JOIN_MJV_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_OUTER_JOIN_MJV_PRINTER_H_
#include "sql/resolver/mv/ob_mv_printer.h"

namespace oceanbase
{
namespace sql
{

class ObOuterJoinMJVPrinter : public ObMVPrinter
{
public:
  explicit ObOuterJoinMJVPrinter(ObMVPrinterCtx &ctx,
                                 const share::schema::ObTableSchema &mv_schema,
                                 const share::schema::ObTableSchema &mv_container_schema,
                                 const ObSelectStmt &mv_def_stmt,
                                 const MlogSchemaPairIArray &mlog_tables)
    : ObMVPrinter(ctx, mv_schema, mv_container_schema, mv_def_stmt, &mlog_tables)
      {}

  ~ObOuterJoinMJVPrinter() {}

private:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  int gen_delta_pre_table_views();
  int gen_refresh_dmls_for_table(const TableItem *table,
                                 const JoinedTable *upper_table,
                                 ObSqlBitSet<> &refreshed_table_idxs,
                                 ObIArray<ObDMLStmt*> &dml_stmts);
  int update_table_idx_array(const int64_t delta_table_idx,
                             const JoinedTable *upper_table,
                             ObSqlBitSet<> &refreshed_table_idxs);
  int gen_refresh_dmls_for_inner_join(const TableItem *delta_table,
                                      const int64_t delta_table_idx,
                                      const ObSqlBitSet<> &refreshed_table_idxs,
                                      ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_refresh_dmls_for_left_join(const TableItem *delta_table,
                                     const int64_t delta_table_idx,
                                     const JoinedTable *upper_table,
                                     const ObSqlBitSet<> &refreshed_table_idxs,
                                     ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_delete_for_inner_join(const TableItem *delta_table,
                                ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_insert_for_outer_join_mjv(const int64_t delta_table_idx,
                                    const ObSqlBitSet<> &refreshed_table_idxs,
                                    const JoinedTable *upper_table,
                                    const bool is_outer_join,
                                    ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_insert_select_stmt(const int64_t delta_table_idx,
                             const ObSqlBitSet<> &refreshed_table_idxs,
                             const JoinedTable *upper_table,
                             const bool is_outer_join,
                             ObSelectStmt *&sel_stmt);
  int gen_tables_for_insert_select_stmt(const int64_t delta_table_idx,
                                        const ObSqlBitSet<> &refreshed_table_idxs,
                                        ObSelectStmt *sel_stmt);
  int gen_delete_for_left_join(const TableItem *delta_table,
                               const int64_t delta_table_idx,
                               const JoinedTable *upper_table,
                               const ObSqlBitSet<> &refreshed_table_idxs,
                               const bool is_first_delete,
                               ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_select_for_left_join_first_delete(const TableItem *delta_table,
                                            const int64_t delta_table_idx,
                                            const JoinedTable *upper_table,
                                            const ObSqlBitSet<> &refreshed_table_idxs,
                                            ObSelectStmt *&cond_sel_stmt);
  int get_left_table_rowkey_exprs(const int64_t delta_table_idx,
                                  const TableItem *outer_table,
                                  const ObIArray<ObRawExpr*> &join_conditions,
                                  const ObSqlBitSet<> &except_table_idxs,
                                  ObIArray<int64_t> &other_join_table_idxs,
                                  ObIArray<ObRawExpr*> &other_join_table_rowkeys,
                                  ObIArray<ObRawExpr*> &all_other_table_rowkeys);
  int gen_mv_stat_winfunc_expr(ObRawExpr *pk_expr,
                               const ObIArray<ObRawExpr*> &part_exprs,
                               ObSelectStmt *sel_stmt);
  int gen_update_for_left_join(const TableItem *delta_table,
                               const int64_t delta_table_idx,
                               const ObSqlBitSet<> &refreshed_table_idxs,
                               ObIArray<ObDMLStmt*> &dml_stmts);
  int add_set_null_to_upd_stmt(const ObRelIds &set_null_tables,
                               const TableItem *mv_table,
                               ObIArray<ObAssignment> &assignments);
  int gen_select_for_left_join_second_delete(const TableItem *delta_table,
                                             const int64_t delta_table_idx,
                                             const JoinedTable *upper_table,
                                             const ObSqlBitSet<> &refreshed_table_idxs,
                                             ObSelectStmt *&cond_sel_stmt);

private:
  ObSEArray<ObSelectStmt*, 8, common::ModulePageAllocator, true> all_delta_table_views_;
  ObSEArray<ObSelectStmt*, 8, common::ModulePageAllocator, true> all_pre_table_views_;
  ObSEArray<ObSqlBitSet<>, 8, common::ModulePageAllocator, true> right_table_idxs_;

  DISALLOW_COPY_AND_ASSIGN(ObOuterJoinMJVPrinter);
};

}
}

#endif