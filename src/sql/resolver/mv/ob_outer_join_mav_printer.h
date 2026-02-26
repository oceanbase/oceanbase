/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_OUTER_JOIN_MAV_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_OUTER_JOIN_MAV_PRINTER_H_
#include "sql/resolver/mv/ob_simple_join_mav_printer.h"
#include "sql/resolver/mv/ob_outer_join_mv_printer_helper.h"

namespace oceanbase
{
namespace sql
{

class ObOuterJoinMAVPrinter : public ObSimpleJoinMAVPrinter, public ObOuterJoinMVPrinterHelper
{
public:
  explicit ObOuterJoinMAVPrinter(ObMVPrinterCtx &ctx,
                                 const share::schema::ObTableSchema &mv_schema,
                                 const share::schema::ObTableSchema &mv_container_schema,
                                 const ObSelectStmt &mv_def_stmt,
                                 const MlogSchemaPairIArray &mlog_tables,
                                 const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs)
    : ObSimpleJoinMAVPrinter(ctx, mv_schema, mv_container_schema, mv_def_stmt, mlog_tables, expand_aggrs),
      ObOuterJoinMVPrinterHelper(static_cast<const ObMVPrinter&>(*this))
    {}

  ~ObOuterJoinMAVPrinter() {}

private:
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  virtual int gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs) override;
  virtual int gen_refresh_dmls_for_inner_join(const TableItem *delta_table,
                                              const int64_t delta_table_idx,
                                              ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_refresh_dmls_for_left_join(const TableItem *delta_table,
                                             const int64_t delta_table_idx,
                                             const JoinedTable *upper_table,
                                             ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int get_delta_pre_view_stmt(const int64_t table_idx,
                                      const int64_t inner_delta_no,
                                      ObSelectStmt *&view_stmt) const override;
  int gen_inner_delta_mav_for_left_join_joined_data(const int64_t delta_table_idx,
                                                    const JoinedTable *upper_table,
                                                    ObSelectStmt *&inner_delta_mav);
  int gen_inner_delta_mav_for_left_join_padded_null_data(const TableItem *delta_table,
                                                         const int64_t delta_table_idx,
                                                         const JoinedTable *upper_table,
                                                         ObSelectStmt *&inner_delta_mav_new_null,
                                                         ObSelectStmt *&inner_delta_mav_pre_null);
  int construct_tables_for_padded_null_data(const int64_t delta_table_idx,
                                            ObRawExprCopier &copier_with_null_table,
                                            ObRawExprCopier &copier_without_null_table,
                                            ObSelectStmt *inner_delta_mav);
  int construct_joined_table_for_padded_null_data(const TableItem *ori_table,
                                                  const ObIArray<TableItem*> &table_item_map,
                                                  ObSelectStmt *inner_delta_mav,
                                                  ObRawExprCopier &expr_copier,
                                                  TableItem *&new_table);
  int gen_exists_padded_null_conds(const TableItem *delta_table,
                                   const int64_t delta_table_idx,
                                   const ObIArray<ObRawExpr*> &join_conds,
                                   const bool is_new_null,
                                   ObIArray<ObRawExpr*> &conds);

private:
  DISALLOW_COPY_AND_ASSIGN(ObOuterJoinMAVPrinter);
};

}
}

#endif