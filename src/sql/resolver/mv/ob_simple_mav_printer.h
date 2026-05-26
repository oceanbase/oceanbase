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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_SIMPLE_MAV_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_SIMPLE_MAV_PRINTER_H_
#include "sql/resolver/mv/ob_mv_printer.h"

namespace oceanbase
{
namespace sql
{

class ObSimpleMAVPrinter : public ObMVPrinter
{
public:
  explicit ObSimpleMAVPrinter(ObMVPrinterCtx &ctx,
                              const share::schema::ObTableSchema &mv_schema,
                              const share::schema::ObTableSchema &mv_container_schema,
                              const ObSelectStmt &mv_def_stmt,
                              const MlogSchemaPairIArray &mlog_tables)
    : ObMVPrinter(ctx, mv_schema, mv_container_schema, mv_def_stmt, &mlog_tables)
    {}

  ~ObSimpleMAVPrinter() {}

protected:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  virtual int gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs);
  int gen_real_time_view_filter_for_mav(ObSelectStmt &sel_stmt);
  int gen_inner_real_time_view_for_mav(ObSelectStmt *&inner_rt_view);
  int gen_merge_for_simple_mav(ObMergeStmt *&merge_stmt);
  int gen_merge_for_simple_mav_use_delta_view(ObSelectStmt *delta_mav, ObMergeStmt *&merge_stmt);
  int gen_insert_values_and_desc(const TableItem *target_table,
                                 const TableItem *source_table,
                                 ObIArray<ObColumnRefRawExpr*> &target_columns,
                                 ObIArray<ObRawExpr*> &values_exprs);
  int gen_select_items_for_mav(const TableItem &target,
                               ObIArray<SelectItem> &select_items);
  int add_replaced_expr_for_other_count(ObRawExprCopier &copier);
  int gen_calc_expr_for_insert_clause_sum(ObRawExpr *source_count,
                                          ObRawExpr *source_sum,
                                          ObRawExpr *&calc_sum);
  int gen_calc_expr_for_update_clause_sum(ObRawExpr *sum_count,
                                          ObRawExpr *target_sum,
                                          ObRawExpr *source_sum,
                                          ObRawExpr *&calc_sum);
  int gen_update_assignments(const TableItem &target_table,
                             const TableItem &source_table,
                             ObIArray<ObAssignment> &assignments);
  int gen_merge_conds(ObMergeStmt &merge_stmt);
  int gen_simple_mav_delta_mv_view(ObSelectStmt *&view_stmt);
  int gen_simple_mav_delta_mv_select_list(ObRawExprCopier &copier,
                                          const TableItem *table,
                                          const int64_t explicit_dml_factor,
                                          const ObIArray<ObRawExpr*> &group_by_exprs,
                                          ObIArray<SelectItem> &select_items);
  int gen_simple_join_mav_basic_select_list(const TableItem &table,
                                            ObIArray<SelectItem> &select_items,
                                            ObIArray<ObRawExpr*> *group_by_exprs);
  int gen_basic_aggr_expr(ObRawExprCopier &copier,
                          ObRawExpr *dml_factor,
                          ObAggFunRawExpr &aggr_expr,
                          ObRawExpr *&aggr_print_expr);
  int add_nvl_above_exprs(ObRawExpr *expr, ObRawExpr *default_expr, ObRawExpr *&res_expr);
  int add_any_value_above_expr(ObRawExpr *expr, ObRawExpr *&res_expr);
  int add_replaced_expr_for_min_max_aggr(const TableItem &source_table, ObRawExprCopier &copier);
  int get_inner_sel_name_for_aggr(const ObAggFunRawExpr &aggr, ObString &sel_name);
  int gen_group_recalculate_aggr_view(ObSelectStmt *&view_stmt);
  int gen_mav_delta_mv_view(ObSelectStmt *simple_delta_stmt, ObSelectStmt *&delta_stmt);
protected:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleMAVPrinter);
};

}
}

#endif
