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
                              const ObSelectStmt &mv_def_stmt,
                              const MlogSchemaPairIArray &mlog_tables,
                              const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs)
    : ObMVPrinter(ctx, mv_schema, mv_def_stmt, &mlog_tables),
      expand_aggrs_(expand_aggrs)
    {}

  ~ObSimpleMAVPrinter() {}

protected:
  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) override;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) override;
  virtual int gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs);

  int get_equivalent_null_check_param(const ObRawExpr *param_expr,
                                      const ObRawExpr *&check_param);
  int get_mav_default_count(const ObIArray<ObAggFunRawExpr*> &aggrs,
                            const ObAggFunRawExpr *&count_aggr);
  int gen_update_insert_delete_for_simple_mav(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_insert_for_mav(ObSelectStmt *delta_mv_stmt,
                         ObIArray<ObRawExpr*> &values,
                         ObInsertStmt *&insert_stmt);
  int gen_select_for_insert_subquery(const ObIArray<ObRawExpr*> &values,
                                     ObIArray<SelectItem> &select_items);
  int gen_exists_cond_for_insert(const ObIArray<ObRawExpr*> &values,
                                 ObIArray<ObRawExpr*> &conds);
  int gen_update_for_mav(ObSelectStmt *delta_mv_stmt,
                         const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                         const ObIArray<ObRawExpr*> &values,
                         ObUpdateStmt *&update_stmt);
  int gen_update_conds(const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                       const ObIArray<ObRawExpr*> &values,
                       ObIArray<ObRawExpr*> &conds);
  int gen_delete_for_mav(const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                         ObDeleteStmt *&delete_stmt);
  int gen_delete_conds(const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                       ObIArray<ObRawExpr*> &conds);
  int gen_real_time_view_filter_for_mav(ObSelectStmt &sel_stmt);
  int gen_inner_real_time_view_for_mav(ObSelectStmt *&inner_rt_view);
  int gen_merge_for_simple_mav(ObMergeStmt *&merge_stmt);
  int gen_merge_for_simple_mav_use_delta_view(ObSelectStmt *delta_mav, ObMergeStmt *&merge_stmt);
  int gen_insert_values_and_desc(const TableItem *target_table,
                                 const TableItem *source_table,
                                 ObIArray<ObColumnRefRawExpr*> &target_columns,
                                 ObIArray<ObRawExpr*> &values_exprs);
  int gen_select_items_for_mav(const ObString &table_name,
                               const uint64_t table_id,
                               ObIArray<SelectItem> &select_items);
  int gen_calc_expr_for_insert_clause_sum(ObRawExpr *source_count,
                                          ObRawExpr *source_sum,
                                          ObRawExpr *&calc_sum);
  int gen_calc_expr_for_update_clause_sum(ObRawExpr *target_count,
                                          ObRawExpr *source_count,
                                          ObRawExpr *target_sum,
                                          ObRawExpr *source_sum,
                                          ObRawExpr *&calc_sum);
  int get_dependent_aggr_of_fun_sum(const ObRawExpr *expr,
                                    const ObIArray<SelectItem> &select_items,
                                    int64_t &idx);
  int gen_update_assignments(const ObIArray<ObColumnRefRawExpr*> &target_columns,
                             const ObIArray<ObRawExpr*> &values_exprs,
                             const TableItem *source_table,
                             ObIArray<ObAssignment> &assignments,
                             const bool for_mysql_update = false);
  int gen_merge_conds(ObMergeStmt &merge_stmt);
  int gen_simple_mav_delta_mv_view(ObSelectStmt *&view_stmt);
  int gen_simple_mav_delta_mv_select_list(ObRawExprCopier &copier,
                                          const TableItem &table,
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
  inline const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &get_expand_aggrs() const {  return expand_aggrs_;  }
protected:
  const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs_;
  DISALLOW_COPY_AND_ASSIGN(ObSimpleMAVPrinter);
};

}
}

#endif
