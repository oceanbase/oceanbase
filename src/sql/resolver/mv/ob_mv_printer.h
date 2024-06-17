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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_PRINTER_H_
#include "lib/string/ob_string.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/container/ob_se_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"

namespace oceanbase
{
namespace sql
{

struct SharedPrinterRawExprs
{
  SharedPrinterRawExprs()
    : null_expr_(NULL),
      int_zero_(NULL),
      int_one_(NULL),
      int_neg_one_(NULL),
      last_refresh_scn_(NULL),
      refresh_scn_(NULL),
      str_n_(NULL),
      str_o_(NULL)
  {}

  ObRawExpr *null_expr_;
  ObConstRawExpr *int_zero_;
  ObConstRawExpr *int_one_;
  ObConstRawExpr *int_neg_one_;
  ObRawExpr *last_refresh_scn_;
  ObConstRawExpr *refresh_scn_;
  ObConstRawExpr *str_n_;
  ObConstRawExpr *str_o_;
};

class ObMVPrinter
{
public:
  explicit ObMVPrinter(ObIAllocator &alloc,
                       const share::schema::ObTableSchema &mv_schema,
                       const ObMVChecker &mv_checker,
                       bool for_rt_expand,
                       ObStmtFactory &stmt_factory,
                       ObRawExprFactory &expr_factory)
    : inited_(false),
      alloc_(alloc),
      mv_checker_(mv_checker),
      mv_schema_(mv_schema),
      for_rt_expand_(for_rt_expand),
      stmt_factory_(stmt_factory),
      expr_factory_(expr_factory),
      exprs_()
    {}
  ~ObMVPrinter() {}

  // view name
  static const ObString DELTA_TABLE_VIEW_NAME;
  static const ObString DELTA_BASIC_MAV_VIEW_NAME;
  static const ObString DELTA_MAV_VIEW_NAME;
  static const ObString INNER_RT_MV_VIEW_NAME;
  // column name
  static const ObString HEAP_TABLE_ROWKEY_COL_NAME;
  static const ObString OLD_NEW_COL_NAME;
  static const ObString SEQUENCE_COL_NAME;
  static const ObString DML_FACTOR_COL_NAME;
  static const ObString WIN_MAX_SEQ_COL_NAME;
  static const ObString WIN_MIN_SEQ_COL_NAME;

  static int print_mv_operators(const share::schema::ObTableSchema &mv_schema,
                                const ObSelectStmt &view_stmt,
                                const bool for_rt_expand,
                                const share::SCN &last_refresh_scn,
                                const share::SCN &refresh_scn,
                                ObIAllocator &alloc,
                                ObIAllocator &str_alloc,
                                ObSchemaGetterGuard *schema_guard,
                                ObStmtFactory &stmt_factory,
                                ObRawExprFactory &expr_factory,
                                ObSQLSessionInfo *session_info,
                                ObIArray<ObString> &operators,
                                ObMVRefreshableType &refreshable_type);

private:
  int init(const share::SCN &last_refresh_scn, const share::SCN &refresh_scn);
  int gen_mv_operator_stmts(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_refresh_dmls_for_mv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_real_time_view_for_mv(ObSelectStmt *&sel_stmt);
  int gen_delete_insert_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_insert_into_select_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_real_time_view_for_simple_mjv(ObSelectStmt *&sel_stmt);
  int gen_delete_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_access_mv_data_for_simple_mjv(ObSelectStmt *&sel_stmt);
  int gen_exists_cond_for_mjv(const ObIArray<ObRawExpr*> &upper_sel_exprs,
                              const TableItem *source_table,
                              bool is_exists,
                              ObRawExpr *&exists_expr);
  int gen_access_delta_data_for_simple_mjv(ObIArray<ObSelectStmt*> &access_delta_stmts);
  int prepare_gen_access_delta_data_for_simple_mjv(ObSelectStmt *&base_delta_stmt,
                                                   ObIArray<ObRawExpr*> &semi_filters,
                                                   ObIArray<ObRawExpr*> &anti_filters);
  int gen_one_access_delta_data_for_simple_mjv(const ObSelectStmt &base_delta_stmt,
                                               const int64_t table_idx,
                                               const ObIArray<ObRawExpr*> &semi_filters,
                                               const ObIArray<ObRawExpr*> &anti_filters,
                                               ObSelectStmt *&sel_stmt);
  int gen_real_time_view_for_simple_mav(ObSelectStmt *&sel_stmt);
  int gen_real_time_view_scn_cte(ObSelectStmt &root_stmt);
  int gen_real_time_view_filter_for_mav(ObSelectStmt &sel_stmt);
  int gen_inner_real_time_view_for_mav(ObSelectStmt &sel_stmt, TableItem *&view_table);
  int gen_inner_real_time_view_tables_for_mav(ObSelectStmt &sel_stmt);
  int gen_inner_real_time_view_select_list_for_mav(ObSelectStmt &sel_stmt);
  int gen_select_items_for_mav(const ObString &table_name,
                               const uint64_t table_id,
                               ObIArray<SelectItem> &select_items);
  int gen_merge_for_simple_mav(ObMergeStmt *&merge_stmt);
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
  int gen_merge_tables(ObMergeStmt &merge_stmt,
                       TableItem *&target_table,
                       TableItem *&source_table);
  int gen_insert_values_and_desc(const TableItem *target_table,
                                 const TableItem *source_table,
                                 ObIArray<ObColumnRefRawExpr*> &target_columns,
                                 ObIArray<ObRawExpr*> &values_exprs);
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
  int init_expr_copier_for_delta_mv_view(const TableItem &table, ObRawExprCopier &copier);
  int gen_simple_mav_delta_mv_select_list(ObRawExprCopier &copier,
                                          const TableItem &table,
                                          const ObIArray<ObRawExpr*> &group_by_exprs,
                                          ObIArray<SelectItem> &select_items);
  int get_mv_select_item_name(const ObRawExpr *expr, ObString &select_name);
  int gen_basic_aggr_expr(ObRawExprCopier &copier,
                          ObRawExpr *dml_factor,
                          ObAggFunRawExpr &aggr_expr,
                          ObRawExpr *&aggr_print_expr);
  int add_nvl_above_exprs(ObRawExpr *expr, ObRawExpr *default_expr, ObRawExpr *&res_expr);
  int gen_simple_mav_delta_mv_filter(ObRawExprCopier &copier,
                                     const TableItem &table_item,
                                     ObIArray<ObRawExpr*> &filters);
  int gen_delta_table_view(const TableItem &source_table, ObSelectStmt *&view_stmt);
  int gen_delta_table_view_conds(const TableItem &table, ObIArray<ObRawExpr*> &conds);
  int gen_delta_table_view_select_list(const TableItem &table,
                                       const TableItem &source_table,
                                       ObSelectStmt &stmt);
  int gen_max_min_seq_window_func_exprs(const ObSqlSchemaGuard &sql_schema_guard,
                                        const TableItem &table,
                                        const TableItem &source_table,
                                        ObRawExpr *sequence_expr,
                                        ObRawExpr *&win_max_expr,
                                        ObRawExpr *&win_min_expr);
  int create_simple_column_expr(const ObString &table_name,
                                const ObString &column_name,
                                const uint64_t table_id,
                                ObRawExpr *&expr);
  int create_simple_table_item(ObDMLStmt *stmt,
                               const ObString &table_name,
                               TableItem *&table_item,
                               ObSelectStmt *view_stmt = NULL,
                               const bool add_to_from = true);
  template <typename StmtType>
  inline int create_simple_stmt(StmtType *&stmt)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(stmt_factory_.get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(stmt_factory_.get_query_ctx()));
    } else if (OB_FAIL(stmt_factory_.create_stmt(stmt))) {
      LOG_WARN("failed to create stmt", K(ret));
    } else if (OB_UNLIKELY(stmt::T_SELECT != stmt->get_stmt_type()
                           && stmt::T_DELETE != stmt->get_stmt_type()
                           && stmt::T_UPDATE != stmt->get_stmt_type()
                           && stmt::T_INSERT != stmt->get_stmt_type()
                           && stmt::T_MERGE != stmt->get_stmt_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(ret), K(stmt->get_stmt_type()));
    } else {
      stmt->set_query_ctx(stmt_factory_.get_query_ctx());
    }
    return ret;
  }

private:
  bool inited_;
  ObIAllocator &alloc_;
  const ObMVChecker &mv_checker_;
  const share::schema::ObTableSchema &mv_schema_;
  ObString mv_db_name_;
  bool for_rt_expand_;
  ObStmtFactory &stmt_factory_;
  ObRawExprFactory &expr_factory_;
  SharedPrinterRawExprs exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObMVPrinter);
};

}
}

#endif
