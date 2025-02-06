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

struct MajorRefreshInfo  {
  MajorRefreshInfo(const int64_t part_idx, const int64_t sub_part_idx, const ObNewRange &range)
    : part_idx_(part_idx),
      sub_part_idx_(sub_part_idx),
      range_(range)
  {}

  bool is_valid_info() const {  return OB_INVALID_INDEX != part_idx_ || OB_INVALID_INDEX != sub_part_idx_ || !range_.is_whole_range();}

  TO_STRING_KV(K_(part_idx), K_(sub_part_idx), K_(range));
  const int64_t part_idx_;
  const int64_t sub_part_idx_;
  const ObNewRange &range_;
  DISALLOW_COPY_AND_ASSIGN(MajorRefreshInfo);
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
      exprs_(),
      major_refresh_info_(NULL)
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
                                const share::schema::ObTableSchema &mv_container_schema,
                                const ObSelectStmt &view_stmt,
                                const bool for_rt_expand,
                                const share::SCN &last_refresh_scn,
                                const share::SCN &refresh_scn,
                                const MajorRefreshInfo *major_refresh_info,
                                ObIAllocator &alloc,
                                ObIAllocator &str_alloc,
                                ObSchemaGetterGuard *schema_guard,
                                ObStmtFactory &stmt_factory,
                                ObRawExprFactory &expr_factory,
                                ObSQLSessionInfo *session_info,
                                ObIArray<ObString> &operators,
                                ObMVRefreshableType &refreshable_type);
  static int set_refresh_table_scan_flag_for_mr_mv(ObSelectStmt &refresh_stmt);
  static int set_real_time_table_scan_flag_for_mr_mv(ObSelectStmt &rt_mv_stmt);
  static const uint64_t MR_MV_RT_QUERY_LEADING_TABLE_FLAG = 0x1 << 2;

private:
  enum MlogExtColFlag {
    MLOG_EXT_COL_OLD_NEW      = 0x1,
    MLOG_EXT_COL_SEQ          = 0x1 << 1,
    MLOG_EXT_COL_DML_FACTOR   = 0x1 << 2,
    MLOG_EXT_COL_WIN_MIN_SEQ  = 0x1 << 3,
    MLOG_EXT_COL_WIN_MAX_SEQ  = 0x1 << 4,
  };

  int init(const share::SCN &last_refresh_scn, const share::SCN &refresh_scn, const MajorRefreshInfo *major_refresh_info);
  int gen_mv_operator_stmts(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_refresh_dmls_for_mv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_real_time_view_for_mv(ObSelectStmt *&sel_stmt);
  int gen_delete_insert_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_insert_into_select_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_update_insert_delete_for_simple_join_mav(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_merge_for_simple_join_mav(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_real_time_view_for_simple_mjv(ObSelectStmt *&sel_stmt);
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
  int gen_delete_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int gen_access_mv_data_for_simple_mjv(ObSelectStmt *&sel_stmt);
  int gen_exists_cond_for_table(const TableItem *source_table,
                                const TableItem *outer_table,
                                const bool is_exists,
                                const bool use_orig_sel_alias,
                                ObRawExpr *&exists_expr);
  int gen_exists_cond_for_mview(const TableItem &source_table,
                                const TableItem &outer_table,
                                ObRawExpr *&exists_expr);
  int gen_rowkey_join_conds_for_table(const TableItem &origin_table,
                                      const TableItem &left_table,
                                      const TableItem &right_table,
                                      const bool right_use_orig_sel_alias,
                                      ObIArray<ObRawExpr*> &all_conds);
  int get_column_name_from_origin_select_items(const uint64_t table_id,
                                               const uint64_t column_id,
                                               const ObString *&col_name);
  int gen_access_delta_data_for_simple_mjv(ObIArray<ObSelectStmt*> &access_delta_stmts);
  int prepare_gen_access_delta_data_for_simple_mjv(ObSelectStmt *&base_delta_stmt,
                                                   ObIArray<ObRawExpr*> &semi_filters,
                                                   ObIArray<ObRawExpr*> &anti_filters);
  int construct_table_items_for_simple_mjv_delta_data(ObSelectStmt *stmt);
  void set_info_for_simple_table_item(TableItem &table, const TableItem &source_table);
  int init_expr_copier_for_stmt(ObSelectStmt &target_stmt, ObRawExprCopier &copier);
  int construct_from_items_for_simple_mjv_delta_data(ObRawExprCopier &copier,
                                                     ObSelectStmt &target_stmt);
  int gen_one_access_delta_data_for_simple_mjv(const ObSelectStmt &base_delta_stmt,
                                               const int64_t table_idx,
                                               const ObIArray<ObRawExpr*> &semi_filters,
                                               const ObIArray<ObRawExpr*> &anti_filters,
                                               ObSelectStmt *&sel_stmt);
  int gen_real_time_view_for_mav(ObSelectStmt *&sel_stmt);
  int gen_real_time_view_filter_for_mav(ObSelectStmt &sel_stmt);
  int gen_inner_real_time_view_for_mav(ObSelectStmt *&inner_rt_view);
  int gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs);
  int gen_select_items_for_mav(const ObString &table_name,
                               const uint64_t table_id,
                               ObIArray<SelectItem> &select_items);
  int gen_merge_for_simple_mav(ObMergeStmt *&merge_stmt);
  int gen_merge_for_simple_mav_use_delta_view(ObSelectStmt *delta_mav, ObMergeStmt *&merge_stmt);
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
  int gen_simple_mav_delta_mv_select_list(ObRawExprCopier &copier,
                                          const TableItem &table,
                                          const ObIArray<ObRawExpr*> &group_by_exprs,
                                          ObIArray<SelectItem> &select_items);
  int gen_simple_join_mav_basic_select_list(const TableItem &table,
                                            ObIArray<SelectItem> &select_items,
                                            ObIArray<ObRawExpr*> *group_by_exprs);
  int get_mv_select_item_name(const ObRawExpr *expr, ObString &select_name);
  int gen_basic_aggr_expr(ObRawExprCopier &copier,
                          ObRawExpr *dml_factor,
                          ObAggFunRawExpr &aggr_expr,
                          ObRawExpr *&aggr_print_expr);
  int add_nvl_above_exprs(ObRawExpr *expr, ObRawExpr *default_expr, ObRawExpr *&res_expr);
  int append_old_new_row_filter(const TableItem &table_item,
                                ObIArray<ObRawExpr*> &filters,
                                const bool get_old_row = true,
                                const bool get_new_row = true);
  int gen_delta_table_view(const TableItem &source_table,
                           ObSelectStmt *&view_stmt,
                           const uint64_t ext_sel_flags = UINT64_MAX);
  int gen_delta_table_view_conds(const TableItem &table, ObIArray<ObRawExpr*> &conds);
  int gen_delta_table_view_select_list(const TableItem &table,
                                       const TableItem &source_table,
                                       ObSelectStmt &stmt,
                                       const uint64_t ext_sel_flags = UINT64_MAX);
  int add_dml_factor_to_select_list(ObRawExpr *old_new_col,
                                    ObIArray<SelectItem> &select_items);
  int add_normal_column_to_select_list(const TableItem &table,
                                       const ObString &col_name,
                                       ObIArray<SelectItem> &select_items);
  int add_normal_column_to_select_list(const TableItem &table,
                                       const TableItem &source_table,
                                       ObIArray<SelectItem> &select_items);
  int add_max_min_seq_window_to_select_list(const TableItem &table,
                                            const TableItem &source_table,
                                            ObRawExpr *sequence_expr,
                                            ObIArray<SelectItem> &select_items,
                                            bool need_win_max_col,
                                            bool need_win_min_col);
  int gen_max_min_seq_window_func_exprs(const TableItem &table,
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
  int create_joined_table_item(ObDMLStmt *stmt,
                               const ObJoinType joined_type,
                               const TableItem &left_table,
                               const TableItem &right_table,
                               const bool is_top,
                               JoinedTable *&joined_table);

  int gen_refresh_select_for_major_refresh_mjv(ObIArray<ObDMLStmt*> &dml_stmts);
  int get_rowkey_pos_in_select(ObIArray<int64_t> &rowkey_sel_pos);
  int gen_real_time_view_for_major_refresh_mjv(ObSelectStmt *&sel_stmt);
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
  int gen_not_exists_cond_for_major_refresh_mjv(const ObIArray<ObRawExpr*> &upper_sel_exprs,
                                                const TableItem *source_table,
                                                ObRawExpr *&exists_expr);
  int gen_mr_rt_mv_left_delta_data_stmt(ObSelectStmt *&stmt);
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
  int append_old_new_col_filter(const TableItem &table, ObIArray<ObRawExpr*>& conds);
  int fill_table_partition_name(const TableItem &src_table, TableItem &table);
  int append_rowkey_range_filter(const ObIArray<SelectItem> &select_items,
                                 uint64_t rowkey_count,
                                 ObIArray<ObRawExpr*> &conds);
  int assign_simple_sel_stmt(ObSelectStmt &target_stmt, ObSelectStmt &source_stmt);
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
  const MajorRefreshInfo *major_refresh_info_;
  DISALLOW_COPY_AND_ASSIGN(ObMVPrinter);
};

}
}

#endif
