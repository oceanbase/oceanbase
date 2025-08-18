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
#include "sql/resolver/dml/ob_select_stmt.h"

#define DELTA_TABLE_FORMAT_NAME "DLT_%.*s$$"
#define PRE_TABLE_FORMAT_NAME "PRE_%.*s$$"

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
      mv_last_refresh_scn_(NULL),
      mv_refresh_scn_(NULL),
      str_n_(NULL),
      str_o_(NULL)
  {}

  ObRawExpr *null_expr_;
  ObConstRawExpr *int_zero_;
  ObConstRawExpr *int_one_;
  ObConstRawExpr *int_neg_one_;
  ObRawExpr *last_refresh_scn_;
  ObConstRawExpr *refresh_scn_;
  ObConstRawExpr *mv_last_refresh_scn_;
  ObConstRawExpr *mv_refresh_scn_;
  ObConstRawExpr *str_n_;
  ObConstRawExpr *str_o_;
};

struct ObMVPrinterRefreshInfo  {
  ObMVPrinterRefreshInfo(const share::SCN &last_refresh_scn,
                         const share::SCN &refresh_scn,
                         const int64_t part_idx = OB_INVALID_INDEX,
                         const int64_t sub_part_idx = OB_INVALID_INDEX,
                         const ObNewRange *range = NULL)
    : last_refresh_scn_(last_refresh_scn),
      refresh_scn_(refresh_scn),
      mv_last_refresh_scn_(NULL),
      mv_refresh_scn_(NULL),
      part_idx_(part_idx),
      sub_part_idx_(sub_part_idx),
      range_(range)
  {}

  TO_STRING_KV(K_(last_refresh_scn), K_(refresh_scn), KPC_(mv_last_refresh_scn), KPC_(mv_refresh_scn),
              K_(part_idx), K_(sub_part_idx), KPC_(range));

  const share::SCN &last_refresh_scn_;
  const share::SCN &refresh_scn_;
  const share::SCN *mv_last_refresh_scn_;
  const share::SCN *mv_refresh_scn_;
  const int64_t part_idx_;
  const int64_t sub_part_idx_;
  const ObNewRange *range_;
};

struct ObMVPrinterCtx {
  ObMVPrinterCtx(ObIAllocator &alloc,
                 ObSQLSessionInfo &session_info,
                 ObStmtFactory &stmt_factory,
                 ObRawExprFactory &expr_factory,
                 const ObMVPrinterRefreshInfo *refresh_info = NULL)
  : alloc_(alloc),
    session_info_(session_info),
    stmt_factory_(stmt_factory),
    expr_factory_(expr_factory),
    refresh_info_(refresh_info),
    marker_idx_(OB_INVALID_INDEX)
  {}
  inline bool for_rt_expand() const { return NULL == refresh_info_; }
  inline bool for_union_all_child_query() const { return OB_INVALID_INDEX != marker_idx_; }
  ObIAllocator &alloc_;
  ObSQLSessionInfo &session_info_;
  ObStmtFactory &stmt_factory_;
  ObRawExprFactory &expr_factory_;
  const ObMVPrinterRefreshInfo *refresh_info_;
  int64_t marker_idx_; // for print union all child query
};

class ObMVPrinter
{
public:
  explicit ObMVPrinter(ObMVPrinterCtx &ctx,
                       const share::schema::ObTableSchema &mv_schema,
                       const share::schema::ObTableSchema &mv_container_schema,
                       const ObSelectStmt &mv_def_stmt,
                       const MlogSchemaPairIArray *mlog_tables)
    : ctx_(ctx),
      inited_(false),
      mv_db_name_(),
      mv_schema_(mv_schema),
      mv_container_schema_(mv_container_schema),
      mv_def_stmt_(mv_def_stmt),
      exprs_(),
      mlog_tables_(mlog_tables)
      {}

  ~ObMVPrinter() {}

  // view name
  static const ObString DELTA_TABLE_VIEW_NAME;
  static const ObString DELTA_BASIC_MV_VIEW_NAME;
  static const ObString DELTA_MV_VIEW_NAME;
  static const ObString INNER_RT_MV_VIEW_NAME;
  static const ObString MV_STAT_VIEW_NAME;
  // column name
  static const ObString HEAP_TABLE_ROWKEY_COL_NAME;
  static const ObString OLD_NEW_COL_NAME;
  static const ObString SEQUENCE_COL_NAME;
  static const ObString DML_FACTOR_COL_NAME;
  static const ObString WIN_MAX_SEQ_COL_NAME;
  static const ObString WIN_MIN_SEQ_COL_NAME;
  static const ObString WIN_ROW_NUM_COL_NAME;
  static const ObString WIN_CNT_ALL_COL_NAME;
  static const ObString WIN_CNT_NOT_NULL_COL_NAME;

  int print_mv_operators(ObIAllocator &str_alloc, ObIArray<ObString> &operators);
  int gen_child_refresh_dmls_for_union_all(const int64_t marker_idx, ObIArray<ObDMLStmt*> &dml_stmts);
  static int print_complete_refresh_mview_operator(ObRawExprFactory &expr_factory,
                                                  const share::SCN *mv_refresh_scn,
                                                  const share::SCN *table_refresh_scn,
                                                  ObSelectStmt &mview_stmt,
                                                  ObIAllocator &str_alloc,
                                                  ObString &mview_str);
protected:
  enum MlogExtColFlag {
    MLOG_EXT_COL_ROWKEY_ONLY  = 0x0,
    MLOG_EXT_COL_OLD_NEW      = 0x1,
    MLOG_EXT_COL_SEQ          = 0x1 << 1,
    MLOG_EXT_COL_DML_FACTOR   = 0x1 << 2,
    MLOG_EXT_COL_WIN_MIN_SEQ  = 0x1 << 3,
    MLOG_EXT_COL_WIN_MAX_SEQ  = 0x1 << 4,
    MLOG_EXT_COL_ALL_NORMAL_COL  = 0x1 << 5,
  };

  virtual int gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts) = 0;
  virtual int gen_real_time_view(ObSelectStmt *&sel_stmt) = 0;
  int init();
  int gen_mv_operator_stmts(ObIArray<ObDMLStmt*> &dml_stmts);
  int get_mlog_table_schema(const TableItem *table,
                            const share::schema::ObTableSchema *&mlog_schema) const;
  int gen_exists_cond_for_table(const TableItem *source_table,
                                const TableItem *outer_table,
                                const bool is_exists,
                                const bool use_orig_sel_alias,
                                ObRawExpr *&exists_expr);
  int gen_rowkey_join_conds_for_table(const TableItem &origin_table,
                                      const TableItem &left_table,
                                      const TableItem &right_table,
                                      const bool right_use_orig_sel_alias,
                                      ObIArray<ObRawExpr*> &all_conds);
  int get_column_name_from_origin_select_items(const uint64_t table_id,
                                               const uint64_t column_id,
                                               const ObString *&col_name);
  void set_info_for_simple_table_item(TableItem &table, const TableItem &source_table);
  int init_expr_copier_for_stmt(ObSelectStmt &target_stmt, ObRawExprCopier &copier);
  int init_expr_copier_for_table(const TableItem *origin_table,
                                 const TableItem *target_table,
                                 ObRawExprCopier &copier);
  int construct_from_items_for_simple_mjv_delta_data(ObRawExprCopier &copier,
                                                     ObSelectStmt &target_stmt);
  int get_mv_select_item_name(const ObRawExpr *expr,
                              ObString &select_name,
                              const bool ignore_empty_res = false);
  int append_old_new_row_filter(const TableItem &table_item,
                                ObIArray<ObRawExpr*> &filters,
                                const bool get_old_row = true,
                                const bool get_new_row = true);
  int gen_delta_pre_table_view(const TableItem *ori_table,
                               ObSelectStmt *&view_stmt,
                               const bool is_delta_view,
                               const bool need_hint = true);
  int gen_delta_mlog_table_view(const TableItem &source_table,
                                ObSelectStmt *&view_stmt,
                                const uint64_t ext_sel_flags = UINT64_MAX);
  int gen_mlog_table_scn_filters(const TableItem &mlog_source_table,
                                 const TableItem &mlog_table,
                                 ObIArray<ObRawExpr*> &conds);
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
                                       ObIArray<SelectItem> &select_items,
                                       bool need_all_normal_col = true);
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
  int create_union_all_child_refresh_filter(const int64_t marker_idx,
                                            const TableItem *table,
                                            ObRawExpr *&marker_filter);
  int assign_simple_sel_stmt(ObSelectStmt &target_stmt, ObSelectStmt &source_stmt);
  int append_old_new_col_filter(const TableItem &table, ObIArray<ObRawExpr*>& conds);
  int deep_copy_mv_def_stmt(ObSelectStmt *&new_stmt);
  int get_table_logic_pk_ids(const ObTableSchema *table_schema, ObIArray<uint64_t> &logic_pk_ids);
  int get_table_rowkey_exprs(const TableItem &table, const TableItem &source_table, const bool use_orig_sel_alias, ObIArray<ObRawExpr *> &rowkey_exprs);
  int gen_mv_rowkey_expr(const TableItem *mv_table, ObOpRawExpr *&rowkey_expr);
  int add_col_exprs_into_select(ObIArray<SelectItem> &select_items, const ObIArray<ObRawExpr*> &col_exprs);
  int add_mv_rowkey_into_select(ObSelectStmt *stmt, const TableItem *mv_table);
  int get_mv_rowkey_column_ids(ObIArray<uint64_t> &rowkey_column_ids);
  int add_semi_to_inner_hint(ObDMLStmt *stmt);
  int add_dynamic_sampling_hint(ObDMLStmt *stmt, const TableItem *table);
  bool is_table_skip_refresh(const TableItem &table) const;
  template <typename StmtType>
  inline int create_simple_stmt(StmtType *&stmt)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(ctx_.stmt_factory_.get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(ctx_.stmt_factory_.get_query_ctx()));
    } else if (OB_FAIL(ctx_.stmt_factory_.create_stmt(stmt))) {
      LOG_WARN("failed to create stmt", K(ret));
    } else if (OB_UNLIKELY(stmt::T_SELECT != stmt->get_stmt_type()
                           && stmt::T_DELETE != stmt->get_stmt_type()
                           && stmt::T_UPDATE != stmt->get_stmt_type()
                           && stmt::T_INSERT != stmt->get_stmt_type()
                           && stmt::T_MERGE != stmt->get_stmt_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(ret), K(stmt->get_stmt_type()));
    } else {
      stmt->set_query_ctx(ctx_.stmt_factory_.get_query_ctx());
    }
    return ret;
  }

  static int set_table_read_snapshot_recursively(ObRawExpr *mv_refresh_scn,
                                                ObRawExpr *table_refresh_scn,
                                                ObSelectStmt *stmt);
protected:
  ObMVPrinterCtx &ctx_;
  bool inited_;
  ObString mv_db_name_;
  const share::schema::ObTableSchema &mv_schema_;
  const share::schema::ObTableSchema &mv_container_schema_;
  const ObSelectStmt &mv_def_stmt_;
  SharedPrinterRawExprs exprs_;
  // map the table in mv to mlog, use physical table id
  const MlogSchemaPairIArray *mlog_tables_;
  DISALLOW_COPY_AND_ASSIGN(ObMVPrinter);
};

}
}

#endif
