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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_CHECKER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_CHECKER_H_
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{
struct TableItem;

enum ObMVRefreshableType // FARM COMPAT WHITELIST
{
  OB_MV_REFRESH_INVALID  = 0,           // refresh type not set
  OB_MV_COMPLETE_REFRESH,               // can not fast refresh
  OB_MV_FAST_REFRESH_SIMPLE_MAV,        // fast refresh for single table MAV
  OB_MV_FAST_REFRESH_SIMPLE_MJV,        // fast refresh for inner join MJV
  OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV,   // fast refresh for inner join MAV
  OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV, // fast refresh for major compaction MV
  OB_MV_FAST_REFRESH_OUTER_JOIN_MJV,    // fast refresh for outer join MJV
  OB_MV_FAST_REFRESH_UNION_ALL,         // fast refresh for union all query
  OB_MV_FAST_REFRESH_OUTER_JOIN_MAV,    // fast refresh for outer join MAV
};

inline bool IS_VALID_FAST_REFRESH_TYPE(ObMVRefreshableType type)
{
  return type >= OB_MV_FAST_REFRESH_SIMPLE_MAV;
}

struct FastRefreshableNotes
{
  FastRefreshableNotes() : error_("ObMVChecker") {}
  ~FastRefreshableNotes() {}

  TO_STRING_KV(K_(error));

  ObSqlString error_;
};

typedef common::ObIArray<std::pair<const TableItem*, const share::schema::ObTableSchema*>> MlogSchemaPairIArray;
typedef common::hash::ObHashMap<uint64_t, common::hash::ObHashSet<uint64_t> *> TableReferencedColumnsMap;

class ObTableReferencedColumnsInfo
{
public:
  ObTableReferencedColumnsInfo() : is_inited_(false), inner_alloc_("TableRefCol", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID) {}
  ~ObTableReferencedColumnsInfo() { destroy(); }

  int init();
  void destroy();
  int record_table_referenced_columns(const uint64_t table_ref_id,
                                      const ObIArray<uint64_t> &col_ids);
  int convert_to_required_columns_infos(ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos);
  int append_to_table_referenced_columns(const uint64_t table_id,
                                         common::hash::ObHashSet<uint64_t> &table_referenced_columns);

private:
  bool is_inited_;
  common::ObArenaAllocator inner_alloc_;
  TableReferencedColumnsMap table_referenced_columns_map_;
};

struct MvCheckerExtraInfo {
  MvCheckerExtraInfo() : mlog_tables_(),
                         union_all_marker_idx_(OB_INVALID_INDEX),
                         child_refresh_types_() {}
  ObSEArray<std::pair<const TableItem*, const share::schema::ObTableSchema*>, 4>  mlog_tables_;
  // for union all mv
  int64_t union_all_marker_idx_;
  ObSEArray<ObMVRefreshableType, 4> child_refresh_types_;
};

class ObMVChecker
{
  public:
  explicit ObMVChecker(uint64_t tenant_id,
                       const ObSelectStmt &stmt,
                       ObRawExprFactory &expr_factory,
                       ObSQLSessionInfo *session_info,
                       const ObTableSchema &mv_container_table_schema,

                       const bool need_on_query_computation,
                       FastRefreshableNotes &note,
                       common::ObIArray<std::pair<ObRawExpr*, int64_t>> &fast_refresh_dependent_columns,
                       ObTableReferencedColumnsInfo *table_referenced_columns_info = nullptr)

    : tenant_id_(tenant_id),
      stmt_(stmt),
      refresh_type_(OB_MV_REFRESH_INVALID),
      expr_factory_(expr_factory),
      session_info_(session_info),
      mv_container_table_schema_(mv_container_table_schema),
      need_on_query_computation_(need_on_query_computation),
      fast_refreshable_error_(note.error_),
      marker_idx_(OB_INVALID_INDEX),
      table_referenced_columns_info_(table_referenced_columns_info),
      refresh_dep_columns_(fast_refresh_dependent_columns),
      stmt_idx_(OB_INVALID_INDEX),
      enable_simplify_aggr_dep_(false)
    {
    }
  ~ObMVChecker() {}
  void reset();
  static int check_mv_fast_refresh_type(const ObSelectStmt *view_stmt,
                                        const bool need_pre_process_view_stmt,
                                        ObIAllocator *allocator,
                                        ObSchemaChecker *schema_checker,
                                        ObStmtFactory *stmt_factory,
                                        ObRawExprFactory *expr_factory,
                                        ObSQLSessionInfo *session_info,
                                        const ObTableSchema &container_table_schema,
                                        const bool need_on_query_computation,
                                        ObMVRefreshableType &refresh_type,
                                        FastRefreshableNotes &note,
                                        ObIArray<std::pair<ObRawExpr*, int64_t>> &fast_refresh_dependent_columns,
                                        ObTableReferencedColumnsInfo *table_referenced_columns_info = NULL,
                                        MvCheckerExtraInfo *extra_info = NULL);
  int check_mv_refresh_type();
  int check_mv_stmt_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  ObMVRefreshableType get_refresh_type() const { return refresh_type_; };
  static bool is_basic_aggr(const ObRawExpr &aggr);
  static bool is_basic_count(const ObRawExpr &aggr);
  static bool is_basic_sum(const ObRawExpr &aggr);
  static bool is_group_recalculate_aggr(const ObRawExpr &aggr);
  static int extract_group_recalculate_aggrs(const ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                             ObIArray<ObAggFunRawExpr*> &group_recalculate_aggrs);
  const ObSelectStmt &get_stmt() const {  return stmt_; }
  const MlogSchemaPairIArray &get_mlog_tables() const {  return mlog_tables_; }
  const ObTableSchema &get_mv_container_table_schema() const {  return mv_container_table_schema_;  }
  int64_t get_union_all_marker_idx() const {  return marker_idx_;  }
  const ObIArray<ObMVRefreshableType> &get_child_refresh_types() const {  return child_refresh_types_;  }
  static int pre_process_view_stmt(ObRawExprFactory *expr_factory,
                                   ObSQLSessionInfo *session_info,
                                   ObSelectStmt &view_stmt);
  static int adjust_set_stmt_sel_item(ObSelectStmt &view_stmt);
  static int reset_ref_query_stmt_id_recursively(ObSelectStmt *stmt);
  static int get_table_rowkey_ids(const ObTableSchema *table_schema,
                                  ObSchemaGetterGuard *schema_guard,
                                  ObIArray<uint64_t> &rowkey_ids);
  static int get_equivalent_count_aggr(const ObSelectStmt &stmt,
                                       const ObRawExpr *count_param,
                                       ObSQLSessionInfo *session_info,
                                       const bool enable_simplify_aggr_dep,
                                       int64_t &select_idx);
private:
  int check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_mv_join_type(const ObSelectStmt &stmt, bool &is_valid_join, bool &has_outer_join);
  int is_mv_join_type_valid(const ObSelectStmt &stmt,
                            const TableItem *table,
                            ObRelIds &null_side_tables,
                            bool &is_valid_join,
                            bool &has_outer_join,
                            bool &null_side_has_non_proctime_table);
  int check_null_reject_or_not_contain(const ObIArray<ObRawExpr*> &conditions,
                                       const ObIArray<const ObRawExpr*> &table_col_exprs,
                                       const int64_t table_rel_id,
                                       bool &is_valid);
  int collect_tables_primary_key_for_select(const ObSelectStmt &stmt,
                                            const bool has_outer_join);
  int collect_all_single_columns_for_select(const ObSelectStmt &stmt);
  int check_mv_table_type_valid(const ObSelectStmt &stmt, bool &is_valid);
  int check_mv_has_non_proctime_table(const ObSelectStmt &stmt, bool &has_non_proctime_table);
  int check_has_rowid_exprs(const ObDMLStmt *stmt, bool &has_rowid);
  int check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid);
  bool check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                              const TableItem *table_item,
                              const ObIArray<ColumnItem> &columns,
                              const share::schema::ObTableSchema &mlog_schema,
                              ObSchemaGetterGuard *schema_guard,
                              bool &is_valid);
  int check_mav_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  int check_mav_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_is_valid_mysql_mode_group_by(const ObSelectStmt &stmt, bool &is_valid);
  int is_standard_select_in_group_by(const hash::ObHashSet<uint64_t> &expr_set,
                                     const ObRawExpr *expr,
                                     const bool is_no_aggr_col_allowed,
                                     bool &is_standard);
  int check_mav_aggr_items(const ObSelectStmt &stmt,
                           bool &is_valid);
  int check_mav_aggr_item(const ObSelectStmt &stmt,
                          ObAggFunRawExpr *aggr,
                          bool &is_valid);
  void check_group_recalculate_constraints(const ObSelectStmt &stmt,
                                           bool &is_valid);
  int collect_equivalent_count_aggr(const ObSelectStmt &stmt,
                                    const ObRawExpr *count_param);
  static int get_equivalent_null_check_param(const ObRawExpr *param_expr, const ObRawExpr *&check_param);
  int collect_mav_default_count(const ObSelectStmt &stmt);
  static int find_default_count_expr(const ObSelectStmt &stmt,
                                     int64_t &select_idx);
  int check_mjv_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  int check_join_mv_fast_refresh_valid(const ObSelectStmt &stmt,
                                       const bool for_join_mav,
                                       bool &is_valid,
                                       bool &has_outer_join);
  int check_match_major_refresh_mv(const ObSelectStmt &stmt, bool &is_match);
  int check_select_item_valid(const ObSelectStmt &stmt,
                              const ObIArray<ObRawExpr*> &on_conds,
                              const uint64_t left_table_id,
                              bool &is_match);
  int check_right_table_join_key_valid(const ObSelectStmt &stmt, const JoinedTable *joined_table,
                                       const ObTableSchema *right_table_schema, bool &is_valid);
  int check_left_table_partition_rule_valid(const ObSelectStmt &stmt, const TableItem *left_table,
                                            const ObTableSchema *left_table_schema, bool &is_valid);
  int check_left_table_rowkey_valid(const ObSelectStmt &stmt,
                                    const ObTableSchema *left_table_schema, bool &is_valid);
  int check_broadcast_table_valid(const ObSelectStmt &stmt, const ObTableSchema *right_table_schema,
                                  bool &is_valid);
  int check_column_store_valid(const ObSelectStmt &stmt, const ObTableSchema *left_table_schema,
                               const ObTableSchema *right_table_schema, bool &is_valid);
  static const char *get_table_type_str(const TableItem::TableType type);
  static bool is_child_refresh_type_supported(const ObMVRefreshableType refresh_type);
  int check_union_all_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  int collect_union_all_mv_marker_column(const ObSelectStmt &stmt);
  inline bool is_child_stmt(const ObSelectStmt *stmt) const { return &stmt_ != stmt; }
  int add_dep_columns_no_dup(ObRawExpr* expr);

private:
  uint64_t tenant_id_;
  const ObSelectStmt &stmt_;
  ObMVRefreshableType refresh_type_;
  // map the table in mv to mlog, use physical table id
  common::ObSEArray<std::pair<const TableItem*, const share::schema::ObTableSchema*>, 4> mlog_tables_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *session_info_;
  const ObTableSchema &mv_container_table_schema_;
  bool need_on_query_computation_;
  ObSqlString &fast_refreshable_error_;
  int64_t marker_idx_;  // union all mv marker column index in select list
  // union all mv child refreshable type
  common::ObSEArray<ObMVRefreshableType, 4> child_refresh_types_;
  ObTableReferencedColumnsInfo *table_referenced_columns_info_;
  // refresh_dep_columns_: automatically generated invisible columns for refresh
  // for the non-set query, ObRawExpr is the dependent column, int64_t is always OB_INVALID_INDEX(-1)
  // for the set query's child dependency, ObRawExpr is the dependent column, int64_t is the index of set query(>=0)
  // for the set query's marker column, ObRawExpr is a const expr(0), int64_t is OB_INVALID_INDEX(-1)
  common::ObIArray<std::pair<ObRawExpr*, int64_t>> &refresh_dep_columns_;
  int64_t stmt_idx_; // union all child mv stmt idx in select list
  bool enable_simplify_aggr_dep_;
  DISALLOW_COPY_AND_ASSIGN(ObMVChecker);
};

}
}

#endif
