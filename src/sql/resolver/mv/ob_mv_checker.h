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

enum ObMVRefreshableType
  {
    OB_MV_REFRESH_INVALID  = 0,           // refresh type not set
    OB_MV_COMPLETE_REFRESH,               // can not fast refresh
    OB_MV_FAST_REFRESH_SIMPLE_MAV,        // fast refresh for single table MAV
    OB_MV_FAST_REFRESH_SIMPLE_MJV,        // fast refresh for inner join MJV
    OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV,   // fast refresh for inner join MAV
    OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV, // fast refresh for major compaction MV
    OB_MV_FAST_REFRESH_OUTER_JOIN_MJV,    // fast refresh for outer join MJV
    OB_MV_FAST_REFRESH_UNION_ALL,         // fast refresh for union all query
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
  int record_table_referenced_columns(const TableItem *table_item,
                                      const ObIArray<ColumnItem> &columns);
  int convert_to_required_columns_infos(ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos);
  int append_to_table_referenced_columns(const uint64_t table_id,
                                         common::hash::ObHashSet<uint64_t> &table_referenced_columns);

private:
  bool is_inited_;
  common::ObArenaAllocator inner_alloc_;
  TableReferencedColumnsMap table_referenced_columns_map_;
};

class ObMVChecker
{
  public:
  explicit ObMVChecker(const ObSelectStmt &stmt,
                       ObRawExprFactory &expr_factory,
                       ObSQLSessionInfo *session_info,
                       const ObTableSchema &mv_container_table_schema,

                       const bool need_on_query_computation,
                       FastRefreshableNotes &note,
                       ObTableReferencedColumnsInfo *table_referenced_columns_info = nullptr)

    : stmt_(stmt),
      refresh_type_(OB_MV_REFRESH_INVALID),
      expr_factory_(expr_factory),
      session_info_(session_info),
      mv_container_table_schema_(mv_container_table_schema),
      need_on_query_computation_(need_on_query_computation),
      fast_refreshable_error_(note.error_),
      marker_idx_(OB_INVALID_INDEX),
      table_referenced_columns_info_(table_referenced_columns_info)
    {
    }
  ~ObMVChecker() {}
  void reset();
  static int check_mv_fast_refresh_type(const ObSelectStmt *view_stmt,
                                        ObIAllocator *allocator,
                                        ObSchemaChecker *schema_checker,
                                        ObStmtFactory *stmt_factory,
                                        ObRawExprFactory *expr_factory,
                                        ObSQLSessionInfo *session_info,
                                        ObTableSchema &container_table_schema,
                                        const bool need_on_query_computation,
                                        ObMVRefreshableType &refresh_type,
                                        FastRefreshableNotes &note,
                                        ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos);
  int check_mv_refresh_type();
  int check_mv_stmt_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  ObMVRefreshableType get_refersh_type() const { return refresh_type_; };
  static bool is_basic_aggr(const ObItemType aggr_type);
  static int get_dependent_aggr_of_fun_sum(const ObSelectStmt &stmt, const ObRawExpr *sum_param, const ObAggFunRawExpr *&dep_aggr);
  const ObSelectStmt &get_stmt() const {  return stmt_; }
  const MlogSchemaPairIArray &get_mlog_tables() const {  return mlog_tables_; }
  const ObTableSchema &get_mv_container_table_schema() const {  return mv_container_table_schema_;  }
  const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &get_expand_aggrs() const {  return expand_aggrs_;  }
  int64_t get_union_all_marker_idx() const {  return marker_idx_;  }
  const ObIArray<ObMVRefreshableType> &get_child_refresh_types() const {  return child_refresh_types_;  }
private:
  int check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_mv_join_type(const ObSelectStmt &stmt, bool &is_valid_join, bool &has_outer_join);
  int is_mv_join_type_valid(const ObSelectStmt &stmt,
                            const TableItem *table,
                            ObRelIds &null_side_tables,
                            bool &is_valid_join,
                            bool &has_outer_join);
  int check_null_reject_or_not_contain(const ObIArray<ObRawExpr*> &conditions,
                                       const ObIArray<const ObRawExpr*> &table_col_exprs,
                                       const int64_t table_rel_id,
                                       bool &is_valid);
  int check_select_contains_all_tables_primary_key(const ObSelectStmt &stmt,
                                                   bool &all_table_exists_rowkey,
                                                   bool &contain_all_rowkey);
  int check_select_all_single_column(const ObSelectStmt &stmt, bool &contain_all_single_columns);
  int check_mv_stmt_use_special_expr(const ObSelectStmt &stmt, bool &has_special_expr);
  int check_mv_table_type_valid(const ObSelectStmt &stmt, bool &is_valid);
  int check_has_rowid_exprs(const ObDMLStmt *stmt, bool &has_rowid);
  int check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid);
  int check_mv_duplicated_exprs(const ObSelectStmt &stmt, bool &has_dup_exprs);
  bool check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                              const ObIArray<ColumnItem> &columns,
                              const share::schema::ObTableSchema &mlog_schema,
                              ObSchemaGetterGuard *schema_guard,
                              bool &is_valid);
  int check_mav_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  int check_mav_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_is_standard_group_by(const ObSelectStmt &stmt, bool &is_standard);
  int is_standard_select_in_group_by(const hash::ObHashSet<uint64_t> &expr_set,
                                     const ObRawExpr *expr,
                                     bool &is_standard);
  int check_and_expand_mav_aggrs(const ObSelectStmt &stmt,
                                 ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs,
                                 bool &is_valid);
  int check_and_expand_mav_aggr(const ObSelectStmt &stmt,
                                ObAggFunRawExpr *aggr,
                                ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs,
                                bool &is_valid);
  int try_replace_equivalent_count_aggr(const ObSelectStmt &stmt,
                                        const int64_t orig_aggr_count,
                                        ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                        ObRawExpr *&replace_expr);
  static int get_equivalent_null_check_param(const ObRawExpr *param_expr, const ObRawExpr *&check_param);
  static int get_mav_default_count(const ObIArray<ObAggFunRawExpr*> &aggrs, const ObAggFunRawExpr *&count_aggr);
  static int get_target_aggr(const ObItemType target_aggr_type,
                             const ObRawExpr *param_expr,
                             const ObIArray<ObAggFunRawExpr*> &aggrs,
                             const ObAggFunRawExpr *&target_aggr);
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
  int check_union_all_mv_marker_column_valid(const ObSelectStmt &stmt, bool &is_valid);
private:
  const ObSelectStmt &stmt_;
  ObMVRefreshableType refresh_type_;
  // map the table in mv to mlog, use physical table id
  common::ObSEArray<std::pair<const TableItem*, const share::schema::ObTableSchema*>, 4, common::ModulePageAllocator, true> mlog_tables_;
  common::ObSEArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>, 4, common::ModulePageAllocator, true> expand_aggrs_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *session_info_;
  const ObTableSchema &mv_container_table_schema_;
  bool need_on_query_computation_;
  ObSqlString &fast_refreshable_error_;
  int64_t marker_idx_;  // union all mv marker column index in select list
  // union all mv child refreshable type
  common::ObSEArray<ObMVRefreshableType, 4, common::ModulePageAllocator, true> child_refresh_types_;
  ObTableReferencedColumnsInfo *table_referenced_columns_info_;
  DISALLOW_COPY_AND_ASSIGN(ObMVChecker);
};

}
}

#endif
