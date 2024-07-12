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
    OB_MV_REFRESH_INVALID  = 0,     // refresh type not set
    OB_MV_COMPLETE_REFRESH,         // can not fast refresh
    OB_MV_FAST_REFRESH_SIMPLE_MAV, // fast refresh for single table MAV
    OB_MV_FAST_REFRESH_SIMPLE_MJV, // fast refresh for inner join MJV
  };

struct FastRefreshableNotes
{
  FastRefreshableNotes()
    : inner_errcode_(OB_SUCCESS),
      basic_("ObMVChecker"),
      simple_mav_("ObMVChecker")
    {}
  ~FastRefreshableNotes() {}

  TO_STRING_KV(K_(basic),
               K_(simple_mav));

  int inner_errcode_;
  ObSqlString basic_;
  ObSqlString simple_mav_;
};

class ObMVChecker
{
  public:
  explicit ObMVChecker(const ObSelectStmt &stmt,
                       ObRawExprFactory &expr_factory,
                       ObSQLSessionInfo *session_info)
    : stmt_(stmt),
      refresh_type_(OB_MV_REFRESH_INVALID),
      expr_factory_(expr_factory),
      session_info_(session_info),
      fast_refreshable_note_(NULL)
    {}
  ~ObMVChecker() {}

  static int check_mv_fast_refresh_valid(const ObSelectStmt *view_stmt,
                                         ObStmtFactory *stmt_factory,
                                         ObRawExprFactory *expr_factory,
                                         ObSQLSessionInfo *session_info);
  int check_mv_refresh_type();
  ObMVRefreshableType get_refersh_type() const { return refresh_type_; };
  static bool is_basic_aggr(const ObItemType aggr_type);
  static int get_dependent_aggr_of_fun_sum(const ObSelectStmt &stmt, const ObRawExpr *sum_param, const ObAggFunRawExpr *&dep_aggr);
  const ObSelectStmt &get_stmt() const {  return stmt_; }
  const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &get_expand_aggrs() const {  return expand_aggrs_;  }
  int get_mlog_table_schema(const TableItem *table, const share::schema::ObTableSchema *&mlog_schema) const;
  void set_fast_refreshable_note(FastRefreshableNotes *note) {  fast_refreshable_note_ = note; }
private:
  int check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_mv_join_type(const ObSelectStmt &stmt, bool &join_type_valid);
  bool is_mv_join_type_valid(const TableItem *table);
  int check_select_contains_all_tables_primary_key(const ObSelectStmt &stmt, bool &contain_all_rowkey);
  int check_mv_stmt_use_special_expr(const ObSelectStmt &stmt, bool &has_special_expr);
  int check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid);
  int check_mv_duplicated_exprs(const ObSelectStmt &stmt, bool &has_dup_exprs);
  bool check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                              const ObIArray<ColumnItem> &columns,
                              const share::schema::ObTableSchema &mlog_schema,
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
  void append_fast_refreshable_note(const char *str, const ObMVRefreshableType type = OB_MV_COMPLETE_REFRESH);

  const ObSelectStmt &stmt_;
  ObMVRefreshableType refresh_type_;
  // map the table in mv to mlog, use physical table id
  common::ObSEArray<std::pair<const TableItem*, const share::schema::ObTableSchema*>, 4, common::ModulePageAllocator, true> mlog_tables_;
  common::ObSEArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>, 4, common::ModulePageAllocator, true> expand_aggrs_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *session_info_;
  FastRefreshableNotes *fast_refreshable_note_;
  DISALLOW_COPY_AND_ASSIGN(ObMVChecker);
};

}
}

#endif
