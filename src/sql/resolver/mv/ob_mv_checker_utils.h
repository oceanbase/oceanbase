/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_CHECKER_UTILS_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_CHECKER_UTILS_H_

#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObSQLSessionInfo;
class ObRawExprFactory;
class ObMVCheckerResult;

class ObMVCheckerUtils
{
public:
  // Aggregate classification.
  static bool is_group_recalculate_aggr(const ObRawExpr &aggr);
  static bool is_basic_count(const ObRawExpr &aggr);
  static bool is_basic_sum(const ObRawExpr &aggr);
  static bool is_count_star(const ObRawExpr &aggr);
  static bool is_basic_aggr(const ObRawExpr &aggr);

  // Aggregate dependent-column collection.
  static int collect_aggr_dep_cols(const ObSelectStmt &stmt,
                                   ObRawExprFactory &expr_factory,
                                   ObSQLSessionInfo &session_info,
                                   const int64_t stmt_idx,
                                   const bool enable_simplify_aggr_dep,
                                   ObMVCheckerResult &result);
  static int get_equivalent_count_aggr(const ObSelectStmt &stmt,
                                       ObRawExpr *count_param,
                                       ObSQLSessionInfo *session_info,
                                       const bool enable_simplify_aggr_dep,
                                       int64_t &select_idx);
private:
  // Aggregate dependent-column collection internals.
  static int collect_default_count_expr(const ObSelectStmt &stmt,
                                        ObRawExprFactory &expr_factory,
                                        ObSQLSessionInfo &session_info,
                                        const int64_t stmt_idx,
                                        ObMVCheckerResult &result);
  static int collect_equivalent_count_aggr(const ObSelectStmt &stmt,
                                           ObRawExpr *count_param,
                                           ObRawExprFactory &expr_factory,
                                           ObSQLSessionInfo &session_info,
                                           const bool enable_simplify_aggr_dep,
                                           const int64_t stmt_idx,
                                           ObMVCheckerResult &result);
  static int find_default_count_expr(const ObSelectStmt &stmt, int64_t &select_idx);
  static int get_equivalent_null_check_param(ObRawExpr *param_expr, ObRawExpr *&check_param);
  static int build_count_expr(ObRawExprFactory &expr_factory,
                              ObSQLSessionInfo &session_info,
                              ObRawExpr *param_expr,
                              ObAggFunRawExpr *&count_expr);
  static int check_union_all_marker_candidate(const ObRawExpr &set_expr,
                                              const ObRawExpr &branch_expr,
                                              const common::ObIArray<ObRawExpr *> &marker_exprs,
                                              bool &is_valid);

public:
  static int collect_outer_join_output_dep_cols(const ObSelectStmt &stmt,
                                                const int64_t stmt_idx,
                                                ObMVCheckerResult &result);
  static int find_union_all_marker_column(const ObSelectStmt &stmt, int64_t &marker_idx);
  static int build_union_all_marker_expr(ObRawExprFactory &expr_factory,
                                         ObSQLSessionInfo &session_info,
                                         ObConstRawExpr *&marker_expr);
  static const char *get_table_type_str(const TableItem::TableType type);
  static int get_table_rowkey_ids(const ObTableSchema *table_schema,
                                  ObSchemaGetterGuard *schema_guard,
                                  ObIArray<uint64_t> &rowkey_ids);
  static int get_missing_table_key_column_ids(const ObSelectStmt &stmt,
                                              const TableItem &table_item,
                                              const ObTableSchema *table_schema,
                                              ObSchemaGetterGuard *schema_guard,
                                              ObIArray<uint64_t> &missing_column_ids);
  static int check_has_rowid_exprs(const ObDMLStmt *stmt, bool &has_rowid);
};

}  // namespace sql
}  // namespace oceanbase

#endif
