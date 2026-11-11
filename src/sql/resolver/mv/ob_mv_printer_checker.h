/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_PRINTER_CHECKER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_PRINTER_CHECKER_H_

#include "sql/resolver/mv/ob_mv_checker.h"

namespace oceanbase
{
namespace sql
{

// Common capability shared by printer-based checkers.
class ObMVPrinterCheckerBase : public ObMVChecker
{
protected:
  explicit ObMVPrinterCheckerBase(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result)
      : ObMVChecker(stmt, params, result)
  {}

  inline bool is_child_stmt(const ObSelectStmt *stmt) const { return &stmt_ != stmt; }

  int check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_join_shape_valid(const ObSelectStmt &stmt, bool &is_valid, bool &has_outer_join);

private:
  int check_mv_table_type_valid(const ObSelectStmt &stmt, bool &is_valid);
  static int check_mv_has_non_proctime_table(const ObSelectStmt &stmt, bool &has_non_proctime_table);

  int check_mv_join_type(const ObSelectStmt &stmt, bool &is_valid_join, bool &has_outer_join);
  int is_mv_join_type_valid(const ObSelectStmt &stmt,
                            const TableItem *table,
                            ObRelIds &null_side_tables,
                            bool &is_valid_join,
                            bool &has_outer_join,
                            bool &null_side_has_non_proctime_table);
  static int check_null_reject_or_not_contain(const ObIArray<ObRawExpr *> &conditions,
                                              const ObIArray<const ObRawExpr *> &table_col_exprs,
                                              const int64_t table_rel_id,
                                              bool &is_valid);

  DISALLOW_COPY_AND_ASSIGN(ObMVPrinterCheckerBase);
};

// Checks fast refresh-ability based on Printer-model
class ObMVPrinterChecker final : public ObMVPrinterCheckerBase
{
public:
  static int check(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result);

private:
  explicit ObMVPrinterChecker(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result)
      : ObMVPrinterCheckerBase(stmt, params, result),
        stmt_idx_(OB_INVALID_INDEX)
  {}

  int check_mv_stmt_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);

  // MAV checks.
  static int is_standard_select_in_group_by(const hash::ObHashSet<uint64_t> &expr_set,
                                            const ObRawExpr *expr,
                                            const bool is_no_aggr_col_allowed,
                                            bool &is_standard);
  int check_mav_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  int check_mav_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid);
  int check_is_valid_mysql_mode_group_by(const ObSelectStmt &stmt, bool &is_valid);
  int check_mav_aggr_items(const ObSelectStmt &stmt, bool &is_valid);
  int check_mav_aggr_item(const ObSelectStmt &stmt, ObAggFunRawExpr *aggr, bool &is_valid);
  void check_group_recalculate_constraints(const ObSelectStmt &stmt, bool &is_valid);

  // MJV checks.
  int check_mjv_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type);
  int collect_join_dep_cols(const ObSelectStmt &stmt, const bool has_outer_join);
  int collect_tables_primary_key_for_select(const ObSelectStmt &stmt, const bool has_outer_join);

  // UNION ALL checks.
  static bool is_child_refresh_type_supported(const ObMVRefreshableType refresh_type);
  int check_union_all_refresh_type(ObMVRefreshableType &refresh_type);
  int collect_union_all_mv_marker_column();

private:
  int64_t stmt_idx_;  // current union all child index
  DISALLOW_COPY_AND_ASSIGN(ObMVPrinterChecker);
};

}  // namespace sql
}  // namespace oceanbase

#endif
