/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_MAJOR_REFRESH_CHECKER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_MAJOR_REFRESH_CHECKER_H_

#include "sql/resolver/mv/ob_mv_printer_checker.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}  // namespace share

namespace sql
{
class ObRawExpr;
class ObSelectStmt;
class TableItem;

/**
 * This checker matches a two-table inner-join MV for major-compaction refresh.
 * Both base tables and the MV use row storage.
 * The left table and the MV use the same hash partition rule and rowkey.
 * The right table uses broadcast distribution.
 * The join uses the complete right-table rowkey.
 * The select list contains columns only and includes all key and left-table predicate columns.
 */
class ObMVMajorRefreshChecker final : public ObMVPrinterCheckerBase
{
public:
  static int check(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result);

private:
  explicit ObMVMajorRefreshChecker(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result)
      : ObMVPrinterCheckerBase(stmt, params, result)
  {}

  static int check_right_table_join_key_valid(const ObSelectStmt &stmt,
                                              const TableItem &left_table,
                                              const TableItem &right_table,
                                              const common::ObIArray<ObRawExpr *> &conditions,
                                              const share::schema::ObTableSchema &right_table_schema,
                                              bool &is_valid);
  static int check_select_item_valid(const ObSelectStmt &stmt,
                                     const common::ObIArray<ObRawExpr *> &conditions,
                                     const uint64_t left_table_id,
                                     bool &is_match);
  static int check_broadcast_table_valid(const share::schema::ObTableSchema &right_table_schema, bool &is_valid);
  int check_match_major_refresh_mv(const ObSelectStmt &stmt, bool &is_match);
  int check_left_table_partition_rule_valid(const ObSelectStmt &stmt,
                                            const TableItem &left_table,
                                            const share::schema::ObTableSchema &left_table_schema,
                                            bool &is_valid);
  int check_left_table_rowkey_valid(const share::schema::ObTableSchema &left_table_schema, bool &is_valid);
  int check_column_store_valid(const share::schema::ObTableSchema &left_table_schema,
                               const share::schema::ObTableSchema &right_table_schema,
                               bool &is_valid);

  DISALLOW_COPY_AND_ASSIGN(ObMVMajorRefreshChecker);
};

}  // namespace sql
}  // namespace oceanbase

#endif
